//go:build cluster

package warren_test

// Partition-under-load — pause_minority isolation, classifiable errors, zero loss
// (Phase 9.5 / T166g; SPEC §1 at-least-once + Lens-13 LT-05-cluster).
//
// A publisher streams confirmed messages to a QUORUM queue under continuous load
// while the harness injects a REAL network partition — disconnecting a follower node
// (rmq2) from the cluster's Docker network so it loses BOTH client and inter-node
// connectivity. A consumer drains concurrently. The contract under test:
//   - pause_minority isolates the minority: the partitioned node drops out of the
//     quorum queue's online member set (and the cluster's running-node count), while
//     the majority {rmq0, rmq1} keeps quorum and stays available — OBSERVED via the
//     management API on the surviving rmq0. A single-node broker has no minority to
//     pause, so this is unobservable below three nodes.
//   - the majority surfaces CLASSIFIABLE errors, not silent stalls: every error the
//     publisher sees during the partition is a known warren sentinel
//     (isTolerableFailoverErr — barrier / channel-closed / confirm gap); anything
//     else fails the test. Non-vacuously, the publisher must keep CONFIRMING after
//     the partition is detected (postPartitionFloor) — a publisher that silently
//     wedged on the cut would satisfy the warmup floor yet never get there.
//   - zero loss + recovery after heal: every confirmed publish is eventually consumed,
//     deduped by MessageID exactly as TV-09 mandates (the reconnect barrier and
//     PublishRetry produce duplicates by design; a genuinely dropped message is
//     caught by lossByMessageID, which chaos_reconnect_loss_test.go self-tests with an
//     injected drop). After heal the partitioned node rejoins (online/running return
//     to 3) and a recovery sentinel proves the consumer pipeline is live end to end.
//
// Why a real network cut and not a Toxiproxy AMQP cut: Toxiproxy fronts only the AMQP
// client ports (5672), so disabling a proxy severs CLIENTS but leaves inter-node
// Erlang distribution intact — it can never make a node a minority, so it can never
// trigger pause_minority. Disconnecting the node's Docker-network membership
// (PartitionNode) is what isolates it from its peers. The leader is pinned to a
// SURVIVOR (rmq1) via a shuffle-immune single-address declare so the queue keeps a
// stable leader through the partition and this campaign isolates the pause_minority +
// zero-loss-via-majority property from leader re-election (that is T166c's campaign).
//
// goleak: after the partition the sockets that had landed on rmq2 rotate (shuffled
// cursor) to the surviving rmq0/rmq1 so Close drains cleanly; the partition is healed
// in t.Cleanup (tolerant of an already-healed state) and the cluster waited whole
// again before the durable queue is deleted.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
)

// awaitQueueOnline polls the management API until the quorum queue reports exactly
// `want` online members (and still spans all 3 as members), or the timeout elapses.
// Partition detection is net-tick-bound (a few seconds to tens of seconds), and the
// API may briefly report a stale set mid-transition, so a poll — not a single read —
// is required. Returns the satisfying state for logging.
func awaitQueueOnline(t *testing.T, queue string, want int, timeout time.Duration) amqptest.QuorumQueueState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last amqptest.QuorumQueueState
	for time.Now().Before(deadline) {
		last = amqptest.QuorumLeader(t, queue)
		if len(last.Members) == 3 && len(last.Online) == want {
			return last
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("quorum queue %q did not reach online=%d (members=3) within %s (last members=%v online=%v)",
		queue, want, timeout, last.Members, last.Online)
	return last // unreachable
}

// TestClusterPartitionUnderLoad_PauseMinorityZeroLoss_cluster streams confirmed load
// to a quorum queue, network-partitions a follower mid-stream, and asserts the
// minority is isolated (pause_minority), the majority keeps confirming with only
// classifiable errors, and every confirmed publish survives to be consumed after heal.
func TestClusterPartitionUnderLoad_PauseMinorityZeroLoss_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const (
		queue         = "test.cluster.partition"
		victimService = "rmq2" // the minority follower we isolate; rmq0 (mgmt) + rmq1 (leader) survive
		majorityCount = 2      // members still online to the quorum during the partition
	)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Readiness gate: an earlier cluster test may have killed/restarted a node;
	// declaring the quorum queue while a member is momentarily absent would scatter
	// leader placement. Wait for all three running first.
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	// Clean slate; the killed-node-style restore here is the partition heal, registered
	// just before the cut so Cleanup (LIFO) heals the cluster whole before this delete.
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })

	// Pin the quorum leader to rmq1 — a SURVIVOR — via a shuffle-immune single-address
	// declare, so the queue keeps a stable leader through the partition (no re-election
	// noise) and the partitioned rmq2 is only ever a follower.
	declareQuorumLeaderOnNode(ctx, t, nodes[1], queue)

	// Load connection across all three nodes; the per-socket shuffle spreads the pool so
	// some sockets land on rmq2 and exercise client failover off the partitioned node.
	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{nodes[1], nodes[2], nodes[0]}),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(2),
		warren.WithChannelPoolSize(8),
		warren.WithReconnectBackoff(clusterFastBackoff),
		warren.WithDialer(boundedClusterDialer),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Durable quorum queue; AttachTo redeclares it on every reconnect barrier the
	// partition triggers, so the consumer's re-subscribe always finds the queue present.
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	// Precondition: a quorum queue spanning all three members, leader on the survivor.
	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes")
	require.Len(t, before.Online, 3, "all three members must be online before the partition")
	require.NotEqual(t, "rabbit@"+victimService, before.Leader,
		"leader must be pinned to a survivor, not the node we are about to partition")

	var (
		mu           sync.Mutex
		publishedSet = make(map[string]struct{})
		consumedSet  = make(map[string]struct{})
		unexpected   []error
	)

	// — Consumer drains into consumedSet (deduped by MessageID) ————————————————
	consumer, err := warren.ConsumerFor[clusterFailoverMsg](conn).
		Queue(queue).
		Concurrency(4).
		Prefetch(64).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- consumer.ConsumeRaw(consumeCtx, func(_ context.Context, d *warren.Delivery[clusterFailoverMsg]) error {
			mu.Lock()
			consumedSet[d.MessageID()] = struct{}{}
			mu.Unlock()
			return d.Ack()
		})
	}()

	// — Publisher streams confirmed messages continuously until told to stop ————
	pub, err := warren.PublisherFor[clusterFailoverMsg](conn).
		RoutingKey(queue). // default exchange "" → route straight to the quorum queue
		ConfirmTimeout(20 * time.Second).
		PublishRetry(clusterPublishRetry).
		Build()
	require.NoError(t, err)

	pubDone := make(chan struct{})
	var (
		pubWG sync.WaitGroup
		seq   int // owned solely by the publisher goroutine
	)
	pubWG.Add(1)
	go func() {
		defer pubWG.Done()
		for {
			select {
			case <-pubDone:
				return
			case <-ctx.Done():
				return
			default:
			}
			id := fmt.Sprintf("partition-%d", seq)
			switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: id}); {
			case perr == nil:
				mu.Lock()
				publishedSet[id] = struct{}{}
				mu.Unlock()
			case isTolerableFailoverErr(perr):
				// The partition tripped the reconnect barrier / confirm gap on a socket
				// that had landed on the isolated node; at-least-once permits dropping
				// this id — it is never recorded, so it is never asserted durable.
			default:
				mu.Lock()
				unexpected = append(unexpected, fmt.Errorf("seq=%d: %w", seq, perr))
				mu.Unlock()
			}
			seq++
		}
	}()

	confirmedCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(publishedSet)
	}

	// Warm up: confirm a batch BEFORE the partition so pre-partition streaming is proven.
	const warmupFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= warmupFloor },
		60*time.Second, 50*time.Millisecond,
		"publisher must confirm a warmup batch before the partition")
	prePartition := confirmedCount()

	// — Inject the partition: isolate rmq2 from the cluster network mid-stream ————
	// Register the heal FIRST (LIFO: runs before the queue-delete cleanup), so the
	// cluster is whole again when the durable queue is removed. The heal is tolerant of
	// an already-healed state, since the test body heals explicitly below.
	t.Cleanup(func() {
		amqptest.HealPartition(t, victimService)
		amqptest.WaitClusterReady(t, len(nodes), 120*time.Second)
	})
	amqptest.PartitionNode(t, victimService)

	// pause_minority: the isolated follower drops out of the quorum's online set while
	// the majority {rmq0, rmq1} keeps quorum. This is the broker-side proof a single
	// node cannot give.
	isolated := awaitQueueOnline(t, queue, majorityCount, 120*time.Second)
	assert.Len(t, isolated.Members, 3, "the partitioned member is still a member, just offline")
	assert.NotContains(t, isolated.Online, "rabbit@"+victimService,
		"the partitioned node must drop out of the quorum's online set")
	running, total := amqptest.RunningNodes(t)
	t.Logf("partition active: queue online=%v (of members=%v); cluster running=%d/%d",
		isolated.Online, isolated.Members, running, total)

	// No silent stall: the publisher must keep CONFIRMING through the partition (the
	// majority stays available). A publisher wedged on the cut would never get here.
	const postPartitionFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= prePartition+postPartitionFloor },
		120*time.Second, 100*time.Millisecond,
		"publisher must keep confirming during the partition (majority available, no silent stall)")

	// — Heal: reconnect rmq2; it rejoins and the quorum returns to full membership ——
	amqptest.HealPartition(t, victimService)
	amqptest.WaitClusterReady(t, len(nodes), 120*time.Second)
	healed := awaitQueueOnline(t, queue, 3, 120*time.Second)
	t.Logf("partition healed: queue online=%v", healed.Online)

	// Stop the load and join the publisher before the recovery gate + drain.
	close(pubDone)
	pubWG.Wait()

	// Recovery sentinel: prove the CONSUMER pipeline is live end to end (re-open channel
	// → redeclare → re-issue basic.consume), not merely the publisher socket.
	recoveryID := fmt.Sprintf("partition-recovery-%d", seq)
	require.NoError(t, pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: recoveryID}),
		"a publish must succeed once the partition is healed")
	mu.Lock()
	publishedSet[recoveryID] = struct{}{}
	mu.Unlock()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := consumedSet[recoveryID]
		return ok
	}, 30*time.Second, 100*time.Millisecond,
		"consumer must re-subscribe and deliver after the heal (recovery sentinel consumed)")

	{
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pub.Close(closeCtx)
		c()
	}

	// Drain: every confirmed publish must eventually be consumed (zero loss).
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(publishedSet) > 0 && len(lossByMessageID(publishedSet, consumedSet)) == 0
	}, 90*time.Second, 250*time.Millisecond, "all confirmed publishes must be consumed across the partition (zero loss)")

	cancelConsume()
	require.NoError(t, filterClusterCanceled(<-consumeErr), "consumer must stop cleanly")

	mu.Lock()
	lost := lossByMessageID(publishedSet, consumedSet)
	nPub, nCon := len(publishedSet), len(consumedSet)
	surface := append([]error(nil), unexpected...)
	mu.Unlock()

	require.Empty(t, surface,
		"publishes failed with errors the partition does not explain (a silent-stall/unclassifiable defect): %v", surface)
	require.Empty(t, lost,
		"zero message loss across the partition: %d confirmed, %d consumed-distinct, lost=%v", nPub, nCon, lost)
	t.Logf("cluster partition-under-load zero-loss: confirmed=%d consumed-distinct=%d (duplicates tolerated), "+
		"minority %s isolated then rejoined", nPub, nCon, victimService)
}
