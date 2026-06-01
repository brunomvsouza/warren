//go:build cluster

package warren_test

// Quorum leader failover under sustained load — the headline cluster claim
// (Phase 9.5 / T166c; SPEC §1 at-least-once + Lens-10 TV-09 zero-loss).
//
// A publisher streams confirmed messages to a QUORUM queue under continuous load
// while the harness KILLS the queue's Raft leader mid-stream (SIGKILL — a real
// crash, not a graceful stop); a consumer drains concurrently. The contract under
// test is the §1 headline carried onto a real multi-node cluster: at-least-once
// with ZERO LOSS across a Raft re-election. Loss is measured exactly as TV-09
// mandates — the published-set minus the consumed-set, deduplicated by MessageID
// (reusing lossByMessageID, the same accounting the T45 reconnect chaos test uses
// and that chaos_reconnect_loss_test.go self-tests with an injected drop) — so the
// duplicates the reconnect barrier and PublishRetry produce by design are
// tolerated while a genuinely dropped message is caught.
//
// Why this needs a cluster: a single-node broker cannot elect a NEW leader, so the
// migration this asserts (and the durability of confirmed publishes ACROSS that
// migration) is unobservable below three nodes. Leader placement is made
// deterministic the same way the T166b control test does it: rabbitmq.conf pins
// queue_leader_locator=client-local and a SINGLE-ADDRESS declare connection
// (declareQuorumLeaderOnNode) pins the quorum leader to rmq1 — the node we then
// kill. That single-address declare is immune to the per-socket WithAddrs shuffle
// (T66), which would otherwise scatter the declaring socket and could place the
// leader on the management node rmq0. rmq0+rmq2 keep a majority and re-elect; the
// surviving rmq0's management API (WARREN_CLUSTER_MGMT) surfaces the new leader.
//
// goleak: after the kill the load connection's sockets that had landed on rmq1
// rotate (shuffled cursor) to rmq0/rmq2 (both alive) so Close drains cleanly; the
// killed node is restarted in t.Cleanup to restore the cluster for later tests.

import (
	"context"
	"errors"
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

type clusterFailoverMsg struct {
	Seq int `json:"seq"`
}

// clusterFastBackoff keeps the reconnect retry tight so the rmq1-pinned
// connections rotate to a surviving node promptly after the leader is killed,
// instead of paying the 1 s default backoff while the cursor advances. Jitter is
// left at the default (full) so the pool's connections re-dial spread out rather
// than in lockstep — exactly the herd behaviour the cluster claim must survive.
var clusterFastBackoff = warren.RetryPolicy{
	Min: 20 * time.Millisecond,
	Max: 200 * time.Millisecond,
}

// clusterPublishRetry retries a publish that hits the reconnect barrier or a
// transient confirm gap during the Raft re-election, so a confirmed publish stays
// durable across the leader's death. Jitter is left at the default (full); the
// bounded [Min, Max] keeps the retry prompt while spreading it like production.
var clusterPublishRetry = warren.RetryPolicy{
	Min:     20 * time.Millisecond,
	Max:     500 * time.Millisecond,
	Factor:  2.0,
	Retries: 8,
}

// isTolerableFailoverErr reports whether a publish error is an expected
// consequence of killing the leader mid-stream — the reconnect barrier swallowing
// a retried publish, the channel closing under it (ErrChannelClosed, or a 504
// channel-error on a channel that went not-open mid-publish), or a confirm timing
// out while no leader is yet elected — as opposed to a defect (pool exhaustion,
// unroutable, nack, a validation bug) the zero-loss test must surface rather than
// silently drop as merely "fewer publishes". ErrChannelError (504) is transient
// and retried by PublishRetry, so it only surfaces when a publish exhausts its
// retry budget while the channel stays not-open across the re-election — never
// recorded as confirmed, so never asserted durable.
func isTolerableFailoverErr(err error) bool {
	return errors.Is(err, warren.ErrReconnecting) ||
		errors.Is(err, warren.ErrConfirmTimeout) ||
		errors.Is(err, warren.ErrChannelClosed) ||
		errors.Is(err, warren.ErrChannelError)
}

// filterClusterCanceled drops the benign context.Canceled a consumer returns when
// it is stopped on purpose, so it is not recorded as a surface error. (A
// cluster-lane local: the integration lane's filterCanceled lives behind the
// `integration` tag and is not compiled here.)
func filterClusterCanceled(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// TestClusterQuorumFailover_ZeroLoss_cluster streams confirmed messages to a
// quorum queue, kills the Raft leader's node mid-stream, and asserts every
// confirmed publish is eventually consumed (deduped by MessageID) across the
// re-election — with substantial confirms BOTH before the kill and after the new
// leader is elected, so a publisher that wedged on the election cannot pass.
func TestClusterQuorumFailover_ZeroLoss_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const queue = "test.cluster.quorum.failover"

	// Full WithAddrs list for the load connection; leader placement no longer
	// depends on its dial order (the single-address declare below pins the leader to
	// rmq1). rmq0 — the management-API node — is kept last only out of convention.
	addrs := []string{nodes[1], nodes[2], nodes[0]}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Readiness gate: an EARLIER cluster test may have killed and restarted rmq1
	// (docker start returns before RabbitMQ has rebooted and rejoined). Declaring
	// the quorum queue while rmq1 is momentarily absent would place the leader on a
	// surviving node instead, breaking the rmq1-pinned precondition below. Wait for
	// all three members to be running first, so leader placement is deterministic
	// regardless of test execution order.
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	// Clean slate: a prior run may have left the durable quorum queue behind with a
	// stale leader; delete it before declaring so the leader is freshly placed. The
	// killed node's restore cleanup is registered later, once we know which node the
	// leader landed on (and Cleanup is LIFO, so it will run before this delete —
	// making the cluster whole again before the durable queue is removed).
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })

	// Pin the quorum leader to rmq1 via a single-address declare connection —
	// shuffle-immune (T66), so the leader is deterministically on a killable
	// non-management node before the load connection (whose sockets shuffle across
	// the cluster) ever opens.
	declareQuorumLeaderOnNode(ctx, t, nodes[1], queue)

	conn, err := warren.Dial(ctx,
		warren.WithAddrs(addrs),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(2),
		warren.WithChannelPoolSize(8),
		warren.WithReconnectBackoff(clusterFastBackoff),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Durable quorum queue, default-exchange routed (routing key == queue name), so
	// there is no exchange/binding to clean up. Declare here is an idempotent
	// redeclare of the queue placed above (identical args do not move the leader);
	// AttachTo redeclares it on every reconnect barrier after the leader node dies
	// and the pool rotates.
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	// Precondition: a quorum queue spanning all three members whose leader is pinned
	// to rmq1 (a killable non-management node). The Contains check stays robust to an
	// earlier test's restart timing while the single-address declare makes rmq1 the
	// deterministic placement.
	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes")
	require.Contains(t, clusterKillableLeaders, before.Leader,
		"leader must land on a killable non-management node (pinned to rmq1 via the single-address declare)")
	originalLeader := before.Leader
	killService := amqptest.NodeService(originalLeader) // "rmq1" (pinned) — "rmq2" only if an external actor moved it

	// Now that we know which node will die, register its restore. Cleanup is LIFO, so
	// this runs before the queue-delete registered above — the cluster is whole again
	// when the durable queue is removed.
	t.Cleanup(func() { amqptest.StartNode(t, killService) })

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
				// A failed require before close(pubDone) cancels ctx as it unwinds;
				// exit cleanly here so the publisher never spins on a canceled context
				// (which would leak into the deferred goleak check and bury the real
				// failure under leak noise).
				return
			default:
			}
			id := fmt.Sprintf("cluster-failover-%d", seq)
			// Pin MessageID so the published set is known exactly; on a confirmed
			// (nil) return the broker durably holds it and it MUST be consumed.
			switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: id}); {
			case perr == nil:
				mu.Lock()
				publishedSet[id] = struct{}{}
				mu.Unlock()
			case isTolerableFailoverErr(perr):
				// The leader's death tripped the reconnect barrier / confirm gap;
				// at-least-once permits dropping this id — it is never recorded, so it
				// is never asserted durable.
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

	// Warm up: confirm a batch BEFORE the kill so pre-failover streaming is proven.
	const warmupFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= warmupFloor },
		60*time.Second, 50*time.Millisecond,
		"publisher must confirm a warmup batch before the leader is killed")
	preKill := confirmedCount()

	// Kill the leader's node — a real crash, not a graceful stop — mid-stream.
	amqptest.KillNode(t, killService)

	// The surviving majority (the two nodes we did NOT kill, one of which is the
	// management-API node rmq0) must re-elect; the management API reports the new
	// leader. This is the migration a single-node broker cannot do.
	after := awaitNewLeaderCluster(t, queue, originalLeader, 60*time.Second)
	assert.NotEqual(t, originalLeader, after.Leader, "a new leader must be elected after the old one is killed")
	assert.Contains(t, survivingClusterLeaders(originalLeader), after.Leader,
		"the new leader must be one of the surviving nodes")

	// Non-vacuity: require a substantial batch confirmed AFTER the new leader is
	// elected. A publisher that wedged on the election would satisfy a pre-kill
	// floor yet never get here — this is the proof it survived the re-election.
	const postElectionFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= preKill+postElectionFloor },
		90*time.Second, 100*time.Millisecond,
		"publisher must resume confirming after the new leader is elected (survived the election)")

	// Stop the load and join the publisher before the recovery gate + drain.
	close(pubDone)
	pubWG.Wait()

	// Recovery gate: prove the CONSUMER pipeline is live again end to end, not merely
	// the publisher socket — publish a sentinel through the recovered connection and
	// require it consumed, which exercises publish AND consumer re-subscribe (re-open
	// channel → redeclare → re-issue basic.consume on the surviving node).
	recoveryID := fmt.Sprintf("cluster-failover-recovery-%d", seq)
	require.NoError(t, pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: recoveryID}),
		"a publish must succeed once the new leader is elected")
	mu.Lock()
	publishedSet[recoveryID] = struct{}{}
	mu.Unlock()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := consumedSet[recoveryID]
		return ok
	}, 30*time.Second, 100*time.Millisecond,
		"consumer must re-subscribe and deliver after the failover (recovery sentinel consumed)")

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
	}, 90*time.Second, 250*time.Millisecond, "all confirmed publishes must be consumed across the failover (zero loss)")

	cancelConsume()
	require.NoError(t, filterClusterCanceled(<-consumeErr), "consumer must stop cleanly")

	mu.Lock()
	lost := lossByMessageID(publishedSet, consumedSet)
	nPub, nCon := len(publishedSet), len(consumedSet)
	surface := append([]error(nil), unexpected...)
	mu.Unlock()

	require.Empty(t, surface,
		"publishes failed with errors the leader kill does not explain: %v", surface)
	require.Empty(t, lost,
		"zero message loss across quorum failover: %d confirmed, %d consumed-distinct, lost=%v", nPub, nCon, lost)
	t.Logf("cluster quorum failover zero-loss: confirmed=%d consumed-distinct=%d (duplicates tolerated), leader %s -> %s",
		nPub, nCon, originalLeader, after.Leader)
}
