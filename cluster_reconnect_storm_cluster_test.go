//go:build cluster

package warren_test

// Reconnect-storm across all nodes under sustained load (Phase 9.5 / T166f; SPEC §1
// at-least-once + Lens-13 LT-05-cluster, consumes the T66/T166e per-connection
// shuffle).
//
// A publisher streams confirmed messages to a quorum queue and a consumer drains
// concurrently while the harness fires repeated ForceReconnect waves — dropping
// EVERY socket in the pool and forcing the whole multi-connection pool to re-dial
// at once. This is the recovery-storm shape DS-10/SRE-04 warn about: when many
// sockets reconnect simultaneously they must NOT all stampede onto addrs[0]. The
// campaign asserts three things across the storm:
//   - no addr[0] stampede — after each wave the genuinely-replaced pool still spans
//     ≥2 nodes (the per-socket shuffle re-spreads the herd; a shared dial cursor
//     would march every socket onto the same node);
//   - recovery — once the storm stops, every socket is reconnected, Health is green,
//     and a fresh sentinel publish flows end to end through the re-subscribed consumer;
//   - zero loss — every confirmed publish is eventually consumed, deduped by
//     MessageID exactly as TV-09 mandates (the reconnect barrier and PublishRetry
//     produce duplicates by design; a genuinely dropped message is caught).
//
// The shuffle seed is PINNED (WithAddrShuffleSeedForTest) so the spread is
// deterministic — the same ≥2-node distribution on every run, no probabilistic
// flake — mirroring the rotation campaign (T166e).
//
// Why this needs a cluster: "the reconnecting pool re-spreads across nodes rather
// than stampeding addrs[0]" is unobservable below three nodes — with one broker
// every socket reconnects to the same place. ForceReconnect with every node UP (no
// kill) isolates the stampede property from any failover: there is no
// preferentially-available node to funnel the herd, so a clean spread is the only
// correct outcome.

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

// stormShuffleSeed pins the per-connection address shuffle (T66) so the storm's
// post-reconnect spread is DETERMINISTIC. See rotationShuffleSeed for the full
// staleness contract (re-pick the seed, not the shuffle, if a change to
// perConnSeed / role strings / pool size / node mapping funnels it onto one node).
// A distinct value, verified to spread the same 3+3 pool.
const stormShuffleSeed int64 = 0x3C4D5E6F

// TestClusterReconnectStorm_ZeroLossNoStampede_cluster streams confirmed load to a
// quorum queue, fires repeated ForceReconnect waves across the whole pool, and
// asserts no addr[0] stampede on each wave, full recovery afterwards, and zero
// message loss across the storm.
func TestClusterReconnectStorm_ZeroLossNoStampede_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	const (
		queue               = "test.cluster.reconnect.storm"
		prefix              = "stormcamp"
		wantConns           = 6                // 3 publisher + 3 consumer sockets
		stormWave           = 3                // ForceReconnect cycles
		stormConfirmTimeout = 20 * time.Second // publish-confirm cap during the storm
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })

	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{nodes[0], nodes[1], nodes[2]}),
		warren.WithConnectionName(prefix),
		warren.WithPublisherConnections(3),
		warren.WithConsumerConnections(3),
		warren.WithChannelPoolSize(8),
		warren.WithReconnectBackoff(clusterFastBackoff),
		warren.WithDialer(boundedClusterDialer),
		warren.WithAddrShuffleSeedForTest(stormShuffleSeed),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Durable quorum queue; AttachTo redeclares it on every reconnect barrier the
	// storm triggers, so the consumer's re-subscribe always finds the queue present.
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	var (
		mu           sync.Mutex
		publishedSet = make(map[string]struct{})
		consumedSet  = make(map[string]struct{})
		deliveries   int // total handler invocations, including redeliveries
		redelivered  int // invocations the broker flagged as a redelivery
		unexpected   []error
	)

	// — Consumer drains into consumedSet (deduped by MessageID) ————————————————
	// Prefetch(64) keeps up to 64 delivered-but-unacked messages in flight per
	// consumer socket; when a storm wave drops that socket the broker requeues every
	// unacked one and redelivers it (Redelivered() == true) after the consumer
	// re-subscribes. Counting deliveries vs redeliveries lets the campaign prove the
	// prefetch↔redelivery interplay was actually exercised, and the zero-loss gate
	// below proves the consumer deduped those redeliveries (at-least-once, TV-09).
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
			deliveries++
			if d.Redelivered() {
				redelivered++
			}
			consumedSet[d.MessageID()] = struct{}{}
			mu.Unlock()
			return d.Ack()
		})
	}()

	// — Publisher streams confirmed messages continuously until told to stop ————
	pub, err := warren.PublisherFor[clusterFailoverMsg](conn).
		RoutingKey(queue).
		ConfirmTimeout(stormConfirmTimeout).
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
			id := fmt.Sprintf("reconnect-storm-%d", seq)
			switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: id}); {
			case perr == nil:
				mu.Lock()
				publishedSet[id] = struct{}{}
				mu.Unlock()
			case isTolerableFailoverErr(perr):
				// A ForceReconnect wave tripped the reconnect barrier / confirm gap;
				// at-least-once permits dropping this id — it is never recorded, so it is
				// never asserted durable.
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

	// Warm up: confirm a batch BEFORE the storm so pre-storm streaming is proven.
	const warmupFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= warmupFloor },
		60*time.Second, 50*time.Millisecond,
		"publisher must confirm a warmup batch before the storm")

	// Initial spread (deterministic via the pinned seed): the six sockets land across
	// ≥2 nodes, not all on addrs[0].
	round := awaitCleanPool(t, prefix, wantConns, 30*time.Second)
	require.GreaterOrEqual(t, distinctNodes(round), 2,
		"initial pool must spread across ≥2 nodes, not stampede addrs[0] (got %v)", nodesByName(round))
	t.Logf("storm wave 0 (initial) spread: %v", nodesByName(round))

	// — The storm: repeated ForceReconnect waves under continuous load —————————
	// Each wave drops every socket; we wait until the pool is fully REPLACED (every
	// old TCP socket gone — ForceReconnect is async, so reading too early would
	// re-observe the pre-wave pool) before asserting the genuinely-new pool still
	// spans ≥2 nodes. No node is preferentially up to funnel the reconnects, so a
	// clean re-spread is the only correct outcome.
	for wave := 1; wave <= stormWave; wave++ {
		prev := brokerNameSet(round)
		require.NoError(t, conn.ForceReconnect(), "ForceReconnect wave %d", wave)
		round = awaitReplacedPool(t, prefix, prev, wantConns, 60*time.Second)
		assert.GreaterOrEqualf(t, distinctNodes(round), 2,
			"wave %d reconnections must re-spread across ≥2 nodes, not stampede addrs[0] (got %v)",
			wave, nodesByName(round))
		t.Logf("storm wave %d spread: %v", wave, nodesByName(round))
	}

	// Non-vacuity: the publisher must keep confirming AFTER the storm — a publisher
	// that wedged on a reconnect would have satisfied the warmup floor yet stalled here.
	const postStormFloor = 15
	preStop := confirmedCount()
	require.Eventually(t, func() bool { return confirmedCount() >= preStop+postStormFloor },
		90*time.Second, 100*time.Millisecond,
		"publisher must resume confirming after the storm (survived every reconnect wave)")

	// Stop the load and join the publisher before the recovery gate + drain.
	close(pubDone)
	pubWG.Wait()

	// Recovery: every socket reconnected and Health is green once the storm is over.
	require.NoError(t, conn.Health(ctx), "Health must pass after the reconnect storm")
	recovered := awaitCleanPool(t, prefix, wantConns, 60*time.Second)
	require.Len(t, recovered, wantConns,
		"every socket must reconnect after the storm (no socket stranded)")

	// Recovery sentinel: prove the CONSUMER pipeline is live end to end (re-open
	// channel → redeclare → re-issue basic.consume), not merely the publisher socket.
	recoveryID := fmt.Sprintf("reconnect-storm-recovery-%d", seq)
	require.NoError(t, pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: recoveryID}),
		"a publish must succeed once the storm is over")
	mu.Lock()
	publishedSet[recoveryID] = struct{}{}
	mu.Unlock()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := consumedSet[recoveryID]
		return ok
	}, 30*time.Second, 100*time.Millisecond,
		"consumer must re-subscribe and deliver after the storm (recovery sentinel consumed)")

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
	}, 90*time.Second, 250*time.Millisecond, "all confirmed publishes must be consumed across the storm (zero loss)")

	cancelConsume()
	require.NoError(t, filterClusterCanceled(<-consumeErr), "consumer must stop cleanly")

	mu.Lock()
	lost := lossByMessageID(publishedSet, consumedSet)
	nPub, nCon := len(publishedSet), len(consumedSet)
	gotDeliveries, gotRedelivered := deliveries, redelivered
	surface := append([]error(nil), unexpected...)
	mu.Unlock()

	require.Empty(t, surface,
		"publishes failed with errors the reconnect storm does not explain: %v", surface)
	require.Empty(t, lost,
		"zero message loss across the reconnect storm: %d confirmed, %d consumed-distinct, lost=%v", nPub, nCon, lost)
	// Prefetch↔redelivery interplay: with 64 unacked in flight per socket, dropping
	// every socket on each of the storm's waves must requeue and redeliver some of
	// them — so at least one delivery carries the broker's redelivered flag. Zero
	// redeliveries would mean prefetch never held an unacked message across a wave,
	// leaving the dedup path (which the zero-loss gate above relies on) unexercised.
	require.GreaterOrEqual(t, gotRedelivered, 1,
		"prefetch must hold unacked messages that are redelivered across the storm "+
			"(deliveries=%d redelivered=%d distinct=%d); zero redeliveries leaves the dedup path unexercised",
		gotDeliveries, gotRedelivered, nCon)
	t.Logf("reconnect-storm zero-loss: confirmed=%d consumed-distinct=%d deliveries=%d redelivered=%d "+
		"(duplicates tolerated + deduped) across %d waves",
		nPub, nCon, gotDeliveries, gotRedelivered, stormWave)
}
