//go:build cluster

package warren_test

// Rolling restart / rolling upgrade under load — continuity across sequential
// single-node restarts on a (optionally mixed-version) cluster (Phase 9.5 / T166g;
// SPEC §1 at-least-once + Lens-13 LT-05-cluster).
//
// A publisher streams confirmed messages to a QUORUM queue under continuous load
// while the harness restarts EACH node, one at a time, waiting for the cluster to be
// whole again between restarts; a consumer drains concurrently. This is the shape of
// a rolling broker upgrade: at every moment two of three members are up, so the
// quorum queue keeps a majority and stays available. The contract under test:
//   - continuity: the publisher keeps CONFIRMING across every node restart (a
//     per-restart floor that a publisher wedged on a restart could never satisfy);
//   - consumers resubscribe: after the restarts a recovery sentinel flows end to end
//     (re-open channel → redeclare → re-issue basic.consume on a surviving node), and
//     zero-loss inherently requires the consumer to have re-subscribed after each
//     node it was pinned to went down;
//   - zero loss: every confirmed publish is eventually consumed, deduped by MessageID
//     exactly as TV-09 mandates (the reconnect barrier and PublishRetry produce
//     duplicates by design; a genuinely dropped message is caught by lossByMessageID);
//   - only classifiable errors surface — any publish error that is not a known warren
//     sentinel fails the test.
//
// Mixed-version: the cluster is homogeneous 3.13.7 by default, so this campaign always
// asserts rolling-restart-under-load continuity. Set WARREN_RMQ2_IMAGE to a
// FEATURE-FLAG-COMPATIBLE different version before `make cluster-up` (e.g. another 3.13
// patch like rabbitmq:3.13.6-management) and rmq2 runs a different RabbitMQ version —
// the genuinely mixed-version cluster a rolling upgrade within a feature-flag
// generation transits (validated live with 3.13.7 + 3.13.6). The campaign reads each
// member's version up front and LOGS whether it ran homogeneous or mixed, so a
// homogeneous run is reported honestly rather than passing as if it had exercised the
// cross-version path. The continuity assertions are identical either way; what changes
// is the version skew they run against.
//
// A 3.13→4.x MAJOR jump is deliberately NOT exercised here: a fresh 4.x node refuses to
// cluster with 3.13 peers (`incompatible_feature_flags`, confirmed live) and runs
// standalone. A real major-version rolling upgrade is an IN-PLACE, data-preserving
// image swap of an existing member — which needs persistent volumes the lane does not
// yet provision (deferred to LATER-88), not the fresh-boot peer discovery this compose
// uses.
//
// Why a cluster: "the queue stays available while a member restarts" needs a majority
// to survive the restart — unobservable below three nodes, where restarting the sole
// broker takes the queue down entirely.
//
// goleak: each restart is graceful (docker restart → clean leave + rejoin), so sockets
// on the restarting node rotate to survivors and Close drains cleanly; RestartNode is
// in-place, so no t.Cleanup node-restore is needed (the rolling loop ends with every
// node up and the cluster waited whole).

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
)

// distinctVersionStrings returns the sorted set of distinct RabbitMQ versions across a
// node→version map, so the campaign can report homogeneous (len 1) vs mixed (len ≥2).
// Empty versions (a member mid-restart with no rabbit app yet) are dropped.
func distinctVersionStrings(versions map[string]string) []string {
	set := make(map[string]struct{}, len(versions))
	for _, v := range versions {
		if v != "" {
			set[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// TestClusterRollingUpgradeUnderLoad_Continuity_cluster streams confirmed load to a
// quorum queue, restarts every node one at a time, and asserts the load keeps
// confirming across each restart, the consumer re-subscribes, and every confirmed
// publish survives to be consumed (zero loss) — on a homogeneous or, when
// WARREN_RMQ2_IMAGE pins a 4.x member, a genuinely mixed-version cluster.
func TestClusterRollingUpgradeUnderLoad_Continuity_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const queue = "test.cluster.rolling.upgrade"
	// Restart order: the management-API node (rmq0) LAST, so the management reads the
	// readiness gate makes between restarts stay available for the first two; while rmq0
	// itself restarts, WaitClusterReady simply polls through its connection-refused gap
	// until rmq0 reboots and all three report running again.
	restartOrder := []string{"rmq2", "rmq1", "rmq0"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	// Version skew up front: report homogeneous vs mixed honestly.
	versions := amqptest.NodeVersions(t)
	distinct := distinctVersionStrings(versions)
	if len(distinct) >= 2 {
		t.Logf("MIXED-VERSION cluster in effect (%v) — asserting rolling-restart continuity across versions; node map=%v",
			distinct, versions)
	} else {
		t.Logf("homogeneous cluster (%v) — rolling-restart-under-load continuity still asserted; "+
			"set WARREN_RMQ2_IMAGE to a feature-flag-compatible different version (e.g. another 3.13 patch) "+
			"before `make cluster-up` to exercise the mixed-version path; node map=%v",
			distinct, versions)
	}

	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })

	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{nodes[0], nodes[1], nodes[2]}),
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

	// Durable quorum queue; AttachTo redeclares it on every reconnect barrier a restart
	// triggers, so the consumer's re-subscribe always finds the queue present. No leader
	// pin: the rolling restart migrates the leader anyway, and the majority re-elects.
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes")

	var (
		mu           sync.Mutex
		publishedSet = make(map[string]struct{})
		consumedSet  = make(map[string]struct{})
		deliveries   int // total handler invocations, including redeliveries
		redelivered  int // invocations the broker flagged as a redelivery
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
			id := fmt.Sprintf("rolling-upgrade-%d", seq)
			switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: id}); {
			case perr == nil:
				mu.Lock()
				publishedSet[id] = struct{}{}
				mu.Unlock()
			case isTolerableFailoverErr(perr):
				// A node restart tripped the reconnect barrier / confirm gap on a socket
				// that had landed on it; at-least-once permits dropping this id — it is
				// never recorded, so it is never asserted durable.
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

	// Warm up: confirm a batch BEFORE the first restart so pre-upgrade streaming is proven.
	const warmupFloor = 15
	require.Eventually(t, func() bool { return confirmedCount() >= warmupFloor },
		60*time.Second, 50*time.Millisecond,
		"publisher must confirm a warmup batch before the rolling restart")

	// — The rolling restart: each node restarted in turn, cluster whole between each ——
	const perRestartFloor = 15 // confirms that must land AFTER each node rejoins (continuity, non-vacuous)
	for i, svc := range restartOrder {
		pre := confirmedCount()
		amqptest.RestartNode(t, svc)
		// Wait for the restarted node to reboot and rejoin before the next restart, so
		// at most one member is ever down (a true rolling upgrade keeps a majority).
		amqptest.WaitClusterReady(t, len(nodes), 120*time.Second)
		require.Eventually(t, func() bool { return confirmedCount() >= pre+perRestartFloor },
			120*time.Second, 100*time.Millisecond,
			"publisher must keep confirming after restarting %s (step %d/%d): continuity broke",
			svc, i+1, len(restartOrder))
		t.Logf("rolling restart %d/%d done (%s); confirmed=%d", i+1, len(restartOrder), svc, confirmedCount())
	}

	// Stop the load and join the publisher before the recovery gate + drain.
	close(pubDone)
	pubWG.Wait()

	// Recovery: every member is back and the queue spans all three again.
	require.NoError(t, conn.Health(ctx), "Health must pass after the rolling restart")
	after := amqptest.QuorumLeader(t, queue)
	require.Len(t, after.Members, 3, "quorum queue must span all three members after the rolling restart")
	require.Len(t, after.Online, 3, "all three members must be online again after the rolling restart")

	// Recovery sentinel: prove the CONSUMER pipeline is live end to end (consumer
	// re-subscribed on a surviving node), not merely the publisher socket.
	recoveryID := fmt.Sprintf("rolling-upgrade-recovery-%d", seq)
	require.NoError(t, pub.Publish(ctx, warren.Message[clusterFailoverMsg]{Body: &clusterFailoverMsg{Seq: seq}, MessageID: recoveryID}),
		"a publish must succeed once the rolling restart is complete")
	mu.Lock()
	publishedSet[recoveryID] = struct{}{}
	mu.Unlock()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := consumedSet[recoveryID]
		return ok
	}, 30*time.Second, 100*time.Millisecond,
		"consumer must re-subscribe and deliver after the rolling restart (recovery sentinel consumed)")

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
	}, 90*time.Second, 250*time.Millisecond, "all confirmed publishes must be consumed across the rolling restart (zero loss)")

	cancelConsume()
	require.NoError(t, filterClusterCanceled(<-consumeErr), "consumer must stop cleanly")

	mu.Lock()
	lost := lossByMessageID(publishedSet, consumedSet)
	nPub, nCon := len(publishedSet), len(consumedSet)
	gotDeliveries, gotRedelivered := deliveries, redelivered
	surface := append([]error(nil), unexpected...)
	mu.Unlock()

	require.Empty(t, surface,
		"publishes failed with errors the rolling restart does not explain (an unclassifiable defect): %v", surface)
	require.Empty(t, lost,
		"zero message loss across the rolling restart: %d confirmed, %d consumed-distinct, lost=%v", nPub, nCon, lost)
	t.Logf("cluster rolling-restart continuity: confirmed=%d consumed-distinct=%d deliveries=%d redelivered=%d "+
		"(duplicates tolerated + deduped) across %d node restarts; versions=%v",
		nPub, nCon, gotDeliveries, gotRedelivered, len(restartOrder), distinct)
}
