//go:build cluster

package warren_test

// SingleActiveConsumer failover with a MULTI-MESSAGE in-flight set — Phase 9.5 /
// T166i (closes LATER-87; sibling of T166d).
//
// T166d proves SAC ordering survives a real node kill, but its active consumer runs
// Prefetch(1), so at most ONE message is unacked at the handoff. The production
// failure mode the "publish-order == handler-order" claim most needs to defend is a
// consumer running Prefetch(N>1) that has SEVERAL unacked messages requeued at SAC
// promotion: does the broker redeliver that multi-message set to the standby in
// their original order, and does warren's dedupe keep the accepted stream
// contiguous? A reordering bug that only manifests with a multi-message in-flight
// set at failover would slip past T166d. This campaign exercises exactly that.
//
// Shape (reuses the T166d topology, helpers, ordered sink and readiness probe):
//
//   - connActive  → rmq1  : the active consumer, Concurrency(1) + Prefetch(N>1). KILLED.
//   - connStandby → rmq2  : the hot standby, Concurrency(1) + Prefetch(1) (the strict
//                           in-order VERIFIER of the redelivered set). SURVIVES.
//   - connMain    → rmq0  : publisher + topology declare + management observation. SURVIVES.
//
// How a multi-message in-flight set is built deterministically (no magic sleep):
// the active handler accepts+acks the prefix 0..holdAt-1 in order (proving active
// participation), then on seq==holdAt it signals "reached hold" and BLOCKS without
// accepting. With Concurrency(1) the single worker is now parked on holdAt, so the
// rest of the published prefix (holdAt+1..prefix-1) sits in the broker's prefetch
// window UNACKED — a multi-message checked-out set. The test then polls the
// management API until messages_unacknowledged >= a floor, an OBSERVED broker-side
// fact that makes the multi-message requeue non-vacuous, before killing rmq1. The
// broker requeues the whole unacked set (holdAt..prefix-1) to the promoted standby,
// which (Prefetch(1)) handles them strictly one-at-a-time, so any reordering across
// the requeue surfaces as an out-of-order violation. The suffix is published only
// after the standby is provably active.
//
// The held seqs are blocked BEFORE tracker.accept, so they are never credited to the
// active consumer and arrive at the standby as fresh (in-order) NEW deliveries —
// keeping the per-consumer accounting clean. Any seq the active actually acked whose
// ack was lost to the SIGKILL is tolerated as an at-least-once duplicate (counted by
// tracker.duplicates(), logged) and never breaks ordering.

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

const (
	sacMultiNumEvents  = 24 // total sequence 0..sacMultiNumEvents-1
	sacMultiPrefix     = 14 // 0..sacMultiPrefix-1 published before the kill
	sacMultiHoldAt     = 4  // active acks 0..sacMultiHoldAt-1, then holds (blocks) at sacMultiHoldAt
	sacMultiPrefetch   = 32 // active prefetch: room for the whole prefix to be checked out unacked
	sacMultiMinUnacked = 6  // broker must show >= this many unacked before the kill (expected: prefix-holdAt = 10)
)

// TestClusterSACFailover_OrderedAcrossNodeKill_MultiInFlight_cluster holds a
// multi-message unacked set on a Prefetch(N>1) active consumer, kills its node, and
// asserts the broker redelivers the whole requeued set to the promoted standby in
// order (deduped accepted stream == 0..N-1, contiguous, across the real node kill).
func TestClusterSACFailover_OrderedAcrossNodeKill_MultiInFlight_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const (
		queue          = "test.cluster.sac.failover.multi"
		activeService  = "rmq1"        // the active consumer's node — KILLED mid-batch
		activeNodeName = "rabbit@rmq1" // as the management API reports it
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Readiness + clean-slate + node-restore, identical discipline to T166d: wait for
	// all members so leader placement is deterministic, delete any stale durable queue,
	// and restore the killed node in LIFO cleanup (before the final delete).
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })
	t.Cleanup(func() { amqptest.StartNode(t, activeService) })

	connMain := dialSACNode(ctx, t, nodes[0]) // rmq0: publisher + declare + observe
	defer sacCloseConn(connMain)
	connActive := dialSACNode(ctx, t, nodes[1]) // rmq1: active consumer (killed)
	var closeActiveOnce sync.Once
	closeActive := func() { closeActiveOnce.Do(func() { sacCloseConn(connActive) }) }
	defer closeActive()
	connStandby := dialSACNode(ctx, t, nodes[2]) // rmq2: hot standby (promoted)
	defer sacCloseConn(connStandby)

	topo := &warren.Topology{
		Queues: []warren.Queue{{
			Name: queue, Durable: true, Type: warren.QueueTypeQuorum, SingleActiveConsumer: true,
		}},
	}
	require.NoError(t, topo.Declare(ctx, connMain))
	require.NoError(t, topo.AttachTo(connMain))

	// Precondition: leader survives the kill (declared on rmq0 → client-local locator),
	// so the SAC promotion is coordinated by a stable leader rather than racing a
	// re-election (T166c's concern). Same isolation as T166d.
	qs := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", qs.Type)
	require.Len(t, qs.Members, 3, "quorum queue must span all three cluster nodes")
	require.NotEqual(t, activeNodeName, qs.Leader,
		"queue leader must survive the active node's kill (declared on rmq0 → client-local locator)")

	// — Ordered sink shared by both consumers ————————————————————————————————————
	tracker := newSACOrderTracker(sacMultiNumEvents)
	allDone := make(chan struct{})
	var allDoneOnce sync.Once
	probeReady := make(chan string, 2) // see T166d: two distinct probes in flight at most
	holdReached := make(chan struct{}, 1)
	releaseHold := make(chan struct{})
	defer close(releaseHold) // safety net: unblock the held handler at teardown if the
	//                          context cancel somehow did not (it always should).
	var (
		violMu     sync.Mutex
		violations []error
	)

	recordViolation := func(err error) error {
		violMu.Lock()
		violations = append(violations, err)
		violMu.Unlock()
		return err // nack-no-requeue; surfaced by the violations assertion below
	}

	// The active handler accepts the prefix in order, then parks on seq==sacMultiHoldAt
	// (before accepting it) so seq holdAt..prefix-1 accumulate as an unacked set.
	activeHandler := func(hctx context.Context, m clusterSACMsg) error {
		if m.Seq == sacProbeSeq {
			select {
			case probeReady <- "active":
			default:
			}
			return nil
		}
		if m.Seq >= sacMultiHoldAt {
			// Reached the hold point. Signal once, then block WITHOUT accepting, so this
			// seq (and everything the broker has prefetched behind it) stays unacked and
			// is requeued as a multi-message set when the node is killed. Unblock only on
			// the kill's context cancel (normal path) or the teardown safety net.
			select {
			case holdReached <- struct{}{}:
			default:
			}
			select {
			case <-hctx.Done():
				return hctx.Err()
			case <-releaseHold:
				return nil
			}
		}
		switch res, want := tracker.accept(m.Seq, "active"); res {
		case sacAcceptOutOfOrder:
			return recordViolation(fmt.Errorf("active: out-of-order delivery: got seq=%d want=%d", m.Seq, want))
		case sacAcceptDuplicate:
			return nil
		case sacAcceptNew:
			// nothing special; the prefix never completes the whole stream on its own
		}
		return nil
	}

	// The standby handler is the strict in-order verifier of the redelivered set.
	standbyHandler := func(_ context.Context, m clusterSACMsg) error {
		if m.Seq == sacProbeSeq {
			select {
			case probeReady <- "standby":
			default:
			}
			return nil
		}
		switch res, want := tracker.accept(m.Seq, "standby"); res {
		case sacAcceptOutOfOrder:
			return recordViolation(fmt.Errorf("standby: out-of-order delivery across requeue: got seq=%d want=%d", m.Seq, want))
		case sacAcceptDuplicate:
			return nil // tolerated at-least-once redelivery (e.g. an ack lost to the SIGKILL)
		case sacAcceptNew:
			if tracker.complete() {
				allDoneOnce.Do(func() { close(allDone) })
			}
		}
		return nil
	}

	var consumerWG sync.WaitGroup
	activeErr := make(chan error, 1)
	standbyErr := make(chan error, 1)

	pub, err := warren.PublisherFor[clusterSACMsg](connMain).
		RoutingKey(queue).
		ConfirmTimeout(20 * time.Second).
		PublishRetry(clusterPublishRetry).
		Build()
	require.NoError(t, err)

	// — Bring up the active consumer (Prefetch(N>1)); it subscribes first → SAC active —
	activeConsumer, err := warren.ConsumerFor[clusterSACMsg](connActive).
		Queue(queue).Tag("sac-multi-active").Concurrency(1).Prefetch(sacMultiPrefetch).Build()
	require.NoError(t, err)
	cctxActive, cancelActive := context.WithCancel(ctx)
	defer cancelActive()
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		activeErr <- activeConsumer.Consume(cctxActive, activeHandler)
	}()

	require.NoError(t, publishSACProbe(ctx, pub))
	awaitSACProbe(ctx, t, probeReady, "active", 30*time.Second)

	// — Add the hot standby; SAC keeps "active" active because it subscribed first ——
	standbyConsumer, err := warren.ConsumerFor[clusterSACMsg](connStandby).
		Queue(queue).Tag("sac-multi-standby").Concurrency(1).Prefetch(1).Build()
	require.NoError(t, err)
	cctxStandby, cancelStandby := context.WithCancel(ctx)
	defer cancelStandby()
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		standbyErr <- standbyConsumer.Consume(cctxStandby, standbyHandler)
	}()

	// — Publish the whole prefix up front; the active acks 0..holdAt-1, then parks ————
	for seq := 0; seq < sacMultiPrefix; seq++ {
		require.NoError(t, publishSAC(ctx, pub, seq), "publish prefix seq=%d", seq)
	}

	// Wait until the active worker is provably parked on the hold point, so the rest of
	// the prefix is in flight (unacked) behind it.
	select {
	case <-holdReached:
	case <-time.After(30 * time.Second):
		t.Fatalf("active consumer did not reach the hold point at seq=%d", sacMultiHoldAt)
	case <-ctx.Done():
		t.Fatalf("context cancelled before hold: %v", ctx.Err())
	}

	// Prove the broker holds a MULTI-message in-flight set before the kill — the fact
	// that makes the requeue-ordering assertion non-vacuous (a single in-flight message
	// could never exceed one). Expected unacked == sacMultiPrefix-sacMultiHoldAt (10).
	unackedAtKill := awaitUnackedAtLeast(t, queue, sacMultiMinUnacked, 45*time.Second)
	t.Logf("active consumer holds %d unacked messages (>= %d required) before the kill — a multi-message in-flight set",
		unackedAtKill, sacMultiMinUnacked)

	// — Kill the active consumer's NODE mid-batch ——————————————————————————————————
	t.Logf("killing active consumer's node %s holding %d unacked — broker must requeue the whole set in order to rmq2",
		activeService, unackedAtKill)
	amqptest.KillNode(t, activeService)
	cancelActive() // unblocks the parked handler (returns context.Canceled) and stops the loop
	closeActive()  // close the dead-node connection so its reconnect supervisor stops spinning

	// — Observe the standby's promotion, then publish the suffix —————————————————————
	require.NoError(t, publishSACProbe(ctx, pub))
	awaitSACProbe(ctx, t, probeReady, "standby", 60*time.Second)

	for seq := sacMultiPrefix; seq < sacMultiNumEvents; seq++ {
		require.NoError(t, publishSAC(ctx, pub, seq), "publish suffix seq=%d", seq)
	}

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("not all %d events handled across the failover (accepted up to seq=%d)",
			sacMultiNumEvents, tracker.want()-1)
	case <-ctx.Done():
		t.Fatalf("context cancelled before completion (accepted up to seq=%d): %v", tracker.want()-1, ctx.Err())
	}

	// — Tear down both consumers and join their goroutines ——————————————————————————
	cancelStandby()
	consumerWG.Wait()
	require.NoError(t, filterSACActiveErr(<-activeErr), "active consumer must stop cleanly after the node kill")
	require.NoError(t, filterClusterCanceled(<-standbyErr), "standby consumer must stop cleanly")

	{
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		_ = pub.Close(closeCtx)
		c()
	}

	// — Assertions ——————————————————————————————————————————————————————————————————
	violMu.Lock()
	viol := append([]error(nil), violations...)
	violMu.Unlock()
	require.Empty(t, viol, "out-of-order deliveries across the multi-message requeue: %v", viol)

	require.NoError(t, tracker.verifyContiguous(),
		"the deduped accepted stream must be exactly 0..%d across the multi-message requeue at SAC promotion", sacMultiNumEvents-1)

	activeHandled := tracker.handledByCount("active")
	standbyHandled := tracker.handledByCount("standby")
	// Active credited only the pre-hold prefix; the standby credited the whole requeued
	// in-flight set plus the suffix. Both must have done real work, or the "failover"
	// was vacuous.
	assert.Equal(t, sacMultiHoldAt, activeHandled, "active consumer must have credited exactly the pre-hold prefix 0..%d", sacMultiHoldAt-1)
	assert.Equal(t, sacMultiNumEvents-sacMultiHoldAt, standbyHandled,
		"promoted standby must have credited the requeued in-flight set plus the suffix (seq %d..%d)", sacMultiHoldAt, sacMultiNumEvents-1)
	t.Logf("cluster SAC multi-in-flight failover: active credited %d, standby credited %d, %d at-least-once duplicates, accepted 0..%d in order (leader %s survived)",
		activeHandled, standbyHandled, tracker.duplicates(), sacMultiNumEvents-1, qs.Leader)
}

// awaitUnackedAtLeast polls the management API until the quorum queue reports at
// least min unacknowledged (checked-out) messages, returning the observed count.
// It is the broker-side readiness gate that proves a MULTI-message in-flight set is
// held before the kill — a poll, not a fixed sleep, because the management stats
// refresh on an interval and may lag the actual checkout by a few seconds.
func awaitUnackedAtLeast(t *testing.T, queue string, min int, within time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(within)
	var last int
	for time.Now().Before(deadline) {
		last = amqptest.QuorumLeader(t, queue).MessagesUnacknowledged
		if last >= min {
			return last
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("queue %q did not reach >= %d unacknowledged messages within %s (last=%d) — "+
		"the active consumer must hold a multi-message in-flight set before the kill", queue, min, within, last)
	return last // unreachable
}
