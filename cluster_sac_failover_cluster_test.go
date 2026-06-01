//go:build cluster

package warren_test

// SingleActiveConsumer failover on a REAL node kill — Phase 9.5 / T166d
// (SPEC §6.3 single-active-consumer ordering carried onto a multi-node cluster).
//
// This is the cluster-grade counterpart of examples/ordered_consume and
// consumer_resubscribe_integration_test.go. Both prove SAC failover by CLOSING the
// active consumer's connection — a faked "the instance died" event that a
// single-node broker can stage. Here the active consumer's whole NODE is KILLED
// (SIGKILL, a real crash), the failure mode only a real cluster can produce: the
// broker must promote a standby that lives on a DIFFERENT surviving node, over a
// DIFFERENT TCP connection, while the quorum queue stays available through the
// surviving majority.
//
// Topology under test: a durable QUORUM queue with x-single-active-consumer. Two
// consumers subscribe, each on its OWN warren connection pinned to its OWN node
// (modelling two service processes on two cluster members):
//
//   - connActive  → rmq1  : the active consumer (subscribes first → SAC active). KILLED.
//   - connStandby → rmq2  : the hot standby (subscribes second). SURVIVES, is promoted.
//   - connMain    → rmq0  : publisher + topology declare + management observation. SURVIVES.
//
// Leader placement: connMain declares on rmq0, and rabbitmq.conf pins
// queue_leader_locator=client-local, so the quorum queue's Raft leader lands on
// rmq0 — a node that SURVIVES the kill of rmq1. Keeping the queue coordinator alive
// isolates the variable under test (the active CONSUMER's node death) from the
// queue-LEADER death that T166c already covers, so the SAC promotion here is
// coordinated by a stable leader rather than racing a concurrent re-election.
//
// The contract asserted: publish order == handler order across the real node death.
// A numbered sequence 0..N-1 is published; the active consumer (Concurrency(1),
// Prefetch(1)) handles the prefix in order; its node is killed mid-sequence; the
// broker promotes the standby, which handles the suffix in order. At-least-once
// permits a single in-flight message to be requeued to the standby at the handoff,
// so the sink dedupes by sequence number and asserts the DEDUPED accepted stream is
// exactly 0..N-1. The per-consumer handled counts prove BOTH instances
// participated — a real failover, not the active consumer quietly draining all N.
//
// Determinism without magic sleeps: each promotion is made observable with an
// out-of-band readiness probe (seq=-1); the suffix is published only after the
// probe proves the standby is the active consumer, so the standby provably handles
// it. goleak: the active consumer's connection (whose only node is now dead) is
// cancelled and closed right after the kill so its reconnect supervisor stops
// spinning; the killed node is restarted in t.Cleanup to restore the cluster.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
)

// clusterDialTimeout bounds both the TCP connect AND the AMQP handshake of every
// dial on these single-node connections. It is the load-bearing detail that keeps
// the campaign goleak-clean across the kill: the active consumer's node is killed
// behind a still-listening Toxiproxy proxy, so a reconnect dial to it ACCEPTS the
// TCP connection (the proxy is up) but never completes the AMQP handshake (the
// upstream broker is dead). amqp091's handshake read is not context-aware, so
// without this deadline that dial hangs ~30 s (the default) and Close cannot drain
// the supervisor goroutine that is blocked waiting on it. With the deadline the
// dial fails fast, the supervisor returns to backoff, and Close drains promptly.
const clusterDialTimeout = 5 * time.Second

// boundedClusterDialer is DefaultDial with a tight timeout. amqp091 clears this
// deadline once the handshake completes (openComplete → SetDeadline(zero) → the
// heartbeater then manages read deadlines), so a HEALTHY connection is unaffected —
// the deadline only bites a dial whose upstream never answers (the killed node).
func boundedClusterDialer(network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, clusterDialTimeout)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(clusterDialTimeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

type clusterSACMsg struct {
	Seq int `json:"seq"`
}

const (
	sacNumEvents = 20
	sacHandoffAt = 9  // the active consumer handles 0..sacHandoffAt; the promoted standby handles the rest
	sacProbeSeq  = -1 // readiness probe: out-of-band sequence number, never part of 0..N-1
)

// filterSACActiveErr drops the benign error the active consumer returns once its
// node is killed and its connection cancelled+closed: context.Canceled when the
// cancel is observed first (the normal path), or a shutdown/closed error if Close
// wins the race. Any OTHER error means the failover path misbehaved and must
// surface. The standby consumer instead stops via a plain context cancel, so it
// reuses filterClusterCanceled (defined in cluster_quorum_failover_cluster_test.go).
//
// ErrConsumerCancelled is deliberately NOT in the tolerated set: a SIGKILLed node
// cannot send a broker-side basic.cancel, so a consumer-cancel arriving here would
// mean the broker cancelled our consumer for some OTHER reason (e.g. the queue was
// deleted out from under us) — a real defect this campaign must surface, not the
// expected local cancel+close of the dead-node connection.
func filterSACActiveErr(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, warren.ErrShutdown) ||
		errors.Is(err, warren.ErrAlreadyClosed) ||
		errors.Is(err, warren.ErrChannelClosed) {
		return nil
	}
	return err
}

// TestClusterSACFailover_OrderedAcrossNodeKill_cluster publishes a numbered
// sequence to a SingleActiveConsumer quorum queue, kills the active consumer's NODE
// mid-sequence, and asserts the broker promotes the standby and publish order ==
// handler order across the real node death (deduped accepted stream == 0..N-1).
func TestClusterSACFailover_OrderedAcrossNodeKill_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const (
		queue          = "test.cluster.sac.failover"
		activeService  = "rmq1"        // the active consumer's node — KILLED mid-sequence
		activeNodeName = "rabbit@rmq1" // as the management API reports it
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Readiness gate: an earlier cluster test may have killed and restarted a node
	// (docker start returns before RabbitMQ has rebooted and rejoined). Declaring the
	// quorum queue while a member is momentarily absent would skew leader placement;
	// wait for all three members running first so this campaign is order-independent.
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	// Clean slate: a prior run may have left the durable quorum queue behind; delete
	// it so the leader is freshly placed by the client-local locator below. Cleanup is
	// LIFO, so the node restore (registered next) runs before this delete — the cluster
	// is whole again when the durable queue is removed.
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })
	t.Cleanup(func() { amqptest.StartNode(t, activeService) })

	// Three single-node connections model three service instances on three members.
	connMain := dialSACNode(ctx, t, nodes[0]) // rmq0: publisher + declare + observe
	defer sacCloseConn(connMain)
	connActive := dialSACNode(ctx, t, nodes[1]) // rmq1: active consumer (killed)
	var closeActiveOnce sync.Once
	closeActive := func() { closeActiveOnce.Do(func() { sacCloseConn(connActive) }) }
	defer closeActive()
	connStandby := dialSACNode(ctx, t, nodes[2]) // rmq2: hot standby (promoted)
	defer sacCloseConn(connStandby)

	// Durable quorum queue + SingleActiveConsumer, default-exchange routed (routing
	// key == queue name), so there is no exchange/binding to clean up.
	topo := &warren.Topology{
		Queues: []warren.Queue{{
			Name: queue, Durable: true, Type: warren.QueueTypeQuorum, SingleActiveConsumer: true,
		}},
	}
	require.NoError(t, topo.Declare(ctx, connMain))
	require.NoError(t, topo.AttachTo(connMain))

	// Precondition: the queue spans all three members and its leader is NOT on the
	// node we are about to kill — so the quorum coordinator survives and the SAC
	// promotion is coordinated by a stable leader (rmq0 via client-local locator)
	// rather than racing a re-election (which is T166c's concern, not this test's).
	qs := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", qs.Type)
	require.Len(t, qs.Members, 3, "quorum queue must span all three cluster nodes")
	require.NotEqual(t, activeNodeName, qs.Leader,
		"queue leader must survive the active node's kill (declared on rmq0 → client-local locator)")

	// — Ordered sink shared by both consumers ————————————————————————————————————
	tracker := newSACOrderTracker(sacNumEvents)
	allDone := make(chan struct{})
	var allDoneOnce sync.Once
	// handoffReached fires once, when the active consumer accepts seq==sacHandoffAt.
	// Buffered (size 1) with a non-blocking select-default send so the handler never
	// parks even though only the test goroutine reads it, and a (defensive) repeat
	// signal is dropped rather than blocking a worker.
	handoffReached := make(chan struct{}, 1)
	// probeReady carries the name of whichever consumer handled a readiness probe.
	// Buffered (size 2) so an active-probe signal not yet drained by awaitSACProbe
	// cannot block the standby's later probe send (the handler sends non-blocking) —
	// two is the most distinct probes ever in flight across the single handoff
	// (one to confirm "active", one to confirm "standby").
	probeReady := make(chan string, 2)
	var (
		violMu     sync.Mutex
		violations []error
	)

	makeHandler := func(name string) warren.Handler[clusterSACMsg] {
		return func(_ context.Context, m clusterSACMsg) error {
			if m.Seq == sacProbeSeq {
				// The readiness probe proves THIS consumer is the active one — the
				// broker only delivers to the active consumer of a SAC queue.
				select {
				case probeReady <- name:
				default:
				}
				return nil
			}
			switch res, want := tracker.accept(m.Seq, name); res {
			case sacAcceptOutOfOrder:
				err := fmt.Errorf("%s: out-of-order delivery: got seq=%d want=%d", name, m.Seq, want)
				violMu.Lock()
				violations = append(violations, err)
				violMu.Unlock()
				return err // nack-no-requeue; surfaced by the violations assertion below
			case sacAcceptDuplicate:
				return nil // ack; an in-flight message was requeued to the standby at the handoff
			case sacAcceptNew:
				if m.Seq == sacHandoffAt {
					select {
					case handoffReached <- struct{}{}:
					default:
					}
				}
				if tracker.complete() {
					allDoneOnce.Do(func() { close(allDone) })
				}
			}
			return nil
		}
	}

	var consumerWG sync.WaitGroup
	activeErr := make(chan error, 1)
	standbyErr := make(chan error, 1)

	pub, err := warren.PublisherFor[clusterSACMsg](connMain).
		RoutingKey(queue). // default exchange "" → route straight to the quorum queue
		ConfirmTimeout(20 * time.Second).
		PublishRetry(clusterPublishRetry).
		Build()
	require.NoError(t, err)

	// — Bring up the active consumer; it subscribes first, so SAC makes it active ——
	activeConsumer, err := warren.ConsumerFor[clusterSACMsg](connActive).
		Queue(queue).Tag("sac-active").Concurrency(1).Prefetch(1).Build()
	require.NoError(t, err)
	cctxActive, cancelActive := context.WithCancel(ctx)
	defer cancelActive()
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		activeErr <- activeConsumer.Consume(cctxActive, makeHandler("active"))
	}()

	// Prove the active consumer's basic.consume is live before adding the standby.
	require.NoError(t, publishSACProbe(ctx, pub))
	awaitSACProbe(ctx, t, probeReady, "active", 30*time.Second)

	// — Add the hot standby; SAC keeps "active" active because it subscribed first ——
	standbyConsumer, err := warren.ConsumerFor[clusterSACMsg](connStandby).
		Queue(queue).Tag("sac-standby").Concurrency(1).Prefetch(1).Build()
	require.NoError(t, err)
	cctxStandby, cancelStandby := context.WithCancel(ctx)
	defer cancelStandby()
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		standbyErr <- standbyConsumer.Consume(cctxStandby, makeHandler("standby"))
	}()

	// — Publish the prefix; the active consumer handles 0..handoffAt in order ————————
	for seq := 0; seq <= sacHandoffAt; seq++ {
		require.NoError(t, publishSAC(ctx, pub, seq), "publish prefix seq=%d", seq)
	}
	select {
	case <-handoffReached:
	case <-time.After(30 * time.Second):
		t.Fatalf("active consumer did not reach the handoff at seq=%d", sacHandoffAt)
	case <-ctx.Done():
		t.Fatalf("context cancelled before handoff: %v", ctx.Err())
	}

	// — Kill the active consumer's NODE (a real crash) mid-sequence ————————————————
	t.Logf("killing active consumer's node %s after seq=%d — broker must promote the standby on rmq2",
		activeService, sacHandoffAt)
	amqptest.KillNode(t, activeService)
	// Stop the active consumer locally: cancel its loop (returns context.Canceled),
	// then close its connection whose only node is now dead so the reconnect supervisor
	// stops spinning and nothing leaks into the goleak check.
	cancelActive()
	closeActive()

	// The broker promotes the standby. Make it observable before publishing the suffix:
	// a probe handled by the standby proves its basic.consume is now the active one.
	require.NoError(t, publishSACProbe(ctx, pub))
	awaitSACProbe(ctx, t, probeReady, "standby", 60*time.Second)

	// — Publish the suffix; only the standby can handle it now ——————————————————————
	for seq := sacHandoffAt + 1; seq < sacNumEvents; seq++ {
		require.NoError(t, publishSAC(ctx, pub, seq), "publish suffix seq=%d", seq)
	}

	select {
	case <-allDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("not all %d events handled across the failover (accepted up to seq=%d)",
			sacNumEvents, tracker.want()-1)
	case <-ctx.Done():
		t.Fatalf("context cancelled before completion (accepted up to seq=%d): %v", tracker.want()-1, ctx.Err())
	}

	// — Tear down both consumers and join their goroutines ——————————————————————————
	cancelStandby() // the active consumer was already cancelled+closed after the kill
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
	require.Empty(t, viol, "out-of-order deliveries across the failover: %v", viol)

	require.NoError(t, tracker.verifyContiguous(),
		"the deduped accepted stream must be exactly 0..%d across the real node kill", sacNumEvents-1)

	activeHandled := tracker.handledByCount("active")
	standbyHandled := tracker.handledByCount("standby")
	// Both instances must have done real work: the active consumer handled the
	// pre-kill prefix and the promoted standby handled the post-failover suffix. If
	// the standby had handled nothing, the "failover" was vacuous (a single-node
	// broker would satisfy completion without any promotion).
	assert.Equal(t, sacHandoffAt+1, activeHandled, "active consumer must have handled the pre-kill prefix 0..%d", sacHandoffAt)
	assert.Equal(t, sacNumEvents-sacHandoffAt-1, standbyHandled, "promoted standby must have handled the post-failover suffix")
	t.Logf("cluster SAC failover ordered across node kill: active handled %d, standby handled %d, accepted 0..%d in order (leader %s survived)",
		activeHandled, standbyHandled, sacNumEvents-1, qs.Leader)
}

// dialSACNode dials a single cluster node (one publisher + one consumer connection)
// with the tight reconnect backoff the cluster campaigns share, so a connection
// pinned to a node that later dies rotates/closes promptly instead of paying the
// 1 s default backoff. Single-addr by design: pinning each consumer to its own node
// is what makes "kill the active consumer's node" a precise, observable event.
func dialSACNode(ctx context.Context, t *testing.T, addr string) *warren.Connection {
	t.Helper()
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := warren.Dial(dialCtx,
		warren.WithAddr(addr),
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithReconnectBackoff(clusterFastBackoff),
		warren.WithDialer(boundedClusterDialer),
	)
	require.NoError(t, err)
	return conn
}

// sacCloseConn closes a connection with a bounded timeout, ignoring the error (a
// connection whose only node was killed has nothing left to flush).
func sacCloseConn(conn *warren.Connection) {
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func publishSAC(ctx context.Context, pub *warren.Publisher[clusterSACMsg], seq int) error {
	id := fmt.Sprintf("cluster-sac-%d", seq)
	return pub.Publish(ctx, warren.Message[clusterSACMsg]{Body: &clusterSACMsg{Seq: seq}, MessageID: id})
}

func publishSACProbe(ctx context.Context, pub *warren.Publisher[clusterSACMsg]) error {
	return pub.Publish(ctx, warren.Message[clusterSACMsg]{Body: &clusterSACMsg{Seq: sacProbeSeq}})
}

// awaitSACProbe blocks until the named consumer handles a readiness probe, proving
// it is the active consumer of the SAC queue. Probes handled by another consumer
// (or any non-probe delivery, which never reaches probeReady) are ignored — no
// magic sleep, the promotion is gated on an observed fact.
func awaitSACProbe(ctx context.Context, t *testing.T, probeReady <-chan string, who string, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case got := <-probeReady:
			if got == who {
				return
			}
		case <-deadline:
			t.Fatalf("%s did not become the active consumer within %s", who, within)
		case <-ctx.Done():
			t.Fatalf("context cancelled before %s became active: %v", who, ctx.Err())
		}
	}
}

// sacAcceptResult classifies a delivery within the ordered sink.
type sacAcceptResult int

const (
	sacAcceptNew        sacAcceptResult = iota // first sight, in order
	sacAcceptDuplicate                         // already accepted (handoff redelivery)
	sacAcceptOutOfOrder                        // gap or regression — a correctness failure
)

// sacOrderTracker is the shared, concurrency-safe ordered sink for the SAC failover
// campaign. With SingleActiveConsumer + Concurrency(1) only one handler runs at a
// time, but the mutex keeps the active→standby handoff race-free, and handledBy
// records which consumer accepted each new sequence number so the test can prove
// both instances participated.
type sacOrderTracker struct {
	mu        sync.Mutex
	n         int
	seen      map[int]struct{}
	nextWant  int
	order     []int
	handledBy map[string]int
	dupCount  int // at-least-once redeliveries (a seq seen twice across the handoff)
}

func newSACOrderTracker(n int) *sacOrderTracker {
	return &sacOrderTracker{n: n, seen: make(map[int]struct{}, n), handledBy: make(map[string]int)}
}

// accept records seq when it is the next expected value, crediting name. It returns
// the verdict and the next-expected sequence number; only the out-of-order verdict
// reports a meaningful want (an out-of-order seq leaves nextWant unchanged).
func (t *sacOrderTracker) accept(seq int, name string) (sacAcceptResult, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.seen[seq]; ok {
		t.dupCount++
		return sacAcceptDuplicate, t.nextWant
	}
	if seq != t.nextWant {
		return sacAcceptOutOfOrder, t.nextWant
	}
	t.seen[seq] = struct{}{}
	t.order = append(t.order, seq)
	t.nextWant++
	t.handledBy[name]++
	return sacAcceptNew, t.nextWant
}

func (t *sacOrderTracker) complete() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextWant >= t.n
}

func (t *sacOrderTracker) want() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextWant
}

func (t *sacOrderTracker) handledByCount(name string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.handledBy[name]
}

// duplicates returns how many deliveries were already-seen sequence numbers — the
// at-least-once redeliveries the handoff produces (a single in-flight message in
// T166d, a multi-message in-flight set in the Prefetch(N>1) campaign). Observable
// across runs so the requeue path's coverage is not silently zero.
func (t *sacOrderTracker) duplicates() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dupCount
}

func (t *sacOrderTracker) verifyContiguous() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.order) != t.n {
		return fmt.Errorf("accepted %d events, want %d", len(t.order), t.n)
	}
	for i, seq := range t.order {
		if seq != i {
			return fmt.Errorf("order mismatch at position %d: got seq=%d want=%d", i, seq, i)
		}
	}
	return nil
}
