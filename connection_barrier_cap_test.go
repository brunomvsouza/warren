package warren

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// reconnectSpyMetrics counts RecordReconnect calls for the barrier-cap test.
type reconnectSpyMetrics struct {
	metrics.NoOpClientMetrics
	reconnects atomic.Int64
}

func (m *reconnectSpyMetrics) RecordReconnect(_ string) { m.reconnects.Add(1) }

// TestWaitBarrier_capReturnsErrReconnecting proves the reconnect barrier is
// bounded (T63 / R10-8 / DS-02): with PublishTimeout=0 + context.Background()
// (no ctx deadline), a publisher blocked on a reconnect barrier that never
// clears returns ErrReconnecting at the configured cap instead of stalling
// forever behind a half-alive broker.
func TestWaitBarrier_capReturnsErrReconnecting(t *testing.T) {
	mc := newBareManaged(t)
	mc.opts.reconnectBarrierTimeout = 100 * time.Millisecond

	// Simulate a barrier that never clears (a half-alive broker stalling the
	// redeclare): reconnecting stays true and nothing broadcasts a clear.
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()

	start := time.Now()
	err := mc.waitBarrier(context.Background())
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrReconnecting), "capped barrier wait must return ErrReconnecting, got %v", err)
	assert.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "must wait ~the cap before giving up")
	assert.Less(t, elapsed, 2*time.Second, "must not stall far past the cap")
}

// TestWaitBarrier_clearsBeforeCap confirms a barrier that clears before the cap
// returns nil (no spurious ErrReconnecting): the cap is a ceiling, not a delay.
func TestWaitBarrier_clearsBeforeCap(t *testing.T) {
	mc := newBareManaged(t)
	mc.opts.reconnectBarrierTimeout = 2 * time.Second

	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()

	go func() {
		time.Sleep(50 * time.Millisecond)
		mc.barrierMu.Lock()
		mc.reconnecting = false
		mc.barrierCond.Broadcast()
		mc.barrierMu.Unlock()
	}()

	start := time.Now()
	err := mc.waitBarrier(context.Background())
	elapsed := time.Since(start)

	require.NoError(t, err, "a barrier that clears before the cap must return nil")
	assert.Less(t, elapsed, 1*time.Second, "must return promptly once the barrier clears")
}

// TestRunBarrier_capForceReconnects proves the barrier EXECUTION is bounded
// (T63): a redeclare hook that stalls (half-alive broker) does not hang the
// barrier forever — at the cap the barrier returns, cancels the hook ctx, and
// leaves reconnecting=true (a fresh reconnect is imminent). The forced
// RecordReconnect is emitted on the supervisor's re-dial path, NOT here —
// counting it in the cap branch too would double-count one logical reconnect.
func TestRunBarrier_capForceReconnects(t *testing.T) {
	mc := newBareManaged(t)
	mc.opts.reconnectBarrierTimeout = 100 * time.Millisecond
	spy := &reconnectSpyMetrics{}
	mc.opts.metrics = spy

	// A hook that blocks until released or its (hook) ctx is cancelled — stands
	// in for queue.declare stalling on a half-alive broker. Observing ctx.Done()
	// proves the cap's hookCancel wiring (so a multi-declare hook bails).
	release := make(chan struct{})
	hookCtxCancelled := make(chan struct{})
	mc.registerHook(func(ctx context.Context) error {
		select {
		case <-release:
		case <-ctx.Done():
			close(hookCtxCancelled)
		}
		return nil
	})

	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		mc.runBarrier(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("runBarrier did not return at the cap — the barrier is unbounded")
	}
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "must run up to the cap")
	assert.Less(t, elapsed, 1*time.Second, "must not run far past the cap")
	assert.Equal(t, int64(0), spy.reconnects.Load(),
		"the cap must NOT self-count RecordReconnect — the forced re-dial counts it on the supervisor path")

	mc.barrierMu.Lock()
	stillReconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	assert.True(t, stillReconnecting, "reconnecting stays true after a capped barrier (fresh reconnect imminent)")

	// The hook ctx must be cancelled at the cap so a multi-declare hook bails
	// rather than issuing the full topology against a doomed socket.
	select {
	case <-hookCtxCancelled:
	case <-time.After(time.Second):
		t.Fatal("hook ctx was not cancelled at the cap")
	}
	mc.wg.Wait()
}

// TestSupervisor_cappedBarrierNilRaw_redialsNotExit is the regression guard for
// the barrier-cap socket-wedge bug (T63): after the cap nils mc.raw while
// reconnecting, the supervisor must take its nil-raw branch and RE-DIAL, not
// exit. Before the fix, the cap left mc.raw pointing at the just-closed conn;
// the supervisor re-read it, registered fresh NotifyClose/NotifyBlocked channels
// that came back pre-closed, and exited via the closeCh !ok arm ~half the time —
// permanently wedging the socket with reconnecting=true.
func TestSupervisor_cappedBarrierNilRaw_redialsNotExit(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newBareManaged(t)
	mc.forceReconnectCh = make(chan struct{}, 1)
	spy := &reconnectSpyMetrics{}
	mc.opts.metrics = spy
	// Fail-fast dial so the re-dial attempt completes quickly and the supervisor
	// then exits cleanly (connected=false → reconnecting cleared → return).
	mc.opts.reconnectBackoff = RetryPolicy{Min: time.Millisecond, Max: time.Millisecond, Retries: 1, WithoutJitter: true}

	var dials atomic.Int64
	mc.dialFactory = func(_ context.Context) (*amqp091.Connection, error) {
		dials.Add(1)
		return nil, errors.New("boom: broker unreachable")
	}

	// Post-cap state: raw nil'd, reconnecting still true.
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()

	go mc.supervisor(context.Background())

	select {
	case <-mc.done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not return — it neither re-dialed-then-gave-up nor exited")
	}

	assert.GreaterOrEqual(t, dials.Load(), int64(1),
		"supervisor must re-dial on a nil-raw-while-reconnecting entry, not exit silently")
	assert.GreaterOrEqual(t, spy.reconnects.Load(), int64(1),
		"the forced re-dial must record RecordReconnect on the supervisor path")
}
