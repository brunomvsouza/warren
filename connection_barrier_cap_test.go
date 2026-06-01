package warren

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
// barrier forever — at the cap the barrier returns, leaves reconnecting=true
// (a fresh reconnect is imminent), and records a forced RecordReconnect.
func TestRunBarrier_capForceReconnects(t *testing.T) {
	mc := newBareManaged(t)
	mc.opts.reconnectBarrierTimeout = 100 * time.Millisecond
	spy := &reconnectSpyMetrics{}
	mc.opts.metrics = spy

	// A hook that blocks until released — stands in for queue.declare stalling
	// on a half-alive broker.
	release := make(chan struct{})
	mc.registerHook(func(_ context.Context) error {
		<-release
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
	assert.Equal(t, int64(1), spy.reconnects.Load(), "a capped barrier force-reconnects (RecordReconnect)")

	mc.barrierMu.Lock()
	stillReconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	assert.True(t, stillReconnecting, "reconnecting stays true after a capped barrier (fresh reconnect imminent)")

	// Release the orphaned hook goroutine and drain it (goleak-clean).
	close(release)
	mc.wg.Wait()
}
