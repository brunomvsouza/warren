package warren

// White-box tests for T34c panic isolation of the three connection-level user
// callbacks: WithOnBlocked, WithOnReconnect, WithOnTopologyDegraded. A panicking
// callback must never crash the process or deadlock an internal loop; it must be
// recovered, logged with a stack trace, and leave the supervisor / barrier intact.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/log"
)

// errorfRecorder captures Errorf lines (the level used for recovered-panic logs)
// while delegating every other Logger method to a NoOp.
type errorfRecorder struct {
	log.Logger
	mu   sync.Mutex
	msgs []string
}

func (r *errorfRecorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *errorfRecorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.msgs...)
}

// — recoverCallback helper ————————————————————————————————————————————————

func TestRecoverCallback_recoversPanicAndLogsWithName(t *testing.T) {
	mc := newBareManaged(t)
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec

	require.NotPanics(t, func() {
		defer mc.recoverCallback("WithOnReconnect")
		panic("boom")
	})

	calls := rec.calls()
	require.Len(t, calls, 1)
	assert.Contains(t, calls[0], "WithOnReconnect")
	assert.Contains(t, calls[0], "panicked")
}

func TestRecoverCallback_noPanic_doesNotLog(t *testing.T) {
	mc := newBareManaged(t)
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec

	func() { defer mc.recoverCallback("WithOnBlocked") }()

	assert.Empty(t, rec.calls())
}

// — WithOnBlocked: async + recover ————————————————————————————————————————

func TestSafeOnBlocked_panickingCallback_doesNotCrash_logsAndDrains(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newBareManaged(t)
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec

	ran := make(chan struct{})
	mc.opts.onBlocked = func(reason string) {
		assert.Equal(t, "low on memory", reason)
		close(ran)
		panic("blocked boom")
	}

	mc.safeOnBlocked("low on memory")

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("onBlocked callback never ran")
	}

	mc.wg.Wait() // recover runs before wg.Done, so the log is present once Wait returns

	calls := rec.calls()
	require.Len(t, calls, 1)
	assert.Contains(t, calls[0], "WithOnBlocked")
}

func TestSafeOnBlocked_runsAsync_doesNotBlockSupervisor(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newBareManaged(t)
	release := make(chan struct{})
	mc.opts.onBlocked = func(string) { <-release }

	returned := make(chan struct{})
	go func() {
		mc.safeOnBlocked("x") // must return immediately, callback runs in its own goroutine
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("safeOnBlocked blocked the caller — the supervisor select loop would stall")
	}

	close(release)
	mc.wg.Wait()
}

func TestSafeOnBlocked_nilCallback_noop(t *testing.T) {
	defer goleak.VerifyNone(t)
	mc := newBareManaged(t)
	mc.opts.onBlocked = nil
	require.NotPanics(t, func() { mc.safeOnBlocked("x") })
	mc.wg.Wait()
}

// — WithOnReconnect: inline recover, barrier still released ————————————————

func TestRunBarrier_onReconnectPanics_releasesBarrierAndLogs(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newTestManaged(t) // reconnecting = true
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec
	mc.opts.onReconnect = func() { panic("reconnect boom") }

	// A Publisher blocked on the barrier must be released despite the panic.
	waiterErr := make(chan error, 1)
	go func() { waiterErr <- mc.waitBarrier(context.Background()) }()

	require.NotPanics(t, func() { mc.runBarrier(context.Background()) })

	select {
	case err := <-waiterErr:
		assert.NoError(t, err, "waiter must be released cleanly after the onReconnect panic")
	case <-time.After(2 * time.Second):
		t.Fatal("Publisher deadlocked: barrier never released after onReconnect panic")
	}

	mc.barrierMu.Lock()
	reconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	assert.False(t, reconnecting, "reconnecting flag must be cleared")

	calls := rec.calls()
	require.Len(t, calls, 1)
	assert.Contains(t, calls[0], "WithOnReconnect")
}

// — WithOnTopologyDegraded: goroutine recover, wg.Done always runs ————————

func TestRunBarrier_onTopoDegradedPanics_callsWgDoneAndLogs(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newTestManaged(t)
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec
	mc.opts.onTopoDegraded = func(error) { panic("degraded boom") }
	mc.registerHook(func(context.Context) error { return errors.New("queue gone") })

	require.NotPanics(t, func() { mc.runBarrier(context.Background()) })

	// wg.Done must run despite the panic, so Close would not hang.
	waitDone := make(chan struct{})
	go func() { mc.wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("wg.Done not called after onTopoDegraded panic — Close would hang forever")
	}

	mc.mu.RLock()
	degraded := mc.degraded
	mc.mu.RUnlock()
	assert.True(t, degraded, "the degraded state must still be entered")

	var found bool
	for _, c := range rec.calls() {
		if strings.Contains(c, "WithOnTopologyDegraded") {
			found = true
		}
	}
	assert.True(t, found, "the panic in onTopoDegraded must be logged")
}

// — WithOnResubscribe: recover-guarded seam (T34c parity, review I-1) ——————
//
// notifyResubscribed runs inside the reconnect barrier on the supervisor
// goroutine (via the consumer re-subscribe hook), so an unrecovered panic there
// would crash the process — the same failure mode T34c eliminated for the other
// five user callbacks. The replacement subscription is already installed before
// this fires, so a panicking callback must degrade to a logged error while the
// metric stays recorded and delivery still resumes.
func TestNotifyResubscribed_panickingCallback_recoversAndLogs_metricStillRecorded(t *testing.T) {
	mc := newBareManaged(t)
	rec := &errorfRecorder{Logger: log.NewNoOp()}
	mc.opts.logger = rec
	mc.opts.onResubscribe = func(string) { panic("resubscribe boom") }

	cm := &recordingResubMetrics{}
	require.NotPanics(t, func() {
		notifyResubscribed(mc, cm, "orders")
	})

	assert.Equal(t, []string{"orders"}, cm.queues,
		"metric must be recorded before the callback panics")

	calls := rec.calls()
	require.Len(t, calls, 1)
	assert.Contains(t, calls[0], "WithOnResubscribe")
	assert.Contains(t, calls[0], "panicked")
}
