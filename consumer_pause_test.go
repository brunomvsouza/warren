package warren

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// fakeSubChannel is a test double for the subset of *amqp091.Channel that
// Pause/Resume drive: Cancel (local basic.cancel) and Consume (re-subscribe).
type fakeSubChannel struct {
	mu           sync.Mutex
	cancelCalls  []string
	consumeCalls []string
	deliveries   chan amqp091.Delivery
	consumeErr   error
	cancelErr    error
	// onConsume, if set, is invoked inside Consume AFTER the call is recorded and the
	// lock is released — i.e. past Resume's top ctx guard, during the resubCh handoff.
	// Tests use it to deterministically cancel the Resume ctx mid-handshake.
	onConsume func()
}

func (f *fakeSubChannel) Cancel(consumer string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, consumer)
	return f.cancelErr
}

func (f *fakeSubChannel) Consume(_, consumer string, _, _, _, _ bool, _ amqp091.Table) (<-chan amqp091.Delivery, error) {
	f.mu.Lock()
	f.consumeCalls = append(f.consumeCalls, consumer)
	if f.deliveries == nil {
		f.deliveries = make(chan amqp091.Delivery)
	}
	hook, cErr, d := f.onConsume, f.consumeErr, f.deliveries
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if cErr != nil {
		return nil, cErr
	}
	return d, nil
}

func (f *fakeSubChannel) cancels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancelCalls...)
}

func (f *fakeSubChannel) consumes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.consumeCalls...)
}

// pausableConsumer builds a started consumer with an injected live channel so
// Pause/Resume can be exercised without a broker. It seeds a cancelable lifecycle
// ctx (runCtx) — the same ctx Resume binds its re-subscribe pump to — and cancels
// it at cleanup so any pump goroutine a test spawns drains for goleak.
func pausableConsumer(t *testing.T, fake *fakeSubChannel) *Consumer[string] {
	t.Helper()
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Tag("ctag-x").Build()
	require.NoError(t, err)
	c.started.Store(true)
	c.resubCh = make(chan deliverySub, 1)
	c.cancelReasonCh = make(chan string, 1)
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	c.runCtx = runCtx
	c.live = &liveSub{ch: fake, closeDone: func() {}, done: make(chan struct{})}
	return c
}

func TestConsumer_Pause_IssuesLocalCancel_AndMarksPaused(t *testing.T) {
	fake := &fakeSubChannel{}
	c := pausableConsumer(t, fake)

	require.NoError(t, c.Pause(context.Background()))
	assert.Equal(t, []string{"ctag-x"}, fake.cancels(), "Pause issues basic.cancel with the consumer tag")
	assert.True(t, c.snapshot().Paused, "Health reports Paused after Pause")
	assert.False(t, c.snapshot().Active, "a paused consumer is not Active")

	// Idempotent: a second Pause is a no-op, no extra cancel.
	require.NoError(t, c.Pause(context.Background()))
	assert.Len(t, fake.cancels(), 1)
}

func TestConsumer_Pause_CancelError_RollsBackPausedState(t *testing.T) {
	fake := &fakeSubChannel{cancelErr: errors.New("boom")}
	c := pausableConsumer(t, fake)

	err := c.Pause(context.Background())
	require.Error(t, err)
	assert.False(t, c.paused.Load(), "a failed Cancel must not leave the consumer marked paused")
}

func TestConsumer_Resume_ReissuesConsume_AndClearsPaused(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)
	c.paused.Store(true)

	// Own the lifecycle ctx so the resume pump can be drained before goleak runs
	// (deferred goleak fires before t.Cleanup's cancel).
	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun()

	require.NoError(t, c.Resume(context.Background()))
	assert.Equal(t, []string{"ctag-x"}, fake.consumes(), "Resume re-issues basic.consume with the consumer tag")
	assert.False(t, c.snapshot().Paused, "Health clears Paused after Resume")

	select {
	case sub := <-c.resubCh:
		assert.NotNil(t, sub.ch, "Resume must hand the running loop a fresh subscription")
	default:
		t.Fatal("Resume must push a new subscription to resubCh")
	}
}

// TestConsumer_Resume_PumpBoundToLifecycleCtx_NotResumeCtx pins the review fix:
// the re-subscribe pump is bound to the consumer-lifecycle ctx (runCtx), not to
// the ctx passed to Resume. Cancelling the Resume ctx must NOT stop delivery — a
// request-scoped Resume ctx is a realistic caller pattern, and a silently dead
// consumer is exactly what T53 set out to make observable (T53).
func TestConsumer_Resume_PumpBoundToLifecycleCtx_NotResumeCtx(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)
	c.paused.Store(true)

	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun() // lifecycle cancel drains the pump before goleak

	resumeCtx, cancelResume := context.WithCancel(context.Background())
	require.NoError(t, c.Resume(resumeCtx))

	var sub deliverySub
	select {
	case sub = <-c.resubCh:
		require.NotNil(t, sub.ch)
	default:
		t.Fatal("Resume must push a new subscription to resubCh")
	}

	// Cancel the Resume ctx: the pump must stay alive and keep forwarding.
	cancelResume()
	fake.deliveries <- amqp091.Delivery{Body: []byte(`"x"`)}
	select {
	case d, ok := <-sub.ch:
		require.True(t, ok, "the re-subscribe pump must stay open after the Resume ctx is cancelled")
		assert.Equal(t, []byte(`"x"`), d.Body)
	case <-time.After(2 * time.Second):
		t.Fatal("re-subscribe pump died when the Resume ctx was cancelled")
	}
}

// TestConsumer_Resume_ConsumeError_StaysPaused mirrors the Pause cancel-error
// rollback: when the re-issued basic.consume fails, Resume returns the wrapped error
// and the consumer stays paused so the caller can retry — it must NOT clear the
// paused flag or hand a subscription to the loop (T53).
func TestConsumer_Resume_ConsumeError_StaysPaused(t *testing.T) {
	fake := &fakeSubChannel{consumeErr: errors.New("boom")}
	c := pausableConsumer(t, fake)
	c.paused.Store(true)

	err := c.Resume(context.Background())
	require.Error(t, err)
	assert.True(t, c.paused.Load(), "a failed Resume must leave the consumer paused for retry")
	select {
	case <-c.resubCh:
		t.Fatal("a failed Resume must not push a subscription to resubCh")
	default:
	}
}

// TestConsumer_Resume_CtxCancelledDuringHandoff_RollsBack pins the rollback fix: if
// the Resume ctx is cancelled after the basic.consume is issued but before the
// running loop adopts the new subscription, Resume must cancel that subscription and
// stay paused — leaving neither an orphaned broker subscription nor a Health that
// reports Paused while actually subscribed (T53).
func TestConsumer_Resume_CtxCancelledDuringHandoff_RollsBack(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)
	c.paused.Store(true)

	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun() // lifecycle cancel drains the rolled-back pump before goleak

	// Fill the size-1 handoff channel so the resubCh send blocks and the ctx.Done()
	// arm is the only one that can proceed.
	c.resubCh <- deliverySub{}

	resumeCtx, cancelResume := context.WithCancel(context.Background())
	// Cancel from inside Consume: past Resume's top ctx guard, during the handoff.
	fake.onConsume = func() { cancelResume() }

	err := c.Resume(resumeCtx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"ctag-x"}, fake.cancels(), "a cancelled handoff must roll back the basic.consume")
	assert.True(t, c.paused.Load(), "a cancelled Resume handoff must leave the consumer paused")
}

func TestConsumer_Resume_BeforeStart_Errors(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	require.ErrorIs(t, c.Resume(context.Background()), ErrInvalidOptions)
}

func TestConsumer_Resume_AfterClose_Errors(t *testing.T) {
	fake := &fakeSubChannel{}
	c := pausableConsumer(t, fake)
	require.NoError(t, c.Close(context.Background()))

	require.ErrorIs(t, c.Resume(context.Background()), ErrAlreadyClosed)
	assert.Empty(t, fake.consumes(), "Resume after Close issues no basic.consume")
}

// TestConsumer_Pause_NoLiveSubscription_ReturnsErrReconnecting covers the
// Pause-during-a-reconnect-window edge: with no live channel, Pause cannot issue a
// local basic.cancel and reports ErrReconnecting rather than panicking (T53).
func TestConsumer_Pause_NoLiveSubscription_ReturnsErrReconnecting(t *testing.T) {
	fake := &fakeSubChannel{}
	c := pausableConsumer(t, fake)
	c.live = nil // reconnect window: no physical channel bound

	require.ErrorIs(t, c.Pause(context.Background()), ErrReconnecting)
	assert.Empty(t, fake.cancels(), "Pause with no live subscription issues no basic.cancel")
}

func TestConsumer_Resume_WhenNotPaused_IsNoOp(t *testing.T) {
	fake := &fakeSubChannel{}
	c := pausableConsumer(t, fake)

	require.NoError(t, c.Resume(context.Background()))
	assert.Empty(t, fake.consumes(), "Resume on a non-paused consumer issues no basic.consume")
}

func TestConsumer_Pause_BeforeStart_Errors(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	require.ErrorIs(t, c.Pause(context.Background()), ErrInvalidOptions)
}

func TestConsumer_Pause_AfterClose_Errors(t *testing.T) {
	fake := &fakeSubChannel{}
	c := pausableConsumer(t, fake)
	require.NoError(t, c.Close(context.Background()))

	require.ErrorIs(t, c.Pause(context.Background()), ErrAlreadyClosed)
	assert.Empty(t, fake.cancels(), "Pause after Close issues no basic.cancel")
}

// TestConsumer_OpenDeliveryCh_ClearsStalePause pins the review fix for
// reconnect-during-pause: (re)opening a delivery channel means the consumer is
// subscribed again, so a stale pause is cleared. This keeps Health accurate and
// makes a later Resume a no-op rather than a duplicate basic.consume (T53).
func TestConsumer_OpenDeliveryCh_ClearsStalePause(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	// Override path stands in for a fresh (reconnect) subscription, no broker needed.
	c.deliverySubOverride = &deliverySub{ch: make(chan amqp091.Delivery), done: nil}
	c.paused.Store(true)

	_, err = c.openDeliveryCh(context.Background())
	require.NoError(t, err)
	assert.False(t, c.paused.Load(), "a fresh subscription clears a stale pause")
}

// TestConsumer_Snapshot_ConcurrentWithPauseResume hammers Pause/Resume on one
// goroutine while another reads snapshot(), under -race. It guards two things: the
// lock-free snapshot reads never race the Pause/Resume writes, and the snapshot is
// always internally consistent — Active and Paused are never both true (T53).
func TestConsumer_Snapshot_ConcurrentWithPauseResume(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)

	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun() // drains any pump Resume spawns before goleak

	// Drain resubCh so a storm of Resume calls never blocks on the size-1 handoff.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-c.resubCh:
			case <-runCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.Pause(context.Background())
			_ = c.Resume(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 400; i++ {
			snap := c.snapshot()
			assert.False(t, snap.Active && snap.Paused, "Active and Paused must never both be true")
		}
	}()
	wg.Wait()

	cancelRun()
	<-drainDone
}

// TestConsumer_Pause_Resume_Close_Race drives Pause, Resume, and Close concurrently
// under -race. pauseMu serializes Pause/Resume; this asserts the trio never panics,
// deadlocks, or races, and that Close always wins the terminal state (T53).
func TestConsumer_Pause_Resume_Close_Race(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)

	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun()

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-c.resubCh:
			case <-runCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = c.Pause(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = c.Resume(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		_ = c.Close(context.Background())
	}()
	wg.Wait()

	// Close is terminal: once closed, Pause/Resume refuse.
	require.ErrorIs(t, c.Pause(context.Background()), ErrAlreadyClosed)
	require.ErrorIs(t, c.Resume(context.Background()), ErrAlreadyClosed)

	cancelRun()
	<-drainDone
}

// TestStartPump_GracefulLocalCancel_KeepsChannelDoneOpen verifies the pump leaves
// channelDone open for a graceful local cancel (Pause): only the delivery stream
// closes, with no closeCh/cancelCh signal, so in-flight handlers survive (T53).
func TestStartPump_GracefulLocalCancel_KeepsChannelDoneOpen(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	c.paused.Store(true) // a Pause is in progress

	deliveries := make(chan amqp091.Delivery)
	closeCh := make(chan *amqp091.Error, 1)
	cancelCh := make(chan string, 1)
	var doneClosed atomic.Bool
	out := c.startPump(context.Background(), deliveries, closeCh, cancelCh, func() { doneClosed.Store(true) })

	close(deliveries) // graceful local cancel closes only the delivery stream
	_, ok := <-out
	assert.False(t, ok, "pump closes out when the delivery stream ends")
	assert.False(t, doneClosed.Load(), "a graceful local cancel must NOT close channelDone")
}

// TestStartPump_ChannelDeath_ClosesChannelDone_EvenWhenPaused closes the race the
// review flagged: a genuine channel death (closeCh fires) during the Pause
// handshake must still close channelDone so in-flight handler contexts are
// cancelled — the death signal, not the paused flag, decides (T53).
func TestStartPump_ChannelDeath_ClosesChannelDone_EvenWhenPaused(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	c.paused.Store(true) // paused, but a real death races in

	deliveries := make(chan amqp091.Delivery)
	closeCh := make(chan *amqp091.Error, 1)
	cancelCh := make(chan string, 1)
	var doneClosed atomic.Bool
	out := c.startPump(context.Background(), deliveries, closeCh, cancelCh, func() { doneClosed.Store(true) })

	// NotifyClose signals AND the delivery stream closes: whichever case the pump
	// wins, channelDone must close.
	closeCh <- &amqp091.Error{Code: 504, Reason: "channel closed"}
	close(deliveries)

	_, ok := <-out
	assert.False(t, ok)
	assert.True(t, doneClosed.Load(), "a genuine channel death must close channelDone even while paused")
}

// TestStartPump_CancelChDeath_ClosesChannelDone_EvenWhenPaused covers the symmetric
// cancelCh arm of the pump's !ok drain (LATER-83): when the delivery stream closes
// while paused AND a broker basic.cancel is buffered on cancelCh, the pump must treat
// it as a genuine death — forward the cancel reason to cancelReasonCh and close
// channelDone — not mistake it for a graceful local Pause. A regression that dropped
// the cancelCh drain (checking only closeCh) would pass the other startPump tests yet
// leak in-flight handler contexts on a real basic.cancel racing a Pause (T53).
func TestStartPump_CancelChDeath_ClosesChannelDone_EvenWhenPaused(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	c.paused.Store(true) // paused, but a real basic.cancel races in
	c.cancelReasonCh = make(chan string, 1)

	deliveries := make(chan amqp091.Delivery)
	closeCh := make(chan *amqp091.Error, 1)
	cancelCh := make(chan string, 1)
	var doneClosed atomic.Bool
	out := c.startPump(context.Background(), deliveries, closeCh, cancelCh, func() { doneClosed.Store(true) })

	// A broker basic.cancel is buffered, then the delivery stream closes: whichever arm
	// the pump wins (the main cancelCh case or the !ok drain), the outcome must be the
	// same — the reason is forwarded and channelDone closes.
	cancelCh <- "ctag-x"
	close(deliveries)

	_, ok := <-out
	assert.False(t, ok, "pump closes out when the delivery stream ends")
	assert.True(t, doneClosed.Load(), "a broker basic.cancel racing a Pause must close channelDone")
	select {
	case reason := <-c.cancelReasonCh:
		assert.Equal(t, "ctag-x", reason, "the cancel reason must be forwarded to runConsume")
	default:
		t.Fatal("the broker cancel reason was not forwarded to cancelReasonCh")
	}
}

// TestConsumer_Resume_RacesReconnect_NoDuplicateSubscribe drives a reconnect
// re-subscribe (openDeliveryCh's override path, which clears the pause under pauseMu —
// the same critical section that publishes the fresh c.live) concurrently with
// Pause/Resume and a snapshot reader, under -race (LATER-84). It guards that the
// (paused, live) handoff on the size-1 resubCh never panics and never lands in the
// forbidden "paused alongside a half-replaced channel" state — snapshot must never
// report Active && Paused both true (T53).
func TestConsumer_Resume_RacesReconnect_NoDuplicateSubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := &fakeSubChannel{deliveries: make(chan amqp091.Delivery)}
	c := pausableConsumer(t, fake)
	// Override path stands in for a reconnect re-subscribe: it clears the pause under
	// pauseMu without a broker, racing the Resume handoff on the size-1 resubCh.
	c.deliverySubOverride = &deliverySub{ch: make(chan amqp091.Delivery), done: nil}

	runCtx, cancelRun := context.WithCancel(context.Background())
	c.runCtx = runCtx
	defer cancelRun() // drains any pump Resume spawns before goleak

	// Drain resubCh so a storm of Resume calls never blocks on the size-1 handoff.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-c.resubCh:
			case <-runCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = c.Pause(context.Background())
			_ = c.Resume(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = c.openDeliveryCh(context.Background()) // reconnect re-subscribe path
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 400; i++ {
			snap := c.snapshot()
			assert.False(t, snap.Active && snap.Paused,
				"Active and Paused must never both be true under a Resume/reconnect race")
		}
	}()
	wg.Wait()

	cancelRun()
	<-drainDone
}

// TestConsumer_Pause_LeavesInFlightHandlerToFinish pins the user-facing draining
// contract end-to-end (LATER-85): a Pause issued while a handler is mid-flight must let
// that handler run to completion — its context is NOT cancelled — and InFlightHandlers
// must drain to 0. It wires the REAL pump into the consume loop so a graceful local
// cancel (Pause closes the delivery stream with no death signal) drives channelDone
// exactly as in production; a regression that cancelled in-flight handler contexts on
// Pause would fail here (T53).
func TestConsumer_Pause_LeavesInFlightHandlerToFinish(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	// HandlerTimeout > 0 selects the goroutine dispatch path, where an erroneous
	// channelDone close would cancel the handler ctx via the <-chanDone arm.
	c, err := ConsumerFor[string](conn).Queue("q").Tag("ctag-x").
		HandlerTimeout(time.Minute).Build()
	require.NoError(t, err)

	// Build the same plumbing openDeliveryCh would, and feed the pump's output into the
	// consume loop via the override so channelDone is driven by the real startPump.
	deliveries := make(chan amqp091.Delivery)
	closeCh := make(chan *amqp091.Error, 1)
	cancelCh := make(chan string, 1)
	channelDone := make(chan struct{})
	var once sync.Once
	closeChannelDone := func() { once.Do(func() { close(channelDone) }) }
	out := c.startPump(context.Background(), deliveries, closeCh, cancelCh, closeChannelDone)
	c.deliverySubOverride = &deliverySub{ch: out, done: channelDone}

	// Pause needs a live channel to issue the local basic.cancel.
	fake := &fakeSubChannel{}
	c.pauseMu.Lock()
	c.live = &liveSub{ch: fake, closeCh: closeCh, cancelCh: cancelCh, done: channelDone, closeDone: closeChannelDone}
	c.pauseMu.Unlock()

	// Deterministic proof the loop observed the closed stream (avoids a timing sleep).
	chClosed := make(chan struct{}, 1)
	c.testHookChannelClosed = func() {
		select {
		case chClosed <- struct{}{}:
		default:
		}
	}

	handlerCtxCh := make(chan context.Context, 1)
	releaseHandler := make(chan struct{})
	var handlerCompleted atomic.Bool

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(consumeCtx, func(hctx context.Context, _ string) error {
			handlerCtxCh <- hctx
			<-releaseHandler
			handlerCompleted.Store(true)
			return nil
		})
	}()

	// Deliver one message; the handler enters and blocks.
	deliveries <- amqp091.Delivery{Body: []byte(`"x"`)}
	hctx := <-handlerCtxCh
	require.Equal(t, 1, c.snapshot().InFlightHandlers, "the blocked handler is in flight")

	// Pause while the handler is mid-flight; the broker then completes the local
	// basic.cancel by closing the delivery stream.
	require.NoError(t, c.Pause(context.Background()))
	close(deliveries)
	<-chClosed // the loop has observed the closed stream (graceful path, no death)

	// The in-flight handler must be untouched: its ctx is alive and it has not returned.
	require.NoError(t, hctx.Err(), "a graceful Pause must NOT cancel an in-flight handler's context")
	assert.False(t, handlerCompleted.Load(), "the handler must still be running across the Pause")
	assert.Equal(t, 1, c.snapshot().InFlightHandlers, "the handler stays in flight until it returns")

	// Release the handler; it runs to completion and InFlightHandlers drains to 0.
	close(releaseHandler)
	require.Eventually(t, func() bool { return c.snapshot().InFlightHandlers == 0 },
		2*time.Second, 5*time.Millisecond, "InFlightHandlers must drain to 0 after the handler finishes")
	assert.True(t, handlerCompleted.Load(), "the in-flight handler ran to completion across the Pause")

	cancelConsume()
	select {
	case <-consumeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop after ctx cancel")
	}
}
