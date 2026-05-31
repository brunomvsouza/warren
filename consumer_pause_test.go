package warren

import (
	"context"
	"errors"
	"sync"
	"testing"

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
}

func (f *fakeSubChannel) Cancel(consumer string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, consumer)
	return f.cancelErr
}

func (f *fakeSubChannel) Consume(_, consumer string, _, _, _, _ bool, _ amqp091.Table) (<-chan amqp091.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumeCalls = append(f.consumeCalls, consumer)
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	if f.deliveries == nil {
		f.deliveries = make(chan amqp091.Delivery)
	}
	return f.deliveries, nil
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
// Pause/Resume can be exercised without a broker.
func pausableConsumer(t *testing.T, fake *fakeSubChannel) *Consumer[string] {
	t.Helper()
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Tag("ctag-x").Build()
	require.NoError(t, err)
	c.started.Store(true)
	c.resubCh = make(chan deliverySub, 1)
	c.cancelReasonCh = make(chan string, 1)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // drains the resume pump goroutine

	require.NoError(t, c.Resume(ctx))
	assert.Equal(t, []string{"ctag-x"}, fake.consumes(), "Resume re-issues basic.consume with the consumer tag")
	assert.False(t, c.snapshot().Paused, "Health clears Paused after Resume")

	select {
	case sub := <-c.resubCh:
		assert.NotNil(t, sub.ch, "Resume must hand the running loop a fresh subscription")
	default:
		t.Fatal("Resume must push a new subscription to resubCh")
	}
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
