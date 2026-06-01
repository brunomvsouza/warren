package warren

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// fakeDedupeStore is an in-memory DedupeStore for tests. It records every Mark
// call (id + ttl) and can be configured to fail Seen or Mark to exercise the
// fail-open path.
type fakeDedupeStore struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	marks   []dedupeMark
	seenErr error
	markErr error

	// seenCalls counts Seen invocations so a test can prove the empty-MessageID
	// path never touches the store.
	seenCalls int
	// markCtxErr / markCtxHasDeadline capture the state of the context handed to
	// the most recent Mark call, so a test can prove Mark runs under a live,
	// bounded context even when the handler context is already cancelled.
	markCtxErr         error
	markCtxHasDeadline bool
}

type dedupeMark struct {
	id  string
	ttl time.Duration
}

func newFakeDedupeStore() *fakeDedupeStore {
	return &fakeDedupeStore{seen: map[string]struct{}{}}
}

func (s *fakeDedupeStore) Seen(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seenCalls++
	if s.seenErr != nil {
		return false, s.seenErr
	}
	_, ok := s.seen[id]
	return ok, nil
}

func (s *fakeDedupeStore) Mark(ctx context.Context, id string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markCtxErr = ctx.Err()
	_, s.markCtxHasDeadline = ctx.Deadline()
	if s.markErr != nil {
		return s.markErr
	}
	s.seen[id] = struct{}{}
	s.marks = append(s.marks, dedupeMark{id: id, ttl: ttl})
	return nil
}

func (s *fakeDedupeStore) markCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.marks)
}

func (s *fakeDedupeStore) seenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenCalls
}

// — Builder wiring —————————————————————————————————————————————————————

func TestConsumerBuilder_WithDedupe_Stored(t *testing.T) {
	conn := newFakeConsumerConn(t)
	store := newFakeDedupeStore()
	c, err := ConsumerFor[string](conn).Queue("q").WithDedupe(store, 15*time.Minute).Build()
	require.NoError(t, err)
	assert.NotNil(t, c.dedupeStore, "WithDedupe must store the dedupe store")
	assert.Equal(t, 15*time.Minute, c.dedupeTTL)
}

func TestConsumerBuilder_WithDedupe_DefaultNil(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Nil(t, c.dedupeStore, "dedupe is opt-in; the store must be nil by default")
}

// — Behaviour ——————————————————————————————————————————————————————————

func TestConsumer_WithDedupe_SkipsDuplicateMessageID(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	store := newFakeDedupeStore()
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCalls int
	var mu sync.Mutex
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return nil
		})
	}()

	const msgID = "dup-001"
	newDeliveryWithAck := func(acked chan struct{}) amqp091.Delivery {
		return amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: msgID,
			Acknowledger: &fakeAcknowledger{
				ackFn: func(_ uint64, _ bool) error { close(acked); return nil },
			},
		}
	}

	// First delivery: store empty → handler runs → Mark recorded → Ack.
	ack1 := make(chan struct{})
	deliveryCh <- newDeliveryWithAck(ack1)
	select {
	case <-ack1:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Ack")
	}

	// Second delivery, SAME MessageID: store HIT → handler skipped → Ack.
	ack2 := make(chan struct{})
	deliveryCh <- newDeliveryWithAck(ack2)
	select {
	case <-ack2:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second Ack")
	}

	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, handlerCalls, "the duplicate MessageID must skip the handler")
	require.Equal(t, 1, store.markCount(), "exactly one Mark for the first (successful) delivery")
	assert.Equal(t, msgID, store.marks[0].id)
	assert.Equal(t, time.Minute, store.marks[0].ttl, "Mark must carry the configured TTL")
}

func TestConsumer_WithDedupe_FailsOpenOnSeenError(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	warnCh := make(chan string, 1)
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		select {
		case warnCh <- msg:
		default:
		}
	}}
	store := newFakeDedupeStore()
	store.seenErr = errors.New("redis: connection refused")
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			close(handled)
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:         []byte(`"hello"`),
		MessageId:    "fail-open-001",
		Acknowledger: &fakeAcknowledger{},
	}

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("fail-open: the handler must still process the message on a store error")
	}

	select {
	case msg := <-warnCh:
		assert.Contains(t, msg, "dedupe", "a store error must log a warning")
	case <-time.After(2 * time.Second):
		t.Fatal("expected a fail-open warning to be logged")
	}

	cancel()
	<-consumeDone
}

func TestConsumer_WithDedupe_DoesNotMarkOnHandlerError(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	store := newFakeDedupeStore()
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCalls int
	var mu sync.Mutex
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return errors.New("handler failed")
		})
	}()

	const msgID = "no-mark-001"
	newDeliveryWithNack := func(nacked chan struct{}) amqp091.Delivery {
		return amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: msgID,
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, _ bool) error { close(nacked); return nil },
			},
		}
	}

	// First delivery: handler fails → Nack → NOT marked.
	nack1 := make(chan struct{})
	deliveryCh <- newDeliveryWithNack(nack1)
	select {
	case <-nack1:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Nack")
	}

	// Second delivery, SAME id: since the first was never marked, the handler must run again.
	nack2 := make(chan struct{})
	deliveryCh <- newDeliveryWithNack(nack2)
	select {
	case <-nack2:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second Nack")
	}

	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, handlerCalls, "a failed handler must not be deduped on redelivery")
	assert.Equal(t, 0, store.markCount(), "Mark must NOT be called when the handler returns an error")
}

func TestConsumer_WithDedupe_FailsOpenOnMarkError(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	warnCh := make(chan string, 1)
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		select {
		case warnCh <- msg:
		default:
		}
	}}
	store := newFakeDedupeStore()
	store.markErr = errors.New("redis: write timeout")
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCalls int
	var mu sync.Mutex
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return nil
		})
	}()

	// Handler succeeds → Mark is attempted and fails. Fail-open means the delivery
	// is still acked (the handler ran), only a warning is emitted.
	acked := make(chan struct{})
	deliveryCh <- amqp091.Delivery{
		Body:      []byte(`"hello"`),
		MessageId: "mark-fail-001",
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error { close(acked); return nil },
		},
	}
	select {
	case <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("a Mark error must fail open: a successful handler's delivery is still acked")
	}

	select {
	case msg := <-warnCh:
		assert.Contains(t, msg, "Mark", "a Mark failure must log a fail-open warning naming the failed op")
	case <-time.After(2 * time.Second):
		t.Fatal("expected a Mark fail-open warning to be logged")
	}

	cancel()
	<-consumeDone

	assert.Equal(t, 0, store.markCount(), "a failed Mark records nothing")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, handlerCalls, "the handler runs exactly once for the successful delivery")
}

func TestConsumer_WithDedupe_EmptyMessageIDBypassesStore(t *testing.T) {
	conn := newFakeConsumerConn(t)
	store := newFakeDedupeStore()
	// Arm both store methods to fail hard: were the bypass to consult the store at
	// all, the error would be observable (a fail-open warning, or a wrong verdict).
	store.seenErr = errors.New("Seen must never be called for an empty MessageID")
	store.markErr = errors.New("Mark must never be called for an empty MessageID")
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	var handlerRan bool
	wrapped := c.wrapDedupe(func(_ context.Context, _ *Delivery[string]) error {
		handlerRan = true
		return nil
	})

	body := "x"
	d := NewDeliveryFixture(DeliveryFixture[string]{Body: &body}) // MessageID left empty
	require.NoError(t, wrapped(context.Background(), d),
		"an empty-MessageID delivery passes straight to the handler")

	assert.True(t, handlerRan, "the handler must run for a delivery that cannot be deduped")
	assert.Equal(t, 0, store.seenCount(), "Seen must not be consulted without a MessageID key")
	assert.Equal(t, 0, store.markCount(), "Mark must not be called without a MessageID key")
}

func TestConsumer_WithDedupe_MarkRunsUnderLiveContext_WhenHandlerCtxCancelled(t *testing.T) {
	conn := newFakeConsumerConn(t)
	store := newFakeDedupeStore()
	c, err := ConsumerFor[string](conn).Queue("testq").WithDedupe(store, time.Minute).Build()
	require.NoError(t, err)

	wrapped := c.wrapDedupe(func(_ context.Context, _ *Delivery[string]) error {
		return nil // handler succeeds → Mark is attempted
	})

	// Model a handler whose context is already cancelled by the time Mark runs —
	// e.g. a tight HandlerTimeout that elapsed, or a shutdown. Mark must NOT inherit
	// that dead context, or a successful handler silently fails to record the id and
	// fails open to a future duplicate.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	body := "x"
	d := NewDeliveryFixture(DeliveryFixture[string]{Body: &body, MessageID: "mark-ctx-001"})
	require.NoError(t, wrapped(ctx, d))

	require.Equal(t, 1, store.markCount(), "a successful handler must record the id even when its ctx is dead")
	assert.NoError(t, store.markCtxErr,
		"Mark must receive a live (non-cancelled) context detached from the handler ctx")
	assert.True(t, store.markCtxHasDeadline,
		"Mark's detached context must still carry a bounded deadline so a wedged store cannot block forever")
}
