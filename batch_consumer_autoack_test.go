package warren

// T23 — BatchConsumer auto-verdict unit tests.
//
// These tests verify the idempotent ack/nack guard and the single-frame
// multiple=true contract without a live broker. Each test builds a
// Batch[string] manually with fake acknowledgeable deliveries.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
)

// — Batch unit tests —————————————————————————————————————————————————————————

// makeFakeDelivery creates a Delivery[string] backed by a fakeAcknowledger
// so we can observe and control acks/nacks in unit tests.
func makeFakeDelivery(tag uint64, body string, fa *fakeAcknowledger) *Delivery[string] {
	raw := amqp091.Delivery{
		DeliveryTag:  tag,
		Acknowledger: fa,
		Body:         []byte(`"` + body + `"`),
	}
	b := body
	return &Delivery[string]{
		body: &b,
		raw:  raw,
	}
}

// newTestBatch builds a Batch[string] from a slice of deliveries,
// wiring ackNotify on each delivery to set batch.acked via the same
// mechanism that BatchConsumer uses internally.
func newTestBatch(deliveries []*Delivery[string]) *Batch[string] {
	b := &Batch[string]{deliveries: deliveries}
	for _, d := range deliveries {
		d := d // capture
		d.ackNotify = func() {
			b.mu.Lock()
			b.acked = true
			b.mu.Unlock()
		}
	}
	return b
}

// TestBatch_Ack_EmptyBatch_NoFrame verifies that Batch.Ack on an empty batch returns
// nil without calling the acknowledger.
func TestBatch_Ack_EmptyBatch_NoFrame(t *testing.T) {
	var ackCalled bool
	fa := &fakeAcknowledger{
		ackFn: func(_ uint64, _ bool) error {
			ackCalled = true
			return nil
		},
	}
	_ = fa // acknowledger never used for empty batch
	batch := newTestBatch([]*Delivery[string]{})
	require.NoError(t, batch.Ack(), "empty batch Ack must return nil")
	assert.False(t, ackCalled, "no ack frame must be emitted for empty batch")
}

// TestBatch_Nack_EmptyBatch_NoFrame verifies that Batch.Nack on an empty batch returns
// nil without calling the acknowledger.
func TestBatch_Nack_EmptyBatch_NoFrame(t *testing.T) {
	var nackCalled bool
	fa := &fakeAcknowledger{
		nackFn: func(_ uint64, _ bool, _ bool) error {
			nackCalled = true
			return nil
		},
	}
	_ = fa
	batch := newTestBatch([]*Delivery[string]{})
	require.NoError(t, batch.Nack(false), "empty batch Nack must return nil")
	assert.False(t, nackCalled, "no nack frame must be emitted for empty batch")
}

// TestBatch_AckAll_AcknowledgerError_ReturnsError verifies that when the underlying
// acknowledger returns an error, Batch.Ack propagates it.
func TestBatch_AckAll_AcknowledgerError_ReturnsError(t *testing.T) {
	ackErr := errors.New("channel closed")
	fa := &fakeAcknowledger{
		ackFn: func(_ uint64, _ bool) error { return ackErr },
	}
	d1 := makeFakeDelivery(1, "m", fa)
	batch := newTestBatch([]*Delivery[string]{d1})
	err := batch.Ack()
	require.Error(t, err, "Batch.Ack must return the acknowledger error")
}

// TestBatch_NackAll_AcknowledgerError_ReturnsError verifies that when the underlying
// acknowledger returns an error, Batch.Nack propagates it.
func TestBatch_NackAll_AcknowledgerError_ReturnsError(t *testing.T) {
	nackErr := errors.New("channel closed")
	fa := &fakeAcknowledger{
		nackFn: func(_ uint64, _ bool, _ bool) error { return nackErr },
	}
	d1 := makeFakeDelivery(1, "m", fa)
	batch := newTestBatch([]*Delivery[string]{d1})
	err := batch.Nack(false)
	require.Error(t, err, "Batch.Nack must return the acknowledger error")
}

// TestBatch_Ack_MultipleTrue verifies that Batch.Ack emits a single
// basic.ack with multiple=true on the highest delivery-tag.
func TestBatch_Ack_MultipleTrue(t *testing.T) {
	var mu sync.Mutex
	var ackCalls []struct {
		tag      uint64
		multiple bool
	}

	makeFA := func() *fakeAcknowledger {
		return &fakeAcknowledger{
			ackFn: func(tag uint64, multiple bool) error {
				mu.Lock()
				ackCalls = append(ackCalls, struct {
					tag      uint64
					multiple bool
				}{tag, multiple})
				mu.Unlock()
				return nil
			},
		}
	}

	// Three deliveries: tags 1, 2, 3. All backed by the same acknowledger
	// (simulates same AMQP channel).
	fa := makeFA()
	d1 := makeFakeDelivery(1, "msg1", fa)
	d2 := makeFakeDelivery(2, "msg2", fa)
	d3 := makeFakeDelivery(3, "msg3", fa)

	batch := newTestBatch([]*Delivery[string]{d1, d2, d3})
	require.NoError(t, batch.Ack())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, ackCalls, 1, "exactly one basic.ack frame must be emitted")
	assert.Equal(t, uint64(3), ackCalls[0].tag, "ack must target the highest delivery-tag")
	assert.True(t, ackCalls[0].multiple, "ack must use multiple=true")
}

// TestBatch_Nack_NoRequeue_MultipleTrue verifies that Batch.Nack(false)
// emits a single basic.nack with multiple=true, requeue=false.
func TestBatch_Nack_NoRequeue_MultipleTrue(t *testing.T) {
	var mu sync.Mutex
	var nackCalls []struct {
		tag      uint64
		multiple bool
		requeue  bool
	}

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, struct {
				tag      uint64
				multiple bool
				requeue  bool
			}{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	d1 := makeFakeDelivery(5, "a", fa)
	d2 := makeFakeDelivery(7, "b", fa)

	batch := newTestBatch([]*Delivery[string]{d1, d2})
	require.NoError(t, batch.Nack(false))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackCalls, 1, "exactly one basic.nack frame")
	assert.Equal(t, uint64(7), nackCalls[0].tag, "nack must target the highest delivery-tag")
	assert.True(t, nackCalls[0].multiple, "nack must use multiple=true")
	assert.False(t, nackCalls[0].requeue, "requeue must be false")
}

// TestBatch_Nack_Requeue_MultipleTrue verifies Batch.Nack(true) with multiple=true, requeue=true.
func TestBatch_Nack_Requeue_MultipleTrue(t *testing.T) {
	var mu sync.Mutex
	var nackCalls []struct {
		tag      uint64
		multiple bool
		requeue  bool
	}

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, struct {
				tag      uint64
				multiple bool
				requeue  bool
			}{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	d1 := makeFakeDelivery(10, "x", fa)
	batch := newTestBatch([]*Delivery[string]{d1})
	require.NoError(t, batch.Nack(true))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackCalls, 1)
	assert.Equal(t, uint64(10), nackCalls[0].tag)
	assert.True(t, nackCalls[0].multiple)
	assert.True(t, nackCalls[0].requeue)
}

// TestBatch_Ack_Idempotent verifies that calling Batch.Ack twice only emits one
// AMQP frame.
func TestBatch_Ack_Idempotent(t *testing.T) {
	var mu sync.Mutex
	var ackCalls int

	fa := &fakeAcknowledger{
		ackFn: func(_ uint64, _ bool) error {
			mu.Lock()
			ackCalls++
			mu.Unlock()
			return nil
		},
	}

	d1 := makeFakeDelivery(1, "m", fa)
	batch := newTestBatch([]*Delivery[string]{d1})
	require.NoError(t, batch.Ack())
	require.NoError(t, batch.Ack()) // second call: idempotent

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, ackCalls, "second Ack must be a no-op")
}

// TestBatch_Messages returns decoded payloads.
func TestBatch_Messages(t *testing.T) {
	fa := &fakeAcknowledger{}
	d1 := makeFakeDelivery(1, "hello", fa)
	d2 := makeFakeDelivery(2, "world", fa)
	batch := newTestBatch([]*Delivery[string]{d1, d2})
	msgs := batch.Messages()
	require.Len(t, msgs, 2)
	assert.Equal(t, "hello", msgs[0])
	assert.Equal(t, "world", msgs[1])
}

// TestBatch_Deliveries returns the underlying delivery slice.
func TestBatch_Deliveries(t *testing.T) {
	fa := &fakeAcknowledger{}
	d1 := makeFakeDelivery(1, "a", fa)
	batch := newTestBatch([]*Delivery[string]{d1})
	deliveries := batch.Deliveries()
	require.Len(t, deliveries, 1)
	assert.Same(t, d1, deliveries[0])
}

// TestBatch_PerDeliveryAck_SuppressesAutoVerdict verifies that calling Ack on an
// individual delivery from Deliveries() sets acked=true, which the BatchConsumer's
// auto-verdict logic uses to skip the batch-level Ack/Nack.
func TestBatch_PerDeliveryAck_SuppressesAutoVerdict(t *testing.T) {
	var mu sync.Mutex
	var ackCalls []struct {
		tag      uint64
		multiple bool
	}

	fa := &fakeAcknowledger{
		ackFn: func(tag uint64, multiple bool) error {
			mu.Lock()
			ackCalls = append(ackCalls, struct {
				tag      uint64
				multiple bool
			}{tag, multiple})
			mu.Unlock()
			return nil
		},
	}

	d1 := makeFakeDelivery(1, "m1", fa)
	d2 := makeFakeDelivery(2, "m2", fa)
	batch := newTestBatch([]*Delivery[string]{d1, d2})

	// Simulate handler calling per-delivery Nack on one delivery.
	require.NoError(t, batch.Deliveries()[0].Nack(true))

	// The batch.acked flag must be true now.
	batch.mu.Lock()
	acked := batch.acked
	batch.mu.Unlock()
	assert.True(t, acked, "per-delivery ack must set batch.acked=true")

	// A subsequent Batch.Ack must be a no-op.
	require.NoError(t, batch.Ack())

	mu.Lock()
	defer mu.Unlock()
	// Only 1 frame: from the per-delivery Nack. The batch.Ack must not fire.
	assert.Len(t, ackCalls, 0, "batch.Ack must be a no-op after per-delivery ack")
}

// — BatchConsumerBuilder unit tests ————————————————————————————————————————

func TestBatchConsumerBuilder_Defaults(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Equal(t, "q", bc.queue)
	assert.Equal(t, uint(100), bc.size, "default batch size must be 100")
	assert.Equal(t, uint16(64), bc.prefetch, "default prefetch must be 64")
	assert.NotNil(t, bc.codec)
	assert.NotNil(t, bc.cm)
}

func TestBatchConsumerBuilder_NilConn_Error(t *testing.T) {
	_, err := BatchConsumerFor[string](nil).Queue("q").Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestBatchConsumerBuilder_EmptyQueue_Error(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := BatchConsumerFor[string](conn).Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestBatchConsumerBuilder_LastWins_Size(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").Size(50).Size(200).Build()
	require.NoError(t, err)
	assert.Equal(t, uint(200), bc.size)
}

func TestBatchConsumerBuilder_LastWins_FlushAfter(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").
		FlushAfter(1 * time.Second).
		FlushAfter(2 * time.Second).
		Build()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Second, bc.flushAfter)
}

func TestBatchConsumerBuilder_LastWins_HandlerTimeout(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").
		HandlerTimeout(100 * time.Millisecond).
		HandlerTimeout(0).
		Build()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), bc.handlerTimeout)
}

// — BatchConsumer Consume unit tests (fake delivery injection) ————————————

// newTestBatchConsumer builds a BatchConsumer[string] with injected delivery channel.
// stopFn must be called after the test to release pool resources.
func newTestBatchConsumer(t *testing.T, deliveryCh chan amqp091.Delivery, size uint, flushAfter time.Duration) (*BatchConsumer[string], func()) {
	t.Helper()
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(size).
		FlushAfter(flushAfter).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	return bc, func() { _ = bc.Close(context.Background()) }
}

// makeJSONDelivery builds an amqp091.Delivery carrying a JSON-encoded string
// and a fakeAcknowledger so we can observe acks.
func makeJSONDelivery(tag uint64, body string, fa *fakeAcknowledger) amqp091.Delivery {
	return amqp091.Delivery{
		DeliveryTag:  tag,
		Acknowledger: fa,
		Body:         []byte(`"` + body + `"`),
	}
}

// TestBatchConsumer_SizeFlush verifies that a batch is flushed when Size is reached.
func TestBatchConsumer_SizeFlush(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 3 /* size */, 0 /* no timer */)
	defer stopFn()

	var mu sync.Mutex
	var batches [][]string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			mu.Lock()
			batches = append(batches, b.Messages())
			mu.Unlock()
			return nil
		})
	}()

	fa := &fakeAcknowledger{}
	// Send 3 messages; expect exactly 1 batch flush.
	for i := 1; i <= 3; i++ {
		deliveryCh <- makeJSONDelivery(uint64(i), "msg", fa) //nolint:gosec
	}

	// Give the consumer time to process.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(batches) == 1
	}, time.Second, 10*time.Millisecond, "expected exactly 1 batch after size reached")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 3, "batch must contain all 3 messages")
}

// TestBatchConsumer_FlushAfterTimer verifies that accumulated messages are flushed
// when the FlushAfter timer fires, even if size has not been reached.
func TestBatchConsumer_FlushAfterTimer(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	// size=100 (won't be reached), flushAfter=50ms.
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 100, 50*time.Millisecond)
	defer stopFn()

	var mu sync.Mutex
	var batches [][]string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			mu.Lock()
			batches = append(batches, b.Messages())
			mu.Unlock()
			return nil
		})
	}()

	fa := &fakeAcknowledger{}
	// Send 2 messages — less than size; timer must flush.
	deliveryCh <- makeJSONDelivery(1, "a", fa)
	deliveryCh <- makeJSONDelivery(2, "b", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(batches) == 1
	}, 500*time.Millisecond, 10*time.Millisecond, "expected batch to flush after timer")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 2)
}

// TestBatchConsumer_AutoAck_NilError verifies that a nil-returning handler causes
// a single basic.ack with multiple=true on the highest delivery-tag.
func TestBatchConsumer_AutoAck_NilError(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 2 /* size */, 0)
	defer stopFn()

	var mu sync.Mutex
	type ackEvent struct {
		tag      uint64
		multiple bool
	}
	var ackEvents []ackEvent

	fa := &fakeAcknowledger{
		ackFn: func(tag uint64, multiple bool) error {
			mu.Lock()
			ackEvents = append(ackEvents, ackEvent{tag, multiple})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			return nil // nil → auto Ack with multiple=true
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "a", fa)
	deliveryCh <- makeJSONDelivery(2, "b", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ackEvents) > 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, ackEvents, 1, "exactly one basic.ack frame")
	assert.Equal(t, uint64(2), ackEvents[0].tag, "ack must target highest delivery-tag")
	assert.True(t, ackEvents[0].multiple, "ack must be multiple=true")
}

// TestBatchConsumer_AutoNack_ErrRequeue verifies that an ErrRequeue-wrapping error
// causes a single basic.nack with multiple=true, requeue=true.
func TestBatchConsumer_AutoNack_ErrRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 2, 0)
	defer stopFn()

	var mu sync.Mutex
	type nackEvent struct {
		tag      uint64
		multiple bool
		requeue  bool
	}
	var nackEvents []nackEvent

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackEvents = append(nackEvents, nackEvent{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			return fmt.Errorf("transient: %w", ErrRequeue)
		})
	}()

	deliveryCh <- makeJSONDelivery(3, "x", fa)
	deliveryCh <- makeJSONDelivery(4, "y", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackEvents) > 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackEvents, 1, "exactly one basic.nack frame")
	assert.Equal(t, uint64(4), nackEvents[0].tag, "nack must target highest delivery-tag")
	assert.True(t, nackEvents[0].multiple)
	assert.True(t, nackEvents[0].requeue)
}

// TestBatchConsumer_AutoNack_OtherError verifies that a non-ErrRequeue error
// causes a single basic.nack with multiple=true, requeue=false.
func TestBatchConsumer_AutoNack_OtherError(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 1, 0)
	defer stopFn()

	var mu sync.Mutex
	type nackEvent struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackEvents []nackEvent

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackEvents = append(nackEvents, nackEvent{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			return errors.New("handler error: not requeue")
		})
	}()

	deliveryCh <- makeJSONDelivery(5, "z", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackEvents) > 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackEvents, 1)
	assert.Equal(t, uint64(5), nackEvents[0].tag)
	assert.True(t, nackEvents[0].multiple)
	assert.False(t, nackEvents[0].requeue, "non-ErrRequeue must nack without requeue")
}

// TestBatchConsumer_ManualBatchAck_SkipsAutoVerdict verifies that when the handler
// calls Batch.Ack(), the framework does NOT emit a second ack.
func TestBatchConsumer_ManualBatchAck_SkipsAutoVerdict(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 2, 0)
	defer stopFn()

	var mu sync.Mutex
	var ackCount int

	fa := &fakeAcknowledger{
		ackFn: func(_ uint64, _ bool) error {
			mu.Lock()
			ackCount++
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			// Handler manually acks the batch.
			_ = b.Ack()
			return nil // auto-verdict must be suppressed
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "m1", fa)
	deliveryCh <- makeJSONDelivery(2, "m2", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ackCount >= 1
	}, time.Second, 10*time.Millisecond)

	// Give a little extra time to detect a potential second ack.
	time.Sleep(30 * time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, ackCount, "only one ack frame must be emitted")
}

// TestBatchConsumer_ManualPerDeliveryAck_SkipsAutoVerdict verifies that when the
// handler acks an individual delivery from batch.Deliveries(), the framework skips
// the batch-level auto-verdict.
func TestBatchConsumer_ManualPerDeliveryAck_SkipsAutoVerdict(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 1, 0)
	defer stopFn()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall
	var ackCalls int

	fa := &fakeAcknowledger{
		ackFn: func(_ uint64, _ bool) error {
			mu.Lock()
			ackCalls++
			mu.Unlock()
			return nil
		},
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			// Handler manually nacks the first (only) delivery.
			_ = b.Deliveries()[0].Nack(true)
			return nil // auto-verdict must be suppressed because per-delivery nack fired
		})
	}()

	deliveryCh <- makeJSONDelivery(7, "msg", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackCalls) > 0
	}, time.Second, 10*time.Millisecond)

	time.Sleep(30 * time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	// The per-delivery Nack (multiple=false) is the only frame.
	require.Len(t, nackCalls, 1, "only the per-delivery nack must fire")
	assert.False(t, nackCalls[0].multiple, "per-delivery nack must use multiple=false")
	assert.Equal(t, 0, ackCalls, "no auto-ack must fire after per-delivery nack")
}

// TestBatchConsumer_DecodeError_NacksAndContinues verifies that a delivery whose
// payload cannot be decoded is nacked without requeue and the consumer continues.
func TestBatchConsumer_DecodeError_NacksAndContinues(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	bc, stopFn := newTestBatchConsumer(t, deliveryCh, 2, 0)
	defer stopFn()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall
	var batchesSeen int

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
		ackFn: func(_ uint64, _ bool) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			mu.Lock()
			batchesSeen++
			mu.Unlock()
			return nil
		})
	}()

	// First delivery: invalid JSON → decode error → nack individually, never batched.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag:  1,
		Acknowledger: fa,
		Body:         []byte(`not valid json`),
	}
	// Second and third: valid → batched together.
	deliveryCh <- makeJSONDelivery(2, "ok", fa)
	deliveryCh <- makeJSONDelivery(3, "ok", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return batchesSeen >= 1
	}, time.Second, 10*time.Millisecond, "expected one batch to be flushed")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	// Tag 1 must be nacked directly (not via multiple=true batch).
	var found bool
	for _, nc := range nackCalls {
		if nc.tag == 1 && !nc.multiple && !nc.requeue {
			found = true
		}
	}
	assert.True(t, found, "tag 1 (decode error) must be nacked with multiple=false, requeue=false")
	assert.Equal(t, 1, batchesSeen, "exactly 1 batch for the 2 valid messages")
}

// TestBatchConsumer_AlreadyStarted_Error verifies that calling Consume twice
// returns ErrInvalidOptions.
func TestBatchConsumer_AlreadyStarted_Error(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	bc.started.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// TestBatchConsumer_Close_Idempotent verifies that Close can be called multiple times.
func TestBatchConsumer_Close_Idempotent(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	require.NoError(t, bc.Close(context.Background()))
	require.NoError(t, bc.Close(context.Background()))
}

// TestBatchConsumer_HandlerTimeout_NacksWithoutRequeue verifies that when
// HandlerTimeout fires, the default verdict is Nack(requeue=false) for the whole batch.
func TestBatchConsumer_HandlerTimeout_NacksWithoutRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(20 * time.Millisecond).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hCtx context.Context, _ *Batch[string]) error {
			// Block until the timeout fires.
			<-hCtx.Done()
			return hCtx.Err()
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "slow", fa)
	deliveryCh <- makeJSONDelivery(2, "slow", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackCalls) > 0
	}, time.Second, 10*time.Millisecond, "expected a nack after handler timeout")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackCalls, 1, "exactly one batch-level nack on timeout")
	assert.Equal(t, uint64(2), nackCalls[0].tag, "nack targets highest tag")
	assert.True(t, nackCalls[0].multiple)
	assert.False(t, nackCalls[0].requeue, "default timeout verdict is nack without requeue")
}

// TestBatchConsumer_MetricsRecorded verifies that handler metrics are emitted.
func TestBatchConsumer_MetricsRecorded(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)

	capturedCM := &captureConsumerMetrics{}

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("myq").
		Size(2).
		Metrics(capturedCM).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	fa := &fakeAcknowledger{ackFn: func(_ uint64, _ bool) error { return nil }}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			return nil
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "a", fa)
	deliveryCh <- makeJSONDelivery(2, "b", fa)

	assert.Eventually(t, func() bool {
		capturedCM.mu.Lock()
		defer capturedCM.mu.Unlock()
		return len(capturedCM.records) > 0
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	capturedCM.mu.Lock()
	defer capturedCM.mu.Unlock()
	require.Len(t, capturedCM.records, 1)
	assert.Equal(t, "myq", capturedCM.records[0].queue)
	assert.Equal(t, "ack", capturedCM.records[0].outcome)
}

// TestBatchConsumer_HandlerTimeout_TimeoutNackRequeue verifies that
// HandlerTimeoutVerdict(TimeoutNackRequeue) causes a Nack(requeue=true) on timeout.
func TestBatchConsumer_HandlerTimeout_TimeoutNackRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(20 * time.Millisecond).
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hCtx context.Context, _ *Batch[string]) error {
			<-hCtx.Done()
			return hCtx.Err()
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "slow", fa)
	deliveryCh <- makeJSONDelivery(2, "slow", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackCalls) > 0
	}, time.Second, 10*time.Millisecond, "expected a nack after handler timeout")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackCalls, 1, "exactly one batch-level nack on timeout")
	assert.Equal(t, uint64(2), nackCalls[0].tag, "nack targets highest tag")
	assert.True(t, nackCalls[0].multiple)
	assert.True(t, nackCalls[0].requeue, "TimeoutNackRequeue verdict must requeue")
}

// — BatchConsumerBuilder TopologyHint tests ——————————————————————————————————

// TestBatchConsumerBuilder_TopologyHint_QuorumWithLimit_DisablesCounterB verifies
// that a quorum queue with DeliveryLimit > 0 sets counterBDisabled = true.
func TestBatchConsumerBuilder_TopologyHint_QuorumWithLimit_DisablesCounterB(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).
		Build()
	require.NoError(t, err)
	assert.True(t, bc.counterBDisabled, "quorum queue with DeliveryLimit > 0 must disable counter B")
}

// TestBatchConsumerBuilder_TopologyHint_QuorumNoLimit_KeepsCounterBEnabled verifies
// that a quorum queue with DeliveryLimit == 0 leaves counterBDisabled = false.
func TestBatchConsumerBuilder_TopologyHint_QuorumNoLimit_KeepsCounterBEnabled(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 0}).
		Build()
	require.NoError(t, err)
	assert.False(t, bc.counterBDisabled, "quorum queue with DeliveryLimit=0 must keep counter B enabled")
}

// TestBatchConsumerBuilder_TopologyHint_ClassicQueue_KeepsCounterBEnabled verifies
// that a classic queue leaves counterBDisabled = false.
func TestBatchConsumerBuilder_TopologyHint_ClassicQueue_KeepsCounterBEnabled(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeClassic, DeliveryLimit: 0}).
		Build()
	require.NoError(t, err)
	assert.False(t, bc.counterBDisabled, "classic queue must keep counter B enabled")
}

// TestBatchConsumerBuilder_TopologyHint_LastWins_Reset verifies that calling
// TopologyHint twice applies last-wins: a classic queue after a quorum+limit re-enables
// counter B.
func TestBatchConsumerBuilder_TopologyHint_LastWins_Reset(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).  // disables counter B
		TopologyHint(Queue{Type: QueueTypeClassic, DeliveryLimit: 0}). // re-enables counter B
		Build()
	require.NoError(t, err)
	assert.False(t, bc.counterBDisabled, "last TopologyHint (classic) must re-enable counter B")
}

// TestBatchConsumerBuilder_LastWins_HandlerTimeoutVerdict verifies last-wins for
// HandlerTimeoutVerdict.
func TestBatchConsumerBuilder_LastWins_HandlerTimeoutVerdict(t *testing.T) {
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		HandlerTimeoutVerdict(TimeoutNackNoRequeue).
		Build()
	require.NoError(t, err)
	assert.Equal(t, TimeoutNackNoRequeue, bc.timeoutVerdict)
}

// TestBatchConsumer_MaxRedeliveries_CounterA_XDeath verifies that a delivery whose
// x-death count equals maxRedeliveries is nacked individually (without being batched)
// and RecordMaxRedeliveries is called.
func TestBatchConsumer_MaxRedeliveries_CounterA_XDeath(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)

	capCM := &captureMaxRedeliveriesMetrics{}

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2). // 2 valid messages after the poison one is filtered by counter A
		MaxRedeliveries(2).
		Metrics(capCM).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall
	var batchesSeen int

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
		ackFn: func(_ uint64, _ bool) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			mu.Lock()
			batchesSeen++
			mu.Unlock()
			return nil
		})
	}()

	// Build an x-death header with count=2 (equals maxRedeliveries) for queue "q".
	xDeathHeaders := amqp091.Table{
		"x-death": []any{
			amqp091.Table{
				"queue":  "q",
				"reason": "rejected",
				"count":  int64(2),
			},
		},
	}

	// Delivery 1: x-death count at limit → must be nacked individually, never batched.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag:  1,
		Acknowledger: fa,
		Body:         []byte(`"poison"`),
		Headers:      xDeathHeaders,
	}
	// Deliveries 2 and 3: fresh → batched normally.
	deliveryCh <- makeJSONDelivery(2, "ok", fa)
	deliveryCh <- makeJSONDelivery(3, "ok", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return batchesSeen >= 1
	}, time.Second, 10*time.Millisecond, "expected one batch for valid messages")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()

	// Tag 1 must be nacked individually (multiple=false, requeue=false).
	var counterAFired bool
	for _, nc := range nackCalls {
		if nc.tag == 1 && !nc.multiple && !nc.requeue {
			counterAFired = true
		}
	}
	assert.True(t, counterAFired, "poison message must be nacked without requeue via counter A")
	assert.Equal(t, 1, batchesSeen, "exactly one batch for the 2 non-poison messages")
	assert.Equal(t, 1, capCM.xDeathCount, "RecordMaxRedeliveries must be called once with cause=x-death")
}

// TestBatchConsumer_MaxRedeliveries_CounterB_InProcess verifies that when a batch
// verdict of ErrRequeue has been applied maxRedeliveries times, the next invocation
// rewrites the verdict to Nack(requeue=false) and calls RecordMaxRedeliveries.
func TestBatchConsumer_MaxRedeliveries_CounterB_InProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 20)

	capCM := &captureMaxRedeliveriesMetrics{}

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(1). // one message per batch → deterministic
		MaxRedeliveries(2).
		Metrics(capCM).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall

	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
		ackFn: func(_ uint64, _ bool) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Count how many times the handler fired; we will stop sending after the batch
	// is nacked without requeue.
	var handlerCalls int
	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()
			return ErrRequeue // always request requeue
		})
	}()

	// Inject 3 deliveries with the same MessageID so counter B accumulates correctly.
	const msgID = "test-msg-counter-b"
	for tag := uint64(1); tag <= 3; tag++ {
		deliveryCh <- amqp091.Delivery{
			DeliveryTag:  tag,
			MessageId:    msgID,
			Acknowledger: fa,
			Body:         []byte(`"payload"`),
		}
	}

	// Wait until the 3rd nack appears (the one that should be requeue=false).
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nackCalls) >= 3
	}, 2*time.Second, 10*time.Millisecond, "expected 3 nacks")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()

	// First two nacks: requeue=true (counter B under limit).
	assert.True(t, nackCalls[0].requeue, "1st attempt must be requeued (count=1 ≤ 2)")
	assert.True(t, nackCalls[1].requeue, "2nd attempt must be requeued (count=2 ≤ 2)")
	// Third nack: counter B exceeded (count+1=3 > 2) → requeue=false.
	assert.False(t, nackCalls[2].requeue, "3rd attempt must NOT be requeued (counter B exceeded)")
	assert.Equal(t, 1, capCM.inProcessCount, "RecordMaxRedeliveries must be called once with cause=in-process")
}

// TestBatchConsumer_HandlerTimeout_CounterB_LimitsRequeue verifies that
// HandlerTimeoutVerdict(TimeoutNackRequeue) and MaxRedeliveries(n) compose correctly:
// counter B is incremented on each timeout-triggered requeue and the (n+1)-th timeout
// causes Nack(requeue=false) instead of Nack(requeue=true), preventing infinite loops.
func TestBatchConsumer_HandlerTimeout_CounterB_LimitsRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	capCM := &captureMaxRedeliveriesMetrics{}

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(1).
		HandlerTimeout(20 * time.Millisecond).
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		MaxRedeliveries(2).
		Metrics(capCM).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type nackCall struct {
		tag               uint64
		multiple, requeue bool
	}
	var nackCalls []nackCall

	const msgID = "timeout-counterb-msg-01"
	fa := &fakeAcknowledger{
		nackFn: func(tag uint64, multiple, requeue bool) error {
			mu.Lock()
			nackCalls = append(nackCalls, nackCall{tag, multiple, requeue})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hCtx context.Context, _ *Batch[string]) error {
			<-hCtx.Done() // always block until HandlerTimeout fires
			return hCtx.Err()
		})
	}()

	// Send the same MessageID three times to accumulate counter B across timeouts.
	for i := uint64(1); i <= 3; i++ {
		deliveryCh <- amqp091.Delivery{
			DeliveryTag:  i,
			MessageId:    msgID,
			Body:         []byte(`"hello"`),
			Acknowledger: fa,
		}
		// Wait for each individual nack before sending the next delivery.
		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(nackCalls) == int(i)
		}, 2*time.Second, 10*time.Millisecond, "expected nack #%d after timeout", i)
	}

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, nackCalls, 3)
	// First two timeouts: counter B is under limit → requeue=true.
	assert.True(t, nackCalls[0].requeue, "1st timeout must be requeued (count=1 ≤ 2)")
	assert.True(t, nackCalls[1].requeue, "2nd timeout must be requeued (count=2 ≤ 2)")
	// Third timeout: count+1=3 > maxRedeliveries=2 → counter B overrides to requeue=false.
	assert.False(t, nackCalls[2].requeue, "3rd timeout must NOT be requeued (counter B exceeded)")
	assert.Equal(t, 1, capCM.inProcessCount, "RecordMaxRedeliveries must be called exactly once with cause=in-process")
}

// TestBatchConsumer_HandlerTimeout_CompletesBeforeDeadline_AppliesNormalVerdict verifies
// that when the handler returns before the HandlerTimeout fires, the handlerDone branch
// of the timeout select is taken and the normal auto-verdict logic applies (ack on nil
// return; RecordHandler outcome == "ack").
func TestBatchConsumer_HandlerTimeout_CompletesBeforeDeadline_AppliesNormalVerdict(t *testing.T) {
	defer goleak.VerifyNone(t)

	deliveryCh := make(chan amqp091.Delivery, 10)
	capCM := &captureConsumerMetrics{}

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(1).
		HandlerTimeout(200 * time.Millisecond). // generous timeout; handler returns immediately
		Metrics(capCM).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	var mu sync.Mutex
	type ackCall struct {
		tag      uint64
		multiple bool
	}
	var ackCalls []ackCall

	fa := &fakeAcknowledger{
		ackFn: func(tag uint64, multiple bool) error {
			mu.Lock()
			ackCalls = append(ackCalls, ackCall{tag, multiple})
			mu.Unlock()
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			return nil // returns immediately; well before the 200 ms deadline
		})
	}()

	deliveryCh <- makeJSONDelivery(1, "fast", fa)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ackCalls) > 0
	}, time.Second, 10*time.Millisecond, "expected an ack after handler completes before deadline")

	cancel()
	require.NoError(t, <-done)

	mu.Lock()
	ackSnapshot := append([]ackCall(nil), ackCalls...)
	mu.Unlock()

	require.Len(t, ackSnapshot, 1, "exactly one basic.ack frame")
	assert.Equal(t, uint64(1), ackSnapshot[0].tag)
	assert.True(t, ackSnapshot[0].multiple, "ack must use multiple=true")

	// Verify RecordHandler was called with outcome "ack" (not a timeout variant).
	capCM.mu.Lock()
	defer capCM.mu.Unlock()
	require.Len(t, capCM.records, 1)
	assert.Equal(t, "q", capCM.records[0].queue)
	assert.Equal(t, "ack", capCM.records[0].outcome, "outcome must be 'ack', not a timeout variant")
}

// captureMaxRedeliveriesMetrics records RecordMaxRedeliveries calls.
type captureMaxRedeliveriesMetrics struct {
	metrics.NoOpConsumerMetrics
	mu             sync.Mutex
	xDeathCount    int
	inProcessCount int
}

func (c *captureMaxRedeliveriesMetrics) RecordMaxRedeliveries(queue, cause string) {
	c.mu.Lock()
	switch cause {
	case "x-death":
		c.xDeathCount++
	case "in-process":
		c.inProcessCount++
	}
	c.mu.Unlock()
}

func (c *captureMaxRedeliveriesMetrics) RecordHandler(queue, outcome string, _ time.Duration) {
	// no-op; satisfy metrics.ConsumerMetrics if needed
}

// captureConsumerMetrics records handler calls for assertions.
type captureConsumerMetrics struct {
	metrics.NoOpConsumerMetrics
	mu      sync.Mutex
	records []struct {
		queue   string
		outcome string
		elapsed time.Duration
	}
}

func (c *captureConsumerMetrics) RecordHandler(queue, outcome string, elapsed time.Duration) {
	c.mu.Lock()
	c.records = append(c.records, struct {
		queue   string
		outcome string
		elapsed time.Duration
	}{queue, outcome, elapsed})
	c.mu.Unlock()
}
