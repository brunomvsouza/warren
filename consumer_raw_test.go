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
)

// — ConsumeRaw: raw handler is responsible for Ack/Nack ——————————————

func TestConsumeRaw_HandlerCalledWithDelivery(t *testing.T) {
	// ConsumeRaw passes the full *Delivery[M] to the raw handler.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	var gotDelivery *Delivery[string]
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			gotDelivery = d
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:         []byte(`"hello"`),
		ContentType:  "application/json",
		Acknowledger: &fakeAcknowledger{},
	}

	<-done
	require.NotNil(t, gotDelivery)
	require.NotNil(t, gotDelivery.Body())
	assert.Equal(t, "hello", *gotDelivery.Body())
}

func TestConsumeRaw_NoAutoAck_HandlerNotAcking(t *testing.T) {
	// When the raw handler does NOT call Ack/Nack, the consumer must NOT
	// auto-ack on its behalf. The message remains unacknowledged.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackOrNack := make(chan string, 1)
	handlerCalled := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, _ *Delivery[string]) error {
			close(handlerCalled)
			cancel() // end consume after handler runs
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handlerCalled
	<-done

	// No ack or nack must have been called by the consumer.
	select {
	case op := <-ackOrNack:
		t.Fatalf("ConsumeRaw must NOT auto-ack; got %q", op)
	default:
		// expected: no ack/nack emitted
	}
}

func TestConsumeRaw_HandlerExplicitAck(t *testing.T) {
	// Raw handler explicitly acks; consumer must not double-ack.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ackCount int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			_ = d.Ack() // explicit ack
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error {
				atomic.AddInt64(&ackCount, 1)
				return nil
			},
		},
	}

	<-done
	assert.Equal(t, int64(1), ackCount, "Ack must be called exactly once (no double-ack)")
}

func TestConsumeRaw_HandlerExplicitNack(t *testing.T) {
	// Raw handler explicitly nacks; consumer must not double-nack.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nackCount int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			_ = d.Nack(false) // explicit nack without requeue
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error {
				atomic.AddInt64(&nackCount, 1)
				return nil
			},
		},
	}

	<-done
	assert.Equal(t, int64(1), nackCount, "Nack must be called exactly once (no double-nack)")
}

func TestConsumeRaw_AckIf_UsedByRawHandler(t *testing.T) {
	// Raw handler may use d.AckIf(err) as a convenience, mapping errors to verdicts.
	// d.AckIf(nil) → Ack; d.AckIf(ErrRequeue) → Nack(requeue=true);
	// d.AckIf(other) → Nack(requeue=false).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	acked := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			err := d.AckIf(nil) // explicit AckIf → calls Ack
			if err == nil {
				close(acked)
			}
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error { return nil },
		},
	}

	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("expected AckIf to succeed")
	}
	<-done
}

func TestConsumeRaw_Headers_Redelivered_DeathCount_Accessible(t *testing.T) {
	// Raw handler can access Redelivered(), Headers(), and DeathCount() from Delivery.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("myq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		redelivered bool
		headers     Headers
		deathCount  int
	}
	got := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			got <- result{
				redelivered: d.Redelivered(),
				headers:     d.Headers(),
				deathCount:  d.DeathCount(),
			}
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		Redelivered: true,
		Headers: amqp091.Table{
			"x-custom": "value",
			"x-death": []any{
				amqp091.Table{
					"queue":  "myq",
					"reason": "rejected",
					"count":  int64(2),
				},
			},
		},
		Acknowledger: &fakeAcknowledger{},
	}

	select {
	case r := <-got:
		assert.True(t, r.redelivered, "Redelivered() must be true")
		assert.Equal(t, "value", r.headers["x-custom"], "custom header must be accessible")
		assert.Equal(t, 2, r.deathCount, "DeathCount() must parse x-death correctly")
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
	<-done
}

func TestConsumeRaw_CounterA_StillFires_ForRawHandler(t *testing.T) {
	// Counter A (x-death >= maxRedeliveries) fires even in ConsumeRaw mode,
	// short-circuiting before the handler is called.
	defer goleak.VerifyNone(t)

	const n = 2
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("rawq").MaxRedeliveries(n).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCalled int64
	nacked := make(chan bool, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, _ *Delivery[string]) error {
			atomic.AddInt64(&handlerCalled, 1)
			return nil
		})
	}()

	// Delivery with DeathCount = n → counter A fires, handler NOT called.
	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  "rawq",
					"reason": "rejected",
					"count":  int64(n),
				},
			},
		},
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nacked <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nacked:
		assert.False(t, requeue)
	case <-time.After(2 * time.Second):
		t.Fatal("expected Nack from counter A for ConsumeRaw")
	}
	<-done

	assert.Equal(t, int64(0), handlerCalled, "handler must NOT be called when counter A fires")
}

func TestConsumeRaw_MultipleDeliveries(t *testing.T) {
	// ConsumeRaw processes multiple deliveries; handler controls acking.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 3)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			n := atomic.AddInt64(&callCount, 1)
			if n == 3 {
				cancel()
			}
			_ = d.Ack()
			return nil
		})
	}()

	for range 3 {
		deliveryCh <- amqp091.Delivery{
			Body:         []byte(`"x"`),
			Acknowledger: &fakeAcknowledger{},
		}
	}

	<-done
	assert.Equal(t, int64(3), callCount, "handler must be called for each delivery")
}

// TestConsumeRaw_vs_Consume_BothAcceptErrors verifies that ConsumeRaw works alongside
// ErrRequeue semantics when the raw handler uses d.AckIf.
func TestConsumeRaw_AckIf_ErrRequeue_Nacks(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nacked := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			// Use AckIf to map ErrRequeue → Nack(true).
			_ = d.AckIf(ErrRequeue)
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nacked <- requeue
				return nil
			},
		},
	}

	select {
	case requeue := <-nacked:
		assert.True(t, requeue, "AckIf(ErrRequeue) must Nack with requeue=true")
	case <-time.After(time.Second):
		t.Fatal("expected Nack")
	}
	<-done
}

// TestConsumeRaw_StillRespects_DecodeError verifies that decode failures in ConsumeRaw
// still result in Nack(false) — the consumer protects against poison decode errors
// even in raw mode (handler never gets called with an undecodable payload).
func TestConsumeRaw_DecodeFailure_NackNoRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nacked := make(chan struct{})
	var handlerCalled int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, _ *Delivery[string]) error {
			atomic.AddInt64(&handlerCalled, 1)
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`not valid json`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error {
				close(nacked)
				cancel()
				return nil
			},
		},
	}

	select {
	case <-nacked:
	case <-time.After(time.Second):
		t.Fatal("expected Nack for decode failure in ConsumeRaw")
	}
	<-done

	assert.Equal(t, int64(0), handlerCalled,
		"handler must NOT be called when decode fails")
}

// TestConsumeRaw_ReturnError_NotAutoAcked verifies that a non-nil return error from
// a ConsumeRaw handler does NOT trigger auto-ack/nack — unlike Consume where the
// error drives the verdict.
func TestConsumeRaw_ReturnError_NotAutoAcked(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackOrNack := make(chan string, 1)
	handlerDone := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, _ *Delivery[string]) error {
			defer close(handlerDone)
			return errors.New("something went wrong") // NOT calling d.Ack/Nack
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handlerDone
	cancel()
	<-consumeDone

	select {
	case op := <-ackOrNack:
		t.Fatalf("ConsumeRaw must NOT auto-ack on handler error return; got %q", op)
	default:
		// expected: no ack/nack
	}
}
