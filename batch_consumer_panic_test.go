package warren

// T34c site (5): a panicking BatchHandler must nack-all-without-requeue the
// offending batch and the batch consumer must keep processing subsequent batches —
// the panic is isolated by safeCallBatchHandler, never killing the flush goroutine.

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestBatchConsumer_Consume_HandlerPanic_NacksAllNoRequeue_ThenContinues(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).Queue("q").Size(1).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	bc.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)
	secondAcked := make(chan struct{})

	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			calls++
			if calls == 1 {
				panic("batch boom")
			}
			return nil
		})
	}()

	// First batch (Size=1): the handler panics → must nack-all without requeue.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`"first"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "a batch handler panic must nack all without requeue")
	case <-time.After(2 * time.Second):
		t.Fatal("expected the panicking batch to be nacked")
	}

	// Second batch: the handler returns nil → must ack, proving the consumer
	// survived the panic and kept processing.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 2,
		Body:        []byte(`"second"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error {
				close(secondAcked)
				cancel()
				return nil
			},
		},
	}

	select {
	case <-secondAcked:
	case <-time.After(2 * time.Second):
		t.Fatal("batch consumer did not process the second batch after a handler panic")
	}
	<-done
}
