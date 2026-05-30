package warren

// T34c site (4): a panicking Handler / RawHandler must nack-without-requeue the
// offending delivery (poison-message contract) and the consumer must keep
// processing subsequent deliveries — the panic is isolated by safeCallHandler and
// never allowed to kill the consume goroutine.

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestConsumer_Consume_HandlerPanic_NacksNoRequeue_ThenContinues(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)
	secondAcked := make(chan struct{})

	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			calls++
			if calls == 1 {
				panic("handler boom")
			}
			return nil
		})
	}()

	// First delivery: the handler panics → must nack without requeue.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 1,
		Body:        []byte(`"first"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "a handler panic must nack without requeue (poison-message contract)")
	case <-time.After(2 * time.Second):
		t.Fatal("expected the panicking delivery to be nacked")
	}

	// Second delivery: the handler returns nil → must ack, proving the consumer
	// survived the panic and kept processing.
	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 2,
		Body:        []byte(`"second"`),
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
		t.Fatal("consumer did not process the second delivery after a handler panic")
	}
	<-done
}
