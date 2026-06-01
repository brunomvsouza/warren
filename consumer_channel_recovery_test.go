package warren

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// resubSpyMetrics counts consumer_resubscribed_total increments (T61 SRE-01).
type resubSpyMetrics struct {
	metrics.NoOpConsumerMetrics
	resubscribed atomic.Int64
}

func (m *resubSpyMetrics) RecordResubscribed(_ string) { m.resubscribed.Add(1) }

// TestConsumer_channelOnlyDeath_selfHeals_andIncrementsMetric proves that when
// the delivery channel closes while the TCP socket stays up (no reconnect hook
// fires), the consumer reopens its channel directly, keeps consuming, and
// increments consumer_resubscribed_total — instead of parking silently (T61).
func TestConsumer_channelOnlyDeath_selfHeals_andIncrementsMetric(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &resubSpyMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	// Each factory call returns a fresh live delivery sub (a new channel + done).
	// This stands in for openDeliveryCh reopening a channel on a healthy socket.
	type liveCh struct {
		ch   chan amqp091.Delivery
		done chan struct{}
	}
	var openCount atomic.Int64
	current := make(chan *liveCh, 8)
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		openCount.Add(1)
		lc := &liveCh{ch: make(chan amqp091.Delivery, 1), done: make(chan struct{})}
		current <- lc
		return deliverySub{ch: lc.ch, done: lc.done}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			received.Add(1)
			return d.Ack()
		})
	}()

	// First subscription: deliver one message, then simulate a channel-only death
	// by closing its delivery channel (TCP still up — no hook fires).
	first := <-current
	first.ch <- amqp091.Delivery{Body: []byte(`"one"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	require.Eventually(t, func() bool { return received.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	close(first.ch) // channel-only death

	// The consumer must self-heal: reopen via the factory and keep consuming.
	var second *liveCh
	require.Eventually(t, func() bool {
		select {
		case second = <-current:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "consumer did not reopen its channel after a channel-only death")

	second.ch <- amqp091.Delivery{Body: []byte(`"two"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	require.Eventually(t, func() bool { return received.Load() == 2 }, 2*time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, cm.resubscribed.Load(), int64(1),
		"consumer_resubscribed_total must increment on a channel-level self-heal (SRE-01)")
	assert.GreaterOrEqual(t, openCount.Load(), int64(2), "factory must be called for the initial open and the self-heal")

	cancel()
	close(second.ch)
	<-consumerDone
}
