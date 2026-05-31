//go:build integration

package warren_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// TestConsumer_PauseResume_DrainsAndResumes_integration is the T53 acceptance: a
// subscribed consumer is paused (local basic.cancel), 100 messages are published
// while paused and NONE reach the handler, then Resume re-issues basic.consume and
// all 100 are delivered. Health reflects the paused/active transitions throughout.
func TestConsumer_PauseResume_DrainsAndResumes_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	const total = 100
	const q = "test.pause.resume"

	url := amqpTestURL(t)
	ctx := context.Background()

	purgeQueues(t, url, q)
	t.Cleanup(func() { deleteQueues(url, q) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{Queues: []warren.Queue{{Name: q, Durable: true}}}
	require.NoError(t, topo.Declare(ctx, conn))

	var received atomic.Int64
	consumer, err := warren.ConsumerFor[string](conn).Queue(q).Prefetch(1).Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(consumeCtx, func(context.Context, string) error {
			received.Add(1)
			return nil
		})
	}()

	// Raw AMQP channel for broker-side consumer-count + backlog assertions.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup

	consumerCount := func() int {
		qi, e := rawCh.QueueInspect(q)
		if e != nil {
			return -1
		}
		return qi.Consumers
	}

	// Consumer attaches before we pause.
	require.Eventually(t, func() bool { return consumerCount() >= 1 },
		5*time.Second, 50*time.Millisecond, "consumer must attach before Pause")

	// — Pause: local basic.cancel — broker drops the subscription —————————————
	require.NoError(t, consumer.Pause(ctx))
	require.Eventually(t, func() bool { return consumerCount() == 0 },
		5*time.Second, 50*time.Millisecond, "Pause must drop the broker-side subscription")

	h, err := consumer.Health(ctx)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.True(t, h.Paused, "Health.Paused is true while paused")
	assert.False(t, h.Active, "Health.Active is false while paused")

	// Publish 100 while paused; none may reach the handler.
	pub, err := warren.PublisherFor[string](conn).Exchange("").RoutingKey(q).Build()
	require.NoError(t, err)
	for i := range total {
		body := "msg-" + strconv.Itoa(i)
		require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))
	}

	// All 100 sit in the queue and none are consumed for a stable window.
	require.Eventually(t, func() bool {
		qi, e := rawCh.QueueInspect(q)
		return e == nil && qi.Messages == total
	}, 5*time.Second, 50*time.Millisecond, "all %d messages must be enqueued while paused", total)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int64(0), received.Load(), "no message may reach the handler while paused")

	// — Resume: re-issue basic.consume — all 100 flow through ————————————————
	require.NoError(t, consumer.Resume(ctx))
	require.Eventually(t, func() bool { return consumerCount() >= 1 },
		5*time.Second, 50*time.Millisecond, "Resume must re-attach the subscription")

	require.Eventually(t, func() bool { return received.Load() == total },
		10*time.Second, 50*time.Millisecond, "all %d messages must be delivered after Resume", total)

	h, err = consumer.Health(ctx)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.False(t, h.Paused, "Health.Paused is false after Resume")
	assert.True(t, h.Active, "Health.Active is true after Resume")

	cancelConsume()
	select {
	case <-consumeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop after ctx cancel")
	}
}
