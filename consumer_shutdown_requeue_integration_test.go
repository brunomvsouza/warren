//go:build integration

package warren_test

// T70 (DS-03 / SRE-07) real-broker assertion: prefetched-but-undispatched
// deliveries are nack-requeued (not dropped) at consumer shutdown, so they are
// redelivered to the next consumer and consumer_shutdown_requeued_total counts
// them.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

type shutdownRequeueSpy struct {
	spyConsumerMetrics
	requeued atomic.Int64
}

func (s *shutdownRequeueSpy) RecordShutdownRequeued(_ string) { s.requeued.Add(1) }

func TestConsumerShutdown_requeuesUndispatched_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const srcQ = "test.t70.shutdown-requeue.src"
	purgeQueues(t, url, srcQ)
	t.Cleanup(func() { deleteQueues(url, srcQ) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(c)
	}()

	topo := &warren.Topology{Queues: []warren.Queue{{Name: srcQ, Durable: false}}}
	require.NoError(t, topo.Declare(ctx, conn))

	pub, err := warren.PublisherFor[int](conn).Exchange("").RoutingKey(srcQ).Build()
	require.NoError(t, err)
	const total = 10
	for i := 0; i < total; i++ {
		i := i
		require.NoError(t, pub.Publish(ctx, warren.Message[int]{Body: &i}))
	}
	_ = pub.Close(context.Background())

	spy := &shutdownRequeueSpy{}
	// Prefetch all 10, concurrency 1, and a handler that blocks on the first
	// message until shutdown — so 1 is dispatched (blocked) and ~9 sit prefetched
	// but undispatched when the consumer ctx is cancelled.
	consumer, err := warren.ConsumerFor[int](conn).
		Queue(srcQ).
		Prefetch(total).
		Concurrency(1).
		Metrics(spy).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	var firstSeen sync.Once
	firstDispatched := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.ConsumeRaw(consumeCtx, func(hctx context.Context, d *warren.Delivery[int]) error {
			firstSeen.Do(func() { close(firstDispatched) })
			<-hctx.Done() // block until shutdown so the rest stay undispatched
			return d.Nack(true)
		})
	}()

	select {
	case <-firstDispatched:
	case <-time.After(5 * time.Second):
		t.Fatal("first delivery never dispatched")
	}
	// Give the broker time to push the remaining prefetched deliveries into the
	// client buffer before we shut down.
	time.Sleep(300 * time.Millisecond)

	cancelConsume()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}

	assert.GreaterOrEqual(t, spy.requeued.Load(), int64(1),
		"undispatched deliveries must be nack-requeued and counted by consumer_shutdown_requeued_total")

	// The requeued messages must still be in the queue (redelivered), not lost.
	require.Eventually(t, func() bool {
		return countBatchMessagesInQueue(t, url, srcQ, 1) >= 1
	}, 3*time.Second, 100*time.Millisecond,
		"requeued deliveries must remain in the queue, not be silently dropped")
}
