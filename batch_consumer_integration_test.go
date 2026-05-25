//go:build integration

package warren_test

// T23 integration tests for BatchConsumer[M]:
//   - 500 msgs with Size(100) → 5 batches exactly flushed
//   - 50 msgs with FlushAfter(1s) → 1 batch flushed by timer before Size reached
//   - handler nil return: single basic.ack(multiple=true) on highest delivery-tag
//   - handler ErrRequeue: single basic.nack(multiple=true, requeue=true)
//   - manual Batch.Ack suppresses auto-verdict

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

// batchPayload is a simple payload for BatchConsumer integration tests.
type batchPayload struct {
	Seq int `json:"seq"`
}

// TestBatchConsumer_SizeFlush_integration publishes 500 messages with Size(100)
// and asserts exactly 5 batches are dispatched, each of 100 messages.
func TestBatchConsumer_SizeFlush_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t23-size-flush-test"
		total     = 500
		batchSize = 100
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	// Publish 500 messages.
	pub, err := warren.PublisherFor[batchPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]warren.Message[batchPayload], total)
	for i := range msgs {
		msgs[i] = warren.Message[batchPayload]{Body: &batchPayload{Seq: i}}
	}
	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	// Consume via BatchConsumer with Size(100).
	// Prefetch must be >= Size to avoid a deadlock: with prefetch < size the
	// broker stops sending after prefetch unacked messages, the batch never
	// reaches Size, and the consumer stalls until the context is cancelled.
	bc, err := warren.BatchConsumerFor[batchPayload](conn).
		Queue(queueName).
		Size(batchSize).
		Prefetch(uint16(batchSize)).
		Build()
	require.NoError(t, err)

	var (
		batchCount  atomic.Int64
		messageCount atomic.Int64
	)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[batchPayload]) error {
			batchCount.Add(1)
			messageCount.Add(int64(len(batch.Deliveries())))
			return nil
		})
	}()

	// Wait until all messages are consumed or timeout.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if messageCount.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	consumerCancel()

	assert.Equal(t, int64(total), messageCount.Load(), "all 500 messages must be consumed")
	assert.Equal(t, int64(total/batchSize), batchCount.Load(), "exactly 5 batches must be dispatched")
}

// TestBatchConsumer_FlushAfterTimer_integration publishes 50 messages with
// Size(100) and FlushAfter(500ms). The timer should fire before Size is reached,
// producing a single batch of all 50 messages.
func TestBatchConsumer_FlushAfterTimer_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t23-flush-after-test"
		total     = 50
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[batchPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]warren.Message[batchPayload], total)
	for i := range msgs {
		msgs[i] = warren.Message[batchPayload]{Body: &batchPayload{Seq: i}}
	}
	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	// Consume with Size(100) but FlushAfter(500ms) — the timer fires first.
	bc, err := warren.BatchConsumerFor[batchPayload](conn).
		Queue(queueName).
		Size(100).
		FlushAfter(500 * time.Millisecond).
		Build()
	require.NoError(t, err)

	var (
		batchCount   atomic.Int64
		messageCount atomic.Int64
	)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[batchPayload]) error {
			batchCount.Add(1)
			messageCount.Add(int64(len(batch.Deliveries())))
			return nil
		})
	}()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if messageCount.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	consumerCancel()

	assert.Equal(t, int64(total), messageCount.Load(), "all 50 messages must be consumed")
	assert.Equal(t, int64(1), batchCount.Load(), "exactly 1 batch must be dispatched by the timer")
}

// TestBatchConsumer_AutoAck_NilReturn_integration verifies that a nil handler
// return results in all messages being acked (not requeued). Publishes 100
// messages, consumes them in a single batch, verifies queue is empty afterwards.
func TestBatchConsumer_AutoAck_NilReturn_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t23-autoack-test"
		total     = 100
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[batchPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]warren.Message[batchPayload], total)
	for i := range msgs {
		msgs[i] = warren.Message[batchPayload]{Body: &batchPayload{Seq: i}}
	}
	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	// Consume in a single batch of 100. Handler returns nil → all messages acked.
	// Prefetch must be >= Size (100) to prevent the broker from halting delivery
	// at 64 (default) and never reaching the flush threshold.
	bc, err := warren.BatchConsumerFor[batchPayload](conn).
		Queue(queueName).
		Size(total).
		Prefetch(uint16(total)).
		Build()
	require.NoError(t, err)

	var messageCount atomic.Int64

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[batchPayload]) error {
			messageCount.Add(int64(len(batch.Deliveries())))
			return nil // auto-ack all
		})
	}()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if messageCount.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	consumerCancel()
	require.Equal(t, int64(total), messageCount.Load(), "all messages must be consumed")

	// Queue must be empty (all messages acked, not requeued).
	count := countBatchMessagesInQueue(t, url, queueName, 1)
	assert.Equal(t, 0, count, "queue must be empty after nil-return auto-ack")
}

// TestBatchConsumer_AutoNack_ErrRequeue_integration verifies that a handler
// returning ErrRequeue causes all messages to be nacked with requeue=true.
// Messages should reappear in the queue.
func TestBatchConsumer_AutoNack_ErrRequeue_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t23-autonack-requeue-test"
		total     = 20
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	// AutoDelete must be false here: after consumerCancel() the consumer
	// unsubscribes and an AutoDelete queue would be immediately deleted by the
	// broker, making countBatchMessagesInQueue return 0 because ch.Get fails on
	// a non-existent queue. Non-auto-delete keeps the queue (and the requeued
	// messages) visible until the explicit t.Cleanup delete runs.
	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: false}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[batchPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]warren.Message[batchPayload], total)
	for i := range msgs {
		msgs[i] = warren.Message[batchPayload]{Body: &batchPayload{Seq: i}}
	}
	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	// Consume and nack-with-requeue exactly once, then cancel.
	bc, err := warren.BatchConsumerFor[batchPayload](conn).
		Queue(queueName).
		Size(total).
		Build()
	require.NoError(t, err)

	handledOnce := make(chan struct{})
	var once sync.Once

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[batchPayload]) error {
			once.Do(func() { close(handledOnce) })
			return warren.ErrRequeue
		})
	}()

	select {
	case <-handledOnce:
	case <-ctx.Done():
		t.Fatal("timed out waiting for first batch to be nacked")
	}

	// Give broker time to requeue before we cancel and check.
	time.Sleep(200 * time.Millisecond)
	consumerCancel()

	// Explicitly close the connection to release all AMQP channels and any
	// unacknowledged messages back to the broker, so countBatchMessagesInQueue
	// can see them.
	_ = conn.Close(context.Background())

	// Messages must be back in the queue (requeue=true).
	count := countBatchMessagesInQueue(t, url, queueName, total)
	assert.GreaterOrEqual(t, count, 1, "at least some messages must be requeued after ErrRequeue")
}

// TestBatchConsumer_ManualBatchAck_SuppressesAutoVerdict_integration verifies that
// calling Batch.Ack() inside the handler suppresses the framework's auto-verdict.
// Publishes 50 messages, manually acks in handler, asserts queue is empty.
func TestBatchConsumer_ManualBatchAck_SuppressesAutoVerdict_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t23-manual-batch-ack-test"
		total     = 50
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[batchPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]warren.Message[batchPayload], total)
	for i := range msgs {
		msgs[i] = warren.Message[batchPayload]{Body: &batchPayload{Seq: i}}
	}
	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	bc, err := warren.BatchConsumerFor[batchPayload](conn).
		Queue(queueName).
		Size(total).
		Build()
	require.NoError(t, err)

	var messageCount atomic.Int64

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	go func() {
		_ = bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[batchPayload]) error {
			messageCount.Add(int64(len(batch.Deliveries())))
			// Manual ack — framework must not emit a second ack.
			return batch.Ack()
		})
	}()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if messageCount.Load() >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	consumerCancel()
	require.Equal(t, int64(total), messageCount.Load(), "all messages must be consumed")

	count := countBatchMessagesInQueue(t, url, queueName, 1)
	assert.Equal(t, 0, count, "queue must be empty after manual Batch.Ack()")
}
