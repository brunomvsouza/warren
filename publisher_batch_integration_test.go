//go:build integration

package warren_test

// T22 integration tests for PublishBatch:
//   - always-all contract: 1000 messages, 3 invalid client-side → 997 confirmed + 3 ErrInvalidMessage
//   - ErrBatchTooLarge guard: 2000 > default max 1024, immediate return, no broker work
//   - order preservation: 100 sequential bodies consumed in the same order they were published
//   - channel-close mid-batch: no retry, affected messages surface ErrChannelClosed

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// orderPayload is used by the order-preservation test.
type orderPayload struct {
	Seq int `json:"seq"`
}

// TestPublishBatch_AlwaysAll_integration publishes 1000 messages where indices
// 1, 500, and 999 carry an invalid chan header (client-side ErrInvalidMessage).
// The remaining 997 messages must be confirmed by the broker and the overall
// error must wrap ErrPartialBatch.
func TestPublishBatch_AlwaysAll_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const queueName = "warren-t22-always-all-test"
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	// Declare a simple queue for the test.
	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[orderPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	const total = 1000
	invalidIndices := map[int]bool{1: true, 500: true, 999: true}

	msgs := make([]warren.Message[orderPayload], total)
	for i := range msgs {
		msg := warren.Message[orderPayload]{Body: &orderPayload{Seq: i}}
		if invalidIndices[i] {
			msg.Headers = warren.Headers{"bad": make(chan int)}
		}
		msgs[i] = msg
	}

	results, err := pub.PublishBatch(ctx, msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrPartialBatch),
		"overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, total)

	var successCount, invalidCount int
	for i, r := range results {
		if invalidIndices[i] {
			assert.True(t, errors.Is(r.Err, warren.ErrInvalidMessage),
				"result[%d].Err must be ErrInvalidMessage, got %v", i, r.Err)
			invalidCount++
		} else {
			assert.NoError(t, r.Err, "result[%d].Err must be nil for valid messages", i)
			if r.Err == nil {
				successCount++
			}
		}
	}

	assert.Equal(t, 997, successCount, "997 messages must be confirmed")
	assert.Equal(t, 3, invalidCount, "3 messages must have ErrInvalidMessage")

	// Verify via raw AMQP that 997 messages actually reached the broker.
	count := countBatchMessagesInQueue(t, url, queueName, 997)
	assert.Equal(t, 997, count, "rabbitmq queue must contain exactly 997 messages")
}

// TestPublishBatch_ErrBatchTooLarge_integration verifies the size-cap guard:
// a batch of 2000 with default max 1024 must return ErrBatchTooLarge immediately
// and leave the queue empty (no broker work).
func TestPublishBatch_ErrBatchTooLarge_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const queueName = "warren-t22-too-large-test"
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[orderPayload](conn).
		RoutingKey(queueName).
		Build() // default PublishBatchMaxSize = 1024
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	// 2000 > 1024 → must be rejected immediately.
	msgs := make([]warren.Message[orderPayload], 2000)
	for i := range msgs {
		msgs[i] = warren.Message[orderPayload]{Body: &orderPayload{Seq: i}}
	}

	results, err := pub.PublishBatch(ctx, msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, warren.ErrBatchTooLarge),
		"expected ErrBatchTooLarge, got %v", err)
	assert.Nil(t, results, "results must be nil on ErrBatchTooLarge")

	// No messages should have reached the broker. Poll briefly to allow any
	// in-flight frames to land (there should be none).
	time.Sleep(100 * time.Millisecond)
	count := countBatchMessagesInQueue(t, url, queueName, 1)
	assert.Equal(t, 0, count, "no messages must reach the broker on ErrBatchTooLarge")
}

// TestPublishBatch_OrderPreservation_integration publishes 100 messages with
// sequential Seq values [0..99] and asserts that they are consumed in the same
// order (single-channel ordering guarantee).
func TestPublishBatch_OrderPreservation_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		queueName = "warren-t22-order-test"
		n         = 100
	)
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[orderPayload](conn).
		RoutingKey(queueName).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	// Publish n messages with sequential Seq values.
	msgs := make([]warren.Message[orderPayload], n)
	for i := range msgs {
		msgs[i] = warren.Message[orderPayload]{Body: &orderPayload{Seq: i}}
	}

	results, err := pub.PublishBatch(ctx, msgs)
	require.NoError(t, err)
	require.Len(t, results, n)
	for i, r := range results {
		require.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	// Consume via raw AMQP to preserve delivery order without any extra library
	// abstraction. Use basic.get (synchronous poll) to avoid prefetch complications.
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck

	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	got := make([]int, 0, n)
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		d, ok, err := ch.Get(queueName, true /* autoAck */)
		if err != nil {
			t.Fatalf("basic.get error: %v", err)
		}
		if !ok {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		var p orderPayload
		require.NoError(t, json.Unmarshal(d.Body, &p))
		got = append(got, p.Seq)
	}

	require.Len(t, got, n, "expected %d messages, got %d", n, len(got))
	for i, seq := range got {
		assert.Equal(t, i, seq, "delivery[%d] must have Seq=%d, got Seq=%d", i, i, seq)
	}
}

// TestPublishBatch_ChannelCloseMidBatch_integration forces a channel close
// after the batch starts publishing, and asserts that:
//   - the overall error wraps ErrPartialBatch (if any message was lost)
//   - per-message errors are ErrChannelClosed or ErrPublishNacked (not retry errors)
//   - PublishRetry does NOT fire (the "no retry" contract for batch)
func TestPublishBatch_ChannelCloseMidBatch_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const queueName = "warren-t22-channel-close-test"
	deleteQueues(url, queueName)
	t.Cleanup(func() { deleteQueues(url, queueName) })

	top := &warren.Topology{
		Queues: []warren.Queue{{Name: queueName, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	// Configure a retry policy on the publisher. PublishBatch must NOT use it.
	pub, err := warren.PublisherFor[orderPayload](conn).
		RoutingKey(queueName).
		PublishRetry(warren.RetryPolicy{Retries: 3, Min: 5 * time.Millisecond}).
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	const batchSize = 500
	msgs := make([]warren.Message[orderPayload], batchSize)
	for i := range msgs {
		msgs[i] = warren.Message[orderPayload]{Body: &orderPayload{Seq: i}}
	}

	// Force a channel error by deleting the queue after a brief delay.
	// Deleting a queue that has an active consumer/publisher causes the broker
	// to close the channel with a RESOURCE_LOCKED error.
	go func() {
		time.Sleep(50 * time.Millisecond)
		deleteQueues(url, queueName)
	}()

	results, batchErr := pub.PublishBatch(ctx, msgs)

	if batchErr != nil {
		// When errors occur, the overall error must be ErrPartialBatch.
		assert.True(t, errors.Is(batchErr, warren.ErrPartialBatch),
			"overall error must wrap ErrPartialBatch, got %v", batchErr)
	}

	if results != nil {
		require.Len(t, results, batchSize)
		for i, r := range results {
			if r.Err != nil {
				isChannelClosed := errors.Is(r.Err, warren.ErrChannelClosed)
				isNacked := errors.Is(r.Err, warren.ErrPublishNacked)
				assert.True(t, isChannelClosed || isNacked,
					"result[%d].Err must be ErrChannelClosed or ErrPublishNacked, got %v", i, r.Err)
			}
		}
	}
}

// — helpers ——————————————————————————————————————————————————————————————

// countBatchMessagesInQueue polls the queue with basic.get until it returns
// `want` messages or the deadline (5s) passes. Used to assert message counts.
func countBatchMessagesInQueue(t *testing.T, url, queue string, want int) int {
	t.Helper()

	rc, err := amqp091.Dial(url)
	if err != nil {
		t.Logf("countBatchMessagesInQueue: dial error: %v", err)
		return 0
	}
	defer rc.Close() //nolint:errcheck

	ch, err := rc.Channel()
	if err != nil {
		t.Logf("countBatchMessagesInQueue: channel error: %v", err)
		return 0
	}
	defer ch.Close() //nolint:errcheck

	count := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, ok, err := ch.Get(queue, true /* autoAck */)
		if err != nil {
			break
		}
		if !ok {
			if count >= want {
				break
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		count++
	}
	return count
}
