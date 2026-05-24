package warren

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
)

// newTestPubBatch builds a Publisher[M] with a single fake-backed pool and a
// pre-set publishBatchMaxSize so tests using direct struct init don't hit the
// "0 means undefined" edge case.
func newTestPubBatch[M any](fake *fakePubChannel, pm metrics.PublisherMetrics, maxSize int) (*Publisher[M], func()) {
	pool, stopPool := wireFakePool(fake)
	mc := &managedConn{}
	pub := &Publisher[M]{
		pools:               []*publisherConnPool{pool},
		mcs:                 []*managedConn{mc},
		codec:               codec.NewJSON(),
		pm:                  pm,
		exchange:            "x",
		publishBatchMaxSize: maxSize,
		confirmTimeout:      2 * time.Second,
	}
	return pub, stopPool
}

// TestPublishBatch_ErrBatchTooLarge verifies that a batch larger than
// PublishBatchMaxSize is rejected immediately with no broker work.
func TestPublishBatch_ErrBatchTooLarge(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 3)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]Message[testPayload], 4) // 4 > max 3
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "x"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBatchTooLarge), "expected ErrBatchTooLarge, got %v", err)
	assert.Nil(t, results, "results must be nil on ErrBatchTooLarge")

	// No broker work should have occurred.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Empty(t, fake.publishes, "no broker publishes expected on ErrBatchTooLarge")
}

// TestPublishBatch_EmptySlice verifies that an empty batch returns empty results
// with no error and no broker work.
func TestPublishBatch_EmptySlice(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	results, err := pub.PublishBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestPublishBatch_AllSuccess verifies that a batch of valid messages returns
// all-nil per-message errors and a nil overall error.
func TestPublishBatch_AllSuccess(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 10
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "hello"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, results, n)
	for i, r := range results {
		assert.NoError(t, r.Err, "result[%d].Err must be nil", i)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, n, "expected %d broker publishes", n)
}

// TestPublishBatch_PartialFailure_InvalidMessage verifies the always-all contract:
// client-side rejections (invalid Headers type) do not abort the batch; valid
// messages proceed and the overall error wraps ErrPartialBatch.
func TestPublishBatch_PartialFailure_InvalidMessage(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Build 10 messages: indices 2, 5, 8 have an invalid chan header.
	msgs := make([]Message[testPayload], 10)
	invalidIndices := map[int]bool{2: true, 5: true, 8: true}
	for i := range msgs {
		body := &testPayload{Value: "v"}
		msg := Message[testPayload]{Body: body}
		if invalidIndices[i] {
			msg.Headers = Headers{"bad": make(chan int)}
		}
		msgs[i] = msg
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch), "overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, 10)

	for i, r := range results {
		if invalidIndices[i] {
			assert.True(t, errors.Is(r.Err, ErrInvalidMessage),
				"result[%d].Err must be ErrInvalidMessage, got %v", i, r.Err)
		} else {
			assert.NoError(t, r.Err, "result[%d].Err must be nil for valid messages", i)
		}
	}

	// 7 valid messages should have been published to the broker.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, 7, "only valid messages reach the broker")
}

// TestPublishBatch_AllInvalid verifies that if all messages are invalid the
// batch returns ErrPartialBatch with no broker work performed.
func TestPublishBatch_AllInvalid(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := []Message[testPayload]{
		{Body: &testPayload{}, Headers: Headers{"bad": make(chan int)}},
		{Body: &testPayload{}, Headers: Headers{"bad": make(chan int)}},
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, 2)
	for _, r := range results {
		assert.True(t, errors.Is(r.Err, ErrInvalidMessage))
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Empty(t, fake.publishes, "no broker publishes on all-invalid batch")
}

// TestPublishBatch_AllNacked verifies that broker nacks are surfaced as
// ErrPublishNacked per message and ErrPartialBatch overall.
func TestPublishBatch_AllNacked(t *testing.T) {
	fake := newFakePubCh(false /* manual ack */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "nack"}}
	}

	// After a short delay, nack all published messages.
	go func() {
		time.Sleep(10 * time.Millisecond)
		fake.mu.Lock()
		ch := fake.confirmCh
		fake.mu.Unlock()
		for i := uint64(1); i <= n; i++ {
			ch <- amqp091.Confirmation{DeliveryTag: i, Ack: false}
		}
	}()

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, n)
	for i, r := range results {
		assert.True(t, errors.Is(r.Err, ErrPublishNacked),
			"result[%d].Err must be ErrPublishNacked, got %v", i, r.Err)
	}
}

// TestPublishBatch_ChannelClosed verifies that a channel close while waiting
// for confirms surfaces ErrChannelClosed per affected message and ErrPartialBatch
// overall.
func TestPublishBatch_ChannelClosed(t *testing.T) {
	fake := newFakePubCh(false /* manual — no ack */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "close"}}
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = fake.Close()
	}()

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, n)
	for i, r := range results {
		assert.True(t, errors.Is(r.Err, ErrChannelClosed),
			"result[%d].Err must be ErrChannelClosed, got %v", i, r.Err)
	}
}

// TestPublishBatch_ErrAlreadyClosed verifies that calling PublishBatch on a
// closed publisher returns ErrAlreadyClosed.
func TestPublishBatch_ErrAlreadyClosed(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()

	require.NoError(t, pub.Close(context.Background()))

	msgs := []Message[testPayload]{{Body: &testPayload{}}}
	_, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAlreadyClosed))
}

// TestPublishBatch_NoRetry verifies that a retry policy configured on the
// publisher does NOT apply to PublishBatch — nacks are not retried.
func TestPublishBatch_NoRetry(t *testing.T) {
	fake := newFakePubCh(false /* manual */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Set an aggressive retry policy on the publisher.
	pub.retryPolicy = &RetryPolicy{Retries: 5, Min: 1 * time.Millisecond, WithoutJitter: true}

	msgs := []Message[testPayload]{{Body: &testPayload{Value: "nack"}}}

	// Nack after a short delay.
	go func() {
		time.Sleep(10 * time.Millisecond)
		fake.mu.Lock()
		ch := fake.confirmCh
		fake.mu.Unlock()
		ch <- amqp091.Confirmation{DeliveryTag: 1, Ack: false}
	}()

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))

	// Only 1 publish should have been attempted (no retry).
	fake.mu.Lock()
	numPublishes := len(fake.publishes)
	fake.mu.Unlock()
	assert.Equal(t, 1, numPublishes, "PublishBatch must not retry; expected 1 publish, got %d", numPublishes)

	// The per-message error must be ErrPublishNacked (not a retry-related error).
	require.Len(t, results, 1)
	assert.True(t, errors.Is(results[0].Err, ErrPublishNacked))
}

// TestPublishBatch_SingleChannelOrdering verifies that all messages in a batch
// are published on a single channel (same pool acquisition), which guarantees
// in-order delivery per AMQP's per-channel ordering rule.
func TestPublishBatch_SingleChannelOrdering(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 5
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "ordered"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, results, n)

	// All messages must have been published on the single fake channel.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, n, "all %d messages must go through the single channel", n)
}
