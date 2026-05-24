package warren

import (
	"context"
	"errors"
	"strings"
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

// TestPublishBatch_MessageTooLarge verifies that messages exceeding
// maxMessageSizeBytes are rejected with ErrMessageTooLarge (wrapped as
// ErrInvalidMessage), while the rest of the batch proceeds normally.
func TestPublishBatch_MessageTooLarge(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// {"value":"ok"} = 14 bytes; {"value":"toolarge!!!"} = 23 bytes.
	// Set cap to 15 bytes so small messages pass and the large one is rejected.
	pub.maxMessageSizeBytes = 15

	msgs := []Message[testPayload]{
		{Body: &testPayload{Value: "ok"}},          // 14 bytes — fits
		{Body: &testPayload{Value: "toolarge!!!"}}, // 23 bytes — rejected
		{Body: &testPayload{Value: "ok"}},          // 14 bytes — fits
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch), "overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, 3)

	assert.NoError(t, results[0].Err, "result[0] (small) must succeed")
	assert.True(t, errors.Is(results[1].Err, ErrMessageTooLarge),
		"result[1] (too large) must be ErrMessageTooLarge, got %v", results[1].Err)
	assert.NoError(t, results[2].Err, "result[2] (small) must succeed")

	// Only the two valid messages should reach the broker.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, 2, "only 2 valid messages must reach the broker")
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

// TestPublishBatch_Mandatory_AllRouted verifies that a mandatory batch where all
// messages are successfully routed returns nil per-message errors and nil overall.
func TestPublishBatch_Mandatory_AllRouted(t *testing.T) {
	fake := newFakePubCh(true /* autoAck — no returns, all messages routed */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "routed"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, results, n)
	for i, r := range results {
		assert.NoError(t, r.Err, "result[%d] must be nil (all messages routed)", i)
	}

	// All messages must have been published with mandatory=true.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, n, "all %d messages must reach the broker", n)
	for i, m := range fake.mandatories {
		assert.True(t, m, "publish[%d] must carry mandatory=true", i)
	}
}

// TestPublishBatch_Mandatory_SomeUnroutable verifies that when a mandatory batch
// contains a message with no matching binding the broker returns basic.return for
// that message, and PublishBatch surfaces ErrUnroutable for that slot while leaving
// the other slots unaffected.
func TestPublishBatch_Mandatory_SomeUnroutable(t *testing.T) {
	const unroutableMsgID = "test-mandatory-batch-unroutable"

	fake := newFakePubCh(true /* autoAck */)
	fake.returnMsgIDs = map[string]uint16{unroutableMsgID: 312}

	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	msgs := []Message[testPayload]{
		{Body: &testPayload{Value: "routed-1"}},
		{Body: &testPayload{Value: "unroutable"}, MessageID: unroutableMsgID},
		{Body: &testPayload{Value: "routed-2"}},
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch),
		"overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, 3)

	assert.NoError(t, results[0].Err, "result[0] (routed) must succeed")
	assert.True(t, errors.Is(results[1].Err, ErrUnroutable),
		"result[1] (unroutable) must be ErrUnroutable, got %v", results[1].Err)
	assert.NoError(t, results[2].Err, "result[2] (routed) must succeed")

	// All 3 messages must have reached the broker.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, 3, "all 3 messages must reach the broker")
}

// TestPublishBatch_Mandatory_AllUnroutable verifies that when every message in a
// mandatory batch is unroutable, all slots carry ErrUnroutable and the overall
// error wraps ErrPartialBatch.
func TestPublishBatch_Mandatory_AllUnroutable(t *testing.T) {
	fake := newFakePubCh(true /* autoAck — sends ack after return */)
	fake.returnAll = true // all mandatory publishes receive basic.return

	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "unroutable"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch),
		"overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, n)
	for i, r := range results {
		assert.True(t, errors.Is(r.Err, ErrUnroutable),
			"result[%d] must be ErrUnroutable, got %v", i, r.Err)
	}

	// All n messages must have reached the broker.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, n, "all %d messages must reach the broker", n)
}

// TestPublishBatch_NilBody verifies that a message with a nil Body is rejected
// with ErrInvalidMessage while the rest of the batch proceeds normally.
func TestPublishBatch_NilBody(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := []Message[testPayload]{
		{Body: &testPayload{Value: "ok"}},
		{Body: nil}, // nil body — must be rejected
		{Body: &testPayload{Value: "ok"}},
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch), "overall error must wrap ErrPartialBatch, got %v", err)
	require.Len(t, results, 3)

	assert.NoError(t, results[0].Err, "result[0] (valid) must succeed")
	assert.True(t, errors.Is(results[1].Err, ErrInvalidMessage),
		"result[1] (nil Body) must be ErrInvalidMessage, got %v", results[1].Err)
	assert.NoError(t, results[2].Err, "result[2] (valid) must succeed")

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, 2, "only 2 valid messages must reach the broker")
}

// TestPublishBatch_UserIDMismatch verifies that a message whose UserID does not
// match the connection's authenticated user is rejected with ErrInvalidMessage,
// and that the error string does not expose the mismatched value (security R-1).
func TestPublishBatch_UserIDMismatch(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.authUser = "alice"

	msgs := []Message[testPayload]{
		{Body: &testPayload{Value: "ok"}},
		{Body: &testPayload{Value: "mismatch"}, UserID: "bob"},
		{Body: &testPayload{Value: "ok"}},
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, 3)

	assert.NoError(t, results[0].Err, "result[0] (no UserID) must succeed")
	assert.True(t, errors.Is(results[1].Err, ErrInvalidMessage),
		"result[1] (mismatched UserID) must be ErrInvalidMessage, got %v", results[1].Err)
	assert.NoError(t, results[2].Err, "result[2] (no UserID) must succeed")

	// The mismatched value "bob" must NOT appear in the error string (security R-1).
	assert.False(t, strings.Contains(results[1].Err.Error(), "bob"),
		"error must not expose the UserID value, got: %s", results[1].Err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Len(t, fake.publishes, 2, "only valid messages must reach the broker")
}

// TestPublishBatch_DefaultMaxSize_Fallback verifies that publishBatchMaxSize == 0
// (unset, e.g. direct struct init without the builder) defaults to 1024 internally.
func TestPublishBatch_DefaultMaxSize_Fallback(t *testing.T) {
	fake := newFakePubCh(true)
	// Use maxSize=0 to exercise the internal default-fallback branch.
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 0)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// 1025 messages must be rejected (default cap is 1024).
	msgs := make([]Message[testPayload], 1025)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "x"}}
	}

	_, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBatchTooLarge),
		"1025 messages with default cap must return ErrBatchTooLarge, got %v", err)
}

// TestPublishBatch_ExactlyMaxSize verifies that a batch of exactly PublishBatchMaxSize
// messages is accepted (the cap is exclusive: > maxSize is rejected, == maxSize is OK).
func TestPublishBatch_ExactlyMaxSize(t *testing.T) {
	const maxSize = 5
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, maxSize)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := make([]Message[testPayload], maxSize)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "boundary"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.NoError(t, err, "batch of exactly maxSize must be accepted")
	require.Len(t, results, maxSize)
	for i, r := range results {
		assert.NoError(t, r.Err, "result[%d] must be nil", i)
	}
}

// TestPublishBatch_PoolExhausted verifies that when the channel pool is fully
// occupied, PublishBatch returns nil results and wraps ErrChannelPoolExhausted.
// The test holds the single pool token before calling PublishBatch to guarantee
// exhaustion without relying on a non-deterministic context-vs-token race in
// pool.acquire's select statement.
func TestPublishBatch_PoolExhausted(t *testing.T) {
	fake := newFakePubCh(true)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Hold the single pool token so PublishBatch cannot acquire a channel.
	_, poolRelease, err := pub.pools[0].acquire(context.Background())
	require.NoError(t, err, "pre-acquiring pool token must succeed")
	defer poolRelease()

	// Short timeout so the test does not block for long.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	msgs := []Message[testPayload]{{Body: &testPayload{Value: "v"}}}
	results, err := pub.PublishBatch(ctx, msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrChannelPoolExhausted),
		"exhausted pool must return ErrChannelPoolExhausted, got %v", err)
	assert.Nil(t, results, "results must be nil on connection-level error")

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Empty(t, fake.publishes, "no broker publishes expected on pool-exhausted")
}

// TestPublishBatch_PublishWithContextFailure verifies that when PublishWithContext
// returns an error for every message, all results carry that error and the overall
// error wraps ErrPartialBatch.
func TestPublishBatch_PublishWithContextFailure(t *testing.T) {
	fake := newFakePubCh(true)
	fake.mu.Lock()
	fake.publishErr = errors.New("simulated network error")
	fake.mu.Unlock()

	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "v"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch),
		"publish failure must yield ErrPartialBatch, got %v", err)
	require.Len(t, results, n)
	for i, r := range results {
		assert.Error(t, r.Err, "result[%d] must have an error on PublishWithContext failure", i)
	}

	// PublishWithContext returned error immediately; confirms were never awaited.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Empty(t, fake.publishes, "publishErr prevents messages from reaching the publish slice")
}

// TestPublishBatch_ConfirmTimeout_PerMessage verifies that when no broker confirm
// arrives within ConfirmTimeout, every published message gets ErrConfirmTimeout
// and the overall error wraps ErrPartialBatch.
func TestPublishBatch_ConfirmTimeout_PerMessage(t *testing.T) {
	fake := newFakePubCh(false /* no auto-ack */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Use a very short timeout so the test runs fast.
	pub.confirmTimeout = 5 * time.Millisecond

	const n = 3
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "timeout"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, n)
	for i, r := range results {
		assert.True(t, errors.Is(r.Err, ErrConfirmTimeout),
			"result[%d].Err must be ErrConfirmTimeout, got %v", i, r.Err)
	}
}

// TestPublishBatch_Metrics_InFlightAndRecordPublish verifies that PublishBatch
// calls InFlightAdd(+N) before waiting for confirms, InFlightAdd(-N) after, and
// RecordPublish once per successfully-confirmed message.
func TestPublishBatch_Metrics_InFlightAndRecordPublish(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pm := &capturePublisherMetrics{}
	pub, stopPool := newTestPubBatch[testPayload](fake, pm, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const n = 4
	msgs := make([]Message[testPayload], n)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "metric"}}
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, results, n)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// InFlightAdd must have been called with +n and then -n.
	require.Len(t, pm.inFlight, 2, "expected exactly 2 InFlightAdd calls (+N and -N)")
	assert.Equal(t, int64(n), pm.inFlight[0].delta, "first InFlightAdd must be +%d", n)
	assert.Equal(t, int64(-n), pm.inFlight[1].delta, "second InFlightAdd must be -%d", n)

	// RecordPublish must have been called once per message with outcome "success".
	require.Len(t, pm.records, n, "expected %d RecordPublish calls", n)
	for i, rec := range pm.records {
		assert.Equal(t, "x", rec.exchange, "record[%d] exchange must be 'x'", i)
		assert.Equal(t, "success", rec.outcome, "record[%d] outcome must be 'success'", i)
	}
}

// TestPublishBatch_Metrics_PartialFailure verifies that messages failing at
// PublishWithContext emit "error" outcome in RecordPublish, while valid confirmed
// messages emit "success".
func TestPublishBatch_Metrics_PartialFailure(t *testing.T) {
	fake := newFakePubCh(false /* manual ack */)
	pm := &capturePublisherMetrics{}
	pub, stopPool := newTestPubBatch[testPayload](fake, pm, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Use a very short timeout so nacke arrive quickly via timeout.
	pub.confirmTimeout = 5 * time.Millisecond

	msgs := []Message[testPayload]{
		{Body: &testPayload{Value: "v"}},
		{Body: &testPayload{Value: "v"}},
	}

	results, err := pub.PublishBatch(context.Background(), msgs)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPartialBatch))
	require.Len(t, results, 2)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Both messages should have timed out → "error" outcome.
	require.Len(t, pm.records, 2)
	for i, rec := range pm.records {
		assert.Equal(t, "error", rec.outcome, "record[%d] timed-out message must have outcome 'error'", i)
	}
}

// TestPublishBatch_Race verifies that concurrent PublishBatch calls on the same
// publisher do not trigger the race detector. The -race flag is what makes this
// test meaningful.
func TestPublishBatch_Race(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, stopPool := newTestPubBatch[testPayload](fake, metrics.NoOpPublisherMetrics{}, 1024)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const goroutines = 8
	const batchSize = 3

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs := make([]Message[testPayload], batchSize)
			for i := range msgs {
				msgs[i] = Message[testPayload]{Body: &testPayload{Value: "race"}}
			}
			_, _ = pub.PublishBatch(context.Background(), msgs)
		}()
	}
	wg.Wait()
}
