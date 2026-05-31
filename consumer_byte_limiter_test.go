package warren

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNewByteLimiter_ZeroOrNegative_Disabled(t *testing.T) {
	assert.Nil(t, newByteLimiter(0), "limit 0 disables the guardrail")
	assert.Nil(t, newByteLimiter(-1), "negative limit disables the guardrail")
}

func TestByteLimiter_AcquireWithinLimit_DoesNotBlock(t *testing.T) {
	bl := newByteLimiter(100)
	bl.acquire(60)

	done := make(chan struct{})
	go func() { bl.acquire(40); close(done) }() // 60+40 == 100 ≤ 100

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acquire that stays within the limit must not block")
	}
}

func TestByteLimiter_AcquireOverLimit_BlocksUntilRelease(t *testing.T) {
	bl := newByteLimiter(100)
	bl.acquire(80)

	acquired := make(chan struct{})
	go func() { bl.acquire(40); close(acquired) }() // 80+40 == 120 > 100

	select {
	case <-acquired:
		t.Fatal("acquire exceeding the limit must block while 80 bytes are in flight")
	case <-time.After(100 * time.Millisecond):
	}

	bl.release(80) // frees the budget; the pending 40 must now proceed

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("acquire must proceed once a release frees enough budget")
	}
}

func TestByteLimiter_OversizedMessage_ProceedsWhenIdle(t *testing.T) {
	// A single message larger than the whole budget must not deadlock: when
	// nothing else is in flight it proceeds alone (memory bounded to its size).
	bl := newByteLimiter(100)

	done := make(chan struct{})
	go func() { bl.acquire(500); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oversized message must proceed when nothing is in flight")
	}
}

func TestByteLimiter_NilReceiver_IsNoOp(t *testing.T) {
	var bl *byteLimiter // disabled
	assert.NotPanics(t, func() {
		bl.acquire(1 << 30)
		bl.release(1 << 30)
	})
}

func TestConsumerBuilder_MaxInFlightBytes_SetsLimiter(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").MaxInFlightBytes(1 << 20).Build()
	require.NoError(t, err)
	assert.Equal(t, int64(1<<20), c.maxInFlightBytes)
	require.NotNil(t, c.byteLimiter, "a positive MaxInFlightBytes must create a limiter")
}

func TestConsumerBuilder_MaxInFlightBytes_LastWins(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").
		MaxInFlightBytes(1 << 20).
		MaxInFlightBytes(4 << 20).
		Build()
	require.NoError(t, err)
	assert.Equal(t, int64(4<<20), c.maxInFlightBytes, "last MaxInFlightBytes call wins")
}

func TestConsumerBuilder_NoMaxInFlightBytes_NilLimiter(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Zero(t, c.maxInFlightBytes)
	assert.Nil(t, c.byteLimiter, "no MaxInFlightBytes leaves the guardrail disabled")
}

// TestConsumer_Dispatch_EmitsInFlightBytesGauge proves the consumer_inflight_bytes
// gauge rises by the body size while the handler runs and returns to zero after it
// completes (T50).
func TestConsumer_Dispatch_EmitsInFlightBytesGauge(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("q").Metrics(cm).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(ctx, func(context.Context, string) error {
			close(handlerStarted)
			<-releaseHandler
			return nil
		})
	}()

	body := []byte(`"twelve-bytes"`) // 14 bytes including the JSON quotes
	deliveryCh <- amqp091.Delivery{Body: body, Acknowledger: &fakeAcknowledger{}}

	<-handlerStarted
	assert.Equal(t, int64(len(body)), cm.inFlightBytesCur.Load(),
		"gauge must equal the in-flight body size while the handler runs")

	close(releaseHandler)
	cancel()
	<-consumeDone

	assert.Equal(t, int64(len(body)), cm.inFlightBytesPeak.Load(),
		"peak gauge must equal the body size")
	assert.Eventually(t, func() bool { return cm.inFlightBytesCur.Load() == 0 },
		time.Second, 10*time.Millisecond,
		"gauge must return to zero after the handler completes")
}
