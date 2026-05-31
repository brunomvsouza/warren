//go:build integration

package warren_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/metrics"
)

// cancelCountingMetrics records consumer_cancelled_total increments for assertion.
type cancelCountingMetrics struct {
	metrics.NoOpConsumerMetrics
	mu              sync.Mutex
	cancelledCount  int
	cancelledQueue  string
	cancelledReason string
}

func (m *cancelCountingMetrics) RecordCancelled(queue, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelledCount++
	m.cancelledQueue = queue
	m.cancelledReason = reason
}

func (m *cancelCountingMetrics) snapshot() (int, string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cancelledCount, m.cancelledQueue, m.cancelledReason
}

// TestConsumer_BasicCancel_QueueDeleted_integration exercises the full basic.cancel
// surfacing contract (T36, SPEC §6.3) against a real broker:
//
//	declare a queue → attach a consumer with OnCancel → delete the queue out from
//	under it → the broker delivers basic.cancel → OnCancel fires with the consumer
//	tag, consumer_cancelled_total increments, and Consume returns ErrConsumerCancelled.
//
// The broker only sends basic.cancel when the client advertised
// consumer_cancel_notify=true in connection.start-ok; this test reaching OnCancel is
// therefore the recorded-frame proof that the capability is advertised (acceptance #6).
func TestConsumer_BasicCancel_QueueDeleted_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		q   = "test.cancel.queuedeleted"
		tag = "ctag-cancel-test"
	)
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

	cm := &cancelCountingMetrics{}
	var onCancelReason atomic.Value
	onCancelFired := make(chan struct{})
	var onCancelOnce sync.Once

	consumer, err := warren.ConsumerFor[string](conn).
		Queue(q).
		Tag(tag).
		Metrics(cm).
		OnCancel(func(reason string) {
			onCancelReason.Store(reason)
			onCancelOnce.Do(func() { close(onCancelFired) })
		}).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(consumeCtx, func(context.Context, string) error { return nil })
	}()

	// Raw AMQP channel for broker-side polling + the queue deletion.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup

	// Wait until the consumer is attached before deleting the queue.
	require.Eventually(t, func() bool {
		qi, e := rawCh.QueueInspect(q)
		return e == nil && qi.Consumers >= 1
	}, 5*time.Second, 100*time.Millisecond, "consumer must attach before the queue is deleted")

	// Delete the queue out from under the consumer; this triggers basic.cancel.
	_, err = rawCh.QueueDelete(q, false, false, false)
	require.NoError(t, err)

	select {
	case <-onCancelFired:
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel did not fire after the queue was deleted (consumer_cancel_notify not advertised?)")
	}
	assert.Equal(t, tag, onCancelReason.Load(), "OnCancel reason must be the cancelled consumer tag")

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, warren.ErrConsumerCancelled, "Consume must return ErrConsumerCancelled after basic.cancel")
		assert.Contains(t, got.Error(), tag, "the returned error must carry the consumer tag")
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return after basic.cancel")
	}

	count, gotQueue, gotReason := cm.snapshot()
	assert.Equal(t, 1, count, "consumer_cancelled_total must increment exactly once")
	assert.Equal(t, q, gotQueue)
	// T49: the queue was deleted out from under the consumer, so the bounded
	// reason enum classifies it as "queue_deleted" (via a passive-declare probe),
	// never the unbounded consumer tag.
	assert.Equal(t, "queue_deleted", gotReason, "metric reason must classify the deleted queue")
	assert.NotContains(t, gotReason, tag, "metric reason must never be the consumer tag")
}

// TestConsumer_ExclusiveConsumer_SecondRefused_integration proves the Exclusive()
// flag round-trips into the basic.consume frame: the first consumer claims the queue
// exclusively, so the broker refuses a second consumer with ACCESS_REFUSED (403).
func TestConsumer_ExclusiveConsumer_SecondRefused_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const q = "test.cancel.exclusive"
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

	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup

	// First consumer claims the queue exclusively.
	excl, err := warren.ConsumerFor[string](conn).Queue(q).Exclusive().Tag("ctag-excl-1").Build()
	require.NoError(t, err)
	exclCtx, cancelExcl := context.WithCancel(ctx)
	defer cancelExcl()
	exclErrCh := make(chan error, 1)
	go func() {
		exclErrCh <- excl.Consume(exclCtx, func(context.Context, string) error { return nil })
	}()

	require.Eventually(t, func() bool {
		qi, e := rawCh.QueueInspect(q)
		return e == nil && qi.Consumers >= 1
	}, 5*time.Second, 100*time.Millisecond, "exclusive consumer must attach before the second attempt")

	// A second consumer on the same queue must be refused with ACCESS_REFUSED.
	// openDeliveryCh's ch.Consume fails synchronously, so Consume returns the wrapped error.
	second, err := warren.ConsumerFor[string](conn).Queue(q).Exclusive().Tag("ctag-excl-2").Build()
	require.NoError(t, err)
	secondCtx, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()

	got := second.Consume(secondCtx, func(context.Context, string) error { return nil })
	require.Error(t, got, "a second consumer on an exclusively-consumed queue must be refused")
	assert.ErrorIs(t, got, warren.ErrAccessRefused,
		"exclusive-consumer refusal must surface as ErrAccessRefused (403), got: %v", got)

	cancelExcl()
	select {
	case <-exclErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive consumer did not stop after ctx cancel")
	}
}
