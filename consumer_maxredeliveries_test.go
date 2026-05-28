package warren

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// — MaxRedeliveries builder tests ——————————————————————————————————————

func TestConsumerBuilder_MaxRedeliveries_Stored(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").MaxRedeliveries(3).Build()
	require.NoError(t, err)
	assert.Equal(t, 3, c.maxRedeliveries)
}

func TestConsumerBuilder_MaxRedeliveries_DefaultZero(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Equal(t, 0, c.maxRedeliveries)
}

func TestConsumerBuilder_MaxRedeliveries_QuorumCarveOut_Disabled(t *testing.T) {
	// Counter B is disabled when the queue is a quorum queue with DeliveryLimit > 0
	// (broker is authoritative; process-local counter B would shadow the broker).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(10).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).
		Build()
	require.NoError(t, err)
	assert.Equal(t, 10, c.maxRedeliveries)
	assert.True(t, c.counterBDisabled, "counter B must be disabled for quorum queue with DeliveryLimit > 0")
}

func TestConsumerBuilder_MaxRedeliveries_QuorumNoLimit_CounterBEnabled(t *testing.T) {
	// Quorum queue without DeliveryLimit: counter B stays enabled (no broker guarantee).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 0}).
		Build()
	require.NoError(t, err)
	assert.False(t, c.counterBDisabled)
}

func TestConsumerBuilder_MaxRedeliveries_ClassicQueue_CounterBEnabled(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeClassic, DeliveryLimit: 0}).
		Build()
	require.NoError(t, err)
	assert.False(t, c.counterBDisabled)
}

// — Counter B: in-process ErrRequeue loop ————————————————————————————

func TestConsumer_MaxRedeliveries_CounterB_NackRewroteAtLimit(t *testing.T) {
	// With MaxRedeliveries(3): deliveries 1-3 should Nack(requeue=true);
	// delivery 4 (n+1) should Nack(requeue=false). Handler IS called each time.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, n+2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requeueCalls []bool // requeue flag for each Nack call
	var requeueMu sync.Mutex
	var handlerCalls int64

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			atomic.AddInt64(&handlerCalls, 1)
			return ErrRequeue
		})
	}()

	// Send n+1 deliveries with the SAME MessageID (to exercise counter B).
	const msgID = "msg-counter-b-001"
	lastNack := make(chan struct{})
	for i := range n + 1 {
		isLast := i == n
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: msgID,
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					requeueMu.Lock()
					requeueCalls = append(requeueCalls, requeue)
					if isLast {
						close(lastNack)
					}
					requeueMu.Unlock()
					return nil
				},
			},
		}
	}

	select {
	case <-lastNack:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for (n+1)-th nack")
	}
	cancel()
	<-consumeDone

	requeueMu.Lock()
	defer requeueMu.Unlock()

	require.Len(t, requeueCalls, n+1, "expected exactly n+1 nack calls")
	// First n deliveries: Nack(requeue=true)
	for i := range n {
		assert.True(t, requeueCalls[i], "delivery %d should Nack with requeue=true", i+1)
	}
	// (n+1)-th delivery: Nack(requeue=false) — counter B rewrite
	assert.False(t, requeueCalls[n], "(n+1)-th delivery must Nack with requeue=false")

	// Handler must have been called on all n+1 deliveries (counter B does NOT short-circuit before handler).
	assert.Equal(t, int64(n+1), handlerCalls, "handler must be called on all n+1 deliveries")

	// Metric: exactly 1 MaxRedeliveries event with cause=in-process
	assert.Equal(t, 1, cm.maxRedeliveries["in-process"], "cause=in-process metric must fire once")
}

func TestConsumer_MaxRedeliveries_CounterB_MapClearedOnAck(t *testing.T) {
	// After a successful Ack, the counter B entry must be deleted (no memory leak).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(5).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	acked := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	deliveryCh <- amqp091.Delivery{
		Body:      []byte(`"hello"`),
		MessageId: "msg-ack-cleared",
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error {
				close(acked)
				return nil
			},
		},
	}

	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("expected Ack")
	}
	cancel()
	<-consumeDone

	// Counter B map must be empty after Ack.
	cs := c.counterState.Load()
	var count int
	cs.m.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 0, count, "counter B map must be empty after Ack")
}

func TestConsumer_MaxRedeliveries_CounterB_MapClearedOnNackNoRequeue(t *testing.T) {
	// After a Nack(requeue=false), the counter B entry must also be deleted.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(5).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First send an ErrRequeue delivery to increment counter B.
	nacked := make(chan struct{}, 2)
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		callCount := 0
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			callCount++
			if callCount == 1 {
				return ErrRequeue // increments counter B
			}
			return errors.New("real error") // Nack(false) — should delete from counter B
		})
	}()

	for range 2 {
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: "msg-nack-cleared",
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, _ bool) error {
					nacked <- struct{}{}
					return nil
				},
			},
		}
	}

	// Wait for both nacks.
	for range 2 {
		select {
		case <-nacked:
		case <-time.After(2 * time.Second):
			t.Fatal("expected Nack")
		}
	}
	cancel()
	<-consumeDone

	// Counter B map must be empty: the Nack(false) path deletes the entry.
	cs := c.counterState.Load()
	var count int
	cs.m.Range(func(_, _ any) bool { count++; return true })
	assert.Equal(t, 0, count, "counter B map must be empty after Nack(false)")
}

func TestConsumer_MaxRedeliveries_CounterB_ChannelClose_ResetsCounter(t *testing.T) {
	// After a channel close (simulated by calling openDeliveryCh again which
	// rotates counterState), the same MessageID starts at count=0.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(3).
		Build()
	require.NoError(t, err)

	// Deliver n-1 ErrRequeue deliveries to bring counter B to n-1.
	deliveryCh := make(chan amqp091.Delivery, 10)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requeueCalls int64
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			atomic.AddInt64(&requeueCalls, 1)
			return ErrRequeue
		})
	}()

	const msgID = "msg-reset-test"
	nackRequeued := make(chan struct{}, 10)
	for range 2 { // n-1 = 2 ErrRequeue deliveries
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: msgID,
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					if requeue {
						nackRequeued <- struct{}{}
					}
					return nil
				},
			},
		}
	}

	// Wait for both requeue-nacks.
	for range 2 {
		select {
		case <-nackRequeued:
		case <-time.After(2 * time.Second):
			t.Fatal("expected Nack(requeue=true)")
		}
	}

	// Verify counter B has count=2 for msgID.
	// Key is "mid:<MessageID>" (namespaced to avoid collision with fallback "dlv:" keys).
	cs := c.counterState.Load()
	v, ok := cs.m.Load("mid:" + msgID)
	require.True(t, ok, "counter B entry must exist for msgID after 2 requeues")
	assert.Equal(t, int64(2), v.(int64))

	// Simulate channel close: rotate counterState (as openDeliveryCh would do).
	// We directly store a new redeliveryCounter here via internal test access.
	newState := &redeliveryCounter{}
	c.counterState.Store(newState)

	// Now send another delivery with the same msgID.
	// Counter B is now 0 (new channel state) → should still be Nack(requeue=true).
	nackAfterReset := make(chan bool, 1)
	deliveryCh <- amqp091.Delivery{
		Body:      []byte(`"hello"`),
		MessageId: msgID,
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackAfterReset <- requeue
				return nil
			},
		},
	}

	select {
	case requeue := <-nackAfterReset:
		assert.True(t, requeue, "after channel close, counter B must reset: first requeue should still be Nack(true)")
	case <-time.After(2 * time.Second):
		t.Fatal("expected Nack after channel reset")
	}

	cancel()
	<-consumeDone
}

// — Counter A: x-death based ————————————————————————————————————————

func TestConsumer_MaxRedeliveries_CounterA_XDeath_ShortCircuitsBeforeHandler(t *testing.T) {
	// Counter A: when DeathCount() >= maxRedeliveries, the handler must NOT be called.
	// The delivery is nacked without requeue and the metric fires with cause=x-death.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handlerCalled int64
	nacked := make(chan bool, 1)

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			atomic.AddInt64(&handlerCalled, 1)
			return nil
		})
	}()

	// Delivery with DeathCount = n (= maxRedeliveries) → counter A fires.
	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  "testq",
					"reason": "rejected",
					"count":  int64(n),
				},
			},
		},
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nacked <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nacked:
		assert.False(t, requeue, "counter A must Nack without requeue")
	case <-time.After(2 * time.Second):
		t.Fatal("expected Nack from counter A")
	}
	<-consumeDone

	assert.Equal(t, int64(0), handlerCalled, "handler must NOT be called when counter A fires")
	assert.Equal(t, 1, cm.maxRedeliveries["x-death"], "cause=x-death metric must fire once")
}

func TestConsumer_MaxRedeliveries_CounterA_BelowLimit_HandlerCalled(t *testing.T) {
	// When DeathCount < maxRedeliveries, counter A must NOT fire.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").MaxRedeliveries(n).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerCalled := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			close(handlerCalled)
			cancel()
			return nil
		})
	}()

	// DeathCount = n-1 (below limit)
	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  "testq",
					"reason": "rejected",
					"count":  int64(n - 1),
				},
			},
		},
		Acknowledger: &fakeAcknowledger{},
	}

	select {
	case <-handlerCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler must be called when DeathCount < maxRedeliveries")
	}
	<-consumeDone
}

func TestConsumer_MaxRedeliveries_Disabled_NoIntercept(t *testing.T) {
	// MaxRedeliveries(0) means unbounded; no interception of any delivery.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build() // default: MaxRedeliveries=0
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nackCount := make(chan bool, 5)
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			return ErrRequeue
		})
	}()

	// Send 5 ErrRequeue deliveries with the same MessageID; without MaxRedeliveries,
	// none should be rewritten to Nack(false).
	for i := range 5 {
		last := i == 4
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: "msg-no-limit",
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					nackCount <- requeue
					if last {
						cancel()
					}
					return nil
				},
			},
		}
	}

	requeued := 0
	for range 5 {
		select {
		case requeue := <-nackCount:
			if requeue {
				requeued++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for nack")
		}
	}
	<-consumeDone
	assert.Equal(t, 5, requeued, "all deliveries must be Nack(requeue=true) when MaxRedeliveries=0")
}

// — Counter B: empty MessageID uses non-stable fallback key ——————————————

func TestConsumer_MaxRedeliveries_CounterB_EmptyMessageID_FallbackKey(t *testing.T) {
	// When MessageID is empty, counter B uses "dlv:<consumerTag>:<deliveryTag>" as the key.
	// Delivery tags change on each redelivery, so the counter never accumulates — counter B
	// is effectively disabled for these messages and must NOT rewrite the verdict.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, n+2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nackCalls []bool
	var mu sync.Mutex

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return ErrRequeue })
	}()

	// Send n+1 deliveries with NO MessageID but distinct DeliveryTags (simulating redeliveries).
	// Each delivery gets a fresh fallback key → counter never accumulates → all stay requeued.
	lastNack := make(chan struct{})
	for i := range n + 1 {
		isLast := i == n
		deliveryCh <- amqp091.Delivery{
			Body:        []byte(`"hello"`),
			MessageId:   "", // intentionally empty
			DeliveryTag: uint64(i + 1),
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					mu.Lock()
					nackCalls = append(nackCalls, requeue)
					if isLast {
						close(lastNack)
					}
					mu.Unlock()
					return nil
				},
			},
		}
	}

	select {
	case <-lastNack:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for last nack")
	}
	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()
	for i, requeue := range nackCalls {
		assert.True(t, requeue,
			"delivery %d: empty MessageID → fallback key is not stable across redeliveries → must remain Nack(requeue=true)", i+1)
	}
	assert.Equal(t, 0, cm.maxRedeliveries["in-process"],
		"counter B must NOT fire when MessageID is absent (fallback key changes per delivery)")
}

func TestConsumer_MaxRedeliveries_CounterB_ContinuationByteMessageID_DoesNotFalselyFire(t *testing.T) {
	// A MessageID composed entirely of UTF-8 continuation bytes, longer than
	// maxMsgIDKeyLen, truncates to an empty string. counterBKeyForMsgID returns ""
	// for it, so dispatch falls back to the per-delivery "dlv:<tag>" key. With
	// distinct delivery tags the fallback key changes every time, so counter B
	// must NOT accumulate and must NOT rewrite the verdict to Nack(false).
	//
	// Without the empty-result guard, every such message would collapse onto the
	// degenerate "mid:" slot, the counter would accumulate across distinct
	// deliveries, and the (n+1)-th would falsely fire ErrMaxRedeliveries.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, n+2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nackCalls []bool
	var mu sync.Mutex

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return ErrRequeue })
	}()

	// All-continuation-byte MessageID longer than the truncation limit.
	badMsgID := strings.Repeat("\x80", maxMsgIDKeyLen+1)
	lastNack := make(chan struct{})
	for i := range n + 1 {
		isLast := i == n
		deliveryCh <- amqp091.Delivery{
			Body:        []byte(`"hello"`),
			MessageId:   badMsgID,
			DeliveryTag: uint64(i + 1), // distinct tags → distinct dlv: fallback keys
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					mu.Lock()
					nackCalls = append(nackCalls, requeue)
					if isLast {
						close(lastNack)
					}
					mu.Unlock()
					return nil
				},
			},
		}
	}

	select {
	case <-lastNack:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for last nack")
	}
	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nackCalls, n+1, "expected exactly n+1 nack calls")
	for i, requeue := range nackCalls {
		assert.True(t, requeue,
			"delivery %d: continuation-byte MessageID → dlv: fallback is not stable across redeliveries → must remain Nack(requeue=true)", i+1)
	}
	assert.Equal(t, 0, cm.maxRedeliveries["in-process"],
		"counter B must NOT fire for a continuation-byte MessageID that truncates to empty")
}

// — Counter B: wrapped ErrRequeue via fmt.Errorf is detected correctly ——

func TestConsumer_MaxRedeliveries_CounterB_WrappedErrRequeue(t *testing.T) {
	// errors.Is(err, ErrRequeue) must match even when ErrRequeue is wrapped.
	// A handler that returns fmt.Errorf("ctx: %w", ErrRequeue) must accumulate in counter B.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, n+2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nackCalls []bool
	var mu sync.Mutex

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			// Wrap ErrRequeue like a real handler that also includes context.
			return fmt.Errorf("transient failure: %w", ErrRequeue)
		})
	}()

	lastNack := make(chan struct{})
	for i := range n + 1 {
		isLast := i == n
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: "wrapped-requeue-msg",
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					mu.Lock()
					nackCalls = append(nackCalls, requeue)
					if isLast {
						close(lastNack)
					}
					mu.Unlock()
					return nil
				},
			},
		}
	}

	select {
	case <-lastNack:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for last nack")
	}
	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, nackCalls, n+1, "expected %d nack calls (n requeue + 1 dead-letter)", n+1)
	// First n calls: Nack(requeue=true)
	for i := range n {
		assert.True(t, nackCalls[i], "delivery %d: wrapped ErrRequeue must Nack(requeue=true)", i+1)
	}
	// (n+1)-th call: counter B fires → Nack(requeue=false)
	assert.False(t, nackCalls[n], "delivery %d: counter B must rewrite to Nack(requeue=false) on wrapped ErrRequeue", n+1)
	assert.Equal(t, 1, cm.maxRedeliveries["in-process"], "counter B must fire once for wrapped ErrRequeue")
}

// — Map leak stress test ——————————————————————————————————————————————

func TestConsumer_MaxRedeliveries_CounterB_LeakFree(t *testing.T) {
	// Deliver N messages each returning nil (Ack). The counter B map must be
	// empty at the end (no entries left over for successfully processed messages).
	defer goleak.VerifyNone(t)

	const total = 100 // reduced from 1M for test speed; the same code path covers all sizes

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(5).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, total)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ackCount int64
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	for i := range total {
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: fmt.Sprintf("msg-%d", i),
			Acknowledger: &fakeAcknowledger{
				ackFn: func(_ uint64, _ bool) error {
					if atomic.AddInt64(&ackCount, 1) == total {
						cancel()
					}
					return nil
				},
			},
		}
	}

	<-consumeDone

	var mapSize int
	c.counterState.Load().m.Range(func(_, _ any) bool { mapSize++; return true })
	assert.Equal(t, 0, mapSize, "counter B map must be empty after %d Ack'd messages", total)
}

// — Quorum carve-out: counter B disabled but counter A still runs ——————

func TestConsumer_MaxRedeliveries_QuorumCarveOut_CounterAStillFires(t *testing.T) {
	// With quorum+DeliveryLimit topology hint, counter B is disabled but counter A runs.
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).
		Metrics(cm).
		Build()
	require.NoError(t, err)
	assert.True(t, c.counterBDisabled)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nacked := make(chan bool, 1)
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	// Delivery with DeathCount = n → counter A must still fire even with quorum carve-out.
	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{
					"queue":  "testq",
					"reason": "rejected",
					"count":  int64(n),
				},
			},
		},
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nacked <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nacked:
		assert.False(t, requeue, "counter A must Nack without requeue even with quorum carve-out")
	case <-time.After(2 * time.Second):
		t.Fatal("expected Nack from counter A")
	}
	<-consumeDone
	assert.Equal(t, 1, cm.maxRedeliveries["x-death"])
}

func TestConsumer_MaxRedeliveries_QuorumCarveOut_CounterBDoesNotFire(t *testing.T) {
	// With quorum+DeliveryLimit topology hint, ErrRequeue loops are NOT bounded
	// by counter B (broker-enforced instead). Multiple ErrRequeue returns must
	// NOT rewrite the verdict to Nack(false).
	defer goleak.VerifyNone(t)

	const n = 3
	conn := newFakeConsumerConn(t)
	cm := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		MaxRedeliveries(n).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, n+2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nackCalls []bool
	var mu sync.Mutex

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error { return ErrRequeue })
	}()

	// Send n+1 deliveries; counter B is disabled so ALL should be Nack(requeue=true).
	lastNack := make(chan struct{})
	for i := range n + 1 {
		isLast := i == n
		deliveryCh <- amqp091.Delivery{
			Body:      []byte(`"hello"`),
			MessageId: "msg-quorum-no-counterb",
			Acknowledger: &fakeAcknowledger{
				nackFn: func(_ uint64, _, requeue bool) error {
					mu.Lock()
					nackCalls = append(nackCalls, requeue)
					if isLast {
						close(lastNack)
					}
					mu.Unlock()
					return nil
				},
			},
		}
	}

	select {
	case <-lastNack:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for last nack")
	}
	cancel()
	<-consumeDone

	mu.Lock()
	defer mu.Unlock()
	for i, requeue := range nackCalls {
		assert.True(t, requeue, "delivery %d: counter B disabled → must remain Nack(requeue=true)", i+1)
	}
	assert.Equal(t, 0, cm.maxRedeliveries["in-process"], "counter B must NOT fire for quorum+DeliveryLimit queue")
}

// — helpers ——————————————————————————————————————————————————————————————

// maxRedeliveriesCountingMetrics records RecordMaxRedeliveries calls by cause.
type maxRedeliveriesCountingMetrics struct {
	metrics.NoOpConsumerMetrics
	mu              sync.Mutex
	maxRedeliveries map[string]int // keyed by cause ("x-death", "in-process", "delivery-limit")
}

func (m *maxRedeliveriesCountingMetrics) RecordMaxRedeliveries(_, cause string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxRedeliveries == nil {
		m.maxRedeliveries = make(map[string]int)
	}
	m.maxRedeliveries[cause]++
}

// TestRedeliveryCounter_load_nonInt64ValueReturnsSafeDefault verifies that
// redeliveryCounter.load returns 0 (the safe default) when the sync.Map
// holds a value of an unexpected type. This is a programming-error guard
// introduced by the redeliveryCounter.load refactor (ab10edd); the branch
// is impossible in normal operation but prevents a panic if the map is
// accidentally populated with a non-int64 value.
func TestRedeliveryCounter_load_nonInt64ValueReturnsSafeDefault(t *testing.T) {
	cs := &redeliveryCounter{}
	cs.m.Store("key", "not-an-int64") // deliberately corrupt stored type
	result := cs.load("key")
	assert.Equal(t, int64(0), result, "non-int64 stored value must return 0 as safe default")
}
