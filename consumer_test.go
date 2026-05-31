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
	"unicode/utf8"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/log"
	"github.com/brunomvsouza/warren/metrics"
)

// — builder unit tests ———————————————————————————————————————————————————

func TestConsumerBuilder_DefaultTag_IsCtagUUIDv7(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(c.tag, "ctag-"), "tag must start with ctag-, got %q", c.tag)
	assert.Len(t, c.tag, len("ctag-")+36, "tag must be ctag-<uuidv7>")
}

func TestConsumerBuilder_UserSuppliedTag_PassedThrough(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Tag("my-tag").Build()
	require.NoError(t, err)
	assert.Equal(t, "my-tag", c.tag)
}

func TestConsumerBuilder_LastWins_Concurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Concurrency(2).Concurrency(8).Build()
	require.NoError(t, err)
	assert.Equal(t, uint(8), c.concurrency)
}

func TestConsumerBuilder_LastWins_HandlerTimeout(t *testing.T) {
	conn := newFakeConsumerConn(t)
	// second call with 0 disables the timeout
	c, err := ConsumerFor[string](conn).Queue("q").
		HandlerTimeout(50 * time.Millisecond).
		HandlerTimeout(0).
		Build()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), c.handlerTimeout)
}

func TestConsumerBuilder_LastWins_HandlerTimeoutVerdict(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		HandlerTimeoutVerdict(TimeoutNackNoRequeue).
		Build()
	require.NoError(t, err)
	assert.Equal(t, TimeoutNackNoRequeue, c.timeoutVerdict)
}

func TestConsumerBuilder_LastWins_Codec(t *testing.T) {
	conn := newFakeConsumerConn(t)
	lax := codec.NewJSON() // default — Postel's Law (lax)
	strict := codec.NewJSONStrict()
	c, err := ConsumerFor[string](conn).Queue("q").
		Codec(strict).Codec(lax).
		Build()
	require.NoError(t, err)
	// lax codec: decoding a payload with unknown fields must succeed
	var out string
	err = c.codec.Decode([]byte(`"hello"`), &out)
	require.NoError(t, err)
	assert.Equal(t, "hello", out)
}

func TestConsumerBuilder_WarnWhenPrefetchLtConcurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	warned := false
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		if strings.Contains(msg, "prefetch") {
			warned = true
		}
	}}
	_, err := ConsumerFor[string](conn).Queue("q").Prefetch(1).Concurrency(4).Build()
	require.NoError(t, err)
	assert.True(t, warned, "expected a prefetch < concurrency warning")
}

func TestConsumerBuilder_DefaultPrefetchAndConcurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Equal(t, uint16(64), c.prefetch)
	assert.Equal(t, uint(1), c.concurrency)
}

func TestConsumerBuilder_NilConn_Error(t *testing.T) {
	_, err := ConsumerFor[string](nil).Queue("q").Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestConsumerBuilder_EmptyQueue_Error(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := ConsumerFor[string](conn).Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestConsumerBuilder_BuildDoesNotMutateDefaults(t *testing.T) {
	// applyDefaults must operate on a copy; original builder state must be unchanged.
	conn := newFakeConsumerConn(t)
	b := ConsumerFor[string](conn).Queue("q") // concurrency=0, prefetch=0 (zero values)
	_, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, uint(0), b.concurrency, "builder.concurrency must not be mutated by Build")
	assert.Equal(t, uint16(0), b.prefetch, "builder.prefetch must not be mutated by Build")
	assert.Nil(t, b.c, "builder.codec must not be mutated by Build")
}

// — connIndexForTag unit tests ————————————————————————————————————————

func TestConnIndexForTag_ZeroOrOneConn_AlwaysZero(t *testing.T) {
	assert.Equal(t, 0, connIndexForTag("any-tag", 0))
	assert.Equal(t, 0, connIndexForTag("any-tag", 1))
}

func TestConnIndexForTag_MultipleConns_InBounds(t *testing.T) {
	const n = 4
	for _, tag := range []string{"ctag-abc", "ctag-xyz", "my-consumer", ""} {
		idx := connIndexForTag(tag, n)
		assert.GreaterOrEqual(t, idx, 0, "index must be >= 0 for tag %q", tag)
		assert.Less(t, idx, n, "index must be < %d for tag %q", n, tag)
	}
}

func TestConnIndexForTag_StableHash(t *testing.T) {
	idx := connIndexForTag("ctag-stable", 4)
	for range 10 {
		assert.Equal(t, idx, connIndexForTag("ctag-stable", 4), "same tag must map to same index")
	}
}

func TestConnIndexForTag_DifferentTags_DistributedAcrossConns(t *testing.T) {
	const n = 8
	seen := make(map[int]bool)
	tags := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	for _, tag := range tags {
		seen[connIndexForTag(tag, n)] = true
	}
	// With 12 tags and 8 slots the hash should hit more than one slot.
	assert.Greater(t, len(seen), 1, "hash should distribute across multiple connection slots")
}

// — Close unit test ———————————————————————————————————————————————————

func TestConsumer_Close_Idempotent(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	require.NoError(t, consumer.Close(context.Background()))
	require.NoError(t, consumer.Close(context.Background())) // sync.Once must prevent panic
}

// — Consume unit tests (fake channel injection) ———————————————————————

func TestConsumer_ConsumeRaw_AlreadyStarted_ReturnsError(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	// Simulate "already started" by setting the guard directly.
	consumer.started.Store(true)

	ctx := t.Context()

	err = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestConsumer_Consume_HandlerCalledWithDecodedPayload(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	var received string
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, msg string) error {
			received = msg
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
	}

	<-done
	assert.Equal(t, "hello", received)
}

func TestConsumer_Consume_HandlerNilReturn_Acks(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	acked := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error {
				close(acked)
				cancel()
				return nil
			},
		},
	}

	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("expected Ack to be called")
	}
	<-done
}

func TestConsumer_Consume_HandlerErrorReturn_NackNoRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return errors.New("handler failed")
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "generic error must nack without requeue")
	case <-time.After(time.Second):
		t.Fatal("expected Nack to be called")
	}
	<-done
}

func TestConsumer_Consume_HandlerErrRequeue_NackRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return ErrRequeue
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.True(t, requeue, "ErrRequeue must nack with requeue=true")
	case <-time.After(time.Second):
		t.Fatal("expected Nack to be called")
	}
	<-done
}

func TestConsumer_Consume_DecodeFailure_NackNoRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			cancel()
			return nil
		})
	}()

	ackErr := make(chan error, 1)
	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`not valid json`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error {
				ackErr <- nil
				cancel()
				return nil
			},
		},
	}

	select {
	case <-ackErr:
	case <-time.After(time.Second):
		t.Fatal("expected nack for decode failure")
	}
	<-done
	assert.Equal(t, 1, cm.decodeErrors)
	// The consumer threads its message type (M = string) into RecordHandler so
	// an enabled message_type label carries a real value (not "").
	assert.Equal(t, "string", cm.lastMessageType)
}

func TestConsumer_Consume_CodecPanic_NackNoRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Codec(panicCodec{}).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	nacked := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error {
				close(nacked)
				cancel()
				return nil
			},
		},
	}

	select {
	case <-nacked:
	case <-time.After(time.Second):
		t.Fatal("expected nack after codec panic")
	}
	<-done
}

func TestConsumer_Consume_HandlerTimeout_DefaultVerdict_NackNoRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(50 * time.Millisecond).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nackedRequeue := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(hCtx context.Context, _ string) error {
			select {
			case <-hCtx.Done():
				return hCtx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "default verdict must be nack-no-requeue")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: expected nack for handler timeout")
	}
	<-done
	assert.Equal(t, 1, cm.handlerTimeouts)
}

func TestConsumer_Consume_HandlerTimeout_RequeueVerdict_NackRequeue(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(50 * time.Millisecond).
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nackedRequeue := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(hCtx context.Context, _ string) error {
			select {
			case <-hCtx.Done():
				return hCtx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.True(t, requeue, "TimeoutNackRequeue verdict must nack with requeue=true")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: expected nack for handler timeout")
	}
	<-done
	assert.Equal(t, 1, cm.handlerTimeouts)
}

func TestConsumer_Consume_FastPath_ChannelClose_AbortsWithoutAck(t *testing.T) {
	// When the AMQP channel closes while a fast-path (no timeout) handler runs
	// and the handler returns an error, dispatch must abort without ack/nack.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	doneCh := make(chan struct{})
	close(doneCh) // pre-closed: channel was already gone before handler runs

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliverySubOverride = &deliverySub{ch: deliveryCh, done: doneCh}

	ctx, cancel := context.WithCancel(context.Background())

	ackOrNack := make(chan string, 1)
	handlerRan := make(chan struct{})

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			close(handlerRan)
			return errors.New("handler error")
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handlerRan
	cancel() // end ConsumeRaw; wg.Wait drains the dispatch goroutine
	<-consumeDone

	select {
	case op := <-ackOrNack:
		t.Fatalf("expected no ack/nack when channel is closed and handler errored, got %q", op)
	default:
	}
	assert.Equal(t, 1, cm.channelAborts, "RecordHandlerAbortedChannelClosed must be called once")
}

func TestConsumer_Consume_TimeoutPath_ChannelClose_AbortsWithoutAck(t *testing.T) {
	// When the AMQP channel closes while the handler goroutine (timeout path) is
	// blocked, the dispatch select must pick <-chanDone, cancel the handler ctx,
	// and return without ack/nack.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(5 * time.Second). // long: must not fire before doneCh
		Metrics(cm).
		Build()
	require.NoError(t, err)

	doneCh := make(chan struct{})
	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliverySubOverride = &deliverySub{ch: deliveryCh, done: doneCh}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackOrNack := make(chan string, 1)
	handlerStarted := make(chan struct{})
	handlerUnblocked := make(chan struct{})

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(ctx, func(hCtx context.Context, _ string) error {
			close(handlerStarted)
			<-hCtx.Done() // unblocks ONLY via dispatch's cancelCause(ErrChannelClosed)
			close(handlerUnblocked)
			return hCtx.Err()
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handlerStarted
	close(doneCh) // signal channel close; dispatch must pick <-chanDone case
	// Wait until the handler unblocks. With the outer ctx still live and
	// HandlerTimeout at 5s, dispatch's handler-ctx can only be cancelled here by the
	// <-chanDone branch (cancelCause(ErrChannelClosed)), so this deterministically
	// proves the select committed to <-chanDone (and recorded channelAborts) before
	// we touch the outer ctx. Cancelling first let the select race <-chanDone against
	// <-hCtx.Done(), intermittently taking the outer-cancel branch (no metric) — the
	// CI flake "expected 1, actual 0".
	<-handlerUnblocked
	cancel() // now safe: dispatch is past the select; drain ConsumeRaw so Consume returns
	<-consumeDone

	select {
	case op := <-ackOrNack:
		t.Fatalf("expected no ack/nack when channel is closed mid-handler, got %q", op)
	default:
	}
	assert.Equal(t, 1, cm.channelAborts)
}

func TestConsumer_Consume_TimeoutPath_OuterCtxCancelled_NoAckNoMetric(t *testing.T) {
	// When the outer ctx is cancelled while the handler goroutine (timeout path) is
	// blocked — before HandlerTimeout fires — dispatch must pick the default branch
	// of the hCtx.Done() switch: no ack/nack emitted, no timeout metric recorded.
	// This covers consumer.go "default: // Outer ctx cancelled; no ack" branch.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(5 * time.Second). // long: must not fire before outer cancel
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	// committed fires once dispatch has committed to the hCtx.Done() (outer-cancel)
	// branch. The handler blocks on a dedicated gate (not hCtx.Done()) so handlerDone
	// stays empty until we release it — making dispatch's select deterministic instead
	// of racing the handler's return (→ AckIf nacks on the ctx error) against hCtx.Done()
	// (→ no frame).
	committed := make(chan struct{})
	consumer.testHookBeforeTimeoutDrain = func() { close(committed) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackOrNack := make(chan string, 1)
	handlerStarted := make(chan struct{})
	handlerCanReturn := make(chan struct{})

	var handlerCtxErr error // read after <-consumeDone (happens-before via the channel)
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(ctx, func(hCtx context.Context, _ string) error {
			close(handlerStarted)
			<-handlerCanReturn         // released only after dispatch committed to the cancel branch
			handlerCtxErr = hCtx.Err() // by release time the cancel has propagated
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handlerStarted
	cancel() // cancel outer ctx before HandlerTimeout fires
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not commit to the ctx-done branch in time")
	}
	close(handlerCanReturn) // unblock the handler so <-handlerDone drains
	<-consumeDone

	// No ack or nack must have been emitted — broker will redeliver.
	select {
	case op := <-ackOrNack:
		t.Fatalf("expected no ack/nack when outer ctx cancelled mid-handler, got %q", op)
	default:
	}
	// No timeout metric must be recorded — this was an outer cancel, not a deadline.
	assert.Equal(t, 0, cm.handlerTimeouts,
		"RecordHandlerTimeout must not be called when outer ctx is cancelled")
	// Confirm dispatch took the cancel branch for the right reason: context.Canceled,
	// not a HandlerTimeout deadline (context.DeadlineExceeded).
	assert.ErrorIs(t, handlerCtxErr, context.Canceled,
		"handler ctx must carry Canceled, confirming the outer-ctx-cancel branch was taken")
}

func TestConsumer_Consume_HandlerTimeout_CompletesBeforeDeadline_Acks(t *testing.T) {
	// Covers the timeout-path handlerDone-wins branch (consumer.go: HandlerTimeout > 0 and
	// the handler returns before the deadline). The normal verdict must apply: exactly one
	// basic.ack with multiple=false, RecordHandler outcome "ack", and NO timeout metric.
	// The fast path (HandlerTimeout == 0) is covered by TestConsumer_Consume_HandlerNilReturn_Acks;
	// this pins the otherwise-untested success branch of the timeout select. No cancel races
	// here — the handler returns immediately, so handlerDone is the only ready select case.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	capCM := &captureConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(200 * time.Millisecond). // generous; the handler returns immediately
		Metrics(capCM).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type ackCall struct {
		tag      uint64
		multiple bool
	}
	acked := make(chan ackCall, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return nil // returns immediately, well before the 200 ms deadline
		})
	}()

	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 7,
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(tag uint64, multiple bool) error { acked <- ackCall{tag, multiple}; cancel(); return nil },
			nackFn: func(_ uint64, _, _ bool) error { t.Error("unexpected nack on handler success"); return nil },
		},
	}

	var got ackCall
	select {
	case got = <-acked:
	case <-time.After(2 * time.Second):
		t.Fatal("expected an ack after the handler completed before the deadline")
	}
	<-done

	assert.Equal(t, uint64(7), got.tag)
	assert.False(t, got.multiple, "Consumer acks a single delivery with multiple=false")

	capCM.mu.Lock()
	defer capCM.mu.Unlock()
	require.Len(t, capCM.records, 1)
	assert.Equal(t, "ack", capCM.records[0].outcome, "outcome must be 'ack', not a timeout variant")
	assert.Equal(t, 0, capCM.timeouts, "no timeout metric must be recorded on the success path")
}

func TestConsumer_Consume_HandlerTimeout_CompletesBeforeDeadline_HandlerError_NacksNoRequeue(t *testing.T) {
	// Complement of the success case: on the timeout-path handlerDone branch, a handler that
	// returns a plain (non-ErrRequeue) error before the deadline must produce exactly one
	// basic.nack with requeue=false, RecordHandler outcome "nack_no_requeue", and NO timeout
	// metric — the normal verdict, not the timeout verdict.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	capCM := &captureConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(200 * time.Millisecond). // generous; the handler returns immediately
		Metrics(capCM).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type nackCall struct {
		tag     uint64
		requeue bool
	}
	nacked := make(chan nackCall, 1)
	handlerErr := errors.New("boom")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return handlerErr // returns immediately, well before the 200 ms deadline
		})
	}()

	deliveryCh <- amqp091.Delivery{
		DeliveryTag: 9,
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { t.Error("unexpected ack on handler error"); return nil },
			nackFn: func(tag uint64, _, requeue bool) error { nacked <- nackCall{tag, requeue}; cancel(); return nil },
		},
	}

	var got nackCall
	select {
	case got = <-nacked:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a nack after the handler returned an error before the deadline")
	}
	<-done

	assert.Equal(t, uint64(9), got.tag)
	assert.False(t, got.requeue, "a plain handler error must nack without requeue")

	capCM.mu.Lock()
	defer capCM.mu.Unlock()
	require.Len(t, capCM.records, 1)
	assert.Equal(t, "nack_no_requeue", capCM.records[0].outcome, "outcome must be the normal verdict, not a timeout variant")
	assert.Equal(t, 0, capCM.timeouts, "no timeout metric must be recorded on the error path")
}

func TestConsumer_Consume_BasicCancel_RecordsCancelledAndReturns(t *testing.T) {
	// When the broker sends basic.cancel (simulated via cancelReasonCh injection),
	// the metric increments with a bounded reason enum (T49) — here "unknown",
	// since the fake connection cannot run the passive-declare probe — and Consume
	// returns ErrConsumerCancelled wrapping the consumer tag (SPEC §6.3).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	// Inject the delivery channel and the basic.cancel notification channel.
	deliveryCh := make(chan amqp091.Delivery)
	cancelCh := make(chan string, 1)
	consumer.deliveryCh = deliveryCh
	consumer.cancelReasonCh = cancelCh

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	// Fire broker cancel carrying the consumer tag (the only basic.cancel frame datum).
	cancelCh <- consumer.tag

	var consumeErr error
	select {
	case consumeErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after basic.cancel")
	}

	require.ErrorIs(t, consumeErr, ErrConsumerCancelled, "Consume must return ErrConsumerCancelled on basic.cancel")
	assert.Contains(t, consumeErr.Error(), consumer.tag, "the returned error must carry the cancelled consumer tag")
	assert.Equal(t, 1, cm.cancelled, "RecordCancelled must be called once for basic.cancel")
	assert.Equal(t, "testq", cm.cancelledQueue, "RecordCancelled queue must be the consumer queue")
	assert.Equal(t, cancelReasonUnknown, cm.cancelledReason,
		"metric reason must be the bounded enum, not the unbounded consumer tag")
	assert.NotContains(t, cm.cancelledReason, "ctag-",
		"metric reason must never be the consumer-tag UUID (cardinality explosion)")
}

func TestConsumer_Consume_DeliveryChannelClosed_WaitsForCtxCancel(t *testing.T) {
	// When cur.ch receives !ok (AMQP channel closed), ConsumeRaw enters the inner
	// select waiting for resubCh or ctx cancel. Without a resubCh signal, ctx
	// cancel must terminate cleanly.
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	close(deliveryCh) // simulate AMQP channel closed
	cancel()          // trigger ctx.Done in the inner select

	select {
	case <-consumeDone:
	case <-time.After(time.Second):
		t.Fatal("ConsumeRaw did not exit after ctx cancel following channel close")
	}
}

func TestConsumer_Consume_Concurrency(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Concurrency(3).Prefetch(8).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 10)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	unblock := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				cur := maxInFlight.Load()
				if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
					break
				}
			}
			<-unblock
			return nil
		})
	}()

	// send 3 messages; with concurrency=3 all 3 should run simultaneously
	for range 3 {
		deliveryCh <- amqp091.Delivery{
			Body:         []byte(`"x"`),
			ContentType:  "application/json",
			Acknowledger: &fakeAcknowledger{},
		}
	}

	// wait until all 3 are in flight, then release
	require.Eventually(t, func() bool {
		return inFlight.Load() == 3
	}, time.Second, 5*time.Millisecond)

	close(unblock)
	cancel()
	<-done

	assert.Equal(t, int32(3), maxInFlight.Load())
}

// — helpers ——————————————————————————————————————————————————————————————

func newFakeConsumerConn(t *testing.T) *Connection {
	t.Helper()
	conn := &Connection{}
	conn.opts.logger = log.NewNoOp()
	mc := &managedConn{opts: &conn.opts}
	conn.conConns = []*managedConn{mc}
	conn.pubConns = []*managedConn{mc}
	return conn
}

// fakeAcknowledger implements amqp091.Acknowledger.
type fakeAcknowledger struct {
	ackFn  func(tag uint64, multiple bool) error
	nackFn func(tag uint64, multiple, requeue bool) error
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	if f.ackFn != nil {
		return f.ackFn(tag, multiple)
	}
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if f.nackFn != nil {
		return f.nackFn(tag, multiple, requeue)
	}
	return nil
}

func (f *fakeAcknowledger) Reject(_ uint64, _ bool) error { return nil }

// countingConsumerMetrics counts specific metric increments.
// cancelledNotify, if non-nil, is closed on the first RecordCancelled call
// (exactly once, via cancelledOnce) so callers can synchronise without a sleep.
type countingConsumerMetrics struct {
	metrics.NoOpConsumerMetrics
	decodeErrors    int
	lastMessageType string
	handlerTimeouts int
	channelAborts   int
	cancelled       int
	cancelledQueue  string
	cancelledReason string
	cancelledOnce   sync.Once
	cancelledNotify chan struct{} // closed on first RecordCancelled call; may be nil
}

func (c *countingConsumerMetrics) RecordHandler(_, messageType, outcome string, _ time.Duration) {
	c.lastMessageType = messageType
	if outcome == "decode_error" {
		c.decodeErrors++
	}
}

func (c *countingConsumerMetrics) RecordHandlerTimeout(_ string) {
	c.handlerTimeouts++
}

func (c *countingConsumerMetrics) RecordHandlerAbortedChannelClosed(_ string) {
	c.channelAborts++
}

func (c *countingConsumerMetrics) RecordCancelled(queue, reason string) {
	c.cancelled++
	c.cancelledQueue = queue
	c.cancelledReason = reason
	if c.cancelledNotify != nil {
		c.cancelledOnce.Do(func() { close(c.cancelledNotify) })
	}
}

// captureLogger captures warning log lines.
type captureLogger struct {
	onWarning func(msg string)
}

func (l *captureLogger) Debug(_ string)            {}
func (l *captureLogger) Info(_ string)             {}
func (l *captureLogger) Warning(msg string)        { l.onWarning(msg) }
func (l *captureLogger) Error(_ string)            {}
func (l *captureLogger) Debugf(_ string, _ ...any) {}
func (l *captureLogger) Infof(_ string, _ ...any)  {}
func (l *captureLogger) Warningf(format string, args ...any) {
	l.onWarning(fmt.Sprintf(format, args...))
}
func (l *captureLogger) Errorf(_ string, _ ...any) {}

// panicCodec simulates a codec that panics during Decode.
type panicCodec struct{}

func (panicCodec) Encode(_ any) ([]byte, error) { return nil, nil }
func (panicCodec) Decode(_ []byte, _ any) error { panic("codec exploded") }
func (panicCodec) ContentType() string          { return "application/octet-stream" }

// — LATER-33: MessageId length bound in counterB key —————————————————————

// TestConsumer_counterBKey_longMessageId_isTruncated verifies that MessageId
// values longer than maxMsgIDKeyLen are truncated before being used as a
// sync.Map key, bounding the memory impact of messages from a compromised
// or misconfigured producer.
func TestConsumer_counterBKey_longMessageId_isTruncated(t *testing.T) {
	longID := strings.Repeat("x", maxMsgIDKeyLen+100)
	key := counterBKeyForMsgID(longID)
	// "mid:" prefix + truncated msgID — total key length must not exceed
	// len("mid:") + maxMsgIDKeyLen.
	maxExpected := len("mid:") + maxMsgIDKeyLen
	assert.LessOrEqual(t, len(key), maxExpected,
		"counterBKey must truncate MessageId to maxMsgIDKeyLen bytes")
}

// TestConsumer_counterBKey_multibyteMessageId_truncatesAtRuneBoundary verifies
// that truncation never splits a multi-byte UTF-8 rune, so the resulting key
// is always valid UTF-8. A MessageId built from 3-byte CJK characters just
// long enough to cross the byte boundary is used as the adversarial input.
func TestConsumer_counterBKey_multibyteMessageId_truncatesAtRuneBoundary(t *testing.T) {
	// "中" is U+4E2D, encoded as 3 bytes (0xE4 0xB8 0xAD) in UTF-8.
	// Repeat enough times to exceed maxMsgIDKeyLen bytes.
	longID := strings.Repeat("中", maxMsgIDKeyLen) // 3×maxMsgIDKeyLen bytes
	key := counterBKeyForMsgID(longID)
	// The key must be valid UTF-8 — no rune was split at the cut point.
	assert.True(t, utf8.ValidString(key),
		"truncated key must be valid UTF-8; got %q", key)
	// The key must still be bounded.
	assert.LessOrEqual(t, len(key), len("mid:")+maxMsgIDKeyLen,
		"truncated key must not exceed the length bound")
}

// TestConsumer_counterBKey_shortMessageId_isNotTruncated verifies that
// MessageId values within the limit pass through unchanged.
func TestConsumer_counterBKey_shortMessageId_isNotTruncated(t *testing.T) {
	shortID := strings.Repeat("a", maxMsgIDKeyLen)
	key := counterBKeyForMsgID(shortID)
	assert.Equal(t, "mid:"+shortID, key,
		"counterBKey must not truncate MessageId within limit")
}

// TestConsumer_counterBKey_emptyMessageId_usesFallback verifies that an
// empty MessageId uses the "dlv:" family and embeds both consumerTag and
// deliveryTag so the key is unique per in-flight delivery.
func TestConsumer_counterBKey_emptyMessageId_usesFallback(t *testing.T) {
	key := counterBKeyForDeliveryTag("ctag-abc", 42)
	assert.True(t, strings.HasPrefix(key, "dlv:"),
		"empty MessageId must use dlv: key family; got %q", key)
	assert.Contains(t, key, "ctag-abc",
		"fallback key must embed the consumerTag; got %q", key)
	assert.Contains(t, key, "42",
		"fallback key must embed the deliveryTag; got %q", key)
}

// TestConsumer_counterBKey_continuationBytesMessageId_doesNotCollide verifies
// that a MessageId composed entirely of UTF-8 continuation bytes (invalid UTF-8)
// does not produce the degenerate "mid:" key that would collapse all such
// messages onto a single sync.Map slot, corrupting the redelivery counter.
func TestConsumer_counterBKey_continuationBytesMessageId_doesNotCollide(t *testing.T) {
	// Bytes 0x80-0xBF are UTF-8 continuation bytes. A string of them is invalid
	// UTF-8 but is a legal Go string. truncateAtRuneBoundary walks back to n=0,
	// so without the empty-result guard, counterBKeyForMsgID would return "mid:".
	invalidUTF8 := strings.Repeat("\x80", maxMsgIDKeyLen+1)
	key := counterBKeyForMsgID(invalidUTF8)
	// The contract is that counterBKeyForMsgID returns "" (not the degenerate
	// "mid:" key) so the call site falls back to the unique delivery-tag key.
	assert.Empty(t, key,
		"pure continuation bytes must yield an empty key so the caller uses the dlv: fallback; got %q", key)
}

// TestConsumer_counterBKeyForDeliveryTag_longConsumerTag_isTruncated verifies
// that a consumerTag longer than maxConsumerTagKeyLen bytes is truncated before
// being embedded in the dlv: key, bounding per-key memory in the sync.Map.
func TestConsumer_counterBKeyForDeliveryTag_longConsumerTag_isTruncated(t *testing.T) {
	longTag := strings.Repeat("t", maxConsumerTagKeyLen+100)
	key := counterBKeyForDeliveryTag(longTag, 1)
	assert.True(t, strings.HasPrefix(key, "dlv:"),
		"dlv: prefix must be preserved; got %q", key)
	// The key must not be longer than "dlv:" + maxConsumerTagKeyLen + ":" + digits.
	// The rough bound is: 4 + maxConsumerTagKeyLen + 1 + 20 (max uint64 decimal).
	maxExpected := len("dlv:") + maxConsumerTagKeyLen + 1 + 20
	assert.LessOrEqual(t, len(key), maxExpected,
		"counterBKeyForDeliveryTag must truncate consumerTag to maxConsumerTagKeyLen bytes")
}

// TestTruncateAtRuneBoundary verifies edge cases of the helper directly:
// n=0, n exactly at a rune boundary, and an all-continuation-bytes input.
func TestTruncateAtRuneBoundary(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		n        int
		wantLen  int // expected byte length of result
		wantUTF8 bool
	}{
		{
			name:     "n=0 returns empty",
			s:        "hello",
			n:        0,
			wantLen:  0,
			wantUTF8: true,
		},
		{
			name:     "n >= len(s) returns full string",
			s:        "hi",
			n:        100,
			wantLen:  2,
			wantUTF8: true,
		},
		{
			name:     "cut exactly on ASCII rune boundary (loop does not walk back)",
			s:        "abcde",
			n:        3,
			wantLen:  3,
			wantUTF8: true,
		},
		{
			name:     "cut in middle of 3-byte CJK rune walks back to rune start",
			s:        "中文", // 6 bytes: E4 B8 AD E6 96 87
			n:        4,    // lands on 0x96 (continuation byte of 文)
			wantLen:  3,    // backs up to start of 文 then further to end of 中 = 3
			wantUTF8: true,
		},
		{
			name:     "all continuation bytes walks back to n=0 → empty",
			s:        strings.Repeat("\x80", 10),
			n:        5,
			wantLen:  0,
			wantUTF8: true,
		},
		{
			name:     "4-byte emoji cut at byte 3 backs up to start",
			s:        "\xf0\x9f\x98\x80", // U+1F600 😀
			n:        3,
			wantLen:  0, // walks back past all 3 continuation bytes to n=0
			wantUTF8: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateAtRuneBoundary(tc.s, tc.n)
			assert.Equal(t, tc.wantLen, len(got), "unexpected byte length")
			assert.Equal(t, tc.wantUTF8, utf8.ValidString(got), "UTF-8 validity mismatch")
		})
	}
}
