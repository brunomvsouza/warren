package warren

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	warrenotel "github.com/brunomvsouza/warren/otel"
)

// markerDelivery builds a JSON-string delivery carrying a producer traceparent
// derived from the given trace/span id seeds, so a batch test can assert one
// span Link per message back to its distinct producer trace.
func markerDelivery(tag uint64, body string, tidSeed, sidSeed byte) amqp091.Delivery {
	var tid trace.TraceID
	var sid trace.SpanID
	tid[0], tid[15] = tidSeed, tidSeed
	sid[0], sid[7] = sidSeed, sidSeed
	producer := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	hdrs := amqp091.Table{}
	warrenotel.NewPropagator().Inject(trace.ContextWithSpanContext(context.Background(), producer), hdrs)
	return amqp091.Delivery{
		DeliveryTag:  tag,
		Body:         []byte(`"` + body + `"`),
		ContentType:  "application/json",
		Headers:      hdrs,
		Acknowledger: &fakeAcknowledger{},
	}
}

func newTracedBatchConsumer(t *testing.T, tr warrenotel.Tracer, deliveryCh chan amqp091.Delivery, size uint) *BatchConsumer[string] {
	t.Helper()
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(size).
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	return bc
}

// — process_batch span: name, attributes, outcome, and one Link per message ——

func TestBatchConsumer_processBatchSpan_linksAndOutcome(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	bc := newTracedBatchConsumer(t, tr, deliveryCh, 3)
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error { return nil })
	}()

	// Three messages, each carrying a distinct producer trace.
	deliveryCh <- markerDelivery(1, "a", 11, 21)
	deliveryCh <- markerDelivery(2, "b", 12, 22)
	deliveryCh <- markerDelivery(3, "c", 13, 23)

	assert.Eventually(t, func() bool { return tr.only() != nil }, time.Second, 10*time.Millisecond,
		"expected exactly one process_batch span")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.Equal(t, "q process_batch", span.name)
	assert.True(t, span.ended)

	op, ok := span.attr("messaging.operation.type")
	require.True(t, ok)
	assert.Equal(t, "process", op.AsString())

	count, ok := span.attr("messaging.batch.message_count")
	require.True(t, ok)
	assert.Equal(t, int64(3), count.AsInt64())

	outcome, ok := span.attr("messaging.rabbitmq.outcome")
	require.True(t, ok)
	assert.Equal(t, "ack", outcome.AsString())
	assert.Equal(t, codes.Ok, span.status)

	// One Link per message, each resolving to its own producer trace.
	require.Len(t, span.links, 3, "batch span must link every incoming message (fan-in)")
	linkedTraces := map[trace.TraceID]bool{}
	for _, l := range span.links {
		sc := trace.SpanContextFromContext(l.Context)
		require.True(t, sc.IsValid(), "each link must carry a valid producer span context")
		linkedTraces[sc.TraceID()] = true
	}
	assert.Len(t, linkedTraces, 3, "the three links must point at three distinct producer traces")
}

// — process_batch span: handler error stamps the nack outcome ————————————————

func TestBatchConsumer_processBatchSpan_errorOutcome(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	bc := newTracedBatchConsumer(t, tr, deliveryCh, 2)
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error { return ErrPoison })
	}()

	deliveryCh <- markerDelivery(1, "a", 11, 21)
	deliveryCh <- markerDelivery(2, "b", 12, 22)

	assert.Eventually(t, func() bool { return tr.only() != nil }, time.Second, 10*time.Millisecond,
		"expected the process_batch span")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "nack_no_requeue", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrPoison", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — process_batch span: handler panic still ends the span ————————————————————

func TestBatchConsumer_processBatchSpan_handlerPanic_endsSpan(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	bc := newTracedBatchConsumer(t, tr, deliveryCh, 2)
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error { panic("batch boom") })
	}()

	deliveryCh <- markerDelivery(1, "a", 11, 21)
	deliveryCh <- markerDelivery(2, "b", 12, 22)

	// tr.only() is mutex-guarded; wait for the span to appear, then synchronise on
	// the consumer goroutine exit before reading span fields directly.
	assert.Eventually(t, func() bool { return tr.only() != nil }, time.Second, 10*time.Millisecond,
		"the batch span must be created")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended, "the batch span must be ended even when the handler panics")
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "nack_no_requeue", outcome.AsString(), "a panic maps to nack without requeue")
	assert.Equal(t, codes.Error, span.status)
}

// — process_batch span: timeout after a manual ack still carries an outcome ——————

// When the handler acks manually before the deadline but keeps running past
// HandlerTimeout, the timeout branch must still stamp the span with a terminal
// outcome: the span is owned by this flush and ended here, so a missing outcome
// would be permanent (there is no later path that would stamp it).
func TestBatchConsumer_processBatchSpan_timeoutAfterManualAck_stampsTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(50 * time.Millisecond).
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hctx context.Context, b *Batch[string]) error {
			// Ack manually before the deadline, then block past it: the handler
			// exceeds HandlerTimeout but has already applied its own verdict.
			_ = b.Ack()
			<-hctx.Done()
			return nil
		})
	}()

	deliveryCh <- markerDelivery(1, "a", 11, 21)
	deliveryCh <- markerDelivery(2, "b", 12, 22)

	// Wait until the span carries an outcome — the whole point of the fix.
	assert.Eventually(t, func() bool {
		s := tr.only()
		if s == nil {
			return false
		}
		_, ok := s.attr("messaging.rabbitmq.outcome")
		return ok
	}, 2*time.Second, 10*time.Millisecond, "a timed-out batch span must carry an outcome even when the handler acked manually")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, ok := span.attr("messaging.rabbitmq.outcome")
	require.True(t, ok)
	assert.Equal(t, "timeout", outcome.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — process_batch span: non-linking tracer falls back to a plain Start ————————

// nonLinkingTracer implements only warrenotel.Tracer (NOT warrenotel.LinkingTracer)
// so startBatchSpan's fallback path — a plain Start with no Links — is exercised.
type nonLinkingTracer struct {
	mu    sync.Mutex
	spans []*recordingSpan
}

func (t *nonLinkingTracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	s := &recordingSpan{name: name}
	s.attrs = append(s.attrs, attrs...)
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return ctx, s
}

func (t *nonLinkingTracer) only() *recordingSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.spans) != 1 {
		return nil
	}
	return t.spans[0]
}

func TestBatchConsumer_processBatchSpan_nonLinkingTracerFallback(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &nonLinkingTracer{}
	// Guard: this tracer must NOT satisfy LinkingTracer, otherwise the fallback
	// path under test would not actually be exercised.
	_, isLinking := interface{}(tr).(warrenotel.LinkingTracer)
	require.False(t, isLinking, "nonLinkingTracer must not implement LinkingTracer")

	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error { return nil })
	}()

	deliveryCh <- markerDelivery(1, "a", 11, 21)
	deliveryCh <- markerDelivery(2, "b", 12, 22)

	assert.Eventually(t, func() bool { return tr.only() != nil }, 2*time.Second, 10*time.Millisecond,
		"expected the process_batch span from the non-linking fallback")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.Equal(t, "q process_batch", span.name)
	assert.True(t, span.ended)
	assert.Empty(t, span.links, "a non-linking tracer must produce a span with no Links")

	count, ok := span.attr("messaging.batch.message_count")
	require.True(t, ok)
	assert.Equal(t, int64(2), count.AsInt64())

	outcome, ok := span.attr("messaging.rabbitmq.outcome")
	require.True(t, ok)
	assert.Equal(t, "ack", outcome.AsString())
	assert.Equal(t, codes.Ok, span.status)
}

// batchDelivery builds a minimal JSON-string delivery with a caller-supplied
// acknowledger so timeout tests can count ack/nack frames. Headers are omitted —
// these tests assert outcomes and frame counts, not producer-trace links.
func batchDelivery(tag uint64, body string, ackr amqp091.Acknowledger) amqp091.Delivery {
	return amqp091.Delivery{
		DeliveryTag:  tag,
		Body:         []byte(`"` + body + `"`),
		ContentType:  "application/json",
		Acknowledger: ackr,
	}
}

// — process_batch span: outer-ctx cancel ends the span with no outcome ——————————

// When the OUTER ctx is cancelled while a batch handler is mid-flight (consumer
// lifecycle end, not a HandlerTimeout), the timeout select takes its non-deadline
// branch: the span is ended via defer but carries no outcome and no Error status —
// it is not a message outcome. This pins that documented contract; the previous
// commit added the branch but no traced test asserted it.
//
// Determinism: the handler blocks on a dedicated gate (not hctx.Done()), so handlerDone
// stays empty after the outer ctx is cancelled — the flush select can only pick
// hCtx.Done(). The test waits on the testHookBeforeTimeoutDrain seam to confirm the
// select committed to that branch, then releases the gate so the drain completes. This
// removes the prior handlerDone-vs-hCtx.Done() race (Go selects at random when both are
// ready), which flaked when the handler's nil return won and stamped a success outcome.
func TestBatchConsumer_processBatchSpan_outerCtxCancel_noOutcome(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(10 * time.Second). // long: the deadline must never fire
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// committed fires once the handler-timeout select has committed to the outer-ctx-cancel
	// branch. The handler blocks on a dedicated gate (not hctx.Done()) so handlerDone stays
	// empty until we release it — this makes the select's choice deterministic instead of
	// racing the handler's nil return (→ success outcome) against hCtx.Done() (→ no outcome).
	committed := make(chan struct{})
	bc.testHookBeforeTimeoutDrain = func() { close(committed) }

	entered := make(chan struct{})
	canReturn := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, _ *Batch[string]) error {
			close(entered)
			<-canReturn // released only after the select committed to the cancel branch
			return nil
		})
	}()

	deliveryCh <- batchDelivery(1, "a", &fakeAcknowledger{})
	deliveryCh <- batchDelivery(2, "b", &fakeAcknowledger{})

	<-entered // span is open and the handler goroutine is running
	cancel()  // cancel the OUTER ctx — lifecycle end, not a timeout
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("select did not commit to the ctx-done branch in time")
	}
	close(canReturn) // unblock the handler so <-handlerDone drains and Consume returns
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended, "span must be ended via defer on outer-ctx cancel")
	_, ok := span.attr("messaging.rabbitmq.outcome")
	assert.False(t, ok, "outer-ctx cancel must not stamp a message outcome")
	assert.Equal(t, codes.Unset, span.status, "no SetStatus call on the outer-ctx-cancel path")
	assert.Empty(t, span.errs, "outer-ctx cancel must not record an error")
}

// — process_batch span: timeout without a manual ack stamps timeout ——————————————

// The common timeout shape — handler blocks past HandlerTimeout and never acks —
// must stamp the span with outcome=timeout and emit exactly one synthetic nack.
func TestBatchConsumer_processBatchSpan_timeoutNoAck_stampsTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(50 * time.Millisecond).
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hctx context.Context, _ *Batch[string]) error {
			<-hctx.Done() // exceed the deadline; never ack
			return nil
		})
	}()

	var acks, nacks atomic.Int32
	ackr := &fakeAcknowledger{
		ackFn:  func(_ uint64, _ bool) error { acks.Add(1); return nil },
		nackFn: func(_ uint64, _, _ bool) error { nacks.Add(1); return nil },
	}
	deliveryCh <- batchDelivery(1, "a", ackr)
	deliveryCh <- batchDelivery(2, "b", ackr)

	assert.Eventually(t, func() bool {
		s := tr.only()
		if s == nil {
			return false
		}
		_, ok := s.attr("messaging.rabbitmq.outcome")
		return ok
	}, 2*time.Second, 10*time.Millisecond, "the timed-out batch span must carry an outcome")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "timeout", outcome.AsString())
	assert.Equal(t, codes.Error, span.status)
	assert.Equal(t, int32(1), nacks.Load(), "an unacked timed-out batch is nacked exactly once")
	assert.Zero(t, acks.Load())
}

// — process_batch span: a panic AFTER the deadline still stamps timeout ——————————

// If the handler panics after HandlerTimeout has already fired, the flush is on the
// timeout branch (the deadline close claimed the select): the recovered panic error
// is discarded by the <-handlerDone drain, the span outcome stays timeout (not a
// panic-derived nack), and the panic never escapes Consume.
func TestBatchConsumer_processBatchSpan_panicAfterTimeout_stampsTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	tr := &recordingTracer{}
	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(50 * time.Millisecond).
		Tracer(tr).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(hctx context.Context, _ *Batch[string]) error {
			<-hctx.Done() // exceed the deadline first
			panic("after timeout")
		})
	}()

	var acks, nacks atomic.Int32
	ackr := &fakeAcknowledger{
		ackFn:  func(_ uint64, _ bool) error { acks.Add(1); return nil },
		nackFn: func(_ uint64, _, _ bool) error { nacks.Add(1); return nil },
	}
	deliveryCh <- batchDelivery(1, "a", ackr)
	deliveryCh <- batchDelivery(2, "b", ackr)

	assert.Eventually(t, func() bool {
		s := tr.only()
		if s == nil {
			return false
		}
		_, ok := s.attr("messaging.rabbitmq.outcome")
		return ok
	}, 2*time.Second, 10*time.Millisecond, "the timed-out batch span must carry an outcome")

	cancel()
	require.NoError(t, <-done, "a panic after the deadline must not escape Consume")

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "timeout", outcome.AsString(), "the timeout outcome wins over a post-deadline panic")
	assert.Equal(t, codes.Error, span.status)
	assert.Equal(t, int32(1), nacks.Load(), "the timeout verdict nacks the unacked batch once")
	assert.Zero(t, acks.Load())
}

// — process_batch span: a per-batch ack inside the timeout window ————————————————

// hookConsumerMetrics runs onHandlerTimeout from inside the batch timeout branch:
// RecordHandlerTimeout is called there, after the select committed to the deadline
// case and after the alreadyAcked read, but before the re-check under the lock.
// A test uses it to flip batch.acked inside that exact window, deterministically
// driving the double-checked guard's else branch (batch_consumer.go:315-317).
type hookConsumerMetrics struct {
	metrics.NoOpConsumerMetrics
	onHandlerTimeout func()
}

func (m *hookConsumerMetrics) RecordHandlerTimeout(_ string) {
	if m.onHandlerTimeout != nil {
		m.onHandlerTimeout()
	}
}

// When a per-batch ack lands in the window between the alreadyAcked read and the
// re-check under the lock, the synthetic timeout nack must be suppressed (no double
// frame), yet the span must still carry outcome=timeout. This exercises the else
// branch of the double-checked guard, which is otherwise unreachable deterministically.
func TestBatchConsumer_processBatchSpan_timeoutAckInWindow_suppressesSyntheticNack(t *testing.T) {
	defer goleak.VerifyNone(t)

	ackGate := make(chan struct{}) // closed by the metrics hook to release the ack
	ackDone := make(chan struct{}) // closed by the handler once it has acked

	tr := &recordingTracer{}
	rec := &hookConsumerMetrics{
		onHandlerTimeout: func() {
			close(ackGate) // let the handler ack inside the timeout window
			<-ackDone      // block the flush until batch.acked is set
		},
	}

	deliveryCh := make(chan amqp091.Delivery, 10)
	conn := newFakeConsumerConn(t)
	bc, err := BatchConsumerFor[string](conn).
		Queue("q").
		Size(2).
		HandlerTimeout(50 * time.Millisecond).
		Tracer(tr).
		Metrics(rec).
		Codec(codec.NewJSON()).
		Build()
	require.NoError(t, err)
	bc.deliveryCh = deliveryCh
	defer func() { _ = bc.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			// Do NOT watch hctx: stay blocked until the hook releases the ack, so
			// the flush deterministically enters the timeout branch first, then the
			// ack lands inside the alreadyAcked/recheck window.
			<-ackGate
			_ = b.Ack()
			close(ackDone)
			return nil
		})
	}()

	var acks, nacks atomic.Int32
	ackr := &fakeAcknowledger{
		ackFn:  func(_ uint64, _ bool) error { acks.Add(1); return nil },
		nackFn: func(_ uint64, _, _ bool) error { nacks.Add(1); return nil },
	}
	deliveryCh <- batchDelivery(1, "a", ackr)
	deliveryCh <- batchDelivery(2, "b", ackr)

	assert.Eventually(t, func() bool {
		s := tr.only()
		if s == nil {
			return false
		}
		_, ok := s.attr("messaging.rabbitmq.outcome")
		return ok
	}, 2*time.Second, 10*time.Millisecond, "the timed-out batch span must carry an outcome")

	cancel()
	require.NoError(t, <-done)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "timeout", outcome.AsString())
	assert.Equal(t, codes.Error, span.status)
	assert.Equal(t, int32(1), acks.Load(), "the in-window manual ack is the only frame")
	assert.Zero(t, nacks.Load(), "the synthetic timeout nack must be suppressed")
}
