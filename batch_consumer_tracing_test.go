package warren

import (
	"context"
	"sync"
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
