//go:build integration

package warren_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	warrenotel "github.com/brunomvsouza/warren/otel"
)

// — in-memory recording tracer (package warren_test) ————————————————————————
//
// Mirrors the unit-test recordingTracer but lives in the external test package so
// the DLX integration test can wire the same tracer into a Publisher and two
// Consumers and then assert trace-id continuity across publish → process → DLX
// bounce → DLQ process.

type recSpan struct {
	mu     sync.Mutex
	name   string
	sc     trace.SpanContext
	parent trace.SpanContext
	attrs  []attribute.KeyValue
	ended  bool
}

func (s *recSpan) End()                                  { s.mu.Lock(); s.ended = true; s.mu.Unlock() }
func (s *recSpan) SetAttributes(a ...attribute.KeyValue) { s.mu.Lock(); s.attrs = append(s.attrs, a...); s.mu.Unlock() }
func (s *recSpan) SetStatus(codes.Code, string)          {}
func (s *recSpan) RecordError(error)                     {}

type recTracer struct {
	mu    sync.Mutex
	n     byte
	spans []*recSpan
}

func (t *recTracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	t.mu.Lock()
	t.n++
	n := t.n
	t.mu.Unlock()

	parent := trace.SpanContextFromContext(ctx)
	var tid trace.TraceID
	if parent.IsValid() {
		tid = parent.TraceID()
	} else {
		tid[0], tid[15] = 1, n
	}
	var sid trace.SpanID
	sid[0], sid[7] = 1, n
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	ctx = trace.ContextWithSpanContext(ctx, sc)

	s := &recSpan{name: name, sc: sc, parent: parent}
	s.attrs = append(s.attrs, attrs...)
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return ctx, s
}

func (t *recTracer) find(name string) *recSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, s := range t.spans {
		if s.name == name {
			return s
		}
	}
	return nil
}

// TestConsumer_tracing_DLX_continuity_integration (T28):
// Publishes one message, the source consumer nacks-without-requeue (ErrPoison),
// the broker dead-letters it to a DLQ preserving the original traceparent, and a
// second consumer on the DLQ processes it. Asserts both consumers' process spans
// share the original publisher trace-id and that the DLQ consumer span's parent
// resolves into the original publisher span.
func TestConsumer_tracing_DLX_continuity_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		inExch  = "test.trace.dlx.in"
		srcQ    = "test.trace.dlx.src"
		dlxExch = "test.trace.dlx.dlx"
		dlqQ    = "test.trace.dlx.dlq"
	)

	purgeQueues(t, url, srcQ, dlqQ)
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, inExch, dlxExch)
	})

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: inExch, Kind: warren.ExchangeDirect, Durable: false, AutoDelete: false},
			{Name: dlxExch, Kind: warren.ExchangeFanout, Durable: false, AutoDelete: false},
		},
		Queues: []warren.Queue{
			{Name: srcQ, Durable: false, Args: map[string]any{"x-dead-letter-exchange": dlxExch}},
			{Name: dlqQ, Durable: false},
		},
		Bindings: []warren.Binding{
			{Exchange: inExch, Queue: srcQ, RoutingKey: "k"},
			{Exchange: dlxExch, Queue: dlqQ, RoutingKey: ""},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	tr := &recTracer{}

	pub, err := warren.PublisherFor[string](conn).
		Exchange(inExch).
		RoutingKey("k").
		Tracer(tr).
		Build()
	require.NoError(t, err)
	body := "trace-me"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	// Source consumer: nack-without-requeue so the broker dead-letters the message.
	srcConsumer, err := warren.ConsumerFor[string](conn).Queue(srcQ).Prefetch(1).Tracer(tr).Build()
	require.NoError(t, err)
	srcCtx, srcCancel := context.WithCancel(ctx)
	srcDone := make(chan struct{})
	go func() {
		defer close(srcDone)
		_ = srcConsumer.Consume(srcCtx, func(_ context.Context, _ string) error {
			defer srcCancel()
			return warren.ErrPoison
		})
	}()
	select {
	case <-srcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("source consumer did not process the message")
	}

	// DLQ consumer: ack so the message is consumed cleanly.
	dlqConsumer, err := warren.ConsumerFor[string](conn).Queue(dlqQ).Prefetch(1).Tracer(tr).Build()
	require.NoError(t, err)
	dlqCtx, dlqCancel := context.WithCancel(ctx)
	dlqDone := make(chan struct{})
	go func() {
		defer close(dlqDone)
		_ = dlqConsumer.Consume(dlqCtx, func(_ context.Context, _ string) error {
			defer dlqCancel()
			return nil
		})
	}()
	select {
	case <-dlqDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DLQ consumer did not process the dead-lettered message")
	}

	publishSpan := tr.find(inExch + " publish")
	srcSpan := tr.find(srcQ + " process")
	dlqSpan := tr.find(dlqQ + " process")
	require.NotNil(t, publishSpan, "publisher span must exist")
	require.NotNil(t, srcSpan, "source consumer process span must exist")
	require.NotNil(t, dlqSpan, "DLQ consumer process span must exist")

	producerTrace := publishSpan.sc.TraceID()
	assert.Equal(t, producerTrace, srcSpan.sc.TraceID(),
		"source consumer process span must share the producer trace-id")
	assert.Equal(t, producerTrace, dlqSpan.sc.TraceID(),
		"DLQ consumer process span must share the producer trace-id (DLX preserves traceparent)")

	require.True(t, dlqSpan.parent.IsValid(), "DLQ process span must carry a parent span context")
	assert.Equal(t, publishSpan.sc.SpanID(), dlqSpan.parent.SpanID(),
		"DLQ process span parent must resolve into the original producer span")
}
