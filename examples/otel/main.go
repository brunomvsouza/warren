// Package main demonstrates OpenTelemetry trace propagation through Warren: a
// publish span and a consume span that share one trace, linked parent → child
// across the broker via W3C TraceContext headers (SPEC §6.9).
//
// What this example demonstrates:
//   - Adapting a real OpenTelemetry SDK trace.Tracer to warren/otel.Tracer (the
//     small interface Warren depends on, so it never imports the OTel SDK
//     itself). The adapter is ~20 lines and is the only wiring you write.
//   - WithTracer(adapter): the Publisher then emits a "<exchange> publish" span
//     and injects the trace context into the AMQP headers; the Consumer extracts
//     it and emits a "<queue> process" span as a CHILD of the publish span.
//   - Trace continuity: the example reads the two recorded spans back and proves
//     they share a trace-id and that the consume span's parent is the publish
//     span's span-id — i.e. one distributed trace end to end.
//
// How to run:
//
//	go run ./examples/otel
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.otel" (topic, non-durable, auto-delete)
//   - Creates queue "warren.examples.otel.events" (non-durable, auto-delete)
//   - Binds the queue to the exchange with routing key "event.#"
//
// The example exits 0 once the publish and consume spans are confirmed to be
// part of the same trace with the correct parent linkage; non-zero otherwise.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/brunomvsouza/warren"
	warrenotel "github.com/brunomvsouza/warren/otel"
)

// Event is the payload type for this example.
type Event struct {
	ID string `json:"id"`
}

const (
	exchange       = "warren.examples.otel"
	queue          = "warren.examples.otel.events"
	routingKey     = "event.created"
	exampleTimeout = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("otel example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	// — OTel SDK setup ————————————————————————————————————————————————————————
	// A SpanRecorder captures finished spans in memory so the example can read
	// them back and prove trace continuity. In a real service you would register
	// a batch exporter (OTLP, Jaeger, stdout) instead; the wiring is identical.
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()

	// Adapt the SDK tracer to the warren/otel.Tracer interface.
	tracer := otelAdapter{tracer: tp.Tracer("github.com/brunomvsouza/warren/examples/otel")}

	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url), warren.WithTracer(tracer))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: exchange, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []warren.Queue{
			{Name: queue, Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: exchange, Queue: queue, RoutingKey: "event.#"},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	pub, err := warren.PublisherFor[Event](conn).
		Exchange(exchange).
		RoutingKey(routingKey).
		ConfirmTimeout(10 * time.Second).
		Build()
	if err != nil {
		return fmt.Errorf("build publisher: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = pub.Close(closeCtx)
	}()

	// — Consume one message, then stop ————————————————————————————————————————
	consumer, err := warren.ConsumerFor[Event](conn).
		Queue(queue).
		Concurrency(1).
		Prefetch(1).
		Build()
	if err != nil {
		return fmt.Errorf("build consumer: %w", err)
	}

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	handled := make(chan struct{}, 1)
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Consume(consumerCtx, func(_ context.Context, e Event) error {
			log.Printf("consumer: handled event id=%s", e.ID)
			select {
			case handled <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	// Publish a single event. The publish span starts a fresh root trace; its
	// context is injected into the message headers for the consumer to continue.
	if err := pub.Publish(ctx, warren.Message[Event]{Body: &Event{ID: "evt-1"}}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("publisher: published event id=evt-1")

	select {
	case <-handled:
	case <-time.After(15 * time.Second):
		return fmt.Errorf("timed out waiting for the message to be consumed")
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before consume: %w", ctx.Err())
	}

	cancelConsumer()
	if err := <-consumerErr; err != nil {
		return fmt.Errorf("consumer returned error: %w", err)
	}

	// Flush so the consume span is recorded before we inspect.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()
	if err := tp.ForceFlush(flushCtx); err != nil {
		return fmt.Errorf("flush spans: %w", err)
	}

	return assertTraceContinuity(recorder.Ended())
}

// assertTraceContinuity finds the publish and consume spans among the recorded
// spans and proves they form one trace with the consume span as a child of the
// publish span.
func assertTraceContinuity(spans []sdktrace.ReadOnlySpan) error {
	var publishSpan, consumeSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch {
		case strings.HasSuffix(s.Name(), " publish"):
			publishSpan = s
		case strings.HasSuffix(s.Name(), " process"):
			consumeSpan = s
		}
	}
	if publishSpan == nil {
		return fmt.Errorf("no publish span recorded (got %d spans)", len(spans))
	}
	if consumeSpan == nil {
		return fmt.Errorf("no consume span recorded (got %d spans)", len(spans))
	}

	pubSC := publishSpan.SpanContext()
	conSC := consumeSpan.SpanContext()
	conParent := consumeSpan.Parent()

	log.Printf("publish span: trace=%s span=%s", pubSC.TraceID(), pubSC.SpanID())
	log.Printf("consume span: trace=%s span=%s parent=%s",
		conSC.TraceID(), conSC.SpanID(), conParent.SpanID())

	if pubSC.TraceID() != conSC.TraceID() {
		return fmt.Errorf("trace-id mismatch: publish=%s consume=%s — propagation broken",
			pubSC.TraceID(), conSC.TraceID())
	}
	if conParent.SpanID() != pubSC.SpanID() {
		return fmt.Errorf("parent mismatch: consume parent=%s want publish span=%s",
			conParent.SpanID(), pubSC.SpanID())
	}

	log.Printf("otel example complete — publish and consume spans share trace %s with correct parent linkage",
		pubSC.TraceID())
	return nil
}

// otelAdapter adapts an OpenTelemetry SDK trace.Tracer to the warren/otel.Tracer
// interface. Warren depends only on this tiny interface so the library never
// imports the OTel SDK; the adapter is the bridge a caller provides.
type otelAdapter struct {
	tracer oteltrace.Tracer
}

func (a otelAdapter) Start(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	ctx, span := a.tracer.Start(ctx, spanName, oteltrace.WithAttributes(attrs...))
	return ctx, spanAdapter{span: span}
}

// spanAdapter adapts an OTel SDK trace.Span to warren/otel.Span.
type spanAdapter struct {
	span oteltrace.Span
}

func (s spanAdapter) End()                                      { s.span.End() }
func (s spanAdapter) SetAttributes(attrs ...attribute.KeyValue) { s.span.SetAttributes(attrs...) }
func (s spanAdapter) SetStatus(code codes.Code, description string) {
	s.span.SetStatus(code, description)
}
func (s spanAdapter) RecordError(err error) { s.span.RecordError(err) }
