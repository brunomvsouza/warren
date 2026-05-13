// Package otel provides the Tracer interface used by Publisher and Consumer
// to emit OpenTelemetry spans, and a Propagator that injects and extracts
// W3C TraceContext headers from AMQP message headers.
//
// See SPEC §6.9 for the span-attribute matrix populated on the publish and
// consume paths. Publisher and Consumer populate those attributes in their
// respective implementations; this package is the hook point.
//
// NoOpTracer is the default Tracer used by Connection when no WithTracer
// option is provided. It satisfies the interface without importing the
// OpenTelemetry SDK, so there is no tracing overhead unless the caller
// supplies a real tracer.
package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

// Span is a single named operation within a distributed trace.
type Span interface {
	// End marks the end of the span.
	End()
	// SetAttributes adds key-value pairs to the span.
	SetAttributes(attrs ...attribute.KeyValue)
	// RecordError records an error as a span event.
	RecordError(err error)
}

// Tracer creates spans for tracing AMQP Publisher and Consumer operations.
// Use NoOpTracer when tracing is not required.
type Tracer interface {
	// Start begins a new span and returns an updated context and the span.
	Start(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, Span)
}

// NoOpTracer is a Tracer whose methods are no-ops. It is the default Tracer
// used by Connection when no WithTracer option is provided.
type NoOpTracer struct{}

// Start returns ctx unchanged and a no-op Span.
func (NoOpTracer) Start(ctx context.Context, _ string, _ ...attribute.KeyValue) (context.Context, Span) {
	return ctx, noOpSpan{}
}

type noOpSpan struct{}

func (noOpSpan) End()                                  {}
func (noOpSpan) SetAttributes(_ ...attribute.KeyValue) {}
func (noOpSpan) RecordError(_ error)                   {}
