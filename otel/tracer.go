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
	"go.opentelemetry.io/otel/codes"
)

// Span is a single named operation within a distributed trace.
type Span interface {
	// End marks the end of the span.
	End()
	// SetAttributes adds key-value pairs to the span.
	SetAttributes(attrs ...attribute.KeyValue)
	// SetStatus sets the span status code and an optional human-readable
	// description. Publisher and Consumer set codes.Error on every failure
	// class so trace UIs render failed spans red (SPEC §6.9).
	SetStatus(code codes.Code, description string)
	// RecordError records an error as a span event.
	RecordError(err error)
}

// Tracer creates spans for tracing AMQP Publisher and Consumer operations.
// Use NoOpTracer when tracing is not required.
type Tracer interface {
	// Start begins a new span and returns an updated context and the span.
	Start(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, Span)
}

// Link associates the span being started with another span context — typically
// the producer span of an incoming message, recovered via Propagator.Extract on
// that message's headers. BatchConsumer creates one Link per message in a batch
// so a single batch span fans in to every producer trace (SPEC §6.9
// "BatchConsumer Links").
type Link struct {
	// Context carries the linked span context, typically the result of
	// Propagator.Extract on an incoming message's headers. A Context with no
	// valid span context contributes no Link.
	Context context.Context
}

// LinkingTracer is an optional extension of Tracer. A Tracer that can attach
// OTel span Links implements StartWithLinks; BatchConsumer uses it to link the
// batch process span to every incoming message's trace context (SPEC §6.9
// "BatchConsumer Links"). Tracers that do not implement it transparently fall
// back to Start with no links, so the interface stays backward-compatible.
type LinkingTracer interface {
	Tracer
	// StartWithLinks begins a new span carrying the given links and returns an
	// updated context and the span.
	StartWithLinks(ctx context.Context, spanName string, links []Link, attrs ...attribute.KeyValue) (context.Context, Span)
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
func (noOpSpan) SetStatus(_ codes.Code, _ string)      {}
func (noOpSpan) RecordError(_ error)                   {}
