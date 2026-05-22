package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
)

// Propagator injects and extracts OpenTelemetry trace context using W3C
// TraceContext headers (traceparent, tracestate) in AMQP message headers.
//
// The header keys traceparent and tracestate do not conflict with
// CloudEvents binary-mode ce-* headers.
type Propagator struct {
	prop propagation.TextMapPropagator
}

// NewPropagator creates a Propagator backed by W3C TraceContext propagation.
func NewPropagator() Propagator {
	return Propagator{prop: propagation.TraceContext{}}
}

// Inject injects the trace context from ctx into h using W3C traceparent
// and tracestate headers. h is typically warren.Headers (map[string]any).
func (p Propagator) Inject(ctx context.Context, h map[string]any) {
	p.prop.Inject(ctx, headerCarrier(h))
}

// Extract extracts a W3C trace context from h and returns a context with
// it attached. h is typically warren.Headers (map[string]any).
// Returns context.Background() enriched with the extracted span context,
// or an empty context if no valid traceparent is present.
func (p Propagator) Extract(h map[string]any) context.Context {
	return p.prop.Extract(context.Background(), headerCarrier(h))
}

// headerCarrier adapts map[string]any to propagation.TextMapCarrier.
type headerCarrier map[string]any

func (c headerCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func (c headerCarrier) Set(key, value string) {
	c[key] = value
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
