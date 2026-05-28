package otel_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"

	warrenotel "github.com/brunomvsouza/warren/otel"
)

// — Compile-time interface check ———————————————————————————————————————————

var _ warrenotel.Tracer = warrenotel.NoOpTracer{}

// — NoOpTracer ——————————————————————————————————————————————————————————————

func TestNoOpTracer_doesNotPanic(t *testing.T) {
	var tr warrenotel.Tracer = warrenotel.NoOpTracer{}
	ctx, span := tr.Start(context.Background(), "warren.publish", attribute.String("k", "v"))
	require.NotNil(t, span)
	require.NotNil(t, ctx)
	span.SetAttributes(attribute.Int("size", 42))
	span.SetStatus(codes.Error, "boom")
	span.SetStatus(codes.Ok, "")
	span.RecordError(nil)
	span.RecordError(assert.AnError)
	span.End()
}

func TestNoOpTracer_returnsOriginalContext(t *testing.T) {
	parent := context.WithValue(context.Background(), "key", "val") //nolint:staticcheck
	tr := warrenotel.NoOpTracer{}
	got, _ := tr.Start(parent, "span")
	assert.Equal(t, "val", got.Value("key"), "Start must preserve the parent context")
}

// — Propagator: Inject → Extract round-trip ———————————————————————————————

func validSpanContext() trace.SpanContext {
	traceID := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	spanID := [8]byte{0, 0, 0, 0, 0, 0, 0, 1}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
}

func TestPropagator_roundTrip(t *testing.T) {
	sc := validSpanContext()
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	p := warrenotel.NewPropagator()
	h := map[string]any{}
	p.Inject(ctx, h)

	require.Contains(t, h, "traceparent", "traceparent header must be set after Inject")

	extractedCtx := p.Extract(h)
	got := trace.SpanContextFromContext(extractedCtx)

	assert.True(t, got.IsValid(), "extracted SpanContext must be valid")
	assert.Equal(t, sc.TraceID(), got.TraceID(), "TraceID must survive Inject → Extract")
	assert.Equal(t, sc.SpanID(), got.SpanID(), "SpanID must survive Inject → Extract")
}

func TestPropagator_emptyHeaders_noValidContext(t *testing.T) {
	p := warrenotel.NewPropagator()
	ctx := p.Extract(map[string]any{})
	sc := trace.SpanContextFromContext(ctx)
	assert.False(t, sc.IsValid(), "empty headers must not produce a valid SpanContext")
}

func TestPropagator_ActiveContext(t *testing.T) {
	p := warrenotel.NewPropagator()
	assert.False(t, p.ActiveContext(context.Background()),
		"a bare context carries no span context")

	ctx := trace.ContextWithSpanContext(context.Background(), validSpanContext())
	assert.True(t, p.ActiveContext(ctx),
		"a context with a valid span context must be reported active")
}

func TestPropagator_zeroValue_usable(t *testing.T) {
	// The zero value must default to W3C TraceContext propagation rather than
	// panicking on a nil inner propagator.
	var p warrenotel.Propagator
	ctx := trace.ContextWithSpanContext(context.Background(), validSpanContext())
	assert.True(t, p.ActiveContext(ctx))

	h := map[string]any{}
	require.NotPanics(t, func() { p.Inject(ctx, h) })
	assert.Contains(t, h, warrenotel.HeaderTraceParent)

	got := trace.SpanContextFromContext(p.Extract(h))
	assert.True(t, got.IsValid())
}

func TestPropagator_injectOnlyWritesTraceHeaders(t *testing.T) {
	sc := validSpanContext()
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	p := warrenotel.NewPropagator()
	h := map[string]any{}
	p.Inject(ctx, h)

	for k := range h {
		assert.True(t, k == "traceparent" || k == "tracestate",
			"Inject must only write traceparent/tracestate, got %q", k)
	}
}

// — CloudEvents ce-* header non-collision ——————————————————————————————————

func TestPropagator_noCEHeaderCollision(t *testing.T) {
	sc := validSpanContext()
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	p := warrenotel.NewPropagator()
	h := map[string]any{}
	p.Inject(ctx, h)

	for k := range h {
		assert.NotContains(t, k, "ce-",
			"W3C trace header %q must not conflict with CloudEvents ce-* headers", k)
	}
}

// — Semconv v1.27.0 snapshot ————————————————————————————————————————————————
//
// If this test fails, the semconv/v1.27.0 import path was changed or the
// attribute keys were renamed. Update SPEC.md §6.9 when intentionally bumping
// to a newer semconv version.

func TestSemconv_v1_27_0_messagingAttributeKeys(t *testing.T) {
	assert.Equal(t, attribute.Key("messaging.system"), semconv.MessagingSystemKey)
	assert.Equal(t, attribute.Key("messaging.destination.name"), semconv.MessagingDestinationNameKey)
	assert.Equal(t, attribute.Key("messaging.operation.type"), semconv.MessagingOperationTypeKey)
	assert.Equal(t, attribute.Key("messaging.message.id"), semconv.MessagingMessageIDKey)
	assert.Equal(t, attribute.Key("messaging.message.conversation_id"), semconv.MessagingMessageConversationIDKey)
	assert.Equal(t, attribute.Key("messaging.message.body.size"), semconv.MessagingMessageBodySizeKey)
	assert.Equal(t, attribute.Key("network.peer.address"), semconv.NetworkPeerAddressKey)
	assert.Equal(t, attribute.Key("network.peer.port"), semconv.NetworkPeerPortKey)
}
