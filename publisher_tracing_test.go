package warren

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

// — in-memory recording tracer ————————————————————————————————————————————
//
// recordingTracer implements warren/otel.Tracer without the OpenTelemetry SDK.
// Each Start mints a fresh, valid span context so the W3C TraceContext
// propagator injects a real traceparent header, letting the tests assert both
// span bookkeeping (name, attributes, status, error) and header propagation.

type recordingSpan struct {
	mu      sync.Mutex
	name    string
	sc      trace.SpanContext
	parent  trace.SpanContext
	links   []warrenotel.Link
	attrs   []attribute.KeyValue
	status  codes.Code
	statMsg string
	errs    []error
	ended   bool
}

func (s *recordingSpan) End() {
	s.mu.Lock()
	s.ended = true
	s.mu.Unlock()
}

func (s *recordingSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.mu.Lock()
	s.attrs = append(s.attrs, attrs...)
	s.mu.Unlock()
}

func (s *recordingSpan) SetStatus(code codes.Code, description string) {
	s.mu.Lock()
	s.status = code
	s.statMsg = description
	s.mu.Unlock()
}

func (s *recordingSpan) RecordError(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

func (s *recordingSpan) attr(key string) (attribute.Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found attribute.Value
	ok := false
	for _, kv := range s.attrs {
		if string(kv.Key) == key {
			found = kv.Value
			ok = true // last-wins, mirroring span semantics
		}
	}
	return found, ok
}

type recordingTracer struct {
	mu    sync.Mutex
	n     uint8
	spans []*recordingSpan
}

func (t *recordingTracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	return t.start(ctx, name, nil, attrs)
}

// StartWithLinks satisfies warrenotel.LinkingTracer so BatchConsumer attaches a
// Link per incoming message; the recorded links let tests assert fan-in.
func (t *recordingTracer) StartWithLinks(ctx context.Context, name string, links []warrenotel.Link, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	return t.start(ctx, name, links, attrs)
}

func (t *recordingTracer) start(ctx context.Context, name string, links []warrenotel.Link, attrs []attribute.KeyValue) (context.Context, warrenotel.Span) {
	t.mu.Lock()
	t.n++
	n := t.n
	t.mu.Unlock()

	// Honour an existing (possibly remote) parent span context so a child span
	// inherits the trace-id — this is what makes publisher→consumer continuity
	// assertable. With no parent we mint a fresh trace-id keyed off n.
	parent := trace.SpanContextFromContext(ctx)
	var tid trace.TraceID
	if parent.IsValid() {
		tid = parent.TraceID()
	} else {
		tid[0], tid[15] = 1, n
	}
	var sid trace.SpanID
	sid[0], sid[7] = 1, n
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx = trace.ContextWithSpanContext(ctx, sc)

	s := &recordingSpan{name: name, sc: sc, parent: parent, links: links}
	s.attrs = append(s.attrs, attrs...)

	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return ctx, s
}

// compile-time guard: the recording tracer must implement the linking extension.
var _ warrenotel.LinkingTracer = (*recordingTracer)(nil)

func (t *recordingTracer) only() *recordingSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.spans) != 1 {
		return nil
	}
	return t.spans[0]
}

// newTracedPub builds a Publisher[M] wired to a recording tracer and a real
// propagator, sharing the fake-pool plumbing used by the rest of the suite.
func newTracedPub[M any](fake *fakePubChannel, tr *recordingTracer) (*Publisher[M], func()) {
	pool, stopPool := wireFakePool(fake)
	pub := &Publisher[M]{
		pools:          []*publisherConnPool{pool},
		mcs:            []*managedConn{{}},
		codec:          codec.NewJSON(),
		pm:             metrics.NoOpPublisherMetrics{},
		exchange:       "x",
		confirmTimeout: 2 * time.Second,
		tracer:         tr,
		propagator:     warrenotel.NewPropagator(),
	}
	return pub, stopPool
}

// — publishOutcome: pure mapping ——————————————————————————————————————————

func TestPublishOutcome_mapsSentinels(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		outcome string
		errType string
	}{
		{"success", nil, "ack", ""},
		{"unroutable", ErrUnroutable, "return", "ErrUnroutable"},
		{"confirm_timeout", ErrConfirmTimeout, "timeout", "ErrConfirmTimeout"},
		{"nacked", ErrPublishNacked, "nack", "ErrPublishNacked"},
		{"too_large", ErrMessageTooLarge, "too_large", "ErrMessageTooLarge"},
		{"pool_exhausted", ErrChannelPoolExhausted, "pool_exhausted", "ErrChannelPoolExhausted"},
		{"blocked", ErrConnectionBlocked, "blocked", "ErrConnectionBlocked"},
		{"invalid_message", ErrInvalidMessage, "error", "ErrInvalidMessage"},
		{"channel_closed", ErrChannelClosed, "error", "ErrChannelClosed"},
		{"rate_limited", ErrRateLimited, "rate_limited", "ErrRateLimited"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// wrap the sentinel to ensure errors.Is matching, not == matching.
			err := tc.err
			if err != nil {
				err = errors.Join(errors.New("ctx"), err)
			}
			outcome, errType := publishOutcome(err)
			assert.Equal(t, tc.outcome, outcome)
			assert.Equal(t, tc.errType, errType)
		})
	}
}

// — Publish span: success ——————————————————————————————————————————————————

func TestPublisher_Publish_span_success(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{
		Body:          &testPayload{Value: "hi"},
		CorrelationID: "corr-1",
	}))

	span := tr.only()
	require.NotNil(t, span, "exactly one span expected")
	assert.Equal(t, "x publish", span.name)
	assert.True(t, span.ended, "span must be ended")

	sys, ok := span.attr("messaging.system")
	require.True(t, ok)
	assert.Equal(t, "rabbitmq", sys.AsString())

	dest, ok := span.attr("messaging.destination.name")
	require.True(t, ok)
	assert.Equal(t, "x", dest.AsString())

	op, ok := span.attr("messaging.operation.type")
	require.True(t, ok)
	assert.Equal(t, "publish", op.AsString())

	conv, ok := span.attr("messaging.message.conversation_id")
	require.True(t, ok)
	assert.Equal(t, "corr-1", conv.AsString())

	_, ok = span.attr("messaging.message.id")
	assert.True(t, ok, "message.id attribute must be set")

	size, ok := span.attr("messaging.message.body.size")
	require.True(t, ok)
	assert.Positive(t, size.AsInt64())

	outcome, ok := span.attr("messaging.rabbitmq.outcome")
	require.True(t, ok)
	assert.Equal(t, "ack", outcome.AsString())

	assert.Equal(t, codes.Ok, span.status)
	assert.Empty(t, span.errs)
}

// — Publish span: trace-context injection ——————————————————————————————————

func TestPublisher_Publish_injectsTraceParent(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	tp, present := p.Headers["traceparent"]
	require.True(t, present, "traceparent must be injected into the published frame headers")
	assert.NotEmpty(t, tp.(string))
}

func TestPublisher_Publish_callerTraceParentWins(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const callerTP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{
		Body:    &testPayload{},
		Headers: Headers{"traceparent": callerTP},
	}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.Equal(t, callerTP, p.Headers["traceparent"],
		"caller-supplied traceparent must win (last-wins)")
}

// TestPublisher_Publish_doesNotMutateCallerHeaders asserts the publish path never
// writes the injected trace context back into the caller's Headers map. The
// HeaderCodec path is already guarded (TestPublisher_encodeMsg_DoesNotMutateCallerHeaders);
// this covers the plain-codec path, where encodeMsg returns the caller's own map
// reference and injectTrace would otherwise mutate it in place.
func TestPublisher_Publish_doesNotMutateCallerHeaders(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	shared := Headers{"x-keep": "v"}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{
		Body:    &testPayload{},
		Headers: shared,
	}))

	_, mutated := shared["traceparent"]
	assert.False(t, mutated, "publish must not write traceparent back into the caller's Headers map")
	assert.Len(t, shared, 1, "caller Headers map must be left exactly as supplied")
}

// TestPublisher_Publish_reusedHeadersMapGetsFreshTraceParent proves each publish
// carries its OWN span's traceparent even when the caller reuses one Headers map.
// If injectTrace mutates the caller map, the first publish's traceparent persists
// into the map and the last-wins restore then treats it as caller-supplied,
// silently stitching every later publish into the first publish's trace.
func TestPublisher_Publish_reusedHeadersMapGetsFreshTraceParent(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	shared := Headers{}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}, Headers: shared}))
	first, ok := fake.lastPublish()
	require.True(t, ok)
	tp1, _ := first.Headers["traceparent"].(string)
	require.NotEmpty(t, tp1)

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}, Headers: shared}))
	second, ok := fake.lastPublish()
	require.True(t, ok)
	tp2, _ := second.Headers["traceparent"].(string)
	require.NotEmpty(t, tp2)

	assert.NotEqual(t, tp1, tp2,
		"each publish must carry its own span's traceparent, not a stale value carried over via the reused map")
}

// TestPublisher_Publish_callerTraceStateWins asserts a caller-supplied tracestate
// is preserved (last-wins) alongside the injected traceparent — exercising the
// tracestate branch of injectTrace that the traceparent-only tests do not.
func TestPublisher_Publish_callerTraceStateWins(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const callerTS = "vendor=opaqueValue"
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{
		Body:    &testPayload{},
		Headers: Headers{"tracestate": callerTS},
	}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.Equal(t, callerTS, p.Headers["tracestate"],
		"caller-supplied tracestate must win (last-wins)")
	_, hasTP := p.Headers["traceparent"]
	assert.True(t, hasTP, "traceparent must still be injected alongside the preserved tracestate")
}

// — Publish span: failure matrix ——————————————————————————————————————————

func TestPublisher_Publish_span_nacked(t *testing.T) {
	fake := newFakePubCh(false)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	go func() {
		time.Sleep(5 * time.Millisecond)
		fake.sendNack(1)
	}()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.ErrorIs(t, err, ErrPublishNacked)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "nack", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrPublishNacked", et.AsString())
	assert.Equal(t, codes.Error, span.status)
	require.Len(t, span.errs, 1)
	assert.ErrorIs(t, span.errs[0], ErrPublishNacked)
}

func TestPublisher_Publish_span_unroutable(t *testing.T) {
	fake := newFakePubCh(true)
	fake.returnAll = true
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()
	pub.mandatory = true

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.ErrorIs(t, err, ErrUnroutable)

	span := tr.only()
	require.NotNil(t, span)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "return", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrUnroutable", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

func TestPublisher_Publish_span_confirmTimeout(t *testing.T) {
	fake := newFakePubCh(false)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()
	pub.confirmTimeout = 10 * time.Millisecond

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.ErrorIs(t, err, ErrConfirmTimeout)

	span := tr.only()
	require.NotNil(t, span)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "timeout", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrConfirmTimeout", et.AsString())
}

func TestPublisher_Publish_span_tooLarge(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()
	pub.maxMessageSizeBytes = 1 // any non-trivial body exceeds this

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "too big"}})
	require.ErrorIs(t, err, ErrMessageTooLarge)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "too_large", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrMessageTooLarge", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

func TestPublisher_Publish_span_encodeError(t *testing.T) {
	type badPayload struct {
		Ch chan int `json:"ch"`
	}
	pool, stopPool := wireFakePool(newFakePubCh(true))
	tr := &recordingTracer{}
	pub := &Publisher[badPayload]{
		pools:      []*publisherConnPool{pool},
		mcs:        []*managedConn{{}},
		codec:      codec.NewJSON(),
		pm:         metrics.NoOpPublisherMetrics{},
		exchange:   "x",
		tracer:     tr,
		propagator: warrenotel.NewPropagator(),
	}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[badPayload]{Body: &badPayload{Ch: make(chan int)}})
	require.ErrorIs(t, err, ErrInvalidMessage)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "error", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrInvalidMessage", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — finishPublishSpan: §8 leakage symmetry with the consume path ————————————
//
// The codec-encode / client-validation class (ErrInvalidMessage) is the only
// publish error whose text can be payload-derived (a caller-supplied custom
// Codec.Encode may embed the message body). It must be redacted to the sentinel
// label on the span, mirroring the consume path's redactedSpanError. Broker /
// framework diagnostics carry no message content and stay verbatim (LATER-59).

func TestFinishPublishSpan_redactsCodecClassKeepsBrokerVerbatim(t *testing.T) {
	const secret = "super-secret-payload-42"

	t.Run("ErrInvalidMessage class is redacted to the sentinel label", func(t *testing.T) {
		s := &recordingSpan{}
		leak := fmt.Errorf("%w: bad value %s", ErrInvalidMessage, secret)
		finishPublishSpan(s, leak)

		assert.Equal(t, codes.Error, s.status)
		assert.Equal(t, "ErrInvalidMessage", s.statMsg,
			"status description must be the closed-vocabulary label, never err.Error()")
		assert.NotContains(t, s.statMsg, secret)

		require.Len(t, s.errs, 1)
		assert.Equal(t, "ErrInvalidMessage", s.errs[0].Error(),
			"recorded error must render the label, not the payload-bearing message")
		assert.NotContains(t, s.errs[0].Error(), secret)
		assert.ErrorIs(t, s.errs[0], ErrInvalidMessage,
			"the recorded error must still unwrap to the sentinel for errors.Is backends")
	})

	t.Run("broker/framework diagnostics stay verbatim", func(t *testing.T) {
		s := &recordingSpan{}
		brokerErr := fmt.Errorf("%w: NO_ROUTE reply-code 312", ErrUnroutable)
		finishPublishSpan(s, brokerErr)

		assert.Equal(t, codes.Error, s.status)
		assert.Equal(t, brokerErr.Error(), s.statMsg,
			"broker diagnostics (no message content) are useful and kept verbatim")
		require.Len(t, s.errs, 1)
		assert.ErrorIs(t, s.errs[0], ErrUnroutable)
	})
}

func TestPublisher_Publish_span_poolExhausted(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Drain the single pool token so acquire has no slot; a short-deadline ctx then
	// makes acquire return ErrChannelPoolExhausted deterministically (with the token
	// gone, only ctx.Done() can fire in acquire's select — no token race).
	<-pub.pools[0].tokens
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := pub.Publish(ctx, Message[testPayload]{Body: &testPayload{}})
	require.ErrorIs(t, err, ErrChannelPoolExhausted)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "pool_exhausted", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrChannelPoolExhausted", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

func TestPublisher_Publish_span_blocked(t *testing.T) {
	fake := newFakePubCh(true)
	tr := &recordingTracer{}
	pub, stopPool := newTracedPub[testPayload](fake, tr)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Pin the publisher to a blocked connection; the publish barrier returns
	// ErrConnectionBlocked once ctx is cancelled while still blocked.
	mc := newBareManaged(t)
	mc.barrierMu.Lock()
	mc.blocked = true
	mc.barrierMu.Unlock()
	pub.mcs[0] = mc

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		mc.barrierCond.Broadcast()
	}()

	err := pub.Publish(ctx, Message[testPayload]{Body: &testPayload{}})
	require.ErrorIs(t, err, ErrConnectionBlocked)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "blocked", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrConnectionBlocked", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — Publish span: codec panic ends the span ————————————————————————————————

// panicEncodeCodec panics during Encode (the existing panicCodec only panics on
// Decode). Used to verify the publish span is ended on a codec panic.
type panicEncodeCodec struct{}

func (panicEncodeCodec) Encode(any) ([]byte, error) { panic("boom") }
func (panicEncodeCodec) Decode([]byte, any) error   { panic("boom") }
func (panicEncodeCodec) ContentType() string        { return "application/x-panic" }

func TestPublisher_Publish_span_codecPanic_endsSpan(t *testing.T) {
	pool, stopPool := wireFakePool(newFakePubCh(true))
	tr := &recordingTracer{}
	pub := &Publisher[testPayload]{
		pools:      []*publisherConnPool{pool},
		mcs:        []*managedConn{{}},
		codec:      panicEncodeCodec{},
		pm:         metrics.NoOpPublisherMetrics{},
		exchange:   "x",
		tracer:     tr,
		propagator: warrenotel.NewPropagator(),
	}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "x"}})
	require.ErrorIs(t, err, ErrInvalidMessage)

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended, "span must be ended even when the codec panics")
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "error", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrInvalidMessage", et.AsString())
}
