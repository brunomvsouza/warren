package warren

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	warrenotel "github.com/brunomvsouza/warren/otel"
)

// runProcessSpan drives a single delivery through a Consume handler and returns
// the one recorded process span after the consumer goroutine has fully drained.
//
// The injected acknowledger cancels the consumer context on the first ack OR
// nack, so every verdict path (ack, nack, counter-A short-circuit, timeout) ends
// the consume loop. Consume's wg.Wait guarantees the dispatch goroutine — and
// therefore finishConsumeSpan + the deferred span.End — has completed before this
// returns, so assertions never race the span bookkeeping.
func runProcessSpan(t *testing.T, configure func(*ConsumerBuilder[string]) *ConsumerBuilder[string], d amqp091.Delivery, h Handler[string]) *recordingSpan {
	t.Helper()

	conn := newFakeConsumerConn(t)
	tr := &recordingTracer{}
	b := ConsumerFor[string](conn).Queue("testq").Tracer(tr)
	if configure != nil {
		b = configure(b)
	}
	c, err := b.Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.Acknowledger = &fakeAcknowledger{
		ackFn:  func(_ uint64, _ bool) error { cancel(); return nil },
		nackFn: func(_ uint64, _, _ bool) error { cancel(); return nil },
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, h)
	}()

	deliveryCh <- d

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not finish")
	}

	span := tr.only()
	require.NotNil(t, span, "exactly one process span expected")
	return span
}

func jsonDelivery(body string) amqp091.Delivery {
	return amqp091.Delivery{Body: []byte(`"` + body + `"`), ContentType: "application/json"}
}

// — process span: happy path ————————————————————————————————————————————————

func TestConsumer_processSpan_success(t *testing.T) {
	defer goleak.VerifyNone(t)

	d := jsonDelivery("hi")
	d.MessageId = "msg-1"
	d.CorrelationId = "corr-1"

	span := runProcessSpan(t, nil, d, func(_ context.Context, _ string) error { return nil })

	assert.Equal(t, "testq process", span.name)
	assert.True(t, span.ended, "span must be ended")

	sys, ok := span.attr("messaging.system")
	require.True(t, ok)
	assert.Equal(t, "rabbitmq", sys.AsString())

	dest, ok := span.attr("messaging.destination.name")
	require.True(t, ok)
	assert.Equal(t, "testq", dest.AsString())

	op, ok := span.attr("messaging.operation.type")
	require.True(t, ok)
	assert.Equal(t, "process", op.AsString())

	mid, ok := span.attr("messaging.message.id")
	require.True(t, ok)
	assert.Equal(t, "msg-1", mid.AsString())

	conv, ok := span.attr("messaging.message.conversation_id")
	require.True(t, ok)
	assert.Equal(t, "corr-1", conv.AsString())

	outcome, ok := span.attr("messaging.rabbitmq.outcome")
	require.True(t, ok)
	assert.Equal(t, "ack", outcome.AsString())

	assert.Equal(t, codes.Ok, span.status)
	assert.Empty(t, span.errs)
}

// — process span: verdict outcome matrix ————————————————————————————————————

func TestConsumer_processSpan_outcomeMatrix(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		outcome string
		errType string
		status  codes.Code
	}{
		{"ack", nil, "ack", "", codes.Ok},
		{"requeue", ErrRequeue, "nack_requeue", "ErrRequeue", codes.Error},
		{"poison", ErrPoison, "nack_no_requeue", "ErrPoison", codes.Error},
		{"generic", assert.AnError, "nack_no_requeue", "error", codes.Error},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)
			span := runProcessSpan(t, nil, jsonDelivery("x"), func(_ context.Context, _ string) error {
				return tc.err
			})
			assert.True(t, span.ended)
			outcome, _ := span.attr("messaging.rabbitmq.outcome")
			assert.Equal(t, tc.outcome, outcome.AsString())
			assert.Equal(t, tc.status, span.status)
			if tc.errType == "" {
				_, ok := span.attr("error.type")
				assert.False(t, ok, "successful span must not set error.type")
				assert.Empty(t, span.errs)
			} else {
				et, _ := span.attr("error.type")
				assert.Equal(t, tc.errType, et.AsString())
				require.Len(t, span.errs, 1)
			}
		})
	}
}

// — process span: counter-A max-redeliveries short-circuit ——————————————————

func TestConsumer_processSpan_maxRedeliveries(t *testing.T) {
	defer goleak.VerifyNone(t)

	const n = 2
	d := jsonDelivery("x")
	d.Headers = amqp091.Table{
		"x-death": []any{
			amqp091.Table{"queue": "testq", "reason": "rejected", "count": int64(n)},
		},
	}

	handlerCalled := false
	span := runProcessSpan(t,
		func(b *ConsumerBuilder[string]) *ConsumerBuilder[string] { return b.MaxRedeliveries(n) },
		d,
		func(_ context.Context, _ string) error { handlerCalled = true; return nil },
	)

	assert.False(t, handlerCalled, "counter A must short-circuit before the handler")
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "max_redeliveries", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrMaxRedeliveries", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — process span: HandlerTimeout outcome ————————————————————————————————————

func TestConsumer_processSpan_timeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	span := runProcessSpan(t,
		func(b *ConsumerBuilder[string]) *ConsumerBuilder[string] {
			return b.HandlerTimeout(40 * time.Millisecond)
		},
		jsonDelivery("x"),
		func(hCtx context.Context, _ string) error {
			<-hCtx.Done() // block until the deadline fires
			return hCtx.Err()
		},
	)

	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "timeout", outcome.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — process span: channel-close mid-handler ends with the abort outcome —————

func TestConsumer_processSpan_channelClosed(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	tr := &recordingTracer{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		Tracer(tr).
		HandlerTimeout(5 * time.Second). // long: must not fire before channel close
		Build()
	require.NoError(t, err)

	doneCh := make(chan struct{})
	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliverySubOverride = &deliverySub{ch: deliveryCh, done: doneCh}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerStarted := make(chan struct{})
	handlerCause := make(chan error, 1)
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(hCtx context.Context, _ string) error {
			close(handlerStarted)
			<-hCtx.Done() // cancelled by cancelCause(ErrChannelClosed)
			handlerCause <- context.Cause(hCtx)
			return hCtx.Err()
		})
	}()

	deliveryCh <- jsonDelivery("x")
	<-handlerStarted
	close(doneCh) // signal channel close → dispatch's only ready select case is <-chanDone
	// The <-chanDone case calls cancelCause(ErrChannelClosed), which unblocks the handler.
	// Waiting for that cause proves dispatch committed to the channel-closed path BEFORE we
	// cancel the outer ctx. Cancelling first would make close(doneCh) and cancel() race for
	// the dispatch select: once both <-chanDone and <-hCtx.Done() are ready, Go picks one at
	// random, and the <-hCtx.Done() branch ends the span with no outcome (flaky failure).
	require.ErrorIs(t, <-handlerCause, ErrChannelClosed)
	cancel() // safe now: dispatch is past the select; let Consume return
	<-consumeDone

	span := tr.only()
	require.NotNil(t, span)
	assert.True(t, span.ended)
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "handler_aborted_channel_closed", outcome.AsString())
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrChannelClosed", et.AsString())
	assert.Equal(t, codes.Error, span.status)
}

// — process span: handler panic still ends the span —————————————————————————

func TestConsumer_processSpan_handlerPanic_endsSpan(t *testing.T) {
	defer goleak.VerifyNone(t)

	span := runProcessSpan(t, nil, jsonDelivery("x"), func(_ context.Context, _ string) error {
		panic("handler boom")
	})

	assert.True(t, span.ended, "span must be ended even when the handler panics")
	outcome, _ := span.attr("messaging.rabbitmq.outcome")
	assert.Equal(t, "nack_no_requeue", outcome.AsString(), "a panic maps to nack without requeue")
	assert.Equal(t, codes.Error, span.status)
	require.Len(t, span.errs, 1)
}

// — span continuity: producer trace-id flows into the process span ——————————

func TestConsumer_processSpan_continuity_inheritsProducerTrace(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Fabricate a producer span context and inject it as the message would carry it.
	var tid trace.TraceID
	var sid trace.SpanID
	tid[0], tid[15] = 7, 7
	sid[0], sid[7] = 9, 9
	producer := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	hdrs := amqp091.Table{}
	warrenotel.NewPropagator().Inject(trace.ContextWithSpanContext(context.Background(), producer), hdrs)
	require.Contains(t, hdrs, "traceparent", "test setup: producer traceparent must be present")

	d := jsonDelivery("x")
	d.Headers = hdrs

	span := runProcessSpan(t, nil, d, func(_ context.Context, _ string) error { return nil })

	assert.Equal(t, tid, span.sc.TraceID(), "process span must inherit the producer trace-id")
	assert.NotEqual(t, sid, span.sc.SpanID(), "process span must be a new span, not the producer span")
	require.True(t, span.parent.IsValid(), "process span must have a parent span context")
	assert.Equal(t, tid, span.parent.TraceID())
	assert.Equal(t, sid, span.parent.SpanID(), "parent span-id must resolve to the producer span")
}

// — headers are never stripped on the consume path ——————————————————————————

func TestConsumer_consumePath_doesNotStripHeaders(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotHeaders Headers
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.ConsumeRaw(ctx, func(_ context.Context, dl *Delivery[string]) error {
			gotHeaders = dl.Headers()
			cancel()
			return dl.Ack()
		})
	}()

	d := jsonDelivery("x")
	d.Headers = amqp091.Table{"x-trace-marker": int64(42)}
	d.Acknowledger = &fakeAcknowledger{}
	deliveryCh <- d

	<-done
	require.Contains(t, gotHeaders, "x-trace-marker")
	assert.Equal(t, int64(42), gotHeaders["x-trace-marker"],
		"marker header must reach the handler exactly as published")
}

// TestConsumer_source_noHeaderMutation is the symbol-presence guard from T28: the
// consume-path source must never delete a key from a Headers/Table map. It mirrors
// `grep -L 'delete(.*Headers' consumer*.go` as an executable, CI-enforced check.
func TestConsumer_source_noHeaderMutation(t *testing.T) {
	deleteHeaders := regexp.MustCompile(`delete\([^)]*(?i:headers|table)`)
	for _, f := range []string{"consumer.go", "batch_consumer.go"} {
		src, err := os.ReadFile(f) //nolint:gosec // G304: f is a fixed literal from the loop, not user input
		require.NoError(t, err)
		assert.NotRegexp(t, deleteHeaders, string(src),
			"%s must not delete header keys on the consume path (SPEC §6.9 contract)", f)
	}
}

// — span error rendering must not leak handler-supplied content (SPEC §8) ——————

// finishConsumeSpan must never render a handler's raw err.Error() onto the span: a
// handler may return an error whose message embeds message payload or PII. Both the
// status description and the recorded error event are reduced to the closed
// error-type vocabulary, while errors.Is still unwraps to the original sentinel so
// assertive alerting keeps working.
func TestConsumer_processSpan_errorRendering_doesNotLeakHandlerMessage(t *testing.T) {
	defer goleak.VerifyNone(t)

	const secret = "pan=4111111111111111"
	span := runProcessSpan(t, nil, jsonDelivery("x"), func(_ context.Context, _ string) error {
		// A poison error whose message embeds a payload-derived secret.
		return fmt.Errorf("%w: %s", ErrPoison, secret)
	})

	// error.type and status still classify the failure for alerting.
	et, _ := span.attr("error.type")
	assert.Equal(t, "ErrPoison", et.AsString())
	assert.Equal(t, codes.Error, span.status)

	// Status description is the closed-vocabulary type, never the raw message.
	assert.Equal(t, "ErrPoison", span.statMsg)
	assert.NotContains(t, span.statMsg, secret, "status description must not leak the handler error message")

	// The recorded error event must not carry the raw message either, but must
	// still unwrap to the sentinel so errors.Is-based backends keep working.
	require.Len(t, span.errs, 1)
	assert.NotContains(t, span.errs[0].Error(), secret, "recorded error must not leak the handler error message")
	assert.ErrorIs(t, span.errs[0], ErrPoison)
}
