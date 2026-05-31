package warren

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// — basic.cancel surfacing (T36, SPEC §6.3) ————————————————————————————————
//
// When the broker sends basic.cancel (queue deleted, exclusive lock revoked) the
// library must: fire OnCancel(reason=tag) if set (else warn), increment the bounded
// consumer_cancelled_total{queue, reason} metric with a closed-vocabulary enum
// ("queue_deleted"/"exclusive_revoked"/"unknown", T49), and return Consume with
// ErrConsumerCancelled wrapping the tag — never silently die. The library does NOT
// auto-redeclare the queue.

func TestConsumer_BasicCancel_FiresOnCancelWithTag(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	var gotReason string
	gotCh := make(chan struct{})
	consumer, err := ConsumerFor[string](conn).Queue("testq").
		OnCancel(func(reason string) {
			gotReason = reason
			close(gotCh)
		}).
		Build()
	require.NoError(t, err)

	cancelCh := make(chan string, 1)
	consumer.deliveryCh = make(chan amqp091.Delivery)
	consumer.cancelReasonCh = cancelCh

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	cancelCh <- consumer.tag

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnCancel was not called after basic.cancel")
	}
	assert.Equal(t, consumer.tag, gotReason, "OnCancel reason must be the cancelled consumer tag (the frame datum)")

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled, "Consume must return ErrConsumerCancelled after firing OnCancel")
		assert.Contains(t, got.Error(), consumer.tag, "the returned error must carry the consumer tag")
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after basic.cancel")
	}
}

func TestConsumer_BasicCancel_OnCancelAndMetrics_BothFire(t *testing.T) {
	// End-to-end regression guard for the brokerCancel{tag,class} refactor and the
	// classifyCancel skip predicate (T49): with OnCancel set AND a non-NoOp metrics
	// sink, classifyCancel must NOT skip the probe (the metric observes the class), so
	// both side effects fire in one pass — OnCancel receives the unbounded tag and
	// RecordCancelled receives the bounded class. The fake conn cannot run the passive
	// probe, so the class resolves to the bounded "unknown" enum (never the tag).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	var gotTag string
	gotCh := make(chan struct{})
	consumer, err := ConsumerFor[string](conn).Queue("testq").
		Metrics(cm).
		OnCancel(func(reason string) {
			gotTag = reason
			close(gotCh)
		}).
		Build()
	require.NoError(t, err)

	cancelCh := make(chan string, 1)
	consumer.deliveryCh = make(chan amqp091.Delivery)
	consumer.cancelReasonCh = cancelCh

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(t.Context(), func(context.Context, string) error { return nil })
	}()

	cancelCh <- consumer.tag

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnCancel was not called")
	}
	assert.Equal(t, consumer.tag, gotTag, "OnCancel must receive the unbounded consumer tag")

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled)
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after basic.cancel")
	}

	assert.Equal(t, 1, cm.cancelled, "RecordCancelled must fire even when OnCancel is also set")
	assert.Equal(t, "testq", cm.cancelledQueue)
	assert.Equal(t, cancelReasonUnknown, cm.cancelledReason,
		"metric must record the bounded class, not the unbounded tag")
	assert.NotContains(t, cm.cancelledReason, "ctag-", "metric reason must never be the consumer tag")
}

func TestConsumer_BasicCancel_NoOnCancel_LogsWarning(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	warnCh := make(chan string, 4)
	conn.opts.logger = &captureLogger{onWarning: func(msg string) { warnCh <- msg }}

	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	cancelCh := make(chan string, 1)
	consumer.deliveryCh = make(chan amqp091.Delivery)
	consumer.cancelReasonCh = cancelCh

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	cancelCh <- consumer.tag

	var warning string
	select {
	case warning = <-warnCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no warning logged when OnCancel is unset")
	}
	assert.Contains(t, warning, "basic.cancel", "warning must mention the broker cancellation")
	assert.Contains(t, warning, consumer.tag, "warning must identify the cancelled consumer")

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled)
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after basic.cancel")
	}
}

func TestBatchConsumer_BasicCancel_ReturnsErrConsumerCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	var gotReason string
	gotCh := make(chan struct{})
	bc, err := BatchConsumerFor[string](conn).Queue("testq").Size(10).Prefetch(64).
		OnCancel(func(reason string) {
			gotReason = reason
			close(gotCh)
		}).
		Build()
	require.NoError(t, err)

	cancelCh := make(chan string, 1)
	bc.deliveryCh = make(chan amqp091.Delivery)
	bc.cancelReasonCh = cancelCh

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- bc.Consume(ctx, func(context.Context, *Batch[string]) error { return nil })
	}()

	cancelCh <- bc.tag

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("batch OnCancel was not called after basic.cancel")
	}
	assert.Equal(t, bc.tag, gotReason)

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled, "batch Consume must return ErrConsumerCancelled on basic.cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("batch Consume did not return after basic.cancel")
	}
}

// — basic.cancel buffered at channel close (I-2) ——————————————————————————————
//
// In production the delivery pump forwards the cancel reason AND closes the
// delivery stream; the main loop's outer select may pick the channel-closed (!ok)
// branch before the cancelReasonCh branch. The inner select must then service the
// already-buffered cancel rather than blocking on a re-subscribe that never comes.
// The testHookChannelClosed seam fires only after the outer select has committed to
// !ok, so buffering the reason there deterministically drives the inner branch
// (no sleep). Without that inner case the consumer would hang — these tests guard it.

func TestConsumer_BasicCancel_BufferedAtChannelClose_ReturnsErrConsumerCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	var gotReason string
	gotCh := make(chan struct{})
	consumer, err := ConsumerFor[string](conn).Queue("testq").
		OnCancel(func(reason string) {
			gotReason = reason
			close(gotCh)
		}).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery)
	cancelCh := make(chan string, 1)
	consumer.deliveryCh = deliveryCh
	consumer.cancelReasonCh = cancelCh
	// Buffer the cancel reason the instant the main loop observes the closed channel
	// (after it has already taken the !ok branch), so the INNER select — not the outer
	// cancelReasonCh case — must service it.
	consumer.testHookChannelClosed = func() { cancelCh <- consumer.tag }

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	// Close the delivery channel → outer select takes !ok (cancelReasonCh still empty)
	// → hook buffers the reason → inner select returns ErrConsumerCancelled.
	close(deliveryCh)

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnCancel was not called via the channel-closed inner select")
	}
	assert.Equal(t, consumer.tag, gotReason)

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled,
			"a basic.cancel buffered when the delivery channel closes must still return ErrConsumerCancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("Consume hung instead of returning on the channel-closed-with-pending-cancel path")
	}
}

func TestBatchConsumer_BasicCancel_BufferedAtChannelClose_ReturnsErrConsumerCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	gotCh := make(chan struct{})
	bc, err := BatchConsumerFor[string](conn).Queue("testq").Size(10).Prefetch(64).
		OnCancel(func(string) { close(gotCh) }).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery)
	cancelCh := make(chan string, 1)
	bc.deliveryCh = deliveryCh
	bc.cancelReasonCh = cancelCh
	bc.testHookChannelClosed = func() { cancelCh <- bc.tag }

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- bc.Consume(ctx, func(context.Context, *Batch[string]) error { return nil })
	}()

	close(deliveryCh)

	select {
	case <-gotCh:
	case <-time.After(2 * time.Second):
		t.Fatal("batch OnCancel was not called via the channel-closed inner select")
	}

	select {
	case got := <-errCh:
		require.ErrorIs(t, got, ErrConsumerCancelled,
			"batch: a cancel buffered when the delivery channel closes must still return ErrConsumerCancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("batch Consume hung instead of returning on the channel-closed-with-pending-cancel path")
	}
}

// TestForwardCancelReason_FullBuffer_FirstWins pins the contract of the free function
// the delivery pump uses to relay a broker basic.cancel reason: it must NEVER block,
// and when the size-1 buffer already holds a pending cancel the first one wins (a
// second cancel is dropped rather than overwriting or blocking the pump). The
// buffered-at-close tests inject cancelReasonCh directly and never call this, so this
// is its only direct coverage.
func TestForwardCancelReason_FullBuffer_FirstWins(t *testing.T) {
	ch := make(chan string, 1)

	forwardCancelReason(ch, "tag-A") // buffer now full

	done := make(chan struct{})
	go func() {
		defer close(done)
		forwardCancelReason(ch, "tag-B") // must not block on the full buffer
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("forwardCancelReason must not block when the size-1 buffer is full")
	}

	assert.Equal(t, "tag-A", <-ch, "the first cancel reason wins; the second is dropped")
	select {
	case extra := <-ch:
		t.Fatalf("only one reason may be buffered; got a second: %q", extra)
	default:
	}
}
