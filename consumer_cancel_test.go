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
// consumer_cancelled_total{queue, reason="broker_initiated"} metric, and return
// Consume with ErrConsumerCancelled wrapping the tag — never silently die. The
// library does NOT auto-redeclare the queue.

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
