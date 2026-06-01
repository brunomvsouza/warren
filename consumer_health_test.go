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

// — Consumer.Health snapshot (T53) ————————————————————————————————————

func TestConsumer_Health_NilSnapshot_OnUnhealthyConn(t *testing.T) {
	// When the pinned connection is unhealthy, Health returns (nil, err): a zeroed
	// snapshot would be meaningless, so the pointer is nil and the error carries the
	// reason (SPEC §6.3, T53).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	h, err := c.Health(context.Background())
	require.ErrorIs(t, err, ErrNotConnected)
	assert.Nil(t, h, "snapshot must be nil when the connection check fails")
}

func TestConsumer_Health_HealthyConn_ReturnsSnapshot(t *testing.T) {
	// When the connection-liveness gate passes, Health returns (&snapshot, nil) — the
	// public success path. The real gate opens a broker channel, so this is otherwise
	// covered only on the integration lane; the healthCheckOverride hook exercises the
	// gate-passes -> return-snapshot passthrough as a unit test, and we assert the
	// returned struct round-trips the consumer's runtime state, not a zero value (T53).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	c.healthCheckOverride = func(context.Context) error { return nil }

	// Seed observable runtime state so a zeroed/misreceived snapshot would fail below.
	c.started.Store(true)
	c.lastDeliveryNanos.Store(time.Now().UnixNano())
	c.inFlight.Store(2)

	h, err := c.Health(context.Background())
	require.NoError(t, err)
	require.NotNil(t, h, "a healthy gate must return a non-nil snapshot")

	want := c.snapshot()
	assert.Equal(t, want, *h, "Health must return exactly the consumer snapshot")
	assert.True(t, h.Active, "started, not stopped/closed/paused -> Active")
	assert.False(t, h.Paused)
	assert.Equal(t, 2, h.InFlightHandlers)
	assert.False(t, h.LastDeliveryAt.IsZero(), "LastDeliveryAt must carry the seeded delivery time")
}

func TestConsumer_Snapshot_ReportsInFlightAndLastDelivery(t *testing.T) {
	// snapshot() reflects a live, in-flight handler: InFlightHandlers counts the
	// executing handler, LastDeliveryAt stamps when the delivery was received, and
	// Active is true / Paused is false for a running, never-paused consumer (T53).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliverySubOverride = &deliverySub{ch: deliveryCh, done: nil}

	ctx, cancel := context.WithCancel(context.Background())

	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			close(handlerEntered)
			<-releaseHandler
			return nil
		})
	}()

	before := time.Now()
	deliveryCh <- amqp091.Delivery{Body: []byte(`"hello"`)}
	<-handlerEntered

	snap := c.snapshot()
	assert.True(t, snap.Active, "a running, never-paused consumer is Active")
	assert.False(t, snap.Paused)
	assert.Equal(t, 1, snap.InFlightHandlers, "the blocked handler must be counted in-flight")
	assert.False(t, snap.LastDeliveryAt.Before(before), "LastDeliveryAt must stamp the received delivery")

	close(releaseHandler)
	cancel()
	<-consumeDone

	// After the handler returns and the loop exits, no handler is in flight.
	assert.Equal(t, 0, c.snapshot().InFlightHandlers)
}

func TestConsumer_Snapshot_ActiveFalseAfterClose(t *testing.T) {
	// Close flips Active to false; a closed consumer is not consuming (T53).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	require.NoError(t, c.Close(context.Background()))

	snap := c.snapshot()
	assert.False(t, snap.Active, "a closed consumer is not Active")
}

func TestConsumer_Snapshot_ActiveFalseAfterCtxCancel(t *testing.T) {
	// A consumer whose Consume loop has exited via ctx cancel — WITHOUT a Close call
	// — is no longer consuming. Active must be false so a readiness probe does not
	// keep a silently-dead consumer in rotation; this is the exact case Health exists
	// to surface (SPEC §6.3, T53).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery)
	c.deliverySubOverride = &deliverySub{ch: deliveryCh, done: nil}

	ctx, cancel := context.WithCancel(context.Background())
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	require.Eventually(t, func() bool { return c.snapshot().Active }, time.Second, 5*time.Millisecond,
		"a running, never-paused consumer is Active")

	cancel()
	<-consumeDone

	assert.False(t, c.snapshot().Active, "a consumer whose loop exited via ctx cancel is not Active")
	assert.False(t, c.snapshot().Paused)
}

func TestConsumer_Snapshot_ActiveFalseAfterBrokerCancel(t *testing.T) {
	// A consumer whose loop exited via a broker basic.cancel (returning
	// ErrConsumerCancelled) is permanently stopped. Active must be false even though
	// neither Close nor a ctx cancel happened (T53).
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	// OnCancel set + NoOp metrics make classifyCancel return without broker I/O.
	c, err := ConsumerFor[string](conn).Queue("q").OnCancel(func(string) {}).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery)
	c.deliverySubOverride = &deliverySub{ch: deliveryCh, done: nil}
	c.cancelReasonCh = make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- c.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	require.Eventually(t, func() bool { return c.snapshot().Active }, time.Second, 5*time.Millisecond,
		"consumer must be running before the broker cancel")

	// Broker basic.cancel: runConsume fires OnCancel and returns ErrConsumerCancelled.
	c.cancelReasonCh <- "ctag-x"

	select {
	case err := <-consumeErr:
		require.ErrorIs(t, err, ErrConsumerCancelled)
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after the broker cancel")
	}

	assert.False(t, c.snapshot().Active, "a broker-cancelled consumer is not Active")
}
