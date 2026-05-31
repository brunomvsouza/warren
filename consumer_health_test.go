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
