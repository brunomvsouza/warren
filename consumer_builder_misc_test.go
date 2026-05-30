package warren

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// idTracer is a distinguishable otel.Tracer for last-wins assertions: it embeds
// NoOpTracer (so Start is satisfied) and carries an id so a test can tell which
// instance the builder actually stored. Plain NoOpTracer{} values are
// indistinguishable, which is why the previous last-wins test could not observe a
// winner.
type idTracer struct {
	otel.NoOpTracer
	id int
}

// — ConsumerBuilder option methods ——————————————————————————————————————

func TestConsumerBuilder_ChannelQoS_Stored(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").ChannelQoS().Build()
	require.NoError(t, err)
	assert.True(t, c.channelQoS)
}

func TestConsumerBuilder_Priority_Stored(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Priority(5).Build()
	require.NoError(t, err)
	assert.Equal(t, 5, c.priority)
	assert.True(t, c.prioritySet)
}

func TestConsumerBuilder_Priority_LastWins(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Priority(1).Priority(9).Build()
	require.NoError(t, err)
	assert.Equal(t, 9, c.priority)
}

func TestConsumerBuilder_WithoutMetrics_SetsNoOp(t *testing.T) {
	conn := newFakeConsumerConn(t)
	custom := &maxRedeliveriesCountingMetrics{}
	c, err := ConsumerFor[string](conn).Queue("q").Metrics(custom).WithoutMetrics().Build()
	require.NoError(t, err)
	// WithoutMetrics last-wins: consumer must have a NoOp metrics (not the custom one).
	// We verify indirectly: RecordMaxRedeliveries on NoOp must not panic and
	// must not call the custom stub's map.
	assert.NotNil(t, c.cm)
	c.cm.RecordMaxRedeliveries("q", "x-death") // must not panic
	assert.Nil(t, custom.maxRedeliveries, "Metrics replaced by WithoutMetrics must not be called")
}

func TestConsumerBuilder_Tracer_Stored(t *testing.T) {
	conn := newFakeConsumerConn(t)
	tracer := otel.NoOpTracer{}
	c, err := ConsumerFor[string](conn).Queue("q").Tracer(tracer).Build()
	require.NoError(t, err)
	assert.NotNil(t, c.tracer)
}

func TestConsumerBuilder_Tracer_LastWins(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").
		Tracer(idTracer{id: 1}).
		Tracer(idTracer{id: 2}).
		Build()
	require.NoError(t, err)
	got, ok := c.tracer.(idTracer)
	require.True(t, ok, "the stored tracer must be the distinguishable last one set")
	assert.Equal(t, 2, got.id, "the last Tracer() call must win")
}

func TestConsumerBuilder_PrefetchBytes_IsNoOp(t *testing.T) {
	// PrefetchBytes is documented as a no-op on RabbitMQ; call must not panic
	// and must return the builder (chainable).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").PrefetchBytes(1024).Build()
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// — ConsumerBuilder remaining options (T36) — Exclusive / Args / OnCancel ———

func TestConsumerBuilder_Exclusive_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Exclusive().Build()
	require.NoError(t, err)
	assert.True(t, c.exclusive, "Exclusive() must set the consumer exclusive flag")
}

func TestConsumerBuilder_Exclusive_DefaultsOff(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.False(t, c.exclusive, "exclusive must default to false")
}

// — AutoAck (T35) — broker no-ack flag is stored on the consumer —————————

func TestConsumerBuilder_AutoAck_EnablesBrokerNoAck(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").AutoAck().Build()
	require.NoError(t, err)
	assert.True(t, c.brokerAutoAck, "AutoAck() must enable the broker no-ack flag")
}

func TestConsumerBuilder_AutoAck_DefaultsOff(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.False(t, c.brokerAutoAck, "brokerAutoAck must default to false (manual ack)")
}

func TestConsumerBuilder_Args_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Args(Headers{"x-custom": "value"}).Build()
	require.NoError(t, err)
	require.NotNil(t, c.consumeArgs)
	assert.Equal(t, "value", c.consumeArgs["x-custom"], "Args() must round-trip into the consumer consume args")
}

func TestConsumerBuilder_OnCancel_RoundTrips(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").OnCancel(func(_ string) {}).Build()
	require.NoError(t, err)
	assert.NotNil(t, c.onCancel, "OnCancel() must store the callback on the consumer")
}

// — TopologyHint last-wins reset ——————————————————————————————————————

func TestConsumerBuilder_TopologyHint_LastWins_Reset(t *testing.T) {
	// Calling TopologyHint(quorum+DeliveryLimit) then TopologyHint(classic)
	// must disable the quorum carve-out (counter B re-enabled).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("q").
		MaxRedeliveries(5).
		TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5}).  // disables counter B
		TopologyHint(Queue{Type: QueueTypeClassic, DeliveryLimit: 0}). // re-enables counter B
		Build()
	require.NoError(t, err)
	assert.False(t, c.counterBDisabled, "last TopologyHint (classic) must re-enable counter B")
}

// — Consumer.Health ————————————————————————————————————————————————

func TestConsumer_Health_ReturnsErrNotConnected_OnFakeConn(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	// The fake conn has no live socket (raw == nil) and is not reconnecting, so
	// Health delegates to managedConn.health and reports ErrNotConnected — a
	// concrete, asserted outcome rather than "did not panic".
	require.ErrorIs(t, c.Health(context.Background()), ErrNotConnected)
}

// — Compile-time interface assertion ————————————————————————————————

var _ metrics.ConsumerMetrics = metrics.NoOpConsumerMetrics{}
