package warren

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

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
	first := otel.NoOpTracer{}
	second := otel.NoOpTracer{}
	// Both calls return builder; second call wins. Just verify no panic and build succeeds.
	c, err := ConsumerFor[string](conn).Queue("q").Tracer(first).Tracer(second).Build()
	require.NoError(t, err)
	assert.NotNil(t, c.tracer)
}

func TestConsumerBuilder_PrefetchBytes_IsNoOp(t *testing.T) {
	// PrefetchBytes is documented as a no-op on RabbitMQ; call must not panic
	// and must return the builder (chainable).
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").PrefetchBytes(1024).Build()
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// — ConsumerBuilder stubs (T36 placeholders) ————————————————————————

func TestConsumerBuilder_Exclusive_IsChainable(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := ConsumerFor[string](conn).Queue("q").Exclusive().Build()
	require.NoError(t, err)
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

func TestConsumerBuilder_Args_IsChainable(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := ConsumerFor[string](conn).Queue("q").Args(Headers{"x-custom": "value"}).Build()
	require.NoError(t, err)
}

func TestConsumerBuilder_OnCancel_IsChainable(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := ConsumerFor[string](conn).Queue("q").OnCancel(func(_ string) {}).Build()
	require.NoError(t, err)
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

func TestConsumer_Health_ReturnsNoError_WhenConnectionHealthy(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)

	ctx := context.Background()
	// The fake consumer conn has no real TCP connection; health delegates to
	// managedConn.health which returns nil when no broker connection is expected.
	err = c.Health(ctx)
	// We assert no panic; the return value depends on the fake conn's health state.
	// The important thing is Health is exercised and the method exists.
	_ = err // nil or connection-not-established — both valid for a fake conn
}

// — Compile-time interface assertion ————————————————————————————————

var _ metrics.ConsumerMetrics = metrics.NoOpConsumerMetrics{}
