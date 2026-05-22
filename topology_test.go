package amqp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// — T14: Topology type validation ————————————————————————————————————————

func TestTopology_validate_happyPath(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
		Queues:    []Queue{{Name: "orders", Durable: true}},
		Bindings:  []Binding{{Exchange: "events", Queue: "orders", RoutingKey: "order.#"}},
	}
	require.NoError(t, topo.validate())
}

func TestTopology_validate_emptyExchangeName(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "", Kind: ExchangeDirect}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_emptyQueueName(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: ""}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_emptyBindingExchange(t *testing.T) {
	topo := &Topology{
		Bindings: []Binding{{Exchange: "", Queue: "q"}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_emptyBindingQueue(t *testing.T) {
	topo := &Topology{
		Bindings: []Binding{{Exchange: "ex", Queue: ""}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_deliveryLimitOnNonQuorum(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeClassic, DeliveryLimit: 5}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_deliveryLimitOnQuorumAllowed(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeQuorum, DeliveryLimit: 5, Durable: true}},
	}
	require.NoError(t, topo.validate())
}

func TestTopology_validate_singleActiveConsumerOnStream(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeStream, SingleActiveConsumer: true}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_streamExclusive(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeStream, Exclusive: true}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_streamAutoDelete(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeStream, AutoDelete: true}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_fanoutBindingWithRoutingKey(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "fan", Kind: ExchangeFanout}},
		Queues:    []Queue{{Name: "q"}},
		Bindings:  []Binding{{Exchange: "fan", Queue: "q", RoutingKey: "oops"}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_delayedExchangeWithoutXDelayedType(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "delay", Kind: ExchangeDelayed}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_delayedExchangeWithXDelayedTypeAllowed(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{
			Name: "delay",
			Kind: ExchangeDelayed,
			Args: map[string]any{"x-delayed-type": "direct"},
		}},
	}
	require.NoError(t, topo.validate())
}

func TestTopology_validate_duplicateQueueNames(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{
			{Name: "orders"},
			{Name: "orders"},
		},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_duplicateExchangeNames(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{
			{Name: "events", Kind: ExchangeTopic},
			{Name: "events", Kind: ExchangeDirect},
		},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// — T14: DeadLetter validation ————————————————————————————————————————————

func TestTopology_validate_emptyDeadLetterSource(t *testing.T) {
	topo := &Topology{
		DeadLetters: []DeadLetter{{Source: ""}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_deadLetterSourceAllowed(t *testing.T) {
	topo := &Topology{
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	require.NoError(t, topo.validate())
}

// — T14: Queue.MaxPriority validation —————————————————————————————————————

func TestTopology_validate_maxPriorityOnQuorum(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeQuorum, MaxPriority: 5}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_maxPriorityOnStream(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeStream, MaxPriority: 5}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_maxPriorityOnClassicAllowed(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueTypeClassic, MaxPriority: 5}},
	}
	require.NoError(t, topo.validate())
}

func TestTopology_validate_maxPriorityOnDefaultTypeAllowed(t *testing.T) {
	// Empty Type means classic (broker default); MaxPriority is allowed.
	topo := &Topology{
		Queues: []Queue{{Name: "q", MaxPriority: 5}},
	}
	require.NoError(t, topo.validate())
}

func TestTopology_validate_maxPriorityOutOfRange(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", MaxPriority: 256}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}
