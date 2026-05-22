package amqp

import (
	"testing"
	"time"

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

// — T14: DeadLetter struct ——————————————————————————————————————————————————

func TestDeadLetter_fields(t *testing.T) {
	dl := DeadLetter{
		Source:         "orders",
		Exchange:       "orders.dlx",
		RoutingKey:     "dead",
		TTL:            time.Minute,
		MaxLength:      100,
		MaxLengthBytes: 1024 * 1024,
		Overflow:       OverflowRejectPublish,
	}
	assert.Equal(t, "orders", dl.Source)
	assert.Equal(t, "orders.dlx", dl.Exchange)
	assert.Equal(t, "dead", dl.RoutingKey)
	assert.Equal(t, time.Minute, dl.TTL)
	assert.Equal(t, 100, dl.MaxLength)
	assert.Equal(t, 1024*1024, dl.MaxLengthBytes)
	assert.Equal(t, OverflowRejectPublish, dl.Overflow)
}
