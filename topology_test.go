package warren

import (
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
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

// — T15: Topology.expand (in-memory pre-pass) ————————————————————————————

func TestTopology_expand_callerUnchanged(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum, DeliveryLimit: 3}},
		DeadLetters: []DeadLetter{
			{Source: "orders", Exchange: "orders.dlx", RoutingKey: "dead"},
		},
	}
	expanded := topo.expand()
	// The caller's topology must be untouched.
	require.Len(t, topo.Queues, 1)
	assert.Nil(t, topo.Queues[0].Args)
	// The expanded copy must carry the injected args.
	require.NotNil(t, expanded)
	assert.NotNil(t, expanded.Queues[0].Args)
}

func TestTopology_expand_dlxMergesArgsIntoSourceQueue(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{
			{
				Source:         "orders",
				Exchange:       "orders.dlx",
				RoutingKey:     "dead",
				TTL:            5 * time.Second,
				MaxLength:      100,
				MaxLengthBytes: 1024,
				Overflow:       OverflowRejectPublish,
			},
		},
	}
	expanded := topo.expand()

	args := expanded.Queues[0].Args
	require.NotNil(t, args)
	assert.Equal(t, "orders.dlx", args["x-dead-letter-exchange"])
	assert.Equal(t, "dead", args["x-dead-letter-routing-key"])
	assert.Equal(t, int64(5000), args["x-message-ttl"])
	assert.Equal(t, int64(100), args["x-max-length"])
	assert.Equal(t, int64(1024), args["x-max-length-bytes"])
	assert.Equal(t, string(OverflowRejectPublish), args["x-overflow"])
}

func TestTopology_expand_dlxOmitsEmptyOptionalArgs(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	args := expanded.Queues[0].Args
	assert.Equal(t, "orders.dlx", args["x-dead-letter-exchange"])
	assert.NotContains(t, args, "x-dead-letter-routing-key")
	assert.NotContains(t, args, "x-message-ttl")
	assert.NotContains(t, args, "x-max-length")
	assert.NotContains(t, args, "x-max-length-bytes")
	assert.NotContains(t, args, "x-overflow")
}

func TestTopology_expand_dlxAppendsDefaultDLXExchange(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	names := make([]string, len(expanded.Exchanges))
	for i, e := range expanded.Exchanges {
		names[i] = e.Name
	}
	assert.Contains(t, names, "orders.dlx")
}

func TestTopology_expand_dlxDoesNotDuplicateExistingDLXExchange(t *testing.T) {
	topo := &Topology{
		Exchanges:   []Exchange{{Name: "orders.dlx", Kind: ExchangeFanout, Durable: true}},
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	count := 0
	for _, e := range expanded.Exchanges {
		if e.Name == "orders.dlx" {
			count++
		}
	}
	assert.Equal(t, 1, count, "DLX exchange must appear exactly once")
}

func TestTopology_expand_dlxAppendsDefaultDLQ(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	names := make([]string, len(expanded.Queues))
	for i, q := range expanded.Queues {
		names[i] = q.Name
	}
	assert.Contains(t, names, "orders.dlq")
}

func TestTopology_expand_dlxDoesNotDuplicateExistingDLQ(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{
			{Name: "orders", Durable: true},
			{Name: "orders.dlq", Durable: true},
		},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	count := 0
	for _, q := range expanded.Queues {
		if q.Name == "orders.dlq" {
			count++
		}
	}
	assert.Equal(t, 1, count, "DLQ queue must appear exactly once")
}

func TestTopology_expand_dlxDefaultExchangeName(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders"}}, // no Exchange name
	}
	expanded := topo.expand()
	found := false
	for _, e := range expanded.Exchanges {
		if e.Name == "orders.dlx" {
			found = true
		}
	}
	assert.True(t, found, "default DLX exchange name should be '<Source>.dlx'")
}

func TestTopology_expand_injectsDeliveryLimit(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum, DeliveryLimit: 5}},
	}
	expanded := topo.expand()
	assert.Equal(t, int64(5), expanded.Queues[0].Args["x-delivery-limit"])
}

func TestTopology_expand_injectsSingleActiveConsumer(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true, SingleActiveConsumer: true}},
	}
	expanded := topo.expand()
	assert.Equal(t, true, expanded.Queues[0].Args["x-single-active-consumer"])
}

func TestTopology_expand_injectsQueueType(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
	}
	expanded := topo.expand()
	assert.Equal(t, string(QueueTypeQuorum), expanded.Queues[0].Args["x-queue-type"])
}

func TestTopology_expand_noQueueTypeWhenEmpty(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true}},
	}
	expanded := topo.expand()
	assert.NotContains(t, expanded.Queues[0].Args, "x-queue-type")
}

func TestTopology_expand_injectsMaxPriority(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Durable: true, MaxPriority: 10}},
	}
	expanded := topo.expand()
	assert.Equal(t, int64(10), expanded.Queues[0].Args["x-max-priority"])
}

// — T15: Topology.Declare — broker declare order (via recorder) —————————

// declareRecorder captures the sequence of broker declare calls.
type declareRecorder struct {
	calls []string
	err   error // if non-nil, returned on every call
}

func (r *declareRecorder) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp091.Table) error {
	r.calls = append(r.calls, "exchange:"+name)
	return r.err
}

func (r *declareRecorder) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error) {
	r.calls = append(r.calls, "queue:"+name)
	return amqp091.Queue{Name: name}, r.err
}

func (r *declareRecorder) QueueBind(name, key, exchange string, noWait bool, args amqp091.Table) error {
	r.calls = append(r.calls, "bind:"+name+"->"+exchange)
	return r.err
}

func (r *declareRecorder) Close() error { return nil }

func TestTopology_declareOnChannel_order(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
		Queues:    []Queue{{Name: "orders", Durable: true}},
		Bindings:  []Binding{{Exchange: "events", Queue: "orders", RoutingKey: "order.#"}},
	}
	rec := &declareRecorder{}
	err := declareOnChannel(topo, rec)
	require.NoError(t, err)
	require.Len(t, rec.calls, 3)
	assert.Equal(t, "exchange:events", rec.calls[0])
	assert.Equal(t, "queue:orders", rec.calls[1])
	assert.Equal(t, "bind:orders->events", rec.calls[2])
}

func TestTopology_declareOnChannel_missingExchangeWrapsTopologyMismatch(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
	}
	amqpErr := &amqp091.Error{Code: 406, Reason: "PRECONDITION_FAILED"}
	rec := &declareRecorder{err: amqpErr}
	err := declareOnChannel(topo, rec)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyMismatch)
	assert.ErrorIs(t, err, ErrPreconditionFailed)
}
