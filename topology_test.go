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

// Every routing kind is a valid x-delayed-type (not just "direct").
func TestTopology_validate_delayedExchangeAcceptsAllRoutingKinds(t *testing.T) {
	for _, kind := range []ExchangeKind{ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders} {
		topo := &Topology{
			Exchanges: []Exchange{{
				Name: "delay",
				Kind: ExchangeDelayed,
				Args: map[string]any{"x-delayed-type": string(kind)},
			}},
		}
		assert.NoError(t, topo.validate(), "x-delayed-type=%q must be accepted", kind)
	}
}

func TestTopology_validate_delayedExchangeWithInvalidXDelayedType(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{
			Name: "delay",
			Kind: ExchangeDelayed,
			Args: map[string]any{"x-delayed-type": "bogus"},
		}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// x-delayed-type may not itself be a delayed exchange (only a routing kind).
func TestTopology_validate_delayedExchangeWithDelayedXDelayedType(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{
			Name: "delay",
			Kind: ExchangeDelayed,
			Args: map[string]any{"x-delayed-type": string(ExchangeDelayed)},
		}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// A non-string x-delayed-type is rejected rather than silently ignored.
func TestTopology_validate_delayedExchangeWithNonStringXDelayedType(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{
			Name: "delay",
			Kind: ExchangeDelayed,
			Args: map[string]any{"x-delayed-type": 42},
		}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// An empty-string x-delayed-type hits the dedicated non-empty branch (not the
// generic "not a valid kind" backstop). Asserting the message locks that branch
// against accidental removal.
func TestTopology_validate_delayedExchangeWithEmptyXDelayedType(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{
			Name: "delay",
			Kind: ExchangeDelayed,
			Args: map[string]any{"x-delayed-type": ""},
		}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "non-empty")
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

// T47: the in-memory expansion must bind the auto-created DLX exchange to the
// auto-created DLQ with RoutingKey "#", otherwise dead-lettered messages route
// into limbo (DLX has no queue bound and silently drops them).
func TestTopology_expand_dlxBindsDLXToDLQ(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	found := false
	for _, b := range expanded.Bindings {
		if b.Exchange == "orders.dlx" && b.Queue == "orders.dlq" && b.RoutingKey == "#" {
			found = true
		}
	}
	assert.True(t, found, "expand() must append Binding{orders.dlx -> orders.dlq, #}")
}

func TestTopology_expand_dlxBindsWithDeadLetterRoutingKey(t *testing.T) {
	// When DeadLetter.RoutingKey is set, dead-lettered messages have their routing
	// key rewritten to it (x-dead-letter-routing-key), so the DLQ binding must use
	// that same key — otherwise the binding is broken on a direct DLX, where "#" is a
	// literal routing key rather than the topic wildcard.
	topo := &Topology{
		Exchanges:   []Exchange{{Name: "orders.dlx", Kind: ExchangeDirect, Durable: true}},
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx", RoutingKey: "dead"}},
	}
	expanded := topo.expand()
	found := false
	for _, b := range expanded.Bindings {
		if b.Exchange == "orders.dlx" && b.Queue == "orders.dlq" {
			assert.Equal(t, "dead", b.RoutingKey,
				"DLQ binding must use the dead-letter routing key, not the topic-only \"#\" wildcard")
			found = true
		}
	}
	assert.True(t, found, "expand() must bind the DLX to the DLQ")
}

func TestTopology_expand_dlxBindsDefaultExchangeName(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders"}}, // default <source>.dlx + <source>.dlq
	}
	expanded := topo.expand()
	found := false
	for _, b := range expanded.Bindings {
		if b.Exchange == "orders.dlx" && b.Queue == "orders.dlq" && b.RoutingKey == "#" {
			found = true
		}
	}
	assert.True(t, found, "expand() must bind the default DLX to the default DLQ")
}

func TestTopology_expand_dlxDoesNotDuplicateExistingBinding(t *testing.T) {
	topo := &Topology{
		Queues:   []Queue{{Name: "orders", Durable: true}},
		Bindings: []Binding{{Exchange: "orders.dlx", Queue: "orders.dlq", RoutingKey: "#"}},
		DeadLetters: []DeadLetter{
			{Source: "orders", Exchange: "orders.dlx"},
		},
	}
	expanded := topo.expand()
	count := 0
	for _, b := range expanded.Bindings {
		if b.Exchange == "orders.dlx" && b.Queue == "orders.dlq" && b.RoutingKey == "#" {
			count++
		}
	}
	assert.Equal(t, 1, count, "DLX->DLQ binding must not be duplicated when already declared")
}

func TestTopology_expand_dlxDoesNotDuplicateExistingBindingWithRoutingKey(t *testing.T) {
	// The dedup index keys on the chosen routing key, not a hardcoded "#". When the
	// caller pre-declares the exact DLX->DLQ binding that DeadLetter.RoutingKey would
	// produce, expand() must not append a second (duplicate) binding.
	topo := &Topology{
		Exchanges: []Exchange{{Name: "orders.dlx", Kind: ExchangeDirect, Durable: true}},
		Queues:    []Queue{{Name: "orders", Durable: true}},
		Bindings:  []Binding{{Exchange: "orders.dlx", Queue: "orders.dlq", RoutingKey: "dead"}},
		DeadLetters: []DeadLetter{
			{Source: "orders", Exchange: "orders.dlx", RoutingKey: "dead"},
		},
	}
	expanded := topo.expand()
	count := 0
	for _, b := range expanded.Bindings {
		if b.Exchange == "orders.dlx" && b.Queue == "orders.dlq" && b.RoutingKey == "dead" {
			count++
		}
	}
	assert.Equal(t, 1, count, "DLX->DLQ binding must not be duplicated when the same routing-key binding is already declared")
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

// findQueue returns a pointer to the queue named name in topo, or nil.
func findQueue(topo *Topology, name string) *Queue {
	for i := range topo.Queues {
		if topo.Queues[i].Name == name {
			return &topo.Queues[i]
		}
	}
	return nil
}

// SPEC §10 decision 52 (Rev 9): Topology.Declare implicitly injects
// x-dead-letter-strategy: at-least-once for quorum queues with a DLX, so
// dead-lettering preserves messages (the project's at-least-once contract).

func TestTopology_expand_injectsAtLeastOnceStrategyForQuorumWithDLX(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.Equal(t, "at-least-once", src.Args["x-dead-letter-strategy"])
}

func TestTopology_expand_injectsStrategyForQuorumWithManualDLX(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{"x-dead-letter-exchange": "my.dlx"},
		}},
	}
	expanded := topo.expand()
	assert.Equal(t, "at-least-once", expanded.Queues[0].Args["x-dead-letter-strategy"])
}

func TestTopology_expand_noStrategyForQuorumWithoutDLX(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
	}
	expanded := topo.expand()
	assert.NotContains(t, expanded.Queues[0].Args, "x-dead-letter-strategy")
}

func TestTopology_expand_noStrategyForClassicWithDLX(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeClassic}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.NotContains(t, src.Args, "x-dead-letter-strategy")
}

// The strategy is gated on quorum; a stream queue with a DLX must not get it.
func TestTopology_expand_noStrategyForStreamWithDLX(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "feed", Durable: true, Type: QueueTypeStream,
			Args: map[string]any{"x-dead-letter-exchange": "my.dlx"},
		}},
	}
	expanded := topo.expand()
	assert.NotContains(t, expanded.Queues[0].Args, "x-dead-letter-strategy")
}

// The auto-created <source>.dlq is a classic queue and must not inherit the
// quorum at-least-once strategy from its source.
func TestTopology_expand_autoCreatedDLQHasNoStrategy(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	dlq := findQueue(expanded, "orders.dlq")
	require.NotNil(t, dlq, "DLX pre-pass must append the <source>.dlq queue")
	assert.NotContains(t, dlq.Args, "x-dead-letter-strategy")
}

func TestTopology_expand_respectsUserDeadLetterStrategy(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{"x-dead-letter-strategy": "at-most-once"},
		}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.Equal(t, "at-most-once", src.Args["x-dead-letter-strategy"])
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

func (r *declareRecorder) ExchangeBind(destination, key, source string, noWait bool, args amqp091.Table) error {
	r.calls = append(r.calls, "ebind:"+source+"->"+destination)
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

func TestTopology_declareOnChannel_queueDeclare406WrapsTopologyMismatch(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true}},
	}
	amqpErr := &amqp091.Error{Code: 406, Reason: "PRECONDITION_FAILED"}
	rec := &declareRecorder{err: amqpErr}
	err := declareOnChannel(topo, rec)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyMismatch)
	assert.ErrorIs(t, err, ErrPreconditionFailed)
}

func TestTopology_declareOnChannel_queueBind406WrapsTopologyMismatch(t *testing.T) {
	topo := &Topology{
		Bindings: []Binding{{Exchange: "events", Queue: "orders", RoutingKey: "order.#"}},
	}
	amqpErr := &amqp091.Error{Code: 406, Reason: "PRECONDITION_FAILED"}
	rec := &declareRecorder{err: amqpErr}
	err := declareOnChannel(topo, rec)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyMismatch)
	assert.ErrorIs(t, err, ErrPreconditionFailed)
}

func TestTopology_declareOnChannel_non406ErrorNotMismatch(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic}},
	}
	amqpErr := &amqp091.Error{Code: 404, Reason: "NOT_FOUND"}
	rec := &declareRecorder{err: amqpErr}
	err := declareOnChannel(topo, rec)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound, "404 must wrap ErrNotFound, not ErrTopologyMismatch")
	assert.NotErrorIs(t, err, ErrTopologyMismatch, "non-406 error must not be wrapped as ErrTopologyMismatch")
}

func TestTopology_declareOnChannel_emptyTopologyNoError(t *testing.T) {
	topo := &Topology{}
	rec := &declareRecorder{}
	err := declareOnChannel(topo, rec)
	require.NoError(t, err)
	assert.Empty(t, rec.calls, "empty topology must emit zero declare calls")
}

func TestTopology_declareOnChannel_onlyExchanges(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "events", Kind: ExchangeTopic, Durable: true}},
	}
	rec := &declareRecorder{}
	require.NoError(t, declareOnChannel(topo, rec))
	require.Len(t, rec.calls, 1)
	assert.Equal(t, "exchange:events", rec.calls[0])
}

func TestTopology_declareOnChannel_onlyQueues(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true}},
	}
	rec := &declareRecorder{}
	require.NoError(t, declareOnChannel(topo, rec))
	require.Len(t, rec.calls, 1)
	assert.Equal(t, "queue:orders", rec.calls[0])
}

// — T15: validate — additional rules —————————————————————————————————————

func TestTopology_validate_unknownExchangeKind(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "ev", Kind: ExchangeKind("x-bogus")}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_emptyExchangeKind(t *testing.T) {
	topo := &Topology{
		Exchanges: []Exchange{{Name: "ev", Kind: ""}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_unknownQueueType(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Type: QueueType("super-queue")}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_maxPriorityNegative(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", MaxPriority: -1}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_streamWithDeadLetter(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "stream.q", Type: QueueTypeStream, Durable: true}},
		DeadLetters: []DeadLetter{{Source: "stream.q"}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestTopology_validate_unknownOverflowPolicy(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "q", Durable: true}},
		DeadLetters: []DeadLetter{
			{Source: "q", Overflow: OverflowPolicy("drop-all")},
		},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// — T15: expand — additional coverage —————————————————————————————————————

func TestTopology_expand_dlxSkipsUnknownSource(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "payments"}}, // not in Queues
	}
	expanded := topo.expand()
	require.Len(t, expanded.Queues, 1, "unknown source must not append DLQ")
	assert.Nil(t, expanded.Queues[0].Args, "unknown source must not inject args into any queue")
	assert.Empty(t, expanded.Exchanges, "unknown source must not append DLX exchange")
}

func TestTopology_expand_exchangeArgsMutationDoesNotAffectSnapshot(t *testing.T) {
	args := map[string]any{"x-custom": "val"}
	topo := &Topology{
		Exchanges: []Exchange{{Name: "ev", Kind: ExchangeTopic, Args: args}},
	}
	expanded := topo.expand()
	expanded.Exchanges[0].Args["x-custom"] = "mutated"
	assert.Equal(t, "val", args["x-custom"], "mutation in expanded copy must not affect original args map")
}

func TestTopology_expand_bindingArgsMutationDoesNotAffectSnapshot(t *testing.T) {
	args := map[string]any{"x-match": "all"}
	topo := &Topology{
		Bindings: []Binding{{Exchange: "ev", Queue: "q", Args: args}},
	}
	expanded := topo.expand()
	expanded.Bindings[0].Args["x-match"] = "any"
	assert.Equal(t, "all", args["x-match"], "mutation in expanded copy must not affect original args map")
}

func TestTopology_expand_nestedArgsAreCopiedRecursively(t *testing.T) {
	nested := map[string]any{"type": "order"}
	topo := &Topology{
		Queues: []Queue{{Name: "q", Args: map[string]any{"inner": nested}}},
	}
	expanded := topo.expand()
	// Mutate the nested map in the expanded snapshot.
	expandedInner := expanded.Queues[0].Args["inner"].(map[string]any)
	expandedInner["type"] = "mutated"
	assert.Equal(t, "order", nested["type"], "recursive copy must isolate nested maps")
}

func TestTopology_expand_multipleDeadLetters(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{
			{Name: "payments", Durable: true},
			{Name: "orders", Durable: true},
		},
		DeadLetters: []DeadLetter{
			{Source: "payments", Exchange: "payments.dlx"},
			{Source: "orders", Exchange: "orders.dlx"},
		},
	}
	expanded := topo.expand()

	exchNames := make(map[string]struct{})
	for _, e := range expanded.Exchanges {
		exchNames[e.Name] = struct{}{}
	}
	assert.Contains(t, exchNames, "payments.dlx", "payments DLX must be present")
	assert.Contains(t, exchNames, "orders.dlx", "orders DLX must be present")

	queueArgs := make(map[string]map[string]any)
	for _, q := range expanded.Queues {
		queueArgs[q.Name] = q.Args
	}
	assert.Equal(t, "payments.dlx", queueArgs["payments"]["x-dead-letter-exchange"])
	assert.Equal(t, "orders.dlx", queueArgs["orders"]["x-dead-letter-exchange"])
}

func TestTopology_expand_dlxMergesIntoPrePopulatedQueueArgs(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Args: map[string]any{"x-custom": "val"}}},
		DeadLetters: []DeadLetter{{Source: "orders", Exchange: "orders.dlx"}},
	}
	expanded := topo.expand()
	args := expanded.Queues[0].Args
	assert.Equal(t, "val", args["x-custom"], "pre-existing args must survive DLX merge")
	assert.Equal(t, "orders.dlx", args["x-dead-letter-exchange"], "DLX arg must be injected")
}

// — copyArgs unit test ————————————————————————————————————————————————————

func TestCopyArgs_isolatesTopLevelKeys(t *testing.T) {
	src := map[string]any{"k": "v"}
	dst := copyArgs(src)
	dst["k"] = "changed"
	assert.Equal(t, "v", src["k"], "mutation of dst must not affect src")
}

func TestCopyArgs_isolatesNestedMaps(t *testing.T) {
	nested := map[string]any{"inner": "val"}
	src := map[string]any{"n": nested}
	dst := copyArgs(src)
	dst["n"].(map[string]any)["inner"] = "changed"
	assert.Equal(t, "val", nested["inner"], "mutation of dst nested map must not affect src")
}

func TestCopyArgs_emptyMap(t *testing.T) {
	dst := copyArgs(map[string]any{})
	assert.Empty(t, dst)
	assert.NotNil(t, dst)
}

// LATER-55 + review: every queue setting the library manages from a dedicated
// typed field must come from that field, not a raw Args entry, so a single
// source of truth drives the type-gated expansion (notably the at-least-once
// injection, which keys on the Type field).
func TestTopology_validate_rejectsRawManagedQueueArgs(t *testing.T) {
	for _, tc := range []struct {
		key   string
		field string
	}{
		{"x-queue-type", "Type"},
		{"x-delivery-limit", "DeliveryLimit"},
		{"x-single-active-consumer", "SingleActiveConsumer"},
		{"x-max-priority", "MaxPriority"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			topo := &Topology{
				Queues: []Queue{{
					Name:    "orders",
					Durable: true,
					Args:    map[string]any{tc.key: "x"},
				}},
			}
			err := topo.validate()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOptions)
			assert.Contains(t, err.Error(), tc.field+" field")
		})
	}
}

// The manual dead-letter path must stay valid: x-dead-letter-* keys (and a
// strategy override) are legitimately caller-set and must not be rejected.
func TestTopology_validate_allowsManualDeadLetterArgs(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{
				"x-dead-letter-exchange": "my.dlx",
				"x-dead-letter-strategy": "at-most-once",
			},
		}},
	}
	require.NoError(t, topo.validate())
}

// FuzzTopologyExpand locks the no-panic guarantee of validate()+expand() against
// arbitrary caller-supplied args (LATER-56). A panic in either fails the fuzz.
//
// The queue and exchange paths are kept separate on purpose: exchange kinds and
// queue types are disjoint string sets, so reusing one fuzz string for both
// would make validate() always fail and expand() never run. The queue topology
// (Type from kind) reaches expand() for valid queue types; the delayed-exchange
// topology exercises the x-delayed-type type-assertion branch.
func FuzzTopologyExpand(f *testing.F) {
	f.Add("orders", "quorum", "x-dead-letter-exchange", "dlx", 0)
	f.Add("feed", "stream", "x-dead-letter-exchange", "dlx", 0)
	f.Add("q", "classic", "x-max-priority", "5", 1)
	f.Add("q", "", "x-queue-type", "quorum", 2)
	f.Add("delay", "topic", "x-delayed-type", "direct", 0)
	f.Add("", "bogus", "", "", 3)

	f.Fuzz(func(t *testing.T, name, kind, argKey, argVal string, valKind int) {
		var v any
		switch ((valKind % 4) + 4) % 4 {
		case 0:
			v = argVal
		case 1:
			v = len(argVal) // int
		case 2:
			v = len(argVal)%2 == 0 // bool
		case 3:
			v = nil
		}
		args := map[string]any{}
		if argKey != "" {
			args[argKey] = v
		}

		// Queue path: Type ranges over (in)valid queue types. When it validates,
		// expand() runs — exercising arg injection and the nil-map write paths.
		queueTopo := &Topology{
			Queues:      []Queue{{Name: name, Type: QueueType(kind), Args: args}},
			DeadLetters: []DeadLetter{{Source: name}},
		}
		if err := queueTopo.validate(); err == nil {
			_ = queueTopo.expand()
		}

		// Exchange path: a delayed exchange exercises the x-delayed-type
		// type-assertion/validation branch with arbitrary arg shapes.
		exchTopo := &Topology{
			Exchanges: []Exchange{{Name: name, Kind: ExchangeDelayed, Args: args}},
		}
		if err := exchTopo.validate(); err == nil {
			_ = exchTopo.expand()
		}
	})
}
