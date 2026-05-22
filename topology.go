package amqp

import (
	"fmt"
	"time"
)

// Topology describes the AMQP exchanges, queues, bindings, and dead-letter rules
// to be declared on the broker. Declare it once and reuse via AttachTo(conn) so
// the reconnect barrier can redeclare the full topology after a reconnect.
//
// Topology.Declare is NOT concurrency-safe with itself or with AttachTo.
// Recommended pattern: call Declare once during application startup (sync.Once),
// then call AttachTo on the same Topology to hook into reconnect.
type Topology struct {
	Exchanges   []Exchange
	Queues      []Queue
	Bindings    []Binding
	DeadLetters []DeadLetter
}

// Exchange declares an AMQP exchange.
type Exchange struct {
	Name       string
	Kind       ExchangeKind
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]any
}

// Queue declares an AMQP queue.
type Queue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	// Type selects the queue implementation (classic, quorum, stream).
	// An empty value means the broker default (classic).
	Type QueueType
	// DeliveryLimit is the broker-enforced redelivery cap for quorum queues
	// (maps to x-delivery-limit). Zero means unbounded. Non-zero on a
	// non-quorum queue is rejected by Topology.validate.
	DeliveryLimit int
	// SingleActiveConsumer maps to x-single-active-consumer.
	// Not allowed on stream queues.
	SingleActiveConsumer bool
	// MaxPriority sets x-max-priority. Only valid on classic queues.
	MaxPriority int
	Args        map[string]any
}

// Binding declares an AMQP queue binding.
type Binding struct {
	Exchange   string
	Queue      string
	RoutingKey string
	NoWait     bool
	Args       map[string]any
}

// DeadLetter describes a dead-letter topology entry. Topology.Declare expands
// it into the required x-dead-letter-* queue args, a DLX exchange, and a DLQ
// during the in-memory pre-pass (Step 1), so the broker sees the args on the
// source queue's first declare and never needs a re-declare.
type DeadLetter struct {
	// Source is the name of the source queue that routes dead letters.
	Source string
	// Exchange is the name of the dead-letter exchange. Defaults to "<Source>.dlx".
	Exchange string
	// RoutingKey is the routing key for dead letters. Empty means the original key.
	RoutingKey string
	// TTL is a per-message TTL (x-message-ttl) applied to the source queue.
	TTL time.Duration
	// MaxLength is the max number of messages (x-max-length) on the source queue.
	MaxLength int
	// MaxLengthBytes is the max byte capacity (x-max-length-bytes).
	MaxLengthBytes int
	// Overflow controls what happens when the queue is full (x-overflow).
	Overflow OverflowPolicy
}

// validate checks the Topology for constraint violations.
// Returns ErrInvalidOptions with a descriptive message on the first violation found.
func (t *Topology) validate() error {
	// Duplicate name checks.
	exchNames := make(map[string]struct{}, len(t.Exchanges))
	for _, e := range t.Exchanges {
		if e.Name == "" {
			return fmt.Errorf("%w: Exchange.Name must not be empty", ErrInvalidOptions)
		}
		if _, dup := exchNames[e.Name]; dup {
			return fmt.Errorf("%w: duplicate Exchange name %q", ErrInvalidOptions, e.Name)
		}
		exchNames[e.Name] = struct{}{}

		if e.Kind == ExchangeDelayed {
			v, ok := e.Args["x-delayed-type"]
			if !ok || v == "" {
				return fmt.Errorf("%w: Exchange %q with Kind=ExchangeDelayed must set Args[\"x-delayed-type\"]", ErrInvalidOptions, e.Name)
			}
		}
	}

	queueNames := make(map[string]struct{}, len(t.Queues))
	for _, q := range t.Queues {
		if q.Name == "" {
			return fmt.Errorf("%w: Queue.Name must not be empty", ErrInvalidOptions)
		}
		if _, dup := queueNames[q.Name]; dup {
			return fmt.Errorf("%w: duplicate Queue name %q", ErrInvalidOptions, q.Name)
		}
		queueNames[q.Name] = struct{}{}

		if q.DeliveryLimit > 0 && q.Type != QueueTypeQuorum {
			return fmt.Errorf("%w: Queue %q: DeliveryLimit requires Type=QueueTypeQuorum", ErrInvalidOptions, q.Name)
		}
		if q.Type == QueueTypeStream {
			if q.SingleActiveConsumer {
				return fmt.Errorf("%w: Queue %q: SingleActiveConsumer is not supported on stream queues", ErrInvalidOptions, q.Name)
			}
			if q.Exclusive {
				return fmt.Errorf("%w: Queue %q: Exclusive is not supported on stream queues", ErrInvalidOptions, q.Name)
			}
			if q.AutoDelete {
				return fmt.Errorf("%w: Queue %q: AutoDelete is not supported on stream queues", ErrInvalidOptions, q.Name)
			}
		}
	}

	// Build an exchange-kind lookup for binding validation.
	exchKind := make(map[string]ExchangeKind, len(t.Exchanges))
	for _, e := range t.Exchanges {
		exchKind[e.Name] = e.Kind
	}

	for _, b := range t.Bindings {
		if b.Exchange == "" {
			return fmt.Errorf("%w: Binding.Exchange must not be empty", ErrInvalidOptions)
		}
		if b.Queue == "" {
			return fmt.Errorf("%w: Binding.Queue must not be empty", ErrInvalidOptions)
		}
		if b.RoutingKey != "" {
			if k, ok := exchKind[b.Exchange]; ok && k == ExchangeFanout {
				return fmt.Errorf("%w: Binding to fanout exchange %q must have an empty RoutingKey", ErrInvalidOptions, b.Exchange)
			}
		}
	}

	return nil
}
