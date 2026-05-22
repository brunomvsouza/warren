package warren

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
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

// AttachTo registers a deep snapshot of t as a reconnect redeclare callback on
// conn. On every reconnect cycle, the snapshot is passed to Topology.Declare
// inside the synchronous reconnect barrier — before publishers resume and before
// consumers re-issue basic.consume.
//
// Snapshots are keyed by the pointer address of t. Calling AttachTo(conn) with
// the same *Topology pointer a second time replaces the prior snapshot (useful
// when the caller edits the topology and wants the new shape on the next
// reconnect). Calling AttachTo(conn) with a different pointer appends a new
// snapshot; all registered snapshots fire in registration order.
//
// Topology.Declare and AttachTo are NOT concurrency-safe with each other.
// Recommended pattern: call Declare once at startup (sync.Once), then call
// AttachTo on the same topology to wire up reconnect.
func (t *Topology) AttachTo(conn *Connection) {
	snapshot := t.expand()

	conn.topoMu.Lock()
	if conn.topoSnaps == nil {
		conn.topoSnaps = make(map[*Topology]*Topology)
	}
	if _, exists := conn.topoSnaps[t]; !exists {
		conn.topoKeys = append(conn.topoKeys, t)
	}
	conn.topoSnaps[t] = snapshot
	alreadyHooked := conn.topoHooked
	conn.topoHooked = true
	conn.topoMu.Unlock()

	if alreadyHooked {
		return
	}

	// Register one hook per managed connection, capturing mc so the hook can
	// open a channel on the specific reconnected socket.
	// conn.pubConns and conn.conConns are immutable after Dial, so this
	// access is safe without a lock.
	for _, mc := range append(conn.pubConns, conn.conConns...) {
		mc := mc
		mc.registerHook(func(ctx context.Context) error {
			return conn.runTopologyRedeclare(ctx, mc)
		})
	}
}

// runTopologyRedeclare iterates the registered topology snapshots in registration
// order and declares each on a fresh channel. Called from the synchronous
// reconnect barrier.
func (c *Connection) runTopologyRedeclare(_ context.Context, mc *managedConn) error {
	// Snapshot both keys and values under a single lock to avoid repeated
	// lock acquisitions inside the loop.
	c.topoMu.RLock()
	keys := make([]*Topology, len(c.topoKeys))
	copy(keys, c.topoKeys)
	snaps := make(map[*Topology]*Topology, len(c.topoSnaps))
	for k, v := range c.topoSnaps {
		snaps[k] = v
	}
	c.topoMu.RUnlock()

	for _, key := range keys {
		ch, err := mc.openChannel()
		if err != nil {
			return err
		}
		declErr := declareOnChannel(snaps[key], ch)
		_ = ch.Close()
		if declErr != nil {
			return declErr
		}
	}
	return nil
}

// topologyChannel is the AMQP channel interface used by Topology.Declare.
// *amqp091.Channel satisfies this interface; tests may inject fakes.
type topologyChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp091.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp091.Table) error
	Close() error
}

// Declare validates the topology, expands DLX entries in-memory (Step 1),
// then opens a temporary channel and emits exchange → queue → binding
// declares in that order (Step 2). It is idempotent: re-declaring the same
// shape returns nil.
//
// Topology.Declare is NOT concurrency-safe with itself or with AttachTo.
// Recommended pattern: call Declare exactly once at application startup
// (e.g. protected by sync.Once), then call AttachTo for reconnect hooks.
func (t *Topology) Declare(ctx context.Context, conn *Connection) error {
	if err := t.validate(); err != nil {
		return err
	}
	expanded := t.expand()

	ch, err := conn.openDeclareChannel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return declareOnChannel(expanded, ch)
}

// expand returns a deep copy of t with all in-memory pre-pass mutations applied:
//   - DeadLetter entries merge x-dead-letter-* args into source Queue.Args and
//     append the DLX exchange and DLQ queue if not already present.
//   - DeliveryLimit > 0 injects x-delivery-limit.
//   - SingleActiveConsumer injects x-single-active-consumer.
//   - Type != "" injects x-queue-type.
//   - MaxPriority > 0 injects x-max-priority.
//
// The caller's Topology value is never mutated.
func (t *Topology) expand() *Topology {
	// Deep-copy all slices so mutations don't affect the original.
	out := &Topology{
		Exchanges:   make([]Exchange, len(t.Exchanges)),
		Queues:      make([]Queue, len(t.Queues)),
		Bindings:    make([]Binding, len(t.Bindings)),
		DeadLetters: make([]DeadLetter, len(t.DeadLetters)),
	}
	copy(out.DeadLetters, t.DeadLetters)
	for i, b := range t.Bindings {
		out.Bindings[i] = b
		if b.Args != nil {
			out.Bindings[i].Args = copyArgs(b.Args)
		}
	}
	for i, e := range t.Exchanges {
		out.Exchanges[i] = e
		if e.Args != nil {
			out.Exchanges[i].Args = copyArgs(e.Args)
		}
	}
	for i, q := range t.Queues {
		out.Queues[i] = q
		if q.Args != nil {
			out.Queues[i].Args = copyArgs(q.Args)
		}
	}

	// Index queues by name for O(1) arg injection.
	queueIdx := make(map[string]int, len(out.Queues))
	for i, q := range out.Queues {
		queueIdx[q.Name] = i
	}

	// Index existing exchange and queue names to avoid duplicates.
	exchNames := make(map[string]struct{}, len(out.Exchanges))
	for _, e := range out.Exchanges {
		exchNames[e.Name] = struct{}{}
	}
	queueNames := make(map[string]struct{}, len(out.Queues))
	for _, q := range out.Queues {
		queueNames[q.Name] = struct{}{}
	}

	// DLX expansion.
	for _, dl := range out.DeadLetters {
		dlxName := dl.Exchange
		if dlxName == "" {
			dlxName = dl.Source + ".dlx"
		}
		dlqName := dl.Source + ".dlq"

		idx, ok := queueIdx[dl.Source]
		if !ok {
			continue // source queue not declared in this topology; skip
		}
		if out.Queues[idx].Args == nil {
			out.Queues[idx].Args = make(map[string]any)
		}
		out.Queues[idx].Args["x-dead-letter-exchange"] = dlxName
		if dl.RoutingKey != "" {
			out.Queues[idx].Args["x-dead-letter-routing-key"] = dl.RoutingKey
		}
		if dl.TTL > 0 {
			out.Queues[idx].Args["x-message-ttl"] = dl.TTL.Milliseconds()
		}
		if dl.MaxLength > 0 {
			out.Queues[idx].Args["x-max-length"] = int64(dl.MaxLength)
		}
		if dl.MaxLengthBytes > 0 {
			out.Queues[idx].Args["x-max-length-bytes"] = int64(dl.MaxLengthBytes)
		}
		if dl.Overflow != "" {
			out.Queues[idx].Args["x-overflow"] = string(dl.Overflow)
		}

		if _, exists := exchNames[dlxName]; !exists {
			out.Exchanges = append(out.Exchanges, Exchange{
				Name:    dlxName,
				Kind:    ExchangeTopic,
				Durable: true,
			})
			exchNames[dlxName] = struct{}{}
		}
		if _, exists := queueNames[dlqName]; !exists {
			out.Queues = append(out.Queues, Queue{
				Name:    dlqName,
				Durable: true,
			})
			queueNames[dlqName] = struct{}{}
		}
	}

	// Inject per-queue x-* args for DeliveryLimit, SingleActiveConsumer, Type, MaxPriority.
	for i := range out.Queues {
		q := &out.Queues[i]
		if q.DeliveryLimit > 0 || q.SingleActiveConsumer || q.Type != "" || q.MaxPriority > 0 {
			if q.Args == nil {
				q.Args = make(map[string]any)
			}
			if q.DeliveryLimit > 0 {
				q.Args["x-delivery-limit"] = int64(q.DeliveryLimit)
			}
			if q.SingleActiveConsumer {
				q.Args["x-single-active-consumer"] = true
			}
			if q.Type != "" {
				q.Args["x-queue-type"] = string(q.Type)
			}
			if q.MaxPriority > 0 {
				q.Args["x-max-priority"] = int64(q.MaxPriority)
			}
		}
	}

	return out
}

// declareOnChannel emits exchange → queue → binding declares onto ch.
// Returns ErrTopologyMismatch (wrapping ErrPreconditionFailed) when the broker
// signals a 406 PRECONDITION_FAILED conflict on any declare.
func declareOnChannel(topo *Topology, ch topologyChannel) error {
	wrapMismatch := func(err error) error {
		wrapped := wrapAMQPError(err)
		if errors.Is(wrapped, ErrPreconditionFailed) {
			return fmt.Errorf("%w: %w", ErrTopologyMismatch, wrapped)
		}
		return wrapped
	}

	for _, e := range topo.Exchanges {
		if err := ch.ExchangeDeclare(e.Name, string(e.Kind), e.Durable, e.AutoDelete, e.Internal, e.NoWait, amqp091.Table(e.Args)); err != nil {
			return wrapMismatch(err)
		}
	}
	for _, q := range topo.Queues {
		if _, err := ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, amqp091.Table(q.Args)); err != nil {
			return wrapMismatch(err)
		}
	}
	for _, b := range topo.Bindings {
		if err := ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, b.NoWait, amqp091.Table(b.Args)); err != nil {
			return wrapMismatch(err)
		}
	}
	return nil
}

func copyArgs(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
		if q.MaxPriority > 0 && q.Type != "" && q.Type != QueueTypeClassic {
			return fmt.Errorf("%w: Queue %q: MaxPriority requires Type=QueueTypeClassic", ErrInvalidOptions, q.Name)
		}
		if q.MaxPriority > 255 {
			return fmt.Errorf("%w: Queue %q: MaxPriority must be in [1, 255]", ErrInvalidOptions, q.Name)
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

	for _, dl := range t.DeadLetters {
		if dl.Source == "" {
			return fmt.Errorf("%w: DeadLetter.Source must not be empty", ErrInvalidOptions)
		}
	}

	return nil
}
