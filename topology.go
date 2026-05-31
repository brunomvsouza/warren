package warren

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	// NoWait sends the declare without waiting for the broker's reply. This
	// downgrades mismatch detection to asynchronous: Declare returns nil even
	// on a conflicting redeclare, and the broker reports the conflict (e.g.
	// ErrPreconditionFailed) out-of-band on a channel Declare has already
	// closed, so it is generally not observable by the caller. Leave
	// NoWait=false if you rely on Declare surfacing ErrTopologyMismatch.
	NoWait bool
	Args   map[string]any
}

// Queue declares an AMQP queue.
type Queue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	// NoWait sends the declare without waiting for the broker's reply. This
	// downgrades mismatch detection to asynchronous: Declare returns nil even
	// on a conflicting redeclare, and the broker reports the conflict (e.g.
	// ErrPreconditionFailed) out-of-band on a channel Declare has already
	// closed, so it is generally not observable by the caller. Leave
	// NoWait=false if you rely on Declare surfacing ErrTopologyMismatch.
	NoWait bool
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
	// NoWait sends the bind without waiting for the broker's reply. This
	// downgrades mismatch detection to asynchronous: Declare returns nil even
	// on a conflicting bind, and the broker reports the conflict (e.g.
	// ErrPreconditionFailed) out-of-band on a channel Declare has already
	// closed, so it is generally not observable by the caller. Leave
	// NoWait=false if you rely on Declare surfacing ErrTopologyMismatch.
	NoWait bool
	Args   map[string]any
}

// DeadLetter describes a dead-letter topology entry. Topology.Declare expands
// it into the required x-dead-letter-* queue args, a DLX exchange, and a DLQ
// during the in-memory pre-pass (Step 1), so the broker sees the args on the
// source queue's first declare and never needs a re-declare.
//
// When the source queue is a quorum queue, Declare also injects
// x-dead-letter-strategy=at-least-once so messages are preserved during
// dead-lettering (SPEC §10 decision 52). Set x-dead-letter-strategy in the
// source Queue.Args to override this default.
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
// Returns ErrInvalidOptions if the topology fails validation. The recommended
// pattern is to call Declare first (which also validates), then AttachTo on the
// same pointer — that way validation errors surface at startup, not on reconnect.
//
// Topology.Declare and AttachTo are NOT concurrency-safe with each other.
func (t *Topology) AttachTo(conn *Connection) error {
	if err := t.validate(); err != nil {
		return err
	}
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
		return nil
	}

	// Register one hook per managed connection, capturing mc so the hook can
	// open a channel on the specific reconnected socket.
	// conn.pubConns and conn.conConns are immutable after Dial, so this
	// access is safe without a lock.
	for _, mc := range append(conn.pubConns, conn.conConns...) {
		mc.registerHook(func(ctx context.Context) error {
			return conn.runTopologyRedeclare(ctx, mc)
		})
	}
	return nil
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
	maps.Copy(snaps, c.topoSnaps)
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
// shape returns nil. A conflicting redeclare returns ErrTopologyMismatch
// (which also satisfies errors.Is(err, ErrPreconditionFailed)).
//
// When any entry sets NoWait=true, Declare cannot detect a conflict on that
// entry synchronously and returns nil even on a mismatch; see the NoWait field
// docs.
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
//   - A quorum queue with a dead-letter exchange injects
//     x-dead-letter-strategy=at-least-once (SPEC §10 decision 52).
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

	// Index existing exchange->queue->routing-key bindings to avoid appending a
	// duplicate DLX->DLQ binding the caller already declared.
	type bindingKey struct{ exchange, queue, routingKey string }
	bindingKeys := make(map[bindingKey]struct{}, len(out.Bindings))
	for _, b := range out.Bindings {
		bindingKeys[bindingKey{b.Exchange, b.Queue, b.RoutingKey}] = struct{}{}
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

		// Bind the DLX to the DLQ so dead-lettered messages actually land in the
		// DLQ rather than routing into limbo. The DLX is a topic exchange, so "#"
		// matches every routing key. Skip if the caller already declared it.
		bk := bindingKey{dlxName, dlqName, "#"}
		if _, exists := bindingKeys[bk]; !exists {
			out.Bindings = append(out.Bindings, Binding{
				Exchange:   dlxName,
				Queue:      dlqName,
				RoutingKey: "#",
			})
			bindingKeys[bk] = struct{}{}
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

		// SPEC §10 decision 52 (Rev 9): a quorum queue with a dead-letter
		// exchange gets x-dead-letter-strategy=at-least-once so messages are
		// preserved across dead-lettering (the project's at-least-once contract).
		// The DLX may come from a DeadLetter pre-pass above or be set manually
		// by the caller in Args. An explicit caller value is respected.
		if q.Type == QueueTypeQuorum {
			if _, hasDLX := q.Args["x-dead-letter-exchange"]; hasDLX {
				if _, hasStrategy := q.Args["x-dead-letter-strategy"]; !hasStrategy {
					q.Args["x-dead-letter-strategy"] = "at-least-once"
				}
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
		if nested, ok := v.(map[string]any); ok {
			dst[k] = copyArgs(nested)
		} else {
			dst[k] = v
		}
	}
	return dst
}

// validate checks the Topology for constraint violations.
// Returns ErrInvalidOptions with a descriptive message on the first violation found.
func (t *Topology) validate() error {
	validKinds := map[ExchangeKind]struct{}{
		ExchangeDirect:  {},
		ExchangeFanout:  {},
		ExchangeTopic:   {},
		ExchangeHeaders: {},
		ExchangeDelayed: {},
	}
	validQueueTypes := map[QueueType]struct{}{
		"":               {}, // broker default (classic)
		QueueTypeClassic: {},
		QueueTypeQuorum:  {},
		QueueTypeStream:  {},
	}
	validOverflow := map[OverflowPolicy]struct{}{
		"":                       {}, // not set
		OverflowDropHead:         {},
		OverflowRejectPublish:    {},
		OverflowRejectPublishDLX: {},
	}

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

		if e.Kind == "" {
			return fmt.Errorf("%w: Exchange %q: Kind must not be empty", ErrInvalidOptions, e.Name)
		}
		if _, ok := validKinds[e.Kind]; !ok {
			return fmt.Errorf("%w: Exchange %q: unknown Kind %q", ErrInvalidOptions, e.Name, e.Kind)
		}
		if e.Kind == ExchangeDelayed {
			v, ok := e.Args["x-delayed-type"]
			if !ok {
				return fmt.Errorf("%w: Exchange %q with Kind=ExchangeDelayed must set Args[\"x-delayed-type\"]", ErrInvalidOptions, e.Name)
			}
			s, isStr := v.(string)
			if !isStr || s == "" {
				return fmt.Errorf("%w: Exchange %q: Args[\"x-delayed-type\"] must be a non-empty exchange-kind string", ErrInvalidOptions, e.Name)
			}
			// x-delayed-type must name a routing kind, not the delayed kind itself.
			if kind := ExchangeKind(s); kind == ExchangeDelayed {
				return fmt.Errorf("%w: Exchange %q: Args[\"x-delayed-type\"] must be a routing kind (direct, fanout, topic, headers), not %q", ErrInvalidOptions, e.Name, s)
			} else if _, valid := validKinds[kind]; !valid {
				return fmt.Errorf("%w: Exchange %q: Args[\"x-delayed-type\"] %q is not a valid exchange kind", ErrInvalidOptions, e.Name, s)
			}
		}
	}

	streamQueues := make(map[string]struct{})
	queueNames := make(map[string]struct{}, len(t.Queues))
	for _, q := range t.Queues {
		if q.Name == "" {
			return fmt.Errorf("%w: Queue.Name must not be empty", ErrInvalidOptions)
		}
		if _, dup := queueNames[q.Name]; dup {
			return fmt.Errorf("%w: duplicate Queue name %q", ErrInvalidOptions, q.Name)
		}
		queueNames[q.Name] = struct{}{}

		// These args are set by the library from dedicated typed fields.
		// Accepting them raw in Args would create a second source of truth that
		// bypasses the type-gated expansion (e.g. the at-least-once DLX strategy
		// keys on Type). x-dead-letter-* keys are intentionally absent here —
		// the manual dead-letter path sets them directly.
		for _, managed := range [...]struct{ key, field string }{
			{"x-queue-type", "Type"},
			{"x-delivery-limit", "DeliveryLimit"},
			{"x-single-active-consumer", "SingleActiveConsumer"},
			{"x-max-priority", "MaxPriority"},
		} {
			if _, raw := q.Args[managed.key]; raw {
				return fmt.Errorf("%w: Queue %q: set the %s field instead of Args[%q]", ErrInvalidOptions, q.Name, managed.field, managed.key)
			}
		}

		if _, ok := validQueueTypes[q.Type]; !ok {
			return fmt.Errorf("%w: Queue %q: unknown Type %q", ErrInvalidOptions, q.Name, q.Type)
		}
		if q.DeliveryLimit > 0 && q.Type != QueueTypeQuorum {
			return fmt.Errorf("%w: Queue %q: DeliveryLimit requires Type=QueueTypeQuorum", ErrInvalidOptions, q.Name)
		}
		if q.MaxPriority != 0 && (q.MaxPriority < 0 || q.MaxPriority > 255) {
			return fmt.Errorf("%w: Queue %q: MaxPriority must be in [1, 255]", ErrInvalidOptions, q.Name)
		}
		if q.MaxPriority > 0 && q.Type != "" && q.Type != QueueTypeClassic {
			return fmt.Errorf("%w: Queue %q: MaxPriority requires Type=QueueTypeClassic", ErrInvalidOptions, q.Name)
		}
		if q.Type == QueueTypeStream {
			streamQueues[q.Name] = struct{}{}
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
		if _, isStream := streamQueues[dl.Source]; isStream {
			return fmt.Errorf("%w: DeadLetter.Source %q: dead-lettering is not supported on stream queues", ErrInvalidOptions, dl.Source)
		}
		if _, ok := validOverflow[dl.Overflow]; !ok {
			return fmt.Errorf("%w: DeadLetter.Source %q: unknown Overflow policy %q", ErrInvalidOptions, dl.Source, dl.Overflow)
		}
	}

	return nil
}
