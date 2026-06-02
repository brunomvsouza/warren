package warren

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/log"
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
	// ExchangeBindings declares exchange→exchange bindings (exchange.bind) for
	// layered fan-out topologies (T69 / EDA-03). It is a SEPARATE slice — Binding
	// (exchange→queue) is intentionally not reshaped (GA-05). The declare-once /
	// deep-snapshot semantics extend to ExchangeBindings.
	ExchangeBindings []ExchangeBinding
}

// ExchangeBinding declares an exchange-to-exchange binding (exchange.bind):
// messages routed to Source that match RoutingKey are forwarded to Destination.
// This enables layered ingest→per-domain fan-out without flattening the topology.
type ExchangeBinding struct {
	// Source is the upstream exchange messages are published to / arrive at.
	Source string
	// Destination is the downstream exchange that receives matching messages.
	Destination string
	// RoutingKey filters which messages are forwarded (matched per Source's kind).
	RoutingKey string
	// NoWait skips the broker confirmation (see the NoWait caveat on Binding).
	NoWait bool
	Args   map[string]any
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
	// AlternateExchange names the server-side catch-all exchange for messages
	// this exchange cannot route (the broker's `alternate-exchange` argument).
	// It is the platform-level unroutable safety net (T68 / EDA-01): a mis-routed
	// publish WITHOUT Mandatory() vanishes silently, and per-publish discipline
	// does not scale across many producers — the alternate exchange catches the
	// unroutable message server-side regardless. The zero value (empty) preserves
	// today's behaviour. Declare the named exchange (and bind a catch-all queue
	// to it) in the same Topology. Set the field, not Args["alternate-exchange"].
	AlternateExchange string
	Args              map[string]any
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
	// (maps to x-delivery-limit). Non-zero on a non-quorum queue is rejected by
	// Topology.validate.
	//
	// The meaning of zero is broker-version dependent and a poison-loop footgun
	// either way, so set it explicitly: on RabbitMQ 4.x a zero DeliveryLimit
	// takes the broker default of 20 (the message is silently dropped/dead-
	// lettered at the 21st delivery), while on RabbitMQ 3.13 a zero
	// DeliveryLimit is genuinely unbounded (an unhandled poison message loops
	// forever). Topology.Declare emits a version-aware warning when a quorum
	// queue is declared with DeliveryLimit==0.
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
//
// at-least-once requires x-overflow=reject-publish; the broker silently accepts
// any overflow but does not honour at-least-once otherwise, so Declare couples
// the two client-side: Overflow left empty is auto-set to reject-publish (with a
// warning), and an Overflow of drop-head or reject-publish-dlx is rejected with
// ErrInvalidOptions. Note the cost: reject-publish blocks publishers
// (ErrPublishNacked / ErrConnectionBlocked) when the source queue is full, and
// at-least-once retains each dead-lettered message in SOURCE-QUEUE memory until
// the DLX consumer acknowledges it — an unconsumed DLX can therefore grow the
// source queue's memory footprint. Size the source queue (DeliveryLimit, TTL,
// MaxLength) and the DLQ, and keep a consumer attached to the DLX.
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

	// The fields below bound the AUTO-declared <Source>.dlq itself (not the
	// source queue). They have no effect when the DLQ is declared explicitly
	// (declare <Source>.dlq yourself for full control). An unbounded DLQ is a
	// disk-fill / broker-wide connection.blocked hazard: one service's poison
	// storm can take out the whole cluster (T65 / SRE-03 / ST-08), so the
	// auto-DLQ is bounded by default.

	// DLQMaxLength caps the auto-declared <Source>.dlq by message count
	// (x-max-length). Zero applies defaultDLQMaxLength unless DLQUnbounded.
	DLQMaxLength int
	// DLQMessageTTL caps the auto-declared <Source>.dlq message lifetime
	// (x-message-ttl) — the personal-data retention control (GDPR 5(1)(e) /
	// LGPD Art. 16). Zero applies defaultDLQMessageTTL unless DLQUnbounded.
	DLQMessageTTL time.Duration
	// DLQOverflow sets x-overflow on the auto-declared DLQ. Empty defaults to
	// OverflowDropHead (keep the most recent failures when full).
	DLQOverflow OverflowPolicy
	// DLQUnbounded opts the auto-declared <Source>.dlq OUT of all default bounds
	// (no x-max-length, no x-message-ttl, no x-overflow). Use ONLY when an
	// external retention policy manages the DLQ — an unbounded DLQ can fill disk
	// and trip a broker-wide connection.blocked alarm (SRE-03 / ST-08).
	DLQUnbounded bool
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
			return conn.ensureTopologyRedeclare(ctx, mc)
		})
	}
	return nil
}

// ensureTopologyRedeclare runs runTopologyRedeclare at most once per recovery
// wave (T62 / DS-09 / SRE-06). The first barrier to enter owns the redeclare;
// concurrent barriers wait for it and then skip (its success anchors the
// coalesce window). A barrier entering within topoRedeclareCoalesceWindow of a
// prior success also skips. Either way the caller's barrier does not return
// until topology is ensured for this wave — so a consumer's subsequent
// basic.consume still finds its queue declared, preserving the per-consumer
// ordering guarantee while collapsing N×pool declares to one.
func (c *Connection) ensureTopologyRedeclare(ctx context.Context, mc *managedConn) error {
	for {
		c.topoRedeclareMu.Lock()
		// Coalesce: a recent success means another barrier in this wave already
		// (re)declared the broker-global topology; skip.
		if !c.topoLastDeclare.IsZero() && time.Since(c.topoLastDeclare) < topoRedeclareCoalesceWindow {
			c.topoRedeclareMu.Unlock()
			return nil
		}
		if wait := c.topoRedeclareWait; wait != nil {
			// Another barrier is mid-redeclare; wait for it, then re-loop (it will
			// have anchored the coalesce window, so we skip on the next pass).
			c.topoRedeclareMu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// We own this wave's redeclare.
		wait := make(chan struct{})
		c.topoRedeclareWait = wait
		c.topoRedeclareMu.Unlock()

		err := c.runTopologyRedeclare(ctx, mc)

		c.topoRedeclareMu.Lock()
		if err == nil {
			c.topoLastDeclare = time.Now()
		}
		c.topoRedeclareWait = nil
		close(wait)
		c.topoRedeclareMu.Unlock()
		return err
	}
}

// runTopologyRedeclare iterates the registered topology snapshots in registration
// order and declares each on a fresh channel. Called from the synchronous
// reconnect barrier.
func (c *Connection) runTopologyRedeclare(ctx context.Context, mc *managedConn) error {
	// Snapshot both keys and values under a single lock to avoid repeated
	// lock acquisitions inside the loop.
	c.topoMu.RLock()
	keys := make([]*Topology, len(c.topoKeys))
	copy(keys, c.topoKeys)
	snaps := make(map[*Topology]*Topology, len(c.topoSnaps))
	maps.Copy(snaps, c.topoSnaps)
	c.topoMu.RUnlock()

	for _, key := range keys {
		// Bail between declares if the barrier was capped (ctx cancelled): no
		// point issuing the rest of the topology against a doomed socket (T63).
		if err := ctx.Err(); err != nil {
			return err
		}
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
	ExchangeBind(destination, key, source string, noWait bool, args amqp091.Table) error
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

	warnDelayedExchanges(conn.opts.logger, expanded.Exchanges)
	warnQuorumDeliveryLimit(conn.opts.logger, expanded.Queues, conn.brokerVersion())
	warnQuorumAtLeastOnceOverflow(conn.opts.logger, t)

	ch, err := conn.openDeclareChannel(ctx)
	if err != nil {
		return err
	}
	defer ch.Close() //nolint:errcheck

	return declareOnChannel(expanded, ch)
}

// warnDelayedExchanges emits one durability warning per ExchangeDelayed in the
// declared topology (T57 / SPEC §6.5). The rabbitmq_delayed_message_exchange
// plugin stores scheduled messages in a node-local, non-replicated table, so a
// confirmed delayed publish can still be lost on node failure — the warning
// makes that load-bearing caveat visible at declare time. A nil logger is a
// no-op (callers pass log.NewNoOp() by default).
func warnDelayedExchanges(logger log.Logger, exchanges []Exchange) {
	if logger == nil {
		return
	}
	for _, e := range exchanges {
		if e.Kind != ExchangeDelayed {
			continue
		}
		logger.Warningf("warren: exchange %q is an ExchangeDelayed (x-delayed-message): "+
			"the plugin stores scheduled messages in a node-local, non-replicated table, "+
			"so a confirmed delayed publish can still be lost on node failure even with "+
			"durable topology and confirms on; for delays that must survive node failure "+
			"prefer a durable queue with x-message-ttl + DLX (see Message.Delay, SPEC §6.5)",
			e.Name)
	}
}

// warnQuorumDeliveryLimit emits a version-aware poison-loop warning for each
// quorum queue declared with DeliveryLimit==0 (T58 / R10-2 / RMQ-06). The
// failure mode is broker-version dependent: RabbitMQ 4.x applies a default
// limit of 20 (a silent drop at the 21st delivery), while RabbitMQ 3.13 treats
// zero as unbounded (an infinite poison loop). brokerVersion is the value read
// from the connection.start server-properties; an empty/unparsable version
// emits a warning covering both modes. A nil logger is a no-op.
func warnQuorumDeliveryLimit(logger log.Logger, queues []Queue, brokerVersion string) {
	if logger == nil {
		return
	}
	major, known := parseBrokerMajor(brokerVersion)
	for _, q := range queues {
		if q.Type != QueueTypeQuorum || q.DeliveryLimit != 0 {
			continue
		}
		// brokerVersion is read verbatim from the connection.start server-
		// properties (broker-controlled, untrusted). Log it with %q so embedded
		// newlines/control bytes are escaped rather than forging log lines.
		switch {
		case known && major >= 4:
			logger.Warningf("warren: quorum queue %q has DeliveryLimit==0; RabbitMQ %q "+
				"applies a broker default of 20, so an unhandled poison message is silently "+
				"dropped/dead-lettered at the 21st delivery — set DeliveryLimit explicitly",
				q.Name, brokerVersion)
		case known && major == 3:
			logger.Warningf("warren: quorum queue %q has DeliveryLimit==0; on RabbitMQ %q "+
				"this is unbounded — an unhandled poison message loops forever — "+
				"set DeliveryLimit explicitly",
				q.Name, brokerVersion)
		default:
			logger.Warningf("warren: quorum queue %q has DeliveryLimit==0; this is a poison-loop "+
				"footgun whose mode depends on the broker version: RabbitMQ 4.x applies a default "+
				"of 20 (silent drop at the 21st delivery) while RabbitMQ 3.13 is unbounded "+
				"(infinite poison loop) — set DeliveryLimit explicitly",
				q.Name)
		}
	}
}

// dlxStrategyAtLeastOnce is the one x-dead-letter-strategy value warren couples
// with x-overflow=reject-publish (decision 52 / RMQ-05). Shared by expand(),
// validate(), and quorumAtLeastOnce so the three reach the same client-side
// verdict for every strategy value — including a non-string one (which the broker
// rejects anyway, but must not yield divergent client-side behaviour).
const dlxStrategyAtLeastOnce = "at-least-once"

// quorumAtLeastOnce reports whether queue q will run the at-least-once
// dead-letter strategy after expansion — i.e. it is a quorum queue, has a DLX
// (via Queue.Args["x-dead-letter-exchange"] or a DeadLetter naming it as
// Source), and its effective x-dead-letter-strategy is at-least-once (the
// injected default unless the caller overrode it). It also returns the
// effective x-overflow that will be set on the source queue: a raw
// Queue.Args["x-overflow"] is overridden by a non-empty DeadLetter.Overflow,
// mirroring expand()'s precedence.
//
// at-least-once REQUIRES x-overflow=reject-publish. The broker accepts any
// overflow (gate G4, and the T76 probe confirmed even reject-publish-dlx is
// accepted on 3.13 and 4.x) but does not honour at-least-once unless overflow
// is reject-publish, so warren couples them client-side: validate() rejects a
// conflicting explicit value and expand() fills reject-publish when unset.
// Both consult this helper so the two stay in lockstep.
func (t *Topology) quorumAtLeastOnce(q Queue) (atLeastOnce bool, overflow string, overflowSet bool) {
	if q.Type != QueueTypeQuorum {
		return false, "", false
	}
	_, hasDLX := q.Args["x-dead-letter-exchange"]
	if v, ok := overflowString(q.Args["x-overflow"]); ok {
		overflow, overflowSet = v, true
	}
	for _, dl := range t.DeadLetters {
		if dl.Source != q.Name {
			continue
		}
		hasDLX = true
		if dl.Overflow != "" {
			overflow, overflowSet = string(dl.Overflow), true
		}
	}
	if !hasDLX {
		return false, "", false
	}
	// A caller override that is not the at-least-once string opts out of the
	// coupling. This mirrors expand()'s effectiveALO exactly — a strategy that is
	// present but not the at-least-once string (including a non-string value) is
	// treated as opt-out, so validate/expand/warn never reach different
	// client-side verdicts for the same input.
	if v, present := q.Args["x-dead-letter-strategy"]; present {
		if s, ok := v.(string); !ok || s != dlxStrategyAtLeastOnce {
			return false, overflow, overflowSet
		}
	}
	return true, overflow, overflowSet
}

// overflowString coerces an x-overflow Args value to its string form, accepting
// both a plain string and the typed OverflowPolicy.
func overflowString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case OverflowPolicy:
		return string(s), true
	default:
		return "", false
	}
}

// warnQuorumAtLeastOnceOverflow emits one warning per quorum queue where warren
// auto-sets x-overflow=reject-publish to satisfy the at-least-once dead-letter
// strategy (T76 / RMQ-05 / decision 52) — i.e. at-least-once applies and the
// caller left overflow unset. The warning surfaces the auto-set and the
// source-queue memory cost: reject-publish blocks publishers when the queue is
// full, and at-least-once retains dead-lettered messages in source-queue memory
// until the DLX consumer acks. A conflicting explicit overflow is rejected by
// validate(), not warned here. A nil logger is a no-op.
func warnQuorumAtLeastOnceOverflow(logger log.Logger, t *Topology) {
	if logger == nil {
		return
	}
	for _, q := range t.Queues {
		atLeastOnce, _, overflowSet := t.quorumAtLeastOnce(q)
		if !atLeastOnce || overflowSet {
			continue
		}
		logger.Warningf("warren: quorum queue %q uses the at-least-once dead-letter strategy, "+
			"which requires x-overflow=reject-publish; warren is setting it automatically. "+
			"reject-publish blocks publishers (ErrPublishNacked) when the queue is full, and "+
			"at-least-once retains dead-lettered messages in source-queue memory until the DLX "+
			"consumer acknowledges them — size the source queue and DLQ accordingly (SPEC §6.6)",
			q.Name)
	}
}

// parseBrokerMajor extracts the leading major-version integer from a broker
// version string such as "4.0.5" or "3.13.2". Returns ok=false when no leading
// digit run is present.
func parseBrokerMajor(version string) (int, bool) {
	i := 0
	for i < len(version) && version[i] >= '0' && version[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	major, err := strconv.Atoi(version[:i])
	if err != nil {
		return 0, false
	}
	return major, true
}

// Auto-DLQ default bounds (T65 / SRE-03 / ST-08 / DP-03). The auto-declared
// <source>.dlq is bounded by default so a poison flood cannot fill disk and trip
// a broker-wide connection.blocked alarm, and so dead-lettered personal data has
// a finite life. Override via the DeadLetter.DLQ* fields, opt out with
// DLQUnbounded, or declare <source>.dlq explicitly for full control.
const (
	defaultDLQMaxLength  = 100_000
	defaultDLQMessageTTL = 7 * 24 * time.Hour
)

// autoDLQArgs builds the x-* argument table for the auto-declared <source>.dlq
// from a DeadLetter, applying the safe defaults unless DLQUnbounded is set.
func autoDLQArgs(dl DeadLetter) map[string]any {
	if dl.DLQUnbounded {
		return nil
	}
	args := make(map[string]any, 3)

	maxLen := int64(dl.DLQMaxLength)
	if maxLen == 0 {
		maxLen = defaultDLQMaxLength
	}
	args["x-max-length"] = maxLen

	ttl := dl.DLQMessageTTL
	if ttl == 0 {
		ttl = defaultDLQMessageTTL
	}
	args["x-message-ttl"] = ttl.Milliseconds()

	overflow := dl.DLQOverflow
	if overflow == "" {
		overflow = OverflowDropHead
	}
	args["x-overflow"] = string(overflow)

	return args
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
		Exchanges:        make([]Exchange, len(t.Exchanges)),
		Queues:           make([]Queue, len(t.Queues)),
		Bindings:         make([]Binding, len(t.Bindings)),
		DeadLetters:      make([]DeadLetter, len(t.DeadLetters)),
		ExchangeBindings: make([]ExchangeBinding, len(t.ExchangeBindings)),
	}
	copy(out.DeadLetters, t.DeadLetters)
	for i, b := range t.Bindings {
		out.Bindings[i] = b
		if b.Args != nil {
			out.Bindings[i].Args = copyArgs(b.Args)
		}
	}
	for i, eb := range t.ExchangeBindings {
		out.ExchangeBindings[i] = eb
		if eb.Args != nil {
			out.ExchangeBindings[i].Args = copyArgs(eb.Args)
		}
	}
	for i, e := range t.Exchanges {
		out.Exchanges[i] = e
		if e.Args != nil {
			out.Exchanges[i].Args = copyArgs(e.Args)
		}
		// AlternateExchange (T68) injects the broker's `alternate-exchange`
		// argument. The field is the single source of truth; validate() rejects a
		// raw Args["alternate-exchange"].
		if e.AlternateExchange != "" {
			if out.Exchanges[i].Args == nil {
				out.Exchanges[i].Args = make(map[string]any, 1)
			}
			out.Exchanges[i].Args["alternate-exchange"] = e.AlternateExchange
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
				Args:    autoDLQArgs(dl),
			})
			queueNames[dlqName] = struct{}{}
		}

		// Bind the DLX to the DLQ so dead-lettered messages actually land in the
		// DLQ rather than routing into limbo. Choose the binding routing key to match
		// how RabbitMQ keys dead-lettered messages:
		//   - dl.RoutingKey set: messages are rewritten to it (x-dead-letter-routing-key
		//     above), so bind that exact key — correct on a direct DLX too, where "#"
		//     is a literal key, not the topic wildcard.
		//   - dl.RoutingKey unset: messages keep their original key, so bind "#". This
		//     is the topic-wildcard catch-all and matches the auto-created topic DLX; a
		//     user-declared direct DLX in this case needs a routing key to route at all.
		// Skip if the caller already declared this exact binding. The dedup index keys
		// on the chosen routing key, so a caller who both sets dl.RoutingKey AND
		// pre-declares a separate "#" binding keeps both: on a topic DLX they coexist
		// idempotently (the queue is delivered once even when both bindings match), and
		// on a direct DLX the stray "#" is simply a dead literal key — neither is a
		// dedup bug.
		bindRoutingKey := "#"
		if dl.RoutingKey != "" {
			bindRoutingKey = dl.RoutingKey
		}
		bk := bindingKey{dlxName, dlqName, bindRoutingKey}
		if _, exists := bindingKeys[bk]; !exists {
			out.Bindings = append(out.Bindings, Binding{
				Exchange:   dlxName,
				Queue:      dlqName,
				RoutingKey: bindRoutingKey,
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
				strategyVal, hasStrategy := q.Args["x-dead-letter-strategy"]
				if !hasStrategy {
					q.Args["x-dead-letter-strategy"] = dlxStrategyAtLeastOnce
				}
				// at-least-once requires x-overflow=reject-publish (decision 52 /
				// RMQ-05). The broker accepts any overflow silently (gate G4) but
				// does not honour at-least-once otherwise, so fill the default when
				// the caller left it unset. A conflicting explicit value was already
				// rejected by validate(). The coupling applies only to the
				// at-least-once strategy — a caller override (e.g. at-most-once)
				// opts out.
				effectiveALO := !hasStrategy
				if s, ok := strategyVal.(string); ok && s == dlxStrategyAtLeastOnce {
					effectiveALO = true
				}
				if effectiveALO {
					if _, hasOverflow := q.Args["x-overflow"]; !hasOverflow {
						q.Args["x-overflow"] = string(OverflowRejectPublish)
					}
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
	for _, eb := range topo.ExchangeBindings {
		if err := ch.ExchangeBind(eb.Destination, eb.RoutingKey, eb.Source, eb.NoWait, amqp091.Table(eb.Args)); err != nil {
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
		// The alternate-exchange arg is set by the library from the typed
		// AlternateExchange field (T68); a raw Args value would be a second source
		// of truth that bypasses the field. RabbitMQ honors both the canonical
		// `alternate-exchange` key and the historical `x-alternate-exchange` alias,
		// so reject both to keep the field the single source of truth.
		for _, aeKey := range []string{"alternate-exchange", "x-alternate-exchange"} {
			if _, raw := e.Args[aeKey]; raw {
				return fmt.Errorf("%w: Exchange %q: set the AlternateExchange field instead of Args[%q]", ErrInvalidOptions, e.Name, aeKey)
			}
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
		// Quorum-queue structural validation (T64 / R10-9). RabbitMQ requires a
		// quorum queue to be Durable and rejects Exclusive / AutoDelete; declaring
		// any of these otherwise closes the channel with PRECONDITION_FAILED at
		// declare time. (x-max-priority on quorum is already rejected above: the
		// raw Args["x-max-priority"] key by the managed-args guard, and the
		// MaxPriority field by the Type=Classic requirement.)
		if q.Type == QueueTypeQuorum {
			if !q.Durable {
				return fmt.Errorf("%w: Queue %q: quorum queues must be Durable", ErrInvalidOptions, q.Name)
			}
			if q.Exclusive {
				return fmt.Errorf("%w: Queue %q: Exclusive is not supported on quorum queues", ErrInvalidOptions, q.Name)
			}
			if q.AutoDelete {
				return fmt.Errorf("%w: Queue %q: AutoDelete is not supported on quorum queues", ErrInvalidOptions, q.Name)
			}
		}
		if q.Type == QueueTypeStream {
			streamQueues[q.Name] = struct{}{}
			if !q.Durable {
				return fmt.Errorf("%w: Queue %q: stream queues must be Durable", ErrInvalidOptions, q.Name)
			}
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

		// at-least-once ⇒ reject-publish coupling (T76 / RMQ-05 / decision 52).
		// A quorum queue running the at-least-once dead-letter strategy must pair
		// with x-overflow=reject-publish. The broker silently accepts any overflow
		// (gate G4 + T76 probe), so reject a conflicting explicit value here;
		// expand() fills reject-publish when overflow is left unset.
		if atLeastOnce, overflow, overflowSet := t.quorumAtLeastOnce(q); atLeastOnce && overflowSet && overflow != string(OverflowRejectPublish) {
			return fmt.Errorf("%w: Queue %q: the at-least-once dead-letter strategy requires x-overflow=%q, "+
				"but x-overflow=%q is set — RabbitMQ accepts the mismatch but does not honour at-least-once "+
				"(set Overflow=OverflowRejectPublish, or override x-dead-letter-strategy to opt out)",
				ErrInvalidOptions, q.Name, OverflowRejectPublish, overflow)
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

	for _, eb := range t.ExchangeBindings {
		if eb.Source == "" {
			return fmt.Errorf("%w: ExchangeBinding.Source must not be empty", ErrInvalidOptions)
		}
		if eb.Destination == "" {
			return fmt.Errorf("%w: ExchangeBinding.Destination must not be empty", ErrInvalidOptions)
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
		// DLQOverflow is written straight into the auto-DLQ's x-overflow by
		// autoDLQArgs; an unknown value would otherwise surface only as a
		// broker PRECONDITION_FAILED at declare time. Validate it up front too.
		if _, ok := validOverflow[dl.DLQOverflow]; !ok {
			return fmt.Errorf("%w: DeadLetter.Source %q: unknown DLQOverflow policy %q", ErrInvalidOptions, dl.Source, dl.DLQOverflow)
		}
	}

	return nil
}
