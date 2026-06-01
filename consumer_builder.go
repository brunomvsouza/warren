package warren

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// ConsumerBuilder configures and builds a Consumer[M].
//
// All option methods follow a last-wins policy: calling the same method twice
// keeps only the final value.
type ConsumerBuilder[M any] struct {
	conn  *Connection
	queue string

	tag              string
	concurrency      uint
	prefetch         uint16
	maxInFlightBytes int64
	channelQoS       bool
	priority         int
	prioritySet      bool
	autoAck          bool
	exclusive        bool

	// consumeArgs are extra basic.consume arguments (Args). x-priority is layered
	// on top at subscribe time when Priority is set, so the typed option wins.
	consumeArgs Headers
	// onCancel fires when the broker sends basic.cancel (queue deleted, exclusive
	// lock revoked). nil means "log a warning instead".
	onCancel func(reason string)

	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	// depthSampleInterval is the WithQueueDepthSampler polling period (T52).
	// 0 (the default) disables the sampler.
	depthSampleInterval time.Duration

	maxRedeliveries  int
	counterBDisabled bool // true when quorum queue with DeliveryLimit > 0

	// dedupeStore + dedupeTTL back the WithDedupe middleware (T55). nil store
	// (the default) disables it.
	dedupeStore DedupeStore
	dedupeTTL   time.Duration

	c      codec.Codec
	cm     metrics.ConsumerMetrics
	tracer otel.Tracer
}

// ConsumerFor returns a builder for a Consumer[M] tied to conn.
func ConsumerFor[M any](conn *Connection) *ConsumerBuilder[M] {
	return &ConsumerBuilder[M]{conn: conn}
}

// Queue sets the AMQP queue name to consume from.
func (b *ConsumerBuilder[M]) Queue(name string) *ConsumerBuilder[M] {
	b.queue = name
	return b
}

// Tag sets the consumer tag. Default: auto-generated "ctag-<uuidv7>" at Build time.
func (b *ConsumerBuilder[M]) Tag(consumerTag string) *ConsumerBuilder[M] {
	b.tag = consumerTag
	return b
}

// Concurrency sets the number of handler goroutines run in parallel. Default: 1.
func (b *ConsumerBuilder[M]) Concurrency(n uint) *ConsumerBuilder[M] {
	b.concurrency = n
	return b
}

// Prefetch sets the per-channel prefetch count (basic.qos count). Default: 64.
func (b *ConsumerBuilder[M]) Prefetch(count uint16) *ConsumerBuilder[M] {
	b.prefetch = count
	return b
}

// PrefetchBytes is a no-op on RabbitMQ; preserved for AMQP 0-9-1 protocol parity.
func (b *ConsumerBuilder[M]) PrefetchBytes(_ uint) *ConsumerBuilder[M] { return b }

// MaxInFlightBytes caps the sum of in-flight message body sizes (the local
// memory guardrail, T50). Once concurrently-dispatched handlers hold n bytes of
// payload, the consumer stops pulling new deliveries — pausing prefetch refill —
// until a handler returns and frees its bytes. This bounds heap use where
// prefetch × concurrency × body-size would otherwise risk an OOM, independent of
// the message-count backpressure that Prefetch provides.
//
// n <= 0 (the default) disables the guardrail. n is a soft ceiling, not a hard
// reject: a single message larger than n is dispatched alone when nothing else is
// in flight (rather than deadlocking forever), so peak resident payload memory can
// briefly reach max(n, largest single body) — size n with that headroom in mind.
// The current reserved total is exported as the consumer_inflight_bytes{queue} gauge.
func (b *ConsumerBuilder[M]) MaxInFlightBytes(n int64) *ConsumerBuilder[M] {
	b.maxInFlightBytes = n
	return b
}

// WithQueueDepthSampler enables a background goroutine that periodically reads the
// broker-side message backlog for the consumer's source queue, and for its
// conventional "<queue>.dlq" dead-letter queue, via a passive queue declare —
// exporting the native queue_depth{queue} and dlq_depth{dlq} gauges. These are the
// leading "work is piling up" / "poison is accumulating" signals that the
// per-message handler metrics cannot show.
//
// interval is the polling period; interval <= 0 (the default) disables the sampler.
// Each sample runs on its own short-lived channel per probe, so a passive declare on
// a missing queue — which the broker answers by closing the channel — never disturbs
// the delivery channel. The dlq_depth gauge is emitted only when "<queue>.dlq"
// actually exists (a 404 is skipped, not reported as zero); the source queue_depth is
// likewise skipped if the queue itself is gone. Sampling stops when the Consume /
// ConsumeRaw context is cancelled.
//
// Size interval against broker load: each tick costs one or two lightweight
// queue.declare-passive round-trips on the consumer's connection. A few seconds is
// typical; sub-second polling on a large cluster adds avoidable broker load, so an
// interval below 100ms is clamped to a 100ms floor with a one-time warning. While a
// whole sample fails to reach the broker — the source queue is gone, or the socket is
// down mid-reconnect — the sampler backs off exponentially (capped at 30s, never below
// interval) and returns to interval on the first sample that emits a gauge, so a
// permanently-missing queue does not probe at full rate forever. The DLQ name follows
// warren's own DeadLetter convention ("<queue>.dlq"); a DLQ under a different name is
// not sampled.
//
// The gauges hold their last sampled value while the consumer runs: a sample that
// cannot reach the broker (e.g. mid-reconnect) is skipped rather than zeroed. When the
// consumer stops, both series are removed from the registry, so a long-lived process
// that cycles consumers over distinct queue names does not accumulate stale frozen
// series. Alert on rate/derivative or pair with consumer liveness rather than reading a
// single point as "current".
func (b *ConsumerBuilder[M]) WithQueueDepthSampler(interval time.Duration) *ConsumerBuilder[M] {
	b.depthSampleInterval = interval
	return b
}

// WithDedupe enables native consumer-side deduplication keyed by MessageID,
// backed by store (T55). It abstracts the manual dedupe pattern (SPEC §6.2.1)
// off the handler: before each delivery, the middleware asks store.Seen(id) and,
// on a hit, acks the message WITHOUT invoking the handler; after the handler
// returns nil (success), it calls store.Mark(id, ttl) so future redeliveries of
// the same MessageID are recognised. A handler error is never marked, so the
// message is reprocessed on redelivery.
//
// Failure mode is fail-OPEN: if store.Seen or store.Mark returns an error, the
// middleware logs a warning and processes the message anyway, trading a possible
// duplicate for availability — consistent with the at-least-once contract. The
// store must therefore be treated as a best-effort cache, not a correctness gate;
// handlers with non-idempotent side-effects should still guard themselves.
//
// Deliveries without a MessageID cannot be deduped and are passed straight to the
// handler. ttl is the retention window passed to store.Mark; size it to cover the
// maximum plausible duplicate gap (broker outage + reconnect + retry budget — 15
// minutes suits most workloads, SPEC §6.2.1). Only the Consume path is wrapped;
// ConsumeRaw handlers manage their own acks and are unaffected. store==nil (the
// default) disables the middleware.
//
// Seen runs before the handler under the per-delivery handler context, so a
// configured HandlerTimeout bounds it. Mark runs after a successful handler under
// a context DETACHED from that deadline (handler trace/span values are preserved,
// but cancellation and the handler deadline are replaced with a fixed grace bound),
// so a near-exhausted HandlerTimeout — or a shutdown that already cancelled the
// handler context — cannot silently skip recording the id and fail open to a future
// duplicate. The grace bound still caps Mark so a wedged store cannot block consumer
// shutdown. Keep store calls fast regardless of which side of the handler they run on.
func (b *ConsumerBuilder[M]) WithDedupe(store DedupeStore, ttl time.Duration) *ConsumerBuilder[M] {
	b.dedupeStore = store
	b.dedupeTTL = ttl
	return b
}

// ChannelQoS applies QoS at channel scope (basic.qos global=true) rather than
// per consumer. This is the RabbitMQ-recommended setting; the broker ignores the
// per-consumer distinction and applies prefetch at channel scope in any case.
func (b *ConsumerBuilder[M]) ChannelQoS() *ConsumerBuilder[M] {
	b.channelQoS = true
	return b
}

// Priority sets the x-priority consumer argument. Higher values are preferred
// when multiple consumers are attached to the same queue (active/standby topology).
func (b *ConsumerBuilder[M]) Priority(p int) *ConsumerBuilder[M] {
	b.priority = p
	b.prioritySet = true
	return b
}

// HandlerTimeout sets a per-message ctx deadline. Zero (default) means no deadline.
// When the deadline expires the handler ctx is cancelled and HandlerTimeoutVerdict
// decides whether to nack with or without requeue.
func (b *ConsumerBuilder[M]) HandlerTimeout(d time.Duration) *ConsumerBuilder[M] {
	b.handlerTimeout = d
	return b
}

// HandlerTimeoutVerdict sets the ack/nack action when HandlerTimeout fires.
// Default: TimeoutNackNoRequeue (message goes to DLX or is dropped).
func (b *ConsumerBuilder[M]) HandlerTimeoutVerdict(v TimeoutVerdict) *ConsumerBuilder[M] {
	b.timeoutVerdict = v
	return b
}

// Codec sets the message codec. Default: JSON (strict).
func (b *ConsumerBuilder[M]) Codec(c codec.Codec) *ConsumerBuilder[M] {
	b.c = c
	return b
}

// Metrics sets the ConsumerMetrics recorder. Default: NoOp.
func (b *ConsumerBuilder[M]) Metrics(cm metrics.ConsumerMetrics) *ConsumerBuilder[M] {
	b.cm = cm
	return b
}

// WithoutMetrics disables all consumer metrics (last-wins against Metrics).
func (b *ConsumerBuilder[M]) WithoutMetrics() *ConsumerBuilder[M] {
	b.cm = metrics.NoOpConsumerMetrics{}
	return b
}

// Tracer sets the OTel tracer for consume spans.
func (b *ConsumerBuilder[M]) Tracer(t otel.Tracer) *ConsumerBuilder[M] {
	b.tracer = t
	return b
}

// Exclusive requests exclusive consumer access to the queue (the basic.consume
// exclusive flag). While an exclusive consumer is attached, the broker refuses any
// other consumer on the same queue with ACCESS_REFUSED (surfaced as ErrAccessRefused).
// Use this for active/standby topologies where exactly one worker must hold the queue.
func (b *ConsumerBuilder[M]) Exclusive() *ConsumerBuilder[M] {
	b.exclusive = true
	return b
}

// AutoAck enables the AMQP no-ack flag on basic.consume, which tells the broker
// to consider every delivery already acknowledged before the client sees it.
// This is a real AMQP feature, exposed for protocol fidelity, but it changes
// critical semantics:
//
//   - Handler error semantics are bypassed. nil/error/ErrRequeue/ErrPoison
//     returns all become no-ops. A handler that panics or errors silently drops
//     the message.
//   - No redelivery on consumer crash. If the consumer dies mid-handle, the
//     broker has already removed the message; it will not be redelivered to
//     another consumer. Use only when at-most-once delivery is acceptable.
//   - No backpressure via prefetch. With AutoAck, prefetch loses its ack-gating
//     effect. The broker streams as fast as the channel will carry, and slow
//     handlers can OOM the consumer.
//   - DLX / MaxRedeliveries do not engage. Both depend on Nacks the client never
//     sends.
//
// Use AutoAck only for genuinely fire-and-forget streams (e.g., high-volume
// telemetry where occasional drops are acceptable). For everything else, leave
// it off and let the error-driven semantics work.
func (b *ConsumerBuilder[M]) AutoAck() *ConsumerBuilder[M] {
	b.autoAck = true
	return b
}

// Args sets extra arguments forwarded in the basic.consume frame (the consumer
// argument table, e.g. broker-specific consumer options). When Priority is also
// set, the typed x-priority value is layered on top, so Priority wins over any
// x-priority slipped through Args.
func (b *ConsumerBuilder[M]) Args(args Headers) *ConsumerBuilder[M] {
	// Copy at call time so a later mutation of the caller's map cannot change the
	// builder's recorded args. buildConsumeArgs copies again at subscribe time; this
	// gives the builder strict value semantics across the configuration window. A nil
	// map stays nil (clears any prior Args, last-wins).
	if args == nil {
		b.consumeArgs = nil
	} else {
		b.consumeArgs = maps.Clone(args)
	}
	return b
}

// OnCancel registers a callback invoked when the broker cancels the consumer via
// basic.cancel (e.g. the queue was deleted or an exclusive lock was revoked). The
// reason is the cancelled consumer's tag — the only datum the AMQP basic.cancel
// frame carries; it is not a free-form description. After OnCancel fires, Consume
// returns ErrConsumerCancelled (the library does not auto-redeclare the queue).
// When OnCancel is unset, the library logs a warning instead. Always wire OnCancel
// in production code: a silently dying consumer is worse than a leaked deletion.
func (b *ConsumerBuilder[M]) OnCancel(fn func(reason string)) *ConsumerBuilder[M] {
	b.onCancel = fn
	return b
}

// MaxRedeliveries caps the number of times a message can be redelivered before
// it is dead-lettered. Default 0 = unbounded.
//
// Two complementary counters enforce the ceiling:
//
//   - Counter A (cross-process): reads x-death headers; bounds DLX-bounce loops
//     that survive consumer restarts. Fires BEFORE the handler is called.
//     With MaxRedeliveries(n), counter A short-circuits when death_count >= n,
//     so the handler is invoked for death_count = 0, 1, …, n-1 (exactly n times).
//
//   - Counter B (in-process, process-local): counts consecutive ErrRequeue returns
//     for the same MessageID on the current channel. Resets on channel close.
//     Fires AFTER the handler returns ErrRequeue for the (n+1)-th time, rewriting
//     the verdict to Nack(false). The handler is therefore called n+1 times before
//     counter B dead-letters the message (one more than counter A, because the
//     final ErrRequeue return triggers the rewrite after the call).
//
// Example with MaxRedeliveries(3):
//
//	Counter A: handler called for death_count=0,1,2; short-circuit on death_count=3.
//	Counter B: handler called 4 times (3 ErrRequeue stored, fires on 4th return).
//
// When the source queue is a quorum queue with DeliveryLimit > 0 (declared via
// TopologyHint), counter B is auto-disabled: the broker is authoritative.
// Counter A still runs as a safety net. See TopologyHint.
func (b *ConsumerBuilder[M]) MaxRedeliveries(n int) *ConsumerBuilder[M] {
	b.maxRedeliveries = n
	return b
}

// TopologyHint provides queue metadata that modifies counter B behaviour.
// Currently used to detect quorum queues with DeliveryLimit > 0, which disable
// the in-process counter B (broker handles redelivery bounding via x-delivery-limit).
//
// Call TopologyHint after MaxRedeliveries so the carve-out takes effect.
func (b *ConsumerBuilder[M]) TopologyHint(q Queue) *ConsumerBuilder[M] {
	if q.Type == QueueTypeQuorum && q.DeliveryLimit > 0 {
		b.counterBDisabled = true
	} else {
		b.counterBDisabled = false
	}
	return b
}

// Build constructs and returns a Consumer[M]. Returns an error if
// the builder state is invalid.
func (b *ConsumerBuilder[M]) Build() (*Consumer[M], error) {
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	if b.queue == "" {
		return nil, fmt.Errorf("%w: queue must not be empty", ErrInvalidOptions)
	}
	// Work on a copy so repeated Build() calls see the original builder state.
	cfg := *b
	cfg.applyDefaults()

	tag := b.tag
	if tag == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("warren: failed to generate consumer tag: %w", err)
		}
		tag = "ctag-" + id.String()
	}

	if cfg.prefetch < uint16(cfg.concurrency) { //nolint:gosec // G115: concurrency bounded by uint
		b.conn.opts.logger.Warningf(
			"warren: consumer prefetch=%d is below concurrency=%d; handlers will stall waiting for deliveries",
			cfg.prefetch, cfg.concurrency,
		)
	}

	return newConsumer[M](&cfg, tag), nil
}

func (b *ConsumerBuilder[M]) applyDefaults() {
	if b.concurrency == 0 {
		b.concurrency = 1
	}
	if b.prefetch == 0 {
		b.prefetch = 64
	}
	if b.c == nil {
		b.c = codec.NewJSON()
	}
	if b.cm == nil {
		b.cm = metrics.NoOpConsumerMetrics{}
	}
	if b.tracer == nil {
		b.tracer = otel.NoOpTracer{}
	}
}
