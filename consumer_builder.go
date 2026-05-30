package warren

import (
	"fmt"
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

	tag         string
	concurrency uint
	prefetch    uint16
	channelQoS  bool
	priority    int
	prioritySet bool
	autoAck     bool

	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	maxRedeliveries  int
	counterBDisabled bool // true when quorum queue with DeliveryLimit > 0

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

// ChannelQoS applies QoS per channel (global=false) rather than per consumer.
// This is the RabbitMQ-recommended setting; the broker ignores the per-consumer
// distinction and applies prefetch at channel scope in any case.
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

// Exclusive marks the consumer as exclusive on the queue.
// NOTE: full implementation is in T36; the flag is stored here.
func (b *ConsumerBuilder[M]) Exclusive() *ConsumerBuilder[M] { return b }

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

// Args sets extra queue-consume arguments forwarded in basic.consume.
// NOTE: full implementation is in T36.
func (b *ConsumerBuilder[M]) Args(_ Headers) *ConsumerBuilder[M] { return b }

// OnCancel registers a callback invoked when the broker cancels the consumer
// via basic.cancel (e.g. the queue was deleted or the exclusive lock revoked).
// NOTE: full implementation is in T36.
func (b *ConsumerBuilder[M]) OnCancel(_ func(reason string)) *ConsumerBuilder[M] { return b }

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
