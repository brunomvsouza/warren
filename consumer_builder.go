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

	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

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

// AutoAck opts into broker-side auto-acknowledgement. Use with caution: messages
// are considered delivered as soon as the broker sends them and cannot be nacked.
// NOTE: full implementation is in T35; the flag is stored here.
func (b *ConsumerBuilder[M]) AutoAck() *ConsumerBuilder[M] { return b }

// Args sets extra queue-consume arguments forwarded in basic.consume.
// NOTE: full implementation is in T36.
func (b *ConsumerBuilder[M]) Args(_ Headers) *ConsumerBuilder[M] { return b }

// OnCancel registers a callback invoked when the broker cancels the consumer
// via basic.cancel (e.g. the queue was deleted or the exclusive lock revoked).
// NOTE: full implementation is in T36.
func (b *ConsumerBuilder[M]) OnCancel(_ func(reason string)) *ConsumerBuilder[M] { return b }

// MaxRedeliveries caps the number of times a message can be redelivered before
// it is dead-lettered. NOTE: full implementation is in T20.
func (b *ConsumerBuilder[M]) MaxRedeliveries(_ int) *ConsumerBuilder[M] { return b }

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
