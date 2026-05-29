package warren

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// BatchConsumerBuilder configures and builds a BatchConsumer[M].
//
// All option methods follow a last-wins policy: calling the same method twice
// keeps only the final value.
type BatchConsumerBuilder[M any] struct {
	conn  *Connection
	queue string
	tag   string

	size       uint
	flushAfter time.Duration

	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	prefetch    uint16
	channelQoS  bool
	priority    int
	prioritySet bool

	maxRedeliveries  int
	counterBDisabled bool

	c      codec.Codec
	cm     metrics.ConsumerMetrics
	tracer otel.Tracer
}

// BatchConsumerFor returns a builder for a BatchConsumer[M] tied to conn.
func BatchConsumerFor[M any](conn *Connection) *BatchConsumerBuilder[M] {
	return &BatchConsumerBuilder[M]{conn: conn}
}

// Queue sets the AMQP queue name to consume from.
func (b *BatchConsumerBuilder[M]) Queue(name string) *BatchConsumerBuilder[M] {
	b.queue = name
	return b
}

// Tag sets the consumer tag. Default: auto-generated "ctag-<uuidv7>" at Build time.
func (b *BatchConsumerBuilder[M]) Tag(consumerTag string) *BatchConsumerBuilder[M] {
	b.tag = consumerTag
	return b
}

// Size sets the maximum number of messages accumulated before a batch is flushed.
// Default: 100. A flush also fires if FlushAfter elapses before Size is reached.
func (b *BatchConsumerBuilder[M]) Size(n uint) *BatchConsumerBuilder[M] {
	b.size = n
	return b
}

// FlushAfter sets a time-based flush trigger. When the first message of a new batch
// arrives the timer starts; when it fires the batch is dispatched even if fewer than
// Size messages have accumulated. Default: 0 (no timer-based flush).
func (b *BatchConsumerBuilder[M]) FlushAfter(d time.Duration) *BatchConsumerBuilder[M] {
	b.flushAfter = d
	return b
}

// HandlerTimeout sets a per-batch ctx deadline. Zero (default) means no deadline.
// When the deadline expires the handler ctx is cancelled and the batch verdict is
// determined by HandlerTimeoutVerdict (default: TimeoutNackNoRequeue).
func (b *BatchConsumerBuilder[M]) HandlerTimeout(d time.Duration) *BatchConsumerBuilder[M] {
	b.handlerTimeout = d
	return b
}

// HandlerTimeoutVerdict sets the ack/nack action when HandlerTimeout fires.
// Default: TimeoutNackNoRequeue (message goes to DLX or is dropped).
func (b *BatchConsumerBuilder[M]) HandlerTimeoutVerdict(v TimeoutVerdict) *BatchConsumerBuilder[M] {
	b.timeoutVerdict = v
	return b
}

// Prefetch sets the per-channel prefetch count (basic.qos count). Default: 64.
func (b *BatchConsumerBuilder[M]) Prefetch(count uint16) *BatchConsumerBuilder[M] {
	b.prefetch = count
	return b
}

// PrefetchBytes is a no-op on RabbitMQ; preserved for AMQP 0-9-1 protocol parity.
func (b *BatchConsumerBuilder[M]) PrefetchBytes(_ uint) *BatchConsumerBuilder[M] { return b }

// ChannelQoS applies QoS per channel (global=false) rather than per consumer.
func (b *BatchConsumerBuilder[M]) ChannelQoS() *BatchConsumerBuilder[M] {
	b.channelQoS = true
	return b
}

// Priority sets the x-priority consumer argument.
func (b *BatchConsumerBuilder[M]) Priority(p int) *BatchConsumerBuilder[M] {
	b.priority = p
	b.prioritySet = true
	return b
}

// MaxRedeliveries caps the number of times a message can be redelivered.
// Default 0 = unbounded. Counter B increments per message when the whole
// batch verdict is Nack(requeue=true).
func (b *BatchConsumerBuilder[M]) MaxRedeliveries(n int) *BatchConsumerBuilder[M] {
	b.maxRedeliveries = n
	return b
}

// TopologyHint provides queue metadata that modifies counter B behaviour.
func (b *BatchConsumerBuilder[M]) TopologyHint(q Queue) *BatchConsumerBuilder[M] {
	if q.Type == QueueTypeQuorum && q.DeliveryLimit > 0 {
		b.counterBDisabled = true
	} else {
		b.counterBDisabled = false
	}
	return b
}

// Codec sets the message codec. Default: JSON (lax).
func (b *BatchConsumerBuilder[M]) Codec(c codec.Codec) *BatchConsumerBuilder[M] {
	b.c = c
	return b
}

// Metrics sets the ConsumerMetrics recorder. Default: NoOp.
func (b *BatchConsumerBuilder[M]) Metrics(cm metrics.ConsumerMetrics) *BatchConsumerBuilder[M] {
	b.cm = cm
	return b
}

// WithoutMetrics disables all consumer metrics (last-wins against Metrics).
func (b *BatchConsumerBuilder[M]) WithoutMetrics() *BatchConsumerBuilder[M] {
	b.cm = metrics.NoOpConsumerMetrics{}
	return b
}

// Tracer sets the OTel tracer for consume spans.
func (b *BatchConsumerBuilder[M]) Tracer(t otel.Tracer) *BatchConsumerBuilder[M] {
	b.tracer = t
	return b
}

// Build constructs and returns a BatchConsumer[M]. Returns an error if
// the builder state is invalid.
func (b *BatchConsumerBuilder[M]) Build() (*BatchConsumer[M], error) {
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	if b.queue == "" {
		return nil, fmt.Errorf("%w: queue must not be empty", ErrInvalidOptions)
	}

	cfg := *b
	cfg.applyDefaults()

	if cfg.size > 65535 {
		return nil, fmt.Errorf("%w: size (%d) cannot exceed 65535 due to AMQP prefetch count limits", ErrInvalidOptions, cfg.size)
	}

	if uint(cfg.prefetch) < cfg.size {
		return nil, fmt.Errorf("%w: prefetch count (%d) must be greater than or equal to size (%d) to avoid deadlocks", ErrInvalidOptions, cfg.prefetch, cfg.size)
	}

	tag := b.tag
	if tag == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("warren: failed to generate consumer tag: %w", err)
		}
		tag = "ctag-" + id.String()
	}

	numConns := b.conn.NumConConns()
	idx := connIndexForTag(tag, numConns)
	mc := b.conn.ConConnAt(idx)

	bc := &BatchConsumer[M]{
		queue:            cfg.queue,
		tag:              tag,
		size:             cfg.size,
		flushAfter:       cfg.flushAfter,
		handlerTimeout:   cfg.handlerTimeout,
		timeoutVerdict:   cfg.timeoutVerdict,
		prefetch:         cfg.prefetch,
		channelQoS:       cfg.channelQoS,
		priority:         cfg.priority,
		prioritySet:      cfg.prioritySet,
		maxRedeliveries:  cfg.maxRedeliveries,
		counterBDisabled: cfg.counterBDisabled,
		codec:            cfg.c,
		cm:               cfg.cm,
		tracer:           cfg.tracer,
		propagator:       otel.NewPropagator(),
		msgType:          metricsTypeName[M](),
		mc:               mc,
		closedCh:         make(chan struct{}),
	}
	bc.counterState.Store(&redeliveryCounter{})
	return bc, nil
}

func (b *BatchConsumerBuilder[M]) applyDefaults() {
	if b.size == 0 {
		b.size = 100
	}
	if b.prefetch == 0 {
		if b.size > 64 {
			b.prefetch = uint16(b.size)
		} else {
			b.prefetch = 64
		}
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
