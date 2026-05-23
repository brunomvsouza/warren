package warren

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// Handler is the function signature for typed message handlers.
// Return nil to ack, ErrRequeue to nack with requeue, or any other error to nack without requeue.
type Handler[M any] func(ctx context.Context, msg M) error

// RawHandler is the function signature for handlers that need full delivery access.
// The Delivery carries the decoded body plus all AMQP envelope fields.
type RawHandler[M any] func(ctx context.Context, d *Delivery[M]) error

// Consumer receives AMQP messages from a single queue, decodes each payload
// to M via the configured codec, and dispatches to a Handler[M] or RawHandler[M].
//
// Use ConsumerFor[M](conn) to build a consumer.
type Consumer[M any] struct {
	conn  *Connection
	queue string
	tag   string

	concurrency    uint
	prefetch       uint16
	channelQoS     bool
	priority       int
	prioritySet    bool
	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	codec  codec.Codec
	cm     metrics.ConsumerMetrics
	tracer otel.Tracer

	// mc is the consumer-role managed connection this consumer is pinned to.
	mc *managedConn

	// deliveryCh is a test-injection hook: when non-nil, openDeliveryCh returns it
	// directly without opening a real amqp091 channel.  Tests set this field before
	// calling Consume / ConsumeRaw.
	deliveryCh chan amqp091.Delivery

	// closedCh is closed when Close is called; signals Delivery.Ack/Nack to refuse.
	closedCh  chan struct{}
	closeOnce sync.Once
}

func newConsumer[M any](b *ConsumerBuilder[M], tag string) *Consumer[M] {
	numConns := b.conn.NumConConns()
	idx := connIndexForTag(tag, numConns)
	mc := b.conn.ConConnAt(idx)

	return &Consumer[M]{
		conn:           b.conn,
		queue:          b.queue,
		tag:            tag,
		concurrency:    b.concurrency,
		prefetch:       b.prefetch,
		channelQoS:     b.channelQoS,
		priority:       b.priority,
		prioritySet:    b.prioritySet,
		handlerTimeout: b.handlerTimeout,
		timeoutVerdict: b.timeoutVerdict,
		codec:          b.c,
		cm:             b.cm,
		tracer:         b.tracer,
		mc:             mc,
		closedCh:       make(chan struct{}),
	}
}

// connIndexForTag returns a stable index in [0, n) for the given consumer tag (FNV-1a).
func connIndexForTag(tag string, n int) int {
	if n <= 1 {
		return 0
	}
	var h uint32 = 2166136261
	for i := range len(tag) {
		h ^= uint32(tag[i])
		h *= 16777619
	}
	return int(h) % n //nolint:gosec // G115: n is bounded by WithConsumerConnections
}

// Consume starts consuming from the configured queue, decoding each message
// and dispatching to h. It blocks until ctx is cancelled or the delivery
// channel closes. Each delivery is processed in its own goroutine (up to
// Concurrency goroutines at a time).
func (c *Consumer[M]) Consume(ctx context.Context, h Handler[M]) error {
	return c.ConsumeRaw(ctx, func(innerCtx context.Context, d *Delivery[M]) error {
		return h(innerCtx, *d.Body())
	})
}

// ConsumeRaw starts consuming, passing the full Delivery envelope to h.
func (c *Consumer[M]) ConsumeRaw(ctx context.Context, h RawHandler[M]) error {
	// resubCh carries replacement delivery channels produced by the reconnect hook.
	resubCh := make(chan chan amqp091.Delivery, 1)

	c.mc.registerHook(func(hookCtx context.Context) error {
		jitter := time.Duration(50+rand.IntN(201)) * time.Millisecond //nolint:gosec // non-crypto jitter
		select {
		case <-hookCtx.Done():
			return hookCtx.Err()
		case <-time.After(jitter):
		}
		newCh, err := c.openDeliveryCh(hookCtx)
		if err != nil {
			return err
		}
		select {
		case resubCh <- newCh:
			c.cm.RecordResubscribed(c.queue)
		case <-hookCtx.Done():
			return hookCtx.Err()
		}
		return nil
	})

	cur, err := c.openDeliveryCh(ctx)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil

		case newCh := <-resubCh:
			cur = newCh

		case d, ok := <-cur:
			if !ok {
				// broker closed the channel; wait for re-subscribe or ctx cancel
				select {
				case <-ctx.Done():
					wg.Wait()
					return nil
				case cur = <-resubCh:
				}
				continue
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(raw amqp091.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				c.dispatch(ctx, raw, h)
			}(d)
		}
	}
}

// openDeliveryCh returns the delivery channel for this consumer.  In unit tests
// c.deliveryCh is pre-set; in production it opens a real amqp091 channel.
func (c *Consumer[M]) openDeliveryCh(ctx context.Context) (chan amqp091.Delivery, error) {
	if c.deliveryCh != nil {
		return c.deliveryCh, nil
	}

	topoCh, err := c.mc.openChannel()
	if err != nil {
		return nil, fmt.Errorf("warren: consumer open channel: %w", err)
	}

	ch, ok := topoCh.(*amqp091.Channel)
	if !ok {
		_ = topoCh.Close()
		return nil, fmt.Errorf("warren: consumer: unexpected channel type %T", topoCh)
	}

	// global=false means per-channel QoS in amqp091 semantics (confusingly inverted).
	// ChannelQoS() flag → global=false (per-channel).  Default (no ChannelQoS) → global=true (per-consumer, which RabbitMQ maps to per-channel anyway).
	global := !c.channelQoS
	if err := ch.Qos(int(c.prefetch), 0, global); err != nil { //nolint:gosec // G115: prefetch is uint16 by protocol
		_ = ch.Close()
		return nil, fmt.Errorf("warren: consumer Qos: %w", wrapAMQPError(err))
	}

	args := amqp091.Table{}
	if c.prioritySet {
		args["x-priority"] = c.priority
	}

	deliveries, err := ch.Consume(c.queue, c.tag, false, false, false, false, args)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("warren: consumer subscribe: %w", wrapAMQPError(err))
	}

	out := make(chan amqp091.Delivery, int(c.prefetch)) //nolint:gosec // G115: prefetch bounded
	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	go func() {
		defer close(out)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				select {
				case out <- d:
				case <-ctx.Done():
					return
				}
			case <-closeCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// dispatch decodes and handles a single delivery.
func (c *Consumer[M]) dispatch(ctx context.Context, raw amqp091.Delivery, h RawHandler[M]) {
	var body M
	if err := safeDecodeConsumer(c.codec, raw.Body, &body); err != nil {
		c.cm.RecordHandler(c.queue, "decode_error", 0)
		_ = raw.Nack(false, false)
		return
	}

	d := newDelivery[M](&body, c.queue, raw, c.closedCh)

	hCtx := ctx
	var cancel context.CancelFunc
	if c.handlerTimeout > 0 {
		hCtx, cancel = context.WithTimeout(ctx, c.handlerTimeout)
		defer cancel()
	}

	start := time.Now()
	handlerDone := make(chan error, 1)
	go func() { handlerDone <- h(hCtx, d) }()

	select {
	case handlerErr := <-handlerDone:
		elapsed := time.Since(start)
		c.cm.RecordHandler(c.queue, handlerOutcome(handlerErr), elapsed)
		_ = d.AckIf(handlerErr)

	case <-hCtx.Done():
		if c.handlerTimeout > 0 && errors.Is(hCtx.Err(), context.DeadlineExceeded) {
			c.cm.RecordHandlerTimeout(c.queue)
			elapsed := time.Since(start)
			switch c.timeoutVerdict {
			case TimeoutNackRequeue:
				c.cm.RecordHandler(c.queue, "timeout_nack_requeue", elapsed)
				_ = raw.Nack(false, true)
			default:
				c.cm.RecordHandler(c.queue, "timeout_nack_no_requeue", elapsed)
				_ = raw.Nack(false, false)
			}
		}
		<-handlerDone
	}
}

func handlerOutcome(err error) string {
	if err == nil {
		return "ack"
	}
	if errors.Is(err, ErrRequeue) {
		return "nack_requeue"
	}
	return "nack_no_requeue"
}

// safeDecodeConsumer decodes payload, recovering from codec panics per T09 contract.
func safeDecodeConsumer(c codec.Codec, payload []byte, out any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: codec panic: %v", ErrInvalidMessage, r)
		}
	}()
	return c.Decode(payload, out)
}

// mapHandlerResult applies handler error mapping; used by unit tests.
func mapHandlerResult(d interface {
	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
}, err error) {
	if err == nil {
		_ = d.Ack(false)
		return
	}
	_ = d.Nack(false, errors.Is(err, ErrRequeue))
}

// Health reports whether the consumer's pinned connection is healthy.
func (c *Consumer[M]) Health(ctx context.Context) error {
	return c.mc.health(ctx)
}

// Close signals the consumer to stop accepting new deliveries.
func (c *Consumer[M]) Close(_ context.Context) error {
	c.closeOnce.Do(func() { close(c.closedCh) })
	return nil
}
