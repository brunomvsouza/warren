package warren

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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

// deliverySub pairs a delivery channel with a signal that closes when the
// underlying AMQP channel physically closes (not when the consumer ctx is cancelled).
// dispatch goroutines watch done to cancel in-flight handler contexts.
type deliverySub struct {
	ch   chan amqp091.Delivery
	done <-chan struct{} // nil when channel-close detection is not needed
}

// redeliveryCounter holds per-channel in-process redelivery state (counter B).
// A new instance is created on every channel open so that channel close automatically
// "drops" the old counts — delivery tags from a previous channel cannot bleed over.
//
// Key families (namespaced to prevent collision):
//   - "mid:<MessageID>"             when MessageID is present (stable across redeliveries)
//   - "dlv:<consumerTag>:<tag>"     fallback when MessageID is absent (delivery-tag-based, not stable)
//
// No chanID prefix is needed: each redeliveryCounter instance owns its own sync.Map, so keys are
// implicitly scoped to the channel that created this instance.
type redeliveryCounter struct {
	m sync.Map // key: "mid:<MessageID>" or "dlv:<tag>:<deliveryTag>", value: int64
}

// Consumer receives AMQP messages from a single queue, decodes each payload
// to M via the configured codec, and dispatches to a Handler[M] or RawHandler[M].
//
// Use ConsumerFor[M](conn) to build a consumer. Each consumer may only be
// started once; create a new consumer via Build() to restart.
type Consumer[M any] struct {
	queue string
	tag   string

	concurrency    uint
	prefetch       uint16
	channelQoS     bool
	priority       int
	prioritySet    bool
	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	// MaxRedeliveries enforcement.
	// maxRedeliveries == 0 means unbounded (feature disabled).
	maxRedeliveries  int
	counterBDisabled bool // true for quorum queues with broker-enforced DeliveryLimit
	// counterState holds the per-channel in-process counter B map.
	// Replaced atomically on every channel open so "channel close resets counter B".
	counterState atomic.Pointer[redeliveryCounter]

	codec  codec.Codec
	cm     metrics.ConsumerMetrics
	tracer otel.Tracer

	// mc is the consumer-role managed connection this consumer is pinned to.
	mc *managedConn

	// deliveryCh is a basic test-injection hook: when non-nil, openDeliveryCh
	// returns it with done=nil (channel-close detection is not exercised).
	deliveryCh chan amqp091.Delivery

	// basicCancelCh is a test-injection hook for basic.cancel notifications.
	// When non-nil, ConsumeRaw's main select loop picks it up and calls
	// cm.RecordCancelled with the received consumer tag. A nil channel is never
	// selected in Go, so production code (where basicCancelCh is always nil) is
	// unaffected.
	basicCancelCh chan string

	// deliverySubOverride is a full test-injection hook: when non-nil, openDeliveryCh
	// returns it directly, including the done channel for channel-close detection tests.
	deliverySubOverride *deliverySub

	// closedCh is closed when Close is called; signals Delivery.Ack/Nack to refuse.
	closedCh  chan struct{}
	closeOnce sync.Once

	// started guards against calling Consume/ConsumeRaw more than once.
	started atomic.Bool
}

func newConsumer[M any](b *ConsumerBuilder[M], tag string) *Consumer[M] {
	numConns := b.conn.NumConConns()
	idx := connIndexForTag(tag, numConns)
	mc := b.conn.ConConnAt(idx)

	c := &Consumer[M]{
		queue:            b.queue,
		tag:              tag,
		concurrency:      b.concurrency,
		prefetch:         b.prefetch,
		channelQoS:       b.channelQoS,
		priority:         b.priority,
		prioritySet:      b.prioritySet,
		handlerTimeout:   b.handlerTimeout,
		timeoutVerdict:   b.timeoutVerdict,
		maxRedeliveries:  b.maxRedeliveries,
		counterBDisabled: b.counterBDisabled,
		codec:            b.c,
		cm:               b.cm,
		tracer:           b.tracer,
		mc:               mc,
		closedCh:         make(chan struct{}),
	}
	// Initialise counterState with an empty map; openDeliveryCh rotates this on every
	// channel open so "channel close resets counter B" holds without explicit cleanup.
	c.counterState.Store(&redeliveryCounter{})
	return c
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
// and dispatching to h. It blocks until ctx is cancelled.
//
// The consumer automatically acks (nil return), nacks without requeue (any
// non-ErrRequeue error), or nacks with requeue (errors.Is(err, ErrRequeue)).
// May only be called once per consumer; create a new consumer to restart.
// Cancelling ctx waits for all in-flight handlers to return; set HandlerTimeout
// to bound shutdown latency when handlers may block indefinitely.
func (c *Consumer[M]) Consume(ctx context.Context, h Handler[M]) error {
	// Wrap the typed handler so dispatch can auto-ack based on the return value.
	wrapped := func(innerCtx context.Context, d *Delivery[M]) error {
		return h(innerCtx, *d.Body())
	}
	return c.runConsume(ctx, wrapped, true /* autoAck */)
}

// ConsumeRaw starts consuming, passing the full Delivery envelope to h.
// The raw handler is responsible for calling d.Ack(), d.Nack(), or d.AckIf()
// to acknowledge the delivery. The consumer does NOT auto-ack on handler return.
//
// Exception — HandlerTimeout: if HandlerTimeout is configured and the deadline fires,
// the consumer still issues a Nack automatically to prevent unacknowledged deliveries
// from accumulating. The handler is free to call Ack/Nack before the deadline;
// if it does so, the library's Nack on timeout will be a no-op (broker de-duplicates).
//
// Use ConsumeRaw to access envelope fields (Headers, Redelivered, DeathCount)
// or to implement custom ack strategies. For most workloads, Consume is simpler.
//
// May only be called once per consumer; create a new consumer to restart.
// Cancelling ctx waits for all in-flight handlers to return; set HandlerTimeout
// to bound shutdown latency when handlers may block indefinitely.
func (c *Consumer[M]) ConsumeRaw(ctx context.Context, h RawHandler[M]) error {
	return c.runConsume(ctx, h, false /* autoAck */)
}

// runConsume is the shared loop for Consume and ConsumeRaw.
// autoAck=true: dispatch applies MaxRedeliveries counter B and calls d.AckIf based on handler error.
// autoAck=false: dispatch skips counter B and d.AckIf; handler is responsible for acking.
func (c *Consumer[M]) runConsume(ctx context.Context, h RawHandler[M], autoAck bool) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: consumer already started; create a new consumer via Build() to restart", ErrInvalidOptions)
	}

	// resubCh carries replacement subscriptions produced by the reconnect hook.
	resubCh := make(chan deliverySub, 1)

	c.mc.registerHook(func(hookCtx context.Context) error {
		jitter := time.Duration(50+rand.IntN(201)) * time.Millisecond //nolint:gosec // non-crypto jitter
		select {
		case <-hookCtx.Done():
			return hookCtx.Err()
		case <-time.After(jitter):
		}
		sub, err := c.openDeliveryCh(hookCtx)
		if err != nil {
			return err
		}
		select {
		case resubCh <- sub:
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

		case sub := <-resubCh:
			cur = sub

		case tag := <-c.basicCancelCh:
			// Test-injection path: simulates broker-initiated basic.cancel.
			// Production code uses ch.NotifyCancel inside openDeliveryCh instead.
			// A nil basicCancelCh is never selected (Go semantics for nil channels).
			c.cm.RecordCancelled(c.queue, tag)

		case d, ok := <-cur.ch:
			if !ok {
				// AMQP channel closed; wait for re-subscribe or ctx cancel.
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
			// Capture the current channel's done signal so in-flight handlers
			// from this channel are cancelled if this channel closes mid-handler.
			chanDone := cur.done
			go func(raw amqp091.Delivery, chanDone <-chan struct{}) {
				defer wg.Done()
				defer func() { <-sem }()
				c.dispatch(ctx, chanDone, raw, h, autoAck)
			}(d, chanDone)
		}
	}
}

// openDeliveryCh opens a subscription. Unit tests pre-set deliverySubOverride or
// deliveryCh to inject deliveries without a live broker; production opens a real AMQP channel.
//
// On every call, a fresh redeliveryCounter (counter B state) is installed atomically
// so that channel close automatically resets all in-process redelivery counts.
func (c *Consumer[M]) openDeliveryCh(ctx context.Context) (deliverySub, error) {
	// Rotate counter B state regardless of the channel source (real or injected).
	// Installing a fresh redeliveryCounter atomically ensures "channel close resets
	// counter B": old dispatch goroutines hold a reference to the previous instance,
	// but new dispatches pick up the fresh (empty) one.
	c.counterState.Store(&redeliveryCounter{})

	if c.deliverySubOverride != nil {
		return *c.deliverySubOverride, nil
	}
	if c.deliveryCh != nil {
		// done is nil: channel-close detection is not exercised in basic unit tests.
		// basicCancelCh (when set) is handled in ConsumeRaw's main select loop.
		return deliverySub{ch: c.deliveryCh, done: nil}, nil
	}

	topoCh, err := c.mc.openChannel()
	if err != nil {
		return deliverySub{}, fmt.Errorf("warren: consumer open channel: %w", err)
	}

	ch, ok := topoCh.(*amqp091.Channel)
	if !ok {
		_ = topoCh.Close()
		return deliverySub{}, fmt.Errorf("warren: consumer: unexpected channel type %T", topoCh)
	}

	// global=true → shared prefetch for all consumers on this channel (per-channel QoS).
	// global=false → each consumer on the channel gets its own prefetch credit.
	// ChannelQoS() sets global=true (per-channel, which is RabbitMQ's recommended default).
	if err := ch.Qos(int(c.prefetch), 0, c.channelQoS); err != nil { //nolint:gosec // G115: prefetch is uint16 by protocol
		_ = ch.Close()
		return deliverySub{}, fmt.Errorf("warren: consumer Qos: %w", wrapAMQPError(err))
	}

	var args amqp091.Table
	if c.prioritySet {
		args = amqp091.Table{"x-priority": c.priority}
	}

	deliveries, err := ch.Consume(c.queue, c.tag, false, false, false, false, args)
	if err != nil {
		_ = ch.Close()
		return deliverySub{}, fmt.Errorf("warren: consumer subscribe: %w", wrapAMQPError(err))
	}

	out := make(chan amqp091.Delivery, int(c.prefetch)) //nolint:gosec // G115: prefetch bounded
	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	// channelDone is closed when the AMQP channel physically closes, not when
	// the consumer ctx is cancelled. dispatch goroutines watch this to cancel
	// in-flight handler contexts with cause ErrChannelClosed.
	channelDone := make(chan struct{})
	var onceDone sync.Once
	closeChannelDone := func() { onceDone.Do(func() { close(channelDone) }) }

	go func() {
		defer close(out)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					// basic.cancel or broker closed delivery stream.
					// Drain cancelCh non-blockingly: both cancelCh and deliveries
					// may become ready simultaneously; Go picks non-deterministically,
					// so the !ok branch can win before the cancelCh case is selected.
					// This ensures RecordCancelled is always called on every basic.cancel.
					select {
					case tag := <-cancelCh:
						c.cm.RecordCancelled(c.queue, tag)
					default:
					}
					closeChannelDone()
					return
				}
				select {
				case out <- d:
				case <-ctx.Done():
					return
				}
			case tag := <-cancelCh:
				// Broker sent basic.cancel for this consumer (e.g. queue deleted,
				// exclusive lock revoked). Record the metric; the delivery stream
				// will also close and drive closeChannelDone via the !ok drain above.
				c.cm.RecordCancelled(c.queue, tag)
			case <-closeCh:
				// AMQP channel close frame received.
				closeChannelDone()
				return
			case <-ctx.Done():
				// Consumer stopped; do NOT close channelDone — this is not a
				// channel failure, just consumer lifecycle end.
				return
			}
		}
	}()

	return deliverySub{ch: out, done: channelDone}, nil
}

// dispatch decodes and handles a single delivery.
//
// chanDone is nil when channel-close detection is not available (test injection path);
// a nil receive case in a Go select is never ready, so the chanDone case is safely disabled.
//
// autoAck=true (Consume path): counter B applied; d.AckIf called with the verdict.
// autoAck=false (ConsumeRaw path): counter B skipped; handler is responsible for acking.
func (c *Consumer[M]) dispatch(ctx context.Context, chanDone <-chan struct{}, raw amqp091.Delivery, h RawHandler[M], autoAck bool) {
	decodeStart := time.Now()
	var body M
	if err := safeDecodeConsumer(c.codec, raw.Body, &body); err != nil {
		// Record actual decode duration so the metric is meaningful even for
		// large or slow-to-fail payloads (previously hardcoded to 0).
		c.cm.RecordHandler(c.queue, "decode_error", time.Since(decodeStart))
		_ = raw.Nack(false, false)
		return
	}

	d := newDelivery[M](&body, c.queue, raw, c.closedCh)

	// — Counter A (x-death, cross-process) ————————————————————————————
	// Fires BEFORE calling the handler. Short-circuits without invoking the
	// handler when the message has already bounced through DLX n+ times.
	if c.maxRedeliveries > 0 && d.DeathCount() >= c.maxRedeliveries {
		c.cm.RecordMaxRedeliveries(c.queue, "x-death")
		c.mc.opts.logger.Warningf(
			"warren: max redeliveries exceeded for queue %q (cause=x-death, death_count=%d, limit=%d)",
			c.queue, d.DeathCount(), c.maxRedeliveries,
		)
		_ = raw.Nack(false, false)
		return
	}

	// — Counter B key (in-process, per channel) ————————————————————————
	// Only applies on the autoAck=true (Consume) path. ConsumeRaw handlers
	// control their own acking, so counter B cannot safely intercept the verdict.
	// Capture the current channel's state atomically at dispatch start so that
	// a mid-handler reconnect does not corrupt the counter map reference.
	//
	// Key is MessageID when present (stable across redeliveries → counter accumulates
	// correctly). Fallback is _dlv_<consumerTag>_<deliveryTag> when MessageID is absent;
	// delivery tags are unique within a channel but change on redelivery, so counter B
	// is effectively disabled for those messages (each delivery gets a fresh key).
	var counterBKey string
	var cs *redeliveryCounter
	if autoAck && c.maxRedeliveries > 0 && !c.counterBDisabled {
		cs = c.counterState.Load()
		if raw.MessageId != "" {
			// Stable key: MessageID persists across redeliveries → counter accumulates correctly.
			// "mid:" prefix ensures no collision with the delivery-tag-based fallback family.
			counterBKey = "mid:" + raw.MessageId
		} else {
			// No stable MessageID: use delivery tag as a unique-but-non-stable key.
			// "dlv:" prefix ensures no collision with the "mid:" family even if a publisher
			// crafts a MessageID that looks like a delivery-tag key.
			// Counter B will not accumulate across redeliveries for these messages.
			counterBKey = fmt.Sprintf("dlv:%s:%d", c.tag, raw.DeliveryTag)
		}
	}

	// hCtxBase is the WithCancelCause context used to propagate ErrChannelClosed
	// to the handler goroutine when the AMQP channel closes mid-handler (timeout path).
	hCtxBase, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)

	hCtx := hCtxBase
	if c.handlerTimeout > 0 {
		var timeoutCancel context.CancelFunc
		hCtx, timeoutCancel = context.WithTimeout(hCtxBase, c.handlerTimeout)
		defer timeoutCancel()
	}

	start := time.Now()

	if c.handlerTimeout == 0 {
		// Fast path: call handler inline; avoids per-message goroutine + channel.
		handlerErr := h(hCtx, d)
		elapsed := time.Since(start)
		// Non-blocking check: did the AMQP channel close while the handler ran?
		channelClosed := false
		if chanDone != nil {
			select {
			case <-chanDone:
				channelClosed = true
			default:
			}
		}
		if channelClosed && handlerErr != nil {
			c.cm.RecordHandlerAbortedChannelClosed(c.queue)
			c.cm.RecordHandler(c.queue, "channel_closed", elapsed)
			return // no ack — broker will redeliver
		}
		if autoAck {
			processedErr := c.applyCounterB(cs, counterBKey, handlerErr)
			c.cm.RecordHandler(c.queue, handlerOutcome(processedErr), elapsed)
			_ = d.AckIf(processedErr)
		} else {
			// ConsumeRaw: handler is responsible for ack/nack.
			c.cm.RecordHandler(c.queue, "raw", elapsed)
		}
		return
	}

	// Timeout path: run handler in a goroutine so we can enforce the deadline.
	// A nil chanDone is never selected in the select below (Go semantics).
	handlerDone := make(chan error, 1)
	go func() { handlerDone <- h(hCtx, d) }()

	select {
	case handlerErr := <-handlerDone:
		elapsed := time.Since(start)
		// Non-blocking check for a channel close that raced with handler completion.
		channelClosed := false
		if chanDone != nil {
			select {
			case <-chanDone:
				channelClosed = true
			default:
			}
		}
		if channelClosed && handlerErr != nil {
			c.cm.RecordHandlerAbortedChannelClosed(c.queue)
			c.cm.RecordHandler(c.queue, "channel_closed", elapsed)
			return
		}
		if autoAck {
			processedErr := c.applyCounterB(cs, counterBKey, handlerErr)
			c.cm.RecordHandler(c.queue, handlerOutcome(processedErr), elapsed)
			_ = d.AckIf(processedErr)
		} else {
			c.cm.RecordHandler(c.queue, "raw", elapsed)
		}

	case <-chanDone: // nil channel: never selected when chanDone is nil
		elapsed := time.Since(start)
		cancelCause(ErrChannelClosed) // cancel handler ctx before draining
		c.cm.RecordHandlerAbortedChannelClosed(c.queue)
		c.cm.RecordHandler(c.queue, "channel_closed", elapsed)
		<-handlerDone

	case <-hCtx.Done():
		elapsed := time.Since(start)
		switch {
		case errors.Is(hCtx.Err(), context.DeadlineExceeded):
			// HandlerTimeout fired.
			c.cm.RecordHandlerTimeout(c.queue)
			switch c.timeoutVerdict {
			case TimeoutNackRequeue:
				c.cm.RecordHandler(c.queue, "timeout_nack_requeue", elapsed)
				_ = raw.Nack(false, true)
			default:
				c.cm.RecordHandler(c.queue, "timeout_nack_no_requeue", elapsed)
				_ = raw.Nack(false, false)
			}
		default:
			// Outer ctx cancelled; no ack — broker will redeliver.
		}
		cancelCause(nil) // signal handler goroutine before draining
		<-handlerDone
	}
}

// applyCounterB enforces the in-process redelivery counter (counter B).
//
// If counterBKey is empty (feature disabled or counter B disabled), the original
// handlerErr is returned unchanged.
//
// Rules:
//   - If handlerErr is NOT ErrRequeue: delete the counter B entry (Ack or Nack(false) path).
//   - If handlerErr IS ErrRequeue: check whether incrementing would exceed maxRedeliveries.
//     If yes: log, record metric, delete entry, and return ErrMaxRedeliveries (rewriting the
//     verdict to Nack(false)). If no: increment and return the original ErrRequeue.
func (c *Consumer[M]) applyCounterB(cs *redeliveryCounter, counterBKey string, handlerErr error) error {
	if counterBKey == "" || cs == nil {
		return handlerErr
	}

	if !errors.Is(handlerErr, ErrRequeue) {
		// Ack or Nack(false): clean up the counter B entry to avoid memory leaks.
		cs.m.Delete(counterBKey)
		return handlerErr
	}

	// Handler returned ErrRequeue: check counter B before incrementing.
	var currentCount int64
	if v, ok := cs.m.Load(counterBKey); ok {
		if n, ok := v.(int64); ok {
			currentCount = n
		}
		// A non-int64 value would indicate a programming error; treat it as zero
		// (safe default: let the delivery proceed rather than crash).
	}

	// "Once incrementing it would exceed n" = current + 1 > n = current >= n.
	if currentCount+1 > int64(c.maxRedeliveries) {
		// Rewrite verdict: Nack(false) instead of Nack(requeue=true).
		c.cm.RecordMaxRedeliveries(c.queue, "in-process")
		c.mc.opts.logger.Warningf(
			"warren: max redeliveries exceeded for queue %q (cause=in-process, count=%d, limit=%d)",
			c.queue, currentCount+1, c.maxRedeliveries,
		)
		cs.m.Delete(counterBKey)
		return fmt.Errorf("%w (in-process counter exceeded)", ErrMaxRedeliveries)
	}

	// Increment and allow Nack(requeue=true) to proceed.
	cs.m.Store(counterBKey, currentCount+1)
	return handlerErr
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
			// Use %T (type only) rather than %v to avoid embedding payload data in the
			// error message; the panic value may carry message content from a custom codec.
			err = fmt.Errorf("%w: codec panic: %T", ErrInvalidMessage, r)
		}
	}()
	return c.Decode(payload, out)
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
