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

// BatchHandler is the function signature for batch message handlers.
//
// Return nil to ack the whole batch (single basic.ack, multiple=true).
// Return a wrapped ErrRequeue to nack with requeue=true.
// Return any other error to nack without requeue (DLX-bound).
//
// Manual acking via Batch.Ack, Batch.Nack, or individual Delivery.Ack/Nack suppresses
// the auto-verdict: the framework will not emit a second acknowledgement frame.
type BatchHandler[M any] func(ctx context.Context, batch *Batch[M]) error

// Batch holds a set of decoded messages accumulated before being dispatched to a
// BatchHandler. The framework emits a single acknowledgement frame covering all
// messages in the batch (via AMQP multiple=true) after the handler returns.
//
// # Auto-verdict semantics
//
// After the handler returns the framework checks whether the handler (or any delivery
// within the batch) already issued an ack/nack. If not, it applies the auto-verdict:
//   - nil return → single basic.ack(multiple=true) on the highest delivery-tag
//   - ErrRequeue-wrapped error → single basic.nack(multiple=true, requeue=true)
//   - any other error → single basic.nack(multiple=true, requeue=false)
//
// If the handler calls Batch.Ack, Batch.Nack, or Delivery.Ack/Nack from Deliveries(),
// the auto-verdict is suppressed (idempotent guard).
type Batch[M any] struct {
	deliveries []*Delivery[M]
	mu         sync.Mutex
	acked      bool // true once any ack/nack frame has been or will be emitted
}

// Messages returns the decoded payloads for all messages in the batch.
func (b *Batch[M]) Messages() []M {
	out := make([]M, len(b.deliveries))
	for i, d := range b.deliveries {
		out[i] = *d.Body()
	}
	return out
}

// Deliveries returns the slice of *Delivery[M] for per-message inspection or
// manual acknowledgement. Calling Ack or Nack on any returned delivery suppresses
// the batch-level auto-verdict (idempotent guard).
func (b *Batch[M]) Deliveries() []*Delivery[M] { return b.deliveries }

// Ack acknowledges all messages in the batch with a single AMQP basic.ack
// (multiple=true) on the highest delivery-tag. Subsequent calls to Ack, Nack, or
// the auto-verdict are no-ops (idempotent guard).
func (b *Batch[M]) Ack() error {
	b.mu.Lock()
	if b.acked {
		b.mu.Unlock()
		return nil
	}
	b.acked = true
	b.mu.Unlock()
	return b.ackAll()
}

// Nack negatively acknowledges all messages in the batch with a single AMQP
// basic.nack (multiple=true) on the highest delivery-tag. requeue=true re-queues
// all messages; requeue=false routes them to the DLX (or drops them).
// Subsequent calls to Ack, Nack, or the auto-verdict are no-ops.
func (b *Batch[M]) Nack(requeue bool) error {
	b.mu.Lock()
	if b.acked {
		b.mu.Unlock()
		return nil
	}
	b.acked = true
	b.mu.Unlock()
	return b.nackAll(requeue)
}

// highest returns the delivery with the largest delivery-tag.
// Returns nil if the batch is empty.
func (b *Batch[M]) highest() *Delivery[M] {
	if len(b.deliveries) == 0 {
		return nil
	}
	h := b.deliveries[0]
	for _, d := range b.deliveries[1:] {
		if d.DeliveryTag() > h.DeliveryTag() {
			h = d
		}
	}
	return h
}

// ackAll emits a single basic.ack(multiple=true) on the highest delivery-tag.
func (b *Batch[M]) ackAll() error {
	h := b.highest()
	if h == nil {
		return nil
	}
	if err := h.raw.Ack(true /* multiple */); err != nil {
		return mapAckErr(err)
	}
	return nil
}

// nackAll emits a single basic.nack(multiple=true) on the highest delivery-tag.
func (b *Batch[M]) nackAll(requeue bool) error {
	h := b.highest()
	if h == nil {
		return nil
	}
	if err := h.raw.Nack(true /* multiple */, requeue); err != nil {
		return mapAckErr(err)
	}
	return nil
}

// BatchConsumer consumes AMQP messages from a single queue in batches, decoding each
// payload to M via the configured codec, and dispatching accumulated groups to a
// BatchHandler[M].
//
// Batches are flushed when Size messages have accumulated or when the FlushAfter timer
// fires (whichever comes first). Each batch is dispatched sequentially; run multiple
// BatchConsumer[M] instances for parallelism.
//
// Use BatchConsumerFor[M](conn) to build a batch consumer.
type BatchConsumer[M any] struct {
	queue string
	tag   string

	size           uint
	flushAfter     time.Duration
	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	prefetch    uint16
	channelQoS  bool
	priority    int
	prioritySet bool

	maxRedeliveries  int
	counterBDisabled bool
	counterState     atomic.Pointer[redeliveryCounter]

	codec  codec.Codec
	cm     metrics.ConsumerMetrics
	tracer otel.Tracer

	mc *managedConn

	// deliveryCh is a test-injection hook; when non-nil, openBatchDeliveryCh
	// returns it with done=nil (channel-close detection not exercised).
	deliveryCh chan amqp091.Delivery

	closedCh  chan struct{}
	closeOnce sync.Once
	started   atomic.Bool
}

// Consume starts accumulating messages from the configured queue and dispatching
// batches to h. It blocks until ctx is cancelled.
//
// May only be called once per consumer; create a new consumer via Build() to restart.
func (c *BatchConsumer[M]) Consume(ctx context.Context, h BatchHandler[M]) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: batch consumer already started; create a new consumer via Build() to restart", ErrInvalidOptions)
	}

	resubCh := make(chan deliverySub, 1)

	c.mc.registerHook(func(hookCtx context.Context) error {
		jitter := time.Duration(50+rand.IntN(201)) * time.Millisecond //nolint:gosec // non-crypto jitter
		select {
		case <-hookCtx.Done():
			return hookCtx.Err()
		case <-time.After(jitter):
		}
		sub, err := c.openBatchDeliveryCh(hookCtx)
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

	cur, err := c.openBatchDeliveryCh(ctx)
	if err != nil {
		return err
	}

	batchCap := int(c.size) // applyDefaults guarantees c.size >= 100; cap avoids shadowing builtin
	pending := make([]*Delivery[M], 0, batchCap)

	var (
		flushTimer *time.Timer
		flushCh    <-chan time.Time
	)

	resetFlushTimer := func() {
		if c.flushAfter > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(c.flushAfter)
			flushCh = flushTimer.C
		}
	}

	stopFlushTimer := func() {
		if flushTimer != nil {
			if !flushTimer.Stop() {
				select {
				case <-flushTimer.C:
				default:
				}
			}
			flushTimer = nil
			flushCh = nil
		}
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		stopFlushTimer()

		toFlush := pending
		pending = make([]*Delivery[M], 0, batchCap)

		// Wire ackNotify so any per-delivery Ack/Nack sets batch.acked.
		batch := &Batch[M]{deliveries: toFlush}
		for _, d := range toFlush {
			d := d // capture per-iteration
			d.ackNotify = func() {
				batch.mu.Lock()
				batch.acked = true
				batch.mu.Unlock()
			}
		}

		start := time.Now()

		if c.handlerTimeout > 0 {
			hCtx, hCancel := context.WithTimeout(ctx, c.handlerTimeout)
			handlerDone := make(chan error, 1)
			go func() { handlerDone <- h(hCtx, batch) }()

			select {
			case handlerErr := <-handlerDone:
				hCancel()
				c.applyBatchVerdict(batch, handlerErr, time.Since(start))

			case <-hCtx.Done():
				hCancel()
				elapsed := time.Since(start)
				if errors.Is(hCtx.Err(), context.DeadlineExceeded) {
					c.cm.RecordHandlerTimeout(c.queue)
					requeue := c.timeoutVerdict == TimeoutNackRequeue
					outcome := "timeout_nack_no_requeue"
					if requeue {
						outcome = "timeout_nack_requeue"
					}
					c.cm.RecordHandler(c.queue, outcome, elapsed)
					// When the timeout verdict is requeue, apply counter B so that
					// MaxRedeliveries is enforced even for timed-out batches.  A
					// message whose handler consistently exceeds HandlerTimeout would
					// otherwise loop indefinitely regardless of MaxRedeliveries.
					if requeue {
						syntheticErr := c.applyBatchCounterB(batch, ErrRequeue)
						requeue = errors.Is(syntheticErr, ErrRequeue)
					}
					// Suppress auto-verdict and apply the (potentially counter-B-adjusted) timeout verdict.
					batch.mu.Lock()
					if !batch.acked {
						batch.acked = true
						batch.mu.Unlock()
						_ = batch.nackAll(requeue)
					} else {
						batch.mu.Unlock()
					}
				}
				<-handlerDone // drain goroutine
			}
			return
		}

		handlerErr := h(ctx, batch)
		c.applyBatchVerdict(batch, handlerErr, time.Since(start))
	}

	for {
		select {
		case <-ctx.Done():
			flush() // flush remaining accumulated messages
			return nil

		case sub := <-resubCh:
			// Channel reconnected: discard buffered messages (broker will redeliver).
			pending = pending[:0]
			stopFlushTimer()
			cur = sub

		case <-flushCh:
			flushCh = nil
			flushTimer = nil
			flush()

		case d, ok := <-cur.ch:
			if !ok {
				// AMQP channel closed; wait for re-subscribe or ctx cancel.
				pending = pending[:0]
				stopFlushTimer()
				select {
				case <-ctx.Done():
					return nil
				case cur = <-resubCh:
				}
				continue
			}

			// Decode payload; nack invalid messages individually without batching.
			var body M
			if err := safeDecodeConsumer(c.codec, d.Body, &body); err != nil {
				_ = d.Nack(false, false)
				continue
			}

			delivery := newDelivery[M](&body, c.queue, d, c.closedCh)

			// Counter A: x-death (cross-process redelivery counter). If the message has
			// already been DLX-bounced n+ times, nack without requeue and skip batching.
			// applyDefaults guarantees maxRedeliveries == 0 means unbounded.
			if c.maxRedeliveries > 0 && delivery.DeathCount() >= c.maxRedeliveries {
				c.cm.RecordMaxRedeliveries(c.queue, "x-death")
				_ = d.Nack(false, false)
				continue
			}

			pending = append(pending, delivery)
			resetFlushTimer()

			if uint(len(pending)) >= c.size { //nolint:gosec // G115: size bounded by uint; applyDefaults ensures size >= 100
				flush()
			}
		}
	}
}

// applyBatchVerdict applies the auto-verdict after the handler returns, provided the
// handler has not already acked/nacked manually (idempotent guard via batch.acked).
func (c *BatchConsumer[M]) applyBatchVerdict(batch *Batch[M], handlerErr error, elapsed time.Duration) {
	batch.mu.Lock()
	if batch.acked {
		// Handler or a per-delivery ack already fired; record the metric and bail.
		batch.mu.Unlock()
		c.cm.RecordHandler(c.queue, handlerOutcome(handlerErr), elapsed)
		return
	}
	batch.acked = true
	batch.mu.Unlock()

	// Apply in-process redelivery counter (counter B) before emitting the AMQP frame.
	// May rewrite ErrRequeue → ErrMaxRedeliveries (→ Nack without requeue).
	handlerErr = c.applyBatchCounterB(batch, handlerErr)

	c.cm.RecordHandler(c.queue, handlerOutcome(handlerErr), elapsed)

	if handlerErr == nil {
		_ = batch.ackAll()
	} else {
		requeue := errors.Is(handlerErr, ErrRequeue)
		_ = batch.nackAll(requeue)
	}
}

// applyBatchCounterB enforces the in-process redelivery counter (counter B) for the
// batch. Returns the (possibly rewritten) handler error.
//
// When handlerErr is ErrRequeue and maxRedeliveries > 0 and counterB is enabled:
// checks each delivery in the batch. If any would exceed the limit, all counter B
// entries for the batch are cleaned up and ErrMaxRedeliveries is returned — rewriting
// the whole batch to Nack(requeue=false). If none exceed the limit, all counters are
// incremented and ErrRequeue is returned unchanged.
//
// When handlerErr is not ErrRequeue, counter B entries for all deliveries are deleted
// (Ack or Nack(false) path — message won't be redelivered, so entries are no longer needed).
//
// If maxRedeliveries == 0 or counterBDisabled, the original handlerErr is returned as-is.
func (c *BatchConsumer[M]) applyBatchCounterB(batch *Batch[M], handlerErr error) error {
	if c.maxRedeliveries <= 0 || c.counterBDisabled {
		return handlerErr
	}
	cs := c.counterState.Load()
	if cs == nil {
		return handlerErr
	}

	if !errors.Is(handlerErr, ErrRequeue) {
		// Ack or Nack(false): clean up counter B entries to prevent memory leaks.
		for _, d := range batch.deliveries {
			cs.m.Delete(batchCounterBKey(c.tag, d.raw.MessageId, d.raw.DeliveryTag))
		}
		return handlerErr
	}

	// Verdict is ErrRequeue: collect (key, currentCount) in a single pass so each
	// sync.Map key is read exactly once (halves map operations compared to a separate
	// check loop followed by an increment loop).
	type kv struct {
		key   string
		count int64
	}
	pairs := make([]kv, 0, len(batch.deliveries))
	for _, d := range batch.deliveries {
		key := batchCounterBKey(c.tag, d.raw.MessageId, d.raw.DeliveryTag)
		var count int64
		if v, ok := cs.m.Load(key); ok {
			if n, ok2 := v.(int64); ok2 {
				count = n
			}
		}
		if count+1 > int64(c.maxRedeliveries) { //nolint:gosec // G115: maxRedeliveries is int; count+1 cannot overflow int64 in practice
			// At least one delivery exceeds the limit: rewrite the whole batch to Nack(false).
			for _, d2 := range batch.deliveries {
				cs.m.Delete(batchCounterBKey(c.tag, d2.raw.MessageId, d2.raw.DeliveryTag))
			}
			c.cm.RecordMaxRedeliveries(c.queue, "in-process")
			return fmt.Errorf("%w (in-process counter exceeded)", ErrMaxRedeliveries)
		}
		pairs = append(pairs, kv{key, count})
	}

	// All deliveries are under the limit: increment counters (reuse collected pairs —
	// no second sync.Map read needed).
	for _, p := range pairs {
		cs.m.Store(p.key, p.count+1)
	}
	return handlerErr
}

// batchCounterBKey builds the counter B sync.Map key for a single delivery.
// Mirrors the "mid:" / "dlv:" families used by Consumer[M].applyCounterB:
//   - "mid:<MessageID>" when MessageID is set (stable across redeliveries → counter accumulates correctly).
//   - "dlv:<consumerTag>:<deliveryTag>" otherwise (unique but not stable; counter resets on redeliver).
func batchCounterBKey(consumerTag, msgID string, deliveryTag uint64) string {
	if msgID != "" {
		return "mid:" + msgID
	}
	return fmt.Sprintf("dlv:%s:%d", consumerTag, deliveryTag)
}

// openBatchDeliveryCh opens a subscription channel. Unit tests inject deliveryCh
// directly; production opens a real AMQP channel.
func (c *BatchConsumer[M]) openBatchDeliveryCh(ctx context.Context) (deliverySub, error) {
	c.counterState.Store(&redeliveryCounter{})

	if c.deliveryCh != nil {
		return deliverySub{ch: c.deliveryCh, done: nil}, nil
	}

	topoCh, err := c.mc.openChannel()
	if err != nil {
		return deliverySub{}, fmt.Errorf("warren: batch consumer open channel: %w", err)
	}

	ch, ok := topoCh.(*amqp091.Channel)
	if !ok {
		_ = topoCh.Close()
		return deliverySub{}, fmt.Errorf("warren: batch consumer: unexpected channel type %T", topoCh)
	}

	if err := ch.Qos(int(c.prefetch), 0, c.channelQoS); err != nil { //nolint:gosec // G115: prefetch is uint16
		_ = ch.Close()
		return deliverySub{}, fmt.Errorf("warren: batch consumer Qos: %w", wrapAMQPError(err))
	}

	var args amqp091.Table
	if c.prioritySet {
		args = amqp091.Table{"x-priority": c.priority}
	}

	deliveries, err := ch.Consume(c.queue, c.tag, false, false, false, false, args)
	if err != nil {
		_ = ch.Close()
		return deliverySub{}, fmt.Errorf("warren: batch consumer subscribe: %w", wrapAMQPError(err))
	}

	out := make(chan amqp091.Delivery, int(c.prefetch)) //nolint:gosec // G115: prefetch bounded
	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	channelDone := make(chan struct{})
	var onceDone sync.Once
	closeChannelDone := func() { onceDone.Do(func() { close(channelDone) }) }

	go func() {
		defer close(out)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					closeChannelDone()
					return
				}
				select {
				case out <- d:
				case <-ctx.Done():
					return
				}
			case <-closeCh:
				closeChannelDone()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return deliverySub{ch: out, done: channelDone}, nil
}

// Health reports whether the batch consumer's pinned connection is healthy.
func (c *BatchConsumer[M]) Health(ctx context.Context) error {
	return c.mc.health(ctx)
}

// Close signals the batch consumer to stop accepting new deliveries.
func (c *BatchConsumer[M]) Close(_ context.Context) error {
	c.closeOnce.Do(func() { close(c.closedCh) })
	return nil
}
