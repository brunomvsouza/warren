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
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

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

	codec      codec.Codec
	cm         metrics.ConsumerMetrics
	tracer     otel.Tracer
	propagator otel.Propagator

	// msgType is the message_type metrics label value (Go type name of M),
	// computed once at build time.
	msgType string

	mc *managedConn

	// deliveryCh is a test-injection hook; when non-nil, openBatchDeliveryCh
	// returns it with done=nil (channel-close detection not exercised).
	deliveryCh chan amqp091.Delivery

	// testHookBeforeTimeoutDrain, when non-nil, is invoked inside the handler-timeout
	// select's hCtx.Done() branch immediately before draining handlerDone. It is a
	// test-only synchronization seam (nil in production, a no-op) that lets a test
	// confirm the select has committed to the ctx-done branch before unblocking the
	// handler goroutine — making the "no frame on outer-ctx cancel" assertion
	// deterministic under -race instead of racing handlerDone vs hCtx.Done().
	testHookBeforeTimeoutDrain func()

	closedCh  chan struct{}
	closeOnce sync.Once
	started   atomic.Bool
}

// recordHandler records a batch-handler outcome, supplying the message_type
// metrics label value so an enabled consumer_handler_seconds histogram carries it.
func (c *BatchConsumer[M]) recordHandler(outcome string, d time.Duration) {
	c.cm.RecordHandler(c.queue, c.msgType, outcome, d)
}

// Consume starts accumulating messages from the configured queue and dispatching
// batches to h. It blocks until ctx is cancelled.
//
// Cancelling ctx flushes any pending batch before returning; set HandlerTimeout
// to bound shutdown latency when batch handlers may block indefinitely.
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
			d.ackNotify = func() {
				batch.mu.Lock()
				batch.acked = true
				batch.mu.Unlock()
			}
		}

		// Open the <queue> process_batch span with one Link per message's producer
		// trace (fan-in semantics, SPEC §6.9). defer ends it even on a handler panic.
		spanCtx, span := c.startBatchSpan(ctx, toFlush)
		defer span.End()

		start := time.Now()

		if c.handlerTimeout > 0 {
			hCtx, hCancel := context.WithTimeout(spanCtx, c.handlerTimeout)
			handlerDone := make(chan error, 1)
			go func() { handlerDone <- safeCallBatchHandler(hCtx, h, batch) }()

			select {
			case handlerErr := <-handlerDone:
				hCancel()
				c.applyBatchVerdict(span, batch, handlerErr, time.Since(start))

			case <-hCtx.Done():
				hCancel()
				elapsed := time.Since(start)
				if errors.Is(hCtx.Err(), context.DeadlineExceeded) {
					// The handler exceeded HandlerTimeout. Emit timeout metrics and apply
					// the timeout verdict only when the handler has not already applied its
					// own verdict (e.g. called batch.Ack() before the deadline expired):
					// emitting timeout metrics for an already-acked batch would produce
					// misleading dashboard spikes that do not correspond to a real nack.
					batch.mu.Lock()
					alreadyAcked := batch.acked
					batch.mu.Unlock()

					if !alreadyAcked {
						c.cm.RecordHandlerTimeout(c.queue)
						requeue := c.timeoutVerdict == TimeoutNackRequeue
						outcome := "timeout_nack_no_requeue"
						if requeue {
							outcome = "timeout_nack_requeue"
						}
						c.recordHandler(outcome, elapsed)
						// When the timeout verdict is requeue, apply counter B so that
						// MaxRedeliveries is enforced even for timed-out batches.  A
						// message whose handler consistently exceeds HandlerTimeout would
						// otherwise loop indefinitely regardless of MaxRedeliveries.
						if requeue {
							syntheticErr := c.applyBatchCounterB(batch, ErrRequeue)
							requeue = errors.Is(syntheticErr, ErrRequeue)
						}
						// Suppress auto-verdict and apply the (potentially counter-B-adjusted)
						// timeout verdict. Re-check batch.acked under the lock: the handler
						// goroutine is still running and may have acked between the
						// alreadyAcked read above and here, in which case the synthetic nack
						// must be dropped to avoid a double ack/nack frame.
						batch.mu.Lock()
						if !batch.acked {
							batch.acked = true
							batch.mu.Unlock()
							_ = batch.nackAll(requeue)
						} else {
							batch.mu.Unlock()
						}
					}
					// Stamp the batch span with the timeout outcome regardless of whether
					// the handler acked manually before the deadline: in both cases the
					// handler exceeded HandlerTimeout and the span must carry a terminal
					// outcome (SPEC §6.9, "ended in every termination path"). This flush
					// invocation owns the span and ends it via the deferred span.End();
					// there is no later path that would stamp it. Note the deliberate
					// span/metric divergence: when alreadyAcked is true the span still
					// carries outcome=timeout, but no timeout metric was recorded (the block
					// above is skipped) — the metric is suppressed so an already-acked batch
					// does not produce a misleading nack-spike on dashboards.
					finishConsumeSpan(span, outcomeTimeout, context.DeadlineExceeded)
				}
				// Otherwise (outer ctx cancelled, not DeadlineExceeded): consumer lifecycle
				// end, not a message outcome. No verdict is stamped on the span — mirroring
				// Consumer.dispatch's outer-ctx-cancel path — and the deferred span.End()
				// still closes it.
				if c.testHookBeforeTimeoutDrain != nil {
					c.testHookBeforeTimeoutDrain()
				}
				<-handlerDone // drain goroutine
			}
			return
		}

		handlerErr := safeCallBatchHandler(spanCtx, h, batch)
		c.applyBatchVerdict(span, batch, handlerErr, time.Since(start))
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
			if err := safeDecodeConsumer(c.codec, d.Body, d.Headers, d.ContentType, &body); err != nil {
				_ = d.Nack(false, false)
				continue
			}

			delivery := newDelivery[M](&body, c.queue, d, c.closedCh)

			// Counter A: x-death (cross-process redelivery counter). If the message has
			// already been DLX-bounced n+ times, nack without requeue and skip batching.
			// applyDefaults guarantees maxRedeliveries == 0 means unbounded.
			if c.maxRedeliveries > 0 && delivery.DeathCount() >= c.maxRedeliveries {
				c.cm.RecordMaxRedeliveries(c.queue, "x-death")
				c.mc.opts.logger.Warningf(
					"warren: max redeliveries exceeded for queue %q (cause=x-death, death_count=%d, limit=%d)",
					c.queue, delivery.DeathCount(), c.maxRedeliveries,
				)
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
// It also stamps the terminal outcome on the batch process span (SPEC §6.9).
func (c *BatchConsumer[M]) applyBatchVerdict(span otel.Span, batch *Batch[M], handlerErr error, elapsed time.Duration) {
	batch.mu.Lock()
	if batch.acked {
		// Handler or a per-delivery ack already fired; record the metric and bail.
		batch.mu.Unlock()
		c.recordHandler(handlerOutcome(handlerErr), elapsed)
		finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)
		return
	}
	batch.acked = true
	batch.mu.Unlock()

	// Apply in-process redelivery counter (counter B) before emitting the AMQP frame.
	// May rewrite ErrRequeue → ErrMaxRedeliveries (→ Nack without requeue).
	handlerErr = c.applyBatchCounterB(batch, handlerErr)

	c.recordHandler(handlerOutcome(handlerErr), elapsed)
	finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)

	if handlerErr == nil {
		_ = batch.ackAll()
	} else {
		requeue := errors.Is(handlerErr, ErrRequeue)
		_ = batch.nackAll(requeue)
	}
}

// startBatchSpan opens the <queue> process_batch span. When the configured tracer
// implements otel.LinkingTracer the span receives one Link per message, each
// pointing at that message's producer trace context (extracted from its headers).
// This models batch fan-in correctly: a batch has many parents, not one (SPEC §6.9
// "BatchConsumer Links"). A non-linking tracer transparently falls back to a span
// with no links.
func (c *BatchConsumer[M]) startBatchSpan(ctx context.Context, deliveries []*Delivery[M]) (context.Context, otel.Span) {
	name := c.queue + " process_batch"
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("rabbitmq"),
		semconv.MessagingDestinationName(c.queue),
		semconv.MessagingOperationTypeKey.String("process"),
		semconv.MessagingBatchMessageCount(len(deliveries)),
	}
	lt, ok := c.tracer.(otel.LinkingTracer)
	if !ok {
		return c.tracer.Start(ctx, name, attrs...)
	}
	links := make([]otel.Link, 0, len(deliveries))
	for _, d := range deliveries {
		// A delivery with no producer traceparent extracts to context.Background()
		// (an invalid span context). Skip it so the LinkingTracer adapter only ever
		// receives Links with a valid producer context, honouring the otel.Link
		// contract ("A Context with no valid span context contributes no Link").
		linkCtx := c.propagator.Extract(d.raw.Headers)
		if !c.propagator.ActiveContext(linkCtx) {
			continue
		}
		links = append(links, otel.Link{Context: linkCtx})
	}
	return lt.StartWithLinks(ctx, name, links, attrs...)
}

// safeCallBatchHandler invokes h, recovering a handler panic into an error so the
// flush loop survives and the process_batch span can be ended with a failure
// outcome (SPEC §6.9). A recovered panic maps to nack-without-requeue. The panic
// value is reported by type only so a panicking handler cannot leak message
// content into the error string or span.
func safeCallBatchHandler[M any](ctx context.Context, h BatchHandler[M], batch *Batch[M]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("warren: batch handler panic: %T", r)
		}
	}()
	return h(ctx, batch)
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

	// Fast-path: single-delivery batches skip the []kv heap allocation.
	// Logic mirrors the general path below but operates on a single delivery directly.
	if len(batch.deliveries) == 1 {
		d := batch.deliveries[0]
		key := batchCounterBKey(c.tag, d.raw.MessageId, d.raw.DeliveryTag)
		count := cs.load(key)
		if count+1 > int64(c.maxRedeliveries) { //nolint:gosec // G115: maxRedeliveries is int; count+1 cannot overflow int64 in practice
			c.cm.RecordMaxRedeliveries(c.queue, "in-process")
			c.mc.opts.logger.Warningf(
				"warren: max redeliveries exceeded for queue %q (cause=in-process, count=%d, limit=%d)",
				c.queue, count+1, c.maxRedeliveries,
			)
			cs.m.Delete(key)
			return fmt.Errorf("%w (in-process counter exceeded)", ErrMaxRedeliveries)
		}
		cs.m.Store(key, count+1)
		return handlerErr
	}

	// General path: multi-delivery batches — collect (key, currentCount) in a single
	// pass so each sync.Map key is read exactly once (halves map operations compared to
	// a separate check loop followed by an increment loop).
	type kv struct {
		key   string
		count int64
	}
	pairs := make([]kv, 0, len(batch.deliveries))
	for _, d := range batch.deliveries {
		key := batchCounterBKey(c.tag, d.raw.MessageId, d.raw.DeliveryTag)
		count := cs.load(key)
		if count+1 > int64(c.maxRedeliveries) { //nolint:gosec // G115: maxRedeliveries is int; count+1 cannot overflow int64 in practice
			// At least one delivery exceeds the limit: rewrite the whole batch to Nack(false).
			c.cm.RecordMaxRedeliveries(c.queue, "in-process")
			c.mc.opts.logger.Warningf(
				"warren: max redeliveries exceeded for queue %q (cause=in-process, count=%d, limit=%d)",
				c.queue, count+1, c.maxRedeliveries,
			)
			for _, d2 := range batch.deliveries {
				cs.m.Delete(batchCounterBKey(c.tag, d2.raw.MessageId, d2.raw.DeliveryTag))
			}
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
//     MessageID is truncated to maxMsgIDKeyLen bytes via counterBKeyForMsgID to bound memory usage.
//   - "dlv:<consumerTag>:<deliveryTag>" otherwise (unique but not stable; counter resets on redeliver).
func batchCounterBKey(consumerTag, msgID string, deliveryTag uint64) string {
	if msgID != "" {
		// counterBKeyForMsgID returns "" when truncation produces an empty result
		// (e.g. a MessageId composed entirely of UTF-8 continuation bytes). Fall
		// through to the delivery-tag fallback in that case to avoid collapsing
		// distinct messages onto the degenerate "mid:" sync.Map slot.
		if key := counterBKeyForMsgID(msgID); key != "" {
			return key
		}
	}
	return counterBKeyForDeliveryTag(consumerTag, deliveryTag)
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
