package warren

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// Consumer-span outcome labels (SPEC §6.9). They largely track the verdict space of
// the consumer_handler_seconds metric so an operator can filter traces much the same
// way they filter metrics. The mapping is not identical: on the ConsumeRaw path the
// metric records the literal "raw" label (the handler owns the ack/nack), while the
// span outcome is still derived from the handler's returned verdict via
// consumeVerdictOutcome. The attribute key matches the publisher's outcome label.
const (
	outcomeAck             = "ack"
	outcomeNackRequeue     = "nack_requeue"
	outcomeNackNoRequeue   = "nack_no_requeue"
	outcomeMaxRedeliveries = "max_redeliveries"
	outcomeTimeout         = "timeout"
	outcomeChannelClosed   = "handler_aborted_channel_closed"
)

// finishConsumeSpan stamps the terminal outcome on a consume span: the
// messaging.rabbitmq.outcome attribute always, and on failure the error.type
// attribute, an Error status, and a recorded error (SPEC §6.9).
//
// A handler may return an error whose message embeds message payload or PII. To
// honour SPEC §8 (never leak message content into observability), both the status
// description and the recorded error are reduced to the closed error-type
// vocabulary (consumeErrorType) rather than err.Error() — which reaches the trace
// backend verbatim via the span status and the exception event. The recorded error
// still unwraps to the original so errors.Is-based backends keep the sentinel
// chain. Publisher spans keep err.Error() because publish errors are framework /
// broker diagnostics, never handler- or payload-derived (see finishPublishSpan).
func finishConsumeSpan(span otel.Span, outcome string, err error) {
	span.SetAttributes(attribute.String("messaging.rabbitmq.outcome", outcome))
	if err == nil {
		span.SetStatus(otelcodes.Ok, "")
		return
	}
	errType := consumeErrorType(err)
	span.SetAttributes(semconv.ErrorTypeKey.String(errType))
	span.SetStatus(otelcodes.Error, errType)
	span.RecordError(redactedSpanError{label: errType, err: err})
}

// redactedSpanError adapts an error for Span.RecordError so the recorded exception
// event renders a closed-vocabulary label (never the raw, possibly payload-derived
// err.Error()) while errors.Is/As still unwrap to the original error. It keeps
// message content out of the tracing backend (SPEC §8) without dropping the
// sentinel chain operators alert on.
type redactedSpanError struct {
	label string
	err   error
}

func (e redactedSpanError) Error() string { return e.label }
func (e redactedSpanError) Unwrap() error { return e.err }

// consumeVerdictOutcome maps a (post-counter-B) handler verdict error to the
// span outcome label. ErrPoison and any non-sentinel handler error both resolve
// to nack_no_requeue, matching d.AckIf's nack-without-requeue default.
func consumeVerdictOutcome(err error) string {
	switch {
	case err == nil:
		return outcomeAck
	case errors.Is(err, ErrMaxRedeliveries):
		return outcomeMaxRedeliveries
	case errors.Is(err, ErrRequeue):
		return outcomeNackRequeue
	default:
		return outcomeNackNoRequeue
	}
}

// consumeErrorType maps a verdict error to the error.type span attribute. Known
// sentinels resolve to their exported name for assertive alerting; anything else
// falls back to "error" (the value is never embedded, to avoid leaking content).
func consumeErrorType(err error) string {
	switch {
	case errors.Is(err, ErrMaxRedeliveries):
		return "ErrMaxRedeliveries"
	case errors.Is(err, ErrRequeue):
		return "ErrRequeue"
	case errors.Is(err, ErrPoison):
		return "ErrPoison"
	case errors.Is(err, ErrChannelClosed):
		return "ErrChannelClosed"
	case errors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	default:
		return "error"
	}
}

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
//
// mu serialises the load→check→store/delete sequence in applyCounterB so the
// read-modify-write is *atomic* (Lens-08 / CR-02). sync.Map alone is only
// memory-safe: two goroutines incrementing the same key could both read the old
// value and both write the same increment, losing one — a logical lost update
// that `go test -race` cannot detect. The mutex closes that window; the embedded
// sync.Map is retained for its lock-free Range/Load fast paths used elsewhere.
type redeliveryCounter struct {
	mu sync.Mutex
	m  sync.Map // key: "mid:<MessageID>" or "dlv:<consumerTag>:<deliveryTag>", value: int64
}

// maxMsgIDKeyLen is the maximum number of bytes of a MessageId that are used as
// part of a sync.Map key in the redelivery counter. Truncating here bounds the
// memory cost of a long (or adversarially crafted) MessageId to a fixed amount
// per in-flight delivery, regardless of the original MessageId length.
const maxMsgIDKeyLen = 512

// maxConsumerTagKeyLen is the maximum number of bytes of a consumerTag embedded
// in the "dlv:" fallback key. The AMQP 0-9-1 shortstr limit is 255 bytes; we
// use 256 as a round ceiling that covers any conforming tag plus one spare byte.
// Truncating here applies the same memory-bound principle as maxMsgIDKeyLen.
const maxConsumerTagKeyLen = 256

// counterBKeyForMsgID builds the "mid:<MessageId>" sync.Map key for redelivery
// counter B, truncating MessageId to maxMsgIDKeyLen bytes when necessary.
// Truncation is done at a UTF-8 rune boundary so the resulting key is always
// valid UTF-8, which is safe to log, trace, or export.
//
// Returns "" when truncation reduces msgID to an empty string (e.g., a MessageId
// composed entirely of UTF-8 continuation bytes). Callers must treat "" as
// "absent MessageId" and fall back to counterBKeyForDeliveryTag to avoid
// collapsing distinct messages onto the same sync.Map slot.
func counterBKeyForMsgID(msgID string) string {
	if len(msgID) > maxMsgIDKeyLen {
		msgID = truncateAtRuneBoundary(msgID, maxMsgIDKeyLen)
	}
	if msgID == "" {
		// Truncation produced an empty result (e.g. all continuation bytes).
		// Return "" so the call site falls back to the delivery-tag key,
		// preventing all such messages from sharing the degenerate "mid:" slot.
		return ""
	}
	return "mid:" + msgID
}

// truncateAtRuneBoundary returns s[:n] stepped back to the nearest rune start,
// so the result is always valid UTF-8. If n >= len(s) the original string is
// returned unchanged.
func truncateAtRuneBoundary(s string, n int) string {
	if n >= len(s) {
		return s
	}
	// Walk backwards from n until we land on a rune-start byte.
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// counterBKeyForDeliveryTag builds the "dlv:<consumerTag>:<deliveryTag>" key
// used as a fallback when MessageId is absent or produces an empty truncation
// result. consumerTag is truncated to maxConsumerTagKeyLen bytes to bound the
// memory cost of in-flight deliveries in the sync.Map.
func counterBKeyForDeliveryTag(consumerTag string, deliveryTag uint64) string {
	if len(consumerTag) > maxConsumerTagKeyLen {
		consumerTag = truncateAtRuneBoundary(consumerTag, maxConsumerTagKeyLen)
	}
	return fmt.Sprintf("dlv:%s:%d", consumerTag, deliveryTag)
}

// load returns the current redelivery count for key, or 0 if absent.
// A non-int64 value in the map indicates a programming error; 0 is the safe default
// (allows the delivery to proceed rather than crash or enter an infinite loop).
func (cs *redeliveryCounter) load(key string) int64 {
	if v, ok := cs.m.Load(key); ok {
		if n, ok2 := v.(int64); ok2 {
			return n
		}
	}
	return 0
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

	codec      codec.Codec
	cm         metrics.ConsumerMetrics
	tracer     otel.Tracer
	propagator otel.Propagator

	// msgType is the message_type metrics label value (Go type name of M),
	// computed once at build time.
	msgType string

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

	// testHookBeforeTimeoutDrain, when non-nil, is invoked inside dispatch's hCtx.Done()
	// branch immediately before draining handlerDone. It is a test-only synchronization
	// seam (nil in production, a no-op) that lets a test confirm dispatch has committed to
	// the ctx-done branch before unblocking the handler goroutine — making the "no
	// ack/nack on outer-ctx cancel" assertion deterministic under -race instead of racing
	// handlerDone vs hCtx.Done().
	testHookBeforeTimeoutDrain func()

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
		propagator:       otel.NewPropagator(),
		msgType:          metricsTypeName[M](),
		mc:               mc,
		closedCh:         make(chan struct{}),
	}
	// Initialise counterState with an empty map; openDeliveryCh rotates this on every
	// channel open so "channel close resets counter B" holds without explicit cleanup.
	c.counterState.Store(&redeliveryCounter{})
	return c
}

// recordHandler records a handler outcome, supplying the message_type metrics
// label value so an enabled consumer_handler_seconds histogram carries it.
func (c *Consumer[M]) recordHandler(outcome string, d time.Duration) {
	c.cm.RecordHandler(c.queue, c.msgType, outcome, d)
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
	if err := safeDecodeConsumer(c.codec, raw.Body, raw.Headers, raw.ContentType, &body); err != nil {
		// Record actual decode duration so the metric is meaningful even for
		// large or slow-to-fail payloads (previously hardcoded to 0).
		c.recordHandler("decode_error", time.Since(decodeStart))
		_ = raw.Nack(false, false)
		return
	}

	d := newDelivery[M](&body, c.queue, raw, c.closedCh)

	// hCtxBase is the WithCancelCause context used to propagate ErrChannelClosed
	// to the handler goroutine when the AMQP channel closes mid-handler (timeout path).
	hCtxBase, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)

	// Parent the process span on the producer's trace context (extracted from the
	// incoming headers) so the trace-id is continuous publisher → consumer, while
	// keeping the handler context a descendant of the consumer context so outer
	// cancellation and HandlerTimeout still propagate (SPEC §6.9).
	parentCtx := c.propagator.ExtractTo(hCtxBase, raw.Headers)

	hCtx := parentCtx
	if c.handlerTimeout > 0 {
		var timeoutCancel context.CancelFunc
		hCtx, timeoutCancel = context.WithTimeout(parentCtx, c.handlerTimeout)
		defer timeoutCancel()
	}

	// Open the <queue> process span; it wraps counter A, the handler, and every
	// verdict path. defer guarantees it is ended even on a handler panic.
	spanCtx, span := c.tracer.Start(hCtx, c.queue+" process", c.processSpanAttrs(raw)...)
	defer span.End()

	// — Counter A (x-death, cross-process) ————————————————————————————
	// Fires BEFORE calling the handler. Short-circuits without invoking the
	// handler when the message has already bounced through DLX n+ times.
	if c.maxRedeliveries > 0 && d.DeathCount() >= c.maxRedeliveries {
		c.cm.RecordMaxRedeliveries(c.queue, "x-death")
		c.mc.opts.logger.Warningf(
			"warren: max redeliveries exceeded for queue %q (cause=x-death, death_count=%d, limit=%d)",
			c.queue, d.DeathCount(), c.maxRedeliveries,
		)
		finishConsumeSpan(span, outcomeMaxRedeliveries, ErrMaxRedeliveries)
		_ = raw.Nack(false, false)
		return
	}

	// — Counter B key (in-process, per channel) ————————————————————————
	// Only applies on the autoAck=true (Consume) path. ConsumeRaw handlers
	// control their own acking, so counter B cannot safely intercept the verdict.
	// Capture the current channel's state atomically at dispatch start so that
	// a mid-handler reconnect does not corrupt the counter map reference.
	// Key families: see redeliveryCounter struct comment.
	var counterBKey string
	var cs *redeliveryCounter
	if autoAck && c.maxRedeliveries > 0 && !c.counterBDisabled {
		cs = c.counterState.Load()
		if raw.MessageId != "" {
			// Stable key: MessageID persists across redeliveries → counter accumulates correctly.
			// counterBKeyForMsgID truncates to maxMsgIDKeyLen to bound memory usage.
			// It returns "" when the truncated result is empty (e.g. all continuation bytes);
			// in that case we fall through to the delivery-tag fallback below.
			counterBKey = counterBKeyForMsgID(raw.MessageId)
		}
		if counterBKey == "" {
			// No stable MessageID (or truncation produced empty result):
			// use delivery tag as a unique-but-non-stable key.
			// Counter B will not accumulate across redeliveries for these messages.
			counterBKey = counterBKeyForDeliveryTag(c.tag, raw.DeliveryTag)
		}
	}

	start := time.Now()

	if c.handlerTimeout == 0 {
		// Fast path: call handler inline; avoids per-message goroutine + channel.
		handlerErr := safeCallHandler(spanCtx, h, d)
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
			c.recordHandler("channel_closed", elapsed)
			finishConsumeSpan(span, outcomeChannelClosed, ErrChannelClosed)
			return // no ack — broker will redeliver
		}
		if autoAck {
			processedErr := c.applyCounterB(cs, counterBKey, handlerErr)
			c.recordHandler(handlerOutcome(processedErr), elapsed)
			finishConsumeSpan(span, consumeVerdictOutcome(processedErr), processedErr)
			_ = d.AckIf(processedErr)
		} else {
			// ConsumeRaw: handler is responsible for ack/nack.
			c.recordHandler("raw", elapsed)
			finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)
		}
		return
	}

	// Timeout path: run handler in a goroutine so we can enforce the deadline.
	// A nil chanDone is never selected in the select below (Go semantics).
	handlerDone := make(chan error, 1)
	go func() { handlerDone <- safeCallHandler(spanCtx, h, d) }()

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
			c.recordHandler("channel_closed", elapsed)
			finishConsumeSpan(span, outcomeChannelClosed, ErrChannelClosed)
			return
		}
		if autoAck {
			processedErr := c.applyCounterB(cs, counterBKey, handlerErr)
			c.recordHandler(handlerOutcome(processedErr), elapsed)
			finishConsumeSpan(span, consumeVerdictOutcome(processedErr), processedErr)
			_ = d.AckIf(processedErr)
		} else {
			c.recordHandler("raw", elapsed)
			finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)
		}

	case <-chanDone: // nil channel: never selected when chanDone is nil
		elapsed := time.Since(start)
		cancelCause(ErrChannelClosed) // cancel handler ctx before draining
		c.cm.RecordHandlerAbortedChannelClosed(c.queue)
		c.recordHandler("channel_closed", elapsed)
		finishConsumeSpan(span, outcomeChannelClosed, ErrChannelClosed)
		<-handlerDone

	case <-hCtx.Done():
		elapsed := time.Since(start)
		switch {
		case errors.Is(hCtx.Err(), context.DeadlineExceeded):
			// HandlerTimeout fired.
			c.cm.RecordHandlerTimeout(c.queue)
			switch c.timeoutVerdict {
			case TimeoutNackRequeue:
				c.recordHandler("timeout_nack_requeue", elapsed)
				_ = raw.Nack(false, true)
			default:
				c.recordHandler("timeout_nack_no_requeue", elapsed)
				_ = raw.Nack(false, false)
			}
			finishConsumeSpan(span, outcomeTimeout, context.DeadlineExceeded)
		default:
			// Outer ctx cancelled; no ack — broker will redeliver. No verdict is
			// recorded on the span: this is consumer lifecycle end, not a message
			// outcome. The deferred span.End() still closes the span.
		}
		cancelCause(nil) // signal handler goroutine before draining
		if c.testHookBeforeTimeoutDrain != nil {
			c.testHookBeforeTimeoutDrain()
		}
		<-handlerDone
	}
}

// processSpanAttrs builds the messaging attributes for a <queue> process span
// (SPEC §6.9). message.id and conversation_id are included only when present.
func (c *Consumer[M]) processSpanAttrs(raw amqp091.Delivery) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("rabbitmq"),
		semconv.MessagingDestinationName(c.queue),
		semconv.MessagingOperationTypeKey.String("process"),
	}
	if raw.MessageId != "" {
		attrs = append(attrs, semconv.MessagingMessageID(raw.MessageId))
	}
	if raw.CorrelationId != "" {
		attrs = append(attrs, semconv.MessagingMessageConversationID(raw.CorrelationId))
	}
	return attrs
}

// safeCallHandler invokes h, recovering a handler panic into an error so the
// consumer goroutine survives and the process span can be ended with a failure
// outcome (SPEC §6.9 "ended in every termination path including panics"). A
// recovered panic maps to nack-without-requeue (it is not ErrRequeue), matching
// the poison-message contract. The panic value is reported by type only so a
// panicking handler cannot leak message content into the error string or span.
func safeCallHandler[M any](ctx context.Context, h RawHandler[M], d *Delivery[M]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("warren: handler panic: %T", r)
		}
	}()
	return h(ctx, d)
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
		// Take cs.mu so this delete cannot interleave with a concurrent increment
		// for the same key (a duplicate-MessageId delivery on another slot).
		cs.mu.Lock()
		cs.m.Delete(counterBKey)
		cs.mu.Unlock()
		return handlerErr
	}

	// Handler returned ErrRequeue: atomically read-modify-write counter B.
	// The whole load→check→store/delete runs under cs.mu so concurrent
	// redeliveries of the same key (at-least-once duplicates sharing a
	// MessageId) cannot lose an increment (Lens-08 / CR-02). Metric and log
	// side effects are emitted after releasing the lock to keep the critical
	// section to the map mutation only.
	cs.mu.Lock()
	currentCount := cs.load(counterBKey)
	// "Once incrementing it would exceed n" = current + 1 > n = current >= n.
	exceeded := currentCount+1 > int64(c.maxRedeliveries)
	if exceeded {
		// Delete on exceed so the entry does not leak. This deliberately resets
		// the slot to 0: under concurrent duplicates of the same MessageId, a
		// sibling delivery may then be re-allowed once rather than rewritten to
		// Nack(false), so the per-process bound is soft (the loop is still
		// bounded — each exceeding delivery is Nacked-without-requeue and drains).
		// Counter B is a process-local heuristic; hard bounding comes from the
		// quorum DeliveryLimit or counter A (x-death). See SPEC §6.3.
		cs.m.Delete(counterBKey)
	} else {
		cs.m.Store(counterBKey, currentCount+1)
	}
	cs.mu.Unlock()

	if exceeded {
		// Rewrite verdict: Nack(false) instead of Nack(requeue=true).
		c.cm.RecordMaxRedeliveries(c.queue, "in-process")
		c.mc.opts.logger.Warningf(
			"warren: max redeliveries exceeded for queue %q (cause=in-process, count=%d, limit=%d)",
			c.queue, currentCount+1, c.maxRedeliveries,
		)
		return fmt.Errorf("%w (in-process counter exceeded)", ErrMaxRedeliveries)
	}

	// Increment accepted; allow Nack(requeue=true) to proceed.
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
// A HeaderCodec (e.g. CloudEvents binary mode) also receives the delivery headers
// and content-type property so its attributes can be reconstituted; it must only
// read the headers, never mutate them (the library does not rewrite consume-path
// headers).
func safeDecodeConsumer(c codec.Codec, payload []byte, headers amqp091.Table, contentType string, out any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Use %T (type only) rather than %v to avoid embedding payload data in the
			// error message; the panic value may carry message content from a custom codec.
			err = fmt.Errorf("%w: codec panic: %T", ErrInvalidMessage, r)
		}
	}()
	if hc, ok := c.(codec.HeaderCodec); ok {
		return hc.DecodeWithHeaders(payload, headers, contentType, out)
	}
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
