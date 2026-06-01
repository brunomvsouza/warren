package warren

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	"github.com/brunomvsouza/warren/log"
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
// chain. finishPublishSpan applies the same redaction to its one payload-derived
// class (the codec-encode ErrInvalidMessage) and keeps err.Error() only for
// broker/framework diagnostics, which carry no message content (see
// finishPublishSpan).
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

	concurrency uint
	prefetch    uint16
	channelQoS  bool
	priority    int
	prioritySet bool

	// maxInFlightBytes is the T50 in-flight memory guardrail budget (MaxInFlightBytes()).
	// 0 means disabled; byteLimiter is non-nil only when > 0.
	maxInFlightBytes int64
	byteLimiter      *byteLimiter

	// exclusive requests the basic.consume exclusive flag (Exclusive()).
	exclusive bool
	// consumeArgs are extra basic.consume arguments (Args()); x-priority is layered
	// on top at subscribe time when prioritySet is true.
	consumeArgs Headers
	// onCancel fires when the broker sends basic.cancel (OnCancel()); nil → warn.
	onCancel func(reason string)

	// brokerAutoAck issues basic.consume with the AMQP no-ack flag (AutoAck()).
	// This is DISTINCT from the dispatch `autoAck` parameter, which only selects
	// the Consume (library applies the verdict) vs ConsumeRaw (handler applies it)
	// path. brokerAutoAck=true means the BROKER already acked on dispatch, so the
	// library issues no ack/nack at all and a handler error degrades to a sampled
	// warning (SPEC §6.3 "handler error semantics are bypassed").
	brokerAutoAck bool

	handlerTimeout time.Duration
	timeoutVerdict TimeoutVerdict

	// depthSampleInterval is the WithQueueDepthSampler polling period (T52); 0 disables it.
	depthSampleInterval time.Duration

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

	// autoAckDropLog rate-limits the "message dropped" warning emitted under
	// brokerAutoAck when a handler errors or a payload fails to decode (the broker
	// already acked, so there is nothing to nack). See dropSampler.
	autoAckDropLog dropSampler

	// mc is the consumer-role managed connection this consumer is pinned to.
	mc *managedConn

	// deliveryCh is a basic test-injection hook: when non-nil, openDeliveryCh
	// returns it with done=nil (channel-close detection is not exercised).
	deliveryCh chan amqp091.Delivery

	// cancelReasonCh carries broker basic.cancel notifications (the cancelled
	// consumer tag) from the openDeliveryCh delivery pump to runConsume's main loop,
	// which then fires OnCancel, records the metric, and returns ErrConsumerCancelled.
	// runConsume lazily creates it (buffered, size 1) when nil; tests may pre-set it
	// to inject a cancel without a live broker.
	cancelReasonCh chan string

	// deliverySubOverride is a full test-injection hook: when non-nil, openDeliveryCh
	// returns it directly, including the done channel for channel-close detection tests.
	deliverySubOverride *deliverySub

	// healthCheckOverride is a test-injection hook for Health's connection-liveness
	// gate: when non-nil, Health calls it instead of c.mc.health. The real gate opens
	// a broker channel, so the happy path (gate passes -> return the snapshot) is
	// otherwise reachable only on the integration lane. nil in production (T53).
	healthCheckOverride func(context.Context) error

	// testHookBeforeTimeoutDrain, when non-nil, is invoked inside dispatch's hCtx.Done()
	// branch immediately before draining handlerDone. It is a test-only synchronization
	// seam (nil in production, a no-op) that lets a test confirm dispatch has committed to
	// the ctx-done branch before unblocking the handler goroutine — making the "no
	// ack/nack on outer-ctx cancel" assertion deterministic under -race instead of racing
	// handlerDone vs hCtx.Done().
	testHookBeforeTimeoutDrain func()

	// testHookChannelClosed, when non-nil, is invoked inside runConsume's main loop
	// the instant cur.ch reports closed (!ok), before the inner select waits for a
	// re-subscribe, a pending basic.cancel, or ctx cancel. It is a test-only seam
	// (nil in production, a no-op): because it fires only after the outer select has
	// already taken the !ok branch, a test can buffer a basic.cancel reason at that
	// exact point to deterministically exercise the "channel closed with a cancel
	// already pending" inner-select branch without a fixed sleep.
	testHookChannelClosed func()

	// closedCh is closed when Close is called; signals Delivery.Ack/Nack to refuse.
	closedCh  chan struct{}
	closeOnce sync.Once

	// started guards against calling Consume/ConsumeRaw more than once.
	started atomic.Bool

	// stopped is set once runConsume returns — via ctx cancel, a broker basic.cancel
	// (ErrConsumerCancelled), or a fatal openDeliveryCh error. It distinguishes a
	// consumer whose loop has permanently exited from a closed one, so ConsumerHealth.
	// Active reports false for a silently-dead consumer even without a Close call (T53).
	stopped atomic.Bool

	// — T53 Health snapshot + draining state —
	// lastDeliveryNanos is the UnixNano of the most recent delivery received from the
	// broker (0 = none yet); surfaced as ConsumerHealth.LastDeliveryAt.
	lastDeliveryNanos atomic.Int64
	// inFlight counts handler goroutines currently executing; surfaced as
	// ConsumerHealth.InFlightHandlers.
	inFlight atomic.Int64
	// paused is true between Pause and Resume; surfaced as ConsumerHealth.Paused and
	// read by the delivery pump to keep in-flight handlers alive across a local cancel.
	paused atomic.Bool

	// pauseMu serializes Pause/Resume and guards live + runCtx. live holds the
	// current physical-channel handle so Pause can issue a local basic.cancel and
	// Resume can re-issue basic.consume on the same channel without reopening it.
	// runCtx is the consumer-lifecycle ctx (the one passed to Consume/ConsumeRaw):
	// Resume binds its re-subscribe pump to runCtx, NOT to the (possibly
	// request-scoped) ctx passed to Resume, so cancelling that caller ctx never
	// silently stops delivery (T53).
	pauseMu sync.Mutex
	live    *liveSub
	runCtx  context.Context

	// resubCh carries replacement subscriptions from the reconnect hook and from
	// Resume into runConsume's loop. runConsume creates it (buffered, size 1) when
	// nil; tests may pre-set it to drive Resume without a running loop.
	resubCh chan deliverySub
}

// subChannel is the subset of *amqp091.Channel that the subscription lifecycle
// needs after the channel is open: re-subscribe via Consume and locally cancel via
// Cancel. Narrowing to an interface lets Pause/Resume be unit-tested with a fake
// (T53).
type subChannel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error)
	Cancel(consumer string, noWait bool) error
}

// liveSub holds the per-physical-channel state shared across a Pause/Resume cycle.
// closeCh/cancelCh/done are registered once per physical channel and reused by every
// pump, so a Pause/Resume cycle re-issues basic.consume on the same channel rather
// than reopening it (preserving in-flight acks) and does not re-register notify
// listeners on each cycle.
type liveSub struct {
	ch        subChannel
	closeCh   <-chan *amqp091.Error
	cancelCh  <-chan string
	done      chan struct{}
	closeDone func() // closes done exactly once (the physical channel died)
}

func newConsumer[M any](b *ConsumerBuilder[M], tag string) *Consumer[M] {
	numConns := b.conn.NumConConns()
	idx := connIndexForTag(tag, numConns)
	mc := b.conn.ConConnAt(idx)

	c := &Consumer[M]{
		queue:               b.queue,
		tag:                 tag,
		concurrency:         b.concurrency,
		prefetch:            b.prefetch,
		maxInFlightBytes:    b.maxInFlightBytes,
		byteLimiter:         newByteLimiter(b.maxInFlightBytes),
		channelQoS:          b.channelQoS,
		priority:            b.priority,
		prioritySet:         b.prioritySet,
		exclusive:           b.exclusive,
		consumeArgs:         b.consumeArgs,
		onCancel:            b.onCancel,
		brokerAutoAck:       b.autoAck,
		handlerTimeout:      b.handlerTimeout,
		timeoutVerdict:      b.timeoutVerdict,
		depthSampleInterval: b.depthSampleInterval,
		maxRedeliveries:     b.maxRedeliveries,
		counterBDisabled:    b.counterBDisabled,
		codec:               b.c,
		cm:                  b.cm,
		tracer:              b.tracer,
		propagator:          otel.NewPropagator(),
		msgType:             metricsTypeName[M](),
		mc:                  mc,
		closedCh:            make(chan struct{}),
	}
	// Initialise counterState with an empty map; openDeliveryCh rotates this on every
	// channel open so "channel close resets counter B" holds without explicit cleanup.
	c.counterState.Store(&redeliveryCounter{})
	// Set the sampler interval in place (not via a struct literal) so the embedded
	// atomic counter is never copied.
	c.autoAckDropLog.every = defaultAutoAckLogEvery
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

// Bounded "reason" label vocabulary for consumer_cancelled_total (T49). The AMQP
// basic.cancel frame carries only the consumer tag (no human-readable reason), and
// using that unbounded tag as a metric label blew up Prometheus cardinality (one
// series per ctag-<uuidv7>). The metric records one of this closed-vocabulary class
// instead, classified by inspecting whether the queue still exists; the per-event
// tag is surfaced through OnCancel and the wrapped ErrConsumerCancelled where
// unbounded values are harmless.
const (
	// cancelReasonQueueDeleted: the source queue no longer exists (passive
	// declare returns 404 NOT_FOUND) — the operator deleted it.
	cancelReasonQueueDeleted = "queue_deleted"
	// cancelReasonExclusiveRevoked: the queue still exists, so the cancel came
	// from an exclusive-lock revocation / single-active-consumer handoff.
	cancelReasonExclusiveRevoked = "exclusive_revoked"
	// cancelReasonUnknown: the classification probe could not run (no live
	// channel) or returned a non-404 error.
	cancelReasonUnknown = "unknown"
)

// dlqNameSuffix is the conventional dead-letter-queue suffix warren's own DeadLetter
// topology appends to a source queue ("<source>.dlq"). WithQueueDepthSampler samples
// the DLQ under this name.
const dlqNameSuffix = ".dlq"

// minDepthSampleInterval is the floor WithQueueDepthSampler intervals clamp to (T52a).
// A sub-100ms poll adds avoidable queue.declare traffic to a connection that already
// serializes I/O without making the gauge meaningfully fresher.
const minDepthSampleInterval = 100 * time.Millisecond

// maxDepthSampleBackoff caps the exponential back-off applied while every probe in a
// sample fails (T52a), so a permanently-missing queue settles at one probe per 30s
// rather than churning a channel-open + 404-close at the base interval forever.
const maxDepthSampleBackoff = 30 * time.Second

// runDepthSampler periodically samples the source-queue and DLQ depths until ctx is
// cancelled (T52). It primes the gauges once immediately so they are populated before
// the first tick, then re-samples on a self-resetting timer. A context already
// cancelled when the goroutine starts skips even the priming sample, so a consumer
// torn down in the same breath as it starts issues no stray declare.
//
// The configured interval is clamped to minDepthSampleInterval (T52a) with a one-time
// warning. While every probe in a sample fails — a permanently-missing queue, or a
// socket down mid-reconnect — the delay backs off exponentially (capped at
// maxDepthSampleBackoff, never below the configured interval); the first sample that
// emits any gauge resets the cadence to the base interval.
func (c *Consumer[M]) runDepthSampler(ctx context.Context) {
	base := c.depthSampleInterval
	if base < minDepthSampleInterval {
		c.mc.opts.logger.Warningf(
			"warren: queue depth sampler interval %s is below the %s floor; clamped to the floor",
			base, minDepthSampleInterval,
		)
		base = minDepthSampleInterval
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	failures := 0
	if !c.sampleDepths() {
		failures++
	}
	timer := time.NewTimer(depthSampleDelay(base, failures))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if c.sampleDepths() {
				failures = 0
			} else {
				failures++
			}
			timer.Reset(depthSampleDelay(base, failures))
		}
	}
}

// depthSampleDelay returns the base interval after a successful sample (failures == 0)
// and an exponentially backed-off interval otherwise: the delay doubles per
// consecutive all-failed sample, capped at maxDepthSampleBackoff but never reduced
// below base (so a configured interval already slower than the cap is preserved). The
// ceiling also bounds the doubling so the shift can never overflow time.Duration.
func depthSampleDelay(base time.Duration, failures int) time.Duration {
	if failures <= 0 {
		return base
	}
	ceiling := maxDepthSampleBackoff
	if base > ceiling {
		ceiling = base
	}
	delay := base
	for i := 0; i < failures && delay < ceiling; i++ {
		delay *= 2
	}
	if delay > ceiling {
		delay = ceiling
	}
	return delay
}

// sampleDepths reads the source-queue depth (always) and the conventional
// "<queue>.dlq" dead-letter-queue depth (only when it exists) via passive declares,
// emitting the queue_depth and dlq_depth gauges. A queue that does not exist is
// skipped rather than reported as zero, so no phantom series appears for a consumer
// without a DLQ. It reports whether any gauge was emitted, so the caller can back off
// when a whole sample reaches nothing (broker down, source queue gone).
func (c *Consumer[M]) sampleDepths() bool {
	emitted := false
	if depth, ok := c.sampleQueueDepth(c.queue); ok {
		c.cm.SetQueueDepth(c.queue, depth)
		emitted = true
	}
	dlq := c.queue + dlqNameSuffix
	if depth, ok := c.sampleQueueDepth(dlq); ok {
		c.cm.SetDLQDepth(dlq, depth)
		emitted = true
	}
	return emitted
}

// sampleQueueDepth opens a short-lived channel and passively declares name to read
// its broker-side message count. It returns (0, false) when the channel cannot be
// opened, the channel does not expose a passive declare, the queue does not exist, or
// the broker rejects the declare. A fresh channel is used per call because the broker
// closes the channel on a failed passive declare (e.g. a 404 for a missing DLQ); the
// throwaway channel keeps that close from disturbing the delivery channel.
func (c *Consumer[M]) sampleQueueDepth(name string) (int64, bool) {
	topoCh, err := c.mc.openChannel()
	if err != nil {
		return 0, false
	}
	defer func() { _ = topoCh.Close() }()
	inspector, ok := topoCh.(passiveQueueInspector)
	if !ok {
		return 0, false
	}
	q, err := inspector.QueueDeclarePassive(name, false, false, false, false, nil)
	if err != nil {
		return 0, false
	}
	return int64(q.Messages), true
}

// passiveQueueInspector is the QueueDeclarePassive subset of *amqp091.Channel used
// to probe whether a queue still exists. A temporary channel opened by
// classifyBrokerCancel type-asserts to this; the topologyChannel interface does not
// expose it because only the cancel-classification path needs a passive declare.
type passiveQueueInspector interface {
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
}

// classifyBrokerCancel maps a broker basic.cancel into the bounded reason enum by
// opening a temporary channel and passively declaring the queue. A 404 means the
// queue was deleted; success means it still exists (so the cancel was an exclusive
// revocation); anything else (open failure, non-404 error, a channel that cannot
// passively declare) is unknown. The probe uses its own channel so a 404 close does
// not disturb the consumer's delivery channel.
func classifyBrokerCancel(mc *managedConn, queue string) string {
	ch, err := mc.openChannel()
	if err != nil {
		return cancelReasonUnknown
	}
	defer func() { _ = ch.Close() }()
	inspector, ok := ch.(passiveQueueInspector)
	if !ok {
		return cancelReasonUnknown
	}
	if _, perr := inspector.QueueDeclarePassive(queue, false, false, false, false, nil); perr != nil {
		if errors.Is(wrapAMQPError(perr), ErrNotFound) {
			return cancelReasonQueueDeleted
		}
		return cancelReasonUnknown
	}
	return cancelReasonExclusiveRevoked
}

// classifyCancel resolves the bounded reason class for a basic.cancel, skipping the
// broker round-trip when no observer would use the result. The class is consumed in
// exactly two places: the fallback warning log (only when onCancel is unset) and the
// consumer_cancelled_total metric. When onCancel is set AND the metrics sink is the
// NoOp default, the class is discarded by both — so the probe is pure overhead and we
// return cancelReasonUnknown without opening a channel.
//
// The type assertion deliberately matches only the bare NoOpConsumerMetrics default.
// A custom sink, or any decorator that embeds NoOp, falls through to probing — which
// is safe by design: over-probing only costs a broker round-trip on a rare cancel,
// never correctness, so the conservative default is to probe whenever in doubt.
func classifyCancel(mc *managedConn, queue string, onCancel func(string), cm metrics.ConsumerMetrics) string {
	if onCancel != nil {
		if _, isNoOp := cm.(metrics.NoOpConsumerMetrics); isNoOp {
			return cancelReasonUnknown
		}
	}
	return classifyBrokerCancel(mc, queue)
}

// brokerCancel pairs the two datums a basic.cancel response needs, so they cannot be
// transposed at a call site: tag is the unbounded consumer tag from the frame (it
// feeds OnCancel and the wrapped ErrConsumerCancelled, where unbounded values are
// harmless), and class is the bounded reason enum recorded on the metric label.
type brokerCancel struct {
	tag   string
	class string
}

// byteLimiter is the in-flight memory guardrail (T50). It caps the sum of
// len(Delivery.Body) across handlers running concurrently, bounding heap use when
// prefetch × concurrency × body-size would otherwise blow the process up. A nil
// *byteLimiter means the guardrail is disabled (acquire/release are no-ops).
//
// acquire blocks until n bytes fit under the limit; release frees them and wakes
// waiters. A message larger than the whole budget proceeds alone (when nothing else
// is in flight) rather than deadlocking forever.
type byteLimiter struct {
	mu       sync.Mutex
	cond     *sync.Cond
	limit    int64
	inflight int64
}

// newByteLimiter returns a limiter for the given byte budget, or nil (disabled)
// when limit <= 0.
func newByteLimiter(limit int64) *byteLimiter {
	if limit <= 0 {
		return nil
	}
	bl := &byteLimiter{limit: limit}
	bl.cond = sync.NewCond(&bl.mu)
	return bl
}

// acquire reserves n bytes, blocking until they fit under the limit. When nothing
// is in flight it always proceeds (even for n > limit) so a single oversized message
// cannot deadlock.
func (bl *byteLimiter) acquire(n int64) {
	if bl == nil {
		return
	}
	bl.mu.Lock()
	for bl.inflight > 0 && bl.inflight+n > bl.limit {
		bl.cond.Wait()
	}
	bl.inflight += n
	bl.mu.Unlock()
}

// release returns n reserved bytes and wakes any blocked acquirers.
func (bl *byteLimiter) release(n int64) {
	if bl == nil {
		return
	}
	bl.mu.Lock()
	bl.inflight -= n
	bl.mu.Unlock()
	bl.cond.Broadcast()
}

// buildConsumeArgs assembles the basic.consume argument table from the user-supplied
// Args plus the typed Priority option. It copies consumeArgs (never mutating the
// caller's map) and, when prioritySet, overlays x-priority so the typed Priority()
// wins over any x-priority slipped through Args(). Returns nil when there is nothing
// to send, matching amqp091's "no arguments" convention.
func buildConsumeArgs(consumeArgs Headers, prioritySet bool, priority int) amqp091.Table {
	if len(consumeArgs) == 0 && !prioritySet {
		return nil
	}
	args := make(amqp091.Table, len(consumeArgs)+1)
	maps.Copy(args, amqp091.Table(consumeArgs))
	if prioritySet {
		args["x-priority"] = priority
	}
	return args
}

// forwardCancelReason relays a broker basic.cancel reason (the consumer tag) onto a
// buffered (size-1) cancel channel without blocking the delivery pump. A full buffer
// means a cancel is already pending, so the first one wins. Shared by Consumer and
// BatchConsumer.
func forwardCancelReason(ch chan<- string, reason string) {
	select {
	case ch <- reason:
	default:
	}
}

// handleBrokerCancel runs the shared basic.cancel response, then drains in-flight
// handlers before returning so the caller (runConsume) leaves no goroutines behind.
func (c *Consumer[M]) handleBrokerCancel(wg *sync.WaitGroup, tag string) error {
	bc := brokerCancel{tag: tag, class: classifyCancel(c.mc, c.queue, c.onCancel, c.cm)}
	err := surfaceBrokerCancel(c.onCancel, c.mc.opts.logger, c.cm, c.queue, bc)
	wg.Wait()
	return err
}

// surfaceBrokerCancel is the shared basic.cancel response for Consumer and
// BatchConsumer (SPEC §6.3): fire OnCancel with the consumer tag (or warn when
// unset), increment the bounded consumer_cancelled_total{queue, reason} metric, and
// build the ErrConsumerCancelled the consumer goroutine returns. The wrapped error
// carries the tag (the only datum the frame provides); the library does NOT
// auto-redeclare the queue — operators usually deleted it on purpose.
// bc carries the unbounded consumer tag (bc.tag, fed to OnCancel and the wrapped
// error) and the bounded reason enum (bc.class, recorded on the metric label),
// kept in one struct so the two strings cannot be swapped at a call site.
func surfaceBrokerCancel(onCancel func(string), logger log.Logger, cm metrics.ConsumerMetrics, queue string, bc brokerCancel) error {
	if onCancel != nil {
		onCancel(bc.tag)
	} else {
		logger.Warningf(
			"warren: broker cancelled consumer %q on queue %q (basic.cancel, reason=%s); Consume returns ErrConsumerCancelled and does not auto-redeclare the queue",
			bc.tag, queue, bc.class,
		)
	}
	cm.RecordCancelled(queue, bc.class)
	return fmt.Errorf("%w: consumer tag %q", ErrConsumerCancelled, bc.tag)
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
	// Mark the consumer stopped on every return path (ctx cancel, broker basic.cancel,
	// fatal openDeliveryCh error) so ConsumerHealth.Active flips to false once the loop
	// exits — a silently-dead consumer must not report Active (T53).
	defer c.stopped.Store(true)

	// Queue-depth sampler (T52): run on its own context + waitgroup so every return
	// path below (ctx cancel, basic.cancel, a fatal openDeliveryCh error) stops and
	// joins the goroutine — no leak. It probes the broker on its own short-lived
	// channels and is independent of the delivery pump.
	if c.depthSampleInterval > 0 {
		samplerCtx, cancelSampler := context.WithCancel(ctx)
		var samplerWG sync.WaitGroup
		samplerWG.Add(1)
		go func() {
			defer samplerWG.Done()
			c.runDepthSampler(samplerCtx)
		}()
		defer func() {
			cancelSampler()
			samplerWG.Wait()
			// Drop the gauge series so a process that cycles consumers over distinct
			// queue names does not accumulate stale frozen series (T52b). Harmless for
			// a series that was never set (e.g. a consumer without a DLQ).
			c.cm.DeleteQueueDepth(c.queue)
			c.cm.DeleteDLQDepth(c.queue + dlqNameSuffix)
		}()
	}

	// cancelReasonCh relays broker basic.cancel notifications from the delivery pump
	// to this loop. Create it before registering the reconnect hook (which may open a
	// fresh pump) so every pump observes the same channel. Tests may pre-set it.
	if c.cancelReasonCh == nil {
		c.cancelReasonCh = make(chan string, 1)
	}

	// resubCh carries replacement subscriptions produced by the reconnect hook and
	// by Resume. Created here (unless a test pre-set it) so both producers and this
	// loop share one channel.
	if c.resubCh == nil {
		c.resubCh = make(chan deliverySub, 1)
	}
	resubCh := c.resubCh

	// Record the consumer-lifecycle ctx so Resume's re-subscribe pump is bound to
	// the consumer's lifetime, not to the ctx passed to Resume. Set before the first
	// openDeliveryCh (which sets c.live); any Resume that sees c.live != nil is
	// guaranteed to observe this write via pauseMu (T53).
	c.pauseMu.Lock()
	c.runCtx = ctx
	c.pauseMu.Unlock()

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
			notifyResubscribed(c.mc, c.cm, c.queue)
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

		case reason := <-c.cancelReasonCh:
			// Broker sent basic.cancel (queue deleted, exclusive lock revoked).
			// Fire OnCancel, record the metric, drain in-flight handlers, and return
			// ErrConsumerCancelled — never silently die (SPEC §6.3).
			return c.handleBrokerCancel(&wg, reason)

		case d, ok := <-cur.ch:
			if !ok {
				if c.testHookChannelClosed != nil {
					c.testHookChannelClosed()
				}
				// AMQP channel closed; wait for re-subscribe, a pending basic.cancel,
				// or ctx cancel. basic.cancel also closes the delivery stream, so the
				// cancel reason may already be buffered when this !ok fires.
				select {
				case <-ctx.Done():
					wg.Wait()
					return nil
				case reason := <-c.cancelReasonCh:
					return c.handleBrokerCancel(&wg, reason)
				case cur = <-resubCh:
				}
				continue
			}
			// Stamp the receipt time for ConsumerHealth.LastDeliveryAt (T53) the moment
			// a delivery arrives, before any decode/dispatch work.
			c.lastDeliveryNanos.Store(time.Now().UnixNano())
			sem <- struct{}{}
			// In-flight memory guardrail (T50): reserve this body's bytes before
			// dispatching, blocking the delivery pump (and thus prefetch refill) when
			// the budget is exhausted. The gauge mirrors the reserved total.
			//
			// Like the sem send above, acquire does not observe ctx.Done(): if the
			// budget is exhausted at shutdown, this loop parks until an in-flight
			// handler frees bytes. That is bounded by handler completion — the
			// dispatch goroutines run under ctx, so cancellation propagates to the
			// handlers, which return and release. Set HandlerTimeout to bound this
			// when handlers may block indefinitely.
			bodyBytes := int64(len(d.Body))
			c.byteLimiter.acquire(bodyBytes)
			c.cm.InFlightBytesAdd(c.queue, bodyBytes)
			wg.Add(1)
			c.inFlight.Add(1)
			// Capture the current channel's done signal so in-flight handlers
			// from this channel are cancelled if this channel closes mid-handler.
			chanDone := cur.done
			go func(raw amqp091.Delivery, chanDone <-chan struct{}, n int64) {
				defer wg.Done()
				defer c.inFlight.Add(-1)
				defer func() { <-sem }()
				defer func() {
					c.cm.InFlightBytesAdd(c.queue, -n)
					c.byteLimiter.release(n)
				}()
				c.dispatch(ctx, chanDone, raw, h, autoAck)
			}(d, chanDone, bodyBytes)
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

	// (Re)opening a delivery channel — initial connect or reconnect — means the
	// consumer is actively subscribed again, so clear any stale pause. The clear runs
	// under pauseMu, paired on the real path with publishing the fresh c.live, so a
	// Resume racing a reconnect observes a consistent (paused, live) pair: it either
	// sees paused==true with the OLD channel and re-subscribes there, or paused==false
	// and becomes a no-op — never paused==true alongside a half-replaced channel. This
	// keeps the Health snapshot accurate after a reconnect-during-pause and makes a
	// subsequent Resume a harmless no-op instead of a duplicate basic.consume with the
	// same consumer tag on a channel that already re-subscribed (T53).

	if c.deliverySubOverride != nil {
		c.clearPause()
		return *c.deliverySubOverride, nil
	}
	if c.deliveryCh != nil {
		// done is nil: channel-close detection is not exercised in basic unit tests.
		// cancelReasonCh (when set) is handled in runConsume's main select loop.
		c.clearPause()
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

	args := buildConsumeArgs(c.consumeArgs, c.prioritySet, c.priority)

	// ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args):
	//   - autoAck   = c.brokerAutoAck (AutoAck()): broker acks on dispatch (SPEC §6.3).
	//   - exclusive = c.exclusive (Exclusive()): refuse other consumers on this queue.
	//   - noLocal   = false: RabbitMQ silently ignores it; never exposed (SPEC §6 note).
	deliveries, err := ch.Consume(c.queue, c.tag, c.brokerAutoAck, c.exclusive, false, false, args)
	if err != nil {
		_ = ch.Close()
		return deliverySub{}, fmt.Errorf("warren: consumer subscribe: %w", wrapAMQPError(err))
	}

	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	// channelDone is closed when the AMQP channel physically closes, not when
	// the consumer ctx is cancelled. dispatch goroutines watch this to cancel
	// in-flight handler contexts with cause ErrChannelClosed.
	channelDone := make(chan struct{})
	var onceDone sync.Once
	closeChannelDone := func() { onceDone.Do(func() { close(channelDone) }) }

	// Record the physical-channel state so Pause/Resume can drive this channel, and
	// clear any stale pause in the SAME critical section so the (paused, live) pair is
	// observed atomically by a concurrent Resume/Pause (T53).
	c.pauseMu.Lock()
	c.live = &liveSub{ch: ch, closeCh: closeCh, cancelCh: cancelCh, done: channelDone, closeDone: closeChannelDone}
	c.paused.Store(false)
	c.pauseMu.Unlock()

	out := c.startPump(ctx, deliveries, closeCh, cancelCh, closeChannelDone)
	return deliverySub{ch: out, done: channelDone}, nil
}

// startPump launches the goroutine that fans one basic.consume's deliveries onto
// the returned out channel, watching the physical channel's close/cancel
// notifications. closeChannelDone fires (once) when the physical channel dies —
// but NOT on a graceful local cancel (Pause): leaving channelDone open lets
// in-flight handlers from this channel run to completion across a Pause/Resume
// cycle. Reused by both the initial/reconnect subscribe and Resume.
func (c *Consumer[M]) startPump(ctx context.Context, deliveries <-chan amqp091.Delivery, closeCh <-chan *amqp091.Error, cancelCh <-chan string, closeChannelDone func()) chan amqp091.Delivery {
	out := make(chan amqp091.Delivery, int(c.prefetch)) //nolint:gosec // G115: prefetch bounded
	go func() {
		defer close(out)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					// The delivery stream closed. Distinguish a genuine channel death
					// — which also signals closeCh and/or cancelCh — from a graceful
					// local cancel (Pause), which closes ONLY the delivery stream. Both
					// notifications and `deliveries` may become ready at once and Go
					// picks non-deterministically, so the !ok branch can win the race;
					// drain both non-blockingly to recover the real reason. Only a real
					// basic.cancel (ok=true) forwards a reason; a closed cancelCh
					// (ok=false) just means the channel is shutting down.
					//
					// This relies on an amqp091-go ordering guarantee: on an abnormal
					// channel shutdown it sends on the NotifyClose/NotifyCancel channels
					// BEFORE closing the delivery channels (both under its notify mutex),
					// so by the time we observe `deliveries` closed the death signal is
					// already buffered and the non-blocking drains above see it. If a
					// future amqp091-go bump reordered that, this drain could miss a real
					// death and skip closeChannelDone — TestStartPump_ChannelDeath_*
					// guards against it.
					death := false
					select {
					case <-closeCh:
						death = true
					default:
					}
					select {
					case tag, ok := <-cancelCh:
						if ok {
							forwardCancelReason(c.cancelReasonCh, tag)
						}
						death = true
					default:
					}
					// Close channelDone (cancelling in-flight handler contexts with
					// ErrChannelClosed) on a genuine death, OR when not paused. Skip it
					// ONLY for a graceful local cancel (paused, no death signal): there
					// the channel is alive, so in-flight handlers keep their contexts
					// and Resume re-subscribes on the same channel. Checking the death
					// signals — not just the paused flag — closes the window where a
					// real channel death races the Pause handshake.
					if death || !c.paused.Load() {
						closeChannelDone()
					}
					return
				}
				select {
				case out <- d:
				case <-ctx.Done():
					return
				}
			case tag, ok := <-cancelCh:
				// A real basic.cancel (ok=true) carries the consumer tag (queue
				// deleted, exclusive lock revoked): forward it so runConsume fires
				// OnCancel and returns ErrConsumerCancelled. A closed cancelCh
				// (ok=false) is just the channel shutting down on reconnect — treat it
				// like a NotifyClose so the consumer re-subscribes instead of dying.
				if ok {
					forwardCancelReason(c.cancelReasonCh, tag)
				}
				closeChannelDone()
				return
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
	return out
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
		if c.brokerAutoAck {
			// No-ack: the broker already removed this message, so there is no nack
			// to send and no DLX routing. Surface the silent drop as a sampled log.
			c.logAutoAckDrop(err)
			return
		}
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
	// Skipped under brokerAutoAck: redelivery bounding depends on Nacks the
	// no-ack client never sends, and the short-circuit itself would Nack.
	if !c.brokerAutoAck && c.maxRedeliveries > 0 && d.DeathCount() >= c.maxRedeliveries {
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
	if autoAck && !c.brokerAutoAck && c.maxRedeliveries > 0 && !c.counterBDisabled {
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
		c.settleVerdict(d, span, cs, counterBKey, handlerErr, elapsed, autoAck)
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
		c.settleVerdict(d, span, cs, counterBKey, handlerErr, elapsed, autoAck)

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
			switch {
			case c.brokerAutoAck:
				// No-ack: the broker already acked on dispatch, so the timeout
				// cannot nack. Record the would-be verdict label for parity with
				// manual-ack observability; the message is silently dropped.
				c.recordHandler("timeout_nack_no_requeue", elapsed)
			case c.timeoutVerdict == TimeoutNackRequeue:
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

// settleVerdict finalises one delivery after the handler returns: it records the
// handler metric and the span outcome, then settles the acknowledgement in one of
// three modes (see the brokerAutoAck field for the autoAck-param vs brokerAutoAck
// distinction):
//
//   - brokerAutoAck: the broker already acked on dispatch (AMQP no-ack), so no
//     ack/nack is sent; a non-nil handler error is surfaced as a sampled warning
//     (SPEC §6.3 "handler error semantics are bypassed").
//   - autoAck (Consume path): apply counter B, then d.AckIf the resulting verdict.
//   - neither (ConsumeRaw path): the handler owns the ack; record the "raw" label.
func (c *Consumer[M]) settleVerdict(d *Delivery[M], span otel.Span, cs *redeliveryCounter, counterBKey string, handlerErr error, elapsed time.Duration, autoAck bool) {
	if c.brokerAutoAck {
		c.recordHandler(handlerOutcome(handlerErr), elapsed)
		finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)
		if handlerErr != nil {
			c.logAutoAckDrop(handlerErr)
		}
		return
	}
	if autoAck {
		processedErr := c.applyCounterB(cs, counterBKey, handlerErr)
		c.recordHandler(handlerOutcome(processedErr), elapsed)
		finishConsumeSpan(span, consumeVerdictOutcome(processedErr), processedErr)
		_ = d.AckIf(processedErr)
		return
	}
	// ConsumeRaw: the handler is responsible for ack/nack.
	c.recordHandler("raw", elapsed)
	finishConsumeSpan(span, consumeVerdictOutcome(handlerErr), handlerErr)
}

// defaultAutoAckLogEvery is the sampling interval for the brokerAutoAck drop
// warning: the first drop logs, then one in every defaultAutoAckLogEvery, so a
// high-volume no-ack stream of failing handlers cannot flood the logs.
const defaultAutoAckLogEvery uint64 = 100

// logAutoAckDrop emits a rate-limited warning that a message was silently dropped
// under brokerAutoAck (handler error or decode failure). The error is reported by
// its closed-vocabulary type only — never its message — so a handler error that
// embeds payload or PII cannot leak into the logs (SPEC §8).
func (c *Consumer[M]) logAutoAckDrop(err error) {
	if emit, total := c.autoAckDropLog.sample(); emit {
		c.mc.opts.logger.Warningf(
			"warren: AutoAck consumer for queue %q dropped a message (handler/decode error %s); nacks are no-ops under AutoAck (total dropped: %d)",
			c.queue, consumeErrorType(err), total,
		)
	}
}

// dropSampler rate-limits a repeated warning. It emits the first occurrence and
// then one in every `every` occurrences (every<=1 emits every time), reporting the
// running total (logged + suppressed) so an operator can still gauge the drop rate.
// Safe for concurrent use.
type dropSampler struct {
	every uint64
	n     atomic.Uint64
}

// sample records one occurrence and reports whether it should be logged plus the
// running total of occurrences observed so far.
func (s *dropSampler) sample() (emit bool, total uint64) {
	total = s.n.Add(1)
	if s.every <= 1 {
		return true, total
	}
	return (total-1)%s.every == 0, total
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

// ConsumerHealth is a point-in-time snapshot of a consumer's runtime state,
// suitable for building Kubernetes liveness/readiness probes (T53). Health
// returns it only when the pinned connection is healthy; on a connection error
// Health returns (nil, err), since a snapshot would carry no meaningful state.
type ConsumerHealth struct {
	// Active is true when the consumer is started, its consume loop has not exited,
	// it is not closed, and it is not paused — i.e. it is receiving and dispatching
	// deliveries. It flips to false when the loop exits for any reason (ctx cancel,
	// a broker basic.cancel, or a fatal subscribe error), so a probe wired to Active
	// will not keep a silently-dead consumer in rotation.
	Active bool
	// Paused is true between Pause and Resume.
	Paused bool
	// LastDeliveryAt is the wall-clock time the most recent delivery was received
	// from the broker; the zero Time if none has arrived yet. A LastDeliveryAt that
	// stops advancing on a queue that should be busy is a liveness signal.
	LastDeliveryAt time.Time
	// InFlightHandlers is the number of handler invocations currently executing.
	InFlightHandlers int
}

// clearPause clears a stale pause flag under pauseMu. A fresh subscription means
// the consumer is subscribed again; doing it under the lock lets a concurrent
// Pause/Resume observe the cleared flag consistently with c.live (T53).
func (c *Consumer[M]) clearPause() {
	c.pauseMu.Lock()
	c.paused.Store(false)
	c.pauseMu.Unlock()
}

// isClosed reports whether Close has been called.
func (c *Consumer[M]) isClosed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

// snapshot builds a ConsumerHealth from the consumer's live atomics. It performs
// no I/O, so it is safe to call concurrently with Consume and from probe handlers.
func (c *Consumer[M]) snapshot() ConsumerHealth {
	var last time.Time
	if n := c.lastDeliveryNanos.Load(); n != 0 {
		last = time.Unix(0, n)
	}
	// Read paused once: deriving Active and Paused from two separate Loads would let a
	// concurrent Pause/Resume land between them and produce an inconsistent snapshot
	// with Active && Paused both true (T53).
	paused := c.paused.Load()
	return ConsumerHealth{
		Active:           c.started.Load() && !c.stopped.Load() && !c.isClosed() && !paused,
		Paused:           paused,
		LastDeliveryAt:   last,
		InFlightHandlers: int(c.inFlight.Load()),
	}
}

// Health verifies the consumer's pinned connection and, when healthy, returns a
// snapshot of the consumer's runtime state. On a connection error it returns
// (nil, err): the connection liveness check is the gate, and a zeroed snapshot
// alongside an error would be misleading (T53).
func (c *Consumer[M]) Health(ctx context.Context) (*ConsumerHealth, error) {
	check := c.mc.health
	if c.healthCheckOverride != nil {
		check = c.healthCheckOverride
	}
	if err := check(ctx); err != nil {
		return nil, err
	}
	snap := c.snapshot()
	return &snap, nil
}

// Pause issues a local basic.cancel so the broker stops delivering to this
// consumer, without closing the channel — in-flight handlers and their acks on
// that channel are unaffected, and the broker holds subsequent messages on the
// queue. Use it for graceful draining (e.g. a Kubernetes preStop hook) ahead of
// Close. Pause is idempotent: a second call while paused is a no-op. It errors
// before Consume/ConsumeRaw has started or after Close. Resume undoes it (T53).
func (c *Consumer[M]) Pause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.started.Load() {
		return fmt.Errorf("%w: Pause before Consume/ConsumeRaw", ErrInvalidOptions)
	}
	if c.isClosed() {
		return ErrAlreadyClosed
	}

	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.paused.Load() {
		return nil
	}
	if c.live == nil {
		return fmt.Errorf("%w: no live subscription to pause", ErrReconnecting)
	}
	// Mark paused before cancelling so the pump leaves channelDone open when the
	// delivery stream ends, keeping in-flight handlers alive.
	c.paused.Store(true)
	if err := c.live.ch.Cancel(c.tag, false); err != nil {
		c.paused.Store(false)
		return fmt.Errorf("warren: consumer pause: %w", wrapAMQPError(err))
	}
	return nil
}

// Resume re-issues basic.consume on the consumer's existing channel after a
// Pause, handing the running loop a fresh subscription. It is idempotent: a call
// while not paused is a no-op. It errors before Consume/ConsumeRaw has started or
// after Close. The ctx scopes only this call (its cancellation aborts the
// re-subscribe handshake); the resulting subscription is bound to the consumer
// lifetime — the ctx passed to Consume/ConsumeRaw — not to this ctx, so a
// request-scoped Resume ctx cannot silently stop delivery (T53).
//
// If the ctx is cancelled mid-handshake (after the basic.consume is issued but
// before the loop adopts it), Resume rolls the subscription back with a local
// basic.cancel and leaves the consumer paused, so the call is a clean no-op-retry
// rather than leaving an orphaned broker subscription.
func (c *Consumer[M]) Resume(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.started.Load() {
		return fmt.Errorf("%w: Resume before Consume/ConsumeRaw", ErrInvalidOptions)
	}
	if c.isClosed() {
		return ErrAlreadyClosed
	}

	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if !c.paused.Load() {
		return nil
	}
	sub, err := c.resubscribeLocked()
	if err != nil {
		return err
	}
	select {
	case c.resubCh <- sub:
		c.paused.Store(false)
		return nil
	case <-ctx.Done():
		// The Resume ctx was cancelled after the basic.consume was issued but before
		// the running loop adopted the new subscription. Roll back so we leave neither
		// an orphaned broker subscription nor an inconsistent Health (Paused while
		// actually subscribed): cancel the basic.consume just issued and stay paused,
		// so a later Resume is a clean retry. The pump started by resubscribeLocked is
		// bound to runCtx and exits when this local cancel closes its delivery stream
		// (paused, no death signal → channelDone stays open, in-flight handlers unharmed).
		if c.live != nil {
			_ = c.live.ch.Cancel(c.tag, false)
		}
		return ctx.Err()
	}
}

// resubscribeLocked re-issues basic.consume on the live channel and starts a fresh
// pump, reusing the channel's close/cancel notifications and done signal so the
// physical channel (and its in-flight acks) is preserved. The pump is bound to
// c.runCtx (the consumer lifecycle), not to the ctx passed to Resume, so a
// request-scoped Resume ctx cannot tear it down. Caller holds pauseMu.
func (c *Consumer[M]) resubscribeLocked() (deliverySub, error) {
	live := c.live
	if live == nil {
		return deliverySub{}, fmt.Errorf("%w: no live subscription to resume", ErrReconnecting)
	}
	// Rotate counter B state, mirroring openDeliveryCh: a fresh subscription resets
	// in-process redelivery counts.
	c.counterState.Store(&redeliveryCounter{})
	args := buildConsumeArgs(c.consumeArgs, c.prioritySet, c.priority)
	deliveries, err := live.ch.Consume(c.queue, c.tag, c.brokerAutoAck, c.exclusive, false, false, args)
	if err != nil {
		return deliverySub{}, fmt.Errorf("warren: consumer resume: %w", wrapAMQPError(err))
	}
	out := c.startPump(c.runCtx, deliveries, live.closeCh, live.cancelCh, live.closeDone)
	return deliverySub{ch: out, done: live.done}, nil
}

// Close signals the consumer to stop accepting new deliveries.
func (c *Consumer[M]) Close(_ context.Context) error {
	c.closeOnce.Do(func() { close(c.closedCh) })
	return nil
}
