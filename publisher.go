package warren

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// pubChannel is the AMQP channel interface required by Publisher.
// *amqp091.Channel satisfies this interface; tests may inject fakes.
type pubChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp091.Confirmation) chan amqp091.Confirmation
	NotifyReturn(c chan amqp091.Return) chan amqp091.Return
	NotifyClose(c chan *amqp091.Error) chan *amqp091.Error
	GetNextPublishSeqNo() uint64
	Close() error
}

// publisherEntry bundles a publisher channel with its per-lifetime confirm tracker.
// One entry is created per channel open; the tracker survives pool recycles until
// the channel closes.
//
// NOTE: publisherEntry is copied by value when the pool returns it from acquire.
// Any field that must be shared between Publish (which holds the copy) and the
// background goroutine (which holds the original) MUST be a pointer type so
// that both sides refer to the same memory. returnTagMap is such a field.
type publisherEntry struct {
	ch      pubChannel
	tracker *confirms.Tracker
	closeCh chan *amqp091.Error
	// returnTagMap maps MessageID (string) → delivery-tag (uint64) for in-flight
	// mandatory messages awaiting a possible basic.return. Stored as a pointer so
	// that the copy returned by pool.acquire and the original entry in the goroutine
	// share the same sync.Map. LoadAndDelete is called by the return goroutine when
	// a basic.return arrives; any entry not consumed by a return is deleted by the
	// caller after the confirm is received.
	returnTagMap *sync.Map
}

// publisherConnPool is a per-publisher-TCP-connection pool of AMQP channels.
// It mirrors channelPool's semaphore design and adds an in-flight counter for
// least-in-flight pool selection across the publisher connection set.
type publisherConnPool struct {
	tokens   chan struct{}
	free     chan publisherEntry
	inflight atomic.Int64
	openFn   func() (publisherEntry, error)
}

func newPublisherConnPool(size int, openFn func() (publisherEntry, error)) *publisherConnPool {
	p := &publisherConnPool{
		tokens: make(chan struct{}, size),
		free:   make(chan publisherEntry, size),
		openFn: openFn,
	}
	for range size {
		p.tokens <- struct{}{}
	}
	return p
}

// acquire returns a pooled entry and a release func. Returns ErrChannelPoolExhausted
// if ctx is cancelled before a semaphore slot is available.
func (p *publisherConnPool) acquire(ctx context.Context) (publisherEntry, func(), error) {
	select {
	case <-ctx.Done():
		return publisherEntry{}, nil, fmt.Errorf("%w: %w", ErrChannelPoolExhausted, ctx.Err())
	case <-p.tokens:
	}

	// Guard against panics in getOrOpen (which calls openFn): if openFn panics
	// the token would be lost permanently, shrinking the effective pool size
	// with each panic until all Publish calls block indefinitely. The defer
	// returns the token unconditionally unless the success or error path below
	// sets tokenConsumed to true. See LATER-04.
	tokenConsumed := false
	defer func() {
		if !tokenConsumed {
			p.tokens <- struct{}{}
		}
	}()

	entry, err := p.getOrOpen()
	if err != nil {
		return publisherEntry{}, nil, err
	}
	tokenConsumed = true
	return entry, func() { p.release(entry) }, nil
}

func (p *publisherConnPool) getOrOpen() (publisherEntry, error) {
	for {
		select {
		case entry := <-p.free:
			select {
			case <-entry.closeCh:
				_ = entry.ch.Close()
				continue
			default:
				return entry, nil
			}
		default:
			return p.openFn()
		}
	}
}

func (p *publisherConnPool) release(entry publisherEntry) {
	defer func() { p.tokens <- struct{}{} }()
	select {
	case <-entry.closeCh:
		_ = entry.ch.Close()
		return
	default:
	}
	select {
	case p.free <- entry:
	default:
		_ = entry.ch.Close()
	}
}

// drainFree closes and removes all idle entries from the free queue.
func (p *publisherConnPool) drainFree() {
	for {
		select {
		case entry := <-p.free:
			_ = entry.ch.Close()
		default:
			return
		}
	}
}

// drain discards idle entries. Called after reconnect so stale channels from
// the dead socket are never reused.
func (p *publisherConnPool) drain() { p.drainFree() }

// closeAll drains and closes all idle entries. Called by Publisher.Close.
func (p *publisherConnPool) closeAll() { p.drainFree() }

// openPublisherEntry opens a new AMQP channel on mc, enables publisher confirms,
// and starts a single goroutine that routes both basic.return and basic.ack/nack
// frames to the tracker. Using one goroutine for both frame types ensures that
// MarkReturned is always called before the corresponding Ack, which is required
// for correct ErrUnroutable resolution.
func (mc *managedConn) openPublisherEntry(poolSize int, onReturn func(amqp091.Return)) (publisherEntry, error) {
	mc.mu.RLock()
	raw := mc.raw
	mc.mu.RUnlock()
	if raw == nil {
		return publisherEntry{}, ErrNotConnected
	}
	ch, err := raw.Channel()
	if err != nil {
		return publisherEntry{}, wrapAMQPError(err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return publisherEntry{}, wrapAMQPError(err)
	}
	tracker := confirms.New()
	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))

	buf := min(
		// Cap at channelPoolSizeMax: validation already rejects poolSize above this
		// value, but openPublisherEntry may be called with internal pool sizes before
		// validation runs, so we enforce the ceiling here too to keep both in sync.
		max(poolSize, 8), channelPoolSizeMax)

	entry := publisherEntry{
		ch:           ch,
		tracker:      tracker,
		closeCh:      closeCh,
		returnTagMap: new(sync.Map),
	}

	confirmCh := ch.NotifyPublish(make(chan amqp091.Confirmation, buf))
	// returnCh is intentionally unbuffered. amqp091's per-connection reader goroutine
	// delivers basic.return frames via a blocking channel send, so the reader stalls
	// until this goroutine receives. That serialises the sequence:
	//   basic.return received → MarkReturned called → reader continues → basic.ack dispatched
	// guaranteeing that MarkReturned always precedes Ack/Nack in the select loop below.
	// The stall is O(1) (map lookup + write) and only occurs on the error path (mandatory
	// unroutable messages). A buffered channel would let the reader dispatch the ack before
	// this goroutine processes the return; with a flat two-case select the scheduler could
	// then pick confirmCh first, silently losing ErrUnroutable ~50 % of the time.
	returnCh := ch.NotifyReturn(make(chan amqp091.Return))

	go func() {
		for {
			select {
			case ret, ok := <-returnCh:
				if !ok {
					returnCh = nil
					continue
				}
				// Look up the delivery tag that corresponds to this returned message.
				// returnTagMap is populated by publishOnce and PublishBatch before each
				// publish; LoadAndDelete removes the entry so it is not double-processed.
				// The MessageID is always set (applyDefaults stamps a UUIDv7 if empty).
				// A broker that omits MessageId in basic.return cannot be correlated;
				// the subsequent ack resolves with nil (success) rather than ErrUnroutable.
				// In practice RabbitMQ always echoes message properties in basic.return.
				if ret.MessageId != "" {
					if v, loaded := entry.returnTagMap.LoadAndDelete(ret.MessageId); loaded { //nolint:gocritic // always non-nil
						tag := v.(uint64) //nolint:forcetypeassert // only uint64 is stored
						if onReturn != nil {
							onReturn(ret)
						}
						tracker.MarkReturned(tag, ret.ReplyCode)
					}
				}
			case c, ok := <-confirmCh:
				if !ok {
					tracker.CloseAll()
					return
				}
				// amqp091-go fans out basic.ack/nack multiple=true into individual
				// Confirmations per delivery tag, so Multiple is always false here.
				if c.Ack {
					tracker.Ack(c.DeliveryTag, false)
				} else {
					tracker.Nack(c.DeliveryTag, false)
				}
			}
		}
	}()

	return entry, nil
}

// defaultConfirmTimeout is the internal default when no ConfirmTimeout builder
// option is set. T13 exposes this via PublisherBuilder.ConfirmTimeout.
const defaultConfirmTimeout = 30 * time.Second

// Publisher publishes typed AMQP messages to the broker.
//
// Publisher is safe for concurrent use by multiple goroutines.
type Publisher[M any] struct {
	conn       *Connection
	pools      []*publisherConnPool
	mcs        []*managedConn
	exchange   string
	routingKey string
	// msgType is the message_type metrics label value (Go type name of M),
	// computed once at build time.
	msgType        string
	codec          codec.Codec
	pm             metrics.PublisherMetrics
	tracer         otel.Tracer
	propagator     otel.Propagator
	confirmTimeout time.Duration
	mandatory      bool
	onReturn       func(Return)
	publishTimeout time.Duration
	// publishBatchMaxSize is validated at PublishBatch-time only (T22).
	publishBatchMaxSize int
	// maxMessageSizeBytes is the per-publish payload cap (0 disables; default 16 MiB).
	// Enforced in Publish after Encode, before any channel is opened.
	maxMessageSizeBytes int
	retryPolicy         *RetryPolicy
	stampUserID         bool
	// authUser is the identity from conn.AuthenticatedUser(); used for UserID
	// validation and StampUserID without holding the conn reference per-publish.
	authUser string

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// callOnReturn is called by the entry's return listener goroutine when a
// basic.return frame arrives. It converts the raw frame and invokes the
// user-supplied OnReturn callback (if any).
func (p *Publisher[M]) callOnReturn(r amqp091.Return) {
	if p.onReturn == nil {
		return
	}
	var exp time.Duration
	if ms := r.Expiration; ms != "" {
		if ms64, err := strconv.ParseInt(ms, 10, 64); err == nil {
			exp = time.Duration(ms64) * time.Millisecond
		}
	}
	p.onReturn(Return{
		ReplyCode:  r.ReplyCode,
		ReplyText:  r.ReplyText,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
		Properties: ReturnedProperties{
			ContentType:     r.ContentType,
			ContentEncoding: r.ContentEncoding,
			Headers:         Headers(r.Headers),
			DeliveryMode:    deliveryModeFromWire(r.DeliveryMode), // SPEC §6.5: wire 2→Persistent, else Transient
			Priority:        r.Priority,
			CorrelationID:   r.CorrelationId,
			ReplyTo:         r.ReplyTo,
			Expiration:      exp,
			MessageID:       r.MessageId,
			Timestamp:       r.Timestamp,
			Type:            r.Type,
			UserID:          r.UserId,
			AppID:           r.AppId,
		},
	})
}

// Health returns nil if the publisher's underlying connection is live.
func (p *Publisher[M]) Health(ctx context.Context) error {
	return p.conn.Health(ctx)
}

// Close drains all in-flight Publish calls and releases pool resources.
// Returns ErrAlreadyClosed if called more than once.
func (p *Publisher[M]) Close(_ context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	p.closed = true
	p.mu.Unlock()

	p.wg.Wait()
	for _, pool := range p.pools {
		pool.closeAll()
	}
	return nil
}

// Publish encodes msg and sends it to the broker. It blocks until the broker
// sends a publisher confirm (basic.ack or basic.nack).
//
// Publish is safe for concurrent use by multiple goroutines.
func (p *Publisher[M]) Publish(ctx context.Context, msg Message[M]) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()

	if p.publishTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}

	// Open the publish span first so it wraps encode failures too. The span is
	// ended in every termination path via defer — including a propagating codec
	// panic (encodeMsg recovers it into ErrInvalidMessage, so it does not
	// propagate, but the defer is the backstop regardless).
	ctx, span := p.tracer.Start(ctx, p.exchange+" publish", p.publishSpanAttrs()...)
	defer span.End()

	msg, body, err := p.encodeMsg(msg)
	if err != nil {
		finishPublishSpan(span, err)
		return err
	}

	span.SetAttributes(
		semconv.MessagingMessageID(msg.MessageID),
		semconv.MessagingMessageBodySize(len(body)),
	)
	if msg.CorrelationID != "" {
		span.SetAttributes(semconv.MessagingMessageConversationID(msg.CorrelationID))
	}

	// Inject the trace context into the message headers before any frame is
	// written, so it travels with basic.publish and survives any DLX bounce.
	msg.Headers = p.injectTrace(ctx, msg.Headers)

	var attempt int
	for {
		err := p.publishOnce(ctx, msg, body)
		if err == nil {
			finishPublishSpan(span, nil)
			return nil
		}
		if p.retryPolicy == nil || !IsTransient(err) {
			finishPublishSpan(span, err)
			return err
		}
		if p.retryPolicy.Retries > 0 && attempt >= p.retryPolicy.Retries {
			finishPublishSpan(span, err)
			return err
		}
		attempt++
		p.pm.RecordRetry(p.exchange, retryReason(err))
		d := p.retryPolicy.NextBackoff(attempt)
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			finishPublishSpan(span, err)
			return err
		case <-timer.C:
		}
	}
}

// publishSpanAttrs builds the static span attributes known before encoding
// (SPEC §6.9). network.peer.* is included only when a Connection is present.
func (p *Publisher[M]) publishSpanAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("rabbitmq"),
		semconv.MessagingDestinationName(p.exchange),
		semconv.MessagingOperationTypeKey.String("publish"),
	}
	if p.conn != nil {
		if host, port, ok := p.conn.peerAddress(); ok {
			attrs = append(attrs, semconv.NetworkPeerAddress(host))
			if port > 0 {
				attrs = append(attrs, semconv.NetworkPeerPort(port))
			}
		}
	}
	return attrs
}

// injectTrace returns a fresh headers map carrying the W3C trace context from
// ctx merged onto the caller's headers, preserving any caller-supplied
// traceparent/tracestate (last-wins per SPEC §6.9). It clones before injecting
// and only when ctx carries a live span: on the plain-codec path encodeMsg
// returns the caller's own Headers map, so mutating it in place would write the
// injected traceparent back into the caller's map — a caller reusing one map
// across publishes would then carry the first publish's traceparent into every
// later publish (the last-wins restore treats the stale value as caller-supplied),
// silently stitching unrelated publishes into one trace.
func (p *Publisher[M]) injectTrace(ctx context.Context, headers Headers) Headers {
	if !p.propagator.ActiveContext(ctx) {
		return headers
	}
	// maps.Clone(nil) returns nil, so allocate explicitly for the no-headers case.
	out := maps.Clone(headers)
	if out == nil {
		out = make(Headers, 2)
	}
	callerTP, hasTP := out[otel.HeaderTraceParent]
	callerTS, hasTS := out[otel.HeaderTraceState]
	p.propagator.Inject(ctx, out)
	if hasTP {
		out[otel.HeaderTraceParent] = callerTP
	}
	if hasTS {
		out[otel.HeaderTraceState] = callerTS
	}
	return out
}

// finishPublishSpan stamps the terminal outcome on the publish span: the
// messaging.rabbitmq.outcome attribute always, and on failure the error.type
// attribute, an Error status, and a recorded error (SPEC §6.9).
//
// The codec-encode / client-validation class (ErrInvalidMessage) is the one
// publish error whose text can be payload-derived: a caller-supplied custom
// Codec.Encode may embed the message body in its error. That class is reduced to
// the sentinel label on both the status description and the recorded error —
// mirroring the consume path's redactedSpanError — so SPEC §8 ("never leak
// message content into observability") holds uniformly, while errors.Is-based
// backends still unwrap to the original sentinel. Every other publish error is a
// framework/broker diagnostic (routing, confirms, channel state) with no message
// content, so its reason text is kept verbatim — it is useful when debugging.
func finishPublishSpan(span otel.Span, err error) {
	outcome, errType := publishOutcome(err)
	span.SetAttributes(attribute.String("messaging.rabbitmq.outcome", outcome))
	if err == nil {
		span.SetStatus(otelcodes.Ok, "")
		return
	}
	span.SetAttributes(semconv.ErrorTypeKey.String(errType))
	// Invariant: this predicate agrees with publishOutcome's first-match switch for
	// every real path. ErrInvalidMessage arises only in encodeMsg (codec encode /
	// header / UserID validation), which returns before any broker interaction, so
	// it is never joined with a broker sentinel (ErrUnroutable, ErrPublishNacked,
	// ...). On a contrived multi-sentinel error the code still degrades safely: it
	// redacts (the conservative choice) and errType remains whatever publishOutcome
	// reported, so the status description and recorded label stay consistent.
	if errors.Is(err, ErrInvalidMessage) {
		span.SetStatus(otelcodes.Error, errType)
		span.RecordError(redactedSpanError{label: errType, err: err})
		return
	}
	span.SetStatus(otelcodes.Error, err.Error())
	span.RecordError(err)
}

// publishOutcome maps a publish error to the (outcome, error.type) pair used on
// the publish span. The outcome mirrors the publisher_publish_seconds{outcome}
// metric label space; error.type is the sentinel name for assertive alerting.
func publishOutcome(err error) (outcome, errorType string) {
	switch {
	case err == nil:
		return "ack", ""
	case errors.Is(err, ErrUnroutable):
		return "return", "ErrUnroutable"
	case errors.Is(err, ErrConfirmTimeout):
		return "timeout", "ErrConfirmTimeout"
	case errors.Is(err, ErrPublishNacked):
		return "nack", "ErrPublishNacked"
	case errors.Is(err, ErrMessageTooLarge):
		return "too_large", "ErrMessageTooLarge"
	case errors.Is(err, ErrChannelPoolExhausted):
		return "pool_exhausted", "ErrChannelPoolExhausted"
	case errors.Is(err, ErrConnectionBlocked):
		return "blocked", "ErrConnectionBlocked"
	case errors.Is(err, ErrInvalidMessage):
		return "error", "ErrInvalidMessage"
	case errors.Is(err, ErrChannelClosed):
		return "error", "ErrChannelClosed"
	case errors.Is(err, ErrReconnecting):
		return "error", "ErrReconnecting"
	default:
		return "error", "error"
	}
}

// encodeMsg applies defaults, validates headers, stamps/validates UserID, encodes
// the body, and enforces the per-message size cap. It is the single choke-point
// for all client-side validation so that Publish and PublishBatch stay in sync.
// Returns the (possibly mutated) message, the encoded body, and any validation error.
func (p *Publisher[M]) encodeMsg(msg Message[M]) (Message[M], []byte, error) {
	if err := msg.applyDefaults(p.codec); err != nil {
		return msg, nil, err
	}
	if err := msg.validateHeaders(); err != nil {
		return msg, nil, err
	}

	// StampUserID: auto-set UserID from the authenticated connection identity.
	if p.stampUserID && msg.UserID == "" {
		msg.UserID = p.authUser
	}

	// Client-side UserID validation: if caller set a UserID that doesn't match
	// the connection identity, reject locally without writing a publish frame.
	// This prevents the 406-channel-close footgun from a mismatched stamp.
	// Note: the mismatch value is intentionally omitted from the error string to
	// prevent it from leaking into log labels, Prometheus metrics, or OTEL spans.
	if msg.UserID != "" && p.authUser != "" && msg.UserID != p.authUser {
		return msg, nil, fmt.Errorf("%w: UserID field does not match the authenticated connection identity", ErrInvalidMessage)
	}

	if msg.Body == nil {
		return msg, nil, fmt.Errorf("%w: Body must not be nil", ErrInvalidMessage)
	}

	// Client-side Delay validation: x-delay is a signed 32-bit millisecond count
	// (see buildPublishing). A Delay beyond that ceiling (~24.8 days, the plugin's
	// own maximum) would overflow int32 to a negative value and be delivered
	// immediately/undefined — reject it locally instead of emitting a corrupt header.
	if msg.Delay.Milliseconds() > math.MaxInt32 {
		return msg, nil, fmt.Errorf("%w: Delay %s exceeds the x-delay ceiling of %d ms (~24.8 days)",
			ErrInvalidMessage, msg.Delay, int64(math.MaxInt32))
	}

	body, ceHeaders, ceContentType, err := safeEncodeBody(p.codec, msg.Body)
	if err != nil {
		return msg, nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	// A HeaderCodec (e.g. CloudEvents binary mode) returns headers and a
	// content-type that travel alongside the body. Merge headers into a fresh map
	// so the caller's Headers map is never mutated; codec headers win on conflict.
	if len(ceHeaders) > 0 {
		// Codec-returned headers bypassed the earlier validateHeaders pass; validate
		// (and coerce) them before merging so a third-party HeaderCodec cannot inject
		// a value type amqp091 fails to serialise at publish time.
		if err := validateHeaders(Headers(ceHeaders)); err != nil {
			return msg, nil, err
		}
		merged := make(Headers, len(msg.Headers)+len(ceHeaders))
		maps.Copy(merged, msg.Headers)
		maps.Copy(merged, ceHeaders)
		msg.Headers = merged
	}
	// The codec's content-type (e.g. CloudEvents datacontenttype) is authoritative
	// for the body it produced, so it overrides any default or caller value.
	if ceContentType != "" {
		msg.ContentType = ceContentType
	}

	// Local payload guardrail: reject before opening a channel so the publisher
	// never allocates broker-side frame buffers for a body it knows is too large.
	if p.maxMessageSizeBytes > 0 && len(body) > p.maxMessageSizeBytes {
		p.recordPublish(p.exchange, "too_large", 0)
		return msg, nil, fmt.Errorf("%w: encoded body is %d bytes (cap %d)", ErrMessageTooLarge, len(body), p.maxMessageSizeBytes)
	}

	return msg, body, nil
}

// safeEncodeBody calls encodeBody, recovering from a codec panic per the T09
// panic-safety contract (mirrors safeDecodeConsumer on the consume path). The
// recovered value is reported by type only — never by content — so a custom
// codec cannot leak message bytes into an error string, log, or span. The
// caller wraps the returned error in ErrInvalidMessage.
func safeEncodeBody(c codec.Codec, body any) (out []byte, headers map[string]any, contentType string, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, headers, contentType = nil, nil, ""
			err = fmt.Errorf("codec panic: %T", r)
		}
	}()
	return encodeBody(c, body)
}

// encodeBody encodes the body, returning any AMQP headers and content-type a
// HeaderCodec wants to travel alongside it. For a plain Codec the headers result
// is nil and the content-type is empty.
func encodeBody(c codec.Codec, body any) ([]byte, map[string]any, string, error) {
	if hc, ok := c.(codec.HeaderCodec); ok {
		return hc.EncodeWithHeaders(body)
	}
	b, err := c.Encode(body)
	return b, nil, "", err
}

// recordPublish records a publish outcome, supplying the routing-key and
// message-type metrics label values so an enabled publisher_publish_seconds
// histogram carries them.
func (p *Publisher[M]) recordPublish(exchange, outcome string, d time.Duration) {
	p.pm.RecordPublish(exchange, p.routingKey, p.msgType, outcome, d)
}

// publishOnce performs a single publish attempt (no retry logic).
func (p *Publisher[M]) publishOnce(ctx context.Context, msg Message[M], body []byte) error {
	exchange := p.exchange
	start := time.Now()

	pool, mc := p.selectPool()
	if err := mc.waitBarrier(ctx); err != nil {
		return err
	}

	entry, release, err := pool.acquire(ctx)
	if err != nil {
		return err
	}
	pool.inflight.Add(1)
	p.pm.InFlightAdd(exchange, 1)
	defer func() {
		pool.inflight.Add(-1)
		p.pm.InFlightAdd(exchange, -1)
		release()
	}()

	pub := buildPublishing(msg, body)

	deliveryTag := entry.ch.GetNextPublishSeqNo()
	// Register MessageID → deliveryTag only when mandatory is set: basic.return
	// frames can only arrive for mandatory publishes. For non-mandatory publishers
	// the broker never sends basic.return, so storing and deleting would be pure
	// sync.Map churn on the hot path with no benefit.
	if p.mandatory {
		entry.returnTagMap.Store(msg.MessageID, deliveryTag)
		defer entry.returnTagMap.Delete(msg.MessageID)
	}

	if err := entry.tracker.Register(deliveryTag); err != nil {
		p.recordPublish(exchange, "error", time.Since(start))
		return p.mapConfirmError(err)
	}

	if err := entry.ch.PublishWithContext(ctx, exchange, p.routingKey, p.mandatory, false, pub); err != nil {
		entry.tracker.Cancel(deliveryTag)
		p.recordPublish(exchange, "error", time.Since(start))
		return wrapAMQPError(err)
	}

	waitErr := entry.tracker.Wait(ctx, deliveryTag, p.confirmTimeout)
	if waitErr != nil {
		p.recordPublish(exchange, "error", time.Since(start))
		return p.mapConfirmError(waitErr)
	}

	p.recordPublish(exchange, "success", time.Since(start))
	return nil
}

// PublishBatch publishes all messages in msgs on a single AMQP channel, preserving
// input order (RabbitMQ's per-channel ordering guarantee). It never short-circuits:
// even if some messages fail client-side validation, valid messages are still
// published and confirmed.
//
// An empty batch (len(msgs) == 0) returns (nil, nil) without contacting the broker.
//
// If len(msgs) exceeds the configured PublishBatchMaxSize (default 1024),
// PublishBatch returns (nil, ErrBatchTooLarge) immediately without any broker work.
//
// Per-message outcomes are in []PublishResult, one slot per input. Result.Err may be:
//   - nil (broker confirmed and routed)
//   - ErrInvalidMessage (client-side header validation, nil Body, or encode failure)
//   - ErrUnroutable (broker returned the message via basic.return — mandatory+no binding)
//   - ErrPublishNacked (broker sent basic.nack, e.g. overflow=reject-publish)
//   - ErrChannelClosed (channel died before confirm arrived)
//   - ErrConfirmTimeout (no confirm received within the configured ConfirmTimeout)
//
// If any message fails, the overall error wraps ErrPartialBatch. Note that when a
// connection-level error occurs (e.g. ErrReconnecting, ErrChannelPoolExhausted),
// results is nil and err is the connection-level error — no per-message results are
// available because no messages were sent to the broker.
//
// # Mandatory delivery
//
// PublishBatch fully supports publishers configured with Mandatory(). When a message
// has no matching binding the broker sends basic.return (before the basic.ack). The
// result for that slot is ErrUnroutable (wrapped with the broker reply code so
// AMQPCode can retrieve it). Messages without a routing failure are unaffected.
// Correlation is performed by MessageID: applyDefaults stamps a UUIDv7 when
// Message.MessageID is empty, so every message always has a unique key.
//
// Note: each message in the batch must have a unique MessageID. When two messages
// share an explicit MessageID the second Store silently overwrites the first in the
// return-correlation map, causing undefined ErrUnroutable attribution for mandatory
// publishers. The library does not enforce uniqueness at call time; callers are
// responsible for assigning distinct MessageIDs (or leaving them empty for
// auto-stamping).
//
// # PublishTimeout
//
// PublishTimeout configured on the publisher is NOT applied to PublishBatch.
// If a per-batch deadline is needed, wrap ctx with context.WithTimeout before
// calling PublishBatch.
//
// # Channel-close recovery
//
// Per-message ErrChannelClosed does NOT distinguish "broker persisted" from
// "broker did not receive". Retry produces duplicates when the broker persisted
// but the ack was lost. PublishRetry does NOT apply to PublishBatch — chunking
// and partial-retry are the caller's responsibility because the right strategy
// is workload-specific. Consumers MUST be idempotent per SPEC §6.2.1.
//
// # PublishRetry
//
// PublishRetry configured on the publisher is intentionally ignored for
// PublishBatch. Retry semantics across a multi-message batch require the caller
// to understand which messages were persisted vs lost, so automatic retry would
// produce uncontrolled duplicates. Use PublishRetry only with Publish (single
// message).
func (p *Publisher[M]) PublishBatch(ctx context.Context, msgs []Message[M]) ([]PublishResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	maxSize := p.publishBatchMaxSize
	if maxSize <= 0 {
		maxSize = 1024
	}
	if len(msgs) > maxSize {
		return nil, ErrBatchTooLarge
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrAlreadyClosed
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()

	results := make([]PublishResult, len(msgs))

	// Step 1: client-side validation and encoding for all messages up front.
	// encodeMsg is the single choke-point for validation so Publish and
	// PublishBatch stay in sync. This avoids holding the channel while doing
	// CPU work (encoding happens before channel acquisition in step 2).
	type ready struct {
		msg  Message[M]
		body []byte
		err  error
	}
	encoded := make([]ready, len(msgs))

	for i, msg := range msgs {
		encMsg, body, err := p.encodeMsg(msg)
		if err != nil {
			encoded[i].err = err
			continue
		}
		encoded[i].msg = encMsg
		encoded[i].body = body
	}

	// Propagate client-side failures to results immediately.
	allFailed := true
	for i, e := range encoded {
		if e.err != nil {
			results[i].Err = e.err
		} else {
			allFailed = false
		}
	}

	if allFailed {
		return results, ErrPartialBatch
	}

	// Step 2: acquire ONE channel from the pool. All valid messages pipeline on it
	// to guarantee per-channel in-order delivery.
	pool, mc := p.selectPool()
	if err := mc.waitBarrier(ctx); err != nil {
		return nil, err
	}

	entry, release, err := pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	pool.inflight.Add(1)
	defer func() {
		pool.inflight.Add(-1)
		release()
	}()

	// Step 3: register + publish all valid messages. Track which delivery tags
	// correspond to which result indices so we can await them in order.
	type tagIdx struct {
		tag uint64
		idx int
	}
	published := make([]tagIdx, 0, len(msgs))

	for i, e := range encoded {
		if e.err != nil {
			continue // already failed client-side
		}

		pub := buildPublishing(e.msg, e.body)
		deliveryTag := entry.ch.GetNextPublishSeqNo()

		// Register MessageID → deliveryTag only when mandatory is set (same
		// rationale as publishOnce): basic.return frames only arrive for mandatory
		// publishes. For non-mandatory publishers this is dead code.
		if p.mandatory {
			entry.returnTagMap.Store(e.msg.MessageID, deliveryTag)
		}

		if regErr := entry.tracker.Register(deliveryTag); regErr != nil {
			// Channel already closed; no publish will happen, so no return can
			// arrive. Clean up the returnTagMap entry immediately (mandatory only)
			// rather than relying on step 4.5 — the comment there applies only to
			// messages that were actually published to the broker.
			if p.mandatory {
				entry.returnTagMap.Delete(e.msg.MessageID)
			}
			results[i].Err = p.mapConfirmError(regErr)
			continue
		}

		if pubErr := entry.ch.PublishWithContext(ctx, p.exchange, p.routingKey, p.mandatory, false, pub); pubErr != nil {
			entry.tracker.Cancel(deliveryTag)
			results[i].Err = wrapAMQPError(pubErr)
			continue
		}

		published = append(published, tagIdx{tag: deliveryTag, idx: i})
	}

	// Step 4: wait for confirms on all successfully-published messages and record
	// per-message metrics so batch publishes are visible to Prometheus/operators.
	// Each latency sample is measured from the moment Wait begins for that message,
	// giving a per-message confirm-wait duration rather than an accumulated batch total.
	p.pm.InFlightAdd(p.exchange, int64(len(published)))
	defer p.pm.InFlightAdd(p.exchange, -int64(len(published)))

	for _, ti := range published {
		msgStart := time.Now()
		if waitErr := entry.tracker.Wait(ctx, ti.tag, p.confirmTimeout); waitErr != nil {
			results[ti.idx].Err = p.mapConfirmError(waitErr)
			p.recordPublish(p.exchange, "error", time.Since(msgStart))
		} else {
			p.recordPublish(p.exchange, "success", time.Since(msgStart))
		}
	}

	// Step 4.5: clean up returnTagMap entries for mandatory publishers only.
	// For unroutable messages the goroutine already called LoadAndDelete when the
	// basic.return arrived — those entries are gone by the time Wait returns
	// (basic.return always precedes basic.ack; returnCh is unbuffered so MarkReturned
	// is always called before the paired ack is processed; see comment above). For
	// successfully-routed messages no return ever
	// arrives, so their entries remain and must be removed here. For messages that
	// failed at PublishWithContext, their entries were also never consumed by a
	// return, so they are cleaned up here too. Messages that failed at Register had
	// their entries removed immediately above (not here).
	// Delete on a missing key is a no-op in sync.Map.
	if p.mandatory {
		for _, e := range encoded {
			if e.err == nil {
				entry.returnTagMap.Delete(e.msg.MessageID)
			}
		}
	}

	// Step 5: check for any failure and wrap ErrPartialBatch if needed.
	for _, r := range results {
		if r.Err != nil {
			return results, ErrPartialBatch
		}
	}

	return results, nil
}

// selectPool returns the publisher connection pool with the lowest in-flight count.
func (p *Publisher[M]) selectPool() (*publisherConnPool, *managedConn) {
	minFlight := int64(-1)
	minIdx := 0
	for i, pool := range p.pools {
		n := pool.inflight.Load()
		if minFlight < 0 || n < minFlight {
			minFlight = n
			minIdx = i
		}
	}
	return p.pools[minIdx], p.mcs[minIdx]
}

// mapConfirmError translates internal confirms sentinel errors to public sentinels.
func (p *Publisher[M]) mapConfirmError(err error) error {
	switch {
	case errors.Is(err, confirms.ErrTimeout):
		return ErrConfirmTimeout
	case errors.Is(err, confirms.ErrNacked):
		return ErrPublishNacked
	case errors.Is(err, confirms.ErrClosed):
		return ErrChannelClosed
	default:
		var ue *confirms.UnroutableError
		if errors.As(err, &ue) {
			return wrapCode(ue.ReplyCode, ErrUnroutable)
		}
		return err
	}
}

// retryReason maps an error to the publisher_retry_total reason label.
func retryReason(err error) string {
	switch {
	case errors.Is(err, ErrPublishNacked):
		return "nacked"
	case errors.Is(err, ErrConfirmTimeout):
		return "confirm_timeout"
	case errors.Is(err, ErrChannelClosed):
		return "channel_closed"
	case errors.Is(err, ErrChannelPoolExhausted):
		return "pool_exhausted"
	case errors.Is(err, ErrConnectionBlocked):
		return "blocked"
	case errors.Is(err, ErrReconnecting):
		return "reconnecting"
	default:
		return "network"
	}
}

// buildPublishing converts a Message[M] into an amqp091.Publishing frame.
func buildPublishing[M any](msg Message[M], body []byte) amqp091.Publishing {
	pub := amqp091.Publishing{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		Headers:         amqp091.Table(msg.Headers),
		DeliveryMode:    msg.DeliveryMode.wire(), // SPEC §6.5: Persistent(0)→2, Transient(1)→1
		Priority:        msg.Priority,
		MessageId:       msg.MessageID,
		CorrelationId:   msg.CorrelationID,
		ReplyTo:         msg.ReplyTo,
		Type:            msg.Type,
		AppId:           msg.AppID,
		UserId:          msg.UserID,
		Timestamp:       msg.Timestamp,
		Body:            body,
	}
	if msg.Expiration > 0 {
		pub.Expiration = fmt.Sprintf("%d", msg.Expiration.Milliseconds())
	}
	if msg.Delay > 0 {
		// Route through the rabbitmq_delayed_message_exchange plugin. x-delay is a
		// signed 32-bit millisecond count (the plugin's ceiling is ~24.8 days). Clone
		// the header table first so a caller reusing Message.Headers across publishes
		// never sees x-delay smuggled into their own map.
		h := make(amqp091.Table, len(pub.Headers)+1)
		for k, v := range pub.Headers {
			h[k] = v
		}
		h["x-delay"] = int32(msg.Delay.Milliseconds())
		pub.Headers = h
	}
	return pub
}
