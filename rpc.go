package warren

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/codec"
)

// directReplyToQueue is RabbitMQ's direct reply-to pseudo-queue. A consumer that
// subscribes to it with no-ack receives replies routed by the broker to the exact
// channel that issued the basic.consume — no real queue is declared. See
// https://www.rabbitmq.com/docs/direct-reply-to.
const directReplyToQueue = "amq.rabbitmq.reply-to"

// ReplyHandler is the function signature a Replier serves: it maps a decoded
// request to a response (or an error, which the Replier surfaces via OnError and
// nacks without requeue — never an error envelope on the wire). Defined here so
// both the Caller (T29) and Replier (T30) sides share one declaration.
type ReplyHandler[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

// callerChannel is the AMQP channel surface a Caller drives: it both publishes
// requests and consumes replies on a single dedicated channel (direct reply-to is
// channel-scoped). *amqp091.Channel satisfies it; unit tests inject a fake.
type callerChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error)
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	NotifyClose(c chan *amqp091.Error) chan *amqp091.Error
	Close() error
}

// Caller performs synchronous request/reply RPC over AMQP. Req is the request
// payload type, Resp the response payload type; both are encoded/decoded with the
// configured codec.
//
// Channel ownership (SPEC §6.7). Because the direct reply-to pseudo-queue is
// channel-scoped — replies are delivered only on the channel that issued the
// basic.consume — a Caller does NOT use the rotating publisher channel pool. It
// holds one dedicated channel (pinned to a consumer-role TCP connection, like a
// Consumer) that both consumes amq.rabbitmq.reply-to and publishes the requests,
// so the reply routes back to it. Concurrent Call invocations share that channel
// and are demultiplexed by CorrelationID. If the channel closes (reconnect),
// in-flight calls resolve with ErrChannelClosed and the next Call transparently
// reopens and re-subscribes.
//
// Use CallerFor[Req, Resp](conn) to build a Caller.
type Caller[Req, Resp any] struct {
	conn       *Connection
	mc         *managedConn // pinned consumer-role TCP connection; nil only in unit tests
	exchange   string
	routingKey string
	codec      codec.Codec

	useExclusiveQueue bool
	prefetch          uint16

	// newChannel, when non-nil, replaces the real amqp091 channel open. Test-only
	// injection seam; nil in production, where openChannel uses the pinned mc.
	newChannel func() (callerChannel, error)

	mu      sync.Mutex
	session *callerSession
	closed  bool

	// pending demultiplexes replies: CorrelationID(string) → chan amqp091.Delivery.
	// Each in-flight Call registers a buffered reply channel before publishing and
	// removes it on return; the session dispatcher routes by CorrelationID.
	pending sync.Map
}

// callerSession bundles one live dedicated channel with the reply address it is
// subscribed to. done is closed by the dispatcher when the channel dies, which
// unblocks every in-flight Call with ErrChannelClosed.
type callerSession struct {
	ch         callerChannel
	replyTo    string
	ackReplies bool // true in exclusive-queue mode (real queue uses regular acks)

	done      chan struct{}
	closeOnce sync.Once
}

func (s *callerSession) alive() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

func (s *callerSession) signalDone() { s.closeOnce.Do(func() { close(s.done) }) }

// Call publishes req and blocks until the matching reply arrives, the ctx
// deadline fires (ErrCallTimeout), or the dedicated channel closes
// (ErrChannelClosed). It auto-stamps a fresh CorrelationID and the reply address
// on the request, encodes req with the configured codec, and decodes the reply
// body into Resp.
//
// At-least-once: a Replier may send a reply more than once (it acks the request
// only after the reply confirms; a crash in that window causes a redelivery and a
// second reply). Treat responses as at-least-once and dedupe by CorrelationID if
// your handler is not naturally idempotent.
func (c *Caller[Req, Resp]) Call(ctx context.Context, req Req) (Resp, error) {
	var zero Resp

	s, err := c.ensureSession(ctx)
	if err != nil {
		return zero, err
	}

	cid, err := uuid.NewV7()
	if err != nil {
		return zero, fmt.Errorf("%w: failed to generate correlation id: %w", ErrInvalidMessage, err)
	}
	correlationID := cid.String()

	msg := Message[Req]{
		Body:          &req,
		CorrelationID: correlationID,
		ReplyTo:       s.replyTo,
	}
	body, err := encodeMessageBody(c.codec, &msg)
	if err != nil {
		return zero, err
	}
	pub := buildPublishing(msg, body)

	// Register the reply slot BEFORE publishing so a fast reply is never missed.
	// Buffered (cap 1) + non-blocking dispatcher send means no goroutine can leak
	// if this Call has already returned on timeout/close by the time a late reply
	// arrives.
	replyCh := make(chan amqp091.Delivery, 1)
	c.pending.Store(correlationID, replyCh)
	defer c.pending.Delete(correlationID)

	if err := s.ch.PublishWithContext(ctx, c.exchange, c.routingKey, false, false, pub); err != nil {
		return zero, fmt.Errorf("warren: caller publish: %w", wrapAMQPError(err))
	}

	select {
	case <-ctx.Done():
		return zero, fmt.Errorf("%w: %w", ErrCallTimeout, ctx.Err())
	case <-s.done:
		return zero, ErrChannelClosed
	case d := <-replyCh:
		var resp Resp
		if err := safeDecodeConsumer(c.codec, d.Body, d.Headers, d.ContentType, &resp); err != nil {
			return zero, err
		}
		return resp, nil
	}
}

// encodeMessageBody applies Message defaults, validates headers, and encodes the
// body (honouring a HeaderCodec). Shared by the Caller (request) and the Replier
// (reply): both construct the Message themselves, so there are no caller-supplied
// headers to merge — a HeaderCodec's headers become the header set.
func encodeMessageBody[M any](c codec.Codec, msg *Message[M]) ([]byte, error) {
	if err := msg.applyDefaults(c); err != nil {
		return nil, err
	}
	if err := msg.validateHeaders(); err != nil {
		return nil, err
	}
	if msg.Body == nil {
		return nil, fmt.Errorf("%w: Body must not be nil", ErrInvalidMessage)
	}
	body, ceHeaders, ceContentType, err := safeEncodeBody(c, msg.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	if len(ceHeaders) > 0 {
		if err := validateHeaders(Headers(ceHeaders)); err != nil {
			return nil, err
		}
		msg.Headers = ceHeaders
	}
	if ceContentType != "" {
		msg.ContentType = ceContentType
	}
	return body, nil
}

// ensureSession returns the live dedicated channel, opening (or reopening, after a
// channel close) one under the lock. It waits out any reconnect barrier on the
// pinned connection first so a call issued mid-reconnect blocks rather than failing.
func (c *Caller[Req, Resp]) ensureSession(ctx context.Context) (*callerSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrAlreadyClosed
	}
	if c.session != nil && c.session.alive() {
		return c.session, nil
	}
	if c.mc != nil {
		if err := c.mc.waitBarrier(ctx); err != nil {
			return nil, err
		}
	}
	s, err := c.openSession()
	if err != nil {
		return nil, err
	}
	c.session = s
	return s, nil
}

// openSession opens the dedicated channel and issues the reply subscription. Per
// the direct reply-to protocol the consumer is declared BEFORE any request is
// published and runs no-ack; UseExclusiveReplyQueue swaps in a real exclusive
// auto-delete queue with regular acks (and re-enables Prefetch).
func (c *Caller[Req, Resp]) openSession() (*callerSession, error) {
	ch, err := c.openChannel()
	if err != nil {
		return nil, err
	}

	replyTo := directReplyToQueue
	autoAck := true
	if c.useExclusiveQueue {
		q, derr := ch.QueueDeclare("", false /* durable */, true /* autoDelete */, true /* exclusive */, false, nil)
		if derr != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("warren: caller declare reply queue: %w", wrapAMQPError(derr))
		}
		replyTo = q.Name
		autoAck = false
		if c.prefetch > 0 {
			if qerr := ch.Qos(int(c.prefetch), 0, false); qerr != nil {
				_ = ch.Close()
				return nil, fmt.Errorf("warren: caller Qos: %w", wrapAMQPError(qerr))
			}
		}
	}

	deliveries, cerr := ch.Consume(replyTo, "", autoAck, false /* exclusive */, false, false, nil)
	if cerr != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("warren: caller subscribe reply: %w", wrapAMQPError(cerr))
	}

	s := &callerSession{
		ch:         ch,
		replyTo:    replyTo,
		ackReplies: !autoAck,
		done:       make(chan struct{}),
	}
	go c.dispatch(s, deliveries)
	return s, nil
}

// openChannel opens the dedicated AMQP channel, preferring the test injection seam.
func (c *Caller[Req, Resp]) openChannel() (callerChannel, error) {
	if c.newChannel != nil {
		return c.newChannel()
	}
	topoCh, err := c.mc.openChannel()
	if err != nil {
		return nil, fmt.Errorf("warren: caller open channel: %w", err)
	}
	ch, ok := topoCh.(*amqp091.Channel)
	if !ok {
		_ = topoCh.Close()
		return nil, fmt.Errorf("warren: caller: unexpected channel type %T", topoCh)
	}
	return ch, nil
}

// dispatch routes each reply delivery to the waiting Call by CorrelationID. It
// exits — and signals every in-flight Call via done — when the deliveries channel
// closes (the dedicated channel died).
func (c *Caller[Req, Resp]) dispatch(s *callerSession, deliveries <-chan amqp091.Delivery) {
	defer s.signalDone()
	for d := range deliveries {
		if chAny, ok := c.pending.Load(d.CorrelationId); ok {
			// Non-blocking send: the reply slot is buffered (cap 1) and a Call that
			// has already returned leaves an unread slot — dropping the duplicate is
			// correct (at-least-once replies; the waiter is gone).
			select {
			case chAny.(chan amqp091.Delivery) <- d:
			default:
			}
		}
		// Exclusive-queue mode uses regular acks. The Acknowledger guard keeps the
		// fake-channel unit tests (no Acknowledger) from panicking.
		if s.ackReplies && d.Acknowledger != nil {
			_ = d.Ack(false)
		}
	}
}

// Health reports whether the Caller's pinned connection is healthy.
func (c *Caller[Req, Resp]) Health(ctx context.Context) error {
	if c.mc == nil {
		return ErrNotConnected
	}
	return c.mc.health(ctx)
}

// Close releases the dedicated channel. Closing it unblocks the dispatcher (the
// deliveries channel closes), which signals any in-flight Call with
// ErrChannelClosed. Returns ErrAlreadyClosed if called more than once.
func (c *Caller[Req, Resp]) Close(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrAlreadyClosed
	}
	c.closed = true
	if c.session != nil {
		_ = c.session.ch.Close()
	}
	return nil
}
