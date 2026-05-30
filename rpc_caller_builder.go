package warren

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/brunomvsouza/warren/codec"
)

// CallerBuilder configures and builds a Caller[Req, Resp].
//
// All option methods follow a last-wins policy: calling the same method twice
// keeps only the final value.
type CallerBuilder[Req, Resp any] struct {
	conn       *Connection
	exchange   string
	routingKey string
	c          codec.Codec

	useExclusiveQueue bool
	prefetch          uint16
	prefetchSet       bool
}

// CallerFor returns a builder for a Caller[Req, Resp] tied to conn.
func CallerFor[Req, Resp any](conn *Connection) *CallerBuilder[Req, Resp] {
	return &CallerBuilder[Req, Resp]{conn: conn}
}

// Exchange sets the AMQP exchange the request is published to. Default: "" (the
// default exchange, which routes by queue name via RoutingKey).
func (b *CallerBuilder[Req, Resp]) Exchange(name string) *CallerBuilder[Req, Resp] {
	b.exchange = name
	return b
}

// RoutingKey sets the routing key every request is published with. For the common
// case (default exchange) this is the request queue name the Replier consumes.
func (b *CallerBuilder[Req, Resp]) RoutingKey(rk string) *CallerBuilder[Req, Resp] {
	b.routingKey = rk
	return b
}

// Codec sets the message codec used to encode requests and decode replies.
// Default: JSON (lax — accepts unknown fields per Postel's Law).
func (b *CallerBuilder[Req, Resp]) Codec(c codec.Codec) *CallerBuilder[Req, Resp] {
	b.c = c
	return b
}

// UseExclusiveReplyQueue switches from the channel-scoped direct reply-to
// pseudo-queue to a real exclusive, auto-delete reply queue declared per Caller,
// with regular ack semantics. It costs one extra declare per Caller but re-enables
// Prefetch and survives more failure modes. Default: direct reply-to.
func (b *CallerBuilder[Req, Resp]) UseExclusiveReplyQueue() *CallerBuilder[Req, Resp] {
	b.useExclusiveQueue = true
	return b
}

// Prefetch sets the basic.qos prefetch count on the reply consumer. It is only
// honoured with UseExclusiveReplyQueue: RabbitMQ rejects basic.qos on the direct
// reply-to pseudo-queue, so Build returns ErrInvalidOptions if Prefetch is set
// without UseExclusiveReplyQueue.
func (b *CallerBuilder[Req, Resp]) Prefetch(count uint16) *CallerBuilder[Req, Resp] {
	b.prefetch = count
	b.prefetchSet = true
	return b
}

// Build constructs and returns a Caller[Req, Resp]. It pins the Caller to a
// consumer-role TCP connection (by stable hash of a generated caller id, like a
// Consumer) and validates the option combination. Returns ErrInvalidOptions on an
// invalid configuration.
func (b *CallerBuilder[Req, Resp]) Build() (*Caller[Req, Resp], error) {
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	if b.prefetchSet && !b.useExclusiveQueue {
		return nil, fmt.Errorf("%w: Prefetch requires UseExclusiveReplyQueue (basic.qos is not honoured on %s)",
			ErrInvalidOptions, directReplyToQueue)
	}

	c := b.c
	if c == nil {
		c = codec.NewJSON()
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("warren: failed to generate caller id: %w", err)
	}
	idx := connIndexForTag("caller-"+id.String(), b.conn.NumConConns())
	mc := b.conn.ConConnAt(idx)

	return &Caller[Req, Resp]{
		conn:              b.conn,
		mc:                mc,
		exchange:          b.exchange,
		routingKey:        b.routingKey,
		codec:             c,
		useExclusiveQueue: b.useExclusiveQueue,
		prefetch:          b.prefetch,
	}, nil
}
