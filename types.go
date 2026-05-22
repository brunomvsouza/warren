package amqp

import "time"

// Headers is an AMQP field-table. Values must be one of the types supported by
// the amqp091-go encoder: bool, byte, int16, int32, int64, float32, float64,
// string, []byte, Decimal, time.Time, map[string]any, []any, or nil.
// int and uint literals auto-coerce to int64/uint64. Any other Go type causes
// Publish to return ErrInvalidMessage.
type Headers map[string]any

// DeliveryMode controls AMQP delivery persistence. The zero value is
// DeliveryModePersistent so a zero-valued Message[M] defaults to durable.
type DeliveryMode uint8

const (
	// DeliveryModePersistent is the default; messages survive broker restarts.
	DeliveryModePersistent DeliveryMode = iota
	// DeliveryModeTransient messages are kept only in-memory and are lost on broker restart.
	DeliveryModeTransient
)

// ExchangeKind is the AMQP exchange type string passed to exchange.declare.
type ExchangeKind string

const (
	// ExchangeDirect routes messages to queues whose binding key matches the routing key exactly.
	ExchangeDirect ExchangeKind = "direct"
	// ExchangeFanout routes messages to all bound queues regardless of routing key.
	ExchangeFanout ExchangeKind = "fanout"
	// ExchangeTopic routes messages to queues whose binding key pattern matches the routing key.
	ExchangeTopic ExchangeKind = "topic"
	// ExchangeHeaders routes messages based on header attributes instead of routing key.
	ExchangeHeaders ExchangeKind = "headers"
	// ExchangeDelayed routes messages via the rabbitmq_delayed_message_exchange plugin.
	ExchangeDelayed ExchangeKind = "x-delayed-message"
)

// QueueType selects the RabbitMQ queue implementation via the x-queue-type
// queue argument. An empty value means the broker default (classic).
type QueueType string

const (
	// QueueTypeClassic is the default RabbitMQ queue type.
	QueueTypeClassic QueueType = "classic"
	// QueueTypeQuorum is the replicated, durable queue type recommended for production.
	QueueTypeQuorum QueueType = "quorum"
	// QueueTypeStream is available for declaration in v0.1; native stream consume is v0.2.
	QueueTypeStream QueueType = "stream"
)

// OverflowPolicy sets the x-overflow queue argument on a source queue with a
// DeadLetter or a max-length cap. An empty value means the broker default (drop-head).
type OverflowPolicy string

const (
	// OverflowDropHead is the broker default; drops the oldest message when the queue is full.
	OverflowDropHead OverflowPolicy = "drop-head"
	// OverflowRejectPublish rejects publisher confirms (ErrPublishNacked) when the queue is full.
	OverflowRejectPublish OverflowPolicy = "reject-publish"
	// OverflowRejectPublishDLX dead-letters the overflow message instead of dropping it.
	OverflowRejectPublishDLX OverflowPolicy = "reject-publish-dlx"
)

// SASLMechanism selects the SASL mechanism for the AMQP 0-9-1 handshake.
// The default is SASLPlain (username + password). SASLExternal delegates
// authentication to the TLS client certificate; WithAuth becomes a no-op
// and emits a Dial-time warning.
type SASLMechanism string

const (
	// SASLPlain authenticates with username and password via the PLAIN mechanism.
	SASLPlain SASLMechanism = "PLAIN"
	// SASLExternal authenticates via TLS client certificate; requires amqps:// and a client cert.
	SASLExternal SASLMechanism = "EXTERNAL"
)

// ReturnedProperties mirrors the 13 AMQP basic.properties fields carried in a
// basic.return frame. It has the same semantics as the corresponding Message[M]
// fields; see those godoc entries for value constraints.
type ReturnedProperties struct {
	ContentType     string
	ContentEncoding string
	Headers         Headers
	DeliveryMode    DeliveryMode
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	// Expiration is the per-message TTL encoded as milliseconds in the wire
	// frame. Zero means no per-message TTL was set.
	Expiration time.Duration
	MessageID  string
	Timestamp  time.Time
	Type       string
	UserID     string
	AppID      string
}

// Return carries the broker's basic.return frame for a mandatory publish that
// could not be routed to any queue. OnReturn callbacks receive this value
// synchronously before the corresponding Publish call unblocks.
type Return struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	Properties ReturnedProperties
}

// TimeoutVerdict decides the ack/nack action when a handler exceeds its
// HandlerTimeout. The zero value is TimeoutNackNoRequeue so that a
// misconfigured handler does not create an infinite requeue loop.
type TimeoutVerdict uint8

const (
	// TimeoutNackNoRequeue is the default; the message goes to the DLX (or is dropped).
	TimeoutNackNoRequeue TimeoutVerdict = iota
	// TimeoutNackRequeue requeues the message; subject to MaxRedeliveries / x-delivery-limit.
	TimeoutNackRequeue
)
