package warren

import (
	"context"
	"fmt"
	"time"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// PublisherBuilder configures and builds a Publisher[M].
//
// All option methods follow a last-wins policy: calling the same method twice
// keeps only the final value.
type PublisherBuilder[M any] struct {
	conn *Connection

	exchange   string
	routingKey string

	c      codec.Codec
	pm     metrics.PublisherMetrics
	tracer otel.Tracer

	mandatory              bool
	onReturn               func(Return)
	confirmTimeout         time.Duration
	confirmTimeoutSet      bool // distinguishes explicit zero from unset
	publishTimeout         time.Duration
	publishBatchMaxSize    int
	maxMessageSizeBytes    int  // 0 = disabled; default applied in applyBuilderDefaults
	maxMessageSizeBytesSet bool // distinguishes explicit zero from unset
	retryPolicy            RetryPolicy
	retryPolicySet         bool
	stampUserID            bool
}

// PublisherFor returns a builder for a Publisher[M] tied to conn.
func PublisherFor[M any](conn *Connection) *PublisherBuilder[M] {
	return &PublisherBuilder[M]{conn: conn}
}

// Exchange sets the AMQP exchange name. Default: "" (default exchange).
func (b *PublisherBuilder[M]) Exchange(name string) *PublisherBuilder[M] {
	b.exchange = name
	return b
}

// RoutingKey sets the default routing key used on every Publish call.
func (b *PublisherBuilder[M]) RoutingKey(rk string) *PublisherBuilder[M] {
	b.routingKey = rk
	return b
}

// Codec sets the message codec. Default: JSON (lax by default — accepts unknown
// fields per Postel's Law so producer-first deploys do not poison v1 consumers'
// DLQs). Use codec.NewJSONStrict for consumer-side schema enforcement.
func (b *PublisherBuilder[M]) Codec(c codec.Codec) *PublisherBuilder[M] {
	b.c = c
	return b
}

// Metrics sets the PublisherMetrics recorder. Default: NoOp.
func (b *PublisherBuilder[M]) Metrics(pm metrics.PublisherMetrics) *PublisherBuilder[M] {
	b.pm = pm
	return b
}

// WithoutMetrics disables all publisher metrics (last-wins against Metrics).
func (b *PublisherBuilder[M]) WithoutMetrics() *PublisherBuilder[M] {
	b.pm = metrics.NoOpPublisherMetrics{}
	return b
}

// Tracer sets the OTel tracer for publish spans.
func (b *PublisherBuilder[M]) Tracer(t otel.Tracer) *PublisherBuilder[M] {
	b.tracer = t
	return b
}

// Mandatory sets the AMQP mandatory flag on every publish. A mandatory
// publish that cannot be routed to any queue triggers a basic.return frame
// and Publish returns ErrUnroutable (OnReturn fires first if set).
func (b *PublisherBuilder[M]) Mandatory() *PublisherBuilder[M] {
	b.mandatory = true
	return b
}

// OnReturn registers a callback that fires synchronously before Publish
// unblocks when a mandatory publish is returned by the broker (basic.return).
// The callback receives the full Return including properties and reply code.
// Last-wins: calling OnReturn twice keeps only the second callback.
func (b *PublisherBuilder[M]) OnReturn(cb func(Return)) *PublisherBuilder[M] {
	b.onReturn = cb
	return b
}

// ConfirmTimeout sets the deadline for receiving a publisher confirm (basic.ack
// or basic.nack) after a publish. Default: 30 s. Zero disables the confirm
// deadline (discouraged; the publisher may block indefinitely if the broker
// never confirms).
func (b *PublisherBuilder[M]) ConfirmTimeout(d time.Duration) *PublisherBuilder[M] {
	b.confirmTimeout = d
	b.confirmTimeoutSet = true
	return b
}

// PublishTimeout sets an end-to-end deadline that bounds pool acquisition +
// write + confirm + blocked-connection wait + reconnect barrier. Zero (default)
// means the caller context is the only deadline. When both PublishTimeout and
// the caller context have deadlines, the shorter one wins.
func (b *PublisherBuilder[M]) PublishTimeout(d time.Duration) *PublisherBuilder[M] {
	b.publishTimeout = d
	return b
}

// PublishBatchMaxSize sets the per-call cap for PublishBatch (T22). Default:
// 1024. This is NOT a sliding in-flight window — it is a per-call limit.
// Validated at PublishBatch-time only.
func (b *PublisherBuilder[M]) PublishBatchMaxSize(n int) *PublisherBuilder[M] {
	b.publishBatchMaxSize = n
	return b
}

// MaxMessageSizeBytes caps the encoded body size each Publish accepts. Publishes
// whose serialised body exceeds n bytes are rejected locally with
// ErrMessageTooLarge — protecting the publisher from OOM and the broker from
// frame fragmentation pressure (the broker-side equivalent, reply code 311
// CONTENT_TOO_LARGE, only fires after the payload has been allocated and
// partially sent).
//
// Default: 16 MiB (16 * 1024 * 1024). Pass 0 to disable the guardrail
// (discouraged for production paths). Negative values fail Build with
// ErrInvalidOptions.
//
// The cap is enforced against the encoded body, not the in-memory Message[M],
// so it matches what travels on the wire. ErrMessageTooLarge is classified
// permanent (IsPermanent == true): the same payload will never fit on retry.
func (b *PublisherBuilder[M]) MaxMessageSizeBytes(n int) *PublisherBuilder[M] {
	b.maxMessageSizeBytes = n
	b.maxMessageSizeBytesSet = true
	return b
}

// PublishRetry configures automatic retry of publishes that fail with a
// transient error (IsTransient(err) == true). Permanent errors are never
// retried. Each retry attempt increments the mandatory metric
// publisher_retry_total{exchange, reason}.
//
// Retries can produce duplicates. Consumers MUST be idempotent (dedupe by
// MessageID). See SPEC §6.2.1.
func (b *PublisherBuilder[M]) PublishRetry(p RetryPolicy) *PublisherBuilder[M] {
	b.retryPolicy = p
	b.retryPolicySet = true
	return b
}

// StampUserID auto-sets Message[M].UserID to conn.AuthenticatedUser() on every
// Publish call. Use this to avoid manually populating UserID when the broker
// validates the stamp. Last-wins against a previous StampUserID() call.
//
// Note: for SASL EXTERNAL with a dynamic GetClientCertificate callback, the
// authenticated user is resolved once at Dial() time and may not reflect
// a certificate rotated after that. In that configuration, set stampUserID=false
// and populate Message.UserID manually from the current certificate's CN.
func (b *PublisherBuilder[M]) StampUserID() *PublisherBuilder[M] {
	b.stampUserID = true
	return b
}

// Build constructs and returns a Publisher[M]. Returns an error if
// the builder state is invalid.
func (b *PublisherBuilder[M]) Build() (*Publisher[M], error) {
	if b.maxMessageSizeBytesSet && b.maxMessageSizeBytes < 0 {
		return nil, fmt.Errorf("%w: MaxMessageSizeBytes must be >= 0 (0 disables; default is 16 MiB)", ErrInvalidOptions)
	}
	if b.publishBatchMaxSize < 0 {
		return nil, fmt.Errorf("%w: PublishBatchMaxSize must be >= 0 (0 uses default 1024)", ErrInvalidOptions)
	}
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	b.applyBuilderDefaults()

	var rp *RetryPolicy
	if b.retryPolicySet {
		p := b.retryPolicy
		rp = &p
	}

	// Build the Publisher first so pool closures can reference pub.callOnReturn.
	pub := &Publisher[M]{
		conn:                b.conn,
		exchange:            b.exchange,
		routingKey:          b.routingKey,
		msgType:             metricsTypeName[M](),
		codec:               b.c,
		pm:                  b.pm,
		tracer:              b.tracer,
		propagator:          otel.NewPropagator(),
		confirmTimeout:      b.confirmTimeout,
		mandatory:           b.mandatory,
		onReturn:            b.onReturn,
		publishTimeout:      b.publishTimeout,
		publishBatchMaxSize: b.publishBatchMaxSize,
		maxMessageSizeBytes: b.maxMessageSizeBytes,
		retryPolicy:         rp,
		stampUserID:         b.stampUserID,
		authUser:            b.conn.AuthenticatedUser(),
	}

	numConns := b.conn.NumPubConns()
	pub.pools = make([]*publisherConnPool, numConns)
	pub.mcs = make([]*managedConn, numConns)
	poolSize := b.conn.opts.channelPoolSize

	for i := range numConns {
		mc := b.conn.PubConnAt(i)
		pub.mcs[i] = mc

		// Capture loop variable for closure.
		connIdx := i
		pub.pools[i] = newPublisherConnPool(poolSize, func() (publisherEntry, error) {
			return b.conn.PubConnAt(connIdx).openPublisherEntry(poolSize, pub.callOnReturn)
		})

		// Register drain hook so stale channels are discarded after reconnect.
		pool := pub.pools[i]
		mc.registerHook(func(_ context.Context) error {
			pool.drain()
			return nil
		})
	}

	return pub, nil
}

// applyBuilderDefaults fills any unset options with sensible defaults.
func (b *PublisherBuilder[M]) applyBuilderDefaults() {
	if b.c == nil {
		b.c = codec.NewJSON()
	}
	if b.pm == nil {
		b.pm = metrics.NoOpPublisherMetrics{}
	}
	if b.tracer == nil {
		b.tracer = otel.NoOpTracer{}
	}
	if !b.confirmTimeoutSet {
		b.confirmTimeout = defaultConfirmTimeout
	}
	if b.publishBatchMaxSize <= 0 {
		b.publishBatchMaxSize = 1024
	}
	if !b.maxMessageSizeBytesSet {
		b.maxMessageSizeBytes = defaultMaxMessageSizeBytes
	}
}

// defaultMaxMessageSizeBytes is the per-publish payload guardrail (16 MiB).
// Matches typical frame-max tuning (128 KiB frames × ~128 frames) and is the
// SRE-recommended default; raise via MaxMessageSizeBytes for streaming workloads.
const defaultMaxMessageSizeBytes = 16 * 1024 * 1024
