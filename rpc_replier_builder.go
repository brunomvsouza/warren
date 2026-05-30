package warren

import (
	"context"
	"fmt"
	"time"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/metrics"
)

// ReplierBuilder configures and builds a Replier[Req, Resp].
//
// All option methods follow a last-wins policy: calling the same method twice
// keeps only the final value.
type ReplierBuilder[Req, Resp any] struct {
	conn  *Connection
	queue string

	c       codec.Codec
	cm      metrics.ConsumerMetrics
	onError func(ctx context.Context, req Req, err error)

	topology        *Topology
	allowMissingDLX bool

	confirmTimeout    time.Duration
	confirmTimeoutSet bool
}

// ReplierFor returns a builder for a Replier[Req, Resp] tied to conn.
func ReplierFor[Req, Resp any](conn *Connection) *ReplierBuilder[Req, Resp] {
	return &ReplierBuilder[Req, Resp]{conn: conn}
}

// Queue sets the request queue the Replier consumes from. Required.
func (b *ReplierBuilder[Req, Resp]) Queue(name string) *ReplierBuilder[Req, Resp] {
	b.queue = name
	return b
}

// Codec sets the codec used to decode requests and encode replies. Default: JSON.
func (b *ReplierBuilder[Req, Resp]) Codec(c codec.Codec) *ReplierBuilder[Req, Resp] {
	b.c = c
	return b
}

// Metrics sets the ConsumerMetrics recorder (which carries replier_drop_no_dlx_total).
// Default: NoOp.
func (b *ReplierBuilder[Req, Resp]) Metrics(cm metrics.ConsumerMetrics) *ReplierBuilder[Req, Resp] {
	b.cm = cm
	return b
}

// OnError registers a hook invoked when a successful reply cannot be produced or
// addressed: the handler returns a non-nil error, the response fails to encode, or
// the request carries no ReplyTo address. (A reply that encodes and is published
// but never confirms is NOT reported here — that is a transport failure, not a
// handler one.) In every case the request is Nack(false)'d (so it routes to a DLX
// if configured, or is dropped if not) and no error envelope is sent to the caller
// — the caller observes ErrCallTimeout once its ctx expires.
//
// The silent-drop failure mode is load-bearing: without a DLX on the request
// queue, Nack(false) is a drop and OnError is the only client-side signal. Log,
// metric, or alert from it. The mandatory metric replier_drop_no_dlx_total
// increments on every such drop even if OnError is not wired.
func (b *ReplierBuilder[Req, Resp]) OnError(fn func(ctx context.Context, req Req, err error)) *ReplierBuilder[Req, Resp] {
	b.onError = fn
	return b
}

// Topology supplies the Topology used to declare the request queue so Build can
// statically validate that the queue has a DeadLetter entry. When it does not,
// Build returns ErrInvalidOptions unless AllowMissingDLX opts out. When the
// request queue is declared out-of-band (no Topology wired), the library cannot
// detect a missing DLX statically and the replier_drop_no_dlx_total metric plus
// OnError remain the only signal.
func (b *ReplierBuilder[Req, Resp]) Topology(t *Topology) *ReplierBuilder[Req, Resp] {
	b.topology = t
	return b
}

// AllowMissingDLX opts out of the Topology DLX-presence validation, acknowledging
// that a handler error or reply-publish failure on this queue is a silent drop
// (still surfaced via OnError and replier_drop_no_dlx_total). Use it when the
// request queue is intentionally declared without a dead-letter exchange.
func (b *ReplierBuilder[Req, Resp]) AllowMissingDLX() *ReplierBuilder[Req, Resp] {
	b.allowMissingDLX = true
	return b
}

// ConfirmTimeout bounds how long the Replier waits for the broker confirm of a
// reply publish before treating it as failed (and Nack(false)'ing the request).
// Default: 30 s.
func (b *ReplierBuilder[Req, Resp]) ConfirmTimeout(d time.Duration) *ReplierBuilder[Req, Resp] {
	b.confirmTimeout = d
	b.confirmTimeoutSet = true
	return b
}

// Build constructs and returns a Replier[Req, Resp]. It validates DLX presence
// against any wired Topology, builds the internal request consumer, and pins the
// confirm-tracked reply publisher to a publisher-role connection. Returns
// ErrInvalidOptions on an invalid configuration.
//
// Silent-drop failure mode: a handler error or a failed reply publish makes the
// Replier Nack(false) the request. With a DLX on the request queue the request is
// preserved there; without one it is dropped. Configure a DLX (and wire it via
// Topology so this method can validate its presence) if you need failed requests
// kept for forensics. The mandatory metric replier_drop_no_dlx_total and the
// OnError hook are the only signals when no DLX is configured.
func (b *ReplierBuilder[Req, Resp]) Build() (*Replier[Req, Resp], error) {
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	if b.queue == "" {
		return nil, fmt.Errorf("%w: queue must not be empty", ErrInvalidOptions)
	}

	knownHasDLX := false
	if b.topology != nil {
		knownHasDLX = topologyHasDLX(b.topology, b.queue)
		if !knownHasDLX && !b.allowMissingDLX {
			//nolint:staticcheck // ST1005: exact SPEC §6.7 / T30 wording, including the terminal period
			return nil, fmt.Errorf("%w: Replier request queue %s has no DeadLetter entry in Topology; "+
				"Nack(false) drops will be silent. Add a DeadLetter or use AllowMissingDLX() to acknowledge.",
				ErrInvalidOptions, b.queue)
		}
	}

	c := b.c
	if c == nil {
		c = codec.NewJSON()
	}
	cm := b.cm
	if cm == nil {
		cm = metrics.NoOpConsumerMetrics{}
	}
	confirmTimeout := defaultConfirmTimeout
	if b.confirmTimeoutSet {
		confirmTimeout = b.confirmTimeout
	}

	consumer, err := ConsumerFor[Req](b.conn).
		Queue(b.queue).
		Codec(c).
		Metrics(cm).
		Build()
	if err != nil {
		return nil, err
	}

	pubMC := b.conn.PubConnAt(connIndexForTag(b.queue, b.conn.NumPubConns()))
	poolSize := b.conn.opts.channelPoolSize
	replyPub := &replyPublisher{
		confirmTimeout: confirmTimeout,
		// openEntry waits out any reconnect barrier on the pinned publisher-role
		// connection, then opens a fresh confirm-enabled channel. Folded into a
		// closure so replyPublisher stays unit-testable via an injected fake.
		openEntry: func(ctx context.Context) (publisherEntry, error) {
			if err := pubMC.waitBarrier(ctx); err != nil {
				return publisherEntry{}, err
			}
			return pubMC.openPublisherEntry(poolSize, nil)
		},
	}

	return &Replier[Req, Resp]{
		queue:       b.queue,
		codec:       c,
		cm:          cm,
		onError:     b.onError,
		knownHasDLX: knownHasDLX,
		consumer:    consumer,
		replyPub:    replyPub,
	}, nil
}

// topologyHasDLX reports whether t declares a DeadLetter whose Source is queue.
func topologyHasDLX(t *Topology, queue string) bool {
	for _, dl := range t.DeadLetters {
		if dl.Source == queue {
			return true
		}
	}
	return false
}
