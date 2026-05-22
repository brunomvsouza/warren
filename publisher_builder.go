package amqp

import (
	"context"
	"fmt"

	"github.com/brunomvsouza/amqp/codec"
	"github.com/brunomvsouza/amqp/metrics"
	"github.com/brunomvsouza/amqp/otel"
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

// Codec sets the message codec. Default: JSON (strict).
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

// Build constructs and returns a Publisher[M]. Returns an error if
// the builder state is invalid.
func (b *PublisherBuilder[M]) Build() (*Publisher[M], error) {
	if b.conn == nil {
		return nil, fmt.Errorf("%w: conn must not be nil", ErrInvalidOptions)
	}
	b.applyBuilderDefaults()

	numConns := b.conn.NumPubConns()
	pools := make([]*publisherConnPool, numConns)
	mcs := make([]*managedConn, numConns)
	poolSize := b.conn.opts.channelPoolSize

	for i := range numConns {
		mc := b.conn.PubConnAt(i)
		mcs[i] = mc

		// Capture loop variable for closure.
		connIdx := i
		pools[i] = newPublisherConnPool(poolSize, func() (publisherEntry, error) {
			return b.conn.PubConnAt(connIdx).openPublisherEntry(poolSize)
		})

		// Register drain hook so stale channels are discarded after reconnect.
		pool := pools[i]
		mc.registerHook(func(_ context.Context) error {
			pool.drain()
			return nil
		})
	}

	return &Publisher[M]{
		conn:           b.conn,
		pools:          pools,
		mcs:            mcs,
		exchange:       b.exchange,
		routingKey:     b.routingKey,
		codec:          b.c,
		pm:             b.pm,
		tracer:         b.tracer,
		confirmTimeout: defaultConfirmTimeout,
	}, nil
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
}
