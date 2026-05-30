package warren

import (
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// noUnkeyedFixtureLiterals, embedded as an unexported blank field, makes
// DeliveryFixture and BatchFixture compile only via keyed literals from other
// packages: a positional (unkeyed) literal would have to supply a value for
// this field, whose type cannot be named outside the warren package. New
// fields can therefore be added in a minor release without breaking callers.
type noUnkeyedFixtureLiterals struct{}

// DeliveryFixture is the keyed-literal input to [NewDeliveryFixture] (and its
// re-export amqpmock.NewDelivery). It fabricates a [Delivery] for unit tests
// without a live broker and — unlike the amqpmock subpackage — without pulling
// in go.uber.org/mock, so consumer/raw/batch unit tests stay gomock-free
// (SPEC §10 decision 9, GA-09).
//
// Only keyed literals compile from outside the package; the trailing guard
// field rejects positional literals so future fields are non-breaking.
type DeliveryFixture[M any] struct {
	// Body is the decoded payload returned by Delivery[M].Body(). A nil Body
	// is allowed and mirrors a decode that produced no value.
	Body *M
	// Queue is the queue the delivery is attributed to; it scopes x-death
	// accounting (DeathCount, DeathCountByReason, DeathReasons).
	Queue string
	// Headers populates the AMQP header table (Delivery.Headers()), including
	// any "x-death" entries the fixture wants the Death* accessors to observe.
	Headers Headers
	// MessageID maps to the AMQP message-id property (Delivery.MessageID()).
	MessageID string
	// CorrelationID maps to the AMQP correlation-id property (Delivery.CorrelationID()).
	CorrelationID string
	// ContentType maps to the AMQP content-type property.
	ContentType string
	// Timestamp maps to the AMQP timestamp property (Delivery.Timestamp()).
	Timestamp time.Time
	// Redelivered sets the redelivered flag (Delivery.Redelivered()).
	Redelivered bool
	// DeliveryTag sets the broker delivery tag (Delivery.DeliveryTag()).
	DeliveryTag uint64

	_ noUnkeyedFixtureLiterals
}

// BatchFixture is the keyed-literal input to [NewBatchFixture] (and its
// re-export amqpmock.NewBatch). It fabricates a [Batch] from a slice of
// per-message fixtures, in order.
type BatchFixture[M any] struct {
	// Deliveries are the per-message fixtures composing the batch, in order.
	Deliveries []DeliveryFixture[M]

	_ noUnkeyedFixtureLiterals
}

// NewDeliveryFixture builds a *Delivery[M] from f for unit tests. The returned
// delivery is not bound to a live channel, so Ack/Nack/AckIf return an error
// rather than reaching a broker; fixtures are for exercising Body, Headers, and
// the metadata/x-death accessors, not acknowledgement mechanics.
func NewDeliveryFixture[M any](f DeliveryFixture[M]) *Delivery[M] {
	raw := amqp091.Delivery{
		Headers:       amqp091.Table(f.Headers),
		ContentType:   f.ContentType,
		Timestamp:     f.Timestamp,
		MessageId:     f.MessageID,
		CorrelationId: f.CorrelationID,
		Redelivered:   f.Redelivered,
		DeliveryTag:   f.DeliveryTag,
	}
	return newDelivery(f.Body, f.Queue, raw, nil)
}

// NewBatchFixture builds a *Batch[M] from f for unit tests. Each entry is
// constructed with [NewDeliveryFixture], so the same acknowledgement caveat
// applies: the batch's Ack/Nack do not reach a broker.
func NewBatchFixture[M any](f BatchFixture[M]) *Batch[M] {
	ds := make([]*Delivery[M], len(f.Deliveries))
	for i := range f.Deliveries {
		ds[i] = NewDeliveryFixture(f.Deliveries[i])
	}
	return &Batch[M]{deliveries: ds}
}
