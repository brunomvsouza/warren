package warren

import (
	"errors"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/headers"
)

// Delivery wraps a broker-delivered message with its decoded payload [M].
// Tests that need a fake delivery use amqpmock.NewDelivery[M] from amqpmock/.
type Delivery[M any] struct {
	body  *M
	queue string
	raw   amqp091.Delivery
	death headers.XDeathResult
}

// NewDelivery constructs a Delivery[M] from a decoded body, the queue name,
// and the raw amqp091.Delivery. Intended for the consumer path and tests.
func NewDelivery[M any](body *M, queue string, d amqp091.Delivery) *Delivery[M] {
	death := headers.ParseXDeath(amqp091.Table(d.Headers), queue)
	return &Delivery[M]{
		body:  body,
		queue: queue,
		raw:   d,
		death: death,
	}
}

// Body returns a pointer to the decoded message payload.
func (d *Delivery[M]) Body() *M { return d.body }

// Headers returns the AMQP header table from the delivery.
func (d *Delivery[M]) Headers() Headers { return Headers(d.raw.Headers) }

// Redelivered reports whether the broker has previously attempted to deliver this message.
func (d *Delivery[M]) Redelivered() bool { return d.raw.Redelivered }

// DeliveryTag is the broker-assigned sequential identifier for this delivery.
func (d *Delivery[M]) DeliveryTag() uint64 { return d.raw.DeliveryTag }

// MessageID returns the application-level message identifier from the AMQP properties.
func (d *Delivery[M]) MessageID() string { return d.raw.MessageId }

// CorrelationID returns the correlation identifier from the AMQP properties.
func (d *Delivery[M]) CorrelationID() string { return d.raw.CorrelationId }

// Timestamp returns the message timestamp from the AMQP properties.
func (d *Delivery[M]) Timestamp() time.Time { return d.raw.Timestamp }

// DeathCount returns the sum of x-death counts for reason ∈ {rejected, delivery-limit}
// matching the delivery's current queue. Returns 0 if the header is absent or malformed.
func (d *Delivery[M]) DeathCount() int { return d.death.Count }

// DeathCountByReason returns the total x-death count for a specific reason string
// (e.g. "rejected", "expired", "maxlen", "delivery-limit") for the current queue.
func (d *Delivery[M]) DeathCountByReason(reason string) int {
	return d.death.CountByReason(reason)
}

// DeathReasons returns the unique x-death reasons in declaration order for the
// current queue. Useful for custom redelivery policies that need all reasons.
func (d *Delivery[M]) DeathReasons() []string { return d.death.Reasons }

// Ack acknowledges the delivery to the broker. Returns ErrChannelClosed if
// the underlying channel was closed before the ack could be sent.
func (d *Delivery[M]) Ack() error {
	if err := d.raw.Ack(false); err != nil {
		return mapAckErr(err)
	}
	return nil
}

// Nack negatively acknowledges the delivery. requeue=true re-queues the message;
// requeue=false routes it to the DLX (or drops it). Returns ErrChannelClosed if
// the underlying channel was closed.
func (d *Delivery[M]) Nack(requeue bool) error {
	if err := d.raw.Nack(false, requeue); err != nil {
		return mapAckErr(err)
	}
	return nil
}

// AckIf applies the standard handler error-mapping semantics:
//   - nil → Ack
//   - errors.Is(err, ErrRequeue) → Nack(requeue=true)
//   - any other error → Nack(requeue=false)
func (d *Delivery[M]) AckIf(handlerErr error) error {
	if handlerErr == nil {
		return d.Ack()
	}
	requeue := errors.Is(handlerErr, ErrRequeue)
	return d.Nack(requeue)
}

// mapAckErr converts broker-level acknowledgement errors to warren sentinels.
func mapAckErr(err error) error {
	if errors.Is(err, amqp091.ErrClosed) {
		return ErrChannelClosed
	}
	return err
}
