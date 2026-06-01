package warren

import (
	"errors"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/headers"
)

// Delivery wraps a broker-delivered message with its decoded payload [M].
//
// Tests fabricate a fake delivery with [NewDeliveryFixture].
type Delivery[M any] struct {
	body  *M
	queue string
	raw   amqp091.Delivery
	death headers.XDeathResult
	// done is closed by the owning Consumer[M].Close to signal consumer shutdown.
	// Nil means the consumer lifecycle is not being tracked (e.g. in tests).
	done <-chan struct{}
	// ackNotify is an optional callback invoked after a successful Ack or Nack.
	// BatchConsumer installs this to detect per-delivery acks and suppress the
	// batch-level auto-verdict (idempotent guard). Nil in all other code paths.
	ackNotify func()
	// resolved is the single-CAS resolved-once guard (T60 / CR-04). It is set
	// to true by the first Ack/Nack/AckIf/timeout-verdict; every later verdict
	// loses the CAS and is a no-op returning ErrAlreadyResolved. This prevents a
	// second wire frame (which channel-closes with PRECONDITION_FAILED and takes
	// out every in-flight handler on that channel).
	resolved atomic.Bool
}

// newDelivery constructs a Delivery[M] from a decoded body, the queue name,
// the raw amqp091.Delivery, and the consumer's done channel. Called by Consumer[M].
// Pass nil for done in unit tests that do not exercise consumer-closed behaviour.
func newDelivery[M any](body *M, queue string, d amqp091.Delivery, done <-chan struct{}) *Delivery[M] {
	return &Delivery[M]{
		body:  body,
		queue: queue,
		raw:   d,
		death: headers.ParseXDeath(d.Headers, queue),
		done:  done,
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

// Ack acknowledges the delivery to the broker.
// Returns ErrAlreadyClosed if the owning consumer was shut down, ErrAlreadyResolved
// if a verdict (Ack/Nack/AckIf or a HandlerTimeout verdict) was already emitted for
// this delivery (a no-op — no second frame), ErrChannelClosed if the underlying
// channel closed before the ack reached the broker.
func (d *Delivery[M]) Ack() error {
	if d.done != nil {
		select {
		case <-d.done:
			return ErrAlreadyClosed
		default:
		}
	}
	if !d.resolved.CompareAndSwap(false, true) {
		return ErrAlreadyResolved
	}
	if err := d.raw.Ack(false); err != nil {
		return mapAckErr(err)
	}
	if d.ackNotify != nil {
		d.ackNotify()
	}
	return nil
}

// Nack negatively acknowledges the delivery. requeue=true re-queues the message;
// requeue=false routes it to the DLX (or drops it).
// Returns ErrAlreadyClosed if the owning consumer was shut down, ErrAlreadyResolved
// if a verdict was already emitted for this delivery (a no-op — no second frame),
// ErrChannelClosed if the underlying channel closed before the nack reached the broker.
func (d *Delivery[M]) Nack(requeue bool) error {
	if d.done != nil {
		select {
		case <-d.done:
			return ErrAlreadyClosed
		default:
		}
	}
	if !d.resolved.CompareAndSwap(false, true) {
		return ErrAlreadyResolved
	}
	if err := d.raw.Nack(false, requeue); err != nil {
		return mapAckErr(err)
	}
	if d.ackNotify != nil {
		d.ackNotify()
	}
	return nil
}

// nackOnTimeout emits the HandlerTimeout verdict (basic.nack) through the same
// resolved-once CAS as the public Ack/Nack, so a timeout-verdict goroutine and a
// late handler verdict (esp. via ConsumeRaw) can never both emit a frame (CR-04).
// Unlike the public Nack it does not consult d.done — the timeout verdict must be
// sent regardless of consumer-shutdown state — and it swallows the broker error
// (the caller is the consumer loop, which has no error channel here). If a handler
// verdict already won the CAS, this is a no-op.
func (d *Delivery[M]) nackOnTimeout(requeue bool) {
	if !d.resolved.CompareAndSwap(false, true) {
		return
	}
	_ = d.raw.Nack(false, requeue)
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
