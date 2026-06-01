package warren

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAcker records Ack/Nack calls for delivery tests.
type fakeAcker struct {
	mu        sync.Mutex
	acked     bool
	nacked    bool
	requeue   bool
	ackCount  int
	nackCount int
	failWith  error
}

func (f *fakeAcker) Ack(tag uint64, multiple bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.acked = true
	f.ackCount++
	return nil
}

func (f *fakeAcker) Nack(tag uint64, multiple, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.nacked = true
	f.requeue = requeue
	f.nackCount++
	return nil
}

// frames returns the total number of ack+nack frames emitted.
func (f *fakeAcker) frames() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ackCount + f.nackCount
}

func (f *fakeAcker) Reject(tag uint64, requeue bool) error { return nil }

func makeTestDelivery[M any](body *M, queue string, d amqp091.Delivery) *Delivery[M] {
	return newDelivery(body, queue, d, nil)
}

func TestDelivery_Body(t *testing.T) {
	val := "hello"
	d := makeTestDelivery(&val, "q", amqp091.Delivery{})
	require.Equal(t, &val, d.Body())
}

func TestDelivery_Headers(t *testing.T) {
	amqpDel := amqp091.Delivery{Headers: amqp091.Table{"foo": "bar"}}
	d := makeTestDelivery[string](nil, "q", amqpDel)
	assert.Equal(t, Headers{"foo": "bar"}, d.Headers())
}

func TestDelivery_Redelivered(t *testing.T) {
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Redelivered: true})
	assert.True(t, d.Redelivered())
}

func TestDelivery_DeliveryTag(t *testing.T) {
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{DeliveryTag: 42})
	assert.Equal(t, uint64(42), d.DeliveryTag())
}

func TestDelivery_MessageID(t *testing.T) {
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{MessageId: "msg-123"})
	assert.Equal(t, "msg-123", d.MessageID())
}

func TestDelivery_CorrelationID(t *testing.T) {
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{CorrelationId: "corr-456"})
	assert.Equal(t, "corr-456", d.CorrelationID())
}

func TestDelivery_Timestamp(t *testing.T) {
	ts := time.Now().Truncate(time.Second)
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Timestamp: ts})
	assert.Equal(t, ts, d.Timestamp())
}

func TestDelivery_DeathCount_Absent(t *testing.T) {
	d := makeTestDelivery[string](nil, "myqueue", amqp091.Delivery{})
	assert.Equal(t, 0, d.DeathCount())
}

func TestDelivery_DeathCount_RejectedOnly(t *testing.T) {
	amqpDel := amqp091.Delivery{
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{"queue": "myqueue", "reason": "rejected", "count": int64(3)},
			},
		},
	}
	d := makeTestDelivery[string](nil, "myqueue", amqpDel)
	assert.Equal(t, 3, d.DeathCount())
}

func TestDelivery_DeathCount_FilterExpired(t *testing.T) {
	// expired=100, rejected=2 → DeathCount() must return 2 only
	amqpDel := amqp091.Delivery{
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{"queue": "myqueue", "reason": "expired", "count": int64(100)},
				amqp091.Table{"queue": "myqueue", "reason": "rejected", "count": int64(2)},
			},
		},
	}
	d := makeTestDelivery[string](nil, "myqueue", amqpDel)
	assert.Equal(t, 2, d.DeathCount())
}

func TestDelivery_DeathCountByReason(t *testing.T) {
	amqpDel := amqp091.Delivery{
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{"queue": "myqueue", "reason": "expired", "count": int64(100)},
				amqp091.Table{"queue": "myqueue", "reason": "rejected", "count": int64(2)},
			},
		},
	}
	d := makeTestDelivery[string](nil, "myqueue", amqpDel)
	assert.Equal(t, 100, d.DeathCountByReason("expired"))
	assert.Equal(t, 2, d.DeathCountByReason("rejected"))
	assert.Equal(t, 0, d.DeathCountByReason("delivery-limit"))
}

func TestDelivery_DeathReasons(t *testing.T) {
	amqpDel := amqp091.Delivery{
		Headers: amqp091.Table{
			"x-death": []any{
				amqp091.Table{"queue": "myqueue", "reason": "expired", "count": int64(1)},
				amqp091.Table{"queue": "myqueue", "reason": "rejected", "count": int64(1)},
			},
		},
	}
	d := makeTestDelivery[string](nil, "myqueue", amqpDel)
	assert.Equal(t, []string{"expired", "rejected"}, d.DeathReasons())
}

func TestDelivery_AckIf_Nil(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	require.NoError(t, d.AckIf(nil))
	assert.True(t, fa.acked)
	assert.False(t, fa.nacked)
}

func TestDelivery_AckIf_ErrRequeue(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	require.NoError(t, d.AckIf(ErrRequeue))
	assert.False(t, fa.acked)
	assert.True(t, fa.nacked)
	assert.True(t, fa.requeue)
}

func TestDelivery_AckIf_OtherError(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	require.NoError(t, d.AckIf(errors.New("something failed")))
	assert.False(t, fa.acked)
	assert.True(t, fa.nacked)
	assert.False(t, fa.requeue)
}

func TestDelivery_Ack_ChannelClosed(t *testing.T) {
	fa := &fakeAcker{failWith: amqp091.ErrClosed}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	assert.ErrorIs(t, d.Ack(), ErrChannelClosed)
}

func TestDelivery_Nack_ChannelClosed(t *testing.T) {
	fa := &fakeAcker{failWith: amqp091.ErrClosed}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	assert.ErrorIs(t, d.Nack(false), ErrChannelClosed)
}

func TestDelivery_Ack_ConsumerClosed(t *testing.T) {
	done := make(chan struct{})
	close(done)
	fa := &fakeAcker{}
	d := newDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1}, done)
	assert.ErrorIs(t, d.Ack(), ErrAlreadyClosed)
	assert.False(t, fa.acked, "Ack must not reach broker after consumer close")
}

func TestDelivery_Nack_ConsumerClosed(t *testing.T) {
	done := make(chan struct{})
	close(done)
	fa := &fakeAcker{}
	d := newDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1}, done)
	assert.ErrorIs(t, d.Nack(false), ErrAlreadyClosed)
	assert.False(t, fa.nacked, "Nack must not reach broker after consumer close")
}

func TestDelivery_AckIf_ConsumerClosed(t *testing.T) {
	done := make(chan struct{})
	close(done)
	fa := &fakeAcker{}
	d := newDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1}, done)
	assert.ErrorIs(t, d.AckIf(nil), ErrAlreadyClosed)
}

func TestDelivery_Ack_WithLiveDoneChannel(t *testing.T) {
	done := make(chan struct{}) // open, not closed — consumer still running
	fa := &fakeAcker{}
	d := newDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1}, done)
	require.NoError(t, d.Ack())
	assert.True(t, fa.acked, "Ack must reach broker when consumer is still running")
}

func TestDelivery_Nack_WithLiveDoneChannel(t *testing.T) {
	done := make(chan struct{}) // open, not closed
	fa := &fakeAcker{}
	d := newDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1}, done)
	require.NoError(t, d.Nack(true))
	assert.True(t, fa.nacked)
	assert.True(t, fa.requeue)
}

func TestDelivery_Ack_UnknownBrokerError(t *testing.T) {
	sentinel := errors.New("unexpected broker error")
	fa := &fakeAcker{failWith: sentinel}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	err := d.Ack()
	assert.ErrorIs(t, err, sentinel, "unknown broker errors must pass through mapAckErr unchanged")
}

func TestDelivery_Nack_UnknownBrokerError(t *testing.T) {
	sentinel := errors.New("unexpected broker error")
	fa := &fakeAcker{failWith: sentinel}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	err := d.Nack(false)
	assert.ErrorIs(t, err, sentinel, "unknown broker errors must pass through mapAckErr unchanged")
}

func TestDelivery_AckIf_WrappedErrRequeue(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})
	wrapped := fmt.Errorf("handler context: %w", ErrRequeue)
	require.NoError(t, d.AckIf(wrapped))
	assert.True(t, fa.nacked, "wrapped ErrRequeue must still trigger Nack")
	assert.True(t, fa.requeue, "wrapped ErrRequeue must nack with requeue=true")
}
