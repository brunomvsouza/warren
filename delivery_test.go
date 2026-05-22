package warren

import (
	"errors"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAcker records Ack/Nack calls for delivery tests.
type fakeAcker struct {
	acked    bool
	nacked   bool
	requeue  bool
	failWith error
}

func (f *fakeAcker) Ack(tag uint64, multiple bool) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.acked = true
	return nil
}

func (f *fakeAcker) Nack(tag uint64, multiple, requeue bool) error {
	if f.failWith != nil {
		return f.failWith
	}
	f.nacked = true
	f.requeue = requeue
	return nil
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
