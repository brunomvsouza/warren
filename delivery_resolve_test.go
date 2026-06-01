package warren

import (
	"errors"
	"sync"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T60 (R10-5 / DS-04 / CR-04): the resolved-once guard on Delivery[M] is a
// single atomic CAS — only the winner emits a wire frame; every later
// Ack/Nack/AckIf (or a timeout verdict followed by a late handler verdict) is a
// no-op returning ErrAlreadyResolved, never a second frame that channel-closes
// with PRECONDITION_FAILED and takes out every in-flight handler on the channel.

func TestDelivery_doubleAck_isNoOp(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})

	require.NoError(t, d.Ack())
	err := d.Ack()
	assert.ErrorIs(t, err, ErrAlreadyResolved, "second Ack must be a no-op sentinel")
	assert.Equal(t, 1, fa.frames(), "exactly one frame may be emitted")
}

func TestDelivery_ackThenNack_isNoOp(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})

	require.NoError(t, d.Ack())
	err := d.Nack(true)
	assert.ErrorIs(t, err, ErrAlreadyResolved)
	assert.Equal(t, 1, fa.frames(), "Nack after Ack must not emit a second frame")
	assert.True(t, fa.acked)
	assert.False(t, fa.nacked)
}

func TestDelivery_doubleNack_isNoOp(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})

	require.NoError(t, d.Nack(false))
	err := d.Nack(false)
	assert.ErrorIs(t, err, ErrAlreadyResolved)
	assert.Equal(t, 1, fa.frames())
}

func TestDelivery_doubleAckIf_isNoOp(t *testing.T) {
	fa := &fakeAcker{}
	d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})

	require.NoError(t, d.AckIf(nil))
	err := d.AckIf(errors.New("boom"))
	assert.ErrorIs(t, err, ErrAlreadyResolved)
	assert.Equal(t, 1, fa.frames())
}

// TestDelivery_concurrentTimeoutVerdictVsHandlerAck_exactlyOneFrame models the
// CR-04 race directly: a timeout-verdict goroutine (nackOnTimeout) and a
// handler-Ack goroutine race on the same Delivery. Exactly one frame must be
// emitted; run with -race.
func TestDelivery_concurrentTimeoutVerdictVsHandlerAck_exactlyOneFrame(t *testing.T) {
	for i := 0; i < 500; i++ {
		fa := &fakeAcker{}
		d := makeTestDelivery[string](nil, "q", amqp091.Delivery{Acknowledger: fa, DeliveryTag: 1})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); d.nackOnTimeout(false) }()
		go func() { defer wg.Done(); _ = d.Ack() }()
		wg.Wait()

		require.Equal(t, 1, fa.frames(), "iteration %d: exactly one frame must win the CAS", i)
	}
}
