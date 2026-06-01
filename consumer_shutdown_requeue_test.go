package warren

import (
	"errors"
	"sync/atomic"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
)

type shutdownSpyMetrics struct {
	metrics.NoOpConsumerMetrics
	requeued atomic.Int64
}

func (m *shutdownSpyMetrics) RecordShutdownRequeued(_ string) { m.requeued.Add(1) }

// T70 (DS-03 / SRE-07): prefetched-but-undispatched deliveries are
// Nack(requeue=true)'d at shutdown (never silently dropped) and counted by
// consumer_shutdown_requeued_total.
func TestRequeueUndispatched_nacksAndCounts(t *testing.T) {
	cm := &shutdownSpyMetrics{}
	a1, a2 := &fakeAcker{}, &fakeAcker{}
	ch := make(chan amqp091.Delivery, 3)
	ch <- amqp091.Delivery{Acknowledger: a1, DeliveryTag: 1}
	ch <- amqp091.Delivery{Acknowledger: a2, DeliveryTag: 2}

	requeueUndispatched(ch, false /* brokerAutoAck */, cm, "orders")

	assert.True(t, a1.nacked && a1.requeue, "first undispatched delivery must be nack-requeued")
	assert.True(t, a2.nacked && a2.requeue, "second undispatched delivery must be nack-requeued")
	assert.Equal(t, int64(2), cm.requeued.Load(), "both requeues must be counted")
}

func TestRequeueUndispatched_autoAckSkips(t *testing.T) {
	cm := &shutdownSpyMetrics{}
	a := &fakeAcker{}
	ch := make(chan amqp091.Delivery, 1)
	ch <- amqp091.Delivery{Acknowledger: a, DeliveryTag: 1}

	requeueUndispatched(ch, true /* brokerAutoAck — nothing to nack */, cm, "orders")

	assert.False(t, a.nacked, "no-ack consumer must not nack (broker already acked on dispatch)")
	assert.Equal(t, int64(0), cm.requeued.Load())
}

func TestRequeueUndispatched_nilChannel(t *testing.T) {
	cm := &shutdownSpyMetrics{}
	requeueUndispatched(nil, false, cm, "orders") // must not panic
	assert.Equal(t, int64(0), cm.requeued.Load())
}

// TestRequeueUndispatched_stopsOnClosedChannel confirms a closed channel ends
// the drain (no spin, no panic).
func TestRequeueUndispatched_stopsOnClosedChannel(t *testing.T) {
	cm := &shutdownSpyMetrics{}
	a := &fakeAcker{}
	ch := make(chan amqp091.Delivery, 2)
	ch <- amqp091.Delivery{Acknowledger: a, DeliveryTag: 1}
	close(ch)

	requeueUndispatched(ch, false, cm, "orders")
	require.True(t, a.nacked)
	assert.Equal(t, int64(1), cm.requeued.Load())
}

// TestRequeueUndispatched_nackErrorStopsDrain locks the documented contract when a
// shutdown-time Nack errors: the drain STOPS rather than continuing. This is
// deliberate and is NOT a silent drop — a nack error means the channel is already
// gone, and the broker requeues every unacked delivery on channel close, so the
// remaining buffered deliveries are still redelivered (at-least-once, §6.2.1). The
// counter therefore reflects only confirmed requeues (a lower bound). A naive
// "keep draining past the error" change would be wrong: nacking on a dead channel
// would error on every remaining delivery and the broker handles them anyway.
func TestRequeueUndispatched_nackErrorStopsDrain(t *testing.T) {
	cm := &shutdownSpyMetrics{}
	dead := &fakeAcker{failWith: errors.New("channel/connection is not open")}
	live := &fakeAcker{}
	ch := make(chan amqp091.Delivery, 3)
	ch <- amqp091.Delivery{Acknowledger: dead, DeliveryTag: 1} // first nack errors
	ch <- amqp091.Delivery{Acknowledger: live, DeliveryTag: 2} // must be left for broker requeue-on-close

	requeueUndispatched(ch, false /* brokerAutoAck */, cm, "orders")

	assert.False(t, live.nacked,
		"drain must stop on the first nack error — the second delivery is left for the broker's requeue-on-close")
	assert.Equal(t, 1, len(ch),
		"the undrained delivery must remain buffered (requeued by the broker on close), not silently dropped")
	assert.Equal(t, int64(0), cm.requeued.Load(),
		"only successful requeues are counted; the errored nack is not")
}
