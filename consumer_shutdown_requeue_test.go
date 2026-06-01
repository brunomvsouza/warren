package warren

import (
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
