package warren

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// TestPublisherConnPool_acquireWait_recordedOnSaturation proves the channel-pool
// acquire-wait is measured only when the pool is saturated (T71): the fast path
// (a free slot) records nothing; a blocked acquire records a positive wait.
func TestPublisherConnPool_acquireWait_recordedOnSaturation(t *testing.T) {
	var waits atomic.Int64
	var lastWait atomic.Int64 // nanoseconds
	p := newPublisherConnPool(1, func() (publisherEntry, error) {
		return publisherEntry{ch: newFakePubCh(true)}, nil
	})
	p.onAcquireWait = func(d time.Duration) {
		waits.Add(1)
		lastWait.Store(d.Nanoseconds())
	}

	// First acquire: a slot is free → no wait recorded.
	_, release1, err := p.acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), waits.Load(), "an immediately-available slot must record no wait")

	// Second acquire must block until the first releases → records a wait.
	acquired := make(chan struct{})
	go func() {
		_, release2, aerr := p.acquire(context.Background())
		if aerr == nil {
			release2()
		}
		close(acquired)
	}()

	time.Sleep(50 * time.Millisecond) // let the second acquire block on the token
	release1()
	<-acquired

	assert.Equal(t, int64(1), waits.Load(), "a saturated acquire must record exactly one wait")
	assert.Greater(t, lastWait.Load(), int64(0), "the recorded wait must be positive")
}

// redeliverInflightSpy captures redelivered + in-flight observations (T71).
type redeliverInflightSpy struct {
	metrics.NoOpConsumerMetrics
	redelivered atomic.Int64
	inFlightCur atomic.Int64
	inFlightMax atomic.Int64
}

func (s *redeliverInflightSpy) RecordRedelivered(_ string) { s.redelivered.Add(1) }
func (s *redeliverInflightSpy) ConsumerInFlightAdd(_ string, delta int64) {
	cur := s.inFlightCur.Add(delta)
	for {
		max := s.inFlightMax.Load()
		if cur <= max || s.inFlightMax.CompareAndSwap(max, cur) {
			break
		}
	}
}

// TestConsumer_redeliveredAndInFlight_metrics proves consumer_redelivered_total
// increments on a Redelivered delivery and consumer_in_flight rises then returns
// to zero around handler execution (T71 / DS-14).
func TestConsumer_redeliveredAndInFlight_metrics(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &redeliverInflightSpy{}
	consumer, err := ConsumerFor[string](conn).Queue("q").Metrics(cm).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	var handled atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			handled.Add(1)
			err := d.Ack()
			if handled.Load() == 2 {
				cancel()
			}
			return err
		})
	}()

	// One fresh delivery and one redelivered.
	deliveryCh <- amqp091.Delivery{Body: []byte(`"a"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	deliveryCh <- amqp091.Delivery{Body: []byte(`"b"`), ContentType: "application/json", Redelivered: true, Acknowledger: &fakeAcker{}}

	<-done
	assert.Equal(t, int64(1), cm.redelivered.Load(), "only the redelivered delivery must increment consumer_redelivered_total")
	assert.GreaterOrEqual(t, cm.inFlightMax.Load(), int64(1), "consumer_in_flight must rise above zero while a handler runs")
	assert.Equal(t, int64(0), cm.inFlightCur.Load(), "consumer_in_flight must return to zero after handlers finish")
}

// TestBatchConsumer_redelivered_metric proves consumer_redelivered_total
// increments on a Redelivered delivery in the batch path too — parity with the
// single-delivery Consumer (T71 / DS-14).
func TestBatchConsumer_redelivered_metric(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &redeliverInflightSpy{}
	bc, err := BatchConsumerFor[string](conn).Queue("q").Size(2).Metrics(cm).Build()
	require.NoError(t, err)
	defer func() { _ = bc.Close(context.Background()) }()

	deliveryCh := make(chan amqp091.Delivery, 2)
	bc.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = bc.Consume(ctx, func(_ context.Context, b *Batch[string]) error {
			err := b.Ack()
			cancel() // one batch is enough; stop the loop
			return err
		})
	}()

	// One fresh delivery and one redelivered → a size-2 batch flushes.
	deliveryCh <- amqp091.Delivery{Body: []byte(`"a"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	deliveryCh <- amqp091.Delivery{Body: []byte(`"b"`), ContentType: "application/json", Redelivered: true, Acknowledger: &fakeAcker{}}

	<-done
	assert.Equal(t, int64(1), cm.redelivered.Load(), "only the redelivered delivery must increment consumer_redelivered_total")
	assert.GreaterOrEqual(t, cm.inFlightMax.Load(), int64(1), "consumer_in_flight must rise above zero while the batch handler runs")
	assert.Equal(t, int64(0), cm.inFlightCur.Load(), "consumer_in_flight must return to zero after the batch handler finishes")
}
