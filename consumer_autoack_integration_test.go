//go:build integration

package warren_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// TestAutoAck_AtMostOnce_StreamedMessagesAreLost_integration documents the
// AutoAck (AMQP no-ack) trade-off end to end (T35 Verify):
//
//	publish 100 → an AutoAck consumer that stalls on the first message →
//	the broker drains all 100 anyway (no ack-gating backpressure) → "crash"
//	(stop) the consumer → a restarted consumer receives NONE of them.
//
// This is the at-most-once semantics SPEC §6.3 warns about, asserted as a
// deliberate property — not a regression. The contrast with manual ack (where
// the 99 unhandled deliveries WOULD be redelivered) is the whole point.
func TestAutoAck_AtMostOnce_StreamedMessagesAreLost_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		srcQ  = "test.autoack.atmostonce.src"
		total = 100
	)

	purgeQueues(t, url, srcQ)
	t.Cleanup(func() { deleteQueues(url, srcQ) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: srcQ, Durable: true, AutoDelete: false}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish 100 messages.
	pub, err := warren.PublisherFor[string](conn).Exchange("").RoutingKey(srcQ).Build()
	require.NoError(t, err)
	for i := range total {
		body := "msg-" + strconv.Itoa(i)
		require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))
	}

	// Raw AMQP channel for broker-side depth assertions (QueueInspect).
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup

	// Sanity: all 100 are enqueued before we attach the consumer.
	require.Eventually(t, func() bool {
		q, qerr := rawCh.QueueInspect(srcQ)
		return qerr == nil && q.Messages == total
	}, 5*time.Second, 50*time.Millisecond, "all %d messages must be enqueued", total)

	// — Phase 1: AutoAck consumer stalls on the first delivery ——————————————
	//
	// Prefetch is set above the backlog so the no-ack stream buffers entirely
	// client-side; the broker can then auto-ack (and remove) all 100 even though
	// only the first delivery ever reaches a handler.
	var entered atomic.Int64
	firstHandled := make(chan struct{})
	var firstOnce sync.Once

	consumer, err := warren.ConsumerFor[string](conn).
		Queue(srcQ).
		AutoAck().
		Prefetch(uint16(total + 50)).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.Consume(consumeCtx, func(hCtx context.Context, _ string) error {
			entered.Add(1)
			firstOnce.Do(func() { close(firstHandled) })
			<-hCtx.Done() // stall until the consumer is stopped ("crash")
			return nil
		})
	}()

	// The first delivery is now stuck in the handler (concurrency defaults to 1,
	// so no second delivery can enter while it holds the only slot).
	select {
	case <-firstHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first AutoAck delivery to be handled")
	}

	// The broker drains to empty despite the stalled handler: no-ack means the
	// broker acked every message the instant it dispatched it.
	require.Eventually(t, func() bool {
		q, qerr := rawCh.QueueInspect(srcQ)
		return qerr == nil && q.Messages == 0
	}, 10*time.Second, 100*time.Millisecond,
		"AutoAck has no backpressure: the broker must drain all %d messages even while the handler stalls", total)

	// Exactly one delivery ever reached a handler; the broker removed the other 99
	// while they sat unprocessed in the client buffer. This is the loss.
	assert.Equal(t, int64(1), entered.Load(),
		"only the first delivery was handled; the rest were auto-acked unprocessed")

	// — "Crash": stop the AutoAck consumer ——————————————————————————————————
	cancelConsume()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the AutoAck consumer to stop")
	}

	// — Phase 2: restart a manual-ack consumer; the streamed messages are gone ——
	restartConsumer, err := warren.ConsumerFor[string](conn).Queue(srcQ).Prefetch(10).Build()
	require.NoError(t, err)

	restartCtx, cancelRestart := context.WithCancel(ctx)
	defer cancelRestart()

	var redelivered atomic.Int64
	restartDone := make(chan struct{})
	go func() {
		defer close(restartDone)
		_ = restartConsumer.Consume(restartCtx, func(_ context.Context, _ string) error {
			redelivered.Add(1)
			return nil
		})
	}()

	// No message must ever arrive: the 99 unhandled deliveries were lost at-most-once.
	require.Never(t, func() bool { return redelivered.Load() > 0 }, 1500*time.Millisecond, 100*time.Millisecond,
		"AutoAck is at-most-once: previously-streamed messages must NOT be redelivered")

	cancelRestart()
	select {
	case <-restartDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the restarted consumer to stop")
	}

	// Broker-side confirmation: the queue is and stays empty.
	q, qerr := rawCh.QueueInspect(srcQ)
	require.NoError(t, qerr)
	assert.Equal(t, 0, q.Messages, "source queue must be empty: AutoAck dropped the unhandled messages")
}
