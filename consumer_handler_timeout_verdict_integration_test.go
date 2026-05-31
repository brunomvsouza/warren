//go:build integration

package warren_test

import (
	"context"
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

// spyConsumerMetrics records handler outcomes for assertion in T18b tests.
// Implements metrics.ConsumerMetrics.
//
// NOTE(T19): when T19 extends the ConsumerMetrics interface (adding methods such as
// RecordDecodeError, RecordCancelled, InFlightAdd), add stub implementations of those
// methods here so the spy continues to satisfy the interface.
type spyConsumerMetrics struct {
	timeoutTotal    atomic.Int64
	mu              sync.Mutex
	handlerOutcomes []string
}

func (s *spyConsumerMetrics) RecordResubscribed(_ string)                {}
func (s *spyConsumerMetrics) RecordHandlerAbortedChannelClosed(_ string) {}
func (s *spyConsumerMetrics) RecordHandlerTimeout(_ string) {
	s.timeoutTotal.Add(1)
}
func (s *spyConsumerMetrics) RecordHandler(_ string, _ string, outcome string, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlerOutcomes = append(s.handlerOutcomes, outcome)
}
func (s *spyConsumerMetrics) RecordReplierDropNoDLX(_ string)    {}
func (s *spyConsumerMetrics) RecordCancelled(_, _ string)        {}
func (s *spyConsumerMetrics) RecordMaxRedeliveries(_, _ string)  {}
func (s *spyConsumerMetrics) InFlightBytesAdd(_ string, _ int64) {}

func (s *spyConsumerMetrics) outcomes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.handlerOutcomes))
	copy(out, s.handlerOutcomes)
	return out
}

// TestHandlerTimeoutVerdict_NackNoRequeue_integration (T18b case A):
//   - HandlerTimeout(50ms) with a handler that blocks on ctx.Done.
//   - Default verdict = TimeoutNackNoRequeue.
//   - Asserts: (1) handler ctx is cancelled near 50ms; (2) message is
//     dead-lettered to the configured DLX; (3) consumer_handler_timeout_total
//     == 1 and outcome label == "timeout_nack_no_requeue"; (4) source queue
//     is empty after the nack (no redelivery).
func TestHandlerTimeoutVerdict_NackNoRequeue_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		srcQ    = "test.htv.nack.src"
		dlxExch = "test.htv.nack.dlx"
		dlqQ    = "test.htv.nack.dlq"
	)

	// Purge any leftover messages from a previous run.
	purgeQueues(t, url, srcQ, dlqQ)

	// Cleanup: delete queues and exchanges after the test so future runs start clean.
	// deleteExchanges is called separately because the exchange is declared with
	// AutoDelete: false and won't be removed when queues are deleted.
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, dlxExch)
	})

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare topology explicitly: source queue with x-dead-letter-exchange arg,
	// a fanout DLX exchange, DLQ queue, and the binding that routes dead letters.
	// NOTE: Topology.DeadLetters expansion is intentionally not used here because it
	// does not auto-create the binding between the DLX exchange and the DLQ queue
	// (tracked as LATER-20). The explicit declaration below exercises the same
	// consumer-side verdict path while keeping the broker topology correct.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: dlxExch, Kind: warren.ExchangeFanout, Durable: false, AutoDelete: false},
		},
		Queues: []warren.Queue{
			{
				Name:    srcQ,
				Durable: false,
				Args:    map[string]any{"x-dead-letter-exchange": dlxExch},
			},
			{Name: dlqQ, Durable: false},
		},
		Bindings: []warren.Binding{
			{Exchange: dlxExch, Queue: dlqQ, RoutingKey: ""},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish one message to the source queue.
	pub, err := warren.PublisherFor[string](conn).
		Exchange("").
		RoutingKey(srcQ).
		Build()
	require.NoError(t, err)
	body := "hello-nack"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	spy := &spyConsumerMetrics{}
	var (
		handlerStarted time.Time
		ctxCancelTime  time.Time
		handlerDone    = make(chan struct{})
	)

	consumer, err := warren.ConsumerFor[string](conn).
		Queue(srcQ).
		Prefetch(1).
		HandlerTimeout(50 * time.Millisecond).
		// TimeoutNackNoRequeue is the default; no explicit call needed.
		Metrics(spy).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.Consume(consumeCtx, func(hCtx context.Context, _ string) error {
			defer close(handlerDone)
			handlerStarted = time.Now()
			<-hCtx.Done() // block until HandlerTimeout fires
			ctxCancelTime = time.Now()
			cancelConsume() // stop consumer after this message
			return nil
		})
	}()

	// Wait for handler to observe ctx cancellation.
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler ctx to be cancelled by HandlerTimeout")
	}

	// Wait for the consumer goroutine to exit cleanly.
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer goroutine to exit")
	}

	// (1) Handler ctx cancelled near 50ms deadline.
	elapsed := ctxCancelTime.Sub(handlerStarted)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond,
		"ctx should be cancelled near the 50ms deadline")
	assert.Less(t, elapsed, 200*time.Millisecond,
		"ctx cancelled too late (handler should not run 200ms)")

	// Open a raw AMQP connection for broker-side assertions (2) and (4).
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup — error is non-actionable in defer
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup — error is non-actionable in defer

	// (2) Message landed on DLQ — poll until the broker routes the dead-lettered message.
	// Using require.Eventually avoids a brittle fixed sleep while bounding the wait.
	var dlqBody string
	require.Eventually(t, func() bool {
		msg, ok, err := rawCh.Get(dlqQ, true)
		if err != nil || !ok {
			return false
		}
		dlqBody = string(msg.Body)
		return true
	}, 3*time.Second, 100*time.Millisecond,
		"DLQ must contain the dead-lettered message after nack-no-requeue")
	assert.Equal(t, `"hello-nack"`, dlqBody)

	// (3) consumer_handler_timeout_total incremented; outcome label is nack_no_requeue.
	assert.Equal(t, int64(1), spy.timeoutTotal.Load(),
		"consumer_handler_timeout_total must be 1")
	outcomes := spy.outcomes()
	require.Len(t, outcomes, 1, "exactly one RecordHandler call expected")
	assert.Equal(t, "timeout_nack_no_requeue", outcomes[0],
		"consumer_handler_seconds outcome label must be timeout_nack_no_requeue")

	// (4) Source queue must be empty — no redelivery.
	var srcOk bool
	_, srcOk, err = rawCh.Get(srcQ, true)
	require.NoError(t, err)
	assert.False(t, srcOk, "source queue must be empty: message must not be requeued")
}

// TestHandlerTimeoutVerdict_NackRequeue_integration (T18b case B):
//   - Same slow handler (blocks on ctx.Done).
//   - Explicit HandlerTimeoutVerdict(TimeoutNackRequeue).
//   - Asserts: (1) message is redelivered at least once (deliveryCount >= 2);
//     (2) outcome label is "timeout_nack_requeue" for every invocation.
//
// NOTE: the x-delivery-limit exhaustion scenario (message dead-lettered after
// reaching the broker-side delivery cap) is deferred — see LATER-21.
func TestHandlerTimeoutVerdict_NackRequeue_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const srcQ = "test.htv.requeue.src"

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
		Queues: []warren.Queue{
			{Name: srcQ, Durable: false, AutoDelete: false},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish one message.
	pub, err := warren.PublisherFor[string](conn).
		Exchange("").
		RoutingKey(srcQ).
		Build()
	require.NoError(t, err)
	requeueBody := "hello-requeue"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &requeueBody}))

	spy := &spyConsumerMetrics{}
	var deliveryCount atomic.Int64
	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()

	consumer, err := warren.ConsumerFor[string](conn).
		Queue(srcQ).
		Prefetch(1).
		HandlerTimeout(50 * time.Millisecond).
		HandlerTimeoutVerdict(warren.TimeoutNackRequeue).
		Metrics(spy).
		Build()
	require.NoError(t, err)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.Consume(consumeCtx, func(hCtx context.Context, _ string) error {
			count := deliveryCount.Add(1)
			<-hCtx.Done() // block until HandlerTimeout fires
			if count >= 2 {
				cancelConsume() // seen at least one redelivery — stop the consumer
			}
			return nil
		})
	}()

	// Wait for consumer to exit (triggered once deliveryCount >= 2).
	select {
	case <-consumerDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for redelivery; deliveryCount=%d", deliveryCount.Load())
	}

	// (1) Message was redelivered at least once (handler called >= 2 times).
	dc := deliveryCount.Load()
	assert.GreaterOrEqual(t, dc, int64(2),
		"message must be redelivered at least once (deliveryCount must be >= 2)")

	// (2) All metric outcome labels must be "timeout_nack_requeue".
	// Parity: timeoutTotal and len(outcomes) must equal deliveryCount — each delivery
	// must produce exactly one RecordHandlerTimeout and one RecordHandler call.
	assert.Equal(t, dc, spy.timeoutTotal.Load(),
		"consumer_handler_timeout_total must equal deliveryCount")
	outcomes := spy.outcomes()
	assert.Equal(t, int(dc), len(outcomes),
		"RecordHandler call count must equal deliveryCount")
	for _, o := range outcomes {
		assert.Equal(t, "timeout_nack_requeue", o,
			"all consumer_handler_seconds outcomes must be timeout_nack_requeue")
	}
}
