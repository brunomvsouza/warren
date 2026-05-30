//go:build integration

package warren_test

import (
	"context"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/metrics"
)

// resubscribeCountingMetrics records consumer_resubscribed_total increments per queue.
type resubscribeCountingMetrics struct {
	metrics.NoOpConsumerMetrics
	mu     sync.Mutex
	counts map[string]int
}

func (m *resubscribeCountingMetrics) RecordResubscribed(queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[queue]++
}

func (m *resubscribeCountingMetrics) count(queue string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[queue]
}

// TestConsumer_OnResubscribe_FiresOnReconnect_integration closes the E2E loop for the
// resubscribe hook (T34, SPEC §6.1) against a real broker: a consumer is attached, the
// connection is forced to reconnect, and the consumer must reopen its channel, reissue
// basic.consume, fire WithOnResubscribe(queue), and increment
// consumer_resubscribed_total{queue}. The unit suite only exercises the notifyResubscribed
// seam; this proves the reconnect path in runConsume actually reaches it.
func TestConsumer_OnResubscribe_FiresOnReconnect_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const q = "test.resubscribe.onreconnect"
	purgeQueues(t, url, q)
	t.Cleanup(func() { deleteQueues(url, q) })

	resubCh := make(chan string, 4)
	conn, err := warren.Dial(ctx,
		warren.WithAddr(url),
		warren.WithOnResubscribe(func(queue string) {
			select {
			case resubCh <- queue:
			default:
			}
		}),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{Queues: []warren.Queue{{Name: q, Durable: true}}}
	require.NoError(t, topo.Declare(ctx, conn))

	cm := &resubscribeCountingMetrics{}
	consumer, err := warren.ConsumerFor[string](conn).
		Queue(q).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Consume(consumeCtx, func(context.Context, string) error { return nil })
	}()

	// Raw AMQP channel just to observe the consumer attaching before we reconnect.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck // raw AMQP cleanup
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck // raw AMQP cleanup

	require.Eventually(t, func() bool {
		qi, e := rawCh.QueueInspect(q)
		return e == nil && qi.Consumers >= 1
	}, 5*time.Second, 100*time.Millisecond, "consumer must attach before the forced reconnect")

	// Force every managed connection (including the consumer's) to drop and reconnect;
	// the consumer must reopen its channel, reissue basic.consume, and resubscribe.
	require.NoError(t, conn.ForceReconnect())

	select {
	case got := <-resubCh:
		assert.Equal(t, q, got, "WithOnResubscribe must fire with the resubscribed queue name")
	case <-time.After(15 * time.Second):
		t.Fatal("WithOnResubscribe did not fire after ForceReconnect")
	}

	assert.Eventually(t, func() bool {
		return cm.count(q) >= 1
	}, 5*time.Second, 50*time.Millisecond, "consumer_resubscribed_total must increment for the queue")

	// Re-attach proven; tear the consumer down cleanly so goleak stays green.
	cancelConsume()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop after ctx cancel")
	}
}
