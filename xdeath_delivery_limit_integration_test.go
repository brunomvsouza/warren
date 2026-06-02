//go:build integration

package warren_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// TestConsumer_xdeath_deliveryLimit_DeathCount_integration (T75 / RMQ-01):
// Real-broker replacement for the previously fabricated delivery-limit unit
// test. It drives a genuine quorum x-delivery-limit eviction and asserts that
// warren's public Delivery x-death API — DeathCount / DeathCountByReason /
// DeathReasons — surfaces it against the reason atom the broker actually emits.
//
// Gate G1 established that RabbitMQ (3.13 and 4.x) writes the reason with an
// UNDERSCORE ("delivery_limit"); warren normalises it to the documented
// hyphenated "delivery-limit". This test locks that fix in against a header the
// broker really produced — no fabricated x-death table.
//
// Why the captured header is replayed through a fixture: a delivery-limit
// eviction is keyed on the SOURCE quorum queue, and the broker will never
// redeliver such a message back to that same queue — returning it (even via an
// intermediate TTL hop) is silently dropped, and a quorum queue re-evicts a
// message that already exceeded its limit. The only place the delivery_limit
// x-death entry is observable is the DLQ, where it is keyed on the source queue
// (not the DLQ the consumer reads from). So we capture the real header on the
// DLQ and replay it scoped to the source queue via NewDeliveryFixture, which is
// exactly the scope a Death* lookup uses. This keeps the x-death table 100%
// broker-authored while asserting the public accessors. (See LATER for the
// keying observation.)
func TestConsumer_xdeath_deliveryLimit_DeathCount_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const (
		inExch = "test.xdeath.dl.in"
		dlx    = "test.xdeath.dl.dlx"
		srcQ   = "test.xdeath.dl.src"
		dlqQ   = "test.xdeath.dl.dlq"
	)

	// Quorum queues are durable; clear any leftover before declaring our args.
	deleteQueues(url, srcQ, dlqQ)
	deleteExchanges(url, inExch, dlx)
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, inExch, dlx)
	})

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: inExch, Kind: warren.ExchangeDirect, Durable: true},
			{Name: dlx, Kind: warren.ExchangeFanout, Durable: true},
		},
		Queues: []warren.Queue{
			{
				Name:          srcQ,
				Durable:       true,
				Type:          warren.QueueTypeQuorum,
				DeliveryLimit: 1, // two attempts (initial + one requeue) trip the limit
				Args:          map[string]any{"x-dead-letter-exchange": dlx},
			},
			{Name: dlqQ, Durable: true},
		},
		Bindings: []warren.Binding{
			{Exchange: inExch, Queue: srcQ, RoutingKey: "k"},
			{Exchange: dlx, Queue: dlqQ, RoutingKey: ""},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	pub, err := warren.PublisherFor[string](conn).Exchange(inExch).RoutingKey("k").Build()
	require.NoError(t, err)
	body := "poison"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	// Source consumer: nack-with-requeue every delivery so the quorum delivery
	// counter climbs past x-delivery-limit and the broker dead-letters the message.
	srcConsumer, err := warren.ConsumerFor[string](conn).Queue(srcQ).Prefetch(1).Build()
	require.NoError(t, err)
	srcCtx, srcCancel := context.WithCancel(ctx)
	defer srcCancel()
	go func() {
		_ = srcConsumer.ConsumeRaw(srcCtx, func(_ context.Context, d *warren.Delivery[string]) error {
			// ConsumeRaw hands the verdict to the handler: nack-requeue explicitly.
			return d.Nack(true)
		})
	}()

	// DLQ consumer: capture the broker-authored x-death header off the
	// dead-lettered message. Its OWN DeathCount() is 0 here — the entry is keyed
	// on the source queue, not the DLQ this consumer reads from.
	dlqConsumer, err := warren.ConsumerFor[string](conn).Queue(dlqQ).Prefetch(1).Build()
	require.NoError(t, err)
	dlqCtx, dlqCancel := context.WithCancel(ctx)
	defer dlqCancel()

	var (
		mu           sync.Mutex
		realHeaders  warren.Headers
		dlqDeathSelf int
		captured     bool
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = dlqConsumer.ConsumeRaw(dlqCtx, func(_ context.Context, d *warren.Delivery[string]) error {
			mu.Lock()
			realHeaders = d.Headers()
			dlqDeathSelf = d.DeathCount() // keyed on dlqQ → expected 0
			captured = true
			mu.Unlock()
			dlqCancel()
			return d.Ack()
		})
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		dlqCancel()
		t.Fatal("message did not dead-letter to the DLQ after exceeding x-delivery-limit")
	}
	srcCancel()

	mu.Lock()
	defer mu.Unlock()
	require.True(t, captured, "DLQ consumer must have captured the dead-lettered message")
	require.NotEmpty(t, realHeaders["x-death"], "dead-lettered message must carry a broker x-death header")
	assert.Equal(t, 0, dlqDeathSelf,
		"DeathCount() on the DLQ consumer is keyed on the DLQ and must not see the source queue's death")

	// Replay the REAL broker header scoped to the source queue (the scope a
	// delivery-limit consumer's DeathCount uses) through the public fixture.
	fx := warren.NewDeliveryFixture(warren.DeliveryFixture[string]{
		Queue:   srcQ,
		Headers: realHeaders,
	})
	assert.GreaterOrEqual(t, fx.DeathCount(), 1,
		"DeathCount must count the broker's delivery-limit eviction for the source queue")
	assert.GreaterOrEqual(t, fx.DeathCountByReason("delivery-limit"), 1,
		"the canonical hyphenated reason must resolve against the real broker header")
	assert.Equal(t, fx.DeathCountByReason("delivery-limit"), fx.DeathCountByReason("delivery_limit"),
		"the raw broker spelling \"delivery_limit\" must resolve to the same count")
	assert.Contains(t, fx.DeathReasons(), "delivery-limit",
		"DeathReasons must surface the canonical hyphenated reason atom")
}
