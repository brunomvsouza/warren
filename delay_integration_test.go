//go:build integration

package warren_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// delayPayload is the trivial JSON body the delayed-delivery round-trip carries.
type delayPayload struct {
	N int `json:"n"`
}

// requireDelayedExchange fails the test loudly when the broker at url lacks the
// rabbitmq_delayed_message_exchange plugin. It probes by declaring a throwaway
// x-delayed-message exchange: a plugin-less broker answers with a command-invalid
// channel error. The integration lane provisions the plugin itself — the broker is
// built from Dockerfile.rabbitmq-delayed and started by `make integration-up` — so a
// broker without it is a misconfiguration, not a reason to silently skip. Fail fast
// with the reason, mirroring how a missing AMQP_TEST_URL fails rather than skips.
// Once amqptest/ (T37) lands this migrates to amqptest.RequireDelayedExchange(t).
func requireDelayedExchange(t *testing.T, url string) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	probe := fmt.Sprintf("warren.delayprobe.%d", time.Now().UnixNano())
	derr := ch.ExchangeDeclare(probe, "x-delayed-message", false, true, false, false,
		amqp091.Table{"x-delayed-type": "topic"})
	if derr != nil {
		t.Fatalf("rabbitmq_delayed_message_exchange plugin unavailable at %s (%v); "+
			"the integration lane requires it — start the broker with `make integration-up` "+
			"(built from Dockerfile.rabbitmq-delayed, which bakes the plugin in) or enable "+
			"the plugin on your own broker", url, derr)
	}
	_ = ch.ExchangeDelete(probe, false, false)
}

// TestDelay_DelayedDelivery_integration publishes a 2s-delayed message through an
// ExchangeDelayed exchange (built via DelayedTopic) and asserts the broker holds it
// for the delay: arrival must fall between 2s and 2.5s of publish.
func TestDelay_DelayedDelivery_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	requireDelayedExchange(t, url)

	ctx := context.Background()
	stamp := time.Now().UnixNano()
	exch := fmt.Sprintf("warren.delay.ex.%d", stamp)
	queue := fmt.Sprintf("warren.delay.q.%d", stamp)
	const rk = "delay.test"
	defer deleteQueues(url, queue)
	defer deleteExchanges(url, exch)

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{warren.DelayedTopic(exch)},
		Queues:    []warren.Queue{{Name: queue, Durable: true}},
		Bindings:  []warren.Binding{{Exchange: exch, Queue: queue, RoutingKey: rk}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	pub, err := warren.PublisherFor[delayPayload](conn).Exchange(exch).RoutingKey(rk).Build()
	require.NoError(t, err)
	defer pub.Close(ctx) //nolint:errcheck

	arrived := make(chan time.Time, 1)
	cons, err := warren.ConsumerFor[delayPayload](conn).Queue(queue).Build()
	require.NoError(t, err)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	consumed := make(chan error, 1)
	go func() {
		consumed <- cons.Consume(cctx, func(_ context.Context, _ delayPayload) error {
			arrived <- time.Now()
			return nil
		})
	}()

	publishedAt := time.Now()
	require.NoError(t, pub.Publish(ctx, warren.Message[delayPayload]{
		Body:  &delayPayload{N: 1},
		Delay: 2 * time.Second,
	}))

	select {
	case at := <-arrived:
		elapsed := at.Sub(publishedAt)
		assert.GreaterOrEqual(t, elapsed, 2*time.Second,
			"the message must not be delivered before its 2s delay")
		assert.Less(t, elapsed, 2500*time.Millisecond,
			"the message must be delivered within 2.5s of publish")
	case <-time.After(10 * time.Second):
		t.Fatal("the delayed message never arrived")
	}

	cancel()
	require.NoError(t, <-consumed)
}
