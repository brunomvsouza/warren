//go:build integration

package warren_test

// T69 (R10-14 / EDA-03) real-broker assertion: an exchange→exchange binding
// forwards a message published to the source exchange through to a queue bound
// on the destination exchange (layered ingest→per-domain fan-out).

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

func TestExchangeBinding_layeredFanout_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		ingest = "warren-t69-ingest"
		orders = "warren-t69-orders"
		q      = "warren-t69-orders-q"
	)
	deleteQueues(url, q)
	deleteExchanges(url, ingest, orders)
	t.Cleanup(func() {
		deleteQueues(url, q)
		deleteExchanges(url, ingest, orders)
	})

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: ingest, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: false},
			{Name: orders, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: false},
		},
		Queues:   []warren.Queue{{Name: q, Durable: false, AutoDelete: false}},
		Bindings: []warren.Binding{{Exchange: orders, Queue: q, RoutingKey: "order.#"}},
		ExchangeBindings: []warren.ExchangeBinding{
			{Source: ingest, Destination: orders, RoutingKey: "order.#"},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish to the SOURCE exchange; the e2e binding forwards to `orders`, whose
	// queue binding routes it to q.
	pub, err := warren.PublisherFor[string](conn).Exchange(ingest).RoutingKey("order.created").Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	body := "via-e2e"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck

	var got string
	require.Eventually(t, func() bool {
		msg, ok, gerr := rawCh.Get(q, true)
		if gerr != nil || !ok {
			return false
		}
		got = string(msg.Body)
		return true
	}, 3*time.Second, 100*time.Millisecond,
		"message published to the source exchange must reach the destination exchange's queue via the e2e binding")
	assert.Equal(t, `"via-e2e"`, got)
}
