//go:build integration

package warren_test

// T68 (R10-13 / EDA-01) real-broker assertion: a NON-mandatory publish to an
// exchange with no matching binding lands on the alternate exchange's bound
// queue — the server-side unroutable catch-all, not a silent drop.

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

func TestAlternateExchange_catchesUnroutable_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const (
		ingest = "warren-t68-ingest"
		aeEx   = "warren-t68-unrouted"
		catchQ = "warren-t68-catch"
	)
	deleteQueues(url, catchQ)
	deleteExchanges(url, ingest, aeEx)
	t.Cleanup(func() {
		deleteQueues(url, catchQ)
		deleteExchanges(url, ingest, aeEx)
	})

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: ingest, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: false, AlternateExchange: aeEx},
			{Name: aeEx, Kind: warren.ExchangeFanout, Durable: false, AutoDelete: false},
		},
		Queues:   []warren.Queue{{Name: catchQ, Durable: false, AutoDelete: false}},
		Bindings: []warren.Binding{{Exchange: aeEx, Queue: catchQ, RoutingKey: ""}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	// Publish NON-mandatory to a routing key with no binding on `ingest`.
	pub, err := warren.PublisherFor[string](conn).Exchange(ingest).RoutingKey("no.such.binding").Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	body := "salvaged"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &body}))

	// The message must surface on the alternate-exchange catch queue.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close() //nolint:errcheck

	var got string
	require.Eventually(t, func() bool {
		msg, ok, gerr := rawCh.Get(catchQ, true)
		if gerr != nil || !ok {
			return false
		}
		got = string(msg.Body)
		return true
	}, 3*time.Second, 100*time.Millisecond,
		"unroutable message must land on the alternate-exchange catch queue")
	assert.Equal(t, `"salvaged"`, got)
}
