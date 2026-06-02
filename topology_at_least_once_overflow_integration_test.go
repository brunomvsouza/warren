//go:build integration

package warren_test

// T76 (RMQ-05 / decision 52) real-broker assertion: a quorum queue with a DLX
// declares the at-least-once dead-letter strategy AND the required
// x-overflow=reject-publish (auto-coupled by warren), the broker accepts the
// declare, and a nacked message actually dead-letters into the DLQ.
//
// Gate G4 + the T76 probe established that the broker silently accepts ANY
// overflow with at-least-once (including the invalid drop-head and
// reject-publish-dlx) on both 3.13 and 4.x, so the coupling is enforced
// client-side; this test pins the broker-side result of that coupling.

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

func TestTopologyQuorumAtLeastOnce_overflowCoupled_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const srcQ = "warren-t76-alo.src"
	const dlqQ = "warren-t76-alo.src.dlq"
	const dlxEx = "warren-t76-alo.src.dlx"
	deleteQueues(url, srcQ, dlqQ)
	deleteExchanges(url, dlxEx)
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, dlxEx)
	})

	// Quorum source queue with a DLX and NO overflow set — warren must
	// auto-couple x-overflow=reject-publish with the at-least-once strategy.
	topo := &warren.Topology{
		Queues:      []warren.Queue{{Name: srcQ, Durable: true, Type: warren.QueueTypeQuorum}},
		DeadLetters: []warren.DeadLetter{{Source: srcQ}},
	}
	require.NoError(t, topo.Declare(ctx, conn), "quorum + DLX must declare successfully on the broker")

	// Broker-side: the source queue carries the coupled args.
	args := queueArgsViaManagement(t, url, srcQ)
	assert.Equal(t, "quorum", args["x-queue-type"])
	assert.Equal(t, "at-least-once", args["x-dead-letter-strategy"],
		"quorum + DLX must declare at-least-once")
	assert.Equal(t, "reject-publish", args["x-overflow"],
		"at-least-once must be coupled with reject-publish overflow")

	// Functional: a nacked message dead-letters into the DLQ. Drive it over raw
	// amqp091 to keep the assertion minimal and deterministic.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close() //nolint:errcheck
	ch, err := rawConn.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	pubCtx, pubCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pubCancel()
	require.NoError(t, ch.PublishWithContext(pubCtx, "", srcQ, false, false, amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent,
		Body:         []byte("poison"),
	}))

	// Consume one delivery from the source and nack it without requeue → DLX.
	deliveries, err := ch.Consume(srcQ, "", false, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-deliveries:
		require.NoError(t, d.Nack(false, false))
	case <-time.After(5 * time.Second):
		t.Fatal("source queue never delivered the published message")
	}

	// The dead-lettered message must arrive in the DLQ (at-least-once preserves it).
	require.Eventually(t, func() bool {
		msg, ok, gErr := ch.Get(dlqQ, true)
		if gErr != nil || !ok {
			return false
		}
		return string(msg.Body) == "poison"
	}, 10*time.Second, 100*time.Millisecond, "nacked message must dead-letter into the DLQ")
}
