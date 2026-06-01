//go:build integration

package warren_test

// T65 (R10-10 / SRE-03 / ST-08 / DP-03) real-broker assertion: the auto-declared
// <source>.dlq is durable AND bounded (x-max-length, x-overflow, x-message-ttl)
// so a poison flood cannot fill disk and trip a broker-wide connection.blocked.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

func TestTopologyAutoDLQ_durableBounded_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const srcQ = "warren-t65-dlq-bounds.src"
	const dlqQ = "warren-t65-dlq-bounds.src.dlq"
	const dlxEx = "warren-t65-dlq-bounds.src.dlx"
	deleteQueues(url, srcQ, dlqQ)
	deleteExchanges(url, dlxEx)
	t.Cleanup(func() {
		deleteQueues(url, srcQ, dlqQ)
		deleteExchanges(url, dlxEx)
	})

	topo := &warren.Topology{
		Queues:      []warren.Queue{{Name: srcQ, Durable: true}},
		DeadLetters: []warren.DeadLetter{{Source: srcQ}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	args := queueArgsViaManagement(t, url, dlqQ)
	// x-max-length and x-message-ttl come back as JSON numbers (float64).
	assert.EqualValues(t, 100000, args["x-max-length"], "auto DLQ must carry the default x-max-length")
	assert.Equal(t, "drop-head", args["x-overflow"], "auto DLQ overflow must default to drop-head")
	require.Contains(t, args, "x-message-ttl", "auto DLQ must carry the default retention TTL")
	assert.EqualValues(t, (7 * 24 * time.Hour).Milliseconds(), args["x-message-ttl"])
}
