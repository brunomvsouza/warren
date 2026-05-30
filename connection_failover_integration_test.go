//go:build integration

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// deadAddr is an AMQP URI that always refuses the connection (port 1 is
// reserved and nothing listens there), so the dial fails fast without a
// network timeout.
const deadAddr = "amqp://guest:guest@127.0.0.1:1/"

// fastFailoverBackoff keeps the dead-address retry near-instant so the test
// does not pay the 1 s default backoff while the round-robin cursor advances
// to the reachable node.
var fastFailoverBackoff = warren.RetryPolicy{
	Min:           5 * time.Millisecond,
	Max:           20 * time.Millisecond,
	WithoutJitter: true,
}

// TestDial_failover_initialConnect_skipsDeadAddr_integration proves the T33
// acceptance "tries addresses in order on initial connect": with a dead node
// first and a healthy node second, Dial walks the list, fails on the dead URI,
// rotates to the healthy URI, and connects. A live Health check confirms the
// socket completed a real AMQP handshake against the second address.
func TestDial_failover_initialConnect_skipsDeadAddr_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	real := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{deadAddr, real}),
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithReconnectBackoff(fastFailoverBackoff),
	)
	require.NoError(t, err, "Dial must fail over from the dead node to the healthy node")
	require.NotNil(t, conn)

	require.NoError(t, conn.Health(ctx), "Health must pass on the failed-over socket")

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(closeCtx))
}

// TestDial_failover_reconnectRotates_recoversOnHealthyNode_integration proves
// the T33 acceptance "on reconnect, rotates to the next address (round-robin)"
// and "first successful address sticks until the next disconnect": with the
// healthy node first and a dead node second, the initial connect sticks to the
// healthy node; ForceReconnect rotates the cursor onto the dead node, which is
// skipped, and the socket recovers on the healthy node again.
func TestDial_failover_reconnectRotates_recoversOnHealthyNode_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	real := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{real, deadAddr}),
		warren.WithPublisherConnections(1),
		warren.WithConsumerConnections(1),
		warren.WithReconnectBackoff(fastFailoverBackoff),
	)
	require.NoError(t, err)
	require.NoError(t, conn.Health(ctx), "Health must pass on the initial healthy node")

	// ForceReconnect drops every socket; the cursor now points at the dead node,
	// which the round-robin path must skip before wrapping back to the healthy one.
	require.NoError(t, conn.ForceReconnect())

	// Poll Health until the barrier clears on the healthy node.
	require.Eventually(t, func() bool {
		return conn.Health(ctx) == nil
	}, 5*time.Second, 25*time.Millisecond,
		"socket must recover on the healthy node after rotating past the dead one")

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(closeCtx))
	assert.NoError(t, ctx.Err())
}
