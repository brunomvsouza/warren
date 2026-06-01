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
// does not pay the 1 s default backoff while the cursor advances through the
// socket's shuffled order to the reachable node. Jitter is left at the default
// (full); the tight [Min, Max] keeps every attempt near-instant while still
// exercising the production spreading path.
var fastFailoverBackoff = warren.RetryPolicy{
	Min: 5 * time.Millisecond,
	Max: 20 * time.Millisecond,
}

// TestDial_failover_initialConnect_skipsDeadAddr_integration proves the failover
// contract on initial connect: with a dead node and a healthy node in the list,
// Dial walks the socket's shuffled order, fails on the dead URI if it comes
// first, rotates to the healthy URI, and connects. A live Health check confirms
// the socket completed a real AMQP handshake against the reachable address. The
// per-socket shuffle (T66) means the dead node may be tried first or second; the
// guarantee under test is that Dial reaches the healthy node either way.
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
// the reconnect contract: "on reconnect, rotates to the next address (round-robin
// over the socket's shuffled order)" and "the connected address sticks until the
// next disconnect". With a healthy and a dead node in the list, the initial
// connect sticks to the healthy node; ForceReconnect drops the socket and the
// cursor advances — rotating past the dead node when it comes up — and the socket
// recovers on the healthy node again.
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
