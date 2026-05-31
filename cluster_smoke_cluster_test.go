//go:build cluster

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
)

// TestClusterDial_health_close_cluster is the Phase 9.5 (T166a) end-to-end smoke
// test: it proves the compose cluster → env-discovery → Dial-over-3-nodes → green
// path before any failover logic lands. It dials the three cluster nodes via
// WithAddrs (the first reachable node wins and sticks), asserts Health is nil, and
// closes cleanly with no leaked goroutines. The single-node integration lane
// cannot exercise a multi-node WithAddrs list, which is why this belongs to the
// dedicated cluster lane.
func TestClusterDial_health_close_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	ctx := context.Background()

	// Bound the dial so a misconfigured or unreachable cluster fails fast with a
	// clear deadline error instead of hanging until the test binary's global
	// timeout. Close below is likewise bounded.
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	conn, err := warren.Dial(dialCtx, warren.WithAddrs(nodes))
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Health must succeed against whichever node the failover list dialled.
	// Bounded for the same fail-fast reason as the dial and close below: a wedged
	// broker should surface a deadline error, not hang to the global test timeout.
	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	defer healthCancel()
	require.NoError(t, conn.Health(healthCtx))

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(closeCtx))
}
