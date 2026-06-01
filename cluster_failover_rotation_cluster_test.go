//go:build cluster

package warren_test

// Per-connection WithAddrs rotation on a real multi-node cluster — the T66
// no-addr[0]-stampede claim (Phase 9.5 / T166e; SPEC §1 multi-conn pool, DS-10 /
// SRE-04). The deterministic distribution is already proven in the default lane by
// the unit test TestPerConnSeeding_distributesInitialDialNoAddr0Stampede; these
// campaigns prove it end to end against three real nodes — that warren's pooled
// sockets actually land on different brokers, and re-spread (rather than all piling
// onto addrs[0]) when they reconnect — observed via the management API's
// /api/connections node field.
//
// Why this needs a cluster: with one node every socket connects to the same place,
// so "the pool spreads across addresses" is unobservable below three nodes —
// exactly the DS-10/SRE-04 gap the single-node integration lane cannot close.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
)

// rotationShuffleSeed pins the per-connection address shuffle (T66) so the initial
// distribution is DETERMINISTIC — the same spread on every run, no probabilistic
// "≥2 of 3 nodes" flake. Any non-zero seed whose six per-socket permutations do not
// all start on the same node works; this one is verified to spread the pool.
const rotationShuffleSeed int64 = 0x5A6B7C8D

// TestClusterFailoverRotation_SpreadAndRestart_cluster dials a pool of six sockets
// across the full 3-node WithAddrs list and asserts:
//   - the INITIAL connect spreads them across ≥2 nodes, not all onto addrs[0];
//   - a RECONNECT (ForceReconnect, every node up) re-spreads them across ≥2 nodes —
//     the no-addr[0]-stampede claim on reconnect;
//   - a rolling full-cluster RESTART leaves the whole pool reconnected and healthy.
//
// A shared dial cursor (the pre-T66 behaviour) would march every socket through the
// same order, parking them all on addrs[0] on the initial connect and again on every
// reconnect; the per-socket shuffle breaks that lockstep. Both spread assertions are
// made deterministic by pinning the shuffle seed.
//
// The reconnect-spread is proven with ForceReconnect while ALL nodes are up, not via
// the restart: a rolling restart legitimately funnels sockets toward whichever node
// stays up across the roll (addrs[0] is bounced first, then stable), so its end
// distribution is order/timing-dependent, not a clean spread. The restart therefore
// asserts only RECOVERY. It is rolling (one node at a time) rather than
// all-down-then-up because with every node stopped at once the 3.13 cluster does not
// reliably reform (a node can exit non-zero waiting for peers) — a harness fragility,
// not the property under test; a rolling bounce keeps a majority up so each node
// rejoins cleanly while still forcing every socket to drop and reconnect.
func TestClusterFailoverRotation_SpreadAndRestart_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	const (
		prefix    = "rotcamp-spread"
		wantConns = 6 // 3 publisher + 3 consumer sockets
	)
	ctx := context.Background()

	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()
	conn, err := warren.Dial(dialCtx,
		warren.WithAddrs([]string{nodes[0], nodes[1], nodes[2]}),
		warren.WithConnectionName(prefix),
		warren.WithPublisherConnections(3),
		warren.WithConsumerConnections(3),
		warren.WithReconnectBackoff(clusterFastBackoff),
		warren.WithDialer(boundedClusterDialer),
		warren.WithAddrShuffleSeedForTest(rotationShuffleSeed),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Initial spread (deterministic via the pinned seed): the six sockets must land
	// across ≥2 nodes, not all on addrs[0].
	round0 := awaitCleanPool(t, prefix, wantConns, 30*time.Second)
	assert.GreaterOrEqual(t, distinctNodes(round0), 2,
		"initial pool must spread across ≥2 nodes, not stampede addrs[0] (got %v)", nodesByName(round0))
	t.Logf("initial pool spread: %v", nodesByName(round0))

	// Reconnect spread: ForceReconnect drops every socket while all nodes stay up, so
	// each re-dials the NEXT entry in its own shuffled order. We wait until every old
	// TCP socket (tracked by its unique broker connection name) has been REPLACED —
	// ForceReconnect is async, so reading too early would just re-observe round0 — then
	// assert the genuinely-new pool still spans ≥2 nodes. Clean because no node is
	// preferentially up to funnel the reconnects.
	require.NoError(t, conn.ForceReconnect())
	round1 := awaitReplacedPool(t, prefix, brokerNameSet(round0), wantConns, 60*time.Second)
	assert.GreaterOrEqual(t, distinctNodes(round1), 2,
		"reconnections must re-spread across ≥2 nodes, not stampede addrs[0] (got %v)", nodesByName(round1))
	t.Logf("post-reconnect pool spread: %v", nodesByName(round1))

	// Rolling full-cluster restart: bounce each node and wait for the cluster to be
	// whole again before bouncing the next, so a majority stays up and each node
	// rejoins cleanly. Every socket on the bounced node drops and reconnects to a
	// survivor. The bounded dialer makes a reconnect attempt to the down node fail
	// fast and rotate. Bouncing rmq0 takes the management API down briefly —
	// WaitClusterReady tolerates the connection-refused window and polls until rmq0
	// answers again. We assert RECOVERY (every socket reconnected, Health green); the
	// end distribution is order-dependent (see the doc comment) so it is not asserted.
	for _, svc := range []string{"rmq0", "rmq1", "rmq2"} {
		amqptest.StopNode(t, svc)
		amqptest.StartNode(t, svc)
		amqptest.WaitClusterReady(t, len(nodes), 120*time.Second)
	}

	recovered := awaitCleanPool(t, prefix, wantConns, 180*time.Second)
	require.Len(t, recovered, wantConns,
		"every socket must reconnect after a full cluster restart (no socket stranded)")
	require.NoError(t, conn.Health(ctx), "Health must pass once the cluster is whole again")
	t.Logf("post-restart pool: %v", nodesByName(recovered))
}

// TestClusterFailoverRotation_RealNodeDeadFailover_cluster is the real-node analog
// of the integration lane's dead-port failover trick (connection_failover_integration_test.go,
// which dials a reserved port that refuses TCP): instead it STOPS a real cluster
// node and proves Dial fails over from the stopped address to a healthy one,
// landing every socket on the survivor. The per-socket shuffle never strands a
// connection on the dead address — whichever order a socket shuffles [dead, healthy]
// into, the cursor rotates past the dead entry to the reachable node.
func TestClusterFailoverRotation_RealNodeDeadFailover_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	const (
		prefix      = "rotcamp-dead"
		deadService = "rmq2"
	)

	// Stop a real node; restart it (and wait for rejoin) on cleanup so later tests see
	// a whole cluster. Cleanup runs after the deferred Close + goleak check, by which
	// point no socket is on the stopped node, so the drain stays clean.
	amqptest.StopNode(t, deadService)
	t.Cleanup(func() {
		amqptest.StartNode(t, deadService)
		amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)
	})

	ctx := context.Background()
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// WithAddrs = [stopped rmq2, healthy rmq0]. Dial must reach rmq0 regardless of the
	// per-socket shuffle order.
	conn, err := warren.Dial(dialCtx,
		warren.WithAddrs([]string{nodes[2], nodes[0]}),
		warren.WithConnectionName(prefix),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(2),
		warren.WithReconnectBackoff(clusterFastBackoff),
		warren.WithDialer(boundedClusterDialer),
	)
	require.NoError(t, err, "Dial must fail over from the stopped node to the healthy node")
	defer sacCloseConn(conn)

	require.NoError(t, conn.Health(ctx), "Health must pass on the failed-over connection")

	// Every socket must have landed on the healthy node (rmq0), never stranded on the
	// stopped rmq2.
	landed := awaitCleanPool(t, prefix, 4, 30*time.Second) // 2 publisher + 2 consumer
	for _, e := range landed {
		assert.Equal(t, "rabbit@rmq0", e.Node,
			"%s must connect to the healthy node, not the stopped %s", e.Name, deadService)
	}
}

// awaitCleanPool polls the management API until EXACTLY `want` warren connections
// matching prefix are present with distinct client names — no lingering, still-closing
// connection from a reconnect — and returns them. Gating on a clean count (not just
// "want names present") is what makes the read reliable: a reconnect briefly leaves
// the old and new socket both registered under the reused connection_name.
func awaitCleanPool(t *testing.T, prefix string, want int, within time.Duration) []amqptest.ConnNode {
	t.Helper()
	deadline := time.Now().Add(within)
	var lastSeen int
	for time.Now().Before(deadline) {
		entries := amqptest.ConnectionNodes(t, prefix)
		lastSeen = len(entries)
		if isCleanPool(entries, want) {
			return entries
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("expected exactly %d distinct connections with prefix %q within %s; last saw %d entries",
		want, prefix, within, lastSeen)
	return nil // unreachable
}

// awaitReplacedPool polls until the pool is clean AND every connection is a NEW TCP
// socket — its broker connection name is absent from prevBrokers. That is how a
// caller waits out an async ForceReconnect: only once every old socket has been
// replaced is the observed distribution the genuine post-reconnect one (reading
// sooner would re-report the still-live pre-reconnect sockets).
func awaitReplacedPool(t *testing.T, prefix string, prevBrokers map[string]struct{}, want int, within time.Duration) []amqptest.ConnNode {
	t.Helper()
	deadline := time.Now().Add(within)
	var lastSeen int
	for time.Now().Before(deadline) {
		entries := amqptest.ConnectionNodes(t, prefix)
		lastSeen = len(entries)
		if isCleanPool(entries, want) && allReplaced(entries, prevBrokers) {
			return entries
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("the pool's %d sockets were not all replaced by a reconnect within %s (last saw %d entries)",
		want, within, lastSeen)
	return nil // unreachable
}

// isCleanPool reports whether entries holds exactly `want` connections with distinct
// client names. A duplicate name (or the wrong count) means a connection is
// mid-reconnect — the old socket has not finished closing — so the caller keeps
// polling rather than read a transient view.
func isCleanPool(entries []amqptest.ConnNode, want int) bool {
	if len(entries) != want {
		return false
	}
	names := make(map[string]struct{}, want)
	for _, e := range entries {
		names[e.Name] = struct{}{}
	}
	return len(names) == want
}

// allReplaced reports whether none of entries' broker connection names appear in
// prevBrokers — i.e. every socket is a fresh TCP connection since prevBrokers was
// captured.
func allReplaced(entries []amqptest.ConnNode, prevBrokers map[string]struct{}) bool {
	for _, e := range entries {
		if _, was := prevBrokers[e.BrokerName]; was {
			return false
		}
	}
	return true
}

// brokerNameSet collects the broker connection names (unique per TCP socket) of a
// pool, so a later read can tell the sockets were all replaced by a reconnect.
func brokerNameSet(entries []amqptest.ConnNode) map[string]struct{} {
	set := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		set[e.BrokerName] = struct{}{}
	}
	return set
}

// distinctNodes counts how many different broker nodes a pool spans.
func distinctNodes(entries []amqptest.ConnNode) int {
	seen := map[string]struct{}{}
	for _, e := range entries {
		seen[e.Node] = struct{}{}
	}
	return len(seen)
}

// nodesByName renders a pool as connection_name → node for diagnostic logging and
// assertion messages.
func nodesByName(entries []amqptest.ConnNode) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Name] = e.Node
	}
	return m
}
