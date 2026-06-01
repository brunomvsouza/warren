//go:build cluster

package warren_test

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

// TestClusterControl_quorumLeaderFailover_cluster is the Phase 9.5 (T166b)
// verification of the cluster control toolkit: it declares a quorum queue whose
// Raft leader is pinned to a known node, kills that node, and asserts the
// management API reports a NEW leader — a behaviour a single-node broker cannot
// produce, which is the whole point of the cluster lane.
//
// Leader placement is made deterministic by rabbitmq.conf setting
// queue_leader_locator=client-local (the leader lands on the declaring
// connection's node) plus a SINGLE-ADDRESS declare connection pinned to rmq1
// (declareQuorumLeaderOnNode) — immune to the per-socket WithAddrs shuffle (T66),
// which would otherwise scatter the declaring socket across nodes and could place
// the leader on the management node rmq0. rmq1 is then KILLED (SIGKILL, a real
// crash); rmq0 and rmq2 keep a majority and re-elect, and the management API —
// served by the surviving rmq0 (WARREN_CLUSTER_MGMT) — surfaces the new leader.
//
// goleak: the live conn below dials the full WithAddrs list; after the kill any of
// its sockets that had landed on rmq1 rotate (shuffled cursor) to a surviving node,
// so Close drains cleanly. The killed node is restarted in t.Cleanup to restore the
// cluster for later tests.
func TestClusterControl_quorumLeaderFailover_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")

	const (
		queue          = "test.cluster.control.leaderfailover"
		killService    = "rmq1"
		originalLeader = "rabbit@rmq1"
	)

	// Full WithAddrs list for the live connection; leader placement no longer
	// depends on its dial order (the single-address declare below pins the leader).
	addrs := []string{nodes[1], nodes[0], nodes[2]}

	ctx := context.Background()

	// All three members must be running before we pin the leader on rmq1: an earlier
	// test may have just restarted it.
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	// Clean slate: a prior run may have left the durable quorum queue behind with a
	// stale leader. Delete it before declaring so the leader is freshly placed on rmq1.
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })
	// Restore the killed node last-in/first-out: this runs before the delete above
	// so the cluster is whole again when the durable queue is removed.
	t.Cleanup(func() { amqptest.StartNode(t, killService) })

	// Pin the quorum leader to rmq1 via a single-address declare connection —
	// shuffle-immune (T66), so the leader is deterministically on the node we kill.
	declareQuorumLeaderOnNode(ctx, t, nodes[1], queue)

	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()
	conn, err := warren.Dial(dialCtx, warren.WithAddrs(addrs))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Idempotent redeclare on the live multi-addr connection (the queue already
	// exists with its leader on rmq1; redeclaring with identical args does not move
	// it) so AttachTo-style reconnect redeclare has a registered topology to use.
	declCtx, declCancel := context.WithTimeout(ctx, 15*time.Second)
	defer declCancel()
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(declCtx, conn))

	// Precondition: the leader is on rmq1 and the queue spans all three members.
	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Equal(t, originalLeader, before.Leader,
		"leader must be pinned to the node we are about to kill (client-local locator + rmq1-first WithAddrs)")
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes")

	// Kill the leader's node — a real crash, not a graceful stop.
	amqptest.KillNode(t, killService)

	// The surviving majority (rmq0+rmq2) must re-elect; the management API on rmq0
	// reports the new leader. Poll until it changes (re-election is not instant).
	after := awaitNewLeaderCluster(t, queue, originalLeader, 60*time.Second)
	assert.NotEqual(t, originalLeader, after.Leader, "a new leader must be elected after the old one is killed")
	assert.Contains(t, survivingClusterLeaders(originalLeader), after.Leader,
		"the new leader must be one of the surviving nodes")
}
