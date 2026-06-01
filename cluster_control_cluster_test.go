//go:build cluster

package warren_test

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
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
// Leader placement is made deterministic by two cooperating facts: rabbitmq.conf
// sets queue_leader_locator=client-local (the leader lands on the declaring
// connection's node) and the WithAddrs list below is ordered rmq1-first, so every
// pooled connection (each managedConn starts its dial cursor at addrs[0]) opens on
// rmq1 and the quorum leader is created there. rmq1 is then KILLED (SIGKILL, a real
// crash); rmq0 and rmq2 keep a majority and re-elect, and the management API —
// served by the surviving rmq0 (WARREN_CLUSTER_MGMT) — surfaces the new leader.
//
// goleak: after the kill, warren's rmq1-pinned connections rotate (T33 cursor) to
// rmq0/rmq2 (both alive, next in the WithAddrs list), so Close drains cleanly. The
// killed node is restarted in t.Cleanup to restore the cluster for later tests.
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

	// rmq1-first address list: every pooled connection dials addrs[0]=rmq1, so the
	// client-local locator places the quorum leader there.
	addrs := []string{nodes[1], nodes[0], nodes[2]}

	ctx := context.Background()

	// Clean slate: a prior run may have left the durable quorum queue behind with a
	// stale leader. Delete it before declaring so the leader is freshly placed on rmq1.
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })
	// Restore the killed node last-in/first-out: this runs before the delete above
	// so the cluster is whole again when the durable queue is removed.
	t.Cleanup(func() { amqptest.StartNode(t, killService) })

	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()
	conn, err := warren.Dial(dialCtx, warren.WithAddrs(addrs))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

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
	assert.Contains(t, []string{"rabbit@rmq0", "rabbit@rmq2"}, after.Leader,
		"the new leader must be one of the surviving nodes")
}

// awaitNewLeaderCluster polls the management API until the quorum queue's leader
// differs from oldLeader (and is non-empty), or the timeout elapses. Re-election
// takes a few seconds, and the API may briefly report a stale or empty leader
// mid-election, so a poll — not a single read — is required.
func awaitNewLeaderCluster(t *testing.T, queue, oldLeader string, timeout time.Duration) amqptest.QuorumQueueState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last amqptest.QuorumQueueState
	for time.Now().Before(deadline) {
		last = amqptest.QuorumLeader(t, queue)
		if last.Leader != "" && last.Leader != oldLeader {
			return last
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("no new leader elected for %q within %s (still %q)", queue, timeout, last.Leader)
	return last // unreachable
}

// deleteQuorumQueueCluster best-effort deletes a durable quorum queue via a raw
// amqp091 connection to a surviving node, so reruns start from a clean slate.
// Failures are ignored — the node may be momentarily unavailable mid-test.
func deleteQuorumQueueCluster(addr, queue string) {
	rawConn, err := amqp091.Dial(addr)
	if err != nil {
		return
	}
	defer rawConn.Close() //nolint:errcheck
	ch, err := rawConn.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck
	_, _ = ch.QueueDelete(queue, false, false, false)
}
