//go:build cluster

package warren_test

import (
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/amqptest"
)

// Shared helpers and node-name constants for the cluster failover campaigns (the
// T166b control test, the T166c quorum-failover campaign, and their successors).
// Hoisted out of the individual campaign files so the literals have one source of
// truth and no campaign depends on a helper defined inside another campaign's file.

// clusterNodeNames is the canonical broker node-name set for the 3-node compose
// cluster (docker-compose.cluster.yml pins hostnames rmq0/rmq1/rmq2). The
// management API reports leaders and members as rabbit@<hostname>.
var clusterNodeNames = []string{"rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"}

// clusterKillableLeaders are the nodes a leader-failover campaign may place the
// quorum leader on and then kill: every node EXCEPT rmq0, which alone exposes the
// management API (WARREN_CLUSTER_MGMT) and is dialed last so the leader never
// lands on it. Keeping rmq0 alive lets a campaign OBSERVE the re-election it
// triggers by killing whichever of rmq1/rmq2 holds the leader.
var clusterKillableLeaders = []string{"rabbit@rmq1", "rabbit@rmq2"}

// survivingClusterLeaders returns the cluster node names that are NOT the killed
// node — the candidates a re-elected quorum leader must come from.
func survivingClusterLeaders(killed string) []string {
	out := make([]string, 0, len(clusterNodeNames)-1)
	for _, n := range clusterNodeNames {
		if n != killed {
			out = append(out, n)
		}
	}
	return out
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
