//go:build cluster

package amqptest

import (
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Cluster lane environment variables. The cluster lane talks to an externally
// provisioned 3-node RabbitMQ cluster (stood up by docker-compose.cluster.yml via
// `make cluster-up`) rather than an in-process testcontainer, because the
// behaviours it proves — quorum leader failover, partition handling, multi-node
// WithAddrs rotation — are properties of a real multi-node cluster.
//
// Every helper below FAILS (t.Fatal) rather than t.Skip when its variable is
// unset, mirroring the AMQP_TEST_URL rule for the integration lane: the `cluster`
// build tag is the opt-in, so a missing variable is a misconfiguration, not a
// reason to silently pass green.
//
// ClusterNodes is consumed by the T166a dial smoke test; ClusterMgmt (queue
// leader/member queries) and ToxiproxyURL (cut/heal campaigns) are scaffolding
// for the T166b control toolkit and its successors, hence defined here but not
// yet referenced by a test.
const (
	// EnvClusterNodes is a comma-separated list of amqp:// URIs, one per cluster
	// node, each pointing at that node's Toxiproxy-fronted AMQP port so a test can
	// cut/heal a node without the client noticing the indirection.
	EnvClusterNodes = "WARREN_CLUSTER_NODES"
	// EnvClusterMgmt is the base URL of any node's management HTTP API (with
	// credentials in the userinfo), e.g. http://guest:guest@localhost:15672. A
	// cluster-wide query (queue leader/members) can be issued against any node.
	EnvClusterMgmt = "WARREN_CLUSTER_MGMT"
	// EnvToxiproxyURL is the base URL of the Toxiproxy control API (e.g.
	// http://localhost:8474) that fronts each node's AMQP port; the control toolkit
	// (T166b) uses it to cut/heal a node's connectivity.
	EnvToxiproxyURL = "WARREN_TOXIPROXY_URL"
)

// ClusterNodes returns the per-node amqp:// URIs from WARREN_CLUSTER_NODES,
// splitting on commas and trimming surrounding whitespace. It fails the test (it
// does not skip) when the variable is unset or yields no non-empty entries — the
// `cluster` build tag is the opt-in, so a missing cluster is a misconfiguration.
func ClusterNodes(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv(EnvClusterNodes)
	nodes := parseClusterNodes(raw)
	if len(nodes) == 0 {
		if raw == "" {
			t.Fatalf("%s must be set to run cluster tests "+
				"(comma-separated amqp:// URIs, one per node; e.g. `make cluster-up`)", EnvClusterNodes)
		}
		t.Fatalf("%s is set but contains no non-empty amqp:// URIs", EnvClusterNodes)
	}
	return nodes
}

// ClusterMgmt returns the management API base URL from WARREN_CLUSTER_MGMT. It
// fails the test (it does not skip) when the variable is unset — broker-side
// assertions are configured explicitly, not silently dropped.
func ClusterMgmt(t *testing.T) string {
	t.Helper()
	mgmt := os.Getenv(EnvClusterMgmt)
	if mgmt == "" {
		t.Fatalf("%s must be set to read cluster state via the management API "+
			"(e.g. http://guest:guest@localhost:15672)", EnvClusterMgmt)
	}
	return mgmt
}

// ToxiproxyURL returns the Toxiproxy control API base URL from
// WARREN_TOXIPROXY_URL. It fails the test (it does not skip) when the variable is
// unset — partition/cut campaigns require the proxy control plane to exist.
func ToxiproxyURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv(EnvToxiproxyURL)
	if u == "" {
		t.Fatalf("%s must be set to cut/heal node connectivity via Toxiproxy "+
			"(e.g. http://localhost:8474)", EnvToxiproxyURL)
	}
	return u
}

// ---------------------------------------------------------------------------
// Cluster control toolkit (T166b)
//
// The thin *testing.T wrappers below drive the pure / transport helpers in
// cluster_control.go against the live compose cluster. Each fails the test
// (t.Fatal) on any error, mirroring the fail-not-skip discipline of the env
// helpers above. They are the primitives the failover campaigns (T166c–g)
// compose: discover the quorum leader, kill/stop/start a node, and cut/heal a
// node's AMQP port via Toxiproxy.
// ---------------------------------------------------------------------------

// QuorumLeader queries the live management API (WARREN_CLUSTER_MGMT) for the
// quorum queue's current Raft leadership (leader/members/online) on the default
// vhost and fails the test on any error. A short-timeout, keep-alive-disabled
// HTTP client is used so no pooled connection goroutine survives into a goleak
// check. Single-node brokers cannot produce the leader migration this observes,
// which is why it lives in the cluster lane.
func QuorumLeader(t *testing.T, queue string) QuorumQueueState {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	state, err := fetchQuorumQueueState(client, mgmt, "/", queue)
	if err != nil {
		t.Fatalf("read quorum queue %q state via management API: %v", queue, err)
	}
	return state
}

// StopNode gracefully stops a cluster node's container (docker stop → SIGTERM)
// by its short compose service name (rmq0/rmq1/rmq2).
func StopNode(t *testing.T, service string) { dockerNode(t, "stop", service) }

// StartNode (re)starts a previously stopped or killed node's container. It is a
// no-op (exit 0) against an already-running container, so it is safe to call from
// t.Cleanup to restore the cluster after a kill.
func StartNode(t *testing.T, service string) { dockerNode(t, "start", service) }

// KillNode forcibly kills a node's container (docker kill → SIGKILL) — the
// simulated crash the real-node-death campaigns need, distinct from StopNode's
// graceful shutdown.
func KillNode(t *testing.T, service string) { dockerNode(t, "kill", service) }

// dockerNode runs `docker <action> warren-<service>` and fails the test with the
// combined output on a non-zero exit. exec.Command(...).CombinedOutput() is
// synchronous, so it leaves no goroutine behind for goleak.
func dockerNode(t *testing.T, action, service string) {
	t.Helper()
	container := nodeContainer(service)
	out, err := exec.Command("docker", dockerNodeArgs(action, container)...).CombinedOutput() //nolint:gosec // fixed argv, test-only
	if err != nil {
		t.Fatalf("docker %s %s: %v: %s", action, container, err, strings.TrimSpace(string(out)))
	}
}

// NewToxiproxy returns a Toxiproxy control client bound to WARREN_TOXIPROXY_URL,
// for cutting/healing a node's AMQP port in the partition campaigns.
func NewToxiproxy(t *testing.T) *ToxiproxyClient {
	t.Helper()
	return NewToxiproxyClient(ToxiproxyURL(t))
}
