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

// ConnectionNodes queries the cluster-wide management API (WARREN_CLUSTER_MGMT)
// for every live AMQP connection whose client-set connection_name starts with
// namePrefix, returning one ConnNode per connection (duplicates included, so a
// caller can detect a mid-reconnect transient). The WithAddrs rotation campaign
// (T166e) uses it to SEE how warren's pooled sockets spread across the cluster —
// the no-addr[0]-stampede assertion a single-node broker cannot make. A
// short-timeout, keep-alive-disabled HTTP client is used so no pooled connection
// goroutine survives into a goleak check.
func ConnectionNodes(t *testing.T, namePrefix string) []ConnNode {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	nodes, err := fetchConnectionNodes(client, mgmt, namePrefix)
	if err != nil {
		t.Fatalf("read connections via management API (prefix %q): %v", namePrefix, err)
	}
	return nodes
}

// WaitClusterReady polls the management API (WARREN_CLUSTER_MGMT) until at least
// `want` nodes report running, failing the test if the timeout elapses. It is the
// readiness gate a campaign uses after an EARLIER test killed and restarted a node:
// `docker start` returns before RabbitMQ has rebooted and rejoined, so without this
// gate a following test can declare a quorum queue against a momentarily-absent
// member and observe a non-deterministic leader placement (the failure mode that
// makes the cluster lane order-dependent). A short-timeout, keep-alive-disabled
// HTTP client is used so no pooled connection goroutine survives into a goleak
// check.
func WaitClusterReady(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(timeout)
	var lastRunning, lastTotal int
	var lastErr error
	for time.Now().Before(deadline) {
		running, total, err := fetchRunningNodes(client, mgmt)
		if err == nil && running >= want {
			return
		}
		lastRunning, lastTotal, lastErr = running, total, err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster not ready: wanted >= %d running nodes within %s (last running=%d total=%d err=%v)",
		want, timeout, lastRunning, lastTotal, lastErr)
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

// RestartNode restarts a node's container in place (docker restart → graceful
// stop + start), the rolling-upgrade campaign's primitive: the node leaves the
// cluster cleanly and rejoins, while the surviving majority keeps the quorum queue
// available. Use WaitClusterReady afterwards to gate on the node having rejoined.
func RestartNode(t *testing.T, service string) { dockerNode(t, "restart", service) }

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

// PartitionNode injects a REAL broker-to-broker network partition by disconnecting a
// node's container from the cluster's Docker network (docker network disconnect).
// Unlike a Toxiproxy AMQP cut — which severs only CLIENT connectivity on port 5672 —
// this also severs inter-node Erlang distribution, so the isolated node becomes a
// minority of one and `pause_minority` pauses it while the surviving majority keeps
// quorum and stays available. Heal with HealPartition.
func PartitionNode(t *testing.T, service string) { dockerNetwork(t, "disconnect", service) }

// HealPartition reverses PartitionNode, reconnecting the node's container to the
// cluster network WITH its compose service-name alias so peers and Toxiproxy can
// resolve it by hostname again (the paused minority node then auto-resumes once it
// can reach the majority). Safe to call from t.Cleanup: a benign "already connected"
// /"not connected" error (the node was never partitioned, or was healed already) is
// tolerated rather than failing the test.
func HealPartition(t *testing.T, service string) { dockerNetworkTolerant(t, "connect", service) }

// dockerNetwork runs `docker network <action> warren-cluster_default warren-<service>`
// and fails the test on a non-zero exit. Synchronous, so it leaves no goroutine for
// goleak.
func dockerNetwork(t *testing.T, action, service string) {
	t.Helper()
	out, err := runDockerNetwork(action, service)
	if err != nil {
		t.Fatalf("docker network %s %s: %v: %s", action, nodeContainer(service), err, out)
	}
}

// dockerNetworkTolerant is dockerNetwork that does NOT fail the test when docker
// reports the container is already in (or already absent from) the network — the
// idempotent end-state a t.Cleanup heal wants regardless of where the test left off.
func dockerNetworkTolerant(t *testing.T, action, service string) {
	t.Helper()
	out, err := runDockerNetwork(action, service)
	if err != nil && !isBenignNetworkErr(out) {
		t.Fatalf("docker network %s %s: %v: %s", action, nodeContainer(service), err, out)
	}
}

// runDockerNetwork executes the docker network argv for service and returns the
// trimmed combined output alongside the error, so callers can classify benign
// already-in-desired-state failures.
func runDockerNetwork(action, service string) (string, error) {
	args := dockerNetworkArgs(action, partitionNetwork(), nodeContainer(service), service)
	out, err := exec.Command("docker", args...).CombinedOutput() //nolint:gosec // fixed argv, test-only
	return strings.TrimSpace(string(out)), err
}

// isBenignNetworkErr reports whether a `docker network connect/disconnect` failure is
// just "the container is already in the state we asked for" — connecting an
// already-connected container or disconnecting an already-disconnected one. Those are
// no-ops for an idempotent heal, not test failures.
func isBenignNetworkErr(out string) bool {
	return strings.Contains(out, "already exists in network") ||
		strings.Contains(out, "is not connected to network")
}

// RunningNodes reads the cluster's running/total node counts from the management API
// (WARREN_CLUSTER_MGMT) in a single query, failing the test on error. The partition
// campaign polls it to OBSERVE the minority drop out (running falls below total) and
// rejoin (running == total) after heal. A short-timeout, keep-alive-disabled HTTP
// client is used so no pooled connection goroutine survives into a goleak check.
func RunningNodes(t *testing.T) (running, total int) {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	running, total, err := fetchRunningNodes(client, mgmt)
	if err != nil {
		t.Fatalf("read running-node count via management API: %v", err)
	}
	return running, total
}

// TryWaitRunningNodes polls the management API until EXACTLY `want` nodes report
// running, returning true, or false if the timeout elapses. Unlike WaitClusterReady it
// does NOT fail the test and matches an exact count, so the partition campaign can use
// it both to observe the minority drop to the majority count (want=2 of 3) and to
// confirm full-membership recovery (want=3). Transient read errors/timeouts — expected
// while a partition is undetected and rmq0's management API briefly hangs trying to
// reach the unreachable member — are tolerated and retried; the caller decides whether
// a false return is a failure (isolation never happened) or a cue to force a rejoin. A
// short-timeout, keep-alive-disabled HTTP client is used so no pooled connection
// goroutine survives into a goleak check.
func TryWaitRunningNodes(t *testing.T, want int, timeout time.Duration) bool {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(timeout)
	for {
		if running, _, err := fetchRunningNodes(client, mgmt); err == nil && running == want {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(1 * time.Second)
	}
}

// NodeVersions reads each cluster member's RabbitMQ version from the management API
// (WARREN_CLUSTER_MGMT), returning node name → version. The rolling-upgrade campaign
// uses it to report whether it ran against a homogeneous or a genuinely mixed-version
// (3.13 + 4.x) cluster, so a homogeneous run is logged as such rather than silently
// passing as if it had exercised cross-version continuity. A short-timeout,
// keep-alive-disabled HTTP client is used so no pooled connection goroutine survives
// into a goleak check.
func NodeVersions(t *testing.T) map[string]string {
	t.Helper()
	mgmt := ClusterMgmt(t)
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	versions, err := fetchNodeVersions(client, mgmt)
	if err != nil {
		t.Fatalf("read node versions via management API: %v", err)
	}
	return versions
}

// NewToxiproxy returns a Toxiproxy control client bound to WARREN_TOXIPROXY_URL,
// for cutting/healing a node's AMQP port in the partition campaigns.
func NewToxiproxy(t *testing.T) *ToxiproxyClient {
	t.Helper()
	return NewToxiproxyClient(ToxiproxyURL(t))
}
