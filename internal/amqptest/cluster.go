//go:build cluster

package amqptest

import (
	"os"
	"testing"
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
