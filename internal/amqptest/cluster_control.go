package amqptest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// This file holds the cluster control toolkit's pure / transport halves: the
// management-API queue-state parse + round-trip, the thin Toxiproxy client, and
// the node-name/container/argv mappers. It carries NO build tag (like
// cluster_parse.go) so these can be unit-tested in the default lane against
// httptest — without the `cluster` tag, a live cluster, or a container runtime.
// The cluster-tagged file (cluster.go) adds the thin *testing.T wrappers that
// drive these against the real compose cluster (T166b).

// QuorumQueueState is the subset of the RabbitMQ management API's
// /api/queues/{vhost}/{name} payload the cluster lane reads to observe Raft
// leadership and message accounting. For a quorum queue the API reports the
// current leader, the full member set, and which members are online — the fields a
// leader-failover assertion needs. A classic queue omits these, so Leader/Members
// are empty for non-quorum queues rather than invented.
//
// MessagesReady / MessagesUnacknowledged are the queue's depth split: ready (not
// yet delivered) vs unacknowledged (delivered to a consumer, not yet acked). The
// SAC Prefetch(N>1) campaign (T166i) polls MessagesUnacknowledged to PROVE a
// multi-message in-flight set is held by the active consumer before it is killed —
// the broker-side fact that makes the multi-message requeue non-vacuous (a single
// in-flight message could be requeued without these ever exceeding one).
type QuorumQueueState struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Leader  string   `json:"leader"`
	Members []string `json:"members"`
	Online  []string `json:"online"`
	// MessagesReady is messages ready for delivery (not yet checked out).
	MessagesReady int `json:"messages_ready"`
	// MessagesUnacknowledged is messages delivered to a consumer but not yet acked —
	// the in-flight (checked-out) set the broker requeues if that consumer dies.
	MessagesUnacknowledged int `json:"messages_unacknowledged"`
}

// parseQuorumQueueState decodes a management-API queue payload into a
// QuorumQueueState. Kept separate from the HTTP round-trip so the decoding rules
// (and the empty-for-classic behaviour) are unit-testable without a server.
func parseQuorumQueueState(body []byte) (QuorumQueueState, error) {
	var s QuorumQueueState
	if err := json.Unmarshal(body, &s); err != nil {
		return QuorumQueueState{}, fmt.Errorf("decode queue state: %w", err)
	}
	return s, nil
}

// fetchQuorumQueueState reads a queue's leadership state from the management API
// at mgmtBase (scheme://userinfo@host[:port]). Credentials in the userinfo ride
// the Authorization header — never the request URL — so they cannot leak via an
// error string or log line, mirroring queueArgsViaManagement in the integration
// lane. An empty vhost defaults to "/". A non-200 status is an error.
func fetchQuorumQueueState(client *http.Client, mgmtBase, vhost, queue string) (QuorumQueueState, error) {
	base, err := url.Parse(mgmtBase)
	if err != nil {
		return QuorumQueueState{}, fmt.Errorf("parse management URL: %w", err)
	}
	if base.Host == "" {
		return QuorumQueueState{}, fmt.Errorf("management URL has no host:port")
	}
	if vhost == "" {
		vhost = "/"
	}

	// The host:port comes from the base URL; the userinfo is stripped here and
	// re-applied as a Basic-Auth header below, so the request URL never carries
	// credentials.
	apiURL := fmt.Sprintf("%s://%s/api/queues/%s/%s",
		base.Scheme, base.Host, url.PathEscape(vhost), url.PathEscape(queue))

	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return QuorumQueueState{}, err
	}
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return QuorumQueueState{}, fmt.Errorf("management GET %s://%s: %w", base.Scheme, base.Host, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return QuorumQueueState{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return QuorumQueueState{}, fmt.Errorf("management GET %s returned %d: %s",
			apiURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return parseQuorumQueueState(body)
}

// clusterNodeState is the subset of a management-API /api/nodes entry the cluster
// lane reads to gate on full-cluster readiness: each node's name and whether it is
// currently running. After a kill/restart, `docker start` returns before RabbitMQ
// has rebooted and rejoined, during which the node is listed but running=false.
type clusterNodeState struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
}

// parseRunningNodes decodes an /api/nodes payload and returns how many nodes report
// running plus the total listed, so a readiness gate can wait for every member to
// be back after a kill/restart. Kept separate from the HTTP round-trip so the
// counting rule is unit-testable without a server.
func parseRunningNodes(body []byte) (running, total int, err error) {
	var nodes []clusterNodeState
	if err := json.Unmarshal(body, &nodes); err != nil {
		return 0, 0, fmt.Errorf("decode nodes state: %w", err)
	}
	for _, n := range nodes {
		if n.Running {
			running++
		}
	}
	return running, len(nodes), nil
}

// fetchRunningNodes reads /api/nodes from the management API at mgmtBase and
// returns the running/total node counts. Credentials in the userinfo ride the
// Authorization header — never the request URL — mirroring fetchQuorumQueueState.
// A non-200 status is an error.
func fetchRunningNodes(client *http.Client, mgmtBase string) (running, total int, err error) {
	base, err := url.Parse(mgmtBase)
	if err != nil {
		return 0, 0, fmt.Errorf("parse management URL: %w", err)
	}
	if base.Host == "" {
		return 0, 0, fmt.Errorf("management URL has no host:port")
	}

	apiURL := fmt.Sprintf("%s://%s/api/nodes", base.Scheme, base.Host)
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return 0, 0, err
	}
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("management GET %s://%s/api/nodes: %w", base.Scheme, base.Host, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("management GET %s returned %d: %s",
			apiURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return parseRunningNodes(body)
}

// ToxiproxyClient is a thin client for the Toxiproxy v2 control API (the
// unauthenticated HTTP API the cluster's toxiproxy sidecar exposes on
// WARREN_TOXIPROXY_URL). It cuts a node's AMQP connectivity by disabling that
// node's proxy — closing live connections and refusing new ones — and heals it
// by re-enabling, the deterministic cut/heal the partition campaigns (T166g)
// drive. Disable/enable (rather than a timeout toxic) is the cleanest full cut.
type ToxiproxyClient struct {
	base   string
	client *http.Client
}

// NewToxiproxyClient returns a client for the Toxiproxy control API rooted at base
// (e.g. http://localhost:8474). The HTTP client disables keep-alives so no pooled
// connection goroutine survives into a goleak check.
func NewToxiproxyClient(base string) *ToxiproxyClient {
	return &ToxiproxyClient{
		base: strings.TrimRight(base, "/"),
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
	}
}

// DisableProxy cuts a node's AMQP connectivity by disabling its proxy.
func (c *ToxiproxyClient) DisableProxy(name string) error { return c.setProxyEnabled(name, false) }

// EnableProxy heals a previously cut node by re-enabling its proxy.
func (c *ToxiproxyClient) EnableProxy(name string) error { return c.setProxyEnabled(name, true) }

// setProxyEnabled flips a proxy's enabled flag via POST /proxies/{name}, the
// Toxiproxy v2 update endpoint.
func (c *ToxiproxyClient) setProxyEnabled(name string, enabled bool) error {
	body, err := json.Marshal(map[string]any{"enabled": enabled})
	if err != nil {
		return err
	}
	apiURL := c.base + "/proxies/" + url.PathEscape(name)
	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy POST %s: %w", apiURL, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("toxiproxy POST %s returned %d: %s",
			apiURL, resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	return nil
}

// clusterContainerPrefix is the container_name prefix pinned in
// docker-compose.cluster.yml (warren-rmq0/1/2): docker stop/start/kill address
// containers by this name, decoupling the toolkit from the Compose project's
// working directory.
const clusterContainerPrefix = "warren-"

// nodeContainer maps a compose service short name (rmq0) to its pinned container
// name (warren-rmq0) — the name docker stop/start/kill addresses.
func nodeContainer(service string) string {
	return clusterContainerPrefix + service
}

// NodeService maps a broker node name (rabbit@rmq0, as the management API reports
// leaders/members) to its compose service short name (rmq0), so a test can take a
// QuorumLeader result and stop the node hosting it. A name without a "@" passes
// through unchanged.
func NodeService(node string) string {
	if i := strings.IndexByte(node, '@'); i >= 0 {
		return node[i+1:]
	}
	return node
}

// dockerNodeArgs builds the argv for a `docker <action> <container>` invocation
// (stop/start/kill a node container). Pure, so the command shape is unit-testable
// without a container runtime.
func dockerNodeArgs(action, container string) []string {
	return []string{action, container}
}
