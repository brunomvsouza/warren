package amqptest

import (
	"bytes"
	"encoding/json"
	"errors"
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

// FullyOnline reports whether the quorum queue has exactly want members AND all
// want of them online — the readiness condition the cluster lane's
// awaitQueueFullyOnline poller waits for before injecting a partition or a rolling
// restart. Membership is assigned at declare, but a follower can still be catching
// up (already counted as a member, yet not yet in Online), so a single read right
// after Declare can race and see only the majority online. The count is exact, not
// a floor: an unexpected extra member is not silently accepted. Kept here (untagged,
// beside the QuorumQueueState it reads) so the gate condition is unit-testable on
// the default lane without a live cluster.
func (s QuorumQueueState) FullyOnline(want int) bool {
	return len(s.Members) == want && len(s.Online) == want
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
		// Don't wrap err: net/url echoes the raw input, which would leak any
		// userinfo credential in a malformed management URL (SPEC §8).
		return QuorumQueueState{}, errors.New("parse management URL: malformed")
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
// lane reads to gate on full-cluster readiness: each node's name, whether it is
// currently running, and its installed applications. After a kill/restart, `docker
// start` returns before RabbitMQ has rebooted and rejoined, during which the node is
// listed but running=false. Applications carries each member's installed OTP/RabbitMQ
// apps; the rabbit app's version IS the RabbitMQ version the rolling-upgrade campaign
// reads to tell a homogeneous cluster from a genuinely mixed-version one.
type clusterNodeState struct {
	Name         string `json:"name"`
	Running      bool   `json:"running"`
	Applications []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"applications"`
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

// parseNodeVersions decodes an /api/nodes payload into a map of node name → its
// RabbitMQ (rabbit application) version, so the rolling-upgrade campaign can SEE and
// log whether it ran against a homogeneous or a genuinely mixed-version (3.13 + 4.x)
// cluster — a homogeneous run is then reported as such rather than silently passing
// as if it had exercised cross-version continuity. A node whose rabbit application is
// absent (e.g. mid-restart) maps to "" rather than being dropped. Kept separate from
// the HTTP round-trip so the extraction rule is unit-testable without a server.
func parseNodeVersions(body []byte) (map[string]string, error) {
	var nodes []clusterNodeState
	if err := json.Unmarshal(body, &nodes); err != nil {
		return nil, fmt.Errorf("decode nodes state: %w", err)
	}
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		var version string
		for _, app := range n.Applications {
			if app.Name == "rabbit" {
				version = app.Version
				break
			}
		}
		out[n.Name] = version
	}
	return out, nil
}

// fetchNodesBody issues an authenticated GET /api/nodes against the management API at
// mgmtBase and returns the raw body. Credentials in the userinfo ride the
// Authorization header — never the request URL — mirroring fetchQuorumQueueState. A
// non-200 status is an error. Shared by the running-count readiness gate and the
// version-map reader, the two /api/nodes consumers.
func fetchNodesBody(client *http.Client, mgmtBase string) ([]byte, error) {
	base, err := url.Parse(mgmtBase)
	if err != nil {
		// Don't wrap err: net/url echoes the raw input, which would leak any
		// userinfo credential in a malformed management URL (SPEC §8).
		return nil, errors.New("parse management URL: malformed")
	}
	if base.Host == "" {
		return nil, fmt.Errorf("management URL has no host:port")
	}

	apiURL := fmt.Sprintf("%s://%s/api/nodes", base.Scheme, base.Host)
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("management GET %s://%s/api/nodes: %w", base.Scheme, base.Host, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("management GET %s returned %d: %s",
			apiURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// fetchRunningNodes reads /api/nodes from the management API at mgmtBase and returns
// the running/total node counts.
func fetchRunningNodes(client *http.Client, mgmtBase string) (running, total int, err error) {
	body, err := fetchNodesBody(client, mgmtBase)
	if err != nil {
		return 0, 0, err
	}
	return parseRunningNodes(body)
}

// fetchNodeVersions reads /api/nodes from the management API at mgmtBase and returns
// the node name → rabbit-version map.
func fetchNodeVersions(client *http.Client, mgmtBase string) (map[string]string, error) {
	body, err := fetchNodesBody(client, mgmtBase)
	if err != nil {
		return nil, err
	}
	return parseNodeVersions(body)
}

// connectionEntry is the subset of a management-API /api/connections entry the
// cluster lane reads to see WHICH node a client connection landed on. Node is the
// broker node the socket is attached to; Name is the broker's per-socket connection
// name (it embeds the client's ephemeral TCP port, so it is unique per TCP socket
// and changes on every reconnect); ClientProperties.ConnectionName is the
// human-readable name warren sets per socket (connectionName-role-idx), used to
// isolate one test's pool from any other client sharing the cluster.
type connectionEntry struct {
	Name             string `json:"name"`
	Node             string `json:"node"`
	ClientProperties struct {
		ConnectionName string `json:"connection_name"`
	} `json:"client_properties"`
}

// ConnNode pairs a warren connection's client-set connection_name (Name) with the
// broker node it is attached to (Node) and the broker's per-socket connection name
// (BrokerName, unique per TCP socket — it changes on reconnect), as read from
// /api/connections. The rotation campaign (T166e) counts these to tell a CURRENT
// connection from a still-closing one with the same reused Name during a reconnect,
// and uses BrokerName to detect when a ForceReconnect has actually replaced the old
// sockets (the async drop has completed) rather than reading the pre-reconnect view.
type ConnNode struct {
	Name       string
	Node       string
	BrokerName string
}

// parseConnectionNodes decodes a management-API /api/connections payload and
// returns one ConnNode for every connection whose client-set connection_name
// starts with namePrefix — duplicates included, so a caller can detect a
// mid-reconnect transient (the same name appearing twice while the old connection
// closes). The prefix filter isolates one test's pool from any other client on the
// shared cluster; connections with no connection_name (or a non-matching one) are
// skipped. An empty prefix matches every named connection. Kept separate from the
// HTTP round-trip so the filter is unit-testable without a server.
func parseConnectionNodes(body []byte, namePrefix string) ([]ConnNode, error) {
	var conns []connectionEntry
	if err := json.Unmarshal(body, &conns); err != nil {
		return nil, fmt.Errorf("decode connections state: %w", err)
	}
	var out []ConnNode
	for _, c := range conns {
		name := c.ClientProperties.ConnectionName
		if name == "" || !strings.HasPrefix(name, namePrefix) {
			continue
		}
		out = append(out, ConnNode{Name: name, Node: c.Node, BrokerName: c.Name})
	}
	return out, nil
}

// fetchConnectionNodes reads /api/connections from the management API at mgmtBase
// and returns the ConnNode list for connections matching namePrefix. Credentials in
// the userinfo ride the Authorization header — never the request URL — mirroring
// fetchQuorumQueueState. A non-200 status is an error.
func fetchConnectionNodes(client *http.Client, mgmtBase, namePrefix string) ([]ConnNode, error) {
	base, err := url.Parse(mgmtBase)
	if err != nil {
		// Don't wrap err: net/url echoes the raw input, which would leak any
		// userinfo credential in a malformed management URL (SPEC §8).
		return nil, errors.New("parse management URL: malformed")
	}
	if base.Host == "" {
		return nil, fmt.Errorf("management URL has no host:port")
	}

	apiURL := fmt.Sprintf("%s://%s/api/connections", base.Scheme, base.Host)
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("management GET %s://%s/api/connections: %w", base.Scheme, base.Host, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("management GET %s returned %d: %s",
			apiURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return parseConnectionNodes(body, namePrefix)
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
// (stop/start/kill/restart a node container). Pure, so the command shape is
// unit-testable without a container runtime.
func dockerNodeArgs(action, container string) []string {
	return []string{action, container}
}

// clusterNetwork is the Docker network the compose cluster runs on: the project name
// (warren-cluster, pinned in docker-compose.cluster.yml) plus Compose's default
// "_default" suffix. PartitionNode disconnects a node from THIS network to inject a
// real broker-to-broker partition. Toxiproxy fronts only the AMQP client ports
// (5672), so disabling a proxy cuts CLIENTS but leaves inter-node Erlang distribution
// intact — it cannot trigger pause_minority. Severing the node's network membership
// is what isolates it from its peers so the minority side pauses.
const clusterNetwork = "warren-cluster_default"

// partitionNetwork returns the Docker network the partition campaign disconnects a
// node from. A function (not a bare const at the call site) so the value has one
// unit-tested source of truth, mirroring nodeContainer.
func partitionNetwork() string { return clusterNetwork }

// dockerNetworkArgs builds the argv for a `docker network <action> ...` invocation
// the partition campaign uses:
//   - disconnect: `network disconnect <network> <container>` — drops the node off the
//     cluster network, isolating it from BOTH clients and its peers (a real partition).
//   - connect:    `network connect --alias <alias> <network> <container>` — re-adds the
//     node, restoring the compose service-name DNS alias (e.g. rmq2) that a manual
//     reconnect would otherwise drop, so peers and Toxiproxy can resolve it by hostname
//     again. Pure, so the command shape is unit-testable without a container runtime.
func dockerNetworkArgs(action, network, container, alias string) []string {
	if action == "connect" {
		return []string{"network", "connect", "--alias", alias, network, container}
	}
	return []string{"network", action, network, container}
}
