package amqptest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise the cluster control toolkit's pure / HTTP halves in the
// DEFAULT lane (no `cluster` tag, no live cluster): JSON parsing, the management
// round-trip against an httptest server (so credential handling and vhost escaping
// are proven without a broker), the Toxiproxy client against an httptest server,
// and the node-name/container/argv mappers. The cluster-tagged thin wrappers
// (QuorumLeader/StopNode/StartNode/KillNode/NewToxiproxy) are verified live on the
// cluster lane by cluster_control_cluster_test.go.

func TestParseQuorumQueueState(t *testing.T) {
	t.Run("quorum queue reports leader, members, online and message counts", func(t *testing.T) {
		body := []byte(`{
			"name": "orders",
			"type": "quorum",
			"leader": "rabbit@rmq1",
			"members": ["rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"],
			"online": ["rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"],
			"messages_ready": 3,
			"messages_unacknowledged": 7
		}`)

		state, err := parseQuorumQueueState(body)
		require.NoError(t, err)
		assert.Equal(t, "orders", state.Name)
		assert.Equal(t, "quorum", state.Type)
		assert.Equal(t, "rabbit@rmq1", state.Leader)
		assert.Equal(t, []string{"rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"}, state.Members)
		assert.Equal(t, []string{"rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"}, state.Online)
		assert.Equal(t, 3, state.MessagesReady, "messages_ready must parse for the in-flight gate")
		assert.Equal(t, 7, state.MessagesUnacknowledged, "messages_unacknowledged must parse for the in-flight gate")
	})

	t.Run("classic queue payload yields empty leader/members (no panic)", func(t *testing.T) {
		// A classic queue has no quorum leadership fields; the parser must not
		// invent them — a caller asserting on Leader sees the empty string.
		body := []byte(`{"name": "legacy", "type": "classic", "node": "rabbit@rmq0"}`)

		state, err := parseQuorumQueueState(body)
		require.NoError(t, err)
		assert.Equal(t, "classic", state.Type)
		assert.Empty(t, state.Leader)
		assert.Empty(t, state.Members)
	})

	t.Run("malformed JSON returns an error", func(t *testing.T) {
		_, err := parseQuorumQueueState([]byte(`{not json`))
		require.Error(t, err)
	})
}

func TestQuorumQueueState_FullyOnline(t *testing.T) {
	// The readiness predicate awaitQueueFullyOnline (cluster lane) polls on before it
	// injects a fault. Verified here on the default lane — without a live cluster — so
	// the gate logic that distinguishes "fully replicated" from "majority only" is not
	// exercised solely by a ~minute-scale cluster run.
	threeMembers := []string{"rabbit@rmq0", "rabbit@rmq1", "rabbit@rmq2"}

	t.Run("all three members present and online satisfies the gate", func(t *testing.T) {
		s := QuorumQueueState{Members: threeMembers, Online: threeMembers}
		assert.True(t, s.FullyOnline(3))
	})

	t.Run("a follower still catching up (member, not yet online) fails the gate", func(t *testing.T) {
		// The exact race the poller exists to cover: the third node is already a
		// member at declare but has not finished joining, so Online lags Members.
		s := QuorumQueueState{Members: threeMembers, Online: threeMembers[:2]}
		assert.False(t, s.FullyOnline(3), "majority-only online must not be read as fully replicated")
	})

	t.Run("only the majority are members yet fails the gate", func(t *testing.T) {
		s := QuorumQueueState{Members: threeMembers[:2], Online: threeMembers[:2]}
		assert.False(t, s.FullyOnline(3))
	})

	t.Run("more members than wanted fails the gate (exact count, not a floor)", func(t *testing.T) {
		four := append(append([]string{}, threeMembers...), "rabbit@rmq3")
		s := QuorumQueueState{Members: four, Online: four}
		assert.False(t, s.FullyOnline(3), "an unexpected extra member must not be silently accepted")
	})

	t.Run("empty state fails the gate (no panic)", func(t *testing.T) {
		assert.False(t, QuorumQueueState{}.FullyOnline(3))
	})
}

func TestFetchQuorumQueueState(t *testing.T) {
	t.Run("issues an authenticated GET against the escaped vhost path", func(t *testing.T) {
		var gotPath string
		var gotUser, gotPass string
		var gotAuthOK bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.EscapedPath()
			gotUser, gotPass, gotAuthOK = r.BasicAuth()
			// Credentials must arrive in the Authorization header, never the URL.
			assert.NotContains(t, r.URL.String(), "guest")
			_, _ = w.Write([]byte(`{"name":"orders","type":"quorum","leader":"rabbit@rmq1","members":["rabbit@rmq0","rabbit@rmq1"],"online":["rabbit@rmq0","rabbit@rmq1"]}`))
		}))
		defer srv.Close()

		state, err := fetchQuorumQueueState(srv.Client(), withUserinfo(t, srv.URL, "guest", "guest"), "/", "orders")
		require.NoError(t, err)

		assert.Equal(t, "/api/queues/%2F/orders", gotPath, "default vhost must be percent-escaped")
		assert.True(t, gotAuthOK, "credentials must ride the Authorization header")
		assert.Equal(t, "guest", gotUser)
		assert.Equal(t, "guest", gotPass)
		assert.Equal(t, "rabbit@rmq1", state.Leader)
	})

	t.Run("a named vhost is percent-escaped into the path", func(t *testing.T) {
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.EscapedPath()
			_, _ = w.Write([]byte(`{"type":"quorum","leader":"rabbit@rmq0"}`))
		}))
		defer srv.Close()

		_, err := fetchQuorumQueueState(srv.Client(), srv.URL, "orders/vh", "q")
		require.NoError(t, err)
		assert.Equal(t, "/api/queues/orders%2Fvh/q", gotPath)
	})

	t.Run("a non-200 status surfaces as an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Object Not Found", http.StatusNotFound)
		}))
		defer srv.Close()

		_, err := fetchQuorumQueueState(srv.Client(), srv.URL, "/", "missing")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "404")
	})

	t.Run("a URL with no host is rejected without an HTTP call", func(t *testing.T) {
		_, err := fetchQuorumQueueState(http.DefaultClient, "http://", "/", "q")
		require.Error(t, err)
	})
}

func TestParseConnectionNodes(t *testing.T) {
	body := []byte(`[
		{"name":"127.0.0.1:5001 -> 127.0.0.1:5680","node":"rabbit@rmq0","client_properties":{"connection_name":"camp-pub-0"}},
		{"name":"127.0.0.1:5002 -> 127.0.0.1:5682","node":"rabbit@rmq2","client_properties":{"connection_name":"camp-con-0"}},
		{"name":"127.0.0.1:5003 -> 127.0.0.1:5681","node":"rabbit@rmq1","client_properties":{"connection_name":"other-pub-0"}},
		{"name":"127.0.0.1:5004 -> 127.0.0.1:5680","node":"rabbit@rmq0","client_properties":{}}
	]`)

	t.Run("filters by connection_name prefix and pairs node + broker name", func(t *testing.T) {
		got, err := parseConnectionNodes(body, "camp")
		require.NoError(t, err)
		assert.ElementsMatch(t, []ConnNode{
			{Name: "camp-pub-0", Node: "rabbit@rmq0", BrokerName: "127.0.0.1:5001 -> 127.0.0.1:5680"},
			{Name: "camp-con-0", Node: "rabbit@rmq2", BrokerName: "127.0.0.1:5002 -> 127.0.0.1:5682"},
		}, got, "only the prefix-matching named connections, paired with their node and broker connection name")
	})

	t.Run("preserves duplicate names (a mid-reconnect transient)", func(t *testing.T) {
		dup := []byte(`[
			{"node":"rabbit@rmq0","client_properties":{"connection_name":"camp-pub-0"}},
			{"node":"rabbit@rmq1","client_properties":{"connection_name":"camp-pub-0"}}
		]`)
		got, err := parseConnectionNodes(dup, "camp")
		require.NoError(t, err)
		assert.Len(t, got, 2, "both entries for the same name must survive so the caller sees the transient")
	})

	t.Run("empty prefix matches every named connection (the unnamed one is skipped)", func(t *testing.T) {
		got, err := parseConnectionNodes(body, "")
		require.NoError(t, err)
		assert.Len(t, got, 3)
	})

	t.Run("malformed JSON returns an error", func(t *testing.T) {
		_, err := parseConnectionNodes([]byte(`{not an array`), "camp")
		require.Error(t, err)
	})
}

func TestFetchConnectionNodes(t *testing.T) {
	t.Run("issues an authenticated GET against /api/connections and filters by prefix", func(t *testing.T) {
		var gotPath string
		var gotAuthOK bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.EscapedPath()
			_, _, gotAuthOK = r.BasicAuth()
			// Credentials must arrive in the Authorization header, never the URL.
			assert.NotContains(t, r.URL.String(), "guest")
			_, _ = w.Write([]byte(`[
				{"node":"rabbit@rmq0","client_properties":{"connection_name":"camp-pub-0"}},
				{"node":"rabbit@rmq1","client_properties":{"connection_name":"camp-pub-1"}},
				{"node":"rabbit@rmq2","client_properties":{"connection_name":"zzz-pub-0"}}
			]`))
		}))
		defer srv.Close()

		nodes, err := fetchConnectionNodes(srv.Client(), withUserinfo(t, srv.URL, "guest", "guest"), "camp")
		require.NoError(t, err)
		assert.Equal(t, "/api/connections", gotPath)
		assert.True(t, gotAuthOK, "credentials must ride the Authorization header")
		assert.ElementsMatch(t, []ConnNode{
			{Name: "camp-pub-0", Node: "rabbit@rmq0"},
			{Name: "camp-pub-1", Node: "rabbit@rmq1"},
		}, nodes, "the non-prefix connection is filtered out")
	})

	t.Run("a non-200 status surfaces as an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()

		_, err := fetchConnectionNodes(srv.Client(), srv.URL, "camp")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("a URL with no host is rejected without an HTTP call", func(t *testing.T) {
		_, err := fetchConnectionNodes(http.DefaultClient, "http://", "camp")
		require.Error(t, err)
	})

	t.Run("a malformed credentialed URL fails without leaking userinfo", func(t *testing.T) {
		// Both credentialed AND malformed (space in host) so url.Parse rejects the
		// raw input. net/url's own error echoes that input verbatim; the wrapper must
		// not, or the password would surface in a t.Fatalf log line (SPEC §8).
		// The "" prefix is irrelevant here (url.Parse fails before it is used) but,
		// being distinct from the other callers' "camp", keeps unparam from flagging
		// namePrefix as constant — the production caller in cluster.go varies it but
		// is hidden behind the cluster build tag on this lane.
		_, err := fetchConnectionNodes(http.DefaultClient, "http://guest:s3cr3t@ host/", "")
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "s3cr3t", "the parse error must not leak the password")
		assert.NotContains(t, err.Error(), "guest", "the parse error must not leak the username")
	})
}

func TestParseRunningNodes(t *testing.T) {
	t.Run("counts only nodes reporting running", func(t *testing.T) {
		// A killed-but-still-listed member reports running=false; the readiness gate
		// must not count it, so a following campaign waits for it to come back.
		body := []byte(`[
			{"name":"rabbit@rmq0","running":true},
			{"name":"rabbit@rmq1","running":false},
			{"name":"rabbit@rmq2","running":true}
		]`)

		running, total, err := parseRunningNodes(body)
		require.NoError(t, err)
		assert.Equal(t, 2, running)
		assert.Equal(t, 3, total)
	})

	t.Run("all running", func(t *testing.T) {
		body := []byte(`[{"name":"rabbit@rmq0","running":true},{"name":"rabbit@rmq1","running":true},{"name":"rabbit@rmq2","running":true}]`)
		running, total, err := parseRunningNodes(body)
		require.NoError(t, err)
		assert.Equal(t, 3, running)
		assert.Equal(t, 3, total)
	})

	t.Run("malformed JSON returns an error", func(t *testing.T) {
		_, _, err := parseRunningNodes([]byte(`{not an array`))
		require.Error(t, err)
	})
}

func TestFetchRunningNodes(t *testing.T) {
	t.Run("issues an authenticated GET against /api/nodes and counts running", func(t *testing.T) {
		var gotPath string
		var gotAuthOK bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.EscapedPath()
			_, _, gotAuthOK = r.BasicAuth()
			// Credentials must arrive in the Authorization header, never the URL.
			assert.NotContains(t, r.URL.String(), "guest")
			_, _ = w.Write([]byte(`[{"name":"rabbit@rmq0","running":true},{"name":"rabbit@rmq1","running":false}]`))
		}))
		defer srv.Close()

		running, total, err := fetchRunningNodes(srv.Client(), withUserinfo(t, srv.URL, "guest", "guest"))
		require.NoError(t, err)
		assert.Equal(t, "/api/nodes", gotPath)
		assert.True(t, gotAuthOK, "credentials must ride the Authorization header")
		assert.Equal(t, 1, running)
		assert.Equal(t, 2, total)
	})

	t.Run("a non-200 status surfaces as an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()

		_, _, err := fetchRunningNodes(srv.Client(), srv.URL)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("a URL with no host is rejected without an HTTP call", func(t *testing.T) {
		_, _, err := fetchRunningNodes(http.DefaultClient, "http://")
		require.Error(t, err)
	})
}

func TestToxiproxyClient_disableEnable(t *testing.T) {
	type update struct {
		Enabled bool `json:"enabled"`
	}
	var gotMethod, gotPath string
	var gotBody update
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.EscapedPath()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"rmq1","enabled":false}`))
	}))
	defer srv.Close()

	c := NewToxiproxyClient(srv.URL)

	require.NoError(t, c.DisableProxy("rmq1"))
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/proxies/rmq1", gotPath)
	assert.False(t, gotBody.Enabled, "DisableProxy must POST enabled=false")

	require.NoError(t, c.EnableProxy("rmq1"))
	assert.True(t, gotBody.Enabled, "EnableProxy must POST enabled=true")
}

func TestToxiproxyClient_trimsTrailingSlashAndReportsErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A double slash would mean the base trailing slash was not trimmed.
		assert.Equal(t, "/proxies/rmq2", r.URL.EscapedPath())
		http.Error(w, "no such proxy", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewToxiproxyClient(srv.URL + "/")
	err := c.DisableProxy("rmq2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestNodeService(t *testing.T) {
	assert.Equal(t, "rmq0", NodeService("rabbit@rmq0"))
	assert.Equal(t, "rmq2", NodeService("rabbit@rmq2"))
	// A bare service name (no rabbit@ prefix) passes through unchanged.
	assert.Equal(t, "rmq1", NodeService("rmq1"))
}

func TestNodeContainer(t *testing.T) {
	assert.Equal(t, "warren-rmq0", nodeContainer("rmq0"))
	assert.Equal(t, "warren-rmq2", nodeContainer("rmq2"))
}

func TestDockerNodeArgs(t *testing.T) {
	assert.Equal(t, []string{"kill", "warren-rmq1"}, dockerNodeArgs("kill", "warren-rmq1"))
	assert.Equal(t, []string{"start", "warren-rmq0"}, dockerNodeArgs("start", "warren-rmq0"))
	// The rolling-upgrade campaign restarts a node in place (stop+start atomically).
	assert.Equal(t, []string{"restart", "warren-rmq2"}, dockerNodeArgs("restart", "warren-rmq2"))
}

func TestPartitionNetwork(t *testing.T) {
	// The compose project is pinned to name `warren-cluster`, so Compose's default
	// network is `warren-cluster_default` — the network PartitionNode disconnects a
	// node from to inject a REAL broker-to-broker partition. Toxiproxy fronts only the
	// AMQP client ports (5672), so disabling a proxy cuts clients but leaves inter-node
	// Erlang distribution intact and cannot trigger pause_minority; severing the node's
	// network membership is what isolates it from its peers.
	assert.Equal(t, "warren-cluster_default", partitionNetwork())
}

func TestDockerNetworkArgs(t *testing.T) {
	t.Run("disconnect drops a node off the cluster network", func(t *testing.T) {
		assert.Equal(t,
			[]string{"network", "disconnect", "warren-cluster_default", "warren-rmq2"},
			dockerNetworkArgs("disconnect", "warren-cluster_default", "warren-rmq2", "rmq2"))
	})
	t.Run("connect re-adds the node WITH its compose service alias", func(t *testing.T) {
		// The heal must restore the compose service-name DNS alias (rmq2): a manual
		// `docker network connect` adds the container name/ID as aliases but NOT the
		// service alias, so without --alias peers and Toxiproxy could no longer resolve
		// the rejoining node by its rmq2 hostname.
		assert.Equal(t,
			[]string{"network", "connect", "--alias", "rmq2", "warren-cluster_default", "warren-rmq2"},
			dockerNetworkArgs("connect", "warren-cluster_default", "warren-rmq2", "rmq2"))
	})
}

func TestParseNodeVersions(t *testing.T) {
	t.Run("extracts each node's rabbit application version", func(t *testing.T) {
		// /api/nodes lists every member's applications; the rabbit app's version is the
		// RabbitMQ version. A genuinely mixed-version cluster reports distinct versions.
		body := []byte(`[
			{"name":"rabbit@rmq0","running":true,"applications":[{"name":"mnesia","version":"4.21"},{"name":"rabbit","version":"3.13.7"}]},
			{"name":"rabbit@rmq2","running":true,"applications":[{"name":"rabbit","version":"4.0.5"}]}
		]`)
		got, err := parseNodeVersions(body)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"rabbit@rmq0": "3.13.7", "rabbit@rmq2": "4.0.5"}, got,
			"a mixed-version cluster must report each member's distinct rabbit version")
	})

	t.Run("a node with no rabbit application yields an empty version (no panic)", func(t *testing.T) {
		body := []byte(`[{"name":"rabbit@rmq1","running":false,"applications":[]}]`)
		got, err := parseNodeVersions(body)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"rabbit@rmq1": ""}, got,
			"a member whose rabbit app is absent (e.g. mid-restart) maps to empty, not dropped")
	})

	t.Run("malformed JSON returns an error", func(t *testing.T) {
		_, err := parseNodeVersions([]byte(`{not an array`))
		require.Error(t, err)
	})
}

func TestFetchNodeVersions(t *testing.T) {
	t.Run("issues an authenticated GET against /api/nodes and maps versions", func(t *testing.T) {
		var gotPath, gotUser string
		var gotAuthOK bool
		// Distinct (non-"guest") credentials here so the management API's basic-auth
		// handling is exercised with a real value AND so unparam does not flag
		// withUserinfo's user/pass as constant across every call site (mirroring the
		// namePrefix-varying note in the connection-nodes test).
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.EscapedPath()
			gotUser, _, gotAuthOK = r.BasicAuth()
			// Credentials must arrive in the Authorization header, never the URL.
			assert.NotContains(t, r.URL.String(), "monitor")
			assert.NotContains(t, r.URL.String(), "s3cr3t")
			_, _ = w.Write([]byte(`[
				{"name":"rabbit@rmq0","applications":[{"name":"rabbit","version":"3.13.7"}]},
				{"name":"rabbit@rmq2","applications":[{"name":"rabbit","version":"4.0.5"}]}
			]`))
		}))
		defer srv.Close()

		versions, err := fetchNodeVersions(srv.Client(), withUserinfo(t, srv.URL, "monitor", "s3cr3t"))
		require.NoError(t, err)
		assert.Equal(t, "/api/nodes", gotPath)
		assert.True(t, gotAuthOK, "credentials must ride the Authorization header")
		assert.Equal(t, "monitor", gotUser)
		assert.Equal(t, map[string]string{"rabbit@rmq0": "3.13.7", "rabbit@rmq2": "4.0.5"}, versions)
	})

	t.Run("a non-200 status surfaces as an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()

		_, err := fetchNodeVersions(srv.Client(), srv.URL)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("a URL with no host is rejected without an HTTP call", func(t *testing.T) {
		_, err := fetchNodeVersions(http.DefaultClient, "http://")
		require.Error(t, err)
	})
}

// withUserinfo injects userinfo into a base URL (httptest.Server.URL carries none)
// so fetchQuorumQueueState's basic-auth handling can be exercised.
func withUserinfo(t *testing.T, raw, user, pass string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	u.User = url.UserPassword(user, pass)
	return u.String()
}
