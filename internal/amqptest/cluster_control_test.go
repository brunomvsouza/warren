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
