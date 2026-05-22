package redact_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/internal/redact"
)

// taggedErr is a test-only error type used to verify errors.As traversal
// through a redactedError chain.
type taggedErr struct{ code int }

func (e *taggedErr) Error() string { return fmt.Sprintf("tagged:%d", e.code) }

func TestURI(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// No userinfo — untouched.
		{name: "bare host", input: "amqp://h", want: "amqp://h"},
		{name: "host with port", input: "amqp://h:5672", want: "amqp://h:5672"},
		{name: "host with vhost", input: "amqp://h:5672/myvhost", want: "amqp://h:5672/myvhost"},
		{name: "amqps no auth", input: "amqps://h:5671", want: "amqps://h:5671"},
		{name: "empty string", input: "", want: ""},
		{name: "plain text no uri", input: "connecting to broker", want: "connecting to broker"},

		// User only (no password).
		{name: "user only", input: "amqp://u@h", want: "amqp://***@h"},
		{name: "user only with port", input: "amqp://u@h:5672", want: "amqp://***@h:5672"},

		// User + password.
		{name: "user+pass basic", input: "amqp://u:p@h", want: "amqp://***@h"},
		{name: "user+pass with port and vhost", input: "amqp://u:p@h:5672/v", want: "amqp://***@h:5672/v"},
		{name: "user+pass with query", input: "amqp://u:p@h:5672/v?heartbeat=10", want: "amqp://***@h:5672/v?heartbeat=10"},
		{name: "amqps user+pass", input: "amqps://u:p@h:5671/v", want: "amqps://***@h:5671/v"},

		// Percent-encoded passwords — common in connection strings.
		{name: "percent-encoded @ in pass", input: "amqp://u:p%40ss@h:5672/v", want: "amqp://***@h:5672/v"},
		{name: "percent-encoded @ in pass with query", input: "amqp://u:p%40w@h:5672/v?heartbeat=10", want: "amqp://***@h:5672/v?heartbeat=10"},
		{name: "percent-encoded space in pass", input: "amqp://u:p%20ss@h/v", want: "amqp://***@h/v"},
		{name: "amqps percent-encoded pass", input: "amqps://user:p%40ss@host:5671/vhost", want: "amqps://***@host:5671/vhost"},

		// Trailing slash variants.
		{name: "trailing slash no auth", input: "amqp://h/", want: "amqp://h/"},
		{name: "trailing slash with auth", input: "amqp://u:p@h/", want: "amqp://***@h/"},

		// Multiple URIs in one string (e.g. log line or error message).
		{
			name:  "two URIs in one string",
			input: "dial amqp://u:p@h1:5672 then amqp://u:p@h2:5672",
			want:  "dial amqp://***@h1:5672 then amqp://***@h2:5672",
		},
		{
			name:  "amqp and amqps in one string",
			input: "primary amqp://u:p@h1 fallback amqps://u:p@h2:5671",
			want:  "primary amqp://***@h1 fallback amqps://***@h2:5671",
		},

		// IPv6 literal hosts.
		{name: "IPv6 with auth", input: "amqp://u:p@[::1]:5672/v", want: "amqp://***@[::1]:5672/v"},
		{name: "IPv6 no auth", input: "amqp://[::1]:5672/v", want: "amqp://[::1]:5672/v"},

		// Malformed / non-AMQP URIs — returned unchanged.
		{name: "http uri", input: "http://u:p@h/path", want: "http://u:p@h/path"},
		{name: "just text with at-sign", input: "user@host", want: "user@host"},
		{name: "truncated scheme", input: "amqp://", want: "amqp://"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, redact.URI(tc.input))
		})
	}
}

func TestURIs(t *testing.T) {
	t.Run("redacts each element independently", func(t *testing.T) {
		input := []string{
			"amqp://u:p@h1:5672",
			"amqps://u:p@h2:5671/v",
			"amqp://h3",
		}
		got := redact.URIs(input)
		require.Len(t, got, 3)
		assert.Equal(t, "amqp://***@h1:5672", got[0])
		assert.Equal(t, "amqps://***@h2:5671/v", got[1])
		assert.Equal(t, "amqp://h3", got[2])
	})

	t.Run("does not modify input slice", func(t *testing.T) {
		input := []string{"amqp://u:p@h"}
		original := input[0]
		redact.URIs(input)
		assert.Equal(t, original, input[0], "URIs must not modify the input slice")
	})

	t.Run("nil slice", func(t *testing.T) {
		assert.Nil(t, redact.URIs(nil))
	})

	t.Run("empty slice", func(t *testing.T) {
		got := redact.URIs([]string{})
		assert.Empty(t, got)
	})
}

func TestError(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, redact.Error(nil))
	})

	t.Run("redacts URI in message", func(t *testing.T) {
		orig := fmt.Errorf("dial failed: amqp://user:secret@host:5672/v")
		redacted := redact.Error(orig)
		require.NotNil(t, redacted)
		assert.Contains(t, redacted.Error(), "amqp://***@host:5672/v")
		assert.NotContains(t, redacted.Error(), "secret")
	})

	t.Run("preserves error chain for errors.Is", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		orig := fmt.Errorf("dial amqp://u:p@h: %w", sentinel)
		redacted := redact.Error(orig)
		require.NotNil(t, redacted)
		assert.True(t, errors.Is(redacted, sentinel), "errors.Is must traverse the redacted chain")
	})

	t.Run("preserves error chain for errors.As", func(t *testing.T) {
		orig := &taggedErr{code: 42}
		wrapped := fmt.Errorf("dial amqp://u:p@h: %w", orig)
		redacted := redact.Error(wrapped)
		var target *taggedErr
		require.True(t, errors.As(redacted, &target))
		assert.Equal(t, 42, target.code)
	})

	t.Run("plain error without URI unchanged in chain", func(t *testing.T) {
		orig := errors.New("some error")
		redacted := redact.Error(orig)
		assert.Equal(t, "some error", redacted.Error())
		assert.True(t, errors.Is(redacted, orig))
	})
}
