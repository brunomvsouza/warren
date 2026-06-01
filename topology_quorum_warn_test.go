package warren

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWarnQuorumDeliveryLimit asserts the version-aware poison-loop warning
// fires only for quorum queues with DeliveryLimit==0 (T58 / R10-2 / RMQ-06).
func TestWarnQuorumDeliveryLimit(t *testing.T) {
	quorumNoLimit := Queue{Name: "orders", Type: QueueTypeQuorum, Durable: true}
	quorumWithLimit := Queue{Name: "events", Type: QueueTypeQuorum, Durable: true, DeliveryLimit: 5}
	classic := Queue{Name: "logs"}

	t.Run("4.x warns about default-20 silent drop", func(t *testing.T) {
		l := &recordingLogger{}
		warnQuorumDeliveryLimit(l, []Queue{quorumNoLimit}, "4.0.5")
		require.Len(t, l.warnings, 1)
		w := strings.ToLower(l.warnings[0])
		assert.Contains(t, w, "orders")
		assert.Contains(t, w, "20")
	})

	t.Run("3.13 warns about unbounded poison loop", func(t *testing.T) {
		l := &recordingLogger{}
		warnQuorumDeliveryLimit(l, []Queue{quorumNoLimit}, "3.13.2")
		require.Len(t, l.warnings, 1)
		w := strings.ToLower(l.warnings[0])
		assert.Contains(t, w, "orders")
		assert.Contains(t, w, "unbounded")
	})

	t.Run("unknown version warns about both failure modes", func(t *testing.T) {
		l := &recordingLogger{}
		warnQuorumDeliveryLimit(l, []Queue{quorumNoLimit}, "")
		require.Len(t, l.warnings, 1)
		w := strings.ToLower(l.warnings[0])
		assert.Contains(t, w, "20")
		assert.Contains(t, w, "unbounded")
	})

	t.Run("no warning when DeliveryLimit set, classic, or version with limit", func(t *testing.T) {
		l := &recordingLogger{}
		warnQuorumDeliveryLimit(l, []Queue{quorumWithLimit, classic}, "4.0.0")
		assert.Empty(t, l.warnings)
	})

	// The broker version is untrusted (connection.start server-properties). A
	// version carrying a newline must not forge a second log line — %q escapes it
	// so the warning stays a single rendered line (review T58, Low).
	t.Run("malicious broker version is escaped, not interpolated raw", func(t *testing.T) {
		l := &recordingLogger{}
		warnQuorumDeliveryLimit(l, []Queue{quorumNoLimit}, "4.0\nFATAL injected")
		require.Len(t, l.warnings, 1)
		assert.NotContains(t, l.warnings[0], "4.0\nFATAL injected",
			"a raw newline in the broker version must be escaped, not interpolated verbatim")
		assert.Contains(t, l.warnings[0], `4.0\nFATAL injected`,
			"%q-escaped version preserves the content for diagnostics with the newline neutralised")
	})
}

// TestParseBrokerMajor covers the version-string parsing used to pick the
// correct poison-loop failure mode.
func TestParseBrokerMajor(t *testing.T) {
	cases := []struct {
		in    string
		major int
		ok    bool
	}{
		{"4.0.5", 4, true},
		{"3.13.2", 3, true},
		{"4.1.0-beta.3", 4, true},
		{"10.2.1", 10, true},
		{"", 0, false},
		{"garbage", 0, false},
	}
	for _, c := range cases {
		major, ok := parseBrokerMajor(c.in)
		assert.Equal(t, c.ok, ok, "ok for %q", c.in)
		if c.ok {
			assert.Equal(t, c.major, major, "major for %q", c.in)
		}
	}
}
