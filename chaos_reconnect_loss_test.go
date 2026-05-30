package warren_test

// Unit coverage for the zero-loss accounting used by the T45 reconnect chaos
// integration test (chaos_reconnect_integration_test.go). The loss function
// lives in an un-tagged test file so its logic — the headline "zero message
// loss" measure (SPEC §1, Lens-10 TV-09) — is verifiable on the fast unit lane
// without a broker, and reused verbatim by the integration test.

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lossByMessageID returns the sorted MessageIDs that were durably published
// (confirmed) but never appeared in the consumed set — the at-least-once "zero
// loss" measure. Both inputs are sets, so duplicate deliveries (which
// PublishRetry and the reconnect barrier produce by design) are tolerated:
// loss counts only IDs that are missing entirely, never IDs delivered more than
// once. An extra consumed ID with no matching publish is not loss and is ignored.
func lossByMessageID(published, consumed map[string]struct{}) []string {
	var lost []string
	for id := range published {
		if _, ok := consumed[id]; !ok {
			lost = append(lost, id)
		}
	}
	sort.Strings(lost)
	return lost
}

func TestLossByMessageID_zeroWhenAllConsumed(t *testing.T) {
	published := setOf("a", "b", "c")
	consumed := setOf("a", "b", "c")
	assert.Empty(t, lossByMessageID(published, consumed))
}

// VG-6 self-test: a harness that cannot detect a single dropped message must
// never be trusted to certify "zero loss". One deliberate drop must surface as
// exactly one lost ID.
func TestLossByMessageID_injectedDropReportsExactlyOne(t *testing.T) {
	published := setOf("a", "b", "c", "d")
	consumed := setOf("a", "b", "d") // "c" deliberately dropped
	lost := lossByMessageID(published, consumed)
	require.Len(t, lost, 1, "exactly one dropped message must be reported")
	assert.Equal(t, []string{"c"}, lost)
}

func TestLossByMessageID_toleratesExtraConsumed(t *testing.T) {
	// Duplicates / replays leave consumed a superset of published — still zero loss.
	published := setOf("a", "b")
	consumed := setOf("a", "b", "ghost-redelivery")
	assert.Empty(t, lossByMessageID(published, consumed))
}

func TestLossByMessageID_reportsAllMissingSorted(t *testing.T) {
	published := setOf("m3", "m1", "m2")
	consumed := setOf("m2")
	assert.Equal(t, []string{"m1", "m3"}, lossByMessageID(published, consumed))
}

func setOf(ids ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[id] = struct{}{}
	}
	return m
}
