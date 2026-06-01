package warren

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expandedDLQ returns the auto-declared orders.dlq from an expanded topology.
func expandedDLQ(t *testing.T, topo *Topology) *Queue {
	t.Helper()
	q := findQueue(topo.expand(), "orders.dlq")
	require.NotNil(t, q, "orders.dlq not found in expanded topology")
	return q
}

// T65 (R10-10 / SRE-03 / ST-08 / DP-03): the auto-declared <source>.dlq is
// durable AND bounded by default — an unbounded DLQ fills disk and trips a
// broker-wide connection.blocked alarm (one service's poison storm → cluster
// outage). Default bounds: x-max-length, x-overflow=drop-head, and a finite
// x-message-ttl (personal-data retention, GDPR 5(1)(e)).
func TestTopology_expand_autoDLQ_boundedByDefault(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	dlq := expandedDLQ(t, topo)

	assert.True(t, dlq.Durable, "auto DLQ must be durable")
	assert.Equal(t, int64(defaultDLQMaxLength), dlq.Args["x-max-length"], "auto DLQ must carry the default x-max-length")
	assert.Equal(t, string(OverflowDropHead), dlq.Args["x-overflow"], "auto DLQ overflow defaults to drop-head")
	assert.Equal(t, defaultDLQMessageTTL.Milliseconds(), dlq.Args["x-message-ttl"], "auto DLQ must carry the default retention TTL")
}

func TestTopology_expand_autoDLQ_unboundedOptOut(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders", DLQUnbounded: true}},
	}
	dlq := expandedDLQ(t, topo)

	assert.True(t, dlq.Durable)
	assert.NotContains(t, dlq.Args, "x-max-length", "DLQUnbounded must drop the length bound")
	assert.NotContains(t, dlq.Args, "x-message-ttl", "DLQUnbounded must drop the TTL")
	assert.NotContains(t, dlq.Args, "x-overflow")
}

func TestTopology_expand_autoDLQ_customBounds(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{
			Source:        "orders",
			DLQMaxLength:  500,
			DLQMessageTTL: 2 * time.Hour,
			DLQOverflow:   OverflowRejectPublish,
		}},
	}
	dlq := expandedDLQ(t, topo)

	assert.Equal(t, int64(500), dlq.Args["x-max-length"])
	assert.Equal(t, (2 * time.Hour).Milliseconds(), dlq.Args["x-message-ttl"])
	assert.Equal(t, string(OverflowRejectPublish), dlq.Args["x-overflow"])
}

// validate() must reject an unknown DLQOverflow up front (review T65): it is
// written straight into the auto-DLQ's x-overflow, so an unvalidated typo would
// otherwise surface only as a late broker PRECONDITION_FAILED at declare time.
func TestTopology_validate_rejectsUnknownDLQOverflow(t *testing.T) {
	t.Run("unknown DLQOverflow is rejected", func(t *testing.T) {
		topo := &Topology{
			Queues:      []Queue{{Name: "orders", Durable: true}},
			DeadLetters: []DeadLetter{{Source: "orders", DLQOverflow: OverflowPolicy("drop_head")}},
		}
		err := topo.validate()
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidOptions)
	})

	t.Run("empty and valid DLQOverflow pass", func(t *testing.T) {
		for _, p := range []OverflowPolicy{"", OverflowDropHead, OverflowRejectPublish} {
			topo := &Topology{
				Queues:      []Queue{{Name: "orders", Durable: true}},
				DeadLetters: []DeadLetter{{Source: "orders", DLQOverflow: p}},
			}
			assert.NoError(t, topo.validate(), "DLQOverflow %q must validate", p)
		}
	})
}

// An explicitly-declared DLQ wins: the auto-bounds are NOT injected over a
// user-declared <source>.dlq (the !exists guard).
func TestTopology_expand_explicitDLQ_keepsUserArgs(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{
			{Name: "orders", Durable: true},
			{Name: "orders.dlq", Durable: true, Args: map[string]any{"x-max-length": int64(7)}},
		},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	dlq := expandedDLQ(t, topo)
	require.Equal(t, int64(7), dlq.Args["x-max-length"], "user-declared DLQ args must be preserved")
	assert.NotContains(t, dlq.Args, "x-message-ttl", "auto defaults must not be injected over an explicit DLQ")
}
