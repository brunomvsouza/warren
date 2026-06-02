package warren

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// T76 / RMQ-05 / decision 52: a quorum queue running the at-least-once
// dead-letter strategy MUST pair with x-overflow=reject-publish. The broker
// silently accepts any overflow (gate G4 + the T76 probe confirmed even
// reject-publish-dlx is accepted on both 3.13 and 4.x) but does not honour
// at-least-once unless overflow is reject-publish, so warren couples the two
// client-side: auto-set reject-publish when unset, reject any other value.

// expand() auto-fills x-overflow=reject-publish for a quorum queue that gets
// the at-least-once strategy via a DeadLetter entry and left overflow unset.
func TestTopology_expand_autoSetsRejectPublishForQuorumAtLeastOnce(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.Equal(t, "at-least-once", src.Args["x-dead-letter-strategy"])
	assert.Equal(t, string(OverflowRejectPublish), src.Args["x-overflow"])
}

// Same auto-fill when the DLX is wired manually through Queue.Args.
func TestTopology_expand_autoSetsRejectPublishForManualDLX(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{"x-dead-letter-exchange": "my.dlx"},
		}},
	}
	expanded := topo.expand()
	assert.Equal(t, string(OverflowRejectPublish), expanded.Queues[0].Args["x-overflow"])
}

// An explicit reject-publish (the valid pairing) is preserved untouched.
func TestTopology_expand_respectsExplicitRejectPublish(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders", Overflow: OverflowRejectPublish}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.Equal(t, string(OverflowRejectPublish), src.Args["x-overflow"])
}

// A classic queue with a DLX does not get at-least-once, so no overflow is
// forced — the caller's choice (or absence) stands.
func TestTopology_expand_noOverflowForClassicWithDLX(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeClassic}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.NotContains(t, src.Args, "x-overflow")
}

// When the caller opts out of at-least-once (x-dead-letter-strategy override),
// the overflow coupling does not apply and overflow stays untouched.
func TestTopology_expand_noOverflowForUserAtMostOnce(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{"x-dead-letter-strategy": "at-most-once"},
		}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	expanded := topo.expand()
	src := findQueue(expanded, "orders")
	require.NotNil(t, src)
	assert.NotContains(t, src.Args, "x-overflow")
}

// validate() rejects drop-head on a quorum at-least-once queue (via DeadLetter).
func TestTopology_validate_rejectsDropHeadWithQuorumAtLeastOnce(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders", Overflow: OverflowDropHead}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "reject-publish")
}

// Same rejection when drop-head is set through raw Queue.Args["x-overflow"].
func TestTopology_validate_rejectsDropHeadViaRawArgs(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{
				"x-dead-letter-exchange": "my.dlx",
				"x-overflow":             "drop-head",
			},
		}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// reject-publish-dlx is also invalid with at-least-once: RabbitMQ requires
// exactly reject-publish, and the broker accepts the mismatch silently (probe).
func TestTopology_validate_rejectsRejectPublishDLXWithAtLeastOnce(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders", Overflow: OverflowRejectPublishDLX}},
	}
	err := topo.validate()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.Contains(t, err.Error(), "reject-publish-dlx")
}

// The valid pairing (reject-publish) passes validation.
func TestTopology_validate_allowsRejectPublishWithAtLeastOnce(t *testing.T) {
	topo := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
		DeadLetters: []DeadLetter{{Source: "orders", Overflow: OverflowRejectPublish}},
	}
	assert.NoError(t, topo.validate())
}

// When the user explicitly opts out of at-least-once, drop-head is allowed
// (the coupling rule applies only to the at-least-once strategy).
func TestTopology_validate_allowsDropHeadWhenNotAtLeastOnce(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{
				"x-dead-letter-strategy": "at-most-once",
				"x-overflow":             "drop-head",
			},
		}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}
	assert.NoError(t, topo.validate())
}

// A non-string x-dead-letter-strategy value is not the at-least-once string, so
// the coupling must not apply — validate() must allow drop-head AND expand() must
// not force reject-publish, in lockstep. Pre-fix the two diverged: quorumAtLeastOnce
// (used by validate) treated a non-string strategy as at-least-once and rejected
// drop-head, while expand treated it as opt-out and left overflow alone (RMQ-05).
func TestTopology_quorumAtLeastOnce_nonStringStrategy_optsOutInLockstep(t *testing.T) {
	mk := func() *Topology {
		return &Topology{
			Queues: []Queue{{
				Name: "orders", Durable: true, Type: QueueTypeQuorum,
				Args: map[string]any{
					"x-dead-letter-strategy": 42, // not the at-least-once string
					"x-overflow":             "drop-head",
				},
			}},
			DeadLetters: []DeadLetter{{Source: "orders"}},
		}
	}
	// validate side: drop-head is allowed (no at-least-once coupling).
	assert.NoError(t, mk().validate(), "a non-string strategy is not at-least-once; drop-head must be allowed")
	// expand side: reject-publish is NOT forced; the explicit drop-head stands.
	expanded := mk().expand()
	var src Queue
	for _, q := range expanded.Queues {
		if q.Name == "orders" {
			src = q
		}
	}
	assert.Equal(t, "drop-head", src.Args["x-overflow"], "expand must not override drop-head for a non-at-least-once strategy")
}

// A quorum queue without any DLX is unaffected — no at-least-once, no coupling.
func TestTopology_validate_dropHeadAllowedWithoutDLX(t *testing.T) {
	topo := &Topology{
		Queues: []Queue{{
			Name: "orders", Durable: true, Type: QueueTypeQuorum,
			Args: map[string]any{"x-overflow": "drop-head"},
		}},
	}
	assert.NoError(t, topo.validate())
}

// warnQuorumAtLeastOnceOverflow fires once per quorum at-least-once queue where
// warren auto-sets reject-publish (overflow left unset), and references the
// source-queue memory cost. No warning when overflow was set explicitly or the
// queue is not at-least-once.
func TestWarnQuorumAtLeastOnceOverflow(t *testing.T) {
	t.Run("warns and references memory cost when overflow auto-set", func(t *testing.T) {
		l := &recordingLogger{}
		topo := &Topology{
			Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
			DeadLetters: []DeadLetter{{Source: "orders"}},
		}
		warnQuorumAtLeastOnceOverflow(l, topo)
		require.Len(t, l.warnings, 1)
		w := strings.ToLower(l.warnings[0])
		assert.Contains(t, w, "orders")
		assert.Contains(t, w, "reject-publish")
		assert.Contains(t, w, "memory")
	})

	t.Run("no warning when overflow set explicitly", func(t *testing.T) {
		l := &recordingLogger{}
		topo := &Topology{
			Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
			DeadLetters: []DeadLetter{{Source: "orders", Overflow: OverflowRejectPublish}},
		}
		warnQuorumAtLeastOnceOverflow(l, topo)
		assert.Empty(t, l.warnings)
	})

	t.Run("no warning for classic queue", func(t *testing.T) {
		l := &recordingLogger{}
		topo := &Topology{
			Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeClassic}},
			DeadLetters: []DeadLetter{{Source: "orders"}},
		}
		warnQuorumAtLeastOnceOverflow(l, topo)
		assert.Empty(t, l.warnings)
	})

	t.Run("nil logger is a no-op", func(t *testing.T) {
		topo := &Topology{
			Queues:      []Queue{{Name: "orders", Durable: true, Type: QueueTypeQuorum}},
			DeadLetters: []DeadLetter{{Source: "orders"}},
		}
		assert.NotPanics(t, func() { warnQuorumAtLeastOnceOverflow(nil, topo) })
	})
}
