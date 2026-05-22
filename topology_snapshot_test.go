package warren

// White-box tests for Topology.AttachTo snapshot semantics.
// Package warren (not warren_test) to access unexported fields.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBareConnection builds a minimal *Connection with one fake managed connection,
// suitable for AttachTo snapshot unit tests that don't need a live broker.
func newBareConnection(t *testing.T) *Connection {
	t.Helper()
	mc := newBareManaged(t)
	return &Connection{
		pubConns: []*managedConn{mc},
		conConns: []*managedConn{},
	}
}

func TestTopology_AttachTo_snapshotStoredOnFirstCall(t *testing.T) {
	conn := newBareConnection(t)
	topo := &Topology{
		Queues: []Queue{{Name: "q1", Durable: true}},
	}
	topo.AttachTo(conn)

	conn.topoMu.RLock()
	snap, ok := conn.topoSnaps[topo]
	conn.topoMu.RUnlock()
	require.True(t, ok, "snapshot must be stored after first AttachTo")
	require.Len(t, snap.Queues, 1)
	assert.Equal(t, "q1", snap.Queues[0].Name)
}

func TestTopology_AttachTo_samePointerReplaces(t *testing.T) {
	conn := newBareConnection(t)
	topo := &Topology{
		Queues: []Queue{{Name: "original", Durable: true}},
	}
	topo.AttachTo(conn)

	// Mutate topology, re-attach with the same pointer.
	topo.Queues[0].Name = "mutated"
	topo.AttachTo(conn)

	conn.topoMu.RLock()
	snap := conn.topoSnaps[topo]
	keys := make([]*Topology, len(conn.topoKeys))
	copy(keys, conn.topoKeys)
	conn.topoMu.RUnlock()

	// Key should appear exactly once.
	assert.Len(t, keys, 1, "same pointer must not add a duplicate key")
	// Snapshot must reflect the mutation.
	require.Len(t, snap.Queues, 1)
	assert.Equal(t, "mutated", snap.Queues[0].Name, "snapshot must use value at second AttachTo time")
}

func TestTopology_AttachTo_differentPointerAppends(t *testing.T) {
	conn := newBareConnection(t)
	t1 := &Topology{Queues: []Queue{{Name: "q1", Durable: true}}}
	t2 := &Topology{Queues: []Queue{{Name: "q2", Durable: true}}}

	t1.AttachTo(conn)
	t2.AttachTo(conn)

	conn.topoMu.RLock()
	keys := make([]*Topology, len(conn.topoKeys))
	copy(keys, conn.topoKeys)
	conn.topoMu.RUnlock()

	assert.Len(t, keys, 2, "two different pointers must produce two ordered keys")
	assert.Same(t, t1, keys[0], "t1 must be first (registration order)")
	assert.Same(t, t2, keys[1], "t2 must be second")
}

func TestTopology_AttachTo_snapshotIsIndependentOfCallerAfterAttach(t *testing.T) {
	conn := newBareConnection(t)
	topo := &Topology{
		Queues: []Queue{{Name: "q1", Durable: true}},
	}
	topo.AttachTo(conn)

	// Mutate the original topology AFTER attach; snapshot must be unaffected.
	topo.Queues = append(topo.Queues, Queue{Name: "q2", Durable: true})

	conn.topoMu.RLock()
	snap := conn.topoSnaps[topo]
	conn.topoMu.RUnlock()

	assert.Len(t, snap.Queues, 1, "post-AttachTo mutations must not affect the stored snapshot")
}

func TestTopology_AttachTo_registersHookOnce(t *testing.T) {
	conn := newBareConnection(t)
	topo := &Topology{Queues: []Queue{{Name: "q1"}}}

	topo.AttachTo(conn)
	topo.AttachTo(conn) // second call with same pointer
	t2 := &Topology{Queues: []Queue{{Name: "q2"}}}
	t2.AttachTo(conn) // different pointer

	// The hook must be registered exactly once on each managed connection.
	mc := conn.pubConns[0]
	mc.hooksMu.Lock()
	hookCount := len(mc.hooks)
	mc.hooksMu.Unlock()
	assert.Equal(t, 1, hookCount, "topology redeclare hook must be registered exactly once per managed connection")
}
