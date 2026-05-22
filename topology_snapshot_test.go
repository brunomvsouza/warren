package warren

// White-box tests for Topology.AttachTo snapshot semantics and runTopologyRedeclare.
// Package warren (not warren_test) to access unexported fields.

import (
	"context"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
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
	require.NoError(t, topo.AttachTo(conn))

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
	require.NoError(t, topo.AttachTo(conn))

	// Mutate topology, re-attach with the same pointer.
	topo.Queues[0].Name = "mutated"
	require.NoError(t, topo.AttachTo(conn))

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

	require.NoError(t, t1.AttachTo(conn))
	require.NoError(t, t2.AttachTo(conn))

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
	require.NoError(t, topo.AttachTo(conn))

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

	require.NoError(t, topo.AttachTo(conn))
	require.NoError(t, topo.AttachTo(conn)) // second call with same pointer
	t2 := &Topology{Queues: []Queue{{Name: "q2"}}}
	require.NoError(t, t2.AttachTo(conn)) // different pointer

	// The hook must be registered exactly once on each managed connection.
	mc := conn.pubConns[0]
	mc.hooksMu.Lock()
	hookCount := len(mc.hooks)
	mc.hooksMu.Unlock()
	assert.Equal(t, 1, hookCount, "topology redeclare hook must be registered exactly once per managed connection")
}

func TestTopology_AttachTo_registersHookOnBothPools(t *testing.T) {
	pub := newBareManaged(t)
	con := newBareManaged(t)
	conn := &Connection{
		pubConns: []*managedConn{pub},
		conConns: []*managedConn{con},
	}
	topo := &Topology{Queues: []Queue{{Name: "q"}}}
	require.NoError(t, topo.AttachTo(conn))

	pub.hooksMu.Lock()
	pubHooks := len(pub.hooks)
	pub.hooksMu.Unlock()
	con.hooksMu.Lock()
	conHooks := len(con.hooks)
	con.hooksMu.Unlock()

	assert.Equal(t, 1, pubHooks, "hook must be registered on publisher connection")
	assert.Equal(t, 1, conHooks, "hook must be registered on consumer connection")
}

func TestTopology_AttachTo_returnsErrorOnInvalidTopology(t *testing.T) {
	conn := newBareConnection(t)
	topo := &Topology{
		Queues: []Queue{{Name: ""}}, // empty name — invalid
	}
	err := topo.AttachTo(conn)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// — runTopologyRedeclare unit tests ———————————————————————————————————————

func TestRunTopologyRedeclare_openChannelFailure_returnsError(t *testing.T) {
	// newBareManaged has raw == nil, so openChannel() returns ErrNotConnected.
	mc := newBareManaged(t)
	conn := &Connection{
		pubConns: []*managedConn{mc},
		conConns: []*managedConn{},
	}
	topo := &Topology{Queues: []Queue{{Name: "q"}}}
	require.NoError(t, topo.AttachTo(conn))

	// runTopologyRedeclare should fail because openChannel returns ErrNotConnected.
	err := conn.runTopologyRedeclare(context.Background(), mc)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotConnected)
}

func TestRunTopologyRedeclare_secondSnapshotFails_stopsOnFirstError(t *testing.T) {
	// Two topologies registered; second declareOnChannel returns 406.
	// Verify the redeclare stops after the first failure.
	mc := newBareManaged(t)
	conn := &Connection{
		pubConns: []*managedConn{mc},
		conConns: []*managedConn{},
	}

	t1 := &Topology{Queues: []Queue{{Name: "q1"}}}
	t2 := &Topology{Queues: []Queue{{Name: "q2"}}}
	require.NoError(t, t1.AttachTo(conn))
	require.NoError(t, t2.AttachTo(conn))

	// Inject a channel factory: first call succeeds, second returns a 406 error.
	callCount := 0
	amqpErr := &amqp091.Error{Code: 406, Reason: "PRECONDITION_FAILED"}
	mc.chanFactory = func() (topologyChannel, error) {
		callCount++
		if callCount == 1 {
			return &declareRecorder{}, nil
		}
		return &declareRecorder{err: amqpErr}, nil
	}

	err := conn.runTopologyRedeclare(context.Background(), mc)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTopologyMismatch, "second snapshot failure must wrap ErrTopologyMismatch")
	assert.Equal(t, 2, callCount, "must have attempted exactly two channels (one per topology)")
}
