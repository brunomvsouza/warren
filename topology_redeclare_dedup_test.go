package warren

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// countingDeclareChannel is a topologyChannel that counts queue/exchange/bind
// declares into a shared atomic counter, for de-amplification assertions (T62).
type countingDeclareChannel struct {
	declares *atomic.Int64
}

func (c countingDeclareChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp091.Table) error {
	c.declares.Add(1)
	return nil
}

func (c countingDeclareChannel) QueueDeclare(_ string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	c.declares.Add(1)
	return amqp091.Queue{}, nil
}

func (c countingDeclareChannel) QueueBind(_, _, _ string, _ bool, _ amqp091.Table) error {
	c.declares.Add(1)
	return nil
}

func (c countingDeclareChannel) ExchangeBind(_, _, _ string, _ bool, _ amqp091.Table) error {
	c.declares.Add(1)
	return nil
}

func (countingDeclareChannel) Close() error { return nil }

// TestTopologyRedeclare_deAmplified_oncePerRecovery proves that a pool-wide
// reconnect wave (every socket's barrier fires the topology hook at once)
// declares the broker-global topology ONCE, not N× (T62 / DS-09 / SRE-06).
func TestTopologyRedeclare_deAmplified_oncePerRecovery(t *testing.T) {
	defer goleak.VerifyNone(t)
	const poolSize = 8 // 4 publisher + 4 consumer, the verify scenario

	var declares atomic.Int64
	mcs := make([]*managedConn, poolSize)
	for i := range mcs {
		mc := newBareManaged(t)
		mc.chanFactory = func() (topologyChannel, error) {
			return countingDeclareChannel{declares: &declares}, nil
		}
		mcs[i] = mc
	}
	conn := &Connection{pubConns: mcs[:4], conConns: mcs[4:]}

	// A topology with exactly one queue → one declare per full redeclare.
	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	// Fire every connection's topology hook concurrently, as a broker restart does.
	var wg sync.WaitGroup
	for _, mc := range mcs {
		wg.Add(1)
		go func(mc *managedConn) {
			defer wg.Done()
			_ = conn.ensureTopologyRedeclare(context.Background(), mc)
		}(mc)
	}
	wg.Wait()

	assert.Equal(t, int64(1), declares.Load(),
		"a pool-wide reconnect wave must declare the topology once (topology size), not %d×", poolSize)
}

// TestTopologyRedeclare_independentReconnectsAfterWindow redeclares again once
// the coalesce window has elapsed (a genuinely new recovery event), so a later
// independent reconnect is not silently skipped.
func TestTopologyRedeclare_independentReconnectsAfterWindow(t *testing.T) {
	var declares atomic.Int64
	mc := newBareManaged(t)
	mc.chanFactory = func() (topologyChannel, error) {
		return countingDeclareChannel{declares: &declares}, nil
	}
	conn := &Connection{pubConns: []*managedConn{mc}}

	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	require.NoError(t, conn.ensureTopologyRedeclare(context.Background(), mc))
	require.Equal(t, int64(1), declares.Load())

	// Simulate the coalesce window having elapsed by clearing the last-declare stamp.
	conn.topoRedeclareMu.Lock()
	conn.topoLastDeclare = time.Time{}
	conn.topoRedeclareMu.Unlock()

	require.NoError(t, conn.ensureTopologyRedeclare(context.Background(), mc))
	assert.Equal(t, int64(2), declares.Load(),
		"a reconnect outside the coalesce window must redeclare again")
}

// TestTopologyRedeclare_hookErrorDoesNotAnchorWindow proves the coalesce window is
// anchored only on SUCCESS (topology.go: topoLastDeclare set iff err == nil). A
// failed redeclare must propagate its error, leave the window un-anchored, and
// clear the in-flight wait channel so the NEXT wave retries rather than coalescing
// onto a failure. The de-amplification footgun this guards against: anchoring on a
// transient broker error would suppress redeclare for the whole coalesce window,
// leaving the topology undeclared after a reconnect.
func TestTopologyRedeclare_hookErrorDoesNotAnchorWindow(t *testing.T) {
	defer goleak.VerifyNone(t)

	var declares atomic.Int64
	var failOpen atomic.Bool
	failOpen.Store(true)
	mc := newBareManaged(t)
	mc.chanFactory = func() (topologyChannel, error) {
		if failOpen.Load() {
			return nil, errors.New("redeclare boom")
		}
		return countingDeclareChannel{declares: &declares}, nil
	}
	conn := &Connection{pubConns: []*managedConn{mc}}

	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	err := conn.ensureTopologyRedeclare(context.Background(), mc)
	require.Error(t, err, "a failed redeclare must surface its error to the owning barrier")

	conn.topoRedeclareMu.Lock()
	assert.True(t, conn.topoLastDeclare.IsZero(),
		"a failed redeclare must NOT anchor the coalesce window")
	assert.Nil(t, conn.topoRedeclareWait,
		"the in-flight wait channel must be cleared (closed) after a failed redeclare so waiters unblock")
	conn.topoRedeclareMu.Unlock()

	// The next wave must retry — not skip onto the failure — and now succeeds.
	failOpen.Store(false)
	require.NoError(t, conn.ensureTopologyRedeclare(context.Background(), mc))
	assert.Equal(t, int64(1), declares.Load(),
		"the wave after a failed redeclare must actually re-declare the topology")
}

// TestTopologyRedeclare_ctxCancelWhileWaitingForOwner covers the waiter's
// `case <-ctx.Done()` arm (topology.go ~250): when one barrier owns the redeclare
// and a second barrier is parked waiting for it, cancelling the waiter's ctx (e.g.
// its own reconnect barrier was capped, T63) must return ctx.Err() promptly without
// redeclaring — the owner's single declare is the only one that runs.
func TestTopologyRedeclare_ctxCancelWhileWaitingForOwner(t *testing.T) {
	defer goleak.VerifyNone(t)

	var declares atomic.Int64
	release := make(chan struct{})
	ownerInside := make(chan struct{})
	var once sync.Once
	mc := newBareManaged(t)
	mc.chanFactory = func() (topologyChannel, error) {
		once.Do(func() { close(ownerInside) }) // signal: owner is inside runTopologyRedeclare
		<-release                              // block the owner so topoRedeclareWait stays set
		return countingDeclareChannel{declares: &declares}, nil
	}
	conn := &Connection{pubConns: []*managedConn{mc}}

	topo := &Topology{Queues: []Queue{{Name: "orders"}}}
	require.NoError(t, topo.AttachTo(conn))

	ownerErr := make(chan error, 1)
	go func() { ownerErr <- conn.ensureTopologyRedeclare(context.Background(), mc) }()
	<-ownerInside // owner now holds topoRedeclareWait, blocked mid-declare

	// A second barrier enters and parks on the owner's wait channel; cancel its ctx.
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() { waiterErr <- conn.ensureTopologyRedeclare(waiterCtx, mc) }()
	cancelWaiter()

	require.ErrorIs(t, <-waiterErr, context.Canceled,
		"a waiter whose ctx is cancelled while the owner redeclares must return ctx.Err()")

	// Release the owner; it completes its single declare. The cancelled waiter added none.
	close(release)
	require.NoError(t, <-ownerErr)
	assert.Equal(t, int64(1), declares.Load(),
		"only the owner declares; the cancelled waiter must not contribute a declare")
}
