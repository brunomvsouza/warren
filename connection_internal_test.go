package warren

// White-box tests for unexported Connection and managedConn methods.
// Package warren (not amqp_test) to access unexported fields and functions.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// selfSignedCertInternal is a local copy of the cert helper for the internal package.
func selfSignedCertInternal(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}
}

// newBareManaged creates a managedConn with minimal fields initialised, suitable
// for unit tests that don't need a live supervisor goroutine.
func newBareManaged(t *testing.T) *managedConn {
	t.Helper()
	mc := &managedConn{
		role: "publisher",
		idx:  0,
		name: "test-pub-0",
		done: make(chan struct{}),
	}
	mc.barrierCond = sync.NewCond(&mc.barrierMu)
	sharedOpts := connOptions{}
	applyConnDefaults(&sharedOpts)
	mc.opts = &sharedOpts
	return mc
}

// newBareConn creates a Connection with a single bare publisher managedConn,
// suitable for Health / ForceReconnect tests.
func newBareConn(t *testing.T) *Connection {
	t.Helper()
	c := &Connection{}
	c.pubConns = []*managedConn{newBareManaged(t)}
	return c
}

// newManagedWithFakeSupervisor creates a managedConn whose done channel closes
// when cancel() is called, simulating a supervisor that exits cleanly.
func newManagedWithFakeSupervisor(t *testing.T) *managedConn {
	t.Helper()
	mc := newBareManaged(t)
	ctx, cancel := context.WithCancel(context.Background())
	mc.cancel = cancel
	go func() {
		<-ctx.Done()
		close(mc.done)
	}()
	t.Cleanup(func() { cancel() })
	return mc
}

// newTestManaged creates a managedConn in the reconnecting state with defaults
// applied, ready for runBarrier tests.
func newTestManaged(t *testing.T) *managedConn {
	t.Helper()
	mc := newBareManaged(t)
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()
	return mc
}

// — computeAuthUser —————————————————————————————————————————————————————————

func TestComputeAuthUser_plain_returnsUsername(t *testing.T) {
	opts := &connOptions{
		saslMechanism: SASLPlain,
		username:      "alice",
	}
	assert.Equal(t, "alice", computeAuthUser(opts))
}

func TestComputeAuthUser_plain_emptyUsername(t *testing.T) {
	opts := &connOptions{saslMechanism: SASLPlain}
	assert.Equal(t, "", computeAuthUser(opts))
}

func TestApplyConnDefaults_extractsUsernameFromAddr(t *testing.T) {
	opts := &connOptions{
		saslMechanism: SASLPlain,
		addr:          "amqp://guest:secret@localhost/",
	}
	applyConnDefaults(opts)
	assert.Equal(t, "guest", computeAuthUser(opts),
		"username must be extracted from URL userinfo when WithAuth is not called")
	assert.Equal(t, "secret", opts.password,
		"password must be extracted from URL userinfo together with username")
}

func TestApplyConnDefaults_doesNotOverwriteExplicitUsername(t *testing.T) {
	opts := &connOptions{
		saslMechanism: SASLPlain,
		username:      "alice",
		addr:          "amqp://bob:pass@localhost/",
	}
	applyConnDefaults(opts)
	assert.Equal(t, "alice", computeAuthUser(opts),
		"explicit WithAuth username must not be overwritten by URL userinfo")
}

func TestComputeAuthUser_external_returnsCN(t *testing.T) {
	cert := selfSignedCertInternal(t, "svc-account")
	opts := &connOptions{
		saslMechanism: SASLExternal,
		tlsConfig:     &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	assert.Equal(t, "svc-account", computeAuthUser(opts))
}

func TestComputeAuthUser_external_noCert_returnsEmpty(t *testing.T) {
	opts := &connOptions{
		saslMechanism: SASLExternal,
		tlsConfig:     &tls.Config{},
	}
	assert.Equal(t, "", computeAuthUser(opts))
}

func TestComputeAuthUser_external_nilTLS_returnsEmpty(t *testing.T) {
	opts := &connOptions{saslMechanism: SASLExternal}
	assert.Equal(t, "", computeAuthUser(opts))
}

// — ForceReconnect ————————————————————————————————————————————————————————

func TestForceReconnect_afterClose_returnsErrAlreadyClosed(t *testing.T) {
	c := &Connection{}
	c.closed = true
	assert.ErrorIs(t, c.ForceReconnect(), ErrAlreadyClosed)
}

func TestForceReconnect_emptyPool_doesNotPanicOrError(t *testing.T) {
	c := &Connection{} // pubConns and conConns are nil
	err := c.ForceReconnect()
	assert.NoError(t, err, "ForceReconnect with empty pools should not error or panic")
}

// — Health: not connected —————————————————————————————————————————————————

func TestHealth_noPubConns_returnsErrNotConnected(t *testing.T) {
	c := &Connection{} // no pubConns
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrNotConnected)
}

func TestHealth_afterClose_returnsErrAlreadyClosed(t *testing.T) {
	c := &Connection{}
	c.closed = true
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestHealth_whileReconnecting_returnsErrReconnecting(t *testing.T) {
	c := newBareConn(t)
	mc := c.pubConns[0]
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrReconnecting)
}

func TestHealth_nilRaw_returnsErrNotConnected(t *testing.T) {
	c := newBareConn(t) // pubConns[0].raw == nil
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrNotConnected)
}

// — waitBarrier: early return paths ——————————————————————————————————————

func TestWaitBarrier_notReconnecting_notBlocked_returnsNil(t *testing.T) {
	mc := newBareManaged(t)
	err := mc.waitBarrier(context.Background())
	assert.NoError(t, err)
}

func TestWaitBarrier_degraded_returnsDegradedErr(t *testing.T) {
	sentinel := errors.New("topology gone")
	mc := newBareManaged(t)
	mc.mu.Lock()
	mc.degraded = true
	mc.degradedErr = sentinel
	mc.mu.Unlock()
	err := mc.waitBarrier(context.Background())
	assert.ErrorIs(t, err, sentinel)
}

func TestWaitBarrier_cancelledCtx_whileReconnecting_returnsErrReconnecting(t *testing.T) {
	defer goleak.VerifyNone(t)
	mc := newBareManaged(t)
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		mc.barrierCond.Broadcast()
	}()

	err := mc.waitBarrier(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReconnecting)
}

func TestWaitBarrier_cancelledCtx_whileBlocked_returnsErrConnectionBlocked(t *testing.T) {
	defer goleak.VerifyNone(t)
	mc := newBareManaged(t)
	mc.barrierMu.Lock()
	mc.blocked = true
	mc.barrierMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		mc.barrierCond.Broadcast()
	}()

	err := mc.waitBarrier(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionBlocked)
}

// — health: SPEC §6.1 degraded-state + ctx awareness ——————————————————————

func TestHealth_degraded_returnsTopologyRedeclareFailed(t *testing.T) {
	// SPEC §6.1: Health verifies the connection is "not in a degraded topology
	// state". A degraded conn must surface ErrTopologyRedeclareFailed, not open
	// a channel.
	mc := newBareManaged(t)
	mc.mu.Lock()
	mc.degraded = true
	mc.degradedErr = ErrTopologyRedeclareFailed
	mc.mu.Unlock()
	err := mc.health(context.Background())
	assert.ErrorIs(t, err, ErrTopologyRedeclareFailed)
}

func TestHealth_cancelledCtx_returnsCtxErr(t *testing.T) {
	// Health must honor a cancelled context instead of ignoring it.
	mc := newBareManaged(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := mc.health(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// — runBarrier: hook paths ————————————————————————————————————————————————

func TestRunBarrier_noHooks_clearsDegradedAndBroadcasts(t *testing.T) {
	mc := newTestManaged(t)
	mc.mu.Lock()
	mc.degraded = true
	mc.degradedErr = errors.New("old")
	mc.mu.Unlock()

	mc.runBarrier(context.Background())

	mc.mu.RLock()
	degraded := mc.degraded
	degradedErr := mc.degradedErr
	mc.mu.RUnlock()
	assert.False(t, degraded)
	assert.NoError(t, degradedErr)

	mc.barrierMu.Lock()
	reconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	assert.False(t, reconnecting)
}

func TestRunBarrier_hookError_entersDegradedState(t *testing.T) {
	hookErr := errors.New("exchange gone")
	mc := newTestManaged(t)
	mc.registerHook(func(_ context.Context) error { return hookErr })

	mc.runBarrier(context.Background())

	mc.mu.RLock()
	degraded := mc.degraded
	degradedErr := mc.degradedErr
	mc.mu.RUnlock()
	assert.True(t, degraded)
	assert.ErrorIs(t, degradedErr, ErrTopologyRedeclareFailed)
}

func TestRunBarrier_hookError_callsOnTopoDegradedOnce(t *testing.T) {
	defer goleak.VerifyNone(t)
	hookErr := errors.New("queue gone")
	called := 0
	var mu sync.Mutex
	mc := newTestManaged(t)
	mc.opts.onTopoDegraded = func(e error) {
		mu.Lock()
		called++
		mu.Unlock()
	}
	mc.registerHook(func(_ context.Context) error { return hookErr })

	// First barrier — should fire callback.
	mc.runBarrier(context.Background())
	mc.wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, called, "callback fires once on first degraded transition")
	mu.Unlock()

	// Re-enter barrier in degraded state — should NOT fire callback again.
	mc.barrierMu.Lock()
	mc.reconnecting = true
	mc.barrierMu.Unlock()
	mc.runBarrier(context.Background())
	mc.wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, called, "callback must not fire again on repeated degraded state")
	mu.Unlock()
}

func TestRunBarrier_degradedRecovery_firesOnReconnect(t *testing.T) {
	onReconnectFired := false
	mc := newTestManaged(t)
	mc.opts.onReconnect = func() { onReconnectFired = true }
	mc.mu.Lock()
	mc.degraded = true
	mc.degradedErr = errors.New("old")
	mc.mu.Unlock()

	// No hook errors → recovery path.
	mc.runBarrier(context.Background())

	assert.True(t, onReconnectFired, "onReconnect must fire after recovery from degraded")
	mc.mu.RLock()
	assert.False(t, mc.degraded)
	mc.mu.RUnlock()
}

// — Connection.Close ——————————————————————————————————————————————————————

func TestClose_alreadyClosed_returnsErrAlreadyClosed(t *testing.T) {
	c := &Connection{}
	c.closed = true
	assert.ErrorIs(t, c.Close(context.Background()), ErrAlreadyClosed)
}

func TestClose_emptyPools_completesImmediately(t *testing.T) {
	c := &Connection{}
	assert.NoError(t, c.Close(context.Background()))
	assert.True(t, c.closed)
}

func TestClose_withSupervisors_cancelsAndDrains(t *testing.T) {
	defer goleak.VerifyNone(t)
	mc1 := newManagedWithFakeSupervisor(t)
	mc2 := newManagedWithFakeSupervisor(t)
	c := &Connection{}
	c.pubConns = []*managedConn{mc1}
	c.conConns = []*managedConn{mc2}
	assert.NoError(t, c.Close(context.Background()))
	assert.True(t, c.closed)
}

func TestClose_idempotent_secondCallReturnsErrAlreadyClosed(t *testing.T) {
	defer goleak.VerifyNone(t)
	mc := newManagedWithFakeSupervisor(t)
	c := &Connection{}
	c.pubConns = []*managedConn{mc}
	require.NoError(t, c.Close(context.Background()))
	assert.ErrorIs(t, c.Close(context.Background()), ErrAlreadyClosed)
}

func TestClose_contextTimeout_returnsWrappedErr(t *testing.T) {
	// A managedConn whose done channel never closes simulates a stuck supervisor.
	mc := newBareManaged(t)
	// install a no-op cancel so Close doesn't panic on nil
	_, cancel := context.WithCancel(context.Background())
	mc.cancel = cancel
	defer cancel()
	// done is already created by newBareManaged but never closed

	c := &Connection{}
	c.pubConns = []*managedConn{mc}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer closeCancel()
	err := c.Close(closeCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

// — closeManagedConns —————————————————————————————————————————————————————

func TestCloseManagedConns_nilSlice_doesNotPanic(t *testing.T) {
	closeManagedConns(nil)
}

func TestCloseManagedConns_emptySlice_doesNotPanic(t *testing.T) {
	closeManagedConns([]*managedConn{})
}

func TestCloseManagedConns_withFakeSupervisors_drainsAll(t *testing.T) {
	defer goleak.VerifyNone(t)
	conns := make([]*managedConn, 3)
	for i := range conns {
		conns[i] = newManagedWithFakeSupervisor(t)
	}
	closeManagedConns(conns)
}
