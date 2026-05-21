package amqp

// White-box tests for unexported Connection and managedConn methods.
// Package amqp (not amqp_test) to access unexported fields and functions.

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
