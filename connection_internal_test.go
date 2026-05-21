package amqp

// White-box tests for unexported Connection methods.
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

// — ForceReconnect after Close —————————————————————————————————————————————

func TestForceReconnect_afterClose_returnsErrAlreadyClosed(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	c.closed = true
	assert.ErrorIs(t, c.ForceReconnect(), ErrAlreadyClosed)
}

func TestForceReconnect_nilRaw_doesNotPanic(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	// raw is nil, closed is false
	err := c.ForceReconnect()
	assert.NoError(t, err, "ForceReconnect with nil raw should not error or panic")
}

// — Health: not connected —————————————————————————————————————————————————

func TestHealth_notConnected_returnsErrNotConnected(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrNotConnected)
}

func TestHealth_afterClose_returnsErrAlreadyClosed(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	c.closed = true
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestHealth_whileReconnecting_returnsErrReconnecting(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	c.barrierMu.Lock()
	c.reconnecting = true
	c.barrierMu.Unlock()
	err := c.Health(context.Background())
	assert.ErrorIs(t, err, ErrReconnecting)
}

// — waitBarrier: early return paths ——————————————————————————————————————

func TestWaitBarrier_notReconnecting_notBlocked_returnsNil(t *testing.T) {
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	err := c.waitBarrier(context.Background())
	assert.NoError(t, err)
}

func TestWaitBarrier_degraded_returnsDegradedErr(t *testing.T) {
	sentinel := errors.New("topology gone")
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.mu.Lock()
	c.degraded = true
	c.degradedErr = sentinel
	c.mu.Unlock()
	err := c.waitBarrier(context.Background())
	assert.ErrorIs(t, err, sentinel)
}

func TestWaitBarrier_cancelledCtx_whileReconnecting_returnsErrReconnecting(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.barrierMu.Lock()
	c.reconnecting = true
	c.barrierMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel ctx after a short delay and broadcast to wake up the cond.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		c.barrierCond.Broadcast()
	}()

	err := c.waitBarrier(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReconnecting)
}

func TestWaitBarrier_cancelledCtx_whileBlocked_returnsErrConnectionBlocked(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.barrierMu.Lock()
	c.blocked = true
	c.barrierMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
		c.barrierCond.Broadcast()
	}()

	err := c.waitBarrier(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionBlocked)
}

// — runBarrier: hook paths ————————————————————————————————————————————————

func newTestConn(t *testing.T) *Connection {
	t.Helper()
	c := &Connection{}
	c.barrierCond = sync.NewCond(&c.barrierMu)
	c.done = make(chan struct{})
	applyConnDefaults(&c.opts)
	c.barrierMu.Lock()
	c.reconnecting = true
	c.barrierMu.Unlock()
	return c
}

func TestRunBarrier_noHooks_clearsDegradedAndBroadcasts(t *testing.T) {
	c := newTestConn(t)
	c.mu.Lock()
	c.degraded = true
	c.degradedErr = errors.New("old")
	c.mu.Unlock()

	c.runBarrier(context.Background())

	c.mu.RLock()
	degraded := c.degraded
	degradedErr := c.degradedErr
	c.mu.RUnlock()
	assert.False(t, degraded)
	assert.NoError(t, degradedErr)

	c.barrierMu.Lock()
	reconnecting := c.reconnecting
	c.barrierMu.Unlock()
	assert.False(t, reconnecting)
}

func TestRunBarrier_hookError_entersDegradedState(t *testing.T) {
	hookErr := errors.New("exchange gone")
	c := newTestConn(t)
	c.registerReconnectHook(func(_ context.Context) error { return hookErr })

	c.runBarrier(context.Background())

	c.mu.RLock()
	degraded := c.degraded
	degradedErr := c.degradedErr
	c.mu.RUnlock()
	assert.True(t, degraded)
	assert.ErrorIs(t, degradedErr, ErrTopologyRedeclareFailed)
}

func TestRunBarrier_hookError_callsOnTopoDegradedOnce(t *testing.T) {
	defer goleak.VerifyNone(t)
	hookErr := errors.New("queue gone")
	called := 0
	var mu sync.Mutex
	c := newTestConn(t)
	c.opts.onTopoDegraded = func(e error) {
		mu.Lock()
		called++
		mu.Unlock()
	}
	c.registerReconnectHook(func(_ context.Context) error { return hookErr })

	// First barrier — should fire callback.
	c.runBarrier(context.Background())
	c.wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, called, "callback fires once on first degraded transition")
	mu.Unlock()

	// Re-enter barrier in degraded state — should NOT fire callback again.
	c.barrierMu.Lock()
	c.reconnecting = true
	c.barrierMu.Unlock()
	c.runBarrier(context.Background())
	c.wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, called, "callback must not fire again on repeated degraded state")
	mu.Unlock()
}

func TestRunBarrier_degradedRecovery_firesOnReconnect(t *testing.T) {
	onReconnectFired := false
	c := newTestConn(t)
	c.opts.onReconnect = func() { onReconnectFired = true }
	c.mu.Lock()
	c.degraded = true
	c.degradedErr = errors.New("old")
	c.mu.Unlock()

	// No hook errors → recovery path.
	c.runBarrier(context.Background())

	assert.True(t, onReconnectFired, "onReconnect must fire after recovery from degraded")
	c.mu.RLock()
	assert.False(t, c.degraded)
	c.mu.RUnlock()
}
