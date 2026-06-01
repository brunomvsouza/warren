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
	"net"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
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
	errCh := make(chan error, 1)
	go func() { errCh <- mc.waitBarrier(ctx) }()

	cancel()
	err := nudgeBarrierUntilDone(mc, errCh)
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
	errCh := make(chan error, 1)
	go func() { errCh <- mc.waitBarrier(ctx) }()

	cancel()
	err := nudgeBarrierUntilDone(mc, errCh)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionBlocked)
}

// nudgeBarrierUntilDone deterministically replaces a fixed time.Sleep gate.
// waitBarrier parks in barrierCond.Wait(); its own ctx-watcher fires exactly one
// Broadcast on cancellation, which is lost if it races ahead of the park. Rather
// than sleeping long enough to "probably" be parked, broadcast repeatedly (yielding
// between attempts) until waitBarrier observes the already-cancelled ctx and returns
// — terminating as soon as the result lands on errCh, with no timing assumption.
func nudgeBarrierUntilDone(mc *managedConn, errCh <-chan error) error {
	for {
		mc.barrierCond.Broadcast()
		select {
		case err := <-errCh:
			return err
		default:
			runtime.Gosched()
		}
	}
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

// — health: broker-error classification (LATER-86, SPEC §6.3) ——————————————

// healthChan is a minimal topologyChannel for health() unit tests: it records
// whether Close was called and returns a configurable Close error. Injected via
// managedConn.chanFactory so the probe's open/close round-trip — otherwise
// integration-only — is exercised as a unit test.
type healthChan struct {
	closeErr error
	closed   bool
}

func (h *healthChan) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp091.Table) error {
	return nil
}

func (h *healthChan) QueueDeclare(name string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{Name: name}, nil
}

func (h *healthChan) QueueBind(_, _, _ string, _ bool, _ amqp091.Table) error    { return nil }
func (h *healthChan) ExchangeBind(_, _, _ string, _ bool, _ amqp091.Table) error { return nil }

func (h *healthChan) Close() error {
	h.closed = true
	return h.closeErr
}

func TestHealth_channelOpenError_wrappedAsReplyCodeSentinel(t *testing.T) {
	// LATER-86: a broker error from opening the health-probe channel must be
	// classified through wrapAMQPError, so callers can errors.Is it against the
	// reply-code sentinels (SPEC §6.3) — not receive a bare *amqp091.Error that
	// matches no sentinel. Every other T53 broker path already wraps; Health must
	// be consistent.
	mc := newBareManaged(t)
	mc.raw = &amqp091.Connection{} // non-nil so the nil-raw guard passes; never dereferenced (chanFactory wins)
	mc.chanFactory = func() (topologyChannel, error) {
		return nil, &amqp091.Error{Code: 403, Reason: "ACCESS_REFUSED - operation not permitted"}
	}

	err := mc.health(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAccessRefused, "a 403 channel-open error must classify as ErrAccessRefused")
	assert.Contains(t, err.Error(), "warren: health:", "the wrap must carry the operation prefix")
}

func TestHealth_channelCloseError_wrappedAsReplyCodeSentinel(t *testing.T) {
	// LATER-86: an error from closing the probe channel must likewise route through
	// wrapAMQPError, not surface bare.
	mc := newBareManaged(t)
	mc.raw = &amqp091.Connection{}
	ch := &healthChan{closeErr: &amqp091.Error{Code: 406, Reason: "PRECONDITION_FAILED - stale"}}
	mc.chanFactory = func() (topologyChannel, error) { return ch, nil }

	err := mc.health(context.Background())
	require.Error(t, err)
	assert.True(t, ch.closed, "Close must have been attempted")
	assert.ErrorIs(t, err, ErrPreconditionFailed, "a 406 close error must classify as ErrPreconditionFailed")
	assert.Contains(t, err.Error(), "warren: health:")
}

func TestHealth_success_returnsNilAfterOpenAndClose(t *testing.T) {
	// The happy path opens then closes a probe channel and returns nil. The real
	// open is integration-only; the chanFactory seam exercises the success path as
	// a unit test and asserts the probe channel is always closed.
	mc := newBareManaged(t)
	mc.raw = &amqp091.Connection{}
	ch := &healthChan{}
	mc.chanFactory = func() (topologyChannel, error) { return ch, nil }

	err := mc.health(context.Background())
	require.NoError(t, err)
	assert.True(t, ch.closed, "a successful health probe must close the channel it opened")
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

// — round-robin address failover (T33) ————————————————————————————————————

func TestDialAddrs_prefersAddrsOverAddr(t *testing.T) {
	opts := &connOptions{addr: "amqp://primary/", addrs: []string{"amqp://x/", "amqp://y/"}}
	assert.Equal(t, []string{"amqp://x/", "amqp://y/"}, opts.dialAddrs())
}

func TestDialAddrs_singleAddrWhenNoAddrs(t *testing.T) {
	opts := &connOptions{addr: "amqp://primary/"}
	assert.Equal(t, []string{"amqp://primary/"}, opts.dialAddrs())
}

func TestNextDialAddr_singleAddr_alwaysReturnsSame(t *testing.T) {
	mc := &managedConn{opts: &connOptions{addr: "amqp://a/"}}
	for range 3 {
		assert.Equal(t, "amqp://a/", mc.nextDialAddr())
	}
}

// Acceptance (T66): with WithAddrs each socket walks a per-connection *shuffled*
// permutation of the list — not the raw input order. Over len(addrs) dials every
// address is returned exactly once (a full cycle), in the order fixed by this
// socket's shuffleSeed.
func TestNextDialAddr_multiAddr_walksFullShuffledPermutation(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/"}
	mc := &managedConn{shuffleSeed: 42, opts: &connOptions{addrs: addrs}}

	want := shuffledAddrs(addrs, 42)
	got := []string{mc.nextDialAddr(), mc.nextDialAddr(), mc.nextDialAddr()}
	assert.Equal(t, want, got, "walks the seeded permutation in order")
	assert.ElementsMatch(t, addrs, got, "every address appears exactly once over one cycle")
}

// Acceptance (T66): on reconnect the cursor advances through the shuffled order
// and wraps, so each reconnect rotates to a DIFFERENT address until the cycle
// repeats.
func TestNextDialAddr_multiAddr_reconnectRotatesAndWraps(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/"}
	mc := &managedConn{shuffleSeed: 7, opts: &connOptions{addrs: addrs}}

	first := mc.nextDialAddr()
	second := mc.nextDialAddr()
	assert.NotEqual(t, first, second, "reconnect rotates to a different address")
	assert.Equal(t, first, mc.nextDialAddr(), "wraps back to the head after one cycle")
	assert.Equal(t, second, mc.nextDialAddr())
}

func TestNextDialAddr_emptyAddrs_fallsBackToAddr(t *testing.T) {
	mc := &managedConn{opts: &connOptions{addr: "amqp://only/"}}
	assert.Equal(t, "amqp://only/", mc.nextDialAddr())
	assert.Equal(t, "amqp://only/", mc.nextDialAddr())
}

// Regression (review Nit): the round-robin cursor must stay within
// [0, len(addrs)) instead of incrementing without bound, so it can never
// overflow int and wrap to a negative index after enough reconnects.
func TestNextDialAddr_cursorStaysBounded(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/"}
	mc := &managedConn{opts: &connOptions{addrs: addrs}}
	for range 10 {
		_ = mc.nextDialAddr()
		assert.GreaterOrEqual(t, mc.dialCursor, 0)
		assert.Less(t, mc.dialCursor, len(addrs))
	}
}

// — T66 per-connection address shuffle —————————————————————————————————————

// shuffledAddrs is a deterministic permutation: the same seed always yields the
// same order, the result contains exactly the input addresses, and the caller's
// slice (the shared, read-only opts.addrs) is never mutated.
func TestShuffledAddrs_deterministicPermutation(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/", "amqp://d/"}
	input := append([]string(nil), addrs...)

	first := shuffledAddrs(input, 99)
	second := shuffledAddrs(input, 99)
	assert.Equal(t, first, second, "same seed → identical permutation")
	assert.ElementsMatch(t, addrs, first, "result is a permutation of the input")
	assert.Equal(t, addrs, input, "input slice must not be mutated")
	assert.NotSame(t, &input[0], &first[0], "returns a copy, not the input backing array")
}

// A single or empty list has no permutation to make; shuffledAddrs returns it
// (as a copy) unchanged.
func TestShuffledAddrs_singleOrEmptyUnchanged(t *testing.T) {
	assert.Equal(t, []string{"amqp://only/"}, shuffledAddrs([]string{"amqp://only/"}, 123))
	assert.Empty(t, shuffledAddrs(nil, 123))
}

// Distinct seeds spread the first dialled address across the list — the
// anti-stampede property: addrs[0] does not monopolise the lead. Deterministic
// (PCG over fixed seeds 0..59), so this either passes or fails reproducibly.
func TestShuffledAddrs_distinctSeedsSpreadFirstAddr(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/"}
	firsts := map[string]int{}
	for seed := int64(0); seed < 60; seed++ {
		firsts[shuffledAddrs(addrs, seed)[0]]++
	}
	assert.GreaterOrEqual(t, len(firsts), 2,
		"distinct seeds must lead with ≥2 different addresses (no addrs[0] monopoly)")
	assert.Less(t, firsts["amqp://a/"], 60, "addrs[0] must not lead for every seed")
}

// perConnSeed is deterministic and decorrelates sockets: publisher-0 and
// consumer-0 — and socket 0 vs 1 — get distinct seeds, so they shuffle to
// different orders even when they share the process-level base.
func TestPerConnSeed_deterministicAndDistinct(t *testing.T) {
	assert.Equal(t, perConnSeed(1000, "publisher", 0), perConnSeed(1000, "publisher", 0),
		"same (base, role, idx) → same seed")

	seeds := map[int64]struct{}{}
	for _, role := range []string{"publisher", "consumer"} {
		for idx := range 4 {
			seeds[perConnSeed(1000, role, idx)] = struct{}{}
		}
	}
	assert.Len(t, seeds, 8, "every (role, idx) pair must get a distinct seed")
}

// TestApplyConnDefaults_seedUnset_fillsNonZeroBase guards the addrShuffleSeed
// defaulting in applyConnDefaults: an unset (0) seed must be filled with a
// non-zero per-process base. The "0 means unset" sentinel is load-bearing — the
// WithAddrShuffleSeedForTest seam and the per-process randomization both depend
// on it — yet a regression dropping the `| 1` (or the whole block) could leave
// the seed at 0, silently re-introducing the addr[0] stampede T66 prevents with
// no other test failing. Two independent fills must also differ, pinning the
// per-process-random intent (not a fixed constant). Collision is ~1/2^64.
func TestApplyConnDefaults_seedUnset_fillsNonZeroBase(t *testing.T) {
	var a, b connOptions // zero value → addrShuffleSeed == 0 (unset)
	applyConnDefaults(&a)
	applyConnDefaults(&b)

	assert.NotZero(t, a.addrShuffleSeed, "an unset seed must default to a non-zero base")
	assert.NotZero(t, b.addrShuffleSeed, "an unset seed must default to a non-zero base")
	assert.NotEqual(t, a.addrShuffleSeed, b.addrShuffleSeed,
		"the default base must be per-process random, not a fixed constant")
}

// TestApplyConnDefaults_seedExplicit_preserved guards the other half of the
// sentinel: a caller-set non-zero seed survives applyConnDefaults untouched.
// The cluster lane's determinism (and the WithAddrShuffleSeedForTest seam's
// "a non-zero value is preserved as-is" contract) silently rely on this; assert
// it on the default lane too.
func TestApplyConnDefaults_seedExplicit_preserved(t *testing.T) {
	const want int64 = 0x5EED_F00D
	opts := connOptions{addrShuffleSeed: want}
	applyConnDefaults(&opts)
	assert.Equal(t, want, opts.addrShuffleSeed, "an explicitly-set non-zero seed must be preserved")
}

// dialOrder builds the shuffled permutation once and caches it (stable across
// calls), matching shuffledAddrs for the socket's seed.
func TestDialOrder_cachedStablePermutation(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/"}
	mc := &managedConn{shuffleSeed: 55, opts: &connOptions{addrs: addrs}}

	first := mc.dialOrder()
	assert.Equal(t, shuffledAddrs(addrs, 55), first)
	assert.Equal(t, first, mc.dialOrder(), "permutation is cached and stable")
}

// Headline T66 acceptance (mirrors openPool's per-socket seeding): N sockets
// over a 3-node list start their dials spread across the nodes, not all on
// addrs[0]. Deterministic given a fixed process-level base.
func TestPerConnSeeding_distributesInitialDialNoAddr0Stampede(t *testing.T) {
	addrs := []string{"amqp://a/", "amqp://b/", "amqp://c/"}
	const base int64 = 0x1234_5678

	firsts := map[string]int{}
	for _, role := range []string{"publisher", "consumer"} {
		for idx := range 3 { // 3 publisher + 3 consumer sockets
			seed := perConnSeed(base, role, idx)
			firsts[shuffledAddrs(addrs, seed)[0]]++
		}
	}
	assert.GreaterOrEqual(t, len(firsts), 2,
		"initial dials must spread across ≥2 nodes — no addr[0] stampede")
	assert.Less(t, firsts["amqp://a/"], 6,
		"addrs[0] must not capture every socket's initial dial")
}

// TestOpenPool_publisherSocketWalksItsSeededPermutation guards the seed WIRING at
// the openPool call site (shuffleSeed: perConnSeed(opts.addrShuffleSeed, role, i)).
// The tests above prove shuffledAddrs/perConnSeed in isolation; this proves openPool
// actually plugs a socket's per-(role, idx) seed into the order it dials — on the
// DEFAULT lane, with no broker. A stub dialer that refuses every address lets the
// single publisher socket walk its full shuffled order before Dial gives up; we
// record that order and assert it is exactly the host:ports of
// shuffledAddrs(addrs, perConnSeed(base, "publisher", 0)).
//
// Without this, a regression in that one wiring line would surface only on the
// integration/cluster lanes (openPool needs a live dial to succeed). It is
// deterministic: the pinned base fixes the permutation, JitterNone + finite Retries
// fix the walk, and WithPublisherConnections(1) makes the publisher pool fail first
// so Dial returns before the consumer pool opens — the recorded order is the one
// publisher socket's, dialed serially in the calling goroutine.
func TestOpenPool_publisherSocketWalksItsSeededPermutation(t *testing.T) {
	defer goleak.VerifyNone(t)

	const base int64 = 0x0BADF00D
	addrs := []string{
		"amqp://guest:guest@127.0.0.1:5001/",
		"amqp://guest:guest@127.0.0.1:5002/",
		"amqp://guest:guest@127.0.0.1:5003/",
	}

	var mu sync.Mutex
	var order []string

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := Dial(ctx,
		WithAddrs(addrs),
		// Pin the process-level base inline. The cluster lane uses the exported
		// WithAddrShuffleSeedForTest seam; in-package we set the field directly so the
		// seam need not leave the cluster tag.
		func(o *connOptions) { o.addrShuffleSeed = base },
		WithPublisherConnections(1),
		WithConsumerConnections(1),
		WithReconnectBackoff(RetryPolicy{
			Min: time.Millisecond, Max: time.Millisecond, Retries: 9, Jitter: JitterNone,
		}),
		WithDialer(func(_, addr string) (net.Conn, error) {
			mu.Lock()
			order = append(order, addr)
			mu.Unlock()
			return nil, errors.New("connection refused")
		}),
	)
	require.Error(t, err, "every address refuses, so Dial must fail")

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(order), len(addrs),
		"the publisher socket must walk at least one full cycle of its dial order")

	want := shuffledAddrs(addrs, perConnSeed(base, "publisher", 0))
	wantHostPorts := make([]string, len(want))
	for i, u := range want {
		pu, perr := url.Parse(u)
		require.NoError(t, perr)
		wantHostPorts[i] = pu.Host
	}
	assert.Equal(t, wantHostPorts, order[:len(addrs)],
		"openPool must seed the publisher socket so it dials shuffledAddrs(addrs, perConnSeed(base, \"publisher\", 0))")
}

// TestConnectOnce_ctxCancelled_returnsCtxErr covers the ctx-cancellation fork of
// connectOnce (which wraps reconnectRaw): when no dial succeeds and the context is
// done, the caller must surface ctx.Err() rather than a redacted dial error. A dialer
// that always fails forces the backoff loop to keep retrying until the cancelled ctx
// ends it; Retries caps the worst case so the test can never spin if ctx were ignored.
func TestConnectOnce_ctxCancelled_returnsCtxErr(t *testing.T) {
	defer goleak.VerifyNone(t)

	mc := newBareManaged(t)
	mc.opts.addr = "amqp://h:5672/"
	mc.opts.dialer = func(_, _ string) (net.Conn, error) {
		return nil, errors.New("no broker")
	}
	mc.opts.reconnectBackoff = RetryPolicy{Min: time.Millisecond, Max: time.Millisecond, Retries: 50}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before any dial can succeed

	err := mc.connectOnce(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"a ctx cancelled before any successful dial must surface ctx.Err(), not a redacted dial error")
}

// TestOpenPool_allFailInRole_failsFastNoLeak covers the failure boundary of the
// T67 partial-pool-connect policy: the policy tolerates SOME sockets failing at
// boot (≥1 live per role → degraded success), but a role where EVERY socket fails
// must fail-fast — a Connection with zero live sockets for a role cannot serve it.
// The integration test covers the ≥1-survives success path; this unit test pins the
// connected==0 edge and asserts no supervisor goroutine is spawned on the fail-fast
// path (goleak), so a regression loosening the per-role floor would not slip by.
func TestOpenPool_allFailInRole_failsFastNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	opts := &connOptions{}
	applyConnDefaults(opts)
	opts.addr = "amqp://h:5672/"
	opts.dialer = func(_, _ string) (net.Conn, error) { return nil, errors.New("no broker") }
	opts.reconnectBackoff = RetryPolicy{Min: time.Millisecond, Max: time.Millisecond, Retries: 1}

	pool, err := openPool(context.Background(), "publisher", 2, opts)
	require.Error(t, err, "a role with zero live sockets must fail Dial, not boot at reduced capacity")
	assert.Nil(t, pool)
	assert.Contains(t, err.Error(), "no connection could be established")
}
