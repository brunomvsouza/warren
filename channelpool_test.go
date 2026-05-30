package warren

// White-box tests for the channelPool (T08).
// Package warren (not amqp_test) to access the unexported type.

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

// — saturation ————————————————————————————————————————————————————————————

// TestChannelPool_Saturated_ReturnsExhausted is the explicit acceptance test
// from T08: pool size 1, two concurrent Acquire calls, second with a 50 ms
// ctx — must return ErrChannelPoolExhausted.
func TestChannelPool_Saturated_ReturnsExhausted(t *testing.T) {
	defer goleak.VerifyNone(t)

	// openFn signals when the token is held, then blocks until released.
	tokenHeld := make(chan struct{})
	openRelease := make(chan struct{})
	openFn := func() (amqpChannel, error) {
		close(tokenHeld) // signal: first goroutine has the token
		<-openRelease    // block until the test tells us to proceed
		return nil, errors.New("test interruption")
	}

	p := newChannelPool(1, openFn)

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _, _ = p.Acquire(context.Background())
	})

	// Wait until the first goroutine has consumed the token.
	<-tokenHeld

	// Second acquire with a short deadline must time out.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := p.Acquire(ctx)
	assert.ErrorIs(t, err, ErrChannelPoolExhausted)

	// Unblock the first goroutine and wait for clean-up.
	close(openRelease)
	wg.Wait()
}

// — acquire/release —————————————————————————————————————————————————————

func TestChannelPool_AcquireRelease_IdleChannelReused(t *testing.T) {
	defer goleak.VerifyNone(t)

	openCount := 0
	var mu sync.Mutex

	openFn := func() (amqpChannel, error) {
		mu.Lock()
		defer mu.Unlock()
		openCount++
		return &fakeChannel{}, nil
	}

	p := newChannelPool(2, openFn)

	// First acquire opens a new channel.
	ch1, rel1, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ch1)
	rel1()

	// Second acquire should reuse the idle channel.
	ch2, rel2, err := p.Acquire(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ch2)
	rel2()

	mu.Lock()
	total := openCount
	mu.Unlock()

	// Only one channel should have been opened (reuse on second acquire).
	assert.Equal(t, 1, total, "expected channel reuse; got %d opens", total)
}

// TestChannelPool_ClosedDuringUse_DiscardedOnRelease asserts that a channel
// closed by the broker *while in use* is not returned to the idle list.
func TestChannelPool_ClosedDuringUse_DiscardedOnRelease(t *testing.T) {
	defer goleak.VerifyNone(t)

	openCount := 0
	var mu sync.Mutex

	openFn := func() (amqpChannel, error) {
		mu.Lock()
		defer mu.Unlock()
		openCount++
		return &fakeChannel{}, nil
	}

	p := newChannelPool(2, openFn)

	// Acquire, simulate broker close while in use, then release.
	ch, rel, err := p.Acquire(context.Background())
	require.NoError(t, err)
	fc, ok := ch.(*fakeChannel)
	require.True(t, ok)
	fc.simulateClose()
	rel()

	// Next acquire must open a fresh channel, not reuse the closed one.
	_, rel2, err := p.Acquire(context.Background())
	require.NoError(t, err)
	rel2()

	mu.Lock()
	total := openCount
	mu.Unlock()

	assert.Equal(t, 2, total, "expected closed channel to be discarded and a new one opened")
}

// TestChannelPool_ClosedWhileIdle_DiscardedOnAcquire asserts that a channel
// closed by the broker *while idle in the free list* is discarded when the
// next Acquire attempts to reuse it (exercises the getOrOpen stale-drain loop).
func TestChannelPool_ClosedWhileIdle_DiscardedOnAcquire(t *testing.T) {
	defer goleak.VerifyNone(t)

	openCount := 0
	var mu sync.Mutex
	var lastFake *fakeChannel

	openFn := func() (amqpChannel, error) {
		mu.Lock()
		defer mu.Unlock()
		openCount++
		fc := &fakeChannel{}
		lastFake = fc
		return fc, nil
	}

	p := newChannelPool(2, openFn)

	// Acquire and release back to the idle list.
	_, rel1, err := p.Acquire(context.Background())
	require.NoError(t, err)
	rel1() // channel now sits in the free list

	// Simulate broker closing the channel while it is idle.
	mu.Lock()
	fc := lastFake
	mu.Unlock()
	require.NotNil(t, fc)
	fc.simulateClose()

	// Next Acquire must detect the stale entry in getOrOpen and open a new one.
	_, rel2, err := p.Acquire(context.Background())
	require.NoError(t, err)
	rel2()

	mu.Lock()
	total := openCount
	mu.Unlock()

	assert.Equal(t, 2, total, "expected stale idle channel to be discarded and a new one opened")
}

// TestChannelPool_OpenFnError_TokenReturned asserts that when openFn fails
// the semaphore token is returned so the pool remains usable.
func TestChannelPool_OpenFnError_TokenReturned(t *testing.T) {
	defer goleak.VerifyNone(t)

	calls := 0
	openFn := func() (amqpChannel, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("transient broker error")
		}
		return &fakeChannel{}, nil
	}

	p := newChannelPool(1, openFn)

	// First acquire fails — token must be returned.
	_, _, err := p.Acquire(context.Background())
	require.Error(t, err)

	// Second acquire must succeed, proving the token was returned.
	_, rel, err := p.Acquire(context.Background())
	require.NoError(t, err)
	rel()
}

// — race ——————————————————————————————————————————————————————————————————

// TestChannelPool_ConcurrentAcquireRelease_Race stresses the pool with many
// goroutines to surface data races when run with go test -race.
func TestChannelPool_ConcurrentAcquireRelease_Race(t *testing.T) {
	defer goleak.VerifyNone(t)

	openFn := func() (amqpChannel, error) {
		return &fakeChannel{}, nil
	}

	p := newChannelPool(4, openFn)

	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, rel, err := p.Acquire(ctx)
			if err == nil {
				rel()
			}
		})
	}
	wg.Wait()
}

// — IsTransient classification ————————————————————————————————————————————

func TestErrChannelPoolExhausted_IsTransient(t *testing.T) {
	assert.True(t, IsTransient(ErrChannelPoolExhausted))
}

// — Drain ————————————————————————————————————————————————————————————————

// TestChannelPool_Drain_DiscardsIdleChannels asserts that Drain (called by the
// reconnect supervisor after a TCP reconnect) closes every idle channel and
// empties the free list, while leaving the pool usable for fresh acquires.
func TestChannelPool_Drain_DiscardsIdleChannels(t *testing.T) {
	defer goleak.VerifyNone(t)

	openFn := func() (amqpChannel, error) { return &fakeChannel{}, nil }
	p := newChannelPool(3, openFn)

	ctx := context.Background()
	ch1, rel1, err := p.Acquire(ctx)
	require.NoError(t, err)
	ch2, rel2, err := p.Acquire(ctx)
	require.NoError(t, err)
	rel1()
	rel2()
	require.Len(t, p.free, 2, "two idle channels expected before drain")

	p.Drain()

	assert.Empty(t, p.free, "free list must be empty after Drain")
	assert.Equal(t, int32(1), ch1.(*fakeChannel).closeCount(), "idle channel 1 must be closed by Drain")
	assert.Equal(t, int32(1), ch2.(*fakeChannel).closeCount(), "idle channel 2 must be closed by Drain")

	// Drain on an already-empty free list is a no-op (covers the default branch).
	p.Drain()
	assert.Empty(t, p.free)

	// Pool remains usable: a fresh Acquire opens a new channel.
	ch3, rel3, err := p.Acquire(ctx)
	require.NoError(t, err)
	require.NotNil(t, ch3)
	rel3()
}

// — release safety valve ——————————————————————————————————————————————————

// TestChannelPool_Release_FreeListFull_DiscardsEntry exercises the safety-valve
// default branch in release: when the idle list is already at capacity, the
// released entry is closed and discarded rather than blocking. This cannot
// happen under correct usage (free and tokens share a capacity), so it is
// constructed white-box.
func TestChannelPool_Release_FreeListFull_DiscardsEntry(t *testing.T) {
	defer goleak.VerifyNone(t)

	openFn := func() (amqpChannel, error) { return &fakeChannel{}, nil }
	p := newChannelPool(1, openFn)

	// Fill the free list (cap 1) so the next release cannot enqueue.
	p.free <- pooledEntry{ch: &fakeChannel{}, closeCh: make(chan *amqp091.Error, 1)}

	// Drain one token so release's deferred token return does not block (the
	// semaphore is also cap 1 and currently full).
	<-p.tokens

	overflow := &fakeChannel{}
	p.release(pooledEntry{ch: overflow, closeCh: make(chan *amqp091.Error, 1)})

	assert.Equal(t, int32(1), overflow.closeCount(), "overflow entry must be closed when the free list is full")
	assert.Len(t, p.tokens, 1, "the semaphore token must still be returned")
}

// — fakeChannel helper ——————————————————————————————————————————————————

// fakeChannel is a test double for amqpChannel that allows simulating a
// broker-side channel close without a real AMQP connection.
type fakeChannel struct {
	mu       sync.Mutex
	notifyCh chan *amqp091.Error // registered via NotifyClose
	once     sync.Once
	closes   atomic.Int32 // number of Close() calls, for Drain/release assertions
}

func (f *fakeChannel) NotifyClose(c chan *amqp091.Error) chan *amqp091.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifyCh = c
	return c
}

func (f *fakeChannel) Close() error { f.closes.Add(1); return nil }

// closeCount returns how many times Close has been called on this channel.
func (f *fakeChannel) closeCount() int32 { return f.closes.Load() }

// simulateClose fires the close notification, mirroring a broker-side close.
// The notification is written to the buffered channel synchronously; no sleep
// is needed — the value is observable immediately after this call returns.
func (f *fakeChannel) simulateClose() {
	f.mu.Lock()
	ch := f.notifyCh
	f.mu.Unlock()

	if ch == nil {
		return
	}
	f.once.Do(func() {
		ch <- &amqp091.Error{Code: 404, Reason: "simulated close"}
	})
}
