package amqp

// White-box tests for the channelPool (T08).
// Package amqp (not amqp_test) to access the unexported type.

import (
	"context"
	"errors"
	"sync"
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = p.Acquire(context.Background())
	}()

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

// — basic acquire/release —————————————————————————————————————————————————

func TestChannelPool_AcquireRelease_IdleChannelReused(t *testing.T) {

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

// TestChannelPool_ClosedChannelDiscarded asserts that a channel whose close
// notification fires is not returned to the idle list on release.
func TestChannelPool_ClosedChannelDiscarded(t *testing.T) {

	openCount := 0
	var mu sync.Mutex

	openFn := func() (amqpChannel, error) {
		mu.Lock()
		defer mu.Unlock()
		openCount++
		return &fakeChannel{}, nil
	}

	p := newChannelPool(2, openFn)

	// Acquire a channel, trigger broker-side close, then release.
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, rel, err := p.Acquire(ctx)
			if err == nil {
				rel()
			}
		}()
	}
	wg.Wait()
}

// — IsTransient classification ————————————————————————————————————————————

func TestErrChannelPoolExhausted_IsTransient(t *testing.T) {
	assert.True(t, IsTransient(ErrChannelPoolExhausted))
}

// — fakeChannel helper ——————————————————————————————————————————————————

// fakeChannel is a test double for amqpChannel that allows simulating a
// broker-side channel close without a real AMQP connection.
type fakeChannel struct {
	mu       sync.Mutex
	notifyCh chan *amqp091.Error // registered via NotifyClose
	once     sync.Once
}

func (f *fakeChannel) NotifyClose(c chan *amqp091.Error) chan *amqp091.Error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifyCh = c
	return c
}

func (f *fakeChannel) Close() error { return nil }

// simulateClose fires the close notification, mirroring a broker-side close.
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
	// Brief pause so the notification is observable on next Acquire/release.
	time.Sleep(5 * time.Millisecond)
}
