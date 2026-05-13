package reconnect_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/amqp/internal/reconnect"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fastBackoff returns a sub-millisecond backoff suitable for unit tests.
func fastBackoff(_ int) time.Duration { return time.Millisecond }

// waitOrFail waits on ch or fails the test after 5 seconds.
func waitOrFail(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for: %s", msg)
	}
}

// — Three attempts then succeed ——————————————————————————————————————————————

func TestLoop_threeAttemptsTheneSucceed(t *testing.T) {
	var attempts atomic.Int32
	var reconnects atomic.Int32
	reconnected := make(chan struct{})

	connect := func(ctx context.Context) error {
		n := attempts.Add(1)
		if n <= 3 {
			return errors.New("refused")
		}
		return nil
	}

	ctx := context.Background()
	loop := reconnect.New(ctx, connect, fastBackoff, 0)
	loop.OnReconnect(func() {
		reconnects.Add(1)
		close(reconnected)
	})

	// wait for the loop to succeed naturally
	waitOrFail(t, reconnected, "OnReconnect to fire")

	// loop has already exited; Stop is a no-op
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, loop.Stop(stopCtx))

	assert.Equal(t, int32(4), attempts.Load(), "exactly 4 connect calls expected")
	assert.Equal(t, int32(1), reconnects.Load(), "OnReconnect must fire once")
}

// — Cancel mid-backoff ————————————————————————————————————————————————————

func TestLoop_cancelMidBackoff(t *testing.T) {
	connect := func(_ context.Context) error {
		return errors.New("always fails")
	}

	longBackoff := func(_ int) time.Duration { return 10 * time.Second }

	ctx, cancelCtx := context.WithCancel(context.Background())
	loop := reconnect.New(ctx, connect, longBackoff, 0)

	// connect() fails; 10-second backoff starts — cancel before it elapses
	cancelCtx()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	err := loop.Stop(stopCtx)

	// loop must exit promptly (not hang for 10 seconds)
	require.NoError(t, err, "loop must exit within 1 second after context cancel")
}

// — Max retries stops the loop ————————————————————————————————————————————

func TestLoop_maxRetriesExhausted(t *testing.T) {
	var attempts atomic.Int32
	connect := func(_ context.Context) error {
		attempts.Add(1)
		return errors.New("refused")
	}

	ctx := context.Background()
	loop := reconnect.New(ctx, connect, fastBackoff, 3)

	// wait for the loop to exhaust retries and exit naturally
	waitOrFail(t, loop.Done(), "loop to exit after max retries")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, loop.Stop(stopCtx))

	// with maxRetries=3 the loop stops after 3 failed attempts
	assert.Equal(t, int32(3), attempts.Load())
}

// — OnReconnect registered after New still fires ——————————————————————————

func TestLoop_onReconnectRegisteredAfterNew(t *testing.T) {
	var attempts atomic.Int32
	connect := func(_ context.Context) error {
		n := attempts.Add(1)
		if n < 2 {
			return errors.New("refused")
		}
		return nil
	}

	fired := make(chan struct{}, 1)

	ctx := context.Background()
	loop := reconnect.New(ctx, connect, fastBackoff, 0)
	loop.OnReconnect(func() { fired <- struct{}{} })

	waitOrFail(t, fired, "OnReconnect to fire")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, loop.Stop(stopCtx))
}

// — Stop is idempotent ————————————————————————————————————————————————————

func TestLoop_stopIdempotent(t *testing.T) {
	connect := func(_ context.Context) error { return errors.New("refused") }

	ctx := context.Background()
	loop := reconnect.New(ctx, connect, fastBackoff, 1)

	// wait for loop to exit naturally after 1 max retry
	waitOrFail(t, loop.Done(), "loop to exit")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, loop.Stop(stopCtx))
	require.NoError(t, loop.Stop(stopCtx)) // second Stop must not panic or block
}

// — Stop timeout ——————————————————————————————————————————————————————————

func TestLoop_stopTimeout(t *testing.T) {
	// connect blocks forever regardless of ctx — simulates a stuck connection
	hold := make(chan struct{})
	connect := func(_ context.Context) error {
		<-hold // ignores ctx cancellation, blocks until hold is closed
		return nil
	}

	ctx := context.Background()
	loop := reconnect.New(ctx, connect, fastBackoff, 0)

	// Stop with a very short deadline — loop is blocked inside connect()
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := loop.Stop(stopCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// cleanup: unblock connect() so the goroutine can exit
	close(hold)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	defer cleanupCancel()
	_ = loop.Stop(cleanupCtx)
}
