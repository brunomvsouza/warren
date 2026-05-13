// Package reconnect provides a supervised reconnect loop with configurable
// exponential backoff.
package reconnect

import (
	"context"
	"sync"
	"time"
)

// Loop manages a supervised connect-retry cycle. It calls connect with
// exponential backoff until the context is cancelled, the max-retry limit is
// reached, or connect returns nil (success). On success it fires the registered
// OnReconnect callbacks and exits.
type Loop struct {
	connect    func(ctx context.Context) error
	backoff    func(attempt int) time.Duration
	maxRetries int

	mu        sync.Mutex
	callbacks []func()
	cancel    context.CancelFunc
	done      chan struct{}
}

// New creates a Loop and immediately starts it in a background goroutine.
//
// connect is called on each attempt. backoff(n) returns the wait duration
// before attempt n+1 (n is 1-indexed). maxRetries caps consecutive failures;
// 0 means unlimited.
func New(ctx context.Context, connect func(ctx context.Context) error, backoff func(int) time.Duration, maxRetries int) *Loop {
	ctx2, cancel := context.WithCancel(ctx)
	l := &Loop{
		connect:    connect,
		backoff:    backoff,
		maxRetries: maxRetries,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	go l.run(ctx2)
	return l
}

// OnReconnect registers fn to be called after each successful connection.
// Safe to call before or after New; fn is invoked in the Loop goroutine.
func (l *Loop) OnReconnect(fn func()) {
	l.mu.Lock()
	l.callbacks = append(l.callbacks, fn)
	l.mu.Unlock()
}

// Done returns a channel that is closed when the loop goroutine exits, whether
// due to success, max-retry exhaustion, or context cancellation.
func (l *Loop) Done() <-chan struct{} { return l.done }

// Stop cancels the reconnect loop and waits for the background goroutine to
// exit. Returns ctx.Err() if ctx expires before the goroutine finishes.
func (l *Loop) Stop(ctx context.Context) error {
	l.cancel()
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Loop) run(ctx context.Context) {
	defer close(l.done)
	attempt := 0
	for {
		err := l.connect(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			l.fireCallbacks()
			return
		}

		attempt++
		if l.maxRetries > 0 && attempt >= l.maxRetries {
			return
		}

		wait := l.backoff(attempt)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (l *Loop) fireCallbacks() {
	l.mu.Lock()
	cbs := make([]func(), len(l.callbacks))
	copy(cbs, l.callbacks)
	l.mu.Unlock()
	for _, fn := range cbs {
		fn()
	}
}
