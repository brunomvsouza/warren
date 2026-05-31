package warren

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a token-bucket limiter that paces publishes to a sustained rate
// (tokens per second) while tolerating a burst of up to one second's worth of
// tokens. It is the local guardrail behind WithPublishRateLimit, protecting the
// broker from an accidental runaway publish loop.
//
// It implements GCRA (the generic cell rate algorithm): a single "theoretical
// arrival time" cursor (tat) is advanced by one emission interval per grant, and
// a grant is conformant (proceeds immediately) as long as the cursor has not run
// more than the burst-tolerance window (tau) ahead of the wall clock. This needs
// only a time.Time and two durations — no background goroutine, no timer per idle
// token. A nil *rateLimiter is a disabled no-op so the publish hot path stays
// branch-light when the option is unset (mirrors byteLimiter).
type rateLimiter struct {
	mu       sync.Mutex
	interval time.Duration // emission interval: one second / perSec
	tau      time.Duration // burst tolerance: (burst-1) * interval
	tat      time.Time     // theoretical arrival time of the next grant
}

// newRateLimiter returns a limiter pacing to perSec grants per second with a burst
// capacity of perSec tokens, or nil (disabled) when perSec <= 0.
func newRateLimiter(perSec int) *rateLimiter {
	if perSec <= 0 {
		return nil
	}
	interval := time.Second / time.Duration(perSec)
	if interval <= 0 {
		// perSec > 1e9 rounds the interval to zero; clamp to the finest tick so the
		// cursor still advances and the bucket never degenerates into "no limit".
		interval = time.Nanosecond
	}
	return &rateLimiter{
		interval: interval,
		tau:      time.Duration(perSec-1) * interval,
	}
}

// reserve advances the bucket and returns how long the caller must wait before its
// grant is conformant; a zero duration means proceed now (the grant fell within
// the burst). reserve mutates state unconditionally: a caller that then abandons
// its slot (ctx cancel) leaves the cursor advanced, which only makes the limiter
// more conservative — it never lets throughput exceed the configured rate.
func (rl *rateLimiter) reserve(now time.Time) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.tat.Before(now) {
		rl.tat = now // idle long enough that the cursor fell behind: reset to now
	}
	allowAt := rl.tat.Add(-rl.tau)
	rl.tat = rl.tat.Add(rl.interval)
	if now.Before(allowAt) {
		return allowAt.Sub(now)
	}
	return 0
}

// wait blocks until a token is available or ctx is done. It reports whether the
// call was throttled (had to wait at all) so the caller can record the
// publisher_rate_limited_total signal, and returns ctx.Err() if ctx fires first.
func (rl *rateLimiter) wait(ctx context.Context) (throttled bool, err error) {
	if rl == nil {
		return false, nil
	}
	d := rl.reserve(time.Now())
	if d <= 0 {
		return false, nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-t.C:
		return true, nil
	}
}
