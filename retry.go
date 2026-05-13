package amqp

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy configures exponential backoff with optional jitter for the
// reconnect loop. Zero values are replaced with safe defaults at call time:
// Min=1s, Max=30s, Factor=2.0.
type RetryPolicy struct {
	// Min is the minimum backoff duration. Defaults to 1s when zero.
	Min time.Duration
	// Max is the maximum backoff duration. Defaults to 30s when zero.
	Max time.Duration
	// Factor is the exponential multiplier applied per failed attempt.
	// Defaults to 2.0 when zero or negative.
	Factor float64
	// Retries is the maximum number of consecutive failed attempts before the
	// reconnect loop gives up. Zero means unlimited retries.
	Retries int
	// WithoutJitter disables the ±25% random jitter applied to each backoff
	// duration. Use in tests where deterministic timing is required.
	WithoutJitter bool
}

// NextBackoff returns the backoff duration for attempt n (1-indexed).
// The result is capped to [Min, Max] and, unless WithoutJitter is set, has
// a random ±25% perturbation applied before capping.
func (p RetryPolicy) NextBackoff(n int) time.Duration {
	min := p.Min
	if min <= 0 {
		min = time.Second
	}
	max := p.Max
	if max <= 0 {
		max = 30 * time.Second
	}
	factor := p.Factor
	if factor <= 0 {
		factor = 2.0
	}

	d := float64(min) * math.Pow(factor, float64(n-1))
	d = math.Min(d, float64(max))

	if !p.WithoutJitter {
		d += d * 0.25 * (2*rand.Float64() - 1) //nolint:gosec
		d = math.Max(float64(min), math.Min(d, float64(max)))
	}

	return time.Duration(d)
}
