package warren

import (
	"math"
	"math/rand/v2"
	"time"
)

// JitterStrategy selects how NextBackoff perturbs the exponential delay to
// de-synchronize a fleet that failed together — the defence against the
// "thundering herd" that hammers a recovering broker when every client retries
// in lockstep. The zero value is JitterFull, the SRE-recommended default, so a
// RetryPolicy that does not set Jitter gets the strongest spreading for free.
type JitterStrategy int

const (
	// JitterFull spreads each delay uniformly across the whole exponential
	// window: random(0, exp), clamped to [Min, Max]. This is the default (zero
	// value) and the AWS-recommended strategy ("Exponential Backoff And Jitter")
	// — two clients that failed at the same instant pick independent delays
	// across the entire window instead of clustering near one value, so a
	// recovering broker sees retries arrive smoothly rather than in a spike.
	JitterFull JitterStrategy = iota
	// JitterEqual keeps half the exponential delay and jitters the other half:
	// exp/2 + random(0, exp/2), clamped to [Min, Max]. A tighter spread than full
	// jitter — choose it when a guaranteed minimum progress per attempt matters
	// more than maximal de-correlation.
	JitterEqual
	// JitterNone disables jitter: NextBackoff returns the pure exponential delay,
	// deterministic for a given attempt. Intended for tests and reproductions;
	// avoid it in production, where a synchronized fleet retrying in lockstep can
	// stampede a recovering broker.
	JitterNone
)

// RetryPolicy configures exponential backoff with jitter for the reconnect loop
// and publish retries. Zero values are replaced with safe defaults at call time:
// Min=1s, Max=30s, Factor=2.0, Jitter=JitterFull.
type RetryPolicy struct {
	// Min is the minimum backoff duration. Defaults to 1s when zero. It is also
	// the floor every jitter strategy clamps up to, so no attempt returns less.
	Min time.Duration
	// Max is the maximum backoff duration. Defaults to 30s when zero.
	Max time.Duration
	// Factor is the exponential multiplier applied per failed attempt.
	// Defaults to 2.0 when not a positive number (zero, negative, or NaN).
	Factor float64
	// Retries is the maximum number of consecutive failed attempts before the
	// reconnect loop gives up. Zero means unlimited retries.
	Retries int
	// Jitter selects how the exponential delay is perturbed to de-synchronize a
	// recovering fleet. The zero value is JitterFull (SRE-recommended); set
	// JitterNone for deterministic timing in tests.
	Jitter JitterStrategy
}

// NextBackoff returns the backoff duration for attempt n (1-indexed). The pure
// exponential delay is Min*Factor^(n-1) capped at Max; the configured Jitter
// strategy then perturbs it, and the result is always clamped to the effective
// [Min, Max]. Min is the floor and wins when Max < Min: a degenerate config
// returns Min rather than a value below it. The result is always finite and
// non-negative for any input — a non-positive or NaN Factor falls back to the
// 2.0 default, and an out-of-range attempt n saturates to Min (n <= 0) or Max
// (very large n) rather than under- or overflowing.
func (p RetryPolicy) NextBackoff(n int) time.Duration {
	lo := p.Min
	if lo <= 0 {
		lo = time.Second
	}
	hi := p.Max
	if hi <= 0 {
		hi = 30 * time.Second
	}
	// Min is the floor: collapse a degenerate Max<Min window to [lo, lo] so the
	// clamp below is well-formed and the result can never drop below Min.
	if hi < lo {
		hi = lo
	}
	factor := p.Factor
	if !(factor > 0) { // also catches NaN, which compares false to everything
		factor = 2.0
	}

	// float64(n)-1 (not float64(n-1)) so an extreme n cannot wrap the int
	// subtraction; math.Pow then saturates to ±Inf and the cap below tames it.
	exp := float64(lo) * math.Pow(factor, float64(n)-1)
	exp = math.Min(exp, float64(hi))

	var d float64
	switch p.Jitter {
	case JitterNone:
		d = exp
	case JitterEqual:
		d = exp/2 + rand.Float64()*(exp/2) //nolint:gosec // jitter spread, not cryptographic
	default: // JitterFull
		d = rand.Float64() * exp //nolint:gosec // jitter spread, not cryptographic
	}

	// Honour the [lo, hi] contract regardless of strategy: full jitter can land
	// below lo, so clamp up; exp is already capped at hi, so clamp down guards
	// only against rounding.
	d = math.Max(float64(lo), math.Min(d, float64(hi)))
	return time.Duration(d)
}
