package warren_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren"
)

func TestRetryPolicy_defaultsApplied(t *testing.T) {
	p := warren.RetryPolicy{Jitter: warren.JitterNone}
	d1 := p.NextBackoff(1)
	assert.Equal(t, time.Second, d1, "attempt 1 with defaults must equal Min (1s)")
}

func TestRetryPolicy_exponentialGrowth(t *testing.T) {
	p := warren.RetryPolicy{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2.0,
		Jitter: warren.JitterNone,
	}
	assert.Equal(t, 100*time.Millisecond, p.NextBackoff(1))
	assert.Equal(t, 200*time.Millisecond, p.NextBackoff(2))
	assert.Equal(t, 400*time.Millisecond, p.NextBackoff(3))
	assert.Equal(t, 800*time.Millisecond, p.NextBackoff(4))
}

func TestRetryPolicy_cappedAtMax(t *testing.T) {
	p := warren.RetryPolicy{
		Min:    time.Second,
		Max:    5 * time.Second,
		Factor: 10.0,
		Jitter: warren.JitterNone,
	}
	assert.Equal(t, 5*time.Second, p.NextBackoff(3), "duration must be capped at Max")
	assert.Equal(t, 5*time.Second, p.NextBackoff(100), "high attempt still capped at Max")
}

func TestRetryPolicy_jitterWithinBounds(t *testing.T) {
	// Default jitter (zero value) is full jitter; every sample must stay within
	// the [Min, Max] bound the strategy promises.
	p := warren.RetryPolicy{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2.0,
	}
	for range 100 {
		d := p.NextBackoff(2)
		assert.GreaterOrEqual(t, d, p.Min, "jitter must not go below Min")
		assert.LessOrEqual(t, d, p.Max, "jitter must not exceed Max")
	}
}

func TestRetryPolicy_jitterNoneIsDeterministic(t *testing.T) {
	p := warren.RetryPolicy{
		Min:    200 * time.Millisecond,
		Max:    time.Minute,
		Factor: 2.0,
		Jitter: warren.JitterNone,
	}
	d1 := p.NextBackoff(3)
	d2 := p.NextBackoff(3)
	assert.Equal(t, d1, d2)
}

func TestRetryPolicy_defaultIsFullJitter(t *testing.T) {
	// The zero value of Jitter must behave as full jitter — the SRE default — not
	// as none (deterministic) nor equal (never below half the exponential delay).
	// Full jitter is the only strategy that is BOTH non-deterministic AND spreads
	// below exp/2, so asserting both pins the default.
	p := warren.RetryPolicy{Min: 10 * time.Millisecond, Max: 10 * time.Second, Factor: 2.0}
	const exp = 160 * time.Millisecond // 10ms * 2^4 at attempt 5

	first := p.NextBackoff(5)
	var sawBelowHalf, sawDistinct bool
	for range 500 {
		d := p.NextBackoff(5)
		assert.GreaterOrEqual(t, d, p.Min, "full jitter must not go below Min")
		assert.LessOrEqual(t, d, exp, "full jitter must not exceed the exponential delay")
		if d < exp/2 {
			sawBelowHalf = true
		}
		if d != first {
			sawDistinct = true
		}
	}
	assert.True(t, sawBelowHalf, "default must be full jitter (spreads below exp/2), not equal jitter")
	assert.True(t, sawDistinct, "default must be full jitter (non-deterministic), not none")
}

func TestRetryPolicy_equalJitter(t *testing.T) {
	// Equal jitter keeps at least half the exponential delay, then jitters the
	// rest: every sample lands in [exp/2, exp].
	p := warren.RetryPolicy{Min: 10 * time.Millisecond, Max: 10 * time.Second, Factor: 2.0, Jitter: warren.JitterEqual}
	const exp = 160 * time.Millisecond // 10ms * 2^4 at attempt 5

	for range 500 {
		d := p.NextBackoff(5)
		assert.GreaterOrEqual(t, d, exp/2, "equal jitter keeps at least half the exponential delay")
		assert.LessOrEqual(t, d, exp, "equal jitter must not exceed the exponential delay")
	}
}

func TestRetryPolicy_overflowSaturatesAtMax(t *testing.T) {
	// A large attempt drives Min*Factor^(n-1) past float64's range to +Inf; the
	// math.Min(exp, Max) cap must tame it to exactly Max rather than overflow into
	// a negative or zero Duration. This guard is reachable in production via the
	// unlimited-retries reconnect loop (Retries: 0), so it must never regress.
	p := warren.RetryPolicy{Min: time.Second, Max: 30 * time.Second, Factor: 2.0, Jitter: warren.JitterNone}
	assert.Equal(t, 30*time.Second, p.NextBackoff(1000), "attempt 1000 must saturate at Max")
	assert.Equal(t, 30*time.Second, p.NextBackoff(math.MaxInt32), "MaxInt32 attempt must saturate at Max, not overflow")
}

func TestRetryPolicy_nonPositiveAttempt(t *testing.T) {
	// n is contractually 1-indexed, but NextBackoff is public: an out-of-contract
	// n <= 0 yields a sub-Min exponential that the floor clamp lifts back to Min,
	// never a negative or zero Duration.
	p := warren.RetryPolicy{Min: time.Second, Max: 30 * time.Second, Factor: 2.0, Jitter: warren.JitterNone}
	assert.Equal(t, time.Second, p.NextBackoff(0), "attempt 0 must clamp up to Min")
	assert.Equal(t, time.Second, p.NextBackoff(-5), "negative attempt must clamp up to Min")
}

func TestRetryPolicy_negativeFieldsDefault(t *testing.T) {
	// Each zero/negative/NaN field must independently fall back to its documented
	// default (Min=1s, Max=30s, Factor=2.0). Isolating one field per case catches
	// a regression that loosens or drops a single guard.
	tests := []struct {
		name string
		p    warren.RetryPolicy
		n    int
		want time.Duration
	}{
		{
			name: "negative Factor defaults to 2.0",
			p:    warren.RetryPolicy{Min: 100 * time.Millisecond, Max: 10 * time.Second, Factor: -1, Jitter: warren.JitterNone},
			n:    2,
			want: 200 * time.Millisecond, // 100ms * 2.0^1
		},
		{
			name: "NaN Factor defaults to 2.0",
			p:    warren.RetryPolicy{Min: 100 * time.Millisecond, Max: 10 * time.Second, Factor: math.NaN(), Jitter: warren.JitterNone},
			n:    3,
			want: 400 * time.Millisecond, // 100ms * 2.0^2
		},
		{
			name: "negative Min defaults to 1s",
			p:    warren.RetryPolicy{Min: -1, Max: 10 * time.Second, Factor: 2.0, Jitter: warren.JitterNone},
			n:    1,
			want: time.Second, // default Min * 2.0^0
		},
		{
			name: "negative Max defaults to 30s",
			p:    warren.RetryPolicy{Min: 100 * time.Millisecond, Max: -1, Factor: 100.0, Jitter: warren.JitterNone},
			n:    3,
			want: 30 * time.Second, // 100ms * 100^2 = 1000s, capped at default Max
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.p.NextBackoff(tt.n))
		})
	}
}

func TestRetryPolicy_maxBelowMinClampsToMin(t *testing.T) {
	// A degenerate Max<Min config collapses the window to [Min, Min]: Min is the
	// floor and wins, so the result is Min, never a value below it.
	p := warren.RetryPolicy{Min: 10 * time.Second, Max: time.Second, Factor: 2.0, Jitter: warren.JitterNone}
	assert.Equal(t, 10*time.Second, p.NextBackoff(1), "Max<Min must collapse to Min, not Max")
	assert.Equal(t, 10*time.Second, p.NextBackoff(5), "Max<Min stays at Min across attempts")
}

func TestRetryPolicy_fullJitterAtSaturation(t *testing.T) {
	// At a saturated attempt the exponential delay equals Max, so full jitter
	// spreads across the entire [Min, Max] window — the widest and most
	// production-relevant spread. Assert samples reach both the lower half and
	// near the top of the window.
	p := warren.RetryPolicy{Min: time.Second, Max: 30 * time.Second, Factor: 2.0} // default full jitter
	var sawLowerHalf, sawNearMax bool
	for range 500 {
		d := p.NextBackoff(10) // 1s * 2^9 = 512s, saturated to Max=30s
		require.GreaterOrEqual(t, d, p.Min, "full jitter must not go below Min")
		require.LessOrEqual(t, d, p.Max, "full jitter must not exceed Max")
		if d < p.Max/2 {
			sawLowerHalf = true
		}
		if d > p.Max*9/10 {
			sawNearMax = true
		}
	}
	assert.True(t, sawLowerHalf, "full jitter at saturation must reach the lower half of [Min, Max]")
	assert.True(t, sawNearMax, "full jitter at saturation must reach near Max")
}

// FuzzNextBackoff asserts the load-bearing numeric invariants of NextBackoff over
// arbitrary inputs: the result is always non-negative, and whenever the caller
// supplies a sane window (Min>0, Max>0, Min<=Max) the result honours [Min, Max]
// for every Jitter strategy. A NaN- or Inf-poisoned internal computation converts
// to a hugely negative time.Duration, so the non-negative check is what catches a
// dropped Factor/overflow guard. Durations are fuzzed in milliseconds via uint16
// to stay within float64's exact-integer range (no int64 round-trip overflow),
// while n (the exponential axis) and factor (including NaN/Inf the fuzzer may
// synthesise) range freely to exercise the overflow/underflow guards.
func FuzzNextBackoff(f *testing.F) {
	seeds := []struct {
		n         int
		minMs     uint16
		maxMs     uint16
		factor    float64
		jitterSel uint8
	}{
		{1, 1000, 30000, 2.0, 0},
		{5, 10, 10000, 2.0, 1},
		{0, 1000, 30000, 2.0, 2},
		{-5, 1000, 30000, 2.0, 0},
		{1000, 1000, 30000, 2.0, 2},
		{3, 30000, 1000, 2.0, 2},         // Max < Min
		{3, 1000, 30000, math.NaN(), 1},  // NaN factor must default, not poison
		{2, 1000, 30000, math.Inf(1), 0}, // +Inf factor saturates, stays finite
		{math.MaxInt32, 1000, 30000, 2.0, 2},
		{math.MinInt32, 1000, 30000, 2.0, 1},
	}
	for _, s := range seeds {
		f.Add(s.n, s.minMs, s.maxMs, s.factor, s.jitterSel)
	}
	f.Fuzz(func(t *testing.T, n int, minMs, maxMs uint16, factor float64, jitterSel uint8) {
		p := warren.RetryPolicy{
			Min:    time.Duration(minMs) * time.Millisecond,
			Max:    time.Duration(maxMs) * time.Millisecond,
			Factor: factor,
			Jitter: warren.JitterStrategy(jitterSel % 3),
		}
		d := p.NextBackoff(n)
		require.GreaterOrEqual(t, d, time.Duration(0),
			"NextBackoff must never be negative (n=%d minMs=%d maxMs=%d factor=%v)", n, minMs, maxMs, factor)
		if minMs > 0 && maxMs > 0 && minMs <= maxMs {
			require.GreaterOrEqual(t, d, p.Min, "result below Min for a sane window")
			require.LessOrEqual(t, d, p.Max, "result above Max for a sane window")
		}
	})
}
