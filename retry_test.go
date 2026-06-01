package warren_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
