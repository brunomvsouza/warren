package warren_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/warren"
)

func TestRetryPolicy_defaultsApplied(t *testing.T) {
	p := warren.RetryPolicy{WithoutJitter: true}
	d1 := p.NextBackoff(1)
	assert.Equal(t, time.Second, d1, "attempt 1 with defaults must equal Min (1s)")
}

func TestRetryPolicy_exponentialGrowth(t *testing.T) {
	p := warren.RetryPolicy{
		Min:           100 * time.Millisecond,
		Max:           10 * time.Second,
		Factor:        2.0,
		WithoutJitter: true,
	}
	assert.Equal(t, 100*time.Millisecond, p.NextBackoff(1))
	assert.Equal(t, 200*time.Millisecond, p.NextBackoff(2))
	assert.Equal(t, 400*time.Millisecond, p.NextBackoff(3))
	assert.Equal(t, 800*time.Millisecond, p.NextBackoff(4))
}

func TestRetryPolicy_cappedAtMax(t *testing.T) {
	p := warren.RetryPolicy{
		Min:           time.Second,
		Max:           5 * time.Second,
		Factor:        10.0,
		WithoutJitter: true,
	}
	assert.Equal(t, 5*time.Second, p.NextBackoff(3), "duration must be capped at Max")
	assert.Equal(t, 5*time.Second, p.NextBackoff(100), "high attempt still capped at Max")
}

func TestRetryPolicy_jitterWithinBounds(t *testing.T) {
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

func TestRetryPolicy_withoutJitterIsDeterministic(t *testing.T) {
	p := warren.RetryPolicy{
		Min:           200 * time.Millisecond,
		Max:           time.Minute,
		Factor:        2.0,
		WithoutJitter: true,
	}
	d1 := p.NextBackoff(3)
	d2 := p.NextBackoff(3)
	assert.Equal(t, d1, d2)
}
