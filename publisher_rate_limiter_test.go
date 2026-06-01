package warren

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/log"
)

// newFakePublisherConn returns a *Connection with one publisher-side managedConn,
// sufficient for PublisherBuilder.Build() (which wires pools lazily and never
// contacts the broker until the first Publish).
func newFakePublisherConn(t *testing.T) *Connection {
	t.Helper()
	conn := &Connection{}
	conn.opts.logger = log.NewNoOp()
	mc := &managedConn{opts: &conn.opts}
	conn.pubConns = []*managedConn{mc}
	return conn
}

func TestNewRateLimiter_ZeroOrNegative_Disabled(t *testing.T) {
	assert.Nil(t, newRateLimiter(0), "perSec 0 disables the limiter")
	assert.Nil(t, newRateLimiter(-1), "negative perSec disables the limiter")
}

func TestRateLimiter_NilReceiver_IsNoOp(t *testing.T) {
	var rl *rateLimiter // disabled
	throttled, err := rl.wait(context.Background())
	assert.False(t, throttled)
	assert.NoError(t, err)
}

func TestRateLimiter_WithinBurst_DoesNotThrottle(t *testing.T) {
	// burst == perSec, so the first perSec acquisitions proceed without waiting.
	rl := newRateLimiter(100)
	for i := range 100 {
		throttled, err := rl.wait(context.Background())
		require.NoError(t, err)
		require.Falsef(t, throttled, "acquisition %d within the burst must not throttle", i)
	}
}

func TestRateLimiter_OverBurst_ThrottlesUntilRefill(t *testing.T) {
	// perSec 50 → 20ms emission interval, burst 50.
	rl := newRateLimiter(50)
	for range 50 { // drain the burst instantly
		_, err := rl.wait(context.Background())
		require.NoError(t, err)
	}

	start := time.Now()
	throttled, err := rl.wait(context.Background()) // 51st must wait ~one interval
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.True(t, throttled, "the over-burst acquisition must throttle")
	assert.GreaterOrEqual(t, elapsed, 10*time.Millisecond,
		"must wait about one refill interval (20ms) before proceeding")
}

func TestRateLimiter_SustainedRate_CapsThroughput(t *testing.T) {
	// Verify the T51 acceptance: WithPublishRateLimit(N) paces to ~N/s. After the
	// burst is drained, each further grant is spaced by one emission interval, so
	// `extra` grants cannot complete faster than (extra-1) intervals.
	const perSec = 200 // 5ms interval
	rl := newRateLimiter(perSec)
	for range perSec { // drain the burst
		_, err := rl.wait(context.Background())
		require.NoError(t, err)
	}

	const extra = 10
	start := time.Now()
	for range extra {
		_, err := rl.wait(context.Background())
		require.NoError(t, err)
	}
	elapsed := time.Since(start)

	interval := time.Second / perSec
	// Lower-bound with generous slack for timer granularity / scheduling.
	assert.GreaterOrEqualf(t, elapsed, time.Duration(extra-1)*interval/2,
		"sustained grants must be paced by the configured rate (got %s for %d grants at %d/s)",
		elapsed, extra, perSec)
}

func TestPublisherBuilder_WithPublishRateLimit_SetsLimiter(t *testing.T) {
	conn := newFakePublisherConn(t)
	p, err := PublisherFor[testPayload](conn).WithPublishRateLimit(100).Build()
	require.NoError(t, err)
	require.NotNil(t, p.rateLimiter, "a positive WithPublishRateLimit must create a limiter")
}

func TestPublisherBuilder_WithPublishRateLimit_LastWins(t *testing.T) {
	conn := newFakePublisherConn(t)
	p, err := PublisherFor[testPayload](conn).
		WithPublishRateLimit(100).
		WithPublishRateLimit(50).
		Build()
	require.NoError(t, err)
	require.NotNil(t, p.rateLimiter)
	// 50/s → 20ms interval; assert the last value won via the limiter's pacing.
	assert.Equal(t, 20*time.Millisecond, p.rateLimiter.interval, "last WithPublishRateLimit call wins")
}

func TestPublisherBuilder_NoRateLimit_NilLimiter(t *testing.T) {
	conn := newFakePublisherConn(t)
	p, err := PublisherFor[testPayload](conn).Build()
	require.NoError(t, err)
	assert.Nil(t, p.rateLimiter, "no WithPublishRateLimit leaves the limiter disabled")
}

func TestPublisher_Publish_RateLimited_RecordsMetricAndProceeds(t *testing.T) {
	// Drain the burst, then the next publish must wait ~one interval, recording
	// publisher_rate_limited_total and then proceeding. perSec is kept low (100ms
	// interval, burst 10) so the over-burst publish stays throttled even if the 10
	// drain publishes take tens of ms of wall time under -race — the over-burst
	// grant only stops throttling once a full interval of wall time has elapsed.
	fake := newFakePubCh(true /* autoAck */)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	const burst = 10
	pub.rateLimiter = newRateLimiter(burst) // 100ms interval, burst 10
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	v := testPayload{Value: "x"}
	for range burst { // drain the burst — none throttled
		require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &v}))
	}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &v})) // over-burst throttles

	pm.mu.Lock()
	defer pm.mu.Unlock()
	require.NotEmpty(t, pm.rateLimited, "a throttled publish must increment publisher_rate_limited_total")
	assert.Equal(t, "x", pm.rateLimited[0], "the exchange label must be recorded")
}

func TestPublisher_Publish_RateLimit_ContextCancel_ReturnsErrRateLimited(t *testing.T) {
	fake := newFakePubCh(true)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	pub.rateLimiter = newRateLimiter(1) // 1s interval, burst 1
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	v := testPayload{Value: "x"}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &v})) // consume burst token

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := pub.Publish(ctx, Message[testPayload]{Body: &v}) // blocks ~1s; ctx fires first

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRateLimited, "a publish cancelled while awaiting a token returns ErrRateLimited")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "ErrRateLimited wraps the originating ctx error")
	assert.True(t, IsTransient(err), "ErrRateLimited is transient")
}

func TestRateLimiter_ContextCancelWhileWaiting_ReturnsErr(t *testing.T) {
	rl := newRateLimiter(1) // 1s interval, burst 1
	_, err := rl.wait(context.Background())
	require.NoError(t, err) // consumes the single burst token

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	throttled, err := rl.wait(ctx) // would block ~1s; ctx fires first at 20ms
	assert.True(t, throttled)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRateLimiter_Reserve_DeterministicBurstAndConservativeCursor(t *testing.T) {
	// reserve takes an explicit clock, so the GCRA contract can be pinned exactly
	// without sleeping (no timing slack, no flakiness). perSec=4 → 250ms interval,
	// burst 4, tau = 3*250ms = 750ms.
	const perSec = 4
	rl := newRateLimiter(perSec)
	interval := time.Second / perSec
	base := time.Unix(0, 0)

	// The first perSec grants at a fixed instant fall within the burst → proceed now.
	for i := range perSec {
		require.Zerof(t, rl.reserve(base), "grant %d is within the burst and must not wait", i)
	}
	// The (perSec+1)th grant at the same instant exceeds the burst by exactly one
	// emission interval — pinning the burst boundary precisely.
	require.Equal(t, interval, rl.reserve(base), "the first over-burst grant waits exactly one interval")

	// Conservative-cursor invariant (security audit LOW finding): abandoning a slot
	// only pushes the cursor further out — each further reserve at the same instant
	// adds exactly one interval to the required wait, never less, so a flood of
	// cancelled-before-send attempts can never let a later grant skip the queue.
	for i := 2; i <= 11; i++ {
		assert.Equalf(t, time.Duration(i)*interval, rl.reserve(base),
			"abandoned-then-retried grant %d adds exactly one interval, never under-throttles", i)
	}

	// Idle reset: once the clock advances past the accumulated cursor the bucket has
	// fully refilled and the next grant is immediate again (burst restored).
	assert.Zero(t, rl.reserve(base.Add(time.Hour)), "after idling past the cursor the burst is restored")
}

func TestRateLimiter_SingleToken_Burst1_PacesEveryGrant(t *testing.T) {
	// The degenerate burst==1 boundary: tau = (1-1)*interval = 0, so the burst window
	// collapses to nothing and every grant after the first must wait a full interval.
	// Pinned deterministically with an injected clock (no sleep). perSec=1 → 1s interval.
	rl := newRateLimiter(1)
	require.Equal(t, time.Second, rl.interval)
	require.Zero(t, rl.tau, "burst 1 leaves zero burst tolerance")
	base := time.Unix(0, 0)

	assert.Zero(t, rl.reserve(base), "the single burst token grants immediately")
	assert.Equal(t, time.Second, rl.reserve(base), "the next grant at the same instant waits a full interval")
	assert.Equal(t, 2*time.Second, rl.reserve(base), "and the one after waits two — strictly paced, no burst")
}

func TestNewRateLimiter_HugePerSec_ClampsInterval(t *testing.T) {
	// perSec > 1e9 rounds time.Second/perSec to zero; the constructor clamps the
	// emission interval to the finest tick so the bucket never degenerates into
	// "no limit". (The arithmetic also stays well within time.Duration's range:
	// tau = (perSec-1)*1ns ≈ 2s for perSec=2e9.)
	rl := newRateLimiter(2_000_000_000)
	require.NotNil(t, rl)
	assert.Equal(t, time.Nanosecond, rl.interval, "interval clamps to 1ns, not 0")
	assert.GreaterOrEqual(t, rl.tau, time.Duration(0), "tau stays non-negative")
	throttled, err := rl.wait(context.Background())
	require.NoError(t, err)
	assert.False(t, throttled, "the first grant is still within the burst")
}

func TestPublisher_PublishBatch_NotRateLimited(t *testing.T) {
	// WithPublishRateLimit governs single Publish only; PublishBatch is explicitly
	// excluded (mirroring PublishRetry's single-message scoping). Drain the single
	// burst token, then a batch must proceed at once — neither blocking on the bucket
	// for ~1s nor recording publisher_rate_limited_total. Guards against a future
	// refactor accidentally wiring the limiter into the batch path.
	fake := newFakePubCh(true /* autoAck */)
	pm := &capturePublisherMetrics{}
	pub, stopPool := newTestPubBatch[testPayload](fake, pm, 1024)
	pub.rateLimiter = newRateLimiter(1) // 1s interval, burst 1
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Exhaust the single burst token so any rate-limited path would block ~1s.
	_, err := pub.rateLimiter.wait(context.Background())
	require.NoError(t, err)

	msgs := make([]Message[testPayload], 5)
	for i := range msgs {
		msgs[i] = Message[testPayload]{Body: &testPayload{Value: "x"}}
	}

	start := time.Now()
	results, err := pub.PublishBatch(context.Background(), msgs)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Len(t, results, 5)
	for i, r := range results {
		assert.NoErrorf(t, r.Err, "batch message %d must publish", i)
	}
	assert.Less(t, elapsed, 500*time.Millisecond,
		"PublishBatch must not block on the drained rate-limit bucket")

	pm.mu.Lock()
	defer pm.mu.Unlock()
	assert.Empty(t, pm.rateLimited, "PublishBatch must not record publisher_rate_limited_total")
}

func TestRateLimiter_Concurrent_RaceClean(t *testing.T) {
	defer goleak.VerifyNone(t)
	// A single limiter shared by many goroutines is guarded only by its mutex. This
	// drives concurrent reserve/wait under -race to prove the tat cursor has no data
	// race. perSec is high so the burst absorbs the load without long waits — the
	// exact rate math is pinned deterministically by the reserve test above; here
	// the contract under test is purely concurrency safety.
	const perSec = 5000 // 200µs interval, burst 5000
	rl := newRateLimiter(perSec)

	const goroutines = 16
	const perG = 50 // 800 total grants, well within the burst
	var wg sync.WaitGroup
	var granted atomic.Int64
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perG {
				if _, err := rl.wait(context.Background()); err == nil {
					granted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(goroutines*perG), granted.Load(),
		"every concurrent grant must succeed; -race proves the shared cursor is safe")
}

func TestPublisher_Publish_RateLimited_PacesEveryRetryAttempt(t *testing.T) {
	// The documented contract: under PublishRetry, every retry of a single Publish
	// acquires its own token and increments publisher_rate_limited_total once per
	// throttled attempt (retries are real broker traffic). Drain the burst, then a
	// persistent transient broker failure forces Retries+1 attempts; each must
	// throttle and record the rate-limited signal exactly once.
	fake := newFakePubCh(false)
	fake.publishErr = &amqp091.Error{Code: 504, Reason: "channel error"} // → ErrChannelError (transient)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	const perSec = 50 // 20ms interval, burst 50
	pub.rateLimiter = newRateLimiter(perSec)
	pub.retryPolicy = &RetryPolicy{Retries: 3, Min: time.Millisecond, Max: time.Millisecond, Jitter: JitterNone}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Drain the burst so the very first publish attempt already throttles. The drain
	// grants fall within the burst, so wait() returns immediately and records nothing.
	for range perSec {
		_, err := pub.rateLimiter.wait(context.Background())
		require.NoError(t, err)
	}

	v := testPayload{Value: "x"}
	err := pub.Publish(context.Background(), Message[testPayload]{Body: &v})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChannelError, "the transient broker error surfaces after retries are exhausted")

	pm.mu.Lock()
	defer pm.mu.Unlock()
	// Retries=3 → 1 initial + 3 retried attempts = 4 broker attempts, each throttled.
	require.Len(t, pm.rateLimited, 4, "publisher_rate_limited_total increments once per throttled attempt")
	assert.Len(t, pm.retries, 3, "exactly Retries retry signals, one between each attempt")
	for _, ex := range pm.rateLimited {
		assert.Equal(t, "x", ex, "the exchange label is recorded on every rate-limited attempt")
	}
}

func TestPublisher_Publish_RateLimit_CancelDuringWait_NoRetryMetric(t *testing.T) {
	// A publish abandoned while awaiting a rate-limit token returns ErrRateLimited
	// (transient), but its ctx is already cancelled — so under PublishRetry it must
	// NOT record a spurious publisher_retry_total nor back off; it returns at once.
	// Guards the retry-loop ctx.Err() short-circuit.
	fake := newFakePubCh(true)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	pub.rateLimiter = newRateLimiter(1) // 1s interval, burst 1
	pub.retryPolicy = &RetryPolicy{Retries: 5, Min: time.Second, Max: time.Second, Jitter: JitterNone}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	v := testPayload{Value: "x"}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &v})) // consume the burst token

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := pub.Publish(ctx, Message[testPayload]{Body: &v}) // blocks ~1s on the token; ctx fires at 20ms
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRateLimited)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 500*time.Millisecond,
		"must return when ctx fires, not retry/back off for the full 1s+ interval")

	pm.mu.Lock()
	defer pm.mu.Unlock()
	assert.Empty(t, pm.retries, "a ctx-cancelled rate-limit wait must not record a retry")
	assert.Len(t, pm.rateLimited, 1, "the throttled-then-cancelled attempt still counts as rate-limited once")
}
