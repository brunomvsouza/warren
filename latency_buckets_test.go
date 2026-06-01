package warren_test

// Fast-lane unit coverage for the publish-latency histogram readback used by the
// T166f quorum confirm-latency campaign (cluster_confirm_latency_cluster_test.go).
//
// These helpers — the +Inf-clip detector (largest finite bucket vs sample count),
// the PromQL-style quantile estimate, and the tail-sample counter — are pure math
// over flattened Prometheus buckets, with no broker dependency. They live in an
// UN-TAGGED test file (not behind the `cluster` tag) for the same reason
// lossByMessageID does in chaos_reconnect_loss_test.go: the cluster lane is never a
// per-PR gate (SPEC §10 D5), so logic that only ever compiled behind `cluster`
// would never be exercised on the fast lane. A bug in quantileFromBuckets — an
// off-by-one in the rank compare, or returning a finite bound instead of +Inf past
// the top bucket — would silently certify a CLIPPED histogram as healthy, the exact
// inverse of the property the campaign asserts. VG-6: a harness that cannot detect
// the failure it certifies must never be trusted. The self-tests below prove each
// detector fires, so the campaign's "no +Inf clip / tail captured" verdict rests on
// readback math that is checked on every push.

import (
	"math"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cumulativeBucket is one finite histogram bucket flattened out of the Prometheus
// dto so the quantile math below is dto-free and unit-testable in isolation:
// upperBound is the bucket's inclusive upper boundary (seconds), cumulative is the
// number of observations ≤ that boundary.
type cumulativeBucket struct {
	upperBound float64
	cumulative uint64
}

// selectPublishLatencyBuckets gathers reg and extracts the publisher_publish_seconds
// histogram for the given outcome label: its finite buckets (ascending by upper
// bound), the total sample count (which, per the Prometheus model, INCLUDES any
// observation that fell into the implicit +Inf overflow), and matched — how many
// series carried that outcome value. matched > 1 means an unanticipated extra label
// dimension produced more than one series for the same outcome, so the readback
// would silently pick just one; the caller asserts matched ≤ 1. matched == 0 (with
// nil buckets, 0 count) means the series is absent. Pure over the registry — no
// *testing.T — so every branch is exercisable on the fast lane.
func selectPublishLatencyBuckets(reg *prometheus.Registry, outcome string) (buckets []cumulativeBucket, count uint64, matched int, err error) {
	mfs, err := reg.Gather()
	if err != nil {
		return nil, 0, 0, err
	}
	for _, mf := range mfs {
		if mf.GetName() != "publisher_publish_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var matches bool
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "outcome" && lp.GetValue() == outcome {
					matches = true
					break
				}
			}
			if !matches {
				continue
			}
			matched++
			h := m.GetHistogram()
			out := make([]cumulativeBucket, 0, len(h.GetBucket()))
			for _, b := range h.GetBucket() {
				out = append(out, cumulativeBucket{upperBound: b.GetUpperBound(), cumulative: b.GetCumulativeCount()})
			}
			// Prometheus emits buckets ascending, but sort defensively so the quantile
			// walk and the "largest finite bucket" read do not depend on emission order.
			sort.Slice(out, func(i, j int) bool { return out[i].upperBound < out[j].upperBound })
			buckets, count = out, h.GetSampleCount()
		}
	}
	return buckets, count, matched, nil
}

// publishLatencyBuckets gathers the publisher_publish_seconds histogram for the
// given outcome label and returns its finite buckets (ascending) plus the total
// sample count. Comparing the largest finite bucket's cumulative count against this
// total is how the caller detects a +Inf clip. It fails the test if more than one
// series matches the outcome (an unexpected label dimension); returns (nil, 0) when
// the series is absent. The selection logic is in the dto-free, fast-lane-tested
// selectPublishLatencyBuckets; this is the thin *testing.T wrapper the campaign uses.
func publishLatencyBuckets(t *testing.T, reg *prometheus.Registry, outcome string) ([]cumulativeBucket, uint64) {
	t.Helper()
	buckets, count, matched, err := selectPublishLatencyBuckets(reg, outcome)
	require.NoError(t, err)
	// Exactly one series is expected for a given outcome; more than one means an
	// unanticipated label dimension was added and the readback would silently pick
	// just one of them. Zero is allowed (callers handle the absent-series case).
	require.LessOrEqualf(t, matched, 1,
		"expected at most one publisher_publish_seconds series for outcome=%q, got %d (unexpected label dimensions?)",
		outcome, matched)
	return buckets, count
}

// quantileFromBuckets estimates the q-quantile (q in [0,1]) from cumulative
// histogram buckets, using the same linear-interpolation-within-bucket rule as
// PromQL's histogram_quantile. Returns +Inf when the quantile's rank falls beyond
// the largest finite bucket (i.e. into the implicit +Inf overflow) — the signal a
// clipped histogram cannot represent its tail. Returns 0 for an empty histogram.
func quantileFromBuckets(buckets []cumulativeBucket, count uint64, q float64) float64 {
	if count == 0 || len(buckets) == 0 {
		return 0
	}
	rank := q * float64(count)
	var prevUpper float64
	var prevCum uint64
	for _, b := range buckets {
		if float64(b.cumulative) >= rank {
			span := float64(b.cumulative - prevCum)
			if span <= 0 {
				return b.upperBound
			}
			frac := (rank - float64(prevCum)) / span
			return prevUpper + frac*(b.upperBound-prevUpper)
		}
		prevUpper, prevCum = b.upperBound, b.cumulative
	}
	// Rank lies beyond the largest finite bucket → the tail is in +Inf.
	return math.Inf(1)
}

// samplesAbove returns the number of observations strictly greater than threshold,
// computed from cumulative buckets: total minus the cumulative count of the largest
// bucket whose upper bound is ≤ threshold. For an exact count, threshold should be a
// bucket boundary (otherwise samples in the gap between the boundary and threshold
// are counted as "above"). The node-down tail campaign uses it to prove a genuinely
// slow confirm landed in a finite high bucket — non-vacuity the all-nodes-up
// baseline cannot show. buckets must be ascending (selectPublishLatencyBuckets sorts).
func samplesAbove(buckets []cumulativeBucket, count uint64, threshold float64) uint64 {
	var atOrBelow uint64
	for _, b := range buckets {
		if b.upperBound <= threshold {
			atOrBelow = b.cumulative
		}
	}
	if atOrBelow > count {
		return 0 // defensive: a finite bucket can never hold more than the total
	}
	return count - atOrBelow
}

// --- quantileFromBuckets ---------------------------------------------------

func TestQuantileFromBuckets_emptyOrZeroCountReturnsZero(t *testing.T) {
	assert.Zero(t, quantileFromBuckets(nil, 0, 0.5))
	assert.Zero(t, quantileFromBuckets([]cumulativeBucket{{upperBound: 1, cumulative: 10}}, 0, 0.5))
	assert.Zero(t, quantileFromBuckets(nil, 10, 0.5))
}

func TestQuantileFromBuckets_interpolatesWithinBucket(t *testing.T) {
	// 10 samples, all in the (1,2] bucket. q=0.5 → rank 5 → halfway across the
	// bucket → 1 + 0.5*(2-1) = 1.5.
	buckets := []cumulativeBucket{{upperBound: 1, cumulative: 0}, {upperBound: 2, cumulative: 10}}
	assert.InDelta(t, 1.5, quantileFromBuckets(buckets, 10, 0.5), 1e-9)
	// q=0.9 → rank 9 → 1 + 0.9*(2-1) = 1.9.
	assert.InDelta(t, 1.9, quantileFromBuckets(buckets, 10, 0.9), 1e-9)
}

func TestQuantileFromBuckets_singleBucketInterpolatesFromZero(t *testing.T) {
	// One bucket [0,5], 10 samples. q=0.5 → rank 5 → 0 + 0.5*(5-0) = 2.5.
	buckets := []cumulativeBucket{{upperBound: 5, cumulative: 10}}
	assert.InDelta(t, 2.5, quantileFromBuckets(buckets, 10, 0.5), 1e-9)
}

func TestQuantileFromBuckets_zeroSpanReturnsUpperBound(t *testing.T) {
	// q=0 → rank 0; the first bucket already satisfies it with zero span below it,
	// so the estimate is that bucket's upper bound rather than a divide-by-zero.
	buckets := []cumulativeBucket{{upperBound: 1, cumulative: 0}, {upperBound: 2, cumulative: 10}}
	assert.Equal(t, 1.0, quantileFromBuckets(buckets, 10, 0))
}

// VG-6 detector: a histogram whose tail spilled into the implicit +Inf bucket
// (sample count exceeds the largest finite bucket's cumulative) MUST read +Inf for
// a high quantile. If this returned a finite number the campaign's "no +Inf clip"
// assertion would pass on a genuinely clipped histogram — the failure mode under test.
func TestQuantileFromBuckets_rankBeyondTopBucketReturnsInf(t *testing.T) {
	// 10 samples but the largest finite bucket only holds 8 → 2 live in +Inf.
	buckets := []cumulativeBucket{{upperBound: 1, cumulative: 5}, {upperBound: 2, cumulative: 8}}
	got := quantileFromBuckets(buckets, 10, 0.99) // rank 9.9 > 8
	assert.True(t, math.IsInf(got, 1), "rank past the top finite bucket must read +Inf, got %v", got)
	// A quantile that DOES fall within the finite buckets stays finite on the same data.
	assert.False(t, math.IsInf(quantileFromBuckets(buckets, 10, 0.5), 1),
		"a quantile within the finite range must not read +Inf")
}

// --- samplesAbove ----------------------------------------------------------

func TestSamplesAbove_countsStrictlyAboveBoundary(t *testing.T) {
	buckets := []cumulativeBucket{{upperBound: 0.5, cumulative: 7}, {upperBound: 1, cumulative: 9}, {upperBound: 2, cumulative: 10}}
	assert.Equal(t, uint64(3), samplesAbove(buckets, 10, 0.5), "3 samples sit above the 0.5 boundary")
	assert.Equal(t, uint64(10), samplesAbove(buckets, 10, 0.0001), "all samples sit above a sub-bucket threshold")
	assert.Equal(t, uint64(0), samplesAbove(buckets, 10, 5), "no samples sit above the top boundary")
}

// VG-6 detector: a single slow sample in a high bucket must be counted as exactly
// one above the healthy boundary — this is the non-vacuity signal the node-down
// tail campaign relies on (it proves a real tens-of-seconds confirm was recorded).
func TestSamplesAbove_detectsLoneTailSample(t *testing.T) {
	buckets := []cumulativeBucket{{upperBound: 0.5, cumulative: 9}, {upperBound: 20, cumulative: 9}, {upperBound: 35, cumulative: 10}}
	assert.Equal(t, uint64(1), samplesAbove(buckets, 10, 0.5), "exactly the one slow sample must count as above 0.5s")
}

func TestSamplesAbove_overflowGuard(t *testing.T) {
	// A threshold at/above the top finite bucket reports the +Inf overflow (count
	// minus the top bucket's cumulative); never negative when cumulative == count.
	buckets := []cumulativeBucket{{upperBound: 1, cumulative: 8}}
	assert.Equal(t, uint64(2), samplesAbove(buckets, 10, 1), "two samples overflowed past the only finite bucket")
}

// --- selectPublishLatencyBuckets / publishLatencyBuckets -------------------

// newLatencyHistogram registers a publisher_publish_seconds histogram (the name the
// readback filters on) with the given label set in a fresh registry, mirroring how
// the metrics package exposes it, so the readback can be exercised without a broker.
func newLatencyHistogram(t *testing.T, labels ...string) (*prometheus.Registry, *prometheus.HistogramVec) {
	t.Helper()
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "publisher_publish_seconds",
		Buckets: []float64{0.005, 0.05, 0.5, 5},
	}, labels)
	require.NoError(t, reg.Register(h))
	return reg, h
}

func TestPublishLatencyBuckets_returnsAscendingBucketsAndCount(t *testing.T) {
	reg, h := newLatencyHistogram(t, "outcome")
	for _, v := range []float64{0.001, 0.06, 1.0} {
		h.WithLabelValues("success").Observe(v)
	}

	buckets, count := publishLatencyBuckets(t, reg, "success")
	require.NotEmpty(t, buckets)
	assert.Equal(t, uint64(3), count)
	assert.True(t, sort.SliceIsSorted(buckets, func(i, j int) bool { return buckets[i].upperBound < buckets[j].upperBound }),
		"buckets must be ascending by upper bound")
	// Cumulative counts are monotonic non-decreasing and the top finite bucket holds
	// every observation (nothing clipped on this in-range data).
	assert.Equal(t, count, buckets[len(buckets)-1].cumulative)
}

func TestPublishLatencyBuckets_absentSeriesReturnsZero(t *testing.T) {
	reg, h := newLatencyHistogram(t, "outcome")
	h.WithLabelValues("success").Observe(0.01) // only "success" exists

	buckets, count := publishLatencyBuckets(t, reg, "error")
	assert.Nil(t, buckets)
	assert.Zero(t, count)
}

// VG-6 detector: the matched>1 guard exists so an unexpected extra label dimension
// cannot make the readback silently pick one of several same-outcome series. Prove
// the guard's trigger condition is real — two series share outcome="success" when a
// second label varies — by asserting selectPublishLatencyBuckets reports matched==2
// (the wrapper would then fail its require.LessOrEqual). Tested on the pure selector
// because the wrapper FailNow-s on this input by design.
func TestSelectPublishLatencyBuckets_detectsExtraLabelDimension(t *testing.T) {
	reg, h := newLatencyHistogram(t, "outcome", "node")
	h.WithLabelValues("success", "a").Observe(0.01)
	h.WithLabelValues("success", "b").Observe(0.02)

	_, _, matched, err := selectPublishLatencyBuckets(reg, "success")
	require.NoError(t, err)
	assert.Equal(t, 2, matched, "two same-outcome series must be detected so the wrapper's matched≤1 guard fires")
}

func TestSelectPublishLatencyBuckets_absentRegistryMetricReturnsNoMatch(t *testing.T) {
	// A registry holding an unrelated metric must yield matched==0, not a false hit.
	reg := prometheus.NewRegistry()
	other := prometheus.NewCounter(prometheus.CounterOpts{Name: "unrelated_total"})
	require.NoError(t, reg.Register(other))
	other.Inc()

	buckets, count, matched, err := selectPublishLatencyBuckets(reg, "success")
	require.NoError(t, err)
	assert.Nil(t, buckets)
	assert.Zero(t, count)
	assert.Zero(t, matched)
}
