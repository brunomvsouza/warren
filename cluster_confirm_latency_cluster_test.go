//go:build cluster

package warren_test

// Quorum publisher-confirm latency under Raft replication (Phase 9.5 / T166f;
// SPEC §9 confirm latency + Lens-13 LT-07 measurement-under-load tail clip).
//
// A pool of publishers streams confirmed messages to a QUORUM queue while the
// built-in Prometheus publisher_publish_seconds histogram records each publish's
// end-to-end latency — which INCLUDES the wait for the broker confirm
// (publisher.go records time.Since(start) AFTER tracker.Wait returns), so on a
// quorum queue it is the 3-node Raft majority-commit latency, the headline number
// a single-node broker cannot produce. We report p50/p99/p999.
//
// The point of the campaign — the LT-07 finding — is the TAIL. The publish path is
// bounded by the 30 s ConfirmTimeout (+ the R10-8 reconnect-barrier wait on top),
// so a worst-case confirm can take tens of seconds. The histogram must be able to
// represent that tail in a FINITE bucket rather than dumping it into the implicit
// +Inf overflow, where it would be invisible to histogram_quantile and every
// percentile would read "+Inf". The default buckets do not cover it, so this
// campaign OVERRIDES them via the buckets argument of NewPrometheusPublisherMetrics
// (the SPEC's "WithLatencyBuckets" override) with a list whose top finite boundary
// sits PAST the ConfirmTimeout.
//
// Unit note (coordinates with the T71/T169 measurement theme): the histogram is
// named publisher_publish_seconds and RecordPublish observes d.Seconds(), so the
// recorded unit is SECONDS and the bucket boundaries below are second-valued. SPEC
// §9 documents the default range in "ms" ([0.5 … 5000]); reconciling that wording
// with the seconds-valued observation is owned by T71/T169 (the default-bucket
// fit), not this campaign — here we simply pick second-valued buckets that bound
// the real observed range INCLUDING the 30 s tail, which is all "the tail is not
// clipped to +Inf" requires.
//
// Why this needs a cluster: the latency under test is the Raft majority commit
// across three nodes; on a single node there is no replication to wait for, so the
// quorum-confirm latency this measures is unobservable below three nodes.

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/internal/amqptest"
	"github.com/brunomvsouza/warren/metrics"
)

// confirmLatencyConfirmTimeout is the publish path's hard upper bound — the
// ConfirmTimeout the publisher is built with. The campaign's bucket list is sized
// against this so the top finite bucket provably exceeds it: a confirmed publish
// cannot take longer than this (plus the bounded reconnect-barrier wait, which is
// not exercised here since no node dies), so a top finite bucket above it captures
// the entire possible latency range with room to spare.
const confirmLatencyConfirmTimeout = 30 * time.Second

// confirmLatencyBucketsSeconds overrides the default publish-latency histogram
// buckets (SPEC §9's WithLatencyBuckets override, threaded as the buckets argument
// of NewPrometheusPublisherMetrics). Values are SECONDS (RecordPublish observes
// d.Seconds()). The low end has fine resolution for healthy quorum commits
// (sub-millisecond to tens of milliseconds); the high end extends to 35 s — PAST
// the 30 s ConfirmTimeout — so even a worst-case confirm lands in a finite bucket
// instead of the implicit +Inf overflow. The 5 s of headroom over the timeout
// absorbs channel-pool acquisition + goroutine scheduling under the concurrent
// load (a confirmed publish's recorded latency is time.Since(start), spanning
// both, not just the confirm wait); on a healthy cluster this is comfortable
// (observed p999 ≈ tens of ms). The default buckets top out far below
// this in seconds terms (their largest boundary is meant as 5000 ms = 5 s), which
// is exactly the tail clip LT-07 flags; this list fixes it for the measurement.
var confirmLatencyBucketsSeconds = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 20, 30, 35,
}

// The publish-latency histogram readback helpers — cumulativeBucket,
// publishLatencyBuckets, quantileFromBuckets, samplesAbove — are pure math over
// flattened Prometheus buckets and live in the un-tagged latency_buckets_test.go so
// they are exercised on the fast lane (VG-6), not only behind the `cluster` tag.

// TestClusterQuorumConfirmLatency_TailNotClipped_cluster streams confirmed publishes
// to a quorum queue under concurrent load, then reads back the publish-latency
// histogram and asserts:
//   - a non-vacuous number of confirmed samples were recorded (real load happened);
//   - NO observation fell into the implicit +Inf bucket — the largest finite
//     bucket's cumulative count equals the total sample count (the tail is captured);
//   - the top finite bucket boundary exceeds the ConfirmTimeout, so by construction
//     no confirmed publish (capped at that timeout) can ever clip to +Inf;
//   - p50/p99/p999 are finite (below the top boundary) — i.e. real numbers a
//     dashboard can chart, not "+Inf".
func TestClusterQuorumConfirmLatency_TailNotClipped_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	const queue = "test.cluster.confirm.latency"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean slate + restore: a prior run may have left the durable quorum queue.
	deleteQuorumQueueCluster(nodes[0], queue)
	t.Cleanup(func() { deleteQuorumQueueCluster(nodes[0], queue) })

	conn, err := warren.Dial(ctx,
		warren.WithAddrs([]string{nodes[0], nodes[1], nodes[2]}),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(1),
		warren.WithChannelPoolSize(8),
		warren.WithReconnectBackoff(clusterFastBackoff),
	)
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// Durable quorum queue spanning all three members; default-exchange routed
	// (routing key == queue name), so there is nothing else to clean up.
	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes (3-node majority commit)")

	// Prometheus publisher metrics with the OVERRIDDEN, tail-covering buckets.
	reg := prometheus.NewRegistry()
	pm, err := metrics.NewPrometheusPublisherMetrics(reg, confirmLatencyBucketsSeconds)
	require.NoError(t, err)

	pub, err := warren.PublisherFor[clusterFailoverMsg](conn).
		RoutingKey(queue). // default exchange "" → route straight to the quorum queue
		ConfirmTimeout(confirmLatencyConfirmTimeout).
		PublishRetry(clusterPublishRetry).
		Metrics(pm).
		Build()
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = pub.Close(closeCtx)
	}()

	// — Concurrent confirmed load —————————————————————————————————————————————
	// Several publisher goroutines share the publisher (and its 2-connection pool),
	// so confirms queue behind one another under sustained pressure — load, not a
	// single quiescent round-trip. Each confirmed (nil) publish is one histogram
	// observation under outcome="success".
	const (
		loadPublishers = 6
		perPublisher   = 300 // 1800 attempted; the success floor below stays well under this
		successFloor   = 1000
	)
	// quantileTopGuard is the top finite bucket boundary; p50/p99/p999 must fall
	// strictly below it (a clipped histogram pushes the high quantiles to +Inf).
	// Derived from the bucket list so it can never drift from the configured top.
	quantileTopGuard := confirmLatencyBucketsSeconds[len(confirmLatencyBucketsSeconds)-1]

	var (
		mu         sync.Mutex
		successes  int
		transient  int
		unexpected []error
		wg         sync.WaitGroup
	)
	for g := 0; g < loadPublishers; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				if ctx.Err() != nil {
					return
				}
				seq := worker*perPublisher + i
				id := fmt.Sprintf("confirm-latency-%d", seq)
				switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{
					Body:      &clusterFailoverMsg{Seq: seq},
					MessageID: id,
				}); {
				case perr == nil:
					mu.Lock()
					successes++
					mu.Unlock()
				case isTolerableFailoverErr(perr):
					// No node is killed here, so a transient reconnect/confirm gap is rare;
					// tolerate (and count) it without failing — it is recorded under
					// outcome="error", never in the success histogram we assert on.
					mu.Lock()
					transient++
					mu.Unlock()
				default:
					mu.Lock()
					unexpected = append(unexpected, fmt.Errorf("seq=%d: %w", seq, perr))
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	gotSuccess, gotTransient, surface := successes, transient, append([]error(nil), unexpected...)
	mu.Unlock()

	require.Empty(t, surface,
		"publishes failed on a healthy cluster with errors that are not transient reconnect/confirm gaps: %v", surface)
	require.GreaterOrEqual(t, gotSuccess, successFloor,
		"too few confirmed publishes to measure latency meaningfully (got %d, want ≥ %d; transient=%d)",
		gotSuccess, successFloor, gotTransient)

	// — Read the histogram back and assert the tail is captured ————————————————
	buckets, sampleCount := publishLatencyBuckets(t, reg, "success")
	require.NotEmpty(t, buckets, "publisher_publish_seconds{outcome=success} histogram must be present")
	require.Equal(t, uint64(gotSuccess), sampleCount,
		"every confirmed publish must be one histogram sample")

	// Structural guarantee: the top finite bucket sits past the ConfirmTimeout, so a
	// confirmed publish (bounded by that timeout) CANNOT land in +Inf by construction.
	top := buckets[len(buckets)-1]
	assert.Greater(t, top.upperBound, confirmLatencyConfirmTimeout.Seconds(),
		"the top finite bucket (%.0fs) must exceed the ConfirmTimeout (%s) so the tail cannot clip to +Inf",
		top.upperBound, confirmLatencyConfirmTimeout)

	// Empirical guarantee: nothing actually clipped — the largest finite bucket's
	// cumulative count equals the total. If any sample exceeded every finite
	// boundary it would live only in the implicit +Inf bucket and this would fail.
	assert.Equal(t, sampleCount, top.cumulative,
		"no observation may fall into the implicit +Inf bucket (largest finite bucket holds %d of %d samples)",
		top.cumulative, sampleCount)

	// Report the tail percentiles, and assert they are FINITE (a clipped histogram
	// would yield +Inf for the high quantiles — the failure mode under test).
	p50 := quantileFromBuckets(buckets, sampleCount, 0.50)
	p99 := quantileFromBuckets(buckets, sampleCount, 0.99)
	p999 := quantileFromBuckets(buckets, sampleCount, 0.999)
	for name, v := range map[string]float64{"p50": p50, "p99": p99, "p999": p999} {
		assert.Falsef(t, math.IsInf(v, 1), "%s read as +Inf — the tail was clipped", name)
		assert.Lessf(t, v, quantileTopGuard, "%s (%.4gs) must fall within the finite bucket range", name, v)
	}

	t.Logf("quorum confirm latency (3-node Raft majority commit, n=%d confirmed, transient=%d): "+
		"p50=%.3fms p99=%.3fms p999=%.3fms — tail captured, no +Inf clip",
		sampleCount, gotTransient, p50*1e3, p99*1e3, p999*1e3)
}
