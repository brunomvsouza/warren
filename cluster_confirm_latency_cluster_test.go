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

// confirmLatencyConfirmTimeout is the all-nodes-up baseline's publish-path hard
// upper bound — the ConfirmTimeout it builds the publisher with. The campaign's
// bucket list is sized against this so the top finite bucket provably exceeds it: a
// confirmed publish cannot take longer than this (plus the bounded reconnect-barrier
// wait, which the partition-tail sibling below DOES exercise), so a top finite bucket
// above it captures the entire possible latency range with room to spare.
const confirmLatencyConfirmTimeout = 30 * time.Second

// partitionTailConfirmTimeout is the partition-tail sibling's per-attempt confirm
// cap. It bounds only the confirm wait (tracker.Wait), NOT the reconnect-barrier wait
// the cut drives (waitBarrier is taken before the confirm and is uncapped by it), so
// it can stay well under the 35 s top finite bucket while a few seconds of barrier
// wait is what populates the tail. A confirmed publish that paid the barrier wait
// plus a fast post-heal confirm therefore lands comfortably below the top bucket,
// keeping the success series' "no +Inf clip" guarantee true by construction.
const partitionTailConfirmTimeout = 15 * time.Second

// tailThresholdSeconds is the boundary (a member of confirmLatencyBucketsSeconds, so
// samplesAbove is exact) separating healthy quorum confirms — sub-100 ms, p999 in the
// tens of ms — from a confirm that waited out the reconnect barrier during the
// partition. At least one recorded latency above it proves the tail was actually
// driven, the non-vacuity the all-nodes-up baseline cannot show.
const tailThresholdSeconds = 0.5

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
		perPublisher   = 300
		attempts       = loadPublishers * perPublisher // 1800
		// On a healthy all-nodes-up cluster nearly every publish confirms; require a
		// solid majority so the histogram is measured under real load. Derived from
		// attempts (not a bare literal) so a future loadPublishers/perPublisher bump
		// cannot silently invert the floor by leaving it above the attempt count.
		successFloor = attempts / 2 // 900
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
	require.Equal(t, quantileTopGuard, top.upperBound,
		"observed top finite bucket must equal the configured top bucket (%.0fs) — the readback and the bucket config must not drift apart",
		quantileTopGuard)
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

// TestClusterQuorumConfirmLatency_PartitionTail_cluster drives the SLOW end of the
// confirm-latency distribution that the all-nodes-up baseline cannot reach. Under
// streaming confirmed load it briefly CUTS the client's connectivity to every node
// (Toxiproxy disable on all three node proxies) and then HEALS it. Each in-flight
// publish attempt blocks in the reconnect barrier (publishOnce's waitBarrier, taken
// after start and uncapped by ConfirmTimeout) for the whole cut, so once the barrier
// clears its recorded latency (time.Since(start)) spans the cut: a deterministic
// multi-second confirm that lands under outcome="success" once it confirms post-heal.
//
// Cutting only the client AMQP proxies — not the inter-node 25672 distribution —
// isolates the CLIENT without partitioning the cluster, so the tail is the publish
// path's bounded reconnect-barrier wait (named in confirmLatencyConfirmTimeout's doc),
// reproducible BY CONSTRUCTION from the cut duration. This is deliberately not a clean
// leader kill: with a healthy majority the Raft re-election completes in ~1-2 s and no
// single publish ATTEMPT ever accumulates a multi-second latency (publishOnce records
// per attempt, and the fast retries after a kill each record a short sample), so a
// kill cannot reliably populate the tail buckets — a cut of a controlled duration can.
//
// Asserts:
//   - the success series captures its tail in a FINITE bucket (no +Inf clip) — true by
//     construction since the barrier wait plus a fast post-heal confirm stays below
//     the 35 s top finite bucket;
//   - at least one confirm/attempt was recorded above tailThresholdSeconds: the
//     non-vacuity the baseline lacks, proving the multi-second tail was actually
//     measured and landed in a finite bucket, not the implicit +Inf overflow.
//
// This is a confirm-LATENCY campaign, so it carries no consumer — zero-loss across a
// disruption is the quorum-failover / reconnect-storm campaigns' job.
func TestClusterQuorumConfirmLatency_PartitionTail_cluster(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodes := amqptest.ClusterNodes(t)
	require.GreaterOrEqual(t, len(nodes), 3,
		"cluster lane expects at least 3 nodes in WARREN_CLUSTER_NODES")
	amqptest.WaitClusterReady(t, len(nodes), 90*time.Second)

	const queue = "test.cluster.confirm.latency.partition"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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

	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: queue, Durable: true, Type: warren.QueueTypeQuorum}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn)) // redeclare on the reconnect barrier the heal triggers

	before := amqptest.QuorumLeader(t, queue)
	require.Equal(t, "quorum", before.Type)
	require.Len(t, before.Members, 3, "quorum queue must span all three cluster nodes (3-node majority commit)")

	// Prometheus publisher metrics with the OVERRIDDEN, tail-covering buckets.
	reg := prometheus.NewRegistry()
	pm, err := metrics.NewPrometheusPublisherMetrics(reg, confirmLatencyBucketsSeconds)
	require.NoError(t, err)

	pub, err := warren.PublisherFor[clusterFailoverMsg](conn).
		RoutingKey(queue). // default exchange "" → route straight to the quorum queue
		ConfirmTimeout(partitionTailConfirmTimeout).
		PublishRetry(clusterPublishRetry).
		Metrics(pm).
		Build()
	require.NoError(t, err)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = pub.Close(closeCtx)
	}()

	// Toxiproxy control plane. Re-enable every proxy no matter how the test exits, so
	// a failed assertion (or panic) can never leave the cluster cut for later tests.
	toxi := amqptest.NewToxiproxy(t)
	proxies := []string{"rmq0", "rmq1", "rmq2"}
	t.Cleanup(func() {
		for _, p := range proxies {
			_ = toxi.EnableProxy(p)
		}
	})

	// — Continuous confirmed load across the cut ———————————————————————————————
	// Several publisher goroutines keep many publishes in flight so the cut catches
	// each of them in waitBarrier. Each owns a disjoint MessageID space.
	const (
		loadPublishers   = 6
		warmupFloor      = 30 // confirmed BEFORE the cut (fast, all nodes reachable)
		postHealFloor    = 30 // additional confirmed AFTER the heal (barrier cleared)
		perWorkerIDSpace = 1_000_000
		cutDuration      = 4 * time.Second // > tailThresholdSeconds, < the 35 s top bucket
	)
	var (
		mu         sync.Mutex
		successes  int
		transient  int
		unexpected []error
		wg         sync.WaitGroup
	)
	pubDone := make(chan struct{})
	confirmedCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return successes
	}
	for g := 0; g < loadPublishers; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-pubDone:
					return
				case <-ctx.Done():
					return
				default:
				}
				seq := worker*perWorkerIDSpace + i
				id := fmt.Sprintf("confirm-latency-partition-%d", seq)
				switch perr := pub.Publish(ctx, warren.Message[clusterFailoverMsg]{
					Body:      &clusterFailoverMsg{Seq: seq},
					MessageID: id,
				}); {
				case perr == nil:
					mu.Lock()
					successes++
					mu.Unlock()
				case isTolerableFailoverErr(perr):
					// The cut tripped the reconnect barrier / confirm gap; recorded under
					// outcome="error" (or not at all if it errored in waitBarrier), never
					// asserted durable.
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

	// Warm up: a healthy batch confirmed before the cut (populates the low buckets).
	require.Eventually(t, func() bool { return confirmedCount() >= warmupFloor },
		60*time.Second, 50*time.Millisecond,
		"publisher must confirm a warmup batch before the partition")
	preCut := confirmedCount()

	// Cut every node's client proxy, hold so the in-flight attempts pile up in the
	// reconnect barrier, then heal. The cut window is what the tail measures.
	for _, p := range proxies {
		require.NoError(t, toxi.DisableProxy(p), "cut proxy %s", p)
	}
	time.Sleep(cutDuration)
	for _, p := range proxies {
		require.NoError(t, toxi.EnableProxy(p), "heal proxy %s", p)
	}

	// The publisher must resume confirming after the heal — proof the barrier cleared
	// and the blocked attempts drained, not wedged.
	require.Eventually(t, func() bool { return confirmedCount() >= preCut+postHealFloor },
		90*time.Second, 100*time.Millisecond,
		"publisher must resume confirming after the partition heals")

	// Stop the load and join before reading the histogram.
	close(pubDone)
	wg.Wait()

	mu.Lock()
	gotSuccess, gotTransient, surface := successes, transient, append([]error(nil), unexpected...)
	mu.Unlock()
	require.Empty(t, surface,
		"publishes failed with errors the partition does not explain: %v", surface)

	// — Read the histogram back ————————————————————————————————————————————————
	quantileTopGuard := confirmLatencyBucketsSeconds[len(confirmLatencyBucketsSeconds)-1]
	successBuckets, successCount := publishLatencyBuckets(t, reg, "success")
	errorBuckets, errorCount := publishLatencyBuckets(t, reg, "error")
	require.NotEmpty(t, successBuckets, "publisher_publish_seconds{outcome=success} histogram must be present")
	require.Equal(t, uint64(gotSuccess), successCount, "every confirmed publish must be one success sample")

	// Success series: no +Inf clip — a confirmed publish's attempt (barrier wait + a
	// fast post-heal confirm) stays below the top finite bucket by construction.
	sTop := successBuckets[len(successBuckets)-1]
	require.Equal(t, quantileTopGuard, sTop.upperBound,
		"observed top success bucket must equal the configured top bucket (%.0fs)", quantileTopGuard)
	assert.Greater(t, sTop.upperBound, partitionTailConfirmTimeout.Seconds(),
		"the top finite bucket (%.0fs) must exceed the confirm timeout (%s) so a confirmed publish cannot clip to +Inf",
		sTop.upperBound, partitionTailConfirmTimeout)
	assert.Equal(t, successCount, sTop.cumulative,
		"no confirmed publish may fall into the implicit +Inf bucket (largest finite bucket holds %d of %d)",
		sTop.cumulative, successCount)

	// Non-vacuity tail: the cut must have driven at least one confirm/attempt — under
	// either outcome — above the healthy boundary, captured in a finite bucket.
	successTail := samplesAbove(successBuckets, successCount, tailThresholdSeconds)
	errorTail := samplesAbove(errorBuckets, errorCount, tailThresholdSeconds)
	require.GreaterOrEqualf(t, successTail+errorTail, uint64(1),
		"the partition must drive at least one confirm/attempt above %.2gs (success-tail=%d error-tail=%d, errorCount=%d); "+
			"all-nodes-up confirms sit far below this, so a vacuous tail means the reconnect-barrier wait was never measured",
		tailThresholdSeconds, successTail, errorTail, errorCount)

	p50 := quantileFromBuckets(successBuckets, successCount, 0.50)
	p99 := quantileFromBuckets(successBuckets, successCount, 0.99)
	p999 := quantileFromBuckets(successBuckets, successCount, 0.999)
	assert.Falsef(t, math.IsInf(p99, 1), "success p99 read as +Inf — the tail was clipped")
	assert.Falsef(t, math.IsInf(p999, 1), "success p999 read as +Inf — the tail was clipped")
	t.Logf("partition-tail confirm latency: confirmed=%d transient=%d; success p50=%.1fms p99=%.1fms p999=%.1fms; "+
		"tail>%.2gs success=%d error=%d (errorCount=%d) — tail captured, no success +Inf clip",
		gotSuccess, gotTransient, p50*1e3, p99*1e3, p999*1e3, tailThresholdSeconds, successTail, errorTail, errorCount)
}
