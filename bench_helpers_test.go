//go:build bench

// Package warren_test (bench tag) holds the throughput benchmark suite (T44b /
// SPEC §9). The benchmarks run against a real broker (AMQP_TEST_URL, the same
// pinned broker the integration lane provisions) and are exercised on the
// release-candidate / nightly bench lane, never on every PR.
//
// Lens-09 PC-03/04/05: every benchmark pins and reports the payload size
// (benchPayloadBytes via b.SetBytes) and states BOTH a classic and a quorum
// number — quorum's majority Raft commit raises confirm latency materially, so a
// throughput figure without the queue type is uninterpretable. Lens-10 TV-04:
// the absolute targets (>=30k single-conn, >=100k multi-conn, batch >=5x) are
// release-tag targets on stated reference hardware, NOT CI gates — author-laptop
// / shared-runner numbers can never gate. The Go benchmarks therefore MEASURE and
// REPORT (msg/s as a custom metric); the relative regression gate lives in
// bench.yml.
package warren_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren"
)

// benchPayloadBytes is the pinned logical payload size reported via b.SetBytes so
// the MB/s figure is comparable across runs. 256 B models a small JSON event;
// vary deliberately in a dedicated change, not silently.
const benchPayloadBytes = 256

// benchPayload is the typed message body. Data is filled to benchPayloadBytes so
// the on-wire size is dominated by the pinned payload rather than JSON framing.
type benchPayload struct {
	Data string `json:"data"`
}

func newBenchPayload() *benchPayload {
	return &benchPayload{Data: strings.Repeat("x", benchPayloadBytes)}
}

// benchURL returns the broker URL or skips the benchmark when unset. Benchmarks
// are not correctness gates, so a missing broker skips (the bench lane always
// sets AMQP_TEST_URL and guards that benchmarks actually ran).
func benchURL(b *testing.B) string {
	b.Helper()
	u := os.Getenv("AMQP_TEST_URL")
	if u == "" {
		b.Skip("AMQP_TEST_URL not set; skipping throughput benchmark")
	}
	return u
}

// benchDial dials with confirms-friendly defaults plus any extra options, and
// registers cleanup.
func benchDial(b *testing.B, url string, opts ...warren.Option) *warren.Connection {
	b.Helper()
	all := append([]warren.Option{warren.WithAddr(url)}, opts...)
	conn, err := warren.Dial(context.Background(), all...)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	return conn
}

// queueKind names the queue implementation under test so each benchmark reports
// a classic and a quorum number side by side.
type queueKind struct {
	name      string
	queueType warren.QueueType
}

var benchQueueKinds = []queueKind{
	{name: "classic", queueType: warren.QueueTypeClassic},
	{name: "quorum", queueType: warren.QueueTypeQuorum},
}

// declareBenchTopology declares a durable exchange + queue for the given kind and
// returns their names. The queue is durable and non-auto-delete (quorum requires
// durable; both are explicitly dropped on cleanup so a benchmark re-run does not
// hit a redeclare mismatch).
func declareBenchTopology(b *testing.B, conn *warren.Connection, kind queueKind) (exchange, queue string) {
	b.Helper()
	exchange = "warren.bench.ex." + kind.name
	queue = "warren.bench.q." + kind.name

	url := os.Getenv("AMQP_TEST_URL")
	dropBenchTopology(url, exchange, queue) // clear any leftover from a prior crashed run

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: queue, Durable: true, Type: kind.queueType}},
		Bindings:  []warren.Binding{{Exchange: exchange, Queue: queue, RoutingKey: "bench.#"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := topo.Declare(ctx, conn); err != nil {
		b.Fatalf("declare %s topology: %v", kind.name, err)
	}
	b.Cleanup(func() { dropBenchTopology(url, exchange, queue) })
	return exchange, queue
}

// reportMsgPerSec reports n messages over the benchmark's measured time as a
// msg/s custom metric, the headline figure the §9 targets are stated in.
func reportMsgPerSec(b *testing.B, n int) {
	b.Helper()
	secs := b.Elapsed().Seconds()
	if secs > 0 {
		b.ReportMetric(float64(n)/secs, "msg/s")
	}
}

// dropBenchTopology deletes the durable bench queue + exchange via a raw AMQP
// connection; best-effort, silent on error.
func dropBenchTopology(url, exchange, queue string) {
	if url == "" {
		return
	}
	rc, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rc.Close() //nolint:errcheck // best-effort cleanup
	ch, err := rc.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck // best-effort cleanup
	_, _ = ch.QueueDelete(queue, false, false, false)
	_ = ch.ExchangeDelete(exchange, false, false)
}
