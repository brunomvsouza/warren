//go:build bench

package warren_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brunomvsouza/warren"
)

// BenchmarkConsume measures end-to-end consume throughput: a background producer
// keeps the queue full via batch publishes while the timed region counts handler
// invocations off an atomic counter. §9 release-tag target on reference hardware:
// >=30k msg/s (classic). Reported for a classic and a quorum queue.
//
// Correctness guard (ship review S2/C1): the handler is not a bare counter — it
// validates each delivered payload's integrity, and the run fails on any payload
// mismatch or non-cancel consumer error. A regression that corrupts deliveries or
// breaks decoding therefore fails the benchmark instead of silently inflating
// msg/s. (Per-message exactly-once accounting is out of scope for a throughput
// bench against an unbounded, unidentified producer stream; the conformance suite
// covers delivery semantics.)
//
// No-head-start measurement (ship review S2/C2): the timed region snapshots the
// atomic counter and waits until it advances by b.N. There is no in-process
// backlog channel that could be pre-filled during warm-up, so the figure cannot
// be inflated by a buffered head start the way a buffered-channel + best-effort
// drain design could (the old drain raced the handler goroutines).
func BenchmarkConsume(b *testing.B) {
	for _, kind := range benchQueueKinds {
		b.Run(kind.name, func(b *testing.B) {
			url := benchURL(b)
			conn := benchDial(b, url,
				warren.WithPublisherConnections(2),
				warren.WithConsumerConnections(2),
			)
			exchange, queue := declareBenchTopology(b, conn, kind)

			// — Background producer: keep the queue full ————————————————————————
			pub, err := warren.PublisherFor[benchPayload](conn).
				Exchange(exchange).
				RoutingKey("bench.created").
				ConfirmTimeout(30 * time.Second).
				PublishBatchMaxSize(256).
				Build()
			if err != nil {
				b.Fatalf("build publisher: %v", err)
			}
			payload := newBenchPayload()
			prodBatch := make([]warren.Message[benchPayload], 256)
			for i := range prodBatch {
				prodBatch[i] = warren.Message[benchPayload]{Body: payload}
			}
			prodCtx, stopProd := context.WithCancel(context.Background())
			var prodWG sync.WaitGroup
			prodWG.Add(1)
			go func() {
				defer prodWG.Done()
				for prodCtx.Err() == nil {
					// Best-effort fill; errors on cancel/timeout are ignored — the
					// consumer-side measurement is what matters.
					_, _ = pub.PublishBatch(prodCtx, prodBatch)
				}
			}()
			b.Cleanup(func() {
				stopProd()
				prodWG.Wait()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = pub.Close(ctx)
			})

			// — Consumer ————————————————————————————————————————————————————————
			var consumed, badPayloads int64
			consumer, err := warren.ConsumerFor[benchPayload](conn).
				Queue(queue).
				Concurrency(4).
				Prefetch(256).
				Build()
			if err != nil {
				b.Fatalf("build consumer: %v", err)
			}
			conCtx, stopCon := context.WithCancel(context.Background())
			consumeErr := make(chan error, 1)
			go func() {
				consumeErr <- consumer.Consume(conCtx, func(_ context.Context, p benchPayload) error {
					// Integrity guard: a corrupted/short payload signals a delivery or
					// decode regression the throughput number would otherwise hide.
					if len(p.Data) != benchPayloadBytes {
						atomic.AddInt64(&badPayloads, 1)
					}
					atomic.AddInt64(&consumed, 1)
					return nil
				})
			}()

			// Warm up: wait for the first delivery so the producer is ahead before
			// timing starts. Polled off the timed clock.
			warmUpDeadline := time.Now().Add(30 * time.Second)
			for atomic.LoadInt64(&consumed) == 0 {
				if time.Now().After(warmUpDeadline) {
					stopCon()
					b.Fatal("no deliveries within warm-up window")
				}
				time.Sleep(time.Millisecond)
			}

			// Timed region: measure how long the consumer takes to process b.N more
			// broker-paced deliveries, read off the atomic counter. The benchmark
			// goroutine only observes the counter (it never reads from a backlog
			// buffer), so the measured wall time is the consumer's steady-state
			// throughput; the <=100µs poll adds at most one interval to a
			// multi-second measurement (negligible).
			base := atomic.LoadInt64(&consumed)
			b.SetBytes(benchPayloadBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for atomic.LoadInt64(&consumed)-base < int64(b.N) {
				time.Sleep(100 * time.Microsecond)
			}
			b.StopTimer()
			reportMsgPerSec(b, b.N)

			stopCon()
			if cerr := <-consumeErr; cerr != nil && !errors.Is(cerr, context.Canceled) {
				b.Errorf("consume: %v", cerr)
			}
			// A throughput number is only trustworthy if the deliveries were
			// well-formed: fail rather than report an inflated msg/s over corrupt
			// deliveries.
			if bad := atomic.LoadInt64(&badPayloads); bad != 0 {
				b.Errorf("payload integrity: %d deliveries had an unexpected payload size", bad)
			}
		})
	}
}
