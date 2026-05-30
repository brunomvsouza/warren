//go:build bench

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/brunomvsouza/warren"
)

// benchBatchSize is the pinned batch size. PublishBatch amortizes the
// confirm round-trip across the whole batch, so its per-message throughput should
// far exceed serial Publish once RTT dominates (SPEC §9 release-tag target:
// PublishBatch >=5x Publish, measured on the same reference hardware — the
// multiple is the RTT-decoupled scale path, PC-05).
const benchBatchSize = 100

// BenchmarkPublishBatch measures confirmed batch publish throughput in msg/s
// (b.N batches x benchBatchSize messages over the measured time), reported for a
// classic and a quorum queue.
func BenchmarkPublishBatch(b *testing.B) {
	for _, kind := range benchQueueKinds {
		b.Run(kind.name, func(b *testing.B) {
			url := benchURL(b)
			conn := benchDial(b, url, warren.WithPublisherConnections(1))
			exchange, _ := declareBenchTopology(b, conn, kind)

			pub, err := warren.PublisherFor[benchPayload](conn).
				Exchange(exchange).
				RoutingKey("bench.created").
				ConfirmTimeout(30 * time.Second).
				PublishBatchMaxSize(benchBatchSize).
				Build()
			if err != nil {
				b.Fatalf("build publisher: %v", err)
			}
			b.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = pub.Close(ctx)
			})

			payload := newBenchPayload()
			batch := make([]warren.Message[benchPayload], benchBatchSize)
			for i := range batch {
				batch[i] = warren.Message[benchPayload]{Body: payload}
			}
			ctx := context.Background()

			b.SetBytes(benchPayloadBytes * benchBatchSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := pub.PublishBatch(ctx, batch)
				if err != nil {
					b.Fatalf("publish batch: %v", err)
				}
				for j := range results {
					if results[j].Err != nil {
						b.Fatalf("batch message %d: %v", j, results[j].Err)
					}
				}
			}
			b.StopTimer()
			reportMsgPerSec(b, b.N*benchBatchSize)
		})
	}
}
