//go:build bench

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/brunomvsouza/warren"
)

// BenchmarkPublishConfirmed measures single-publisher-connection confirmed
// publish throughput. §9 release-tag target on reference hardware: >=30k msg/s
// (classic). The quorum number is reported alongside and is expected to be lower
// (majority Raft commit on every confirm) — both are stated, neither gates CI.
func BenchmarkPublishConfirmed(b *testing.B) {
	for _, kind := range benchQueueKinds {
		b.Run(kind.name, func(b *testing.B) {
			url := benchURL(b)
			conn := benchDial(b, url, warren.WithPublisherConnections(1))
			exchange, _ := declareBenchTopology(b, conn, kind)

			pub, err := warren.PublisherFor[benchPayload](conn).
				Exchange(exchange).
				RoutingKey("bench.created").
				ConfirmTimeout(10 * time.Second).
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
			ctx := context.Background()

			b.SetBytes(benchPayloadBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := pub.Publish(ctx, warren.Message[benchPayload]{Body: payload}); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}
			b.StopTimer()
			reportMsgPerSec(b, b.N)
		})
	}
}
