//go:build bench

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/brunomvsouza/warren"
)

// BenchmarkPublishConfirmedMultiConn measures confirmed publish throughput with
// the role-split pool fanned out across 4 publisher TCP connections and a 16-deep
// channel pool, driven concurrently (b.RunParallel) so the multiple sockets are
// actually exercised — a single serial publisher would bottleneck one socket
// (~50k msg/s per socket, SPEC §6.1). §9 release-tag target on reference
// hardware: >=100k msg/s (classic).
func BenchmarkPublishConfirmedMultiConn(b *testing.B) {
	for _, kind := range benchQueueKinds {
		b.Run(kind.name, func(b *testing.B) {
			url := benchURL(b)
			conn := benchDial(b, url,
				warren.WithPublisherConnections(4),
				warren.WithChannelPoolSize(16),
			)
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
			// Concurrency here is RunParallel's default p = GOMAXPROCS goroutines, so
			// the in-flight publish count — and thus the achievable msg/s — is bounded
			// by GOMAXPROCS, NOT by the 4 conns × 16-channel pool (64 channels) under
			// test. On a runner with GOMAXPROCS < 64 the sockets are deliberately
			// under-subscribed; raise GOMAXPROCS to push toward the pool ceiling. We do
			// NOT call b.SetParallelism to oversubscribe: more concurrent publishers
			// than pooled channels would surface ErrChannelPoolExhausted and fail the
			// bench rather than measure it. The >=100k release-tag target therefore
			// assumes the reference runner's core count, stated alongside the number.
			b.RunParallel(func(pb *testing.PB) {
				// Publisher.Publish is safe for concurrent use: each call acquires a
				// channel from the per-connection pool, fanning load across sockets.
				for pb.Next() {
					if err := pub.Publish(ctx, warren.Message[benchPayload]{Body: payload}); err != nil {
						b.Errorf("publish: %v", err)
						return
					}
				}
			})
			b.StopTimer()
			reportMsgPerSec(b, b.N)
		})
	}
}
