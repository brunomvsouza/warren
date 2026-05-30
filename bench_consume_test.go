//go:build bench

package warren_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/brunomvsouza/warren"
)

// BenchmarkConsume measures end-to-end consume throughput: a background producer
// keeps the queue full via batch publishes while the timed loop counts handler
// invocations. §9 release-tag target on reference hardware: >=30k msg/s
// (classic). Reported for a classic and a quorum queue.
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
			received := make(chan struct{}, 1<<16)
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
				consumeErr <- consumer.Consume(conCtx, func(_ context.Context, _ benchPayload) error {
					received <- struct{}{}
					return nil
				})
			}()

			// Warm up: wait for the first delivery so the producer is ahead before
			// timing starts.
			select {
			case <-received:
			case <-time.After(30 * time.Second):
				stopCon()
				b.Fatal("no deliveries within warm-up window")
			}

			b.SetBytes(benchPayloadBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				<-received
			}
			b.StopTimer()
			reportMsgPerSec(b, b.N)

			stopCon()
			if cerr := <-consumeErr; cerr != nil && !errors.Is(cerr, context.Canceled) {
				b.Errorf("consume: %v", cerr)
			}
		})
	}
}
