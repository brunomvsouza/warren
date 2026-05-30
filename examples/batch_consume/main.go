// Package main demonstrates how to consume AMQP messages in batches using the
// Warren library's BatchConsumer[M], with both size-based and timer-based flush
// triggers.
//
// What this example demonstrates:
//   - Building a BatchConsumer[Order] via BatchConsumerFor with Size(100) and
//     FlushAfter(500ms) — whichever trigger fires first dispatches the pending batch.
//   - Two-burst publishing: 200 messages published before the consumer starts
//     (triggers two size-based flushes of 100 each) followed by 50 messages
//     published after a deliberate pause (triggers a timer flush when the 500 ms
//     FlushAfter fires before the remaining 100-message threshold is reached).
//   - Auto-verdict: returning nil from the handler causes the framework to emit a
//     single basic.ack(multiple=true) for the highest delivery-tag in the batch
//     (one AMQP frame, not one per message).
//   - Manual batch acknowledgement via Batch.Ack is also accepted; the framework's
//     auto-verdict is suppressed (idempotent guard inside Batch[M]).
//
// Important constraints:
//
// PublishRetry does NOT apply to PublishBatch. If a channel closes mid-batch
// the caller must retry the affected messages using the per-result MessageID
// for at-least-once deduplication. Consumers of these messages MUST implement
// idempotent processing keyed on MessageID; see examples/idempotent_consume/
// for a reference pattern.
//
// How to run:
//
//	go run ./examples/batch_consume
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.batchconsume" (direct, non-durable, auto-delete)
//   - Creates queue "warren.examples.batchconsume.orders" (non-durable, auto-delete)
//   - Binds queue to exchange with routing key "batchconsume.order"
//
// The example exits 0 after all 250 messages are observed (≥3 batches: 2 by size,
// 1 by timer, and possibly extra batches if the burst arrives more slowly than the
// consumer processes).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/brunomvsouza/warren"
)

// Order is the payload type for this example.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

const (
	batchConsumeExchange   = "warren.examples.batchconsume"
	batchConsumeQueue      = "warren.examples.batchconsume.orders"
	batchConsumeRoutingKey = "batchconsume.order"

	batchSize  = 100
	flushAfter = 500 * time.Millisecond

	firstBurst  = 200 // size-based flushes (2 × 100)
	secondBurst = 50  // timer-based flush
	total       = firstBurst + secondBurst

	exampleTimeout = 90 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("batch_consume example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{
				Name:       batchConsumeExchange,
				Kind:       warren.ExchangeDirect,
				Durable:    false,
				AutoDelete: true,
			},
		},
		Queues: []warren.Queue{
			{Name: batchConsumeQueue, Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: batchConsumeExchange, Queue: batchConsumeQueue, RoutingKey: batchConsumeRoutingKey},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared")

	pub, err := warren.PublisherFor[Order](conn).
		Exchange(batchConsumeExchange).
		RoutingKey(batchConsumeRoutingKey).
		ConfirmTimeout(30 * time.Second).
		Build()
	if err != nil {
		return fmt.Errorf("build publisher: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = pub.Close(closeCtx)
	}()

	// Publish first burst of 200 messages (will trigger 2 size-based flushes of 100).
	log.Printf("publishing first burst of %d messages (expect flush-by-size)...", firstBurst)
	firstMsgs := make([]warren.Message[Order], firstBurst)
	for i := range firstMsgs {
		firstMsgs[i] = warren.Message[Order]{
			Body: &Order{ID: fmt.Sprintf("ord-%04d", i), Amount: i + 1},
		}
	}
	firstResults, err := pub.PublishBatch(ctx, firstMsgs)
	if err != nil {
		return fmt.Errorf("PublishBatch first burst: %w", err)
	}
	for i, r := range firstResults {
		if r.Err != nil {
			return fmt.Errorf("first burst result[%d].Err: %w", i, r.Err)
		}
	}
	log.Printf("first burst of %d messages published", firstBurst)

	// Build and start the batch consumer.
	bc, err := warren.BatchConsumerFor[Order](conn).
		Queue(batchConsumeQueue).
		Size(batchSize).
		FlushAfter(flushAfter).
		Prefetch(200).
		Build()
	if err != nil {
		return fmt.Errorf("build batch consumer: %w", err)
	}

	var (
		totalConsumed atomic.Int64
		batchNumber   atomic.Int64
		sizeFlushes   atomic.Int64
		timerFlushes  atomic.Int64
	)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- bc.Consume(consumerCtx, func(_ context.Context, batch *warren.Batch[Order]) error {
			n := batchNumber.Add(1)
			count := int64(len(batch.Deliveries()))
			totalConsumed.Add(count)

			flushType := "flush-by-size"
			if count < batchSize {
				flushType = "flush-by-timer"
				timerFlushes.Add(1)
			} else {
				sizeFlushes.Add(1)
			}

			log.Printf("batch #%d dispatched: %d messages (%s)", n, count, flushType)
			return nil
		})
	}()

	// Wait for the first burst (200 messages = 2 size-based batches).
	waitDeadline := time.NewTimer(30 * time.Second)
	defer waitDeadline.Stop()
	for totalConsumed.Load() < firstBurst {
		select {
		case <-time.After(50 * time.Millisecond):
		case <-waitDeadline.C:
			return fmt.Errorf("timed out waiting for first burst; consumed %d/%d",
				totalConsumed.Load(), firstBurst)
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for first burst")
		}
	}
	log.Printf("first burst consumed: %d messages in %d size-based batches",
		totalConsumed.Load(), sizeFlushes.Load())

	// Pause so the consumer's FlushAfter timer is in a clean state, then publish
	// the second burst (50 messages) which is less than batchSize=100. The
	// FlushAfter(500ms) timer must fire and dispatch this as a timer-based flush.
	time.Sleep(200 * time.Millisecond)
	log.Printf("publishing second burst of %d messages (expect flush-by-timer)...", secondBurst)
	secondMsgs := make([]warren.Message[Order], secondBurst)
	for i := range secondMsgs {
		secondMsgs[i] = warren.Message[Order]{
			Body: &Order{ID: fmt.Sprintf("ord-%04d", firstBurst+i), Amount: firstBurst + i + 1},
		}
	}
	secondResults, err := pub.PublishBatch(ctx, secondMsgs)
	if err != nil {
		return fmt.Errorf("PublishBatch second burst: %w", err)
	}
	for i, r := range secondResults {
		if r.Err != nil {
			return fmt.Errorf("second burst result[%d].Err: %w", i, r.Err)
		}
	}
	log.Printf("second burst of %d messages published", secondBurst)

	// Wait for the timer flush to dispatch the remaining 50 messages.
	// Allow up to 3× the FlushAfter duration for the timer to fire and the
	// handler to complete on a slow CI machine.
	waitDeadline.Reset(3 * flushAfter * 10)
	for totalConsumed.Load() < total {
		select {
		case <-time.After(50 * time.Millisecond):
		case <-waitDeadline.C:
			return fmt.Errorf("timed out waiting for timer flush; consumed %d/%d",
				totalConsumed.Load(), total)
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for timer flush")
		}
	}

	consumerCancel()
	if err := <-consumerErr; err != nil {
		return fmt.Errorf("consumer returned error: %w", err)
	}

	if timerFlushes.Load() < 1 {
		return fmt.Errorf("expected at least 1 timer-based flush, got %d", timerFlushes.Load())
	}
	if sizeFlushes.Load() < 2 {
		return fmt.Errorf("expected at least 2 size-based flushes, got %d", sizeFlushes.Load())
	}

	log.Printf("batch_consume example complete — %d messages in %d batches (%d flush-by-size, %d flush-by-timer)",
		totalConsumed.Load(), batchNumber.Load(), sizeFlushes.Load(), timerFlushes.Load())
	return nil
}
