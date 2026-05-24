// Package main demonstrates how to publish messages in batches using the
// Warren library's PublishBatch — a single always-all pipeline that preserves
// per-channel message ordering and returns a []PublishResult slice.
//
// # What this example demonstrates
//
//   - Declaring AMQP topology (exchange + queue + binding) via Topology.Declare
//   - Building a typed Publisher[Order] with PublisherFor
//   - Publishing 1000 messages atomically via PublishBatch with ErrBatchTooLarge guard
//   - Interpreting []PublishResult to detect per-message errors
//   - Attempting a batch of 2000 against default PublishBatchMaxSize=1024 to
//     demonstrate the ErrBatchTooLarge guard (immediate return, no broker work)
//
// # Important constraints
//
// PublishRetry does NOT apply to PublishBatch. If a channel closes mid-batch,
// the affected messages surface ErrChannelClosed in their PublishResult.Err —
// it is the caller's responsibility to retry the affected subset (using unique
// MessageID values for deduplication, since the broker may or may not have
// received the message before the channel closed). Consumers of these messages
// MUST implement idempotent processing keyed on MessageID; see
// examples/idempotent_consume/ for a reference pattern.
//
// # How to run
//
//	go run ./examples/batch_publish
//
// # Environment variables
//
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// # Topology side-effects on the broker
//
//   - Creates exchange "warren.examples.batch" (direct, non-durable, auto-delete)
//   - Creates queue "warren.examples.batch.orders" (non-durable, auto-delete)
//   - Binds queue to exchange with routing key "batch.order"
//
// The example exits 0 when all 1000 messages are confirmed by the broker.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"context"

	"github.com/brunomvsouza/warren"
)

// Order is the payload type for this example.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

const (
	batchExchange   = "warren.examples.batch"
	batchQueue      = "warren.examples.batch.orders"
	batchRoutingKey = "batch.order"

	exampleTimeout = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("batch_publish example failed: %v", err)
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
				Name:       batchExchange,
				Kind:       warren.ExchangeDirect,
				Durable:    false,
				AutoDelete: true,
			},
		},
		Queues: []warren.Queue{
			{Name: batchQueue, Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: batchExchange, Queue: batchQueue, RoutingKey: batchRoutingKey},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared")

	pub, err := warren.PublisherFor[Order](conn).
		Exchange(batchExchange).
		RoutingKey(batchRoutingKey).
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

	// — Step 1: demonstrate ErrBatchTooLarge ——————————————————————————————————
	// A batch of 2000 against the default PublishBatchMaxSize=1024 must return
	// ErrBatchTooLarge immediately with a nil result slice and no broker work.
	oversized := make([]warren.Message[Order], 2000)
	for i := range oversized {
		oversized[i] = warren.Message[Order]{Body: &Order{ID: fmt.Sprintf("large-%d", i), Amount: i}}
	}
	results, err := pub.PublishBatch(ctx, oversized)
	if !errors.Is(err, warren.ErrBatchTooLarge) {
		return fmt.Errorf("expected ErrBatchTooLarge for 2000-message batch, got: %w", err)
	}
	if results != nil {
		return fmt.Errorf("results must be nil on ErrBatchTooLarge, got len=%d", len(results))
	}
	log.Printf("ErrBatchTooLarge guard OK: 2000-message batch rejected immediately (max=%d)", 1024)

	// — Step 2: PublishBatch of 1000 messages — always-all ——————————————————
	const total = 1000
	msgs := make([]warren.Message[Order], total)
	for i := range msgs {
		msgs[i] = warren.Message[Order]{
			Body: &Order{ID: fmt.Sprintf("ord-%04d", i), Amount: i + 1},
		}
	}

	log.Printf("publishing %d messages via PublishBatch...", total)
	start := time.Now()
	results, err = pub.PublishBatch(ctx, msgs)
	elapsed := time.Since(start)

	if err != nil {
		return fmt.Errorf("PublishBatch failed: %w", err)
	}

	var successCount int
	for i, r := range results {
		if r.Err != nil {
			log.Printf("  result[%d].Err = %v", i, r.Err)
		} else {
			successCount++
		}
	}

	log.Printf("PublishBatch complete: %d/%d confirmed in %s (%.0f msg/s)",
		successCount, total, elapsed.Round(time.Millisecond),
		float64(successCount)/elapsed.Seconds())

	if successCount != total {
		return fmt.Errorf("expected %d confirmed, got %d", total, successCount)
	}

	log.Println("batch_publish example complete — all 1000 messages confirmed")
	return nil
}
