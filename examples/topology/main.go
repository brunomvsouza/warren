// Package main demonstrates topology declaration, idempotency, and reconnect
// redeclare using the Warren library.
//
// What this example demonstrates:
//   - Building a Topology with two exchanges, three queues, and four bindings
//   - Calling Topology.Declare and confirming idempotency on a second call
//   - Registering a reconnect hook via Topology.AttachTo
//   - Triggering a manual reconnect with (*Connection).ForceReconnect and
//     observing the topology being redeclared automatically
//
// How to run:
//
//	go run ./examples/topology
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.events" (topic, durable)
//   - Creates exchange "warren.examples.notify" (fanout, durable)
//   - Creates queues "warren.examples.orders", "warren.examples.payments",
//     "warren.examples.alerts" (all durable)
//   - Binds orders queue to events with routing key "order.#"
//   - Binds payments queue to events with routing key "payment.#"
//   - Binds alerts queue to notify (fanout, no routing key)
//   - Binds orders queue to notify (fanout, no routing key)
//
// The example exits 0 on success and non-zero on any error.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brunomvsouza/warren"
)

func main() {
	if err := run(); err != nil {
		log.Printf("topology example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	ctx := context.Background()

	reconnected := make(chan struct{}, 1)

	conn, err := warren.Dial(ctx,
		warren.WithAddr(url),
		warren.WithOnReconnect(func() {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{
				Name:    "warren.examples.events",
				Kind:    warren.ExchangeTopic,
				Durable: true,
			},
			{
				Name:    "warren.examples.notify",
				Kind:    warren.ExchangeFanout,
				Durable: true,
			},
		},
		Queues: []warren.Queue{
			{Name: "warren.examples.orders", Durable: true},
			{Name: "warren.examples.payments", Durable: true},
			{Name: "warren.examples.alerts", Durable: true},
		},
		Bindings: []warren.Binding{
			{Exchange: "warren.examples.events", Queue: "warren.examples.orders", RoutingKey: "order.#"},
			{Exchange: "warren.examples.events", Queue: "warren.examples.payments", RoutingKey: "payment.#"},
			{Exchange: "warren.examples.notify", Queue: "warren.examples.alerts"},
			{Exchange: "warren.examples.notify", Queue: "warren.examples.orders"},
		},
	}

	// First declare — creates exchanges, queues, and bindings on the broker.
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared")

	// Second declare — must return nil without mutating broker state.
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("idempotent re-declare: %w", err)
	}
	log.Println("topology re-declared (idempotent — no broker mutation)")

	// Register the topology as a reconnect hook so the barrier redeclares it
	// on every reconnect before publishers resume.
	if err := topo.AttachTo(conn); err != nil {
		return fmt.Errorf("attach topology: %w", err)
	}
	log.Println("topology attached to connection")

	// Trigger a manual reconnect cycle.
	if err := conn.ForceReconnect(); err != nil {
		return fmt.Errorf("force reconnect: %w", err)
	}

	// Wait for the reconnect barrier to complete (topology is redeclared inside
	// the barrier before WithOnReconnect fires).
	select {
	case <-reconnected:
		log.Println("topology re-declared (after reconnect)")
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timed out waiting for reconnect to complete")
	}

	return nil
}
