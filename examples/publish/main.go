// Package main demonstrates how to publish a typed AMQP message using the
// Warren library with publisher confirms, mandatory routing, and retry policy.
//
// What this example demonstrates:
//   - Declaring AMQP topology (exchange + queue + binding) via Topology.Declare
//   - Building a typed Publisher[Order] with PublisherFor
//   - Publishing a single message with Mandatory flag and OnReturn callback
//   - ConfirmTimeout for broker-confirm deadline
//   - PublishRetry for automatic transient-error retries
//
// How to run:
//
//	go run ./examples/publish
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples" (topic, non-durable, auto-delete)
//   - Creates queue "warren.examples.orders" (non-durable, auto-delete)
//   - Binds queue to exchange with routing key "order.#"
//
// The example exits 0 on successful publish and non-zero on any error.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brunomvsouza/warren"
)

// Order is the payload type for this example.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Printf("publish example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
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
				Name:       "warren.examples",
				Kind:       warren.ExchangeTopic,
				Durable:    false,
				AutoDelete: true,
			},
		},
		Queues: []warren.Queue{
			{
				Name:       "warren.examples.orders",
				Durable:    false,
				AutoDelete: true,
			},
		},
		Bindings: []warren.Binding{
			{
				Exchange:   "warren.examples",
				Queue:      "warren.examples.orders",
				RoutingKey: "order.#",
			},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	var returned bool
	pub, err := warren.PublisherFor[Order](conn).
		Exchange("warren.examples").
		RoutingKey("order.created").
		Mandatory().
		OnReturn(func(r warren.Return) {
			returned = true
			log.Printf("message returned: code=%d text=%s", r.ReplyCode, r.ReplyText)
		}).
		ConfirmTimeout(30 * time.Second).
		PublishRetry(warren.RetryPolicy{
			Min:     100 * time.Millisecond,
			Max:     5 * time.Second,
			Factor:  2.0,
			Retries: 3,
		}).
		Build()
	if err != nil {
		return fmt.Errorf("build publisher: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(closeCtx)
	}()

	order := Order{ID: "ord-001", Amount: 42}
	msg := warren.Message[Order]{Body: &order}
	if err := pub.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	if returned {
		return fmt.Errorf("message was returned by broker (unroutable)")
	}

	log.Printf("order published successfully: id=%s amount=%d", order.ID, order.Amount)
	return nil
}
