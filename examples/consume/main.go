// Package main demonstrates how to consume typed AMQP messages using the
// Warren library with per-message handler verdict, MaxRedeliveries enforcement,
// and HandlerTimeout.
//
// What this example demonstrates:
//   - Declaring AMQP topology with a dead-letter exchange (DLX) via Topology.Declare
//   - Building a typed Consumer[Order] with ConsumerFor
//   - Three handler result classes:
//     nil → Ack; error → Nack(false); errors.Join(err, ErrRequeue) → Nack(true)
//   - MaxRedeliveries(3): after 3 ErrRequeue returns the in-process counter B rewrites
//     the verdict to Nack(false) and the message is dead-lettered
//   - HandlerTimeout(2s): a slow handler that exceeds the deadline is nacked automatically
//
// How to run:
//
//	go run ./examples/consume
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.consume" (topic, non-durable)
//   - Creates exchange "warren.examples.consume.dlx" (topic, non-durable)
//   - Creates queue "warren.examples.consume.orders" (classic, non-durable, DLX wired)
//   - Creates queue "warren.examples.consume.orders.dlq" (classic, non-durable)
//   - Binds orders queue to the main exchange with routing key "order.#"
//   - Binds DLQ to the DLX exchange with routing key "#"
//
// The example exits 0 after all 5 published messages have been finalized
// (acked, nacked, or dead-lettered), non-zero on any error.
package main

import (
	"context"
	"errors"
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
	mainExchange   = "warren.examples.consume"
	dlxExchange    = "warren.examples.consume.dlx"
	sourceQueue    = "warren.examples.consume.orders"
	dlq            = "warren.examples.consume.orders.dlq"
	maxRedeliv     = 3
	numMessages    = 5
	exampleTimeout = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("consume example failed: %v", err)
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

	// Declare topology: main exchange + source queue (with DLX) + DLX exchange + DLQ.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: mainExchange, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: false},
			{Name: dlxExchange, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: false},
		},
		Queues: []warren.Queue{
			{Name: sourceQueue, Durable: false, AutoDelete: false},
			{Name: dlq, Durable: false, AutoDelete: false},
		},
		Bindings: []warren.Binding{
			{Exchange: mainExchange, Queue: sourceQueue, RoutingKey: "order.#"},
			{Exchange: dlxExchange, Queue: dlq, RoutingKey: "#"},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: sourceQueue, Exchange: dlxExchange, RoutingKey: "#"},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared")

	// Build the publisher.
	pub, err := warren.PublisherFor[Order](conn).
		Exchange(mainExchange).
		RoutingKey("order.created").
		ConfirmTimeout(10 * time.Second).
		Build()
	if err != nil {
		return fmt.Errorf("build publisher: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = pub.Close(closeCtx)
	}()

	// — Handler —————————————————————————————————————————————————————————————
	// finalized is a buffered channel that receives the IDs of finalized messages.
	// Each unique message ID is signalled exactly once.
	finalized := make(chan string, numMessages)

	// Per-message delivery counters (used to decide when to signal finalized).
	var (
		transientCount atomic.Int64
		poisonCount    atomic.Int64
	)

	h := func(ctx context.Context, order Order) error {
		switch order.ID {
		case "good":
			// nil → Ack. Message consumed exactly once.
			log.Printf("handler: id=good amount=%d → nil (Ack)", order.Amount)
			finalized <- "good"
			return nil

		case "bad":
			// Non-ErrRequeue error → Nack(false) → dead-lettered immediately.
			log.Printf("handler: id=bad amount=%d → error (Nack no-requeue)", order.Amount)
			finalized <- "bad"
			return fmt.Errorf("bad message: permanent error")

		case "slow":
			// Handler exceeds HandlerTimeout(2s); ctx is cancelled by the library.
			// The consumer automatically nacks the message; the handler return is ignored.
			log.Printf("handler: id=slow amount=%d → sleeping past deadline", order.Amount)
			select {
			case <-ctx.Done():
				// Deadline hit or consumer cancelled; return cleanly.
			case <-time.After(30 * time.Second):
			}
			log.Printf("handler: id=slow → handler ctx cancelled (HandlerTimeout fired)")
			finalized <- "slow"
			return nil

		case "transient":
			// errors.Join(err, ErrRequeue) → Nack(true) on the first call; nil on the second.
			n := transientCount.Add(1)
			if n == 1 {
				log.Printf("handler: id=transient attempt=%d → ErrRequeue (Nack requeue)", n)
				return errors.Join(fmt.Errorf("transient: first attempt"), warren.ErrRequeue)
			}
			log.Printf("handler: id=transient attempt=%d → nil (Ack)", n)
			finalized <- "transient"
			return nil

		case "poison":
			// Always returns ErrRequeue. After MaxRedeliveries(3), counter B rewrites
			// the verdict to Nack(false) and the message is dead-lettered.
			n := poisonCount.Add(1)
			log.Printf("handler: id=poison attempt=%d → ErrRequeue", n)
			if n == int64(maxRedeliv)+1 {
				// This is the call where counter B fires; signal finalization.
				log.Printf("handler: id=poison max redeliveries reached — dead-lettering")
				finalized <- "poison"
			}
			return errors.Join(fmt.Errorf("poison: always fails"), warren.ErrRequeue)

		default:
			return nil
		}
	}

	// Build the consumer.
	consumer, err := warren.ConsumerFor[Order](conn).
		Queue(sourceQueue).
		Concurrency(4).
		Prefetch(8).
		MaxRedeliveries(maxRedeliv).
		HandlerTimeout(2 * time.Second).
		Build()
	if err != nil {
		return fmt.Errorf("build consumer: %w", err)
	}

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- consumer.Consume(consumerCtx, h) }()

	// Publish 5 messages demonstrating each handler behaviour.
	orders := []Order{
		{ID: "good", Amount: 1},
		{ID: "bad", Amount: 2},
		{ID: "slow", Amount: 3},
		{ID: "transient", Amount: 4},
		{ID: "poison", Amount: 5},
	}
	for i := range orders {
		if err := pub.Publish(ctx, warren.Message[Order]{Body: &orders[i]}); err != nil {
			return fmt.Errorf("publish %s: %w", orders[i].ID, err)
		}
		log.Printf("published: id=%s", orders[i].ID)
	}

	// Wait until all numMessages unique IDs have been finalized or the overall
	// context deadline fires.
	seen := make(map[string]bool, numMessages)
	waitDeadline := time.NewTimer(30 * time.Second)
	defer waitDeadline.Stop()
	for len(seen) < numMessages {
		select {
		case id := <-finalized:
			if !seen[id] {
				seen[id] = true
				log.Printf("finalized: id=%s (%d/%d)", id, len(seen), numMessages)
			}
		case <-waitDeadline.C:
			return fmt.Errorf("timed out waiting for all messages to finalize; only saw: %v", seen)
		case <-ctx.Done():
			return fmt.Errorf("overall context cancelled before all messages finalized; saw: %v", seen)
		}
	}

	cancelConsumer()
	if err := <-consumerErr; err != nil {
		return fmt.Errorf("consumer returned error: %w", err)
	}

	log.Println("consume example complete — all messages finalized")
	return nil
}
