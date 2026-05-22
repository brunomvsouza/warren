// Package main demonstrates dead-letter topology expansion and the quorum-queue
// DeliveryLimit using the Warren library.
//
// What this example demonstrates:
//   - Declaring a QueueTypeQuorum source queue with DeliveryLimit and a full
//     DeadLetter entry (Exchange, RoutingKey, TTL, MaxLength, Overflow)
//   - The in-memory DLX expansion that merges x-dead-letter-* args into the
//     source queue before the first broker declare (no re-declare needed)
//   - Publishing a message and having a raw consumer nack it without requeue
//     so it is routed to the configured DLX and lands in the DLQ
//   - Inspecting the DLQ message body and x-death header
//
// How to run:
//
//	go run ./examples/deadletter
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.dl.topic" (topic, durable)
//   - Creates exchange "warren.examples.dl.orders.dlx" (topic, durable)
//   - Creates queue "warren.examples.dl.orders" (quorum, durable, DeliveryLimit=3)
//   - Creates queue "warren.examples.dl.orders.dlq" (classic, durable)
//   - Binds orders queue to topic exchange with routing key "order.#"
//   - Binds DLQ to DLX with routing key "#" (catch-all)
//
// The example exits 0 on success and non-zero on any error.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	amqp "github.com/brunomvsouza/warren"
)

// Order is the payload type for this example.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func main() {
	if err := run(); err != nil {
		log.Printf("deadletter example failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	ctx := context.Background()

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare the full topology. The DLX exchange and DLQ queue are listed
	// explicitly so we can add the binding from DLX → DLQ. The DeadLetter
	// entry instructs the in-memory pre-pass to inject x-dead-letter-* args
	// into the source queue before the broker call — no re-declare needed.
	topo := &amqp.Topology{
		Exchanges: []amqp.Exchange{
			{
				Name:    "warren.examples.dl.topic",
				Kind:    amqp.ExchangeTopic,
				Durable: true,
			},
			{
				// DLX exchange — also created by the DLX pre-pass, but declared
				// here explicitly so we can bind the DLQ to it below.
				Name:    "warren.examples.dl.orders.dlx",
				Kind:    amqp.ExchangeTopic,
				Durable: true,
			},
		},
		Queues: []amqp.Queue{
			{
				Name:          "warren.examples.dl.orders",
				Durable:       true,
				Type:          amqp.QueueTypeQuorum,
				DeliveryLimit: 3,
			},
			{
				// DLQ — also created by the DLX pre-pass, declared here
				// explicitly so we can bind it to the DLX exchange.
				Name:    "warren.examples.dl.orders.dlq",
				Durable: true,
			},
		},
		Bindings: []amqp.Binding{
			{
				Exchange:   "warren.examples.dl.topic",
				Queue:      "warren.examples.dl.orders",
				RoutingKey: "order.#",
			},
			{
				// Catch-all binding so every dead-lettered message reaches the DLQ.
				Exchange:   "warren.examples.dl.orders.dlx",
				Queue:      "warren.examples.dl.orders.dlq",
				RoutingKey: "#",
			},
		},
		DeadLetters: []amqp.DeadLetter{
			{
				Source:    "warren.examples.dl.orders",
				Exchange:  "warren.examples.dl.orders.dlx",
				TTL:       30 * time.Second,
				MaxLength: 100,
				Overflow:  amqp.OverflowRejectPublishDLX,
			},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared (DLX expansion applied in-memory before broker call)")

	// Publish one message to the source queue.
	pub, err := amqp.PublisherFor[Order](conn).
		Exchange("warren.examples.dl.topic").
		RoutingKey("order.created").
		ConfirmTimeout(10 * time.Second).
		Build()
	if err != nil {
		return fmt.Errorf("build publisher: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(closeCtx)
	}()

	order := Order{ID: "dead-001", Amount: 99}
	if err := pub.Publish(ctx, amqp.Message[Order]{Body: &order}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("order published: id=%s", order.ID)

	// Open a raw amqp091 connection to consume from the source queue and nack
	// every message without requeue so the broker routes to the DLX.
	rawConn, err := amqp091.Dial(url)
	if err != nil {
		return fmt.Errorf("raw dial: %w", err)
	}
	defer rawConn.Close() //nolint:errcheck

	ch, err := rawConn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close() //nolint:errcheck

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	srcMsgs, err := ch.Consume(
		"warren.examples.dl.orders",
		"example-nacker",
		false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume source queue: %w", err)
	}

	// Nack the message without requeue — broker routes to DLX immediately.
	select {
	case msg := <-srcMsgs:
		if err := msg.Nack(false, false); err != nil {
			return fmt.Errorf("nack: %w", err)
		}
		log.Printf("message nacked (dead-lettered): routing-key=%s", msg.RoutingKey)
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out waiting for message on source queue")
	}

	// Consume from DLQ and inspect x-death header.
	dlqMsgs, err := ch.Consume(
		"warren.examples.dl.orders.dlq",
		"example-inspector",
		true, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume DLQ: %w", err)
	}

	select {
	case msg := <-dlqMsgs:
		log.Printf("DLQ message received: body=%s", string(msg.Body))
		xDeath := msg.Headers["x-death"]
		log.Printf("x-death header: %v", xDeath)
		log.Println("deadletter example complete")
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out waiting for message on DLQ")
	}

	return nil
}
