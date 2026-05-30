// Package main demonstrates delayed message delivery using the Warren library and
// the RabbitMQ rabbitmq_delayed_message_exchange plugin.
//
// What this example demonstrates:
//   - Declaring an x-delayed-message exchange via warren.DelayedTopic and binding
//     a queue to it
//   - Publishing a Message[Event] with Delay = 2*time.Second so the broker holds
//     the message for the delay before routing it (emitted as the x-delay header)
//   - A consumer that records the arrival time and asserts the message was held
//     for at least the delay (≥ 2s, < 2.5s)
//
// Plugin requirement. ExchangeDelayed needs the rabbitmq_delayed_message_exchange
// plugin enabled on every broker node; against a broker without it, declaring the
// exchange fails with a command-invalid channel error and this example exits
// non-zero. The integration smoke test for this example skips cleanly when the
// plugin is absent (and migrates to amqptest.RequireDelayedExchange(t) once that
// helper lands in T37).
//
// Durability caveat. The plugin stores scheduled messages in a node-local,
// non-replicated table: a confirmed delayed publish can still be lost if the
// owning node fails before the delay elapses. See warren.DelayedTopic.
//
// How to run (requires the delayed-message-exchange plugin):
//
//	go run ./examples/delayed
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.delayed" (x-delayed-message, durable)
//   - Creates queue "warren.examples.delayed.events" (classic, durable)
//   - Binds the queue to the exchange with routing key "event.delayed"
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

const (
	delayExchange  = "warren.examples.delayed"
	delayQueue     = "warren.examples.delayed.events"
	routingKey     = "event.delayed"
	delay          = 2 * time.Second
	tolerance      = 500 * time.Millisecond
	exampleTimeout = 60 * time.Second
)

// Event is the payload type for this example.
type Event struct {
	ID string `json:"id"`
}

func main() {
	if err := run(); err != nil {
		log.Printf("delayed example failed: %v", err)
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

	// Declare the delayed-message exchange (via the DelayedTopic helper, which sets
	// Kind=x-delayed-message and the required x-delayed-type=topic arg) and bind a
	// queue to it. Declaring this exchange fails if the plugin is not enabled.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{warren.DelayedTopic(delayExchange)},
		Queues:    []warren.Queue{{Name: delayQueue, Durable: true}},
		Bindings:  []warren.Binding{{Exchange: delayExchange, Queue: delayQueue, RoutingKey: routingKey}},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("delayed-message topology declared")

	pub, err := warren.PublisherFor[Event](conn).
		Exchange(delayExchange).
		RoutingKey(routingKey).
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

	// Consumer records the arrival time of the single delayed message.
	arrived := make(chan time.Time, 1)
	consumer, err := warren.ConsumerFor[Event](conn).Queue(delayQueue).Build()
	if err != nil {
		return fmt.Errorf("build consumer: %w", err)
	}
	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Consume(consumerCtx, func(_ context.Context, ev Event) error {
			log.Printf("handler: received event id=%s", ev.ID)
			arrived <- time.Now()
			return nil
		})
	}()

	ev := Event{ID: "delayed-001"}
	publishedAt := time.Now()
	if err := pub.Publish(ctx, warren.Message[Event]{Body: &ev, Delay: delay}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("published event id=%s with Delay=%s", ev.ID, delay)

	select {
	case at := <-arrived:
		elapsed := at.Sub(publishedAt)
		log.Printf("event arrived after %s", elapsed.Round(time.Millisecond))
		if elapsed < delay {
			return fmt.Errorf("message arrived too early: %s < %s", elapsed, delay)
		}
		if elapsed >= delay+tolerance {
			return fmt.Errorf("message arrived too late: %s >= %s", elapsed, delay+tolerance)
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out waiting for the delayed message")
	}

	cancelConsumer()
	if err := <-consumerErr; err != nil {
		return fmt.Errorf("consumer: %w", err)
	}

	log.Println("delayed example complete")
	return nil
}
