// Package main demonstrates strict per-queue ordering with consumer failover,
// the SPEC §6.3 pattern: Queue.SingleActiveConsumer=true + Consumer.Concurrency(1).
//
// What this example demonstrates:
//   - SingleActiveConsumer (x-single-active-consumer): the broker delivers to at
//     most ONE consumer of the queue at a time, even when several are subscribed.
//     The others are hot standbys.
//   - Concurrency(1): a single handler goroutine processes deliveries serially,
//     so within the active consumer order is preserved end to end.
//   - Failover: two consumer instances subscribe; the active one processes a
//     prefix of a numbered sequence in order, then is cancelled ("killed"); the
//     broker promotes the standby, which continues the sequence in order.
//   - At-least-once at the handoff: a message in flight when the active consumer
//     drops may be redelivered to the standby. The example dedupes by sequence
//     number and asserts the DEDUPED accepted stream is exactly 0..N-1 — i.e.
//     publish order == handler order across the failover.
//
// How to run:
//
//	go run ./examples/ordered_consume
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.ordered" (topic, non-durable, auto-delete)
//   - Creates queue "warren.examples.ordered.events" (non-durable, auto-delete,
//     x-single-active-consumer=true)
//   - Binds the queue to the exchange with routing key "event.#"
//
// The example exits 0 once every sequence number 0..N-1 has been handled exactly
// once, in order, across the active-consumer kill; non-zero on any out-of-order
// delivery or timeout.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/brunomvsouza/warren"
)

// Event carries a monotonically increasing sequence number.
type Event struct {
	Seq int `json:"seq"`
}

const (
	exchange       = "warren.examples.ordered"
	queue          = "warren.examples.ordered.events"
	routingKey     = "event.created"
	numEvents      = 20
	handoffAt      = 9 // kill the active consumer after it accepts this sequence number
	exampleTimeout = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("ordered_consume example failed: %v", err)
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

	// SingleActiveConsumer is the load-bearing topology flag: it tells the broker
	// to deliver to one consumer at a time across the whole queue.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: exchange, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []warren.Queue{
			{Name: queue, Durable: false, AutoDelete: true, SingleActiveConsumer: true},
		},
		Bindings: []warren.Binding{
			{Exchange: exchange, Queue: queue, RoutingKey: "event.#"},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	pub, err := warren.PublisherFor[Event](conn).
		Exchange(exchange).
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

	// — Shared ordered sink ———————————————————————————————————————————————————
	tracker := &orderTracker{
		seen:     make(map[int]struct{}, numEvents),
		nextWant: 0,
	}
	allDone := make(chan struct{})      // closed when every sequence number is accepted
	handoffOnce := make(chan string, 1) // receives the active consumer's name once at handoffAt

	// makeHandler builds a Concurrency(1) handler for a named consumer instance.
	makeHandler := func(name string) warren.Handler[Event] {
		return func(_ context.Context, e Event) error {
			result := tracker.accept(e.Seq)
			switch result {
			case acceptOutOfOrder:
				return fmt.Errorf("%s: out-of-order delivery: got seq=%d want=%d",
					name, e.Seq, tracker.want())
			case acceptDuplicate:
				log.Printf("%s: duplicate seq=%d at handoff → ack & skip", name, e.Seq)
				return nil // ack; the standby re-received an in-flight message
			case acceptNew:
				log.Printf("%s: handled seq=%d (in order)", name, e.Seq)
				if e.Seq == handoffAt {
					select {
					case handoffOnce <- name: // trigger the kill of THIS (active) consumer
					default:
					}
				}
				if tracker.complete(numEvents) {
					close(allDone)
				}
				return nil
			}
			return nil
		}
	}

	// Start two consumer instances. SingleActiveConsumer makes exactly one active.
	cancels := make(map[string]context.CancelFunc, 2)
	consumerErrs := make(chan error, 2)
	var wg sync.WaitGroup

	startConsumer := func(name string) error {
		c, err := warren.ConsumerFor[Event](conn).
			Queue(queue).
			Tag(name).
			Concurrency(1).
			Prefetch(1).
			Build()
		if err != nil {
			return fmt.Errorf("build consumer %s: %w", name, err)
		}
		cctx, ccancel := context.WithCancel(ctx)
		cancels[name] = ccancel
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer ccancel() // idempotent with the external kill; guarantees cancel on return
			consumerErrs <- c.Consume(cctx, makeHandler(name))
		}()
		return nil
	}

	if err := startConsumer("consumer-a"); err != nil {
		return err
	}
	// Give consumer-a a head start so it becomes the active consumer, then add the
	// standby.
	time.Sleep(500 * time.Millisecond)
	if err := startConsumer("consumer-b"); err != nil {
		return err
	}

	// — Publish the ordered sequence ——————————————————————————————————————————
	for seq := 0; seq < numEvents; seq++ {
		e := Event{Seq: seq}
		if err := pub.Publish(ctx, warren.Message[Event]{Body: &e}); err != nil {
			return fmt.Errorf("publish seq=%d: %w", seq, err)
		}
	}
	log.Printf("published %d ordered events", numEvents)

	// — Kill the active consumer at the handoff point —————————————————————————
	go func() {
		select {
		case name := <-handoffOnce:
			log.Printf("killing active consumer %s after seq=%d — broker will promote the standby",
				name, handoffAt)
			cancels[name]()
		case <-ctx.Done():
		}
	}()

	// Wait for the full ordered sequence or a deadline.
	select {
	case <-allDone:
		log.Printf("all %d events handled in order across the failover", numEvents)
	case <-time.After(40 * time.Second):
		return fmt.Errorf("timed out: only accepted up to seq=%d", tracker.want()-1)
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before completion: accepted up to seq=%d", tracker.want()-1)
	}

	// Tear down both consumers and join their goroutines.
	for _, c := range cancels {
		c()
	}
	wg.Wait()
	close(consumerErrs)
	for err := range consumerErrs {
		if err != nil {
			return fmt.Errorf("consumer returned error: %w", err)
		}
	}

	// Final ordering assertion: the deduped accepted stream is exactly 0..N-1.
	if err := tracker.verifyContiguous(numEvents); err != nil {
		return err
	}

	log.Printf("ordered_consume example complete — publish order == handler order across failover")
	return nil
}

// acceptResult classifies a delivery within the ordered sink.
type acceptResult int

const (
	acceptNew        acceptResult = iota // first sight, in order
	acceptDuplicate                      // already accepted (handoff redelivery)
	acceptOutOfOrder                     // gap or regression — a correctness failure
)

// orderTracker is the shared, concurrency-safe ordered sink. With
// SingleActiveConsumer + Concurrency(1) only one handler runs at a time, but the
// mutex keeps the failover handoff (consumer-a → consumer-b) race-free.
type orderTracker struct {
	mu       sync.Mutex
	seen     map[int]struct{}
	nextWant int   // the next sequence number expected, in order
	order    []int // the accepted sequence, in acceptance order
}

func (t *orderTracker) accept(seq int) acceptResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.seen[seq]; ok {
		return acceptDuplicate
	}
	if seq != t.nextWant {
		return acceptOutOfOrder
	}
	t.seen[seq] = struct{}{}
	t.order = append(t.order, seq)
	t.nextWant++
	return acceptNew
}

func (t *orderTracker) want() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextWant
}

func (t *orderTracker) complete(n int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextWant >= n
}

func (t *orderTracker) verifyContiguous(n int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.order) != n {
		return fmt.Errorf("accepted %d events, want %d", len(t.order), n)
	}
	for i, seq := range t.order {
		if seq != i {
			return fmt.Errorf("order mismatch at position %d: got seq=%d want=%d", i, seq, i)
		}
	}
	return nil
}
