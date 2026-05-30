// Package main demonstrates strict per-queue ordering with consumer failover,
// the SPEC §6.3 pattern: Queue.SingleActiveConsumer=true + Consumer.Concurrency(1).
//
// What this example demonstrates:
//   - SingleActiveConsumer (x-single-active-consumer): the broker delivers to at
//     most ONE consumer of the queue at a time, even when several are subscribed.
//     The others are hot standbys.
//   - Concurrency(1): a single handler goroutine processes deliveries serially,
//     so within the active consumer order is preserved end to end.
//   - REAL failover: two consumer instances subscribe, each on its OWN warren
//     connection (modelling two service processes). The active consumer (a)
//     handles a prefix of a numbered sequence in order; then its connection is
//     CLOSED — the realistic "the instance died" event. Closing the connection
//     drops the broker-side basic.consume registration, so the broker promotes
//     the standby (b), which continues the sequence in order.
//   - Why a whole connection and not just a cancelled context: cancelling the
//     Consume context stops local dispatch but does NOT send basic.cancel or
//     close the AMQP channel, so the broker still sees the consumer as active and
//     never promotes the standby. Closing the connection is what triggers SAC
//     failover (and requeues any in-flight unacked message to the standby).
//   - Determinism without magic sleeps: each promotion is made OBSERVABLE with a
//     readiness probe (an out-of-band seq=-1 message); the suffix is published
//     only after the probe proves the standby is now the active consumer, so the
//     standby provably handles it.
//   - At-least-once at the handoff: a message in flight when the active
//     consumer's connection dropped is requeued to the standby. The example
//     dedupes by sequence number and asserts the DEDUPED accepted stream is
//     exactly 0..N-1 — i.e. publish order == handler order across the failover.
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
// once, in order, across a real active-consumer failover; non-zero on any
// out-of-order delivery or timeout.
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
	handoffAt      = 9  // consumer-a handles 0..handoffAt; the promoted standby handles the rest
	probeSeq       = -1 // readiness probe: out-of-band sequence number, never part of 0..N-1
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

	// Two connections model two service instances consuming the same queue. The
	// active consumer lives on its OWN connection (connActive) so that "killing"
	// it is a REAL broker-side event: closing the connection drops its
	// basic.consume registration and the broker promotes the standby. The
	// publisher, topology and the standby live on connMain, which survives the
	// failover.
	connMain, err := warren.Dial(ctx, warren.WithAddr(url))
	if err != nil {
		return fmt.Errorf("dial main: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = connMain.Close(closeCtx)
	}()

	connActive, err := warren.Dial(ctx, warren.WithAddr(url))
	if err != nil {
		return fmt.Errorf("dial active: %w", err)
	}
	// connActive is closed mid-run to trigger the failover; guard the double close
	// (the deferred cleanup and the explicit failover both call it).
	var closeActiveOnce sync.Once
	closeActive := func() {
		closeActiveOnce.Do(func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			_ = connActive.Close(closeCtx)
		})
	}
	defer closeActive()

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
	if err := topo.Declare(ctx, connMain); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	pub, err := warren.PublisherFor[Event](connMain).
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
	allDone := make(chan struct{})           // closed when every sequence number is accepted
	handoffReached := make(chan struct{}, 1) // signalled once consumer-a accepts seq=handoffAt
	probeReady := make(chan string, 2)       // receives the name of the consumer that handled a probe

	// makeHandler builds a Concurrency(1) handler for a named consumer instance.
	makeHandler := func(name string) warren.Handler[Event] {
		return func(_ context.Context, e Event) error {
			if e.Seq == probeSeq {
				// The readiness probe: receiving it proves THIS consumer is the
				// active one (the broker only delivers to the active consumer of a
				// SAC queue).
				log.Printf("%s: readiness probe handled → ack (active consumer confirmed)", name)
				select {
				case probeReady <- name:
				default:
				}
				return nil
			}
			switch tracker.accept(e.Seq) {
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
					case handoffReached <- struct{}{}: // unblock the failover
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

	var wg sync.WaitGroup
	consumerErrs := make(chan error, 2)

	// startConsumer subscribes a named Concurrency(1) consumer on the given
	// connection and runs its Consume loop until cctx is cancelled.
	startConsumer := func(cctx context.Context, conn *warren.Connection, name string) error {
		c, err := warren.ConsumerFor[Event](conn).
			Queue(queue).
			Tag(name).
			Concurrency(1).
			Prefetch(1).
			Build()
		if err != nil {
			return fmt.Errorf("build consumer %s: %w", name, err)
		}
		wg.Go(func() {
			consumerErrs <- c.Consume(cctx, makeHandler(name))
		})
		return nil
	}

	publishProbe := func(who string) error {
		if err := pub.Publish(ctx, warren.Message[Event]{Body: &Event{Seq: probeSeq}}); err != nil {
			return fmt.Errorf("publish readiness probe (%s): %w", who, err)
		}
		return nil
	}
	// awaitProbe blocks until the named consumer handles a readiness probe,
	// proving it is the active consumer. No magic sleep.
	awaitProbe := func(who string, within time.Duration) error {
		for {
			select {
			case got := <-probeReady:
				if got == who {
					return nil
				}
			case <-time.After(within):
				return fmt.Errorf("%s did not become the active consumer within %s", who, within)
			case <-ctx.Done():
				return fmt.Errorf("context cancelled before %s became active: %w", who, ctx.Err())
			}
		}
	}

	// — Bring up consumer-a as the active consumer ————————————————————————————
	cctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	if err := startConsumer(cctxA, connActive, "consumer-a"); err != nil {
		return err
	}
	// While consumer-a is the only subscriber it is necessarily the active
	// consumer. Publish a readiness probe and block until consumer-a handles it,
	// proving its basic.consume is live; only then add the standby.
	if err := publishProbe("consumer-a"); err != nil {
		return err
	}
	if err := awaitProbe("consumer-a", 15*time.Second); err != nil {
		return err
	}

	// Add the standby. SingleActiveConsumer keeps consumer-a active because it
	// subscribed first; consumer-b waits as a hot standby.
	cctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	if err := startConsumer(cctxB, connMain, "consumer-b"); err != nil {
		return err
	}

	// — Publish the prefix; consumer-a handles 0..handoffAt in order ——————————
	for seq := 0; seq <= handoffAt; seq++ {
		e := Event{Seq: seq}
		if err := pub.Publish(ctx, warren.Message[Event]{Body: &e}); err != nil {
			return fmt.Errorf("publish seq=%d: %w", seq, err)
		}
	}
	select {
	case <-handoffReached:
	case <-time.After(20 * time.Second):
		return fmt.Errorf("timed out waiting for consumer-a to reach the handoff at seq=%d", handoffAt)
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before handoff: %w", ctx.Err())
	}

	// — Fail over: close consumer-a's connection so the broker promotes the standby —
	log.Printf("killing active consumer consumer-a after seq=%d — broker will promote the standby", handoffAt)
	closeActive()
	cancelA() // let consumer-a's Consume goroutine return now that its connection is gone

	// Make the promotion OBSERVABLE before publishing the suffix: a second probe
	// proves consumer-b is now the active consumer, so it provably handles the
	// rest of the sequence.
	if err := publishProbe("consumer-b"); err != nil {
		return err
	}
	if err := awaitProbe("consumer-b", 20*time.Second); err != nil {
		return err
	}

	// — Publish the suffix; only consumer-b can handle it now ——————————————————
	for seq := handoffAt + 1; seq < numEvents; seq++ {
		e := Event{Seq: seq}
		if err := pub.Publish(ctx, warren.Message[Event]{Body: &e}); err != nil {
			return fmt.Errorf("publish seq=%d: %w", seq, err)
		}
	}
	log.Printf("published %d ordered events", numEvents)

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
	cancelA()
	cancelB()
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
