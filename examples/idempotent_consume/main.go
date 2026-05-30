// Package main demonstrates the canonical at-least-once dedupe pattern from
// SPEC §6.2.1: an idempotent consumer that keys a bounded LRU+TTL cache on
// Delivery.MessageID so a message redelivered by PublishRetry, the reconnect
// barrier, or a confirm timeout is processed by the business handler exactly
// once.
//
// What this example demonstrates:
//   - Warren guarantees at-least-once delivery, never exactly-once. Duplicates
//     are produced by design (PublishRetry, the reconnect barrier, confirm
//     timeouts). Consumers MUST dedupe by MessageID — see SPEC §6.2.1.
//   - ConsumeRaw exposes the full *Delivery[M] (envelope) so the handler can
//     read MessageID; the value-typed Consume handler only sees the payload.
//   - A bounded in-memory LRU cache (~10k entries / 15-min TTL) keyed by
//     MessageID. markSeen is an atomic check-and-insert: the first sight of an
//     ID runs the business handler; every later sight is acked and skipped.
//   - A deliberate duplicate: the publisher emits the SAME MessageID twice, and
//     the example asserts the business handler ran exactly once for it.
//
// How to run:
//
//	go run ./examples/idempotent_consume
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates exchange "warren.examples.idem" (topic, non-durable, auto-delete)
//   - Creates queue "warren.examples.idem.orders" (non-durable, auto-delete)
//   - Binds the queue to the exchange with routing key "order.#"
//
// The example exits 0 after every unique MessageID has been handled exactly
// once (the duplicate is observed by the dedupe cache, not the handler), and
// non-zero on any error.
package main

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/brunomvsouza/warren"
)

// Order is the payload type for this example.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

const (
	exchange       = "warren.examples.idem"
	queue          = "warren.examples.idem.orders"
	routingKey     = "order.created"
	cacheCapacity  = 10_000           // bounded entry count (LRU eviction past this)
	cacheTTL       = 15 * time.Minute // per-entry expiry
	exampleTimeout = 60 * time.Second

	// numDistinct is the number of unique MessageIDs the publisher emits (the
	// duplicate shares an ID with ord-2, so 4 publishes carry 3 distinct IDs).
	numDistinct = 3
)

func main() {
	if err := run(); err != nil {
		log.Printf("idempotent_consume example failed: %v", err)
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
			{Name: exchange, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []warren.Queue{
			{Name: queue, Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: exchange, Queue: queue, RoutingKey: "order.#"},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	pub, err := warren.PublisherFor[Order](conn).
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

	// — Dedupe cache + idempotent handler ————————————————————————————————————
	dedupe := newSeenCache(cacheCapacity, cacheTTL)

	// handledCounts records how many times the BUSINESS handler ran per ID. With
	// dedupe in place every value must be exactly 1, even for the duplicated ID.
	var handledMu sync.Mutex
	handledCounts := make(map[string]int)

	// distinctHandled signals each ID the first (and only) time it is processed.
	distinctHandled := make(chan string, numDistinct)

	// dedupeObserved fires once the first duplicate is suppressed, so teardown can
	// wait on the actual event instead of sleeping a fixed interval (the project
	// bans magic sleeps; this also makes the example *prove* the duplicate was
	// deduped rather than hope it arrived in time).
	dedupeObserved := make(chan string, 1)

	rawHandler := func(_ context.Context, d *warren.Delivery[Order]) error {
		id := d.MessageID()

		// Atomic check-and-insert. If the ID was already seen, this is a
		// duplicate: ack it without re-running the business handler.
		if !dedupe.markSeen(id) {
			log.Printf("dedupe: messageID=%s already processed → ack & skip handler", id)
			select {
			case dedupeObserved <- id:
			default:
			}
			return d.Ack()
		}

		// First sight of this MessageID: run the business handler exactly once.
		order := d.Body()
		log.Printf("handler: messageID=%s id=%s amount=%d", id, order.ID, order.Amount)

		handledMu.Lock()
		handledCounts[id]++
		handledMu.Unlock()
		distinctHandled <- id

		return d.Ack()
	}

	consumer, err := warren.ConsumerFor[Order](conn).
		Queue(queue).
		Concurrency(4).
		Prefetch(16).
		Build()
	if err != nil {
		return fmt.Errorf("build consumer: %w", err)
	}

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- consumer.ConsumeRaw(consumerCtx, rawHandler) }()

	// — Publish ——————————————————————————————————————————————————————————————
	// Three distinct orders, plus a deliberate duplicate of the second one: the
	// SAME MessageID is published twice. In production the duplicate is produced
	// involuntarily (PublishRetry / reconnect barrier / confirm timeout); here we
	// force it by pinning Message.MessageID so the run is deterministic.
	dupID := "fixed-msg-id-duplicated"
	publishes := []warren.Message[Order]{
		{Body: &Order{ID: "ord-1", Amount: 10}},                   // auto MessageID (UUIDv7)
		{Body: &Order{ID: "ord-2", Amount: 20}, MessageID: dupID}, // first copy of the duplicate
		{Body: &Order{ID: "ord-3", Amount: 30}},                   // auto MessageID (UUIDv7)
		{Body: &Order{ID: "ord-2", Amount: 20}, MessageID: dupID}, // duplicate — same MessageID
	}
	for i := range publishes {
		if err := pub.Publish(ctx, publishes[i]); err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
	}
	log.Printf("published %d messages (%d distinct MessageIDs; one ID sent twice)",
		len(publishes), numDistinct)

	// Wait until every DISTINCT MessageID has been handled once.
	seen := make(map[string]struct{}, numDistinct)
	waitDeadline := time.NewTimer(30 * time.Second)
	defer waitDeadline.Stop()
	for len(seen) < numDistinct {
		select {
		case id := <-distinctHandled:
			seen[id] = struct{}{}
			log.Printf("handled distinct: messageID=%s (%d/%d)", id, len(seen), numDistinct)
		case <-waitDeadline.C:
			return fmt.Errorf("timed out: only %d/%d distinct IDs handled", len(seen), numDistinct)
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: only %d/%d distinct IDs handled", len(seen), numDistinct)
		}
	}

	// Wait until the duplicate has actually been observed and suppressed by the
	// dedupe cache before tearing down — no fixed sleep, so a slow broker cannot
	// race the assertion that the "already processed" path ran.
	select {
	case <-dedupeObserved:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out: the duplicate MessageID %s was never deduped", dupID)
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before the duplicate was deduped: %w", ctx.Err())
	}

	cancelConsumer()
	if err := <-consumerErr; err != nil {
		return fmt.Errorf("consumer returned error: %w", err)
	}

	// — Assert exactly-once handler invocation ———————————————————————————————
	handledMu.Lock()
	defer handledMu.Unlock()
	for id, n := range handledCounts {
		if n != 1 {
			return fmt.Errorf("dedupe failed: messageID=%s handled %d times (want 1)", id, n)
		}
	}
	if got := handledCounts[dupID]; got != 1 {
		return fmt.Errorf("duplicate MessageID %s handled %d times (want exactly 1)", dupID, got)
	}

	log.Printf("idempotent_consume example complete — duplicate MessageID %s handled exactly once", dupID)
	return nil
}

// seenCache is a bounded, TTL-expiring LRU set of MessageIDs. It is the
// in-memory form of the SPEC §6.2.1 dedupe window. For cross-process dedupe
// (multiple consumer instances), back this with Redis/a database keyed by the
// same MessageID — see this example's README.
type seenCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	ll       *list.List               // front = most-recently-seen
	index    map[string]*list.Element // MessageID → list element
}

type seenEntry struct {
	id      string
	expires time.Time
}

func newSeenCache(capacity int, ttl time.Duration) *seenCache {
	return &seenCache{
		capacity: capacity,
		ttl:      ttl,
		ll:       list.New(),
		index:    make(map[string]*list.Element, capacity),
	}
}

// markSeen records id as seen and returns true if it was NEWLY added (first
// sight) or false if id is already present and unexpired (a duplicate). The
// check-and-insert is atomic under mu, so concurrent handlers cannot both treat
// the same MessageID as new.
func (c *seenCache) markSeen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if el, ok := c.index[id]; ok {
		ent := el.Value.(*seenEntry)
		if now.Before(ent.expires) {
			c.ll.MoveToFront(el) // refresh recency
			return false         // unexpired duplicate
		}
		// Expired: treat as new, refresh the entry below.
		ent.expires = now.Add(c.ttl)
		c.ll.MoveToFront(el)
		return true
	}

	el := c.ll.PushFront(&seenEntry{id: id, expires: now.Add(c.ttl)})
	c.index[id] = el

	// Evict the least-recently-used entry past capacity.
	if c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.index, oldest.Value.(*seenEntry).id)
		}
	}
	return true
}
