// Package main demonstrates synchronous request/reply RPC over AMQP using the
// Warren library's Caller and Replier over RabbitMQ direct reply-to.
//
// What this example demonstrates:
//   - Declaring a request queue with a DeadLetter entry so a handler error or a
//     failed reply publish is preserved in a DLQ instead of silently dropped
//   - Serving requests with ReplierFor[PriceReq, PriceResp].Serve, wired to the
//     Topology so Build statically rejects a missing DLX on the request queue
//   - Issuing three CONCURRENT CallerFor[PriceReq, PriceResp].Call invocations,
//     each demultiplexed back to its own goroutine by CorrelationID
//   - The negative path: a Call whose ctx deadline (50ms) fires before a slow
//     handler (200ms) can reply surfaces as ErrCallTimeout
//
// At-least-once reply ordering (SPEC §6.7). The Replier publishes the reply,
// awaits its broker confirm, and only then acks the request. A crash between the
// confirm and the ack makes the broker redeliver the request and the Replier send
// a SECOND reply — so callers MUST treat replies as at-least-once and dedupe by
// CorrelationID whenever the operation is not naturally idempotent.
//
// How to run:
//
//	go run ./examples/rpc
//
// Environment variables:
//   - AMQP_URL: broker URL (default: amqp://guest:guest@localhost:5672/)
//
// Topology side-effects on the broker:
//   - Creates queue "warren.examples.rpc.prices" (classic, durable, DLX wired)
//   - Creates exchange "warren.examples.rpc.prices.dlx" (topic, durable)
//   - Creates queue "warren.examples.rpc.prices.dlq" (classic, durable)
//   - Binds the DLQ to the DLX with routing key "#"
//
// The example exits 0 on success and non-zero on any error.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brunomvsouza/warren"
)

const (
	requestQueue   = "warren.examples.rpc.prices"
	dlxExchange    = "warren.examples.rpc.prices.dlx"
	dlq            = "warren.examples.rpc.prices.dlq"
	exampleTimeout = 60 * time.Second
)

// PriceReq is the request payload. SleepMs lets a single Replier serve both the
// fast happy path (SleepMs == 0) and the slow negative path used to demonstrate
// ErrCallTimeout, with no separate replier.
type PriceReq struct {
	Symbol  string `json:"symbol"`
	SleepMs int    `json:"sleep_ms"`
}

// PriceResp is the response payload.
type PriceResp struct {
	Symbol string `json:"symbol"`
	Price  int    `json:"price"`
}

// priceFor is a deterministic stand-in for a real pricing lookup so the example
// has no external dependency: the reply for a symbol is reproducible, which lets
// the concurrent calls each assert their response matched their own request.
func priceFor(symbol string) int {
	sum := 0
	for _, r := range symbol {
		sum += int(r)
	}
	return sum
}

func main() {
	if err := run(); err != nil {
		log.Printf("rpc example failed: %v", err)
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

	// Declare the request queue with a DeadLetter entry. The DeadLetter pre-pass
	// injects x-dead-letter-exchange on the queue; we declare the DLX and a DLQ and
	// bind them so a Nack(false)'d request (handler error or failed reply publish)
	// is preserved in the DLQ. Wiring the Topology into the Replier lets Build
	// statically reject a missing DLX rather than discover it at runtime.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: dlxExchange, Kind: warren.ExchangeTopic, Durable: true},
		},
		Queues: []warren.Queue{
			{Name: requestQueue, Durable: true},
			{Name: dlq, Durable: true},
		},
		Bindings: []warren.Binding{
			{Exchange: dlxExchange, Queue: dlq, RoutingKey: "#"},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: requestQueue, Exchange: dlxExchange},
		},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	log.Println("topology declared (request queue has a DLX)")

	// — Replier ——————————————————————————————————————————————————————————————
	replier, err := warren.ReplierFor[PriceReq, PriceResp](conn).
		Queue(requestQueue).
		Topology(topo). // validates the DLX is present; Build fails otherwise
		OnError(func(_ context.Context, req PriceReq, err error) {
			log.Printf("replier OnError: symbol=%s err=%v", req.Symbol, err)
		}).
		Build()
	if err != nil {
		return fmt.Errorf("build replier: %w", err)
	}

	handler := func(ctx context.Context, req PriceReq) (PriceResp, error) {
		if req.SleepMs > 0 {
			// Simulate a slow backend. The handler runs on the Serve ctx, not the
			// caller's, so it finishes even after the caller has given up — the late
			// reply is then published with no waiter and harmlessly dropped.
			select {
			case <-time.After(time.Duration(req.SleepMs) * time.Millisecond):
			case <-ctx.Done():
				return PriceResp{}, ctx.Err()
			}
		}
		return PriceResp{Symbol: req.Symbol, Price: priceFor(req.Symbol)}, nil
	}

	serveCtx, stopServe := context.WithCancel(ctx)
	defer stopServe()
	serveErr := make(chan error, 1)
	go func() { serveErr <- replier.Serve(serveCtx, handler) }()

	// — Caller ———————————————————————————————————————————————————————————————
	caller, err := warren.CallerFor[PriceReq, PriceResp](conn).
		RoutingKey(requestQueue). // default exchange routes by queue name
		Build()
	if err != nil {
		return fmt.Errorf("build caller: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = caller.Close(closeCtx)
	}()

	// Happy path: three concurrent calls. A shared Caller demultiplexes replies by
	// CorrelationID, so each goroutine receives the response to its own request.
	symbols := []string{"AAPL", "GOOG", "MSFT"}
	type result struct {
		symbol string
		resp   PriceResp
		err    error
	}
	results := make(chan result, len(symbols))
	for _, sym := range symbols {
		go func(sym string) {
			cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
			defer ccancel()
			resp, err := caller.Call(cctx, PriceReq{Symbol: sym})
			results <- result{symbol: sym, resp: resp, err: err}
		}(sym)
	}
	for range symbols {
		r := <-results
		if r.err != nil {
			return fmt.Errorf("call %s: %w", r.symbol, r.err)
		}
		want := priceFor(r.symbol)
		if r.resp.Symbol != r.symbol || r.resp.Price != want {
			return fmt.Errorf("call %s: got %+v, want symbol=%s price=%d",
				r.symbol, r.resp, r.symbol, want)
		}
		log.Printf("call ok: symbol=%s price=%d", r.resp.Symbol, r.resp.Price)
	}
	log.Printf("all %d concurrent calls matched their responses by CorrelationID", len(symbols))

	// Negative path: a 50ms ctx against a handler told to sleep 200ms. The ctx
	// deadline fires first, so Call returns ErrCallTimeout while the handler keeps
	// running server-side (its eventual reply has no waiter and is dropped).
	nctx, ncancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer ncancel()
	if _, err := caller.Call(nctx, PriceReq{Symbol: "SLOW", SleepMs: 200}); !errors.Is(err, warren.ErrCallTimeout) {
		return fmt.Errorf("expected ErrCallTimeout for the slow handler, got: %w", err)
	}
	log.Println("slow call timed out cleanly with ErrCallTimeout")

	// Shutdown: stop the Replier and wait for it to drain. Serve returns nil on ctx
	// cancellation; tolerate context.Canceled defensively.
	stopServe()
	if err := <-serveErr; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("replier serve: %w", err)
	}

	log.Println("rpc example complete")
	return nil
}
