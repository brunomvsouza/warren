# Warren

> [!WARNING]
> **This project is in active, early development.** 
> While the goal is to provide a highly reliable operational layer, stability is **not yet guaranteed**. Although we aim to maintain a stable public API as defined in [`SPEC.md`](SPEC.md), occasional breaking changes may still occur prior to `v1.0.0`. Use in production environments at your own risk.

**A generics-typed Go client for RabbitMQ (AMQP 0-9-1), with reliability as a first-class design goal.**

Warren wraps [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) with a type-safe API and an operational layer that aims to cover the reliability concerns production teams usually end up building themselves: supervised reconnect with publisher confirms, centralized topology declaration, pluggable codecs, channel pooling across role-split TCP connections, error classification (transient vs. permanent) derived from AMQP reply codes, safety guardrails (credential redaction, fail-fast validation, payload caps), and native observability (logging, Prometheus, OpenTelemetry). How well it meets that goal is still being proven — see the status note below.

> **Current Status:** Active development toward [`v0.1.0`](SPEC.md). Implementation follows [`tasks/plan.md`](tasks/plan.md). **Connection**, **Publisher**, **Topology**, **Consumer** (with `MaxRedeliveries` + handler timeouts), **batch publish/consume**, the **JSON / Protobuf / CloudEvents codecs**, **end-to-end OpenTelemetry tracing** (publisher + consumer), and the **RPC + delayed-message patterns** are usable today. The feature surface is in place; final release and CI hardening are the remaining work before the first tag.

[![CI](https://github.com/brunomvsouza/warren/actions/workflows/ci.yml/badge.svg)](https://github.com/brunomvsouza/warren/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/brunomvsouza/warren)](https://goreportcard.com/report/github.com/brunomvsouza/warren)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/brunomvsouza/warren.svg)](https://pkg.go.dev/github.com/brunomvsouza/warren)

---

## Why Warren?

Raw `amqp091-go` is correct and minimal. Production RabbitMQ clients need more:

| Concern | Raw driver | Warren |
| --- | --- | --- |
| Message typing | `[]byte` + manual JSON | `PublisherFor[M]` / `ConsumerFor[M]` with pluggable `codec` |
| Reconnect | Roll your own | Per-TCP supervisors + synchronous barrier (redeclare topology → resume traffic) |
| Throughput ceiling | One connection serializes I/O | Role-split pool: publisher vs consumer TCP connections + per-conn channel pool |
| Poison messages | Easy to create infinite requeue loops | Default handler error → `Nack(requeue=false)`; requeue is opt-in via `ErrRequeue` |
| Credentials in logs | Easy to leak | `internal/redact` strips `userinfo` from every URI in logs, metrics, spans, and errors |
| Broker errors | Opaque `*amqp091.Error` | Reply-code sentinels + `AMQPCode(err)` + `IsTransient` / `IsPermanent` |
| Safety guardrails | None | Fail-fast `UserID` validation, payload size caps, and `frame_max` enforcement |
| Infra failures | Hard to detect | Degraded state machine with `OnTopoDegraded` and `ForceReconnect` operator escape hatch |

**Design north stars** (from the spec): AMQP 0-9-1 protocol fidelity without misleading sugar, and a short, safe path for the common case (typed publish/consume over JSON).

**Reliability contract:** at-least-once delivery. Reconnect, `PublishRetry`, and confirm timeouts can produce duplicates — consumers must dedupe (default `MessageID` is UUIDv7). There is no exactly-once toggle.

---

## Quick start

Requires **Go 1.25+** and a **RabbitMQ 3.13 LTS or 4.x** broker.

```bash
# Until the first tag (v0.1.0), pin to main:
go get github.com/brunomvsouza/warren@main
```

### Publish and Consume

The whole path — dial, declare, publish, consume — with every error handled. The
handler takes the **decoded value** (`Order`), not a pointer: `nil` → `Ack`, an
error → `Nack(no-requeue)`, wrap `ErrRequeue` to requeue. Consumers must dedupe
by `MessageID` (at-least-once; see [`examples/idempotent_consume`](examples/idempotent_consume/main.go)).

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/brunomvsouza/warren"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func main() {
	ctx := context.Background()

	// 1. Dial with role-split connection pool (default: 2 publisher, 2 consumer).
	conn, err := warren.Dial(ctx, warren.WithAddr("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := conn.Close(closeCtx); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	// 2. Declare topology (idempotent; redeclared automatically on reconnect).
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: "orders", Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: "orders.created", Durable: true}},
		Bindings:  []warren.Binding{{Exchange: "orders", Queue: "orders.created", RoutingKey: "order.#"}},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		log.Fatalf("declare topology: %v", err)
	}
	if err := topo.AttachTo(conn); err != nil { // redeclare on every reconnect barrier
		log.Fatalf("attach topology: %v", err)
	}

	// 3. Publish a typed message.
	pub, err := warren.PublisherFor[Order](conn).
		Exchange("orders").
		RoutingKey("order.created").
		Build()
	if err != nil {
		log.Fatalf("build publisher: %v", err)
	}
	if err := pub.Publish(ctx, warren.Message[Order]{Body: &Order{ID: "ord-001", Amount: 42}}); err != nil {
		log.Fatalf("publish: %v", err)
	}

	// 4. Consume typed messages (blocks until ctx is cancelled).
	con, err := warren.ConsumerFor[Order](conn).
		Queue("orders.created").
		Concurrency(4).
		Build()
	if err != nil {
		log.Fatalf("build consumer: %v", err)
	}
	err = con.Consume(ctx, func(ctx context.Context, o Order) error {
		log.Printf("processing order %s (amount %d)", o.ID, o.Amount)
		return nil // nil → Ack
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("consume: %v", err)
	}
}
```

### Production setup (TLS, multi-node HA, OpenTelemetry, dead-letters)

The four knobs a production deployment turns on, all on one `Dial` plus a
dead-letter topology:

```go
import (
	"context"
	"crypto/tls"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/otel"
)

func dialProduction(ctx context.Context, tracer otel.Tracer) (*warren.Connection, error) {
	return warren.Dial(ctx,
		// Multi-node HA: round-robin failover across cluster nodes on (re)connect.
		warren.WithAddrs([]string{
			"amqps://app:secret@rabbit-1.internal:5671/prod",
			"amqps://app:secret@rabbit-2.internal:5671/prod",
			"amqps://app:secret@rabbit-3.internal:5671/prod",
		}),
		// TLS: amqps:// URIs use this config (mTLS-ready; SASL EXTERNAL is also supported).
		warren.WithTLSConfig(&tls.Config{ServerName: "rabbit.internal", MinVersion: tls.VersionTLS12}),
		// OpenTelemetry: WithTracer just stores a connection-level tracer (reserved;
		// it drives no spans today). Publish/consume tracing — inject on publish,
		// extract on consume — is turned on per builder with .Tracer(tracer) on
		// PublisherFor / ConsumerFor; see examples/otel for the load-bearing wiring.
		warren.WithTracer(tracer),
	)
}

// Dead-letter exchange: messages that are Nacked without requeue, exceed
// MaxRedeliveries, or expire (TTL) route to a DLQ instead of vanishing.
var prodTopology = &warren.Topology{
	Exchanges: []warren.Exchange{
		{Name: "orders", Kind: warren.ExchangeTopic, Durable: true},
		{Name: "orders.dlx", Kind: warren.ExchangeTopic, Durable: true},
	},
	Queues: []warren.Queue{
		{Name: "orders.created", Durable: true},
		{Name: "orders.dlq", Durable: true},
	},
	Bindings: []warren.Binding{
		{Exchange: "orders", Queue: "orders.created", RoutingKey: "order.#"},
		{Exchange: "orders.dlx", Queue: "orders.dlq", RoutingKey: "#"},
	},
	DeadLetters: []warren.DeadLetter{
		{Source: "orders.created", Exchange: "orders.dlx", RoutingKey: "#"},
	},
}
```

See the [examples table](#examples) for runnable, broker-tested versions of each
of these — including [`examples/otel`](examples/otel/main.go) (trace continuity)
and [`examples/deadletter`](examples/deadletter/main.go) (DLX wiring).

---

## Architecture

A single `warren.Connection` owns a **pool of TCP connections** split by role (default: 2 publisher + 2 consumer sockets). Each socket has its own reconnect supervisor. Publishers borrow channels from a per-connection pool; consumers pin to one consumer connection (stable hash of consumer tag).

### High-level Flow

```mermaid
flowchart TB
  subgraph app [Your application]
    P[PublisherFor M]
    C[ConsumerFor M]
    T[Topology]
  end

  subgraph warren [warren.Connection]
    subgraph pub [Publisher TCP pool]
      PC1[pub-0 + channel pool]
      PC2[pub-1 + channel pool]
    end
    subgraph con [Consumer TCP pool]
      CC1[con-0 + dedicated channel]
      CC2[con-1 + dedicated channel]
    end
    R[Reconnect barrier per socket]
  end

  B[(RabbitMQ)]

  P --> PC1 & PC2
  C --> CC1 & CC2
  T --> R
  PC1 & PC2 --> B
  CC1 & CC2 --> B
  R -->|redeclare topology + resubscribe| B
```

On reconnect, Warren runs a **synchronous barrier** before resuming traffic on that socket: reopen channels → redeclare attached topology → re-issue `basic.consume` on consumer channels (firing `WithOnResubscribe(queue)` per consumer) → fire `WithOnReconnect`. `Publish` blocks on `ErrReconnecting` until the barrier clears (or the context is cancelled).

---

## Reliability & SRE

Warren leans on a few reliability patterns baked into the core. These are design goals backed by tests, not guarantees of zero message loss — the delivery contract is at-least-once (see above):

- **Synchronous Reconnect Barrier:** Restores all attached topology (exchanges, queues, bindings) before traffic resumes on a freshly reconnected socket, so a publish does not race ahead of the queue or binding it depends on. This reduces the window for misrouting on reconnect; it does not turn at-least-once into exactly-once.
- **Error Classification:** `IsTransient(err)` and `IsPermanent(err)` map AMQP reply codes to a transient/permanent split, so your retry logic can key off the actual broker response instead of string matching.
- **Safety Guardrails:** 
    - **Fail-Fast Validation:** Validates `UserID` and message headers client-side to catch a common class of "channel-close" errors before they reach the broker and tear down a publisher socket.
    - **Payload Size Caps:** Rejects oversized messages client-side, before they reach the broker.
    - **Credential Redaction:** The URIs Warren emits to logs, metric labels, trace spans, and error strings are routed through `internal/redact` to strip `userinfo`. This covers the credentials Warren itself formats — it is not a blanket scrubber for secrets your own code may log.
- **Degraded State Awareness:** If the topology fails to redeclare persistently, the connection enters a `degraded` state, triggering `OnTopoDegraded` callbacks and metrics, and providing a `ForceReconnect` escape hatch for operators.

---

## Features

### Available now

- **Connection** — `Dial`, role-split TCP pool, PLAIN SASL, heartbeat and frame sizing, multi-address cluster failover (`WithAddrs`, in-order initial connect + round-robin rotation on reconnect), `Health` / `Close` / `ForceReconnect` (TLS and SASL EXTERNAL options are wired up under production hardening — see roadmap)
- **Publisher** — `PublisherFor[M]`, publisher confirms, mandatory + returns, `PublishRetry`, confirm/publish timeouts, concurrent-safe `Publish`, payload size guardrail
- **Topology** — declarative exchanges, queues, bindings, dead-letter expansion (quorum + DLX gets `x-dead-letter-strategy: at-least-once` automatically); `Declare` + `AttachTo` for reconnect redeclare; degraded state on persistent redeclare failure
- **Consumer** — `ConsumerFor[M]`, prefetch, concurrency, handler error mapping (`ErrRequeue`), re-subscribe loop, `ConsumeRaw`; `MaxRedeliveries` (quorum `DeliveryLimit` + `x-death` + in-process counters) and per-handler `HandlerTimeout` with a configurable `HandlerTimeoutVerdict`; opt-in `AutoAck()` (at-most-once no-ack, with a sampled drop-warning when handlers error); `Exclusive()`, `Args()`, `Tag()`, and broker `basic.cancel` surfacing — `OnCancel(func(reason))` fires and `Consume` returns `ErrConsumerCancelled` (no silent death, no auto-redeclare) with a bounded `consumer_cancelled_total{queue, reason}` metric
- **Batch** — `Publisher.PublishBatch` (always-all, single-channel, `[]PublishResult` + `ErrBatchTooLarge`) and `BatchConsumerFor[M]` with size- and timer-based flush triggers and `multiple=true` acking
- **Codec** — lax JSON by default (Postel's Law — unknown fields are tolerated so producer-first deploys do not poison v1 DLQs); opt-in `codec.NewJSONStrict()`; `codec.NewProtobuf()`; CloudEvents in both `codec.NewCloudEventsStructured()` and `codec.NewCloudEventsBinary()` modes (AMQP protocol binding via the `HeaderCodec` interface)
- **Errors** — AMQP reply-code sentinels, `AMQPCode`, transient/permanent classifiers
- **Observability** — pluggable `log.Logger`, Prometheus metrics (default), OpenTelemetry tracer + W3C propagation helpers; **publisher** spans inject trace context into AMQP headers before the frame is written and **consumer** spans extract it on the other side, so continuity survives DLX bounces (the broker preserves headers verbatim); `BatchConsumer` spans carry one Link per message
- **Patterns** — RPC over direct reply-to (`CallerFor[Req, Resp]` + `ReplierFor[Req, Resp]`, at-least-once replies deduped by `CorrelationID`, DLX-on-request-queue validation); delayed publish via `Message[M].Delay` + the `DelayedTopic` helper for the `x-delayed-message` plugin
- **Testing** — `warren.NewDeliveryFixture` / `NewBatchFixture` fabricate `Delivery[M]` / `Batch[M]` values (keyed-literal `DeliveryFixture`/`BatchFixture`, no live broker) so you can unit-test `ConsumeRaw` handlers — the one fixture only the library can provide. No mock package is shipped: generate your own interface mocks with whatever tool you prefer. Warren's own integration suite uses an internal testcontainers RabbitMQ fixture (`internal/amqptest`), so no test dependency leaks into your build
- **Examples** — [`examples/publish`](examples/publish/main.go), [`examples/consume`](examples/consume/main.go), [`examples/topology`](examples/topology/main.go), [`examples/deadletter`](examples/deadletter/main.go), [`examples/batch_publish`](examples/batch_publish/main.go), [`examples/batch_consume`](examples/batch_consume/main.go), [`examples/rpc`](examples/rpc/main.go), [`examples/delayed`](examples/delayed/main.go), [`examples/idempotent_consume`](examples/idempotent_consume/main.go), [`examples/ordered_consume`](examples/ordered_consume/main.go), [`examples/otel`](examples/otel/main.go)

### On the roadmap

The `v0.1.0` feature surface is in place — connection/reliability (TLS / `amqps://`,
SASL EXTERNAL, multi-node failover, supervised reconnect), publishing, topology,
consuming, codecs, RPC, delayed publish, observability, and the test/release
tooling (real-broker conformance suite, reconnect chaos test, throughput
benchmarks, coverage gate, CI + release automation). What remains is the
hardening and validation that earns the first tag: shaking the suite out against
real brokers, closing the gaps tracked in [`LATER.md`](LATER.md), and the release
automation itself.

See [`tasks/todo.md`](tasks/todo.md) for the live checklist and post-v0.1 ideas.

---

## Examples

| Example | Demonstrates |
| --- | --- |
| [`examples/publish`](examples/publish/main.go) | Typed publish, confirms, mandatory, returns, `PublishRetry` |
| [`examples/consume`](examples/consume/main.go) | Typed consume, three handler verdicts (Ack / Nack / `ErrRequeue`), `MaxRedeliveries`, `HandlerTimeout` |
| [`examples/topology`](examples/topology/main.go) | Multi-exchange topology, idempotent declare, reconnect redeclare |
| [`examples/deadletter`](examples/deadletter/main.go) | Dead-letter exchange / queue wiring |
| [`examples/batch_publish`](examples/batch_publish/main.go) | `PublishBatch` always-all, `[]PublishResult`, `ErrBatchTooLarge` |
| [`examples/batch_consume`](examples/batch_consume/main.go) | `BatchConsumerFor[M]` with `Size` + `FlushAfter` triggers and `multiple=true` ack |
| [`examples/rpc`](examples/rpc/main.go) | Request/reply via `CallerFor`/`ReplierFor` over direct reply-to: concurrent calls demuxed by `CorrelationID`, DLX on the request queue, `ErrCallTimeout` |
| [`examples/delayed`](examples/delayed/main.go) | Delayed delivery via `Message[M].Delay` + `DelayedTopic` against the `x-delayed-message` plugin |
| [`examples/idempotent_consume`](examples/idempotent_consume/main.go) | At-least-once dedupe by `MessageID` with a bounded LRU+TTL cache (SPEC §6.2.1); forced-duplicate handled exactly once |
| [`examples/ordered_consume`](examples/ordered_consume/main.go) | Strict per-queue ordering via `SingleActiveConsumer` + `Concurrency(1)`, preserved across an active-consumer failover |
| [`examples/otel`](examples/otel/main.go) | OpenTelemetry trace propagation: publish→consume spans share one trace, adapter from the OTel SDK to `warren/otel.Tracer` |

```bash
# Build all examples (no broker required)
make examples-build

# Smoke-run against a broker
make integration-up
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ make examples-smoke
make integration-down
```

---

## Observability

```go
conn, err := warren.Dial(ctx,
    warren.WithAddr(uri),
    warren.WithLogger(myLogger),           // log.Logger — std, slog, or custom
    warren.WithMetrics(promMetrics),       // default: Prometheus; WithoutMetrics() to disable
    warren.WithTracer(otelTracer),         // reserved (no spans today); add .Tracer(t) per builder for publish/consume spans
)
```

Warren aims to never emit AMQP URI credentials in clear text: the URIs it formats for logs, metrics, spans, and error strings are routed through `internal/redact`, which strips `userinfo`. This applies to the URIs Warren itself produces — secrets your own code logs are your responsibility.

---

## Error handling

```go
if err := pub.Publish(ctx, msg); err != nil {
    switch {
    case errors.Is(err, warren.ErrConfirmTimeout):
        // broker may have persisted — treat as duplicate risk
    case warren.IsTransient(err):
        // safe to retry at application level
    case warren.IsPermanent(err):
        // fix topology, permissions, or message shape
    case errors.Is(err, warren.ErrMessageTooLarge):
        // message exceeds PublisherBuilder.MaxMessageSizeBytes(n)
    }
}
```

---

## Development

```bash
make build              # compile all packages
make test               # unit tests (-race -cover)
make test-stress        # hammer scheduling-sensitive tests (-race, stress tag)
make cover              # coverage gate: per-package >=80%, critical-path >=95%
make lint               # golangci-lint

make integration-up     # RabbitMQ via Docker Compose
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ \
AMQP_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672 make test-integration
make integration-down

AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ \
make bench              # throughput benchmarks (msg/s per classic+quorum queue)
```

Pre-commit hook (opt-in): `make hooks` installs `lint` + `test` on commit.

---

## Documentation

| Document | Purpose |
| --- | --- |
| [`SPEC.md`](SPEC.md) | Full v1 public API, semantics, and success criteria |
| [`CHANGELOG.md`](CHANGELOG.md) | Release notes (Keep a Changelog) |
| [`tasks/plan.md`](tasks/plan.md) | Phased implementation plan |
| [pkg.go.dev](https://pkg.go.dev/github.com/brunomvsouza/warren) | Generated API reference |
| [`doc.go`](doc.go) | Package overview godoc |

---

## License

MIT — see [LICENSE](LICENSE).
