# Warren

**Type-safe RabbitMQ for Go, with reliability built in.**

A generics-typed wrapper over [`amqp091-go`](https://github.com/rabbitmq/amqp091-go) that handles the production concerns you'd otherwise build by hand: supervised reconnect, publisher confirms, centralized topology, pluggable codecs, credential redaction, error classification, and native observability.

[![CI](https://github.com/brunomvsouza/warren/actions/workflows/ci.yml/badge.svg)](https://github.com/brunomvsouza/warren/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/brunomvsouza/warren)](https://goreportcard.com/report/github.com/brunomvsouza/warren)
[![Go Reference](https://pkg.go.dev/badge/github.com/brunomvsouza/warren.svg)](https://pkg.go.dev/github.com/brunomvsouza/warren)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> [!NOTE]
> **Pre-`v1.0` — the API may still change.** Delivery is **at-least-once**: consumers must dedupe by `MessageID` (auto-populated UUIDv7). The reliability features below are design goals backed by tests, not guarantees of zero message loss. [Full scope & status →](SPEC.md)

## Install

```bash
# Until the first tag (v0.1.0), pin to main:
go get github.com/brunomvsouza/warren@main
```

Requires **Go 1.25+** and **RabbitMQ 3.13 LTS or 4.x**.

## Quick start

Dial, declare, publish, consume — typed end to end. The handler receives the **decoded value**: return `nil` to `Ack`, an `error` to `Nack` (no requeue), or wrap [`ErrRequeue`](https://pkg.go.dev/github.com/brunomvsouza/warren#ErrRequeue) to requeue.

```go
package main

import (
	"context"
	"errors"
	"log"

	"github.com/brunomvsouza/warren"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func main() {
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	// Declared once; redeclared automatically on every reconnect.
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: "orders", Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: "orders.created", Durable: true}},
		Bindings:  []warren.Binding{{Exchange: "orders", Queue: "orders.created", RoutingKey: "order.#"}},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		log.Fatal(err)
	}
	if err := topo.AttachTo(conn); err != nil {
		log.Fatal(err)
	}

	pub, err := warren.PublisherFor[Order](conn).Exchange("orders").RoutingKey("order.created").Build()
	if err != nil {
		log.Fatal(err)
	}
	if err := pub.Publish(ctx, warren.Message[Order]{Body: &Order{ID: "ord-001", Amount: 42}}); err != nil {
		log.Fatal(err)
	}

	con, err := warren.ConsumerFor[Order](conn).Queue("orders.created").Concurrency(4).Build()
	if err != nil {
		log.Fatal(err)
	}
	err = con.Consume(ctx, func(ctx context.Context, o Order) error {
		log.Printf("order %s (amount %d)", o.ID, o.Amount)
		return nil // nil → Ack
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

For TLS, multi-node failover, dead-letters, and OpenTelemetry wiring, see the [examples](#examples) below.

## Why Warren?

Raw `amqp091-go` is correct and minimal. Production clients need more — and Warren is what you'd otherwise write yourself:

| Concern | Raw driver | Warren |
| --- | --- | --- |
| **Typing** | `[]byte` + manual JSON | `PublisherFor[M]` / `ConsumerFor[M]` with pluggable codecs |
| **Reconnect** | Roll your own | Per-socket supervisors + synchronous redeclare barrier |
| **Throughput** | One connection serializes I/O | Role-split TCP pool + per-connection channel pool |
| **Poison messages** | Easy infinite requeue loops | `Nack(requeue=false)` by default; requeue is opt-in |
| **Credentials in logs** | Easy to leak | URI `userinfo` stripped from every log, metric, span, and error |
| **Broker errors** | Opaque `*amqp091.Error` | Reply-code sentinels + `IsTransient` / `IsPermanent` |
| **Observability** | None | Pluggable logging, Prometheus, OpenTelemetry trace propagation |

**Design north stars:** AMQP 0-9-1 protocol fidelity without misleading sugar, and a short, safe path for the common case.

## How it works

One `warren.Connection` owns a **pool of TCP sockets split by role** (default: 2 publisher + 2 consumer). Each socket has its own reconnect supervisor. Publishers borrow channels from a per-connection pool; consumers pin to one consumer socket by stable hash of consumer tag.

```mermaid
flowchart LR
  P[PublisherFor M] --> PUB[Publisher pool<br/>2 sockets + channel pools]
  C[ConsumerFor M] --> CON[Consumer pool<br/>2 sockets]
  T[Topology] --> R[Reconnect barrier]
  PUB --> B[(RabbitMQ)]
  CON --> B
  R -->|redeclare + resubscribe| B
```

On reconnect, a **synchronous barrier** runs before traffic resumes on that socket: reopen channels → redeclare attached topology → re-issue `basic.consume` → fire `WithOnReconnect`. `Publish` blocks on `ErrReconnecting` until the barrier clears.

## Features

| Area | What you get |
| --- | --- |
| **Connection** | Role-split TCP pool, PLAIN/EXTERNAL SASL, TLS (`amqps://`), multi-node failover, `Health` / `Close` / `ForceReconnect` |
| **Publisher** | Publisher confirms, mandatory + returns, `PublishRetry`, `WithPublishRateLimit`, confirm/publish timeouts, payload-size guardrail, concurrency-safe |
| **Topology** | Declarative exchanges/queues/bindings, dead-letter expansion, reconnect redeclare, degraded-state callbacks |
| **Consumer** | Prefetch, concurrency, `MaxInFlightBytes` memory guardrail, `MaxRedeliveries`, `HandlerTimeout`, `AutoAck`, `SingleActiveConsumer`, `WithQueueDepthSampler` (`queue_depth`/`dlq_depth` gauges), `basic.cancel` surfacing, `ConsumeRaw` escape hatch |
| **Batch** | `PublishBatch` (always-all, `[]PublishResult`) and `BatchConsumerFor[M]` with size + timer flush triggers |
| **Codec** | Lax JSON (default), strict JSON, Protobuf, CloudEvents (structured + binary) |
| **Errors** | AMQP reply-code sentinels, `AMQPCode`, `IsTransient` / `IsPermanent` |
| **Observability** | Pluggable `log.Logger`, Prometheus metrics, OpenTelemetry spans with W3C propagation that survives DLX bounces |
| **Patterns** | RPC over direct reply-to (`CallerFor` / `ReplierFor`), delayed publish via `Message.Delay` + `DelayedTopic` |
| **Testing** | `NewDeliveryFixture` / `NewBatchFixture` to unit-test handlers without a live broker |

Full semantics and contracts live in [`SPEC.md`](SPEC.md). What remains before `v0.1.0` is hardening against real brokers and release automation — see [`tasks/todo.md`](tasks/todo.md) and [`LATER.md`](LATER.md).

## Examples

Every example builds on each PR and smoke-runs against a real broker in CI.

| Example | Demonstrates |
| --- | --- |
| [`publish`](examples/publish/main.go) | Typed publish, confirms, mandatory, returns, `PublishRetry` |
| [`consume`](examples/consume/main.go) | Typed consume, Ack / Nack / `ErrRequeue`, `MaxRedeliveries`, `HandlerTimeout` |
| [`topology`](examples/topology/main.go) | Multi-exchange topology, idempotent declare, reconnect redeclare |
| [`deadletter`](examples/deadletter/main.go) | Dead-letter exchange / queue wiring |
| [`batch_publish`](examples/batch_publish/main.go) | `PublishBatch` always-all, `[]PublishResult`, `ErrBatchTooLarge` |
| [`batch_consume`](examples/batch_consume/main.go) | `BatchConsumerFor[M]` with size + `FlushAfter` triggers |
| [`rpc`](examples/rpc/main.go) | Request/reply over direct reply-to, demuxed by `CorrelationID` |
| [`delayed`](examples/delayed/main.go) | Delayed delivery via `Message.Delay` + `DelayedTopic` |
| [`idempotent_consume`](examples/idempotent_consume/main.go) | At-least-once dedupe by `MessageID` with an LRU+TTL cache |
| [`ordered_consume`](examples/ordered_consume/main.go) | Strict per-queue ordering via `SingleActiveConsumer` |
| [`otel`](examples/otel/main.go) | OpenTelemetry: publish→consume spans share one trace |

```bash
make examples-build      # build all examples (no broker)
make integration-up
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ make examples-smoke
make integration-down
```

## Error handling

```go
if err := pub.Publish(ctx, msg); err != nil {
	switch {
	case errors.Is(err, warren.ErrConfirmTimeout):
		// broker may have persisted — treat as duplicate risk
	case warren.IsTransient(err):
		// safe to retry at the application level
	case warren.IsPermanent(err):
		// fix topology, permissions, or message shape
	}
}
```

## Documentation

| Document | Purpose |
| --- | --- |
| [pkg.go.dev](https://pkg.go.dev/github.com/brunomvsouza/warren) | Generated API reference |
| [`SPEC.md`](SPEC.md) | Full v1 public API, semantics, and success criteria |
| [`CHANGELOG.md`](CHANGELOG.md) | Release notes (Keep a Changelog) |
| [`tasks/plan.md`](tasks/plan.md) | Phased implementation plan |

## Contributing

```bash
make build        # compile all packages
make test         # unit tests (-race -cover)
make lint         # golangci-lint
make integration-up && make test-integration && make integration-down
```

Pre-commit hook (opt-in): `make hooks` installs `lint` + `test` on commit.

## License

MIT — see [LICENSE](LICENSE).
