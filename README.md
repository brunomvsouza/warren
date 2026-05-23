# Warren

**A production-grade, generics-typed Go client for RabbitMQ (AMQP 0-9-1).**

Warren wraps [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) with a type-safe API and the operational layer every team rebuilds on the low-level driver: supervised reconnect with publisher confirms, centralized topology declaration, pluggable codecs, channel pooling across role-split TCP connections, and built-in observability (logging, Prometheus metrics, OpenTelemetry).

> **Status:** Active development toward [`v0.1.0`](SPEC.md). The public API is defined in [`SPEC.md`](SPEC.md); implementation follows [`tasks/plan.md`](tasks/plan.md). **Connection**, **Publisher**, and **Topology** are usable today; **Consumer**, RPC, batch APIs, and additional codecs are in progress.

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

**Design north stars** (from the spec): AMQP 0-9-1 protocol fidelity without misleading sugar, and a short, safe path for the common case (typed publish/consume over JSON).

**Reliability contract:** at-least-once delivery. Reconnect, `PublishRetry`, and confirm timeouts can produce duplicates — consumers must dedupe (default `MessageID` is UUIDv7). There is no exactly-once toggle.

---

## Quick start

Requires **Go 1.23+** and a **RabbitMQ 3.13 LTS or 4.x** broker.

```bash
# Until the first tag (v0.1.0), pin to main:
go get github.com/brunomvsouza/warren@main
```

```go
package main

import (
	"context"
	"fmt"
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

	conn, err := warren.Dial(ctx, warren.WithAddr("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: "orders", Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: "orders.created", Durable: true}},
		Bindings:  []warren.Binding{{Exchange: "orders", Queue: "orders.created", RoutingKey: "order.#"}},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		log.Fatal(err)
	}

	pub, err := warren.PublisherFor[Order](conn).
		Exchange("orders").
		RoutingKey("order.created").
		ConfirmTimeout(30 * time.Second).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close(context.Background())

	order := Order{ID: "ord-001", Amount: 42}
	if err := pub.Publish(ctx, warren.Message[Order]{Body: &order}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("published")
}
```

Run against a local broker:

```bash
docker compose -f docker-compose.integration.yml up -d --wait
AMQP_URL=amqp://guest:guest@localhost:5672/ go run ./examples/publish
```

---

## Architecture

A single `warren.Connection` owns a **pool of TCP connections** split by role (default: 2 publisher + 2 consumer sockets). Each socket has its own reconnect supervisor. Publishers borrow channels from a per-connection pool; consumers pin to one consumer connection (stable hash of consumer tag).

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

On reconnect, Warren runs a **synchronous barrier** before resuming traffic on that socket: reopen channels → redeclare attached topology → re-issue `basic.consume` on consumer channels → fire `WithOnReconnect`. `Publish` blocks on `ErrReconnecting` until the barrier clears (or the context is cancelled).

---

## Features

### Available now

- **Connection** — `Dial`, multi-address failover, TLS, PLAIN and EXTERNAL (mTLS) SASL, role-split TCP pool, heartbeat and frame sizing, `Health` / `Close` / `ForceReconnect`
- **Publisher** — `PublisherFor[M]`, publisher confirms, mandatory + returns, `PublishRetry`, confirm/publish timeouts, concurrent-safe `Publish`
- **Topology** — declarative exchanges, queues, bindings, dead-letter expansion; `Declare` + `AttachTo` for reconnect redeclare; degraded state on persistent redeclare failure
- **Codec** — lax JSON by default (Postel's Law — unknown fields are tolerated so producer-first deploys do not poison v1 DLQs); opt-in `codec.NewJSONStrict()` for compliance pipelines
- **Errors** — AMQP reply-code sentinels, `AMQPCode`, transient/permanent classifiers
- **Observability** — pluggable `log.Logger`, Prometheus metrics (default), OpenTelemetry tracer + W3C propagation helpers
- **Examples** — [`examples/publish`](examples/publish/main.go), [`examples/topology`](examples/topology/main.go), [`examples/deadletter`](examples/deadletter/main.go)

### On the roadmap (`v0.1.0`)

- **Consumer** — `ConsumerFor[M]`, prefetch, concurrency, handler timeouts, `MaxRedeliveries`, `ConsumeRaw`
- **Batch** — `PublishBatch` (always-all), `BatchConsumerFor[M]`
- **Patterns** — RPC (direct reply-to), delayed-message exchange helpers
- **Codecs** — Protobuf, CloudEvents (structured + binary)
- **Tooling** — `amqpmock/`, conformance suite, chaos/reconnect benchmarks per [SPEC §9](SPEC.md#9-success-criteria-v100)

See [`tasks/todo.md`](tasks/todo.md) for the live checklist.

---

## Examples

| Example | Demonstrates |
| --- | --- |
| [`examples/publish`](examples/publish/main.go) | Typed publish, confirms, mandatory, returns, `PublishRetry` |
| [`examples/topology`](examples/topology/main.go) | Multi-exchange topology, idempotent declare, reconnect redeclare |
| [`examples/deadletter`](examples/deadletter/main.go) | Dead-letter exchange / queue wiring |

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
    warren.WithTracer(otelTracer),         // no-op tracer baked in; swap for a real one
    warren.WithOnReconnect(func() { /* ... */ }),
    warren.WithOnBlocked(func(reason string) { /* connection.blocked */ }),
)
```

Credentials in AMQP URIs are **never** emitted in clear text — redaction is enforced at `internal/redact` for logs, metrics, spans, and error strings.

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
    }
    if code, ok := warren.AMQPCode(err); ok {
        _ = code // e.g. 404 ErrNotFound
    }
}
```

---

## Development

```bash
make build              # compile all packages
make test               # unit tests (-race -cover)
make lint               # golangci-lint

make integration-up     # RabbitMQ via Docker Compose
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ make test-integration
make integration-down
```

Pre-commit hook (opt-in): `make hooks` installs `lint` + `test` on commit.

**Contributing:** read [`SPEC.md`](SPEC.md) before changing public API — the spec is the contract. Implementation tasks live in [`tasks/plan.md`](tasks/plan.md) and [`tasks/todo.md`](tasks/todo.md). API changes require a spec amendment first.

---

## Documentation

| Document | Purpose |
| --- | --- |
| [`SPEC.md`](SPEC.md) | Full v1 public API, semantics, and success criteria |
| [`tasks/plan.md`](tasks/plan.md) | Phased implementation plan |
| [pkg.go.dev](https://pkg.go.dev/github.com/brunomvsouza/warren) | Generated API reference |
| [`doc.go`](doc.go) | Package overview godoc |

---

## License

MIT — see [LICENSE](LICENSE).
