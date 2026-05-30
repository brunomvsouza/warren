# Warren

> [!WARNING]
> **This project is in active, early development.** 
> While the goal is to provide a highly reliable operational layer, stability is **not yet guaranteed**. Although we aim to maintain a stable public API as defined in [`SPEC.md`](SPEC.md), occasional breaking changes may still occur prior to `v1.0.0`. Use in production environments at your own risk.

**A production-grade, generics-typed Go client for RabbitMQ (AMQP 0-9-1).**

Warren wraps [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) with a type-safe API and a **hardened operational layer** built for high-scale reliability. It embeds the "SRE batteries" every production team needs: supervised reconnect with publisher confirms, centralized topology declaration, pluggable codecs, channel pooling across role-split TCP connections, **intelligent error classification (transient vs. permanent)**, **safety guardrails (credential redaction, fail-fast validation, payload caps)**, and native observability (logging, Prometheus, OpenTelemetry).

> **Current Status:** Active development toward [`v0.1.0`](SPEC.md). Implementation follows [`tasks/plan.md`](tasks/plan.md). **Connection**, **Publisher**, **Topology**, **Consumer** (with `MaxRedeliveries` + handler timeouts), **batch publish/consume**, the **JSON / Protobuf / CloudEvents codecs**, **end-to-end OpenTelemetry tracing** (publisher + consumer), and the **RPC + delayed-message patterns** are usable today; the test tooling is in progress.

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

	// 1. Dial with role-split connection pool (default: 2 pub, 2 con)
	conn, err := warren.Dial(ctx, warren.WithAddr("amqp://guest:guest@localhost:5672/"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	// 2. Declare topology (idempotent; redeclared automatically on reconnect)
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: "orders", Kind: warren.ExchangeTopic, Durable: true}},
		Queues:    []warren.Queue{{Name: "orders.created", Durable: true}},
		Bindings:  []warren.Binding{{Exchange: "orders", Queue: "orders.created", RoutingKey: "order.#"}},
	}
	if err := topo.Declare(ctx, conn); err != nil {
		log.Fatal(err)
	}
	topo.AttachTo(conn) // Ensure it redeclares on every reconnect barrier

	// 3. Publish a typed message
	pub, _ := warren.PublisherFor[Order](conn).
		Exchange("orders").
		RoutingKey("order.created").
		Build()
	
	_ = pub.Publish(ctx, warren.Message[Order]{Body: &Order{ID: "ord-001", Amount: 42}})

	// 4. Consume typed messages
	con, _ := warren.ConsumerFor[Order](conn).
		Queue("orders.created").
		Concurrency(4).
		Build()

	con.Consume(ctx, func(ctx context.Context, o *Order) error {
		fmt.Printf("Processing order %s\n", o.ID)
		return nil // Ack
	})
}
```

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

Warren is built with "production-first" principles, embedding several reliability patterns directly into the core:

- **Synchronous Reconnect Barrier:** Guaranteed restoration of all topology (exchanges, queues, bindings) before traffic resumes, preventing message loss or misrouting on fresh connections.
- **Intelligent Error Classification:** Native `IsTransient(err)` and `IsPermanent(err)` helpers allow your application to implement robust retry logic based on actual AMQP reply codes.
- **Safety Guardrails:** 
    - **Fail-Fast Validation:** Validates `UserID` and message headers client-side to prevent "channel-close" errors that crash publisher sockets.
    - **Payload Size Caps:** Rejects oversized messages before they hit the broker, protecting your infrastructure from OOM.
    - **Credential Redaction:** Every log line, metric label, and trace span is automatically passed through `internal/redact` to strip secrets.
- **Degraded State Awareness:** If the topology fails to redeclare persistently, the connection enters a `degraded` state, triggering `OnTopoDegraded` callbacks and metrics, and providing a `ForceReconnect` escape hatch for operators.

---

## Features

### Available now

- **Connection** — `Dial`, role-split TCP pool, PLAIN SASL, heartbeat and frame sizing, multi-address cluster failover (`WithAddrs`, in-order initial connect + round-robin rotation on reconnect), `Health` / `Close` / `ForceReconnect` (TLS and SASL EXTERNAL options are wired up under production hardening — see roadmap)
- **Publisher** — `PublisherFor[M]`, publisher confirms, mandatory + returns, `PublishRetry`, confirm/publish timeouts, concurrent-safe `Publish`, payload size guardrail
- **Topology** — declarative exchanges, queues, bindings, dead-letter expansion (quorum + DLX gets `x-dead-letter-strategy: at-least-once` automatically); `Declare` + `AttachTo` for reconnect redeclare; degraded state on persistent redeclare failure
- **Consumer** — `ConsumerFor[M]`, prefetch, concurrency, handler error mapping (`ErrRequeue`), re-subscribe loop, `ConsumeRaw`; `MaxRedeliveries` (quorum `DeliveryLimit` + `x-death` + in-process counters) and per-handler `HandlerTimeout` with a configurable `HandlerTimeoutVerdict`; opt-in `AutoAck()` (at-most-once no-ack, with a sampled drop-warning when handlers error)
- **Batch** — `Publisher.PublishBatch` (always-all, single-channel, `[]PublishResult` + `ErrBatchTooLarge`) and `BatchConsumerFor[M]` with size- and timer-based flush triggers and `multiple=true` acking
- **Codec** — lax JSON by default (Postel's Law — unknown fields are tolerated so producer-first deploys do not poison v1 DLQs); opt-in `codec.NewJSONStrict()`; `codec.NewProtobuf()`; CloudEvents in both `codec.NewCloudEventsStructured()` and `codec.NewCloudEventsBinary()` modes (AMQP protocol binding via the `HeaderCodec` interface)
- **Errors** — AMQP reply-code sentinels, `AMQPCode`, transient/permanent classifiers
- **Observability** — pluggable `log.Logger`, Prometheus metrics (default), OpenTelemetry tracer + W3C propagation helpers; **publisher** spans inject trace context into AMQP headers before the frame is written and **consumer** spans extract it on the other side, so continuity survives DLX bounces (the broker preserves headers verbatim); `BatchConsumer` spans carry one Link per message
- **Patterns** — RPC over direct reply-to (`CallerFor[Req, Resp]` + `ReplierFor[Req, Resp]`, at-least-once replies deduped by `CorrelationID`, DLX-on-request-queue validation); delayed publish via `Message[M].Delay` + the `DelayedTopic` helper for the `x-delayed-message` plugin
- **Examples** — [`examples/publish`](examples/publish/main.go), [`examples/consume`](examples/consume/main.go), [`examples/topology`](examples/topology/main.go), [`examples/deadletter`](examples/deadletter/main.go), [`examples/batch_publish`](examples/batch_publish/main.go), [`examples/batch_consume`](examples/batch_consume/main.go), [`examples/rpc`](examples/rpc/main.go), [`examples/delayed`](examples/delayed/main.go)

### On the roadmap (`v0.1.0`)

- **Consumer cancellation** — `OnCancel(func(reason))` callback + `consumer_cancelled_total` metric for broker `basic.cancel`
- **Production hardening** — TLS / `amqps://`, SASL EXTERNAL (mTLS), remaining connection options, panic isolation for user callbacks
- **Tooling** — `amqpmock/`, `amqptest/` (testcontainers), conformance suite, chaos/reconnect benchmarks

See [`tasks/todo.md`](tasks/todo.md) for the live checklist.

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
make lint               # golangci-lint

make integration-up     # RabbitMQ via Docker Compose
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ \
AMQP_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672 make test-integration
make integration-down
```

Pre-commit hook (opt-in): `make hooks` installs `lint` + `test` on commit.

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
