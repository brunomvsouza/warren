# Spec: amqp.go

A modern, ergonomic Go client for AMQP 0-9-1 (RabbitMQ), built on top of
`github.com/rabbitmq/amqp091-go`.

> **Status:** Draft v1 spec. Implementation is **in progress** (see `tasks/todo.md`);
> the API surface and contracts match **plan Rev 6 / SPEC §10**. Final approval still
> gates `v1.0.0`; `v0.1.0` follows SPEC §9 success criteria.

> **Design north stars:**
> 1. **AMQP 0-9-1 protocol fidelity** — surface real primitives
>    (exchanges, queues, bindings, channels, confirms, returns,
>    mandatory, blocked notifications) without hiding them behind
>    misleading sugar.
> 2. **Ease of use** — the common case (publish/consume typed messages
>    over JSON) is short, type-safe, and impossible to get wrong by
>    accident (no silent ack-loss, no silent poison loops).

---

## 1. Objective

A higher-level Go library that wraps `rabbitmq/amqp091-go` with a
generics-based, type-safe API. The library handles the production-grade
concerns every team rebuilds from scratch on top of the low-level driver:

- Automatic reconnect with publisher confirms preserved
- Generics-typed messages with pluggable codecs
- Centralized topology declaration (separate from publishers/consumers)
- Built-in observability: metrics, logging, OpenTelemetry tracing
- Multi-connection pool, with role-based separation
  (publisher / consumer) and a per-connection channel pool
- Common patterns out of the box: batch consume + publish, RPC, delayed
  messages, DLX/DLQ
- Graceful shutdown that drains in-flight work

### Reliability bar

This library is built to be **trusted in flight paths that handle
billions of messages per day**. That bar is the tie-breaker on every
design decision below. Concretely it means:

- **No silent message loss.** Any path that could drop a message
  without surfacing an error to the caller is a defect. Default
  behaviour favours surfacing over swallowing; ergonomic shortcuts
  (`AutoAck`, `NoWait`, etc.) are opt-in with prominent godoc on
  what they trade away.
- **No silent backpressure failure.** When a buffer is full, a pool
  is exhausted, or the broker blocks the connection, callers see a
  classifiable error (`ErrConnectionBlocked`,
  `ErrChannelPoolExhausted`, `ErrPublishNacked`) — never a silent
  stall or an unbounded queue.
- **No single-TCP-socket bottleneck on the hot path.** The connection
  abstraction can fan out across multiple TCP connections per role
  (publishers and consumers), because `amqp091-go` serializes I/O
  per connection. A single socket is fine for tens of thousands of
  messages/second; sustained higher rates require the role-based
  pools documented in §6.1.
- **No silent poison loop.** The default for a handler error is
  `Nack(requeue=false)`. Requeue is opt-in via wrapping the error
  with `ErrRequeue`, and `MaxRedeliveries` plus the quorum-queue
  `x-delivery-limit` cap the loop. Two-counter design (in-process +
  `x-death`) bounds both `ErrRequeue` loops and DLX-bounce loops; on
  quorum queues, `x-delivery-limit` is the preferred mechanism.
- **No silent credential leak.** URIs in logs and metrics have
  `userinfo` redacted. SASL EXTERNAL (mTLS-only) is supported for
  deployments that need passwordless authentication.
- **No silent duplicate.** `PublishRetry`, automatic
  reconnect, and any path that resolves a publish with
  `ErrConfirmTimeout` or `ErrChannelClosed` can produce a duplicate:
  the broker may have persisted the message before the ack was
  observed. Consumers MUST be idempotent (dedupe by `MessageID`,
  which is auto-populated with UUIDv7 by default — see §6.2.1). The
  `publisher_retry_total{exchange, reason}` metric makes retries observable so
  the duplicate budget is never invisible.
- **Observable at scale.** Every operation emits a metric and a
  tracer span by default; metric labels are deliberately bounded to
  avoid cardinality explosions, with high-cardinality labels
  opt-in.
- **Graceful under chaos.** Reconnect is automatic and idempotent:
  topology re-declared, consumers automatically re-issue
  `basic.consume` on the new channel, in-flight handler contexts
  are cancelled on channel close so handlers can abort orphaned
  work.

The Success Criteria in §9 encode quantitative versions of this bar.

### Target users

Go developers building message-driven systems against RabbitMQ.

### Success looks like

- Publish a strongly-typed message in <10 lines including connection
  and topology setup.
- Consume strongly-typed messages with concurrency, manual or
  error-driven Ack/Nack semantics, in <15 lines.
- Connection loss is invisible (besides metric blips); confirms and
  topology re-declared automatically.
- OpenTelemetry traces span publish → consume across services with no
  manual context propagation.
- No silent failure modes: forgetting to ack is impossible; poison
  messages do not loop by default; unroutable publishes raise an error.

---

## 2. Tech Stack

| Component       | Choice                                             |
| --------------- | -------------------------------------------------- |
| Language        | Go **1.23+**                                       |
| Module path     | `github.com/brunomvsouza/amqp`                     |
| Repo            | `github.com/brunomvsouza/amqp.go` (working dir)    |
| AMQP transport  | `github.com/rabbitmq/amqp091-go` (BSD-2-Clause)    |
| Broker          | RabbitMQ 3.13 LTS and 4.x                          |
| Mocks           | `go.uber.org/mock` (gomock) — in `amqpmock/`       |
| Metrics default | `github.com/prometheus/client_golang`              |
| Tracing         | `go.opentelemetry.io/otel` (opt-in)                |
| UUID            | `github.com/google/uuid`                           |
| Testing         | stdlib `testing` + `testify` + `testcontainers-go` + `goleak` |
| Lint            | `golangci-lint`                                    |
| License         | MIT                                                |

Dependency policy: minimal. Anything beyond the table above requires
explicit approval. The root package imports zero test-only code at
runtime; gomock is a build/test dependency.

---

## 3. Commands

```bash
# Build
go build ./...

# Unit tests (fast, no broker)
go test -race -cover ./...

# Integration tests (spins up RabbitMQ via testcontainers-go)
go test -race -tags=integration ./...

# Conformance tests (AMQP 0-9-1 protocol compliance)
go test -race -tags=conformance ./...

# All test levels (CI)
go test -race -cover -tags='integration conformance' ./...

# Fuzz (per target, time-bounded)
go test -fuzz=FuzzCodecJSON -fuzztime=10m ./codec

# Bench
go test -bench=. -benchmem -run=^$ ./...

# Lint
golangci-lint run ./...

# Generate gomock mocks into amqpmock/
go generate ./...

# Tidy
go mod tidy
```

A `Makefile` will expose these as `make build`, `make test`,
`make test-integration`, `make test-conformance`, `make lint`,
`make mocks`, `make bench`, `make fuzz`.

---

## 4. Project Structure

```
.
├── go.mod
├── go.sum
├── LICENSE                       # MIT
├── README.md
├── SPEC.md                       # this document
├── CHANGELOG.md                  # Keep a Changelog format
├── Makefile
├── doc.go                        # top-level godoc package overview
│
├── connection.go                 # Connection + supervised reconnect
├── channelpool.go                # internal channel pool
├── publisher.go                  # Publisher[M]
├── publisher_builder.go          # PublisherFor[M] generic builder
├── consumer.go                   # Consumer[M]
├── consumer_builder.go           # ConsumerFor[M] generic builder
├── batch_consumer.go             # BatchConsumer[M]
├── batch_consumer_builder.go     # BatchConsumerFor[M]
├── message.go                    # Message[M] struct + defaulting
├── delivery.go                   # Delivery[M], Batch[M] envelope types
├── topology.go                   # Topology, DeadLetter, Declare, AttachTo
├── retry.go                      # RetryPolicy + backoff
├── rpc.go                        # Caller[Req,Resp], Replier[Req,Resp]
├── delay.go                      # delayed-message-exchange helpers
├── errors.go                     # sentinel errors + classifiers
│
├── options_connection.go         # WithAddrs, WithTLSConfig, …
│
├── internal/
│   ├── confirms/                 # publisher confirm tracker
│   ├── reconnect/                # supervised reconnect loop
│   ├── amqperror/                # *amqp091.Error → reply-code sentinel translator
│   ├── headers/                  # AMQP header helpers (otel propagation, x-death)
│   └── redact/                   # AMQP URI/credential redaction
│
├── codec/
│   ├── codec.go                  # Codec interface
│   ├── json.go                   # built-in JSON
│   ├── protobuf.go               # built-in Protobuf
│   └── cloudevents.go            # built-in CloudEvents
│
├── log/
│   ├── logger.go                 # Logger interface
│   ├── slog.go                   # adapter for stdlib log/slog
│   ├── std.go                    # adapter for stdlib log
│   └── noop.go
│
├── metrics/
│   ├── metrics.go                # ClientMetrics, PublisherMetrics, ConsumerMetrics
│   ├── prometheus.go             # Prometheus implementations
│   └── noop.go
│
├── otel/
│   ├── tracer.go                 # Tracer wrapper (OTel semantic conventions)
│   └── propagation.go            # AMQP-header context propagation
│
├── amqpmock/                     # gomock-generated; refreshed by `go generate`
│   ├── connection.go
│   ├── publisher.go
│   ├── consumer.go
│   └── codec.go
│
├── amqptest/                     # public testcontainers helper (exported for downstream users)
│   ├── rabbitmq.go               # spins up RabbitMQ with delayed-message plugin
│   └── certs/                    # pre-generated TLS test certs
│
├── examples/
│   ├── publish/main.go
│   ├── consume/main.go
│   ├── batch_consume/main.go
│   ├── batch_publish/main.go
│   ├── rpc/main.go
│   ├── delayed/main.go
│   ├── deadletter/main.go
│   ├── topology/main.go
│   ├── idempotent_consume/main.go   # canonical dedupe-by-MessageID pattern (§6.2.1)
│   ├── ordered_consume/main.go      # SingleActiveConsumer + Concurrency(1) for strict ordering with failover
│   └── otel/main.go
│
└── .github/
    └── workflows/
        ├── ci.yml                # lint + test + integration on push/PR
        └── release.yml           # tag → GitHub Release (`gh release create --generate-notes`; no goreleaser)
```

---

## 5. Code Style

### Conventions

- **Functional options for `Connection` only.** They are **not generic**:
  `WithAddr`, `WithTLSConfig`, `WithLogger`, etc.
- **Generic builders for `Publisher[M]`, `Consumer[M]`, `BatchConsumer[M]`,
  `Caller[Req,Resp]`, `Replier[Req,Resp]`.** Type parameter appears
  exactly once, at the builder constructor. Builder methods are not
  generic.
- **`Message[M]` is a plain struct with exported fields.** No builder
  ceremony. Defaults applied at publish time.
- **Handler signature is payload-first**: `func(ctx, M) error`.
  Returning `nil` Acks; any other error Nacks **without requeue**
  (poison messages go to DLX, never loop). To request requeue, wrap
  the error: `fmt.Errorf("transient: %w", amqp.ErrRequeue)`.
  Escape hatch: `Consumer[M].ConsumeRaw(ctx, func(ctx, Delivery[M]) error)`.
- **`Topology` is the only place that declares.** Publisher/Consumer
  receive names; they do not declare exchanges/queues/bindings.
- **Sentinel errors** in `errors.go`. Wrap with
  `fmt.Errorf("amqp: …: %w", err)` at boundaries. Callers use
  `errors.Is`/`errors.As`.
- **`context.Context` is mandatory** on every blocking operation
  (`Dial`, `Publish`, `PublishBatch`, `Consume`, `Health`, `Close`).
- **No magic sleeps.** Backpressure is `prefetch_count`. Retry delay is
  explicit (`RetryPolicy`), not a hidden `time.Sleep` in the consume
  loop.
- **No globals, no `init()`-side-effects.** Everything constructible
  and injectable.
- **Mocks** are generated into `amqpmock/`; root package has no
  runtime dependency on gomock.

### Example: publish

```go
package main

import (
    "context"
    "log/slog"
    "os/signal"
    "syscall"
    "time"

    "github.com/brunomvsouza/amqp"
    "github.com/brunomvsouza/amqp/codec"
    amqplog "github.com/brunomvsouza/amqp/log"
)

type OrderPlaced struct {
    OrderID string  `json:"order_id"`
    Total   float64 `json:"total"`
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    conn, err := amqp.Dial(ctx,
        amqp.WithAddrs([]string{
            "amqps://user:pass@rabbit-1:5671",
            "amqps://user:pass@rabbit-2:5671",
        }),
        amqp.WithLogger(amqplog.NewSlog(slog.Default())),
    )
    if err != nil {
        panic(err)
    }
    defer conn.Close(ctx)

    // Topology — declared once, separately.
    topo := amqp.Topology{
        Exchanges: []amqp.Exchange{
            {Name: "orders", Kind: amqp.ExchangeTopic, Durable: true},
        },
        Queues: []amqp.Queue{
            {Name: "orders.placed", Durable: true},
        },
        Bindings: []amqp.Binding{
            {Exchange: "orders", Queue: "orders.placed", RoutingKey: "orders.placed"},
        },
    }
    if err := topo.Declare(ctx, conn); err != nil {
        panic(err)
    }
    topo.AttachTo(conn) // re-declare automatically on reconnect

    // Publisher — references topology by name; does not declare anything.
    pub, err := amqp.PublisherFor[OrderPlaced](conn).
        Codec(codec.NewJSON()).
        Exchange("orders").
        RoutingKey("orders.placed").
        Mandatory().
        ConfirmTimeout(5 * time.Second).
        Build()
    if err != nil {
        panic(err)
    }
    defer pub.Close(ctx)

    err = pub.Publish(ctx, amqp.Message[OrderPlaced]{
        Body:          &OrderPlaced{OrderID: "o-42", Total: 99.90},
        CorrelationID: "trace-abc",
    })
    if err != nil {
        panic(err)
    }
}
```

### Example: consume

```go
con, err := amqp.ConsumerFor[OrderPlaced](conn).
    Codec(codec.NewJSON()).
    Queue("orders.placed").
    Concurrency(8).
    Prefetch(100).
    Build()
if err != nil { panic(err) }

err = con.Consume(ctx, func(ctx context.Context, o OrderPlaced) error {
    if !o.Valid() {
        return amqp.ErrPoison           // Nack(requeue=false) → DLX
    }
    if err := process(ctx, o); err != nil {
        return fmt.Errorf("retry: %w", amqp.ErrRequeue)  // Nack(requeue=true)
    }
    return nil                          // Ack
})
```

### Example: consume raw (escape hatch)

```go
err = con.ConsumeRaw(ctx, func(ctx context.Context, d amqp.Delivery[OrderPlaced]) error {
    if d.Redelivered() && d.DeathCount() > 3 {
        return d.Nack(false) // give up
    }
    o := d.Body()
    return d.AckIf(process(ctx, *o))
})
```

### Naming

- **Package names**: lowercase, short, no underscores (`codec`, `log`,
  `metrics`, `otel`, `amqpmock`).
- **Exported types**: `PascalCase`. Constructors `NewX` or `XFor[M]`
  for generic builders.
- **Connection options**: `With*` (positive), `Without*` (negation).
- **Builder methods**: noun verbs (`Codec(c)`, `Exchange(name)`,
  `Concurrency(n)`), not `WithX`.
- **Sentinel errors**: `Err<Condition>`.

---

## 6. Public API Surface (v1)

### Note on AMQP 0-9-1 vs RabbitMQ

This library targets **AMQP 0-9-1 as implemented by RabbitMQ** (3.13 LTS
and 4.x). It does not aim to be portable to other AMQP 0-9-1 brokers
(Qpid, ActiveMQ, etc) — several features below are RabbitMQ extensions,
flagged inline.

**RabbitMQ extensions exposed:**
- Publisher confirms (`confirm.select` + `basic.ack`/`basic.nack` on the
  publish-side channel).
- `connection.blocked` / `connection.unblocked` notifications.
- `basic.nack` with `multiple` + `requeue` flags (AMQP 0-9-1 core only
  has `basic.reject`; `nack` is universally implemented but is a
  RabbitMQ extension by origin).
- Direct reply-to pseudo-queue (`amq.rabbitmq.reply-to`).
- Queue arguments: `x-message-ttl`, `x-max-length`, `x-max-length-bytes`,
  `x-overflow`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`,
  `x-queue-type`, `x-max-priority`.
- Header `x-death` (delivery-count tracking for DLX cycles).
- `rabbitmq_delayed_message_exchange` plugin
  (`ExchangeKindDelayed = "x-delayed-message"`).

**Spec features intentionally NOT exposed** because RabbitMQ does not
implement them:
- `basic.publish` `immediate` flag — removed in RabbitMQ 3.0 (channel
  is closed by broker if set).
- `basic.consume` `no-local` flag — silently ignored by RabbitMQ.

**Spec features exposed with a RabbitMQ-specific note:**
- `basic.qos` `prefetch-size` (bytes) — RabbitMQ ignores this; we
  expose `PrefetchBytes` for protocol parity, marked as no-op.
- `basic.qos` `global` flag — RabbitMQ applies it per-channel rather
  than per-connection as the AMQP 0-9-1 spec says. Exposed as
  `ChannelQoS` to reflect the actual semantics.

**Stream queues — scope.** `QueueTypeStream` is exposed in §6.6 for
topology declaration. Full stream-protocol semantics — `x-stream-offset`
(`first`/`last`/`next`/timestamp/offset), `x-stream-max-age`,
`x-stream-max-segment-size-bytes`, dedicated stream-consume API, and
super-stream support — are **out of scope for v0.1** and tracked for
**v0.2**. Stream-typed queues declared via `Topology` work today for
publish-from-RabbitMQ-classic-AMQP-clients reading via the stream
protocol elsewhere, but native stream consume from this library
ships in v0.2. The "Reliability bar" applies to the v0.1 surface
(classic + quorum); stream v0.2 will publish updated success
criteria.

### 6.1 Connection

```go
type Connection struct { /* … */ }

func Dial(ctx context.Context, opts ...ConnectionOption) (*Connection, error)
func (c *Connection) Close(ctx context.Context) error    // drains in-flight work
func (c *Connection) Health(ctx context.Context) error

// SASLMechanism selects the SASL mechanism for the AMQP 0-9-1
// connection.start-ok handshake. Default is PLAIN. EXTERNAL uses the
// underlying TLS client certificate (mTLS) and ignores any user/pass.
type SASLMechanism string

const (
    SASLPlain    SASLMechanism = "PLAIN"
    SASLExternal SASLMechanism = "EXTERNAL"
)
```

| Option                                  | Default                          |
| --------------------------------------- | -------------------------------- |
| `WithAddr(uri string)`                  | required if no `WithAddrs`       |
| `WithAddrs(uris []string)`              | cluster failover                 |
| `WithTLSConfig(*tls.Config)`            | nil                              |
| `WithVHost(v string)`                   | from URI or "/"                  |
| `WithAuth(user, pass string)`           | from URI                         |
| `WithSASLMechanism(SASLMechanism)`      | `SASLPlain`                      |
| `WithHeartbeat(d time.Duration)`        | 10s (TCP-partition detection window ≈ 2 × heartbeat = 20s) |
| `WithChannelMax(n uint16)`              | 0 (server-driven)                |
| `WithFrameMax(n int)`                   | 131072 (128 KiB; raise for streaming large messages, see §6.1 sizing notes) |
| `WithDialer(net.Dialer)`                | default                          |
| `WithConnectionName(name string)`       | `<binary>-<hostname>-<pid>`      |
| `WithClientProperties(map[string]any)`  | name + version + connection name |
| `WithPublisherConnections(n int)`       | 2 (raise to 4–8 for sustained > 50k msg/s; see §6.1 sizing) |
| `WithConsumerConnections(n int)`        | 2 (one socket cannot bound dispatch under broker restart)   |
| `WithChannelPoolSize(n int)`            | 8 (per publisher connection)     |
| `WithConnectDelay(d)`                   | 1s                               |
| `WithReconnectBackoff(p RetryPolicy)`   | exponential + jitter             |
| `WithOnReconnect(func())`               | nil                              |
| `WithOnBlocked(func(reason string))`    | nil                              |
| `WithOnResubscribe(func(queue string))` | nil                              |
| `WithOnTopologyDegraded(func(error))`   | nil (logs to `WithLogger` only)  |
| `WithLogger(log.Logger)`                | NoOp                             |
| `WithMetrics(metrics.ClientMetrics)`    | Prometheus                       |
| `WithoutMetrics()`                      | — (last-wins with `WithMetrics`) |
| `WithTracer(otel.Tracer)`               | NoOp tracer (always-on)          |

**Option precedence.** All `With*` / `Without*` options follow
**last-wins** semantics — the last call in the option slice
overrides any earlier setting of the same field. `WithoutMetrics()`
is equivalent to `WithMetrics(metrics.NoOp{})`. The same rule
applies on per-builder options
(`PublisherBuilder.WithoutMetrics()` vs `Metrics(...)` ): the last
call on the same builder wins. Builder-level options override
connection-level options for the same field on the resulting
component.

#### Connection topology and concurrency model

A single `*Connection` wraps an internal **pool of TCP connections**,
split by role:

- `WithPublisherConnections(n)` — number of dedicated publisher
  connections. Default `2` (minimum viable for graceful failover:
  one socket draining while the other is reconnecting). For sustained
  throughput > ~50k msg/s, raise to 4–8; `amqp091-go` serializes I/O
  per `amqp.Connection`, so one TCP socket bounds the achievable
  confirm rate. Setting `n=1` is supported but explicitly logs a
  warning at `Dial` ("single publisher connection: broker restart
  produces a full-availability gap during reconnect").
- `WithConsumerConnections(n)` — number of dedicated consumer
  connections. Default `2`. Consumers are pinned to one of these
  connections by stable hash of consumer-tag; adding consumer
  connections rebalances long-lived consumers across sockets.
  Setting `n=1` produces the same warning at `Dial`.
- **Consumer-tag default.** When `ConsumerBuilder.Tag(string)` is
  left empty, the library generates a UUIDv7-based tag
  (`ctag-<uuid7>`) at `Build` time, before pinning to a connection
  — this is essential because hashing the empty string would
  pin every defaulted consumer to the same connection,
  defeating the multi-conn fan-out. User-supplied tags are passed
  through verbatim.

Every TCP connection in the pool has a dedicated `internal/reconnect`
supervisor and runs publisher-confirm mode on every channel. The role
split exists because RabbitMQ's own guidance is to isolate publisher
and consumer traffic onto separate connections: a slow consumer
cannot starve publish acks, and a flow-controlled publisher cannot
block consumer dispatch.

For publishers, each publisher connection owns a channel pool of
size `WithChannelPoolSize(n)` (default 8 — this is per connection,
not global). `Publisher[M].Publish` acquires a channel from any
publisher connection's pool, publishes, awaits confirm, and returns
the channel. Many goroutines may share a single `Publisher[M]`:
`Publish` and `PublishBatch` are safe for concurrent use.

For consumers, each `Consumer[M]` / `BatchConsumer[M]` holds **one
dedicated channel** on one of the consumer connections; AMQP 0-9-1
requires this for delivery-tag bookkeeping. A `Consume` call must be
made from one goroutine; the consumer fans out handler invocations
internally via `Concurrency(n)`.

#### Internals

- Auto-reconnect with backoff configured via `WithReconnectBackoff`,
  per TCP connection independently. A single failed socket does not
  ripple across the pool.
- **Reconnect synchronization barrier.** On a successful reconnect
  of any TCP connection in the pool, the supervisor runs the
  following steps **synchronously** before resuming application
  traffic on that connection:
  1. Re-open the AMQP channel(s) needed by the connection's role
     (publisher channel pool, or consumer's single channel).
  2. Run every registered `Topology.AttachTo` redeclare against a
     temporary channel (in the order they were registered).
  3. For consumer connections, re-apply `basic.qos` and re-issue
     `basic.consume` on the consumer's channel.
  4. Fire any `WithOnReconnect(func())` user callback last; by the
     time it runs the application sees a topology-restored,
     consumer-resubscribed state.
  Until step 2 completes, `Publisher.Publish` calls routed to the
  reconnected connection **block** on a per-connection condition
  variable (still cancellable via `ctx`); on `ctx` cancel they
  return `ErrReconnecting` (wraps `ctx.Err()`, classified
  `IsTransient`). The `topology_redeclare_seconds{role}` histogram
  records the duration of step 2 per reconnect cycle.
- **Degraded mode on persistent redeclare failure.** If the
  topology redeclare in step 2 fails (e.g., broker has a
  conflicting definition for a queue that cannot be reconciled),
  the supervisor:
  1. Logs the error with the offending entity name.
  2. Increments `connection_degraded_total{role, reason}`.
  3. Fires `WithOnTopologyDegraded(func(error))` exactly once per
     transition into the degraded state.
  4. Holds the connection in **degraded state**: all `Publish`
     calls return `ErrTopologyRedeclareFailed` (permanent),
     consumers do NOT re-issue `basic.consume`, and the supervisor
     retries the redeclare with backoff on every subsequent
     reconnect cycle (which can also be triggered manually via
     `(*Connection).ForceReconnect()` exposed for operators).
  5. Recovers automatically: the first successful redeclare clears
     the degraded flag, fires `WithOnReconnect`, and traffic
     resumes. Silently consuming a "no-op" `Publish` while topology
     is broken is the failure mode this prevents.
- `Dial` validates `WithChannelPoolSize` against the negotiated
  `channel-max` (which can differ across connections in theory; the
  most-restrictive negotiated value wins). Returns `ErrInvalidOptions`
  if the pool would exceed the broker's allowance.
- Publisher confirms enabled on every channel.
- `connection.blocked`/`unblocked` events surfaced via `WithOnBlocked`
  and metrics, with one callback fire per affected TCP connection.
  While *any* publisher connection is blocked, `Publisher.Publish` and
  `Publisher.PublishBatch` route around it to an unblocked publisher
  connection if one exists; if **all** publisher connections are
  blocked, they **wait** for unblock or `ctx.Done()`; on cancellation
  they return `ErrConnectionBlocked` (wrapping `ctx.Err()`). This
  matches `amqp091-go` defaults and prevents silent message loss.
- After a successful reconnect of a consumer connection, every
  `Consumer[M]` / `BatchConsumer[M]` pinned to it automatically
  reopens its channel, reapplies `basic.qos`, and reissues
  `basic.consume`. The optional `WithOnResubscribe(queue)` callback
  fires once per resubscribed queue. The metric
  `consumer_resubscribed_total{queue=<name>}` increments on each
  resubscribe.
- When a consumer's channel closes mid-handler, the handler's
  `context.Context` is cancelled with cause `ErrChannelClosed`. The
  handler should observe `ctx.Done()` to abort orphan work; the
  broker will redeliver the original message because the ack was
  never received. The metric
  `consumer_handler_aborted_channel_closed_total` increments per
  such event.
- `Close(ctx)` drains in-flight publishes/handlers up to the context
  deadline, then closes every TCP connection in the pool.
- A no-op `otel.Tracer` is baked in by default so instrumentation code
  paths run uniformly. `WithTracer` swaps in a real tracer; there is no
  "tracing-disabled" code branch.
- `WithConnectionName` writes `client_properties.connection_name`,
  visible in `rabbitmqctl list_connections name client_properties`.
  Default is `<binary>-<hostname>-<pid>`. Role and index suffixes
  (`<base>-pub-0`, `<base>-pub-1`, `<base>-con-0`, …) are appended
  per TCP connection so the broker side shows every socket
  individually.

#### Heartbeat and partition detection

AMQP heartbeats are negotiated to the smaller of client and server
values. RabbitMQ defaults to 60s server-side; the library defaults
to 10s client-side, so the negotiated value is typically **10s**.
Partition detection takes roughly **2 × heartbeat** (one missed
heartbeat plus the timeout of the second), so a default install
detects a hung TCP connection in **≈ 20s**. During that window,
publishes are unconfirmed and consumers receive no new deliveries.

Sizing guidance:
- **High-throughput / low-latency workloads (≥10k msg/s):** keep
  10s or lower. The detection latency dominates the unconfirmed
  publish backlog. The overhead (one frame per heartbeat per
  channel-wise) is negligible at this scale.
- **Low-throughput / battery-conscious workloads:** raise to 30–60s
  to halve heartbeat overhead.
- **Behind chatty load balancers / NAT:** never set lower than the
  smallest idle-timeout in the path; otherwise the LB drops the
  connection between heartbeats. 30s is the safest floor for
  most cloud-hosted brokers.

`amqp091-go` enforces the negotiated heartbeat; setting `0`
disables heartbeats entirely, which is strongly discouraged.

#### Frame size and message size limits

`WithFrameMax(131072)` is the AMQP-spec minimum (128 KiB) and the
sensible default for typical messages. Messages larger than the
frame max are split into multiple frames, so payloads up to RabbitMQ's
**practical limit of ~512 MiB per message** work without further
configuration — but with the overhead of N frames per message
(N = ⌈body_size / frame_max⌉).

Sizing guidance:
- **Small messages (< 4 KiB):** leave at default. The overhead is
  one frame either way.
- **Streaming large messages (≥ 1 MiB):** raise to `1048576` (1 MiB).
  Reduces per-message frame count and CPU overhead on the broker.
- **Hard maximum:** RabbitMQ rejects `frame-max < 4096` at the
  protocol-tune step. The library refuses values < 4096 at
  `Dial` with `ErrInvalidOptions`.
- The negotiated value is `min(client, server)`; RabbitMQ default
  is 131072. To use 1 MiB end-to-end, set
  `frame_max = 1048576` on the broker side as well.

For truly large payloads (≥ 100 MiB), AMQP is the wrong tool —
treat the message body as a pointer (S3 URL, object-store key) and
publish only the reference.

#### Authentication

- `WithAuth(user, pass)` selects PLAIN by default. Passwords are
  scrubbed from any error message, log line, or metric label this
  library emits (see §8 Boundaries).
- `WithSASLMechanism(SASLExternal)` combined with a `WithTLSConfig`
  that presents a client certificate enables passwordless
  authentication on RabbitMQ. The broker must have
  `rabbitmq_auth_mechanism_ssl` enabled and the `external_auth`
  user permissions wired up. `WithAuth` becomes a no-op under
  EXTERNAL; setting it logs a warning at `Dial`. **`Dial` validates
  at call time that:**
  1. `WithTLSConfig(*tls.Config)` was provided (no TLS → broker
     cannot identify the principal).
  2. The provided `*tls.Config` carries a client certificate — i.e.
     `len(cfg.Certificates) > 0 || cfg.GetClientCertificate != nil`.
     A TLS config that only verifies the server cert (no client
     cert) cannot satisfy EXTERNAL.
  3. The server endpoint is `amqps://`, not plain `amqp://`. (TLS
     is mandatory for EXTERNAL.)
  Any of these unmet returns `ErrInvalidOptions` at `Dial` with the
  specific reason. This is a fail-closed validation: the broker
  side would close the channel with 403 otherwise, which is a much
  worse failure mode for operators to diagnose.
- OAuth2 (via the `rabbitmq_auth_backend_oauth2` plugin) is out of
  scope for v0.1; planned for v0.2.

### 6.2 Publisher

```go
type Publisher[M any] struct { /* … */ }

func PublisherFor[M any](conn *Connection) *PublisherBuilder[M]

func (p *Publisher[M]) Publish(ctx context.Context, msg Message[M]) error
func (p *Publisher[M]) PublishBatch(ctx context.Context, msgs []Message[M]) ([]PublishResult, error)
func (p *Publisher[M]) Health(ctx context.Context) error
func (p *Publisher[M]) Close(ctx context.Context) error  // drains in-flight

type PublishResult struct {
    Index int    // position in input slice
    Err   error  // nil on success
}
```

Builder:

```go
type PublisherBuilder[M any] struct { /* … */ }

func (b *PublisherBuilder[M]) Codec(codec.Codec) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) Exchange(name string) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) RoutingKey(rk string) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) Mandatory() *PublisherBuilder[M] // set, not toggle; no inverse method
func (b *PublisherBuilder[M]) StampUserID() *PublisherBuilder[M] // auto-set Message[M].UserID to the connection's authenticated user
func (b *PublisherBuilder[M]) OnReturn(func(Return)) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) ConfirmTimeout(time.Duration) *PublisherBuilder[M]                  // broker confirm window only
func (b *PublisherBuilder[M]) PublishTimeout(time.Duration) *PublisherBuilder[M]                  // end-to-end cap (pool acquire + write + confirm)
func (b *PublisherBuilder[M]) PublishBatchMaxSize(n int) *PublisherBuilder[M]                    // max messages per PublishBatch call; default 1024 (per-call cap, not a sliding in-flight window)
func (b *PublisherBuilder[M]) PublishRetry(p RetryPolicy) *PublisherBuilder[M]                    // automatic retry of ErrTransient publishes
func (b *PublisherBuilder[M]) Metrics(metrics.PublisherMetrics) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) WithoutMetrics() *PublisherBuilder[M]
func (b *PublisherBuilder[M]) Tracer(otel.Tracer) *PublisherBuilder[M]
func (b *PublisherBuilder[M]) Build() (*Publisher[M], error)

type Return struct {
    ReplyCode  uint16              // AMQP reply code (e.g. 312 NO_ROUTE)
    ReplyText  string              // AMQP reply text
    Exchange   string              // exchange the message was published to
    RoutingKey string              // routing key the message was published with
    Body       []byte              // raw message body (codec NOT applied)
    Properties ReturnedProperties  // basic.properties of the returned message
}

// ReturnedProperties carries the AMQP basic.properties of a returned
// message. Mirrors basic.properties one-to-one — distinct from a
// field-table (`Headers`), which lives inside Properties.Headers.
type ReturnedProperties struct {
    ContentType     string
    ContentEncoding string
    Headers         Headers
    DeliveryMode    DeliveryMode
    Priority        uint8
    CorrelationID   string
    ReplyTo         string
    Expiration      string  // raw AMQP shortstr (milliseconds as string)
    MessageID       string
    Timestamp       time.Time
    Type            string
    UserID          string
    AppID           string
}
```

`basic.publish` does **not** support an `immediate` flag on RabbitMQ —
the flag was removed in RabbitMQ 3.0 and setting it closes the channel.
The builder intentionally omits an `Immediate()` method.

Semantics:
- **Concurrency-safe.** `Publish` and `PublishBatch` are safe for
  concurrent use from many goroutines on a single `Publisher[M]`.
  Each call acquires a channel from the connection's publisher
  channel pool, publishes, awaits its own confirm, and returns the
  channel. Ordering across goroutines is **not** preserved (different
  channels = different confirm streams); ordering within a single
  goroutine using a single `Publisher[M]` is also not guaranteed,
  because consecutive `Publish` calls may take different channels.
  Callers needing in-order delivery should publish from one goroutine
  and use `PublishBatch` (which pins to one channel and preserves
  input order — RabbitMQ guarantees per-channel order).
- `Publish` is **synchronous-with-confirm**: returns after the broker
  acks the publish (or after `ConfirmTimeout` → `ErrConfirmTimeout`).
- `Publish` with `Mandatory()` + unroutable → `ErrUnroutable` (the
  `OnReturn` handler also fires, if set).
- **`basic.nack` from broker → `ErrPublishNacked`.** When a queue's
  overflow policy is `reject-publish` or `reject-publish-dlx`, or
  the broker raises a disk/memory alarm mid-publish, RabbitMQ sends
  `basic.nack` on the publisher channel. The confirm tracker
  resolves the matching `delivery-tag` with `ErrPublishNacked`
  (classified `ErrTransient`). For `reject-publish-dlx`, the broker
  also dead-letters the message; the publisher caller still sees
  `ErrPublishNacked` so the application can re-publish or back off.
- **`basic.return` / `basic.ack` correlation for mandatory publishes.**
  For an unroutable mandatory publish, RabbitMQ sends `basic.return`
  *first*, then `basic.ack` (the broker acks because *it* handled the
  message — by returning it). The confirm tracker correlates the two
  frames by `delivery-tag`: when an ack arrives for a `delivery-tag`
  that already saw a return, `Publish` resolves with `ErrUnroutable`
  rather than success. The `OnReturn` callback fires synchronously
  before `Publish` unblocks, so users can inspect the rejection
  before the publish path completes.
- **No double-acknowledgement guarantee.** AMQP `basic.ack` from
  broker → publisher means "the broker took responsibility" (queued
  for routing). It does *not* guarantee the message is persisted to
  disk on every queue's replica before a broker crash; users
  publishing to mirrored / quorum queues should still treat their
  consumers as idempotent (dedupe by `MessageID`).
- `PublishBatch` pipelines all N publishes on a **single channel**
  (preserving input order per RabbitMQ's per-channel ordering
  guarantee), waits one confirm window, returns per-message
  `PublishResult` and (if any failed) a non-nil error wrapping
  `ErrPartialBatch`. Per-message error values include
  `ErrPublishNacked`, `ErrUnroutable`, `ErrChannelClosed` (if the
  channel died mid-batch and unconfirmed publishes were not
  resolved), and `ErrInvalidMessage` (client-side validation).
- **Channel-close mid-batch recovery contract.** If the channel
  dies between `basic.publish` and confirm receipt for some
  subset of the batch, each affected `PublishResult.Err` is
  `ErrChannelClosed`. The library does NOT distinguish "broker
  persisted the message before the channel died" from "broker did
  not receive the publish frame" — that information does not exist
  on the wire. Callers who retry only the `ErrChannelClosed`
  indices MUST accept the at-least-once semantics from §6.2.1:
  retrying produces a duplicate when the broker persisted but the
  ack was lost. Idempotent consumers handle this correctly.
  `PublishRetry` on the publisher applies to **single** `Publish`
  calls only, not to `PublishBatch` — batch-level retry is the
  caller's responsibility, because the retry semantics
  (chunk size, partial retry vs full retry, ordering)
  are workload-specific.
- **`PublishBatch` size cap.** Each `Publisher[M]` honours a
  configurable per-call maximum size (`PublishBatchMaxSize`,
  default `1024`). Passing more messages than the cap in a single
  call returns `ErrBatchTooLarge` immediately, with an empty
  result slice; callers should chunk. The cap bounds tracker
  memory per call and the confirm-window worst case; raise it for
  higher throughput against fast brokers. This is a **per-call**
  cap, not a sliding in-flight window across calls — `Publisher[M]`
  does not currently throttle multiple concurrent `PublishBatch`
  invocations against one another.
- **Channel pool exhaustion.** If all channels in the publisher's
  connection pool are in-flight when a `Publish` arrives, the call
  waits with respect to `ctx`. On `ctx` cancellation it returns
  `ErrChannelPoolExhausted` (transient) wrapping `ctx.Err()`. Tune
  pool size via `WithChannelPoolSize` and add publisher connections
  via `WithPublisherConnections` if exhaustion is steady-state.
- **Channel close mid-publish.** If the underlying channel closes
  between `basic.publish` and confirm receipt (e.g. reconnect), the
  affected publish resolves with `ErrChannelClosed`. The
  `Publisher[M]` reacquires from the pool on the next call.
- **Timeouts compose top-down.** `PublishTimeout(d)` is the end-to-end
  cap on a `Publish` call (pool acquisition + write + confirm +
  blocked-wait + reconnect barrier) and returns the underlying error
  (`ErrChannelPoolExhausted`, `ErrConfirmTimeout`,
  `ErrConnectionBlocked`, `ErrReconnecting`, …) wrapped with
  `ctx.DeadlineExceeded`. `ConfirmTimeout(d)` only bounds the
  broker's confirm window once the frame has been written; it does
  NOT include pool acquisition or blocked-wait. Caller's `ctx`
  deadline still applies and wins if shorter. **Defaults: `ConfirmTimeout = 30s`,
  `PublishTimeout = 0` (caller `ctx` is authoritative).** A 30s
  default on `ConfirmTimeout` prevents the silent-stall failure
  mode where a misbehaving broker keeps the TCP connection alive
  but never sends a `basic.ack`; setting `ConfirmTimeout(0)`
  explicitly disables it and is documented as discouraged for any
  production path that uses `context.Background()`. The blocked-wait
  window (broker memory/disk alarm) IS accounted for inside
  `PublishTimeout` but NOT inside `ConfirmTimeout` — the latter
  starts ticking only after the publish frame leaves the socket.
- **`PublishRetry(p)`.** Automatic retry, with policy `p`, of
  publishes that fail with an `errors.Is(err, ErrTransient)` error
  (`ErrPublishNacked`, `ErrChannelPoolExhausted`,
  `ErrConnectionBlocked`, `ErrConfirmTimeout`, `ErrChannelClosed`,
  network errors during reconnect window, AMQP codes 311/320/504/541).
  Permanent errors (`ErrUnroutable`, `ErrInvalidMessage`, AMQP 4xx/5xx
  permanent codes) are never retried — the policy is for transient
  broker conditions, not for re-routing decisions. Default: no retry
  (zero value of `RetryPolicy` short-circuits). When retry is active,
  each attempt honours `PublishTimeout` independently, and the caller
  `ctx` remains the overall budget. Each retry attempt increments the
  `publisher_retry_total{exchange, reason}` metric; `reason` is one
  of `nacked|confirm_timeout|channel_closed|pool_exhausted|blocked|network`.

  **Duplicate hazard — load-bearing.** `PublishRetry` is **not
  exactly-once**. The two retry triggers below can resolve a publish
  the broker has already persisted:
  - `ErrConfirmTimeout` — the broker may have committed the message;
    only the `basic.ack` was lost or delayed. Retry → duplicate.
  - `ErrChannelClosed` (and "network errors during reconnect window")
    — the message may have crossed the wire and been routed before
    the channel/socket failed. Retry → duplicate.
  - `ErrPublishNacked` with `x-overflow=reject-publish-dlx` — the
    broker dead-lettered the original; retry can land in DLX again.

  Consumers MUST be idempotent. See §6.2.1 for the canonical
  pattern.
- Publish errors are classifiable:
  `errors.Is(err, amqp.ErrTransient)` indicates the caller may retry;
  `errors.Is(err, amqp.ErrPermanent)` indicates the caller should not.

### 6.2.1 At-least-once semantics and consumer-side deduplication

This library is at-least-once by default and by design. AMQP 0-9-1
provides no exactly-once primitive, and the wire protocol cannot
distinguish "broker received but ack was lost" from "broker did not
receive". The library therefore commits to:

- **Default `MessageID` is UUIDv7 (RFC 9562)** when the user leaves
  `Message[M].MessageID` empty. UUIDv7 is time-ordered, which makes
  cache eviction by recency trivial. The library never reuses a
  generated `MessageID`; user-supplied IDs are passed through
  verbatim and the duplicate-detection burden moves to the user.
- **Every retry path is observable.** `publisher_retry_total{exchange,
  reason}` increments on each automatic retry; manual retries by the
  caller bump `publisher_publish_total{outcome}` normally.
- **Every consumer delivery surfaces `Redelivered() bool`**, which
  the broker sets when the message has been delivered before
  (channel close, requeue, recovery from a crashed consumer).

**Canonical consumer dedupe pattern** (recommended for any handler
that has external side-effects — DB writes, HTTP calls, payments):

```go
type dedupCache struct {
    mu  sync.Mutex
    seen *lru.Cache[string, struct{}] // bounded by message volume × retention window
}

func (d *dedupCache) Seen(id string) bool {
    d.mu.Lock(); defer d.mu.Unlock()
    if _, ok := d.seen.Get(id); ok { return true }
    d.seen.Add(id, struct{}{})
    return false
}

err = con.Consume(ctx, func(ctx context.Context, o OrderPlaced) error {
    if cache.Seen(d.MessageID()) { // see ConsumeRaw for envelope access
        return nil // already processed; ack
    }
    return process(ctx, o)
})
```

The cache window must cover the maximum plausible duplicate gap:
broker outage + reconnect + retry budget. For most workloads, **15
minutes of MessageID retention** is sufficient; for paranoid
workloads, persist the dedupe cache to Redis or a database.

For workloads where consumer-side dedupe is too expensive, the
`rabbitmq_message_deduplication` plugin offers broker-side dedupe
keyed by a `x-deduplication-header`. The library does not depend on
the plugin in v0.1; users who enable it set the header explicitly
via `Message[M].Headers`.

The `examples/idempotent_consume/` directory ships a runnable
reference for the pattern above.

### 6.3 Consumer

```go
type Consumer[M any] struct { /* … */ }

func ConsumerFor[M any](conn *Connection) *ConsumerBuilder[M]

func (c *Consumer[M]) Consume(ctx context.Context, h Handler[M]) error
func (c *Consumer[M]) ConsumeRaw(ctx context.Context, h RawHandler[M]) error
func (c *Consumer[M]) Health(ctx context.Context) error
func (c *Consumer[M]) Close(ctx context.Context) error  // drains in-flight

type Handler[M any]     func(ctx context.Context, msg M) error
type RawHandler[M any]  func(ctx context.Context, d Delivery[M]) error

type Delivery[M any] struct { /* unexported fields */ }

func (d *Delivery[M]) Body() *M
func (d *Delivery[M]) Headers() Headers
func (d *Delivery[M]) Redelivered() bool
func (d *Delivery[M]) DeliveryTag() uint64
func (d *Delivery[M]) DeathCount() int                  // sum of x-death entries with reason ∈ {rejected, delivery-limit}
func (d *Delivery[M]) DeathCountByReason(r string) int  // count for a specific reason ("rejected", "expired", "maxlen", "delivery-limit")
func (d *Delivery[M]) DeathReasons() []string           // unique reasons present in x-death (in declaration order)
func (d *Delivery[M]) MessageID() string
func (d *Delivery[M]) CorrelationID() string
func (d *Delivery[M]) Timestamp() time.Time
func (d *Delivery[M]) Ack() error
func (d *Delivery[M]) Nack(requeue bool) error
func (d *Delivery[M]) AckIf(err error) error // nil→Ack; err→Nack(requeue=errors.Is(err,ErrRequeue))
```

`Delivery[M]` is a **concrete struct**, not an interface — methods can be
added in minor releases without breaking implementers. Tests that need a
fake delivery use `amqpmock.NewDelivery[M](amqpmock.DeliveryFixture{…})`
from the `amqpmock/` subpackage.

`Ack`, `Nack`, and `AckIf` may return:
- `ErrChannelClosed` — the delivery's channel was closed before the
  ack/nack reached the broker (rare under healthy operation).
- `ErrAlreadyClosed` — the consumer was closed via `Close(ctx)`.
- `nil` — broker received the ack/nack. Note that AMQP `basic.ack` /
  `basic.nack` are not confirmed by the broker; success here means
  "the frame was written to the socket", not "the broker recorded it".
  Network partition between write and broker-side processing can
  cause the message to be redelivered (`Redelivered() == true`).

Handler error semantics:
- `nil` → Ack.
- any error → Nack, `requeue=false` (DLX if configured).
- `errors.Is(err, ErrRequeue)` → Nack, `requeue=true`.
- `errors.Is(err, ErrPoison)` → identical to default error semantically;
  exists for readability when "this is unambiguously bad input".

Builder:

```go
type ConsumerBuilder[M any] struct { /* … */ }

func (b *ConsumerBuilder[M]) Codec(codec.Codec) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Queue(name string) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Tag(consumerTag string) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Concurrency(n uint) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Prefetch(count uint16) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) PrefetchBytes(bytes uint) *ConsumerBuilder[M] // no-op on RabbitMQ; preserved for protocol parity
func (b *ConsumerBuilder[M]) ChannelQoS() *ConsumerBuilder[M]              // RabbitMQ: apply QoS per channel, not per consumer
func (b *ConsumerBuilder[M]) Priority(p int) *ConsumerBuilder[M]           // x-priority on basic.consume; higher = preferred
func (b *ConsumerBuilder[M]) HandlerTimeout(d time.Duration) *ConsumerBuilder[M] // per-message handler ctx deadline
func (b *ConsumerBuilder[M]) HandlerTimeoutVerdict(v TimeoutVerdict) *ConsumerBuilder[M] // default: TimeoutNackNoRequeue
func (b *ConsumerBuilder[M]) Exclusive() *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) AutoAck() *ConsumerBuilder[M]           // explicit opt-in; see warning below
func (b *ConsumerBuilder[M]) Args(Headers) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) OnCancel(func(reason string)) *ConsumerBuilder[M] // basic.cancel from broker; reason e.g. "queue deleted"
func (b *ConsumerBuilder[M]) MaxRedeliveries(n int) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Metrics(metrics.ConsumerMetrics) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) WithoutMetrics() *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Tracer(otel.Tracer) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Build() (*Consumer[M], error)
```

Defaults: `Concurrency=1`, `Prefetch=64`, requeue **off** for invalid
payload (poison messages go to DLX or are discarded), no auto-ack,
no handler timeout, `HandlerTimeoutVerdict=TimeoutNackNoRequeue` (when
a timeout is configured, an exceeded handler is treated as poison
unless the user opts into requeue — aligns with the "no silent poison
loop" north star). Override via `HandlerTimeoutVerdict(TimeoutNackRequeue)`
when the dependency is known-transient (rate limits, lock waits).

#### Sizing `Prefetch` and `Concurrency`

Sustainable throughput per consumer channel is bounded by:

  throughput ≈ Concurrency / handler_latency

Prefetch is the broker-side credit window: it caps the number of
unacked messages on the channel. `Prefetch < Concurrency` is **almost
always wrong** — handlers will stall waiting for the next delivery
even though they have capacity. The builder logs a warning at
`Build` if `Prefetch < Concurrency`. A safe rule of thumb:

  Prefetch = 2 × Concurrency  (idle handlers, fast handlers)
  Prefetch = 4 × Concurrency  (typical latency, want some buffering)
  Prefetch = 8 × Concurrency  (high latency, want maximum throughput)

The default `Prefetch=64, Concurrency=1` is conservative and fits
development workloads. For sustained > 1k msg/s/consumer against a
remote broker, raise both. Note the trade-off: a large `Prefetch`
means a consumer crash can leave many unacked messages, all of which
get redelivered (and may be processed twice — see §6.2.1).

```go
type TimeoutVerdict uint8
const (
    TimeoutNackNoRequeue TimeoutVerdict = iota // default; message goes to DLX (or is dropped)
    TimeoutNackRequeue                         // requeue, subject to MaxRedeliveries / x-delivery-limit
)
```

#### Concurrency and ordering

`Concurrency(n)` with `n > 1` dispatches up to `n` handlers in
parallel for messages received on the same channel. RabbitMQ
guarantees in-order delivery **on the wire** for a single channel,
but parallel dispatch breaks ordering at the handler boundary — two
goroutines may finish in a different order than they started.
**Concurrency > 1 means you have given up per-queue ordering.**

For strict per-queue ordering with redundancy, the canonical setup
is `Concurrency(1)` plus `Queue.SingleActiveConsumer=true` on the
declared queue: one consumer process is active at a time, others
stand by; on the active one's death, the broker promotes a standby.
A worked example lives in `examples/ordered_consume/`.

#### Poison protection — preferred path: quorum queues + `x-delivery-limit`

On RabbitMQ 4.x, **quorum queues with `x-delivery-limit` are the
canonical poison-protection mechanism**. The broker increments the
delivery counter on *every* redelivery — including
`Nack(requeue=true)` — and dead-letters the message when the limit
is exceeded. This is exactly the behaviour that AMQP 0-9-1 alone
cannot provide (because `x-death` is only written on dead-letter
events).

Set this once on the queue in `Topology`:

```go
amqp.Queue{
    Name:          "orders.placed",
    Type:          amqp.QueueTypeQuorum,
    DeliveryLimit: 5,           // broker-enforced; survives consumer restarts
    Durable:       true,
}
```

When `DeliveryLimit > 0` on a quorum queue, set
`MaxRedeliveries(0)` (unbounded) on the consumer — the broker is
already doing the bounding, and a second in-process counter only
adds skew. Metrics still surface `ErrMaxRedeliveries` when the
broker dead-letters; the consumer reads `x-death` to attribute the
drop.

#### Poison protection — fallback path: consumer-side counters

For classic queues (which do not honour `x-delivery-limit`), or for
quorum queues where the operator chose not to set a broker-side
limit, `MaxRedeliveries(n)` bounds redelivery loops via **two
complementary counters**, because AMQP 0-9-1 only writes `x-death`
on dead-letter events (TTL expiry, length limit,
`basic.nack`/`basic.reject` with `requeue=false`) — *not* on
`Nack(requeue=true)`. The consumer:

1. **Counter A — cross-process, via `x-death`.** Reads
   `DeathCount()` (which sums `x-death` entries with
   `reason ∈ {rejected, delivery-limit}` for the current queue —
   `expired` and `maxlen` reasons are NOT counted as redelivery
   evidence, because they reflect broker-side TTL/length policies
   rather than handler-driven rejection) and short-circuits to
   `Nack(requeue=false)` if it already exceeds `n`. Bounds loops
   that bounce through a DLX-back-to-source binding; survives
   consumer restarts because `x-death` lives on the message. Users
   needing custom reason policies use
   `DeathCountByReason(reason)` and `DeathReasons()` on the
   `Delivery[M]` directly.

2. **Counter B — in-process, keyed by `MessageID`.** A race-free map
   keyed by `(channel-instance-id, MessageID)` — falling back to
   `(channel-instance-id, consumer-tag, delivery-tag)` when
   `MessageID` is empty. The channel-instance-id is a UUID generated
   per consumer channel and reset on channel close, so delivery-tags
   reused after a reconnect cannot collide. Each `ErrRequeue`-driven
   `Nack(requeue=true)` increments the counter; once it would exceed
   `n`, the consumer rewrites the handler's verdict to
   `Nack(requeue=false)` and emits `ErrMaxRedeliveries` via metrics +
   log. The entry is deleted on `Ack` or `Nack(false)` so it does
   not leak across long-lived consumers.

Counter B is **process-local**: a consumer restart resets it. Users
who need cross-process bounding must either (a) use a quorum queue
with `DeliveryLimit > 0` (preferred), or (b) wire a DLX-back-to-source
binding so counter A takes over via `x-death`. Both fallback counters
together ensure a handler that always returns `ErrRequeue` is bounded
in the current process, and a message bouncing through DLX is bounded
across processes.

Metric and log fields distinguish the cause: `cause=delivery-limit`
(quorum-broker), `cause=x-death` (counter A), or `cause=in-process`
(counter B).

#### `basic.cancel` (consumer cancellation notification)

The library always advertises the `consumer_cancel_notify=true`
client capability in `connection.start-ok` (as `amqp091-go`
already does), so the broker delivers a `basic.cancel` frame when
the queue is deleted under the consumer (or when an exclusive
consumer is forced off). The library:

- Increments `consumer_cancelled_total{queue, reason}` on every
  received `basic.cancel`.
- Fires `OnCancel(reason)` if set; otherwise emits a warning log.
- **Does NOT auto-redeclare the queue** — the operator likely
  deleted it for a reason. The consumer goroutine returns from
  `Consume(ctx, …)` with `ErrConsumerCancelled` (wrapped with
  the reason). Callers that want resilient redeclare-and-resume
  semantics wrap `Consume` in their own loop that also calls
  `Topology.Declare` first.

A leaked queue deletion (e.g. operator typo) is a real production
incident; a silently dying consumer is worse. Always wire `OnCancel`
in production code.

#### `AutoAck()` — warning

`AutoAck()` enables the AMQP `no-ack` flag on `basic.consume`, which
tells the broker to consider every delivery already acknowledged
**before the client sees it**. This is a real AMQP feature, exposed for
protocol fidelity, but it changes critical semantics. The godoc on
`AutoAck()` will repeat this verbatim:

- **Handler error semantics are bypassed.** `nil`/error/`ErrRequeue`/
  `ErrPoison` returns all become no-ops. A handler that panics or
  errors silently drops the message.
- **No redelivery on consumer crash.** If the consumer dies mid-handle,
  the broker has already removed the message; it will not be redelivered
  to another consumer. Use only when at-most-once delivery is
  acceptable.
- **No backpressure via prefetch.** With `AutoAck`, prefetch loses its
  ack-gating effect. The broker streams as fast as the channel will
  carry, and slow handlers can OOM the consumer.
- **DLX / `MaxRedeliveries` do not engage.** Both depend on Nacks the
  client never sends.

Use `AutoAck()` only for genuinely fire-and-forget streams (e.g.,
high-volume telemetry where occasional drops are acceptable). For
everything else, leave it off and let the error-driven semantics work.

### 6.4 BatchConsumer

```go
type BatchConsumer[M any] struct { /* … */ }

func BatchConsumerFor[M any](conn *Connection) *BatchConsumerBuilder[M]

func (c *BatchConsumer[M]) Consume(ctx context.Context, h BatchHandler[M]) error
func (c *BatchConsumer[M]) Health(ctx context.Context) error
func (c *BatchConsumer[M]) Close(ctx context.Context) error

type BatchHandler[M any] func(ctx context.Context, batch Batch[M]) error

type Batch[M any] struct { /* unexported fields */ }

func (b *Batch[M]) Messages() []*M
func (b *Batch[M]) Deliveries() []*Delivery[M]  // for partial ack/nack
func (b *Batch[M]) Ack() error
func (b *Batch[M]) Nack(requeue bool) error
```

`Batch[M]` is a concrete struct (same rationale as `Delivery[M]`).

Builder mirrors `ConsumerBuilder` with `Size(uint)` and
`FlushAfter(time.Duration)`. `BatchConsumer.Consume` runs the
handler sequentially per batch (`Concurrency(n)` would interleave
ack ranges and is intentionally not exposed here; for parallel
batches, run multiple `BatchConsumer[M]` instances).

#### Handler error semantics

The contract mirrors `Consumer[M]` at the batch granularity:

- `nil` → `batch.Ack()` is called automatically. Implementation
  emits a **single** `basic.ack` frame with `multiple=true` for the
  highest delivery-tag in the batch — one frame instead of N.
- non-nil error → `batch.Nack(false)` is called automatically, sent
  as a single `basic.nack` with `multiple=true` + `requeue=false`.
  Wrap with `ErrRequeue` (`fmt.Errorf("retry: %w", amqp.ErrRequeue)`)
  to request `requeue=true` for the whole batch instead.
- For **partial outcomes**, the handler calls `Batch.Deliveries()`
  and acks/nacks individual deliveries explicitly, then returns
  `nil`. When the handler did any explicit acking, the framework
  skips the auto-ack (idempotent guard: `Batch` tracks whether it
  was acked/nacked already).
- `HandlerTimeout(d)` from the builder (same as on `ConsumerBuilder`)
  derives a per-batch ctx; on timeout, the framework emits the
  configured `HandlerTimeoutVerdict` for the whole batch
  (default `TimeoutNackNoRequeue` → `Nack(false)` with `multiple=true`).
  Override via `HandlerTimeoutVerdict(TimeoutNackRequeue)` for
  transient-slowness workloads. Inside the handler, the user can
  still observe `ctx.Done()` and emit an explicit per-delivery
  verdict before returning; that suppresses the framework's auto
  verdict (the `Batch` idempotent guard).

The `MaxRedeliveries` two-counter design from §6.3 applies
per-message inside a batch: counter B (in-process) keyed by
`MessageID` increments on `ErrRequeue`-driven nacks. A whole-batch
nack with requeue increments counter B for every message in the
batch.

### 6.5 Message

```go
type Message[M any] struct {
    Body            *M             // required

    // basic.properties — one-to-one mapping with AMQP 0-9-1
    MessageID       string         // default: UUID v7 (RFC 9562)
    CorrelationID   string
    ReplyTo         string
    Type            string         // application-defined "kind" of the message
    AppID           string
    UserID          string         // see UserID note below — RabbitMQ closes the channel if this disagrees with the connection user
    ContentType     string         // MIME — default: codec.ContentType()
    ContentEncoding string         // transfer encoding — default: "" (set "gzip" etc. if applicable)
    Headers         Headers        // AMQP field-table; see Headers below
    Priority        uint8          // 0–255 in the wire; see note below
    Timestamp       time.Time      // default: time.Now()
    Expiration      time.Duration  // per-message TTL; see note below
    DeliveryMode    DeliveryMode   // default (zero): Persistent

    // RabbitMQ extensions
    Delay           time.Duration  // requires rabbitmq_delayed_message_exchange plugin
}

type DeliveryMode uint8
const (
    DeliveryModePersistent DeliveryMode = iota // default (zero value)
    DeliveryModeTransient
)

type Headers map[string]any  // AMQP field-table; see typing note below
```

**Field notes:**

- **`ContentType` vs `ContentEncoding`.** Two distinct AMQP properties.
  `ContentType` is the MIME type of the body (e.g.
  `application/json`); `ContentEncoding` is the transfer encoding
  applied on top (e.g. `gzip`, `deflate`, or empty for `identity`).
  Codecs populate `ContentType` by default; `ContentEncoding` is
  only relevant if the user wraps the codec's output (compression,
  etc.).

- **`Priority`.** AMQP `basic.properties.priority` is an `octet`
  (0–255). RabbitMQ priority queues only use 0–9 by convention;
  values above the queue's `x-max-priority` are silently clamped by
  the broker. Setting `Priority` on a non-priority queue has no
  effect.

- **`Expiration`.** Per-message TTL. AMQP `basic.properties.expiration`
  is a `shortstr` containing milliseconds as ASCII digits (e.g.
  `"60000"`). The publisher truncates `time.Duration` to milliseconds
  and serialises it. Sub-millisecond durations round to 0 (the broker
  interprets `"0"` as "expire immediately").

- **`UserID`.** AMQP `basic.properties.user-id`. RabbitMQ validates
  this against the user that authenticated the connection at the
  broker side: if the values differ, the broker closes the channel
  with `406 PRECONDITION_FAILED` and every in-flight publish on
  that channel is lost. To prevent this footgun:
  - `Publisher.Publish` performs a **client-side check** before
    writing the publish frame: if `Message[M].UserID != ""` and
    differs from the connection's authenticated user, the call
    returns `ErrInvalidMessage` locally (no broker round-trip, no
    channel close).
  - If the user wants the broker stamp without managing the
    string, leaving `UserID = ""` is correct — the broker writes
    nothing in that field. To force a stamp matching the
    connection user, set `Publisher.StampUserID()` on the builder.
  - SASL EXTERNAL connections expose the identity via
    `(*Connection).AuthenticatedUser()` so this check works
    transparently regardless of mechanism.

- **`Headers` typing.** The AMQP 0-9-1 field-table supports a fixed set
  of value types, matched 1:1 to Go via `amqp091-go`'s encoder:
  - `bool` (AMQP type `t`)
  - Signed integers `int8`/`int16`/`int32`/`int64` (`b`/`s`/`I`/`l`)
  - Unsigned integers `uint8`/`uint16`/`uint32`/`uint64` (`B`/`u`/`i`/`L`)
  - Floats `float32`/`float64` (`f`/`d`)
  - `Decimal{Scale uint8, Value int32}` (`D`) — re-exported from
    `amqp091-go` for protocol parity
  - `string` (`S`, long string)
  - `[]byte` (`x`)
  - `time.Time` (`T`)
  - `nil` (`V`, void)
  - Nested `Headers` (`F`, field-table)
  - `[]any` (`A`, field-array; each element re-typed by the same rules)

  **Native Go `int`/`uint` are auto-coerced** to `int64`/`uint64` at
  publish time so the common case `Headers{"count": 5}` works without
  surprise. Any other Go type (channels, structs, function values,
  pointers, named types not in this list) returns `ErrInvalidMessage`
  at publish time.

`Persistent` is the zero-value default. Any user constructing a
`Message[M]{}` literally gets durable delivery without thinking. A
user who explicitly wants transient sets `DeliveryMode: DeliveryModeTransient`.

### 6.6 Topology

```go
type Topology struct {
    Exchanges   []Exchange
    Queues      []Queue
    Bindings    []Binding
    DeadLetters []DeadLetter
}

type Exchange struct {
    Name       string
    Kind       ExchangeKind
    Durable    bool
    AutoDelete bool
    Internal   bool
    NoWait     bool     // skip broker confirmation (faster, but errors are silent)
    Args       Headers
}

type ExchangeKind string
const (
    ExchangeDirect  ExchangeKind = "direct"
    ExchangeFanout  ExchangeKind = "fanout"
    ExchangeTopic   ExchangeKind = "topic"
    ExchangeHeaders ExchangeKind = "headers"
    ExchangeDelayed ExchangeKind = "x-delayed-message" // requires plugin
)

type Queue struct {
    Name                 string
    Durable              bool
    AutoDelete           bool
    Exclusive            bool
    NoWait               bool       // skip broker confirmation
    Type                 QueueType  // sets x-queue-type arg; empty = broker default

    // DeliveryLimit caps the number of times the broker will redeliver
    // a message before dead-lettering it. Maps to the RabbitMQ
    // x-delivery-limit queue argument. Quorum queues (Type=QueueTypeQuorum)
    // increment the counter on EVERY redelivery, including Nack(requeue=true);
    // this is the preferred poison-protection mechanism on RabbitMQ 4.x
    // and supersedes the in-process counter B of MaxRedeliveries (see §6.3).
    // Classic queues do not honour this argument as of RabbitMQ 4.x and
    // require the consumer-side MaxRedeliveries mechanism instead.
    // Zero = unbounded (broker default).
    DeliveryLimit        int

    // SingleActiveConsumer enables the x-single-active-consumer queue
    // argument: only one consumer at a time receives deliveries; others
    // wait as standby. On failover the broker promotes another standby.
    // Required for strict per-queue ordering with redundancy.
    SingleActiveConsumer bool

    Args       Headers
}

// QueueType is a RabbitMQ extension via the x-queue-type queue argument.
type QueueType string
const (
    QueueTypeClassic QueueType = "classic"
    QueueTypeQuorum  QueueType = "quorum"
    QueueTypeStream  QueueType = "stream"
)

type Binding struct {
    Exchange   string
    Queue      string
    RoutingKey string
    NoWait     bool     // skip broker confirmation
    Args       Headers
}

// DeadLetter expands to x-dead-letter-exchange, x-dead-letter-routing-key,
// x-message-ttl, x-max-length, x-max-length-bytes, x-overflow args on the
// source queue, declaring the target exchange and queue as well.
type DeadLetter struct {
    Source         string          // source queue name
    Exchange       string          // DLX name (declared if absent in Exchanges)
    RoutingKey     string          // optional; if empty, original routing key is preserved
    TTL            time.Duration   // x-message-ttl on source
    MaxLength      int             // x-max-length on source (message count)
    MaxLengthBytes int             // x-max-length-bytes on source (byte cap)
    Overflow       OverflowPolicy  // x-overflow on source; empty = broker default (drop-head)
}

// OverflowPolicy is a RabbitMQ extension via the x-overflow queue argument.
type OverflowPolicy string
const (
    OverflowDropHead         OverflowPolicy = "drop-head"          // broker default
    OverflowRejectPublish    OverflowPolicy = "reject-publish"
    OverflowRejectPublishDLX OverflowPolicy = "reject-publish-dlx" // dead-letters the publisher's overflow
)

func (t *Topology) Declare(ctx context.Context, conn *Connection) error
func (t *Topology) AttachTo(conn *Connection)  // redeclare on reconnect
```

Behaviour:
- `Declare` runs a two-step pipeline:
  1. **In-memory expansion** (before any broker call). The library
     mutates a *copy* of the input `Topology` so the caller's value
     is never modified, then injects the following into each affected
     source queue's `Args` map:
     - For every `DeadLetter` entry whose `Source` matches:
       `x-dead-letter-exchange`, `x-dead-letter-routing-key`,
       `x-message-ttl`, `x-max-length`, `x-max-length-bytes`,
       `x-overflow`. Any DLX exchange or `<Source>.dlq` queue not
       already present in `Exchanges` / `Queues` is appended.
     - For every queue with `DeliveryLimit > 0`: `x-delivery-limit`.
       Validation rejects `DeliveryLimit > 0` on a non-quorum queue
       with `ErrInvalidOptions` (broker silently ignores it on
       classic queues; emitting the error prevents misconfiguration).
     - For every queue with `SingleActiveConsumer == true`:
       `x-single-active-consumer = true`.
     - For every queue with `Type != ""`: `x-queue-type`.
  2. **Broker-side declare** on a temporary channel, in fixed order:
     **exchanges → queues → bindings**. This guarantees the source
     queue is declared *once*, already carrying its full arg set —
     never re-declared with different args. (AMQP 0-9-1 rejects
     `queue.declare` with non-matching args using `PRECONDITION_FAILED`
     /406, so post-hoc arg injection on a live queue is not an option.)
- `Declare` is idempotent against an unchanged broker state. If the
  broker has a conflicting definition, the call returns
  `ErrTopologyMismatch` (wrapping `ErrPreconditionFailed`) — never
  silently mutates.
- **`(*Topology).validate()` runs before `Declare` and before
  `AttachTo` snapshot capture.** Validation rules (each returns
  `ErrInvalidOptions` with a specific message):
  - Empty names on `Exchange`, `Queue`, `Binding`.
  - Unknown `ExchangeKind`, `QueueType`, `OverflowPolicy`.
  - `DeliveryLimit > 0` on a non-quorum queue (broker silently
    ignores it on classic; reject loudly).
  - `SingleActiveConsumer=true` on a stream queue (broker rejects
    with `INTERNAL_ERROR`; reject locally).
  - `Type=QueueTypeStream` combined with `Exclusive=true`,
    `AutoDelete=true`, or `MaxPriority` set (stream queues do not
    support these).
  - `Type=QueueTypeStream` with a `DeadLetter` whose source is
    this queue (streams do not dead-letter; the DLX args are
    ignored by the broker).
  - `Binding.RoutingKey` non-empty on a binding to a fanout exchange
    (silently ignored by the broker, but flag for clarity).
  - `Exchange.Kind=ExchangeDelayed` without `Args["x-delayed-type"]`
    set to a valid kind (`direct|fanout|topic|headers`).
  - Duplicate names within a slice (two `Queue{Name: "orders"}`).
- **`NoWait=true` disables synchronous mismatch detection.** A
  successful `Declare` return with `NoWait=true` on any entry only
  means "the frame was written"; a real mismatch surfaces later as a
  channel close (and the next operation on that channel fails with
  `ErrPreconditionFailed`). Users who need synchronous mismatch
  diagnostics must leave `NoWait` at its zero value (false).
- **`Declare` is not concurrency-safe with itself or with `AttachTo`.**
  Call it once during application startup; if multiple goroutines
  may race, wrap it in `sync.Once`. `AttachTo` is concurrency-safe
  to register and may be called from any goroutine, but registers
  the same `Declare` underneath, so two concurrent `AttachTo` +
  reconnect races are still ordered by the reconnect supervisor.
- `AttachTo` registers a **deep snapshot** of the `Topology` taken
  at call time as a reconnect hook on the connection. Mutations to
  the original `Topology` value after `AttachTo` returns do not
  affect the registered redeclare — call `AttachTo` again to
  re-register a fresh snapshot (the previous one is replaced,
  keyed by topology identity at the AttachTo call site). The
  redeclare fires **before** any user `WithOnReconnect` callback
  inside the synchronous reconnect barrier (§6.1), so callbacks
  observe a topology-restored state. If the reconnect redeclare
  fails repeatedly, the connection enters degraded state (§6.1)
  and `WithOnTopologyDegraded` fires; the failure is also passed
  to `WithOnReconnect` for backward compatibility.
- All `Args` values are subject to the AMQP field-table typing rules
  documented under `Headers` (§6.5).

### 6.7 RPC

```go
type Caller[Req, Resp any] struct { /* … */ }
type Replier[Req, Resp any] struct { /* … */ }

func CallerFor[Req, Resp any](conn *Connection) *CallerBuilder[Req, Resp]
func ReplierFor[Req, Resp any](conn *Connection) *ReplierBuilder[Req, Resp]

func (c *Caller[Req, Resp]) Call(ctx context.Context, req Req) (Resp, error)
func (r *Replier[Req, Resp]) Serve(ctx context.Context, h ReplyHandler[Req, Resp]) error

type ReplyHandler[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)
```

Uses RabbitMQ `direct reply-to` pseudo-queue
(`amq.rabbitmq.reply-to`) by default; falls back to an exclusive
auto-delete reply queue when the builder calls
`UseExclusiveReplyQueue()`. `ctx` deadline maps to per-call timeout
(returns `ErrCallTimeout`).

**`direct reply-to` constraints** (per RabbitMQ documentation):
- The reply consumer must be configured with no-ack (`AutoAck`); the
  pseudo-queue does not deliver acks. `Caller` enables this
  automatically — the user does not opt in.
- The reply consumer must declare itself before publishing the request
  (`Caller.Build` does this).
- The reply pseudo-queue is bound to the consuming channel; if the
  channel closes (e.g. reconnect), pending calls error with
  `ErrChannelClosed`. `Caller` retries via its connection's reconnect
  loop transparently for new calls — in-flight calls are surfaced.
- **`basic.qos` (`Prefetch`) is not honoured** on a `direct reply-to`
  consumer — the broker rejects the frame. `Caller` does not expose a
  `Prefetch()` method for that mode; switching to
  `UseExclusiveReplyQueue()` re-enables it.
- The fallback `UseExclusiveReplyQueue()` declares a real exclusive
  auto-delete queue per `Caller` instance, with regular ack semantics.
  Slightly higher latency, but survives more failure modes.

**`Replier` ordering** (at-least-once for replies):
- For a successful handler: the replier **publishes the reply
  first**, awaits the broker confirm (subject to
  `PublishTimeout`/`ConfirmTimeout` of the internal reply publisher),
  and **then** acks the request. If the reply publish fails
  (`ErrPublishNacked`, `ErrConfirmTimeout`, `ErrChannelClosed`), the
  request is `Nack(false)` so it goes to the request queue's DLX (if
  configured) for inspection — the caller observes `ErrCallTimeout`
  via its `ctx` deadline.
- Consequence: a crash after publish-confirm but before ack causes
  the broker to redeliver the request; the replier handler runs
  again and sends a second reply. Callers must therefore treat the
  reply pipeline as at-least-once and dedupe by `CorrelationID`.
  This is the canonical at-least-once trade-off and is documented on
  `Serve`.

**`Replier` handler-error behaviour** (configured via `OnError`):
- A handler that returns a non-nil error triggers
  `Nack(requeue=false)` on the request delivery and invokes the
  `OnError(ctx, req, err)` builder hook. **No error envelope is sent
  back to the caller** — the caller observes a `ctx`-deadline
  `ErrCallTimeout`. This is intentional (keeps the wire protocol
  unambiguous: a reply is always a successful `Resp`), but it means
  a misconfigured request queue silently discards failed requests.
- **Configure a DLX on the request queue** if you want failed requests
  preserved for forensics. With no DLX, `Nack(requeue=false)` is a
  drop. The `OnError` hook is the only client-side signal; treat it
  as load-bearing (log, metric, alert). The mandatory metric
  `replier_drop_no_dlx_total{queue}` increments every time the
  framework `Nack(false)`s a request whose source queue has no
  declared DLX, so drops are never invisible — even if `OnError` is
  not wired.
- **`Replier.Build()` auto-validates DLX presence when the request
  queue was declared via `Topology` on the same connection.** When
  the user passes the `*Topology` to the builder via
  `(*ReplierBuilder).Topology(t)`, the library inspects `t.DeadLetters`
  for an entry matching the request queue. If none is present, the
  builder returns `ErrInvalidOptions` with the message
  `"Replier request queue <name> has no DeadLetter entry in Topology;
  Nack(false) drops will be silent. Add a DeadLetter or use
  AllowMissingDLX() to acknowledge."`. The `AllowMissingDLX()`
  escape hatch documents the trade-off in godoc. When the request
  queue is declared **out-of-band** (not via this library's
  `Topology`), the library cannot detect missing DLX statically and
  the metric-plus-`OnError` combination remains the only signal.
- Querying the broker via the `management` plugin is intentionally
  out of scope: it requires an extra round-trip, fails closed if the
  plugin is disabled, and adds a runtime dependency the library does
  not otherwise need.

### 6.8 Errors

```go
var (
    // Connection lifecycle
    ErrNotConnected         = errors.New("amqp: not connected")
    ErrAlreadyClosed        = errors.New("amqp: already closed")
    ErrShutdown             = errors.New("amqp: client is shutting down")
    ErrChannelClosed        = errors.New("amqp: channel closed")
    ErrConnectionBlocked    = errors.New("amqp: connection blocked by broker") // memory/disk alarm
    ErrChannelPoolExhausted = errors.New("amqp: channel pool exhausted")        // all channels in-flight; transient

    // Publisher
    ErrConfirmTimeout = errors.New("amqp: publisher confirm timeout")
    ErrUnroutable    = errors.New("amqp: mandatory publish was returned")
    ErrPublishNacked = errors.New("amqp: broker nacked publish")         // basic.nack from broker (e.g. overflow=reject-publish, disk alarm mid-publish)
    ErrPartialBatch  = errors.New("amqp: batch publish partially failed")
    ErrBatchTooLarge = errors.New("amqp: PublishBatch exceeds max in-flight budget")

    // Consumer
    ErrRequeue           = errors.New("amqp: nack with requeue")
    ErrPoison            = errors.New("amqp: poison message (nack no requeue)")
    ErrMaxRedeliveries   = errors.New("amqp: max redeliveries exceeded")
    ErrConsumerCancelled = errors.New("amqp: consumer cancelled by broker (basic.cancel)") // queue deleted, exclusive forced off, etc.

    // Codec / payload
    ErrInvalidMessage = errors.New("amqp: invalid message payload")

    // Topology
    ErrTopologyMismatch        = errors.New("amqp: topology mismatch")         // wraps ErrPreconditionFailed
    ErrTopologyRedeclareFailed = errors.New("amqp: topology redeclare failed") // connection in degraded state; permanent until next successful redeclare

    // Reconnect lifecycle
    ErrReconnecting = errors.New("amqp: connection reconnecting") // blocks Publish during the redeclare barrier; transient

    // RPC
    ErrCallTimeout = errors.New("amqp: rpc call timed out")

    // Config
    ErrInvalidOptions = errors.New("amqp: invalid options")

    // AMQP 0-9-1 reply codes — broker-originated errors. Any error from
    // a publish/consume/declare operation that traces back to a broker
    // channel/connection close wraps one of these so users can branch
    // on `errors.Is`.
    ErrContentTooLarge    = errors.New("amqp: content too large (311)")
    ErrConnectionForced   = errors.New("amqp: connection forced (320)")
    ErrInvalidPath        = errors.New("amqp: invalid path (402)")
    ErrAccessRefused      = errors.New("amqp: access refused (403)")
    ErrNotFound           = errors.New("amqp: not found (404)")
    ErrResourceLocked     = errors.New("amqp: resource locked (405)")
    ErrPreconditionFailed = errors.New("amqp: precondition failed (406)")
    ErrFrameError         = errors.New("amqp: frame error (501)")
    ErrSyntaxError        = errors.New("amqp: syntax error (502)")
    ErrCommandInvalid     = errors.New("amqp: command invalid (503)")
    ErrChannelError       = errors.New("amqp: channel error (504)")
    ErrUnexpectedFrame    = errors.New("amqp: unexpected frame (505)")
    ErrResourceError      = errors.New("amqp: resource error (506)")
    ErrNotAllowed         = errors.New("amqp: not allowed (530)")
    ErrNotImplemented     = errors.New("amqp: not implemented (540)")
    ErrInternalError      = errors.New("amqp: internal error (541)")

    // Retry classifiers (errors are wrapped with one of these for callers
    // that don't want to switch over the specific reply codes).
    ErrTransient = errors.New("amqp: transient error")
    ErrPermanent = errors.New("amqp: permanent error")
)

// AMQPCode returns the AMQP reply code embedded in err (if any) and
// true on success. Returns 0, false otherwise. Useful for fine-grained
// branching when errors.Is over the reply-code sentinels is too narrow.
//
// Recognised codes:
//   - Channel/connection close codes: 311, 320, 402-406, 501-506,
//     530, 540, 541 (each has a corresponding sentinel: ErrContentTooLarge,
//     ErrConnectionForced, …).
//   - basic.return codes: 312 (NO_ROUTE), 313 (NO_CONSUMERS).
//     These are NOT channel-close codes; they arrive in a basic.return
//     frame and the library surfaces them by wrapping ErrUnroutable
//     with the originating code. `AMQPCode(ErrUnroutable_wrapped)`
//     returns 312 or 313 depending on which the broker emitted.
func AMQPCode(err error) (code uint16, ok bool)

// IsTransient reports whether err is classified as retryable.
// True for: ErrTransient wraps; network errors during reconnect window;
// ErrChannelPoolExhausted; ErrPublishNacked (the broker may stop nacking
// once the alarm clears); ErrConnectionBlocked; ErrConfirmTimeout;
// ErrChannelClosed; ErrReconnecting; AMQP codes 311, 320, 504, 541.
//
// Note on 506 (ErrResourceError): NOT classified as transient by
// default. "Resource error" covers both transient conditions (disk
// pressure clearing) and operator-intervention conditions (FD
// exhaustion); silently retrying amplifies pressure. Callers that
// know their workload can re-classify by wrapping with ErrTransient
// explicitly.
//
// Note on ErrChannelClosed: classified transient because the next
// channel acquired from the pool will succeed; callers using
// PublishRetry must accept that a retry can produce a duplicate
// (the broker may have persisted the message before the channel
// died). See §6.2.1.
func IsTransient(err error) bool

// IsPermanent reports whether err is classified as non-retryable.
// True for: ErrPermanent wraps; ErrTopologyRedeclareFailed
// (connection in degraded state); AMQP codes 402, 403, 404, 405,
// 406, 501, 502, 503, 505, 506, 530, 540.
func IsPermanent(err error) bool
```

Errors originating from the broker (`amqp091.Error` with reply code)
are translated into wraps of the corresponding reply-code sentinel.
Users may classify either coarsely
(`errors.Is(err, amqp.ErrPermanent)`) or precisely
(`errors.Is(err, amqp.ErrNotFound)`) — both work on the same error
value.

### 6.9 Subpackages

- **`codec`** — `Codec` interface (`Encode`, `Decode`, `ContentType`),
  built-in `JSON`, `Protobuf`, and **CloudEvents in both modes**:
  - `codec.NewJSON()` — **strict** by default
    (`json.Decoder.DisallowUnknownFields()`). At billions/day with
    schema drift across services, silently dropping unknown fields
    on the consumer side is a real correctness risk; the strict
    default surfaces the drift as `ErrInvalidMessage` instead.
    `codec.NewJSONLax()` is an opt-in variant that accepts unknown
    fields for back-compat scenarios.
  - `codec.NewProtobuf()` — proto3 binary; `ContentType` =
    `application/x-protobuf`.
  - `cloudevents.NewStructured()` — full CloudEvent JSON envelope is
    the payload body; content-type `application/cloudevents+json`.
  - `cloudevents.NewBinary()` — payload body carries only `data`;
    CloudEvent attributes are mapped to AMQP headers prefixed `ce-*`
    per the CloudEvents AMQP Protocol Binding spec.

  **Panic safety contract.** Every `Codec.Encode` and `Codec.Decode`
  call is wrapped by the Publisher / Consumer in a `defer recover`
  that converts a codec panic into `ErrInvalidMessage` (wrapping the
  recovered value). Third-party codecs may not crash the
  publisher/consumer goroutine; the contract is enforced and tested.

- **`log`** — `Logger` interface, `Slog`, `Std`, `NoOp` adapters.
  All adapters route through `internal/redact.URI(s)` before
  emitting any string that contains an AMQP URI, so passwords never
  leak.

- **`metrics`** — `ClientMetrics`, `PublisherMetrics`, `ConsumerMetrics`
  interfaces + Prometheus + NoOp.

  **Default Prometheus labels (bounded).**
  - `ClientMetrics`: `addr` (host:port, no userinfo), `role`
    (`publisher`/`consumer`), `connection_name`.
  - `PublisherMetrics`: `exchange`, `outcome`
    (`ack`/`nack`/`return`/`timeout`/`pool_exhausted`/`blocked`/`error`).
  - `ConsumerMetrics`: `queue`, `outcome`
    (`ack`/`nack`/`requeue`/`decode_error`/`handler_timeout`/`resubscribed`/`aborted`).

  **High-cardinality labels (opt-in).**
  `routing_key` and `message_type` are not labelled by default
  because they explode cardinality on workloads with many routing
  patterns. Enable via `WithMetricsLabels(MetricsLabelRoutingKey,
  MetricsLabelMessageType)` on the connection or per-builder.

  **Histogram buckets.** Publish/handle latency histograms default
  to `[0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]` ms,
  optimised for AMQP-local latencies. Override with
  `WithLatencyBuckets([]float64)`.

  Mandatory metrics (always present, regardless of labels):
  `connection_reconnects_total{role}`,
  `connection_blocked_seconds{role}`,
  `consumer_resubscribed_total{queue}`,
  `consumer_handler_aborted_channel_closed_total{queue}`,
  `consumer_handler_timeout_total{queue}`,
  `publisher_in_flight{exchange}`,
  `publisher_retry_total{exchange, reason}` (reason ∈
  `nacked|confirm_timeout|channel_closed|pool_exhausted|blocked|network`),
  `replier_drop_no_dlx_total{queue}` (increments every time a
  `Replier` `OnError` fires for a request queue without a configured
  DLX — see §6.7),
  `topology_redeclare_seconds{role}` (histogram of synchronous
  redeclare windows on reconnect — see §6.6).

- **`otel`** — Tracer interface (with NoOp default baked into
  `Connection`) + AMQP header propagation following the OpenTelemetry
  **Messaging semantic conventions v1.27.0** (or later — the version
  is pinned in `go.mod` and tested against it). Attributes emitted:
  `messaging.system = "rabbitmq"`, `messaging.destination.name`
  (exchange or queue), `messaging.operation.type`
  (`publish`/`receive`/`process`), `messaging.message.id`,
  `messaging.message.conversation_id` (CorrelationID),
  `messaging.message.body.size`, `network.peer.address`,
  `network.peer.port`. Header propagation uses `traceparent` and
  `tracestate` keys; CloudEvents binary-mode `ce-*` headers do not
  conflict.

- **`amqpmock`** — generated gomock mocks for the public interfaces
  (`codec.Codec`, `log.Logger`, the three metrics interfaces, `otel.Tracer`)
  plus hand-written constructors `NewDelivery[M]` and `NewBatch[M]` to
  fabricate concrete `Delivery[M]`/`Batch[M]` values in tests.

- **`amqptest`** — public testcontainers-go helper that spins up a
  RabbitMQ node with the `rabbitmq_delayed_message_exchange` plugin
  and `rabbitmq_auth_mechanism_ssl` plugin enabled, plus
  pre-generated TLS server/client certificates under `amqptest/certs/`
  for `amqps://` integration tests. Exported so downstream
  applications can use the same fixture in their own integration
  suites; not imported by the root package at runtime.

  **Plugin enablement strategy.** Because
  `rabbitmq_delayed_message_exchange` is a community plugin that
  is **not bundled** in the official `rabbitmq:*-management` image,
  the helper supports three modes and picks the first one available
  at fixture-construction time:
  1. **Pre-baked image (preferred for CI):** if
     `AMQPTEST_IMAGE` env var is set, that image is used as-is —
     CI is expected to publish a private image with the plugin
     `.ez` baked into `/plugins/`. The library ships the
     `Dockerfile.amqptest` recipe under `amqptest/docker/` so
     downstream consumers can publish their own.
  2. **Mounted `.ez` (works offline):** if
     `AMQPTEST_DELAYED_PLUGIN_FILE` env var points at a local
     `.ez` file, the helper mounts it into `/plugins/` and enables
     it via `RABBITMQ_ENABLED_PLUGINS_FILE`. Plugin versions per
     RabbitMQ minor are listed in `amqptest/README.md` with their
     `community-rabbitmq-plugins` download URLs.
  3. **Skip the test (fallback):** if neither is set, `amqptest`
     calls `t.Skip("delayed-message plugin not available; set
     AMQPTEST_IMAGE or AMQPTEST_DELAYED_PLUGIN_FILE")`. Tests that
     declare a non-delayed topology run normally.

  This avoids the "plugin not enabled" CI surprise documented in
  `plan.md` §"Risks". Tests gated on the delayed exchange call
  `amqptest.RequireDelayedExchange(t)` at the top to opt into the
  skip-on-unavailable behaviour.

---

## 7. Testing Strategy

### Levels

| Level         | Tag             | Location                  | Frequency |
| ------------- | --------------- | ------------------------- | --------- |
| Unit          | none            | `*_test.go`               | Every commit |
| Integration   | `integration`   | `*_integration_test.go`   | Every commit (testcontainers RabbitMQ) |
| Conformance   | `conformance`   | `conformance/*`           | Every commit, separate CI job |
| Fuzz          | Go fuzz         | `*_fuzz_test.go`          | Nightly, 10m budget per target |
| Benchmark     | `-bench`        | `*_bench_test.go`         | On-demand and on release tag |

### Frameworks

- stdlib `testing` for execution.
- `github.com/stretchr/testify` for `assert`/`require`.
- `go.uber.org/mock` (mocks in `amqpmock/`).
- `github.com/testcontainers/testcontainers-go` + the official
  RabbitMQ module; pinned image tags for 3.13 LTS and 4.x.
- `go.uber.org/goleak` to assert no goroutine leaks at test exit.

### Coverage

- **Floor:** 80% per package, enforced in CI.
- **Critical paths:** `internal/reconnect`, `internal/confirms`,
  `channelpool`, codec round-trips — target ≥ 95% with table-driven
  tests for every error branch.
- Coverage uploaded as CI artifact and posted as PR comment.

### Reconnect chaos test

Integration test that disconnects the broker mid-publish and
mid-consume, asserts:
- No message loss with confirms over a 60s outage at 1k msg/s.
- No goroutine leak (`goleak.VerifyNone(t)`).
- Topology re-declared on reconnect when `AttachTo` is used.

### Poison-loop test

Integration test that pushes a message that always errors:
- Default config: 1 redelivery max (immediate Nack-no-requeue).
- `MaxRedeliveries(3)` + handler wrapping `ErrRequeue`: 3 attempts
  then permanent Nack via `ErrMaxRedeliveries`.

### Conformance

Conformance tests assert wire-protocol behaviour against the AMQP
0-9-1 spec (frame types, content header encoding, confirm ordering,
exchange semantics, mandatory return path, basic.cancel
notifications). Some run against the live broker; protocol-level
checks use a test AMQP server stub.

### Memory and concurrency

- All tests run with `-race`.
- `goleak` checks at end of every test that starts goroutines.
- Nightly 100x stress loop of connect/disconnect cycles to surface
  goroutine leaks under realistic timing.

### Executable examples at checkpoints

The library ships a runnable example for every public-surface
checkpoint, not only at v0.1.0 cut. A user-visible feature lands
together with the `examples/<feature>/main.go` that demonstrates
it, so anyone reading the repo at any commit on `main` can see
how to exercise what is already merged.

- **Mandatory checkpoint examples** (the phases listed are the
  ones in `tasks/plan.md`):
  - **Phase 2 — Producer:** `examples/publish/main.go` —
    `PublisherFor[M]`, `Publish`, mandatory + `OnReturn`,
    `ConfirmTimeout`, `PublishRetry`.
  - **Phase 3 — Topology:** `examples/topology/main.go` and
    `examples/deadletter/main.go` — `Topology.Declare`,
    `AttachTo`, DLX expansion, quorum queue.
  - **Phase 4 — Consumer:** `examples/consume/main.go` —
    `ConsumerFor[M]`, payload-first handler, default
    `Nack(false)` on error, `ErrRequeue` opt-in,
    `MaxRedeliveries`, `HandlerTimeout`.
  - **Phase 5 — Batch:** `examples/batch_publish/main.go` and
    `examples/batch_consume/main.go` — `PublishBatch`
    always-all + `[]PublishResult`, `BatchConsumer` with
    `Size`/`FlushAfter`.
  - **Phase 7 — Advanced:** `examples/rpc/main.go` and
    `examples/delayed/main.go` — `Caller[Req,Resp]`,
    `Replier[Req,Resp]`, `Message[M].Delay` via
    `x-delayed-message`.
- **Phases 1, 6, 8** do not introduce new mandatory examples.
  Phase 1 has no user-facing publish/consume surface yet; Phase 6
  (codecs + OTel) and Phase 8 (TLS/mTLS/cluster failover) evolve
  the examples already on disk (e.g. `examples/publish/main.go`
  gains an OTel exporter wiring; `examples/consume/main.go` gains
  an mTLS variant). New top-level examples are encouraged but not
  required at those checkpoints.
- **Format.** Every example is a `package main` under
  `examples/<feature>/` with a single `main.go`. It dials a local
  broker (env-driven `AMQP_URL`, default
  `amqp://guest:guest@localhost:5672/`), declares any required
  topology in-process via `Topology.Declare`, and runs to
  completion (or until `ctrl-c` for long-lived consumer
  examples). Each example carries a top-of-file godoc block
  explaining what it demonstrates and the command to run it.
- **CI verification.** Every PR runs `go build ./examples/...`
  on the unit lane (fast, no broker required). The
  `integration` lane additionally runs every example end-to-end
  against a `amqptest.NewRabbitMQ(t)` broker as a smoke test
  (re-using the same testcontainer the integration suite spins
  up). Failure to build or to run end-to-end on the integration
  lane blocks merge.
- **Pinning to plan checkpoints.** Each Phase 2/3/4/5/7
  checkpoint in `tasks/plan.md` carries an `Example(s):` line
  listing the example file(s) that must build + smoke-run before
  the checkpoint is considered closed. T38 (Phase 9) is the
  consolidation/polish pass: it does not introduce the
  checkpoint examples (they already exist on `main`) — it adds
  the remaining release-only examples (`otel/`,
  `idempotent_consume/`, `ordered_consume/`) and aligns the
  README links.

---

## 8. Boundaries

### Always

- Run `make lint test` before any commit; CI re-runs both plus
  integration and conformance.
- Preserve the MIT `LICENSE` and copyright header on every public
  source file.
- Bump semver (`MAJOR.MINOR.PATCH`) on every release tag; breaking
  changes only on `MAJOR` bumps.
- Add a `CHANGELOG.md` entry for every user-visible change.
- Document every exported identifier with a `godoc` comment.
- Default to publisher confirms enabled; never silently disable them.
- Default to `Nack(requeue=false)` on handler error; never silently
  loop poison messages.
- Propagate `context.Context` on every blocking operation.
- Run `goleak.VerifyNone` at the end of every test that starts
  goroutines.
- Keep mocks in `amqpmock/` only; root package must have zero gomock
  runtime imports.
- **Redact credentials.** Every log line, error message, span
  attribute, and metric label that includes an AMQP URI replaces the
  `userinfo` component with `***`. The redaction helper lives in
  `internal/redact` and is unit-tested against the AMQP URI shapes
  (`amqp://u:p@h`, `amqps://u:p@h:5671/vhost?heartbeat=…`,
  trailing-`/` variants).
- **Surface, do not swallow.** Backpressure conditions
  (`ErrConnectionBlocked`, `ErrChannelPoolExhausted`,
  `ErrPublishNacked`, `ErrConfirmTimeout`) return classifiable
  errors. A blocked, exhausted, or timed-out path that returns `nil`
  is a defect.
- **Re-subscribe after reconnect.** Every consumer pinned to a
  recovered TCP connection reopens its channel, reapplies
  `basic.qos`, reissues `basic.consume`, and increments
  `consumer_resubscribed_total`. Silently missing redeliveries after
  a reconnect is a defect.
- **Cancel handler ctx on channel close.** Handler contexts are
  cancelled with cause `ErrChannelClosed` when the underlying
  channel dies, so handlers can abort orphan work. `Close(ctx)` on
  the consumer also cancels with cause `ErrAlreadyClosed`.
- **Ship an executable example whenever a checkpoint exposes new
  user-facing surface.** Closing a Phase 2/3/4/5/7 checkpoint in
  `tasks/plan.md` requires the matching `examples/<feature>/main.go`
  to build (every PR) and to smoke-run end-to-end against the
  testcontainer broker (`integration` CI lane). Examples are part
  of the deliverable, not a release-only artefact (see §7
  "Executable examples at checkpoints"). Feature work that
  invalidates an existing example updates the example in the same
  PR.

### Ask first

- Adding a new external dependency.
- Changing the default value of any builder method or `With*` option.
- Breaking change to a public type, function, or option.
- Adding a new top-level subpackage.
- Migrating to a different underlying AMQP driver.
- Bumping the Go minimum version.
- Bumping the RabbitMQ supported range.
- Re-introducing any of the design decisions captured in this spec
  (generic options, decentralized topology, builder-style `Message`,
  magic sleeps, default requeue, raw-only handler, sync-only
  publish, mocks in root, single-connection abstraction, lax JSON
  default, swallowing broker nack, missing re-subscribe).
- Introducing an `OnMismatch` policy for `Topology.Declare` (only
  `ErrTopologyMismatch` is the agreed behaviour; "recreate" is
  destructive and out of scope for the lib).
- Loosening the `internal/` boundary (the rule is: nothing inside
  `internal/` is re-exported, period).

### Never

- Commit credentials, tokens, or `.env` files.
- Force-push to `main`.
- Remove or `// skip` a failing test without reproducing the failure
  and getting explicit approval.
- Bypass publisher confirms by default.
- Default to `Nack(requeue=true)` on handler error.
- Use `panic` in library code (return wrapped errors).
- Leak goroutines from a `Close()` path.
- Use `interface{}`/`any` where a generic parameter would do.
- Add `init()` side effects.
- Introduce global mutable state.

---

## 9. Success Criteria (v1.0.0)

Functional:
- [ ] Every API in §6 compiles, has godoc, is exercised by at least one
      test.
- [ ] `go test -race ./...` passes on **every Go minor version released
      and still supported by the Go team from 1.23 onward** (currently
      1.23 and 1.24; nightly when a new minor lands).
- [ ] Integration suite passes against RabbitMQ 3.13 LTS and 4.x.
- [ ] Conformance suite passes (Direct, Fanout, Topic, Headers,
      delayed-message exchanges; Quorum and Classic queues).
- [ ] All `examples/*/main.go` build and run end-to-end against a
      local broker (CI smoke test).
- [ ] Public API is `// Deprecated`-free between rc1 and v1.0.0.
- [ ] First public tag is `v0.1.0`; `v1.0.0` is cut only after every
      criterion below is checked.
- [ ] README contains a one-screen quickstart (TLS, multi-addrs, OTel,
      DLX) and links every example.

Reliability (billions/day bar):
- [ ] Reconnect chaos test: zero message loss over a **5-minute outage
      at 10k msg/s** with confirms (was 60s @ 1k msg/s in earlier
      revisions). Run nightly; flaky-rate <1% over 50 runs.
- [ ] Poison-loop test (classic queue, counter B): a perpetually-failing
      handler returning wrapped `ErrRequeue` causes at most
      `MaxRedeliveries + 1` deliveries before `Nack(requeue=false)` +
      `ErrMaxRedeliveries`.
- [ ] Poison-loop test (quorum queue, `x-delivery-limit`): same
      handler causes at most `DeliveryLimit + 1` deliveries before the
      broker dead-letters the message; `x-death` reports the right
      reason.
- [ ] **1000 connect/disconnect cycles** produce zero leaked goroutines
      (was 100).
- [ ] After a forced reconnect, every active `Consumer[M]` re-issues
      `basic.consume` and reapplies `basic.qos`;
      `consumer_resubscribed_total` increments exactly once per
      consumer.
- [ ] During a forced channel close, in-flight handler `ctx` is
      cancelled with cause `ErrChannelClosed`;
      `consumer_handler_aborted_channel_closed_total` increments.
- [ ] Broker `basic.nack` (forced via `x-overflow=reject-publish` plus
      `x-max-length=0` on the target queue) surfaces as
      `errors.Is(err, ErrPublishNacked)` and is classified
      `IsTransient(err) == true`.

Throughput:
- [ ] `go test -bench=BenchmarkPublishConfirmed -benchmem` sustains
      **≥ 30k msg/s** on a single Apple M-series laptop against a
      local broker with confirms ON and JSON codec.
- [ ] With `WithPublisherConnections(4)` + `WithChannelPoolSize(16)`,
      `BenchmarkPublishConfirmed` sustains **≥ 100k msg/s** on the
      same hardware (demonstrates the multi-connection fan-out).
- [ ] `BenchmarkPublishBatch` ≥ 5× the `BenchmarkPublishConfirmed`
      single-publish rate.

Security:
- [ ] No log line, error message, span attribute, or metric label
      emitted by the library contains a clear-text password. Verified
      by a grep-style test that scans recorded outputs after a 60s
      integration run with a credentialed URI.
- [ ] `WithSASLMechanism(SASLExternal)` + client cert authenticates
      against a RabbitMQ test broker with `rabbitmq_auth_mechanism_ssl`
      enabled; `WithAuth` becomes a no-op and logs a warning at `Dial`.

Coverage and tooling:
- [ ] Coverage ≥ 80% per package; ≥ 95% on `internal/reconnect`,
      `internal/confirms`, `channelpool`, `internal/amqperror`,
      `internal/redact`.
- [ ] `Connection` ships with a no-op `otel.Tracer` by default; no code
      path branches on "tracing disabled".
- [ ] `codec.CloudEvents` supports both structured and binary modes per
      the CloudEvents AMQP Protocol Binding spec; `codec.NewJSON()`
      defaults to strict (`DisallowUnknownFields`).
- [ ] AMQP reply codes from the broker are surfaced as wraps of the
      §6.8 reply-code sentinels (`ErrAccessRefused`, `ErrNotFound`,
      `ErrPreconditionFailed`, etc.) and parseable via `AMQPCode(err)`.
- [ ] `Message[M].ContentType` and `ContentEncoding` populate the
      correct AMQP `basic.properties` fields (not swapped); verified by
      a round-trip test asserting the broker-side values via
      `rabbitmqadmin get`.

Operational invariants (Rev 6):
- [ ] **`ConfirmTimeout` default is 30s**; `Publisher` with
      `context.Background()` and no overrides surfaces
      `ErrConfirmTimeout` within 30s if the broker withholds the ack
      (verified by a mock-channel unit test).
- [ ] **Reconnect synchronisation barrier:** during the §6.1
      barrier, `Publisher.Publish` returns `ErrReconnecting` on
      `ctx` cancel; once the barrier clears, publishes succeed
      without 404 closes against re-declared exchanges/queues.
      Verified by a chaos integration test that publishes
      continuously while a broker restart is in progress.
- [ ] **Topology degraded state:** persistent redeclare failure
      drives the connection into degraded state;
      `connection_degraded_total{role, reason}` increments;
      `WithOnTopologyDegraded` fires exactly once per transition;
      publishes return `ErrTopologyRedeclareFailed` (permanent).
- [ ] **`PublishRetry` produces duplicates by design:** the
      `publisher_retry_total{exchange, reason}` metric increments
      on each retry; `examples/idempotent_consume/` demonstrates
      the dedupe pattern from §6.2.1.
- [ ] **Consumer-tag default uniqueness:** N defaulted
      `Consumer[M]` instances open with N distinct UUIDv7-derived
      tags and pin to N (or `WithConsumerConnections(m)`,
      whichever is smaller) connections evenly.
- [ ] **`HandlerTimeout` default verdict** is `TimeoutNackNoRequeue`
      (consistent between Consumer and BatchConsumer); flipping
      to `TimeoutNackRequeue` via the builder is the documented
      escape hatch.
- [ ] **`basic.cancel`** delivered by the broker fires `OnCancel`
      and returns `Consume` with `ErrConsumerCancelled`; the
      consumer goroutine does not silently die.
- [ ] **SASL EXTERNAL fail-closed validation:** `Dial` returns
      `ErrInvalidOptions` if TLS is missing, the cert is absent,
      or the scheme is plain `amqp://`.
- [ ] **`UserID` client-side validation:** `Publish` with a
      diverging `UserID` returns `ErrInvalidMessage` locally
      without writing the publish frame.

---

## 10. Resolved Decisions

The questions raised during spec review are closed. Recorded here for
the next reader so the rationale survives the conversation:

1. **`AutoAck()` is exposed**, with a prominent warning block in both
   §6.3 and the method's godoc (semantics bypassed, no redelivery, no
   prefetch backpressure, no DLX engagement). Protocol fidelity wins;
   ergonomics is preserved via the warning.

2. **`Topology.Declare` mismatch is always an error** —
   `ErrTopologyMismatch`. No `OnMismatch(policy)`. Reasoning:
   "recreate" deletes the queue (and every message in it), which is
   too destructive to expose as a library policy. Operators handle
   migrations explicitly (rename queue, delete via `rabbitmqctl`, or
   change the spec to match the broker).

3. **OTel: no-op tracer baked into `Connection` by default.**
   `WithTracer` swaps in a real one. Single code path, no `if tracer
   != nil` branching.

4. **CloudEvents codec: both modes** — structured
   (`application/cloudevents+json` envelope as body) and binary
   (`data` in body, attributes in `ce-*` AMQP headers). The latter is
   the real interop story with non-Go clients.

5. **Versioning: first tag is `v0.1.0`**, stabilising to `v1.0.0` once
   all success criteria are checked.

6. **CI Go matrix: every push runs the full matrix of every Go minor
   version still supported by the Go team, starting at 1.23.**
   Currently that means 1.23 and 1.24; widens automatically as 1.25+
   land. No "only minimum on push, matrix nightly" split.

7. **`internal/` is strictly internal.** No re-export from the root
   package. If a type is genuinely needed by external callers, it
   lives outside `internal/` from the start.

8. **`PublishBatch` always publishes all N** and returns
   `[]PublishResult` (one slot per input). On any failure the function
   error wraps `ErrPartialBatch`; per-message detail is in
   `PublishResult.Err`. No short-circuit.

9. **`Delivery[M]` and `Batch[M]` are concrete structs**, not
   interfaces. Methods can be added in minor releases without
   breaking implementers. Tests use `amqpmock.NewDelivery[M](…)` /
   `amqpmock.NewBatch[M](…)` constructors.

10. **AMQP 0-9-1 compliance fixes** applied after a dedicated review
    (see the `Note on AMQP 0-9-1 vs RabbitMQ` at the head of §6).
    Summary of the 22 changes:
    - **Removed:** `Immediate()` publisher option (RabbitMQ closes the
      channel if set); `NoLocal()` consumer option (silently ignored
      by RabbitMQ).
    - **Renamed:** `GlobalQoS()` → `ChannelQoS()` to reflect
      RabbitMQ's per-channel semantics.
    - **Kept with explicit no-op note:** `PrefetchBytes()` (RabbitMQ
      honours only `prefetch-count`).
    - **Added:** `Message[M].ContentType` separated from
      `ContentEncoding`; `Exchange/Queue/Binding.NoWait`;
      `Queue.Type` (`x-queue-type`); `DeadLetter.MaxLengthBytes` +
      `Overflow` (`OverflowPolicy`); `ReturnedProperties` struct in
      `Return`; AMQP reply-code sentinels (311, 320, 402–406,
      501–506, 530, 540, 541) + `AMQPCode(err)` helper;
      `ErrConnectionBlocked`; godoc notes on `Priority` range,
      `Expiration` wire format, `Headers` field-table typing,
      `Delivery.Ack/Nack` failure modes, `direct reply-to`
      constraints, and topology declare ordering.
    - **Default change:** `WithChannelMax` default `2047` → `0`
      (server-driven negotiation) for portability across broker
      configurations.
    - **Validation:** `Dial` checks `ChannelPoolSize <=` negotiated
      `channel-max` and returns `ErrInvalidOptions` if violated.
    - **Semantic clarification:** `Publish` / `PublishBatch` block
      while the connection is broker-blocked, returning
      `ErrConnectionBlocked` only on `ctx` cancellation.

11. **`Topology.Declare` is a two-step pipeline**: an in-memory
    DeadLetter expansion that merges DLX args into source queue
    definitions, followed by a single broker-side declare in the
    order exchanges → queues → bindings. This is required because
    AMQP 0-9-1 rejects `queue.declare` with non-matching args
    (`PRECONDITION_FAILED` /406) — args cannot be added to an existing
    queue via re-declare. The previous wording ("exchanges → queues
    → bindings → DeadLetter expansion") implied a post-hoc arg
    injection that is impossible against a live broker.

12. **`MaxRedeliveries` uses two counters** because AMQP 0-9-1 only
    writes `x-death` on dead-letter events, not on
    `Nack(requeue=true)`. The consumer tracks (a) `DeathCount()` from
    `x-death` (bounds DLX-bounce loops, survives consumer restarts
    when a DLX-back-to-source binding exists) and (b) an in-process
    counter keyed by `MessageID` (bounds `ErrRequeue` loops within
    the consumer's lifetime). Both ceilings escalate to
    `Nack(requeue=false)` + `ErrMaxRedeliveries`. The previous
    single-counter wording ("`x-death` count exceeds the limit ...
    even when the handler keeps wrapping `ErrRequeue`") was
    architecturally impossible — `ErrRequeue` never bumps `x-death`.

13. **Headers field-table typing matches `amqp091-go`'s encoder
    surface**, including `Decimal{Scale, Value}` (AMQP type `D`).
    Native Go `int` / `uint` literals auto-coerce to `int64` /
    `uint64` so the ergonomic `Headers{"count": 5}` works without
    surprise — previously the strict "any other Go type returns
    `ErrInvalidMessage`" wording would have rejected it.

14. **Mandatory + `basic.return` correlation.** RabbitMQ sends
    `basic.return` before `basic.ack` for unroutable mandatory
    publishes. The confirm tracker correlates the two frames by
    `delivery-tag` and resolves the publish as `ErrUnroutable`
    rather than success, with `OnReturn` firing synchronously before
    `Publish` unblocks.

15. **`NoWait=true` on topology declares disables synchronous
    mismatch detection.** A mismatch with `NoWait=true` surfaces only
    later as a channel close wrapping `ErrPreconditionFailed`. Users
    who need synchronous `ErrTopologyMismatch` must leave `NoWait` at
    its zero value (false).

16. **`Replier.OnError` is the only signal for handler-side failures.**
    The library does not send an error envelope back to the `Caller`;
    the caller observes `ErrCallTimeout` on `ctx` deadline. Failed
    requests are `Nack`'d without requeue — if the request queue has
    no DLX, this is a drop. Users must configure a DLX on the
    request queue for forensics, and the godoc on `OnError`
    documents this load-bearing behaviour in full.

17. **Connection abstraction fans out across multiple TCP
    connections, split by role.** `*Connection` wraps a pool with
    `WithPublisherConnections(n)` (default 1) and
    `WithConsumerConnections(n)` (default 1). The reason is
    `amqp091-go` serializes I/O per `amqp.Connection`: one TCP
    socket bounds confirm throughput to whatever a single goroutine
    can drive. A single socket is fine for tens of thousands of
    messages/second; sustained higher rates require role-based
    fan-out. Consumers are pinned by hash of their consumer-tag so
    long-lived consumers are balanced across sockets without
    reshuffling on every call.

18. **Quorum-queue `x-delivery-limit` is the preferred poison
    bound, not the consumer-side counter B.** When the queue is
    declared `Type=QueueTypeQuorum` with `DeliveryLimit>0`, the
    broker increments the counter on every redelivery (including
    `Nack(requeue=true)`) and dead-letters on overflow. The
    consumer's in-process counter B is unnecessary in that
    configuration; the godoc on `MaxRedeliveries` and `DeliveryLimit`
    cross-references this. The consumer-side two-counter design
    survives for classic queues and for quorum queues where the
    operator chose not to set a broker limit.

19. **Single Active Consumer and consumer priorities are
    first-class.** `Queue.SingleActiveConsumer bool` maps to
    `x-single-active-consumer`; `ConsumerBuilder.Priority(int)` maps
    to `x-priority` on `basic.consume`. Required for ordered
    processing with failover and for active/standby consumer
    topologies, both common at billions/day.

20. **`basic.nack` from the broker is a distinct sentinel,
    `ErrPublishNacked`.** Overflow policies (`reject-publish`,
    `reject-publish-dlx`) and mid-publish disk/memory alarms cause
    the broker to nack the publisher channel. Earlier wording
    folded this into `ErrConfirmTimeout` or success; it is now an
    explicit, classifiable transient error. `IsTransient(err) ==
    true` for `ErrPublishNacked`.

21. **Consumers automatically re-subscribe after reconnect.**
    `Topology.AttachTo` redeclares topology; the consumer connection
    independently reopens its channel, reapplies `basic.qos`, and
    reissues `basic.consume`. The metric
    `consumer_resubscribed_total{queue}` is mandatory. Earlier
    wording left this implicit; with no resubscribe, after a single
    network blip the consumer is silent.

22. **Handler ctx is cancelled with cause `ErrChannelClosed` when
    the consumer channel dies.** The broker will redeliver, so the
    in-flight handler is wasted work; cancelling its `ctx` lets the
    handler abort. `consumer_handler_aborted_channel_closed_total`
    increments per such event.

23. **`codec.NewJSON()` is strict by default
    (`DisallowUnknownFields`).** With dozens of services sharing
    schemas at scale, silent unknown-field dropping is a real
    correctness risk. `codec.NewJSONLax()` is the explicit opt-out.
    Every codec call is also wrapped in `recover` — codec panics
    become `ErrInvalidMessage`, never crash the
    publisher/consumer goroutine.

24. **Stream queues are v0.2.** `QueueTypeStream` ships in v0.1 for
    declaration only; native stream consume (`x-stream-offset`,
    super-streams, etc.) is tracked for v0.2 to keep v0.1 scope
    bounded.

### Rev 6 — post code-review (specialist pass)

The decisions below were captured during the "act as an AMQP/
RabbitMQ specialist" review and close the duplicates / ordering /
timeout-stall / reconnect-race / Replier-drop family of gaps that
the Rev 5 surface still left invisible to a non-specialist user.

25. **At-least-once is the contract; consumers MUST dedupe.** The
    library uses publisher confirms and automatic retry; under
    `PublishRetry` (and even without it, on `ErrConfirmTimeout` or
    `ErrChannelClosed`), the broker can persist a message whose
    ack the publisher never observes. Retry → duplicate. The new
    §6.2.1 documents the canonical dedupe pattern keyed by
    `MessageID` (auto-populated UUIDv7), and a runnable reference
    lives in `examples/idempotent_consume/`. The
    `publisher_retry_total{exchange, reason}` metric makes
    retries observable.

26. **`HandlerTimeout` default verdict is `TimeoutNackNoRequeue`.**
    Consistent across `Consumer[M]` and `BatchConsumer[M]`. A
    handler that runs over the deadline is poison until proven
    otherwise; the explicit override is
    `HandlerTimeoutVerdict(TimeoutNackRequeue)`. Earlier Rev 5
    text contradicted itself across SPEC and TODO (`Nack(false)`
    in §6.4 vs `Nack(true)` in T18 acceptance) — now unambiguous.

27. **Reconnect synchronisation barrier.** On every reconnect, the
    supervisor runs (1) channel re-open → (2) every
    `Topology.AttachTo` redeclare → (3) consumer re-issue of
    `basic.consume` and re-apply of `basic.qos` → (4) user
    `WithOnReconnect`, **synchronously**. `Publisher.Publish`
    routed to a reconnecting connection blocks until step 2 ends
    (returning `ErrReconnecting` only on `ctx` cancel). This
    closes the race where a publish during reconnect lands before
    the exchange is re-declared and crashes the channel with 404.

28. **Topology degraded mode.** If the reconnect redeclare fails
    persistently, the connection enters a `degraded` state:
    publishes return `ErrTopologyRedeclareFailed` (permanent),
    consumers do not re-issue `basic.consume`,
    `connection_degraded_total{role, reason}` increments, and
    `WithOnTopologyDegraded` fires exactly once. The library
    keeps retrying redeclare on every reconnect cycle; the first
    success clears the degraded flag and resumes traffic. Silent
    no-op publishes against a broken topology is the failure mode
    this prevents.

29. **`ConfirmTimeout` default is 30 s.** With the Rev 5 default
    of zero, a `Publisher.Publish(context.Background(), …)` call
    could block forever if the broker withheld `basic.ack`,
    violating the §1 "no silent backpressure failure" north star.
    30 s aligns with conservative production tuning; explicit
    `ConfirmTimeout(0)` is the documented escape hatch and is
    discouraged.

30. **`Concurrency(n)` with `n > 1` is explicitly documented as
    "you have given up per-queue ordering".** RabbitMQ's wire-level
    in-order delivery is preserved only at the channel boundary;
    parallel dispatch reorders at the handler boundary. The
    canonical ordered-with-failover pattern is
    `Concurrency(1) + Queue.SingleActiveConsumer=true`, with a
    runnable reference in `examples/ordered_consume/`.

31. **`PublishBatchMaxSize` (renamed from
    `PublishBatchMaxInFlight`)** is a per-call cap, not a sliding
    in-flight window. The previous name implied semantics the
    library does not provide. Default remains 1024.

32. **`Prefetch` guidance is explicit.** The builder logs a
    warning at `Build` if `Prefetch < Concurrency`, and §6.3
    documents the throughput formula
    `throughput ≈ Concurrency / handler_latency` plus rule-of-thumb
    multipliers (2×, 4×, 8× Concurrency depending on latency).

33. **`Replier` missing-DLX validation + mandatory metric.** When
    `ReplierBuilder.Topology(t)` is wired, the builder validates
    that the request queue has a `DeadLetter` entry; absence
    returns `ErrInvalidOptions` unless `AllowMissingDLX()` opts in
    explicitly. The mandatory metric `replier_drop_no_dlx_total`
    increments every time the framework `Nack(false)`s a request
    whose queue has no DLX, so drops are observable even when
    `OnError` is not wired.

34. **`x-death` parser filters by reason.** `DeathCount()` sums
    entries with `reason ∈ {rejected, delivery-limit}` only;
    `expired` and `maxlen` reasons reflect broker policy rather
    than handler-driven rejection and would falsely trigger
    `MaxRedeliveries` on innocently-aged messages. New methods
    `DeathCountByReason(r)` and `DeathReasons()` expose the full
    parsed shape for custom policies.

35. **SASL EXTERNAL fail-closed validation.** `Dial` rejects
    `WithSASLMechanism(SASLExternal)` when (a) no `WithTLSConfig`
    was provided, (b) the TLS config carries no client cert, or
    (c) the endpoint is plain `amqp://`. The broker-side 403 close
    is unhelpfully terse and operators frequently misdiagnose it;
    local validation surfaces the real cause.

36. **Default `WithPublisherConnections=2` and
    `WithConsumerConnections=2`.** The Rev 5 default of 1
    contradicted the "billions/day" reliability bar: a single
    socket reconnecting is a full-availability gap on its role.
    2 is the minimum viable for graceful failover. `n=1` is still
    supported and logs a `Dial`-time warning.

37. **Default consumer-tag is `ctag-<uuidv7>`.** Hashing the empty
    string would pin every defaulted consumer to the same
    connection, defeating the multi-conn fan-out. User-supplied
    tags via `Tag(string)` pass through unchanged.

38. **`AMQPCode(err)` covers `basic.return` codes 312 and 313.**
    The previous omission was protocol-correct (those are not
    channel-close codes) but API-inconsistent: users could
    `errors.Is(err, ErrUnroutable)` but `AMQPCode(err)` returned
    `(0, false)`. The library now wraps `ErrUnroutable` with the
    originating code from `basic.return`.

39. **`UserID` client-side validation.** When
    `Message[M].UserID != ""` and disagrees with the connection's
    authenticated user, `Publish` returns `ErrInvalidMessage`
    locally without writing the frame, avoiding the
    406-channel-close-on-divergence footgun. The
    `PublisherBuilder.StampUserID()` opt-in auto-sets `UserID`
    from the connection so the broker-side stamp survives without
    user bookkeeping.

40. **`amqptest` plugin enablement is explicit.** Three modes:
    pre-baked image via `AMQPTEST_IMAGE`, mounted `.ez` via
    `AMQPTEST_DELAYED_PLUGIN_FILE`, or `t.Skip` via
    `amqptest.RequireDelayedExchange(t)`. The "Plugin not enabled"
    failure mode noted in `plan.md` §"Risks" is now a deliberate,
    documented skip rather than a confusing test failure.

41. **`AttachTo` takes a deep snapshot.** The registered redeclare
    operates on a frozen copy of `Topology`; later mutations are
    invisible. Re-`AttachTo` to register a fresh snapshot.

42. **Topology validation rejects incompatible combinations.** SAC
    on a stream queue, `DeliveryLimit > 0` on a classic queue,
    stream queues with `Exclusive`/`AutoDelete`/dead-letter
    config, fanout binding with non-empty routing key, delayed
    exchange without `x-delayed-type` — all reported as
    `ErrInvalidOptions` at `Topology.Declare` time instead of
    failing obscurely later.

43. **`PublishBatch` retry is the caller's problem, not
    `PublishRetry`'s.** `PublishRetry` only applies to single
    `Publish` calls. Per-message `ErrChannelClosed` in a
    `[]PublishResult` cannot be distinguished from "broker
    persisted but ack was lost" — retrying produces duplicates;
    the application owns the chunking and partial-retry policy.

44. **Options follow last-wins.** `WithoutMetrics()` is equivalent
    to `WithMetrics(NoOp{})`; later calls override earlier ones.
    Builder-level options override connection-level options for
    the same field on the constructed component.

45. **`basic.cancel` surfaces as `ErrConsumerCancelled`.** The
    library advertises `consumer_cancel_notify=true`; on receipt,
    `OnCancel(reason)` fires and `Consume` returns
    `ErrConsumerCancelled` so the consumer goroutine is never
    silently dead. The metric `consumer_cancelled_total{queue,
    reason}` is mandatory.

46. **Frame max sizing guidance is documented.** Default 128 KiB
    fits typical messages; raise to 1 MiB for streaming. The
    library refuses `frame_max < 4096` (AMQP-spec minimum) at
    `Dial`. Payloads > ~100 MiB should pointer-out to an object
    store, not flow as AMQP bodies.

47. **Heartbeat partition-detection window is documented.**
    Default 10 s ⇒ ≈20 s partition detection. Sizing guidance:
    keep tight for high-throughput, raise behind chatty LBs, never
    set to 0.

48. **`golangci-lint` enables `errorlint`.** This codebase depends
    crucially on `errors.Is` / `errors.As` over wrapped sentinels;
    `errorlint` catches `if err == ErrFoo` typos and missing
    `%w` verbs that would otherwise corrupt error classification.

These 24 Rev 6 decisions are closed. Any reopening needs a fresh
spec amendment.

49. **Executable examples land at checkpoints, not only at release.**
    Phase 2/3/4/5/7 checkpoints in `tasks/plan.md` each carry an
    `Example(s):` line listing one or more `examples/<feature>/main.go`
    files that must build (every PR, unit lane) and smoke-run against
    a testcontainer broker (`integration` lane) before the checkpoint
    is considered closed. Layout is one directory per feature
    (matching §4); later phases enrich the same directory rather
    than spawning new ones (e.g. Phase 6 adds OTel wiring to the
    existing `examples/publish/`). T38 in Phase 9 becomes the
    consolidation/polish pass: it adds the remaining release-only
    examples (`otel/`, `idempotent_consume/`, `ordered_consume/`)
    and aligns the README quickstart links — it does not introduce
    the checkpoint examples, which already exist on `main`. The
    motivation is that prior iterations of this library shipped
    examples only at release time, leaving users without a runnable
    reference for any feature that had landed but not yet been cut.
    See §7 "Executable examples at checkpoints" for the policy and
    §8 Always for the enforcement bullet.
