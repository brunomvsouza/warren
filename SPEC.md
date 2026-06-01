# Spec: Warren

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
| Language        | Go **1.25+**                                       |
| Module path     | `github.com/brunomvsouza/warren`                     |
| Repo            | `github.com/brunomvsouza/warren` (working dir)      |
| AMQP transport  | `github.com/rabbitmq/amqp091-go` (BSD-2-Clause)    |
| Broker          | RabbitMQ 3.13 LTS and 4.x                          |
| Test fixtures   | `warren.NewDeliveryFixture`/`NewBatchFixture` (no mock library shipped) |
| Metrics default | `github.com/prometheus/client_golang`              |
| Tracing         | `go.opentelemetry.io/otel` (opt-in)                |
| UUID            | `github.com/google/uuid`                           |
| Testing         | stdlib `testing` + `testify` + `testcontainers-go` + `goleak` |
| Lint            | `golangci-lint`                                    |
| License         | MIT                                                |

Dependency policy: minimal. Anything beyond the table above requires
explicit approval. No mock library is shipped — downstream code generates
its own interface mocks with whatever tool it prefers. The testcontainers
broker fixture lives in `internal/amqptest`, so its docker dependencies
never reach a consumer's build.

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
├── fixture.go                    # NewDeliveryFixture / NewBatchFixture (public test fixtures)
│
├── internal/amqptest/            # internal testcontainers helper (warren's own integration suite)
│   ├── rabbitmq.go               # spins up RabbitMQ with delayed-message plugin
│   ├── docker/Dockerfile.amqptest
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
  the error: `fmt.Errorf("transient: %w", warren.ErrRequeue)`.
  Escape hatch: `Consumer[M].ConsumeRaw(ctx, func(ctx, *Delivery[M]) error)`.
- **`Topology` is the only place that declares.** Publisher/Consumer
  receive names; they do not declare exchanges/queues/bindings.
- **Sentinel errors** in `errors.go`. Wrap with
  `fmt.Errorf("warren: …: %w", err)` at boundaries. Callers use
  `errors.Is`/`errors.As`.
- **`context.Context` is mandatory** on every blocking operation
  (`Dial`, `Publish`, `PublishBatch`, `Consume`, `Health`, `Close`).
- **No magic sleeps.** Backpressure is `prefetch_count`. Retry delay is
  explicit (`RetryPolicy`), not a hidden `time.Sleep` in the consume
  loop.
- **No globals, no `init()`-side-effects.** Everything constructible
  and injectable.
- **No mock library is shipped.** Downstream generates its own
  interface mocks; warren provides only the `Delivery[M]`/`Batch[M]`
  fixtures it alone can build (`NewDeliveryFixture`/`NewBatchFixture`).
  The testcontainers broker fixture is `internal/amqptest`.

### Example: publish

```go
package main

import (
    "context"
    "log/slog"
    "os/signal"
    "syscall"
    "time"

    "github.com/brunomvsouza/warren"
    "github.com/brunomvsouza/warren/codec"
    amqplog "github.com/brunomvsouza/warren/log"
)

type OrderPlaced struct {
    OrderID string  `json:"order_id"`
    Total   float64 `json:"total"`
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    conn, err := warren.Dial(ctx,
        warren.WithAddrs([]string{
            "amqps://user:pass@rabbit-1:5671",
            "amqps://user:pass@rabbit-2:5671",
        }),
        warren.WithLogger(amqplog.NewSlog(slog.Default())),
    )
    if err != nil {
        panic(err)
    }
    defer conn.Close(ctx)

    // Topology — declared once, separately.
    topo := warren.Topology{
        Exchanges: []warren.Exchange{
            {Name: "orders", Kind: warren.ExchangeTopic, Durable: true},
        },
        Queues: []warren.Queue{
            {Name: "orders.placed", Durable: true},
        },
        Bindings: []warren.Binding{
            {Exchange: "orders", Queue: "orders.placed", RoutingKey: "orders.placed"},
        },
    }
    if err := topo.Declare(ctx, conn); err != nil {
        panic(err)
    }
    topo.AttachTo(conn) // re-declare automatically on reconnect

    // Publisher — references topology by name; does not declare anything.
    pub, err := warren.PublisherFor[OrderPlaced](conn).
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

    err = pub.Publish(ctx, warren.Message[OrderPlaced]{
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
con, err := warren.ConsumerFor[OrderPlaced](conn).
    Codec(codec.NewJSON()).
    Queue("orders.placed").
    Concurrency(8).
    Prefetch(100).
    Build()
if err != nil { panic(err) }

err = con.Consume(ctx, func(ctx context.Context, o OrderPlaced) error {
    if !o.Valid() {
        return warren.ErrPoison           // Nack(requeue=false) → DLX
    }
    if err := process(ctx, o); err != nil {
        return fmt.Errorf("retry: %w", warren.ErrRequeue)  // Nack(requeue=true)
    }
    return nil                          // Ack
})
```

### Example: consume raw (escape hatch)

```go
err = con.ConsumeRaw(ctx, func(ctx context.Context, d *warren.Delivery[OrderPlaced]) error {
    if d.Redelivered() && d.DeathCount() > 3 {
        return d.Nack(false) // give up
    }
    o := d.Body()
    return d.AckIf(process(ctx, *o))
})
```

### Naming

- **Package names**: lowercase, short, no underscores (`codec`, `log`,
  `metrics`, `otel`).
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
func (c *Connection) Close(ctx context.Context) error    // gracefully shuts down components in sequence
func (c *Connection) Health(ctx context.Context) error   // verifies TCP socket and topology state

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
| `WithReconnectBackoff(p RetryPolicy)`   | exponential + full jitter (default; configurable via `RetryPolicy.Jitter`) |
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
  per `warren.Connection`, so one TCP socket bounds the achievable
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
     temporary channel (in the order they were registered). The
     redeclare is **de-amplified to once per recovery wave** at the
     `*Connection` level (not once per pooled connection): a broker
     restart closes every socket at once, and firing N×pool×fleet
     `queue.declare`s at a just-recovered, fragile broker compounds with
     the `WithAddrs` rotation (DS-10/T66) into a self-amplifying
     **recovery storm**. The first reconnecting barrier in a wave
     performs the declares and anchors a short coalesce window;
     concurrent or window-local barriers wait for it and skip, so each
     consumer still sees topology restored before its `basic.consume`
     (step 3) while the broker sees the topology declared once
     (DS-09/SRE-06).
  3. For consumer connections, re-apply `basic.qos` and re-issue
     `basic.consume` on the consumer's channel (this stays **per
     consumer connection** — only the broker-global topology declare is
     coalesced).
  4. Fire any `WithOnReconnect(func())` user callback last; by the
     time it runs the application sees a topology-restored,
     consumer-resubscribed state.
  Until step 2 completes, `Publisher.Publish` calls routed to the
  reconnected connection **block** on a per-connection condition
  variable (still cancellable via `ctx`); on `ctx` cancel they
  return `ErrReconnecting` (wraps `ctx.Err()`, classified
  `IsTransient`). The `topology_redeclare_seconds{role}` histogram
  records the duration of step 2 per reconnect cycle.
- **Reconnect barrier max-duration cap (`WithReconnectBarrierTimeout`,
  default 15 s).** The barrier is bounded so a "half-alive" broker that
  accepts the socket but stalls on `queue.declare` (e.g. Khepri
  Raft-quorum recovery during a partition, RMQ-17) cannot hang
  publishers indefinitely. This is a **distinct mechanism from
  `ConfirmTimeout`**: `ConfirmTimeout` covers a written-but-unconfirmed
  publish, whereas at the barrier the publish frame is **still
  unwritten**, so `ConfirmTimeout` does not apply (DS-02). Two effects on
  cap: (a) a publisher blocked on the barrier returns `ErrReconnecting`
  (transient, retryable) rather than stalling past the cap — this holds
  even with the default `PublishTimeout=0` and a `context.Background()`;
  (b) the **post-cap connection state is force-reconnect** — the
  supervisor closes the half-alive socket and re-dials (with `WithAddrs`
  the rotation lands on a different node), then re-runs the barrier. It
  is deliberately **not** the degraded state: degraded means a
  *conflicting* topology the broker rejected (permanent until fixed),
  whereas a capped barrier is a *slow/half-alive* broker that a fresh
  connection escapes. The default (15 s) is well under `ConfirmTimeout`
  (30 s); SRE-11/T113 must set the default histogram top bucket ≥ this
  cap so a capped stall is visible in `publisher_publish_seconds` rather
  than collapsed into `+Inf`.
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
  The callback runs **asynchronously** on a tracked goroutine (drained on
  `Close`), so it may still be running — or start — after the matching
  `unblocked` has been processed; do not assume it completes before traffic
  resumes.
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
- `Close(ctx)` gracefully shuts down the connection and its managed
  components in a strict cascade to prevent message loss:
  1. Issues `basic.cancel` to all active consumers (stopping broker delivery).
  2. Waits for active consumer handlers to finish and send their ack/nacks.
  3. Waits for in-flight publishes to receive their confirms from the broker.
  4. Closes every TCP connection in the pool.
  This entire cascade runs up to the `ctx` deadline; if exceeded,
  connections are closed forcefully.
  **Component registration (lifecycle contract).** "Managed
  components" are the `Publisher[M]` / `Consumer[M]` / `BatchConsumer[M]`
  / `Caller` / `Replier` values built from this `*Connection`: each
  `XFor[M](conn).…Build()` **registers** the component with the
  connection, which is how `Close` can issue `basic.cancel` to every
  active consumer and drain every in-flight publisher without the
  caller passing them back in. Calling a component's own `Close(ctx)`
  is still supported and is **idempotent and safe alongside**
  `Connection.Close` — whichever runs second is a no-op returning
  `ErrAlreadyClosed`, never a double `channel.close` / double-cancel
  on the broker. Closing a component individually also deregisters it
  from the connection's cascade.
- `Health(ctx)` verifies that the connection pool has at least one
  healthy TCP socket and that the connection is not in a degraded
  topology state. It opens and closes a temporary channel to
  validate broker responsiveness.
- A no-op `otel.Tracer` is baked in by default so instrumentation code
  paths run uniformly. `WithTracer` swaps in a real tracer; there is no
  "tracing-disabled" code branch.
- `WithConnectionName` writes `client_properties.connection_name`,
  visible in `rabbitmqctl list_connections name client_properties`.
  Default is `<binary>-<hostname>-<pid>`. Role and index suffixes
  (`<base>-pub-0`, `<base>-pub-1`, `<base>-con-0`, …) are appended
  per TCP connection so the broker side shows every socket
  individually. **Keep the base name reasonably short:** the
  management UI and some `rabbitmqctl` listings truncate long
  connection names for display, and the per-socket role/index
  suffixes add to the length — an over-long base makes the
  individual sockets hard to tell apart in operator tooling.

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
- **TLS and client-properties hygiene.** The library passes
  `WithTLSConfig(*tls.Config)` through verbatim — it never overrides
  your verification settings. For production `amqps://`, set
  `tls.Config.ServerName` to the broker hostname and do **not** set
  `InsecureSkipVerify` (it disables certificate validation and defeats
  EXTERNAL's identity guarantees). Anything placed in
  `WithClientProperties` is visible broker-side via
  `rabbitmqctl list_connections client_properties` — never put secrets
  there.

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
func (b *PublisherBuilder[M]) MaxMessageSizeBytes(n int) *PublisherBuilder[M]                    // per-publish payload guardrail; default 16 MiB; 0 disables
func (b *PublisherBuilder[M]) PublishRetry(p RetryPolicy) *PublisherBuilder[M]                    // automatic retry of ErrTransient publishes
func (b *PublisherBuilder[M]) WithPublishRateLimit(perSec int) *PublisherBuilder[M]               // local token bucket; paces Publish to perSec msg/s (burst perSec); 0 disables; not applied to PublishBatch
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
- **`multiple=true` resolution.** Batching against RabbitMQ is efficient at the
  wire level: the broker coalesces many confirms into one `basic.ack`/`basic.nack`
  frame with `multiple=true`. **In the current wiring the confirm tracker never
  sees that flag, though:** `amqp091-go`'s confirm listener (`NotifyPublish`)
  expands and resequences a `multiple=true` frame into individual, in-order
  `Confirmation` values before they reach the tracker, so steady-state production
  resolution is one `delivery-tag` at a time (`tracker.Ack(tag, false)`). The
  tracker still resolves a `multiple=true` frame correctly — a contract-level
  safety net for direct-frame use, not the production confirm path — by advancing
  a contiguous confirmed low-water-mark over an ascending index of registered
  tags. That index, maintained on `Register`, is what keeps both the per-confirm
  steady state ordered and any `multiple=true` frame **O(resolved)** amortised
  (the count actually confirmed, not O(outstanding)) with no per-frame
  allocation; it is also self-compacting so it stays bounded by the outstanding
  window. The earlier "single pass, critical for high-throughput batching"
  framing overstated the tracker's role: the throughput win is the broker's
  frame coalescing, which `amqp091-go` unfolds client-side.
- **`basic.return` / `basic.ack` correlation for mandatory publishes.**
  For an unroutable mandatory publish, RabbitMQ sends `basic.return`
  *first*, then `basic.ack` (the broker acks because *it* handled the
  message — by returning it). For **`Publish`** (single message) the
  confirm tracker correlates the two frames by `delivery-tag`: when an
  ack arrives for a `delivery-tag` that already saw a return, `Publish`
  resolves with `ErrUnroutable` rather than success. The `OnReturn`
  callback fires synchronously before `Publish` unblocks.
  **Ordering invariant (load-bearing, see §10 Rev 10).** This
  correlation is only correct if the return is *recorded* before the
  ack is *processed*. `amqp091-go` delivers returns and confirms on two
  **separate** Go channels, so the wire ordering (return-then-ack) is
  NOT preserved across them — a naïve two-goroutine drain, or a
  buffered return channel, lets the ack win the race and silently
  resolves an unroutable publish as success (~50% of the time under
  load). The library therefore demuxes both notify channels from a
  **single goroutine** and registers the `basic.return` channel
  **unbuffered**, so `amqp091-go`'s single reader blocks on the return
  send until `MarkReturned` runs and only then dispatches the ack.
  Buffering the return channel or splitting the drain across goroutines
  reintroduces the silent-loss race and is a breaking change to this
  contract. For
  **`PublishBatch`** with `Mandatory()`, multiple `basic.return` frames
  can arrive for different messages before any ack is processed; the
  library correlates each return to its delivery-tag via `MessageID`
  (a UUIDv7 auto-stamped by `applyDefaults` when `Message.MessageID`
  is empty). Per-message `ErrUnroutable` results are independent: a
  batch can have some slots succeed and others fail routing.
- **No double-acknowledgement guarantee.** AMQP `basic.ack` from
  broker → publisher means "the broker took responsibility" (queued
  for routing). It does *not* guarantee the message is persisted to
  disk on every queue's replica before a broker crash; users
  publishing to mirrored / quorum queues should still treat their
  consumers as idempotent (dedupe by `MessageID`).
- `PublishBatch` pipelines all N publishes on a **single channel**
  (preserving input order per RabbitMQ's per-channel ordering
  guarantee), waits up to `ConfirmTimeout` for the batch's confirms
  (any delivery-tag still outstanding when the window elapses resolves
  as `ErrConfirmTimeout` in its slot), returns per-message
  `PublishResult` and (if any failed) a non-nil error wrapping
  `ErrPartialBatch`. Per-message error values include
  `ErrPublishNacked`, `ErrUnroutable`, `ErrChannelClosed` (if the
  channel died mid-batch and unconfirmed publishes were not
  resolved), and `ErrInvalidMessage` (client-side validation).
- **`PublishBatch` supports `Mandatory()`.** When the publisher is
  configured with `Mandatory()`, each message in the batch is
  published with the mandatory flag set. Unroutable messages produce
  `ErrUnroutable` in their result slot (via the `MessageID`-based
  return correlation described above); routable messages succeed
  independently. `OnReturn` fires for each returned message, before
  the corresponding slot is resolved.
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
- **Per-message payload guardrail (`MaxMessageSizeBytes`).** Each
  `Publisher[M]` rejects encoded bodies larger than
  `MaxMessageSizeBytes` (**default 16 MiB**) with
  `ErrMessageTooLarge` (permanent) *before* opening a channel.
  This protects the publisher from OOM on accidentally-massive
  payloads and the broker from frame fragmentation pressure — the
  broker-side equivalent (reply code 311 `CONTENT_TOO_LARGE`) only
  fires after the payload has been allocated and partially sent,
  closing the channel and forcing reconnect. The metric outcome
  label is `too_large`. Pass `MaxMessageSizeBytes(0)` to disable
  the guard (discouraged). The cap applies per-message inside
  `PublishBatch` as well: a single oversized message in a batch
  fails that index with `ErrMessageTooLarge` without aborting the
  channel.
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
- **`WithPublishRateLimit(perSec)`.** A local token bucket that paces
  `Publish` to `perSec` messages per second, tolerating an initial burst
  of `perSec` then spacing the remainder evenly. It is a guardrail
  against an accidental runaway publish loop overwhelming the broker —
  not a substitute for broker-side flow control. A publish that cannot
  acquire a token before its context (the caller `ctx` or `PublishTimeout`)
  is cancelled returns `ErrRateLimited` (transient, wrapping `ctx.Err()`);
  a throttled-but-completed publish returns `nil`. Every throttled attempt
  increments `publisher_rate_limited_total{exchange}`. Each broker attempt
  acquires a token, so when `PublishRetry` is configured every retry of a
  single `Publish` paces against the bucket too. `PublishBatch` is **not**
  rate-limited, the same single-message scoping as `PublishRetry`.
  `perSec <= 0` (default) disables it. Last-wins.
- Publish errors are classifiable:
  `errors.Is(err, warren.ErrTransient)` indicates the caller may retry;
  `errors.Is(err, warren.ErrPermanent)` indicates the caller should not.

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

**Native `WithDedupe` middleware.** For the common case the library
offers `ConsumerBuilder.WithDedupe(store DedupeStore, ttl)`, which
abstracts the pattern above off the handler:

```go
type DedupeStore interface {
    Seen(ctx context.Context, id string) (bool, error)
    Mark(ctx context.Context, id string, ttl time.Duration) error
}
```

On the `Consume` path (not `ConsumeRaw`, which owns its acks) the
middleware, keyed by `MessageID`:

- asks `store.Seen(id)` before the handler — a hit **acks** the
  delivery without invoking the handler;
- on a miss, runs the handler and, only on a `nil` return, calls
  `store.Mark(id, ttl)` so future redeliveries are recognised — a
  handler **error is never marked**, so the message is reprocessed;
- **fails open**: any `Seen`/`Mark` error logs a sampled warning
  (MessageID never logged; store error reported by type only, §8) and
  processes the message anyway. The store is a best-effort cache, not
  a correctness gate — non-idempotent handlers must still self-guard.

Deliveries without a `MessageID` cannot be deduped and pass straight
to the handler. `store == nil` (the default) disables the middleware.
Size `ttl` exactly as the manual cache window above (15 minutes suits
most workloads). `DedupeStore` implementations must be safe for
concurrent use.

`Seen` runs *before* the handler under the per-delivery handler
context, so a configured `HandlerTimeout` bounds it. `Mark` runs
*after* a successful handler under a context **detached** from that
deadline (the handler's trace/span values are preserved, but
cancellation and the handler deadline are replaced with a fixed grace
bound): a near-exhausted `HandlerTimeout`, or a shutdown that already
cancelled the handler context, must not silently skip recording the
id, since that would fail open to a future duplicate. The grace bound
still caps `Mark` so a wedged store cannot block the dispatch goroutine
(and thus consumer shutdown) indefinitely. Keep store calls fast
regardless — a `Seen` slower than the handler budget, or a `Mark`
slower than the grace bound, still fails open.

**Operability.** Because fail-open intentionally hides a degraded store
from message flow, the only signal of a store outage is the sampled
fail-open warning (which carries a running `total:` count and the
failing op, never the `MessageID` or payload). Operators relying on
dedupe for side-effect suppression should **alert on the rate of that
warning** as an early indicator that the store is down and the consumer
has silently reverted to plain at-least-once.

### 6.3 Consumer

```go
type Consumer[M any] struct { /* … */ }

func ConsumerFor[M any](conn *Connection) *ConsumerBuilder[M]

func (c *Consumer[M]) Consume(ctx context.Context, h Handler[M]) error
func (c *Consumer[M]) ConsumeRaw(ctx context.Context, h RawHandler[M]) error
func (c *Consumer[M]) Pause(ctx context.Context) error  // local basic.cancel; stops broker delivery without closing the channel
func (c *Consumer[M]) Resume(ctx context.Context) error // re-issues basic.consume on the same channel
func (c *Consumer[M]) Health(ctx context.Context) (*ConsumerHealth, error) // (nil, err) when the connection is unhealthy
func (c *Consumer[M]) Close(ctx context.Context) error  // drains in-flight

// ConsumerHealth is a point-in-time runtime snapshot for liveness/readiness probes.
type ConsumerHealth struct {
	Active           bool      // started, loop not exited, not closed, not paused
	Paused           bool      // between Pause and Resume
	LastDeliveryAt   time.Time // receipt time of the most recent delivery (zero if none)
	InFlightHandlers int       // handler invocations currently executing
}

type Handler[M any]     func(ctx context.Context, msg M) error
type RawHandler[M any]  func(ctx context.Context, d *Delivery[M]) error

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
added in minor releases without breaking implementers. Because the fields are
unexported, only the library can build one: tests fabricate a fake delivery
with `warren.NewDeliveryFixture[M](warren.DeliveryFixture[M]{…})` (and a batch
with `NewBatchFixture`), which need no live broker and no third-party mock
library (GA-09). Both `DeliveryFixture`/`BatchFixture` are keyed-literal only
(an unexported guard field rejects positional literals), so new fields are
non-breaking.

`Ack`, `Nack`, and `AckIf` may return:
- `ErrChannelClosed` — the delivery's channel was closed before the
  ack/nack reached the broker (rare under healthy operation).
- `ErrAlreadyClosed` — the consumer was closed via `Close(ctx)`.
- `ErrAlreadyResolved` — a verdict was **already** emitted for this
  delivery (a no-op; see the double-verdict guard below).
- `nil` — broker received the ack/nack. Note that AMQP `basic.ack` /
  `basic.nack` are not confirmed by the broker; success here means
  "the frame was written to the socket", not "the broker recorded it".
  Network partition between write and broker-side processing can
  cause the message to be redelivered (`Redelivered() == true`).

**Double-verdict guard (resolved-once, single atomic CAS).** A
`Delivery[M]` resolves **exactly once**. The first of any
`Ack`/`Nack`/`AckIf`, or a `HandlerTimeout` verdict, wins a single
atomic compare-and-swap and emits the one wire frame; every later
verdict — including a late handler `Ack`/`Nack` after the timeout
already nacked, or a double `Delivery.Ack` via `ConsumeRaw` — **loses
the CAS and is a no-op returning `ErrAlreadyResolved`**, never a second
frame. This is **not** a check-then-act: the guard is the same atomic
CAS the `Batch[M]` verdict uses, so a timeout-verdict goroutine and a
handler-`Ack` goroutine racing on the same delivery still produce
exactly one frame. **Pre-fix** (before this guard) the second frame
caused the broker to close the channel with `406 PRECONDITION_FAILED`,
which took out **every** in-flight handler on that channel — collateral
loss, not merely a duplicate ack. The timeout verdict is routed through
the same guard (`nackOnTimeout`) so it cannot race the handler's own
ack on the `ConsumeRaw` path.

Handler error semantics:
- `nil` → Ack.
- any error → Nack, `requeue=false` (DLX if configured).
- `errors.Is(err, ErrRequeue)` → Nack, `requeue=true`.
- `errors.Is(err, ErrPoison)` → identical to default error semantically;
  exists for readability when "this is unambiguously bad input".

**Tracing on handler outcome.** The `<queue> process` span (created
by T28) terminates with an outcome attribute and an OTel status
matching the handler verdict. `nil` → `messaging.rabbitmq.outcome=ack`
+ `Status=Ok`; any error → `outcome` ∈
`nack_requeue|nack_no_requeue|max_redeliveries|timeout|handler_aborted_channel_closed`,
`Status=Error`, `Span.RecordError(err)`, and `error.type` set to the
sentinel name (e.g. `"ErrMaxRedeliveries"`). A poisoned message
therefore renders red in trace UIs and supports assertive alerts like
"spike of `error.type="ErrMaxRedeliveries"` over 5m". Full contract
in §6.9 "Tracing continuity post-mortem".

Builder:

```go
type ConsumerBuilder[M any] struct { /* … */ }

func (b *ConsumerBuilder[M]) Codec(codec.Codec) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Queue(name string) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Tag(consumerTag string) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Concurrency(n uint) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) Prefetch(count uint16) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) PrefetchBytes(bytes uint) *ConsumerBuilder[M] // no-op on RabbitMQ; preserved for protocol parity
func (b *ConsumerBuilder[M]) MaxInFlightBytes(n int64) *ConsumerBuilder[M] // local memory guardrail; caps sum of in-flight body sizes; n<=0 disables
func (b *ConsumerBuilder[M]) WithQueueDepthSampler(interval time.Duration) *ConsumerBuilder[M] // poll queue + "<queue>.dlq" depth via declare-passive; exports queue_depth{queue} and dlq_depth{dlq}; interval<=0 disables; interval<100ms clamped to 100ms; backs off (cap 30s) while a whole sample fails; series removed when the consumer stops
func (b *ConsumerBuilder[M]) ChannelQoS() *ConsumerBuilder[M]              // RabbitMQ: apply QoS per channel, not per consumer
func (b *ConsumerBuilder[M]) Priority(p int) *ConsumerBuilder[M]           // x-priority on basic.consume; higher = preferred
func (b *ConsumerBuilder[M]) HandlerTimeout(d time.Duration) *ConsumerBuilder[M] // per-message handler ctx deadline
func (b *ConsumerBuilder[M]) HandlerTimeoutVerdict(v TimeoutVerdict) *ConsumerBuilder[M] // default: TimeoutNackNoRequeue
func (b *ConsumerBuilder[M]) Exclusive() *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) AutoAck() *ConsumerBuilder[M]           // explicit opt-in; see warning below
func (b *ConsumerBuilder[M]) Args(Headers) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) OnCancel(func(tag string)) *ConsumerBuilder[M] // basic.cancel from broker; arg is the cancelled consumer tag (the frame carries no description). The consumer_cancelled_total metric label uses a bounded reason enum, not this tag — see §6.3
func (b *ConsumerBuilder[M]) MaxRedeliveries(n int) *ConsumerBuilder[M]
func (b *ConsumerBuilder[M]) WithDedupe(store DedupeStore, ttl time.Duration) *ConsumerBuilder[M] // native MessageID dedupe middleware (Consume path); fail-open; store==nil disables — see §6.2.1
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

**Queue Type Nuance:**
- For **Classic Queues**, the rules above apply directly.
- For **Quorum Queues**, RabbitMQ reads from disk and delivers in batches to the channel. A prefetch that is too small prevents the broker from batching efficiently. The absolute minimum for a quorum queue should be `16`, regardless of `Concurrency`, to allow the broker-side read-ahead to engage. The default `Prefetch=64` is well-suited for quorum queues under typical workloads.

The default `Prefetch=64, Concurrency=1` is conservative and fits
development workloads. For sustained > 1k msg/s/consumer against a
remote broker, raise both. Note the trade-off: a large `Prefetch`
means a consumer crash can leave many unacked messages, all of which
get redelivered (and may be processed twice — see §6.2.1).

`Prefetch` bounds in-flight *message count*; `MaxInFlightBytes` bounds
in-flight *memory*. With variable or large payloads,
`Prefetch × Concurrency × body-size` can exhaust the heap before the
count limit bites. `MaxInFlightBytes(n)` caps the sum of in-flight body
sizes: once running handlers hold `n` bytes the consumer stops pulling
deliveries (pausing prefetch refill) until a handler returns and frees
its bytes. `n<=0` (default) disables it. A single body larger than `n`
is not rejected — when nothing else is in flight it is dispatched alone,
so memory is bounded to `max(n, largest single body)`. The current
reserved total is exported as the `consumer_inflight_bytes{queue}` gauge.

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

**Non-blocking dispatcher:** The library implements a non-blocking
dispatcher pattern for concurrency. The AMQP channel reader loop
does not block waiting for a free handler goroutine slot; this
prevents head-of-line blocking on the AMQP connection and ensures
vital consumer-side broker frames (like `basic.cancel` and
channel-close notifications) are processed promptly even if all `n`
handlers are currently busy. (`basic.return` is a publisher-side
frame and never arrives on a consumer channel.)

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
warren.Queue{
    Name:          "orders.placed",
    Type:          warren.QueueTypeQuorum,
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

**`DeliveryLimit == 0` is broker-version dependent (load-bearing,
RMQ-06).** The zero value is a poison-loop footgun in **both** broker
families, but with **opposite** failure modes:

- **RabbitMQ 4.x:** a quorum queue with `DeliveryLimit == 0` is **not**
  unbounded — the broker applies a default `x-delivery-limit` of **20**,
  dead-lettering (or dropping, if no DLX is configured) on the 21st
  delivery. So "`DeliveryLimit:0` + `MaxRedeliveries(0)`", which reads
  as "unbounded on both sides", actually drops the message at 20
  broker-side — exactly the kind of silent drop §1 forbids.
- **RabbitMQ 3.13:** a quorum queue with `DeliveryLimit == 0` is
  **genuinely unbounded** — an unhandled poison message loops forever,
  the opposite failure mode.

`Topology.Declare` reads the broker version from the `connection.start`
server-properties and emits a **version-aware** warning for
`Type=QueueTypeQuorum && DeliveryLimit==0` (see §6.6): the 4.x default-20
silent-drop wording on 4.x, the unbounded-infinite-loop wording on 3.13,
and a combined warning when the version is unknown. To genuinely disable
the broker cap on 4.x set `DeliveryLimit` to a very large value
explicitly; always pair a quorum queue with a DLX so the dropped message
is preserved.

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

2. **Counter B — in-process, keyed by `MessageID`.** A per-channel map
   keyed by `(channel-instance-id, MessageID)` — falling back to
   `(channel-instance-id, consumer-tag, delivery-tag)` when
   `MessageID` is empty. The channel-instance-id is a UUID generated
   per consumer channel and reset on channel close, so delivery-tags
   reused after a reconnect cannot collide. Each `ErrRequeue`-driven
   `Nack(requeue=true)` increments the counter; once it would exceed
   `n`, the consumer rewrites the handler's verdict to
   `Nack(requeue=false)` and emits `ErrMaxRedeliveries` via metrics +
   log. The entry is deleted on `Ack` or `Nack(false)` so it does
   not leak across long-lived consumers. The increment is an **atomic
   read-modify-write**: the load→check→store/delete runs under a
   per-channel mutex so that, under `Concurrency(n>1)`, two handler
   goroutines processing redeliveries of the same key (at-least-once
   duplicates sharing a `MessageID`) cannot both read the old value
   and lose an increment. `go test -race` proves only memory-safety
   here, not lost-update freedom (a memory-safe `sync.Map` still loses
   updates across a non-atomic RMW); the guarantee is enforced by a
   behavioural N-goroutine-same-key test asserting the final count
   equals the number of increments.

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

**Interaction with RabbitMQ's own cycle detection.** Independently of
the counters above, RabbitMQ drops a dead-lettered message that would
cycle back to a queue already present in its `x-death` with the same
reason. So a DLX-back-to-source binding may see the broker discard a
message *before* counter A's `DeathCount()` reaches `MaxRedeliveries`;
do not assume the consumer always observes exactly `n` redeliveries
before the drop on cyclic topologies. This is a broker behaviour, not
a library one, and is surfaced (when observed) as `cause=x-death`.

#### `basic.cancel` (consumer cancellation notification)

The library always advertises the `consumer_cancel_notify=true`
client capability in `connection.start-ok` (as `amqp091-go`
already does), so the broker delivers a `basic.cancel` frame when
the queue is deleted under the consumer (or when an exclusive
consumer is forced off). The library:

- Increments `consumer_cancelled_total{queue, reason}` on every
  received `basic.cancel`. The `reason` label is a **bounded enum**
  (`queue_deleted` | `exclusive_revoked` | `unknown`), classified by
  probing whether the queue still exists — **not** the consumer tag,
  which is unbounded (`ctag-<uuidv7>`) and would explode label
  cardinality.
- Fires `OnCancel(tag)` if set; otherwise emits a warning log. The
  callback receives the **consumer tag** — the only datum the
  `basic.cancel` frame carries — where an unbounded value is harmless.
- **Does NOT auto-redeclare the queue** — the operator likely
  deleted it for a reason. The consumer goroutine returns from
  `Consume(ctx, …)` with `ErrConsumerCancelled` (wrapped with
  the consumer tag). Callers that want resilient redeclare-and-resume
  semantics wrap `Consume` in their own loop that also calls
  `Topology.Declare` first.

A leaked queue deletion (e.g. operator typo) is a real production
incident; a silently dying consumer is worse. Always wire `OnCancel`
in production code.

#### Draining API & liveness probes — `Pause` / `Resume` / `Health`

`Pause(ctx)` issues a **local** `basic.cancel` so the broker stops
delivering to this consumer, **without closing the channel**. In-flight
handlers and their acks on that channel are unaffected — RabbitMQ
flushes already-prefetched deliveries to the client before the
delivery stream ends — and subsequent messages stay on the queue. This
is the graceful-drain primitive for a Kubernetes `preStop` hook: pause,
let in-flight work finish, then `Close`. Unlike a broker-initiated
`basic.cancel`, a local pause does **not** surface `ErrConsumerCancelled`
and does **not** end `Consume`; the consumer goroutine parks until
`Resume`.

`Resume(ctx)` re-issues `basic.consume` on the **same** channel,
handing the running loop a fresh subscription. Both calls are
idempotent (a second `Pause` while paused, or `Resume` while not paused,
is a no-op) and both return `ErrInvalidOptions` before `Consume`/
`ConsumeRaw` has started and `ErrAlreadyClosed` after `Close`. The `ctx`
passed to `Pause`/`Resume` scopes **only that call** (its cancellation
aborts the in-progress handshake); the resulting subscription is bound to
the consumer's lifetime — the `ctx` given to `Consume`/`ConsumeRaw` — so
cancelling a request-scoped `Resume` ctx never silently stops delivery.
If a `Resume` ctx is cancelled mid-handshake (after the `basic.consume`
is issued but before the loop adopts it), `Resume` rolls the subscription
back with a local `basic.cancel` and stays paused, so the call is a clean
no-op-retry rather than leaving an orphaned broker subscription.

A TCP reconnect during a pause re-subscribes via the normal reconnect
barrier and **clears** the pause flag: the consumer is genuinely
consuming again, so `Health` reports `Active` and a subsequent `Resume`
is a harmless no-op rather than a duplicate `basic.consume`. The pause
flag is per-consumer in-process state and does not survive a reconnect —
re-call `Pause` if a drained state must persist across one.

`Health(ctx)` returns `(*ConsumerHealth, error)`. The pinned-connection
liveness check is the gate: on a connection error it returns
`(nil, err)`, since a zeroed snapshot alongside an error would mislead a
probe. A broker error from the liveness probe's channel open/close
round-trip is classified through the §6.8 reply-code sentinels, so a
probe may `errors.Is(err, ErrAccessRefused)` (etc.) to distinguish a
permission/precondition failure from a transient one. When the
connection is healthy it returns a populated `*ConsumerHealth`:

- `Active` — started, the consume loop has not exited, not closed, not
  paused (i.e. it is receiving deliveries). It flips to `false` when the
  loop exits for any reason — a ctx cancel, a broker `basic.cancel`
  (`ErrConsumerCancelled`), or a fatal subscribe error — so a probe wired
  to `Active` will not keep a silently-dead consumer in rotation.
- `Paused` — between `Pause` and `Resume`.
- `LastDeliveryAt` — receipt time of the most recent delivery (the zero
  `Time` if none yet). A `LastDeliveryAt` that stops advancing on a queue
  that should be busy is a liveness signal.
- `InFlightHandlers` — handler invocations currently executing.

`snapshot()` performs no I/O, so a readiness/liveness HTTP handler may
call `Health` concurrently with `Consume`.

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
  Wrap with `ErrRequeue` (`fmt.Errorf("retry: %w", warren.ErrRequeue)`)
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
    Type            string         // application-defined "kind" of the message (basic.properties.type, NOT x-queue-type)
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
    Delay           time.Duration  // requires rabbitmq_delayed_message_exchange plugin; NOT durable across node failure — see Delay note below
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
  - **Caveat — auth backends that rewrite the username.** The
    client-side check compares against the credential the client
    presented (URI/`WithAuth` user, or the cert principal under
    EXTERNAL). If the broker is configured with an auth backend that
    *maps* that principal to a different internal username (some LDAP /
    OAuth2 / `auth_backend_http` setups), the client-side value can
    disagree with the broker's notion of the user — producing a false
    `ErrInvalidMessage` reject, or letting a value through that the
    broker then 406s. In those deployments, leave `UserID` empty (the
    common case) so neither side stamps it.

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

- **`Delay` (durability warning — load-bearing).** `Delay` routes the
  message through a `rabbitmq_delayed_message_exchange` exchange
  (§6.6 `ExchangeDelayed`). **The plugin stores scheduled messages in
  a node-local Mnesia/ETS table that is NOT replicated and NOT a
  quorum/durable queue.** A publisher confirm for a delayed message
  means "the broker accepted it for scheduling", **not** "it will be
  delivered" — if the owning node fails before the delay elapses, the
  scheduled message is **lost silently**, even with `Durable` topology
  and confirms on. This is the one path in the library where a
  confirmed publish can still be lost, so it stands **outside the §1
  "no silent message loss" bar**. For delays that must survive node
  failure, prefer the durable pattern: publish to a normal durable
  (ideally quorum) queue with `x-message-ttl` and a DLX that routes
  the expired message onward — TTL + DLX is fully replicated. Use the
  delayed-message plugin only when occasional loss of a scheduled
  message is acceptable (e.g. best-effort retries, non-critical
  reminders).

`Persistent` is the zero-value default. Any user constructing a
`Message[M]{}` literally gets durable delivery without thinking. A
user who explicitly wants transient sets `DeliveryMode: DeliveryModeTransient`.
**Note on wire mapping:** The library maps its zero-value (`0`) to the AMQP
wire value `2` (persistent), and `DeliveryModeTransient` (`1`) to the wire
value `1`.

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
    ExchangeDelayed ExchangeKind = "x-delayed-message" // requires plugin; scheduled messages are NOT durable across node failure — see Message.Delay note (§6.5)
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
    // Zero leaves x-delivery-limit UNSET by this library, and its meaning
    // is BROKER-VERSION DEPENDENT (RMQ-06): on RabbitMQ 4.x a quorum queue
    // with no explicit limit receives a broker-applied DEFAULT delivery-limit
    // of 20 (the message dead-letters, or is dropped if no DLX, on the 21st
    // delivery — NOT unbounded); on RabbitMQ 3.13 the same zero value is
    // GENUINELY UNBOUNDED (an infinite poison loop), the opposite failure
    // mode. To truly disable the cap on 4.x, set a very large value
    // explicitly; to bound tighter, set DeliveryLimit > 0. Classic
    // queues ignore the argument entirely. See §6.3 poison-protection.
    // Topology.Declare reads the broker version and warns (version-aware)
    // when Type=QueueTypeQuorum && DeliveryLimit==0 so neither default is a
    // surprise.
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
// For quorum queues, it also implicitly sets x-dead-letter-strategy: at-least-once.
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
- **DLX preserves message context (load-bearing).** When a consumer
  `Nack`'s without requeue and the source queue has a DLX, RabbitMQ
  re-publishes the original message to the DLX preserving every
  `basic.properties` field and every header — including
  `traceparent`/`tracestate` so downstream observers can link the
  DLQ delivery span to the original producer's trace. The broker
  appends `x-death` to record the dead-letter event; it does not
  strip caller-supplied headers. The library never strips, rewrites,
  or normalises message headers on the consume path; any future
  code path that touches headers on consume MUST first update the
  §6.9 "Tracing continuity post-mortem" contract.
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

**Channel/connection ownership (relative to the §6.1 pool).** Because
`amq.rabbitmq.reply-to` is channel-scoped — replies are delivered only
on the channel that issued the `basic.consume` for the pseudo-queue —
a `Caller[Req,Resp]` does **not** acquire request-publish channels from
the rotating publisher channel pool. It holds **one dedicated channel**
(on one of the pool's TCP connections, pinned like a consumer) that
both consumes `amq.rabbitmq.reply-to` and publishes the requests, so
the reply routes back to it. Concurrent `Call`s share that channel and
are demultiplexed by `CorrelationID`. If that channel closes (reconnect),
in-flight calls resolve with `ErrChannelClosed`; the `Caller` reopens
and re-`basic.consume`s for subsequent calls. `UseExclusiveReplyQueue()`
swaps the pseudo-queue for a real exclusive auto-delete queue owned by
the same dedicated channel, which restores `Prefetch` and survives more
failure modes at the cost of one extra declare per `Caller`.

**Auto-population:** `Caller[Req,Resp]` automatically stamps a
`CorrelationID` (e.g., UUIDv7) and the appropriate `ReplyTo` address
on the request `Message[M]` if those fields are left empty by the
user.

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
    ErrNotConnected         = errors.New("warren: not connected")
    ErrAlreadyClosed        = errors.New("warren: already closed")
    ErrShutdown             = errors.New("warren: client is shutting down")
    ErrChannelClosed        = errors.New("warren: channel closed")
    ErrConnectionBlocked    = errors.New("warren: connection blocked by broker") // memory/disk alarm
    ErrChannelPoolExhausted = errors.New("warren: channel pool exhausted")        // all channels in-flight; transient

    // Publisher
    ErrConfirmTimeout = errors.New("warren: publisher confirm timeout")
    ErrUnroutable    = errors.New("warren: mandatory publish was returned")
    ErrPublishNacked = errors.New("warren: broker nacked publish")         // basic.nack from broker (e.g. overflow=reject-publish, disk alarm mid-publish)
    ErrPartialBatch  = errors.New("warren: batch publish partially failed")
    ErrBatchTooLarge   = errors.New("warren: PublishBatch exceeds max in-flight budget")
    ErrMessageTooLarge = errors.New("warren: message body exceeds MaxMessageSizeBytes") // local guard; permanent
    ErrRateLimited     = errors.New("warren: publish rate limited")                      // WithPublishRateLimit token unavailable before ctx cancel; wraps ctx.Err(); transient

    // Consumer
    ErrRequeue           = errors.New("warren: nack with requeue")
    ErrPoison            = errors.New("warren: poison message (nack no requeue)")
    ErrMaxRedeliveries   = errors.New("warren: max redeliveries exceeded")
    ErrConsumerCancelled = errors.New("warren: consumer cancelled by broker (basic.cancel)") // queue deleted, exclusive forced off, etc.

    // Codec / payload
    ErrInvalidMessage = errors.New("warren: invalid message payload")

    // Topology
    ErrTopologyMismatch        = errors.New("warren: topology mismatch")         // wraps ErrPreconditionFailed
    ErrTopologyRedeclareFailed = errors.New("warren: topology redeclare failed") // connection in degraded state; permanent until next successful redeclare

    // Reconnect lifecycle
    ErrReconnecting = errors.New("warren: connection reconnecting") // blocks Publish during the redeclare barrier; transient

    // RPC
    ErrCallTimeout = errors.New("warren: rpc call timed out")

    // Config
    ErrInvalidOptions = errors.New("warren: invalid options")

    // AMQP 0-9-1 reply codes — broker-originated errors. Any error from
    // a publish/consume/declare operation that traces back to a broker
    // channel/connection close wraps one of these so users can branch
    // on `errors.Is`.
    ErrContentTooLarge    = errors.New("warren: content too large (311)")
    ErrConnectionForced   = errors.New("warren: connection forced (320)")
    ErrInvalidPath        = errors.New("warren: invalid path (402)")
    ErrAccessRefused      = errors.New("warren: access refused (403)")
    ErrNotFound           = errors.New("warren: not found (404)")
    ErrResourceLocked     = errors.New("warren: resource locked (405)")
    ErrPreconditionFailed = errors.New("warren: precondition failed (406)")
    ErrFrameError         = errors.New("warren: frame error (501)")
    ErrSyntaxError        = errors.New("warren: syntax error (502)")
    ErrCommandInvalid     = errors.New("warren: command invalid (503)")
    ErrChannelError       = errors.New("warren: channel error (504)")
    ErrUnexpectedFrame    = errors.New("warren: unexpected frame (505)")
    ErrResourceError      = errors.New("warren: resource error (506)")
    ErrNotAllowed         = errors.New("warren: not allowed (530)")
    ErrNotImplemented     = errors.New("warren: not implemented (540)")
    ErrInternalError      = errors.New("warren: internal error (541)")

    // Retry classifiers (errors are wrapped with one of these for callers
    // that don't want to switch over the specific reply codes).
    ErrTransient = errors.New("warren: transient error")
    ErrPermanent = errors.New("warren: permanent error")
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
// ErrChannelClosed; ErrReconnecting; ErrRateLimited; AMQP codes 311, 320, 504, 541.
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
//
// Note on context.Canceled: NEVER transient — returns false even when
// the error also wraps a transient sentinel (e.g. ErrChannelPoolExhausted
// observed while the caller's ctx was cancelled mid-acquire). An upstream
// request cancellation fails identically on every retry, so PublishRetry
// must not loop on it. context.DeadlineExceeded is NOT special-cased — a
// timeout may succeed on a later attempt.
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
(`errors.Is(err, warren.ErrPermanent)`) or precisely
(`errors.Is(err, warren.ErrNotFound)`) — both work on the same error
value.

### 6.9 Subpackages

- **`codec`** — `Codec` interface (`Encode`, `Decode`, `ContentType`),
  built-in `JSON`, `Protobuf`, and **CloudEvents in both modes**:
  - `codec.NewJSON()` — **lax by default** (accepts unknown fields on
    `Decode`). The codec follows **Postel's Law**: conservative on send
    (`Encode` emits exactly the fields declared on `M`), liberal on receive
    (unknown fields on the wire are silently ignored). At billions/day across
    dozens of services, producer-first deploys — a v2 service publishing a
    new field alongside v1 services that have not yet rolled — must not
    poison v1 consumers' DLQs. Strict-default makes every such deploy a
    fire drill; lax-default makes them a non-event.
    `codec.NewJSONStrict()` is an opt-in variant that calls
    `json.Decoder.DisallowUnknownFields()` for the rare pipelines where
    consumer-side schema drift MUST be a hard error (regulated/compliance
    workloads).
    `codec.NewJSON(codec.WithUnknownFieldObserver(fn))` keeps the lax
    decode but calls `fn(path)` once per unknown top-level field, making
    the otherwise-silent drift observable without failing the message
    (T56). The canonical wiring increments a
    `codec_unknown_fields_total{type}` counter from `fn` so drift can be
    alerted on before a field becomes load-bearing. The observer fires
    only for struct targets, adds one extra `json.Unmarshal` pass per
    `Decode` (and only when set; the per-type known-field set is
    memoized, but the extra pass scans the full payload, so its cost
    scales with message size, not just schema size), does not report
    nested-object drift, and `fn` must be concurrency-safe.
    The `path` is attacker-controllable wire data, so `fn` must NOT use it
    as an unbounded metric label — label on the bounded message `{type}`,
    as the canonical wiring does, never on the field name.
  - `codec.NewProtobuf()` — proto3 binary; `ContentType` =
    `application/x-protobuf`.
    The built-in CloudEvents codecs operate on the canonical
    `cloudevents.Event` type from the official Go SDK
    (`github.com/cloudevents/sdk-go/v2`), re-exported as
    `codec.CloudEvent`. Using the upstream type and its JSON
    serialization keeps the wire format faithful to clients in other
    languages — see §10 decision "Codec interop is grounded in the
    canonical binding spec".
  - `codec.NewCloudEventsStructured()` — full CloudEvent JSON envelope
    is the payload body; content-type `application/cloudevents+json`.
    Serialization is delegated to the SDK's JSON event format, so
    `data` / `data_base64`, extensions, and `time` follow the spec
    exactly.
  - `codec.NewCloudEventsBinary()` — implements binary content mode of
    the **CloudEvents AMQP Protocol Binding**: the event `data` is the
    AMQP body, `datacontenttype` maps to the AMQP **content-type
    property**, and every other context attribute (and extension) maps
    to an AMQP header prefixed **`cloudEvents:`** (the official Go SDK
    default; RabbitMQ bridges 0-9-1 headers ⇄ AMQP 1.0
    application-properties, so a non-Go AMQP-1.0 CloudEvents client
    interoperates). `ContentType()` returns `""` because the per-event
    content type is supplied dynamically by `EncodeWithHeaders`.

  **Header-aware codecs (`HeaderCodec`).** A codec whose wire format
  spans the body, AMQP headers, and the content-type property
  implements the optional `HeaderCodec` interface (embeds `Codec`):

  ```go
  type HeaderCodec interface {
      Codec
      EncodeWithHeaders(v any) (body []byte, headers map[string]any, contentType string, err error)
      DecodeWithHeaders(body []byte, headers map[string]any, contentType string, v any) error
  }
  ```

  Publishers and consumers detect `HeaderCodec` by type assertion: on
  publish the returned headers are merged into `Message.Headers` and a
  non-empty `contentType` overrides `Message.ContentType`; on consume
  the delivery's headers and content-type property are passed to
  `DecodeWithHeaders`. A codec that does not implement `HeaderCodec`
  uses the plain `Encode`/`Decode` path unchanged.
  `NewCloudEventsBinary()` is the built-in `HeaderCodec`; its plain
  `Encode`/`Decode` reject use outside a header-aware
  publisher/consumer with `ErrInvalidMessage`, so the
  `cloudEvents:`-prefixed attributes can never be silently dropped.

  **Panic safety contract.** Every `Codec.Encode` / `Codec.Decode`
  (and `HeaderCodec.EncodeWithHeaders` / `DecodeWithHeaders`) call is
  wrapped by the Publisher / Consumer in a `defer recover` that
  converts a codec panic into `ErrInvalidMessage` (wrapping the
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
  patterns or message types. They are opted in **at metrics
  construction** — Prometheus fixes a vector's label set when the
  vector is created, so the choice cannot be a later builder option:

  ```go
  pm, _ := metrics.NewPrometheusPublisherMetrics(reg, nil,
      metrics.MetricsLabelRoutingKey, metrics.MetricsLabelMessageType)
  cm, _ := metrics.NewPrometheusConsumerMetrics(reg, nil,
      metrics.MetricsLabelMessageType)
  ```

  Scope of each label:
  - `MetricsLabelRoutingKey` adds `routing_key` to
    `publisher_publish_seconds`, carrying the publisher's configured
    routing key. It is accepted but **ignored** for consumer metrics:
    a consumer's per-delivery routing key is not a stable dimension
    and would explode cardinality.
  - `MetricsLabelMessageType` adds `message_type` to
    `publisher_publish_seconds` and `consumer_handler_seconds`,
    carrying the Go type name of the message type `M`.

  The library always threads these values to `RecordPublish` /
  `RecordHandler`; an implementation emits a label only when it was
  enabled at construction. `NoOp` and custom implementations that do
  not register the labels simply ignore the values.

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
  `consumer_inflight_bytes{queue}` (gauge of the current sum of
  in-flight message body sizes; rises while handlers run and returns to
  zero when they complete, independent of whether `MaxInFlightBytes`
  enforcement is enabled),
  `publisher_in_flight{exchange}`,
  `publisher_retry_total{exchange, reason}` (reason ∈
  `nacked|confirm_timeout|channel_closed|pool_exhausted|blocked|reconnecting|rate_limited|network`),
  `publisher_rate_limited_total{exchange}` (increments once per attempt
  throttled by the local `WithPublishRateLimit` token bucket — whether it
  then proceeded or its context was cancelled into `ErrRateLimited`; under
  `PublishRetry` each throttled retry of a single `Publish` counts),
  `replier_drop_no_dlx_total{queue}` (increments every time a
  `Replier` `OnError` fires for a request queue without a configured
  DLX — see §6.7),
  `queue_depth{queue}` and `dlq_depth{dlq}` (gauges of the broker-side
  message backlog of the consumer's source queue and its conventional
  `<queue>.dlq` dead-letter queue, sampled via `declare-passive` only
  when `WithQueueDepthSampler(interval)` is enabled; a queue that does
  not exist is skipped, not reported as zero; the series is removed when
  the consumer stops so a process cycling through queue names does not
  accumulate stale frozen series — see §6.3),
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
  `tracestate` keys; CloudEvents binary-mode `cloudEvents:`-prefixed
  headers do not conflict.

  **Tracing continuity post-mortem (load-bearing).** The library treats
  dead-lettered and poisoned messages as first-class trace artefacts so
  on-call engineers can pivot from a DLQ entry to the producing trace
  in Jaeger/Datadog without rebuilding context by hand.

  - **Publisher side.** `Publish` injects `traceparent` and
    `tracestate` into `Message[M].Headers` via `otel.Propagator`
    *before* any frame is written; these headers travel as part of
    `basic.properties.headers`. Headers explicitly set by the caller
    win (last-wins), so callers can override propagation in tests
    without the library silently overwriting.
  - **Broker side (DLX path).** When a consumer `Nack(requeue=false)`s
    a message and the source queue has a configured DLX, RabbitMQ
    re-publishes the message to the DLX preserving every original
    `basic.properties` field *and* every header — including
    `traceparent` / `tracestate`. The broker appends an `x-death`
    field-array describing the dead-letter event; it does not strip
    or rewrite caller-supplied headers. The library never strips,
    rewrites, or normalises message headers on the consume path
    either, so a downstream consumer reading from the DLQ extracts
    the *original* trace context and links its own span to the
    original producer's trace. Documented as a contract — any
    future code path that mutates headers on consume MUST surface
    the change in this section first.
  - **Consumer span lifecycle on poison.** When the handler returns
    a non-nil error (including `ErrRequeue`, `ErrPoison`, an
    `errors.Is(ErrMaxRedeliveries)` escalation, or a `HandlerTimeout`
    verdict), the `<queue> process` span:
    1. Records the error via `Span.RecordError(err)`.
    2. Sets `messaging.rabbitmq.outcome` to one of `ack`,
       `nack_requeue`, `nack_no_requeue` (poison), `max_redeliveries`,
       `timeout`, `handler_aborted_channel_closed`.
    3. Sets a non-`Unset` status: `Ok` on `nil`, `Error` on every
       failure class above (so trace UIs render the span red).
    4. Sets `error.type` to the sentinel name (e.g. `"ErrRequeue"`,
       `"ErrPoison"`, `"ErrMaxRedeliveries"`) for assertive
       alerting and aggregation. Alerts like "spike of
       `error.type="ErrMaxRedeliveries"` over 5m" become trivial.

    The span is `End()`ed in every termination path (including
    panics — wrapped in `recover` per the codec panic-safety
    contract) so there are never open spans after a handler dies.
  - **BatchConsumer Links.** A `BatchConsumer[M]` processes multiple
    messages at once, each potentially carrying a different `traceparent`.
    Instead of adopting a single parent context, the `<queue> process_batch`
    span adds OTel **Links** pointing to the `traceparent` of every message
    in the batch. This models the fan-in correctly.
  - **Publisher span lifecycle on failure.** `Publish` mirrors the
    consumer contract for the `<exchange> publish` span: every
    `ErrUnroutable`, `ErrConfirmTimeout`, `ErrPublishNacked`,
    `ErrMessageTooLarge`, encode error, and channel-pool exhaustion
    is recorded via `RecordError` with `error.type` set to the
    sentinel name and span status `Error`. `messaging.rabbitmq.outcome`
    mirrors the metric `publisher_publish_seconds{outcome}` label
    (`ack`/`nack`/`return`/`timeout`/`too_large`/`pool_exhausted`/
    `blocked`/`error`).
  - **Why this matters.** Without the outcome attribute and error
    status, a poisoned message looks identical to a successful one
    in trace UIs — the consumer span ends "successfully" because
    the handler returned a value, even though the value was an
    error. Operators lose the ability to filter traces by "show me
    only the messages that hit the DLX in the last hour", which is
    the most common debug query during incidents.

- **Test fixtures** — the root package exports `NewDeliveryFixture[M]` /
  `NewBatchFixture[M]` (keyed-literal `DeliveryFixture`/`BatchFixture` inputs)
  to fabricate concrete `Delivery[M]`/`Batch[M]` values in tests without a live
  broker (GA-09). The library ships **no mock package** — `Delivery`/`Batch`
  are the only doubles a consumer cannot build itself (unexported fields), so
  they are the only ones warren provides; interface mocks are the consumer's to
  generate with whatever tool they prefer.

- **`internal/amqptest`** — internal testcontainers-go helper (`NewRabbitMQ(t, opts…)`
  → `URI()`/`AMQPSURI()`/`Container()`/`Cleanup(t)`) that spins up a
  RabbitMQ node with the `rabbitmq_delayed_message_exchange` plugin
  and `rabbitmq_auth_mechanism_ssl` plugin enabled, plus
  pre-generated TLS server/client certificates under
  `internal/amqptest/certs/` for `amqps://` integration tests. TLS is
  opt-in via `WithTLS()` because the underlying RabbitMQ module
  configures TLS as `listeners.tcp = none` (TLS-only), which would
  otherwise break the common plain-AMQP fixture; options are
  `WithRabbitMQVersion`, `WithEnabledPlugins`, `WithExtraConfig`,
  `WithTLS`. It lives under `internal/` — warren's own integration
  suite uses it; it is not part of the public API and never reaches a
  consumer's build.

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
| Cluster       | `cluster`       | `*_cluster_test.go`       | On-demand / nightly — **never a per-PR gate** (Rev 23 D5) |

### Frameworks

- stdlib `testing` for execution.
- `github.com/stretchr/testify` for `assert`/`require`.
- `github.com/testcontainers/testcontainers-go` + the official
  RabbitMQ module (in `internal/amqptest`); pinned image tags for
  3.13 LTS and 4.x.
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
- No message loss with confirms over a **5-minute outage at 10k
  msg/s** (the nightly/release-candidate intensity; CI runs a scaled
  duration via `WARREN_CHAOS_DURATION`). "No loss" is measured as the
  **published-set minus the consumed-set, deduplicated by
  `MessageID`** — at-least-once **duplicates** (produced by
  `PublishRetry` and the reconnect barrier) are tolerated; only a
  message that is never consumed counts as loss. The loss accounting
  carries a VG-6 injected-drop self-test so a green run cannot mean
  "the harness can't see loss".
- No goroutine leak (`goleak.VerifyNone(t)`).
- Topology re-declared on reconnect when `AttachTo` is used.

### Poison-loop test

Integration test that pushes a message that always errors:
- Default config: 1 redelivery max (immediate Nack-no-requeue).
- `MaxRedeliveries(3)` + handler wrapping `ErrRequeue`: 3 attempts
  then permanent Nack via `ErrMaxRedeliveries`.

### Conformance

Conformance tests assert wire-protocol behaviour against the AMQP
0-9-1 spec (confirm ordering, broker-nack on
`x-overflow=reject-publish`, the quorum `x-delivery-limit` poison
bound, `basic.cancel` notifications, and the mandatory return/ack
correlation). For v0.1 they are **real-broker-only** (Lens-10 TV-06):
a test AMQP server stub cannot prove these contracts — they are
emergent broker behaviours (R10-3 ordering, R10-2 quorum limit,
`x-death` tokens, broker-nack, `basic.cancel`, 406-on-`UserID`,
direct-reply-to), not frame-encoding details. They live in the
`conformance/` package behind the `conformance` build tag and run via
`make test-conformance` against `AMQP_TEST_URL` (Docker required).

### Memory and concurrency

- All tests run with `-race`.
- `goleak` checks at end of every test that starts goroutines.
- Nightly **1000-cycle** stress loop of connect/disconnect +
  confirm churn to surface goroutine leaks under realistic timing
  (`goleak.VerifyNone`); CI runs a smaller cycle count, dialed up to
  1000 via `WARREN_CHAOS_CHURN_CYCLES`.

### Cluster reliability lane

The half of the §9 reliability claims a single-node broker cannot prove
— quorum leader failover, SingleActiveConsumer failover on real node
death, multi-node `WithAddrs` rotation, partition handling, and
mixed-version rolling upgrade — runs on a dedicated **`cluster`
build-tag lane** against a real multi-node cluster. It is **on-demand /
nightly, never a required per-PR gate** (Rev 23 D5); the single-node
`integration` lane stays the per-PR gate, and the **standing** dedicated
multi-node environment is deferred to **LATER-49**.

**Harness.** `test/docker-compose.cluster.yml` (`make cluster-up`) stands
a **3-node quorum cluster** — pinned `rabbitmq:3.13.7-management`,
`default_queue_type = quorum`, `cluster_partition_handling =
pause_minority` (autoheal is intentionally unset so the pause is
observable), `queue_leader_locator = client-local` (pinned, so leader
placement is deterministic across the mixed-version member whose 4.x
default is `balanced`) — plus a **Toxiproxy sidecar** hosting one proxy
per node, each fronting that node's AMQP port. Tests dial the
Toxiproxy-fronted ports so the control toolkit can cut/heal a node's
client connectivity transparently. The lane talks to the externally
provisioned cluster via `WARREN_CLUSTER_NODES` / `WARREN_CLUSTER_MGMT` /
`WARREN_TOXIPROXY_URL`; each helper **fails (does not skip)** when its
variable is unset, mirroring the `integration` lane's `AMQP_TEST_URL`
rule, and `make test-cluster` carries a **zero-run guard** that fails if
no `*_cluster` test executed. A single member (`rmq2`) is image-swappable
via **`WARREN_RMQ2_IMAGE`** (default homogeneous 3.13.7) to form a
mixed-version cluster the rolling-upgrade campaign asserts continuity
across — for **feature-flag-compatible** version pairs (e.g. two 3.13
patches; validated live with 3.13.7 + 3.13.6). A fresh **4.x** node will
**not** join a 3.13 cluster via peer discovery (`incompatible_feature_flags`,
confirmed live) and runs standalone; a genuine 3.13→4.x **major** rolling
upgrade is an in-place, data-preserving image swap of an existing member,
which needs persistent volumes the lane does not yet provision (LATER-88).

**Version matrix.** The scheduled workflow (`.github/workflows/cluster.yml`
— schedule + `workflow_dispatch`, standalone until T151's general nightly
workflow consolidates it) runs the **whole** lane over a **homogeneous
RabbitMQ version matrix**: every node is set to the same image via
**`WARREN_RMQ_IMAGE`**, once on the **3.13 LTS** most estates run and once on
the **4.x** line whose quorum/Khepri behaviour the campaigns assert. Matrix
legs are independent (`fail-fast: false`) so a 3.13 regression cannot mask a
4.x one. This is the cluster-lane analogue of T151/TV-05's single-node
`integration` version matrix. Distinguish the two image knobs:
`WARREN_RMQ_IMAGE` sets **all three** nodes (the homogeneous matrix axis;
all campaigns), while `WARREN_RMQ2_IMAGE` overrides only **`rmq2`** (the
opt-in mixed-version member the rolling-upgrade campaign asserts continuity
across, feature-flag-compatible only). The campaigns are validated to pass
on a homogeneous 4.x cluster before the axis is wired.

**Fault injection — two distinct mechanisms, used honestly.** Toxiproxy
fronts only the **AMQP client ports** (5672), so disabling a proxy severs
**clients** but leaves inter-node Erlang distribution intact — it can
never make a node a minority and so **cannot** trigger `pause_minority`.
Accordingly:
- **Client-connectivity** faults (the confirm-latency partition-tail
  campaign) use the Toxiproxy cut/heal.
- A **real broker-to-broker partition** (the pause_minority campaign)
  is injected by disconnecting a node from the cluster's Docker network
  (`docker network disconnect`, `amqptest.PartitionNode`), which severs
  both client and inter-node links so the isolated node becomes a
  minority and pauses.
- **Node death / restart** use `docker kill` (crash) and `docker
  restart` (graceful rolling restart).

**Zero loss** is measured everywhere as the **published-set minus the
consumed-set, deduplicated by `MessageID`** (the TV-09 method the
reconnect chaos test self-tests with an injected drop): at-least-once
duplicates from the reconnect barrier and `PublishRetry` are tolerated;
only a never-consumed confirmed message counts as loss.

**Per-campaign topology labels** — what each campaign asserts, the fault
it injects, and the cluster property a single node cannot give:

| Campaign (file)                         | Topology / fault                                   | Cluster-only property proven |
| --------------------------------------- | -------------------------------------------------- | ---------------------------- |
| Dial smoke (`_smoke`)                   | 3-node quorum; no fault                            | dial across 3 nodes → `Health` green, end to end |
| Control toolkit (`_control`)            | quorum queue; `docker kill` the leader's node      | management API reports a **new** Raft leader |
| Quorum leader failover (`_quorum_failover`) | quorum queue, leader pinned to a killable node; kill under load | zero loss across a **Raft re-election** |
| SAC failover (`_sac_failover`)          | SingleActiveConsumer `Prefetch(1)`; kill active node | broker promotes the standby, publish-order == handler-order |
| SAC multi in-flight (`_sac_failover_multi`) | SAC `Prefetch(N>1)`, gated handler; kill active node | the whole **multi-message** unacked set redelivered to the standby in order |
| WithAddrs rotation (`_failover_rotation`) | N connections over 3 addrs; full-cluster restart  | reconnections **re-spread** — no `addr[0]` stampede |
| Confirm latency (`_confirm_latency`)    | quorum queue under load; baseline + Toxiproxy cut/heal tail | majority-commit confirm latency tail **not clipped** to `+Inf` |
| Reconnect storm (`_reconnect_storm`)    | 3+3 pool; repeated `ForceReconnect` waves under load | no stampede + zero loss + prefetch↔redelivery exercised |
| Partition under load (`_partition`)     | quorum queue under load; **Docker-network** partition of a follower | `pause_minority` isolates the minority (drops from the quorum's `online` set), majority surfaces **classifiable errors** not silent stalls, zero loss + recovery on heal |
| Rolling upgrade (`_rolling_upgrade`)    | quorum queue under load; rolling `docker restart` of each node (opt-in mixed-version, feature-flag-compatible — LATER-88 for the in-place major jump) | continuity — load keeps confirming, consumers resubscribe, zero loss across every restart |

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
- Ship no mock library; the testcontainers broker fixture stays in
  `internal/amqptest`, so no test infrastructure reaches a consumer's
  build.
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
- **Be liberal in what you receive (Postel's Law).** Default codecs
  tolerate forward-compatible payloads (unknown fields on `Decode` are
  ignored, not rejected). Producer-first deploys must not poison v1
  consumers' DLQs. Strict modes (`codec.NewJSONStrict`) are opt-in for
  the rare pipelines where consumer-side schema drift must be a hard
  error.
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
  publish, mocks in root, single-connection abstraction, strict JSON
  default that breaks producer-first deploys, swallowing broker nack,
  missing re-subscribe).
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
      and still supported by the Go team from 1.25 onward** (currently
      1.25 and 1.26; nightly when a new minor lands).
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
- [ ] **Return/ack ordering invariant (T59, R10-3/RMQ-16/TV-02).** The
      `basic.return`-before-`basic.ack` ordering that `ErrUnroutable`
      depends on is locked deterministically by a unit test
      (`startConfirmDemux` registers an **unbuffered** return channel and
      a single demux goroutine). The real-broker assertion —
      concurrent unroutable-mandatory publishes during confirm load — is
      **flaky-prone by design** (it exercises a ~50%-under-load timing
      race against amqp091-go's two-channel notify dispatch that a mock
      tracker cannot reproduce) and runs on the **nightly trigger**
      (T151) at high iteration/concurrency, **not** as a per-PR gate.
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
- [ ] `codec.NewCloudEventsStructured()` and
      `codec.NewCloudEventsBinary()` support both modes per the
      CloudEvents AMQP Protocol Binding spec; the binary codec
      implements `HeaderCodec` so `cloudEvents:`-prefixed attributes
      round-trip through AMQP headers and `datacontenttype` through the
      content-type property. `codec.NewJSON()` is lax by default
      (Postel's Law — accepts unknown fields on `Decode`);
      `codec.NewJSONStrict()` is the opt-in `DisallowUnknownFields` mode.
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

4. **Codec interop is grounded in the canonical binding spec.** Codec
   and wire-format decisions — especially for cross-ecosystem formats
   like CloudEvents — are made to interoperate with **non-Go (or
   non-warren) clients**, not for warren↔warren convenience. Built-in
   codecs follow the authoritative binding/format spec, and an
   **official upstream library is preferred over a hand-rolled mapping**
   when it improves fidelity. Concretely, the CloudEvents codecs use the
   official Go SDK's `cloudevents.Event` type and JSON event format, and
   the binary codec implements the **CloudEvents AMQP Protocol Binding**:
   structured mode puts the full `application/cloudevents+json` envelope
   in the body; binary mode puts `data` in the body, maps
   `datacontenttype` to the AMQP content-type property, and maps every
   other attribute (and extension) to a `cloudEvents:`-prefixed AMQP
   header (the SDK default — RabbitMQ bridges 0-9-1 headers ⇄ AMQP 1.0
   application-properties). Any deliberate divergence from the canonical
   binding MUST be called out and justified by an interop need.

5. **Versioning: first tag is `v0.1.0`**, stabilising to `v1.0.0` once
   all success criteria are checked.

6. **CI Go matrix: every push runs the full matrix of every Go minor
   version still supported by the Go team, starting at 1.25.**
   Currently that means 1.25 and 1.26; widens automatically as 1.27+
   land. No "only minimum on push, matrix nightly" split. The floor
   moved from 1.23 to 1.25 when the `golang.org/x/sys` bump to v0.44.0
   (the GO-2026-5024 fix, LATER-63) raised the minimum toolchain.

7. **`internal/` is strictly internal.** No re-export from the root
   package. If a type is genuinely needed by external callers, it
   lives outside `internal/` from the start.

8. **`PublishBatch` always publishes all N** and returns
   `[]PublishResult` (one slot per input). On any failure the function
   error wraps `ErrPartialBatch`; per-message detail is in
   `PublishResult.Err`. No short-circuit.

9. **`Delivery[M]` and `Batch[M]` are concrete structs**, not
   interfaces. Methods can be added in minor releases without
   breaking implementers. Because their fields are unexported, only
   the library can fabricate one, so warren exports the fixture path
   `warren.NewDeliveryFixture[M]` / `warren.NewBatchFixture[M]`
   (keyed-literal `DeliveryFixture`/`BatchFixture` inputs with an
   unexported guard field) — no live broker, no third-party mock
   library (GA-09). No mock package is shipped: interface doubles a
   consumer can build itself are the consumer's to generate.

52. **Quorum Queue `x-dead-letter-strategy: at-least-once` (Rev 9)** —
    Implicitly injected by `Topology.Declare` for Quorum queues with DLXs
    to guarantee message preservation during dead-lettering, removing the
    need for the user to specify it manually.

53. **Strict Shutdown Cascade (Rev 9)** — `Close(ctx)` cancels consumers
    *before* draining publishes, and drains publishes *before* closing
    sockets. The order is load-bearing.

54. **BatchConsumer OTel Links (Rev 9)** — A single batch span receives
    `Link` entries for every incoming message's `traceparent` rather than
    attempting to adopt a single parent.

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
    publishes. For `Publish` (single message), the confirm tracker
    correlates the two frames by `delivery-tag` and resolves the
    publish as `ErrUnroutable` rather than success, with `OnReturn`
    firing synchronously before `Publish` unblocks. For
    `PublishBatch`, correlation is done via `MessageID` rather than
    `delivery-tag` because multiple `basic.return` frames for
    different batch messages can arrive before the corresponding acks;
    a single `delivery-tag` slot cannot distinguish them. The library
    stores a `MessageID → delivery-tag` map per channel entry before
    each batch publish; the return goroutine looks up and removes the
    entry on each `basic.return`. `Message.MessageID` is
    auto-populated (UUIDv7) when left empty, so every message always
    has a unique key without caller effort.

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
    `amqp091-go` serializes I/O per `warren.Connection`: one TCP
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

23. **`codec.NewJSON()` is lax by default (Postel's Law).** Earlier
    revisions made the default strict (`DisallowUnknownFields`) to
    surface schema drift; SRE review (2026-05-22) inverted the
    decision after the on-call evidence: at billions/day across
    dozens of services, the realistic failure mode is a v2 producer
    deploying a new field while v1 consumers are still rolling. A
    strict default turns every producer-first deploy into a
    DLQ-poisoning fire drill that wakes the on-call. The library
    now follows Postel's Law — conservative on send, liberal on
    receive — and ships `codec.NewJSONStrict()` for the minority
    of pipelines (regulated/compliance) where consumer-side drift
    must be a hard error. Every codec call is still wrapped in
    `recover` — codec panics become `ErrInvalidMessage`, never
    crash the publisher/consumer goroutine.

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
    `OnCancel(tag)` fires (carrying the unbounded consumer tag) and
    `Consume` returns `ErrConsumerCancelled` so the consumer goroutine
    is never silently dead. The metric `consumer_cancelled_total{queue,
    reason}` is mandatory; its `reason` label is the bounded enum
    `queue_deleted | exclusive_revoked | unknown` (never the consumer
    tag — see §6.3).

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

50. **Local payload guardrail (`MaxMessageSizeBytes`) with 16 MiB
    default.** SRE on-call review (2026-05-22): without a local cap,
    an accidental 100 MiB payload allocates frame buffers in the
    publisher, fragments into AMQP frames, partially streams to the
    broker, and is then rejected with `CONTENT_TOO_LARGE` (311)
    — closing the channel and forcing a reconnect. The new
    `PublisherBuilder.MaxMessageSizeBytes(n)` rejects the publish
    locally with `ErrMessageTooLarge` (permanent) before any
    channel is acquired. Default 16 MiB (matches typical 128 KiB
    frame-max × ~128 frames); `n=0` disables the guard explicitly;
    `n<0` fails `Build` with `ErrInvalidOptions`. Mandatory metric:
    `publisher_publish_seconds{exchange, outcome="too_large"}`.

51. **Tracing continuity post-mortem.** SRE on-call review
    (2026-05-22): a dead-lettered message that loses its trace
    context is a debug black hole — Jaeger/Datadog show a producer
    span with no downstream link, and the DLQ entry shows no
    upstream link. The library treats trace continuity through DLX
    as a contract, not a side effect:
    1. **Publisher injects `traceparent`/`tracestate` before any
       frame is written** so the propagated context is part of
       every `basic.publish`.
    2. **The library never strips or rewrites message headers on
       consume.** RabbitMQ's DLX path preserves the original
       headers verbatim while appending `x-death`, so the DLQ
       consumer extracts the original trace context — but only if
       the library does not mutate headers in between. Documented
       as a load-bearing invariant of §6.6 and §6.9.
    3. **Consumer span is terminated with an outcome attribute and
       an `Error` status whenever the handler does not return
       `nil`.** `messaging.rabbitmq.outcome` carries the verdict
       (`ack`/`nack_requeue`/`nack_no_requeue`/`max_redeliveries`/
       `timeout`/`handler_aborted_channel_closed`); `error.type`
       carries the sentinel name. Poisoned messages render red and
       support assertive alerts like "spike of
       `error.type="ErrMaxRedeliveries"`". Publisher span mirrors
       the contract for `ErrUnroutable`/`ErrConfirmTimeout`/
       `ErrPublishNacked`/`ErrMessageTooLarge`.

    T27/T28 acceptance criteria carry the verbatim expected
    attributes and status codes. Reverting any of the three is a
    breaking change to the tracing contract and requires a SPEC
    amendment.

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

### Rev 10 — AMQP/SRE specialist re-review (2026-05-25)

A fresh "act as a battle-hardened AMQP091/RabbitMQ/SRE specialist"
pass over the whole spec surfaced four correctness/factual defects and
a family of reliability-bar gaps that survived Rev 6/9. The
**corrections (R10-1..R10-4) are applied inline in this SPEC**; the
**behaviour changes (R10-5..R10-17) are tracked as Phase 11 tasks
T57–T72 in `tasks/plan.md`** and amend the relevant section when each
lands; **R10-18 is deferred to `LATER.md` (LATER-34)** because it is an
architecture decision, not a defect. Reopening any of these needs a
fresh spec amendment.

**Corrections applied in this revision:**

- **R10-1 — `Message.Delay` / `ExchangeDelayed` are NOT durable across
  node failure.** The `rabbitmq_delayed_message_exchange` plugin stores
  scheduled messages in a node-local, non-replicated Mnesia/ETS table.
  A confirm means "accepted for scheduling", not "will be delivered";
  losing the owning node before the delay elapses loses the message
  silently — so delayed messages stand *outside* the §1 no-loss bar.
  §6.5/§6.6 now carry the warning and recommend durable-queue + TTL +
  DLX for delays that must survive node failure. Godoc + an optional
  declare-time warning are tracked by **T57**.
- **R10-2 — Quorum `DeliveryLimit == 0` is not unbounded on RabbitMQ
  4.x.** The broker applies a **default delivery-limit of 20**; the old
  "Zero = unbounded" wording was factually wrong and made
  "`DeliveryLimit:0` + `MaxRedeliveries(0)`" a silent drop at the 21st
  delivery. §6.3/§6.6 corrected. `Topology.validate()` warning for
  `Quorum && DeliveryLimit==0` is tracked by **T58**.
- **R10-3 — Return/ack correlation depends on a load-bearing ordering
  invariant.** `amqp091-go` delivers returns and confirms on two
  separate channels; a buffered return channel or a two-goroutine drain
  lets the ack win the race and resolves an unroutable mandatory publish
  as success. The library demuxes from a single goroutine with an
  **unbuffered** return channel. §6.2 now states this as a contract; a
  regression test that locks it is tracked by **T59**.
- **R10-4 — Minor protocol/clarity fixes (applied):** `PublishBatch`
  confirm window is bounded by `ConfirmTimeout` (§6.2);
  `basic.return` is publisher-side, not a consumer dispatcher frame
  (§6.3); `UserID` client-side check caveat for username-rewriting auth
  backends (§6.5); TLS/`client_properties` hygiene note (§6.1);
  `Caller` RPC channel/connection ownership relative to the pool
  (§6.7); RabbitMQ native dead-letter cycle-detection interaction with
  the two-counter design (§6.3).

**Behaviour changes (Phase 11 — amend SPEC on landing):**

- **R10-5 — Single-delivery double-verdict idempotency guard (T60).**
  A handler-timeout verdict followed by a late handler `Ack`/`Nack`
  (or any double call on `Delivery.Ack/Nack/AckIf` via `ConsumeRaw`)
  must be a no-op, never a second frame: a duplicate ack/nack is a
  channel-closing `PRECONDITION_FAILED` that takes down every in-flight
  handler on that channel. `Delivery[M]` gains an idempotent
  resolved-once guard mirroring `Batch[M]`.
- **R10-6 — Channel-level recovery, distinct from TCP reconnect (T61).**
  A 404/406/ack-error closes only the channel while the TCP connection
  stays up; the supervisor watches the connection, so the consumer can
  die silently on its closed channel. The consumer must observe its own
  channel close and reopen + re-`basic.qos` + re-`basic.consume`,
  incrementing `consumer_resubscribed_total`, without waiting for a TCP
  reconnect.
- **R10-7 — Reconnect topology-redeclare de-amplification (T62).**
  `AttachTo` registers the redeclare hook on every pooled connection
  (`topology.go`), so a broker restart triggers N×(pool size) declares
  of broker-global topology against a just-recovered broker. Redeclare
  topology once per recovery event at the `*Connection` level; keep
  only `basic.consume`/`basic.qos` reissue per consumer connection.
- **R10-8 — Reconnect barrier max-duration cap (T63).** The synchronous
  redeclare barrier has no upper bound; a "half-alive" broker (accepts
  the socket, stalls on `queue.declare`) plus the default
  `PublishTimeout=0` and `context.Background()` stalls publishers
  indefinitely — the very silent-stall mode `ConfirmTimeout=30s` was
  added to prevent. Bound the barrier and surface `ErrReconnecting`
  when the cap is hit.
- **R10-9 — Quorum-queue structural validation + `MaxPriority` fix
  (T64).** `Topology.validate()` validates streams but not quorum
  queues, which the broker requires to be `Durable` and forbids from
  being `Exclusive`/`AutoDelete` or carrying `x-max-priority`. Reject
  these locally as `ErrInvalidOptions`. Also reconcile the validation's
  reference to "MaxPriority" (no such struct field) to check
  `Args["x-max-priority"]`.
- **R10-10 — DLQ durability/bounds + Consumer missing-DLX parity
  (T65).** The auto-declared `<source>.dlq` has unspecified
  type/durability and no bounds — an unbounded DLQ fills disk and
  triggers a broker-wide disk alarm. Declare it durable (quorum-capable)
  with configurable bounds. And mirror the `Replier` missing-DLX
  validation on the `Consumer`: when `MaxRedeliveries>0` and the wired
  `Topology` has no DLX for the source queue, warn at `Build` and emit
  a drop metric (poison drops are currently silent there).
- **R10-11 — `WithAddrs` shuffle + reconnect rotation (T66).** Cluster
  failover must shuffle the address list per connection and rotate on
  reconnect, or every client stampedes the same node on recovery.
- **R10-12 — `Dial` partial-pool-connect policy (T67).** Define and
  implement what happens when some of the 2+2 pooled connections fail
  at boot (succeed-if-≥1-per-role with supervised reconnect of the
  rest, vs fail-fast) — currently unspecified.
- **R10-13 — Alternate-exchange support (T68).** Expose
  `x-alternate-exchange` so unroutable messages have a server-side
  catch-all, complementing per-publish `Mandatory()`+`OnReturn`.
- **R10-14 — Exchange-to-exchange bindings (T69).** `Binding` only
  binds exchange→queue; add exchange→exchange (`exchange.bind`) for
  layered fan-out topologies (a real RabbitMQ extension).
- **R10-15 — Graceful-shutdown completeness (T70).** Specify and handle
  prefetched-but-undispatched deliveries on `Close` (drain or
  nack-requeue, documented), and flush a `BatchConsumer`'s pending
  partial batch on `Close`/final `FlushAfter`.
- **R10-16 — Observability gaps (T71).** Add the leading-indicator
  metrics the cliffs above need: channel-pool acquire-wait/saturation,
  a `consumer_in_flight{queue}` gauge, and a `consumer_redelivered_total`
  counter (redelivery ratio is the #1 instability signal). Coordinates
  with Phase 10 T50/T52/T53.
- **R10-17 — TCP keepalive / dialer hardening (T72).** AMQP heartbeats
  cover read-side partition detection; add `net.Dialer` keepalive (and
  document `TCP_USER_TIMEOUT` where available) so a write to a
  half-open socket fails promptly instead of relying on
  `ConfirmTimeout` as the only backstop.

**Deferred (architecture decision):**

- **R10-18 — Sync-confirm throughput ceiling / async publish API
  (LATER-34).** `Publish` holds a pooled channel for a full confirm
  round-trip, so per-`Publish` throughput is `pool/RTT` and a confirm-
  latency spike cascades into `ErrChannelPoolExhausted`. The §9
  30k/100k targets hold only at sub-ms (local-broker) RTT. Whether to
  add an async/streaming publish API with a bounded in-flight window
  (reversing Rev-6 decision 31) vs documenting the ceiling honestly is
  an owner decision, tracked in `LATER.md` as LATER-34.

**Second-report verification pass (2026-05-25).** A separate SRE/AMQP
gap report was cross-checked against this revision. 9 of its 11 items
were already covered: at-least-once DLX strategy (decision 52), shutdown
cascade (decision 53), non-blocking dispatcher (§6.3), RPC
auto-population (§6.7), BatchConsumer Links (decision 54),
`multiple=true` resolution (§6.2), `DeliveryMode` wire mapping (§6.5),
quorum-vs-classic prefetch (§6.3 "Queue Type Nuance"), and `Health()`
semantics (§6.1) — items 1–9/11 in that report correspond to existing
text, notably the Rev 9 decisions. Its prefetch suggestion ("hundreds"
for quorum) was rejected as unsupported; the 16-min/64-default guidance
stands. Two genuinely-new doc notes were added from it: a connection-name
length caveat on `WithConnectionName` (§6.1), and an explicit
component-registration + idempotent double-close contract on
`Connection.Close` (§6.1).
