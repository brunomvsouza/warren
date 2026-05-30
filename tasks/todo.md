# Task list: Warren v0.1.0

> Live checklist. Tick boxes as work progresses. See `plan.md` for
> phase rationale, dependency graph, and risks. See `SPEC.md` for the
> public API surface.

Sizing: **XS** 1 file · **S** 1–2 files · **M** 3–5 files · **L** 5–8.
Any **L** here should be split into S/M before starting.

Total: **54 tasks**, **9 phases**. Rev 6 added T18b
(HandlerTimeoutVerdict matrix), T38b (`examples/idempotent_consume/`),
T38c (`examples/ordered_consume/`) for the specialist-review fixes:
PublishRetry duplicate contract, HandlerTimeout verdict consistency,
synchronous reconnect barrier + degraded state, ConfirmTimeout
default 30 s, Concurrency vs ordering, default conn counts 2/2,
consumer-tag UUID default, AMQPCode 312/313, UserID validation,
Replier missing-DLX validation, `x-death` reason filter, SASL
EXTERNAL fail-closed, `errorlint`, `basic.cancel` surfacing. See
`plan.md` "Revision history" Rev 6 for full rationale.

**Rev 9 — SRE & AMQP 0-9-1 specialist review.** Five surface changes:
`Topology.Declare` auto-injects `x-dead-letter-strategy: at-least-once` for quorum queues (T15);
`Connection.Close` graceful cascade (T07); `Concurrency(n)` non-blocking dispatcher requirement (T18);
`Caller[Req,Resp]` auto-stamps `CorrelationID` and `ReplyTo` (T29); `BatchConsumer` OTel span creates Links for all incoming trace contexts (T28).

**Rev 7 — cross-check tightening pass (no structural changes).** Six
acceptance criteria sharpened so they live on the producing task
instead of the T40 godoc sweep: **T07** (`AuthenticatedUser()`
populated by `Dial` + PLAIN/EXTERNAL unit cases), **T13** + **T18**
(functional-options last-wins on `PublisherBuilder` and
`ConsumerBuilder`), **T16** (`AttachTo` snapshots keyed by `*Topology`
pointer address — replace-on-same-pointer, append-on-different-
pointer), **T34** (`WithFrameMax` and `WithHeartbeat` godoc carry the
SPEC §6.1 sizing tiers; `WithHeartbeat(0)` emits a `Dial` warning),
**T41** (`internal/amqperror` and `internal/redact` added to the
≥95% critical-path coverage list per SPEC §9 line 2107–2109).
Task count unchanged (54); phase layout unchanged; SPEC.md
unchanged.

---

## Phase 1 — Foundation: a Connection that survives outages

### [x] T01 — Repo bootstrap · S
Set up the empty Go module with the bare-minimum dev ergonomics.
- **Acceptance:**
  - [ ] `go.mod` declares `module github.com/brunomvsouza/warren` and Go 1.23.
  - [ ] `LICENSE` is MIT with the user's copyright.
  - [ ] `Makefile` exposes `build`, `test`, `test-integration`, `test-conformance`, `lint`, `mocks`, `doc`, `hooks`.
  - [ ] `make hooks` writes `.git/hooks/pre-commit` running `make lint test` (opt-in only; never auto-installed).
  - [ ] `.golangci.yml` enables `errcheck`, `govet`, `staticcheck`, `gosec`, `revive`, `gocritic`, `unparam`, `bodyclose`, `nilerr`, **`errorlint`** (catches `err == ErrFoo` typos and missing `%w` verbs that would corrupt the error-classification surface).
  - [ ] `.gitignore` covers Go build artifacts.
- **Verify:** `go build ./...` and `golangci-lint run ./...` both succeed on an empty package; `make hooks` installs hook, hook fires on commit.
- **Files:** `go.mod`, `go.sum`, `LICENSE`, `Makefile`, `.golangci.yml`, `.gitignore`, `doc.go`.
- **Deps:** none.

### [x] T02 — Sentinel errors, type aliases, constants · M
Static foundation that everything else imports. Rev 5 added three
new sentinels (`ErrPublishNacked`, `ErrChannelPoolExhausted`,
`ErrBatchTooLarge`) and the `SASLMechanism` typed enum. Rev 6 adds
three more (`ErrReconnecting`, `ErrTopologyRedeclareFailed`,
`ErrConsumerCancelled`) plus the `TimeoutVerdict` typed enum.
- **Acceptance:**
  - [ ] All sentinels from SPEC §6.8 declared in `errors.go`, including
        `ErrConnectionBlocked`, **`ErrChannelPoolExhausted`** (transient),
        **`ErrPublishNacked`** (transient), **`ErrBatchTooLarge`**
        (permanent), **`ErrReconnecting`** (transient — connection in
        reconnect barrier), **`ErrTopologyRedeclareFailed`** (permanent
        — connection in degraded state), **`ErrConsumerCancelled`**
        (consumer received `basic.cancel`; not a classifier sentinel),
        the 15 AMQP reply-code sentinels (311, 320, 402–406, 501–506,
        530, 540, 541), and the classifier sentinels
        `ErrTransient`/`ErrPermanent`.
  - [ ] `AMQPCode(err) (uint16, bool)` returns the AMQP reply code
        embedded in a wrapped sentinel (`ErrAccessRefused` → 403, etc.)
        and `(0, false)` otherwise.
  - [ ] `IsTransient(err)` / `IsPermanent(err)` classifier helpers
        implemented per the matrix in SPEC §6.8: transient: 311, 320,
        504, 541, wrapped `ErrTransient`, `ErrPublishNacked`,
        `ErrChannelPoolExhausted`, `ErrConnectionBlocked`,
        `ErrConfirmTimeout`, **`ErrChannelClosed`**,
        **`ErrReconnecting`**; permanent: 402–406, 501, 502, 503, 505,
        **506**, 530, 540, wrapped `ErrPermanent`,
        **`ErrTopologyRedeclareFailed`**. **`506`
        (ErrResourceError) is permanent** by decision (SPEC §6.8 godoc
        explains; was transient in Rev 4).
  - [ ] `types.go` exports `Headers map[string]any`, `DeliveryMode uint8`
        (with `DeliveryModePersistent` as the zero value),
        `ExchangeKind string` (Direct/Fanout/Topic/Headers/Delayed),
        `OverflowPolicy string` (DropHead/RejectPublish/RejectPublishDLX),
        `QueueType string` (Classic/Quorum/Stream), **`SASLMechanism string`
        with constants `SASLPlain` and `SASLExternal`**, **`TimeoutVerdict uint8`
        with constants `TimeoutNackNoRequeue` (zero value) and
        `TimeoutNackRequeue`**.
  - [ ] Table-driven tests cover `AMQPCode`, `IsTransient`, `IsPermanent`
        for `nil`, plain `errors.New`, direct sentinels, and wrapped
        sentinels (via `fmt.Errorf("...: %w", sentinel)`). Includes
        an explicit test that `IsTransient(ErrResourceError) == false`
        and `IsPermanent(ErrResourceError) == true`.
- **Verify:** `make test lint` clean. `go vet ./...` clean.
- **Files:** `errors.go`, `errors_test.go`, `types.go`, `types_test.go` (constants).
- **Deps:** T01.

### [x] T03 — `log/` package · S
Pluggable logger with three adapters. All adapters route through
`internal/redact.URI` before emitting any string that contains an
AMQP URI.
- **Acceptance:**
  - [ ] `Logger` interface matches SPEC §6.9 contract (`Debug/Info/Warning/Error` + `f` variants).
  - [ ] `NoOp`, `Slog` (wraps `log/slog`), `Std` (wraps stdlib `log`) adapters present.
  - [ ] Each adapter has a passing round-trip test (capture output, assert level + message).
  - [ ] Each adapter has a passing redaction test: pass a string containing `amqp://user:p%40ss@h:5672/v` and assert the captured output contains `amqp://***@h:5672/v` and never `p@ss` / `p%40ss`. Covers `amqp://` and `amqps://` schemes and URI-encoded passwords.
- **Verify:** `go test ./log/...` passes.
- **Files:** `log/logger.go`, `log/noop.go`, `log/slog.go`, `log/std.go`, `log/*_test.go`.
- **Deps:** T02, **T07c**.

### [x] T04 — `metrics/` package · M
Three metric interfaces, Prometheus default, NoOp. Rev 5 added
bounded labels, opt-in high-cardinality, configurable latency
buckets, and a set of mandatory metrics.
- **Acceptance:**
  - [ ] `ClientMetrics`, `PublisherMetrics`, `ConsumerMetrics` interfaces defined per SPEC §6.9.
  - [ ] `Prometheus*` implementations register their collectors lazily into a passed-in `prometheus.Registerer` (no global `prometheus.DefaultRegisterer`).
  - [ ] **Default Prometheus labels are bounded** per SPEC §6.9: `ClientMetrics{addr, role, connection_name}`, `PublisherMetrics{exchange, outcome}`, `ConsumerMetrics{queue, outcome}`. `addr` is host:port (no userinfo, redacted via `internal/redact.URI`).
  - [x] **Opt-in high-cardinality labels** (`MetricsLabelRoutingKey`, `MetricsLabelMessageType`) enabled at metrics construction — `NewPrometheusPublisherMetrics(reg, buckets, labels...)` / `NewPrometheusConsumerMetrics(reg, buckets, labels...)`. Prometheus fixes a vector's label set at creation, so the opt-in lives at construction, not on a later builder option (SPEC §6.9 amended). `routing_key` → `publisher_publish_seconds` (publisher routing key; ignored for consumers); `message_type` → `publisher_publish_seconds` + `consumer_handler_seconds` (Go type name of M). The library threads the values; an impl emits a label only when enabled.
  - [ ] **Configurable histogram buckets** via `WithLatencyBuckets([]float64)`; default `[0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]` ms.
  - [ ] **Mandatory metrics** (always present): `connection_reconnects_total{role}`, `connection_blocked_seconds{role}`, `consumer_resubscribed_total{queue}`, `consumer_handler_aborted_channel_closed_total{queue}`, `publisher_in_flight{exchange}` (gauge), `publisher_publish_seconds{exchange, outcome}` (histogram), `consumer_handler_seconds{queue, outcome}` (histogram).
  - [ ] `NoOp*` implementations have zero allocations on every method (verified with `testing.AllocsPerRun`).
- **Verify:** `go test ./metrics/...` passes; `go test -bench=BenchmarkNoOp -benchmem ./metrics/...` reports `0 allocs/op`; integration test scrapes a `prometheus.Registry` and asserts each mandatory metric exists with the documented labels after a 5-second canned workload.
- **Files:** `metrics/metrics.go`, `metrics/prometheus.go`, `metrics/noop.go`, `metrics/*_test.go`.
- **Deps:** T02, **T07c**.

### [x] T05 — `otel/` package · S
Tracer interface with NoOp default and AMQP header propagator
skeleton. Pinned to **OpenTelemetry Messaging semantic conventions
v1.27.0+**.
- **Acceptance:**
  - [ ] `Tracer` interface wraps the subset of OTel APIs used by Publisher/Consumer (start span, end span, set attributes, record error).
  - [ ] `NoOpTracer` is the package-level zero value; methods are no-ops.
  - [ ] `Propagator` struct with `Inject(ctx, Headers)` and `Extract(Headers) ctx` matching OTel Messaging semantic conventions **v1.27.0** (uses `go.opentelemetry.io/otel/semconv/v1.27.0`).
  - [ ] Header keys used by `Propagator`: `traceparent`, `tracestate`. Unit test asserts no collision with CloudEvents binary-mode `cloudEvents:`-prefixed headers.
  - [ ] Package godoc references SPEC §6.9 for the span-attribute matrix on the wire; **Publisher/Consumer populate those attributes in T27/T28** (not required for T05 merge).
- **Verify:** Unit tests assert round-trip `Inject → Extract` preserves traceparent. Snapshot test against `go.opentelemetry.io/otel/semconv/v1.27.0` attribute keys (will fail loudly if the semconv pin is bumped without intention). **Full span attributes on publish/consume paths are acceptance-tested in T27/T28.**
- **Files:** `otel/tracer.go`, `otel/propagation.go`, `otel/*_test.go`.
- **Deps:** T02.

### [x] T06 — `internal/reconnect` + `RetryPolicy` · M
Generic supervised reconnect loop usable by Connection.
- **Acceptance:**
  - [ ] `RetryPolicy` struct (exposed in root package) with exponential backoff + jitter; `Min`, `Max`, `Factor`, `Retries`, `WithoutJitter`.
  - [ ] `internal/reconnect.Loop` wraps a connect function, applies the policy, exposes `Stop(ctx)` and `OnReconnect(func())`.
  - [ ] No `time.Sleep` — uses `time.NewTimer` so contexts can cancel.
- **Verify:** Unit tests assert: 3 attempts then succeed; cancel mid-backoff returns `ctx.Err()`; `goleak.VerifyNone` clean.
- **Files:** `retry.go`, `internal/reconnect/loop.go`, `internal/reconnect/*_test.go`.
- **Deps:** T02, T03.

### [x] T07 — `connection.go`: single-TCP Dial, Health, Close · M
Wire `amqp091-go` + reconnect loop + metric/logger/tracer plumbing
for a **single TCP connection**. Multi-conn fan-out lands in T07d;
T07 keeps the per-socket lifecycle focused.
- **Acceptance:**
  - [ ] `Dial(ctx, opts...) (*Connection, error)` with the single-conn subset of options listed in SPEC §6.1 (`WithAddr`, `WithAddrs`, `WithTLSConfig`, `WithAuth`, `WithSASLMechanism`, `WithVHost`, `WithHeartbeat`, `WithChannelMax`, `WithFrameMax`, `WithDialer`, `WithClientProperties`, `WithConnectionName`, `WithConnectDelay`, `WithReconnectBackoff`, `WithOnBlocked`, **`WithOnTopologyDegraded`**, `WithLogger`, `WithMetrics`, `WithoutMetrics`, `WithTracer`).
  - [ ] **`Connection.Logger()` is removed from the public API** (was a leak of internals — see Rev 5 review). Internal logging continues unchanged.
  - [ ] **`(*Connection).AuthenticatedUser() string`** exported for `Publisher` client-side `UserID` validation (T12/T13).
  - [ ] **`AuthenticatedUser()` populated by `Dial` before it returns** (Rev 7): the field is set from the SASL outcome on the underlying `amqp091-go` connection so any `Publisher` built *after* `Dial` returns observes a non-empty value. Unit test matrix: (a) PLAIN auth with `WithAuth("alice", "…")` → returns `"alice"`; (b) SASL EXTERNAL with a client cert whose subject is `CN=svc-orders,UID=42` → returns the broker-side resolved identity (typically the CN); (c) on a connection in degraded state, the field is still readable (frozen at last successful authentication).
  - [ ] **`(*Connection).ForceReconnect() error`** exported operator helper for recovering from degraded state without restarting the process.
  - [ ] `Connection.Health(ctx)` opens a temporary channel, declares a passive queue, and closes it (verifies socket+topology).
  - [ ] `Connection.Close(ctx)` implements graceful cascade: cancel all active consumers, wait for handlers to finish, wait for publisher confirms, close TCP sockets.
  - [ ] `WithOnBlocked` callback fires when broker sends `connection.blocked`.
  - [ ] While the connection is broker-blocked, publishes wait until unblock or `ctx.Done()`; on ctx cancel, return `ErrConnectionBlocked` (wrapping `ctx.Err()`).
  - [ ] **Synchronous reconnect barrier.** On every reconnect, run channel re-open → `Topology.AttachTo` redeclare → consumer re-`basic.consume` + re-`basic.qos` → user `WithOnReconnect` synchronously, in that order. `Publisher.Publish` routed to a reconnecting connection blocks on a per-conn `sync.Cond` until step 2 completes; on `ctx` cancel returns `ErrReconnecting` (transient, wrapping `ctx.Err()`). Mandatory metric `topology_redeclare_seconds{role}` (histogram) records the barrier duration per cycle.
  - [ ] **Degraded-state machine.** If the redeclare in step 2 fails, increment `connection_degraded_total{role, reason}`, fire `WithOnTopologyDegraded(func(error))` exactly once per transition (re-arm on recovery), and hold the state: subsequent `Publisher.Publish` returns `ErrTopologyRedeclareFailed` (permanent), consumers do NOT re-issue `basic.consume`. The supervisor retries redeclare on every next reconnect cycle; first success clears the flag and resumes traffic. `(*Connection).ForceReconnect()` can trigger a manual reconnect cycle for operator recovery.
  - [ ] `Dial` validates `WithChannelPoolSize` against the negotiated `channel-max`; returns `ErrInvalidOptions` if pool > channel-max.
  - [ ] `Dial` rejects `WithFrameMax < 4096` with `ErrInvalidOptions` (AMQP-spec minimum).
  - [ ] **`Dial` validates SASL EXTERNAL fail-closed:** with `WithSASLMechanism(SASLExternal)`, require (a) `WithTLSConfig` provided, (b) `len(cfg.Certificates) > 0 || cfg.GetClientCertificate != nil`, (c) endpoint scheme is `amqps://`. Any unmet returns `ErrInvalidOptions` with the specific reason.
  - [ ] **`Dial` logs a warning** when `WithPublisherConnections(1)` or `WithConsumerConnections(1)` (single socket = full-availability gap during reconnect).
  - [ ] Default `WithChannelMax(0)` triggers server-driven negotiation (not the old `2047` literal).
  - [ ] Default `WithConnectionName` is `<binary>-<hostname>-<pid>`; verified via `rabbitmqctl list_connections name`.
  - [ ] Default tracer is NoOp (no nil-checks in code paths).
  - [ ] `connection_reconnects_total{role=single}` increments exactly once per reconnect (not in a loop); regression test forces 3 reconnects and asserts the counter reads exactly 3.
  - [ ] **Lens-08 (CR-03/CR-11):** §6.1 describes the *actual* ctx-cancellable barrier mechanism (a `sync.Cond` woken by a per-Wait `ctx`-watcher goroutine, or a channel-broadcast barrier), not the impossible-as-worded "condition variable cancellable via `ctx`" (`connection.go:43/597-604`); the per-Wait-iteration watcher is bounded/pooled (a reconnect storm with K blocked publishers must not spawn K×iterations goroutines); `ForceReconnect` is documented idempotent/coalesced (cap-1 channel) and safe during an in-progress reconnect; a test asserts a ctx-cancel during the barrier returns `ErrReconnecting` with no goroutine leak.
- **Verify:** Integration test (testcontainers RabbitMQ): Dial succeeds, Health passes, Close completes cleanly. `goleak.VerifyNone` at test exit. A second test sets `WithChannelPoolSize` higher than `WithChannelMax` and asserts `ErrInvalidOptions`.
- **Files:** `connection.go`, `options_connection.go`, `connection_test.go`, `connection_integration_test.go`.
- **Deps:** T03, T04, T05, T06, **T07c**.

### [x] T07b — `internal/amqperror`: AMQP reply-code translation · S
Centralised translator that converts `*amqp091.Error` (carried by
`amqp091-go` on channel/connection close) into wraps of the SPEC §6.8
reply-code sentinels, and powers `AMQPCode` + `IsTransient` /
`IsPermanent`.
- **Acceptance:**
  - [ ] `amqperror.Wrap(err error) error` returns the input wrapped with the appropriate sentinel (e.g. `*amqp091.Error{Code: 404}` → wraps `ErrNotFound`). Non-AMQP errors pass through unchanged.
  - [ ] `AMQPCode` in the root package delegates to the table maintained here; covers every code listed in SPEC §6.8.
  - [ ] `Wrap` preserves the original error so `errors.As(err, &amqp091.Error{})` still works (chain length 2).
  - [ ] Helper table mapping is exhaustive against the AMQP 0-9-1 channel/connection-close reply codes. Codes that are **not** channel-close codes — notably `312 NO_ROUTE` and `313 NO_CONSUMERS`, which travel in `basic.return` frames and are surfaced by the publisher path as `ErrUnroutable` — are documented as intentional omissions in a header comment with a one-line rationale ("not a channel-close code; handled in `internal/confirms` via `basic.return`").
  - [ ] **`IsTransient(ErrResourceError)` returns false** (506 is now permanent by default per SPEC §6.8 godoc); explicit table entry + unit test cover the change versus Rev 4 wording.
- **Verify:** Table-driven unit test in `internal/amqperror/translate_test.go` runs every documented code through `Wrap` and asserts the right sentinel + classifier outcome.
- **Files:** `internal/amqperror/translate.go`, `internal/amqperror/translate_test.go`.
- **Deps:** T02.
- **Blocks:** T07 (consumes the translator), and all downstream broker-touching tasks.

### [x] T07c — `internal/redact`: AMQP URI credential redaction · S
Mandatory choke-point for the SPEC §8 "Always: redact credentials"
rule. Every string the library hands to logs, metric labels, span
attributes, or error messages that contains an AMQP URI passes
through this helper.
- **Acceptance:**
  - [ ] `redact.URI(s string) string` replaces the `userinfo` component of any AMQP URI inside `s` with `***`, preserving scheme, host, port, vhost, and query string. Handles `amqp://`, `amqps://`, percent-encoded passwords (`%40` for `@`, etc.), and trailing-`/` variants.
  - [ ] `redact.URIs([]string) []string` for the cluster-failover case.
  - [ ] `redact.Error(err error) error` returns a new error whose `Error()` string is redacted; preserves the chain for `errors.Is`/`errors.As`.
  - [ ] Table-driven unit tests cover: bare host (`amqp://h`), user-only (`amqp://u@h`), user+pass (`amqp://u:p@h`), percent-encoded pass (`amqp://u:p%40w@h:5672/v?heartbeat=10`), `amqps://`, multiple URIs in one string, malformed URIs (return unchanged).
  - [ ] **Negative test**: scan the source tree for log/metric emission sites and assert each one routes through `redact.URI` before emitting (lint helper or audit doc — flagged in T45b for the runtime regression scan).
  - [ ] **`FuzzRedactURI` fuzz target** exercising malformed URIs (truncated authority, double `@`, percent-decoding edge cases, IPv6 literal hosts, non-AMQP schemes mixed in) — asserts the redactor never panics and any returned string with a userinfo passes `regexp.MustCompile("://[^@]*:[^@]*@")` only when it was already malformed before.
- **Verify:** `go test ./internal/redact/...` covers every shape; coverage ≥ 95%; `go test -fuzz=FuzzRedactURI -fuzztime=30s` clean locally.
- **Files:** `internal/redact/redact.go`, `internal/redact/redact_test.go`, `internal/redact/redact_fuzz_test.go`.
- **Deps:** none (pure helper, no AMQP deps).
- **Blocks:** T03, T04, T07, T07d (all emit URI-bearing strings).

### [x] T07d — Multi-TCP fan-out by role · M
SPEC §6.1 calls for `*Connection` to wrap a pool of TCP connections
split by role. `amqp091-go` serializes I/O per `warren.Connection`,
so a single socket bounds confirm throughput. T07 covered the
single-socket lifecycle; T07d adds the pool.
- **Acceptance:**
  - [ ] `WithPublisherConnections(n int)` **default 2** (Rev 6: was 1); opens `n` dedicated publisher TCP connections at `Dial`. Each has its own `internal/reconnect` supervisor. `n=1` is supported and emits a warning log at `Dial`.
  - [ ] `WithConsumerConnections(n int)` **default 2**; same for consumers. Consumers are pinned to one of these by stable hash of consumer-tag. **Default consumer-tag is `ctag-<uuidv7>`** (generated at `ConsumerBuilder.Build`) so defaulted consumers hash to distinct values and actually fan out — pinning the empty string would collide every defaulted consumer onto the same socket.
  - [ ] Connection-name suffix per socket: `<base>-pub-0..n-1` and `<base>-con-0..m-1`. Verified via `rabbitmqctl list_connections name`.
  - [ ] `Publisher[M]` channel acquisition picks the **least in-flight** publisher connection's pool (load-balancing across sockets); falls back to any unblocked publisher connection if the chosen one is broker-blocked.
  - [ ] Consumer pin stability under reconnect: when a consumer connection fails over, every pinned `Consumer[M]` follows it (re-issues `basic.consume` on the new channel of the same logical connection-id, not on a different consumer connection). Unit-tested with a simulated failover.
  - [ ] `Close(ctx)` closes every TCP connection in the pool, draining publish + handler work first.
  - [ ] Metric `connection_reconnects_total{role}` distinguishes `role=publisher` vs `role=consumer`.
- **Verify:** Integration test with `WithPublisherConnections(3) + WithConsumerConnections(2)` asserts 5 sockets visible to `rabbitmqctl list_connections name`. Failover test: kill the consumer connection that hosts a known consumer, assert the consumer's re-subscribe lands on the same logical pin-index (verified via the named connection suffix). Bench in T44b exercises throughput.
- **Files:** edits to `connection.go`, `options_connection.go`, new `internal/connpool/pool.go`, `internal/connpool/*_test.go`, `connection_multiconn_integration_test.go`.
- **Deps:** T07, T07b, T07c.

### [x] T08 — `channelpool.go` per publisher TCP connection · M
Internal channel pool used by publishers, **one pool per publisher
TCP connection** (not a global pool across all publisher conns).
- **Acceptance:**
  - [x] `channelPool` with `Acquire(ctx) (amqpChannel, release func(), error)` semantics (interface; `*amqp091.Channel` satisfies it).
  - [x] Size driven by `WithChannelPoolSize(n)` (default 8); **applies per publisher TCP connection**, so `WithPublisherConnections(4)+WithChannelPoolSize(8) = 32` channels total.
  - [x] Channels are discarded and replaced when broker signals channel close.
  - [x] **`ErrChannelPoolExhausted`** returned from `Acquire(ctx)` when `ctx` is cancelled while waiting on a saturated pool. Classified `IsTransient`.
  - [x] Race-free: `go test -race` clean under concurrent Acquire/release.
  - [ ] **Lens-08 (CR-08):** §6.2 documents pool `Acquire` is best-effort, **not FIFO** (Go channel receive has no waiter ordering, `channelpool.go:57`), so a waiter can starve under *permanent* exhaustion; recommend sizing the pool to peak concurrency (a FIFO wait queue is deferred). The dead-channel liveness guard (`channelpool.go:80,122`) stays do-not-regress. *dep T143 (CG-6).*
- **Verify:** Unit + integration tests; `goleak.VerifyNone`. Saturation test: pool size 1, two concurrent `Acquire` calls, second with a 50ms ctx — asserts `ErrChannelPoolExhausted`.
- **Files:** `channelpool.go`, `channelpool_test.go`.
- **Deps:** T07, T07d.

### Checkpoint — Phase 1 done
- [ ] `make lint test` clean.
- [ ] Reconnect smoke test: kill broker, Connection recovers within
      configured backoff window.
- [ ] **1000** connect/disconnect cycles produce zero goroutine leaks.
- [ ] Multi-conn fan-out demo: `WithPublisherConnections(4)` opens
      4 sockets, throughput scales ≥ 3× over single-conn (see T44b).
- [ ] No log line emitted during phase-1 integration contains a
      clear-text password.
- [ ] **Review with human before Phase 2.**

---

## Phase 2 — Producer: synchronous-with-confirm publish

### [x] T09 — `codec/` package + JSON · S
Codec interface and the first implementation. **Rev 8** makes JSON
**lax by default** (Postel's Law — conservative on send, liberal on
receive) and keeps the panic safety contract. The Rev 5 strict-default
was reverted because at billions/day, producer-first deploys must not
poison v1 consumers' DLQs.
- **Acceptance:**
  - [ ] `Codec` interface: `Encode(any) ([]byte, error)`, `Decode([]byte, any) error`, `ContentType() string`.
  - [ ] **`codec.NewJSON()` is lax by default (Postel's Law)** — `Decode` accepts unknown fields on the wire and ignores them; producer-first deploys do not poison v1 DLQs.
  - [ ] **`codec.NewJSONStrict()`** opt-in variant that calls `json.Decoder.DisallowUnknownFields()`; an unknown field surfaces as `ErrInvalidMessage` wrapping the `json` error. Documented in godoc as the choice for regulated/compliance pipelines where consumer-side drift MUST be a hard error.
  - [ ] Both variants share the same `ContentType` = `application/json`.
  - [ ] **Panic safety contract** (consumed by Publisher / Consumer): Publisher/Consumer wrap every `Encode`/`Decode` call in `defer recover` and convert a recovered value into `ErrInvalidMessage` (wrapping `fmt.Errorf("codec panic: %v", r)`). A unit test in `publisher_test.go` / `consumer_test.go` injects a panicking fake codec and asserts the path returns `ErrInvalidMessage` without the goroutine crashing.
  - [ ] Round-trip property tests for lax + strict + one fuzz target (`FuzzCodecJSON`).
- **Verify:** `go test ./codec/...` + `go test -fuzz=FuzzCodecJSON -fuzztime=30s ./codec` (locally). Lax-default regression test: `NewJSON()` accepts `{"id":1,"extra":true}` decoded into `struct{ID int}` without error. Strict opt-in regression: `NewJSONStrict()` rejects the same payload with `ErrInvalidMessage`.
- **Files:** `codec/codec.go`, `codec/json.go`, `codec/json_test.go`, `codec/json_fuzz_test.go`.
- **Deps:** T02.

### [x] T10 — `message.go`: `Message[M]` struct · S
Plain struct + default application at publish time. SPEC compliance
fix: `ContentType` (MIME) is a separate field from `ContentEncoding`
(transfer encoding).
- **Acceptance:**
  - [ ] All fields from SPEC §6.5 exported, including the **distinct** `ContentType` and `ContentEncoding`.
  - [ ] Helper `(*Message[M]).applyDefaults(codec.Codec)` fills:
    - `MessageID` ← UUID v7 (RFC 9562) if empty.
    - `Timestamp` ← `time.Now()` if zero.
    - `ContentType` ← `codec.ContentType()` if empty.
    - **Does NOT** touch `ContentEncoding` — left empty unless user set it.
  - [ ] `DeliveryModePersistent` is the zero value.
  - [ ] `Headers` are validated against AMQP field-table value types before publish; unsupported Go types return `ErrInvalidMessage`.
  - [ ] **Field godoc** (per SPEC §6.5 "Field notes"):
    - `Priority`: documents wire range 0–255, RabbitMQ convention 0–9, and the broker's silent clamp against `x-max-priority`.
    - `Expiration`: documents the AMQP shortstr wire format (ASCII milliseconds), sub-millisecond rounding to 0, and that `"0"` means "expire immediately".
    - `Headers`: enumerates the supported AMQP field-table value types and references `ErrInvalidMessage` for rejections.
    - `ContentType` vs `ContentEncoding`: one-line cross-reference clarifying the MIME-vs-transfer-encoding split.
  - [ ] Unit tests cover defaulting, that `applyDefaults` doesn't overwrite user-set fields, and the Headers validation matrix (one happy + one rejected case per unsupported Go kind).
  - [x] **Lens-09 (PC-09):** call `uuid.EnableRandPool()` once at init to batch the per-publish `crypto/rand` reads; document the google/uuid **process-global `timeMu` lock** taken per UUIDv7 (a process-wide serialization point at the billions/day bar) and that `MessageID` is load-bearing for at-least-once dedupe so it cannot be skipped; an `AllocsPerRun` guard asserts the per-call entropy buffer is gone (coordinated with T148's combined guard). dep PG-1.
  - [x] **Lens-12 (DX-12):** the load-bearing `Message[M]` warning must live in the **call-site godoc**, not only in SPEC — add the **`UserID` 406-channel-close footgun** (§6.5 L1487-1513: a `UserID` that is not the authenticated connection user makes the broker close the channel) and the **`DeliveryMode` Persistent-is-the-zero-value-default** note (§6.5 L1450/L1554-1556) to the mandated field godoc, so a first-hour reader hovering the field sees the hazard without opening SPEC. dep XG-1.
- **Verify:** `go test -run TestMessage ./...`; `golangci-lint run --enable=revive` passes the missing-godoc rule on `message.go`.
- **Files:** `message.go`, `message_test.go`.
- **Deps:** T02, T09.

### [x] T11 — `internal/confirms` tracker · M
Publisher confirm tracker preserved across reconnects. Rev 5 adds
`basic.nack` from broker → `ErrPublishNacked` resolution.
- **Acceptance:**
  - [ ] `Tracker.Wait(deliveryTag, timeout)` returns ack/nack or `ErrConfirmTimeout`.
  - [ ] Handles `multiple=true` ack/nack semantics correctly (a single ack with `multiple=true` and tag T resolves every pending publish with `tag <= T`).
  - [ ] **`basic.nack` from broker → `ErrPublishNacked`.** A `nack` frame for a `delivery-tag` (possibly with `multiple=true`) resolves the matching `Wait` calls with `ErrPublishNacked` (transient classification). Covers RabbitMQ overflow policies (`reject-publish`, `reject-publish-dlx`) and mid-publish disk/memory alarms. Distinct from `basic.return` (which marks "returned" first and then resolves via `basic.ack` → `ErrUnroutable`).
  - [ ] **`basic.return` / `basic.ack` correlation** for mandatory publishes: when a `Return` frame arrives, the tracker marks the matching `delivery-tag` as "returned" and **records the originating reply code (312 NO_ROUTE or 313 NO_CONSUMERS)**; the subsequent `basic.ack` for that tag resolves `Wait` with `ErrUnroutable` **wrapped with the recorded code** so `AMQPCode(err)` returns 312 or 313 (per SPEC §6.8 godoc). `OnReturn` callback fires synchronously **before** `Wait` returns so the user-visible publish completion order is: callback → error to caller. (See SPEC §6.2 "basic.return / basic.ack correlation".)
  - [ ] Reset on channel close (in-flight publishes become `ErrChannelClosed`).
  - [ ] **One tracker per channel.** Each acquired channel from the per-conn pool gets its own tracker; channel close drops the tracker; new channel from pool gets a fresh one.
  - [ ] `goleak.VerifyNone` clean.
  - [x] **Lens-09 (PC-06 + PC-11):** two hot-path fixes — (PC-06) pool/reset the per-`Wait` `time.Timer` (the default `ConfirmTimeout=30s` arms a timer on every publish and every batch element, `tracker.go:171`), `AllocsPerRun` guard; (PC-11) replace the `resolveUpTo` whole-map scan + `slices.Sort` on every `multiple=true` frame under `t.mu` (`tracker.go:219-230` — O(outstanding)/frame, not the O(resolved) the §6.2 "single pass … critical for high-throughput batching" wording implies) with a contiguous confirmed low-water-mark + an ordered index (or min-heap) → O(resolved + log n), and amend §6.2 to state the real complexity; the one-shot resolve / `Wait`-self-delete / `CloseAll` mechanism stays do-not-regress. dep PG-1, PG-3.
- **Verify:** Table-driven unit tests covering ordered, out-of-order, multiple-ack, multiple-nack, return-then-ack (mandatory unroutable), broker-nack alone, broker-nack with `multiple=true`, and channel-close scenarios. The return-then-ack and nack-only tests use a hand-rolled fake `amqp091.Channel` that emits frames in the documented order.
- **Files:** `internal/confirms/tracker.go`, `internal/confirms/*_test.go`.
- **Deps:** T02.

### [x] T12 — Publisher + builder (no mandatory yet) · M
- **Acceptance:**
  - [x] `PublisherFor[M](conn) *PublisherBuilder[M]` with builder methods from SPEC §6.2 (excluding Mandatory/OnReturn — those are T13). **Note:** no `Immediate()` method — the flag is unsupported by RabbitMQ.
  - [x] `Publisher.Publish(ctx, Message[M])` is synchronous-with-confirm. **Concurrency-safe**: many goroutines may share one `*Publisher[M]`; each call acquires a channel from the publisher-conn pool with `least-in-flight` selection (see T07d), publishes, awaits its own confirm, returns the channel. Verified by a `go test -race` stress with N=64 goroutines.
  - [x] `Publisher.Close(ctx)` drains in-flight publishes.
  - [x] `PublisherMetrics` calls fire for success and error paths; `publisher_in_flight{exchange}` gauge tracks outstanding confirms.
  - [x] Errors from the broker are wrapped via `internal/amqperror.Wrap`, so `errors.Is(err, ErrAccessRefused)` etc. work for publish failures.
  - [x] `ErrChannelPoolExhausted` surfaces when ctx is cancelled waiting on a saturated pool (asserted in a unit test against T08).
- **Verify:** Integration test: publish a JSON message, fetch it via `rabbitmqadmin get`, assert body + properties (including the right `content-type` vs `content-encoding` placement). Concurrency test: 64 goroutines × 1000 publishes each = 64k publishes total, all confirmed, `go test -race` clean.
- **Files:** `publisher.go`, `publisher_builder.go`, `publisher_test.go`, `publisher_integration_test.go`.
- **Deps:** T07b, T07d, T08, T09, T10, T11.

### [x] T13 — Mandatory + Returns + Timeouts + PublishRetry + broker-nack + UserID + retry metric · M
Rev 5 adds `PublishTimeout`, `PublishBatchMaxInFlight`,
`PublishRetry`, and `ErrPublishNacked`. `PublisherBuilder.RetryPolicy()`
from Rev 4 is **renamed** to `PublishRetry()`. Rev 6:
`PublishBatchMaxInFlight` **renamed** to `PublishBatchMaxSize`;
`ConfirmTimeout` **default = 30 s** (was 0); new builder methods
`StampUserID()`; client-side `UserID` validation in `Publish`;
mandatory metric `publisher_retry_total{exchange, reason}`.
- **Acceptance:**
  - [ ] `Mandatory()` sets the AMQP mandatory flag (set-only; no inverse method).
  - [ ] `OnReturn(func(Return))` fires on broker `basic.return`, synchronously before `Publish` unblocks for the matching `delivery-tag` (correlation handled in T11).
  - [x] `Return.Properties` populated as a full `ReturnedProperties` struct — **12 `basic.properties` fields + `Headers` (13 total)**, mirroring SPEC §6.2 field-for-field. Not a flat map.
  - [ ] Unroutable mandatory publish returns `ErrUnroutable`. The `OnReturn` callback observes the same `Return` instance that informed the error.
  - [ ] **`ConfirmTimeout(d)` default = 30 s** (Rev 6); explicit `ConfirmTimeout(0)` disables it (documented as discouraged). Returns `ErrConfirmTimeout` deterministically (unit-test with a mock channel so timing is not load-dependent).
  - [ ] **`PublishTimeout(d)` end-to-end cap.** Bounds pool acquisition + write + confirm + blocked-wait + reconnect barrier. Returns the underlying error wrapped with `ctx.DeadlineExceeded`. Caller `ctx` deadline wins if shorter; both zero → caller `ctx` is authoritative.
  - [ ] **`PublishBatchMaxSize(n)` builder method** (Rev 6: renamed from `PublishBatchMaxInFlight`), default 1024. Validates at `Publish`-time only via T22 (not in T13). Stored on the publisher; no broker work here. Godoc clarifies "per-call cap, NOT a sliding in-flight window across calls".
  - [ ] **`MaxMessageSizeBytes(n)` builder method** (Rev 8 — SPEC §10 #50). Default 16 MiB. `n=0` disables guard explicitly; `n<0` fails `Build` with `ErrInvalidOptions`. `Publish` rejects encoded bodies above the cap with `ErrMessageTooLarge` (`IsPermanent==true`) before opening a channel; emits `publisher_publish_seconds{exchange, outcome="too_large"}`. Tests in `publisher_max_size_test.go` cover: builder last-wins, default value, explicit-zero-disable, negative-rejected, body-over-cap rejected without touching channel, body-under-cap accepted, outcome metric emitted, sentinel classifier (IsPermanent).
  - [ ] **`PublishRetry(p RetryPolicy)`** automatic retry of publishes failing with `IsTransient(err) == true` (`ErrPublishNacked`, `ErrChannelPoolExhausted`, `ErrConnectionBlocked`, `ErrConfirmTimeout`, **`ErrChannelClosed`**, **`ErrReconnecting`**, transient AMQP codes, network errors). Permanent errors never retried. Each retry attempt honours `PublishTimeout` independently; caller `ctx` is the overall budget. **Increments mandatory metric `publisher_retry_total{exchange, reason}`** on every retry (reason ∈ `nacked|confirm_timeout|channel_closed|pool_exhausted|blocked|network|reconnecting`).
  - [ ] **godoc on `PublishRetry` carries the duplicate warning verbatim** (per SPEC §6.2): "Retries can produce duplicates. Consumers MUST be idempotent (dedupe by `MessageID`). See §6.2.1."
  - [ ] **`ErrPublishNacked` from broker.** Builder configures a path that triggers broker `basic.nack` and asserts the caller sees `errors.Is(err, ErrPublishNacked)` + `IsTransient(err) == true`.
  - [ ] **`StampUserID()` builder method** auto-sets `Message[M].UserID` to `conn.AuthenticatedUser()` so the broker stamp survives without user bookkeeping.
  - [ ] **Client-side `UserID` validation** in `Publish`: if `Message[M].UserID != "" && != conn.AuthenticatedUser()`, return `ErrInvalidMessage` locally **without writing the publish frame** (prevents the 406-channel-close footgun). Cross-references SASL EXTERNAL flow.
  - [ ] While the connection is broker-blocked, `Publish` waits until unblock or `ctx.Done()`; on ctx cancel, returns `ErrConnectionBlocked`. While in reconnect barrier, returns `ErrReconnecting` on ctx cancel.
  - [ ] **Functional-options last-wins on `PublisherBuilder`** (Rev 7, per SPEC §6.1 line 515). Unit-test matrix: (a) `PublisherFor[M](conn).Metrics(a).Metrics(b).Build()` retains only `b` (assert by observing emitted metrics under a canned publish); (b) `.Metrics(b).WithoutMetrics().Build()` produces a publisher whose metric calls land on the no-op recorder, not on `b`; (c) builder-level option overrides the connection-level one — `Dial(WithMetrics(connLevel))` then `PublisherFor[M](conn).Metrics(builderLevel)` retains `builderLevel` on the publisher (the connection's own metrics remain `connLevel`). Same matrix applies to `.Codec(…)` chains and `.Tracer(…)` chains.
  - [x] **Lens-08 (CR-01):** the `OnReturn` timing wording (the `OnReturn` acceptance above) also **names the goroutine it runs on** — today it fires inline on the single unbuffered-return demux (`publisher.go:226`), a connection-reader path; the cross-cutting callback-invocation-goroutine contract + the dispatch-vs-doc decision live in **T144** (cited here).
- **Verify:**
  - Integration test against a routing-key that has no binding asserts `errors.Is(err, ErrUnroutable)` AND that `OnReturn` fired exactly once with a populated `Return.Properties.MessageID` matching what was published.
  - Unit test forces a confirm-window timeout via a mock channel that withholds the `basic.ack` frame; asserts `errors.Is(err, ErrConfirmTimeout)` and the publish goroutine releases (`goleak.VerifyNone`).
  - Integration test forces a broker-side block via `rabbitmqctl set_disk_free_limit 1TB` (preferred over the flaky `vm_memory_high_watermark=0.000001`, which can crash the testcontainer) and asserts that a publishing goroutine receives `ErrConnectionBlocked` after `ctx` cancellation. The disk-free knob is restored to `default` in the test teardown.
  - **Broker-nack integration test:** declare a queue with `Args{"x-overflow":"reject-publish","x-max-length":0}`, publish, assert `errors.Is(err, ErrPublishNacked)` and `IsTransient(err) == true`. Cleanup: delete the queue.
  - **`PublishRetry` unit test:** mock channel returns `ErrPublishNacked` on the first attempt and ack on the second; assert one retry occurred and the caller saw success. Permanent variant: channel returns `ErrUnroutable`; assert no retry.
  - **`PublishTimeout` unit test:** mock channel withholds confirm; `PublishTimeout(20ms)` returns within 20–25ms with the error chain containing both `ctx.DeadlineExceeded` and `ErrConfirmTimeout`.
  - **`ConfirmTimeout` default test:** mock channel withholds confirm; `Publish(context.Background(), …)` returns `ErrConfirmTimeout` within 30–31 s with no `PublishTimeout` set.
  - **`AMQPCode` 312/313 test:** mandatory publish to a queue without consumers (313) and to a non-existing routing key (312); assert `errors.Is(err, ErrUnroutable)` AND `AMQPCode(err) == (312, true)` or `(313, true)` as appropriate.
  - **`UserID` validation test:** open a connection as user `alice`, attempt `Publish` with `Message[M].UserID = "bob"`; assert `errors.Is(err, ErrInvalidMessage)` returned locally; assert via a channel-frame recorder that no publish frame was written. `StampUserID()` happy path: `UserID` left empty in `Message[M]`, builder option set; assert the broker-side stamp matches `alice` via `rabbitmqadmin get`.
  - **`publisher_retry_total` metric test:** mock channel returns `ErrPublishNacked` twice then ack; assert `publisher_retry_total{exchange=…, reason=nacked}` == 2 after the call.
- **Files:** edits to `publisher.go`, `publisher_builder.go`, `errors.go`, plus `publisher_returns_integration_test.go`, `publisher_confirm_timeout_test.go`, `publisher_blocked_integration_test.go`, `publisher_nack_integration_test.go`, `publisher_retry_test.go`, `publisher_timeout_test.go`, `publisher_userid_test.go`, `publisher_amqpcode_returns_test.go`, `publisher_retry_metric_test.go`, `publisher_max_size_test.go` (Rev 8).
- **Deps:** T11, T12.

### [x] T13b — Checkpoint example: `examples/publish/main.go` · S
First runnable example shipped on `main`, per SPEC §7 "Executable
examples at checkpoints" + §10 Rev decision 49. Must land in the
same PR (or immediately after) as T13 so the Phase 2 checkpoint
can close.
- **Acceptance:**
  - [ ] `examples/publish/main.go` is `package main`, reads broker URL from `AMQP_URL` (default `amqp://guest:guest@localhost:5672/`), declares its own ad-hoc topology (one exchange + one queue + one binding) via `Topology.Declare`, and demonstrates: `PublisherFor[M]` with a concrete user-defined `Order` payload, single `Publish` with `Mandatory()` + `OnReturn` callback, `ConfirmTimeout(30*time.Second)`, and `PublishRetry(warren.RetryPolicy{...})`. Returns non-zero exit code on any unexpected error; exits 0 on success.
  - [ ] Top-of-file godoc block lists (a) what the example demonstrates, (b) the command to run it (`go run ./examples/publish`), (c) the env vars it reads, (d) any topology side-effects on the broker.
  - [ ] `go build ./examples/...` is green on the unit lane (no broker required).
  - [ ] An integration test (`examples/publish/example_integration_test.go`, build tag `integration`) spins up the same testcontainer the rest of the integration suite uses, runs the example as a subprocess (`exec.Command("go", "run", ".")`) against it with `AMQP_URL` injected, and asserts (a) exit code 0; (b) the test-side consumer attached to the example's queue receives the expected payload exactly once; (c) `goleak.VerifyNone(t)` is clean after the subprocess exits.
  - [ ] CI workflow (`.github/workflows/ci.yml`) runs `go build ./examples/...` as a unit-lane step and the example integration tests as part of the `integration` lane. Failure in either blocks merge.
- **Verify:** `go build ./examples/...` locally; `go test -race -tags=integration ./examples/...` against a local broker; subprocess smoke-run produces the expected message; review the godoc header for clarity.
- **Files:** `examples/publish/main.go`, `examples/publish/example_integration_test.go`, edits to `.github/workflows/ci.yml`, `Makefile` (add `examples-build` and `examples-smoke` targets), `README.md` (link to the new example).
- **Deps:** T13. (Strong pairing — T13b cannot close before T13.)

### Checkpoint — Phase 2 done
- [ ] One end-to-end publish/consume-via-cli demo works.
- [ ] Mandatory + Returns integration test green.
- [ ] **`examples/publish/main.go` builds (unit lane) and smoke-runs end-to-end (integration lane)** per T13b — SPEC §7 + Rev decision 49.
- [ ] **Review with human before Phase 3.**

---

## Phase 3 — Topology: declared once, separately

### [x] T14 — Topology types · S
Rev 5 adds `Queue.DeliveryLimit` and `Queue.SingleActiveConsumer`.
- **Acceptance:**
  - [ ] `Topology`, `Exchange`, `Queue`, `Binding`, `DeadLetter` struct types from SPEC §6.6 — each with the **`NoWait bool`** field where applicable.
  - [ ] `Queue.Type QueueType` field; `QueueType` constants moved out of T02 are consumed here.
  - [ ] **`Queue.DeliveryLimit int`** — broker-enforced redelivery cap on quorum queues; maps to `x-delivery-limit`. Zero = unbounded.
  - [ ] **`Queue.SingleActiveConsumer bool`** — maps to `x-single-active-consumer`.
  - [ ] `DeadLetter` has `MaxLengthBytes int` and `Overflow OverflowPolicy` in addition to TTL/MaxLength.
  - [ ] `ExchangeKind` (Direct, Fanout, Topic, Headers, Delayed) and `OverflowPolicy` (from T02) consumed and used.
  - [ ] Validation helper `(*Topology).validate()` rejects (Rev 6 strengthened — each rule returns `ErrInvalidOptions` with a specific message):
    - Empty names on `Exchange`, `Queue`, `Binding`.
    - Unknown `ExchangeKind`, `QueueType`, `OverflowPolicy`.
    - **`DeliveryLimit > 0` on a non-quorum queue.**
    - **`SingleActiveConsumer=true` on a stream queue.**
    - **`Type=QueueTypeStream` combined with `Exclusive=true`, `AutoDelete=true`, or a `MaxPriority` arg.**
    - **`Type=QueueTypeStream` with a `DeadLetter` entry targeting it as `Source`** (streams do not dead-letter).
    - **`Binding.RoutingKey != ""` on a binding to a fanout exchange** (silently ignored by broker; reject for clarity).
    - **`Exchange.Kind=ExchangeDelayed` without `Args["x-delayed-type"]`** set to a valid kind.
    - **Duplicate names within a slice** (two `Queue{Name: "orders"}`).
- **Verify:** Unit tests for validation (each rule above gets a happy + unhappy case).
- **Files:** `topology.go` (types only at this stage), `topology_test.go`.
- **Deps:** T02.

### [x] T15 — `Topology.Declare` idempotent + mismatch · M
SPEC compliance: `Declare` is a **two-step pipeline** — an in-memory
expansion happens *before* any broker call, then the broker sees a
single declare sequence in fixed order. AMQP 0-9-1 rejects
`queue.declare` with non-matching args (`PRECONDITION_FAILED` /406),
so DLX args cannot be added to an already-declared queue via
re-declare; they must be present the first time. Rev 5 generalizes
the expansion to also inject `x-delivery-limit`,
`x-single-active-consumer`, and `x-queue-type`, and mandates that
the expansion mutates a **copy** so the caller's `Topology` value
stays untouched.
- **Acceptance:**
  - [ ] **Step 1 (in-memory, copy-on-mutate):** `Topology.Declare` first deep-copies the input `Topology`, then runs a pre-pass that, for each affected queue:
    - For every `DeadLetter{Source, Exchange, RoutingKey, TTL, MaxLength, MaxLengthBytes, Overflow}` matching the queue: merges `x-dead-letter-exchange`, `x-dead-letter-routing-key`, `x-message-ttl`, `x-max-length`, `x-max-length-bytes`, `x-overflow` into the source `Queue.Args` (creating the map if nil); appends a default DLX `Exchange{Name, Kind: ExchangeTopic, Durable: true}` to `Exchanges` if the user did not declare one with that name; appends a DLQ `Queue{Name: "<Source>.dlq", Durable: true}` to `Queues` if the user did not declare one.
    - For every queue with `DeliveryLimit > 0`: injects `x-delivery-limit=<n>`.
    - For every queue with `SingleActiveConsumer == true`: injects `x-single-active-consumer=true`.
    - For every queue with `Type != ""`: injects `x-queue-type=<value>`.
    - For every queue with `Type == QueueTypeQuorum`: injects `x-dead-letter-strategy=at-least-once`.
    A unit test snapshots the in-memory `Topology` before and after the pre-pass and asserts the expected mutations. A second unit test asserts the caller's original `Topology` value is **unchanged** after `Declare` returns.
  - [ ] **Step 2 (broker):** `Declare` opens a temporary channel and emits frames in the order **exchanges → queues → bindings**. Order is asserted by intercepting AMQP calls (e.g., wrapping the channel with a recorder) in a unit test. The source queue is declared **exactly once**, carrying its full arg set from step 1.
  - [ ] Conflicting `Durable` / args returns `ErrTopologyMismatch`, which itself wraps `ErrPreconditionFailed` (so `errors.Is(err, ErrPreconditionFailed)` is also true).
  - [ ] `QueueTypeQuorum` results in `x-queue-type=quorum` on declare; `DeliveryLimit=5` on the same queue results in `x-delivery-limit=5`.
  - [ ] Same `Topology.Declare` called twice = no error.
  - [ ] **`Topology.Declare` is not concurrency-safe with itself or with `AttachTo`.** Godoc explicitly says so. Recommended pattern (`sync.Once` at app level) documented.
  - [ ] **`NoWait=true` caveat.** When *any* `Exchange`, `Queue`, or `Binding` in the topology sets `NoWait=true`, mismatch detection for that entry is asynchronous: `Declare` returns `nil`, and a subsequent operation on the channel fails with a wrapped `ErrPreconditionFailed`. This is documented in the godoc on the `NoWait` field and surfaced as a separate regression test (declare a queue with `NoWait=true, Durable=true` against a broker that already has `Durable=false`; assert `Declare` returns `nil` and the next `Health` call returns `ErrPreconditionFailed`).
- **Verify:** Integration tests covering happy path, mismatch (assert both `errors.Is(err, ErrTopologyMismatch)` AND `errors.Is(err, ErrPreconditionFailed)`), DLX expansion (assert via `rabbitmqctl list_queues -p / name arguments` that the source queue carries the DLX args **on its first declare**, never via a re-declare), quorum-queue declare with `DeliveryLimit` (assert `x-delivery-limit` is visible via `rabbitmqctl`), single-active-consumer declare, and the `NoWait` async-mismatch path.
- **Files:** edits to `topology.go`, `topology_test.go`, `topology_integration_test.go`.
- **Deps:** T07b, T14.

### [x] T16 — `Topology.AttachTo` reconnect redeclare + barrier + degraded state · M
Rev 6 grows the contract: deep snapshot, synchronous barrier
integration with T07, persistent-failure degraded state.
- **Acceptance:**
  - [ ] `AttachTo(conn)` registers a **deep snapshot** (deep-copied at call time) as a redeclare callback via `Connection`'s reconnect supervisor; subsequent mutations to the caller's `Topology` value do NOT affect the registered redeclare. Re-`AttachTo` to register a fresh snapshot.
  - [ ] **Snapshots are keyed by the pointer address of the input `*Topology`** (Rev 7, disambiguating SPEC §6.6 line 1565 "keyed by topology identity at the AttachTo call site"). Calling `AttachTo(t)` a second time with the **same pointer** replaces the prior snapshot for that key (used for "I edited my topology and want the new shape on the next reconnect" — note: the snapshot is still deep-copied at re-`AttachTo` time, so the caller may freely mutate after the call). Calling `AttachTo(other)` with a **different pointer** appends an additional snapshot; both registered snapshots fire on every reconnect in registration order (used for composing topology fragments declared by different subsystems). Unit-test matrix: (1) same-pointer replace — `AttachTo(t)`; mutate `t.Queues[0].Name`; `AttachTo(t)`; force reconnect; assert the redeclare uses the mutated value, not the original; (2) different-pointer append — `AttachTo(t1)`; `AttachTo(t2)`; force reconnect; assert both `t1` and `t2` are redeclared in that order via a channel recorder.
  - [ ] **Synchronous reconnect barrier integration (T07).** Redeclare runs inside the barrier in step 2; until step 2 completes, `Publisher.Publish` routed to that connection blocks on `ErrReconnecting`. Unit test asserts ordering via a recorder: `Publish` calls during reconnect see `ErrReconnecting` on `ctx` cancel, then succeed once barrier clears.
  - [ ] **Degraded-state machine.** If redeclare returns `ErrTopologyMismatch` / `ErrPreconditionFailed` / any other error after `n` configured retries within the barrier, the supervisor transitions the connection to `degraded` state: subsequent `Publish` returns `ErrTopologyRedeclareFailed` (permanent), consumers do NOT re-issue `basic.consume`. `connection_degraded_total{role, reason}` increments once per transition; `WithOnTopologyDegraded(func(error))` fires once per transition. On the next reconnect cycle (auto or via `ForceReconnect`), redeclare is retried; first success clears the flag and resumes traffic.
  - [ ] **Mandatory histogram `topology_redeclare_seconds{role}`** records the duration of step 2 per reconnect cycle (both success and failure).
  - [ ] **Snapshot test:** mutate `t.Queues = append(t.Queues, ...)` AFTER `AttachTo(t)`; force a reconnect; assert the broker side does NOT see the post-mutation queue.
- **Verify:** Integration test: declare, disconnect broker, reconnect, assert queue exists with correct args; ordering test: register both `AttachTo` and a `WithOnReconnect` callback; assert the callback observes the queue already declared. **Degraded-state test:** declare a queue with `Durable=true`, then change the spec to `Durable=false` and force reconnect; assert `Publish` returns `ErrTopologyRedeclareFailed`, `connection_degraded_total` == 1, `WithOnTopologyDegraded` fired once. Recover by reverting the spec and calling `ForceReconnect`; assert flag clears.
- **Files:** edits to `topology.go`, `topology_attach_integration_test.go`, `topology_degraded_integration_test.go`, `topology_snapshot_test.go`.
- **Deps:** T15, T07.

### [x] T16b — Checkpoint examples: `examples/topology/main.go` + `examples/deadletter/main.go` · S
Two examples land together (per SPEC §7 "Executable examples at
checkpoints" + §10 Rev decision 49): one for the bare `Topology`
flow, one for the DLX + quorum-queue flow that hardens production
workloads.
- **Acceptance:**
  - [x] `examples/topology/main.go` is `package main`, reads `AMQP_URL`, builds a `Topology` with two exchanges + three queues + four bindings, calls `Topology.Declare(ctx, conn)`, calls it again to demonstrate idempotency (no error, no broker mutation), then calls `Topology.AttachTo(conn)` and forces a reconnect via `(*Connection).ForceReconnect()`; on reconnect the redeclare callback fires and the example prints "topology re-declared" before exiting 0.
  - [x] `examples/deadletter/main.go` is `package main`, reads `AMQP_URL`, declares a `QueueTypeQuorum` source queue with `DeliveryLimit(3)` and a `DeadLetter{Exchange, RoutingKey, TTL, MaxLength, Overflow: OverflowRejectPublishDLX}` entry; publishes one message; runs a consumer that always errors so the message dead-letters; the example then opens an inspection consumer on the DLQ and prints the DLX-bounced payload + the `x-death` header before exiting 0.
  - [x] Top-of-file godoc blocks on both files follow the same shape as T13b (purpose, run command, env vars, broker side-effects).
  - [x] `go build ./examples/...` green on the unit lane.
  - [x] Integration test per example (`example_integration_test.go` in each subdir, `integration` build tag) spins up a testcontainer, runs the example as a subprocess, and asserts (a) exit 0; (b) for `topology/`: the declared queue is visible via `rabbitmqctl list_queues name arguments` after the example exits (test cleans up afterwards); (c) for `deadletter/`: a message lands in the configured DLQ and carries the expected `x-death` header; (d) `goleak.VerifyNone(t)` clean after each subprocess exits.
- **Verify:** `go build ./examples/...`; `go test -race -tags=integration ./examples/topology/... ./examples/deadletter/...`; manual subprocess smoke-run against a local broker.
- **Files:** `examples/topology/main.go`, `examples/topology/example_integration_test.go`, `examples/deadletter/main.go`, `examples/deadletter/example_integration_test.go`, edits to `README.md`.
- **Deps:** T16. (`deadletter/` also depends on T15 for DLX expansion semantics — already a prerequisite of T16.)

### Checkpoint — Phase 3 done
- [x] Topology declare idempotent under repeat.
- [x] Mismatch detected and surfaced.
- [x] AttachTo re-declares cleanly after broker restart.
- [x] **`examples/topology/main.go` and `examples/deadletter/main.go` build (unit lane) and smoke-run end-to-end (integration lane)** per T16b — SPEC §7 + Rev decision 49.
- [ ] **Review with human before Phase 4.**

---

## Phase 4 — Consumer: error-driven semantics + escape hatch

### [x] T17 — `delivery.go`: concrete `Delivery[M]` · S
- **Acceptance:**
  - [x] `Delivery[M]` struct with all methods listed in SPEC §6.3 (`Body`, `Headers`, `Redelivered`, `DeliveryTag`, `DeathCount`, **`DeathCountByReason`**, **`DeathReasons`**, `MessageID`, `CorrelationID`, `Timestamp`, `Ack`, `Nack`, `AckIf`).
  - [x] `DeathCount()` parses the AMQP `x-death` header — which is a **field-array (`[]any`) of field-tables (`amqp091.Table` / `Headers`)**, one entry per dead-letter event. The parser sums the `count` (int64 in the wire) across all entries whose `queue` field matches the delivery's current queue **AND whose `reason` is one of `rejected` or `delivery-limit`** (Rev 6: filter out `expired` and `maxlen` which reflect broker policy rather than handler-driven rejection); returns 0 if the header is absent or shaped unexpectedly. A `FuzzXDeathParser` target exercises malformed inputs.
  - [x] `DeathCountByReason(reason string) int` and `DeathReasons() []string` (unique reasons in declaration order) expose the full parsed shape for custom policies (e.g. users who DO want to count `expired` for their workload).
  - [x] `AckIf(err error) error` implements the error-mapping semantics (nil → Ack; `errors.Is(err, ErrRequeue)` → `Nack(true)`; any other err → `Nack(false)`).
  - [x] `Ack` / `Nack` / `AckIf` return `ErrChannelClosed` when the underlying channel is closed and `ErrAlreadyClosed` when the consumer was closed; otherwise `nil` on success — documented behaviour.
  - [x] **Lens-09 (PC-08):** allocate the x-death `byReason` map lazily — `make(map[string]int)` ran before the `tbl==nil` / x-death-absent / `![]any` early returns (`xdeath.go:32`), allocating a map on 100 % of deliveries including the common no-DLX path. Fixed: the alloc now happens lazily on the first x-death entry matching the delivery's queue, after every early return → zero map alloc on the no-DLX delivery. Guarded by `TestParseXDeath_NoAllocOnNoDLXPath` (`AllocsPerRun == 0` across nil-table / x-death-absent / wrong-shape) + `TestParseXDeath_CountByReason_NilMapSafe` (nil-map read is safe). dep PG-2.
- **Verify:** Unit tests with hand-built `amqp091.Delivery` values + table-driven AckIf cases + closed-channel error path test + `x-death` parser test fixtures (absent, empty, single entry, multiple entries, mixed reasons `rejected`+`expired`+`delivery-limit`, wrong shape) + a `FuzzXDeathParser` fuzz target (per `plan.md` §"Fuzz targets"). Reason-discrimination test: a delivery with `x-death=[{reason: expired, count: 100}, {reason: rejected, count: 2}]` reports `DeathCount() == 2` (not 102).
- **Files:** `delivery.go`, `internal/headers/xdeath.go`, `delivery_test.go`, `internal/headers/xdeath_test.go`, `internal/headers/xdeath_fuzz_test.go`.
- **Deps:** T02, T09.

### [x] T18 — Consumer + builder + handler error mapping + re-subscribe + verdict + UUID-tag · M
Rev 5 adds `Priority(int)`, `HandlerTimeout(d)`, the re-subscribe
loop, and handler-ctx cancel on channel close. Rev 6 adds
`HandlerTimeoutVerdict(TimeoutVerdict)` (default
`TimeoutNackNoRequeue`), default consumer-tag `ctag-<uuidv7>`,
`Build`-time warning when `Prefetch < Concurrency`, and the
documented Concurrency-vs-ordering trade-off.
- **Acceptance:**
  - [ ] `ConsumerFor[M](conn) *ConsumerBuilder[M]` with the methods from SPEC §6.3 except `AutoAck` (T35) and `MaxRedeliveries` (T20).
  - [ ] Builder includes `ChannelQoS()` (RabbitMQ per-channel semantics) — **not** `GlobalQoS()`. No `NoLocal()` method (RabbitMQ ignores). `PrefetchBytes()` exists with godoc "no-op on RabbitMQ; preserved for protocol parity".
  - [ ] **`Priority(p int)`** sets `x-priority` in `basic.consume` args; documented for active/standby consumer topologies.
  - [ ] **`HandlerTimeout(d time.Duration)`** derives a per-message ctx with deadline `d`; on timeout, the handler ctx is cancelled and the configured **`HandlerTimeoutVerdict`** is emitted. Mandatory metric `consumer_handler_timeout_total{queue, verdict}` increments per occurrence (verdict label distinguishes `nack_no_requeue` vs `nack_requeue`).
  - [ ] **`HandlerTimeoutVerdict(v TimeoutVerdict)`** builder method (Rev 6, in scope for v0.1): default `TimeoutNackNoRequeue` (aligns Consumer with BatchConsumer and the "no silent poison loop" north star — Rev 5 had `Nack(true)` as default for Consumer and `Nack(false)` for BatchConsumer, which contradicted itself across SPEC §6.3 / §6.4 / TODO T18). Override to `TimeoutNackRequeue` for known-transient slowness workloads.
  - [ ] **`Build`-time warning** when `Prefetch < Concurrency`: log "consumer prefetch=N is below concurrency=M; handlers will stall waiting for deliveries". Not a hard error; the user may have a workload-specific reason.
  - [ ] **Default consumer-tag is `ctag-<uuidv7>`** when `Tag(string)` is left empty: generated at `Build` time, before connection pinning (so the hash distinguishes consumers correctly). User-supplied tags are passed through verbatim.
  - [ ] `Consumer.Consume(ctx, Handler[M])` decodes payload via codec, calls handler with decoded value.
  - [ ] Error mapping: `nil` → Ack; default error → `Nack(false)`; `errors.Is(err, ErrRequeue)` → `Nack(true)`.
  - [ ] Decode failure → `Nack(false)` (poison protection by default) + ConsumerMetrics counter increment (`outcome=decode_error`).
  - [ ] Concurrency: `Concurrency(n)` runs up to N handlers in parallel using a **non-blocking dispatcher** that drops to sequential mode when all worker slots are full to enforce `prefetch_count` correctly.
  - [ ] **Re-subscribe loop.** After a successful reconnect of the consumer connection that hosts this `Consumer[M]`, the consumer reopens its channel, reapplies `basic.qos` (with the configured `ChannelQoS` flag and prefetch), and reissues `basic.consume`. The consumer-tag is preserved across reconnects. Metric `consumer_resubscribed_total{queue}` increments exactly once per re-subscribe. A small bounded jitter (50–250ms) staggers parallel resubscribes after a broker restart to avoid storms.
  - [ ] **Handler ctx cancel on channel close.** When the consumer's channel closes mid-handler, the handler's `context.Context` is cancelled with cause `ErrChannelClosed`. Metric `consumer_handler_aborted_channel_closed_total{queue}` increments. The original message will be redelivered by the broker (the ack was never received).
  - [ ] Broker-originated errors during consume (channel close 404, 405, etc.) are translated via `internal/amqperror` and surface as wraps of the right sentinel.
  - [ ] Codec calls are wrapped in `defer recover` → `ErrInvalidMessage` (per T09 contract).
  - [ ] **Functional-options last-wins on `ConsumerBuilder`** (Rev 7, per SPEC §6.1 line 515). Unit-test matrix: (a) `.Concurrency(2).Concurrency(8).Build()` produces a consumer running 8 handlers in parallel (assert via in-flight gauge under load); (b) `.HandlerTimeout(50*time.Millisecond).HandlerTimeout(0).Build()` disables the timeout (no `consumer_handler_timeout_total` increment under a slow handler); (c) `.Codec(jsonStrict).Codec(jsonLax).Build()` decodes a payload with an unknown field successfully (lax wins); (d) `.HandlerTimeoutVerdict(TimeoutNackRequeue).HandlerTimeoutVerdict(TimeoutNackNoRequeue).Build()` plus `.HandlerTimeout(50ms)` lands the timed-out message in the DLX, not the source.
  - [ ] **Lens-08 (CR-06):** §6.3 states the non-blocking dispatcher's **sole** bound is `prefetch` (the `out` channel is prefetch-sized, `consumer.go:487`); there is **no second queue**; the reader blocks when the buffer is full (that *is* the backpressure); `basic.cancel`/channel-close stay observable via the closed deliveries channel even when all `n` handler slots are busy; a test asserts the dispatch buffer == prefetch and `basic.cancel` is observed with all slots busy.
  - [ ] **Lens-09 (PC-10):** amend §6.3 to state the consume scaling lever — one consumer = one channel = one reader on one TCP, so beyond the per-TCP I/O ceiling raising `Concurrency` alone does not add throughput; scale needs more consumer channels/connections (`WithConsumerConnections` / more consumers). (The new §9 consume-side throughput target + latency SLO is the capstone T149.)
- **Verify:** Integration test sending good + bad payloads + handlers that return each of the three result classes. **`ChannelQoS()` is verified at the wire level** via a channel recorder that captures the `basic.qos` frame and asserts the `global` bit is `true` when `ChannelQoS()` is set and `false` otherwise. A second, longer-running integration test reuses the recorded channel via package-private accessors to attach a second raw `Consume` and asserts that the prefetch budget is shared rather than doubled — flagged as a conformance probe, not part of the public-API surface.

  **Re-subscribe regression test:** start a consumer, kill its underlying TCP connection via the testcontainer driver, wait for reconnect, assert `consumer_resubscribed_total{queue}` == 1 and a fresh publish lands in the handler. `goleak.VerifyNone` clean.

  **Handler-ctx cancel test:** handler that blocks on `<-ctx.Done()`; close the underlying channel forcibly; assert handler returns within 100ms and `consumer_handler_aborted_channel_closed_total` == 1.

  **`HandlerTimeout` smoke test (default verdict):** `HandlerTimeout(50ms)` with a handler that `time.Sleep(200ms)`; default `HandlerTimeoutVerdict = TimeoutNackNoRequeue` (Rev 6); assert handler ctx is cancelled around 50ms; assert the message lands in the configured DLX (not requeued on the source). Full matrix is in T18b.

  **`Priority` test:** declare a quorum queue, start consumer A with `Priority(10)` and consumer B with `Priority(0)`; publish 10 messages; assert all 10 land on A while it's alive; kill A, assert remaining deliveries land on B.
- **Files:** `consumer.go`, `consumer_builder.go`, `consumer_test.go`, `consumer_integration_test.go`, `consumer_qos_conformance_test.go`, `consumer_resubscribe_integration_test.go`, `consumer_handler_ctx_integration_test.go`, `consumer_handler_timeout_integration_test.go`, `consumer_priority_integration_test.go`.
- **Deps:** T07b, T07d, T08, T09, T17.

### [x] T18b — `HandlerTimeoutVerdict` matrix test · S
Rev 6 explicit test for the new builder method (T18 ships the
mechanism; T18b is the dedicated test matrix to make the trade-off
visible).
- **Acceptance:**
  - [x] Test case A — `TimeoutNackNoRequeue` (default): `HandlerTimeout(50ms)` + handler that blocks on `ctx.Done`; assert (1) handler ctx cancelled around 50ms; (2) message appears in the configured DLX queue (polled, no fixed sleep); (3) `consumer_handler_timeout_total` == 1 and `consumer_handler_seconds{outcome="timeout_nack_no_requeue"}`; (4) no redelivery on the source queue. Uses explicit topology with binding (workaround for LATER-20).
  - [x] Test case B — `TimeoutNackRequeue` opt-in: builder calls `HandlerTimeoutVerdict(TimeoutNackRequeue)`; assert (1) `deliveryCount >= 2` (at least one redelivery); (2) all `consumer_handler_seconds` outcome labels are `"timeout_nack_requeue"`.
  - [ ] Test case B3 — `x-delivery-limit` exhaustion (deferred — see LATER-21): declare a quorum queue with `x-delivery-limit: 3`; assert the message is dead-lettered after exactly 3 deliveries.
- **Verify:** `go test -tags=integration -run TestHandlerTimeoutVerdict ./...` green; `goleak.VerifyNone` clean.
- **Files:** `consumer_handler_timeout_verdict_integration_test.go`, `integration_helpers_test.go`.
- **Deps:** T18, T15.

### [x] T19 — `ConsumerMetrics` + Prometheus + wiring · S
Rev 5 promotes `consumer_resubscribed_total`,
`consumer_handler_aborted_channel_closed_total`, and
`consumer_handler_timeout_total` to mandatory metrics. Rev 6 adds
`consumer_cancelled_total`, `topology_redeclare_seconds`,
`publisher_retry_total`, `replier_drop_no_dlx_total`,
`connection_degraded_total` (covered in T07/T16/T13/T30 wiring; T19
asserts they all land in the registry).
- **Acceptance:**
  - [x] `metrics.ConsumerMetrics` interface defined per SPEC §6.9: handle latency histogram, ack/nack/requeue/decode_error/handler_timeout/resubscribed/aborted/cancelled counters, in-flight gauge.
  - [x] Prometheus impl in `metrics/prometheus.go` uses bounded default labels `{queue, outcome}`; high-cardinality labels (`routing_key`, `message_type`) opt-in.
  - [x] Consumer instruments handler invocation, ack/nack, decode error paths, **re-subscribe events**, **channel-close handler aborts**, **handler timeouts** (with verdict label), **basic.cancel events**.
  - [x] Histogram buckets default to SPEC §6.9 set; configurable via `WithLatencyBuckets`.
  - [x] **All Rev 6 mandatory metrics present in the Prometheus registry after a canned workload:** `publisher_retry_total{exchange, reason}`, `consumer_cancelled_total{queue, reason}`, `replier_drop_no_dlx_total{queue}`, `topology_redeclare_seconds{role}` (histogram), `connection_degraded_total{role, reason}`, plus the Rev 5 mandatory set.
- **Verify:** Integration test scrapes a `prometheus.Registry` and asserts each mandatory metric (Rev 5 + Rev 6) exists with the documented labels after a canned workload that exercises every outcome — including a forced reconnect, a forced redeclare failure, a forced `basic.cancel`, and a `PublishRetry`-triggered nack.
- **Files:** edits to `metrics/metrics.go`, `metrics/prometheus.go`, `consumer.go`.
- **Deps:** T18, T04.

### [x] T20 — `MaxRedeliveries` enforcement with quorum carve-out · S
SPEC compliance: AMQP 0-9-1 only writes `x-death` on dead-letter
events (TTL, length limit, `Nack(requeue=false)`) — **not** on
`Nack(requeue=true)`. Bounding an `ErrRequeue` loop with `x-death`
alone is impossible. Rev 5 introduces the quorum-queue
`x-delivery-limit` carve-out: when the source queue is quorum with
`DeliveryLimit > 0`, the broker bounds redeliveries natively and
the consumer-side counter B is auto-disabled.
- **Acceptance:**
  - [ ] Builder method `MaxRedeliveries(n int)` (default 0 = unbounded; user opts in).
  - [ ] **Counter A (cross-process, via `x-death`).** When `DeathCount() > n`, the consumer forces `Nack(false)` and emits `ErrMaxRedeliveries` via metrics + log without invoking the handler. Bounds loops that bounce through a DLX-back-to-source binding and survive consumer restarts.
  - [ ] **Counter B (in-process, keyed by `(channel-instance-id, MessageID)`).** A `sync.Map`-backed counter (or equivalent — must be race-free; verified with `go test -race`) keyed by `(channel-instance-id, Delivery.MessageID)`. Falls back to `(channel-instance-id, consumer-tag, delivery-tag)` when `MessageID` is empty. The **channel-instance-id is a UUID generated per consumer channel and reset on channel close**, so delivery-tags reused across reconnects cannot collide. Each `ErrRequeue`-driven `Nack(requeue=true)` increments the counter; once incrementing it would exceed `n`, the consumer rewrites the verdict to `Nack(requeue=false)` and emits `ErrMaxRedeliveries`. The counter entry is deleted on `Ack` or `Nack(false)`, and the entire map drops on channel close.
  - [ ] **Quorum carve-out.** When the source queue at `Build()`-time has `Queue.Type == QueueTypeQuorum && Queue.DeliveryLimit > 0` (introspected via the topology hint or the queue's args), counter B is **auto-disabled** (broker is authoritative). Counter A still runs as a safety net. Godoc and a debug log line document the disable.
  - [ ] Metric/log field `cause` distinguishes the three paths: `cause=delivery-limit` (broker, quorum), `cause=x-death` (counter A), `cause=in-process` (counter B).
  - [ ] Consumer godoc documents that counter B is **process-local**: a restart resets it. Users wanting cross-process bounding must use a quorum queue with `DeliveryLimit > 0` (preferred) or configure a DLX-back-to-source binding (counter A then takes over via `x-death`).
  - [x] **Lens-08 (CR-02, Blocker):** counter B's `load`→`Store` in `applyCounterB` was a **non-atomic read-modify-write** with no lock between — under `Concurrency(n>1)` two handler goroutines processing redeliveries of the same key (at-least-once duplicates sharing a `MessageId`) both read then both write, losing an increment, so `MaxRedeliveries` undercounted and a poison message could loop past its limit. Because `sync.Map` is memory-safe, `go test -race` **cannot** catch this logical lost-update, so the "verified with `go test -race`" acceptance was a **false guarantee**. Fixed: `redeliveryCounter` now carries a `sync.Mutex` held across the whole load→check→store/delete in `applyCounterB` (the delete-on-Ack/Nack(false) path takes it too), making the RMW atomic; metric/log side effects are emitted after releasing the lock. Verified by a **behavioural** N-goroutine-same-key test (`TestConsumer_MaxRedeliveries_CounterB_AtomicUnderConcurrency`, 500 goroutines): asserts final count == N — which FAILED on the old code (~50/500 survived) and passes now under `-race -count=20`. *dep T143 (CG-1).*
- **Verify:**
  - Poison-loop integration test (in-process counter B, classic queue): handler always returns wrapped `ErrRequeue`; assert at most `n+1` deliveries within a single consumer run, and that the `(n+1)`-th nack is `requeue=false`. Asserts `cause=in-process` in the metric label.
  - **Quorum-queue test (broker-enforced):** declare a quorum queue with `DeliveryLimit=5`, set `MaxRedeliveries(10)`; handler always returns wrapped `ErrRequeue`; assert exactly 6 deliveries before the broker dead-letters; metric label is `cause=delivery-limit`; counter B map size is 0 throughout (auto-disabled).
  - DLX-bounce integration test (counter A): set up `Source → DLX → Source` ping-pong, handler always returns a plain error (drives `Nack(false)`), assert `DeathCount()` increments on each loop and short-circuit fires at exactly `n+1`; metric label `cause=x-death`.
  - **Channel-instance-id key reset test:** drive counter B for `MessageID=foo` up to `n-1`, force a channel close + reconnect, send another delivery with `MessageID=foo`; assert counter B treats it as new (reset on channel close).
  - **Map leak stress test:** publish 1M `ErrRequeue`-then-final-ack cycles; assert counter B map size returns to 0 at the end (`unsafe.Sizeof`+reflection or a private accessor for the test).
  - Restart test: run counter B to `n`, restart the consumer, send the same `MessageID` again, assert counter B resets (documented behaviour).
- **Files:** edits to `consumer.go`, plus `consumer_maxredeliveries_inproc_integration_test.go`, `consumer_maxredeliveries_quorum_integration_test.go`, `consumer_maxredeliveries_dlx_integration_test.go`, `consumer_maxredeliveries_restart_integration_test.go`, `consumer_maxredeliveries_leak_test.go`.
- **Deps:** T17, T18.

### [x] T21 — `ConsumeRaw` + `Delivery.AckIf` polish · S
- **Acceptance:**
  - [x] `Consumer.ConsumeRaw(ctx, RawHandler[M])` available; handler receives `*Delivery[M]`.
  - [x] Raw handler is responsible for Ack/Nack — consumer does not auto-ack.
  - [x] Integration test exercises `Redelivered()`, `Headers()`, `DeathCount()`.
- **Verify:** Integration test.
- **Files:** edits to `consumer.go`, `consumer_raw_integration_test.go`.
- **Deps:** T18.

### [x] T21b — Checkpoint example: `examples/consume/main.go` · S
Per SPEC §7 "Executable examples at checkpoints" + §10 Rev
decision 49. Lands together with the rest of the Phase 4
acceptance to close the checkpoint.
- **Acceptance:**
  - [x] `examples/consume/main.go` is `package main`, reads `AMQP_URL`, declares topology in-process (one exchange + one queue + one DLX), spawns a `PublisherFor[Order]` that publishes 5 messages (good + bad + slow + transient + poison), and runs a `ConsumerFor[Order]` with `Concurrency(4)`, `Prefetch(8)`, `MaxRedeliveries(3)`, `HandlerTimeout(2*time.Second)`, and a payload-first handler that demonstrates each of the three result classes (`nil` → Ack; default error → `Nack(false)`; `errors.Join(err, ErrRequeue)` → `Nack(true)`). The example logs each verdict and exits 0 after observing the expected mix on the source + DLX queues.
  - [x] Top-of-file godoc block follows the T13b shape.
  - [x] `go build ./examples/...` green on the unit lane.
  - [x] Integration test (`examples/consume/example_integration_test.go`, `integration` tag) runs the example as a subprocess against a testcontainer, with `AMQP_URL` injected; asserts (a) exit 0; (b) the source queue is empty after the example exits; (c) one message is observable on the DLX queue (the always-erroring one, after `MaxRedeliveries` exhaustion); (d) `goleak.VerifyNone(t)` clean.
  - [x] If T21b lands before T19 (consumer metrics), the example skips the metrics assertion and the integration test marks that section as `t.Skip("metrics arrive in T19")`. (Acceptable because T19 ships in the same phase.)
- **Verify:** `go build ./examples/...`; `go test -race -tags=integration ./examples/consume/...`; manual subprocess smoke-run.
- **Files:** `examples/consume/main.go`, `examples/consume/example_integration_test.go`, edits to `README.md`.
- **Deps:** T18, T20, T21. (Strong pairing — T21b cannot close until T18/T20/T21 land.)

### Checkpoint — Phase 4 done
- [x] Error-driven semantics validated for all three classes.
- [x] Poison-loop bounded.
- [x] Escape hatch usable for raw envelope inspection.
- [x] **`examples/consume/main.go` builds (unit lane) and smoke-runs end-to-end (integration lane)** per T21b — SPEC §7 + Rev decision 49.
- [ ] **Review with human before Phase 5.**

---

## Phase 5 — Batch APIs: throughput

### [x] T22 — `PublishBatch` always-all + MaxSize cap + order preservation + channel-close recovery doc · M
Rev 5 enforces `PublishBatchMaxInFlight` (default 1024) returning
`ErrBatchTooLarge`, and pipelines on a **single channel** to
preserve input order. Rev 6 renames to `PublishBatchMaxSize`,
documents the channel-close recovery contract, and clarifies
`PublishRetry` does NOT apply to batches.
- **Acceptance:**
  - [x] `Publisher.PublishBatch(ctx, []Message[M]) ([]PublishResult, error)` publishes every input message (never short-circuits, except the size-cap guard below).
  - [x] **Size-cap guard:** if `len(msgs) > PublishBatchMaxSize`, returns immediately with `(nil, ErrBatchTooLarge)`. No channel work performed. Caller chunks.
  - [x] **Single-channel pipelining:** all N publishes occur on **one** acquired channel from the publisher pool, so RabbitMQ's per-channel ordering guarantee makes input order = consume order. Documented as a hard guarantee in godoc.
  - [x] Returns `[]PublishResult` with one slot per input; per-message error in `Result.Err` (`ErrInvalidMessage`, `ErrPublishNacked`, `ErrUnroutable`, `ErrChannelClosed`).
  - [x] Overall error wraps `ErrPartialBatch` if any failed.
  - [x] Pipelines all publishes, then waits one confirm window — including correctly resolving a single `multiple=true` ack that covers many delivery tags (see T11) and a single `multiple=true` nack that covers many delivery tags with `ErrPublishNacked`.
  - [x] **Channel-close recovery contract documented in godoc** (per SPEC §6.2): "Per-message `ErrChannelClosed` does NOT distinguish 'broker persisted' from 'broker did not receive'. Retry produces duplicates when the broker persisted but the ack was lost. `PublishRetry` does NOT apply to `PublishBatch` — chunking and partial-retry are the caller's responsibility, because the right strategy is workload-specific. Consumers MUST be idempotent per §6.2.1."
  - [x] **Lens-09 (PC-13):** document the `PublishBatchMaxSize=1024` memory/throughput trade-off + sizing guidance (a deeper window = more pipelining vs more tracker memory held per call); the per-call cap is decision 31 (not a sliding in-flight window) — do not reopen it.
- **Verify:**
  - **Always-all integration test:** 1000 messages, 3 deliberately invalid via client-side rejection (Headers with `chan int`); the remaining 997 traverse normally, get confirmed, and the batch returns 997 nil + 3 `ErrInvalidMessage` per-message results plus an overall error wrapping `ErrPartialBatch`. The channel stays open across the batch.
  - **`ErrBatchTooLarge`:** publish 2000 messages with default `PublishBatchMaxSize=1024`; assert `(nil, ErrBatchTooLarge)` is returned immediately; no broker work observed (channel recorder snapshot empty for that call).
  - **Order preservation:** publish 100 messages with sequential bodies `[0..99]`, consume into a single-consumer single-channel sink; assert the consumed order matches the published order exactly. Bounded by the per-channel ordering guarantee.
  - **Channel-close mid-batch chaos test:** publish 500 messages; force a channel close after ~100 have been written but before any confirm arrives; assert (a) `PublishResult.Err` is `ErrChannelClosed` for the affected indices; (b) the overall error wraps `ErrPartialBatch`; (c) no `PublishRetry` invocation regardless of policy configured (validates the "PublishRetry does not apply to batch" contract).
- **Files:** edits to `publisher.go`, `publisher_batch_integration_test.go`, `publisher_batch_order_integration_test.go`.
- **Deps:** T11, T12, T13.

### [x] T23 — `BatchConsumer` + concrete `Batch[M]` + auto-verdict · M
Rev 5 documents the handler error semantics (auto-Ack/Nack with
`multiple=true`) and `HandlerTimeout` at batch granularity.
- **Acceptance:**
  - [x] `BatchConsumerFor[M](conn) *BatchConsumerBuilder[M]`.
  - [x] Builder methods mirror `ConsumerBuilder` + `Size(uint)` + `FlushAfter(d)` + `HandlerTimeout(d)`. **No `Concurrency` exposed** — batches run sequentially per consumer (run multiple `BatchConsumer[M]` for parallelism).
  - [x] `Batch[M]` concrete struct with `Messages()`, `Deliveries()`, `Ack()`, `Nack(requeue)`. Internally tracks an `acked bool` guard so manual + auto don't double-act.
  - [x] **Auto-verdict semantics:**
    - Handler returns `nil` and `Batch.Ack/Nack` never called → framework emits a **single `basic.ack` with `multiple=true`** for the highest delivery-tag in the batch (one frame, not N).
    - Handler returns non-nil error wrapped with `ErrRequeue` → framework emits a single `basic.nack` with `multiple=true` + `requeue=true`.
    - Handler returns any other non-nil error → framework emits a single `basic.nack` with `multiple=true` + `requeue=false` (DLX-bound).
    - Handler called `Batch.Ack` / `Batch.Nack` / per-`Deliveries()` acks/nacks → framework skips the auto-verdict (idempotent guard).
  - [x] `HandlerTimeout(d)` derives a per-batch ctx; on timeout the default verdict is `Nack(requeue=false)` for the whole batch (`ErrPartialBatch`-style aggregate not applicable here — it's a batch verdict, not per-message).
  - [x] Flush triggers: size reached OR timer elapsed.
  - [x] `MaxRedeliveries` counter B (from T20) increments per message in the batch when a `Nack(requeue=true)` is emitted for the whole batch.
- **Verify:**
  - [x] Integration test: send 500 messages with `Size(100)` → 5 batches; send 50 messages with `FlushAfter(1s)` → 1 batch after 1s.
  - [x] **Multiple=true ack test:** unit tests assert single `basic.ack` frame with `multiple=true` per nil-returning handler via fakeAcknowledger.
  - [x] **Auto-Nack test:** handler returns `errors.New("bad")`; assert single `basic.nack` with `multiple=true,requeue=false`.
  - [x] **Manual override test:** handler calls `batch.Deliveries()[0].Nack(true)` and returns nil; assert only the per-delivery nack lands, no auto-Ack on the batch.
- **Files:** `batch_consumer.go`, `batch_consumer_builder.go`, `batch_consumer_integration_test.go`, `batch_consumer_autoack_test.go`.
- **Deps:** T18.

### [x] T23b — Checkpoint examples: `examples/batch_publish/main.go` + `examples/batch_consume/main.go` · S
Per SPEC §7 "Executable examples at checkpoints" + §10 Rev
decision 49. Both files land in the same PR (or back-to-back PRs)
before Phase 5 can close.
- **Acceptance:**
  - [x] `examples/batch_publish/main.go` is `package main`, reads `AMQP_URL`, declares topology in-process, builds a `[]Message[Order]` of length 1000, demonstrates `PublishBatch` returning `[]PublishResult` with all-nil errors, and additionally demonstrates the `ErrBatchTooLarge` guard by attempting a batch of length 2000 against the default `PublishBatchMaxSize=1024` and printing the error class. Exits 0.
  - [x] `examples/batch_consume/main.go` is `package main`, reads `AMQP_URL`, runs a `BatchConsumerFor[Order]` with `Size(100)` + `FlushAfter(500*time.Millisecond)`, prints the batch size for each flush (demonstrating both flush triggers — by publishing 250 messages in two bursts, the first 200 trigger size-based flushes and the trailing 50 trigger a timer flush), and exits 0 once all 250 messages are observed.
  - [x] Top-of-file godoc on both files explicitly notes (per SPEC §6.2 / Rev 6 decision 43) that `PublishRetry` does NOT apply to `PublishBatch` and that consumers MUST be idempotent — a one-paragraph reminder linked to `examples/idempotent_consume/` (which lands in T38b).
  - [x] `go build ./examples/...` green on the unit lane.
  - [x] Integration test per example (`example_integration_test.go` in each subdir, `integration` tag) runs the example as a subprocess; asserts exit 0; for `batch_publish/` asserts 1000 messages reached the broker; for `batch_consume/` asserts the example's stdout contains both "flush-by-size" and "flush-by-timer" log lines. `goleak.VerifyNone(t)` clean.
- **Verify:** `go build ./examples/...`; `go test -race -tags=integration ./examples/batch_publish/... ./examples/batch_consume/...`; manual subprocess smoke-run.
- **Files:** `examples/batch_publish/main.go`, `examples/batch_publish/example_integration_test.go`, `examples/batch_consume/main.go`, `examples/batch_consume/example_integration_test.go`, edits to `README.md`.
- **Deps:** T22, T23.

### Checkpoint — Phase 5 done
- [ ] Bench documented: `PublishBatch` ≥ 5× `Publish` on local broker.
      *(Deferred by design to **T44b** — the throughput-benchmark suite,
      `deps: Phases 1–5, T37`. The `5×` gate is owned there, not in Phase 5;
      it requires the testcontainers helper (T37) + reference hardware.)*
- [x] BatchConsumer flushes on both triggers. *(Unit-tested:
      `TestBatchConsumer_SizeFlush` + `TestBatchConsumer_FlushAfterTimer`;
      integration `TestBatchConsumer_SizeFlush_integration` +
      `TestBatchConsumer_FlushAfterTimer_integration`.)*
- [x] **`examples/batch_publish/main.go` and `examples/batch_consume/main.go` build (unit lane) and smoke-run end-to-end (integration lane)** per T23b — SPEC §7 + Rev decision 49. *(Unit-lane build verified green; integration smoke-run runs on the `integration` CI lane via `example_integration_test.go` in each subdir — compiles under `-tags=integration`; not executed locally, no broker.)*
- [ ] **Review with human before Phase 6.** *(Human gate — pending.)*

---

## Phase 6 — Codecs + OTel observability

### [x] T24 — `codec/protobuf.go` · S
- **Acceptance:**
  - [x] `codec.NewProtobuf()` round-trips any `proto.Message`.
  - [x] `ContentType()` returns `application/x-protobuf`.
- **Verify:** Round-trip test with a representative `.proto`-generated type + fuzz target.
- **Files:** `codec/protobuf.go`, `codec/protobuf_test.go`, `codec/protobuf_fuzz_test.go`.
- **Deps:** T09.

### [x] T25 — `codec/cloudevents.go` — structured mode · S
- **Acceptance:**
  - [x] `codec.NewCloudEventsStructured()` encodes the full CloudEvent JSON envelope into the message body. Encode/Decode operate on the official SDK's `cloudevents.Event` (re-exported as `codec.CloudEvent`), delegating JSON serialization to the SDK event format.
  - [x] `ContentType()` returns `application/cloudevents+json`.
  - [x] `data` / `data_base64`, extensions, and `time` follow the CloudEvents JSON format spec (handled by the SDK).
- **Verify:** Round-trip test against representative CloudEvents v1.0 events (JSON data, binary `data_base64`, extensions, time).
- **Files:** `codec/cloudevents.go`, `codec/cloudevents_structured_test.go`.
- **Deps:** T09.

### [x] T26 — `codec/cloudevents.go` — binary mode · M
- **Acceptance:**
  - [x] `codec.NewCloudEventsBinary()` implements the CloudEvents AMQP Protocol Binding binary mode: `data` in the body, `datacontenttype` → AMQP content-type property, all other attributes/extensions → `cloudEvents:`-prefixed AMQP headers (official Go SDK default prefix).
  - [x] Decode reconstitutes the full `cloudevents.Event` from body + headers + content-type property.
  - [x] Interoperates with non-Go AMQP-1.0 CloudEvents clients via RabbitMQ's 0-9-1 ⇄ 1.0 header/property bridging.
  - [x] Introduces the optional `codec.HeaderCodec` interface (`EncodeWithHeaders`/`DecodeWithHeaders`, both carrying a `contentType`, embeds `Codec`); the binary codec's plain `Encode`/`Decode` reject use outside a header-aware publisher/consumer with `ErrInvalidMessage` (no silent attribute loss).
  - [x] Publisher (`encodeMsg`) and Consumer (`safeDecodeConsumer`, streaming + batch) detect `HeaderCodec` and route headers + content-type, so the codec works end-to-end.
- **Verify:** Round-trip + cross-encoding test (structured-encoded message decodes via binary decoder fails cleanly with `ErrInvalidMessage`) + an end-to-end publisher→consumer test (no broker) asserting `cloudEvents:`-prefixed headers, content-type property, and `data` body round-trip. Fuzz target `FuzzCodecCloudEventsBinary` feeds arbitrary body+headers into `DecodeWithHeaders` and asserts no panic.
- **Files:** edits to `codec/cloudevents.go`, `codec/codec.go` (`HeaderCodec`), `publisher.go`, `consumer.go`, `batch_consumer.go`; `codec/cloudevents_binary_test.go`, `codec/cloudevents_binary_fuzz_test.go`, `cloudevents_wiring_test.go`.
- **Deps:** T25.

### [x] T27 — OTel in Publisher · S
Per SPEC §6.9 "Tracing continuity post-mortem" + §10 #51 (Rev 8 item M).
- **Acceptance:**
  - [x] `Publisher.Publish` opens a span named `<exchange> publish` with messaging attributes from OTel semantic conventions.
  - [x] Span attributes match SPEC §6.9 for publish: `messaging.system="rabbitmq"`, `messaging.destination.name`, `messaging.operation.type=publish`, `messaging.message.id`, `messaging.message.conversation_id`, `messaging.message.body.size`, `network.peer.address`, `network.peer.port` (where applicable).
  - [x] Context is injected into the AMQP headers via `otel.Propagator` **before any frame is written**, so the propagated context travels as part of `basic.publish` and is therefore preserved through any DLX bounce. Caller-supplied `traceparent`/`tracestate` in `Message[M].Headers` win (last-wins) — the library does not overwrite explicit values.
  - [x] **Span outcome contract.** On termination, set `messaging.rabbitmq.outcome` to one of `ack`/`nack`/`return`/`timeout`/`too_large`/`pool_exhausted`/`blocked`/`error` (matches the `publisher_publish_seconds{outcome}` metric label). On every failure class set OTel status to `Error`, call `Span.RecordError(err)`, and set `error.type` to the sentinel name (`"ErrUnroutable"`, `"ErrConfirmTimeout"`, `"ErrPublishNacked"`, `"ErrMessageTooLarge"`, `"ErrChannelPoolExhausted"`, `"ErrConnectionBlocked"`). Encode errors set `error.type="ErrInvalidMessage"`.
  - [x] Span is `End()`ed in every termination path including panics (handled by the codec panic-safety `recover`); a test injects a panicking codec and asserts no open spans remain via the in-memory tracer.
- **Verify:** Integration test with an in-memory tracer asserts span name, attributes, `messaging.rabbitmq.outcome`, status code, and `error.type` across the full failure matrix (`ErrUnroutable`, `ErrConfirmTimeout`, `ErrPublishNacked`, `ErrMessageTooLarge`, encode error, pool-exhausted).
- **Files:** edits to `publisher.go`, `publisher_tracing_test.go`.
- **Deps:** T12, T13 (`ErrMessageTooLarge` from Rev 8), T05.

### [x] T28 — OTel in Consumer · S
Per SPEC §6.9 "Tracing continuity post-mortem" + §10 #51 (Rev 8 item M).
- **Acceptance:**
  - [x] Consumer extracts the parent context from AMQP headers (`traceparent`/`tracestate`) before invoking the handler — works on **direct deliveries and DLQ deliveries alike** (DLX preserves headers verbatim per SPEC §6.6). Implemented via the new `otel.Propagator.ExtractTo` (extracts into the handler ctx so trace continuity and cancellation/deadline coexist).
  - [x] A `<queue> process` span wraps each handler invocation with messaging attributes from OTel semantic conventions. **For `BatchConsumer`, the `<queue> process_batch` span creates one OTel Link per incoming message** via the new `otel.LinkingTracer` extension (`StartWithLinks`); a non-linking tracer falls back to `Start`.
  - [x] Span attributes for receive/process paths match SPEC §6.9 (`messaging.system`, `messaging.destination.name`, `messaging.operation.type=process`, `messaging.message.id`, `messaging.message.conversation_id`; batch adds `messaging.batch.message_count`).
  - [x] **Span outcome contract.** On handler return, set `messaging.rabbitmq.outcome` to one of `ack` (nil), `nack_requeue` (`ErrRequeue`), `nack_no_requeue` (any other error / `ErrPoison`), `max_redeliveries` (when the two-counter ceiling fires), `timeout` (HandlerTimeout exceeded), `handler_aborted_channel_closed` (mid-handler channel close). Set OTel status to `Ok` on `nil`, `Error` on every failure class. Call `Span.RecordError(err)` with the handler's error; set `error.type` to the sentinel name (e.g. `"ErrRequeue"`, `"ErrPoison"`, `"ErrMaxRedeliveries"`, `"ErrChannelClosed"`).
  - [x] **The library does NOT strip, rewrite, or normalise message headers** on the consume path. A symbol-presence test in the consumer (`grep -L 'delete(.*Headers' consumer*.go`, implemented as an executable regexp guard over `consumer.go`/`batch_consumer.go`) plus a unit test that publishes a message with a marker header `x-trace-marker=42` and asserts the value is present in `Delivery.Headers` exactly as published.
  - [x] Span is `End()`ed in every termination path including handler panics (wrapped in `recover` via `safeCallHandler`/`safeCallBatchHandler` per the panic-safety contract — a recovered panic maps to nack-without-requeue); tests inject a panicking handler (single + batch) and assert the span is ended via the in-memory tracer.
  - [x] Span continuity verified end-to-end: trace-id consistent across publisher → consumer → re-publisher to DLQ (when DLX'd) → DLQ consumer; integration test publishes one message, forces a `Nack(false)` (`ErrPoison`), attaches a second consumer to the DLQ, asserts both consumers' `process` spans share the *original* publisher trace-id and that `parent_span_id` of the DLQ consumer span resolves into the original producer span. A unit test additionally asserts producer-trace inheritance from an injected `traceparent`.
- **Verify:** Integration test publishes with a tracer; consumers with the same tracer; assert spans share traceID and DLQ-span parent resolves to the producer (DLX path). Outcome-matrix unit test exercises every verdict (`ack`/`nack_requeue`/`nack_no_requeue`/`max_redeliveries`/`timeout`/`handler_aborted_channel_closed`) and asserts span status + `messaging.rabbitmq.outcome` + `error.type`. Batch test asserts `process_batch` span name, one Link per message, and the verdict outcome.
- **Files:** `consumer.go` (`newConsumer` sets the propagator), `batch_consumer.go`, `batch_consumer_builder.go`, `otel/tracer.go`, `otel/propagation.go`; tests `consumer_tracing_test.go`, `batch_consumer_tracing_test.go`, `consumer_tracing_dlx_integration_test.go`, `otel/otel_test.go`, `publisher_tracing_test.go` (shared recording tracer).
- **Deps:** T18, T27.

### Checkpoint — Phase 6 done
- [x] Codecs: 3 codecs, 5 modes (JSON, Protobuf, CE structured, CE binary, raw bytes via `codec.JSON` of `[]byte`).
- [x] Span continuity end-to-end (publisher inject → consumer extract → DLX bounce → DLQ consumer; unit + `integration`-tagged DLX test).
- [ ] **Review with human before Phase 7.**

---

## Phase 7 — Advanced patterns

### [x] T29 — RPC `Caller[Req,Resp]` · M
- **Acceptance:**
  - [x] `CallerFor[Req,Resp](conn).Build()` returns a configured caller.
  - [x] `Call(ctx, req)` uses RabbitMQ direct reply-to (`amq.rabbitmq.reply-to`) by default; reply consumer is declared **before** the request is published; consumer auto-enables `no-ack` (required by the pseudo-queue protocol). Auto-stamps `CorrelationID` and `ReplyTo` on the request message if empty.
  - [x] `UseExclusiveReplyQueue()` builder method switches to a real exclusive auto-delete reply queue per Caller, with regular ack semantics.
  - [x] `ctx` deadline maps to per-call timeout → `ErrCallTimeout`.
  - [x] Concurrent calls return the right response (`CorrelationID` matching).
  - [x] If the underlying channel closes during a Call, in-flight calls return `ErrChannelClosed`; new calls reconnect transparently.
- **Verify:** Integration tests: (a) 100 concurrent calls, every response matches its request; (b) ctx timeout returns `ErrCallTimeout` cleanly; (c) `UseExclusiveReplyQueue` round-trip; (d) channel close mid-call surfaces `ErrChannelClosed`.
- **Files:** `rpc.go`, `rpc_caller_builder.go`, `rpc_caller_integration_test.go`.
- **Deps:** T12, T18.

### [x] T30 — RPC `Replier[Req,Resp]` + at-least-once ordering · S
SPEC compliance: handler errors do **not** send an error envelope to
the `Caller` — the caller observes `ErrCallTimeout` on `ctx` deadline.
Failed requests are `Nack(requeue=false)`; without a DLX configured
on the request queue, **this is a silent drop**. The `OnError` hook
is the only client-side signal; treat it as load-bearing. Rev 5
adds the at-least-once reply ordering contract.
- **Acceptance:**
  - [x] `ReplierFor[Req,Resp](conn).Build()` returns a configured replier.
  - [x] `Serve(ctx, ReplyHandler)` consumes requests and publishes responses to `ReplyTo` with matching `CorrelationID`.
  - [x] **At-least-once reply ordering**: for a successful handler, the replier publishes the reply, **awaits its confirm** (subject to `PublishTimeout`/`ConfirmTimeout` of the internal reply publisher), and **then** acks the request. If the reply publish fails (`ErrPublishNacked`, `ErrConfirmTimeout`, `ErrChannelClosed`), the request is `Nack(false)` so it goes to the request queue's DLX (if configured); the caller observes `ErrCallTimeout` on `ctx` deadline.
  - [x] **Crash-between-confirm-and-ack contract** documented: broker redelivers the request, replier sends a second reply. Callers MUST dedupe by `CorrelationID`. Godoc on `Serve` carries this verbatim.
  - [x] `OnError(func(ctx, req, err))` builder hook: handler error is reported via the hook; the request is `Nack`'d without requeue (so it goes to a DLX if configured, or is dropped if not); the caller observes `ErrCallTimeout` once its `ctx` expires.
  - [x] **Godoc on `OnError` and `Build` documents the silent-drop failure mode in full**, with explicit guidance: "Configure a DLX on the request queue if you need failed requests preserved for forensics. Without a DLX, `Nack(requeue=false)` is a drop and `OnError` is the only signal."
  - [x] **`ReplierBuilder.Topology(t)` auto-validates DLX presence** when the request queue was declared via this library's `Topology` on the same connection: inspects `t.DeadLetters` for an entry matching the request queue; if missing, `Build()` returns `ErrInvalidOptions` with the message `"Replier request queue <name> has no DeadLetter entry in Topology; Nack(false) drops will be silent. Add a DeadLetter or use AllowMissingDLX() to acknowledge."`. (Rev 6)
  - [x] **`AllowMissingDLX()` escape hatch** opts out of the validation when the request queue is declared out-of-band; the godoc documents the trade-off.
  - [x] **Mandatory metric `replier_drop_no_dlx_total{queue}`** increments every time the framework `Nack(false)`s a request whose source queue has no declared DLX (regardless of whether `Topology(t)` was wired) — drops are never invisible. (Rev 6)
  - [x] No broker-side validation of DLX presence at `Build()` time (would require management-plugin access and an extra round-trip). Static validation via `Topology(t)` plus the runtime metric is the contract.
  - Note (test level): DLX-presence validation and the reply-publish-failure verdict are covered by deterministic **unit** tests in `rpc_replier_test.go` (fake reply publisher + fabricated delivery) rather than the separately-suggested `rpc_replier_reply_failure_integration_test.go` / `rpc_replier_dlx_validation_test.go`; the broker round-trips (happy path, handler-error+DLX→DLQ, handler-error+no-DLX drop+metric) live in `rpc_replier_integration_test.go`.
- **Verify:** Integration tests:
  - Happy path round-trip Caller↔Replier with success.
  - **Reply-publish-failure path:** simulate a forced reply-publisher channel close immediately after the handler returns; assert the request is `Nack(false)`, lands in the DLQ if configured, and the caller times out with `ErrCallTimeout`.
  - Handler error + DLX configured: `OnError` fires once with the original error, the request lands in the DLQ (assert via `rabbitmqctl list_queues <dlq> messages`), and the caller times out cleanly with `ErrCallTimeout`.
  - Handler error + no DLX: `OnError` fires once, **`replier_drop_no_dlx_total{queue}` increments by 1**, the request is gone from the source queue, and `rabbitmqctl list_queues <source> messages` returns 0 — explicit assertion that the drop is real (this is a negative-path documentation test, *not* a regression we intend to silently change later).
  - **`Topology(t)` validation test:** declare a request queue WITHOUT a `DeadLetter` entry, build a `Replier` with `.Topology(t)`; assert `Build()` returns `ErrInvalidOptions`. Repeat with `.AllowMissingDLX()`; assert `Build()` succeeds.
- **Files:** `rpc_replier.go`, `rpc_replier_builder.go`, `rpc_replier_integration_test.go`, `rpc_replier_reply_failure_integration_test.go`, `rpc_replier_dlx_validation_test.go`.
- **Deps:** T29.

### [x] T31 — Delayed messages · S
- **Acceptance:**
  - [x] `Message[M].Delay` field honored at publish time (sets `x-delay` header). `buildPublishing` emits `x-delay` as a signed 32-bit millisecond count only when `Delay > 0`, cloning the header table so a reused `Message.Headers` map is never mutated.
  - [x] `Topology` declares `x-delayed-message` exchanges when `Kind = ExchangeDelayed`; `Args` carries `x-delayed-type` to specify the underlying type. (Already modelled + validated in `topology.go`/`types.go`; covered by existing `topology_test.go`/`types_test.go`.)
  - [x] Helper `warren.DelayedTopic(name string)` constructs the right `Exchange{}` literal (Kind=ExchangeDelayed, Durable, Args["x-delayed-type"]="topic").
- **Verify:** Integration test (`delay_integration_test.go`) publishes a 2s-delayed message through a `DelayedTopic` exchange and asserts delivery between 2s and 2.5s. It **skip-guards** via a `requireDelayedExchange` probe (declares a throwaway `x-delayed-message` exchange; a 406/`command-invalid` answer → `t.Skip`), so it skips cleanly on the default `rabbitmq:3-management` image (no plugin) and activates once a plugin-enabled broker exists. Per-plan allowance ("if T31 lands before T37, keep delayed tests behind skip/minimal wiring"); the 2s–2.5s assertion runs under T37's `amqptest.RequireDelayedExchange(t)`. The `x-delay` emission itself is fully covered by deterministic unit tests in `delay_test.go`. Verified: unit `-race` green, full integration lane `-race` green (28.7s, delay test skips cleanly), examples smoke `-race` green, lint 0 issues, build green.
- **README:** deferred to **T31b** — the roadmap bundles "RPC + delayed-message helpers" as one item (README L184); moving it to "Available now" with the examples-table entries lands cohesively when both `examples/rpc/` and `examples/delayed/` ship in T31b.
- **Files:** `delay.go`, edits to `topology.go`, `message.go`, `delay_integration_test.go`.
- **Integration fixture:** **`amqptest/`** (T37) — three plugin modes and `amqptest.RequireDelayedExchange(t)` per SPEC §6.9; do not add a parallel `testing/` package. If T31 lands before T37, keep delayed tests behind skip/minimal container wiring until the shared `amqptest` helper exists.
- **Deps:** T15, T12. (Strong pairing with **T37** for the canonical broker image/plugins.)

### [x] T31b — Checkpoint examples: `examples/rpc/main.go` + `examples/delayed/main.go` · S
Per SPEC §7 "Executable examples at checkpoints" + §10 Rev
decision 49. Closes the Phase 7 checkpoint.
- **Acceptance:**
  - [x] `examples/rpc/main.go` is `package main`, reads `AMQP_URL`, declares a request queue with a `DeadLetter` entry, runs a `ReplierFor[PriceReq, PriceResp]` with `.Topology(t)` (which auto-validates DLX presence) and `.Serve(ctx, handler)`, runs a `CallerFor[PriceReq, PriceResp]` that performs 3 concurrent `Call(ctx, req)` invocations and asserts each response matches its request via `CorrelationID`, and exits 0 after all three succeed. A negative-path block additionally demonstrates `ErrCallTimeout` by sending a request with a 50ms ctx against a handler that sleeps 200ms.
  - [x] `examples/delayed/main.go` is `package main`, reads `AMQP_URL`, declares an `ExchangeDelayed` exchange via the `DelayedTopic` helper (sets `Kind=x-delayed-message` + `x-delayed-type=topic`) + a bound queue, publishes a message with `Message[M].Delay = 2*time.Second`, runs a consumer that records the arrival time, asserts the arrival is between 2s and 2.5s of publish, and exits 0.
  - [x] Top-of-file godoc on `rpc/` documents the at-least-once reply ordering contract (per Rev 5 + T30) — consumers MUST dedupe by `CorrelationID`. Top-of-file godoc on `delayed/` documents the `x-delayed-message` plugin requirement, the durability caveat, and points at `amqptest.RequireDelayedExchange(t)` (T37).
  - [x] `go build ./examples/...` green on the unit lane (no plugin required for build).
  - [x] Integration test per example (`example_integration_test.go` in each subdir, `integration` tag) runs the example as a subprocess. The `rpc/` test runs against the standard broker (verified live: PASS 2.32s). The `delayed/` test guards itself with a broker probe (`requireDelayedExchange` — declares a throwaway `x-delayed-message` exchange; a 406 PRECONDITION_FAILED → `t.Skip`) so it skips cleanly on the plugin-less `rabbitmq:3-management` image and activates on a plugin-enabled broker; migrates to `amqptest.RequireDelayedExchange(t)` once T37 lands. Asserts exit 0 + the example's expected stdout. `goleak.VerifyNone(t)` clean.
- **Verify:** `go build ./examples/...` ✅; `golangci-lint run ./...` ✅ (0 issues); `go test -race -tags=integration ./examples/rpc/...` ✅ PASS; `./examples/delayed/...` ✅ SKIP (plugin absent, as designed).
- **Files:** `examples/rpc/main.go`, `examples/rpc/example_integration_test.go`, `examples/delayed/main.go`, `examples/delayed/example_integration_test.go`, edits to `README.md`.
- **Deps:** T29, T30, T31. (Soft pairing with T37 for the delayed-exchange plugin; not a blocker — skip-clean until T37 lands.)
- **Note:** chose a broker-probe skip-guard over the `AMQPTEST_IMAGE` / `AMQPTEST_DELAYED_PLUGIN_FILE` env-var approach the plan sketched — it mirrors the T31 `delay_integration_test.go` guard, needs no env wiring, and auto-activates on any plugin-enabled broker (better DX). The live 2s–2.5s plugin assertion remains exercised once a plugin broker exists (the example's own internal assertion + the smoke test); deferred verification path is T37.

### Checkpoint — Phase 7 done
- [x] RPC happy path + timeout green. (rpc example smoke PASS live; T29/T30 integration suites green.)
- [x] Delayed delivery within ±20% of requested delay. (Asserted [2s, 2.5s) in the example + T31 `delay_integration_test.go`; both skip-clean on a plugin-less broker, activate on a plugin-enabled one — verification path closes in T37.)
- [x] **`examples/rpc/main.go` and `examples/delayed/main.go` build (unit lane) and smoke-run end-to-end (integration lane; `delayed/` skips cleanly when the plugin is unavailable)** per T31b — SPEC §7 + Rev decision 49.
- [ ] **Review with human before Phase 8.**

---

## Phase 8 — Production hardening

### [ ] T32 — TLS / mTLS · S
- **Acceptance:**
  - [ ] `WithTLSConfig(*tls.Config)` option wires into the AMQP dialer.
  - [ ] `amqps://` URIs work out of the box.
  - [ ] Test fixtures: pre-generated server + client certs in **`amqptest/certs/`** (same paths as T37/T34b; landing certs with T32 is fine even before the full `amqptest` API ships).
- **Verify:** Integration test against a TLS-enabled RabbitMQ testcontainer; mTLS variant with client cert verification.
- **Files:** edits to `connection.go`, `options_connection.go`, `amqptest/certs/*`, `connection_tls_integration_test.go`.
- **Deps:** T07.

### [x] T33 — Cluster failover via `WithAddrs` · S
- **Acceptance:**
  - [x] `WithAddrs([]string)` tries addresses in order on initial connect.
  - [x] On reconnect, rotates to the next address (round-robin).
  - [x] First successful address sticks until the next disconnect.
- **Verify:** Integration test: docker-compose two RabbitMQ nodes; stop the first, assert reconnect succeeds against the second.
- **Files:** edits to `connection.go`, `options_connection.go`, `connection_failover_integration_test.go`.
- **Deps:** T07.
- **Done:** Per-socket round-robin cursor (`managedConn.dialCursor` + `nextDialAddr`/`connOptions.dialAddrs`); `dialAMQP` now takes the concrete `addr` selected by the cursor instead of hard-coding `addrs[0]`. Each dial attempt advances the cursor, so the initial connect walks the list in order and every reconnect resumes at the next node (wrapping at the end); the connected address sticks until a disconnect. `WithAddrs` godoc rewritten (was "first URI always used; round-robin planned"). Unit tests in `connection_internal_test.go` cover the rotation sequence (single-addr stickiness, in-order initial, round-robin wrap, `WithAddr` fallback). Integration `connection_failover_integration_test.go` proves both directions against a real broker using a dead `127.0.0.1:1` node — initial-connect skip and ForceReconnect rotate-past-dead-and-recover — needing only one broker instead of a two-node compose. **Verified:** unit `-race` green, full integration lane `-race` green (30s), examples smoke `-race` green, lint 0 issues, build green.
- **Deferred:** the reconnect loop applies its backoff between every dial attempt, so with N addresses a full sweep over a partitioned cluster waits N×Min before wrapping rather than fast-cycling all addresses before the first backoff. Acceptable for v0.1 (acceptance asks only for in-order + round-robin); a "try all addresses before backing off" optimisation is a future refinement.

### [x] T34 — Remaining Connection options · S
Rev 5 adds `WithConnectionName`, `WithPublisherConnections`,
`WithConsumerConnections`, `WithOnResubscribe`.
- **Acceptance:**
  - [x] `WithVHost`, `WithAuth`, `WithHeartbeat`, `WithChannelMax`, `WithFrameMax`, `WithDialer`, `WithClientProperties` implemented. *(all pre-existing in `options_connection.go`.)*
  - [x] **`WithConnectionName(name string)`** — default `<binary>-<hostname>-<pid>`; sets `client_properties.connection_name`. Role and index suffixes (`-pub-0`, `-con-0`, …) appended per TCP connection. *(pre-existing via `connpool.ConnName(opts.connectionName, role, i)`.)*
  - [x] **`WithPublisherConnections(n int)`** + **`WithConsumerConnections(n int)`** — already implemented in T07d; T34 covers the option wiring + default values (both 1). *(pre-existing.)*
  - [x] **`WithOnResubscribe(func(queue string))`** — fires once per consumer re-subscribe (alongside the mandatory metric in T19). *(net-new: option + `notifyResubscribed` seam wired into `consumer.go` and `batch_consumer.go` resubscribe paths.)*
  - [x] `WithClientProperties` default sets `product=warren`, `version=<from runtime/debug>`, `platform=Go <ver>`, `connection_name=<from WithConnectionName>`. *(net-new: `version`/`platform` added to `buildClientProperties`; `version` from `debug.ReadBuildInfo()` via `warrenVersion()`; `product`/`connection_name` pre-existing.)*
  - [x] **`WithFrameMax` godoc sizing table** (Rev 7, per SPEC §6.1 lines 677–700 + §10 #46): the doc-comment includes the three sizing tiers — small (≤8 KiB messages: `WithFrameMax(8192)`), streaming (32 KiB–1 MiB messages: `WithFrameMax(131072)`), hard-max (`WithFrameMax(0)` = server-negotiated, currently 128 KiB on RabbitMQ 3.13/4.x) — and the explicit pointer-out that messages >100 MiB should be chunked at the application layer. The `< 4096` rejection (already asserted in T07) is cross-referenced from this godoc. *(pre-existing godoc.)*
  - [x] **`WithHeartbeat` godoc sizing table + zero-warning** (Rev 7, per SPEC §6.1 lines 652–675 + §10 #47): the doc-comment includes the partition-detection guidance (timeout ≈ 2× heartbeat) and the workload tiers (5s/30s/60s with detection latencies). **Deviation from the original "`WithHeartbeat(0)` triggers a warning" wording:** the warning is keyed on a **negative** value (explicit disable), not zero. Zero is the struct zero value — indistinguishable from "option never set" without a sentinel — and AMQP/amqp091-go semantics treat `Heartbeat:0` as "use the server's negotiated default (~10 s)", not "disabled". Warning on zero would fire on **every** connection that does not set the option, which is the opposite of the intent. So `WithHeartbeat(0)` = server default (no warning); `WithHeartbeat(<0)` = disabled + `Dial`-time warning. The godoc states this explicitly. *(Better DX + protocol fidelity per the project's API north-stars; the disable-warning is asserted in `TestDial_negativeHeartbeat_*`.)*
  - [x] **Lens-12 (DX-13):** route the caveat to the **call-site godoc** — the **`ConfirmTimeout(0)`-discouraged**, **`PrefetchBytes()`-no-op-on-RabbitMQ**, and **`ChannelQoS()`-per-channel-semantics** caveats. *(verified already present at the call-site godocs in `publisher_builder.go` / `rpc_replier_builder.go` (`ConfirmTimeout`) and `consumer_builder.go` / `batch_consumer_builder.go` (`PrefetchBytes`/`ChannelQoS`); no change needed.)*
- **Verify:** Unit tests for each option's effect on the underlying `amqp091.Config`. Smoke integration test asserts `rabbitmqctl list_connections name client_properties` matches: `name` ends with `-pub-N` / `-con-N`, `client_properties` includes the documented keys.
- **Files:** edits to `options_connection.go`, `options_connection_test.go`.
- **Deps:** T07, T07d.
- **Done:** The bulk of the option surface was already implemented in earlier phases; T34's genuine net-new work was (1) `WithOnResubscribe(func(queue string))` — a connection-level callback fired once per consumer re-subscribe through a new `notifyResubscribed(cm, onResubscribe, queue)` seam that pairs the `consumer_resubscribed_total` metric with the user callback in a single place (also gives T34c one site to add `recover` instead of two), wired into both `consumer.go` and `batch_consumer.go`; and (2) enriching the AMQP `client_properties` table with `version` (`debug.ReadBuildInfo()` → warren module version, `(devel)`/`unknown` fallback) and `platform` (`"Go " + runtime.Version()`) via a new pure `buildClientProperties(opts, name)` helper, with user `WithClientProperties` keys still overlaid last. `WithHeartbeat` godoc gained the partition-detection (2× heartbeat) guidance and the 60s battery/LB tier. White-box unit tests in `options_connection_test.go` cover the option (set + last-wins), the `notifyResubscribed` seam (metric+callback, nil-callback), `buildClientProperties` (defaults + user-override), and `warrenVersion`. **Verified:** unit `-race` green (root cov 79.5%), full integration lane `-race` green (31.5s), examples smoke `-race` green, lint 0 issues, build green.

### [ ] T34b — SASL EXTERNAL (mTLS-only auth) · S
SPEC §6.1 + §10 #17: enterprise deployments at billions/day commonly
use mTLS-only auth via SASL EXTERNAL (password-less, identity from
client cert).
- **Acceptance:**
  - [ ] `WithSASLMechanism(mech SASLMechanism)` option wires `mech` into the `amqp091.Config.SASL` field. `SASLPlain` (default) constructs an `&amqp091.PlainAuth{}`; `SASLExternal` constructs the `EXTERNAL` mechanism (no user/pass).
  - [ ] When `SASLExternal` is selected, **`WithAuth` becomes a no-op** and `Dial` logs a single warning ("WithAuth ignored under SASL EXTERNAL"). The TLS config must present a client certificate; absence of TLS config returns `ErrInvalidOptions` at `Dial`.
  - [ ] godoc on `WithSASLMechanism` documents the broker-side requirement: `rabbitmq_auth_mechanism_ssl` plugin enabled, user mapped via `external_auth`. Cross-references SPEC §10 #17.
- **Verify:** Integration test against a testcontainer with `rabbitmq_auth_mechanism_ssl` enabled and a user created via the `external_auth` backend. Test matrix (Rev 6 expanded):
  - **(a) success:** `WithSASLMechanism(SASLExternal)` + `amqps://` + TLS config with client cert: `Dial` succeeds; basic publish/consume round-trip works.
  - **(b) WithAuth no-op:** `WithSASLMechanism(SASLExternal)` + `WithAuth("wrong", "password")` + valid client cert: `Dial` succeeds, warning log emitted, password is ignored.
  - **(c) no TLS config:** `WithSASLMechanism(SASLExternal)` + no `WithTLSConfig`: `Dial` returns `ErrInvalidOptions` with reason "TLS required for SASL EXTERNAL".
  - **(d) TLS without client cert:** `WithSASLMechanism(SASLExternal)` + `WithTLSConfig(&tls.Config{})` (no `Certificates`, no `GetClientCertificate`): `Dial` returns `ErrInvalidOptions` with reason "client certificate required for SASL EXTERNAL".
  - **(e) plain amqp scheme:** `WithSASLMechanism(SASLExternal)` + valid TLS + endpoint `amqp://...`: `Dial` returns `ErrInvalidOptions` with reason "amqps:// required for SASL EXTERNAL".
- **Files:** edits to `options_connection.go`, `connection.go`, `connection_sasl_external_integration_test.go`.
- **Deps:** T07, T32 (TLS), T37 (`amqptest` for the SSL-auth-enabled testcontainer fixture).

### [x] T34c — Panic isolation for user-provided callbacks · S
Every callback registered by the caller must be wrapped in `recover()` so a panicking
handler cannot crash internal library goroutines. Pure-notification callbacks must also
run in a goroutine to avoid blocking the event-loop that dispatches them.
- **Acceptance:**
  - [x] **`WithOnBlocked`** — call moved to a dedicated goroutine (`go func() { defer recover; fn(reason) }()`); panic is logged with a stack trace via `logger.Errorf`; the `supervisor` `select` loop is never blocked while the callback executes.
  - [x] **`WithOnReconnect`** — wrapped in `recover()` at the inline call site in `runBarrier`; on panic, log the stack trace and ensure `barrierCond.Broadcast()` is still emitted (no permanent Publisher deadlock).
  - [x] **`WithOnTopologyDegraded`** — `recover()` added inside the already-spawned goroutine (`mc.wg.Add(1)` in `runBarrier`); panic is logged; `wg.Done()` is always executed via `defer`.
  - [x] **`Handler[M]` / `RawHandler[M]`** — invocation extracted into a `safeCallHandler` helper with `recover()`; panic results in `nack(requeue=false)` (prevents infinite poison-message loop) and log of the stack trace; applies to both the inline path (no timeout) and the goroutine path (with timeout). *(Already shipped in Phase 6 / T73; T34c adds the `..._ThenContinues` characterization test proving the consumer keeps processing after a panic.)*
  - [x] **`BatchHandler[M]`** — same pattern via `safeCallBatchHandler`; panic results in `nackAll(requeue=false)` + log. *(Already shipped; T34c adds the batch `..._ThenContinues` characterization test.)*
  - [x] No public API change (no new exported error; recover is transparent to the caller).
  - [x] Unit tests for each site:
    - `WithOnBlocked`: a panicking callback does not kill the `supervisor` goroutine (verified via goleak + mock conn).
    - `WithOnReconnect`: panicking callback → barrier released, Publishers not deadlocked.
    - `WithOnTopologyDegraded`: panicking callback → `wg.Done()` called, process does not crash.
    - `Handler`: panic → nack without requeue emitted; consumer continues processing subsequent messages.
    - `BatchHandler`: panic → nackAll without requeue; consumer continues.
  - [x] `goleak.VerifyNone` clean in all tests above.
  - [ ] **Lens-08 (CR-05):** the five sites above cover user *callbacks* but not the **infra-goroutine boundaries** nor `OnReturn`. Add a `recover` (a) in `reconnect.Loop.run` around the user `connect` fn (`internal/reconnect/loop.go:72`; today a panic crashes the whole process — `defer close(l.done)` runs but there is no recover) and (b) on the supervisor / `runBarrier` around the resubscribe hook (`consumer.go:357` / `batch_consumer.go:190`; today a panic kills the supervisor → reconnect silently disabled for that socket); plus the missing `OnReturn` recover (CR-01, owned by T144). A panic must **degrade the socket** (`WithOnTopologyDegraded` + a metric), never crash the process or silently disable reconnect. *dep T143 (CG-5).*
- **Verify:** `go test -race ./...` green; `go test -race -tags=integration ./...` green (when broker available).
- **Files:** edits to `connection.go` (WithOnBlocked goroutine, WithOnReconnect + WithOnTopologyDegraded recover), `consumer.go` (safeCallHandler), `batch_consumer.go` (safeCallBatchHandler); tests in `connection_panic_test.go`, `consumer_panic_test.go`, `batch_consumer_panic_test.go`.
- **Deps:** T07, T18, T23.
- **Done (Phase-8 core, the five user-callback sites):** Added a shared `(*managedConn).recoverCallback(name)` seam (deferred recover → `logger.Errorf` with `debug.Stack()`), wired into all three connection callbacks in `connection.go`: (1) **WithOnBlocked** now runs in a `mc.wg`-tracked, recover-guarded goroutine via the new `safeOnBlocked(reason)` so the supervisor `select` loop never blocks on a slow/panicking callback (and `Close` drains it); (2) **WithOnReconnect** is recover-guarded inline in `runBarrier` so a panic can no longer skip `barrierCond.Broadcast()` and deadlock every Publisher; (3) **WithOnTopologyDegraded** gained a `recover` inside its existing `mc.wg` goroutine (ordered before `wg.Done` via LIFO defers so the log lands before `Close`'s `wg.Wait` returns). Sites (4)/(5) — `safeCallHandler`/`safeCallBatchHandler` — were already implemented in Phase 6 (T73 panic-safety contract); T34c adds the `_ThenContinues` characterization tests proving a panicking handler nacks-no-requeue **and** the consumer keeps processing the next delivery/batch. New tests in `connection_panic_test.go` (+ `errorfRecorder` logger), `consumer_panic_test.go`, `batch_consumer_panic_test.go`. **Verified:** unit `-race` green (root cov 79.5%), integration lane `-race` green (30.5s), examples smoke `-race` green, lint 0 issues, build green.
- **Deferred (Lens-08 / CR-05, gated by T143):** the infra-goroutine boundaries (reconnect-loop `connect` fn, the resubscribe hook) + the missing `OnReturn` recover are **not** done here. They require the **"degrade the socket" mechanism (a new metric)** and are explicitly **gate-first**: T143/CG-5 must first capture the current blast radius (process crash / silent reconnect-disable) before the fix lands. The plan sequences this in Phase 11 sub-phase (C) (`CG-5→T34c`); doing it now would violate the gate-first discipline. `OnReturn`'s recover is owned by T144, not T34c.

### [ ] T35 — `AutoAck()` opt-in + warning · S
- **Acceptance:**
  - [ ] `ConsumerBuilder.AutoAck()` enables the AMQP `no-ack` flag.
  - [ ] godoc on the method contains the four-bullet warning from SPEC §6.3 verbatim.
  - [ ] Consumer with `AutoAck` does not call `Ack/Nack`; handler errors are logged as warnings (with sample suppression).
- **Verify:** Integration test that publishes 100 messages, AutoAck consumer crashes mid-stream, restarts → asserts that previously-streamed messages are gone (demonstrating the trade-off, not a regression).
- **Files:** edits to `consumer.go`, `consumer_builder.go`, `consumer_autoack_integration_test.go`.
- **Deps:** T18.

### [ ] T36 — Remaining consumer options · S
SPEC compliance reminder: **`NoLocal()` is intentionally omitted** (SPEC
§6 "Note on AMQP 0-9-1 vs RabbitMQ" and SPEC §10 decision 10). RabbitMQ
silently ignores the `no-local` flag on `basic.consume`; exposing it
would be misleading API surface.
- **Acceptance:**
  - [ ] `Exclusive()`, `Args(Headers)`, `Tag(string)` builder methods land on both `ConsumerBuilder` and `BatchConsumerBuilder` and round-trip the values into the underlying `basic.consume` frame.
  - [ ] **No `NoLocal()` method** on either builder — verified by a unit test that asserts the symbol is absent (`grep`-style guard in `consumer_api_test.go`).
  - [ ] **`OnCancel(func(reason string))`** fires when broker sends `basic.cancel` (queue deleted, exclusive forced off, etc.). The reason is sourced from the AMQP frame.
  - [ ] **`Consume(ctx, ...)` returns `ErrConsumerCancelled` (wrapping the reason)** after delivering the `OnCancel` callback, so the consumer goroutine is never silently dead. The library does NOT auto-redeclare or reissue `basic.consume` — operators usually deleted the queue on purpose.
  - [ ] **Mandatory metric `consumer_cancelled_total{queue, reason}`** increments per received `basic.cancel`.
  - [ ] The library advertises `consumer_cancel_notify=true` in `connection.start-ok` client capabilities (already default in `amqp091-go`; assert via a recorded frame).
- **Verify:** Integration tests: declare a queue, attach consumer with `OnCancel` callback, delete the queue, assert callback fires with reason `"queue deleted"`, `Consume` returns `errors.Is(err, ErrConsumerCancelled)`, and `consumer_cancelled_total{queue, reason}` == 1. Symbol-absence test asserts the public surface has no `NoLocal` method on either builder.
- **Files:** edits to `consumer.go`, `consumer_builder.go`, `batch_consumer_builder.go`, `consumer_cancel_integration_test.go`, `consumer_api_test.go`.
- **Deps:** T18.

### [ ] T37 — `amqpmock/` + `amqptest/` subpackages · M
Rev 5 promotes the testcontainers helper to a public `amqptest/`
subpackage so downstream applications can reuse the fixture.
- **Acceptance:**
  - [ ] `go generate ./...` produces gomock mocks for `codec.Codec`, `log.Logger`, all three metrics interfaces, `otel.Tracer`.
  - [ ] Hand-written `amqpmock.NewDelivery[M](Fixture)` and `amqpmock.NewBatch[M](Fixture)` constructors that produce usable `*Delivery[M]` / `*Batch[M]` values for tests.
  - [ ] Root package has zero gomock imports at runtime (only in `amqpmock/` and `*_test.go`).
  - [ ] **Lens-06 (GA-09):** a **lightweight `Delivery[M]`/`Batch[M]` fixture path with no `go.uber.org/mock` dependency** (e.g. `DeliveryFixture`/`BatchFixture` constructors, guarded against unkeyed struct literals) lets consumer/raw/batch unit tests fabricate deliveries without importing the gomock-heavy mock subpackage.
  - [ ] **`amqptest/` public package**: `amqptest.NewRabbitMQ(t *testing.T, opts ...Option) *RabbitMQ` spins up a `rabbitmq:3.13.x-management` or `rabbitmq:4.0.x-management` testcontainer with:
    - `rabbitmq_delayed_message_exchange` plugin (for T31).
    - `rabbitmq_auth_mechanism_ssl` plugin + `external_auth` user (for T34b).
    - Pre-generated TLS server + client certs in `amqptest/certs/` (for T32 + T34b).
    Options: `WithRabbitMQVersion(string)`, `WithEnabledPlugins(...string)`, `WithExtraConfig(map[string]string)`.
  - [ ] **Plugin enablement strategy (Rev 6) — three explicit modes**, evaluated in order:
    1. **Pre-baked image:** if env `AMQPTEST_IMAGE` is set, that image is used as-is. Library ships `amqptest/docker/Dockerfile.amqptest` so consumers can publish their own.
    2. **Mounted `.ez`:** if env `AMQPTEST_DELAYED_PLUGIN_FILE` points at a local `.ez`, mount it into `/plugins/` and enable via `RABBITMQ_ENABLED_PLUGINS_FILE`. `amqptest/README.md` lists tested plugin versions per RabbitMQ minor + download URLs.
    3. **Skip fallback:** neither set → `amqptest.RequireDelayedExchange(t)` calls `t.Skip("delayed-message plugin not available; set AMQPTEST_IMAGE or AMQPTEST_DELAYED_PLUGIN_FILE")`. Tests not gated on the delayed exchange run normally.
  - [ ] `amqptest.RabbitMQ` exposes `URI() string` (with credentials), `AMQPSURI() string`, `Cleanup(t)`, and `Container() testcontainers.Container` for advanced cases.
  - [ ] godoc and a README in `amqptest/` document downstream usage (`go test ./...` from another module) and the three plugin modes.
  - [ ] **Lens-10 (TV-08):** the `rabbitmq_delayed_message_exchange` plugin is guaranteed present in ≥1 **required** CI lane (the pre-baked-image mode `AMQPTEST_IMAGE`), so the delayed-exchange conformance/example criteria do **not** silently `t.Skip` green; the three plugin-enablement modes stay, but the required lane uses the present-plugin mode and **fails (not skips)** if the plugin is expected but missing. dep VG-1.
- **Verify:** Run `go list -deps ./... | grep go.uber.org/mock` and confirm only test files match. Downstream-usability test: a separate `examples/integration-test-fixture/` module imports `amqptest` and asserts the fixture spins up cleanly without root-package leakage.
- **Files:** `amqpmock/codec.go`, `amqpmock/logger.go`, `amqpmock/metrics.go`, `amqpmock/tracer.go`, `amqpmock/delivery.go`, `amqpmock/batch.go`, `amqpmock/*_test.go`, plus `//go:generate` lines in source files. **New:** `amqptest/rabbitmq.go`, `amqptest/options.go`, `amqptest/plugins.go` (RequireDelayedExchange/RequireSSLAuth helpers), `amqptest/certs/{ca.pem,server.pem,server.key,client.pem,client.key}`, `amqptest/docker/Dockerfile.amqptest`, `amqptest/README.md`, `amqptest/*_test.go`.
- **Deps:** T03, T04, T05, T09, T17, T23.

### Checkpoint — Phase 8 done
- [ ] mTLS + cluster failover green.
- [ ] All Connection/Consumer/BatchConsumer options surfaced.
- [ ] AutoAck warning documented and demonstrated.
- [ ] Mocks usable downstream.
- [ ] **Review with human before Phase 9.**

---

## Phase 9 — Release readiness: v0.1.0

### [ ] T38 — Examples consolidation/polish pass · M
Per SPEC §7 "Executable examples at checkpoints" + §10 Rev
decision 49: the checkpoint examples (`publish/`, `topology/`,
`deadletter/`, `consume/`, `batch_publish/`, `batch_consume/`,
`rpc/`, `delayed/`) already exist on `main` when Phase 9 starts —
they landed in T13b / T16b / T21b / T23b / T31b. T38 is the
consolidation/polish pass: it adds the remaining release-only
example (`otel/`, requires Phase 6 instrumentation) and aligns
the existing examples for consistency.
- **Acceptance:**
  - [ ] `examples/otel/main.go` is `package main`, reads `AMQP_URL`, wires an in-process OTel tracer (stdout exporter is fine), runs a publish → consume round-trip, and prints the publisher and consumer span IDs demonstrating trace continuity (same trace-id, parent-span-id linkage).
  - [ ] **Consistency pass** across every `examples/*/main.go` already on disk: same env-var conventions (`AMQP_URL` default), same top-of-file godoc shape (purpose / run command / env vars / broker side-effects), same exit-code conventions (0 on success, non-zero on error), same idiomatic `log.Fatal` vs `os.Exit` choices, same `flag` package wiring where applicable. No silent reflows that change behaviour.
  - [ ] `Makefile` targets `examples-build` and `examples-smoke` exist (added in T13b) and run the full set.
  - [ ] CI workflow (`.github/workflows/ci.yml`) runs `make examples-build` on the unit lane and `make examples-smoke` on the integration lane — confirmed green on a clean checkout.
- **Verify:** CI smoke step builds and runs each example against a testcontainer RabbitMQ; manual `make examples-smoke` against a local broker.
- **Files:** `examples/otel/main.go`, `examples/otel/example_integration_test.go`, edits to `examples/*/main.go` for consistency, edits to `Makefile`, `.github/workflows/ci.yml`, `README.md`.
- **Deps:** T13b, T16b, T21b, T23b, T27, T28, T31b. (Cannot close until OTel instrumentation from Phase 6 + every checkpoint example is on `main`.)

### [ ] T38b — `examples/idempotent_consume/` · S
Rev 6 canonical reference for the dedupe-by-`MessageID` pattern
from SPEC §6.2.1. Cited from every godoc that mentions duplicates
(PublishRetry, PublishBatch channel-close, Replier at-least-once).
- **Acceptance:**
  - [ ] `examples/idempotent_consume/main.go` ships a runnable consumer with a bounded LRU cache (~10k entries / 15-min TTL) keyed by `MessageID`, demonstrating the §6.2.1 pattern verbatim.
  - [ ] A companion publisher (in the same file or a sibling `cmd/main.go`) deliberately publishes the same `MessageID` twice (via a `PublishRetry`-induced retry) and the consumer demonstrates exactly-once handler invocation.
  - [ ] README in the example folder explains: when to use, cache-sizing guidance, persistence options (Redis/DB) for cross-process dedupe.
- **Verify:** CI smoke test runs the example against a testcontainer; asserts the handler observed each `MessageID` exactly once across a forced reconnect that triggers duplicates.
- **Files:** `examples/idempotent_consume/main.go`, `examples/idempotent_consume/README.md`.
- **Deps:** Phase 2 (T12, T13), Phase 4 (T18).

### [ ] T38c — `examples/ordered_consume/` · S
Rev 6 canonical reference for strict per-queue ordering with
failover, demonstrating the `Concurrency(1) + SingleActiveConsumer`
pattern from SPEC §6.3. Cited from the `Concurrency` godoc.
- **Acceptance:**
  - [ ] `examples/ordered_consume/main.go` declares a queue with `Queue.SingleActiveConsumer=true`; starts two consumer instances with `Concurrency(1)`; publishes a numbered sequence `[0..N]`; the active consumer prints them in order; killing the active consumer demonstrates the broker promoting the standby with continued in-order delivery.
  - [ ] README explains: when ordering matters, the trade-off (one active worker at a time = lower throughput), and the failover semantics.
- **Verify:** CI smoke test asserts publish order matches handler order across an active-consumer kill.
- **Files:** `examples/ordered_consume/main.go`, `examples/ordered_consume/README.md`.
- **Deps:** Phase 3 (T14, T15), Phase 4 (T18).

### [ ] T39 — README quickstart · S
- **Acceptance:** README has a one-screen quickstart (Dial → Topology → Publisher → Consumer), feature list, link to every example, link to SPEC.md, link to godoc.
  - [ ] **Lens-12 (DX-04/DX-14):** the first code a user copies must compile and be honest — the quickstart (a) **compiles** against the value-typed `Handler[M]` (today's `func(ctx, o *Order)` does not build), (b) **does not swallow errors** (no `_ =`/`pub, _ :=`), (c) actually covers the §9 four (TLS / multi-addrs / OTel / DLX, L2442-2443), and (d) hits a stated minimal-line target — or the §1 `<10/<15` claim (L94-97) is softened if the honest minimum exceeds it (D6); the snippet-compile gate (XG-3, T160) is the lock. dep XG-3.
- **Verify:** Markdown lints clean.
- **Files:** `README.md`.
- **Deps:** T38.

### [ ] T40 — CHANGELOG + final godoc pass · S
- **Acceptance:**
  - [ ] CHANGELOG.md follows Keep a Changelog with a single Unreleased section.
  - [ ] Every exported identifier has a godoc comment.
  - [ ] **Lens-12 (DX-17):** the final godoc pass also asserts the **footgun→godoc routing table** (XG-1, T159) is fully satisfied — every load-bearing warning appears verbatim at its call site — and `CHANGELOG.md` actually exists for pkg.go.dev users (§4 L179 / §8 L2343 mandate a file that does not exist yet). dep XG-1.
- **Verify:** `golangci-lint run --enable=revive` with revive's missing-godoc rule passes.
- **Files:** `CHANGELOG.md`, godoc edits across the tree.
- **Deps:** Phases 1–8.

### [ ] T41 — Coverage gate · S
- **Acceptance:**
  - [ ] ≥ 80% line coverage per package.
  - [ ] ≥ 95% on `internal/reconnect`, `internal/confirms`, `channelpool`, **`internal/amqperror`**, **`internal/redact`** (Rev 7, per SPEC §9 line 2107–2109 — both packages are choke-points for AMQP correctness and credential safety; their coverage is load-bearing for the §9 reliability bar, not optional).
  - [ ] Coverage badge or coverage delta posted in CI.
  - [ ] **Lens-10 (TV-03):** the floor is **enforced**, not reported — a hard fail-under (the same per-package / critical-path thresholds) **fails CI** below the floor, the `-coverprofile` is uploaded as an artifact, and the PR comment §7 (L2235/L2239) + §9 (L2491) promise ("enforced in CI") is posted; today `go test -race -cover` discards the number. dep VG-2.
- **Verify:** `go test -cover ./...` per package.
- **Files:** add test cases as needed; CI workflow assertion.
- **Deps:** Phases 1–8.

### [ ] T42 — CI workflow · S
- **Acceptance:**
  - [ ] `.github/workflows/ci.yml` runs on push/PR: lint, unit, integration, conformance (matrix over Go 1.23 + 1.24).
  - [ ] Concurrency cancellation: PR push cancels in-flight run for the same ref.
  - [ ] **Lens-10 (TV-03/07/13):** implement what the scope already names — (TV-07) `-race` on **both** Go 1.23 **and 1.24** (today CI runs 1.23 only); (TV-03) wire the coverage fail-under + `-coverprofile` artifact + PR comment (with T41); (TV-13) an **integration-broker-required guard** that **fails** the required integration job if **zero** integration tests actually ran (today they `t.Skip` green when `AMQP_TEST_URL` is unset). dep VG-1/VG-2/VG-3.
- **Verify:** Workflow passes on the first push.
- **Files:** `.github/workflows/ci.yml`.
- **Deps:** Phases 1–8.

### [ ] T43 — Release workflow · XS
- **Acceptance:**
  - [ ] `.github/workflows/release.yml` triggered on tag push matching `v*.*.*`.
  - [ ] Single step: `gh release create "$GITHUB_REF_NAME" --generate-notes`.
  - [ ] No goreleaser, no binary artifacts (pure library).
- **Verify:** Cut a `v0.0.1-test` tag, observe workflow creates a GitHub release with auto-generated notes.
- **Files:** `.github/workflows/release.yml`.
- **Deps:** T42.

### [ ] T44 — Conformance tests · M
- **Acceptance:**
  - [ ] `conformance/` package with `//go:build conformance` tagged tests.
  - [ ] Covers: confirm ordering, content header encoding, mandatory return path, **broker-nack path** (`x-overflow=reject-publish` + `x-max-length=0`), `basic.cancel` notifications, **`basic.qos.global=true`** for `ChannelQoS()`, exchange types (Direct, Fanout, Topic, Headers, x-delayed-message), Quorum + Classic queue semantics, **`x-delivery-limit` enforcement on quorum queues**, **mandatory return/ack correlation order (return → ack)**.
  - [ ] **Lens-10 (TV-06/08/11):** (TV-06) add the **stub-vs-real-broker contract matrix** (brief §12) as a §7 artifact and **drop the "test AMQP server stub" language** for v0.1 — a stub cannot prove the real-broker-only contracts (R10-3 ordering, R10-2 quorum limit, `x-death` tokens/cycle-detection, broker-nack, `basic.cancel`, 406-on-`UserID`, `reject-publish-dlx`, direct-reply-to); conformance is **real-broker-only** for v0.1 (D5); (TV-08) the delayed-exchange criteria run against a plugin-enabled image in a required lane (coord T37) or are conditionally-verified and **fail, not skip**, when the plugin is expected but missing; (TV-11) assert the quorum poison-loop **bound** (`DeliveryLimit + 1`, not `==` — non-deterministic on cyclic topologies per §6.3) on a real **4.x** quorum queue via the version matrix (T151). dep VG-1/VG-5.
- **Verify:** `go test -tags=conformance ./conformance/...` green against both RabbitMQ 3.13 and 4.x.
- **Files:** `conformance/*.go`.
- **Deps:** Phases 1–7.

### [ ] T44b — Throughput benchmark suite · S
SPEC §9 reliability bar: bilhões/dia requires demonstrated throughput
on a reference runner. Bench gates block the `v0.1.0` tag.
- **Acceptance:**
  - [ ] `BenchmarkPublishConfirmed` (single publisher conn, single channel via pool): **≥ 30k msg/s** sustained on the reference runner (Apple M-series laptop or GH-hosted `macos-14`) against a local pinned-image testcontainer, JSON codec, confirms ON, 1 KB message body.
  - [ ] `BenchmarkPublishConfirmedMultiConn` with `WithPublisherConnections(4) + WithChannelPoolSize(16)`: **≥ 100k msg/s** sustained on the same hardware. Demonstrates that the multi-conn fan-out scales ≥ 3× over single-conn.
  - [ ] `BenchmarkPublishBatch` ≥ 5× the `BenchmarkPublishConfirmed` single-publish rate.
  - [ ] `BenchmarkConsume`: ≥ 30k msg/s consume with `Concurrency(8) + Prefetch(256)`.
  - [ ] Bench results CI-recorded as a JSON artifact; nightly drift report compares against the previous tag.
  - [ ] **Lens-09 (PC-03/04/05/14):** the bench must report + pin **payload size** and **queue type**, stating both a classic and a quorum number (quorum's majority Raft commit raises confirm latency materially, so "100k" without the queue type is uninterpretable — D4); reframe `BenchmarkPublishBatch ≥ 5×` into an RTT-stated absolute (the `5×` is pegged to the local single-publish baseline where sync is already writer-bound and ~20× understates the remote benefit) with `PublishBatch` documented as the RTT-decoupled scale path; document the release-tag-only regression cadence (perf can rot between releases on a normal PR), optionally adding a lightweight CI microbench smoke; and source the "~50k msg/s per socket" figure (§6.1) with a measured single-socket ceiling + the `sendM`-writer knee (PG-4). dep PG-4, PG-6.
  - [ ] **Lens-10 (TV-04):** add a **CI-gradeable relative regression gate** (fail if > X% slower than the last release baseline on the same runner class) as the *checked* §9 criterion, and reclassify the absolute `30k/100k/5×` numbers as **release-tag targets on stated reference hardware** — author-laptop numbers can never gate CI (HW/RTT/neighbour contention); the per-criterion §9 classification is the capstone T152, coordinated with Lens-09 T149. dep VG-1.
  - [ ] **Lens-13 (LT-10/11):** load-campaign distinction: the §9 `30k/100k` throughput numbers are **one-shot benchmark iterations**, not a sustained campaign — the **sustained-throughput campaign** (sustain the §9 target ≥1h without latency creep / pool saturation / memory growth) is **T167**, which inherits this bench's pinned payload-size + queue-type discipline; **LT-11 (quorum-vs-classic under load) is already satisfied** by PC-04's "state both a classic and a quorum number" — confirm, no new bench work.
- **Verify:** `go test -bench=. -benchmem -run=^$ ./...` reaches the gates locally and on the reference CI runner. Bench gate fails the build if a number drops > 20% versus the previous tag.
- **Files:** `bench_publish_test.go`, `bench_publish_batch_test.go`, `bench_consume_test.go`, `bench_multiconn_test.go`, CI workflow `.github/workflows/bench.yml`.
- **Deps:** Phases 1–5, T37.

### [ ] T45 — Reconnect chaos test (scaled up) · S
- **Acceptance:** Integration test: **5-minute outage @ 10k msg/s** with confirms (was 60s @ 1k msg/s), zero loss, `goleak.VerifyNone`. **`WithPublisherConnections(4)` enabled** so the test also exercises the multi-conn fan-out under chaos. Re-subscribe metric (`consumer_resubscribed_total`) and handler-aborted metric (`consumer_handler_aborted_channel_closed_total`) asserted non-zero by the test, demonstrating Rev 5 invariants hold under chaos. Topology re-declared on reconnect; in-flight handlers cancel via ctx with cause `ErrChannelClosed`.
  - [ ] **Lens-08 (CR-10):** add an explicit **1000-cycle** connect/disconnect + confirm-churn `goleak.VerifyNone` sub-test (no such churn test exists today) and reconcile §7's "100x" (L2268) **up** to §9's "1000" so the two stress criteria agree; confirm every goroutine in the Phase-19 inventory is joined. *dep T143 (CG-4).*
  - [ ] **Lens-10 (TV-09/10):** (TV-09 — the lens's highest-leverage finding; it guards the §1 no-loss headline) specify the chaos "zero message loss" counting method in §7 **and** the test: loss = **published-set − consumed-set deduplicated by `MessageID`** (UUIDv7), explicitly tolerating at-least-once **duplicates** (`PublishRetry`/the reconnect barrier produce them by design); add the **VG-6 injected-drop self-test** — the harness reports loss == 1 for one deliberate drop, proving it cannot pass while losing; (TV-10) reconcile the residual **§7 chaos prose** still reading "60s outage at 1k msg/s" (L2245) **up** to §9's "5-minute outage at 10k" (the §7 "100x"→"1000" memory-stress reconciliation is already this task's Lens-08 scope). dep VG-6.
  - [ ] **Lens-13 (LT-09):** intensity honesty: the chaos `10k msg/s` is **billions/day at average, never at peak** — 1 B/day ≈ ~11.6k/s average, and "billions/day" with typical 3–10× peaks means 10k validates neither a multi-billion/day system nor any system at peak; state the chaos/sustained intensity as a **peak multiple** of the average bar (the multiple is the owner's call, Q4), reusing this task's TV-09 loss-counting method unchanged.
- **Verify:** Test runs in <7 minutes on CI; flaky-rate <1% over 50 runs.
- **Files:** `chaos_reconnect_integration_test.go`.
- **Deps:** Phase 1, T07d, T12, T18, T20.

### [ ] T45b — Security regression scan · S
SPEC §8 + §9 reliability bar: credential leakage is a defect.
- **Acceptance:**
  - [ ] Integration test runs a 60s end-to-end workload against a credentialed URI (`amqp://leak_user:s3cret-pass@host:5672/v`); captures every emitted log line, error string, span attribute snapshot, and Prometheus metric label value into a single buffer.
  - [ ] Test scans the buffer with `regexp.MustCompile("s3cret-pass|leak_user:")` and asserts **zero matches**. A control assertion in the same test verifies the buffer is non-empty (otherwise a no-op test would pass trivially).
  - [ ] Also asserts every captured AMQP URI matches the redacted shape (`amqp[s]?://\*\*\*@`).
  - [ ] **Lens-10 (TV-14):** breadth of *exercise*, not just of scan — the 60s credentialed run must **force** a wrapped-`amqp091-go` error path (an auth failure / a broker error whose message embeds the dial URL; per Lens-07 the likeliest leak is a wrapped underlying error, not a warren-own string) so the wrapped-error surface is actually produced before the grep runs, since a grep only catches output that was exercised. coord Lens-07. dep VG-1.
- **Verify:** Test fails if any redaction site bypasses `internal/redact.URI`. Acts as a runtime regression for the SPEC §8 invariant.
- **Files:** `security_redaction_integration_test.go`.
- **Deps:** T07c, T07d, T12, T18, T37.

### [ ] T46 — Cut `v0.1.0` · XS
- **Acceptance:** `git tag v0.1.0` + `git push --tags`; release workflow runs successfully.
- **Verify:** GitHub release page exists, includes auto-generated notes, links to godoc.
- **Files:** none (tag operation).
- **Deps:** T38, T38b, T38c, T39–T45b.

### [ ] T47 — DLX Binding in Topology Expansion [P0] · XS
- **Acceptance:**
  - [ ] `expandDeadLetters` in `topology.go` appends a `Binding` between the expanded DLX exchange and DLQ queue with `RoutingKey: "#"`.
- **Verify:** Integration test declaring a queue with DLX args, publishing a message, nacking it (no-requeue), and asserting it arrives in the DLQ.
- **Files:** `topology.go`, `topology_integration_test.go`.
- **Deps:** T15.

### [ ] T48 — Strict JSON Codec & Trailing Data [P0] · XS
- **Acceptance:**
  - [ ] `codec.NewJSON()` and `codec.NewJSONStrict()` evaluates `dec.More()` after first decode and returns `ErrInvalidMessage` if true.
  - [ ] `FuzzCodecJSONStrict` target added to `json_fuzz_test.go`.
- **Verify:** Decodes `{"id":1}{"id":2}` return `ErrInvalidMessage`. Fuzz target runs without panics.
- **Files:** `codec/json.go`, `codec/json_test.go`, `codec/json_fuzz_test.go`.
- **Deps:** T09.

### [ ] T49 — Consumer Tag Cardinality Explosion [P1] · S
- **Acceptance:**
  - [ ] The Prometheus metric `consumer_cancelled_total` uses a static string (enum) for the `reason` label instead of the raw UUID consumer-tag.
  - [ ] Reason mapping: checks if queue exists via `QueueInspect`; if missing → `"queue_deleted"`, else → `"exclusive_revoked"`, default `"unknown"`.
- **Verify:** Unit test asserting `reason` label is one of the enums, not a `ctag-` UUID.
- **Files:** `metrics/prometheus.go`, `consumer.go`.
- **Deps:** T19, T36.

### [ ] T50 — In-Flight Memory Guardrail [P1] · M
- **Acceptance:**
  - [ ] `ConsumerBuilder.MaxInFlightBytes(n int64)` implemented.
  - [ ] Sits before handler execution; blocks/pauses new deliveries if `sum(len(Delivery.Body))` exceeds `n`. Decrements when handler returns.
  - [ ] Emits `consumer_inflight_bytes{queue}` gauge.
- **Verify:** Benchmark/load-test with 64 goroutines and 5MB bodies stays within memory bounds.
- **Files:** `consumer.go`, `consumer_builder.go`.
- **Deps:** T18, T19.

### [ ] T51 — Publisher Rate Limiting [P1] · S
- **Acceptance:**
  - [ ] `PublisherBuilder.WithPublishRateLimit(perSec int)` token-bucket limiter implementation.
  - [ ] `Publish` awaits token; on context cancel returns `ErrRateLimited` (transient).
  - [ ] Metric `publisher_rate_limited_total{exchange}`.
- **Verify:** `WithPublishRateLimit(100)` allows max 100 msg/s.
- **Files:** `publisher.go`, `publisher_builder.go`.
- **Deps:** T12, T13.

### [ ] T52 — Native Queue & DLQ Observability [P2] · M
- **Acceptance:**
  - [ ] `Consumer[M].WithQueueDepthSampler(interval)` option.
  - [ ] Background goroutine does `queue.declare-passive` to emit `queue_depth{queue}` and `dlq_depth{dlq}` gauges.
- **Verify:** Gauge metrics reflect external enqueues correctly.
- **Files:** `consumer.go`, `consumer_builder.go`.
- **Deps:** T18, T19.

### [ ] T53 — Consumer Draining API & Liveness Probes [P2] · M
- **Acceptance:**
  - [ ] `(*Consumer[M]).Pause(ctx)` sends `basic.cancel` locally without closing channel. `Resume(ctx)` re-issues `basic.consume`.
  - [ ] `(*Consumer[M]).Health() ConsumerHealth` exposes `Active`, `Paused`, `LastDeliveryAt`, `InFlightHandlers`.
- **Verify:** Test `Pause()`, publish 100 msgs, check none received, `Resume()`, check all 100 received. Liveness probe checks.
- **Files:** `consumer.go`, `consumer_builder.go`.
- **Deps:** T18, T36.

### [ ] T54 — Context Cancellation vs Transient Errors [P2] · XS
- **Acceptance:**
  - [ ] `IsTransient(err)` returns `false` if `errors.Is(err, context.Canceled)`.
- **Verify:** Table-driven test explicitly for `ErrChannelPoolExhausted` wrapped with `context.Canceled`.
- **Files:** `errors.go`, `errors_test.go`.
- **Deps:** T02, T07.

### [ ] T55 — Deduplication Middleware [P3] · M
- **Acceptance:**
  - [ ] `WithDedupe(DedupeStore, ttl)` exposed on ConsumerBuilder.
  - [ ] Pre-handler check (HIT -> Ack), Post-handler mark (on success).
- **Verify:** Dedupes messages with same `MessageID`. Fails open on store error (logs warning, processes msg).
- **Files:** `consumer.go`, `consumer_builder.go`.
- **Deps:** T18, T38b.

### [ ] T56 — Schema Drift Observability [P3] · S
- **Acceptance:**
  - [ ] `WithUnknownFieldObserver(func(path string))` on `codec.NewJSON`.
  - [ ] Hook emits `codec_unknown_fields_total` prometheus counter.
- **Verify:** Decoding `{"id":1, "unknown_new_field": "test"}` triggers the observer without failing the decode.
- **Files:** `codec/json.go`, `codec/json_test.go`.
- **Deps:** T09.

## Phase 11 — AMQP/SRE Specialist Re-review (Rev 10)

Closes the Rev 10 specialist findings (SPEC §10 "Rev 10"). SPEC
corrections R10-1..R10-4 are already applied inline; these tasks make
code/validation/tests/observability match. **Reconnect trio T61→T62→T63
shares the supervisor — sequence, do not parallelize.**

**Lens-01 re-review (2026-05-28):** T60, T61, T65, T66 are pulled forward
into Phase 12's priority sequence (they violate the §1 no-silent-failure
bar); their definitions remain here. T58, T59, T63, T64 are extended below.

### [ ] T57 — Delayed-exchange durability godoc/warning [P0] · XS
- **Acceptance:**
  - [ ] Godoc on `Message.Delay` and `ExchangeKindDelayed` mirrors the SPEC §6.5 warning: scheduled messages live in a non-replicated node-local table and are lost on node failure; recommends durable-queue + `x-message-ttl` + DLX.
  - [ ] (Optional) `Topology.Declare` emits a one-time warning log when an `ExchangeDelayed` is declared.
- **Verify:** `go doc` shows the warning; unit test asserts the warning log fires once per delayed-exchange declare.
- **Files:** `message.go`, `topology.go`, `types.go`.
- **Deps:** T10, T14, T31. **(R10-1, P0.1)**

### [ ] T58 — Quorum `DeliveryLimit==0` default-20 warning [P0] · XS
- **Acceptance:**
  - [ ] `Topology.validate()` logs a warning when `Type=QueueTypeQuorum && DeliveryLimit==0`.
  - [ ] **Version-aware (Lens-01 RMQ-06):** read the broker version from `connection.start` server-properties — on 4.x warn "broker default 20, drops at 21"; on **3.13** warn "unbounded — infinite poison loop". The stale `topology.go:48-49` "Zero means unbounded" godoc is corrected.
  - [ ] SPEC §9 poison-loop wording aligned with the corrected §6.3/§6.6.
- **Verify:** Table test: quorum + `DeliveryLimit==0` → warning; quorum + `DeliveryLimit>0` → no warning; classic → no warning. Per-version poison-loop **integration** assertion on 3.13 and 4.x (gate G3).
- **Files:** `topology.go`, `connection.go` (broker-version helper), `topology_test.go`.
- **Deps:** T14, T15, T20. **(R10-2, P0.2)** — coordinate with T64 (same `validate()`); dep gate G3/T74.

### [ ] T59 — Return/ack ordering invariant regression test [P0] · S
- **Acceptance:**
  - [ ] Test fails if the `basic.return` notify channel is made buffered, or if confirm/return demux is split across two goroutines.
  - [ ] Under concurrent mandatory-unroutable publishes, every publish resolves `ErrUnroutable` (zero false successes); asserts `MarkReturned` precedes the ack resolution.
  - [ ] **Lens-01 (RMQ-16):** a real-broker assertion (not just the mock tracker) covers the same path; `amqp091-go` is pinned in `go.mod`; a comment records the dependency on amqp091-go's single synchronous reader goroutine (a buffered/worker-pool dispatcher upstream would silently break the invariant).
  - [ ] **Lens-10 (TV-02):** the real-broker assertion runs **many iterations** under **concurrent unroutable-mandatory publishes during confirm load** to actually trip the ~50%-under-load race against amqp091-go's two-channel notify dispatch (a mock tracker cannot reproduce the dispatch timing → a green mock proves nothing about the race it exists to lock); a §7 note records that this contract is **flaky-prone by design** and belongs on the nightly trigger (T151), not a single PR run. dep VG-4.
- **Verify:** Run with `-race -count=100`; a deliberately-buffered return channel variant in the test makes it red. Real-broker variant on the integration lane.
- **Files:** `internal/confirms/tracker_test.go`, `publisher_test.go`, `go.mod`.
- **Deps:** T11, T13. **(R10-3, P0.3)**

### [ ] T60 — Single-delivery double-verdict idempotency guard [P0] · S
- **Acceptance:**
  - [ ] `Delivery[M]` has a resolved-once guard (mirrors `Batch[M]`): the second of any `Ack`/`Nack`/`AckIf`, or a handler-timeout verdict followed by a late handler verdict, is a no-op returning a sentinel (e.g. `ErrAlreadyClosed`-class), never a wire frame.
  - [ ] Channel stays open after the double call.
  - [ ] **Lens-02 (DS-04):** SPEC §6.3 documents the double verdict (incl. a late verdict after `HandlerTimeout`, esp. via `ConsumeRaw`) as a no-op, and states that **pre-fix** it channel-closes (406/`PRECONDITION_FAILED`), taking out *every* in-flight handler on that channel — collateral loss, not just a duplicate.
  - [ ] **Lens-08 (CR-04, High):** the resolved-once guard on `Delivery[M]` is a **single atomic CAS** (only the winner emits a frame; the loser is a no-op), explicitly **not** a check-then-act — today `Delivery.Ack/Nack` only test `d.done` (`delivery.go:79-115`), a window in which a timeout-verdict goroutine and a handler-`Ack` goroutine both emit → `PRECONDITION_FAILED` → channel close → every in-flight handler on that channel dies; unify with the `Batch[M]` guard and add a **race + behavioural** test (timeout vs handler-`Ack`) asserting exactly one frame and the late call a no-op. *dep T143 (CG-3).*
- **Verify:** Integration test: `HandlerTimeout` fires, handler later returns `nil`; assert no second frame, channel not closed, no `PRECONDITION_FAILED`. Unit test: double `Delivery.Ack` via `ConsumeRaw` is a no-op.
- **Files:** `delivery.go`, `consumer.go`, `delivery_test.go`, `consumer_test.go`, SPEC §6.3.
- **Deps:** T18, T19. **(R10-5, P0.4)** — *pulled into Phase 12; Lens-02 adds the §6.3 wording.*

### [ ] T61 — Channel-level recovery (distinct from TCP reconnect) [P1] · M
- **Acceptance:**
  - [ ] A consumer whose channel closes while the TCP connection stays up (404/406/ack-error) reopens its channel and re-`basic.qos` + re-`basic.consume` without waiting for a TCP reconnect.
  - [ ] `consumer_resubscribed_total{queue}` increments; consumer does not return silently.
  - [ ] **Lens-05 (SRE-01):** silent channel death is the highest-severity *invisible* failure — `consumer_resubscribed_total` must increment on a channel-only death (a `rate()==0` while traffic is expected is the alert), and its absence drives the `Connection.Health` readiness false-green (SRE-13/T115).
- **Verify:** Integration test forces a channel-only exception (e.g. passive-declare a missing queue on the consumer channel) and asserts the consumer recovers and keeps consuming.
- **Files:** `connection.go`, `consumer.go`, `consumer_integration_test.go`.
- **Deps:** T07, T07d, T18. **(R10-6, P1.4)** — sequence with T62/T63.

### [ ] T62 — Reconnect topology-redeclare de-amplification [P1] · M
- **Acceptance:**
  - [ ] Broker-global topology is redeclared **once per recovery event** at the `*Connection` level, not once per pooled `managedConn`.
  - [ ] `basic.consume`/`basic.qos` reissue stays per consumer connection.
  - [ ] **Lens-02 (DS-09):** SPEC §6.1 notes this compounds with DS-10 (T66) into a recovery storm; the chaos lane exercises a full-cluster restart against a just-recovered (possibly Khepri-quorum-forming) broker and asserts declares stay == topology size.
  - [ ] **Lens-05 (SRE-06):** the recovery action must not hammer the just-recovered (fragile) broker with N×pool×fleet `queue.declare`s; couple the chaos exercise with the SRE-04/T66 full-cluster restart.
- **Verify:** Integration/chaos test with `WithPublisherConnections(4)+WithConsumerConnections(4)` and an instrumented declare counter (or broker-side `queue.declare` count) asserts declares == topology size, not 8×.
- **Files:** `connection.go`, `topology.go`, `connection_internal_test.go`.
- **Deps:** T07, T16, T84 (chaos lane). **(R10-7, P1.2)** — sequence with T61/T63; *pulled into Phase 13 (v0.1).*

### [ ] T63 — Reconnect barrier max-duration cap [P1] · S
- **Acceptance:**
  - [ ] The synchronous redeclare barrier is bounded by a configurable max duration; on cap, blocked `Publish` calls return `ErrReconnecting` rather than stalling indefinitely.
  - [ ] **Lens-01 (RMQ-17):** the cap covers **Khepri (4.1 default)**, where `queue.declare` is a Raft-quorum op that can block during partition recovery.
  - [ ] **Lens-02 (DS-02):** SPEC names the cap option, its default, and the post-cap connection state (force-reconnect vs degraded), and states explicitly that `ConfirmTimeout` does **not** cover the barrier (the frame is still unwritten) — the cap is a distinct mechanism.
  - [ ] **Lens-05 (SRE-02):** the barrier-cap default must be **≤ the new default histogram top bucket** (SRE-11/T113) so a capped stall is *visible* in `publisher_publish_seconds`, not collapsed into `+Inf`.
- **Verify:** Unit test with a mock channel whose redeclare blocks longer than the cap asserts `Publish` returns `ErrReconnecting` at the cap (with `PublishTimeout=0` + `context.Background()`). Chaos: a half-alive-broker proxy (accepts the socket, stalls `queue.declare`) asserts the same on a real broker; `goleak` clean.
- **Files:** `connection.go`, `options_connection.go`, `connection_internal_test.go`, SPEC §6.1/§6.2.
- **Deps:** T07, T62, T84 (half-alive proxy). **(R10-8, P1.6)** — sequence with T61/T62; *pulled into Phase 13 (v0.1).*

### [ ] T64 — Quorum-queue structural validation + MaxPriority fix [P1] · S
- **Acceptance:**
  - [ ] `Topology.validate()` returns `ErrInvalidOptions` for a quorum queue that is non-`Durable`, `Exclusive`, `AutoDelete`, or carries `x-max-priority` (via the `MaxPriority` field **and** a raw `Args["x-max-priority"]`).
  - [ ] Stream queues are required to be `Durable` too.
  - [ ] **Lens-01 (RMQ-20):** the `Queue.MaxPriority` field **does** exist in code (`topology.go:56`) — retire the stale "no such struct field" note; instead **document `Queue.MaxPriority` in SPEC §6.6** (spec/code drift).
- **Verify:** Table-driven test covering each rejected quorum/stream combination + a valid quorum queue passing.
- **Files:** `topology.go`, `topology_test.go`, SPEC §6.6.
- **Deps:** T14, T15. **(R10-9, P1.5)** — coordinate with T58 (same `validate()`).

### [ ] T65 — DLQ durability/bounds + Consumer missing-DLX parity [P1] · M
- **Acceptance:**
  - [ ] Auto-declared `<source>.dlq` is `Durable` (quorum-capable) with configurable bounds (`x-max-length`/`x-max-length-bytes`).
  - [ ] A `Consumer` with `MaxRedeliveries>0` and a wired `Topology` lacking a DLX for the source queue warns at `Build` and increments a drop metric (parity with `Replier`'s `replier_drop_no_dlx_total`).
  - [ ] **Lens-05 (SRE-03):** highest blast radius in the spec — an unbounded DLQ fills disk → broker-wide `connection.blocked` (one service's poison storm → cluster-wide publish outage); bound the DLQ *by default* (overflow `drop-head`/`reject` is a deliberate bound, not unbounded) and emit a DLQ-depth signal so the storm is visible *before* the broker alarm.
  - [ ] **Lens-07 (ST-08):** the unbounded DLQ is an *attacker-reachable* resource-exhaustion vector (a producer's poison flood → disk-fill → broker-wide `connection.blocked` → cluster-wide publish outage); the default bound is the security control, asserted under an *adversarial* poison flood (not just an accidental one).
  - [ ] **Lens-11 (DP-03):** storage limitation (GDPR 5(1)(e) / LGPD Art. 16) — the auto-`<source>.dlq` bound must also carry a **default or strongly-recommended `x-message-ttl`** (not only a length bound) so dead-lettered *personal data* has a finite life by default (today retained **indefinitely**, DG-4); document the TTL as the personal-data retention control with a conservative default + a prominent godoc opt-out (exact value the T65 owner's call).
- **Verify:** Integration: nacked-poison lands in a durable bounded DLQ. Unit: consumer `Build` warns + metric increments when no DLX.
- **Files:** `topology.go`, `consumer.go`, `consumer_builder.go`, `metrics/`.
- **Deps:** T15, T47, T18, T30. **(R10-10, P1.3)**

### [ ] T66 — `WithAddrs` shuffle + reconnect rotation [P2] · S
- **Acceptance:**
  - [ ] The address list is shuffled per connection at `Dial`; reconnect rotates to the next address rather than always retrying index 0.
  - [ ] **Lens-02 (DS-10):** SPEC §6.1 notes this compounds with DS-09 (T62) into a recovery storm; the chaos lane asserts no `addr[0]` stampede on a full-cluster restart.
  - [ ] **Lens-05 (SRE-04):** the chaos lane asserts no `addr[0]` stampede on a full-cluster restart (compounds with SRE-06/T62 into a recovery storm).
  - [ ] **Lens-13 (LT-01/05):** harness fidelity: the "no `addr[0]` stampede on a full-cluster restart" assertion is **unrunnable on the single-node testcontainers harness** it is scheduled against — it binds to the **T166** multi-node cluster harness; the reconnect-storm is also a spike-on-recovery, exercised by the **T168** composite set. This assertion is confirmed, not re-filed — T166 finally gives it a harness that can run it.
- **Verify:** Unit test asserts N connections start from a distribution of addresses; reconnect picks a different address. Chaos: a full-cluster restart shows reconnections spread across addresses.
- **Files:** `connection.go`, `options_connection.go`, `connection_internal_test.go`.
- **Deps:** T07, T07d. **(R10-11, P2.1)** — *already pulled into Phase 12.*

### [ ] T67 — `Dial` partial-pool-connect policy [P2] · S
- **Acceptance:**
  - [ ] Policy recorded in SPEC §6.1 and implemented: `Dial` succeeds if ≥1 connection per role connects (supervisor reconnects the rest) — or fail-fast, per the decision.
  - [ ] **Lens-05 (SRE-08):** resolve to **succeed-if-≥1-per-role** with supervised reconnect of the rest **and** a metric/log for booting at reduced capacity — an undefined policy means fail-fast blocks *every* deploy on one flaky node, or succeed-degraded is *silent* capacity loss; an integration test boots a 2+2 pool with one consumer connection unreachable, asserts `Dial` succeeds, the missing socket reconnects under supervision, and the degraded-capacity signal fired.
- **Verify:** Integration test where a subset of pooled connections cannot connect asserts the chosen behaviour deterministically + the degraded-capacity signal.
- **Files:** `connection.go`, SPEC §6.1, `metrics/`, `connection_integration_test.go`.
- **Deps:** T07, T07d. **(R10-12, P2.2)** — *pulled into Phase 16 (v0.1).*

### [ ] T68 — Alternate-exchange support [P2] · S
- **Acceptance:**
  - [ ] `x-alternate-exchange` declarable on an `Exchange` (server-side catch-all for unroutable messages), complementing `Mandatory()`+`OnReturn`.
  - [ ] **Lens-04 (EDA-01):** the platform-level unroutable safety net — a mis-routed publish *without* `Mandatory()` vanishes silently (EG-1); the AE catches it server-side regardless of per-publish discipline. Complements T103's client-side exchange-name validation.
  - [ ] **Lens-06 (GA-05):** the alternate exchange is exposed **additively** — via the existing `Exchange.Args` or a new optional field whose zero value = today's behaviour; **no exported `Exchange` field is renamed or removed** (T124 pins the topology roadmap additive).
- **Verify:** Integration: publish (non-mandatory) to no matching binding with an AE configured → message arrives on the AE-bound queue.
- **Files:** `topology.go`, `topology_test.go`, `topology_integration_test.go`.
- **Deps:** T14, T15. **(R10-13, P2.4)** — *pulled into Phase 15 (v0.1).*

### [ ] T69 — Exchange-to-exchange bindings [P2] · S
- **Acceptance:**
  - [ ] `Binding` (or a typed variant) supports an exchange destination (`exchange.bind`) for layered fan-out.
  - [ ] **Lens-04 (EDA-03):** ingest→per-domain layered fan-out is declarable without flattening the topology; the declare-once/deep-snapshot semantics stay intact.
  - [ ] **Lens-06 (GA-05):** the destination-exchange shape is **pinned by T124** to a **separate `Topology.ExchangeBindings []ExchangeBinding{Source, Destination, RoutingKey, NoWait, Args}`** — `Binding` is **not** reshaped (no `Source`/`Destination` rename, no exported `Binding` field renamed or removed); the declare-once/deep-snapshot semantics extend to `ExchangeBindings`.
- **Verify:** Integration: bind exchange→exchange, publish to source, assert delivery via the destination exchange's bound queue (`rabbitmqctl list_bindings`).
- **Files:** `topology.go`, `topology_test.go`, `topology_integration_test.go`.
- **Deps:** T14, T15. **(R10-14, P2.3)** — *pulled into Phase 15 (v0.1).*

### [ ] T70 — Graceful-shutdown completeness [P2] · M
- **Acceptance:**
  - [ ] `Close` handles prefetched-but-undispatched deliveries deterministically (drain or nack-requeue), documented in SPEC §6.1.
  - [ ] `BatchConsumer` flushes its pending partial batch on `Close`/final `FlushAfter`.
  - [ ] **Lens-02 (DS-03):** the choice is resolved to **nack-requeue (`requeue=true`)** the undispatched buffer before channel close (never drop → no silent loss); `consumer_shutdown_requeued_total` increments; the forced-close (ctx-deadline) abandoned-in-flight duplicate window is named in SPEC (see DS-16/T85).
  - [ ] **Lens-05 (SRE-07):** every rolling deploy is a low-grade incident — the deploy-time duplicate rate must be **boundable and observable** via `consumer_shutdown_requeued_total`.
  - [ ] **Lens-06 (GA-06):** the new `consumer_shutdown_requeued_total` metric adds a method to the user-implementable `metrics.*` interfaces — it lands behind the embeddable `metrics.NoOp` base (T125) so external implementers don't break-compile, before rc1.
  - [ ] **Lens-08 (CR-09):** on a **forced** (ctx-deadline) close, *detach* a non-cooperative handler that ignores its cancelled `ctx` (bounded by the cascade ctx), increment a `consumer_handler_leaked_total`-style metric, and do **not** hang the cascade on it (today the timeout-drain `<-handlerDone` waits unboundedly, `consumer.go:650`); the §7/§9 goleak **carve-out** for the ctx-ignoring handler (a caller defect — Go cannot force-kill a goroutine) lands in the capstone T146.
- **Verify:** Integration: prefetch N, dispatch < N, `Close`; assert undispatched are nack-requeued (redelivered), not silently dropped. Batch partial flush asserted with `goleak` clean. Gated by G2 (capture the current v0.1 behaviour first).
- **Files:** `connection.go`, `consumer.go`, `batch_consumer.go`, `metrics/`, SPEC §6.1/§6.4.
- **Deps:** T18, T22, T84 (G2). **(R10-15, P2.5)** — *pulled into Phase 13 (v0.1).*

### [ ] T71 — Observability gaps: pool-wait, in-flight, redelivered [P2] · M
- **Acceptance:**
  - [ ] Channel-pool acquire-wait/saturation metric exposed.
  - [ ] `consumer_in_flight{queue}` gauge (active handlers) exposed.
  - [ ] `consumer_redelivered_total{queue}` counter increments on `Redelivered()==true` deliveries.
  - [ ] **Lens-02 (DS-14):** `consumer_redelivered_total` is the redelivery-class duplicate-budget signal `publisher_retry_total` does not cover — required for the §1 "duplicate budget never invisible" claim to hold for the dominant duplicate source.
  - [ ] **Lens-05 (SRE-05):** this is the single most important on-call *leading* indicator — without it a brewing poison storm / pool saturation is invisible until it is an outage; assert the redelivery ratio / pool-acquire-wait p99 are alertable.
  - [ ] **Lens-06 (GA-06):** these new gauges/counters add methods to the user-implementable `metrics.*` interfaces — they land behind the embeddable `metrics.NoOp` base (T125) so adding interface methods stays forward-compatible for external adapters, before rc1.
  - [ ] **Lens-13 (LT-07/08):** measurement under load: the pool-wait / `consumer_in_flight` / `consumer_redelivered_total` saturation metrics this task adds must land **before** the spike/stress/soak campaigns (T167/T168) can observe the saturation they provoke (LT-08 — a hard dependency of those campaigns); and the default `WithLatencyBuckets` top bucket (5000 ms, §6.9) clips the load tail (`ConfirmTimeout` 30s + the R10-8 barrier cap), so the load reports override the buckets (the override is owned by T169) — coordinate the override range with this metric set (LT-07).
- **Verify:** Unit/integration assert each metric moves under the relevant condition (pool saturation, busy handlers, a forced redelivery).
- **Files:** `metrics/`, `channelpool.go`, `consumer.go`.
- **Deps:** T04, T08, T18. **(R10-16, P2.6)** — coordinates with T50/T52/T53; *pulled into Phase 13 (v0.1).*

### [ ] T72 — TCP keepalive / dialer hardening [P2] · XS
- **Acceptance:**
  - [ ] Default `net.Dialer` sets a keepalive; `TCP_USER_TIMEOUT` documented where available, so a write to a half-open socket fails promptly.
  - [ ] **Lens-05 (SRE-09):** AMQP heartbeats cover only *read-side* partition detection (~20s); a *write* to a half-open socket can block far longer with `ConfirmTimeout=30s` the only backstop — the dialer keepalive must make a publish on a dead socket error promptly (well under 30s); a half-open-socket integration/`chaos` test asserts it.
- **Verify:** Unit test asserts the default dialer carries keepalive; documented in SPEC §6.1 heartbeat/partition section; a half-open-socket test asserts a publish errors well under 30s.
- **Files:** `options_connection.go`, `connection.go`, SPEC §6.1.
- **Deps:** T07. **(R10-17, P2.7)** — *pulled into Phase 16 (v0.1).*

### [ ] T73 — Codec-call panic safety: `defer recover` → `ErrInvalidMessage` · S
Formalises the T09 panic-safety contract (todo.md T09 / SPEC §6 "Panic
safety contract") as a standalone, trackable task. The recover wrapper
is the safety net for **user-supplied** codecs — a third-party codec may
panic and the library cannot statically know whether it will, so every
`Codec.Encode`/`Codec.Decode` call must be wrapped. Built-in codecs
(`NewJSON`, `NewJSONStrict`, `NewProtobuf`) must never panic by design;
the recover is not a license for them to do so.
- **Acceptance:**
  - [x] **Consumer `Decode`** is wrapped in `defer recover` → `ErrInvalidMessage` (already implemented in `consumer.go`; the recovered value is wrapped as `fmt.Errorf("%w: codec panic: %T", ErrInvalidMessage, r)`). A unit test injects a panicking fake codec and asserts the delivery is nacked-no-requeue with no goroutine crash.
  - [ ] **Publisher `Encode`** is wrapped in `defer recover` → `ErrInvalidMessage` (gap today: `publisher.go` calls `p.codec.Encode` directly with no recover). On a codec panic, `Publish` returns `ErrInvalidMessage` (wrapping the recovered value), increments `publisher_publish_seconds{outcome="error"}`, and does **not** open a channel or write a frame.
  - [ ] A unit test in `publisher_test.go` injects a panicking fake codec and asserts `errors.Is(err, ErrInvalidMessage)` with `goleak.VerifyNone` clean.
- **Verify:** `go test -race -run 'CodecPanic' .` exercises both the publisher and consumer panic paths with a fake codec whose `Encode`/`Decode` panic.
- **Files:** edits to `publisher.go`, `publisher_test.go`; `consumer.go`/`consumer_test.go` already satisfy the consumer half.
- **Deps:** T09, T13, T18.

### Checkpoint — Phase 11 (Rev 10) closed
- [ ] All T57–T72 acceptance criteria ticked; `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane green; `goleak.VerifyNone` clean.
- [ ] Reconnect trio (T61/T62/T63) landed in sequence with chaos coverage.
- [ ] Each per-task SPEC amendment landed in the same PR as its code; SPEC §10 "Rev 10" stays the source of record.

## Phase 12 — Protocol-Correctness Re-review (Lens 01: RabbitMQ 3.13 + 4.x)

Closes the Lens-01 protocol findings (`spec-validation/01-rabbitmq-amqp-protocol.md`,
`RMQ-01..RMQ-31`). Reconciliation: several *spec* findings are already
correct in code (SPEC drifted → doc-only fixes), while `at-least-once`
dead-lettering is unimplemented and quorum has no structural validation.
Owner decisions: implement `at-least-once` with forced `reject-publish`;
pull T60/T61/T65/T66 forward (defined in Phase 11); async API stays
LATER-34. **Differential 3.13-vs-4.x integration assertions required.**
Gate T74 runs first. Per-task SPEC amendment lands in the same PR.

### [ ] T74 — Verification gates G1–G6 (real broker, 3.13 + 4.x) [P0] · S
- **Acceptance:**
  - [ ] Ground-truth captured on **both** broker versions for: G1 `x-death` delivery-limit reason atom (`delivery-limit` vs `delivery_limit`); G2 x-death `count` accumulation shape; G3 4.x *classic* queue `x-delivery-limit` honoring; G4 valid `{quorum, overflow, at-least-once}` declare permutations; G5 broker `max_message_size` defaults per version; G6 `prefetch_size!=0` reject-vs-ignore.
  - [ ] Results table committed (under `spec-validation/` or task notes); downstream tasks cite their gate.
- **Verify:** `make integration-up` + `AMQP_TEST_URL=… make test-integration` against the 3.13 and 4.x images.
- **Files:** `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T07d, T14, T15. **(Lens-01 gates, P0)**

### [ ] T75 — x-death delivery-limit reason token (RMQ-01) [P0] · S
- **Acceptance:**
  - [ ] If G1 shows the broker emits `delivery_limit`, the matched atom in `internal/headers/xdeath.go:83` is corrected and `-`↔`_` normalised defensively.
  - [ ] The **fabricated** unit test (`makeEntry(...,"delivery-limit",...)`) is replaced by a real-broker integration test driving a quorum `x-delivery-limit` dead-letter and asserting `DeathCount()` increments.
- **Verify:** Integration on 4.x: a poison message past `DeliveryLimit` dead-letters and `DeathCount()` > 0 with the real reason.
- **Files:** `internal/headers/xdeath.go`, `internal/headers/xdeath_test.go`, a new integration test, SPEC §6.3 + decision 34.
- **Deps:** T17, T74 (G1). **(RMQ-01, P0)**

### [ ] T76 — at-least-once DLX strategy implemented (RMQ-05) [P0] · M
- **Acceptance:**
  - [ ] For a quorum queue with a DLX, `Declare` injects `x-dead-letter-strategy: at-least-once`.
  - [ ] `x-overflow=reject-publish` is forced/validated for that queue (auto-set with a warning, or `ErrInvalidOptions` if the user set `drop-head`) — at-least-once is invalid with drop-head.
  - [ ] The source-queue memory cost of at-least-once is documented.
- **Verify:** Unit: injection + overflow rule. Integration: quorum + DLX declares successfully and dead-letters at-least-once.
- **Files:** `topology.go`, `topology_test.go`, a new integration test, SPEC §6.6 + decision 52.
- **Deps:** T14, T15, T47, T74 (G4). **(RMQ-05, P0)** — coordinate with T64/T65.

### [ ] T77 — PublishBatch+Mandatory duplicate-MessageID validation (RMQ-15) [P1] · S
- **Acceptance:**
  - [ ] A `Mandatory()` `PublishBatch` containing duplicate explicit `MessageID`s returns `ErrInvalidMessage` before publishing (the documented-trap comment at `publisher.go:689-694` is replaced by enforcement).
- **Verify:** Unit test: duplicate explicit IDs in a mandatory batch → `errors.Is(err, ErrInvalidMessage)`; auto-stamped IDs still pass.
- **Files:** `publisher.go`, `publisher_test.go`, SPEC §6.2 + decision 14.
- **Deps:** T13. **(RMQ-15, P1)**

### [ ] T78 — SPEC↔implementation reconciliation (no behaviour change) (RMQ-02/03/04/14) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.8 IsTransient godoc + PublishRetry trigger list mark **311 permanent** (matches `errors.go:248`).
  - [ ] SPEC §6.3 says `DeathCount` is the **sum of the `count` sub-field** (matches `xdeath.go:77-88`).
  - [ ] SPEC §6.2 + decision 20 + the `errors.go:38` comment say a disk/memory alarm surfaces `ErrConnectionBlocked`, **not** `ErrPublishNacked`.
  - [ ] SPEC §6/§6.3 state `PrefetchBytes` is dropped client-side and the broker **rejects** non-zero `prefetch_size` (the code already sends 0 at `consumer.go:367`).
- **Verify:** Guard unit tests: 311 `IsPermanent` (confirm existing); a test asserting the `Qos` size arg is 0.
- **Files:** SPEC §6.2/§6.3/§6.8, `errors.go` (comment), `consumer_test.go`.
- **Deps:** —. **(RMQ-02/03/04/14, P1)**

### [ ] T79 — Reply-code channel/connection scope annotation (RMQ-18) [P2] · XS
- **Acceptance:**
  - [ ] SPEC §6.8 annotates each reply-code sentinel as channel-level (311/403/404/405/406) or connection-level (320/402/501–505/506/530/540/541), with the recovery implication noted (ties to T61).
- **Verify:** Doc review; cross-reference check against T61.
- **Files:** SPEC §6.8 (`errors.go` godoc).
- **Deps:** —. **(RMQ-18, P2)**

### [ ] T80 — Sizing/limits factual fixes (RMQ-12/13) [P2] · XS
- **Acceptance:**
  - [ ] SPEC §6.1 states the per-version broker `max_message_size` defaults (128 MiB 3.13 / 16 MiB 4.0+, per G5) and that >default needs the broker raised; reconciled with the ≥100 MiB pointer-out guidance.
  - [ ] SPEC §6.1 fixes "131072 is the AMQP-spec minimum" → "4096 is the minimum; 131072 the default".
- **Verify:** Doc review against G5 results.
- **Files:** SPEC §6.1.
- **Deps:** T74 (G5). **(RMQ-12/13, P2)**

### [ ] T81 — Version-divergence documentation (RMQ-17/19/21/23/30/31) [P2] · S
- **Acceptance:**
  - [ ] Khepri caveat (declares are Raft-quorum ops); CloudEvents 0-9-1⇄1.0 bridge version note + a round-trip interop test (coord. lens 03); §9 verification pinned to the management HTTP API instead of `rabbitmqadmin` CLI (v2 rewrite in 4.0); mirrored-queue staleness fixed (§6.2); transient-queues-deprecated-feature note; mixed-version-cluster caveat.
  - [ ] **Lens-10 (TV-05/11):** the version-divergence claims are **verified** by the new RabbitMQ 3.13 + 4.x integration matrix (T151) — assert the quorum default `x-delivery-limit` (R10-2, default 20) drops the 21st delivery on **4.x** and the classic-queue `x-delivery-limit`-ignore behaviour, with §9 verification pinned to the management HTTP API on the version where each binds; the poison-loop **bound** assertion (TV-11) rides the same 4.x quorum queue. dep VG-5.
- **Verify:** Doc review; the CloudEvents interop round-trip test passes on both versions.
- **Files:** SPEC §6.1/§6.2/§6.9/§9, a CloudEvents interop test.
- **Deps:** T26. **(RMQ-17/19/21/23/30/31, P2)**

### [ ] T82 — Contract-precision SPEC fixes (RMQ-24/25/26/27/28/29) [P3] · S
- **Acceptance:**
  - [ ] decision-17 default-"1" staleness fixed; ack-vs-confirm wording (§6.2); sub-ms `Expiration`→`"0"` footgun documented (optionally reject `<1ms` non-zero, §6.5); Priority range + "quorum has no priority" (§6.5); exclusive-reply-queue redeclare-on-reconnect (§6.7); prefetch-16 reworded as guidance not a broker constant (§6.3).
  - [ ] **Lens-02 (DS-07):** the §6.2 ack-vs-confirm wording is the **single source** for Phase 13 T88's queue-type confirm-semantics table — coordinate, do not duplicate or contradict.
- **Verify:** Doc review; if `<1ms` reject is implemented, a unit test asserts `ErrInvalidMessage`.
- **Files:** SPEC §6.2/§6.3/§6.5/§6.7/§10, optionally `message.go` + `message_test.go`.
- **Deps:** —. **(RMQ-24/25/26/27/28/29, P3)** — *coordinate with Phase 13 T88.*

### [ ] T83 — §9 throughput-honesty wording (RMQ-11) [P2] · XS
- **Acceptance:**
  - [ ] SPEC §9 qualifies the 30k/100k targets with the local-broker/sub-ms-RTT assumption, documents the `pool/RTT` ceiling + a remote projection, and cross-references LATER-34.
  - [ ] **Lens-09 (PC-01):** bake the explicit RTT-collapse model table (rate @1/5/10 ms — brief §11) into §9 *beside* the 30k/100k numbers as the "remote projection", so the load-bearing local-only caveat is computable at the number rather than parked ~680 lines away in LATER-34 (the 30k/100k targets imply ~0.27–0.64 ms loopback RTT; they collapse to ~64k/~12.8k/~6.4k multi-conn at 1/5/10 ms). dep PG-5.
- **Verify:** Doc review.
- **Files:** SPEC §9.
- **Deps:** —. **(RMQ-11, P2)**

### Checkpoint — Phase 12 (Lens 01) closed
- [ ] T74 gate results documented; downstream tasks cite their gate.
- [ ] Poison path correct on **both** 3.13 and 4.x: T75 (real-broker x-death), T58 (version-aware warning), T64 (quorum validation).
- [ ] DLX correct: T76 (at-least-once + reject-publish), T65 (durable bounded DLQ + Consumer missing-DLX).
- [ ] §1 silent-failure defects closed: T60, T61, T65, T66.
- [ ] SPEC matches implementation (T78); version caveats + honest §9 numbers (T79/T80/T81/T82/T83).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean; README synced.

## Phase 13 — Distributed-Systems Re-review (Lens 02: failure modes, consistency, ordering, duplicates)

Closes the Lens-02 adversarial spec validation
(`spec-validation/02-distributed-systems.md`, `DS-01..DS-17`; brief
`spec-validation/02-distributed-systems-plan.md`). Lens verdict: **NO-GO for the
§1 bar as written; GO-WITH-CHANGES** once the High findings land. Owner decisions
(2026-05-28): pull **T62/T63/T70/T71** forward into v0.1; stand up a new **`chaos`
build tag** (3-node cluster + fault injector + half-alive proxy) because a
single-broker lane cannot falsify DS-05/06/07/13; build the **opt-in
structured-error RPC reply mode** now (DS-12); invest in **per-entity redeclare**
(DS-08). No new `LATER.md` entries. Failure-mode claims are tested against a real
broker/cluster, not a mock. **Gate task T84 runs first**; no SPEC edit to an
affected section lands before its gate returns. Per-task SPEC amendment lands in
the same PR; a SPEC §10 "Rev 12" note records the pass.

### [ ] T84 — Chaos lane + verification gates G1–G6 (real broker + 3-node cluster, 3.13 + 4.x) [P0] · L
- **Acceptance:**
  - [ ] A `chaos` build tag + `make test-chaos` target stands up a 3-node RabbitMQ cluster (configurable `cluster_partition_handling`), a fault injector (`toxiproxy`/`iptables`/`rabbitmqctl stop_app`), and a half-alive-broker proxy. (Size **L** — split into a fixture sub-task and a gate-capture sub-task before starting.)
  - [ ] Ground truth captured on **both** versions for: **G1** SAC-failover reorder/duplicate with `Prefetch>1` (classic **and** quorum); **G2** the *current* `Close` behaviour for prefetched-but-undispatched deliveries (requeue or drop?); **G3** quorum publish pinned to a minority-partition node (hang/timeout/error + duplicate-on-heal); **G4** the client signal per `pause_minority`/`autoheal`/`ignore`; **G5** a poison crash-loop defeating process-local Counter B; **G6** the `Caller`'s handling of a second reply for an already-resolved `CorrelationID`.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate.
- **Verify:** `make test-chaos` green on the 3.13 and 4.x cluster images; the gate table is reviewable.
- **Files:** `docker-compose.chaos.yml`, `Makefile`, `*_chaos_test.go`, `amqptest/`, `spec-validation/` (results table).
- **Deps:** T07d, T14, T15. **(Lens-02 gates, P0)**

### [ ] T85 — Dedupe-pattern rework: crash-unsafe LRU + persistent example (DS-01/DS-15/DS-16) [P0] · M
- **Acceptance:**
  - [ ] SPEC §6.2.1 splits **publish-retry** duplicates (bounded by outage+reconnect+retry → in-memory LRU OK) from **crash/requeue redelivery** duplicates (unbounded gap, and the crash wipes the in-memory cache → ~zero protection); states that handlers with external side-effects (DB/HTTP/payments) **require persistent dedupe**, not "paranoid"; recommends bounding queue residency with a TTL.
  - [ ] **DS-15:** the "UUIDv7 makes eviction-by-recency trivial" non-sequitur is dropped (an `lru.Cache` evicts by access order, not the key's timestamp); SPEC §6.2.1/§6.5 document that `MessageID`/`Timestamp` ordering is per-publisher wall clock — not global, not monotonic across NTP steps — and only ID *uniqueness* is load-bearing for dedupe.
  - [ ] **DS-16:** the forced-close (ctx-deadline) abandoned-in-flight duplicate window is named in §6.2.1.
  - [ ] `examples/idempotent_consume/` ships a persistent (Redis/DB) variant.
  - [ ] **Lens-04 (EDA-18):** the §6.2.1 L1067–1068 dangling `examples/idempotent_consume/` reference is closed by this task + T38b — the directory ships and matches the reworked pattern.
  - [ ] **Lens-11 (DP-05):** data minimisation (GDPR 5(1)(c) / LGPD Art. 6.III) — a *natural-key* `MessageID` turns the dedupe cache into a **store of personal data** (and flows to `x-death`, logs, the OTel `messaging.message.id` attr → APM); frame the recommended TTL/residency bound as **storage limitation** (a finite, definable retention window for whatever the user puts in the key), and the privacy-favourable path is to keep `MessageID` the UUIDv7 default.
- **Verify:** A chaos test crashes the consumer mid-handler and asserts the persistent path dedupes the redelivery while the in-memory path does not.
- **Files:** SPEC §6.2.1/§6.5, `examples/idempotent_consume/`, a chaos test.
- **Deps:** T38b, T84. **(DS-01/DS-15/DS-16, P0)**

### [ ] T86 — Cluster partition-handling modes subsection + §1 carve-out (DS-05) [P0] · M
- **Acceptance:**
  - [ ] A new SPEC §6.1 subsection documents the client-side observation per `pause_minority`/`autoheal`/`ignore` (per G4), with an explicit **§1 carve-out** that under `ignore` acked messages can be lost silently on heal (mirroring the R10-1 delayed-message carve-out).
  - [ ] SPEC recommends against `ignore`; recommends `pause_minority` + `WithAddrs` failover.
  - [ ] README reliability copy updated for the partition carve-out.
- **Verify:** Chaos test asserts the client sees a classifiable reconnect under `pause_minority`/`autoheal`; doc review of the `ignore` carve-out against G4.
- **Files:** SPEC §6.1, `README.md`, a chaos test.
- **Deps:** T84 (G4). **(DS-05, P0)**

### [ ] T87 — SAC ordering qualification (DS-06) [P0] · M
- **Acceptance:**
  - [ ] SPEC §6.3/§6.6 + decision 30 drop "strict ordering with failover"; state per-channel ordering holds **steady-state only**, and at the failover boundary up to `Prefetch` messages from the dead active consumer are redelivered (duplicates) and may reorder relative to messages published during the gap.
  - [ ] SPEC recommends `Prefetch(1)` with SAC when cross-failover order matters (reduces, never eliminates).
  - [ ] `examples/ordered_consume/` README states the boundary caveat.
- **Verify:** G1 chaos test asserts the documented reorder/duplicate behaviour per queue-type per broker-version.
- **Files:** SPEC §6.3/§6.6/§10, `examples/ordered_consume/`, a chaos test.
- **Deps:** T84 (G1), T38c. **(DS-06, P0)**

### [ ] T88 — Queue-type confirm-semantics table + minority-partition window (DS-07) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.2 carries a queue-type confirm-semantics table: **quorum** = confirm after Raft majority-commit; **classic durable+persistent** = after fsync/batch; **transient/non-durable** = immediate, no durability.
  - [ ] The **quorum minority-partition** window is named (per G3): no quorum → publish unconfirmed → `ErrConfirmTimeout` → `PublishRetry` → duplicate on heal — tied to DS-05.
- **Verify:** G3 confirms the timeout→retry→duplicate path; the table is reviewed against the RabbitMQ quorum-queue docs; no contradiction with T82's ack-vs-confirm wording.
- **Files:** SPEC §6.2, a chaos test.
- **Deps:** T84 (G3). **(DS-07, P1)** — coordinate with T82 (merge the ack-vs-confirm wording, do not duplicate).

### [ ] T89 — Per-entity redeclare (degraded-mode blast radius) (DS-08) [P1] · M
- **Acceptance:**
  - [ ] On a genuine durable-definition conflict, only publishes routing to the conflicting entity fail; the rest of the role's publish path stays live (replaces the whole-role degraded halt).
  - [ ] SPEC §6.1 + decision 28 document the new granularity and that `ForceReconnect()` is ineffective for non-transient conflicts.
- **Verify:** Integration test: declare a conflicting durable queue; assert publishes to a *different* entity still succeed while publishes to the conflicting entity return `ErrTopologyRedeclareFailed`.
- **Files:** `connection.go`, `topology.go`, `connection_internal_test.go`, SPEC §6.1/§10.
- **Deps:** T62. **(DS-08, P1)** — sequence after T62 (shared redeclare path).

### [ ] T90 — RPC orphan-reply handling (DS-11) [P1] · M
- **Acceptance:**
  - [ ] The `Caller` discards a reply whose `CorrelationID` has no pending entry, emitting a metric/log; a UUIDv7 `CorrelationID` is never reused, so a late reply cannot bind to a subsequent `Call`.
  - [ ] In `UseExclusiveReplyQueue()` mode the orphan reply is ack-and-dropped (not left unacked).
  - [ ] SPEC §6.7 specifies the above.
- **Verify:** G6 chaos test (Replier publishes-confirms then crashes before ack → second reply) with concurrent `Call`s asserts the orphan does not resolve/disturb another in-flight call; repeated for `UseExclusiveReplyQueue()`.
- **Files:** `rpc.go`, `metrics/`, `rpc_test.go`, a chaos test, SPEC §6.7.
- **Deps:** T84 (G6). **(DS-11, P1)**

### [ ] T91 — Opt-in structured-error RPC reply mode (DS-12) [P1] · M
- **Acceptance:**
  - [ ] An opt-in mode lets a `Replier` send a structured error reply so a deterministic handler rejection is distinguishable at the `Caller` from timeout/loss, instead of collapsing all three into `ErrCallTimeout`.
  - [ ] SPEC §6.7 + decision 16 document the mode and warn that **without** it callers MUST NOT blind-retry without idempotency + a bounded budget (the non-converging re-run-and-re-DLX hazard). Revises part of decision 16.
- **Verify:** Integration test: a Replier handler returns a deterministic error in structured-error mode; the `Caller` receives a distinguishable error, not `ErrCallTimeout`; default mode unchanged.
- **Files:** `rpc.go`, `rpc_test.go`, a `*_integration_test.go`, SPEC §6.7/§10.
- **Deps:** T84. **(DS-12, P1)**

### [ ] T92 — Poison Counter-B crash-loop honesty (DS-13) [P1] · S
- **Acceptance:**
  - [ ] SPEC §1/§6.3/§9 state that Counter B (process-local, resets per restart) does **not** bound a poison message that crashes the consumer process; the only crash-safe bound is quorum `x-delivery-limit`.
  - [ ] The §1 "no silent poison loop" + §9 "at most `MaxRedeliveries+1` deliveries" claims are downgraded to "per-process-lifetime, classic-queue; crash-safe only with quorum `x-delivery-limit`".
- **Verify:** G5 chaos test demonstrates the unbounded reprocessing across restarts on a classic queue; a quorum counterpart shows the broker-side bound holds.
- **Files:** SPEC §1/§6.3/§9, a chaos test.
- **Deps:** T84 (G5). **(DS-13, P1)** — coordinate with T58 (version-aware delivery-limit).

### [ ] T93 — `PublishBatch` order-under-retry caveat (DS-17) [P3] · XS
- **Acceptance:**
  - [ ] SPEC §6.2 + decision 43 note the input-order guarantee holds only absent a mid-batch channel close; a caller-retried slot (decision 43) loses its position, so callers needing order must re-publish the whole batch, not just failed indices.
- **Verify:** Doc review.
- **Files:** SPEC §6.2/§10.
- **Deps:** —. **(DS-17, P3)** — may ride T85.

### Checkpoint — Phase 13 (Lens 02) closed
- [ ] T84 chaos lane (`make test-chaos`: 3-node cluster + fault injector + half-alive proxy) green on 3.13 **and** 4.x; gate results table committed; downstream tasks cite their gate.
- [ ] Active §1 violations closed: DS-02/T63 (bounded barrier, `ErrReconnecting` within the cap; `ConfirmTimeout` does not cover the barrier), DS-03/T70 (`Close` nack-requeues undispatched, never drops; `consumer_shutdown_requeued_total`).
- [ ] Missing failure domains filled: DS-05/T86 (partition-mode subsection + `ignore` carve-out), DS-07/T88 (queue-type confirm table + minority-partition window).
- [ ] Overclaims corrected: DS-06/T87 (SAC qualified, `examples/ordered_consume/` caveat), DS-13/T92 (poison crash-loop honesty), DS-15 (UUIDv7 eviction wording, in T85).
- [ ] Dedupe remedy + correctness windows: DS-01/T85 (crash-unsafe LRU + persistent example), DS-04/T60 (§6.3 wording + `ConsumeRaw` test), DS-11/T90 (orphan reply), DS-16 (forced-close window, in T85), DS-17/T93 (batch order-under-retry).
- [ ] Recovery-storm + escalated features: DS-09/T62 + DS-10/T66 + DS-14/T71 (de-amplification + shuffle + redelivery metric), DS-08/T89 (per-entity redeclare), DS-12/T91 (structured-error reply mode).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the new chaos lane green; `goleak.VerifyNone` clean.
- [ ] README "Available now / On the roadmap" + reliability copy synced (partition carve-out, SAC caveat, `consumer_redelivered_total`, structured-error RPC mode).
- [ ] SPEC §10 "Rev 12" note records the Lens-02 pass; no duplicate tasks created (T60/T62/T63/T66/T70/T71/T82 amended in place); no new `LATER.md` entries.

## Phase 14 — Interoperability & Wire-Format Re-review (Lens 03: polyglot clients, CloudEvents/Proto/JSON, field-tables)

Closes the Lens-03 adversarial spec validation
(`spec-validation/03-interoperability-wire-format.md`, findings `IW-01..IW-13`;
brief `spec-validation/03-interoperability-wire-format-plan.md`). Lens verdict:
**GO-WITH-CHANGES** — no active §1 message-loss bug, but interop overclaims (IW-01
CloudEvents 1.0 bridge, IW-08 field-table "matched 1:1"), two silent-lossy mappings
(IW-07 `time.Time`→`T`, IW-04 JSON `int64`), and no non-Go interop test (IW-13).
Owner decisions (2026-05-28): stand up the **FULL** polyglot lane (new **`interop`
build tag**: `pika` + Java + AMQP-1.0 CloudEvents SDK) because the Go-only lane
cannot falsify cross-language claims; **remove** the CloudEvents binary 0-9-1→1.0
guarantee (binary = 0-9-1 consumers only; structured = the cross-ecosystem path);
**defer** the proto multi-type discriminator to **LATER-39**; **document +
recommend RFC3339 string** for `time.Time` headers. One new `LATER.md` entry
(LATER-39). Cross-language claims are tested by a non-Go-client round-trip, not a
Go↔Go mock. **Gate task T94 runs first**; no SPEC interop-claim edit lands before
its gate returns. Per-task SPEC amendment lands in the same PR; a SPEC §10 "Rev 13"
note records the pass.

### [ ] T94 — Interop lane + verification gates IG-1–IG-6 (real broker, polyglot, 3.13 + 4.x) [P0] · L
- **Acceptance:**
  - [ ] An `interop` build tag + `make test-interop` target stands up Python `pika` + a Java client + an AMQP-1.0 CloudEvents SDK in containers, extending `amqptest` (T37). (Size **L** — split into a fixture sub-task and a gate-capture sub-task before starting.)
  - [ ] Ground truth captured on **both** versions for: **IG-1** `time.Time`→`T` second-resolution read; **IG-2** unsigned `B/u/i/L` + `Decimal` `D` + `[]byte` `x` cross-client decode (faithful / mis-typed / unreadable); **IG-3** what an AMQP-1.0 CloudEvents SDK sees for a binary-mode publish (prefix, message section, colon key); **IG-4** structured-mode reconstruction from `application/cloudevents+json` (id/source/type/time/extensions); **IG-5** JSON `int64 > 2^53` into a Go `int64` field vs `any`; **IG-6** ContentType/ContentEncoding not swapped via a non-Go consumer.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate.
- **Verify:** `make test-interop` green on the 3.13 and 4.x broker images; the gate table is reviewable.
- **Files:** `docker-compose.interop.yml`, `Makefile`, `*_interop_test.go`, `amqptest/`, `spec-validation/` (results table).
- **Deps:** T37 (extend, no dup), T07d, T09, T24, T26. **(IW-13 + gates, P0)**

### [ ] T95 — `time.Time` header truncation doc + RFC3339 recommendation (IW-07) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.5 + decision 13 document that `time.Time`→AMQP `T` is 64-bit POSIX **seconds** — sub-second precision and timezone are silently lost on the wire; a Go reader sees a truncated value and a Java reader a second-resolution `Date`.
  - [ ] SPEC recommends an RFC3339 **string** header when sub-second/TZ fidelity matters (no API change).
- **Verify:** A round-trip test asserts the Go-reader truncation; IG-1 asserts the `pika` second-resolution read.
- **Files:** SPEC §6.5/§10, `message_test.go` (or a codec/headers round-trip test), an interop test.
- **Deps:** T94 (IG-1). **(IW-07, P1)**

### [ ] T96 — Field-table cross-client interop scoping (IW-08/IW-09/IW-10) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.5 + decision 13 scope the field-table cross-client guarantee against RabbitMQ's documented field-table type table; flag unsigned `uint8/16/32/64`→`B/u/i/L` and `Decimal{Scale,Value}`→`D` as **low-interop** (Go/Java-leaning; may be unreadable/mis-decoded by some Python/.NET consumers).
  - [ ] SPEC recommends signed `int64` (`l`) and string headers for maximum cross-language safety, and documents `[]byte`(`x`) vs `string`(`S`).
- **Verify:** A cross-client decode test (IG-2) via the harness asserts which types `pika`/Java read faithfully.
- **Files:** SPEC §6.5/§10, an interop test.
- **Deps:** T94 (IG-2). **(IW-08/IW-09/IW-10, P1)**

### [ ] T97 — CloudEvents binary: remove 1.0-bridge claim, promote structured (IW-01/IW-02/IW-03/IW-12) [P1] · M
- **Acceptance:**
  - [ ] SPEC §6.9 + decision 4 **remove** the binary-mode "RabbitMQ bridges 0-9-1 headers ⇄ AMQP-1.0 application-properties, so a non-Go AMQP-1.0 CloudEvents client interoperates" guarantee; document binary mode for **0-9-1 consumers only**, and promote **structured mode** (`application/cloudevents+json` body) as the official cross-ecosystem path.
  - [ ] **IW-03:** the binary-mode `time` attribute is confirmed emitted as an RFC3339 string `S` (not `T`). **IW-02:** colon-key (`cloudEvents:type`) survival through the 0-9-1→1.0 conversion is documented per IG-3.
  - [ ] **IW-12:** the `traceparent`/`tracestate` 0-9-1→1.0-bridge caveat moves here; the DLX-preservation + no-prefix-collision claims stay (do-not-regress).
  - [ ] `examples/cloudevents/` ships (structured + binary), documenting the cross-ecosystem path.
- **Verify:** IG-3 characterises the AMQP-1.0 binary-mode view; IG-4 proves structured-mode reconstruction by a non-Go consumer; `examples/cloudevents/` builds + smoke-runs.
- **Files:** SPEC §6.9/§10, `examples/cloudevents/`, `README.md`, interop tests.
- **Deps:** T94 (IG-3, IG-4). **(IW-01/IW-02/IW-03/IW-12, P1)**

### [ ] T98 — JSON int64 precision hazard doc + test (IW-04) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.9 + decision 23 document that a JSON `int64`/snowflake > 2^53 decodes losslessly only into a typed `int64` field; into `any`/`map[string]any`/`interface{}` it silently becomes `float64`. State the mitigation (type `M` fields as `int64`/`json.Number`; avoid `any` for large ints) and that the codec does **not** call `UseNumber()` by design.
- **Verify:** `FuzzCodecJSON` extended for large ints; a cross-language `int64 > 2^53` round-trip test (IG-5) asserts the typed-field path is faithful and the `any` path is documented-lossy.
- **Files:** SPEC §6.9/§10, `codec/json_fuzz_test.go`, an interop test.
- **Deps:** T94 (IG-5). **(IW-04, P1)**

### [ ] T99 — Protobuf single-type constraint + media-type (IW-05/IW-06) [P2] · S
- **Acceptance:**
  - [ ] SPEC §6.9 documents the proto3 **single-type-per-`Consumer`** constraint — proto3 binary carries no type info, so a multi-type topic-exchange queue needs a caller-supplied discriminator (deferred to LATER-39).
  - [ ] **IW-06:** SPEC justifies `application/x-protobuf` and the codec **accepts `application/protobuf` on decode** (Postel).
  - [ ] **LATER-39** files the Any/type-URL/registry discriminator (prereq T99).
- **Verify:** A unit test asserts `application/protobuf` is accepted on decode; doc review of the constraint.
- **Files:** SPEC §6.9/§10, `codec/protobuf.go`, `codec/protobuf_test.go`, `LATER.md`.
- **Deps:** —. **(IW-05/IW-06, P2)**

### [ ] T100 — ContentType/ContentEncoding swap-test sharpening (IW-11) [P3] · XS
- **Acceptance:**
  - [ ] SPEC §9 mandates the round-trip swap test set **both** fields to **distinct non-empty** values (`ContentType=application/json`, `ContentEncoding=gzip`) and assert each independently via `rabbitmqadmin get` / a non-Go consumer (an empty ContentEncoding would hide a swap).
- **Verify:** The test (or its spec criterion) sets distinct values and asserts both (IG-6).
- **Files:** SPEC §9, a round-trip/interop test.
- **Deps:** T94 (IG-6). **(IW-11, P3)** — may ride T94.

### Checkpoint — Phase 14 (Lens 03) closed
- [ ] T94 interop lane (`make test-interop`: `pika` + Java + AMQP-1.0 clients) green on 3.13 **and** 4.x; IG-1..IG-6 results table committed; downstream tasks cite their gate.
- [ ] Silent-lossy mappings flagged: IW-07/T95 (`time.Time`→`T` truncation + RFC3339 recommendation), IW-04/T98 (JSON `int64` precision + `FuzzCodecJSON` extension).
- [ ] Interop overclaims corrected: IW-01/T97 (CloudEvents binary 1.0 guarantee removed, structured promoted, `examples/cloudevents/`), IW-08/IW-09/IW-10/T96 (field-table cross-client scoping).
- [ ] Silent hazards documented: IW-05/IW-06/T99 (proto single-type + media-type, LATER-39 filed), IW-02/IW-03/IW-12 (folded into T97).
- [ ] Test-quality: IW-11/T100 (ContentType/ContentEncoding swap test with distinct non-empty values).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the new `interop` lane green; `goleak.VerifyNone` clean.
- [ ] README interop contract synced (CloudEvents binary scoped to 0-9-1 / structured = cross-ecosystem, `time.Time` header caveat, JSON int64 caveat, low-interop field-table types).
- [ ] SPEC §10 "Rev 13" note records the Lens-03 pass; no duplicate tasks created; exactly one new `LATER.md` entry (LATER-39).

## Phase 15 — Event-Driven-Architecture Re-review (Lens 04: pattern expressiveness, safe-default analysis, topology completeness)

Closes the Lens-04 adversarial spec validation
(`spec-validation/04-event-driven-architecture.md`, findings `EDA-01..EDA-18`;
brief `spec-validation/04-event-driven-architecture-plan.md`). Lens verdict:
**GO-WITH-CHANGES** — no *new* §1 message-loss bug (unbounded-DLQ/`Close`-loss are
already T65/T70), but ordered keyed streams at scale are effectively unsupported
(no consistent-hash, EDA-04), the platform-level unroutable black-hole has no
server-side net (EDA-01/EDA-02), the lossy delayed exchange is the easy retry tool
(EDA-05/EDA-06), `return nil` silently acks a batch poison (EDA-15), and several
boundaries are unstated (redrive, breaking schema evolution, structured-mode
routing opacity, layered fan-out). Owner decisions (2026-05-28): **pull into scope**
the `x-consistent-hash` ordered-keyed-stream primitive (EDA-04); **pull T68 + T69
forward** (alternate exchange + e2e bindings); **build a DLQ redrive helper**
(EDA-09); **document the schema-evolution boundary + versioned-type convention +
LATER-40** (EDA-13). **No new build-tag lane** — gates ride the existing integration
(3.13 + 4.x) + `chaos` lanes. Five findings are already owned by prior-lens tasks
and are **not** re-filed (EDA-07/08→T65, EDA-10→T91, EDA-11→T90, EDA-16→T70,
EDA-18 extends T85). One new `LATER.md` entry (LATER-40). Pattern claims are tested
by exercising the pattern on a live broker, not a unit of API in isolation. **Gate
task T101 runs first**; no SPEC edit to an affected section lands before its gate
returns. Per-task SPEC amendment lands in the same PR; a SPEC §10 "Rev 14" note
records the pass.

### [ ] T101 — Verification gates EG-1–EG-4 (real broker, existing integration lane, 3.13 + 4.x) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured on **both** versions (existing integration lane — **no new build-tag lane**) for: **EG-1** a publish to a non-existent/mis-routed exchange **without** `Mandatory()` succeeds silently (no error, no `OnReturn`, message gone) — confirm `Mandatory()` is the only client-side net; **EG-2** a short per-message-TTL message **behind** a long-TTL message fails to expire at its own TTL on a single queue (head-of-line blocking); **EG-3** a `BatchConsumer` handler returning `nil` emits one `basic.ack` with `multiple=true` over the **whole** delivery range; **EG-4** `SingleActiveConsumer` permits exactly one active consumer (no horizontal scale) **and** an `x-consistent-hash` exchange distributes by routing-key hash across N bound queues.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate.
- **Verify:** The integration lane (3.13 + 4.x) green with the EG assertions; the gate table is reviewable.
- **Files:** `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T09, T15, T18, T22. **(EDA gates, P0)**

### [ ] T102 — Consistent-hash exchange + partitioned ordered consumer (EDA-04) [P0] · M
- **Acceptance:**
  - [ ] An `x-consistent-hash` `ExchangeKind` is declarable in `Topology`; the partitioned-consumer wiring preserves per-key ordering across N queues each consumed by one consumer (ordered work scaled horizontally), eliminating the v0.1-only `SingleActiveConsumer + Concurrency(1)` (one active consumer, no scale) forced choice.
  - [ ] SPEC §6.3/§6.6 document the per-key-ordering-across-N-queues pattern and the rebalancing trade-off (changing the partition count reshuffles keys).
- **Verify:** EG-4 integration test: N consistent-hash-bound queues preserve per-key order while distributing across queues; `goleak` clean. `examples/partitioned_consume/` builds + smoke-runs.
- **Files:** `topology.go`, `consumer.go`, SPEC §6.3/§6.6, `examples/partitioned_consume/`, `topology_integration_test.go`, `README.md`.
- **Deps:** T101 (EG-4), T15, T18. **(EDA-04, P0)**

### [ ] T103 — Publisher-side unroutable safety / exchange-name validation (EDA-02) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.6 documents the silent-drop-without-`Mandatory()` behaviour (per EG-1) and the topology-drift risk (an `Exchange("oders")` typo publishes into the void).
  - [ ] An **optional** validation, when a `*Topology` is wired to the builder, checks the referenced exchange exists and warns/errors at `Build`; `Mandatory()` is recommended as the default discipline.
- **Verify:** A `PublisherFor[M]` given a `Topology` lacking the named exchange warns/fails at `Build`; the silent-drop behaviour is asserted + documented (EG-1).
- **Files:** `publisher.go`, `publisher_builder.go`, SPEC §6.6, a publisher build test, an integration test.
- **Deps:** T101 (EG-1), T13, T15. **(EDA-02, P1)** — complements T68 (server-side AE net).

### [ ] T104 — Durable retry-ladder example + per-message-TTL HOL doc (EDA-05/EDA-06) [P1] · M
- **Acceptance:**
  - [ ] `examples/retry_ladder/` ships a runnable, durable, **multi-tier TTL+DLX** backoff ladder (one queue per delay tier), so users don't reach for the lossy delayed-message exchange (R10-1, do-not-regress).
  - [ ] SPEC §6.5 documents the **per-message-TTL head-of-line-blocking** trap (RabbitMQ only expires from the head) and the queue-per-tier requirement (per EG-2).
- **Verify:** `examples-build` + `examples-smoke` green; the example demonstrates a message cycling through delay tiers and finally to the DLQ; EG-2 captures the HOL behaviour.
- **Files:** `examples/retry_ladder/`, SPEC §6.5, an integration/example smoke test, `README.md`.
- **Deps:** T101 (EG-2), T15, T31. **(EDA-05/EDA-06, P1)**

### [ ] T105 — DLQ redrive helper + example (EDA-09) [P1] · M
- **Acceptance:**
  - [ ] A minimal `DLQ → source` republish utility dedupes by `MessageID` (preserving at-least-once) and is observable (metric/log).
  - [ ] SPEC §6.6 documents the redrive pattern and its scope boundary; `examples/redrive/` ships.
- **Verify:** Integration: dead-letter N messages, run the helper, assert they reappear on the source queue exactly once per `MessageID`; `examples-build` + `examples-smoke` green; `goleak` clean.
- **Files:** a redrive helper (`redrive.go` or `dlq.go`), SPEC §6.6, `examples/redrive/`, an integration test, `README.md`.
- **Deps:** T13, T15, T18, T47. **(EDA-09, P1)** — DLQ bounds + Consumer DLX validation are T65.

### [ ] T106 — Event-evolution boundary + versioned-type convention + LATER-40 (EDA-13) [P2] · S
- **Acceptance:**
  - [ ] SPEC §6.9/§8 state the boundary: additive change is safe (lax JSON / Postel, decision 23); **breaking** evolution (field removal, rename, semantic change) is user-owned via **versioned type names** (`order.created.v2`) / a new exchange / dual-publish.
  - [ ] SPEC recommends the `Message.Type` discriminator convention for versioned events; an example or godoc branches on `Type` before decode.
  - [ ] **LATER-40** files a pluggable schema-registry/validation hook (prereq T106).
- **Verify:** Doc review of the boundary + convention; the `Type`-branching snippet builds.
- **Files:** SPEC §6.9/§8/§10, `message.go` (godoc), `LATER.md`, optionally an example.
- **Deps:** T09. **(EDA-13, P2)**

### [ ] T107 — Structured-mode CloudEvents routing-opacity doc (EDA-14) [P2] · S
- **Acceptance:**
  - [ ] SPEC §6.9 documents that a structured event (`application/cloudevents+json` body, the T97 cross-ecosystem path) is **opaque to broker routing** — the broker cannot route on a `type`/`subject` that lives in the body — and recommends setting the AMQP routing key / a routing header explicitly (or binary mode for 0-9-1 attribute routing).
- **Verify:** An example routes a structured CloudEvent by an explicitly-set routing key; doc review.
- **Files:** SPEC §6.9, `examples/cloudevents/` (or a routing example), `README.md`.
- **Deps:** T26. **(EDA-14, P2)** — coordinate with T97 (same §6.9 CloudEvents subsection).

### [ ] T108 — Batch partial-failure footgun doc + example (EDA-15) [P1] · S
- **Acceptance:**
  - [ ] SPEC §6.4 **prominently** (up front, not buried) documents that a `BatchConsumer` handler returning `nil` triggers one `basic.ack` with `multiple=true` over the **whole** range — acking an in-batch poison the handler never individually processed — and documents the per-delivery `Batch.Deliveries()` idiom for safe partial failure.
  - [ ] A worked example demonstrates per-delivery ack/nack for a 1-poison-in-N batch.
- **Verify:** EG-3 captures the `multiple=true` whole-range ack; the example asserts the poison is nacked while the rest are acked; `examples-build` + `examples-smoke` green.
- **Files:** SPEC §6.4, `examples/batch_consume/` (or a new partial-failure example), an integration test.
- **Deps:** T101 (EG-3), T22. **(EDA-15, P1)**

### [ ] T109 — RPC usage-guidance preamble (EDA-12) [P2] · XS
- **Acceptance:**
  - [ ] SPEC §6.7 carries a usage-guidance preamble framing RPC as a deliberate-use primitive: the synchronous-coupling-over-async-transport caveat, when to prefer a normal request/response *event* pair, and the blind-retry/amplification consequence (cross-link T91's opt-in structured-error reply mode).
- **Verify:** Doc review of the §6.7 preamble.
- **Files:** SPEC §6.7, `README.md` (if the roadmap copy references RPC).
- **Deps:** —. **(EDA-12, P2)** — coordinate with T91 (Lens-02 structured-error RPC).

### [ ] T110 — Consumer-tag pinning clarity (EDA-17) [P3] · XS
- **Acceptance:**
  - [ ] SPEC §6.1 documents the stable-hash-of-consumer-tag pinning to consumer connections, the hot-spot risk at low connection/consumer counts (all tags hash to one socket with the default 2 connections), and whether adding consumer connections migrates live consumers (and the reconnect cost) or only affects new ones.
  - [ ] If a code clarification is warranted (e.g. the rebalancing mechanism), it is scoped here.
- **Verify:** Doc review; if code touched, a unit test asserts the pinning/rebalancing behaviour.
- **Files:** SPEC §6.1, optionally `connection.go`/`consumer.go`.
- **Deps:** T07d, T18. **(EDA-17, P3)**

### Checkpoint — Phase 15 (Lens 04) closed
- [ ] T101 gate results (EG-1..EG-4) captured on the existing integration lane (3.13 **and** 4.x); results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced.
- [ ] Ordered keyed streams scale (EDA-04/T102): `x-consistent-hash` `ExchangeKind` + partitioned-consumer wiring + example preserve per-key order across N parallel-consumed queues; §6.6 documents the pattern + rebalancing trade-off.
- [ ] Unroutable black-hole closed: `x-alternate-exchange` exposed (EDA-01/T68); publisher-side exchange-name validation warns/errors at `Build` + silent-drop-without-`Mandatory()` documented (EDA-02/T103).
- [ ] Layered fan-out enabled (EDA-03/T69): exchange-to-exchange bindings declarable without breaking declare-once/deep-snapshot.
- [ ] Safe retry is the easy one (EDA-05/EDA-06/T104): `examples/retry_ladder/` ships (durable multi-tier TTL+DLX); §6.5 documents the per-message-TTL HOL trap + queue-per-tier; R10-1 warning preserved.
- [ ] DLQ lifecycle complete (EDA-09/T105): redrive helper republishes DLQ→source (dedupe by `MessageID`), observable; `examples/redrive/` ships; §6.6 documents the pattern.
- [ ] Boundaries stated: schema-evolution (EDA-13/T106, LATER-40 filed), structured-mode routing opacity (EDA-14/T107), RPC usage framing (EDA-12/T109), consumer-tag pinning (EDA-17/T110).
- [ ] Batch footgun defused (EDA-15/T108): §6.4 prominently documents the `return nil` → `multiple=true` trap + the per-delivery idiom; example covers 1-poison-in-N.
- [ ] `examples/idempotent_consume/` ships (EDA-18) via T85 + T38b; the §6.2.1 dangling reference closed.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green (incl. new examples' smoke run); `goleak.VerifyNone` clean.
- [ ] README "Available now / On the roadmap" synced (consistent-hash + alternate + e2e-binding topology, redrive helper, retry-ladder + schema-evolution guidance).
- [ ] SPEC §10 "Rev 14" note records the Lens-04 pass; no finding re-filed that a prior lens owns; exactly one new `LATER.md` entry (LATER-40).

## Phase 16 — SRE & Production-Operability Re-review (Lens 05: detect/respond/verify, recovery-amplification, capacity honesty)

Closes the Lens-05 adversarial spec validation
(`spec-validation/05-sre-operability.md`, findings `SRE-01..SRE-16`; brief
`spec-validation/05-sre-operability-plan.md`). Lens verdict: **GO-WITH-CHANGES** —
no *new* §1 silent-message-loss path (the registry footgun is a *loud* crash), and
the five highest operability blockers (R10-6/8/10/11/16 = T61/T63/T65/T66/T71) are
**already pulled into v0.1** by Lenses 01/02; this lens hardens their
detect/respond/verify acceptance rather than re-filing. What it *adds* is an
observability-correctness set: a metrics-registration footgun that crashes the
process on a double-`Dial` (SRE-10), a histogram blind above 5s (SRE-11), no
current-state degraded gauge (SRE-12), a readiness probe false-green over a dead
consumer (SRE-13), throughput numbers unreachable on any remote broker (SRE-14),
unbounded label cardinality (SRE-15), no operator runbook (SRE-16) — plus two
pull-forwards (T67 partial-pool boot, T72 half-open write). Owner decisions
(2026-05-28): **pull both** T67 + T72 forward; **extend** the default histogram
buckets (add 10s/30s/60s); **aggregate** consumer liveness into `Connection.Health`;
the throughput ceiling is a **doc-only** honesty fix (async-API stays LATER-34); and
the §8 "no globals" rule forces a **private per-`Connection` registry** default for
SRE-10. **No new build-tag lane** — gates ride the existing integration (3.13 + 4.x)
+ `chaos` lanes. Seven findings are already owned by prior-lens tasks and are
**not** re-filed (SRE-01→T61, 02→T63, 03→T65, 04→T66, 05→T71, 06→T62, 07→T70).
**No** new `LATER.md` entry. Operability claims are tested by exercising the signal
and the recovery on a live broker / `chaos` lane, not the code path in isolation.
**Gate task T111 runs first**; no SPEC edit to an affected section lands before its
gate returns. Per-task SPEC amendment lands in the same PR; a SPEC §10 "Rev 15"
note records the pass. Reverting any of the seven prior-lens pulls flips this lens
to NO-GO.

### [ ] T111 — Verification gates SG-1–SG-4 (unit + existing integration/`chaos` lanes, 3.13 + 4.x) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (unit + the **existing** integration/`chaos` lanes — **no new build-tag lane**) for: **SG-1** whether a second `Dial` in one process panics on duplicate Prometheus registration today (confirm the registerer is currently unspecified; a private-registry default removes the panic); **SG-2** whether a publish that stalls for the full 30s `ConfirmTimeout` lands in the `+Inf` bucket of `publisher_publish_seconds` under the default `[0.5…5000]`ms buckets; **SG-3** whether a channel-only consumer death (404/`basic.cancel`/ack-error, TCP up) leaves `Connection.Health(ctx)` returning OK while the consumer is unsubscribed; **SG-4** whether per-`Publish` throughput tracks `≈ pool/RTT` under injected confirm-RTT and a confirm spike drives `ErrChannelPoolExhausted`.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate.
- **Verify:** Unit + integration/`chaos` lanes (3.13 + 4.x where broker-bound) green with the SG assertions; the gate table is reviewable.
- **Files:** `metrics/*_test.go`, `connection_internal_test.go`, `*_integration_test.go`, `*_chaos_test.go`, `spec-validation/` (results table).
- **Deps:** T04, T07, T07d, T18, T84 (chaos lane). **(SRE gates, P0)**

### [ ] T112 — Prometheus registry injection: `WithMetricsRegisterer` + private-registry default (SRE-10) [P0] · S
- **Acceptance:**
  - [ ] `WithMetricsRegisterer(prometheus.Registerer)` is added; the connection-level default is a **private per-`Connection` registry**, never `prometheus.DefaultRegisterer` (a hidden global §8 forbids), wired into the existing `NewPrometheus*` constructors (which already accept an injected registerer but have no caller today).
  - [ ] SPEC §6.9/§6.1/§8 document the injection and the private-registry default.
  - [ ] **Lens-06 (GA-03):** this opt-in Prometheus *registry-injection* composes with T122's correction that the **default** metrics recorder is `NoOpClientMetrics` (not Prometheus); T122 corrects the §6.1 L511 SPEC table, the injection is wired here.
- **Verify:** SG-1 unit test: two `Dial`s in one process with default metrics do **not** panic; an injected-registerer test asserts metrics register into the provided registry.
- **Files:** `options_connection.go`, `connection.go`, `metrics/`, SPEC §6.9/§6.1/§8, `connection_test.go`, `README.md`.
- **Deps:** T111 (SG-1), T04, T07. **(SRE-10, P0)**

### [ ] T113 — Extend default latency histogram buckets (SRE-11) [P1] · S
- **Acceptance:**
  - [ ] The default `publisher_publish_seconds` / consumer latency buckets are extended past the current `[0.5…5000]`ms top (add `10s, 30s, 60s`) so the 30s `ConfirmTimeout` + ~20s reconnect-barrier envelope + cross-region RTT are visible, not collapsed into `+Inf`.
  - [ ] `WithLatencyBuckets` remains the override; SPEC §6.9 explains the envelope rationale (and that the barrier-cap SRE-02/T63 default should sit ≤ the new top bucket).
  - [ ] **Lens-09 (PC-12, confirm/do-not-regress):** confirm the SRE-11 extended buckets (`10s/30s/60s`) span the confirm-RTT capacity tail for p99/p999 (the default 5000 ms top bucket otherwise collapses 5–30s — with the 30s `ConfirmTimeout` — into `+Inf` exactly where capacity problems live); add a one-line §6.9 capacity-tail rationale. No re-file.
- **Verify:** SG-2 mock-channel unit test withholds the ack for 30s and asserts the observation lands in a **finite** bucket, not `+Inf`.
- **Files:** `metrics/`, SPEC §6.9, `metrics/*_test.go`, `README.md`.
- **Deps:** T111 (SG-2), T04. **(SRE-11, P1)**

### [ ] T114 — Current-state `connection_degraded{role}` gauge (SRE-12) [P2] · S
- **Acceptance:**
  - [ ] A `connection_degraded{role}` **gauge** (0/1) is set to 1 on entering degraded state and 0 on the first successful redeclare — the current-state signal the transition counter `connection_degraded_total` does not provide.
  - [ ] Listed in SPEC §6.9 mandatory metrics.
- **Verify:** Unit/`chaos` test drives a connection into degraded state, asserts the gauge reads 1, then 0 after recovery; `goleak` clean.
- **Files:** `connection.go`, `metrics/`, SPEC §6.1/§6.9, `connection_internal_test.go`, `README.md`.
- **Deps:** T04, T07, T16. **(SRE-12, P2)** — coordinate with T71's gauges (distinct metric, shared registration path).

### [ ] T115 — `Connection.Health` consumer-liveness aggregation (SRE-13) [P1] · M
- **Acceptance:**
  - [ ] `Connection.Health` aggregates consumer-subscription liveness: it returns non-nil while any registered consumer is not currently subscribed (closing the readiness/liveness probe false-green over a silently-dead consumer), in addition to the existing socket + topology-degraded checks.
  - [ ] SPEC §6.1 documents the semantics and that T61's channel-level self-heal returns `Health` to green once the consumer resubscribes.
- **Verify:** SG-3 `chaos` test forces a channel-only consumer death and asserts `Connection.Health` returns non-nil while unsubscribed, then nil after T61 reopens + resubscribes; `goleak` clean.
- **Files:** `connection.go`, `consumer.go`, `batch_consumer.go`, SPEC §6.1, `connection_internal_test.go`, a `chaos` test, `README.md`.
- **Deps:** T111 (SG-3), T07, T18, T61, T84 (chaos lane). **(SRE-13, P1)** — interacts with T61 (channel-level recovery).

### [ ] T116 — Honest throughput ceiling: §9/§6.2 RTT caveat + `pool/RTT` (SRE-14) [P1] · S
- **Acceptance:**
  - [ ] SPEC §9/§6.2 state prominently that the 30k/100k targets are **local-broker (sub-ms RTT)**, that the per-`Publish` ceiling is `pool/RTT` (a pooled channel is held for a full confirm RTT, R10-18/LATER-34), that a remote broker collapses the rate 1–2 orders of magnitude, and that a confirm-latency spike cascades into `ErrChannelPoolExhausted`.
  - [ ] Every throughput number carries its RTT + hardware + broker config; the async/streaming publish-API decision **remains LATER-34** (not pulled).
  - [ ] **Lens-09 (PC-02):** add the quantified pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT` per connection) so the `ErrChannelPoolExhausted` cascade onset under a confirm-latency spike (broker GC, disk sync, quorum Raft) is computable, not just narrated; confirm SG-4 covers the onset. dep PG-5.
- **Verify:** SG-4 injected-RTT test demonstrates the `pool/RTT` relationship and the `ErrChannelPoolExhausted` onset; the §9 benchmark prose states RTT/hardware/broker.
- **Files:** SPEC §9/§6.2, the benchmark suite (T44b), `README.md`.
- **Deps:** T111 (SG-4). **(SRE-14, P1)** — doc + benchmark caveat; coordinate with LATER-34.

### [ ] T117 — Metric label cardinality budget + `queue`/`exchange` opt-out (SRE-15) [P2] · S
- **Acceptance:**
  - [ ] SPEC §6.9 documents the cardinality budget (rough series-count math per `queue`/`exchange`) for billions/day across thousands of queues/exchanges.
  - [ ] An opt-out omits/aggregates the always-on `queue`/`exchange` labels for very-high-fan-out deployments (so they cannot OOM Prometheus during an incident).
- **Verify:** Unit test asserts the opt-out drops the `queue`/`exchange` label; doc review of the budget.
- **Files:** `metrics/`, `options_connection.go` (or the metrics-labels option), SPEC §6.9, `metrics/*_test.go`, `README.md`.
- **Deps:** T04, T19. **(SRE-15, P2)**

### [ ] T118 — Operator runbook (metric→action) + §1-bar→metric audit (SRE-16) [P2] · S
- **Acceptance:**
  - [ ] A runbook table (§9 or §6.9) maps each mandatory metric → detect signal → operator action → recovery-verify signal.
  - [ ] An explicit **§1 "no silent X" bar → metric + example alert query** audit shows every §1 bar has a corresponding always-on metric (surfacing the redelivery leading indicator SRE-05/T71 and the current-degraded signal SRE-12/T114); any bar without one is flagged.
- **Verify:** A doc test / review checklist asserts every §1 bar and every mandatory metric appears in the table with an alert query.
- **Files:** SPEC §9/§6.9/§1, `README.md`.
- **Deps:** T71, T114. **(SRE-16, P2)** — land last so the runbook references every metric the prior tasks added.

### Checkpoint — Phase 16 (Lens 05) closed
- [ ] T111 gate results (SG-1..SG-4) captured on unit + the **existing** integration/`chaos` lanes (3.13 **and** 4.x where broker-bound); results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced.
- [ ] Registry footgun closed (SRE-10/T112): `WithMetricsRegisterer` exists; the default is a private per-`Connection` registry (never `DefaultRegisterer`); a double-`Dial` does **not** panic (SG-1).
- [ ] Incident latency visible (SRE-11/T113): default buckets cover the 30s `ConfirmTimeout` + reconnect-barrier envelope; a 30s stall lands in a finite bucket, not `+Inf` (SG-2).
- [ ] Current-state degraded signal (SRE-12/T114): `connection_degraded{role}` gauge reads 1 while degraded, 0 after recovery; listed in §6.9.
- [ ] Readiness false-green killed (SRE-13/T115): `Connection.Health` returns non-nil while a registered consumer is unsubscribed, nil after resubscribe (SG-3); semantics documented.
- [ ] Capacity honesty (SRE-14/T116): §9/§6.2 state the local-broker caveat + `pool/RTT` ceiling + remote collapse + `ErrChannelPoolExhausted` cascade; every number carries RTT/hardware/broker; async-API stays LATER-34.
- [ ] Cardinality bounded (SRE-15/T117): §6.9 documents the budget + ships the `queue`/`exchange` opt-out.
- [ ] Runbook shipped (SRE-16/T118): metric→detect→respond→verify table + every §1 bar mapped to a metric + alert query.
- [ ] Pull-forwards landed: T67 (succeed-if-≥1-per-role + degraded-capacity boot signal, SRE-08); T72 (dialer keepalive + half-open-write test errors promptly, not at 30s, SRE-09).
- [ ] Cut-line endorsed: T61/T62/T63/T65/T66/T70/T71 each carry a `Lens-05 (SRE-xx)` detect/respond/verify acceptance bullet; none re-filed, none re-pulled.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the `chaos` lane green; `goleak.VerifyNone` clean.
- [ ] README observability/reliability copy synced (`WithMetricsRegisterer`, default-bucket change, `connection_degraded` gauge, `Health` consumer-liveness, cardinality opt-out, honest §9 ceiling).
- [ ] SPEC §10 "Rev 15" note records the Lens-05 pass; no finding re-filed that a prior lens owns; **no** new `LATER.md` entry.

## Phase 17 — Go API & Library-Design Re-review (Lens 06: discoverability, hard-to-misuse, forever-stable surface, safe zero values)

Closes the Lens-06 adversarial spec validation
(`spec-validation/06-go-api-library-design.md`, findings `GA-01..GA-16`; brief
`spec-validation/06-go-api-library-design-plan.md`). Lens verdict:
**GO-WITH-CHANGES** — the public surface is fundamentally sound (the
`PublisherFor[M]`/`ConsumerFor[M]` generics split, mostly safe zero-value defaults,
concrete-struct decision 9, a navigable error taxonomy), but the review found **one
Blocker that is a silent durability loss, not an API-shape flaw**: a zero-valued
`Message[M]{}` ships **non-persistent** on the wire because `buildPublishing`
(`publisher.go:946`) casts the `DeliveryMode` enum raw instead of translating `0→2`,
violating the §6.5 durable-by-default headline + the §1 no-silent-loss bar, and is
unverified by any wire-level test (GA-01/T120). Owner decisions (2026-05-28): GA-02
observability inheritance = **reword independent** (no inheritance; doc-only); GA-03
metrics default = **NoOp (correct the SPEC)**; GA-04 `PrefetchBytes` = **cut**; GA-05
exchange→exchange binding shape = **separate `Topology.ExchangeBindings`** (`Binding`
not reshaped). **No new build-tag lane** — gates GG-1..GG-4 are unit/mock-channel
characterizations on the existing unit lane; only GA-01's persistence assertion rides
the existing integration lane (3.13 + 4.x). Five findings are already owned by
prior-lens / Phase-11 tasks and are **not** re-filed (GA-03→T112, GA-05→T68/T69,
GA-06→T70/T71, GA-09→T37). Exactly **one** new `LATER.md` entry (LATER-41, a
dedicated `ReturnCode` accessor). **Gate task T119 runs first**; no SPEC edit to an
affected section, and no fix, lands before its gate returns. Per-task SPEC amendment
lands in the same PR; a SPEC §10 "Rev 16" note records the pass.

### [ ] T119 — Verification gates GG-1–GG-4 (unit + existing integration lane, 3.13 + 4.x) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (unit + the **existing** integration lane for the persistence check — **no new build-tag lane**) for: **GG-1** that a zero-valued `Message[M]{Body:&x}` currently produces `amqp091.Publishing.DeliveryMode == 0` (transient) — the §6.5 `0→2` mapping is **absent** in `buildPublishing` — and that such a message does **not** survive a broker restart; **GG-2** that with `Dial(WithTracer(realTracer))` and a builder that never calls `.Tracer(...)`, the publish path emits **NoOp spans** (no builder reads `conn.opts.tracer`/`metrics`); **GG-3** that with no `WithMetrics(...)` the default `Connection` metrics recorder is **`NoOpClientMetrics`** (not Prometheus) and there is **no** caller of `NewPrometheus*` in non-test code; **GG-4** that `PublisherFor[Order](conn).Codec(codec.NewProtobuf())` **compiles** and fails only at the first `Publish` with `ErrInvalidMessage`.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate; first task records §10 **Rev 16**.
- **Verify:** Unit + integration lane (3.13 + 4.x where broker-bound) green with the GG assertions; the gate table is reviewable.
- **Files:** `publisher_internal_test.go`, `connection_internal_test.go`, `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T07, T07d, T13, T18, T04. **(GA gates, P0)**

### [ ] T120 — Fix the DeliveryMode silent non-persistence (GA-01, Blocker) [P0] · S
- **Acceptance:**
  - [ ] `buildPublishing` translates enum→wire at the choke point: `DeliveryModePersistent(0)→2`, `DeliveryModeTransient(1)→1`; the `basic.return` rehydration path (`publisher.go:310`) is fixed the same way.
  - [ ] The §6.5 durable-by-default contract is kept **authoritative** (not weakened); the explicit wire-value table is present in §6.5.
- **Verify:** GG-1 unit test asserts `buildPublishing(Message[M]{Body:&x}).DeliveryMode == 2` and the transient case `== 1`; an integration test (3.13 **and** 4.x) publishes a zero-valued message, restarts the broker, and asserts it survives; `goleak` clean.
- **Files:** `publisher.go`, SPEC §6.5, `publisher_internal_test.go`, `*_integration_test.go`, `README.md`.
- **Deps:** T119 (GG-1), T11, T13. **(GA-01, P0)** — the lone Blocker; land first.

### [ ] T121 — Reword observability to independence (GA-02, High; owner decision) [P1] · XS
- **Acceptance:**
  - [ ] The "builder-overrides-connection" clause is struck from decision 44 and §6.1; the SPEC states tracer *and* metrics are configured **independently** at the connection and builder levels (each defaults to NoOp; connection-level observability covers lifecycle/pool events only).
  - [ ] §6.1 documents that to instrument a publisher/consumer the caller must set `.Tracer(...)`/`.Metrics(...)` on the builder.
- **Verify:** GG-2 unit test asserts a builder that never set `.Tracer(...)` emits NoOp spans even under a real connection tracer; the §6.1/decision-44 prose no longer promises inheritance.
- **Files:** SPEC §6.1/§10 dec.44, `publisher_internal_test.go`, `README.md`.
- **Deps:** T119 (GG-2). **(GA-02, P1)** — doc-only (matches the code).

### [ ] T122 — Make the metrics default honest: NoOp, not Prometheus (GA-03, Med; owner decision) [P1] · XS
- **Acceptance:**
  - [ ] §6.1 L511 + §3 L117 are corrected to "NoOp (opt-in Prometheus via `metrics.NewPrometheus*`)"; §9/§6.9 carry a one-sentence NoOp-default rationale (globals-free; inject your own registerer).
  - [ ] The registry-injection mechanics stay owned by **T112** (which carries the `Lens-06 (GA-03)` bullet); the two compose.
- **Verify:** GG-3 unit test asserts the default `Connection` metrics is `NoOpClientMetrics`; the SPEC table reads NoOp.
- **Files:** SPEC §6.1/§3/§9/§6.9, `connection_internal_test.go`, `README.md`.
- **Deps:** T119 (GG-3), T112. **(GA-03, P1)**

### [ ] T123 — Cut the `PrefetchBytes` no-op footgun (GA-04, Med; owner decision cut) [P2] · XS
- **Acceptance:**
  - [ ] `PrefetchBytes` is removed from `ConsumerBuilder` and `BatchConsumerBuilder`; it is listed in the §6 "intentionally not exposed" set alongside `Immediate()`/`NoLocal()`.
  - [ ] Decision 10 records the removal (was "kept with no-op note").
- **Verify:** The method no longer exists on either builder; a doc/grep test asserts no `PrefetchBytes` in the public surface; `go build ./...` + `make lint` clean.
- **Files:** `consumer_builder.go`, `batch_consumer_builder.go`, SPEC §6.3/§10 dec.10, `*_test.go`, `README.md`.
- **Deps:** T18, T22. **(GA-04, P2)** — pre-tag-safe removal (it never had an effect); must land before the first tag.

### [ ] T124 — Pin the topology roadmap additive: `ExchangeBindings` pre-spec + §9 gate (GA-05/GA-16; owner decision) [P1] · S
- **Acceptance:**
  - [ ] §6.6 specs a **separate `Topology.ExchangeBindings []ExchangeBinding`** with `ExchangeBinding{Source string, Destination string, RoutingKey string, NoWait bool, Args Headers}`; `Binding` is **not** reshaped (T69 implements against this shape; no `Source`/`Destination` rename); R10-13/T68 alternate-exchange stays additive via `Exchange.Args` / an optional field.
  - [ ] §9 carries an additive-only-after-first-tag gate ("no exported §6 type changes field names or removes fields after `v0.1.0`; topology extensions T68/T69/T102 and stream-consume v0.2 are additive-only") + a one-line `rc1`-is-pre-`v0.1.0` clarification.
  - [ ] Decision 24 commits the v0.2 stream-consume API to **purely additive** (`StreamOffset`/`StreamConsumerFor[M]` + additive `Delivery[M]` methods; `x-stream-offset` via `Args` in v0.1).
- **Verify:** §6.6 specs `ExchangeBinding`; the deep-snapshot/declare-once semantics extend to `ExchangeBindings`; T68/T69 carry the `Lens-06 (GA-05)` no-field-rename bullet.
- **Files:** SPEC §6.6/§9/§10 dec.24, `README.md`.
- **Deps:** T14, T15. **(GA-05/GA-16, P1)** — must complete before T46 cuts `v0.1.0`.

### [ ] T125 — Extension-tolerant observability interfaces: embeddable `metrics.NoOp` base (GA-06) [P1] · S
- **Acceptance:**
  - [ ] A SPEC policy (§6.9 note + a §10 decision): the `metrics`/`log`/`otel` user-implementable interfaces ship with an **embeddable NoOp base struct** (e.g. `metrics.NoOp`) users embed, so adding interface methods is forward-compatible (the embed satisfies new methods as no-ops).
  - [ ] The SPEC documents that all v0.1 metric additions (R10-15/T70, R10-16/T71) land **before the first tag** (§9 `// Deprecated`-free rc1→v1.0).
- **Verify:** An example shows a custom metrics adapter embedding the NoOp base surviving a method addition (compiles after a new method is added to the interface); T70/T71 carry the `Lens-06 (GA-06)` bullet.
- **Files:** `metrics/`, `log/`, `otel/`, SPEC §6.9/§10, `metrics/*_test.go`, an `examples/` snippet, `README.md`.
- **Deps:** T04, T05, T70, T71. **(GA-06, P1)** — must complete before T46 cuts `v0.1.0`.

### [ ] T126 — Error-model correctness: 311 classification, precedence, `AMQPCode` caveat (GA-07/GA-08/GA-15) [P2] · M
- **Acceptance:**
  - [ ] (GA-07) 311 is removed from the §6.8 transient list (code authoritative — 311 is permanent-only); the SPEC states the transient/permanent partition is **partial** and `ErrUnroutable` (312/313) is deliberately in **neither** set; precedence is defined — **`ErrTransient` in the chain wins** for re-classification (or `IsPermanent` returns false when `ErrTransient` is also present).
  - [ ] (GA-08) §6.8 warns `AMQPCode` MAY return a `basic.return` code (312/313) and callers needing to distinguish must combine with `errors.Is(err, ErrUnroutable)`; **LATER-41** files the dedicated `ReturnCode(err) (uint16, bool)` accessor.
  - [ ] (GA-15) §6.8 notes `ErrTopologyMismatch` is a named alias over `ErrPreconditionFailed`; §6.3 notes `ErrPoison` and a bare handler error are behaviourally identical (intent-only); any "~30 sentinels" figure is corrected to 40.
- **Verify:** A test asserts a 506-wrapped-with-`ErrTransient` classifies transient (the §6.8 L1957 re-classify path no longer drops); `errorlint` clean (`errors.Is`/`As` only).
- **Files:** `errors.go`, SPEC §6.8/§6.3, `LATER.md`, `errors_test.go`, `README.md`.
- **Deps:** T06. **(GA-07/GA-08/GA-15, P2)** — files LATER-41.

### [ ] T127 — Reconcile §6.1/§6.2 surface signature drift (GA-12) [P2] · S
- **Acceptance:**
  - [ ] Each drift is reconciled: `WithOnResubscribe` (phantom in the §6.1 table vs prose at L629 — resolved to table or prose, not both); `WithDialer` (documented `net.Dialer` vs the dial-func at `options_connection.go:176`); `WithFrameMax` `uint32` (not `int`); `WithChannelMax` `uint16` (untyped in table); `PublishResult{Index int; Err error}` vs `{Err error}` in code; §6.2 `Return.Body []byte` and `ReturnedProperties.Expiration` (`time.Duration`, not `string`).
  - [ ] For each, the SPEC matches the implementation (or the documented option is implemented where it is the intended contract).
- **Verify:** Every §6.1/§6.2 signature maps to a code `file:line`; the phantom option is resolved; `go build ./...` clean.
- **Files:** SPEC §6.1/§6.2, possibly `options_connection.go`/`publisher.go` where SPEC is the intended contract, `README.md`.
- **Deps:** T07, T13. **(GA-12, P2)**

### [ ] T128 — Document the deliberate `any`/generics/struct choices (GA-10/GA-11/GA-14) [P2] · S
- **Acceptance:**
  - [ ] A §10 decision: `codec.Codec` is intentionally **non-generic** (a payload↔codec mismatch is a runtime `ErrInvalidMessage`, not a compile error; each non-JSON codec documents its required `M` and fails fast), cross-referenced from §5/§8.
  - [ ] §6.5 explains `Message[M].Body *M` (publish/consume symmetry; loud nil-`Body` `ErrInvalidMessage`, never a silent drop); §6.9 has a `HeaderCodec` author caveat (full method set; recommend `var _ codec.HeaderCodec = (*MyCodec)(nil)`); §8 lists the closed set of sanctioned `any` (Headers / `*.Args` / `WithClientProperties` / OTel carriers; `log` printf variadics; the codec `v any`).
  - [ ] The GA-09 fixture unkeyed-literal guard note is recorded (coordinated with the T37 lightweight-fixture bullet).
- **Verify:** GG-4 doc example shows the runtime-mismatch contract; the §8 sanctioned-`any` list is auditable.
- **Files:** SPEC §10/§5/§6.5/§6.9/§8, a doc example, `README.md`.
- **Deps:** T119 (GG-4), T09, T24, T26, T37. **(GA-10/GA-11/GA-14, P2)**

### [ ] T129 — Naming-grammar carve-outs + last-wins scoping + `ChannelQoS` doc fix (GA-13) [P3] · XS
- **Acceptance:**
  - [ ] §5 carve-outs for: the lone `WithoutMetrics()` builder method (sanctioned metrics-disable exception); the `Use*`/`Allow*` verb-prefix builder methods; the noun-phrase setters (`MaxMessageSizeBytes`/`PublishBatchMaxSize`).
  - [ ] Decision 44's "last-wins" is scoped to **value-carrying** options; boolean flag-setters (`Mandatory`/`StampUserID`/`ChannelQoS`/`Exclusive`/`AutoAck`/`WithoutMetrics`) are documented as monotonic-set (no inverse).
  - [ ] The `consumer_builder.go:72` `ChannelQoS` godoc bug is fixed (says `global=false`; code sets `global=true`, `consumer.go:460`); the `basic.qos global=true` mapping is added to the §6.3 doc.
- **Verify:** §5 sanctions the four patterns; decision 44 scopes last-wins; the `ChannelQoS` godoc matches the code; `make lint` clean.
- **Files:** SPEC §5/§10 dec.44/§6.3, `consumer_builder.go` (godoc fix), `README.md`.
- **Deps:** T18, T19. **(GA-13, P3)** — land last so the docs reference the corrected surface.

### Checkpoint — Phase 17 (Lens 06) closed
- [ ] T119 gate results (GG-1..GG-4) captured on unit + the **existing** integration lane (3.13 **and** 4.x for the persistence check); results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced; first task records §10 **Rev 16**.
- [ ] Silent durability loss fixed (GA-01/T120): `buildPublishing` translates `DeliveryModePersistent(0)→wire 2`, `DeliveryModeTransient(1)→wire 1` (and the `basic.return` path); a unit test asserts the wire value, an integration test (3.13 **and** 4.x) proves a zero-valued message survives a broker restart; §6.5 contract unchanged.
- [ ] Silent observability loss documented (GA-02/T121): §6.1 + decision 44 state tracer and metrics are configured **independently** (no inheritance); a builder without `.Tracer(...)` emits NoOp spans even under a real connection tracer.
- [ ] Defaults honest (GA-03/T122): §6.1 L511 + §3 read "NoOp (opt-in Prometheus)"; the default `Connection` metrics is `NoOpClientMetrics`; T112 owns the registry-injection opt-in.
- [ ] Footgun removed (GA-04/T123): `PrefetchBytes` is gone from both builders, listed in §6 "intentionally not exposed"; decision 10 records the removal.
- [ ] Roadmap pinned additive (GA-05/GA-16/T124): §6.6 specs `Topology.ExchangeBindings []ExchangeBinding` (`Binding` untouched); §9 carries the additive-only gate + rc1 clarification; decision 24 commits the v0.2 stream API additive; T68/T69 carry the no-field-rename acceptance.
- [ ] Interfaces extension-tolerant (GA-06/T125): the `metrics`/`log`/`otel` interfaces ship an embeddable NoOp base; an example survives a method addition; T70/T71 land before the first tag.
- [ ] Error model sound (GA-07/GA-08/GA-15/T126): §6.8 lists 311 permanent-only; the precedence rule is specified + tested; the partial partition + `ErrUnroutable`-in-neither documented; the `AMQPCode` frame-class caveat exists; LATER-41 filed.
- [ ] Surface matches code (GA-12/T127): every §6.1/§6.2 signature maps to a code `file:line`; the `WithOnResubscribe` phantom is resolved.
- [ ] Deliberate choices documented (GA-09/GA-10/GA-11/GA-14/T128): §10 records the non-generic-codec decision; §6.5 explains `Body *M`; §6.9 has the `HeaderCodec` caveat; §8 lists the sanctioned `any`; the fixture guard is noted.
- [ ] Naming + last-wins honest (GA-13/T129): §5 carve-outs exist; decision 44 scoped to value-setters; the `ChannelQoS` godoc matches the code.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced (metrics-default correction, `PrefetchBytes` removal, `ExchangeBindings`, independent-observability semantics).
- [ ] SPEC §10 "Rev 16" note records the Lens-06 pass; no finding re-filed that a prior task owns (GA-03→T112, GA-05→T68/T69, GA-06→T70/T71, GA-09→T37); exactly **one** new `LATER.md` entry (LATER-41); T119–T129 contiguous, no duplicate IDs.

## Phase 18 — Security & Threat-Modeling Re-review (Lens 07: credential confidentiality, fail-closed controls, untrusted-input boundedness, supply-chain surface)

Closes the Lens-07 adversarial spec validation
(`spec-validation/07-security-threat-modeling.md`, findings `ST-01..ST-14`; brief
`spec-validation/07-security-threat-modeling-plan.md`). Lens verdict:
**GO-WITH-CHANGES** — the posture is fundamentally sound (the `internal/redact`
choke-point holds on owned egress incl. wrapped errors; the codec/handler
panic-recover is type-only `%T`; bodies are never logged and never decompressed;
internal buffers are bounded; `403` is permanent and never publish-retried; SASL
EXTERNAL is fail-closed-validated; `UserID` is client-side-checked) — but the review
found **one must-fix Blocker, a fail-open inbound DoS**: there is **no consume-side
message-size cap** (`MaxMessageSizeBytes` §6.2 L796 is publish-side only), so a single
hostile/buggy producer ships a ~512 MiB body that `amqp091` reassembles **in memory**
before the codec, OOMing the consumer (security analog of the Lens-06 GA-01 Blocker;
previously tracked **LOW as LATER-35**, now **re-classified Blocker and promoted to
T131**); plus **one High** fail-open confidentiality gap (ST-01: PLAIN credentials
base64-cleartext over `amqp://` with no warning). Owner decisions (2026-05-29,
recommended dispositions, overridable before execution): **D1** inbound cap default
**16 MiB** (`0` disables), over-cap → `ErrMessageTooLarge` + `Nack(requeue=false)` +
drop metric, per-message for `BatchConsumer`; **D2** PLAIN-cleartext = **warn-only at
Dial** for v0.1; **D3** codec split = **build-tag** CloudEvents (non-breaking);
**D4** EXTERNAL principal = **doc-only** (CN + `ssl_cert_login_from` divergence;
config → LATER-42); **D5** reconnect on permanent auth = **stop + surface
`ErrAccessRefused` + degrade** (no 403 loop). **No new build-tag lane for the gates**
— TG-1..TG-5 ride the existing unit + `integration` lanes (3.13 + 4.x); the T135
codec build-tag is a *compilation* tag, not a CI lane. **One** finding is already
owned by a prior-lens task and is **not** re-filed (ST-08→**T65**, gains a `Lens-07
(ST-08)` bullet). Exactly **one** new `LATER.md` entry (LATER-42; LATER-44 only if D3
is overridden to defer); the ST-06 fix **promotes/supersedes the pre-existing
LATER-35**. **Gate task T130 runs first**; no SPEC edit to an affected section, and no
fix, lands before its gate returns. Per-task SPEC amendment lands in the same PR; a
SPEC §10 "Rev 17" note records the pass.

### [ ] T130 — Verification gates TG-1–TG-5 (unit + existing `integration` lane, 3.13 + 4.x) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (unit + the **existing** `integration` lane where broker-bound — **no new build-tag lane**) for: **TG-1** that a real `amqp091.Dial`/`DialConfig` failure with `amqp://user:secret@badhost` does **not** surface `secret` in warren's returned/wrapped error chain, any `log` adapter line, **or** the `amqp091` package `Logger` (capture the raw driver error-string shape); **TG-2** that a single ~256 MiB body is reassembled by `amqp091` **in memory before the codec runs** with **no consume-side cap** (measure consumer RSS; confirm it scales with body size independent of `FrameMax`); **TG-3** that EXTERNAL principal extraction = **CN** of the first client cert (`connection.go:122`) and how the client-side `UserID` check behaves under `ssl_cert_login_from` = `common_name` / `distinguished_name` / `subject_alternative_name`; **TG-4** whether forcing a reconnect against revoked creds makes the supervisor loop on **403 `ACCESS_REFUSED`** indefinitely (capture loop timing + log-spam) or surface/degrade; **TG-5** via `go list -deps ./...` + `go mod graph` the transitive surface `cloudevents/sdk-go/v2` adds to a **core** import and that a user cannot avoid it today.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate; first task records §10 **Rev 17**.
- **Verify:** Unit + integration lane (3.13 + 4.x where broker-bound) green with the TG assertions; the gate table is reviewable.
- **Files:** `connection_internal_test.go`, `consumer_internal_test.go`, `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T07, T07c, T07d, T13, T18, T34b. **(ST gates, P0)**

### [ ] T131 — Consume-side `MaxInboundMessageSizeBytes`: close the fail-open inbound DoS (ST-06, Blocker; supersedes LATER-35) [P0] · M
- **Acceptance:**
  - [ ] `MaxInboundMessageSizeBytes` added to `ConsumerBuilder` and `BatchConsumerBuilder` (default **16 MiB** mirroring the publish guard; `0` disables explicitly); **per-message** for `BatchConsumer`.
  - [ ] An oversized delivery is rejected **before** the codec runs, fail-closed: `Nack(requeue=false)` + a classifiable `ErrMessageTooLarge` (DLQ if wired, observable, never a silent drop) + a `too_large` drop metric.
  - [ ] §6.2 frame-size prose (L703–727) gains the "frame-max bounds frames, not the reassembled body — the inbound cap is the body guard" note; §6.3 documents the symmetry with the publish guard and that the cap measures the **encoded body** (cf. LATER-37 for the HeaderCodec-header gap); **LATER-35 promoted/superseded** (re-classified LOW→Blocker by the security lens).
- **Verify:** TG-2 integration test publishes an over-cap body and asserts the consumer RSS stays bounded and the message lands in the DLQ (when wired), not an OOM; a unit test asserts the pre-codec reject + the `too_large` metric; `goleak` clean.
- **Files:** `consumer_builder.go`, `batch_consumer_builder.go`, `consumer.go`, `batch_consumer.go`, `errors.go`, SPEC §6.2/§6.3, `metrics/`, `*_test.go`, `*_integration_test.go`, `LATER.md`, `README.md`.
- **Deps:** T130 (TG-2), T18, T22, T47. **(ST-06, P0)** — the lone Blocker; land first.

### [ ] T132 — `Dial`-time warning for PLAIN credentials over plaintext `amqp://` (ST-01, High; owner decision D2 warn-only) [P1] · S
- **Acceptance:**
  - [ ] A `Dial` with `WithAuth`/PLAIN over a non-TLS `amqp://` endpoint emits a warning through the `log` adapter (password travels base64-cleartext); §6.1 Authentication documents the exposure alongside the EXTERNAL fail-closed block.
  - [ ] Warn-only for v0.1 (no behaviour break; `amqp://` still works); an opt-in acknowledgement (`AllowInsecureAuth()`) is noted for v1.0.
  - [ ] **Lens-11 (DP-08):** security of processing (GDPR Art. 32 / LGPD Art. 46) — the `Dial`-time warning + the §6.1 text name **personal data in transit** (not only credentials) as the Art. 32 exposure and discourage `amqp://` for any flow carrying personal data.
- **Verify:** A unit test asserts the warning fires for PLAIN-over-`amqp://` and that EXTERNAL-over-`amqp://` remains a fail-closed `ErrInvalidOptions` (decision 35 unchanged); `make lint` clean.
- **Files:** `connection.go`, `options_connection.go`, SPEC §6.1, `connection_internal_test.go`, `README.md`.
- **Deps:** T07, T34b. **(ST-01, P1)**

### [ ] T133 — Guarantee wrapped-error redaction + neutralise the `amqp091` `Logger` (ST-02, Med) [P1] · S
- **Acceptance:**
  - [ ] §8 makes explicit that the redaction guarantee covers errors **wrapped from `amqp091`** (not only wrapper-formatted strings); the wrapped dial error stays re-redacted (`connection.go:397`), now spec-pinned for wrapped errors.
  - [ ] The `amqp091` package-level `Logger` (the un-owned egress) is pinned to a redacting adapter or a no-op by default, **or** documented that callers who enable it must redact.
- **Verify:** An **end-to-end** test dials a bad host with `amqp://user:secret@…` and asserts `secret` appears in no returned error string, no `log` line, and no `amqp091` `Logger` output; `errorlint` clean.
- **Files:** `connection.go`, SPEC §8/§6.9, `connection_internal_test.go`, `*_integration_test.go`, `README.md`.
- **Deps:** T130 (TG-1), T07c. **(ST-02, P1)**

### [ ] T134 — Payload-safe egress: never-log-bodies + panic-value type-only (ST-03/ST-14, Med + Low-Med) [P2] · S
- **Acceptance:**
  - [ ] (ST-03) "Never log message payloads / bodies" is added to the §8 *Never* list (today only credentials); a §9 criterion is backed by a grep/AST test that no non-test code path formats a body into a log/error string.
  - [ ] (ST-14) §6.9 L2047 "wrapping the recovered value" is corrected to "wrapping the recovered value's **type only — never its content**" (matches the code, which stores `%T`).
  - [ ] **Lens-11 (DP-04):** integrity & confidentiality (GDPR 5(1)(f)/Art. 32 / LGPD Art. 46) — the "never log message bodies / header values" §8 *Never* boundary is framed as a **data-protection** control (a body/header debug log is unlawful processing **+ retention** of personal data in the log store, not only a secrets leak); the grep/AST regression test is the privacy lock.
- **Verify:** A runtime test that `OnReturn` and the decode-error path emit no body bytes; a panic-with-payload test asserts no payload bytes in the resulting error string; the grep/AST test passes.
- **Files:** SPEC §8/§6.9/§9, `consumer_test.go`, `publisher_test.go`, a grep/AST guard test, `README.md`.
- **Deps:** T13, T18, T73. **(ST-03/ST-14, P2)** — code already correct; locks the spec + adds regression tests.

### [ ] T135 — Build-tag the CloudEvents codec to keep the core dependency-light (ST-10, Med; owner decision D3 build-tag) [P1] · M
- **Acceptance:**
  - [ ] The CloudEvents codec is behind a `//go:build` tag so a core (non-cloudevents) import does **not** pull `cloudevents/sdk-go/v2`'s transitive closure (Protobuf assessed the same way); §2 (deps) + §6.9 amend to state the core stays dependency-light and how to opt into the heavy codecs.
  - [ ] If the owner overrides D3 to a **sub-module** split (breaking, import-path change → §8 Ask-first) or to accept+document, **LATER-44** is filed for the full split and T135 ships as doc-only.
- **Verify:** A build/import test proves a core import excludes `cloudevents/sdk-go/v2` (TG-5 surface quantified); `make build` + `make examples-build` green with and without the codec tag.
- **Files:** `codec/cloudevents.go` (+ build tags), `go.mod`, SPEC §2/§6.9, a build/import test, possibly `Makefile`/CI, `README.md`.
- **Deps:** T130 (TG-5), T25, T26. **(ST-10, P1)** — may file LATER-44 only if D3 overridden.

### [ ] T136 — Document EXTERNAL CN principal extraction + `ssl_cert_login_from` divergence (ST-04, Med; owner decision D4 doc-only) [P2] · XS
- **Acceptance:**
  - [ ] §6.5/§6.1 + decision 35 document that warren extracts the EXTERNAL principal from the cert **CN** (`connection.go:122`) and that RabbitMQ's `ssl_cert_login_from` (CN / DN / SAN) must match or the client-side `UserID` check diverges (false reject, or a value the broker 406s).
  - [ ] The R10-4 caveat (L3070) is extended to the **extraction** divergence (not only username-rewriting backends); empty `UserID` is recommended under non-CN broker mappings; **LATER-42** is filed for configurable SAN/DN extraction.
- **Verify:** The §6.5/§6.1 + decision 35 wording matches `connection.go:122`; TG-3's characterisation is cited; `LATER.md` has a well-formed LATER-42.
- **Files:** SPEC §6.1/§6.5/§10 dec.35, `LATER.md`, `README.md`.
- **Deps:** T130 (TG-3), T13, T34b. **(ST-04, P2)** — files LATER-42.

### [ ] T137 — `InsecureSkipVerify` `Dial`-time warning + TLS-floor note (ST-11, Low-Med) [P2] · XS
- **Acceptance:**
  - [ ] A `Dial` with `InsecureSkipVerify=true` on an `amqps://` connection emits a warning through the `log` adapter (a partial doc-only mitigation today, L758–759 → a runtime control).
  - [ ] §6.1 states warren relies on Go's default min TLS (1.2+) and never overrides the caller's `*tls.Config` (never sets `InsecureSkipVerify`, `options_connection.go:114`). Non-breaking.
- **Verify:** A unit test asserts the warning fires for `InsecureSkipVerify=true` on `amqps://` and is silent otherwise; `make lint` clean.
- **Files:** `connection.go`, SPEC §6.1, `connection_internal_test.go`, `README.md`.
- **Deps:** T07. **(ST-11, P2)**

### [ ] T138 — Specify reconnect on a permanent auth failure: no infinite 403 loop (ST-09, Med; owner decision D5 stop+surface+degrade) [P1] · M
- **Acceptance:**
  - [ ] The supervisor does **not** loop on `403 ACCESS_REFUSED` indefinitely; on a permanent auth/authorization failure during re-dial or redeclare it surfaces `ErrAccessRefused`, stops or applies bounded backoff (confirmed against TG-4), and fires the degraded signal (`WithOnTopologyDegraded`-style).
  - [ ] §6.1/§6.8 amend; coordinates with T61/T63/T66 (reconnect supervisor) and T79 (channel-vs-connection reply-code annotation) — distinct findings, cited not re-filed.
- **Verify:** A chaos/integration test revokes creds, forces a reconnect, and asserts the supervisor surfaces `ErrAccessRefused` + bounded backoff/stop + the degraded signal, not an unbounded 403 loop; `goleak` clean.
- **Files:** `connection.go`, `internal/reconnect`, SPEC §6.1/§6.8, `*_integration_test.go`, `README.md`.
- **Deps:** T130 (TG-4), T07, T61, T63, T66. **(ST-09, P1)** — cites T61/T63/T66/T79.

### [ ] T139 — State the decompression-bomb boundary (ST-07, Med — doc; rides T131) [P2] · XS
- **Acceptance:**
  - [ ] §6.5/§8 state that decompression is the **caller's** responsibility (the lib never decompresses; `ContentEncoding` is metadata-only; no `compress/*` import) and recommend a bounded (`io.LimitReader`-wrapped) decompressor.
  - [ ] §6.5 notes the T131 inbound cap applies to the **compressed** wire body (pre-inflation) — the cap alone does not bound the inflated size; the caller must bound that too.
- **Verify:** The §6.5/§8 wording is present and references T131's pre-inflation boundary; no code change required (boundary doc).
- **Files:** SPEC §6.5/§8, `README.md`.
- **Deps:** T131. **(ST-07, P2)** — rides T131.

### [ ] T140 — Extend fuzzing: `FuzzAMQPCode` + field-table encoder (ST-13, Low; coord T98) [P3] · S
- **Acceptance:**
  - [ ] `FuzzAMQPCode` fuzzes the `internal/amqperror` reply-code translation over a malformed `*amqp091.Error`; a field-table encoder fuzz/round-trip covers the `message.go` typing path (both parse attacker-influenced input, currently un-fuzzed).
  - [ ] §7 amends the fuzz-target list; coordinates with T98 (Lens-03, extends `FuzzCodecJSON` for `int64`) — same surface, different targets; confirms `FuzzXDeathParser` + `FuzzCodecCloudEventsBinary` already cover their surfaces.
- **Verify:** `go test -run=Fuzz` smoke + a short `-fuzz` run is green for both new targets; `make lint` clean.
- **Files:** `internal/amqperror/*_test.go`, `message_test.go`, SPEC §7, `README.md`.
- **Deps:** T06, T08. **(ST-13, P3)** — coord T98.

### [ ] T141 — Document the accepted trace-context spoofing risk (ST-05, Low — risk-accepted) [P3] · XS
- **Acceptance:**
  - [ ] §6.9 states that caller/upstream `traceparent`/`tracestate` win last-wins over warren's injected values (L2033–2042), so a hostile producer can forge/oversize them (trace poisoning); the risk is **accepted** under producer-trust and trace context MUST NOT drive security/authorization decisions.
  - [ ] The encryption-at-rest / message-level-payload-encryption boundary is noted here (application concern, out of scope for a transport wrapper).
  - [ ] **Lens-11 (DP-07):** security of processing (GDPR Art. 32 / LGPD Art. 46) — the at-rest boundary is framed as the **operator's Art. 32 "appropriate technical measures"** responsibility (bodies are **plaintext** on broker disk + backups; RabbitMQ does not encrypt at rest); disk/volume encryption is the operator's, message-level payload encryption (and crypto-erasure) is an application concern (→ LATER-47).
- **Verify:** The §6.9 threat-model note is present and explicit about the no-security-decisions rule; no code change required.
- **Files:** SPEC §6.9, `README.md`.
- **Deps:** T28. **(ST-05, P3)**

### [ ] T142 — §9 security-success-criteria capstone (ST-12, Low, high-leverage) [P2] · S
- **Acceptance:**
  - [ ] §9 gains criteria/tests for: the inbound size cap (ST-06/T131), the PLAIN-cleartext warning (ST-01/T132), never-log-payloads (ST-03/ST-14/T134), e2e wrapped-error redaction (ST-02/T133), the `InsecureSkipVerify` warning (ST-11/T137), and the new fuzz targets (ST-13/T140).
  - [ ] Each new criterion has a backing test (depends on the WS-1..WS-5 controls landing).
- **Verify:** The §9 security criteria are present and each maps to a green test; `go test -race ./...` covers them.
- **Files:** SPEC §9, `*_test.go` (cross-cutting), `README.md`.
- **Deps:** T131, T132, T133, T134, T137, T140. **(ST-12, P2)** — lands last; asserts the controls T131–T141 added.

### Checkpoint — Phase 18 (Lens 07) closed
- [ ] T130 gate results (TG-1..TG-5) captured on unit + the **existing** `integration` lane (3.13 **and** 4.x where broker-bound); results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced; first task records §10 **Rev 17**.
- [ ] Inbound DoS closed (ST-06/T131, Blocker): a consume-side `MaxInboundMessageSizeBytes` (default 16 MiB; `0` disables) rejects an oversized delivery **before** the codec with `ErrMessageTooLarge` + `Nack(requeue=false)` + a drop metric; an integration test proves the RSS stays bounded and the message lands in the DLQ (when wired), not an OOM; LATER-35 promoted/superseded.
- [ ] Cleartext-auth warning (ST-01/T132): a `Dial` with PLAIN over `amqp://` warns; EXTERNAL-over-`amqp://` stays a fail-closed `ErrInvalidOptions`.
- [ ] Egress credential-safe end-to-end (ST-02/T133): an e2e test proves `secret` leaks into no returned error, no `log` line, and no `amqp091` `Logger` output; the `amqp091` `Logger` is pinned/redacted/documented.
- [ ] Egress payload-safe (ST-03/ST-14/T134): §8 *Never* lists "log message payloads"; §6.9 says the panic-recover wraps the recovered value's **type only**; the grep/AST + panic-with-payload tests pass.
- [ ] Supply-chain surface shrunk (ST-10/T135): a build/import test proves a core import excludes `cloudevents/sdk-go/v2` (build-tag) — or the split is deferred to LATER-44 with the surface documented in §2.
- [ ] EXTERNAL principal documented (ST-04/T136): §6.5/decision 35 document the CN extraction + `ssl_cert_login_from` divergence and recommend empty `UserID` under non-CN mappings; LATER-42 filed.
- [ ] `InsecureSkipVerify` warning (ST-11/T137): a `Dial` with `InsecureSkipVerify=true` on `amqps://` warns; §6.1 states the Go-default min-TLS floor + verbatim-config policy.
- [ ] Reconnect on permanent auth (ST-09/T138): a chaos/integration test asserts the supervisor surfaces `ErrAccessRefused` + bounded backoff/stop + the degraded signal, not an unbounded 403 loop; cites T61/T63/T66/T79.
- [ ] Residual risks stated/tested (ST-05/ST-07/ST-13): the trace-spoofing + decompression boundaries are documented; `FuzzAMQPCode` + a field-table fuzz are added and green.
- [ ] §9 capstone (ST-12/T142): the new security success criteria are present and each has a backing test.
- [ ] Cross-lens (ST-08): T65 carries a `Lens-07 (ST-08)` bullet and a test that the default DLQ bound holds under an adversarial poison flood; not re-filed.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced (the inbound size cap, the cleartext-auth warning, the codec build-tag, the `InsecureSkipVerify` warning, the EXTERNAL CN caveat).
- [ ] SPEC §10 "Rev 17" note records the Lens-07 pass; no finding re-filed that a prior task owns (ST-08→T65); the ST-06 fix promotes/supersedes LATER-35; exactly **one** new `LATER.md` entry (LATER-42; LATER-44 only if D3 is overridden); T130–T142 contiguous, no duplicate IDs.

## Phase 19 — Go Concurrency & Runtime-Correctness Re-review (Lens 08: goroutine lifecycles, reader-fed/supervisor-critical callbacks, race/deadlock/leak-freedom, every "race-free"/"idempotent" claim a real primitive)

Closes the Lens-08 adversarial spec validation
(`spec-validation/08-go-concurrency-runtime.md`, findings `CR-01..CR-13`; brief
`spec-validation/08-go-concurrency-runtime-plan.md`). Lens verdict: **GO-WITH-CHANGES**
— the architecture is sound (the return/confirm demux is a single goroutine over an
*intentionally* unbuffered return channel per R10-3; every message-data buffer is bounded
— dispatch by prefetch, confirm by batch size, pool by capacity; the confirm tracker
resolves waiters via a one-shot send with no leak; `started`/`Close` use `atomic.Bool`
CAS / `sync.Once`; the `Batch[M]` guard is correct; the barrier's AB/BA lock order is
handled) — but the review found **one must-fix Blocker, a logical lost-update**: counter
B of the two-counter `MaxRedeliveries` map (`consumer.go:767→782`) is a **non-atomic
read-modify-write**, so under `Concurrency(n>1)` concurrent redeliveries of the same key
undercount and a poison message loops **past** its limit — and because `sync.Map` is
memory-safe, `go test -race` **cannot** catch it, so decision 12 / T20's "race-free,
verified with `-race`" is a **false guarantee**; plus **one High** liveness footgun
(CR-01: a user `OnReturn` runs **inline on the unbuffered-return demux goroutine**
(`publisher.go:226`), so a blocking callback stalls `amqp091`'s per-connection reader →
heartbeats stop → the broker drops the socket). Owner decisions (2026-05-29, recommended,
overridable): **D1** `OnReturn` = keep `MarkReturned` synchronous + add the missing
recover + **dispatch the user callback to a bounded (1-deep) per-publisher worker**
(documents the timing change); **D2** counter-B = **per-channel mutex** across
load-increment-store + a **behavioural** test (not `-race`-only); **D3** `Delivery` guard
= **single `atomic.Bool` CAS** (consider unifying `Batch[M]`); **D4** confirm-tracker =
**document the per-call boundary + defer the aggregate window to LATER-43**; **D5**
non-cooperative handler = **detach + metric + goleak carve-out** (caller defect). **No new
build-tag lane** — CG-1..CG-6 ride the existing unit / `-race` / `integration` lanes
(3.13 + 4.x) or `amqpmock`/`amqptest`. **Nine** findings **extend an existing task in
place** (cross-lens; `Lens-08 (CR-xx)` bullet, not re-filed): T20 (CR-02), T07
(CR-03/CR-11), T60 (CR-04), T34c (CR-05), T18 (CR-06), T08 (CR-08), T70 (CR-09), T45
(CR-10), T13 (CR-01 coordination). Exactly **one** new `LATER.md` entry (LATER-43).
**Gate task T143 runs first**; no SPEC edit to an affected section, and no fix, lands
before its gate returns. Per-task SPEC amendment lands in the same PR; a SPEC §10 "Rev 18"
note records the pass.

### [ ] T143 — Verification gates CG-1–CG-6 (unit + `-race` + existing `integration` lane, 3.13 + 4.x; no new build-tag lane) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (unit + `-race` + the **existing** `integration` lane where broker-bound, or `amqpmock`/`amqptest` — **no new build-tag lane**) for: **CG-1** the counter-B lost update reproduces (N goroutines, `Concurrency(n>1)`, same `(channel-instance-id, MessageID)` key on one channel → stored count **below** the true increment count; `go test -race` **passes** while the count is wrong; quantify how far past `MaxRedeliveries` a poison loops); **CG-2** a blocking `OnReturn` on a mandatory-unroutable publish stalls confirms for other in-flight publishes on the same channel **and** backs up `amqp091`'s reader/heartbeats (broker drops the socket) — capture timing; **CG-3** a handler that times out then late-`Ack`s on another goroutine emits a **second frame** today → `PRECONDITION_FAILED` → channel close → sibling in-flight handlers die, and the atomic-CAS guard makes the late call a no-op; **CG-4** a **1000**-cycle connect/disconnect + confirm-churn loop under `goleak.VerifyNone` (leaked count; every §11-inventory goroutine joined); **CG-5** a panic in (a) the reconnect-loop `connect` fn and (b) the resubscribe hook on the supervisor (current blast radius: process crash / silent reconnect-disable; recover degrades instead); **CG-6** under sustained pool exhaustion Acquire is **not** FIFO (max wait / starvation).
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate; first task records §10 **Rev 18**.
- **Verify:** Unit + `-race` + integration lane (3.13 + 4.x where broker-bound) green with the CG assertions; the gate table is reviewable.
- **Files:** `consumer_internal_test.go`, `publisher_internal_test.go`, `connection_internal_test.go`, `internal/reconnect/*_test.go`, `channelpool_internal_test.go`, `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T07, T08, T13, T18, T20, T34c, T45, T60, T70. **(CR gates, P0)**

### [ ] T144 — Callback invocation-goroutine contract + `OnReturn` dispatch decision (CR-01, High; owner decision D1) [P1] · M
- **Acceptance:**
  - [ ] §6.1/§6.2/§6.3 **name the invocation goroutine for every `On*` callback** (brief §12 inventory: `OnReturn` on the unbuffered-return demux; `OnReconnect`/`OnBlocked`/resubscribe on the supervisor, *inside* the open barrier; `OnTopologyDegraded` safe-dispatched) and state the **must-not-block / no-I/O contract** for the reader-fed and supervisor-critical ones (a blocking callback stalls the connection reader / holds the barrier).
  - [ ] Per **D1**: `MarkReturned` stays synchronous (R10-3 load-bearing); the user `OnReturn` gains a **panic-recover** (coordinate T34c) and is **dispatched to a bounded (1-deep) per-publisher worker** — documenting the timing change from "synchronously before `Publish` unblocks" to "concurrently with / shortly after"; the alternative (synchronous + loud doc + watchdog) is recorded. Extends **T13** (the `OnReturn` timing wording now names the goroutine).
- **Verify:** A test (CG-2 harness) asserts a blocking `OnReturn` no longer stalls confirms on its channel and the connection reader / heartbeats stay live; `goleak` clean.
- **Files:** `publisher.go`, `connection.go`, SPEC §6.1/§6.2/§6.3, `metrics/`, `publisher_internal_test.go`, `*_integration_test.go`, `README.md`.
- **Deps:** T143 (CG-2), T13, T34c. **(CR-01, P1)**

### [ ] T145 — Confirm-tracker aggregate-memory boundary + LATER-43 (CR-07, Med; owner decision D4 document+defer) [P2] · S
- **Acceptance:**
  - [ ] §6.2 documents the confirm-tracker memory bound is **per-call** (`PublishBatchMaxSize`), **not** aggregate — N concurrent `PublishBatch`/`Publish` calls hold N independent windows, an unbounded growth surface under publisher fan-out (already admitted §6.2 L930), owned by the *publisher*; recommend caller-side fan-out limiting.
  - [ ] **LATER-43** is filed for an optional aggregate in-flight window (`WithMaxInFlightConfirms`, default off). The one-shot per-waiter resolve (`tracker.go:211-214`) + `Wait`-deletes-own-entry + `CloseAll` stay do-not-regress.
- **Verify:** The §6.2 per-call-boundary wording is present and references the fan-out recommendation; `LATER.md` has a well-formed LATER-43; no code change required (boundary doc).
- **Files:** SPEC §6.2, `LATER.md`, `README.md`.
- **Deps:** T13. **(CR-07, P2)** — files LATER-43.

### [ ] T146 — Concurrency capstone: goroutine-inventory appendix + §9 criteria + close-idempotency pin (CR-13 + CR-12) [P2] · S
- **Acceptance:**
  - [ ] §7/§9 gain a **goroutine-inventory appendix** (every long-lived goroutine + its start owner + stop signal — the brief §11 table) and concurrency success criteria/tests for: counter-B atomicity (CR-02/T20), the double-verdict CAS (CR-04/T60), the `OnReturn`-must-not-block contract (CR-01/T144), the supervisor/loop panic-degrade (CR-05/T34c), the non-cooperative-handler goleak **carve-out** (CR-09/T70), pool starvation (CR-08/T08), and the 1000-cycle churn (CR-10/T45).
  - [ ] §6.1 pins that close-idempotency is enforced **atomically** (CR-12 — code already correct: `connection.go:237-242` mutex/bool, `consumer.go` `sync.Once`/`atomic.Bool`); a concurrent double-`Close` `-race` test is added.
- **Verify:** The §7/§9 inventory appendix + the new criteria are present and each maps to a green test; the double-`Close` `-race` test passes; `go test -race ./...` covers them.
- **Files:** SPEC §6.1/§7/§9, `connection_internal_test.go`, `*_test.go` (cross-cutting), `README.md`.
- **Deps:** T143, T20, T144, T34c, T07, T60, T18, T08, T145, T70, T45. **(CR-12/CR-13, P2)** — lands last; asserts the controls A–E added.

### Checkpoint — Phase 19 (Lens 08) closed
- [ ] T143 gate results (CG-1..CG-6) captured on unit + `-race` + the **existing** `integration` lane (3.13 + 4.x where broker-bound) / `amqpmock`; results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced; first task records §10 **Rev 18**.
- [ ] Counter-B lost-update closed (CR-02/T20, Blocker): the load-increment-store is **atomic**; a **behavioural** N-goroutine-same-key test asserts the final count == N and `MaxRedeliveries` is enforced exactly; §6.3/decision 12 say "atomic read-modify-write" + note `-race` proves memory-safety only.
- [ ] Callback liveness contract (CR-01/T144): §6.1/§6.2/§6.3 name every `On*` callback's invocation goroutine + the must-not-block contract; `OnReturn` has a panic-recover; a test asserts a blocking `OnReturn` no longer stalls confirms (D1: dispatched to a bounded worker, timing change documented).
- [ ] Double-verdict atomicity (CR-04/T60): the `Delivery[M]` guard is a single atomic CAS; a race test (timeout vs handler-`Ack`) asserts exactly one frame + the late call a no-op (no `PRECONDITION_FAILED`, no cascade).
- [ ] Barrier & ForceReconnect (CR-03/CR-11/T07): §6.1 describes the real ctx-cancellable mechanism (no contradiction), the per-Wait watcher churn is bounded, `ForceReconnect` is documented idempotent/coalesced; a test asserts a ctx-cancel during the barrier returns `ErrReconnecting` with no leak.
- [ ] Panic-safety (CR-05/T34c): a chaos test asserts a panic in the reconnect `connect` fn or the resubscribe hook degrades the socket (`WithOnTopologyDegraded` + metric), not a process crash or a silent reconnect-disable.
- [ ] Dispatcher & pool (CR-06/T18, CR-08/T08): §6.3 states prefetch is the sole dispatch bound (no second queue) + a test asserts the buffer == prefetch and `basic.cancel` observed with all slots busy; §6.2 documents Acquire is best-effort with a starvation caveat.
- [ ] Boundaries & leaks (CR-07/T145, CR-09/T70): §6.2 documents the per-call (not aggregate) tracker bound (+ LATER-43); a forced close detaches a non-cooperative handler + increments the leaked-handler metric + does not hang the cascade; §7/§9 carve the ctx-ignoring handler out of the goleak guarantee.
- [ ] Stress reconciled (CR-10/T45): §7 and §9 agree on **1000** cycles; a 1000-cycle connect/disconnect + confirm-churn `goleak.VerifyNone` test is green.
- [ ] Capstone (CR-12/CR-13/T146): §6.1 pins atomic close-idempotency (+ double-`Close` `-race` test); §7/§9 carry the goroutine-inventory appendix + the new concurrency criteria, each backed by a test.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 + 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced if the external contract changed (the `OnReturn` callback invocation contract; a `WithMaxInFlightConfirms` option only if D4 ships it).
- [ ] SPEC §10 "Rev 18" note records the Lens-08 pass; nine findings extend their owning task in place (T07/T08/T13/T18/T20/T34c/T45/T60/T70), not re-filed; exactly **one** new `LATER.md` entry (LATER-43); T143–T146 contiguous, no duplicate IDs.

## Phase 20 — Performance & Capacity Re-review (Lens 09: every throughput number reduced to its Little's-Law model + back-solved RTT re-projected at 1/5/10 ms, every per-message hot-path allocation traced, every "single pass"/"efficient" claim read for real complexity)

Closes the Lens-09 adversarial spec validation
(`spec-validation/09-performance-capacity.md`, findings `PC-01..PC-15`; brief
`spec-validation/09-performance-capacity-plan.md`). Lens verdict: **GO-WITH-CHANGES**, **no
Blocker**. The performance architecture is sound where it counts (the `amqp091-go`
per-connection serialization premise is real — a `sendM` mutex serialises all writes + a
single `reader` goroutine demuxes all reads, v1.11.0 — so the multi-TCP role-split fan-out
of §1/§6.1 is the *correct* answer to the single-socket ceiling; Prometheus uses
`WithLabelValues` not per-message `Labels{}` maps; trace injection zero-allocates on the
no-span path; decode runs off the per-channel reader; the NoOp tracer/metrics bodies are
empty). Unusually, **the headline this lens exists to raise was already caught by a prior
lens**: the sync-confirm capacity-honesty finding (the `pool/RTT` ceiling, the local-only
30k/100k numbers, the confirm-latency → `ErrChannelPoolExhausted` cascade) is **already
owned by T83/RMQ-11 + T116/SRE-14**, and the histogram capacity-tail by **T113/SRE-11**, so
Lens-09 confirms their scope (do-not-regress) and contributes the artifacts they lack (the
explicit rate-@1/5/10 ms model table; the pool-sizing-for-rate formula) rather than
re-filing. The net-new value is **performance debt the prior lenses did not touch**: a
cluster of avoidable **per-message hot-path allocations** (a `time.Timer` per `Publish`
confirm-wait PC-06; span name/attrs/`url.Parse` built even under the NoOp tracer on both
publish and consume PC-07; an x-death `map` on every delivery before the absence check
PC-08; an un-pooled UUIDv7 entropy draw + a process-global `timeMu` lock per publish PC-09),
one **algorithmic** finding (the `multiple=true` resolution is O(outstanding)/frame under
the tracker mutex, not the O(resolved) its "single pass" wording implies PC-11), and the
**§9-criteria / benchmark-methodology gaps** (no payload size or queue type on the numbers;
no consume-side throughput target and no latency SLO; the `5×` batch ratio pegged to the
wrong baseline). Owner decisions (2026-05-29, recommended, overridable): **D1** allocation
depth = land the cheap wins now (timer pool T11; span-arg gating T148; `EnableRandPool` T10;
lazy x-death map T17; Prometheus child caching T148) + **defer the deep wins to LATER-45**
(pooled-buffer codec + a `timeMu`-free UUIDv7); **D2** PC-07 vs decision 3 = gate only the
*argument construction* behind a precomputed `tracingActive` flag (single `Start`/`Record`
call site, **no** `if tracer != nil` branch); **D3** PC-11 = contiguous low-water-mark +
ordered index → O(resolved + log n); **D4** benchmark queue type = require **both** a
classic and a quorum number; **D5** §9 consume criteria = add a consume-side throughput
target **and** a publish/handle latency SLO (p99/p999). **No new build-tag lane** — the
allocation gates ride unit / `-benchmem` / `AllocsPerRun`; the RTT/throughput/queue-type
gates ride the existing `integration`/bench lanes + the T44b release-tag cadence; the
injected-RTT gate coordinates with T116's SG-4. **Nine** findings **extend an existing task
in place** (cross-lens; `Lens-09 (PC-xx)` bullet, not re-filed): T83 (PC-01), T116 (PC-02),
T44b (PC-03/04/05/14), T11 (PC-06 + PC-11), T17 (PC-08), T10 (PC-09), T18 (PC-10), T22
(PC-13); **T113 confirmed** (PC-12, do-not-regress). Exactly **one** new `LATER.md` entry
(LATER-45, gated on D1; the Phase-18 conditional LATER-44 reservation stands). **Gate task
T147 runs first**; no SPEC edit/fix to an affected finding lands before its gate returns.
Per-task SPEC amendment lands in the same PR; a SPEC §10 "Rev 19" note records the pass.

### [ ] T147 — Verification gates PG-1–PG-6 (unit + `-benchmem`/`AllocsPerRun` + existing `integration`/bench lanes; no new build-tag lane) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (unit + `-benchmem`/`testing.AllocsPerRun` + the **existing** `integration`/bench lanes — **no new build-tag lane**) for: **PG-1** the **publish** hot-path allocs/op for `Publish` at the NoOp tracer + default config, attributing each alloc (the confirm-`Wait` `time.Timer`, the span name concat + attrs slice + `peerAddress`/`url.Parse`, the `waiter`+`done` chan, the UUIDv7 string, the JSON body, the release closure) → gates T11/T148/T10; **PG-2** the **consume** hot-path allocs/op per delivery at the NoOp tracer + **no x-death header** (confirm the `map` alloc lands on the no-DLX delivery, plus the span-arg slice + the `context.WithCancelCause`) → gates T17/T148; **PG-3** a microbench of `resolveUpTo` cost vs in-flight depth D (16/256/1024) for a `multiple=true` frame resolving **one** tag, showing per-frame cost grows **O(D)** (whole-map scan + sort) while holding `t.mu` → gates T11; **PG-4** the **single-socket** publish-confirm ceiling (1 conn, sweep `WithChannelPoolSize`) to **source** the "~50k msg/s per socket" figure + locate the `sendM`-writer knee → gates T44b/T07d; **PG-5** an injected-RTT publish-confirm bench at RTT ∈ {~0,1,5,10} ms (default pool, then `4×16`) → the §11 model-table numbers + the `ErrChannelPoolExhausted` onset under a confirm-latency spike (**extends T116's SG-4**) → gates T83/T116; **PG-6** a full-conditions bench recording RTT + **payload size** + broker version + **queue type**, demonstrating the **classic-vs-quorum** confirm-latency delta on the same target → gates T44b/T149.
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate; first task records §10 **Rev 19**.
- **Verify:** Unit + `-benchmem`/`AllocsPerRun` + integration/bench lanes green with the PG measurements captured; the gate table is reviewable.
- **Files:** `publisher_internal_test.go`, `consumer_internal_test.go`, `internal/confirms/*_test.go`, `internal/headers/*_test.go`, `message_internal_test.go`, `metrics/prometheus_test.go`, `*_integration_test.go`, `spec-validation/` (results table).
- **Deps:** T10, T11, T17, T44b, T83, T116, T07d. **(PC gates, P0)**

### [ ] T148 — Hot-path allocation hardening: NoOp-tracer arg gating + Prometheus child caching + combined `AllocsPerRun` guard (PC-07 + PC-15) [P1] · M
- **Acceptance:**
  - [ ] **PC-07:** the span argument-construction (the `exchange+" publish"` / consume name concat, the `[]attribute.KeyValue` attrs slice, and `peerAddress()`'s `url.Parse`) is gated behind a precomputed `tracingActive` flag on both publish (`publisher.go:371/423`, `connection.go:842`) and consume (`consumer.go:571/716`) paths, **preserving decision 3** — one `Start`/`Record` call site, **no** `if tracer != nil` behavioral branch (per **D2**: set once via `_, isNoOp := tracer.(NoOpTracer)`); the reconciliation is documented in §6.9/decision 3; the no-span zero-alloc trace-injection fast path (`publisher.go:444`) is **do-not-regress**.
  - [ ] **PC-15:** the Prometheus child `Observer`/`Counter` for the fixed-outcome label sets is resolved **once at build**, not per-message (`prometheus.go:125/130/235` currently do a `WithLabelValues` hash + `RWMutex` lookup per message); a `prometheus.Labels{}` map is **not** reintroduced (do-not-regress).
  - [ ] T148 owns the **combined `AllocsPerRun` hot-path guard** asserting PC-06/07/08/09/15 collectively at the NoOp tracer + default config (a future regression on any of them fails one test).
- **Verify:** `AllocsPerRun` guard green at NoOp + default config (publish + consume); `go test -race ./...` + `-benchmem` show the gated allocs gone; the no-span fast path and the `WithLabelValues`-not-`Labels{}` invariant unchanged.
- **Files:** `publisher.go`, `consumer.go`, `connection.go`, `otel/`, `metrics/prometheus.go`, SPEC §6.9/decision 3, `*_internal_test.go`.
- **Deps:** T147 (PG-1/PG-2), T11, T17, T10. **(PC-07/PC-15, P1)**

### [ ] T149 — Capacity & performance capstone: §9 model appendix + consume target + latency SLO + batch-as-scale-path + §6.1 write-mechanism wording (PC-05 + PC-10 + PC-14 §9 portions) [P2] · S
- **Acceptance:**
  - [ ] §9 gains a **performance-model appendix** referencing the explicit rate-@1/5/10 ms RTT-collapse table that T83 inlines beside the numbers; §9 gains the **missing consume-side throughput target** (per **D5**: e.g. `BenchmarkConsume ≥ 30k msg/s` at `Concurrency(8)+Prefetch(256)` — already benched by T44b but never encoded in §9) **and** a **publish/handle latency SLO** (p99/p999 against the §6.9 histogram).
  - [ ] The batch-target wording is reframed as the RTT-decoupled **scale path** (PC-05, paired with T44b's absolute reframe); §6.1's "one goroutine drives the socket" write-mechanism wording is corrected → writes are **`sendM`-mutex-serialised**, reads are a **single `reader` goroutine** (the "serializes I/O per connection" *conclusion* is correct; the write *mechanism* was imprecise; PC-14).
  - [ ] Asserts the WS-2/WS-3/WS-4 controls (T11/T17/T10/T148/T44b) landed.
- **Verify:** The §9 appendix + consume target + latency SLO + the §6.1 wording fix are present and each maps to a backing bench/test; the controls B–D are referenced; `make lint` clean.
- **Files:** SPEC §9/§6.1/§6.3, `*_test.go` (cross-cutting), `README.md`.
- **Deps:** T147, T11, T17, T10, T148, T44b, T18, T83. **(PC-05/PC-10/PC-14, P2)** — lands last; asserts the controls B–D added.

### Checkpoint — Phase 20 (Lens 09) closed
- [ ] T147 gate results (PG-1..PG-6) captured on unit + `-benchmem`/`AllocsPerRun` + the **existing** `integration`/bench lanes; results table committed; downstream tasks cite their gate; **no new build-tag lane** introduced; first task records §10 **Rev 19**.
- [ ] Capacity model inline (PC-01/T83, PC-02/T116): §9 carries the explicit rate-@1/5/10 ms RTT-collapse table beside the 30k/100k numbers; §6.2/§9 carry the pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT`) + the `ErrChannelPoolExhausted` onset; the async-publish API stays LATER-34, decision 31 stays closed; the headline is **not** re-filed.
- [ ] Publish hot path (PC-06/T11, PC-09/T10, PC-07/T148): an `AllocsPerRun` test asserts `Publish` at the NoOp tracer + default config no longer allocates the confirm-`Wait` timer, the span name/attrs/`url.Parse`, or (via `EnableRandPool`) the per-call entropy buffer; the `timeMu` global-lock cost is documented; the no-span fast path is unchanged.
- [ ] Consume hot path (PC-08/T17, PC-07/T148): an `AllocsPerRun` test asserts a no-x-death, NoOp-tracer delivery allocates **no** x-death `byReason` map and **no** span name/attrs slice; decode still runs off the per-channel reader goroutine.
- [ ] Confirm complexity (PC-11/T11): a microbench shows `multiple=true` resolution is O(resolved + log n), not O(outstanding); §6.2 states the real complexity; the one-shot resolve / `Wait`-self-delete / `CloseAll` mechanism is unchanged.
- [ ] Benchmark methodology (PC-03/04/05/14/T44b): the bench reports + pins RTT + payload size + broker version + **queue type**, with both a classic and a quorum number; the `PublishBatch` target is an RTT-stated absolute with batch documented as the scale path; the release-tag-only regression cadence is documented; the ~50k/socket figure is replaced with a measured single-socket ceiling + the `sendM` knee.
- [ ] Pin & capstone (PC-10/T18+T149, PC-13/T22, PC-12/T113, §9/T149): §6.3 states consume scaling needs more channels/connections beyond the per-TCP ceiling; §9 gains a consume-side throughput target **and** a p99/p999 latency SLO; §6.2 documents the `PublishBatchMaxSize` trade-off; §6.9's extended buckets (T113) span the confirm-RTT tail; §6.1's write mechanism is described accurately.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + the `-benchmem`/`AllocsPerRun` guards green; integration on 3.13 **and** 4.x + the T44b bench cadence green.
- [ ] README synced if the external contract changed (none expected — internal hot-path + spec wording; no new public option in this phase).
- [ ] SPEC §10 "Rev 19" note records the Lens-09 pass; nine findings extend their owning task in place (T10/T11/T17/T18/T22/T44b/T83/T116) + T113 confirmed, not re-filed; exactly **one** new `LATER.md` entry (LATER-45; the Phase-18 conditional LATER-44 reservation stands); T147–T149 contiguous, no duplicate IDs.

## Phase 21 — Test-Strategy & Verifiability Re-review (Lens 10: every §9 success criterion classified falsifiable? / tested-at-what-level? / right-reason? / CI-gateable?, every load-bearing §6 contract matched to the weakest test level that can prove it, every "nightly"/"enforced in CI"/"suite passes" claim checked against the actual CI)

Closes the Lens-10 adversarial spec validation
(`spec-validation/10-test-strategy-verifiability.md`, findings `TV-01..TV-15`; brief
`spec-validation/10-test-strategy-verifiability-plan.md`). Lens verdict: **GO-WITH-CHANGES**,
**no Blocker**. The library's *behaviour* is well tested (631 unit tests, 6 fuzz targets, 212
`goleak.VerifyNone` assertions, `-race` everywhere) and the highest-risk contracts already
have **owned** tasks with **named tests** — so, like Lens-09, **this lens is heavily
cross-lens and its headline is already owned**: every "untested contract" the prompt
anticipated is already someone's task (R10-3 ordering → T59 + Lens-01 RMQ-16; polyglot
CloudEvents / field-table / `time.Time`→`T` fidelity → T94 + T37; the Rev-10 failure modes
R10-6/8/12 → T61/T63/T67, pulled into v0.1 with named tests; the version-divergent quorum
limit → T81; the security credential grep → T45b; the concurrency-interleavings coverage%
gap → T143/T146). The gap this lens exposes is **not in the code** — it is in the
**verification infrastructure and the honesty of the criteria**: a CI that runs only `unit` +
`integration` (single `rabbitmq:3-management`, Go 1.23, `-cover` with no fail-under, **no
`schedule:` trigger**) leaves ~¼ of §9 **structurally unrun** (every "nightly" criterion —
fuzz 10m-budget, 1000-cycle stress, chaos 5-min @ 10k × 50 runs — has no runner; the
conformance lane + throughput bench don't exist in CI; the 4.x + Go-1.24 rows never run) and
lets three reliability-relevant criteria **pass while broken** (chaos "zero message loss"
with no loss-counting method; the unenforced coverage floor; the security grep that only
catches *exercised* output). The net-new value is that **infrastructure + criteria-honesty
layer no prior lens owns**: a scheduled/nightly workflow (TV-01), a RabbitMQ 3.13 + 4.x
matrix (TV-05), an integration-broker-required guard (TV-13), the conformance
stub-vs-real-broker §7 matrix with the stub language dropped for v0.1 (TV-06), and a
§7/§9/§6.2.1 verifiability rewrite that classifies every criterion by *where it runs*
(TV-10/12/15). Owner decisions (2026-05-29, recommended, overridable): **D1** throughput
(TV-04) = keep `30k/100k/5×` as release-tag targets on reference HW + add a CI-gradeable
relative regression gate; **D2** nightly (TV-01/13) = add a `schedule:` workflow (fuzz budget
+ 1000-cycle stress + chaos 50-run flaky < 1%) + the broker-required guard — the only honest
way to claim "nightly"; **D3** version matrix (TV-05/11) = run **both** 3.13 LTS + 4.x on the
integration lane; **D4** coverage (TV-03) = hard fail-under at 80%/95% + artifact + PR comment
(§7 already promises it); **D5** conformance (TV-06) = real-broker-only for v0.1 + drop the
"test AMQP server stub" language. **New lanes are in scope** (unlike Lenses 04–09): the
`conformance` lane is already in §7/§3 but not CI (remediation, not invention); the
scheduled/nightly is a new *trigger*, the version matrix a new *axis*. **Nine** findings
**extend an existing task in place** (cross-lens; `Lens-10 (TV-xx)` bullet, not re-filed): T41
(TV-03), T42 (TV-03/07/13), T44 (TV-06/08/11), T44b (TV-04), T45 (TV-09/10), T45b (TV-14),
T59 (TV-02), T81 (TV-05/11), T37 (TV-08); **three groups confirmed** (do-not-regress, no
task): T94 + T37 (polyglot interop), T61/T63/T67 (Rev-10 modes), T143/T146 (concurrency
criteria). Exactly **one** new `LATER.md` entry (LATER-46). **Gate task T150 runs first**; no
§7/§9/§6.2.1 wording change and no workflow edit lands before its gate returns. Per-task SPEC
amendment lands in the same PR; a SPEC §10 "Rev 20" note records the pass.

### [ ] T150 — Verification gates VG-1–VG-6 (unit + existing `integration` lane + a throwaway 4.x broker for VG-5; no behaviour change) [P0] · S
- **Acceptance:**
  - [ ] Ground truth captured (gate-first) for: **VG-1** the "green by not running" baseline — every §9 checkbox marked *executes-in-current-CI* (`unit`+`integration`, Go 1.23, `rabbitmq:3-management`) vs **structurally unrun** (the nightly criteria, the conformance suite, the throughput bench, the 4.x-only contracts, the Go-1.24 row, the delayed-exchange/TLS/SASL-EXTERNAL criteria), filling the brief §11 "CI-gateable?" column with *measured* facts → gates TV-01/04/05/06/07/08; **VG-2** the unenforced coverage floor — `go test -race -cover ./...` produces a number but **no step fails** below 80%/95% (no `-coverprofile` threshold, no artifact, no PR comment); compute current per-package coverage + confirm a hypothetical sub-floor drop still passes → gates TV-03; **VG-3** the green-by-skipping integration lane — `go test -race -tags=integration ./...` with `AMQP_TEST_URL` **unset** exits **0** having asserted nothing (count the `t.Skip`ped tests) → gates TV-13; **VG-4** R10-3 mock-vs-real — whether the mock-tracker test can *ever* fail when the demux is intentionally broken (split goroutines / buffered notify chan), + how many real-broker iterations under concurrent unroutable-mandatory load trip the race ≥1× → gates TV-02 (T59); **VG-5** version-divergent contracts on 4.x vs 3.13 — stand up both `rabbitmq:3.13` + `rabbitmq:4`; assert the quorum default `x-delivery-limit` (default 20) drops the 21st delivery on 4.x + the classic-queue `x-delivery-limit`-ignore behaviour; record divergence → gates TV-05 (T81); **VG-6** chaos loss-counting — verify "zero loss" = published-set − consumed-set deduped by `MessageID` (tolerating at-least-once duplicates) by **injecting one deliberate drop** and confirming the harness reports loss == 1 → gates TV-09 (T45).
  - [ ] Results table committed (under `spec-validation/`); each downstream task cites its gate; first task records §10 **Rev 20**; **no behaviour change** in this task.
- **Verify:** unit + the existing `integration` lane (+ a throwaway 4.x broker for VG-5) green with the VG measurements captured; the brief §11 "CI-gateable?" column is filled from measured reality; the gate table is reviewable.
- **Files:** `*_internal_test.go`, `internal/confirms/*_test.go`, `*_integration_test.go`, `.github/workflows/ci.yml` (read-only inspection), `docker-compose.integration.yml` (read-only), `spec-validation/` (results table).
- **Deps:** T45, T59, T81 (gate targets). **(TV gates, P0)**

### [ ] T151 — CI verification infrastructure: scheduled/nightly workflow + RabbitMQ 3.13/4.x matrix + integration-broker-required guard (TV-01 + TV-05 + TV-13) [P1] · M
- **Acceptance:**
  - [ ] **TV-01:** a `schedule:`-triggered workflow (nightly) runs the **fuzz 10m-budget per target**, the **1000-cycle connect/disconnect stress** (T45/CR-10), and the **chaos 5-min @ 10k × 50-run** flaky harness (T45), reporting the **flaky-rate** (< 1%) — `ci.yml` today triggers only on `push`/`pull_request`, so every "nightly" criterion has **no runner**; §9 names which criteria run on this cadence (coord T152).
  - [ ] **TV-05:** a **RabbitMQ 3.13 + 4.x matrix** axis on the `integration` lane (+ the pinned images in `docker-compose.integration.yml`), so version-divergent contracts (R10-2 quorum default `x-delivery-limit`, classic `x-delivery-limit`-ignore, Khepri `queue.declare`) are verified on the version where they bind (today a single `rabbitmq:3-management`); the matrix *verifies* T81's version-divergence docs.
  - [ ] **TV-13:** an **integration-broker-required guard** that **fails** the required integration job if **zero** integration tests actually ran. (Note: as of the Phase 3 validation work the integration helpers `t.Fatal` on an unset `AMQP_TEST_URL` / `AMQP_TEST_MANAGEMENT_URL`, so the unset-URL case already fails at the test layer; this guard now covers the residual "zero tests ran for another reason" — e.g. a `-run` filter matching nothing.)
  - [ ] **Lens-13 (LT-02/03/04/05):** campaign cadence: the soak/endurance (T167) + spike/stress-to-failure/single-node-composite (T168) campaigns ride this scheduled/nightly workflow; the **multi-node cluster load harness (T166) is a distinct on-demand/nightly lane** this task's infrastructure wires; state honestly which campaigns gate **release tags** vs nightly (the heavy campaigns gate release tags, not per-PR — D5).
- **Verify:** the nightly workflow runs on its `schedule:` and reports the flaky-rate; the integration lane runs both 3.13 and 4.x green; the residual zero-tests-ran case (e.g. a `-run` filter matching nothing) **fails** the guard — an unset `AMQP_TEST_URL`/`AMQP_TEST_MANAGEMENT_URL` already fails at the test layer; the existing `unit`/`integration` lanes stay green.
- **Files:** `.github/workflows/*.yml`, `docker-compose.integration.yml`, `Makefile`, `README.md` (CI lane / `make` target description).
- **Deps:** T150 (VG-1/VG-3/VG-5), T45, T45b, T41, T42, T37, T44, T81. **(TV-01/TV-05/TV-13, P1)** **(D2, D3)**

### [ ] T152 — Verifiability honesty capstone: §9 per-criterion classification + §7 stub-vs-real matrix + §7/§9/§6.2.1 reconciliation (TV-06 + TV-10 + TV-12 + TV-15) [P2] · S
- **Acceptance:**
  - [ ] **Every** §9 success criterion is classified `CI-gate | nightly | release-only | operator-validated | polyglot-lane` so the spec never implies a check that does not exist; the §9 cross-language criteria are scoped to gate on the T94 polyglot lane.
  - [ ] **TV-06:** the §7 **conformance stub-vs-real-broker contract matrix** (brief §12) is added and the **"test AMQP server stub" language is dropped** for v0.1 (real-broker-only; a stub cannot prove the §12 broker-bound contracts).
  - [ ] **TV-10:** the residual §7 prose the per-task fixes do not catch is reconciled — §3/§7's "spins up RabbitMQ via testcontainers-go" / `amqptest.NewRabbitMQ(t)` claims vs the real docker-compose + `AMQP_TEST_URL` harness until T37 lands.
  - [ ] **TV-12:** §7 Coverage states the line-coverage floor is **necessary-not-sufficient** for `internal/confirms`/`internal/reconnect` (the bugs are in interleavings, not lines) and **cross-links the §9 concurrency criteria** (T143/T146, confirmed do-not-regress).
  - [ ] **TV-15:** §6.2.1's "15 minutes of MessageID retention is sufficient" is labelled an **operator-validated recommendation, not a library-tested guarantee** (no test can exercise an hours-later redelivery).
  - [ ] Asserts the WS-1/WS-2 lanes (T150/T151 + T41/T42/T44) landed.
- **Verify:** every §9 criterion carries a classification tag; the §7 stub-vs-real matrix is present and the stub language is gone; §7 prose matches the real harness; §6.2.1 is labelled operator-validated; `make lint` clean.
- **Files:** SPEC §9/§7/§6.2.1, `README.md` (if the testing-strategy description changes).
- **Deps:** T150, T151, T41, T42, T44, T44b, T45, T143, T146, T94. **(TV-06/TV-10/TV-12/TV-15, P2)** — lands last; asserts the controls B–D added.

### Checkpoint — Phase 21 (Lens 10) closed
- [ ] T150 gate results (VG-1..VG-6) captured on unit + the existing `integration` lane (+ a throwaway 4.x broker for VG-5); the brief §11 "CI-gateable?" column filled from measured reality; results table committed; downstream tasks cite their gate; first task records §10 **Rev 20**; **no behaviour change** in T150.
- [ ] Nightly runner (TV-01/T151): a `schedule:` workflow runs the fuzz 10m-budget + 1000-cycle stress + chaos 5-min @ 10k × 50-run + flaky-rate report; §9 names which criteria run there.
- [ ] Version matrix (TV-05/11/T151+T81+T44): the `integration` lane runs **both** `rabbitmq:3.13` + `rabbitmq:4`; the quorum default `x-delivery-limit` drop + the poison-loop **bound** (`DeliveryLimit + 1`, not `==`) are asserted on 4.x; T81's docs are verified by it.
- [ ] Coverage / Go-matrix / skip-guard enforced (TV-03/07/13/T41+T42): CI **fails** below 80% per package / 95% on critical-path packages, uploads the coverage artifact + posts the PR comment; `-race` runs on Go **1.23 and 1.24**; the required integration job fails if zero integration tests ran.
- [ ] Conformance honesty (TV-06/08/T44+T37): the §7 stub-vs-real matrix exists; the "test AMQP server stub" language is dropped; delayed-exchange criteria run against a plugin-enabled required lane or are conditionally-verified and **fail, not skip**.
- [ ] Make-checkable numbers (TV-04/09/14): §9 has a CI-gradeable relative regression gate with the absolutes reclassified as release-tag targets (T44b, coord Lens-09 T149); §7 + T45 define chaos loss as published − consumed (deduped by `MessageID`, tolerating duplicates) + the VG-6 injected-drop self-test reports loss == 1; the 60s security run forces a wrapped-`amqp091-go` error path + scans logs/errors/spans/labels (T45b).
- [ ] R10-3 honesty (TV-02/T59): the real-broker test runs many iterations under concurrent unroutable-mandatory load; §7 notes the contract is flaky-prone by design + nightly-only; the mock-tracker lock stays on the fast lane.
- [ ] §7/§9/§6.2.1 reconciled (TV-10/12/15/T45+T152): §7 prose matches §9 (5-min @ 10k, 1000 cycles, the docker-compose/`AMQP_TEST_URL`/`amqptest` reality); §7 states the coverage floor is necessary-not-sufficient + cross-links T143/T146; §6.2.1 labels the dedupe window operator-validated; **every** §9 criterion is classified.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new nightly / 3.13+4.x-matrix / conformance lanes green on their cadence.
- [ ] README synced if the external contract changed (the `make` target list / CI lane description if a `make ci-nightly`/matrix target is added; the §9 numbers + classification are SPEC-side — checked per task).
- [ ] SPEC §10 "Rev 20" note records the Lens-10 pass; nine findings extend their owning task in place (T37/T41/T42/T44/T44b/T45/T45b/T59/T81) + three groups confirmed (T94, T61/T63/T67, T143/T146), not re-filed; exactly **one** new `LATER.md` entry (LATER-46); T150–T152 contiguous, no duplicate IDs.

## Phase 22 — Data-Protection Compliance Re-review (Lens 11: every `Message[M]` field traced to where it persists, every default rated for privacy-by-default, every finding mapped to a GDPR *and* LGPD article, "can the library delete one message / surface residency?" answered = no)

Closes the Lens-11 adversarial spec validation (`spec-validation/11-compliance-gdpr-lgpd.md`; brief `spec-validation/11-compliance-gdpr-lgpd-plan.md`; findings `DP-01..DP-15`). Verdict **GO-WITH-CHANGES** — **no Blocker.** The library is a processor that already does the hard confidentiality work (URI redaction T07c/T133, OTel size-not-content, bounded PII-free labels, strict-codec opt-in) and the security-of-processing controls are **already owned** by the security lens (never-log-payloads T134, PLAIN warning T132, at-rest boundary T141) — so **this lens is heavily cross-lens and its confidentiality half is already owned**; Lens-11 **extends** those tasks in place (cross-lens rule: a shared finding adds a `Lens-11 (DP-xx)` acceptance bullet, never re-filed) and the net-new value is the **privacy-by-design layer no prior lens owns** (erasure / storage limitation, data minimisation, international transfer, records of processing). Owner decisions (recommended, overridable): **D1** pointer-out → first-class §8 pattern + runnable `examples/pointer_out/` + §9 criterion; **D2** the auto-`<source>.dlq` gains a default/strongly-recommended TTL (inside T65); **D3** crypto-erasure / message-level payload encryption → **LATER-47**; **D4** residency = documented caveat only for v0.1; **D5** the capstone draws the controller-vs-processor boundary. **No new build-tag lane** (the example rides `examples-build`/`examples-smoke`; the DLQ-TTL default rides `integration`). Five findings extend in place (T65/T85/T132/T134/T141) + four confirmed (T07c, T133, bounded labels, OTel size-not-content + header-non-mutation); four net-new (T153–T156); one new LATER (LATER-47). **Gate task T153 runs first**; per-task SPEC amendment lands same-PR ("amend SPEC first"); §10 **Rev 21** recorded when T153 lands.

### [ ] T153 — Data-protection gates DG-1..DG-5 (data-inventory ledger + defaults audit + no-PII observability baseline + DLQ-retention reality + erasure/residency reality) [P0] · S
- **Acceptance:**
  - [ ] **DG-1** data-inventory audit: every field/metadata the library writes or propagates (`Message[M]` fields, `x-death`, `traceparent`/`tracestate`, `connection_name`, OTel attrs, metric labels) is classified `NP`/`PP`/`ID→PII` with **where each persists** (wire, broker disk, DLQ copy, dedupe cache, logs, traces/APM, metric store, `client_properties`); brief §11 ledger filled from the *implemented* code.
  - [ ] **DG-2** defaults audit (Art. 25 / LGPD Art. 6): each default rated for privacy-by-default impact (`DeliveryMode=Persistent`, auto-DLQ no TTL/bounds, absent never-log-PII boundary, absent at-rest note, `MessageID=UUIDv7`, bounded labels, OTel size-not-content, `connection_name`, PLAIN-over-`amqp://`, codec lax-by-default); brief §12 audit filled.
  - [ ] **DG-3** observability no-PII baseline: confirmed in `otel`+`metrics` code that by default only `messaging.message.body.size`, ID attrs, and infra addresses are emitted — no body/header *content* ever becomes a span attr or default label (do-not-regress invariant).
  - [ ] **DG-4** DLQ-retention reality: confirmed in `topology.go` the auto-`<source>.dlq` is declared with **no `x-message-ttl` and no `x-max-length`** by default and that `DeadLetter.TTL`/`MaxLength`/`MaxLengthBytes` bind the **source** queue (§6.6 L1642), not the DLQ.
  - [ ] **DG-5** erasure & residency reality: confirmed there is **no API path** to delete a single in-flight message (only queue-purge/delete, all-or-nothing) and that quorum replication + `WithAddrs` failover + the delayed-plugin node-local store (R10-1) are residency-opaque (no region/jurisdiction in the API).
  - [ ] **No behaviour change**; records §10 **Rev 21**.
- **Verify:** Results table under `spec-validation/`; the brief §11 ledger + §12 audit are filled from measured code; each downstream task cites its gate.
- **Files:** `spec-validation/` (results), read-only audit of `message.go`, `topology.go`, `otel/`, `metrics/`, `internal/headers`, `internal/redact`.
- **Deps:** none (gate-first). **(DP gates, P0)**

### [ ] T154 — Erasure & storage-limitation: pointer-out as the canonical privacy control + runnable example (the headline) [P1] · M
- **Acceptance:**
  - [ ] (DP-01, GDPR Art. 17 / LGPD Art. 18.VI+16) §8 + §6.5 state that personal data in a body on a durable queue/DLQ is effectively **un-erasable** at the bus layer (no single-message delete) and route the reader to pointer-out.
  - [ ] (DP-02, GDPR Art. 25/17/33-34 / LGPD Art. 6+46+48) pointer-out is **promoted** from a size-only tip (§6.1 L724-726, §10 L2952) to a first-class **privacy / erasure / breach-minimisation** pattern, with a runnable `examples/pointer_out/` (PII in an erasable store of record, opaque reference on the wire) that round-trips.
  - [ ] (DP-03 positioning, GDPR 5(1)(e) / LGPD Art. 16) `Expiration`/`x-message-ttl`/`DeadLetter.TTL` (§6.5/§6.6) are positioned as the **retention control for personal data**.
  - [ ] (DP-13, GDPR Art. 25) §8 *Always*/recommended boundary "personal data should not flow as message bodies — use a reference".
- **Verify:** `make examples-build` builds `examples/pointer_out/`; it smoke-runs (round-trips a reference) on the `integration` lane; the §8/§6.5 wording is present.
- **Files:** SPEC §6.1/§6.5/§6.6/§8, `examples/pointer_out/`, `README.md` (examples table).
- **Deps:** T153 (DG-5); coordinates T65 + T85. **(DP-01/02/03/13, P1)** **(D1, D2)**

### [ ] T155 — Data-minimisation footguns: IDs, principal, hostname, labels, trace-as-processor [P1] · M
- **Acceptance:**
  - [ ] (DP-05, GDPR 5(1)(c)/25 / LGPD Art. 6.III) §6.5 warns **never to put personal data in `MessageID`/`CorrelationID`** (they flow to the dedupe cache, `x-death`, logs, the OTel `messaging.message.id`/`conversation_id` attrs → APM).
  - [ ] (DP-06, GDPR 5(1)(c)/25 / LGPD Art. 6.III) the `UserID`/`StampUserID` privacy implication is documented (embeds an identifiable principal into every message + DLQ copy + broker logs; leave it empty unless the broker-side identity stamp is required).
  - [ ] (DP-11, GDPR 5(1)(c) / LGPD Art. 6.III) a minimisation note at `WithConnectionName`/`WithClientProperties` (`<binary>-<hostname>-<pid>` leaks hostname broker-side + into the `ClientMetrics{connection_name}` label).
  - [ ] (DP-12, GDPR 5(1)(c) / LGPD Art. 6.III) a one-line note at `WithMetricsLabels(routing_key/message_type)` that opt-in labels can export identifiers.
  - [ ] (DP-10, GDPR Art. 28+Ch. V / LGPD Art. 39+33) the **trace-backend-as-a-processor** implication noted in §6.9 (the APM needs its own lawful basis + retention + transfer mechanism for whatever the user puts in IDs).
- **Verify:** The §6.5/§6.9/§8 minimisation notes are present at each named option; coordinates with T85 (dedupe-cache-as-PII-store aspect of DP-05).
- **Files:** SPEC §6.5/§6.9/§8, `README.md` (if a privacy note is added).
- **Deps:** T153 (DG-1/DG-3); coordinates T85. **(DP-05/06/10/11/12, P1)**

### [ ] T156 — International-transfer / residency caveat + §8/§9 compliance capstone + LGPD-specific notes (lands last) [P2] · S
- **Acceptance:**
  - [ ] (DP-09, GDPR Ch. V Art. 44-49 / LGPD Art. 33) §6.1/§6.5/§10 carry a data-residency caveat — quorum replicates the body to every member node; `WithAddrs` failover + the delayed-plugin node-local store (R10-1) can cross jurisdictions; residency is invisible at the API; the international-transfer trigger is named; mitigations (single-jurisdiction membership or pointer-out) recommended.
  - [ ] (DP-13 §9) a §9 success criterion that `examples/pointer_out/` exists and round-trips.
  - [ ] (DP-15, GDPR Art. 30 / LGPD Art. 37) a records-of-processing / APM-trail note (the trace-continuity-through-DLX contract creates an APM personal-data trail needing its own retention; cross-link DP-10 + T141).
  - [ ] (DP-14, GDPR 33/34+6 / LGPD Art. 48+7) an LGPD-specific pointer note (ANPD breach timing Art. 48 vs GDPR's 72h; lawful-basis set Art. 7 vs GDPR Art. 6 — both the controller's).
  - [ ] (D5) the explicit **controller-vs-processor boundary** is drawn so warren never implies it discharges the controller's duties; every finding maps to a GDPR *and* LGPD article.
- **Verify:** The §6.1/§6.5/§9/§10 caveats + boundary are present; the §9 pointer-out criterion is checkable; asserts the WS-1..WS-3 controls (T154/T155 + T65/T85/T132/T134/T141 extensions) landed.
- **Files:** SPEC §6.1/§6.5/§9/§10, `README.md` (if a privacy/guarantees note is added).
- **Deps:** T153 (DG-5), T154. **(DP-09/13/14/15, P2)** **(D4, D5)** — lands last.

### Checkpoint — Phase 22 (Lens 11) closed
- [ ] T153 gate results (DG-1..DG-5) captured into a results table; brief §11 ledger + §12 audit filled from measured code; first task records §10 **Rev 21**; no behaviour change.
- [ ] Erasure honesty (DP-01): §8 + §6.5 state bodies on a durable bus/DLQ are un-erasable; route to pointer-out.
- [ ] Pointer-out as a privacy control (DP-02/DP-13): positioned in §6.5/§8 as the erasure + breach-minimisation pattern; `examples/pointer_out/` builds (`examples-build`) + round-trips (`examples-smoke`); a §9 criterion enforces it (T156).
- [ ] Storage limitation (DP-03): §6.5/§6.6 position TTL as the personal-data retention control; the auto-DLQ gains a default/strongly-recommended TTL + length bound, asserted on `integration` (T65).
- [ ] Never-log-PII (DP-04): §8 *Never* carries "never log bodies/header values" as a data-protection control; grep/AST test green (T134).
- [ ] Minimisation footguns (DP-05/06/11/12): §6.5 warns on `MessageID`/`CorrelationID`; `UserID`/`StampUserID`, `WithConnectionName`/`WithClientProperties`, `WithMetricsLabels` carry notes; dedupe-cache-as-PII-store framed in T85.
- [ ] Confidentiality (DP-07/08): at-rest boundary as the Art. 32 operator responsibility (T141); PLAIN-over-`amqp://` warning names personal-data-in-transit (T132).
- [ ] Transfer/residency (DP-09): §6.1/§6.5/§10 carry the data-residency caveat + mitigations (T156).
- [ ] Trace-as-processor & records (DP-10/DP-15): §6.9 notes the APM is a processor + the dual nature of the trace-continuity contract for Art. 30 / LGPD Art. 37 records.
- [ ] LGPD-specific + scope (DP-14/D5): the capstone states breach timing + lawful basis are the controller's and draws the controller-vs-processor boundary.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; `make examples-build` green (incl. `examples/pointer_out/`); the example smoke-runs on `integration`.
- [ ] README synced if the external contract changed (the examples table gains `examples/pointer_out/`; checked per task; no new public option this phase).
- [ ] SPEC §10 "Rev 21" note records the Lens-11 pass; five findings extend their owning task in place (T65/T85/T132/T134/T141) + four groups confirmed (T07c, T133, bounded labels, OTel size-not-content + header-non-mutation), not re-filed; exactly **one** new `LATER.md` entry (LATER-47); T153–T156 contiguous, no duplicate IDs; tally **165 tasks / 22 phases**.

## Phase 23 — Developer-Experience & Documentation Re-review (Lens 12: can a competent Go developer go from `go get` to a working, *correct* publish/consume in the first hour without reading the source? — every load-bearing warning checked for call-site-godoc presence; the conceptual overview, the most-copied snippets, and the runnable-example gaps audited against the first hour)

Closes the Lens-12 adversarial spec validation (`spec-validation/12-dx-documentation.md`; brief `spec-validation/12-dx-documentation-plan.md`; findings `DX-01..DX-17`). Verdict **GO-WITH-CHANGES** — **no Blocker.** The API is well-designed and the hardest prose is exemplary (the §6.3 poison/`AutoAck` treatment, the §6.5 `UserID`/`Delay` field notes, the six shipped examples' "What this example demonstrates" headers), and a determined reader can learn the library from `SPEC.md` — so it is not NO-GO. But **for a reliability library the documentation *is* the product**: a load-bearing warning that lives only in `SPEC.md` is, for DX purposes, invisible. **This lens is heavily cross-lens and most of its placement surface is already owned** (godoc sweeps T10/T34/T40, the `AutoAck` exemplar T35, the `Delay` godoc T57, the dedupe example T38b, the `DeliveryLimit==0` warning T58, the README quickstart T39) — Lens-12 **confirms** the exemplars and **extends** the owning tasks in place (cross-lens rule: a shared finding adds a `Lens-12 (DX-xx)` acceptance bullet, never re-filed); the net-new value is the **pit-of-success / first-hour-learning-path layer no prior lens owns**. Owner decisions (recommended, overridable): **D1** `doc.go` → canonical conceptual overview + SPEC-defined mandatory contents; **D2** top-level `MIGRATION.md`; **D3** a production-tuning page in `doc.go`/`docs/`; **D4** error-remedy in godoc-on-sentinel (no new sentinels); **D5** P1 examples = `durable_retry` + `graceful_shutdown`, rest tracked; **D6** keep `<10/<15` but make it measurable or soften. **No new build-tag lane** (new examples ride `examples-build`/`examples-smoke`; the snippet-compile gate XG-3 rides existing CI). Four findings extend in place (T10/T34/T39/T40) + five confirmed (T35, T38b, T57, T58, example godoc headers + redaction docs); seven net-new (T157–T163); one new LATER (LATER-48). **Gate task T157 runs first**; per-task SPEC amendment lands same-PR ("amend SPEC first"); §10 **Rev 22** recorded when T157 lands.

### [ ] T157 — DX gates XG-1..XG-5 (footgun→godoc placement audit + example inventory/outcome audit + copy-paste-correctness audit + cross-reference integrity sweep + first-hour-journey audit) [P0] · S
- **Acceptance:**
  - [ ] **XG-1** footgun→godoc placement audit: for every load-bearing footgun/contract, a table records *symptom*, *where documented today* (SPEC §+line / code comment / godoc / nowhere), *whether a task already routes it to call-site godoc*, and *the gap* (min rows: at-least-once/must-dedupe §6.2.1, `AutoAck` §6.3/T35, `Delay` §6.5/T57, `UserID` 406 §6.5, `ConfirmTimeout(0)` §6.2, `PrefetchBytes()`+`ChannelQoS()` §6.3, `DeliveryMode` default §6.5, quorum `DeliveryLimit==0` §6.3/T58, `MaxRedeliveries` counter-B §6.3, "always wire `OnCancel`" §6.3, `Mandatory()` set-not-toggle §6.2, `Prefetch < Concurrency` §6.3) → the **footgun-doc-placement table**.
  - [ ] **XG-2** example inventory & outcome-assertion audit: the §4 list (eleven) vs on-disk (six) enumerated; for each existing example, whether its smoke test asserts an **outcome** or only **exit-0**; the hard paths with no example listed → the **examples-gap list**.
  - [ ] **XG-3** copy-paste-correctness audit: every code block from README + SPEC §5 + §6.2.1 (+ §6.3/§6.5 snippets) **compiled** against the real signatures; every non-building block flagged (catches DX-04 README `*Order`, DX-05 §6.2.1 out-of-scope `d`); a permanent anti-rot mechanism recommended (tangle-and-`go build`, or a `go:build ignore` doctest harness).
  - [ ] **XG-4** cross-reference integrity sweep: every `§x.y`/`decision N`/`RNN`/`Rev N` reference resolved; dangling/stale targets flagged; the `### Rev` heading list reconciled with the narrative (`spec-validation/README.md` cites Rev 9 with no SPEC heading) → a fix-list (no fabrication).
  - [ ] **XG-5** first-hour-journey audit: dial → topology → publish → consume → handle-failure → observe walked end-to-end; at each step, every place the user must already know something the docs do not teach there is recorded → the **first-hour journey walkthrough** (brief §11 is the seed).
  - [ ] **No behaviour change**; records §10 **Rev 22**.
- **Verify:** Results tables under `spec-validation/`; the footgun-doc-placement table, examples-gap list, broken-snippet list, cross-ref fix-list, and first-hour walkthrough are filled from *measured* code/docs; each downstream task cites its gate.
- **Files:** `spec-validation/` (results), read-only audit of `doc.go`, `README.md`, `SPEC.md`, `consumer.go`, `publisher.go`, `message.go`, `errors.go`, `examples/`.
- **Deps:** none (gate-first). **(DX gates, P0)**

### [ ] T158 — Teach the mental model: `doc.go` conceptual overview + the `amqp.go`/`warren` naming note [P1] · M
- **Acceptance:**
  - [ ] (DX-01, GDPR n/a) SPEC §4 (+ a §6.9 or §1 pointer) defines `doc.go`'s **mandatory contents** so it cannot rot back to a feature stub.
  - [ ] (DX-01) `doc.go` is written as the **canonical conceptual overview**: role-split pool (default 2 publisher + 2 consumer TCP connections), **at-least-once → you MUST dedupe by `MessageID`**, topology-declared-separately + `AttachTo` (reconnect redeclare), reconnect-is-a-synchronous-barrier (`Publish` blocks on `ErrReconnecting`), the degraded state, the no-silent-X bars, a **minimal correct publish/consume** snippet (compiles — gated by XG-3/T160), and pointers to the examples + §6.2.1.
  - [ ] (DX-15) a one-line **`amqp.go` directory vs `package warren` module** reconciliation note in `doc.go` + README.
- **Verify:** `go doc .` shows the mental model; the `doc.go` snippet compiles under the XG-3 gate; SPEC §4/§6 lists the mandatory sections.
- **Files:** `doc.go`, SPEC §4/§6.9, `README.md`.
- **Deps:** T157 (XG-5, XG-1). **(DX-01/15, P1)** **(D1)**

### [ ] T159 — Route footguns to the call site, systematically: the at-least-once/dedupe godoc routing + the §8 boundary + the routing table [P1] · M
- **Acceptance:**
  - [ ] (DX-03) a §8 *Always* boundary "load-bearing warnings appear verbatim in the godoc at the call site" + a **footgun→godoc routing table** in SPEC (seeded by XG-1), citing the `AutoAck()` mandate (§6.3 L1351, T35) as the template.
  - [ ] (DX-02) the **at-least-once → must-dedupe** contract (§6.2.1 L1013-1068) is routed onto the godoc of `Publish`, `Consume`, `PublishRetry`, `Delivery.Redelivered`, and `Message.MessageID`, each pointing to §6.2.1 + `examples/idempotent_consume/`.
  - [ ] the `Mandatory()` set-not-toggle note is folded into the table; the §8 boundary + table land in SPEC **before** the sweeps that satisfy them (T10/T34 extensions + the T40 final-godoc pass verify against this table).
- **Verify:** The §8 boundary + table are present in SPEC; the five call-site godocs carry the dedupe pointer; the T40 final pass can check the table.
- **Files:** SPEC §6.2/§6.2.1/§6.3/§8, godoc on `publisher.go`/`consumer.go`/`message.go`.
- **Deps:** T157 (XG-1). **(DX-02/03, P1)**

### [ ] T160 — Make the most-copied code correct + the snippet-compile gate [P1] · M
- **Acceptance:**
  - [ ] (DX-04) the README quickstart **compiles** against the value-typed `Handler[M]` (today's `func(ctx, o *Order)` does not build) and **handles errors** (no `_`-swallow).
  - [ ] (DX-05) the §6.2.1 canonical dedupe snippet **builds as written** (use `ConsumeRaw` for envelope access, or key the cache off the delivery — today it references out-of-scope `d.MessageID()` at L1049).
  - [ ] (XG-3) the **snippet-compile gate** is wired as a permanent CI mechanism (tangle fenced Go blocks + `go build`, or a `go:build ignore` doctest harness) so snippet rot cannot recur.
  - [ ] (DX-14, D6) the `<10/<15` claim is made measurable by proving the fixed quickstart against a stated definition (coordinates with the T39 extension).
- **Verify:** XG-3 gate green in CI; the README + §6.2.1 snippets build; the line-count definition is checkable.
- **Files:** `README.md`, SPEC §6.2.1, CI workflow / a doctest harness.
- **Deps:** T157 (XG-3); coordinates T39. **(DX-04/05/14, P1)**

### [ ] T161 — Migration guide from the 2023 library (`MIGRATION.md`) [P2] · S
- **Acceptance:**
  - [ ] (DX-06) a top-level `MIGRATION.md` (linked from README, not a README section — long and versioned) maps the 2023 API → warren, including the behavioural-change list: default-requeue → **default-nack-no-requeue**, single-connection → **role-split pool**, generic-options → **non-generic functional options**, builder-`Message` → **struct literal**, and the at-least-once/dedupe contract every adopter must internalise.
- **Verify:** `MIGRATION.md` exists and is linked from README; markdown lints clean.
- **Files:** `MIGRATION.md`, `README.md`.
- **Deps:** none (gate-independent, pure doc). **(DX-06, P2)** **(D2)**

### [ ] T162 — Discoverability of the right path: the production-tuning page + the error-message-names-the-remedy policy [P2] · M
- **Acceptance:**
  - [ ] (DX-07) a single **"Production tuning / recommended defaults for high throughput"** page (a `doc.go` section or `docs/tuning.md` linked from README) consolidates publisher/consumer connections, channel-pool size, the **prefetch/concurrency formula**, heartbeat, frame-max, and `ConfirmTimeout` — cross-linking the existing §6.1/§6.3 prose, not duplicating it.
  - [ ] (DX-08) a §6.8 policy "every operational error documents the cause **and** the remedy"; the remedy/knob lives in the godoc on each operational sentinel (`ErrChannelPoolExhausted` → `WithChannelPoolSize`/`WithPublisherConnections`, `ErrConfirmTimeout` → raise `ConfirmTimeout`/connections, …), with wrap sites adding the knob where context allows — **no new sentinels** (model: the actionable `IsTransient`/`IsPermanent` godoc §6.8 L1947-1971).
- **Verify:** The tuning page exists and cross-links §6.1/§6.3; each operational sentinel's godoc names a remedy; no new exported sentinel is added.
- **Files:** SPEC §6.1/§6.2/§6.3/§6.8, `doc.go` (or `docs/tuning.md`), godoc on `errors.go`, `README.md`.
- **Deps:** T157 (XG-5). **(DX-07/08, P2)** **(D3, D4)**

### [ ] T163 — Runnable references for the hard paths + the example outcome-assertion gate (lands last) [P2] · M
- **Acceptance:**
  - [ ] (DX-09) `examples/durable_retry/` ships — the **TTL + DLX retry ladder**, the durable path §6.5/R10-1 steers users toward and away from the lossy `delayed/` exchange (no example today) — builds (`examples-build`) and round-trips (`examples-smoke`).
  - [ ] (DX-10, D5) the ranked operational hard-case examples land or are tracked: P1 `graceful_shutdown` (drain in-flight on SIGTERM), then `tls_mtls` (gated on Phase-8 hardening), `cluster_failover` (`WithAddrs`), `callbacks` (`OnReturn`/`OnBlocked`/`OnCancel`/`OnTopologyDegraded` wired correctly — the §6.3 "always wire `OnCancel`" obligation), `observability` (metrics + alert rules).
  - [ ] (DX-11) §7 mandates that every example smoke test asserts an **outcome** (round-trip/observable effect), not exit-0 — model `examples/idempotent_consume/` (T38b), coordinated with the Lens-10/TV outcome-assertion gate.
  - [ ] each new example rides `examples-build`/`examples-smoke` (no new lane) and opens with a "What this example demonstrates" godoc header (do-not-regress).
- **Verify:** `make examples-build` builds the new examples; they smoke-run with outcome assertions on the `integration` lane; §7 carries the outcome-assertion mandate.
- **Files:** `examples/durable_retry/` (+ ranked operational examples), SPEC §7, `README.md` (examples table).
- **Deps:** T157 (XG-2); coordinates T38b. The full Grafana-dashboard cookbook is **LATER-48**. **(DX-09/10/11, P2)** **(D5)** — lands last.

### Checkpoint — Phase 23 (Lens 12) closed
- [ ] T157 gate results (XG-1..XG-5) captured into a results table; footgun-doc-placement table, examples-gap list, broken-snippet list, cross-ref fix-list, first-hour walkthrough filled from measured code/docs; first task records §10 **Rev 22**; no behaviour change.
- [ ] Mental model taught (DX-01/15): a reader who opens only `doc.go`/pkg.go.dev learns at-least-once→dedupe, topology-separate + `AttachTo`, reconnect-barrier, role-split pool, no-silent-X bars; SPEC §4/§6 defines `doc.go`'s contents; the `amqp.go`/`warren` naming is reconciled (T158).
- [ ] Must-dedupe impossible to miss (DX-02/03): `Publish`/`Consume`/`PublishRetry`/`Delivery.Redelivered`/`Message.MessageID` godoc carry the dedupe pointer; a §8 *Always* boundary + a SPEC footgun→godoc table make routing systematic (T159).
- [ ] Most-copied snippets compile (DX-04/05/14): README quickstart + §6.2.1 dedupe snippet build (XG-3 green); the quickstart handles errors, covers the §9 four, meets a stated line target (or the §1 claim is softened — D6) (T160 + T39).
- [ ] Call-site placement extensions (DX-12/13): `Message[M]` field godoc carries the `UserID` 406 + `DeliveryMode`-default footguns (T10); the connection-option/builder godoc carries the `ConfirmTimeout(0)`/`PrefetchBytes()`/`ChannelQoS()` caveats (T34).
- [ ] Right path discoverable (DX-06/07/08): `MIGRATION.md` maps the 2023 API → warren (T161); a single production-tuning page consolidates the knobs + the prefetch/concurrency formula; the operational sentinels' godoc names the remedy (no new sentinels) (T162).
- [ ] Hard paths have runnable references (DX-09/10/11): `examples/durable_retry/` (TTL+DLX) ships + smoke-runs; ranked operational examples land or are tracked; every example smoke test asserts an **outcome**, not exit-0 (model T38b) (T163).
- [ ] CHANGELOG exists (DX-17): `CHANGELOG.md` (Keep a Changelog) exists; the final godoc pass asserts the XG-1 footgun→godoc table is fully satisfied (T40).
- [ ] Cross-reference integrity (DX-16): the XG-4 fix-list resolves dangling `§x.y`/`decision N`/`RNN`/`Rev N` refs + reconciles the `### Rev` heading list; fixes ride the referencing task (T157).
- [ ] Confirmations hold (do-not-regress): `AutoAck` (T35), `Delay` (T57), the dedupe example (T38b), the `DeliveryLimit==0` warning (T58), the example godoc headers, and the redaction docs are unchanged or strengthened.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; `make examples-build` green (incl. `examples/durable_retry/`); new examples smoke-run on `integration`; the snippet-compile gate (XG-3) green in CI.
- [ ] README synced: the examples table gains `examples/durable_retry/` (+ operational examples that land); the "Available now / On the roadmap" split stays accurate; `MIGRATION.md` linked — checked per task; **no new public option this phase**.
- [ ] SPEC §10 "Rev 22" note records the Lens-12 pass; four findings extend their owning task in place (T10/T34/T39/T40) + five groups confirmed (T35, T38b, T57, T58, example godoc headers + redaction docs), not re-filed; exactly **one** new `LATER.md` entry (LATER-48); T157–T163 contiguous, no duplicate IDs; tally **172 tasks / 23 phases**.

## Phase 24 — Load & Reliability-Testing Re-review (Lens 13: are the spec's load-validation campaigns and harness realistic, complete, and capable of *proving* the billions/day reliability bar under real and hostile load? — every load/stress/bench/chaos artifact classified against the seven workload shapes; every §9 reliability claim mapped to the topology its proof needs; "can the single-node testcontainers harness prove a clustered claim?" answered = no)

Closes the Lens-13 adversarial spec validation (`spec-validation/13-load-testing.md`; brief `spec-validation/13-load-testing-plan.md`; findings `LT-01..LT-12`; gates `LG-1..LG-5`). Verdict **GO-WITH-CHANGES** — **no Blocker.** The v0.1 code is not defective, the chaos + benchmark + nightly-runner spine is sound and honest about cadence, and the load-bearing instrumentation + test mechanics are **already owned** (T44b/PC-04 queue-type, T45/TV-09 loss-counting, T71/R10-16 saturation metrics, T151 nightly runner) — so it is not NO-GO. But the spec's load-validation strategy **cannot prove its own billions/day-clustered bar**: every cluster-level reliability claim is validated only on a **single-node testcontainers** harness (the spec even *asserts* "connection loss is invisible" §1 L98-99 and "no `addr[0]` stampede on a full-cluster restart" T66 against a topology that cannot run them), four of the seven workload shapes (soak, spike, stress-to-failure, composite) are **never exercised** (so §1's no-silent-backpressure bar is asserted, not demonstrated), the chaos intensity is one-billion/day *at average* never at peak, the generators are uniform-small-no-op, and the load tail clips at 5000 ms while the operations that matter take 30s. **This lens is heavily cross-lens and its load-bearing substrate is already owned** — Lens-13 **confirms** the substrate (do-not-regress) and **extends** the owning tasks in place (cross-lens rule: a shared finding adds a `Lens-13 (LT-xx)` acceptance bullet, never re-filed); the net-new value is the **load-campaign layer no prior lens owns**. **Stays in lane:** no throughput-*model* re-derivation (Lens-09: T44b/T147-T149/T83/T116) and no per-criterion *falsifiability* re-audit (Lens-10: T150-T152/T151) — cited and confirmed, not re-opened. Owner decisions (recommended, overridable): **D1** the headline split — the §9 topology-honesty pass in-phase (T165, P1) *and* the multi-node harness (T166), standing env deferred (LATER-49); **D2** a ≥24h soak + a ≥1h sustained-throughput campaign on T151's on-demand/nightly cadence; **D3** spike + stress-to-failure to *demonstrate* §1 (both depend on T71's metrics); **D4** poison-storm + slow-broker + slow-consumer composites in-phase (T168), failover/upgrade composites in T166; **D5** defer the standing multi-node cluster + load-gen fleet to LATER-49, gate release tags not per-PR; **D6** a shared realistic generator in `amqptest`. **No new *required* per-PR build-tag lane** (the campaigns ride T151's scheduled workflow + on-demand `make` targets; the multi-node harness T166 is a distinct on-demand/nightly lane). Five findings extend in place (T44b/T45/T66/T71/T151) + five confirmed (T45/TV-09, T44b/PC-04, T63/R10-8, T59/R10-3, T65); six net-new (T164–T169); one new LATER (LATER-49). **Gate task T164 runs first**; per-task SPEC amendment lands same-PR ("amend SPEC first"); §10 **Rev 23** recorded when T164 lands.

### [ ] T164 — Load gates LG-1..LG-5 (workload-shape coverage matrix + harness-fidelity map + workload-realism audit + measurement-under-load audit + load-env/cadence reality) [P0] · S
- **Acceptance:**
  - [ ] **LG-1** workload-shape coverage audit: enumerate every load/stress/bench/chaos artifact the spec defines (§7 chaos L2241-2247, poison-loop L2249-2254, 100x/1000-cycle stress L2264-2269/§9 L2457; §9 reliability L2445-2469 + throughput L2471-2479) and classify each against {baseline, sustained, peak, soak, spike, stress-to-failure, composite}, marking *covered/partial/absent* with intensity/topology/measurement/assertion → the **workload-shape coverage matrix** (brief §11).
  - [ ] **LG-2** harness-fidelity audit: confirm in §7 L2229-2230 + the `amqptest`/`integration`/`chaos` lanes that the harness is **single-node testcontainers** (no multi-node cluster, no partition injection, no mixed-version); map each §9 reliability criterion to `provable-single-node | needs-multi-node | needs-partition-injection | needs-mixed-version`; record the **T66 "full-cluster restart" assertion is unrunnable on the current harness** as the anchor fact → the **harness-adequacy map** (brief §12).
  - [ ] **LG-3** workload-realism audit: confirm whether the spec specifies the generators' payload-size distribution (incl. near `MaxMessageSizeBytes`/multi-frame), handler-latency distribution, routing-key/queue cardinality, and arrival burstiness, or defaults to uniform-small-no-op.
  - [ ] **LG-4** measurement-under-load audit: confirm the default histogram tops at **5000 ms** (§6.9 L2073-2076) vs `ConfirmTimeout` 30s + the R10-8 barrier cap (L3098-3104) so the load tail clips to `+Inf`; confirm the leading-indicator saturation metrics (pool-wait, `consumer_in_flight`, `consumer_redelivered_total`) are not-yet-present (deferred to R10-16/T71).
  - [ ] **LG-5** load-environment & cadence reality: confirm §7 names benchmarks "On-demand and on release tag" (L2222) and stress "Nightly" (L2268) with **no defined multi-node load environment** (dedicated cluster + load-gen fleet) and **no release-gating cadence**.
  - [ ] **No behaviour change**; records §10 **Rev 23**.
- **Verify:** Results tables under `spec-validation/`; the coverage matrix + harness-adequacy map filled from *measured* spec/lane reality; each downstream task cites its gate (LG-1→LT-02/03/04/05/09/10, LG-2→LT-01/05-cluster, LG-3→LT-06, LG-4→LT-07/08, LG-5→LT-12).
- **Files:** `spec-validation/` (results), read-only audit of `SPEC.md §7/§9/§6.9/§10`, the `amqptest`/`integration`/`chaos` lanes.
- **Deps:** none (gate-first). **(LG gates, P0)**

### [ ] T165 — §9/§7 reliability-topology honesty pass + the §7 workload-shape coverage matrix (the cheap, mandatory half of the headline) [P1] · S
- **Acceptance:**
  - [ ] (LT-01 honesty half) every §9 reliability criterion is labelled with the topology it is **actually proven against** (`single-node | multi-node-quorum | needs-partition-injection | needs-mixed-version`) so the spec stops implying a clustered guarantee it validates single-node — including §1 L98-99 "connection loss is invisible" and the T66 full-cluster-restart assertion.
  - [ ] (LT-01) the **§7 workload-shape coverage matrix** is added (the seven shapes × run?/intensity/topology/measurement/assertion).
  - [ ] (LT-09) the chaos/sustained intensity is stated as a **peak multiple** of the average bar (~11.6k/s ≈ 1 B/day average; peaks 3–10×), not an arbitrary 10k.
  - [ ] (LT-12 cadence half) the heavy campaigns are stated to gate **release tags**, not per-PR.
- **Verify:** every §9 criterion carries a topology label; §7 has the coverage matrix; the intensity is a stated peak multiple; the cadence wording names release-tag vs nightly; `make lint` clean.
- **Files:** SPEC §9/§7, `README.md` (if the testing-strategy description changes).
- **Deps:** T164 (LG-1, LG-2); coordinates T45 (LT-09 intensity) + T44b (LT-10 pointer). **(LT-01/09/12, P1)** **(D1, D5)**

### [ ] T166 — Multi-node cluster load harness + cluster-dependent campaign specs (the headline's expensive half, lands last) [P2] · L
- **Acceptance:**
  - [ ] (LT-01 harness half) a **3-node quorum cluster harness** in `amqptest` — multi-node compose, partition injection via `pause_minority`/`autoheal`, mixed-version 3.13/4.x members — distinct from the single-node lanes, on a new **on-demand/nightly multi-node lane**.
  - [ ] (LT-05 cluster half) the cluster-dependent campaigns are specified: **quorum leader failover under sustained load**, **partition-under-load**, **reconnect-storm across nodes** (R10-11/T66's full-cluster-restart assertion finally has a harness), **quorum confirm latency under Raft replication**, **repeated-failover-under-load**, and **rolling 3.13→4.x broker-upgrade-under-load**.
  - [ ] the *standing* dedicated environment is recorded as **LATER-49** (T166 lands the harness contract + campaign specs against an ephemeral compose cluster, not the standing env).
- **Verify:** the multi-node lane stands up a 3-node quorum cluster on its on-demand/nightly cadence; the cluster campaigns run and assert their outcomes; T66's full-cluster-restart assertion runs on it.
- **Files:** `amqptest/`, `.github/workflows/*.yml` (the multi-node lane), `docker-compose.*.yml`, SPEC §7.
- **Deps:** T164 (LG-2); extends T66, T151. Lands last (heaviest). **(LT-01/05-cluster, P2)** **(D1, D4)**

### [ ] T167 — Soak/endurance + sustained-throughput campaigns [P2] · M
- **Acceptance:**
  - [ ] (LT-02) a **soak campaign (≥24h at target load)** asserts RSS/goroutine/fd/Prometheus-series trends over the named leak surfaces (confirm-tracker map, dedupe cache, `(channel-instance-id, MessageID)` counter map, fd/channel, series cardinality) — the 1000-cycle fast-churn check (§9 L2457) catches fast-cycle leaks, not slow steady-state growth.
  - [ ] (LT-10) a **sustained-throughput campaign (sustain the §9 target ≥1h** without latency creep / pool saturation / memory growth), distinct from the one-shot benchmark.
  - [ ] both ride T151's on-demand/nightly cadence (not per-PR); the loss-counting reuses T45/TV-09 unchanged.
- **Verify:** the soak campaign runs ≥24h on the on-demand/nightly lane and asserts the trend bounds; the sustained campaign holds the §9 target ≥1h without creep; `goleak.VerifyNone` clean.
- **Files:** SPEC §7/§9, `amqptest/`, `.github/workflows/*.yml` (T151's scheduled workflow).
- **Deps:** T164 (LG-1), T71; extends T44b, T151. **(LT-02/10, P2)** **(D2)**

### [ ] T168 — Spike, stress-to-failure & composite-incident campaigns (demonstrate §1 no-silent-backpressure) [P2] · M
- **Acceptance:**
  - [ ] (LT-03) a **spike campaign** (1×→N× in seconds) asserts graceful backpressure — classifiable errors, bounded memory, no silent stall/OOM.
  - [ ] (LT-04) a **stress-to-failure campaign** drives to saturation, locates the cliff, and asserts every overload path surfaces a classifiable error (not a stall or OOM).
  - [ ] (LT-05 single-node half) the **single-node composite campaigns** run: poison-storm (10k + X% poison → two-counter+DLX holds + DLQ bound **T65** + redelivery observable **T71**), slow-consumer+fast-publisher flow control, slow-broker confirm-latency→pool-saturation cascade (R10-18/LATER-34). The failover/upgrade composites live in T166.
- **Verify:** the spike/stress campaigns demonstrate the §1 bar on the on-demand/nightly lane; the composites run and the T65 bound + T71 redelivery signal hold under sustained adversarial load.
- **Files:** SPEC §7/§9, `amqptest/`, `.github/workflows/*.yml` (T151's scheduled workflow).
- **Deps:** T164 (LG-1), T71; extends T151; confirms T65. **(LT-03/04/05-single-node, P2)** **(D3, D4)**

### [ ] T169 — Realistic workload generators + measurement-under-load fixes [P2] · M
- **Acceptance:**
  - [ ] (LT-06) configurable generators in `amqptest` — payload-size distribution (incl. near `MaxMessageSizeBytes`/multi-frame), handler-latency distribution, routing-key cardinality, arrival burstiness — shared across bench/chaos/soak/spike, with the §9/§7 headline numbers stating the distribution used (replaces uniform-small-no-op).
  - [ ] (LT-07) the load reports **override `WithLatencyBuckets`** to cover the 30s `ConfirmTimeout` + the R10-8 barrier (the default 5000 ms top bucket clips the tail) and capture **p99/p999 + saturation indicators**, not mean throughput.
  - [ ] no new public option (uses the existing `WithLatencyBuckets`); the realistic generator must not regress T59's R10-3 ordering-under-load contract.
- **Verify:** a generator drives a configurable distribution; the load reports' histogram covers the 30s window and reports p99/p999; the headline numbers state the distribution.
- **Files:** `amqptest/`, SPEC §7/§6.9.
- **Deps:** T164 (LG-3, LG-4); coordinates T71 for the LT-08 saturation-metric set. **(LT-06/07, P2)** **(D6)**

### Checkpoint — Phase 24 (Lens 13) closed
- [ ] T164 gate results (LG-1..LG-5) captured into a results table; the workload-shape coverage matrix (§11) + harness-adequacy map (§12) filled from measured spec/lane reality; first task records §10 **Rev 23**; no behaviour change.
- [ ] Topology honesty (LT-01/09/12): every §9 reliability criterion carries the topology it is proven against; §7 has the workload-shape coverage matrix; the spec no longer implies a clustered guarantee validated single-node (incl. §1 L98-99 + the T66 assertion); the intensity is a stated peak multiple; the heavy campaigns gate release tags, not per-PR (T165).
- [ ] Cluster harness (LT-01/05): a 3-node quorum harness with partition injection + a mixed-version axis is specified; the quorum-failover / partition / reconnect-storm / quorum-confirm-latency / rolling-upgrade campaigns are specified; T66's full-cluster-restart assertion runs on it; the standing env is LATER-49 (T166).
- [ ] Soak + sustained (LT-02/10): a ≥24h soak asserts RSS/goroutine/fd/series trends over the named leak surfaces; a ≥1h sustained-throughput campaign is distinct from the one-shot benchmark (T167).
- [ ] Overload demonstrated (LT-03/04/05): spike + stress-to-failure campaigns *demonstrate* §1's no-silent-backpressure bar; the poison-storm + slow-broker + slow-consumer composites run, and the T65 bound + T71 redelivery signal hold under sustained load (T168).
- [ ] Realism + measurement (LT-06/07/08): the generators model payload-size/handler-latency distributions + routing-key cardinality + burstiness, the headline numbers state the distribution, the load reports override `WithLatencyBuckets` to cover the 30s window + report p99/p999, and T71's saturation metrics land before the spike/stress/soak campaigns run (T169 + extend T71).
- [ ] Quorum-vs-classic (LT-11): confirmed already satisfied by T44b/PC-04; the campaigns inherit the queue-type-stated discipline (no regression).
- [ ] Cross-lens extensions land (LT-07/08/09/10): T44b/T45/T66/T71/T151 each carry their `Lens-13 (LT-xx)` acceptance bullet; headers unchanged.
- [ ] Confirmations hold (do-not-regress): the TV-09 loss-counting method (T45), the PC-04 queue-type discipline (T44b), the R10-8 barrier cap (T63), the R10-3 ordering load (T59), and the T65 DLQ bound are unchanged or strengthened.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new campaign/harness lanes green on their stated (on-demand/nightly/release) cadence; **no new *required* per-PR build-tag lane**.
- [ ] README synced: only if T166/T167 ship a runnable `examples/` artifact does the examples table change; if a "load & reliability testing" subsection is added, the testing/reliability docs stay accurate — checked per task; **no new public option this phase**.
- [ ] SPEC §10 "Rev 23" note records the Lens-13 pass; five findings extend their owning task in place (T44b/T45/T66/T71/T151) + five groups confirmed (T45/TV-09, T44b/PC-04, T63/R10-8, T59/R10-3, T65), not re-filed; exactly **one** new `LATER.md` entry (LATER-49); T164–T169 contiguous, no duplicate IDs; tally **178 tasks / 24 phases**.

### Checkpoint — v0.1.0 shipped
- [ ] Every SPEC §9 success criterion ticked.
- [ ] `v0.1.0` tag on `main`.
- [ ] README + examples link from the GitHub repo landing page.
- [ ] **Done.**

---

## Quick stats
- Total tasks: **178** (Rev 5: +T07c redaction, +T07d multi-conn, +T34b SASL EXTERNAL, +T44b bench, +T45b security scan; Rev 6: +T18b HandlerTimeoutVerdict matrix, +T38b idempotent_consume example, +T38c ordered_consume example; 2026-05-24: +T34c panic isolation for user-provided callbacks; Phase 10: +T47-T56 SRE Resilience; Phase 11: +T57-T72 Rev 10 AMQP/SRE re-review; 2026-05-28: +T73 codec-call panic-safety recover; Phase 12 (2026-05-28): +T74-T83 Lens-01 protocol-correctness re-review; Phase 13 (2026-05-28): +T84-T93 Lens-02 distributed-systems re-review, pulls T62/T63/T70/T71 forward, adds the `chaos` lane; Phase 14 (2026-05-28): +T94-T100 Lens-03 interoperability/wire-format re-review, adds the `interop` lane + LATER-39; Phase 15 (2026-05-28): +T101-T110 Lens-04 event-driven-architecture re-review, pulls T68/T69 forward, extends T85, adds LATER-40, brings `x-consistent-hash` into scope (no new build-tag lane); Phase 16 (2026-05-28): +T111-T118 Lens-05 SRE/production-operability re-review, pulls T67/T72 forward, extends T61/T62/T63/T65/T66/T70/T71 (cross-lens), no new build-tag lane, no new LATER; Phase 17 (2026-05-28): +T119-T129 Lens-06 Go-API/library-design re-review, fixes the GA-01 DeliveryMode silent-non-persistence Blocker, extends T37/T68/T69/T70/T71/T112 (cross-lens), adds LATER-41, no new build-tag lane; Phase 18 (2026-05-29): +T130-T142 Lens-07 security/threat-modeling re-review, fixes the ST-06 inbound-DoS Blocker (promotes/supersedes LATER-35), extends T65 (cross-lens), adds LATER-42, no new build-tag lane; Phase 19 (2026-05-29): +T143-T146 Lens-08 go-concurrency-runtime re-review, fixes the CR-02 counter-B lost-update Blocker, extends T07/T08/T13/T18/T20/T34c/T45/T60/T70 (cross-lens), adds LATER-43, no new build-tag lane; Phase 20 (2026-05-29): +T147-T149 Lens-09 performance-capacity re-review, no Blocker, extends T10/T11/T17/T18/T22/T44b/T83/T116 (cross-lens) + confirms T113, adds LATER-45, no new build-tag lane; net-new value is the per-message hot-path allocation cluster (PC-06/07/08/09), the multiple=true O(n) resolution (PC-11), and the §9-criteria/benchmark-methodology gaps — the capacity-honesty headline is already owned by T83/T116/T113; Phase 21 (2026-05-29): +T150-T152 Lens-10 test-strategy & verifiability re-review, no Blocker, extends T37/T41/T42/T44/T44b/T45/T45b/T59/T81 (cross-lens) + confirms T94/T61/T63/T67/T143/T146, adds LATER-46; **new lanes in scope** (a scheduled/nightly `schedule:` trigger + a RabbitMQ 3.13/4.x integration matrix + an integration-broker-required guard + conformance-lane wiring — the conformance lane is already in §7/§3 but not CI, so this is remediation not invention); net-new value is the **CI-infrastructure + criteria-honesty layer** (no nightly runner, no version matrix, an unenforced coverage floor, author-laptop throughput numbers, §7/§9 internal inconsistencies) — every load-bearing contract the lens targets is already owned (T59/T94/T61/T63/T67/T81/T45b); Phase 22 (2026-05-29): +T153-T156 Lens-11 compliance-GDPR-LGPD re-review, no Blocker, extends T65/T85/T132/T134/T141 (cross-lens) + confirms T07c/T133 + bounded metric labels + OTel size-not-content + the header-non-mutation contract, adds LATER-47, no new build-tag lane; net-new value is the privacy-by-design layer no prior lens owns — the erasure-structural-silence + pointer-out-mis-positioned-as-size-only headline (DP-01/02), the data-minimisation footguns (DP-05/06/10/11/12), the international-transfer/residency caveat (DP-09), and the records-of-processing/controller-vs-processor boundary (DP-13/14/15); the confidentiality half is already owned by T134/T132/T141/T07c/T133/T65/T85; Phase 23 (2026-05-29): +T157-T163 Lens-12 DX & documentation re-review, no Blocker, extends T10/T34/T39/T40 (cross-lens) + confirms T35/T38b/T57/T58 + the example godoc headers + the redaction-in-logs contract, adds LATER-48, no new build-tag lane; net-new value is the **pit-of-success / first-hour-learning-path layer no prior lens owns** — the `doc.go` conceptual overview undefined-placeholder + naming (DX-01/15), the at-least-once→dedupe contract not routed to the call-site godoc + the ad-hoc footgun→godoc placement policy (DX-02/03), the two most-copied snippets not compiling — README `*Order` handler + §6.2.1 out-of-scope `d` (DX-04/05), the missing migration guide / production-tuning page / error-remedy policy (DX-06/07/08), and the durable-retry-ladder + operational examples + outcome-assertion gate (DX-09/10/11); most placement work is already owned by T10/T34/T35/T38/T38b/T39/T40/T57/T58; Phase 24 (2026-05-29): +T164-T169 Lens-13 load-&-reliability-testing re-review, no Blocker, extends T44b/T45/T66/T71/T151 (cross-lens) + confirms T45/TV-09 loss-counting + T44b/PC-04 queue-type + T63/R10-8 barrier cap + T59/R10-3 ordering load + the T65 DLQ bound, adds LATER-49, no new required per-PR build-tag lane (campaigns ride T151's scheduled workflow + on-demand make targets; the multi-node harness T166 is a distinct on-demand/nightly lane); net-new value is the load-campaign layer no prior lens owns — the single-node-harness-can't-validate-cluster-claims headline (LT-01) + the §9 topology-honesty pass (T165), the four never-run workload shapes soak/spike/stress/composite (LT-02/03/04/05 → T166/T167/T168), the realistic generators + measurement-under-load tail-clip fix (LT-06/07 → T169), and the intensity-is-average-not-peak + one-shot-vs-sustained gaps (LT-09/10); the load-bearing instrumentation + test mechanics are already owned by T44b/T45/T71/T151; stays in lane (no throughput-model re-derivation Lens-09, no falsifiability re-audit Lens-10)).
- Phases: **24**.
- Estimated sizing: 8× XS · 40× S · 23× M · 0× L (none too big).
- Sequential pinch-points: T07c (`internal/redact`) before T03/T04/T07/T07d; T07 (single-TCP Connection with reconnect barrier + degraded state) and T07b/T07c before T07d (multi-conn pool); T07d before everything in §6 of the spec; T15 (Declare) before T31 (delayed); T18 (Consumer + re-subscribe + handler-ctx cancel + HandlerTimeoutVerdict + UUID-tag default) before T18b (verdict matrix test) and T28 (OTel consume); T45 chaos + T45b security gate T46 release; T38b/T38c examples gate T46 release.
- Fuzz targets in v0.1.0: `FuzzCodecJSON` (T09), `FuzzCodecProtobuf` (T24), `FuzzCodecCloudEventsBinary` (T26), `FuzzXDeathParser` (T17), **`FuzzRedactURI` (T07c)**. Others added later as bugs surface.
- Bench gates (T44b): ≥ 30k msg/s single-conn, ≥ 100k msg/s with `WithPublisherConnections(4)+WithChannelPoolSize(16)`, `PublishBatch` ≥ 5× `Publish`.
- Operational decisions: deps pinned exact in `go.mod`; testcontainer images pinned minor-patch; conformance against a live broker (no stub); pre-commit hooks opt-in via `make hooks`; no goreleaser (pure library); OTel Messaging semconv pinned to v1.27.0+; `golangci-lint` includes `errorlint`; `amqptest` plugin enablement supports three explicit modes (pre-baked image / mounted `.ez` / `t.Skip`).
- Reliability invariants (mandatory): credential redaction (T07c, T45b), consumer re-subscribe (T18, T19), handler-ctx cancel on channel close (T18, T19), broker-nack → `ErrPublishNacked` (T11, T13), JSON lax default per Postel's Law + opt-in `NewJSONStrict` (T09, Rev 8), quorum-queue `x-delivery-limit` (T14, T15, T20), **synchronous reconnect barrier + degraded state** (T07, T16, T45), **at-least-once with documented dedupe pattern** (T13, T38b, SPEC §6.2.1), **HandlerTimeout verdict consistency** (T18, T18b), **client-side UserID validation** (T13), **Replier missing-DLX validation** (T30), **SASL EXTERNAL fail-closed** (T34b), **basic.cancel surfacing** (T36), **default conn counts 2/2 with consumer-tag UUID** (T07d, T18), **panic isolation for user-provided callbacks** (T34c).
