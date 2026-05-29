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
  - [ ] **Opt-in high-cardinality labels** via `WithMetricsLabels(MetricsLabelRoutingKey, MetricsLabelMessageType)` on connection or per-builder.
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
  - [ ] `Return.Properties` populated as a full `ReturnedProperties` struct — **12 `basic.properties` fields + `Headers` (13 total)**, mirroring SPEC §6.2 field-for-field. Not a flat map.
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
- [ ] Topology declare idempotent under repeat.
- [ ] Mismatch detected and surfaced.
- [ ] AttachTo re-declares cleanly after broker restart.
- [ ] **`examples/topology/main.go` and `examples/deadletter/main.go` build (unit lane) and smoke-run end-to-end (integration lane)** per T16b — SPEC §7 + Rev decision 49.
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
  - [ ] Integration test exercises `Redelivered()`, `Headers()`, `DeathCount()`.
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
- [ ] Error-driven semantics validated for all three classes.
- [ ] Poison-loop bounded.
- [ ] Escape hatch usable for raw envelope inspection.
- [ ] **`examples/consume/main.go` builds (unit lane) and smoke-runs end-to-end (integration lane)** per T21b — SPEC §7 + Rev decision 49.
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
  - [ ] `Publisher.PublishBatch(ctx, []Message[M]) ([]PublishResult, error)` publishes every input message (never short-circuits, except the size-cap guard below).
  - [ ] **Size-cap guard:** if `len(msgs) > PublishBatchMaxSize`, returns immediately with `(nil, ErrBatchTooLarge)`. No channel work performed. Caller chunks.
  - [ ] **Single-channel pipelining:** all N publishes occur on **one** acquired channel from the publisher pool, so RabbitMQ's per-channel ordering guarantee makes input order = consume order. Documented as a hard guarantee in godoc.
  - [ ] Returns `[]PublishResult` with one slot per input; per-message error in `Result.Err` (`ErrInvalidMessage`, `ErrPublishNacked`, `ErrUnroutable`, `ErrChannelClosed`).
  - [ ] Overall error wraps `ErrPartialBatch` if any failed.
  - [ ] Pipelines all publishes, then waits one confirm window — including correctly resolving a single `multiple=true` ack that covers many delivery tags (see T11) and a single `multiple=true` nack that covers many delivery tags with `ErrPublishNacked`.
  - [ ] **Channel-close recovery contract documented in godoc** (per SPEC §6.2): "Per-message `ErrChannelClosed` does NOT distinguish 'broker persisted' from 'broker did not receive'. Retry produces duplicates when the broker persisted but the ack was lost. `PublishRetry` does NOT apply to `PublishBatch` — chunking and partial-retry are the caller's responsibility, because the right strategy is workload-specific. Consumers MUST be idempotent per §6.2.1."
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
  - [ ] `MaxRedeliveries` counter B (from T20) increments per message in the batch when a `Nack(requeue=true)` is emitted for the whole batch.
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
- [ ] BatchConsumer flushes on both triggers.
- [ ] **`examples/batch_publish/main.go` and `examples/batch_consume/main.go` build (unit lane) and smoke-run end-to-end (integration lane)** per T23b — SPEC §7 + Rev decision 49.
- [ ] **Review with human before Phase 6.**

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

### [ ] T29 — RPC `Caller[Req,Resp]` · M
- **Acceptance:**
  - [ ] `CallerFor[Req,Resp](conn).Build()` returns a configured caller.
  - [ ] `Call(ctx, req)` uses RabbitMQ direct reply-to (`amq.rabbitmq.reply-to`) by default; reply consumer is declared **before** the request is published; consumer auto-enables `no-ack` (required by the pseudo-queue protocol). Auto-stamps `CorrelationID` and `ReplyTo` on the request message if empty.
  - [ ] `UseExclusiveReplyQueue()` builder method switches to a real exclusive auto-delete reply queue per Caller, with regular ack semantics.
  - [ ] `ctx` deadline maps to per-call timeout → `ErrCallTimeout`.
  - [ ] Concurrent calls return the right response (`CorrelationID` matching).
  - [ ] If the underlying channel closes during a Call, in-flight calls return `ErrChannelClosed`; new calls reconnect transparently.
- **Verify:** Integration tests: (a) 100 concurrent calls, every response matches its request; (b) ctx timeout returns `ErrCallTimeout` cleanly; (c) `UseExclusiveReplyQueue` round-trip; (d) channel close mid-call surfaces `ErrChannelClosed`.
- **Files:** `rpc.go`, `rpc_caller_builder.go`, `rpc_caller_integration_test.go`.
- **Deps:** T12, T18.

### [ ] T30 — RPC `Replier[Req,Resp]` + at-least-once ordering · S
SPEC compliance: handler errors do **not** send an error envelope to
the `Caller` — the caller observes `ErrCallTimeout` on `ctx` deadline.
Failed requests are `Nack(requeue=false)`; without a DLX configured
on the request queue, **this is a silent drop**. The `OnError` hook
is the only client-side signal; treat it as load-bearing. Rev 5
adds the at-least-once reply ordering contract.
- **Acceptance:**
  - [ ] `ReplierFor[Req,Resp](conn).Build()` returns a configured replier.
  - [ ] `Serve(ctx, ReplyHandler)` consumes requests and publishes responses to `ReplyTo` with matching `CorrelationID`.
  - [ ] **At-least-once reply ordering**: for a successful handler, the replier publishes the reply, **awaits its confirm** (subject to `PublishTimeout`/`ConfirmTimeout` of the internal reply publisher), and **then** acks the request. If the reply publish fails (`ErrPublishNacked`, `ErrConfirmTimeout`, `ErrChannelClosed`), the request is `Nack(false)` so it goes to the request queue's DLX (if configured); the caller observes `ErrCallTimeout` on `ctx` deadline.
  - [ ] **Crash-between-confirm-and-ack contract** documented: broker redelivers the request, replier sends a second reply. Callers MUST dedupe by `CorrelationID`. Godoc on `Serve` carries this verbatim.
  - [ ] `OnError(func(ctx, req, err))` builder hook: handler error is reported via the hook; the request is `Nack`'d without requeue (so it goes to a DLX if configured, or is dropped if not); the caller observes `ErrCallTimeout` once its `ctx` expires.
  - [ ] **Godoc on `OnError` and `Build` documents the silent-drop failure mode in full**, with explicit guidance: "Configure a DLX on the request queue if you need failed requests preserved for forensics. Without a DLX, `Nack(requeue=false)` is a drop and `OnError` is the only signal."
  - [ ] **`ReplierBuilder.Topology(t)` auto-validates DLX presence** when the request queue was declared via this library's `Topology` on the same connection: inspects `t.DeadLetters` for an entry matching the request queue; if missing, `Build()` returns `ErrInvalidOptions` with the message `"Replier request queue <name> has no DeadLetter entry in Topology; Nack(false) drops will be silent. Add a DeadLetter or use AllowMissingDLX() to acknowledge."`. (Rev 6)
  - [ ] **`AllowMissingDLX()` escape hatch** opts out of the validation when the request queue is declared out-of-band; the godoc documents the trade-off.
  - [ ] **Mandatory metric `replier_drop_no_dlx_total{queue}`** increments every time the framework `Nack(false)`s a request whose source queue has no declared DLX (regardless of whether `Topology(t)` was wired) — drops are never invisible. (Rev 6)
  - [ ] No broker-side validation of DLX presence at `Build()` time (would require management-plugin access and an extra round-trip). Static validation via `Topology(t)` plus the runtime metric is the contract.
- **Verify:** Integration tests:
  - Happy path round-trip Caller↔Replier with success.
  - **Reply-publish-failure path:** simulate a forced reply-publisher channel close immediately after the handler returns; assert the request is `Nack(false)`, lands in the DLQ if configured, and the caller times out with `ErrCallTimeout`.
  - Handler error + DLX configured: `OnError` fires once with the original error, the request lands in the DLQ (assert via `rabbitmqctl list_queues <dlq> messages`), and the caller times out cleanly with `ErrCallTimeout`.
  - Handler error + no DLX: `OnError` fires once, **`replier_drop_no_dlx_total{queue}` increments by 1**, the request is gone from the source queue, and `rabbitmqctl list_queues <source> messages` returns 0 — explicit assertion that the drop is real (this is a negative-path documentation test, *not* a regression we intend to silently change later).
  - **`Topology(t)` validation test:** declare a request queue WITHOUT a `DeadLetter` entry, build a `Replier` with `.Topology(t)`; assert `Build()` returns `ErrInvalidOptions`. Repeat with `.AllowMissingDLX()`; assert `Build()` succeeds.
- **Files:** `rpc_replier.go`, `rpc_replier_builder.go`, `rpc_replier_integration_test.go`, `rpc_replier_reply_failure_integration_test.go`, `rpc_replier_dlx_validation_test.go`.
- **Deps:** T29.

### [ ] T31 — Delayed messages · S
- **Acceptance:**
  - [ ] `Message[M].Delay` field honored at publish time (sets `x-delay` header).
  - [ ] `Topology` declares `x-delayed-message` exchanges when `Kind = ExchangeDelayed`; `Args` carries `x-delayed-type` to specify the underlying type.
  - [ ] Helper `warren.DelayedTopic(name string)` constructs the right `Exchange{}` literal.
- **Verify:** Integration test: testcontainer with `rabbitmq_delayed_message_exchange` plugin enabled; publish with 2s delay; assert delivery happens between 2s and 2.5s.
- **Files:** `delay.go`, edits to `topology.go`, `message.go`, `delay_integration_test.go`.
- **Integration fixture:** **`amqptest/`** (T37) — three plugin modes and `amqptest.RequireDelayedExchange(t)` per SPEC §6.9; do not add a parallel `testing/` package. If T31 lands before T37, keep delayed tests behind skip/minimal container wiring until the shared `amqptest` helper exists.
- **Deps:** T15, T12. (Strong pairing with **T37** for the canonical broker image/plugins.)

### [ ] T31b — Checkpoint examples: `examples/rpc/main.go` + `examples/delayed/main.go` · S
Per SPEC §7 "Executable examples at checkpoints" + §10 Rev
decision 49. Closes the Phase 7 checkpoint.
- **Acceptance:**
  - [ ] `examples/rpc/main.go` is `package main`, reads `AMQP_URL`, declares a request queue with a `DeadLetter` entry, runs a `ReplierFor[PriceReq, PriceResp]` with `.Topology(t)` (which auto-validates DLX presence) and `.Serve(ctx, handler)`, runs a `CallerFor[PriceReq, PriceResp]` that performs 3 concurrent `Call(ctx, req)` invocations and asserts each response matches its request via `CorrelationID`, and exits 0 after all three succeed. A negative-path block additionally demonstrates `ErrCallTimeout` by sending a request with a 50ms ctx against a handler that sleeps 200ms.
  - [ ] `examples/delayed/main.go` is `package main`, reads `AMQP_URL`, declares an `Exchange{Kind: ExchangeDelayed, Args: warren.Headers{"x-delayed-type": "topic"}}` + a bound queue, publishes a message with `Message[M].Delay = 2*time.Second`, runs a consumer that records the arrival time, asserts the arrival is between 2s and 2.5s of publish, and exits 0.
  - [ ] Top-of-file godoc on `rpc/` documents the at-least-once reply ordering contract (per Rev 5 + T30) — consumers MUST dedupe by `CorrelationID`. Top-of-file godoc on `delayed/` documents the `x-delayed-message` plugin requirement and points at `amqptest.RequireDelayedExchange(t)`.
  - [ ] `go build ./examples/...` green on the unit lane (no plugin required for build).
  - [ ] Integration test per example (`example_integration_test.go` in each subdir, `integration` tag) runs the example as a subprocess. The `rpc/` test runs against the standard testcontainer broker. The `delayed/` test calls `amqptest.RequireDelayedExchange(t)` once T37 has landed, skipping cleanly when the plugin is unavailable; if T31b lands before T37, the `delayed/` integration test guards itself with the same env-var check (`AMQPTEST_IMAGE` / `AMQPTEST_DELAYED_PLUGIN_FILE`) and `t.Skip`s otherwise. Asserts exit 0 + the example's stdout contains the expected ordering / timing logs. `goleak.VerifyNone(t)` clean.
- **Verify:** `go build ./examples/...`; `go test -race -tags=integration ./examples/rpc/... ./examples/delayed/...`; manual subprocess smoke-run.
- **Files:** `examples/rpc/main.go`, `examples/rpc/example_integration_test.go`, `examples/delayed/main.go`, `examples/delayed/example_integration_test.go`, edits to `README.md`.
- **Deps:** T29, T30, T31. (Soft pairing with T37 for the delayed-exchange plugin; not a blocker — skip-clean until T37 lands.)

### Checkpoint — Phase 7 done
- [ ] RPC happy path + timeout green.
- [ ] Delayed delivery within ±20% of requested delay.
- [ ] **`examples/rpc/main.go` and `examples/delayed/main.go` build (unit lane) and smoke-run end-to-end (integration lane; `delayed/` may skip when the plugin is unavailable)** per T31b — SPEC §7 + Rev decision 49.
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

### [ ] T33 — Cluster failover via `WithAddrs` · S
- **Acceptance:**
  - [ ] `WithAddrs([]string)` tries addresses in order on initial connect.
  - [ ] On reconnect, rotates to the next address (round-robin).
  - [ ] First successful address sticks until the next disconnect.
- **Verify:** Integration test: docker-compose two RabbitMQ nodes; stop the first, assert reconnect succeeds against the second.
- **Files:** edits to `connection.go`, `options_connection.go`, `connection_failover_integration_test.go`.
- **Deps:** T07.

### [ ] T34 — Remaining Connection options · S
Rev 5 adds `WithConnectionName`, `WithPublisherConnections`,
`WithConsumerConnections`, `WithOnResubscribe`.
- **Acceptance:**
  - [ ] `WithVHost`, `WithAuth`, `WithHeartbeat`, `WithChannelMax`, `WithFrameMax`, `WithDialer`, `WithClientProperties` implemented.
  - [ ] **`WithConnectionName(name string)`** — default `<binary>-<hostname>-<pid>`; sets `client_properties.connection_name`. Role and index suffixes (`-pub-0`, `-con-0`, …) appended per TCP connection.
  - [ ] **`WithPublisherConnections(n int)`** + **`WithConsumerConnections(n int)`** — already implemented in T07d; T34 covers the option wiring + default values (both 1).
  - [ ] **`WithOnResubscribe(func(queue string))`** — fires once per consumer re-subscribe (alongside the mandatory metric in T19).
  - [ ] `WithClientProperties` default sets `product=warren`, `version=<from runtime/debug>`, `platform=Go <ver>`, `connection_name=<from WithConnectionName>`.
  - [ ] **`WithFrameMax` godoc sizing table** (Rev 7, per SPEC §6.1 lines 677–700 + §10 #46): the doc-comment includes the three sizing tiers — small (≤8 KiB messages: `WithFrameMax(8192)`), streaming (32 KiB–1 MiB messages: `WithFrameMax(131072)`), hard-max (`WithFrameMax(0)` = server-negotiated, currently 128 KiB on RabbitMQ 3.13/4.x) — and the explicit pointer-out that messages >100 MiB should be chunked at the application layer. The `< 4096` rejection (already asserted in T07) is cross-referenced from this godoc.
  - [ ] **`WithHeartbeat` godoc sizing table + zero-warning** (Rev 7, per SPEC §6.1 lines 652–675 + §10 #47): the doc-comment includes the partition-detection guidance (timeout ≈ 2× heartbeat) and three workload tiers — high-throughput / low-latency (`5s` = 10s detection), batch / low-priority (`30s` = 60s detection, default), battery / behind-LB (`60s` = 120s detection). `WithHeartbeat(0)` triggers a `Dial`-time warning log "heartbeats disabled — strongly discouraged: broker partitions become undetectable until the next frame is written" (asserted via a captured log buffer in the unit test).
- **Verify:** Unit tests for each option's effect on the underlying `amqp091.Config`. Smoke integration test asserts `rabbitmqctl list_connections name client_properties` matches: `name` ends with `-pub-N` / `-con-N`, `client_properties` includes the documented keys.
- **Files:** edits to `options_connection.go`, `options_connection_test.go`.
- **Deps:** T07, T07d.

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

### [ ] T34c — Panic isolation for user-provided callbacks · S
Every callback registered by the caller must be wrapped in `recover()` so a panicking
handler cannot crash internal library goroutines. Pure-notification callbacks must also
run in a goroutine to avoid blocking the event-loop that dispatches them.
- **Acceptance:**
  - [ ] **`WithOnBlocked`** — call moved to a dedicated goroutine (`go func() { defer recover; fn(reason) }()`); panic is logged with a stack trace via `logger.Errorf`; the `supervisor` `select` loop is never blocked while the callback executes.
  - [ ] **`WithOnReconnect`** — wrapped in `recover()` at the inline call site in `runBarrier`; on panic, log the stack trace and ensure `barrierCond.Broadcast()` is still emitted (no permanent Publisher deadlock).
  - [ ] **`WithOnTopologyDegraded`** — `recover()` added inside the already-spawned goroutine (`mc.wg.Add(1)` in `runBarrier`); panic is logged; `wg.Done()` is always executed via `defer`.
  - [ ] **`Handler[M]` / `RawHandler[M]`** — invocation extracted into a `safeCallHandler` helper with `recover()`; panic results in `nack(requeue=false)` (prevents infinite poison-message loop) and log of the stack trace; applies to both the inline path (no timeout) and the goroutine path (with timeout).
  - [ ] **`BatchHandler[M]`** — same pattern via `safeCallBatchHandler`; panic results in `nackAll(requeue=false)` + log.
  - [ ] No public API change (no new exported error; recover is transparent to the caller).
  - [ ] Unit tests for each site:
    - `WithOnBlocked`: a panicking callback does not kill the `supervisor` goroutine (verified via goleak + mock conn).
    - `WithOnReconnect`: panicking callback → barrier released, Publishers not deadlocked.
    - `WithOnTopologyDegraded`: panicking callback → `wg.Done()` called, process does not crash.
    - `Handler`: panic → nack without requeue emitted; consumer continues processing subsequent messages.
    - `BatchHandler`: panic → nackAll without requeue; consumer continues.
  - [ ] `goleak.VerifyNone` clean in all tests above.
- **Verify:** `go test -race ./...` green; `go test -race -tags=integration ./...` green (when broker available).
- **Files:** edits to `connection.go` (WithOnBlocked goroutine, WithOnReconnect + WithOnTopologyDegraded recover), `consumer.go` (safeCallHandler), `batch_consumer.go` (safeCallBatchHandler); tests in `connection_panic_test.go`, `consumer_panic_test.go`, `batch_consumer_panic_test.go`.
- **Deps:** T07, T18, T23.

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
- **Verify:** Markdown lints clean.
- **Files:** `README.md`.
- **Deps:** T38.

### [ ] T40 — CHANGELOG + final godoc pass · S
- **Acceptance:**
  - [ ] CHANGELOG.md follows Keep a Changelog with a single Unreleased section.
  - [ ] Every exported identifier has a godoc comment.
- **Verify:** `golangci-lint run --enable=revive` with revive's missing-godoc rule passes.
- **Files:** `CHANGELOG.md`, godoc edits across the tree.
- **Deps:** Phases 1–8.

### [ ] T41 — Coverage gate · S
- **Acceptance:**
  - [ ] ≥ 80% line coverage per package.
  - [ ] ≥ 95% on `internal/reconnect`, `internal/confirms`, `channelpool`, **`internal/amqperror`**, **`internal/redact`** (Rev 7, per SPEC §9 line 2107–2109 — both packages are choke-points for AMQP correctness and credential safety; their coverage is load-bearing for the §9 reliability bar, not optional).
  - [ ] Coverage badge or coverage delta posted in CI.
- **Verify:** `go test -cover ./...` per package.
- **Files:** add test cases as needed; CI workflow assertion.
- **Deps:** Phases 1–8.

### [ ] T42 — CI workflow · S
- **Acceptance:**
  - [ ] `.github/workflows/ci.yml` runs on push/PR: lint, unit, integration, conformance (matrix over Go 1.23 + 1.24).
  - [ ] Concurrency cancellation: PR push cancels in-flight run for the same ref.
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
- **Verify:** `go test -bench=. -benchmem -run=^$ ./...` reaches the gates locally and on the reference CI runner. Bench gate fails the build if a number drops > 20% versus the previous tag.
- **Files:** `bench_publish_test.go`, `bench_publish_batch_test.go`, `bench_consume_test.go`, `bench_multiconn_test.go`, CI workflow `.github/workflows/bench.yml`.
- **Deps:** Phases 1–5, T37.

### [ ] T45 — Reconnect chaos test (scaled up) · S
- **Acceptance:** Integration test: **5-minute outage @ 10k msg/s** with confirms (was 60s @ 1k msg/s), zero loss, `goleak.VerifyNone`. **`WithPublisherConnections(4)` enabled** so the test also exercises the multi-conn fan-out under chaos. Re-subscribe metric (`consumer_resubscribed_total`) and handler-aborted metric (`consumer_handler_aborted_channel_closed_total`) asserted non-zero by the test, demonstrating Rev 5 invariants hold under chaos. Topology re-declared on reconnect; in-flight handlers cancel via ctx with cause `ErrChannelClosed`.
- **Verify:** Test runs in <7 minutes on CI; flaky-rate <1% over 50 runs.
- **Files:** `chaos_reconnect_integration_test.go`.
- **Deps:** Phase 1, T07d, T12, T18, T20.

### [ ] T45b — Security regression scan · S
SPEC §8 + §9 reliability bar: credential leakage is a defect.
- **Acceptance:**
  - [ ] Integration test runs a 60s end-to-end workload against a credentialed URI (`amqp://leak_user:s3cret-pass@host:5672/v`); captures every emitted log line, error string, span attribute snapshot, and Prometheus metric label value into a single buffer.
  - [ ] Test scans the buffer with `regexp.MustCompile("s3cret-pass|leak_user:")` and asserts **zero matches**. A control assertion in the same test verifies the buffer is non-empty (otherwise a no-op test would pass trivially).
  - [ ] Also asserts every captured AMQP URI matches the redacted shape (`amqp[s]?://\*\*\*@`).
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
- **Verify:** Run with `-race -count=100`; a deliberately-buffered return channel variant in the test makes it red. Real-broker variant on the integration lane.
- **Files:** `internal/confirms/tracker_test.go`, `publisher_test.go`, `go.mod`.
- **Deps:** T11, T13. **(R10-3, P0.3)**

### [ ] T60 — Single-delivery double-verdict idempotency guard [P0] · S
- **Acceptance:**
  - [ ] `Delivery[M]` has a resolved-once guard (mirrors `Batch[M]`): the second of any `Ack`/`Nack`/`AckIf`, or a handler-timeout verdict followed by a late handler verdict, is a no-op returning a sentinel (e.g. `ErrAlreadyClosed`-class), never a wire frame.
  - [ ] Channel stays open after the double call.
  - [ ] **Lens-02 (DS-04):** SPEC §6.3 documents the double verdict (incl. a late verdict after `HandlerTimeout`, esp. via `ConsumeRaw`) as a no-op, and states that **pre-fix** it channel-closes (406/`PRECONDITION_FAILED`), taking out *every* in-flight handler on that channel — collateral loss, not just a duplicate.
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
- **Verify:** Integration: nacked-poison lands in a durable bounded DLQ. Unit: consumer `Build` warns + metric increments when no DLX.
- **Files:** `topology.go`, `consumer.go`, `consumer_builder.go`, `metrics/`.
- **Deps:** T15, T47, T18, T30. **(R10-10, P1.3)**

### [ ] T66 — `WithAddrs` shuffle + reconnect rotation [P2] · S
- **Acceptance:**
  - [ ] The address list is shuffled per connection at `Dial`; reconnect rotates to the next address rather than always retrying index 0.
  - [ ] **Lens-02 (DS-10):** SPEC §6.1 notes this compounds with DS-09 (T62) into a recovery storm; the chaos lane asserts no `addr[0]` stampede on a full-cluster restart.
  - [ ] **Lens-05 (SRE-04):** the chaos lane asserts no `addr[0]` stampede on a full-cluster restart (compounds with SRE-06/T62 into a recovery storm).
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

### Checkpoint — v0.1.0 shipped
- [ ] Every SPEC §9 success criterion ticked.
- [ ] `v0.1.0` tag on `main`.
- [ ] README + examples link from the GitHub repo landing page.
- [ ] **Done.**

---

## Quick stats
- Total tasks: **138** (Rev 5: +T07c redaction, +T07d multi-conn, +T34b SASL EXTERNAL, +T44b bench, +T45b security scan; Rev 6: +T18b HandlerTimeoutVerdict matrix, +T38b idempotent_consume example, +T38c ordered_consume example; 2026-05-24: +T34c panic isolation for user-provided callbacks; Phase 10: +T47-T56 SRE Resilience; Phase 11: +T57-T72 Rev 10 AMQP/SRE re-review; 2026-05-28: +T73 codec-call panic-safety recover; Phase 12 (2026-05-28): +T74-T83 Lens-01 protocol-correctness re-review; Phase 13 (2026-05-28): +T84-T93 Lens-02 distributed-systems re-review, pulls T62/T63/T70/T71 forward, adds the `chaos` lane; Phase 14 (2026-05-28): +T94-T100 Lens-03 interoperability/wire-format re-review, adds the `interop` lane + LATER-39; Phase 15 (2026-05-28): +T101-T110 Lens-04 event-driven-architecture re-review, pulls T68/T69 forward, extends T85, adds LATER-40, brings `x-consistent-hash` into scope (no new build-tag lane); Phase 16 (2026-05-28): +T111-T118 Lens-05 SRE/production-operability re-review, pulls T67/T72 forward, extends T61/T62/T63/T65/T66/T70/T71 (cross-lens), no new build-tag lane, no new LATER; Phase 17 (2026-05-28): +T119-T129 Lens-06 Go-API/library-design re-review, fixes the GA-01 DeliveryMode silent-non-persistence Blocker, extends T37/T68/T69/T70/T71/T112 (cross-lens), adds LATER-41, no new build-tag lane).
- Phases: **17**.
- Estimated sizing: 8× XS · 40× S · 23× M · 0× L (none too big).
- Sequential pinch-points: T07c (`internal/redact`) before T03/T04/T07/T07d; T07 (single-TCP Connection with reconnect barrier + degraded state) and T07b/T07c before T07d (multi-conn pool); T07d before everything in §6 of the spec; T15 (Declare) before T31 (delayed); T18 (Consumer + re-subscribe + handler-ctx cancel + HandlerTimeoutVerdict + UUID-tag default) before T18b (verdict matrix test) and T28 (OTel consume); T45 chaos + T45b security gate T46 release; T38b/T38c examples gate T46 release.
- Fuzz targets in v0.1.0: `FuzzCodecJSON` (T09), `FuzzCodecProtobuf` (T24), `FuzzCodecCloudEventsBinary` (T26), `FuzzXDeathParser` (T17), **`FuzzRedactURI` (T07c)**. Others added later as bugs surface.
- Bench gates (T44b): ≥ 30k msg/s single-conn, ≥ 100k msg/s with `WithPublisherConnections(4)+WithChannelPoolSize(16)`, `PublishBatch` ≥ 5× `Publish`.
- Operational decisions: deps pinned exact in `go.mod`; testcontainer images pinned minor-patch; conformance against a live broker (no stub); pre-commit hooks opt-in via `make hooks`; no goreleaser (pure library); OTel Messaging semconv pinned to v1.27.0+; `golangci-lint` includes `errorlint`; `amqptest` plugin enablement supports three explicit modes (pre-baked image / mounted `.ez` / `t.Skip`).
- Reliability invariants (mandatory): credential redaction (T07c, T45b), consumer re-subscribe (T18, T19), handler-ctx cancel on channel close (T18, T19), broker-nack → `ErrPublishNacked` (T11, T13), JSON lax default per Postel's Law + opt-in `NewJSONStrict` (T09, Rev 8), quorum-queue `x-delivery-limit` (T14, T15, T20), **synchronous reconnect barrier + degraded state** (T07, T16, T45), **at-least-once with documented dedupe pattern** (T13, T38b, SPEC §6.2.1), **HandlerTimeout verdict consistency** (T18, T18b), **client-side UserID validation** (T13), **Replier missing-DLX validation** (T30), **SASL EXTERNAL fail-closed** (T34b), **basic.cancel surfacing** (T36), **default conn counts 2/2 with consumer-tag UUID** (T07d, T18), **panic isolation for user-provided callbacks** (T34c).
