# Implementation Plan: amqp.go v0.1.0

> Companion document to `SPEC.md`. The spec defines **what** to build; this
> plan defines **how** and **in what order**. Read the spec first.

---

## Overview

Implement the v1 surface of `github.com/brunomvsouza/amqp` defined in
`SPEC.md` §6, sliced vertically so every phase ends in a state that
compiles, has passing tests, and demonstrates a usable end-to-end path.
First public tag is `v0.1.0`; `v1.0.0` is cut only after every success
criterion in SPEC §9 is checked off.

**Targets:** AMQP 0-9-1 as implemented by **RabbitMQ 3.13 LTS / 4.x**.
The library does not aim to be portable to other AMQP 0-9-1 brokers
(Qpid, ActiveMQ). RabbitMQ extensions surfaced as first-class API are
listed in `SPEC.md` §6 ("Note on AMQP 0-9-1 vs RabbitMQ").

**Revision history:** (Rev 7 → Rev 6 → Rev 5 ordering preserved at the top — newest specialist pass first; Rev 5 sits below Rev 6 because it was the prior milestone the Rev 6 pass superseded.)
- Rev 7 — post cross-check tightening pass (no structural changes). A
  decision-by-decision audit of SPEC §10 (32 decisions: 8 originals +
  24 Rev 6 specialist) against the 54 plan tasks found **zero structural
  gaps** — every public §6 symbol and every §9 success criterion already
  has a producing task. Six acceptance criteria were sharpened on
  tasks that previously relied on the T40 godoc sweep or were silent on
  load-bearing wiring: **T07** (`AuthenticatedUser()` populated by
  `Dial` before it returns, with PLAIN + EXTERNAL unit cases),
  **T13** + **T18** (functional-options last-wins explicitly asserted
  on `PublisherBuilder` and `ConsumerBuilder`), **T16** (`AttachTo`
  snapshots are keyed by the pointer address of the input `*Topology`
  — replace-on-same-pointer, append-on-different-pointer), **T34**
  (`WithFrameMax` and `WithHeartbeat` godoc carry the SPEC §6.1
  sizing-tier tables; `WithHeartbeat(0)` emits a `Dial` warning),
  **T41** (the ≥95% critical-path coverage list extended to include
  `internal/amqperror` and `internal/redact`, per SPEC §9 line 2107–
  2109). Seven other proposals from the audit (T07b godoc, T10
  rabbitmqadmin round-trip, T11 `ErrUnroutable` 312/313 wrap, T12 Build
  snapshot, T14 nine-rule table test, T15 caller-Topology immutability,
  T19 `consumer_handler_timeout_total`, T30 reply-publish-failure
  integration) were verified already covered in `todo.md` and are
  documented here as no-op confirmations rather than re-stated. SPEC.md
  unchanged. Task count unchanged (54). Phase layout unchanged.
- Rev 1 — initial plan: 46 tasks, 9 phases.
- Rev 2 — post AMQP 0-9-1 compliance review (commit on SPEC.md): 47
  tasks. T02 scope grew (reply-code sentinels + AMQPCode + new typed
  enums); new T07b for `internal/amqperror` translation layer; T10,
  T13, T14, T15, T17, T18, T29 acceptance criteria refined.
- Rev 3 — audit pass against SPEC Rev 2 to close two cross-document
  drifts: 47 tasks (unchanged count).
  - **T36 fix:** `NoLocal()` removed from the "remaining consumer
    options" scope. RabbitMQ silently ignores `no-local`, and SPEC §6
    (note block) plus §10 decision 10 explicitly forbid exposing it.
    T36 now ships a symbol-absence guard test so the omission cannot
    regress.
  - **T10 godoc:** explicit acceptance criteria for the SPEC §6.5
    field notes (`Priority` range + clamp, `Expiration` shortstr
    semantics, `Headers` field-table typing, `ContentType` vs
    `ContentEncoding`). Previously delegated to the T40 "every
    exported identifier has godoc" sweep, which is too coarse to
    catch a missing semantic detail.
- Rev 4 — post code-review pass (AMQP 0-9-1 / `amqp091-go`
  conformance). 47 tasks (unchanged count). All fixes are spec /
  documentation only; the dependency graph is unchanged. SPEC §10
  gained six new resolved decisions (#11–#16) recording the
  rationale for each fix.
  - **SPEC §6.6 + T15 (Critical):** `Topology.Declare` rewritten as a
    two-step pipeline. The previous "exchanges → queues → bindings
    → DeadLetter expansion" wording implied that DLX args could be
    added to an already-declared source queue via a second
    `queue.declare` — AMQP 0-9-1 returns `PRECONDITION_FAILED` /406
    in that scenario. The pipeline is now (1) in-memory pre-pass
    that merges DLX args into the source `Queue.Args` and appends
    DLX/DLQ topology entries, followed by (2) broker-side declare
    of exchanges → queues → bindings in one pass. T15 acceptance
    asserts the pre-pass via an in-memory snapshot and the on-wire
    order via a channel recorder.
  - **SPEC §6.3 + T20 (Critical):** `MaxRedeliveries` redesigned as
    two complementary counters. AMQP 0-9-1 only writes `x-death` on
    dead-letter events; `Nack(requeue=true)` does *not* increment it,
    so an `ErrRequeue` loop is invisible to a pure `x-death` ceiling
    and the spec's previous "bounded even when the handler keeps
    wrapping `ErrRequeue`" claim was architecturally impossible.
    Counter A reads `DeathCount()` (bounds DLX-bounce loops, survives
    restarts when a DLX-back-to-source binding exists). Counter B is
    an in-process map keyed by `MessageID` (bounds `ErrRequeue`
    loops within the consumer's lifetime). Both escalate to
    `Nack(false)` + `ErrMaxRedeliveries`. The process-locality of
    counter B is documented as load-bearing in the godoc.
  - **SPEC §6.5 (Important):** `Headers` field-table typing list
    aligned to `amqp091-go`'s actual encoder surface — `Decimal{Scale,
    Value}` added (AMQP type `D`), and native Go `int`/`uint`
    literals now auto-coerce to `int64`/`uint64` so the ergonomic
    `Headers{"count": 5}` works without surprise. The strict "any
    other Go type returns `ErrInvalidMessage`" rule applies to
    everything else.
  - **SPEC §6.2 + T11 + T13 (Important):** `basic.return` /
    `basic.ack` correlation is now an explicit acceptance criterion
    on the confirm tracker (T11). For an unroutable mandatory
    publish, RabbitMQ sends `basic.return` first and then
    `basic.ack` (the broker acks because *it* handled the message
    by returning it). The tracker correlates the two frames by
    `delivery-tag` and resolves `Wait` with `ErrUnroutable` rather
    than success; `OnReturn` fires synchronously before `Publish`
    unblocks. T13 also corrects the `ReturnedProperties` field
    count from 16 (impossible — AMQP `basic.properties` has 12
    standalone fields + `Headers`) to 13.
  - **SPEC §6.6 + T15 (Important):** `NoWait=true` caveat
    documented. With `NoWait=true`, mismatch detection is async
    (surfaces later as a channel close wrapping
    `ErrPreconditionFailed`), so the synchronous
    `ErrTopologyMismatch` guarantee only holds with `NoWait=false`.
  - **SPEC §6.7 + T30 (Important):** `Replier` silent-drop failure
    mode promoted to a load-bearing godoc warning. Without a DLX on
    the request queue, a handler error is a real drop and `OnError`
    is the only client-side signal. T30 ships an explicit negative-
    path integration test asserting this is documented behaviour.
  - **T18 (Important):** `ChannelQoS()` verification redesigned.
    The old plan called for "two consumers on the same channel" to
    assert per-channel prefetch, but the public API enforces one
    channel per `Consumer[M]` (SPEC §6.1) — the test was unwritable
    against the surface. Verification is now wire-level (channel
    recorder asserting `basic.qos.global=true`) plus an optional
    conformance probe that bypasses the public API.
  - **T22 (Important):** `PublishBatch` per-message failure fixture
    switched from "body too large" (which triggers
    `CONTENT_TOO_LARGE` /311 and *closes* the channel, aborting the
    rest of the batch with `ErrChannelClosed`) to client-side
    `Headers` validation failures (caught in T10's `applyDefaults`
    before a frame is written). The channel stays open across the
    batch and the always-all contract holds.
  - **T07b (Suggestion):** carve-out comment for `312 NO_ROUTE` /
    `313 NO_CONSUMERS` clarified — they are `basic.return` reply
    codes, never channel-close codes, and therefore never flow
    through `amqperror.Wrap`. The comment now explains *why* they
    are omitted instead of listing them as exceptions to a rule
    they could not satisfy in the first place.
  - **T17 (Suggestion):** `x-death` parser acceptance now describes
    the header's actual shape — a field-array of field-tables, one
    entry per dead-letter event — and adds a fuzz target for
    malformed inputs.
  - **T13 (Suggestion):** broker-blocked integration test now uses
    `rabbitmqctl set_disk_free_limit 1TB` (clean, restorable) rather
    than `vm_memory_high_watermark=0.000001` (frequently crashes
    the testcontainer).
- Rev 6 — post specialist code-review (AMQP/RabbitMQ specialist
  acting as code reviewer). 54 tasks (51 → 54, +3). SPEC §1 grew
  the "No silent duplicate" reliability-bar bullet; SPEC §6.1 grew
  the synchronous reconnect barrier + degraded-state contract;
  SPEC §6.2.1 (new subsection) documents at-least-once semantics
  and the canonical consumer-side dedupe pattern; SPEC §6.6 grew
  validation rules; SPEC §10 grew decisions #25–#48 (24 new). The
  Rev 6 changes are all targeted at the gap between "works under
  normal load" and "trusted by a non-specialist for billions of
  messages a day". Summary of changes:
  - **PublishRetry duplicate contract (Critical).** SPEC §6.2 +
    §6.2.1 + §1 spell out that `PublishRetry` is at-least-once by
    design and consumers MUST dedupe. New `publisher_retry_total`
    mandatory metric. T13 acceptance now requires the metric and
    the godoc warning verbatim. New T38b ships
    `examples/idempotent_consume/` as the canonical reference.
  - **HandlerTimeout default verdict consistency (Critical).** SPEC
    §6.3 + §6.4 align on `TimeoutNackNoRequeue` as default with the
    new `HandlerTimeoutVerdict(TimeoutVerdict)` builder option.
    The Rev 5 contradiction (SPEC §6.4 `Nack(false)` vs TODO T18
    `Nack(true)`) is closed; T18 acceptance is corrected and a
    new T18b ships the `HandlerTimeoutVerdict` test matrix.
  - **Reconnect synchronisation barrier (Critical).** SPEC §6.1
    documents the synchronous reconnect barrier (re-open channel →
    redeclare topology → re-`basic.consume` → fire
    `WithOnReconnect`). New error `ErrReconnecting` (transient);
    new mandatory metric `topology_redeclare_seconds{role}`. T07
    + T07d + T16 acceptance grow the barrier-aware behaviour.
    T45 chaos test asserts no 404 closes during reconnect.
  - **Topology degraded state (Critical).** SPEC §6.1 + §6.6
    introduce the degraded state on persistent redeclare failure:
    new error `ErrTopologyRedeclareFailed` (permanent), new
    `WithOnTopologyDegraded(func(error))` option, new mandatory
    metric `connection_degraded_total{role, reason}`,
    `(*Connection).ForceReconnect()` exposed for operators. T16
    acceptance asserts degraded-state transitions and recovery.
  - **ConfirmTimeout default = 30 s (Critical).** SPEC §6.2 sets a
    non-zero default so `context.Background()` callers never stall.
    T13 acceptance updated with a mock-channel test that asserts
    the 30 s default deterministically.
  - **Concurrency vs ordering (Critical, doc-only).** SPEC §6.3
    documents the trade-off explicitly. New T38c ships
    `examples/ordered_consume/` (SAC + Concurrency(1)).
  - **`PublishBatchMaxInFlight` → `PublishBatchMaxSize` (Important,
    rename).** Reflects the actual per-call semantics. T13/T22
    acceptance carry the new name; godoc on the renamed method
    clarifies "not a sliding in-flight window".
  - **`Prefetch < Concurrency` warning + sizing formula
    (Important).** T18 acceptance adds the `Build`-time warning.
    Godoc references the throughput formula
    `throughput ≈ Concurrency / handler_latency`.
  - **Replier missing-DLX validation + metric (Important).** SPEC
    §6.7 grows `ReplierBuilder.Topology(t)` validation +
    `AllowMissingDLX()` escape hatch + mandatory
    `replier_drop_no_dlx_total` metric. T30 acceptance grows the
    validation test and the metric assertion.
  - **`x-death` parser reason filter (Important).** SPEC §6.3:
    `DeathCount()` filters to `reason ∈ {rejected, delivery-limit}`;
    new `DeathCountByReason(r)` and `DeathReasons()` methods. T17
    acceptance adds reason-discrimination fixtures.
  - **SASL EXTERNAL fail-closed validation (Important).** SPEC §6.1
    grows the three-fold check (TLS present, cert present, amqps
    scheme). T34b acceptance asserts each rejection path returns
    `ErrInvalidOptions`.
  - **Default conn counts 2/2 (Important).** SPEC §6.1: default
    `WithPublisherConnections=2`, `WithConsumerConnections=2`;
    `n=1` logs warning at `Dial`. T07d acceptance updated.
  - **Consumer-tag default UUID (Important).** SPEC §6.1: empty
    `Tag(string)` becomes `ctag-<uuidv7>` at `Build` so multi-conn
    hashing actually fans out. T18 acceptance asserts distinct
    tags + distinct pin connections.
  - **`AMQPCode` covers 312/313 (Important).** SPEC §6.8 + §6.2:
    confirm tracker tags `ErrUnroutable` with the originating
    `basic.return` code. T07b acceptance updated; T13 returns-test
    asserts `AMQPCode(err)` returns the right code.
  - **`UserID` client-side validation (Important).** SPEC §6.5 +
    §6.2: `Publish` rejects `Message[M].UserID` divergent from the
    authenticated user with `ErrInvalidMessage` locally;
    `PublisherBuilder.StampUserID()` opt-in auto-populates. T12
    acceptance adds the validation test; T13 adds StampUserID
    test.
  - **`amqptest` plugin enablement (Important).** SPEC §6.9 + plan
    Risks: three explicit modes (pre-baked image, mounted `.ez`,
    `t.Skip`). T37 acceptance grows the three-mode helper +
    `amqptest.RequireDelayedExchange(t)` skip helper.
  - **`AttachTo` deep snapshot semantics (Important).** SPEC §6.6
    documents the snapshot capture; mutations post-AttachTo are
    invisible. T16 acceptance asserts via a recorder.
  - **Topology validation strengthened (Important).** SPEC §6.6
    lists nine new validate-rejected combinations (SAC on stream,
    stream + Exclusive/AutoDelete, fanout binding with routing
    key, delayed exchange without `x-delayed-type`, duplicate
    names, etc.). T14 acceptance covers each.
  - **`PublishBatch` channel-close recovery doc (Important).** SPEC
    §6.2 documents that per-message `ErrChannelClosed` cannot
    distinguish persisted vs lost; retry produces duplicates;
    `PublishRetry` does NOT apply to `PublishBatch`. T22 acceptance
    adds the chaos test asserting the documented behaviour.
  - **Options last-wins (Important, doc-only).** SPEC §6.1.
  - **`basic.cancel` surfaces as `ErrConsumerCancelled` (Important).**
    SPEC §6.3: `Consume` returns `ErrConsumerCancelled`; mandatory
    metric `consumer_cancelled_total{queue, reason}`. T36 acceptance
    grows the cancel test to assert the metric + error.
  - **Frame max + heartbeat sizing docs (Suggestion).** SPEC §6.1.
  - **`errorlint` linter (Suggestion).** T01 acceptance adds
    `errorlint` to `.golangci.yml`.
  - **`internal/redact` fuzz target (Suggestion).** T07c acceptance
    adds `FuzzRedactURI`. Listed in plan §"Fuzz targets".
  - **3 new tasks:** T18b (HandlerTimeoutVerdict matrix), T38b
    (`examples/idempotent_consume/`), T38c (`examples/ordered_consume/`).
    Total: 54 tasks.

- Rev 5 — billions/day reliability bar. 51 tasks (47 → 51, +4). SPEC
  §1 grew a "Reliability bar" subsection; SPEC §10 grew decisions
  #17–#24. All fixes target the gap between "works in production"
  and "trusted in flight paths handling billions of messages/day".
  Summary of changes:
  - **Connection multi-conn fan-out (Critical).** SPEC §6.1 rewritten
    to expose a pool of TCP connections split by role
    (`WithPublisherConnections(n)`, `WithConsumerConnections(n)`).
    `amqp091-go` serializes I/O per connection; a single TCP socket
    bottlenecks sustained > ~50k msg/s. New T07d implements the
    multi-conn pool. T07 reduced to single-conn supervisor; T08
    becomes "channel pool per publisher connection". T34 grows the
    `WithPublisherConnections`/`WithConsumerConnections`/`WithConnectionName`
    options. T45 chaos test scaled up to 5min @ 10k msg/s with
    multi-conn fan-out; new T44b throughput bench gates ≥30k/s
    single-conn and ≥100k/s multi-conn.
  - **Quorum-queue `x-delivery-limit` (Critical).** SPEC §6.3 +
    §6.6 promote `x-delivery-limit` to the preferred poison bound
    on quorum queues; the consumer-side counter B is fallback for
    classic queues. T14 grows `Queue.DeliveryLimit` +
    `Queue.SingleActiveConsumer`; T15 expansion now also injects
    `x-delivery-limit` / `x-single-active-consumer` / `x-queue-type`
    (mutates a copy, not the input). T20 acceptance distinguishes
    `cause=delivery-limit|x-death|in-process` in metrics and adds a
    quorum-queue path.
  - **`ErrPublishNacked` (Critical).** SPEC §6.2 + §6.8 add the
    sentinel for broker-side nack of publishes (overflow
    `reject-publish`, mid-publish disk alarm). T11 confirm tracker
    gains the nack-resolution branch; T13 ships the integration
    test forcing `x-overflow=reject-publish` + `x-max-length=0`.
  - **Consumer re-subscribe + handler ctx cancel (Critical).** SPEC
    §6.1 + §6.3 + §8 make consumer re-subscribe and handler-ctx
    cancellation on channel close part of the contract. T18 + T19
    grow the loop and the mandatory metrics
    (`consumer_resubscribed_total`,
    `consumer_handler_aborted_channel_closed_total`). T45 chaos test
    asserts both.
  - **SASL EXTERNAL + credential redaction (Critical).** SPEC §6.1
    adds `WithSASLMechanism(SASLMechanism)`; SPEC §8 mandates URI
    redaction in logs/metrics/spans/errors. New T07c builds
    `internal/redact`; new T34b ships the EXTERNAL integration test
    against a broker with `rabbitmq_auth_mechanism_ssl`. T32 (TLS)
    re-uses the certs from T07c/T34b.
  - **Quorum queue + SingleActiveConsumer + Priority (Important).**
    T14/T15 cover the queue-level features; T18 adds
    `ConsumerBuilder.Priority(int)` for `x-priority` on
    `basic.consume`.
  - **HandlerTimeout, PublishTimeout, PublishBatch.MaxInFlight,
    PublishRetry (Important).** T13 grows
    `PublishTimeout`/`PublishBatchMaxInFlight`/`PublishRetry`;
    `PublisherBuilder.RetryPolicy()` renamed to `PublishRetry()`
    (only retries `ErrTransient`-classified errors). T18 adds
    `ConsumerBuilder.HandlerTimeout(d)`. T22 honours
    `PublishBatchMaxInFlight` and returns `ErrBatchTooLarge`.
  - **JSON strict default + codec panic safety (Important).** T09
    flips `codec.NewJSON()` to strict; adds `codec.NewJSONLax()`.
    Publisher/Consumer wrap every codec call in `recover` →
    `ErrInvalidMessage`.
  - **BatchHandler / Replier ordering (Important).** T23 documents
    auto-Ack/Nack with `multiple=true` for batch verdicts. T30
    documents at-least-once Replier ordering (publish reply, await
    confirm, then ack request).
  - **OTel pin + Prometheus cardinality + mandatory metrics
    (Important).** T05 pins OTel Messaging semantic conventions to
    v1.27.0+. T04 / T19 / metrics chapter document bounded labels,
    `WithMetricsLabels(...)` opt-ins, and histogram buckets.
  - **`ErrChannelPoolExhausted` + counter-B key fix + `Connection.Logger`
    removal + `Topology.Declare` concurrency note + `IsTransient(506)`
    moved to permanent (Important).** T02, T07, T08, T15, T17 acceptance
    criteria refined; counter B key in T20 is now
    `(channel-instance-id, MessageID)` with channel-instance-id reset
    on channel close.
  - **Streams scoped to v0.2 (decision).** T14 keeps `QueueTypeStream`
    constant; native stream consume removed from v0.1 scope, tracked
    in plan §"Out of scope".

## Architecture decisions (recap)

These are inherited from `SPEC.md` and **closed**. Anything that
contradicts them needs the spec amended first. The full list with
rationale lives in SPEC §10; this is the quick-reference.

1. Functional options non-generic; generic builders `XFor[M]`.
2. Topology declared separately from publishers/consumers.
3. No magic sleeps; backpressure is `prefetch_count`.
4. NACK without requeue is the default for handler errors.
5. Handler signature is `func(ctx, M) error`; raw escape hatch.
6. `Message[M]` is a struct literal, not a builder.
7. `PublishBatch` always-all + `[]PublishResult`.
8. Mocks live in `amqpmock/`; testcontainers helper in `amqptest/`.
9. `Delivery[M]` / `Batch[M]` are concrete structs.
10. `Connection` fans out across multiple TCP connections, role-split
    (`WithPublisherConnections`, `WithConsumerConnections`).
11. Quorum-queue `x-delivery-limit` is the preferred poison bound;
    consumer-side counter B is the classic-queue fallback.
12. SASL EXTERNAL is supported alongside PLAIN. Credentials are
    redacted from logs/metrics/spans/errors.
13. JSON codec is **strict by default** (`DisallowUnknownFields`);
    codec calls are wrapped in `recover` → `ErrInvalidMessage`.
14. Consumers automatically re-issue `basic.consume` after reconnect;
    handler `ctx` cancels with cause `ErrChannelClosed` on mid-handler
    channel close.
15. Broker `basic.nack` on publishes maps to `ErrPublishNacked`.
16. Streams: `QueueTypeStream` for declaration only in v0.1; native
    stream consume is v0.2.
17. **At-least-once is the contract.** `PublishRetry` and reconnect
    can produce duplicates; consumers MUST dedupe by `MessageID`
    (auto-populated UUIDv7). `publisher_retry_total` makes retries
    observable.
18. **Reconnect is a synchronous barrier:** redeclare → re-subscribe
    → `WithOnReconnect`. Publish blocks on `ErrReconnecting` until
    barrier clears. Persistent redeclare failure → degraded state +
    `ErrTopologyRedeclareFailed`.
19. **`ConfirmTimeout` default 30 s; `HandlerTimeoutVerdict` default
    `TimeoutNackNoRequeue`.** Both align with the "no silent stall /
    no silent poison loop" north stars.
20. **Default `WithPublisherConnections=2` and
    `WithConsumerConnections=2`.** `n=1` is a `Dial` warning.
    Consumer tag default is `ctag-<uuidv7>` so multi-conn hashing
    actually fans out.

---

## Dependency graph

```
                          errors.go + types.go
                                    │
   ┌──────────┬──────────┬──────────┴──────────┬──────────┬──────────────┬──────────────┐
   │          │          │                     │          │              │              │
 log/     metrics/     otel/                codec/   internal/      internal/       internal/
 (Logger) (interfaces) (Tracer+nop)       (Codec    amqperror      redact          headers
                                            iface)  (reply codes)  (URI redaction) (x-death, otel)
   │          │          │                     │          │              │              │
   └────┬─────┴──────┬───┘                     │          │              │              │
        │            │                         │          │              │              │
  options_conn   internal/reconnect            │          │              │              │
        │            │                         │          │              │              │
        └─────┬──────┘                         │          │              │              │
              │                                │          │              │              │
        per-conn pool ── internal/confirms     │          │              │              │
        (channelpool)         │                │          │              │              │
              │               │                │          │              │              │
              └───── Connection (multi-TCP) ───┴──────────┴──────────────┴──── uses ────┘
                              │
                    ┌─────────┼──────────┐
                    │         │          │
                 Topology  Publisher  Consumer
                              │          │
                              │       Delivery[M]
                              │          │
                        PublishBatch  BatchConsumer
                              │          │
                              └────┬─────┘
                                   │
                                RPC, Delayed
                                   │
                                   └─── examples
                                              │
                                          amqpmock + amqptest
```

`internal/amqperror` translates `*amqp091.Error` into wraps of the
reply-code sentinels in `errors.go` (`ErrAccessRefused`, `ErrNotFound`,
`ErrPreconditionFailed`, etc.) and feeds `IsTransient`/`IsPermanent`
classifiers. Every component that talks to the broker uses it.

`internal/redact` strips `userinfo` from AMQP URIs in any string the
library hands to logs, metric labels, span attributes, or error
messages. Mandatory choke-point for the SPEC §8 "Always: redact
credentials" rule.

`Connection` now wraps a pool of TCP connections, role-split into
publisher and consumer sockets. Each TCP connection has its own
`internal/reconnect` supervisor; each publisher TCP connection has
its own `channelpool` of size `WithChannelPoolSize`. Consumers are
pinned to a consumer TCP connection by hash of their consumer-tag.

`amqptest` is the public testcontainers helper; pre-generated TLS
certs and the `rabbitmq_delayed_message_exchange` plugin enabling
live here. Downstream applications import it in their own
integration suites.

Implementation order follows the graph bottom-up, but every phase
delivers a complete vertical slice (you can publish/consume something
real after each phase).

---

## Phases

### Phase 1 — Foundation: a Connection that survives outages

Goal: a `Connection` you can `Dial`, `Health`-check, and `Close`; it
auto-reconnects with backoff; emits metrics, logs, and traces (no-op
defaults); fans out across multiple TCP connections by role.

- **T01** Repo bootstrap (`go.mod`, `LICENSE`, `Makefile`, `.golangci.yml` enabling `errcheck`, `govet`, `staticcheck`, `gosec`, `revive`, `gocritic`, `unparam`, `bodyclose`, `nilerr`, **`errorlint`**, `.gitignore`)
- **T02** `errors.go` (sentinels: lifecycle + publisher + consumer + 15 AMQP reply-code sentinels + `ErrPublishNacked` + `ErrChannelPoolExhausted` + `ErrBatchTooLarge` + `AMQPCode` helper + `IsTransient`/`IsPermanent`) and `types.go` (`Headers`, `DeliveryMode`, `ExchangeKind`, `OverflowPolicy`, `QueueType`, `SASLMechanism`)
- **T03** `log/` package: `Logger` interface + `NoOp`, `Slog`, `Std` adapters (routed through `internal/redact`)
- **T04** `metrics/` package: three interfaces + `NoOp` + Prometheus with bounded default labels, opt-in `WithMetricsLabels`, configurable latency buckets, mandatory metrics (`connection_reconnects_total`, `consumer_resubscribed_total`, `consumer_handler_aborted_channel_closed_total`, `publisher_in_flight`, `connection_blocked_seconds`)
- **T05** `otel/` package: `Tracer` interface + NoOp default + AMQP header propagator following OTel Messaging semantic conventions **v1.27.0+**
- **T06** `internal/reconnect` supervised reconnect loop + `RetryPolicy` (exponential + jitter)
- **T07** `connection.go`: single-TCP `Dial` + `Health` + `Close(ctx)` with drain; validates `ChannelPoolSize ≤ channel-max`; blocked-connection semantics; **synchronous reconnect barrier** (channel re-open → topology redeclare → consumer re-`basic.consume` → user `WithOnReconnect`) with `Publish` blocking on `ErrReconnecting` until step 2 completes; **degraded-state machine** on persistent redeclare failure (`ErrTopologyRedeclareFailed`, `WithOnTopologyDegraded`, `connection_degraded_total`, `(*Connection).ForceReconnect()` operator helper, `topology_redeclare_seconds{role}` histogram); **`Connection.Logger()` removed** from public API; **`(*Connection).AuthenticatedUser()` populated by `Dial` before it returns** and exposed for `UserID` client-side validation; **`Dial` validates SASL EXTERNAL** fail-closed (TLS + client cert + `amqps://`); **`Dial`-time warning** when `WithPublisherConnections(1)` or `WithConsumerConnections(1)`; **`frame_max < 4096` rejected** with `ErrInvalidOptions`
- **T07b** `internal/amqperror`: translates `*amqp091.Error` → wraps of reply-code sentinels; powers `AMQPCode` and the classifiers. **`IsTransient(506)` returns false** (resource-error classified as permanent by default).
- **T07c** `internal/redact`: AMQP URI userinfo redaction; mandatory choke-point used by `log/`, error formatting, metric labels, and span attributes; ships a `FuzzRedactURI` fuzz target exercising malformed URI inputs (added to plan §"Fuzz targets")
- **T07d** Multi-TCP fan-out: extend `Connection` to wrap a role-split pool of TCP connections (`WithPublisherConnections(n)` default **2**, `WithConsumerConnections(n)` default **2**); each TCP connection gets its own `internal/reconnect` supervisor; consumer pinning by stable hash of consumer-tag with **`ctag-<uuidv7>` auto-generation** for defaulted tags (so multi-conn fan-out works out-of-the-box); `Dial` warns when either count is 1
- **T08** `channelpool.go`: per-publisher-TCP-connection channel pool with `ErrChannelPoolExhausted` sentinel for ctx-cancel under exhaustion

**Checkpoint Phase 1:**
- [ ] `go build ./...` clean.
- [ ] `go test -race -tags=integration ./...` passes.
- [ ] `goleak.VerifyNone` clean after **1000** connect/disconnect cycles
      (`-count=1000` chaos micro-test; was 100).
- [ ] Tracing/logging/metrics code paths run uniformly (no `if nil`
      branches).
- [ ] Forcing a broker-side channel close with code 404 surfaces as
      `errors.Is(err, ErrNotFound)` and `AMQPCode(err) == (404, true)`.
- [ ] `Dial` with `ChannelPoolSize` > broker-negotiated `channel-max`
      returns `ErrInvalidOptions`.
- [ ] `WithPublisherConnections(4)` opens 4 TCP sockets named
      `<base>-pub-0..3` (verifiable via `rabbitmqctl list_connections name`).
- [ ] No log line emitted during the phase-1 integration run contains
      a clear-text password (grep test against the recorded output).

---

### Phase 2 — Producer: synchronous-with-confirm publish

Goal: a `Publisher[OrderPlaced]` that publishes one message at a time,
synchronously, with broker confirms; mandatory + returns + broker
nacks wired; emits Prometheus metrics and OTel spans; concurrency-safe.

- **T09** `codec/` package: `Codec` interface + `codec.NewJSON()` (**strict by default** — `DisallowUnknownFields`) + `codec.NewJSONLax()` opt-in + round-trip tests + fuzz target; Publisher/Consumer wrap codec calls in `defer recover` → `ErrInvalidMessage`
- **T10** `message.go`: `Message[M]` struct with `ContentType` + `ContentEncoding` separated; default-apply logic (UUID v7, timestamp, persistent, ContentType ← codec.ContentType); Headers field-table typing validation; field godoc covering `Priority` range/clamp, `Expiration` wire format, `Headers` typing per SPEC §6.5
- **T11** `internal/confirms`: publisher-confirm tracker handling ack, nack (→ `ErrPublishNacked`), return-then-ack (→ `ErrUnroutable` **wrapped with the originating `basic.return` reply code 312/313** so `AMQPCode(err)` returns it), and channel-close (→ `ErrChannelClosed`); per-channel tracker; `multiple=true` semantics
- **T12** `publisher.go` + `publisher_builder.go`: `PublisherFor[M]` builder (no `Immediate()` — RabbitMQ rejects it) + `Publish` (sync-confirm, concurrency-safe, channel acquired from publisher pool) + `Close(ctx)` drain
- **T13** Mandatory + `OnReturn(Return)` with rich `ReturnedProperties` + `ConfirmTimeout` **(default 30 s)** + `PublishTimeout` (end-to-end cap including pool acquire / blocked-wait / reconnect barrier) + **`PublishBatchMaxSize`** (renamed from `PublishBatchMaxInFlight`) + `PublishRetry(p)` (retries only `IsTransient` errors, **emits `publisher_retry_total{exchange, reason}`** on each retry) + `StampUserID()` builder + **client-side `UserID` validation** in `Publish` → `ErrInvalidMessage` on divergence + **functional-options last-wins** asserted on `PublisherBuilder` (per SPEC §6.1 line 515; covers `.Metrics`/`.WithoutMetrics`/`.Tracer` chains) + `ErrUnroutable` + `ErrConfirmTimeout` + `ErrPublishNacked` + `ErrConnectionBlocked` on ctx-cancel while broker-blocked + `ErrReconnecting` on ctx-cancel while reconnect barrier holds

**Checkpoint Phase 2:**
- [ ] Publish a typed message end-to-end against a testcontainer
      RabbitMQ; assert via `rabbitmqadmin get` that body + properties
      match.
- [ ] Mandatory publish to nowhere returns `ErrUnroutable` and fires
      `OnReturn`.
- [ ] Publish with timeout shorter than broker latency returns
      `ErrConfirmTimeout`; no leak.
- [ ] Publish against a queue with `x-overflow=reject-publish` +
      `x-max-length=0` returns `errors.Is(err, ErrPublishNacked)` and
      `IsTransient(err) == true`.
- [ ] Concurrent `Publish` from N goroutines on a single `Publisher[M]`
      with `Concurrency` workers: zero data races (`go test -race`),
      `goleak.VerifyNone` clean.

---

### Phase 3 — Topology: declared once, separately

Goal: declare exchanges/queues/bindings/DLX from one place;
re-declare automatically on reconnect.

- **T14** `topology.go`: `Topology` + `Exchange`/`Queue`/`Binding` (all with `NoWait`) + `Queue.Type` (`QueueType`) + `DeadLetter` (with `MaxLengthBytes` + `Overflow`) + `OverflowPolicy` constants
- **T15** `Topology.Declare(ctx, conn)`: **two-step pipeline** — in-memory DLX expansion (merges args into source `Queue.Args`, appends DLX exchange + `<Source>.dlq` queue) **before** any broker call; then broker-side declare in order exchanges → queues → bindings. Idempotent; `ErrTopologyMismatch` (wrapping `ErrPreconditionFailed`) on conflict; `NoWait=true` downgrades mismatch detection to async (documented)
- **T16** `Topology.AttachTo(conn)` registers a **deep snapshot keyed by the pointer address of the input `*Topology`** (Rev 7) as a reconnect hook: `AttachTo` with the same pointer replaces the prior snapshot for that key; `AttachTo` with a different pointer appends an additional snapshot — both fire on every reconnect in registration order; redeclare runs **inside the synchronous reconnect barrier** (§6.1) before publishers resume / consumers re-`basic.consume`; persistent redeclare failure → **degraded state** with `ErrTopologyRedeclareFailed`, mandatory metric `connection_degraded_total{role, reason}`, `WithOnTopologyDegraded(func(error))` callback; `topology_redeclare_seconds{role}` histogram records the barrier duration

**Checkpoint Phase 3:**
- [ ] Declare same topology twice → no error, no state change.
- [ ] Declare conflicting `Durable` flag → `ErrTopologyMismatch`,
      `errors.Is(err, ErrPreconditionFailed)` is also true.
- [ ] Kill broker, restart, `AttachTo` redeclares → assert via
      `rabbitmqctl list_queues` that queue exists with right args.
- [ ] DLX helper expands to `x-dead-letter-exchange`,
      `x-dead-letter-routing-key`, `x-message-ttl`, `x-max-length`,
      `x-max-length-bytes`, `x-overflow`.
- [ ] `QueueTypeQuorum` declares a queue with `x-queue-type=quorum`
      visible via `rabbitmqctl list_queues name type`.

---

### Phase 4 — Consumer: error-driven semantics + escape hatch

Goal: `Consumer[OrderPlaced].Consume` with a payload-first handler;
`nil`/error/`ErrRequeue`/`ErrPoison` map to the right ack/nack;
`ConsumeRaw` for envelope access; `MaxRedeliveries` shields against
infinite loops; consumer survives reconnects (re-issues
`basic.consume`); handler ctx cancels on channel close.

- **T17** `delivery.go`: concrete `Delivery[M]` struct + `x-death` header parsing + `DeathCount()` (sums entries with `reason ∈ {rejected, delivery-limit}` — `expired` and `maxlen` reflect broker policy, not handler-driven rejection) + new `DeathCountByReason(r string) int` + `DeathReasons() []string`; `Ack`/`Nack`/`AckIf` return `ErrChannelClosed`/`ErrAlreadyClosed` paths documented
- **T18** `consumer.go` + `consumer_builder.go`: `ConsumerFor[M]` builder (no `NoLocal()`; `ChannelQoS()` instead of `GlobalQoS()`; `PrefetchBytes()` documented as no-op on RabbitMQ; new `Priority(int)` for `x-priority`; new `HandlerTimeout(d)` for per-message ctx deadline; new `HandlerTimeoutVerdict(TimeoutVerdict)` default `TimeoutNackNoRequeue`; **functional-options last-wins** asserted on the builder per SPEC §6.1 line 515) + `Consume` + handler error mapping + **re-subscribe loop** that, after a successful reconnect of the consumer TCP connection, reopens the channel, reapplies `basic.qos`, and reissues `basic.consume` exactly once per active consumer; **handler ctx cancel** with cause `ErrChannelClosed` on mid-handler channel close; **default consumer-tag = `ctag-<uuidv7>`** generated at `Build` for multi-conn fan-out; **`Build`-time warning** when `Prefetch < Concurrency`
- **T18b** `HandlerTimeoutVerdict` matrix test: configure `HandlerTimeout(50ms)` with each of the two verdicts; assert (1) `TimeoutNackNoRequeue` lands the timed-out message in DLX; (2) `TimeoutNackRequeue` requeues subject to `MaxRedeliveries` / `x-delivery-limit`
- **T19** `ConsumerMetrics` interface + Prometheus impl + wired into `Consume`; mandatory metrics include `consumer_resubscribed_total`, `consumer_handler_aborted_channel_closed_total`, `consumer_handler_seconds` (histogram)
- **T20** `MaxRedeliveries` enforcement: **two-counter design** with quorum-queue carve-out. (A) `x-death`-based ceiling for DLX-bounce loops (cross-process); (B) in-process counter keyed by **`(channel-instance-id, MessageID)`** for `ErrRequeue` loops (process-local, resets on consumer restart and on channel close). Either ceiling escalates to `Nack(false)` + `ErrMaxRedeliveries`. When the source queue is `QueueTypeQuorum` with `DeliveryLimit>0`, counter B is auto-disabled (broker is authoritative); metric label `cause=delivery-limit|x-death|in-process` distinguishes the three paths. Required because AMQP 0-9-1 only writes `x-death` on dead-letter events, not on `Nack(requeue=true)`
- **T21** `ConsumeRaw(ctx, RawHandler[M])` + `Delivery.AckIf(err)`

**Checkpoint Phase 4:**
- [ ] Handler returning `nil` ⇒ Ack; `errors.New("bad")` ⇒
      `Nack(false)`; wrapped `ErrRequeue` ⇒ `Nack(true)`.
- [ ] `ChannelQoS()` applied: prefetch counted per channel, verified
      at wire level by a channel recorder asserting `basic.qos.global=true`.
- [ ] Poison-loop test (classic queue): handler that always errors
      causes at most 1 delivery (default) or `MaxRedeliveries+1` (when set).
- [ ] Poison-loop test (quorum queue + `DeliveryLimit=5`): broker
      dead-letters at most after 6 deliveries; counter B does not fire
      (`cause=delivery-limit` in metric label).
- [ ] `ConsumeRaw` sees `Redelivered`, `Headers`, `DeathCount`.
- [ ] Concurrency=8 confirmed by goroutine sampling.
- [ ] Forced reconnect during steady-state consume: every active
      consumer re-issues `basic.consume`; `consumer_resubscribed_total`
      increments exactly once per consumer; no goroutine leak.
- [ ] Forced channel close mid-handler: handler's `ctx` is cancelled
      with cause `ErrChannelClosed`; `consumer_handler_aborted_channel_closed_total`
      increments; broker redelivers the message on the new channel.
- [ ] `HandlerTimeout(50ms)` with a 200ms handler: handler ctx is
      cancelled at 50ms, default verdict is `Nack(false)`, message
      goes to DLX.

---

### Phase 5 — Batch APIs: throughput

Goal: `Publisher.PublishBatch` always-all semantics with explicit
in-flight cap; `BatchConsumer` with size + flush-after timer and
documented Ack/Nack-with-`multiple=true` semantics.

- **T22** `Publisher.PublishBatch(ctx, []Message[M]) ([]PublishResult, error)` + `ErrPartialBatch` + **`ErrBatchTooLarge`** when the call exceeds `PublishBatchMaxSize` (default 1024). All N publishes pipeline on a **single channel** to preserve per-channel input order. Per-message failure fixture uses **client-side `ErrInvalidMessage`** (unsupported `Headers` type) so the channel stays open across the batch — body-too-large /311 would close the channel mid-batch and corrupt the always-all contract. **Documented contract:** `PublishRetry` does NOT apply to `PublishBatch`; per-message `ErrChannelClosed` cannot distinguish "broker persisted" from "broker did not receive", so retry is the caller's problem (chunking + dedupe-by-`MessageID`)
- **T23** `batch_consumer.go` + `batch_consumer_builder.go` + concrete `Batch[M]` struct + auto-Ack/Nack with `multiple=true` on the highest delivery-tag in the batch; handler-explicit acking via `Batch.Deliveries()` suppresses the auto-verdict (idempotent guard inside `Batch`); `HandlerTimeout(d)` applies to the whole batch with default verdict `Nack(false)` on timeout

**Checkpoint Phase 5:**
- [ ] `PublishBatch` of 1000 JSON messages: zero loss, single
      confirm-window round-trip.
- [ ] `PublishBatch` of 2000 messages with default `PublishBatchMaxSize=1024`:
      returns `ErrBatchTooLarge` immediately, empty result slice; no
      channel work performed.
- [ ] `PublishBatch` preserves input order: deliveries on the consumer
      side arrive in the same order they were published (single-channel
      guarantee).
- [ ] `BatchConsumer` flushes on `Size(N)` reached.
- [ ] `BatchConsumer` flushes on `FlushAfter(d)` even if size <N.
- [ ] `BatchConsumer` handler returning nil: single `basic.ack` frame
      with `multiple=true` for the highest delivery-tag (verified via
      channel recorder).
- [ ] Per-message benchmark report: `Publish` baseline vs
      `PublishBatch` throughput (must be at least 5× faster on local
      broker).

---

### Phase 6 — Codecs + Observability beyond JSON

Goal: Protobuf + CloudEvents (both modes) codecs; OTel spans across
publish→consume with header propagation.

- **T24** `codec/protobuf.go` + round-trip tests against a representative `.proto`
- **T25** `codec/cloudevents.go` structured mode (`application/cloudevents+json`)
- **T26** `codec/cloudevents.go` binary mode (`ce-*` AMQP headers)
- **T27** OTel integration in `Publisher` (publish span + inject context into AMQP headers)
- **T28** OTel integration in `Consumer` (extract span context from headers + handler span)

**Checkpoint Phase 6:**
- [ ] Protobuf round-trip: encode → publish → consume → decode →
      identical message.
- [ ] CloudEvents structured: body is full envelope; content-type is
      `application/cloudevents+json`.
- [ ] CloudEvents binary: body is `data` only; `ce-id`, `ce-source`,
      `ce-type`, `ce-specversion` present as AMQP headers.
- [ ] Span continuity: trace-id and parent-span-id consistent from
      publisher → consumer.

---

### Phase 7 — Advanced patterns: RPC + delayed messages

Goal: request/reply via `direct reply-to`; delayed publish via
`x-delayed-message` exchange plugin.

- **T29** `rpc.go`: `Caller[Req,Resp]` + `Call(ctx, req)` + `ErrCallTimeout` (direct reply-to: caller auto-enables no-ack, declares reply consumer before publishing) + `UseExclusiveReplyQueue()` fallback builder method
- **T30** `rpc.go`: `Replier[Req,Resp]` + `Serve(ctx, handler)` + `OnError(func)` for error replies + **`ReplierBuilder.Topology(t)` auto-validates DLX presence on the request queue** (returns `ErrInvalidOptions` unless `AllowMissingDLX()` opts in) + mandatory metric **`replier_drop_no_dlx_total{queue}`** so the silent-drop failure mode is always observable. **At-least-once reply ordering**: handler runs → reply published → reply confirm awaited → request acked. If reply publish fails (`ErrPublishNacked`/`ErrConfirmTimeout`/`ErrChannelClosed`), request is `Nack(false)` so it goes to the request queue's DLX if configured. Crash between confirm and ack causes broker to redeliver the request, replier sends second reply — callers MUST dedupe by `CorrelationID`. Documented in godoc on `Serve`.
- **T31** `delay.go` + `Message[M].Delay` + `Topology` support for `x-delayed-message`

**Checkpoint Phase 7:**
- [ ] RPC happy path: caller gets response within 100ms locally.
- [ ] RPC timeout: caller times out cleanly; replier doesn't deliver
      to dead channel.
- [ ] Delayed message: `Delay: 2 * time.Second` causes consumer to
      see message after ≥ 2s, < 2.5s.

---

### Phase 8 — Production hardening

Goal: TLS/mTLS + SASL EXTERNAL, cluster failover, full Connection
options surface, and `amqpmock/` ready for downstream tests.

- **T32** TLS: `WithTLSConfig` + amqps:// integration test (testcontainer with self-signed cert from `amqptest/certs`)
- **T33** Cluster failover: `WithAddrs` + round-robin reconnect ordering
- **T34** Remaining Connection options: `WithVHost`, `WithAuth`, `WithHeartbeat` (godoc carries the SPEC §6.1 sizing tiers; `WithHeartbeat(0)` emits a `Dial`-time warning "heartbeats disabled — strongly discouraged"), `WithChannelMax`, `WithFrameMax` (godoc carries the SPEC §6.1 small/streaming/hard-max sizing table + the 100 MiB pointer-out), `WithDialer`, `WithClientProperties`, **`WithConnectionName`** (default `<binary>-<hostname>-<pid>`; role and index suffixed per TCP connection), **`WithPublisherConnections`**, **`WithConsumerConnections`**, **`WithOnResubscribe(func(queue string))`**
- **T34b** **SASL EXTERNAL** via `WithSASLMechanism(SASLExternal)`: integration test against a RabbitMQ testcontainer with `rabbitmq_auth_mechanism_ssl` plugin enabled + `external_auth` user; asserts `WithAuth(user, pass)` becomes a no-op under EXTERNAL and emits a warning log at `Dial`. **Fail-closed `Dial` validation** test matrix: (a) no `WithTLSConfig` → `ErrInvalidOptions`; (b) TLS config without client cert → `ErrInvalidOptions`; (c) plain `amqp://` scheme → `ErrInvalidOptions`; (d) full valid config → success
- **T35** `AutoAck()` opt-in + verbose godoc warning + integration test that documents the trade-off
- **T36** Remaining consumer options: `Exclusive`, `Args`, `OnCancel(func(reason string))` for `basic.cancel` (consumer goroutine returns `ErrConsumerCancelled`; mandatory metric `consumer_cancelled_total{queue, reason}`), `Tag` (note: no `NoLocal()` — RabbitMQ silently ignores it; explicitly omitted per SPEC §6 and §10.10; defaulted tags use `ctag-<uuidv7>` per T18)
- **T37** `amqpmock/` subpackage: `go generate` for interface mocks + hand-written `NewDelivery[M]` / `NewBatch[M]`. Also **`amqptest/`** subpackage exported: testcontainers helper supporting **three plugin-enablement modes** ((1) pre-baked image via `AMQPTEST_IMAGE`; (2) mounted `.ez` file via `AMQPTEST_DELAYED_PLUGIN_FILE`; (3) `t.Skip` fallback via `amqptest.RequireDelayedExchange(t)`); ships `rabbitmq_auth_mechanism_ssl` + pre-generated TLS certs in `amqptest/certs/`; ships `Dockerfile.amqptest` under `amqptest/docker/` for downstream consumers

**Checkpoint Phase 8:**
- [ ] mTLS handshake passes against a testcontainer with server +
      client certs.
- [ ] SASL EXTERNAL authenticates against the same testcontainer with
      `WithAuth` set to a wrong password (asserts password is
      ignored).
- [ ] `WithAddrs([node-down, node-up])` connects to second node.
- [ ] `WithConnectionName("orders")` + `WithPublisherConnections(3)`
      produces 3 sockets named `orders-pub-0..2` in
      `rabbitmqctl list_connections name`.
- [ ] `AutoAck` integration test logs the "you lose messages on
      crash" warning and demonstrates a deliberate drop.
- [ ] `amqpmock.NewDelivery[Order](Fixture{…})` constructs a usable
      `*Delivery[Order]` for unit tests.
- [ ] `amqptest.NewRabbitMQ(t)` spins up a broker with the delayed
      and SSL-auth plugins enabled; downstream tests in another module
      import it cleanly.

---

### Phase 9 — Release readiness: examples, docs, CI, tag

Goal: cut `v0.1.0`.

- **T38** Examples: `publish/`, `consume/`, `batch_publish/`, `batch_consume/`, `rpc/`, `delayed/`, `deadletter/`, `topology/`, `otel/`
- **T38b** `examples/idempotent_consume/main.go`: canonical dedupe-by-`MessageID` pattern referenced from SPEC §6.2.1; uses a bounded in-memory LRU cache; CI smoke test asserts duplicate publishes (forced via `PublishRetry`) are observed once by the handler
- **T38c** `examples/ordered_consume/main.go`: strict per-queue ordering via `Queue.SingleActiveConsumer=true` + `Consumer[M].Concurrency(1)`; CI smoke test asserts the publish order = handler observe order across an active-consumer failover
- **T39** README quickstart + links to every example
- **T40** CHANGELOG (Keep a Changelog format) + final godoc pass
- **T41** Coverage gate: ≥80% per package; ≥95% on `internal/reconnect`, `internal/confirms`, `channelpool`, **`internal/amqperror`**, **`internal/redact`** (Rev 7: amqperror + redact added per SPEC §9 line 2107–2109)
- **T42** CI: `.github/workflows/ci.yml` (lint + unit + integration + conformance) + Go matrix (1.23, 1.24)
- **T43** Release tooling: `.github/workflows/release.yml` running `gh release create --generate-notes` on tag push (no goreleaser per Op decision §5)
- **T44** Conformance tests: AMQP 0-9-1 wire-protocol assertions (separate `conformance/` directory); includes broker-nack path (`x-overflow=reject-publish`), `basic.qos.global=true` for `ChannelQoS()`, `x-delivery-limit` on quorum queues, mandatory return/ack correlation
- **T44b** **Throughput benchmark suite**: `BenchmarkPublishConfirmed` (single publisher conn), `BenchmarkPublishConfirmedMultiConn` (4 publisher conns), `BenchmarkPublishBatch`, `BenchmarkConsume`. Gates: ≥30k msg/s single-conn, ≥100k msg/s with `WithPublisherConnections(4)+WithChannelPoolSize(16)`, `PublishBatch` ≥5× `Publish`. Run on every release-candidate tag against the pinned testcontainer; nightly drift report
- **T45** Reconnect chaos test: **5-minute outage @ 10k msg/s** (was 60s @ 1k msg/s), zero loss with confirms, multi-conn fan-out enabled, `goleak.VerifyNone` clean; flaky-rate <1% over 50 runs
- **T45b** **Security regression test**: scan the recorded outputs of a 60s integration run with a credentialed URI; assert no log line, error message, span attribute, or metric label contains the clear-text password
- **T46** Cut `v0.1.0`

**Checkpoint Phase 9 / v0.1.0 release:**
- [ ] Every SPEC §9 success criterion is satisfied.
- [ ] `examples/*/main.go` builds and runs against a local broker
      (CI smoke step).
- [ ] `git tag v0.1.0` and GitHub release created automatically by
      `release.yml`.

---

## Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Reconnect loop under contention causes goroutine leaks. | High | `goleak.VerifyNone` in every test; chaos test in T45; tight unit tests for `internal/reconnect`. |
| Publisher-confirm tracker races (lost ACKs under reconnect). | High | Dedicated `internal/confirms` package with table-driven tests covering ordered acks, multiple acks, out-of-order nacks; integration test in T13. |
| `Topology.Declare` mismatch is silently destructive in `AttachTo` reconnect path. | Med | T16 explicitly asserts that mismatch returns `ErrTopologyMismatch` and does **not** mutate broker state; reconnect hook propagates the error to `OnReconnect` callback. |
| `delayed-message-exchange` plugin not enabled in testcontainer image. | Med | T31 integration uses **`amqptest/`** (T37): `RABBITMQ_ENABLED_PLUGINS_FILE`, `AMQPTEST_IMAGE` / `AMQPTEST_DELAYED_PLUGIN_FILE`, or `amqptest.RequireDelayedExchange(t)` per SPEC §6.9. |
| OTel propagation header names diverge from semantic conventions. | Med | T05/T27/T28 follow `go.opentelemetry.io/contrib/instrumentation/.../otelamqp` patterns; conformance test asserts header shape. |
| Channel pool exhaustion under load drops publishes silently. | Med | T08 returns `ErrChannelPoolExhausted` (added during T08 if needed); benchmark in T05/T22 surfaces backpressure behaviour. |
| TLS test failures from cert generation timing on slow CI. | Low | T32 ships pre-generated certs in **`amqptest/certs/`** rather than generating on each test run. |
| AMQP 4.x semantic changes vs 3.13. | Low | Integration matrix runs both; conformance tests use only 0-9-1 features available in both. |
| Reply-code translation inconsistent across publish/consume/declare paths. | Med | T07b centralises translation in `internal/amqperror`; every component that talks to the broker funnels errors through it. Integration tests in T13/T18/T15 assert `errors.Is` on representative codes (403, 404, 406, 540). |
| Multi-conn pool concurrency bugs (race in publisher conn selection, consumer pin drift after reconnect). | High | T07d is isolated and unit-tested with `-race`; consumer pinning is by stable hash of consumer-tag, asserted in a unit test that simulates connection failover. Bench in T44b stresses the path under load. |
| Credential leak via log adapter that bypasses `internal/redact`. | High | T07c choke-point + a `nolint`-grep test in CI (`rg "amqp.*://.*:.*@" -- *.go` excluded inside `internal/redact`); T45b runtime regression scan on integration-test output. |
| Re-subscribe storm after broker restart (every consumer races to reissue `basic.consume` at the same instant). | Med | T18 staggers resubscribes with a small bounded jitter (50–250ms) per consumer; chaos test in T45 asserts the broker accepts every resubscribe without overload. |
| Counter B (in-process redelivery counter) memory leak in long-lived consumers. | Med | T20 deletes the map entry on `Ack` or `Nack(false)`; entries reset on channel close. Stress test pushes 1M `MessageID` values through `ErrRequeue` loops and asserts steady-state map size. |

---

## Parallelization opportunities

The dependency graph allows the following parallelism after Phase 1
clears:

- **T07c (`internal/redact`)** is independent of every broker-touching
  task; can be drafted in parallel with T06.
- **T09 (codec/JSON)** and **T14 (topology types)** are independent of
  each other and of Publisher/Consumer — can be done in parallel.
- **T24 (Protobuf)** and **T25/T26 (CloudEvents)** are independent codec
  modules.
- **T38 (examples)** can be drafted in parallel with **T40–T42** once
  Phases 1–7 are done.
- **T44 (conformance)** can begin once Phase 2 lands; doesn't block
  later phases.
- **T44b (bench)** and **T45b (security regression)** can run in
  parallel once Phase 8 lands.

Sequential pinch-points (do **not** parallelize):

- Phase 1 must complete before Phase 2 (Connection underlies Publisher).
- **T07c (`internal/redact`)** must land before T03 (log adapters
  consume it) and before T07/T07d (errors and metrics emit URIs).
- T07b (`internal/amqperror`) is on the critical path between T07
  and T08 — T08+ all consume the translator, so a broken translator
  cascades.
- **T07d (multi-conn pool)** depends on T07 + T07c (redaction for
  per-conn names) and gates everything in Phase 2 onwards (Publisher
  pool acquisition).
- Phase 3 must complete before Phase 4 (`x-death` reading requires
  DeadLetter helper present in topology; `x-delivery-limit` arg
  comes from `Queue.DeliveryLimit` in T14).
- Phase 6 OTel integration (T27/T28) modifies Publisher and Consumer
  code paths added in Phases 2 and 4 — needs those settled first.
- **T45 (chaos) + T45b (security scan)** gate T46 (cut v0.1.0).

---

## Operational decisions (closed)

All operational questions were resolved during plan review. Recorded
here so the next reader doesn't have to re-derive.

1. **Pinning policy for transitive deps:** **Pin exact versions in
   `go.mod`**, refresh quarterly via a dedicated dependency-bump PR.
   Rationale: reproducible builds; the library is downstream-facing,
   so a surprise auto-patch could cascade.

2. **testcontainers image tags:** **Pin minor-patch**
   (`rabbitmq:3.13.7-management` + `rabbitmq:4.0.5-management` or
   their then-current patch). Reviewed on each release tag.
   Rationale: deterministic CI, conscious upgrades.

3. **Conformance suite implementation:** **Live broker** for v0.1.0.
   A protocol stub is too much engineering for now; revisit if v1
   reveals coverage gaps the live broker can't surface.

4. **Fuzz targets for v0.1.0:** `FuzzCodecJSON` (T09),
   `FuzzCodecProtobuf` (T24), `FuzzCodecCloudEventsBinary` (T26),
   `FuzzXDeathParser` (`internal/headers`, T17), **`FuzzRedactURI`**
   (`internal/redact`, T07c). Other targets added later as bugs
   surface.

5. **goreleaser scope:** **Skipped for v0.1.0** — pure library, no
   binaries to release. T43 ships a thin `release.yml` that uses
   `gh release create --generate-notes` from the latest tag.
   Reconsider goreleaser only if we end up needing artifact bundles
   (unlikely for a library).

6. **Pre-commit hooks:** Opt-in via `make hooks` (writes
   `.git/hooks/pre-commit` running `make lint test`). Never
   auto-installed. T01 adds the `hooks` target.

7. **Throughput gates:** Bench gates (≥30k/s single-conn, ≥100k/s
   multi-conn, `PublishBatch` ≥5× `Publish`) run on every release
   candidate against the pinned testcontainer image on a reference
   runner (Apple M-series laptop or GH-hosted `macos-14`). Drift
   is reported nightly; gates block the `v0.1.0` tag.

8. **OTel Messaging semconv:** Pinned to **v1.27.0+**
   (`go.opentelemetry.io/otel/semconv/v1.27.0`). Re-evaluate on any
   breaking semantic-conventions bump.

## Out of scope (tracked for v0.2)

- Native stream-protocol consume (`x-stream-offset`,
  super-streams, dedicated `StreamConsumeBuilder`). `QueueTypeStream`
  declaration ships in v0.1; native consume in v0.2.
- OAuth2 SASL mechanism (via `rabbitmq_auth_backend_oauth2`).
  PLAIN + EXTERNAL ship in v0.1.
- Per-message deduplication via `rabbitmq_message_deduplication`
  plugin (separate from `MessageID`-based application-side dedupe,
  which is unaffected).
- Consistent-hash routing helper (`x-consistent-hash` plugin).
- Federation / shovel topology helpers.
