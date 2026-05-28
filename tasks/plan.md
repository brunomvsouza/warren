# Implementation Plan: Warren v0.1.0

> Companion document to `SPEC.md`. The spec defines **what** to build; this
> plan defines **how** and **in what order**. Read the spec first.

---

## Overview

Implement the v1 surface of `github.com/brunomvsouza/warren` defined in
`SPEC.md` §6, sliced vertically so every phase ends in a state that
compiles, has passing tests, and demonstrates a usable end-to-end path.
First public tag is `v0.1.0`; `v1.0.0` is cut only after every success
criterion in SPEC §9 is checked off.

**Targets:** AMQP 0-9-1 as implemented by **RabbitMQ 3.13 LTS / 4.x**.
The library does not aim to be portable to other AMQP 0-9-1 brokers
(Qpid, ActiveMQ). RabbitMQ extensions surfaced as first-class API are
listed in `SPEC.md` §6 ("Note on AMQP 0-9-1 vs RabbitMQ").

**Revision history:** (Rev 9 → Rev 8 → Rev 7 → Rev 6 → Rev 5 ordering preserved at the top — newest specialist pass first; Rev 5 sits below Rev 6 because it was the prior milestone the Rev 6 pass superseded.)
- Rev 9 — SRE & AMQP 0-9-1 specialist review. 54 tasks in existing phases (T01–T46 surface updated, no new tasks added from spec review; Phase 10 adds T47–T56). Five surface changes:
  1. `Topology.Declare` auto-injects `x-dead-letter-strategy: at-least-once` for quorum queues.
  2. `Connection.Close` cascade (cancel consumers → wait handlers → wait confirms → close sockets).
  3. `Concurrency(n)` non-blocking dispatcher requirement.
  4. `Caller[Req,Resp]` auto-stamps `CorrelationID` and `ReplyTo`.
  5. `BatchConsumer` OTel span creates Links for all incoming trace contexts.
- Rev 8 — post SRE on-call review (2026-05-22). 54 tasks (unchanged).
  Three surface changes from the SRE review (items A + E + M in the
  analysis).
  - **Item A (codec):** **`codec.NewJSON()` is now lax by default
    (Postel's Law)**; `codec.NewJSONStrict()` is the opt-in
  `DisallowUnknownFields` variant. The Rev 5 strict-default was
  optimised for "surface schema drift as `ErrInvalidMessage`", but on-call
  evidence from operating Rabbit at billions/day shows the realistic
  failure mode is a v2 producer deploying a new field while v1
  consumers are still rolling — strict-default makes every
  producer-first deploy a DLQ-poisoning fire drill. SPEC §6.9 + §8
  Always + §9 success criteria + §10 #23 updated. T09 acceptance,
  architecture decision #13, and the previously-removed `NewJSONLax`
  symbol (now collapsed into `NewJSON`) all updated. Code change:
  `codec/json.go` + `codec/json_test.go` flip default; existing
  callers using `codec.NewJSON()` automatically get the safer
  producer-first-deploy behaviour.
  - **Item E (payload guardrail):** new
    `PublisherBuilder.MaxMessageSizeBytes(n)` rejects oversized
    bodies locally with `ErrMessageTooLarge` (permanent) **before**
    opening a channel. Default 16 MiB; `n=0` disables; `n<0` fails
    `Build` with `ErrInvalidOptions`. Mandatory metric outcome:
    `publisher_publish_seconds{exchange, outcome="too_large"}`.
    SPEC §6.2 + §6.8 + §10 #50 updated. T13 acceptance amended in
    todo.md. Code lives in `publisher_builder.go` (option) +
    `publisher.go` (Publish-time check) + `errors.go` (new sentinel
    classified `IsPermanent==true`).
  - **Item M (tracing continuity post-mortem):** the contract that
    T27/T28 must satisfy when implemented is hardened. Three load-
    bearing invariants:
    1. **Publisher injects `traceparent`/`tracestate` before any
       frame is written** so the propagated context travels as part
       of `basic.publish` and survives any DLX bounce automatically
       (broker preserves headers + properties on dead-letter).
    2. **The library never strips, rewrites, or normalises message
       headers on the consume path.** A symbol-absence test
       enforces this; any future code that mutates headers must
       update SPEC §6.9 + §6.6 first.
    3. **Spans terminate with an outcome attribute and an OTel
       status matching the verdict.** Consumer:
       `messaging.rabbitmq.outcome` ∈ `ack|nack_requeue|
       nack_no_requeue|max_redeliveries|timeout|
       handler_aborted_channel_closed`; failures call
       `Span.RecordError`, set `Status=Error`, and set `error.type`
       to the sentinel name (e.g. `"ErrMaxRedeliveries"`).
       Publisher mirrors the contract for the publish failure
       matrix. Poisoned messages render red in trace UIs and
       support assertive alerts like "spike of
       `error.type="ErrMaxRedeliveries"`" without query gymnastics.

    SPEC §6.3 + §6.6 + §6.9 + §10 #51 updated. T27/T28 acceptance
    in todo.md amended with the full outcome matrix, error-type
    matrix, panic-cleanup test, and a new DLX-path integration test
    that asserts trace-id continuity across producer → consumer →
    DLQ consumer.

  No new tasks; no dependency-graph change.
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
    `ErrInvalidMessage`. *(Superseded by Rev 8: the strict default
    was inverted to lax per Postel's Law after SRE review evidence
    on producer-first deploys.)*
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
13. JSON codec is **lax by default** (Postel's Law — `Decode` tolerates
    unknown fields so producer-first deploys do not poison v1 DLQs);
    `codec.NewJSONStrict()` is the opt-in `DisallowUnknownFields` mode.
    Codec calls are wrapped in `recover` → `ErrInvalidMessage`.
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
- **T07** `connection.go`: single-TCP `Dial` + `Health` (verifies socket+topology) + `Close(ctx)` **with graceful cascade** (cancel consumers → wait handlers → wait confirms → close TCP); validates `ChannelPoolSize ≤ channel-max`; blocked-connection semantics; **synchronous reconnect barrier** (channel re-open → topology redeclare → consumer re-`basic.consume` → user `WithOnReconnect`) with `Publish` blocking on `ErrReconnecting` until step 2 completes; **degraded-state machine** on persistent redeclare failure (`ErrTopologyRedeclareFailed`, `WithOnTopologyDegraded`, `connection_degraded_total`, `(*Connection).ForceReconnect()` operator helper, `topology_redeclare_seconds{role}` histogram); **`Connection.Logger()` removed** from public API; **`(*Connection).AuthenticatedUser()` populated by `Dial` before it returns** and exposed for `UserID` client-side validation; **`Dial` validates SASL EXTERNAL** fail-closed (TLS + client cert + `amqps://`); **`Dial`-time warning** when `WithPublisherConnections(1)` or `WithConsumerConnections(1)`; **`frame_max < 4096` rejected** with `ErrInvalidOptions`
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

- **T09** `codec/` package: `Codec` interface + `codec.NewJSON()` (**lax by default** — Postel's Law, accepts unknown fields on `Decode` so producer-first deploys do not poison v1 DLQs) + `codec.NewJSONStrict()` opt-in (`DisallowUnknownFields` for compliance pipelines) + round-trip tests + fuzz target; Publisher/Consumer wrap codec calls in `defer recover` → `ErrInvalidMessage`
- **T10** `message.go`: `Message[M]` struct with `ContentType` + `ContentEncoding` separated; default-apply logic (UUID v7, timestamp, persistent, ContentType ← codec.ContentType); Headers field-table typing validation; field godoc covering `Priority` range/clamp, `Expiration` wire format, `Headers` typing per SPEC §6.5
- **T11** `internal/confirms`: publisher-confirm tracker handling ack, nack (→ `ErrPublishNacked`), return-then-ack (→ `ErrUnroutable` **wrapped with the originating `basic.return` reply code 312/313** so `AMQPCode(err)` returns it), and channel-close (→ `ErrChannelClosed`); per-channel tracker; `multiple=true` semantics
- **T12** `publisher.go` + `publisher_builder.go`: `PublisherFor[M]` builder (no `Immediate()` — RabbitMQ rejects it) + `Publish` (sync-confirm, concurrency-safe, channel acquired from publisher pool) + `Close(ctx)` drain
- **T13** Mandatory + `OnReturn(Return)` with rich `ReturnedProperties` + `ConfirmTimeout` **(default 30 s)** + `PublishTimeout` (end-to-end cap including pool acquire / blocked-wait / reconnect barrier) + **`PublishBatchMaxSize`** (renamed from `PublishBatchMaxInFlight`) + `PublishRetry(p)` (retries only `IsTransient` errors, **emits `publisher_retry_total{exchange, reason}`** on each retry) + `StampUserID()` builder + **client-side `UserID` validation** in `Publish` → `ErrInvalidMessage` on divergence + **functional-options last-wins** asserted on `PublisherBuilder` (per SPEC §6.1 line 515; covers `.Metrics`/`.WithoutMetrics`/`.Tracer` chains) + `ErrUnroutable` + `ErrConfirmTimeout` + `ErrPublishNacked` + `ErrConnectionBlocked` on ctx-cancel while broker-blocked + `ErrReconnecting` on ctx-cancel while reconnect barrier holds
- **T13b** Checkpoint example `examples/publish/main.go` (SPEC §7 + Rev decision 49): `package main` reading `AMQP_URL` env (default `amqp://guest:guest@localhost:5672/`), declaring topology in-process, demonstrating `PublisherFor[M]`, `Publish`, mandatory + `OnReturn`, `ConfirmTimeout`, and `PublishRetry`. CI: build in unit lane (`go build ./examples/...`) + smoke-run in `integration` lane against a testcontainer broker (the integration suite's existing wiring; will migrate to `amqptest.NewRabbitMQ(t)` once T37 lands).

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
- [ ] **Example(s):** `examples/publish/main.go` builds on the unit
      lane and smoke-runs end-to-end against `amqptest.NewRabbitMQ(t)`
      on the integration lane. Demonstrates `PublisherFor[M]`,
      `Publish`, mandatory + `OnReturn`, `ConfirmTimeout`, and
      `PublishRetry` per SPEC §7 "Executable examples at checkpoints".

---

### Phase 3 — Topology: declared once, separately

Goal: declare exchanges/queues/bindings/DLX from one place;
re-declare automatically on reconnect.

- **T14** `topology.go`: `Topology` + `Exchange`/`Queue`/`Binding` (all with `NoWait`) + `Queue.Type` (`QueueType`) + `DeadLetter` (with `MaxLengthBytes` + `Overflow`) + `OverflowPolicy` constants
- **T15** `Topology.Declare(ctx, conn)`: **two-step pipeline** — in-memory DLX expansion (merges args into source `Queue.Args`, appends DLX exchange + `<Source>.dlq` queue, **injects `x-dead-letter-strategy: at-least-once` for quorum queues**) **before** any broker call; then broker-side declare in order exchanges → queues → bindings. Idempotent; `ErrTopologyMismatch` (wrapping `ErrPreconditionFailed`) on conflict; `NoWait=true` downgrades mismatch detection to async (documented)
- **T16** `Topology.AttachTo(conn)` registers a **deep snapshot keyed by the pointer address of the input `*Topology`** (Rev 7) as a reconnect hook: `AttachTo` with the same pointer replaces the prior snapshot for that key; `AttachTo` with a different pointer appends an additional snapshot — both fire on every reconnect in registration order; redeclare runs **inside the synchronous reconnect barrier** (§6.1) before publishers resume / consumers re-`basic.consume`; persistent redeclare failure → **degraded state** with `ErrTopologyRedeclareFailed`, mandatory metric `connection_degraded_total{role, reason}`, `WithOnTopologyDegraded(func(error))` callback; `topology_redeclare_seconds{role}` histogram records the barrier duration
- **T16b** Checkpoint examples `examples/topology/main.go` and `examples/deadletter/main.go` (SPEC §7 + Rev decision 49): both `package main` reading `AMQP_URL`. `topology/` demonstrates `Topology.Declare` (exchanges → queues → bindings), idempotent re-declare, and `AttachTo` with a forced reconnect. `deadletter/` demonstrates a `DeadLetter` entry expanding to the right `x-dead-letter-*` args + a quorum queue with `DeliveryLimit`. CI build (unit lane) + smoke-run (integration lane).

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
- [ ] **Example(s):** `examples/topology/main.go` and
      `examples/deadletter/main.go` build on the unit lane and
      smoke-run end-to-end on the integration lane. Demonstrate
      `Topology.Declare`, `AttachTo`, DLX expansion, and a quorum
      queue per SPEC §7 "Executable examples at checkpoints".

---

### Phase 4 — Consumer: error-driven semantics + escape hatch

Goal: `Consumer[OrderPlaced].Consume` with a payload-first handler;
`nil`/error/`ErrRequeue`/`ErrPoison` map to the right ack/nack;
`ConsumeRaw` for envelope access; `MaxRedeliveries` shields against
infinite loops; consumer survives reconnects (re-issues
`basic.consume`); handler ctx cancels on channel close.

- **T17** `delivery.go`: concrete `Delivery[M]` struct + `x-death` header parsing + `DeathCount()` (sums entries with `reason ∈ {rejected, delivery-limit}` — `expired` and `maxlen` reflect broker policy, not handler-driven rejection) + new `DeathCountByReason(r string) int` + `DeathReasons() []string`; `Ack`/`Nack`/`AckIf` return `ErrChannelClosed`/`ErrAlreadyClosed` paths documented
- **T18** `consumer.go` + `consumer_builder.go`: `ConsumerFor[M]` builder (no `NoLocal()`; `ChannelQoS()` instead of `GlobalQoS()`; `PrefetchBytes()` documented as no-op on RabbitMQ; new `Priority(int)` for `x-priority`; new `HandlerTimeout(d)` for per-message ctx deadline; new `HandlerTimeoutVerdict(TimeoutVerdict)` default `TimeoutNackNoRequeue`; **functional-options last-wins** asserted on the builder per SPEC §6.1 line 515) + `Consume` **(with non-blocking dispatcher)** + handler error mapping + **re-subscribe loop** that, after a successful reconnect of the consumer TCP connection, reopens the channel, reapplies `basic.qos`, and reissues `basic.consume` exactly once per active consumer; **handler ctx cancel** with cause `ErrChannelClosed` on mid-handler channel close; **default consumer-tag = `ctag-<uuidv7>`** generated at `Build` for multi-conn fan-out; **`Build`-time warning** when `Prefetch < Concurrency`
- **T18b** `HandlerTimeoutVerdict` matrix test: configure `HandlerTimeout(50ms)` with each of the two verdicts; assert (1) `TimeoutNackNoRequeue` lands the timed-out message in DLX; (2) `TimeoutNackRequeue` requeues subject to `MaxRedeliveries` / `x-delivery-limit`
- **T19** `ConsumerMetrics` interface + Prometheus impl + wired into `Consume`; mandatory metrics include `consumer_resubscribed_total`, `consumer_handler_aborted_channel_closed_total`, `consumer_handler_seconds` (histogram)
- **T20** `MaxRedeliveries` enforcement: **two-counter design** with quorum-queue carve-out. (A) `x-death`-based ceiling for DLX-bounce loops (cross-process); (B) in-process counter keyed by **`(channel-instance-id, MessageID)`** for `ErrRequeue` loops (process-local, resets on consumer restart and on channel close). Either ceiling escalates to `Nack(false)` + `ErrMaxRedeliveries`. When the source queue is `QueueTypeQuorum` with `DeliveryLimit>0`, counter B is auto-disabled (broker is authoritative); metric label `cause=delivery-limit|x-death|in-process` distinguishes the three paths. Required because AMQP 0-9-1 only writes `x-death` on dead-letter events, not on `Nack(requeue=true)`
- **T21** `ConsumeRaw(ctx, RawHandler[M])` + `Delivery.AckIf(err)`
- **T21b** Checkpoint example `examples/consume/main.go` (SPEC §7 + Rev decision 49): `package main` reading `AMQP_URL`, declaring topology in-process, running a `ConsumerFor[M]` with a payload-first handler that demonstrates the three result classes (Ack on nil, `Nack(false)` on error, `Nack(true)` on `ErrRequeue`), `MaxRedeliveries(3)`, and `HandlerTimeout(2*time.Second)`. CI build (unit lane) + smoke-run (integration lane).

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
- [ ] **Example(s):** `examples/consume/main.go` builds on the unit
      lane and smoke-runs end-to-end on the integration lane.
      Demonstrates `ConsumerFor[M]` with a payload-first handler,
      default `Nack(false)` on error, opt-in `ErrRequeue`,
      `MaxRedeliveries`, and `HandlerTimeout` per SPEC §7
      "Executable examples at checkpoints".

---

### Phase 5 — Batch APIs: throughput

Goal: `Publisher.PublishBatch` always-all semantics with explicit
in-flight cap; `BatchConsumer` with size + flush-after timer and
documented Ack/Nack-with-`multiple=true` semantics.

- **T22** `Publisher.PublishBatch(ctx, []Message[M]) ([]PublishResult, error)` + `ErrPartialBatch` + **`ErrBatchTooLarge`** when the call exceeds `PublishBatchMaxSize` (default 1024). All N publishes pipeline on a **single channel** to preserve per-channel input order. Per-message failure fixture uses **client-side `ErrInvalidMessage`** (unsupported `Headers` type) so the channel stays open across the batch — body-too-large /311 would close the channel mid-batch and corrupt the always-all contract. **Documented contract:** `PublishRetry` does NOT apply to `PublishBatch`; per-message `ErrChannelClosed` cannot distinguish "broker persisted" from "broker did not receive", so retry is the caller's problem (chunking + dedupe-by-`MessageID`)
- **T23** `batch_consumer.go` + `batch_consumer_builder.go` + concrete `Batch[M]` struct + auto-Ack/Nack with `multiple=true` on the highest delivery-tag in the batch; handler-explicit acking via `Batch.Deliveries()` suppresses the auto-verdict (idempotent guard inside `Batch`); `HandlerTimeout(d)` applies to the whole batch with default verdict `Nack(false)` on timeout
- **T23b** Checkpoint examples `examples/batch_publish/main.go` and `examples/batch_consume/main.go` (SPEC §7 + Rev decision 49): both `package main` reading `AMQP_URL`. `batch_publish/` demonstrates `PublishBatch` always-all + `[]PublishResult` interpretation + `ErrBatchTooLarge` guard. `batch_consume/` demonstrates `BatchConsumerFor[M]` with `Size(100)` + `FlushAfter(1s)` + auto-`multiple=true` ack on nil handler return. CI build (unit lane) + smoke-run (integration lane).

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
- [ ] **Example(s):** `examples/batch_publish/main.go` and
      `examples/batch_consume/main.go` build on the unit lane and
      smoke-run end-to-end on the integration lane. Demonstrate
      `PublishBatch` always-all + `[]PublishResult` and
      `BatchConsumer` with `Size`/`FlushAfter` per SPEC §7
      "Executable examples at checkpoints".

---

### Phase 6 — Codecs + Observability beyond JSON

Goal: Protobuf + CloudEvents (both modes) codecs; OTel spans across
publish→consume with header propagation.

**Codec interop principle (SPEC §10 decision 4):** codec/wire-format
decisions are grounded in interop with **non-Go (or non-warren)
clients**, following the authoritative binding/format spec; an official
upstream library is preferred over a hand-rolled mapping when it
improves fidelity. The CloudEvents codecs therefore use the official Go
SDK and the canonical CloudEvents AMQP Protocol Binding.

- **T24** `codec/protobuf.go` + round-trip tests against a representative `.proto`
- **T25** `codec/cloudevents.go` structured mode — `codec.NewCloudEventsStructured()` (`application/cloudevents+json`); operates on the official SDK's `cloudevents.Event` type (re-exported as `codec.CloudEvent`) and delegates JSON serialization to the SDK event format
- **T26** `codec/cloudevents.go` binary mode — `codec.NewCloudEventsBinary()` per the **CloudEvents AMQP Protocol Binding**: `data` in the body, `datacontenttype` → AMQP content-type property, all other attributes/extensions → `cloudEvents:`-prefixed AMQP headers (official Go SDK default; interoperates with non-Go AMQP-1.0 clients via RabbitMQ's protocol bridging). Introduces the optional `codec.HeaderCodec` interface (`EncodeWithHeaders`/`DecodeWithHeaders`, both carrying a `contentType`) and wires it into `publisher.go` (`encodeMsg` merges returned headers into `Message.Headers` and overrides `Message.ContentType`) and `consumer.go`/`batch_consumer.go` (`safeDecodeConsumer` passes `Delivery.Headers` + content-type to `DecodeWithHeaders`) so the codec is functional end-to-end.
- **T27** OTel integration in `Publisher` (publish span + inject context into AMQP headers)
- **T28** OTel integration in `Consumer` (extract span context from headers + handler span). For `BatchConsumer`, the span must contain **Links** to the extracted `traceparent` of every message in the batch.

**Checkpoint Phase 6:**
- [x] Protobuf round-trip: encode → publish → consume → decode →
      identical message.
- [x] CloudEvents structured: body is full envelope; content-type is
      `application/cloudevents+json`.
- [x] CloudEvents binary: body is `data` only; `cloudEvents:id`,
      `cloudEvents:source`, `cloudEvents:type`, `cloudEvents:specversion`
      present as AMQP headers; `datacontenttype` on the content-type
      property.
- [x] Span continuity: trace-id and parent-span-id consistent from
      publisher → consumer (T28; unit + `integration` DLX test).

---

### Phase 7 — Advanced patterns: RPC + delayed messages

Goal: request/reply via `direct reply-to`; delayed publish via
`x-delayed-message` exchange plugin.

- **T29** `rpc.go`: `Caller[Req,Resp]` + `Call(ctx, req)` + `ErrCallTimeout` (direct reply-to: caller auto-enables no-ack, declares reply consumer before publishing) + `UseExclusiveReplyQueue()` fallback builder method + **auto-populates `CorrelationID` and `ReplyTo`**
- **T30** `rpc.go`: `Replier[Req,Resp]` + `Serve(ctx, handler)` + `OnError(func)` for error replies + **`ReplierBuilder.Topology(t)` auto-validates DLX presence on the request queue** (returns `ErrInvalidOptions` unless `AllowMissingDLX()` opts in) + mandatory metric **`replier_drop_no_dlx_total{queue}`** so the silent-drop failure mode is always observable. **At-least-once reply ordering**: handler runs → reply published → reply confirm awaited → request acked. If reply publish fails (`ErrPublishNacked`/`ErrConfirmTimeout`/`ErrChannelClosed`), request is `Nack(false)` so it goes to the request queue's DLX if configured. Crash between confirm and ack causes broker to redeliver the request, replier sends second reply — callers MUST dedupe by `CorrelationID`. Documented in godoc on `Serve`.
- **T31** `delay.go` + `Message[M].Delay` + `Topology` support for `x-delayed-message`
- **T31b** Checkpoint examples `examples/rpc/main.go` and `examples/delayed/main.go` (SPEC §7 + Rev decision 49): both `package main` reading `AMQP_URL`. `rpc/` demonstrates `Caller[Req,Resp]` (direct reply-to) + `Replier[Req,Resp]` with `Topology(t)` + a DLX on the request queue. `delayed/` demonstrates `Message[M].Delay = 2*time.Second` against an `ExchangeDelayed` exchange. CI build (unit lane) + smoke-run (integration lane); the `delayed/` smoke-run uses `amqptest.RequireDelayedExchange(t)` once T37 lands (skip-clean otherwise).

**Checkpoint Phase 7:**
- [ ] RPC happy path: caller gets response within 100ms locally.
- [ ] RPC timeout: caller times out cleanly; replier doesn't deliver
      to dead channel.
- [ ] Delayed message: `Delay: 2 * time.Second` causes consumer to
      see message after ≥ 2s, < 2.5s.
- [ ] **Example(s):** `examples/rpc/main.go` and
      `examples/delayed/main.go` build on the unit lane and
      smoke-run end-to-end on the integration lane (the delayed
      example uses `amqptest.RequireDelayedExchange(t)` to skip
      cleanly when the plugin is absent). Demonstrate
      `Caller[Req,Resp]`/`Replier[Req,Resp]` and `Message[M].Delay`
      via `x-delayed-message` per SPEC §7 "Executable examples at
      checkpoints".

---

### Phase 8 — Production hardening

Goal: TLS/mTLS + SASL EXTERNAL, cluster failover, full Connection
options surface, and `amqpmock/` ready for downstream tests.

- **T32** TLS: `WithTLSConfig` + amqps:// integration test (testcontainer with self-signed cert from `amqptest/certs`)
- **T33** Cluster failover: `WithAddrs` + round-robin reconnect ordering
- **T34** Remaining Connection options: `WithVHost`, `WithAuth`, `WithHeartbeat` (godoc carries the SPEC §6.1 sizing tiers; `WithHeartbeat(0)` emits a `Dial`-time warning "heartbeats disabled — strongly discouraged"), `WithChannelMax`, `WithFrameMax` (godoc carries the SPEC §6.1 small/streaming/hard-max sizing table + the 100 MiB pointer-out), `WithDialer`, `WithClientProperties`, **`WithConnectionName`** (default `<binary>-<hostname>-<pid>`; role and index suffixed per TCP connection), **`WithPublisherConnections`**, **`WithConsumerConnections`**, **`WithOnResubscribe(func(queue string))`**
- **T34b** **SASL EXTERNAL** via `WithSASLMechanism(SASLExternal)`: integration test against a RabbitMQ testcontainer with `rabbitmq_auth_mechanism_ssl` plugin enabled + `external_auth` user; asserts `WithAuth(user, pass)` becomes a no-op under EXTERNAL and emits a warning log at `Dial`. **Fail-closed `Dial` validation** test matrix: (a) no `WithTLSConfig` → `ErrInvalidOptions`; (b) TLS config without client cert → `ErrInvalidOptions`; (c) plain `amqp://` scheme → `ErrInvalidOptions`; (d) full valid config → success
- **T34c** **Panic isolation for user-provided callbacks**: every user callback must be wrapped in `recover()` and every pure-notification callback must run in a goroutine so a slow or panicking handler cannot block internal event loops. Five sites identified: (1) `WithOnBlocked` — move to a goroutine + recover (currently inline in the `supervisor` event-select; a slow/panicking callback delays broker unblock detection); (2) `WithOnReconnect` — add recover around the inline call (currently before `barrierCond.Broadcast()`; a panic permanently deadlocks all Publishers); (3) `WithOnTopologyDegraded` — add recover inside the existing goroutine (panic crashes the process); (4) `Handler[M]` / `RawHandler[M]` dispatch — add recover in both inline and goroutine paths (panic should nack without requeue, not crash the consumer goroutine); (5) `BatchHandler[M]` — same recovery wrapper as (4).
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

- **T38** Examples consolidation/polish pass (per SPEC §7
  "Executable examples at checkpoints" + Rev decision 49):
  `publish/`, `consume/`, `batch_publish/`, `batch_consume/`,
  `rpc/`, `delayed/`, `deadletter/`, `topology/` already exist on
  `main` (landed in their respective Phase 2/3/4/5/7 checkpoints);
  T38 adds the remaining release-only examples — `otel/`
  (depends on Phase 6 instrumentation), polishes existing examples
  for consistency (env-var conventions, godoc header, README
  cross-links), and ensures every example builds on the unit lane
  and smoke-runs on the integration lane via `examples/...`
  targets in the CI workflow.
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

### Phase 10 — SRE & Resilience Hardening

Goal: Address gaps identified in the SRE assessment (operability at scale, memory limits, and cardinality) originating from `LATER.md`.

- **T47** `topology.go`: Inject `Binding{Exchange: dlxName, Queue: dlqName, RoutingKey: "#"}` in the in-memory expansion of `DeadLetter` (fix routing limbo).
- **T48** `codec/json.go`: Evaluate `dec.More()` in `Decode` and return `ErrInvalidMessage` if trailing data exists. Add Fuzz target for strict mode.
- **T49** `metrics/prometheus.go` and `consumer.go`: Change the `reason` label in `consumer_cancelled_total` from a raw UUIDv7 to a closed enum (`"queue_deleted"`, `"exclusive_revoked"`, `"unknown"`), preventing Prometheus OOM.
- **T50** `consumer.go`: Expose `MaxInFlightBytes(n)` on the builder. Decrement semaphore after handler; pause ingestion when the limit is reached to avoid local OOM.
- **T51** `publisher.go`: Add `WithPublishRateLimit(perSec)` (local token-bucket) to protect the broker from accidental runaway loops.
- **T52** `consumer.go` and `metrics/`: Create `WithQueueDepthSampler` to poll (via `declare-passive`) and export native `queue_depth` and `dlq_depth` gauges.
- **T53** `consumer.go`: Expose `Pause(ctx)` and `Resume(ctx)` for manual graceful degradation. Evolve the check to a rich `Health()` with In-Flight Handlers and Last Delivery info for k8s liveness probes.
- **T54** `errors.go`: Refine `IsTransient()` to return false when the root cause is `context.Canceled`, blocking useless PublishRetries from upstream request cancellations.
- **T55** `consumer_builder.go`: Create native `WithDedupe(store, ttl)` middleware, abstracting LRU/Redis cache from the handler and ensuring correct commits.
- **T56** `codec/json.go`: Add `WithUnknownFieldObserver(func(path string))` to lax `NewJSON`, emitting the `codec_unknown_fields_total` metric to monitor silent schema drift.

### Phase 11 — AMQP/SRE Specialist Re-review (Rev 10)

Goal: close the correctness defects and reliability-bar gaps found in
the Rev 10 specialist pass (SPEC §10 "Rev 10"). The SPEC corrections
(R10-1..R10-4) are already applied inline; these tasks make the code,
validation, tests, and observability match the corrected contract.
Vertical slices: each task carries its SPEC touch-up (if any) + code +
test in one path. **The reconnect-path trio (T61→T62→T63) shares the
supervisor and MUST be sequenced, not parallelized.** R10-18
(async-publish ceiling) is deferred to `LATER.md` LATER-34.

**Lens-01 re-review (2026-05-28):** the protocol re-review (Phase 12
below) pulls **T60, T61, T65, T66** forward into its priority sequence
(they each violate the §1 no-silent-failure bar in v0.1); their
definitions remain here. It also extends **T58, T59, T63, T64** (see the
amended bullets) and adds **T74–T83**.

**Lens-02 re-review (2026-05-28):** the distributed-systems / failure-mode
re-review (Phase 13 below) pulls **T62, T63, T70, T71** forward into v0.1 (active
§1 failure-mode gaps), extends **T60, T66** (already pulled into Phase 12) and
**T82**, adds **T84–T93**, and introduces a new `chaos` test lane (3-node cluster
+ fault injector).

- **T57** `message.go` + `topology.go`: godoc on `Message.Delay` and `ExchangeKindDelayed` mirroring the SPEC §6.5 durability warning (scheduled messages lost on node failure; recommend durable-queue + TTL + DLX); optional declare-time warning when an `ExchangeDelayed` is declared. **(R10-1, P0.1)**
- **T58** `topology.go`: `validate()` warning when `Type=QueueTypeQuorum && DeliveryLimit==0` (RabbitMQ 4.x applies a broker default of 20 — not unbounded). Align §9 poison-loop wording. **Lens-01 (RMQ-06):** make the warning **version-aware** — read the broker version from the `connection.start` server-properties: on 4.x the default is 20 (silent drop at the 21st delivery), but on **3.13 a `DeliveryLimit==0` quorum queue is genuinely unbounded (infinite poison loop)**, the opposite failure mode. Fix the stale `topology.go:48-49` "Zero means unbounded" godoc; add a per-version poison-loop integration assertion (gate G3). **(R10-2, P0.2)**
- **T59** `internal/confirms` / `publisher_test.go`: regression test that locks the return/ack ordering invariant — fails if the `basic.return` notify channel is buffered or the demux is split across goroutines (assert `MarkReturned` precedes `Ack` resolution for a mandatory-unroutable publish under load). **Lens-01 (RMQ-16):** also add a real-broker assertion (not just the mock tracker), pin the `amqp091-go` version in `go.mod`, and comment that the invariant depends on amqp091-go's single synchronous reader goroutine (a buffered or worker-pool dispatcher upstream would silently break it). **(R10-3, P0.3)**
- **T60** `delivery.go` + `consumer.go`: idempotent resolved-once guard on `Delivery[M]` (mirrors `Batch[M]`). A handler-timeout verdict followed by a late handler `Ack`/`Nack`, or a double `Delivery.Ack/Nack/AckIf` via `ConsumeRaw`, is a no-op — never a second frame that channel-closes with `PRECONDITION_FAILED`. **Lens-02 (DS-04):** amend §6.3 to document the double verdict as a no-op and that **pre-fix** it channel-closes (406/`PRECONDITION_FAILED`) and takes out *every* in-flight handler on that channel — collateral loss, not just a duplicate; add a `ConsumeRaw` double-call unit test. **(R10-5, P0.4)** — *pulled into Phase 12; Lens-02 adds the §6.3 wording.*
- **T61** `connection.go` + `consumer.go`: channel-level recovery distinct from TCP reconnect. Consumer observes its own channel close (404/406/ack-error with the TCP connection still up) and reopens + re-`basic.qos` + re-`basic.consume`, incrementing `consumer_resubscribed_total`, without a full reconnect. **(R10-6, P1.4)** — *sequence with T62/T63.*
- **T62** `connection.go` + `topology.go`: redeclare broker-global topology **once per recovery event** at the `*Connection` level instead of per pooled `managedConn` (today `AttachTo` registers the hook on every conn → N×pool declares on broker restart). Keep `basic.consume`/`basic.qos` reissue per consumer connection. **Lens-02 (DS-09):** this compounds with DS-10 (T66) into a self-amplifying recovery storm; add a §6.1 note and have the chaos lane exercise a full-cluster restart against a just-recovered (possibly Khepri-quorum-forming) broker. **(R10-7, P1.2)** — *sequence with T61/T63; pulled into Phase 13 (v0.1).*
- **T63** `connection.go`: bound the synchronous reconnect barrier with a max duration; on cap, blocked publishers get `ErrReconnecting` instead of stalling indefinitely behind a half-alive broker (matters most with `PublishTimeout=0` + `context.Background()`). **Lens-01 (RMQ-17):** the cap must also cover **Khepri (4.1 default)**, where `queue.declare` is a Raft-quorum op that can block during partition recovery. **Lens-02 (DS-02):** SPEC must state `ConfirmTimeout` does **not** cover the barrier (the frame is still unwritten) → the cap is a *distinct* mechanism; name the option, its default, and the post-cap connection state (force-reconnect vs degraded). A half-alive-broker proxy test (accepts socket, stalls `queue.declare`) with `PublishTimeout=0`+`context.Background()` asserts `Publish` returns `ErrReconnecting` within the cap, not ∞; `goleak` clean. **(R10-8, P1.6)** — *sequence with T61/T62; pulled into Phase 13 (v0.1).*
- **T64** `topology.go`: quorum-queue structural validation in `validate()` — reject non-`Durable`, `Exclusive`, `AutoDelete` quorum queues with `ErrInvalidOptions`, and reject `x-max-priority` on quorum (via the `MaxPriority` field, already classic-gated at `topology.go:436`, **and** via a raw `Args["x-max-priority"]`). Require `Durable` on stream queues too. **Lens-01 (RMQ-20):** the `Queue.MaxPriority` field **does** exist in code (`topology.go:56`) — retire the stale "no such struct field" note and instead **document `Queue.MaxPriority` in SPEC §6.6** (spec/code drift). **(R10-9, P1.5)** — *coordinate with T58 (same `validate()`).*
- **T65** `topology.go` + `consumer.go` + `metrics/`: auto-declared `<source>.dlq` becomes durable (quorum-capable) with configurable bounds; Consumer mirrors the `Replier` missing-DLX validation — when `MaxRedeliveries>0` and the wired `Topology` has no DLX for the source queue, warn at `Build` and emit a drop metric so poison drops are observable. **(R10-10, P1.3)** — *builds on T47 (DLX binding), T52 (`dlq_depth`).*
- **T66** `connection.go` + `options_connection.go`: shuffle the `WithAddrs` list per connection and rotate addresses on reconnect, to avoid every client stampeding the same node on broker recovery. **Lens-02 (DS-10):** add a §6.1 note that this compounds with DS-09 (T62) into a recovery storm; the chaos lane asserts no `addr[0]` stampede on a full-cluster restart. **(R10-11, P2.1)** — *already pulled into Phase 12.*
- **T67** `connection.go`: define + implement `Dial` partial-pool-connect policy (succeed if ≥1 connection per role connects, supervisor reconnects the rest; or fail-fast — decision recorded in SPEC §6.1). **(R10-12, P2.2)**
- **T68** `topology.go` + `publisher_builder.go`: expose `x-alternate-exchange` (server-side catch-all for unroutable messages) on `Exchange`/topology, complementing per-publish `Mandatory()`+`OnReturn`. **(R10-13, P2.4)**
- **T69** `topology.go`: exchange-to-exchange bindings (`exchange.bind`) — extend `Binding` (or add a typed variant) so an exchange can be a binding destination, for layered fan-out topologies. **(R10-14, P2.3)**
- **T70** `connection.go` + `consumer.go` + `batch_consumer.go`: graceful-shutdown completeness — specify + handle prefetched-but-undispatched deliveries on `Close` (drain or nack-requeue, documented), and flush a `BatchConsumer`'s pending partial batch on `Close`/final `FlushAfter`. **Lens-02 (DS-03):** resolve the "drain or nack-requeue" choice to **nack-requeue (`requeue=true`)** the undispatched buffer before channel close (an at-least-once duplicate, acceptable under §6.2.1) — never drop (silent loss); add `consumer_shutdown_requeued_total`; gated by G2 (capture the current behaviour first). **(R10-15, P2.5)** — *pulled into Phase 13 (v0.1).*
- **T71** `metrics/` + `channelpool.go` + `consumer.go`: add channel-pool acquire-wait/saturation metric, a `consumer_in_flight{queue}` gauge, and a `consumer_redelivered_total{queue}` counter (redelivery ratio = leading instability signal). Coordinates with Phase 10 T50/T52/T53. **Lens-02 (DS-14):** `consumer_redelivered_total{queue}` is the redelivery-class duplicate-budget signal `publisher_retry_total` does not cover — required for the §1 "duplicate budget never invisible" claim to hold for the dominant duplicate source; assert it increments on a `Redelivered()` delivery. **(R10-16, P2.6)** — *pulled into Phase 13 (v0.1).*
- **T72** `options_connection.go` + `connection.go`: `net.Dialer` keepalive on the default dialer (document `TCP_USER_TIMEOUT` where available) so a write to a half-open socket fails promptly rather than relying on `ConfirmTimeout` as the only backstop. **(R10-17, P2.7)**
- **T73** `publisher.go` + `consumer.go`: wrap every `Codec.Encode`/`Codec.Decode` (and `HeaderCodec.EncodeWithHeaders`/`DecodeWithHeaders`) call in a `defer recover` that converts a recovered value into `ErrInvalidMessage` (T09 panic-safety contract, formalised as a trackable task). The consumer path already recovers its decode call (`safeDecodeConsumer`, which after T26 also routes `DecodeWithHeaders`); the publisher's `Encode`/`EncodeWithHeaders` call does **not** — close that gap so a panicking **user-supplied** codec cannot crash the publish goroutine. Built-in codecs (`NewJSON`/`NewJSONStrict`/`NewProtobuf`/`NewCloudEventsStructured`/`NewCloudEventsBinary`) must never panic by design; the recover is the net for third-party codecs, not a license for the built-ins. **(post-T24 review)**

**Checkpoint Phase 11:**
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` and the integration lane pass; `goleak.VerifyNone` clean.
- [ ] `Message.Delay`/`ExchangeKindDelayed` godoc carries the durability warning; SPEC §6.5/§6.6 reference a durable alternative (T57).
- [ ] Declaring a `QueueTypeQuorum` with `DeliveryLimit==0` logs the RabbitMQ-4.x-default-20 warning (T58).
- [ ] A regression test fails if the `basic.return` channel is buffered or the confirm/return demux is split across goroutines (T59).
- [ ] Handler timeout followed by a late `Ack`, and a double `Delivery.Ack` via `ConsumeRaw`, issue **no** second frame and do not close the channel (T60).
- [ ] Forcing a consumer-channel-only close (404/406, TCP still up) reopens + re-`basic.consume`s; `consumer_resubscribed_total` increments; consumer does not die silently (T61).
- [ ] A broker restart with an N-connection pool issues topology declares **once per recovery**, not N×pool (asserted via declare instrumentation/broker counters) (T62).
- [ ] Against an injected slow-`queue.declare` broker, a blocked `Publish` returns `ErrReconnecting` at the barrier cap instead of stalling past it (T63).
- [ ] Quorum queue that is non-`Durable`/`Exclusive`/`AutoDelete`/`x-max-priority` returns `ErrInvalidOptions` at `Declare`; "MaxPriority" validation reference reconciled (T64).
- [ ] Auto-declared DLQ is durable with bounds; a `Consumer` with `MaxRedeliveries>0` and no DLX in the wired `Topology` warns at `Build` and increments the drop metric (T65).
- [ ] `WithAddrs` order is shuffled per connection and rotates on reconnect (T66); `Dial` partial-pool policy behaves per SPEC §6.1 (T67).
- [ ] `x-alternate-exchange` (T68) and exchange-to-exchange bindings (T69) declare correctly (verified via `rabbitmqctl`/integration).
- [ ] `Close` drains/nack-requeues prefetched-but-undispatched deliveries; `BatchConsumer` flushes a partial batch on `Close` (T70).
- [ ] New observability metrics present and exercised (T71); default dialer sets keepalive (T72).
- [ ] A panicking fake codec injected into both the publisher and consumer paths surfaces `ErrInvalidMessage` (not a crash); the publisher `Encode` call is wrapped in `defer recover` (T73).
- [ ] SPEC §10 "Rev 10" decisions reflected; any per-task SPEC amendment landed in the same PR as its code.

### Phase 12 — Protocol-Correctness Re-review (Lens 01: RabbitMQ 3.13 + 4.x)

Goal: close the protocol defects from the Lens-01 adversarial spec
validation (`spec-validation/01-rabbitmq-amqp-protocol.md`, findings
`RMQ-01..RMQ-31`). A reconciliation against the in-progress implementation
showed several *spec* findings are already correct in code (the SPEC
drifted) while a documented feature (`at-least-once` dead-lettering) is
unimplemented and quorum queues have no structural validation. Owner
decisions (2026-05-28): implement `at-least-once` with forced
`reject-publish`; pull the four §1-violating defects (T60/T61/T65/T66) into
this phase; keep the async publish API in `LATER.md` LATER-34 (§9 wording
fixed only). Per-task SPEC amendments land in the same PR as the code
(CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 11" note records the pass
when the first task lands. **Differential 3.13-vs-4.x integration
assertions are required**, not just "suite passes". Gate task T74 runs first.

- **T74** verification gates (real broker, **3.13 and 4.x**): capture ground truth for G1 `x-death` delivery-limit reason atom (`delivery-limit` vs `delivery_limit`), G2 x-death `count` accumulation shape, G3 whether 4.x *classic* queues honour `x-delivery-limit`, G4 valid `{quorum, overflow, at-least-once}` declare permutations, G5 broker `max_message_size` defaults per version, G6 `prefetch_size!=0` reject-vs-ignore. Results table gates T75/T76/T58/T78/T80. **(Lens-01 gates, P0)**
- **T75** `internal/headers/xdeath.go` + integration: x-death delivery-limit reason token (RMQ-01). If G1 shows the broker emits `delivery_limit`, fix the matched atom and normalise `-`↔`_`; **replace the fabricated unit test** (`makeEntry(...,"delivery-limit",...)`) with a real-broker test driving a quorum `x-delivery-limit` dead-letter and asserting `DeathCount()` increments. SPEC §6.3 + decision 34. **(RMQ-01, P0; dep G1)**
- **T76** `topology.go`: implement `at-least-once` DLX strategy (decision 52, RMQ-05) — for quorum + DLX, inject `x-dead-letter-strategy: at-least-once` **and** force/validate `x-overflow=reject-publish` (auto-set with a warning, or `ErrInvalidOptions` on `drop-head`). Document the source-queue memory cost. SPEC §6.6 + decision 52. **(RMQ-05, P0; dep G4; coordinate T64/T65)**
- **T77** `publisher.go`: reject a `Mandatory()` `PublishBatch` containing duplicate explicit `MessageID`s with `ErrInvalidMessage` (RMQ-15) — replace the documented-trap comment at `publisher.go:689-694`. SPEC §6.2 + decision 14. **(RMQ-15, P1)**
- **T78** SPEC↔implementation reconciliation, **no behaviour change** (RMQ-02/03/04/14): align SPEC + code comments with the correct code — 311 permanent (§6.8 IsTransient godoc + PublishRetry trigger list); `DeathCount` "sum of the `count` sub-field" (§6.3); disk/memory alarm → `ErrConnectionBlocked`, not nack (§6.2, decision 20, `errors.go:38` comment); `PrefetchBytes` dropped client-side / broker **rejects** non-zero `prefetch_size` (§6/§6.3). Guard tests: 311-permanent + `Qos` size-arg == 0. **(RMQ-02/03/04/14, P1)**
- **T79** SPEC §6.8: annotate reply-code sentinels channel-level (311/403/404/405/406) vs connection-level (320/402/501–505/506/530/540/541); document the recovery implication (ties to T61). **(RMQ-18, P2)**
- **T80** SPEC §6.1: per-version `max_message_size` defaults (128 MiB 3.13 / 16 MiB 4.0+, per G5); "4096 is the AMQP-spec minimum; 131072 the default" (fix the stale "131072 is the minimum"). **(RMQ-12/13, P2; dep G5)**
- **T81** version-divergence docs (RMQ-17/19/21/23/30/31): Khepri caveat; CloudEvents 0-9-1⇄1.0 bridge version note + round-trip interop test (coord. lens 03); pin §9 verification to the management HTTP API instead of `rabbitmqadmin` CLI (v2 rewrite in 4.0); mirrored-queue staleness (§6.2); transient-queues-deprecated-feature note; mixed-version-cluster caveat. **(P2)**
- **T82** contract-precision SPEC fixes (RMQ-24/25/26/27/28/29): decision-17 default-"1" staleness; ack-vs-confirm wording (§6.2); sub-ms `Expiration`→`"0"` footgun (optionally reject `<1ms` non-zero, §6.5); Priority range + "quorum has no priority"; exclusive-reply-queue redeclare-on-reconnect (§6.7); prefetch-16 as guidance not a broker constant (§6.3). **Lens-02 (DS-07):** coordinate the ack-vs-confirm wording with Phase 13 T88's queue-type confirm-semantics table — single source, no contradiction. **(P3)**
- **T83** SPEC §9 throughput-honesty wording (RMQ-11): qualify 30k/100k with local-broker/sub-ms-RTT, document the `pool/RTT` ceiling + a remote projection, cross-reference LATER-34. **(RMQ-11, P2)**

**Pulled forward into this phase (definitions in Phase 11; each violates the §1 no-silent-failure bar):** T60 (double-ack guard, RMQ-08), T61 (channel-level recovery, RMQ-09 — couples with T62/T63), T65 (DLQ bounds + Consumer missing-DLX, RMQ-07; coordinate with T76), T66 (`WithAddrs` shuffle, RMQ-10).

**Sequencing:** T74 first → gates T75/T76/T58/T78/T80. T64 → T76 → T65 (shared `validate()`/DLX path). T60 → T61 (couples with T62/T63). Others independent.

**Checkpoint Phase 12:**
- [ ] T74 gate results documented; each downstream task cites its gate.
- [ ] x-death delivery-limit `DeathCount()` verified on a **real** 4.x broker; fabricated unit test replaced (T75).
- [ ] Quorum + DLX declares `at-least-once` with `reject-publish`; an invalid `drop-head` combo is rejected/auto-fixed (T76).
- [ ] Mandatory `PublishBatch` with duplicate `MessageID` returns `ErrInvalidMessage` (T77).
- [ ] SPEC matches implementation: 311 permanent, `DeathCount` sums `count`, alarm → `ErrConnectionBlocked`, `prefetch_size` always 0 (T78).
- [ ] Version-aware `DeliveryLimit==0` warning correct on **both** 3.13 and 4.x (T58); quorum structural validation + documented `Queue.MaxPriority` (T64).
- [ ] §1 silent-failure defects closed: T60, T61, T65, T66.
- [ ] §9 throughput numbers state their topology/RTT assumptions (T83); version caveats documented (T79/T80/T81/T82).
- [ ] Integration lane green on **both** RabbitMQ 3.13 and 4.x with differential assertions; `go build ./...` + `make lint` clean; `goleak.VerifyNone` clean.
- [ ] README "Available now / On the roadmap" synced where the contract changed (at-least-once feature, error classification, defaults).

### Phase 13 — Distributed-Systems Re-review (Lens 02: failure modes, consistency, ordering, duplicates)

Goal: bring `SPEC.md` and the v0.1 surface into honest alignment with the §1
reliability bar ("no silent loss / backpressure / duplicate / poison loop")
**under crash, partition, and clock-skew conditions** — not just the happy path.
Closes the Lens-02 adversarial spec validation
(`spec-validation/02-distributed-systems.md`, findings `DS-01..DS-17`; planning
brief `spec-validation/02-distributed-systems-plan.md`). The lens verdict was
**NO-GO for the §1 bar as written; GO-WITH-CHANGES** once the High findings land
(DS-02 unbounded barrier, DS-03 undefined `Close` loss-or-dup, DS-05 absent
partition modes, DS-06 SAC oversell, with DS-01/DS-04/DS-13 close behind).

Owner decisions (2026-05-28): (1) pull **T62/T63/T70/T71** forward into v0.1
(active §1 violations + the recovery storm + the redelivery-budget gap), mirroring
Phase 12's pull-forward; (2) stand up a **new `chaos` build tag** — a 3-node
RabbitMQ cluster with configurable `cluster_partition_handling` + a fault injector
(`toxiproxy`/`iptables`/`rabbitmqctl stop_app`) and a half-alive-broker proxy —
because a single-broker `testcontainers` lane cannot falsify DS-05/06/07/13; gate
it on the same 3.13-vs-4.x matrix; (3) build the **opt-in structured-error RPC
reply mode** now (DS-12, reverses part of decision 16) rather than deferring it;
(4) invest in **per-entity redeclare** (DS-08) rather than only documenting the
degraded-mode blast radius. Phase 13 therefore adds **no** `LATER.md` entries.
Per-task SPEC amendments land in the same PR as the code (CLAUDE.md "amend SPEC
first"); a SPEC §10 "Rev 12" note records the pass when the first task lands.
**Failure-mode claims are tested against a real broker / cluster, not a mock.**
Gate task T84 runs first and **no SPEC edit to an affected section lands before
its gate returns**.

- **T84** chaos lane + verification gates (real broker **and** 3-node cluster, **3.13 and 4.x**): introduce a `chaos` build tag + a `make test-chaos` target that stands up a 3-node cluster (configurable `cluster_partition_handling`), a fault injector, and a half-alive-broker proxy. Capture ground truth for **G1** SAC-failover reorder/duplicate with `Prefetch>1` (classic **and** quorum), **G2** the *current* `Close` behaviour for prefetched-but-undispatched deliveries (requeue or drop?), **G3** quorum publish pinned to a minority-partition node (hang/timeout/error + duplicate-on-heal), **G4** the client-side signal per `pause_minority`/`autoheal`/`ignore`, **G5** a poison crash-loop defeating process-local Counter B, **G6** the `Caller`'s handling of a second reply for an already-resolved `CorrelationID`. Results table (under `spec-validation/`) gates T86/T87/T88/T90/T92 and the pulled-forward T70. **(Lens-02 gates, P0)**
- **T85** `SPEC.md §6.2.1/§6.5` + `examples/idempotent_consume/`: rework the dedupe pattern (DS-01) — split **publish-retry** duplicates (bounded by outage+reconnect+retry → in-memory LRU OK) from **crash/requeue redelivery** duplicates (unbounded gap, and the crash that triggers redelivery wipes the in-memory cache → ~zero protection); state that handlers with external side-effects (DB/HTTP/payments) **require persistent dedupe**, not "paranoid", and recommend bounding queue residency with a TTL so a finite persistent retention window is definable. Fold **DS-15** (drop the "UUIDv7 makes eviction-by-recency trivial" non-sequitur — an `lru.Cache` evicts by access order, not the key's embedded timestamp; document that `MessageID`/`Timestamp` ordering is **per-publisher wall clock**, not global, not monotonic across NTP steps; only ID *uniqueness* is load-bearing) and **DS-16** (name the forced-close duplicate window: ctx-deadline force-close abandons in-flight handlers → redelivery). Ship a persistent (Redis/DB) `examples/idempotent_consume/` variant + a chaos test that crashes the consumer mid-handler and asserts the persistent path dedupes the redelivery while the in-memory path does not. **(DS-01/DS-15/DS-16, P0; dep T38b)**
- **T86** `SPEC.md §6.1` + `README.md`: new partition-handling-modes subsection (DS-05) documenting the client-side observation per `pause_minority`/`autoheal`/`ignore` (per G4), with an explicit **§1 carve-out** that under `ignore` (split-brain) acked messages can be lost silently on heal — mirroring the R10-1 delayed-message carve-out template. Recommend against `ignore`; recommend `pause_minority` + `WithAddrs` failover. Chaos test asserts the client sees a classifiable reconnect under `pause_minority`/`autoheal`. **(DS-05, P0; dep G4)**
- **T87** `SPEC.md §6.3/§6.6` + decision 30 + `examples/ordered_consume/`: qualify SAC (DS-06) — drop "strict ordering with failover"; state that per-channel ordering holds **steady-state only**, and at the failover boundary up to `Prefetch` messages from the dead active consumer are redelivered (duplicates) and may reorder relative to messages published during the gap. Recommend `Prefetch(1)` with SAC when cross-failover order matters (reduces, never eliminates). `examples/ordered_consume/` README states the caveat. G1 chaos test asserts the documented reorder/duplicate behaviour per queue-type per broker-version. **(DS-06, P0; dep G1, T38c)**
- **T88** `SPEC.md §6.2`: queue-type confirm-semantics table (DS-07) — **quorum** = confirm after Raft majority-commit (understated today → users add redundant dedupe); **classic durable+persistent** = after fsync/batch; **transient/non-durable** = immediate, no durability. Plus name the **quorum minority-partition** window (per G3): no quorum → publish unconfirmed → `ErrConfirmTimeout` → `PublishRetry` → duplicate on heal — tie to DS-05. **(DS-07, P1; dep G3)** — *coordinate with T82 (Lens-01 RMQ-25): merge the ack-vs-confirm wording, do not duplicate.*
- **T89** `connection.go` + `topology.go` + `SPEC.md §6.1` + decision 28: per-entity redeclare (DS-08) — on a genuine durable-definition conflict, fail only the publishes routing to the conflicting entity instead of degrading the whole role's publish path; document that `ForceReconnect()` is ineffective for non-transient conflicts. **(DS-08, P1)** — *sequence after T62 (shared redeclare path).*
- **T90** `rpc.go` + `SPEC.md §6.7` + `metrics/`: RPC orphan-reply handling (DS-11) — the `Caller` discards a reply whose `CorrelationID` has no pending entry (emitting a metric/log), UUIDv7 `CorrelationID`s are never reused so a late reply cannot bind to a subsequent `Call`, and in `UseExclusiveReplyQueue()` mode the orphan is ack-and-dropped (not left unacked). G6 chaos test (Replier publishes-confirms then crashes before ack → second reply) asserts the orphan does not resolve/disturb a concurrent `Call`. **(DS-11, P1; dep G6)**
- **T91** `rpc.go` + `SPEC.md §6.7` + decision 16: opt-in structured-error RPC reply mode (DS-12) so a deterministic `Replier`-handler rejection is distinguishable from timeout/loss at the `Caller`, instead of collapsing all three into `ErrCallTimeout`. Document that without it callers MUST NOT blind-retry without idempotency + a bounded budget (the non-converging re-run-and-re-DLX hazard). Revises part of decision 16. **(DS-12, P1)**
- **T92** `SPEC.md §1/§6.3/§9`: poison Counter-B crash-loop honesty (DS-13) — state that Counter B (process-local, resets per restart) does **not** bound a poison message that crashes the consumer process; the only crash-safe bound is quorum `x-delivery-limit`. Downgrade the §1 "no silent poison loop" + §9 "at most `MaxRedeliveries+1` deliveries" claims to "per-process-lifetime, classic-queue; crash-safe only with quorum `x-delivery-limit`". G5 chaos test demonstrates the unbounded reprocessing; a quorum counterpart shows the broker-side bound holds across restarts. **(DS-13, P1; dep G5)** — *coordinate with T58 (version-aware delivery-limit).*
- **T93** `SPEC.md §6.2` + decision 43: `PublishBatch` order-under-retry caveat (DS-17) — the input-order guarantee holds only absent a mid-batch channel close; a caller-retried slot (decision 43) loses its position, so callers needing order must re-publish the whole batch, not just failed indices. **(DS-17, P3)** — *may ride T85.*

**Pulled forward into this phase (definitions in Phase 11; each is an active §1 gap in v0.1):** T62 (redeclare de-amplification, DS-09 — recovery storm at scale), T63 (reconnect-barrier cap, DS-02 — unbounded silent stall), T70 (`Close` undispatched nack-requeue, DS-03 — undefined loss-or-dup), T71 (`consumer_redelivered_total`, DS-14 — invisible redelivery budget). T60 (DS-04) and T66 (DS-10) were already pulled into Phase 12.

**Sequencing:** T84 first → gates T86/T87/T88/T90/T92 + T70. The reconnect trio T61→T62→T63 stays sequenced even when pulled forward; T62 → T89 (shared redeclare path). T85 dep T38b; T87 dep T38c. T88 coordinates with T82; T92 coordinates with T58. T91 and T93 are independent. SPEC sub-phasing: (A) T84; (B) T63, T70; (C) T86, T88; (D) T87, T92; (E) T85, T60, T90, T93; (F) T62, T66, T71, T89, T91.

**Checkpoint Phase 13:**
- [ ] T84 chaos lane (`make test-chaos`: 3-node cluster + fault injector + half-alive proxy) green on 3.13 **and** 4.x; gate results table committed; each downstream task cites its gate.
- [ ] Unbounded-barrier silent stall closed (DS-02/T63): half-alive-broker test shows `Publish` returns `ErrReconnecting` within a stated cap under `PublishTimeout=0`+`context.Background()`; SPEC states the cap, its default, the post-cap state, and that `ConfirmTimeout` does not cover the barrier; `goleak` clean.
- [ ] `Close` undispatched behaviour defined + tested (DS-03/T70): undispatched deliveries nack-requeued (not dropped) per G2; `consumer_shutdown_requeued_total` exists; `BatchConsumer` flushes its partial batch.
- [ ] §6.1 partition-handling-modes subsection (DS-05/T86) documents the client signal per `pause_minority`/`autoheal`/`ignore` (per G4) and carves `ignore` out of the §1 no-loss bar; README reliability copy updated.
- [ ] §6.2 queue-type confirm-semantics table (DS-07/T88) + the quorum minority-partition timeout→retry→duplicate window (per G3); no contradiction with T82's ack-vs-confirm wording.
- [ ] SAC no longer described as "strict ordering with failover" without qualification (DS-06/T87); G1 chaos test asserts the documented reorder/duplicate per queue-type per version; `examples/ordered_consume/` README states the caveat.
- [ ] §1/§9 no longer claim a crash-safe poison bound for classic queues (DS-13/T92); G5 crash-loop test demonstrates the unbounded reprocessing and the quorum counterpart shows the broker-side bound holds.
- [ ] §6.2.1 distinguishes publish-retry vs crash-redelivery duplicate classes (DS-01/T85), states in-memory dedupe is crash-unsafe, ships a persistent-dedupe example, corrects the UUIDv7 eviction wording (DS-15), and names the forced-close duplicate window (DS-16).
- [ ] RPC orphan-reply handling specified + tested (DS-11/T90, per G6); opt-in structured-error reply mode lands (DS-12/T91); the double-verdict no-op guard (DS-04/T60) carries the §6.3 wording + a `ConsumeRaw` double-call test.
- [ ] Per-entity redeclare lands (DS-08/T89): a single conflicting durable def fails only its entity's routes, not the whole role; `ForceReconnect()` documented as ineffective for genuine conflicts.
- [ ] Recovery-storm de-amplification (DS-09/T62) + `WithAddrs` shuffle (DS-10/T66) + `consumer_redelivered_total` (DS-14/T71) landed; the chaos lane exercises a full-cluster restart.
- [ ] `PublishBatch` order-under-retry caveat documented (DS-17/T93).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the new chaos lane green; `goleak.VerifyNone` clean.
- [ ] README "Available now / On the roadmap" + reliability copy synced (partition carve-out, SAC caveat, `consumer_redelivered_total`, structured-error RPC mode).
- [ ] SPEC §10 "Rev 12" note records the Lens-02 pass; no duplicate tasks created (T60/T62/T63/T66/T70/T71/T82 amended in place); no new `LATER.md` entries.

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
