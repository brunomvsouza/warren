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
    opt-in high-cardinality labels (enabled at Prometheus-metrics
    construction; see SPEC §6.9), and histogram buckets.
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
8. No mock package is shipped; `Delivery`/`Batch` fixtures are public root constructors; the testcontainers helper is `internal/amqptest`.
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
- **T04** `metrics/` package: three interfaces + `NoOp` + Prometheus with bounded default labels, opt-in high-cardinality labels (`MetricsLabelRoutingKey`/`MetricsLabelMessageType`, enabled at metrics construction — see SPEC §6.9), configurable latency buckets, mandatory metrics (`connection_reconnects_total`, `consumer_resubscribed_total`, `consumer_handler_aborted_channel_closed_total`, `publisher_in_flight`, `connection_blocked_seconds`)
- **T05** `otel/` package: `Tracer` interface + NoOp default + AMQP header propagator following OTel Messaging semantic conventions **v1.27.0+**
- **T06** `internal/reconnect` supervised reconnect loop + `RetryPolicy` (exponential + jitter)
- **T07** `connection.go`: single-TCP `Dial` + `Health` (verifies socket+topology) + `Close(ctx)` **with graceful cascade** (cancel consumers → wait handlers → wait confirms → close TCP); validates `ChannelPoolSize ≤ channel-max`; blocked-connection semantics; **synchronous reconnect barrier** (channel re-open → topology redeclare → consumer re-`basic.consume` → user `WithOnReconnect`) with `Publish` blocking on `ErrReconnecting` until step 2 completes; **degraded-state machine** on persistent redeclare failure (`ErrTopologyRedeclareFailed`, `WithOnTopologyDegraded`, `connection_degraded_total`, `(*Connection).ForceReconnect()` operator helper, `topology_redeclare_seconds{role}` histogram); **`Connection.Logger()` removed** from public API; **`(*Connection).AuthenticatedUser()` populated by `Dial` before it returns** and exposed for `UserID` client-side validation; **`Dial` validates SASL EXTERNAL** fail-closed (TLS + client cert + `amqps://`); **`Dial`-time warning** when `WithPublisherConnections(1)` or `WithConsumerConnections(1)`; **`frame_max < 4096` rejected** with `ErrInvalidOptions`. **Lens-08 (CR-03/CR-11):** amend §6.1 to describe the *actual* ctx-cancellable barrier mechanism (a `sync.Cond` woken by a per-Wait `ctx`-watcher goroutine, or a channel-broadcast barrier) instead of the impossible-as-worded "condition variable cancellable via `ctx`" (`connection.go:43/597-604`); bound/pool the per-Wait-iteration watcher so a reconnect storm with K blocked publishers does not spawn K×iterations goroutines; document `ForceReconnect` is idempotent/coalesced (cap-1 channel) and safe during an in-progress reconnect. A test asserts a ctx-cancel during the barrier returns `ErrReconnecting` with no goroutine leak. *design/doc — no live gate.*
- **T07b** `internal/amqperror`: translates `*amqp091.Error` → wraps of reply-code sentinels; powers `AMQPCode` and the classifiers. **`IsTransient(506)` returns false** (resource-error classified as permanent by default).
- **T07c** `internal/redact`: AMQP URI userinfo redaction; mandatory choke-point used by `log/`, error formatting, metric labels, and span attributes; ships a `FuzzRedactURI` fuzz target exercising malformed URI inputs (added to plan §"Fuzz targets")
- **T07d** Multi-TCP fan-out: extend `Connection` to wrap a role-split pool of TCP connections (`WithPublisherConnections(n)` default **2**, `WithConsumerConnections(n)` default **2**); each TCP connection gets its own `internal/reconnect` supervisor; consumer pinning by stable hash of consumer-tag with **`ctag-<uuidv7>` auto-generation** for defaulted tags (so multi-conn fan-out works out-of-the-box); `Dial` warns when either count is 1
- **T08** `channelpool.go`: per-publisher-TCP-connection channel pool with `ErrChannelPoolExhausted` sentinel for ctx-cancel under exhaustion. **Lens-08 (CR-08):** document that pool `Acquire` is best-effort, **not FIFO** (Go channel receive has no waiter ordering, `channelpool.go:57`), so a waiter can starve under *permanent* exhaustion; recommend sizing the pool to peak concurrency; a FIFO wait queue is deferred. The dead-channel liveness guard (`channelpool.go:80,122`) is do-not-regress. *dep CG-6.*

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
- **T10** `message.go`: `Message[M]` struct with `ContentType` + `ContentEncoding` separated; default-apply logic (UUID v7, timestamp, persistent, ContentType ← codec.ContentType); Headers field-table typing validation; field godoc covering `Priority` range/clamp, `Expiration` wire format, `Headers` typing per SPEC §6.5. **Lens-09 (PC-09):** call `uuid.EnableRandPool()` once at init to batch the per-publish `crypto/rand` reads, and document the google/uuid **process-global `timeMu` lock** taken per UUIDv7 (a process-wide serialization point at the billions/day bar) + that `MessageID` is load-bearing for at-least-once dedupe so it cannot be skipped; an `AllocsPerRun` guard asserts the per-call entropy buffer is gone (coordinated with T148's combined guard). dep PG-1. **Lens-12 (DX-12):** the load-bearing `Message[M]` warning must live in the **call-site godoc**, not only in SPEC — add the **`UserID` 406-channel-close footgun** (§6.5 L1487-1513: a `UserID` that is not the authenticated connection user makes the broker close the channel) and the **`DeliveryMode` Persistent-is-the-zero-value-default** note (§6.5 L1450/L1554-1556) to the mandated field godoc, so a first-hour reader hovering the field sees the hazard without opening SPEC. dep XG-1.
- **T11** `internal/confirms`: publisher-confirm tracker handling ack, nack (→ `ErrPublishNacked`), return-then-ack (→ `ErrUnroutable` **wrapped with the originating `basic.return` reply code 312/313** so `AMQPCode(err)` returns it), and channel-close (→ `ErrChannelClosed`); per-channel tracker; `multiple=true` semantics. **Lens-09 (PC-06 + PC-11):** two hot-path fixes — (PC-06) pool/reset the per-`Wait` `time.Timer` (the default `ConfirmTimeout=30s` arms a timer on every publish and every batch element, `tracker.go:171`); (PC-11) replace the `resolveUpTo` whole-map scan + `slices.Sort` on every `multiple=true` frame under `t.mu` (`tracker.go:219-230` — O(outstanding)/frame, not the O(resolved) the §6.2 "single pass … critical for high-throughput batching" wording implies) with a contiguous confirmed low-water-mark + an ordered index (or min-heap) → O(resolved + log n), and amend §6.2 to state the real complexity; both land with an `AllocsPerRun`/microbench guard, the one-shot resolve / `Wait`-self-delete / `CloseAll` mechanism stays do-not-regress. dep PG-1, PG-3.
- **T12** `publisher.go` + `publisher_builder.go`: `PublisherFor[M]` builder (no `Immediate()` — RabbitMQ rejects it) + `Publish` (sync-confirm, concurrency-safe, channel acquired from publisher pool) + `Close(ctx)` drain
- **T13** Mandatory + `OnReturn(Return)` with rich `ReturnedProperties` + `ConfirmTimeout` **(default 30 s)** + `PublishTimeout` (end-to-end cap including pool acquire / blocked-wait / reconnect barrier) + **`PublishBatchMaxSize`** (renamed from `PublishBatchMaxInFlight`) + `PublishRetry(p)` (retries only `IsTransient` errors, **emits `publisher_retry_total{exchange, reason}`** on each retry) + `StampUserID()` builder + **client-side `UserID` validation** in `Publish` → `ErrInvalidMessage` on divergence + **functional-options last-wins** asserted on `PublisherBuilder` (per SPEC §6.1 line 515; covers `.Metrics`/`.WithoutMetrics`/`.Tracer` chains) + `ErrUnroutable` + `ErrConfirmTimeout` + `ErrPublishNacked` + `ErrConnectionBlocked` on ctx-cancel while broker-blocked + `ErrReconnecting` on ctx-cancel while reconnect barrier holds. **Lens-08 (CR-01):** the `OnReturn` timing wording must also **name the goroutine it runs on** — today it fires inline on the single unbuffered-return demux (`publisher.go:226`), a connection-reader path; the cross-cutting callback-invocation-goroutine contract + the dispatch-vs-doc decision live in **T144** (cited here).
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

- **T17** `delivery.go`: concrete `Delivery[M]` struct + `x-death` header parsing + `DeathCount()` (sums entries with `reason ∈ {rejected, delivery-limit}` — `expired` and `maxlen` reflect broker policy, not handler-driven rejection) + new `DeathCountByReason(r string) int` + `DeathReasons() []string`; `Ack`/`Nack`/`AckIf` return `ErrChannelClosed`/`ErrAlreadyClosed` paths documented. **Lens-09 (PC-08):** allocate the x-death `byReason` map lazily — `make(map[string]int)` runs before the `tbl==nil` / x-death-absent / `![]any` early returns today (`xdeath.go:32`), so a map is allocated on 100 % of deliveries including the common no-DLX path; move the alloc after the early returns → zero map alloc on the no-DLX delivery; `AllocsPerRun` guard. dep PG-2.
- **T18** `consumer.go` + `consumer_builder.go`: `ConsumerFor[M]` builder (no `NoLocal()`; `ChannelQoS()` instead of `GlobalQoS()`; `PrefetchBytes()` documented as no-op on RabbitMQ; new `Priority(int)` for `x-priority`; new `HandlerTimeout(d)` for per-message ctx deadline; new `HandlerTimeoutVerdict(TimeoutVerdict)` default `TimeoutNackNoRequeue`; **functional-options last-wins** asserted on the builder per SPEC §6.1 line 515) + `Consume` **(with non-blocking dispatcher)** + handler error mapping + **re-subscribe loop** that, after a successful reconnect of the consumer TCP connection, reopens the channel, reapplies `basic.qos`, and reissues `basic.consume` exactly once per active consumer; **handler ctx cancel** with cause `ErrChannelClosed` on mid-handler channel close; **default consumer-tag = `ctag-<uuidv7>`** generated at `Build` for multi-conn fan-out; **`Build`-time warning** when `Prefetch < Concurrency`. **Lens-08 (CR-06):** amend §6.3 to state the non-blocking dispatcher's **sole** bound is `prefetch` (the `out` channel is prefetch-sized, `consumer.go:487`); there is **no second queue**; the reader blocks when the buffer is full (that *is* the backpressure); and `basic.cancel`/channel-close stay observable via the closed deliveries channel even when all `n` handler slots are busy. A test asserts the dispatch buffer == prefetch and that `basic.cancel` is observed with all slots busy. **Lens-09 (PC-10):** amend §6.3 to state the consume scaling lever — one consumer = one channel = one reader on one TCP, so beyond the per-TCP I/O ceiling raising `Concurrency` alone does not add throughput; scale needs more consumer channels/connections (`WithConsumerConnections` / more consumers). (The new §9 consume-side throughput target + latency SLO is the capstone T149.)
- **T18b** `HandlerTimeoutVerdict` matrix test: configure `HandlerTimeout(50ms)` with each of the two verdicts; assert (1) `TimeoutNackNoRequeue` lands the timed-out message in DLX; (2) `TimeoutNackRequeue` requeues subject to `MaxRedeliveries` / `x-delivery-limit`
- **T19** `ConsumerMetrics` interface + Prometheus impl + wired into `Consume`; mandatory metrics include `consumer_resubscribed_total`, `consumer_handler_aborted_channel_closed_total`, `consumer_handler_seconds` (histogram)
- **T20** `MaxRedeliveries` enforcement: **two-counter design** with quorum-queue carve-out. (A) `x-death`-based ceiling for DLX-bounce loops (cross-process); (B) in-process counter keyed by **`(channel-instance-id, MessageID)`** for `ErrRequeue` loops (process-local, resets on consumer restart and on channel close). Either ceiling escalates to `Nack(false)` + `ErrMaxRedeliveries`. When the source queue is `QueueTypeQuorum` with `DeliveryLimit>0`, counter B is auto-disabled (broker is authoritative); metric label `cause=delivery-limit|x-death|in-process` distinguishes the three paths. Required because AMQP 0-9-1 only writes `x-death` on dead-letter events, not on `Nack(requeue=true)`. **Lens-08 (CR-02, Blocker):** counter B's `load`→`Store` (`consumer.go:767→782`) is a **non-atomic read-modify-write** with no lock between — under `Concurrency(n>1)` two handler goroutines processing redeliveries of the same key both read then both write, losing an increment, so `MaxRedeliveries` undercounts and a poison message loops past its limit. Because `sync.Map` is memory-safe, `go test -race` **cannot** catch this logical lost-update, so the existing "must be race-free; verified with `go test -race`" acceptance is a **false guarantee**. Make the RMW atomic (per-channel mutex held across load-increment-store, or a lock-striped map keyed by `counterBKey`); amend §6.3/decision 12 to "**atomic** read-modify-write" and note `-race` proves memory-safety, not lost-update freedom; **replace** the `-race`-only check with a **behavioural** N-goroutine-same-key test asserting the final count == N and `MaxRedeliveries` enforced exactly. *dep CG-1.*
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

- **T22** `Publisher.PublishBatch(ctx, []Message[M]) ([]PublishResult, error)` + `ErrPartialBatch` + **`ErrBatchTooLarge`** when the call exceeds `PublishBatchMaxSize` (default 1024). All N publishes pipeline on a **single channel** to preserve per-channel input order. Per-message failure fixture uses **client-side `ErrInvalidMessage`** (unsupported `Headers` type) so the channel stays open across the batch — body-too-large /311 would close the channel mid-batch and corrupt the always-all contract. **Documented contract:** `PublishRetry` does NOT apply to `PublishBatch`; per-message `ErrChannelClosed` cannot distinguish "broker persisted" from "broker did not receive", so retry is the caller's problem (chunking + dedupe-by-`MessageID`). **Lens-09 (PC-13):** document the `PublishBatchMaxSize=1024` memory/throughput trade-off + sizing guidance (a deeper window = more pipelining vs more tracker memory held per call); the per-call cap is decision 31 (not a sliding in-flight window) — do not reopen it.
- **T23** `batch_consumer.go` + `batch_consumer_builder.go` + concrete `Batch[M]` struct + auto-Ack/Nack with `multiple=true` on the highest delivery-tag in the batch; handler-explicit acking via `Batch.Deliveries()` suppresses the auto-verdict (idempotent guard inside `Batch`); `HandlerTimeout(d)` applies to the whole batch with default verdict `Nack(false)` on timeout
- **T23b** Checkpoint examples `examples/batch_publish/main.go` and `examples/batch_consume/main.go` (SPEC §7 + Rev decision 49): both `package main` reading `AMQP_URL`. `batch_publish/` demonstrates `PublishBatch` always-all + `[]PublishResult` interpretation + `ErrBatchTooLarge` guard. `batch_consume/` demonstrates `BatchConsumerFor[M]` with `Size(100)` + `FlushAfter(1s)` + auto-`multiple=true` ack on nil handler return. CI build (unit lane) + smoke-run (integration lane).

**Checkpoint Phase 5:**
- [x] `PublishBatch` of 1000 JSON messages: zero loss, single
      confirm-window round-trip. *(integration `TestPublishBatch_AlwaysAll_integration`;
      unit `TestPublishBatch_AllSuccess`.)*
- [x] `PublishBatch` of 2000 messages with default `PublishBatchMaxSize=1024`:
      returns `ErrBatchTooLarge` immediately, empty result slice; no
      channel work performed. *(`TestPublishBatch_ErrBatchTooLarge` +
      `TestPublishBatch_ErrBatchTooLarge_integration`.)*
- [x] `PublishBatch` preserves input order: deliveries on the consumer
      side arrive in the same order they were published (single-channel
      guarantee). *(`TestPublishBatch_SingleChannelOrdering` +
      `TestPublishBatch_OrderPreservation_integration`.)*
- [x] `BatchConsumer` flushes on `Size(N)` reached.
- [x] `BatchConsumer` flushes on `FlushAfter(d)` even if size <N.
- [x] `BatchConsumer` handler returning nil: single `basic.ack` frame
      with `multiple=true` for the highest delivery-tag (verified via
      channel recorder). *(`batch_consumer_autoack_test.go`.)*
- [ ] Per-message benchmark report: `Publish` baseline vs
      `PublishBatch` throughput (must be at least 5× faster on local
      broker). *(Deferred by design to **T44b** — throughput-benchmark
      suite; the `5×` gate is owned there, `deps: Phases 1–5, T37`.)*
- [x] **Example(s):** `examples/batch_publish/main.go` and
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
options surface, and the `Delivery`/`Batch` test fixtures +
`internal/amqptest` broker helper ready.

- **T32** TLS: `WithTLSConfig` + amqps:// integration test (testcontainer with self-signed cert from `amqptest/certs`)
- **T33** Cluster failover: `WithAddrs` + round-robin reconnect ordering
- **T34** Remaining Connection options: `WithVHost`, `WithAuth`, `WithHeartbeat` (godoc carries the SPEC §6.1 sizing tiers; `WithHeartbeat(0)` emits a `Dial`-time warning "heartbeats disabled — strongly discouraged"), `WithChannelMax`, `WithFrameMax` (godoc carries the SPEC §6.1 small/streaming/hard-max sizing table + the 100 MiB pointer-out), `WithDialer`, `WithClientProperties`, **`WithConnectionName`** (default `<binary>-<hostname>-<pid>`; role and index suffixed per TCP connection), **`WithPublisherConnections`**, **`WithConsumerConnections`**, **`WithOnResubscribe(func(queue string))`**. **Lens-12 (DX-13):** route the caveat to the **call-site godoc** — add the **`ConfirmTimeout(0)`-discouraged** (§6.2 L972-978), **`PrefetchBytes()`-no-op-on-RabbitMQ** (§6.3 L1145), and **`ChannelQoS()`-per-channel-semantics** (§6.3 L1146) caveats, today SPEC prose / code comments only, to the godoc on the exported option/builder method, so they are visible where the user sets them. dep XG-1.
- **T34b** **SASL EXTERNAL** via `WithSASLMechanism(SASLExternal)`: integration test against a RabbitMQ testcontainer with `rabbitmq_auth_mechanism_ssl` plugin enabled + `external_auth` user; asserts `WithAuth(user, pass)` becomes a no-op under EXTERNAL and emits a warning log at `Dial`. **Fail-closed `Dial` validation** test matrix: (a) no `WithTLSConfig` → `ErrInvalidOptions`; (b) TLS config without client cert → `ErrInvalidOptions`; (c) plain `amqp://` scheme → `ErrInvalidOptions`; (d) full valid config → success
- **T34c** **Panic isolation for user-provided callbacks**: every user callback must be wrapped in `recover()` and every pure-notification callback must run in a goroutine so a slow or panicking handler cannot block internal event loops. Five sites identified: (1) `WithOnBlocked` — move to a goroutine + recover (currently inline in the `supervisor` event-select; a slow/panicking callback delays broker unblock detection); (2) `WithOnReconnect` — add recover around the inline call (currently before `barrierCond.Broadcast()`; a panic permanently deadlocks all Publishers); (3) `WithOnTopologyDegraded` — add recover inside the existing goroutine (panic crashes the process); (4) `Handler[M]` / `RawHandler[M]` dispatch — add recover in both inline and goroutine paths (panic should nack without requeue, not crash the consumer goroutine); (5) `BatchHandler[M]` — same recovery wrapper as (4). **Lens-08 (CR-05):** the five sites cover user *callbacks* but not the **infra-goroutine boundaries** nor `OnReturn`. Add a `recover` (a) in `reconnect.Loop.run` around the user `connect` fn (`internal/reconnect/loop.go:72`; today a panic crashes the whole process — `defer close(l.done)` runs but there is no recover) and (b) on the supervisor / `runBarrier` around the resubscribe hook (`consumer.go:357` / `batch_consumer.go:190`; today a panic kills the supervisor → reconnect silently disabled for that socket); plus the missing `OnReturn` recover (CR-01, owned by T144). A panic must **degrade the socket** (`WithOnTopologyDegraded` + a metric), never crash the process or silently disable reconnect. *dep CG-5.*
- **T35** `AutoAck()` opt-in + verbose godoc warning + integration test that documents the trade-off
- **T36** Remaining consumer options: `Exclusive`, `Args`, `OnCancel(func(reason string))` for `basic.cancel` (consumer goroutine returns `ErrConsumerCancelled`; mandatory metric `consumer_cancelled_total{queue, reason}`), `Tag` (note: no `NoLocal()` — RabbitMQ silently ignores it; explicitly omitted per SPEC §6 and §10.10; defaulted tags use `ctag-<uuidv7>` per T18)
- **T37** **(SHIPPED, amended by maintainer review 2026-05-30 — see `tasks/todo.md` T37 note):** the public `amqpmock` gomock subpackage was **dropped** (unused in-repo; exporting it forced `go.uber.org/mock` on consumers — they generate their own mocks) and `amqptest` was **moved to `internal/amqptest`** (warren's own test infra, not a public commitment). The one fixture only the library can build stays **public in the root**: `warren.NewDeliveryFixture` / `NewBatchFixture`. The original plan text below is kept for provenance. ~~`amqpmock/` subpackage: `go generate` for interface mocks + hand-written `NewDelivery[M]` / `NewBatch[M]`. Also **`amqptest/`** subpackage exported:~~ testcontainers helper supporting **three plugin-enablement modes** ((1) pre-baked image via `AMQPTEST_IMAGE`; (2) mounted `.ez` file via `AMQPTEST_DELAYED_PLUGIN_FILE`; (3) `t.Skip` fallback via `amqptest.RequireDelayedExchange(t)`); ships `rabbitmq_auth_mechanism_ssl` + pre-generated TLS certs in `amqptest/certs/`; ships `Dockerfile.amqptest` under `amqptest/docker/` for downstream consumers. **Lens-06 (GA-09):** ship a **lightweight `Delivery[M]`/`Batch[M]` fixture path with no `go.uber.org/mock` dependency** (e.g. `DeliveryFixture`/`BatchFixture` constructors guarded against unkeyed struct literals) so consumer/raw/batch unit tests can fabricate deliveries without importing the gomock-heavy mock subpackage. **Lens-10 (TV-08):** guarantee the `rabbitmq_delayed_message_exchange` plugin is present in ≥1 **required** lane (the pre-baked-image mode `AMQPTEST_IMAGE`), so the delayed-exchange conformance/example criteria do not silently `t.Skip` green — the three plugin-enablement modes stay, but the required lane must use the present-plugin mode and **fail (not skip)** if the plugin is expected but missing. *dep VG-1.*

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
- [x] `warren.NewDeliveryFixture[Order](DeliveryFixture[Order]{…})`
      constructs a usable `*Delivery[Order]` for unit tests (public root
      fixture; no mock package — maintainer review 2026-05-30).
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
- **T39** README quickstart + links to every example. **Lens-12 (DX-04/DX-14):** the first code a user copies must compile and be honest — the quickstart must (a) **compile** against the value-typed `Handler[M]` (today's `func(ctx, o *Order)` does not build), (b) **not swallow errors** (replace every `_ =`/`pub, _ :=` with real handling), (c) actually cover the §9 four (TLS / multi-addrs / OTel / DLX, L2442-2443), and (d) hit a stated minimal-line target — or soften the §1 `<10/<15` claim (L94-97) if the honest minimum exceeds it (D6); the snippet-compile gate (XG-3, T160) is the lock. dep XG-3.
- **T40** CHANGELOG (Keep a Changelog format) + final godoc pass. **Lens-12 (DX-17):** the final godoc pass also asserts the **footgun→godoc routing table** (XG-1, T159) is fully satisfied — every load-bearing warning appears verbatim at its call site — and `CHANGELOG.md` actually exists for pkg.go.dev users (§4 L179 / §8 L2343 mandate a file that does not exist yet). dep XG-1.
- **T41** Coverage gate: ≥80% per package; ≥95% on `internal/reconnect`, `internal/confirms`, `channelpool`, **`internal/amqperror`**, **`internal/redact`** (Rev 7: amqperror + redact added per SPEC §9 line 2107–2109). **Lens-10 (TV-03):** make the coverage floor **enforced**, not reported — add a hard fail-under (same per-package / critical-path thresholds) so a drop below the floor **fails CI**, upload the `-coverprofile` as an artifact, and post the PR comment §7 (L2235/L2239) + §9 (L2491) already promise ("enforced in CI"); today `go test -race -cover` discards the number. *dep VG-2.*
- **T42** CI: `.github/workflows/ci.yml` (lint + unit + integration + conformance) + Go matrix (1.23, 1.24). **Lens-10 (TV-03/07/13):** the workflow must actually implement what its scope names — (TV-07) `-race` on **both** Go 1.23 **and 1.24** (the matrix row is named but unimplemented; CI runs 1.23 only); (TV-03) wire the coverage fail-under + `-coverprofile` artifact + PR comment (with T41); (TV-13) add an **integration-broker-required guard** that **fails** the required integration job if **zero** integration tests actually ran (today they `t.Skip` green when `AMQP_TEST_URL` is unset). *dep VG-1/VG-2/VG-3.*
- **T43** Release tooling: `.github/workflows/release.yml` running `gh release create --generate-notes` on tag push (no goreleaser per Op decision §5)
- **T44** Conformance tests: AMQP 0-9-1 wire-protocol assertions (separate `conformance/` directory); includes broker-nack path (`x-overflow=reject-publish`), `basic.qos.global=true` for `ChannelQoS()`, `x-delivery-limit` on quorum queues, mandatory return/ack correlation. **Lens-10 (TV-06/08/11):** (TV-06) add the **stub-vs-real-broker contract matrix** (brief §12) as a §7 artifact and **drop the "test AMQP server stub" language** for v0.1 — a stub cannot prove the real-broker-only contracts (R10-3 ordering, R10-2 quorum limit, `x-death` tokens/cycle-detection, broker-nack, `basic.cancel`, 406-on-`UserID`, `reject-publish-dlx`, direct-reply-to); conformance is **real-broker-only** for v0.1 (D5); (TV-08) the delayed-exchange conformance criteria run against a plugin-enabled image in a required lane (coord T37) or are marked conditionally-verified and **fail, not skip**, when the plugin is expected but missing; (TV-11) assert the quorum poison-loop **bound** (`DeliveryLimit + 1`, not `==` — the count is non-deterministic on cyclic topologies per §6.3) on a real **4.x** quorum queue via the version matrix (T151). *dep VG-1/VG-5.*
- **T44b** **Throughput benchmark suite**: `BenchmarkPublishConfirmed` (single publisher conn), `BenchmarkPublishConfirmedMultiConn` (4 publisher conns), `BenchmarkPublishBatch`, `BenchmarkConsume`. Gates: ≥30k msg/s single-conn, ≥100k msg/s with `WithPublisherConnections(4)+WithChannelPoolSize(16)`, `PublishBatch` ≥5× `Publish`. Run on every release-candidate tag against the pinned testcontainer; nightly drift report. **Lens-09 (PC-03/04/05/14):** the bench must report + pin **payload size** and **queue type**, stating both a classic and a quorum number (quorum's majority Raft commit raises confirm latency materially, so "100k" without the queue type is uninterpretable — D4); reframe `BenchmarkPublishBatch ≥ 5×` into an RTT-stated absolute (the `5×` is pegged to the local single-publish baseline where sync is already writer-bound and ~20× understates the remote benefit) with `PublishBatch` documented as the RTT-decoupled scale path; document the release-tag-only regression cadence (perf can rot between releases on a normal PR), optionally adding a lightweight CI microbench smoke; and source the "~50k msg/s per socket" figure (§6.1) with a measured single-socket ceiling + the `sendM`-writer knee (PG-4). dep PG-4, PG-6. **Lens-10 (TV-04):** add a **CI-gradeable relative regression gate** (fail if > X% slower than the last release baseline on the same runner class) as the *checked* §9 criterion, and reclassify the absolute `30k/100k/5×` numbers as **release-tag targets on stated reference hardware** (M-series, named broker config) — author-laptop numbers can never gate CI (HW/RTT/neighbour contention); the per-criterion §9 classification is the capstone T152. *dep VG-1.* **Lens-13 (LT-10/11):** load-campaign distinction: the §9 `30k/100k` throughput numbers are **one-shot benchmark iterations**, not a sustained campaign — the **sustained-throughput campaign** (sustain the §9 target ≥1h without latency creep / pool saturation / memory growth) is **T167**, which inherits this bench's pinned payload-size + queue-type discipline; **LT-11 (quorum-vs-classic under load) is already satisfied** by PC-04's "state both a classic and a quorum number" — confirm, no new bench work.
- **T45** Reconnect chaos test: **5-minute outage @ 10k msg/s** (was 60s @ 1k msg/s), zero loss with confirms, multi-conn fan-out enabled, `goleak.VerifyNone` clean; flaky-rate <1% over 50 runs. **Lens-08 (CR-10):** add an explicit **1000-cycle** connect/disconnect + confirm-churn `goleak.VerifyNone` sub-test (no such churn test exists today) and reconcile §7's "100x" (L2268) **up** to §9's "1000" so the two stress criteria agree; confirm every goroutine in the Phase-19 inventory is joined. *dep CG-4.* **Lens-10 (TV-09/10):** (TV-09 — the lens's highest-leverage finding; it guards the §1 no-loss headline) specify the chaos "zero message loss" counting method in §7 **and** the test: loss = **published-set − consumed-set deduplicated by `MessageID`** (UUIDv7), explicitly tolerating at-least-once **duplicates** (`PublishRetry`/the reconnect barrier produce them by design); add the **VG-6 injected-drop self-test** — the harness must report loss == 1 for one deliberate drop, proving it cannot pass while losing; (TV-10) reconcile the residual **§7 chaos prose** still reading "60s outage at 1k msg/s" (L2245) **up** to §9's "5-minute outage at 10k" (the §7 "100x"→"1000" memory-stress reconciliation is already this task's Lens-08 scope). *dep VG-6.* **Lens-13 (LT-09):** intensity honesty: the chaos `10k msg/s` is **billions/day at average, never at peak** — 1 B/day ≈ ~11.6k/s average, and "billions/day" with typical 3–10× peaks means 10k validates neither a multi-billion/day system nor any system at peak; state the chaos/sustained intensity as a **peak multiple** of the average bar (the multiple is the owner's call, Q4), reusing this task's TV-09 loss-counting method unchanged.
- **T45b** **Security regression test**: scan the recorded outputs of a 60s integration run with a credentialed URI; assert no log line, error message, span attribute, or metric label contains the clear-text password. **Lens-10 (TV-14):** breadth of *exercise*, not just of scan — the 60s credentialed run must **force** a wrapped-`amqp091-go` error path (an auth failure / a broker error whose message embeds the dial URL; per Lens-07 the likeliest leak is a wrapped underlying error, not a warren-own string) so the wrapped-error surface is actually produced before the grep runs, since a grep only catches output that was exercised. coord Lens-07. *dep VG-1.*
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

### Phase 9.5 — Cluster Reliability Lane (decomposed T166, pulled forward)

Goal: stand up the **multi-node cluster harness** and the **cluster-only-provable campaigns** behind a new on-demand/nightly **`cluster` build tag** — the half of the §9 reliability claims a single-node broker fundamentally **cannot prove** (quorum leader failover, SingleActiveConsumer failover on real node death, multi-node `WithAddrs` rotation, partition handling, mixed-version rolling upgrade). This phase is the **decomposition of T166** (Phase 24, Lens-13, originally an "L" task that "lands last") pulled forward to execute right after Phase 9 (release readiness) and before Phase 10 (SRE hardening). T166 in Phase 24 is retained as a **pointer** to this decomposition so the Lens-13 cross-references (LT-01/05, LG-2) stay valid; references to "T166" in **T66**, **T165**, **T164/LG-2** and **LATER-49** resolve to the **T166a–T166h** family.

**Harness decision.** A `test/docker-compose.cluster.yml` forms **3 RabbitMQ nodes** into a cluster (quorum queues, `cluster_partition_handling = pause_minority` + `autoheal`) fronted by a **Toxiproxy sidecar** on each node's AMQP port (deterministic, scriptable partition injection — owner choice over `docker network disconnect`/iptables). Brought up with `make cluster-up`. An `internal/amqptest` cluster helper **discovers** the node URIs + management URLs + the Toxiproxy control URL from env (`WARREN_CLUSTER_NODES`, `WARREN_CLUSTER_MGMT`, `WARREN_TOXIPROXY_URL`) and **fails (`t.Fatal`), never skips,** when they are unset — mirroring the `AMQP_TEST_URL` discipline (the `cluster` tag is the opt-in, so a missing cluster is a misconfiguration, not a reason to pass green). Tests are tagged `//go:build cluster`.

**Invariants.** The per-PR `integration` lane stays **single-node and fast** and remains the required gate; the `cluster` lane is **on-demand/nightly and never a required per-PR gate** (D5, consistent with Phase 24's "no new required per-PR build-tag lane"). The existing single-node tests (`connection_failover_integration_test.go`'s dead-port trick, `examples/ordered_consume`'s connection-close fake, `consumer_resubscribe`) are **kept** — the cluster lane **adds** real-node-death counterparts, it does not delete the fast tests. Zero-loss is measured exactly as TV-09 mandates: published − consumed, deduplicated by `MessageID` (at-least-once duplicates tolerated). Each sub-task is a **vertical slice** that leaves the tree green.

- **T166a** `test/docker-compose.cluster.yml` + `Makefile` + `internal/amqptest/cluster.go` + `cluster_smoke_cluster_test.go`: **harness skeleton + first green dial-over-3-nodes test.** 3-node quorum cluster (pinned images, `pause_minority`+`autoheal`) + a Toxiproxy sidecar per node; `make cluster-up`/`cluster-down`/`test-cluster`; the `cluster` build tag; the env-discovery helper (fail-not-skip) and a zero-run guard mirroring the integration TV-13 guard. One smoke test: `Dial(WithAddrs([3 node URIs]))` → `Health` green, `goleak.VerifyNone`. Proves compose→env→dial→green end-to-end before any failover logic lands. **Deps:** T07d (multi-conn pool), the existing `WithAddrs`. **(T166-decomp, P1)**
- **T166b** `internal/amqptest/cluster.go` (+ a thin Toxiproxy client) + `cluster_control_cluster_test.go`: **cluster control toolkit** — stop/start/kill a named broker node, read a quorum queue's `leader`/`members`/`online` from the management API, cut/heal a node's AMQP port via Toxiproxy. Verified by a test that declares a quorum queue, records its leader, stops the leader node, and asserts the management API reports a **new** leader (a behaviour single-node cannot produce). **Deps:** T166a. **(T166-decomp, P1)**
- **T166c** `cluster_quorum_failover_cluster_test.go`: **campaign — quorum leader failover under sustained load (zero loss).** Stream confirmed messages to a quorum queue under load while killing the leader mid-stream; assert zero loss (published − consumed deduped by `MessageID`, reusing the T45/TV-09 helpers) and `goleak`-clean across the Raft re-election. The headline cluster claim. **Deps:** T166b; reuses T45 loss accounting. **(T166-decomp / LT-05-cluster, P1)**
- **T166d** `cluster_sac_failover_cluster_test.go`: **campaign — SingleActiveConsumer failover on real node kill.** Active consumer pinned to node A, hot standby on node B (each on its own connection over distinct addrs); kill node A; assert the broker promotes the standby and publish-order == handler-order across the **real node death** with at-least-once dedupe. The cluster-grade counterpart of `examples/ordered_consume` (today it fakes failover by closing one connection) and `consumer_resubscribe_integration_test.go`. **Deps:** T166b. **(T166-decomp / LT-05-cluster, P1)**
- **T166e** `connection.go` + `options_connection.go` + `connection_internal_test.go` + `cluster_failover_rotation_cluster_test.go`: **implement T66's per-connection `WithAddrs` shuffle + campaign — real multi-node rotation & no-`addr[0]` stampede.** *Code change (TDD):* today only the T33 round-robin cursor exists (`connection.go:468`); add the per-connection shuffle of the address list at `Dial` and unit-test its distribution deterministically. Then the cluster campaign: N connections spread across the 3 nodes; a full-cluster restart shows reconnections spread across addresses (**no `addr[0]` stampede** — the DS-10/SRE-04/T66 assertion that is unrunnable single-node). Adds a real-node variant of the dead-port failover test. **This slice satisfies T66.** **Deps:** T166b. **(T166-decomp / satisfies T66 / LT-01/05, P1)**
- **T166f** `cluster_confirm_latency_cluster_test.go` + `cluster_reconnect_storm_cluster_test.go`: **campaigns — quorum confirm-latency under Raft + reconnect-storm across nodes.** Measure publisher-confirm latency on a quorum queue (3-node majority commit) under load, overriding `WithLatencyBuckets` so the 30 s `ConfirmTimeout` + R10-8 barrier tail is not clipped to `+Inf` (coordinate with the T71/T169 measurement theme); reconnect-storm: `ForceReconnect` across all nodes under load, assert recovery + zero loss + no `addr[0]` stampede (consumes T166e's shuffle). **Deps:** T166c, T166e. **(T166-decomp / LT-05-cluster, P2)**
- **T166g** `test/docker-compose.cluster.yml` (4.x member) + `internal/amqptest/cluster.go` + `cluster_partition_cluster_test.go` + `cluster_rolling_upgrade_cluster_test.go` + `SPEC.md §7`: **campaigns — partition-under-load (Toxiproxy) + rolling 3.13→4.x upgrade-under-load.** Inject a partition via Toxiproxy under sustained load; assert `pause_minority` isolates the minority, the majority surfaces **classifiable errors** (not silent stalls), and zero loss + recovery after heal. Rolling upgrade: mixed-version members (3.13 + 4.x pinned images), restart one node at a time under load, assert continuity. Amend SPEC §7 (cluster harness + per-campaign topology labels). **Deps:** T166b, T166e. **(T166-decomp / LT-05-cluster, P2)**
- **T166h** `.github/workflows/*.yml` + `Makefile` + `README.md`: **cluster CI lane wiring.** Wire the on-demand/nightly `cluster` lane into the scheduled workflow (**extends T151**) — runs `make test-cluster` against the compose cluster on its stated cadence; **explicitly not a required per-PR gate** (D5). Sync README (cluster lane + new `make` targets — external contract). Confirm the standing-env deferral remains **LATER-49** (no new entry). **Deps:** T166a–T166g (wires the lane that runs them); extends T151; defers to LATER-49. **(T166-decomp / LT-05-cluster, P2)**

**Checkpoint — Phase 9.5:**
- [ ] Harness (T166a/b): `make cluster-up` stands a 3-node quorum cluster + per-node Toxiproxy; the cluster env helper **fails, not skips** when the cluster vars are unset; the `cluster` lane has a zero-run guard; the control toolkit can kill a node and observe quorum-leader migration via the management API.
- [ ] Campaigns (T166c–g): quorum leader failover / SAC node-kill failover / `WithAddrs` no-stampede / quorum confirm-latency / reconnect-storm / partition-under-load / rolling 3.13→4.x all run on the cluster lane and assert their outcomes; zero-loss reuses the TV-09 dedup-by-`MessageID` method unchanged.
- [ ] Code (T166e): the T66 per-connection shuffle is implemented + unit-tested; the dead-port failover test and the `ordered_consume`/`consumer_resubscribe` patterns gain real-node-kill cluster counterparts; **the single-node `integration` lane is unchanged and stays the per-PR gate.**
- [ ] Cadence: the `cluster` lane is **on-demand/nightly, not a required per-PR gate** (D5); the standing dedicated environment remains **LATER-49**.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green (cluster tests excluded without the tag); SPEC §7 + README synced (new make targets + lane = external contract).
- [ ] Phase-24 **T166** entry points here (decomposed → T166a–h, pulled forward); **T66/T165/T164-LG-2/LATER-49** references to "T166" resolve to the T166a–h family; tallies updated (no duplicate IDs).

### Phase 10 — SRE & Resilience Hardening

Goal: Address gaps identified in the SRE assessment (operability at scale, memory limits, and cardinality) originating from `LATER.md`.

- **T47** `topology.go`: Inject `Binding{Exchange: dlxName, Queue: dlqName, RoutingKey: "#"}` in the in-memory expansion of `DeadLetter` (fix routing limbo).
- **T48** `codec/json.go`: Evaluate `dec.More()` in `Decode` and return `ErrInvalidMessage` if trailing data exists. Add Fuzz target for strict mode.
- **T49** `metrics/prometheus.go` and `consumer.go`: Change the `reason` label in `consumer_cancelled_total` from a raw UUIDv7 to a closed enum (`"queue_deleted"`, `"exclusive_revoked"`, `"unknown"`), preventing Prometheus OOM.
- **T50** `consumer.go`: Expose `MaxInFlightBytes(n)` on the builder. Decrement semaphore after handler; pause ingestion when the limit is reached to avoid local OOM.
- **T51** `publisher.go`: Add `WithPublishRateLimit(perSec)` (local token-bucket) to protect the broker from accidental runaway loops.
- **T52** `consumer.go` and `metrics/`: Create `WithQueueDepthSampler` to poll (via `declare-passive`) and export native `queue_depth` and `dlq_depth` gauges.
- **T52a** `consumer.go`: Failure-aware depth sampler — clamp sub-100ms intervals to a 100ms floor (one-time warn) and back off exponentially (capped, never below the configured interval) while every probe in a sample fails, so a permanently-missing queue or a reconnecting socket stops probing at full rate. Resolves LATER-80.
- **T52b** `consumer.go` and `metrics/`: Delete the `queue_depth`/`dlq_depth` series when the sampler stops, so a long-lived process cycling through distinct queue names cannot accumulate stale frozen series. Resolves LATER-81.
- **T53** ✅ `consumer.go`: Expose `Pause(ctx)` and `Resume(ctx)` for manual graceful degradation (local `basic.cancel` / re-`basic.consume` on the same channel; in-flight handlers survive). Evolved the check to a rich `Health(ctx) (*ConsumerHealth, error)` with `Active`/`Paused`/`LastDeliveryAt`/`InFlightHandlers` for k8s liveness probes (returns `(nil, err)` on connection failure; signature reconciled in SPEC §6/§6.3, `rpc_replier.go` adapted).
- **T54** ✅ `errors.go`: Refine `IsTransient()` to return false when the root cause is `context.Canceled`, blocking useless PublishRetries from upstream request cancellations.
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

**Lens-04 re-review (2026-05-28):** the event-driven-architecture re-review
(Phase 15 below) pulls **T68, T69** forward into v0.1 (topology-expressiveness
gaps the EDA lens elevates — the platform-level unroutable black-hole and layered
fan-out), adds **T101–T110**, and extends **T85** (the §6.2.1 dangling
`examples/idempotent_consume/` reference, EDA-18). It introduces **no** new
build-tag lane (its gates ride the existing integration + `chaos` lanes).

**Lens-05 re-review (2026-05-28):** the SRE / production-operability re-review
(Phase 16 below) pulls **T67, T72** forward into v0.1 (the two remaining unclaimed
operability deferrals — an unpredictable partial-pool boot and a publisher hung up
to 30s on a half-open socket), adds **T111–T118** (a metrics-correctness +
capacity-honesty set), and extends **T61, T62, T63, T65, T66, T70, T71** in place
with a `Lens-05 (SRE-xx)` detect/respond/verify acceptance bullet (the seven
operability findings already owned by prior-lens tasks — **not** re-filed). It
introduces **no** new build-tag lane (its gates ride the existing integration +
`chaos` lanes) and **no** new `LATER.md` entry (the async-publish ceiling stays
LATER-34). Reverting any of those seven pulls flips Lens-05 to NO-GO.

- **T57** `message.go` + `topology.go`: godoc on `Message.Delay` and `ExchangeKindDelayed` mirroring the SPEC §6.5 durability warning (scheduled messages lost on node failure; recommend durable-queue + TTL + DLX); optional declare-time warning when an `ExchangeDelayed` is declared. **(R10-1, P0.1)**
- **T58** `topology.go`: `validate()` warning when `Type=QueueTypeQuorum && DeliveryLimit==0` (RabbitMQ 4.x applies a broker default of 20 — not unbounded). Align §9 poison-loop wording. **Lens-01 (RMQ-06):** make the warning **version-aware** — read the broker version from the `connection.start` server-properties: on 4.x the default is 20 (silent drop at the 21st delivery), but on **3.13 a `DeliveryLimit==0` quorum queue is genuinely unbounded (infinite poison loop)**, the opposite failure mode. Fix the stale `topology.go:48-49` "Zero means unbounded" godoc; add a per-version poison-loop integration assertion (gate G3). **(R10-2, P0.2)**
- **T59** `internal/confirms` / `publisher_test.go`: regression test that locks the return/ack ordering invariant — fails if the `basic.return` notify channel is buffered or the demux is split across goroutines (assert `MarkReturned` precedes `Ack` resolution for a mandatory-unroutable publish under load). **Lens-01 (RMQ-16):** also add a real-broker assertion (not just the mock tracker), pin the `amqp091-go` version in `go.mod`, and comment that the invariant depends on amqp091-go's single synchronous reader goroutine (a buffered or worker-pool dispatcher upstream would silently break it). **Lens-10 (TV-02):** the real-broker assertion must run **many iterations** under **concurrent unroutable-mandatory publishes during confirm load** to actually trip the ~50%-under-load race against amqp091-go's two-channel notify dispatch (a mock tracker cannot reproduce the dispatch timing → a green mock proves nothing about the race it exists to lock); add a §7 note that this contract is **flaky-prone by design** and belongs on the nightly trigger (T151), not a single PR run. *dep VG-4.* **(R10-3, P0.3)**
- **T60** `delivery.go` + `consumer.go`: idempotent resolved-once guard on `Delivery[M]` (mirrors `Batch[M]`). A handler-timeout verdict followed by a late handler `Ack`/`Nack`, or a double `Delivery.Ack/Nack/AckIf` via `ConsumeRaw`, is a no-op — never a second frame that channel-closes with `PRECONDITION_FAILED`. **Lens-02 (DS-04):** amend §6.3 to document the double verdict as a no-op and that **pre-fix** it channel-closes (406/`PRECONDITION_FAILED`) and takes out *every* in-flight handler on that channel — collateral loss, not just a duplicate; add a `ConsumeRaw` double-call unit test. **Lens-08 (CR-04, High):** specify the resolved-once guard on `Delivery[M]` is a **single atomic CAS** (only the winner emits a frame; the loser is a no-op), explicitly **not** a check-then-act — today `Delivery.Ack/Nack` only test `d.done` (`delivery.go:79-115`), a window in which a timeout-verdict goroutine and a handler-`Ack` goroutine both emit → `PRECONDITION_FAILED` → channel close → every in-flight handler on that channel dies; unify with the `Batch[M]` guard and add a **race + behavioural** test (timeout vs handler-`Ack`) asserting exactly one frame and the late call a no-op. *dep CG-3.* **(R10-5, P0.4)** — *pulled into Phase 12; Lens-02 adds the §6.3 wording.*
- **T61** `connection.go` + `consumer.go`: channel-level recovery distinct from TCP reconnect. Consumer observes its own channel close (404/406/ack-error with the TCP connection still up) and reopens + re-`basic.qos` + re-`basic.consume`, incrementing `consumer_resubscribed_total`, without a full reconnect. **Lens-05 (SRE-01):** silent channel death is the highest-severity *invisible* failure — the resubscribe must increment `consumer_resubscribed_total` so a `rate()==0` while traffic is expected is alertable, and its absence is the root cause of the `Connection.Health` readiness false-green (SRE-13/T115). **(R10-6, P1.4)** — *sequence with T62/T63.*
- **T62** `connection.go` + `topology.go`: redeclare broker-global topology **once per recovery event** at the `*Connection` level instead of per pooled `managedConn` (today `AttachTo` registers the hook on every conn → N×pool declares on broker restart). Keep `basic.consume`/`basic.qos` reissue per consumer connection. **Lens-02 (DS-09):** this compounds with DS-10 (T66) into a self-amplifying recovery storm; add a §6.1 note and have the chaos lane exercise a full-cluster restart against a just-recovered (possibly Khepri-quorum-forming) broker. **Lens-05 (SRE-06):** the recovery action must not hammer the just-recovered (fragile) broker with N×pool×fleet `queue.declare`s; couple the chaos exercise with the SRE-04/T66 full-cluster restart. **(R10-7, P1.2)** — *sequence with T61/T63; pulled into Phase 13 (v0.1).*
- **T63** `connection.go`: bound the synchronous reconnect barrier with a max duration; on cap, blocked publishers get `ErrReconnecting` instead of stalling indefinitely behind a half-alive broker (matters most with `PublishTimeout=0` + `context.Background()`). **Lens-01 (RMQ-17):** the cap must also cover **Khepri (4.1 default)**, where `queue.declare` is a Raft-quorum op that can block during partition recovery. **Lens-02 (DS-02):** SPEC must state `ConfirmTimeout` does **not** cover the barrier (the frame is still unwritten) → the cap is a *distinct* mechanism; name the option, its default, and the post-cap connection state (force-reconnect vs degraded). A half-alive-broker proxy test (accepts socket, stalls `queue.declare`) with `PublishTimeout=0`+`context.Background()` asserts `Publish` returns `ErrReconnecting` within the cap, not ∞; `goleak` clean. **Lens-05 (SRE-02):** the barrier-cap default must be **≤ the new default histogram top bucket** (SRE-11/T113) so a capped stall is *visible* in `publisher_publish_seconds`, not collapsed into `+Inf`. **(R10-8, P1.6)** — *sequence with T61/T62; pulled into Phase 13 (v0.1).*
- **T64** `topology.go`: quorum-queue structural validation in `validate()` — reject non-`Durable`, `Exclusive`, `AutoDelete` quorum queues with `ErrInvalidOptions`, and reject `x-max-priority` on quorum (via the `MaxPriority` field, already classic-gated at `topology.go:436`, **and** via a raw `Args["x-max-priority"]`). Require `Durable` on stream queues too. **Lens-01 (RMQ-20):** the `Queue.MaxPriority` field **does** exist in code (`topology.go:56`) — retire the stale "no such struct field" note and instead **document `Queue.MaxPriority` in SPEC §6.6** (spec/code drift). **(R10-9, P1.5)** — *coordinate with T58 (same `validate()`).*
- **T65** `topology.go` + `consumer.go` + `metrics/`: auto-declared `<source>.dlq` becomes durable (quorum-capable) with configurable bounds; Consumer mirrors the `Replier` missing-DLX validation — when `MaxRedeliveries>0` and the wired `Topology` has no DLX for the source queue, warn at `Build` and emit a drop metric so poison drops are observable. **Lens-05 (SRE-03):** highest blast radius in the spec — an unbounded DLQ fills disk → broker-wide `connection.blocked` (one service's poison storm → cluster-wide publish outage); bound the DLQ *by default* (overflow `drop-head`/`reject` is a deliberate bound, not unbounded) and add a DLQ-depth signal so the storm is visible *before* the broker alarm. **Lens-07 (ST-08):** the same unbounded DLQ is an *attacker-reachable* resource-exhaustion vector — a producer that floods the queue with poison fills `<source>.dlq`, fills disk, and trips broker-wide `connection.blocked` (one service's poison storm → cluster-wide publish outage); the default bound is the security control, and a test asserts it holds under an *adversarial* poison flood (not just an accidental one). **Lens-11 (DP-03):** storage limitation (GDPR 5(1)(e) / LGPD Art. 16) — the auto-`<source>.dlq` bound must also carry a **default or strongly-recommended `x-message-ttl`** (not only a length bound) so dead-lettered *personal data* has a finite life by default (today it is retained **indefinitely**, DG-4); document the TTL as the personal-data retention control with a conservative default + a prominent godoc opt-out (exact value the T65 owner's call). **(R10-10, P1.3)** — *builds on T47 (DLX binding), T52 (`dlq_depth`).*
- **T66** `connection.go` + `options_connection.go`: shuffle the `WithAddrs` list per connection and rotate addresses on reconnect, to avoid every client stampeding the same node on broker recovery. **Lens-02 (DS-10):** add a §6.1 note that this compounds with DS-09 (T62) into a recovery storm; the chaos lane asserts no `addr[0]` stampede on a full-cluster restart. **Lens-05 (SRE-04):** the chaos lane asserts no `addr[0]` stampede on a full-cluster restart (compounds with SRE-06/T62 into a recovery storm). **(R10-11, P2.1)** — *already pulled into Phase 12.* **Lens-13 (LT-01/05):** harness fidelity: the "no `addr[0]` stampede on a full-cluster restart" assertion is **unrunnable on the single-node testcontainers harness** it is scheduled against — it binds to the **T166** multi-node cluster harness; the reconnect-storm is also a spike-on-recovery, exercised by the **T168** composite set. This assertion is confirmed, not re-filed — T166 finally gives it a harness that can run it.
- **T67** `connection.go`: define + implement `Dial` partial-pool-connect policy (succeed if ≥1 connection per role connects, supervisor reconnects the rest; or fail-fast — decision recorded in SPEC §6.1). **Lens-05 (SRE-08):** an undefined boot policy means operators cannot predict restart/rollout behaviour — fail-fast lets one flaky node block *every* deploy, succeed-degraded is *silent* capacity loss; resolve to **succeed-if-≥1-per-role** with supervised reconnect of the rest **and** a metric/log for booting at reduced capacity (so "silent capacity loss" becomes observed). **(R10-12, P2.2)** — *pulled into Phase 16 (v0.1).*
- **T68** `topology.go` + `publisher_builder.go`: expose `x-alternate-exchange` (server-side catch-all for unroutable messages) on `Exchange`/topology, complementing per-publish `Mandatory()`+`OnReturn`. **Lens-04 (EDA-01):** this is the platform-level unroutable safety net — a mis-routed publish *without* `Mandatory()` vanishes silently (EG-1), and per-publish discipline does not scale across dozens of producers; the alternate exchange is the server-side catch-all. Complements T103's client-side exchange-name validation. **Lens-06 (GA-05):** expose the alternate exchange **additively** — via the existing `Exchange.Args` or a new optional field whose zero value = today's behaviour; **no exported `Exchange` field is renamed or removed** (T124 pins the topology roadmap additive). **(R10-13, P2.4)** — *pulled into Phase 15 (v0.1).*
- **T69** `topology.go`: exchange-to-exchange bindings (`exchange.bind`) — extend `Binding` (or add a typed variant) so an exchange can be a binding destination, for layered fan-out topologies. **Lens-04 (EDA-03):** ingest→per-domain layered fan-out is bread-and-butter EDA; without `exchange.bind` users must flatten the topology or declare out-of-band, and the pull-forward must keep the declare-once/deep-snapshot semantics intact. **Lens-06 (GA-05):** the destination-exchange shape is **pinned by T124** to a **separate `Topology.ExchangeBindings []ExchangeBinding{Source, Destination, RoutingKey, NoWait, Args}`** — `Binding` is **not** reshaped (no `Source`/`Destination` rename, no exported `Binding` field renamed or removed); the declare-once/deep-snapshot semantics extend to `ExchangeBindings`. **(R10-14, P2.3)** — *pulled into Phase 15 (v0.1).*
- **T70** `connection.go` + `consumer.go` + `batch_consumer.go`: graceful-shutdown completeness — specify + handle prefetched-but-undispatched deliveries on `Close` (drain or nack-requeue, documented), and flush a `BatchConsumer`'s pending partial batch on `Close`/final `FlushAfter`. **Lens-02 (DS-03):** resolve the "drain or nack-requeue" choice to **nack-requeue (`requeue=true`)** the undispatched buffer before channel close (an at-least-once duplicate, acceptable under §6.2.1) — never drop (silent loss); add `consumer_shutdown_requeued_total`; gated by G2 (capture the current behaviour first). **Lens-05 (SRE-07):** every rolling deploy is a low-grade incident — the deploy-time duplicate rate must be **boundable and observable** via `consumer_shutdown_requeued_total`. **Lens-06 (GA-06):** the new `consumer_shutdown_requeued_total` metric adds a method to the user-implementable `metrics.*` interfaces — it must land behind the embeddable `metrics.NoOp` base (T125) so external implementers don't break-compile, before rc1. **Lens-08 (CR-09):** on a **forced** (ctx-deadline) close, *detach* a non-cooperative handler that ignores its cancelled `ctx` (bounded by the cascade ctx), increment a `consumer_handler_leaked_total`-style metric, and do **not** hang the cascade on it (today the timeout-drain `<-handlerDone` waits unboundedly, `consumer.go:650`); the §7/§9 goleak **carve-out** for the ctx-ignoring handler (a caller defect — Go cannot force-kill a goroutine) lands in the capstone T146. **(R10-15, P2.5)** — *pulled into Phase 13 (v0.1).*
- **T71** `metrics/` + `channelpool.go` + `consumer.go`: add channel-pool acquire-wait/saturation metric, a `consumer_in_flight{queue}` gauge, and a `consumer_redelivered_total{queue}` counter (redelivery ratio = leading instability signal). Coordinates with Phase 10 T50/T52/T53. **Lens-02 (DS-14):** `consumer_redelivered_total{queue}` is the redelivery-class duplicate-budget signal `publisher_retry_total` does not cover — required for the §1 "duplicate budget never invisible" claim to hold for the dominant duplicate source; assert it increments on a `Redelivered()` delivery. **Lens-05 (SRE-05):** this is the single most important on-call *leading* indicator — without it a brewing poison storm / pool saturation is invisible until it is an outage, and the §1 "duplicate budget never invisible" claim fails for the dominant duplicate source (redelivery). **Lens-06 (GA-06):** these new gauges/counters add methods to the user-implementable `metrics.*` interfaces — they must land behind the embeddable `metrics.NoOp` base (T125) so adding interface methods stays forward-compatible for external adapters, before rc1 (§9 `// Deprecated`-free rc1→v1.0). **(R10-16, P2.6)** — *pulled into Phase 13 (v0.1).* **Lens-13 (LT-07/08):** measurement under load: the pool-wait / `consumer_in_flight` / `consumer_redelivered_total` saturation metrics this task adds must land **before** the spike/stress/soak campaigns (T167/T168) can observe the saturation they provoke (LT-08 — a hard dependency of those campaigns); and the default `WithLatencyBuckets` top bucket (5000 ms, §6.9) clips the load tail (`ConfirmTimeout` 30s + the R10-8 barrier cap), so the load reports override the buckets (the override is owned by T169) — coordinate the override range with this metric set (LT-07).
- **T72** `options_connection.go` + `connection.go`: `net.Dialer` keepalive on the default dialer (document `TCP_USER_TIMEOUT` where available) so a write to a half-open socket fails promptly rather than relying on `ConfirmTimeout` as the only backstop. **Lens-05 (SRE-09):** AMQP heartbeats cover only *read-side* partition detection (~20s); a *write* to a half-open socket can block far longer with `ConfirmTimeout=30s` the only backstop — the dialer keepalive must make a publish on a dead socket error promptly (well under 30s), not at the confirm timeout. **(R10-17, P2.7)** — *pulled into Phase 16 (v0.1).*
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
validation (`docs/spec-validation/01-rabbitmq-amqp-protocol.md`, findings
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
- **T81** version-divergence docs (RMQ-17/19/21/23/30/31): Khepri caveat; CloudEvents 0-9-1⇄1.0 bridge version note + round-trip interop test (coord. lens 03); pin §9 verification to the management HTTP API instead of `rabbitmqadmin` CLI (v2 rewrite in 4.0); mirrored-queue staleness (§6.2); transient-queues-deprecated-feature note; mixed-version-cluster caveat. **Lens-10 (TV-05/11):** the version-divergence claims are **verified** by the new RabbitMQ 3.13 + 4.x integration matrix (T151) — assert the quorum default `x-delivery-limit` (R10-2, default 20) drops the 21st delivery on **4.x** and the classic-queue `x-delivery-limit`-ignore behaviour, and pin §9 verification to the management HTTP API on the version where each binds; the poison-loop **bound** assertion (TV-11) rides the same 4.x quorum queue. *dep VG-5.* **(P2)**
- **T82** contract-precision SPEC fixes (RMQ-24/25/26/27/28/29): decision-17 default-"1" staleness; ack-vs-confirm wording (§6.2); sub-ms `Expiration`→`"0"` footgun (optionally reject `<1ms` non-zero, §6.5); Priority range + "quorum has no priority"; exclusive-reply-queue redeclare-on-reconnect (§6.7); prefetch-16 as guidance not a broker constant (§6.3). **Lens-02 (DS-07):** coordinate the ack-vs-confirm wording with Phase 13 T88's queue-type confirm-semantics table — single source, no contradiction. **(P3)**
- **T83** SPEC §9 throughput-honesty wording (RMQ-11): qualify 30k/100k with local-broker/sub-ms-RTT, document the `pool/RTT` ceiling + a remote projection, cross-reference LATER-34. **Lens-09 (PC-01):** bake the explicit RTT-collapse model table (rate @1/5/10 ms — brief §11) into §9 *beside* the 30k/100k numbers as the "remote projection", so the load-bearing local-only caveat is computable at the number rather than parked ~680 lines away in LATER-34 (the 30k/100k targets imply ~0.27–0.64 ms loopback RTT; they collapse to ~64k/~12.8k/~6.4k multi-conn at 1/5/10 ms). dep PG-5. **(RMQ-11, P2)**

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
(`docs/spec-validation/02-distributed-systems.md`, findings `DS-01..DS-17`; planning
brief `docs/spec-validation/02-distributed-systems-plan.md`). The lens verdict was
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

- **T84** chaos lane + verification gates (real broker **and** 3-node cluster, **3.13 and 4.x**): introduce a `chaos` build tag + a `make test-chaos` target that stands up a 3-node cluster (configurable `cluster_partition_handling`), a fault injector, and a half-alive-broker proxy. Capture ground truth for **G1** SAC-failover reorder/duplicate with `Prefetch>1` (classic **and** quorum), **G2** the *current* `Close` behaviour for prefetched-but-undispatched deliveries (requeue or drop?), **G3** quorum publish pinned to a minority-partition node (hang/timeout/error + duplicate-on-heal), **G4** the client-side signal per `pause_minority`/`autoheal`/`ignore`, **G5** a poison crash-loop defeating process-local Counter B, **G6** the `Caller`'s handling of a second reply for an already-resolved `CorrelationID`. Results table (under `docs/spec-validation/`) gates T86/T87/T88/T90/T92 and the pulled-forward T70. **(Lens-02 gates, P0)**
- **T85** `SPEC.md §6.2.1/§6.5` + `examples/idempotent_consume/`: rework the dedupe pattern (DS-01) — split **publish-retry** duplicates (bounded by outage+reconnect+retry → in-memory LRU OK) from **crash/requeue redelivery** duplicates (unbounded gap, and the crash that triggers redelivery wipes the in-memory cache → ~zero protection); state that handlers with external side-effects (DB/HTTP/payments) **require persistent dedupe**, not "paranoid", and recommend bounding queue residency with a TTL so a finite persistent retention window is definable. Fold **DS-15** (drop the "UUIDv7 makes eviction-by-recency trivial" non-sequitur — an `lru.Cache` evicts by access order, not the key's embedded timestamp; document that `MessageID`/`Timestamp` ordering is **per-publisher wall clock**, not global, not monotonic across NTP steps; only ID *uniqueness* is load-bearing) and **DS-16** (name the forced-close duplicate window: ctx-deadline force-close abandons in-flight handlers → redelivery). Ship a persistent (Redis/DB) `examples/idempotent_consume/` variant + a chaos test that crashes the consumer mid-handler and asserts the persistent path dedupes the redelivery while the in-memory path does not. **Lens-04 (EDA-18):** the §6.2.1 L1067–1068 dangling `examples/idempotent_consume/` reference is closed by this task + T38b — verify the directory ships and matches the reworked pattern. **Lens-11 (DP-05):** data minimisation (GDPR 5(1)(c) / LGPD Art. 6.III) — a *natural-key* `MessageID` turns the dedupe cache into a **store of personal data** (and flows to `x-death`, logs, the OTel `messaging.message.id` attr → APM); frame the recommended TTL/residency bound as **storage limitation** (a finite, definable retention window for whatever the user puts in the key) and note the privacy-favourable path is to keep `MessageID` the UUIDv7 default. **(DS-01/DS-15/DS-16, P0; dep T38b)**
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

### Phase 14 — Interoperability & Wire-Format Re-review (Lens 03: polyglot clients, CloudEvents/Proto/JSON, field-tables)

Goal: bring `SPEC.md` into honest alignment with the project's own
`feedback_codec_interop` rule — codec and wire-format decisions must be grounded
in **non-Go-client interop**, never warren↔warren convenience — at the §1
"billions/day" bar, which for a polyglot estate means *cross-language* wire
fidelity, not just Go round-trips. Closes the Lens-03 adversarial spec validation
(`docs/spec-validation/03-interoperability-wire-format.md`; this pass *conducted* the
review and produced findings `IW-01..IW-13`; planning brief
`docs/spec-validation/03-interoperability-wire-format-plan.md`). The lens verdict was
**GO-WITH-CHANGES** — no active §1 message-loss bug, but several interop overclaims
(IW-01 CloudEvents 1.0 bridge, IW-08 field-table "matched 1:1"), two silent-lossy
mappings (IW-07 `time.Time`→`T` truncation, IW-04 JSON `int64` precision), and the
absence of any non-Go interop test (IW-13).

Owner decisions (2026-05-28): (1) stand up the **FULL** polyglot interop lane — a
new **`interop` build tag** with Python `pika` + a Java client + an AMQP-1.0
CloudEvents SDK in containers — because the Go-only `testcontainers` lane cannot
falsify the cross-language claims; gate it on the same 3.13-vs-4.x matrix and
extend **T37** (`amqptest`), do not duplicate; (2) **remove** the CloudEvents
binary-mode 0-9-1→AMQP-1.0 interop guarantee (IW-01) — document binary mode for
**0-9-1 consumers only** and promote **structured mode**
(`application/cloudevents+json`) as the official cross-ecosystem path; (3) **defer**
the Protobuf multi-type discriminator (IW-05) to **LATER-39**, documenting only the
single-type-per-`Consumer` constraint now; (4) for `time.Time` headers (IW-07),
**document + recommend an RFC3339 string** (no API change). Phase 14 therefore adds
exactly **one** `LATER.md` entry (LATER-39). Per-task SPEC amendments land in the
same PR as the code (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 13" note
records the pass when the first task lands. **Cross-language claims are tested by a
round-trip through a non-Go client, not a Go↔Go mock.** Gate task T94 runs first
and **no SPEC interop-claim edit to an affected section lands before its gate
returns**.

- **T94** interop lane + verification gates (real broker, **3.13 and 4.x**): introduce an `interop` build tag + a `make test-interop` target that stands up Python `pika` + a Java client + an AMQP-1.0 CloudEvents SDK in containers (extending `amqptest`/T37). Capture ground truth for **IG-1** `time.Time`→`T` second-resolution read, **IG-2** unsigned `B/u/i/L` + `Decimal` `D` + `[]byte` `x` cross-client decode, **IG-3** what an AMQP-1.0 CloudEvents SDK sees for a binary-mode publish (prefix, section, colon key), **IG-4** structured-mode reconstruction from `application/cloudevents+json`, **IG-5** JSON `int64 > 2^53` into a Go `int64` field vs `any`, **IG-6** ContentType/ContentEncoding not swapped via a non-Go consumer. Results table (under `docs/spec-validation/`) gates T95/T96/T97/T98/T100. **(IW-13 + gates, P0)** — *coordinate with T37 (amqptest): extend, do not duplicate.*
- **T95** `SPEC.md §6.5` + decision 13: document the `time.Time`→AMQP `T` truncation (IW-07) — AMQP 0-9-1 `timestamp` is 64-bit POSIX **seconds**, so sub-second precision and timezone are silently lost; `Headers{"ts": time.Now()}` truncates on the wire and a Java reader sees a second-resolution `Date`. Recommend an RFC3339 **string** header when sub-second/TZ fidelity matters. Round-trip test asserts the Go-reader truncation; IG-1 asserts the `pika` second-resolution read. **(IW-07, P1; dep IG-1)**
- **T96** `SPEC.md §6.5` + decision 13: scope the field-table cross-client guarantee (IW-08/IW-09/IW-10) against RabbitMQ's documented field-table type table — the unsigned types `uint8/16/32/64`→`B/u/i/L` and `Decimal{Scale,Value}`→`D` are Go/Java-leaning and may be unreadable/mis-decoded by some Python/.NET consumers; recommend signed `int64` (`l`) and string headers for maximum cross-language safety, and document `[]byte`(`x`) vs `string`(`S`). Cross-client decode test via the harness. **(IW-08/IW-09/IW-10, P1; dep IG-2)**
- **T97** `SPEC.md §6.9` + decision 4 + `examples/cloudevents/`: **remove** the CloudEvents binary-mode "RabbitMQ bridges 0-9-1 headers ⇄ AMQP-1.0 application-properties, so a non-Go AMQP-1.0 CloudEvents client interoperates" guarantee (IW-01) — the CloudEvents AMQP binding is defined for **1.0**, not 0-9-1, and the bridge is version-dependent and untested. Document binary mode for **0-9-1 consumers only**; promote **structured mode** (`application/cloudevents+json` body) as the official cross-ecosystem path. Confirm the binary `time` attribute is emitted as an RFC3339 string `S` not `T` (IW-03) + colon-key (`cloudEvents:type`) survival through the 0-9-1→1.0 conversion (IW-02); move the `traceparent`/`tracestate` 1.0-bridge caveat here (IW-12). The harness AMQP-1.0 leg (IG-3) characterises actual binary behaviour and (IG-4) proves structured reconstruction. **(IW-01/IW-02/IW-03/IW-12, P1; dep IG-3, IG-4)**
- **T98** `SPEC.md §6.9` + decision 23: document the JSON 64-bit-integer precision hazard (IW-04) — a Java/Python producer's `int64`/snowflake > 2^53 decodes losslessly only into a typed `int64` field; into `any`/`map[string]any`/`interface{}` it silently becomes `float64`. State the mitigation (type `M` fields as `int64`/`json.Number`; avoid `any` for large ints) and that the codec does **not** call `UseNumber()` by design. Extend `FuzzCodecJSON` + add a cross-language `int64 > 2^53` round-trip test. **(IW-04, P1; dep IG-5)**
- **T99** `SPEC.md §6.9`: document the proto3 **single-type-per-`Consumer`** constraint (IW-05) — proto3 binary carries no type info, so a multi-type topic-exchange queue needs a caller-supplied discriminator (deferred to LATER-39); justify `application/x-protobuf` and **accept `application/protobuf` on decode** (Postel) (IW-06). File **LATER-39** for the Any/type-URL/registry discriminator. **(IW-05/IW-06, P2)**
- **T100** `SPEC.md §9`: sharpen the ContentType/ContentEncoding swap test (IW-11) — it only catches a swap if it sets **both** fields to **distinct non-empty** values (`application/json` + `gzip`) and asserts each independently via `rabbitmqadmin get` / a non-Go consumer. **(IW-11, P3; dep IG-6)** — *may ride T94.*

**Sequencing:** T94 first → gates IG-1→T95, IG-2→T96, IG-3+IG-4→T97, IG-5→T98, IG-6→T100. T99 is independent of the gates (doc + a content-type accept-test) and files LATER-39. T97 creates `examples/cloudevents/`. SPEC sub-phasing: (A) T94; (B) T95, T96, T97; (C) T98, T99; (D) T100. No tasks pulled forward (Lens 03 pulls nothing).

**Checkpoint Phase 14:**
- [ ] T94 interop lane (`make test-interop`: `pika` + Java + AMQP-1.0 clients) green on 3.13 **and** 4.x; IG-1..IG-6 results table committed; each downstream task cites its gate.
- [ ] `time.Time`→`T` truncation documented (IW-07/T95): §6.5/decision 13 flag the second-precision/no-TZ loss and recommend an RFC3339 string header; a round-trip test asserts the truncation.
- [ ] Field-table cross-client guarantee scoped (IW-08/IW-09/IW-10/T96): unsigned `B/u/i/L` + `Decimal` `D` flagged low-interop, `x` vs `S` documented, signed `int64`/string recommended; cross-client decode test green.
- [ ] CloudEvents binary 0-9-1→1.0 guarantee **removed** (IW-01/T97): binary mode scoped to 0-9-1 consumers, structured mode promoted as the cross-ecosystem path (proven by IG-4), `time` confirmed RFC3339-string (IW-03), colon-key survival verified (IW-02), trace-context 1.0 caveat moved (IW-12); `examples/cloudevents/` ships.
- [ ] JSON `int64` precision hazard documented (IW-04/T98): §6.9/decision 23 + `FuzzCodecJSON` extension + a cross-language `int64 > 2^53` test.
- [ ] Protobuf single-type constraint documented + `application/protobuf` accepted on decode (IW-05/IW-06/T99); **LATER-39** files the discriminator.
- [ ] ContentType/ContentEncoding swap test sharpened to distinct non-empty values (IW-11/T100).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the new `interop` lane green; `goleak.VerifyNone` clean.
- [ ] README interop contract synced (CloudEvents binary scoped to 0-9-1 / structured = cross-ecosystem, `time.Time` header caveat, JSON int64 caveat, low-interop field-table types).
- [ ] SPEC §10 "Rev 13" note records the Lens-03 pass; no duplicate tasks created; exactly one new `LATER.md` entry (LATER-39).

### Phase 15 — Event-Driven-Architecture Re-review (Lens 04: pattern expressiveness, safe-default analysis, topology completeness)

Goal: validate `SPEC.md` not from the API surface in isolation but from **the
architectures users build on it** — at the §1 "billions/day" bar, which for an EDA
platform means every canonical pattern must be expressible *cleanly*, the *safe*
variant must be the *default*, and every failure mode must be *observable*. Closes
the Lens-04 adversarial spec validation
(`docs/spec-validation/04-event-driven-architecture.md`; this pass *conducted* the review
— pub/sub fan-out, competing-consumer work queue, request/reply, retry ladder, DLQ +
redrive, ordered keyed stream, idempotent consume, batch consume, event versioning,
event mesh — and produced findings `EDA-01..EDA-18`; planning brief
`docs/spec-validation/04-event-driven-architecture-plan.md`). The lens verdict was
**GO-WITH-CHANGES** — no *new* §1 message-loss bug (the unbounded-DLQ and `Close`-loss
gaps are already pulled forward as T65/T70 by Phases 12/13), but one pattern is
effectively unsupported at the bar (ordered keyed streams at scale — no
consistent-hash, EDA-04), one platform-level safety net is missing (unroutable
black-hole — no alternate exchange, EDA-01/EDA-02), one default nudges toward the
lossy variant (delayed exchange for retries vs the example-less durable ladder,
EDA-05/EDA-06), one batch default silently acks poison (EDA-15), and several
boundaries are unstated (redrive, breaking schema evolution, structured-mode routing
opacity, layered fan-out).

Owner decisions (2026-05-28): (1) **pull into scope** the ordered-keyed-stream scaling
primitive (EDA-04) — add an `x-consistent-hash` `ExchangeKind` + partitioned-consumer
support + a worked example, eliminating the ordering-vs-scale forced choice; (2) **pull
both topology safety nets forward** from Phase 11 — **T68** (alternate exchange, EDA-01)
*and* **T69** (exchange-to-exchange bindings, EDA-03); (3) **build a DLQ redrive helper**
(EDA-09) — a minimal `DLQ → source` republish utility (dedupe by `MessageID`) +
`examples/redrive/`; (4) **schema evolution = document the boundary + the versioned-type
convention + LATER** (EDA-13) — §6.9/§8 state additive-safe / breaking-user-owned via
versioned type names and recommend the `Message.Type` discriminator; **LATER-40** files
the pluggable schema-registry hook. **No new build-tag lane** — unlike Lens 03's
`interop` lane, Lens 04's gates ride the **existing integration lane** (3.13 + 4.x) and,
where a crash is needed, the existing `chaos` lane (T84). Five findings are **already
owned by prior-lens tasks** and are *not* re-filed (EDA-07/EDA-08→T65, EDA-10→T91,
EDA-11→T90, EDA-16→T70, EDA-18 extends T85). Phase 15 therefore adds exactly **one**
`LATER.md` entry (LATER-40). Per-task SPEC amendments land in the same PR as the code
(CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 14" note records the pass when the first
task lands. **Pattern claims are tested by exercising the pattern on a live broker, not a
unit of API in isolation.** Gate task T101 runs first and **no SPEC edit to an affected
section lands before its gate returns**.

- **T101** verification gates (real broker, **3.13 and 4.x**, **existing integration lane — no new build-tag lane**): capture ground truth for **EG-1** a publish to a non-existent/mis-routed exchange **without** `Mandatory()` succeeds silently (no error, no `OnReturn`, message gone) — confirm `Mandatory()` is the only client-side net today; **EG-2** a short per-message-TTL message **behind** a long-TTL message fails to expire at its own TTL on a single queue (head-of-line blocking); **EG-3** a `BatchConsumer` handler returning `nil` emits one `basic.ack` with `multiple=true` over the **whole** delivery range; **EG-4** `SingleActiveConsumer` permits exactly one active consumer (no horizontal scale) **and** an `x-consistent-hash` exchange distributes by routing-key hash across N bound queues. Results table (under `docs/spec-validation/`) gates T102/T103/T104/T108. **(EDA gates, P0)**
- **T102** `topology.go` + `consumer.go` + `SPEC.md §6.3/§6.6` + an `examples/partitioned_consume/`: close the ordering-vs-scale forced choice (EDA-04) — add an `x-consistent-hash` `ExchangeKind` and the partitioned-consumer wiring so per-key ordering is preserved across N queues each consumed by one consumer (scaling ordered work horizontally), instead of the v0.1-only `SingleActiveConsumer + Concurrency(1)` (ordering on one queue, one active consumer, no scale). §6.6 documents the per-key-ordering-across-N-queues pattern and its rebalancing trade-off (changing the partition count reshuffles keys). EG-4 integration test asserts N consistent-hash-bound queues preserve per-key order while distributing across queues. **(EDA-04, P0; dep EG-4)**
- **T103** `publisher.go` + `publisher_builder.go` + `SPEC.md §6.6`: publisher-side unroutable safety (EDA-02) — document the silent-drop-without-`Mandatory()` behaviour (per EG-1) and the topology-drift risk (an `Exchange("oders")` typo publishes into the void); add an **optional** validation that, when a `*Topology` is wired to the builder, checks the referenced exchange exists and warns/errors at `Build`; recommend `Mandatory()` as the default discipline. Complements T68's server-side alternate-exchange net. **(EDA-02, P1; dep EG-1)**
- **T104** `SPEC.md §6.5` + `examples/retry_ladder/`: make the safe retry pattern the easy one (EDA-05/EDA-06) — ship a runnable, durable, **multi-tier TTL+DLX** backoff ladder (one queue per delay tier) so users don't reach for the lossy delayed-message exchange (R10-1, do-not-regress); §6.5 documents the **per-message-TTL head-of-line-blocking** trap and the queue-per-tier requirement (per EG-2). The example builds (`examples-build`) and smoke-runs (`examples-smoke`), demonstrating a message cycling through delay tiers and finally to the DLQ. **(EDA-05/EDA-06, P1; dep EG-2)**
- **T105** `SPEC.md §6.6` + a redrive helper + `examples/redrive/`: complete the DLQ lifecycle (EDA-09) — ship a minimal `DLQ → source` republish utility that dedupes by `MessageID` (preserving at-least-once) and is observable (metric/log); §6.6 documents the redrive pattern and its scope boundary. Integration test dead-letters N messages, runs the helper, and asserts they reappear on the source queue exactly once per `MessageID`. (DLQ *bounds* + Consumer *missing-DLX* validation land via T65.) **(EDA-09, P1)**
- **T106** `SPEC.md §6.9/§8` + `LATER.md`: state the event-evolution boundary (EDA-13) — additive change is safe (lax JSON / Postel, decision 23); **breaking** evolution (field removal, rename, semantic change) is user-owned via **versioned type names** (`order.created.v2`) / a new exchange / dual-publish; recommend the `Message.Type` discriminator convention for versioned events (an example or godoc branches on `Type` before decode). File **LATER-40** for a pluggable schema-registry/validation hook. **(EDA-13, P2; LATER-40)**
- **T107** `SPEC.md §6.9` + an example: document structured-mode CloudEvents routing opacity (EDA-14) — a structured event (`application/cloudevents+json` body), promoted by T97 (Lens 03) as the cross-ecosystem path, is **opaque to broker routing** (the broker cannot route on a `type`/`subject` that lives in the body); recommend setting the AMQP routing key / a routing header explicitly (or binary mode for 0-9-1 attribute routing). An example routes a structured CloudEvent by an explicitly-set routing key. **(EDA-14, P2)** — *coordinate with T97 (same §6.9 CloudEvents subsection).*
- **T108** `SPEC.md §6.4` + an example: defuse the batch partial-failure footgun (EDA-15) — §6.4 **prominently** documents (up front, not buried) that a `BatchConsumer` handler returning `nil` triggers one `basic.ack` with `multiple=true` over the **whole** range, acking an in-batch poison the handler never individually processed, and documents the per-delivery `Batch.Deliveries()` idiom for safe partial failure. A worked example demonstrates per-delivery ack/nack for a 1-poison-in-N batch (per EG-3). **(EDA-15, P1; dep EG-3)**
- **T109** `SPEC.md §6.7`: add an RPC usage-guidance preamble (EDA-12) — frame RPC as a deliberate-use primitive: the synchronous-coupling-over-async-transport caveat, when to prefer a normal request/response *event* pair, and the blind-retry/amplification consequence (cross-link T91's opt-in structured-error reply mode). **(EDA-12, P2; doc)** — *coordinate with T91 (Lens-02 structured-error RPC).*
- **T110** `SPEC.md §6.1`: clarify consumer-tag pinning (EDA-17) — document the stable-hash-of-consumer-tag pinning to consumer connections, the hot-spot risk at low connection/consumer counts (all tags hash to one socket with the default 2 connections), and whether adding consumer connections migrates live consumers (and the reconnect cost) or only affects new ones. If a code clarification is warranted it is scoped here. **(EDA-17, P3; doc)**

**Pulled forward into this phase (definitions in Phase 11; each is a v0.1 topology-expressiveness gap the EDA lens elevates):** T68 (alternate-exchange support, EDA-01 — the server-side unroutable catch-all; complements T103's client-side validation), T69 (exchange-to-exchange bindings, EDA-03 — layered ingest→per-domain fan-out).

**Extended in place (cross-lens, not re-filed):** T85 (Phase 13 dedupe-pattern rework) gains an EDA-18 acceptance criterion — the §6.2.1 L1067–1068 dangling `examples/idempotent_consume/` reference is closed by T85 + T38b; verify the directory ships and matches the reworked pattern.

**Sequencing:** T101 first → gates EG-1→T103, EG-2→T104, EG-3→T108, EG-4→T102. Land the topology-surface tasks together (T68, T69, T102, T103 all reshape `Topology`/publisher). T107 coordinates with T97; T109 coordinates with T91. T105/T106/T110 and the T85 extension are independent of the gates. SPEC sub-phasing: (A) T101; (B) T68, T69, T102, T103; (C) T104, T105; (D) T106, T107, T108, T109, T110, extend T85.

**Checkpoint Phase 15:**
- [ ] T101 gate results (EG-1..EG-4) captured on the existing integration lane (3.13 **and** 4.x) into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced.
- [ ] Ordered keyed streams scale (EDA-04/T102): `x-consistent-hash` `ExchangeKind` + partitioned-consumer wiring + a worked example preserve per-key order across N parallel-consumed queues; §6.6 documents the pattern + rebalancing trade-off (EG-4).
- [ ] Unroutable black-hole closed: `x-alternate-exchange` exposed (EDA-01/T68); publisher-side exchange-name validation against the wired `Topology` warns/errors at `Build`, and the silent-drop-without-`Mandatory()` behaviour is documented (EDA-02/T103, EG-1).
- [ ] Layered fan-out enabled (EDA-03/T69): exchange-to-exchange bindings declarable via `Topology` without breaking the declare-once/deep-snapshot semantics.
- [ ] Safe retry is the easy one (EDA-05/EDA-06/T104): `examples/retry_ladder/` ships (builds + smoke-runs) as a durable multi-tier TTL+DLX ladder; §6.5 documents the per-message-TTL HOL trap + queue-per-tier requirement (EG-2); the R10-1 delayed-exchange warning is preserved (do-not-regress).
- [ ] DLQ lifecycle complete (EDA-09/T105): a redrive helper republishes DLQ→source (dedupe by `MessageID`), observable; `examples/redrive/` ships; §6.6 documents the pattern. (DLQ bounds + Consumer DLX validation land via T65.)
- [ ] Event-evolution boundary stated (EDA-13/T106): §6.9/§8 document additive-safe / breaking-user-owned via versioned type names + the `Message.Type` convention; **LATER-40** files the schema-registry hook.
- [ ] Structured-mode routing opacity documented (EDA-14/T107): §6.9 states structured CloudEvents are routing-opaque + recommends an explicit routing key/header (coordinate T97).
- [ ] Batch footgun defused (EDA-15/T108): §6.4 prominently documents the `return nil` → `multiple=true` whole-range-ack trap + the per-delivery idiom; an example demonstrates 1-poison-in-N partial failure (EG-3).
- [ ] RPC usage framing (EDA-12/T109) and consumer-tag pinning clarity (EDA-17/T110) documented.
- [ ] `examples/idempotent_consume/` ships (EDA-18) via T85 + T38b, matching the reworked dedupe pattern; the §6.2.1 dangling reference is closed.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green (incl. the new examples' smoke run); `goleak.VerifyNone` clean.
- [ ] README "Available now / On the roadmap" synced (consistent-hash + alternate + e2e-binding topology, redrive helper, retry-ladder + schema-evolution guidance).
- [ ] SPEC §10 "Rev 14" note records the Lens-04 pass; no finding re-filed that a prior lens owns (EDA-07/08→T65, 10→T91, 11→T90, 16→T70); exactly one new `LATER.md` entry (LATER-40).

### Phase 16 — SRE & Production-Operability Re-review (Lens 05: detect/respond/verify, recovery-amplification, capacity honesty)

Goal: validate `SPEC.md` not from the API surface but from **the pager** — at 3am,
with this library in a billions/day flight path, *what do I see, what do I do, and
how do I know it worked?* At the §1 bar every failure mode must be **detectable**
before the customer notices, every recovery must be **bounded** (the self-healing
must not itself cause an outage), every capacity cliff must be **documented
honestly** (RTT + hardware), and every blast radius must be **contained** (one bad
queue / node / consumer must not take down everything). Closes the Lens-05
adversarial spec validation (`docs/spec-validation/05-sre-operability.md`; this pass
*conducted* the review — for every failure mode it answered the three on-call
questions detect/respond/verify — and produced findings `SRE-01..SRE-16`; planning
brief `docs/spec-validation/05-sre-operability-plan.md`). The lens verdict was
**GO-WITH-CHANGES** — no *new* §1 silent-message-loss path (the registry footgun is
a *loud* crash, not silent loss), and the five highest operability blockers
(R10-6/8/10/11/16 = T61/T63/T65/T66/T71) are **already pulled into v0.1** by Lenses
01/02. What remains is an observability-correctness set, two pull-forwards, and a
capacity-honesty doc fix.

Owner decisions (2026-05-28): (1) **pull both** remaining unclaimed operability
deferrals forward — **T67** (`Dial` partial-pool-connect policy, R10-12) *and*
**T72** (TCP keepalive / half-open write, R10-17); (2) **extend the default
histogram buckets** to cover the real latency envelope (add `10s, 30s, 60s`) so a
`ConfirmTimeout`/reconnect-barrier stall is visible, not collapsed into `+Inf`
(T113); (3) **aggregate consumer-subscription liveness** into `Connection.Health`,
closing the readiness/liveness probe false-green over a silently-dead consumer
(T115); (4) the throughput-ceiling fix is **doc-only** — §9/§6.2 state the
local-broker (sub-ms RTT) caveat + the `pool/RTT` ceiling + the remote collapse, and
the async/streaming publish-API decision **remains deferred in LATER-34** (T116).
The §8 "no globals" rule additionally **forces** the metrics-registration fix
(T112): the default is a **private per-`Connection` registry**, never
`prometheus.DefaultRegisterer`, and `WithMetricsRegisterer(prometheus.Registerer)`
is the injection point. **No new build-tag lane** — unlike Lens 02's `chaos` lane and
Lens 03's `interop` lane, Lens 05's gates ride the **existing integration lane**
(3.13 + 4.x) and the **existing `chaos` lane** (T84). Seven findings are **already
owned by prior-lens tasks** and are *not* re-filed (SRE-01→T61, SRE-02→T63,
SRE-03→T65, SRE-04→T66, SRE-05→T71, SRE-06→T62, SRE-07→T70) — each gains a
`Lens-05 (SRE-xx)` detect/respond/verify acceptance bullet. Phase 16 therefore adds
**no** new `LATER.md` entry. Per-task SPEC amendments land in the same PR as the code
(CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 15" note records the pass when the
first task lands. **Operability claims are tested by exercising the signal and the
recovery on a live broker / `chaos` lane, not asserting the code path in isolation.**
Gate task T111 runs first and **no SPEC edit to an affected section lands before its
gate returns**. Reverting any of the seven prior-lens pulls flips this lens to NO-GO.

- **T111** verification gates SG-1–SG-4 (unit + **existing** integration/`chaos` lanes, **3.13 and 4.x** where broker-bound — **no new build-tag lane**): capture ground truth for **SG-1** whether a second `Dial` in one process panics on duplicate Prometheus registration today (confirm the registerer is currently unspecified; a private-registry default removes the panic), **SG-2** whether a publish that stalls for the full 30s `ConfirmTimeout` lands in the `+Inf` bucket of `publisher_publish_seconds` under the default `[0.5…5000]`ms buckets (the invisible slow tail), **SG-3** whether a channel-only consumer death (404/`basic.cancel`/ack-error, TCP up) leaves `Connection.Health(ctx)` returning OK while the consumer is no longer subscribed (the readiness false-green), **SG-4** whether per-`Publish` throughput tracks `≈ pool/RTT` under injected confirm-RTT and a confirm spike drives `ErrChannelPoolExhausted` (the capacity cliff). Results table (under `docs/spec-validation/`) gates T112/T113/T115/T116. **(SRE gates, P0)**
- **T112** `options_connection.go` + `connection.go` + `metrics/` + `SPEC.md §6.9/§6.1/§8`: close the Prometheus registry footgun (SRE-10) — add `WithMetricsRegisterer(prometheus.Registerer)`; the **default is a private per-`Connection` registry**, never `prometheus.DefaultRegisterer` (a hidden global §8 forbids), wiring the connection-level default into the existing `NewPrometheus*` constructors (which already accept an injected registerer but have no caller today). §6.9 documents the injection. SG-1 unit test: two `Dial`s in one process with default metrics do **not** panic; an injected-registerer test asserts metrics land in the provided registry. **Lens-06 (GA-03):** this is the opt-in Prometheus *registry-injection* mechanism; it composes with T122's correction that the **default** metrics recorder is `NoOpClientMetrics` (not Prometheus) — T122 corrects the §6.1 L511 SPEC table, the injection is wired here. **(SRE-10, P0; dep SG-1)**
- **T113** `metrics/` + `SPEC.md §6.9`: extend the default latency histogram buckets (SRE-11) — the current default `[0.5…5000]`ms top bucket is below the real envelope (30s `ConfirmTimeout`, ~20s reconnect barrier, cross-region RTT), so slow/stalled publishes collapse into `+Inf` exactly when an incident needs them; add `10s, 30s, 60s` so the barrier-cap (SRE-02/T63) and `ConfirmTimeout` are visible; keep `WithLatencyBuckets` for tuning; §6.9 explains the envelope rationale. SG-2 mock-channel unit test withholds the ack for 30s and asserts the observation lands in a **finite** bucket. **Lens-09 (PC-12, confirm/do-not-regress):** confirm the SRE-11 extended buckets (`10s/30s/60s`) span the confirm-RTT capacity tail for p99/p999 (the default 5000 ms top bucket otherwise collapses 5–30s — with the 30s `ConfirmTimeout` — into `+Inf` exactly where capacity problems live); add a one-line §6.9 capacity-tail rationale. No re-file. **(SRE-11, P1; dep SG-2)**
- **T114** `connection.go` + `metrics/` + `SPEC.md §6.1/§6.9`: add a current-state degraded gauge (SRE-12) — today degraded mode fires only the *transition* counter `connection_degraded_total{role,reason}`; on-call needs "is this connection degraded *right now*". Add a `connection_degraded{role}` **gauge** (0/1) set to 1 on entering degraded state and 0 on the first successful redeclare; list it in §6.9 mandatory metrics. Unit/`chaos` test drives a connection into degraded state, asserts the gauge reads 1, then 0 after recovery. **(SRE-12, P2)** — *coordinate with T71's gauges (distinct metric, shared registration path).*
- **T115** `connection.go` + `consumer.go` + `batch_consumer.go` + `SPEC.md §6.1`: kill the `Health` readiness false-green (SRE-13) — `Connection.Health` checks socket + topology-degraded but **not** consumer-subscription liveness, so given silent channel death (SRE-01/T61) it returns OK while a consumer is dead (k8s keeps routing to a pod that consumes nothing). Aggregate consumer-subscription liveness into `Connection.Health` (returns non-nil while any registered consumer is not currently subscribed); document the semantics and that T61's self-heal returns it to green once resubscribed. SG-3 `chaos` test forces a channel-only consumer death and asserts `Health` returns non-nil while unsubscribed, then nil after T61 resubscribes. **(SRE-13, P1; dep SG-3, interacts with T61)**
- **T116** `SPEC.md §9/§6.2` + the benchmark suite: tell the capacity truth (SRE-14) — the §9 30k/100k targets hold only at sub-ms (local-broker) RTT; the per-`Publish` ceiling is `pool/RTT` (a pooled channel is held for a full confirm RTT, R10-18/LATER-34) and on a remote broker the achievable rate collapses 1–2 orders of magnitude, with a confirm-latency spike cascading into `ErrChannelPoolExhausted`. Amend §9/§6.2 to state the local-broker caveat, the `pool/RTT` ceiling, the remote collapse, and the cascade prominently; every throughput number carries RTT + hardware + broker config. The async-publish-API decision **remains LATER-34** (not pulled). SG-4 injected-RTT test demonstrates the `pool/RTT` relationship and the `ErrChannelPoolExhausted` onset. **Lens-09 (PC-02):** add the quantified pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT` per connection) so the `ErrChannelPoolExhausted` cascade onset under a confirm-latency spike (broker GC, disk sync, quorum Raft) is computable, not just narrated; confirm SG-4 covers the onset. dep PG-5. **(SRE-14, P1; dep SG-4; doc + benchmark caveat)**
- **T117** `metrics/` + `SPEC.md §6.9`: bound always-on label cardinality (SRE-15) — `routing_key`/`message_type` are already opt-in, but the always-on `queue`/`exchange` labels can OOM Prometheus at billions/day across thousands of queues/exchanges (a monitoring outage during an incident). §6.9 documents the cardinality budget (rough series-count math per queue/exchange) **and** adds an opt-out to omit/aggregate `queue`/`exchange` for very-high-fan-out deployments. Unit test asserts the opt-out drops the `queue`/`exchange` label. **(SRE-15, P2)**
- **T118** `SPEC.md §9/§6.9/§1`: ship the operator runbook + §1-bar audit (SRE-16) — tracing has a runbook-shaped `error.type`→action mapping but metrics do not. Add a runbook table mapping each mandatory metric → detect signal → operator action → recovery-verify signal, and an explicit **§1 "no silent X" bar → metric + example alert query** audit (surfacing that the redelivery leading indicator is SRE-05/T71 and the current-degraded signal is SRE-12/T114; any §1 bar without an always-on metric is silent in practice). A doc test / review checklist asserts every §1 bar and every mandatory metric appears with an alert query. **(SRE-16, P2; doc)** — *land last so the runbook references every metric the prior tasks added.*

**Pulled forward into this phase (definitions in Phase 11; the two remaining unclaimed operability deferrals the SRE lens elevates to v0.1 Blockers in spirit):** T67 (`Dial` partial-pool-connect policy → succeed-if-≥1-per-role + a degraded-capacity boot signal, SRE-08 — unpredictable restart/rollout otherwise), T72 (TCP keepalive / half-open write, SRE-09 — a publisher hung up to 30s on a dead socket otherwise).

**Extended in place (cross-lens, not re-filed — each is an operability finding already owned by a prior-lens task, gaining a `Lens-05 (SRE-xx)` detect/respond/verify acceptance bullet):** T61 (SRE-01 silent channel death — the readiness false-green driver), T63 (SRE-02 unbounded reconnect-barrier stall — cap ≤ the new histogram top bucket), T65 (SRE-03 unbounded DLQ → cluster-wide disk alarm, the highest blast radius), T66 (SRE-04 reconnect thundering herd), T71 (SRE-05 the #1 instability metric `consumer_redelivered_total` + pool saturation), T62 (SRE-06 redeclare amplification), T70 (SRE-07 deploy-drain loss/duplication). Reverting any of these pulls flips Lens-05 to NO-GO.

**Sequencing:** T111 first → gates SG-1→T112, SG-2→T113, SG-3→T115, SG-4→T116. SPEC sub-phasing: (A) T111; (B) metrics correctness — T112, T113, T117; (C) current-state signals + liveness — T114, T115 (coordinate with T61); (D) operability pull-forwards — T67, T72 (after the metrics so the new boot/half-open signals slot into the extended buckets + runbook); (E) capacity honesty & runbook — T116, T118 (land last so the runbook references every metric the prior tasks added).

**Checkpoint Phase 16:**
- [ ] T111 gate results (SG-1..SG-4) captured on unit + the **existing** integration/`chaos` lanes (3.13 **and** 4.x where broker-bound) into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced.
- [ ] Registry footgun closed (SRE-10/T112): `WithMetricsRegisterer` exists; the default is a **private per-`Connection` registry** (never `DefaultRegisterer`); a double-`Dial` in one process does **not** panic (SG-1).
- [ ] Incident latency visible (SRE-11/T113): default histogram buckets cover the 30s `ConfirmTimeout` + reconnect-barrier envelope; a 30s stall lands in a finite bucket, not `+Inf` (SG-2); §6.9 explains the envelope.
- [ ] Current-state degraded signal (SRE-12/T114): a `connection_degraded{role}` gauge reads 1 while degraded, 0 after recovery; listed in §6.9 mandatory metrics.
- [ ] Readiness false-green killed (SRE-13/T115): `Connection.Health` returns non-nil while a registered consumer is unsubscribed and nil after resubscribe (SG-3); semantics documented.
- [ ] Capacity honesty (SRE-14/T116): §9/§6.2 state the local-broker (sub-ms RTT) caveat, the `pool/RTT` ceiling, the remote collapse, and the `ErrChannelPoolExhausted` cascade; every throughput number carries RTT + hardware + broker config (SG-4); the async-API decision stays LATER-34.
- [ ] Cardinality bounded (SRE-15/T117): §6.9 documents the `queue`/`exchange` cardinality budget and ships an opt-out to omit/aggregate those labels.
- [ ] Runbook shipped (SRE-16/T118): a metric→detect→respond→verify table exists and every §1 "no silent X" bar maps to a metric + an example alert query.
- [ ] Pull-forwards landed: T67 ships succeed-if-≥1-per-role + a degraded-capacity boot signal (SRE-08); T72 ships dialer keepalive + a half-open-write test (a publish on a dead socket errors promptly, not at 30s) (SRE-09).
- [ ] Cut-line endorsed: T61/T62/T63/T65/T66/T70/T71 each carry a `Lens-05 (SRE-xx)` detect/respond/verify acceptance bullet; none re-filed, none re-pulled; reverting any pull would flip Lens-05 to NO-GO.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) **and** the `chaos` lane green; `goleak.VerifyNone` clean.
- [ ] README observability/reliability copy synced (`WithMetricsRegisterer`, the default-bucket change, the `connection_degraded` gauge, `Health` consumer-liveness, the cardinality opt-out, the honest §9 ceiling).
- [ ] SPEC §10 "Rev 15" note records the Lens-05 pass; no finding re-filed that a prior lens owns (SRE-01→T61, 02→T63, 03→T65, 04→T66, 05→T71, 06→T62, 07→T70); **no** new `LATER.md` entry.

### Phase 17 — Go API & Library-Design Re-review (Lens 06: discoverability, hard-to-misuse, forever-stable surface, safe zero values)

Goal: validate `SPEC.md` not from the wire or the pager but from **the seat of the
library consumer who never reads the docs** — for every exported identifier, can they
fall into the pit of success? A library's API is its most permanent decision: a wire
format can be versioned, but an exported signature is forever once people depend on
it. At the §1 bar every exported identifier must be **discoverable**,
**hard-to-misuse**, **stable across the planned v0.1→v1.0→post-v1 evolution**, and
have a zero value that is either correct-by-default or loudly-invalid — **never
silently-wrong**. Closes the Lens-06 adversarial spec validation
(`docs/spec-validation/06-go-api-library-design.md`; this pass *conducted* the review —
for every exported identifier it answered the four lens questions
discoverable/hard-to-misuse/stable/safe-zero-value — and produced findings
`GA-01..GA-16`; planning brief `docs/spec-validation/06-go-api-library-design-plan.md`).
The lens verdict was **GO-WITH-CHANGES** — the public surface is fundamentally sound
(the `PublisherFor[M]`/`ConsumerFor[M]` generics split is the cleanest practical Go
expression, the zero-value defaults are *mostly* safe, the concrete-struct decision-9
choice is forward-compatible, the error taxonomy is navigable) — but the review found
**one Blocker that is not an API-shape flaw at all: a silent durability loss.** A
zero-valued `Message[M]{}` ships **non-persistent** on the wire because
`buildPublishing` (`publisher.go:946`) casts the `DeliveryMode` enum raw instead of
translating `0→2`, directly violating the §6.5 "durable-by-default" headline and the
§1 "no silent message loss" bar — and it is **unverified by any wire-level test**
(GA-01/T120). What remains is one silent-observability gap, an honest-defaults set,
compatibility hardening to keep the Phase-11 roadmap additive before the first tag,
error-model correctness, and deliberate-choice documentation.

Owner decisions (2026-05-28): (1) **GA-02 observability inheritance = reword
independent** — the spec is corrected to state tracer *and* metrics are configured
**independently** at the connection and builder levels (no inheritance), each
defaulting to NoOp; the "builder-overrides-connection" clause is struck from decision
44 / §6.1 (doc-only, matches the code) (T121); (2) **GA-03 metrics default = NoOp
(correct the SPEC)** — a library must not auto-wire a concrete backend or a global
registry (§8), so §6.1 L511 / §3 are corrected to "NoOp (opt-in Prometheus)" and the
opt-in registry injection stays owned by T112 (T122); (3) **GA-04 `PrefetchBytes` =
cut** — remove the no-op from both consumer builders, list it in §6 "intentionally
not exposed" alongside `Immediate()`/`NoLocal()`, and record the removal in decision
10 (T123); (4) **GA-05 exchange→exchange binding shape = separate
`Topology.ExchangeBindings`** — a new typed `ExchangeBinding{Source, Destination,
RoutingKey, NoWait, Args}` slice on `Topology`; the existing `Binding` is **not**
reshaped, pinned in §6.6 now so T69 cannot implement the breaking variant (T124 +
T69 bullet). **GA-01's fix direction is unambiguous** (translate enum→wire, keep the
§6.5 contract authoritative) and needs no owner input. **No new build-tag lane** —
unlike Lens 02's `chaos` lane and Lens 03's `interop` lane, the gates GG-1..GG-4 are
unit/mock-channel characterizations on the existing unit lane; only GA-01's on-broker
persistence assertion rides the **existing integration lane** (3.13 **and** 4.x).
Five findings are **already owned by prior-lens / Phase-11 tasks** and are *not*
re-filed (GA-03→T112, GA-05→T68/T69, GA-06→T70/T71, GA-09→T37) — each gains a
`Lens-06 (GA-xx)` acceptance bullet. Phase 17 therefore adds **one** new `LATER.md`
entry (LATER-41, a dedicated `ReturnCode` accessor). Per-task SPEC amendments land in
the same PR as the code (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 16" note
records the pass when the first task lands. **API claims are tested by exercising the
surface — the wire output, the emitted spans, the default recorder type — not by
asserting the API-level constant in isolation.** Gate task T119 runs first and **no
SPEC edit to an affected section, and no fix, lands before its gate returns.**

- **T119** verification gates GG-1–GG-4 (unit + **existing** integration lane for GG-1's persistence check, **3.13 and 4.x** where broker-bound — **no new build-tag lane**): capture ground truth for **GG-1** that a zero-valued `Message[M]{Body:&x}` currently produces `amqp091.Publishing.DeliveryMode == 0` (transient) — i.e. the §6.5 `0→2` persistent mapping is **absent** in `buildPublishing` today — and confirm on the broker that such a message does **not** survive a restart; **GG-2** that with `Dial(WithTracer(realTracer))` and a `PublisherFor[M]` that never calls `.Tracer(...)`, the publish path emits **NoOp spans** (no builder reads `conn.opts.tracer`/`metrics`); **GG-3** that with no `WithMetrics(...)` the default `Connection` metrics recorder is **`NoOpClientMetrics`** (not Prometheus) and there is **no** caller of `NewPrometheus*` in non-test code (so the §6.1 L511 "Prometheus default" is factually wrong); **GG-4** that `PublisherFor[Order](conn).Codec(codec.NewProtobuf())` **compiles** and fails only at the first `Publish` with `ErrInvalidMessage` (the codec↔`M` mismatch is runtime, not compile). Results table (under `docs/spec-validation/`) gates T120/T121/T122/T128. Records §10 **Rev 16**. **(GA gates, P0)**
- **T120** `publisher.go` + `SPEC.md §6.5`: fix the silent durability loss (GA-01, **Blocker**) — `buildPublishing` (`publisher.go:946`) casts `uint8(msg.DeliveryMode)` raw, so a zero-valued `Message[M]{}` ships wire `0` (**transient**), the opposite of the §6.5 durable-by-default headline and a §1 silent-message-loss violation; the `basic.return` rehydration path (`publisher.go:310`) has the same cast. Translate enum→wire at the choke point: `DeliveryModePersistent(0)→2`, `DeliveryModeTransient(1)→1` (and the return path); keep the §6.5 contract **authoritative** (do not weaken it); add the explicit wire-value table to §6.5 if absent. GG-1 unit test asserts `buildPublishing(Message[M]{Body:&x}).DeliveryMode == 2` and the transient case `== 1`; an integration test (3.13 **and** 4.x) publishes a zero-valued message, restarts the broker, and asserts it survives. **(GA-01, P0; dep GG-1)**
- **T121** `SPEC.md §6.1/§10 dec.44`: reword observability to independence (GA-02, High; owner decision) — decision 44 + §6.1 imply a publisher/consumer inherits the connection's `WithTracer`/`WithMetrics`, but builders default to `NoOpTracer`/`NoOp*Metrics` and **never read** `conn.opts.tracer`/`metrics`, so `Dial(WithTracer(t))` yields NoOp spans on every publisher with no error. Strike the "builder-overrides-connection" clause from decision 44 and §6.1; state plainly that tracer *and* metrics are configured **independently** at each level (each defaults to NoOp; connection-level observability covers lifecycle/pool events only) and that to instrument a publisher/consumer the caller must set `.Tracer(...)`/`.Metrics(...)` on the builder. GG-2 unit test asserts a builder that never set `.Tracer(...)` emits NoOp spans even under a real connection tracer. Doc-only (matches the code). **(GA-02, P1; dep GG-2)**
- **T122** `SPEC.md §6.1/§3/§9` (+ `Lens-06` bullet on T112): make the metrics default honest (GA-03, Med; owner decision NoOp) — §6.1 L511 / §3 L117 say `WithMetrics` defaults to Prometheus; the code defaults to **NoOp** (`connection.go:641`). Correct §6.1 L511 + §3 L117 to "NoOp (opt-in Prometheus via `metrics.NewPrometheus*`)"; add one sentence to §9/§6.9 stating the NoOp-default rationale (globals-free; inject your own registerer). The registry-injection mechanics (`WithMetricsRegisterer`, private per-`Connection` registry) stay owned by **T112** (SRE-10). GG-3 unit test asserts the default `Connection` metrics is `NoOpClientMetrics`. **(GA-03, P1; dep GG-3)**
- **T123** `consumer_builder.go` + `batch_consumer_builder.go` + `SPEC.md §6.3/§10 dec.10`: cut the `PrefetchBytes` no-op footgun (GA-04, Med; owner decision cut) — `PrefetchBytes(_ uint)` (`consumer_builder.go:69`, `batch_consumer_builder.go:95`) accepts a byte budget, discards it, and silently does nothing on RabbitMQ. Remove it from both builders; list it in the §6 "intentionally not exposed" set alongside `Immediate()`/`NoLocal()`; amend decision 10 to record the removal (was "kept with no-op note"). A doc/grep test asserts no `PrefetchBytes` in the public surface. Pre-tag-safe removal (it never had an effect); must land before the first tag. **(GA-04, P2)**
- **T124** `SPEC.md §6.6/§9/§10 dec.24` (+ `Lens-06` bullets on T68/T69): pin the topology roadmap additive (GA-05/GA-16, owner decision) — spec a **separate `Topology.ExchangeBindings []ExchangeBinding`** with `ExchangeBinding{Source string, Destination string, RoutingKey string, NoWait bool, Args Headers}` in §6.6 **now**, so R10-14/T69 cannot reshape `Binding` (the destination-to-queue struct stays untouched; no `Source`/`Destination` rename); R10-13/T68 alternate-exchange stays additive via `Exchange.Args` / an optional field. Add a §9 additive-only-after-first-tag gate ("no exported §6 type changes field names or removes fields after `v0.1.0`; topology extensions T68/T69/T102 and stream-consume v0.2 are additive-only") + a one-line `rc1`-is-pre-`v0.1.0` clarification. Commit decision 24 to a **purely additive** v0.2 stream-consume API (`StreamOffset`/`StreamConsumerFor[M]` + additive `Delivery[M]` methods; `x-stream-offset` via `Args` in v0.1). Acceptance: §6.6 specs `ExchangeBinding`; T69 carries a no-`Binding`-field-rename bullet; the deep-snapshot/declare-once semantics extend to `ExchangeBindings`. **(GA-05/GA-16, P1)**
- **T125** `SPEC.md §6.9/§10` + `metrics/` + `log/` + `otel/` (+ `Lens-06` bullets on T70/T71): make the user-implementable interfaces extension-tolerant (GA-06, Med-High) — R10-15/T70 + R10-16/T71 add methods to the exported `metrics.*` interfaces; adding a method to an exported interface breaks every external implementer at compile time, and the §9 `// Deprecated`-free rc1→v1.0 rule (L2439) forbids fixing it after rc1. Define a SPEC policy (§6.9 note + a §10 decision): the `metrics`/`log`/`otel` interfaces ship with an **embeddable NoOp base struct** (e.g. `metrics.NoOp`) users embed, so adding interface methods is forward-compatible (the embed satisfies new methods as no-ops); document that all v0.1 metric additions land before the first tag. Acceptance: the SPEC states the embeddable-base policy; an example shows a custom adapter embedding the base surviving a method addition; T70/T71 carry the `Lens-06 (GA-06)` bullet. **(GA-06, P1)**
- **T126** `errors.go` + `SPEC.md §6.8/§6.3` + `LATER.md`: fix the error-model contradictions (GA-07/GA-08/GA-15) — (GA-07) §6.8 L1951 lists reply code `311` as transient while L1970 lists it permanent, contradicting the code (`errors.go:207–268`, 311 permanent-only): remove 311 from the §6.8 transient list (code authoritative); state the transient/permanent partition is **partial** and `ErrUnroutable` (312/313) is deliberately in **neither** set (callers branch on the sentinel); define precedence — **`ErrTransient` in the chain wins** for re-classification (or `IsPermanent` returns false when `ErrTransient` is also present) — and add a test that a 506-wrapped-with-`ErrTransient` classifies transient (the §6.8 L1957 re-classify-506 guidance currently produces a both-true error a permanent-first caller drops). (GA-08) amend §6.8 to warn `AMQPCode` MAY return a `basic.return` code (312/313) and callers needing to distinguish must combine with `errors.Is(err, ErrUnroutable)`; file **LATER-41** for a dedicated `ReturnCode(err) (uint16, bool)` accessor. (GA-15) §6.8 notes `ErrTopologyMismatch` is a named alias over `ErrPreconditionFailed` for the declare path and §6.3 notes `ErrPoison` and a bare handler error are behaviourally identical (intent-only); correct any "~30 sentinels" figure to 40. **(GA-07/GA-08/GA-15, P2)**
- **T127** `SPEC.md §6.1/§6.2` (and code where SPEC is the intended contract): reconcile the surface signature drift (GA-12, Med) — `WithOnResubscribe` appears in the §6.1 table (L508) but no such option exists (the callback lives in prose at L629); `WithDialer` is documented as `net.Dialer` but is a dial-func (`options_connection.go:176`); `WithFrameMax` is `uint32` (not `int`); `WithChannelMax` is `uint16` (untyped in table); `PublishResult{Index int; Err error}` (L776–779) is `{Err error}` in code; §6.2 `Return.Body []byte` and `ReturnedProperties.Expiration string` drift (`Expiration` is `time.Duration`). For each, make the SPEC match the implementation (or implement the documented option where it is the intended contract — decide `WithOnResubscribe`: table or prose, not both). Acceptance: every §6.1/§6.2 signature matches a code `file:line`; the phantom option is resolved. **(GA-12, P2)**
- **T128** `SPEC.md §10/§5/§6.5/§6.9/§8`: document the deliberate `any`/generics/struct choices (GA-10/GA-11/GA-14) — add a §10 decision that `codec.Codec` is intentionally **non-generic** (a payload↔codec mismatch is a runtime `ErrInvalidMessage`, not a compile error; each non-JSON codec documents its required `M` — `proto.Message` for Protobuf, `codec.CloudEvent` for CloudEvents — and fails fast), cross-referenced from §5/§8's "no `any` where a generic would do"; a §6.5 note explaining `Message[M].Body *M` (publish/consume symmetry with `Delivery[M].Body() *M`; loud nil-`Body` `ErrInvalidMessage`, never a silent drop); a §6.9 `HeaderCodec` author caveat (opt-in requires the full method set; recommend `var _ codec.HeaderCodec = (*MyCodec)(nil)`); a §8 closed list of sanctioned `any` (Headers / `*.Args` / `WithClientProperties` / OTel carriers = field-tables; `log` printf variadics; the codec `v any` per GA-10); and the GA-09 fixture unkeyed-literal guard note (coordinated with the T37 lightweight-fixture bullet). GG-4 doc example shows the runtime-mismatch contract. **(GA-10/GA-11/GA-14, P2; dep GG-4)**
- **T129** `SPEC.md §5/§10 dec.44` + a `consumer_builder.go` doc fix: close the naming-grammar gaps and scope last-wins honestly (GA-13, Low) — add §5 carve-outs for the lone `WithoutMetrics()` builder method (sanctioned metrics-disable exception), the `Use*`/`Allow*` verb-prefix builder methods (`UseExclusiveReplyQueue`, `AllowMissingDLX`), and the noun-phrase setters (`MaxMessageSizeBytes`/`PublishBatchMaxSize`, deliberately field-style for explicitness); scope decision 44's "last-wins" to **value-carrying** options (boolean flag-setters — `Mandatory`/`StampUserID`/`ChannelQoS`/`Exclusive`/`AutoAck`/`WithoutMetrics` — are monotonic-set, no inverse); fix the `consumer_builder.go:72` `ChannelQoS` godoc bug (says `global=false`; code sets `global=true`, `consumer.go:460`) and add the `basic.qos global=true` mapping to the §6.3 doc. **(GA-13, P3)**

**Extended in place (cross-lens, not re-filed — each is an API/library-design finding already owned by a prior-lens / Phase-11 task, gaining a `Lens-06 (GA-xx)` acceptance bullet):** T37 (GA-09 lightweight `Delivery[M]`/`Batch[M]` fixture path with no `go.uber.org/mock` dependency), T68 (GA-05 alternate-exchange exposed additively, no `Exchange` field rename), T69 (GA-05 destination-exchange shape pinned to a separate `ExchangeBindings`; `Binding` not reshaped), T70 (GA-06 the new `consumer_shutdown_requeued_total` lands behind an embeddable `metrics.NoOp` base), T71 (GA-06 the new gauges/counters land behind the embeddable base, before rc1), T112 (GA-03 opt-in Prometheus registry-injection composes with the corrected NoOp default).

**Sequencing:** T119 first → gates GG-1→T120, GG-2→T121, GG-3→T122, GG-4→T128. SPEC sub-phasing: (A) T119 (gate, records Rev 16); (B) silent-loss fixes — T120 (the Blocker) + T121 (observability independence) land first; (C) honest defaults & footgun removal — T122 (+ bullet T112), T123; (D) compatibility hardening (pin before any tag) — T124 (+ bullets T68/T69), T125 (+ bullets T70/T71) must complete before T46 cuts `v0.1.0`; (E) error-model + surface accuracy + docs — T126 (→ LATER-41), T127, T128 (GG-4), T129 land last so the docs reference the corrected surface.

**Checkpoint Phase 17:**
- [ ] T119 gate results (GG-1..GG-4) captured on unit + the **existing** integration lane (3.13 **and** 4.x for GG-1's persistence check) into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced; first task records §10 **Rev 16**.
- [ ] Silent durability loss fixed (GA-01/T120): `buildPublishing` translates `DeliveryModePersistent(0)→wire 2`, `DeliveryModeTransient(1)→wire 1` (and the `basic.return` path); a unit test asserts the wire value and an integration test (3.13 **and** 4.x) proves a zero-valued message survives a broker restart; §6.5 contract unchanged.
- [ ] Silent observability loss documented (GA-02/T121): §6.1 + decision 44 state tracer and metrics are configured **independently** at each level (no inheritance); a test locks that a builder without `.Tracer(...)` emits NoOp spans even under a real connection tracer.
- [ ] Defaults honest (GA-03/T122): §6.1 L511 + §3 read "NoOp (opt-in Prometheus)"; a test asserts the default `Connection` metrics is `NoOpClientMetrics`; T112 owns the registry-injection opt-in.
- [ ] Footgun removed (GA-04/T123): `PrefetchBytes` is gone from both builders and listed in §6 "intentionally not exposed"; decision 10 records the removal.
- [ ] Roadmap pinned additive (GA-05/GA-16/T124): §6.6 specs `Topology.ExchangeBindings []ExchangeBinding` (`Binding` untouched); §9 carries the additive-only-after-first-tag gate + rc1 clarification; decision 24 commits the v0.2 stream API additive; T68/T69 carry the `Lens-06` no-field-rename acceptance.
- [ ] Interfaces extension-tolerant (GA-06/T125): the `metrics`/`log`/`otel` interfaces ship an embeddable NoOp base; an example shows a custom adapter surviving a method addition; T70/T71 land before the first tag.
- [ ] Error model sound (GA-07/GA-08/GA-15/T126): §6.8 lists 311 permanent-only; the transient/permanent precedence rule is specified + tested; the partial partition and `ErrUnroutable`-in-neither are documented; the `AMQPCode` frame-class caveat exists; LATER-41 (`ReturnCode`) is filed.
- [ ] Surface matches code (GA-12/T127): every §6.1/§6.2 signature matches a code `file:line`; the `WithOnResubscribe` phantom is resolved.
- [ ] Deliberate choices documented (GA-09/GA-10/GA-11/GA-14/T128): §10 records the non-generic-codec decision; §6.5 explains `Body *M` + loud-invalid; §6.9 has the `HeaderCodec` author caveat; §8 lists the sanctioned `any` exceptions; the fixture unkeyed-literal guard is noted (coordinated with the T37 lightweight-fixture bullet).
- [ ] Naming + last-wins honest (GA-13/T129): §5 carve-outs exist; decision 44 scoped to value-setters; the `ChannelQoS` godoc matches the code.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced (the metrics-default correction, the `PrefetchBytes` removal, the `ExchangeBindings` addition, the independent-observability semantics).
- [ ] SPEC §10 "Rev 16" note records the Lens-06 pass; no finding re-filed that a prior task owns (GA-03→T112, GA-05→T68/T69, GA-06→T70/T71, GA-09→T37); exactly **one** new `LATER.md` entry (LATER-41); T119–T129 contiguous, no duplicate IDs.

### Phase 18 — Security & Threat-Modeling Re-review (Lens 07: credential confidentiality, fail-closed controls, untrusted-input boundedness, supply-chain surface)

Goal: validate `SPEC.md` from the seat of an **application-security engineer who
assumes the broker is compromised, the network is hostile, and producers are
attacker-controlled**. The lens bar is binary: every security control must be
**fail-closed**, every egress path (log line, metric label, error string, span
attribute, panic-recover value, returned-message handler) must be credential-
**and** payload-safe, and every untrusted-input ingress (consumed body, broker
headers incl. `x-death`, `basic.return` data, broker error strings) must be
**size-bounded and panic-safe**. A doc-only mitigation for a runtime risk is a
*partial* control; the runtime guard is the real control. Closes the Lens-07
adversarial spec validation (`docs/spec-validation/07-security-threat-modeling.md`; this
pass *conducted* the review — every trust boundary was enumerated, every egress
audited for credential- and payload-safety, every ingress audited for boundedness
and panic-safety — and produced findings `ST-01..ST-14`; brief
`docs/spec-validation/07-security-threat-modeling-plan.md`). Lens verdict:
**GO-WITH-CHANGES** — the posture is fundamentally sound (the `internal/redact`
choke-point holds on owned egress incl. wrapped errors via `redact.Error`; the
codec/handler panic-recover is **type-only** (`%T`, never the recovered value's
content); bodies are never logged and never decompressed; every internal buffer is
bounded; `403 ACCESS_REFUSED` is permanent and never publish-retried; SASL EXTERNAL
is fail-closed-validated; `UserID` is client-side-checked) — but the review found
**one must-fix Blocker, a fail-open inbound denial-of-service**: there is **no
consume-side message-size cap** (`MaxMessageSizeBytes` §6.2 L796 is publish-side
only), so a single hostile or buggy producer can ship a ~512 MiB body that
`amqp091-go` reassembles **in memory** before the codec runs, OOMing the consumer
(the security analog of the Lens-06 GA-01 Blocker; this hole was previously tracked
**LOW as LATER-35** and is **re-classified Blocker and promoted to T131**). Plus
**one High** fail-open confidentiality gap (ST-01: PLAIN credentials sent
base64-cleartext over `amqp://` with no warning, asymmetric to the fail-closed
EXTERNAL validation) and a set of egress-hardening, transport-hygiene, supply-chain,
and test corrections. No redesign required.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's
D1–D5; each is reversible/additive and may be overridden before execution): (1)
**D1 ST-06 inbound cap** = ship a consume-side `MaxInboundMessageSizeBytes` (default
**16 MiB**, mirrors the publish guard; `0` disables explicitly); over-cap →
`ErrMessageTooLarge` + `Nack(requeue=false)` (DLQ if wired) + drop metric;
**per-message** for `BatchConsumer` (consistent with the publish guard). (2) **D2
ST-01 cleartext auth** = **warn-only at `Dial`** for v0.1 (does not break local/dev
`amqp://`); an opt-in acknowledgement (`AllowInsecureAuth()`) considered for v1.0.
(3) **D3 ST-10 codec split** = **build-tag** the CloudEvents codec (non-breaking,
keeps the core dependency-light) for v0.1; a sub-module split (breaking, §8
Ask-first) is deferred to **LATER-44** only if the owner overrides to defer. (4)
**D4 ST-04 EXTERNAL principal** = **doc-only for v0.1** (document the CN assumption +
`ssl_cert_login_from` divergence, recommend empty `UserID` under non-CN mappings);
configurable SAN/DN extraction deferred to **LATER-42**. (5) **D5 ST-09 reconnect on
permanent auth** = **stop + surface `ErrAccessRefused` + degraded signal** (do not
loop on 403), confirmed against TG-4's observed behaviour before choosing
stop-vs-bounded-retry. **No new build-tag lane for the gates** — TG-1..TG-5 ride the
existing unit + `integration` lanes (3.13 **and** 4.x where broker-bound), mirroring
Lenses 04/05/06; the codec build-tag in T135 is a *compilation* tag, not a CI test
lane. **One** finding is already owned by a prior-lens task and is **not** re-filed —
**ST-08** (unbounded auto-DLQ disk-fill → broker-wide `connection.blocked`) extends
**T65** with a `Lens-07 (ST-08)` availability/DoS bullet. Exactly **one** new
`LATER.md` entry (LATER-42; LATER-44 only if D3 is overridden to defer); the ST-06
fix **promotes and supersedes the pre-existing LATER-35**. Per-task SPEC amendment
lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 17" note records
the pass. **Gate task T130 runs first**; no SPEC edit to an affected section, and no
fix, lands before its gate returns.

- **T130** verification gates TG-1–TG-5 (unit + **existing** `integration` lane, 3.13 **and** 4.x where broker-bound — **no new build-tag lane**): capture ground truth before any wording/fix lands (gate-first, mirroring T84/T94/T101/T111/T119) for **TG-1** that a real `amqp091.Dial`/`DialConfig` failure with `amqp://user:secret@badhost` does **not** surface `secret` in warren's returned/wrapped error chain, any `log` adapter line, **or** the `amqp091` package-level `Logger` (capture the raw driver error-string shape); **TG-2** that a single ~256 MiB body published to a queue a warren consumer is subscribed to is reassembled by `amqp091` **in memory before the codec runs** with **no consume-side cap** (measure the consumer RSS, quantify the OOM headroom, confirm it scales with body size independent of `FrameMax`); **TG-3** that EXTERNAL principal extraction = **CN** of the first client cert (`connection.go:122`) and characterise the client-side `UserID` check when the broker's `ssl_cert_login_from` is `common_name` vs `distinguished_name` vs `subject_alternative_name` (does it diverge into a false reject / a broker 406?); **TG-4** that forcing a reconnect against revoked/denied credentials makes the supervisor loop on **403 `ACCESS_REFUSED`** indefinitely (capture the loop timing + auth-backend log-spam) or surface/degrade; **TG-5** via `go list -deps ./...` + `go mod graph` quantify the transitive package/module surface `cloudevents/sdk-go/v2` adds to a **core** (non-cloudevents) import and confirm a user cannot avoid it today (direct, unconditional require). Results table (under `docs/spec-validation/`) gates T131/T133/T135/T136/T138. Records §10 **Rev 17**. **(ST gates, P0)**
- **T131** `consumer_builder.go` + `batch_consumer_builder.go` + `consumer.go` + `batch_consumer.go` + `SPEC.md §6.2/§6.3` + `errors.go` + `metrics/`: close the fail-open inbound DoS (ST-06, **Blocker**; **promotes and supersedes LATER-35**, re-classified from LOW to Blocker by the security lens) — there is **no** consume-side body-size cap, so `amqp091-go` reassembles a delivery up to RabbitMQ's ~512 MiB practical limit **in memory** before warren's codec runs (`FrameMax` bounds frames, not the reassembled body; the dispatch channel is bounded by *count* not bytes, `consumer.go:476`). Add `MaxInboundMessageSizeBytes` to both consumer builders (default **16 MiB** mirroring the publish guard; `0` disables explicitly); an oversized delivery is rejected **before** the codec runs, fail-closed: `Nack(requeue=false)` + a classifiable `ErrMessageTooLarge` (routed to the DLQ if one is wired, so it is observable, never a silent drop) + a `too_large` drop metric; **per-message** for `BatchConsumer`. §6.2's frame-size prose (L703–727) gains an explicit "frame-max bounds frames, not the reassembled body — the inbound cap is the body guard" note; §6.3 documents the symmetry with the publish guard and that the cap measures the **encoded body** (like the publish guard; cf. LATER-37 for the HeaderCodec-header gap). TG-2 integration test publishes an over-cap body and asserts the consumer RSS stays bounded and the message lands in the DLQ (when wired), not an OOM; a unit test asserts the pre-codec reject + the metric. **(ST-06, P0; dep TG-2)** — the lone Blocker; land first.
- **T132** `connection.go` + `options_connection.go` + `SPEC.md §6.1`: close the cleartext-credential fail-open asymmetry (ST-01, High; owner decision D2 = warn-only) — decision 35 makes SASL EXTERNAL **fail-closed** (`Dial` rejects EXTERNAL without TLS), but the symmetric exposure — **PLAIN credentials over plain `amqp://`** (password base64-cleartext on the wire) — is **fail-open** with no warning. Emit a `Dial`-time warning through the `log` adapter when `WithAuth`/PLAIN is used over a non-TLS `amqp://` endpoint; document the exposure in §6.1 Authentication alongside the EXTERNAL fail-closed block. Warn-only for v0.1 (no behaviour break; `amqp://` still works); an opt-in acknowledgement (`AllowInsecureAuth()`) noted for v1.0. A unit test asserts the warning fires for PLAIN-over-`amqp://` and that EXTERNAL-over-`amqp://` remains a fail-closed `ErrInvalidOptions`. **Lens-11 (DP-08):** security of processing (GDPR Art. 32 / LGPD Art. 46) — the `Dial`-time warning + the §6.1 text must name **personal data in transit** (not only credentials) as the Art. 32 exposure and discourage `amqp://` for any flow carrying personal data. **(ST-01, P1)**
- **T133** `connection.go` + `SPEC.md §8/§6.9`: guarantee redaction of **wrapped underlying errors** + neutralise the un-owned `amqp091` `Logger` (ST-02, Med) — §8 L2353 already says "every error message that includes an AMQP URI"; make explicit that this covers errors **wrapped from `amqp091`** (not only wrapper-formatted strings; the wrapped dial error *is* re-redacted at `connection.go:397`, but it is not spec-pinned for wrapped errors nor e2e-tested against the real driver string, the realistic credential carrier). Pin or document the `amqp091` package-level `Logger` (the egress the wrapper does not own): set it to a redacting adapter or a no-op by default, or document that callers who enable it must redact. An **end-to-end** test dials a bad host with `amqp://user:secret@…` and asserts `secret` appears in no returned error string, no `log` line, and no `amqp091` `Logger` output. **(ST-02, P1; dep TG-1)**
- **T134** `SPEC.md §8/§6.9` + tests: make every egress payload-safe, not just credential-safe (ST-03 + ST-14, Med + Low-Med — code already correct; this locks the spec + adds regression tests) — (ST-03) add **"Never log message payloads / bodies"** to the §8 *Never* list (today only credentials are listed; `Return.Body` and `Delivery` bodies may carry PII/secrets); add a §9 criterion backed by a grep/AST test that no non-test code path formats a body into a log/error string, plus a runtime test that the `OnReturn` and decode-error paths emit no body bytes. (ST-14) correct §6.9 L2047 "wrapping the recovered value" → **"wrapping the recovered value's *type* only — never its content"** (the code stores `%T`; the current wording blesses a payload leak via a panic message); lock with a test that a codec panicking with a payload-bearing value yields an error string containing no payload bytes. **Lens-11 (DP-04):** integrity & confidentiality (GDPR 5(1)(f)/Art. 32 / LGPD Art. 46) — frame the "never log message bodies / header values" §8 *Never* boundary as a **data-protection** control (a body/header debug log is unlawful processing **+ retention** of personal data in the log store, not only a secrets leak); the grep/AST regression test is the privacy lock. **(ST-03/ST-14, P2)**
- **T135** `SPEC.md §2/§6.9` + `codec/cloudevents.go` (+ build tags) + `go.mod`: shrink the supply-chain surface (ST-10, Med; owner decision D3 = build-tag) — `cloudevents/sdk-go/v2` is an **unconditional direct dependency** (`go.mod:6`) every warren user pulls for a codec most won't use, a large transitive attack surface. Build-tag the CloudEvents codec behind `//go:build` so a core (non-cloudevents) import does **not** pull `sdk-go/v2`'s transitive closure (assess Protobuf the same way); §2 (deps) + §6.9 amend to state the core stays dependency-light and how to opt into the heavy codecs. A build/import test proves a core import excludes `cloudevents/sdk-go/v2`. If the owner overrides D3 to a sub-module split (breaking, import-path change → §8 Ask-first gate) or to accept+document, file **LATER-44** for the full split and ship T135 as doc-only. **(ST-10, P1; dep TG-5)**
- **T136** `SPEC.md §6.1/§6.5/§10 dec.35` + `LATER.md`: document EXTERNAL principal extraction + the `ssl_cert_login_from` divergence (ST-04, Med; owner decision D4 = doc-only) — warren extracts the EXTERNAL principal from the cert **CN** (`connection.go:122`), but RabbitMQ's `ssl_cert_login_from` is configurable (CN / DN / SAN); the existing R10-4 caveat (L3070) covers username-*rewriting* backends, not the CN-vs-SAN **extraction** divergence, so the client-side `UserID` check can mis-fire (false reject, or a value the broker then 406s). Document in §6.5/§6.1 + decision 35 that warren uses the CN and that `ssl_cert_login_from` must match; extend the R10-4 caveat to the extraction divergence; recommend leaving `UserID` empty under non-CN broker mappings. File **LATER-42** for configurable SAN/DN extraction. **(ST-04, P2; dep TG-3)** — files LATER-42.
- **T137** `connection.go` + `SPEC.md §6.1`: turn the `InsecureSkipVerify` partial control into a runtime control + state the TLS floor (ST-11, Low-Med) — TLS is passed verbatim (correct; warren never weakens the caller's `*tls.Config`, never sets `InsecureSkipVerify`, `options_connection.go:114`), but a doc-only mitigation (L758–759) does not stop a caller from setting `InsecureSkipVerify=true` on an `amqps://` connection (silently defeating cert validation). Emit a `Dial`-time warning when `InsecureSkipVerify=true` is detected on an `amqps://` connection; state in §6.1 that warren relies on Go's default min TLS (1.2+) and never overrides the caller's config. Non-breaking. A unit test asserts the warning fires. **(ST-11, P2)**
- **T138** `connection.go` + `internal/reconnect` + `SPEC.md §6.1/§6.8` (cites T61/T63/T66/T79): specify reconnect on a **permanent** auth failure (ST-09, Med; owner decision D5 = stop + surface + degrade) — the supervisor must not loop on `403 ACCESS_REFUSED` indefinitely (auth-backend log-spam DoS + a silent stall masquerading as a transient outage); on a permanent auth/authorization failure during re-dial or redeclare it surfaces `ErrAccessRefused`, stops or applies bounded backoff (confirm against TG-4's observed behaviour), and fires the degraded signal (`WithOnTopologyDegraded`-style). §6.1/§6.8 amend; coordinates with the reconnect-supervisor tasks T61/T63/T66 (Lens-05) and the channel-vs-connection reply-code annotation T79 (Lens-02/12) — distinct findings, cited not re-filed. A chaos/integration test revokes creds, forces a reconnect, and asserts the supervisor surfaces `ErrAccessRefused` + bounded backoff/stop + the degraded signal, not an unbounded 403 loop. **(ST-09, P1; dep TG-4)**
- **T139** `SPEC.md §6.5/§8`: state the decompression-bomb boundary (ST-07, Med — doc; rides T131) — the library never decompresses (`ContentEncoding` is metadata-only; no `compress/*` import). State in §6.5/§8 that decompression is the **caller's** responsibility, recommend a bounded (`io.LimitReader`-wrapped) decompressor, and note that the T131 inbound cap applies to the **compressed** wire body (pre-inflation) — so the cap alone does not bound the inflated size; the caller must bound that too. **(ST-07, P2)** — *rides T131.*
- **T140** `internal/amqperror` + `message.go` + `SPEC.md §7` (coord T98): extend fuzzing to the attacker-influenced broker-header parsers not yet covered (ST-13, Low) — add `FuzzAMQPCode` (the `internal/amqperror` reply-code translation over a malformed `*amqp091.Error`) and a field-table encoder fuzz/round-trip (the `message.go` typing path); both parse attacker-influenced input and are currently un-fuzzed. §7 amend. Coordinates with T98 (Lens-03, extends `FuzzCodecJSON` for `int64`) — same fuzz surface area, different targets. Confirms `FuzzXDeathParser` + `FuzzCodecCloudEventsBinary` already cover their surfaces. **(ST-13, P3; coord T98)**
- **T141** `SPEC.md §6.9`: document the accepted trace-context spoofing risk (ST-05, Low — risk-accepted-and-stated) — caller/upstream headers win last-wins over warren's injected `traceparent`/`tracestate` (§6.9 L2033–2042), so a hostile producer can forge or oversize them (trace poisoning). State it in the threat model: **accepted** under producer-trust; trace context MUST NOT be used for security/authorization decisions. Note the encryption-at-rest / message-level-payload-encryption boundary here too (application concern, explicitly out of scope for a transport wrapper). **Lens-11 (DP-07):** security of processing (GDPR Art. 32 / LGPD Art. 46) — frame the at-rest boundary as the **operator's Art. 32 "appropriate technical measures"** responsibility (bodies are **plaintext** on broker disk + backups; RabbitMQ does not encrypt at rest); disk/volume encryption is the operator's, message-level payload encryption (and, with it, crypto-erasure) is an application concern (→ LATER-47) — so silence stops implying the bus encrypts. **(ST-05, P3)**
- **T142** `SPEC.md §9` + tests (capstone): close the §9 security-success-criteria gap (ST-12, Low, high-leverage) — today §9 has only credential-grep + EXTERNAL + `UserID` (L2543–2548). Add criteria/tests for: the inbound size cap (ST-06/T131), the PLAIN-cleartext warning (ST-01/T132), never-log-payloads (ST-03/ST-14/T134), e2e wrapped-error redaction (ST-02/T133), the `InsecureSkipVerify` warning (ST-11/T137), and the new fuzz targets (ST-13/T140). Depends on the WS-1..WS-5 controls landing. **(ST-12, P2)** — lands last; asserts the controls T131–T141 added.

**Extended in place (cross-lens, not re-filed — a security/availability finding already owned by a prior-lens task, gaining a `Lens-07 (ST-08)` acceptance bullet):** T65 (ST-08 the unbounded auto-DLQ as an attacker-reachable resource-exhaustion vector — a producer's poison flood fills the auto-declared `<source>.dlq`, fills disk, and trips broker-wide `connection.blocked`, a cluster-wide publish outage caused by one service; T65 already bounds the DLQ by default for Lens-05 SRE-03 — the Lens-07 bullet adds the threat-model framing and a test that the bound holds under an *adversarial* poison flood, not just an accidental one).

**Coordination (distinct findings touching adjacent code — cited, not re-filed):** T98 (Lens-03; T140/ST-13 adds `FuzzAMQPCode` + a field-table fuzz alongside T98's `FuzzCodecJSON` int64 work — same fuzz surface, different targets); T61/T63/T66 (Lens-05 reconnect supervisor) + T79 (Lens-02/12 channel-vs-connection reply-code annotation) (T138/ST-09 specifies the supervisor's behaviour on a permanent 403).

**Sequencing:** T130 first → gates TG-2→T131, TG-1→T133, TG-3→T136, TG-4→T138, TG-5→T135. SPEC sub-phasing: (A) T130 (gate, records Rev 17, **no new build-tag lane**); (B) must-fix availability — T131 (the Blocker, supersedes LATER-35) + the cross-lens T65/ST-08 bullet; (C) confidentiality egress — T132 (PLAIN-cleartext warning), T133 (wrapped-error redaction guarantee + e2e), T134 (never-log-payloads + panic-value-type-only); (D) transport / identity / supply chain — T135 (codec build-tag), T136 (EXTERNAL principal doc → LATER-42), T137 (`InsecureSkipVerify` warning), T138 (reconnect on permanent auth); (E) boundaries & tests — T139 (decompression boundary, rides T131), T140 (fuzz additions, coord T98), T141 (trace-spoofing note), T142 (§9 security-criteria capstone, lands last — it asserts the controls A–E added). T134/T137/T139/T140/T141 are gate-independent (doc/test); T65/ST-08 is independent of the gates.

**Checkpoint Phase 18:**
- [ ] T130 gate results (TG-1..TG-5) captured on unit + the **existing** `integration` lane (3.13 **and** 4.x where broker-bound) into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced; first task records §10 **Rev 17**.
- [ ] Inbound DoS closed (ST-06/T131, Blocker): a consume-side `MaxInboundMessageSizeBytes` (default 16 MiB; `0` disables) rejects an oversized delivery **before** the codec with `ErrMessageTooLarge` + `Nack(requeue=false)` + a drop metric; an integration test proves the consumer RSS stays bounded and the message lands in the DLQ (when wired), not an OOM; LATER-35 promoted/superseded.
- [ ] Cleartext-auth warning (ST-01/T132): a `Dial` with `WithAuth`/PLAIN over `amqp://` warns through the `log` adapter; a unit test asserts the warning fires and that EXTERNAL-over-`amqp://` stays a fail-closed `ErrInvalidOptions`.
- [ ] Egress credential-safe end-to-end (ST-02/T133): an e2e test dials a bad host with `amqp://user:secret@…` and asserts `secret` is in no returned error, no `log` line, and no `amqp091` `Logger` output; the `amqp091` `Logger` is pinned/redacted/documented.
- [ ] Egress payload-safe (ST-03/ST-14/T134): §8 *Never* lists "log message payloads"; §6.9 says the panic-recover wraps the recovered value's **type only**; a grep/AST test + a panic-with-payload test pass (no body bytes in any log/error).
- [ ] Supply-chain surface shrunk (ST-10/T135): a build/import test proves a core (non-cloudevents) import does **not** pull `cloudevents/sdk-go/v2` (build-tag) — or the split is consciously deferred to LATER-44 with the surface documented in §2.
- [ ] EXTERNAL principal documented (ST-04/T136): §6.5/decision 35 document the CN extraction + `ssl_cert_login_from` divergence and recommend empty `UserID` under non-CN mappings; LATER-42 filed for configurable SAN/DN extraction.
- [ ] `InsecureSkipVerify` warning (ST-11/T137): a `Dial` with `InsecureSkipVerify=true` on `amqps://` warns (unit-tested); §6.1 states the Go-default min-TLS floor and verbatim-config policy.
- [ ] Reconnect on permanent auth (ST-09/T138): a chaos/integration test revokes creds, forces a reconnect, and asserts the supervisor surfaces `ErrAccessRefused` + bounded backoff/stop + the degraded signal — not an unbounded 403 loop; cites T61/T63/T66/T79.
- [ ] Residual risks stated/tested (ST-05/ST-07/ST-13): the trace-spoofing and decompression boundaries are documented; `FuzzAMQPCode` + a field-table fuzz are added and green (coord T98).
- [ ] §9 capstone (ST-12/T142): the new security success criteria are present and each has a backing test.
- [ ] Cross-lens (ST-08): T65 carries a `Lens-07 (ST-08)` bullet and a test that the default DLQ bound holds under an adversarial poison flood; not re-filed.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced (the inbound size cap, the cleartext-auth warning, the codec build-tag, the `InsecureSkipVerify` warning, the EXTERNAL CN caveat).
- [ ] SPEC §10 "Rev 17" note records the Lens-07 pass; no finding re-filed that a prior task owns (ST-08→T65); the ST-06 fix promotes/supersedes LATER-35; exactly **one** new `LATER.md` entry (LATER-42; LATER-44 only if D3 is overridden); T130–T142 contiguous, no duplicate IDs.

### Phase 19 — Go Concurrency & Runtime-Correctness Re-review (Lens 08: goroutine lifecycles, reader-fed/supervisor-critical callbacks, race/deadlock/leak-freedom, every "race-free"/"idempotent" claim a real primitive)

Goal: validate `SPEC.md` from the seat of a **Go concurrency specialist who reads
every contract as a set of goroutines, channels, mutexes, and `context.Context`
lifetimes** and asks, for each: *who starts it, who stops it, what does it block on,
and what happens on shutdown / panic / ctx-cancel?* The lens bar is binary: **every
goroutine must have a clean stop path**; **no user callback may run synchronously on a
reader-fed or supervisor-critical goroutine** such that a slow or panicking callback
stalls the connection; and **every "race-free" / "condition variable" / "non-blocking
dispatcher" / "idempotent close" claim must be a real primitive** (CAS / `sync.Once` /
channel-select), not a property asserted over a check-then-act. Closes the Lens-08
adversarial spec validation (`docs/spec-validation/08-go-concurrency-runtime.md`; this pass
*conducted* the review — every goroutine the contracts imply was traced
start→block→stop→panic/ctx-cancel, every user callback mapped to the goroutine it
actually runs on, and every claimed primitive verified against the *implemented* code —
producing findings `CR-01..CR-13`; brief
`docs/spec-validation/08-go-concurrency-runtime-plan.md`). Lens verdict: **GO-WITH-CHANGES**
— the architecture is fundamentally sound (the return/confirm demux is a single
goroutine over an *intentionally* unbuffered return channel per R10-3; every
message-data buffer is bounded — dispatch by prefetch, confirm by batch size, pool by
capacity; the confirm tracker resolves waiters via a one-shot non-blocking send with no
double-resolve and no leak; `started`/`Close` use `atomic.Bool` CAS / `sync.Once`; the
`Batch[M]` guard is correct; the barrier's AB/BA lock order is explicitly handled) — but
the lens bar exposes **one must-fix Blocker, a logical lost-update**: counter B of the
two-counter `MaxRedeliveries` map (`consumer.go:767→782`) is a **non-atomic
read-modify-write** (`load` then `Store` with no lock between), so under
`Concurrency(n>1)` concurrent redeliveries of the same key undercount and a poison
message loops **past** its limit — and because `sync.Map` is memory-safe, `go test
-race` **cannot** catch the logical lost-update, so decision 12 / T20's "race-free,
verified with `-race`" is a **false guarantee** (a control that silently fails open).
Plus **one High** liveness footgun (CR-01: a user `OnReturn` runs **inline on the
unbuffered-return demux goroutine** (`publisher.go:226`), so a blocking callback stalls
`amqp091`'s per-connection reader → heartbeats stop → the broker drops the socket → every
publisher on it stalls — and the SPEC never names the invocation goroutine). The
remaining eleven findings pin underspecified primitives (the impossible-as-worded
`sync.Cond` "cancellable via ctx", the double-verdict guard atomicity, the "non-blocking
dispatcher" bound, the `100x`/`1000` stress mismatch) and turn three silent leak/crash
surfaces (a panic at the supervisor / reconnect-loop goroutine boundary, a ctx-ignoring
handler past `Close`, non-FIFO pool starvation) into stated, tested contracts. No
redesign required.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D5;
each is reversible/additive and may be overridden before execution): (1) **D1 CR-01
`OnReturn` invocation** = keep `MarkReturned` synchronous on the demux (R10-3 is
load-bearing), **add the missing panic-recover**, and **dispatch the user `OnReturn` to
a bounded (1-deep) per-publisher worker** so a blocking callback can never stall the
connection reader — documenting that `OnReturn` may then fire concurrently with / shortly
after `Publish` unblocks (a timing change from "synchronously before"); the alternative
is keep-synchronous + a loud must-not-block doc + a watchdog. (2) **D2 CR-02 counter-B
atomicity** = a **per-channel mutex held across load-increment-store** (simplest correct;
the dispatch path already has the `redeliveryCounter` per channel); the test must be
**behavioural** (N goroutines, same key, assert exact final count), not `-race`-only. (3)
**D3 CR-04 double-verdict guard** = a **single `atomic.Bool` CAS** (resolved-once; only
the winner emits) on `Delivery[M]`, consider unifying `Batch[M]` onto the same primitive.
(4) **D4 CR-07 confirm-tracker aggregate cap** = **document the per-call boundary now +
defer the aggregate window to LATER-43** (`WithMaxInFlightConfirms`); the owner may pull
the option forward if a fan-out deployment needs it. (5) **D5 CR-09 non-cooperative
handler** = **detach after the cascade-ctx deadline + increment a
`consumer_handler_leaked_total` metric + document that Go cannot force-kill a goroutine**,
and **exclude** the ctx-ignoring handler from the library's goleak guarantee (a caller
defect); do not hang the cascade on it. **No new build-tag lane** — the gates CG-1..CG-6
ride the existing unit / `-race` / `integration` lanes (3.13 **and** 4.x where
broker-bound), or run against `amqpmock`/`amqptest` (T37), mirroring Lenses 04/05/06/07.
**This lens is overwhelmingly cross-lens** — the concurrency machinery is already built
and owned, so **nine** findings **extend an existing task in place** (cross-lens rule:
shared findings extend the owning task with a `Lens-08 (CR-xx)` acceptance bullet, never
re-filed) and only **four** tasks are net-new. Exactly **one** new `LATER.md` entry
(LATER-43, gated on D4 = defer). Per-task SPEC amendment lands in the same PR (CLAUDE.md
"amend SPEC first"); a SPEC §10 "Rev 18" note records the pass. **Gate task T143 runs
first**; no SPEC edit to an affected section, and no fix, lands before its gate returns.

- **T143** verification gates CG-1–CG-6 (unit + `-race` + the **existing** `integration` lane, 3.13 **and** 4.x where broker-bound — **no new build-tag lane**; broker-free probes run against `amqpmock`/`amqptest`): capture ground truth before any wording/fix lands (gate-first, mirroring T84/T94/T101/T111/T119/T130) for **CG-1** that the counter-B lost update reproduces — N goroutines (`Concurrency(n>1)`) processing redeliveries of the **same** `(channel-instance-id, MessageID)` key on one channel produce a stored count **below** the true increment count, that `go test -race` **passes** (no memory race) while the count is wrong (logical lost-update), and quantify how far past `MaxRedeliveries` a poison message loops; **CG-2** that a blocking `OnReturn` (e.g. `time.Sleep`/lock) on a mandatory-unroutable publish stalls confirm resolution for other in-flight publishes on the same channel **and** backs up `amqp091`'s connection reader / heartbeats (broker drops the socket) — capture the timing; **CG-3** that a handler which times out and then late-`Ack`s on a different goroutine emits a **second frame** today → `PRECONDITION_FAILED` → channel close → **sibling in-flight handlers on that channel die**, then that the atomic-CAS guard makes the late call a no-op; **CG-4** a **1000**-cycle connect/disconnect + confirm-churn loop under `goleak.VerifyNone` (capture the leaked-goroutine count — no such churn test exists today — and confirm every goroutine in the §11 inventory is joined); **CG-5** that a panic in (a) the reconnect-loop `connect` fn and (b) the resubscribe hook that runs on the supervisor shows the current blast radius (process crash for (a); silent reconnect-disable for (b)) and that the recover degrades the socket instead; **CG-6** that under sustained pool exhaustion (waiters ≫ pool size) Acquire is **not** FIFO and measure whether a waiter starves. Results table (under `docs/spec-validation/`) gates T20/T144/T60/T45/T34c/T08. Records §10 **Rev 18**. **(CR gates, P0)**
- **T144** `SPEC.md §6.1/§6.2/§6.3` + `publisher.go` + `connection.go` + `metrics/` (cites T13/T34c): establish the **callback invocation-goroutine contract** (CR-01, High) — §6.2 L870 says `OnReturn` "fires synchronously before `Publish` unblocks" but **none** of the four mentions names the goroutine, and the implementation invokes the user callback **inline on the single return/confirm demux goroutine** (`publisher.go:226-228`), before `MarkReturned` (`:229`), over an **unbuffered** return channel (R10-3, `:206`) — so a blocking/slow/panicking `OnReturn` stalls the demux → stalls `amqp091`'s per-connection reader → stops heartbeats → the broker drops the socket and every publisher on it stalls. **Name the invocation goroutine for every `On*` callback** in §6.1/§6.2/§6.3 (the brief's §12 inventory: `OnReturn` on the demux; `OnReconnect`/`OnBlocked`/resubscribe on the supervisor, *inside* the open barrier; `OnTopologyDegraded` safe-dispatched), and state the **must-not-block / no-I/O contract** for the reader-fed and supervisor-critical ones. Per **D1**: keep `MarkReturned` synchronous (R10-3 load-bearing), add the missing **panic-recover** for `OnReturn` (coordinate with T34c), and **dispatch the user `OnReturn` to a bounded (1-deep) per-publisher worker** (documenting the timing change from "synchronously before" to "concurrently with / shortly after `Publish` unblocks"); the alternative (synchronous + loud doc + watchdog) is recorded. Extends **T13** (the `OnReturn` timing wording now also names the goroutine). A test (CG-2 harness) asserts a blocking `OnReturn` no longer stalls confirms on the channel. **(CR-01, P1; dep CG-2)**
- **T145** `SPEC.md §6.2` + `LATER.md` (cites `internal/confirms`): document the **confirm-tracker aggregate-memory boundary** (CR-07, Med; owner decision D4 = document + defer) — the tracker memory bound is **per-call** (`PublishBatchMaxSize`), **not** aggregate: N concurrent `PublishBatch`/`Publish` calls hold N independent windows, an unbounded growth surface under publisher fan-out (already admitted §6.2 L930), owned by the *publisher*. Document the per-call boundary in §6.2, recommend caller-side fan-out limiting, and **file LATER-43** for an optional aggregate in-flight window (`WithMaxInFlightConfirms`, default off). The one-shot per-waiter resolve (`tracker.go:211-214`) + `Wait`-deletes-own-entry + `CloseAll` are **do-not-regress**. **(CR-07, P2)** — files LATER-43.
- **T146** `SPEC.md §6.1/§7/§9` + tests (capstone): close the §7/§9 concurrency-criteria gap (CR-13, Low, high-leverage) + pin atomic close-idempotency (CR-12) — add a **goroutine-inventory appendix** to §7/§9 (every long-lived goroutine + its start owner + stop signal — the brief's §11 table) and concurrency success criteria/tests for: counter-B atomicity (CR-02/T20), the double-verdict CAS (CR-04/T60), the `OnReturn`-must-not-block contract (CR-01/T144), the supervisor/loop panic-degrade (CR-05/T34c), the non-cooperative-handler goleak **carve-out** (CR-09/T70 — a caller defect, excluded from the goleak guarantee), pool starvation (CR-08/T08), and the 1000-cycle churn (CR-10/T45). Pin §6.1 that close-idempotency is enforced **atomically** (CR-12 — code already correct: `connection.go:237-242` mutex/bool, `consumer.go` `sync.Once`/`atomic.Bool`) and add a concurrent double-`Close` `-race` test. Depends on the controls A–E landing. **(CR-12/CR-13, P2)** — lands last; asserts the controls T143/T144/T145 + the nine extensions added.

**Extended in place (cross-lens, not re-filed — each is a concurrency/runtime finding already owned by a prior-lens / Phase-2/3/11 task, gaining a `Lens-08 (CR-xx)` acceptance bullet):** T20 (CR-02 the counter-B non-atomic RMW lost-update Blocker — atomic RMW + a behavioural N-goroutine test, not `-race`-only), T07 (CR-03/CR-11 the barrier `sync.Cond` "cancellable via ctx" wording + the per-Wait watcher churn + `ForceReconnect` idempotency), T60 (CR-04 the `Delivery[M]` double-verdict guard as a single atomic CAS), T34c (CR-05 the missing panic-recover at the reconnect-loop/supervisor/resubscribe infra-goroutine boundaries + the `OnReturn` site), T18 (CR-06 the non-blocking dispatcher's sole bound is prefetch, no second queue), T08 (CR-08 pool Acquire is best-effort/non-FIFO → starvation caveat), T70 (CR-09 detach a non-cooperative handler on forced close + a leaked-handler metric), T45 (CR-10 the 1000-cycle connect/disconnect + confirm-churn goleak test + the §7→1000 reconcile), T13 (CR-01 coordination — the `OnReturn` timing wording now names the goroutine; the contract itself lives in T144).

**Coordination (distinct findings touching adjacent code — cited, not re-filed):** T13 (the `OnReturn` timing wording) + T34c (the `OnReturn` recover site) are cited by T144, which owns the cross-cutting callback-goroutine contract; T41 (coverage gate) + T45 (chaos test) are cited by T146, which owns the new concurrency success criteria.

**Sequencing:** T143 first → gates CG-1→T20, CG-2→T144, CG-3→T60, CG-4→T45, CG-5→T34c, CG-6→T08. SPEC sub-phasing: (A) T143 (gate, records Rev 18, **no new build-tag lane**); (B) the must-fix race — T20/CR-02 (the Blocker; land first); (C) the liveness contract — T144/CR-01 + T34c/CR-05; (D) pin the primitives — T07/CR-03+CR-11, T60/CR-04, T18/CR-06, T08/CR-08; (E) boundaries, tests & capstone — T145/CR-07, T70/CR-09, T45/CR-10, T146/CR-12+CR-13 (lands last — it asserts the controls A–E added). T07/T18/T70/T145 are gate-independent (doc/test).

**Checkpoint Phase 19:**
- [ ] T143 gate results (CG-1..CG-6) captured on unit + `-race` + the **existing** `integration` lane (3.13 **and** 4.x where broker-bound) / `amqpmock` into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced; first task records §10 **Rev 18**.
- [ ] Counter-B lost-update closed (CR-02/T20, Blocker): the load-increment-store is **atomic** (per-channel mutex / lock-striped map); a **behavioural** N-goroutine-same-key test asserts the final count == N and `MaxRedeliveries` is enforced exactly (no poison loops past the limit); §6.3/decision 12 say "atomic read-modify-write" and note `-race` proves memory-safety only.
- [ ] Callback liveness contract (CR-01/T144): §6.1/§6.2/§6.3 name the invocation goroutine for every `On*` callback and state the must-not-block contract for the reader-fed/supervisor-critical ones; `OnReturn` has a panic-recover; a test asserts a blocking `OnReturn` no longer stalls confirms (per D1: dispatched to a bounded worker, the timing change documented).
- [ ] Double-verdict atomicity (CR-04/T60): the `Delivery[M]` resolved-once guard is a single atomic CAS; a race test (timeout vs handler-`Ack`) asserts exactly one frame and the late call a no-op (no `PRECONDITION_FAILED`, no channel-close cascade).
- [ ] Barrier & ForceReconnect (CR-03/CR-11/T07): §6.1 describes the real ctx-cancellable mechanism (no "condition variable cancellable via ctx" contradiction), the per-Wait watcher churn is bounded, and `ForceReconnect` is documented idempotent/coalesced; a test asserts a ctx-cancel during the barrier returns `ErrReconnecting` with no goroutine leak.
- [ ] Panic-safety (CR-05/T34c): a chaos test asserts a panic in the reconnect `connect` fn or the resubscribe hook degrades the socket (fires `WithOnTopologyDegraded` + a metric), not a process crash or a silent reconnect-disable.
- [ ] Dispatcher & pool (CR-06/T18, CR-08/T08): §6.3 states prefetch is the sole dispatch bound (no second queue) and a test asserts the dispatch buffer == prefetch + `basic.cancel` observed with all slots busy; §6.2 documents Acquire is best-effort with a starvation caveat.
- [ ] Boundaries & leaks (CR-07/T145, CR-09/T70): §6.2 documents the per-call (not aggregate) tracker bound (+ LATER-43); a forced close detaches a non-cooperative handler, increments the leaked-handler metric, and does not hang the cascade; §7/§9 carve the ctx-ignoring handler out of the goleak guarantee.
- [ ] Stress reconciled (CR-10/T45): §7 and §9 agree on **1000** cycles; a 1000-cycle connect/disconnect + confirm-churn `goleak.VerifyNone` test is green.
- [ ] Capstone (CR-12/CR-13/T146): §6.1 pins atomic close-idempotency (+ a double-`Close` `-race` test); §7/§9 carry a goroutine-inventory appendix and the new concurrency success criteria, each with a backing test.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + integration lane (3.13 **and** 4.x) green; `goleak.VerifyNone` clean.
- [ ] README synced if the external contract changed (the `OnReturn` callback invocation contract; a `WithMaxInFlightConfirms` option only if D4 ships it).
- [ ] SPEC §10 "Rev 18" note records the Lens-08 pass; nine findings extend their owning task in place (T07/T08/T13/T18/T20/T34c/T45/T60/T70) and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-43); T143–T146 contiguous, no duplicate IDs.

### Phase 20 — Performance & Capacity Re-review (Lens 09: every throughput number reduced to its Little's-Law model + back-solved RTT, re-projected at 1/5/10 ms; every per-message hot-path allocation traced against the implemented code; every "single pass"/"efficient" claim read for real complexity)

Goal: validate `SPEC.md` from the seat of a **performance engineer who benchmarks for a
living and distrusts every throughput number that does not carry a stated RTT, hardware,
broker config, queue type, payload size, and confirm mode.** The lens bar is binary:
**every throughput number is a finding unless it states its conditions** (reduce it to its
model `in-flight = throughput × latency`, back-solve the implied RTT, re-project at
realistic remote RTTs); and **every avoidable allocation or lock on the per-message hot
path is debt paid billions of times a day** (enumerate each, mark it necessary or
avoidable). Closes the Lens-09 adversarial spec validation
(`docs/spec-validation/09-performance-capacity.md`; this pass *conducted* the review — every §9
number reduced to its Little's-Law model and re-projected at 1/5/10 ms RTT, every
publish/consume hot-path allocation traced end-to-end against the *implemented* code, the
`amqp091-go` v1.11.0 single-writer/single-reader premise verified against upstream, and the
confirm-tracker `multiple=true` resolution read for its real O(n) complexity — producing
findings `PC-01..PC-15`; brief `docs/spec-validation/09-performance-capacity-plan.md`). Lens
verdict: **GO-WITH-CHANGES** — the performance architecture is sound where it counts (the
`amqp091-go` per-connection serialization premise is real: a `sendM` mutex serialises all
writes + a single `reader` goroutine demuxes all reads, so the multi-TCP role-split fan-out
of §1/§6.1 is the *correct* answer to the single-socket ceiling; Prometheus uses
`WithLabelValues`, not per-message `Labels{}` maps; trace injection zero-allocates on the
no-span path; decode runs on the bounded dispatch goroutines, not the per-channel reader;
the NoOp tracer/metrics bodies are genuinely empty) — and, unusually, **its own headline
was already caught by a prior lens**: the sync-confirm capacity-honesty finding (the
`pool/RTT` ceiling, the local-only 30k/100k numbers, the confirm-latency →
`ErrChannelPoolExhausted` cascade) is **already owned and being remediated by T83 (RMQ-11)
and T116 (SRE-14)**, and the histogram capacity-tail by **T113 (SRE-11)**, so Lens-09 does
**not** re-file it — it confirms their scope (do-not-regress) and contributes the concrete
artifacts they lack (the explicit rate-@1/5/10 ms model table; the pool-sizing-for-rate
formula). **There is no Blocker.** What the lens exposes as net-new is **performance debt
the prior lenses did not touch**: a cluster of avoidable **per-message hot-path
allocations** at the billions/day bar — a `time.Timer` allocated on *every* `Publish`
confirm-wait (default `ConfirmTimeout=30s`; PC-06), a span-name concat + attrs slice +
`url.Parse` built on every publish *and* every delivery even under the NoOp tracer (PC-07),
an x-death `map` allocated on *every* delivery before the header-absence check (PC-08), and
an un-pooled UUIDv7 entropy draw + a process-global `timeMu` lock per publish (PC-09) —
plus one **algorithmic** finding (the confirm tracker resolves `multiple=true` in
O(outstanding) per frame under the tracker mutex, not the O(resolved) its "single pass"
wording implies; PC-11) and the **§9-criteria / benchmark-methodology gaps** (no payload
size or queue type on the numbers; no consume-side throughput target and no latency SLO at
all; the `5×` batch ratio pegged to the wrong baseline). No redesign required; all are
local hot-path fixes, an ordered tracker index, and spec/criteria sharpening.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D5;
each is reversible/additive and may be overridden before execution): (1) **D1
allocation-hardening depth** = land the **cheap, zero-risk** wins now (timer pool/reset in
T11; span-arg gating in T148; `uuid.EnableRandPool()` in T10; lazy x-death map in T17;
Prometheus child caching in T148) and **defer the deeper wins to LATER-45** (a pooled-buffer
codec `Encode` + a UUIDv7 generator without the google/uuid process-global `timeMu` lock —
both larger, with codec-API / dependency implications). (2) **D2 PC-07 vs decision 3** = gate
only the *argument construction* (the attrs slice, the span-name concat, the
`peerAddress`/`url.Parse`) behind a precomputed `tracingActive bool` (set once:
`_, isNoOp := tracer.(NoOpTracer)`), keeping a **single** `Start`/`Record` call site with
**no** `if tracer != nil` behavioral branch — so decision 3's intent (uniform path, no
tracing-disabled branch) holds while the wasted allocation is removed. (3) **D3 PC-11
mechanism** = a contiguous confirmed low-water-mark + an ordered index (or a min-heap keyed
by delivery-tag) → O(resolved + log n); the alternative (keep O(outstanding) + merely amend
§6.2 to drop the implied efficiency) is disfavoured at the billions/day bar. (4) **D4 PC-03
benchmark queue type** = require the bench to state **both** a classic-queue and a
quorum-queue number (quorum's majority Raft commit raises confirm latency materially, so a
single "100k" without the queue type is uninterpretable). (5) **D5 PC-10 §9 consume
criteria** = add a consume-side throughput target (e.g. `BenchmarkConsume ≥ 30k msg/s` at
`Concurrency(8)+Prefetch(256)` — already benched by T44b but not encoded in §9) **and** a
publish/handle latency SLO (p99/p999 against the §6.9 histogram), since "billions/day" with
no consume target and no latency SLO is an incomplete success bar. **No new build-tag lane**
— the allocation gates ride the unit / `-benchmem` / `AllocsPerRun` lanes; the
RTT/throughput/queue-type gates ride the existing `integration` lane + the T44b release-tag
bench cadence; the injected-RTT gate coordinates with T116's SG-4, mirroring Lenses 04–08.
**This lens is heavily cross-lens** — the capacity machinery is already built and owned, so
**nine** findings **extend an existing task in place** (cross-lens rule: shared findings
extend the owning task with a `Lens-09 (PC-xx)` acceptance bullet, never re-filed) and only
**three** tasks are net-new. Exactly **one** new `LATER.md` entry (LATER-45, gated on D1 =
defer the deep wins; the Phase-18 conditional LATER-44 codec-split reservation stands).
Per-task SPEC amendment lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10
"Rev 19" note records the pass. **Gate task T147 runs first**; no SPEC edit to an affected
section, and no hot-path fix, lands before its gate returns.

- **T147** verification gates PG-1–PG-6 (unit + `-benchmem`/`testing.AllocsPerRun` + the **existing** `integration`/bench lanes — **no new build-tag lane**): capture ground truth before any wording/fix lands (gate-first, mirroring T84/T94/T101/T111/T119/T130/T143) for **PG-1** the **publish** hot-path allocs/op for `Publish` at the NoOp tracer + default config, attributing each alloc (the confirm-`Wait` `time.Timer`, the span name concat + attrs slice + `peerAddress`/`url.Parse`, the `waiter`+`done` chan, the UUIDv7 string, the JSON body, the release closure) → gates T11/T148/T10; **PG-2** the **consume** hot-path allocs/op per delivery at the NoOp tracer + **no x-death header** (the common path), confirming the x-death `map` alloc lands on the no-DLX delivery, plus the span-arg slice and the `context.WithCancelCause` → gates T17/T148; **PG-3** a microbench of `resolveUpTo` cost vs in-flight depth D (16/256/1024) for a `multiple=true` frame resolving **one** tag, showing the per-frame cost grows **O(D)** (whole-map scan + sort) while holding `t.mu` → gates T11; **PG-4** the **single-socket** publish-confirm ceiling (1 conn, sweep `WithChannelPoolSize`) to **source** the "~50k msg/s per socket" figure with a real number and locate the `sendM`-writer-serialization knee → gates T44b/T07d; **PG-5** an injected-RTT publish-confirm bench at RTT ∈ {~0, 1, 5, 10} ms (default pool, then `4×16`) producing the §11 model-table numbers and the `ErrChannelPoolExhausted` onset under a confirm-latency spike (**coordinates with / extends T116's SG-4**) → gates T83/T116; **PG-6** a full-conditions bench running `BenchmarkPublishConfirmed`/`PublishBatch` recording RTT + **payload size** + broker version + **queue type**, demonstrating the **classic-vs-quorum** confirm-latency delta on the same target → gates T44b/T149. Results table (under `docs/spec-validation/`). Records §10 **Rev 19**. **(PC gates, P0)**
- **T148** `publisher.go` + `consumer.go` + `connection.go` + `otel/` + `metrics/prometheus.go` + `SPEC.md §6.9/decision 3`: **hot-path allocation hardening** (PC-07 Med + PC-15 Low) — the two findings with no single owning task, plus the combined regression guard. **PC-07:** the span argument-construction (the `exchange+" publish"` / consume name concat, the `[]attribute.KeyValue` attrs slice, and `peerAddress()`'s `url.Parse`) is built **unconditionally** on every publish (`publisher.go:371/423`, `connection.go:842`) and every delivery (`consumer.go:571/716`) even under the NoOp tracer (the args are evaluated before the no-op `Start` discards them); gate the arg-construction behind a precomputed `tracingActive` flag on both paths, **preserving decision 3** — one `Start`/`Record` call site, **no** `if tracer != nil` behavioral branch (per **D2**: set once via `_, isNoOp := tracer.(NoOpTracer)`); document the reconciliation in §6.9/decision 3; the no-span zero-alloc trace-injection fast path (`publisher.go:444`) is **do-not-regress**. **PC-15:** resolve the Prometheus child `Observer`/`Counter` for the fixed-outcome label sets **once at build** rather than per-message (`prometheus.go:125/130/235` do a `WithLabelValues` hash + `RWMutex` lookup per message); do **not** reintroduce a `prometheus.Labels{}` map (do-not-regress). **Owns the combined `AllocsPerRun` hot-path guard** asserting PC-06/07/08/09/15 collectively at the NoOp tracer + default config, so a future regression on any of them fails one test. **(PC-07/PC-15, P1; dep PG-1/PG-2)**
- **T149** `SPEC.md §9/§6.1/§6.3` + tests (capstone): the **capacity & performance capstone** (PC-05 + PC-10 + the §9 portions of PC-14) — add a **§9 performance-model appendix** referencing the explicit rate-@1/5/10 ms RTT-collapse table that T83 inlines beside the numbers; add the **missing** §9 **consume-side throughput target** (per **D5**: e.g. `BenchmarkConsume ≥ 30k msg/s` at `Concurrency(8)+Prefetch(256)` — already benched by T44b but never encoded in §9) **and** a **publish/handle latency SLO** (p99/p999 against the §6.9 histogram — §9 today has neither); reframe the batch-target wording as the RTT-decoupled **scale path** (PC-05, paired with T44b's absolute reframe); and fix the §6.1 "one goroutine drives the socket" write-mechanism wording → writes are **`sendM`-mutex-serialised**, reads are a **single `reader` goroutine** (the *conclusion* "serializes I/O per connection" is correct; the write *mechanism* is imprecise; PC-14). Asserts the WS-2/WS-3/WS-4 controls (T11/T17/T10/T148/T44b) landed. **(PC-05/PC-10/PC-14, P2)** — lands last; asserts the controls B–D added.

**Extended in place (cross-lens, not re-filed — each is a performance/capacity finding already owned by a prior-lens / Phase-2/3/9/14/16/17 task, gaining a `Lens-09 (PC-xx)` acceptance bullet):** T83 (PC-01 bake the explicit RTT-collapse model table — §11 — into §9 *beside* the 30k/100k numbers as the "remote projection", not parked ~680 lines away in LATER-34), T116 (PC-02 add the quantified pool-sizing-for-rate formula `pool ≥ target_rate × confirm_RTT` per conn so the `ErrChannelPoolExhausted` cascade onset is computable; confirm SG-4 covers it), T44b (PC-03/04/05/14 the bench reports+pins payload size + queue type and states both a classic and a quorum number; reframes the `5×` ratio to an RTT-stated absolute with batch as the scale path; documents the release-tag-only regression cadence; sources the ~50k/socket figure), T11 (PC-06 pool/reset the per-`Wait` `time.Timer` + PC-11 the `multiple=true` low-water-mark + ordered index → O(resolved + log n)), T17 (PC-08 lazy-allocate the x-death `byReason` map after the early returns → zero map alloc on the no-DLX delivery), T10 (PC-09 `uuid.EnableRandPool()` + document the process-global `timeMu` lock), T18 (PC-10 the consume single-channel scaling note — one consumer = one channel = one reader on one TCP, so beyond the per-TCP ceiling scale needs more channels/connections), T22 (PC-13 the `PublishBatchMaxSize=1024` memory/throughput trade-off). **Confirmed (do-not-regress, no re-file):** T113 (PC-12 verify the SRE-11 extended buckets `10s/30s/60s` span the confirm-RTT capacity tail; add a §6.9 capacity-tail rationale line).

**Coordination (distinct findings touching adjacent code — cited, not re-filed):** the §9 *criteria* changes (PC-05 batch-as-scale-path wording, the new consume-side throughput target and the missing latency SLO PC-10, the §6.1 write-mechanism wording PC-14) are cross-cutting §9/§6 work → owned by the capstone **T149**, which cites T44b (bench) and T18 (consume sizing); the implementation of PC-06/PC-08/PC-09 lands in its owning task (T11/T17/T10) but their **combined `AllocsPerRun` hot-path guard** is asserted by **T148** (so a future regression on any of them fails one test).

**Sequencing:** T147 first → gates PG-1→T11(PC-06)/T10/T148, PG-2→T17/T148, PG-3→T11(PC-11), PG-4→T44b(PC-14), PG-5→T83/T116, PG-6→T44b(PC-03/05). SPEC sub-phasing: (A) T147 (gate, records Rev 19, **no new build-tag lane**); (B) hot-path allocations — T11/PC-06, T17/PC-08, T10/PC-09, T148/PC-07+PC-15 (+ the combined `AllocsPerRun` guard); (C) confirm complexity — T11/PC-11 (low-water-mark + ordered index; §6.2 wording); (D) capacity model & benchmark methodology — T83/PC-01, T116/PC-02, T44b/PC-03+04+05+14; (E) pin & capstone — T18/PC-10, T22/PC-13, T113/PC-12, T149 (lands last — it asserts the controls B–D + the §11 table). T18/T22/T113 + PC-04 are gate-independent (doc).

**Checkpoint Phase 20:**
- [ ] T147 gate results (PG-1..PG-6) captured on unit + `-benchmem`/`AllocsPerRun` + the **existing** `integration`/bench lanes into a committed results table; each downstream task cites its gate; **no new build-tag lane** introduced; first task records §10 **Rev 19**.
- [ ] Capacity model inline (PC-01/T83, PC-02/T116): §9 carries the explicit rate-@1/5/10 ms RTT-collapse table beside the 30k/100k numbers (the "remote projection"); §6.2/§9 carry the pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT`) and the `ErrChannelPoolExhausted` onset; the async-publish API stays LATER-34, decision 31 stays closed; the headline is **not** re-filed.
- [ ] Publish hot path (PC-06/T11, PC-09/T10, PC-07/T148): an `AllocsPerRun` test asserts `Publish` at the NoOp tracer + default config no longer allocates the confirm-`Wait` `time.Timer`, the span name/attrs/`url.Parse`, or (via `EnableRandPool`) the per-call entropy buffer; the `timeMu` global-lock cost is documented; the no-span trace-injection fast path is unchanged.
- [ ] Consume hot path (PC-08/T17, PC-07/T148): an `AllocsPerRun` test asserts a no-x-death, NoOp-tracer delivery allocates **no** x-death `byReason` map and **no** span name/attrs slice; decode still runs off the per-channel reader goroutine.
- [ ] Confirm complexity (PC-11/T11): a microbench shows `multiple=true` resolution is O(resolved + log n), not O(outstanding); §6.2 states the real complexity; the one-shot resolve / `Wait`-self-delete / `CloseAll` mechanism is unchanged.
- [ ] Benchmark methodology (PC-03/04/05/14/T44b): the bench reports + pins RTT + payload size + broker version + **queue type**, with both a classic and a quorum number; the `PublishBatch` target is an RTT-stated absolute with batch documented as the scale path; the release-tag-only regression cadence is documented; the ~50k/socket figure is replaced with a measured single-socket ceiling + the `sendM` knee.
- [ ] Pin & capstone (PC-10/T18+T149, PC-13/T22, PC-12/T113, §9/T149): §6.3 states consume scaling needs more channels/connections beyond the per-TCP ceiling; §9 gains a consume-side throughput target **and** a p99/p999 latency SLO; §6.2 documents the `PublishBatchMaxSize` trade-off; §6.9's extended buckets (T113) are confirmed to span the confirm-RTT tail; §6.1's write mechanism is described accurately (`sendM`-serialised + single reader).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` + the `-benchmem`/`AllocsPerRun` guards green; integration on 3.13 **and** 4.x + the T44b bench cadence green.
- [ ] README synced if the external contract changed (none expected — the fixes are internal hot-path + spec wording; `EnableRandPool`, the timer pool, the lazy map, the ordered tracker index, the `tracingActive` gate, and the Prometheus caching are all internal; no new public option is added in this phase).
- [ ] SPEC §10 "Rev 19" note records the Lens-09 pass; nine findings extend their owning task in place (T10/T11/T17/T18/T22/T44b/T83/T116) + T113 confirmed, and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-45; the Phase-18 conditional LATER-44 reservation stands); T147–T149 contiguous, no duplicate IDs.

### Phase 21 — Test-Strategy & Verifiability Re-review (Lens 10: every §9 success criterion classified falsifiable? / tested-at-what-level? / right-reason? / CI-gateable?; every load-bearing §6 contract matched to the weakest test level that can actually prove it; every "nightly"/"enforced in CI"/"suite passes" claim checked against the actual CI)

Goal: validate `SPEC.md` from the seat of a **test architect who treats §9 "Success Criteria" as a test plan and asks of every line: is this falsifiable, is it actually tested, and can the test fail for the right reason?** The lens bar is binary: **a success criterion that cannot fail — or cannot run in CI — is a finding, not a pass** (a number measured on the author's laptop is not a CI gate; a criterion behind a lane that does not exist is not verified; a floor the workflow never enforces is vanity); and **each contract must be matched to the weakest test level that can actually prove it** (a stub "proving" a real-broker behaviour is a finding; a Go-only harness "proving" cross-language fidelity is a finding; a probabilistic-race contract like R10-3 needs a load+repeat real-broker test, not a mock that can pass while the invariant is broken). Closes the Lens-10 adversarial spec validation (`docs/spec-validation/10-test-strategy-verifiability.md`; this pass *conducted* the review — every one of the 34 §9 checkboxes classified `falsifiable? / test-level / right-reason? / CI-gateable?`, every load-bearing §6 contract scored stub-vs-real-broker, the actual `ci.yml`/`test/docker-compose.integration.yml`/integration-helper reality read against what §7/§9/§3 *claim* — producing findings `TV-01..TV-15`; brief `docs/spec-validation/10-test-strategy-verifiability-plan.md`). Lens verdict: **GO-WITH-CHANGES** — **no Blocker.** The library's *behaviour* is well tested (631 unit tests, 6 fuzz targets on every attacker-influenced byte-parser, 212 `goleak.VerifyNone` assertions, `-race` on every lane) and the highest-risk contracts already have **owned** tasks with **named tests** — so, like Lens-09, **this lens is heavily cross-lens and its headline is already owned**: every "untested contract" the prompt anticipated is already someone's task (R10-3 return/ack ordering → T59 + Lens-01 RMQ-16; polyglot CloudEvents/field-table/`time.Time`→`T` fidelity → T94 + T37, Lens-03 IW-13; the Rev-10 failure modes R10-6/8/12 → T61/T63/T67, *pulled into v0.1 with named tests* including T63's half-alive-broker proxy test; the version-divergent quorum limit → T81; the security credential grep → T45b; the concurrency-interleavings coverage% gap → T143/T146). So Lens-10 does **not** re-file them — it **confirms** them (do-not-regress) and **contributes the verification-infrastructure layer they all silently depend on**. The gap this lens exposes is therefore **not in the code** — it is in the **verification infrastructure and the honesty of the criteria**: a CI that runs only `unit` + `integration` (single `rabbitmq:3-management` image, Go 1.23, `go test -race -cover` with no fail-under, **no `schedule:` trigger**) leaves roughly a quarter of §9 **structurally unrun** (every "nightly" criterion — fuzz 10m-budget, 1000-cycle stress, chaos 5-min @ 10k × 50 runs — has no runner; the conformance lane and throughput bench do not exist in CI; the 4.x and Go-1.24 rows never run) and lets three reliability-relevant criteria **pass while broken** (the chaos "zero message loss" with no specified loss-counting method; the unenforced coverage floor; the security grep that only catches *exercised* output). What the lens delivers as net-new is exactly that **infrastructure + criteria-honesty layer no prior lens owns**: a **scheduled/nightly workflow** (TV-01), a **RabbitMQ 3.13 + 4.x integration matrix** (TV-05), an **integration-broker-required guard** (TV-13), the **conformance stub-vs-real-broker contract matrix** as a §7 artifact with the stub language dropped for v0.1 (TV-06), and a **§7/§9/§6.2.1 verifiability rewrite** that classifies every criterion by *where it actually runs* (TV-10/12/15 capstone). No message-loss Blocker: the highest-severity finding (TV-09, chaos loss-counting) is a harness-honesty gap on an *already-owned* test (T45).

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D5; each is reversible/additive and may be overridden before execution): (1) **D1 throughput-criteria disposition (TV-04)** = keep the absolute `30k/100k/5×` numbers as **release-tag targets on stated reference hardware** *and* add a **CI-gradeable relative regression gate** (fail if > X% slower than the last release baseline on the same runner class) as the checked §9 criterion — author-laptop numbers can never gate CI (HW/RTT/neighbour contention); coordinate with Lens-09 T44b/T149. (2) **D2 nightly/scheduled workflow (TV-01/13)** = add a `schedule:`-triggered workflow running the fuzz 10m-budget + the 1000-cycle stress + the chaos 5-min @ 10k 50-run flaky harness with a flaky-rate report, plus the integration-broker-required guard — it is the only honest way to claim "nightly". (3) **D3 RabbitMQ version matrix (TV-05/11)** = run **both** `rabbitmq:3.13` LTS and `rabbitmq:4` on the integration lane (≈ 2× lane time; put the heavier on the nightly trigger if PR-lane time is a concern) — single-version un-tests either the LTS most estates run or the 4.x quorum behaviour the spec asserts. (4) **D4 coverage-floor enforcement (TV-03)** = a **hard fail-under** at 80%/95% + the coverage artifact + the PR comment §7 already promises (today the number is discarded). (5) **D5 conformance harness (TV-06)** = **real-broker-only for v0.1** (matches T44's op-decision) and **remove the "test AMQP server stub" language from §7** — a stub cannot prove the broker-bound contracts in the §12 matrix; defer any stub to v0.2. **New lanes are in scope here** (unlike Lenses 04–09): the `conformance` lane already exists in §7's Levels table (L2220) and §3 (L142-143) but **not** in CI — wiring it is remediation, not invention; the **scheduled/nightly** workflow is a new *trigger*, the **version matrix** a new *axis* on the existing `integration` lane. Cost is a real constraint — the planner placed the expensive criteria (chaos 50-run, fuzz budget, 4.x×3.13) on the **nightly** trigger / **release-tag** cadence, not every PR, and §9 must *say so* per criterion. **This lens is heavily cross-lens** — nine findings **extend an existing task in place** (cross-lens rule: shared findings extend the owning task with a `Lens-10 (TV-xx)` acceptance bullet, never re-filed) and only **three** tasks are net-new; three prior groups are **confirmed** (do-not-regress) and add no task. Exactly **one** new `LATER.md` entry (LATER-46). Per-task SPEC amendment lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 20" note records the pass. **Gate task T150 runs first**; no §7/§9/§6.2.1 wording change and no workflow edit lands before its gate returns.

- **T150** verification gates VG-1–VG-6 (unit + the **existing** `integration` lane + a throwaway `rabbitmq:4` broker for VG-5; **no behaviour change**): capture ground truth before any wording/workflow edit lands (gate-first, mirroring T84/T94/T101/T111/T119/T130/T143/T147) for **VG-1** the "green by not running" baseline — enumerate every §9 checkbox and mark which *execute* in the current CI (`unit` + `integration`, Go 1.23, `rabbitmq:3-management`) vs which are **structurally unrun** (the nightly criteria, the conformance suite, the throughput bench, the 4.x-only contracts, the Go-1.24 row, the delayed-exchange/TLS/SASL-EXTERNAL criteria), filling the brief §11 "CI-gateable?" column with *measured* facts → gates TV-01/04/05/06/07/08; **VG-2** the unenforced coverage floor — show `go test -race -cover ./...` produces a number but **no step fails** below 80%/95% (no `-coverprofile` threshold, no artifact, no PR comment), computing current per-package coverage and confirming a hypothetical sub-floor drop still passes → gates TV-03; **VG-3** the green-by-skipping integration lane — run `go test -race -tags=integration ./...` with `AMQP_TEST_URL` **unset**, count the `t.Skip`ped tests, and confirm the lane exits **0** having asserted nothing → gates TV-13; **VG-4** R10-3 mock-vs-real reproduction — measure whether the current mock-tracker test can *ever* fail when the demux is intentionally broken (split across goroutines / buffered notify chan), and characterize how many real-broker iterations under concurrent unroutable-mandatory load trip the race ≥1× → gates TV-02 (T59); **VG-5** version-divergent contracts on 4.x vs 3.13 — stand up both a `rabbitmq:3.13` and a `rabbitmq:4` broker and assert (a) the quorum default `x-delivery-limit` (R10-2, default 20) drops the 21st delivery on 4.x and (b) the classic-queue `x-delivery-limit`-ignore behaviour, recording any divergence → gates TV-05 (T81); **VG-6** chaos loss-counting can detect an injected drop — verify "zero message loss" is computed as **published-set − consumed-set deduped by `MessageID`** (tolerating at-least-once duplicates) by **injecting a single deliberate drop** and confirming the harness reports loss == 1 → gates TV-09 (T45). Results table (under `docs/spec-validation/`). Records §10 **Rev 20**. **(TV gates, P0)**
- **T151** `.github/workflows/` + `test/docker-compose.integration.yml` + `Makefile`: **CI verification infrastructure** (TV-01 High + TV-05 High + TV-13 Med — the net-new spine; nothing the unrun criteria promise runs without it). **TV-01:** add a `schedule:`-triggered workflow (nightly) running the **fuzz 10m-budget per target**, the **1000-cycle connect/disconnect stress** (T45/CR-10), and the **chaos 5-min @ 10k × 50-run** flaky harness (T45), reporting the **flaky-rate** (< 1%) — every §9/§7/§3 "nightly" criterion currently has **no runner** (`ci.yml` triggers only on `push`/`pull_request`). **TV-05:** add a **RabbitMQ 3.13 + 4.x matrix** axis to the `integration` lane (and the corresponding pinned images in `test/docker-compose.integration.yml`) so version-divergent contracts (R10-2 quorum default `x-delivery-limit`, classic `x-delivery-limit`-ignore, Khepri `queue.declare`) are **verified on the version where they bind** (today a single `rabbitmq:3-management`); the matrix is what *verifies* T81's version-divergence docs. **TV-13:** add an **integration-broker-required guard** that **fails** the required integration job if **zero** integration tests actually ran (the lane `t.Skip`s green when `AMQP_TEST_URL` is unset). Coordinates with T45/T45b (the jobs it schedules), T41/T42 (the coverage/Go-matrix lanes), T37/T44 (broker images incl. the delayed-exchange plugin). **(TV-01/TV-05/TV-13, P1; dep VG-1/VG-3/VG-5)** **(D2, D3)** **Lens-13 (LT-02/03/04/05):** campaign cadence: the soak/endurance (T167) + spike/stress-to-failure/single-node-composite (T168) campaigns ride this scheduled/nightly workflow; the **multi-node cluster load harness (T166) is a distinct on-demand/nightly lane** this task's infrastructure wires; state honestly which campaigns gate **release tags** vs nightly (the heavy campaigns gate release tags, not per-PR — D5).
- **T152** `SPEC.md §9/§7/§6.2.1` (capstone, lands last): the **verifiability honesty capstone** (TV-06 + TV-10 + TV-12 + TV-15) — classify **every** §9 success criterion as `CI-gate | nightly | release-only | operator-validated | polyglot-lane` so the spec never implies a check that does not exist; add the §7 **conformance stub-vs-real-broker contract matrix** (brief §12) and **drop the "test AMQP server stub" language** (real-broker-only v0.1, TV-06/D5); reconcile the residual **§7 prose** the per-task fixes do not catch (§3/§7's "spins up RabbitMQ via testcontainers-go" / `amqptest.NewRabbitMQ(t)` claims vs the real docker-compose + `AMQP_TEST_URL` harness until T37 lands, TV-10); add the §7 Coverage sentence that the line-coverage floor is **necessary-not-sufficient** for `internal/confirms`/`internal/reconnect` (the bugs are in **interleavings**, not lines) and **cross-link the §9 concurrency criteria** (T143/T146, TV-12); label the §6.2.1 "15 minutes of MessageID retention is sufficient" recommendation an **operator-validated recommendation, not a library-tested guarantee** (no test can exercise an hours-later redelivery, TV-15); scope the §9 cross-language criteria to gate on the T94 polyglot lane. Asserts the WS-1/WS-2 lanes (T150/T151 + T41/T42/T44) landed. **(TV-06/TV-10/TV-12/TV-15, P2)** — lands last; asserts the controls B–D added.

**Extended in place (cross-lens, not re-filed — each is a test/verifiability finding already owned by a prior-lens / Phase-9/11/12 task, gaining a `Lens-10 (TV-xx)` acceptance bullet):** T41 (TV-03 hard coverage fail-under + artifact + PR comment — the floor §9/§7 promise is unenforced), T42 (TV-03/07/13 implement what the workflow scope names — Go 1.24 in the matrix, the coverage fail-under wiring, the integration-broker-required guard), T44 (TV-06/08/11 the stub-vs-real-broker §7 matrix + drop the stub language + the delayed-exchange present-plugin requirement + the real-4.x quorum poison-loop **bound** assertion), T44b (TV-04 the CI-gradeable relative regression gate, absolutes reclassified as release-tag targets on reference HW), T45 (TV-09 the chaos loss-counting method + the VG-6 injected-drop self-test + TV-10 the §7-text 60s@1k→5-min@10k / 100x→1000 reconciliation), T45b (TV-14 force a wrapped-`amqp091-go` error path + scan errors/spans/labels, not just logs), T59 (TV-02 the real-broker load+repeat R10-3 assertion + a §7 flaky-prone-by-design note), T81 (TV-05/11 the version-divergence claims verified by the 3.13/4.x matrix; the quorum-limit + poison-loop bound asserted on 4.x), T37 (TV-08 guarantee the delayed-message-exchange plugin in ≥1 required lane). **Confirmed (do-not-regress, no re-file):** T94 + T37 (the polyglot `interop` lane already owns cross-language fidelity — every §9 cross-language criterion gates on it; Lens-10 adds no interop task), T61/T63/T67 (the Rev-10 failure modes R10-6/8/12 are already tested in v0.1 with named tests — the prompt's anticipated "deferred, untested" gap is **resolved**; the only residual is ensuring T63's half-alive-broker proxy harness runs in a real lane), T143/T146 (the concurrency interleaving / goroutine-inventory criteria already own what coverage% cannot prove — TV-12 only cross-links them from §7).

**Coordination (distinct findings touching adjacent code — cited, not re-filed):** the §9 *criteria classification* (TV-10/12/15 + the throughput reclass TV-04) is cross-cutting §9/§7/§6.2.1 work → owned by the capstone **T152**, which cites T44b (the throughput gate it classifies), T45 (the §7 chaos-text reconciliation it completes), and T143/T146 (the concurrency criteria it cross-links); the **nightly jobs** T151 schedules are *implemented* in their owning tasks (T45 chaos, T45b security, the fuzz targets) — T151 only adds the **trigger**, the **matrix axis**, and the **skip-guard**; the throughput **relative regression gate** (TV-04) lands in T44b but is **classified** by T152, coordinated with Lens-09 T149.

**Sequencing:** T150 first → gates VG-1→TV-01/04/05/06/07/08, VG-2→TV-03, VG-3→TV-13, VG-4→TV-02(T59), VG-5→TV-05(T81), VG-6→TV-09(T45). SPEC/CI sub-phasing: (A) gates — T150 (records Rev 20; fills brief §11 with measured CI-reality; **no behaviour change**); (B) infrastructure — T151 (nightly trigger + 3.13/4.x matrix + integration-broker-required guard) so the unrun criteria gain a runner, plus T42/T41 (conformance lane wiring + Go 1.24 + coverage fail-under/artifact/PR-comment); (C) make-checkable — T44b (relative regression gate, TV-04), T45 (loss-counting + VG-6 self-test, TV-09), T45b (wrapped-error breadth, TV-14); (D) version-bound & real-broker — T81/T44 (the 4.x matrix verifies the quorum limit + poison-loop bound, TV-05/11), T44 (stub-vs-real matrix + drop stub language, TV-06), T37/T44 (delayed-plugin guarantee, TV-08), T59 (load+repeat R10-3, TV-02); (E) honesty capstone — T152 (§7/§9/§6.2.1 classification + reconcile), lands last; asserts B–D. TV-10/TV-12/TV-15 are gate-independent (doc/wording, fold into T45/T152).

**Checkpoint Phase 21:**
- [ ] T150 gate results (VG-1..VG-6) captured on unit + the **existing** `integration` lane (+ a throwaway 4.x broker for VG-5) into a committed results table; the brief §11 "CI-gateable?" column is filled from *measured* CI reality; each downstream task cites its gate; first task records §10 **Rev 20**; **no behaviour change** in this task.
- [ ] Nightly runner (TV-01/T151): a `schedule:`-triggered workflow runs the fuzz 10m-budget, the 1000-cycle stress, and the chaos 5-min @ 10k × 50-run harness and reports the flaky-rate (< 1%); §9 names which criteria run there.
- [ ] Version matrix (TV-05/11/T151+T81+T44): the `integration` lane runs **both** `rabbitmq:3.13` and `rabbitmq:4`; the quorum default `x-delivery-limit` drop and the poison-loop **bound** (`DeliveryLimit + 1`, not `==`) are asserted on 4.x; T81's version-divergence docs are verified by it.
- [ ] Coverage enforced (TV-03/07/13/T41+T42): CI **fails** below 80% per package / 95% on the critical-path packages, uploads the profile as an artifact, and posts the PR comment §7 promises; Go 1.24 is in the `-race` matrix (TV-07); the required integration job fails if zero integration tests ran (TV-13).
- [ ] Conformance honesty (TV-06/08/T44+T37): the §7 stub-vs-real-broker matrix exists; the "test AMQP server stub" language is dropped for v0.1; the delayed-exchange criteria run against a plugin-enabled image in a required lane or are marked conditionally-verified and **fail, not skip**, when the plugin is expected but missing.
- [ ] Make-checkable numbers (TV-04/09/14): §9 carries a CI-gradeable relative regression gate with the absolutes reclassified as release-tag targets (T44b, coord Lens-09 T149); §7 + T45 define chaos loss as published − consumed (deduped by `MessageID`, tolerating duplicates) and the harness reports loss == 1 for one injected drop (VG-6); the 60s security run forces a wrapped-`amqp091-go` error path and the grep scans logs, errors, spans, and metric labels (T45b).
- [ ] R10-3 honesty (TV-02/T59): the real-broker return/ack ordering test runs many iterations under concurrent unroutable-mandatory load; §7 notes the contract is flaky-prone by design and belongs on the nightly trigger; the mock-tracker test stays as the fast-lane lock.
- [ ] §7/§9/§6.2.1 reconciled (TV-10/12/15/T45+T152): §7 prose matches §9 (5-min @ 10k, 1000 cycles, the docker-compose/`AMQP_TEST_URL`/`amqptest` reality); §7 states the coverage floor is necessary-not-sufficient and cross-links the §9 concurrency criteria (T143/T146); §6.2.1 labels the dedupe window operator-validated; **every** §9 criterion is classified (`CI-gate | nightly | release-only | operator-validated | polyglot-lane`).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new nightly / 3.13+4.x-matrix / conformance lanes green on their cadence.
- [ ] README synced if the external contract changed (expected: the `make` target list if a `make ci-nightly`/matrix target is added, and the CI lane/badge description; the §9 numbers + latency classification are SPEC-side — checked per task).
- [ ] SPEC §10 "Rev 20" note records the Lens-10 pass; nine findings extend their owning task in place (T37/T41/T42/T44/T44b/T45/T45b/T59/T81) + three groups confirmed (T94, T61/T63/T67, T143/T146) and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-46); T150–T152 contiguous, no duplicate IDs.

### Phase 22 — Data-Protection Compliance Re-review (Lens 11: every `Message[M]` field traced to where it persists; every default rated for privacy-by-default; every finding mapped to a GDPR *and* LGPD article; "can the library delete one message / surface residency?" answered against the implemented code = no)

Goal: validate `SPEC.md` from the seat of a **privacy-engineering specialist who reads a message bus as a processor / operador surface and asks of every default: does this make the downstream controller's compliance harder or impossible, and is the mitigating pattern recommended as a privacy control — not just an optimisation?** The lens bar is binary: **a default that opts the user into a non-compliant posture is a finding** (durable-by-default writes personal data to broker disk and, on dead-letter, to an auto-`<source>.dlq` with no `x-message-ttl`/`x-max-length`, retaining it indefinitely — GDPR 5(1)(e) storage limitation / LGPD Art. 16); **a mitigating pattern that exists but is mis-positioned is a finding** (pointer-out — publish a reference, keep PII in an erasable store of record — is recommended only for payload *size*, never as the canonical erasure / breach-minimisation control); and **silence on a hazard only the library can warn about is a finding** (the spec never states that bodies on a durable queue are un-erasable at the bus layer, that RabbitMQ does not encrypt at rest, that quorum replication + `WithAddrs` failover can cross jurisdictions, or that a natural-key `MessageID`/`CorrelationID` turns the dedupe cache, `x-death`, logs, and the OTel ID attributes — which flow to a third-party APM — into stores of personal data). Closes the Lens-11 adversarial spec validation (`docs/spec-validation/11-compliance-gdpr-lgpd.md`; this pass *conducted* the review — every `Message[M]` field traced to where it persists, every default rated for privacy-by-default impact, every finding mapped to a concrete GDPR **and** LGPD article, the "is there an API path to delete one message / surface residency?" questions answered against the implemented code = **no** — producing findings `DP-01..DP-15`; brief `docs/spec-validation/11-compliance-gdpr-lgpd-plan.md`). Lens verdict: **GO-WITH-CHANGES** — **no Blocker.** The library never *violates* the law (it is a processor/tool, not a controller) and it already does the hard confidentiality work — URI credentials are redacted at a choke-point (T07c/T133), default OTel attributes carry **size not content**, default metric labels are bounded + PII-free, the codec ships a strict opt-in *for compliance workloads*, and the demanding security-of-processing controls a privacy reviewer would demand (never-log-payloads, the PLAIN-cleartext warning, the at-rest-encryption boundary) are **already owned** by the security lens (T134/T132/T141). So, like Lenses 09–10, **this lens is heavily cross-lens and its confidentiality half is already owned** — Lens-11 does **not** re-file it; it **extends** each owning task in place with the data-protection framing (cross-lens rule: a shared finding extends the owning task with a `Lens-11 (DP-xx)` acceptance bullet, never re-filed). The gap this lens exposes is therefore **not a bug — it is a positioning + silence gap**: the **privacy-by-design layer above confidentiality** (erasure / storage limitation, data minimisation, international transfer, records of processing, accountability) has **no owner**. What the lens delivers as net-new is exactly that layer: the **erasure-structural-silence** warning (DP-01), **pointer-out promoted from a size tip to the canonical privacy / erasure / breach-minimisation pattern with a runnable example** (DP-02 — the headline), the **data-minimisation footguns** (DP-05/06/10/11/12: a natural-key `MessageID`/`CorrelationID`, `UserID`/`StampUserID`, `connection_name`, opt-in metric labels, the trace-backend-as-a-processor), the **international-transfer / data-residency caveat** (DP-09), and the **records-of-processing + controller-vs-processor boundary** capstone (DP-13/14/15). No message-loss Blocker: the one *default change* worth making (a bounded **and TTL'd** auto-DLQ for storage limitation) is already owned by **T65**; everything else is documentation, a positioned control, and one runnable example — valid mitigations for a processor library.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D5; each is reversible/additive and may be overridden before execution): (1) **D1 pointer-out strength + home (DP-02/DP-13)** = promote pointer-out to a **first-class §8 *Always*/recommended pattern for personal data** (kept a *should*, not a hard default — the library cannot know which bodies are personal data) **plus** a runnable `examples/pointer_out/` and a §9 success criterion; a prose-only note is the weaker alternative the prompt explicitly rejects. (2) **D2 DLQ default (DP-03)** = T65 already bounds the auto-`<source>.dlq` by *length* for SRE-03/ST-08; **add a default or strongly-recommended `x-message-ttl`** so dead-lettered personal data has a finite life by default (the *stronger* control per lens Rule 3) — exact value the T65 owner's call, with a conservative default + a prominent godoc opt-out. (3) **D3 crypto-erasure / message-level payload encryption (DP-01, overlaps T141)** = **LATER-47** — a codec wrapper that encrypts the body with a per-data-subject key (delete the key on an erasure request → the ciphertext on the bus/DLQ/disk becomes unrecoverable = effective erasure without a delete primitive) is the elegant answer to DP-01, but it is large + key-management-heavy and T141 already states message-level encryption is out of scope for v0.1. (4) **D4 residency (DP-09)** = **documented caveat only** for v0.1 — a transport wrapper cannot enforce node/region placement (that is broker/infra config); a typed residency/region surface is a possible v0.2 idea, **not** a task. (5) **D5 scope (lens Q4)** = the library provides **enabling patterns (pointer-out), defaults (bounded+TTL DLQ), and caveats (un-erasable, at-rest, residency, minimisation footguns)** and **explicitly disclaims** the controller's duties (lawful basis, DPIA, breach timing, records of processing); the capstone (T156) draws the **controller-vs-processor boundary** so the spec never implies warren makes a system compliant. **No new build-tag lane** (unlike Lens-10): the runnable `examples/pointer_out/` rides the existing `examples-build`/`examples-smoke` targets and the DLQ-TTL default is asserted on the existing `integration` lane — mirrors Lenses 04–09. **This lens is heavily cross-lens** — five findings **extend an existing task in place** (T65/T85/T132/T134/T141, each gaining a `Lens-11 (DP-xx)` acceptance bullet, never re-filed) and only **four** tasks are net-new (T153–T156); four groups are **confirmed** (do-not-regress) and add no task (T07c, T133, bounded metric labels, OTel size-not-content + the header-non-mutation contract). Exactly **one** new `LATER.md` entry (LATER-47). Per-task SPEC amendment lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 21" note records the pass. **Gate task T153 runs first**; no §1/§6.x/§8/§9/§10 wording change and no example lands before its gate returns.

- **T153** data-protection gates DG-1–DG-5 (data-inventory ledger + defaults audit + no-PII-in-observability baseline + DLQ-retention reality + erasure/residency reality; **no behaviour change**): capture ground truth before any §-wording change (gate-first, mirroring T84/T94/T101/T111/T119/T130/T143/T147/T150) for **DG-1** the data-inventory audit — enumerate every field/metadata the library writes or propagates (`Message[M]` fields, broker-appended `x-death`, the auto-injected `traceparent`/`tracestate`, `connection_name`, OTel attributes, metric labels), classify each `NP`/`PP`/`ID→PII` and record **where each persists** (wire, broker disk, DLQ copy, dedupe cache, logs, traces/APM, metric store, `client_properties`), filling the brief §11 ledger from the *implemented* code → gates DP-05/06/10/11/12; **DG-2** the defaults audit (privacy-by-default, Art. 25 / LGPD Art. 6) — rate each default (`DeliveryMode=Persistent`, the auto-`<source>.dlq` with no TTL/bounds, the absent never-log-PII boundary, the absent at-rest note, `MessageID=UUIDv7`, bounded metric labels, OTel size-not-content, `connection_name=<binary>-<hostname>-<pid>`, PLAIN-over-`amqp://`, codec lax-by-default), filling the brief §12 audit → gates DP-03/04/07/08; **DG-3** the observability no-PII baseline — confirm in the `otel`+`metrics` code that by default **only** `messaging.message.body.size`, the ID attributes, and infra addresses are emitted and **no body content or header value** ever becomes a span attribute or default metric label, establishing the do-not-regress invariant DP-04/DP-10 build on → gates DP-04/10; **DG-4** the DLQ-retention reality — confirm in `topology.go` the auto-`<source>.dlq` is declared with **no `x-message-ttl` and no `x-max-length`** by default and that `DeadLetter.TTL`/`MaxLength`/`MaxLengthBytes` bind the **source** queue (§6.6 L1642), not the DLQ, anchoring T65's compliance bullet → gates DP-03; **DG-5** the erasure & residency reality — confirm there is **no API path** to delete a single in-flight message (AMQP 0-9-1 has no such primitive; only queue-purge/queue-delete, all-or-nothing) and that quorum replication + `WithAddrs` failover + the delayed-plugin node-local store (R10-1) are residency-opaque (no region/jurisdiction anywhere in the public API), establishing DP-01/09 are structural + documentation-only → gates DP-01/09. Results table (under `docs/spec-validation/`). Records §10 **Rev 21**. **(DP gates, P0)**
- **T154** `SPEC.md §6.1/§6.5/§6.6/§8` + `examples/pointer_out/`: **erasure & storage-limitation — pointer-out as the canonical privacy control + runnable example** (DP-01 High + DP-02 High + DP-03 positioning + DP-13 §8 boundary — the headline, net-new). State the **un-erasable-on-the-bus** limitation (DP-01, GDPR Art. 17 / LGPD Art. 18.VI+16 — personal data in a body on a durable queue/DLQ cannot be erased; no single-message delete) in §8 and §6.5, routing the reader to pointer-out; **promote pointer-out** from a size-only tip (§6.1 L724-726, §10 L2952) to a first-class **privacy / erasure / breach-minimisation** pattern (DP-02, GDPR Art. 25/17/33-34 / LGPD Art. 6+46+48) with a **runnable `examples/pointer_out/`** (PII in an erasable store of record, opaque reference on the wire, round-trips end-to-end on `examples-smoke`); position `Expiration`/`x-message-ttl`/`DeadLetter.TTL` (§6.5/§6.6) as the **retention control for personal data** (DP-03 positioning, GDPR 5(1)(e) / LGPD Art. 16); add the §8 *Always*/recommended boundary "personal data should not flow as message bodies — use a reference" (DP-13, GDPR Art. 25). Coordinates with **T65** (the auto-DLQ default TTL+bound) and **T85** (the dedupe-cache retention window). **(DP-01/02/03/13, P1; dep DG-5)** **(D1, D2)**
- **T155** `SPEC.md §6.5/§6.9/§8`: **data-minimisation footguns — IDs, principal, hostname, labels, trace-as-processor** (DP-05/06/10/11/12, net-new). Warn **never to put personal data in `MessageID`/`CorrelationID`** (DP-05, GDPR 5(1)(c)/25 / LGPD Art. 6.III — a natural-key value flows to the dedupe cache, `x-death`, logs, and the OTel `messaging.message.id`/`conversation_id` attributes → APM); document the **`UserID`/`StampUserID` privacy implication** (DP-06 — embeds an identifiable principal into every message + DLQ copy + broker logs; leave it empty unless the broker-side identity stamp is required); add a **minimisation note** at `WithConnectionName`/`WithClientProperties` (DP-11 — `<binary>-<hostname>-<pid>` leaks hostname broker-side + into the `ClientMetrics{connection_name}` label) and at `WithMetricsLabels(routing_key/message_type)` (DP-12 — opt-in labels can export identifiers to the metrics store); note the **trace-backend-as-a-processor** implication (DP-10, GDPR Art. 28+Ch. V / LGPD Art. 39+33 — the APM needs its own lawful basis + retention policy + transfer mechanism for whatever the user puts in IDs). Coordinates with **T85** (the dedupe-cache-becomes-a-PII-store aspect of DP-05). **(DP-05/06/10/11/12, P1; dep DG-1/DG-3)**
- **T156** `SPEC.md §6.1/§6.5/§9/§10` (capstone, lands last): **international-transfer / residency caveat + §8/§9 compliance capstone + LGPD-specific notes** (DP-09 + DP-13 §9 + DP-14 + DP-15, net-new). Document the **data-residency** caveat (DP-09, GDPR Ch. V Art. 44-49 / LGPD Art. 33 — a quorum queue replicates the body to every member node; `WithAddrs` failover + the delayed-plugin node-local store (R10-1) can cross jurisdictions; residency is invisible at the API; name the international-transfer trigger; recommend single-jurisdiction membership or pointer-out so only references cross borders); add the **§9 success criterion** that `examples/pointer_out/` exists and round-trips (DP-13); add the **records-of-processing / APM-trail** note (DP-15, GDPR Art. 30 / LGPD Art. 37 — the trace-continuity-through-DLX contract creates an APM personal-data trail needing its own retention; cross-link DP-10 + T141); add the **LGPD-specific** pointer note (DP-14 — ANPD breach timing Art. 48 vs GDPR's 72h, lawful-basis set Art. 7 vs GDPR Art. 6, both the controller's responsibility); draw the explicit **controller-vs-processor boundary** (D5) so the spec offers the enabling patterns + caveats without implying warren discharges the controller's duties. Asserts the WS-1..WS-3 controls (T154/T155 + the T65/T85/T132/T134/T141 extensions) landed. **(DP-09/13/14/15, P2; dep DG-5, T154)** — lands last; asserts B–C. **(D4, D5)**

**Extended in place (cross-lens, not re-filed — each is a data-protection finding whose *confidentiality* mechanism is already owned by a prior-lens task, gaining a `Lens-11 (DP-xx)` acceptance bullet):** T65 (DP-03 — the auto-`<source>.dlq` bound gains a **default/strongly-recommended `x-message-ttl`** so dead-lettered personal data is not retained indefinitely; storage-limitation framing, GDPR 5(1)(e) / LGPD Art. 16), T85 (DP-05 — a natural-key `MessageID` turns the dedupe cache into a **store of personal data**; the recommended TTL/residency bound is **storage limitation**, GDPR 5(1)(c) / LGPD Art. 6.III), T132 (DP-08 — the PLAIN-over-`amqp://` warning names **personal data in transit** as the Art. 32 / LGPD Art. 46 exposure), T134 (DP-04 — the never-log-bodies/headers §8 *Never* boundary is framed as a **data-protection** control, GDPR 5(1)(f)/32 / LGPD Art. 46), T141 (DP-07 — the at-rest boundary is framed as the **operator's Art. 32 "appropriate technical measures"** responsibility, GDPR Art. 32 / LGPD Art. 46). **Confirmed (do-not-regress, no re-file):** T07c (URI credential redaction at the choke-point), T133 (wrapped-`amqp091`-error redaction), the bounded PII-free default metric labels (`routing_key`/`message_type` opt-in), the default OTel size-not-content attributes, and the **header-non-mutation contract** (§6.6 L1702-1706 — *why* erasure is documentation-only; the library must never silently strip/rewrite/normalise a user's bodies/headers/IDs).

**Coordination (distinct findings touching adjacent code — cited, not re-filed):** T154 (pointer-out + example) cites **T65** (the auto-DLQ default TTL+bound it positions as the retention control) and **T85** (the dedupe-cache retention window); the §9 success criterion enforcing the pointer-out example is owned by the capstone **T156**, which cites T154's runnable example; DP-05 is split — the dedupe-cache-becomes-a-PII-store aspect is owned by **T85** while the `MessageID`/`CorrelationID` minimisation note is owned by **T155**; the crypto-erasure answer to DP-01 (and DP-07's at-rest) is deferred to **LATER-47** (prereq T154).

**Sequencing:** T153 first → gates DG-1→DP-05/06/10/11/12 (T155), DG-2→DP-03/04/07/08 (T65/T134/T141/T132), DG-3→DP-04/10 (do-not-regress baseline, T155), DG-4→DP-03 (T65), DG-5→DP-01/09 (T154/T156). SPEC sub-phasing: (A) gates — T153 (records Rev 21; fills the brief §11 ledger + §12 audit from measured code; **no behaviour change**); (B) erasure & storage limitation — T154 (pointer-out + `examples/pointer_out/` + un-erasable warning + TTL positioning) + the **T65** default-TTL extension + the **T85** retention-window extension; (C) minimisation & confidentiality — T155 (ID/principal/hostname/label/trace footguns) + the **T134** (never-log-PII), **T132** (in-transit), **T141** (at-rest) extensions; (D) transfer & capstone — T156 (residency caveat + §9 criterion + records-of-processing + LGPD-specific + controller/processor boundary), lands last; asserts B–C. Gate-independent (pure positioning/doc): DP-02, DP-13, DP-14.

**Checkpoint Phase 22:**
- [ ] T153 gate results (DG-1..DG-5) captured into a committed results table; the brief §11 data-inventory ledger + §12 defaults audit are filled from *measured* code; first task records §10 **Rev 21**; **no behaviour change** in this task.
- [ ] Erasure honesty (DP-01/T154): §8 + §6.5 state that personal data in a message body is effectively **un-erasable** at the bus layer (no single-message delete; durable + DLQ copies) and route the reader to pointer-out.
- [ ] Pointer-out as a privacy control (DP-02/DP-13/T154+T156): pointer-out is positioned in §6.5/§8 as the canonical **erasure + breach-minimisation** pattern (not only size); a runnable `examples/pointer_out/` ships, builds on `examples-build`, and round-trips on `examples-smoke`; a §9 criterion enforces it.
- [ ] Storage limitation (DP-03/T65+T154): §6.5/§6.6 position `Expiration`/`x-message-ttl`/`DeadLetter.TTL` as the **retention control for personal data**; the auto-`<source>.dlq` gains a default (or strongly-recommended) **TTL** in addition to its length bound, asserted on the `integration` lane (T65).
- [ ] Never-log-PII (DP-04/T134): the §8 *Never* list carries "never log message bodies or header values" framed as a data-protection control; the grep/AST test confirms no non-test path formats a body/header value into a log/error string.
- [ ] Minimisation footguns (DP-05/06/11/12/T155+T85): §6.5 warns never to put personal data in `MessageID`/`CorrelationID`; the `UserID`/`StampUserID` privacy implication is documented; `WithConnectionName`/`WithClientProperties` and `WithMetricsLabels` carry minimisation notes; the dedupe-cache-as-PII-store + retention window is framed in T85.
- [ ] Confidentiality (DP-07/08/T141+T132): the at-rest boundary (T141) is framed as the Art. 32 operator responsibility (bodies plaintext on disk + backups); the PLAIN-over-`amqp://` warning (T132) names personal-data-in-transit as the Art. 32 exposure.
- [ ] Transfer/residency (DP-09/T156): §6.1/§6.5/§10 carry a data-residency caveat (quorum replication + `WithAddrs` failover + delayed node-local can cross jurisdictions; residency is invisible at the API; mitigations named).
- [ ] Trace-as-processor & records (DP-10/DP-15/T155+T156): §6.9 notes the APM is a processor receiving IDs + `traceparent` (needing its own lawful basis/retention/transfer) and the dual nature of the trace-continuity contract for Art. 30 / LGPD Art. 37 records.
- [ ] LGPD-specific + scope (DP-14/D5/T156): the capstone states breach timing (ANPD Art. 48 vs GDPR 72h) and lawful basis (LGPD Art. 7 vs GDPR Art. 6) are the **controller's**, and draws the explicit controller-vs-processor boundary so warren never implies it discharges the controller's duties; every finding maps to a GDPR *and* LGPD article.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; `make examples-build` green (incl. `examples/pointer_out/`); the example smoke-runs on the `integration` lane.
- [ ] README synced if the external contract changed (expected: the examples table gains `examples/pointer_out/`; a "privacy / data-protection guarantees" note if one is added — checked per task; no new public option in this phase).
- [ ] SPEC §10 "Rev 21" note records the Lens-11 pass; five findings extend their owning task in place (T65/T85/T132/T134/T141) + four groups confirmed (T07c, T133, bounded labels, OTel size-not-content + header-non-mutation) and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-47); T153–T156 contiguous, no duplicate IDs; tally **165 tasks / 22 phases**.

### Phase 23 — Developer-Experience & Documentation Re-review (Lens 12: can a competent Go developer go from `go get` to a working, *correct* publish/consume in the first hour without reading the source? — every load-bearing warning checked for call-site-godoc presence; the conceptual overview, the most-copied snippets, and the runnable-example gaps audited against the developer's first hour)

Goal: validate `SPEC.md` from the seat of a **developer-experience / technical-documentation specialist who has shipped popular open-source Go libraries and judges the library by its first hour — because for a reliability library the documentation *is* the product: a guarantee the user does not understand is a guarantee they will violate.** The lens bar is binary: **a load-bearing warning that lives only in `SPEC.md` is a finding** (the user hovers a symbol in their IDE or reads pkg.go.dev — they do not open `SPEC.md` mid-keystroke; the at-least-once/must-dedupe contract §6.2.1, the `UserID` 406 footgun §6.5, `ConfirmTimeout(0)`-discouraged §6.2, and `PrefetchBytes()`-no-op §6.3 are all SPEC prose with **no mandate to appear verbatim in the godoc at the call site** — only `AutoAck()` §6.3 L1351 carries that mandate, so the placement policy is ad-hoc); **a teaching gap the only-reference-complete plan won't close is a finding** (§4 L181 lists `doc.go` as the package overview but no SPEC section defines what it must cover, and the shipped `doc.go` is a 12-line feature list ending "See SPEC.md" — for a library this opinionated, a weak conceptual overview is guaranteed misuse); and **a broken or unmeasurable promise is a finding** (§1 L94-97 promises publish in <10 lines / consume in <15, yet the README quickstart handler `func(ctx, o *Order)` **does not compile** against the value-typed `Handler[M]` and **swallows every error with `_`**, and the §6.2.1 canonical dedupe snippet references out-of-scope `d.MessageID()` at L1049 — the first code a user copies must compile and be honest). Closes the Lens-12 adversarial spec validation (`docs/spec-validation/12-dx-documentation.md`; this pass *conducted* the review — walked dial → topology → publish → consume → handle-failure → observe end-to-end, checked every load-bearing warning for call-site-godoc presence, compiled the most-copied snippets against the real signatures, and inventoried the runnable examples against the hard paths — producing findings `DX-01..DX-17`; brief `docs/spec-validation/12-dx-documentation-plan.md`). Lens verdict: **GO-WITH-CHANGES** — **no Blocker.** The API itself is well-designed and the hardest prose is exemplary (the §6.3 poison/`AutoAck` treatment and the §6.5 `UserID`/`Delay` field notes are model documentation; the six shipped examples each open with a "What this example demonstrates" godoc header), and a determined reader *can* learn the library from `SPEC.md` — so it does not silently lose, duplicate, or leak anything because of a documentation gap, and is not NO-GO. Like Lenses 09–11, **this lens is heavily cross-lens and most of its placement surface is already owned** — the godoc sweeps (T10/T34/T40), the `AutoAck` verbatim-godoc exemplar (T35), the `Delay` durability godoc (T57), the dedupe **example** with an outcome-asserting smoke test (T38b), the quorum `DeliveryLimit==0` declare-time warning (T58), and the README quickstart (T39) — so Lens-12 does **not** re-file them; it **confirms** the exemplars and **extends** the owning tasks in place with explicit DX acceptance (cross-lens rule: a shared finding adds a `Lens-12 (DX-xx)` acceptance bullet, never re-filed). The gap this lens exposes is therefore **not a correctness bug — it is a pit-of-success gap**: the **conceptual-overview / first-hour-learning-path layer no prior lens owns**. What the lens delivers as net-new is exactly that layer: the **`doc.go` conceptual overview** + its SPEC definition (DX-01 — the mental model: role-split pool, at-least-once→dedupe, topology-separate, reconnect-barrier), the **at-least-once→dedupe godoc routing** onto `Publish`/`Consume`/`PublishRetry`/`Redelivered`/`MessageID` (DX-02 — the single most important contract, today a section users skip), the **§8 footgun→godoc routing boundary + table** that makes placement systematic (DX-03), the **copy-paste-correctness** fixes for the README + §6.2.1 snippets (DX-04/05), the **migration guide** from the 2023 library (DX-06), the **production-tuning page** (DX-07), the **error-message-names-the-remedy** policy (DX-08), the **durable-retry-ladder + operational-hard-case examples** (DX-09/10), the **example outcome-assertion gate** (DX-11), the **`<10/<15` line measurability** (DX-14), the **`amqp.go`-vs-`warren` naming** reconciliation (DX-15), and the **cross-reference integrity sweep** (DX-16). No Blocker: every finding is documentation, placement, a compile-correctness fix, or a runnable example — all valid, none a silent loss/duplicate/leak defect.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D6; each is reversible/additive and may be overridden before execution): (1) **D1 `doc.go` scope (DX-01)** = `doc.go` becomes the **canonical conceptual overview** (the mental model + a minimal correct publish/consume + pointers to the examples and the dedupe contract), and SPEC §4/§6 defines its mandatory section list so it cannot rot back to a feature stub; a standalone `docs/` site is out of scope for v0.1 (LATER if demand appears). (2) **D2 migration-guide home (DX-06)** = a top-level **`MIGRATION.md`** linked from README (not a README section — it is long and versioned), since 2023-library users are the most likely early adopters and need a mapping table + behavioural-change list. (3) **D3 production-tuning home (DX-07)** = a dedicated **"Production tuning / recommended defaults for high throughput"** section in `doc.go` (or `docs/tuning.md` linked from README) consolidating publisher/consumer connections, channel-pool size, the prefetch/concurrency formula, heartbeat, frame-max, and `ConfirmTimeout` — cross-linking the existing §6.1/§6.3 prose, not duplicating it. (4) **D4 error-remedy mechanism (DX-08)** = because Go bare-sentinel text is fixed, the remedy lives in the **godoc on each operational sentinel** (e.g. `ErrChannelPoolExhausted` godoc names `WithChannelPoolSize`/`WithPublisherConnections`) and wrap sites add the knob to the wrapped message where context allows; adopt a §6.8 policy "every operational error documents the cause **and** the remedy" — **no new sentinels minted**. (5) **D5 example ranking (DX-09/10)** = P1 = (1) **durable_retry** (TTL+DLX ladder — the footgun-avoidance path with no example today) + (2) **graceful_shutdown** (drain in-flight on SIGTERM); (3) **tls_mtls**, (4) **cluster_failover**, (5) **callbacks**, (6) **observability** tracked, landing as their dependencies (TLS/mTLS, cluster failover) ship; the full Grafana-dashboard cookbook is **LATER-48**. (6) **D6 the `<10/<15` line claim (DX-14)** = **keep but make it measurable** (define what counts — the minimal correct snippet excluding imports/signal-handling, with errors handled — and prove it with the fixed README quickstart); if the honest minimum exceeds the numbers, **soften the claim** rather than ship a broken promise. **No new build-tag lane** (the new `examples/durable_retry/` + the operational examples ride the existing `examples-build`/`examples-smoke` targets; the snippet-compile gate XG-3 rides the existing CI). **This lens is heavily cross-lens** — four findings **extend an existing task in place** (T10/T34/T39/T40, each gaining a `Lens-12 (DX-xx)` acceptance bullet, never re-filed) and only **seven** tasks are net-new (T157–T163); five groups are **confirmed** (do-not-regress) and add no task (T35, T38b, T57, T58, the example godoc headers + the redaction-in-logs contract). Exactly **one** new `LATER.md` entry (LATER-48). Per-task SPEC amendment lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 22" note records the pass. **Gate task T157 runs first**; no §1/§4/§6.x/§8/§9/§10 wording change, no `doc.go`/`MIGRATION.md`/example, and no snippet fix lands before its gates return.

- **T157** DX gates XG-1–XG-5 (footgun→godoc placement audit + example inventory/outcome audit + copy-paste-correctness audit + cross-reference integrity sweep + first-hour-journey audit; **no behaviour change**): capture ground truth before any wording/fix (gate-first, mirroring T84/T94/T101/T111/T119/T130/T143/T147/T150/T153) for **XG-1** the footgun→godoc placement audit — for every load-bearing footgun/contract record *symptom*, *where documented today* (SPEC §+line / code comment / godoc / nowhere), *whether a task already routes it to call-site godoc*, and *the gap* (minimum rows: at-least-once/must-dedupe §6.2.1, `AutoAck` §6.3/T35, `Delay` durability §6.5/T57, `UserID` 406 §6.5, `ConfirmTimeout(0)` §6.2, `PrefetchBytes()`-no-op + `ChannelQoS()` §6.3, `DeliveryMode` persistent-default §6.5, quorum `DeliveryLimit==0` §6.3/T58, `MaxRedeliveries` counter-B process-local §6.3, "always wire `OnCancel`" §6.3, `Mandatory()` set-not-toggle §6.2, `Prefetch < Concurrency` §6.3) producing the **footgun-doc-placement table** → gates DX-02/03/12/13; **XG-2** the example inventory & outcome-assertion audit — enumerate the §4 example list (eleven) vs what exists on disk (six) and, for each existing example, record whether its smoke test asserts an **outcome** (round-trip/observable effect) or only **exit-0**, listing the hard paths with no example, producing the **examples-gap list** → gates DX-09/10/11; **XG-3** the copy-paste-correctness audit — extract every code block from README + SPEC §5 + §6.2.1 (and the §6.3/§6.5 snippets) and **compile each** against the real signatures, flagging every block that does not build (catches DX-04 README `*Order`, DX-05 §6.2.1 out-of-scope `d`), and recommend a permanent anti-rot mechanism (a `go:build ignore` doctest harness or a CI step that tangles fenced Go blocks and `go build`s them) → gates DX-04/05; **XG-4** the cross-reference integrity sweep — resolve every `§x.y`/`decision N`/`RNN`/`Rev N` reference in SPEC, flag dangling/stale targets, and reconcile the `### Rev` heading list with the narrative (the `docs/spec-validation/README.md` cites Rev 9, which has no SPEC heading) producing a fix-list (no fabrication — only references proven dangling) → gates DX-16; **XG-5** the first-hour-journey audit — walk dial → topology → publish → consume → handle-failure → observe end-to-end and record, at each step, every place the user must already know something the docs do not teach at that step, producing the **first-hour journey walkthrough** (the brief §11 draft is the seed) → gates DX-01/02/07/08. Results table (under `docs/spec-validation/`). Records §10 **Rev 22**. **(DX gates, P0)**
- **T158** `doc.go` + `SPEC.md §4/§6.9`: **teach the mental model — the conceptual overview + the naming note** (DX-01 High + DX-15 Low, net-new; WS-1). Define `doc.go`'s **mandatory contents** in SPEC §4 (+ a §6.9 or §1 pointer) so it cannot rot back to a feature stub, then write it as the canonical conceptual overview: role-split pool (default 2 publisher + 2 consumer TCP connections), **at-least-once → you MUST dedupe by `MessageID`**, topology-declared-separately + `AttachTo` (reconnect redeclare), reconnect-is-a-synchronous-barrier (`Publish` blocks on `ErrReconnecting`), the degraded state, the no-silent-X bars, a **minimal correct publish/consume** snippet (compiles — gated by XG-3/T160), and pointers to the examples + §6.2.1; add the one-line **`amqp.go` directory vs `package warren` module** reconciliation note (DX-15) in `doc.go` + README. **(DX-01/15, P1; dep XG-5, XG-1)** **(D1)**
- **T159** `SPEC.md §6.2/§6.2.1/§6.3/§8` + godoc: **route footguns to the call site, systematically — the at-least-once/dedupe contract + the §8 boundary + the routing table** (DX-02 High + DX-03 High, net-new; WS-2). Add a §8 *Always* boundary — **"load-bearing warnings appear verbatim in the godoc at the call site"** — plus a **footgun→godoc routing table** in SPEC (seeded by XG-1) that makes the placement policy systematic and verifiable, citing the `AutoAck()` mandate (§6.3 L1351, T35) as the template (DX-03); route the **at-least-once → must-dedupe** contract (§6.2.1 L1013-1068) onto the godoc of `Publish`, `Consume`, `PublishRetry`, `Delivery.Redelivered`, and `Message.MessageID`, each pointing to §6.2.1 + `examples/idempotent_consume/` (DX-02); fold the `Mandatory()` set-not-toggle note into the table. The §8 boundary + table land in SPEC **before** the sweep that satisfies them (the T10/T34 extensions + the T40 final-godoc pass verify against this table). **(DX-02/03, P1; dep XG-1)**
- **T160** `README.md` + `SPEC.md §6.2.1` + CI: **make the most-copied code correct + the snippet-compile gate** (DX-04 High + DX-05 High + DX-14 Low, net-new; WS-3). Fix the README quickstart so it **compiles** against the value-typed `Handler[M]` (today's `func(ctx, o *Order)` does not build) and **handles errors** (no `_`-swallow), and rewrite the §6.2.1 canonical dedupe snippet to **build as written** (use `ConsumeRaw` for envelope access, or key the cache off the delivery — today it references out-of-scope `d.MessageID()` at L1049); wire the **snippet-compile gate XG-3** as a permanent CI mechanism (tangle fenced Go blocks + `go build`, or a `go:build ignore` doctest harness) so snippet rot cannot recur; make the `<10/<15` claim measurable (D6) by proving the fixed quickstart against a stated definition (coordinates with the T39 extension). **(DX-04/05/14, P1; dep XG-3)** — coordinates T39.
- **T161** `MIGRATION.md` (new top-level file) + README link: **migration guide from the 2023 library** (DX-06 Medium, net-new; WS-4). Add a top-level `MIGRATION.md` (linked from README, not a README section — it is long and versioned) mapping the 2023 API → warren, including the behavioural-change list: default-requeue → **default-nack-no-requeue**, single-connection → **role-split pool**, generic-options → **non-generic functional options**, builder-`Message` → **struct literal**, plus the at-least-once/dedupe contract every adopter must internalise. Gate-independent (pure doc). **(DX-06, P2)** **(D2)**
- **T162** `SPEC.md §6.1/§6.2/§6.3/§6.8` + `doc.go` + godoc: **discoverability of the right path — the production-tuning page + the error-message-names-the-remedy policy** (DX-07 Medium + DX-08 Medium, net-new; WS-4). Add a single **"Production tuning / recommended defaults for high throughput"** page (a `doc.go` section or `docs/tuning.md` linked from README) consolidating publisher/consumer connections, channel-pool size, the **prefetch/concurrency formula**, heartbeat, frame-max, and `ConfirmTimeout` — cross-linking the existing §6.1/§6.3 prose, not duplicating it (DX-07); adopt the §6.8 policy **"every operational error documents the cause AND the remedy"** and put the remedy/knob in the godoc on each operational sentinel (`ErrChannelPoolExhausted` → `WithChannelPoolSize`/`WithPublisherConnections`, `ErrConfirmTimeout` → raise `ConfirmTimeout`/connections, …), with wrap sites adding the knob where context allows — **no new sentinels** (DX-08; the model is the actionable `IsTransient`/`IsPermanent` godoc §6.8 L1947-1971). **(DX-07/08, P2; dep XG-5)** **(D3, D4)**
- **T163** `examples/durable_retry/` + ranked operational examples + `SPEC.md §7`: **runnable references for the hard paths + the example outcome-assertion gate** (DX-09 Medium + DX-10 Medium + DX-11 Medium, net-new; WS-5 — lands last). Add `examples/durable_retry/` (the **TTL + DLX retry ladder** — the durable path §6.5/R10-1 steers users toward and away from the lossy `delayed/` exchange, with no example today) (DX-09); add the ranked operational hard-case examples — P1 `graceful_shutdown` (drain in-flight on SIGTERM), then `tls_mtls` (gated on Phase-8 hardening), `cluster_failover` (`WithAddrs`), `callbacks` (`OnReturn`/`OnBlocked`/`OnCancel`/`OnTopologyDegraded` wired correctly — the §6.3 "always wire `OnCancel`" obligation), and `observability` (metrics + alert rules) (DX-10, D5); mandate in §7 that every example smoke test asserts an **outcome** (round-trip/observable effect), not exit-0 — the model is `examples/idempotent_consume/` (T38b), coordinated with the Lens-10/TV outcome-assertion gate (DX-11). Each new example rides `examples-build`/`examples-smoke` (no new lane) and opens with a "What this example demonstrates" godoc header (do-not-regress). The full Grafana-dashboard cookbook is **LATER-48**. **(DX-09/10/11, P2; dep XG-2)** — lands last. **(D5)**

**Extended in place (cross-lens, not re-filed — each is a DX placement/correctness finding whose owning task already exists, gaining a `Lens-12 (DX-xx)` acceptance bullet):** T10 (DX-12 — the mandated `Message[M]` field godoc adds the **`UserID` 406-channel-close footgun** §6.5 L1487-1513 and the **`DeliveryMode` Persistent-is-the-zero-value-default** note §6.5 L1450/L1554-1556, so the hazard is visible where the user sets the field), T34 (DX-13 — the connection-option/builder godoc adds the **`ConfirmTimeout(0)`-discouraged** §6.2 L972-978, **`PrefetchBytes()`-no-op-on-RabbitMQ** §6.3 L1145, and **`ChannelQoS()`-per-channel-semantics** §6.3 L1146 caveats, today SPEC prose / code comments only), T39 (DX-04/DX-14 — the README quickstart must compile against the value-typed `Handler[M]`, must not swallow errors, must cover the §9 four — TLS / multi-addrs / OTel / DLX — and must hit a stated minimal-line target or soften the §1 `<10/<15` claim; the snippet-compile gate XG-3/T160 is the lock), T40 (DX-17 — the final godoc pass also asserts the **footgun→godoc routing table** XG-1/T159 is fully satisfied, and `CHANGELOG.md` actually exists for pkg.go.dev users — §4 L179 / §8 L2343 mandate a file that does not exist yet). **Confirmed (do-not-regress, no re-file):** T35 (the `AutoAck()` verbatim-godoc mandate — the exemplar the footgun→godoc table references, never weakened), T57 (the `Delay`/`ExchangeDelayed` durability godoc routed to the call site), T38b (the `examples/idempotent_consume/` outcome-asserting smoke test — the model for the DX-11 gate), T58 (the quorum `DeliveryLimit==0` declare-time warning — a footgun already routed to a warning rather than godoc, recorded in the placement table as already-routed), the **example top-of-file godoc headers** (§7 L2312-2313 — every shipped example opens with "What this example demonstrates"; preserve the format on the new examples), and the **redaction-in-logs documentation** (§6.9 L2051-2054, README L226 — "credentials never emitted in clear text").

**Coordination (distinct findings touching adjacent work — cited, not re-filed):** T158's minimal publish/consume snippet and T160's README quickstart both depend on the snippet-compile gate XG-3 (T160); T159's footgun→godoc table is the artifact the T10/T34 extensions and the T40 final-godoc pass verify against; T163's outcome-assertion gate (DX-11) coordinates with the Lens-10/TV example-smoke gate (own the §7 statement here, implement the per-example assertions with the TV gate); DX-16's cross-ref fix-list (XG-4, T157) is applied by whichever task owns the referencing section (no standalone fix task); the full observability/alerting cookbook with Grafana dashboards is deferred to **LATER-48** (prereq T163's `observability` example).

**Sequencing:** T157 first → gates XG-1→DX-02/03/12/13 (T159/T10/T34), XG-2→DX-09/10/11 (T163), XG-3→DX-04/05 (T160), XG-4→DX-16 (cross-ref fixes ride the relevant task), XG-5→DX-01/02/07/08 (T158/T159/T162). SPEC sub-phasing: (A) gates — T157 (records Rev 22; produces the placement table, examples-gap list, broken-snippet list, cross-ref fix-list, first-hour walkthrough; **no behaviour change**); (B) teach + route — T158 (`doc.go` + naming note) + T159 (dedupe/footgun godoc routing + §8 boundary) + the **T10**/**T34** godoc extensions; (C) correctness — T160 (README + §6.2.1 snippet fixes + snippet-compile gate) + the **T39** quickstart extension; (D) discoverability — T161 (migration guide) + T162 (tuning page + error-remedy policy); (E) hard-path examples + capstone — T163 (durable-retry + operational examples + outcome gate), lands last, + the **T40** final-godoc-pass / CHANGELOG extension asserting the XG-1 table is fully satisfied. Gate-independent (pure doc): DX-06 (migration), DX-15 (naming).

**Checkpoint Phase 23:**
- [ ] T157 gate results (XG-1..XG-5) captured into a committed results table — the footgun-doc-placement table, the examples-gap list, the broken-snippet list, the cross-ref fix-list, and the first-hour journey walkthrough are filled from *measured* code/docs; first task records §10 **Rev 22**; **no behaviour change** in this task.
- [ ] Mental model taught (DX-01/15/T158): a reader who opens only `doc.go` (or pkg.go.dev's package page) learns at-least-once→dedupe-by-`MessageID`, topology-declared-separately + `AttachTo`, reconnect-is-a-barrier, the role-split pool, and the no-silent-X bars; SPEC §4/§6 defines `doc.go`'s mandatory contents; the `amqp.go`/`warren` naming is reconciled.
- [ ] Must-dedupe contract impossible to miss (DX-02/03/T159): the godoc on `Publish`, `Consume`, `PublishRetry`, `Delivery.Redelivered`, and `Message.MessageID` each carry the at-least-once→dedupe pointer (§6.2.1 + `examples/idempotent_consume/`); a §8 *Always* boundary + a SPEC footgun→godoc table make the routing systematic and verifiable.
- [ ] Most-copied snippets compile (DX-04/05/14/T160+T39): the README quickstart and the §6.2.1 dedupe snippet build against the real signatures (XG-3 green); the quickstart handles errors (no `_`-swallow), covers the §9 four (TLS/multi-addrs/OTel/DLX), and meets a stated line target (or the §1 claim is softened — D6).
- [ ] Call-site placement extensions (DX-12/13/T10+T34): the `Message[M]` field godoc carries the `UserID` 406 + `DeliveryMode`-default footguns (T10); the connection-option/builder godoc carries the `ConfirmTimeout(0)`/`PrefetchBytes()`/`ChannelQoS()` caveats (T34).
- [ ] Right path discoverable (DX-06/07/08/T161+T162): `MIGRATION.md` maps the 2023 API → warren (incl. default-requeue→default-nack-no-requeue, single-conn→role-split, generic-options→non-generic, builder-`Message`→struct); a single production-tuning page consolidates the knobs + the prefetch/concurrency formula; the operational sentinels' godoc names the remedy/knob (no new sentinels).
- [ ] Hard paths have runnable references (DX-09/10/11/T163): `examples/durable_retry/` (TTL+DLX) ships, builds (`examples-build`), and smoke-runs (`examples-smoke`); the ranked operational examples land or are tracked; every example smoke test asserts an **outcome**, not exit-0 (model: T38b).
- [ ] CHANGELOG exists (DX-17/T40): `CHANGELOG.md` (Keep a Changelog) exists for pkg.go.dev users; the final godoc pass asserts the XG-1 footgun→godoc routing table is fully satisfied.
- [ ] Cross-reference integrity (DX-16/T157): the XG-4 fix-list resolves dangling `§x.y`/`decision N`/`RNN`/`Rev N` references and reconciles the `### Rev` heading list with the narrative; fixes ride the referencing task.
- [ ] Confirmations hold (do-not-regress): `AutoAck` (T35), `Delay` (T57), the dedupe example (T38b), the `DeliveryLimit==0` warning (T58), the example godoc headers, and the redaction docs are unchanged or strengthened.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; `make examples-build` green (incl. `examples/durable_retry/`); the new examples smoke-run on the `integration` lane; the snippet-compile gate (XG-3) is green in CI.
- [ ] README synced: the examples table gains `examples/durable_retry/` (+ any operational examples that land); the "Available now / On the roadmap" split stays accurate; `MIGRATION.md` is linked — checked per task; **no new public option in this phase**.
- [ ] SPEC §10 "Rev 22" note records the Lens-12 pass; four findings extend their owning task in place (T10/T34/T39/T40) + five groups confirmed (T35, T38b, T57, T58, example godoc headers + redaction docs) and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-48); T157–T163 contiguous, no duplicate IDs; tally **172 tasks / 23 phases**.

### Phase 24 — Load & Reliability-Testing Re-review (Lens 13: are the spec's load-validation campaigns and harness realistic, complete, and capable of *proving* the billions/day reliability bar under real and hostile load? — every load/stress/bench/chaos artifact classified against the seven workload shapes; every §9 reliability claim mapped to the topology its proof needs; "can the single-node testcontainers harness prove a clustered claim?" answered = no)

Goal: validate `SPEC.md` from the seat of a **load- and reliability-testing specialist who designs the campaigns that decide whether a system meets its SLOs under realistic and adversarial traffic — thinking in workload shapes (baseline, sustained, peak, soak/endurance, spike, stress-to-failure, composite-incident), measuring tail latency and saturation rather than mean throughput, and insisting the test topology matches production.** The lens bar is binary: **a reliability claim validated only on a single-node broker is *unproven* for a clustered production topology** (the billions/day story is fundamentally clustered — quorum queues, leader failover under load, partition handling, multi-node reconnect rotation — and a single-node testcontainers node cannot exercise any of it; §7 L2229-2230); **a workload shape the spec never runs is a defect class that goes uncaught** (of the seven shapes only baseline and a fast-cycle reconnect stress are run — soak, spike, stress-to-failure, and composite-incident are absent, so §1's "no silent backpressure failure" L48-52 is asserted, never demonstrated); and **a microbenchmark is not a load campaign and an unmeasurable tail is not a measurement** (the chaos 10k msg/s is ~1 billion/day at *average* and never at peak, the generators are unspecified uniform-small-no-op, and the default histogram tops at 5000 ms while `ConfirmTimeout` is 30s + the R10-8 barrier cap, so the load tail clips to `+Inf`). Closes the Lens-13 adversarial spec validation (`docs/spec-validation/13-load-testing.md`; this pass *conducted* the review — classified every load/stress/bench/chaos artifact against the seven workload shapes, mapped every §9 reliability claim to the topology its proof needs, and confirmed the single-node testcontainers harness cannot prove any cluster-level claim — producing findings `LT-01..LT-12`; brief `docs/spec-validation/13-load-testing-plan.md`). Lens verdict: **GO-WITH-CHANGES** — **no Blocker.** The v0.1 *code* is not defective, the existing chaos + benchmark + nightly-runner spine is sound and honest about cadence ("nightly", "on release tag"), and the load-bearing instrumentation + test mechanics are **already owned** (T44b/PC-04 queue-type discipline, T45/TV-09 loss-counting, T71/R10-16 saturation metrics, T151 nightly runner) — so it does not silently lose, duplicate, or leak anything, and is not NO-GO. Like Lenses 09–12, **this lens is heavily cross-lens and its load-bearing substrate is already owned** — Lens-13 **confirms** the substrate (do-not-regress) and **extends** the owning tasks in place with the load-campaign framing (cross-lens rule: a shared finding adds a `Lens-13 (LT-xx)` acceptance bullet, never re-filed); it does **not** re-file. **Stays in lane:** it does **not** re-derive the throughput *model* (Lens-09, owned by T44b/T147-T149/T83/T116) and does **not** re-audit per-criterion *falsifiability* (Lens-10, owned by T150-T152/T151) — both are cited and confirmed, not re-opened. The gap this lens exposes is therefore **not a correctness bug — it is a proof gap**: the **load-campaign layer no prior lens owns**. What the lens delivers as net-new is exactly that layer: the **§9/§7 topology-honesty pass** (LT-01 — label each reliability criterion with the topology it is actually proven against; stop implying a clustered guarantee validated single-node — the cheap, mandatory half of the headline), the **multi-node cluster load harness** + cluster-dependent campaign specs (LT-01/05 — the expensive half), the **soak/endurance + sustained-throughput campaigns** (LT-02/10 — the slow-growth leak surfaces the 1000-cycle fast-churn check misses), the **spike + stress-to-failure + single-node composite campaigns** (LT-03/04/05 — which *demonstrate* §1's no-silent-backpressure bar rather than asserting it), and the **realistic generators + measurement-under-load fixes** (LT-06/07 — configurable payload/latency distributions and a histogram that does not clip the 30s tail). No Blocker: every finding is a missing campaign, an inadequate harness, an unrealistic workload, or an unmeasurable-under-load gap — none a silent loss/duplicate/leak defect.

Owner decisions (2026-05-29, recommended dispositions — adopted from the brief's D1–D6; each is reversible/additive and may be overridden before execution): (1) **D1 the headline split (LT-01)** = land the **§9 topology-honesty pass in-phase (T165, P1)** — the spec must stop implying a clustered guarantee it validates single-node, today — **and** build the **multi-node cluster harness (T166)** as the net-new campaign spine, with the *standing* environment deferred (D5/LATER-49); the bar is too central to leave silently single-node-validated. (2) **D2 soak cadence (LT-02/10)** = spec a **≥24h soak** (RSS/goroutine/fd/series trends) + a **≥1h sustained-throughput** campaign, both on T151's **on-demand/nightly** cadence (not per-PR); the 1000-cycle fast-churn check catches fast-cycle leaks, not slow-growth. (3) **D3 spike + stress (LT-03/04)** = spec a **spike** (1×→N×, assert graceful backpressure) and a **stress-to-failure** (locate the cliff, assert classifiable errors) campaign — these *demonstrate* the §1 no-silent-backpressure bar; both depend on T71's saturation metrics landing first. (4) **D4 composite-incident set (LT-05)** = the **poison-storm** (10k + X% poison) + **slow-broker** + **slow-consumer** composites in-phase (single-node-runnable, T168); the **repeated-failover-under-load** + **rolling 3.13→4.x broker-upgrade-under-load** composites in T166 (need the multi-node harness). (5) **D5 standing env & release gating (LT-12)** = spec the campaigns now + land the single-node-runnable ones in-phase, **defer the standing dedicated multi-node cluster + load-generator fleet to LATER-49** (an ops/infra investment), and state the release-gating cadence honestly (the heavy campaigns gate **release tags**, not per-PR). (6) **D6 realistic generators (LT-06)** = a **shared realistic generator in `amqptest`** (configurable payload-size + handler-latency distributions, routing-key cardinality, burstiness), reused across bench/chaos/soak/spike, with the §9/§7 headline numbers stating the distribution. **No new *required* per-PR build-tag lane** (the campaigns are on-demand/nightly/release-tag by nature — the soak/spike/stress/composite campaigns ride T151's scheduled workflow + on-demand `make` targets, and the multi-node harness T166 is a distinct on-demand/nightly lane; the existing `integration`/`chaos`/`examples-smoke` lanes are unchanged). **This lens is heavily cross-lens** — five findings **extend an existing task in place** (T44b/T45/T66/T71/T151, each gaining a `Lens-13 (LT-xx)` acceptance bullet, never re-filed) and only **six** tasks are net-new (T164–T169); five groups are **confirmed** (do-not-regress) and add no task (T45/TV-09 loss-counting, T44b/PC-04 queue-type, T63/R10-8 barrier cap, T59/R10-3 ordering load, the T65 DLQ bound). Exactly **one** new `LATER.md` entry (LATER-49). Per-task SPEC amendment lands in the same PR (CLAUDE.md "amend SPEC first"); a SPEC §10 "Rev 23" note records the pass. **Gate task T164 runs first**; no §7/§9/§6.9 wording change lands before its gates return.

- **T164** `docs/spec-validation/` (results) + read-only audit of `SPEC.md §7/§9/§6.9/§10` + the `amqptest`/`integration`/`chaos` lanes: **load gates LG-1..LG-5** (workload-shape coverage matrix + harness-fidelity map + workload-realism audit + measurement-under-load audit + load-env/cadence reality; **no behaviour change**). Capture ground truth before any §-wording change (gate-first, mirroring T84/T94/T101/T111/T119/T130/T143/T147/T150/T153/T157): **LG-1** workload-shape coverage audit — enumerate every load/stress/bench/chaos artifact the spec defines (§7 chaos L2241-2247, poison-loop L2249-2254, 100x/1000-cycle stress L2264-2269/§9 L2457; §9 reliability L2445-2469 + throughput L2471-2479), classify each against {baseline, sustained, peak, soak, spike, stress-to-failure, composite} marking *covered/partial/absent* with intensity/topology/measurement/assertion → fills brief §11 → gates LT-02/03/04/05/09/10; **LG-2** harness-fidelity audit — confirm in §7 L2229-2230 + the lanes that the harness is **single-node testcontainers** (no multi-node cluster, no partition injection, no mixed-version), map each §9 reliability criterion to `provable-single-node | needs-multi-node | needs-partition-injection | needs-mixed-version`, record the **T66 "full-cluster restart" assertion is unrunnable on the current harness** as the anchor fact → fills brief §12 → gates LT-01 + the failover/upgrade half of LT-05; **LG-3** workload-realism audit — confirm whether the spec specifies the generators' payload-size distribution (incl. near `MaxMessageSizeBytes`/multi-frame), handler-latency distribution, routing-key/queue cardinality, and arrival burstiness, or defaults to uniform-small-no-op → gates LT-06; **LG-4** measurement-under-load audit — confirm the default histogram tops at **5000 ms** (§6.9 L2073-2076) vs `ConfirmTimeout` 30s + the R10-8 barrier cap (L3098-3104) so the tail clips to `+Inf`, and the leading-indicator saturation metrics (pool-wait, `consumer_in_flight`, `consumer_redelivered_total`) are not-yet-present (deferred to R10-16/T71) → gates LT-07/08; **LG-5** load-environment & cadence reality — confirm §7 names benchmarks "On-demand and on release tag" (L2222) and stress "Nightly" (L2268) with **no defined multi-node load environment** and **no release-gating cadence** → anchors LT-12 + D5. Results table under `docs/spec-validation/`; records §10 **Rev 23**. **(LG gates, P0)**
- **T165** `SPEC.md §9/§7`: **reliability-topology honesty pass + the §7 workload-shape coverage matrix** (the cheap, mandatory half of the headline; LT-01 honesty + LT-09 + LT-12-cadence, net-new). Label each §9 reliability criterion with the topology it is **actually proven against** (`single-node | multi-node-quorum | needs-partition-injection | needs-mixed-version`) so the spec stops implying a clustered guarantee it validates single-node (LT-01 honesty half — including §1 L98-99 "connection loss is invisible" and the T66 full-cluster-restart assertion); add the **§7 workload-shape coverage matrix** (the seven shapes × run?/intensity/topology/measurement/assertion); state the chaos/sustained intensity as a **peak multiple** of the average bar, not an arbitrary 10k (LT-09); state the heavy campaigns gate **release tags**, not per-PR (LT-12 cadence half). Coordinates with the T45 (LT-09 intensity) + T44b (LT-10 pointer) extensions. **(LT-01/09/12, P1; dep LG-1, LG-2)** **(D1, D5)**
- **T166** **DECOMPOSED → T166a–T166h and pulled forward to Phase 9.5 (see "Cluster Reliability Lane").** Originally the headline's expensive half (multi-node cluster load harness + cluster-dependent campaign specs; LT-01 harness + LT-05 cluster). The 3-node quorum cluster harness (multi-node compose; partition injection — resolved to a **Toxiproxy sidecar**, owner choice over `pause_minority`-only network control; mixed-version 3.13/4.x members) and the cluster-dependent campaigns — **quorum leader failover under sustained load**, **partition-under-load**, **reconnect-storm across nodes** (R10-11/T66's full-cluster-restart assertion finally has a harness), **quorum confirm latency under Raft replication**, and the **rolling 3.13→4.x broker-upgrade-under-load** composite (LT-05 cluster half) — are now implemented as the `cluster` build-tag lane in Phase 9.5. The *standing* environment remains **LATER-49**. This entry is retained as a pointer so the LT-01/05 + LG-2 + T66 + T165 cross-references resolve to the **T166a–T166h** family. Extends T66, T151. **(LT-01/05-cluster, P2; dep LG-2)** **(D1, D4)**
- **T167** `SPEC.md §7/§9` + `amqptest/` + T151's scheduled workflow: **soak/endurance + sustained-throughput campaigns** (LT-02 + LT-10, net-new). A **soak campaign (≥24h at target load)** with RSS/goroutine/fd/Prometheus-series trend assertions over the named leak surfaces (confirm-tracker map, dedupe cache, `(channel-instance-id, MessageID)` counter map, fd/channel, series cardinality) — the 1000-cycle fast-churn check (§9 L2457) catches fast-cycle leaks, not slow steady-state growth (LT-02); a **sustained-throughput campaign (sustain the §9 target ≥1h** without latency creep / pool saturation / memory growth), distinct from the one-shot benchmark (LT-10). Extends T44b, T151. **(LT-02/10, P2; dep LG-1, T71)** **(D2)**
- **T168** `SPEC.md §7/§9`: **spike, stress-to-failure & composite-incident campaigns** (demonstrate §1 no-silent-backpressure; LT-03 + LT-04 + LT-05 single-node, net-new). A **spike campaign** (1×→N× in seconds; assert graceful backpressure — classifiable errors, bounded memory, no silent stall/OOM) (LT-03); a **stress-to-failure campaign** (drive to saturation, locate the cliff, assert every overload path surfaces a classifiable error) (LT-04); the **single-node composite campaigns** — poison-storm (10k + X% poison → two-counter+DLX holds + DLQ bound **T65** + redelivery observable **T71**), slow-consumer+fast-publisher flow control, slow-broker confirm-latency→pool-saturation cascade (R10-18/LATER-34) (LT-05 single-node half; the failover/upgrade composites live in T166). Extends T151; confirms T65. **(LT-03/04/05-single-node, P2; dep LG-1, T71)** **(D3, D4)**
- **T169** `amqptest/` + `SPEC.md §7/§6.9`: **realistic workload generators + measurement-under-load fixes** (LT-06 + LT-07, net-new). Configurable generators (payload-size distribution incl. near `MaxMessageSizeBytes`/multi-frame, handler-latency distribution, routing-key cardinality, burstiness) shared across bench/chaos/soak/spike, with the §9/§7 headline numbers stating the distribution used (LT-06 — replaces uniform-small-no-op); the load reports **override `WithLatencyBuckets`** to cover the 30s `ConfirmTimeout` + the R10-8 barrier (the default 5000 ms top bucket clips the tail) and capture **p99/p999 + saturation indicators**, not mean throughput (LT-07). Coordinates T71 for the LT-08 saturation-metric set; no new public option (uses the existing `WithLatencyBuckets`). **(LT-06/07, P2; dep LG-3, LG-4)** **(D6)**

**Extended in place (cross-lens, not re-filed — each is a load-campaign finding whose owning task already exists, gaining a `Lens-13 (LT-xx)` acceptance bullet):** T44b (LT-10/11 — the §9 `30k/100k` numbers are one-shot benchmark iterations, not the sustained campaign T167; LT-11 quorum-vs-classic is already satisfied by PC-04 → confirm), T45 (LT-09 — the chaos 10k is billions/day at *average*, never at peak; state the intensity as a peak multiple), T66 (LT-01/05 — the "no `addr[0]` stampede on a full-cluster restart" assertion is unrunnable single-node → binds to the T166 multi-node harness; the reconnect-storm is also a spike-on-recovery in T168), T71 (LT-07/08 — the saturation metrics must land before the spike/stress/soak campaigns can observe the saturation they provoke; the 5000 ms top bucket clips the load tail), T151 (LT-02/03/04/05 — the soak/spike/stress/composite campaigns ride its scheduled workflow; T166 is a distinct on-demand lane; state release-tag vs nightly honestly). **Confirmed (do-not-regress, no re-file):** T45/TV-09 (the published−consumed-deduped-by-`MessageID` loss-counting method the soak/composite campaigns reuse, tolerating at-least-once duplicates), T44b/PC-04 (the classic-and-quorum-number queue-type discipline the campaigns inherit — LT-11 satisfied), T63/R10-8 (the reconnect-barrier max-duration cap that sizes the LT-07 measurement window), T59/R10-3 (the ordering-under-load+repeat contract the realistic generator must not regress), T65 (the auto-`<source>.dlq` bound the poison-storm composite uses as the thing-under-test).

**Coordination (distinct findings touching adjacent work — cited, not re-filed):** T167 (soak + sustained) and T168 (spike + stress + single-node composites) both depend on **T71**'s saturation metrics landing first (LT-08 precondition) and ride **T151**'s scheduled/nightly cadence; T166's cluster composites consume **T66**'s full-cluster-restart assertion (which finally has a harness that can run it); T169's generators feed the bench/chaos/soak/spike campaigns and its `WithLatencyBuckets` override coordinates with **T71**'s metric set; the poison-storm composite (T168) uses **T65**'s DLQ bound as the thing-under-test; the slow-broker composite (T168) *provokes* the R10-18/LATER-34 pool-saturation cascade without deciding whether to add the async API.

**Sequencing:** T164 first → gates LG-1→LT-02/03/04/05/09/10 (T165/T167/T168), LG-2→LT-01/05-cluster (T165/T166), LG-3→LT-06 (T169), LG-4→LT-07/08 (T169/T71), LG-5→LT-12 (T165 + LATER-49). SPEC sub-phasing: (A) gates — T164 (records Rev 23; fills the brief §11 coverage matrix + §12 harness-adequacy map from *measured* spec/lane reality; **no behaviour change**); (B) honesty — T165 (the spec stops over-claiming today: per-criterion topology label + the §7 coverage matrix + peak-multiple intensity + release-tag cadence) + the **T45** peak-multiple extension + the **T44b** one-shot-vs-sustained pointer — cheap, mandatory, P1; (C) realism & measurement — T169 (generators + bucket override) so every later campaign measures a realistic workload with a visible tail, the **T71** saturation metrics landing here (LT-08 precondition); (D) campaigns — T167 (soak + sustained) + T168 (spike + stress + single-node composites), riding **T151**'s cadence, depending on **T71**, the poison-storm composite using the **T65** bound; (E) cluster fidelity — T166 (multi-node harness + cluster campaigns), lands last (heaviest), unlocking the **T66** full-cluster-restart assertion and the failover/upgrade composites, the standing environment deferred to **LATER-49**. Gate-independent (pure honesty/doc): the LT-01 honesty half, LT-09, LT-12-cadence (all T165).

**Checkpoint Phase 24:**
- [ ] T164 gate results (LG-1..LG-5) captured into a committed results table — the workload-shape coverage matrix (brief §11) and the harness-adequacy map (brief §12) are filled from *measured* spec/lane reality; first task records §10 **Rev 23**; **no behaviour change** in this task.
- [ ] Topology honesty (LT-01/09/12/T165): every §9 reliability criterion carries the topology it is proven against; §7 has the workload-shape coverage matrix; the spec no longer implies a clustered guarantee validated single-node (incl. §1 L98-99 + the T66 assertion); the chaos/sustained intensity is stated as a peak multiple; the heavy campaigns are stated to gate release tags, not per-PR.
- [ ] Cluster harness (LT-01/05/T166): a 3-node quorum harness with partition injection and a mixed-version axis is specified; the quorum-failover / partition / reconnect-storm / quorum-confirm-latency / rolling-upgrade campaigns are specified; T66's full-cluster-restart assertion runs on it; the standing environment is recorded as LATER-49.
- [ ] Soak + sustained (LT-02/10/T167): a ≥24h soak campaign asserts RSS/goroutine/fd/series trends over the named leak surfaces; a ≥1h sustained-throughput campaign is distinct from the one-shot benchmark.
- [ ] Overload demonstrated (LT-03/04/05/T168): spike + stress-to-failure campaigns *demonstrate* §1's no-silent-backpressure bar — every overload path surfaces a classifiable error; the poison-storm + slow-broker + slow-consumer composites run, and the T65 bound + T71 redelivery signal hold under sustained load.
- [ ] Realism + measurement (LT-06/07/08/T169): the generators model payload-size/handler-latency distributions + routing-key cardinality + burstiness, and the headline numbers state the distribution; the load reports override `WithLatencyBuckets` to cover the 30s window and report p99/p999 + saturation; T71's saturation metrics land before the spike/stress/soak campaigns run.
- [ ] Call-site/instrumentation extensions (LT-07/08/09/10/T44b+T45+T66+T71+T151): the five owning tasks each carry their `Lens-13 (LT-xx)` acceptance bullet; headers unchanged.
- [ ] Quorum-vs-classic (LT-11): confirmed already satisfied by T44b/PC-04; the campaigns inherit the queue-type-stated discipline (no regression).
- [ ] Confirmations hold (do-not-regress): the TV-09 loss-counting method (T45), the PC-04 queue-type discipline (T44b), the R10-8 barrier cap (T63), the R10-3 ordering load (T59), and the T65 DLQ bound are unchanged or strengthened.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new campaign/harness lanes green on their stated (on-demand/nightly/release) cadence — **no new *required* per-PR build-tag lane**.
- [ ] README synced: only if T166/T167 ship a runnable `examples/` artifact does the examples table change; if a "load & reliability testing" subsection is added, the testing/reliability docs stay accurate — checked per task; **no new public option in this phase**.
- [ ] SPEC §10 "Rev 23" note records the Lens-13 pass; five findings extend their owning task in place (T44b/T45/T66/T71/T151) + five groups confirmed (T45/TV-09, T44b/PC-04, T63/R10-8, T59/R10-3, T65) and are **not** re-filed; exactly **one** new `LATER.md` entry (LATER-49); T164–T169 contiguous, no duplicate IDs; tally **185 tasks / 25 phases** (Phase 9.5 decomposes T166 into T166a–T166h and pulls it forward; the Phase-24 T166 entry is now a pointer, not a counted leaf).

## Out of scope (tracked for v0.2)

- Native stream-protocol consume (`x-stream-offset`,
  super-streams, dedicated `StreamConsumeBuilder`). `QueueTypeStream`
  declaration ships in v0.1; native consume in v0.2.
- OAuth2 SASL mechanism (via `rabbitmq_auth_backend_oauth2`).
  PLAIN + EXTERNAL ship in v0.1.
- Per-message deduplication via `rabbitmq_message_deduplication`
  plugin (separate from `MessageID`-based application-side dedupe,
  which is unaffected).
- Federation / shovel topology helpers.
