# LATER.md — Deferred Improvements

## Motivation and how to use this file

This file records improvements identified during code reviews (`/ship`), security audits, or
coverage analyses that were **consciously deferred** — they are not bugs, do not block the current
merge, but deserve attention at some future point at the maintainer's discretion.

### How to use it with `agent-skills:plan`

When you want to turn items in this file into formal tasks in the implementation plan:

1. Invoke `/plan` or the `agent-skills:planning-and-task-breakdown` skill.
2. Point the agent at this file as the source of requirements: *"use LATER.md as input"*.
3. The agent decomposes each item into tasks with acceptance criteria, dependencies, and an
   execution order, following the same pattern as `tasks/plan.md` (Txx numbering, verifiable
   acceptance criteria, a verification section with commands).
4. The new tasks must be inserted into `tasks/plan.md` in the correct phase and into `tasks/todo.md`
   with a `[ ]` checkbox.

### Format of each entry in this file

Each item must contain:
- **Title** — short (the problem in one line)
- **Context** — which code is involved and why the problem exists
- **Impact** — what can go wrong if the item is never resolved
- **Evidence** — where it came from (e.g. `/ship` review, security audit, test-engineer)
- **Suggested solution** — technical direction without prescribing the full implementation
- **Prerequisites** — which plan task (`Txx`) must exist before this item is tackled

> Note: all entries are written in English (CLAUDE.md); the early entries that were once in
> Portuguese have been translated.

---

## Pending items

---

<!-- LATER-02 resolved: commit 48ee170 (fix: coerce int/uint headers to int64/uint64 during validation) -->

### LATER-05 — `wrapAMQPError` propagates the broker's `Reason` field verbatim into errors

**Context:** `errors.go:wrapAMQPError` — the `fmt.Errorf("%w: %w", sentinel, err)` wrap includes the
`.Error()` string of the `*amqp091.Error`, which carries the broker's `Reason` field with internal
topology details (queue name, vhost, resource type). E.g. `"Exception (405) Reason:
\"RESOURCE_LOCKED - exclusive access to queue 'jobs' in vhost '/'\""`.

**Impact:** In multi-tenant deployments, or ones with context separation, queue names and vhosts of
other tenants can leak into application logs, HTTP responses, or external observability systems.

**Evidence:** `/ship` security-audit T12 — MEDIUM finding.

**Suggested solution:** Add `redact.AMQPError(err)` that formats only the numeric code
(`"Exception (%d)"`) without the Reason, or redact the `Reason` before including it in the returned
error. The two-wrap step (`%w: %w`) must be preserved so `errors.Is`/`AMQPCode` keep working.

**Prerequisites:** `internal/redact` (already exists). Can be done at any time after T12.

---

<!-- LATER-06 resolved: commit 65924b5 (fix: add upper bound validation for WithChannelPoolSize) -->

<!-- LATER-03 resolved: commit 7d4dde9 (test(codec): improve fuzz coverage for lax and strict JSON codecs) -->

<!-- LATER-08 resolved: commit 57d25e4 (ci: SHA-pin third-party actions and benchstat) — every third-party action pinned to its commit SHA with a version comment across ci.yml/bench.yml/release.yml, and benchstat pinned off @latest (Phase 9 /review I6). -->

<!-- LATER-09 resolved: commit 1a2f2ad (ci: bump GitHub Actions versions) — golangci-lint binary pinned to `version: v2.12` (no longer `latest`). -->

---

<!-- LATER-10 resolved (same work as LATER-65): the integration/conformance lanes no longer pull
the floating rabbitmq:3-management tag. test/Dockerfile.rabbitmq-delayed pins `FROM rabbitmq:3.13-management`
(by SHA-256) and ci.yml builds that image for the broker jobs — exactly the rabbitmq:3.13-management
pin this entry suggested. Confirmed by Phase 9 /ship code-reviewer (S5, 2026-05-31). -->


---

### LATER-11 — `registerReconnectHook` in `connection.go` has no direct callers

**Context:** `connection.go:308` — `Connection.registerReconnectHook` was added as an internal API
for `Topology.AttachTo` and `Consumer` (T18), but `AttachTo` iterates `pubConns`/`conConns`
directly and does not use this function. As a result the function sits at 0% coverage with no
callers.

**Impact:** Dead code that confuses future readers about the hook-registration pattern.

**Evidence:** `/ship` review of T15/T16 — code-reviewer (Suggestion).

**Suggested solution:** When Consumer (T18) is implemented, check whether `registerReconnectHook`
is the correct entry point. If Consumer also iterates directly, remove the function. If Consumer
uses it, keep it and add coverage.

**Prerequisites:** T18 (Consumer re-subscribe hooks).

---

### LATER-21 — Test case B3: `x-delivery-limit` exhaustion on a quorum queue

**Context:** `consumer_handler_timeout_verdict_integration_test.go` — T18b test case B only checks
that the message is requeued at least twice (`deliveryCount >= 2`). It does not exercise the
`x-delivery-limit` exhaustion scenario: after N redeliveries the broker must automatically
dead-letter the message to the configured DLX.

**Impact:** The "requeue up to the limit → dead-letter" path is an important contract of
`TimeoutNackRequeue` (the user needs a DLX on the source queue so stuck messages do not circulate
forever). Without a test, the behavior can regress with no automatic detection.

**Evidence:** Recorded during code review of commit `23834a7` (T18b). T18b's original acceptance
criterion (case B, item 3) called for this scenario, but it was omitted because configuring a
quorum queue with `x-delivery-limit` fell outside the immediate scope.

**Suggested solution:**
Add `TestHandlerTimeoutVerdict_NackRequeue_DeliveryLimit_integration` to
`consumer_handler_timeout_verdict_integration_test.go`:

```go
// Topology: quorum queue with x-delivery-limit: 3 + DLX fanout + binding
topo := &warren.Topology{
    Exchanges: []warren.Exchange{
        {Name: dlxExch, Kind: warren.ExchangeFanout, Durable: false},
    },
    Queues: []warren.Queue{
        {
            Name:    srcQ,
            Durable: false,
            Args: map[string]any{
                "x-queue-type":          "quorum",
                "x-delivery-limit":      3,
                "x-dead-letter-exchange": dlxExch,
            },
        },
        {Name: dlqQ, Durable: false},
    },
    Bindings: []warren.Binding{
        {Exchange: dlxExch, Queue: dlqQ, RoutingKey: ""},
    },
}
// After 3 timeouts, assert deliveryCount == 3 and the message appears in the DLQ.
```

**Prerequisites:** LATER-20 (DLX/DLQ binding), RabbitMQ 3.10+ (quorum-queue `x-delivery-limit`
support). The integration/conformance lanes build the pinned `rabbitmq:3.13-management` image
(test/Dockerfile.rabbitmq-delayed), which supports quorum queues.

---

### LATER-24 — `sync.Map` retains internal memory after large batches

**Context:** `publisher.go:returnTagMap` — a `sync.Map` keeps its internal read-copy state
(`readOnly` + `dirty`) even after all pairs are deleted. After a `PublishBatch` with a large batch
(e.g. 1024 messages), the `sync.Map` can retain internal capacity proportional to the batch peak,
which is reused by the next caller that obtains the same `publisherEntry` via `pool.acquire`.

**Impact:** O(peak_batch_size) memory overhead per entry while the entry is not discarded (by
channel close) or replaced (by reconnect). In normal use the GC eventually frees it; not remotely
exploitable. Not a blocker.

**Evidence:** `/ship` security-audit — LOW finding (2026-05-24, mandatory+batch review).

**Suggested solution:** In `publisherConnPool.release`, replace `returnTagMap` with a fresh
`new(sync.Map)` if the processed batch was larger than a threshold (e.g. 256 entries):

```go
// In release, after the selects:
if shouldResetMap(entry.returnTagMap) {
    entry.returnTagMap = new(sync.Map)
}
```

Alternatively, reconsider whether `returnTagMap` should be recreated in `openPublisherEntry` on
every channel reopen (post-reconnect), since at that point the prior state is invalid anyway.

**Prerequisites:** None. Standalone, but low priority.

---

### LATER-25 — Batch latency excludes channel-acquisition time in the metric

**Context:** `publisher.go:649` — in `PublishBatch`, `msgStart = time.Now()` is set before
`tracker.Wait` (after channel acquisition). In `publishOnce`, `start = time.Now()` is set before
`pool.acquire`. So batch-latency metrics exclude the pool-wait time, while single-publish metrics
include it.

**Impact:** Dashboards comparing `publisher_publish_latency_seconds` between single and batch
publishers will observe systematically lower latencies for batch, even when the pool-wait time is
identical. It can lead operators to wrongly conclude batch is faster when the difference is a
measurement-method artifact.

**Evidence:** `/ship` code-review — Suggestion S-2 (2026-05-24, mandatory+batch review).

**Suggested solution:** Move `msgStart = time.Now()` to before channel acquisition in
`PublishBatch`, or explicitly document the semantic difference in the godoc of the
`publisher_publish_latency_seconds` metric in `metrics/`.

**Prerequisites:** None. A design decision to be made when reviewing the metric's contract.

---

### LATER-27 — Broad panic-safety audit and preventive linter for user-provided callbacks

**Context:** A manual review (2026-05-24) identified five call sites in `connection.go`,
`consumer.go`, and `batch_consumer.go` where user-provided callbacks
(`WithOnBlocked`, `WithOnReconnect`, `WithOnTopologyDegraded`, `Handler[M]`,
`BatchHandler[M]`) are invoked without `recover()` and, in some cases, inline inside
tight event-loop goroutines. T34c addresses those known sites. It is plausible that
analogous patterns exist in code not yet implemented (T25–T46) or in internal call sites
not covered by this pass.

**Impact:**
- A panic in a user callback can: crash the entire process; permanently deadlock all
  Publishers (barrier never broadcast); or silently kill a consumer goroutine, halting
  consumption with no error log.
- Blocking callbacks inside tight event-loops (`supervisor`, `runBarrier`) delay detection
  of critical broker events (connection unblock, reconnect).
- The bug is hard to reproduce in normal tests and typically surfaces in production under
  load or with buggy callback code.

**Evidence:** Manual review — session 2026-05-24 (user-requested analysis).
T34c covers the already-identified sites; this entry tracks the residual sweep and
long-term prevention via tooling.

**Suggested solution:**

1. **Systematic sweep post-T34c:** after T34c lands, review every user-supplied
   closure/func invocation in production code
   (`grep -rn "opts\." --include="*.go" | grep "nil"` as a starting point) and verify
   that `recover()` is present and that the blocking vs. non-blocking behaviour is
   documented in the option's godoc.

2. **Static linter:** evaluate tools that detect goroutines without a top-level `recover()`
   or unprotected calls to external functions:
   - [`github.com/nikolaydubina/go-recover`](https://github.com/nikolaydubina/go-recover):
     detects goroutines missing a `recover()` at the top.
   - [`revive`](https://github.com/mgechev/revive) `defer` rule can flag `defer wg.Done()`
     without a preceding `recover()`.
   - Alternatively, a custom `golangci-lint` analyzer targeting the pattern
     `go func() { userCallback() }()` without recover can be written and wired into
     `.golangci.yml` as a `custom-gcl` plugin.
   - Enable `paniccheck` if available in the golangci-lint version in use.

3. **godoc convention:** document on `WithOnBlocked`, `WithOnReconnect`, and
   `WithOnTopologyDegraded` whether the callback is blocking or non-blocking — the
   "blocks the reconnect barrier" semantic of `WithOnReconnect` is a deliberate feature
   that belongs in the public contract, not a hidden implementation detail.

**Prerequisites:** T34c (panic isolation for the already-identified sites). This entry
covers the residual audit and tooling-based prevention.

---

### LATER-26 — `wireReturnPool` and `wireFakePool` are near-duplicates

**Context:** `publisher_t13_test.go:wireReturnPool` and `publisher_test.go:wireFakePool` — both
implement the same confirm+return correlation select goroutine. They differ only in signature:
`wireReturnPool` accepts any `pubChannel` and returns the tracker separately; `wireFakePool` is
specialized for `*fakePubChannel`. Any future change to the goroutine's logic must be applied in
both places.

**Impact:** Risk of silent divergence. E.g. if the `if ret.MessageId != ""` guard is updated in one
function but forgotten in the other, the tests would exercise semantics different from production.

**Evidence:** `/ship` code-review — Suggestion S-1 (2026-05-24, mandatory+batch review).

**Suggested solution:** Extract the goroutine body into a shared unexported function
`runReturnCorrelationLoop(returnCh <-chan amqp091.Return, confirmCh <-chan amqp091.Confirmation,
rtm *sync.Map, tracker *confirms.Tracker, onReturn func(amqp091.Return))` and have both functions
call it. Alternatively, collapse `wireReturnPool` into `wireFakePool` with a generic `pubChannel`
parameter and optionally return the tracker.

**Prerequisites:** None. Pure test refactor, no impact on production code.

---

<!-- LATER-33 resolved: commit 167f0a4 (fix: truncate MessageId to 512 bytes in redelivery counter B keys) -->

### LATER-34 — Synchronous-confirm throughput ceiling / async publish API (architecture decision)

**Context:** `publisher.go` — `Publish` acquires a channel from the per-connection pool,
publishes, **blocks on its own confirm**, and only then returns the channel (SPEC §6.1/§6.2).
A single in-flight publish therefore holds a whole channel for a full broker round-trip, so
per-`Publish` throughput is bounded by `(channels × connections) / RTT_confirm`. Rev 6 decision
31 deliberately rejected a sliding in-flight window; `PublishBatch` is the only pipelining path,
and it is itself synchronous (blocks until the whole batch's confirms or `ConfirmTimeout`).
Surfaced by the Rev 10 specialist re-review (2026-05-25) as finding **P1.1 / R10-18**.

**Impact:**
- The §9 throughput targets (≥30k/s single-conn, ≥100k/s multi-conn) hold only at sub-millisecond
  RTT (local broker). Against a remote broker at 2–5 ms RTT the achievable single-`Publish` rate
  drops by the same factor, and the SPEC does not state this honestly.
- **Cliff under confirm-latency spikes.** If broker confirm latency rises (GC pause, disk pressure,
  quorum failover), the same fixed channel pool yields far fewer msg/s and the overflow surfaces as
  `ErrChannelPoolExhausted` — confirm latency converts directly into publish unavailability. This
  is a classic on-call cascade and is invisible in the current design's stated guarantees.

**Evidence:** Rev 10 AMQP/SRE specialist re-review, finding P1.1 (2026-05-25). Recorded in SPEC §10
"Rev 10" as R10-18, deferred here because it is an owner architecture decision, not a defect.

**Suggested solution (decision required, then a design task):**
1. **Document the ceiling honestly now (cheap):** add a SPEC §6.2 note that single-`Publish`
   throughput is `pool/RTT`, that confirm-latency spikes cause `ErrChannelPoolExhausted`, and that
   `PublishBatch` is the high-throughput path.
2. **Decide on an async API (reverses Rev 6 decision 31):** evaluate either
   - `PublishAsync(ctx, msg) (<-chan error)` backed by a per-channel confirm-tracking goroutine that
     pipelines many publishes on one channel with a **bounded in-flight window** for backpressure; or
   - a streaming publisher handle with an explicit in-flight budget and confirm callbacks.
   Either decouples throughput from RTT and removes the pool-exhaustion cliff, at the cost of API
   surface and the duplicate-budget bookkeeping the async path implies (still at-least-once;
   consumers dedupe by `MessageID` per §6.2.1).

**Prerequisites:** Owner decision on reopening Rev 6 decision 31. Builds on T11
(`internal/confirms`) and T12/T13 (publisher). If accepted, it becomes a new task (likely a
Phase 11 follow-on) with its own SPEC amendment; if rejected, item 1 (the honesty note) lands and
this entry is closed.

---

### LATER-35 — No consumer-side message-size guard (inbound deserialization backpressure)

> **Promoted to T131 (Phase 18, Lens-07 ST-06), 2026-05-29 — re-classified LOW → Blocker.** The
> security & threat-modeling lens re-scored this fail-open inbound DoS as a must-fix Blocker (a single
> hostile or buggy producer ships a ~512 MiB body that `amqp091` reassembles in memory before the codec
> runs → consumer OOM, with no consume-side cap). T131 implements the consume-side
> `MaxInboundMessageSizeBytes` guard (default 16 MiB, fail-closed). **Remove this entry when T131
> lands** (per CLAUDE.md, cite T131 in the commit message). Kept here until then so the finding is not
> re-filed.

**Context:** `publisher.go` enforces `maxMessageSizeBytes` before sending, but the consume path
(`consumer.go:dispatch` → `safeDecodeConsumer`, and the equivalent in `batch_consumer.go`) has no
symmetric cap. For the structured CloudEvents codec (and any future body-parsing codec), the full
delivery body is handed to the SDK's `event.UnmarshalJSON` (json-iterator) and parsed in memory
before the handler runs. The binary CloudEvents codec is NOT affected — it stores the body as raw
`DataEncoded` bytes without parsing.

**Impact:** Memory/CPU pressure under a malicious-or-buggy producer model. An authorized producer
(or a compromised upstream) can publish a very large / deeply-nested JSON body that a consumer
parses fully before deciding anything. Bounded today by RabbitMQ frame size and prefetch count, so
this is defense-in-depth, not a remote-unauthenticated crash. Not a blocker.

**Evidence:** `/ship` security-audit (2026-05-28, T25/T26 CloudEvents review) — LOW finding.

**Suggested solution:** Add an optional consumer-side `MaxMessageSizeBytes` (builder option on
`ConsumerFor`/`BatchConsumerFor`) that rejects-and-nacks (no requeue) before invoking the codec,
mirroring the publisher guardrail. Record a `decode_error`/`too_large` outcome metric so it is
observable. Re-check the SDK's `jsoniter.ConfigFastest` parsing behaviour whenever
`cloudevents/sdk-go/v2` is bumped.

**Prerequisites:** None. Standalone consumer-side hardening; coordinates with the consumer builder
options surface.

---

### LATER-36 — Binary CloudEvents mode narrows non-string extension types to strings

**Context:** `codec/cloudevents.go` — `NewCloudEventsBinary().EncodeWithHeaders` formats every
context attribute and extension via the SDK's `types.Format`, so a typed extension (e.g. an
integer or boolean) is written to the `cloudEvents:`-prefixed AMQP header as its canonical string
form, and `DecodeWithHeaders` reads it back as a string (non-string header values are treated as
absent). Structured mode preserves the JSON type. The CloudEvents AMQP Protocol Binding does allow
non-string AMQP property types for Integer/Boolean extension attributes, so a fully spec-faithful
binary codec would preserve the type both ways.

**Impact:** A non-Go producer that sends an integer/boolean CloudEvents *extension* as a typed AMQP
property would have it round-trip through a warren consumer as a string; conversely, warren always
emits string-typed extension headers. Core attributes (id/source/type/specversion/subject/
dataschema/time/datacontenttype) are unaffected — they are String/URI/Timestamp by spec. This is a
fidelity gap only for typed *extensions*, documented in the codec godoc, and accepted for now.

**Evidence:** `/ship` test-engineer coverage analysis (2026-05-28, T25/T26) — finding #8; behaviour
is now pinned by `TestCloudEventsBinary_RoundTrip_NonStringExtensionNarrowsToString` and contrasted
with `TestCloudEventsStructured_RoundTrip_PreservesExtensionType`.

**Suggested solution:** If full type fidelity is desired, change `EncodeWithHeaders` to write the
native AMQP type for scalar extension values (int → AMQP long, bool → AMQP boolean) and
`DecodeWithHeaders`/`ceHeaderString` to pass the typed value to `SetExtension` for the
amqp091-supported scalar types, treating tables/slices as invalid. This is a wire-format change and
must amend SPEC §6.9 first; it widens the round-trip contract and needs new round-trip tests against
a non-Go producer fixture.

**Prerequisites:** SPEC §6.9 amendment (codec interop, decision 4). Standalone after that.

---

### LATER-37 — Publisher size cap measures the body only; HeaderCodec attribute headers bypass it

**Context:** `publisher.go:encodeMsg` enforces `maxMessageSizeBytes` against `len(body)` only. For a
`HeaderCodec` such as the binary CloudEvents codec, the event attributes and extensions travel as
`cloudEvents:`-prefixed AMQP headers rather than in the body, so a publish with many or large
extension headers can exceed the intended on-wire size while passing the body cap.

**Impact:** The per-publish guardrail's contract ("reject before opening a channel when the message
is too large") is partially circumvented when a HeaderCodec carries significant data in headers. Low
severity: header sizes are still bounded by amqp091 frame limits and by `validateHeaders`, the body
(usually the bulk of a message) is still capped, and the gap is not CloudEvents-specific — it
applies to any HeaderCodec. No correctness impact.

**Evidence:** `/ship` code-reviewer (2026-05-28, T25/T26 CloudEvents review) — Important finding,
`publisher.go:454`.

**Suggested solution:** Either (a) document in the `WithMaxMessageSizeBytes` godoc that the cap
measures the encoded body only, not codec-emitted headers; or (b) for HeaderCodecs, add the
serialized header (and content-type) byte sizes into the size check before the `too_large` verdict.
Prefer (a) unless a header-heavy codec makes the gap material in practice.

**Prerequisites:** None. Standalone publisher-side hardening.

---

### LATER-38 — `publisher_publish_seconds{outcome}` metric label space lags the span outcome contract

**Context:** T27 implemented the publish span's `messaging.rabbitmq.outcome` attribute using the rich
label space mandated by SPEC §6.9 (`ack`/`nack`/`return`/`timeout`/`too_large`/`pool_exhausted`/
`blocked`/`error`), via `publishOutcome` in `publisher.go`. The `publisher_publish_seconds{outcome}`
Prometheus histogram, however, is still recorded with the coarse legacy labels `success`/`error`/
`too_large` in `publishOnce` and `PublishBatch`. SPEC §6.9 states the span outcome "mirrors the
metric `publisher_publish_seconds{outcome}` label" — i.e. both are intended to share the rich space.

**Impact:** Operators cannot pivot 1:1 between a trace's outcome and the metric's outcome dimension;
"show me the publish-latency distribution for `outcome=return`" is not answerable from the metric
because the metric only emits `error` for unroutable/nacked/timeout/blocked/pool-exhausted alike.
No correctness impact — both signals are individually correct; only the label vocabularies diverge.

**Evidence:** T27 (OTel in Publisher) implementation, 2026-05-28. Out of T27 scope: the task's
Files list is `publisher.go` + `publisher_tracing_test.go`, while aligning the metric labels also
requires updating the metric-assertion tests (`publisher_test.go`, `publisher_batch_test.go`).

**Suggested solution:** Route the metric's outcome label through the same `publishOutcome` mapper
used by the span, so `RecordPublish` receives `ack`/`nack`/`return`/`timeout`/`too_large`/
`pool_exhausted`/`blocked`/`error`. Update the affected metric-assertion tests in the same change.
Note this is a metric-label vocabulary change and may affect downstream dashboards/alerts.

**Prerequisites:** T27 (done). Coordinate with any dashboard/alert owners before changing labels.

---

### LATER-39 — Protobuf multi-type-queue discriminator (Any / type-URL / type registry)

**Context:** `codec.NewProtobuf()` decodes proto3 binary, which carries **no type
information on the wire**. `ConsumerFor[M]` fixes the Go message type at compile
time, so a single-type queue decodes fine — but a topic-exchange queue carrying
**multiple** proto event types (a common event-driven pattern) cannot be decoded:
the consumer has no way to know which `proto.Message` a given body is. Phase 14
(T99, IW-05) documents this single-type-per-`Consumer` constraint but deliberately
defers a first-class fix.

**Impact:** Polyglot/event-driven pipelines that publish several proto types to one
queue must either split the queue per type or hand-roll a discriminator on top of
the library (e.g. read `Message[M].Type` and switch). Without a built-in mechanism,
`ConsumerFor[M]` is effectively single-type-only for protobuf, which is a real
ergonomics + interop gap relative to JSON/CloudEvents where the body is
self-describing.

**Evidence:** Lens-03 interoperability/wire-format spec validation, 2026-05-28
(finding IW-05; `docs/spec-validation/03-interoperability-wire-format-plan.md`). Owner
decision (2026-05-28): defer the discriminator to LATER, document the constraint now.

**Suggested solution:** Offer an opt-in discriminator strategy that does not break
the typed `ConsumerFor[M]` ergonomics, e.g. (a) a documented convention that
`Message[M].Type` (`basic.properties.type`) carries the proto full message name,
plus a registry-backed `RawConsumer` / decode helper that dispatches by name to the
right `proto.Message`; or (b) support `google.protobuf.Any` (type-URL carried in
the body) for self-describing multi-type payloads. Prefer (a) for cross-language
interop (Java/Python set `type` too) and reserve (b) for callers already on `Any`.
Either way it is additive — single-type `ConsumerFor[M]` stays the default.

**Prerequisites:** T99 (documents the single-type constraint + the `type`-as-name
convention this builds on).

---

### LATER-40 — Pluggable schema-registry / validation hook for event evolution

**Context:** `warren` relies on lax-JSON-by-default (Postel, decision 23) for
*additive* schema evolution, and Phase 15 (T106, EDA-13) documents the boundary —
**breaking** evolution (field removal, rename, semantic change) is user-owned via
versioned type names (`order.created.v2`) and the `Message.Type` discriminator
convention. There is no library primitive to *enforce* a schema or validate a payload
against a registered contract before it reaches the handler (consume side) or hits the
wire (publish side). Across dozens of independently-deployed services, schema drift is
currently caught only at the first decode failure or, worse, a silently-mis-decoded
field that lax JSON tolerates.

**Impact:** Event-mesh / multi-team estates have no central, machine-checkable contract
enforcement. A producer can ship a breaking change and the only signal is downstream
decode errors (or silent data corruption where lax JSON tolerates the change). Teams
that want Confluent-style schema-registry guarantees (compatibility checks, schema IDs
on the wire) must hand-roll them on top of the codec interface.

**Evidence:** Lens-04 event-driven-architecture spec validation, 2026-05-28 (finding
EDA-13; `docs/spec-validation/04-event-driven-architecture-plan.md`). Owner decision
(2026-05-28): document the boundary + versioned-type convention now (T106), defer a
pluggable registry hook to LATER.

**Suggested solution:** Offer an opt-in, codec-adjacent `SchemaValidator` hook — e.g. a
builder option on `PublisherFor`/`ConsumerFor` taking an interface
`Validate(ctx, contentType, typeName string, body []byte) error` — that runs after
encode / before decode and rejects (publish: `ErrInvalidMessage`; consume:
nack-no-requeue + a `schema_invalid` outcome metric) on a contract violation. Provide a
no-op default and a reference adapter for one registry (e.g. JSON Schema, or a
Confluent-compatible registry that carries a schema ID in a header). Keep it additive —
the lax-JSON default and the `Message.Type` convention stay the baseline; the validator
is for teams that need enforced contracts. Any wire-format change (a schema-ID header)
must amend SPEC §6.9 first.

**Prerequisites:** T106 (documents the evolution boundary + the `Message.Type`
versioned-type convention this builds on); coordinates with the codec/`HeaderCodec`
interface in `codec/`.

---

### LATER-41 — Dedicated `ReturnCode(err) (uint16, bool)` accessor to separate `basic.return` codes from channel-close codes

**Context:** `AMQPCode(err) (uint16, bool)` (`errors.go:171–195`, decision 38) returns
both a `basic.return` reply-code (312 `NO_ROUTE` / 313 `NO_CONSUMERS`, from a `*codeError`
raised on an unroutable mandatory publish) **and** channel/connection-close reply-codes
(311/320/402–406/…) through one `(uint16, bool)` with no provenance signal. A caller that
`switch`-es on the returned code cannot tell which AMQP frame class produced it — a 312
from a returned message looks identical to a close-code path. Lens-06 (GA-08) ships the
doc caveat in T126 (combine `AMQPCode` with `errors.Is(err, ErrUnroutable)` to
disambiguate) but does **not** add a typed accessor.

**Impact:** Callers building routing/alerting logic on the numeric code must remember to
cross-check `ErrUnroutable` by hand; a naive `switch AMQPCode(err)` mishandles the
`basic.return` 312/313 space, conflating an unroutable-publish signal with a
channel-close failure. The two frame classes deserve distinct, self-describing accessors.

**Evidence:** Lens-06 Go-API/library-design spec validation, 2026-05-28 (finding GA-08;
`docs/spec-validation/06-go-api-library-design-plan.md`). Owner-deferred: T126 ships the §6.8
caveat now; the typed accessor is non-blocking.

**Suggested solution:** Add a dedicated `ReturnCode(err) (uint16, bool)` that returns a
code **only** when the error chain carries `ErrUnroutable` (the `basic.return` space),
leaving `AMQPCode` for channel/connection-close codes; or introduce a small code-class
enum (`CodeClassReturn` / `CodeClassClose`) so a single accessor reports provenance. Keep
both `errors.Is`/`As`-friendly and `errorlint`-clean; document in §6.8. Purely additive
(a new function; no signature change to `AMQPCode`).

**Prerequisites:** T126 (ships the §6.8 `AMQPCode` frame-class caveat this builds on); the
error taxonomy in `errors.go` + `internal/amqperror`.

---

### LATER-42 — Configurable SASL EXTERNAL principal extraction (SAN / DN beyond CN)

**Context:** `connection.go:122` (`computeAuthUser`) derives the SASL EXTERNAL principal from the
**CN** of the first client certificate only. RabbitMQ's `rabbitmq_auth_mechanism_ssl` plugin exposes
`ssl_cert_login_from`, configurable to `common_name` (default), `distinguished_name`, or
`subject_alternative_name` (with a SAN type/index). When the broker is configured for DN or SAN,
warren's client-side `UserID` check (decision 39) compares against the CN it extracted, which diverges
from the principal the broker actually authenticates — a false `ErrInvalidMessage` reject, or a
`UserID` value the broker then 406s. T136 (Phase 18, Lens-07 ST-04) ships the **doc-only** mitigation
(document the CN assumption + the divergence; recommend leaving `UserID` empty under non-CN broker
mappings) but does not add configurable extraction.

**Impact:** EXTERNAL deployments whose broker maps the principal from DN or SAN cannot use warren's
client-side `UserID` stamping/validation without leaving `UserID` empty (losing the client-side
guard). Low severity: a documented workaround exists (empty `UserID`), EXTERNAL itself still
authenticates correctly (the broker decides), and CN is the RabbitMQ default. The gap is ergonomic,
not a correctness/security hole once documented.

**Evidence:** Lens-07 security & threat-modeling spec validation, 2026-05-29 (finding ST-04;
`docs/spec-validation/07-security-threat-modeling-plan.md`). Owner decision D4: doc-only for v0.1;
configurable extraction deferred.

**Suggested solution:** Add a `WithExternalPrincipalFrom(...)` connection option (or a small
`ExternalPrincipalSource` enum: `CN` / `DN` / `SAN`) that selects how `computeAuthUser` extracts the
principal from the client cert, matching the broker's `ssl_cert_login_from`. For SAN, accept the SAN
type (DNS / email / URI) to read. Keep CN the default (no behaviour change). Add a round-trip
integration test against a broker configured for each `ssl_cert_login_from` value.

**Prerequisites:** T136 (ships the §6.5/decision-35 CN-extraction documentation this builds on); the
EXTERNAL auth path in `connection.go` (`computeAuthUser`).

---

### LATER-43 — Optional aggregate in-flight confirm window (`WithMaxInFlightConfirms`)

**Context:** The publisher confirm tracker (`internal/confirms`) bounds outstanding delivery-tags
**per call** — a single `PublishBatch` holds at most `PublishBatchMaxSize` waiters, and a single
`Publish` holds one. There is **no aggregate cap across concurrent calls**: N goroutines each issuing
`PublishBatch`/`Publish` on the same `Connection` hold N independent windows, so the total in-flight
confirm memory is `N × PublishBatchMaxSize`, bounded only by how much the caller fans out. SPEC §6.2
already admits the per-call scope ("does not currently throttle multiple concurrent `PublishBatch`").

**Impact:** Under an unbounded publisher fan-out (e.g. one goroutine per request at the billions/day
bar) the confirm-tracker memory grows with concurrency rather than with a fixed window — a slow or
degraded broker that stalls confirms lets the in-flight set balloon. Low severity: the common case
(bounded worker pools, a handful of publishers) is already bounded by the per-call cap, the growth is
publisher-driven (the caller controls fan-out), and there is no message loss — only memory pressure.
T145 (Phase 19, Lens-08 CR-07) ships the **doc-only** mitigation (document the per-call boundary +
recommend caller-side fan-out limiting) but adds no aggregate cap.

**Evidence:** Lens-08 Go concurrency & runtime-correctness spec validation, 2026-05-29 (finding CR-07;
`docs/spec-validation/08-go-concurrency-runtime-plan.md`). Owner decision D4: document the per-call boundary
for v0.1; the aggregate window deferred.

**Suggested solution:** Add a `WithMaxInFlightConfirms(n int)` connection (or publisher) option that
bounds the **aggregate** count of outstanding confirm waiters across all concurrent
`Publish`/`PublishBatch` calls on the `Connection` — a semaphore acquired before a publish admits its
waiter(s) and released as confirms resolve, so a stalled broker applies backpressure to publishers
instead of growing memory. `0` (default) keeps today's per-call-only behaviour (no aggregate cap, no
behaviour change). Surface waiting on the semaphore as ctx-cancellable (mirrors pool-acquire ergonomics)
and add a saturation metric. A unit test asserts the aggregate cap holds under N concurrent publishers.

**Prerequisites:** T145 (ships the §6.2 per-call-boundary documentation this builds on); the confirm
tracker in `internal/confirms` and the publisher confirm path.

---

### LATER-45 — Deeper hot-path allocation elimination (pooled-buffer codec + a `timeMu`-free UUIDv7 generator)

**Context:** Phase 20 (Lens-09) lands the *cheap, zero-risk* per-message hot-path allocation wins — pool/reset
the confirm-`Wait` `time.Timer` (T11/PC-06), gate the NoOp-tracer span argument-construction (T148/PC-07),
`uuid.EnableRandPool()` (T10/PC-09), lazy-allocate the x-death `byReason` map (T17/PC-08), and cache the
Prometheus child collectors (T148/PC-15) — under a combined `AllocsPerRun` guard (T148). Three deeper
allocations/locks remain on the per-message path after those: the JSON codec `Marshal`s the body **without a
buffer pool** (`codec/json.go:39`), the confirm tracker allocates a `&waiter{}` + a `make(chan error, 1)` **per
`Register`** (`tracker.go:77`), and google/uuid takes a **process-global `timeMu` lock per UUIDv7** even after
`EnableRandPool` (`message.go:73`).

**Impact:** At the billions/day bar these are residual per-message costs the cheap wins do not remove — a body
allocation + copy per publish, a waiter+channel allocation per in-flight publish, and a process-wide
serialization point on `timeMu` under high publish concurrency. Low-Med severity: none is a correctness or
message-loss issue (the body and waiter are *necessary* objects, just poolable; `timeMu` is a contention point,
not a deadlock), and the cheap-win baseline already removes the highest-frequency avoidable allocations. The
deep wins carry codec-API and dependency implications (a pooled `Encode` changes the codec call shape; a
`timeMu`-free generator means a custom UUIDv7 source rather than google/uuid), so they are consciously deferred.

**Evidence:** Lens-09 performance & capacity spec validation, 2026-05-29 (§12 hot-path allocation ledger;
`docs/spec-validation/09-performance-capacity-plan.md`). Owner decision D1: land the cheap wins in Phase 20; defer
the deep wins.

**Suggested solution:** (1) A pooled-buffer codec `Encode` path — a `sync.Pool` of `bytes.Buffer` + a reused
`json.Encoder` so the per-message body buffer is recycled (assess the consume-side `json.NewDecoder`/
`bytes.Reader` per delivery the same way, `codec/json.go:48-55`). (2) A `sync.Pool` of `*waiter` (with its
`done` channel) recycled when `Wait` returns, so a steady-state publish stream reuses waiters. (3) A UUIDv7
generator that avoids the google/uuid process-global `timeMu` (a per-P / sharded monotonic time source, or an
internal generator), keeping RFC 9562 v7 layout and the at-least-once dedupe contract. Each lands behind the
T148 `AllocsPerRun` guard so the win is measured against the cheap-win baseline.

**Prerequisites:** T148 (the cheap-win allocation hardening + the combined `AllocsPerRun` baseline these deeper
wins are measured against); the codec interface (`codec/`), the confirm tracker (`internal/confirms`), and the
UUIDv7 default-apply path (`message.go`).

---

### LATER-46 — Residual fuzz target for otel propagation-header extraction (`FuzzHeadersExtract`)

**Context:** The 6 v0.1 fuzz targets cover every attacker-influenced **byte-parser** in the library —
`FuzzRedactURI` (`internal/redact`), `FuzzCodecJSON` + the strict variant (`codec/json.go`),
`FuzzCodecProtobuf` (`codec/protobuf.go`), `FuzzCodecCloudEventsBinary` (`codec/cloudevents.go`), and
`FuzzXDeathParser` (`internal/headers`). Lens-10 (test-strategy & verifiability) checked for residual fuzz
gaps and found one genuine candidate and one non-candidate. The **non-candidate:** `internal/amqperror` keys
on the **numeric** reply code (`*amqp091.Error.Code`, a `uint16`), not free-form bytes — there is no
byte-parser to break, so a `FuzzAMQPCode` would be low-value (noted honestly, not filed as a gap). The
**genuine residual:** the otel propagation-header **extraction** path in `internal/headers` reads
producer-controlled header *values* (the inbound `traceparent`/`tracestate`/baggage carrier on consume),
which are attacker-influenceable but not currently fuzzed.

**Impact:** Low. The extraction path is bounded (it feeds the otel propagator, not a hand-rolled parser) and a
malformed carrier degrades to "no parent span", not a panic or message loss; but a `FuzzHeadersExtract` would
close the last attacker-influenced-input gap and lock the no-panic / bounded-allocation contract on the consume
header path, matching the bar the other 6 targets already hold.

**Evidence:** Lens-10 test-strategy & verifiability spec validation, 2026-05-29 (brief §5 WS-5 + §13 finding row
"LATER-46 residual fuzz"; `docs/spec-validation/10-test-strategy-verifiability-plan.md`). The lens routes the
load-bearing byte-parsers to the existing 6 targets and defers this one residual.

**Suggested solution:** Add `FuzzHeadersExtract` in `internal/headers` that feeds randomized `amqp091.Table`
header sets (string + `[]byte` `traceparent`/`tracestate`/baggage values, including oversized / non-UTF-8 /
deeply-nested forms) through the otel extraction path and asserts no panic, bounded allocation, and a
well-formed-or-empty `SpanContext`. Wire it into the nightly fuzz 10m-budget runner (T151) alongside the
existing targets. Explicitly record in the test-strategy docs that `internal/amqperror` is intentionally
**not** fuzzed (numeric-code keyed, no byte-parser).

**Prerequisites:** T44 / T37 (the conformance + `amqptest` harness this rides alongside) and T151 (the nightly
fuzz-budget runner that gives the new target a cadence beyond the unit-lane seed corpus); the otel header
extraction path in `internal/headers`.

---

### LATER-47 — Message-level payload encryption / crypto-erasure (per-subject key → effective erasure without a delete primitive)

**Context:** Lens-11 (data-protection compliance, GDPR & LGPD) found that personal data published as a message
**body** to a durable queue is effectively **un-erasable** at the bus layer (DP-01) — AMQP 0-9-1 has no
primitive to delete a single message, and with `DeliveryModePersistent` the zero-value default the body sits on
broker disk, is replicated (quorum), and is copied to any DLQ on dead-letter. The same body is **plaintext at
rest** (DP-07) — RabbitMQ does not encrypt bodies on disk or in backups. T154 (pointer-out) and T141 (the
at-rest boundary note) address these at the *documentation + pattern* level; this entry tracks the
*cryptographic* answer, deliberately deferred (owner decision D3).

**Impact:** Low (deferred by design). The pointer-out pattern (T154) already gives controllers a documented,
runnable way to make personal data erasable (keep PII in an erasable store of record; publish only an opaque
reference). Crypto-erasure is the *in-band* alternative for teams that cannot externalise the payload, but it is
a large, opinionated feature with key-management implications, and T141 already states message-level payload
encryption is out of scope for v0.1.

**Evidence:** Lens-11 data-protection compliance spec validation, 2026-05-29 (brief
`docs/spec-validation/11-compliance-gdpr-lgpd-plan.md` §13 findings table DP-01/DP-07, §10 owner decision D3, §14
out-of-scope). The lens routes the documentation-level mitigations to T154/T141 and defers the cryptographic
control here.

**Suggested solution:** A `codec` wrapper (e.g. `codec.NewEncrypted(inner, keyProvider)`) that encrypts the
body with a **per-data-subject key** before `Encode` and decrypts after `Decode`; deleting a subject's key on an
erasure request renders the ciphertext on the bus / DLQ / disk / backups unrecoverable = **effective erasure**
despite the absence of a single-message delete primitive (and effective at-rest protection for DP-07). Requires
a pluggable key-management interface, an AEAD scheme (key id in a header, nonce per message), a documented
key-rotation + key-destruction story, and interop guidance for non-Go consumers (the ciphertext envelope must
be a documented, language-neutral format). Position it alongside pointer-out as the in-band alternative, not a
default.

**Prerequisites:** T154 (pointer-out + the un-erasable warning land first as the documentation-level
mitigation, so this is an *additional* in-band control, not the only answer); coordinates with T141 (the at-rest
boundary it cryptographically closes) and the `codec` package design (T135's build-tag treatment of heavy
codecs is the precedent for keeping an optional codec out of the core dependency closure).

---

### LATER-48 — Full observability & alerting cookbook example (Prometheus + Grafana dashboards + alert-rule library)

**Context:** Lens-12 (developer-experience & documentation) found that the operational hard cases have no
runnable references (DX-10). T163 (Phase 23) lands the P1 examples (`durable_retry`, `graceful_shutdown`) and
a minimal `observability` example (metrics wiring + the mandatory-metric alert rules — `publisher_retry_total`,
`consumer_handler_aborted_…`, `topology_redeclare_seconds`). The *full* cookbook — a ready-to-import Grafana
dashboard JSON, a complete Prometheus alert-rule file covering every mandatory metric, and a narrated runbook
mapping each alert to its remedy/knob — is deliberately deferred (owner decision D5).

**Impact:** Low (deferred by design). The minimal `observability` example (T163) and the SRE-lens metrics/alert
documentation already give operators the wiring and the alert thresholds; the cookbook is a polish/onboarding
accelerator, not a correctness gap. Without it, operators must assemble dashboards and the full alert set
themselves from the metric reference.

**Evidence:** Lens-12 DX & documentation spec validation, 2026-05-29 (brief
`docs/spec-validation/12-dx-documentation-plan.md` §13 findings table DX-10, §14 examples-gap list item 6, §10 owner
decision D5, §16 out-of-scope). The lens routes the minimal example to T163 and defers the full cookbook here.

**Suggested solution:** Ship a `examples/observability/` (or a `docs/observability/` companion) containing: a
versioned Grafana dashboard JSON (publish/consume rates, confirm latency, reconnect/degraded-state gauges,
handler-abort + retry counters), a complete `prometheus/alerts.yml` covering every mandatory metric with
documented thresholds and `for:` windows, and a runbook table mapping each alert → likely cause → remedy/knob
(reusing the DX-08 error-remedy policy). Keep it outside the unit/`examples-smoke` lanes if the dashboard JSON
cannot be meaningfully smoke-tested; at minimum lint the alert-rule file (`promtool check rules`).

**Prerequisites:** T163 (the minimal `observability` example + the mandatory-metric alert rules land first as the
runnable baseline); coordinates with the SRE-lens metrics catalogue (the metric names + the alert thresholds it
documents) and T162 (the error-remedy/knob policy the runbook reuses).

---

### LATER-49 — Standing multi-node load environment + load-generator fleet (dedicated 3-node cluster + sustained-traffic generators for the heavy campaigns)

**Context:** Lens-13 (load & reliability testing) found that every clustered §9 reliability claim is validated only
on a single-node testcontainers harness (LT-01), and the heavy workload shapes (soak/endurance ≥24h, peak-multiple
sustained, spike, stress-to-failure, composite) have no defined load environment or release-gating cadence
(LT-02/12). T166 (Phase 24) lands the multi-node cluster harness *contract* + the cluster campaign *specs* against
an **ephemeral** compose cluster spun up on-demand; T167/T168 land the single-node-runnable campaigns on T151's
on-demand/nightly cadence. The *standing* infrastructure to run them continuously — a dedicated, always-on 3-node
quorum cluster plus a load-generator fleet able to sustain peak-multiple traffic for ≥24h soaks and release-gating
runs — is deliberately deferred (owner decision D5).

**Impact:** Medium (deferred by design). Without it the cluster/soak/peak campaigns run on-demand against ephemeral
infrastructure and **cannot gate releases continuously**, so the §9 clustered criteria remain proven-on-request
rather than proven-every-release; with it the billions/day-clustered bar becomes a continuously enforced gate. It
is an ops/infra investment (cost, hosting, ops cadence), not a code or correctness gap — the harness contract and
campaign specs (T166) already make the campaigns runnable.

**Evidence:** Lens-13 load & reliability-testing spec validation, 2026-05-29 (brief
`docs/spec-validation/13-load-testing-plan.md` §13 findings table LT-01/02/12, §14 minimum-campaign-set item 5, §15 open
question Q1, §10 owner decision D5, §16 out-of-scope). The lens lands the harness contract + campaign specs in T166
and defers the standing environment here.

**Suggested solution:** Stand up a dedicated multi-node load environment: an always-on (or scheduled-provision)
3-node RabbitMQ quorum cluster with partition-injection tooling (`pause_minority`/`autoheal`) and a pinned
mixed-version (3.13/4.x) capability, plus a load-generator fleet (separate hosts/runners, not co-located with the
broker) able to sustain a stated peak multiple of the ~11.6k/s average for ≥24h soaks; wire it as a release-gating
lane that blocks a `v0.x.0` cut on a green run (or runs advisory, per Q2). Reuse the T166 harness contract + the
T169 realistic generators + the T45/TV-09 loss-counting method.

**Prerequisites:** T166 (the multi-node cluster harness *contract* + cluster campaign *specs* land first as the
on-demand mitigation), T167/T168 (the campaign definitions the standing environment runs), T169 (the realistic
generators). Coordinates with T151 (the scheduled/nightly CI infrastructure this lane extends). The LATER-44 gap is
unrelated (Phase-18's conditional codec-split reservation) and is left untouched.

---

### LATER-50 — `ChannelPoolSize == channel-max` leaves no channel-ID headroom for transient channels

**Context:** `connection.go` — `Dial` now rejects `WithChannelPoolSize` strictly greater than the
broker-negotiated channel-max (SPEC §6.1 / Phase 1 checkpoint), so `poolSize == channelMax` is permitted.
But the publisher channel pool, `Connection.Health` (`openChannel` on `pubConns[0]`), and topology redeclare
during the reconnect barrier all open channels on the *same* publisher TCP connection. With `poolSize` equal
to the negotiated ceiling and the pool fully populated, no channel ID remains for those transient channels.

**Impact:** Under a maxed-out pool, `Health` and post-reconnect topology redeclare can fail with a broker
channel-max error (504 `CHANNEL_ERROR` / connection-level `NOT_ALLOWED`) instead of succeeding — a latent
operational footgun that only manifests at peak channel usage. Not a correctness bug (the validation matches
the literal protocol ceiling), but surprising.

**Evidence:** `/agent-skills:review` five-axis review of branch `fix/phase1-verification-gaps`, 2026-05-29
(fresh-context code-reviewer, "operational tightness" suggestion).

**Suggested solution:** Either (a) document in the `WithChannelPoolSize` godoc that the pool should be sized
below the channel-max to leave headroom for Health/redeclare/transient channels, or (b) reserve a small fixed
headroom (e.g. validate `poolSize <= channelMax - reserved`, reserved ≥ 1) — note (b) tightens the SPEC §6.1
"strictly greater" wording and must amend SPEC first.

**Prerequisites:** none (self-contained doc or validation tweak); coordinates with T34 (`WithFrameMax`/sizing
godoc sweep) if the doc route is chosen.

---

### LATER-51 — `uuid.EnableRandPool()` mutates a google/uuid process-global on behalf of the whole process

**Context:** `message.go` — the package `init()` calls `uuid.EnableRandPool()` (Lens-09 PC-09) to batch the
per-publish `crypto/rand` read and drop one allocation per `MessageID`. That flips a process-global flag inside
`github.com/google/uuid`, so any application that imports `warren` — even transitively, even if it never publishes —
inherits the changed entropy-batching behaviour for its own `uuid.NewV7`/`NewRandom` calls. `EnableRandPool`/
`DisableRandPool` are documented by google/uuid as **not thread-safe**.

**Impact:** Low and not exploitable as written: the only call site is `init()` (single-threaded, before any
goroutine), nothing in the tree calls `DisableRandPool`/`SetRand` to race it, and the security audit confirmed the
pool draws from `crypto/rand` so UUIDv7 entropy/uniqueness is unchanged (the at-least-once dedupe contract is
intact). The residual concern is etiquette: a library silently mutating a shared third-party global can surprise a
downstream app with its own uuid tuning, and would become a real data race if any future code path toggled the pool
at runtime.

**Evidence:** `/agent-skills:ship` parallel review of branch `worktree-phase-2-validation`, 2026-05-29
(security-auditor LOW finding).

**Suggested solution:** Replace the global toggle with a package-scoped pooled generator: keep a `sync.Pool`/buffered
crypto reader inside `warren` and call `uuid.NewV7FromReader(r)` (exported by google/uuid v1.6.0) in `applyDefaults`,
so the allocation win is kept without mutating a shared global. Adjust the PC-09 `AllocsPerRun` guard to target the
package-local path instead of `uuid.NewV7()`.

**Prerequisites:** none (self-contained; touches `message.go` + its alloc guard). Coordinates with T148 (the combined
hot-path `AllocsPerRun` guard) so both guards target the same generation path.

---

### LATER-52 — No end-to-end broker round-trip for explicit `DeliveryModeTransient` (wire 1)

**Context:** `publisher_batch_integration_test.go` now asserts that a zero-value (persistent-default) message arrives
with wire delivery-mode 2 against a real broker — the durable-by-default regression guard for the §6.5 mapping fix.
The symmetric case, an explicit `DeliveryModeTransient` publish arriving with wire delivery-mode 1, is proven only at
the `buildPublishing` unit level, never end-to-end.

**Impact:** Minor/Important-for-completeness. The transient→1 branch is fully unit-tested both directions
(`DeliveryMode.wire()`/`deliveryModeFromWire`), so the risk is low; the gap is asymmetric evidence — the less-common
branch has no broker-confirmed proof, and only the integration (Docker) lane can supply it.

**Evidence:** `/agent-skills:ship` parallel review of branch `worktree-phase-2-validation`, 2026-05-29
(test-engineer coverage gap #2, integration-lane).

**Suggested solution:** Add a small integration-tagged test (same file/helpers) that publishes one message with
`DeliveryMode: DeliveryModeTransient`, reads it back via `basic.get`, and asserts `d.DeliveryMode == 1`. Verifiable
only on the integration lane.

**Prerequisites:** none (self-contained integration test reusing the existing batch-integration harness).

---

### LATER-53 — Confirm-tracker memory bound and compaction threshold lack direct documentation/tests

**Context:** `internal/confirms/tracker.go` — the low-water-mark index (`order`/`head`, PC-11) is bounded by the
outstanding-publish window, but the reasoning is load-bearing and implicit: the bound holds because `head` is gated
by the lowest still-pending tag, which is itself bounded by in-flight publishes. Separately, `compactOrder`'s
"leave-alone" boundary (`head <= orderCompactMinPrefix`, `head*2 < len`) is exercised only incidentally by
`TestTracker_LongLived_OrderStaysBounded`, not pinned by a targeted boundary test.

**Impact:** Minor (hardening). The behaviour is correct and covered at 100% line coverage; the gaps are (a) a
one-sentence rationale on the `order` field comment explaining why the bound holds, and (b) a boundary test locking
`orderCompactMinPrefix` against accidental change. Without them a future refactor could silently weaken the bound.

**Evidence:** `/agent-skills:ship` parallel review of branch `worktree-phase-2-validation`, 2026-05-29
(code-reviewer suggestion #1 + test-engineer gap #3).

**Suggested solution:** Add one sentence to the `order` field godoc noting the head-gated bound, and a white-box
test asserting that at `head == orderCompactMinPrefix` the slice is not compacted while at `head ==
orderCompactMinPrefix + 1` with `head*2 >= len` it is.

**Prerequisites:** none (self-contained doc + test in `internal/confirms`).

---

<!-- LATER-54 resolved (branch worktree-phase-3-validation): broker-side arg assertion via the management API — TestTopology_Declare_quorumDLXStrategy_integration. -->
<!-- LATER-55 resolved (same branch): validate() rejects a raw x-queue-type arg, directing callers to the Type field. -->
<!-- LATER-56 resolved (same branch): FuzzTopologyExpand locks the validate()+expand() no-panic guarantee. -->

---

### LATER-57 — Adding `x-dead-letter-strategy=at-least-once` 406s on a pre-existing quorum+DLX queue (upgrade hazard)

**Context:** `topology.go` `expand()` now injects `x-dead-letter-strategy=at-least-once` on any quorum queue that has
a dead-letter exchange (SPEC §10 decision 52, added this session). RabbitMQ includes `x-dead-letter-strategy` in the
quorum-queue declaration-equivalence check. So a quorum+DLX queue that was first declared by a build *without* the
injection (the arg absent, broker default `at-most-once`) will be rejected when a build *with* the injection
re-declares it: `Declare` returns `ErrTopologyMismatch` and a reconnect redeclare drives the connection into the
degraded state (`ErrTopologyRedeclareFailed`).

**Impact:** None for a fresh deployment (the queue is created with the strategy from the first declare). For an
*upgrade* across the injection boundary against an already-declared quorum+DLX queue, the operator must delete and
recreate the queue (quorum-queue args are immutable) or set `x-dead-letter-strategy` via a policy before upgrading.
warren is pre-v0.1 (no released version), so there are no real upgrades yet — this is a forward-looking
changelog/migration note for the v0.1 release and for current dev users running against queues declared by an
earlier checkout.

**Evidence:** `/agent-skills:ship` review of branch `worktree-phase-3-validation`, 2026-05-29. Confirmed empirically
with a raw `amqp091` probe: redeclaring an existing quorum+DLX queue with the added strategy returned
`Exception (406) PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-strategy' ... received the value
'at-least-once' ... but current is none`.

**Suggested solution:** Record the breaking-on-upgrade behaviour in `CHANGELOG.md` (the file is mandated by SPEC
§4/§8 for pkg.go.dev but does not exist yet — bind this note to that file's creation) and add a short migration
paragraph: upgrading requires recreating pre-existing quorum+DLX queues (or applying the strategy via policy first).
Optionally, consider detecting this specific 406 and wrapping it with a more actionable error that names the
queue-recreation requirement.

**Prerequisites:** the `CHANGELOG.md` creation task (SPEC §4 L179 / §8 L2343 — currently missing) is the natural home
for the migration note; the optional error-wrapping enhancement is self-contained in `topology.go`/`internal/amqperror`.

---

### LATER-58 — `BatchConsumer.applyBatchCounterB` keeps the non-atomic counter-B RMW that was fixed in `Consumer.applyCounterB`

**Context:** T20/CR-02 (this session) made `Consumer[M].applyCounterB`'s in-process redelivery counter (counter B)
an atomic read-modify-write by adding a `sync.Mutex` to the shared `redeliveryCounter` type (`consumer.go`) and
holding it across the whole `load → check → store/delete`. `BatchConsumer[M].applyBatchCounterB`
(`batch_consumer.go:512-580`) implements the *same* counter B on the *same* `redeliveryCounter` type but still does a
bare `cs.load(key)` → `cs.m.Store(key, count+1)` (single-delivery fast path `:534→:544`; multi-delivery collect/increment
`:558→:577`) **without taking `cs.mu`**. It is not a live bug today: a single `BatchConsumer` dispatches batches
**sequentially** (`batch_consumer.go:138-139` — "run multiple `BatchConsumer[M]` instances for parallelism"), each
instance owns its own per-channel `redeliveryCounter`, and instances do not share that map — so only one goroutine
ever touches a given counter. The hazard is **latent + a maintenance divergence**.

**Impact:**
- The two counter-B implementations now diverge: one atomic, one not, on a type that *carries* a mutex the batch path
  silently ignores. A future reader can reasonably assume both paths are protected and they are not.
- If `BatchConsumer` ever gains intra-instance concurrent batch dispatch (a `Concurrency(n)`-style option), or the
  per-channel counter is ever shared across goroutines, the exact CR-02 lost-update reappears — `MaxRedeliveries`
  undercounts and a poison batch loops past its limit, and `go test -race` will *not* catch it (memory-safe `sync.Map`).
- Secondary, pre-existing and sequential (not a race): a single multi-delivery batch that contains two deliveries
  sharing one `MessageId` (at-least-once duplicates inside one batch window) reads `count` for both before either
  `Store`, so the two collapse to a single increment — counter B undercounts by one per duplicate pair. Minor; worth a
  decision (de-dup keys within a batch, or document as accepted).

**Evidence:** Phase 4 validation, 2026-05-29 (user flagged the noted-but-not-fixed batch RMW during `/agent-skills:build`).
The `Consumer` lost-update was proven by `TestConsumer_MaxRedeliveries_CounterB_AtomicUnderConcurrency` (500 goroutines,
~50/500 increments survived pre-fix). The batch path was left untouched as out-of-Phase-4 scope.

**Suggested solution:** Factor the atomic RMW into a method on `redeliveryCounter` (e.g.
`incrementIfWithin(key string, limit int) (count int64, allowed bool)` that locks `mu` across load→check→store/delete,
and a `delete(key)` that locks `mu`), then have both `Consumer.applyCounterB` and `BatchConsumer.applyBatchCounterB`
call it — eliminating the duplication and the divergence in one move. For the multi-delivery batch, hold `mu` once
across the whole collect-then-increment pass so the batch verdict is computed atomically. Add a behavioural
N-goroutine-same-key test for the batch path mirroring the `Consumer` one (only meaningful once concurrent batch
dispatch exists; until then it guards the shared helper). Decide and document the within-batch duplicate-`MessageId`
collapse.

**Prerequisites:** none to refactor the shared helper now (pure internal cleanup, behaviour-preserving for the current
sequential dispatch). The behavioural concurrency guard only becomes load-bearing if/when a concurrent batch-dispatch
option is added. Aligns with the Phase 19 counter-B atomicity scope (T143/CG-1, T20 extension) — fold in there if that
phase is tackled first.

---

### LATER-64 — Reconcile the Phase 9 CI Go-matrix planning docs with the new 1.25 baseline

**Context:** Resolving LATER-63 raised warren's minimum Go from 1.23 to 1.25 and amended the *living*
contract accordingly (SPEC §2 Tech Stack, §9 success criterion, §10 decision 6, README, GEMINI,
CLAUDE). The *planning* docs for the not-yet-implemented Phase 9 CI work still describe the old
`Go matrix (1.23, 1.24)`: `tasks/plan.md` (T42, T150, T151 and their VG-1..VG-6 gate references),
`tasks/todo.md` (the T42 checkbox and the VG-1/VG-3 ground-truth rows), and the dated lens brief
`docs/spec-validation/10-test-strategy-verifiability-plan.md` (TV-07 "Go 1.24 not in the CI matrix",
plus several "Go 1.23 only" CI-reality observations).

**Impact:** Low and bounded. The dated `docs/spec-validation/*.md` briefs are **historical review
records** — at review time (2026-05-28/29) CI genuinely ran 1.23 only, so rewriting them would
falsify the record; they should stay as-is. The `tasks/plan.md`/`tasks/todo.md` matrix references
are forward-looking specs for unstarted work, so the only real risk is that whoever implements
Phase 9 (T42/T150/T151) builds a 1.23/1.24 matrix that contradicts the 1.25 floor this PR set. No
runtime or contract impact today (Phase 9 CI matrix does not exist yet).

**Evidence:** Surfaced while resolving LATER-63 (2026-05-29). Deferred to keep that PR's blast
radius on the *contract* (SPEC/README/CI build version) rather than silently rewriting large,
cross-referenced planning blocks and dated historical briefs.

**Suggested solution:** When Phase 9 (T42/T150/T151) is picked up, update the matrix floor in
`tasks/plan.md` + `tasks/todo.md` from `1.23/1.24` to the then-current Go-team-supported minors
(1.25/1.26 today), and resolve the TV-07 finding against the real matrix. Leave the dated
`docs/spec-validation/10-*.md` brief intact as a historical artifact (or add a dated addendum noting the
floor moved), rather than rewriting its review-time observations.

**Prerequisites:** Coordinates with T42/T150/T151 (Phase 9 CI matrix + verification gates). No
dependency for the doc reconciliation itself.

---

<!-- LATER-63 resolved (LATER-63 — Go 1.25 baseline + govulncheck gate): bumped golang.org/x/sys
     v0.35.0 → v0.44.0 (the only version carrying the GO-2026-5024 fix), which raised the module's
     go directive 1.23.0 → 1.25.0; govulncheck ./... now reports "No vulnerabilities found." The
     1.25 floor cascaded to .github/workflows/ci.yml (go-version 1.23 → 1.25), SPEC §2/§9/§10 dec 6
     (matrix floor 1.23 → 1.25; "currently 1.25 and 1.26"), README.md, GEMINI.md, CLAUDE.md. Added a
     pinned `make vuln` target (govulncheck@v1.3.0, GOVULNCHECK_VERSION-overridable) and a
     "Vulnerability scan" CI job that runs it — anticipating the Phase 9 T38 required-gates wiring.
     The gate fails only on a vulnerability warren's code actually calls (default symbol scan); an
     uncalled/transitive advisory is reported but does not break the build. Phase 9 planning-doc
     reconciliation (T42/T150/T151 still naming the 1.23/1.24 matrix) is tracked as LATER-64. -->

---

<!-- LATER-59 resolved: commit 810adb1 (Phase 6 validation) — finishPublishSpan now redacts the
     codec-encode / client-validation class (errors.Is(err, ErrInvalidMessage)) to the sentinel label
     on both the span status description and the recorded error, via the shared redactedSpanError
     adapter, while errors.Is backends still unwrap to the sentinel. Broker/framework diagnostics stay
     verbatim. SPEC §8 leakage guarantee is now uniform across the publish and consume span paths.
     Pinned by TestFinishPublishSpan_redactsCodecClassKeepsBrokerVerbatim. -->

---

<!-- LATER-60 resolved: commit aab9b1b (Phase 6 validation) — ceBinaryCodec.DecodeWithHeaders now
     caps the number of cloudEvents:-prefixed extensions it reconstructs at maxCEBinaryExtensions (128)
     and rejects the delivery with ErrInvalidMessage past the cap, mirroring maxHeaderDepth. Pinned by
     TestCloudEventsBinary_DecodeWithHeaders_RejectsTooManyExtensions / _AcceptsExtensionsAtCap. -->

<!-- LATER-61 resolved: commit aab9b1b (Phase 6 validation) — ceStructuredCodec.Encode delegates the
     json.Marshal call to a per-instance injected marshaler (the marshal field, set to json.Marshal by
     NewCloudEventsStructured) so a test can inject a synthetic failure and cover the otherwise-
     unreachable ErrInvalidMessage wrap, with no mutable package global. cloudevents.go Encode is now
     at 100% statement coverage. Pinned by
     TestCloudEventsStructured_Encode_MarshalFailureWrapsErrInvalidMessage. -->

---

<!-- LATER-62 resolved: commit aa3af0c (Phase 6 validation) — startBatchSpan now skips deliveries
     whose producer context is invalid (Extract → context.Background()) via propagator.ActiveContext,
     so a LinkingTracer adapter only ever receives Links with a valid producer span context. Pinned by
     TestBatchConsumer_processBatchSpan_linksOnlyValidProducerContext (and the all-bare-deliveries edge
     TestBatchConsumer_processBatchSpan_allBareDeliveries_noLinks). -->

---

<!-- LATER-65 resolved (option b): test/Dockerfile.rabbitmq-delayed bakes the
     rabbitmq_delayed_message_exchange plugin (pinned by SHA-256) into a
     rabbitmq:3.13-management broker; test/docker-compose.integration.yml builds it and the
     ci.yml integration job provisions it via `make integration-up`, so
     TestDelay_DelayedDelivery_integration and the examples/delayed smoke test now run
     the 2s–2.5s window assertion on every integration lane instead of skipping. The
     probe (requireDelayedExchange) now fails fast with the reason when the broker lacks
     the plugin (was t.Skip), since the lane provisions it and a plugin-less broker is a
     misconfiguration — consistent with the missing-AMQP_TEST_URL rule. Option (a) — folding
     the duplicated requireDelayedExchange helper into amqptest.RequireDelayedExchange —
     stays deferred to T37 and is tracked by the inline TODO in both test files; it is a
     code-dedup nicety, not a coverage gap, now that (b) makes the assertion run. -->

---

### LATER-66 — `requireDelayedExchange` probe blames every declare error on the missing plugin and prints the raw broker URL

**Context:** `delay_integration_test.go:42-49` and `examples/delayed/example_integration_test.go:47-54` — the `requireDelayedExchange` helper (duplicated in both files, behind `//go:build integration`) probes for the `rabbitmq_delayed_message_exchange` plugin by declaring a throwaway `x-delayed-message` exchange. It treats *any* non-nil `ExchangeDeclare` error as "plugin unavailable" and now (since the Skip→Fatal flip) `t.Fatalf`s with that diagnosis. The genuine plugin-absent signal is a specific AMQP reply code (observed: `406 PRECONDITION_FAILED - unknown exchange type 'x-delayed-message'`); the helper never inspects `*amqp091.Error.Code`. The same `t.Fatalf` interpolates the raw `url` (`amqp://guest:guest@localhost:5672/`), printing userinfo to test output — inconsistent with the `internal/redact` choke-point (SPEC §8) and with `topology_integration_test.go`, which prints scheme+host only.

**Impact:** A non-plugin channel error (access-refused under a restrictive vhost, a precondition failure, a name collision) would fail with a misleading "enable the plugin / `make integration-up`" remediation that does not match the real cause. Credentials in the broker URL surface in CI failure logs (currently only the well-known `guest:guest` default, so no real secret — but the pattern violates the always-redact invariant).

**Evidence:** `/ship` review — code-reviewer (Suggestion ×2), security-auditor (LOW), test-engineer (Medium); all three converge on folding the fix into the planned T37 `amqptest.RequireDelayedExchange(t)` consolidation. Note: the reviewers assumed reply code `503 COMMAND_INVALID`, but the actual RED evidence is `406 PRECONDITION_FAILED` — which is exactly why hard-coding a single literal code is fragile and the classification belongs in `internal/amqperror`.

**Suggested solution:** When the helper consolidates into `amqptest.RequireDelayedExchange(t)` (T37): (a) discriminate plugin-absence by routing `derr` through `internal/amqperror`/`AMQPCode` (via `errors.As`, per the `errorlint` contract) and emit the plugin-specific remediation only for the unknown-exchange-type reply code, failing other errors with a generic "probe failed" message; (b) wrap `url` (and the error) through `internal/redact` so no userinfo reaches test output. Both land in one place once the duplication is removed.

**Prerequisites:** T37 (`amqptest/` package + `amqptest.RequireDelayedExchange(t)` consolidation). Do not hand-roll reply-code discrimination inline before then — the choke-point belongs in the shared helper.

---

### LATER-67 — Delayed-delivery timing assertion's 2.5s upper bound may flake under CI load

**Context:** `delay_integration_test.go:104-107` — `TestDelay_DelayedDelivery_integration` publishes a 2s-delayed message and asserts `elapsed >= 2s && elapsed < 2.5s`. The `>= 2s` lower bound is robust (the broker cannot deliver early). The `< 2.5s` upper bound is a 500ms budget for broker scheduling + network + consumer dispatch + goroutine wakeup. The Skip→Fatal flip (and LATER-65 before it) is what makes this assertion actually execute on CI runners for the first time.

**Impact:** On a loaded/shared CI runner the 500ms ceiling could be exceeded by scheduling jitter, producing a flaky FAIL on a correct implementation. The 10s overall arrival timeout is a separate, safe net and is unaffected.

**Evidence:** `/ship` review — test-engineer (Medium). Pre-existing assertion; this change activates the risk by making it run in CI for the first time.

**Suggested solution:** Only if a flake is actually observed (do not pre-emptively weaken a sharp assertion): widen the upper bound (e.g. `< 3s`) or read a CI-tunable budget from an env var, keeping the `>= 2s` lower bound strict. Prefer the env-tunable form so local runs keep the tight window and only CI relaxes it.

**Prerequisites:** None. Independent of T37; act on the first observed flake.

---

### LATER-69 — `internal/amqptest` testcontainers tests contend with the compose-broker lane on small CI runners

**Context:** `internal/amqptest.NewRabbitMQ` spins a fresh RabbitMQ container per test. Running those `//go:build integration` tests in the same `go test ./...` invocation as the root/example integration tests (which hammer the docker-compose broker) means two broker workloads share one host. T37 made the helper robust to slow boots — `brokerStartupTimeout` is 180s (the module default is a hard 60s cap, applied at both the `WithWaitStrategy` deadline and the inner `ForLog` startup timeout) and termination is registered before `require.NoError` so a failed start never leaks a container — but the underlying contention remains.

**Impact:** On an undersized/loaded CI runner the broker boot can still exceed 180s and fail the testcontainers tests; co-locating both broker styles also inflates wall-clock. Verified locally only because the author's 3.9 GB Docker VM, degraded by repeated runs, pushed broker startup to ~140s (the 180s headroom absorbed it).

**Evidence:** T37 refactor — the full `-race` integration lane failed only on `internal/amqptest` (broker startup > 60s) under memory pressure; root/example lanes were green. Root cause was the 60s wait cap + leaked containers (Ryuk disabled), both fixed; the resource co-location is the residual.

**Suggested solution:** In T42/T151 (CI wiring) run the testcontainers-based tests in a **separate job** from the compose-broker integration lane (or size the runner accordingly), and keep Ryuk **enabled** in CI so failed containers are reaped. Consider an env-tunable `brokerStartupTimeout` if a real runner ever needs more than 180s.

**Prerequisites:** T42 / T151 (CI lane wiring).

---

### LATER-70 — Benchmark relative-regression gate is inert until a reference baseline is committed

**Context:** `.github/workflows/bench.yml` (T44b) implements the Lens-10 TV-04 relative-regression gate as a `benchstat testdata/bench-baseline.txt bench.txt` comparison, but the comparison is guarded by `if [ -f testdata/bench-baseline.txt ]` and no baseline file is committed yet. The bench suite (`bench_*_test.go`, `//go:build bench`) currently MEASURES and REPORTS msg/s per classic+quorum queue; the absolute §9 targets (>=30k single-conn, >=100k multi-conn with 4 conns + pool 16, batch >=5x) are documented in the benchmark godoc as release-tag targets on reference hardware, not asserted, because shared-runner numbers cannot gate.

**Impact:** Until a baseline exists, a throughput regression between releases is visible only by manually reading the job summary / artifact — nothing fails CI. The gate is scaffolding, not yet enforcing.

**Evidence:** T44b implementation. TV-04 asks for a CI-gradeable relative gate; the mechanism is wired but the baseline (the thing it compares against) is deferred because it must be captured on the named reference runner, not an author laptop.

**Suggested solution:** Capture `testdata/bench-baseline.txt` on the stated reference runner class (e.g. the nightly `ubuntu-latest` bench job, `-count>=6`), commit it, and add a threshold step that fails when benchstat reports a regression beyond X% (e.g. p<0.05 and delta > +10%) on the headline `msg/s` metric. Refresh the baseline deliberately on each release tag, like any other pinned artifact.

**Prerequisites:** T44b (this suite) + one green run of `.github/workflows/bench.yml` on the reference runner to source the baseline numbers. Capstone classification is T152.

---

### LATER-71 — Cancelling the `Consume` context does not deregister the consumer at the broker

**Context:** `consumer.go` `runConsume` — on `ctx.Done()` (the case in the main select and the channel-closed `!ok` branch) the loop just does `wg.Wait(); return nil`. It does NOT send `basic.cancel` and does NOT close the AMQP channel obtained from `openDeliveryCh`/`mc.openChannel()`. `Consumer.Close()` only closes `closedCh` (a flag that makes `Delivery.Ack/Nack` refuse) — it likewise does not `basic.cancel` or close the channel. The consumer's broker-side `basic.consume` registration therefore lives until the whole `Connection` is closed (`Connection.Close` cancels the supervisors / drops the TCP sockets). Verified against a real broker via the management API: after a cancelled `Consume`, the queue still reports the consumer as `active=true` holding its prefetched-but-unacked message.

**Impact:** Two surprising consequences for callers who cancel a single consumer's context expecting a graceful per-consumer shutdown: (1) any message in that consumer's prefetch window that was delivered but not yet acked when the context was cancelled is **stranded unacked** (not requeued) until the connection closes — it is neither processed nor redelivered; (2) `SingleActiveConsumer` failover cannot be driven by cancelling the active consumer's context, because the broker still sees it as the active consumer and never promotes a standby. This bit `examples/ordered_consume` (T38c): the example "killed" the active consumer by cancelling its context, the standby was never promoted, the last in-flight message hung, and the integration test flaked/failed — fixed by reworking the example to close the active consumer's **connection** instead (the only current way to deregister at the broker).

**Evidence:** Phase 9 CI debugging (PR #20, `TestOrderedConsumeExample_integration` failure, 2026-05-30). Root-caused with the broker management API showing `active=true` + 1 unacked after a context-cancel. The example became a real failover demonstration only after switching to connection-close.

**Suggested solution:** Decide and document the intended contract, then make code + godoc agree. Either (a) on `ctx` cancel (or `Consumer.Close`), send `basic.cancel` and close the consumer's AMQP channel so the broker requeues in-flight messages and promotes a SAC standby — i.e. context cancel becomes a true graceful per-consumer shutdown; or (b) keep the current behavior but state explicitly in the `Consume`/`Close` godoc that broker-side deregistration happens only at `Connection.Close`, and that per-consumer SAC failover requires a dedicated connection per consumer. Option (a) is the least-surprising and removes the stranded-unacked window; it needs a unit test (channel-close/`basic.cancel` issued on cancel) plus an integration test asserting the standby is promoted by a per-consumer cancel.

**Prerequisites:** Formalize as a task in `tasks/plan.md` + `tasks/todo.md` before implementing (it changes the consumer lifecycle contract and likely touches SPEC §6.3). Coordinates with the SPEC §6.3 ordering/failover wording and the `examples/ordered_consume` workaround already in place.

---

### LATER-72 — `redact.Error` retains the un-redacted cause via `Unwrap`; a chain-walking surface would bypass redaction

**Context:** `internal/redact/redact.go:40-53` — `redact.Error(err)` returns `&redactedError{msg: URI(err.Error()), cause: err}`. `Error()` returns the redacted string, but `Unwrap()` returns the **original, un-redacted** `cause` (deliberately, so `errors.Is`/`errors.As` keep working). Every production surface today formats errors through `.Error()` (logs via the redacting Slog adapter, returned error strings), so the redacted message is what is emitted — confirmed by `TestSecurityRedaction_NoCredentialLeak_integration`, which scans the top-level `err.Error()` of the forced connect-failure. The residual is that a *future* surface that walks the chain — `fmt.Sprintf("%+v", err)` on a verbose-formatting wrapper, an error reporter that recurses through `errors.Unwrap` and prints each cause, or a structured logger that serializes the cause tree — would reach the retained un-redacted cause and re-leak the URI the redactor stripped.

**Impact:** Defense-in-depth gap, not a live leak. No current code path formats the unwrapped cause, and the security scan proves the `.Error()` path is clean. But the un-redacted credential is retained in-memory inside the error value and is one chain-walking formatter away from an observable surface, so the redaction guarantee is "true for `.Error()`", not "true for the whole error value".

**Evidence:** Phase 9 `/review` (2026-05-31) — security axis Suggestion. The T45b security scan reads only the top-level `err.Error()`; the reviewer noted the `Unwrap`-reachable cause is outside that scan's reach.

**Suggested solution:** Either (a) make the scan/exercise also assert on the unwrapped chain (`for c := err; c != nil; c = errors.Unwrap(c) { scan(c.Error()) }`) so a regression in any cause's redaction is caught — cheap, and tightens the existing test; or (b) change `redactedError` to also wrap the cause in a redacting shim (so `Unwrap().Error()` is redacted too) while keeping a separate typed-target field for `errors.As`. Prefer (a) for v0.1 (the production paths use `.Error()`, so the in-memory cause is a latent risk, not an active one); revisit (b) if any surface starts formatting cause chains.

**Prerequisites:** None. Standalone; coordinates with `internal/redact` and the T45b security scan.

---

### LATER-73 — Root package coverage sits ~0.4pt above the 80% floor (gate-flap risk)

**Context:** `internal/cmd/covercheck` enforces per-package ≥80% and critical-path ≥95%. The root `warren` package currently measures ~80.4% — a ~0.4-percentage-point margin above the default floor. Any root-package edit that adds a few uncovered statements (a new error branch, an option, a guard) can tip it under 80% and fail the coverage gate on an otherwise-correct change.

**Impact:** Low but recurring friction: the gate can flap red on small, correct additions to root-package files, forcing an unrelated coverage top-up in the same PR. It is a thin-margin maintainability issue, not a correctness gap.

**Evidence:** Phase 9 `/review` (2026-05-31) — coverage axis Suggestion.

**Suggested solution:** Add targeted unit tests to the lowest-covered root-package files (identify them via the per-file table `go tool cover -func=coverage.out | sort -k3 -n`) to lift the root package to a comfortable ~85%+ headroom, so routine edits do not graze the floor. Do not raise the floor itself — keep 80% as the contract; buy margin with tests.

**Prerequisites:** None. Standalone test-coverage top-up.

---

### LATER-74 — No real-broker conformance assertion for `ChannelQoS()` → `basic.qos global=true`

**Context:** T44 names `basic.qos.global=true for ChannelQoS()` as a conformance target. AMQP 0-9-1 does not echo the `global` flag back on the wire, so the only broker-side observation is the RabbitMQ management API's per-channel `global_prefetch_count`. This was attempted as `TestConformance_ChannelQoS_GlobalPrefetchAtChannelScope` and validated against the conformance image (`rabbitmq:3.13-management`, `rates_mode=basic`) on 2026-05-31; it proved too unreliable to gate a required CI lane: (1) `/api/channels` lags ~7s to even list a live channel; (2) the channel's `connection_details.name` is the peer form (`host:port -> host:port`), not the warren connection name — that name lives only in `/api/connections` under `client_properties.connection_name` (`warren-conf-qos-con-0`), so a channel must be matched by cross-referencing two endpoints; and (3) worst, the consumer's channel reported `consumer_count=0` and `global_prefetch_count=0` even while actively consuming with `Prefetch(137)+ChannelQoS()`. The behavior IS unit-covered (`consumer.go` issues `ch.Qos(int(c.prefetch), 0, c.channelQoS)` with `c.channelQoS=true`), so this is a conformance-assertion gap, not a behavior gap.

**Impact:** One T44 named conformance target lacks a real-broker assertion. Low: the `global=true` wiring is unit-tested and the management-API path is flaky rather than wrong; a regression in the `global` argument would be caught by the unit test, just not pinned end-to-end against a broker.

**Evidence:** Phase 9 `/review` I4 (2026-05-31) + live-broker validation of the would-be conformance test the same day (the data above).

**Suggested solution:** Pick one: (a) match the consumer channel reliably by first reading `/api/connections` (filter `client_properties.connection_name` by the warren connection-name prefix → connection `name`), then `/api/channels` filtered by that `connection_details.name`, AND give the stats DB a generous poll window (e.g. 30s) — accepting some CI-timing fragility (cf. LATER-67); optionally raise the conformance broker to `rates_mode=detailed` so channel QoS surfaces faster; (b) assert via a thin TCP proxy that captures the `basic.qos` frame's `global` bit on the wire — most reliable, most work, and reusable for other wire-level conformance checks; (c) accept the unit-level coverage as sufficient for v0.1 and formally drop the conformance target from T44. Prefer (a) only if the flakiness can be bounded; otherwise (b) for a real wire assertion, or (c) to close the target honestly.

**Prerequisites:** T44 conformance harness; the management-API helper pattern in `topology_integration_test.go` (for option a) or a new wire-proxy fixture (for option b).

---

### LATER-75 — Conformance suite lacks nack-requeue/`Redelivered` and basic SAC-exclusivity assertions

**Context:** `conformance/conformance_test.go` proves NO_ROUTE 312, broker-nack on overflow, broker-initiated cancel, the quorum `x-delivery-limit` bound, and the 406 user-id mismatch. Two headline contracts have no real-broker assertion: (1) warren's "nack-without-requeue is the default; requeue opt-in via `ErrRequeue`" — the positive case (a single `ErrRequeue` redelivers the same message with `Delivery.Redelivered()==true`, and a plain handler error does NOT requeue) is emergent broker behavior, exactly what a conformance test should pin; the quorum test only uses `ErrRequeue` as an infinite-poison driver. (2) SingleActiveConsumer exclusivity (`x-single-active-consumer`) is shipped (`topology.go`) and sold by `examples/ordered_consume`, but no conformance test asserts that with two consumers exactly one is active and the second takes over when the active one closes.

**Impact:** The two most-claimed delivery contracts have the weakest real-broker proof. A regression in the nack-requeue verdict mapping or the SAC arg injection would not be caught by the conformance lane (unit tests cover the wiring, not the broker-observed behavior).

**Evidence:** Phase 9 `/ship` test-engineer (S2, 2026-05-31) — Important.

**Suggested solution:** Add `TestConformance_NackRequeue_RedeliveredFlag` (classic queue: one `ErrRequeue` → same MessageID redelivered with `Redelivered()==true`; a plain error → not requeued / DLX'd) and `TestConformance_SingleActiveConsumer_Exclusivity` (declare a SAC queue, start two `ConsumeRaw` consumers, assert only one receives; close it, assert the second takes over). Both are deterministic broker behaviors (no timing flake like LATER-74).

**Prerequisites:** T44 conformance harness.

---

### LATER-76 — Example pure-logic helpers and otel header-propagation lack unit-level assertions

**Context:** `examples/idempotent_consume/main.go` (`seenCache` LRU+TTL eviction, ~lines 285-314) and `examples/ordered_consume/main.go` (`orderTracker` contiguity/dedupe verdict, ~lines 362-401) are pure, I/O-free logic that is the load-bearing assertion of each example, yet they are exercised only through the `integration`-tagged subprocess — the eviction and out-of-order branches never run in a passing happy path. Separately, `examples/otel` proves the consume span is a child of the publish span, but because publisher and consumer share one in-process TracerProvider the assertion would also pass if the parent leaked through Go memory rather than the W3C `traceparent` AMQP header the example claims to demonstrate.

**Impact:** A regression that broke cache eviction, flipped the TTL comparison, weakened the order detector, or broke `traceparent` injection/extraction could ship green. These are teaching examples, so a subtly-wrong helper teaches a wrong pattern.

**Evidence:** Phase 9 `/ship` test-engineer (S1, 2026-05-31) — Important.

**Suggested solution:** Add plain (un-tagged) `_test.go` unit tests next to each example: table-driven cases for `seenCache` (capacity-1 eviction returns the evicted id as new; sub-ms TTL returns false then true after expiry) and `orderTracker` (in-order / duplicate / gap / regression / short slice). For otel, have the handler assert the delivery carries a `traceparent` header whose trace-id equals the publish span's (e.g. via `ConsumeRaw`), proving on-the-wire propagation rather than in-process linkage.

**Prerequisites:** None (example-local tests).

---

### LATER-77 — Security redaction scan: walk the error chain, exercise the std logger adapter, cover encoded-secret shapes

**Context:** `security_redaction_integration_test.go` / `security_redaction_scan_test.go` capture `err.Error()` (top-level, already redacted) but never walk `errors.Unwrap` (the un-redacted cause LATER-72 documents); drive logs only through the Slog adapter, leaving the equally-public `log/std.go` adapter unexercised; and match secrets by exact plaintext substring + an `amqps?://…@` regex, so a URL-encoded or transport-encoded sentinel, or a credential after a rewritten scheme, would slip past. The redaction itself is sound on every production path (traced caller-by-caller during the Phase 9 audit); these are coverage gaps in the regression suite, not live leaks.

**Impact:** A future surface that formats the error chain (`%+v`, a recursive reporter), a redaction regression in `log/std.go`, or a leak in an encoded shape would not be caught by the project's credential-leak gate.

**Evidence:** Phase 9 `/ship` security-auditor (S3, 2026-05-31) — Medium (×3); relates to LATER-72.

**Suggested solution:** (a) Capture the full chain in the scan (`for c := err; c != nil; c = errors.Unwrap(c) { record(c.Error()) }`); (b) parametrize the redaction test over both `log.NewSlog` and `log.NewStd`, or add a fast unit test asserting each adapter level redacts a URI; (c) add `url.QueryEscape`/`url.PathEscape`/base64 variants of the sentinel to the `secrets` set and broaden the scanner regex to scheme-agnostic `://([^@/?#]*:[^@/?#]*)@`.

**Prerequisites:** T45b security-scan harness; LATER-72 for the chain-walk redaction shim (if chosen instead of scanning the chain).

---

### LATER-78 — Coverage gate: a package with no test files is invisible to the floor; tool binaries are version- not hash-pinned

**Context:** `scripts/coverage.sh` computes coverage over `go list ./... | grep -vE …` and floors only packages that emit profile blocks — a package that compiles but has no `_test.go` produces no entry and silently escapes the 80% floor (the new critical-package fail-closed check in `internal/cmd/covercheck` only covers the four declared choke-points). Separately, `golangci-lint` (`version: v2.12` minor track), `govulncheck` (`GOVULNCHECK_VERSION`), and `benchstat` (`go install @<pseudo-version>`) are resolved at run time through the checksum DB rather than pinned by exact patch/hash like the SHA-pinned actions.

**Impact:** A new non-critical package can ship with zero tests and the coverage job stays green; a broadened tool tag could silently change the enforced ruleset / advisory DB. Both are gate-integrity weaknesses, not live vulnerabilities (the tools are reputable first-party and the jobs are least-privilege/read-only).

**Evidence:** Phase 9 `/ship` security-auditor (S4, 2026-05-31) — Medium + Low.

**Suggested solution:** Cross-check the profiled package set against `go list` (fail if an expected package is absent from the profile), or pass `-coverpkg=./…` so untested packages surface as 0% rows and trip the floor; document the permitted exclusion list (`examples`, `scripts`, `internal/amqptest`). Pin `golangci-lint`/`govulncheck` to exact `vX.Y.Z` and add `benchstat` as a `go.mod` tool dependency so its hash lands in `go.sum`.

**Prerequisites:** T41/T42 coverage gate; T43 release/bench workflows.

---

### LATER-79 — `main` has no branch protection, so the CI gates run but do not block a merge

**Context:** Phase 9 wired the CI gates (`Unit tests (Go 1.25)` / `(Go 1.26)`, `Coverage gate`, `Integration tests`, `Vulnerability scan`) into `.github/workflows/ci.yml`. Verified on 2026-05-31 that `main` currently has NO branch-protection rule (`GET repos/brunomvsouza/warren/branches/main/protection` → 404 "Branch not protected"), so there is no required-status-checks list at all. Note also that the Go-matrix addition is purely additive: the unit job's name already interpolated the matrix value (`Unit tests (Go ${{ matrix.go }})`), so `1.25` was never the bare `Unit tests` — adding `1.26` only introduces a new `Unit tests (Go 1.26)` check and renames nothing. The original /ship finding (a stale `Unit tests` required-check name) therefore does not apply.

**Impact:** The gates are advisory until protection is enabled: a red CI run does not block a merge to `main`, and a direct push to `main` bypasses them entirely. For a solo maintainer this may be a deliberate workflow choice, not a defect — recorded so the gating posture is explicit rather than assumed.

**Evidence:** Phase 9 `/ship` code-reviewer (S4, 2026-05-31), corrected by a live `gh api` check the same day.

**Suggested solution:** If/when the gates should actually block, enable branch protection on `main` with these required status checks: `Unit tests (Go 1.25)`, `Unit tests (Go 1.26)`, `Coverage gate`, `Integration tests`, `Vulnerability scan`. Until then no action is needed — there is no stale required-check name to fix.

**Prerequisites:** Repo admin access (a settings change, not a Txx). No code dependency.

