# Plan Input — Remediate Event-Driven-Architecture Findings (Lens 04)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> event-driven-architecture lens
> (`spec-validation/04-event-driven-architecture.md`). Like Lens 03, no findings
> report pre-existed — this brief was produced by *conducting* the review (tracing
> each canonical EDA pattern — pub/sub fan-out, competing-consumer work queue,
> request/reply, retry ladder, DLQ + redrive, ordered keyed stream, idempotent
> consume, batch consume, event versioning, event mesh — against "can a user
> express it cleanly, is the safe variant the default, is the failure mode
> observable?"). It enumerates confirmed defects (`EDA-01..EDA-18`), their
> evidence, the desired outcome, and an acceptance test for each, grouped into
> workstreams and sequenced by dependency.
>
> **Numbering:** new task IDs are **T101–T110** (highest existing is T100, after
> Phase 14). Lens 04 becomes **Phase 15**, mirroring how Lens 01/02/03 became
> Phases 12/13/14. Two Phase-11 tasks are **pulled forward** (T68, T69). One
> existing task is **extended** (T85). One `LATER.md` entry is added: **LATER-40**.
> Five findings are **already covered by prior-lens tasks** and are *not* re-filed
> (project rule: cross-lens shared findings extend the shared task).

---

## 1. Objective

Validate `SPEC.md` not from the API surface in isolation but from **the
architectures users will build on it** — at the §1 "billions of messages per day"
bar, which for an EDA platform means: every canonical pattern must be expressible
*cleanly*, the *safe* variant must be the *default*, and every failure mode must
be *observable*. The lens north star: a library that makes the right patterns hard
or the wrong patterns easy corrupts every system built on it.

Concretely, the plan must:
1. Close the **ordering-vs-scale forced choice** (EDA-04): v0.1 offers ordered work
   only via `SingleActiveConsumer + Concurrency(1)` (one active consumer, no
   horizontal scale). Add the consistent-hash exchange so partitioned ordered keyed
   streams (ordering per key across N queues) are expressible.
2. Close the **platform-level unroutable black-hole** (EDA-01/EDA-02): without a
   server-side catch-all, the only safety net is opt-in per-publish
   `Mandatory()`+`OnReturn`, and a publish *without* `Mandatory()` silently drops a
   mis-routed event. Pull the alternate-exchange task forward and add publisher-side
   exchange-name validation.
3. Make the **safe retry pattern the easy one** (EDA-05/EDA-06): the lossy delayed
   exchange is the obvious tool for "retry with backoff"; the durable TTL+DLX ladder
   is prose-only, has no example, and its per-message-TTL head-of-line-blocking trap
   is undocumented.
4. Complete the **DLQ lifecycle** (EDA-09): "send to DLX" is half a strategy; ship a
   redrive helper and document the replay pattern (DLQ bounds + Consumer missing-DLX
   validation are already covered by T65).
5. State the **event-evolution boundary** (EDA-13/EDA-14): additive change is safe
   (Postel); breaking change (removal/rename/semantic) is user-owned via versioned
   type names; the cross-ecosystem structured-CloudEvents path is opaque to broker
   routing.
6. Defuse the **batch partial-failure footgun** (EDA-15): `return nil` acks the whole
   range (`multiple=true`) including an unprocessed/poison message.
7. Enable **layered fan-out** (EDA-03): exchange-to-exchange bindings (pulled forward).

This is **remediation of an existing spec**, not a new feature. **Lens verdict:
GO-WITH-CHANGES** — there is no active §1 message-loss bug introduced by this lens
that a prior lens did not already catch (the DLQ-unbounded and Close-loss gaps are
T65/T70, already pulled forward by Phases 12/13). What remains are pattern gaps
(ordering-at-scale, alternate exchange, e2e bindings, redrive), one default that
nudges toward the unsafe shape (delayed exchange for retries), one batch footgun,
and several unstated architectural boundaries.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing.** Sections referenced:
  - §6.1 connection/consumer pinning (`WithConsumerConnections` default 2,
    consumer-tag hash L538–549).
  - §6.2 publisher unroutable handling (`Mandatory()`/`OnReturn`,
    `basic.return`/`basic.ack` correlation L850–871).
  - §6.2.1 at-least-once / idempotency contract (UUIDv7 `MessageID` L1020–1024;
    canonical dedupe pattern L1032–1053; `examples/idempotent_consume/` reference
    L1067–1068).
  - §6.3 Consumer (Concurrency/Prefetch defaults L1161; ordering + SAC L1203–1225;
    two-counter poison design L1227–1308; DLX context preservation L1696–1706).
  - §6.4 BatchConsumer (auto-ack `multiple=true` L1403–1405; partial-outcome path
    L1410–1414; sequential-per-batch L1393–1397).
  - §6.5 Message — `Delay` durability warning (R10-1) L1536–1552; `Message.Type`
    convention L1441–1451.
  - §6.6 Topology (declare-once/deep-snapshot/redeclare L1668–1750; DeadLetter
    expansion L1642–1654; DLX context preservation L1696–1706; `ExchangeDelayed`
    L1587).
  - §6.7 RPC (Replier OnError / no error envelope L1829–1836; at-least-once replies
    L1813–1827; single-shot `Call` / CorrelationID demux L1763–1773; Replier DLX
    validation L1845–1857).
  - §6.9 codecs (lax JSON L1985–1992; CloudEvents structured L2006–2010 / binary
    L2011–2019).
  - §8 Postel's Law L2373–2378.
  - §10 decisions 16 (RPC error contract L2705–2711), 23 (lax JSON L2765–2778),
    52/53/54 (Rev 9 L2612–2623); Rev 10 deferrals R10-1 L3045–3053, R10-10/T65
    L3112–3119, R10-13/T68 L3127–3129, R10-14/T69 L3130–3132, R10-15/T70 L3133–3136;
    highest Rev note **Rev 10** L3031.
- Implementation state (read this pass): examples present are
  `batch_consume/`, `batch_publish/`, `consume/`, `deadletter/`, `publish/`,
  `topology/` — **absent**: `idempotent_consume/`, `retry_ladder/`, `cloudevents/`,
  `delayed/`, `rpc/`, `ordered_consume/`. `consumer.go` does **not** validate DLX
  presence on `MaxRedeliveries>0` (T65 adds it); `replier.go`/`rpc.go` not yet in
  this worktree (Phase 7).
- `tasks/plan.md` / `tasks/todo.md` — Phase 15 lands here (T101–T110, pull-forward
  T68/T69, extend T85). Highest existing task is **T100**.
- `LATER.md` — highest entry is **LATER-39**; this brief adds **LATER-40**.
- The validation prompt this brief derives from:
  `spec-validation/04-event-driven-architecture.md`.

### Cross-lens reconciliation (do **not** re-file — project rule)

Five EDA findings are already owned by prior-lens tasks; this brief records them
for traceability and routes them to the existing task:

| EDA finding | Already covered by | Phase |
|-------------|--------------------|-------|
| EDA-07 (DLQ unbounded → disk alarm) | **T65** (DLQ bounds) | 12 (pulled fwd) |
| EDA-08 (Consumer missing-DLX silent poison drop) | **T65** (Consumer DLX validation) | 12 (pulled fwd) |
| EDA-10 (RPC failure always looks like timeout) | **T91** (opt-in structured-error reply) | 13 |
| EDA-11 (duplicate reply to returned Call) | **T90** (orphan-reply handling) | 13 |
| EDA-16 (partial batch / undispatched on Close) | **T70** (Close nack-requeue) | 13 (pulled fwd) |
| EDA-18 (idempotent example absent) | **extend T85** (dedupe-pattern rework) | 13 |

---

## 3. Constraints & working agreements (planner must honour)

- **Reason from architectures, not the API surface.** Every finding is framed as a
  pattern a user will build; the acceptance test exercises the *pattern* on a live
  broker, not a unit of API in isolation.
- **The safe variant must be the default, or the unsafe one must be loudly flagged.**
  Where v0.1 makes the unsafe shape easy (delayed exchange for retries; `return nil`
  acking a poison in a batch), the fix is docs + an example of the safe shape +
  (where cheap) a warning — not silent reliance on the user reading deep SPEC prose.
- **A "deferred to Phase 11" gap is still a v0.1 gap.** Each carries the user-facing
  consequence. Two are pulled forward per owner decision (T68, T69); the rest are
  documented now with the consequence stated.
- **No new build-tag lane.** Unlike Lens 03's `interop` lane, Lens 04's gates ride
  the **existing integration lane** (3.13 + 4.x) and, where a crash is needed, the
  **existing `chaos` lane** (T84). Do not introduce a new tag.
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in the
  same PR (or earlier), then implementation/example follows.
- **`testify` `assert`/`require`** in every `_test.go`; **`goleak`** on any
  goroutine-spawning test (project rules).
- **Docs in English** (`CLAUDE.md`). Module path `github.com/brunomvsouza/warren`;
  `errorlint` is on.
- **Choke points:** topology in `topology.go`; consume/poison in `consumer.go` /
  `batch_consumer.go`; RPC in `rpc.go`; codec/event-format in `codec/`; error
  translation in `internal/amqperror`. Corrections land at the choke point.
- **Executable examples at checkpoints** (SPEC §7 + Rev decision 49): every new
  `examples/<feature>/` must build on every PR (`examples-build`) and smoke-run on
  the integration lane (`examples-smoke`). Lens 04 adds `retry_ladder/`,
  `redrive/`, and (via T85) `idempotent_consume/`.
- **README sync:** any change to the external contract (consistent-hash exchange,
  alternate exchange, e2e bindings, redrive helper, the retry-ladder and
  schema-evolution guidance) updates `README.md`'s "Available now" / roadmap split.

---

## 4. Pre-work: verification gates (EG-1..EG-4 — sequence FIRST; they gate wording)

These behaviours must be observed on a **live broker** (existing integration lane,
3.13 **and** 4.x) before the dependent amendments can be worded correctly. The gate
task (T101) captures them into a committed results table each downstream task cites.

| Gate | Question (ground truth to capture) | Blocks |
|------|-----------------------------------|--------|
| **EG-1** | Does a publish to a non-existent / mis-routed exchange **without** `Mandatory()` silently succeed (no error, no `OnReturn`, message gone)? Confirm `Mandatory()` is the *only* client-side net today. | EDA-01/EDA-02 / T103 (+ informs T68) |
| **EG-2** | On a single queue, does a short per-message-TTL message **behind** a long-TTL message fail to expire at its own TTL (head-of-line blocking), i.e. does the naive single-queue TTL ladder mis-behave? | EDA-06 / T104 |
| **EG-3** | Does a `BatchConsumer` handler returning `nil` emit one `basic.ack` with `multiple=true` over the **whole** delivery range — acking an in-batch message the handler never individually processed? | EDA-15 / T108 |
| **EG-4** | Does `SingleActiveConsumer` permit exactly **one** active consumer (standbys idle, no parallelism = no horizontal scale on that queue)? And does a consistent-hash exchange distribute by routing-key hash across N bound queues (the partitioned-ordering primitive)? | EDA-04 / T102 |

**Planner note:** EG-1/EG-2/EG-3 each reveal a *current* behaviour the spec must
name or a default that must change; EG-4 confirms both the ceiling of the existing
ordering story and the behaviour of the proposed consistent-hash primitive. No SPEC
edit to an affected section until the relevant gate returns.

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets. Findings already owned by a prior lens are in WS-0.

### WS-0 — Already covered by prior-lens tasks (record, do **not** re-file)

- **EDA-07 [High, deferred-gap]** — the auto-declared `<source>.dlq` has unspecified
  durability and no bounds (§6.6; R10-10 L3112–3119); an unbounded DLQ fills disk
  and triggers a **cluster-wide** disk alarm that blocks *all* publishers. *Owned by
  **T65*** (DLQ becomes durable/quorum-capable with configurable bounds), pulled
  forward into Phase 12. EDA endorses the priority — at billions/day an unbounded
  DLQ is a latent platform outage.
- **EDA-08 [High, deferred-gap]** — `Consumer` with `MaxRedeliveries>0` and no DLX
  drops poison **silently**, whereas `Replier` validates DLX presence and returns
  `ErrInvalidOptions` (§6.7 L1845–1857). *Owned by **T65*** (mirror the Replier
  validation on Consumer: warn at `Build` + drop metric), Phase 12.
- **EDA-10 [Med-High, design-disagreement]** — the Replier sends **no error
  envelope**; every handler failure reaches the caller as `ErrCallTimeout`
  (§6.7 L1829–1836; decision 16 L2705–2711), so a caller cannot tell "retry the
  transient" from "fail fast on the business error" and is forced into uniform blind
  retry (amplification). *Owned by **T91*** (opt-in structured-error RPC reply mode,
  DS-12), Phase 13.
- **EDA-11 [Med, underspecified]** — at-least-once replies (§6.7 L1813–1827) mean a
  replier crash after publish-confirm but before ack produces a **duplicate reply**;
  `Call` is single-shot, demultiplexed by `CorrelationID` (L1763–1773), but the
  contract for a late duplicate arriving after the original `Call` returned is
  implicit. *Owned by **T90*** (orphan-reply handling: discard + metric, UUIDv7
  CorrelationIDs never reused so no cross-talk, G6 chaos test), Phase 13.
- **EDA-16 [High, deferred-gap]** — §6.4 is **silent** on a `BatchConsumer`'s pending
  partial batch (and prefetched-but-undispatched deliveries) on `Close`; R10-15
  (L3133–3136) defers it. A dropped partial batch on deploy = silent loss. *Owned by
  **T70*** (nack-requeue the undispatched buffer + flush the partial batch on Close,
  DS-03), pulled forward into Phase 13.
- **EDA-18 [Med, missing-artifact]** — §6.2.1 (L1067–1068) references
  `examples/idempotent_consume/` as "a runnable reference for the pattern," but the
  directory does **not** exist. Idempotency is the linchpin of the at-least-once
  contract (CLAUDE.md invariant 3); the canonical example must ship. *Disposition:*
  **extend T85** (dedupe-pattern rework, DS-01/15/16) with an explicit acceptance
  criterion that the runnable `examples/idempotent_consume/` ships and matches the
  reworked pattern.

### WS-1 — The ordering-vs-scale forced choice *(the headline EDA gap)*

- **EDA-04 [High, design-disagreement]** — for ordered keyed streams at
  billions/day, v0.1 offers only `SingleActiveConsumer + Concurrency(1)`
  (§6.3 L1203–1225): in-order delivery on one queue with exactly one active consumer
  — **ordering at the cost of horizontal scale**. `Concurrency>1` explicitly
  "means you have given up per-queue ordering" (L1205–1210). The **consistent-hash
  exchange** (RabbitMQ plugin) — the canonical pattern for ordering *per key* across
  N queues consumed in parallel — is unmentioned anywhere in §6.6. *Location:*
  §6.3 L1203–1225; §6.6 (no `x-consistent-hash` `ExchangeKind`). *Outcome (owner
  decision):* **pull into scope** — add an `x-consistent-hash` `ExchangeKind`, the
  partitioned-consumer wiring, and a worked `examples/` of an ordered keyed stream
  that scales horizontally; SPEC §6.6 documents the per-key-ordering-across-N-queues
  pattern and its trade-offs (rebalancing on partition-count change). *Acceptance
  (EG-4):* an integration test shows N queues bound to an `x-consistent-hash`
  exchange, each consumed by one consumer, preserving per-key order while
  distributing across queues. *Disposition:* **new task T102**.

### WS-2 — Unroutable-message safety nets *(silent black-hole at the platform level)*

- **EDA-01 [High, design-disagreement / deferred-gap]** — there is no server-side
  catch-all for unroutable messages; the **only** net is per-publish
  `Mandatory()`+`OnReturn` (§6.2 L850–871), a publisher-by-publisher concern. For a
  multi-producer topology, one mis-configured routing key silently black-holes
  events, and (EG-1) a publish **without** `Mandatory()` succeeds silently.
  `x-alternate-exchange` is the standard server-side safety net (R10-13/T68 L3127–3129,
  deferred). *Outcome (owner decision):* **pull T68 forward** — expose
  `x-alternate-exchange` on `Exchange`/topology so unroutable messages land in a
  catch-all regardless of per-publish discipline. *Acceptance:* a mis-routed publish
  (no `Mandatory()`) lands in the alternate exchange's queue, observable. *Disposition:*
  **pull forward T68** into Phase 15.
- **EDA-02 [Med-High, underspecified]** — publishers reference exchange names by
  string and §6.6 declares topology separately; nothing validates `Exchange("oders")`
  (typo) at `Build` against the wired `Topology`, so it publishes into the void
  (silently, without `Mandatory()` per EG-1). *Location:* §6.6; publisher build path.
  *Outcome:* SPEC documents the silent-drop-without-`Mandatory()` behaviour and the
  topology-drift risk; add an **optional** publisher-side validation that, when the
  `*Topology` is wired to the builder, checks the referenced exchange exists and
  warns/errors at `Build`; recommend `Mandatory()` as the default discipline.
  *Acceptance (EG-1):* a `PublisherFor[M]` given a `Topology` that lacks the named
  exchange fails/warns at `Build`; the silent-drop behaviour is asserted and
  documented. *Disposition:* **new task T103** (complements the T68 server-side net).

### WS-3 — The retry-ladder pattern *(the unsafe variant is the easy one)*

- **EDA-05 [Med-High, underspecified / missing example]** — the obvious tool for
  "retry with backoff" is the delayed-message exchange, which §6.5 (L1536–1552, R10-1)
  correctly warns is node-local, non-replicated, and **lost silently** on node
  failure — exactly the messages you least want to lose. The *safe* pattern
  (durable TTL+DLX ladder) is recommended in prose but has **no example**
  (`examples/` has no `retry_ladder/` or even `delayed/`). The safe pattern must be
  the easy one. *Outcome:* ship `examples/retry_ladder/` — a runnable, durable,
  multi-tier TTL+DLX backoff ladder. *Acceptance:* the example builds
  (`examples-build`) and smoke-runs (`examples-smoke`), demonstrating a message
  cycling through delay tiers and finally to the DLQ. *Disposition:* **new task
  T104** (with EDA-06).
- **EDA-06 [Med, underspecified]** — the recommended TTL+DLX ladder has a sharp edge
  the spec omits: **per-message** TTL on a single queue causes head-of-line blocking
  (RabbitMQ only expires from the head, so a 1s-TTL message behind a 3600s-TTL
  message waits an hour). The canonical fix is a **queue per delay tier**. §6.5
  recommends "TTL + DLX" without naming the per-message-vs-per-queue TTL trap or the
  multi-tier-queue requirement. *Location:* §6.5 L1536–1552. *Outcome:* §6.5
  documents the per-message-TTL HOL-blocking caveat and the queue-per-tier
  requirement; the T104 example demonstrates queue-per-tier. *Acceptance (EG-2):*
  the gate captures the HOL behaviour; the example uses one queue per tier.
  *Disposition:* fold into **T104**.

### WS-4 — DLQ lifecycle completeness *(send-to-DLX is half a strategy)*

- **EDA-09 [Med, underspecified / scope]** — there is no tooling or documented
  pattern for **replaying/redriving** dead-lettered messages back to the source after
  the bug is fixed; the scope boundary is unstated, so users assume it exists.
  (DLQ *bounds* and Consumer *validation* are EDA-07/08, owned by T65.) *Location:*
  §6.6. *Outcome (owner decision):* **build a redrive helper** — a minimal
  `DLQ → source` republish utility that dedupes by `MessageID` (preserving the
  at-least-once contract) and is observable; SPEC §6.6 documents the redrive pattern
  and ships `examples/redrive/`. *Acceptance:* an integration test dead-letters N
  messages, runs the redrive helper, and asserts they reappear on the source queue
  exactly once per `MessageID`. *Disposition:* **new task T105**.

### WS-5 — Event versioning & event-mesh boundaries *(unstated → users assume)*

- **EDA-13 [Med, underspecified / boundary]** — additive schema change is handled by
  lax-JSON / Postel (§6.9 L1985–1992; §8 L2373–2378; decision 23 L2765–2778), but
  **breaking** evolution (field removal, rename, semantic change) is unguided and the
  boundary is unstated. `Message.Type` is positioned as an **optional convention**,
  not a load-bearing discriminator (L1441–1451). *Outcome (owner decision):* SPEC
  §6.9/§8 state the boundary explicitly — additive = safe; breaking = user-owned via
  **versioned type names** (`order.created.v2`) / a new exchange / dual-publish — and
  recommend the `Message.Type` discriminator convention for versioned events; **file
  LATER-40** for a pluggable schema-registry/validation hook. *Acceptance:* the
  boundary and the versioned-type convention are documented; an example or godoc
  shows branching on `Type` before decode. *Disposition:* **new task T106** +
  **LATER-40**.
- **EDA-14 [Med, design-disagreement]** — structured-mode CloudEvents
  (`application/cloudevents+json` body, §6.9 L2006–2010) is promoted by T97 (Lens 03)
  as **the** cross-ecosystem path, but a structured event is **opaque to broker
  routing**: the broker cannot route on a CloudEvents `type`/`subject` that lives in
  the body. Binary mode exposes attributes as headers (routable on 0-9-1) but is
  scoped to 0-9-1 by T97. So the cross-ecosystem path sacrifices broker-side routing
  on event attributes. *Location:* §6.9 L2006–2019. *Outcome:* SPEC §6.9 documents
  that structured-mode events are routing-opaque and recommends setting the AMQP
  routing key / a routing header explicitly (or binary mode for 0-9-1 attribute
  routing). *Acceptance:* the trade-off is documented; an example routes a structured
  CloudEvent by an explicitly-set routing key. *Disposition:* **new task T107**
  (coordinate T97 — same §6.9 CloudEvents subsection).

### WS-6 — Consumption-path footguns & clarity

- **EDA-15 [Med, design-disagreement / footgun]** — a `BatchConsumer` handler that
  returns `nil` triggers `batch.Ack()` = one `basic.ack` with `multiple=true` over
  the **whole** range (§6.4 L1403–1405). If message 3 of 10 was poison but the
  handler returned `nil`, the poison is acked (lost). Safe partial-failure requires
  the non-obvious `Batch.Deliveries()` per-delivery path (L1410–1414). The
  safe-looking `return nil` is the dangerous path. *Outcome:* §6.4 **prominently**
  documents the "`return nil` acks everything (`multiple=true`)" footgun and the
  per-delivery idiom up front (not buried), with a worked partial-failure example.
  *Acceptance (EG-3):* the gate captures the `multiple=true` ack; an example
  demonstrates per-delivery ack/nack for a 1-poison-in-N batch. *Disposition:* **new
  task T108**.
- **EDA-12 [Low-Med, design-disagreement]** — RPC over a broker is a known
  anti-pattern smell (synchronous coupling over async transport), legitimate for some
  flows. §6.7 ships it without a "when (not) to use this" architectural framing.
  (The error-contract and duplicate-reply issues are EDA-10/EDA-11, owned by
  T91/T90.) *Outcome:* add a §6.7 preamble framing RPC as a deliberate-use primitive
  — the coupling caveat, when to prefer a normal request/response *event* pair, and
  the blind-retry/amplification consequence (cross-link T91's structured-error mode).
  *Acceptance:* §6.7 carries the usage-guidance preamble. *Disposition:* **new task
  T109** (doc; coordinate T91).
- **EDA-17 [Low, underspecified]** — consumers are pinned to consumer connections by
  a stable hash of consumer-tag (§6.1 L538–549); with the default 2 connections and
  few consumers this can **hot-spot** (all hash to one socket), and "adding consumer
  connections rebalances long-lived consumers" is unspecified as to mechanism (a
  modulo hash reshuffles ~all keys on N→N+1; consistent hashing does not). *Outcome:*
  §6.1 clarifies the pinning hash, the hot-spot risk at low connection/consumer
  counts, and whether scale-up migrates live consumers (and the reconnect cost) or
  only affects new ones. *Acceptance:* the pinning/rebalancing behaviour is
  documented; if a code clarification is warranted it is scoped here. *Disposition:*
  **new task T110** (doc).

### WS-7 — Pull-forward (topology expressiveness)

- **EDA-03 [Med, deferred-gap]** — `Binding` only binds exchange→queue; layered
  fan-out (a top-level ingest exchange routing to per-domain exchanges) requires
  flattening the topology or declaring out-of-band. `exchange.bind` (R10-14/T69
  L3130–3132) is deferred. *Outcome (owner decision):* **pull T69 forward** — extend
  `Binding` (or a typed variant) so an exchange can be a binding destination.
  *Acceptance:* a `Topology` declares an ingest→per-domain e2e binding; a publish to
  ingest reaches a per-domain queue. *Disposition:* **pull forward T69** into
  Phase 15.

---

## 6. Do-not-regress list (confirmed-correct, protect with tests)

1. **`Message.Delay` / `ExchangeDelayed` durability warning (R10-1, §6.5 L1536–1552)**
   — the node-local/non-replicated/lost-silently warning and the TTL+DLX
   recommendation are correct and load-bearing; EDA-05/06 only add the missing
   *example* and the *HOL caveat*, never weaken the warning.
2. **Two-counter poison design + quorum `x-delivery-limit`** (§6.3 L1227–1308) — the
   cross-process (`x-death`) + in-process counters and the RabbitMQ-4.x default
   delivery-limit-of-20 honesty are solid.
3. **`HandlerTimeoutVerdict=TimeoutNackNoRequeue` default** (§6.3 L1163–1167) — a
   misconfigured handler cannot create an infinite requeue loop; keep.
4. **Nack-without-requeue default + `ErrRequeue` opt-in** (§6.3 L1117–1122) — the safe
   default for handler errors; keep.
5. **DLX preserves every property + header incl. `traceparent`/`tracestate`**
   (§6.6 L1696–1706) — load-bearing for DLQ forensics; T70/T90 protect related paths.
6. **`MessageID` UUIDv7 + at-least-once contract** (§6.2.1 L1013–1053) — the
   dedupe-by-MessageID linchpin; EDA-18 only ships the missing example (via T85).
7. **Decision 53 strict shutdown cascade** (L2617–2619) — consumers cancelled before
   publish drain before socket close; the order is load-bearing.
8. **Decision 54 BatchConsumer OTel Links** (L2621–2623) — a batch span receives a
   `Link` per incoming `traceparent` rather than adopting a single parent; this is the
   architecturally correct fan-in model. Keep.
9. **Topology declare-once / deep-snapshot / redeclare-on-reconnect**
   (§6.6 L1668–1750) — never re-declares a queue with different args; correct. T68/T69
   pull-forward extends this model, must not break the snapshot semantics.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Amend SPEC | Disposition | LATER.md | Owner decision? |
|---------|-----|-----------|-------------|----------|-----------------|
| EDA-01 | High | ✅ §6.6 | **pull forward T68** | — | ✅ pull forward |
| EDA-02 | Med-High | ✅ §6.6 | **T103** | — | — |
| EDA-03 | Med | ✅ §6.6 | **pull forward T69** | — | ✅ pull forward |
| EDA-04 | High | ✅ §6.3/§6.6 | **T102** + example | — | ✅ pull into scope |
| EDA-05 | Med-High | ✅ §6.5 | **T104** + `examples/retry_ladder/` | — | — |
| EDA-06 | Med | ✅ §6.5 | **T104** | — | — |
| EDA-07 | High | (T65) | **covered by T65** — do not re-file | — | — |
| EDA-08 | High | (T65) | **covered by T65** — do not re-file | — | — |
| EDA-09 | Med | ✅ §6.6 | **T105** + `examples/redrive/` | — | ✅ build helper |
| EDA-10 | Med-High | (T91) | **covered by T91** — do not re-file | — | — |
| EDA-11 | Med | (T90) | **covered by T90** — do not re-file | — | — |
| EDA-12 | Low-Med | ✅ §6.7 | **T109** (doc) | — | — |
| EDA-13 | Med | ✅ §6.9/§8 | **T106** | **LATER-40** | ✅ doc + convention + LATER |
| EDA-14 | Med | ✅ §6.9 | **T107** (coord. T97) | — | — |
| EDA-15 | Med | ✅ §6.4 | **T108** + example | — | — |
| EDA-16 | High | (T70) | **covered by T70** — do not re-file | — | — |
| EDA-17 | Low | ✅ §6.1 | **T110** (doc) | — | — |
| EDA-18 | Med | (T85) | **extend T85** — `examples/idempotent_consume/` | — | — |

---

## 8. Suggested phasing (planner may revise — "Phase 15")

1. **Phase A — Gates (no new lane).** Capture EG-1..EG-4 on the existing integration
   lane (3.13 + 4.x); commit the results table. No SPEC edit to an affected section
   until its gate returns. **(T101)**
2. **Phase B — Topology expressiveness (pull-forward + scale).** Alternate exchange
   (**T68**), exchange-to-exchange bindings (**T69**), consistent-hash exchange +
   partitioned ordered consumer (**T102**), publisher unroutable-safety (**T103**).
   These reshape the `Topology`/publisher surface; land them together. Locked by
   EG-1/EG-4.
3. **Phase C — Reliability patterns.** Durable retry ladder + HOL doc (**T104**),
   DLQ redrive helper (**T105**). Locked by EG-2.
4. **Phase D — Event-format & consumption guidance (SPEC-first, mostly doc).**
   Schema-evolution boundary + LATER-40 (**T106**), structured-mode routing opacity
   (**T107**, coord. T97), batch partial-failure footgun (**T108**, locked by EG-3),
   RPC usage framing (**T109**, coord. T91), consumer-tag pinning clarity (**T110**),
   and **extend T85** to ship `examples/idempotent_consume/`.

---

## 9. Acceptance criteria for the whole effort

- [ ] EG-1..EG-4 captured on the existing integration lane (3.13 **and** 4.x) into a
      committed results table each downstream task cites (T101). No new build-tag lane.
- [ ] **Ordered keyed streams scale (EDA-04/T102):** an `x-consistent-hash`
      `ExchangeKind` + partitioned-consumer wiring + a worked example preserve per-key
      order across N parallel-consumed queues; §6.6 documents the pattern + rebalancing
      trade-off.
- [ ] **Unroutable black-hole closed:** `x-alternate-exchange` exposed (EDA-01/T68);
      publisher-side exchange-name validation against the wired `Topology` warns/errors
      at `Build`, and the silent-drop-without-`Mandatory()` behaviour is documented
      (EDA-02/T103).
- [ ] **Layered fan-out enabled (EDA-03/T69):** exchange-to-exchange bindings declarable
      via `Topology`.
- [ ] **Safe retry is the easy one (EDA-05/06/T104):** `examples/retry_ladder/` ships
      (builds + smoke-runs) as a durable multi-tier TTL+DLX ladder; §6.5 documents the
      per-message-TTL HOL trap + queue-per-tier requirement.
- [ ] **DLQ lifecycle complete (EDA-09/T105):** a redrive helper republishes DLQ→source
      (dedupe by `MessageID`), observable; `examples/redrive/` ships; §6.6 documents the
      pattern. (DLQ bounds + Consumer DLX validation land via T65.)
- [ ] **Event-evolution boundary stated (EDA-13/T106):** §6.9/§8 document
      additive-safe / breaking-user-owned via versioned type names + the `Message.Type`
      convention; **LATER-40** files the schema-registry hook.
- [ ] **Structured-mode routing opacity documented (EDA-14/T107):** §6.9 states
      structured CloudEvents are routing-opaque + recommends explicit routing key/header
      (coordinate T97).
- [ ] **Batch footgun defused (EDA-15/T108):** §6.4 prominently documents the
      `return nil` → `multiple=true` whole-range-ack trap + the per-delivery idiom; an
      example demonstrates 1-poison-in-N partial failure.
- [ ] **RPC usage framing (EDA-12/T109)** and **consumer-tag pinning clarity
      (EDA-17/T110)** documented.
- [ ] **`examples/idempotent_consume/` ships (EDA-18)** via extended T85, matching the
      reworked dedupe pattern.
- [ ] No finding re-filed that a prior lens owns (EDA-07/08→T65, 10→T91, 11→T90,
      16→T70, 18→T85). `LATER.md` adds only LATER-40. No duplicate task IDs.
- [ ] `README.md` reflects the changed contract (consistent-hash + alternate +
      e2e-binding topology, redrive helper, retry-ladder + schema-evolution guidance).
- [ ] Tree green: `make test` (race + cover), `make lint`, the per-version integration
      lane (incl. the new examples' smoke run); `goleak` clean.

---

## 10. Owner decisions (recorded 2026-05-28)

1. **Ordered keyed-stream scaling = PULL INTO SCOPE (EDA-04).** Add an
   `x-consistent-hash` `ExchangeKind` + partitioned-consumer support + a worked
   example, eliminating the ordering-vs-scale forced choice in v0.1. **(T102)**
2. **Topology safety nets = PULL BOTH FORWARD (EDA-01, EDA-03).** Pull **T68**
   (alternate exchange) *and* **T69** (exchange-to-exchange bindings) forward from
   Phase 11 into Phase 15.
3. **DLQ redrive = BUILD A HELPER (EDA-09).** Ship a minimal DLQ→source republish
   utility (dedupe by `MessageID`) + `examples/redrive/`. **(T105)**
4. **Schema evolution = DOCUMENT BOUNDARY + VERSIONED-TYPE CONVENTION + LATER
   (EDA-13).** §6.9/§8 state the additive/breaking boundary and recommend versioned
   type names via `Message.Type`; file **LATER-40** for a pluggable schema-registry
   hook. **(T106 + LATER-40)**

(The remaining new tasks — T103, T104, T107, T108, T109, T110 — and the T85
extension follow directly from the findings and need no further owner input.)

---

## 11. Pattern support matrix (lens output #1)

Status = supported / awkward / unsupported. "Safe default?" = is the *safe* variant
the *default*. "Observable?" = is the failure mode visible (error/metric/return).

| Pattern | Status (v0.1) | Safe default? | Failure observable? | After Phase 15 | Notes |
|---------|---------------|---------------|---------------------|----------------|-------|
| Pub/sub fan-out | supported | partial | partial | supported / safe | Mis-route silently black-holes w/o `Mandatory()` (EDA-01/02) → T68/T103. |
| Competing-consumer work queue | supported | yes | yes | supported | Concurrency/Prefetch defaults sane (§6.3 L1161–1193); pinning hot-spot clarity (EDA-17/T110). |
| Request/reply (RPC) | awkward | no | no | awkward (honest) | All failures = timeout (EDA-10/T91); dup reply (EDA-11/T90); no usage framing (EDA-12/T109). |
| Retry ladder (backoff) | awkward | **no** | partial | supported / safe | Lossy delayed-exchange is the easy tool; safe TTL+DLX prose-only, no example, HOL trap (EDA-05/06/T104). |
| DLQ + redrive | awkward | no | partial | supported | DLX routing works; DLQ unbounded (EDA-07/T65), silent drop (EDA-08/T65), no redrive (EDA-09/T105). |
| Ordered keyed stream (at scale) | **unsupported** | n/a | n/a | supported | SAC+Concurrency(1) = ordering w/o scale; consistent-hash absent (EDA-04/T102). |
| Idempotent consume | supported | yes | yes | supported | Contract + pattern in §6.2.1; example absent (EDA-18/T85); MessageID UUIDv7 auto. |
| Batch consume | supported | **no** (partial failure) | partial | supported / safe | `return nil` acks whole range incl. poison (EDA-15/T108); partial batch on Close (EDA-16/T70). |
| Layered fan-out (e2e bindings) | **unsupported** | n/a | n/a | supported | No `exchange.bind` (EDA-03/T69). |
| Event versioning | awkward (breaking) | partial | no | awkward (boundary stated) | Additive safe (Postel); breaking unguided (EDA-13/T106). |
| Event mesh (CloudEvents) | supported (structured) | yes | n/a | supported (caveat) | Structured = cross-ecosystem (T97) but routing-opaque (EDA-14/T107). |
| Delayed publish (best-effort) | supported | **yes (warned)** | yes | supported | R10-1 warning clear; do-not-regress. |

---

## 12. Findings table (lens output #2)

| ID | Sev | Class | Location (§ + lines) | Pattern/architecture impact | Why it matters at billions/day | Disposition |
|----|-----|-------|----------------------|-----------------------------|--------------------------------|-------------|
| EDA-01 | High | design-disagreement | §6.6; §6.2 L850–871; R10-13 L3127–3129 | No server-side unroutable catch-all; w/o `Mandatory()` mis-routed events vanish silently | One typo in one of dozens of producers silently black-holes a stream | pull fwd **T68** |
| EDA-02 | Med-High | underspecified | §6.6; publisher build | Exchange-name typo not caught at `Build` vs `Topology` → publish-into-void | Topology drift across independently-deployed services is invisible | **T103** (EG-1) |
| EDA-03 | Med | deferred-gap | R10-14 L3130–3132 | No exchange→exchange bindings → layered fan-out needs flattening | Ingest→per-domain fan-out is bread-and-butter EDA | pull fwd **T69** |
| EDA-04 | High | design-disagreement | §6.3 L1203–1225; §6.6 | Ordered work scales only to one active consumer; no consistent-hash | Forces choosing between ordering and throughput on keyed streams | **T102** (EG-4) |
| EDA-05 | Med-High | underspecified | §6.5 L1536–1552; `examples/` | Lossy delayed-exchange is the obvious retry tool; safe ladder has no example | Retries are the messages you least want to lose | **T104** |
| EDA-06 | Med | underspecified | §6.5 L1536–1552 | Per-message-TTL HOL blocking / queue-per-tier not documented | Naive single-queue TTL ladder stalls under mixed delays | **T104** (EG-2) |
| EDA-07 | High | deferred-gap | §6.6; R10-10 L3112–3119 | Unbounded `<source>.dlq` → cluster-wide disk-alarm outage | An unbounded DLQ is a latent platform outage | **T65** (covered) |
| EDA-08 | High | deferred-gap | §6.3 vs §6.7 L1845–1857 | `Consumer` w/ `MaxRedeliveries>0` + no DLX drops poison silently | Silent poison drops violate §1 | **T65** (covered) |
| EDA-09 | Med | underspecified | §6.6 | No DLQ replay/redrive tooling; boundary unstated | "Replay the DLQ" with no replay tooling is a non-plan | **T105** |
| EDA-10 | Med-High | design-disagreement | §6.7 L1829–1836; dec.16 L2705–2711 | Every RPC failure looks like a timeout → forced blind-retry | Blind retry over async transport is an amplification vector | **T91** (covered) |
| EDA-11 | Med | underspecified | §6.7 L1813–1827 | Duplicate reply to a returned `Call` — cross-talk contract implicit | At-least-once replies guarantee duplicates at scale | **T90** (covered) |
| EDA-12 | Low-Med | design-disagreement | §6.7 | RPC shipped w/o "when (not) to use" framing | Blessing sync-over-async without a caveat corrupts designs | **T109** |
| EDA-13 | Med | underspecified | §6.9 L1985–1992; §8 L2373–2378; L1441–1451 | Breaking schema evolution unguided; `Type` is optional convention | Field removal/rename across dozens of services needs a story | **T106** + LATER-40 |
| EDA-14 | Med | design-disagreement | §6.9 L2006–2019 | Structured CloudEvents (the cross-ecosystem path) is routing-opaque | An event mesh that can't route on event type isn't a mesh | **T107** |
| EDA-15 | Med | design-disagreement | §6.4 L1403–1414 | `return nil` acks whole batch (`multiple=true`) incl. unprocessed poison | Silent loss hidden behind the safe-looking return | **T108** (EG-3) |
| EDA-16 | High | deferred-gap | §6.4; R10-15 L3133–3136 | Partial batch / undispatched on `Close` undefined → loss on deploy | Deploy is the most frequent operation | **T70** (covered) |
| EDA-17 | Low | underspecified | §6.1 L538–549 | Consumer-tag-hash pinning hot-spot; rebalancing mechanism unclear | Hot-spotting wastes a TCP socket's throughput headroom | **T110** |
| EDA-18 | Med | missing-artifact | §6.2.1 L1067–1068 | `examples/idempotent_consume/` referenced but absent | The at-least-once linchpin example is missing | **T85** (extend) |

---

## 13. Out of scope for this plan

- The other validation lenses (01 protocol — Phase 12; 02 distributed-systems —
  Phase 13; 03 interoperability — Phase 14; 05 SRE; 06–13). Cross-lens shared
  findings extend the shared task, never re-filed (EDA-07/08→T65, 10→T91, 11→T90,
  16→T70, 18→T85).
- A pluggable schema-registry/validation hook (→ **LATER-40**).
- Stream-protocol semantics (v0.2 per decision 24).
- Removing or redesigning RPC (EDA-12 adds framing only; the error contract is
  T91's opt-in mode — RPC ships in v1 with honest caveats).
- Any change to the §10 API design north stars beyond the corrections above.

---

## 14. Verdict for this lens

**GO-WITH-CHANGES.** The library expresses the everyday EDA patterns
(competing-consumer, idempotent consume, pub/sub, best-effort delayed publish)
cleanly and safely, and the prior lenses already pulled forward the two active
silent-loss gaps (DLQ-unbounded/T65, Close-loss/T70) plus the RPC duplicate/error
contracts (T90/T91). What this lens adds is **architecture-shaped**: one pattern is
effectively unsupported at the stated bar (ordered keyed streams at scale — no
consistent-hash, EDA-04), one safety net is missing at the platform level
(unroutable black-hole — no alternate exchange, EDA-01/02), one default nudges
toward the lossy variant (delayed exchange for retries vs the example-less safe
ladder, EDA-05/06), one batch default silently acks poison (EDA-15), and several
boundaries are unstated (redrive, breaking schema evolution, structured-mode
routing opacity, layered fan-out). None is a *new* §1 message-loss bug; all are
closed by pulling two deferred tasks forward, adding the consistent-hash + redrive
primitives the owner brought into scope, shipping the missing safe-pattern examples,
and stating the boundaries honestly.
