# Plan Input — Remediate Distributed-Systems / Failure-Mode Findings (Lens 02)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> distributed-systems / failure-mode lens
> (`spec-validation/02-distributed-systems.md`). It enumerates confirmed
> defects, their evidence, the desired outcome, and an acceptance test for
> each, grouped into workstreams and sequenced by dependency. Produce a phased
> implementation plan that sequences the SPEC amendments, regression/chaos
> tests, task conversions, and `LATER.md` entries below.
>
> **Cross-lens note:** several findings here overlap Phase 11 (R10-x / T57–T72)
> and Phase 12 (Lens 01, T75–T83). The disposition for those is **extend the
> existing task's acceptance criteria — do not create a duplicate task.** New
> task IDs proposed below (**T84–T91**) are *suggestions*; the planner owns the
> final numbering and may fold them into a new "Phase 13 — Distributed-Systems
> Re-review (Lens 02)" mirroring how Lens 01 became Phase 12.

---

## 1. Objective

Bring `SPEC.md` and the v0.1 implementation surface into honest alignment with
the library's own §1 reliability bar ("no silent loss / backpressure /
duplicate / poison loop") **under crash, partition, and clock-skew conditions** —
not just on the happy path. The lens verdict is **NO-GO for the v0.1 §1 bar as
written; GO-WITH-CHANGES** once the High findings land.

Concretely, the plan must:
1. Correct the confirmed **factual errors / overclaims** in `SPEC.md` (amend
   SPEC first, per `CLAUDE.md` "Anything contradicting SPEC must amend SPEC
   first"): the SAC ordering oversell (DS-06), the poison-counter crash-loop
   overclaim (DS-13), and the UUIDv7-eviction non-sequitur (DS-15).
2. Close the **two active §1 violations shipping in v0.1**: the unbounded
   reconnect barrier (DS-02) and the undefined loss-or-duplicate on `Close`
   (DS-03). Decide pull-forward vs documented-deferral for the rest.
3. Fill the **silent failure domains the spec omits entirely**: cluster
   partition-handling modes (DS-05) and queue-type-dependent confirm semantics
   (DS-07).
4. Specify the **underspecified correctness windows**: RPC orphan-reply handling
   (DS-11), forced-close / `PublishBatch`-retry reorder windows (DS-16/DS-17).
5. Resolve the **verification gates** (G1–G6) that block correct wording —
   each needs a live broker, a 3-node cluster, or a chaos harness.
6. Route every non-blocking finding to `LATER.md` with the required format and
   record the **owner decisions** (RPC error-collapse DS-12; pull-forward of
   T62/T71).

This plan is **remediation of an existing spec**, not a new feature. Treat each
finding as a test-first work item; for failure-mode claims the "test" is a
chaos/partition integration assertion, not a mock.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. Section/line anchors below are
  against the current revision (post Rev 10). **Re-confirm line numbers before
  editing.** Sections referenced: §1 (Reliability bar L37–86), §6.1 (reconnect
  barrier L576–594, degraded mode L595–612, Close cascade L640–647, partition
  detection L678–702), §6.2 (confirm semantics L892–897, retry duplicate hazard
  L996–1008, batch recovery L915–929), §6.2.1 (at-least-once + dedupe L1013–1069),
  §6.3 (ordering L1203–1225, poison counters L1267–1321, Delivery.Ack/Nack
  L1107–1115), §6.5 (`Timestamp`/`MessageID` defaults L1438/L1448), §6.7 (RPC
  L1754–1862), §9 (success criteria L2426–2549), §10 (decisions 25/27/28/30/43/53,
  Rev 10 R10-5/7/8/11/15/16).
- `tasks/plan.md` — phased plan. Findings map to existing tasks: **T60** (R10-5,
  double-ack guard — *already pulled into Phase 12*), **T62** (R10-7, redeclare
  de-amplification), **T63** (R10-8, barrier cap), **T66** (R10-11, addr shuffle
  — *already pulled into Phase 12*), **T70** (R10-15, shutdown completeness),
  **T71** (R10-16, redelivery metric), **T82** (Lens 01 RMQ-25, ack-vs-confirm
  wording). Do **not** create duplicate tasks; amend their acceptance criteria.
- `tasks/todo.md` — granular checklist tied to `plan.md`.
- `LATER.md` — deferred non-blockers (existing **LATER-34/35** are adjacent;
  DS-12 likely becomes a new **LATER-39**).
- The validation report this brief is derived from:
  `spec-validation/02-distributed-systems.md` (the prompt) and the findings
  (IDs `DS-01`..`DS-17`).

---

## 3. Constraints & working agreements (planner must honour)

- **TDD is a hard discipline.** Every behaviour/contract change: failing test
  first, then the smallest amendment. For failure-mode claims the test runs
  against a **real broker** (and, for partition/SAC findings, a **multi-node
  cluster + a fault injector** — `toxiproxy`, `iptables`, or
  `rabbitmqctl stop_app` on one node). A mock cannot falsify a partition claim.
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in
  the same PR (or earlier), then implementation follows.
- **Chaos lane, not just integration lane.** The existing `integration` tag
  (single-broker `testcontainers-go`) cannot reproduce DS-05/06/07. Introduce a
  cluster fixture (3-node RabbitMQ, configurable `cluster_partition_handling`)
  behind a new `chaos` build tag or extend the conformance lane. Gate it on the
  same per-version matrix (3.13 LTS + 4.x) the spec already requires.
- **`goleak` on every goroutine-spawning test** (project rule) — the barrier-cap
  (DS-02) and channel-recovery paths must not leak supervisors.
- **`testify` `assert`/`require`** in every `_test.go` (hard rule).
- **Docs in English** (`CLAUDE.md`). All SPEC/LATER/plan/todo text in English.
- **Module path** `github.com/brunomvsouza/warren`; `errorlint` is on (compare
  with `errors.Is`/`errors.As`).
- **Choke points:** reconnect/barrier logic in `internal/reconnect` +
  `connection.go`; confirm tracking in `internal/confirms`; broker-error
  translation through `internal/amqperror`; redaction through `internal/redact`.
  Corrections land at the choke point, not ad-hoc.
- **README sync:** any change to the external reliability contract (a new §1
  carve-out for partitions/delays, the SAC qualification, a redelivery metric)
  updates `README.md` "Available now / On the roadmap" + the reliability-guarantee
  copy, per the project rule.

---

## 4. Pre-work: verification gates (sequence FIRST — they gate wording)

These behaviours must be observed on a live broker / cluster before the
dependent amendments can be worded correctly. Each is a short spike. **Front-load
G1, G2, G4** — they gate the highest-value workstreams (WS-1, WS-2). Gates that
need a cluster (G3, G4) share one fixture; build it once.

| Gate | Question | Blocks | How to resolve |
|------|----------|--------|----------------|
| **G1** | On SAC failover with `Prefetch>1`, do the dead consumer's requeued unacked messages reorder relative to messages published during the failover gap? Differs classic vs quorum, 3.13 vs 4.x? | DS-06 | 3-node cluster, SAC queue, `Prefetch=64`. Publish a monotonic sequence; kill the active consumer mid-stream; on the promoted consumer, record delivery order + `Redelivered()`. Assert whether strict order holds across the boundary. Run on classic **and** quorum, 3.13 **and** 4.x. |
| **G2** | What does the **current** implementation do with prefetched-but-undispatched deliveries on `Close(ctx)` — requeue or drop? | DS-03 / T70 | Consumer with `Prefetch=64`, slow/blocked handler; publish 64; call `Close` before dispatch drains. Observe broker queue depth + `Redelivered()` on a subsequent consume. This reveals the *actual* v0.1 behaviour the spec must now name. |
| **G3** | Quorum queue, publisher pinned to a **minority-partition** node: does the publish hang unconfirmed, time out, or error? What is observed on heal (duplicate)? | DS-07 | 3-node cluster + fault injector to isolate one node. Publish to the minority node with confirms; observe `ErrConfirmTimeout` timing; heal; check for a duplicate delivery. |
| **G4** | Exact **client-side signal** per `cluster_partition_handling` mode — `pause_minority` / `autoheal` / `ignore`. Connection-forced (320)? Silent socket drop? Nothing? | DS-05 | For each mode, partition the cluster and record what `amqp091-go` surfaces to the client (close reason code, error, or silence). Confirm `ignore` can lose acked messages silently on heal. |
| **G5** | Does a poison message that **crashes the consumer process** defeat Counter B (process-local), causing >`MaxRedeliveries+1` total reprocessing across restarts? | DS-13 | Classic queue, no broker `x-delivery-limit`, `MaxRedeliveries=3`. Handler panics/os.Exit on a specific payload. Supervise-restart the consumer; count total deliveries of that message. Expect unbounded (resets per restart). |
| **G6** | What does the `Caller` do with a **second reply** for an already-resolved `CorrelationID` (the documented double-reply, §6.7 L1822)? Clean discard, unacked leak, or mis-route? | DS-11 | Replier that publishes-confirms then crashes before ack → request redelivered → second reply. Concurrent `Call`s on the shared `amq.rabbitmq.reply-to` channel; assert the orphan reply does not resolve/affect another in-flight call. Repeat for `UseExclusiveReplyQueue()`. |

**Planner note:** G1, G2, G4, G5 each reveal a *current* behaviour the spec
must name; G3/G6 reveal whether a window is benign or a real defect. No SPEC
edits to the affected sections until the relevant gate returns.

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets. "Disposition" = where the change lands (see routing matrix §7).

### WS-1 — Active v0.1 §1 violations *(highest value; the NO-GO drivers)*

Two paths shipping in v0.1 silently violate §1 and are only deferred to Phase 11.

- **DS-02 [High]** — unbounded reconnect barrier = silent stall. *Location:* §6.1
  L589–594; §6.2 L980–981 (`ConfirmTimeout` starts only after the frame leaves
  the socket); R10-8/**T63** L3098–3104. *Outcome:* the barrier is bounded; on
  cap, blocked publishers get `ErrReconnecting` (wrapping a deadline) instead of
  hanging. **Spec must state explicitly that `ConfirmTimeout` does NOT cover the
  barrier** (the frame is unwritten), so the cap is a *distinct* mechanism — name
  the option (`BarrierTimeout`? reuse `PublishTimeout`?), its default, and the
  post-cap connection state (force-reconnect vs degraded). *Acceptance:*
  integration test — half-alive broker (accepts socket, stalls `queue.declare`,
  e.g. via a proxy) + `PublishTimeout=0` + `context.Background()`; assert
  `Publish` returns `ErrReconnecting` within the cap, not ∞. `goleak` clean.
  *Disposition:* amend §6.1/§6.2 + **extend T63** (already covers Khepri per Lens
  01 RMQ-17); **owner decision: pull T63 into v0.1.**
- **DS-03 [High]** — `Close` prefetched-but-undispatched = undefined loss-or-dup.
  *Location:* §6.1 L640–647 (cascade drains *active* handlers only);
  R10-15/**T70** L3133–3136. *Outcome (gated by G2):* the spec **picks one** —
  nack-requeue (`requeue=true`) the undispatched buffer before channel close
  (at-least-once duplicate, acceptable under §6.2.1) — never drop (silent loss).
  Same for a `BatchConsumer`'s pending partial batch. *Acceptance:* G2's harness,
  asserting the undispatched messages are redelivered (not lost) after `Close`;
  add `consumer_shutdown_requeued_total`. *Disposition:* amend §6.1 + **extend
  T70**; **owner decision: pull T70 into v0.1.**

### WS-2 — Missing silent-failure domains *(spec is wholly silent)*

- **DS-05 [High]** — cluster partition-handling modes absent. *Location:*
  whole-spec (0 hits for `pause_minority`/`autoheal`/`ignore`); §6.1 partition
  detection L678–702 covers only TCP/heartbeat, not cluster partitions.
  *Outcome (gated by G4):* a new §6.1 subsection documenting the client-side
  observation per mode and an explicit **§1 carve-out** that under `ignore`
  (split-brain) acked messages can be lost silently on heal — analogous to the
  R10-1 delayed-message carve-out. Recommend against `ignore`; recommend
  `pause_minority` + `WithAddrs` failover. *Acceptance:* G4 documents the exact
  signal per mode; a chaos test asserts the client sees a classifiable
  reconnect under `pause_minority`/`autoheal`. *Disposition:* amend §6.1 +
  README reliability copy + **new task T85** (doc + chaos assertion).
- **DS-07 [Med-High]** — confirm semantics are queue-type-blind. *Location:* §6.2
  L892–897 (single blanket "ack ≠ persisted"). *Outcome:* a queue-type table —
  **quorum** = confirm after Raft majority-commit (understated today; users add
  redundant dedupe); **classic durable+persistent** = after fsync/batch;
  **transient/non-durable** = immediate, no durability. **Plus** name the
  *quorum minority-partition* window (gated by G3): no quorum → publish
  unconfirmed → `ErrConfirmTimeout` → `PublishRetry` → duplicate on heal — tie
  to DS-05. *Acceptance:* G3 confirms the timeout→retry→duplicate path; SPEC
  table reviewed against RabbitMQ quorum-queue docs. *Disposition:* amend §6.2 +
  **coordinate T82** (Lens 01 RMQ-25 already touches ack-vs-confirm wording —
  merge, don't duplicate) + **new task T87** for the minority-partition window +
  test (dep G3).

### WS-3 — Factual overclaims to correct *(amend SPEC first)*

- **DS-06 [High]** — SAC oversold as "strict ordering with failover". *Location:*
  §6.3 L1221–1225; §6.6 L1617–1619; decision 30 L2840. *Outcome (gated by G1):*
  qualify to "per-channel ordering **steady-state only**; at the failover
  boundary up to `Prefetch` messages are redelivered (duplicates) and may
  reorder." Recommend `Prefetch(1)` with SAC when cross-failover order matters
  (reduces, never eliminates). Update `examples/ordered_consume/` README to state
  the boundary caveat. *Acceptance:* G1's failover test, asserting the documented
  reorder/duplicate behaviour per queue-type per broker-version. *Disposition:*
  amend §6.3/§6.6 + decision 30 + **new task T86** (doc + chaos test, dep G1).
- **DS-13 [Med-High]** — poison Counter B is not crash-safe; §1/§9 overclaim.
  *Location:* §1 L59–64; §6.3 L1267–1321; §9 L2449–2452. *Outcome (gated by G5):*
  state that Counter B (process-local, L1302) does **not** bound a poison message
  that crashes the consumer process (resets per restart); the only crash-safe
  bound is quorum `x-delivery-limit`. Downgrade the §1 "no silent poison loop" +
  §9 "at most `MaxRedeliveries+1` deliveries" claims to "per-process-lifetime,
  classic-queue; crash-safe only with quorum `x-delivery-limit`." *Acceptance:*
  G5's crash-loop test demonstrates the unbounded reprocessing; a quorum
  counterpart shows the broker-side bound holds across restarts. *Disposition:*
  amend §1/§6.3/§9 + **new task T90** (doc + chaos test, dep G5); coordinate T58
  (version-aware delivery-limit).
- **DS-15 [Low-Med]** — UUIDv7 "makes eviction by recency trivial" non-sequitur +
  undocumented wall-clock dependence. *Location:* §6.2.1 L1020–1022; §6.5 L1448
  (`Timestamp` default `time.Now()`), L1438 (`MessageID` UUIDv7). *Outcome:* drop
  the eviction claim (an `lru.Cache` evicts by access order, not the key's
  embedded timestamp); document that `MessageID`/`Timestamp` ordering is
  **per-publisher wall clock**, not global, not monotonic across NTP steps; only
  ID *uniqueness* is load-bearing for dedupe. *Disposition:* amend §6.2.1/§6.5 —
  fold into **T84** (the §6.2.1 dedupe rework).

### WS-4 — Dedupe-pattern adequacy *(the at-least-once remedy itself is inadequate)*

- **DS-01 [High, design-disagreement]** — the canonical dedupe is crash-unsafe.
  *Location:* §6.2.1 L1032–1059; challenges decision 25. *Outcome:* split the two
  duplicate classes: (a) **publish-retry** dups (bounded by outage+reconnect+retry
  → in-memory LRU OK) vs (b) **crash/requeue redelivery** dups (unbounded gap,
  AND the crash that triggers redelivery wipes the in-memory cache → in-memory
  dedupe gives ~zero protection). State that for handlers with external
  side-effects (the headline DB/HTTP/payments case), **persistent dedupe is
  required, not "paranoid."** Recommend bounding queue residency with a TTL so a
  finite persistent retention window is definable. *Acceptance:* a new
  `examples/idempotent_consume/` variant (or sibling) backed by Redis/DB; a chaos
  test that crashes the consumer mid-handler and asserts the persistent path
  dedupes the redelivery while the in-memory path does not. *Disposition:* amend
  §6.2.1 + **new task T84** (rework §6.2.1 + persistent-dedupe example; absorbs
  DS-15 + DS-16). *Note:* this is a remedy-quality change, not a code defect —
  but it undermines the whole at-least-once contract's usability, so it ranks High.

### WS-5 — Underspecified correctness windows

- **DS-04 [High]** — double-verdict → `406` → channel close → collateral handler
  death. *Location:* §6.3 L1107–1115; R10-5/**T60** L3078–3084. *Outcome:* the
  `Delivery[M]` resolved-once guard (already T60, *already pulled into Phase 12*).
  This WS only adds the **§6.3 wording**: state that a double `Ack`/`Nack`/`AckIf`
  (or a late verdict after handler-timeout, esp. via `ConsumeRaw`) is a no-op,
  and that pre-fix it is a channel-killing event taking out every in-flight
  handler on the channel. *Acceptance:* T60's existing criteria + a `ConsumeRaw`
  double-call unit test. *Disposition:* amend §6.3 + **T60 (Phase 12 — extend
  acceptance with the §6.3 doc + ConsumeRaw case).**
- **DS-11 [Med]** — RPC orphan-reply handling unspecified. *Location:* §6.7
  L1779–1784, 1822–1827. *Outcome (gated by G6):* state that the `Caller`
  discards a reply whose `CorrelationID` has no pending entry (with a
  metric/log), that UUIDv7 `CorrelationID`s are never reused so a late reply
  cannot bind to a subsequent `Call`, and that in `UseExclusiveReplyQueue()` mode
  the orphan is ack-and-dropped (not left unacked). *Acceptance:* G6's
  double-reply test asserts the orphan does not resolve/disturb a concurrent
  `Call`. *Disposition:* amend §6.7 + **new task T89** (spec + test + the discard
  metric, dep G6).
- **DS-16 [Low-Med]** — forced-close duplicate window unnamed. *Location:* §6.1
  L646–647; §6.2.1 enumeration. *Outcome:* name it — ctx-deadline force-close
  abandons in-flight handlers → redelivery → duplicate; distinct from DS-03's
  *undispatched* case (this is *abandoned-in-flight*). *Disposition:* amend
  §6.2.1 — fold into **T84**.
- **DS-17 [Low]** — `PublishBatch` order broken by mid-batch channel-close +
  retry. *Location:* §6.2 L898–929; decision 43 L2930. *Outcome:* note that the
  input-order guarantee holds only absent a mid-batch channel close; a
  caller-retried slot (decision 43) loses its position — callers needing order
  must re-publish the whole batch, not just failed indices. *Disposition:* amend
  §6.2 + decision 43 — **new task T91** (one-paragraph SPEC fix; can ride with
  T84).

### WS-6 — Recovery-storm de-amplification *(compounding, deferred)*

These compound into a self-amplifying cluster-recovery cascade; both already have
tasks.

- **DS-09 [Med, High at scale]** — `AttachTo` redeclares broker-global topology on
  **every** pooled connection → N×(topology) declares on recovery (quorum
  declares are Raft ops). *Location:* §6.1 L583–584; R10-7/**T62** L3092–3097.
  *Disposition:* **T62**; **owner decision: pull into v0.1** given it strikes a
  just-recovered (possibly Khepri-quorum-forming) broker.
- **DS-10 [Med]** — `WithAddrs` no shuffle/rotate → every client stampedes
  `addr[0]` on recovery. *Location:* §6.1 L490; R10-11/**T66** L3120–3122.
  *Disposition:* **T66** (*already pulled into Phase 12*). Add a §6.1 note that
  DS-09 + DS-10 compound; ensure the chaos lane exercises a full-cluster restart.

### WS-7 — Observability of the duplicate budget

- **DS-14 [Med]** — no aggregate redelivery metric in v0.1. *Location:* §1 L68–75
  ("duplicate budget never invisible"); §6.2.1 L1028–1030; R10-16/**T71** L3137–3141.
  *Outcome:* `consumer_redelivered_total` (redelivery ratio is the #1 instability
  signal) covers the redelivery-class duplicate budget that `publisher_retry_total`
  does not. Today that budget is fleet-invisible until T71. *Acceptance:* T71's
  existing criteria; assert the counter increments on a `Redelivered()` delivery.
  *Disposition:* **T71**; **owner decision: pull into v0.1** (cheap; without it
  the §1 "never invisible" claim is false for the dominant duplicate source).

### WS-8 — RPC error-model design decision *(owner)*

- **DS-12 [Med, design-disagreement]** — `Replier` handler error collapses into
  `ErrCallTimeout`, indistinguishable from loss/slowness → forces unsafe blind
  retry that re-runs the handler and re-drops to DLX, never converging.
  *Location:* §6.7 L1829–1844; decision 16 L2705. *Outcome (owner decision):*
  either (a) accept and **document loudly** — `ErrCallTimeout` conflates
  rejection/loss/slowness; callers MUST NOT blind-retry without idempotency + a
  bounded budget; or (b) add an **opt-in structured-error reply mode** so
  deterministic rejections are distinguishable from timeouts (reverses part of
  decision 16). *Disposition:* amend §6.7 wording now (cheap); the structured-error
  mode is a feature → **new LATER-39** pending the owner decision.

---

## 6. Do-not-regress list (confirmed-correct, protect with tests)

These were verified **correct** and must not be broken by any amendment:

1. Publish ordering correctly disclaimed cross-goroutine and same-goroutine
   (§6.2 L841–847) — keep the warning.
2. `Concurrency(n>1)` "you have given up per-queue ordering" (§6.3 L1203–1210,
   decision 30) — correct; DS-06 adds the *failover* caveat without weakening this.
3. `Redelivered()` exposed on every delivery (§6.2.1 L1028) — the per-message
   signal; DS-14 only adds the aggregate.
4. Delayed-message node-failure §1 carve-out (§6.5 L1536–1552, R10-1) — the
   *template* DS-05's partition carve-out should mirror.
5. At-least-once is the contract; no "exactly-once" toggle (§6.2.1 L1015–1018,
   decision 25) — DS-01 sharpens the *remedy*, not the contract.
6. Reconnect barrier ordering (channel → redeclare → re-consume → callback) and
   the `ErrReconnecting`-on-ctx-cancel semantics (§6.1 L576–594, decision 27) —
   DS-02 adds a cap; it must not change the ordering.
7. Degraded-mode loudness (`ErrTopologyRedeclareFailed` permanent +
   `WithOnTopologyDegraded` + metric, §6.1 L595–612) — DS-08 questions blast
   radius, not the loudness; keep it loud.
8. `PublishBatch` per-channel order preservation **on the happy path** (§6.2
   L898–899) — DS-17 only caveats the channel-close-retry case.
9. RPC at-least-once double-reply trade-off documented on `Serve` (§6.7
   L1822–1827, decision 16) — DS-11 adds the *transport-level* orphan handling.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Amend SPEC | New/updated task | LATER.md | Owner decision? |
|---------|-----|-----------|------------------|----------|-----------------|
| DS-01 | High | ✅ §6.2.1 | **T84** (new) + example | — | — |
| DS-02 | High | ✅ §6.1/§6.2 | extend **T63** | — | ✅ pull-forward? |
| DS-03 | High | ✅ §6.1 | extend **T70** | — | ✅ pull-forward? |
| DS-04 | High | ✅ §6.3 | **T60** (Phase 12 — extend) | — | — |
| DS-05 | High | ✅ §6.1 (new subsec) | **T85** (new) | — | — |
| DS-06 | High | ✅ §6.3/§6.6, dec.30 | **T86** (new) | — | — |
| DS-07 | Med-High | ✅ §6.2 | **T87** (new) + coord. **T82** | — | — |
| DS-08 | Med | ✅ §6.1, dec.28 | **T88** (new) | maybe (per-entity redeclare) | ✅ per-entity vs doc? |
| DS-09 | Med (High@scale) | ✅ §6.1 note | **T62** | — | ✅ pull-forward? |
| DS-10 | Med | ✅ §6.1 note | **T66** (Phase 12) | — | — |
| DS-11 | Med | ✅ §6.7 | **T89** (new) | — | — |
| DS-12 | Med | ✅ §6.7 wording | — | **LATER-39** (new) | ✅ structured-error reply? |
| DS-13 | Med-High | ✅ §1/§6.3/§9 | **T90** (new) | — | — |
| DS-14 | Med | ✅ §1 copy | **T71** | — | ✅ pull-forward? |
| DS-15 | Low-Med | ✅ §6.2.1/§6.5 | fold into **T84** | — | — |
| DS-16 | Low-Med | ✅ §6.2.1 | fold into **T84** | — | — |
| DS-17 | Low | ✅ §6.2, dec.43 | **T91** (new; may ride T84) | — | — |

---

## 8. Suggested phasing (planner may revise — likely "Phase 13")

1. **Phase A — Verification gates (G1–G6).** Stand up the 3-node cluster + fault
   injector fixture (shared by G1/G3/G4/G5) and the half-alive-broker proxy
   (G2/DS-02). No SPEC edits to affected sections until the relevant gate returns.
2. **Phase B — Active §1 violations (WS-1: DS-02, DS-03).** Barrier cap (extend
   T63) + `Close` requeue decision (extend T70). These drive the NO-GO; they are
   the gate to flipping the lens verdict to GO-WITH-CHANGES.
3. **Phase C — Missing failure domains (WS-2: DS-05, DS-07).** Partition-mode
   subsection + queue-type confirm table + minority-partition window. SPEC +
   chaos tests; coordinate DS-07 with Lens 01's T82.
4. **Phase D — Overclaim corrections (WS-3: DS-06, DS-13, DS-15).** SAC ordering
   qualification, poison crash-loop honesty, UUIDv7 clarity. SPEC-first, locked
   by G1/G5 chaos tests.
5. **Phase E — Dedupe remedy + correctness windows (WS-4, WS-5: DS-01, DS-04,
   DS-11, DS-16, DS-17).** §6.2.1 rework + persistent-dedupe example; RPC orphan
   handling (G6); the small reorder-window notes.
6. **Phase F — Recovery-storm + observability decisions (WS-6, WS-7, WS-8).**
   Owner decisions: pull T62/T71 into v0.1; RPC error-model (DS-12) accept-and-doc
   vs structured-error feature (LATER-39).

Phases B–D carry the findings that drive the verdict
(**NO-GO → GO-WITH-CHANGES**): DS-02, DS-03, DS-05, DS-06 (with DS-01, DS-04,
DS-13 close behind).

---

## 9. Acceptance criteria for the whole effort

- [ ] The unbounded-barrier silent stall (DS-02) is closed: a half-alive-broker
      integration test shows `Publish` returns `ErrReconnecting` within a stated
      cap under `PublishTimeout=0` + `context.Background()`; SPEC states the cap,
      its default, the post-cap state, and that `ConfirmTimeout` does not cover
      the barrier. `goleak` clean.
- [ ] `Close` prefetched-but-undispatched behaviour (DS-03) is **defined and
      tested**: undispatched messages are nack-requeued (not dropped), verified
      by G2's harness; `consumer_shutdown_requeued_total` exists.
- [ ] A §6.1 partition-handling-modes subsection (DS-05) documents the client
      observation per `pause_minority`/`autoheal`/`ignore` (per G4) and carves
      `ignore` out of the §1 no-loss bar; README reliability copy updated.
- [ ] §6.2 carries a queue-type confirm-semantics table (DS-07) and names the
      quorum minority-partition → timeout → retry → duplicate window (per G3);
      no contradiction with Lens 01's T82 ack-vs-confirm wording.
- [ ] SAC is no longer described as "strict ordering with failover" without
      qualification (DS-06); G1's chaos test asserts the documented
      reorder/duplicate behaviour per queue-type per broker-version;
      `examples/ordered_consume/` README states the caveat.
- [ ] §1/§9 no longer claim a crash-safe poison bound for classic queues
      (DS-13); G5's crash-loop test demonstrates the unbounded reprocessing and
      the quorum counterpart shows the broker-side bound holds.
- [ ] §6.2.1 distinguishes publish-retry vs crash-redelivery duplicate classes
      (DS-01), states in-memory dedupe is crash-unsafe, ships a persistent-dedupe
      example, and corrects the UUIDv7 eviction wording (DS-15); the
      forced-close duplicate window is named (DS-16).
- [ ] RPC orphan-reply handling is specified and tested (DS-11, per G6); the
      double-verdict no-op guard (DS-04) lands with §6.3 wording + a `ConsumeRaw`
      double-call test.
- [ ] Owner decisions recorded: pull-forward of T62 (DS-09), T63 (DS-02), T70
      (DS-03), T71 (DS-14) into v0.1 vs explicit README limitation; RPC
      error-model (DS-12) disposition.
- [ ] Existing Phase 11/12 tasks (T60, T62, T63, T66, T70, T71, T82) have
      **amended** acceptance criteria; **no duplicate tasks created**.
- [ ] `LATER.md` updated only for genuine non-blockers (DS-12 → LATER-39 if the
      structured-error mode is deferred); entries follow `LATER-NN` format.
- [ ] `README.md` reflects the changed external reliability contract (partition
      carve-out, SAC caveat, redelivery metric).
- [ ] Tree green: `make test` (race + cover), `make lint`, the per-version
      integration lane, **and the new chaos/cluster lane**.

---

## 10. Open questions / decisions needed from the owner

1. **Pull-forward of active §1 violations.** DS-02 (T63 barrier cap) and DS-03
   (T70 `Close` requeue) are *active* §1 violations in v0.1. Pull into v0.1, or
   ship v0.1 with an explicit README limitation? The lens recommends pulling both.
2. **RPC error model (DS-12).** Accept the `ErrCallTimeout` collapse and document
   the blind-retry hazard loudly, or add an opt-in structured-error reply mode
   (reverses part of decision 16)? This is the resolved decision most worth
   reopening for the at-scale RPC case.
3. **Degraded-mode granularity (DS-08).** Keep the current per-connection
   blast radius (one bad queue def halts all publishing on the role) and document
   it, or invest in per-entity redeclare so only routes to the conflicting entity
   fail? Confirm that `ForceReconnect()` is documented as ineffective for genuine
   (non-transient) conflicts.
4. **Recovery-storm pull-forward (DS-09/DS-14).** Pull T62 (redeclare
   de-amplification) and T71 (`consumer_redelivered_total`) into v0.1? Both are
   cheap and both close a "billions/day-at-recovery" gap; T71 is required for the
   §1 "duplicate budget never invisible" claim to be true for the redelivery
   class.
5. **Chaos lane investment.** Approve standing up the 3-node-cluster +
   fault-injector fixture (new `chaos` build tag)? DS-05/06/07/13 cannot be
   falsified without it; a single-broker `testcontainers` lane is insufficient.

---

## 11. Out of scope for this plan

- The other validation lenses (01 protocol — now Phase 12; 03 interop, 04 EDA,
  05 SRE, 06–13). Cross-lens de-duplication: DS-04≈Lens-01 RMQ-08/T60,
  DS-07≈RMQ-25/T82, DS-10≈RMQ-10/T66 — these are *shared* findings and rise in
  priority for being seen by multiple lenses; extend the shared task, do not
  re-file.
- Stream-protocol semantics (v0.2 per decision 24).
- The async/bounded-in-flight publish API (LATER-34 / R10-18) — adjacent to
  DS-02's pool-exhaustion-under-confirm-latency cascade but a separate
  architecture decision.
- Any change to the API design north stars (§10) beyond the corrections above.
