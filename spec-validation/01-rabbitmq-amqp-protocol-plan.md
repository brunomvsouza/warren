# Plan Input — Remediate RabbitMQ / AMQP 0-9-1 Protocol Findings (Lens 01)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the RabbitMQ
> 3.13 LTS + 4.x / AMQP 0-9-1 protocol lens (`spec-validation/01-rabbitmq-amqp-protocol.md`).
> It enumerates confirmed defects, their evidence, the desired outcome, and an
> acceptance test for each, grouped into workstreams and sequenced by dependency.
> Produce a phased implementation plan that sequences the SPEC amendments,
> regression tests, task conversions, and `LATER.md` entries below.

---

## 1. Objective

Bring `SPEC.md` and the v0.1 implementation surface into protocol-correct
alignment with **both** supported broker generations (RabbitMQ 3.13 LTS and
4.x), closing the defects that make the library's own "no silent X" reliability
bar (§1) and §9 success criteria unachievable as currently specified.

Concretely, the plan must:
1. Correct the confirmed **factual errors** in `SPEC.md` (amend SPEC first, per
   `CLAUDE.md` "Anything contradicting SPEC must amend SPEC first").
2. Add **regression tests** (real-broker, per-version where behaviour diverges)
   that lock each corrected claim so it cannot silently regress.
3. Decide and record disposition for the **deferred v0.1 silent-failure
   defects** already tracked as Phase 11 tasks (pull forward vs keep deferred).
4. Resolve the **verification gates** (open questions) that block correct
   wording of several amendments.
5. Route every non-blocking finding to `LATER.md` with the required format.

This plan is **remediation of an existing spec**, not a new feature. Treat each
finding as a test-first work item.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. Section/line anchors below are
  against the current revision (post Rev 10). Re-confirm line numbers before
  editing; sections referenced: §1, §6.1, §6.2, §6.2.1, §6.3, §6.5, §6.6, §6.7,
  §6.8, §6.9, §9, §10 (decisions 10/12/14/17/20/34/36/52, Rev 6, Rev 10
  R10-1..R10-18).
- `tasks/plan.md` — phased plan. Several findings already map to Phase 11 tasks
  **T57–T72** (see routing matrix §7). Do not create duplicate tasks; amend the
  existing ones' acceptance criteria.
- `tasks/todo.md` — granular checklist tied to `plan.md`.
- `LATER.md` — deferred non-blockers (existing entry **LATER-34** = R10-18).
- The validation report this brief is derived from:
  `spec-validation/01-rabbitmq-amqp-protocol.md` (the prompt) and the findings
  (IDs `RMQ-01`..`RMQ-31`).

---

## 3. Constraints & working agreements (planner must honour)

- **TDD is a hard discipline.** Every behaviour/contract change: failing test
  first (real broker where the claim is broker-dependent), then the smallest
  amendment. For a wording-only SPEC fix, the "test" is the corrected text plus,
  where a runtime behaviour is implied, an integration assertion.
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in
  the same PR (or earlier), then implementation follows.
- **Per-version testing.** Any claim that differs between 3.13 and 4.x must be
  asserted on **both** brokers in the integration matrix (§9 already requires
  the suite to pass on both — extend it to *differential* assertions, not just
  "passes").
- **Real broker for protocol claims.** Claims like `x-death` reason tokens,
  default delivery-limit, alarm-vs-nack, and the return/ack ordering invariant
  **cannot** be validated by a mock or a conformance stub. Use
  `testcontainers-go` integration lane against real 3.13 and 4.x images.
- **Docs in English** (`CLAUDE.md`). All SPEC/LATER/plan/todo text in English.
- **Module path** `github.com/brunomvsouza/warren`; `errorlint` is on (compare
  with `errors.Is`/`errors.As`).
- **Choke points:** broker-error translation goes through `internal/amqperror`;
  the `x-death` parsing lives in `internal/headers`. Corrections to reason
  tokens / counting land there.
- **README sync:** if a correction changes the external contract (error
  classification, defaults, codecs), update `README.md` "Available now / On the
  roadmap" per the project rule.

---

## 4. Pre-work: verification gates (sequence FIRST — they gate wording)

These open questions must be resolved on a live broker before the dependent
amendments can be worded correctly. Each is a short spike (declare + observe on
real 3.13 and 4.x). Gate ownership: assign to the integration lane.

| Gate | Question | Blocks | How to resolve |
|------|----------|--------|----------------|
| **G1** | Exact `x-death` reason atom for delivery-limit exceedance — `delivery_limit` (underscore) vs `delivery-limit` (hyphen)? | RMQ-01 | Quorum queue, `x-delivery-limit=1`, force a redelivery past the limit, read the dead-lettered message's `x-death` header; assert the literal string on 3.13 **and** 4.x. |
| **G2** | Does `DeathCount()` need to sum the `count` sub-field (RabbitMQ accumulates one entry per `(queue,reason)` with an incrementing `count`)? | RMQ-02 | Dead-letter the *same* message N times via a DLX-back-to-source binding; inspect the `x-death` array length vs the `count` sub-field. |
| **G3** | Do RabbitMQ **4.0+ classic queues** honour `x-delivery-limit`? (Spec asserts they do not "as of 4.x".) | RMQ-20, §6.3/§6.6 wording | Classic queue + `x-delivery-limit`, drive redeliveries, observe whether the broker dead-letters. Test on the exact 4.x minor(s) in CI. |
| **G4** | Is `x-overflow=reject-publish-dlx` valid on a **quorum** queue, and is `at-least-once` strategy rejected with `drop-head`? | RMQ-05 | Declare permutations of {quorum, overflow, at-least-once} and record which declares succeed/fail and the broker error. |
| **G5** | Broker `max_message_size` defaults on the exact 3.13 and 4.x minors in CI (expected 128 MiB / 16 MiB). | RMQ-12 | `rabbitmqctl environment` / publish-just-over-default and observe rejection. |
| **G6** | Does a non-zero `basic.qos` `prefetch_size` get **rejected** (NOT_IMPLEMENTED) or **ignored**? | RMQ-14 | Send `basic.qos(prefetch_size=1)` and observe channel close vs silent ignore on 3.13 and 4.x. |

**Planner note:** G1 and G2 gate the highest-value workstream (WS-1). Front-load
them.

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets. "Disposition" = where the change lands (see routing matrix §7).

### WS-1 — Poison-protection correctness *(highest value; gated by G1, G2)*

The "preferred" (quorum + `x-delivery-limit`) and "fallback" (consumer-side
Counter A via `x-death`) poison bounds are both non-functional as specified.

- **RMQ-01 [High]** — `x-death` reason token. *Location:* §6.3 L1091–1093, L1279;
  decision 34 L2869. *Outcome:* matched token corrected to the value G1 confirms
  (expected `delivery_limit`); parser optionally normalises `-`↔`_`. *Acceptance:*
  integration test on a real 4.x broker asserts `DeathCount()` increments when a
  quorum queue dead-letters on delivery-limit; `DeathCountByReason` returns the
  correct value for the real atom. *Disposition:* amend SPEC + fix
  `internal/headers` parser + new test task.
- **RMQ-02 [High]** — `DeathCount()` semantics. *Location:* §6.3 L1091, L1277–1283.
  *Outcome:* spec states "sum of the `count` sub-field across `x-death` entries
  whose reason ∈ {rejected, <delivery atom>}". *Acceptance:* test dead-letters
  the same message N times; assert `DeathCount() == N`. *Disposition:* amend SPEC
  + fix parser + test.
- **RMQ-06 [High]** — quorum `DeliveryLimit==0` is version-dependent. *Location:*
  §6.3 L1255–1265; §6.6 L1606–1614; R10-2. *Outcome:* analysis + `validate()`
  warning conditionalised on broker version (read from `connection.start`
  server-properties): 4.x → silent drop at 21; 3.13 → **unbounded loop**.
  *Acceptance:* per-version poison-loop integration test shows the documented
  behaviour on each broker; warning text matches the connected version.
  *Disposition:* amend SPEC + extend T58 (the `validate()` warning task).

### WS-2 — Publish failure-mode classification

- **RMQ-03 [High]** — disk/memory alarm ≠ `basic.nack`. *Location:* §6.2 L853–855;
  L1878; decision 20 L2743–2746; IsTransient note L1949–1951. *Outcome:* remove
  "disk/memory alarm" from `ErrPublishNacked` causes (it is the
  `connection.blocked` → `ErrConnectionBlocked` path, already specified); keep
  `reject-publish`/`reject-publish-dlx`; optionally add "rare broker internal
  error / queue-process crash". *Acceptance:* integration test raises a disk
  alarm and asserts `ErrConnectionBlocked` (not `ErrPublishNacked`); a
  `reject-publish` test asserts `ErrPublishNacked`. *Disposition:* amend SPEC.
- **RMQ-04 [High]** — 311 CONTENT_TOO_LARGE misclassified transient. *Location:*
  §6.8 L951/L986/L1881; `IsTransient`/`PublishRetry` trigger lists. *Outcome:*
  move 311 to `IsPermanent`; remove 311 from `PublishRetry` triggers; consistent
  with client-side `ErrMessageTooLarge` (permanent). *Acceptance:* unit test
  asserts `IsPermanent(311-wrapped) == true` and that `PublishRetry` does not
  retry a 311. *Disposition:* amend SPEC + fix classifier in
  `internal/amqperror`/`errors.go` + test.

### WS-3 — DLX / topology-declare correctness *(gated by G4)*

- **RMQ-05 [High]** — `at-least-once` strategy requires `reject-publish` overflow.
  *Location:* decision 52 L2612–2615; §6.6 L1645/L1653/L1659. *Outcome:* when
  injecting `x-dead-letter-strategy: at-least-once`, also force
  `x-overflow=reject-publish` (or only inject when `Overflow==reject-publish`,
  else warn/reject); document the source-queue memory cost of at-least-once.
  *Acceptance:* declaring quorum + DLX with default overflow now produces a valid
  broker declare (integration); `validate()` covers the combination.
  *Disposition:* amend SPEC + fix `topology.go` expansion + test.
- **RMQ-07 [High]** — unbounded auto `<source>.dlq`. *Location:* §6.6; R10-10/T65.
  *Outcome:* declare the DLQ durable (quorum-capable) with default
  `x-max-length`/`x-message-ttl` bounds. Compounds RMQ-05 (an at-least-once
  source that cannot drop + a DLQ that cannot bound backs up the source).
  *Acceptance:* DLQ declared with bounds; a fill test shows the DLQ caps rather
  than filling disk. *Disposition:* this is **T65** — amend its acceptance
  criteria; planner decides whether to pull T65's DLQ-bounds half into v0.1.
- **RMQ-20 [Medium]** — quorum structural validation missing (T64). *Location:*
  R10-9. *Outcome:* `validate()` rejects locally: quorum must be `Durable`; no
  `Exclusive`/`AutoDelete`; no `x-max-priority`; note `x-queue-mode`/lazy ignored;
  **streams also require `Durable`** (currently unchecked); resolve G3/G4
  interactions. *Acceptance:* unit tests per rejected combination. *Disposition:*
  this is **T64** — extend its scope with the enumerated rules above.

### WS-4 — Deferred v0.1 silent-failure defects *(decision: pull forward vs keep in Phase 11)*

Each already has a Phase 11 task and each violates §1 "no silent X" in the
shipping v0.1. The planning decision is **disposition + sequencing**, plus a
README/roadmap honesty note if kept deferred.

- **RMQ-08 [High]** — no idempotency guard on `Delivery.Ack/Nack` → double-ack →
  406 closes channel → all in-flight handlers die. *Task:* **T60** (R10-5).
- **RMQ-09 [High]** — channel-level close (404/406) with TCP alive → consumer
  dies silently. *Task:* **T61** (R10-6).
- **RMQ-10 [High]** — `WithAddrs` no shuffle/rotate → reconnect thundering-herd.
  *Task:* **T66** (R10-11).

*Outcome:* the planner recommends, for each, "pull into v0.1" or "keep in Phase
11 + document as a v0.1 limitation in README roadmap." *Acceptance:* the
existing task acceptance criteria already encode the fix; this WS only re-orders
and adds the roadmap-honesty note if deferred.

### WS-5 — Throughput honesty

- **RMQ-11 [High, design-disagreement]** — sync-confirm-per-channel ceiling.
  *Location:* §9 L2471–2479; R10-18/LATER-34. *Outcome (owner decision needed):*
  either (a) qualify §9 numbers as local-broker/sub-ms-RTT and add a remote-RTT
  projection (`throughput ≈ pool / RTT`), or (b) promote LATER-34 (async publish
  with bounded in-flight window) into v0.1. *Acceptance:* §9 states the topology
  the numbers were obtained against and the remote-RTT ceiling. *Disposition:*
  amend §9 wording now (low cost); the async-API decision stays in `LATER.md`
  (LATER-34) unless the owner promotes it to a task.

### WS-6 — Sizing / limits factual fixes *(gated by G5, G6)*

- **RMQ-12 [Medium]** — "~512 MiB works without further config" false. *Location:*
  §6.1 L707–710. *Outcome:* state per-version broker `max_message_size` defaults
  (128 MiB 3.13 / 16 MiB 4.0+ per G5); ">default needs broker-side
  `max_message_size` raised"; reconcile with the L724–726 pointer-out guidance.
  *Disposition:* amend SPEC.
- **RMQ-13 [Medium]** — 131072 mislabelled "AMQP-spec minimum". *Location:* §6.1
  L705 (vs L717). *Outcome:* "131072 (128 KiB) is the sensible default; **4096**
  is the AMQP-spec minimum." *Disposition:* amend SPEC (one line).
- **RMQ-14 [Medium]** — "RabbitMQ ignores `prefetch_size`". *Location:* §6 L449–450;
  §6.3 L1145. *Outcome (per G6):* state RabbitMQ **rejects** non-zero
  `prefetch_size` (NOT_IMPLEMENTED) and the library always sends `0` / drops the
  user's `PrefetchBytes` **client-side**. *Acceptance:* a test asserts the
  library never forwards a non-zero `prefetch_size`. *Disposition:* amend SPEC +
  confirm client behaviour + test.

### WS-7 — Version-divergence documentation (3.13 ↔ 4.x)

Mostly documentation/caveats; low code risk but load-bearing for "supports both".

- **RMQ-17 [Medium]** — Khepri (4.1 default) makes declares Raft-quorum ops that
  can block/fail during partition recovery; amplifies the unbounded reconnect
  barrier. *Location:* §6.1, §6.6. *Outcome:* add Khepri caveat; make the barrier
  cap (**T63**/R10-8) version-aware. *Disposition:* amend SPEC + extend T63.
- **RMQ-19 [Medium]** — CloudEvents-binary 0-9-1⇄1.0 header bridge differs 3.13
  (plugin) vs 4.0 (native). *Location:* §6.9 L2011–2019; decision 4. *Outcome:*
  version-pin or test the `cloudEvents:` header round-trip on both; coordinate
  with the interop lens (03). *Disposition:* amend SPEC + interop test.
- **RMQ-21 [Medium]** — `rabbitmqadmin` v2 rewrite (4.0). *Location:* §9 L2510;
  §6.1 L668. *Outcome:* pin verification to the management HTTP API
  (version-stable) or specify per-version CLI. *Disposition:* amend §9 + test
  harness.
- **RMQ-23 [Low]** — "mirrored / quorum queues" stale (mirroring removed in 4.0).
  *Location:* §6.2 L896. *Outcome:* "quorum queues (or, on 3.13, mirrored classic
  queues)". *Disposition:* amend SPEC.
- **RMQ-30 [Low]** — non-durable classic queues are a 4.0 deprecated feature
  (operator-disablable) → `Queue{Durable:false}` declare can fail on 4.x.
  *Outcome:* add a caveat. *Disposition:* amend SPEC.
- **RMQ-31 [Low, open]** — mixed-version rolling-upgrade cluster behaviour
  unaddressed. *Outcome:* a short documented caveat. *Disposition:* amend SPEC or
  `LATER.md`.

### WS-8 — API / contract precision

- **RMQ-15 [Medium]** — PublishBatch+Mandatory undefined for duplicate
  user-supplied MessageIDs. *Location:* decision 14 L2689–2697; §6.2 L886–891.
  *Outcome:* validate MessageID uniqueness within a mandatory batch (or document
  per-slot result as undefined). *Acceptance:* test with duplicate IDs in a
  mandatory batch. *Disposition:* amend SPEC + (optional) validation + test.
- **RMQ-16 [Medium]** — R10-3 invariant silently depends on amqp091-go internals.
  *Location:* §6.2 L872–885. *Outcome:* state the upstream dependency, pin the
  amqp091-go version, make **T59** a frame-level/real-broker regression (not a
  mock). *Disposition:* amend SPEC + extend T59.
- **RMQ-18 [Medium]** — sentinel list doesn't separate channel-level vs
  connection-level codes. *Location:* §6.8 L1909–1971. *Outcome:* tag each
  sentinel with scope (channel: 311/403/404/405/406; connection: 320/402/501–505/
  506/530/540/541); ties to R10-6/T61 recovery path. *Disposition:* amend SPEC.
- **RMQ-24 [Low]** — decision 17 says default connections "1"; §6.1 + decision 36
  say "2". *Location:* §10 L2715. *Outcome:* update stale decision 17.
  *Disposition:* amend SPEC.
- **RMQ-25 [Low]** — "ack = merely queued for routing" understates confirms.
  *Location:* §6.2 L892–897. *Outcome:* note that for persistent/durable/quorum,
  the confirm implies disk-persist / majority-commit. *Disposition:* amend SPEC.
- **RMQ-26 [Low]** — sub-ms `Expiration` silently rounds to `"0"` = immediate
  expiry. *Location:* §6.5 L1483–1485. *Outcome:* reject non-zero `<1ms`
  durations as `ErrInvalidMessage` (or document the silent-drop footgun loudly).
  *Disposition:* amend SPEC + (optional) validation + test.
- **RMQ-27 [Low]** — Priority "0–9 by convention" + missing "quorum queues have no
  priority support". *Location:* §6.5 L1475–1479. *Outcome:* clarify
  `x-max-priority` range (≤10 recommended, up to 255) and note quorum has no
  priority. *Disposition:* amend SPEC.
- **RMQ-28 [Low]** — exclusive reply queue auto-deleted on disconnect → must
  redeclare on reconnect. *Location:* §6.7 L1786–1788. *Outcome:* specify the
  redeclare-on-reconnect step for `UseExclusiveReplyQueue()`. *Disposition:*
  amend SPEC.
- **RMQ-29 [Low]** — quorum "prefetch floor 16" is a heuristic, not a broker
  constant. *Location:* §6.3 L1187. *Outcome:* reword as guidance, not a
  protocol-enforced floor. *Disposition:* amend SPEC.

---

## 6. Do-not-regress list (confirmed-correct, protect with tests)

These were verified **correct** and must not be broken by any amendment;
where a regression test does not yet exist, add one:

1. **R10-3 return/ack ordering** (single-goroutine demux + unbuffered
   `NotifyReturn`) — correct against amqp091-go's single synchronous reader.
   Protect via T59 (see RMQ-16).
2. `basic.return` precedes `basic.ack` for unroutable mandatory publishes.
3. `multiple=true` resolves all tags ≤ the given tag.
4. `ChannelQoS` rename reflects RabbitMQ's per-channel `global=true` semantics.
5. `UserID` 406-on-divergence + client-side check + auth-rewrite caveat.
6. Dead-letter cycle detection can fire before Counter A reaches the limit.
7. `DeliveryMode` wire mapping (zero→2 persistent).
8. `Expiration` shortstr-ms / `"0"` = immediate (modulo RMQ-26).
9. direct reply-to: no-ack + consume-before-publish + channel-scoped.
10. Heartbeat min(client,server) → ~2× detection; frame-max concept.
11. `Topology.Declare` order exchanges→queues→bindings; DLX need not pre-exist.
12. Default delivery-limit value (20) and its 4.x attribution.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Amend SPEC | New/updated task | LATER.md | Owner decision? |
|---------|-----------|------------------|----------|-----------------|
| RMQ-01 | ✅ §6.3, dec.34 | parser fix + test (new) | — | — |
| RMQ-02 | ✅ §6.3 | parser fix + test (new) | — | — |
| RMQ-03 | ✅ §6.2, dec.20 | integration test (new) | — | — |
| RMQ-04 | ✅ §6.8, §6.2 | classifier fix + test (new) | — | — |
| RMQ-05 | ✅ dec.52, §6.6 | topology.go fix + test (new) | — | — |
| RMQ-06 | ✅ §6.3/§6.6 | extend **T58** | — | — |
| RMQ-07 | ✅ §6.6 | **T65** (pull forward?) | — | ✅ pull-forward? |
| RMQ-08 | — | **T60** | — | ✅ pull-forward? |
| RMQ-09 | — | **T61** | — | ✅ pull-forward? |
| RMQ-10 | — | **T66** | — | ✅ pull-forward? |
| RMQ-11 | ✅ §9 wording | — | LATER-34 (exists) | ✅ async API? |
| RMQ-12 | ✅ §6.1 | — | — | — |
| RMQ-13 | ✅ §6.1 | — | — | — |
| RMQ-14 | ✅ §6/§6.3 | client behaviour test (new) | — | — |
| RMQ-15 | ✅ §6.2, dec.14 | optional validation + test | — | — |
| RMQ-16 | ✅ §6.2 | extend **T59** | — | — |
| RMQ-17 | ✅ §6.1/§6.6 | extend **T63** | — | — |
| RMQ-18 | ✅ §6.8 | — | — | — |
| RMQ-19 | ✅ §6.9 | interop test (coord. lens 03) | — | — |
| RMQ-20 | ✅ — | extend **T64** | — | — |
| RMQ-21 | ✅ §9 | test harness | — | — |
| RMQ-22 | — | **T68/T69** (deferred) | — | ✅ defer ok? |
| RMQ-23 | ✅ §6.2 | — | — | — |
| RMQ-24 | ✅ dec.17 | — | — | — |
| RMQ-25 | ✅ §6.2 | — | — | — |
| RMQ-26 | ✅ §6.5 | optional validation + test | — | — |
| RMQ-27 | ✅ §6.5 | — | — | — |
| RMQ-28 | ✅ §6.7 | — | — | — |
| RMQ-29 | ✅ §6.3 | — | — | — |
| RMQ-30 | ✅ §6.5/§6.6 | — | — | — |
| RMQ-31 | ✅ or LATER | — | maybe | ✅ scope? |

---

## 8. Suggested phasing (planner may revise)

1. **Phase A — Verification gates (G1–G6).** Live-broker spikes on 3.13 + 4.x.
   No SPEC edits until these return; they fix the *wording* of WS-1/3/6.
2. **Phase B — Poison-protection correctness (WS-1).** Highest value; the
   "preferred" path is silently broken. SPEC + `internal/headers` + per-version
   tests.
3. **Phase C — Publish failure classification + DLX declare (WS-2, WS-3).**
   `ErrPublishNacked`/311 fixes; at-least-once + `reject-publish`; DLQ bounds.
4. **Phase D — Throughput & sizing honesty (WS-5, WS-6).** Low-cost SPEC wording
   that prevents production surprises.
5. **Phase E — Deferred silent-failure decision (WS-4).** Owner decides
   pull-forward vs roadmap-documented deferral for T60/T61/T66 (+T65 bounds).
6. **Phase F — Version-divergence docs + API precision (WS-7, WS-8).** Caveats,
   sentinel scoping, smaller corrections; can run in parallel with C–E.

Phases B and C carry the findings that drive the lens verdict
(**GO-WITH-CHANGES**): RMQ-01, RMQ-02, RMQ-05 (with RMQ-03/RMQ-04 close behind).

---

## 9. Acceptance criteria for the whole effort

- [ ] Every confirmed factual-error finding (RMQ-01..06, 12, 13, 14, 23, 24, 25)
      is corrected in `SPEC.md`, in English, with no remaining internal
      contradiction (re-grep for the old wording).
- [ ] `DeathCount()` increments correctly on a real 4.x broker for a
      delivery-limit dead-letter and for N repeated rejections (RMQ-01/02).
- [ ] Disk-alarm path surfaces `ErrConnectionBlocked`, not `ErrPublishNacked`,
      in an integration test (RMQ-03).
- [ ] 311 is `IsPermanent` and not retried by `PublishRetry` (RMQ-04).
- [ ] Quorum + DLX + default overflow produces a **valid** broker declare
      (RMQ-05); `validate()` covers the combination.
- [ ] The integration matrix asserts the **divergent** behaviours on **both**
      3.13 and 4.x (delivery-limit default, `max_message_size`, x-death atom,
      classic-queue delivery-limit), not just "suite passes" (RMQ-06, G1, G3, G5).
- [ ] §9 throughput numbers state their topology/RTT assumptions (RMQ-11).
- [ ] Each deferred defect (RMQ-07/08/09/10/22) is either pulled into v0.1 or
      explicitly listed as a v0.1 limitation in the README roadmap (no implicit
      Phase-11-only silent-failure modes).
- [ ] Existing Phase 11 tasks (T58, T59, T63, T64, T65) have amended acceptance
      criteria; no duplicate tasks created.
- [ ] `LATER.md` updated only where a finding is a genuine non-blocker; entries
      follow the `LATER-NN` format.
- [ ] `README.md` reflects any changed external contract (error classification,
      defaults, codecs).
- [ ] Tree green: `make test` (race + cover), `make lint`, and the per-version
      integration lane.

---

## 10. Open questions / decisions needed from the owner

1. **WS-4 pull-forward.** Should the deferred silent-failure defects (T60
   double-ack guard, T61 channel-level recovery, T66 addr shuffle, T65 DLQ
   bounds) move into v0.1, or stay in Phase 11 with an explicit README
   limitation? Each violates §1 in v0.1.
2. **RMQ-11 async API.** Promote LATER-34 (async/bounded-in-flight publish) into
   v0.1, or keep the §9 numbers and document the `pool/RTT` ceiling honestly?
3. **Scope of version-conditional behaviour.** Is reading broker version from
   `connection.start` server-properties (to drive the RMQ-06 warning and other
   version-divergent paths) acceptable as a v0.1 mechanism?
4. **RMQ-22** alternate-exchange (T68) / e2e bindings (T69) — confirm deferral is
   acceptable for v0.1 and add the roadmap note.

---

## 11. Out of scope for this plan

- The other validation lenses (02 distributed-systems, 03 interop, 04 EDA, 05
  SRE, 06–13). Cross-lens de-duplication happens after all reports are in; some
  findings here (RMQ-01/02 poison, RMQ-07 DLQ) are expected to resurface from
  those lenses and rise in priority.
- Stream-protocol semantics (v0.2 per decision 24).
- Any change to the API design north stars (§10) beyond the corrections above.
