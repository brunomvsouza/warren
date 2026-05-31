# Plan Input — Remediate Developer-Experience & Documentation Findings (Lens 12)

> **Lens:** Developer Experience (DX) & Documentation — "can a competent Go developer go from
> `go get` to a working, *correct* publish/consume in the first hour without reading the source?"
> (`spec-validation/12-dx-documentation.md`).
> **Persona:** a DX / technical-documentation specialist who has shipped popular open-source Go
> libraries and judges the library by its first hour. For a reliability library, **the
> documentation *is* the product** — a guarantee the user does not understand is a guarantee they
> will violate. Every footgun is a documentation obligation; every "the user MUST…" is an
> onboarding test; a load-bearing warning that lives only in `SPEC.md` (not in the godoc at the
> call site) is, for DX purposes, **invisible**.
> **Verdict:** **GO-WITH-CHANGES** — **no Blocker.** The API itself is well-designed and the
> hardest prose is excellent (the §6.3 poison/`AutoAck` treatment and the §6.5 `UserID`/`Delay`
> field notes are model documentation; the six shipped examples each carry a "What this example
> demonstrates" godoc header). Most placement work is **already owned** — the godoc sweeps
> (T10/T34/T40), the `AutoAck` verbatim-godoc exemplar (T35), the `Delay` durability godoc (T57),
> the dedupe **example** with an outcome-asserting smoke test (T38b), the README quickstart (T39).
> What this lens exposes is **not a correctness bug — it is a pit-of-success gap**: the single most
> important contract a user must internalise (**at-least-once → you MUST dedupe**, §6.2.1) is a
> SPEC section with **no mandate to appear in the godoc on `Publish` / `Consume` / `PublishRetry`**
> where the user actually is; the package's **conceptual overview** (`doc.go`) is an
> **undefined placeholder** that no SPEC section scopes, so the mental model (role-split pool,
> at-least-once, topology-separate, reconnect barrier) is taught nowhere a hurried reader will
> look; the **footgun → godoc routing is ad-hoc** (only `AutoAck` carries an explicit
> "godoc repeats this verbatim" mandate); and the **two most-copied code snippets are broken**
> (the README quickstart handler will not compile; the §6.2.1 canonical dedupe snippet references
> an out-of-scope variable). **17 findings (DX-01..DX-17); 5 gates (XG-1..XG-5); 7 net-new tasks
> (T157–T163); 4 cross-lens extensions (T10/T34/T39/T40); confirmations (T35/T38b/T57/T58, the
> example godoc headers, the redaction-in-logs contract); LATER-48.**

---

## 1. Objective

Make `warren` a **pit of success**, not merely a complete reference: ensure a competent Go
developer can dial → declare → publish → consume → handle a failure → observe **correctly in the
first hour without reading the source**, by routing every load-bearing warning to the godoc **at
the call site**, by teaching the mental model **once** in `doc.go`, by making the most-copied
snippets **compile**, and by filling the **runnable-example** gaps for the paths users most need
to get right. The bar is the developer's first hour, and it is binary:

- **A load-bearing warning that lives only in `SPEC.md` is a finding.** The user hovers a symbol
  in their IDE or reads pkg.go.dev; they do not open `SPEC.md` mid-keystroke. The at-least-once /
  must-dedupe contract (§6.2.1 L1013-1068), the `UserID` 406-channel-close footgun (§6.5
  L1487-1513), `ConfirmTimeout(0)` "discouraged" (§6.2 L972-978), and `PrefetchBytes()` no-op
  (§6.3 L1145) are all SPEC prose with **no explicit mandate to appear verbatim in the godoc on
  the exported identifier** — only `AutoAck()` carries that mandate ("the godoc will repeat this
  verbatim", §6.3 L1351). The placement policy is ad-hoc; it must be **systematic**.

- **A teaching gap the only-reference-complete plan won't close is a finding.** §4 lists `doc.go`
  as the "top-level godoc package overview" (L181) but **no SPEC section defines what it must
  cover**; the shipped `doc.go` is a 12-line feature list ending "See SPEC.md". For a library
  this opinionated — at-least-once-or-you-lose-correctness, topology-declared-separately,
  reconnect-is-a-barrier, role-split-pool — a weak `doc.go` is **guaranteed misuse**. The mental
  model must be teachable in one place the user will actually open.

- **A broken or unmeasurable promise is a finding.** §1 promises "publish in <10 lines, consume
  in <15" (L94-97); the smallest runnable artifact is the ~40-line README quickstart (which
  **swallows every error with `_`** and whose consume handler — `func(ctx, o *Order)` — **does not
  compile** against the value-typed `Handler[M]`) and the 133-line `examples/publish/main.go`. The
  §6.2.1 canonical dedupe snippet references `d.MessageID()` where `d` is **out of scope**
  (L1049). The §9 README criterion demands a quickstart covering TLS, multi-addrs, OTel, and DLX
  (L2442-2443) that the current quickstart does not satisfy. The first code a user copies must
  compile and be honest.

The net-new value is the **pit-of-success layer no prior lens owns.** The library-design lens
(06) owns the *shape* of the API (generics, builders, error model, semver); the SRE lens (05)
owns *operability*; but **none** owns the developer's **first-hour learning path** — the
conceptual overview, the footgun→godoc routing *policy*, the migration story for the prior
library's users, the production-tuning discoverability, the error-message-names-the-remedy
contract, and the runnable examples for the operational hard cases. **There is no Blocker:** the
library *works* and a determined reader *can* learn it from `SPEC.md`; every finding is
documentation, placement, a compile-correctness fix, or a runnable example — all valid, none a
silent loss/duplicate/leak defect.

---

## 2. Source of truth & references

- **SPEC §1** (L21-106): the reliability bar (L37-86) the docs must make legible; "Success looks
  like" — publish in **<10 lines**, consume in **<15** (L94-97); "No silent failure modes:
  forgetting to ack is impossible" (L102-103) — the promise the docs must keep.
- **SPEC §4** (L170-256): `doc.go` "top-level godoc package overview" (L181, **content
  undefined**); the `examples/` layout (L239-251) lists **eleven** examples — `publish`, `consume`,
  `batch_consume`, `batch_publish`, `rpc`, `delayed`, `deadletter`, `topology`,
  `idempotent_consume`, `ordered_consume`, `otel`; **six** exist on disk today (`rpc`, `delayed`,
  `idempotent_consume`, `ordered_consume`, `otel` are roadmap); `CHANGELOG.md` "Keep a Changelog
  format" (L179, **file does not exist yet**).
- **SPEC §5** (L260-415): handler signature **`func(ctx, M) error` — value `M`** (L272, confirmed
  in `consumer.go:113` `type Handler[M any] func(ctx context.Context, msg M) error`); the publish
  example (L292-369, full `main` + imports + signal handling); consume (L371-391); consume-raw
  (L393-403).
- **SPEC §6.1** (L467-763): the **~20 `ConnectionOption`s** table (L487-513) + the
  publisher/consumer-connection / channel-pool / prefetch tuning prose (L530-563) — production
  tuning is **scattered**, no single page; `n=1` warnings logged at `Dial` (L535-537/L542),
  not in godoc; pointer-out-for-size (L725; 100 MiB at L2952).
- **SPEC §6.2 / §6.2.1** (L764-1068): `ConfirmTimeout(0)` "documented as discouraged" (L976-978);
  `PublishRetry` "Duplicate hazard — load-bearing… Consumers MUST be idempotent" (L996-1008); the
  **at-least-once + canonical dedupe** section (L1013-1068) — the **single most important contract**
  — with the snippet bug at L1049 (`d.MessageID()` where the `Consume` handler only binds `o`);
  `examples/idempotent_consume/` referenced (L1067-1068, owned by T38b).
- **SPEC §6.3** (L1070-1369): the `AutoAck()` warning with the **only verbatim-godoc mandate**
  (L1345-1368, "the godoc on `AutoAck()` will repeat this verbatim" L1351); `PrefetchBytes()`
  no-op (L1145); `ChannelQoS()` (L1146); the `Prefetch < Concurrency` `Build`-time warning
  (L1178-1179); the quorum `DeliveryLimit==0` → broker-default-20 silent-drop (L1255-1265, warning
  owned by T58); "**Always wire `OnCancel` in production**" (L1342).
- **SPEC §6.5** (L1431-1559): `UserID` 406-channel-close footgun (L1487-1513); `Delay` durability
  warning "load-bearing" (L1536-1552, godoc owned by T57/R10-1); `DeliveryMode` **Persistent is
  the zero-value default** (L1450/L1554-1556).
- **SPEC §6.8** (L1863-1980): the sentinel `var` block (L1866-1930) — every message is **terse and
  names the symptom, not the remedy** ("warren: channel pool exhausted", "warren: publisher
  confirm timeout"); the §6.2 prose names the knob (`WithChannelPoolSize` /
  `WithPublisherConnections`, L957-959) but the **error value does not**; the `IsTransient` /
  `IsPermanent` godoc is good (L1947-1971).
- **SPEC §6.9** (L1981-2208): does **not** define `doc.go` content; codec/metrics/otel/`amqptest`
  prose is strong.
- **SPEC §7** (L2271-2329): executable-examples policy — every checkpoint ships a runnable example
  with a top-of-file godoc block (L2312-2313, **done well**); the CI gate runs each example
  "**end-to-end … as a smoke test**" (L2314-2320) — **runs to completion, does not mandate an
  outcome assertion** (T38b is the lone exception that asserts duplicate-observed-once).
- **SPEC §8** (L2333-2423): "**Document every exported identifier with a godoc comment**" (L2344)
  — present but generic; there is **no** "load-bearing warnings appear verbatim in the godoc at
  the call site" boundary; "Add a `CHANGELOG.md` entry for every user-visible change" (L2343,
  references a file that does not exist yet).
- **SPEC §9** (L2426-2548): "README contains a one-screen quickstart (**TLS, multi-addrs, OTel,
  DLX**) and links every example" (L2442-2443); "Every API in §6 compiles, **has godoc**, is
  exercised by at least one test" (L2429).
- **SPEC §10** (L2552-3172): R10-1 `Delay` godoc tracked by **T57** (L3045-3053); R10-2 quorum
  `DeliveryLimit==0` warning tracked by **T58** (L3054-3059); the dense `§x.y` / `decision N` /
  `RNN` / `Rev N` cross-reference scheme; only **two** `### Rev` headings exist (Rev 6 L2785, Rev
  10 L3031) though the `spec-validation/README.md` narrative cites "Rev 6, **Rev 9**, Rev 10".
- **README.md** (278 lines): quickstart (L52-109) — consume handler **`func(ctx, o *Order)`**
  (L104, will not compile against value-`M` `Handler`), every error **swallowed** (`pub, _ :=`,
  `_ = pub.Publish`, `con, _ :=`, unchecked `con.Consume`), single `WithAddr` over plain
  `amqp://` (no TLS/OTel/DLX in the block); honest "Available now / On the roadmap" split
  (L169-188); examples table links the **six** that exist (L194-201).
- **`doc.go`** (12 lines): a feature list ending "See SPEC.md in the repository root for the
  complete public API surface." — no mental model, routes the reader **into** SPEC.
- **Reality (verified this pass):** `Handler[M]` is value-`M` (`consumer.go:113`); `ErrPoison`
  exists (`errors.go:58`, so §5's `warren.ErrPoison` is valid); both `WithAddr` and `WithAddrs`
  exist (`options_connection.go:101/108`); **no** `MIGRATION.md`, **no** `CHANGELOG.md`; the six
  example `main.go` files are 133–267 lines and each opens with a "What this example demonstrates"
  godoc block.
- **Lens prompt:** `spec-validation/12-dx-documentation.md` (this pass *conducted* the review; this
  file is its remediation brief for `/plan`).

### Cross-lens reconciliation (do **not** re-file — project rule)

A finding already owned by a prior task **extends that task** with a `Lens-12 (DX-xx)` acceptance
bullet; it is **never** re-filed. DX is a wide lens and much of its surface is already owned —
this lens **confirms** the exemplars and **extends** the godoc/README/CHANGELOG sweeps with
explicit DX acceptance, and only files the **pit-of-success layer** that has no owner.

| Prompt-anticipated finding | Already owned by | Lens-12 disposition |
| --- | --- | --- |
| `Delay`/`ExchangeDelayed` durability footgun must be in call-site godoc | **T57** (R10-1: §6.5/§6.6 godoc + optional declare-time warning) | **confirm** — the model for routing a load-bearing warning to godoc; DX adds "verbatim at the call site" to the acceptance |
| `AutoAck()` semantics-bypass warning must be in call-site godoc | **T35** (`AutoAck()` opt-in + verbose godoc warning) | **confirm (do-not-regress)** — the **exemplar** every other footgun must match; DX cites it as the policy template |
| §6.5 `Message[M]` field godoc (`Priority`, `Expiration`, `Headers`) | **T10** (field godoc per §6.5) | **extend** T10 — add the **`UserID` 406-channel-close footgun** and the **`DeliveryMode` Persistent-is-the-zero-value-default** note to the mandated call-site godoc (DX-12) |
| Connection-option godoc (`WithFrameMax`/`WithHeartbeat` sizing, `WithConnectionName`) | **T34** (remaining connection options + godoc sizing tiers) | **extend** T34 — add `ConfirmTimeout(0)`-discouraged, `PrefetchBytes()`-no-op, and `ChannelQoS()`-semantics to the mandated call-site godoc (DX-13) |
| README one-screen quickstart (TLS, multi-addrs, OTel, DLX) + links every example | **T39** (README quickstart + links) | **extend** T39 — the quickstart must (a) **compile** (fix the `*Order` handler), (b) **not swallow errors**, (c) actually cover the §9 four (TLS/multi-addrs/OTel/DLX), and (d) hit a stated minimal-line target (DX-04, DX-14) |
| `CHANGELOG.md` ("Keep a Changelog") + final godoc pass | **T40** (CHANGELOG + final godoc pass) | **extend** T40 — the final godoc pass asserts the **footgun→godoc routing table** (XG-1) is fully satisfied, and the CHANGELOG file actually exists (DX-17) |
| Canonical dedupe **example** with an outcome-asserting smoke test | **T38b** (`examples/idempotent_consume/` asserts duplicate observed once) | **confirm (do-not-regress)** — the **model** for §7 outcome assertions; DX cites it as the template the other example smoke tests must match (DX-11) |
| `otel/` example + examples polish/godoc headers + README cross-links | **T38** (examples consolidation; adds `otel/`) | **confirm/extend** — the example godoc headers are already good (do-not-regress); DX adds the durable-retry-ladder + operational-hard-case examples (DX-09/DX-10) as siblings |
| Quorum `DeliveryLimit==0` declare-time warning | **T58** (R10-2 `Topology.validate()` warning) | **confirm** — a footgun routed to a `Dial`/declare-time warning rather than godoc; DX records it in the placement table as already-routed |

**Net-new (no prior owner — the pit-of-success layer):** the **`doc.go` conceptual overview**
content + its SPEC definition (DX-01), the **at-least-once→dedupe godoc routing** onto
`Publish`/`Consume`/`PublishRetry`/`Redelivered`/`MessageID` (DX-02), the **§8 footgun→godoc
routing boundary + table** that makes placement systematic (DX-03), the **copy-paste-correctness**
fixes for the README + §6.2.1 snippets (DX-04/DX-05), the **migration guide** from the 2023
library (DX-06), the **production-tuning page** (DX-07), the **error-message-names-the-remedy**
policy (DX-08), the **durable-retry-ladder + operational-hard-case examples** (DX-09/DX-10), the
**example outcome-assertion gate** (DX-11), the **`<10/<15` line measurability** (DX-14), the
**`amqp.go`-vs-`warren` naming reconciliation** (DX-15), and the **cross-reference integrity
sweep** (DX-16).

---

## 3. Constraints (carried into every Phase-23 task)

1. **A SPEC-only warning is invisible.** Every load-bearing caveat must be routed to the godoc on
   the exported identifier the user touches; SPEC remains the source of truth but is not the
   delivery surface. Treat "documented in SPEC" and "documented at the call site" as distinct
   acceptance criteria.
2. **Amend SPEC first.** Per CLAUDE.md, any wording/behaviour change amends `SPEC.md` in the same
   PR as the godoc/README/example change; the §8 boundary and the footgun→godoc table land in SPEC
   before the sweep that satisfies them.
3. **Confirm-don't-duplicate.** `AutoAck` (T35), `Delay` (T57), the dedupe example (T38b), the
   `DeliveryLimit==0` warning (T58), and the example godoc headers are **do-not-regress** — extend
   their owners, never re-file.
4. **Snippets must compile.** Every code block added or edited in README/SPEC must compile against
   the real signatures; the snippet-correctness gate (XG-3) is the lock. Examples stay on the
   existing `examples-build`/`examples-smoke` lanes — **no new build-tag lane**.
5. **Honest under-promising beats silent gaps.** Keep the README "Available now / On the roadmap"
   split (it is good DX); make the `<10/<15` claim measurable or soften it; never let a
   roadmap-only example appear "available".
6. **Gate-first.** The audits (XG-1..XG-5) run before any wording change so the placement table,
   examples-gap list, and broken-snippet list are ground truth, not assertion. The first task
   records SPEC §10 **Rev 22**.
7. **English only** for all docs (SPEC, README, `doc.go`, `MIGRATION.md`, `CHANGELOG.md`, godoc,
   examples, tasks, LATER).
8. **No public API change.** This phase adds docs, examples, and one migration file; it adds no
   exported option/type/error. (If the error-remedy policy DX-08 needs richer wrapped context, it
   does so via godoc on the existing sentinels and existing wrap sites — not new sentinels.)

---

## 4. Pre-work: DX gates (XG-1..XG-5 — sequence FIRST; they gate wording/fixes)

Mirrors the gate-first discipline of T84/T94/T101/T111/T119/T130/T143/T147/T150/T153. **No
behaviour change**; capture ground truth, then route fixes. The first task records §10 **Rev 22**.

- **XG-1 — Footgun → godoc placement audit.** For every load-bearing footgun/contract, record in
  a table: *symptom*, *where it is documented today* (SPEC §+line / code comment / godoc / nowhere),
  *whether a task already routes it to call-site godoc*, and *the gap*. Minimum rows: at-least-once
  /must-dedupe (§6.2.1), `AutoAck` (§6.3, T35), `Delay` durability (§6.5, T57), `UserID` 406 (§6.5),
  `ConfirmTimeout(0)` (§6.2), `PrefetchBytes()` no-op + `ChannelQoS()` (§6.3), `DeliveryMode`
  persistent-default (§6.5), quorum `DeliveryLimit==0` (§6.3, T58), `MaxRedeliveries` counter-B
  process-local (§6.3), "always wire `OnCancel`" (§6.3), `Mandatory()` set-not-toggle (§6.2),
  `Prefetch < Concurrency` (§6.3). Output = the **footgun-doc-placement table** (lens output #2).
  → gates DX-02/03/12/13.
- **XG-2 — Example inventory & outcome-assertion audit.** Enumerate the §4 example list vs what
  exists on disk; for each existing example, record whether its smoke test asserts an **outcome**
  (round-trip/observable effect) or only **exit-0**; list the hard paths with **no** example.
  Output = the **examples-gap list** (lens output #4). → gates DX-09/10/11.
- **XG-3 — Copy-paste-correctness audit.** Extract every code block from README + SPEC §5 + §6.2.1
  (and the §6.3/§6.5 snippets) and **compile** each against the real signatures; flag every block
  that does not build (catches DX-04 README `*Order`, DX-05 §6.2.1 out-of-scope `d`). Recommend a
  permanent mechanism (a `go:build ignore` doctest harness or a CI step that tangles fenced Go
  blocks and `go build`s them) so snippet rot cannot recur. → gates DX-04/05.
- **XG-4 — Cross-reference integrity sweep.** Resolve every `§x.y`, `decision N`, `RNN`, and
  `Rev N` reference in SPEC; flag dangling/stale targets and reconcile the `### Rev` heading list
  with the narrative (the `spec-validation/README.md` cites Rev 9, which has no SPEC heading).
  Output = a fix-list (no fabrication — only references proven dangling). → gates DX-16.
- **XG-5 — First-hour-journey audit.** Walk dial → topology → publish → consume → handle-failure →
  observe end-to-end and record, at each step, every place the user must **already know something
  the docs do not teach at that step**. Output = the **first-hour journey walkthrough** (lens
  output #1; the §11 draft below is the seed). → gates DX-01/02/07/08.

---

## 5. Workstreams (WS-1..WS-5)

- **WS-1 — Teach the mental model (the conceptual overview).** Define `doc.go`'s mandatory
  contents in SPEC (§4 + a §6.9 or §1 pointer) and write it: role-split pool, at-least-once →
  dedupe-by-`MessageID`, topology-declared-separately + `AttachTo`, reconnect-is-a-synchronous-
  barrier (`Publish` blocks on `ErrReconnecting`), degraded state, the no-silent-X bars, and the
  `amqp.go`-dir-vs-`warren`-module note. (DX-01, DX-15.) → **T158**.
- **WS-2 — Route footguns to the call site, systematically.** Add a §8 *Always* boundary
  ("load-bearing warnings appear verbatim in the godoc at the call site") + a footgun→godoc
  routing table in SPEC; route the at-least-once/must-dedupe contract onto `Publish`, `Consume`,
  `PublishRetry`, `Delivery.Redelivered`, and `Message.MessageID` godoc; extend T10 (`UserID`/
  `DeliveryMode`) and T34 (`ConfirmTimeout(0)`/`PrefetchBytes`/`ChannelQoS`). (DX-02, DX-03,
  DX-12, DX-13.) → **T159** (+ T10/T34 extensions).
- **WS-3 — Make the most-copied code correct.** Fix the README quickstart (compiling handler,
  real error handling, the §9 four, a stated line target) and the §6.2.1 dedupe snippet (use
  `ConsumeRaw` for envelope access, or show the cache keyed off the delivery); wire the
  snippet-compile gate. (DX-04, DX-05, DX-14.) → **T160** (+ T39 extension).
- **WS-4 — Discoverability of the *right* path.** A migration guide from the 2023 library
  (`MIGRATION.md`); a single "Production tuning / recommended defaults for high throughput" page
  consolidating §6.1/§6.2/§6.3; the error-message-names-the-remedy policy on the operational
  sentinels' godoc. (DX-06, DX-07, DX-08.) → **T161**, **T162**.
- **WS-5 — Runnable references for the hard paths.** A durable-retry-ladder example
  (`examples/durable_retry/`, TTL+DLX — the path §6.5/R10-1 steers users toward but that has no
  example); the operational hard-case examples (mTLS/SASL EXTERNAL, cluster failover, graceful
  shutdown, callback-wiring, metrics+alerts), ranked; the example outcome-assertion gate (model:
  T38b). (DX-09, DX-10, DX-11.) → **T163** (+ confirm T38b as the template).

---

## 6. Do-not-regress (confirmations — the DX assets already in place)

- **`AutoAck()` verbatim-godoc mandate** (§6.3 L1351, T35) — the exemplar; the new footgun→godoc
  table must reference it as the policy template, not weaken it.
- **`Delay` durability godoc** (§6.5, T57/R10-1) — keep the "confirmed ≠ delivered, silent loss on
  node failure" warning routed to the call site.
- **Example top-of-file godoc headers** (§7 L2312-2313) — every shipped example already opens with
  "What this example demonstrates"; preserve the format on the new examples.
- **`examples/idempotent_consume/` outcome assertion** (T38b) — the smoke test that asserts a
  duplicate is observed exactly once is the **model** for DX-11; do not regress to exit-0-only.
- **Redaction-in-logs documentation** (§6.9 L2051-2054, README L226) — keep the "credentials never
  emitted in clear text" statement; it is correct and well-placed.
- **"Available now / On the roadmap" honesty split** (README L169-188) — good under-promising;
  keep it accurate as examples land.
- **`IsTransient`/`IsPermanent` godoc** (§6.8 L1947-1971) — actionable and precise; the model for
  the error-remedy policy (DX-08) to extend to the terse sentinels.

---

## 7. Routing matrix (every DX-01..DX-17 accounted for exactly once)

DX-01 (doc.go content + SPEC definition) → **T158** ·
DX-02 (at-least-once→dedupe godoc routing) → **T159** ·
DX-03 (§8 footgun→godoc boundary + table) → **T159** ·
DX-04 (README quickstart compiles + no swallowed errors) → **T160** (+ extend **T39**) ·
DX-05 (§6.2.1 dedupe snippet out-of-scope `d`) → **T160** ·
DX-06 (migration guide) → **T161** ·
DX-07 (production-tuning page) → **T162** ·
DX-08 (error-message-names-the-remedy policy) → **T162** ·
DX-09 (durable-retry-ladder example) → **T163** ·
DX-10 (operational hard-case examples, ranked) → **T163** ·
DX-11 (example outcome-assertion gate) → **T163** (model: confirm **T38b**; coordinate Lens-10/TV) ·
DX-12 (`UserID` 406 + `DeliveryMode`-default godoc) → extend **T10** ·
DX-13 (`ConfirmTimeout(0)`/`PrefetchBytes`/`ChannelQoS` godoc) → extend **T34** ·
DX-14 (`<10/<15` line claim measurable) → extend **T39** (+ **T160**) ·
DX-15 (`amqp.go` dir vs `warren` module naming note) → **T158** ·
DX-16 (cross-reference integrity sweep) → **T157** (XG-4 produces the fix-list; fixes ride the
relevant task) ·
DX-17 (`CHANGELOG.md` exists; final godoc pass asserts XG-1 satisfied) → extend **T40**.
New: **T157** (gates XG-1..XG-5, Rev 22), **T158** (doc.go + naming), **T159** (dedupe/footgun
godoc routing + §8 boundary), **T160** (snippet correctness + gate), **T161** (migration), **T162**
(tuning page + error-remedy policy), **T163** (examples + outcome gate). Confirmed
(do-not-regress): **T35, T38b, T57, T58**, example godoc headers, redaction docs. Deferred:
**LATER-48** (full observability/alerting cookbook example with Grafana dashboards).

---

## 8. Phasing (Phase 23, A–E)

- **A** Gates — **T157** (XG-1..XG-5; records Rev 22; produces the placement table, examples-gap
  list, broken-snippet list, cross-ref fix-list, first-hour walkthrough; no behaviour change).
- **B** Teach + route — **T158** (doc.go + naming note) + **T159** (dedupe/footgun godoc routing +
  §8 boundary) + the **T10**/**T34** godoc extensions.
- **C** Correctness — **T160** (README + §6.2.1 snippet fixes + snippet-compile gate) + the **T39**
  quickstart extension.
- **D** Discoverability — **T161** (migration guide) + **T162** (tuning page + error-remedy policy).
- **E** Hard-path examples + capstone — **T163** (durable-retry + operational examples + outcome
  gate), lands last; + the **T40** final-godoc-pass / CHANGELOG extension asserting the XG-1 table
  is fully satisfied.

Gate deps: XG-1→DX-02/03/12/13 (T159/T10/T34); XG-2→DX-09/10/11 (T163); XG-3→DX-04/05 (T160);
XG-4→DX-16 (cross-ref fixes); XG-5→DX-01/02/07/08 (T158/T159/T162). Gate-independent
(pure doc): DX-06 (migration), DX-15 (naming). T163 + the T40 extension land last.

**Tally.** Highest existing task = **T156** (Phase 22). New IDs **T157–T163** (7 net-new). Current
tally **165 tasks / 22 phases** → **172 / 23**. New SPEC §10 Rev = Phase − 1 = **Rev 22** (recorded
by T157). New `LATER` = **LATER-48** (highest filed is LATER-47; the LATER-44 gap stays untouched).

---

## 9. Acceptance criteria (lens-level; each task carries its own)

- **`doc.go` teaches the mental model.** A reader who opens only `doc.go` (or pkg.go.dev's package
  page) learns: at-least-once → dedupe-by-`MessageID`, topology-declared-separately + `AttachTo`,
  reconnect-is-a-barrier, role-split pool, the no-silent-X bars, and the `amqp.go`/`warren` naming.
  SPEC §4/§6 defines these as `doc.go`'s mandatory contents. (DX-01/15)
- **The must-dedupe contract is impossible to miss.** The godoc on `Publish`, `Consume`,
  `PublishRetry`, `Delivery.Redelivered`, and `Message.MessageID` each carry the at-least-once →
  dedupe pointer (to §6.2.1 + `examples/idempotent_consume/`). A §8 *Always* boundary + a SPEC
  footgun→godoc table make the routing systematic and verifiable. (DX-02/03)
- **The most-copied snippets compile.** The README quickstart and the §6.2.1 dedupe snippet build
  against the real signatures (XG-3 green); the README quickstart handles errors (no `_`-swallow),
  covers the §9 four (TLS/multi-addrs/OTel/DLX), and meets a stated line target. (DX-04/05/14)
- **The right path is discoverable.** `MIGRATION.md` maps the 2023 API → warren (incl.
  default-requeue→default-nack-no-requeue, single-connection→role-split-pool, generic-options→
  non-generic, builder-`Message`→struct); a single production-tuning page consolidates the knobs +
  the prefetch/concurrency formula; the operational sentinels' godoc names the remedy/knob.
  (DX-06/07/08)
- **The hard paths have runnable references.** `examples/durable_retry/` (TTL+DLX) ships and
  smoke-runs; the ranked operational examples land or are tracked; every example smoke test
  asserts an **outcome**, not exit-0 (DX-09/10/11).
- **Confirmations hold.** `AutoAck` (T35), `Delay` (T57), the dedupe example (T38b), the example
  godoc headers, and the redaction docs are unchanged or strengthened. (do-not-regress)
- **Project gates green.** `make lint test`, `make examples-build`, `make examples-smoke` pass;
  README "Available now / roadmap" and examples table stay in sync; numbering integrity (T157–T163
  contiguous, no collision) holds; Rev 22 recorded.

---

## 10. Owner decisions (2026-05-29, recommended dispositions — overridable before execution)

- **D1 — `doc.go` scope (DX-01).** *Recommended:* `doc.go` becomes the **canonical conceptual
  overview** (the mental model + a minimal correct publish/consume + pointers to the examples and
  the dedupe contract), and SPEC §4/§6 defines its mandatory section list so it cannot rot back to
  a feature stub. A standalone `docs/` site is **out of scope** for v0.1 (LATER if demand appears).
- **D2 — Migration-guide home (DX-06).** *Recommended:* a top-level **`MIGRATION.md`** linked from
  README (not a README section — it is long and versioned), since 2023-library users are the most
  likely early adopters and need a mapping table + behavioural-change list. *Alternative:* a
  `doc.go`/`docs/` subsection.
- **D3 — Production-tuning home (DX-07).** *Recommended:* a **dedicated section in `doc.go`** (or a
  `docs/tuning.md` linked from README) titled "Production tuning / recommended defaults for high
  throughput", consolidating publisher/consumer connections, channel-pool size, the
  prefetch/concurrency formula, heartbeat, frame-max, and `ConfirmTimeout` — with the existing
  §6.1/§6.3 prose cross-linked, not duplicated.
- **D4 — Error-remedy mechanism (DX-08).** *Recommended:* because Go bare-sentinel text is fixed,
  the **remedy lives in the godoc on each operational sentinel** (e.g. `ErrChannelPoolExhausted`
  godoc names `WithChannelPoolSize`/`WithPublisherConnections`), and **wrap sites add the knob to
  the wrapped message** where context allows. Adopt a stated §6.8 policy: "every operational error
  documents the cause **and** the remedy." *Do not* mint new sentinels.
- **D5 — Which examples, and priority (DX-09/10).** *Recommended ranking* (P1 first): (1)
  **durable_retry** (TTL+DLX ladder — the footgun-avoidance path with no example today); (2)
  **graceful_shutdown** (drain in-flight on SIGTERM); (3) **tls_mtls** (`amqps://` + SASL
  EXTERNAL — gated on Phase-8 hardening); (4) **cluster_failover** (`WithAddrs`); (5)
  **callbacks** (`OnReturn`/`OnBlocked`/`OnCancel`/`OnTopologyDegraded` wired correctly); (6)
  **observability** (metrics + alert rules). P1 = (1)–(2) for v0.1; (3)–(6) tracked, landing as
  their dependencies (TLS/mTLS, cluster failover) ship. The full Grafana-dashboard cookbook is
  **LATER-48**.
- **D6 — The `<10/<15` line claim (DX-14).** *Recommended:* **keep but make it measurable** —
  define what counts (the minimal correct snippet excluding imports/signal-handling, with errors
  handled) and prove it with the fixed README quickstart; if the honest minimum exceeds the
  numbers, **soften the claim** rather than ship a broken promise.

---

## 11. First-hour journey walkthrough (lens output #1 — seed for XG-5)

1. **`go get`.** README says pin to `@main` (no tag yet) — honest. **Friction:** cloning the repo
   shows directory `amqp.go` but `package warren`; nothing reconciles the names (DX-15).
2. **Dial.** README quickstart dials a single plain-`amqp://` addr with errors checked; SPEC §5
   dials two `amqps://` addrs with a logger. **Friction:** the user must already know they want
   role-split pool defaults (2+2), that `WithAddr` vs `WithAddrs` both exist, and that TLS is a
   separate `WithTLSConfig` — none is taught at the dial step; the production-tuning knobs are
   scattered (DX-07).
3. **Topology.** Both quickstart and §5 declare then `AttachTo`. **Friction:** *why* declare
   separately and *why* `AttachTo` (reconnect redeclare) is explained in §6.6/§6.1, not at the
   call site; a cargo-culting user who omits `AttachTo` silently loses redeclare-on-reconnect. The
   mental model belongs in `doc.go` (DX-01).
4. **Publish.** §5 wires `Codec`→`Exchange`→`RoutingKey`→`Mandatory`→`ConfirmTimeout`→`Build`→
   `Publish` with each error checked. **Friction:** the at-least-once duplicate hazard of
   `PublishRetry` (§6.2 L996-1008) and the `Mandatory()` set-not-toggle semantics are SPEC prose,
   not `Publish`/`PublishRetry` godoc (DX-02/03). The README's `_ = pub.Publish(...)` teaches the
   user to ignore the very error the library exists to surface (DX-04).
5. **Consume.** The user copies a handler. **Friction (high-impact):** the README handler
   `func(ctx, o *Order)` **does not compile** against `Handler[Order] = func(ctx, Order) error`
   (DX-04); and the single most important thing — *you MUST dedupe* — is a §6.2.1 section the
   hurried user skips, with **no pointer on `Consume`'s godoc** (DX-02). When they do reach §6.2.1,
   the canonical snippet references out-of-scope `d.MessageID()` (DX-05).
6. **Handle a failure.** The user hits `ErrChannelPoolExhausted` / `ErrConfirmTimeout`.
   **Friction:** the error text names the symptom, not the knob; the remedy
   (`WithChannelPoolSize` / raise connections) is in §6.2 prose, not the error's godoc (DX-08).
   For poison handling, the quorum-`DeliveryLimit==0`-drops-at-20 trap is documented (T58) but the
   user must connect three sections (§6.3 poison + §6.6 declare + §1 no-silent-drop).
7. **Observe.** OTel/metrics wiring is in README's Observability section and §6.9 — discoverable,
   but there is **no runnable `otel/` example yet** (roadmap, T38) and **no metrics+alerts
   example** (DX-10). For durable delays/retries the user reaches for `examples/delayed/` (the
   **lossy** path) because there is **no durable-retry-ladder example** to steer them right
   (DX-09).

---

## 12. Footgun-doc-placement table (lens output #2 — seed for XG-1)

| Footgun / contract | SPEC location | Mandated verbatim in call-site godoc? | Runnable example? | Gap / disposition |
| --- | --- | --- | --- | --- |
| at-least-once → **MUST dedupe** | §6.2.1 L1013-1068 | **No** (SPEC section only) | Yes — `idempotent_consume/` (T38b) | **DX-02** — route to `Publish`/`Consume`/`PublishRetry`/`Redelivered`/`MessageID` godoc (T159) |
| `AutoAck()` semantics bypass | §6.3 L1345-1368 | **Yes** ("godoc repeats verbatim", L1351) | trade-off in integration test (T35) | **Confirm (do-not-regress)** — the exemplar |
| `Delay`/`ExchangeDelayed` not durable | §6.5 L1536-1552 | Planned (T57/R10-1) | `delayed/` (lossy) | **Confirm T57**; add durable-retry example (DX-09) |
| `UserID` 406 channel-close | §6.5 L1487-1513 | **No** | — | **DX-12** — extend T10 |
| `DeliveryMode` Persistent is zero-value default | §6.5 L1450/L1554-1556 | **No** | — | **DX-12** — extend T10 |
| `ConfirmTimeout(0)` discouraged | §6.2 L972-978 | **No** ("documented as discouraged", not in godoc) | — | **DX-13** — extend T34/builder godoc |
| `PrefetchBytes()` no-op on RabbitMQ | §6.3 L1145 | code comment only | — | **DX-13** — extend T34/builder godoc |
| `ChannelQoS()` per-channel semantics | §6.3 L1146 | code comment only | — | **DX-13** — extend T34/builder godoc |
| quorum `DeliveryLimit==0` → drop at 20 | §6.3 L1255-1265 | declare-time warning (T58) | — | **Confirm T58** (routed to warning, not godoc) |
| "always wire `OnCancel`" | §6.3 L1342 | **No** | — | route to `OnCancel`/`Consume` godoc + callbacks example (DX-10) |
| `Mandatory()` set-not-toggle | §6.2 L790 | inline comment | publish/ | minor — fold into T159 table |
| `Prefetch < Concurrency` mis-sizing | §6.3 L1178-1179 | `Build`-time warning | — | **Confirm** (routed to warning); cross-link from tuning page (DX-07) |

---

## 13. Findings table (lens output #3)

| ID | Severity | Classification | Location (§+lines) | DX impact | Recommended SPEC amendment / task |
| --- | --- | --- | --- | --- | --- |
| **DX-01** | High | scattered / missing-doc | §4 L181; `doc.go` (12 lines); §6.9 | Mental model taught nowhere a hurried reader opens; opinionated library → guaranteed misuse | Define `doc.go`'s mandatory contents in SPEC §4/§6; write the conceptual overview → **T158** |
| **DX-02** | High | wrong-placement | §6.2.1 L1013-1068 | The single most important contract (must-dedupe) is a section users skip; invisible at `Publish`/`Consume`/`PublishRetry` | Mandate the at-least-once/dedupe pointer in call-site godoc → **T159** |
| **DX-03** | High | inconsistent / wrong-placement | §6.3 L1351 vs §8 L2344 | Footgun→godoc routing is ad-hoc (only `AutoAck` mandated verbatim); placement not systematic | Add a §8 *Always* boundary + a footgun→godoc routing table → **T159** |
| **DX-04** | High | inconsistent / honesty | README L91/96/99/104 | First code a user copies **does not compile** (`*Order` handler) and **swallows all errors** (`_`) | Fix the quickstart; cover the §9 four; wire snippet gate → **T160** (+ extend T39) |
| **DX-05** | High | missing-example (broken) | §6.2.1 L1048-1054 | The canonical dedupe snippet references out-of-scope `d.MessageID()`; doesn't build as written | Rewrite the snippet via `ConsumeRaw` (envelope access) → **T160** |
| **DX-06** | Medium | missing-doc | CLAUDE.md / §10 north stars | 2023-library users (most likely adopters) have no old→new mapping or behaviour-change list | Add `MIGRATION.md` → **T161** |
| **DX-07** | Medium | scattered | §6.1 L487-563, §6.2, §6.3 | Production tuning is reverse-engineered from option tables/prose; the right path is not discoverable | Add a production-tuning page consolidating the knobs + formula → **T162** |
| **DX-08** | Medium | missing-doc / inconsistent | §6.8 L1866-1930 | Terse sentinels name the symptom not the remedy; no consistent "cause + remedy" policy | §6.8 policy + godoc-on-sentinel naming the knob → **T162** |
| **DX-09** | Medium | missing-example | §6.5/§6.6 L1536-1552; `delayed/` | The only delay example is the **lossy** one; users are pushed to the footgun for durable retries | Add `examples/durable_retry/` (TTL+DLX) → **T163** |
| **DX-10** | Medium | missing-example | §6.1/§6.3/§6.9 | Operational hard cases (mTLS, failover, graceful shutdown, callbacks, metrics+alerts) undemonstrated | Add ranked operational examples → **T163** (D5) |
| **DX-11** | Medium | honesty / wrong-placement | §7 L2314-2320 | Example smoke gate proves exit-0, not correctness (T38b is the lone outcome-asserting exception) | Mandate outcome assertions in the smoke gate → **T163** (coordinate Lens-10/TV) |
| **DX-12** | Medium | wrong-placement | §6.5 L1487-1513/L1450 | `UserID` 406 + `DeliveryMode`-default footguns absent from call-site godoc | extend **T10** |
| **DX-13** | Low | wrong-placement | §6.2 L976; §6.3 L1145-1146 | `ConfirmTimeout(0)`/`PrefetchBytes`/`ChannelQoS` caveats only in SPEC/code comments | extend **T34** |
| **DX-14** | Low | honesty-gap | §1 L94-97; §9 L2442-2443 | `<10/<15` line claim unmeasurable vs the smallest runnable artifact; quickstart misses the §9 four | Make measurable or soften → extend **T39** (+ T160) |
| **DX-15** | Low | inconsistent | dir `amqp.go` vs `package warren` | Newcomer cloning `amqp.go` and finding `package warren` is confused; reconciled nowhere | One-line note in `doc.go` + README → **T158** |
| **DX-16** | Low | inconsistent | §10 cross-refs; Rev headings | Dense `§x.y`/`decision N`/`RNN`/`Rev N` scheme; `spec-validation/README` cites Rev 9 with no SPEC heading | Cross-reference integrity sweep → **T157** (XG-4) |
| **DX-17** | Low | honesty / missing-doc | §4 L179; §8 L2343 | `CHANGELOG.md` mandated but does not exist; pkg.go.dev users have no changelog | extend **T40** (file exists; final godoc pass asserts XG-1) |

---

## 14. Examples-gap list (lens output #4 — hard paths with no runnable example, ranked)

1. **Durable retry ladder (TTL + DLX)** — the path §6.5/R10-1 steers users toward and away from
   the lossy `delayed/` exchange; **no example today** → users default to the footgun. **(DX-09,
   P1)**
2. **Graceful shutdown** — `Close(ctx)` drains in-flight work (§1 L35); no example shows
   SIGTERM → drain. **(DX-10, P1)**
3. **mTLS / SASL EXTERNAL** — `amqps://` + `WithSASLMechanism(SASLExternal)` (§6.1, §9 L2486);
   gated on Phase-8 hardening. **(DX-10, P2)**
4. **Cluster failover (`WithAddrs`)** — multi-addr reconnect across nodes; no example. **(DX-10,
   P2)**
5. **Callback wiring** — `OnReturn` / `OnBlocked` / `OnCancel` / `OnTopologyDegraded` correctly
   handled (the §6.3 "always wire `OnCancel`" obligation). **(DX-10, P2)**
6. **Production metrics + alerts** — Prometheus wiring + the mandatory-metric alert rules
   (`publisher_retry_total`, `consumer_handler_aborted_…`, `topology_redeclare_seconds`); the
   full Grafana cookbook is **LATER-48**. **(DX-10, P3)**

(Already owned/roadmap, not re-filed: `otel/`, `idempotent_consume/`, `ordered_consume/`, `rpc/`,
`delayed/` — T38/T38b/T38c/T31b.)

---

## 15. Open questions for the owner

1. **D1/D2/D3 doc home:** `doc.go` sections vs a `docs/` directory vs top-level `MIGRATION.md` —
   which surface for the conceptual overview, migration guide, and tuning page? (Recommendation:
   `doc.go` for overview+tuning, top-level `MIGRATION.md` for migration.)
2. **D4 error-remedy:** acceptable that the remedy lives in **godoc on the sentinel** (since bare
   `errors.New` text is fixed and no new sentinels are minted), with wrap sites adding the knob
   where context allows?
3. **D5 example scope for v0.1:** are durable_retry + graceful_shutdown the right P1 pair, with
   mTLS/failover/callbacks/observability tracked to land as their dependencies ship?
4. **DX-11 outcome gate:** is the example smoke-test outcome-assertion requirement owned here or by
   the test-strategy lens (Lens-10/TV)? (Recommendation: state it in §7 here; implement the
   assertions per-example, coordinated with the TV gate.)
5. **DX-14 line claim:** keep `<10/<15` with a precise definition, or soften to "a few lines for
   the common case"?
6. **Doctest mechanism (XG-3):** adopt a tangle-and-`go build` CI step for fenced Go blocks, or a
   `go:build ignore` doctest file, to prevent snippet rot permanently?

---

## 16. Out of scope

- SPEC.md / code / README / example / `doc.go` / `MIGRATION.md` / `CHANGELOG.md` edits in **this**
  step (per-task, during execution, "amend SPEC first"). Committing (separate explicit request).
- A standalone documentation site / tutorial series (LATER if demand appears).
- The full observability + alerting **cookbook** example with Grafana dashboards (**LATER-48**).
- Re-filing already-owned DX work — `AutoAck` godoc (T35), `Delay` godoc (T57),
  `DeliveryLimit==0` warning (T58), the dedupe example (T38b), `otel/`/`ordered_consume/` examples
  (T38/T38c) — this lens **confirms/extends**, never re-files.
- New public API (no new exported option/type/error; the error-remedy policy uses godoc + existing
  wrap sites, not new sentinels).
- Behaviour changes to the example smoke **harness** beyond adding outcome assertions (the harness
  itself is owned by T37/T38; DX adds only the assertion requirement, coordinated with Lens-10).
- Other lenses (13 load-testing).

---

## 17. Verdict for this lens

**GO-WITH-CHANGES — no Blocker.** The library is well-architected and its hardest prose is
exemplary; the six shipped examples are well-documented; and the majority of DX placement work is
already owned (T10/T34/T35/T38/T38b/T39/T40/T57/T58). The library does not silently lose,
duplicate, or leak anything because of a documentation gap — so this is not NO-GO. But it is not a
clean GO either: a first-hour developer is set up to get two things **wrong** without reading the
source — they will copy a **non-compiling** quickstart handler (DX-04) and they will **miss the
must-dedupe contract** because it lives in a section, not on the `Consume` godoc (DX-02) — and the
package's conceptual overview, the one place that should teach the mental model, is an **undefined
placeholder** (DX-01). Fixing the two broken snippets (DX-04/05) is urgent and cheap; routing the
dedupe contract to the call site (DX-02) and writing a real `doc.go` (DX-01) are the headline
pit-of-success investments; the rest is discoverability (migration, tuning, error-remedy) and
runnable references for the hard paths. **17 findings, 5 gates, 7 net-new tasks (T157–T163), 4
cross-lens extensions, LATER-48. Phase 23 → SPEC §10 Rev 22. Tally 165 → 172 tasks / 22 → 23
phases.**
