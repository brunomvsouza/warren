# Plan Input — Remediate Performance & Capacity Findings (Lens 09)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> performance & capacity lens (`spec-validation/09-performance-capacity.md`). Like
> Lenses 03..08, no findings report pre-existed — this brief was produced by
> *conducting* the review: every throughput number in §9 was reduced to its
> Little's-Law model (`in-flight = throughput × latency`), the implied RTT was
> back-solved, and the rate was re-projected at realistic remote RTTs (1/5/10 ms);
> every per-message heap allocation on the publish and consume hot paths was traced
> end-to-end against the *implemented* code; the `amqp091-go` per-connection I/O
> serialization premise was verified against the upstream source (v1.11.0); and the
> confirm-tracker `multiple=true` resolution was read for its real complexity rather
> than taken at the "single pass" wording. It enumerates confirmed findings
> (`PC-01..PC-15`), each with evidence (SPEC §+line *and* `file:line`), the model
> arithmetic, and a recommended SPEC amendment / hot-path fix, grouped into
> workstreams and sequenced by dependency.
>
> **Numbering:** new task IDs are **T147–T149** (highest existing is T146, after
> Phase 19). Lens 09 becomes **Phase 20**, mirroring how Lenses 01..08 became Phases
> 12..19. **This lens is heavily cross-lens** — and, unusually, *its own headline was
> already caught by a prior lens*: the sync-confirm capacity-honesty finding the
> performance lens exists to raise is **already owned and being remediated by T83
> (RMQ-11, §9 throughput-honesty wording) and T116 (SRE-14, §9/§6.2 capacity truth +
> the `pool/RTT` ceiling + the `ErrChannelPoolExhausted` cascade)**, and the
> histogram capacity-tail finding is already owned by **T113 (SRE-11)**. So most
> findings **extend an existing task in place** (cross-lens rule: shared findings
> extend the shared task, never re-filed). **Nine prior tasks are extended** with a
> `Lens-09 (PC-xx)` acceptance bullet — **T83** (the explicit RTT-collapse model
> table), **T116** (the pool-sizing-for-rate formula), **T44b** (benchmark conditions:
> payload size + queue type classic-vs-quorum + regression-gate cadence + the 50k
> source), **T22** (`PublishBatchMaxSize` trade-off + the batch-as-scale-path target),
> **T11** (the per-publish confirm-`Wait` timer alloc + the `multiple=true`
> O(outstanding) resolution), **T17** (the x-death map allocated on every delivery),
> **T10** (UUIDv7 entropy/`timeMu`/string per publish), **T18** (the consume
> single-channel scaling note), **T113** (confirm the capacity-tail buckets, +
> rationale). **Three tasks are net-new** (T147 gates, T148 hot-path allocation
> hardening, T149 capacity & performance capstone). **One new `LATER.md` entry**
> (LATER-45: the deeper allocation wins — pooled-buffer codec + a UUIDv7 generator
> without the process-global lock), gated on owner decision D1. **No new build-tag
> lane** (the allocation gates ride the unit / `-benchmem` / `AllocsPerRun` lanes; the
> RTT/throughput/queue-type gates ride the existing `integration` lane and the T44b
> release-tag bench cadence; the injected-RTT gate coordinates with T116's SG-4). The
> first Phase-20 task records SPEC §10 **Rev 19**.
>
> **Numbering reconciliation (LATER-45).** The highest *filed* `LATER` is **LATER-43**
> (Lens-08, aggregate confirm window). **LATER-44 is a standing *conditional*
> reservation by Phase 18** (the CloudEvents codec sub-module split, filed only if its
> D3 is overridden — `plan.md:1463/1475/1486/1507/1516`, `todo.md:2033/2083/2086/2150/2159`).
> To keep filed numbers gapless and collision-free without re-touching Phase 18,
> Lens-09 files **LATER-45**; the Phase-18 conditional LATER-44 reservation stands.
>
> **Lens verdict: GO-WITH-CHANGES.** The architecture is performance-sound where it
> matters: the `amqp091-go` per-connection serialization premise is real (verified —
> a `sendM` mutex serialises all writes + a single reader goroutine demuxes all reads,
> v1.11.0), so the multi-TCP role-split fan-out (§1/§6.1) is the *correct* answer to
> the single-socket ceiling; Prometheus uses `WithLabelValues` (no per-message
> `Labels{}` map); trace injection zero-allocates on the no-span path; decode runs on
> the bounded dispatch goroutines, not the per-channel reader; and the NoOp
> tracer/metrics method bodies are genuinely empty. **There is no Blocker** — and,
> critically, the capacity-honesty headline this lens would otherwise file as one is
> *already owned* by T83/T116, so Lens-09 confirms their scope (do-not-regress) and
> contributes the explicit model table rather than re-filing it. What the lens exposes
> as net-new is **performance debt the prior lenses did not touch**: a cluster of
> avoidable **per-message hot-path allocations** at the billions/day bar — a
> `time.Timer` allocated on *every* `Publish` confirm-wait (default `ConfirmTimeout`
> 30s; PC-06), span-name + attrs-slice + `url.Parse` built on every publish *and*
> every delivery even under the NoOp tracer (PC-07), an x-death `map` allocated on
> *every* delivery before the header-absence check (PC-08), and an un-pooled UUIDv7
> entropy draw + a process-global `timeMu` lock per publish (PC-09) — plus one
> **algorithmic** finding (the confirm tracker resolves `multiple=true` in
> O(outstanding) per frame, not O(resolved); PC-11) and benchmark-methodology /
> §9-criteria gaps (no payload size or queue type on the numbers; no consume-side
> throughput target and no latency SLO at all; the `5×` batch target pegged to the
> wrong baseline). None require a redesign; all are local hot-path fixes, an ordered
> tracker index, and spec/criteria sharpening.

---

## 1. Objective

Validate `SPEC.md` from the seat of a **performance engineer who benchmarks for a
living and distrusts every throughput number that does not come with a stated RTT,
hardware, broker config, queue type, payload size, and confirm mode.** The lens bar is
concrete: *for every throughput number, reconstruct the model (`in-flight =
throughput × latency`), back-solve the implied RTT, and re-project the rate at
realistic remote RTTs; for every hot-path operation on the billions/day path,
enumerate the per-message allocations and locks and mark each necessary or avoidable;
for every "single pass"/"efficient" claim, read the real complexity.* The two
highest-value classes are **a stated target that is achievable only on a loopback
broker** (a capacity lie) and **an avoidable allocation or lock on the per-message hot
path** (a tax paid billions of times a day).

Concretely, the plan must:

1. **Contribute the explicit throughput model, don't re-file the honesty headline
   (PC-01/PC-02).** The sync-confirm ceiling — `Publish` holds a pooled channel for a
   full confirm round-trip, so per-`Publish` throughput is `(conns × channels) / RTT`,
   the §9 30k/100k targets imply ~0.27–0.64 ms RTT (loopback), and a confirm-latency
   spike cascades into `ErrChannelPoolExhausted` — is **already owned by T83
   (RMQ-11) and T116 (SRE-14)** (`plan.md:1049`, `plan.md:1307`). Lens-09 does **not**
   re-file it. It **extends** those tasks with the concrete deliverable they lack: the
   explicit RTT-collapse table (rate @1/5/10 ms — §11 below) baked into §9 *beside the
   numbers*, and a pool-sizing-for-target-rate formula (`pool ≥ target_rate ×
   confirm_RTT` per connection) so the cascade is not just stated but quantified.

2. **Eliminate the avoidable per-message allocations (PC-06/07/08/09).** Trace the
   publish and consume hot paths and remove the allocations that are paid on *every*
   message at the NoOp/default config: pool the per-`Publish` confirm-`Wait`
   `time.Timer` (`tracker.go:171`), gate the span argument-construction behind a
   precomputed `tracingActive` flag (`publisher.go:371/423`, `consumer.go:571/716`,
   `connection.go:842`) *without* introducing a tracing-disabled code branch (decision
   3 stays intact — one `Start` call site, only the *arguments* are gated), make the
   x-death `byReason` map lazy (`xdeath.go:32` → allocate only when an entry for the
   queue is found), and enable the pooled UUIDv7 entropy path
   (`uuid.EnableRandPool()` once at init) + document the google/uuid process-global
   `timeMu` lock. Each lands with an `AllocsPerRun` guard test.

3. **Fix the `multiple=true` resolution complexity (PC-11).** `resolveUpTo`
   (`tracker.go:219-230`) scans the **entire** `pending` map and `slices.Sort`s the
   match on **every** `multiple=true` frame, under `t.mu`, even when one tag resolves —
   O(outstanding) per frame, not the O(resolved) the §6.2 "single pass … critical for
   high-throughput batching" wording implies. Under `PublishBatch` (default in-flight
   1024) or fan-out this is per-frame O(n) on the hot path holding the tracker mutex.
   Track a contiguous confirmed low-water-mark + an ordered index → O(resolved + log n);
   amend §6.2 to state the real complexity.

4. **Close the benchmark-methodology and §9-criteria gaps (PC-03/04/05/10).** Require
   the benchmark to report and pin **payload size** and **queue type (classic vs
   quorum** — quorum's majority Raft commit materially raises confirm latency) on top
   of the RTT/hardware T116 already demands; reframe `BenchmarkPublishBatch ≥ 5×` (a
   ratio pegged to the *local* single-publish baseline, where it is both hard to hit
   and a ~20× understatement of the remote benefit) into an RTT-stated absolute with
   `PublishBatch` documented as *the* scale path; add the **missing** §9 consume-side
   throughput target and the **missing** publish/handle latency SLO (§9 today has
   neither); and note the regression gate is release-tag-only (no per-PR smoke).

5. **Pin the remaining underspecified figures (PC-12/13/14/15).** Confirm T113's
   capacity-tail histogram buckets cover the confirm-RTT region; document the
   `PublishBatchMaxSize=1024` memory/throughput trade-off; source the unsourced
   "~50k msg/s per socket" figure with a measured single-socket ceiling (and correct
   the §6.1 "one goroutine drives the socket" wording — writes are `sendM`-serialised,
   reads are single-goroutine); and cache the per-message Prometheus child collector.

This is **hot-path allocation hardening + one algorithmic fix + spec/criteria
sharpening**, layered on top of a capacity-honesty remediation (T83/T116/T113) that is
already in flight. No redesign; the async-publish-API decision **stays in LATER-34**
(not pulled), and decision 31 (`PublishBatchMaxSize` is a per-call cap, not a sliding
window) **stays closed**.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing** (project convention; anchors below are this-pass snapshots).
  - §1 Reliability bar: billions/day **L39-41**; **"No single-TCP-socket bottleneck"**
    + "a single socket is fine for tens of thousands of messages/second" **L53-58**;
    "Success Criteria in §9 encode quantitative versions" **L86**.
  - §6.1 Connection: `WithPublisherConnections` default **2** (raise to 4–8 for
    > 50k msg/s) **L501/L530-535**; `WithConsumerConnections` default **2** **L502**;
    `WithChannelPoolSize` default **8** per publisher connection **L503/L558-560**;
    **"~50k msg/s … one TCP socket bounds the achievable confirm rate"** (the unsourced
    figure) **L533**; `Publish` "acquires a channel … awaits confirm … returns the
    channel" **L560-562**; NoOp tracer "instrumentation code paths run uniformly … no
    'tracing-disabled' code branch" **L664-666**.
  - §6.2 Publisher: `Publish` **synchronous-with-confirm** **L838-841/L848-849**;
    `multiple=true` "resolving all outstanding delivery-tags … in a **single pass** …
    **critical for high-throughput batching** against RabbitMQ 4.x" **L860-863**;
    `PublishBatch` "pipelines all N publishes on a **single channel**" **L898-900**;
    `PublishBatchMaxSize` default **1024** (per-call cap) **L795/L930-933**;
    `ErrChannelPoolExhausted` on exhaustion+ctx **L954-957**; `ConfirmTimeout = 30s`,
    `PublishTimeout = 0` **L972-973**.
  - §6.3 Consumer: throughput formula **`throughput ≈ Concurrency / handler_latency`**
    **L1171-1173**; prefetch rules of thumb **L1181-1183**; quorum read-ahead floor
    **16** **L1187**; defaults `Concurrency=1`, `Prefetch=64` **L1161**; non-blocking
    dispatcher **L1212-1219**.
  - §6.9 Observability: **histogram buckets default `[0.5, 1, 2, 5, 10, 25, 50, 100,
    250, 500, 1000, 5000]` ms** **L2073-2076**; `WithLatencyBuckets` override.
  - §7 Testing: benchmark cadence "**On-demand and on release tag**" **L2222** (the
    `Benchmark*` names + "single Apple M-series laptop / local broker" live in §9, not §7).
  - §9 Success Criteria — **the numbers**: reconnect chaos "**5-minute outage at
    10k msg/s** with confirms" **L2446-2448**; "**1000** connect/disconnect cycles →
    zero leaked goroutines" **L2457-2458**; **`BenchmarkPublishConfirmed` ≥ 30k msg/s,
    single M-series laptop, local broker, confirms ON, JSON codec** **L2472-2474**;
    **`WithPublisherConnections(4)` + `WithChannelPoolSize(16)` → ≥ 100k msg/s**
    **L2475-2477**; **`BenchmarkPublishBatch` ≥ 5× the single-publish rate**
    **L2478-2479**. **No RTT, no payload size, no broker version, no queue type
    (classic vs quorum) is stated for these numbers; there is no consume-side
    throughput target and no latency SLO anywhere in §9.**
  - §10 Decisions: **decision 31** `PublishBatchMaxSize` is a per-call cap, not a
    sliding in-flight window (default 1024) **L2848-2851**; **R10-18** sync-confirm
    ceiling "`per-Publish throughput is pool/RTT`", "§9 30k/100k targets hold only at
    sub-ms (local-broker) RTT", "deferred to `LATER.md` as LATER-34" **L3150-3157**
    (deferral note **L3039**); highest revision heading **Rev 10** **L3031** (Lens 09
    records **Rev 19**).

- `LATER.md` — **LATER-34** (sync-confirm ceiling / async publish API; **L322-359**)
  is the standing home of the architecture decision; its "suggested solution item 1"
  (document the ceiling honestly now) is exactly what T83/T116 execute. **LATER-43**
  (aggregate confirm window) is filed (Lens-08, **L621**). **LATER-44** is Phase-18's
  *conditional* codec-split reservation (not filed). Lens-09 files **LATER-45**.

- **Code (ground truth confirmed this pass — extends, never duplicates):**
  - `internal/confirms/tracker.go:171` `time.NewTimer(confirmTimeout)` per `Wait`
    (default 30s → **a timer allocated + stopped on every publish and every batch
    element**); `tracker.go:77` `make(chan error, 1)` + `&waiter{}` per `Register`;
    `tracker.go:58` `pending map[uint64]*waiter` (**unordered**); `tracker.go:219-230`
    `resolveUpTo` ranges the **whole** map + `slices.Sort` under `t.mu` → **PC-06/PC-11**.
  - `publisher.go:104` per-acquire release **closure** alloc; `publisher.go:371`
    `p.exchange+" publish"` concat + `:423` `publishSpanAttrs()` slice built
    **unconditionally** (NoOp ignores them); `connection.go:842` `peerAddress()` runs
    `url.Parse` per publish inside the attrs build; `publisher.go:253` default
    `confirmTimeout = 30s`; `publisher.go:444` `injectTrace` **early-returns zero-alloc
    on the no-span path** (do-not-regress) → **PC-07** (+ release-closure noted in §12).
  - `message.go:73` `uuid.NewV7()` (only when `MessageID==""`) → `crypto/rand`
    `io.ReadFull` per call, **`EnableRandPool` never called** (verified), + google/uuid
    `getV7Time` takes a **process-global `timeMu.Lock`** per call; `message.go:77`
    `id.String()` 36-byte alloc → **PC-09**.
  - `codec/json.go:23` default codec is **JSON lax**; `:39` `json.Marshal` allocates
    the body per publish, no buffer pool; `:48-55` consume-side uses
    `json.NewDecoder(bytes.NewReader(...))` + `dec.More()` per delivery (extra
    decoder/reader alloc vs `Unmarshal`) → **PC-08-adjacent / LATER-45**.
  - `internal/headers/xdeath.go:32` `make(map[string]int)` **before** the `tbl==nil`
    (`:33`), `x-death` absent (`:36-38`), and `![]any` (`:40-42`) early returns → a map
    allocated on **100 % of deliveries** including the common no-DLX path → **PC-08**.
  - `consumer.go:487-526` per-channel **reader** is a pure frame-pump into the
    prefetch-sized `out` chan (`:476`); `consumer.go:541` decode, `:549` `newDelivery`
    (with the x-death map), `:571` span start + `:716` `processSpanAttrs` slice, `:553`
    `context.WithCancelCause` **all run on the bounded dispatch goroutine** (cap =
    `concurrency`), **not** the reader → decode is parallelised; the per-TCP I/O
    serialization is the real ceiling → **PC-07/PC-10** (do-not-regress: reader stays light).
  - `metrics/prometheus.go:125/130/235-236` hot-path uses **`WithLabelValues(...)`**
    (no `Labels{}` map; verified `grep prometheus.Labels{` empty) but does **not** cache
    the resolved child `Observer`/`Counter` → per-message hash + `RWMutex` lookup →
    **PC-15**; `metrics/noop.go` empty bodies (do-not-regress).
  - `github.com/rabbitmq/amqp091-go@v1.11.0/connection.go:98/529-555` **`sendM`
    mutex** serialises all `WriteFrame`s; `:303/:773-798` a **single `reader`
    goroutine** demuxes all inbound frames (`:670-734` `demux`/`dispatchN`). The §1/§6.1
    "serializes I/O per connection" *conclusion* is correct; the §6.1 "one goroutine
    drives the socket" *mechanism* is imprecise for writes (mutex-serialised, not a
    dedicated writer goroutine) → **PC-14** (do-not-regress: the fan-out architecture is right).

### Cross-lens reconciliation (do **not** re-file — project rule)

Nine Lens-09 findings are already owned by a prior-lens / earlier-phase task and must
**extend** it with a `Lens-09 (PC-xx)` acceptance bullet, never spawn a new task:

| Lens-09 finding | Already owned by | Action |
|---|---|---|
| **PC-01** §9 30k/100k local-only; needs the explicit RTT-collapse model **inline** | **T83** (Phase 14; RMQ-11 §9 throughput-honesty wording: "qualify 30k/100k with local-broker/sub-ms-RTT … remote projection … cross-reference LATER-34") | **Extend T83**: bake in the explicit rate-@1/5/10 ms table (§11) as the "remote projection", placed *beside* the §9 numbers (not 680 lines away in LATER-34). |
| **PC-02** confirm-latency → pool-exhaustion → availability cascade; pool-sizing formula | **T116** (Phase 17; SRE-14 §9/§6.2 capacity truth: states the `pool/RTT` ceiling + the `ErrChannelPoolExhausted` cascade + SG-4 injected-RTT test) | **Extend T116**: add the quantified pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT` per conn) so the cascade onset is computable, not just narrated. Confirm SG-4 covers the onset. |
| **PC-03** §9 numbers state no payload size, no queue type (classic vs quorum) | **T44b** (Phase 9; the throughput benchmark suite) | **Extend T44b**: the bench must report+pin payload size and queue type; recommend stating **both** a classic and a quorum number (quorum Raft commit raises confirm latency materially). |
| **PC-04** regression gate is release-tag-only (no per-PR smoke) | **T44b** (Phase 9; "run on every release-candidate tag … nightly drift report") | **Extend T44b**: document the cadence limitation (perf can rot between releases on a normal PR); optionally add a lightweight CI microbench smoke or state the gap explicitly. |
| **PC-05** `≥ 5×` batch target pegged to the local single-publish baseline | **T44b** (Phase 9; "`PublishBatch` ≥ 5× `Publish`") | **Extend T44b** (+ §9 wording in T149): reframe to an RTT-stated absolute; document `PublishBatch` as *the* scale path (RTT-decoupled); note 5× is a floor that ~20× understates the remote benefit. |
| **PC-06** per-publish/per-batch-element confirm-`Wait` `time.Timer` alloc | **T11** (Phase 2; `internal/confirms` tracker; `multiple=true` semantics) | **Extend T11**: pool/reset the per-`Wait` timer (or a per-tracker timer wheel) so the default 30s `ConfirmTimeout` does not allocate a timer on every publish; `AllocsPerRun` guard. |
| **PC-08** x-death `byReason` map allocated on every delivery before the absence check | **T17** (Phase 3; `delivery.go` + `internal/headers/xdeath.go` x-death parsing) | **Extend T17**: allocate `byReason` lazily *after* the `tbl==nil` / absent / `![]any` early returns → zero map alloc on the common no-DLX delivery; `AllocsPerRun` guard. |
| **PC-09** UUIDv7 per publish: un-pooled entropy + global `timeMu` lock + 36-byte string | **T10** (Phase 2; `message.go` default-apply; `MessageID ← UUID v7`) | **Extend T10**: call `uuid.EnableRandPool()` once at init (batches `crypto/rand` reads); document the google/uuid process-global `timeMu` serialization at the billions/day bar (and that `MessageID` is load-bearing for at-least-once dedupe, so it cannot be skipped). |
| **PC-10** consume single-channel scaling note (§6.3) | **T18** (Phase 3; `consumer.go` + non-blocking dispatcher + sizing formula; already carries Lens-08 CR-06) | **Extend T18**: state that one consumer = one channel = one reader on one TCP, so beyond the per-TCP I/O ceiling raising `Concurrency` alone does not help — scale needs more consumer channels/connections (`WithConsumerConnections` / more consumers). (The §9 consume-side *target* + latency SLO is the **new** capstone T149.) |
| **PC-11** `multiple=true` resolution O(outstanding)/frame, not O(resolved) | **T11** (Phase 2; `internal/confirms`; `multiple=true` semantics) | **Extend T11**: track a contiguous confirmed low-water-mark + an ordered index (or min-heap) so `multiple=true` is O(resolved + log n); amend §6.2 to state the real complexity (the "single pass" wording stays true; the implied O(resolved) efficiency becomes real). |
| **PC-12** §6.9 5000 ms top bucket loses the 5–30s capacity tail | **T113** (Phase 16; SRE-11 — already extends the default buckets with `10s/30s/60s`) | **Confirm T113** (do-not-regress): verify the extended buckets span the confirm-RTT region for capacity p99/p999; add a one-line §6.9 capacity-tail rationale. No re-file. |

Coordination (not re-file — distinct findings that touch adjacent code):
- **PC-05/PC-10** (the §9 *criteria* changes: batch-as-scale-path wording, the new
  consume-side throughput target, the missing latency SLO) are cross-cutting §9 work →
  owned by the **capstone T149**, which cites T44b (bench) and T18 (consume sizing).
- **PC-07/PC-15** (the NoOp-tracer argument-construction tax and the Prometheus
  child-collector caching) span publisher + consumer + `otel` + `metrics` with no single
  owner → owned by the **new T148** (hot-path allocation hardening), which must thread
  decision 3 (single instrumentation code path) — see D2.
- **PC-14** (the 50k/socket source + the §6.1 write-mechanism wording) → **extend T44b**
  for the measured ceiling; the one-line §6.1 wording fix lands in T149.
- **PC-06/PC-08/PC-09** land their *implementation* in their owning task (T11/T17/T10)
  but their **combined `AllocsPerRun` hot-path guard** is asserted by **T148** (so a
  future regression on any of them fails one test).

---

## 3. Constraints & working agreements (planner must honour)

1. **Amend SPEC before code, same PR** (CLAUDE.md). Each task updates the cited
   §/decision, then lands the hot-path fix/test. The first Phase-20 task records §10
   **Rev 19**.
2. **Cross-lens shared findings extend the owning task** — see the table above. Never
   re-file. T147/T148/T149 are the only *new* tasks. **Do not re-file the capacity
   honesty headline** — it is T83/T116's; Lens-09 only contributes the model table and
   the sizing formula into them.
3. **A performance number is a finding unless it carries RTT + hardware + broker config
   + queue type + payload size** (lens rule). The §9 targets carry hardware + locality +
   codec but not RTT/queue-type/payload — PC-03 closes the gap; PC-01 supplies the model.
4. **Reduce every throughput number to its model** (`in-flight / latency`), back-solve
   the implied RTT, re-project @1/5/10 ms (§11). "Achievable only on a local broker" is
   a **headline caveat beside the number**, not a LATER footnote (lens rule) — satisfied
   by extending T83/T116 in-place, not by LATER-34 (the *architecture* decision stays
   in LATER-34; the *honesty note* is already pulled into §9/§6.2 by T83/T116).
5. **No avoidable allocation on the per-message hot path.** Each PC-06/07/08/09 fix
   lands with an `AllocsPerRun` (or `-benchmem`) guard asserting the alloc is gone at
   the NoOp/default config; T148 owns the combined guard.
6. **Decision 3 (single instrumentation code path, no tracing-disabled branch) is
   preserved.** PC-07 gates the *argument construction*, not the `Start`/`Record` call
   — one call site remains; there is no `if tracer != nil` behavioral branch (D2).
7. **Decision 31 (`PublishBatchMaxSize` is a per-call cap) stays closed; the
   async-publish API stays LATER-34.** PC-05/PC-13 document the batch path and the cap
   trade-off; they do **not** reopen the sliding-window decision.
8. **No new build-tag lane** — allocation gates ride unit / `-benchmem` /
   `AllocsPerRun`; RTT/throughput/queue-type gates ride the existing `integration` lane
   + the T44b release-tag bench cadence; the injected-RTT gate coordinates with T116's
   SG-4. Mirrors Lenses 04–08.
9. **testify + goleak** in every `_test.go`; route any new broker-touching errors
   through `internal/amqperror`. Codec/wire-format choices remain interop-grounded.
10. **Distinguish the three states per finding** (lens rule): *misleading / local-only*
    (PC-01, PC-05), *underspecified — missing conditions* (PC-03, PC-04, PC-10, PC-13,
    PC-14), *honest+correct, lock it in / fix the cost* (PC-06, PC-07, PC-08, PC-09,
    PC-11, PC-12, PC-15). The brief labels each.

---

## 4. Pre-work: verification gates (PG-1..PG-6 — sequence FIRST; they gate wording/fixes)

Each gate captures **ground truth** (a measured allocs/op, a measured rate vs RTT, a
measured O(n) curve) before any SPEC wording or fix lands (gate-first, mirroring
T84/T94/T101/T111/T119/T130/T143). No downstream task edits the spec or the hot path
for its finding before its gate returns. Results go into a committed table under
`spec-validation/` (cited by each downstream task). The first task records §10 **Rev 19**.

| Gate | Question (ground truth to capture) | Lane | Blocks |
|------|-----------------------------------|------|--------|
| **PG-1** | Measure **publish** hot-path allocs/op (`-benchmem` + `testing.AllocsPerRun`) for `Publish` at NoOp tracer + default config; attribute each alloc — the confirm-`Wait` `time.Timer`, the span name concat + attrs slice + `peerAddress`/`url.Parse`, the `waiter`+`done` chan, the UUIDv7 string, the JSON body, the release closure. | unit | PC-06/07/09 → T11/T148/T10 |
| **PG-2** | Measure **consume** hot-path allocs/op per delivery at NoOp tracer + **no x-death header** (the common path); confirm the x-death `map` alloc lands on the no-DLX delivery, plus the span-arg slice and the `context.WithCancelCause`. | unit | PC-08/07 → T17/T148 |
| **PG-3** | Microbench `resolveUpTo` cost vs in-flight depth D (16/256/1024) for a `multiple=true` frame resolving **one** tag; show the per-frame cost grows **O(D)** (whole-map scan + sort) while holding `t.mu`. | unit | PC-11 → T11 |
| **PG-4** | Measure the **single-socket** publish-confirm ceiling (1 conn, sweep `WithChannelPoolSize`) to **source** the "~50k msg/s per socket" figure with a real number and locate the `sendM`-writer-serialization knee (where adding channels stops helping). | integration / bench | PC-14 → T44b/T07d |
| **PG-5** | Injected-RTT publish-confirm bench at RTT ∈ {~0, 1, 5, 10} ms (default pool, then `4×16`) → produce the §11 model-table numbers and the `ErrChannelPoolExhausted` onset under a confirm-latency spike. **Coordinates with / extends T116's SG-4.** | integration | PC-01/02 → T83/T116 |
| **PG-6** | Full-conditions bench: run `BenchmarkPublishConfirmed`/`PublishBatch` recording RTT + **payload size** + broker version + **queue type**, and demonstrate the **classic-vs-quorum** confirm-latency delta on the same target (so the §9 number is interpretable). | integration / bench | PC-03/05 → T44b/T149 |

Findings without a live gate (doc/wording/do-not-regress, ground truth already known
from the code audit): PC-04 (regression-gate cadence — doc), PC-10 (consume scaling —
doc + the §9 target is capstone), PC-12 (histogram tail — confirm T113), PC-13
(`PublishBatchMaxSize` trade-off — doc), PC-15 (Prometheus child caching — micro-opt +
guard).

---

## 5. Workstreams (grouped findings, sequenced)

### WS-1 — Capacity-honesty model (confirm + contribute, do **not** re-file)

- **PC-01** (High, misleading) → **extend T83**. Bake the explicit rate-@1/5/10 ms
  table (§11) into §9 beside the 30k/100k numbers as the "remote projection". *dep PG-5.*
- **PC-02** (Med-High, cascade) → **extend T116**. Add the pool-sizing-for-rate formula
  (`pool ≥ target_rate × confirm_RTT` per conn) quantifying the `ErrChannelPoolExhausted`
  onset; confirm SG-4 covers it. *dep PG-5.*

### WS-2 — Hot-path allocation hardening *(the net-new performance debt)*

- **PC-06** (Med-High, alloc) → **extend T11**. Pool/reset the per-`Wait` `time.Timer`
  (default `ConfirmTimeout=30s` allocates one per publish + per batch element). *dep PG-1.*
- **PC-08** (Med, alloc) → **extend T17**. Lazy-allocate the x-death `byReason` map
  after the early returns → zero map alloc on the no-DLX delivery (100 % of the common
  path). *dep PG-2.*
- **PC-09** (Med, alloc/lock) → **extend T10**. `uuid.EnableRandPool()` once at init;
  document the process-global `timeMu` serialization. *dep PG-1.*
- **PC-07** (Med, alloc) → **T148** (NEW). Gate the span argument-construction (name
  concat + attrs slice + `peerAddress`/`url.Parse`) behind a precomputed `tracingActive`
  flag on both publish and consume paths, **preserving decision 3** (one `Start` call
  site; only the args are gated — D2). *dep PG-1/PG-2.*
- **PC-15** (Low, micro-opt) → **T148** (NEW). Resolve the Prometheus child
  `Observer`/`Counter` for the fixed-outcome label sets once at build time; avoid the
  per-message `WithLabelValues` hash + `RWMutex` lookup.
- **(guard)** **T148** owns the combined `AllocsPerRun` hot-path guard asserting
  PC-06/07/08/09/15 collectively (so any regression fails one test).

### WS-3 — Confirm-resolution complexity *(algorithmic)*

- **PC-11** (Med, complexity) → **extend T11**. Track a contiguous low-water-mark +
  ordered index so `multiple=true` is O(resolved + log n), not O(outstanding); amend
  §6.2 to state the real complexity. *dep PG-3.* (The one-shot resolve / `Wait`
  self-delete / `CloseAll` mechanism is **do-not-regress** — change only the scan/order.)

### WS-4 — Benchmark methodology & §9 criteria *(underspecified → stated)*

- **PC-03** (Med, underspecified) → **extend T44b**. Bench reports + pins payload size
  + queue type; state both a classic and a quorum number. *dep PG-6.*
- **PC-04** (Low-Med, underspecified) → **extend T44b**. Document the release-tag-only
  regression cadence; optionally add a CI microbench smoke.
- **PC-05** (Med, misleading) → **extend T44b** (+ §9 wording in **T149**). Reframe the
  `5×` ratio to an RTT-stated absolute; document `PublishBatch` as *the* scale path. *dep PG-6.*
- **PC-14** (Low, underspecified) → **extend T44b**. Source the ~50k/socket figure
  (PG-4) and correct the §6.1 write-mechanism wording (in T149). *dep PG-4.*

### WS-5 — Pin & capstone *(stated + asserted)*

- **PC-10** (Med, underspecified) → **extend T18** (consume scaling note) + **T149**
  (the missing §9 consume-side throughput target + the missing publish/handle latency
  SLO).
- **PC-13** (Low, underspecified) → **extend T22**. Document the
  `PublishBatchMaxSize=1024` memory/throughput trade-off + sizing guidance.
- **PC-12** (Low, do-not-regress) → **confirm T113**. Verify the extended buckets cover
  the confirm-RTT capacity tail; add a §6.9 rationale line.
- **T149** (NEW, capstone): §9 performance-model appendix (the §11 table referenced),
  the new consume-side throughput target + latency SLO (PC-10), the batch-as-scale-path
  wording (PC-05), the §6.1 write-mechanism wording fix (PC-14), and assertions that the
  WS-2/WS-3/WS-4 controls (T11/T17/T10/T148/T44b) landed. Lands last.

---

## 6. Do-not-regress list (confirmed-correct — protect with tests)

These are the contracts the audit found **already correct**. Reverting any flips the
lens toward NO-GO; each must keep (or gain) a guard.

1. **The multi-TCP role-split fan-out is the right answer to the single-socket
   ceiling.** `amqp091-go` v1.11.0 *does* serialise per connection (`sendM` mutex over
   all writes `connection.go:529-555`; a single `reader` goroutine `:303/:773-798`), so
   extra channels on one socket do not add I/O parallelism — exactly what §1/§6.1's
   `WithPublisherConnections`/`WithConsumerConnections` address. (PC-14 only sources the
   number and fixes the "one goroutine drives the socket" *write* wording.)
2. **Prometheus uses `WithLabelValues`, not `prometheus.Labels{}` maps** — no
   per-message label-map allocation (`prometheus.go:125/130/235`). (PC-15 caches the
   resolved child; it does **not** reintroduce a map.)
3. **Trace injection zero-allocates on the no-span path** (`publisher.go:444` early
   return when no active span). The PC-07 `tracingActive` gate must preserve this (do
   not regress the no-span fast path to always-build).
4. **Decode runs on the bounded dispatch goroutines, not the per-channel reader.** The
   reader (`consumer.go:487-526`) is a light frame-pump into the prefetch-sized `out`
   chan; decode/x-death/span/handler run on up to `Concurrency` dispatch goroutines
   (`consumer.go:541+`). (PC-10 documents scaling; it must **not** move decode onto the
   reader.)
5. **NoOp tracer and NoOp metrics are genuinely zero-work** (empty method bodies,
   `otel/tracer.go:72`, `metrics/noop.go`). The tax PC-07 removes is the *argument
   construction* before the call, not the call.
6. **`PublishBatch` amortizes channel acquire / release closure / in-flight gauge /
   barrier wait across N, and opens no span** (`publisher.go:744-857`). PC-06/09/11
   shave the per-element residue; the amortization stays.
7. **`buildPublishing` returns by value; `amqp091.Table(headers)` is a zero-cost
   conversion; non-mandatory publishers skip all `returnTagMap` `sync.Map` churn**
   (`publisher.go:629/945/636`).
8. **The confirm tracker's one-shot resolve, `Wait`-deletes-own-entry, and `CloseAll`
   are correct** (Lens-08 do-not-regress; `tracker.go:211-214/178-191/126-134`). PC-11
   changes only the *order/scan* of `resolveUpTo`, never the resolve mechanism.
9. **Decision 31 is correct**: `PublishBatchMaxSize` is a per-call cap, not a sliding
   in-flight window. PC-13 documents the trade-off; the async window stays LATER-34.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Class | Disposition | Task |
|---------|-----|-------|-------------|------|
| **PC-01** §9 30k/100k local-only (imply ~0.27–0.64 ms RTT); needs the model inline | High | misleading | **CROSS-LENS — extend T83** (the RTT-collapse table; confirms T116) | **T83** (dep PG-5) |
| **PC-02** confirm-latency → `ErrChannelPoolExhausted` → availability cascade; sizing formula | Med-High | capacity-risk | **CROSS-LENS — extend T116** (pool-sizing-for-rate formula) | **T116** (dep PG-5) |
| **PC-06** per-publish/per-batch-element confirm-`Wait` `time.Timer` alloc (default 30s) | Med-High | alloc | **CROSS-LENS — extend T11** (pool/reset timer) | **T11** (dep PG-1) |
| **PC-08** x-death `byReason` map allocated on every delivery before absence check | Med | alloc | **CROSS-LENS — extend T17** (lazy map) | **T17** (dep PG-2) |
| **PC-09** UUIDv7 per publish: un-pooled entropy + global `timeMu` + 36-byte string | Med | alloc/lock | **CROSS-LENS — extend T10** (`EnableRandPool` + doc) | **T10** (dep PG-1) |
| **PC-07** NoOp tracer still builds span name + attrs + `url.Parse` per publish & per delivery | Med | alloc | NEW — gate arg-construction, keep decision 3 (D2) | **T148** (dep PG-1/2) |
| **PC-11** `multiple=true` resolution O(outstanding)/frame, not O(resolved) | Med | complexity | **CROSS-LENS — extend T11** (low-water-mark + ordered index) | **T11** (dep PG-3) |
| **PC-03** §9 numbers state no payload size, no queue type (classic vs quorum) | Med | underspecified | **CROSS-LENS — extend T44b** | **T44b** (dep PG-6) |
| **PC-05** `≥ 5×` batch target pegged to local baseline; batch is *the* scale path | Med | misleading | **CROSS-LENS — extend T44b** (+ §9 wording in T149) | **T44b/T149** (dep PG-6) |
| **PC-10** consume single-channel scaling (§6.3) + missing §9 consume target & latency SLO | Med | underspecified | **CROSS-LENS — extend T18** (scaling) + NEW §9 criteria (capstone) | **T18/T149** |
| **PC-04** regression gate release-tag-only (no per-PR smoke) | Low-Med | underspecified | **CROSS-LENS — extend T44b** | **T44b** |
| **PC-13** `PublishBatchMaxSize=1024` memory/throughput trade-off undocumented | Low | underspecified | **CROSS-LENS — extend T22** | **T22** |
| **PC-14** "~50k msg/s per socket" unsourced; §6.1 write-mechanism wording imprecise | Low | underspecified | **CROSS-LENS — extend T44b** (measure) + §6.1 fix (capstone) | **T44b/T149** (dep PG-4) |
| **PC-12** §6.9 5000 ms top bucket loses the 5–30s capacity tail | Low | do-not-regress | **CONFIRM T113** (SRE-11 already extends buckets) + rationale line | **T113** |
| **PC-15** per-message Prometheus `WithLabelValues` hash+lock not cached | Low | micro-opt | NEW — cache child collectors at build | **T148** |

Every `PC-01..PC-15` is accounted for exactly once: PC-01→T83, PC-02→T116, PC-03→T44b,
PC-04→T44b, PC-05→T44b (+T149), PC-06→T11, PC-07→T148, PC-08→T17, PC-09→T10,
PC-10→T18 (+T149), PC-11→T11, PC-12→T113, PC-13→T22, PC-14→T44b (+T149), PC-15→T148.

**New tasks (Phase 20):** T147 (gates PG-1..PG-6; records §10 Rev 19), T148 (PC-07 +
PC-15 hot-path allocation hardening + the combined `AllocsPerRun` guard), T149
(capacity & performance capstone: §9 model appendix + consume target + latency SLO +
batch-as-scale-path + §6.1 wording + assert controls). **Extended in place:** T10, T11,
T17, T18, T22, T44b, T83, T116; **confirmed (do-not-regress):** T113.

---

## 8. Suggested phasing (planner may revise — "Phase 20")

- **A — Gates (T147).** Stand up PG-1..PG-6 ground truth into a committed results
  table; records §10 **Rev 19**. No new build-tag lane.
- **B — Hot-path allocations (T11/CR↔PC-06, T17/PC-08, T10/PC-09, T148/PC-07+PC-15).**
  Timer pool, lazy x-death map, `EnableRandPool`, span-arg gating, Prometheus caching;
  the combined `AllocsPerRun` guard.
- **C — Confirm complexity (T11/PC-11).** Low-water-mark + ordered index for
  `multiple=true`; §6.2 complexity wording.
- **D — Capacity model & benchmark methodology (T83/PC-01, T116/PC-02, T44b/PC-03+04+05+14).**
  The RTT-collapse table, the sizing formula, payload/queue-type/cadence, the sourced
  50k figure.
- **E — Pin & capstone (T18/PC-10, T22/PC-13, T113/PC-12, T149).** Consume scaling,
  batch-cap trade-off, histogram-tail confirm, then the §9 capstone (model appendix +
  consume target + latency SLO + batch-as-scale-path + §6.1 wording + assert B–D).

Sequencing notes: T11(PC-06)/T10/T148 depend on PG-1; T17 on PG-2; T11(PC-11) on PG-3;
T44b(PC-14) on PG-4; T83/T116 on PG-5; T44b(PC-03/05) on PG-6. T18/T22/T113/PC-04 are
gate-independent (doc). T149 lands last (it asserts the B–D controls and the §11 table).

---

## 9. Acceptance criteria for the whole effort

- **Capacity model inline (PC-01/PC-02):** §9 carries the explicit rate-@1/5/10 ms
  table beside the 30k/100k numbers (extends T83); §6.2/§9 carry the
  pool-sizing-for-rate formula and the `ErrChannelPoolExhausted` onset (extends T116);
  the async-publish API stays LATER-34, decision 31 stays closed.
- **Publish hot path (PC-06/07/09):** an `AllocsPerRun` test asserts `Publish` at NoOp
  tracer + default config no longer allocates the confirm-`Wait` timer, the span
  name/attrs/`url.Parse`, or (via `EnableRandPool`) the per-call entropy buffer; the
  `timeMu` global-lock cost is documented; the no-span trace-injection fast path is
  unchanged.
- **Consume hot path (PC-08/07):** an `AllocsPerRun` test asserts a no-x-death,
  NoOp-tracer delivery allocates **no** x-death map and **no** span name/attrs slice;
  decode still runs off the reader goroutine.
- **Confirm complexity (PC-11):** a microbench shows `multiple=true` resolution is
  O(resolved + log n), not O(outstanding); §6.2 states the real complexity; the
  one-shot resolve / self-delete / `CloseAll` mechanism is unchanged.
- **Benchmark methodology (PC-03/04/05/14):** the bench reports + pins RTT + payload
  size + broker version + **queue type**, with both a classic and a quorum number; the
  `PublishBatch` target is an RTT-stated absolute with batch documented as the scale
  path; the release-tag-only regression cadence is documented; the ~50k/socket figure
  is replaced with a measured single-socket ceiling and the `sendM` knee.
- **Pin & capstone (PC-10/PC-13/PC-12/§9):** §6.3 states consume scaling needs more
  channels/connections beyond the per-TCP ceiling; §9 gains a consume-side throughput
  target **and** a publish/handle latency SLO (p99/p999); §6.2 documents the
  `PublishBatchMaxSize` trade-off; §6.9's extended buckets (T113) are confirmed to span
  the confirm-RTT tail; §6.1's write mechanism is described accurately.
- **Project gates green:** `make lint test` (race + cover), `-benchmem`/`AllocsPerRun`
  guards, integration on 3.13 **and** 4.x, the T44b bench cadence. `README.md` reflects
  any changed external contract (none expected — the fixes are internal hot-path + spec
  wording; `EnableRandPool` and the timer pool are internal).

---

## 10. Open questions for the owner (decisions the planner needs to record)

1. **D1 — PC-06/07/15 allocation-hardening depth + LATER-45.** Recommend: land the
   **cheap, zero-risk** wins now (timer pool/reset in T11; span-arg gating in T148;
   `EnableRandPool` in T10; lazy x-death map in T17; Prometheus child caching in T148),
   and **defer the deeper wins to LATER-45** — a pooled-buffer codec `Encode` (`sync.Pool`
   `bytes.Buffer` + `json.Encoder` to cut the per-message body churn) and a UUIDv7
   generator without the google/uuid process-global `timeMu` lock (both larger, with
   codec-API / dependency implications). Owner may pull either forward.
2. **D2 — PC-07 vs decision 3 (single instrumentation code path).** Recommend: gate
   only the *argument construction* (the attrs slice, the span-name concat, the
   `peerAddress`/`url.Parse`) behind a precomputed `tracingActive bool` (set once:
   `_, isNoOp := tracer.(NoOpTracer)`), keeping a **single** `Start`/`Record` call site
   with **no** `if tracer != nil` behavioral branch — so decision 3's intent (uniform
   path, no tracing-disabled branch) holds while the wasted allocation is removed.
   Document the reconciliation in §6.9/decision 3. Alternative: accept the tax (rejected
   at billions/day).
3. **D3 — PC-11 `multiple=true` complexity mechanism.** Recommend: a contiguous
   confirmed low-water-mark plus an ordered index (or a min-heap keyed by delivery-tag)
   → O(resolved + log n). Alternative: keep O(outstanding) and merely amend §6.2 to drop
   the implied efficiency (cheaper, but leaves a per-frame O(n) under the tracker mutex
   at deep in-flight — disfavoured at the billions/day bar).
4. **D4 — PC-03 benchmark queue type.** Recommend: require the bench to state **both** a
   classic-queue and a quorum-queue number (quorum's majority Raft commit raises confirm
   latency materially, so a single "100k" without the queue type is uninterpretable).
   Alternative: pin classic only (cheaper, hides the quorum cost).
5. **D5 — PC-10 §9 consume-side criteria.** Recommend: add a consume-side throughput
   target (e.g. `BenchmarkConsume ≥ 30k msg/s` at `Concurrency(8)+Prefetch(256)` — the
   number T44b already benches but §9 does not encode) **and** a publish/handle latency
   SLO (p99/p999 against the §6.9 histogram) to §9, since "billions/day" with no
   consume target and no latency SLO is an incomplete success bar. Alternative: leave §9
   publish-only (disfavoured).

**Proposed LATER entries:** **LATER-45** — deeper hot-path allocation elimination
(pooled-buffer codec `Encode` + a UUIDv7 generator without the process-global `timeMu`
lock), prereq **T148**, conditional on D1 = defer the deep wins. (Phase-18's conditional
**LATER-44** codec-sub-module reservation stands; Lens-09 takes the next gapless filed
number, LATER-45.)

---

## 11. Throughput model table (lens output #1)

Model: `Publish` holds one pooled channel for a full confirm round-trip, so the
sustainable rate is `in-flight_window / RTT`, where `in-flight_window = publisher_conns
× channel_pool_size` (the cap on concurrent in-flight publishes). The implied RTT is
back-solved from the stated number; the @1/5/10 ms columns re-project the same window at
realistic remote RTTs. (`PublishBatch` pipelines up to `PublishBatchMaxSize` on one
channel, so it is **RTT-decoupled** and bounded instead by the `sendM` writer +
tracker, not by `window/RTT` — that is the whole point of the path.)

| Target (§9) | Stated number | In-flight window | Implied RTT | Rate @1 ms | Rate @5 ms | Rate @10 ms | Honest? |
|---|---|---|---|---|---|---|---|
| §9 L2473 single conn | **≥ 30k msg/s** | 8 ch (default `WithChannelPoolSize=8`, 1 conn — **§9 does not pin the pool size**, a gap) | **≈ 0.27 ms** | ~8k | ~1.6k | ~0.8k | **Local-only** — needs a sub-ms (loopback) broker; collapses 1–2 orders of magnitude remote |
| §9 L2476 multi-conn | **≥ 100k msg/s** | 64 ch (`4 conns × 16 ch`) | **≈ 0.64 ms** | ~64k | ~12.8k | ~6.4k | **Local-only** — same collapse; the headline fan-out number is a loopback number |
| §9 L2478 batch ratio | **≥ 5× single** | up to 1024 pipelined / channel (RTT-decoupled) | n/a (not `window/RTT`) | ≫ 5× | ≫ 5× | ≫ 5× | **Misleading baseline** — `5×` is pegged to the *local* single-publish rate (where sync is already writer-bound, so 5× is *hard*); at remote RTT pipelining beats sync by ~100×+, so `5×` understates by ~20× and the ratio is meaningless without a stated RTT |
| §9 L2447 chaos floor | **10k msg/s, 5-min outage** | reliability floor, not a steady-state capacity number | n/a | — | — | — | **Honest** as a chaos/zero-loss floor (not a throughput claim) |
| consume throughput | **(none in §9)** | — | — | — | — | — | **Missing** — no §9 consume-side target (T44b benches `≥30k` but §9 does not encode it) — PC-10 |
| publish/handle latency | **(none in §9)** | — | — | — | — | — | **Missing** — no p99/p999 latency SLO anywhere in §9, despite the §6.9 histogram existing — PC-10 |

**Dominant bottleneck at the target rates.** In the *degenerate synchronous* (shallow
in-flight) case, **RTT** dominates (`window/RTT`). Once pipelined (`PublishBatch`, or
deep confirms), RTT stops being the per-connection ceiling and the **single-writer
serialization** dominates: every outbound frame contends on `amqp091`'s per-connection
`sendM` mutex and every inbound confirm is parsed by the one `reader` goroutine — extra
channels on the same socket add **no** I/O parallelism (verified v1.11.0). That is
exactly why the multi-TCP role-split fan-out exists. Codec/alloc (the §12 ledger) and
lock contention (the tracker mutex during the O(n) `multiple=true` scan PC-11; the
google/uuid global `timeMu` PC-09) are the next contributors at the billions/day bar.

---

## 12. Hot-path allocation ledger (lens output #2)

Per-message heap allocations / locks at the **NoOp tracer + default config** (the
common case). `necessary` = required for the contract; `avoidable` = removable without
changing behaviour. Verified against the implemented code.

### Per `Publish`

| Step | file:line | Allocates / locks | Necessary / Avoidable | Fix | Finding |
|---|---|---|---|---|---|
| confirm-`Wait` `time.Timer` | `tracker.go:171` (default `ConfirmTimeout=30s`, `publisher.go:253`) | **a `time.Timer` per publish + per batch element** | **Avoidable** | pool/reset the timer or a per-tracker timer wheel | **PC-06** |
| span name + attrs + `peerAddress` | `publisher.go:371`/`:423`, `connection.go:842` | **string concat + `[]attribute.KeyValue` + `url.Parse` built even under NoOp** | **Avoidable** | gate arg-construction on `tracingActive` (keep 1 call site, D2) | **PC-07** |
| UUIDv7 entropy + string | `message.go:73`/`:77` (when `MessageID==""`) | `crypto/rand` read (no pool) + process-global `timeMu` lock + 36-byte string | **Avoidable** (entropy/lock) / necessary (string) | `uuid.EnableRandPool()`; document `timeMu` | **PC-09** |
| acquire release closure | `publisher.go:104` | a closure capturing `entry` per acquire | **Avoidable** | method value / direct `pool.release(entry)` | (PC-07 task; minor) |
| JSON body | `codec/json.go:39` | `json.Marshal` body, no buffer pool | **Necessary** (body must exist) / poolable | pooled `bytes.Buffer` + `json.Encoder` | **LATER-45** (D1) |
| `waiter` + `done` chan | `tracker.go:77` | `&waiter{}` + `make(chan error,1)` per `Register` | **Necessary** (the sync primitive) / poolable | `sync.Pool` of `*waiter` recycled in `Wait` | **LATER-45** (D1) |
| trace injection (no span) | `publisher.go:444` | **zero alloc** (early return) | **do-not-regress** | preserve under the PC-07 gate | DNR #3 |
| Prometheus record | `prometheus.go:125/130` | `WithLabelValues` hash + `RWMutex` lookup (no map) | **Avoidable** (the lookup) | cache child `Observer`/`Counter` at build | **PC-15** |

### Per delivery (consume)

| Step | file:line | Allocates / locks | Necessary / Avoidable | Fix | Finding |
|---|---|---|---|---|---|
| x-death `byReason` map | `xdeath.go:32` | **`make(map[string]int)` on every delivery, before the absence check** | **Avoidable** (no-DLX path) | lazy-allocate after the early returns | **PC-08** |
| span name + attrs | `consumer.go:571`/`:716` | string concat + `[]attribute.KeyValue` built under NoOp | **Avoidable** | gate on `tracingActive` (D2) | **PC-07** |
| `context.WithCancelCause` | `consumer.go:553` | a `cancelCtx` per delivery, even when `handlerTimeout==0` | **Avoidable** (fast path) | skip on the `handlerTimeout==0` poll path | PC-07 task (adjacent) |
| codec `Decode` | `consumer.go:541`, `codec/json.go:48-55` | typed payload + `json.NewDecoder`/`bytes.Reader` (vs `Unmarshal`) | **Necessary** (payload) / decoder avoidable | `Unmarshal` + cheap trailing-byte check, or pool decoders | **LATER-45** (D1) |
| `*Delivery[M]` | `consumer.go:549` (`newDelivery`) | heap struct per delivery (embeds the x-death map) | **Necessary** (RawHandler API) | combine with PC-08 lazy map | (PC-08) |
| span/ctx extract (no traceparent) | `consumer.go:560` | **zero alloc** when header absent | **do-not-regress** | — | DNR |
| Prometheus record | `prometheus.go:235` | `WithLabelValues` hash + lock (no map) | **Avoidable** (the lookup) | cache child at build | **PC-15** |
| reader goroutine | `consumer.go:487-526` | light frame-pump (no decode/parse) | **do-not-regress** | keep decode off the reader | DNR #4 |

---

## 13. Findings table (lens output #3)

`Severity` High/Med/Low · `Class` misleading / underspecified / alloc / complexity /
capacity-risk / do-not-regress.

| ID | Sev | Class | Location (§+lines / file:line) | Performance/capacity concern | Recommended SPEC amendment / fix | Task |
|----|-----|-------|-------------------------------|------------------------------|----------------------------------|------|
| **PC-01** | High | misleading | §9 L2472-2479; §6.1 L530-535; §10 R10-18 L3150-3157 | the 30k/100k targets imply ~0.27–0.64 ms RTT (loopback); at 1/5/10 ms they collapse to ~64k/~12.8k/~6.4k (multi-conn) — the load-bearing caveat is parked in LATER-34, ~680 lines from the numbers | bake the explicit RTT-collapse table (§11) into §9 beside the numbers (extend T83; confirms T116) | **T83** |
| **PC-02** | Med-High | capacity-risk | §6.2 L954-957; §10 R10-18; `publisher.go` pool | a confirm-latency spike (broker GC, disk sync, quorum Raft) raises per-channel hold time → channels saturate → `ErrChannelPoolExhausted` → latency spike becomes an availability event; the coupling is narrated (T116) but not quantified | add the pool-sizing-for-rate formula (`pool ≥ target_rate × confirm_RTT` per conn) + onset (extend T116; SG-4) | **T116** |
| **PC-06** | Med-High | alloc | `tracker.go:171`; `publisher.go:253` | a `time.Timer` allocated + stopped on **every** publish and **every** batch element because the default `ConfirmTimeout=30s` arms the wait — the highest-frequency avoidable alloc on the publish path | pool/reset the timer (or a per-tracker timer wheel); `AllocsPerRun` guard | **T11** |
| **PC-07** | Med | alloc | `publisher.go:371/423`, `connection.go:842`, `consumer.go:571/716` | span name concat + `[]attribute.KeyValue` + `peerAddress`/`url.Parse` built on every publish **and** every delivery even under the NoOp tracer (args evaluated before the no-op `Start` discards them) | gate arg-construction on a precomputed `tracingActive` flag, keeping one `Start` call site (decision 3 intact, D2) | **T148** |
| **PC-08** | Med | alloc | `internal/headers/xdeath.go:32` | `make(map[string]int)` runs **before** the `tbl==nil`/absent/`![]any` early returns → a map allocated on 100 % of deliveries including the common no-DLX path | allocate `byReason` lazily after the early returns; `AllocsPerRun` guard | **T17** |
| **PC-09** | Med | alloc/lock | `message.go:73/77`; google/uuid `version7.go` | UUIDv7 per publish (when `MessageID` unset): un-pooled `crypto/rand` read + a **process-global `timeMu` lock** + a 36-byte string — the global lock is a process-wide serialization point at billions/day | `uuid.EnableRandPool()` once at init; document `timeMu`; note `MessageID` is load-bearing for dedupe | **T10** |
| **PC-10** | Med | underspecified | §6.3 L1171-1219; §9 (absent) | one consumer = one channel = one reader on one TCP, so beyond the per-TCP I/O ceiling raising `Concurrency` alone does not scale; and §9 has **no** consume-side throughput target and **no** latency SLO at all | §6.3 states the scaling lever (more channels/conns); §9 gains a consume target + a p99/p999 latency SLO (capstone) | **T18/T149** |
| **PC-11** | Med | complexity | §6.2 L860-863; `tracker.go:58/219-230` | `resolveUpTo` scans the **whole** unordered `pending` map + `slices.Sort`s on **every** `multiple=true` frame under `t.mu` → O(outstanding)/frame, not the O(resolved) the "single pass … critical for batching" wording implies; per-frame O(n) at deep in-flight | low-water-mark + ordered index → O(resolved + log n); amend §6.2 to state the real complexity | **T11** |
| **PC-03** | Med | underspecified | §9 L2472-2479 | the numbers state hardware + locality + codec but **no payload size and no queue type (classic vs quorum)** — quorum's majority Raft commit raises confirm latency materially, so "100k" is uninterpretable without the queue type | bench reports + pins payload size + queue type; state both a classic and a quorum number (D4) | **T44b** |
| **PC-05** | Med | misleading | §9 L2478-2479; §6.2 L898-900 | `BenchmarkPublishBatch ≥ 5×` is pegged to the *local* single-publish baseline (where sync is already writer-bound, so 5× is hard) and ~20× understates the remote benefit; the ratio is meaningless without a stated RTT | reframe to an RTT-stated absolute; document `PublishBatch` as *the* scale path | **T44b/T149** |
| **PC-04** | Low-Med | underspecified | §7 L2222; §9; T44b | the regression gate is **release-tag-only** (no per-PR/CI smoke) → performance can silently rot between releases | document the cadence limitation; optionally add a lightweight CI microbench smoke | **T44b** |
| **PC-13** | Low | underspecified | §6.2 L930-933; dec.31 L2848-2851 | `PublishBatchMaxSize=1024` bounds tracker memory per call but the memory/throughput trade-off and the "is 1024 the right pipelining window" question are undocumented | document the trade-off + sizing guidance (deeper window = more pipelining vs more tracker memory) | **T22** |
| **PC-14** | Low | underspecified | §1 L53-58; §6.1 L533; amqp091 `connection.go:98/303` | the "~50k msg/s per socket" figure is unsourced; and §6.1 "one goroutine drives the socket" is imprecise for writes (mutex-serialised `sendM`, not a dedicated writer goroutine) | source the figure with a measured single-socket ceiling (PG-4); correct the §6.1 write-mechanism wording | **T44b/T149** |
| **PC-12** | Low | do-not-regress | §6.9 L2073-2076 | the default 5000 ms top bucket collapses 5s–30s (with the 30s `ConfirmTimeout`) into `+Inf`, losing the tail where capacity problems live — **already addressed by T113 (SRE-11)** which adds 10s/30s/60s | confirm T113's extended buckets span the confirm-RTT capacity tail; add a §6.9 capacity-tail rationale line | **T113** |
| **PC-15** | Low | micro-opt | `prometheus.go:125/130/235` | per-message `WithLabelValues` does a label-hash + `RWMutex` map lookup (cheaper than a `Labels{}` map, but the resolved child is not cached) | resolve the child `Observer`/`Counter` for the fixed-outcome label sets once at build | **T148** |

---

## 14. Out of scope for this plan

- Other validation lenses (10 test-strategy, 11 compliance, 12 DX, 13 load) — separate
  passes. (PC-04's "are the numbers reproducible in CI" question coordinates with the
  test-strategy lens; PC-03's queue-type/RTT conditions with the load-testing lens.)
- The **async/streaming publish API** with a bounded in-flight window — stays
  **LATER-34** (architecture decision; not pulled). Decision 31 (`PublishBatchMaxSize`
  is a per-call cap, not a sliding window) stays closed.
- Re-filing the capacity-honesty headline — it is **T83/T116's** (RMQ-11/SRE-14);
  Lens-09 only contributes the model table and the sizing formula into them.
- Re-filing the histogram-bucket fix — it is **T113's** (SRE-11); Lens-09 only confirms it.
- `rpc.go` / `delay.go` performance (not yet implemented; future tasks).
- The deeper allocation wins (pooled-buffer codec, a `timeMu`-free UUIDv7 generator) —
  **LATER-45** (D1 = defer); T148 lands the cheap wins.
- Replacing `amqp091-go`'s per-connection serialization — it is upstream and
  load-bearing; the multi-TCP fan-out is the correct mitigation (do-not-regress).
- Stream-protocol throughput (v0.2, decision 24). Cross-lens shared findings extend the
  owning task (T10/T11/T17/T18/T22/T44b/T83/T113/T116), never re-filed.

---

## 15. Verdict for this lens

**GO-WITH-CHANGES.** The performance architecture is sound where it counts: the
`amqp091-go` per-connection serialization premise is real (verified — `sendM`
mutex-serialised writes + a single reader goroutine, v1.11.0), so the multi-TCP
role-split fan-out (§1/§6.1) is the *correct* answer to the single-socket ceiling;
Prometheus avoids per-message label maps; trace injection zero-allocates on the no-span
path; decode runs off the per-channel reader; and the NoOp tracer/metrics bodies are
empty. **There is no Blocker.** Notably, the capacity-honesty headline this lens exists
to raise — the sync-confirm `pool/RTT` ceiling, the local-only 30k/100k numbers, and the
confirm-latency → `ErrChannelPoolExhausted` cascade — was **already caught and is being
remediated by the SRE lens (T83/RMQ-11 + T116/SRE-14)**, and the histogram capacity-tail
gap by **T113/SRE-11**; Lens-09 confirms their scope (do-not-regress) and contributes
the concrete artifacts they lack (the explicit rate-@1/5/10 ms model table, the
pool-sizing-for-rate formula) rather than re-filing them. What the lens exposes as
net-new is **performance debt the prior lenses did not touch**: a cluster of avoidable
**per-message hot-path allocations** at the billions/day bar — a `time.Timer` on every
`Publish` confirm-wait (PC-06), span name/attrs/`url.Parse` built even under the NoOp
tracer on both publish and consume (PC-07), an x-death `map` on every delivery before
the absence check (PC-08), and an un-pooled UUIDv7 entropy draw + a process-global lock
per publish (PC-09) — plus one **algorithmic** finding (the confirm tracker resolves
`multiple=true` in O(outstanding) per frame under the tracker mutex, not the O(resolved)
its "single pass" wording implies; PC-11) and the **§9-criteria / benchmark-methodology
gaps** (no payload size or queue type on the numbers; no consume-side throughput target
and no latency SLO; the `5×` batch ratio pegged to the wrong baseline). Landing the
allocation hardening with `AllocsPerRun` guards, the ordered `multiple=true` resolution,
the explicit model table + sizing formula into T83/T116, and the benchmark/§9-criteria
sharpening brings the library to a defensible posture at the billions/day, remote-broker
bar. No redesign required; the async-publish API stays in LATER-34 and decision 31 stays
closed.
