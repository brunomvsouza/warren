# Plan Input — Remediate Load & Reliability-Testing Findings (Lens 13)

> **Lens:** Load & Reliability Testing — "are the spec's load-validation **campaigns and harness**
> realistic, complete, and capable of *proving* the billions/day reliability bar under real and
> hostile load?" (`spec-validation/13-load-testing.md`).
> **Persona:** a load- and reliability-testing specialist who designs the campaigns that decide
> whether a system meets its SLOs under realistic and adversarial traffic. Thinks in **workload
> shapes** — baseline, sustained, peak, soak/endurance, spike, stress-to-failure, composite-incident
> — each catching a different defect class; measures **tail latency and saturation**, not just mean
> throughput; insists the **test topology matches production** (a single-node broker cannot validate
> a clustered, quorum-replicated reliability claim); is ruthless about realism (uniform-small-message
> microbenchmarks prove almost nothing about a billions/day flight path).
> **Stays in lane:** does **not** re-derive the throughput *model* (that is Lens-09, owned by
> T44b/T147-T149/T83/T116) and does **not** re-audit per-criterion *falsifiability* (that is Lens-10,
> owned by T150-T152). The lane is load-campaign **realism, completeness, and harness fidelity**.
> **Verdict:** **GO-WITH-CHANGES — no Blocker.** The v0.1 *code* is not defective and the strategy
> is honest about cadence ("nightly", "on release tag"). The defect is that the spec's
> load-validation strategy **cannot prove its own bar**: every clustered reliability claim is
> validated only on a **single-node testcontainers** harness, four of the seven workload shapes
> (soak, spike, stress-to-failure, composite) are **never run**, the chaos intensity (10k msg/s) is
> "billions/day **at average**, never at peak", the generators are unspecified (uniform-small-no-op =
> microbenchmark), and the default histogram tops out at **5000 ms** so the load tail you most need
> (`ConfirmTimeout` 30s, the R10-8 barrier cap ~20s+) clips to `+Inf`. The cheap, mandatory fix is a
> **§9 topology-honesty pass** (label each criterion with the topology it is actually proven against;
> stop implying a clustered guarantee validated single-node). The expensive fix — a **multi-node
> cluster harness + the soak/spike/stress/composite campaign set + realistic generators** — is real
> engineering, phased as net-new tasks, with the *standing* multi-node load environment + load-gen
> fleet (an ops/infra investment) deferred to **LATER-49**. **12 findings (LT-01..LT-12); 5 gates
> (LG-1..LG-5); 6 net-new tasks (T164–T169); 5 cross-lens extensions (T44b/T45/T66/T71/T151);
> confirmations (T45/TV-09 loss-counting, T44b/PC-04 queue-type, T63/R10-8 barrier cap, T59/R10-3,
> the T65 DLQ bound); LATER-49.**

---

## 1. Objective

Make the spec's load-validation strategy **prove the bar it states**, instead of asserting it. The
§1 bar is "trusted in flight paths that handle **billions of messages per day**; **no silent message
loss**, **no silent backpressure failure**, **graceful under chaos**" (`SPEC.md` §1 L37-86). A
reliability claim is only as good as the campaign that exercises it, against a topology that
actually contains the failure mode. The lens is binary and ruthless on three axes:

- **A reliability claim validated only on a single-node broker is *unproven* for a clustered
  production topology.** §7's harness is **testcontainers-go + a single RabbitMQ node** (L2229-2230);
  the `integration` and `chaos` lanes are single-node. But the reliability story is fundamentally
  clustered — quorum queues (3-node Raft), role-split connections, **leader failover under load**,
  **network-partition** handling (`pause_minority`/`autoheal`), and **multi-node reconnect rotation**
  (R10-11/T66). A single-node container cannot load-test quorum leader election/failover under
  traffic, partition behaviour, the reconnect-storm across cluster nodes, or quorum confirm latency
  under Raft replication. The latent proof: **T66 already asserts "no `addr[0]` stampede on a
  full-cluster restart"** (plan.md T66, Lens-02 DS-10 / Lens-05 SRE-04) — an assertion that is
  **unrunnable on the single-node harness it is scheduled against.** Every clustered §9 reliability
  criterion is, today, effectively unproven.

- **A workload shape the spec never runs is a defect class that goes uncaught.** Of {baseline,
  sustained, peak, soak/endurance, spike, stress-to-failure, composite-incident}, the spec runs only
  baseline (implicit in the §9 benchmark) and a fast-cycle reconnect stress. There is **no soak**
  (the 1000-cycle connect/disconnect check, §9 L2457, catches fast-cycle leaks, not 24h
  steady-state growth of the confirm-tracker map / dedupe cache / `(channel-instance-id, MessageID)`
  counter map / fd / Prometheus series cardinality), **no spike**, **no stress-to-failure**, and
  **no composite-incident** campaign. So §1's "no silent backpressure failure" (L48-52) is
  **asserted, never demonstrated** — nothing drives to saturation on purpose to locate the cliff and
  prove every overload path surfaces a classifiable error rather than stalling or OOMing.

- **A microbenchmark is not a load campaign, and an unmeasurable tail is not a measurement.** The
  §9 chaos runs at **10k msg/s** (L2446-2448): 1 billion/day ≈ ~11.6k msg/s *average*, and
  "billions/day" with typical 3–10× peaks means 10k validates **neither a multi-billion/day system
  nor any system at peak** — it is one-billion-at-average. The §9 throughput targets (30k / 100k,
  L2471-2479) are **one-shot benchmark iterations**, not sustained campaigns. The generators are
  unspecified, defaulting to uniform-small-no-op. And the default histogram tops at **5000 ms**
  (§6.9 L2073-2076) while `ConfirmTimeout` is 30s and the R10-8 reconnect barrier blocks to a
  multi-second cap (L3098-3104) — so the **tail you most need under load is clipped to `+Inf`**, and
  the leading-indicator saturation metrics (pool-wait, `consumer_in_flight`,
  `consumer_redelivered_total`) are **deferred to R10-16/T71**, so during a campaign you cannot even
  observe the saturation you are trying to provoke.

The net-new value is the **load-campaign layer no prior lens owns.** Lens-09 owns the throughput
*model* and the *benchmark methodology* (T44b/T147-T149); Lens-10 owns *falsifiability* and the *CI
runner* (T150-T152/T151); Lens-05 owns *observability* (T71); Lens-02 owns *failure-mode* tests
(T45/T59). **None** owns the *workload-shape campaign set*, the *multi-node cluster load harness*, the
*realistic generators*, or the *§9 topology-honesty* that keeps the spec from over-claiming. **There
is no Blocker:** the code works, the strategy is honest about cadence, and the existing chaos +
benchmark + nightly-runner spine is sound. Every finding is a missing campaign, an inadequate
harness, an unrealistic workload, or an unmeasurable-under-load gap — none is a silent
loss/duplicate/leak defect in the implementation.

---

## 2. Source of truth & references

- **SPEC §1** (L37-86): the **billions/day** bar (L39-40); **no silent message loss** (L43-47);
  **no silent backpressure failure** — "callers see a classifiable error … never a silent stall"
  (L48-52, the bar the spike/stress campaigns must *demonstrate*); **no single-TCP-socket
  bottleneck** / role-based pools (L53-58); **no silent poison loop** / two-counter design
  (L59-64); **graceful under chaos** / reconnect re-declare + re-`basic.consume` + ctx-cancel
  (L80-84). **§1 "Success looks like"** (L92-103): "Connection loss is invisible besides metric
  blips" (L98-99) — a claim only a multi-node failover campaign can substantiate.
- **SPEC §6.1** (L467-763): multi-connection fan-out (`WithPublisherConnections`/
  `WithConsumerConnections`, default 2+2), the `n=1` `Dial`-time warning, the **blocked-wait** path
  (`ErrConnectionBlocked`), the synchronous reconnect barrier — the elasticity surface a spike
  campaign drives.
- **SPEC §6.3** (L1070-1369): `Prefetch` (the backpressure cap on unacked), `Concurrency`, the
  `Prefetch < Concurrency` `Build`-time warning, the poison-loop two-counter mechanism, the
  classic-vs-quorum prefetch nuance — the dials a spike/stress campaign exercises.
- **SPEC §6.9** (L1981-2208): **histogram buckets default `[0.5,1,2,5,10,25,50,100,250,500,1000,
  5000]` ms, override via `WithLatencyBuckets`** (L2073-2076) — the top bucket clips the load tail;
  the mandatory metric set (L2078-2091) — note **absent**: pool acquire-wait,
  `consumer_in_flight`, `consumer_redelivered_total` (deferred to R10-16/T71).
- **SPEC §7** (L2212-2329): **Levels** (L2214-2222: unit/integration/conformance/fuzz/bench;
  benchmark "On-demand and on release tag"); **Frameworks** — "testcontainers-go + the official
  RabbitMQ module; pinned image tags for 3.13 LTS and 4.x" (L2229-2230, **single-node**);
  **Reconnect chaos test** (L2241-2247: "60s outage at 1k msg/s" residual prose, reconciled to
  5-min @ 10k by T45/TV-10); **Poison-loop test** (L2249-2254); **Conformance** (L2256-2262,
  single-node + "test AMQP server stub" being dropped by T44/TV-06); **Memory and concurrency**
  (L2264-2269: "Nightly 100x stress loop of connect/disconnect", reconciled to 1000 by T45/CR-10);
  **Executable examples at checkpoints** (L2271-2329, `examples-build`/`examples-smoke`).
- **SPEC §9** (L2426-2549): **Reliability** — chaos "**5-minute outage at 10k msg/s** … nightly;
  flaky-rate <1% over 50 runs" (L2446-2448); poison-loop bounds (L2449-2456); "**1000
  connect/disconnect cycles** produce zero leaked goroutines" (L2457-2458); re-`basic.consume` +
  `consumer_resubscribed_total` (L2459-2462); ctx-cancel on channel close (L2463-2465); broker-nack
  → `ErrPublishNacked` (L2466-2469). **Throughput** — `BenchmarkPublishConfirmed` **≥ 30k** single
  / **≥ 100k** with `WithPublisherConnections(4)+WithChannelPoolSize(16)` / `PublishBatch` ≥ 5×
  (L2471-2479).
- **SPEC §10** (R-decisions): **R10-8** reconnect barrier max-duration cap (L3098-3104, sizes the
  measurement window); **R10-10** DLQ durability/bounds → **T65** (L3112-3119, the DLQ-fill of the
  poison-storm composite); **R10-11** `WithAddrs` shuffle + reconnect rotation → **T66**
  (L3120-3122, the reconnect-storm/thundering-herd); **R10-16** observability gaps → **T71**
  (L3137-3141, the saturation instrumentation the campaigns need); **R10-18** sync-confirm
  throughput ceiling → **LATER-34** (L3150-3157, the slow-broker→pool-saturation cascade the
  composite campaign provokes).
- **The lens prompt** (`spec-validation/13-load-testing.md`): the seven workload shapes, the
  harness-fidelity demand, the four required outputs (coverage matrix / harness-adequacy / findings
  table / minimum campaign set).

### Cross-lens reconciliation (do **not** re-file — project rule)

This lens is **heavily cross-lens on instrumentation and the existing test spine, and net-new on the
campaigns themselves.** Five prior tasks own the pieces the campaigns build on; they are **extended
in place** (a `Lens-13 (LT-xx)` acceptance bullet appended to the owning task) or **confirmed**
(do-not-regress), never re-filed:

- **T44b** (Throughput benchmark suite; plan.md:815) — **owns** the benchmark methodology, and
  Lens-09 PC-04 already mandates the bench "state both a classic and a quorum number" (so **LT-11
  quorum-vs-classic is already satisfied** — confirm, do not re-file). Lens-13 **extends** it with
  the one-shot-benchmark-vs-sustained-campaign distinction (LT-10): the sustained campaign is T167.
- **T45** (Reconnect chaos test, 5-min @ 10k; plan.md:816) — **owns** the chaos campaign + the
  TV-09 loss-counting method (`published − consumed deduped by MessageID`, tolerating duplicates).
  Lens-13 **extends** it with the peak-multiple intensity framing (LT-09): 10k is average, not peak.
- **T66** (`WithAddrs` shuffle + reconnect rotation, R10-11; plan.md:1002) — **owns** the
  reconnect-storm rotation, and **already asserts** "no `addr[0]` stampede on a full-cluster
  restart". Lens-13 **extends** it (LT-01/05): that assertion is unrunnable single-node → it binds
  to the T166 multi-node harness; the reconnect-storm is also a spike-on-recovery (T168).
- **T71** (Observability gaps: pool-wait, `consumer_in_flight`, `consumer_redelivered_total`,
  R10-16; plan.md:1002) — **owns** the saturation metrics. Lens-13 **extends** it (LT-07/08): these
  metrics must land **before** the spike/stress/soak campaigns can observe the saturation they
  provoke; and the default 5000 ms histogram top bucket clips the load tail.
- **T151** (CI verification infrastructure: scheduled/nightly workflow + 3.13/4.x matrix +
  broker-required guard; plan.md:1721) — **owns** the nightly runner. Lens-13 **extends** it
  (LT-02/03/04/05): the soak (T167) + spike/stress/composite (T168) campaigns ride its cadence; the
  multi-node harness (T166) is a distinct lane it wires; state honestly which campaigns gate
  **release tags** vs nightly.

**Out of lane (defer, confirm — do not re-derive):** the throughput *model* and the per-message
hot-path allocation cost belong to **Lens-09** (T44b/T147-T149/T83/T116/PC-04); the per-criterion
*falsifiability* and the CI-runner *plumbing* belong to **Lens-10** (T150-T152/T151). Lens-13 cites
them as the substrate its campaigns run on and **confirms** them; it does not re-open them.

---

## 3. Constraints (carried into every Phase-24 task)

- **Gate-first.** The first Phase-24 task (T164) records SPEC §10 **Rev 23** and runs the LG-1..LG-5
  audits **before** any §-wording change. Mirrors T84/T94/T101/T111/T119/T130/T143/T147/T150/T153/
  T157.
- **Amend SPEC first, per task, during execution.** This `/spec` step (the brief) and the `/plan`
  step (materialisation) touch **no `SPEC.md`/code/example**. SPEC §7/§9/§6.9 amendments land
  per-task in the same PR ("amend SPEC first").
- **No new *required* per-PR build-tag lane.** The campaigns are **on-demand / nightly / release-tag**
  by nature — they cannot gate per-PR. The soak/spike/stress/composite campaigns ride **T151's
  scheduled workflow** (or an on-demand `make` target); the multi-node harness (T166) is a distinct
  on-demand/nightly lane. The existing `integration`/`chaos`/`examples-smoke` lanes are unchanged.
- **Stay in lane.** No throughput-model re-derivation (Lens-09); no falsifiability re-audit
  (Lens-10). Where a campaign needs a model number or a CI hook, it **cites** the owning task.
- **Topology honesty is mandatory; the heavy harness is phased.** The cheap half of the headline
  (the §9 topology-honesty pass, T165) is **P1, in-phase, non-deferrable** — the spec must stop
  over-claiming today. The expensive half (the multi-node harness + campaign set) is phased; the
  *standing* environment is LATER-49.
- **Cross-lens checkbox discipline.** The five extended tasks (T44b/T45/T66/T71/T151) keep their
  unchecked `[ ]` headers; a `Lens-13 (LT-xx)` bullet is **added** under `- **Acceptance:**` (todo.md)
  and **appended inline** to the `- **Txx**` bullet (plan.md). Headers are **not** flipped.

---

## 4. Pre-work: load gates (LG-1..LG-5 — sequence FIRST; they gate wording)

Capture ground truth from the **spec text + the existing lanes** before any §-wording change. **No
behaviour change.** Results recorded under `spec-validation/`; T164 records §10 **Rev 23**.

- **LG-1 — Workload-shape coverage audit.** Enumerate every load/stress/bench/chaos artifact the
  spec defines (§7 chaos L2241-2247, poison-loop L2249-2254, 100x/1000-cycle stress L2264-2269/§9
  L2457; §9 reliability L2445-2469 + throughput L2471-2479) and classify each against the **seven
  workload shapes** {baseline, sustained, peak, soak, spike, stress-to-failure, composite}, marking
  *covered / partial / absent* with intensity, topology, measurement, assertion. Fills §11.
  → gates **LT-02/03/04/05/09/10**.
- **LG-2 — Harness-fidelity audit.** Confirm in §7 (L2229-2230) + the `amqptest`/`integration`/`chaos`
  lanes that the harness is **single-node testcontainers** — **no** multi-node cluster, **no**
  partition injection, **no** mixed-version cluster. Map each §9 reliability criterion to
  `provable-single-node | needs-multi-node | needs-partition-injection | needs-mixed-version`. Record
  the **T66 "full-cluster restart" assertion is unrunnable on the current harness** as the anchor
  fact. Fills §12. → gates **LT-01** + the failover/upgrade half of **LT-05**.
- **LG-3 — Workload-realism audit.** Confirm whether the spec specifies the generators' **payload-size
  distribution** (incl. near `MaxMessageSizeBytes` / multi-frame), **handler-latency distribution**,
  **routing-key/queue cardinality**, and **arrival burstiness**, or whether they default to
  uniform-small-no-op. → gates **LT-06**.
- **LG-4 — Measurement-under-load audit.** Confirm the default histogram tops at **5000 ms** (§6.9
  L2073-2076) vs `ConfirmTimeout` 30s + the R10-8 barrier cap (L3098-3104), so the load tail clips to
  `+Inf`; confirm the leading-indicator saturation metrics (pool-wait, `consumer_in_flight`,
  `consumer_redelivered_total`) are **not-yet-present** (deferred to R10-16/T71). → gates
  **LT-07/08**.
- **LG-5 — Load-environment & cadence reality.** Confirm §7 names benchmarks "On-demand and on
  release tag" (L2222) and stress "Nightly" (L2268) with **no defined multi-node load environment**
  (dedicated cluster + load-generator fleet) and **no release-gating cadence** tying the heavy
  campaigns to a release tag. → anchors **LT-12** + the owner decision (D5).

---

## 5. Workstreams (WS-1..WS-5)

### WS-1 — Topology honesty *(the cheap, mandatory half of the headline — make the spec stop over-claiming)*
- **LT-01 (honesty half)** §9/§7: label each reliability criterion with the topology it is **actually
  proven against**; add the §7 workload-shape coverage matrix; stop implying a clustered guarantee
  validated single-node. **LT-09**: state the chaos/sustained intensity as a **peak multiple** of the
  average bar, not an arbitrary 10k. **LT-12 (cadence half)**: state the heavy campaigns gate
  **release tags**, not per-PR. → **T165** (extends **T45** for LT-09 intensity, **T44b** for LT-10
  pointer). **(D1, D5)**

### WS-2 — Cluster-fidelity harness *(the expensive half of the headline)*
- **LT-01 (harness half)** + the **failover/upgrade** half of **LT-05**: a **3-node quorum cluster
  harness** (multi-node compose; partition injection via `pause_minority`/`autoheal`; mixed-version
  3.13/4.x members) distinct from the single-node lanes; specify the cluster-dependent campaigns —
  quorum leader failover under sustained load, partition-under-load, reconnect-storm across nodes
  (R10-11/T66's full-cluster-restart assertion gets a harness), quorum confirm latency under Raft
  replication. → **T166** (extends **T66**, **T151**). The *standing* environment → **LATER-49**.
  **(D1, D4)**

### WS-3 — Endurance & sustained load *(slow-growth defects the fast-cycle stress misses)*
- **LT-02** soak/endurance (≥24h at target load) with RSS/goroutine/fd/series trend assertions over
  the named leak surfaces (confirm-tracker map, dedupe cache, `(channel-instance-id, MessageID)`
  counter map, fd/channel, Prometheus series cardinality). **LT-10** sustained-throughput campaign
  (sustain the §9 target ≥1h without latency creep / pool saturation / memory growth), distinct from
  the one-shot benchmark. → **T167** (extends **T44b**, **T151**; dep **T71** for instrumentation).
  **(D2)**

### WS-4 — Overload demonstration *(turn §1's assertion into a demonstration)*
- **LT-03** spike (1×→N× in seconds; assert graceful backpressure — classifiable errors, bounded
  memory, no silent stall/OOM). **LT-04** stress-to-failure (drive to saturation, locate the cliff,
  assert every overload path surfaces a classifiable error). **LT-05 (single-node half)** composite
  incidents — poison-storm (10k + X% poison → two-counter+DLX holds + DLQ bound **T65** + redelivery
  observable **T71**), slow-consumer+fast-publisher flow control, slow-broker confirm-latency→pool-
  saturation cascade (R10-18/LATER-34). → **T168** (extends **T71**, **T151**; confirms **T65**;
  the failover/upgrade composites live in T166). **(D3, D4)**

### WS-5 — Realism & measurement *(make the workload real and the tail visible)*
- **LT-06** realistic workload generators (configurable payload-size + handler-latency distributions,
  routing-key cardinality, burstiness) shared across bench/chaos/soak/spike, with the §9/§7 headline
  numbers stating the distribution used. **LT-07** measurement-under-load: the load reports must
  override `WithLatencyBuckets` to cover the 30s `ConfirmTimeout` + the R10-8 barrier (the default
  5000 ms top bucket clips the tail) and capture **p99/p999 + saturation indicators**, not mean
  throughput. → **T169** (extends **T71** for LT-08 coordination). **(D6)**

---

## 6. Do-not-regress (confirmations — protect, do not re-file)

- **Chaos loss-counting method (T45 / Lens-10 TV-09).** Loss = published-set − consumed-set deduped
  by `MessageID`, tolerating at-least-once duplicates, with the VG-6 injected-drop self-test. The
  soak (T167) + composite (T168) campaigns **reuse** this method; they must not invent a weaker one.
- **Benchmark queue-type discipline (T44b / Lens-09 PC-04).** The bench already states a **classic
  and a quorum** number with pinned payload size — so **LT-11 (quorum-vs-classic under load) is
  already satisfied** for the benchmark; the new campaigns inherit the same queue-type-stated
  discipline.
- **Reconnect-barrier max-duration cap (T63 / R10-8).** The cap sizes the measurement window LT-07
  must cover; the histogram override (T169) is bounded by it.
- **R10-3 ordering load+repeat (T59 / Lens-10 TV-02).** The realistic generator (T169) must not
  regress T59's many-iteration concurrent-unroutable-mandatory load contract.
- **DLQ bound under poison flood (T65 / Lens-05 SRE-03 + Lens-07 ST-08).** The poison-storm composite
  (T168) **uses** the T65 default bound as the thing-under-test — it confirms the bound holds under a
  sustained adversarial flood; it does not re-file the bound mechanism.

---

## 7. Routing matrix (every LT-01..LT-12 accounted for exactly once)

| Finding | Disposition | Owning task(s) |
|---|---|---|
| **LT-01** single-node harness can't validate cluster claims (headline) | §9 honesty (cheap) + multi-node harness (expensive) | **T165** + **T166** (+ extend **T66**) |
| **LT-02** no soak/endurance campaign | net-new soak campaign | **T167** (+ extend **T151**) |
| **LT-03** no spike/elasticity campaign | net-new spike campaign | **T168** (+ extend **T151**) |
| **LT-04** no stress-to-failure/cliff campaign | net-new stress campaign | **T168** |
| **LT-05** no composite-incident campaigns | single-node composites + failover/upgrade composites | **T168** (single-node) + **T166** (cluster) (+ confirm **T65**, **T71**) |
| **LT-06** unrealistic (uniform-small-no-op) workload | net-new realistic generators | **T169** |
| **LT-07** histogram top bucket 5000 ms clips the load tail | measurement-under-load fix | **T169** (+ coordinate **T71**) |
| **LT-08** saturation metrics deferred → can't instrument the campaign | extend the metric owner; sequence-before campaigns | **extend T71** (+ dep of **T167/T168**) |
| **LT-09** chaos 10k = billions/day **at average**, not peak | intensity-honesty / peak-multiple | **extend T45** + **T165** |
| **LT-10** 30k/100k are one-shot bench iterations, not sustained | sustained campaign | **T167** (+ extend **T44b**) |
| **LT-11** quorum-vs-classic under load | **already owned** by T44b/PC-04 → confirm | **T44b** (confirm, no new work) |
| **LT-12** no defined load environment / release-gating cadence | cadence honesty in-phase + standing env deferred | **T165** (cadence) + **LATER-49** (env) |

**Net-new:** T164 (gates, Rev 23), T165 (§9 topology honesty), T166 (multi-node harness + cluster
campaigns), T167 (soak + sustained), T168 (spike + stress + single-node composites), T169
(realistic generators + measurement). **Extended in place:** T44b, T45, T66, T71, T151.
**Confirmed (do-not-regress):** T45/TV-09, T44b/PC-04, T63/R10-8, T59/R10-3, T65 DLQ-bound.
**Deferred:** LATER-49 (standing multi-node load environment + load-gen fleet).

---

## 8. Suggested phasing (planner may revise — "Phase 24")

**Phase 24 — Load & Reliability-Testing Re-review (Lens 13).** Reconciled against the repo this pass:
highest existing task = **T163** (Phase 23, committed) → new IDs **T164–T169**; current tally =
**172 tasks / 23 phases** (todo.md Quick stats) → **178 / 24**; highest filed `LATER` = **LATER-48**
→ **LATER-49** (the LATER-44 gap stays untouched — it is Phase-18's conditional reservation); highest
*written* SPEC §10 Rev heading = **Rev 10**, remediation Revs land per-phase → **Rev 23** recorded
when T164 lands (Rev = Phase − 1, mirrors T157→Rev 22). **No new *required* per-PR build-tag lane**
(the campaigns are on-demand/nightly/release-tag; the multi-node harness is a distinct on-demand
lane).

- **T164 — Load gates LG-1..LG-5** (workload-shape coverage matrix + harness-fidelity map +
  realism audit + measurement-under-load audit + load-env/cadence reality; **no behaviour change**).
  Records §10 **Rev 23**; fills §11 + §12 from measured spec/lane reality. **[P0] · S**
- **T165 — §9/§7 reliability-topology honesty pass + §7 workload-shape coverage matrix** (SPEC
  wording; the cheap, mandatory half of the headline). Label each §9 reliability criterion with the
  topology it is proven against (`single-node | multi-node-quorum | needs-partition-injection |
  needs-mixed-version`); add the §7 coverage matrix; state the chaos/sustained intensity as a peak
  multiple of the average bar (LT-09); state the heavy campaigns gate **release tags**, not per-PR
  (LT-12 cadence). dep LG-1/LG-2. **[P1] · S** **(D1, D5)**
- **T166 — Multi-node cluster load harness + cluster-dependent campaign specs** (the headline's
  expensive half). `amqptest` + a new on-demand/nightly multi-node lane + SPEC §7: a 3-node quorum
  cluster harness (multi-node compose, partition injection via `pause_minority`/`autoheal`,
  mixed-version 3.13/4.x members); specify quorum leader-failover-under-load, partition-under-load,
  reconnect-storm-across-nodes (R10-11/T66's full-cluster-restart assertion finally has a harness),
  and quorum-confirm-latency-under-Raft-replication campaigns. The standing environment is LATER-49.
  dep LG-2. **[P2] · L** **(D1, D4)**
- **T167 — Soak/endurance + sustained-throughput campaigns.** SPEC §7/§9 + `amqptest` + T151's
  scheduled workflow: a soak campaign (≥24h at target load) with RSS/goroutine/fd/Prometheus-series
  trend assertions over the named leak surfaces; a sustained-throughput campaign (sustain the §9
  target ≥1h without latency creep / pool saturation / memory growth) distinct from the one-shot
  benchmark. dep LG-1, T71. **[P2] · M** **(D2)**
- **T168 — Spike, stress-to-failure & composite-incident campaigns** (demonstrate §1 no-silent-
  backpressure). SPEC §7/§9: a spike campaign (1×→N× in seconds; assert graceful backpressure), a
  stress-to-failure campaign (drive to the cliff; assert every overload path is a classifiable
  error), and the single-node composite campaigns (poison-storm 10k + X% poison → two-counter+DLX +
  DLQ bound T65 + redelivery observable T71; slow-consumer+fast-publisher flow control; slow-broker
  confirm-latency→pool-saturation cascade R10-18). The failover/upgrade composites live in T166.
  dep LG-1, T71. **[P2] · M** **(D3, D4)**
- **T169 — Realistic workload generators + measurement-under-load fixes.** `amqptest` + SPEC
  §7/§6.9: configurable generators (payload-size distribution incl. near `MaxMessageSizeBytes`/
  multi-frame, handler-latency distribution, routing-key cardinality, burstiness) shared across
  bench/chaos/soak/spike, with §9/§7 headline numbers stating the distribution; the load reports
  override `WithLatencyBuckets` to cover the 30s `ConfirmTimeout` + R10-8 barrier (the default 5000
  ms top bucket clips the tail) and capture p99/p999 + saturation indicators, not mean throughput.
  dep LG-3, LG-4. **[P2] · M** **(D6)**

**Cross-lens extensions (acceptance bullets, not new tasks):** T44b (LT-10/11), T45 (LT-09), T66
(LT-01/05), T71 (LT-07/08), T151 (LT-02/03/04/05). **Confirmations:** T45/TV-09, T44b/PC-04,
T63/R10-8, T59/R10-3, T65.

**Sequencing (A–E):**
- **A** Gates — T164 (records Rev 23; fills §11 + §12 from measured reality).
- **B** Honesty — T165 (the spec stops over-claiming today: per-criterion topology label + the §7
  coverage matrix + peak-multiple intensity + release-tag cadence). Cheap, mandatory, P1.
- **C** Realism & measurement — T169 (generators + bucket override) so every later campaign measures
  a realistic workload with a visible tail; the **T71** saturation metrics land here (LT-08
  precondition).
- **D** Campaigns — T167 (soak + sustained) + T168 (spike + stress + single-node composites), riding
  T151's cadence, depending on T71.
- **E** Cluster fidelity — T166 (multi-node harness + cluster campaigns), lands last (heaviest);
  unlocks the T66 full-cluster-restart assertion and the failover/upgrade composites; the standing
  environment defers to LATER-49.

Gate deps: LG-1→LT-02/03/04/05/09/10 (T165/T167/T168); LG-2→LT-01/05-cluster (T165/T166);
LG-3→LT-06 (T169); LG-4→LT-07/08 (T169/T71); LG-5→LT-12 (T165 + LATER-49). Gate-independent
(pure honesty/doc): the LT-01 honesty half, LT-09, LT-12-cadence (all T165).

---

## 9. Acceptance criteria for the whole effort

- [ ] **Gates first (LG gates):** T164 lands before any §7/§9/§6.9 wording change; §11 + §12 filled
      from measured spec/lane reality; §10 **Rev 23** recorded.
- [ ] **Topology honesty (LT-01/09/12):** every §9 reliability criterion carries the topology it is
      proven against; §7 has the workload-shape coverage matrix; the spec no longer implies a
      clustered guarantee validated single-node; the chaos/sustained intensity is stated as a peak
      multiple; the heavy campaigns are stated to gate release tags, not per-PR.
- [ ] **Cluster harness (LT-01/05):** a 3-node quorum harness with partition injection and a
      mixed-version axis exists; the quorum-failover / partition / reconnect-storm / quorum-confirm-
      latency campaigns are specified; T66's full-cluster-restart assertion runs on it; the standing
      environment is recorded as LATER-49.
- [ ] **Soak + sustained (LT-02/10):** a ≥24h soak campaign asserts RSS/goroutine/fd/series trends
      over the named leak surfaces; a ≥1h sustained-throughput campaign is distinct from the one-shot
      benchmark.
- [ ] **Overload demonstrated (LT-03/04/05):** spike + stress-to-failure campaigns demonstrate (not
      assert) §1's no-silent-backpressure bar — every overload path surfaces a classifiable error;
      the poison-storm + slow-broker + slow-consumer composites run and the T65 bound + T71
      redelivery signal hold under sustained load.
- [ ] **Realism + measurement (LT-06/07/08):** the generators model payload-size/handler-latency
      distributions + routing-key cardinality + burstiness, and the headline numbers state the
      distribution; the load reports override `WithLatencyBuckets` to cover the 30s window and report
      p99/p999 + saturation; T71's saturation metrics land before the spike/stress/soak campaigns
      run.
- [ ] **Quorum-vs-classic (LT-11):** confirmed already satisfied by T44b/PC-04; the campaigns inherit
      the queue-type-stated discipline (no regression).
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new campaign/harness
      lanes green on their stated (on-demand/nightly/release) cadence.
- [ ] **Ledger integrity:** five tasks extended in place (T44b/T45/T66/T71/T151), confirmations add
      no task, exactly one new `LATER.md` entry (LATER-49), T164–T169 contiguous with no duplicate
      IDs; README examples table updated only if T166/T167 add a runnable `examples/` artifact.

---

## 10. Owner decisions (2026-05-29, recommended dispositions — overridable before execution)

- **D1 — The headline split: honesty now, harness phased (LT-01).** *Recommend:* land the **§9
  topology-honesty pass in-phase (T165, P1)** — the spec must stop implying a clustered guarantee it
  validates single-node, today — **and** build the **multi-node cluster harness (T166)** as the
  net-new campaign spine, with the *standing* environment deferred (D5/LATER-49). *Alternative:* keep
  single-node only and **down-scope §9's reliability claims** to "single-node-validated" (honest, but
  concedes the billions/day-clustered bar is unproven for v0.1). *Recommend: honesty in-phase +
  phased harness — the bar is too central to leave silently single-node-validated.*
- **D2 — Soak campaign cadence (LT-02/10).** *Recommend:* spec a **≥24h soak** (RSS/goroutine/fd/
  series trends) + a **≥1h sustained-throughput** campaign, both on T151's **on-demand/nightly**
  cadence (not per-PR). *Alternative:* rely on the 1000-cycle fast-churn check (insufficient — catches
  fast-cycle, not slow-growth). *Recommend: add the soak spec; the runner may be on-demand if nightly
  is too costly.*
- **D3 — Spike + stress demonstrate §1 (LT-03/04).** *Recommend:* spec a **spike** (1×→N×, assert
  graceful backpressure) and a **stress-to-failure** (locate the cliff, assert classifiable errors)
  campaign — these *demonstrate* the §1 no-silent-backpressure bar rather than asserting it; both
  depend on T71's saturation metrics landing first. *Alternative:* leave §1 asserted-only (the lens
  flags this as the gap). *Recommend: both.*
- **D4 — Composite-incident set (LT-05).** *Recommend:* the **poison-storm** (10k + X% poison) +
  **slow-broker** + **slow-consumer** composites in-phase (single-node-runnable, T168); the
  **repeated-failover-under-load** + **rolling 3.13→4.x broker-upgrade-under-load** composites in
  T166 (need the multi-node harness). *Alternative:* single-variable tests only (real incidents are
  composite — flagged). *Recommend: the full set, split by harness need.*
- **D5 — Standing load environment & release gating (LT-12).** *Recommend:* spec the campaigns now +
  land the single-node-runnable ones in-phase, **defer the standing dedicated multi-node cluster +
  load-generator fleet to LATER-49** (an ops/infra investment — cost, hosting, ops), and state the
  release-gating cadence honestly (the heavy campaigns gate **release tags**, not per-PR).
  *Alternative:* stand up the env now (cost) or never (criteria stay aspirational). *Recommend: spec
  now, defer the standing env, gate releases.*
- **D6 — Realistic generators (LT-06).** *Recommend:* a **shared realistic generator in `amqptest`**
  (configurable payload-size + handler-latency distributions, routing-key cardinality, burstiness),
  reused across bench/chaos/soak/spike, with the §9/§7 headline numbers stating the distribution.
  *Alternative:* keep uniform-small (microbenchmark — flagged). *Recommend: the shared generator.*

---

## 11. Workload-shape coverage matrix (lens output #1)

Each shape rated against the spec **as written** (seed for LG-1; the planner fills measured detail in
T164). Intensity / Topology / Measurement / Assertion — a campaign missing any of the four is
**incomplete**.

| Shape | Run? | Intensity | Topology | Measurement | Assertion | Disposition |
|---|---|---|---|---|---|---|
| **Baseline** | partial | §9 bench iteration | single-node | mean throughput | ≥30k/100k/5× | confirm (Lens-09 T44b) |
| **Sustained** | **absent** | — | — | — | — | **T167** (extend T44b) |
| **Peak** | **absent** | chaos is 10k = *average* | single-node | mean | none-at-peak | **T165** intensity + **T45** |
| **Soak / endurance** | **absent** | — | — | — | — | **T167** |
| **Spike / elasticity** | **absent** | — | — | — | — | **T168** |
| **Stress-to-failure** | **absent** | — | — | — | — | **T168** |
| **Composite-incident** | **absent** | — | — | — | — | **T168** (single-node) + **T166** (cluster) |
| *(fast-cycle reconnect stress — adjacent)* | covered | 1000 cycles | single-node | goroutine count | zero leak | confirm (T45/CR-10) |

Only **baseline** (partial) and the **fast-cycle reconnect stress** are covered. Four of the seven
named shapes are **absent**; **peak** is mislabelled (10k is average); **composite** is the canonical
incident shape and is wholly missing.

---

## 12. Harness-adequacy assessment (lens output #2)

Harness today (§7 L2229-2230): **testcontainers-go + a single RabbitMQ node** (`integration` /
`chaos` / `examples-smoke` lanes). Each §9 reliability claim mapped to what its proof actually needs:

| §9 reliability claim (L#) | Needs | Provable on current harness? |
|---|---|---|
| Chaos zero-loss 5-min @ 10k (L2446-2448) | single-node ok for loss-counting | **partial** — proves loss-accounting, but not *at peak* and not *under failover* |
| Poison-loop bounds, classic + quorum (L2449-2456) | real 4.x quorum (T151 matrix) | provable (matrix) — **but not under load** |
| 1000-cycle zero goroutine leak (L2457-2458) | single-node ok | provable (fast-cycle only; **not soak**) |
| Re-`basic.consume` after reconnect (L2459-2462) | single-node ok | provable single-node; **leader-failover variant needs multi-node** |
| ctx-cancel on channel close (L2463-2465) | single-node ok | provable |
| broker-nack → `ErrPublishNacked` (L2466-2469) | single-node ok | provable |
| §1 "connection loss is invisible" (L98-99) | **multi-node failover** | **needs multi-node** |
| Quorum failover ordering/poison-protection (§1 L59-64) | **3-node quorum under load** | **needs multi-node** |
| Reconnect rotation / no `addr[0]` stampede (T66, R10-11) | **full-cluster restart** | **needs multi-node** (asserted, **unrunnable today**) |
| Partition behaviour (`pause_minority`/`autoheal`) | **partition injection** | **needs partition injection** |
| Quorum confirm latency under Raft at load | **3-node quorum at load** | **needs multi-node** |
| Rolling 3.13→4.x upgrade under load | **mixed-version cluster** | **needs mixed-version** |

**Conclusion:** the single-node testcontainers harness proves the *channel-/connection-level*
reliability mechanics (loss-accounting, ctx-cancel, nack, fast-cycle leaks, the matrix-verified
poison bounds) but **cannot** prove any *cluster-level* claim — failover, partition, reconnect-storm,
quorum-confirm-latency — and the spec **asserts** several of those (§1 L98-99, T66). That over-claim
is the headline; T165 fixes the honesty cheaply, T166 builds the harness.

---

## 13. Findings table (lens output #3)

`ID | Severity | Classification | Location | Defect class uncaught | Recommended campaign/harness change`

| ID | Sev | Class | Location | What goes uncaught | Recommendation |
|---|---|---|---|---|---|
| **LT-01** | High | inadequate-harness | §7 L2229-2230; §9 L2445-2469; §1 L98-99; T66 | quorum failover, partition, reconnect-storm, quorum-confirm-latency — every cluster claim | §9 topology-honesty pass (**T165**) + multi-node cluster harness (**T166**) |
| **LT-02** | High | missing-campaign | §7 L2264-2269; §9 L2457 | slow steady-state growth: confirm-tracker map, dedupe cache, counter map, fd, series cardinality | soak ≥24h with RSS/goroutine/fd/series trends (**T167**) |
| **LT-03** | High | missing-campaign | §1 L48-52; §6.1/§6.3 | backpressure under sudden N× spike (pool block→exhausted, `connection.blocked`) | spike campaign asserting graceful backpressure (**T168**) |
| **LT-04** | High | missing-campaign | §1 L48-52 | the cliff — does overload surface classifiable errors or stall/OOM? | stress-to-failure campaign (**T168**) |
| **LT-05** | High | missing-campaign | §7; R10-10/T65; R10-18 | composite incidents: poison-storm, repeated failover, rolling upgrade, slow consumer/broker | composite campaigns (**T168** single-node + **T166** cluster) |
| **LT-06** | Med | unrealistic-load | §7; §9 L2471-2479 | variable payload/latency, routing-key cardinality, burstiness — uniform-small-no-op = microbench | realistic generators in `amqptest` (**T169**) |
| **LT-07** | Med | unmeasurable-under-load | §6.9 L2073-2076 | the load tail beyond 5000 ms (ConfirmTimeout 30s, R10-8 barrier ~20s+) clips to `+Inf` | load reports override `WithLatencyBuckets`; report p99/p999 (**T169**) |
| **LT-08** | Med | unmeasurable-under-load | §6.9 L2078-2091; R10-16/T71 | saturation invisible during the campaign (no pool-wait / in-flight / redelivered) | T71 saturation metrics land **before** the spike/stress/soak campaigns (**extend T71**) |
| **LT-09** | Med | unrealistic-load | §9 L2446-2448 | the bar at peak — 10k ≈ 1 B/day at *average*, peaks are 3–10× | run chaos/sustained at a stated peak multiple (**extend T45** + **T165**) |
| **LT-10** | Med | missing-campaign | §9 L2471-2479 | latency creep / pool saturation / memory growth over a sustained run (bench is one-shot) | sustained-throughput campaign ≥1h (**T167** + extend **T44b**) |
| **LT-11** | Low | (confirm) | §9 L2471-2479; T44b/PC-04 | — (already owned) | confirm queue-type stated; campaigns inherit the discipline |
| **LT-12** | Med | inadequate-harness | §7 L2222/L2268 | no defined load env / release-gating cadence → criteria aspirational | cadence honesty in-phase (**T165**); standing env deferred (**LATER-49**) |

---

## 14. Minimum load-campaign set the v0.1 bar requires (lens output #4, prioritised)

1. **§9 topology-honesty pass (T165, P1).** The cheapest, highest-leverage fix: stop the spec
   over-claiming a clustered bar it validates single-node. Non-deferrable.
2. **Soak / sustained-throughput (T167, P2).** The slow-growth leak surfaces are the most likely
   silent killer of a billions/day flight path; the 1000-cycle check does not cover them.
3. **Spike + stress-to-failure (T168, P2).** The only way to *demonstrate* §1's no-silent-backpressure
   bar; depends on T71's saturation metrics.
4. **Realistic generators + measurement fix (T169, P2).** Without realistic distributions and a
   visible tail, every campaign above degrades into a microbenchmark with an unreadable p999.
5. **Multi-node cluster harness + cluster campaigns (T166, P2, heaviest).** Proves the failover /
   partition / reconnect-storm / quorum-confirm-latency claims; the standing environment is
   LATER-49.

---

## 15. Open questions for the owner (lens output #5)

- **Q1 — Load environment.** Is there budget for a **standing 3-node cluster + a load-generator
  fleet**, or do the cluster campaigns run **on-demand** against an ephemeral compose cluster? (Drives
  D5 / LATER-49.)
- **Q2 — Release-gating cadence.** Should the soak/spike/stress/composite campaigns **gate release
  tags** (block a `v0.x.0` cut on a green run) or be **advisory** (run + report, do not block)? (Drives
  T165's cadence wording.)
- **Q3 — Soak duration & frequency.** Is **≥24h** the right soak window, and does it run **nightly**
  or **per-release-candidate**? (Drives D2 / T167.)
- **Q4 — Peak multiple.** What peak multiple of the ~11.6k/s average should the chaos + sustained
  campaigns target (e.g. 3×, 5×, 10×)? (Drives D… LT-09 / T165 / T45.)
- **Q5 — Mixed-version upgrade.** Is the **rolling 3.13→4.x broker-upgrade-under-load** campaign in
  scope for v0.1, or a v0.2 item once a 4.x estate is the norm? (Drives D4 / T166.)

---

## 16. Out of scope for this plan

- **This step (the brief) touches no `SPEC.md`/code/example/README and makes no commit.** Per-task
  SPEC §7/§9/§6.9 amendments + the harness/campaign code land per-task during execution ("amend SPEC
  first"); the `/plan` step materialises Phase 24 into `tasks/plan.md` + `tasks/todo.md` + `LATER.md`.
- **The throughput *model* and benchmark *methodology*** (Lens-09: T44b/T147-T149/T83/T116/PC-04) —
  cited and confirmed, not re-derived.
- **Per-criterion *falsifiability* and the CI-runner *plumbing*** (Lens-10: T150-T152/T151) — cited
  and confirmed, not re-audited.
- **The standing multi-node load environment + load-generator fleet** (an ops/infra investment) →
  **LATER-49** (D5); T166 lands the harness *contract* + campaign *specs*, not the standing env.
- **The async/streaming publish API** (R10-18/LATER-34) — the slow-broker composite (T168)
  *provokes* the pool-saturation cascade R10-18 describes; whether to add the async API stays an
  owner decision in LATER-34, not this lens.
- **Inventing a new required per-PR build-tag lane** — the campaigns are on-demand/nightly/release by
  nature; they ride T151's scheduled workflow + on-demand `make` targets.

---

## 17. Verdict for this lens

**GO-WITH-CHANGES — no Blocker.** The v0.1 *code* is not defective; the existing chaos + benchmark +
nightly-runner spine is sound and honest about cadence; and the load-bearing instrumentation and
test mechanics are already owned (T44b/PC-04 queue-type, T45/TV-09 loss-counting, T71/R10-16
saturation metrics, T151 nightly runner). So this is not NO-GO. But it is not a clean GO either: the
spec's load-validation strategy **cannot prove its own billions/day-clustered bar.** Every
cluster-level reliability claim is validated only on a **single-node testcontainers** harness — the
spec even **asserts** "connection loss is invisible" (§1 L98-99) and "no `addr[0]` stampede on a
full-cluster restart" (T66) against a topology that **cannot run them**; four of the seven workload
shapes (soak, spike, stress-to-failure, composite) are **never exercised**, so §1's
no-silent-backpressure bar is **asserted, not demonstrated**; the chaos intensity is one-billion/day
**at average**, never at peak; the generators are uniform-small-no-op microbenchmarks; and the load
tail clips at 5000 ms while the operations that matter take 30s. The **cheap, mandatory** remedy is a
§9 topology-honesty pass (T165) — make the spec stop over-claiming **today**. The **expensive**
remedy — a multi-node cluster harness (T166), the soak/spike/stress/composite campaign set
(T167/T168), and realistic generators (T169) — is real engineering, phased, with the *standing*
multi-node load environment deferred to LATER-49. **12 findings (LT-01..LT-12), 5 gates (LG-1..LG-5),
6 net-new tasks (T164–T169), 5 cross-lens extensions (T44b/T45/T66/T71/T151), LATER-49. Phase 24 →
SPEC §10 Rev 23. Tally 172 → 178 tasks / 23 → 24 phases.**
