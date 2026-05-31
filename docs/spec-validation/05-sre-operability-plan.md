# Plan Input — Remediate SRE / Production-Operability Findings (Lens 05)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the SRE /
> production-operability lens (`spec-validation/05-sre-operability.md`). Like
> Lenses 03 and 04, no findings report pre-existed — this brief was produced by
> *conducting* the review: for every major failure mode (reconnect storm, poison
> storm, DLQ disk-fill, silent channel death, half-open write, pool exhaustion,
> degraded topology, deploy drain, metrics blindness) it answers the three on-call
> questions — **detect** (which signal fires?), **respond** (what knob/action
> exists?), **verify** (how do I know recovery happened?) — and a "no" to any one
> is a finding. It enumerates confirmed defects (`SRE-01..SRE-16`), their evidence,
> the desired outcome, and an acceptance test for each, grouped into workstreams
> and sequenced by dependency.
>
> **Numbering:** new task IDs are **T111–T118** (highest existing is T110, after
> Phase 15). Lens 05 becomes **Phase 16**, mirroring how Lenses 01/02/03/04 became
> Phases 12/13/14/15. Two Phase-11 tasks are **pulled forward** (T67, T72). Seven
> findings are **already owned by prior-lens tasks** and are *not* re-filed (project
> rule: cross-lens shared findings extend the shared task) — they receive a
> `Lens-05 (SRE-xx)` acceptance bullet on the existing task. **No new `LATER.md`
> entry** (the async-publish-API ceiling stays in the existing LATER-34). **No new
> build-tag lane** (the gates ride the existing integration + `chaos` lanes). The
> first Phase-16 task records SPEC §10 **Rev 15**.

---

## 1. Objective

Validate `SPEC.md` not from the API surface but from **the pager**: at 3am, with
this library in a billions/day flight path, *what do I see, what do I do, and how
do I know it worked?* The §1 bar for an SRE is concrete: every failure mode must be
**detectable** before the customer notices, every recovery must be **bounded** (the
self-healing must not itself cause an outage), every capacity cliff must be
**documented honestly** (RTT + hardware stated), and every blast radius must be
**contained** (one bad queue / node / consumer must not take down everything).

Concretely, the plan must:
1. Confirm the **operability cut-line** (the prompt's central demand): which Rev-10
   deferrals are operability *Blockers* that must gate v0.1, not v1.0. The finding
   is that **all five** the lens would demand (R10-6/8/10/11/16 = T61/T63/T65/T66/T71)
   are **already pulled forward** into Phases 12/13 by Lenses 01/02 — so the cut-line
   is satisfied *because of those pulls*. This lens **endorses and hardens** them
   with SRE detect/respond/verify acceptance (cross-lens, not re-filed) and warns
   that reverting any pull would flip this lens to NO-GO.
2. Pull forward the **two remaining unclaimed operability deferrals** (owner
   decision): **T67** (`Dial` partial-pool-connect policy — unpredictable
   restart/rollout behaviour) and **T72** (TCP keepalive / half-open-write — a
   publisher hung up to 30s on a dead socket with no signal).
3. Close the **metrics registration footgun** (SRE-10): `WithMetrics` "defaults to
   Prometheus" but the registerer is unspecified — a hidden-global / double-`Dial`
   duplicate-registration **panic** waiting to happen, and a §8 "no globals"
   violation.
4. Make incident latency **visible** (SRE-11): the default histogram top bucket
   (5000ms) is below the real latency envelope (30s `ConfirmTimeout`, ~20s reconnect
   barrier, cross-region RTT), so the slow/stalled publishes you most need to see
   collapse into `+Inf`. Extend the default buckets (owner decision).
5. Give on-call a **current-state degraded signal** (SRE-12): today there is only a
   transition *counter*, not a gauge for "is this connection degraded *right now*".
6. Kill the **readiness-probe false-green** (SRE-13): `Connection.Health` returns OK
   while a consumer is silently dead. Aggregate consumer-subscription liveness into
   `Health` (owner decision).
7. Tell the **capacity truth** (SRE-14): the §9 30k/100k targets hold only at sub-ms
   (local-broker) RTT; the per-`Publish` ceiling is `pool/RTT`; document it
   prominently in §9/§6.2 (owner decision: doc-only; the async-API stays LATER-34).
8. Bound **always-on label cardinality** (SRE-15) and ship an **operator runbook**
   (SRE-16): every mandatory metric → an action; every §1 "no silent X" bar → a
   metric and an alert query.

This is **remediation of an existing spec**, not a new feature. **Lens verdict:
GO-WITH-CHANGES** — there is no *new* §1 silent-message-loss path introduced here
(the registry footgun is a *loud* crash, not silent loss), and the five highest
operability blockers are already in v0.1 scope and pulled forward. What remains is a
metrics/observability hardening set, two pull-forwards, and a capacity-honesty doc
fix.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing.** Sections referenced:
  - §1 reliability bar L37–85 (the "no silent X" bars L43–84); "success looks like"
    L92–104.
  - §6.1 Connection — options table L487–514; reconnect synchronisation barrier
    L576–594; degraded mode L595–612; blocked-wait routing L618–625; resubscribe
    L626–632; channel-close handler abort L633–639; `Close` cascade L640–659;
    `Health` L660–663; connection naming L667–676; heartbeat/partition detection +
    LB guidance L678–701.
  - §6.3 Consumer — `Prefetch`/`Concurrency` sizing + `Prefetch<Concurrency` build
    warning L1169–1193; ordering + SAC L1203–1225; two-counter poison design
    L1227–1321; `basic.cancel` L1323–1343.
  - §6.9 metrics — default labels L2059–2071; **histogram buckets L2073–2076**;
    mandatory metrics L2078–2091; otel tracing-for-postmortem (`error.type` →
    action) L2093–2168.
  - §9 Success Criteria — throughput targets L2471–2479; operational invariants
    L2512–2548.
  - §10 decisions — Rev 6 operational invariants L2785+; **Rev 10** deferrals L3031
    (highest Rev note): R10-6/T61 L3085–3091, R10-7/T62 L3092–3097, R10-8/T63
    L3098–3104, R10-10/T65 L3112–3119, R10-11/T66 L3120–3122, R10-12/T67 L3123–3126,
    R10-15/T70 L3133–3136, R10-16/T71 L3137–3141, R10-17/T72 L3142–3146,
    R10-18/LATER-34 L3150–3157.
- Implementation state (read this pass):
  - `metrics/prometheus.go` constructors `NewPrometheusClientMetrics` /
    `…PublisherMetrics` / `…ConsumerMetrics` (L27/L96/L168) **already take an
    injected `prometheus.Registerer`** — good — but there is **no connection-level
    default wiring** (`grep` finds no caller of `NewPrometheus*` outside the package
    and its tests) and **no `WithMetricsRegisterer` option** (`options_connection.go:262`
    `WithMetrics` is a setter only). So the SPEC's "defaults to Prometheus" leaves
    the registerer undefined — the SRE-10 gap.
  - `connection.go` carries an internal `degraded bool` / `degradedErr` (L45–47) and
    fires the **counter** `connection_degraded_total{role, reason}` (via
    `metrics.RecordDegraded`, `metrics/metrics.go:37`) on transition, but exposes
    **no current-state gauge** — the SRE-12 gap.
  - `connection.go:220` `Connection.Health` checks socket + degraded only;
    per-component `Consumer.Health` (`consumer.go:816`) / `BatchConsumer.Health`
    (`batch_consumer.go:650`) exist but are **not aggregated** into
    `Connection.Health` — the SRE-13 false-green.
  - Default latency buckets `[0.5 … 5000]` ms are the literal default (§6.9 L2074);
    `WithLatencyBuckets` is the override (L2075–2076).
- `tasks/plan.md` / `tasks/todo.md` — Phase 16 lands here (T111–T118, pull-forward
  T67/T72, cross-lens bullets on T61/T62/T63/T65/T66/T70/T71). Highest existing task
  is **T110**.
- `LATER.md` — highest entry is **LATER-40**; this brief adds **none** (the async
  publish-API ceiling remains **LATER-34**).
- The validation prompt this brief derives from: `spec-validation/05-sre-operability.md`.

### Cross-lens reconciliation (do **not** re-file — project rule)

Seven SRE findings are already owned by prior-lens / Phase-11 tasks; this brief
records them for traceability and routes each to the existing task with an added
`Lens-05 (SRE-xx)` acceptance bullet (detect/respond/verify). **None is re-filed and
none is re-pulled** (they are already pulled forward):

| SRE finding | Already covered by | Phase | What Lens-05 adds |
|-------------|--------------------|-------|-------------------|
| SRE-01 (silent channel death) | **T61** (channel-level recovery, RMQ-09) | 12 (pulled fwd) | resubscribe-on-channel-only-death is observable + its absence is the readiness false-green driver (SRE-13) |
| SRE-02 (unbounded reconnect barrier → silent publisher stall) | **T63** (barrier cap, DS-02/RMQ-17) | 13 (pulled fwd) | the barrier-cap default must be ≤ the new histogram top bucket (SRE-11) so a capped stall is *visible* |
| SRE-03 (unbounded auto-DLQ → cluster-wide disk alarm) | **T65** (DLQ bounds, RMQ-07/EDA-07) | 12 (pulled fwd) | highest blast radius; add a DLQ-depth signal + overflow policy acceptance |
| SRE-04 (reconnect thundering herd) | **T66** (`WithAddrs` shuffle, RMQ-10/DS-10) | 12 (pulled fwd) | chaos-lane assertion: no `addr[0]` stampede on full-cluster restart |
| SRE-05 (missing #1 instability metric + pool saturation) | **T71** (`consumer_redelivered_total` + pool acquire-wait + `consumer_in_flight`, DS-14) | 13 (pulled fwd) | this is the *leading indicator*; without it a poison storm / pool saturation is undetectable before outage |
| SRE-06 (redeclare amplification on recovery) | **T62** (redeclare once per recovery, DS-09) | 13 (pulled fwd) | the recovery action must not hammer the just-recovered broker (N×pool×fleet declares) |
| SRE-07 (undispatched deliveries dropped/dup'd on `Close`) | **T70** (nack-requeue undispatched, DS-03) | 13 (pulled fwd) | every rolling deploy is a low-grade incident; the deploy-time dup rate must be observable (`consumer_shutdown_requeued_total`) |

---

## 3. Constraints & working agreements (planner must honour)

- **Every finding answers detect/respond/verify.** A worry without all three is not
  a finding. The acceptance test exercises the *signal and the recovery*, not just
  the code path.
- **Invisible failure modes and recovery-amplification are the highest severities.**
  A "tracked as Phase 11" operability gap is **open in v0.1** and rated as such.
- **No new build-tag lane.** Unlike Lens 02's `chaos` lane and Lens 03's `interop`
  lane, Lens 05's gates ride the **existing integration lane** (3.13 + 4.x) and the
  **existing `chaos` lane** (T84). Do not introduce a new tag.
- **No globals (§8).** The metrics-registration fix (SRE-10) must inject a
  `prometheus.Registerer`; the *default* must be a private per-`Connection` registry,
  **never** `prometheus.DefaultRegisterer`. A double-`Dial` in one process must not
  panic.
- **Demand honest numbers.** Any throughput claim states RTT + hardware + broker
  config or it is a finding (SRE-14). The §9 targets are explicitly local-broker.
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in the
  same PR (or earlier), then implementation/test follows. The first Phase-16 task
  records §10 **Rev 15**.
- **`testify` `assert`/`require`** in every `_test.go`; **`goleak`** on any
  goroutine-spawning test (project rules).
- **Docs in English** (`CLAUDE.md`). Module path `github.com/brunomvsouza/warren`;
  `errorlint` is on.
- **Choke points:** metrics in `metrics/` + the connection-level default wiring in
  `connection.go`/`options_connection.go`; reconnect/degraded/Health in
  `connection.go`; dialer in `connection.go` (T72). Corrections land at the choke
  point.
- **README sync:** any change to the external contract (`WithMetricsRegisterer`, the
  default-bucket change, the `connection_degraded` gauge, the `Health` consumer-
  liveness semantics, the cardinality opt-out, the honest §9 ceiling) updates
  `README.md`'s observability/reliability guarantees.

---

## 4. Pre-work: verification gates (SG-1..SG-4 — sequence FIRST; they gate wording)

These behaviours must be observed (unit + existing integration/`chaos` lanes) before
the dependent amendments can be worded correctly. The gate task (T111) captures them
into a committed results table each downstream task cites. No SPEC edit to an
affected section until the relevant gate returns.

| Gate | Question (ground truth to capture) | Blocks |
|------|-----------------------------------|--------|
| **SG-1** | With the default Prometheus metrics, does a **second `Dial` in the same process** panic on duplicate registration (or where exactly does the default register today)? Confirm the registerer is currently unspecified and that a private-registry default removes the panic. | SRE-10 / T112 |
| **SG-2** | Does a publish that stalls for the full 30s `ConfirmTimeout` (mock channel withholds the ack) land in the **`+Inf` bucket** of `publisher_publish_seconds` under the default `[0.5…5000]`ms buckets — i.e. is the slow tail invisible today? | SRE-11 / T113 |
| **SG-3** | On the `chaos` lane, force a **channel-only consumer death** (404/`basic.cancel`/ack-error closes the channel, TCP stays up) and confirm `Connection.Health(ctx)` returns **OK (nil)** while the consumer is no longer subscribed — the readiness false-green. | SRE-13 / T115 (interacts with T61) |
| **SG-4** | Under **injected confirm-RTT** (or a remote broker), confirm per-`Publish` throughput tracks `≈ pool/RTT` and that a confirm-latency spike drives `ErrChannelPoolExhausted` — quantify the cliff that §9/§6.2 must document. | SRE-14 / T116 |

**Planner note:** SG-1/SG-3 capture a *current broken behaviour* the fix must close;
SG-2 calibrates the new default buckets; SG-4 quantifies the capacity ceiling the
doc must state. No new lane — SG-1/SG-2 are unit/mock-channel, SG-3/SG-4 ride the
existing `chaos`/integration lanes.

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets. Findings already owned by a prior lens are in WS-0.

### WS-0 — Already covered by prior-lens tasks (record + add a `Lens-05` bullet, do **not** re-file)

- **SRE-01 [Blocker, deferred-gap]** — *silent channel death.* A 404/406/ack-error
  closes only the channel; the TCP connection stays up; the supervisor watches the
  *connection*, so a consumer can die silently on its closed channel and stop
  consuming with **no reconnect, no metric, no log** (R10-6 L3085–3091). **Detect:**
  none today → **gap**. **Respond:** none (no auto channel-reopen). **Verify:** none.
  *Owned by **T61*** (consumer observes its own channel close → reopen + `basic.qos`
  + `basic.consume`, increment `consumer_resubscribed_total`), pulled into Phase 12.
  *Lens-05 adds:* the resubscribe-on-channel-only-death must increment
  `consumer_resubscribed_total` (so a `rate()==0` while expecting traffic is the
  alert), and its absence is the root cause of the SRE-13 readiness false-green.

- **SRE-02 [Blocker, deferred-gap]** — *unbounded reconnect barrier → silent
  publisher stall.* A half-alive broker (accepts the socket, stalls on
  `queue.declare`) + `PublishTimeout=0` + `context.Background()` stalls publishers
  **indefinitely** — the exact silent-stall the 30s `ConfirmTimeout` was meant to
  kill, reintroduced one layer up (R10-8 L3098–3104). **Detect:** none (publisher
  hung, no error, no metric). **Respond:** none. **Verify:** none. *Owned by **T63***
  (bound the barrier; on cap, blocked publishers get `ErrReconnecting`), pulled into
  Phase 13. *Lens-05 adds:* the barrier-cap default must be **≤ the new histogram
  top bucket** (SRE-11) so a capped stall is *visible* in `publisher_publish_seconds`,
  not collapsed into `+Inf`.

- **SRE-03 [Blocker, deferred-gap]** — *unbounded auto-DLQ → cluster-wide disk
  alarm.* The auto-declared `<source>.dlq` has no bounds; a poison storm fills it,
  hits the disk high-watermark, RabbitMQ raises a disk alarm and **blocks every
  publisher on the whole broker** (`connection.blocked`). One service's bug →
  cluster-wide publish outage — the **highest blast radius in the spec** (R10-10
  L3112–3119). **Detect:** `connection_blocked_seconds` fires *after* the alarm (too
  late) → leading signal **gap**. **Respond:** none by default (DLQ unbounded).
  **Verify:** none. *Owned by **T65*** (durable/quorum-capable DLQ with configurable
  bounds; mirror Replier missing-DLX validation on Consumer), pulled into Phase 12.
  *Lens-05 adds:* bounded-DLQ-**by-default** (overflow policy `drop-head`/`reject` is
  a deliberate bound, not unbounded) + a DLQ-depth signal so the storm is visible
  *before* the broker-wide alarm.

- **SRE-04 [High, deferred-gap]** — *reconnect thundering herd.* `WithAddrs` has no
  shuffle/rotation, so on a cluster-wide restart every client reconnects to the first
  reachable node → that node is stampeded and falls over → cascade (R10-11 L3120–3122).
  **Detect:** `connection_reconnects_total` spike (lagging). **Respond:** none by
  default. **Verify:** none. *Owned by **T66*** (shuffle per connection + rotate on
  reconnect), pulled into Phase 12. *Lens-05 adds:* the `chaos` lane asserts **no
  `addr[0]` stampede** on a full-cluster restart (compounds with SRE-06/T62).

- **SRE-05 [High, deferred-gap]** — *the #1 instability metric is missing from v0.1.*
  R10-16 adds `consumer_redelivered_total` ("redelivery ratio is the #1 instability
  signal"), channel-pool acquire-wait/saturation, and a `consumer_in_flight{queue}`
  gauge (L3137–3141) — all **leading indicators**. **Detect:** with only the v0.1
  set, a brewing poison storm or pool saturation is **invisible until it is an
  outage** → **gap**. **Respond:** alert on redelivery ratio / pool-wait p99.
  **Verify:** the metrics increment. *Owned by **T71***, pulled into Phase 13.
  *Lens-05 adds:* this is the single most important on-call metric; the §1 "duplicate
  budget never invisible" claim does **not** hold for the dominant duplicate source
  (redelivery) without it.

- **SRE-06 [Med-High, deferred-gap]** — *topology redeclare amplification.* `AttachTo`
  registers redeclare on *every* pooled connection, so a broker restart triggers
  N×(pool size) `queue.declare`s against a just-recovered (fragile) broker, per
  client, across the fleet — the recovery action hammers the thing it is recovering
  from (R10-7 L3092–3097). **Detect:** `topology_redeclare_seconds` spike. **Respond:**
  redeclare once per recovery event at `*Connection` level. **Verify:** declare count
  per recovery == 1, not N×pool. *Owned by **T62***, pulled into Phase 13. *Lens-05
  adds:* couple the chaos exercise with SRE-04/T66 (a full-cluster restart against a
  just-recovered, possibly Khepri-quorum-forming broker).

- **SRE-07 [High, deferred-gap]** — *deploy-drain loss/duplication.* R10-15 (L3133–3136)
  leaves prefetched-but-undispatched deliveries on `Close` undefined → every deploy
  either drops or duplicates those messages; "every rolling deploy causes a blip of N
  lost/duplicated messages per pod" is a chronic low-grade incident. **Detect:** none
  (silent). **Respond:** nack-requeue the undispatched buffer (at-least-once dup,
  acceptable under §6.2.1). **Verify:** `consumer_shutdown_requeued_total`. *Owned by
  **T70*** (nack-requeue + flush partial batch on `Close`), pulled into Phase 13.
  *Lens-05 adds:* the deploy-time duplicate rate must be **boundable and observable**
  via the new counter.

### WS-1 — Operability pull-forwards *(unclaimed Phase-11 deferrals — owner: pull both)*

- **SRE-08 [High, deferred-gap]** — *`Dial` partial-pool-connect policy undefined.*
  If 1 of the 2+2 pooled connections fails at boot, `Dial`'s behaviour is unspecified
  (R10-12 L3123–3126): fail-fast (a single flaky node blocks **every deploy**) or
  succeed-degraded (silent **capacity loss**) — operators cannot predict restart /
  rollout behaviour. **Detect:** none (no signal for "booted at reduced capacity").
  **Respond:** the chosen policy + a degraded-capacity boot signal. **Verify:** boot
  with 1/4 connections down asserts the chosen policy and emits the signal. *Location:*
  §6.1 `Dial` L472, options L501–502; R10-12 L3123–3126. *Outcome (owner decision):*
  **pull T67 forward** — define **succeed-if-≥1-per-role** with supervised reconnect
  of the rest *and* a metric/log for booting at reduced capacity (so "silent capacity
  loss" becomes observed). *Acceptance:* an integration test boots a 2+2 pool with one
  consumer connection unreachable, asserts `Dial` succeeds, the missing socket
  reconnects under supervision, and a degraded-capacity signal fired. *Disposition:*
  **pull forward T67** (Lens-05 bullet records SRE-08).

- **SRE-09 [High, deferred-gap]** — *half-open write / no TCP keepalive.* AMQP
  heartbeats cover **read-side** partition detection (~20s); a **write** to a half-open
  socket can block far longer with no `net.Dialer` keepalive / `TCP_USER_TIMEOUT`
  (R10-17 L3142–3146). v0.1 relies on `ConfirmTimeout` as the *only* backstop → a
  publisher can hang for the full 30s on a dead socket. **Detect:** the publish lands
  in the 30s/`+Inf` tail (only after SRE-11 widens buckets). **Respond:** dialer
  keepalive so the write fails promptly. **Verify:** a half-open-socket test fails the
  write well under 30s. *Location:* §6.1 heartbeat/partition L678–701, `WithDialer`
  L498; R10-17 L3142–3146. *Outcome (owner decision):* **pull T72 forward** — set
  `net.Dialer` keepalive defaults and document `TCP_USER_TIMEOUT` where available.
  *Acceptance:* a half-open-socket integration/`chaos` test asserts a publish on a
  dead socket errors promptly (keepalive-bounded), not at 30s. *Disposition:* **pull
  forward T72** (Lens-05 bullet records SRE-09).

### WS-2 — Metrics correctness & blindness *(new — the observability core)*

- **SRE-10 [High, underspecified/latent-defect]** — *Prometheus registry injection
  footgun.* `WithMetrics` "defaults to Prometheus" (§6.1 L511) but the registerer is
  **unspecified**: the constructors accept an injected `prometheus.Registerer`
  (`metrics/prometheus.go:27/96/168`) yet there is **no connection-level default
  wiring** and **no `WithMetricsRegisterer` option**. If the default ever uses
  `prometheus.DefaultRegisterer`, then (a) it is a **hidden global side effect** §8
  "no globals" forbids, and (b) a **double-`Dial` in one process panics** on
  duplicate registration — an operational footgun discovered only in production.
  **Detect:** panic at the second `Dial` (loud, but a crash). **Respond:** inject a
  registerer. **Verify:** double-`Dial` does not panic; metrics land in the expected
  registry. *Location:* §6.9 metrics L2056–2091; §6.1 options L511; §8 "no globals".
  *Outcome (owner decision recorded — §8 forces it):* add
  `WithMetricsRegisterer(prometheus.Registerer)`; the **default is a private
  per-`Connection` registry**, never `DefaultRegisterer`; document the injection in
  §6.9. *Acceptance (SG-1):* a unit test calls `Dial` twice in one process with
  default metrics and asserts **no panic**; an injected registerer test asserts the
  metrics register into the provided registry. *Disposition:* **new task T112**.

- **SRE-11 [Med-High, design-disagreement]** — *histogram buckets below the latency
  envelope.* Default buckets `[0.5 … 5000]`ms (§6.9 L2073–2076), but the publish path
  can block for the **reconnect barrier (~20s)** and `ConfirmTimeout=30s`; a 30s
  confirm timeout lands far outside the 5000ms top bucket → **all slow/stalled
  publishes collapse into `+Inf`**, hiding exactly the latency needed during an
  incident (and cross-region RTT alone can exceed the top bucket). **Detect:** the
  histogram — but it is blind above 5s today → **gap**. **Respond:**
  `WithLatencyBuckets`, but the *default* is wrong. **Verify:** a 30s stall lands in a
  finite bucket. *Location:* §6.9 L2073–2076. *Outcome (owner decision):* **extend the
  default buckets** to cover the real envelope (add `10s, 30s, 60s` so the
  barrier-cap (SRE-02/T63) and `ConfirmTimeout` are visible); keep `WithLatencyBuckets`
  for tuning; §6.9 explains the envelope rationale. *Acceptance (SG-2):* a mock-channel
  unit test withholds the ack for 30s and asserts the observation lands in a **finite**
  bucket, not `+Inf`. *Disposition:* **new task T113**.

- **SRE-15 [Med, underspecified]** — *always-on label cardinality.* `routing_key` /
  `message_type` are opt-in (good, §6.9 L2067–2071), but the always-on `queue` /
  `exchange` labels can themselves be a **Prometheus-killer** at billions/day across
  thousands of queues/exchanges; there is no guidance on bounding them. **Detect:**
  Prometheus scrape-timeout / OOM (a monitoring outage during an incident). **Respond:**
  a way to drop/aggregate `queue`/`exchange` to a bounded label. **Verify:** the
  cardinality budget is documented and the opt-out works. *Location:* §6.9 L2059–2071.
  *Outcome:* §6.9 documents the cardinality budget (rough series-count math per
  queue/exchange) **and** adds an opt-out to omit/aggregate `queue`/`exchange` (reuse
  the `WithMetricsLabels` mechanism inverted, e.g. a "bounded labels" mode) for
  very-high-fan-out deployments. *Acceptance:* a unit test asserts the opt-out drops
  the `queue`/`exchange` label; §6.9 states the budget. *Disposition:* **new task
  T117**.

### WS-3 — Current-state operability signals *(new)*

- **SRE-12 [Med, underspecified]** — *no current-degraded-state gauge.* Degraded mode
  fires the **counter** `connection_degraded_total{role, reason}` (§6.1 L600;
  `connection.go` internal `degraded bool` L45–47) — a *transition* count. On-call
  needs **"is this connection degraded *right now*"** for alerting/dashboards, not
  "how many times did it transition." **Detect:** no current-state series → **gap**.
  **Respond:** `ForceReconnect()` exists (good). **Verify:** the gauge clears on
  recovery. *Location:* §6.1 degraded mode L595–612; §6.9 mandatory metrics L2078–2091.
  *Outcome:* add a `connection_degraded{role}` **gauge** (0/1) set to 1 on entering
  degraded state and 0 on the first successful redeclare; list it in §6.9 mandatory
  metrics. *Acceptance:* a unit/`chaos` test drives a connection into degraded state
  and asserts the gauge reads 1, then 0 after recovery. *Disposition:* **new task
  T114** (coordinate with T71's gauges — distinct metric, shared registration path).

- **SRE-13 [Med-High, underspecified/design-disagreement]** — *`Health` false-green
  for consumer liveness.* `Connection.Health(ctx)` (§6.1 L660–663; `connection.go:220`)
  opens/closes a temp channel + checks ≥1 healthy socket + not topology-degraded, but
  **not consumer-subscription liveness**. Given silent channel death (SRE-01/T61),
  `Health` returns **OK while a consumer is silently dead** → a **false-green on
  liveness/readiness probes** (k8s keeps routing to a pod that consumes nothing).
  **Detect:** the probe is the detector — and it lies. **Respond:** make `Health`
  reflect consumer state. **Verify:** a channel-only consumer death flips `Health` to
  unhealthy until resubscribed. *Location:* §6.1 `Health` L660–663; interacts with
  T61. *Outcome (owner decision):* **aggregate consumer-subscription liveness** into
  `Connection.Health` — it returns non-nil while any registered consumer is not
  currently subscribed; document the semantics (and that T61's self-heal returns it
  to green once resubscribed). *Acceptance (SG-3):* on the `chaos` lane, force a
  channel-only consumer death and assert `Connection.Health` returns non-nil while
  unsubscribed, then nil after T61 reopens + resubscribes. *Disposition:* **new task
  T115**.

### WS-4 — Capacity honesty & the runbook *(new — mostly doc)*

- **SRE-14 [High, factual-error/underspecified]** — *vanity throughput numbers.* §9
  targets 30k msg/s (single conn) and 100k (4 conns × 16 channels) on an M-series
  laptop against a **local** broker (L2471–2479). R10-18 (LATER-34, L3150–3157) admits
  `Publish` holds a pooled channel for a full confirm RTT, so per-`Publish` throughput
  is `pool/RTT` and the targets "hold only at sub-ms (local-broker) RTT." For a remote
  broker (RTT 1–10ms) the achievable rate collapses **1–2 orders of magnitude**, and a
  confirm-latency spike cascades into `ErrChannelPoolExhausted`. The documented numbers
  are **unreachable in any real deployment**, and the ceiling is buried in a LATER
  entry. **Detect:** `ErrChannelPoolExhausted` + the pool acquire-wait metric (SRE-05/
  T71). **Respond:** raise pool size / accept the ceiling. **Verify:** a benchmark that
  *states RTT + hardware + broker config*. *Location:* §9 L2471–2479; §6.2 publisher;
  R10-18/LATER-34. *Outcome (owner decision: doc-only):* amend **§9 and §6.2** to state
  prominently that the targets are **local-broker (sub-ms RTT)**, the per-`Publish`
  ceiling is `pool/RTT`, the remote-broker collapse, and the confirm-spike →
  `ErrChannelPoolExhausted` cascade; every throughput number carries its RTT + hardware
  + broker config. The async-publish-API decision **remains LATER-34** (not pulled).
  *Acceptance (SG-4):* the §9 benchmark prose states RTT/hardware/broker; an injected-
  RTT test demonstrates the `pool/RTT` relationship and the `ErrChannelPoolExhausted`
  onset. *Disposition:* **new task T116** (doc + benchmark caveat).

- **SRE-16 [Med, underspecified]** — *no runbook; no §1-bar→metric audit.* Tracing has
  a runbook-shaped mapping (`error.type` → action, §6.9 L2140–2143), but **metrics do
  not**: there is no documented mapping from each mandatory metric to an operator
  action + alert query, and no proof that **every §1 "no silent X" bar** has a
  corresponding always-on metric (any bar without one is *silent in practice*).
  **Detect:** the audit itself. **Respond:** the runbook. **Verify:** every §1 bar maps
  to a metric + an example alert query. *Location:* §1 L43–84; §6.9 mandatory metrics
  L2078–2091; §9. *Outcome:* add a **runbook table** (§9 or §6.9) mapping each
  mandatory metric → detect signal → operator action → recovery-verify signal, and an
  explicit **§1-bar → metric + alert query** audit (surfacing that the redelivery
  leading indicator is SRE-05/T71 and the current-degraded signal is SRE-12/T114).
  *Acceptance:* the table exists; a doc test (or review checklist) asserts every §1 bar
  and every mandatory metric appears with an alert query. *Disposition:* **new task
  T118** (doc).

---

## 6. Do-not-regress list (confirmed-correct — protect with tests)

These are operability strengths the review confirmed; protect them:

- **Role-split pool isolation** (§6.1 L551–556): publisher vs consumer connections —
  a slow consumer cannot starve publish acks and a flow-controlled publisher cannot
  block consumer dispatch. Keep a test asserting a blocked publisher connection does
  not stall consumer dispatch and vice versa.
- **Blocked-connection wait, not drop** (§6.1 L618–625): while any publisher conn is
  blocked, `Publish` routes around it; if all are blocked it **waits** for unblock or
  `ctx.Done()` and returns `ErrConnectionBlocked` — never silent loss.
- **`ConfirmTimeout=30s` default** (§9 L2513–2516): the backstop against an
  indefinitely-withheld ack. (SRE-09/T72 + SRE-02/T63 close the layers *around* it; do
  not weaken the default.)
- **Reconnect synchronisation barrier** (§6.1 L576–594): re-open → redeclare →
  re-`consume` → `OnReconnect`; `Publish` blocks (cancellable) on `ErrReconnecting`.
- **Degraded mode surfacing** (§6.1 L595–612): persistent redeclare failure →
  `connection_degraded_total` + `WithOnTopologyDegraded` once-per-transition +
  `ForceReconnect()` + `ErrTopologyRedeclareFailed` (never a silent no-op `Publish`).
- **`Prefetch < Concurrency` build warning** (§6.3 L1176–1179) and the sizing math
  `throughput ≈ Concurrency / handler_latency` (L1171–1173); the large-prefetch →
  larger-redelivery-blast-on-crash trade-off (L1190–1193).
- **`basic.cancel` never silently kills the consumer** (§6.3 L1323–1343):
  `consumer_cancelled_total` + `OnCancel` + `Consume` returns `ErrConsumerCancelled`.
- **Tracing-for-postmortem** (§6.9 L2106–2168): DLX path preserves `traceparent`/
  `tracestate`; consumer/publisher span `error.type` + `outcome` make poison filterable.
- **Heartbeat / LB floor guidance** (§6.1 L695–698): never below the smallest path
  idle-timeout; 30s floor behind cloud LBs — verify the advice survives edits.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Amend SPEC | Disposition | LATER.md | Owner decision? |
|---------|-----|-----------|-------------|----------|-----------------|
| SRE-01 | Blocker | (T61) | **covered by T61** — Lens-05 bullet, do not re-file | — | — |
| SRE-02 | Blocker | (T63) | **covered by T63** — Lens-05 bullet | — | — |
| SRE-03 | Blocker | (T65) | **covered by T65** — Lens-05 bullet | — | — |
| SRE-04 | High | (T66) | **covered by T66** — Lens-05 bullet | — | — |
| SRE-05 | High | (T71) | **covered by T71** — Lens-05 bullet | — | — |
| SRE-06 | Med-High | (T62) | **covered by T62** — Lens-05 bullet | — | — |
| SRE-07 | High | (T70) | **covered by T70** — Lens-05 bullet | — | — |
| SRE-08 | High | ✅ §6.1 | **pull forward T67** | — | ✅ pull forward |
| SRE-09 | High | ✅ §6.1 | **pull forward T72** | — | ✅ pull forward |
| SRE-10 | High | ✅ §6.9/§6.1/§8 | **T112** (`WithMetricsRegisterer`) | — | ✅ private-registry default |
| SRE-11 | Med-High | ✅ §6.9 | **T113** (extend default buckets) | — | ✅ extend default |
| SRE-12 | Med | ✅ §6.1/§6.9 | **T114** (`connection_degraded` gauge) | — | — |
| SRE-13 | Med-High | ✅ §6.1 | **T115** (Health consumer-liveness) | — | ✅ aggregate |
| SRE-14 | High | ✅ §9/§6.2 | **T116** (honest ceiling, doc) | (LATER-34) | ✅ doc-only |
| SRE-15 | Med | ✅ §6.9 | **T117** (cardinality budget + opt-out) | — | — |
| SRE-16 | Med | ✅ §9/§6.9 | **T118** (runbook + §1-bar audit, doc) | — | — |

---

## 8. Suggested phasing (planner may revise — "Phase 16")

1. **Phase A — Gate (no new lane).** Capture SG-1..SG-4 (unit + existing integration/
   `chaos` lanes); commit the results table. No SPEC edit to an affected section until
   its gate returns. **(T111)** Records §10 **Rev 15**.
2. **Phase B — Metrics correctness (SPEC-first, unit-testable).** Registry injection
   (**T112**, SG-1), default-bucket extension (**T113**, SG-2), cardinality budget +
   opt-out (**T117**). These reshape the `metrics` surface + the connection-level
   default wiring; land them together.
3. **Phase C — Current-state signals + liveness (chaos-lane).** Degraded-state gauge
   (**T114**), `Health` consumer-liveness aggregation (**T115**, SG-3 — coordinate
   with T61). 
4. **Phase D — Operability pull-forwards (chaos/integration).** `Dial`
   partial-pool-connect policy + degraded-capacity signal (**T67**, pulled), TCP
   keepalive / half-open-write (**T72**, pulled). Sequence after the metrics so the
   new boot/half-open signals slot into the extended buckets + runbook.
5. **Phase E — Capacity honesty & runbook (doc).** Honest §9/§6.2 throughput ceiling
   (**T116**, SG-4), runbook metric→action + §1-bar→metric/alert audit (**T118**).
   Land last so the runbook references every metric the prior tasks added.

---

## 9. Acceptance criteria for the whole effort

- [ ] SG-1..SG-4 captured (unit + existing integration/`chaos` lanes, 3.13 **and**
      4.x where broker-bound) into a committed results table each downstream task
      cites (T111). **No new build-tag lane.** First task records §10 **Rev 15**.
- [ ] **Cut-line endorsed (SRE-01..07):** each of T61/T62/T63/T65/T66/T70/T71 carries
      a `Lens-05 (SRE-xx)` acceptance bullet (detect/respond/verify); none re-filed,
      none re-pulled. A note states reverting any pull flips this lens to NO-GO.
- [ ] **Pull-forwards landed (SRE-08/09):** T67 ships succeed-if-≥1-per-role +
      degraded-capacity boot signal; T72 ships dialer keepalive + half-open-write test
      (publish on a dead socket errors promptly, not at 30s).
- [ ] **Registry footgun closed (SRE-10/T112):** `WithMetricsRegisterer` exists; the
      default is a **private per-`Connection` registry** (never `DefaultRegisterer`);
      a double-`Dial` in one process does **not** panic (SG-1).
- [ ] **Incident latency visible (SRE-11/T113):** default histogram buckets cover the
      30s `ConfirmTimeout` + reconnect-barrier envelope; a 30s stall lands in a finite
      bucket, not `+Inf` (SG-2); §6.9 explains the envelope.
- [ ] **Cardinality bounded (SRE-15/T117):** §6.9 documents the `queue`/`exchange`
      cardinality budget and ships an opt-out to omit/aggregate those labels.
- [ ] **Current-state signals (SRE-12/T114):** a `connection_degraded{role}` gauge
      reads 1 while degraded, 0 after recovery; listed in §6.9 mandatory metrics.
- [ ] **Readiness false-green killed (SRE-13/T115):** `Connection.Health` returns
      non-nil while a registered consumer is unsubscribed and nil after resubscribe
      (SG-3); semantics documented.
- [ ] **Capacity honesty (SRE-14/T116):** §9/§6.2 state the local-broker (sub-ms RTT)
      caveat, the `pool/RTT` ceiling, the remote collapse, and the
      `ErrChannelPoolExhausted` cascade; every throughput number carries RTT +
      hardware + broker config (SG-4). The async-API decision stays **LATER-34**.
- [ ] **Runbook shipped (SRE-16/T118):** a metric→detect→respond→verify table exists
      and every §1 "no silent X" bar maps to a metric + an example alert query.
- [ ] No finding re-filed that a prior lens owns (SRE-01→T61, 02→T63, 03→T65, 04→T66,
      05→T71, 06→T62, 07→T70). **No new `LATER.md` entry.** No duplicate task IDs.
- [ ] `README.md` reflects the changed contract (`WithMetricsRegisterer`, the
      default-bucket change, the `connection_degraded` gauge, `Health` consumer-
      liveness, the cardinality opt-out, the honest §9 ceiling).
- [ ] Tree green: `make test` (race + cover), `make lint`, the per-version
      integration lane, the `chaos` lane; `goleak` clean.

---

## 10. Owner decisions (recorded 2026-05-28)

1. **Pull both unclaimed operability deferrals forward (SRE-08/SRE-09).** Pull **T67**
   (`Dial` partial-pool-connect policy, R10-12) *and* **T72** (TCP keepalive /
   half-open write, R10-17) forward from Phase 11 into Phase 16.
2. **Histogram default buckets = EXTEND (SRE-11).** Raise the default top buckets to
   cover the real envelope (add `10s, 30s, 60s`) so a `ConfirmTimeout`/barrier stall
   is visible, not collapsed into `+Inf`. **(T113)**
3. **`Health` = AGGREGATE consumer liveness (SRE-13).** `Connection.Health` reports
   unhealthy while a registered consumer is not currently subscribed, closing the
   readiness/liveness probe false-green. **(T115)**
4. **Throughput ceiling = DOC-ONLY honesty fix (SRE-14).** Amend §9/§6.2 to state the
   local-broker (sub-ms RTT) caveat + the `pool/RTT` ceiling + the remote collapse;
   the async/streaming publish-API decision **remains deferred in LATER-34** (not
   pulled into v0.1). **(T116)**

Recorded by owner decision and the §8 "no globals" rule (no separate prompt needed):
**Registry injection (SRE-10/T112)** — the default is a private per-`Connection`
registry and `WithMetricsRegisterer(prometheus.Registerer)` is the injection point;
`prometheus.DefaultRegisterer` is never used by default.

(The remaining new tasks — T112, T114, T117, T118 — and the cross-lens bullets follow
directly from the findings and need no further owner input.)

---

## 11. Detect / Respond / Verify table (lens output #1)

For each major failure mode: the signal that fires, the knob/action that responds,
the signal that confirms recovery, and whether a gap remains **after** Phase 16.

| Failure mode | Detect (signal) | Respond (knob/action) | Verify (recovery signal) | Gap after Phase 16? |
|--------------|-----------------|------------------------|--------------------------|---------------------|
| Reconnect storm (thundering herd) | `connection_reconnects_total` spike; chaos asserts no `addr[0]` stampede | `WithAddrs` shuffle + rotate (T66) | reconnect spread across nodes | closed (T66 + SRE-04) |
| Redeclare amplification | `topology_redeclare_seconds` spike; declare count per recovery | redeclare once per recovery at `*Connection` (T62) | declare count == 1, not N×pool | closed (T62 + SRE-06) |
| Poison storm | `consumer_redelivered_total` ratio (T71); `consumer_handler_timeout_total` | quorum `x-delivery-limit` / `MaxRedeliveries` → DLX | redelivery ratio falls; DLX receives | closed (T71/T65 + SRE-05) |
| DLQ disk-fill (cluster-wide block) | DLQ-depth + `connection_blocked_seconds` (lagging) | bounded-DLQ-by-default + overflow policy (T65) | DLQ depth bounded; no broker alarm | closed (T65 + SRE-03) |
| Silent channel death | `consumer_resubscribed_total` (must increment on channel-only death) | auto channel reopen + resubscribe (T61) | resubscribe metric + `Health` flips green | closed (T61 + SRE-01/13) |
| Half-open write hang | publish lands in 30s/finite bucket (after T113) | dialer keepalive / `TCP_USER_TIMEOUT` (T72) | publish errors promptly on dead socket | closed (T72 + SRE-09) |
| Pool exhaustion (confirm-RTT spike) | pool acquire-wait/saturation (T71); `ErrChannelPoolExhausted` | raise pool/conns; honest sizing (T116) | acquire-wait p99 falls | closed (T71/T116 + SRE-05/14) |
| Degraded topology | `connection_degraded` **gauge** (T114) + `connection_degraded_total` | `ForceReconnect()`; fix conflicting def | gauge → 0; `WithOnReconnect` fires | closed (T114 + SRE-12) |
| Deploy drain (rolling restart) | `consumer_shutdown_requeued_total` (T70) | nack-requeue undispatched + flush batch (T70) | requeued count == undispatched | closed (T70 + SRE-07) |
| Partial-pool boot | degraded-capacity boot signal (T67) | succeed-if-≥1-per-role + supervised reconnect (T67) | missing socket reconnects; signal clears | closed (T67 + SRE-08) |
| Metrics blackhole (cardinality) | Prometheus scrape-timeout/OOM | bounded-labels opt-out + budget doc (T117) | series count within budget | closed (T117 + SRE-15) |
| Registry panic (double-`Dial`) | panic at second `Dial` | private-registry default + `WithMetricsRegisterer` (T112) | double-`Dial` no panic | closed (T112 + SRE-10) |

---

## 12. Findings table (lens output #2)

| ID | Sev | Class | Location (§ + lines) | On-call impact | Blast radius | Disposition |
|----|-----|-------|----------------------|----------------|--------------|-------------|
| SRE-01 | Blocker | deferred-gap | §6.1; R10-6 L3085–3091 | consumer stops consuming with no metric/log; readiness lies | one queue → one service backs up | **T61** (covered) |
| SRE-02 | Blocker | deferred-gap | §6.1 L576–594; R10-8 L3098–3104 | publisher fleet hung indefinitely, no error | every publisher behind a half-alive broker | **T63** (covered) |
| SRE-03 | Blocker | deferred-gap | §6.6; R10-10 L3112–3119 | one service's poison storm → broker-wide publish outage | **whole cluster** (disk alarm) | **T65** (covered) |
| SRE-04 | High | deferred-gap | R10-11 L3120–3122 | cluster restart stampedes one node → cascade | the recovering cluster | **T66** (covered) |
| SRE-05 | High | deferred-gap | §6.9; R10-16 L3137–3141 | poison/pool saturation invisible until it is an outage | every consumer fleet | **T71** (covered) |
| SRE-06 | Med-High | deferred-gap | §6.1; R10-7 L3092–3097 | recovery action hammers the just-recovered broker | N×pool×fleet declares | **T62** (covered) |
| SRE-07 | High | deferred-gap | §6.4; R10-15 L3133–3136 | every rolling deploy loses/duplicates N msgs/pod | every deploy | **T70** (covered) |
| SRE-08 | High | deferred-gap | §6.1 L501–502; R10-12 L3123–3126 | unpredictable boot: deploy-blocking or silent capacity loss | every restart/rollout | **pull fwd T67** |
| SRE-09 | High | deferred-gap | §6.1 L678–701; R10-17 L3142–3146 | publisher hangs up to 30s on a dead socket | per-socket publishers | **pull fwd T72** |
| SRE-10 | High | underspecified | §6.9 L2056–2091; §6.1 L511; §8 | second `Dial` panics; hidden-global metrics | the whole process (crash) | **T112** (SG-1) |
| SRE-11 | Med-High | design-disagreement | §6.9 L2073–2076 | slow/stalled publishes invisible during incident | every dashboard/alert | **T113** (SG-2) |
| SRE-12 | Med | underspecified | §6.1 L595–612; §6.9 L2078–2091 | can't tell if a connection is degraded *right now* | per-connection alerting | **T114** |
| SRE-13 | Med-High | underspecified | §6.1 L660–663 | readiness probe false-green; k8s routes to a dead consumer | per-pod (silent backlog) | **T115** (SG-3) |
| SRE-14 | High | factual-error | §9 L2471–2479; R10-18 L3150–3157 | documented throughput unreachable on any remote broker | every capacity plan | **T116** (SG-4) |
| SRE-15 | Med | underspecified | §6.9 L2059–2071 | always-on `queue`/`exchange` labels OOM Prometheus | the monitoring system | **T117** |
| SRE-16 | Med | underspecified | §1 L43–84; §6.9 L2078–2091 | no metric→action runbook; some §1 bars unaudited | on-call response time | **T118** |

---

## 13. Cut-line recommendation (lens output #3)

The prompt's central demand: *which deferred Rev-10 items are operability Blockers
that must gate v0.1, not v1.0?* The answer, after reconciliation:

- **R10-6 (T61), R10-8 (T63), R10-10 (T65), R10-11 (T66), R10-16 (T71)** — the five
  the SRE lens would demand — are **already pulled into v0.1** (Phases 12/13) by
  Lenses 01/02. Plus **R10-7 (T62)** and **R10-15 (T70)**. The cut-line is therefore
  **already drawn correctly**; this lens **endorses and hardens** it (cross-lens
  bullets) and flags that **reverting any of these pulls flips this lens to NO-GO** —
  shipping v0.1 with silent consumer death, an unbounded silent publisher stall, an
  unbounded DLQ that can take out the cluster, or without the #1 instability metric
  is not acceptable at the stated bar.
- **R10-12 (T67)** and **R10-17 (T72)** — the two remaining unclaimed operability
  deferrals — **move into v0.1** (this phase, owner decision): a publisher hung 30s on
  a dead socket and an unpredictable partial-pool boot are both on-call Blockers in
  spirit; pull them forward.
- **R10-18 (LATER-34, async publish API)** stays deferred as an **architecture
  decision** — but its **operational consequence is made honest now** (SRE-14/T116):
  the §9 numbers get their RTT/hardware caveat and the `pool/RTT` ceiling moves out of
  LATER and into §9/§6.2.

Net: v0.1's operability cut-line = the seven already-pulled tasks + T67 + T72 + the
new metrics/honesty set (T112–T118). Nothing further needs to move from v1.0 into
v0.1.

---

## 14. Out of scope for this plan

- The other validation lenses (01 protocol — Phase 12; 02 distributed-systems —
  Phase 13; 03 interoperability — Phase 14; 04 EDA — Phase 15; 06–13). Cross-lens
  shared findings extend the shared task, never re-filed (SRE-01→T61, 02→T63, 03→T65,
  04→T66, 05→T71, 06→T62, 07→T70).
- The **async/streaming publish API** with a bounded in-flight window (reverses Rev-6
  decision 31) — remains **LATER-34**; this lens only makes the ceiling honest
  (SRE-14/T116).
- A full metric-relabeling/aggregation pipeline beyond the simple `queue`/`exchange`
  opt-out (T117 ships the opt-out + budget doc; anything richer is a future concern).
- OAuth2 auth backend (v0.2). Stream-protocol semantics (v0.2, decision 24).
- Any change to the §10 API design north stars beyond the corrections above.

---

## 15. Verdict for this lens

**GO-WITH-CHANGES.** From the pager's seat, the most dangerous failure modes — silent
consumer death, an unbounded silent publisher stall, an unbounded DLQ that escalates
to a cluster-wide publish outage, a thundering-herd reconnect, and the absence of the
#1 instability metric — are **real and severe**, but they are **already pulled into
v0.1** by Lenses 01/02 (T61/T62/T63/T65/T66/T70/T71); this lens hardens their
detect/respond/verify acceptance rather than re-filing them. What Lens 05 *adds* is an
**observability-correctness** set: a metrics-registration footgun that crashes the
process on a double-`Dial` (T112), a histogram that goes blind above 5s exactly when
you need it (T113), no current-state degraded signal (T114), a readiness probe that
returns false-green over a dead consumer (T115), throughput numbers that are
unreachable on any real broker (T116), unbounded label cardinality (T117), and no
operator runbook (T118) — plus two pull-forwards (T67 partial-pool boot, T72 half-open
write). None introduces a *new* silent-message-loss path (the registry footgun is a
loud crash). The cut-line is correct **because** the prior lenses pulled the blockers
forward — and this lens records that reverting any of those pulls would make it
**NO-GO**.
