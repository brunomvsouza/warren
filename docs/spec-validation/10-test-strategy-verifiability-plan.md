# Plan Input — Remediate Test-Strategy & Verifiability Findings (Lens 10)

> **Lens:** Test Strategy & Verifiability (`spec-validation/10-test-strategy-verifiability.md`).
> **Persona:** a test architect who treats §9 "Success Criteria" as a test plan and asks of
> every line *is this falsifiable, is it actually tested, and can the test fail for the right
> reason?*
> **Verdict:** **GO-WITH-CHANGES** — **no Blocker.** The library's behaviour is well tested
> at the unit level (631 unit tests, 6 fuzz targets, 212 `goleak` assertions, `-race`
> everywhere) and the highest-risk contracts already have **owned** tasks with named tests.
> The gap this lens exposes is **not** in the code — it is in the **verification
> infrastructure and the honesty of the criteria**: a CI that runs only `unit` + `integration`
> (single broker image, Go 1.23) silently leaves a large slice of §9 **unrun**, and several §9
> numbers cannot fail in CI at all. **Like Lens 09, this lens is heavily cross-lens and its
> headline is already owned** — every "untested contract" the prompt anticipated (R10-3 ordering,
> polyglot interop, the Rev-10 failure modes, the version-divergent quorum limit, the security
> credential grep) is **already owned by a prior task** (T59/T94/T61/T63/T67/T81/T45b). This
> lens **confirms** those (do-not-regress) and **contributes the verification-infrastructure
> layer they all silently depend on**: a scheduled/nightly runner, a RabbitMQ version matrix,
> an enforced coverage floor, and a §7/§9 rewrite that classifies every criterion by *where it
> actually runs*. **15 findings (TV-01..TV-15); 6 gates (VG-1..VG-6); 3 net-new tasks
> (T150–T152); 8 cross-lens extensions; 3 confirmations; LATER-46.**

---

## 1. Objective

Make the `warren` test strategy **verifiable**: every §9 success criterion must either run in a
*named CI lane* and be able to *fail for the right reason*, or be *honestly reclassified* (as a
nightly job, a release-only benchmark, an operator-validated recommendation, or a polyglot-lane
assertion) so the spec stops implying a check that does not exist. The bar is binary:

- **A success criterion that cannot fail (or cannot run in CI) is a finding, not a pass.** A
  number measured on the author's laptop (`≥30k msg/s on a single Apple M-series laptop`,
  §9 L2473) is not a CI gate. A criterion behind a lane that does not exist (`Conformance suite
  passes`, §9 L2435) is not verified. A floor that the workflow never enforces (`Coverage ≥80%
  per package`, §9 L2491; §7 "enforced in CI", L2235) is vanity.
- **Match each contract to the *weakest* test level that can actually prove it.** A stub
  "proving" a real-broker behaviour (§7 conformance "protocol-level checks use a test AMQP
  server stub", L2261-2262) is a finding. A Go-only harness "proving" cross-language fidelity
  is a finding (already owned by the polyglot lane T94).
- **Probabilistic-race contracts (R10-3 return/ack ordering) need load+repeat real-broker
  tests** and the spec must be honest they are flaky-prone — a mock tracker cannot reproduce
  `amqp091-go`'s two-channel notify dispatch timing, so a green mock test proves nothing about
  the race it exists to lock.

The net-new value is the **infrastructure and criteria-honesty layer no prior lens owns**: the
missing **scheduled/nightly workflow** (every "nightly" criterion is currently unrun), the
missing **RabbitMQ 3.13 + 4.x matrix** (version-divergent contracts unverified on the version
where they bind), the **unenforced coverage floor**, the **author-laptop throughput numbers**
(reframe to a relative regression gate), and the **§7/§9 internal inconsistencies** (§7 says
60s @ 1k / 100x; §9 says 5-min @ 10k / 1000; §3/§7 say "testcontainers"; the harness is
docker-compose + `AMQP_TEST_URL`). **There is no message-loss Blocker** — the closest, the
chaos test's unspecified loss-counting method (TV-09), is a harness-honesty gap on an
*already-owned* test (T45), scored High.

---

## 2. Source of truth & references

- **SPEC §7 Testing Strategy** (L2212-2330): Levels table (L2216-2222), Frameworks (L2224-2231),
  Coverage (L2233-2239), Reconnect chaos test (L2241-2247), Poison-loop test (L2249-2254),
  Conformance (L2256-2262), Memory and concurrency (L2264-2269), Executable examples
  (L2271-2329).
- **SPEC §9 Success Criteria** (L2426-2549): the 34 checkboxes — Functional, Reliability,
  Throughput, Security, Coverage/tooling, Operational (Rev 6).
- **SPEC §3 Commands** (L130-166): claims integration "spins up RabbitMQ via testcontainers-go"
  (L139), a conformance lane (L142-143), nightly fuzz "10m budget" (L148-149), bench (L151-152).
- **SPEC §6.2.1 At-least-once / dedupe** (L1013-1068): the "15 minutes of MessageID retention"
  recommendation (L1056-1059) — a design claim with no possible test.
- **SPEC §6.3 / §6.8** load-bearing contracts: R10-3 return/ack ordering, R10-2 quorum
  default-limit, broker `basic.nack`, `basic.cancel`, x-death reason tokens.
- **Reality (verified this pass):**
  - `.github/workflows/ci.yml` — **only `unit` + `integration`**; no conformance lane, no fuzz
    lane, no bench lane, no `schedule:` trigger; Go **1.23 only**; integration service is a
    single `rabbitmq:3-management`; `go test -race -cover` with **no fail-under, no artifact,
    no PR comment**.
  - `docker-compose.integration.yml` — single `rabbitmq:3-management`; **no 4.x, no
    delayed-message-exchange plugin, no TLS / `rabbitmq_auth_mechanism_ssl`**.
  - Integration tests dial `AMQP_TEST_URL` via raw `amqp091.Dial` and **`t.Skip` when it is
    unset** — `testcontainers-go`/`amqptest` is **not built yet** (owned by T37).
  - **No `//go:build conformance` files exist**; no AMQP-server stub exists. Conformance is owned
    by **T44** (op-decision §5: live broker for v0.1, stub deferred).
  - Only **3 benchmarks** exist (`metrics/metrics_test.go` NoOp overhead micro-benchmarks); the
    `BenchmarkPublishConfirmed`/`MultiConn`/`Batch`/`Consume` throughput suite is owned by
    **T44b** and not yet implemented.
- **Lens prompt:** `spec-validation/10-test-strategy-verifiability.md` (this pass *conducted*
  the review; this file is its remediation brief for `/plan`).

### Cross-lens reconciliation (do **not** re-file — project rule)

A finding already owned by a prior task **extends that task** with a `Lens-10 (TV-xx)` acceptance
bullet; it is **never** re-filed. This lens is dominated by such overlaps — the prompt's headline
("the load-bearing contracts the stated strategy cannot verify") is, almost line for line,
already owned:

| Prompt-anticipated gap | Already owned by | Lens-10 disposition |
| --- | --- | --- |
| R10-3 return/ack ordering needs a real-broker load+repeat test (not a stub/mock) | **T59** (Phase 11; extended by Lens-01 RMQ-16: real-broker assertion + pin `amqp091-go` + single-reader comment) | **extend** T59 — add the *load+repeat* dimension + *flaky-by-design* honesty |
| Cross-language CloudEvents / field-table / `time.Time`→`T` fidelity is unverified by a Go-only harness | **T94 + T37** (Lens-03 IW-13: a full polyglot `interop` lane — `pika` + Java + AMQP-1.0) | **confirm** (do-not-regress) — §9 cross-language criteria gate on the polyglot lane |
| R10-2 quorum default-limit-20 / classic `x-delivery-limit`-ignore need a 4.x broker | **T81** (Lens-01 version-divergence docs) | **extend** T81 + the new version matrix (TV-05) is what *verifies* its claims |
| Rev-10 failure modes (R10-6 channel-only close, R10-8 half-alive broker, R10-12 partial-pool boot) ship in v0.1 with no test | **T61 / T63 / T67** — *pulled into v0.1*, with **named tests** (T63 = "half-alive-broker proxy test … asserts `Publish` returns `ErrReconnecting` within the cap") | **confirm** (do-not-regress) — the anticipated "deferred, untested" gap is **resolved**; coordinate that the proxy harness lands in a runnable lane |
| Security credential grep must cover wrapped `amqp091-go` errors | **T45b** (Lens-07) | **extend** T45b — grep breadth + forced-broker-error exercise |
| Coverage % is not correctness for `internal/confirms`/`internal/reconnect` (bugs are in interleavings) | **T143/T146** (Lens-08 concurrency criteria + goroutine-inventory) | **confirm** + cross-link in the §7 wording (TV-12 → capstone) |
| Throughput numbers are not CI-gradeable / need RTT + queue type | **T44b / T83 / T116 / T149** (Lens-09) | **extend** T44b — the *CI-gateability* reframe (relative regression gate) layered on Lens-09's methodology |

**Net-new (no prior owner):** the **scheduled/nightly workflow** (TV-01), the **RabbitMQ
3.13/4.x integration matrix** (TV-05), the **integration-broker-required guard** (TV-13), the
**conformance stub-vs-real-broker contract matrix** as a §7 artifact (TV-06), and the **§7/§9
verifiability rewrite** that classifies every criterion by where it runs (TV-10/11/12 capstone).

---

## 3. Constraints & working agreements (planner must honour)

1. **No re-file.** The 8 cross-lens findings extend their owning task in place; the 3
   confirmations (T94, T61/T63/T67, T143/T146) add **no** new task — at most a do-not-regress
   bullet. Only TV findings with **no** owner become net-new tasks (T150–T152).
2. **Gate-first.** T150 captures ground truth (VG-1..VG-6) **before** any §7/§9 wording change or
   any workflow edit — mirroring T84/T94/T101/T111/T119/T130/T143/T147. It records SPEC §10
   **Rev 20** when it lands.
3. **Amend SPEC first, same PR.** Every §7/§9/§6.2.1 wording change is a SPEC amendment that
   lands *with* its task during execution (CLAUDE.md "amend SPEC first") — **not** in this
   `/spec` step and **not** in the subsequent `/plan` materialization step.
4. **New build-tag lanes are allowed here** (unlike Lenses 04–09): a `conformance` lane already
   exists in the SPEC's Levels table (§7 L2220) and §3 (L142-143) but **not** in CI; wiring it
   is remediation, not a new invention. The **scheduled/nightly** workflow is a new
   *trigger*, not a new tag. The **version matrix** is a new *axis* on the existing
   `integration` lane.
5. **Cost is a real constraint.** A 4.x×3.13 matrix ≈ 2× the integration lane; a nightly chaos
   run is 50 × 5 min @ 10k msg/s. The planner must place expensive criteria on the **nightly**
   trigger or **release-tag** cadence, not on every PR — and §9 must *say so* per criterion.
6. **Do not weaken any guarantee.** Reframing a vanity number to a relative regression gate, or a
   stub claim to real-broker-only, must not *drop* a contract — it makes the existing contract
   *checkable*. The §1 "no silent message loss" bar is non-negotiable.
7. **No code/SPEC/README edits in `/spec` or `/plan`.** This brief and the Phase-21 materialization
   only touch the brief + the task ledger. README touch points to expect later (during execution):
   the `make` target list (a new `make ci-nightly` / matrix target), the CI badge/lane
   description — checked per task.

---

## 4. Pre-work: verification gates (VG-1..VG-6 — sequence FIRST; they gate wording/fixes)

Capture ground truth so every later wording change is anchored to a measured fact, not a guess.
Results table under `spec-validation/`. **T150 owns these and records §10 Rev 20.**

- **VG-1 — "what is green by not running" baseline.** Enumerate every §9 checkbox and mark which
  currently *execute* in CI (`unit` + `integration`, Go 1.23, `rabbitmq:3-management`) vs which
  are **structurally unrun**: the `nightly` criteria (chaos 5-min @ 10k × 50 runs, 1000-cycle
  stress, fuzz 10m/target), the `conformance` suite, the throughput bench, the 4.x-only contracts,
  the Go-1.24 row, the delayed-exchange / TLS / SASL-EXTERNAL criteria. Output: the §11 table
  filled with the *actual* "CI-gateable?" column. → gates TV-01/04/05/06/07/08/20.
- **VG-2 — coverage floor is unenforced.** Show `go test -race -cover ./...` in `ci.yml` produces
  a coverage number but **no step fails the build below 80%/95%** (no `-coverprofile` threshold,
  no artifact upload, no PR comment). Demonstrate by computing current per-package coverage and
  confirming a hypothetical drop below the floor would still pass. → gates TV-03.
- **VG-3 — integration lane skips green without a broker.** Run `go test -race -tags=integration
  ./...` with `AMQP_TEST_URL` **unset**; count the `t.Skip`ped tests and confirm the lane exits
  **0** (green) having asserted nothing. This is the "integration suite passes" criterion
  (§9 L2437) satisfiable by skipping everything. → gates TV-13.
- **VG-4 — R10-3 mock vs real-broker reproduction.** For the return/ack ordering invariant (T59):
  measure whether the current mock-tracker test can *ever* fail when the demux is intentionally
  broken (split across goroutines / buffered notify chan), then characterize how many real-broker
  iterations under concurrent unroutable-mandatory-publish load are needed to trip the race ≥1×.
  Establishes whether the test fails for the right reason and how flaky it is. → gates TV-02 (T59).
- **VG-5 — version-divergent contracts on 4.x vs 3.13.** Stand up both a `rabbitmq:3.13` and a
  `rabbitmq:4` broker; assert (a) the quorum default `x-delivery-limit` (R10-2, default 20) drops
  the 21st delivery on 4.x and (b) the classic-queue `x-delivery-limit`-ignore behaviour, and
  record any divergence. Establishes the matrix is *necessary*, not cosmetic. → gates TV-05 (T81).
- **VG-6 — chaos loss-counting can detect an injected drop.** Run the chaos harness (or its T45
  skeleton) and verify "zero message loss" is computed as **published-set − consumed-set with
  dedupe by `MessageID`** (accounting for at-least-once duplicates) by **injecting a single
  deliberate drop** and confirming the harness reports loss == 1. A harness that cannot detect an
  injected drop can pass while quietly losing. → gates TV-09 (T45).

---

## 5. Workstreams (grouped findings, sequenced)

### WS-1 — CI verification infrastructure *(the net-new spine — nothing runs without it)*

- **TV-01 (High) — no scheduled/nightly workflow exists.** §7 (L2268) + §9 (L2448) + §3 (L148)
  all say "nightly" (fuzz 10m/target, 100x/1000-cycle stress, chaos 5-min @ 10k × 50 runs,
  flaky-rate < 1%). `ci.yml` has **only `push`/`pull_request`** triggers — **no `schedule:`**. So
  *every reliability/fuzz criterion that says "nightly" has no runner*. A reliability criterion
  that never runs is nearly as bad as one that cannot fail. **→ new task T151** (add a
  `schedule:`-triggered workflow running the fuzz budget + 1000-cycle stress + chaos 50-run, and
  reporting flaky-rate); coordinate with T45 (chaos), T45b (security), the fuzz targets, and the
  Lens-08 1000-cycle test (T45/CR-10). **(D2)**
- **TV-05 (High) — no RabbitMQ 3.13 + 4.x matrix.** §9 L2434 requires the integration suite to
  pass against **both 3.13 LTS and 4.x**; §7 Frameworks (L2230) says "pinned image tags for 3.13
  LTS and 4.x". CI and docker-compose run a **single `rabbitmq:3-management`**. Version-divergent
  contracts — R10-2 quorum default `x-delivery-limit` (4.x behaviour), classic
  `x-delivery-limit`-ignore, Khepri `queue.declare` semantics (T63/T81) — are **unverified on the
  version where they bind**. **→ new task T151** owns the integration matrix axis; **extend T81**
  (version-divergence docs are *verified* by this matrix). **(D3)**
- **TV-03 (Med) — coverage floor asserted but not enforced.** §9 L2491-2493 + §7 L2235 ("Floor:
  80% per package, **enforced in CI**") + L2239 ("uploaded as CI artifact and posted as PR
  comment"). `ci.yml` runs `go test -race -cover` and **discards the number** — no fail-under, no
  artifact, no comment. Coverage can fall to 0% and CI stays green. **→ extend T41** (the gate
  owner) + **T42** (the CI wiring) to add a real fail-under + artifact + PR comment. **(D4)**
- **TV-07 (Med) — Go 1.24 not in the CI matrix.** §9 L2431-2433 requires `-race` to pass on
  "every supported Go minor … currently 1.23 and 1.24". CI runs **only `go-version: "1.23"`**.
  **→ extend T42** (its scope already names "Go matrix (1.23, 1.24)", plan.md L812 — the workflow
  simply does not implement it yet). Confirm/do-not-regress.
- **TV-13 (Med) — integration lane skips green without a broker.** Tests `t.Skip` when
  `AMQP_TEST_URL` is unset (correct for local/fork dev), but that makes "integration suite
  passes" (§9 L2437) **satisfiable by asserting nothing** on any runner that forgets to set the
  URL. The *required* CI lane sets it — but there is no guard that *fails* if the broker is
  unexpectedly absent in the required lane. **→ extend T42** (or fold into T151): add a sentinel
  that fails the required integration job if zero integration tests actually ran. **(D2)**

### WS-2 — Conformance: stub vs real broker *(the strategy can't prove what it claims)*

- **TV-06 (Med) — the conformance suite does not exist and the stated stub strategy cannot prove
  the broker-bound contracts.** §7 L2256-2262 promises a `conformance` lane (every commit,
  separate CI job) whose "protocol-level checks use a test AMQP server stub". No
  `//go:build conformance` files exist; no stub exists; no conformance CI job exists (owned by T44,
  op-decision: live broker for v0.1, stub deferred). The deeper finding is **strategic**: a stub
  **cannot** prove return/ack ordering (R10-3), the quorum delivery-limit (R10-2), `x-death`
  reason tokens, `x-death` cycle-detection, `prefetch_size` no-op-vs-error, direct-reply-to
  constraints, 406-on-`UserID`-divergence, or `reject-publish-dlx` dual behaviour — these are
  **real-broker-only**. **→ extend T44**: add the **stub-vs-real-broker contract matrix** (§12
  below) as a §7 artifact and **drop the "test AMQP server stub" language** for v0.1 (real-broker
  only). **(D5)**
- **TV-08 (Med) — delayed-exchange criteria silently skip when the plugin is absent.** §9 L2435
  ("Conformance suite passes (… delayed-message exchanges …)") and the `examples/delayed/`
  smoke-run depend on the `rabbitmq_delayed_message_exchange` plugin. Decision 40 / T37 give three
  modes, one of which is `amqptest.RequireDelayedExchange(t)` → **`t.Skip`**. The current
  docker-compose/CI image has **no plugin**, so every delayed criterion **skips green** in the
  required lane. **→ extend T37/T44**: guarantee the plugin is present in **≥1 required lane** (a
  pre-baked image), or mark the criterion **conditionally verified** in §9 and fail (not skip) if
  the plugin is expected but missing.

### WS-3 — Probabilistic & version-bound contracts *(load+repeat, real broker)*

- **TV-02 (Med) — R10-3 needs a load+repeat real-broker test and the spec must call it
  flaky-prone.** The return/ack ordering invariant is a ~50%-under-load race against
  `amqp091-go`'s two-channel notify dispatch; the current lock is a **mock-tracker** test
  (`internal/confirms`) plus one integration assertion. A mock cannot reproduce the dispatch
  timing → it can pass while the real invariant is broken. **→ extend T59** (already gets a
  real-broker assertion + `amqp091-go` pin from Lens-01 RMQ-16): require the real-broker test to
  run **many iterations** under **concurrent unroutable-mandatory publishes during confirm load**
  to actually trip the race, and add a §7 note that this contract is **flaky-prone by design** and
  belongs on the nightly trigger, not a single PR run.
- **TV-11 (Med) — quorum poison-loop must assert a *bound*, not an exact count, and run on 4.x.**
  §9 L2453-2456 ("at most `DeliveryLimit + 1` deliveries") is correctly phrased as a bound — good,
  because the §6.3 broker cycle-detection caveat makes the count **non-deterministic on cyclic
  topologies**. But the *test* must (a) assert the bound (not `==`, or it flakes) and (b) run
  against a **real 4.x quorum queue** (TV-05 matrix). The classic counter-B path is exhaustively
  *unit*-tested (21 tests) but the quorum `x-delivery-limit` drop has only topology-injection unit
  coverage. **→ extend T81/T44** (poison-loop integration on 4.x via the matrix).

### WS-4 — Vanity & under-specified criteria *(make checkable or reclassify honestly)*

- **TV-04 (Med) — throughput numbers are author-laptop, not CI-gateable.** §9 L2472-2479
  (`≥30k msg/s on a single Apple M-series laptop`, `≥100k with 4 conns`, `batch ≥5×`). CI runners
  are not M-series, RTT differs, neighbours contend → these can **never gate CI**; even the planned
  T44b runs only on release tags. They are either never enforced (vanity) or flaky.
  **→ extend T44b** (already carries Lens-09 PC-03/04/05/14 methodology): add a **CI-gradeable
  relative regression gate** (fail if > X% slower than the last release baseline on the same
  runner class) as the *checked* criterion, keep the absolute numbers as **release-tag targets on
  stated reference hardware**, and have the §9 capstone (T149/T152) classify each accordingly.
  Coordinate with Lens-09. **(D1)**
- **TV-09 (High) — chaos "zero message loss" has no specified counting method.** §9 L2446-2448 is
  falsifiable *only if* "loss" is defined. The harness (T45) must count loss as **published-set −
  consumed-set, deduplicated by `MessageID`** (UUIDv7 auto-populated), explicitly tolerating
  at-least-once **duplicates** (which `PublishRetry`/the reconnect barrier produce by design) so it
  does not false-positive on dupes nor pass while truly losing. **→ extend T45**: specify the
  loss-counting method in §7 + the test, and require VG-6's injected-drop self-test (the harness
  must report loss == 1 for one deliberate drop). This is the lens's highest-leverage finding
  because it guards the §1 headline guarantee.
- **TV-14 (Med) — the security credential grep must cover wrapped `amqp091-go` errors and exercise
  enough surface.** §9 L2482-2485 + T45b scan recorded outputs for a clear-text password. The most
  likely leak (per Lens-07) is a **wrapped underlying `amqp091-go` error** that embeds the dial
  URL, *not* a warren-own string — and a grep only catches what was *exercised*. **→ extend T45b**:
  the 60s run must **force** an auth failure / a broker error containing a credentialed URI so the
  wrapped-error path is exercised, and the grep must scan errors/spans/metric-labels, not just log
  lines. Coordinate with Lens-07.

### WS-5 — §7/§9 honesty rewrite & residual fuzz *(capstone + LATER)*

- **TV-10 (Low, high-leverage) — §7 prose contradicts §9.** §7 chaos says "60s outage at 1k
  msg/s" (L2245) while §9 says "5-minute outage at 10k msg/s" (L2447); §7 memory says "Nightly
  100x stress loop" (L2268) while §9 says "1000 connect/disconnect cycles" (L2457). Lens-08
  (CR-10/T45) reconciled the **ledger** to 1000 but the **SPEC §7 text still says 100x and
  60s @ 1k**. §3/§7 also claim integration "spins up RabbitMQ via testcontainers-go" (L139) and
  examples smoke-run against `amqptest.NewRabbitMQ(t)` (L2317) — the real harness is
  docker-compose + `AMQP_TEST_URL` until T37 lands. **→ extend T45** (the 60s @ 1k → 5-min @ 10k
  + 100x → 1000 §7-text fix) + **capstone T152** (the testcontainers/amqptest reality note).
- **TV-12 (Low, high-leverage) — §7 must state coverage % is a floor, not a correctness proof,
  and cross-link the concurrency criteria.** The 95% floor on `internal/confirms` /
  `internal/reconnect` (§9 L2491-2493) says nothing about the **interleavings** those packages
  exist to get right — the bugs are in timing, not lines. Lens-08 already added the
  race/ordering/goroutine-inventory criteria (T143/T146). **→ capstone T152**: add one sentence to
  §7 Coverage that the floor is necessary-not-sufficient for these packages and cross-link the
  §9 concurrency criteria. **Confirm** (do-not-regress) T143/T146.
- **TV-15 (Low) — the dedupe-window recommendation (§6.2.1) is unverifiable by construction.** The
  "15 minutes of MessageID retention is sufficient" claim (L2056-2059) describes a failure mode
  (redelivery hours later → reprocessed) that **no test can exercise**. **→ capstone T152**: label
  it explicitly as an **operator-validated recommendation, not a library-tested guarantee**, in
  §6.2.1 and in the §9 classification — honesty, not a new test.
- **LATER-46 (deferred, Low) — residual fuzz gaps.** `internal/amqperror` keys on the **numeric**
  reply code (`*amqp091.Error.Code`, a `uint16`), not free-form bytes, so a `FuzzAMQPCode` is
  **low value** (no byte-parser to break) — note this honestly rather than implying a gap. The
  one genuine residual is the **otel propagation-header extraction** (`internal/headers`), which
  reads producer-controlled header *values*; a `FuzzHeadersExtract` would close it. Defer both to
  LATER-46 (the existing 6 fuzz targets cover the real attacker-influenced byte-parsers:
  `FuzzRedactURI`, `FuzzCodecJSON`/`Strict`, `FuzzCodecProtobuf`, `FuzzCodecCloudEventsBinary`,
  `FuzzXDeathParser`).

---

## 6. Do-not-regress list (confirmed-correct — protect, do not re-file)

The strategy is strong where it counts; these must survive the remediation:

- **631 unit tests, `-race` on every lane, 212 `goleak.VerifyNone` assertions** + 2
  `VerifyTestMain` (`internal/reconnect`, `internal/confirms`). The goroutine-leak net is real.
- **6 fuzz targets** on every attacker-influenced byte-parser (redaction, JSON lax+strict,
  Protobuf, CloudEvents binary, x-death). Keep them in the unit lane *and* add the nightly
  10m-budget runner (TV-01) — do not move them off the fast lane.
- **The R10-3 ordering invariant is already locked** by T59 + Lens-01 RMQ-16 (real-broker
  assertion, `amqp091-go` pinned, single-reader comment). TV-02 *adds* load+repeat, it does not
  replace.
- **The Rev-10 failure modes are already tested in v0.1** — T61 (channel-only close, R10-6), T63
  (half-alive-broker proxy test, R10-8), T67 (partial-pool boot, R10-12). The prompt's anticipated
  "deferred, untested" gap is **resolved**; the only residual is ensuring the proxy harness runs in
  a real lane (coordinate, no re-file).
- **The polyglot interop lane is already owned** by T94 + T37 (Lens-03 IW-13: `pika` + Java +
  AMQP-1.0). Every §9 cross-language criterion **gates on it**; this lens adds no interop task.
- **The concurrency interleaving criteria are already owned** by T143/T146 (Lens-08). TV-12 only
  cross-links them from §7.
- **The poison-loop classic path** (counter A x-death short-circuit + counter B in-process map,
  quorum carve-out) is **exhaustively unit-tested** (21 tests, including the atomic-RMW
  behavioural test from Lens-08 CR-02/T20). TV-11 only adds the *real-broker quorum* assertion.
- **Examples smoke-runs assert outcomes**, not just "ran without panic" (payload fields, `x-death`
  presence, output strings). Keep that — TV-13's guard protects it from skipping green.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Class | Disposition | Owner |
| --- | --- | --- | --- | --- |
| TV-01 no nightly/scheduled workflow | High | untested (unrun) | **new** | **T151** + cite T45/T45b/fuzz |
| TV-02 R10-3 load+repeat + flaky honesty | Med | wrong-reason / flaky-by-design | extend | **T59** (+Lens-01 RMQ-16) |
| TV-03 coverage floor not enforced | Med | unfalsifiable (vanity) | extend | **T41** + **T42** |
| TV-04 throughput author-laptop, not CI-gateable | Med | unfalsifiable (vanity) | extend | **T44b** (+T149; coord Lens-09) |
| TV-05 no 3.13/4.x matrix | High | untested (version-bound) | **new** + extend | **T151** + **T81** |
| TV-06 conformance stub can't prove broker contracts | Med | wrong-reason (stub) | extend | **T44** |
| TV-07 Go 1.24 not in CI | Med | untested | extend | **T42** |
| TV-08 delayed-exchange criteria skip green | Med | untested (skips) | extend | **T37** + **T44** |
| TV-09 chaos loss-counting unspecified | High | wrong-reason (can pass while losing) | extend | **T45** |
| TV-10 §7 prose vs §9 (60s@1k/100x/testcontainers) | Low | inconsistency | extend | **T45** + **T152** |
| TV-11 quorum poison-loop bound + 4.x | Med | untested (version-bound) | extend | **T81** + **T44** |
| TV-12 coverage% ≠ correctness; cross-link concurrency | Low | wording | capstone | **T152** (confirm T143/T146) |
| TV-13 integration skips green without broker | Med | unfalsifiable (skips) | extend | **T42** / **T151** |
| TV-14 security grep wrapped errors + breadth | Med | wrong-reason (narrow exercise) | extend | **T45b** (coord Lens-07) |
| TV-15 dedupe-window unverifiable by construction | Low | unfalsifiable (honest reclassify) | capstone | **T152** |
| LATER-46 residual fuzz (headers extract) | Low | deferred | LATER | LATER-46 (prereq T44) |

**New:** T150 (gates VG-1..VG-6, Rev 20), T151 (nightly workflow + 3.13/4.x matrix + skip-guard),
T152 (§7/§9/§6.2.1 verifiability capstone). **Extended in place:** T41, T42, T44, T44b, T45,
T45b, T59, T81, T37. **Confirmed (do-not-regress):** T94 (polyglot), T61/T63/T67 (Rev-10 modes),
T143/T146 (concurrency criteria).

---

## 8. Suggested phasing (planner may revise — "Phase 21")

**Phase 21 — Test-Strategy & Verifiability Re-review (Lens 10).** Highest existing task = **T149**
(Phase 20) → new IDs **T150–T152**. Highest filed `LATER` = **LATER-45** → **LATER-46**. SPEC §10
Rev recorded when T150 lands = **Rev 20** (Rev = Phase − 1). Proposed tally after materialization:
**161 tasks / 21 phases**.

- **T150 — Verification gates VG-1..VG-6** (unit + the existing `integration` lane + a throwaway
  4.x broker for VG-5; **no behaviour change**). Capture ground truth (gate-first); fill the §11
  "CI-gateable?" column with *measured* facts; records §10 **Rev 20**. **[P0] · S**
- **T151 — CI verification infrastructure** (net-new): a `schedule:`-triggered workflow running
  the fuzz 10m-budget + the 1000-cycle stress + the chaos 5-min @ 10k × 50-run flaky harness with
  a flaky-rate report; a **RabbitMQ 3.13 + 4.x matrix** axis on the `integration` lane; an
  **integration-broker-required guard** (fail the required job if zero integration tests ran).
  Coordinates with T45/T45b (the jobs it schedules), T41/T42 (the lanes), T37 (broker images).
  **[P1] · M** **(D2, D3)**
- **T152 — §7/§9/§6.2.1 verifiability capstone** (SPEC wording, lands last): classify **every**
  §9 criterion as `CI-gate | nightly | release-only | operator-validated | polyglot-lane`; add the
  §7 **conformance stub-vs-real-broker contract matrix** (§12) and drop the stub language; reconcile
  §7 prose (testcontainers/amqptest reality, the residual 60s@1k/100x text not caught by T45); add
  the §7 "coverage % is a floor, not correctness" sentence cross-linking the concurrency criteria
  (TV-12); label the §6.2.1 dedupe-window an operator-validated recommendation (TV-15); scope the
  cross-language criteria to the T94 polyglot lane. Asserts the WS-1/WS-2 lanes landed. **[P2] · S**

**Cross-lens extensions (acceptance bullets, not new tasks):** T41 (TV-03), T42 (TV-03/07/13),
T44 (TV-06/08/11), T44b (TV-04), T45 (TV-09/10), T45b (TV-14), T59 (TV-02), T81 (TV-05/11),
T37 (TV-08). **Confirmations:** T94, T61/T63/T67, T143/T146.

**Sequencing (A–E):**
- **A** Gates — T150 (records Rev 20; fills §11 with measured CI-reality).
- **B** Infrastructure — T151 (nightly trigger + matrix + skip-guard) so the unrun criteria gain a
  runner; T42/T41 (conformance lane + Go 1.24 + coverage fail-under).
- **C** Make-checkable — T44b (relative regression gate, TV-04), T45 (loss-counting + VG-6 self-test,
  TV-09), T45b (wrapped-error breadth, TV-14).
- **D** Version-bound & real-broker — T81/T44 (4.x matrix verifies quorum limit + poison-loop bound,
  TV-05/11), T44 (stub-vs-real matrix, TV-06), T37/T44 (delayed-plugin guarantee, TV-08), T59
  (load+repeat R10-3, TV-02).
- **E** Honesty capstone — T152 (§7/§9/§6.2.1 classification + reconcile), lands last; asserts B–D.

Gate deps: VG-1→TV-01/04/05/06/07/08; VG-2→TV-03; VG-3→TV-13; VG-4→TV-02(T59); VG-5→TV-05(T81);
VG-6→TV-09(T45). Gate-independent (doc/wording): TV-10, TV-12, TV-15 (capstone).

---

## 9. Acceptance criteria for the whole effort

- [ ] **Gates first (TV gates):** T150 lands before any §7/§9 wording or workflow edit; the §11
      table's "CI-gateable?" column is filled from *measured* CI reality; §10 **Rev 20** recorded.
- [ ] **Nightly runner (TV-01):** a `schedule:`-triggered workflow runs the fuzz 10m-budget, the
      1000-cycle stress, and the chaos 5-min @ 10k × 50-run harness, and reports the flaky-rate;
      §9 names which criteria run there.
- [ ] **Version matrix (TV-05/11):** the `integration` lane runs **both** `rabbitmq:3.13` and
      `rabbitmq:4`; the quorum default `x-delivery-limit` drop and the poison-loop **bound** are
      asserted on 4.x; T81's version-divergence docs are verified by it.
- [ ] **Coverage enforced (TV-03):** CI **fails** below 80% per package / 95% on the critical-path
      packages, uploads the profile as an artifact, and posts the PR comment §7 promises.
- [ ] **Go 1.24 (TV-07):** CI runs `-race` on both 1.23 and 1.24.
- [ ] **No green-by-skipping (TV-13):** the required integration job fails if zero integration
      tests ran.
- [ ] **Conformance honesty (TV-06/08):** the §7 stub-vs-real-broker matrix exists; the "test AMQP
      server stub" language is dropped for v0.1; delayed-exchange criteria either run against a
      plugin-enabled image in a required lane or are marked conditionally verified (and fail, not
      skip, when the plugin is expected but missing).
- [ ] **Throughput checkable (TV-04):** §9 carries a CI-gradeable **relative regression gate**;
      the absolute `30k/100k/5×` numbers are reclassified as release-tag targets on stated
      reference hardware (coord Lens-09 T149).
- [ ] **Chaos can detect loss (TV-09):** §7 + T45 define loss as published − consumed (deduped by
      `MessageID`, tolerating duplicates); the harness reports loss == 1 for one injected drop
      (VG-6).
- [ ] **R10-3 honesty (TV-02):** T59's real-broker test runs many iterations under concurrent
      unroutable-mandatory load; §7 notes the contract is flaky-prone and belongs on the nightly
      trigger.
- [ ] **Security breadth (TV-14):** the 60s credentialed run forces a wrapped-`amqp091-go` error
      path; the grep scans logs, errors, spans, and metric labels.
- [ ] **§7/§9 reconciled (TV-10/12/15):** §7 prose matches §9 (5-min @ 10k, 1000 cycles,
      docker-compose/`amqptest` reality); §7 states coverage % is a floor not a correctness proof
      and cross-links the concurrency criteria; §6.2.1 labels the dedupe window operator-validated.
- [ ] **Every §9 criterion is classified** (`CI-gate | nightly | release-only |
      operator-validated | polyglot-lane`) so the spec never implies a check that does not exist.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; the new
      nightly/matrix/conformance lanes green on their cadence.
- [ ] **Ledger integrity:** nine tasks extended in place (T37/T41/T42/T44/T44b/T45/T45b/T59/T81),
      three confirmations add no task, exactly one new `LATER.md` entry (LATER-46), T150–T152
      contiguous with no duplicate IDs.

---

## 10. Open questions for the owner (decisions the planner needs to record)

- **D1 — Throughput criteria disposition (TV-04).** Recommended: **keep** the absolute `30k/100k/5×`
  numbers as **release-tag targets on stated reference hardware** (M-series, named broker config),
  **and** add a **relative regression gate** (fail if > X% slower than the last release baseline on
  the same runner class) as the CI-gradeable §9 criterion. Alternatives: pin a self-hosted M-series
  runner (cost/ops burden) or drop the numbers from §9 entirely (loses the documented target).
  *Recommend: relative-gate-in-CI + absolute-as-release-target.* (Coordinate with Lens-09 T149.)
- **D2 — Nightly/scheduled workflow (TV-01/13).** Recommended: **add a `schedule:` workflow**
  running fuzz (10m/target) + 1000-cycle stress + chaos (5-min @ 10k, 50-run flaky < 1%) +
  flaky-rate report, plus the integration-broker-required guard. Alternative: document these as
  **release-candidate-only manual gates** (cheaper, but then §7/§9 must drop "nightly").
  *Recommend: add the scheduled workflow — it is the only honest way to claim "nightly".*
- **D3 — RabbitMQ version matrix (TV-05/11).** Recommended: **both** `3.13` LTS **and** `4.x` on the
  integration lane (≈ 2× lane time). Alternatives: single 4.x (newest, but un-tests the LTS most
  estates run) or single 3.13 (un-tests the 4.x quorum behaviour the spec asserts).
  *Recommend: both; put the heavier of the two on the nightly trigger if PR-lane time is a concern.*
- **D4 — Coverage-floor enforcement (TV-03).** Recommended: **hard fail-under** at 80%/95% +
  artifact + PR comment. Alternative: report-only (matches today, but then §7/§9 must drop
  "enforced in CI"). *Recommend: hard fail-under — the spec already promises it.*
- **D5 — Conformance harness (TV-06).** Recommended: **real-broker-only for v0.1** (matches T44's
  op-decision) and **remove the "test AMQP server stub" language from §7** (a stub cannot prove the
  broker-bound contracts in §12). Defer any stub to v0.2. Alternative: build a stub now (large,
  and it still cannot prove the load-bearing contracts). *Recommend: real-broker-only + drop the
  stub claim.*

---

## 11. Success-criteria verifiability table (lens output #1)

Every §9 checkbox classified. **CI-gateable?** = runs deterministically in the *current* CI
(`unit` + `integration`, Go 1.23, `rabbitmq:3-management`). **Could pass while broken?** flags a
wrong-reason/skip risk.

| §9 criterion (abridged) | Falsifiable? | Test level | CI-gateable now? | Could pass while broken? | Finding |
| --- | --- | --- | --- | --- | --- |
| Every API compiles / godoc / ≥1 test (L2429) | Yes | unit | **Yes** | No | — |
| `-race` on Go 1.23 **and 1.24** (L2431) | Yes | unit | **Partly** (1.23 only) | Yes (1.24 unrun) | **TV-07** |
| Integration passes on **3.13 + 4.x** (L2434) | Yes | integration | **No** (single image) | Yes (4.x unrun) | **TV-05** |
| Conformance suite passes (incl. delayed) (L2435) | Yes | conformance | **No** (lane absent) | Yes (skips/absent) | **TV-06/08** |
| All examples build + run e2e (L2437) | Yes | unit+integration | **Yes** (asserts outcomes) | No | — (TV-13 guard) |
| API `// Deprecated`-free rc1→v1.0 (L2439) | Yes | grep | **No** (unchecked) | Yes | minor → T152 |
| README quickstart + links every example (L2442) | Weak | link-check | **No** | Yes | minor → T152 |
| Chaos zero-loss 5-min @ 10k, nightly, flaky <1%/50 (L2446) | Yes | integration (nightly) | **No** (no nightly; loss undefined) | **Yes** | **TV-01/09** |
| Poison-loop classic `MaxRedeliveries+1` (L2449) | Yes | unit + integration | **Partly** (unit only) | Low | — (real-broker → T44) |
| Poison-loop quorum `DeliveryLimit+1`, x-death reason (L2453) | Yes (bound) | integration 4.x | **No** (no 4.x) | Yes | **TV-05/11** |
| 1000 connect/disconnect → 0 leaked goroutines (L2457) | Yes | integration (nightly) | **No** (no nightly) | Yes (unrun) | **TV-01** |
| Reconnect → resubscribe + qos, metric once (L2459) | Yes | integration | **Yes** | No | — |
| Channel close cancels handler ctx + metric (L2463) | Yes | unit + integration | **Yes** | No | — (Lens-08 T60/T61) |
| Broker `basic.nack` (x-overflow) → `ErrPublishNacked` + transient (L2466) | Yes | integration | **Partly** | Med | → T44 |
| `BenchmarkPublishConfirmed ≥30k` (M-series) (L2472) | Yes | bench (laptop) | **No** | n/a (vanity) | **TV-04** |
| `≥100k` with 4 conns + pool 16 (L2475) | Yes | bench (laptop) | **No** | n/a (vanity) | **TV-04** |
| `BenchmarkPublishBatch ≥5×` (L2478) | Yes | bench (laptop) | **No** | n/a (vanity) | **TV-04** (+Lens-09 PC-05) |
| No clear-text password in any output (60s run) (L2482) | Yes | integration | **Partly** | **Yes** (wrapped errors / narrow exercise) | **TV-14** |
| SASL EXTERNAL + client cert authenticates (L2486) | Yes | integration (TLS broker) | **No** (no TLS/ssl-auth in CI) | Yes | → T34b/T37 |
| Coverage ≥80%/≥95% (L2491) | Yes | CI | **No** (not enforced) | **Yes** | **TV-03** |
| No "tracing disabled" branch (L2494) | Yes | unit (alloc) | **Yes** | No | — (Lens-09 PG-1) |
| CloudEvents both modes round-trip (L2496) | Yes (Go) | unit + polyglot | **Partly** (Go-only) | Yes (cross-lang) | confirm **T94** |
| Reply codes → §6.8 sentinels + `AMQPCode` (L2504) | Yes | unit | **Yes** | No | — (fuzz → LATER-46) |
| ContentType/Encoding not swapped, via mgmt API (L2507) | Yes | integration | **Partly** | Med | → T81 (mgmt API, not `rabbitmqadmin`) |
| `ConfirmTimeout` default 30s (mock unit) (L2513) | Yes | unit | **Yes** | No | — |
| Reconnect barrier `ErrReconnecting` + no 404 (L2517) | Yes | integration | **Yes** | No | — (T63 adversarial) |
| Topology degraded state + metric + callback once (L2523) | Yes | unit + integration | **Yes** | No | — |
| `PublishRetry` duplicates + metric + example (L2528) | Yes | unit + example | **Yes** | No | — |
| Consumer-tag default uniqueness, even pinning (L2532) | Yes | unit | **Yes** | Low | — |
| `HandlerTimeout` default verdict (L2536) | Yes | unit + integration | **Yes** | No | — |
| `basic.cancel` → `OnCancel` + `ErrConsumerCancelled` (L2540) | Yes | integration | **Partly** | Med | → T44 |
| SASL EXTERNAL fail-closed `Dial` validation (L2543) | Yes | unit | **Yes** | No | — (T34b) |
| `UserID` client-side validation (L2546) | Yes | unit | **Yes** | No | — |

**Summary:** of the 34 criteria, ~19 are genuinely CI-gateable today; ~8 are *structurally unrun*
(nightly/conformance/bench/4.x/1.24/TLS) and ~3 *could pass while broken* (chaos loss-counting,
coverage floor, security grep). The remediation moves the unrun set onto a named cadence and
makes the can-pass-while-broken set fail for the right reason.

---

## 12. Load-bearing-contract coverage table — stub vs real broker (lens output #2)

The §7 conformance strategy says "protocol-level checks use a test AMQP server stub" (L2261). This
table is the evidence that the load-bearing contracts are **real-broker-only** — the §7 artifact
TV-06 asks T44 to adopt.

| Contract (§ref) | Needs real broker? | Stub-coverable? | Tested at | Status |
| --- | --- | --- | --- | --- |
| R10-3 return/ack ordering (§6.2) | **Yes** (amqp091 2-channel notify timing) | No | mock tracker + 1 integration | **load+repeat needed** → TV-02/T59 |
| R10-2 quorum default `x-delivery-limit`=20 (§6.3/§6.6) | **Yes** (4.x) | No | topology-injection unit | **untested on 4.x** → TV-05/11 |
| Classic-queue `x-delivery-limit` ignore (§6.3) | **Yes** (version-divergent) | No | — | **untested** → TV-05 |
| `x-death` reason tokens (§6.3) | **Yes** | No | unit parse + integration DLX | partial → T44 |
| `x-death` cycle-detection non-determinism (§6.3) | **Yes** | No | — | bound-assert honesty → TV-11 |
| Broker `basic.nack` via x-overflow (§6.8/§9) | **Yes** | No | — | asserted-only → T44 |
| `basic.cancel` → `OnCancel`/`ErrConsumerCancelled` (§6.3) | **Yes** | No | — | partial → T44 |
| 406 on `UserID` divergence (broker-side) (§6.5) | **Yes** | No | client-side unit only | broker path → T44 |
| `reject-publish` vs `reject-publish-dlx` dual behaviour (§6.6) | **Yes** | No | — | → T44 |
| direct-reply-to pseudo-queue constraints (§6.7) | **Yes** | No | — | → T44 |
| delayed-message exchange delivery (§6.6) | **Yes** (plugin) | No | validation unit only | skips green → TV-08 |
| CloudEvents binary cross-language fidelity (§6.9) | **Yes** (non-Go client) | No | Go round-trip | polyglot lane → **T94** |
| Field-table unsigned/Decimal/`[]byte` type fidelity (§6.5) | **Yes** (non-Go client) | No | Go round-trip | polyglot lane → **T94** |
| `time.Time`→`T` second-resolution truncation (§6.5) | **Yes** (non-Go client) | No | Go round-trip | polyglot lane → **T94** |
| Frame / content-header encoding (§6.5) | No | **Yes** | unit + fuzz | OK (stub-legit, but Go handles it) |
| Reconnect barrier / degraded mode (§6.1) | **Yes** (broker restart) | No | unit + integration | OK (T07/T63) |
| TLS / SASL EXTERNAL (§6.1) | **Yes** (TLS broker) | No | unit (fail-closed) + planned integration | needs amqptest TLS → T34b/T37 |

**Conclusion:** essentially every load-bearing contract is **real-broker-only**; the only
stub-legitimate row is frame/content-header encoding, which Go already round-trips and fuzzes. The
§7 "stub for protocol-level checks" promise should be **dropped for v0.1** (TV-06/D5) — it implies
coverage a stub cannot deliver.

---

## 13. Findings table (lens output #3)

| ID | Sev | Classification | Location (§ + lines) | Why the current strategy can't prove it | Recommended amendment | Owner |
| --- | --- | --- | --- | --- | --- | --- |
| **TV-01** | High | untested (unrun) | §7 L2268 / §9 L2448 / §3 L148; `ci.yml` (no `schedule:`) | Every "nightly" criterion (fuzz budget, 1000-cycle stress, chaos 50-run) has no runner | Add a `schedule:` workflow running them + flaky-rate report | **T151** (new) |
| **TV-02** | Med | wrong-reason / flaky | §6.2 R10-3; T59 | A mock tracker can't reproduce amqp091's 2-channel notify timing → green proves nothing | Real-broker load+repeat under concurrent unroutable-mandatory load; §7 flaky-prone note | **T59** |
| **TV-03** | Med | unfalsifiable (vanity) | §7 L2235/L2239; §9 L2491; `ci.yml` | `-cover` runs but nothing fails below the floor; no artifact/comment | Hard fail-under + artifact + PR comment | **T41/T42** |
| **TV-04** | Med | unfalsifiable (vanity) | §9 L2472-2479 | Author-laptop numbers can't gate CI (HW/RTT/neighbours) | Relative regression gate in CI; absolutes as release-tag targets on reference HW | **T44b** (coord Lens-09 T149) |
| **TV-05** | High | untested (version-bound) | §7 L2230; §9 L2434; `ci.yml`/compose (single image) | Version-divergent contracts unverified on 4.x (and 3.13 LTS) | 3.13 + 4.x matrix on the integration lane | **T151 + T81** |
| **TV-06** | Med | wrong-reason (stub) | §7 L2256-2262 | A stub can't prove the real-broker contracts in §12; suite/lane absent | Add the stub-vs-real matrix; drop the stub language (real-broker v0.1) | **T44** |
| **TV-07** | Med | untested | §9 L2431; `ci.yml` (1.23 only) | Go 1.24 row never runs | Add 1.24 to the CI Go matrix | **T42** |
| **TV-08** | Med | untested (skips) | §9 L2435; compose (no plugin) | Delayed criteria `t.Skip` when the plugin is absent → skip green | Plugin-enabled image in ≥1 required lane, or mark conditional + fail-not-skip | **T37/T44** |
| **TV-09** | High | wrong-reason (can pass while losing) | §9 L2446-2448 | "Zero message loss" undefined → harness can pass while losing | Define loss = published − consumed (deduped by MessageID); VG-6 injected-drop self-test | **T45** |
| **TV-10** | Low | inconsistency | §7 L2245/L2268; §3 L139; §7 L2317 | §7 prose says 60s@1k / 100x / testcontainers; §9 says 5-min@10k / 1000; harness is compose+`AMQP_TEST_URL` | Reconcile §7 text to §9 + the real harness | **T45 + T152** |
| **TV-11** | Med | untested (version-bound) | §9 L2453-2456; §6.3 | Quorum drop + x-death reason need a real 4.x quorum queue; count is non-deterministic on cyclic topologies | Assert the **bound** on 4.x via the matrix | **T81/T44** |
| **TV-12** | Low | wording | §7 L2233-2239; §9 L2491 | High line-coverage on confirms/reconnect says nothing about interleavings | State "floor ≠ correctness" + cross-link the concurrency criteria | **T152** (confirm T143/T146) |
| **TV-13** | Med | unfalsifiable (skips) | §9 L2437; integration helpers (`t.Skip`) | "Integration suite passes" satisfiable by skipping all when `AMQP_TEST_URL` unset | Required-job guard: fail if zero integration tests ran | **T42/T151** |
| **TV-14** | Med | wrong-reason (narrow) | §9 L2482-2485; T45b | Grep only catches exercised output; the likely leak is a wrapped amqp091 error | Force a wrapped-error path; scan errors/spans/labels too | **T45b** (coord Lens-07) |
| **TV-15** | Low | unfalsifiable (honest reclassify) | §6.2.1 L2056-2059 | "15-min retention sufficient" describes an hours-later failure no test can exercise | Label it operator-validated, not library-tested | **T152** |

---

## 14. Out of scope for this plan

- **SPEC.md / code / README / workflow edits** — they land per-task during execution, same PR
  ("amend SPEC first"), **not** in this `/spec` step or the `/plan` materialization.
- **Building the conformance stub** — explicitly *removed* for v0.1 (D5); a stub cannot prove the
  §12 contracts. Any stub is a v0.2 consideration.
- **Building the polyglot interop lane** — already owned by **T94 + T37** (Lens-03 IW-13). This
  lens only routes the §9 cross-language criteria to gate on it.
- **The R10-3 / Rev-10 / concurrency *implementations*** — owned by T59 / T61/T63/T67 /
  T143-T146. This lens adds verification dimensions and honesty notes, not new mechanisms.
- **The throughput *methodology*** (payload size, queue type, RTT model, `sendM` knee) — owned by
  Lens-09 (T44b/T83/T116/T149). This lens adds only the *CI-gateability* reframe (TV-04).
- **The deeper fuzz target** (`FuzzHeadersExtract`) and the (low-value) `amqperror` fuzz —
  **LATER-46**.
- **Other lenses** (11 compliance/GDPR-LGPD, 12 DX/documentation, 13 load-testing).
- **Committing** — a separate, explicit user request (the two-commit-per-lens convention would
  later add `docs: add test-strategy-verifiability lens remediation plan-brief for /plan` for this
  brief + `docs(tasks): add Phase 21 test-strategy-verifiability re-review (Lens 10)` for the
  ledger).

---

## 15. Verdict for this lens

**GO-WITH-CHANGES — no Blocker.**

The code is well tested; the **strategy** is under-instrumented and the **criteria** over-promise.
A CI that runs only `unit` + `integration` (single broker, Go 1.23, no fail-under, no nightly)
leaves roughly a quarter of §9 **structurally unrun** and lets three reliability-relevant criteria
(chaos loss-counting, coverage floor, security grep) **pass while broken**. None of this is a
message-loss bug — the highest-severity finding (TV-09, chaos loss-counting) is a harness-honesty
gap on an *already-owned* test (T45), and the headline contracts (R10-3, polyglot interop, the
Rev-10 failure modes, the quorum limit) are *already owned* with named tests (T59/T94/T61/T63/T67/
T81). The remediation is therefore **infrastructure + honesty**, not a rewrite: add the
**scheduled/nightly runner** and the **3.13/4.x matrix** so the unrun criteria gain a cadence;
**enforce the coverage floor**; reframe the **author-laptop throughput numbers** to a relative
regression gate; **specify the chaos loss-counting** so it cannot pass while losing; and **rewrite
§7/§9** to classify every criterion by *where it actually runs* — turning a spec that *implies*
checks into one whose every line is either gated, scheduled, release-tagged, polyglot-gated, or
honestly labelled operator-validated. **3 net-new tasks (T150–T152), 8 cross-lens extensions, 3
confirmations, LATER-46.**
