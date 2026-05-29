# Plan Input — Remediate Go API & Library-Design Findings (Lens 06)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the Go API &
> library-design lens (`spec-validation/06-go-api-library-design.md`). Like Lenses
> 03/04/05, no findings report pre-existed — this brief was produced by *conducting*
> the review: for every exported identifier on the public surface it asks the four
> lens questions — **is it discoverable, is it hard-to-misuse, is it stable across
> the planned v0.1→v1.0→post-v1 evolution, and is its zero value safe?** — and a
> "no" to any one is a finding. It enumerates confirmed findings (`GA-01..GA-16`),
> their evidence (SPEC §+line *and* `file:line`), the desired outcome, and an
> acceptance test for each, grouped into workstreams and sequenced by dependency.
>
> **Numbering:** new task IDs are **T119–T129** (highest existing is T118, after
> Phase 16). Lens 06 becomes **Phase 17**, mirroring how Lenses 01/02/03/04/05
> became Phases 12/13/14/15/16. Five prior-lens / Phase-11 tasks are **extended in
> place** with a `Lens-06` acceptance bullet (cross-lens rule: shared findings
> extend the shared task, never re-filed) — **T37** (test fixtures), **T68/T69**
> (topology evolution), **T70/T71** (observability interfaces), **T112** (metrics
> registry). **One new `LATER.md` entry** (LATER-41: a dedicated `ReturnCode`
> accessor). **No new build-tag lane** (the gates GG-1..GG-4 are unit-level and ride
> the existing unit lane). The first Phase-17 task records SPEC §10 **Rev 16**.
>
> **Lens verdict: GO-WITH-CHANGES.** The public surface is fundamentally sound — the
> `PublisherFor[M]`/`ConsumerFor[M]` generics split is the cleanest practical Go
> expression, the zero-value defaults are *mostly* safe-by-default, the error
> taxonomy is navigable, and the concrete-struct-with-unexported-fields choice
> (decision 9) is forward-compatible. But the review found **one Blocker** — a
> *silent durability defect*: a zero-valued `Message[M]{}` ships **non-persistent**
> on the wire, directly violating the §6.5 "durable-by-default" headline and the §1
> "no silent message loss" bar — plus **one High** silent-observability gap and a
> set of compatibility-hardening and documentation corrections. No API redesign is
> required; the surface is kept, made honest, and pinned additive for the roadmap.

---

## 1. Objective

Validate `SPEC.md` from the seat of the **library consumer who never reads the
docs**: can they fall into the pit of success? The lens bar is concrete — *a
library's API is its most permanent decision; a wire format can be versioned, but
an exported signature is forever once people depend on it.* So every exported
identifier must be discoverable, hard-to-misuse, stable across the planned
evolution, and have a zero value that is either correct-by-default or
loudly-invalid (**never silently-wrong**).

Concretely, the plan must:
1. **Stop the silent durability loss (GA-01, Blocker).** The single most important
   reliability guarantee in the library — "a zero-valued `Message[M]{}` is durable"
   (§6.5) — is **false in code**: `buildPublishing` casts `DeliveryMode` raw, so the
   zero value `0` (`DeliveryModePersistent`) goes on the wire as `0` (**transient**),
   not `2` (persistent). This is a silently-wrong zero value at the "billions/day, no
   silent message loss" bar. Fix the translation + add a wire-level regression test.
2. **Stop the silent observability loss (GA-02, High).** Decision 44 promises
   "builder-overrides-connection," but builders never inherit the connection's
   tracer/metrics — `Dial(WithTracer(t))` produces **NoOp spans** on every
   publisher/consumer with no error. Owner decision: **reword the spec to "fully
   independent, no inheritance"** so the contract matches the code and the surprise
   is documented, not silent.
3. **Make the defaults honest (GA-03).** §6.1/§3 say `WithMetrics` defaults to
   Prometheus; the code defaults to **NoOp** (`connection.go:641`). Owner decision:
   **NoOp default is correct** for a library (§8 "no globals"); correct the SPEC,
   keep opt-in Prometheus, and route the registry-injection mechanics to the
   already-filed T112.
4. **Remove the no-op footgun (GA-04).** `PrefetchBytes(uint)` accepts a meaningful
   value, discards it (`_ uint`), and silently does nothing on RabbitMQ. Owner
   decision: **cut it** (join `Immediate()`/`NoLocal()` in the "intentionally not
   exposed" list).
5. **Pin the roadmap additive (GA-05/GA-06/GA-16).** Phase 11 (Rev 10) adds surface
   that can reshape exported types. Owner decision: the exchange→exchange binding
   (R10-14/T69) lands as a **separate `Topology.ExchangeBindings []ExchangeBinding`**
   (Binding untouched). Add a §9 "additive-only after first tag" gate, pin the
   stream-consume v0.2 API additive (decision 24), and give the user-implementable
   `metrics`/`log`/`otel` interfaces an extension-tolerance policy (embeddable NoOp)
   so the planned metric additions (R10-16/T71) don't break external adapters.
6. **Fix the error-model contradictions (GA-07/GA-08/GA-15).** §6.8 lists reply code
   `311` as **both** transient and permanent; the transient/permanent partition is
   partial but undocumented; `ErrTransient`+`ErrPermanent` can both match one error
   with no precedence rule (the spec's own "re-classify 506 as transient" guidance is
   a trap); `AMQPCode` overloads two AMQP frame classes through one int.
7. **Document the deliberate `any`/generics/struct choices (GA-09/GA-10/GA-11/GA-14)**
   so a reviewer reads them as decisions, not gaps: the non-generic `codec.Codec`
   (a payload↔codec mismatch is a runtime `ErrInvalidMessage`), the `Message[M].Body
   *M` rationale (loud-invalid + publish/consume symmetry), the `HeaderCodec`
   type-assertion opt-in (author caveat), and a lighter test-fixture path so consumer
   unit tests don't drag in the gomock-heavy `amqpmock`.
8. **Square the surface with the code (GA-12) and the naming grammar (GA-13).**
   Several §6.1/§6.2 signatures drift from the implementation (`WithOnResubscribe`
   phantom, `WithDialer`/`WithFrameMax`/`WithChannelMax` types, `PublishResult.Index`,
   `Return.Body`, `ReturnedProperties.Expiration`); §5 lacks carve-outs for the
   `WithoutMetrics`-on-builder, `Use*`/`Allow*` verb-prefix, and noun-phrase setters,
   and decision 44's "last-wins" should be scoped to value-carrying options (boolean
   flag-setters are monotonic-set).

This is **remediation of an existing spec + one code-contract fix**, not a new
feature. The only Blocker (GA-01) is a code/spec-contract violation on
already-implemented code; everything else is a documentation correction, a footgun
removal, an additive-compat pin, or a small surface fix.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing** (project convention; the anchors below are this-pass snapshots).
  - §5 Code Style / conventions L260–272 (generics split L264–269; handler
    payload-first L272); §5 Naming L405–414.
  - §6.1 Connection L467; options table L487–513 (`WithMetrics`→"Prometheus" **L511**,
    `WithoutMetrics` L512, `WithTracer`→"NoOp" L513, `WithDialer` L498, `WithFrameMax`
    L497, phantom `WithOnResubscribe` L508); precedence prose L515–523; resubscribe
    callback prose L629.
  - §6.2 Publisher L764; `PublishResult` L776–779; `Return`/`ReturnedProperties`
    L808; `Mandatory()` **L790**; `StampUserID()` L791; `MaxMessageSizeBytes` L796;
    `PublishBatchMaxSize` L931.
  - §6.3 Consumer L1070; `PrefetchBytes` **L1145**; `ChannelQoS` L1146; `Delivery[M]`
    L1085 (rationale L1102–1105).
  - §6.4 BatchConsumer L1370; `Batch[M]` L1383 (rationale L1391).
  - §6.5 Message L1434; `Body *M` **L1435**; durable-by-default + wire-mapping promise
    **L1554–1559**.
  - §6.6 Topology L1561; `Exchange` L1571–1579; `Binding` **L1634–1640**;
    `QueueTypeStream` L1631; stream validation L1714–1721.
  - §6.8 Errors L1863; `AMQPCode` L1940–1944; `IsTransient` L1947–1965 (lists 311 at
    **L1951**); `IsPermanent` L1967–1971 (506 at L1970).
  - §6.9 codec interface L1983; `HeaderCodec` L2021–2042 (type-assertion detection
    **L2033**; CloudEvents built-in L2039–2042); metrics labels L2059–2071; histogram
    buckets L2073–2076.
  - §8 Boundaries — "no globals / no `init()` side effects" L287–288, L2421; "never use
    `any` where a generic would do" **L2420**.
  - §9 Success Criteria — `// Deprecated`-free rc1→v1.0 rule **L2439**; first tag
    `v0.1.0` L2440; throughput targets L2471–2479.
  - §10 decisions — 4 (codec interop) L2573; 9 (concrete structs) **L2607–2610**; 10
    (AMQP compliance / no-op kept / `GlobalQoS`→`ChannelQoS` rename) **L2631–2634**;
    13 (Headers field-table) L2676–2681; 23 (codec panics → `ErrInvalidMessage`)
    L2696; 24 (streams v0.2) **L2780–2783**; 38 (`AMQPCode` covers 312/313)
    **L2895–2900**; 44 (last-wins + builder-overrides-connection) **L2937–2940**;
    Rev 10 **L3031**+ (R10-13 L3127–3129, R10-14 L3130–3132, R10-15 L3133–3136,
    R10-16 L3137–3141).
- Implementation state (read this pass — the anchors that *ground* the findings):
  - **GA-01:** `types.go:14–21` (`DeliveryModePersistent = iota` = 0); `publisher.go:946`
    (`DeliveryMode: uint8(msg.DeliveryMode)` raw cast — **no 0→2 translation**); the
    `basic.return` rehydration path `publisher.go:310`. Confirmed: no wire-translation
    helper exists anywhere.
  - **GA-02:** `connection.go:643–644` (conn defaults tracer→`NoOpTracer{}`);
    builders default to `NoOpTracer`/`NoOp*Metrics` and **never read** `conn.opts.tracer`/
    `conn.opts.metrics` (`publisher_builder.go`/`consumer_builder.go` defaults). The
    connection `ClientMetrics` and builder `Publisher/ConsumerMetrics` are disjoint
    interfaces (`metrics/metrics.go`).
  - **GA-03:** `connection.go:640–641` (`if opts.metrics == nil { opts.metrics =
    metrics.NoOpClientMetrics{} }` — default is **NoOp**, contradicting §6.1 L511);
    `options_connection.go:262` (`WithMetrics` godoc itself says "Defaults to
    NoOp…"). `NewPrometheus*` constructors already accept an injected registerer and
    have **no caller today** (corroborated by T112, `plan.md`).
  - **GA-04:** `consumer_builder.go:69–70` and `batch_consumer_builder.go:95–96`
    (`PrefetchBytes(_ uint) … { return b }` — discards the argument).
  - **GA-05:** `topology.go:61–67` (`Binding{Exchange, Queue, RoutingKey, NoWait,
    Args}` — destination hard-wired to `Queue`; bound via `ch.QueueBind` at
    `topology.go:349`); `Exchange` has `Args map[string]any` (`topology.go:28–36`) so
    alternate-exchange (R10-13) is already additive.
  - **GA-06:** R10-16/T71 adds methods to the user-implementable `metrics.*`
    interfaces; adding a method to an exported interface breaks every external
    implementer. No extension-tolerance policy stated.
  - **GA-07/08:** `errors.go:207–230` (`IsTransient` — 311 *excluded* with a comment),
    `:235–268` (`IsPermanent` — 311/506 included), `:171–195` (`AMQPCode` returns
    312/313 from `*codeError` then channel-close codes from the table), `:270–285`
    (`codeError`). SPEC §6.8 **L1951 lists 311 as transient** — direct contradiction
    with the code and with §6.8 L1970 (311 permanent).
  - **GA-09:** `delivery.go:14–26` / `batch_consumer.go:45–49` (all-unexported fields;
    only fabrication path is `amqpmock.NewDelivery[M]`/`NewBatch[M]`, owned by **T37**,
    which pulls `go.uber.org/mock`).
  - **GA-10/11/14:** `codec/codec.go:18–25` (`Codec` non-generic, `Encode(any)`/
    `Decode([]byte, any)`), `:39–50` (`HeaderCodec`); `publisher.go:600` /
    `consumer.go:809` (type-assertion detection); `message.go:22` (`Body *M`),
    `publisher.go:538` (loud nil-body `ErrInvalidMessage`).
  - **GA-12:** `options_connection.go:101–276` (24 `With*`; `WithDialer` is a dial-func
    `:176`, not `net.Dialer`; `WithFrameMax` `uint32` `:170`; `WithChannelMax` `uint16`
    `:155`; no `WithOnResubscribe`); `types.go` `PublishResult{Err error}` (no `Index`).
  - **GA-13:** `publisher_builder.go:86` (`Mandatory()` set-only), `:168`
    (`StampUserID()`); `WithoutMetrics()` on all three builders is the lone `Without*`
    builder method.
- `tasks/plan.md` / `tasks/todo.md` — Phase 17 lands here (T119–T129 + Lens-06
  acceptance bullets on T37/T68/T69/T70/T71/T112). Highest existing task is **T118**.
- `LATER.md` — highest entry is **LATER-40**; this brief adds **one** (**LATER-41**,
  `ReturnCode` accessor, prereq T126).
- The validation prompt this brief derives from:
  `spec-validation/06-go-api-library-design.md`.

### Cross-lens reconciliation (do **not** re-file — project rule)

Five findings are already owned by a prior-lens / Phase-11 task; this brief routes
each to the existing task with an added `Lens-06` acceptance bullet. **None is
re-filed.**

| Lens-06 finding | Already owned by | Phase | What Lens-06 adds |
|-----------------|------------------|-------|-------------------|
| GA-03 (metrics registry mechanics) | **T112** (`WithMetricsRegisterer` + private per-`Connection` registry, SRE-10) | 16 | the *default* is NoOp (this brief corrects the SPEC table); the opt-in Prometheus path is T112's registry injection — the two compose |
| GA-05 (alternate-exchange additive) | **T68** (R10-13 `x-alternate-exchange`) | 15 | acceptance: expose it **additively** (existing `Exchange.Args` or a new optional field; zero value = today) — no exported-field rename |
| GA-05 (exchange→exchange binding shape) | **T69** (R10-14 exchange-to-exchange bindings) | 15 | **pin the shape**: a new `Topology.ExchangeBindings []ExchangeBinding{Source,Destination,RoutingKey,Args}`; `Binding` is **not** reshaped (no `Source`/`Destination` rename); acceptance asserts no exported `Binding` field is renamed/removed |
| GA-06 (interface evolution breaks external implementers) | **T70/T71** (R10-15/R10-16 add methods to `metrics.*`) | 13 | the new metric/consumer-interface methods must land behind an **embeddable `metrics.NoOp` base** (or equivalent) so external adapters don't break-compile; land before rc1 |
| GA-09 (test-fixture coupling to `amqpmock`/gomock) | **T37** (`amqpmock`/testcontainers helper) | 8 | ship a **lightweight `Delivery[M]`/`Batch[M]` fixture path with no `go.uber.org/mock` dependency** so consumer unit tests aren't forced to import the gomock subpackage |

---

## 3. Constraints & working agreements (planner must honour)

- **Every finding answers the four lens questions** (discoverable / hard-to-misuse /
  stable-across-evolution / safe-zero-value). A worry that fails none is not a finding.
- **A breaking change forced before v1.0 is a Blocker, not a Medium.** GA-05/GA-06
  are kept *additive* by owner decision, so they are compat-hardening, not blockers;
  GA-01 is the only Blocker (a silent-loss code/spec violation).
- **Challenge "protocol parity" whenever it ships a confusing or no-op surface.**
  GA-04 cuts the `PrefetchBytes` no-op; parity is not a licence for an API that lies.
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in the
  same PR (or earlier), then implementation/test follows. The first Phase-17 task
  records §10 **Rev 16**. Where the SPEC contract is *correct* and the code is wrong
  (GA-01), the code is fixed to the contract (no SPEC weakening).
- **No new build-tag lane.** GG-1..GG-4 are unit/mock-channel characterizations on the
  existing unit lane. The DeliveryMode regression rides the existing integration lane
  (3.13 **and** 4.x) for the on-broker persistence assertion.
- **No globals (§8).** GA-03's NoOp default keeps the library globals-free; the
  Prometheus opt-in injects a `prometheus.Registerer` (T112), never
  `DefaultRegisterer`.
- **`testify` `assert`/`require`** in every `_test.go`; **`goleak`** on any
  goroutine-spawning test (project rules).
- **Docs in English** (`CLAUDE.md`). Module path `github.com/brunomvsouza/warren`;
  `errorlint` is on (so the error-model changes must keep `errors.Is`/`As`-friendly
  sentinels).
- **Choke points:** wire encoding in `publisher.go` (`buildPublishing`); observability
  wiring in `connection.go`/`options_connection.go` + the builders; error
  classification in `errors.go` + `internal/amqperror`; topology shape in `topology.go`;
  codec contract in `codec/`. Corrections land at the choke point.
- **README sync:** any change to the external contract (the DeliveryMode fix is
  behaviour-preserving-to-the-*documented*-contract so no README change for the
  guarantee; the metrics-default correction, the `PrefetchBytes` removal, the
  `ExchangeBindings` addition, the independent-observability semantics) updates
  `README.md`.

---

## 4. Pre-work: verification gates (GG-1..GG-4 — sequence FIRST; they gate wording/fixes)

Each gate captures the **current (broken/surprising) behaviour** as a failing
characterization test that becomes the regression test. The gate task (T119) commits
the results table each downstream task cites. No SPEC edit to an affected section,
and no fix, lands before its gate returns. **No new lane.**

| Gate | Question (ground truth to capture) | Blocks |
|------|-----------------------------------|--------|
| **GG-1** | Does a zero-valued `Message[M]{Body:&x}` currently produce `amqp091.Publishing.DeliveryMode == 0` (transient) — i.e. is the §6.5 0→2 persistent mapping **absent** in `buildPublishing` today? Confirm against the broker (integration) that such a message does **not** survive a restart. | GA-01 / T120 |
| **GG-2** | With `Dial(WithTracer(realTracer))` and a `PublisherFor[M]` that never calls `.Tracer(...)`, does the publish path emit **NoOp spans** (no inheritance)? Confirm no builder reads `conn.opts.tracer`/`metrics`. | GA-02 / T121 |
| **GG-3** | With no `WithMetrics(...)`, is the default `Connection` metrics recorder **`NoOpClientMetrics`** (not Prometheus), and is there **no** caller of `NewPrometheus*` in non-test code — i.e. is the §6.1 L511 "Prometheus default" factually wrong? | GA-03 / T122 |
| **GG-4** | Does `PublisherFor[Order](conn).Codec(codec.NewProtobuf())` **compile** and fail only at the first `Publish` with `ErrInvalidMessage` (the codec↔`M` mismatch is runtime, not compile)? | GA-10 / T128 |

**Planner note:** GG-1 and GG-3 capture *current broken behaviour* the fix/correction
must close; GG-2 confirms the absent inheritance the spec reword must document; GG-4
characterizes the deliberate runtime-mismatch contract the doc must own. All are
unit-level except GG-1's on-broker persistence check (existing integration lane).

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets. Cross-lens findings already owned by a prior task are in WS-0.

### WS-0 — Cross-lens findings (record + add a `Lens-06` bullet; do **not** re-file)

- **GA-03 (registry mechanics) [→ T112]**, **GA-05 (alt-exchange) [→ T68]**,
  **GA-05 (exchange-binding shape) [→ T69]**, **GA-06 (interface evolution) [→ T70/T71]**,
  **GA-09 (fixture coupling) [→ T37]** — see the reconciliation table in §2. Each gets a
  `Lens-06` acceptance bullet on the existing task; the *new* SPEC scaffolding those
  bullets depend on (the additive-only gate, the `ExchangeBinding` pre-spec, the
  interface-evolution policy) is filed as the new tasks below.

### WS-1 — Silent-loss defects *(the must-fix core)*

- **GA-01 [Blocker, factual-error/latent-defect]** — *silent non-persistence.* §6.5
  L1554–1559 promises the library maps its zero value `0` (`DeliveryModePersistent`)
  to AMQP wire `2` (persistent) and `DeliveryModeTransient` (1) to wire `1`.
  `buildPublishing` (`publisher.go:946`) does a **raw cast** `uint8(msg.DeliveryMode)`
  with **no translation**, so a zero-valued `Message[M]{}` ships wire `0` =
  **transient** — the exact opposite of the durable-by-default headline, and a §1
  "no silent message loss" violation (a broker restart drops the message). The
  `basic.return` rehydration path (`publisher.go:310`) has the same cast. There is no
  wire-translation helper anywhere; existing tests assert only the API-level constant
  values, never the wire output, so the most important guarantee in the library is
  **unverified and currently false**. *Outcome:* translate enum→wire in
  `buildPublishing` (and the return path): `Persistent(0)→2`, `Transient(1)→1`; keep
  the §6.5 contract authoritative (do **not** weaken it); add the wire-value table to
  §6.5 explicitly if not already present. *Acceptance (GG-1):* a unit test asserts
  `buildPublishing(Message[M]{Body:&x}).DeliveryMode == 2` and the transient case
  `== 1`; an integration test (3.13 **and** 4.x) publishes a zero-valued message,
  restarts the broker, and asserts the message survives. *Disposition:* **new task
  T120** (P0).

- **GA-02 [High, design-disagreement]** — *silent observability loss.* Decision 44
  (L2937–2940) + §6.1 (L515–523) state "builder-level options override connection-level
  options for the same field," implying a publisher/consumer inherits the connection's
  `WithTracer`/`WithMetrics`. In code, builders default to `NoOpTracer`/`NoOp*Metrics`
  and **never read** `conn.opts.tracer`/`conn.opts.metrics`; connection metrics
  (`ClientMetrics`, lifecycle/pool events) and builder metrics
  (`Publisher/ConsumerMetrics`, publish/consume events) are **disjoint interfaces**. So
  `Dial(WithTracer(t))` yields **NoOp spans** on every publisher with no error — easy
  to misuse without reading docs. *Outcome (owner decision):* **reword the spec to
  "fully independent, no inheritance"** — state plainly that tracer *and* metrics are
  configured independently at the connection and builder levels, each defaults to NoOp,
  and connection-level observability covers lifecycle/pool events only; strike the
  "builder-overrides-connection" clause from decision 44 and §6.1; document that to
  instrument a publisher/consumer the caller must set `.Tracer(...)`/`.Metrics(...)` on
  the builder. *Acceptance (GG-2):* a unit test asserts a builder that never set
  `.Tracer(...)` emits NoOp spans even when the connection has a real tracer (locks the
  documented independence); the §6.1/decision-44 prose no longer promises inheritance.
  *Disposition:* **new task T121** (P1, doc-only).

### WS-2 — Honest defaults & footgun removal *(new)*

- **GA-03 [Med, factual-error]** — *metrics default drift + metrics/tracer
  inconsistency.* §6.1 L511 (and §3 overview L117) say `WithMetrics` defaults to
  Prometheus; the code defaults to **NoOp** (`connection.go:641`; the `WithMetrics`
  godoc agrees). The tracer defaults to NoOp (L513). For a *library*, auto-wiring a
  concrete backend (and any global registry) is a §8 "no globals" hazard. *Outcome
  (owner decision: NoOp default is correct):* correct §6.1 L511 and §3 L117 to
  "NoOp (opt-in Prometheus via `metrics.NewPrometheus*`)"; add one sentence to §9/§6.9
  stating the NoOp-default rationale (globals-free; inject your own registerer); the
  registry-injection (`WithMetricsRegisterer`, private per-`Connection` registry) is
  owned by **T112** (cross-lens bullet). *Acceptance (GG-3):* a unit test asserts the
  default `Connection` metrics is `NoOpClientMetrics`; the SPEC table reads NoOp.
  *Disposition:* **new task T122** (P1, SPEC) + `Lens-06` bullet on T112.

- **GA-04 [Med, design-disagreement]** — *`PrefetchBytes` no-op footgun.*
  `PrefetchBytes(uint)` (`consumer_builder.go:69–70`, `batch_consumer_builder.go:95–96`)
  accepts a meaningful value, discards it (`_ uint`), and is documented as a no-op on
  RabbitMQ — a user sets a byte budget and silently gets count-based prefetch. Parity is
  applied inconsistently: `Immediate()`/`NoLocal()` were *removed* (§6) for the same
  "RabbitMQ ignores it" reason. *Outcome (owner decision: cut it):* remove
  `PrefetchBytes` from `ConsumerBuilder` and `BatchConsumerBuilder`; list it in the §6
  "intentionally not exposed" set alongside `Immediate`/`NoLocal`; amend decision 10
  (L2631–2634) to record the removal (was "kept with no-op note"). *Acceptance:* the
  method no longer exists on either builder; §6/decision-10 document the removal; a
  doc/grep test asserts no `PrefetchBytes` in the public surface. *Disposition:* **new
  task T123** (P2, code+spec). *Note:* removing an exported no-op pre-v0.1 is safe (it
  never did anything); must land before the first tag.

### WS-3 — Compatibility hardening for the planned evolution *(new + cross-lens)*

- **GA-05 [High→resolved-additive, compat-risk]** — *`Binding` reshape risk.*
  `Binding{Exchange, Queue, RoutingKey, ...}` (`topology.go:61–67`) hard-wires the
  destination to a queue. R10-14/T69 (exchange→exchange bindings) tempts a rename to
  `{Source, Destination, DestinationKind, ...}` = **breaking** to every struct literal;
  the plan currently leaves the shape open ("extend `Binding` **or** add a typed
  variant"). *Outcome (owner decision):* pin a **separate
  `Topology.ExchangeBindings []ExchangeBinding`** with `ExchangeBinding{Source string,
  Destination string, RoutingKey string, NoWait bool, Args Headers}` in §6.6 **now**;
  `Binding` is **not** reshaped. R10-13/T68 (alternate-exchange) stays additive via
  `Exchange.Args` (or an optional `AlternateExchange` field, zero = today).
  *Acceptance:* §6.6 specs `ExchangeBinding`; an acceptance bullet on T69 asserts no
  exported `Binding` field is renamed/removed and the deep-snapshot/declare-once
  semantics extend to `ExchangeBindings`. *Disposition:* **new task T124** (P1, SPEC
  pre-spec) + `Lens-06` bullets on T68 and T69.

- **GA-16 [Low-Med, compat-risk]** — *streams v0.2 not pinned additive.* Decision 24
  (L2780–2783) ships `QueueTypeStream` declaration-only in v0.1 but makes **no
  additive-only commitment** for the v0.2 native-stream-consume API; a future author
  could reshape `ConsumerBuilder[M]`/`Delivery[M]`. *Outcome:* add to decision 24 (and
  the §9 gate from T124) that the v0.2 stream-consume API will be **purely additive** —
  new builder methods (`StreamOffset`, …) and/or a new `StreamConsumerFor[M]` type and
  additive `Delivery[M]` methods — and `x-stream-offset` is already expressible via
  `ConsumerBuilder.Args` in v0.1. *Acceptance:* decision 24 + §9 state the additive-only
  commitment. *Disposition:* **folded into T124**.

- **GA-06 [Med-High, compat-risk]** — *interface evolution breaks external
  implementers.* R10-15/T70 + R10-16/T71 add methods to the user-implementable
  `metrics.*` interfaces (`metrics.ConsumerMetrics` etc.); adding a method to an
  exported interface **breaks every external type that implements it** at compile time.
  Combined with the §9 "`// Deprecated`-free rc1→v1.0" rule (L2439), these must land
  before rc1 *and* the interfaces need an extension-tolerance policy. *Outcome:* define
  a SPEC policy (new §6.9 note + §10 decision): the `metrics`/`log`/`otel` interfaces
  ship with an **embeddable `NoOp` base struct** (e.g. `metrics.NoOp`) that users embed,
  so adding interface methods is forward-compatible (the embed satisfies new methods as
  no-ops); document that all v0.1 metric additions land before the first tag.
  *Acceptance:* the SPEC states the embeddable-base policy; an example shows a custom
  metrics adapter embedding the NoOp base surviving a method addition. *Disposition:*
  **new task T125** (P1, SPEC+code) + `Lens-06` bullets on T70/T71.

- **§9 additive-only gate** — *Outcome (part of T124):* add a §9 Success-Criteria
  gate — "No exported type in §6 changes field names or removes fields after the first
  published tag (`v0.1.0`); topology extensions (T68/T69/T102) and stream-consume (v0.2)
  are additive-only" — and a one-line clarification that `rc1` is the pre-`v0.1.0`
  candidate and Phases 11–17 complete **before** T46 cuts the tag (removing the
  phase-number-vs-tag ambiguity). *Disposition:* **folded into T124**.

### WS-4 — Error-model correctness *(new)*

- **GA-07 [Med, factual-error/design-disagreement]** — *classification
  contradictions.* (a) §6.8 **L1951 lists reply code 311 as transient** while L1970
  lists it as permanent — self-contradictory; the code (`errors.go`) correctly treats
  311 as **permanent-only** (a frame-max overflow fails on every retry). (b) The
  transient/permanent partition is **partial** (defined only over transport/broker
  errors); `ErrUnroutable` (312/313) is intentionally in **neither** set — undocumented,
  reads as an omission. (c) `ErrTransient` + `ErrPermanent` can **both** match one error
  by wrapping accident, with **no precedence rule**; §6.8 L1957 actively invites
  wrapping a 506 (`ErrResourceError`, permanent) with `ErrTransient` to re-classify it —
  producing a both-true error a permanent-first caller (`if IsPermanent {drop} else if
  IsTransient {retry}`) silently **drops**, defeating the documented guidance. *Outcome:*
  (a) remove 311 from the §6.8 transient list (code is authoritative); (b) state the
  partition is partial and `ErrUnroutable` is deliberately in neither (callers branch on
  the sentinel); (c) define precedence — **`ErrTransient` in the chain wins** for
  re-classification (or `IsPermanent` returns false when `ErrTransient` is also present)
  — and add a test asserting a 506-wrapped-with-`ErrTransient` is classified transient.
  *Acceptance:* §6.8 lists 311 as permanent only; the precedence rule is specified and
  tested; the partial-partition + `ErrUnroutable` note exists. *Disposition:* **new task
  T126** (P2, SPEC+code+test).

- **GA-08 [Med, design-disagreement]** — *`AMQPCode` overloads two frame classes.*
  `AMQPCode` (`errors.go:171–195`, decision 38 L2895–2900) returns 312/313 (a
  `basic.return` reply-code) *and* channel/connection-close codes (311/320/402–406/…)
  through one `(uint16, bool)` with no provenance signal; a caller `switch`-ing on the
  code cannot tell which frame class produced it. *Outcome:* amend §6.8 to warn loudly
  that a returned code MAY be a `basic.return` code (312/313) and that callers needing to
  distinguish must combine with `errors.Is(err, ErrUnroutable)`; file **LATER-41** for a
  dedicated `ReturnCode(err) (uint16, bool)` accessor (or a code-class enum) that
  separates the two frame spaces. *Acceptance:* §6.8 carries the frame-class caveat;
  LATER-41 exists. *Disposition:* **folded into T126** + **LATER-41**.

- **GA-15 [Low, underspecified]** — *near-redundant sentinels.* The taxonomy is 40
  sentinels (16 are the mechanical AMQP reply-code mirror — justified), not the prompt's
  "~30". `ErrTopologyMismatch` **wraps** `ErrPreconditionFailed` (same 406 trigger, same
  caller action) so `errors.Is` matches both — a documented alias, not a bug.
  `ErrPoison` produces the *same* broker effect (nack-no-requeue) as returning any bare
  handler error — it is intent-only. *Outcome:* §6.8 notes that `ErrTopologyMismatch` is
  a named alias over `ErrPreconditionFailed` for the declare path, and §6.3 notes
  `ErrPoison` and a bare handler error are behaviourally identical (intent-only); correct
  any "~30" figure to 40. *Disposition:* **folded into T126**.

### WS-5 — Deliberate-choice documentation & surface accuracy *(new)*

- **GA-10 [Med, underspecified]** — *codec non-generic gap undocumented.* `codec.Codec`
  is non-generic (`Encode(any)`/`Decode([]byte, any)`, `codec/codec.go:18–25`) while
  `PublisherFor[M]`/`ConsumerFor[M]` are generic, so `PublisherFor[Order](conn).Codec(
  codec.NewProtobuf())` **compiles** and fails only at the first `Publish` with
  `ErrInvalidMessage` (Order ∉ `proto.Message`). The non-generic choice is *defensible*
  — a generic `Codec[M]` would break registry-sharing, the `any`-shaped CloudEvents SDK
  path (decision 4), and the differing type bounds of JSON (`any`) vs Protobuf
  (`proto.Message`) — but it is **implicit**, contradicting the §5/§8 "no `any` where a
  generic would do" direction. *Outcome:* add an explicit §10 decision: `codec.Codec` is
  intentionally non-generic; a payload↔codec mismatch is a runtime `ErrInvalidMessage`,
  not a compile error; each built-in non-JSON codec documents its required `M`
  (`proto.Message` for Protobuf, `codec.CloudEvent` for CloudEvents) and fails fast;
  cross-reference from §5. *Acceptance (GG-4):* §10 records the decision; a doc example
  shows the runtime-mismatch contract. *Disposition:* **new task T128** (P2, SPEC).

- **GA-11 [Low-Med, underspecified]** — *`Message[M].Body *M` rationale undocumented.*
  `Body` is `*M` (`message.go:22`), so the zero `Message[M]{}` is invalid — but
  **loudly** (`publisher.go:538` returns `ErrInvalidMessage: Body must not be nil`),
  which is acceptable; the *reason* for `*M` (publish/consume symmetry with
  `Delivery[M].Body() *M`; avoid copying large payloads in the struct literal) is
  unstated, and it makes the documented "durable-by-default zero value" simultaneously
  *invalid* — a mild pit-of-success contradiction. *Outcome:* §6.5 note explaining the
  `*M` choice and that nil `Body` is a loud `ErrInvalidMessage` (never a silent drop).
  *Disposition:* **folded into T128**.

- **GA-14 [Low, underspecified]** — *`HeaderCodec` silent feature-miss.* The optional
  `HeaderCodec` interface is detected by runtime type assertion (`publisher.go:600` /
  `consumer.go:809`) — idiomatic Go (keep), but a third-party codec author who typos a
  method signature silently falls back to `Encode`/`Decode` with no diagnostic.
  *Outcome:* §6.9 author caveat — opt-in requires satisfying the full `HeaderCodec`
  method set; recommend a compile-time assertion `var _ codec.HeaderCodec =
  (*MyCodec)(nil)` (as the built-in does); cross-reference from the `.Codec(...)` builder
  godoc. *Disposition:* **folded into T128**. Plus the §8 sanctioned-`any` closed list
  (Headers / `*.Args` / `WithClientProperties` / OTel carriers = field-tables; `log`
  printf variadics; the codec `v any` per GA-10) so future reviewers audit against a
  list, not site-by-site. *Disposition:* **folded into T128**. Plus the GA-09 fixture
  unkeyed-literal guard note (`DeliveryFixture`/`BatchFixture`). *Disposition:* **folded
  into T128**, coordinating with the T37 cross-lens bullet.

- **GA-12 [Med, factual-error]** — *surface signature drift.* §6.1/§6.2 drift from the
  code: `WithOnResubscribe` appears in the §6.1 table (L508) but **no such option
  exists** (the callback lives in prose at L629); `WithDialer` is documented as
  `net.Dialer` but is a dial-func (`options_connection.go:176`); `WithFrameMax(int)` is
  `uint32`; `WithChannelMax` is `uint16` (untyped in table); `PublishResult{Index int;
  Err error}` (L776–779) is `{Err error}` in code; §6.2 `Return.Body []byte` and
  `ReturnedProperties.Expiration string` drift from the code (`Expiration` is
  `time.Duration`). *Outcome:* reconcile the §6.1 option table and §6.2 types with the
  code — for each field, make the SPEC match the implementation (or implement the
  documented option where it is the intended contract, e.g. decide `WithOnResubscribe`:
  table or prose, not both). *Acceptance:* every §6.1/§6.2 signature matches a code
  `file:line`; the phantom option is resolved. *Disposition:* **new task T127** (P2,
  SPEC, may also touch code where SPEC is the intended contract).

- **GA-13 [Low, underspecified]** — *naming grammar gaps + last-wins scope.* §5 Naming
  lacks carve-outs for: `WithoutMetrics()` on a builder (the lone `Without*` builder
  method — currently a silent §5 violation; legalize it as the sanctioned metrics-disable
  exception); `Use*`/`Allow*` verb-prefix builder methods (`UseExclusiveReplyQueue`,
  `AllowMissingDLX`); noun-phrase setters (`MaxMessageSizeBytes`, `PublishBatchMaxSize` —
  deliberately field-style for explicitness). Decision 44's "last-wins" is literally true
  only for value-carrying setters; **boolean flag-setters** (`Mandatory`, `StampUserID`,
  `ChannelQoS`, `Exclusive`, `AutoAck`, `WithoutMetrics`) are **monotonic-set, no
  inverse** — scope the decision honestly. Also weigh the builder-level `WithoutMetrics`
  redundancy (builder metrics already default to NoOp; the method only acts as a
  last-wins reset) — keep for cross-level symmetry but document the narrow purpose. Also
  fix the `consumer_builder.go:72` `ChannelQoS` godoc bug (says `global=false`; code sets
  `global=true`, `consumer.go:460`) and add the `basic.qos global=true` mapping to the
  §6.3 doc. *Outcome:* §5 carve-outs + decision-44 scoping + the `ChannelQoS` doc fix.
  *Acceptance:* §5 sanctions the four patterns; decision 44 scopes last-wins to
  value-setters; the `ChannelQoS` godoc matches the code. *Disposition:* **new task
  T129** (P3, SPEC + a doc fix in code).

---

## 6. Do-not-regress list (confirmed-correct — protect with tests)

These are API-design strengths the review confirmed; protect them:

- **The generics split** (§5 L264–269): `PublisherFor[M]`/`ConsumerFor[M]` carry `M`
  once at the constructor; builder methods and functional options are non-generic. This
  is the cleanest practical Go expression — keep it (it is the *cause* of GA-10's runtime
  codec contract, which GA-10 documents rather than "fixes").
- **Safe-by-default zero values (except GA-01):** `DeliveryMode` zero *intent* is
  persistent (GA-01 fixes the wire bug; do not change the enum so the zero stays
  Persistent); `TimeoutVerdict` zero → `TimeoutNackNoRequeue`; `Priority`/`Expiration`
  zero are safe; `QueueType`/`OverflowPolicy` empty → broker default. Keep a wire-level
  test for each after GA-01 lands.
- **Loud-invalid `Message[M]{}`** (`publisher.go:538`): nil `Body` → `ErrInvalidMessage`,
  never a silent drop. Keep.
- **Concrete structs with unexported fields + constructors** (decision 9 L2607–2610):
  `Delivery[M]`/`Batch[M]` let methods be added in minor releases without breaking
  implementers — adding a *method* to a concrete struct never breaks anyone. Keep
  (GA-09 only adds a *lighter fabrication path*, not a reshape).
- **Justified `any` field-tables** (decision 13): `Headers`, `*.Args`,
  `WithClientProperties`, OTel carriers model the AMQP field-table union — keep (GA-14
  documents the closed exception list).
- **`HeaderCodec` fail-fast for the built-in** (§6.9 L2039–2042): `NewCloudEventsBinary`'s
  plain `Encode`/`Decode` reject non-header use with `ErrInvalidMessage` so its
  `cloudEvents:` attributes can never be silently dropped. Keep.
- **Last-wins for value-carrying options** (decision 44, asserted in tests): keep; GA-13
  only scopes the claim away from boolean flag-setters.
- **The AMQP reply-code sentinel mirror** (§6.8): 16 sentinels 1:1 with the wire codes,
  `errors.Is`/`As`-friendly, `errorlint`-clean. Keep; GA-07 only fixes the 311
  contradiction and the precedence rule.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Amend SPEC | Disposition | LATER.md | Owner decision? |
|---------|-----|-----------|-------------|----------|-----------------|
| GA-01 | **Blocker** | ✅ §6.5 (confirm) | **T120** (fix wire mapping + test) | — | — (fix is unambiguous) |
| GA-02 | High | ✅ §6.1/dec.44 | **T121** (reword: independent) | — | ✅ reword independent |
| GA-03 | Med | ✅ §6.1/§3/§9 | **T122** + bullet T112 | (T112) | ✅ NoOp default |
| GA-04 | Med | ✅ §6/dec.10 | **T123** (cut `PrefetchBytes`) | — | ✅ cut |
| GA-05 | High→additive | ✅ §6.6 | **T124** + bullets T68/T69 | — | ✅ `ExchangeBindings` |
| GA-06 | Med-High | ✅ §6.9/§10 | **T125** + bullets T70/T71 | — | — |
| GA-07 | Med | ✅ §6.8 | **T126** | — | — |
| GA-08 | Med | ✅ §6.8 | **T126** | **LATER-41** | — |
| GA-09 | Med | ✅ §6.3/§6.4 | bullet **T37** (+ guard note in T128) | — | — |
| GA-10 | Med | ✅ §10/§5 | **T128** | — | — |
| GA-11 | Low-Med | ✅ §6.5 | **T128** | — | — |
| GA-12 | Med | ✅ §6.1/§6.2 | **T127** | — | — |
| GA-13 | Low | ✅ §5/dec.44 | **T129** | — | — |
| GA-14 | Low | ✅ §6.9/§8 | **T128** | — | — |
| GA-15 | Low | ✅ §6.8/§6.3 | **T126** | — | — |
| GA-16 | Low-Med | ✅ §10/§9 | **T124** | — | — |

---

## 8. Suggested phasing (planner may revise — "Phase 17")

1. **Phase A — Gate (no new lane).** Capture GG-1..GG-4 (unit + the existing
   integration lane for GG-1's persistence check); commit the results table. **(T119)**
   Records §10 **Rev 16**.
2. **Phase B — Silent-loss fixes (highest priority).** DeliveryMode wire fix
   (**T120**, GG-1) — the Blocker; observability-independence reword (**T121**, GG-2).
   Land first.
3. **Phase C — Honest defaults & footgun removal.** Metrics-default correction
   (**T122**, GG-3, + bullet T112); cut `PrefetchBytes` (**T123**).
4. **Phase D — Compatibility hardening (pin before any tag).** `ExchangeBindings`
   pre-spec + §9 additive-only gate + stream additive-only (**T124**, + bullets
   T68/T69); interface-evolution policy (**T125**, + bullets T70/T71). Must complete
   before T46 cuts `v0.1.0`.
5. **Phase E — Error-model + surface accuracy + docs.** Error-classification fixes
   (**T126**, → LATER-41); signature-accuracy sweep (**T127**); deliberate-choice docs
   (**T128**, GG-4); naming carve-outs + last-wins scoping (**T129**). Land last so the
   docs reference the corrected surface.

---

## 9. Acceptance criteria for the whole effort

- [ ] GG-1..GG-4 captured (unit + the existing integration lane for GG-1) into a
      committed results table each downstream task cites (T119). **No new build-tag
      lane.** First task records §10 **Rev 16**.
- [ ] **Silent durability loss fixed (GA-01/T120):** `buildPublishing` translates
      `Persistent(0)→wire 2`, `Transient(1)→wire 1` (and the `basic.return` path);
      a unit test asserts the wire value and an integration test (3.13 **and** 4.x)
      proves a zero-valued message survives a broker restart. §6.5 contract unchanged.
- [ ] **Silent observability loss documented (GA-02/T121):** §6.1 + decision 44 state
      tracer and metrics are configured **independently** at each level (no
      inheritance); a test locks that a builder without `.Tracer(...)` emits NoOp spans
      even under a real connection tracer.
- [ ] **Defaults honest (GA-03/T122):** §6.1 L511 + §3 read "NoOp (opt-in Prometheus)";
      a test asserts the default `Connection` metrics is `NoOpClientMetrics`; T112 owns
      the registry-injection opt-in.
- [ ] **Footgun removed (GA-04/T123):** `PrefetchBytes` is gone from both builders and
      listed in §6 "intentionally not exposed"; decision 10 records the removal.
- [ ] **Roadmap pinned additive (GA-05/GA-16/T124):** §6.6 specs
      `Topology.ExchangeBindings []ExchangeBinding` (Binding untouched); §9 carries the
      additive-only-after-first-tag gate + rc1 clarification; decision 24 commits the
      v0.2 stream API additive; T68/T69 carry `Lens-06` no-field-rename acceptance.
- [ ] **Interfaces extension-tolerant (GA-06/T125):** the `metrics`/`log`/`otel`
      interfaces ship an embeddable NoOp base; an example shows a custom adapter
      surviving a method addition; T70/T71 land before the first tag.
- [ ] **Error model sound (GA-07/GA-08/GA-15/T126):** §6.8 lists 311 permanent-only;
      the transient/permanent precedence rule is specified + tested; the partial
      partition and `ErrUnroutable`-in-neither are documented; the `AMQPCode`
      frame-class caveat exists; LATER-41 (`ReturnCode`) is filed.
- [ ] **Surface matches code (GA-12/T127):** every §6.1/§6.2 signature matches a code
      `file:line`; the `WithOnResubscribe` phantom is resolved.
- [ ] **Deliberate choices documented (GA-09/GA-10/GA-11/GA-14/T128):** §10 records the
      non-generic-codec decision; §6.5 explains `Body *M` + loud-invalid; §6.9 has the
      `HeaderCodec` author caveat; §8 lists the sanctioned `any` exceptions; the fixture
      unkeyed-literal guard is noted (coordinated with the T37 lightweight-fixture
      bullet).
- [ ] **Naming + last-wins honest (GA-13/T129):** §5 carve-outs exist; decision 44
      scoped to value-setters; the `ChannelQoS` godoc matches the code.
- [ ] No finding re-filed that a prior task owns (GA-03→T112, GA-05→T68/T69,
      GA-06→T70/T71, GA-09→T37). **One** new `LATER.md` entry (LATER-41). No duplicate
      task IDs (T119–T129 contiguous).
- [ ] `README.md` reflects the changed contract (metrics-default correction,
      `PrefetchBytes` removal, `ExchangeBindings`, independent-observability semantics).
- [ ] Tree green: `make test` (race + cover), `make lint`, the per-version integration
      lane; `goleak` clean.

---

## 10. Owner decisions (recorded 2026-05-28)

1. **GA-02 — observability inheritance = REWORD INDEPENDENT.** The spec is corrected to
   state tracer and metrics are configured **independently** at the connection and
   builder levels (no inheritance); each defaults to NoOp. Doc-only; matches the code.
   The "builder-overrides-connection" clause is struck from decision 44 / §6.1. **(T121)**
2. **GA-03 — metrics default = NoOp (correct the SPEC).** A library must not auto-wire a
   concrete backend or a global registry (§8). §6.1 L511 / §3 are corrected to
   "NoOp (opt-in Prometheus)"; the opt-in registry injection stays owned by **T112**.
   **(T122)**
3. **GA-04 — `PrefetchBytes` = CUT.** Remove the no-op from both consumer builders and
   list it in §6 "intentionally not exposed" alongside `Immediate()`/`NoLocal()`; record
   the removal in decision 10. **(T123)**
4. **GA-05 — exchange→exchange binding shape = SEPARATE `Topology.ExchangeBindings`.** A
   new typed `ExchangeBinding{Source, Destination, RoutingKey, NoWait, Args}` slice on
   `Topology`; the existing `Binding` is **not** reshaped (no `Source`/`Destination`
   rename). Pinned in §6.6 now so T69 cannot implement the breaking variant. **(T124 +
   T69 bullet)**

(The remaining tasks — T119, T120, T125, T126, T127, T128, T129, the §9 additive-only
gate, the stream additive-only commitment, the interface-evolution embeddable-NoOp
policy, the error-model fixes, the surface-accuracy sweep, and the documentation tasks
— follow directly from the findings and need no further owner input. GA-01's fix
direction is unambiguous: translate enum→wire, keep the §6.5 contract.)

---

## 11. Compatibility ledger (lens output #2)

Every planned change marked `additive` / `breaking` / `unclear→pinned`, with the fix
that keeps it additive. All land **before** the first tag (`v0.1.0`/T46), so the §9
`// Deprecated`-free-rc1→v1.0 rule (L2439) is satisfied.

| Planned change | Task / phase | Exported identifier | Verdict | Fix to keep it additive |
|----------------|--------------|---------------------|---------|-------------------------|
| Alternate-exchange (R10-13) | T68 / 15 | `Exchange` | **additive** | Express via `Exchange.Args` today, or add optional `AlternateExchange string` (zero = today). |
| Exchange→exchange binding (R10-14) | T69 / 15 | `Binding` | **was unclear → pinned additive** | New `Topology.ExchangeBindings []ExchangeBinding`; **do not** rename `Binding.{Exchange,Queue}` → `{Source,Destination}`. (Owner decision; T124 pre-specs it.) |
| Graceful-shutdown drain/flush (R10-15) | T70 / 13 | `Close(ctx)` semantics; `metrics.ConsumerMetrics` | **additive (behaviour)** + interface caveat | Behaviour-only on `Close`; the new metric inherits the interface caveat below. |
| New observability metrics (R10-16) | T71 / 13 | `metrics.*` interfaces | **breaking for external implementers** | Embeddable `metrics.NoOp` base so adding methods is forward-compatible (T125); land before rc1. |
| Cut `PrefetchBytes` (GA-04) | T123 / 17 | `ConsumerBuilder`/`BatchConsumerBuilder` | **breaking removal, pre-tag safe** | It is a no-op that never did anything; remove before the first tag — no consumer depends on its (absent) effect. |
| DeliveryMode wire fix (GA-01) | T120 / 17 | none (behaviour → documented contract) | **non-breaking** | Aligns wire output with the documented §6.5 contract; no signature change. |
| Stream-consume v0.2 (decision 24) | v0.2 | `ConsumerBuilder[M]`/`Delivery[M]` | **achievable additive → pinned** | New `StreamOffset`/`StreamConsumerFor[M]` + additive `Delivery[M]` methods; `x-stream-offset` via `Args` in v0.1 (T124 commits additive-only). |

---

## 12. Footgun list (lens output #3 — exported identifiers easy to misuse, ranked)

1. **`Message[M]{}` zero value (GA-01)** — *silently ships non-persistent* despite the
   "durable-by-default" headline. The worst kind: the misuse is invisible and the loss is
   silent. **Fixed by T120.**
2. **`Dial(WithTracer(t))` / `WithMetrics(m)` (GA-02)** — silently *not* inherited by
   builders; the user instruments the connection and gets NoOp spans on publishers.
   **Documented away by T121** (independence made explicit).
3. **`PrefetchBytes(n)` (GA-04)** — accepts a byte budget, silently does nothing.
   **Cut by T123.**
4. **`AMQPCode(err)` (GA-08)** — a naive `switch` mixes channel-close codes and
   `basic.return` 312/313 from two frame classes. **Caveat + LATER-41 (T126).**
5. **506 re-classified with `ErrTransient` (GA-07)** — becomes both-transient-and-permanent;
   a permanent-first caller drops it. **Precedence rule (T126).**
6. **`PublisherFor[Order].Codec(NewProtobuf())` (GA-10)** — compiles, fails at first
   `Publish`. **Documented as the deliberate runtime contract (T128).**
7. **Custom `HeaderCodec` with a typo'd signature (GA-14)** — silently falls back to
   plain `Encode`; headers vanish. **Author caveat + compile-time-assertion advice (T128).**
8. **Consumer unit tests must import `amqpmock` (gomock) to fabricate `Delivery[M]`
   (GA-09)** — heavy test coupling. **Lightweight fixture path (T37 bullet).**

---

## 13. Findings table (lens output #1)

| ID | Sev | Class | Location (§ + lines / file:line) | API concern | Misuse / evolution scenario | Disposition |
|----|-----|-------|----------------------------------|-------------|------------------------------|-------------|
| GA-01 | **Blocker** | factual-error / latent-defect | §6.5 L1554–1559; `publisher.go:946`,`:310`; `types.go:18` | zero value is **silently-wrong** on the wire | every default publish is non-persistent; lost on broker restart | **T120** (GG-1) |
| GA-02 | High | design-disagreement | dec.44 L2937–2940; §6.1 L515–523; `connection.go:643`, builders | promised inheritance absent | `Dial(WithTracer)` → NoOp spans, no error | **T121** (GG-2) |
| GA-03 | Med | factual-error | §6.1 L511; §3 L117; `connection.go:641` | documented default ≠ code default; metrics/tracer inconsistent | "default Prometheus" never wired; surprise | **T122** (GG-3) + T112 |
| GA-04 | Med | design-disagreement | §6.3 L1145; `consumer_builder.go:69`; dec.10 L2631 | exported no-op that lies | sets byte budget, gets count-based | **T123** (cut) |
| GA-05 | High→additive | compat-risk | §6.6 L1634–1640; `topology.go:61`; R10-14 L3130 | exported struct reshape risk | `Binding` rename breaks v0.1 literals | **T124** + T68/T69 |
| GA-06 | Med-High | compat-risk | §6.9; R10-16 L3137; `metrics/metrics.go` | interface method add breaks implementers | external metrics adapter fails compile | **T125** + T70/T71 |
| GA-07 | Med | factual-error / design-disagreement | §6.8 L1951/L1970/L1957; `errors.go:207–268` | 311 in both sets; no precedence; partial partition | re-classified 506 silently dropped | **T126** |
| GA-08 | Med | design-disagreement | dec.38 L2895; `errors.go:171–195` | `AMQPCode` overloads two frame classes | naive `switch` mishandles 312/313 | **T126** + LATER-41 |
| GA-09 | Med | design-disagreement | dec.9 L2607; `delivery.go:14`,`batch_consumer.go:45` | fixtures force gomock import | every raw/batch consumer test pulls `amqpmock` | **T37** bullet (+T128 guard) |
| GA-10 | Med | underspecified | §6.9 L1983; `codec/codec.go:18`; §8 L2420 | non-generic codec gap implicit | `M`↔codec mismatch is runtime only | **T128** (GG-4) |
| GA-11 | Low-Med | underspecified | §6.5 L1435; `message.go:22`; `publisher.go:538` | `*M` rationale unstated | invalid (loud) zero value; mild contradiction | **T128** |
| GA-12 | Med | factual-error | §6.1 L498/L497/L508; §6.2 L776/L808; `options_connection.go:176` | SPEC signatures drift from code | phantom `WithOnResubscribe`; wrong types | **T127** |
| GA-13 | Low | underspecified | §5 L405–414; dec.44; `consumer_builder.go:72` | naming gaps; last-wins over-claimed; `ChannelQoS` doc bug | composer can't un-set flags; doc says `global=false` | **T129** |
| GA-14 | Low | underspecified | §6.9 L2033; `publisher.go:600`,`consumer.go:809` | optional interface silent miss | typo'd `HeaderCodec` drops headers | **T128** |
| GA-15 | Low | underspecified | §6.8; `errors.go` | near-redundant sentinels (count 40 not ~30) | `ErrTopologyMismatch`⊃`ErrPreconditionFailed`; `ErrPoison` intent-only | **T126** |
| GA-16 | Low-Med | compat-risk | dec.24 L2780; §9 | v0.2 stream API not pinned additive | future `ConsumerBuilder` reshape | **T124** |

---

## 14. Out of scope for this plan

- The other validation lenses (01 protocol — Phase 12; 02 distributed-systems — Phase
  13; 03 interoperability — Phase 14; 04 EDA — Phase 15; 05 SRE — Phase 16; 07–13).
  Cross-lens shared findings extend the shared task, never re-filed (GA-03→T112,
  GA-05→T68/T69, GA-06→T70/T71, GA-09→T37).
- A **generic `Codec[M]`** redesign — rejected (breaks registry-sharing, the
  CloudEvents `any`-shaped SDK path, and the JSON-vs-Protobuf type bounds); the
  non-generic choice is documented (T128), not changed.
- A dedicated `ReturnCode(err)` accessor / code-class enum — deferred to **LATER-41**
  (T126 ships the doc caveat now).
- Implementing observability *inheritance* — rejected by owner decision (GA-02 reworded
  to independence); not revisited.
- The async/streaming publish API (LATER-34), OAuth2 (v0.2), stream-protocol semantics
  (v0.2, decision 24 — only the additive-only *commitment* is added here).
- Any change to the §10 API design north stars beyond the corrections above.

---

## 15. Verdict for this lens

**GO-WITH-CHANGES.** Judged by *hard-to-misuse* and *forever-stable*, the public
surface is in good shape: the `XFor[M]` generics split is the cleanest Go expression
available, the zero-value defaults are mostly safe-by-default, the concrete-struct
(decision 9) and field-table-`any` (decision 13) choices are sound and
forward-compatible, and the error taxonomy is navigable and `errorlint`-clean. But the
review found **one Blocker that is not an API-shape flaw at all — it is a silent
durability loss**: a zero-valued `Message[M]{}` ships **non-persistent** on the wire
(`buildPublishing` casts the enum raw instead of translating `0→2`), directly
contradicting the §6.5 "durable-by-default" headline and the §1 "no silent message
loss" bar, and it is **unverified by any wire-level test** (GA-01/T120). The second
most dangerous finding is also a *silent* one: `Dial(WithTracer(t))` instruments
nothing on publishers because the promised builder-overrides-connection inheritance
does not exist (GA-02/T121) — reworded to honest independence. The rest is
honest-defaults work (NoOp metrics default, GA-03; cut the `PrefetchBytes` no-op,
GA-04), compatibility hardening that keeps the Phase-11 roadmap **additive** before the
first tag (a separate `ExchangeBindings`, an embeddable-NoOp interface policy, a §9
additive-only gate — GA-05/06/16), error-model correctness (the 311 both-sets
contradiction and the missing transient/permanent precedence — GA-07/08/15), and a set
of documentation + surface-accuracy corrections so the deliberate `any`/generics/struct
choices read as decisions, not gaps (GA-09..GA-14). No API redesign is required: the
surface is **kept, made honest, and pinned additive** — provided GA-01 is treated as
the release-blocking durability bug it is.
