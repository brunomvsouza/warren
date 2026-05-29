# Plan Input — Remediate Data-Protection Compliance Findings (GDPR & LGPD) (Lens 11)

> **Lens:** Data-Protection Compliance — GDPR (EU 2016/679) & LGPD (Brazil, Lei 13.709/2018)
> (`spec-validation/11-compliance-gdpr-lgpd.md`).
> **Persona:** a privacy-engineering specialist who reads a message bus as a **processor /
> operador** surface and asks of every default *does this make the downstream controller's
> compliance harder or impossible, and is the mitigating pattern recommended as a privacy
> control — not just an optimisation?*
> **Verdict:** **GO-WITH-CHANGES** — **no Blocker.** The library never *violates* the law (it is
> a processor/tool, not a controller) and it already does the hard confidentiality work: URI
> credentials are redacted at a choke-point, default OTel attributes carry **size not content**,
> default metric labels are bounded and PII-free, and the high-confidentiality controls a privacy
> reviewer would demand (never-log-payloads, PLAIN-cleartext warning, at-rest-encryption boundary)
> are **already owned** by the security lens (T134/T132/T141). What this lens exposes is **not a
> bug — it is a positioning and silence gap**: the spec recommends the one pattern that makes
> personal data on a bus *survivable* (pointer-out: keep PII in an erasable store of record, put
> only an opaque reference on the wire) **purely as a size optimisation** (≥100 MiB → S3), never
> as the canonical **privacy / erasure / breach-minimisation** control it actually is; and it is
> **silent** on the structural reality that personal data written to a durable queue (and
> `DeliveryModePersistent` is the **zero-value default**) is effectively **un-erasable** while in
> flight or in a DLQ — there is no AMQP primitive to delete a single message. **15 findings
> (DP-01..DP-15); 5 gates (DG-1..DG-5); 4 net-new tasks (T153–T156); 5 cross-lens extensions
> (T65/T85/T132/T134/T141); 4 confirmations (T07c/T133, bounded metric labels, OTel
> size-not-content); LATER-47.**

---

## 1. Objective

Make `warren`'s defaults and documentation let a downstream **controller / controlador** meet
its GDPR/LGPD obligations — by *positioning the controls the library already has as
privacy controls*, by *changing the one default that silently opts users into a
storage-limitation violation* (the unbounded, TTL-less DLQ — already owned by T65), and by
*breaking the silence* on the data-protection hazards a transport wrapper is uniquely placed to
warn about. The bar is binary and framed for a processor, not a controller:

- **A default that opts the user into a non-compliant posture is a finding.** Durable-by-default
  (§6.5 L1450/L1554) writes personal data to broker disk and, on dead-letter, to a DLQ that the
  library auto-declares with **no `x-message-ttl` and no `x-max-length`** (R10-10/T65) — so
  dead-lettered personal data is retained **indefinitely** by default (GDPR 5(1)(e) storage
  limitation / LGPD Art. 16 eliminação após término). The default-change is owned by T65; this
  lens supplies the **storage-limitation framing** that makes the bound a *privacy* control.
- **A mitigating pattern that exists but is mis-positioned is a finding.** §6.1 L724-726 and §10
  L2952 recommend pointer-out (publish a reference, not the body) **only for payload size**. The
  *same* pattern is the canonical answer to right-to-erasure (you can delete the record of
  store; the bus only ever held a reference) and to breach minimisation (a queue dump leaks
  references, not PII). It must be positioned as a **first-class privacy control with a runnable
  example** — the prompt's headline.
- **Silence on a known privacy hazard is a finding when the library is the only layer that can
  warn.** The spec never states that (a) message bodies on a durable queue are **un-erasable**
  at the bus layer, (b) RabbitMQ does **not** encrypt bodies at rest, (c) quorum replication and
  `WithAddrs` failover can cross **jurisdictions** (international transfer, GDPR Ch. V / LGPD
  Art. 33) with residency invisible at the API, or (d) a natural-key `MessageID`/`CorrelationID`
  turns the **dedupe cache, `x-death`, logs, and the OTel `messaging.message.id` /
  `conversation_id` attributes** (which flow to a third-party APM) into stores of personal data.

The net-new value is the **privacy-by-design layer no prior lens owns**: the security lens (07)
owns *confidentiality of processing* (Art. 32 / LGPD Art. 46 — redaction, transport, at-rest
boundary), but **none** of the data-protection *principles* that sit above it — **erasure /
storage limitation, data minimisation, international transfer, records of processing,
accountability** — has an owner. **There is no Blocker:** every load-bearing *default change* the
lens would demand is already owned by T65 (DLQ bound+TTL); everything else is documentation, a
positioned control, or a runnable example — valid mitigations for a processor library.

---

## 2. Source of truth & references

- **SPEC §1** (L37-85): the reliability bar ("no silent message loss", durable confirms) — the
  *reason* durable-by-default exists and the *reason* erasure is hard; "No silent credential
  leak" (L65) is **credentials only**, no PII parallel.
- **SPEC §6.1** (L467-763): `WithConnectionName` default `<binary>-<hostname>-<pid>` (L499/L667),
  client-properties broker-visibility + "never put secrets there" (L754-762), the ~512 MiB / 100
  MiB **pointer-out for size** (L703-726), Authentication — PLAIN vs `amqps://` + SASL EXTERNAL
  fail-closed (L728-762).
- **SPEC §6.2.1** (L1013-1068): the consumer dedupe cache keyed by `MessageID`; "**15 minutes of
  MessageID retention**" (L1056-1059) — a *retention window* on a store that holds whatever the
  user put in `MessageID`.
- **SPEC §6.3 / §6.6** (L1070-1369 / L1561-1753): DLQ / poison; `DeadLetter` expands
  `x-message-ttl`/`x-max-length` **on the source queue** (L1642-1654), and any `<Source>.dlq`
  **not already present is auto-appended** (L1677-1678) with **no TTL/bounds of its own**;
  `x-death` is broker-appended on dead-letter (L1696-1706) and mirrors the original properties.
- **SPEC §6.5** (L1431-1560): the `Message[M]` field inventory — `Body`, `MessageID`,
  `CorrelationID`, `UserID` (L1487-1513), `Headers`, `Expiration`/TTL (L1481-1485), `Delay`
  (node-local, non-replicated — L1536-1552), `DeliveryMode` default **Persistent** (L1450/L1554).
- **SPEC §6.9** (L1981-2208): `log` redacts **only** AMQP URIs (L2051-2054); default metric
  labels bounded + `routing_key`/`message_type` opt-in (L2059-2071); **OTel attributes**
  (L2096-2104) emit `messaging.message.id` (MessageID), `messaging.message.conversation_id`
  (CorrelationID), `messaging.message.body.size` (**size, not content**), `network.peer.*`;
  `traceparent`/`tracestate` propagated into headers → into the tracing backend; the
  trace-continuity-through-DLX contract (L2106-2168) is a cross-system data-flow record.
- **SPEC §8 Boundaries** (L2333-2423): "**Redact credentials**" (L2353-2358) — the *Never* list
  has **no** "never log message bodies / header values" entry; no at-rest-encryption note.
- **SPEC §10** (L3045-3119): R10-1 delayed-plugin node-local non-replicated store (L3045-3053);
  R10-10 unbounded auto-`<source>.dlq` → disk alarm (L3112-3119, owned by T65).
- **Reality (verified this pass):** the auto-`<source>.dlq` is declared with no TTL/length by
  default (DG-4); OTel emits size+IDs only, never body/header *content* (DG-3); there is **no API
  path** to delete a single in-flight message and **no** region/jurisdiction surface anywhere in
  the API (DG-5); "residency"/"jurisdiction"/"region" appears **nowhere** in SPEC/plan.
- **Lens prompt:** `spec-validation/11-compliance-gdpr-lgpd.md` (this pass *conducted* the
  review; this file is its remediation brief for `/plan`).

### Cross-lens reconciliation (do **not** re-file — project rule)

A finding already owned by a prior task **extends that task** with a `Lens-11 (DP-xx)` acceptance
bullet; it is **never** re-filed. The *confidentiality* half of this lens is dominated by
security-lens (07) overlaps — but the *data-protection-principle* half is genuinely unowned:

| Prompt-anticipated finding | Already owned by | Lens-11 disposition |
| --- | --- | --- |
| Unbounded, TTL-less auto-DLQ retains PII indefinitely (storage limitation) | **T65** (R10-10; +Lens-05 SRE-03 bound-by-default; +Lens-07 ST-08 adversarial flood) | **extend** T65 — add the **storage-limitation framing**: position the default DLQ bound **+ a default/recommended TTL** as the personal-data retention control (GDPR 5(1)(e)/LGPD Art. 16) |
| No "never log message bodies / header values" boundary (credential redaction ≠ PII redaction) | **T134** (Lens-07 ST-03/ST-14: add "Never log payloads" to §8 *Never*; panic-value-type-only) | **extend** T134 — frame the never-log-payloads boundary as a **data-protection control** (unlawful processing + retention of PII in logs; GDPR 5(1)(f)/Art. 32 / LGPD Art. 46) |
| Personal data in transit over plain `amqp://` (PLAIN cleartext) | **T132** (Lens-07 ST-01: PLAIN-over-`amqp://` `Dial`-time warning) | **extend** T132 — the warning must name **personal data in transit** as an Art. 32 / LGPD Art. 46 gap; discourage `amqp://` for any flow carrying personal data |
| Bodies are plaintext on broker disk + backups; spec silent → teams assume the bus encrypts | **T141** (Lens-07 ST-05: already plans the at-rest / message-level-payload-encryption boundary note in §6.9) | **extend** T141 — frame the at-rest boundary as the **Art. 32 "appropriate technical measures"** operator responsibility (GDPR Art. 32 / LGPD Art. 46) so silence stops implying the bus encrypts |
| The dedupe-cache retention window; a natural-key `MessageID` makes the cache a PII store | **T85** (Lens-02 DS-01/DS-15/DS-16: recommends bounding residency with a TTL → a finite retention window) | **extend** T85 — frame the retention window as **storage limitation** and warn that a natural-key `MessageID` turns the dedupe cache (and `x-death`/logs/traces) into a store of personal data |

**Net-new (no prior owner — the privacy-by-design layer):** the **erasure structural warning**
(DP-01), **pointer-out-as-privacy-control** positioning + runnable example (DP-02), the
**MessageID/CorrelationID/UserID/connection-name minimisation footguns** (DP-05/06/11/12), the
**international-transfer / data-residency caveat** (DP-09), the **trace-backend-as-a-processor /
records-of-processing** note (DP-10/DP-15), and the **§8/§9 "personal data should not flow as
message bodies — use a reference" first-class pattern** + the **LGPD-specific** notes (DP-13/14).

---

## 3. Constraints & working agreements (planner must honour)

1. **No re-file.** The 5 cross-lens findings extend their owning task in place; the 4
   confirmations (T07c URI redaction, T133 wrapped-error redaction, bounded metric labels, OTel
   size-not-content) add **no** new task — at most a do-not-regress bullet. Only DP findings with
   **no** owner become net-new tasks (T153–T156).
2. **Frame as a processor, never a controller.** Every finding reads "the design/default prevents
   the controller from complying" or "the default opts users into a non-compliant posture" —
   **not** "the library violates the law." Map every finding to a concrete **GDPR and LGPD
   article**; a privacy worry without a regulatory hook is not a finding (lens Rule 1).
3. **Gate-first.** T153 captures ground truth (DG-1..DG-5: the data-inventory ledger, the
   defaults audit, the no-PII-in-observability baseline, the DLQ-retention reality, the
   erasure/residency reality) **before** any §-wording change — mirroring
   T84/T94/T101/T111/T119/T130/T143/T147/T150. It records SPEC §10 **Rev 21** when it lands.
4. **Amend SPEC first, same PR.** Every §1/§6.x/§8/§9/§10 wording change is a SPEC amendment that
   lands *with* its task during execution (CLAUDE.md "amend SPEC first") — **not** in this
   `/spec` step and **not** in the subsequent `/plan` materialization step.
5. **No new build-tag lane.** This lens is documentation + one already-owned default change (T65)
   + a runnable example. The example rides the existing `examples-build`/`examples-smoke`
   targets; the DLQ-TTL default is asserted on the existing `integration` lane. Mirrors
   Lenses 04–09.
6. **Documentation-only is a valid mitigation for a library — but name where a default change is
   the stronger control** (lens Rule 3). The only default change here is the **bounded + TTL'd
   DLQ** (T65); say so explicitly. Everything else (erasure warning, pointer-out positioning,
   minimisation footguns, residency caveat) is correctly documentation, because a transport
   wrapper must not silently rewrite or drop a user's bodies/headers/IDs (§6.6 L1702-1706 — the
   "never strips, rewrites, or normalises headers" contract is load-bearing and **do-not-regress**).
7. **Do not over-claim scope.** Privacy guidance in a transport library has a ceiling: the
   library cannot perform erasure, cannot know which fields are personal data, and must not
   become a DLP product. The capstone (T156) must draw the **controller-vs-processor boundary**
   explicitly (D5) so the spec offers the *enabling patterns and caveats* without implying the
   library discharges the controller's duties.
8. **No code/SPEC/README/example edits in `/spec` or `/plan`.** This brief and the Phase-22
   materialization only touch the brief + the task ledger. README touch points to expect later
   (during execution): the examples table (a new `examples/pointer_out/`), the "reliability /
   privacy guarantees" section if one is added — checked per task.

---

## 4. Pre-work: data-protection gates (DG-1..DG-5 — sequence FIRST; they gate wording)

Capture ground truth so every later wording change is anchored to a measured fact, not an
assumption. Results (the §11 ledger + §12 audit) under `spec-validation/`. **T153 owns these and
records §10 Rev 21.** No behaviour change.

- **DG-1 — Data-inventory audit.** Enumerate **every field and metadata** the library writes or
  propagates (`Message[M]` fields, broker-appended `x-death`, the auto-injected
  `traceparent`/`tracestate`, `connection_name`, OTel attributes, metric labels) and classify each
  `not-personal-data` / `potentially-personal-data` / `identifier-linking-to-PII`, recording
  **where each persists** (wire, broker disk, DLQ copy, dedupe cache, logs, traces/APM, metric
  store, broker `client_properties`). Output: the **§11 ledger** filled from the *implemented*
  code. → gates DP-05/06/10/11/12.
- **DG-2 — Defaults audit (privacy-by-default, Art. 25 / LGPD Art. 6).** Rate **each default** for
  privacy-by-default impact: `DeliveryMode=Persistent`, the auto-`<source>.dlq` (no TTL/bounds),
  the absent never-log-PII boundary, the absent at-rest note, `MessageID=UUIDv7`, bounded metric
  labels, OTel size-not-content, `connection_name=<binary>-<hostname>-<pid>`, PLAIN-over-`amqp://`
  fail-open, codec lax-by-default. Output: the **§12 defaults audit**. → gates DP-03/04/07/08.
- **DG-3 — Observability no-PII-by-default baseline.** Inspect the `otel` + `metrics` code to
  confirm, as ground truth, that by default **only** `messaging.message.body.size` (size), the
  ID attributes, and infra addresses are emitted, and that **no body content or header value**
  ever becomes a span attribute or a default metric label. Establishes the **do-not-regress**
  invariant DP-04/DP-10 build on (so the never-log-PII boundary and the trace-as-processor note
  are about *user-controlled* content reaching the APM, not a library leak). → gates DP-04/10.
- **DG-4 — DLQ retention reality.** Confirm in `topology.go` that the auto-declared
  `<source>.dlq` is declared with **no `x-message-ttl` and no `x-max-length`** by default, and
  record precisely that `DeadLetter.TTL`/`MaxLength`/`MaxLengthBytes` bind the **source** queue
  (`x-message-ttl` on source — §6.6 L1642), **not** the DLQ itself. This is the storage-limitation
  ground truth anchoring T65's compliance bullet. → gates DP-03 (T65).
- **DG-5 — Erasure & residency reality.** Confirm there is **no API path** to selectively delete a
  single in-flight message (AMQP 0-9-1 has no such primitive; the only deletions are queue-purge
  and queue-delete, both all-or-nothing), and that quorum replication + `WithAddrs` failover + the
  delayed-plugin node-local store (R10-1) are **residency-opaque** — no region/jurisdiction
  appears anywhere in the public API. Establishes DP-01 (erasure) and DP-09 (residency) are
  **structural and documentation-only** (a processor cannot fix them in code). → gates DP-01/09.

---

## 5. Workstreams (grouped findings, sequenced)

### WS-1 — Erasure & storage limitation *(the headline — make survivability a first-class pattern)*

- **DP-01 (High) — the spec is silent that personal data in a message body is un-erasable at the
  bus layer.** GDPR Art. 17 (right to erasure) / LGPD Art. 18.VI (eliminação) + Art. 16 (término
  do tratamento). RabbitMQ has **no primitive to delete a single message**; once a body with
  personal data is published to a **durable** queue (`DeliveryModePersistent` is the zero-value
  default, §6.5 L1450/L1554) it is on disk, replicated (quorum), and copied to any **DLQ** on
  dead-letter. An erasure request for that data subject **cannot be satisfied** at the bus layer,
  and the spec never says so — a controller building on warren will not learn this until an
  erasure request arrives. **→ new task T154**: add a §8 boundary + a §6.5 note stating the
  limitation explicitly and routing the reader to the pointer-out pattern (DP-02).
- **DP-02 (High) — pointer-out is recommended only for size, not as the canonical privacy control.**
  §6.1 L724-726 + §10 L2952 recommend "treat the body as a pointer (S3 URL, object-store key),
  publish only the reference" **for payloads ≥ 100 MiB**. The identical pattern is the canonical
  control for **erasure** (delete the row in the store of record; the bus only ever held an opaque
  reference → effective erasure) and **breach minimisation** (GDPR Art. 33/34 / LGPD Art. 48 — a
  queue/DLQ dump leaks references, not personal data). It is the single most important privacy
  recommendation a bus library can make, and it is buried as a size optimisation. **→ T154**:
  promote pointer-out to a first-class privacy pattern in §6.5/§8 with a **runnable
  `examples/pointer_out/`** (reference-on-the-bus, PII-in-an-erasable-store-of-record), satisfying
  the prompt's "is it established as a first-class pattern with a runnable example?". **(D1)**
- **DP-03 (High) — storage limitation: TTL is positioned as a queueing nicety, and the default DLQ
  retains personal data indefinitely.** GDPR 5(1)(e) (storage limitation) / LGPD Art. 16. Two
  parts: **(a) positioning** — `Message.Expiration` / `x-message-ttl` / `DeadLetter.TTL` (§6.5
  L1481-1485, §6.6 L1650) are documented purely as TTL mechanics, never as the **retention control
  for personal data**; **(b) default** — the auto-declared `<source>.dlq` has **no TTL and no
  length bound** (DG-4), so dead-lettered personal data is retained **forever** by default. The
  default-change (bound + TTL the DLQ) is **already owned by T65** (R10-10/SRE-03/ST-08) — this
  lens adds the **storage-limitation framing**: T65 should add a **default or strongly-recommended
  DLQ TTL** (not just a length bound) and document it as the personal-data retention control;
  T154 adds the §6.5/§6.6 "TTL is your retention mechanism for personal data" positioning. **→
  extend T65** (default TTL + storage-limitation framing) **+ T154** (positioning). **(D2)**

### WS-2 — Data minimisation & privacy by default *(footguns the library can warn about)*

- **DP-05 (Med) — `MessageID`/`CorrelationID` are treated as opaque keys; a natural-key value
  becomes personal data in many stores.** GDPR 5(1)(c) (minimisation) + Art. 25 / LGPD Art. 6.III
  (necessidade). The UUIDv7 default is **not** PII (good — DG-2), but if a user sets a natural-key
  `MessageID` (email, CPF, user-id), it lands in: the **dedupe cache** (§6.2.1), **`x-death`** on
  dead-letter, **logs/traces**, the **OTel `messaging.message.id`** attribute (→ APM, DP-10), and
  the `(channel-instance-id, MessageID)` counter map (§6.3). `CorrelationID` similarly flows to
  **OTel `messaging.message.conversation_id`** (→ APM). The spec never warns **"never put personal
  data in `MessageID`/`CorrelationID`."** **→ new task T155** (the minimisation footgun §6.5 note)
  **+ extend T85** (the dedupe-cache-becomes-a-PII-store + retention-window aspect).
- **DP-06 (Med) — `UserID`/`StampUserID` embeds an identifiable principal into every message and
  its DLQ copy and broker logs, with no privacy note.** GDPR 5(1)(c)/Art. 25 / LGPD Art. 6.III.
  §6.5 L1487-1513 documents the `UserID` *footgun* exhaustively (broker 406, auth-backend
  rewrites) but says **nothing** about the privacy implication: `UserID` is frequently a **natural
  person's** authenticated identity, stamped into `basic.properties.user-id` → persisted on disk,
  copied to the DLQ, and written to broker logs. `StampUserID()` opts every message into carrying
  it. **→ T155**: add a minimisation note — `UserID` carries an identifiable principal; leave it
  empty unless the broker-side identity stamp is required.
- **DP-10 (Med) — the tracing backend is a processor; IDs + `traceparent` flow to a third party.**
  GDPR Art. 28 (processors) + Ch. V (transfers) / LGPD Art. 39 (operador) + Art. 33. §6.9
  L2096-2104 emit `messaging.message.id` and `messaging.message.conversation_id` as span
  attributes and propagate `traceparent`/`tracestate` into headers → all reach the configured APM
  (Jaeger/Datadog/…), frequently a **third-party SaaS in another jurisdiction**. If a user encodes
  identifiers in `MessageID`/`CorrelationID` (DP-05), they are exported there. The
  trace-continuity-through-DLX contract (§6.9 L2106-2168) — load-bearing and do-not-regress —
  *also* creates a cross-system personal-data trail (DP-15). **→ T155**: note the
  trace-backend-as-a-processor implication (the APM needs its own lawful basis + retention
  policy + transfer mechanism for whatever the user puts in IDs).
- **DP-11 (Low) — `connection_name` / `WithClientProperties` leak hostname broker-side.** GDPR
  5(1)(c) / LGPD Art. 6.III. The `WithConnectionName` default `<binary>-<hostname>-<pid>` (§6.1
  L499/L667) and `WithClientProperties` are visible via `rabbitmqctl list_connections` and land in
  the `ClientMetrics{connection_name}` label (§6.9 L2061); a hostname can reveal infrastructure or
  be personal data in some deployments. Minor, but a one-line minimisation note is warranted. **→
  T155** (folded with DP-06/DP-10/DP-12).
- **DP-12 (Low) — opt-in high-cardinality metric labels can carry identifiers.** GDPR 5(1)(c) /
  LGPD Art. 6.III. Default labels are PII-free (DG-3, do-not-regress), but
  `WithMetricsLabels(MetricsLabelRoutingKey, MetricsLabelMessageType)` (§6.9 L2067-2071) opt the
  user into labelling `routing_key`/`message_type`, which a user may have designed as identifying
  (e.g. routing key `user.<id>.orders`). **→ T155**: a one-line note at `WithMetricsLabels` that
  enabling these may export identifiers into the metrics store.

### WS-3 — Security of processing & international transfer *(confidentiality + residency)*

- **DP-04 (Med-High) — credential redaction is solved; PII redaction is not.** GDPR 5(1)(f)
  (integrity & confidentiality) + Art. 32 / LGPD Art. 6.VII + Art. 46. §8 L2353 mandates URI/
  credential redaction; there is **no** corresponding "**never log message bodies or header
  values**" boundary. A future debug log of a returned/failed/poison message would be unlawful
  processing **+ retention** of personal data in the log store. This is **already owned by T134**
  (Lens-07 ST-03/ST-14: add the never-log-payloads boundary + panic-value-type-only). **→ extend
  T134** with the data-protection framing (the boundary is a privacy control, not only a secrets
  control). **(confirm the code is already correct — T134 is a spec-lock + regression test.)**
- **DP-07 (Med) — at-rest encryption: bodies are plaintext on broker disk and in backups; the
  spec is silent.** GDPR Art. 32 (appropriate technical measures) / LGPD Art. 46. RabbitMQ does
  **not** encrypt bodies at rest by default; with durable-by-default, personal data sits in
  plaintext on disk and in backups. Silence lets teams assume the bus encrypts. **Already owned by
  T141** (Lens-07 ST-05 already plans the at-rest / message-level-payload-encryption boundary
  note). **→ extend T141**: frame it as the **Art. 32 operator-responsibility** boundary — bodies
  are plaintext on disk; disk/volume encryption is the operator's; message-level payload
  encryption (and, with it, crypto-erasure) is an application concern (→ LATER-47).
- **DP-08 (Med) — personal data in transit over plain `amqp://` is an Art. 32 gap.** GDPR Art. 32
  / LGPD Art. 46. `amqps://` + TLS is supported, but PLAIN over plain `amqp://` sends credentials
  *and message bodies* in cleartext. **Already owned by T132** (Lens-07 ST-01: PLAIN-over-`amqp://`
  `Dial`-time warning). **→ extend T132**: the warning + §6.1 text must name **personal data in
  transit** as the Art. 32 exposure and discourage `amqp://` for any flow carrying personal data.
- **DP-09 (Med) — international transfer / data residency is invisible at the API.** GDPR Ch. V
  Art. 44-49 / LGPD Art. 33. A **quorum queue replicates the body to every member node**;
  `WithAddrs` cluster failover (§6.1) and quorum membership can span regions/countries → publishing
  personal data can trigger an **international transfer** with its own legal-basis requirement. The
  **delayed-message plugin** stores scheduled messages **node-local** (R10-1, §6.5 L1536-1552) —
  *which* node, in which region, is non-obvious. Nothing in the API surfaces residency (DG-5). **→
  new task T156**: a documented data-residency caveat (§6.1/§6.5/§10) for regulated/multi-region
  deployments — name the transfer trigger; recommend keeping replication members and the delayed
  node within one jurisdiction (or pointer-out so only references cross borders). **(D4)**

### WS-4 — Accountability, breach & LGPD-specific *(capstone + records of processing)*

- **DP-13 (Med, high-leverage) — "personal data should not flow as bodies — use a reference" is
  not a first-class §8/§9 pattern.** GDPR Art. 25 (privacy by design) / LGPD Art. 6 + Art. 46 §2.
  The pointer-out pattern (DP-02) needs a home in the **Boundaries (§8)** *Always*/recommended
  list and a **§9 success criterion** that the runnable example exists and round-trips, so the
  recommendation is enforced, not buried in prose. **→ T154** (the §8 boundary + example) **+
  T156** (the §9 criterion in the capstone).
- **DP-15 (Low) — records of processing / accountability: the observability that aids debugging
  also creates a personal-data trail.** GDPR Art. 30 (records of processing) / LGPD Art. 37. The
  metrics/traces help a controller reconstruct *that* data flowed (useful for Art. 30 records),
  but the trace-continuity-through-DLX contract (§6.9) *also* creates a personal-data trail in the
  APM that **itself** needs a retention policy and a lawful basis. **→ T156**: a one-paragraph
  note on the dual nature (cross-link DP-10 and T141's trace-spoofing note); **confirm**
  do-not-regress on the contract itself.
- **DP-14 (Low) — LGPD-specific differences the spec should call out, given the Brazil-based
  owner.** Map every finding to its LGPD article (done, §13). Two LGPD specifics deserve an
  explicit note in the capstone: **(a)** the **ANPD breach-notification regime** (LGPD Art. 48 —
  notification in a "prazo razoável" defined by the ANPD) differs from GDPR Art. 33's fixed 72-hour
  clock; **(b)** LGPD's **lawful-basis set** (Art. 7 — ten bases, including *tutela da saúde* and
  *proteção do crédito*) differs from GDPR Art. 6's six. The library makes **no** lawful-basis or
  breach-timing claim (correct — that is the controller's), so this is a **pointer note**, not a
  control. **→ T156** (capstone): state that compliance specifics (breach timing, lawful basis)
  are the controller's and differ between GDPR and LGPD; warren provides only the enabling patterns.

---

## 6. Do-not-regress list (confirmed-correct — protect, do not re-file)

The library's privacy posture is **strong where confidentiality meets minimisation**; these must
survive the remediation:

- **URI credential redaction at a choke-point** (`internal/redact`, §8 L2353-2358; T07c) — every
  log/error/span/label that includes an AMQP URI has `userinfo` → `***`. Confirm; do not re-file.
- **Wrapped-error credential redaction** is **already owned** by T133 (Lens-07 ST-02 — the
  `amqp091`-wrapped dial error is re-redacted and will be e2e-tested). Confirm; no new task.
- **Default OTel attributes carry size, not content** (§6.9 L2101 `messaging.message.body.size`) —
  **no** span attribute carries body content or a header *value*. This is the minimisation-by-
  default invariant DG-3 captures; DP-04/DP-10 are about *user-controlled* content the user puts
  in IDs/bodies, not a library leak. Do-not-regress.
- **Default metric labels are bounded and PII-free** (§6.9 L2059-2065 — `addr` without userinfo,
  `role`, `connection_name`, `exchange`, `outcome`, `queue`); `routing_key`/`message_type` are
  **opt-in** (L2067-2071). Do-not-regress; DP-12 only adds a note at the opt-in.
- **The library never strips, rewrites, or normalises headers on the consume path** (§6.6
  L1702-1706; §6.9 L2122-2129) — load-bearing for trace continuity. This is *why* erasure is
  documentation-only (the library must not silently mutate a user's data); do **not** "fix" DP-01
  by having the library scrub fields.
- **`MessageID` default is UUIDv7, not a natural key** (§6.2.1 L1020) — a privacy-favourable
  default (random, not identifying). The footgun (DP-05) is only when the user overrides it.
- **`codec.NewJSONStrict()` exists for regulated/compliance pipelines** (§6.9 L1993-1996) — the
  strict-on-receive opt-in is already positioned for compliance workloads. Do-not-regress.
- **SASL EXTERNAL fail-closed + TLS hygiene** (§6.1 L728-762; T34b/T137) — the transport-identity
  controls a privacy reviewer wants are present; DP-08 only adds the personal-data-in-transit
  framing to T132's warning.

---

## 7. Routing matrix (each DP-01..DP-15 → disposition)

| Finding | Sev | GDPR / LGPD hook | Class | Disposition | Owner |
| --- | --- | --- | --- | --- | --- |
| DP-01 bodies un-erasable at the bus | High | Art. 17 / Art. 18.VI+16 | silence (documentation) | **new** | **T154** |
| DP-02 pointer-out mis-positioned as size-only | High | Art. 25/17 / Art. 6+46 | mis-positioned control | **new** | **T154** (+example) |
| DP-03 storage limitation: TTL nicety + unbounded DLQ | High | 5(1)(e) / Art. 16 | default + positioning | extend + new | **T65** (default TTL) + **T154** |
| DP-04 no never-log-PII boundary | Med-High | 5(1)(f)/32 / Art. 46 | silence (spec-lock) | extend | **T134** |
| DP-05 MessageID/CorrelationID PII footgun | Med | 5(1)(c)/25 / Art. 6.III | minimisation footgun | new + extend | **T155** (+**T85** cache aspect) |
| DP-06 UserID/StampUserID embeds principal | Med | 5(1)(c)/25 / Art. 6.III | minimisation footgun | **new** | **T155** |
| DP-07 at-rest plaintext; spec silent | Med | Art. 32 / Art. 46 | silence (boundary) | extend | **T141** |
| DP-08 personal data over plain `amqp://` | Med | Art. 32 / Art. 46 | silence (framing) | extend | **T132** |
| DP-09 international transfer / residency invisible | Med | Ch. V 44-49 / Art. 33 | silence (caveat) | **new** | **T156** |
| DP-10 trace backend is a processor | Med | Art. 28+Ch. V / Art. 39+33 | minimisation/transfer note | **new** | **T155** |
| DP-11 connection_name/client-properties hostname | Low | 5(1)(c) / Art. 6.III | minimisation note | **new** | **T155** |
| DP-12 opt-in metric labels carry identifiers | Low | 5(1)(c) / Art. 6.III | minimisation note | **new** | **T155** |
| DP-13 no first-class "use a reference" pattern | Med | Art. 25 / Art. 6+46 | positioning (§8/§9) | new | **T154** (§8/ex) + **T156** (§9) |
| DP-14 LGPD-specific (ANPD timing, lawful basis) | Low | 33/34 + 6 / Art. 48 + 7 | pointer note | **new** | **T156** |
| DP-15 records of processing / APM trail | Low | Art. 30 / Art. 37 | accountability note | **new** | **T156** (confirm §6.9) |
| LATER-47 message-level payload encryption / crypto-erasure | Low | Art. 17/32 / Art. 18.VI+46 | deferred | LATER | LATER-47 (prereq T154) |

**New:** T153 (gates DG-1..DG-5, Rev 21), T154 (erasure + pointer-out + storage-limitation
positioning + example), T155 (minimisation footguns), T156 (residency + §9 + LGPD-specific +
records-of-processing capstone). **Extended in place:** T65, T85, T132, T134, T141.
**Confirmed (do-not-regress):** T07c (URI redaction), T133 (wrapped-error redaction), bounded
metric labels, OTel size-not-content, the header-non-mutation contract.

---

## 8. Suggested phasing (planner may revise — "Phase 22")

**Phase 22 — Data-Protection Compliance Re-review (GDPR & LGPD) (Lens 11).** Highest existing task
= **T152** (Phase 21) → new IDs **T153–T156**. Current total = **161 tasks** (includes sub-lettered
tasks) → **165** after materialization. Highest filed `LATER` = **LATER-46** → **LATER-47**. SPEC
§10 Rev recorded when T153 lands = **Rev 21** (Rev = Phase − 1). Proposed tally after
materialization: **165 tasks / 22 phases**.

- **T153 — Data-protection gates DG-1..DG-5** (data-inventory ledger + defaults audit +
  no-PII-in-observability baseline + DLQ-retention reality + erasure/residency reality; **no
  behaviour change**). Capture ground truth (gate-first); fill the §11 ledger + §12 audit with
  *measured* facts from the implemented code; records §10 **Rev 21**. **[P0] · S**
- **T154 — Erasure & storage-limitation: pointer-out as the canonical privacy control + runnable
  example** (the headline, net-new). `SPEC.md §6.1/§6.5/§6.6/§8` + `examples/pointer_out/`:
  state the **un-erasable-on-the-bus** limitation (DP-01); promote **pointer-out** from a
  size-only tip to a first-class **privacy / erasure / breach-minimisation** pattern (DP-02) with
  a runnable example (PII in an erasable store of record, opaque reference on the wire); position
  `Expiration`/`x-message-ttl`/`DeadLetter.TTL` as the **retention control for personal data**
  (DP-03 positioning); add the §8 *Always*/recommended boundary "personal data should not flow as
  message bodies — use a reference" (DP-13). Coordinates with **T65** (the DLQ default TTL+bound)
  and **T85** (the dedupe-cache retention window). **[P1] · M** **(D1, D2)**
- **T155 — Data-minimisation footguns: IDs, principal, hostname, labels, trace-as-processor**
  (net-new). `SPEC.md §6.5/§6.9/§8`: warn **never to put personal data in
  `MessageID`/`CorrelationID`** (DP-05 — they flow to the dedupe cache, `x-death`, logs, and the
  OTel `messaging.message.id`/`conversation_id` attributes → APM); document the **`UserID` /
  `StampUserID` privacy implication** (DP-06 — embeds an identifiable principal into every message
  + DLQ copy + broker logs); add a **minimisation note** on `WithConnectionName`/
  `WithClientProperties` (DP-11, hostname) and on `WithMetricsLabels(routing_key/message_type)`
  (DP-12, identifiers); note the **trace-backend-as-a-processor** implication (DP-10 — the APM
  needs its own lawful basis + retention + transfer mechanism for whatever the user puts in IDs).
  Coordinates with **T85** (the dedupe-cache-becomes-a-PII-store aspect of DP-05). **[P1] · M**
- **T156 — International-transfer / residency caveat + §8/§9 compliance capstone + LGPD-specific
  notes** (net-new, lands last). `SPEC.md §6.1/§6.5/§9/§10`: document the **data-residency**
  caveat (DP-09 — quorum replicates the body to every member node; `WithAddrs` failover + the
  delayed-plugin node-local store can cross jurisdictions; residency is invisible at the API; name
  the international-transfer trigger and recommend single-jurisdiction membership or pointer-out);
  add the **§9 success criterion** that the `examples/pointer_out/` reference pattern exists and
  round-trips (DP-13); add the **records-of-processing / APM-trail** note (DP-15, cross-link
  DP-10 + T141); add the **LGPD-specific** pointer note (DP-14 — ANPD breach timing Art. 48 vs
  GDPR's 72h, lawful-basis set Art. 7 vs GDPR Art. 6, both the controller's responsibility) and
  the explicit **controller-vs-processor boundary** (D5). Asserts the WS-1..WS-3 controls landed.
  **[P2] · S** **(D4, D5)**

**Cross-lens extensions (acceptance bullets, not new tasks):** T65 (DP-03 — default/recommended
DLQ TTL + storage-limitation framing), T85 (DP-05 — dedupe cache as a PII store + retention window
as storage limitation), T132 (DP-08 — personal-data-in-transit framing), T134 (DP-04 —
never-log-payloads as a data-protection control), T141 (DP-07 — at-rest boundary as the Art. 32
operator responsibility). **Confirmations:** T07c (URI redaction), T133 (wrapped-error redaction),
bounded metric labels, OTel size-not-content, the header-non-mutation contract.

**Sequencing (A–D):**
- **A** Gates — T153 (records Rev 21; fills the §11 ledger + §12 audit from measured code).
- **B** Erasure & storage limitation — T154 (pointer-out + example + un-erasable warning + TTL
  positioning) + the **T65** default-TTL extension + the **T85** retention-window extension.
- **C** Minimisation & confidentiality — T155 (ID/principal/hostname/label/trace footguns) + the
  **T134** (never-log-PII), **T132** (in-transit), **T141** (at-rest) extensions.
- **D** Transfer & capstone — T156 (residency caveat + §9 criterion + records-of-processing +
  LGPD-specific + controller/processor boundary), lands last; asserts B–C.

Gate deps: DG-1→DP-05/06/10/11/12 (T155); DG-2→DP-03/04/07/08 (T65/T134/T141/T132); DG-3→DP-04/10
(do-not-regress baseline); DG-4→DP-03 (T65); DG-5→DP-01/09 (T154/T156). Gate-independent (pure
positioning/doc): DP-02, DP-13, DP-14.

---

## 9. Acceptance criteria for the whole effort

- [ ] **Gates first (DP gates):** T153 lands before any §-wording change; the §11 data-inventory
      ledger and §12 defaults audit are filled from *measured* code; §10 **Rev 21** recorded.
- [ ] **Erasure honesty (DP-01):** §8 + §6.5 state that personal data in a message body is
      effectively **un-erasable** at the bus layer (no single-message delete; durable + DLQ
      copies) and route the reader to pointer-out.
- [ ] **Pointer-out as a privacy control (DP-02/DP-13):** pointer-out is positioned in §6.5/§8 as
      the canonical **erasure + breach-minimisation** pattern (not only size); a runnable
      `examples/pointer_out/` ships and round-trips; a §9 criterion enforces it.
- [ ] **Storage limitation (DP-03):** §6.5/§6.6 position `Expiration`/`x-message-ttl`/
      `DeadLetter.TTL` as the **retention control for personal data**; **T65** gives the
      auto-`<source>.dlq` a default (or strongly-recommended) **TTL** in addition to its length
      bound, so dead-lettered personal data is not retained indefinitely by default.
- [ ] **Never-log-PII (DP-04):** the §8 *Never* list carries "never log message bodies or header
      values" framed as a data-protection control (extends T134); a test confirms no non-test path
      formats a body/header value into a log/error string.
- [ ] **Minimisation footguns (DP-05/06/11/12):** §6.5 warns never to put personal data in
      `MessageID`/`CorrelationID`; the `UserID`/`StampUserID` privacy implication is documented;
      `WithConnectionName`/`WithClientProperties` and `WithMetricsLabels` carry minimisation notes.
- [ ] **Confidentiality (DP-07/08):** the at-rest boundary (T141) is framed as the Art. 32 operator
      responsibility (bodies plaintext on disk + backups); the PLAIN-over-`amqp://` warning (T132)
      names personal-data-in-transit as the Art. 32 exposure.
- [ ] **Transfer/residency (DP-09):** §6.1/§6.5/§10 carry a data-residency caveat (quorum
      replication + `WithAddrs` failover + delayed node-local can cross jurisdictions; residency is
      invisible at the API; mitigations named).
- [ ] **Trace-as-processor & records (DP-10/DP-15):** §6.9 notes the APM is a processor receiving
      IDs + `traceparent` (needing its own lawful basis/retention/transfer) and the dual nature of
      the trace-continuity contract for Art. 30 / LGPD Art. 37 records.
- [ ] **LGPD-specific + scope (DP-14, D5):** the capstone states that breach timing (ANPD Art. 48
      vs GDPR 72h) and lawful basis (LGPD Art. 7 vs GDPR Art. 6) are the **controller's**, and
      draws the explicit controller-vs-processor boundary so warren never implies it discharges
      the controller's duties.
- [ ] **Every finding maps to a GDPR *and* LGPD article** (lens Rule 1); no finding re-files an
      already-owned task.
- [ ] `go build ./...` + `make lint` clean; `go test -race ./...` green; `make examples-build`
      green (incl. `examples/pointer_out/`); the example smoke-runs on the `integration` lane.
- [ ] **Ledger integrity:** five tasks extended in place (T65/T85/T132/T134/T141), four
      confirmations add no task, exactly one new `LATER.md` entry (LATER-47), T153–T156 contiguous
      with no duplicate IDs; tally **165 tasks / 22 phases**.

---

## 10. Open questions for the owner (decisions the planner needs to record)

- **D1 — Pointer-out: how strong a recommendation, and where (DP-02/DP-13)?** Recommended: promote
  it to a **first-class §8 *Always*/recommended pattern** *for personal data* (kept as a *should*,
  not a hard default — the library cannot know which bodies are personal data), **plus** a runnable
  `examples/pointer_out/` and a §9 criterion. Alternative: a prose note only (weaker — the prompt
  explicitly asks for a first-class pattern *with a runnable example*). *Recommend: §8 boundary +
  example + §9 criterion.*
- **D2 — DLQ default: TTL by default, or strongly-recommended TTL (DP-03)?** Recommended: T65
  already bounds the DLQ by *length* (drop-head/reject) by default for SRE-03/ST-08; **add a
  default or template TTL** (e.g. a documented default `x-message-ttl` on the auto-`<source>.dlq`,
  overridable) so dead-lettered personal data has a finite life by default — the *stronger*
  control per lens Rule 3. Alternative: keep TTL opt-in but **document it as the required
  retention control** (weaker, but avoids silently expiring DLQ entries operators expect to
  inspect). *Recommend: a conservative default TTL with prominent godoc + an opt-out, decided
  inside T65; this lens defers the exact value to the T65 owner.*
- **D3 — Crypto-erasure / message-level payload encryption now or LATER (overlaps T141)?**
  Recommended: **LATER-47.** A codec wrapper that encrypts the body with a per-subject key (delete
  the key on an erasure request → the ciphertext on the bus/DLQ/disk becomes unrecoverable =
  effective erasure without a delete primitive) is the elegant answer to DP-01, but it is a large,
  opinionated feature with key-management implications, and T141 already states message-level
  payload encryption is out of scope for v0.1. *Recommend: defer to LATER-47, prerequisite T154.*
- **D4 — Residency: caveat only, or a typed surface (DP-09)?** Recommended: **documented caveat
  only** for v0.1 — a transport wrapper cannot enforce node/region placement (that is broker/infra
  configuration). Alternative: a future advisory option to assert/log the connected node's
  region (large, broker-version-dependent). *Recommend: caveat in §6.1/§6.5/§10; note the typed
  surface as a possible v0.2 idea, not a task.*
- **D5 — Scope: how much privacy guidance belongs in a transport library (lens Q4)?** Recommended:
  the library provides **enabling patterns (pointer-out), defaults (bounded+TTL DLQ), and caveats
  (un-erasable, at-rest, residency, minimisation footguns)** — and **explicitly disclaims** the
  controller's duties (lawful basis, DPIA, breach timing, records of processing). The capstone
  (T156) must state this boundary so the spec never implies warren makes a system compliant.
  *Recommend: enabling-patterns-and-caveats + an explicit controller/processor boundary.*

---

## 11. Data-inventory ledger (lens output #2 — what the library writes / propagates)

Every field/metadata the library writes or propagates, classified and located. **Filled by
DG-1 from the implemented code.** Classification: `NP` not-personal-data · `PP`
potentially-personal-data (depends on what the user puts there) · `ID→PII`
identifier-linking-to-PII.

| Field / metadata | Source | Class | Persists where | Finding |
| --- | --- | --- | --- | --- |
| `Body` | user payload | **PP** | wire, broker disk (durable-by-default, plaintext), **DLQ copy**, backups | DP-01/02/03/07 |
| `MessageID` (UUIDv7 default) | lib default / user | NP default; **ID→PII** if natural key | wire, **dedupe cache**, `x-death`, logs, OTel `messaging.message.id` (→APM), counter map | DP-05/10 |
| `CorrelationID` | user | **PP / ID→PII** | wire, OTel `messaging.message.conversation_id` (→APM), DLQ copy | DP-05/10 |
| `UserID` | user / `StampUserID` | **ID→PII** (authenticated principal) | wire `basic.properties.user-id`, broker logs, DLQ copy | DP-06 |
| `Headers` (values) | user | **PP** | wire, broker disk, DLQ copy (values **not** emitted to OTel/metrics — DG-3) | DP-04 |
| `ReplyTo` / `Type` / `AppID` | user | **PP** (may be identifying) | wire, DLQ copy | DP-05 (minor) |
| `Timestamp` | lib default `time.Now()` | NP | wire | — |
| `traceparent` / `tracestate` | lib-injected (otel) | NP (trace id) — but a **data-flow record** | headers, DLQ copy, **APM backend** | DP-10/15 |
| `x-death` (broker-appended) | broker on dead-letter | **PP** (mirrors original properties) | DLQ message, logs if logged | DP-04/05 |
| `connection_name` `<binary>-<hostname>-<pid>` | lib default | **PP** (hostname) | broker `client_properties`, broker logs, `ClientMetrics{connection_name}` | DP-11 |
| `addr` (host:port) | connection | NP (infra) | metrics label (no userinfo), span `network.peer.*` | — (confirm redaction) |
| OTel `messaging.message.body.size` | lib (otel) | NP (**size, not content**) | APM backend | — (do-not-regress) |
| Metric labels `exchange`/`queue`/`outcome`/`role` | lib | NP | metrics store | — (do-not-regress) |
| Opt-in labels `routing_key`/`message_type` | user opt-in | **PP / ID→PII** if designed so | metrics store | DP-12 |
| AMQP URI userinfo (credentials) | connection | secret (not PII) | **redacted everywhere** (T07c/T133) | — (do-not-regress) |

**Conclusion:** the library's own additions are **minimisation-favourable by default** (UUIDv7,
size-not-content, bounded labels, redacted URIs); the personal-data exposure is driven by **what
the user places in `Body`/`Headers`/`MessageID`/`CorrelationID`/`UserID`** and where those then
persist — which is exactly what WS-1/WS-2 document and DP-04 prevents from leaking into logs.

---

## 12. Defaults audit (lens output #3 — privacy-by-default, Art. 25 / LGPD Art. 6)

Each default rated for privacy-by-default impact. **Filled by DG-2.**

| Default | Rating | Why | Disposition |
| --- | --- | --- | --- |
| `DeliveryMode = Persistent` (durable-by-default, §6.5 L1450/L1554) | **Adverse (justified)** | PII written to disk by default; with no delete primitive → un-erasable. Justified by §1 no-loss, but the privacy cost must be documented + pointer-out recommended. | document (T154); **do not** flip the default (reliability bar) |
| Auto-`<source>.dlq` no TTL / no length bound (DG-4, R10-10) | **Adverse** | Dead-lettered PII retained **indefinitely** (storage-limitation violation by default). | **default change** — T65 (bound + **TTL**) |
| No "never log bodies/headers" boundary (§8) | **Adverse (gap)** | Nothing prevents a future body/header debug log → unlawful processing + retention. | boundary + test (T134) |
| No at-rest-encryption note (§8/§6.9) | **Adverse (silence)** | Teams may assume the bus encrypts; bodies are plaintext on disk. | boundary (T141) |
| PLAIN over plain `amqp://` fail-open (§6.1) | **Adverse** | Personal data + credentials in cleartext in transit. | warning + framing (T132) |
| `connection_name = <binary>-<hostname>-<pid>` (§6.1) | **Mild-adverse** | Leaks hostname broker-side + into a metric label. | minimisation note (T155) |
| `MessageID = UUIDv7` (§6.2.1) | **Favourable** | Random, non-identifying default; footgun only on user override. | document the footgun (T155) |
| OTel attributes = size + IDs, **no body/header content** (§6.9, DG-3) | **Favourable** | Minimisation by default. | confirm (do-not-regress) |
| Metric labels bounded; `routing_key`/`message_type` opt-in (§6.9) | **Favourable** | Minimisation by default; opt-in can export identifiers. | note at opt-in (T155) |
| `codec` lax-by-default; `NewJSONStrict()` opt-in (§6.9) | **Neutral / favourable** | Strict opt-in already positioned for "regulated/compliance workloads". | confirm (do-not-regress) |
| URI userinfo redacted everywhere (§8, T07c/T133) | **Favourable** | Credential confidentiality by default. | confirm (do-not-regress) |

**Conclusion:** the defaults split cleanly — the **adverse** ones are either already owned by a
prior task (DLQ → T65; never-log → T134; at-rest → T141; PLAIN → T132) or pure documentation
(durable-by-default, connection_name); the **favourable** ones (UUIDv7, size-not-content, bounded
labels, strict-codec opt-in, redaction) are the privacy-by-default wins to protect. The one
**default change** this lens endorses is **T65's bounded + TTL'd DLQ** (storage limitation).

---

## 13. Compliance findings table (lens output #1)

Framed for a processor: "how the design/default impedes the **controller's** compliance" — never
"the library violates the law" (lens Rule 2).

| ID | Sev | GDPR art. | LGPD art. | Principle / right at stake | Location (§ + lines) | How the design/default impedes compliance | Recommended SPEC amendment | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **DP-01** | High | Art. 17 | Art. 18.VI, 16 | Erasure / término | §6.5 L1450/L1554; §8 | Personal data in a body on a durable queue/DLQ is un-erasable (no single-message delete); spec is silent → controllers cannot satisfy erasure | State the limitation in §8/§6.5; route to pointer-out | **T154** |
| **DP-02** | High | Art. 25, 17, 33/34 | Art. 6, 46, 48 | Privacy by design / breach min. | §6.1 L724-726; §10 L2952 | Pointer-out (the survivability control) is positioned **only for size** | Promote to a first-class privacy pattern + runnable example | **T154** |
| **DP-03** | High | 5(1)(e) | Art. 16, 15 | Storage limitation | §6.5 L1481; §6.6 L1650; DG-4 | TTL framed as a queueing nicety; auto-DLQ has no TTL → PII retained indefinitely | Position TTL as a retention control; **default/recommended DLQ TTL** | **T65** + **T154** |
| **DP-04** | Med-High | 5(1)(f), 32 | Art. 6.VII, 46 | Integrity & confidentiality | §8 L2353 | Only credentials are redaction-bound; no "never log bodies/headers" → unlawful log processing/retention | Add the never-log-payloads *Never* boundary + test | **T134** |
| **DP-05** | Med | 5(1)(c), 25 | Art. 6.III | Data minimisation | §6.2.1; §6.3; §6.9 L2099-2100 | A natural-key `MessageID`/`CorrelationID` becomes PII in the dedupe cache, `x-death`, logs, OTel attrs (→APM) | "Never put personal data in `MessageID`/`CorrelationID`" | **T155** + **T85** |
| **DP-06** | Med | 5(1)(c), 25 | Art. 6.III | Data minimisation | §6.5 L1487-1513 | `UserID`/`StampUserID` embeds an identifiable principal into every message + DLQ copy + broker logs; no privacy note | Add the minimisation note; leave `UserID` empty unless needed | **T155** |
| **DP-07** | Med | Art. 32 | Art. 46 | Security of processing | §8; §6.9 | Bodies plaintext on disk + backups; spec silent → teams assume encryption | Frame the at-rest boundary as the Art. 32 operator responsibility | **T141** |
| **DP-08** | Med | Art. 32 | Art. 46 | Security of processing | §6.1 L728-762 | Personal data over plain `amqp://` is cleartext in transit | Warning names personal-data-in-transit; discourage `amqp://` | **T132** |
| **DP-09** | Med | Ch. V 44-49 | Art. 33 | International transfer | §6.1; §6.5 L1536; §10 R10-1 | Quorum replication + `WithAddrs` failover + node-local delayed store can cross jurisdictions; residency invisible at the API | Documented residency caveat + mitigations | **T156** |
| **DP-10** | Med | Art. 28, Ch. V | Art. 39, 33 | Processor / transfer | §6.9 L2099-2104 | IDs + `traceparent` flow to a third-party APM (often another jurisdiction) | Note the trace-backend-as-a-processor implication | **T155** |
| **DP-11** | Low | 5(1)(c) | Art. 6.III | Data minimisation | §6.1 L499/L667; §6.9 L2061 | `connection_name` leaks hostname broker-side + into a metric label | One-line minimisation note | **T155** |
| **DP-12** | Low | 5(1)(c) | Art. 6.III | Data minimisation | §6.9 L2067-2071 | Opt-in `routing_key`/`message_type` labels can export identifiers to the metrics store | One-line note at `WithMetricsLabels` | **T155** |
| **DP-13** | Med | Art. 25 | Art. 6, 46 §2 | Privacy by design | §8; §9 | "Use a reference, not a body" is not a first-class boundary or success criterion | §8 boundary + example (T154) + §9 criterion (T156) | **T154** + **T156** |
| **DP-14** | Low | 33/34, 6 | Art. 48, 7 | Breach / lawful basis | §9; §10 | LGPD ANPD breach timing + lawful-basis set differ from GDPR; not noted | Pointer note (controller's responsibility; GDPR≠LGPD specifics) | **T156** |
| **DP-15** | Low | Art. 30 | Art. 37 | Records of processing | §6.9 L2106-2168 | The trace-continuity-through-DLX contract creates an APM personal-data trail needing its own retention | Note the dual nature; cross-link DP-10 + T141 | **T156** |

---

## 14. Out of scope for this plan

- **SPEC.md / code / README / example edits** — they land per-task during execution, same PR
  ("amend SPEC first"), **not** in this `/spec` step or the `/plan` materialization.
- **Performing erasure / scrubbing fields in the library** — the library must **not** strip,
  rewrite, or normalise bodies/headers/IDs (§6.6 L1702-1706 do-not-regress); erasure is a
  controller duty satisfied via the store of record (pointer-out) or crypto-erasure (LATER-47).
- **Message-level payload encryption / crypto-erasure** — **LATER-47** (D3; T141 states it is out
  of scope for v0.1).
- **The DLQ bound/durability *mechanism*** — owned by **T65** (R10-10/SRE-03/ST-08); this lens
  adds only the storage-limitation framing + the default-TTL ask.
- **The never-log-payloads / PLAIN-warning / at-rest-boundary *controls*** — owned by **T134 /
  T132 / T141** (Lens-07); this lens adds only the data-protection framing.
- **The dedupe *mechanism* and persistent-dedupe example** — owned by **T85** (Lens-02); this lens
  adds only the storage-limitation + PII-store framing.
- **A typed residency/region API surface** — caveat only for v0.1 (D4); a possible v0.2 idea, not
  a task.
- **Lawful basis, DPIA, breach-timing, records-of-processing *as library features*** — controller
  duties; the capstone (T156) disclaims them (D5).
- **Other lenses** (12 DX/documentation, 13 load-testing).
- **Committing** — a separate, explicit user request (the two-commit-per-lens convention would
  later add `docs: add compliance-gdpr-lgpd lens remediation plan-brief for /plan` for this brief
  + `docs(tasks): add Phase 22 compliance-gdpr-lgpd re-review (Lens 11)` for the ledger).

---

## 15. Verdict for this lens

**GO-WITH-CHANGES — no Blocker.**

The library does the hard confidentiality work well — credentials are redacted at a choke-point,
default observability carries **size not content** and **bounded, PII-free labels**, the codec
ships a strict opt-in *for compliance workloads*, and the demanding security-of-processing
controls (never-log-payloads, PLAIN warning, at-rest boundary) are **already owned** by the
security lens (T134/T132/T141). It is a *processor* and behaves like one. What it lacks is the
**privacy-by-design layer above confidentiality**: it recommends the single pattern that makes
personal data on a bus *survivable* — **pointer-out** — only as a size optimisation, and it is
**silent** on the structural realities a controller must know (bodies are **un-erasable** on a
durable bus, **plaintext** at rest, potentially **transferred across jurisdictions** by quorum
replication, and turned into PII stores when a natural-key `MessageID`/`UserID` lands in the
dedupe cache, `x-death`, logs, and the APM). None of this is a defect or a message-loss bug — the
one *default change* worth making (a bounded **and TTL'd** DLQ for storage limitation) is already
owned by T65; everything else is documentation, a positioned control, and one runnable example.
The remediation is therefore **positioning + breaking silence**: promote **pointer-out** to a
first-class **erasure / breach-minimisation** pattern with an example; reframe **TTL** as the
personal-data **retention** control and give the auto-DLQ a default TTL (T65); add the
**minimisation footgun** warnings (IDs, principal, hostname, labels, trace-as-processor); add the
**at-rest / in-transit / residency** framings to the owning security tasks; and draw the explicit
**controller-vs-processor boundary** so the spec offers the enabling patterns and caveats without
ever implying warren makes a system GDPR/LGPD-compliant. **4 net-new tasks (T153–T156), 5
cross-lens extensions (T65/T85/T132/T134/T141), 4 confirmations, LATER-47.**
