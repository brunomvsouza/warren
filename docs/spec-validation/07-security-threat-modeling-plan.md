# Plan Input — Remediate Security & Threat-Modeling Findings (Lens 07)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> security & threat-modeling lens (`spec-validation/07-security-threat-modeling.md`).
> Like Lenses 03/04/05/06, no findings report pre-existed — this brief was produced
> by *conducting* the review: every trust boundary was enumerated, every egress path
> (log, metric label, error string, span attribute, panic-recover value,
> returned-message handler) was audited for credential- and payload-safety, and
> every untrusted-input ingress (consumed body, broker headers incl. `x-death`,
> `basic.return` data, broker error strings) was audited for size-boundedness and
> panic-safety. It enumerates confirmed threats (`ST-01..ST-14`), their evidence
> (SPEC §+line *and* `file:line`), the assumed attacker capability, the
> exploit/leak scenario, and a recommended control, grouped into workstreams and
> sequenced by dependency.
>
> **Numbering:** new task IDs are **T130–T142** (highest existing is T129, after
> Phase 17). Lens 07 becomes **Phase 18**, mirroring how Lenses 01/02/03/04/05/06
> became Phases 12/13/14/15/16/17. **One** prior-lens task is **extended in place**
> with a `Lens-07` acceptance bullet (cross-lens rule: shared findings extend the
> shared task, never re-filed) — **T65** (the unbounded auto-DLQ → disk-fill, which
> is a DoS-by-poison-storm as much as an SRE finding). **One new `LATER.md` entry**
> (LATER-42: configurable SASL EXTERNAL principal extraction beyond CN), with a
> conditional LATER-43 gated on the T135 owner decision. **No new build-tag lane**
> (the gates TG-1..TG-5 ride the existing unit + integration lanes; the OOM and
> reconnect-auth probes are broker-bound on the existing `integration` lane). The
> first Phase-18 task records SPEC §10 **Rev 17**.
>
> **Lens verdict: GO-WITH-CHANGES.** The security posture is fundamentally sound —
> the `internal/redact` choke-point holds on the egress paths the wrapper owns
> (incl. wrapped errors via `redact.Error`), the codec panic-recover is **type-only**
> (`%T`, never the recovered value's content), the library never logs message
> bodies and never decompresses, every internal buffer is bounded (dispatcher by
> prefetch, confirm tracker by batch size), `403 ACCESS_REFUSED` is permanent and
> never auto-retried on publish, SASL EXTERNAL is **fail-closed**-validated, and
> `UserID` is client-side-checked. But the review found **one must-fix Blocker** —
> a *fail-open inbound denial-of-service*: there is **no consume-side message-size
> cap** (`MaxMessageSizeBytes` is publish-side only), so a single hostile or buggy
> producer can ship a ~512 MiB body that `amqp091-go` reassembles **in memory**
> before the codec runs, OOMing the consumer — plus **one High** fail-open
> confidentiality gap (PLAIN credentials sent base64-cleartext over `amqp://` with
> no warning, while EXTERNAL's TLS requirement *is* fail-closed-validated) and a set
> of egress-hardening, transport-hygiene, supply-chain, and test corrections. No
> redesign is required; the boundary is closed where it is currently fail-open and
> the silent risks are made explicit and testable.

---

## 1. Objective

Validate `SPEC.md` from the seat of an **application-security engineer who assumes
the broker is compromised, the network is hostile, and producers are
attacker-controlled**. The lens bar is concrete and binary: *every security control
must be fail-closed, every egress path must be credential- **and** payload-safe,
and every untrusted-input ingress must be size-bounded and panic-safe.* A
documentation-only mitigation for a runtime risk is a **partial** control — the
runtime guard is the real control. The two highest-severity classes are
**fail-open-where-fail-closed-is-required** and **unbounded-untrusted-input**.

Concretely, the plan must:

1. **Close the inbound DoS hole (ST-06, Blocker).** `MaxMessageSizeBytes` (§6.2
   L796, default 16 MiB) is a **publish-side** guardrail only. On consume there is
   **no** body-size cap: `amqp091-go` reassembles a message up to RabbitMQ's
   ~512 MiB practical limit **in memory** before warren's codec ever runs
   (`FrameMax` bounds individual frames, not the reassembled body; the dispatch
   channel is bounded by *count*, not bytes — `consumer.go:476`). One hostile or
   buggy producer with publish rights to the queue OOMs the consumer. This is a
   **fail-open** availability gap at the "billions/day, hostile producers" bar —
   the security analog of the Lens-06 GA-01 Blocker. Add a consume-side guard
   (default-on, fail-closed) that rejects an oversized delivery *before* the codec.

2. **Close the cleartext-credential hole (ST-01, High).** Decision 35 (L2876)
   makes SASL EXTERNAL **fail-closed** — `Dial` rejects EXTERNAL without TLS. But
   the symmetric exposure — **PLAIN credentials over plain `amqp://`** (password
   base64-encoded, trivially decoded on the wire) — is **fail-open**: no warning,
   no opt-in. At billions/day across data centers a plaintext-credential
   connection is a real on-path-eavesdropper exposure. Warn at `Dial` (owner
   decides warn-vs-require-opt-in).

3. **Make every egress payload-safe, not just credential-safe (ST-02/ST-03/ST-14).**
   §8 "redact credentials" (L2353) is the only egress boundary. It is **silent on
   never-logging-payloads** (bodies carry PII/secrets; `Return.Body` is raw bytes
   to `OnReturn`), the redaction guarantee for **wrapped underlying errors** is
   aspirational (no e2e test against the real `amqp091` dial-error string, which is
   the realistic credential carrier; the `amqp091` package `Logger` is an un-owned
   egress), and §6.9's panic-recover wording "wrapping the recovered value"
   (L2047) invites a payload leak the **code already avoids** (it stores `%T` only).
   Lock the safe behaviour into the spec and back it with grep + e2e tests.

4. **Harden transport & identity (ST-04/ST-11).** EXTERNAL derives the principal
   from the client cert **CN only** (`connection.go:122`), but RabbitMQ's
   `ssl_cert_login_from` is configurable (CN / DN / SAN); the existing R10-4 caveat
   (L3070) covers username-*rewriting* backends but not the CN-vs-SAN **extraction**
   divergence, so the client-side `UserID` check can mis-fire. And TLS is passed
   verbatim (correct) but there is **no runtime warning** when `InsecureSkipVerify=true`
   on an `amqps://` connection (doc-only = partial control).

5. **Shrink the supply-chain surface (ST-10).** `github.com/cloudevents/sdk-go/v2`
   is an **unconditional direct dependency** (`go.mod:6`) every warren user pulls,
   for a codec most won't use — a large transitive attack surface. Build-tag or
   sub-module the heavy codecs so the core stays dependency-light.

6. **Make the residual risks explicit and the controls testable (ST-05/ST-07/ST-08/
   ST-09/ST-12/ST-13).** Document the accepted trace-context-spoofing risk; state the
   decompression-bomb boundary (lib never decompresses — caller's responsibility);
   pull the unbounded-auto-DLQ disk-fill into its availability framing (cross-lens,
   T65); specify reconnect behaviour on a **permanent** auth failure (no infinite
   403 loop / auth-backend log-spam DoS); close the §9 security-success-criteria gap
   (no inbound-DoS / cleartext / body-logging / e2e-redaction / `InsecureSkipVerify`
   tests today); and extend fuzzing to the `AMQPCode` reply-code translation and the
   field-table encoder (both parse attacker-influenced input).

This is **remediation of an existing spec + a small number of runtime guards**, not
a new feature. The only Blocker (ST-06) is a fail-open runtime hole on
already-implemented consume paths; everything else is a runtime warning, a doc
boundary, a supply-chain split, a fuzz/test addition, or a spec correction that
locks behaviour the code already gets right.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing** (project convention; the anchors below are this-pass snapshots).
  - §1 Reliability bar L37–86 (no silent credential leak L65–67; no silent
    backpressure L48–52).
  - §6.1 Connection: `SASLMechanism`/`SASLExternal` L477–483; Frame/message-size
    limits L703–727; **Authentication** L728–763 (PLAIN L730–732; EXTERNAL
    fail-closed validation **L738–751**; TLS/`client_properties` hygiene **L754–762**).
  - §6.2 Publisher: `MaxMessageSizeBytes` **L796** (publish-side guard);
    `Return.Body` raw bytes **L808**.
  - §6.3 Consumer L1070; dispatch/prefetch model (no inbound byte cap).
  - §6.5 Message: `UserID` client-side check + auth-rewrite caveat **L1487–1513**;
    `Headers` field-table typing **L1515–1534**; `ContentEncoding` (metadata-only);
    durable-by-default wire mapping L1554–1559.
  - §6.8 Errors L1863 (reply-code classification; `403` permanent).
  - §6.9 Subpackages L1981: codec panic-safety contract **L2044–2049**; `log`
    adapters route through `internal/redact.URI` **L2051–2054**; metrics labels
    (`addr` host:port-no-userinfo) **L2059–2065**; OTel propagation.
  - §7 Testing / fuzz targets (`FuzzCodecJSON`/`FuzzCodecProtobuf`/
    `FuzzCodecCloudEventsBinary`; `FuzzRedactURI`; `FuzzXDeathParser`).
  - §8 Boundaries: **Always** "Redact credentials" **L2353–2358**; **Ask first**
    "Adding a new external dependency" **L2391**; **Never** "Commit credentials"
    L2412 (no "never log payloads" entry) **L2410–2422**.
  - §9 Success Criteria: SASL EXTERNAL fail-closed + `UserID` client-side
    **L2543–2548** (the only security criteria today).
  - §10 Decisions: **35** SASL EXTERNAL fail-closed **L2876**; **38** `AMQPCode`
    covers 312/313 **L2895**; **39** `UserID` client-side **L2902**; **R10-4** auth
    rewrite + TLS hygiene **L3067–3072**; Rev 10 L3031.

- **Code (ground truth confirmed this pass — extends, never duplicates):**
  - `internal/redact/redact.go:21` `URI(s)` (regex strips `userinfo`); `redact.Error`
    re-redacts an error chain preserving `errors.Is/As`; `FuzzRedactURI` exists.
  - `connection.go:812` raw `amqp091.DialConfig`; `connection.go:397`
    `return redact.Error(lastErr)` (wrapped dial errors **are** re-redacted —
    the §8 guarantee is met in code, but not spec-pinned for *wrapped* errors nor
    e2e-tested against the real driver string).
  - `connection.go:122` `computeAuthUser` → EXTERNAL principal = **CN** of first
    client cert; `(*Connection).AuthenticatedUser()` L209.
  - `publisher.go:586` `safeEncodeBody` / `consumer.go:801` `safeDecodeConsumer` /
    `consumer.go:736` `safeCallHandler` / `batch_consumer.go:470` — every codec &
    handler call recovers to `ErrInvalidMessage` with `%T` **type only** (no
    payload content).
  - `consumer.go:476` `make(chan amqp091.Delivery, int(c.prefetch))` — dispatch
    buffer bounded by prefetch (count, **not bytes**); **no consume-side size cap
    anywhere**.
  - `errors.go:251` `ErrAccessRefused` (403) in the **permanent** set →
    `internal/amqperror/translate.go:29` maps 403; never auto-retried on publish.
  - `options_connection.go:114` `WithTLSConfig` passes `*tls.Config` verbatim;
    library never sets `InsecureSkipVerify`, never sets a min TLS version, never
    warns; never imports `compress/*` (no auto-decompression).
  - `go.mod:6` direct deps: `cloudevents/sdk-go/v2`, `google/uuid`,
    `prometheus/client_golang`, `rabbitmq/amqp091-go`, `otel`, `otel/trace`,
    `google.golang.org/protobuf` (+ `testify`, `goleak` test-only). CloudEvents is
    **unconditional**.
  - Fuzz: `FuzzRedactURI`, `FuzzXDeathParser`, `FuzzCodecJSON`/`...Strict`,
    `FuzzCodecCloudEventsBinary`, `FuzzCodecProtobuf`. **No** `AMQPCode` fuzz; the
    field-table encoder is not directly fuzzed.
  - Not yet implemented (planned): `rpc.go`, `delay.go`, `amqptest/` (T37),
    `conformance/`.

### Cross-lens reconciliation (do **not** re-file — project rule)

One Lens-07 finding is already owned by a prior-lens task and must **extend** it
with a `Lens-07 (ST-xx)` acceptance bullet, never spawn a new task:

| Lens-07 finding | Already owned by | Action |
|---|---|---|
| **ST-08** unbounded auto-DLQ → disk-fill → broker-wide `connection.blocked` (DoS-by-poison-storm) | **T65** (Phase 11; already carries the **Lens-05 (SRE-03)** bullet — highest blast radius) | **Extend T65** with a `Lens-07 (ST-08)` *availability/DoS* bullet: frame the unbounded DLQ as an attacker-reachable resource-exhaustion vector (one service's poison storm → cluster-wide publish outage), reinforcing the default DLQ bound + depth signal. Do **not** re-file. |

Coordination (not re-file — distinct findings that touch adjacent code):
- **ST-13** (fuzz the `AMQPCode` parser + field-table encoder) coordinates with
  **T98** (Lens 03, extends `FuzzCodecJSON` for `int64`) — same fuzz surface area,
  different targets; T140 adds the new targets, cites T98.
- **ST-10** (codec dependency split) coordinates with the Lens-03 (interop) and
  Lens-06 (library-design) codec work — but the *security/supply-chain* framing
  (transitive attack surface) is net-new; T135 owns it.
- **ST-09** (reconnect on **permanent** auth failure) coordinates with the
  reconnect-supervisor tasks **T61/T63/T66** (Lens 05) and the channel-vs-connection
  reply-code annotation **T79** (Lens 02/12) — but "does the supervisor loop forever
  on a 403" is an unaddressed behaviour; T138 owns it and cites them.

---

## 3. Constraints & working agreements (planner must honour)

1. **Amend SPEC before code, same PR** (CLAUDE.md). Each task: update the cited
   §/decision, then add the runtime guard/test. The first Phase-18 task records
   §10 **Rev 17**.
2. **Cross-lens shared findings extend the owning task** — ST-08 → T65. Never
   re-file.
3. **Fail-closed is the default for new guards.** The inbound size cap (T131)
   rejects oversized deliveries *before* the codec and surfaces a classifiable
   error (`Surface, do not swallow`, §8 L2359) — never a silent drop or a silent
   OOM.
4. **Runtime guard beats documentation for a runtime risk.** Where a doc-only
   mitigation exists today (PLAIN-cleartext, `InsecureSkipVerify`), the task adds
   the runtime warning/guard and keeps the doc.
5. **Additive-only on the public surface** (pre-v1, §8 Ask-first). New builder
   options (`MaxInboundMessageSizeBytes`) and Dial-time warnings are additive and
   non-breaking. A codec **sub-module** split (T135) IS breaking (import-path
   change) → requires the §8 "Ask first" gate; a **build-tag** split is not — the
   owner decision (D3) chooses.
6. **No new build-tag lane** — gates ride the existing unit + `integration` lanes
   (3.13 and 4.x where broker-bound), mirroring Lenses 04/05/06.
7. **testify + goleak** in every `_test.go`; route all new broker-touching errors
   through `internal/amqperror` and all URI-formatting through `internal/redact`
   (do not create ad-hoc redaction).
8. **Distinguish the three states per finding** (lens rule): *insecure* (fail-open
   hole — ST-06, ST-01), *silent on a known risk* (ST-03, ST-07, ST-09), *risk
   accepted & boundary stated* (ST-05 once documented). The brief labels each.

---

## 4. Pre-work: verification gates (TG-1..TG-5 — sequence FIRST; they gate wording/fixes)

Each gate captures **ground truth** on a live broker / the real driver before any
SPEC wording or control lands (gate-first, mirroring T84/T94/T101/T111/T119). No
downstream task edits the spec for its finding before its gate returns. Results go
into a committed table under `spec-validation/` (cited by each downstream task).

| Gate | Question (ground truth to capture) | Lane | Blocks |
|------|-----------------------------------|------|--------|
| **TG-1** | Does a real `amqp091.Dial`/`DialConfig` **failure** with `amqp://user:secret@badhost` surface `secret` anywhere warren returns or logs — the wrapped error chain (`.Error()` string), a `log` adapter line, **and** the `amqp091` package-level `Logger` (the egress the wrapper doesn't own)? Capture the raw driver error string shape. | unit + integration | ST-02 / T133 |
| **TG-2** | Publish a single ~256 MiB body to a queue a warren consumer is subscribed to; measure consumer RSS as `amqp091` reassembles it **before** the codec runs. Confirm there is no consume-side cap and quantify the OOM headroom (does it scale to `FrameMax`-independent body size?). | integration | ST-06 / T131 |
| **TG-3** | Confirm EXTERNAL principal extraction = **CN** of the first client cert (`connection.go:122`). Characterise the client-side `UserID` check when the broker's `ssl_cert_login_from` is set to `common_name` vs `distinguished_name` vs `subject_alternative_name`: does the check diverge (false `ErrInvalidMessage` reject, or a value the broker then 406s)? | integration | ST-04 / T136 |
| **TG-4** | Force a reconnect against revoked/denied credentials (or a vhost the user lost access to): does the supervisor loop on **403 `ACCESS_REFUSED`** indefinitely (auth-backend log-spam DoS + silent stall), or does it surface a permanent error / degrade with bounded backoff? Capture the current loop behaviour and timing. | integration | ST-09 / T138 |
| **TG-5** | `go list -deps ./...` and `go mod graph`: quantify the transitive package/module surface `cloudevents/sdk-go/v2` adds to a **core** (non-cloudevents) import, and confirm whether a user can avoid it today (it is a direct, unconditional require). | unit/tooling | ST-10 / T135 |

Findings without a live gate (design/doc/test, ground truth already known from the
code audit): ST-01 (PLAIN-cleartext — design), ST-03/ST-14 (payload-safe egress —
grep + unit test), ST-05 (trace-spoofing — doc), ST-07 (decompression boundary —
doc), ST-11 (`InsecureSkipVerify` warning — unit test), ST-12 (§9 criteria —
capstone), ST-13 (fuzz — unit).

---

## 5. Workstreams (grouped findings, sequenced)

### WS-0 — Cross-lens findings (record + add a `Lens-07` bullet; do **not** re-file)

- **ST-08** (Med, availability) → **extend T65**. The unbounded auto-DLQ is an
  attacker-reachable resource-exhaustion vector: a producer that floods a queue
  with poison messages fills the auto-declared `<source>.dlq`, fills disk, and
  trips broker-wide `connection.blocked` — a cluster-wide publish outage caused by
  one service. T65 already bounds the DLQ by default (Lens-05 SRE-03); the Lens-07
  bullet adds the *threat-model* framing and a test asserting the bound holds under
  an adversarial poison flood (not just an accidental one).

### WS-1 — Inbound denial-of-service *(the must-fix core — fail-open → fail-closed)*

- **ST-06** (Blocker, High/Critical at the bar) → **T131**. Add a consume-side
  `MaxInboundMessageSizeBytes` guard on `ConsumerBuilder`/`BatchConsumerBuilder`
  (default 16 MiB to mirror the publish guard; `0` disables — explicit opt-out).
  An oversized delivery is rejected **before** the codec runs, fail-closed:
  `Nack(requeue=false)` + a classifiable `ErrMessageTooLarge` (routed to DLQ if one
  is wired, so it is observable, not silently dropped) + a drop metric. §6.2/§6.3
  amend; the §6.2 L703–727 frame-size prose gains an explicit "frame-max bounds
  frames, not the reassembled body — the inbound cap is the body guard" note.
  *dep TG-2.*
- **ST-07** (Med, availability) → **T139**. Decompression-bomb boundary: the lib
  never decompresses (`ContentEncoding` is metadata-only; no `compress/*` import).
  State in §6.5/§8 that decompression is the **caller's** responsibility, recommend
  a bounded (`io.LimitReader`-wrapped) decompressor, and note the T131 inbound cap
  applies to the **compressed** wire body (pre-inflation) — so the cap alone does
  not bound the inflated size; the caller must bound that too. *Doc; rides T131.*

### WS-2 — Credential & payload confidentiality *(egress hardening)*

- **ST-01** (High, confidentiality) → **T132**. PLAIN over plaintext: emit a
  `Dial`-time warning (via the `log` adapter) when `WithAuth`/PLAIN is used over a
  non-TLS `amqp://` endpoint — the password travels base64-cleartext. Document the
  exposure in §6.1 Authentication alongside the EXTERNAL fail-closed block. Owner
  decides warn-only (recommended for v0.1, no behaviour break) vs an opt-in
  acknowledgement (`AllowInsecureAuth()` / `WithInsecureCleartextAuth()`). The
  warning is the minimum; this closes the fail-open asymmetry with decision 35.
- **ST-02** (Med, confidentiality) → **T133**. Guarantee redaction of **wrapped
  underlying errors**: §8 L2353 already says "every error message that includes an
  AMQP URI" — make it explicit that this covers errors *wrapped from `amqp091`*
  (not only wrapper-formatted strings), back it with an **end-to-end test** (real
  dial failure with creds → assert no `secret` in the returned error string, any
  `log` line, or the `amqp091` `Logger` output), and **neutralise or document the
  `amqp091` package `Logger`** (the egress the wrapper doesn't own — pin it to a
  redacting adapter or a no-op, or document that callers who enable it must redact).
  *dep TG-1.*
- **ST-03 + ST-14** (Med + Low-Med, confidentiality) → **T134** (payload-safe
  egress). Two payload-leak boundaries the code already respects but the spec
  doesn't pin:
  - **ST-03:** add **"Never log message payloads / bodies"** to the §8 *Never* list
    (today only credentials are listed); `Return.Body` and `Delivery` bodies may
    carry PII/secrets. Add a §9 success criterion: a grep/AST test that no non-test
    code path formats a body into a log/error string, plus a runtime test that
    `OnReturn` and the decode-error path emit no body bytes.
  - **ST-14:** correct §6.9 L2047 "wrapping the recovered value" → **"wrapping the
    recovered value's *type* only — never its content"** (the code stores `%T`; the
    current wording blesses a payload leak via a panic message). Lock with a test
    that a codec panicking with a payload-bearing value yields an error string
    containing no payload bytes.

### WS-3 — Transport & identity hygiene *(partial controls → runtime controls)*

- **ST-04** (Med, integrity) → **T136**. EXTERNAL principal extraction: document
  in §6.5/§6.1 + decision 35 that warren extracts the principal from the cert
  **CN** and that RabbitMQ's `ssl_cert_login_from` (CN / DN / SAN) must match, or
  the client-side `UserID` check diverges; extend the R10-4 caveat (L3070) to cover
  the **extraction** divergence (not only username-rewriting backends); recommend
  leaving `UserID` empty under non-CN broker mappings. Configurable extraction
  (SAN/DN) is deferred → **LATER-42**. *dep TG-3.*
- **ST-11** (Low-Med, confidentiality) → **T137**. TLS floor + `InsecureSkipVerify`
  runtime warning: state in §6.1 that warren relies on Go's default min TLS (1.2+)
  and never overrides the caller's `*tls.Config`; emit a `Dial`-time warning when
  `InsecureSkipVerify=true` is detected on an `amqps://` connection (a partial
  doc-only mitigation today, L758–759 → a runtime control). Non-breaking.

### WS-4 — Supply chain & dependency surface *(new)*

- **ST-10** (Med, availability/supply-chain) → **T135**. Split the heavy codecs so
  the **core** stays dependency-light: build-tag the CloudEvents codec (and assess
  Protobuf) behind `//go:build` tags, or move `codec/cloudevents` to a sub-module,
  so a user who never uses CloudEvents does not pull `sdk-go/v2`'s transitive
  surface. §2 (Tech Stack deps) + §6.9 amend. Owner decides build-tag (non-breaking,
  recommended for v0.1) vs sub-module (breaking, needs §8 Ask-first) vs accept +
  document (defer the split → **LATER-43**). *dep TG-5.*

### WS-5 — Threat-model documentation & test/fuzz hardening *(new)*

- **ST-09** (Med, availability) → **T138**. Specify reconnect on a **permanent**
  auth failure: the supervisor must not loop on `403 ACCESS_REFUSED` indefinitely
  (auth-backend log-spam DoS + a silent stall masquerading as a transient outage);
  on a permanent auth/authorization failure during re-dial or redeclare it surfaces
  `ErrAccessRefused`, applies bounded backoff or stops, and fires the degraded
  signal (`WithOnTopologyDegraded`-style). §6.1/§6.8 amend; cites T61/T63/T66/T79.
  *dep TG-4.*
- **ST-13** (Low, integrity) → **T140**. Extend fuzzing to the attacker-influenced
  broker-header parsers not yet covered: add `FuzzAMQPCode` (the
  `internal/amqperror` reply-code translation over a malformed `*amqp091.Error`)
  and a field-table encoder fuzz/round-trip (`message.go` typing path). §7 amend;
  coordinate with T98. *Confirms `FuzzXDeathParser` + `FuzzCodecCloudEventsBinary`
  already cover their surfaces.*
- **ST-05** (Low, integrity) → **T141**. Document the accepted trace-context
  spoofing risk: caller/upstream headers win last-wins over warren's injected
  `traceparent`/`tracestate` (§6.9 L2033–2042), so a hostile producer can forge or
  oversize them (trace poisoning). State it in the threat model: **accepted** under
  producer-trust; trace context MUST NOT be used for security/authorization
  decisions. *Doc — risk-accepted-and-stated.*
- **ST-12** (Low, high-leverage) → **T142** (capstone). Close the §9
  security-success-criteria gap (today only credential-grep + EXTERNAL + `UserID`,
  L2543–2548). Add criteria/tests for: inbound size cap (ST-06), PLAIN-cleartext
  warning (ST-01), never-log-payloads (ST-03/ST-14), e2e wrapped-error redaction
  (ST-02), `InsecureSkipVerify` warning (ST-11), and the new fuzz targets (ST-13).
  Depends on WS-1..WS-5 landing the controls.

---

## 6. Do-not-regress list (confirmed-correct — protect with tests)

These are the controls the audit found **already correct**. Reverting any flips the
lens toward NO-GO; each must keep (or gain) a guard test.

1. **Redaction choke-point holds on owned egress.** `internal/redact.URI` strips
   `userinfo`; `redact.Error` re-redacts wrapped error chains preserving
   `errors.Is/As`; the wrapped dial error is redacted at `connection.go:397`; all
   `log` adapters route through `redact.URI` (§6.9 L2051). `FuzzRedactURI` exists.
2. **Codec/handler panic-recover is type-only.** Every codec & handler call
   recovers to `ErrInvalidMessage` with `%T` (`publisher.go:586`,
   `consumer.go:736/801`, `batch_consumer.go:470`) — the recovered value's *content*
   is never logged. (T134/ST-14 only locks the spec wording to match.)
3. **The library never logs message bodies** (audit: CONFIRMED-none) and **never
   decompresses** (no `compress/*` import). (T134/T139 only pin the boundary.)
4. **All internal buffers are bounded.** Dispatch channel by prefetch
   (`consumer.go:476`); confirm tracker by `PublishBatchMaxSize`; pool exhaustion
   surfaces `ErrChannelPoolExhausted` (no unbounded queue). (ST-06 is about the
   *reassembled body*, not these buffers.)
5. **`403 ACCESS_REFUSED` is permanent and never auto-retried on publish**
   (`errors.go:251`, `internal/amqperror/translate.go:29`). (ST-09 is about the
   *reconnect supervisor*, a different loop.)
6. **SASL EXTERNAL is fail-closed-validated** (decision 35, §6.1 L738–751, §9
   L2543) and **`UserID` is client-side-checked** before the frame is written
   (decision 39, §6.5 L1492). (ST-04 only refines the principal-extraction caveat.)
7. **Metric labels are URI-free.** `addr` is host:port-no-userinfo (§6.9 L2060);
   no other label carries a URI; high-cardinality labels are opt-in.
8. **TLS is passed verbatim** — warren never weakens the caller's verification and
   never sets `InsecureSkipVerify` (`options_connection.go:114`). (ST-11 only adds
   a warning when the *caller* sets it.)
9. **Attacker-influenced `x-death` is fuzzed** (`FuzzXDeathParser`); CloudEvents
   binary mapping is fuzzed (`FuzzCodecCloudEventsBinary`). (ST-13 adds the two
   uncovered parsers.)

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Class | Disposition | Task |
|---------|-----|-------|-------------|------|
| **ST-06** inbound size cap (fail-open OOM) | **High/Critical** | insecure (fail-open) | NEW — consume-side `MaxInboundMessageSizeBytes`, fail-closed | **T131** (Blocker; dep TG-2) |
| **ST-01** PLAIN cleartext over `amqp://` | High | insecure (fail-open) | NEW — Dial-time warning (+ optional opt-in) | **T132** |
| **ST-02** wrapped-error redaction unverified | Med | silent risk | NEW — spec guarantee + e2e test + amqp091 Logger | **T133** (dep TG-1) |
| **ST-03** no "never log payloads" boundary | Med | silent risk | NEW — §8 Never + grep/runtime test | **T134** |
| **ST-14** panic-value egress wording | Low-Med | silent risk | NEW — §6.9 type-only correction + test | **T134** |
| **ST-04** EXTERNAL CN-only principal | Med | underspecified | NEW — doc + caveat; config → LATER-42 | **T136** (dep TG-3) |
| **ST-11** `InsecureSkipVerify` doc-only | Low-Med | partial control | NEW — Dial-time warning + TLS-floor note | **T137** |
| **ST-10** CloudEvents unconditional dep | Med | supply-chain | NEW — build-tag/sub-module split | **T135** (dep TG-5) |
| **ST-09** reconnect loop on permanent 403 | Med | underspecified | NEW — bounded/stop + surface + degrade | **T138** (dep TG-4) |
| **ST-07** decompression-bomb boundary | Med | silent risk | NEW — doc boundary (rides T131) | **T139** |
| **ST-13** AMQPCode/field-table fuzz gap | Low | test gap | NEW — `FuzzAMQPCode` + field-table fuzz | **T140** (coord T98) |
| **ST-05** trace-context spoofing | Low | risk-accepted | NEW — threat-model note | **T141** |
| **ST-12** §9 security criteria insufficient | Low | test gap | NEW — capstone criteria/tests | **T142** |
| **ST-08** unbounded auto-DLQ disk-fill | Med | availability/DoS | **CROSS-LENS — extend T65 (Lens-07 bullet)** | **T65** |

Every `ST-01..ST-14` is accounted for exactly once: ST-01→T132, ST-02→T133,
ST-03→T134, ST-04→T136, ST-05→T141, ST-06→T131, ST-07→T139, ST-08→T65 (cross-lens),
ST-09→T138, ST-10→T135, ST-11→T137, ST-12→T142, ST-13→T140, ST-14→T134.

---

## 8. Suggested phasing (planner may revise — "Phase 18")

- **A — Gates (T130).** Stand up TG-1..TG-5 ground truth into a committed results
  table; records §10 **Rev 17**. No new build-tag lane.
- **B — Must-fix availability (T131 + cross-lens T65/ST-08).** Inbound size cap
  (the Blocker) + the auto-DLQ DoS-framing bullet on T65.
- **C — Confidentiality egress (T132, T133, T134).** PLAIN-cleartext warning,
  wrapped-error redaction guarantee+test, payload-safe egress (never-log-bodies +
  panic-value-type-only).
- **D — Transport / identity / supply chain (T135, T136, T137, T138).** Codec
  dependency split, EXTERNAL principal doc, `InsecureSkipVerify` warning, reconnect
  on permanent auth failure.
- **E — Boundaries & tests (T139, T140, T141, T142).** Decompression boundary,
  fuzz additions, trace-spoofing note, §9 security-criteria capstone.

Sequencing notes: T131 depends on TG-2; T133 on TG-1; T136 on TG-3; T138 on TG-4;
T135 on TG-5. T134/T137/T139/T140/T141 are gate-independent (doc/test). T142 lands
last (it asserts the controls A–E added). T65/ST-08 is independent of the gates.

---

## 9. Acceptance criteria for the whole effort

- **Inbound DoS closed (ST-06):** an oversized inbound delivery (> the configured
  `MaxInboundMessageSizeBytes`) is rejected before the codec with
  `ErrMessageTooLarge` + `Nack(requeue=false)` + a drop metric; an integration test
  publishes an over-cap body and asserts the consumer's RSS stays bounded and the
  message lands in the DLQ (when wired), not an OOM. Default-on (16 MiB); `0`
  disables explicitly.
- **Cleartext-auth warning (ST-01):** a `Dial` with `WithAuth` over `amqp://`
  emits a warning through the `log` adapter; a unit test asserts the warning fires
  and that EXTERNAL-over-`amqp://` remains a fail-closed `ErrInvalidOptions`.
- **Egress is credential- and payload-safe (ST-02/ST-03/ST-14):** an e2e test dials
  a bad host with `amqp://user:secret@…` and asserts `secret` appears in no returned
  error, no `log` line, and no `amqp091` `Logger` output; a grep/AST test asserts no
  non-test code formats a body into a log/error; a panic-with-payload test asserts no
  payload bytes in the resulting error string. §8 *Never* lists "log message
  payloads"; §6.9 says "type only".
- **Transport/identity (ST-04/ST-11):** §6.5/decision 35 document the CN extraction
  + `ssl_cert_login_from` divergence and recommend empty `UserID` under non-CN
  mappings; a `Dial` with `InsecureSkipVerify=true` on `amqps://` emits a warning
  (unit-tested).
- **Supply chain (ST-10):** a build/import test proves a core (non-cloudevents)
  import does **not** pull `cloudevents/sdk-go/v2` (build-tag) — or the split is
  consciously deferred to LATER-43 with the surface documented in §2.
- **Reconnect on permanent auth (ST-09):** a chaos/integration test revokes creds,
  forces a reconnect, and asserts the supervisor surfaces `ErrAccessRefused` +
  bounded backoff/stop + the degraded signal — not an unbounded 403 loop.
- **Residual risks stated/tested (ST-05/ST-07/ST-13):** trace-spoofing and
  decompression boundaries documented; `FuzzAMQPCode` + field-table fuzz added and
  green.
- **§9 capstone (ST-12):** the new security success criteria are present and each
  has a backing test.
- **Cross-lens (ST-08):** T65 carries a `Lens-07 (ST-08)` bullet and a test
  asserting the default DLQ bound holds under an adversarial poison flood.
- **Project gates green:** `make lint test` (race + cover), `goleak.VerifyNone`,
  integration on 3.13 **and** 4.x. `README.md` reflects the changed external
  contract (the inbound size cap, the cleartext-auth warning, the codec build-tag,
  the `InsecureSkipVerify` warning).

---

## 10. Open questions for the owner (decisions the planner needs to record)

1. **D1 — ST-06 inbound cap default & disposition.** Recommend
   `MaxInboundMessageSizeBytes` default **16 MiB** (mirrors the publish guard), `0`
   disables; over-cap → `ErrMessageTooLarge` + `Nack(requeue=false)` (DLQ if wired)
   + drop metric. *Strongly recommend shipping in v0.1 — it is the Blocker.*
   Open: should the cap also apply to `BatchConsumer` per-message vs per-batch?
   (Recommend per-message, consistent with the publish guard.)
2. **D2 — ST-01 cleartext auth: warn vs require opt-in.** Recommend **warn-only at
   Dial** for v0.1 (does not break local/dev `amqp://` usage), document loudly; an
   opt-in acknowledgement (`AllowInsecureAuth()`) considered for v1.0. Owner may
   choose require-opt-in now if the deployment bar justifies it.
3. **D3 — ST-10 codec split mechanism.** Recommend **build-tag** the CloudEvents
   codec (non-breaking, keeps the core light) for v0.1; a **sub-module** split is
   breaking (import-path change) and needs the §8 "Ask first" gate; or **accept +
   document** and defer to **LATER-43**. Decision drives whether T135 is code or
   doc-only.
4. **D4 — ST-04 EXTERNAL principal extraction.** Recommend **doc-only for v0.1**
   (document the CN assumption + `ssl_cert_login_from` divergence, recommend empty
   `UserID` under non-CN mappings); configurable SAN/DN extraction deferred to
   **LATER-42**. Owner may pull the configurable extraction forward if EXTERNAL
   deployments need SAN today.
5. **D5 — ST-09 reconnect on permanent auth.** Recommend **stop + surface
   `ErrAccessRefused` + degraded signal** (do not loop) on a permanent auth failure
   during reconnect; confirm against TG-4's observed current behaviour before
   choosing stop-vs-bounded-retry.

**Proposed LATER entries:** **LATER-42** — configurable SASL EXTERNAL principal
extraction (SAN/DN beyond CN), prereq T136. **LATER-43** (conditional on D3 =
defer) — full codec sub-module split to remove `cloudevents/sdk-go/v2` from the
core dependency closure, prereq T135.

---

## 11. Egress-path audit (lens output #2)

Every credential/PII egress path, marked **safe** / **leaky** / **unverified**.

| Egress path | Carries | Status | Evidence / control |
|-------------|---------|--------|--------------------|
| `log` adapter lines | AMQP URI | **safe** | all adapters route through `redact.URI` (§6.9 L2051); `FuzzRedactURI` |
| Wrapper-formatted error strings | AMQP URI | **safe** | `redact.Error` at `connection.go:397` |
| **Wrapped `amqp091` dial error** (`.Error()`) | AMQP URI (driver-embedded) | **unverified** | re-redacted in code, but **no e2e test** vs the real driver string → **ST-02/T133** |
| **`amqp091` package `Logger`** (driver's own logging) | AMQP URI | **unverified/leaky** | egress the wrapper doesn't own → **ST-02/T133** (pin/redact/document) |
| Metric labels (`addr`, `exchange`, `queue`, `connection_name`, opt-in `routing_key`/`message_type`) | host:port; user strings | **safe** | `addr` is host:port-no-userinfo (§6.9 L2060); no label carries a URI |
| OTel span attributes (`network.peer.address`) | host:port | **safe (verify in TG-1 capture)** | host:port only by design (§6.9) |
| `WithClientProperties` | broker-visible | **safe (documented)** | §6.1 L760 "never put secrets there" (broker-side visible; not a client egress) |
| Codec/handler **panic-recover value** | payload bytes | **safe (code) / spec-leaky (wording)** | code stores `%T` only; §6.9 L2047 "wrapping the recovered value" → **ST-14/T134** |
| **Message bodies in logs** (`Return.Body`, decode-error path) | PII/secrets | **safe (code) / unbounded (spec)** | audit: never logged today; §8 has no "never log payloads" → **ST-03/T134** |

---

## 12. Ingress-DoS audit (lens output #3)

Every untrusted-input path, marked **bounded** / **unbounded** / **unverified**.

| Ingress path | Attacker control | Status | Evidence / control |
|--------------|------------------|--------|--------------------|
| **Consumed message body** (pre-codec) | any producer w/ publish rights | **UNBOUNDED** | no consume-side cap; `amqp091` reassembles ≤ ~512 MiB in memory; dispatch chan bounded by *count* not bytes (`consumer.go:476`) → **ST-06/T131 (Blocker)** |
| Compressed body (`ContentEncoding`) | producer | **unbounded (inflated)** | lib never decompresses; caller may inflate 1 KiB→GB; boundary unstated → **ST-07/T139** |
| Dispatch channel | producer rate | **bounded** | capacity = prefetch (`consumer.go:476`) |
| Confirm tracker | publish rate | **bounded** | by `PublishBatchMaxSize` |
| Channel pool | concurrency | **bounded** | `ErrChannelPoolExhausted` (no unbounded queue) |
| Auto-DLQ (`<source>.dlq`) | poison flood | **unbounded → bounded by T65** | disk-fill → broker-wide `connection.blocked` → **ST-08 / T65 (cross-lens)** |
| `x-death` broker header | compromised/buggy broker | **bounded + panic-safe** | parsed defensively; `FuzzXDeathParser` |
| `basic.return` data (`Return.Body`) | broker | **bounded + panic-safe** | raw bytes to `OnReturn`; not logged |
| Broker error string (`*amqp091.Error`) | broker | **panic-safe / fuzz-gap** | translated via `internal/amqperror`; **no `FuzzAMQPCode`** → **ST-13/T140** |
| Field-table encode (`Headers`) | caller (+ relayed producer) | **panic-safe / fuzz-gap** | typed at publish; encoder not directly fuzzed → **ST-13/T140** |
| Codec decode (JSON/Proto/CE) | producer | **panic-safe** | recover → `ErrInvalidMessage` (`%T`); `FuzzCodec*` |
| Reconnect on revoked creds (403) | broker/operator | **unverified** | supervisor loop behaviour unspecified → **ST-09/T138** |
| Trace headers (`traceparent`/`tracestate`) | upstream producer | **spoofable (accepted)** | last-wins; forgeable → **ST-05/T141** |

---

## 13. Threat table (lens output #1)

`Severity` Critical/High/Medium/Low · `Category` C(onfidentiality)/I(ntegrity)/A(vailability).

| ID | Sev | Cat | Trust boundary | Attacker capability assumed | Location (§+lines / file:line) | Exploit / leak scenario | Recommended control | Task |
|----|-----|-----|----------------|-----------------------------|-------------------------------|-------------------------|---------------------|------|
| **ST-06** | **High** (Critical at bar) | A | broker→consumer (consumed body) | a producer with publish rights to the queue (hostile or buggy) | §6.2 L796 publish-only; §6.3; `consumer.go:476` | ship a ~512 MiB body; `amqp091` reassembles it in memory before the codec → consumer OOM/crash; fail-open | consume-side `MaxInboundMessageSizeBytes` (default 16 MiB), reject before codec, fail-closed | **T131** |
| **ST-01** | High | C | network (wire) | on-path passive eavesdropper between client and broker | §6.1 L730–751; dec.35 L2876 | `WithAuth` over `amqp://` sends password base64-cleartext; EXTERNAL is TLS-fail-closed but PLAIN-plaintext is fail-open, no warning | Dial-time warning (+ optional opt-in) for PLAIN over non-TLS | **T132** |
| **ST-02** | Med | C | error/log egress | reads logs/error output; the un-owned `amqp091` `Logger` | §8 L2353; §6.9 L2051; `connection.go:397/812` | a wrapped driver dial error / the driver's own logger embeds `amqp://user:pass@host`; redaction is aspirational + untested e2e | spec guarantee for wrapped errors + e2e test + pin amqp091 `Logger` | **T133** |
| **ST-03** | Med | C | log egress (future) | reads logs | §8 Never L2410; `Return.Body` L808 | a future debug log of a returned/failed message emits PII/secret body bytes; no boundary forbids it | add "Never log payloads" to §8; grep + runtime test | **T134** |
| **ST-14** | Low-Med | C | panic-recover value egress | crafts input that panics a third-party codec | §6.9 L2047 | spec wording "wrapping the recovered value" blesses including payload bytes from a panic in the error string | §6.9 "type only"; lock with test (code already safe) | **T134** |
| **ST-04** | Med | I | identity (cert principal) | controls a cert whose CN≠broker-mapped principal | §6.5 L1501–1513; dec.35; `connection.go:122` | broker maps `ssl_cert_login_from=SAN` but warren checks CN → client-side `UserID` check false-rejects or false-passes | document CN + divergence; config → LATER-42 | **T136** |
| **ST-11** | Low-Med | C | transport config | MITM when caller set `InsecureSkipVerify` | §6.1 L754–762; `options_connection.go:114` | `InsecureSkipVerify=true` on `amqps://` defeats cert validation silently; doc-only mitigation | Dial-time warning + TLS-floor note (partial→runtime) | **T137** |
| **ST-10** | Med | A | supply chain | compromise of a transitive dep | §2 L107–119; §8 L2391; `go.mod:6` | `cloudevents/sdk-go/v2` is pulled by every user for an unused codec → large transitive attack surface | build-tag/sub-module split; keep core light | **T135** |
| **ST-09** | Med | A | reconnect (revoked creds) | broker/operator revokes creds mid-flight | §6.1 reconnect; §6.8; `internal/amqperror` | supervisor loops on 403 `ACCESS_REFUSED` forever → auth-backend log-spam DoS + silent stall | stop/bounded + surface `ErrAccessRefused` + degrade | **T138** |
| **ST-07** | Med | A | consumed body (compressed) | producer | §6.5 ContentEncoding; `message.go:38` | gzip body inflates 1 KiB→GB on caller decompress; boundary unstated; T131 cap is pre-inflation | document caller responsibility + bounded decompressor | **T139** |
| **ST-08** | Med | A | poison storm → disk | producer flooding poison | dec.R10-10; **T65** | unbounded auto-DLQ fills disk → broker-wide `connection.blocked` (cluster-wide publish outage) | **cross-lens — extend T65** (default bound + adversarial-flood test) | **T65** |
| **ST-13** | Low | I | attacker-influenced broker headers | compromised/buggy broker | §7; `internal/amqperror`; `message.go` | malformed `*amqp091.Error` / field-table not fuzzed → undiscovered parser panic/misclassification | `FuzzAMQPCode` + field-table fuzz | **T140** |
| **ST-05** | Low | I | consumed headers | upstream producer | §6.9 L2033–2042 | forged/oversized `traceparent`/`tracestate` (trace poisoning); last-wins over injected | document accepted risk; never trust trace ctx for security | **T141** |
| **ST-12** | Low | C/I/A | (test surface) | — | §9 L2543–2548 | security criteria miss inbound-DoS / cleartext / body-logging / e2e-redaction / `InsecureSkipVerify` | add §9 criteria + tests (capstone) | **T142** |

---

## 14. Out of scope for this plan

- Other validation lenses (08 concurrency, 09 performance, 10 test-strategy, 11
  compliance, 12 DX, 13 load) — separate passes.
- OAuth2 / `rabbitmq_auth_backend_oauth2` (already §6.1 v0.2 out-of-scope).
- Configurable SASL EXTERNAL principal extraction (SAN/DN) → **LATER-42**.
- Full codec sub-module split if D3 defers → **LATER-43**.
- Stream-protocol security (v0.2, decision 24).
- Encryption-at-rest / message-level payload encryption (application concern, not a
  transport-wrapper concern) — explicitly out of scope; note the boundary in T141's
  threat-model section.
- Any change to the §10 API north stars beyond the corrections above. Cross-lens
  shared findings extend the shared task (T65/ST-08), never re-filed.

---

## 15. Verdict for this lens

**GO-WITH-CHANGES.** The library's security architecture is sound where it counts:
the credential-redaction choke-point holds on the egress paths the wrapper owns,
the codec/handler panic-recover is payload-safe (type-only), bodies are never
logged and never decompressed, every internal buffer is bounded, `403` is permanent
and never publish-retried, SASL EXTERNAL is fail-closed-validated, and `UserID` is
client-side-checked. But the lens bar — *fail-closed controls and bounded
untrusted input under a hostile broker, network, and producers* — exposes **one
must-fix Blocker** (ST-06: a fail-open inbound DoS — no consume-side size cap →
single-message OOM) and **one High** fail-open confidentiality gap (ST-01: PLAIN
credentials sent cleartext over `amqp://` with no warning, asymmetric to the
fail-closed EXTERNAL validation). The remaining twelve findings make silent risks
explicit and turn partial (doc-only) controls into runtime controls. Closing ST-06
and ST-01, hardening the egress (ST-02/03/14), and shipping the documented
boundaries and tests brings the library to a defensible posture at the billions/day
hostile-producer bar. No redesign required.
