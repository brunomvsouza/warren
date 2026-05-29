# Plan Input — Remediate Interoperability / Wire-Format Findings (Lens 03)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> interoperability / wire-format lens
> (`spec-validation/03-interoperability-wire-format.md`). Unlike Lens 01/02, no
> findings report pre-existed — this brief was produced by *conducting* the review
> (tracing every codec / field-table claim against a non-Go-client round-trip). It
> enumerates confirmed defects (`IW-01..IW-13`), their evidence, the desired
> outcome, and an acceptance test for each, grouped into workstreams and sequenced
> by dependency.
>
> **Numbering:** new task IDs are **T94–T100** (highest existing is T93). Lens 03
> becomes **Phase 14**, mirroring how Lens 01 became Phase 12 and Lens 02 became
> Phase 13. One `LATER.md` entry is added: **LATER-39**.

---

## 1. Objective

Bring `SPEC.md` into honest alignment with the project's own
`feedback_codec_interop` rule — **codec and wire-format decisions must be grounded
in non-Go-client interop, never warren↔warren convenience** — at the §1 "billions
of messages per day" bar, which for a polyglot estate means *cross-language* wire
fidelity, not just Go round-trips.

Concretely, the plan must:
1. Correct the confirmed **interop overclaims** (amend SPEC first): the CloudEvents
   binary-mode 0-9-1→AMQP-1.0 bridge guarantee (IW-01), the unqualified field-table
   "matched 1:1" claim for cross-client decoding (IW-08).
2. Flag the **silent-lossy wire mappings** the spec never names: `time.Time`→`T`
   second-precision/no-TZ truncation (IW-07), JSON `int64` > 2^53 precision loss on
   decode into `any` (IW-04).
3. Fill the **interop hazards the spec is silent about**: Decimal `D` / unsigned
   field-table types' low cross-client support (IW-09/IW-10), the proto3 multi-type
   discrimination gap (IW-05), the protobuf media-type-string choice (IW-06).
4. Close the **structural test gap**: there is **no non-Go interop test** — every
   cross-language claim is asserted, not proven (IW-13). Stand up a polyglot harness.
5. Sharpen the **one test-quality defect**: the ContentType/ContentEncoding swap
   test only catches a swap with distinct non-empty values (IW-11).

This is **remediation of an existing spec**, not a new feature. For every
cross-language claim the "test" is a **round-trip through a non-Go client**
(Python `pika`, a Java client, an AMQP-1.0 CloudEvents SDK) on a live broker — not
a Go↔Go mock. **Lens verdict: GO-WITH-CHANGES** (no active §1 message-loss bug;
the defects are interop overclaims, two silent-lossy header/number mappings, and
the absence of a polyglot test).

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing.** Sections referenced: §1 (reliability bar L37–85), §6.5 (Message
  properties L1434–1473; `ContentType`/`ContentEncoding` L1467–1473; Headers
  field-table typing L1515–1535), §6.9 (codecs L1981–2049 — Protobuf L1997,
  CloudEvents binary L2011–2019, structured L2006–2010; OTel propagation
  L2093–2152), §9 (success criteria L2426–2549; `rabbitmqadmin get` swap test
  L2496–2510), §10 (decisions 4 L2573–2588, 10 L2625–2652, 13 L2676–2681,
  23 L2765–2778; highest Rev note **Rev 10** L3031).
- Implementation (already shipped — findings amend SPEC + add tests/examples, they
  do not gate on unwritten code): `codec/json.go` (`NewJSON` lax, `NewJSONStrict`;
  `json.Decoder.Decode` **without `UseNumber()`** — numbers into `any` become
  `float64`), `codec/protobuf.go` (`application/x-protobuf`), `codec/cloudevents.go`
  (`NewCloudEventsStructured` = `application/cloudevents+json`; `NewCloudEventsBinary`
  with `ceAMQPPrefix = "cloudEvents:"`; binary `time` emitted as a header),
  `message.go` (`validateHeadersDepth`; int/uint→int64/uint64 coercion),
  `otel/propagation.go` (`traceparent`/`tracestate`).
- `tasks/plan.md` / `tasks/todo.md` — Phase 14 lands here (T94–T100). No existing
  task to extend (codecs are implemented, not pending); coordinate **T37** (the
  planned `amqptest` testcontainers helper) — the `interop` lane extends it.
- `LATER.md` — highest entry is **LATER-38**; this brief adds **LATER-39**.
- The validation prompt this brief derives from:
  `spec-validation/03-interoperability-wire-format.md`.

---

## 3. Constraints & working agreements (planner must honour)

- **Interop is proven, not asserted.** Every cross-language claim is tested by a
  round-trip through a **non-Go client** (Python `pika`, a Java client, an
  AMQP-1.0 CloudEvents SDK) on a live broker. A Go↔Go round-trip cannot falsify a
  cross-language claim (`feedback_codec_interop`).
- **Amend SPEC before code.** A correction that contradicts SPEC amends SPEC in the
  same PR (or earlier), then implementation follows.
- **Interop lane, not just integration lane.** The existing Go-only
  `testcontainers` lane cannot reproduce the cross-language findings. Introduce a
  polyglot fixture behind a new `interop` build tag; gate it on the same per-version
  matrix (3.13 LTS + 4.x) the spec already requires. Extend `amqptest` (T37), do
  not duplicate it.
- **`testify` `assert`/`require`** in every `_test.go`; **`goleak`** on any
  goroutine-spawning test (project rules).
- **Docs in English** (`CLAUDE.md`). Module path `github.com/brunomvsouza/warren`;
  `errorlint` is on.
- **Choke points:** codec logic in `codec/`; field-table typing/coercion in
  `message.go`; header propagation in `otel/propagation.go`; x-death/header parsing
  in `internal/headers`. Corrections land at the choke point, not ad-hoc.
- **README sync:** any change to the external interop contract (CloudEvents binary
  scoped to 0-9-1, the `time.Time` header caveat, the JSON int64 caveat, the
  low-interop field-table types) updates `README.md`.

---

## 4. Pre-work: verification gates (IG-1..IG-6 — sequence FIRST; they gate wording)

These behaviours must be observed through a **non-Go client** before the dependent
amendments can be worded correctly. The harness (T94) stands them all up.

| Gate | Question (ground truth to capture) | Blocks |
|------|-----------------------------------|--------|
| **IG-1** | Does `pika`/Java read a `time.Time`→`T` header at **second resolution** (sub-second + TZ gone)? Confirm the truncation a Go reader also sees. | IW-07 / T95 |
| **IG-2** | Do `pika`/Java decode the unsigned field-table types `B/u/i/L`, the `Decimal` `D`, and `[]byte` `x` correctly, or do they round-trip only Go↔Go (mis-type / unreadable)? | IW-08/09/10 / T96 |
| **IG-3** | For a warren **binary-mode** CloudEvents publish, what does an **AMQP-1.0 CloudEvents SDK** (Java/Python) actually see — prefix, which message section (`application-properties` vs `message-annotations`), and does the colon key survive the 0-9-1→1.0 conversion? | IW-01/02/12 / T97 |
| **IG-4** | Does a non-Go consumer reconstruct the event from a **structured-mode** `application/cloudevents+json` body (id/source/type/time/extensions intact)? This is the proposed cross-ecosystem path. | IW-01 / T97 |
| **IG-5** | Does a Java/Python producer's `int64` > 2^53 survive into a Go `int64` struct field, and what happens when the target is `any`/`map[string]any`? | IW-04 / T98 |
| **IG-6** | Via a non-Go consumer / `rabbitmqadmin get`, are `ContentType` and `ContentEncoding` the two distinct fields (set to distinct non-empty values), not swapped? | IW-11 / T100 |

**Planner note:** IG-1/IG-2/IG-3/IG-5 each reveal a *current* behaviour the spec
must name; IG-4/IG-6 confirm whether the proposed path/test holds. No SPEC
interop-claim edit to an affected section until the relevant gate returns.

---

## 5. Workstreams (grouped findings, sequenced)

Each item: **problem → location → desired outcome → acceptance test → disposition**.
Severity in brackets.

### WS-1 — The structural test gap *(everything else depends on this)*

- **IW-13 [High, design-disagreement]** — no non-Go interop test exists; every
  cross-language fidelity claim rests on Go↔Go round-trips, violating
  `feedback_codec_interop`. *Location:* whole spec / §9. *Outcome:* a polyglot
  harness — Python `pika` + a Java client + an AMQP-1.0 CloudEvents SDK in
  containers, behind a new `interop` build tag, capturing IG-1..IG-6 ground truth.
  *Acceptance:* `make test-interop` green on 3.13 **and** 4.x; a committed results
  table. *Disposition:* **new task T94**; coordinate **T37** (`amqptest`).

### WS-2 — Silent-lossy wire mappings *(the spec never flags these)*

- **IW-07 [High, underspecified/silent-lossy]** — `time.Time`→AMQP `T` is second
  precision, no sub-second, no timezone (AMQP 0-9-1 `timestamp` = 64-bit POSIX
  seconds). *Location:* §6.5 L1515–1535; decision 13 L2676–2681. *Outcome:*
  document the truncation; recommend an RFC3339 **string** header for sub-second/TZ
  fidelity. *Acceptance (IG-1):* a round-trip test asserts the Go reader sees a
  truncated value and `pika` sees second resolution. *Disposition:* amend
  §6.5/decision 13 + **new task T95**.
- **IW-04 [High, underspecified]** — JSON `int64` > 2^53 from a Java/Python
  producer decodes losslessly only into a typed `int64` field; into
  `any`/`map[string]any`/`interface{}` it silently becomes `float64`. The codec does
  not call `UseNumber()`. *Location:* §6.9; decision 23 L2765–2778; `codec/json.go`.
  *Outcome:* document the precision hazard + the mitigation (type `M` fields as
  `int64`/`json.Number`; avoid `any` for large ints; note `UseNumber()` is *not*
  used by design — it would push the burden onto every consumer). *Acceptance
  (IG-5):* extend `FuzzCodecJSON` + a cross-language `int64 > 2^53` round-trip test.
  *Disposition:* amend §6.9/decision 23 + **new task T98**.

### WS-3 — Interop overclaims to correct *(amend SPEC first)*

- **IW-01 [High, factual-error]** — the CloudEvents binary-mode claim "RabbitMQ
  bridges 0-9-1 headers ⇄ AMQP-1.0 application-properties, so a non-Go AMQP-1.0
  CloudEvents client interoperates." The CloudEvents AMQP Protocol Binding is
  defined for **AMQP 1.0**, not 0-9-1; the bridge is version-dependent (3.13 vs 4.x)
  and **untested**. *Location:* §6.9 L2011–2019; decision 4 L2573–2588.
  *Outcome (owner decision):* **remove the 1.0 guarantee** — document binary mode
  for **0-9-1 consumers only**, and promote **structured mode**
  (`application/cloudevents+json` body) as the **official cross-ecosystem path**.
  Fold **IW-02** (colon-key survival) and **IW-03** (confirm `time` is emitted as
  an RFC3339 string `S`, not `T`). *Acceptance (IG-3/IG-4):* the harness AMQP-1.0
  leg characterises actual binary-mode behaviour and proves structured-mode
  reconstruction. *Disposition:* amend §6.9/decision 4 + **new task T97**; add
  `examples/cloudevents/`.
- **IW-08 [Med-High, underspecified]** — unsigned field-table types
  `uint8/16/32/64`→`B/u/i/L` are claimed "matched 1:1 via amqp091-go," but the
  0-9-1 field-table type set is RabbitMQ-specific and Java/.NET historically
  disagreed on some types. *Location:* §6.5 L1515–1535. *Outcome:* scope the
  cross-client guarantee against RabbitMQ's documented field-table type table; flag
  the unsigned types as Go/Java-leaning; recommend signed `int64` (`l`) for maximum
  cross-client safety. *Acceptance (IG-2):* a cross-client decode test.
  *Disposition:* amend §6.5/decision 13 + **new task T96** (with IW-09/IW-10).

### WS-4 — Interop hazards the spec is silent about

- **IW-09 [Med, underspecified]** — `Decimal{Scale,Value}` (`D`) is re-exported
  "for protocol parity," but few non-Java clients implement `D`; a `D` header may be
  unreadable/mis-decoded by Python/.NET consumers. *Location:* §6.5 L1521; decision
  13. *Outcome:* flag `D` as a low-interop type; recommend string/float for
  cross-language headers. *Disposition:* fold into **T96**.
- **IW-10 [Low, underspecified]** — `[]byte`→`x` vs `string`→`S`: some clients
  don't distinguish. *Location:* §6.5 L1525–1526. *Outcome:* document the
  distinction and cross-client behaviour. *Disposition:* fold into **T96**.
- **IW-05 [Med, underspecified → LATER-39]** — proto3 binary carries no type info;
  `ConsumerFor[M]` fixes one Go type per queue, so a multi-type topic-exchange queue
  cannot be decoded without a discriminator. *Location:* §6.9 L1997.
  *Outcome (owner decision):* document the single-type-per-`Consumer` constraint now;
  **defer** the Any/type-URL/registry discriminator to **LATER-39**.
  *Disposition:* amend §6.9 + **new task T99**; file LATER-39.
- **IW-06 [Low, design-disagreement]** — media type `application/x-protobuf`
  (de-facto) vs `application/protobuf` (IANA-registered). *Location:* §6.9 L1997.
  *Outcome:* justify the chosen string and **accept `application/protobuf` on
  decode** (Postel). *Disposition:* fold into **T99**.

### WS-5 — Underspecified correctness / test-quality windows

- **IW-02 [Med, underspecified]** — colon in a field-table key (`cloudEvents:type`):
  legal in 0-9-1 (shortstr, RabbitMQ ≤128) but survival through the 0-9-1→1.0
  conversion is unstated. *Disposition:* fold into **T97** (verified by IG-3).
- **IW-03 [Med, underspecified]** — binary-mode `time` must be an RFC3339 string
  `S`, not `T` (else it truncates, contradicting the CloudEvents JSON format).
  *Disposition:* fold into **T97**.
- **IW-12 [Med, underspecified]** — `traceparent`/`tracestate` survive as `S`
  headers and across DLX re-publish (**confirmed-correct, do-not-regress**); only
  their survival through the 0-9-1→1.0 bridge to a non-Go 1.0 consumer is
  uncertain — the same unverified bridge as IW-01. *Disposition:* the 1.0-bridge
  caveat moves with **T97**; the DLX/no-collision claims stay (§6 do-not-regress).
- **IW-11 [Low, test-quality]** — the §9 `rabbitmqadmin get` swap test only catches
  a ContentType/ContentEncoding swap if it sets **both** to distinct non-empty
  values. *Location:* §9 L2496–2510. *Outcome:* mandate `application/json` + `gzip`,
  asserted independently. *Acceptance (IG-6).* *Disposition:* amend §9 + **new task
  T100** (may ride T94).

---

## 6. Do-not-regress list (confirmed-correct, protect with tests)

1. `ContentType` and `ContentEncoding` are two distinct AMQP properties
   (§6.5 L1467–1473, decision 10) — correct; IW-11 only sharpens the *test*.
2. DLX re-publish preserves every `basic.properties` field and every header,
   including `traceparent`/`tracestate` and `x-death` (§6.9 L2117–2129) — correct.
3. The `cloudEvents:` prefix does not collide with `traceparent`/`tracestate`
   (§6.9 L2102–2104) — correct.
4. Structured mode delegates serialization to the official SDK JSON event format
   (`application/cloudevents+json`) (§6.9 L2006–2010) — correct; T97 promotes it as
   **the** cross-ecosystem path, so it must stay faithful.
5. Native `int`/`uint` auto-coerce to `int64`/`uint64` (decision 13) — ergonomic and
   correct; IW-08 scopes the *interop* of the resulting types, not the coercion.
6. JSON lax-by-default per Postel's Law + opt-in `NewJSONStrict` (decision 23) —
   correct; IW-04 adds the int64 caveat without weakening Postel.
7. Fuzz targets `FuzzCodecJSON` / `FuzzCodecProtobuf` / `FuzzCodecCloudEventsBinary`
   exist — keep; T98 extends `FuzzCodecJSON` for large ints.

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Amend SPEC | New task | LATER.md | Owner decision? |
|---------|-----|-----------|----------|----------|-----------------|
| IW-01 | High | ✅ §6.9 / dec.4 | **T97** + `examples/cloudevents/` | — | ✅ remove 1.0 claim |
| IW-02 | Med | ✅ §6.9 | **T97** | — | — |
| IW-03 | Med | ✅ §6.9 | **T97** | — | — |
| IW-04 | High | ✅ §6.9 / dec.23 | **T98** | — | — |
| IW-05 | Med | ✅ §6.9 | **T99** | **LATER-39** | ✅ defer discriminator |
| IW-06 | Low | ✅ §6.9 | **T99** | — | — |
| IW-07 | High | ✅ §6.5 / dec.13 | **T95** | — | ✅ doc + RFC3339 string |
| IW-08 | Med-High | ✅ §6.5 / dec.13 | **T96** | — | — |
| IW-09 | Med | ✅ §6.5 / dec.13 | **T96** | — | — |
| IW-10 | Low | ✅ §6.5 | **T96** | — | — |
| IW-11 | Low | ✅ §9 | **T100** | — | — |
| IW-12 | Med | ✅ §6.9 (move caveat) | **T97** | — | — |
| IW-13 | High | — (test infra) | **T94** + coord. **T37** | — | ✅ full polyglot lane |

---

## 8. Suggested phasing (planner may revise — "Phase 14")

1. **Phase A — Interop lane (IW-13).** Stand up the FULL polyglot harness (`pika` +
   Java + AMQP-1.0) behind the `interop` build tag; capture IG-1..IG-6. No SPEC
   interop-claim edits until the relevant gate returns.
2. **Phase B — Silent-lossy + overclaim corrections (WS-2, WS-3).** `time.Time`
   truncation (T95), field-table cross-client scoping (T96), CloudEvents 1.0-claim
   removal + structured-as-cross-ecosystem (T97). SPEC-first, locked by IG-1/2/3/4.
3. **Phase C — Codec precision + discrimination (WS-4 + IW-04).** JSON int64
   precision (T98), protobuf single-type doc + media-type + LATER-39 (T99).
4. **Phase D — Test-quality (WS-5).** ContentType/ContentEncoding swap test (T100).

---

## 9. Acceptance criteria for the whole effort

- [ ] A polyglot `interop` lane (IW-13/T94) exists: `pika` + Java + AMQP-1.0
      clients in containers, `make test-interop` green on 3.13 **and** 4.x, with a
      committed IG-1..IG-6 results table each downstream task cites.
- [ ] `time.Time`→`T` truncation (IW-07/T95) is documented (§6.5/decision 13) with
      an RFC3339-string recommendation; a round-trip test asserts the truncation.
- [ ] The field-table cross-client guarantee (IW-08/09/10/T96) is scoped against
      RabbitMQ's type table; unsigned `B/u/i/L` and `Decimal` `D` are flagged
      low-interop; `x` vs `S` documented; a cross-client decode test passes.
- [ ] The CloudEvents binary 0-9-1→1.0 interop guarantee (IW-01/T97) is **removed**;
      binary mode is scoped to 0-9-1 consumers; structured mode is documented as the
      cross-ecosystem path and proven by IG-4; `time` is confirmed RFC3339-string
      (IW-03); the trace-context 1.0-bridge caveat (IW-12) moves with it;
      `examples/cloudevents/` ships.
- [ ] The JSON int64 precision hazard (IW-04/T98) is documented (§6.9/decision 23);
      `FuzzCodecJSON` + a cross-language `int64 > 2^53` test cover it.
- [ ] The protobuf single-type-per-`Consumer` constraint (IW-05/T99) is documented;
      `application/protobuf` is accepted on decode (IW-06); **LATER-39** files the
      discriminator.
- [ ] The §9 ContentType/ContentEncoding swap test (IW-11/T100) sets distinct
      non-empty values and asserts each.
- [ ] Owner decisions recorded (§10 below). No SPEC edit to an affected section
      lands before its gate returns. No duplicate tasks; `LATER.md` adds only
      LATER-39.
- [ ] `README.md` reflects the changed interop contract (CloudEvents binary scoped
      to 0-9-1, the `time.Time` header caveat, the JSON int64 caveat, the
      low-interop field-table types).
- [ ] Tree green: `make test` (race + cover), `make lint`, the per-version
      integration lane, **and the new `interop` lane**; `goleak` clean.

---

## 10. Owner decisions (recorded 2026-05-28)

1. **Interop lane scope = FULL.** Stand up `pika` + Java + an AMQP-1.0 CloudEvents
   SDK behind a new `interop` build tag (not pika-only, not doc-only). The
   AMQP-1.0 leg is required to characterise the CloudEvents binary path and prove
   the structured-mode cross-ecosystem path.
2. **CloudEvents binary 0-9-1→1.0 claim = REMOVE.** Drop the guarantee from
   §6.9/decision 4; binary mode is documented for 0-9-1 consumers only; structured
   mode is the official cross-ecosystem path.
3. **Protobuf multi-type discriminator = DEFER (LATER-39).** Document only the
   single-type-per-`Consumer` constraint and the media-type string now.
4. **`time.Time` header policy = DOC + recommend RFC3339 string.** No API change;
   document the `T` second-precision/no-TZ truncation and recommend a string header.

---

## 11. Interop matrix (target wording after Phase 14)

| Format | Go↔Go | Go↔Java | Go↔Python/.NET | Go↔AMQP-1.0 |
|--------|-------|---------|----------------|-------------|
| JSON (scalars) | faithful | faithful | faithful | faithful |
| JSON (`int64` > 2^53 into `any`) | **lossy** (T98 documents) | lossy | lossy | n/a |
| Protobuf (single type) | faithful | faithful | faithful | faithful |
| Protobuf (multi-type queue) | **needs discriminator** (LATER-39) | — | — | — |
| Headers: scalars / `int64`(`l`) / `string`(`S`) | faithful | faithful | faithful | faithful (T94 proves) |
| Headers: `time.Time`(`T`) | **lossy** (sec/TZ, T95) | lossy | lossy | lossy |
| Headers: unsigned `B/u/i/L`, `Decimal`(`D`) | faithful | likely | **unknown → test** (T96) | test |
| CloudEvents structured (`+json` body) | faithful | faithful | faithful | faithful (T97 = the path) |
| CloudEvents binary (`cloudEvents:` headers) | faithful | faithful (0-9-1) | faithful (0-9-1) | **claim removed** (T97) |
| OTel `traceparent`/`tracestate` | faithful | faithful | faithful | caveat (with IW-01) |

---

## 12. Out of scope for this plan

- The other validation lenses (01 protocol — Phase 12; 02 distributed-systems —
  Phase 13; 04 EDA, 05 SRE, 06–13).
- The Protobuf Any/type-URL/registry multi-type discriminator (→ **LATER-39**).
- Stream-protocol semantics (v0.2 per decision 24).
- Any change to the API design north stars (§10) beyond the corrections above.

---

## 13. Verdict for this lens

**GO-WITH-CHANGES.** The wire format is faithful Go↔Go and for 0-9-1 consumers, but
several headline cross-language claims are asserted-not-proven (IW-01, IW-08,
IW-13), two mappings are silently lossy at the polyglot bar (IW-07 `time.Time`,
IW-04 JSON int64), and the proto multi-type path is undefined (IW-05). None is an
active §1 message-loss bug; all are closed by honest SPEC scoping + a polyglot
test lane.
