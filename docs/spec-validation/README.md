# Spec Validation Prompts — `warren`

This directory holds **adversarial spec-validation prompts**, one per domain
expert. Each file is a self-contained prompt meant to be pasted into the
`/agent-skills:spec` skill (or any reviewing agent) to drive a deep, skeptical
review of `SPEC.md` from a single expert lens.

## Why this exists

`warren` (module `github.com/brunomvsouza/warren`; the working directory is
named `amqp.go`) is a generics-typed Go client for AMQP 0-9-1 (RabbitMQ),
built on `rabbitmq/amqp091-go`. Its stated reliability bar is **"trusted in
flight paths that handle billions of messages per day"** (`SPEC.md` §1). At
that bar, a single under-specified failure mode is an outage.

`SPEC.md` has already survived three specialist review passes (Rev 6, Rev 9,
Rev 10 — see §10). That maturity is a trap: the remaining defects are exactly
the ones three prior reviews **missed**. These prompts are therefore tuned to
hunt for the second-order gaps, the load-bearing factual claims that were
asserted but never verified, and the design decisions that were "resolved"
without being stress-tested against an adversarial scenario.

## The experts

| # | File | Lens | Owner-requested? |
|---|------|------|------------------|
| 01 | `01-rabbitmq-amqp-protocol.md`    | RabbitMQ **3.13 LTS + 4.x** / AMQP 0-9-1 wire-protocol fidelity (incl. cross-version divergence) | ✅ |
| 02 | `02-distributed-systems.md`       | Failure modes, consistency, ordering, duplicates | ✅ |
| 03 | `03-interoperability-wire-format.md` | Polyglot clients, CloudEvents/Proto/JSON, field-tables | ✅ |
| 04 | `04-event-driven-architecture.md` | Messaging patterns, topology, RPC, DLQ design | ✅ |
| 05 | `05-sre-operability.md`           | Observability, recovery, capacity, on-call ergonomics | ✅ |
| 06 | `06-go-api-library-design.md`     | Generics, builders, error model, semver, ergonomics | supplementary |
| 07 | `07-security-threat-modeling.md`  | Credentials, TLS/mTLS, untrusted input, DoS | supplementary |
| 08 | `08-go-concurrency-runtime.md`    | Goroutine lifecycles, races, deadlocks, leaks | supplementary |
| 09 | `09-performance-capacity.md`      | Throughput ceilings, latency, benchmark methodology | supplementary |
| 10 | `10-test-strategy-verifiability.md` | Falsifiability of success criteria, test coverage of the hard parts | supplementary |
| 11 | `11-compliance-gdpr-lgpd.md`      | GDPR & LGPD data-protection exposure of the design and its defaults | ✅ |
| 12 | `12-dx-documentation.md`          | Developer experience, godoc placement, examples, onboarding | ✅ |
| 13 | `13-load-testing.md`              | Load-campaign realism, harness fidelity, soak/spike/stress | ✅ |

Experts 01–05 and 11–13 are the lenses the owner explicitly requested. 06–10 are
additional lenses judged load-bearing for a library at this reliability bar.
Expert 01 is deliberately scoped to the **two broker generations the lib
supports (RabbitMQ 3.13 LTS and 4.x)** and runs a cross-version divergence pass
first. Experts 09 (performance model), 10 (verifiability), and 13 (load
campaigns) are intentionally distinct, non-overlapping slices of "does it
perform and is that proven" — each states its lane.

## How to use

1. Pick one expert file. Paste its contents into `/agent-skills:spec` (or hand
   it to a fresh reviewing agent with no prior context on this spec).
2. Run them **independently** and in **separate contexts** — the value is the
   diversity of independent perspectives. Do not let one review's conclusions
   bias the next.
3. Each prompt produces a structured findings report with a per-lens verdict
   (`GO` / `GO-WITH-CHANGES` / `NO-GO`). Collect all reports.
4. De-duplicate findings across experts (the same defect will surface from
   multiple lenses — that is signal, not noise: a defect seen by 3 experts is
   higher-priority than one seen by 1).
5. For every confirmed defect, the workflow is: amend `SPEC.md` first (per
   `CLAUDE.md` "Anything contradicting SPEC must amend SPEC first"), then route
   non-blocking findings to `LATER.md`, then convert blockers into tasks in
   `tasks/plan.md` + `tasks/todo.md`.

## Shared conventions every prompt enforces

- **Validate the existing spec; do not author a new one.** The `/agent-skills:spec`
  default is to write a spec — these prompts redirect it to *review* mode.
- **Ground every claim.** Cite the RabbitMQ docs, the AMQP 0-9-1 spec, the OTel
  Messaging semantic conventions, or the CloudEvents binding — with the specific
  broker version where behaviour is version-dependent (the spec supports both
  **RabbitMQ 3.13 LTS and 4.x**, and several claims differ between them).
- **Classify each finding** as `factual-error` (the spec states something
  untrue), `underspecified` (a real behaviour is left undefined), or
  `design-disagreement` (the spec is internally consistent but the decision is
  wrong for the stated bar).
- **No rubber-stamping.** If a section looks clean, state *why* you are
  confident rather than skipping it silently.
