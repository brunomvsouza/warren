# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Source of truth

- **`SPEC.md`** — complete public API surface and contracts (large; read in slices). Anything contradicting SPEC must amend SPEC first.
- **`tasks/plan.md`** — phased implementation plan (54 tasks T01..T46, with sub-letters). Tasks are numbered and ordered by dependency; recent commits are titled `feat: ... (Txx)` to track progress.
- **`tasks/todo.md`** — granular checklist tied to `plan.md` (very large; read with `offset`/`limit`).

When the user references "Txx", look it up in `tasks/plan.md` for scope and acceptance criteria.

## Build, lint, test

```
make build              # go build ./...
make test               # go test -race -cover ./...
make test-integration   # build tag 'integration' (requires Docker)
make test-conformance   # build tag 'conformance' (requires Docker)
make test-all           # unit + integration + conformance
make lint               # golangci-lint run ./...
make mocks              # go generate ./... (gomock)
make examples-build     # go build ./examples/... (unit lane; no broker required)
make examples-smoke     # go test -race -tags=integration ./examples/... (requires Docker)
```

`examples-build` and `examples-smoke` enforce SPEC §7 "Executable
examples at checkpoints" + §10 Rev decision 49: every checkpoint
example under `examples/<feature>/` must build on every PR and
smoke-run end-to-end on the `integration` CI lane. The targets
are introduced by T13b (Phase 2) and extended by every subsequent
checkpoint task (T16b, T21b, T23b, T31b). T38 (Phase 9) wires
them into `.github/workflows/ci.yml` as required gates.

Run a single test:
```
go test -race -run '^TestRetryPolicy_NextBackoff$' ./...
go test -race -tags=integration -run '^TestDial_' .
```

Module path is `github.com/brunomvsouza/amqp` (note: **no `.go` suffix** despite the directory name). Go 1.23. `goimports` local-prefix is set to the module path — keep imports grouped accordingly.

## Linter contract

`.golangci.yml` enables `errcheck`, `govet`, `staticcheck`, `gosec`, `revive`, `gocritic`, `unparam`, `bodyclose`, `nilerr`, **`errorlint`**. Consequences:

- Always compare errors with `errors.Is` / `errors.As` — never `==` (errorlint).
- `revive`'s `exported` rule runs with `disableStutteringCheck` (package `amqp` exports `amqp.Headers` etc. on purpose).
- `gosec` excludes `G115` (AMQP frames are uint16/uint8 by spec) and `G101` (test fixtures use fake AMQP credentials).

## Testing conventions

- Use `github.com/stretchr/testify` (`assert` / `require`) in every `_test.go` — this is a hard project rule.
- Use `go.uber.org/goleak.VerifyNone(t)` at the end of any test that spawns goroutines (reconnect, channel pool, consumer loop).
- Integration tests live behind the `integration` build tag; conformance tests behind `conformance`. Both expect Docker (`amqptest/` will host the testcontainers helper).
- Fuzz targets are part of the contract (e.g. `FuzzRedactURI`, future `FuzzCodecJSON`, `FuzzXDeathParser`).

## Architecture — the big picture

A generics-typed wrapper over `github.com/rabbitmq/amqp091-go`. The public API is being built bottom-up following `tasks/plan.md`. Layout (current + planned):

```
errors.go, types.go, retry.go, doc.go        ← root package `amqp`
log/      metrics/   otel/                   ← pluggable observability (NoOp default)
internal/reconnect                           ← supervised reconnect loop + RetryPolicy
internal/redact                              ← AMQP URI userinfo redaction (choke-point)
internal/amqperror   (planned T07b)          ← *amqp091.Error → reply-code sentinel wraps
internal/confirms    (planned T11)           ← publisher-confirm tracker
internal/headers     (planned)               ← x-death parser, otel propagation headers
codec/               (planned T09/T24-T26)   ← JSON (strict default), Protobuf, CloudEvents
connection.go, channelpool.go (planned)     ← multi-TCP pool, role-split
topology.go          (planned T14-T16)
publisher.go, consumer.go, rpc.go, delay.go  (planned)
amqpmock/, amqptest/ (planned T37)
```

Three architectural invariants that span multiple files:

1. **`Connection` is a role-split pool of TCP connections.** Default `WithPublisherConnections=2`, `WithConsumerConnections=2`. Each TCP socket has its own `internal/reconnect` supervisor. Publishers acquire channels from a per-conn channel pool; consumers are pinned to a consumer-side TCP connection by stable hash of consumer-tag (auto-defaulted to `ctag-<uuidv7>`). A single TCP socket bottlenecks above ~50k msg/s because `amqp091-go` serializes I/O per connection.

2. **Reconnect is a synchronous barrier.** After a TCP reconnect: re-open channel → redeclare topology → re-issue `basic.consume` → fire `WithOnReconnect`. `Publish` blocks on `ErrReconnecting` until the barrier clears. Persistent redeclare failure → degraded state + `ErrTopologyRedeclareFailed` + `WithOnTopologyDegraded` + `(*Connection).ForceReconnect()`.

3. **At-least-once is the contract.** `PublishRetry`, the reconnect barrier, `ErrConfirmTimeout`, and `ErrChannelClosed` can all produce duplicates. Consumers MUST dedupe by `MessageID` (auto-populated UUIDv7). The `publisher_retry_total` metric makes retries observable. There is no "exactly once" toggle.

Two choke-point packages every broker-touching component funnels through:

- **`internal/amqperror`** translates `*amqp091.Error` into wraps of reply-code sentinels in `errors.go` (`ErrAccessRefused`, `ErrNotFound`, `ErrPreconditionFailed`, ...) and powers `AMQPCode(err)` plus `IsTransient`/`IsPermanent`. If you add a new broker-touching path, route its errors through this package — do not create ad-hoc `errors.New`/`fmt.Errorf` for reply codes.
- **`internal/redact`** strips `userinfo` from any AMQP URI before it reaches a log line, metric label, span attribute, or error message. SPEC §8 "always redact credentials" is enforced here. Anything that formats a URI for human consumption goes through `redact.URI`.

## API design principles

The v1 surface deliberately breaks from prior iterations of the same library. Decisions are listed in SPEC §10; quick reference:

- **Functional options are non-generic; builders are `XFor[M]`** (`PublisherFor[Order]`, `ConsumerFor[Order]`).
- **Topology is declared separately** from publishers/consumers (`Topology.Declare(ctx, conn)` + `AttachTo(conn)` for reconnect re-declare; deep-snapshotted by pointer address of the input `*Topology`).
- **No magic sleeps.** Backpressure is `prefetch_count`; `PublishBatchMaxSize` (not in-flight window) caps batch calls; pool exhaustion surfaces as `ErrChannelPoolExhausted`.
- **Nack without requeue is the default** for handler errors. Requeue is opt-in via `errors.Is(err, ErrRequeue)`.
- **Handler signature is `func(ctx, M) error`** with payload-first; `ConsumeRaw` is the escape hatch for envelope access.
- **`Message[M]` is a struct literal, not a builder.** Defaults applied in one place.
- **`PublishBatch` is always-all** + `[]PublishResult` (no partial-failure mid-iteration). `PublishRetry` does NOT apply to `PublishBatch`.
- **Functional options last-wins** on every builder (asserted in tests).
- **Default `HandlerTimeoutVerdict = TimeoutNackNoRequeue`** so a misconfigured handler cannot create an infinite requeue loop.

When asked to design or change public API: prefer protocol fidelity (surface real AMQP primitives) and ease of use over preserving the prior library's decisions. Challenge any spec point that contradicts those two north stars.

## Deferred improvements — LATER.md

`LATER.md` is the single place for improvements that were consciously deferred during a task or `/ship` review. It is **not** a backlog of bugs — only findings that are not blockers and not worth fixing in the current PR.

Rules:
- **Write to LATER.md** when a `/ship` review, security audit, or test-coverage gap identifies a non-blocking finding that should be tracked for future attention.
- **Do not fix LATER.md items inline** in an unrelated task. If an item becomes urgent, convert it to a formal task in `tasks/plan.md` + `tasks/todo.md` first, then implement it.
- **Each entry must follow the format** defined at the top of `LATER.md`: title, context (file:line), impact, evidence (which review/task), suggested solution, prerequisites.
- **Number entries sequentially** (`LATER-NN`). Do not reuse numbers.
- **Remove an entry** when its corresponding fix is committed, updating the entry's task number in the commit message.

When running `/ship` and the decision is NO-GO, fix the blockers, then move non-blocker findings to LATER.md rather than leaving them as open review comments.

## Working style

The project uses TDD + incremental implementation as a hard discipline (see `.cursor/rules/`). For any behavior change: write the failing test first, then the smallest implementation. For multi-file features: ship in vertical slices, each leaving the tree green. Each task in `tasks/plan.md` has explicit acceptance criteria — treat them as the test list.

Recent commits follow `feat: <subject> (Txx)` / `fix: <subject> (Txx)`. Keep that format when committing task work so progress is greppable.
