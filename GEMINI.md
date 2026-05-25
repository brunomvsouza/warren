# Warren - Project Context & Instructions

This project, **Warren**, is a production-grade, generics-typed Go client for RabbitMQ (AMQP 0-9-1). It is built on top of `github.com/rabbitmq/amqp091-go` but provides a much higher level of abstraction, safety, and operational features.

## Project Overview

- **Core Goal:** Provide a "trusted in flight" AMQP client for high-scale environments (billions of messages/day). Targets AMQP 0-9-1 as implemented by **RabbitMQ 3.13 LTS / 4.x**.
- **Key Features:**
    - **Generics-Typed API:** `PublisherFor[M]` and `ConsumerFor[M]` for type-safe message handling.
    - **Automatic Reconnect:** Supervised reconnect with a synchronous barrier.
    - **Role-Split TCP Pool:** Separates publisher and consumer TCP connections to avoid I/O serialization bottlenecks.
    - **Centralized Topology:** Declarative topology management separate from publishers/consumers.
    - **Observability:** Built-in Prometheus metrics, OpenTelemetry tracing, and pluggable logging.
    - **Security:** Strict redaction of credentials in logs and metrics via `internal/redact`.
    - **Error Handling:** AMQP reply-code sentinels and transient/permanent error classifiers.

## Architecture Invariants & Principles

1. **At-Least-Once Delivery:** The library prioritizes reliability. `PublishRetry` and reconnects may cause duplicates. Consumers MUST be idempotent (dedupe via `MessageID` - UUIDv7 by default). See `examples/idempotent_consume/` for the canonical pattern.
2. **Synchronous Reconnect Barrier:** On connection loss, the library blocks traffic until the connection is restored, channels are reopened, and topology is redeclared. Publish blocks on `ErrReconnecting` until the barrier clears. Persistent failure leads to a degraded state.
3. **Role-Split TCP Pool:** Default `WithPublisherConnections=2`, `WithConsumerConnections=2`. Each TCP socket has its own `internal/reconnect` supervisor.
4. **JSON Lax by Default (Postel's Law):** `codec.NewJSON()` accepts unknown fields so producer-first deploys do not poison v1 DLQs. Strict parsing is opt-in via `codec.NewJSONStrict()`.
5. **Payload Guardrail:** `PublisherBuilder.MaxMessageSizeBytes(n)` rejects oversized bodies locally with `ErrMessageTooLarge` before opening a channel.
6. **Tracing Continuity:** The library never strips/rewrites headers on the consume path, injects trace info before frames are written (survives DLX bounces), and spans terminate with an outcome attribute and OTel status matching the verdict.
7. **No Magic sugar:** Prefers protocol fidelity. Real AMQP primitives (exchanges, queues, bindings) are surfaced clearly. Backpressure is `prefetch_count`.
8. **Poison Message Protection:** Default handler error results in `Nack(requeue=false)`. Requeue is explicit via `ErrRequeue`.
9. **Choke Points:**
    - `internal/amqperror`: All broker errors must be routed here for translation into reply-code sentinels.
    - `internal/redact`: All AMQP URIs must be passed through `redact.URI` before logging/metrics.

## Development Workflow

### Key Commands

```bash
make build              # Compile all packages (go build ./...)
make test               # Run unit tests (with -race and -cover)
make test-integration   # Run integration tests (requires AMQP_TEST_URL)
make test-conformance   # Run conformance tests (requires Docker)
make test-all           # Run all tests (unit + integration + conformance)
make lint               # Run golangci-lint
make mocks              # Generate mocks via gomock
make examples-build     # Build all example applications
make examples-smoke     # Smoke-run examples end-to-end (requires Docker)
```

### Local Integration Testing

Use `docker-compose.integration.yml` to spin up a RabbitMQ identical to CI:
```bash
make integration-up
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ make test-integration
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ make examples-smoke
make integration-down
```

### Source of Truth

- **`SPEC.md`**: The definitive contract for public API and behavior. Anything contradicting SPEC must amend SPEC first.
- **`tasks/plan.md`**: The phased implementation roadmap (currently at Rev 8). Tasks are numbered and ordered by dependency.
- **`tasks/todo.md`**: Granular checklist tied to `plan.md`. Look up "Txx" here for acceptance criteria.
- **`LATER.md`**: For deferred improvements (non-blockers) identified during tasks or `/ship` reviews. Not a backlog of bugs. Must follow specific formatting rules.
- **`CLAUDE.md` / `GEMINI.md`**: Specific instructions for AI agents regarding coding standards and architecture.

### Coding Standards & Testing Conventions

- **Language:** Go 1.23+ (Generics are central).
- **Testing:** 
  - Use `github.com/stretchr/testify` (`assert`/`require`) in every `_test.go`.
  - Use `go.uber.org/goleak.VerifyNone(t)` at the end of any test that spawns goroutines.
  - Fuzz targets are part of the contract (e.g., `FuzzRedactURI`, `FuzzXDeathParser`).
- **Linter Contract:** `.golangci.yml` is strict (`errcheck`, `errorlint`, etc.). Always compare errors with `errors.Is` / `errors.As` — never `==`.
- **Commit Messages:** Follow the format `feat: <subject> (Txx)` or `fix: <subject> (Txx)`, where `Txx` is the task ID from `plan.md`.
- **Imports:** Grouped with local prefix `github.com/brunomvsouza/warren`.

## Directory Structure

- `/codec`: Pluggable message encoders/decoders (JSON is default).
- `/internal/amqperror`: Error translation logic.
- `/internal/confirms`: Publisher confirm tracking.
- `/internal/reconnect`: Reconnect supervisors and retry policies.
- `/internal/redact`: Credential redaction logic.
- `/log`, `/metrics`, `/otel`: Observability interfaces and implementations.
- `/examples`: Checkpoint examples that are verified in CI.
- `/amqpmock`, `/amqptest`: Testing and mock helpers for downstream users.

## Language

All project documents (`SPEC.md`, `tasks/plan.md`, `tasks/todo.md`, `LATER.md`, `CLAUDE.md`, `GEMINI.md`, godoc comments, commit messages, PR descriptions) are written in **English**. Any text added to these files — including new tasks, LATER entries, inline comments, and acceptance criteria — must be in English.