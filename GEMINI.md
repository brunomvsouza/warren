# Warren - Project Context & Instructions

This project, **Warren**, is a production-grade, generics-typed Go client for RabbitMQ (AMQP 0-9-1). It is built on top of `github.com/rabbitmq/amqp091-go` but provides a much higher level of abstraction, safety, and operational features.

## Project Overview

- **Core Goal:** Provide a "trusted in flight" AMQP client for high-scale environments (billions of messages/day).
- **Key Features:**
    - **Generics-Typed API:** `PublisherFor[M]` and `ConsumerFor[M]` for type-safe message handling.
    - **Automatic Reconnect:** Supervised reconnect with a synchronous barrier (topology redeclared before traffic resumes).
    - **Role-Split TCP Pool:** Separates publisher and consumer TCP connections to avoid I/O serialization bottlenecks.
    - **Centralized Topology:** Declarative topology management separate from publishers/consumers.
    - **Observability:** Built-in Prometheus metrics, OpenTelemetry tracing, and pluggable logging.
    - **Security:** Strict redaction of credentials in logs and metrics via `internal/redact`.
    - **Error Handling:** AMQP reply-code sentinels and transient/permanent error classifiers.

## Architecture & Principles

1. **At-Least-Once Delivery:** The library prioritizes reliability. Retries and reconnects may cause duplicates; consumers must be idempotent (dedupe via `MessageID` - UUIDv7 by default).
2. **Synchronous Reconnect Barrier:** On connection loss, the library blocks traffic until the connection is restored, channels are reopened, and topology is redeclared.
3. **No Magic sugar:** Prefers protocol fidelity. Real AMQP primitives (exchanges, queues, bindings) are surfaced clearly.
4. **Poison Message Protection:** Default handler error results in `Nack(requeue=false)`. Requeue is explicit via `ErrRequeue`.
5. **Choke Points:**
    - `internal/amqperror`: All broker errors must be routed here for translation.
    - `internal/redact`: All AMQP URIs must be passed through `redact.URI` before logging/metrics.

## Development Workflow

### Key Commands

```bash
make build              # Compile all packages
make test               # Run unit tests (with -race and -cover)
make lint               # Run golangci-lint
make integration-up     # Start RabbitMQ via Docker
make test-integration   # Run integration tests (requires AMQP_TEST_URL)
make test-all           # Run all tests (unit + integration + conformance)
make examples-build     # Build all example applications
```

### Source of Truth
- **`SPEC.md`**: The definitive contract for public API and behavior. **Read this first** before making API changes.
- **`tasks/plan.md`**: The phased implementation roadmap.
- **`tasks/todo.md`**: Granular checklist of tasks.
- **`CLAUDE.md`**: Specific instructions for AI agents regarding coding standards and architecture.

### Coding Standards
- **Language:** Go 1.23+ (Generics are central).
- **Testing:** Use `github.com/stretchr/testify` (`assert`/`require`). Use `goleak` to verify no goroutine leaks.
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
