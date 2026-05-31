# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-30

First public release line. Everything below is new.

### Added

#### Connection & reliability
- `Dial` returns a `*Connection` that owns a **role-split pool of TCP
  connections** (default: 2 publisher + 2 consumer sockets, configurable via
  `WithPublisherConnections` / `WithConsumerConnections`). Each socket has its
  own supervised reconnect loop.
- **Synchronous reconnect barrier**: after a TCP reconnect a socket reopens its
  channel, redeclares attached topology, re-issues `basic.consume`, and fires
  `WithOnReconnect` before traffic resumes. `Publish` blocks on
  `ErrReconnecting` until the barrier clears.
- **Degraded state**: persistent topology-redeclare failure surfaces
  `ErrTopologyRedeclareFailed`, fires `WithOnTopologyDegraded`, and exposes the
  `(*Connection).ForceReconnect()` operator escape hatch.
- Round-robin cluster failover across `WithAddrs([...])`.
- TLS via `WithTLSConfig` + `amqps://`, including SASL EXTERNAL (mTLS).
- Named connections (`WithConnectionName`) → per-socket names
  `name-pub-N` / `name-con-N` visible to `rabbitmqctl list_connections`.
- Panic isolation for the user callbacks (`OnReconnect`, `OnResubscribe`,
  `OnTopologyDegraded`).
- Hooks: `WithOnReconnect`, `WithOnResubscribe`, `WithOnTopologyDegraded`.

#### Publishing
- Typed `PublisherFor[M]` builder with publisher confirms.
- `Mandatory()` + `OnReturn` for unroutable-message handling.
- `ConfirmTimeout` (broker-confirm deadline → `ErrConfirmTimeout`).
- `PublishRetry(RetryPolicy)` for automatic transient-error retries.
- Client-side `UserID` validation (turns the broker's 406 footgun into a local
  `ErrInvalidMessage`); `StampUserID()` to stamp the authenticated user.
- `PublishBatch` — always-all, single-channel, returns `[]PublishResult`;
  rejects the whole call with `ErrBatchTooLarge` when the batch exceeds
  `PublishBatchMaxSize` (`PublishRetry` does **not** apply to `PublishBatch`).

#### Topology
- `Topology.Declare(ctx, conn)` + `AttachTo(conn)` (deep-snapshotted for
  reconnect redeclare).
- Exchanges, queues, bindings, and `DeadLetters` (DLX wiring).
- Quorum and classic queues, `SingleActiveConsumer`, `DeliveryLimit`,
  `MaxPriority`.
- `DelayedTopic` helper for the `rabbitmq_delayed_message_exchange` plugin.

#### Consuming
- Typed `ConsumerFor[M]` with a value-typed `Handler[M] = func(ctx, M) error`:
  `nil` → Ack, error → Nack(no-requeue), `errors.Is(err, ErrRequeue)` →
  Nack(requeue).
- `Concurrency`, `Prefetch`, `Tag`.
- `MaxRedeliveries` (in-process counter B) with default
  `HandlerTimeoutVerdict = TimeoutNackNoRequeue`.
- `HandlerTimeout` with configurable verdict.
- `AutoAck()` opt-in (broker no-ack) with a sampled drop-warning.
- `ConsumeRaw` escape hatch exposing the full `*Delivery[M]` (MessageID,
  Headers, x-death accessors, Redelivered, …).
- `basic.cancel` surfaced as `ErrConsumerCancelled`.
- `BatchConsumerFor[M]` with size- and timer-based flush (`Size`, `FlushAfter`)
  and `multiple=true` acking.

#### Codecs
- JSON (lax by default per Postel's Law; `codec.NewJSONStrict()` opt-in).
- Protobuf (`codec.NewProtobuf()`).
- CloudEvents in structured and binary modes
  (`codec.NewCloudEventsStructured()` / `codec.NewCloudEventsBinary()`).

#### Errors
- AMQP reply-code sentinels (`ErrAccessRefused`, `ErrNotFound`,
  `ErrPreconditionFailed`, …) with `AMQPCode(err)` and `IsTransient` /
  `IsPermanent` classifiers, centralized in `internal/amqperror`.
- Operational sentinels: `ErrReconnecting`, `ErrChannelClosed`,
  `ErrChannelPoolExhausted`, `ErrConfirmTimeout`, `ErrTopologyMismatch`,
  `ErrTopologyRedeclareFailed`, `ErrRequeue`, `ErrInvalidMessage`,
  `ErrBatchTooLarge`, `ErrCallTimeout`, `ErrConsumerCancelled`.

#### Observability
- Pluggable `log.Logger` (NoOp default).
- Prometheus metrics (default), including `publisher_retry_total`,
  `consumer_resubscribed_total`, and the handler/confirm histograms.
- OpenTelemetry: `otel.Tracer` + W3C `otel.Propagator`. Publisher spans inject
  trace context into AMQP headers before the frame is written; consumer spans
  extract it; `BatchConsumer` spans carry one Link per message.

#### Patterns
- RPC over direct reply-to: `CallerFor[Req, Resp]` / `ReplierFor[Req, Resp]`,
  concurrent calls demuxed by `CorrelationID`, at-least-once replies, DLX
  validation on the request queue, `ErrCallTimeout`.
- Delayed publish via `Message[M].Delay` + `DelayedTopic`.

#### Safety
- `internal/redact` strips `userinfo` from every AMQP URI before it reaches a
  log line, metric label, span attribute, or error message (SPEC §8).

#### Testing
- `warren.NewDeliveryFixture[M]` / `NewBatchFixture[M]` fabricate
  `*Delivery[M]` / `*Batch[M]` values without a live broker.
- `internal/amqptest` testcontainers RabbitMQ fixture (delayed + TLS/SASL plugin
  modes) used by Warren's own integration suite.

#### Examples
- `examples/publish`, `examples/consume`, `examples/topology`,
  `examples/deadletter`, `examples/batch_publish`, `examples/batch_consume`,
  `examples/rpc`, `examples/delayed`, `examples/idempotent_consume`,
  `examples/ordered_consume`, `examples/otel` — each builds on the unit CI lane
  and smoke-runs end-to-end on the integration lane.

[Unreleased]: https://github.com/brunomvsouza/warren/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/brunomvsouza/warren/releases/tag/v0.1.0
