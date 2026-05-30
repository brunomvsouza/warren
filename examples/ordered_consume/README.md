# Ordered consumer with failover (`SingleActiveConsumer` + `Concurrency(1)`)

The reference for strict per-queue ordering from **SPEC §6.3**.

AMQP does not guarantee ordered processing by default: with multiple consumers
(or one consumer running multiple handler goroutines) messages are processed
concurrently and finish out of order. To get **strict in-order processing** you
need two things together:

1. **`Queue.SingleActiveConsumer = true`** (`x-single-active-consumer`): the
   broker delivers to at most **one** consumer of the queue at a time. The other
   subscribers are hot standbys. If the active one disconnects, the broker
   promotes a standby — failover with no manual coordination.
2. **`Consumer.Concurrency(1)`**: a single handler goroutine, so the active
   consumer processes its deliveries serially rather than fanning them out.

Either one alone is insufficient: `Concurrency(1)` on two active consumers still
interleaves two streams; `SingleActiveConsumer` with `Concurrency(8)` still
parallelizes within the active consumer.

## What this example does

1. Declares a queue with `SingleActiveConsumer = true`.
2. Starts **two** consumer instances (`consumer-a`, `consumer-b`), each with
   `Concurrency(1)`. `consumer-a` gets a head start and becomes active.
3. Publishes `0..19` in order. The active consumer prints them in order.
4. After the active consumer accepts seq `9`, the example **cancels it**
   (simulating a crash/deploy). The broker promotes the standby, which continues
   with `10..19`, still in order.
5. Asserts the deduped accepted stream is exactly `0..19` — publish order ==
   handler order across the failover.

Run it against a local broker:

```sh
AMQP_URL=amqp://guest:guest@localhost:5672/ go run ./examples/ordered_consume
```

## The trade-off

Strict ordering caps throughput at **one message at a time, one worker at a
time**. The whole queue is processed by a single handler goroutine on a single
consumer; the standbys add availability, not parallelism. This is the right
choice when correctness depends on order (a per-aggregate event stream, a
state-machine feed) and the wrong choice when you only need high throughput
(scale out with many active consumers and `Concurrency(N)` instead, and dedupe —
see `examples/idempotent_consume/`).

For ordering *and* parallelism, partition the stream: shard by a key (e.g.
`hash(aggregate_id) % N`) into N queues, each with `SingleActiveConsumer` +
`Concurrency(1)`. You get in-order processing **per shard** and N-way
parallelism across shards.

## Failover semantics (at-least-once at the handoff)

When the active consumer drops, any message it had received but not yet acked is
requeued and redelivered to the promoted standby. So the standby may see **one
redelivery** of the in-flight message at the handoff. That is at-least-once
working as designed — ordering is preserved, but a single message can appear
twice at the seam. This example dedupes by sequence number (`acceptDuplicate`)
and acks the redelivery without reprocessing. A production handler must be
idempotent for the same reason (see `examples/idempotent_consume/`).

## Related

- `examples/idempotent_consume/` — dedupe-by-`MessageID`, the other half of
  correct consumption.
- SPEC §6.3 — ordering, poison-message handling, and the `Concurrency` /
  `SingleActiveConsumer` contract.
