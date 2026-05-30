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
2. Starts `consumer-a` **on its own connection**, then publishes a **readiness
   probe** and blocks until `consumer-a` handles it — proving it is the active
   consumer before adding the standby `consumer-b` (on the main connection). No
   `time.Sleep` guessing. Both use `Concurrency(1)`.
3. Publishes the prefix `0..9`. The active consumer prints them in order.
4. After `consumer-a` accepts seq `9`, the example **closes its connection**
   (simulating an instance dying). The broker drops its subscription and promotes
   the standby. A second readiness probe blocks until `consumer-b` is confirmed
   active, then the suffix `10..19` is published — so the standby provably
   handles it, still in order.
5. Asserts the deduped accepted stream is exactly `0..19` — publish order ==
   handler order across the failover.

Run it against a local broker:

```sh
AMQP_URL=amqp://guest:guest@localhost:5672/ go run ./examples/ordered_consume
```

### Why close the connection instead of cancelling the context

Cancelling the `Consume` context stops local dispatch but does **not** send
`basic.cancel` or close the AMQP channel — the broker still sees the consumer as
active and never promotes the standby, so no failover happens (and an in-flight
unacked message would be stranded). Closing the consumer's **connection** is what
deregisters it at the broker, triggers the `SingleActiveConsumer` promotion, and
requeues any in-flight message to the standby. That is why each consumer runs on
its own connection — closing one models a real instance failure without
disturbing the publisher or the standby.

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
