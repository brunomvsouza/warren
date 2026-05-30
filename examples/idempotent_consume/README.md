# Idempotent consumer (dedupe by `MessageID`)

The canonical reference for the at-least-once dedupe pattern from **SPEC §6.2.1**.

Warren guarantees **at-least-once** delivery, never exactly-once. Duplicates are
produced *by design*:

- `PublishRetry` re-publishes on a transient error after the original may have
  already reached the broker.
- The reconnect barrier can replay an in-flight publish across a TCP reconnect.
- A confirm timeout (`ErrConfirmTimeout`) leaves the publish outcome unknown, so
  a retry is safe only because the consumer dedupes.

There is no "exactly-once" toggle. **The consumer is where exactly-once-effect is
achieved**, by deduplicating on the `MessageID` that Warren auto-populates
(UUIDv7) on every publish.

## What this example does

1. Declares an exchange + queue.
2. Runs a `ConsumeRaw` consumer (raw access is required to read
   `Delivery.MessageID()`; the value-typed `Consume` handler only sees the
   payload).
3. Keeps a bounded **LRU + TTL** cache keyed by `MessageID`. `markSeen` is an
   atomic check-and-insert: the first sight of an ID runs the business handler;
   every later sight is acked and skipped.
4. Publishes four messages whose `MessageID`s collapse to **three distinct**
   values — the second ID is published twice — and asserts the business handler
   ran **exactly once** per distinct ID.

Run it against a local broker:

```sh
AMQP_URL=amqp://guest:guest@localhost:5672/ go run ./examples/idempotent_consume
```

## When to use this pattern

Always, on every consumer, unless the downstream effect is *naturally*
idempotent (e.g. `SET key = value`, an upsert keyed by a business ID). If
processing a message twice would double-charge a card, send two emails, or
double-count a metric, you need dedupe.

## Cache sizing guidance

The cache is a **dedupe window**, not a permanent ledger. Size it to cover the
realistic redelivery horizon:

- **Capacity** (`~10k` here): the number of in-flight/recently-processed
  messages you expect within the TTL. Past capacity the LRU evicts the
  oldest-seen ID — an ID evicted before its duplicate arrives would be
  reprocessed, so capacity must exceed `peak_throughput × TTL`.
- **TTL** (`15 min` here): how long after first processing a duplicate can still
  plausibly arrive. Redeliveries from the reconnect barrier and `PublishRetry`
  land within seconds; a generous TTL costs only memory.

A safe rule of thumb: `capacity ≥ peak_msgs_per_second × TTL_seconds`, then add
headroom. At 1k msg/s and a 15-min TTL that is ~900k entries — tune both down to
your real redelivery window, or move to a shared store (below) before the
in-memory set gets large.

## Cross-process dedupe (multiple consumer instances)

This in-memory cache only dedupes within a single process. When you run **N**
consumer instances (the normal scale-out shape), a duplicate can be delivered to
a *different* instance than the original, so each instance's local cache misses
it. For cross-process dedupe, back the same `MessageID` key with a shared store:

- **Redis**: `SET dedupe:<MessageID> 1 NX EX <ttl_seconds>`. `NX` makes the
  insert atomic and tells you whether this instance is the first to see the ID;
  `EX` gives you the TTL window for free. This is the most common choice.
- **A database**: a unique constraint on `(message_id)` in the same transaction
  that performs the business write — a duplicate insert fails the constraint and
  you roll back/skip. This makes dedupe and the side-effect atomic, which the
  Redis approach does not (the side-effect and the `SET NX` are two steps).

Keep the in-memory cache in front of the shared store as a cheap first-level
filter if you like, but correctness across instances comes from the shared key.

## Related

- `examples/ordered_consume/` — strict per-queue ordering with
  `SingleActiveConsumer` + `Concurrency(1)`.
- SPEC §6.2.1 — the normative description of the dedupe contract.
