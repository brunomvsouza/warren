# T74 — Verification Gate Results (Lens-01, RabbitMQ 3.13 + 4.x)

This is the **ground-truth table** the Phase-12 protocol-correctness pass (Lens
01, `01-rabbitmq-amqp-protocol.md`) is built on. Several downstream tasks
(T75/T76/T58/T78/T80) make protocol claims that are only correct against an
*observed* broker, not a remembered one. T74 runs first and pins those
observations so every later task can cite a gate instead of re-asserting.

## How these were captured

The instrument is `gate_verification_integration_test.go`
(`TestGate_VerificationGates_integration`, `integration` build tag). It drives a
real broker over raw `amqp091` and reads the broker version from the management
API (`/api/overview`), so every assertion is version-differential. Each captured
value is emitted on a `GATE-RESULT` log line.

The integration broker image is overridable (see
`test/docker-compose.integration.yml` / `test/Dockerfile.rabbitmq-delayed`), so
the *same* lane runs against both versions:

```sh
# 3.13 (default)
make integration-up
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ \
AMQP_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672 \
  go test -race -v -count=1 -tags=integration \
    -run '^TestGate_VerificationGates_integration$' . | grep GATE-
make integration-down

# 4.x
WARREN_RMQ_IMAGE=rabbitmq:4.0-management \
WARREN_DELAYED_PLUGIN_URL=https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v4.0.7/rabbitmq_delayed_message_exchange-v4.0.7.ez \
WARREN_DELAYED_PLUGIN_SHA=9f746962d8f4e9ec2ce52fc86856859c30ed11abc67dd93cd80ebb3ef925d3fd \
WARREN_INTEGRATION_IMAGE=warren-rabbitmq-delayed:4.0 \
  make integration-up
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/ \
AMQP_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672 \
  go test -race -v -count=1 -tags=integration \
    -run '^TestGate_VerificationGates_integration$' . | grep GATE-
make integration-down
```

> **`-count=1` is mandatory when switching broker versions on one host.** Go's
> test cache keys on the test binary + the env vars the test reads, *not* on the
> external broker state. Without `-count=1`, the second version's run is served
> from the first version's cache (observed: a 4.0 run reported `version=3.13.7`
> from cache). In CI each version runs on its own fresh runner, so the cache is
> never shared across versions there.

## Versions observed

| | Version | Image |
|---|---|---|
| 3.13 LTS | **3.13.7** | `rabbitmq:3.13-management` + delayed-msg v3.13.0 |
| 4.x      | **4.0.9**  | `rabbitmq:4.0-management` + delayed-msg v4.0.7 |

## Results

| Gate | Question | 3.13.7 | 4.0.9 | Divergent? |
|---|---|---|---|---|
| **G1** | x-death delivery-limit **reason atom** | `delivery_limit` | `delivery_limit` | no |
| **G2** | x-death **count** accumulation shape | one entry per (queue, reason); `count` starts at **1** and sums | identical | no |
| **G3** | Does a **classic** queue accept/honour `x-delivery-limit`? | **declare rejected** — 406 `PRECONDITION_FAILED` ("invalid arg 'x-delivery-limit' … of queue type rabbit_classic_queue") | **declare rejected** — same 406 | no |
| **G4** | Which `{quorum, x-overflow, x-dead-letter-strategy}` declares are accepted? | **all 4 accepted** (incl. invalid `at-least-once`+`drop-head` and `at-least-once`+no-DLX) | **all 4 accepted** | no |
| **G5** | Broker `max_message_size` default (probe: 17 MiB publish) | **accepted** (≈128 MiB default) | **rejected** (16 MiB default) | **YES** |
| **G6** | Non-zero per-consumer `prefetch_size` on `basic.qos` | **rejected** — 540 `NOT_IMPLEMENTED` ("prefetch_size!=0 (1024)") | **rejected** — same 540 | no |

## Per-gate detail and the task each gates

### G1 — delivery-limit reason atom → **T75** (RMQ-01) — ✅ RESOLVED
The broker writes the x-death `reason` as **`delivery_limit`** (underscore) on
**both** 3.13.7 and 4.0.9. `internal/headers/xdeath.go` previously matched only
the hyphenated literal `"delivery-limit"`, so a real quorum delivery-limit
eviction went **uncounted** by `DeathCount()`.

**T75 fix:** `ParseXDeath` now normalises reason separators (`_`→`-`) on both the
stored reason and the `CountByReason` lookup key, so the broker's `delivery_limit`
counts toward `DeathCount()` and surfaces under the documented `"delivery-limit"`
spelling (and a caller may query either spelling). The fabricated `makeEntry(…,
"delivery-limit", …)` unit cases were rewritten to feed the real underscore atom,
and a real-broker test (`xdeath_delivery_limit_integration_test.go`) drives a
genuine quorum eviction and asserts the public `Death*` accessors against the
broker-authored header.

**Broker constraint discovered while building the test:** a `delivery_limit`
eviction is keyed on the **source** quorum queue, and the broker **never
redelivers** such a message back to that queue — a same-queue return (even via an
intermediate TTL hop with a differing `expired` reason) is silently dropped, and
a quorum queue re-evicts a message that already exceeded its limit. So the only
place the `delivery_limit` x-death entry is observable is the **DLQ**, where
warren's queue-scoped `DeathCount()` keys on the DLQ name (not the source) and
returns 0. The integration test captures the real DLQ header and replays it
scoped to the source queue via `NewDeliveryFixture`. This queue-scoping gap for
DLQ consumers is tracked as **LATER-92** (not a T75 blocker — RMQ-01 is the
reason-atom fix; cross-queue inspection is a separate API question). Contrast:
the same-queue **retry-loop** pattern (reason `rejected`/`expired`) *does*
re-arrive and is correctly counted.

### G2 — x-death count shape → **T78** (RMQ-03)
A single dead-letter event produces **one** x-death entry for the
`(queue, reason)` pair with `count == 1`. This is the shape `DeathCount()` sums
over (§6.3 "sum of the `count` sub-field"). T78's SPEC reconciliation can cite
this directly.

### G3 — classic + x-delivery-limit → **T58 / T81** (RMQ-01/RMQ-17)
`x-delivery-limit` is a **quorum-only** argument on both versions: a classic
queue declare carrying it is **rejected** with 406 `PRECONDITION_FAILED` — not
silently ignored, on **neither** 3.13.7 **nor** 4.0.9. (4.0.9 has **not** started
honouring `x-delivery-limit` on classic queues.) This confirms the SPEC §6.6
"classic queues do not honour x-delivery-limit" caveat and means the
version-aware delivery-limit warning (T58) only applies to quorum queues. The
4.x poison-loop **bound** assertion (T81/TV-11) must therefore ride a **quorum**
queue, not a classic one.

### G4 — quorum/overflow/at-least-once permutations → **T76** (RMQ-05) — ✅ RESOLVED
The broker is **permissive** on both versions: it accepts every combination,
including the semantically invalid `at-least-once`+`drop-head` and
`at-least-once` without a DLX. Therefore the `at-least-once` ⇒ `reject-publish`
coupling and the at-least-once-requires-DLX rule **cannot** be delegated to the
broker — T76 must enforce them **client-side** (`ErrInvalidOptions` on
`drop-head`, auto-set `reject-publish` with a warning), which is what its
acceptance criteria already prescribe. The canonical valid combo
(`quorum`+`reject-publish`+`at-least-once`+DLX) is accepted everywhere; the gate
asserts that invariant.

**T76 follow-up probe (extends G4):** the gate tested `drop-head` permutations;
T76 added a probe of the **full** overflow set and confirmed the broker also
accepts `at-least-once`+**`reject-publish-dlx`** on both 3.13.7 and 4.0.9. Since
RabbitMQ documents that at-least-once requires *exactly* `reject-publish`, T76's
client-side rule rejects **any** non-`reject-publish` overflow (both `drop-head`
and `reject-publish-dlx`) — not just `drop-head`.

**T76 fix:** `Topology` couples the two client-side. `expand()` injects
`x-dead-letter-strategy=at-least-once` (pre-existing) and now auto-sets
`x-overflow=reject-publish` when a quorum-at-least-once queue leaves overflow
unset; `validate()` returns `ErrInvalidOptions` for a conflicting explicit
overflow; `Declare` emits a warning on the auto-set that names the source-queue
memory cost. A shared `(*Topology).quorumAtLeastOnce` helper keeps validate /
expand / warn in lockstep, and a caller override of `x-dead-letter-strategy`
opts out. Verified by unit tests plus a real-broker integration test
(`topology_at_least_once_overflow_integration_test.go`) that reads the coupled
args back via the management API and drives a real dead-letter — **green on both
3.13.7 and 4.0.9**.

### G5 — max_message_size default → **T80** (RMQ-12/13)
The only **version-divergent** gate. A 17 MiB message is accepted on 3.13.7 and
**rejected** on 4.0.9, confirming the documented default drop from **128 MiB
(3.13)** to **16 MiB (4.0+)**. T80 fixes SPEC §6.1 to state the per-version
defaults and that publishing above the default needs the broker's
`max_message_size` raised (and corrects "131072 is the AMQP-spec minimum" →
"4096 is the minimum; 131072 the default").

### G6 — non-zero prefetch_size → **T78** (RMQ-14)
`basic.qos` with a non-zero per-consumer `prefetch_size` is **rejected** with
540 `NOT_IMPLEMENTED` ("prefetch_size!=0 (...)") on both versions — the broker
does **not** silently ignore it. warren already sends `prefetch_size = 0`
(`consumer.go`), so T78 documents that `PrefetchBytes` is dropped client-side
and the broker rejects a non-zero value (SPEC §6/§6.3), backed by a guard test.

## Gate → task index

| Gate | Downstream task(s) |
|---|---|
| G1 | T75 |
| G2 | T78 |
| G3 | T58, T81 |
| G4 | T76 |
| G5 | T80 |
| G6 | T78 |
