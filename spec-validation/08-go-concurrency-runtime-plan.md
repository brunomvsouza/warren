# Plan Input — Remediate Go Concurrency & Runtime-Correctness Findings (Lens 08)

> **For `/agent-skills:plan`.** This is a self-contained planning brief. It is the
> output of an adversarial spec-validation pass over `SPEC.md` from the
> Go concurrency & runtime-correctness lens
> (`spec-validation/08-go-concurrency-runtime.md`). Like Lenses 03/04/05/06/07, no
> findings report pre-existed — this brief was produced by *conducting* the review:
> every goroutine the contracts imply was traced (start owner → block points → stop
> signal → behaviour on panic / ctx-cancel / forced-close), every user callback was
> mapped to the goroutine it actually runs on, and every "race-free" / "condition
> variable" / "non-blocking dispatcher" / "idempotent close" claim was verified
> against the *implemented* code rather than taken as given. It enumerates confirmed
> hazards (`CR-01..CR-13`), each with evidence (SPEC §+line *and* `file:line`), a
> concrete interleaving, and a recommended SPEC amendment / runtime fix, grouped
> into workstreams and sequenced by dependency.
>
> **Numbering:** new task IDs are **T143–T146** (highest existing is T142, after
> Phase 18). Lens 08 becomes **Phase 19**, mirroring how Lenses 01..07 became Phases
> 12..18. **This lens is overwhelmingly cross-lens** — the concurrency machinery is
> already built and owned, so most findings **extend an existing task in place**
> (cross-lens rule: shared findings extend the shared task, never re-filed). **Nine
> prior tasks are extended** with a `Lens-08 (CR-xx)` acceptance bullet — **T07**
> (reconnect barrier / `sync.Cond` wording + `ForceReconnect` idempotency), **T08**
> (channel-pool fairness/starvation), **T13** (OnReturn invocation-goroutine wording),
> **T18** (non-blocking dispatcher "prefetch IS the bound"), **T20** (the counter-B
> lost-update Blocker), **T34c** (panic-recover at the supervisor/loop infra-goroutine
> boundaries), **T45** (1000-cycle goleak churn + §7/§9 reconcile), **T60** (the
> single-delivery double-verdict guard atomicity), **T70** (non-cooperative-handler
> shutdown detach). **One new `LATER.md` entry** (LATER-43: optional aggregate
> in-flight confirm window), gated on owner decision D4. **No new build-tag lane**
> (the gates CG-1..CG-6 ride the existing unit / `-race` / `integration` lanes; the
> reconnect-churn and double-verdict-cascade probes are broker-bound on the existing
> `integration` lane or run against `amqpmock`). The first Phase-19 task records SPEC
> §10 **Rev 18**.
>
> **Lens verdict: GO-WITH-CHANGES.** The concurrency architecture is fundamentally
> sound — the return/confirm demux is a single goroutine over an *intentionally*
> unbuffered return channel (R10-3, correct), every message-data buffer is bounded
> (dispatch by prefetch, confirm by batch size, pool by capacity), `started`/`Close`
> use `atomic.Bool` CAS / `sync.Once`, the confirm tracker resolves waiters via a
> one-shot non-blocking send (no double-resolve, no leak), and the barrier's AB/BA
> lock-order is explicitly handled. But the lens bar — *every goroutine has a clean
> stop, no user callback stalls a reader-fed or supervisor-critical goroutine, every
> "race-free"/"idempotent" claim is a real CAS not a check-then-act* — exposes **one
> must-fix Blocker**: a **logical lost-update** in the two-counter `MaxRedeliveries`
> map (`consumer.go:767→782` is a non-atomic `load`-then-`Store` under
> `Concurrency(n>1)`, so concurrent redeliveries of the same key undercount and a
> poison message can loop past its limit — and because `sync.Map` is memory-safe,
> `go test -race` **cannot** catch it, yet decision 12 / T20 claim "race-free,
> verified with `-race`"). Plus **one High** liveness footgun — a user `OnReturn`
> runs **inline on the unbuffered-return demux goroutine** (`publisher.go:226`), so a
> blocking callback stalls `amqp091`'s per-connection reader → heartbeats stop →
> the broker drops the socket → every publisher on it stalls — and the SPEC never
> names the invocation goroutine. The remaining eleven findings pin underspecified
> claims (the impossible-as-worded `sync.Cond` "cancellable via ctx", the
> "non-blocking dispatcher" with no stated bound, the double-verdict guard atomicity,
> the `100x` vs `1000` stress mismatch) and turn three silent leak/crash surfaces
> (panic at the supervisor goroutine boundary, ctx-ignoring handler past `Close`,
> non-FIFO pool starvation) into stated, tested contracts. No redesign is required.

---

## 1. Objective

Validate `SPEC.md` from the seat of a **Go concurrency specialist who reads every
contract as a set of goroutines, channels, mutexes, and `context.Context`
lifetimes** and asks, for each: *who starts it, who stops it, what does it block on,
and what happens on shutdown / panic / ctx-cancel?* The lens bar is concrete and
binary: *every goroutine must have a clean stop path; no user callback may run
synchronously on a reader-fed or supervisor-critical goroutine such that a slow or
panicking callback stalls the connection; every "race-free", "idempotent guard",
"condition variable", and "non-blocking dispatcher" claim must be a real primitive
(CAS / `sync.Once` / channel-select), not a property asserted over a check-then-act.*
The two highest-severity classes are **a contract that forces a race/deadlock** and
**a user callback on the connection-reader path**.

Concretely, the plan must:

1. **Close the counter-B lost-update (CR-02, Blocker).** Decision 12 (§6.3 L1290)
   mandates a "**race-free** map keyed by `(channel-instance-id, MessageID)`" and
   T20's acceptance says "must be race-free; verified with `go test -race`." The
   implementation (`consumer.go:767` `cs.load` → `consumer.go:782` `cs.m.Store`) is a
   **non-atomic read-modify-write** with no lock between the load and the store. Under
   `Concurrency(n>1)`, two handler goroutines processing redeliveries of the same key
   on the same channel both read `currentCount`, both write `currentCount+1` → a lost
   update → the in-process redelivery counter undercounts → `MaxRedeliveries` is
   exceeded and a poison message loops past its limit (defeating the very control the
   counter exists for). Because `sync.Map` is internally synchronised, **`-race` does
   not flag this** (it is a *logical* TOCTOU, not a memory race) — so the
   "verified with `-race`" wording is a false guarantee. Make the RMW atomic and
   replace the test with a *behavioural* concurrency assertion.

2. **Close the reader-fed-callback liveness hole (CR-01, High).** §6.2 (L870) says
   "the `OnReturn` callback fires synchronously before `Publish` unblocks" but **none
   of the four mentions names which goroutine runs it.** The implementation invokes
   the user callback **inline on the single return/confirm demux goroutine**
   (`publisher.go:226-228`), *before* `MarkReturned` (`:229`), and the return channel
   is **unbuffered by design** (R10-3, `publisher.go:206`) so `amqp091`'s
   per-connection reader is blocked on the send until this goroutine receives. A
   blocking / slow / panicking `OnReturn` therefore stalls the demux → stalls the
   connection reader → stops heartbeats → the broker drops the socket (~20 s) and
   every publisher on it stalls. There is no panic-recover around it
   (T34c's five panic-isolation sites do **not** include `OnReturn`). Name the
   invocation goroutine for *every* callback in the spec, state the
   must-not-block-on-the-reader-path contract, add the missing recover, and decide
   dispatch-async-vs-document for `OnReturn`.

3. **Pin the impossible-as-worded barrier primitive (CR-03).** §6.1 (L589-593) says
   publishers "block on a per-connection **condition variable** (still cancellable
   via `ctx`)." A raw `sync.Cond.Wait` is **not** ctx-selectable; the wording demands
   a primitive that cannot satisfy both clauses. The code achieves cancellability via
   a **watcher goroutine spawned per Wait iteration** that `Broadcast`s on
   `ctx.Done()` (`connection.go:597-604`) — functional, but churns a goroutine per
   blocked-publish wakeup under a reconnect storm, and the public wording is a
   contradiction. Pin the spec to the real mechanism and bound the watcher churn.

4. **Specify the double-verdict guard atomicity (CR-04, High).** R10-5/T60 names a
   "resolved-once guard on `Delivery[M]` mirroring `Batch[M]`" but never specifies its
   **atomicity**. The hazard is a *race*: a handler-timeout verdict fires on one
   goroutine while the handler calls `Ack` on another — unless the guard is a single
   atomic compare-and-swap, that is a check-then-act window and **both frames reach the
   broker** → `PRECONDITION_FAILED` → channel close → **every in-flight handler on
   that channel dies**. Today `Delivery` has **no guard at all** (`delivery.go:79-115`
   only checks `d.done`). Mandate a single CAS and a race+behavioural test.

5. **Stop the silent panic / leak surfaces (CR-05, CR-09).** No reconnect-loop or
   supervisor goroutine has a panic-recover at its boundary: a panic in the
   user-supplied `connect` fn (`internal/reconnect/loop.go:72`) or in the resubscribe
   hook that runs *on the supervisor* (`consumer.go:357`) **crashes the whole process**
   or **silently disables reconnect for that socket** — T34c covers user *callbacks*
   but not the infra-goroutine boundaries. And a handler that ignores its cancelled
   `ctx` **cannot be force-killed in Go**: it leaks a goroutine past `Close`, the
   timeout-drain (`<-handlerDone`) waits for it unboundedly, and the §7/§9
   goleak criteria ("1000 cycles → zero leaked goroutines") have **no carve-out** for
   it. State the boundary, detach the orphan, and exclude it from the goleak guarantee.

6. **Pin the remaining underspecified contracts and reconcile the test criteria
   (CR-06/CR-07/CR-08/CR-10/CR-11/CR-12).** State that the non-blocking dispatcher's
   sole bound is prefetch (no second queue); document the confirm tracker is bounded
   *per call*, not across concurrent `PublishBatch` invocations; document pool Acquire
   is best-effort (not FIFO) and can starve under permanent exhaustion; reconcile §7's
   "100x" with §9's "1000" connect/disconnect stress count and make the churn test
   real; document `ForceReconnect` idempotency; and pin that close-idempotency is
   enforced atomically (the code already is — lock it in).

This is **remediation of an existing spec + two real runtime fixes** (the counter-B
atomicity and the `OnReturn` invocation path), not a new feature. The only Blocker
(CR-02) is a logical-race correctness hole on an already-implemented consume path;
everything else is a runtime guard, a spec correction that locks behaviour the code
already gets right, or a test/wording reconciliation.

---

## 2. Source of truth & references

- `SPEC.md` — the document under remediation. **Re-confirm line numbers before
  editing** (project convention; the anchors below are this-pass snapshots).
  - §1 Reliability bar L37–86 (no silent message loss; no magic sleeps L284–285).
  - §6.1 Connection: per-conn supervisor **L551, L573**; reconnect barrier
    "**condition variable** … cancellable via `ctx`" **L589-593**; reconnect step
    order / `WithOnReconnect` synchronous **L606-608**; Close cascade (decision 53)
    **L640-647**; idempotent double-close "whichever runs second is a no-op"
    **L654-659**; `ForceReconnect` (only mention) **L606-608**; handler-ctx cancel on
    channel close **L633-639**.
  - §6.2 Publisher: demux "**single goroutine** … **unbuffered** return channel"
    (R10-3) **L872-885**; `OnReturn` "fires synchronously before `Publish` unblocks"
    **L870-871** (batch **L913-914**); `multiple=true` single pass **L860-863**;
    channel-close resolves "the affected publish" **L960-963**; channel-pool
    exhaustion **L954-959**; `PublishBatch` cap is **per-call only**, "does not
    currently throttle multiple concurrent `PublishBatch`" **L930-939**.
  - §6.3 Consumer: non-blocking dispatcher **L1212-1219**; `Concurrency(n)` fan-out
    **L1205-1210**; two-counter map (counter B, "race-free") **L1290-1300**.
  - §6.4 BatchConsumer: sequential per batch **L1393-1397**; `FlushAfter` timer
    (accumulator/timer concurrency never described) — open item R10-15 **L3133-3136**.
  - §7 Testing: `goleak` **L2231**; "-race / goleak / **100x** stress loop"
    **L2266-2269**.
  - §9 Success Criteria: "**1000** connect/disconnect cycles → zero leaked goroutines
    (was 100)" **L2457-2458**; reconnect barrier invariant **L2517-2520**;
    handler-ctx cancel on forced close **L2463-2465**.
  - §10 Decisions: **53** shutdown cascade **L2617-2619**; **27** reconnect barrier
    **L2811-2819**; **30** `Concurrency(n)` ordering **L2840-2846**; **22** handler-ctx
    cancel **L2759-2763**; **12** two-counter map **L2664-2674**; **R10-3** demux
    **L3060-3066**; **R10-5** double-verdict **L3078-3084**; **R10-6** channel recovery
    **L3085-3091**; **R10-8** unbounded barrier **L3098-3104**; Rev 10 heading L3031.

- **Code (ground truth confirmed this pass — extends, never duplicates):**
  - `consumer.go:767` `cs.load(counterBKey)` → `consumer.go:782`
    `cs.m.Store(counterBKey, currentCount+1)` — **non-atomic RMW** on the counter-B
    `sync.Map` (no lock between load and store); `redeliveryCounter` behind
    `atomic.Pointer` rotated per channel open (`consumer.go:435`). The dispatch path is
    concurrent (cap=`concurrency`), so the lost-update is reachable → **CR-02/T20**.
  - `publisher.go:206` `returnCh := ch.NotifyReturn(make(chan amqp091.Return))`
    (**unbuffered**, R10-3, comment L197-205); `publisher.go:208` the single demux
    goroutine; `publisher.go:226-228` `onReturn(ret)` invoked **inline before**
    `tracker.MarkReturned` (`:229`); `callOnReturn` converter `publisher.go:291`. **No
    panic-recover.** → **CR-01/T144** (+extend T13/T34c).
  - `connection.go:43` `barrierCond` is a real `sync.Cond`; `connection.go:593-625`
    `waitBarrier` spawns a watcher goroutine **per Wait iteration** (`:597-604`) to
    `Broadcast` on `ctx.Done()`; AB/BA lock-order handled (`:614-621`). → **CR-03/T07**.
  - `delivery.go:79-115` `Delivery.Ack`/`Nack` check only `d.done` (a check-then-act
    `select`), then call `raw.Ack`/`raw.Nack` — **no resolved-once verdict guard**
    (contrast `batch_consumer.go:47-92`, which has a mutex+`acked` guard). →
    **CR-04/T60**.
  - `internal/reconnect/loop.go:68` `run` — `defer close(l.done)` runs on panic, but
    there is **no `recover`** at the goroutine boundary, so a panic in the
    user-supplied `connect` (`:72`) **propagates to the top of the goroutine and
    crashes the process**; `consumer.go:357` / `batch_consumer.go:190` resubscribe hook
    runs **synchronously on the supervisor** inside `runBarrier` with no recover →
    panic kills the supervisor → reconnect silently disabled for that socket. →
    **CR-05/T34c**.
  - `connection.go:573` `onReconnect()` and `connection.go:461` `onBlocked(...)`
    invoked **inline on the supervisor goroutine** (OnReconnect *inside* the open
    barrier → holds every `Publish` on that socket); `connection.go:555`
    `onTopoDegraded` is the only lifecycle callback dispatched to its **own goroutine**
    (safe, but no recover, no ctx). → **CR-01/CR-05 evidence**.
  - `consumer.go:487` delivery-pump (bounded `out` chan, cap=prefetch); `consumer.go:416`
    per-message dispatch is **spawn-per-message** gated by a `sem` semaphore
    (cap=`concurrency`); `consumer.go:650` handler-timeout goroutine drained via
    `<-handlerDone`. The "non-blocking dispatcher" (§6.3 L1212) is implemented as
    "drain into a prefetch-bounded buffer" — but the spec never states *prefetch IS the
    bound*. → **CR-06/T18**.
  - `channelpool.go:57` ctx-aware Acquire over a token channel (Go channel receive is
    **not FIFO** across waiters); dead-channel guard via non-blocking `<-closeCh`
    (`:80, :122`); `publisher.go:80` publisher-conn pool same shape. → **CR-08/T08**.
  - `internal/confirms/tracker.go:211-214` one-shot resolve via `select{w.done<-err;
    default}` (correct, no double-resolve); `Wait` deletes its own pending entry on
    every exit (`:178-191`); `CloseAll` (`:126-134`) resolves buffer-empty waiters.
    **Bounded per call** by `PublishBatchMaxSize`; **not** bounded across concurrent
    `PublishBatch` (admitted §6.2 L930). → **CR-07/T145**.
  - `connection.go:237-242` Connection idempotent-close is **mutex+bool check-then-act
    (atomic via the mutex — correct)**; `consumer.go:265/350` `started` is `atomic.Bool`
    CAS; `consumer.go:262/822` Close is `sync.Once`. → **CR-12 (do-not-regress)**.
  - `OnCancel` (`consumer_builder.go:143`) is a **no-op stub** (impl deferred to
    T35/T36); there is **no `OnError`** callback anywhere; `rpc.go`/`delay.go` not yet
    implemented. (Scope note — these are not Lens-08 findings.)

### Cross-lens reconciliation (do **not** re-file — project rule)

Nine Lens-08 findings are already owned by a prior-lens task and must **extend** it
with a `Lens-08 (CR-xx)` acceptance bullet, never spawn a new task:

| Lens-08 finding | Already owned by | Action |
|---|---|---|
| **CR-02** counter-B non-atomic RMW (lost update under `Concurrency(n>1)`) | **T20** (Phase 3; MaxRedeliveries enforcement; acceptance already says "must be race-free; verified with `go test -race`") | **Extend T20**: make the load-increment-store atomic per key (per-channel mutex or lock-striped map); replace the `-race`-only check with a **behavioural** N-goroutine-same-key test asserting the exact final count; correct §6.3/decision 12 to say "atomic read-modify-write" and note `-race` proves memory-safety, not lost-update-freedom. |
| **CR-03** + **CR-11** reconnect barrier `sync.Cond` wording + `ForceReconnect` serialization | **T07** (Phase 2; Connection core + sync barrier; barrier is the `sync.Cond` here) | **Extend T07**: amend §6.1 to describe the *actual* ctx-cancellable mechanism (cond-var + ctx-watcher, or migrate to a channel-broadcast barrier) instead of the impossible "condition variable cancellable via ctx"; bound/pool the per-Wait-iteration watcher; document `ForceReconnect` is idempotent/coalesced (cap-1 channel) and safe to call during an in-progress reconnect. |
| **CR-04** double-verdict guard atomicity | **T60** (Phase 12, pulled to v0.1; R10-5 resolved-once guard on `Delivery[M]`) | **Extend T60**: specify the guard is a **single atomic CAS** (resolved-once; only the CAS winner emits a frame), explicitly not a check-then-act with the frame emitted outside the lock; add a race+behavioural test (timeout goroutine vs handler-`Ack` goroutine). |
| **CR-05** no panic-recover at supervisor / reconnect-loop / resubscribe-hook goroutine boundaries | **T34c** (Phase 11; panic isolation for user-provided callbacks — five sites) | **Extend T34c**: add the missing **`OnReturn`** recover site (CR-01) and add a recover at the **infra-goroutine boundaries** (`reconnect.Loop.run` around `connect`; the supervisor / `runBarrier` around the resubscribe hook) so a panic *degrades the socket* (fires `WithOnTopologyDegraded`) rather than crashing the process or silently disabling reconnect. |
| **CR-06** "non-blocking dispatcher": prefetch IS the bound (never stated) | **T18** (Phase 3; Consumer + non-blocking dispatcher) | **Extend T18**: amend §6.3 to state the dispatch buffer is prefetch-sized, prefetch is the *sole* bound, the reader blocks when it is full (that is the backpressure), there is **no second queue**, and `basic.cancel`/channel-close remain observable via the closed deliveries channel. |
| **CR-08** channel-pool Acquire not FIFO-fair → starvation | **T08** (Phase 2; channelpool.go + `ErrChannelPoolExhausted`) | **Extend T08**: document Acquire is best-effort (Go channel receive has no waiter ordering), starvation under *permanent* exhaustion is possible, recommend sizing the pool to peak concurrency; a FIFO wait queue is deferred. |
| **CR-09** ctx-ignoring handler leaks a goroutine past `Close`; goleak has no carve-out | **T70** (Phase 13; graceful-shutdown completeness; already carries Lens-02/05/06 bullets) | **Extend T70**: on a forced (ctx-deadline) close, *detach* a non-cooperative handler (bounded by the cascade ctx), increment a `consumer_handler_leaked_total`-style metric, and do not hang the cascade on it; the §7/§9 goleak carve-out wording lands in the capstone (T146). |
| **CR-10** §7 "100x" vs §9 "1000" stress mismatch; no churn stress test exists | **T45** (Phase 9; reconnect chaos test, `goleak.VerifyNone`) | **Extend T45**: add an explicit **1000-cycle connect/disconnect + confirm-churn** goleak sub-test; reconcile §7 (L2268 "100x") up to §9's "1000". |
| **CR-12** idempotent double-close asserts a property without a primitive | confirmed-correct in code (mutex/`Once`/CAS) | **do-not-regress** + a one-line §6.1 spec-pin in the **capstone (T146)**: close-idempotency is enforced atomically; add a concurrent double-`Close` `-race` test. |

Coordination (not re-file — distinct findings that touch adjacent code):
- **CR-01** (callback-goroutine contract) coordinates with **T13** (the `OnReturn`
  timing wording) and **T34c** (the `OnReturn` recover site): T144 owns the
  cross-cutting contract; it cites and extends both.
- **CR-09 / CR-13** coordinate with the §7/§9 criteria tasks **T41** (coverage gate)
  and **T45** (chaos test): T146 (capstone) owns the new concurrency success criteria
  and cites them.

---

## 3. Constraints & working agreements (planner must honour)

1. **Amend SPEC before code, same PR** (CLAUDE.md). Each task: update the cited
   §/decision, then add the runtime guard/test. The first Phase-19 task records
   §10 **Rev 18**.
2. **Cross-lens shared findings extend the owning task** — see the table above. Never
   re-file. T143/T144/T145/T146 are the only *new* tasks.
3. **A "race-free" claim must be a real primitive, not a property.** CR-02 and CR-04
   are the canonical violations: the fix is an atomic CAS / lock-held compound op, and
   the test must be **behavioural** (assert the invariant under concurrency), because
   `-race` proves memory-safety, **not** lost-update or double-emit freedom.
4. **No user callback may stall a reader-fed or supervisor-critical goroutine.** Every
   callback's invocation goroutine is named in the spec, and the connection-critical
   ones (`OnReturn` on the demux; `OnReconnect`/`OnBlocked`/resubscribe on the
   supervisor) get a must-not-block contract + a panic-recover; dispatch-off is an
   owner decision (D1).
5. **Every goroutine has a clean stop path.** The plan enumerates them (§11) and each
   task that touches one preserves its stop signal; the capstone asserts the inventory.
6. **No new build-tag lane** — gates ride the existing unit / `-race` / `integration`
   lanes (3.13 and 4.x where broker-bound), mirroring Lenses 04/05/06/07. Probes that
   need a broker (CR-01 stall, CR-04 cascade, CR-10 churn) run on `integration` or
   against the `amqpmock`/`amqptest` doubles (T37).
7. **testify + goleak** in every `_test.go`; route all new broker-touching errors
   through `internal/amqperror` (do not create ad-hoc reply-code errors).
8. **Additive-only on the public surface** (pre-v1). The counter-B fix, the
   double-verdict CAS, the panic-recovers, and the dispatcher/pool wording are
   internal or doc; a new `OnReturn` dispatch mode or a `WithMaxInFlightConfirms`
   option (D4) is additive and non-breaking.
9. **Distinguish the three states per finding** (lens rule): *the contract forces a
   race/deadlock* (CR-02, CR-04, CR-01), *underspecified — could go either way*
   (CR-03, CR-06, CR-07, CR-08, CR-10, CR-11), *fine — the model is sound, lock it in*
   (CR-12, and the do-not-regress list). The brief labels each.

---

## 4. Pre-work: verification gates (CG-1..CG-6 — sequence FIRST; they gate wording/fixes)

Each gate captures **ground truth** (a reproduced interleaving, a race/leak count, a
stall measurement) before any SPEC wording or fix lands (gate-first, mirroring
T84/T94/T101/T111/T119/T130). No downstream task edits the spec for its finding
before its gate returns. Results go into a committed table under `spec-validation/`
(cited by each downstream task).

| Gate | Question (ground truth to capture) | Lane | Blocks |
|------|-----------------------------------|------|--------|
| **CG-1** | Reproduce the counter-B lost update: N goroutines (`Concurrency(n>1)`) process redeliveries of the **same** counter-B key on one channel; assert the final stored count vs the true increment count. Confirm `go test -race` **passes** (no memory race) while the count is wrong (logical lost-update). Quantify how far past `MaxRedeliveries` a poison message can loop. | unit / `-race` | CR-02 / T20 |
| **CG-2** | Reproduce the `OnReturn` demux stall: register a blocking `OnReturn` (e.g. `time.Sleep`/lock), trigger a mandatory unroutable publish, and measure that (a) confirm resolution for other in-flight publishes on the same channel stalls, and (b) `amqp091`'s connection reader / heartbeats back up (broker drops the socket). Capture the timing. | integration | CR-01 / T144 |
| **CG-3** | Reproduce the double-verdict cascade (pre-T60): a handler that times out and then late-`Ack`s on a different goroutine; assert today it emits a second frame → `PRECONDITION_FAILED` → channel close → **sibling in-flight handlers on that channel die**. Then assert the atomic-CAS guard makes the late call a no-op. | integration / amqpmock | CR-04 / T60 |
| **CG-4** | Run **1000** connect/disconnect cycles + a confirm-churn loop under `goleak.VerifyNone`; capture the leaked-goroutine count (no such churn test exists today). Confirm every goroutine in the §11 inventory is joined. | integration | CR-10 / T45 |
| **CG-5** | Inject a panic in (a) the reconnect-loop `connect` fn and (b) the resubscribe hook that runs on the supervisor; observe the current blast radius (process crash for (a); silent reconnect-disable for (b)) and confirm the recover degrades the socket instead. | unit / amqpmock | CR-05 / T34c |
| **CG-6** | Under sustained pool exhaustion (waiters ≫ pool size, churning Acquire/Release), measure whether any waiter starves (max wait time / fairness); confirm Acquire is not FIFO. | unit | CR-08 / T08 |

Findings without a live gate (design/doc/test, ground truth already known from the
code audit): CR-03 (`sync.Cond` wording — design), CR-06 (dispatcher bound — doc +
test), CR-07 (tracker aggregate — doc), CR-09 (handler-leak carve-out — doc + test),
CR-11 (`ForceReconnect` — doc), CR-12 (double-close — do-not-regress), CR-13 (§9
criteria — capstone).

---

## 5. Workstreams (grouped findings, sequenced)

### WS-1 — The must-fix race *(fail-the-invariant → atomic)*

- **CR-02** (Blocker, race/lost-update) → **extend T20**. Make the counter-B
  read-modify-write atomic per key — hold a per-channel mutex across
  `load`-increment-`Store`, or use a lock-striped map keyed by `counterBKey`. Amend
  §6.3 / decision 12 from "race-free map" to "**atomic** read-modify-write" and add
  the caveat that `-race` proves memory-safety, not lost-update freedom. Replace the
  `-race`-only acceptance with a behavioural test: N goroutines increment the same key
  concurrently; assert the final count == N and that `MaxRedeliveries` is enforced
  *exactly*. *dep CG-1.*

### WS-2 — The reader-fed / supervisor-critical callback contract *(liveness)*

- **CR-01** (High, liveness) → **T144** (NEW). Establish the **callback
  invocation-goroutine contract** in §6.1/§6.2/§6.3: for *every* `On*` callback name
  the goroutine it runs on, and state that callbacks on the connection-reader path
  (`OnReturn` on the unbuffered-return demux) and the supervisor path
  (`OnReconnect`/`OnBlocked`/resubscribe — which run *inside* the open barrier) **must
  not block or do I/O** because they stall the connection / hold the barrier. Add the
  missing panic-recover for `OnReturn` (coordinate with T34c). Owner decides (D1)
  whether `OnReturn` is **dispatched to a bounded worker** (safe, but may fire after
  `Publish` unblocks — record the timing change) or kept **synchronous with a loud
  must-not-block doc + a watchdog**. Extends **T13** (the `OnReturn` timing wording now
  also names the goroutine). *dep CG-2.*
- **CR-05** (Med-High, leak/crash) → **extend T34c**. Add a `recover` at the
  infra-goroutine boundaries: `reconnect.Loop.run` around the user `connect`, and the
  supervisor / `runBarrier` around the resubscribe hook (and the supervisor select
  body). A panic must **degrade the socket** (fire `WithOnTopologyDegraded` + a
  metric), never crash the process or silently kill reconnect. *dep CG-5.*

### WS-3 — Pin the underspecified primitives *(underspecified → stated)*

- **CR-03 + CR-11** (Med-High + Low) → **extend T07**. Amend §6.1 to describe the
  *actual* ctx-cancellable barrier mechanism (cond-var + ctx-watcher, or a
  channel-broadcast barrier) instead of "condition variable cancellable via `ctx`";
  bound/pool the per-Wait-iteration watcher so a reconnect storm with K blocked
  publishers does not spawn K×iterations goroutines; document `ForceReconnect` is
  idempotent/coalesced and safe during an in-progress reconnect.
- **CR-04** (High, race/liveness) → **extend T60**. Specify the resolved-once guard on
  `Delivery[M]` is a **single atomic CAS** (only the winner emits a frame; the loser is
  a no-op), explicitly not a check-then-act; mirror/unify with the `Batch[M]` guard;
  add a race+behavioural test (timeout goroutine vs handler-`Ack`). *dep CG-3.*
- **CR-06** (Med, underspecified) → **extend T18**. State in §6.3 that the dispatch
  buffer is prefetch-sized, prefetch is the **sole** bound, the reader blocks when it
  is full (the backpressure), there is **no second queue**, and `basic.cancel` /
  channel-close stay observable via the closed deliveries channel even when all `n`
  slots are busy.
- **CR-08** (Med, underspecified) → **extend T08**. Document Acquire is best-effort
  (not FIFO), starvation under permanent exhaustion is possible, recommend sizing the
  pool to peak concurrency; FIFO wait queue deferred. *dep CG-6.*

### WS-4 — Supply / memory boundary *(new)*

- **CR-07** (Med, underspecified) → **T145** (NEW). Sharpen §6.2: the confirm-tracker
  memory bound is **per-call** (`PublishBatchMaxSize`), not aggregate — N concurrent
  `PublishBatch`/`Publish` calls hold N independent windows; under the "billions/day"
  bar that is an unbounded growth surface owned by the *publisher*. Document the
  boundary and recommend caller-side fan-out limiting; file **LATER-43** for an
  optional aggregate in-flight window (`WithMaxInFlightConfirms`). Owner decides (D4)
  ship-now vs document+defer.

### WS-5 — Test-criteria reconciliation & capstone *(test gap)*

- **CR-09** (Med, leak) → **extend T70**. On forced (ctx-deadline) close, *detach* a
  non-cooperative handler (bounded by the cascade ctx), increment a leaked-handler
  metric, and do not hang the cascade on it.
- **CR-10** (Low, test) → **extend T45**. Add the explicit 1000-cycle connect/disconnect
  + confirm-churn goleak sub-test; reconcile §7 (L2268 "100x") up to §9's "1000". *dep
  CG-4.*
- **CR-12** (Low, do-not-regress) → folded into **T146**. One-line §6.1 spec-pin that
  close-idempotency is atomic; add a concurrent double-`Close` `-race` test.
- **CR-13** (Low, high-leverage) → **T146** (NEW, capstone). Close the §7/§9
  concurrency-criteria gap: add a **goroutine-inventory appendix** (every long-lived
  goroutine + its start owner + stop signal) and success criteria/tests for the
  counter-B atomicity (CR-02), the double-verdict CAS (CR-04), the
  `OnReturn`-must-not-block contract (CR-01), the supervisor panic-degrade (CR-05),
  the non-cooperative-handler carve-out (CR-09), pool starvation (CR-08), and the
  1000-cycle churn (CR-10). Depends on WS-1..WS-5 landing the controls.

---

## 6. Do-not-regress list (confirmed-correct — protect with tests)

These are the contracts the audit found **already correct**. Reverting any flips the
lens toward NO-GO; each must keep (or gain) a guard test.

1. **The return/confirm demux is a single goroutine over an *intentionally*
   unbuffered return channel** (`publisher.go:206`, R10-3) — this serialises
   return-before-ack and prevents the ~50% silent-loss race. (CR-01 fixes the *user
   callback* on this goroutine, **not** the demux design — the unbuffered channel
   stays.)
2. **The confirm tracker resolves waiters via a one-shot non-blocking send**
   (`tracker.go:211-214`); `Wait` deletes its own entry on every exit (`:178-191`);
   `CloseAll` (`:126-134`) resolves outstanding waiters with no double-resolve and no
   leak. (CR-07 is about *aggregate* memory across calls, not the per-waiter mechanism.)
3. **Every message-data buffer is bounded.** Delivery-pump `out` chan = prefetch
   (`consumer.go:487`); dispatch concurrency = `sem` cap=`concurrency`
   (`consumer.go:416`); confirm buffer ≤ `channelPoolSizeMax`; pool by capacity. The
   only unbounded surface is per-call tracker memory across concurrent publishers
   (CR-07).
4. **`started` and `Close` are atomic.** `Consumer.started`/`BatchConsumer.started`
   are `atomic.Bool` CAS (`consumer.go:265/350`); component `Close` is `sync.Once`
   (`consumer.go:262/822`); Connection/Publisher close is mutex+bool (atomic via the
   mutex). (CR-12 only pins the spec wording to match.)
5. **The `Batch[M]` resolved-once guard is correct** (`batch_consumer.go:47-92`):
   mutex+`acked`, the frame is emitted outside the lock but only the one path that set
   `acked` reaches the emit, and the timeout path re-checks `acked` under the lock.
   (CR-04 brings `Delivery[M]` up to this bar with a CAS.)
6. **The barrier AB/BA lock order is explicitly handled** (`connection.go:614-621`):
   `barrierMu` is dropped before `mu` is taken. (CR-03 fixes the *wording* and the
   watcher churn, not this ordering.)
7. **`onTopoDegraded` is dispatched to its own goroutine** (`connection.go:555`), so it
   does not stall the supervisor or the barrier. (CR-01/CR-05 add a recover; the
   dispatch is already right.)
8. **The dead-channel guard works**: `getOrOpen`/`release` non-blocking-check
   `<-closeCh` (`channelpool.go:80,122`) so a closed channel is not handed back live.
   (CR-08 is about *fairness*, not the liveness guard.)
9. **The reconnect-loop backoff sleep is interruptible** (`internal/reconnect/loop.go:88`
   `select{ctx.Done, t.C}`) and the supervisor stops on `mc.cancel()`. (CR-05 adds
   panic-safety, not a stop path — the stop path exists.)

---

## 7. Routing matrix (each finding → disposition)

| Finding | Sev | Class | Disposition | Task |
|---------|-----|-------|-------------|------|
| **CR-02** counter-B non-atomic RMW → MaxRedeliveries lost-update | **Blocker** (High) | race (lost-update) | **CROSS-LENS — extend T20** (atomic RMW + behavioural test) | **T20** (dep CG-1) |
| **CR-01** `OnReturn` inline on unbuffered-return demux = connection-killer; SPEC never names the goroutine | High | liveness | NEW — callback-goroutine contract + dispatch-vs-doc (D1); extends T13/T34c | **T144** (dep CG-2) |
| **CR-04** `Delivery` double-verdict guard atomicity unspecified (absent today) | High | race/liveness | **CROSS-LENS — extend T60** (atomic CAS + test) | **T60** (dep CG-3) |
| **CR-03** barrier `sync.Cond` "cancellable via ctx" impossible-as-worded; watcher-per-wait churn | Med-High | underspecified/liveness | **CROSS-LENS — extend T07** | **T07** |
| **CR-05** no panic-recover at supervisor / loop / resubscribe goroutine boundaries | Med-High | leak/crash | **CROSS-LENS — extend T34c** | **T34c** (dep CG-5) |
| **CR-06** "non-blocking dispatcher" bound unstated (prefetch IS the bound) | Med | underspecified | **CROSS-LENS — extend T18** | **T18** |
| **CR-07** confirm-tracker memory bounded per-call only (concurrent `PublishBatch` unbounded) | Med | underspecified | NEW — doc boundary + LATER-43 | **T145** |
| **CR-08** channel-pool Acquire not FIFO → starvation | Med (Low-Med) | underspecified | **CROSS-LENS — extend T08** | **T08** (dep CG-6) |
| **CR-09** ctx-ignoring handler leaks goroutine past Close; goleak no carve-out | Med | leak | **CROSS-LENS — extend T70** (+ §7 carve-out in T146) | **T70** |
| **CR-10** §7 "100x" vs §9 "1000" mismatch; no churn stress test | Low | test/underspecified | **CROSS-LENS — extend T45** | **T45** (dep CG-4) |
| **CR-11** `ForceReconnect` serialization unspecified (code coalesces) | Low | underspecified | **CROSS-LENS — extend T07** (with CR-03) | **T07** |
| **CR-12** idempotent double-close asserts property w/o primitive (code correct) | Low | do-not-regress | spec-pin in capstone | **T146** |
| **CR-13** §7/§9 concurrency criteria + goroutine-inventory appendix | Low (high-leverage) | test gap | NEW — capstone | **T146** |

Every `CR-01..CR-13` is accounted for exactly once: CR-01→T144, CR-02→T20
(cross-lens), CR-03→T07 (cross-lens), CR-04→T60 (cross-lens), CR-05→T34c
(cross-lens), CR-06→T18 (cross-lens), CR-07→T145, CR-08→T08 (cross-lens),
CR-09→T70 (cross-lens), CR-10→T45 (cross-lens), CR-11→T07 (cross-lens),
CR-12→T146, CR-13→T146.

**New tasks (Phase 19):** T143 (gates CG-1..CG-6; records §10 Rev 18), T144 (CR-01
callback-goroutine contract), T145 (CR-07 tracker aggregate boundary + LATER-43),
T146 (CR-13 capstone + CR-12 pin). **Extended in place:** T07, T08, T13, T18, T20,
T34c, T45, T60, T70.

---

## 8. Suggested phasing (planner may revise — "Phase 19")

- **A — Gates (T143).** Stand up CG-1..CG-6 ground truth into a committed results
  table; records §10 **Rev 18**. No new build-tag lane.
- **B — Must-fix race (T20/CR-02).** The counter-B atomic RMW + behavioural test —
  the Blocker.
- **C — Liveness contract (T144/CR-01 + T34c/CR-05).** The callback-goroutine
  contract + `OnReturn` recover/dispatch decision; panic-recover at the infra-goroutine
  boundaries.
- **D — Pin the primitives (T07/CR-03+CR-11, T60/CR-04, T18/CR-06, T08/CR-08).**
  Barrier mechanism + `ForceReconnect`, double-verdict CAS, dispatcher bound, pool
  fairness.
- **E — Boundaries, tests & capstone (T145/CR-07, T70/CR-09, T45/CR-10, T146/CR-12+CR-13).**
  Tracker aggregate boundary + LATER-43, handler-leak detach, 1000-cycle churn,
  §7/§9 reconcile + goroutine-inventory appendix.

Sequencing notes: T20 depends on CG-1; T144 on CG-2; T60 on CG-3; T34c on CG-5;
T08 on CG-6; T45 on CG-4. T07/T18/T70/T145 are gate-independent (doc/test). T146
lands last (it asserts the controls A–E added).

---

## 9. Acceptance criteria for the whole effort

- **Counter-B race closed (CR-02):** a behavioural test runs N goroutines incrementing
  the same counter-B key concurrently and asserts the final count == N and that
  `MaxRedeliveries` is enforced exactly (no poison message loops past the limit); the
  RMW is atomic (per-key mutex / lock-striped map); §6.3/decision 12 say "atomic
  read-modify-write" and note `-race` proves memory-safety only.
- **Callback liveness contract (CR-01):** §6.1/§6.2/§6.3 name the invocation goroutine
  for every `On*` callback and state the must-not-block contract for the
  reader-fed/supervisor-critical ones; `OnReturn` has a panic-recover; a test asserts a
  blocking `OnReturn` no longer stalls confirms on the channel (per D1: either
  dispatched off the demux, or a watchdog/loud-doc with the timing documented).
- **Double-verdict atomicity (CR-04):** the `Delivery[M]` resolved-once guard is a
  single atomic CAS; a race test (timeout goroutine vs handler-`Ack`) asserts exactly
  one frame is emitted and the late call is a no-op (no `PRECONDITION_FAILED`, no
  channel-close cascade).
- **Barrier & ForceReconnect (CR-03/CR-11):** §6.1 describes the real ctx-cancellable
  mechanism (no "condition variable cancellable via ctx" contradiction); the watcher
  churn is bounded; `ForceReconnect` is documented idempotent/coalesced; a test asserts
  a ctx-cancel during the barrier returns `ErrReconnecting` without a goroutine leak.
- **Panic-safety (CR-05):** a panic in the reconnect `connect` fn or the resubscribe
  hook degrades the socket (fires `WithOnTopologyDegraded` + metric), does not crash
  the process or silently disable reconnect (chaos test).
- **Dispatcher & pool (CR-06/CR-08):** §6.3 states prefetch is the sole dispatch bound
  (no second queue); §6.2 documents Acquire is best-effort with a starvation caveat;
  tests assert the dispatch buffer == prefetch and that `basic.cancel` is observed
  with all slots busy.
- **Boundaries & leaks (CR-07/CR-09):** §6.2 documents the per-call (not aggregate)
  tracker bound (+ LATER-43 or `WithMaxInFlightConfirms`); a forced close detaches a
  non-cooperative handler, increments the leaked-handler metric, and does not hang the
  cascade; §7/§9 carve out the ctx-ignoring handler from the goleak guarantee.
- **Stress reconciled (CR-10):** §7 and §9 agree on **1000** cycles; a 1000-cycle
  connect/disconnect + confirm-churn `goleak.VerifyNone` test is green.
- **Capstone (CR-12/CR-13):** §6.1 pins atomic close-idempotency (+ double-`Close`
  `-race` test); §7/§9 carry a goroutine-inventory appendix and the new concurrency
  success criteria, each with a backing test.
- **Project gates green:** `make lint test` (race + cover), `goleak.VerifyNone`,
  integration on 3.13 **and** 4.x. `README.md` reflects any changed external contract
  (the `OnReturn` callback contract, a new `WithMaxInFlightConfirms` option if D4 ships
  it).

---

## 10. Open questions for the owner (decisions the planner needs to record)

1. **D1 — CR-01 `OnReturn` invocation.** Recommend for v0.1: keep `MarkReturned`
   synchronous on the demux (R10-3 is load-bearing), **add the missing panic-recover**,
   and **dispatch the user `OnReturn` to a bounded (1-deep) per-publisher worker** so a
   blocking callback can never stall the connection reader — documenting that `OnReturn`
   may then fire *concurrently with / shortly after* `Publish` unblocks (a timing change
   from the current "synchronously before"). Alternative: keep it synchronous with a
   loud must-not-block doc + a watchdog that logs/degrades if the callback exceeds a
   deadline. Owner picks; the dispatch-off option is the safer default at the
   billions/day bar.
2. **D2 — CR-02 counter-B atomicity mechanism.** Recommend a **per-channel mutex held
   across load-increment-store** (simplest correct; the dispatch path already has the
   `redeliveryCounter` per channel) over a lock-striped map; the test must be
   behavioural, not `-race`-only.
3. **D3 — CR-04 double-verdict guard primitive.** Recommend a **single `atomic.Bool`
   CAS** (resolved-once; only the winner emits) for `Delivery[M]`, and consider
   unifying `Batch[M]` onto the same primitive. Confirm against CG-3's observed cascade.
4. **D4 — CR-07 confirm-tracker aggregate cap.** Recommend **document the per-call
   boundary now + defer the aggregate window to LATER-43** for v0.1 (the per-call cap
   bounds the common case; aggregate growth is a caller-fan-out concern). Owner may pull
   `WithMaxInFlightConfirms` forward if a fan-out deployment needs it.
5. **D5 — CR-09 non-cooperative-handler shutdown.** Recommend **detach after the
   cascade ctx deadline + increment a `consumer_handler_leaked_total` metric +
   document that Go cannot force-kill a goroutine**, and **exclude** the ctx-ignoring
   handler from the library's goleak guarantee (it is a caller defect). Do not hang the
   cascade on it.

**Proposed LATER entries:** **LATER-43** — optional aggregate in-flight confirm window
(`WithMaxInFlightConfirms`) to bound confirm-tracker memory across concurrent
`PublishBatch`/`Publish` calls, prereq T145 (conditional on D4 = defer).

---

## 11. Goroutine inventory (lens output #1)

Every long-lived goroutine the contracts imply, with its stop path. *A goroutine with
no clean stop is a leak finding.* All confirmed against the implemented code.

| Goroutine (file:line) | Started by | Blocks on | Stop signal | On panic | On ctx-cancel | Risk |
|---|---|---|---|---|---|---|
| reconnect `Loop.run` (`internal/reconnect/loop.go:68`) | `reconnect.New` | `connect()`, then `select{ctx.Done, timer.C}` | internal ctx via `Stop()` / parent ctx / maxRetries | **no recover** → process crash (defer closes `done` first) | returns, closes `done` | **CR-05** — no panic boundary |
| managedConn `supervisor` (`connection.go:429`) | `openPool` | `select{ctx.Done, blockedCh, closeCh, forceReconnectCh}`; inside reconnect `<-loop.Done()` | `mc.cancel()` (by `Connection.Close`) | **no recover** → kills supervisor → reconnect disabled | closes `raw`, closes `mc.done` | **CR-05** — resubscribe hook runs here |
| publisher confirm+return **demux** (`publisher.go:208`) | `openPublisherEntry` | `select{returnCh(unbuffered), confirmCh}` | `confirmCh` close → `tracker.CloseAll(); return` | **no recover**; user `OnReturn` inline | not ctx-aware (stops on channel close) | **CR-01** — connection-killer |
| consumer delivery-pump (`consumer.go:487`) | `openDeliveryCh` | `select{deliveries, cancelCh, closeCh, ctx.Done}`; inner `out<-d` | ctx cancel / channel close; `defer close(out)` | no user code | returns | sound (bounded `out`=prefetch); CR-06 wording |
| consumer per-message dispatch (`consumer.go:416`) | `runConsume` (**spawn-per-message**, gated by `sem` cap=concurrency) | `sem<-{}` then handler | `wg.Wait()` on ctx cancel | recovered in `safeCallHandler` | inherits ctx; per-handler `WithCancelCause`(+timeout) | sound; CR-09 if handler ignores ctx |
| consumer handler-timeout goroutine (`consumer.go:650`) | `dispatch` (when `handlerTimeout>0`) | `handlerDone<-` (cap 1) | always drained (`<-handlerDone`) | recovered | `cancelCause` then drain | **CR-09** — drain waits unboundedly on a non-cooperative handler |
| batch flush loop (`batch_consumer.go:348`) | caller (foreground) | `select{ctx.Done, resubCh, flushCh, cur.ch}` | ctx cancel (flushes, returns) | recovered in `safeCallBatchHandler` | flushes, returns | sound |
| batch delivery-pump (`batch_consumer.go:623`) | `openBatchDeliveryCh` | `select{deliveries, closeCh, ctx.Done}` | ctx cancel / channel close; `defer close(out)` | no user code | returns | sound |
| batch handler-timeout goroutine (`batch_consumer.go:273`) | `flush` (when `handlerTimeout>0`) | `handlerDone<-` (cap 1) | always drained | recovered | `hCancel()` then drain | same CR-09 caveat |
| resubscribe hook (`consumer.go:357` / `batch_consumer.go:190`) | **runs ON the supervisor inside `runBarrier`** (not its own goroutine) | `time.After(jitter)`, `openDeliveryCh`, `resubCh<-` | `hookCtx.Done()` | **no recover** → kills supervisor | returns `hookCtx.Err()` | **CR-05** — panic on supervisor |
| `onTopoDegraded` callback goroutine (`connection.go:555`) | `runBarrier` (degraded transition) | user `onTopoDegraded(err)` | fire-and-forget; tracked by `mc.wg` | **no recover** (user panic) | not cancellable (no ctx) | safe-dispatched; CR-05 adds recover |
| `waitBarrier` ctx-watcher (`connection.go:598`) | `waitBarrier` — **one per Wait iteration** | `select{ctx.Done, done}` | `close(done)` after `Cond.Wait` returns | no user code | `Broadcast`s to wake the Wait | **CR-03** — churn under reconnect storm |
| Connection.Close drainer (`connection.go:269`) | `Connection.Close` | `mc.wg.Wait()` | runs to completion / Close ctx | no recover | Close returns timeout; goroutine lingers | documented; CR-09-adjacent |

---

## 12. Callback inventory (lens output #2)

Every user callback and the goroutine it actually runs on. *The critical column is
"runs on which goroutine."* Confirmed against the call sites.

| Callback | Invocation site (file:line) | Runs on which goroutine | Synchronous? | If user code blocks here, what stalls? | Finding |
|---|---|---|---|---|---|
| **OnReturn** | `publisher.go:226-228` → `callOnReturn` (`:291`) | **the confirm+return demux** (reader-fed: `returnCh` **unbuffered**) | yes, inline before `MarkReturned` | the whole confirm/return demux for that channel → all `tracker.Wait` block to `ConfirmTimeout`; and because `returnCh` is unbuffered, **`amqp091`'s per-connection reader stalls → heartbeats stop → broker drops the socket** | **CR-01** (High) |
| **OnReconnect** | `connection.go:573` (inside `runBarrier`) | **the supervisor** | yes, inline, last barrier step | **holds the reconnect barrier open** → every `Publish`/`waitBarrier` on that socket blocks until it returns | CR-01 (doc) / CR-05 (recover) |
| **OnBlocked** | `connection.go:461` | **the supervisor** (reader-fed via `NotifyBlocked`) | yes, inline | the supervisor select loop → reconnect detection for that socket is paused while it runs | CR-01 (doc) / CR-05 (recover) |
| **OnResubscribe** (internal hook; no public option) | `consumer.go:357` / `batch_consumer.go:190` | **the supervisor** (inside `runBarrier`) | yes, inline | holds the barrier (jitter + `openDeliveryCh` before it clears) | CR-05 |
| **OnTopologyDegraded** | `connection.go:555` (`go func`) | **a separate dispatched goroutine** | no — fire-and-forget | only stalls `Connection.Close` (bounded by Close ctx) — **safe-dispatched** | do-not-regress (CR-05 adds recover) |
| **Handler** / **RawHandler** | `consumer.go:617` (inline) / `:650` (timeout goroutine) via `safeCallHandler` | per-message dispatch goroutine (bounded by `sem`) | per-message | one dispatch slot; all slots full → `runConsume` blocks at `sem<-` (correct backpressure = prefetch). Panic recovered. | CR-06 (bound) / CR-09 (ctx-ignore) |
| **BatchHandler** | `batch_consumer.go:273` (timeout) / `:344` (inline) via `safeCallBatchHandler` | the batch flush loop (sequential) | yes | the whole batch consumer loop (no new accumulate/flush until it returns). Panic recovered. | CR-09 (ctx-ignore) |
| **OnCancel** | `consumer_builder.go:143` | — | — | **no-op stub** (impl deferred to T35/T36) — not a Lens-08 finding | — |
| **OnError** | — | — | — | **does not exist** | — |

---

## 13. Findings table (lens output #3)

`Severity` Blocker/High/Med/Low · `Class` race / deadlock / leak / liveness /
underspecified.

| ID | Sev | Class | Location (§+lines / file:line) | Concurrency scenario (concrete interleaving) | Recommended SPEC amendment / fix | Task |
|----|-----|-------|-------------------------------|----------------------------------------------|----------------------------------|------|
| **CR-02** | **Blocker** | race (lost-update) | §6.3 L1290-1300; dec.12 L2664-2674; `consumer.go:767→782` | G1 and G2 (both handler goroutines, `Concurrency(n>1)`) process redeliveries of the same key: both run `cs.load`→read `c`; both run `cs.m.Store(c+1)` → one increment lost → counter undercounts → `MaxRedeliveries` exceeded; poison loops past limit. `sync.Map` is memory-safe so `-race` **passes**. | atomic RMW (per-channel mutex / lock-striped map); §6.3/dec.12 "atomic read-modify-write"; behavioural N-goroutine test (not `-race`-only) | **T20** |
| **CR-01** | High | liveness | §6.2 L870-871/L913-914; `publisher.go:206/226-229` | a mandatory unroutable publish returns; the demux calls user `OnReturn` **inline** (`:226`) before `MarkReturned` (`:229`); the user callback blocks (lock/I/O); the **unbuffered** `returnCh` keeps `amqp091`'s reader blocked → heartbeats stop → broker drops the socket → every publisher on it stalls. No recover. | name the invocation goroutine for every callback; must-not-block contract on reader-fed/supervisor paths; recover for `OnReturn`; dispatch-off vs doc (D1) | **T144** |
| **CR-04** | High | race/liveness | §10 R10-5 L3078-3084; `delivery.go:79-115` | timeout goroutine emits `Nack` while the handler goroutine emits `Ack`; with only a check-then-act `d.done` test (no resolved-once guard), **both frames** reach the broker → `PRECONDITION_FAILED` → channel close → all in-flight handlers on that channel die. | single atomic CAS resolved-once guard on `Delivery[M]`; race+behavioural test | **T60** |
| **CR-03** | Med-High | underspecified/liveness | §6.1 L589-593; dec.27 L2811-2819; `connection.go:43/597-604` | the spec promises a "condition variable … cancellable via `ctx`"; a raw `sync.Cond.Wait` cannot select on `ctx.Done()`; the code spawns a watcher goroutine **per Wait iteration** — under a reconnect storm with K blocked publishers that is K×iterations goroutines. | describe the real mechanism (cond-var+watcher or channel-broadcast); bound/pool the watcher; document `ForceReconnect` idempotency (CR-11) | **T07** |
| **CR-05** | Med-High | leak/crash | `internal/reconnect/loop.go:68/72`; `connection.go:429`; `consumer.go:357` | a panic in the user `connect` fn unwinds to the top of `Loop.run` (no recover) → **process crash**; a panic in the resubscribe hook (runs on the supervisor) → **supervisor dies → reconnect silently disabled** for that socket. | recover at the infra-goroutine boundaries → degrade the socket (`WithOnTopologyDegraded` + metric), not crash/silently-disable | **T34c** |
| **CR-06** | Med | underspecified | §6.3 L1212-1219; dec.30 L2840-2846; `consumer.go:487` | spec says "the reader loop does not block waiting for a free handler slot" but never says where the delivery goes when all `n` slots are busy → a future maintainer could add an unbounded buffer (blowup) or drop a delivery (loss). | state prefetch IS the sole bound; no second queue; `basic.cancel`/close stay observable | **T18** |
| **CR-07** | Med | underspecified | §6.2 L930-939; `tracker.go` | N concurrent `PublishBatch` calls each hold a `PublishBatchMaxSize` window; the cap is per-call, so aggregate tracker memory is unbounded under fan-out (admitted by the spec). | document the per-call boundary; recommend caller fan-out limit; LATER-43 for `WithMaxInFlightConfirms` | **T145** |
| **CR-08** | Med | underspecified | §6.2 L954-959; `channelpool.go:57` | under permanent pool exhaustion, Go channel receive has no waiter ordering → an unlucky Acquire can starve while others churn. | document best-effort (non-FIFO) Acquire + starvation caveat; recommend pool sizing | **T08** |
| **CR-09** | Med | leak | §6.1 L633-639; §7 L2266-2269; `consumer.go:650` | a handler ignores its cancelled `ctx`; on `Close` it cannot be force-killed → goroutine leaks past `Close`, the timeout-drain waits for it, goleak (no carve-out) fails. | detach the orphan (bounded by cascade ctx) + leaked-handler metric; §7/§9 carve-out (caller defect) | **T70** |
| **CR-10** | Low | test/underspecified | §7 L2266-2269 ("100x") vs §9 L2457-2458 ("1000") | the two stress criteria disagree on the cycle count; no churn test exists. | reconcile §7→1000; add the 1000-cycle connect/disconnect + confirm-churn goleak test | **T45** |
| **CR-11** | Low | underspecified | §6.1 L606-608 | `ForceReconnect` is mentioned once; concurrent calls racing an in-progress reconnect are unspecified (the code coalesces via a cap-1 channel). | document idempotent/coalesced, safe during an in-progress reconnect | **T07** |
| **CR-12** | Low | do-not-regress | §6.1 L654-659; `connection.go:237-242` | "whichever runs second is a no-op" asserts a property; the code is correct (mutex/`Once`/CAS) but the spec names no primitive. | pin atomic close-idempotency; add a concurrent double-`Close` `-race` test | **T146** |
| **CR-13** | Low | test gap | §7/§9 L2266-2269/L2457-2465 | the criteria lack a goroutine-inventory / per-goroutine-stop-path assertion and behavioural tests for the new guards. | add the goroutine-inventory appendix + concurrency success criteria/tests (capstone) | **T146** |

---

## 14. Out of scope for this plan

- Other validation lenses (09 performance, 10 test-strategy, 11 compliance, 12 DX, 13
  load) — separate passes.
- `rpc.go` / `delay.go` concurrency (not yet implemented; future tasks).
- `OnCancel` callback wiring (no-op stub today; T35/T36 own the implementation).
- Replacing the demux's unbuffered return channel — it is **load-bearing** (R10-3) and
  stays; CR-01 fixes only the *user callback* on that goroutine.
- A FIFO channel-pool wait queue (CR-08 documents the caveat; the FIFO queue is
  deferred).
- Stream-protocol concurrency (v0.2, decision 24).
- Any change to the §10 API north stars beyond the corrections above. Cross-lens shared
  findings extend the owning task (T07/T08/T13/T18/T20/T34c/T45/T60/T70), never
  re-filed.

---

## 15. Verdict for this lens

**GO-WITH-CHANGES.** The concurrency architecture is sound where it counts: the
return/confirm demux is a single goroutine over an intentionally unbuffered return
channel (R10-3), every message-data buffer is bounded (prefetch, batch size, pool
capacity), the confirm tracker resolves waiters via a one-shot send with no leak,
`started`/`Close` use `atomic.Bool`/`sync.Once`, the `Batch[M]` guard is correct, and
the barrier's AB/BA lock order is explicitly handled. But the lens bar — *every
goroutine has a clean stop, no user callback stalls a reader-fed or
supervisor-critical goroutine, and every "race-free"/"idempotent" claim is a real
primitive* — exposes **one must-fix Blocker** (CR-02: the counter-B `load`-then-`Store`
is a non-atomic RMW, so `Concurrency(n>1)` undercounts redeliveries and a poison
message loops past `MaxRedeliveries`; `-race` cannot catch the logical lost-update, so
the "race-free, verified with `-race`" guarantee is false) and **one High liveness
footgun** (CR-01: a user `OnReturn` runs inline on the unbuffered-return demux, so a
blocking callback stalls the connection reader and the broker drops the socket — and
the spec never names the invocation goroutine). The remaining eleven findings pin
underspecified primitives (the impossible-as-worded `sync.Cond` "cancellable via
ctx", the double-verdict guard atomicity, the dispatcher bound, the `100x`/`1000`
mismatch) and turn three silent leak/crash surfaces (supervisor panic, ctx-ignoring
handler, non-FIFO pool starvation) into stated, tested contracts. Making the counter-B
RMW atomic, moving `OnReturn` off the reader path, pinning the double-verdict CAS, and
shipping the documented boundaries and tests brings the library to a defensible
posture at the billions/day, high-concurrency bar. No redesign required.
