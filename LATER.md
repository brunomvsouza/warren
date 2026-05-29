# LATER.md — Melhorias Diferidas

## Motivação e uso deste arquivo

Este arquivo registra melhorias identificadas durante revisões de código (`/ship`), auditorias de
segurança ou análises de cobertura que foram **conscientemente adiadas** — não são bugs, não
bloqueiam o merge atual, mas merecem atenção em algum momento futuro a critério do mantenedor.

### Como usar com `agent-skills:plan`

Quando você quiser transformar itens deste arquivo em tarefas formais do plano de implementação:

1. Invoque `/plan` ou a skill `agent-skills:planning-and-task-breakdown`.
2. Aponte o agente para este arquivo como fonte de requisitos: *"use LATER.md como input"*.
3. O agente irá decompor cada item em tarefas com acceptance criteria, dependências e ordem de
   execução, seguindo o mesmo padrão de `tasks/plan.md` (numeração Txx, acceptance criteria
   verificáveis, seção de verificação com comandos).
4. As novas tarefas devem ser inseridas em `tasks/plan.md` na fase correta e em `tasks/todo.md`
   com o checkbox `[ ]`.

### Padrão de cada entrada neste arquivo

Cada item deve conter:
- **Título** curto (o problema em uma linha)
- **Contexto** — qual código está envolvido e por que o problema existe
- **Impacto** — o que pode dar errado se o item nunca for resolvido
- **Evidência** — de onde veio (ex: `/ship` review, auditoria de segurança, test-engineer)
- **Sugestão de solução** — direção técnica sem prescrever a implementação completa
- **Pré-requisitos** — qual task do plano (`Txx`) deve existir antes deste item ser atacado

---

## Itens pendentes

---

<!-- LATER-02 resolved: commit 48ee170 (fix: coerce int/uint headers to int64/uint64 during validation) -->

### LATER-05 — `wrapAMQPError` propaga o campo `Reason` do broker verbatim nos erros

**Contexto:** `errors.go:wrapAMQPError` — o wrapping `fmt.Errorf("%w: %w", sentinel, err)` inclui
a string `.Error()` do `*amqp091.Error`, que contém o campo `Reason` do broker com detalhes de
topologia interna (nome de fila, vhost, tipo de recurso). Ex: `"Exception (405) Reason:
\"RESOURCE_LOCKED - exclusive access to queue 'jobs' in vhost '/'\""`.

**Impacto:** Em deployments multi-tenant ou com separação de contextos, nomes de filas e vhosts
de outras tenants podem vazar em logs de aplicação, responses HTTP ou sistemas de observabilidade
externos.

**Evidência:** `/ship` security-audit T12 — MEDIUM finding.

**Sugestão de solução:** Criar `redact.AMQPError(err)` que formata apenas o código numérico
(`"Exception (%d)"`) sem o Reason, ou aplicar redação no `Reason` antes de incluí-lo no erro
retornado. O passo de dois wraps (`%w: %w`) deve ser mantido para `errors.Is`/`AMQPCode`
continuarem funcionando.

**Pré-requisitos:** `internal/redact` (já existe). Pode ser feito a qualquer momento após T12.

---

<!-- LATER-06 resolved: commit 65924b5 (fix: add upper bound validation for WithChannelPoolSize) -->

<!-- LATER-03 resolved: commit 7d4dde9 (test(codec): improve fuzz coverage for lax and strict JSON codecs) -->

### LATER-08 — CI actions pinadas em tags semver mutáveis

**Contexto:** `.github/workflows/ci.yml` — `actions/checkout@v6`, `actions/setup-go@v6`,
`golangci/golangci-lint-action@v9` usam tags semver que podem ser force-pushed por um maintainer
comprometido, injetando código malicioso no contexto do workflow com acesso ao `GITHUB_TOKEN`.

**Impacto:** Risco de supply chain no CI. Baixo agora (sem secrets críticos além do token de
leitura), mas se torna crítico quando tokens de publicação (pkg.go.dev, GitHub Packages) forem
adicionados.

**Evidência:** `/ship` review de T15/T16 — security-auditor (Low).

**Sugestão de solução:** Fixar cada action ao SHA imutável do commit com comentário do semver:
```yaml
- uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4.2.2
```

**Pré-requisitos:** Nenhum. Tarefa standalone de hardening de CI.

---

### LATER-09 — `golangci-lint-action` usa `version: latest` — builds não-determinísticos

**Contexto:** `.github/workflows/ci.yml:33` — `version: latest` baixa a versão mais recente do
linter a cada execução. Uma nova release pode introduzir regras que quebram o CI de forma
imprevisível, ou regredir regras de segurança (`gosec`, `errcheck`) silenciosamente.

**Impacto:** CI não-reproduzível; regras de lint auditáveis podem mudar sem rastreabilidade.

**Evidência:** `/ship` review de T15/T16 — security-auditor (Low).

**Sugestão de solução:** Fixar em versão específica: `version: v1.64.x` (ou versão corrente) e
atualizar deliberadamente via commit dedicado.

**Pré-requisitos:** LATER-08 (fixar SHAs é o passo natural para consolidar ambos numa única PR).

---

### LATER-10 — Imagem RabbitMQ com tag flutuante no CI de integração

**Contexto:** `.github/workflows/ci.yml:43` — `rabbitmq:3-management` é uma tag mutável no
Docker Hub. Um push malicioso para a tag pode injetar uma imagem comprometida que rode no
runner de CI.

**Impacto:** CI não-reproduzível; testes de integração podem quebrar por mudança de comportamento
do broker sem aviso; risco teórico de supply chain.

**Evidência:** `/ship` review de T15/T16 — security-auditor (Low).

**Sugestão de solução:** Fixar em digest imutável ou ao menos em minor tag:
`rabbitmq:3.13-management`.

**Pré-requisitos:** LATER-08.

---

### LATER-11 — `registerReconnectHook` em `connection.go` sem chamadores diretos

**Contexto:** `connection.go:308` — `Connection.registerReconnectHook` foi adicionado como API
interna para `Topology.AttachTo` e `Consumer` (T18), mas `AttachTo` itera `pubConns`/`conConns`
diretamente e não usa esta função. Com isso, a função está em 0% de cobertura e sem chamadores.

**Impacto:** Dead code que confunde futuros leitores sobre o padrão de registro de hooks.

**Evidência:** `/ship` review de T15/T16 — code-reviewer (Suggestion).

**Sugestão de solução:** Quando Consumer (T18) for implementado, verificar se `registerReconnectHook`
é o ponto de entrada correto. Se Consumer também iterar diretamente, remover a função. Se Consumer
a usa, manter e adicionar cobertura.

**Pré-requisitos:** T18 (Consumer re-subscribe hooks).

---

### LATER-21 — Test case B3: `x-delivery-limit` exhaustion em quorum queue

**Contexto:** `consumer_handler_timeout_verdict_integration_test.go` — T18b test case B
verifica apenas que a mensagem é reenfileirada ao menos duas vezes (`deliveryCount >= 2`).
Não exercita o cenário de esgotamento de `x-delivery-limit`: após N redeliveries, o broker
deve dead-letter a mensagem automaticamente para o DLX configurado.

**Impacto:** O caminho de "requeue até o limite → dead-letter" é um contrato importante
do `TimeoutNackRequeue` (o usuário precisa de um DLX na fila de origem para evitar que
mensagens travadas circulem para sempre). Sem teste, o comportamento pode regredir sem
detecção automática.

**Evidência:** Registrado durante revisão de código do commit `23834a7` (T18b). O critério
original de aceite de T18b (caso B, item 3) previa este cenário, mas foi omitido pois a
configuração de quorum queue com `x-delivery-limit` ficou fora do escopo imediato.

**Sugestão de solução:**
Adicionar `TestHandlerTimeoutVerdict_NackRequeue_DeliveryLimit_integration` em
`consumer_handler_timeout_verdict_integration_test.go`:

```go
// Topologia: quorum queue com x-delivery-limit: 3 + DLX fanout + binding
topo := &warren.Topology{
    Exchanges: []warren.Exchange{
        {Name: dlxExch, Kind: warren.ExchangeFanout, Durable: false},
    },
    Queues: []warren.Queue{
        {
            Name:    srcQ,
            Durable: false,
            Args: map[string]any{
                "x-queue-type":          "quorum",
                "x-delivery-limit":      3,
                "x-dead-letter-exchange": dlxExch,
            },
        },
        {Name: dlqQ, Durable: false},
    },
    Bindings: []warren.Binding{
        {Exchange: dlxExch, Queue: dlqQ, RoutingKey: ""},
    },
}
// Após 3 timeouts, assert que deliveryCount == 3 e a mensagem aparece na DLQ.
```

**Pré-requisitos:** LATER-20 (binding DLX/DLQ), RabbitMQ 3.10+ (quorum queue
`x-delivery-limit` suportado). CI usa `rabbitmq:3-management` que suporta quorum
queues desde 3.8.

---

### LATER-24 — `sync.Map` retém memória interna após batches grandes

**Contexto:** `publisher.go:returnTagMap` — `sync.Map` mantém o estado de cópia de leitura
interna (`readOnly` + `dirty`) mesmo após todos os pares serem deletados. Após um
`PublishBatch` com batch grande (ex: 1024 mensagens), a `sync.Map` pode reter capacidade
interna proporcional ao pico do batch, que é reaproveitada no próximo caller que obtiver o
mesmo `publisherEntry` via `pool.acquire`.

**Impacto:** Overhead de memória O(peak_batch_size) por entry enquanto o entry não for
descartado (por fechamento de canal) ou substituído (por reconexão). Em uso normal o GC
eventualmente libera; não é exploitável remotamente. Não é bloqueante.

**Evidência:** `/ship` security-audit — LOW finding (2026-05-24, mandatory+batch review).

**Sugestão de solução:** Em `publisherConnPool.release`, substituir a `returnTagMap` por uma
nova `new(sync.Map)` se o batch processado foi maior que um threshold (ex: 256 entries):

```go
// Em release, após os selects:
if shouldResetMap(entry.returnTagMap) {
    entry.returnTagMap = new(sync.Map)
}
```

Alternativamente, reavaliar se `returnTagMap` deve ser recriada no `openPublisherEntry` a
cada canal reaberto (pós-reconexão), já que nesse ponto o estado anterior é inválido de
qualquer forma.

**Pré-requisitos:** Nenhum. Standalone, mas de baixa prioridade.

---

### LATER-25 — Latência de batch exclui tempo de aquisição de canal na métrica

**Contexto:** `publisher.go:649` — em `PublishBatch`, `msgStart = time.Now()` é definido
antes de `tracker.Wait` (depois da aquisição do canal). Em `publishOnce`, `start = time.Now()`
é definido antes de `pool.acquire`. Logo, métricas de latência de batch excluem o tempo de
espera no pool, enquanto métricas de publish unitário incluem.

**Impacto:** Dashboards que comparam `publisher_publish_latency_seconds` entre publishers
unitários e em batch vão observar latências sistematicamente menores para batch, mesmo quando
o tempo de espera no pool é idêntico. Pode induzir operadores a concluir incorretamente que
batch é mais rápido quando a diferença é de método de medição.

**Evidência:** `/ship` code-review — Suggestion S-2 (2026-05-24, mandatory+batch review).

**Sugestão de solução:** Mover `msgStart = time.Now()` para antes da aquisição do canal em
`PublishBatch`, ou documentar explicitamente a diferença de semântica no godoc da métrica
`publisher_publish_latency_seconds` em `metrics/`.

**Pré-requisitos:** Nenhum. Decisão de design a ser tomada ao revisar o contrato da métrica.

---

### LATER-27 — Broad panic-safety audit and preventive linter for user-provided callbacks

**Context:** A manual review (2026-05-24) identified five call sites in `connection.go`,
`consumer.go`, and `batch_consumer.go` where user-provided callbacks
(`WithOnBlocked`, `WithOnReconnect`, `WithOnTopologyDegraded`, `Handler[M]`,
`BatchHandler[M]`) are invoked without `recover()` and, in some cases, inline inside
tight event-loop goroutines. T34c addresses those known sites. It is plausible that
analogous patterns exist in code not yet implemented (T25–T46) or in internal call sites
not covered by this pass.

**Impact:**
- A panic in a user callback can: crash the entire process; permanently deadlock all
  Publishers (barrier never broadcast); or silently kill a consumer goroutine, halting
  consumption with no error log.
- Blocking callbacks inside tight event-loops (`supervisor`, `runBarrier`) delay detection
  of critical broker events (connection unblock, reconnect).
- The bug is hard to reproduce in normal tests and typically surfaces in production under
  load or with buggy callback code.

**Evidence:** Manual review — session 2026-05-24 (user-requested analysis).
T34c covers the already-identified sites; this entry tracks the residual sweep and
long-term prevention via tooling.

**Suggested solution:**

1. **Systematic sweep post-T34c:** after T34c lands, review every user-supplied
   closure/func invocation in production code
   (`grep -rn "opts\." --include="*.go" | grep "nil"` as a starting point) and verify
   that `recover()` is present and that the blocking vs. non-blocking behaviour is
   documented in the option's godoc.

2. **Static linter:** evaluate tools that detect goroutines without a top-level `recover()`
   or unprotected calls to external functions:
   - [`github.com/nikolaydubina/go-recover`](https://github.com/nikolaydubina/go-recover):
     detects goroutines missing a `recover()` at the top.
   - [`revive`](https://github.com/mgechev/revive) `defer` rule can flag `defer wg.Done()`
     without a preceding `recover()`.
   - Alternatively, a custom `golangci-lint` analyzer targeting the pattern
     `go func() { userCallback() }()` without recover can be written and wired into
     `.golangci.yml` as a `custom-gcl` plugin.
   - Enable `paniccheck` if available in the golangci-lint version in use.

3. **godoc convention:** document on `WithOnBlocked`, `WithOnReconnect`, and
   `WithOnTopologyDegraded` whether the callback is blocking or non-blocking — the
   "blocks the reconnect barrier" semantic of `WithOnReconnect` is a deliberate feature
   that belongs in the public contract, not a hidden implementation detail.

**Prerequisites:** T34c (panic isolation for the already-identified sites). This entry
covers the residual audit and tooling-based prevention.

---

### LATER-26 — `wireReturnPool` e `wireFakePool` são near-duplicatas

**Contexto:** `publisher_t13_test.go:wireReturnPool` e `publisher_test.go:wireFakePool` —
ambas implementam o mesmo select goroutine de correlação confirm+return. Diferem apenas na
assinatura: `wireReturnPool` aceita qualquer `pubChannel` e retorna o tracker separadamente;
`wireFakePool` é especializada para `*fakePubChannel`. Qualquer mudança futura na lógica do
goroutine deve ser aplicada em ambos os lugares.

**Impacto:** Risco de divergência silenciosa. Ex: se o guard `if ret.MessageId != ""` for
atualizado em uma função mas esquecido na outra, os testes exercitariam semânticas diferentes
da produção.

**Evidência:** `/ship` code-review — Suggestion S-1 (2026-05-24, mandatory+batch review).

**Sugestão de solução:** Extrair o corpo do goroutine para uma função shared unexported
`runReturnCorrelationLoop(returnCh <-chan amqp091.Return, confirmCh <-chan amqp091.Confirmation,
rtm *sync.Map, tracker *confirms.Tracker, onReturn func(amqp091.Return))` e ter ambas as
funções chamá-la. Alternativamente, colapsar `wireReturnPool` em `wireFakePool` com parâmetro
`pubChannel` genérico e retornar o tracker opcionalmente.

**Pré-requisitos:** Nenhum. Refactoring de teste puro, sem impacto no código de produção.

---

<!-- LATER-33 resolved: commit 167f0a4 (fix: truncate MessageId to 512 bytes in redelivery counter B keys) -->

### LATER-34 — Synchronous-confirm throughput ceiling / async publish API (architecture decision)

**Context:** `publisher.go` — `Publish` acquires a channel from the per-connection pool,
publishes, **blocks on its own confirm**, and only then returns the channel (SPEC §6.1/§6.2).
A single in-flight publish therefore holds a whole channel for a full broker round-trip, so
per-`Publish` throughput is bounded by `(channels × connections) / RTT_confirm`. Rev 6 decision
31 deliberately rejected a sliding in-flight window; `PublishBatch` is the only pipelining path,
and it is itself synchronous (blocks until the whole batch's confirms or `ConfirmTimeout`).
Surfaced by the Rev 10 specialist re-review (2026-05-25) as finding **P1.1 / R10-18**.

**Impact:**
- The §9 throughput targets (≥30k/s single-conn, ≥100k/s multi-conn) hold only at sub-millisecond
  RTT (local broker). Against a remote broker at 2–5 ms RTT the achievable single-`Publish` rate
  drops by the same factor, and the SPEC does not state this honestly.
- **Cliff under confirm-latency spikes.** If broker confirm latency rises (GC pause, disk pressure,
  quorum failover), the same fixed channel pool yields far fewer msg/s and the overflow surfaces as
  `ErrChannelPoolExhausted` — confirm latency converts directly into publish unavailability. This
  is a classic on-call cascade and is invisible in the current design's stated guarantees.

**Evidence:** Rev 10 AMQP/SRE specialist re-review, finding P1.1 (2026-05-25). Recorded in SPEC §10
"Rev 10" as R10-18, deferred here because it is an owner architecture decision, not a defect.

**Suggested solution (decision required, then a design task):**
1. **Document the ceiling honestly now (cheap):** add a SPEC §6.2 note that single-`Publish`
   throughput is `pool/RTT`, that confirm-latency spikes cause `ErrChannelPoolExhausted`, and that
   `PublishBatch` is the high-throughput path.
2. **Decide on an async API (reverses Rev 6 decision 31):** evaluate either
   - `PublishAsync(ctx, msg) (<-chan error)` backed by a per-channel confirm-tracking goroutine that
     pipelines many publishes on one channel with a **bounded in-flight window** for backpressure; or
   - a streaming publisher handle with an explicit in-flight budget and confirm callbacks.
   Either decouples throughput from RTT and removes the pool-exhaustion cliff, at the cost of API
   surface and the duplicate-budget bookkeeping the async path implies (still at-least-once;
   consumers dedupe by `MessageID` per §6.2.1).

**Pré-requisitos:** Owner decision on reopening Rev 6 decision 31. Builds on T11
(`internal/confirms`) and T12/T13 (publisher). If accepted, it becomes a new task (likely a
Phase 11 follow-on) with its own SPEC amendment; if rejected, item 1 (the honesty note) lands and
this entry is closed.

---

### LATER-35 — No consumer-side message-size guard (inbound deserialization backpressure)

> **Promoted to T131 (Phase 18, Lens-07 ST-06), 2026-05-29 — re-classified LOW → Blocker.** The
> security & threat-modeling lens re-scored this fail-open inbound DoS as a must-fix Blocker (a single
> hostile or buggy producer ships a ~512 MiB body that `amqp091` reassembles in memory before the codec
> runs → consumer OOM, with no consume-side cap). T131 implements the consume-side
> `MaxInboundMessageSizeBytes` guard (default 16 MiB, fail-closed). **Remove this entry when T131
> lands** (per CLAUDE.md, cite T131 in the commit message). Kept here until then so the finding is not
> re-filed.

**Context:** `publisher.go` enforces `maxMessageSizeBytes` before sending, but the consume path
(`consumer.go:dispatch` → `safeDecodeConsumer`, and the equivalent in `batch_consumer.go`) has no
symmetric cap. For the structured CloudEvents codec (and any future body-parsing codec), the full
delivery body is handed to the SDK's `event.UnmarshalJSON` (json-iterator) and parsed in memory
before the handler runs. The binary CloudEvents codec is NOT affected — it stores the body as raw
`DataEncoded` bytes without parsing.

**Impact:** Memory/CPU pressure under a malicious-or-buggy producer model. An authorized producer
(or a compromised upstream) can publish a very large / deeply-nested JSON body that a consumer
parses fully before deciding anything. Bounded today by RabbitMQ frame size and prefetch count, so
this is defense-in-depth, not a remote-unauthenticated crash. Not a blocker.

**Evidence:** `/ship` security-audit (2026-05-28, T25/T26 CloudEvents review) — LOW finding.

**Suggested solution:** Add an optional consumer-side `MaxMessageSizeBytes` (builder option on
`ConsumerFor`/`BatchConsumerFor`) that rejects-and-nacks (no requeue) before invoking the codec,
mirroring the publisher guardrail. Record a `decode_error`/`too_large` outcome metric so it is
observable. Re-check the SDK's `jsoniter.ConfigFastest` parsing behaviour whenever
`cloudevents/sdk-go/v2` is bumped.

**Prerequisites:** None. Standalone consumer-side hardening; coordinates with the consumer builder
options surface.

---

### LATER-36 — Binary CloudEvents mode narrows non-string extension types to strings

**Context:** `codec/cloudevents.go` — `NewCloudEventsBinary().EncodeWithHeaders` formats every
context attribute and extension via the SDK's `types.Format`, so a typed extension (e.g. an
integer or boolean) is written to the `cloudEvents:`-prefixed AMQP header as its canonical string
form, and `DecodeWithHeaders` reads it back as a string (non-string header values are treated as
absent). Structured mode preserves the JSON type. The CloudEvents AMQP Protocol Binding does allow
non-string AMQP property types for Integer/Boolean extension attributes, so a fully spec-faithful
binary codec would preserve the type both ways.

**Impact:** A non-Go producer that sends an integer/boolean CloudEvents *extension* as a typed AMQP
property would have it round-trip through a warren consumer as a string; conversely, warren always
emits string-typed extension headers. Core attributes (id/source/type/specversion/subject/
dataschema/time/datacontenttype) are unaffected — they are String/URI/Timestamp by spec. This is a
fidelity gap only for typed *extensions*, documented in the codec godoc, and accepted for now.

**Evidence:** `/ship` test-engineer coverage analysis (2026-05-28, T25/T26) — finding #8; behaviour
is now pinned by `TestCloudEventsBinary_RoundTrip_NonStringExtensionNarrowsToString` and contrasted
with `TestCloudEventsStructured_RoundTrip_PreservesExtensionType`.

**Suggested solution:** If full type fidelity is desired, change `EncodeWithHeaders` to write the
native AMQP type for scalar extension values (int → AMQP long, bool → AMQP boolean) and
`DecodeWithHeaders`/`ceHeaderString` to pass the typed value to `SetExtension` for the
amqp091-supported scalar types, treating tables/slices as invalid. This is a wire-format change and
must amend SPEC §6.9 first; it widens the round-trip contract and needs new round-trip tests against
a non-Go producer fixture.

**Prerequisites:** SPEC §6.9 amendment (codec interop, decision 4). Standalone after that.

---

### LATER-37 — Publisher size cap measures the body only; HeaderCodec attribute headers bypass it

**Context:** `publisher.go:encodeMsg` enforces `maxMessageSizeBytes` against `len(body)` only. For a
`HeaderCodec` such as the binary CloudEvents codec, the event attributes and extensions travel as
`cloudEvents:`-prefixed AMQP headers rather than in the body, so a publish with many or large
extension headers can exceed the intended on-wire size while passing the body cap.

**Impact:** The per-publish guardrail's contract ("reject before opening a channel when the message
is too large") is partially circumvented when a HeaderCodec carries significant data in headers. Low
severity: header sizes are still bounded by amqp091 frame limits and by `validateHeaders`, the body
(usually the bulk of a message) is still capped, and the gap is not CloudEvents-specific — it
applies to any HeaderCodec. No correctness impact.

**Evidence:** `/ship` code-reviewer (2026-05-28, T25/T26 CloudEvents review) — Important finding,
`publisher.go:454`.

**Suggested solution:** Either (a) document in the `WithMaxMessageSizeBytes` godoc that the cap
measures the encoded body only, not codec-emitted headers; or (b) for HeaderCodecs, add the
serialized header (and content-type) byte sizes into the size check before the `too_large` verdict.
Prefer (a) unless a header-heavy codec makes the gap material in practice.

**Prerequisites:** None. Standalone publisher-side hardening.

---

### LATER-38 — `publisher_publish_seconds{outcome}` metric label space lags the span outcome contract

**Context:** T27 implemented the publish span's `messaging.rabbitmq.outcome` attribute using the rich
label space mandated by SPEC §6.9 (`ack`/`nack`/`return`/`timeout`/`too_large`/`pool_exhausted`/
`blocked`/`error`), via `publishOutcome` in `publisher.go`. The `publisher_publish_seconds{outcome}`
Prometheus histogram, however, is still recorded with the coarse legacy labels `success`/`error`/
`too_large` in `publishOnce` and `PublishBatch`. SPEC §6.9 states the span outcome "mirrors the
metric `publisher_publish_seconds{outcome}` label" — i.e. both are intended to share the rich space.

**Impact:** Operators cannot pivot 1:1 between a trace's outcome and the metric's outcome dimension;
"show me the publish-latency distribution for `outcome=return`" is not answerable from the metric
because the metric only emits `error` for unroutable/nacked/timeout/blocked/pool-exhausted alike.
No correctness impact — both signals are individually correct; only the label vocabularies diverge.

**Evidence:** T27 (OTel in Publisher) implementation, 2026-05-28. Out of T27 scope: the task's
Files list is `publisher.go` + `publisher_tracing_test.go`, while aligning the metric labels also
requires updating the metric-assertion tests (`publisher_test.go`, `publisher_batch_test.go`).

**Suggested solution:** Route the metric's outcome label through the same `publishOutcome` mapper
used by the span, so `RecordPublish` receives `ack`/`nack`/`return`/`timeout`/`too_large`/
`pool_exhausted`/`blocked`/`error`. Update the affected metric-assertion tests in the same change.
Note this is a metric-label vocabulary change and may affect downstream dashboards/alerts.

**Prerequisites:** T27 (done). Coordinate with any dashboard/alert owners before changing labels.

---

### LATER-39 — Protobuf multi-type-queue discriminator (Any / type-URL / type registry)

**Context:** `codec.NewProtobuf()` decodes proto3 binary, which carries **no type
information on the wire**. `ConsumerFor[M]` fixes the Go message type at compile
time, so a single-type queue decodes fine — but a topic-exchange queue carrying
**multiple** proto event types (a common event-driven pattern) cannot be decoded:
the consumer has no way to know which `proto.Message` a given body is. Phase 14
(T99, IW-05) documents this single-type-per-`Consumer` constraint but deliberately
defers a first-class fix.

**Impact:** Polyglot/event-driven pipelines that publish several proto types to one
queue must either split the queue per type or hand-roll a discriminator on top of
the library (e.g. read `Message[M].Type` and switch). Without a built-in mechanism,
`ConsumerFor[M]` is effectively single-type-only for protobuf, which is a real
ergonomics + interop gap relative to JSON/CloudEvents where the body is
self-describing.

**Evidence:** Lens-03 interoperability/wire-format spec validation, 2026-05-28
(finding IW-05; `spec-validation/03-interoperability-wire-format-plan.md`). Owner
decision (2026-05-28): defer the discriminator to LATER, document the constraint now.

**Suggested solution:** Offer an opt-in discriminator strategy that does not break
the typed `ConsumerFor[M]` ergonomics, e.g. (a) a documented convention that
`Message[M].Type` (`basic.properties.type`) carries the proto full message name,
plus a registry-backed `RawConsumer` / decode helper that dispatches by name to the
right `proto.Message`; or (b) support `google.protobuf.Any` (type-URL carried in
the body) for self-describing multi-type payloads. Prefer (a) for cross-language
interop (Java/Python set `type` too) and reserve (b) for callers already on `Any`.
Either way it is additive — single-type `ConsumerFor[M]` stays the default.

**Prerequisites:** T99 (documents the single-type constraint + the `type`-as-name
convention this builds on).

---

### LATER-40 — Pluggable schema-registry / validation hook for event evolution

**Context:** `warren` relies on lax-JSON-by-default (Postel, decision 23) for
*additive* schema evolution, and Phase 15 (T106, EDA-13) documents the boundary —
**breaking** evolution (field removal, rename, semantic change) is user-owned via
versioned type names (`order.created.v2`) and the `Message.Type` discriminator
convention. There is no library primitive to *enforce* a schema or validate a payload
against a registered contract before it reaches the handler (consume side) or hits the
wire (publish side). Across dozens of independently-deployed services, schema drift is
currently caught only at the first decode failure or, worse, a silently-mis-decoded
field that lax JSON tolerates.

**Impact:** Event-mesh / multi-team estates have no central, machine-checkable contract
enforcement. A producer can ship a breaking change and the only signal is downstream
decode errors (or silent data corruption where lax JSON tolerates the change). Teams
that want Confluent-style schema-registry guarantees (compatibility checks, schema IDs
on the wire) must hand-roll them on top of the codec interface.

**Evidence:** Lens-04 event-driven-architecture spec validation, 2026-05-28 (finding
EDA-13; `spec-validation/04-event-driven-architecture-plan.md`). Owner decision
(2026-05-28): document the boundary + versioned-type convention now (T106), defer a
pluggable registry hook to LATER.

**Suggested solution:** Offer an opt-in, codec-adjacent `SchemaValidator` hook — e.g. a
builder option on `PublisherFor`/`ConsumerFor` taking an interface
`Validate(ctx, contentType, typeName string, body []byte) error` — that runs after
encode / before decode and rejects (publish: `ErrInvalidMessage`; consume:
nack-no-requeue + a `schema_invalid` outcome metric) on a contract violation. Provide a
no-op default and a reference adapter for one registry (e.g. JSON Schema, or a
Confluent-compatible registry that carries a schema ID in a header). Keep it additive —
the lax-JSON default and the `Message.Type` convention stay the baseline; the validator
is for teams that need enforced contracts. Any wire-format change (a schema-ID header)
must amend SPEC §6.9 first.

**Prerequisites:** T106 (documents the evolution boundary + the `Message.Type`
versioned-type convention this builds on); coordinates with the codec/`HeaderCodec`
interface in `codec/`.

---

### LATER-41 — Dedicated `ReturnCode(err) (uint16, bool)` accessor to separate `basic.return` codes from channel-close codes

**Context:** `AMQPCode(err) (uint16, bool)` (`errors.go:171–195`, decision 38) returns
both a `basic.return` reply-code (312 `NO_ROUTE` / 313 `NO_CONSUMERS`, from a `*codeError`
raised on an unroutable mandatory publish) **and** channel/connection-close reply-codes
(311/320/402–406/…) through one `(uint16, bool)` with no provenance signal. A caller that
`switch`-es on the returned code cannot tell which AMQP frame class produced it — a 312
from a returned message looks identical to a close-code path. Lens-06 (GA-08) ships the
doc caveat in T126 (combine `AMQPCode` with `errors.Is(err, ErrUnroutable)` to
disambiguate) but does **not** add a typed accessor.

**Impact:** Callers building routing/alerting logic on the numeric code must remember to
cross-check `ErrUnroutable` by hand; a naive `switch AMQPCode(err)` mishandles the
`basic.return` 312/313 space, conflating an unroutable-publish signal with a
channel-close failure. The two frame classes deserve distinct, self-describing accessors.

**Evidence:** Lens-06 Go-API/library-design spec validation, 2026-05-28 (finding GA-08;
`spec-validation/06-go-api-library-design-plan.md`). Owner-deferred: T126 ships the §6.8
caveat now; the typed accessor is non-blocking.

**Suggested solution:** Add a dedicated `ReturnCode(err) (uint16, bool)` that returns a
code **only** when the error chain carries `ErrUnroutable` (the `basic.return` space),
leaving `AMQPCode` for channel/connection-close codes; or introduce a small code-class
enum (`CodeClassReturn` / `CodeClassClose`) so a single accessor reports provenance. Keep
both `errors.Is`/`As`-friendly and `errorlint`-clean; document in §6.8. Purely additive
(a new function; no signature change to `AMQPCode`).

**Prerequisites:** T126 (ships the §6.8 `AMQPCode` frame-class caveat this builds on); the
error taxonomy in `errors.go` + `internal/amqperror`.

---

### LATER-42 — Configurable SASL EXTERNAL principal extraction (SAN / DN beyond CN)

**Context:** `connection.go:122` (`computeAuthUser`) derives the SASL EXTERNAL principal from the
**CN** of the first client certificate only. RabbitMQ's `rabbitmq_auth_mechanism_ssl` plugin exposes
`ssl_cert_login_from`, configurable to `common_name` (default), `distinguished_name`, or
`subject_alternative_name` (with a SAN type/index). When the broker is configured for DN or SAN,
warren's client-side `UserID` check (decision 39) compares against the CN it extracted, which diverges
from the principal the broker actually authenticates — a false `ErrInvalidMessage` reject, or a
`UserID` value the broker then 406s. T136 (Phase 18, Lens-07 ST-04) ships the **doc-only** mitigation
(document the CN assumption + the divergence; recommend leaving `UserID` empty under non-CN broker
mappings) but does not add configurable extraction.

**Impact:** EXTERNAL deployments whose broker maps the principal from DN or SAN cannot use warren's
client-side `UserID` stamping/validation without leaving `UserID` empty (losing the client-side
guard). Low severity: a documented workaround exists (empty `UserID`), EXTERNAL itself still
authenticates correctly (the broker decides), and CN is the RabbitMQ default. The gap is ergonomic,
not a correctness/security hole once documented.

**Evidence:** Lens-07 security & threat-modeling spec validation, 2026-05-29 (finding ST-04;
`spec-validation/07-security-threat-modeling-plan.md`). Owner decision D4: doc-only for v0.1;
configurable extraction deferred.

**Suggested solution:** Add a `WithExternalPrincipalFrom(...)` connection option (or a small
`ExternalPrincipalSource` enum: `CN` / `DN` / `SAN`) that selects how `computeAuthUser` extracts the
principal from the client cert, matching the broker's `ssl_cert_login_from`. For SAN, accept the SAN
type (DNS / email / URI) to read. Keep CN the default (no behaviour change). Add a round-trip
integration test against a broker configured for each `ssl_cert_login_from` value.

**Prerequisites:** T136 (ships the §6.5/decision-35 CN-extraction documentation this builds on); the
EXTERNAL auth path in `connection.go` (`computeAuthUser`).

---

### LATER-43 — Optional aggregate in-flight confirm window (`WithMaxInFlightConfirms`)

**Context:** The publisher confirm tracker (`internal/confirms`) bounds outstanding delivery-tags
**per call** — a single `PublishBatch` holds at most `PublishBatchMaxSize` waiters, and a single
`Publish` holds one. There is **no aggregate cap across concurrent calls**: N goroutines each issuing
`PublishBatch`/`Publish` on the same `Connection` hold N independent windows, so the total in-flight
confirm memory is `N × PublishBatchMaxSize`, bounded only by how much the caller fans out. SPEC §6.2
already admits the per-call scope ("does not currently throttle multiple concurrent `PublishBatch`").

**Impact:** Under an unbounded publisher fan-out (e.g. one goroutine per request at the billions/day
bar) the confirm-tracker memory grows with concurrency rather than with a fixed window — a slow or
degraded broker that stalls confirms lets the in-flight set balloon. Low severity: the common case
(bounded worker pools, a handful of publishers) is already bounded by the per-call cap, the growth is
publisher-driven (the caller controls fan-out), and there is no message loss — only memory pressure.
T145 (Phase 19, Lens-08 CR-07) ships the **doc-only** mitigation (document the per-call boundary +
recommend caller-side fan-out limiting) but adds no aggregate cap.

**Evidence:** Lens-08 Go concurrency & runtime-correctness spec validation, 2026-05-29 (finding CR-07;
`spec-validation/08-go-concurrency-runtime-plan.md`). Owner decision D4: document the per-call boundary
for v0.1; the aggregate window deferred.

**Suggested solution:** Add a `WithMaxInFlightConfirms(n int)` connection (or publisher) option that
bounds the **aggregate** count of outstanding confirm waiters across all concurrent
`Publish`/`PublishBatch` calls on the `Connection` — a semaphore acquired before a publish admits its
waiter(s) and released as confirms resolve, so a stalled broker applies backpressure to publishers
instead of growing memory. `0` (default) keeps today's per-call-only behaviour (no aggregate cap, no
behaviour change). Surface waiting on the semaphore as ctx-cancellable (mirrors pool-acquire ergonomics)
and add a saturation metric. A unit test asserts the aggregate cap holds under N concurrent publishers.

**Prerequisites:** T145 (ships the §6.2 per-call-boundary documentation this builds on); the confirm
tracker in `internal/confirms` and the publisher confirm path.

---

### LATER-45 — Deeper hot-path allocation elimination (pooled-buffer codec + a `timeMu`-free UUIDv7 generator)

**Context:** Phase 20 (Lens-09) lands the *cheap, zero-risk* per-message hot-path allocation wins — pool/reset
the confirm-`Wait` `time.Timer` (T11/PC-06), gate the NoOp-tracer span argument-construction (T148/PC-07),
`uuid.EnableRandPool()` (T10/PC-09), lazy-allocate the x-death `byReason` map (T17/PC-08), and cache the
Prometheus child collectors (T148/PC-15) — under a combined `AllocsPerRun` guard (T148). Three deeper
allocations/locks remain on the per-message path after those: the JSON codec `Marshal`s the body **without a
buffer pool** (`codec/json.go:39`), the confirm tracker allocates a `&waiter{}` + a `make(chan error, 1)` **per
`Register`** (`tracker.go:77`), and google/uuid takes a **process-global `timeMu` lock per UUIDv7** even after
`EnableRandPool` (`message.go:73`).

**Impact:** At the billions/day bar these are residual per-message costs the cheap wins do not remove — a body
allocation + copy per publish, a waiter+channel allocation per in-flight publish, and a process-wide
serialization point on `timeMu` under high publish concurrency. Low-Med severity: none is a correctness or
message-loss issue (the body and waiter are *necessary* objects, just poolable; `timeMu` is a contention point,
not a deadlock), and the cheap-win baseline already removes the highest-frequency avoidable allocations. The
deep wins carry codec-API and dependency implications (a pooled `Encode` changes the codec call shape; a
`timeMu`-free generator means a custom UUIDv7 source rather than google/uuid), so they are consciously deferred.

**Evidence:** Lens-09 performance & capacity spec validation, 2026-05-29 (§12 hot-path allocation ledger;
`spec-validation/09-performance-capacity-plan.md`). Owner decision D1: land the cheap wins in Phase 20; defer
the deep wins.

**Suggested solution:** (1) A pooled-buffer codec `Encode` path — a `sync.Pool` of `bytes.Buffer` + a reused
`json.Encoder` so the per-message body buffer is recycled (assess the consume-side `json.NewDecoder`/
`bytes.Reader` per delivery the same way, `codec/json.go:48-55`). (2) A `sync.Pool` of `*waiter` (with its
`done` channel) recycled when `Wait` returns, so a steady-state publish stream reuses waiters. (3) A UUIDv7
generator that avoids the google/uuid process-global `timeMu` (a per-P / sharded monotonic time source, or an
internal generator), keeping RFC 9562 v7 layout and the at-least-once dedupe contract. Each lands behind the
T148 `AllocsPerRun` guard so the win is measured against the cheap-win baseline.

**Prerequisites:** T148 (the cheap-win allocation hardening + the combined `AllocsPerRun` baseline these deeper
wins are measured against); the codec interface (`codec/`), the confirm tracker (`internal/confirms`), and the
UUIDv7 default-apply path (`message.go`).

---

