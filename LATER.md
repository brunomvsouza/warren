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

