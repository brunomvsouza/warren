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

### LATER-02 — `int`/`uint` aceitos em `validateHeaders` mas não coagidos para `int64`/`uint64`

**Contexto:** `message.go:validateHeaderValue` — os tipos `int` e `uint` são aceitos na
validação (retornam `nil`) porque o comentário do campo `Headers` em `types.go` promete
"auto-coerce para int64/uint64 at publish time". No entanto, a coerção ainda não foi
implementada. Quando o publisher (T12) serializar o header via `amqp091.Table`, passar um valor
Go `int` (não `int64`) pode resultar em erro no encoder do `amqp091-go` ou em comportamento
não determinístico conforme versão da biblioteca.

**Impacto:** Erro em runtime ao publicar headers com `int`/`uint` nativos, potencialmente
mascarado por `ErrInvalidMessage` genérico sem indicar qual header falhou. A API promete
coerção silenciosa; sem implementá-la a promessa é falsa.

**Evidência:** `/ship` code-review + security-auditor MEDIUM-2 em T09/T10.

**Sugestão de solução:** Realizar a coerção dentro de `validateHeaderValue` (ou num `coerceHeaders`
separado chamado antes da serialização), transformando `int` → `int64` e `uint` → `uint64`
in-place no mapa antes de passar para o encoder:

```go
case int:
    h[key] = int64(v.(int))
    return nil
case uint:
    h[key] = uint64(v.(uint))
    return nil
```

Isso exige mudar a assinatura para passar o mapa e a chave, ou criar um passo de normalização
separado. Avaliar junto com T12 (publisher) quando a serialização de `amqp091.Table` for
integrada.

**Pré-requisitos:** T12 (`publisher.go`) — só faz sentido implementar quando o caminho de
serialização existir para validar o round-trip de ponta a ponta.

**Acceptance criteria quando implementado:**
- `Headers{"count": int(5)}` serializado via `amqp091.Table` sem erro e recebido pelo broker.
- `Headers{"count": uint(5)}` idem.
- Teste de integração (tag `integration`) confirma round-trip via `rabbitmqadmin get`.
- `validateHeaderValue` continua retornando `ErrInvalidMessage` para tipos realmente inválidos.

---

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

### LATER-06 — Upper bound ausente em `WithChannelPoolSize` / buffer de confirms

**Contexto:** `publisher.go:openPublisherEntry` — o buffer do canal de confirms é
`max(poolSize, 8)`. `poolSize` vem de `opts.channelPoolSize` que é validado como `≥ 1` mas não
tem teto. Um valor extremo (ex: 1_000_000) causa uma alocação massiva de `chan amqp091.Confirmation`
em cada `openPublisherEntry`.

**Impacto:** Misconfiguration local causa OOM imediato em vez de erro de validação descritivo na
inicialização. Em infraestrutura compartilhada ou gerenciada, pode ser explorado para DoS.

**Evidência:** `/ship` security-audit T12 — LOW finding.

**Sugestão de solução:**
1. Adicionar validação de upper bound (ex: 4096) em `validateConnOptions`.
2. Cap o buffer de confirms em `openPublisherEntry`: `if buf > 4096 { buf = 4096 }`.

**Pré-requisitos:** T12. Pode ser adicionado ao mesmo PR ou em hotfix posterior.

---

### LATER-03 — Fuzz target do codec cobre apenas `NewJSON()` com `any` como destino

**Contexto:** `codec/json_fuzz_test.go:16-21` — `FuzzCodecJSON` usa `var v any` como destino de
decode, o que nunca exercita comportamentos sensíveis a schema (qualquer JSON cabe em
`interface{}`). Os caminhos divergentes entre `NewJSON()` (lax default, Postel) e
`NewJSONStrict()` (`DisallowUnknownFields` opt-in) não são distinguidos pelo fuzz engine.

**Impacto:** Panics ou comportamentos inesperados em qualquer dos dois codecs sob inputs
malformados poderiam não ser detectados antes de chegar em produção. O valor principal do fuzz
target atual é testar "não panics" — objetivo válido mas incompleto.

**Evidência:** `/ship` code-review + test-engineer gaps T09/T10.

**Sugestão de solução:**
1. Adicionar um segundo fuzz target `FuzzCodecJSONStrict` que use `NewJSONStrict()`.
2. Usar uma struct tipada como destino (ex: a `order` já definida em `json_test.go`) para que
   `DisallowUnknownFields` seja exercitado com inputs reais do fuzz engine.
3. Adicionar seeds adversariais ao corpus inicial:
   ```go
   f.Add(bytes.Repeat([]byte(`[`), 500))        // deeply nested array
   f.Add([]byte(`{"id":` + strings.Repeat("9", 100) + `}`)) // large integer
   f.Add([]byte("\xff\xfe"))                     // invalid UTF-8
   ```

**Pré-requisitos:** Nenhum. Pode ser feito a qualquer momento após T09.

**Acceptance criteria quando implementado:**
- `FuzzCodecJSON` (lax default) usa struct tipada como destino; comportamento de "aceita unknown
  field" exercitado por seeds com campos extras.
- `FuzzCodecJSONStrict` existe e cobre o caminho `DisallowUnknownFields`.
- Seeds adversariais adicionados ao corpus de ambos os targets.
- `go test -fuzz=FuzzCodecJSON -fuzztime=60s ./codec` e o equivalente strict passam sem panics.

---

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

### LATER-33 — `MessageId` sem limite de comprimento como chave do `sync.Map` em `applyBatchCounterB`

**Contexto:** `batch_consumer.go:batchCounterBKey` — constrói a chave do `sync.Map` do counter B
como `"mid:" + msgID` quando `MessageId` não é vazio, sem nenhuma validação de comprimento. Um
broker comprometido ou produtor malicioso pode enviar mensagens com `MessageId` de comprimento
arbitrário (ex: 1 MB), causando acúmulo de strings longas no map. O problema é idêntico ao
mapeado em LATER-24 para `returnTagMap`, mas ocorre na instância `counterState` do
`BatchConsumer`.

**Impacto:** Amplificação de memória dentro de uma sessão de canal (antes do próximo reconnect que
cria um novo `redeliveryCounter`). Com prefetch padrão de 100 e `MessageId` de 1 MB, a exposição
é ~100 MB por consumidor por ciclo de canal. Não é explorável remotamente sem acesso ao broker;
não há risco de vazamento de dados.

**Evidência:** `/ship` security-auditor — LOW finding (2026-05-25, post-LATER-32 review).
OWASP A05:2021 — Security Misconfiguration (resource exhaustion, defence-in-depth gap). Ver
relatório completo em `batch_consumer.go:applyBatchCounterB`.

**Sugestão de solução:** Truncar `msgID` em `batchCounterBKey` antes de usá-lo como chave:

```go
const maxMsgIDKeyLen = 512
if len(msgID) > maxMsgIDKeyLen {
    msgID = msgID[:maxMsgIDKeyLen]
}
```

Aplicar a mesma lógica em `consumer.go:counterBKey` para consistência. Considerar extrair a
lógica de key construction para `internal/headers` ou um helper compartilhado.

**Pré-requisitos:** [[LATER-24]] (sync.Map memory retention) — agrupar as correções de memória
do `sync.Map` no mesmo PR para revisão coesa. T37 (amqptest) pode facilitar um teste de
integração que verifique o bound.

---

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

