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

### LATER-01 — `dec.More()` não verificado no JSON Decoder

**Contexto:** `codec/json.go:40` — o método `Decode` usa `json.NewDecoder` mas não verifica
`dec.More()` após o primeiro decode. Se `data` contiver dois objetos JSON concatenados
(ex: `{"id":1}{"id":2}`), o segundo é silenciosamente ignorado sem erro.

**Impacto:** Em cenários onde o payload AMQP é montado incorretamente (dois frames colados por
bug de serialização upstream), o consumer processa apenas o primeiro objeto sem nenhum sinal de
erro. Difícil de diagnosticar em produção.

**Evidência:** `/ship` code-review T09/T10 — sugestão do code-reviewer.

**Sugestão de solução:**
```go
if err := dec.Decode(v); err != nil {
    return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
}
if dec.More() {
    return fmt.Errorf("%w: payload contains trailing data after first JSON value", ErrInvalidMessage)
}
return nil
```

**Pré-requisitos:** Nenhum. Pode ser feito a qualquer momento após T09.

**Acceptance criteria quando implementado:**
- `NewJSON().Decode([]byte(`{"id":1}{"id":2}`), &v)` retorna `ErrInvalidMessage`.
- `NewJSONStrict()` tem o mesmo comportamento (trailing data não é "campo desconhecido").
- Teste de regressão: payload single-object continua passando.

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

### LATER-04 — Token do semáforo de pool não retornado em caso de panic em `openFn`

**Contexto:** `publisher.go:acquire` — o token de semáforo é devolvido ao canal `p.tokens` apenas
no caminho de erro explícito (linha `p.tokens <- struct{}{}`). Se `openFn` disparar um panic, o
`defer` de retorno do token não existe e o token é perdido permanentemente, reduzindo o tamanho
efetivo do pool a cada panic.

**Impacto:** Panics repetidos em `openFn` (ex: nil pointer em `openPublisherEntry` durante uma
janela de race em `raw`) esgotariam silenciosamente o pool, fazendo todos os `Publish` bloquearem
até deadline. `openFn` é código interno controlado, portanto o risco é baixo, mas a falha é
não-óbvia.

**Evidência:** `/ship` security-audit T12 — LOW finding.

**Sugestão de solução:** Usar `defer` + flag booleana em `acquire` para retornar o token
incondicionalmente em caso de panic:
```go
tokenReturned := false
defer func() {
    if !tokenReturned {
        p.tokens <- struct{}{}
    }
}()
entry, err := p.getOrOpen()
if err != nil {
    return publisherEntry{}, nil, err
}
tokenReturned = true
return entry, func() { p.release(entry) }, nil
```

**Pré-requisitos:** Nenhum. Pode ser feito a qualquer momento após T12.

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

### LATER-07 — `IsTransient` retorna `true` para `ErrChannelPoolExhausted` mesmo em cancelamento voluntário

**Contexto:** `errors.go:IsTransient` — a função verifica `errors.Is(err, ErrChannelPoolExhausted)` para
classificar o erro como transiente (re-tentável). Após o fix em T12, `acquire` retorna
`fmt.Errorf("%w: %w", ErrChannelPoolExhausted, ctx.Err())` quando o ctx dispara antes de um token
estar disponível. Isso significa que um cancelamento explícito (`context.Canceled`) também satisfaz
`IsTransient`, incentivando retry loops a re-tentarem contra um contexto já morto.

**Impacto:** Quando `PublishRetry` for implementado (T13+), um retry automático baseado em
`IsTransient` vai re-tentar a publicação mesmo quando o chamador cancelou o contexto
intencionalmente (ex.: shutdown de serviço, request HTTP cancelado pelo cliente). A re-tentativa
falha imediatamente em `waitBarrier` com `context.Canceled`, mas desperdiça uma iteração do loop
e ofusca a causa real da falha nos logs e métricas.

**Evidência:** `/ship` code-review + security-auditor — T12 review-fix (2026-05-22).

**Sugestão de solução:**

Em `IsTransient`, adicionar uma guarda antes de classificar `ErrChannelPoolExhausted` como
transiente: retornar `false` se o erro também satisfaz `context.Canceled`, pois cancelamentos
voluntários nunca são re-tentáveis:

```go
if errors.Is(err, ErrChannelPoolExhausted) {
    return !errors.Is(err, context.Canceled)
}
```

Alternativamente, usar um tipo de erro estruturado para `ErrChannelPoolExhausted` que carregue
`ctxErr error` como campo, permitindo que `IsTransient` inspecione a causa sem depender de
`errors.Is` em cadeia.

**Pré-requisitos:** T13 (PublishRetry). Atacar este item antes de implementar PublishRetry para
evitar que o loop de retry seja liberado com semântica incorreta.

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

### LATER-12 — `Consumer.Pause`/`Resume` para drain manual sem matar o pod

**Contexto:** `consumer.go` (T18) — hoje a única forma de parar de consumir é (a) `Close(ctx)`
permanente ou (b) reagir ao `basic.cancel` do broker via `OnCancel`. Não há API explícita para o
operador drenar um consumer durante manutenção (deploy, schema migration, troubleshooting) sem
encerrar o processo ou tirar o pod do load-balancer.

**Impacto:** SREs precisam de truques (kill consumer + spin up replacement, ou pausar via
`rabbitmqctl set_policy` em tempo de execução) para drenar uma fila durante operações de
manutenção. Operações simples de "para de consumir desta fila por 5 min" demandam orquestração
externa. Em incidentes, falta de pausa fina leva a kills de pod inteiro com perda de in-flight.

**Evidência:** SRE on-call review (2026-05-22), gap F.

**Sugestão de solução:**
Adicionar `(*Consumer[M]).Pause(ctx) error` que envia `basic.cancel` local (mantendo o canal e
permitindo in-flight handlers terminarem dentro do `ctx`) e `(*Consumer[M]).Resume(ctx) error`
que reissui `basic.consume` na mesma topologia. Métricas:
`consumer_paused_total{queue}` / `consumer_resumed_total{queue}` e gauge
`consumer_paused{queue}` (1/0). Status exposto via `(*Consumer[M]).Paused() bool` para health
probes derivarem prontidão.

Cuidados:
- Pausa não pode interferir com a barreira de reconnect (T07/T16): se reconectar durante pausa,
  o re-subscribe não deve disparar enquanto `paused==true`.
- Documentar que pausa é local ao processo; outros consumers na mesma queue continuam ativos
  (use SAC + Pause para draining estrito do leader).

**Pré-requisitos:** T18 (Consumer base) e T19 (ConsumerMetrics) — depende de re-subscribe loop
existir para reaproveitar o caminho de reissue.

**Acceptance criteria quando implementado:**
- `Pause` retorna após o broker confirmar `basic.cancel-ok` ou após `ctx.Done()`.
- Após `Pause`, deliveries deixam de chegar; in-flight handlers terminam normalmente.
- `Resume` é idempotente; chamar em consumer já ativo é no-op + log warning.
- Forced reconnect durante pausa NÃO reissui `basic.consume`; gauge permanece `1`.
- Teste de integração: pausa, publica 100 msg, sleep 1s (broker enfileira), `Resume`, recebe
  todas as 100; `goleak.VerifyNone` clean.

---

### LATER-13 — Consumer liveness probe (`Healthy()` com last-delivery timestamp)

**Contexto:** `connection.go:Health` cobre apenas a saúde do TCP (broker responde, canal aberto).
Não há sinal disponível para o operador responder "este consumer ainda está consumindo desta
queue ou ele está stuck/quiet?". K8s liveness/readiness probes precisam dessa distinção para
reciclar pods de consumers parados sem matar pods de consumers ociosos por falta de mensagens.

**Impacto:** Probes baseadas só em `Connection.Health()` falham positivos (consumer "vivo" mas
handler em deadlock), enquanto probes naive baseadas em "tem delivery nos últimos N segundos"
falham negativos em queues low-volume. Sem API estruturada, cada time inventa heurística
própria e a comparabilidade entre serviços é zero.

**Evidência:** SRE on-call review (2026-05-22), gap G.

**Sugestão de solução:**
```go
type ConsumerHealth struct {
    Active            bool      // basic.consume em vigor
    Paused            bool      // ver LATER-12
    LastDeliveryAt    time.Time // zero se ainda não recebeu nenhuma
    LastHandlerEndAt  time.Time
    InFlightHandlers  int
}
func (c *Consumer[M]) Health() ConsumerHealth
```
Operador combina os campos em probes apropriadas (ex: "Paused==false && (now-LastDeliveryAt <
30s || InFlightHandlers > 0)"). Não embute decisão de "vivo/morto" — a lib expõe fatos.

Bonus: helper `consumer.HealthCheck(maxIdle time.Duration) http.HandlerFunc` em subpackage
`amqphttp/` separado (opt-in).

**Pré-requisitos:** T18 (Consumer). Independente de LATER-12 mas se conjugam bem.

**Acceptance criteria quando implementado:**
- `Health()` é thread-safe e barato (≤ 5 µs em hot path; sem mutex contention).
- Teste cobre: consumer recém-`Build`, primeira delivery, handler em curso, handler concluído,
  consumer pausado, consumer reconectando.

---

### LATER-14 — Queue-depth sampler / lag observability

**Contexto:** Nenhuma observabilidade nativa de profundidade de fila. Operadores dependem de
scraping externo (`rabbitmq_exporter`, `rabbitmqctl list_queues`) que vive fora da app e
frequentemente atrasa em relação ao estado real. "Queue depth > X" é uma das alertas SRE
mais comuns e mais frequentemente quebra na 3ª da manhã por feed atrasado.

**Impacto:** Sem métrica in-process, o consumer não sabe a profundidade da fila que está
servindo; back-pressure adaptativa (prefetch dinâmico, scale-out hint) fica fora de alcance.
Operadores precisam manter um exporter em paralelo só para essa métrica básica.

**Evidência:** SRE on-call review (2026-05-22), gap H (e gap L é o output desta).

**Sugestão de solução:**
- Nova opção: `Consumer[M].WithQueueDepthSampler(interval time.Duration)` que, num goroutine
  separado, faz `queue.declare-passive` (não-destrutivo) ou GET na management API a cada
  `interval` e exporta `queue_depth{queue}` gauge.
- Defaults conservadores: `interval=30s`, opt-in (default desabilitado para não desperdiçar
  ops em consumers que não precisam).
- Para múltiplos consumers da mesma queue, single-flight para não duplicar overhead no broker.

Cuidados:
- `queue.declare-passive` em uma queue de stream pode ter custo elevado; documentar.
- Não acoplar dependência ao client de management API; usar protocolo AMQP-only.

**Pré-requisitos:** T18 + T19. Não tem urgência para v0.1.

**Acceptance criteria quando implementado:**
- `queue_depth{queue}` Prometheus gauge é exportado quando `WithQueueDepthSampler` está ativo.
- Sampler para automaticamente quando `Consumer.Close` é chamado.
- Falha de `declare-passive` (queue removida) emite `queue_depth_sampler_errors_total{queue,
  reason}` e cessa polling até `Resume` ou re-Subscribe.

---

### LATER-15 — `MaxInFlightBytes` (cap de memória por consumer)

**Contexto:** `consumer_builder.go` (T18) — `Concurrency(n)` limita goroutines mas não memória.
Em queues com mensagens grandes (ex: 5 MiB cada) × `Concurrency=64`, o pico de memória do
processo pode passar de 320 MiB só em handlers in-flight, antes do GC tocar. SRE pediu o
guardrail correspondente ao `MaxMessageSizeBytes` do publisher (LATER-12 deste cycle já
implementado), mas no lado consumer.

**Impacto:** OOM kill em pods com `Concurrency` alto + mensagens grandes. Acontece em
produção quando producer escala message size sem coordenar com consumer (caso real de
schema migration).

**Evidência:** SRE on-call review (2026-05-22), gap I.

**Sugestão de solução:**
- `ConsumerBuilder.MaxInFlightBytes(n int64)` cap de bytes total de bodies em handlers ativos
  (somatório). Quando atingido, novas deliveries não são entregues ao handler até libertar
  bytes (handler retorna → cap libera).
- Implementação: semáforo de bytes (channel ou atomic) decremented após `Decode` (na verdade
  após o handler retornar — quando a memória deve ser liberada).
- Métrica: `consumer_inflight_bytes{queue}` gauge.
- Interação com `Concurrency`: ambos aplicáveis; bytes é o "soft" cap, goroutines o "hard".

Cuidados:
- Considerar contar o body decodificado (estimado por `unsafe.Sizeof` reflexão) ou apenas o
  raw `Delivery.Body` length. Raw é determinístico, decoded é mais realista mas caro de
  medir.

**Pré-requisitos:** T18 + T19. Considerar conjugar com T20 (`MaxRedeliveries`) que já mexe em
hot path.

**Acceptance criteria quando implementado:**
- Carga sintética: 64 goroutines × 5 MiB body, `MaxInFlightBytes(64 * 1024 * 1024)` mantém
  o consumo ≤ 64 MiB de in-flight; bench `BenchmarkConsumerLargeBody` valida.
- Quando o cap está saturado, novas deliveries enfileiram localmente (qos prefetch) ou
  bloqueiam (escolha de design); documentar trade-off.

---

### LATER-16 — Dedupe-by-MessageID middleware first-class (não só exemplo)

**Contexto:** `examples/idempotent_consume/` (T38b) — hoje a recomendação de dedupe está
apenas como exemplo executável. Toda equipe que precisar de idempotência reimplementa o
cache LRU + check + commit, com chances altas de bugs sutis (TTL errado, race entre check e
processamento, double-commit em retries).

**Impacto:** Bugs de dedupe em produção são silenciosos e caros — duplicatas processadas
viram cobranças duplas, emails duplos, etc. Sem helper first-class, cada equipe paga o custo
de aprender o padrão completo (dedupe ANTES do handler, commit DEPOIS do handler retornar
sucesso, TTL ≥ tempo máximo de redelivery, persistente se restart-tolerance for requisito).

**Evidência:** SRE on-call review (2026-05-22), gap J.

**Sugestão de solução:**
```go
type DedupeStore interface {
    Seen(ctx context.Context, key string) (bool, error)
    Mark(ctx context.Context, key string, ttl time.Duration) error
}
func (b *ConsumerBuilder[M]) WithDedupe(s DedupeStore, opts ...DedupeOption) *ConsumerBuilder[M]
```
Defaults: chave = `MessageID` (auto-populado com UUIDv7), TTL = 24h, store in-memory LRU
(opt-in para Redis/etc via implementação da interface).

A lib gerencia: pré-handler check (cache HIT → Ack + métrica), pós-handler mark on success,
no-op em failure (re-deliver replays handler).

Métricas: `consumer_dedupe_hits_total{queue}`, `consumer_dedupe_store_errors_total{queue,
reason}` (store cair não deve quebrar consumo — fail-open com warning).

Cuidados:
- Documentar trade-off "store cair = duplicates passa" vs "store cair = paralisa consumer".
  Fail-open é o default seguro mas precisa estar explícito.
- Cache LRU in-memory NÃO sobrevive restart; documentar como inadequada para guarantees
  cross-restart.

**Pré-requisitos:** T18 + T38b (exemplo serve como referência canônica para a implementação).

**Acceptance criteria quando implementado:**
- `WithDedupe(NewLRUDedupeStore(10_000))` deduplica 100% das mensagens com `MessageID`
  repetido dentro do TTL.
- Falha do store retorna `dedupe_store_error` na métrica mas não bloqueia consumo (fail-open).
- Substituir o exemplo `examples/idempotent_consume/` por uma versão usando `WithDedupe`
  diretamente.

---

### LATER-17 — `WithPublishRateLimit` (token-bucket local no Publisher)

**Contexto:** `publisher.go` — único back-pressure proativo hoje é pool exhaustion
(`ErrChannelPoolExhausted`), que é reativo (já tentou e falhou). Em apps com loops descontrolados
(bug em background job, retry recursivo mal-feito), o publisher pode metralhar o broker até
disparar `connection_blocked_total` — momento em que o broker já está sob pressão.

**Impacto:** Bug em código de aplicação causa incidente em infraestrutura compartilhada. Rate
limiting client-side é a defesa-em-profundidade clássica e está faltando.

**Evidência:** SRE on-call review (2026-05-22), gap K.

**Sugestão de solução:**
- `PublisherBuilder.WithPublishRateLimit(perSec int)` token-bucket via `golang.org/x/time/rate`
  ou implementação minimalista (evitar dependência se possível).
- `Publish` aguarda token; em ctx-cancel retorna `ErrRateLimited` (novo sentinel transient).
- Métrica: `publisher_rate_limited_total{exchange}`.

Cuidados:
- Default = 0 (desabilitado). Não acoplar com `PublishBatch` (que tem semântica all-at-once).
- Documentar que rate limit é client-local; para enforcement broker-side use policies do
  RabbitMQ.

**Pré-requisitos:** T13 (já implementado). Standalone.

**Acceptance criteria quando implementado:**
- `WithPublishRateLimit(100)` permite 100 publishes/s; carga acima é serializada com latência
  observável (benchmark).
- `Publish` com ctx expirado durante wait retorna `ErrRateLimited` wrapping `ctx.Err()`.

---

### LATER-18 — `dlq_depth` gauge (acoplado a LATER-14)

**Contexto:** Hoje a lib emite eventos quando DLX'a uma mensagem (`consumer_handler_seconds`
com outcome=`nack_no_requeue` etc), mas não há gauge de "quantas mensagens estão acumuladas
na DLQ agora". Crescimento monotônico de DLQ é o sinal mais frequente de incidente
backend silencioso (handler degradado, dependência semi-quebrada).

**Impacto:** Operador descobre crescimento de DLQ tarde demais — geralmente quando alguém
abre o management UI por outro motivo. Alertas baseadas em "mensagens DLX'd nos últimos 5
min" são noisy (sazonalidade), enquanto "tamanho atual da DLQ" é sinal claro.

**Evidência:** SRE on-call review (2026-05-22), gap L.

**Sugestão de solução:** Estender o sampler de LATER-14 para também sampler queues DLX
configuradas via `Topology.DeadLetters`. Auto-discovery: para cada `Queue.DeadLetter.Queue`
declarado, exportar `dlq_depth{source_queue, dlq_queue}` gauge.

**Pré-requisitos:** LATER-14 (infraestrutura de sampler). Implementar como extensão natural.

**Acceptance criteria quando implementado:**
- `dlq_depth` gauge exportado para cada DLQ derivada de `DeadLetter` entries.
- Sampler único cobre source queue + DLQ (não duplica goroutines).
- Documentar como configurar alerta Prometheus de exemplo: `dlq_depth > 1000 for 5m`.

---

### LATER-19 — Observability de schema drift quando `NewJSON` (lax-default) ignora campos

**Contexto:** `codec/json.go` — após Rev 8 (Postel's Law), `NewJSON()` aceita campos
desconhecidos silenciosamente. Isso é o comportamento correto para sobrevivência em deploys,
mas torna invisíveis legítimos casos de schema drift que o operador deveria saber sobre (ex:
"v2 do producer subiu mas nunca migramos o consumer; estamos perdendo a feature nova").

**Impacto:** Equipes podem permanecer no schema antigo indefinidamente sem perceber, porque
nada quebra. Drift que deveria ser visível (e levaria a uma decisão consciente: migrar ou
remover o campo) fica enterrado.

**Evidência:** SRE on-call review (2026-05-22), follow-up do item A (Postel) — gap A.1 da
análise original. Reconhecido como desejável mas adiado para manter o commit de Rev 8 focado
em flip de default.

**Sugestão de solução:**
Adicionar opção opcional para hook ao detectar unknown fields no `Decode`:
```go
codec.NewJSON(codec.WithUnknownFieldObserver(func(path string) {
    metrics.SchemaDriftHit(path)
}))
```
Implementação requer duas passadas (parse para `map[string]any` + comparar com struct fields)
ou usar reflection cuidadosa no resultado do `json.Decoder` — não-trivial.

Métrica natural: `codec_unknown_fields_total{codec, field_path}` (cardinalidade controlada;
usar `field_path` truncado se necessário).

Cuidados:
- Overhead do hook deve ser opt-in (default sem hook = zero custo, mantendo a performance
  do path Postel).
- Para field paths nested (ex: `order.items[3].metadata.extra`), decidir granularidade.

**Pré-requisitos:** Rev 8 (já implementado). Standalone.

**Acceptance criteria quando implementado:**
- `WithUnknownFieldObserver` callback dispara uma vez por campo desconhecido por `Decode`.
- Sem hook: zero overhead comparado a antes (bench mostra dentro de ±2%).
- Documentado como o caminho recomendado para detectar drift em produção sem voltar para
  `NewJSONStrict`.

---

### LATER-20 — `Topology.DeadLetters` expansion não cria o binding entre DLX exchange e DLQ

**Contexto:** `topology.go` (~linha 284–298) — a função `expandDeadLetters` cria
automaticamente o DLX exchange (`<source>.dlx`, tipo topic, durable) e a DLQ queue
(`<source>.dlq`, durable), mas **não cria um `Binding`** entre eles. Sem binding, mensagens
dead-lettered chegam ao DLX exchange mas não são roteadas para nenhuma fila e são descartadas
silenciosamente pelo broker.

**Impacto:** Qualquer usuário que declare `Topology.DeadLetters` e espere que mensagens
mortas apareçam na DLQ ficará com a DLQ vazia. O `topo.Declare` não retorna erro (as filas
e exchanges são criados com sucesso), mas o roteamento é silenciosamente quebrado.

**Evidência:** Descoberto durante T18b (`TestHandlerTimeoutVerdict_NackNoRequeue_integration`)
quando a verificação `rawCh.Get(dlqQ)` retornou `ok=false` mesmo após o consumer emitir um
nack-no-requeue. O teste foi adaptado para usar topologia explícita com binding manual como
workaround; esse LATER registra a correção pendente na expansion.

**Sugestão de solução:**
Adicionar ao loop de `expandDeadLetters` (após criar o DLQ queue) um `Binding` que liga
a DLQ ao DLX exchange. Para exchange topic, usar `#` como routing key (captura todos os
dead letters independente do routing key original). Para exchange fanout (se mudarmos o tipo),
qualquer routing key serve.

```go
// Adicionar dentro do if !exists para o dlqName:
out.Bindings = append(out.Bindings, Binding{
    Exchange:   dlxName,
    Queue:      dlqName,
    RoutingKey: "#", // captura qualquer routing key dead-lettered
})
```

Cuidado: o tipo do DLX exchange é atualmente `ExchangeTopic` — `#` funciona corretamente
com topic. Se o tipo for alterado para `ExchangeFanout`, o routing key no binding é ignorado
e pode ficar vazio.

**Pré-requisitos:** T15 (Topology.Declare implementado). Adicionar um teste de integração
em `topology_integration_test.go` que publique uma mensagem, a dead-letter via nack, e
verifique que aparece na DLQ.

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

### LATER-22 — Cardinalidade ilimitada do label `reason` em `consumer_cancelled_total`

**Contexto:** `metrics/prometheus.go:183-188`, `consumer.go:276` — o counter
`consumer_cancelled_total{queue, reason}` usa o **consumer tag** como valor do label
`reason`. Tags auto-geradas seguem o padrão `ctag-<uuidv7>`: cada consumer recebe
um UUID único ao ser criado, e cada restart gera um novo UUID. Cada par único
`(queue, reason)` cria uma nova série temporal no Prometheus, sem limite superior.

O protocolo AMQP 0-9-1 não inclui um campo de causa no frame `basic.cancel` — apenas
`consumer-tag` (shortstr) e `no-wait` (bit). A função `ch.NotifyCancel` do amqp091-go
retorna o consumer tag. Não há como obter "queue deleted" diretamente do frame.

O SPEC §1086 descreve `reason` como `e.g. "queue deleted"`, sugerindo um conjunto
fechado de causas humanas. Esse texto é aspiracional: o protocolo não fornece a causa.

**Impacto:** Em ambientes com muitos restarts (Kubernetes, falhas intermitentes), a
cardinalidade cresce indefinidamente → Prometheus OOM ou degradação de storage. O
cenário crítico é: consumers com UUIDs auto-gerados que são cancelados (queue deletada,
exclusive revoked) e recriados em loop.

**Evidência:** Identificado durante revisão `/ship` do commit `884cf44` (T19), eixo
Segurança/Performance. Não é bloqueador para T19 porque o counter está correto
protocolarmente; o problema é de operabilidade a longo prazo.

**Sugestão de solução:** Quando T36 implementar o ciclo completo de `basic.cancel`
(`OnCancel`, `ErrConsumerCancelled`, retorno de `Consume`), avaliar uma das abordagens:

1. **Normalizar `reason` para um enum fixo**: mapear o consumer tag recebido para uma
   causa derivável do contexto (e.g., checar se a queue existe após o cancel via
   `amqp091.Channel.QueueInspect`; se não existir → `"queue_deleted"`; se existir →
   `"exclusive_revoked"`; default `"unknown"`). Isso mantém cardinalidade O(1).

2. **Remover `reason` do label do counter e expô-lo apenas no log**: o label `{queue}`
   já identifica o recurso afetado; o consumer tag no log (com `queue`, `tag`, causa
   inferida) fornece rastreabilidade sem inflar o cardinality set.

3. **Documentar o risco no godoc de `NewPrometheusConsumerMetrics`**: se a mudança for
   considerada breaking change, ao menos alertar operadores sobre a cardinalidade.

**Pré-requisitos:** T36 (ciclo completo de `basic.cancel`). Não atacar antes: a solução
correta depende de ter o contexto do consumer cancel disponível na goroutine de
`openDeliveryCh`, que T36 vai refatorar.

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

### LATER-29 — `TopologyHint` has 0% test coverage

**Context:** `batch_consumer_builder.go:120-127` — the `TopologyHint(q Queue)` builder method is
never invoked in any test. Neither the "quorum queue with delivery limit → disables counter B" path
nor the "non-quorum → keeps counter B enabled" path is exercised.

**Impact:** `counterBDisabled` can be set incorrectly without any test catching it. A regression
that always disables (or always enables) counter B for quorum queues would silently pass the suite.

**Evidence:** `/ship` test-engineer — Important gap 7 (2026-05-24, post-T23 review).

**Suggested solution:**
- `TestBatchConsumerBuilder_TopologyHint_QuorumWithLimit_DisablesCounterB` — asserts
  `counterBDisabled == true` after `TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 5})`.
- `TestBatchConsumerBuilder_TopologyHint_QuorumNoLimit_KeepsCounterBEnabled` — asserts
  `counterBDisabled == false` after `TopologyHint(Queue{Type: QueueTypeQuorum, DeliveryLimit: 0})`.
- `TestBatchConsumerBuilder_TopologyHint_ClassicQueue_KeepsCounterBEnabled` — non-quorum queue.

**Prerequisites:** None.

---

### LATER-30 — `ackAll` / `nackAll` error paths have 0 test hits

**Context:** `batch_consumer.go:113-115` and `125-127` — the branches that handle an error returned
by `h.raw.Ack` / `h.raw.Nack` inside `ackAll` and `nackAll` are never exercised. All current tests
use a `fakeAcknowledger` with `ackFn`/`nackFn` returning `nil`.

**Impact:** If the error-handling logic in these branches regresses (e.g., wrong error wrapping,
missing return), no test would catch it.

**Evidence:** `/ship` test-engineer — Important gap 8 (2026-05-24, post-T23 review).

**Suggested solution:**
- `TestBatch_AckAll_AcknowledgerError_ReturnsWrappedErr` — `fakeAcknowledger.ackFn` returns
  `errors.New("channel closed")`; assert that `batch.Ack()` returns a non-nil error.
- `TestBatch_NackAll_AcknowledgerError_ReturnsWrappedErr` — same for `nackFn`.
- `TestBatch_Ack_EmptyBatch_NoFrame` / `TestBatch_Nack_EmptyBatch_NoFrame` — create a `Batch[string]`
  with no deliveries and verify that `Ack()`/`Nack()` return `nil` without calling the acknowledger
  (`highest()` returns nil → ackAll/nackAll short-circuit at the nil check on lines 110/122).

**Prerequisites:** None.

---

### LATER-31 — `applyBatchCounterB` reads each sync.Map key twice per delivery

**Context:** `batch_consumer.go:415-442` — the check loop (lines 415-430) and the increment loop
(lines 433-444) each call `cs.m.Load` for every delivery key, so each key is read twice from the
`sync.Map` per batch dispatch. Because `Consume` is single-goroutine there is no correctness risk,
but every key is loaded twice unnecessarily.

**Impact:** Negligible at typical batch sizes (≤ 100); noticeable under microbenchmark at batch
sizes ≥ 1000. Not a production blocker.

**Evidence:** `/ship` code-reviewer — Suggestion (2026-05-24, post-T23 review).

**Suggested solution:** Collect `(key, currentCount)` pairs into a local `[]struct{key string; count int64}`
slice in the first loop, reuse them in the second. Halves sync.Map reads with no added complexity:
```go
type kv struct{ key string; count int64 }
pairs := make([]kv, len(batch.deliveries))
for i, d := range batch.deliveries {
    key := batchCounterBKey(c.tag, d.raw.MessageId, d.raw.DeliveryTag)
    var count int64
    if v, ok := cs.m.Load(key); ok {
        if n, ok2 := v.(int64); ok2 { count = n }
    }
    if count+1 > int64(c.maxRedeliveries) { /* ... */ }
    pairs[i] = kv{key, count}
}
for _, p := range pairs { cs.m.Store(p.key, p.count+1) }
```

**Prerequisites:** None.

---

### LATER-32 — `BatchConsumer` counter B does not emit a warning log when max redeliveries is hit

**Context:** `batch_consumer.go:428` — `Consumer[M].applyCounterB` emits a `logger.Warningf`
(`"warren: max redeliveries exceeded…"`) when counter B fires. `BatchConsumer[M]` silently records
the metric (`c.cm.RecordMaxRedeliveries`) without any log output. The `mc` field on `BatchConsumer`
carries the managed connection (which has `opts.logger`) but the warning is missing.

**Impact:** Operational asymmetry: a `Consumer[M]` emits a warning that appears in log aggregators
when counter B fires; a `BatchConsumer[M]` does not, making the event invisible unless the operator
is actively scraping the `RecordMaxRedeliveries` metric.

**Evidence:** `/ship` code-reviewer — Suggestion (2026-05-24, post-T23 review).

**Suggested solution:** Add a `Warningf` call in `applyBatchCounterB` immediately before
`return fmt.Errorf("%w ...", ErrMaxRedeliveries)`, mirroring `Consumer[M]`'s log line.

**Prerequisites:** None.

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

