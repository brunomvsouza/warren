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
- `NewJSONLax()` tem o mesmo comportamento (trailing data não é "campo desconhecido").
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

### LATER-03 — Fuzz target do codec cobre apenas `NewJSON()` strict com `any` como destino

**Contexto:** `codec/json_fuzz_test.go:16-21` — `FuzzCodecJSON` usa `var v any` como destino de
decode, o que nunca dispara `DisallowUnknownFields` (qualquer JSON cabe em `interface{}`). O
path strict do decoder — o diferencial de segurança de `NewJSON()` — não é exercitado pelo fuzz
engine. Adicionalmente, apenas `NewJSON()` é testado; `NewJSONLax()` não tem cobertura de fuzz.

**Impacto:** Panics ou comportamentos inesperados em `NewJSONLax()` sob inputs malformados
poderiam não ser detectados antes de chegar em produção. O valor principal do fuzz target atual
é testar "não panics" — objetivo válido mas incompleto.

**Evidência:** `/ship` code-review + test-engineer gaps T09/T10.

**Sugestão de solução:**
1. Adicionar um segundo fuzz target `FuzzCodecJSONLax` que use `NewJSONLax()`.
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
- `FuzzCodecJSON` usa struct tipada como destino; `DisallowUnknownFields` é acionado.
- `FuzzCodecJSONLax` existe e cobre o caminho permissivo.
- Seeds adversariais adicionados ao corpus de ambos os targets.
- `go test -fuzz=FuzzCodecJSON -fuzztime=60s ./codec` e o equivalente lax passam sem panics.

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
