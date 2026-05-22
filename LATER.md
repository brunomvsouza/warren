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
