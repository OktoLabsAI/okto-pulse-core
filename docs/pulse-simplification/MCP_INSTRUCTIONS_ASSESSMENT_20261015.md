# Assessment das instruções MCP — 2026-10-15

## Resultado e escopo

Há contradições operacionais que induzem chamadas inválidas, trabalho repetido
e uso de regras já substituídas. A prioridade é corrigir essas divergências e
definir uma fonte de cada regra. Encurtar texto indiscriminadamente não resolve
o problema e pode remover orientações importantes de autoridade e evidência.

Solicitação: conferir todas as instruções MCP e resources associados quanto a
melhoria, clareza, duplicidade e ambiguidade. Este checkpoint é **assessment**:
não altera produto, instruções publicadas, permissões, schema ou gates.

Base: Core `3376ba3f`, Community `8e75f23f`, `feature/v0.4.0`.
Captura autenticada do runtime instalado de qualificação em API8240/MCP8241;
home padrão e seus dados não foram alterados.

## Cobertura e método

- Capturados `initialize.instructions`, todas as páginas de `tools/list`,
  `resources/list` e `resources/templates/list`.
- Inventário efetivo: **285 tools, 54 resources, zero templates**. Os resources
  são 53 documentos Markdown e o manifesto do servidor.
- Lidos integralmente pelo coletor os 54 corpos publicados e os 60 documentos
  Markdown locais: 55 do Core, quatro overrides Community e o bootstrap.
- Conferidos nomes de tools, URIs, argumentos documentados em seções `Args:`,
  descrições das 285 tools, hashes, propriedade dos resources e parágrafos
  repetidos. Revisão dirigida das regras normativas e dos fluxos, com leitura
  aprofundada dos trechos conflitantes e confronto com código/schema/testes.
- Todas as 285 tools têm descrição; não foram encontrados links quebrados ou
  nomes de tools inexistentes dentro dessas descrições. Isso não certifica
  semanticamente cada operação ou cada campo de retorno.
- Comparação atual byte a byte: **867 Python Core e 326 Community**, mesmos
  conjuntos de arquivos da instalação isolada, zero divergências. Todos os
  documentos Markdown servidos correspondem ao respectivo proprietário local;
  o manifesto é dinâmico.
- Quatro exemplos confrontados offline com o JSON Schema efetivamente servido:
  string delimitada para alternativas de Decision e alvo cognitivo antigo são
  inválidos; array nativo e `source_ref` são válidos no schema. Não executadas
  mutações para testar esses exemplos; validade de schema não prova autorização
  nem existência dos IDs ilustrativos.

A análise automatizada alcança todo o inventário. Não é uma certificação de
execução semântica das 285 ferramentas, nem uma nova rodada E2E de todas elas.
Nenhum teste de comportamento do produto é declarado como executado aqui.

Evidência local: `PULSE_REFACTOR/.validation-v040/mcp-instructions-review-20261015/`
(`collect.py`, `analyze.py`, `verify.py`, `wire-inventory.json`,
`served-instructions.md`, `served-bodies.json`, `scan.json`, `analysis.json`,
`verification.json`). O recibo versionado
[MCP_INSTRUCTIONS_REVIEW_EVIDENCE_20261015.json](MCP_INSTRUCTIONS_REVIEW_EVIDENCE_20261015.json)
preserva hashes, inventário por URI e resultados, sem credenciais.

## Divergências confirmadas

Referências abaixo usam `core:` para `src/okto_pulse/core/` e `community:` para
`src/okto_pulse/community/`. As linhas correspondem à base acima. Prioridade
alta significa orientação capaz de bloquear/desviar o consumidor; não significa
que tenha ocorrido bypass dos controles do servidor.

### A01 — Criação de Cards: bootstrap contradiz o fluxo atual — alta

`core:mcp/agent_instructions.md:132` proíbe criar Cards antes de Approved.
`reference/card_types.md:15,24` também omite Draft e apresenta Done como estado
permitido para Cards normais, sem ressalvar o congelamento.

O fluxo atual em `workflows/specs.md:209` e `services/main.py:3526–3588` permite
planejar Cards normais/Test em Draft, exclusivamente Not Started. O congelamento
de Cards normais em Spec Done é aplicado por `card_operational_freeze.py` e
coberto por `tests/test_f3_done_spec_admission.py`.

**Correção:** separar criação de planejamento, início da execução e manutenção
após Done. Atualizar bootstrap/card_types e conferir errors/transitions com a
mesma matriz. Não remover a restrição de execução em Draft.

### A02 — Vínculo de cenário exigido duas vezes no mesmo workflow — alta

`core:mcp/resources/workflows/cards.md:170` afirma corretamente que criar um
Test Card vincula os cenários atomicamente e manda não repetir a chamada.
Na linha 191, a tabela de antipadrões manda **sempre** chamar `link_task` depois.

**Correção:** o exemplo de antipadrão deve tratar ausência real de vínculo;
criação bem-sucedida exige conferir o resultado, não escrever o mesmo vínculo
novamente. `link_task` continua válido para associações adicionais.

### A03 — Decision ainda descrita como cobertura por Task — alta

`core:mcp/resources/reference/tool-docs/decision.md:17` e o override Community
equivalente descrevem gate opt-in de pelo menos uma Task por Decision.
`reference/spec_gates.md:513–537` define o contrato vigente: plano explícito
por `obligation_refs` e/ou inspeção, sem mínimo de Decisions; vínculos de Tasks
são contexto, não prova. O schema de `add_decision` já expõe `verification`,
ausente da lista `Args:` dessas duas páginas.

**Correção:** descrever o plano atual, incluir `verification` e um exemplo de
Decision de escopo verificável por inspeção. Remeter à referência de gate e às
tools de review. Não pedir Task ou Test Card artificial para cobrir uma escolha.

### A04 — Community recomenda strings onde o contrato exige arrays — alta

`community:resources/operational/reference/tool-docs/decision.md:26,28` recomenda
strings separadas por `|`. O schema servido aceita array de strings ou null
para `alternatives_considered` e `linked_requirements`. A linha 261 do errors
Community ainda recomenda JSON codificado em string e valores delimitados.
Isso contradiz `core:.../reference/multivalue.md`, que proíbe esses formatos.

Também há exemplos `linked_test_task_ids="<test_task_id>"` nos workflows de
Cards/card_types a reconciliar com o contrato nativo de coleções.

**Correção:** arrays nativos em exemplos e remediações. Não criar parser de
compatibilidade para fazer a documentação antiga funcionar. Não aplicar uma
substituição global: `update_test_scenario_status.evidence` ainda é string JSON
no schema atual; esse caso não é uma divergência documental.

### A05 — Onze argumentos obsoletos em cinco tools de KG — alta

Todos no resource **efetivo Community** `reference/tool-docs/kg.md`:

| Seção/linha | Documentado indevidamente | Contrato servido relevante |
|---|---|---|
| `kg_list_cognitive_readiness_items`, 382 | `entity_type`, `entity_id` | Filtros `artifact_id`, `source_ref`, `signal`, `reason_code` etc. |
| `kg_evaluate_cognitive_readiness`, 400 | `entity_type`, `entity_id` | `board_id`, `source_ref`, `kg_generation_id` |
| `kg_record_cognitive_skip`, 442 | `entity_type`, `entity_id`, `reason` | `source_ref`, `reason_code`, `justification`, `evidence_refs`, `revisit_at`, `kg_generation_id` |
| `kg_clear_cognitive_skip`, 458 | `entity_type`, `entity_id`, `reason` | `board_id`, `source_ref`, `kg_generation_id` |
| `kg_schema_info`, 609 | `include_internal` | Apenas `board_id` |

**Correção:** sincronizar nomes, obrigatoriedade e exemplos com o schema vivo;
preservar autoridade humana de skip. Não equiparar cada parâmetro extra a uma
rejeição executada: a reprodução offline confirma especificamente a ausência
do obrigatório `source_ref`; os demais são divergências de documentação/schema.

### A06 — Bootstrap recomenda `profile="legacy"` removido — alta

`core:mcp/agent_instructions.md:31,94–96` e o KG tool-doc Community:311,320
oferecem esse perfil. `reference/projection_profiles.md:46` diz que ele é
recusado; o contrato de projeção e seus testes confirmam a remoção.

**Correção:** orientar summary/detail/full conforme a família. Preservar a
distinção entre leitura econômica para exploração e contexto de gate antes de
mutação. Não reintroduzir modo legacy.

### A07 — Knowledge: ferramenta inexistente e herança contraditória — alta

`community:resources/operational/reference/errors.md:21` recomenda
`okto_pulse_copy_knowledge_to_card`, ausente das 285 tools.
`core:.../tool-docs/knowledge.md:12–14` diz que omissão não copia Knowledge;
na mesma página, linhas 21–23 prometem propagação por padrão.

**Correção:** explicitar seleção na criação; para Cards existentes usar
get/replace/drop/refresh conforme a intenção. Refresh de snapshot não significa
substituir seleção. Knowledge continua advisory; não exigir anexos ou justificações
em massa para satisfazer um gate que não existe.

### A08 — Remediações ainda dependem de Sprint e migração — alta

Bootstrap:28,41 ainda menciona movimentar Sprints.
`community:.../reference/errors.md:96,124` prescreve criar/ativar Sprint hotfix;
linha 257 sugere reparar ou migrar causa de rejeição por caminho de operador.
`community:.../tool-docs/kg.md:108` inclui Sprint entre os exemplos de origem.

**Correção:** remover a receita de Sprint do contrato vigente e apontar apenas
remediações reais de Bug/Amendment. Para causa atual ausente, descrever a recusa
e a escalada com diagnóstico, sem inventar uma API de migração/reparo. Não
apagar evidência ou reabrir histórico para contornar a inconsistência.

### A09 — Histórico de Spec promete compatibilidade inexistente — média

`core:mcp/resources/reference/spec_gates.md:399–400` diz que registros com
`score`/`summary`/`completeness` permanecem legíveis por compatibilidade.
`tests/test_spec_validation_native_history.py:62–103` e o schema nativo recusam
esses campos, inclusive antes de filtrar/paginar resultados.

**Correção:** explicar somente Current/Previous nativos da v0.4.0; registros
incompatíveis são recusados, não convertidos. Histórico anterior da própria
v0.4.0 continua preservado.

## Oportunidades de simplificação e clareza

### O01 — Uma fonte comum por regra; Community só acrescenta sua operação

O adapter `community:adapters/resources.py` substitui **quatro documentos
inteiros** com precedência 10: workflow KG, errors, tool-doc KG e tool-doc
Decision. Assim, corrigir Core não corrige necessariamente a edição instalada.
A04/A05/A07/A08 demonstram o drift, não apenas um risco hipotético.

Recomendação: manter contratos de domínio e de ferramentas no Core. Mover
detalhes realmente operacionais de Grafx/host para referência Community ligada
pelo catálogo. Remover substituições integrais sem necessidade, como a de
Decision usada para trocar um exemplo de banco. Usar a composição pública já
existente; não criar mecanismo genérico novo nem importar Community no Core.
Antes de remover override, conferir diferenças e preservar o conteúdo operacional
útil. Não copiar cegamente a versão Core: A03 também existe nela.

### O02 — Separar roteiro, contrato e troubleshooting

Bootstrap deve orientar acesso, segurança, seleção do resource e leitura do
resultado. Workflow deve dizer **quando, em que ordem e quando parar**.
Referência deve possuir regra, critérios e rubrica. Tool-doc deve explicar
inputs, outputs, exemplos e exceções. Errors deve ligar código → diagnóstico →
ação, remetendo ao protocolo em vez de reproduzi-lo.

Exemplo concreto: `reference/code-traceability` tem inventário tipado de tools
além de `reference/tool-docs/code-traceability`. Juntos são aproximadamente
90 KB; requisitos gerais e assinatura não precisam ser mantidos em ambos.
O protocolo Path B também reaparece em cards, card_types, errors e tool-docs.
Manter avisos curtos junto da operação quando evitam erro de autoridade;
centralizar a explicação longa e a sequência completa.

### O03 — Obrigatoriedade por ação; evitar leituras e registros mecânicos

O preflight já distingue gate de servidor, protocolo do agente, advisory e
autoridade humana. Essa classificação deve orientar o restante do material.
O bootstrap fala em preflight antes de **ANY card work**, enquanto o roteiro
detalhado o aplica à execução. Tornar claro o que vale para consulta, planejamento,
execução e fechamento, reutilizando resources inalterados na sessão e atualizando
o contexto mutável nos pontos já definidos.

No roteiro de criação, full context é seguido por leituras obrigatórias de
coleções já incluídas. Avaliar reuso **apenas quando completas, atuais e não
truncadas**; manter paginação/drilldown e as leituras de gate exigidas. Comentário
de progresso deve comunicar decisão/obstáculo relevante, não reproduzir cada
recibo de Delivery. Mudanças de uma obrigação de protocolo devem ser explícitas;
não classificá-las como mera revisão ortográfica.

### O04 — Corrigir a causa, não aumentar o volume da Spec

`reference/spec_gates.md:402` prescreve **ADD content** como resposta a uma
dimensão reprovada. Isso incentiva saturação burocrática: clareza pode melhorar
removendo redundância, restringindo escopo ou resolvendo uma contradição.

Substituir por “corrigir os achados fundamentados e reavaliar com evidência”.
Preservar integralmente dimensões, rubricas, severidades, thresholds e proibição
de inflar scores. “MANDATORY”, “GRAVE violation” e IDs históricos de milestones
devem ceder a condição, consequência e ação precisas quando não acrescentam
informação operacional.

### O05 — Melhorar a descoberta dos resources

Os quatro overrides têm a mesma descrição em `resources/list`:
`(operational replacement — backend-specific content)`. Isso explica a
implementação do catálogo, mas não ajuda o consumidor a escolher o recurso.
Descrever finalidade e momento de uso: consultas/consolidação de KG, diagnóstico
de erros, contratos de tools KG, autoria de Decisions.

Não há evidência para perseguir uma redução percentual arbitrária de tokens.
Os 54 corpos somam 687.446 bytes, mas são carregados sob demanda. A análise
local encontrou 69 grupos repetidos, sobretudo entre originais e overrides;
**no conjunto efetivamente servido há só dois grupos literais** (>180 caracteres,
normalizados, excluindo tabelas e blocos de código), 5.067 bytes redundantes.
Um é o preâmbulo padrão de 27 famílias. O ganho principal é remover contradições
e duplicidade de responsabilidade, não apagar esse pequeno preâmbulo.

## Sequência de correção proposta — escopo fechado

| Etapa | Trabalho | Aceite |
|---|---|---|
| 1. Correção factual | A01–A09, nas fontes Core e nos corpos efetivos Community; conferir também exemplos | Nenhuma receita conhecida exige tool/argumento/formato/estado removido; criação de Test não repete vínculo; Decision não exige Task artificial |
| 2. Responsabilidade editorial | O01/O02/O05, preservando URIs úteis e conteúdo operacional | Uma referência normativa por regra; navegação funciona na edição efetiva; ausência de mecanismo concreto no Core |
| 3. Clareza de operação | O03/O04, com revisão explícita de qualquer obrigação do agente alterada | Sem ampliação ou redução silenciosa de autoridade/gate; correção por evidência em vez de texto adicional obrigatório |
| 4. Qualificação | Testes de contrato documental, resources/overlays e jornadas focadas | Evidência da resposta instalada, schemas e instruções coerentes; ledger e catálogo gerado atualizados |

Preservar avaliações independentes da validação unificada, seus critérios,
severidades, autores e histórico. Não é nova unificação de avaliações nem
autorização para enfraquecer controles ou fabricar provas.

## Verificação necessária na execução

1. Comparar todos os nomes e parâmetros de exemplos com `tools/list`; validar
   tipos, obrigatoriedade, enum e arrays, distinguindo exemplos parciais.
2. Testar o catálogo **efetivo** Core+Community, não somente Markdown Core.
   Incluir os casos conhecidos de Draft, Decision, vínculos automáticos,
   Knowledge, ausência de legacy e remediação sem Sprint.
3. Usar as suítes existentes de resources, manifest, catálogo e overlay;
   acrescentar verificações de contrato aos exemplos confirmados. Hash/headings
   isolados detectam edição, não detectam uma regra errada.
4. Regenerar catálogo/manifest somente pelos geradores oficiais quando houver
   mudança relevante; não editar os artefatos gerados à mão.
5. Na instalação de validação, provar paridade de arquivos e processo novo;
   repetir initialize/list/read paginado. Exercitar jornadas mínimas: planejar
   Cards em Draft, Decision com inspeção, seleção Knowledge e diagnóstico KG.
   Não repetir toda a iniciativa fictícia para qualificar uma correção textual.
6. Conferir as oito budgets arquiteturais em zero se a composição de resources
   mudar. Caso uma melhoria exija alterar comportamento/API, separar essa mudança
   da limpeza documental e qualificar o contrato correspondente.

Não são achados: placeholders `<nome>` em READMEs internos; referências a migração
como exemplo do projeto do usuário; menção explícita à recusa de legado; campos
ainda presentes no contrato real apenas porque contêm “legacy/deprecated” na
descrição. Também não foi inferido runtime antigo só pela extensão `.lbug`:
localização física e geração ativa pertencem ao adapter e exigem conferência
própria antes de qualquer alteração.

## Estado para retomada

Atualização solicitada e concluída em 2026-10-15: etapas 1–4, A01–A09 e
O01–O05 implementados. Este documento preserva os achados da base avaliada;
eles não representam pendências atuais. Detalhes no
[ledger](IMPLEMENTATION_LEDGER.md) e no
[recibo de qualificação](MCP_INSTRUCTIONS_UPDATE_EVIDENCE_20261015.json).

261 testes distintos aprovados; 285 tools e 54 resources preservados.
Contratos comuns servidos pelo Core, com um suplemento operacional KG composto
no Community. Captura instalada isolada conferida após reinstalação e reinício;
zero drift de argumentos/links e oito budgets arquiteturais em zero.
Nenhuma alteração dos gates/avaliações do servidor ou da instalação padrão.
