# Pulse v0.4.0 — ledger integrado de implementação

## Estado para retomada

Iniciativa **em andamento**. Etapa atual: caracterização conjunta F0/F1 + K0 +
I0 + P0 e correção do writer F09. O patch F09/porta passou nos testes focados e
na auditoria completa; publicação do incremento em andamento. Nenhuma migração
real autorizada. Não confundir esse incremento com
a conclusão dos contratos novos de entrega, arquitetura ou verificabilidade.

Este é o ledger único dos dois repositórios. Atualizar após cada incremento
coerente com arquivos, decisões, testes, commits e próximo passo; não interpretar
um documento localizado ou um teste histórico como revisão/execução desta sessão.

## Base e instruções confirmadas — 2026-09-19

| Repositório local | Base de feature/v0.4.0 |
| --- | --- |
| `okto_labs_pulse_core` | `207072509a282e8adec1481d05aee2d54c382bde` |
| `okto_labs_pulse_community` | `b6dda64f512920fa4aaaf9d50c331b1796b87e27` |

Os diretórios inicialmente estavam em v0.3.2, limpos (Core `5e8010db`, Community
`b2b94aa8`). O usuário escolheu expressamente a v0.3.4 **local** como base.
As branches v0.4.0 foram criadas nesses diretórios, sem modificar os outros
worktrees. Commits e pushes de progresso estão autorizados. Releases, tags de
release, merge, parada do Pulse ativo e alterações de dados/permissões reais não.

Correções explícitas do usuário:

- Okto Grafx é o mecanismo atual. Kùzu/LadybugDB foram removidos.
- Não procurar nem usar a ideação original; seguir plano e complementos.
- Core não contém mapeamento relacional nem router concreto. Community consome
  portas públicas, sem reach-in privado. Contrato ausente exige porta no Core.
- Todos os budgets transitórios do `okto-pulse-saas-closure` permanecem zero:
  `import_boundary_baseline`, `singleton_baseline`,
  `dependency_temporary_exceptions`, `graph_runtime_compatibility`,
  `rebuild_artifact_compatibility`, `community_private_reach_ins`,
  `community_adapter_bridges`, `af35_relational_residue`.
- Não acrescentar dialeto Cypher/HNSW concreto ao Core. Contaminação antiga
  reconhecida não autoriza ampliá-la.
- Provar `.py` instalados byte a byte contra **ambas** as árvores `src` antes de
  validar comportamento; fixar imports do par e processo novo para cada execução.
- Catálogo MCP é gerado por `python -m okto_pulse.core.mcp.tools_catalog_generator`.
  Nunca editar `tools_catalog.md` manualmente.

## Especificação lida e precedência

Pacote local: `PULSE_REFACTOR/Pulse_Plano_Codex_v1_3_Arquitetura_Verificabilidade`.
Lidos integralmente, na ordem indicada:

1. `INICIAR_NO_CODEX_ENTREGA.md`.
2. `PLANO_PULSE_CODEX.md` (BASE).
3. `COMPLEMENTO_KG_RASTREABILIDADE.md` (KG).
4. `COMPLEMENTO_ENTREGA_INCREMENTAL.md` (DEI).
5. `ESPECIFICACAO_ARQUITETURA_VERIFICABILIDADE.md` (ARQ/VER).

Também lidos README/FONTES_E_BASELINE do pacote, CONTRIBUTING/CLAUDE dos dois
repos, os dois planos KG internos e os dois documentos Delivery internos.
As referências à ideação original ficam excluídas por instrução posterior do
usuário. Não recuperar instruções históricas conflitantes a partir dela.
Nenhum AGENTS.md localizado nos dois repositórios ou nos diretórios pais.

BASE mantém mandato/invariantes; KG delimita fonte autoritativa/projeção e
adaptações D1–D20; DEI substitui mecanismos de registro/impacto/entrega;
ARQ/VER aplica apenas seus deltas explícitos (§12). IDs de testes/invariantes
devem sempre carregar documento de origem (INV-01 é repetido).

Documentação interna anterior contém premissas superadas (Core com mecanismos,
Ladybug, writers spec-scoped, prova pós-Done, ausência de skip, repair público,
Learning graph-backed/LLM). Corrigir conforme contratos efetivamente revisados,
sem tomar essas instruções históricas como autoridade sobre o pacote v1.3.

## Dependências únicas de engenharia

| Incremento integrado | Depende de | Evidência de conclusão |
| --- | --- | --- |
| F0/F1 + K0 + I0 + P0 | Base confirmada, leitura integral | Ambiente pareado, autoridade/locks, cadeias reais e reproduções F09/F11 |
| Correções writer/docs DEI | Caracterização de writers/consumers | DEI-T53/F09 reproduzido, erro ou convergência explícita, waiver/revoke preservados |
| P1 candidatos/classificação | Arquitetura efetiva, proveniência e locks | AC-ARQ-01…16, população completa, revisão focal, transação/idempotência |
| P2 + I1 resolução efetiva | Modelos reais FR/TR/IR/OR/BR/AC e verifiers | Critérios canônicos, herança delimitada/acíclica, contribuição por card |
| P3 + I2/I3/I4 | Resolução efetiva + admissão por método | Mesmo inventário no plano/contexto/admissão/rollup; ledger/batch/retomada/fechamento |
| F2/F3 + K1/K2/K3/K4 | Policies/histórico/owners e fontes identificados | Retirada de Sprint, migração íntegra, projeção/replay/frescor, captura semântica |
| F4/F5 + K5/K6 + I5 + P4 | Contratos e fontes acima | Health read-only; UI/REST/MCP/KG/analytics e distribuição coerentes |
| F6/F7/F8 + K7 + I6 + P5 | Incrementos integrados | Gates zero, suites/upgrade/rollback/pacote, custos observados e relatório |

Os cortes acima não são novas entidades do produto. I2 não pode congelar schema
de entrega ignorando P2/P3; redução de links depende da resolução efetiva; rollout
de ARQ/VER §11 deve coexistir com migrações Sprint/KG/Delivery. Nenhum checkpoint
vira nó gráfico ou aprovação. Métodos precisam de admission path até o adapter.

## Investigações abertas

- DEI F09: reproduzido com SQLite descartável e requests antigos válidos de
  implementação/teste: ambos retornavam HTTP 200. Patch fecha esse writer de
  provas com `delivery_card_scope_required` (422), orienta rota por card, mantém
  waiver/revoke e leitura histórica. Validação do patch em andamento.
- DEI F11: fallback por título versus alterações description/details e versões/
  receipts; não mudar digests históricos por hipótese.
- P0: interfaces/IDs/proveniência, snapshots efetivos, locks, gates iniciais,
  coverage read model versus inventory e verifiers por método.
- K0/F0: caracterizar implementação já incorporada; não repetir evolução de
  schema, emissores ou trabalho de v0.3.4 existente.
- Isolar ambiente descartável; nunca usar data homes, processos ou dados reais.

Seguir `entrada → autorização → lock → serviço → persistência → evento →
projeção → consumidor`. Escolhas técnicas reversíveis são autônomas; somente
conflito material de política/autoridade/histórico/semântica é escalado, com
reprodução, alternativas e frente isolada.

## Matriz de validação — status inicial

| Documento | Critérios | Estado nesta sessão |
| --- | --- | --- |
| BASE | INV-01…18; T01…46 | Não executados |
| KG | KG-01…66; D/G/Q | Não executados |
| DEI | DEI-01…24; DEI-T01…64 | Não executados |
| ARQ/VER | AC-ARQ-01…16, AC-VER-01…18, AC-INT-01…12; ADV-01…24 | Não executados |
| Arquitetura executável | Todos os budgets zero | Aprovada: oito budgets zero, sem findings, inclusive wheels e READMEs |
| Pacote/ambiente | Wheels iguais às fontes, par/imports/processos | Patch F09 + porta: 771 Core + 311 Community `.py` idênticos |

Custos antes/depois e ensaios de migração/rollback ainda não medidos/executados.
Não copiar resultados de validações de setembro anteriores como resultados atuais.

## Próximo passo concreto

Concluir revisão de distribuição F09/porta e registrar commits pareados e push;
reconstruir/reinstalar/comparar o par após qualquer alteração em `src`. Registrar
commits pareados e push. Prosseguir caracterização F11 (receipt, versão, conteúdo
e edição autorizada), P0 e dependências do inventário efetivo antes de congelar
schema/migrações de entrega incremental.

## Execução 2026-09-19 — primeiro incremento

- Bootstrap publicado: Core `cfedfd5d`, Community `e8d4f31`, ambos na
  `feature/v0.4.0` com upstream correspondente.
- Ambiente descartável: `PULSE_REFACTOR/.validation-v040/venv`, Python 3.13,
  dependências herdadas do ambiente existente; instalação do par apenas nessa
  venv. Dados SQLite/receipts assinados em diretórios temporários dos testes.
  Cada comando abre processo novo. Nenhum processo Pulse ativo foi alterado.
- `verify_pair.py` compara conjuntos completos de `.py` e bytes, origem de
  imports e payloads source → wheel → site-packages com o comparador existente
  `scripts/release_artifact_gate.py`. Baseline e F09: Core 769 `.py` / 834
  payloads; Community 311 `.py` / 393 payloads; igualdade integral.
- Artefatos locais: `wheels-baseline`, `wheels-f09`,
  `provenance-baseline.json`, `provenance-f09.json` no diretório descartável.
- Baseline Core: 64 testes passaram (`test_delivery_evidence_domain`,
  `contract`, `lifecycle`, `test_card_delivery_inventory`,
  `test_coverage_traceability_read_model`); Community: 22 passaram em
  `test_delivery_evidence_integration`. Logs `core-delivery-baseline.log` e
  `community-delivery-baseline.log`.
- Regressão F09 antes do patch: 2 falhas esperadas (HTTP 200 em vez de 422),
  log `f09-red.log`. Caminho revisado: REST `api/code_traceability.py` →
  `RecordDeliveryEvidenceUseCase` → `CommunityDeliveryEvidenceStore.record`
  → tabela antiga; `load_rollup_snapshot` lê provas por card e só waivers
  antigos. A escolha de rejeição explícita é autorizada por DEI §11.1; não há
  tradução implícita sem fence de versão do card.
- Patch Core: DTO spec-scoped fechado para waiver/revoke, defesa de chamadas
  diretas no caso de uso, contrato da porta, documentação MCP e testes de
  DTO. Patch Community: defesa no adapter, retirada da admissão de provas
  antigas, tipo de frontend e testes REST/caso de uso/store/histórico.
- Catálogo MCP e manifest de resources regenerados pelos geradores oficiais;
  documentos Delivery anteriores demarcados como históricos/corrigidos.
- Auditoria baseline `closure-baseline.json`: 7 budgets zero e
  `community_adapter_bridges=3` (limite 0), quatro achados de provenance e
  drift em ambos os READMEs. **Não aprovada.** Investigação obrigatória antes
  de declarar conformidade; não aumentar exceções.
- `tsc -b` passou. A primeira chamada Vitest apontou para caminho inexistente
  e não executou testes; repetição com os caminhos reais passou: 23 testes,
  `DeliveryEvidencePanel` + `CardDeliveryDoDPanel`.
- F09 antes da extração: 76 Core e 26 Community passaram. Após extração para a
  porta: 83 Core (inclui catálogo MCP e report F16) e 51 Community (inclui
  integração Delivery, F13 provenance e AF21 import boundary) passaram. Logs
  `core-f09-port.log` e `community-f09-port.log`.
- Causa das 3 bridges: import e 2 chamadas de `card_delivery_inventory` do
  serviço privado pelo adapter Delivery. Criada porta pública
  `ports/delivery_inventory.py` (`DeliveryInventoryPolicy`), com política pura
  única em `domain/delivery_inventory.py`. Adapter depende da porta; serviços
  mantêm nomes compatíveis apontando à mesma regra de domínio. Nenhuma mudança
  de digest/seleção, novo singleton, mecanismo no Core ou exceção no manifest.
  Teste F13 também bloqueia o import do serviço privado em subprocesso real.
- Wheels `wheels-f09-port`, `provenance-f09-port.json`: 771 `.py` / 836 payloads
  Core e 311 `.py` / 393 payloads Community, todos idênticos. Auditoria
  `closure-f09-port.json`: **oito budgets zero**, nenhum finding de código ou
  distribuição. Só restava drift dos READMEs; fragmentos regenerados pela função
  oficial `render_saas_closure_readme` usando esse relatório. CLI completa
  repetida: exit 0, `ok=true`, nenhum finding de código/distribuição/documentação
  em `closure-f09-final.json`.

Símbolos adicionais revisados: `card_inventory`, `require_card_delivery`,
`require_spec_delivery`, `resolve_delivery_gate_mode`, `delivery_digest`,
`coverage_traceability_read_model`, `CardService.update_card`,
`require_card_operational_mutation_allowed`, listeners de
`sqlalchemy_policy_subject_versioning`, admission/projection `_implementation`
e `_test`, `record_card`, contratos de `ArchitectureInterface`/Design e início
de `services/architecture.py`. A cadeia F11/P0 não está concluída; não modificar
digests selados nem inferir exploração apenas da ausência de campo no hash.
