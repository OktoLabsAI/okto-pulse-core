# Pulse v0.4.0 — ledger integrado de implementação

## Estado para retomada

Iniciativa **em andamento**. Etapa atual: caracterização conjunta F0/F1 + K0 +
I0 + P0: correção do writer F09/porta concluída e publicada, F11 caracterizado;
inventário e suites amplas em andamento. Nenhuma migração real autorizada.
Não confundir esses incrementos com
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
  waiver/revoke e leitura histórica. Patch validado e publicado no par abaixo.
- DEI F11: reproduzido por caso de uso real: description/details incrementam a
  versão, mas o predicado Delivery aceita a prova fallback anterior; title
  invalida o digest. Correção prospectiva depende do contrato/rollout novo,
  preservando históricos (DEI §11.4 + ARQ/VER §11).
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
| DEI | DEI-01…24; DEI-T01…64 | DEI-T53/F09 coberto; F11 caracterizado; demais critérios ainda sem matriz completa |
| ARQ/VER | AC-ARQ-01…16, AC-VER-01…18, AC-INT-01…12; ADV-01…24 | Não executados |
| Arquitetura executável | Todos os budgets zero | Aprovada: oito budgets zero, sem findings, inclusive wheels e READMEs |
| Pacote/ambiente | Wheels iguais às fontes, par/imports/processos | Patch F09 + porta: 771 Core + 311 Community `.py` idênticos |

Custos antes/depois e ensaios de migração/rollback ainda não medidos/executados.
Não copiar resultados de validações de setembro anteriores como resultados atuais.

## Próximo passo concreto

Examinar suites amplas e separar falhas do par atual de falhas reproduzidas na
base. Prosseguir P0 e inventários F0/K0/I0 em [inventory.md](inventory.md), antes
de congelar schema/migrações de entrega incremental. F11 exige correção
prospectiva integrada ao rollout. Após qualquer mudança em `src`, reconstruir,
reinstalar e comparar os bytes do par antes de validar comportamento.

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
de `services/architecture.py`. A cadeia P0 não está concluída; não modificar
digests selados nem extrapolar F11 para a aprovação de todos os gates de Done.

## Marcos publicados e validação complementar

| Repositório | Commit publicado em feature/v0.4.0 | Conteúdo |
| --- | --- | --- |
| Core | `54832726a0adf61e3123b3fca21c209c20913e04` | DTO/guard F09, porta de inventário, política pura única, docs/resources e matriz zero |
| Community | `cac95ef3d962d9f14d99ad7f8e0124a67de5c960` | Writer fechado, consumo da porta, tipos/docs e regressões reais/F13 |
| Community | `ff390b3` | Caracterização F11 por UpdateCardUseCase, sem mocks de autorização/persistência |
| Community | `27f3654` | Inventário ARCHITECTURE alinhado a 1.227 imports públicos; texto exige zero exceções |

F11: 3 testes passaram (`f11-characterization.log`). Execução conjunta com a
integração Delivery: **29 passaram** (`delivery-with-f11.log`). As primeiras
tentativas do fixture falharam por portas ainda não compostas (domain event
reader, critical context e knowledge propagation); foram compostos os adapters
reais, sem desligar guards. Não classificar essas falhas de fixture como bugs
de produto nem como baseline.

Coleta completa (`pytest --collect-only -q`) do par atual: **12.088 Core e 5.265
Community**, exit 0. Primeira suite Core com `pytest -q --maxfail=10`:
**299 passed, 10 failed**, 442,34 s (`core-suite-current.log`). As dez falhas
AF23 chegaram ao mesmo erro SQLite: coluna `specs.skip_delivery_evidence`
ausente. O fixture cria a tabela com sua metadata antes da Community;
`create_all` não acrescenta colunas a tabelas já existentes. Corrigido somente
`tests/sqlalchemy_test_models.py`, incluindo a coluna com o mesmo default false.
As duas suites AF23 afetadas passaram: **39 passed** (`af23-fixture.log`).
Reexecução ampla em `core-suite-fixture.log`, excluindo apenas o stress de
release já aprovado na primeira execução (dados e efeitos do stress isolados
por tmp_path e adapters de teste). Nenhuma mudança em produção para corrigir
essa falha de fixture. Não afirmar reprodução dinâmica na base anterior.

Suite Community continua em `community-suite-current.log`; resultado ainda
pendente. Uma falha conhecida é drift do número de imports em
`docs/ARCHITECTURE.md`, corrigido em `27f3654`; quatro testes AF25 passaram
(`af25-docs.log`). Não declarar a suite ampla aprovada com esse reteste isolado.
Dados permanecem descartáveis; nenhuma fonte `src` mudou durante essas suites.

Captura real de catálogo/OpenAPI/CLI descrita em [inventory.md](inventory.md):
340 tools, 372 paths/460 operações HTTP. Hashes, SHAs e tamanho observado em
[surface-inventory-2026-09-19.json](surface-inventory-2026-09-19.json). Nenhuma
medição de redução de tokens ou fluxo completo foi concluída.

Este incremento não muda schema nem migra registros. Rollback de código exige
reverter **o par** `54832726`/`cac95ef` (Community passa a requerer a porta Core),
sem alterar os ledgers existentes. Nenhuma release/tag foi criada; versões de
pacote continuam 0.3.4 enquanto a iniciativa 0.4.0 está em implementação.
