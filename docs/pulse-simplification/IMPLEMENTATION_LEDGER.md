# Pulse v0.4.0 — ledger integrado de implementação

## Estado para retomada

Iniciativa **em andamento**. Estado consolidado em 2026-09-20: candidatos e
classificação arquitetural, autoria em lote, perfis/vínculos/herança, Test Cards,
contribuições por Card e contrato conjunto com adoção explícita estão publicados.
Entrega incremental registra progress, partial/complete, execução inline/lotes,
seleção e relatório atômico; gate inicial e rollup usam o plano compartilhado.
O gate de conclusão direta do Card foi corrigido para exigir as implementações
selecionadas e preservar falha fechada estrutural. Incrementos e provas abaixo
não equivalem à conclusão integral de I0–I6/P0–P5 ou do plano-base.

Checkpoint funcional atual: Core `57552199` (ledger `683a8a79`) / Community
`88ad207d`. Frente atual: integração de contribuições distintas e provas por
critério, seguida da retomada F2B autorizada. Continuam pendentes a matriz completa
de permissões/paginação/concorrência, receipt→impacto, reconciliação gravável,
observações bufferizadas, métodos especializados, retomada integral, remoção de
Sprints e sua migração, frentes KG, rollout/rollback instalado, benchmarks e
auditoria requisito a requisito. F2B por Card está **autorizado**, com comentário
de depreciação; isso ainda não prova campos/migração implementados.
Nenhuma migração real autorizada. Os últimos resultados e próximos passos ficam
na seção final deste ledger; se divergirem de um checkpoint histórico, prevalece
a evidência mais recente, sem apagar o histórico.

Este é o ledger único dos dois repositórios. Atualizar após cada incremento
coerente com arquivos, decisões, testes, commits e próximo passo; não interpretar
um documento localizado ou um teste histórico como revisão/execução desta sessão.

### 2026-09-19 — progresso declarado no ledger canônico (DEI I1/I2/I3 parcial)

Turno anterior classificado como progresso: par f08aacf1/ed65c2c e ledger 8817145a
enviados. Partida atual: árvores limpas. A investigação do binding confirmou que
o rollup ainda precisa da integração de API/Decision/fallback, adoção e contribuição
versionada; não trocar somente os cinco requisitos e excluir as outras obrigações.

Incremento atual: variante progress na superfície card-scoped existente, na mesma
tabela append-only, sem receipt obrigatório e sem crédito no evaluator. Usa o leaf
canônico card.conclusion.write (domain/permissions.py, relatórios do executor),
não board.read. Resumo reaproveita justification; source_state é claim fechado;
impact_delta reutiliza ImpactEvidence; remaining registra o próximo trabalho.
Targets devem pertencer ao Card; fonte conhecida deve existir no board. Dirty e
unknown não exigem commit nem afirmam recuperação por outro ator. Novos appends
exigem Card normal/bug/Test não arquivado em started/in_progress. Replay exato
mantém identidade e autorização antes do acesso, sem alterar policy_version.

Migração Community expande apenas o CHECK predecessor exato, compara contrato,
preserva todas as colunas/linhas e triggers append-only em transação SQLite.
Drift de schema/triggers ou FK externa nova falha fechado. Não foi executada
em dados reais. Serialização sem progress preserva o digest das requests legadas.
UI integra relato e últimos fatos no painel Delivery, com autoridade própria,
versão real, reuso de chave após timeout e indicação explícita de truncamento.

Ainda não entrega batch/aliases, prova inline, partial/complete, seleção final,
retomada integral paginada, leitor básico sem permissão técnica, selagem do impacto
ou cutover ARQ/VER. Testes, migração descartável e auditoria aprovados; evidências
detalhadas no final deste ledger. Par publicado e confirmado em origin.

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
- Instrução adicional: cada feature que impactar o frontend deve incluir testes
  do frontend dos fluxos afetados e dos estados de erro/permissão relevantes;
  build/typecheck e testes de backend não substituem essa evidência.
  Registrar os cenários e resultados junto ao incremento da feature, incluindo
  a integração na tela que expõe o fluxo quando ela for afetada.

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
| ARQ/VER | AC-ARQ-01…16, AC-VER-01…18, AC-INT-01…12; ADV-01…24 | P1 parcial: política de identidade/contrato e leitura efetiva exercitadas; critérios integrados de aceitação ainda pendentes |
| Arquitetura executável | Todos os budgets zero | Aprovada: oito budgets zero, sem findings, inclusive wheels e READMEs |
| Pacote/ambiente | Wheels iguais às fontes, par/imports/processos | P1 leitor: 773 Core + 311 Community `.py` idênticos; E2E/release completos pendentes |

Custos antes/depois e ensaios de migração/rollback ainda não medidos/executados.
Não copiar resultados de validações de setembro anteriores como resultados atuais.

## Próximo passo concreto

Implementar decisões e promoção/reuso transacionais sobre a adoção prospectiva,
com locks/edição/idempotência existentes e
rollout explícito. Não publicar aprovação ou gate fictício. Continuar
inventários F0/K0/I0 em [inventory.md](inventory.md).
F2B pode implementar compatibilidade autorizada com aviso de depreciação; F11
exige correção prospectiva integrada ao rollout. Falhas amplas e E2E instalados
continuam abertos conforme o registro abaixo. Após qualquer mudança em `src`,
reconstruir, reinstalar e comparar os bytes do par antes de validar comportamento.

## Decisão F2B autorizada — compatibilidade depreciada por Card

O resolver `CardService._resolve_validation_config` usa Sprint → Spec → Board;
o argumento `card` não participa. CardCreate/CardUpdate/CardResponse e o modelo
Community Card não expõem os quatro overrides. O histórico `validations` contém
thresholds de avaliações anteriores, mas não é fonte de policy atual e não pode
ser reinterpretado como tal.

Reprodução em `tests/test_sprint_policy_migration_characterization.py`: dois
cards na mesma Spec resolvem min_confidence 90 e 60 pelas respectivas Sprints;
sem Sprint ambos resolvem 70. Segundo caso preserva False/zero como overrides e
null como herança independente. **2 passed**, log
`sprint-policy-characterization.log`; teste do resolver, sem simular autorização
de transição nem acessar dados reais.

BASE F2B item 5 exigiu decisão porque não existe escopo fiel. Em 2026-09-19 o
usuário **autorizou a preservação por Card**, como compatibilidade para evitar
breaking change/atrito, exigindo comentário e registro de deprecation warning.
Decisão autorizada, ainda sem migração implementada: acrescentar representação
tipada por Card exclusivamente para retenção dos overrides migrados, com origem
histórica opaca e versão. Materializar somente campos cujo valor efetivo muda
sem Sprint; conservar herança dos demais. Isso adiciona representação sem
escolher novos limites, sem novo endpoint para o executor alterar policy e sem
transformar avaliações antigas em aprovação. Regra/resolver no Core; mapping,
migração e persistência no Community por porta pública. Testar equivalência por
campo, novas tasks sem override e falha/retomada da migração.

Alternativa: bloquear o upgrade dos casos divergentes até harmonização humana
explícita das policies nos escopos existentes. Essa alternativa impede a
retirada completa de Sprint nesses ambientes enquanto houver divergências;
não escolher automaticamente a policy mais forte/fraca nem dividir Specs.

**Deprecation warning (de projeto):** essa representação existe somente para
preservar os overrides migrados; não é uma nova hierarquia permanente nem uma
superfície de tuning por executores. Incluir o mesmo aviso no contrato/resolver
e na migração que a materializar. Planejar sua retirada futura somente após
inventário comprovar ausência de overrides ainda necessários ou revisão humana
explícita que os substitua. Não apagar por prazo fixo, reinterpretar histórico
ou convergir automaticamente para a policy Board/Spec. A depreciação não reduz
os gates enquanto a compatibilidade for necessária. Não gerar warnings a cada
leitura nem exigir ação do agente para continuar executando.

O recorte F2B está liberado para implementação/testes. A autorização não inclui
migrar ambiente real; investigação KG/health, arquitetura e evidência continuam.

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

## Investigação da suite ampla e início P1 — 2026-09-19

- Core `25bd70c9` publicado: fixture Spec corrigido, inventário real de
  superfícies e ledger atualizado. A segunda suite ampla Core foi interrompida
  deliberadamente em cerca de 20%, após sete falhas, para investigar e mudar o
  código com processos novos. Somente o processo pytest descartável identificado
  pelo launcher/command line foi encerrado; nenhum Pulse ativo foi tocado.
  `core-suite-fixture.log` é **execução incompleta**, sem summary de aprovação.
- Sete falhas observadas: quatro em `test_card_lifecycle` (duas de relatório
  único, dependência concluída e evento de conclusão), três em
  `test_cognitive_closeout_service_wiring` (advisory, blocking sem debt e
  advisory sem instanciar readiness). Dois casos reproduzidos separadamente
  retornam rejected em vez de done (`core-lifecycle-investigation.log`). O
  adapter de teste só tem o store Delivery antigo, sem `load_card_snapshot`/
  `record_card`; o gate atual recusa esse seam. Não relaxar o gate nem transformar
  o adapter global em prova sempre aceita. A fixture/composição adequada ainda
  precisa ser corrigida e esses testes repetidos.
- Community ampla: **1.510 passed, 5 failed, 2 skipped, 5 errors** em 1.272,97 s.
  Além do drift AF25 já corrigido, falharam init offline, manifesto de schema,
  versão frontend e seis casos/setup E2E instalados por ausência da indicação
  explícita de artefato Grafx. Suite não aprovada.
- Init: meu `DATABASE_URL` fixo e depois `KG_BASE_DIR` fixo interferiam nos roots
  próprios do fixture. Repetido removendo ambos do ambiente, mantendo DATA_DIR
  descartável e caminhos de imports fixos: **1 passed**, 83 s,
  `cli-init-own-roots.log`, engine Grafx real/offline. Erro de harness, não
  alteração necessária no produto. Para novas suites Community deixar o fixture
  controlar DATABASE_URL/KG_BASE_DIR.
- Localizado checkout Grafx `D:/Projetos/Techridy/okto_grafx`, versão 0.0.7.
  E2E requer `OKTO_E2E_GRAFX_REPO` ou wheel explícita. Também foram localizadas
  várias expectativas 0.3.3 no E2E/release gate; precisam alinhar ao par atual
  antes de declarar pacote certificado. Nenhum E2E instalado passou nesta etapa.
- Reprodução na base exata: venv separada `baseline-venv`, wheels reconstruídas
  dos worktrees limpos 20707250/b6dda64. A primeira tentativa de reutilizar
  wheels da árvore principal falhou na comparação estrita de bytes; não foi
  usada para validar comportamento. `wheels-baseline-reproduction` e
  `provenance-baseline-reproduction.json` comprovam 769/311 `.py` e todos os
  payloads idênticos. Baseline AF23 reproduziu coluna ausente; baseline Community
  reproduziu manifesto de schema e versão frontend. Logs `baseline-core-af23`
  e `baseline-community-drift`. Tentativa baseline dos dois casos de lifecycle
  esbarrou antes na coluna ausente (`baseline-core-lifecycle`); não demonstra
  reprodução da rejeição após corrigir essa fixture.
- Drift de schema provado em `schema-drift-proof.json`: retirar somente
  `specs.skip_delivery_evidence` do manifesto atual e classificar a nova tabela
  `card_delivery_evidence_records` como extensão reproduz exatamente o hash
  governado anterior `cac384...aa4a`. Atualizada a expectativa para
  `8b43b7...c09d`, mantendo 65 tabelas herdadas. Nenhum DDL, constraint ou dado
  alterado; o teste continua verificando igualdade exata. Frontend package/lock
  alinhados a 0.3.4; quatro testes de versão passaram.
- P1 iniciado em `domain/architecture_candidates.py`: projeção pura de snapshots
  adotados/autorizados fornecidos pelo chamador. Identidade Spec+raiz+interface;
  edição/revisão separadas; cópias equivalentes mantêm proveniência; conflitos,
  falta de ID e enumeração incompleta não viram conjunto resolvido. Contratos
  `{}`, referência e erro textual são preservados; protocolo/endpoint isolados
  não criam candidato. Conteúdo/digest independem de layout/reordenação de
  participantes e não atribuem provider/consumer pela posição.
- Esse projetor **ainda não está ligado a REST/MCP/UI, classificação, admissão
  ou gates**. Não declarar AC-ARQ integrado satisfeito. IDs legados devem ser
  normalizados em write/migração autorizada; o projetor não os fabrica.
- Validação nova: `core-p1-foundation.log` teve **107 passed, 1 error**; erro era
  o nome parametrizado contendo `:` incompatível com o arquivo de log Windows.
  IDs de teste corrigidos; **24 passed** em `architecture-candidates.log`.
  Os demais casos cobrem propagação/raiz, Spec Validation e caracterização F2B.
  Community: **38 passed**, schema/versão/Delivery/F11,
  `community-p1-foundation.log`. Nenhuma flexibilização de autoridade.
- Build pareado `wheels-p1-foundation`, `provenance-p1-foundation.json`:
  **772 Core + 311 Community `.py`**, payloads 837/393, todos idênticos.
  `closure-p1-foundation.json`: oito budgets zero e zero findings de código;
  somente READMEs divergiam porque o inventário Core passou de 7.338 a 7.344
  imports. Fragmentos regenerados por `render_saas_closure_readme`; repetição
  completa em `closure-p1-final.json`: **exit 0, ok=true**, oito budgets zero,
  nenhum finding de código/distribuição/documentação.

Autorização F2B do usuário incorporada acima e em comentário no resolver atual.
Não há ainda campos/migração por Card implementados; manter o aviso de
depreciação ao implementar o contrato e a migração correspondentes.

## P1 — leitor de arquitetura adotada e decisão F2B preservada — 2026-09-19

Par de código deste incremento:

- Core `eeca39d2c8b70e1b3426de5305d492edd1aa1397`.
- Community `6e3112da3b2a5271e0b53c016940dec6c4f836de`.
- Incremento anterior já publicado: Core `7c547310` e Community `b466038`.
  O comentário de depreciação autorizado está em
  `services/main.py::CardService._resolve_validation_config`, e a decisão,
  proveniência pretendida e condição de retirada estão na seção F2B acima.
  A autorização não cria permissão de edição de policy nem executa migração.

`services/architecture_candidates.py` reutiliza `ResolvedResourceLineageService`
e `ResourceGateService`, consumindo a persistência pela porta pública existente.
A linhagem canônica escolhe o proprietário adotado mais próximo de cada raiz;
todas as variantes físicas nesse proprietário são mantidas. Uma cópia da Spec
prevalece sobre sua própria origem herdada, sem excluir raízes independentes.
Uma cópia adotada no refinamento continua representando seu snapshot quando a
origem muda. Não foi criado outro mecanismo de propagação ou atualização.

O leitor exige Spec no board pedido, recupera todos os IDs selecionados e
confere proprietário/revisão entre metadados e conteúdo. Limite de enumeração,
fonte indisponível, payload ausente, revisão divergente e coleção legada inválida
produzem população desconhecida; não viram zero confirmado. Erros do provider
não expõem mensagens privadas. IDs ausentes continuam pendência, sem geração
em leitura. Classificações, IRs, gates e histórico não são escritos.

Limite explícito: é um **serviço interno** cujo chamador deve ter autorizado a
leitura. Ainda não há rota/tool/UI, classificação, promoção/reuso ou gate de
início conectados a ele. Estes testes não provam autorização de entrada,
concorrência de escrita transacional, aprovação ou AC-ARQ completo. A operação
futura de classificação/transição deve revalidar fontes e locks na transação.

Validação em processos novos, com caminhos do par fixados:

- Rebuild/reinstall dos dois pacotes: `wheels-p1-reader`,
  `provenance-p1-reader.json`. **773/311 `.py`**, payloads **838/393**, todos
  idênticos entre fonte, wheel e instalação antes dos testes.
- Core: `test_architecture_candidates`, `test_resource_lineage_service`,
  `test_copy_architecture_root_coverage_06` e
  `test_sprint_policy_migration_characterization`: **47 passed**, 4,83 s,
  `architecture-reader-core.log`.
- Community: `test_architecture_candidates_integration` (12 casos novos) e
  `test_resource_gate_metadata_only_adapter` (4 existentes): **16 passed**,
  35,40 s, `architecture-reader-community-final.log`. SQLite real/ports reais;
  captura SQL das leituras repetidas contém somente SELECT, preservando versão
  de Design/Spec e conteúdo legado. Falhas de payload/revisão são injetadas
  explicitamente nos testes, sem fingir prova de concorrência real.
- Tentativas intermediárias conservadas: oito casos iniciais passaram em
  isolamento; suite conjunta encontrou 12 erros de fixture por sessão comum
  (`architecture-reader-community.log`). Corrigido para
  `CommunitySemanticSession`, com versioning instalado explicitamente. A
  repetição teve 15 passed/1 failed porque criar Design já incrementa Spec;
  o oráculo passou a capturar a versão **após setup e antes da leitura**, em vez
  de presumir versão 1. Nenhuma proteção de produção foi relaxada.
- `closure-p1-reader.json`: budgets zero/findings de código zero, com apenas
  drift dos READMEs por Core 7.344 → 7.350 imports. Fragmentos regenerados pelo
  renderer oficial. `closure-p1-reader-final.json`: **exit 0, ok=true**, oito
  budgets zero, nenhum finding de código, distribuição ou documentação.

Nenhuma migração de schema/dados, mudança de superfície MCP ou edição de
catálogo ocorreu neste incremento. Não foram repetidas as suites amplas nem
certificado E2E instalado; falhas e limites anteriores continuam abertos.

## P1 — leitura pública autorizada e testes de frontend — 2026-09-19

Par de código: Core `4267c90b35fd674b7aa37398aa4a44e83e882951` e
Community `e93c1f160173db8da2249817d0f3188edf8f48ca`.

O caso de uso `GetArchitectureCandidatesUseCase` compartilha autorização e
projeção entre REST e MCP. Usa snapshot consistente, resolve o board acessível
e a Spec sem includes, exige `spec.entity.read` e `spec.architecture.read`, e
só então carrega fontes. O reader rico de Spec carregava Designs antes dessas
verificações: os testes HTTP reais detectaram isso e motivaram o preflight leve,
sem relaxar permissões. IDs de outro board/usuário retornam 404 sem ler Designs.

REST expõe `GET /boards/{board_id}/specs/{spec_id}/architecture-candidates`;
MCP expõe `okto_pulse_list_architecture_candidates`, registrado como reader.
Resumos paginados (25 padrão, 100 máximo) mantêm totais e diagnósticos globais;
variantes conflitantes não são colapsadas. Detalhe exige ID e digest correntes;
fonte alterada retorna conflito. Fontes desconhecidas não viram zero confirmado.
Nenhuma referência externa é buscada. Catálogo e manifest de resources foram
regenerados pelos geradores oficiais, sem edição manual do catálogo.

Frontend na aba IR da Spec inclui painel de candidatos, paginação e detalhes
sob demanda. Troca de Spec/versão, revogação de permissão e refresh escondem
imediatamente o conteúdo anterior; respostas tardias são ignoradas. Contratos
e referências são texto, sem navegação automática ou execução. Ainda não há
classificar/promover/reusar IR ou novo gate de início neste incremento.

Evidência final, após rebuild/reinstall pareado e antes dos testes de backend:

- `wheels-p1-read-authorized` / `provenance-p1-read-authorized.json`:
  **774 Core + 311 Community `.py`**, payloads **839/395**, igualdade byte a byte
  source/wheel/instalação e resolução dos módulos comprovadas.
- Core: **56 passed**, 9,52 s, `p1-read-authorized-core.log` (política, use case,
  ordem de autorização, wrapper MCP, registry e drift do catálogo).
- Community: **20 passed**, 63,76 s, `p1-read-authorized-community.log` (HTTP
  FastAPI real/UOW/SQLite, reader interno e versão de distribuição). Captura SQL
  comprova ausência de leitura de Designs para board/Spec/usuário negados;
  interceptação DNS comprova que `schema_ref` não é buscado nesses cenários.
- Frontend: **15 passed**, `p1-read-paged-frontend.log`: 10 casos do painel,
  1 caso do cliente HTTP e 4 regressões das entidades estruturadas. Inclui
  permissões, 403 após refresh, resposta tardia de outra Spec, escopo inválido,
  paginação com totais globais, conflito e detalhe com digest desatualizado.
- Build TypeScript/Vite e `verify:frontend-dist` passaram; 78 arquivos, hash
  `35b11f54d87b4859a3fb1ed0c7a1f98ccfa7f4420952c883cbe0e690b88d537b`.
  Assets versionados foram gerados pelo build. Warning de chunk grande persiste;
  não há alegação de medição de performance nem E2E instalado completo.
- `closure-p1-read-authorized-final.json`: **ok=true**, nenhum finding de código
  ou documentação, oito budgets **0/0**. Matrizes README regeneradas: 7.364
  imports Core e 1.229 Community→Core. `git diff --check` passou.

Tentativas intermediárias são mantidas nos logs: fixtures de permissão vazia
(dict herda defaults; lista vazia nega), chamada MCP por wrapper/keywords e realm
local no fixture corrigidos sem mudar semântica. Testes HTTP inicialmente
falharam pela leitura antecipada descrita acima; corrigido o código de produção,
reconstruído/reinstalado/provado o par e só então repetidos os testes afetados.
Closure intermediário apontou exclusivamente drift dos fragmentos README.

Pendência antes de declarar AC-ARQ-01: a propagação recebe
`architecture_design_ids` e copia os selecionados, enquanto a linhagem herdada
existente mantém raízes independentes do ancestral. Investigar com reprodução
da derivação real se seleção representa adoção exclusiva ou somente cópia;
não alterar silenciosamente ResourceGate, histórico ou contrato de herança.
Também continuam pendentes classificação/promoção transacional, rollout,
migração F2B e as demais frentes do pacote. Esta seção supera somente o limite
de ausência de REST/MCP/UI da seção anterior; não certifica P1 completo.

## P0/P1 — seleção de cópia não é adoção exclusiva persistida — 2026-09-19

Commit Community `aea2d9ed98f856e7c81f2cd42185578486d95fff`; código Core
permanece no par `4267c90b` (HEAD de documentação anterior `181c1239`).

Reprodução nova em Community
`tests/test_architecture_adoption_characterization.py`, usando
`McpDeriveSpecUseCase`, UOW/SQLite, publisher e adapters reais, snapshot Done de
Refinement e dois Designs elegíveis. Após commit, a consulta usa uma sessão nova.

| Entrada na derivação | Cópias na Spec | Candidatos depois de reabrir |
| --- | --- | --- |
| `design_ids=[chosen]`, `copy` | 1, raiz chosen preservada | chosen + not-chosen |
| `design_ids=[]`, `copy` | 0 | chosen + not-chosen |
| `design_ids=[chosen]`, `reference_only` | 0 | chosen + not-chosen |

Os três casos preservam IRs vazios e zero Cards. São **caracterizações do legado,
não aprovação de AC-ARQ-01**. Preflight pareado repetido sem alterações em src:
`provenance-p1-adoption-characterization.json`, 774/311 `.py` e 839/395 payloads
idênticos. **3 passed**, 19,66 s, `p1-adoption-characterization.log`.
O helper de seed agora aceita Designs opcionais antes de fixar o snapshot
Done; seus testes existentes passaram: **3 passed**, 14,32 s,
`p1-adoption-fixture-regression.log`. Nenhum gate de produção foi alterado.

Cadeia observada: `McpDeriveSpecUseCase` encaminha seleção para
`RefinementService.derive_spec`; `preflight_architecture_designs` valida a
seleção e `propagate_architecture_designs`/`copy_from_parent` criam snapshots.
`resource_propagation.requested_ids` é resultado transitório anexado ao record,
não uma seleção de adoção persistida. `reference_only`/`none` retornam antes do
preflight de seleção. O filtro Community de linhagem herdada seleciona apenas
Knowledge v2; arquitetura mantém as outras raízes herdadas.
`ResolvedResourceLineageService._prefer_direct_per_unique_resource` exclui
somente a origem equivalente à cópia direta. Além disso, o critic de
`ResourceGateService._effective_architecture_refs` usa referências diretas
quando existem, enquanto as obrigações de cobertura da linhagem mantêm raízes
independentes. Não unificar esses dois comportamentos sem testar seus gates.

Consequência: não inferir adoção histórica exclusiva da presença de uma cópia,
nem tratar lista vazia como exclusão histórica de todas as fontes. O caminho
prospectivo deve registrar seleção/versão de adoção no write autorizado,
reutilizar os snapshots existentes e respeitar o rollout ARQ/VER §11. Specs
legadas em curso/Done conservam seu contrato; adoção normativa usa a revisão
e os locks existentes. Antes de conectar o novo gate, testar juntos critic,
cobertura existente e denominador de candidatos, sem usar classificação como
waiver de obrigação já normativa. Não criar um segundo mecanismo de snapshot.

## P1 — adoção prospectiva persistida na linhagem canônica — 2026-09-19

Par de código: Core `7ac696c2f46eeff119a7c7d6821ba70c756c6f12` e Community
`eda98a8e403998cbd0423814353c9cef5fb4b5a8`.

`domain/architecture_adoption.py::ArchitectureAdoptionScope` define o contrato
versionado, vinculado a board/Spec/edição/ator. `SpecService.create_spec` calcula
a seleção antes do insert, depois do preflight de autorização/linhagem existente,
e a registra no histórico de criação. As duas derivações encaminham a seleção.
O Core usa suas portas; somente Community possui o mapping JSON nullable e o
passo de schema `_migrate_add_spec_architecture_adoption`.

Sem seleção explícita, novas Specs adotam as raízes efetivas então conhecidas;
uma lista adota somente essas raízes; `[]` não adota arquitetura herdada. Tokens
de seleção usam a normalização/aliases já aceitos na cópia. IDs inexistentes
falham antes de gravar a Spec, inclusive em reference_only/none. Não há IR/Card
automático. Copy/derive continuam usando o snapshot existente; referências
continuam acompanhando a fonte corrente, tornando visível seu novo digest.

`ResolvedResourceLineageService` aplica a mesma seleção às obrigações de
cobertura, candidatos e contexto de Cards. As raízes não selecionadas continuam
na linhagem histórica com `effective=false`. Designs diretamente autorados ou
anexados à Spec permanecem efetivos. Fonte adotada ausente ou manifest inválido,
de outro board, com edição futura ou sem versão não vira população vazia.

A porta de metadados ganhou seleção explícita de tipos de recurso. Os leitores
de adoção/candidatos pedem somente arquitetura; o adapter seleciona somente seus
metadados. Isso evita fazer da indisponibilidade de KB/mockup um novo gate de
criação arquitetural. A consulta geral dos gates mantém todos os tipos.

O upgrade adiciona a coluna e **não faz backfill**: NULL mantém a herança legada
exatamente como antes. Instalação limpa usa create_all; replay sem mudança é
`skipped`; coluna incompatível falha fechado. Nenhum dado real foi migrado.
Ainda falta o fluxo autorizado de adoção/revisão para Specs legadas e a
classificação/promoção/reuso com locks, idempotência e gate de início. Esta
etapa satisfaz a seleção prospectiva exercitada de AC-ARQ-01, não P1 inteiro.

Documentação da tool de candidatos atualizada; gerador oficial de manifest de
resources executado (sem delta no manifest). Não houve alteração de registry
nem edição manual do catálogo. Frontend de produção não mudou; o teste novo
verifica que uma revisão troca a população inteira e remove a raiz excluída.

Diagnósticos preservados:

- Primeira execução Community: `community-p1-adoption.log`, 60 passed/4 failed.
  Um fixture de Knowledge não registrava o adapter real de Resource Gate, agora
  necessário à criação; fixture alinhado ao wiring de produção, sem fallback
  permissivo. Repetição desse arquivo: 3 passed, 14,37 s.
- As outras três falhas de schema foram reproduzidas no par v0.3.4 intacto após
  `verify_baseline.py`: 769/311 `.py` byte-identical e payloads pareados. Log
  `baseline-p1-adoption-oracles.log`: 3 failed/27 deselected. Baseline já possui
  72 passos de migração (oráculo dizia 71), skip idempotente de Delivery Evidence
  ausente da lista esperada e 873 objetos de schema (oráculo dizia 868).
- Oráculos corrigidos de forma exata: 73 passos com a adoção nova, skips de
  Delivery Evidence/adoção e os mesmos 873 objetos (esta mudança só adiciona
  uma coluna). A tentativa intermediária atribuiu o skip ao recovery e falhou;
  o diff de conjuntos identificou Delivery Evidence, sem relaxar o teste.
- `community-p1-adoption-final.log`: 63 passed/1 failed antes dessa última
  correção. O arquivo inteiro de schema passou nos demais 29 casos, incluindo
  upgrade real do fixture v0.3.0 e replay/igualdade do schema. O replay antes
  falho passou depois em `community-p1-adoption-scoped.log` (36 passed no lote).
- Auditorias `closure-p1-adoption-final.json` e
  `closure-p1-adoption-scoped.json`: ok=true, todos os budgets 0/0, zero findings.
  Matrizes dos READMEs regeneradas: 7.374 imports Core, 1.229 Community→Core.

Evidência final (processos novos, caminhos fixados para o par):

- `wheels-p1-adoption-verified` / `provenance-p1-adoption-verified.json`:
  **776 Core + 311 Community `.py`**, payloads **841/395**, igualdade byte a byte
  source/wheel/instalação comprovada antes dos testes.
- Core: **125 passed**, 20,65 s, `core-p1-adoption-verified.log`: candidatos,
  caso de uso, linhagem, cópia/propagação, criação/lineage preflight de Spec e
  drift do catálogo. Inclui os gates de propagação já existentes.
- Community: **39 passed**, 82,67 s, `community-p1-adoption-verified.log`:
  criação/cópia/referência/seleção vazia/aliases/whitespace, cobertura e contexto
  de Card, mudança posterior da origem versus snapshot, fontes novas não
  adotadas, desconhecidas sem escrita, corrupção sem falso zero, HTTP autorizado,
  fixtures Knowledge reais e replay de schema. Teste de indisponibilidade
  KB/mockup prova que só metadados de arquitetura são consultados nesse fluxo.
- Caso adicional: **1 passed**, 10,11 s,
  `community-p1-adoption-local-design.log`; uma Spec com adoção herdada vazia
  continua expondo seu Design autorado diretamente, sem reintroduzir o ancestral.
- Frontend: **12 passed**, `frontend-p1-adoption.log` (11 casos do painel e 1 do
  cliente HTTP). Inclui a troca da população após revisão. Como nenhum código
  de frontend de produção mudou, o bundle já conferido no incremento anterior
  permanece byte-identical no wheel/instalação comprovados acima.
- `closure-p1-adoption-verified.json`: **exit 0, ok=true**, zero findings de
  código/distribuição/documentação e oito budgets **0/0**. `git diff --check`
  passou. E2E instalado/Grafx e suites amplas continuam pendentes.

Próximo incremento: decisões/promover/reusar IR com batch atômico e recibo
idempotente. Reutilizar `StructuredSpecEntityService` para autorização, draft,
content lock, canonicalização e validação de vínculos; não chamar repetidamente
o writer de item sem pré-validar todo o lote. A persistência das decisões e do
resultado precisa compartilhar UOW/fence com o IR, sem lógica no adapter.

### Publicação pendente por autenticação

Os commits locais do incremento acima estão concluídos, com ledger pareado em
`1344487f`. Os pushes dos dois repositórios foram recusados pelo GitHub com
`Invalid username or token. Password authentication is not supported`.
`gh auth status` confirmou token inválido no keyring; não há GH_TOKEN ou
GITHUB_TOKEN no ambiente. Não foi alterada configuração de conta/credencial,
nem recriado o histórico por outro transporte. Solicitada reautenticação local
com `gh auth login -h github.com`; retomar push normal após a confirmação.
Esse impedimento afeta publicação, não o trabalho local de implementação.
Últimos pushes confirmados nesta sessão antes da falha: Core `8eb02456` e
Community `aea2d9e`. Não declarar este novo incremento como publicado.

## P1 — regressões de frontend e integração na Spec — 2026-09-19

Requisito de testes de frontend mantido para toda feature que afete a interface.
Community `8a1b934671c7db9bf0ed5a6dccddc20e424626ff` acrescenta **18 casos** aos
testes da leitura de candidatos, sem alterar código de produção:

- Painel: cancelamento e descarte de detalhe tardio após troca de Spec, Board,
  versão, permissão, refresh ou página; rejeição de detalhe de outro
  Board/Spec/candidato/digest; perda e recuperação de permissão sem reutilizar
  contrato antigo; recuperação de indisponibilidade por refresh explícito.
- Cliente HTTP: preservação da página e da identidade/digest exatos na query,
  incluindo escape de caracteres, propagação de cancelamento e leitura GET.
- SpecModal real com painel real: consulta somente ao abrir IRs; ausência de
  cada permissão (`spec.entity.read`, `spec.architecture.read`,
  `spec.integration_requirements.read`) impede a consulta. Esses testes
  complementam os testes isolados do painel e os HTTP/backend já registrados.

Antes dos testes, `verify_pair.py wheels-p1-adoption-verified
provenance-p1-frontend-regression.json` comprovou novamente os **776/311 `.py`**
e os payloads **841/395** byte-identical entre source/wheel/instalação. Processos
de teste novos, frontend carregado diretamente desta working tree.

Evidência: **49 passed** em três arquivos: painel + cliente HTTP, **26 passed**
em 35,81 s (`frontend-p1-late-responses.log`); navegação/integração de SpecModal,
**23 passed** em 41,20 s (`frontend-p1-modal-integration.log`). `npx tsc -b`
e `git diff --check` passaram. São testes de componentes/integração com API
mockada; não equivalem a E2E instalado. Sem alteração de fontes distribuídas,
bundle, schema ou registry; nenhuma alegação de nova execução do closure.

Commit local ainda sujeito à mesma pendência de autenticação do GitHub acima.
Próximo passo funcional permanece classificação/promoção/reuso de IR em lote
atômico e idempotente, com os testes de frontend correspondentes ao expor a UI.

## P1 — preparação integral dos IRs antes da persistência — 2026-09-19

Par de código: Core `6e3fcc5137c83c8c05bb194902bc4aaddf290ad1`, Community
`e9caac46d16454b6296874cb80b4213e35fa32b5`, ambos em `feature/v0.4.0`.

Investigação do writer confirmou que cada `mutate` já publica eventos,
registra histórico e salva. Não pode ser usado repetidamente como preflight de
um lote. Também confirmou que `expected_spec_edition` era comparado com um
`StructuredSpecRecord` sem edição: a condição sempre via `None`, mesmo quando
a edição enviada era a persistida. A projeção agora transporta `Spec.edition`
exatamente; adapters antigos sem esse valor permanecem desconhecidos (`None`),
sem inventar uma edição inicial ou retirar a conferência.

`StructuredSpecEntityService.prepare_integration_requirement_creates` prepara
o conjunto inteiro sem salvar ou emitir eventos. Foram extraídas do writer as
mesmas rotinas de autorização, fence/lock e canonicalização/validação final.
O fluxo individual continua usando essas rotinas; o novo preflight exige
permissões autenticadas e versão/edição explícitas, mantém Draft e content lock,
valida IDs duplicados, campos desconhecidos e vínculos. Canonicalização de
FR/TR/AC legados e remapeamento de referências fazem parte do resultado que o
futuro writer atômico deverá persistir junto aos IRs e decisões.

Isto **não é a classificação/promoção concluída**: ainda não há endpoint de
escrita, recibo de replay, decisão persistida nem gate novo de início. O objeto
preparado é estado do chamador, não autorização durável; o coordenador deve
revalidar/fixar a fonte e usar a mesma UOW com fence de versão/edição para a
gravação conjunta. Nenhuma semântica foi transferida ao adapter, schema ou
catálogo. O frontend de produção permaneceu igual.

Evidência:

- `wheels-ir-preparation` + `provenance-ir-preparation.json`: **776/311 `.py`**
  e payloads **841/395** source/wheel/instalação byte-identical antes dos testes;
  imports pareados e processos novos.
- Core `core-ir-preparation.log`: **72 passed / 1 failed**, incluindo os
  primeiros 20 testes novos e o arquivo inteiro do writer existente.
- Core `core-ir-preparation-regression.log`: **33 passed**, 4,76 s: 21 testes
  novos (incluindo remapeamento legado), Project Structure e drift do catálogo.
- Core `core-ir-preparation-canonicalization.log`: **32 passed**, 4,52 s:
  canonicalização de entidades/requisitos e erros canônicos de APIContract.
- Community: os cinco casos existentes de persistência Project Structure
  passaram no primeiro lote. Os três casos novos com adapter real passaram em
  `community-ir-preparation-scoped.log` (**3 passed**, 14,25 s). Captura SQL prova
  somente SELECTs, inclusive após commit explícito e releitura: sucesso de
  preparação, edição obsoleta e último IR inválido não escrevem nada. O fixture
  inicialmente omitia realm_scope, foi corrigido para o escopo local real; o
  gate `realm_scope_required` foi preservado.
- Frontend `frontend-ir-preparation-compat.log`: **4 passed**, 2,12 s, cliente
  das operações estruturadas. Não é evidência de UI de classificação, que
  permanece pendente e terá testes próprios quando implementada.
- `closure-ir-preparation.json`: **exit 0, ok=true**, oito budgets **0/0**,
  zero findings de código/documentação; distribuição, conformance, AF35 e
  singleton aprovados. Matrizes continuam 7.374/1.229 imports e 25 dependências,
  sem drift dos READMEs. `git diff --check` passou.

Falha preexistente comprovada, ainda aberta: o teste
`test_link_task_validates_target_card_before_persisting` espera rejeição ao
vincular um Card ausente, mas recebe sucesso. `verify_baseline.py` comprovou
769/311 `.py` e payloads pareados da v0.3.4 intacta; o teste reproduziu a mesma
falha em `baseline-structured-missing-card.log` (1 failed, 3,51 s). O validador
comum documenta limpeza de vínculos a Cards removidos; investigar a distinção
entre criação explícita de vínculo e resíduo legado antes de corrigir. Não foi
alterado nem enfraquecido esse teste ou comportamento neste incremento.

Próximo passo obrigatório P1: DTO de decisões com limites/escopo/digest estritos,
porta de persistência atômica IRs + decisões + recibo vinculado ao ator,
coordenador com autorização de todo o lote, bloqueio/fence e replay. Usar o
preflight acima, não fazer loop sobre `mutate`. Em seguida REST/MCP/UI e gates
de atualidade/início conforme ARQ/VER, sem declarar esta fundação como P1 pronto.

Publicação: pushes normais dos dois repos repetidos após `3123b770` e
`e9caac46`; ambos recusados novamente por `Invalid username or token`.
Os commits deste incremento permanecem locais; nenhuma credencial alterada.

## P1 — contrato de classificação e armazenamento atômico — 2026-09-19

Par de código: Core `c987214c5f30d3c6d0739585e0c1179a694c7cfe`, Community
`825476bee4ce571487a32606f862d57a95005905`, em `feature/v0.4.0`.
Contrato novo em `domain/architecture_classification.py`:
intenção tipada (três disposições), fences obrigatórios, limite 1–50/256 KiB,
escopos por membros nomeados sem índices de array, justificativa do restante,
digest vinculado a ator/Board/Spec e resolução de fontes/IRs locais ativos.
Não infere HTTP nem papéis dos participantes; não altera IR existente.

Valores públicos em `ports/architecture_classification.py` e métodos na porta
`StructuredSpecStore`. Community implementa gravação conjunta sob savepoint:
claim do recibo, CAS Board/versão/edição, writer ORM (listeners preservados),
decisões append-only. Replay ator+digest devolve o recibo original sem DML;
conflito não revela recibo de outro ator. Duas tabelas e um índice novos,
criadas pela fronteira create_all existente, mais guard de drift registrado
como passo idempotente/não destrutivo. Não há classificação/backfill legado.

Primeira evidência: `core-classification-store.log`, **92 passed**, 6,73 s;
`community-classification-store.log`, **19 passed**, 35,53 s (persistência,
preflight e Project Structure). Rollback externo, falha injetada, fences
independentes, duas UOWs concorrentes e preservação das partições/histórico
exercitados. Fonte/wheel/install comprovados antes: 778/311 `.py`, 843/395
payloads em `provenance-classification-store.json`.

Segundo lote `community-classification-store-final.log`: **45 passed / 2 failed**,
78,29 s. Casos novos de drift/dados legados passaram. O upgrade real v0.3.0
observou **876 objetos**, +2 tabelas/+1 índice; oráculo antigo de 873 corrigido
para 876. Registro de callable do novo guard foi movido para a mesma ordem
post-create_all do ledger, sem relaxar a comparação de ordem. Essas duas
correções passaram em `community-classification-store-verified.log`: **2 passed**,
19,97 s, incluindo upgrade/replay exato. O ledger possui agora 74 passos
`_migrate_*`; o novo guard valida colunas/tipos/nullability, PK, FKs, índices e
checks das tabelas, sem backfill.

Closure inicial apontou apenas matrizes README antigas; regeneradas pelo
renderer oficial. `closure-classification-store-final.json` passou, budgets
0/0 e zero findings, antes da correção final da ordem do registry. A revisão
final também rejeita IRs com ID ausente/não textual ou duplicado: não transforma
`None` em ID `"None"` nem escolhe uma das duplicatas. Evidência final:

- `wheels-classification-store-hardened` /
  `provenance-classification-store-hardened.json`: **778/311 `.py`** e
  payloads **843/395** source/wheel/install byte-identical, comprovados antes
  dos testes em processos novos. Frontend distribuído também permanece igual.
- `core-classification-store-hardened.log`: **95 passed**, 6,28 s: contratos,
  limites de bytes UTF-8 e cardinalidade, conflitos/partições/IDs/reuso,
  projeção de candidatos e preflight completo dos IRs.
- `closure-classification-store-hardened.json`: **exit 0, ok=true**, oito
  budgets **0/0**, zero findings de código/documentação; distribuição,
  conformance, AF35 e singleton aprovados. Matrizes geradas atualizadas para
  **7.388 imports Core / 1.230 Community→Core**, 25 dependências.
- Ruff dos módulos novos/testes novos e `git diff --check` passaram. Catálogo
  MCP não mudou: nenhum novo handler foi registrado neste incremento.

Ainda falta conectar este armazenamento à preparação dos IRs em um caso de
uso autorizado (sem loop de writers), publicar eventos/histórico no mesmo UOW,
e expor REST/MCP/UI com testes de frontend. Gate/currentness/rollout continuam
pendentes. O armazenamento testado **não prova** o fluxo público completo.

Publicação: após o ledger `30b01a6d`, pushes normais do Core e Community foram
novamente recusados por credencial inválida. Este par permanece local;
nenhuma alteração de credencial, force-push, release ou migração real ocorreu.

## P1 — coordenador autorizado e transação completa — 2026-09-19

Par de código: Core `a78cfc06d14b9f485b7059e051c96964af4d64ee`, Community
`38c568d8579b9f9916a685e22db3322669e58254`, em `feature/v0.4.0`.
Core adiciona `ClassifyArchitectureCandidatesUseCase` e
`ArchitectureClassificationService`, expostos no catálogo público de serviços.
Community adiciona testes com UOW/SQLAlchemy reais e bancos descartáveis.

O caso de uso abre a intenção de escrita antes das leituras, valida acesso ao
Board/realm e Spec local, e autoriza **todo** o lote antes de carregar Designs.
Operações comuns: `spec.entity.read`, `spec.architecture.read`,
`spec.integration_requirements.read`, `spec.entity.edit_fields`. Promoção também
exige `spec.structured_entity.integration_requirement.create`; associação exige
`spec.structured_entity.integration_requirement.update`. `context_only` exige
edição local, preserva IRs normativos e não recebe poder de waiver.
Usa a política central e `interact_in`; nenhum preset/flag é alterado. Claims
legados MCP/system passam pela mesma política e só então são materializados em
um conjunto efêmero fechado das operações concedidas para o preflight antigo.

Dentro da mesma UOW: recibo vinculado ao ator, Draft/content lock, fences de
versão/edição, fontes e digest atuais, resolução de IRs existentes, preparação
única de todos os IRs novos, persistência conjunta, eventos e histórico.
Um lote incrementa a versão uma vez, inclusive quando contém só contexto.
Histórico/eventos conservam identidade e tipo do ator; decisões preservam
contrato completo, fontes e escopos. O recibo lista pendências de readiness/gate,
sem aprovar execução. Replay exige as permissões atuais, devolve o resultado
original mesmo após mudança da fonte e libera a transação sem DML.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `wheels-classification-coordinator` e
  `provenance-classification-coordinator.json`: **780/311 `.py`**, payloads
  **845/395**, source/wheel/install byte-identical antes de testes em processos
  novos. O frontend distribuído integra essa comparação de payloads.
- `core-classification-coordinator.log`: **75 passed**, 6,15 s, contratos de
  classificação, autorização do reader e preparação de IRs.
- Primeiro lote `community-classification-coordinator.log`: **22 passed /
  3 failed**, 50,04 s. Fixture não restaurava os handlers após reset do registry
  e reutilizava a mesma sessão ao trocar de ator. Corrigido para registrar o
  subscriber real e abrir outra sessão para outra identidade; guard de identidade
  permaneceu intacto, nenhuma asserção de atomicidade enfraquecida.
- `community-classification-coordinator-verified.log`: **25 passed**, 46,50 s.
  Oito negações de permissão sem consulta a Designs nem DML; lote misto e contexto
  preservam IR normativo; erro no último IR, fonte obsoleta, fences, lock e estado
  revertem tudo; falhas injetadas no outbox/histórico revertem IRs, decisões e
  recibo já gravados; replay, conflito de ator/payload e isolamento de escopo.
- `community-classification-coordinator-concurrency.log`: **2 passed**, 10,55 s:
  duas sessões semânticas concorrentes devolvem o mesmo recibo e uma única
  gravação; `owner_review_required` continua negando acesso.
- Fixture humana alinhada a `PrincipalKind="human"`; repetição focada humano/
  agente em `community-classification-coordinator-identity.log`: **2 passed**,
  12,68 s, auditoria com tipos `user`/`agent` corretos. São **27 cenários únicos**
  no arquivo novo, sem contar essa repetição como casos adicionais.
- `closure-classification-coordinator-final.json`: **exit 0, ok=true**, oito
  budgets **0/0**, zero findings de código/documentação. Distribuição,
  conformance, AF35 e singleton aprovados. Matrizes README regeneradas pelo
  renderer oficial: **7.414 imports Core / 1.230 Community→Core**, 25 dependências.
  O primeiro closure falhou apenas pelo drift dessas duas matrizes.
- Ruff dos três arquivos novos e `git diff --check` passaram.

Frontend continua com os cenários de leitura já registrados neste ledger.
Este incremento é interno e ainda não altera handlers nem UI. A instrução do
usuário permanece requisito de conclusão: ao expor promoção/associação/contexto
no frontend, incluir testes do cliente, componente e integração na tela afetada,
com permissões, erros, respostas atrasadas e atualização após sucesso/replay.
Build/typecheck e testes desta UOW não substituem esses testes de frontend.

Próximo passo obrigatório: conectar REST e MCP a esse mesmo caso de uso, com
limite bruto antes das dependências/validação do transporte, mapeamento seguro de
erros, permissão estática de writer no registry e regeneração oficial do catálogo.
Depois, UI e testes de frontend, projeção de decisões/currentness, gate de início
e rollout autorizado legado. Não declarar P1 completo nem liberar execução com
base apenas na presença de um recibo. Nenhum processo/dado real foi alterado.

Publicação: após o ledger `f9daf63b`, pushes normais de ambos os repositórios
novamente recusados por `Invalid username or token`. Core `a78cfc06` e Community
`38c568d8` permanecem locais. A solicitação anterior de reautenticação continua
pendente; não alterar credenciais nem reescrever histórico para contorná-la.

## P1 — classificação em REST/MCP e schema publicado — 2026-09-19

Par de código: Core `78b9f3387febc11ce2531505c5d6b845cc4172fb`, Community
`32fe2c52a0cb719aa0af8a8a19228430496dc127`, em `feature/v0.4.0`.

Superfícies disponíveis, usando o mesmo coordenador do incremento anterior:

- REST `POST /boards/{board_id}/specs/{spec_id}/architecture-classifications`,
  com o lote no body. Limite bruto de 256 KiB e validação antes das dependências
  de autenticação/UOW, inclusive whitespace e erro no último item.
- MCP `okto_pulse_classify_architecture_candidates(board_id, spec_id, batch)`:
  schema fechado, classificação de admissão **writer**, permissões exatas no
  registry. FastMCP mantém o envelope outcome v2; payload em `data`, falhas
  tratadas com `isError=true`. A validação anterior ao handler usa a mesma
  projeção segura de erros e não serializa contratos/inputs/ctx do Pydantic.
- Erros compartilhados: 403 autoridade, 404 escopo indisponível, 409 conflito de
  versão/edição/fonte/idempotência/lock/Draft, 422 entrada inválida, 413 tamanho.
  O projector puro tem três símbolos públicos explícitos; módulo inteiro,
  tabela interna e imports auxiliares continuam privados. Expectativa pareada
  de contratos Core/Community atualizada com os mesmos três símbolos.

O schema da promoção agora expõe `AuthoredIntegrationRequirement`, TypedDict
fechado com campos do IR e tipo explícito, status ativo e contrato como JSON
tipado. Preserva campos omitidos, não infere provider/consumer/HTTP e continua
usando o preflight relacional compartilhado para validar o estado final.
Campos extras de autoridade são recusados antes da transação.

Dois defeitos encontrados na montagem do schema MCP foram corrigidos em
`mcp/catalog.py`: `$ref` de modelos aninhados agora aponta para `$defs` sob o
parâmetro correto; a remoção de títulos de metadados preserva propriedades de
negócio chamadas `title`, nomes de definições e valores literais de JSON Schema.
Testes validam um IR real e dois parâmetros tipados com definições independentes.
Não houve alteração de política/gate de negócio para acomodar o schema.

Catálogo/documentação/manifests regenerados pelos geradores oficiais
`tools_catalog_generator`, `ska_tool_manifest` e `ska_resource_manifest`.
Inventário atual: **342 tools / 339 policies / 3 isenções humanas existentes**,
**44 tools de schema fechado**. O teste do server manifest tinha oráculos antigos
340/0.3.3; agora exige 342, presença da nova tool e igualdade com a versão real
do pacote Core. Nenhuma versão de release foi alterada.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-classification-transports.json`, `...-typed.json`, `...-final.json`
  e `...-native.json`: comparação antes de cada lote comportamental. Par final
  `wheels-classification-transports-final`, **781/311 `.py`**, **846/395 payloads**
  source/wheel/install byte-identical, processos novos e PYTHONPATH pareado.
- Core inicial **119 passed**, 11,41 s. Lote após DTO fechado:
  `core-classification-transports-typed.log`, **167 passed / 1 failed**, 13,66 s;
  reproduziu a perda indevida do campo `title` no schema. Corrigido o catálogo,
  não a exigência do campo: `core-classification-transports-final.log`,
  **168 passed**, 14,72 s. Inclui domínio, JSON Schema, contrato público,
  permissões, catálogos/manifests, budgets MCP existentes e governança.
- Community primeira coleta falhou por import do adapter REST no namespace
  incorreto do fixture; corrigido para o adapter Community. Lote seguinte:
  **12 passed / 2 failed**, 28,53 s, por expectativas do limite físico de Board
  e contagem das tools fechadas. Limite real preservado e contagem 43→44.
- `community-classification-transports-typed.log`: **40 passed / 1 failed**,
  79,84 s, incluindo os 27 cenários do coordenador com o DTO fechado. A falha
  restante era leitura do payload no nível errado do envelope nativo no teste.
- `community-classification-transports-final.log`: **14 passed**, 29,62 s.
  REST real e cliente FastMCP real com SQLite/UOW reais: gravação/replay único,
  erro nativo de fonte obsoleta, paridade de permissões/fences/lock/IR inválido,
  zero gravação parcial, schema do host e validação REST antes das dependências.
- `community-classification-transports-native.log`: **5 passed**, 15,95 s,
  após adaptação do erro pré-handler no host. Três casos novos (tamanho, versão
  booleana, campo extra no IR) negados sem autenticação/UOW e sem eco de conteúdo;
  repetidos a chamada/replay real e o gate de schema do host.
- Closure inicial identificou o novo projector ainda não declarado como público
  (cinco bridges), e o seguinte recusou manifestos Core/Community não pareados.
  Resolvido publicando somente os símbolos de contrato e alinhando a expectativa
  pareada; nenhum budget, baseline ou exceção foi aumentado. Matrizes README
  regeneradas oficialmente. `closure-classification-transports-final.json`:
  **exit 0, ok=true**, oito budgets **0/0**, zero findings de código/documentação;
  distribuição/conformance/AF35/singleton aprovados. **7.426 imports Core /
  1.234 Community→Core**, 25 dependências.
- Ruff dos novos contratos/testes e catálogo, mais `git diff --check`, aprovados.

P1 ainda **não concluído**: UI de escrita e seus testes de frontend, projeção das
decisões/atualidade/diferenças, sugestões determinísticas, gate de início e adoção
legada/rollout continuam pendentes. Este incremento não altera frontend; não
atribuir os testes de API/UOW à cobertura de UI. Próximo passo: leitura atual das
decisões com autorização adequada antes de exibir IRs, seguida da autoria na tela
de Spec e testes de cliente/componente/integração, mantendo a lista como decisão
local e nunca aprovação ou waiver. Testes de pacote/Grafx/E2E amplo e demais fases
do plano continuam abertos. Nenhum processo/dado real foi alterado.

Publicação: após o ledger `d75e47b7`, os pushes normais de ambos os repos
continuaram recusados por `Invalid username or token`. O par
`78b9f338` / `32fe2c52` permanece local. Reautenticação solicitada anteriormente
continua pendente; nenhuma credencial ou histórico remoto foi alterado.

## P1 — revisão de classificações e testes de frontend — 2026-09-19

Par de código: Core `df56fa1145d788ef89d59394e47ad6fba3cf5a5b`, Community
`de3355eccfcf55e6ac64fb24fbb0688906378514`, em `feature/v0.4.0`.

O projetor puro `domain/architecture_classification_review.py` reúne candidatos
atuais e testemunhos relacionais da edição, sem criar ou alterar decisões/IRs.
Classifica atualidade como pending/current/review_required/unresolved/retired/
unavailable. Calcula contadores globais antes de paginação/filtro, mantém total
desconhecido para enumeração incompleta e nunca usa fonte inacessível como zero.
Layout, ordem de participantes e revisão física sem delta semântico preservam
a classificação. Alteração de contrato afeta somente o candidato/fragmento ou
restante contextual alterado; IR ausente/inativo/ambíguo exige revisão.

O detalhe usa identidade e digest exatos, retém contrato analisado/atual,
autoria/data/versão/IRs/escopos/proveniência e diferenças RFC6901 (máximo de 100
caminhos/16 KiB, truncamento explícito sem alterar atualidade). Arrays são
atômicos; ausência, null, boolean e número não são equivalentes. Retirada
confirmada permite ler o testemunho pelo digest analisado e mantém obrigações
dos IRs. Conflitos/corrupção detectável ficam não resolvidos ou indisponíveis;
nenhum reparo em leitura. Histórico de outra edição não satisfaz a atual.

Superfícies: REST `GET /boards/{board_id}/specs/{spec_id}/architecture-classifications`
e MCP `okto_pulse_list_architecture_classifications`, schema fechado, limite
1–100/default 25 e filtro opcional de estado. Permissões exigidas antes dos
corpos: `spec.entity.read`, `spec.architecture.read` e
`spec.integration_requirements.read`; leitor anterior de candidatos inalterado.
O caso de uso mantém um snapshot consistente e usa a projeção pública existente
`ApplicationQuery(select_fields=(id, board_id))`, filtrada pelos dois IDs antes
de carregar a Spec. Sem reach-in, exceção ou infraestrutura nova no Core.

Investigação executável: `get_application_record(..., includes=())` não era uma
projeção leve como suposto; suprime relacionamentos, mas lê escalares JSON.
Três testes de negação detectaram leitura de IR antes da autorização. Corrigida
a ordem das verificações e usada projeção explícita/escopo na consulta, mantendo
os asserts de zero leitura de corpos antes da autorização. Dois outros testes
falharam porque o fixture reutilizava sessão após fechar o snapshot; agora cada
leitura abre/fecha sua própria sessão/UOW, como REST/MCP reais. Nenhum guard de
snapshot, permissão ou estado foi relaxado.

Frontend: `ArchitectureClassificationsPanel` na aba IRs da Spec, carregamento
sob demanda, contadores globais, filtro/paginação, contratos/diferenças e
proveniência/IRs em detalhe. Invalida resultados e aborta consultas ao trocar
Spec/versão/página, atualizar, fechar ou perder permissão. Referências externas
são texto, sem fetch. `classification_complete` não é aprovação semântica,
adoção de rollout ou autorização de início; os três flags de avaliação permanecem
false. Fonte removida/context_only não dispensa IRs. Specs Done/arquivadas não
são reabertas nem migradas pela leitura.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-classification-review.json` e `...-final.json`: antes dos lotes
  comportamentais, **783/311 `.py` e 848/395 payloads** idênticos entre source,
  wheels e install; par `wheels-classification-review`, processos novos e
  PYTHONPATH pareado. Frontend build/typecheck passou, **78 arquivos**, SHA256
  `8b03c619139b4943705f449daedde4531231fdfb853dd89dd55488b21ef199ee`.
- `frontend-classification-review.log`: **71 passed**, 52,28 s, cinco arquivos
  (novo componente/cliente e regressões de candidatos/cliente/SpecModal). Inclui
  pendência fora da página, filtros, população desconhecida, detalhes exatos,
  contratos como texto, IRs preservados, escopo/edição/versão/digest divergentes,
  cancelamento/resposta tardia e erros/permissões. Build não substituiu os testes.
- `core-classification-review.log`: **150 passed / 1 failed**, 15,05 s, contagem
  antiga 342 tools. Oráculo atualizado para **343 tools / 340 policies / três
  isenções existentes**, sem aumentar isenções. `...-final.log`: **151 passed**,
  14,73 s, domínio/leitores/contratos públicos/permissões/catalog/manifests.
- `community-classification-review.log`: **16 passed / 5 failed**, 32,65 s,
  investigação descrita acima. `...-final.log`: **40 passed**, 60,40 s; inclui
  SQLite real, ausência de DML em leituras, negação antes dos corpos, filtro de
  outra Board, histórico corrompido, edição/Done, ASGI/FastMCP reais, erro de
  digest 409, leitura negada 403, schema fechado e regressões do writer/replay.
- Catálogo e manifests regenerados pelos geradores oficiais; **45 tools de
  schema fechado**. Ruff dos novos módulos/testes e ESLint dos novos arquivos
  frontend passaram; `git diff --check` aprovado.
- Closure inicial: zero findings de código, oito budgets 0/0; somente as duas
  matrizes README desatualizadas. Regeneradas pelo renderer oficial.
  `closure-classification-review-final.json`: **exit 0, ok=true**, zero findings
  de código/documentação, oito budgets **0/0**, distribuição/conformance/AF35/
  singleton aprovados, **7.447 imports Core / 1.236 Community→Core**, 25 deps.

O usuário confirmou reautenticação. A conta ativa do GitHub e `ls-remote` dos
dois repos foram verificados com sucesso. **Pushes normais concluídos**:
Core `8eb02456` → `1ad0091b` (código `df56fa11` e ledger) e Community
`aea2d9ed` → `de3355ec`. Todos os commits locais acumulados foram enviados a
`origin/feature/v0.4.0`; não houve force-push, release, tag ou merge.

P1 continua **em andamento**. Próximo passo: autoria das três decisões na UI e
testes frontend de envio/erros/replay/conflito, sugestões determinísticas e
integração da atualidade com o gate/rollout autorizado. `architecture_adoption`
continua significando somente escopo de Designs; não é marcador de adoção do
contrato ARQ/VER. Gate inicial, adoção legada, verificabilidade/P2, Delivery/P3,
migração F2B, KG, suites amplas/E2E instalado/Grafx, benchmarks e rollback final
permanecem abertos. Nenhum processo ou dado real foi alterado.

## P1 — autoria em lote na UI e sugestões determinísticas — 2026-09-19

Par de código **publicado por pushes normais** em `feature/v0.4.0`:
Core `ac3ed04699248790a6a67e0b6c70c664d53eefd6`, Community
`9a12b6d47f9510f216f61e7c22c6c36d51d31a41`. Sem release/tag/merge.

`domain/architecture_promotion_suggestion.py` oferece uma proposta pura de IR
no detalhe já autorizado do candidato: título/descrição/endpoint/ref/notas
declarados e schemas/erros/direção/protocolo/participantes preservados. Somente
discriminador explícito reconhecido fornece tipo (`http` → `api`); MCP, gRPC,
in_process ou tipo desconhecido exigem escolha do autor. Nenhum provider,
consumer ou verbo HTTP é inferido. Proposta para contrato inteiro, com campos
obrigatórios faltantes explícitos; só a aceitação/edição do autor a leva ao lote.
Não há fetch de referências, IR automático, tarefa ou nova aprovação.

`ArchitectureClassificationAuthoring`, integrado ao painel e à aba IRs:

- Seleção por identidade/digest exatos, preservada entre páginas/filtros;
  candidatos futuros não entram implicitamente no lote. Contexto e associação
  aceitam vários candidatos selecionados. Promoção permite vários IRs por
  candidato; promoções diferentes podem ser enfileiradas no mesmo lote misto.
- Formulários editáveis, IRs ativos/unívocos da mesma Spec, razão de contexto e
  escopo parcial por caminhos nomeados com razão comum do restante. Espaços em
  nomes JSON são preservados; não são corrigidos silenciosamente para outro
  membro. A proposta de contrato inteiro não é aplicada automaticamente a uma
  adoção parcial; proposta não editada é limpa ao trocar sua fonte/escopo.
- Revisão/edição/remoção de decisões antes do envio, um POST para todo o lote,
  fences de Spec/edição e UUID por intenção, limite de 50 decisões/256 KiB UTF-8.
  JSON inválido, raiz não objeto e números não finitos não viram null omitido.
- Requisição sem retries automáticos. Resultado desconhecido congela a
  tentativa exata para replay; nova negação de permissão não prova que uma
  tentativa anterior incerta nunca gravou. Recibo de escopo/versão/chave
  divergente não é aceito. Sucesso seguido de falha de refresh não reenvia.
- Recusa definida por validação permite editar o conteúdo preservado e gerar
  nova intenção; conflito de fonte/versão/lock exige refresh/revisão, sem
  retarget automático. Uma chamada em voo não duplica por clique repetido.
  Resposta tardia após desmontar/trocar o contexto não atualiza a outra Spec.
- Draft não arquivada, leituras e `edit_fields`/`interact_in` controlam autoria;
  create/update de IR controlam as escolhas correspondentes. Permissão e lock
  continuam exigidos pelo mesmo coordenador no Core. Contexto não dispensa IRs;
  salvar não inicia Spec, não muda policy e não declara prontidão de entrega.

Investigação adicional no writer: a premissa de `includes=()` como projeção
leve também existia antes de sua autorização. Asserts SQL adicionados aos sete
casos de permissão negada reproduziram leitura de `specs.integration_requirements`.
`ClassifyArchitectureCandidatesUseCase` agora usa a porta pública existente
`ApplicationQuery`, filtrada por Board/Spec, projetando apenas id/board_id/status/
archived para autorizar o lote antes dos corpos. Mesmas permissões, gates,
transação e respostas; nenhum budget ou contrato de autoridade relaxado.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- Provas `provenance-classification-authoring.json`, `...-write-auth-before.json`
  e `...-final.json`, antes dos respectivos lotes: **784/311 `.py`, 849/395
  payloads**, source/wheel/install idênticos. Par final
  `wheels-classification-authoring-final`; testes em processos novos, PYTHONPATH
  pareado, SQLite descartável e sem processos/dados reais.
- `frontend-classification-authoring.log`: **111 passed**, 55,38 s, seis
  arquivos, incluindo 32 cenários do novo componente, cliente REST, integração
  no SpecModal e regressões de leitura. Lote misto, vários IRs/escopo parcial,
  sugestão explícita, nenhum HTTP/role inventado, JSON inválido/overflow,
  permissões/estados, byte budget, replay, correção de lote recusado, respostas
  tardias e atualização após sucesso. Estes são testes frontend efetivos.
- `core-classification-authoring.log`: **166 passed**, 14,07 s, propostas,
  domínio/currentness, contratos, catálogo, manifests e permissões. Executados
  antes do ajuste final da projeção de metadados do writer.
- `community-classification-authoring.log`: **26 passed**, 51,39 s, antes desse
  ajuste. `...-write-auth-before.log`: **7 failed / 20 deselected**, 20,52 s,
  reprodução específica da leitura antes da autorização. Após correção e nova
  prova instalada, `...-final.log`: **53 passed**, 104,79 s: coordenador completo,
  guards/lock/replay, leituras, REST/cliente FastMCP e persistência reais.
- ESLint inicialmente recusou o nome `useSuggestion` como hook; renomeado para
  `loadSourceSuggestion`. Typecheck apontou narrowing nullable perdido dentro
  do callback; o draft é materializado após o guard, antes do callback.
  ESLint/Ruff/typecheck/build finais e `git diff --check` aprovados.
- `frontend-classification-authoring-build-final.log` e
  `npm run verify:frontend-dist`: **78 arquivos** sincronizados/verificados,
  SHA256 `fc9694cf60e0d9e7860cafc0d7732ecf755cde1b40442d219a582c2ffdc49dec`.
  Catálogo/manifests regenerados oficialmente; resource manifest `--check`
  passou. Permanecem 343 tools/340 policies/três isenções e 45 schemas fechados.
- Closure inicial tinha somente drift das matrizes README. Regeneradas pelo
  renderer oficial. `closure-classification-authoring-final.json`: **exit 0,
  ok=true**, zero findings de código/documentação, oito budgets **0/0**;
  distribuição/conformance/AF35/singleton aprovados. **7.451 imports Core /
  1.236 Community→Core**, 25 dependências.

P1 **ainda não completo**. Próxima frente: incorporar o predicado relacional de
classificação atual ao gate existente de início e ao readiness, com rollout
ARQ/VER explícito conforme seção 11, coordenado com perfis/critério/método de P2.
Caracterizar `services/spec_readiness.py`, `spec_readiness_read_model.py` e o
writer real da transição antes de editar. `architecture_adoption` continua
apenas escopo de Designs; não transformar esse campo em marcador de rollout.
Legado em andamento preserva contrato aprovado até adoção/revisão autorizada;
Done não reabre. P2/P3, F2B, KG, E2E instalado/Grafx/suites amplas, benchmarks e
rollback continuam pendentes. Não declarar o pacote consolidado concluído.

## Retomada — autenticação e caracterização do gate de início — 2026-09-19

Reautenticação confirmada pelo acesso aos dois remotes. `git ls-remote` retornou
os mesmos HEADs locais de `feature/v0.4.0`: Core `71595f1a8d31348bc0b17d904cce70b72fe8e879`
e Community `9a12b6d47f9510f216f61e7c22c6c36d51d31a41`. Nenhum push de implementação
pendente e ambas as árvores limpas no início desta retomada. A antiga pendência
de autenticação não constitui bloqueio atual.

Caracterização inspecionada, sem alterar semântica ou ativar rollout parcial:

- `services/spec_readiness.py` e `spec_readiness_read_model.py` compõem analytics
  a partir das validações persistidas. `validation.lifecycle_ready` não é o
  veredito completo de admissão ao início; não converter esse fato histórico
  em autorização ARQ/VER nem reescrever validações históricas.
- REST `MoveSpecUseCase` e MCP `McpMoveSpecUseCase` autorizam a transição e
  convergem em `SpecService.move_spec` (`services/main.py`). Este writer confere
  máquina de estados, contexto de entrega e proveniência fixados, rastreabilidade
  e contexto crítico. Em validated→in_progress reexecuta dez verificações de
  cobertura/presença, depois avaliações qualitativas aplicáveis. Guidelines,
  precedência entre Specs e fences de lifecycle continuam no mesmo caminho.
- `ListAllowedTransitionsUseCase._spec_blocked_reason` projeta os bloqueios de
  cobertura, dependências e avaliação. É outro consumidor a integrar ao mesmo
  predicado novo, para não anunciar uma transição que o writer recusará.
- O writer adquire o fence do grafo de dependências, verifica precedência e
  marca a edição iniciada na transação do chamador. O fence da linha confere
  status/edition/version/archived/current_validation_id antes de mutar status;
  validação/checklist e entrega possuem rechecks específicos após o lock.
  A integração ARQ/VER também precisa considerar fontes arquiteturais mutáveis,
  não apenas o version da Spec, antes de afirmar ausência de corrida.
- `last_started_edition` também é gravado por criação direta de Card STARTED
  e pela transição de Card para execução (`CardService`), através de
  `SpecDependencyService.require_ready_for_execution`. É fato de execução da
  edição, não autorização de adoção ARQ/VER. Tampouco `architecture_adoption`
  significa essa adoção: segue sendo o manifesto de escopo de Designs.
- Reabertura em Draft avança `edition`, limpa apenas o ponteiro da validação
  corrente e conserva tentativas anteriores. A seção 11 do complemento exige
  distinguir novas Specs, legado ainda não iniciado, execução já em curso e
  histórico Done; não inferir aprovação nova de nenhum desses dois marcadores.

Verificação do código atual, antes de futura alteração do gate:

- `provenance-start-gate-characterization.json`: origem instalada confirmada,
  **784/311 arquivos `.py`** e **849/395 payloads** idênticos byte a byte entre
  source/wheels/install, usando `wheels-classification-authoring-final`.
- `core-start-gate-characterization.log`: **101 passed em 7,23 s**, em processo
  novo e PYTHONPATH pareado: `test_allowed_transitions_mutation_parity_regressions`,
  `test_spec_readiness` e `test_spec_dependencies_core`. Esta evidência cobre o
  comportamento existente dessas suites, não a integração ARQ/VER ainda ausente.
- Nenhum código de produto/frontend alterado nesta caracterização; a evidência
  frontend do incremento anterior permanece registrada acima, sem alegar nova
  execução. Nenhum processo ou dado real do Pulse foi alterado.

Próximo passo permanece implementação coordenada de adoção ARQ/VER e predicados
de planejamento de P2, seguida da integração no writer/preview e respectivas
superfícies. Não ativar classificação globalmente por ausência de marcador,
não promover `classification_complete` a prontidão integral e não usar qualquer
skip existente como dispensa dos novos contratos. O pacote permanece incompleto.

## P2 — perfis e vínculos canônicos nos critérios existentes — 2026-09-19

Commits de implementação: Core `70ba71899adbac475572b874183c83379fa3d4a2`;
Community `b263b35231e229a1ed0cc445f84f9c583ad9eee2`, ambos em `feature/v0.4.0`.

Incremento de AC-VER-03/04 e da autoria necessária a AC-VER-01/05/06. Reutiliza
`acceptance_criteria`, os IDs, texto da condição, histórico, permissões, content
lock, eventos e writers existentes. Não cria outra entidade de critério ou
ledger de verificação e não ativa o gate/rollout ARQ/VER prematuramente.

Contrato e integração:

- `domain/criterion_verification.py`: metadados fechados por Pydantic,
  `verification_profile` (functional/integration/technical/operational) e
  `requirement_links` com `requirement_type`, `requirement_id` e `aspect`
  opcional. Suporta os cinco tipos FR/TR/IR/OR/BR; BR mantém significado de
  Business Rule. Um alvo por critério, no máximo 100 vínculos; identificador
  estrito não vazio, aspecto não vazio quando fornecido, campos/valores não
  suportados e duplicatas recusados. A condição continua no texto do AC.
- Ausência/null/lista vazia são lacunas possíveis em Draft. Não materializa
  perfis por leitura, não cria condição nem declara passing ou dispensa.
  Targets inativos permanecem referenciáveis para integridade histórica;
  atividade/adequação são predicados do planejamento, ainda a integrar.
- O AC é dono do vínculo. A validação de estado final compara tipo e ID exatos
  às coleções da mesma Spec, sem aproximação por texto/posição. Alvo ausente,
  ambíguo ou de outro escopo é recusado antes da gravação. Alteração de texto
  pelo writer estruturado conserva os metadados. Remoção física de requisito
  ainda referenciado não gera vínculo órfão.
- Canonicalização FR/TR/AC, criação/edição integral da Spec e preparação de
  alterações estruturadas consomem o mesmo contrato. Na criação da Spec a
  validação nova é explícita: o validador geral antigo só era chamado quando
  havia project_structure, o que deixaria a nova referência sem validação.
  Nenhuma validação antiga foi suprimida nem ampliada por suposição.
- Preview de impacto deriva os ACs afetados a partir dos vínculos canônicos,
  com tipo/ID exatos, e mantém o reconhecimento de impacto existente ao revogar/
  substituir/reordenar requisito. Não há array reverso editável no requisito.
- REST e MCP genéricos continuam encaminhando ao writer estruturado. A
  documentação da tool existente descreve os novos campos. Catálogo e manifests
  foram regenerados oficialmente; não há nova tool ou permissão.
- `CriterionVerificationPanel`, no SpecModal, mostra perfil/vínculos e permite
  editar o conjunto num PATCH versionado por critério. Picker por tipo/ID,
  aspectos explícitos, vários requisitos por critério, restrições de leitura
  IR/OR respeitadas. Link existente indisponível permanece no payload se não
  for removido pelo autor. Opções ambíguas não são selecionáveis.
- A UI restringe edição a Draft não arquivada com permissão de update e
  interação no estado. Conteúdo legado/identidade ambígua/metadado desconhecido
  aparece como pendência e não é regravado silenciosamente. Mudança de versão
  ou perda de permissão desmonta o editor. Clique duplo não duplica a chamada;
  resposta de contexto desmontado não recarrega outra Spec. Sucesso seguido de
  falha de refresh oferece apenas reload, sem reenviar a mutação confirmada.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- Par `wheels-criterion-verification` instalado, conferido antes dos testes em
  `provenance-criterion-verification.json`: **785/311 `.py`, 850/395 payloads**,
  source/wheels/install idênticos. Processos novos, PYTHONPATH pareado, dados
  descartáveis; nenhum processo ou dado real do Pulse alterado.
- `core-criterion-verification.log`: **97 passed / 2 failed**, 16,86 s, abrangendo
  novo contrato/integridade, canonicalização e writers estruturados. A falha
  nova era somente o teste tratar `impact_report` como objeto em vez do dict
  público; corrigido para verificar seu conteúdo real. A outra é
  `test_link_task_validates_target_card_before_persisting`, já reproduzida no
  baseline e registrada anteriormente: cleanup legado poda Card inexistente.
  O teste e a pendência permanecem, sem relaxamento ou correção incidental.
- `core-criterion-verification-final.log`: **35 passed**, 7,33 s, incluindo os
  **21 casos novos** e catálogo/manifests/contrato público. Exercita criação e
  atualização integrais, writer estruturado, handler MCP do registry até persistência, IDs
  exatos dos cinco tipos, rejeições sem consumir versão/histórico, preservação
  em edição de texto e impacto reverso. O teste REST novo passou no lote inicial.
- `frontend-criterion-verification.log`: **55 passed**, 55,97 s, três arquivos:
  15 cenários do painel, 36 do SpecModal e quatro verificações existentes do
  roteamento estruturado. `frontend-criterion-authority.log`: **39 passed**,
  27,42 s, repetição do SpecModal após acrescentar três casos de autoridade
  (sem update, sem interact_in e Draft arquivada). **58 testes distintos**, sem
  somar a repetição como cobertura adicional. Inclui envio versionado pelo componente,
  metadados incompletos, alvo indisponível, leitura, permissões/estados,
  recusas, duplicação de clique, contexto tardio e falha de refresh após salvar.
- Ruff dos arquivos Python alterados e ESLint dos novos módulos/frontend testes
  passaram. Typecheck inicialmente identificou retorno `Spec | null` no callback;
  adaptação explícita ao callback void. Helper puro extraído para eliminar aviso
  de Fast Refresh. Build final e `verify:frontend-dist`: **78 arquivos**,
  SHA256 `6aa007259822fa5e2702cff1af9a9839f8ef5da6ce249f3f7235a903cfcea422`.
  Resource manifest `--check` e `git diff --check` aprovados.
- Closure inicial: zero findings de código, oito budgets **0/0**, somente as
  duas matrizes README desatualizadas pelo aumento para **7.459 imports Core**.
  Corrigidas pelo renderer oficial; imports Community→Core **1.236**, 25
  dependências. `closure-criterion-verification-final.json`: **exit 0, ok=true**,
  zero findings de código/documentação, oito budgets **0/0**; distribuição,
  conformance, AF35 e singleton aprovados.

P2 permanece parcial: faltam configuração explicit/inherited por obrigação,
defaults versionados, perfis mínimos, herança/critério terminal e o resolver
relacional compartilhado com `card_delivery_inventory`, contribuição por Card,
métodos com admissão até o adapter e predicados de planejamento. Escopo explícito
sem requisitos estruturados e demais obrigações suportadas pelo inventário
também precisam ser conciliados; estes cinco tipos não redefinem o escopo final.
Próximo incremento: construir essa resolução sobre os vínculos agora persistidos,
sem usar links BR→FR como prova automática, e coordenar adoção ARQ/VER com os
gates de início já caracterizados. P1 integrado, P2/P3, F2B, KG, E2E/Grafx,
migração/rollback, benchmarks e fechamento do pacote continuam pendentes.

## P2 — qualificação por requisito e leitura dos caminhos — 2026-09-19

Commits enviados em `feature/v0.4.0`: Core
`bcd30af85e52a535283283f99b074d0d632d946e`; Community
`77623b0626eb8f9f62ff522916db52aaf0ebfd52`.
A autenticação ativa `oktolabsai-developer` foi confirmada por pushes normais dos
dois repositórios. Os HEADs anteriores `17c8cf1` / `b263b35` já coincidiam com o
remoto. Nenhuma conta, permissão real, processo ativo ou dado real foi alterado.

- `domain/requirement_verification.py`: configuração fechada `explicit|inherited`,
  quatro perfis, seleções de fontes tipadas da mesma Spec, critérios terminais,
  aspecto coberto e digest da definição. No máximo 20 fontes, 100 critérios por
  fonte e 32 KiB de configuração. Não admite `mode=none`, flags de prova ou policy
  de dispensa. FR/TR/IR/OR recebem propostas de defaults v1; BR exige decisão
  explícita. Consultar proposta não grava nada; o writer grava valores autorados,
  sem fabricar recibo de aceite de default ou aprovação semântica.
- Modelos BR/IR/OR, canonicalização FR/TR e writers integrais/estruturados
  preservam o campo. Ausência antiga não vira null por serialização. Comparação
  tipada distingue preenchimento de defaults de alteração autorada. Seleção nova
  exige o digest atual; alteração material posterior da fonte conserva a seleção
  anterior e a torna pendente, sem reescrever o histórico. Preview de impacto
  inclui herdeiros de fontes e de critérios selecionados no rito já existente.
- Resolver relacional puro: todos os requisitos/ACs ativos são examinados antes
  da paginação; vínculos AC→requisito e herança selecionada são as únicas arestas.
  BR→FR, tasks e vizinhança KG não geram crédito. Detecta lacunas, identidade
  ambígua, fonte inativa, digest divergente, ciclo mesmo com digest antigo,
  término ausente e perfil sem caminho. Diamantes mantêm as origens, sem duplicar
  a população de requisitos. Limites de nós/caminhos/profundidade falham fechado.
  Configuração incompleta continua possível como ausência/null em Draft; uma
  configuração preenchida precisa respeitar o contrato fechado.
- `GetRequirementVerificationUseCase` usa a projeção pública de persistência
  existente e snapshot consistente, com board acessível e as três permissões
  de leitura Spec/IR/OR verificadas antes do corpo. REST e nova tool MCP
  `okto_pulse_get_requirement_verification` compartilham esse caso de uso.
  Resposta limitada, paginação de requisitos e caminhos, totais desconhecidos
  quando a população não é completa; catálogo/manifests gerados oficialmente.
- `RequirementVerificationPanel` no SpecModal permite consultar propostas,
  escolher perfis/fontes/critérios/aspectos e enviar um PATCH versionado ao writer
  existente. Draft não arquivada e permissões de update/interação são necessárias.
  Escopo, edição, versão e autoridade desmontam o editor; respostas tardias,
  fontes divergentes, clique duplicado e refresh falho após sucesso são tratados.
  Leitura exige as mesmas permissões do backend; não há defaults silenciosos.

Limite explícito: `criteria_resolution_complete` só descreve estrutura declarada.
Métodos, atribuição de trabalho, suficiência semântica, entrega e rollout retornam
flags `False` de avaliação. O resolver admite entrada de perfis mínimos fornecida
pela autoridade, mas o leitor ainda não tem provider de mínimos do board.
Seleção estrutural de login válido para BR de bloqueio pode estar ligada e ainda
precisa ser rejeitada semanticamente; o backend não simula essa cognição.
Escopo sem requisitos estruturados fica pendente, não satisfeito por vazio.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-requirement-final.json`: par `wheels-requirement-verification-final`
  instalado e revalidado após a correção do registry; **788/311 `.py` e 853/395
  payloads**, source/wheels/install idênticos. Processos de teste novos,
  PYTHONPATH pareado e bancos descartáveis. Nenhum E2E é inferido dessa conferência.
- `core-requirement-final.log`: **100 passed / 1 failed em 13,90 s**. Os 42 casos
  de qualificação passaram, incluindo persistência dos cinco tipos, preservação
  da seleção após editar fonte, recusas sem consumir histórico, impacto reverso,
  ciclos, diamantes, paginação, desconhecido e ausência de crédito BR→FR.
  A falha restante foi o snapshot do registry ainda esperar 343 tools/340 policies.
  Atualizado para **344/341**, mantendo exatamente três exceções humanas antigas.
  A repetição detectou também a policy nova fora de ordem; movida para a posição
  alfabética, sem mudar flags. `core-requirement-registry-final.log`: **17 passed
  em 4,70 s**, registry/catalog/manifests aprovados. São **101 testes Core
  distintos** com os checks afetados aprovados após correção; não somar repetições.
- Nos ensaios anteriores, o teste de persistência usava a projeção retornada pela
  criação como se fosse ORM vivo e comparava tuplas Python com arrays JSON.
  Corrigido para reler/refrescar a linha persistida, sem alterar o produto para
  satisfazer a fixture. A falha baseline de cleanup de Card inexistente registrada
  no incremento anterior não foi reexecutada neste lote nem declarada resolvida.
- `community-requirement-final.log`: **6 passed em 27,95 s**, com SQL real
  descartável, três permissões antes do corpo, isolamento de Spec/board,
  nenhuma gravação nas leituras e paridade HTTP/FastMCP, inclusive rejeições.
- `frontend-requirement-final.log`: **64 passed em 37,57 s**: 15 do novo painel,
  48 do SpecModal (nove novos para estados/autoridade) e um da API. No lote anterior,
  15 regressões do painel de critérios e 16 de atividade também passaram;
  **95 casos distintos de frontend**. A falha inicial do teste de clique duplo
  buscava novamente o botão pelo rótulo anterior, já trocado para Saving;
  corrigido para usar a mesma referência DOM nos dois cliques.
- Typecheck/build e ESLint dos módulos novos aprovados, Ruff dos arquivos Python
  alterados e `git diff --check` aprovados. Build e `verify:frontend-dist`:
  **78 arquivos**, SHA256 `8cd2e0e3e3388c1d4761d51d5e5fc50f25e958d1239423be77054bfbcdb0c22d`.
  O build emitiu aviso de chunks acima de 500 kB; não é medição de custo do fluxo.
- Closure inicial: zero findings de código, oito budgets **0/0**, somente duas
  matrizes README desatualizadas. Regeneradas pelo renderer oficial: **7.490
  imports Core / 1.237 imports Community→Core / 25 dependências**. Distribuição,
  conformance, AF35 e singleton aprovados. `closure-requirement-final.json`:
  **exit 0, ok=true**, zero findings de código/documentação e oito budgets **0/0**.

Retomada: continuar métodos/admissão até adapters e resolução compartilhada com
inventário/contribuição por Card; adoção ARQ/VER e integração writer/preview dos
gates seguem dependentes desses predicados. P2 continua parcial, assim como P1
integrado, P3, F2B, KG, migração/rollback, E2E/Grafx e benchmarks do pacote inteiro.

### 2026-09-19 — autoria de método e primeira admissão autenticada (P2 parcial)

Par de implementação enviado para `origin/feature/v0.4.0` e confirmado por
`ls-remote`: Core **4461ced0dbb6edc88d3454587adeec3f3e97bcb5**, Community
**75952f319011cf08d9c8869e1b1277d2d878d08b**. Reautenticação funcionou;
nenhum bloqueio de push restante. Árvores limpas após os commits do incremento.

- Método fechado independente de scenario_type, writer compartilhado
  REST/MCP versionado e editor de frontend. Ausência histórica não ganha default.
  Método explícito entra no digest da prova; passed/failed exige recibo V2
  autenticado e capacidade publicada pela porta. O adapter atual declara somente
  automated_test; demais métodos podem ser planejados, mas não ganham crédito.
- SpecLockedError movido para contrato público de domínio e reexportado pelo
  serviço, para preservar identidade e evitar reach-in do transporte Community.
- A edição versionada adquire fence condicional da Spec antes de montar a lista
  alterada, incluindo versão/status/edição/archive/Current. Conflito entre leitura
  e escrita não sobrescreve conteúdo. Alterar/limpar método usa a invalidação
  semântica existente, sem mudar scenario_type ou desbloquear conteúdo validado.
- Digest com método explícito usa semantic_schema_version=2 e vincula também
  perfil e links/aspectos de obrigação dos critérios. Sem método, mantém bytes
  da projeção histórica V1: não reescreve recibos anteriores nem infere adoção.
- UI no detalhe expandido de cenário exige Draft, não arquivado e permissões
  de edição/interação; PATCH tem versão, escopo e zero retries. Mudança de
  escopo/versão/autoridade desmonta o editor; clique duplo, resposta tardia e
  falha de refresh após salvar não repetem a gravação. Método desconhecido
  permanece visível e sem fallback silencioso.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-method-final.json`, par `wheels-method-final`: **789/311 .py,
  854/395 payloads**, árvores source/wheel/install idênticas. Testes em processos
  novos com PYTHONPATH pareado e bancos descartáveis; runtime do usuário intacto.
- `core-method-final.log`: **131 passed em 21,24 s**, incluindo vocabulário
  fechado, ausência histórica, digest e qualificação, prova não autenticada com
  board Skip, fence após leitura, lifecycle, Evidence V2, contrato da exceção
  pública, catálogo/manifests gerados e manifesto de contratos públicos.
- `community-method-final.log`: **61 passed em 68,59 s**. REST e handler MCP
  usam o mesmo writer; versão antiga, método inválido, campos extras, board
  alheio, falta de permissão e conteúdo fora de Draft são recusados. Replay HTTP
  real via ASGI produz recibo assinado pelo adapter; alteração de método e
  adulteração de recibo não ganham crédito. Inclui regressões de Evidence V2
  e entrega. Este ensaio não é E2E do Pulse instalado/Grafx.
- As falhas preparatórias eram fixtures: versão herdada não era 1; permissões
  novas conservam a autoridade histórica `spec.tests.create/update_status`;
  GET sem o contexto completo foi substituído por releitura SQL da persistência.
  O SpecModal precisava isolar o painel de policy não relacionado. As regras
  do produto não foram relaxadas para satisfazer esses testes.
- `frontend-method-final.log`: **36 passed em 15,96 s**, sendo 10 do editor,
  24 de SpecModal.activity e dois da API. Cobrem métodos pendentes, remoção,
  valor histórico desconhecido, read-only por estado/archive/permissão, versão,
  refresh, clique duplo, resposta tardia e URL/body/retries do PATCH.
- Typecheck/build, ESLint dos módulos novos, Ruff Python e diff-check aprovados.
  `frontend-method-build.log` e `frontend-method-dist.log`: **78 arquivos**,
  SHA256 `f84501abf2b26ba282bb0de52473dc7ed27e66a707ac8c8cb8c21b0f484486e0`.
  Aviso de chunks acima de 500 kB permanece; não substitui benchmark do fluxo.
- Closure inicial: sem findings de código, oito budgets **0/0**; somente os
  dois READMEs precisaram do renderer oficial. Matriz: **7.497 imports Core,
  1.239 Community→Core, 25 dependências**. `closure-method-final.json`: **exit 0,
  ok=true**, zero findings de código/documentação, oito budgets **0/0**;
  distribuição, conformance, singleton e AF35 aprovados.

Limites e retomada: somente automated_test tem admissão concreta neste incremento.
Static analysis/inspection/demonstration precisam de caminhos autenticados reais;
enum ou checklist não são prova. Integrar capacidade/método e atribuição de trabalho
ao resolver de requisitos, mínimos por autoridade e fallback de escopo; depois
compartilhar inventário/contribuição por Card e integrar adoção ARQ/VER e gates
writer/preview. P1 integrado, P2 completo, P3, F2B, KG, migração/rollback,
E2E/Grafx e benchmarks seguem pendentes. O objetivo consolidado permanece ativo.

### 2026-09-19 — resolução de método e associação a Test Cards (P2 parcial)

Par enviado e confirmado em `origin/feature/v0.4.0`: Core
**ca45ccd15ae80897fb1709b803c414003d7bc655**, Community
**3eba6e880be8c2675094a6c86718986809299784**. Árvores de implementação limpas.

- Turno anterior classificado como progresso: par 4461ced0/75952f3 publicado,
  provas e closure registrados acima. Partida atual: árvores limpas e sem blocker.
- Resolver relacional de plano, compartilhando os caminhos
  de qualificação existentes. Observação GWT, método admitido pela porta concreta
  e Test Cards vivos no mesmo board/Spec; nenhum resultado passing é exigido.
  Todos os cenários declarados para o critério contam, sem alternativa implícita.
- Leitor REST/MCP calcula a população inteira antes de paginar. Cenários/cards
  só são consultados após spec.tests.read + card.entity.read; sem essas flags,
  qualificação autorizada permanece disponível e planejamento fica indisponível.
- UI recebe resumo global e detalhes limitados de cenários/cards. Perder autoridade
  de leitura de cenários/Card desmonta o painel e elimina os detalhes carregados;
  respostas com planejamento restrito não escondem a qualificação autorizada.
- Populações inválidas, desconhecidas, duplicadas e limites excedidos ficam
  indisponíveis. IDs são exatos, sem matching por índice/texto. Cards cancelados,
  arquivados, de outro escopo ou de tipo não Test não satisfazem a associação.
  Critérios inativos seguem a mesma exclusão da resolução de requisitos; seus
  cenários históricos não obrigam trabalho novo e não são apagados.
- `methods_evaluated` e `verification_work_evaluated` indicam a avaliação dessa
  estrutura. `method_plan_complete` e `verification_work_complete` são fatos de
  planejamento declarado; execution/semantic_review/delivery/rollout continuam
  não avaliados. Não são autorização para iniciar nem evidência de entrega.
- Resolução inteira antes da paginação, com 5.000 nós por população e 4.096
  vínculos/expansões; resumo limita 20 cenários por caminho e 20 Cards por cenário,
  com contagens/truncamento explícitos. As pendências omitidas continuam afetando
  o resultado global. A consulta privilegiada acrescenta uma leitura relacional
  limitada de Cards; ainda não há benchmark do fluxo completo.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-plan-final.json`, par `wheels-plan-final`: **790/311 .py,
  855/395 payloads**, source/wheel/install byte a byte. Processos novos e
  PYTHONPATH pareado; nenhuma reinicialização do runtime do usuário.
- `core-plan-final.log`: **109 passed em 18,88 s**: plano sem execução prévia,
  métodos/capacidade ausentes, todos os cenários declarados obrigatórios,
  Test Card removido/cancelado/arquivado/fora do escopo, herança com seleção
  limitada, pendência fora da página/resumo, limites/duplicidade e quatro
  estados de critério inativo, além das regressões de qualificação/métodos e
  contratos/catálogos. O lote inicial de 105 passou; os quatro casos de histórico
  foram adicionados após identificar a exclusão ativa faltante na revisão.
- `community-plan-final.log`: **9 passed em 43,56 s**, SQL descartável real,
  REST/FastMCP materializado com e sem autoridade de planejamento, capacidade
  do verifier Community real, contagem global com TR pendente fora da página,
  ausência de writes e de SELECT dos corpos protegidos quando leitura é negada.
- `frontend-plan-final.log`: **48 passed em 25,08 s**: 20 do painel de qualificação,
  24 de SpecModal.activity e quatro de structuredEditing. Resumo global não é
  inferido da página; métodos sem suporte, Test Cards ausentes, indisponibilidade,
  truncamento e perda de autoridade permanecem explícitos.
- Typecheck/build, ESLint dos módulos alterados, Ruff e diff-check aprovados.
  `frontend-plan-dist.log`: **78 arquivos**, SHA256
  `6e4427fcd6cdc4f3eb141dacc2031074c69c061fd66ee225e960bc73a3588b8d`.
  Aviso de chunks >500 kB permanece. Catálogos/manifests só via generators oficiais.
- Closure inicial sem findings de código e com oito budgets zero; README requeria
  atualização de matriz. Após a correção de histórico, par e testes foram refeitos;
  renderer oficial aplicado aos READMEs. `closure-plan-final.json`: **exit 0,
  ok=true**, sem findings de código/documentação e oito budgets **0/0**.
  Matriz: **7.504 imports Core / 1.239 Community→Core / 25 dependências**;
  distribuição, conformance, singleton e AF35 aprovados.

Retomada investigada: `DefaultDeliveryInventoryPolicy`/`card_delivery_inventory`
em `domain/delivery_inventory.py` ainda selecionam somente `linked_task_ids` e
fallback por título de Card. A porta pública `DeliveryInventoryPolicy` já existe;
não criar seleção paralela no adapter. Evoluir contribuição aprovada por Card e
resolução direta/herdada compartilhada antes de reduzir esses links manuais.

- Isto não resolve contribuição de implementação, dependências, mínimos de policy,
  escopo sem requisitos, adoção ou integração de gates; pendências anteriores mantidas.

### 2026-09-19 — declaração tipada e responsabilidade por contribuição (P2/P3 parcial)

Par enviado e confirmado por `ls-remote` em `origin/feature/v0.4.0`: Core
**f08aacf1e9be4374ff045381ddd3ed8ae5f5d1f4**, Community
**ed65c2c511c744bb9ca08ac72ef551192643fc52**. Árvores de implementação limpas.
Credencial ativa autenticada; pushes concluídos sem alterar contas/permissões.

- Turno anterior: progresso comprovado, par ca45ccd1/3eba6e8 publicado; partida
  atual limpa. A inspeção confirmou que linked_task_ids pode mudar sem revisar
  corpo aprovado; vínculo operacional sozinho não é declaração de divisão.
- Novo implementation_plan opcional nos cinco requisitos, sob writer/lock de
  conteúdo existente: contributions anotam linked_task_ids, com Card exato,
  whole_requirement ou selected_criteria e resumo obrigatório para parte selecionada.
  Schema fechado, até 50 contribuições, 100 critérios por parte e 32 KiB agregado;
  nenhum approved/complete/trusted do cliente é aceito. Ausência histórica não
  é preenchida. Writers novos/alterados devem conferir Card normal/bug vivo no
  mesmo board/Spec e IDs exatos de critérios; plano inalterado pode ficar pendente
  após remoção/cancelamento de dependência, preservando sua declaração histórica.
- Resolver tipado na política pública DeliveryInventoryPolicy: escopos diretos
  prevalecem; BR sem alocação direta reutiliza contribuição FR inequívoca pelos
  critérios selecionados ou um único responsável explícito pelo FR inteiro.
  Sobreposição ambígua fica pendente, sem atribuir BR a todos os Cards.
- Hash de contribuição vincula requisito, critérios e fontes herdadas, separado
  do digest da definição usada pela herança de verificação. Alterar somente o
  escopo de um Card não deve invalidar o do outro. Isso ainda não é prova de execução.
- Leitor/REST/MCP e UI recebem declaração/proveniência e pendências limitadas.
  Autoria disponível pelo structured writer; editor dedicado de contribuição
  ainda pendente. Validação e publicação do incremento concluídas conforme abaixo.
- Não houve cutover do ledger legado: card_obligations e seus hashes/rollup
  permanecem na compatibilidade antiga até integrar binding de contribuição,
  partial/complete e adoção autorizada. Redução de links manuais e gates continuam
  pendentes. A porta nova concentra a resolução futura; adapters não a duplicam.

Evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-contribution-final.json`, par `wheels-contribution-final`:
  **792/311 .py e 857/395 payloads**, source/wheel/install idênticos byte a byte.
  Processos de teste novos com PYTHONPATH pareado e dados descartáveis;
  runtime do usuário preservado. As alterações posteriores foram em testes,
  ledger e matriz README, sem mudar o payload validado.
- `core-contribution-final.log`: 128 passed e uma falha de expectativa do teste
  de lock. O writer recusou corretamente a edição fora de Draft por
  `SubjectEditRequiresDraftError`; o teste esperava retorno de erro. Corrigida
  somente essa expectativa e acrescentados dois casos de digest/população.
  `core-contribution-regression.log`: **24 passed em 2,16 s**. São **131 casos
  distintos aprovados** entre os dois lotes, sem apresentar o lote inicial
  como execução inteiramente verde.
- Casos de contribuição: schema fechado; vínculo operacional insuficiente;
  autoria nos cinco tipos; Card ausente/Test/fora do escopo; escrita inválida
  sem versão/histórico novo; lock; impacto reverso de AC; herança BR→FR limitada
  ao responsável correto; ambiguidade; alocação direta legítima; alteração de
  escopo/critério invalidando só as contribuições dependentes; notas editoriais
  sem invalidação; população indisponível/excedida sem resultado completo.
- `community-contribution-final.log`: **35 passed em 106,72 s**. Inclui paridade
  REST/FastMCP sobre SQL descartável, população completa antes da página,
  permissões sem leitura dos corpos negados e regressões de Delivery Evidence.
  Não equivale a E2E do runtime instalado com Grafx.
- `frontend-contribution-final.log`: **51 passed em 29,98 s**: 23 do painel,
  24 de SpecModal.activity e quatro de structuredEditing. Origem da contribuição,
  Card correto, ambiguidade, truncamento e ausência de autoridade estão cobertos.
- Typecheck/build, ESLint dos módulos alterados, Ruff e diff-check aprovados.
  `frontend-contribution-dist.log`: **78 arquivos**, SHA256
  `9c80796b931024c4e3a2b94e2082fd026e05b9b0db2bd522e224c7980fb573de`.
  Aviso de chunks >500 kB permanece; não é benchmark. Generators oficiais
  executados; catálogo/manifests sem edição manual.
- Closure inicial: zero findings de código e oito budgets **0/0**; somente
  matriz README divergente. Renderer oficial aplicado: **7.522 imports Core,
  1.239 Community→Core, 25 dependências**. `closure-contribution-final.json`:
  **exit 0, ok=true**, zero findings de código/documentação, oito budgets **0/0**.

Retomada: a resolução publicada aqui cobre os cinco tipos de requisito
qualificado (`implementation_scope=qualified_requirements`), não o inventário
integral de API Contracts/Decisions/fallback de escopo. A declaração não é
aprovação semântica, execução ou autorização de início. Integrar o binding
versionado por contribuição, partial/complete, admissão e rollup canônico junto
à adoção ARQ/VER; completar o editor de autoria e o inventário integral antes
de reduzir links manuais. P1 integrado, P2/P3 completos, F2B, KG, migração,
rollback, E2E/Grafx e benchmarks continuam pendentes; objetivo consolidado ativo.

### 2026-09-19 — validação do progresso canônico e migração

Par confirmado por `ls-remote` em `origin/feature/v0.4.0`: Core
**14895c5c1a7c27900f8a58dff8b648273f1e12d4**, Community
**c5c55e30fea626d1f74fd1f63b96b25d9a6db695**. Árvores de implementação limpas.

- `provenance-progress-final.json`, par `wheels-progress-final`: **793/312 .py,
  858/396 payloads**, source/wheel/install byte a byte. Reconstrução final após
  ajuste de ordem da migração, documentação e proteção de resposta tardia na UI.
  Processos novos, PYTHONPATH pareado, bancos descartáveis; runtime real intacto.
- `core-progress-final.log`: **97 passed em 8,60 s**. Contrato fechado, progresso
  dirty sem receipt/commit, recusa de autoridade falsa/shape incompatível, limites
  de bytes e paths de impacto, serialização legada preservada, freeze e permissões
  reais (board.read, execução de Target ou teste não concedem relato), regressões
  de Delivery e contratos/manifests/catálogo.
- `community-progress-final.log`: **62 passed / 4 failed** inicialmente. As falhas
  identificaram ordem divergente entre registry e ledger de migração, contagem
  explícita antiga (74→75), lista de passos com reconstrução e expectativas de
  no-op em base nova. Ordem corrigida; testes passam a nomear a etapa concreta,
  sem remover assertions ou ampliar budgets arquiteturais.
- `community-progress-regression.log`: **42 passed em 63,27 s**, incluindo 30
  testes do migrador e 12 de progresso. `community-progress-rollback.log`:
  **13 passed em 30,30 s**, acrescentando falha injetada entre DROP e RENAME:
  transação restaura tabela, todas as linhas, três triggers e remove a tabela
  temporária; reexecução converge e é idempotente. Junto aos 26 testes de Delivery
  integration aprovados no primeiro lote, são **69 casos distintos aprovados**.
- SQL real: identidade/autoria persistida após fechar a sessão, sem avanço de
  policy_version/Spec.version, nenhum crédito de implementação/teste, estados
  congelados recusados sem registros, refs fora de escopo, resumo de 23 fatos
  limitado a 20 com truncamento e revogação preservada. REST→MCP reaproveita o
  mesmo registro/chave e rejeita campos falsos. São ensaios ASGI/handler e SQL,
  não E2E do Pulse instalado com Grafx nem rollback completo de release.
- `frontend-progress-final.log`: **81 passed em 19,48 s** em quatro arquivos,
  incluindo CardModal, painel DoD, painel de evidência e novo painel de progresso.
  Cobertura de dirty, reload com vários fatos, histórico incompleto, status,
  perda de permissão, retry com mesma chave, clique duplo e resposta de Card antigo.
- Build/typecheck, ESLint dos novos módulos, Ruff e diff-check aprovados.
  `frontend-progress-dist.log`: **78 arquivos**, SHA256
  `659e2cfadcd65de62de663972b38e8cda0a749b136d2f98fa50a9941df954c6b`.
  Avisos preexistentes de chunks e Browserslist não foram tratados como benchmark.
  Resources explicam progresso como claim, autoridade, limites e recuperação;
  catálogo/manifests passaram exclusivamente pelos generators oficiais.
- `closure-progress-final.json`: **exit 0, ok=true**, sem findings de código ou
  documentação, oito budgets **0/0**. READMEs via renderer oficial: **7.526 imports
  Core / 1.240 Community→Core / 25 dependências**. Sem mecanismo concreto novo
  no Core, reach-in privado ou projeção de checkpoint no KG.

Implantação/rollback: etapa integrada ao migrador existente, testada somente em
cópia descartável. Exige par de binários/dados consistente; o teste de rollback
transacional não autoriza instalar binário antigo sobre writes progress novos.
Rollback de release/backup e preservação dos writes posteriores ainda precisam
do ensaio integrado. Nenhuma migration real, release ou tag executada.

Próximo incremento: contrato único de entries/batch e revisão de seleção no mesmo
ledger; composição de receipt/binding com autoridade de origem; inventário completo
e contribuição versionada/admissão antes do cutover dos gates. Completar leitura
de retomada por Card com paginação/detalhe e autoridade básica, impacto acumulado
e partial/complete. P1/P2/P3 completos, F2B, KG, migrações integradas, E2E/Grafx e
benchmarks seguem pendentes; não considerar este incremento conclusão da iniciativa.

### Validado — lote atômico na superfície Delivery existente

Turno anterior: progresso, par 14895c5c/c5c55e3 e ledger 0da59319 publicados.
Partida atual: duas árvores limpas. Releitura de DEI §5 confirmou um único Card,
board, Spec/edição e ator por lote, all-of antes de writes, erro atômico e replay
do envelope completo. O inventário/contribuição e admissão prospectiva continuam
dependências do cutover; este lote reaproveita a admissão existente, sem fabricar
partial/complete ou antecipar crédito de Test Card.

- `card-delivery-batch/v1`: 1–50 entries, client_ref único, fences de Card/edição
  mais expected_delivery_revision. Limite agregado 128 KiB e 200 referências
  (contando usos em obrigações/implementações/Targets, não multiplicando tetos
  individuais). Progress/implementation/test usam os mesmos contratos tipados;
  waiver/revoke não entram no lote do executor.
- A revisão é a contagem dos registros imutáveis no escopo Card/Spec/edição,
  incluindo registros legados e revogações, calculada sob fence do board. Não
  altera Card.policy_version nem numera novamente histórico. Uma escrita legada
  também faz o próximo lote com revisão antiga conflitar.
- Recibo de lote na primeira entrada append-only, com digest do envelope,
  client_ref→ID e revisão aceita; demais entradas apontam a essa primeira.
  Nenhuma tabela/ledger paralelo, migration nova ou update de registro existente.
  Savepoint envolve todos os appends e protege inclusive caller que captura
  a exceção e faz commit externo. Replay confere escopo, ator e conjunto completo.
- Core autoriza todos os tipos antes de chamar o adapter; aplica tipo de Card
  e freeze sem emprestar autoridade por agrupamento. Done mantém a associação
  autorizada de prova existente; progress continua exigindo execução. Erro de
  entrada expõe somente índice/client_ref do chamador e código de domínio.
- REST/MCP usam o mesmo parser e use case. UI de progresso envia entries de
  tamanho um com revisão real; sem revisão disponível não inventa zero.
- Ainda pendentes: proof inline, aliases que referenciam criações locais, seleção
  final, partial/complete, inventário completo/adoção, retomada paginada e métricas.
  Não confundir este incremento com conclusão da iniciativa.

Validação concluída em 2026-09-19, em ambiente descartável:

- `provenance-batch-final.json` e reconfirmação `provenance-batch-resume.json`
  em `PULSE_REFACTOR/.validation-v040`: **793 Core / 312 Community .py** com
  conjuntos e bytes idênticos; payload source→wheel→install **858 / 396**,
  sem divergências. Wheels SHA256 Core
  `797d200234e709c26ac408ae760ed5366b41029be320eeeffcd509a7ddc64aab` e Community
  `1336ee5f77ce375a68c8edd7e74b77f547d899333d6e882a07164a601afed90c`.
  Testes em processos novos, com PYTHONPATH pareado; runtime ativo não alterado.
- `core-batch-final.log`: **116 passed em 9,13 s**. Contrato fechado, limites
  agregados, tipo de Card, autorização all-of antes do adapter, replay autorizado,
  compatibilidade de payload legado, domínio/lifecycle, catálogo e manifests.
- `community-batch-final.log`: **52 passed em 87,37 s**. Lote inteiro persistido
  ou revertido, inclusive commit externo após erro na segunda entrada; dois
  concorrentes na mesma revisão produzem só um sucesso; append legado invalida
  revisão antiga; chave/envelope divergentes conflitam; prova real de implementação
  e resultado autenticado de Test Card usam a admissão existente. REST→MCP
  reaproveita IDs/revisão do mesmo lote; erro identifica entrada sem conceder crédito.
- `frontend-batch-final.log`: **83 passed em 21,69 s**, quatro arquivos
  (CardProgressPanel, CardDeliveryDoDPanel, DeliveryEvidencePanel, CardModal).
  Sem revisão não grava; revisão zero é válida; retry/clique duplo/Card antigo,
  permissão e estados continuam cobertos. Build/typecheck e verificação de dist
  passaram: **78 arquivos**, SHA256
  `e5d89652273c63a85ebcbbf5c0688a2c6a8653a2f23c86e7f5f84f7ff9f7319f`.
- `closure-batch-initial.json`: **exit 0, ok=true**, findings de código e
  documentação vazios; **oito budgets 0/0**. Mantidos 7.526 imports Core,
  1.240 Community→Core e 25 dependências. README já coincide com o renderer.
  Catálogo/manifests gerados somente pelos generators oficiais; Ruff, ESLint
  dos módulos de progresso e diff-check aprovados.

Limites: estes são testes de domínio/SQL/ASGI/handler e frontend; não constituem
E2E do runtime instalado com Grafx, benchmark ou rollback integrado de release.
Não há migration nova neste incremento. O lote reutiliza verificações por entrada;
custo das consultas e da projeção de revisão ainda precisa de medição. Próximo
trabalho continua na composição de receipt/binding na mesma UoW, inventário e
contribuição versionada, adoção/gates e retomada completa; F2B e métodos de prova
restantes mantêm as dependências já registradas. Nenhum gate/histórico foi relaxado.

Par publicado em `feature/v0.4.0`, push normal e HEADs remotos conferidos:
Core `0fb9bb0f66ed7bc2a01c36222c77fbf5a8752be3`; Community
`3e2dde3a4463ffab9e8d30a8e4fd6b2f80c18cd4`. Árvores limpas após os commits
funcionais; este apontamento é o follow-up documental. Autenticação ativa de
`oktolabsai-developer` validada pelos pushes, sem alterar contas ou permissões.
Estado da iniciativa: **progresso**, sem bloqueio externo; escopo consolidado
ainda incompleto. Retomar das dependências acima, sem refazer este lote validado.

### Validado — composição inline da execução de Target com Delivery

Turno anterior classificado como progresso (par de lote publicado e 251 testes).
Árvores limpas na partida. DEI §5.5–5.7 exige composição pelo serviço de origem;
investigação confirmou que `SubmitImplementationTargetExecutionUseCase.execute`
fazia commit interno. Separada a operação pública `execute_in_transaction`, com
autorização/allowlist/ator novamente conferidos antes do adapter, inclusive replay.
Standalone mantém seu commit; o composto controla o único commit.

`execution_submission` reutiliza os campos e validadores tipados de origem,
exclusivo com execution_id. Escopo/ator/chave/justificativa vêm do envelope.
Callback Protocol fornecido pelo Core executa admissão original no mesmo UoW;
Community inclui recibo, binding e outbox no savepoint (fence antes do savepoint
também no caso único). Prova persistida contém apenas execução canônica, sem
duplicar os campos técnicos; response devolve execution_id, preservado no replay.
Investigação autenticada/challenge continua como pré-requisito, não é fabricada.
Fixtures SQL com origem real e evento/handler validadas conforme evidências abaixo.
Sem mudança no frontend neste incremento; formulário inline ainda precisa integrar
o fluxo final. Aliases, contribuição/seleção, retomada completa e gates continuam
nas dependências registradas, não substituídos por esta composição.

Validação 2026-09-19 em `.validation-v040`:

- `provenance-inline-final.json`: **793/312 .py** e **858/396 payloads** idênticos
  entre source, wheel e install, sem mismatches. SHA256 dos wheels Core
  `b059ae4b91cb2c0308a57b7ef7a58668b294537dff0d75e0c2ecde7c5e1a0b87`, Community
  `62ffbdc9af1ecae877a9c406de1cee1dde17f4b5a18babd205af6c4aa3c71b53`.
  Processos de teste novos, PYTHONPATH pareado, dados descartáveis.
- `core-inline-final.log`: **108 passed em 8,78 s**, contrato inline exclusivo,
  autoridade/escopo fechados, validação de path/disposition compartilhada, limite
  de 200 referências incluindo Target/receipt no caso único, compatibilidade de
  digests legados, serviço de origem, replay sequencial e catálogo/manifests.
- `community-inline-final.log`: **63 passed em 119,00 s**. Onze casos novos
  comprovam origem real com SQL, único commit, retorno dos mesmos IDs, rollback
  de ExecutionRecord + Delivery + DomainEvent + handler após erro posterior,
  rollback externo no caso único, binding inválido sem receipt órfão, origem
  inválida sem promoção para progresso, humano recusado e allowlist revalidada
  no replay. Standalone preserva commit/replay. REST→MCP usa a composição real;
  somente o authorizer do teste de mapeamento de transportes é substituído,
  enquanto os testes de autoridade anteriores usam o authorizer real.
- Tentativas anteriores preservadas nos logs: inicialmente **52 passed/6 errors**
  por fixture trocar selector antes do digest; depois **5 failed/5 passed** por
  revisão fictícia `revision-1` não ser commit aceito pelo Delivery; depois
  **9 passed/1 failed** por campos obrigatórios ausentes no progresso da segunda
  entrada. Corrigidas somente fixtures (digest atômico, hash de commit e estado
  explícito). Nenhuma validação de produção foi relaxada para fazê-las passar.
- `frontend-inline.log`: **83 passed em 27,45 s** nos quatro arquivos do fluxo
  Card/Delivery/progresso. Sem modificação da SPA neste incremento; formulário
  inline permanece pendente no fluxo final. Dist anterior preservado.
- `closure-inline-final.json`: **exit 0, ok=true**, zero findings de código e
  documentação; **oito budgets 0/0**. Renderer oficial atualizou os READMEs para
  **7.530 imports Core / 1.242 Community→Core / 25 dependências**. Ruff/diff-check
  aprovados; catálogo/manifests somente por generators oficiais.

Sem migration nova, alteração de status, crédito parcial inventado, exceção de
arquitetura, release/tag ou ação sobre dados/runtime reais. E2E instalado/Grafx,
benchmarks, rollback integrado e conclusão requisito a requisito continuam pendentes.
Próxima dependência: aliases locais de referência e contribuição/seleção versionada,
conectadas ao inventário completo e adoção ARQ/VER; completar UI/retomada com essa
mesma autoridade, sem criar writer ou ledger paralelo. Estado: **progresso**.

Par inline publicado por push normal em `feature/v0.4.0`: Core
`85d11e9c462b13a320e57507a5689bbfe9a5e5fd`, Community
`000506a42b948312f5c8a0b4e519040d6768280d`. HEADs remotos iguais aos locais e
árvores limpas após commits funcionais; este follow-up registra o checkpoint.

### Validado — referências locais e vínculo ao progresso histórico

Turno anterior: progresso publicado (par inline 85d11e9c/000506a). Árvores limpas
na partida. Releitura DEI §5.1–5.7: batch permanece um único Card/Spec/edição/ator;
alias é referência local, não criação ou autorização. `execution_client_ref`
reutiliza execução admitida de entrada implementation anterior, exclusivo com
execution_id/execution_submission. `progress_refs` tem identidade fechada por
record_id persistido ou client_ref anterior de progress. Core valida tipos,
unicidade/ordem/limites e resolve IDs; Community verifica existência no exato
escopo e persiste identidades canônicas dentro do mesmo savepoint.

Referência a progresso é histórica, inclusive se a origem depois for revogada;
não é seleção, supersession, contribuição completa ou crédito de prova. Aliases
não entram no caso único e não alcançam outro lote/Card. Envelopes antigos omitem
os campos novos vazios no digest. Preparados testes de replay em sessão esvaziada,
uma execução/evento para vários bindings, falha posterior com rollback integral,
progress real de outro Card e tipo de registro incorreto. Validação abaixo.
Contribuição versionada, UI/retomada completa e adoção/gates continuam pendentes.

Evidências 2026-09-19, ambiente descartável `.validation-v040`:

- `provenance-localrefs.json`: **793/312 arquivos .py**, conjuntos e bytes exatos;
  payloads source→wheel→install **858/396**, sem mismatches. SHA256 dos wheels:
  Core `5e9cfcd265b792ff8fa25b2364771bce356e4a0aecf9f5b93c2c71d921f540b6`;
  Community `31f19da3db0770bfc7847e8d28c52fd3d6fd4b03342c4a3abc3cf868625b6a82`.
  Testes em processos novos com PYTHONPATH pareado; runtime real preservado.
- `core-localrefs.log`: **140 passed em 12,34 s**. Referências tipadas anteriores,
  inexistentes/adiantadas/autorreferentes/tipo errado recusadas, limites agregados,
  exclusividade de origem de execução, caso único sem aliases, compatibilidade
  de digests, domínio/lifecycle e catálogo/manifests.
- `community-localrefs.log`: **68 passed em 127,48 s**. Cinco casos novos com
  SQL e serviço de origem reais: uma execução/evento para múltiplos bindings,
  cadeia de aliases, progress_refs canônicos, replay após fechar a sessão,
  rollback integral no erro posterior, progresso real de outro Card recusado,
  registro implementation não confundido com progress e permissão de execução
  exigida antes de qualquer escrita. Sem falhas nesta rodada.
- `frontend-localrefs.log`: **83 passed em 26,84 s**, quatro arquivos do fluxo
  Card/Delivery/progresso. Sem alterações de UI/dist neste incremento.
- `closure-localrefs.json`: **exit 0, ok=true**, findings de código/documentação
  vazios; oito budgets **0/0**, 7.530 imports Core, 1.242 Community→Core,
  25 dependências. README já coincide com o renderer; generators oficiais,
  Ruff e diff-check aprovados.

Reconfirmação da próxima dependência: `DeliveryBinding` ainda sela somente
obligation_ref/semantic_sha256. `domain/delivery_inventory.py` preserva os oito
conjuntos legados (incluindo AC/API/Decision e fallback); já existe resolução
tipada de responsabilidade em `domain/implementation_responsibility.py`, com
scope_sha256 por contribuição/Card, mas sem crédito de execução. Integrar a
versão nova de binding/seleção com o inventário completo e a adoção ARQ/VER;
não substituir os oito conjuntos pelas cinco famílias qualificadas e perder
obrigações. Referências históricas adicionadas aqui não realizam esse cutover.
Sem migration nova, benchmark, E2E/Grafx, release/tag ou mudança de gates/histórico.
Estado da iniciativa: **progresso**, escopo completo ainda não concluído.

Par de referências locais publicado por push normal em `feature/v0.4.0`:
Core `cc5d025977b8a39fbd926691c555d0c54a50d153`; Community
`f8e3be731719833bd7e12e04c80642a7db9d652d`. HEADs remotos conferidos iguais
aos locais; árvores limpas após os commits funcionais.

### Validado — inventário efetivo completo para planejamento e futura adoção

Turno anterior: progresso (referências locais publicadas). Árvores limpas na
partida. Releitura ARQ/VER §4.2/§5/§11: centralizar a população sem eliminar
obrigações e preservar o contrato aprovado até adoção explícita. O inventário
legado tem oito conjuntos e fallback; a responsabilidade tipada qualificava cinco.

Nova política pública `effective_inventory` resolve responsabilidade qualificada
uma vez e agrega AC/API/Decision e escopo dos Cards sem vínculos. Mantém todas as
obrigações observadas, inclusive sem responsável. Não sintetiza divisão entre
vários Cards suplementares. Definição qualificada e scope_sha256 por contribuição
permanecem separados, para não invalidar outro Card por alteração só de alocação.
Fallback prospectivo card-delivery-scope/v2 inclui title/description/details;
F11 já havia sido reproduzido no ledger. Nenhum digest selado legado é reescrito.

Reader de qualificação usa essa mesma resolução e inclui resumo global delimitado
effective_inventory antes de paginar requisitos. Campos suplementares e conteúdo
de Card só são carregados com a autoridade de planejamento já exigida. UI distingue
população completa, pendências e indisponibilidade, sem inferir zero ou crédito.
Gate/admissão/rollup ativos permanecem no contrato atual até adoção/cutover integrado;
esta é uma dependência concreta, não conclusão de RF-INT-01.

Validação em 2026-09-19 (`.validation-v040`):

- `provenance-inventory-final.json`: **794 Core / 312 Community .py**, conjuntos
  e bytes exatos; **859/396 payloads** source→wheel→install, sem mismatches.
  Wheels SHA256 Core `b5b4b261373cd887e7b0b450f0a9f795faf068596d7f05beae8ad8a5a4e88593`,
  Community `3bca3fdfc5485df15ab84473b0f628ae2241af6bc686a400fb18786f7b8ba963`.
  Processos novos, PYTHONPATH pareado; nenhum runtime real alterado.
- `core-inventory-final.log`: **90 passed em 10,68 s**. Preservação de AC/API/
  Decision, herança BR→Card exata, alocação suplementar ambígua permanece pendente
  para os Cards vinculados, limites/duplicidade/ausência não viram zero, mudanças
  normativas do fallback prospectivo alteram o digest sem reescrever o legado.
  Definição de requisito e contribuição de outro Card permanecem estáveis sob
  alteração de alocação não relacionada. Catálogo/manifests também aprovados.
- `community-inventory.log`: **36 passed em 90,91 s** (10 leitura + 26 regressão
  Delivery). Após o ajuste final da projeção por Card e digest global, os dez
  testes de leitura foram repetidos: `community-inventory-final.log`, **10 passed
  em 33,72 s**. SQL real comprova extras/escopo genérico fora da primeira página,
  sem carregar cenários/Cards/API/Decision quando falta autoridade de planejamento.
  REST/MCP continuam usando o mesmo reader e contrato de qualificação.
- `frontend-inventory.log`: **108 passed em 31,40 s**, cinco arquivos incluindo
  RequirementVerificationPanel. Novos testes distinguem escopo completo pendente
  de requisitos qualificados alocados, desconhecido de zero e ocultação por perda
  de permissão. Build/typecheck e ESLint aprovados; dist verificado, **78 arquivos**,
  SHA256 `372e37ed677d0bb1eb2358c5ea2df205a5c865e3d126d610f4f456009e78f729`.
- `closure-inventory-final.json`: **exit 0, ok=true**, zero findings de código e
  documentação, oito budgets **0/0**. READMEs atualizados via renderer oficial:
  **7.539 imports Core / 1.242 Community→Core / 25 dependências**. Ruff e diff-check
  aprovados. Sem falha de teste; primeira auditoria pediu apenas atualização dos
  READMEs para as novas contagens.

Próximo passo: integrar bindings de contribuição/seleção e a adoção explícita do
contrato a este inventário, fazendo admissão, gate e rollup consumirem a mesma
resolução. A projeção de planejamento entregue ainda não muda o crédito ativo.
Divisão de AC/API/Decision entre vários Cards sem plano suficiente continua
pendente, não recebe ownership fabricado. Sem migration nova, benchmark, E2E/Grafx
ou release/tag. Estado: **progresso**; iniciativa completa permanece em andamento.

Par de inventário publicado por push normal em `feature/v0.4.0`: Core
`9e55f2b373eb9fef0d0678cd17116a93bfa8540e`, Community
`31c63b1cc5d6e0d814d9fefb89709a4ff0331225`. HEADs remotos iguais aos locais,
árvores limpas após os commits funcionais; este follow-up registra a retomada.

### Validado — declaração parcial/completa por binding

Autenticação ativa e HEADs remotos reconfirmados: Core `874ce3c1`, Community
`31c63b1`; nada pendente de push na partida. Investigação da adoção conjunta:
`architecture_adoption` governa seleção de Designs, não o contrato ARQ/VER.
`move_spec` e `allowed_transitions` preservam os gates existentes. Antes de
ativar o contrato novo, Delivery precisa distinguir contribuição parcial de
completa: hoje `require_card_delivery` aceita qualquer binding com execução
atual, inclusive antes de Done. Esta é a próxima dependência implementada.

Escopo deste incremento: declaração tipada por obrigação no writer canônico,
persistência no mesmo Card ledger, distinção no avaliador/DoD/rollup e UI,
mantendo recibos, autorização, replay e legado sem declaração. Ausência histórica
continua identificável como legado; não converter registros antigos para
`complete`. Não ativa adoção ARQ/VER nem redefine ownership: composição de
múltiplos Targets, escopos tipados, seleção final e cutover permanecem dependências
explícitas. DEI §4.3/§11 e DEI-T14/T15/T56 orientam os testes.

Implementado: `bindings` fechado substitui `obligation_refs` somente em
implementation, com estado `partial|complete` por obrigação; não recebe hashes,
ator nem flags de prova. Lote contabiliza as referências novas no mesmo limite.
Serializer omite o campo ausente para preservar os digests de replay legados.
Community resolve os hashes e guarda `card-binding-contribution/v1` e as
declarações no payload imutável existente, junto ao recibo canônico. Um parser
de domínio recusa formato novo incompleto/corrompido em vez de tratá-lo como legado.
O predicado de completude é compartilhado pelo avaliador e pelo DoD pré-Done.
`partial` não satisfaz implementação nem o join de teste; duas entradas parciais
não se somam. `complete` continua sujeito à cadeia atual, tipo/estado e avaliações.
Registros anteriores não são sobrescritos nem implicitamente revogados.

UI do Card declara cada obrigação separadamente (default visível partial),
apresenta checkpoint parcial sem o check de implementação e identifica recibos
legados sem declaração. Test Cards e waivers mantêm contratos próprios. REST e
MCP compartilham o schema e a composição original de execução/ledger/outbox.

Validação em 2026-09-19 (`.validation-v040`), sem alterar o Pulse ativo:

- `provenance-contributions.json`: **794 Core / 312 Community .py**, conjuntos
  e bytes idênticos antes dos testes; **859/396 payloads** source→wheel→install,
  sem mismatches. Wheels SHA256 Core
  `3064dca4c49b770bebd292ed0dfe75ca47ced6f38b6721562d824fb8f9debfa1`;
  Community `95769a0f7df68b164d1894bfd316c30f80b98788560eb7a5fbfa2840641cde1c`.
  Testes em processos novos, PYTHONPATH pareado e dados descartáveis.
- `core-contributions.log`: **158 passed em 13,59 s**. Estados distintos no
  mesmo recibo, duas parciais sem crédito, complete sem dispensar prova/lifecycle,
  legado preservado, schema fechado, limites agregados, payload novo corrompido
  recusado e gate pré-Done; regressões batch/aliases/inline/catálogo/manifests.
- `community-contributions.log`: **72 passed em 149,93 s**. Quatro testes novos
  com SQL e serviço real de origem: FR parcial/TR completa, DoD/rollup concordam,
  replay após fechar sessão, mudança de declaração conflita, declaração complete
  posterior preserva as duas parciais, autorização, escopo e rollback de quatro
  tabelas. `community-contributions-transports.log`: **2 passed em 13,60 s**,
  REST→MCP replay com o contrato legado e o novo. Total **73 casos distintos**
  (o caso legado de transporte foi repetido após parametrizar o teste).
- Frontend: **86 passed** em quatro arquivos. `frontend-contributions.log`
  **38 em 32,39 s** (DoD/progresso/rollup) e
  `frontend-contributions-modal.log` **48 em 30,27 s** (CardModal). Testes novos
  de declaração independente, leitura parcial sem crédito e origem legacy.
  Build/typecheck, ESLint e dist aprovados: **78 arquivos**, tree SHA256
  `8245fd3e000b2171bff2d8262b8104b2543931f57d4951651bd8ca8f5815cee0`.
- `closure-contributions-final.json`: **exit 0, ok=true**, nenhum finding de
  código/documentação, oito budgets **0/0**; **7.540 imports Core / 1.242
  Community→Core / 25 dependências**. READMEs via renderer oficial; catálogo e
  manifests pelos generators oficiais. Ruff e diff-check aprovados.

Nenhuma falha de comportamento nas rodadas. Ajustes de verificação: Ruff
identificou reexports históricos sem alias explícito, agora declarados como tal;
uma chamada ESLint foi feita na raiz errada e repetida com o binário local do
frontend; o filtro inicial de CardModal tinha caminho incorreto e sua suíte foi
executada separadamente no caminho real. A primeira closure pediu apenas as
novas contagens dos READMEs; nenhum budget foi alterado.

Retomada: o contrato de adoção ARQ/VER ainda deve ser distinto de seleção de
Designs. Integrar versão/escopo de contribuição do inventário efetivo, composição
de múltiplos Targets, referências de consolidação e seleção final antes do cutover
de admissão/rollup/gate inicial. O writer legado continua em compatibilidade até
esse rollout; a extensão entregue não declara RF-INT-01 nem I1/I6 completos.
Não houve migration física neste incremento. Downgrade isolado de binários após
novos writes não é rollback seguro: o avaliador anterior ignora estas declarações
e pode creditar partial como legado. Preparar backup/binários consistentes no
ensaio integrado; nenhuma promessa de rollback sem perda foi feita ou testada.
E2E instalado/Grafx, benchmark, adoção/migração e restante do plano seguem pendentes.
Estado da iniciativa: **progresso**, não conclusão integral.

Par de declarações por binding publicado por push normal em `feature/v0.4.0`:
Core `8a48fcded92bcd8dbb5e9c7827ac27a2e7902fe9`; Community
`4fa85bcfed1129db0627a74415845b8aa5bb6b42`. `ls-remote` confirmou os dois
HEADs publicados e as árvores estavam limpas após os commits funcionais.

### Implementado — conjuntos explícitos de execuções por binding

Turno anterior: progresso. Árvores limpas em Core `b70807f9` / Community
`4fa85bcf`. DEI §4.4/§7.3–7.4 exige nomear o conjunto exato de Targets por
contribuição e invalidar apenas o escopo afetado. O adapter atual lê um único
execution_id por registro, inclusive na checagem temporal de Test Evidence.
Evoluir o mesmo ledger com referências tipadas por binding, admitidas pela cadeia
existente; nenhuma tabela paralela, inferência de ancestralidade ou mudança de
autoridade. A composição verificável inicialmente exige a mesma identidade de
source/revisão imutável dentro de cada conjunto; bases divergentes precisam de
observação compatível, não comparação cronológica de hashes. Adoção ARQ/VER,
escopo versionado e seleção final continuam no caminho crítico do cutover.

Entregue neste incremento:

- Bindings novos podem nomear conjuntos próprios de execuções persistidas ou
  aliases anteriores do mesmo batch. Contrato fechado, sem mistura com execução
  no envelope, sem alias ambíguo para outro conjunto, sem duplicatas após resolução
  e com orçamento agregado de 200 referências. Digest anterior preservado quando
  a opção não é usada; persistência v2 guarda somente IDs canônicos por binding.
- A cadeia original de admissão de Execution/Target/Receipt foi extraída uma vez
  no adapter Community e reutilizada. O Core decide atualidade por binding; a
  obsolescência de um Target invalida apenas os conjuntos que o nomeiam. Parciais
  não se somam, testes continuam presos ao registro imutável de implementação e
  a checagem temporal inclui todos os recibos do conjunto testado.
- Frontend permite selecionar explicitamente os recibos de cada obrigação e
  exibe atualidade por binding. Não há expansão cartesiana automática nem escolha
  de um recibo representativo que esconda os demais. REST e MCP usam o mesmo batch,
  CAS, autorização, transação e replay existentes; nenhuma migration física.

Validação em 2026-09-19, evidências em `PULSE_REFACTOR/.validation-v040/`:

- `provenance-execution-sets.json`: conjuntos e bytes dos **794/312 .py**
  Core/Community idênticos antes dos testes; **859/396 payloads** idênticos em
  source→wheel→install, sem mismatches. SHA256 dos wheels: Core
  `03a3aa32ee569c2e42180cc697cef66f67d1248f2bdc0c096e2a4bb742e00410`;
  Community `6ee52e7f202462e47ebdfc9e79fc483268a3aca118888c31ce12dac3fe5c8338`.
  Processos novos, PYTHONPATH pareado e dados descartáveis; runtime do usuário
  não foi reiniciado nem usado como evidência deste código.
- `core-execution-sets.log`: **175 passed em 21,95 s**, incluindo 17 casos novos
  de composição, bases incompatíveis, atualidade seletiva, limites e aliases.
- `community-execution-sets.log`: **79 passed / 1 failed em 227,50 s**. A falha
  era uma expectativa de lista contra a tupla retornada pela projeção interna;
  corrigida somente a asserção. `community-execution-sets-rerun.log`: **6 passed
  em 24,22 s**. Total final **80 casos distintos aprovados**, incluindo composição
  via origem real, rollback integral, replay após fechar sessão, REST→MCP e teste
  assinado contra todos os recibos nomeados. Os casos temporais/base conflitante
  usam fixtures de recibos admitidos; não constituem E2E instalado/Grafx.
- Frontend: **88 casos distintos aprovados** em quatro arquivos. Primeira rodada
  teve 87 aprovados e uma consulta de título ambígua (lista e banner). Consulta
  restrita à lista; `frontend-execution-sets-dod.log`: **13 passed em 4,34 s**.
  Build/typecheck, ESLint e dist aprovados: **78 arquivos**, tree SHA256
  `e5deaad76f2e32eb241cfb01f5f29676e9b8e689c88493cb542cdeacc518500c`.
- `closure-execution-sets.json`: **exit 0, ok=true**, nenhum finding de código ou
  documentação, oito budgets **0/0**; **7.540 imports Core / 1.242 Community→Core /
  25 dependências**. Catálogo/manifests pelos generators oficiais. Diff-check e
  Ruff dos arquivos de implementação/testes aprovados; a verificação adicional
  do módulo MCP apontou dois F401 já presentes no HEAD anterior, nos reexports
  DeliveryEvidenceInput/DeliveryEvidenceCommand, sem alteração nesta rodada.

Limites e retomada: a prova composta exige source_ref e revisão imutável iguais
no mesmo conjunto. Bindings diferentes podem ter bases diferentes; um conjunto
com bases divergentes é recusado com delivery_execution_base_conflict. Integração
entre bases/fontes distintas ainda requer observação compatível pelo contrato
consolidado; não inferir ancestralidade de hashes nem considerar este recorte como
DEI §7.4 completo. Adoção conjunta ARQ/VER, scope hash/ownership do inventário
efetivo, seleção final/consolidação, invalidação por progresso dirty e cutover dos
gates continuam pendentes. Preservação de policy por Card autorizada na decisão
F2B continua exigindo proveniência e deprecation warning quando implementada.
Estado da iniciativa: **progresso**, não conclusão integral. Não houve release,
migração de dados reais nem promessa de downgrade isolado seguro.

Par de conjuntos de execuções publicado por push normal em `feature/v0.4.0`:
Core `04e321dc01ce15642719a180d5d6718ae5f4b585`; Community
`575ab904741267b588e3a860a3d4ff942f5c6a4f`. `ls-remote` confirmou ambos os
HEADs publicados e as duas árvores limpas após os commits funcionais.

### Implementado — invalidação por progresso material (DEI §7.1–7.2)

Turno anterior: progresso publicado (Core `bebcedb5`, Community `575ab904`).
Investigação confirmou que `_execution_proof` validava apenas o head técnico,
sem consultar checkpoints dirty posteriores. DEI-T20/T21/T23 exigem distinguir
nota de contexto de alteração material, mantendo a fonte relacional autoritativa.
Contrato v2 declara none/targets/source/unknown; v1 não é reescrito e conserva
seu digest. Dirty ou delta material v1 significa incerteza no escopo declarado,
não uma afirmação inventada de ausência de mudança. Sem esses sinais, uma nota
antiga não é tratada como mudança apenas por timestamp. A regra de atualidade
fica no domínio Core; leitura SQL, revogações e observações continuam no adapter.
Uma observação admissível estritamente posterior ao checkpoint pode suportar
um sucessor; nova nota clean ou rebind do recibo velho não restaura prova.
Revogação continua humana/autorizada, não uma permissão nova do executor.
Seleção final/impacto selado e adoção ARQ/VER ainda pendentes, sem cutover nesta etapa.

Autorização adicional do usuário: quando necessário para pushes já autorizados,
pode-se executar `gh auth switch -u <usuario>` sem nova confirmação. Não foi
necessário mudar conta neste ponto.

Implementação inclui escopo tipado, leitura de toda a população de checkpoints
ativos (independente do cap de resumo), IDs de bloqueio limitados a 20 com sinal
de truncamento, e retirada de execuções antigas da seleção quando afetadas.
O frontend exige declaração explícita do efeito no código e permite selecionar
Targets existentes; nenhuma opção cria/altera Target ou concede autoridade.
Os testes de aliases/batch que pretendiam apenas citar trabalho anterior agora
declaram contexto sem mudança; um teste separado exige rollback quando dirty
precede a tentativa de usar uma observação antiga no mesmo lote.

Validação intermediária: `core-material-progress.log` **184 passed em 20,37 s**;
`frontend-material-progress-final.log` **92 passed em 32,84 s**, após remover o
default implícito de ausência de mudança. A primeira integração teve **52 failed /
36 passed em 215,05 s**: `_active_material_progress` usava `spec_edition` no
DeliveryScope, que expõe `edition`. Corrigidos o campo e a anotação do parâmetro;
par reconstruído/reinstalado antes da repetição integral. Nenhum gate foi relaxado.

Investigação para próxima etapa: `CardService.move_card` em `services/main.py`
é o writer do relatório (`report_target` para Validation/Done, impacto e conclusão
em `cards.conclusions`). `MoveCardUseCase` aplica autoridade de transição; não
substituir por simples permissão de append. O gate de impacto resolve
off/advisory/require em `services/impact_evidence.py`. Integrar o manifest/CAS da
seleção e a composição líquida nessa conclusão, preservando completeness/drift,
gates de dependência/revisão e o caminho separado de submit_task_validation.
O schema atual de impacto distingue repo core/community, mas surfaces não têm
identidade de source/base; a composição precisa expor ambiguidades, sem deduzir
source a partir do nome do repo nem somar arrays como se fossem impacto líquido.

Validação final deste incremento:

- `community-material-progress-final.log`: **88 passed em 202,67 s**, incluindo
  oito novos casos de persistência/atualidade, candidatos, gate pré-Done, origem
  inline/rollback, escopo conflitante e revogação. Nenhuma falha residual da suíte
  selecionada. O cenário de observação renovada usa fixture de fatos admitidos;
  não equivale a E2E instalado autenticando um checkout Grafx.
- `provenance-material-progress-final.json`: **794/312 .py** e **859/396 payloads**
  idênticos entre source/wheel/install antes da repetição, nenhum mismatch. SHA256
  Core `2f4482ea2808260ddc97e8cd40b4af2b4809e0a6ad0b7a0cc4a11e1bf3d167f7`;
  Community `61acd0b86c463a67ee3a1edd143c3a34f3d6df03b4a81ba1c06be6c64ef75ad5`.
  O payload Core final é byte-a-byte igual ao validado nos **184 testes Core**;
  não houve repetição dessa suíte sem mudança correspondente. Processos novos,
  PYTHONPATH pareado e bancos descartáveis em todas as rodadas.
- **92 testes frontend** aprovados; build/typecheck, ESLint e verificação do
  frontend_dist aprovados. **78 arquivos**, tree SHA256
  `6d4bf9c2ea2c005bbc1b0b59119d6fd6d567c14e5e6a11318e46721b617f5671`.
- `closure-material-progress-final.json`: **exit 0, ok=true**, nenhum finding
  de código/documentação, oito budgets **0/0**. **7.541 imports Core / 1.242
  Community→Core / 25 dependências**. READMEs pelo renderer oficial após drift
  somente de contagem; catálogo/manifests pelos generators oficiais. Ruff dos
  arquivos Python alterados e diff-check aprovados. Uma chamada inicial de Ruff
  usou o cwd do frontend; repetida no repo Community com o caminho correto.

Limite de cronologia: esta etapa exige observação posterior ao recebimento do
checkpoint material. Um batch com esse checkpoint seguido de prova baseada em
observação anterior é recusado atomicamente; mero progress_ref não demonstra
que o recibo antigo cobre a mudança. A composição de registros acumulados fora
do servidor precisa de demonstração admissível de aplicabilidade no contrato de
consolidação/seleção final, ainda pendente. Não declarar todo DEI §7/I4 concluído
apenas com esta proteção. Sem migration física, alteração de policy ou execução
de comandos no backend. O runtime do usuário foi preservado. A iniciativa segue
em **progresso**, com adoção ARQ/VER, seleção/impacto, migrações e validação integral
ainda no caminho crítico.

Par publicado por push normal em `feature/v0.4.0`: Core
`7dafe542b59376a6c6a7fef6f286df8f60a7f92a`; Community
`48eaa9df210d6dda7c9cfa746851180bb6860dc4`. `ls-remote` confirmou os dois
HEADs e as árvores limpas após os commits funcionais. A conta ativa permitiu os
pushes; não foi necessário executar `gh auth switch`.

### Em implementação — seleção selada no relatório existente

Turno anterior: progresso. Base limpa Core `9cbde122` / Community `48eaa9df`.
DEI §9.1 requer selar os IDs/revisões e o impacto apresentado no relatório,
preservando a autoridade de transição. O request recebe uma seleção fechada com
CAS Card/Spec/ledger; o servidor calcula hashes do payload persistido (o digest
de request da linha não identifica necessariamente seus IDs canônicos resolvidos).
O manifest fica em `cards.conclusions`, não em outra entidade de handoff.
No estado congelado, a leitura usa os IDs selecionados, mas mantém revogações,
heads de Target/teste e checkpoints materiais fora desse filtro: omissão não
apaga defeito nem restaura prova. Rework autorizado volta a ler o ledger corrente;
o relatório anterior permanece histórico. Relatórios legados sem manifest mantêm
compatibilidade até a adoção/cutover integrado. Este incremento sela também o hash
do impacto apresentado; a composição líquida/reconciliação para eliminar o corpo
manual ainda precisa ser integrada, sem fingir que uma união de arrays basta.

### Checkpoint — seleção selada implementada e validada (2026-09-19/20)

O relatório existente recebe `delivery_selection` tipada por REST/MCP. A porta
pública sela IDs, hashes, edição, versão do Card e revisão do ledger sob o fence
existente. O manifest é persistido no mesmo `cards.conclusions`; não há entidade
paralela, novo privilégio, implementação concreta no Core ou alteração de policy.
Relatórios em validation/rejected/done restringem a prova aos registros escolhidos.
Manifest inválido torna a projeção incompleta, inclusive no rollup da Spec;
revogações, heads e progresso material continuam sendo avaliados integralmente.
Seleção vazia é explícita e não concede completude. Rework preserva o relatório
histórico e volta a consultar o ledger atual.

Frontend: seleção optativa no Execution Report, com leitura das versões reais,
limite visível de 200 registros, refresh e bloqueio do submit durante erro/carregamento.
População truncada não é selecionada automaticamente. O manifest aparece na leitura
do relatório. A compatibilidade legada permanece até a adoção conjunta ARQ/VER.

Validação do payload final, antes de commits/pushes:

- `provenance-selection.json`: comparação integral de conjuntos e bytes de **796
  .py Core / 312 Community**, e **861 / 396 membros** source→wheel→site-packages.
  Wheels SHA256 Core
  `26d83151cd8e4bb58ca143f77d8a49bfef61fb59530fa2176426c3f6c43b224e`;
  Community `d1336d171a5d0d924e8c7938394be96abbfdde1d0ee6914d9f467dd7c1c7d21b`.
  Processos novos, PYTHONPATH pareado e bancos descartáveis. O runtime do usuário
  não foi reiniciado ou modificado. Nenhuma alteração de runtime após essa prova.
- **120 casos Core aprovados**, agregando 108 casos da rodada inicial e 12 de
  `core-selection-impact-final.log`. A rodada inicial teve 118 pass/1 fail:
  a fixture de impacto não implementava a porta CardDelivery. O diagnóstico exato
  foi `card_delivery_evidence_adapter_unavailable`. O teste agora fornece fatos de
  domínio explícitos pela porta pública e verifica os dois ramos do gate real:
  Done com prova, Rejected sem prova. Não se relaxou o gate nem se declarou esse
  teste de Core como validação de armazenamento/recibos reais. Permanecem três
  warnings preexistentes de marca asyncio em testes síncronos.
- **112 casos Community aprovados**: sete casos de seleção, 17 casos em
  `community-selection-real.log` e 88 em `community-selection-regression.log`.
  A integração real passou por origem/recibo/batch, CardService, adapter e commit
  com `CommunitySemanticSession`; uma sessão nova releu o manifest e aprovou o
  predicado de entrega. As primeiras tentativas identificaram configuração
  incompleta da fixture: persistence/realm/fact reader/critical context/session.
  Corrigida com adapters reais, sem simular o writer ou dispensar seus gates.
- **98 casos frontend aprovados**, combinando os 49 casos das outras quatro
  suítes com os 49 do CardModal na repetição. O único fail inicial foi histórico
  acumulado do mock entre os dois parâmetros; `mockReset` isolou as chamadas.
  Build/typecheck e verificação frontend_dist aprovados: **78 arquivos**, tree
  SHA256 `cdf918beb0501ca3bce511b8307c8ae6c17860b7c141ce0ed4eebd7af8e0c543`.
  ESLint sem erros; 17 warnings históricos nos arquivos existentes.
- `closure-selection-final.json`: **ok=true**, zero findings de código e docs,
  oito budgets **0/0**, **7.552 imports Core / 1.243 Community→Core / 25 dependências**.
  READMEs atualizados pelo renderer oficial; catálogo/manifests pelos generators
  oficiais. Uma tentativa intermediária usou nome incorreto do wheel Community;
  corrigido para o caminho comprovado em provenance (`okto_pulse-0.3.4`).
  Ruff dos arquivos Python alterados/testes e diff-check aprovados.

**Retomada:** falta composição líquida/reconciliação do impacto e seu reaproveitamento
canônico, incluindo aplicabilidade de observações buffered após checkpoint material;
falta composição atômica do último batch com o relatório. Esta seleção não fecha
DEI §9/I4. Adoção/cutover ARQ/VER e inventário completo, F2B com override por Card
autorizado e aviso de depreciação, migrações/rollback e validação integral do pacote
continuam pendentes. A iniciativa segue em **progresso**, sem novo bloqueio.

Par publicado por push normal em `feature/v0.4.0`: Core
`e624a7c61c6946447c58a93db86a6caaae695702`; Community
`ae54e4425c8a8e890cfd6107d2496107033db2d5`. `ls-remote` confirmou os dois
HEADs e as árvores limpas. A reautenticação estava válida para a conta ativa
`jpbraga`, que realizou os pushes. Não foi necessário `gh auth switch`; a
autorização do usuário para alternar contas, se necessário, permanece registrada.

### Em implementação — composição ordenada de impacto declarado

Turno anterior classificado como progresso; par publicado e árvores limpas
reconfirmados (Core `eed0b110` / Community `ae54e442`). DEI §6.2 exige distinguir
histórico e resultado líquido. O payload atual tinha source/revisão resultante,
mas nenhuma base anterior; não se deve inferir sequência por timestamp/hash.
`impact_base_revision` opcional declara essa base no mesmo checkpoint. Ausência
preserva o digest/replay legado e aparece como reconciliação quando há delta.

O domínio compõe arestas explícitas base→resultado por source, preservando repo,
path e todas as origens. Creates removidos não aparecem no líquido; renames
encadeados mantêm a origem. Branches, lacunas, ciclos, restaurações ambíguas e
conflitos pedem reconciliação. Symbols/tests afetados por rename/delete de arquivo
e superfícies anteriores sem confirmação explícita não são presumidos atuais.
O adapter lê todos os deltas ativos e exclui revogações autorizadas apenas da
projeção; não altera histórico. O painel de entrega mostra claims e pendências,
com caps/totais. Limites: 200 declarações, 20 itens de reconciliação visíveis,
64 KiB de resultado líquido; não transformar overflow em resultado parcial verde.

Esta projeção é preparação para a consolidação final, não conclusão de DEI I4:
`composed` prova somente composição determinística das declarações. Ainda falta
reuso de campos dos receipts e detecção de divergência, reconciliação gravável
dos resíduos, atualidade da base observada e uso do conjunto selecionado no
relatório/impact policy. Nenhuma dessas autoridades foi inferida de um claim.

Investigação para a próxima integração: `ImplementationTargetExecutionRecordView`
expõe source, revisão resultante, disposição, path/símbolo e recibo resultante;
não expõe a base anterior da mudança. `ImplementationTargetView.baseline_evidence_id`
e a resolução atual não demonstram, por si, essa aresta. Reutilizar os campos
selados do receipt quando disponíveis sem inventar base/ancestralidade. A nova
base declarada continua claim; não autentica a realidade nem satisfaz policy.

### Checkpoint — projeção de impacto líquido declarado validada

- **92 testes Core** em `core-net-impact-final.log`: composição por arestas,
  create/delete, rename encadeado/retorno, cancelamento conjunto de arquivo e
  símbolo, separação entre repos/fontes, identidades de origem preservadas,
  conflitos e limites; regressão de progresso, seleção, contribuições e catálogo.
- **98 testes Community** em `community-net-impact-final.log`: integração real
  da declaração até a projeção, releitura persistida, revogação sem apagar
  histórico, delta legado sem base e regressão completa de Delivery Evidence.
  Nenhuma falha de teste. Os casos adicionais após revisão foram executados no
  par final de wheels, com nova instalação/prova de correspondência.
- **32 testes frontend** aprovados. Após corrigir a legenda para “active
  declarations”, os três testes do painel alterado passaram novamente; os outros
  29 casos mantêm a validação do mesmo comportamento. Build/typecheck, ESLint
  sem erros/warnings nos arquivos alterados e verificação frontend_dist passaram.
  Artefato final: **78 arquivos**, tree SHA256
  `ff45ad6a5f6bac4ad5a3090c16e65c4571f0adc1bf53780959614d7cfb3a329e`.
- `provenance-net-impact-final-ui.json`: **797 / 312 .py**, **862 / 396 membros**
  source→wheel→site-packages byte-a-byte. O último rebuild Community alterou apenas
  o frontend; os payloads Python dos 92/98 testes permanecem idênticos. Core wheel
  SHA256 `0dd82dfe44cf473a811db10c437311465e8c8dd6368e00b3e5faf87b85e5d733`;
  Community `2fb652ffd60b56b0495f65e34ddebdb635d0b50063f4d67c47cf37ff7d139553`.
  Processos novos/PYTHONPATH pareado/bancos descartáveis; runtime do usuário intacto.
- `closure-net-impact-final.json` passou sem findings, com oito budgets **0/0**:
  **7.556 imports Core / 1.244 Community→Core / 25 dependências**. READMEs pelo
  renderer oficial após drift somente de contagem. Catálogo/manifests gerados
  oficialmente; o resource manifest mudou com o tool-doc e foi incluído no rebuild
  antes dos testes. `closure-net-impact-final-ui.json` confirmou o wheel com a
  legenda final: **ok=true**, zero findings e os mesmos oito budgets **0/0**.
  Ruff e diff-check passaram.

**Retomada:** continuar integração do impacto canônico com receipts, reconciliação
dos resíduos e conjunto selecionado/relatório, preservando policy e atualidade.
O preview lê deltas ativos do Card inteiro; não se apresenta como o snapshot
submetido. `history_count` conta as declarações ativas consideradas nessa
composição, não toda a história com revogações; a UI explicita essa distinção.
Sem migrations físicas neste incremento. Adoção ARQ/VER, F2B/Sprints, KG,
migrações/rollback e auditoria integral continuam no escopo. Goal em progresso.

Par publicado por push normal em `feature/v0.4.0`: Core
`c2d24b4a9f67e7fe022ceb35c7a8290786522478`; Community
`f30452b439906f039af68a47ffdeb9ef396c5237`. `ls-remote` confirmou ambos os
HEADs e as árvores limpas após os commits funcionais. Nenhuma troca de conta,
tag, release, merge ou migração em dados reais foi executada.

### Em implementação — reuso do impacto selecionado no relatório

Turno anterior: progresso; árvores limpas/HEADs Core `add2545c` e Community
`f30452b` confirmados. DEI §6.4/9.1 permite atender `impact_evidence_mode=require`
com o conjunto acumulado selado. `delivery_selection.reuse_impact` solicita essa
resolução pela porta pública; não aceita hashes, flags de validade ou bases
autoritativas do cliente. Manual + acumulado no mesmo request é conflito.

O writer resolve o agregado antes da policy, mantém o mesmo `impact_evidence`
para consumidores existentes e sela as bases em manifest v2. v1 preserva sua
serialização/hash. Fonte/revisão/identidade observadas e progresso material
integral entram na revalidação; a seleção não oculta mudança fora do resumo.
Head/receipt são lidos sob fence no relatório. A atualidade do impacto é
exposta separadamente (`report_impact`) e o gate de impacto require a consome na
conclusão; off/advisory não viram gate de Delivery Evidence.

Investigação adicional: o checkpoint guardava source_ref, mas não a identidade
observada no append. Novos deltas capturam `_impact_source_identity_sha256` do
head aceito no servidor; o request fechado não pode fornecê-lo. Ausência ou
conflito não impede salvar progresso, mas não autoriza reuso canônico. Histórico
antigo não é reescrito nem recebe identidade retroativa por coincidência de hash.
Reaproveitar revisão/path de receipt não prova change_kind de arquivo: um Target
de símbolo `created` pode estar em arquivo existente. A derivação completa ainda
precisa respeitar essa distinção; não fabricar file-created a partir dela.

### Checkpoint — reuso do impacto selecionado validado

O relatório pode consumir o impacto líquido dos registros selecionados sem
redigitar o bloco. O manifest v2 sela as bases observadas; a representação de
impacto consumida pelas policies permanece a existente. O fechamento em modo
`require` revalida pela porta pública com `for_update=True`: lock de Board,
head e receipts antes de ler revogações/progresso. A auditoria da implementação
de revogação confirmou a mesma serialização por Board/head. Os testes SQLite
abaixo validam as transições sequenciais; não são prova de contenção PostgreSQL.

- **87 testes Core** passaram em `core-reused-impact-isolated.log`: contratos
  fechados, preservação do hash v1, bases v2, composição, writer real, policy
  off/advisory/require, regressões MCP e catálogo. A primeira execução encontrou
  quatro problemas nas fixtures/asserts novas (mensagem versus código de erro e
  status alterado apenas no objeto de aplicação). Após corrigi-los, uma execução
  conjunta revelou vazamento de `delivery_evidence_gate=advisory` no Board
  compartilhado. A fixture agora restaura a configuração em `finally`; a suíte
  completa passou sem alterar qualquer gate. Três warnings preexistentes de
  marcação asyncio permanecem.
- **124 testes Community** passaram em `community-reused-impact-final.log`:
  relatório real persistido/recarregado, identidade/revisão, revogação, head
  conflitante, observação antiga, identidade ausente em registro legado,
  progresso material fora da seleção e mesma base observada novamente, além
  das regressões de seleção/batch/progresso/contribuições/execução. As fixtures
  semeiam receipts aceitos; estes casos não certificam admissão criptográfica.
- **73 testes frontend** em quatro arquivos passaram em
  `frontend-reused-impact.log`. Cobrem escolha de reuso, preservação da seleção,
  envio/retry do modal e atualidade exibida separadamente da policy de Delivery.
  Build/typecheck e verificação do frontend_dist passaram: **78 arquivos**, SHA256
  `1b293608269a1096b931e579cc4141ed225e12f2b75eb9df76b73745c6196621`.
  ESLint: zero erros, 15 warnings existentes no CardModal.
- `provenance-reused-impact-final.json`: **797 / 312 .py**, **862 / 396 membros**
  source→wheel→site-packages idênticos byte-a-byte, sem divergências. Core wheel
  SHA256 `4cfc1f0f8a4465266704d2ff07ddc518d11d0a0c72239200c0d908ef122fb943`;
  Community `a394b6c9736928e791bb0371f3cd96d4035041393a0b42e897153ae435aa2592`.
  Testes em processos novos, PYTHONPATH pareado e bancos descartáveis. A última
  correção foi somente na fixture; nenhum payload runtime mudou após essa prova.
- `closure-reused-impact-final.json`: **ok=true**, zero findings de código ou
  documentação, oito budgets **0/0**; **7.565 imports Core / 1.245 Community→Core /
  25 dependências**. O primeiro relatório apontou somente contagens dos READMEs;
  ambos foram atualizados pelo renderer oficial e o gate foi reexecutado.
  Catálogo/manifests gerados oficialmente, Ruff e diff-check aprovados.

**Retomada:** concluir integração com campos derivados dos receipts sem confundir
Target de símbolo com arquivo, reconciliação gravável, aplicabilidade de receipts
bufferizados e último batch+relatório atômico. Este incremento não conclui DEI I4
nem a iniciativa: adoção conjunta ARQ/VER, F2B/Sprints com compatibilidade por Card
autorizada e depreciação, KG, migrations/rollback e auditoria integral seguem no
escopo consolidado. Nenhuma migração física/dados reais, release/tag/merge ou
reinício do runtime do usuário. Goal em progresso, sem bloqueio novo.

Par publicado por push normal em `feature/v0.4.0`: Core
`a815243f719b312f1bec2ccd9ac154748e550291`; Community
`a58f2b152353d5dd3541577d8a140d9e41f78212`. `ls-remote` confirmou ambos os
commits e as árvores limpas. Autenticação ativa `jpbraga` válida; não foi preciso
alternar contas. A autorização para `gh auth switch -u <usuario>` permanece.

### Em implementação — resultados autenticados de teste antes de Done

Turno anterior: progresso, par publicado; Core `37e90843` / Community `a58f2b1`
limpos reconfirmados. DEI §7.4 / DEI-T25–27 exige salvar passed/failed durante
execução, promovendo elegibilidade por leitura. A reprodução com SQLite e recibo
HMAC real (`incremental-tests-baseline.log`) falhou no writer: a associação usava
`evaluate_delivery_coverage`, exigindo Done e passing para admitir qualquer run.
Antes da reprodução, `provenance-incremental-tests-baseline.json` comprovou ambos
os payloads instalados contra source/wheels byte-a-byte.

Separar admissão de resultado autenticado do crédito final em predicado de domínio
público. A edição mantém verificador de origem, fences e guard de estado; persiste
o resultado observado no servidor junto ao receipt, sem flags do cliente. Partial
e in_progress podem receber associação, mas o rollup final continua exigindo
contribuição completa, passing atual e cards Done. Não reescrever linhas legadas:
seu leitor compatível continua usando o cenário vivo sem inventar resultado antigo.
Frontend mostra os resultados e sua atualidade, sem confundir registro com crédito.

### Checkpoint — resultados incrementais de teste autenticados

- **115 testes Core** passaram em `core-incremental-tests-fenced.log`: admissão
  independente de crédito, partial/passed/failed, escopo e IDs exatos, população
  completa, estados congelados, regressões de domínio/lifecycle/contratos/seleção,
  reuso de impacto e catálogo/manifests. A primeira chamada apontou um nome de
  arquivo de teste inexistente e não executou testes; a lista real foi localizada
  e as execuções válidas estão nos logs subsequentes.
- **75 casos distintos Community** validados no payload final:
  `community-incremental-tests-final.log` teve 74 passed/1 failed; o caso novo de
  batch omitia `contract_version` na fixture. Após a correção exclusivamente de
  teste, `community-incremental-tests-new-final.log` passou os oito casos novos
  (22,29 s), incluindo o batch. As demais 67 regressões já haviam passado nesse
  mesmo payload. A primeira execução anterior teve 71 passed/3 failed: duas
  fixtures tentavam recriar o diretório de receipts; a terceira identificou que
  separar admissão de crédito retirava implicitamente o bloqueio de validation.
  O predicado agora exige started/in_progress/done para o Test Card; validation
  e rejected continuam recusados. Não foi relaxado nenhum teste ou gate final.
- SQLite real e receipts HMAC emitidos/verificados pelo mecanismo Community:
  associação antes de Done, promoção por leitura sem novo append, failed sem
  crédito, sucessor preservando o resultado original, nova falha invalidando
  passing antes da associação, assinatura/status inconsistentes, implementação
  inexistente, freeze, rollback integral e replay. O executor externo das
  fixtures é determinístico; esta validação não é um E2E de inspeção de repositório
  ou do runtime de testes externo. REST/MCP/autorizações e demais joins permanecem
  cobertos pelas regressões de integração/contratos executadas.
- **33 testes frontend** passaram em `frontend-incremental-tests-final.log`:
  registro de passed/failed durante execução sem campo de confiança do cliente,
  histórico/atualidade por Card e rollup sem aprovação implícita. Build/typecheck,
  ESLint sem warnings/erros nos arquivos alterados e frontend_dist aprovados.
  **78 arquivos**, tree SHA256
  `016c2d132bc1f2f3ed30133c0272fbb353fa0507bc9c4383378766ff8958a4b9`.
- `provenance-incremental-tests-final.json`: **797 / 312 .py**, **862 / 396 membros**
  source→wheel→site-packages idênticos byte-a-byte. Core wheel SHA256
  `2d8e315d53ab1b8047dab257250d063bc5c9d6c73a373dc9508b35711e6418e0`;
  Community `9f95b6041d6a665c4bfdc488d8e3c7a8291c41c4784e0a7116a26d82428bda00`.
  Novos processos/PYTHONPATH pareado/bancos descartáveis, sem alterar o runtime
  do usuário. Não houve mudança de payload após a prova final.
- `closure-incremental-tests-final.json`: **ok=true**, zero findings de código
  ou documentação, oito budgets **0/0**, **7.565 / 1.245 imports, 25 dependências**.
  Nenhum drift de README. Catálogo/manifests regenerados oficialmente; somente
  resource manifest alterado pelo tool-doc. Ruff e diff-check aprovados.

`current_verified_run` agora pode descrever uma falha autenticada. A revisão dos
consumidores confirmou que o crédito final ainda exige explicitamente `PASSED`,
além de Done/atualidade/contribuição completa. O resultado dos novos registros
é preservado pelo servidor; ausência do campo em registros legados mantém o
leitor compatível e não autoriza backfill de história. Nenhuma migração física.

**Retomada:** DEI-T25–27 avançaram com o caminho automatizado já admitido. Métodos
especializados continuam exigindo verifiers reais; esta mudança não os simula.
Continuar a integração receipt→impacto, reconciliação gravável, observações
bufferizadas, batch+relatório atômico e resumo de retomada completo; manter a adoção
conjunta ARQ/VER, F2B/Sprints/depreciação autorizada, KG, migrações/rollback,
benchmark e auditoria integral no escopo. Goal em progresso, sem novo bloqueio.

Par publicado por push normal em `feature/v0.4.0`: Core
`8c265787963a9fab3e0c316e75048ee15650cc93`; Community
`f2cd851c6af23845d0d9f400f171f0af9126ddac`. Ambos confirmados por `ls-remote`,
árvores limpas após os commits funcionais. Sem troca de conta, release, tag,
merge, migração de dados reais ou reinício do runtime do usuário.

### Em implementação — último batch e relatório na mesma unidade de trabalho

Turno anterior: progresso; Core `15828d64` / Community `f2cd851` limpos. DEI §9.2
permite compor append e relatório usando os writers canônicos. A investigação
confirmou que o batch sela recibo na primeira entrada imutável e CardService
guarda o manifest na conclusão existente, sem commit interno no move. A nova
variante fechada `card-delivery-report/v1` une esses caminhos; não cria entidade
de handoff, tabela ou diário paralelo. O adapter mantém a fence e o savepoint;
Core autoriza tipos/execução e transição antes da escrita, delegando às portas.

Digest de replay cobre todo o comando (batch, relatório, estado esperado e
seleção), sem mudar o hash dos batches comuns. A seleção inclui todos os novos
registros e os IDs existentes explícitos. O resultado retorna recibo histórico
do primeiro relatório que incluiu aqueles IDs; replay após retrabalho não move
o Card nem emite evento. Erro de relatório preserva o código/gate acionável e
reverte a UoW. Ainda em validação; não considerar este checkpoint publicado.

### Checkpoint — batch e relatório atômicos validados

- **66 testes Core** passaram (`core-delivery-report.log`): contrato fechado,
  limites, autorização antes de escrita, regressões de batch/seleção/evidência
  e igualdade dos catálogos/manifests gerados.
- **95 casos distintos Community** passaram no mesmo payload: 92 na regressão
  `community-delivery-report-regression.log`; os 11 casos de
  `community-delivery-report-final.log` incluem nove já nessa regressão e dois
  adicionais de rollback; o caso adicional de concorrência passou em
  `community-delivery-report-concurrency.log` (10,87 s). Duas sessões independentes
  obtêm o mesmo recibo, com um único batch e uma única conclusão persistida.
- SQLite real, writer canônico de Card e integração REST/MCP: permissões,
  seleção, estado esperado e gates de impacto preservados; rollback inclusive
  após conclusão/evento preparados e após execução inline; replay integral,
  conflito de corpo e replay após retrabalho sem nova transição. A primeira
  execução falhou somente na fixture que tratava ActorContext como dataclass;
  corrigida para construir o contexto explicitamente. Nenhum gate relaxado.
- **33 testes frontend** passaram (`frontend-delivery-report-regression.log`).
  Este incremento oferece composição opcional por REST/MCP; ainda não adiciona
  o compositor à UI. Frontend_dist verificado, 78 arquivos, tree SHA256
  `016c2d132bc1f2f3ed30133c0272fbb353fa0507bc9c4383378766ff8958a4b9`.
- `provenance-delivery-report.json`: **798 / 312 .py**, **863 / 396 membros**
  source→wheel→site-packages idênticos byte-a-byte antes dos testes de comportamento.
  Core wheel SHA256
  `61f5b59e2fd2afd527f4c5ff7f826a3836d20f2bb0b3f63c59b3f22117cb907b`;
  Community `ea15753a395d62762befb4b4157f5d8eab875a03af39a1a5ca478cfac400eeff`.
  Nenhuma mudança de payload posterior; testes em novos processos com PYTHONPATH
  pareado e bancos descartáveis. Runtime do usuário não reiniciado.
- `closure-delivery-report-final.json`: **ok=true**, zero findings de código ou
  documentação, oito budgets **0/0**, **7.574 / 1.246 imports, 25 dependências**.
  O primeiro closure apontou apenas contagens nos READMEs, atualizadas pelo
  renderer oficial. Catálogo/manifests regenerados oficialmente; somente o
  resource manifest mudou pelo tool-doc. Ruff e diff-check aprovados.

Sem nova tabela, entidade de handoff, migração de história ou alteração de policy.
O recibo usa a primeira conclusão histórica que selou os IDs do batch; o replay
continua sujeito ao escopo atual de Card/Spec e à autorização. Test Card→Validation
continua sem relatório de execução e não admite esta composição.

**Retomada:** publicar este par e registrar os hashes abaixo. A iniciativa continua
em progresso: composição na UI, integração receipt→impacto/reconciliação gravável,
observações bufferizadas e resumo de retomada; adoção conjunta ARQ/VER e cutover de
inventário/gates; F2B/Sprints com compatibilidade por Card e depreciação autorizadas;
KG, migrações/rollback, benchmark e auditoria integral permanecem no escopo.

Par publicado por push normal em `feature/v0.4.0`: Core
`b56123854eb608e12bfc5d721ff00b46a829ba34`; Community
`011afcbec219f7ec356c654198f97f7bd2cfd6f7`. Ambos confirmados por `ls-remote`,
com árvores limpas após os commits funcionais. Autenticação `jpbraga` válida;
não foi necessário usar a troca de conta autorizada pelo usuário. Este par
encerra o checkpoint de batch+relatório por REST/MCP; as pendências de retomada
acima continuam abertas.

### Em implementação — composição do último lote na interface

Turno anterior classificado como progresso: Core `7ccc7f74` e Community
`011afcb` publicados e limpos. DEI §9.1–9.3/DEI-T64: o relatório da UI já sela
seleção e reutiliza impacto, mas ainda precisava salvar o último lote em chamada
separada. Reutilizar os formulários de progresso e associação de provas para
preparar entradas locais no diálogo do relatório; enviar pela variante canônica
`card-delivery-report/v1`, sem escrita antecipada, aprovação ou nova autoridade.
Seleção e draft devem concordar nas três revisões; retry idêntico preserva a chave
e rejeição conserva conteúdo. Validar UI, transportes reais existentes, pacote
instalado e closure antes de publicar. Adoção conjunta e demais frentes continuam
abertas; este registro ainda não declara a UI validada.

### Validação — último lote preparado no relatório da UI

- **102 testes frontend** passaram em `frontend-report-ui-final.log` (24,26 s):
  relatório/seleção, múltiplos rascunhos removíveis, nenhuma escrita ao preparar,
  associação de contribuição completa e runs passed/failed sem confiança enviada
  pelo cliente, permissões, três revisões, limites de 50/200, timeout/retry idêntico,
  edição gerando nova chave, bloqueio de duplo submit e refresh falho após commit.
  A primeira rodada teve 92 passed/1 failed por mock residual no describe novo;
  a fixture passou a limpar chamadas entre casos. Nenhum comportamento de gate
  alterado para fazer os testes passarem.
- Build/typecheck e `verify:frontend-dist` aprovados: **78 arquivos**, tree SHA256
  `0fd51cbd990753554f13b68dcd3aa9a16ea3ec758d9976acb479cf1b1bba189c`.
  Lint completo aprovado pelo ratchet: **402 warnings / baseline 402**, zero erros
  (`frontend-report-ui-lint-ratchet.log`); o build mantém o aviso de chunks grandes.
- Baseline pareado provado antes dos testes de comportamento. Após rebuild/install,
  `provenance-report-ui-final.json` confirmou **798 / 312 .py**, **863 / 396 membros**
  source→wheel→site-packages idênticos. Core wheel SHA256
  `65192df44dd7efbc0967549809b330a7adacbbf9804a3fcce0100b11192e57bf`;
  Community `f6c2406e14099c2e7e28a0623e8cdd1d0c1fff8f61b6a9ee6135cbdaa486af07`.
- No par instalado final: **14 testes Core** de contrato/catálogo/manifest
  (`core-report-ui-contract.log`, 3,78 s) e **12 testes Community** de atomicidade,
  autorizações, concorrência, rollback, REST/MCP e replay
  (`community-report-ui-transport.log`, 33,43 s). Novos processos/PYTHONPATH pareado,
  dados descartáveis, nenhum reinício do runtime do usuário.

A UI usa os formulários existentes para preparar progresso e associações a provas
existentes; execução de origem inline continua no contrato REST/MCP. Não há novo
diário persistente: o rascunho é local ao diálogo e o texto informa seu descarte ao
fechar. Test Card→Validation mantém seu fluxo sem relatório. Nenhuma migração,
mudança de policy, permissão ou crédito final. A documentação Community foi
atualizada, inclusive para não chamar resultados admissíveis de “somente passing”.

**Retomada:** concluir closure/publicação deste checkpoint; depois continuar a
integração receipt→impacto, reconciliação gravável, observações bufferizadas,
retomada delimitada e adoção conjunta ARQ/VER com cutover dos gates/inventário.
F2B/Sprints/depreciação autorizada, KG, migrações/rollback, benchmark e auditoria
integral permanecem abertos. Este incremento não prova o E2E completo DEI-T64 nem
a conclusão da iniciativa.

Closure final `closure-report-ui-final.json`: **ok=true**, findings de código e
documentação vazios, oito budgets **0/0**, **7.574 / 1.246 imports, 25 dependências**.
Não houve drift de README nem mudança de catálogo/manifests neste incremento.
Diff-check aprovado; o payload não mudou após a prova final.

Community publicado por push normal em `feature/v0.4.0`:
`51b43080bb950ba46cd8fab09423afcbfc3f7bb9`, confirmado por `ls-remote`, árvore limpa.
O Core executável permanece o de `7ccc7f74a682972579700f8acdc7062ccb85f0c7`
(implementação `b5612385`); neste incremento o Core recebe somente este ledger.
Sem troca de conta, release, tag, merge ou intervenção no runtime do usuário.

### Em implementação — crédito por contribuição aprovada e critério

Turno anterior: progresso, Core `cf3bf643` / Community `51b4308` publicados.
Investigação ARQ/VER §4.2/§6.3/§11 e DEI §7: `architecture_adoption` seleciona
Designs, não o contrato conjunto. O inventário efetivo já preserva escopos por
Card, mas o avaliador legado considera uma implementação completa suficiente
para a obrigação e não exige cada condição selecionada por contribuição.
Não ativar adoção conjunta com esse veredito: Card de UI não pode concluir o
trabalho de autorização, nem um passing funcional cobrir condição técnica ausente.

Implementar o avaliador canônico do contrato adotado pela porta pública de
inventário, reutilizando os predicados de prova/estado/waiver existentes e exigindo
atestado do scope_sha256 por binding/Card e critérios do cenário autenticado.
Ausência histórica desses fatos não gera preenchimento por leitura. O caminho
legado permanece distinto até o cutover integrado de writers/readers/gates/adoção.
Baseline source→wheel→install comprovado em
`provenance-contribution-scope-baseline.json`; validar a reprodução, a composição
por critérios, herança e limites antes de ligar qualquer writer ao novo contrato.

### Validação — avaliador de entrega por escopo e condição

- Reprodução executável em `test_effective_delivery_coverage.py`: um FR com
  contribuições de UI e autorização, apenas UI entregue/testada, recebe crédito
  do avaliador legado. O novo avaliador conserva a implementação observada e
  aponta `authorization` em missing_card_ids; não conclui o requisito.
- **128 testes Core** passaram (`core-contribution-scope-tests.log`, 6,61 s):
  escopos completos/parciais, critérios funcionais/técnicos distintos, várias
  execuções de teste sobre a mesma implementação, IDs exatos, método não admitido,
  falha/assinatura/atualidade, reatribuição seletiva, BR herdada do resolver real,
  waivers por fase, população divergente/duplicada e limites de expansão. Leitor
  `card-contribution-scope/v1` exige formato exato; ausência histórica permanece
  sem atestado e formato novo inválido nunca vira fallback legado. Catálogo e
  manifests continuam iguais aos geradores.
- **38 testes Community** passaram (`community-contribution-scope-regression.log`,
  61,74 s) com SQLite real: regressão dos caminhos Delivery/REST/MCP/batch+relatório,
  autorizações, concorrência e rollback. Estes testes confirmam compatibilidade
  do runtime atual; não afirmam que os writers já persistem o novo atestado.
- `provenance-contribution-scope-final.json`: **799 / 312 .py**, **864 / 396 membros**
  source→wheel→install byte-a-byte. Core wheel SHA256
  `84f4abee50d39ab25dc14669d3b5b802cc7b271daefe013cd6eb1aea2df86dd4`;
  Community `f6c2406e14099c2e7e28a0623e8cdd1d0c1fff8f61b6a9ee6135cbdaa486af07`.
  Novos processos, PYTHONPATH pareado e dados descartáveis. Nenhuma alteração de
  frontend neste incremento; dist preservado do checkpoint anterior. Ruff e
  diff-check aprovados; a primeira verificação estática encontrou apenas estilo
  E701 no teste novo, corrigido pelo formatter antes do build/testes.

**Integração ainda obrigatória:** a porta pública `DeliveryInventoryPolicy`
agora oferece o avaliador, mas ele NÃO substituiu o rollup/gates ativos. Próximo
passo é ligar o contrato conjunto à persistência: marcador de adoção por Spec e
edição, criação nova e adoção/revisão autorizada, atestados imutáveis de escopo
gerados no writer canônico, critérios/método da execução autenticada no adapter,
admissão/DoD/rollup/readers e gate de início consumindo a mesma resolução.
Não reutilizar `architecture_adoption` como esse marcador, não expor hashes de
confiança ao cliente, não preencher scopes em registros antigos e não ligar só
o gate inicial deixando o fechamento no avaliador legado. A falta de atestado
não deve invalidar Specs legadas em andamento sem adoção explícita.

Não há migração ou mudança de autoridade/história neste checkpoint. RF-INT-01/02
e a adoção conjunta continuam incompletos até essa integração e sua validação
real descartável. As demais frentes do pacote permanecem no escopo.

Mapa confirmado para retomar essa integração sem reinvestigar o mesmo desenho:
`services/main.py::SpecService.create_spec/move_spec` e
`application/use_cases/allowed_transitions.py`; Community
`adapters/sqlalchemy_models.py::Spec`, `relational_schema_migrator.py` e
`relational_schema_steps.py` (a migração de architecture_adoption é apenas exemplo
de coluna nullable, não deve ser reaproveitada como contrato conjunto).
`sqlalchemy_delivery_evidence.py::_record_inventory/_record_card_entry`,
`load_card_snapshot/load_rollup_snapshot` ainda usam o inventário legado.
`services/delivery_evidence.py::require_card_delivery` e gate da Spec ainda chamam
o avaliador legado. O reader `GetRequirementVerificationUseCase` resolve o plano
completo, mas declara `adoption_evaluated=False`; centralizar seu snapshot para
os writers/gates sem chamar um reader paginado como autoridade de transição.

`closure-contribution-scope-final.json`: **ok=true**, zero findings de código ou
documentação, oito budgets **0/0**, **7.580 / 1.246 imports, 25 dependências**.
A primeira auditoria encontrou apenas drift nas contagens dos READMEs; ambos
foram atualizados pelo renderer oficial. Community muda somente esse README
neste checkpoint. Nenhum payload alterado após a prova byte-a-byte final.

Publicado por push normal em `feature/v0.4.0`: Core
`33077cd75a9df2d5e7be1137e0c4cee60391e4fb`; Community
`4f9d5de71166e42718d3581f2a3eadb87987fe38` (somente documentação).
Referências remotas confirmadas por `ls-remote`, árvores limpas após os commits.
Iniciativa em progresso; integração da adoção conjunta é a próxima dependência,
não uma frente concluída por estes testes de domínio.

### Em implementação — adoção conjunta e integração dos writers/gates

Retomada de 2026-09-19: autenticação `jpbraga` válida, sem troca de conta;
`ls-remote` confirma Core `f3cabf44` / Community `4f9d5de` publicados.
Há WIP não publicado em ambos os checkouts. A prova de payload e os testes
do checkpoint anterior **não validam estas alterações**.

- Marcador tipado `spec-execution-contract/v1` independente da seleção de
  Architecture Designs; novos registros adotam, coluna nullable sem backfill
  conserva o contrato histórico. Adoção explícita pelo writer de Draft com
  versão/edição esperadas, fence e proveniência do ator existente.
- Resolução compartilhada do plano e contexto efetivo no snapshot; writer de
  implementação persiste atestados de escopo por contribuição, sem input do
  cliente nem atualização de provas históricas por leitura. Rollup e gate de
  Card passam a consultar esse contexto quando a Spec adota o contrato.
- Ainda pendentes: admissão por critério/método, reader e gate de início comuns,
  transportes/UI, testes de domínio e persistência real, migração/rollback,
  rebuild pareado/prova byte-a-byte, frontend, catálogo e closure zero.

Não publicar este WIP como feature pronta nem ativar rollout fora dos testes
descartáveis. Retomar pela inspeção do diff de `sqlalchemy_delivery_evidence.py`
e integração de `require_test_result_admission` com o contexto efetivo.

Atualização 2026-09-20 (ainda WIP, sem novos commits): admissão já consulta
critério/método e escopo; reader usa `SpecExecutionPlan`; primeiro início
Validated→InProgress consulta classificação global e plano sob o fence do Board.
Specs já em andamento não recebem esse predicado por retomada. REST/MCP e painel
de verificabilidade oferecem adoção explícita em Draft; UI trata CAS, permissão,
duplo submit e refresh falho após commit. Migração continua nullable/sem backfill.

Evidência intermediária:
- `provenance-joint-first.json` e `provenance-joint-second.json`: **801/312 .py**,
  **866/396 membros**, source→wheel→install idênticos em processos novos.
- `core-joint-first.log`: **124 passed**; `frontend-joint-tests.log`: **34 passed**.
  Build frontend aprovado: **78 arquivos**, árvore
  `7b6dad37abc5ac83dc8dcdea65508597f2e6a6d0360dd784a957dcf5298b9367`.
  Lint **402 warnings/baseline 402**, zero erros (`frontend-joint-lint.log`).
- `community-joint-first.log`: **52 passed** (migração, leitura, ledger e relatório).
  Integração inicial com SQLite+assinatura: **4 passed** após corrigir o digest
  default capturado pela fixture de manifest (`community-joint-integration.log`).
- `community-joint-adoption.log`: **22 passed/2 failed**; os 12 casos de criação
  passaram. Falhas das novas fixtures: porta de Knowledge faltando no pós-write
  REST e porta de application persistence faltando no teste do gate. Corrigidas
  as configurações de teste; repetição em andamento.
- `core-joint-gates.log`: **45 passed/15 failed**, por coluna ausente no schema
  duplicado de testes `tests/sqlalchemy_test_models.py`. Adicionada a mesma coluna
  nullable no modelo de teste; repetir antes de interpretar paridade dos gates.

Ainda não há prova final deste incremento, closure final ou publicação. Verificar
logs `*-joint-*-retry.log`, ampliar concorrência/content lock/start gate/rollback,
completar documentação servida e repetir o rebuild/prova caso o payload mude.

### Checkpoint validado — contrato conjunto seleciona writers, plano e crédito

2026-09-20: os itens implementados acima formam agora um incremento integrado.
O caminho adotado usa o mesmo plano relacional na leitura, gravação, admissão,
gate de primeiro início e avaliação da entrega; o marcador ausente mantém o
avaliador histórico. A origem da seleção de Designs permanece independente.
O writer persiste `card-contribution-scope/v1`, não aceita hash autorado pelo
cliente e não preenche registros anteriores. A UI de rollup mostra contribuições
e critérios pendentes mesmo quando há alguma prova aceita, com listas limitadas.

Validação no payload final:
- **168 passed**, `core-joint-final.log` (13,63 s): domínio, seleção do contrato,
  contribuição/critério, planejamento, catálogo e paridade de transições.
  A repetição revelou uma diferença real na ordem do primeiro bloqueador:
  o preview consultava adoção antes da avaliação qualitativa existente. A ordem
  foi alinhada à mutação, preservando ambos os gates e o teste original.
- **22 passed**, `core-joint-packaged-contracts.log` (2,91 s): manifest MCP e
  envelope de content lock. Catálogo e manifest regenerados pelos geradores
  oficiais; nenhum diff gerado necessário nesta mudança de parâmetro.
- **15 passed**, `community-joint-final-adoption-retry.log` (30,22 s): REST e
  FastMCP reais, CAS concorrente (uma adoção/uma versão/um histórico), autorização,
  content lock, Draft versus Approved/Validated/InProgress/Done, rollback de
  conteúdo companheiro inválido, atestados persistidos/replay, assinatura real,
  mudança de alocação, ausência de upgrade por leitura e gate com o plano real.
  O teste de rollback inicialmente usou Card inexistente, mas o writer legado
  tem política explícita de prune para esses links. Investigado em
  `_validate_spec_linked_refs`; preservada essa semântica. O teste usa referência
  de API Contract inválida, que o mesmo validador sempre rejeita. O teste MCP
  trata o envelope de erro real do host (`raise_on_error=False`).
- **52 passed** em `community-joint-first.log` (89,76 s), migração/leitura/ledger/
  relatório; `community-joint-final.log` teve **47 passed/1 failed**, incluindo
  regressões de teste incremental e relatório. A falha era a fixture de gate
  criando Design fora de `CommunitySemanticSession`; foi composta a sessão
  correta e esse teste passou na rodada final de 15. Nenhum erro conhecido
  permanece nestas rodadas. **12 casos de criação** passaram na rodada
  `community-joint-adoption.log`, comprovando marcador prospectivo em derivação.
- **125 passed**, `frontend-joint-final-tests.log` (27,25 s), painel de
  verificabilidade/adoção, rollup, DoD e CardModal. Build/typecheck e verificação
  de distribuição aprovados: **78 arquivos**, tree SHA256
  `6ca565b8ddcb2f189fba5b5e9c00b06db4105e5c35733ff163fb88ebbd301911`.
  `frontend-joint-final-lint.log`: zero erros, **402 warnings/baseline 402**.
- `provenance-joint-final.json`: **801/312 .py**, **866/396 membros**, igualdade
  byte-a-byte source→wheel→install. Wheel Core
  `2130809ab00291dcee568f24c99b61e5cf1dab7c3b2ee416a13b133d27300594`;
  Community `bcbb6a22d3750c686c7798a65c116c8cd0ab861da114462c7361ba57cfb8bf5b`.
  Nenhum payload alterado após esta prova. Novos processos, PYTHONPATH pareado,
  bancos descartáveis; nenhum processo do usuário reiniciado.
- `closure-joint-packaged-final.json`: **ok=true**, zero findings de código ou
  documentação, oito budgets **0/0**, **7.606/1.248 imports**, 25 dependências.
  Drift inicial limitado às contagens dos READMEs; renderer oficial aplicado.

Migração: coluna JSON nullable idempotente, sem backfill nem mudança de status,
edição, versão ou histórico legado. Rejeição da adoção reverte a unidade de
trabalho. O rollback de implantação para binários anteriores sobre dados já
adotados **não foi validado**; a matriz de upgrade/rollback do rollout continua
obrigatória. Nenhuma migração em banco real foi executada.

Retomada após publicar este checkpoint: ampliar a matriz DEI/ARQ/VER para
contribuições distintas em persistência real, paginação global/concorrência de
fontes e transições, seleção/relatório atômico sob contrato adotado e rollout
instalado. Continuam abertos receipt→impacto, reconciliação gravável, observações
bufferizadas, retomada delimitada, métodos especializados, F2B/Sprints com a
depreciação por Card já autorizada, KG, benchmark e auditoria integral. Este
checkpoint não conclui a iniciativa nem autoriza release/tag/merge.

Publicado por push normal em `feature/v0.4.0`: Core
`2a768e4ef1458f8b8969e09ad5ceda562a86a798`; Community
`6c0cb9032cf88a80681c8fa2365dab054a6ae2c7`. Ambos confirmados por `ls-remote`,
árvores limpas após os commits. Conta ativa `jpbraga`, sem necessidade de
`gh auth switch`. Este registro posterior altera somente o ledger.

### Em implementação — relatório adotado e falha fechada no gate do Card

2026-09-20, retomada de Core `b1bcc0c7` / Community `6c0cb90`, árvores limpas.
Turno anterior classificado como progresso. `provenance-adopted-report-baseline.json`
confirma novamente o par instalado antes da reprodução (**801/312 .py**,
**866/396 membros**, bytes idênticos).

Reprodução `core-adopted-report-reproduction.log`: **4 failed/5 passed**. Em
`require_card_delivery`, o filtro de implementações faltantes iterava apenas
as linhas devolvidas pelo avaliador; resultados estruturalmente desconhecidos
podiam devolver zero linhas e passar o gate, ou permitir crédito quando o
registro de métodos estava indisponível. Casos: limite de resolução, identidade
ambígua, contexto efetivo ausente/inválido e métodos desconhecidos.

Correção em andamento: só os blockers das fases de implementação/teste são
interpretados pelo gate específico do Card; qualquer blocker estrutural continua
falha fechada. Prova de implementação atual antes de Done segue admissível;
o Card não passa a exigir teste passing. A policy advisory existente é preservada.
Próximo: composição real de seleção/relatório atômico sob contrato adotado,
contribuições de Cards distintos e rollback/replay sem crédito emprestado.

Investigação adicional e implementação deste checkpoint:
- `community-adopted-report-composed2.log`: **2 failed/1 passed**. Composição
  Community real (SQLite descartável, sessão semântica, KG com diretórios
  temporários), recibo aceito e contrato adotado. Sem validação humana, o caminho
  direto `CardService.move_card` chegava a Done com prova ausente ou partial,
  embora `delivery_evidence_gate=blocking`; o caso complete passava. FR-3 e o
  encerramento por obrigações atuais do pacote determinam a correção, sem nova
  policy nem alteração de autoridade. N/A de Architecture/Mockup e skip cognitivo
  são somente dados explícitos desta fixture; os respectivos gates reais rodam.
- O caminho direto agora adquire o fence existente do Board, relê o Card e
  verifica a entrega antes de acrescentar conclusão/evento/status. A porta
  pública aceita o relatório prospectivo construído pelo servidor. A mesma
  validação de manifesto limita o crédito aos registros selecionados, sem
  simular Done nem modificar histórico para avaliar a proposta. Testes passing
  continuam sendo responsabilidade do rollup da Spec; `advisory` não vira crédito.
- O teste integrado de seleção comprova que prova complete não selecionada não
  pode liberar um relatório partial; selecioná-la explicitamente permite a
  conclusão. Rejeição reverte lote e outbox mesmo se o chamador tentar commit.
- A ampliação da regressão encontrou uma chamada sem `await` de
  `_snapshot_obligations` em `report_impact_status`, depois da integração do plano
  compartilhado. Corrigida a chamada; não foi relaxada a integridade do manifesto.
  `community-adopted-report-final.log` documenta **2 failed/28 passed**, ambos os
  failures desse consumidor, a repetir no payload corrigido.
- Seis casos de ciclo de vida do Core dependiam do adapter de teste que só
  expunha snapshot de Spec, sem porta de Card. Receberam fixture explícita de
  prova pronta para testar histórico/dependências/eventos, mantendo a policy
  blocking. O gate real (ausência/partial/advisory/seleção) é coberto no Community.
  A rodada anterior está em `core-adopted-report-final.log` (**6 failed/131 passed**).
- `provenance-adopted-report-final.json`: **801/312 .py**, **866/396 membros**,
  source→wheel→install idênticos. Novos processos com PYTHONPATH pareado.
- `core-adopted-report-verified.log`: **137 passed**, 20,83 s, três avisos
  preexistentes de marca asyncio em testes síncronos de impacto.
- `frontend-adopted-report.log`: **54 passed**, 34,04 s, incluindo rejeição de Done
  que mantém relatório, seleção e lote não enviado. Produção do frontend não
  mudou; `frontend-adopted-report-dist.log` verifica os 78 arquivos publicados,
  tree SHA256 `6ca565b8ddcb2f189fba5b5e9c00b06db4105e5c35733ff163fb88ebbd301911`.

Validação final encerrada:
- `community-adopted-report-verified.log`: **53 passed**, 113,54 s. Contrato
  legado/adotado, ausência/partial/complete, seleção sem crédito emprestado,
  rollback mesmo com commit após erro, replay, concorrência (um lote/relatório
  para duas submissões idênticas), reuso de impacto e atestados incrementais.
  Os dois failures e os avisos de coroutine não aguardada anteriores desapareceram.
- `closure-adopted-report-final.json`: **ok=true**, zero findings de código e
  documentação, oito budgets **0/0**, **7.611/1.248 imports**, 25 dependências.
  READMEs atualizados somente pelo renderer oficial.
- Wheels do payload testado: Core
  `ef874bbc619051ca7beda515a7fbd0fa889fe36aeb432d923b993854f1a94c55`;
  Community `dde9f3b70453430c1179373aa060482478860d9604065874c4d67471baf3dc9e`.
  Nenhuma alteração de payload após a prova final; diff check aprovado.

Sem migração neste incremento. A instalação de rollback para binários anteriores
reintroduziria a omissão do gate direto e não está validada como rollout. Nenhuma
mudança em dados reais, runtime do usuário, permissão, release/tag/merge.
`gh auth status` confirma `jpbraga` ativo; autorização de switch registrada, sem
necessidade de usá-la. Este checkpoint corrige a conclusão do Card e o consumidor
de impacto; não conclui a iniciativa. Retomada: contribuições de Cards distintos
com prova real por critério, paginação/globalidade, rollout instalado e as frentes
DEI/ARQ/VER/F2B/KG já listadas acima.

Publicado por push normal em `feature/v0.4.0`: Core
`5755219997eb5128f4fa6c342b2e4cb901c3959e`; Community
`88ad207d4a05301314ac923d4cc287e2725fe344`. `ls-remote` confirmou ambos e as
árvores ficaram limpas. Este registro posterior altera somente o ledger.

### 2026-09-20 — contribuições distintas, herança e prova por critério

Turno anterior classificado como progresso: Core `683a8a79` / Community
`88ad207d`, árvores limpas. `provenance-multicard-baseline.json` confirmou
novamente **801/312 .py** e **866/396 membros** source→wheel→install. Este
incremento acrescenta testes e atualiza o resumo de retomada; nenhum payload de
produção foi alterado. A closure `closure-adopted-report-final.json` continua
correspondendo ao mesmo payload: ok=true, oito budgets 0/0.

Novo `community/tests/test_multicard_delivery_integration.py`: SQLite descartável
com `CommunitySemanticSession`, dois Cards normais com execuções aceitas próprias,
FR com escopos selected_criteria distintos, BR sem link manual herdada apenas
pelo Card de autorização e Test Card com dois cenários. Os resultados são emitidos
pelo `CommunityHttpManifestExecutor` contra endpoints ASGI controlados e assinados
no `CommunityEvidenceLedger`; o verifier de produção os admite. Os recibos de
implementação são dados de entrada aceitos da fixture, não uma execução real de
agente nem uma alegação de validação de autorização da aplicação de pagamento.

Sete casos comprovam a composição dos contratos (AC-INT-01/02, RN-07/09/16,
ADV-12 e recortes DEI-T13/15/19/23/26/27):
- UI concluída não entrega autorização nem BR; a BR aparece no inventário do
  responsável correto, sem criar vínculo direto artificial.
- Ambas as contribuições e ambos os testes autenticados permitem o rollup.
- Falha técnica permanece visível ao lado de teste funcional passing; um novo
  passing pode resolver a pendência sem reescrever o resultado failed.
- Referenciar a implementação de autorização em teste que só observa UI é
  recusado na admissão, sem registro parcial; não ganha crédito por associação.
- Mudança de Target revision ou de escopo declarado invalida apenas a contribuição
  afetada; a prova independente da UI continua satisfeita.
- Um run failed posterior retira o crédito antigo antes de outro binding.
- Dois registros partial não satisfazem FR/BR, mesmo com resultado assinado.
Todos os casos preservam o histórico imutável relevante.

As rodadas iniciais encontraram composição inadequada da fixture (sessão comum
para Card protegido e challenge hash repetido), corrigida sem mexer nos guards.
`community-multicard-scoped.log` teve 2 passed/1 failed: o terceiro caso esperava
leitura incompleta após admissão, mas o sistema corretamente recusava já na
admissão; o teste passou a exigir essa recusa e ausência de registro. Não houve
falha de produto reproduzida nesta matriz. `community-multicard-matrix.log`:
**7 passed**, 22,41 s.

Rodada final, novos processos e imports pareados:
- `core-multicard-final.log`: **62 passed**, 4,82 s; responsabilidades, inventário
  efetivo e cobertura.
- `community-multicard-final.log`: **22 passed**, 51,76 s; sete casos novos,
  contrato conjunto, relatório adotado, rollback/replay/concorrência.
- `frontend-multicard-final.log`: **38 passed**, 3,81 s; rollup e DoD com
  contribuições/critério pendentes. Sem mudança na produção do frontend.
- Diff check aprovado. Sem migração, processo do usuário, release/tag/merge.

Retomada prioritária: implementar o contrato tipado de compatibilidade F2B já
autorizado, resolver a policy preservada por campo e preparar migração em cópia,
com depreciação e sem expor escrita ao executor. A retirada completa de Sprint
depende também de arquivo histórico/ACL, referências, permissões e KG conforme
F2A/F2C/F3; não remover tabelas nem vínculos antes dessas dependências. As demais
frentes e a auditoria integral continuam abertas; os sete casos não encerram
todos os critérios citados nem a iniciativa.

Community publicado por push normal: `d91529ae178347ddebc8b5fe611b0b18be5e63a5`
em `feature/v0.4.0`. O commit companheiro do Core contém somente este ledger.
