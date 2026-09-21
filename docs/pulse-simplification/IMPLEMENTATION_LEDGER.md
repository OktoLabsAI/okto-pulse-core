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

Frente atual: F2A/F2C, com preflight relacional, eventos/jobs, censo de referências
polimórficas e snapshot de recuperação SQLite restaurável implementados. Captura
interna do histórico relacional próprio, referências polimórficas, referências
embutidas em JSON/source_ref e work items relacionados em storage/audit de Board implementada;
consulta pública, reconciliação completa, captura coordenada do KG e cutover ainda
pendentes. Decisão de autoridade de leitura do arquivo registrada ao final. F2B autorizado:
compatibilidade por Card, resolver,
leitura pública, armazenamento nullable e UI estão implementados e testados;
materialização/cutover de dados continuam pendentes de F2A/F2C. A integração de
contribuições distintas e provas por critério foi ampliada com recibos assinados.
Os SHAs e as evidências de cada checkpoint estão nas seções finais.
Continuam pendentes a matriz completa
de permissões/paginação/concorrência, receipt→impacto, reconciliação gravável,
observações bufferizadas, métodos especializados, retomada integral, remoção de
Sprints e sua migração, frentes KG, rollout/rollback instalado, benchmarks e
auditoria requisito a requisito. A compatibilidade F2B tem comentários de
depreciação e não concede escrita ao executor; ainda não há cutover de Sprint.
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

### Em implementação — F2B, contrato de preservação por Card

2026-09-20, partida Core `b00385c5` / Community `d91529ae`, árvores limpas;
turno anterior classificado como progresso. Decisão autorizada relida junto a
BASE F2A/F2B/F2C. O resolver foi extraído para domínio puro, preservando defaults,
null-coalescing independente e a distinção histórica entre `required` ausente
(True no Board) e explicitamente null (fallback False).

Em implementação: contrato fechado e frozen `card-validation-compatibility/v1`,
com IDs históricos opacos e ID de migração, por Card/Board. Guarda somente valores
efetivos que diferem do resultado sem Sprint; campos restantes continuam herdados.
Comentários de depreciação no contrato/resolver/migração, sem warning por leitura.
O planner não grava nem remove vínculos. Compatibilidade malformada, estrangeira
ou coexistente com vínculo ativo de Sprint falha fechado. CardCreate/CardUpdate
rejeitam o campo reservado até quando null; CardResponse o projeta para leitura.

Community adiciona somente coluna JSON nullable, sem backfill; step idempotente
antes de create_all, recusando schema drift. A migração de dados e retirada de
Sprint ainda dependem de arquivo histórico/ACL e cutover coordenado. Não confundir
o teste descartável de representação com esse cutover: nenhum writer público ou
automação de upgrade está autorizado a inventar proveniência ou desprender Cards.

Frontend em integração: thresholds preservados distinguem a origem Card dos
campos Spec/Board; sem controle de edição de policy. Testes novos de domínio,
armazenamento idempotente/drift, duas policies na mesma Spec, DTOs reservados e
CardModal. Primeiro build apontou campo obrigatório ausente na fixture TypeScript;
corrigida a fixture. Preparação do par de wheels/prova antes dos testes Python
ainda em andamento. Nenhum teste de comportamento deste payload foi declarado
aprovado até esta etapa.

Validação do contrato/armazenamento concluída neste checkpoint (F2B parcial):
- Core `domain/task_validation_policy.py`: resolver único extraído sem alterar
  o comportamento legado; planner puramente funcional para diferenças efetivas.
  `schemas.py` expõe a representação somente em CardResponse e rejeita o campo
  em CardCreate/CardUpdate, inclusive null. A leitura pública também recusa
  proveniência estrangeira ou coexistente com Sprint ativo.
- Community `sqlalchemy_models.py`, `relational_schema_steps.py` e ledger de
  migração: coluna nullable idempotente, sem backfill nem desvinculação automática.
  O teste de representação em banco descarta a cópia ao terminar e preserva
  status/validations. O ensaio da migração completa existente também passou
  sobre a fixture física de v0.3.0, com replay e comparação de schema.
- A revisão encontrou caso adicional no planner: scores inválidos iguais antes/
  depois escapavam pelo retorno sem diferenças. `core-f2b-malformed-reproduction.log`
  reproduziu **4 failed** (True, -1, 101, string "70"). O planner agora valida
  o resultado efetivo antigo e preserva diferenças de tipo, sem normalizar dados
  corrompidos. Uma camada inferior inválida mascarada por override válido pode
  continuar mascarada pela preservação fiel; a leitura legada não foi alterada.
- `core-f2b-validated.log`: **110 passed**, 13,53 s; contrato, equivalência,
  defaults/null/False/zero, corrupção, DTOs reservados, ciclo de vida e gates
  byte-a-byte de catálogo/manifest MCP (nenhum artefato MCP manualmente editado).
- `community-f2b-final.log`: **44 passed**, 68,18 s; storage, migração/golden
  upgrade/replay, projeção Card e relações de policy. Após a correção final do
  planner, os quatro testes de storage/equivalência afetados foram repetidos em
  `community-f2b-validated.log`: **4 passed**, 10,61 s.
  A primeira rodada teve 3 failed/31 passed: dois erros de fixture (bind JSON e
  relações async não carregadas) e expectativas de contagem/skips do ledger
  que ainda não incluíam o contrato conjunto anterior e a coluna nova. Corrigidas
  as expectativas exatas: **77 steps** de migração, sem remover checks de schema.
- `frontend-f2b-final.log`: **62 passed**, 38,26 s, após a prova do par instalado;
  thresholds 60/90 na mesma Spec, zero preservado, herança dos demais campos,
  label de origem e ausência de escrita/leitura de Sprint vivo no cenário migrado.
  Build/typecheck aprovado; `frontend-f2b-dist.log`: **78 arquivos**, tree SHA256
  `fa9d601e590c3cfbaeb7981e00dbf072fb1221e3c4f04268db79379373ac7647`.
  `frontend-f2b-lint.log`: zero erros, **402 warnings/baseline 402**.
- `provenance-f2b-validated.json`: **802/312 .py**, **867/396 membros**,
  source→wheel→install idênticos; processos novos, PYTHONPATH pareado. Wheels:
  Core `03c4d3cea96ff65e887baf1b2fa676a67d4aac17432efb1c3a83387a46a3e252`;
  Community `1bad185cde6e80472aefe145b7e5946bf7d08beaf3d05c1bc2f55117b93cbd16`.
- `closure-f2b-validated.json`: **ok=true**, zero findings de código/documentação,
  oito budgets **0/0**, **7.616/1.248 imports**, 25 dependências. READMEs pelo
  renderer oficial; nenhum payload alterado após a prova final.

Limite de rollout: somente upgrade aditivo foi validado. **Não existe ainda
materialização automática nem retirada de Sprint.** Rollback de binário antigo
após preencher a compatibilidade não é seguro por suposição: ignoraria o novo
campo e poderia mudar a policy. Preparar matriz pareada/cópia antes do cutover.
Não houve alteração em banco real, runtime do usuário, contas, permissões, tags
ou release. A representação depreciada só pode ser retirada quando os overrides
deixarem de ser necessários ou houver substituição humana explicitamente autorizada.

Retomada: F2A/F2C — snapshot consistente e arquivo histórico existente sob ACL,
inventário completo de filhos/referências/achados substantivos e fronteiras de
policy; em seguida writer de cutover com lock, equivalência por campo/tipo de
Card, idempotência e rollback, antes das remoções F3. A extração do resolver não
autoriza reclassificar avaliações de Sprint como aprovação de Spec. Preservar as
demais dependências DEI/ARQ/VER/KG e a auditoria integral ainda em aberto.

Publicado por push normal em `feature/v0.4.0`: Core
`ea883c9aae0500acc59474e243ea736ccaa43b03`; Community
`421b73fa8a20854028b5d0cb990e7840d4298499`. `ls-remote` confirmou ambos;
árvores limpas após publicação. Este registro posterior altera somente o ledger.

### 2026-09-20 — autenticação confirmada e investigação F2A retomada

`gh auth status` confirmou `jpbraga` ativa e válida; não foi necessário executar
`gh auth switch`. O usuário autorizou a troca se necessária. `ls-remote`
confirmou Core `5e3d939b13b274193618368a9e20a2e8e95f457f` e Community
`421b73fa8a20854028b5d0cb990e7840d4298499` iguais aos HEADs locais, sem
commits pendentes de push e sem alterações locais na partida deste checkpoint.

Investigação de mecanismos existentes, ainda sem implementação do arquivo:
- `application/use_cases/entity_export.py::GetEntityExportBundleUseCase`
  inicia leitura consistente antes de consultar Board/ACL/permissões. É um
  precedente válido para isolamento da leitura, não uma prova de backup completo.
- `community/adapters/sqlalchemy_entity_export.py` exporta Sprint com seções
  cards, Q&A, avaliações, cenários, regras, history, policy e resources. Cards
  usam projeção de colunas humanas; a definição Sprint não inclui
  `sprint_activation_baselines`. Não usar esse bundle como arquivo integral de
  migração sem reconciliar os dados que não projeta.
- `community/adapters/sqlalchemy_models.py` confirma quatro tabelas diretamente
  próprias: `sprints`, `sprint_history`, `sprint_qa_items` e
  `sprint_activation_baselines`. Além de `cards.sprint_id`, há a origem de hotfix
  em `sprints.origin_sprint_id`/`origin_bug_id`; avaliações e configurações estão
  embutidas em Sprint. Este levantamento ainda não fecha referências polimórficas,
  JSON histórico, filas, analytics ou KG.
- `Attachment.card_id` é obrigatório e sua FK tem CASCADE. O mecanismo atual
  não comporta diretamente arquivo de uma Sprint vazia independente de Card;
  não fabricar Card nem anexar arbitrariamente a um cartão para satisfazer F2A.
- `community/adapters/logical_graph_transfer.py` e
  `logical_transfer_grafx.py` fazem snapshot lógico consistente de Grafx;
  não constituem snapshot relacional ou transação conjunta entre os armazenamentos.

Próximo passo: fechar o inventário de referências/consumidores e o mecanismo
genérico de arquivo sob ACL de Board, incluindo Sprints vazias, retenção e leitura
histórica sem dependência operacional da entidade removida. Só então integrar
snapshot/restauração e cutover F2B/F2C. Nenhuma semântica de gate, dado real,
schema, runtime ou payload distribuído foi alterado nesta investigação.
Não houve nova execução de testes de comportamento; os resultados do checkpoint
F2B anterior permanecem históricos, sem declaração de cobertura de F2A/F2C.

### 2026-09-20 — F2A/F2C, preflight relacional consistente (parcial)

Turno anterior classificado como progresso: investigação registrada/publicada em
Core `25df2ac9`, com fatos que impedem reutilizar o export humano como backup
integral. Árvores limpas na partida; nenhuma mudança de autorização necessária.

Community `adapters/sprint_retirement_inventory.py` implementa preparação interna
sem endpoint, CLI, registro no bootstrap ou writer. O engine é explicitamente
fornecido pelo chamador; não descobre nem abre o banco/runtime do usuário.
Lê Sprints, Cards vinculados, history, Q&A e baselines de todos os Boards numa
única transação. SQLite usa BEGIN físico explícito; o caminho PostgreSQL usa
REPEATABLE READ e READ ONLY. Faz rollback/fecha a conexão inclusive em falhas.

As contagens são integrais; os diagnósticos trazem relação, ID da linha, ID de
destino e motivo (orphan/cross_board/scope_mismatch). `require_valid_relations`
recusa qualquer violação, sem remover, reatribuir ou reparar dados. Schema
incompleto, referência física nova às tabelas aposentadas, coluna não classificada
com nome Sprint e esgotamento do limite agregado de 100.000 linhas interrompem
a inspeção — não devolvem sucesso vazio/truncado. O limite é explícito e pode
ser ajustado pelo chamador interno; não é paginação de produto.

Não exige igualdade entre Spec do Card e da Sprint: regressão cross-spec no mesmo
Board continua representável e sua autorização pertence aos gates existentes.
Membros históricos de baseline não viram FKs atuais de Cards. A inspeção não
reavalia status, decisões ou elegibilidade passada como condição de aprovação.
Nenhuma flag genérica `ready`: relação válida não prova completude do arquivo,
hash de baseline, achados transferidos, ACL, referências polimórficas, filas ou KG.
O cutover deverá repetir o preflight sob seu lock de escrita após arquivamento;
uma leitura de preparação não é autorização nem fence para mutação futura.

Validação desta versão:
- Build dos dois wheels aprovado; `provenance-f2a-inventory.json`: **802/313 .py**,
  **867/397 membros** de payload, source→wheel→install byte a byte, antes dos
  testes. Core SHA256 `03c4d3cea96ff65e887baf1b2fa676a67d4aac17432efb1c3a83387a46a3e252`;
  Community `bbae1446bfbceb2625cfed05894a12f57ffe2580e90c711766ea0918d4718b4f`.
- `community-f2a-inventory.log`: **31 passed**, 34,62 s. Inclui novo preflight,
  storage/equivalência F2B, diagnóstico existente de origem hotfix e adapter de
  baseline. Bancos SQLite físicos descartáveis, PYTHONPATH pareado e processo novo.
  Casos novos: Sprint vazia sem fabricar Card; histórico intacto; Test cross-spec;
  origens/filhos órfãos; cross-board; baseline fora de escopo; 25 órfãos sem amostra
  de 20; extensões físicas com/sem FK; schema ausente; limite/falha e retomada.
  Writer concorrente confirmou uma gravação no WAL entre consultas: a primeira
  leitura não mistura gerações e a próxima transação enxerga a linha nova.
- `closure-f2a-inventory.json`: **ok=true**, zero findings, oito budgets **0/0**,
  **7.616/1.248 imports**, 25 dependências. Ruff dos dois arquivos e diff-check
  aprovados. Nenhuma mudança de frontend ou catálogo MCP neste incremento.
- PostgreSQL não foi executado; o teste real desta transação cobre SQLite/WAL.
  Não declarar restauração de backup nem upgrade/cutover com este teste de leitura.

Investigação adicional que define o próximo passo: `DomainEventRow.payload_json`
e `ConsolidationQueue` guardam referências sem FK. `CardCreated` inclui sprint_id,
enquanto `SprintCreated/Moved/Closed` são eventos próprios; `artifact.archive_changed`
é polimórfico. `ConsolidationEnqueuer` também projeta eventos de Card para Card/Spec.
Não classificar todo payload que menciona Sprint como trabalho supersedido: isso
descartaria fatos de Card que o plano F3 exige preservar. Fechar esse inventário,
os demais subjects polimórficos e arquivo genérico sob ACL antes do cutover.
F2A/F2C/F3 e a auditoria integral da iniciativa continuam em aberto.

Community publicado por push normal em `feature/v0.4.0`, commit `c4ef1ce`;
`ls-remote` confirmou o HEAD remoto. Este checkpoint Core altera somente o ledger.

### 2026-09-20 — F2C/F3, inventário de eventos mistos e trabalho pendente

Turno anterior classificado como progresso: preflight relacional publicado em
Community `c4ef1ce`, ledger Core `b6a7b0c3`. Estado local/remoto limpo na partida.

Core `domain/sprint_retirement_events.py` define classificação histórica pura,
sem import de mecanismo ou registro de handler. Contratos exatos de
`sprint.created/moved/closed` e `artifact.archive_changed` dirigido a Sprint são
elegíveis para supersessão **somente no futuro cutover após arquivamento**.
`card.created` com sprint_id mantém o fato completo de Card; não reescreve payload.
Eventos próprios novos, campos extras em evento supostamente exclusivo e demais
referências estruturadas não classificadas exigem investigação. Mencionar a palavra
Sprint em prosa não apaga um fato. Referências aninhadas têm limite de nós/profundidade.

O planejamento de execuções preserva status done como história. Processing,
status desconhecido ou handler não classificado exige review. Os handlers próprios
conhecidos foram verificados em `ConsolidationEnqueuer`,
`SourceCancellationLifecycleHandler` e `SourceArchiveLifecycleHandler`.
Não converte supersessão em done nem simula um processamento removido.
A fila usa a mesma política pura: alvo Sprint, work_kind conhecido e payload vazio
podem ser aposentados quando pendentes/paused/failed; claimed exige investigação
da operação em voo. Payload com efeitos adicionais não é descartado.

Community `adapters/sprint_retirement_work.py` integra essa política ao preflight
existente, na **mesma transação consistente**, via contratos públicos de domínio.
Inspeciona domain_events, domain_event_handler_executions e consolidation_queue,
com IDs, Board, ação planejada, motivo, refs, handler e status originais.
`work.require_classified_work()` recusa itens review; a verificação relacional
continua separada e nenhuma delas é certificação geral de cutover.
Evento/execution cross-board, execução órfã, schema incompleto, payload inválido
ou orçamento excedido não viram resultado vazio. Históricos, status, tentativas,
erros e payloads permanecem byte a byte no banco descartável do teste.

Limites explícitos: o orçamento de linhas é compartilhado com relações (100.000
por padrão); consultas buscam no máximo o restante + uma sentinela e transferem
lotes de 16 linhas com stream_results. Não aguardar fetchall do driver antes de
verificar o limite. CASE limita cada payload transferido a 131.072 caracteres;
o orçamento agregado UTF-8 é 64 MiB. Excesso falha, sem truncar uma lista de jobs.
Nenhuma nova API/tool/CLI, worker, writer, bootstrap, permissão ou controle de UI.

Validação:
- `core-f2a-work.log`: **103 passed**, 106,27 s; classificadores, contratos reais
  serializados pelos eventos, preservação de Card, drift, limites, handlers e
  suítes existentes de EventBus/dispatcher/projeções/delivery.
- `community-f2a-work.log`: **36 passed**, 47,99 s. Após adicionar streaming e
  LIMIT no driver, rebuild/reinstall/prova repetidos e todos os 36 casos afetados
  repetidos em `community-f2a-work-final.log`: **36 passed**, 43,88 s. Incluem
  casos anteriores de relações/F2B/WAL e novos eventos mistos/exclusivos, execução
  em voo/desconhecida/concluída, cross-board, órfão, orçamento e payload falsey
  malformado (False/0/[]/string vazia não tratados como objeto vazio).
- `provenance-f2a-work-final.json`: **803/314 .py**, **868/398 membros**, par
  source→wheel→install idêntico antes da rodada final, PYTHONPATH pareado e processos
  novos. Wheels em `wheels-f2a-work-final`: Core SHA256
  `b3ab9edf42ff01a49b045cf4cd9ed05d325ff531f6596228e873196c6dc34788`;
  Community `4a14f0254896429e07cbf89ca5bcbe750f1581ff920943c5ef3942020af53ab5`.
- Ruff passou após corrigir a importação da fixture compartilhada de teste;
  nenhum lint foi dispensado. Sem mudança de frontend neste incremento.
  O caminho PostgreSQL continua sem execução real; a evidência física é SQLite/WAL.

Limites de conclusão: ainda não há escrita de supersessão, arquivo histórico,
backup/restauração ou cutover. Não marcar jobs reais, remover tabelas nem chamar
esta classificação de migração concluída. A auditoria completa dos complementos
DEI/ARQ/VER/KG e demais fases permanece pendente.

Próximo passo — referências polimórficas e arquivo: o levantamento estático
encontrou families semantic_subject_*, semantic_guideline_*, quality_*, tombstones,
canonical_debt, DLQ/audit, Global Discovery, knowledge bases e refs de código.
Não generalizar por sufixos: `policy_compliance_receipts` usa **entity_type com
subject_id**, portanto uma busca por pares homogêneos type/id o perderia. Esse
levantamento localiza os próximos contratos; não prova que todos foram revisados.
Reconciliar esses históricos e seus filhos antes de fechar o formato genérico de
arquivo sob ACL de Board e proceder à transformação atômica F2B/F2C/F3.

Auditoria final `closure-f2a-work-validated.json`: **ok=true**, findings de código
e documentação vazios, oito budgets **0/0**, **7.618/1.249 imports**, 25 dependências.
A primeira rodada acusou somente drift da matriz README (contagens antigas);
os dois fragmentos foram regenerados pelo renderer oficial e o par reconstruído.
Nenhum payload mudou após a prova final. Nenhuma operação em banco/runtime real.

Publicado em `feature/v0.4.0` por push normal: Core `5ab2a8af`, Community
`c1b2004`; HEADs confirmados em `ls-remote`, árvores limpas após publicação.
Este apontamento posterior é somente de ledger.

### 2026-09-20 — F2A/F2C, referências polimórficas e filhos de histórico

Turno anterior classificado como progresso: Core `5ab2a8af`/ledger `8849325a`,
Community `c1b2004`, eventos/jobs publicados. Partida com árvores limpas.

Community `adapters/sprint_retirement_references.py` adiciona censo interno à
mesma transação do preflight: **44 seletores de raiz e 9 caminhos de filhos,
51 tabelas**. Mapeamento explícito de discriminador, ID e Board, incluindo
`policy_compliance_receipts.entity_type/subject_id`, famílias semantic_guideline,
policies/waivers, KBs, auditoria/DLQ/Discovery, code references e candidatos quality.
Filhos por recibo/waiver incluem revisões adotadas, eventos de waiver, perguntas/
links/outbox quality, revogações e heads de investigação. O censo guarda chaves
primárias físicas completas, inclusive compostas, papel da referência, origem
opaca e Boards proprietário/referenciado separados. Não copia payload para outro
Board nem concede acesso ao source; não é uma resposta pública de produto.

KBs e links sem board_id derivam proprietário do pai relacional explícito. Board
ausente ou referência a Sprint de outro Board exige investigação; não reatribui
propriedade. Source já ausente permanece registrado como proveniência opaca,
sem fabricar Sprint. `require_resolved_scopes()` verifica resolução de escopo,
não validade/autoridade integral do histórico ou autorização de cutover.
Uma mesma linha pode aparecer sob mais de um papel: o futuro arquivo deverá
deduplicar por tabela/PK, preservando a linha original uma única vez.

A investigação refinou a premissa sobre quality: `quality_findings` tem CHECK
que só admite ideation/refinement/spec como sujeito e âncora. Não estender esse
contrato para Sprint. O teste físico confirma a recusa; se uma base legada/driftada
contiver tal linha, o censo marca `unsupported_subject_contract` para investigação.
Não se pode deduzir que todas as famílias com coluna subject_type admitem Sprint.

Bug reproduzido durante revisão do incremento: JOIN de filhos com seus pais
ocultava filho órfão. `f2a-reference-orphan-reproduction.log`: **1 failed**, 7,10 s,
porque a inspeção não recusava uma revisão adotada com recibo ausente. Corrigido
com validação prévia da FK física, incluindo todas as colunas das FKs compostas,
e diagnóstico com a PK do filho. Ausência/drift da relação também falha fechado.
Vínculos opcionais null seguem MATCH SIMPLE; não os inventariar como órfãos.
O teste de evento de waiver demonstra pai existente/mesmo Board versus Board
diferente, preservando a chave e o Board efetivamente observado no caso inválido.

Todas as consultas de dados mantêm streaming de 16 linhas e limite restante +
sentinela. O orçamento é compartilhado com relações/eventos/jobs; contagens por
papel são leituras de referência, não uma afirmação de linhas físicas distintas.
Schema incompleto, nova coluna discriminadora sem classificação e excesso de
limite interrompem a inspeção em vez de devolver censo parcial como completo.
Sem novo endpoint, tool, CLI, permissão, scheduler ou writer de histórico.

Evidência desta versão:
- `community-f2a-references.log`: primeira rodada **44 passed**, 59,62 s.
  Após reprodução/correção do órfão e casos adicionais de FK composta:
  `community-f2a-references-final.log`: **47 passed**, 64,07 s, cobrindo referências,
  relações, jobs/eventos e armazenamento/paridade F2B em SQLite descartável.
  Dump antes/depois confirma ausência de mutação no cenário de recibos/filhos.
  Fixtures de linkage não certificam hashes/autoria de guideline nem emitem
  aprovação real; o foco é censo e preservação dos registros já armazenados.
- Build pareado e `provenance-f2a-references-final.json` antes da rodada final:
  **803/315 .py**, **868/399 membros**, source→wheel→install idênticos, PYTHONPATH
  pareado e processo novo. Core SHA256
  `b3ab9edf42ff01a49b045cf4cd9ed05d325ff531f6596228e873196c6dc34788`;
  Community `aabd22a7d61b2e65efd60b4e6b148e455abe31c8bae985726c669a700671d742`.
- `closure-f2a-references-final.json`: **ok=true**, zero findings de código/
  documentação, oito budgets **0/0**, **7.618/1.249 imports**, 25 dependências.
  Ruff e diff-check aprovados. Core de produção, frontend e MCP não mudaram;
  não repetir testes históricos do Core como se executados neste turno.

Limites: isso não é ainda arquivo, snapshot restaurável ou fechamento transitivo
de todo JSON/source ref/KG. O manifesto cobre os seletores declarados; não prova
por si só semântica, hashes ou completude de todas as autoridades relacionadas.
PostgreSQL não foi exercitado. Nenhum banco/runtime real foi tocado.

Próximo passo: materializar o arquivo genérico com origem, IDs, contagens e hashes,
reusando armazenamento/audit de forma protegida por Board e preservando as
fronteiras de leitura existentes; validar snapshot consistente e restauração
descartável antes do writer de cutover. Completar source refs/JSON e projeções KG
nessa reconciliação. F2A/F2C/F3 e a iniciativa completa continuam em andamento.

Community publicado por push normal em `feature/v0.4.0`, commit `3e1c1b3`;
HEAD confirmado em `ls-remote`. Este checkpoint Core altera somente o ledger.

### 2026-09-20 — F2A, snapshot relacional consistente e restauração isolada

Partida: Core `a251645c`, Community `3e1c1b3`, árvores limpas. Incremento anterior
classificado como progresso. Community `adapters/relational_recovery_snapshot.py`
implementa mecanismo interno de recuperação SQLite, sem CLI, rota, startup hook
ou acesso ao runtime real. Não é o arquivo histórico consultável por Board:
o banco completo pode conter vários Boards e credenciais e exige diretório de
recuperação protegido pelo operador, nunca servido pelas rotas de attachments.

Captura usa a API de backup do SQLite com transação de leitura efetivamente
iniciada antes da cópia e do censo, preservando o estado commitado do WAL mesmo
com escrita concorrente. Artefato standalone em journal_mode DELETE, manifesto
`relational-recovery-snapshot/v1`, ID, origem, SHA256 do banco e schema, contagens
por tabela, user_version e application_id. Compara fatos na origem pinada e no
destino, verifica integrity_check, faz flush/fsync e publica staging sob lock.
O digest esperado do manifesto deve ser retido pelo chamador confiável, nunca
obtido do mesmo artefato cuja integridade está sendo verificada.

Restauração valida manifesto/banco/censo e publica exclusivamente um arquivo novo
via hard link no mesmo filesystem. Recusa alvo existente, sidecars WAL/SHM/journal,
IDs de snapshot existentes e aliases de filesystem na origem, ancestralidade e
lockfile. Falha antes da publicação limpa somente staging contido no diretório
explicitamente fornecido. Não sobrescreve banco ativo, não reinicia processos,
não executa downgrade de wheel e não altera permissões do produto.

Evidências em `PULSE_REFACTOR/.validation-v040`:
- Primeira rodada `community-f2a-recovery.log`: **57 passed**, 62,83 s. Após
  proteção adicional do lockfile contra symlink, rebuild/reinstall e nova prova:
  `community-f2a-recovery-final.log`: **59 passed**, 64,74 s, nenhum skip.
  Cobertura: WAL, IDs/valores/blobs, índices/views/triggers/FKs, gravação concorrente,
  adulteração de manifesto/banco, interrupção/retry, colisão de publicação, sidecars,
  travessia de caminho e aliases. Restauração do schema atual completo reproduz
  exatamente o preflight relacional/eventos/jobs/referências da fixture original.
- `provenance-f2a-recovery-final.json`: **803/316 .py**, **868/400 membros**,
  source→wheel→install byte a byte idênticos, verificados antes dos testes finais,
  PYTHONPATH pareado e processos novos. Core wheel SHA256
  `b3ab9edf42ff01a49b045cf4cd9ed05d325ff531f6596228e873196c6dc34788`;
  Community `f4eddd6b98d806a38ef98ae105d1ecacff227249fd43478e055a917348649163`.
- `closure-f2a-recovery-final.json`: **ok=true**, findings de código/documentação
  vazios, oito budgets **0/0**, **7.618/1.249 imports**, 25 dependências. Ruff e
  diff-check aprovados. Sem mudança em Core de produção, frontend ou MCP; não há
  testes frontend novos a atribuir a este incremento interno.

Limites: integridade de recuperação não certifica autoridade, FKs válidas na
origem nem prontidão de migração. Não captura arquivos de attachments ou Grafx,
não é backup coordenado de todo o ambiente e não foi exercitado em PostgreSQL.
Cutover terá de adquirir fence de escrita e vincular backup, preflight e arquivo
à mesma operação; a função de snapshot não fornece esse fence. O teste de falha
é interrupção controlada antes da publicação, não simulação de perda de energia.
Filesystem sem hard link falha sem sobrescrever o alvo. Todas as fixtures são
descartáveis; nenhum dado/runtime real alterado.

Investigação adicional para o próximo incremento: `Attachment.card_id` é
obrigatório e CASCADE; não atende Sprint vazia sem fabricar Card. O adapter
`CommunityAuditRepository` é específico de consolidação KG, incluindo undo e
purge_by_board; não o tratar como arquivo histórico genérico imutável. O log
`DomainEventRow` é Board-scoped e usado por dispatcher: sua reutilização exige
reconciliar leitura, retenção e efeitos antes de escolher formato. Não foi tomada
decisão de expor backup de ambiente como histórico de produto.

Próximo passo: arquivo genérico de histórico com origem opaca, IDs, contagens,
hashes e ACL de Board, preservando fronteiras de leitura; completar source refs
JSON/KG, coordenar fence/snapshot e só então writer de transformação/cutover
F2B/F2C/F3 com retomada/idempotência. Demais requisitos DEI/ARQ/VER/KG e auditoria
integral permanecem pendentes; a iniciativa continua em andamento.

Community publicado por push normal em `feature/v0.4.0`, commit `bf1d88d`.
Este checkpoint Core altera somente o ledger.

### Decisão F2A pendente — leitura do arquivo sem permissões Sprint ativas

**Atualização 2026-09-20:** proposta autorizada pelo usuário com “sim”. A pendência
de decisão está resolvida; permanece a implementação e sua prova. Ver “Decisão
F2A — autorização recebida em 2026-09-20” ao fim deste ledger. A investigação e
os limites originais abaixo continuam como base do contrato autorizado.

Fato investigado em 2026-09-20: `GetEntityExportBundleUseCase` exige
`sprint.entity.read` na raiz. `sqlalchemy_entity_export.py` distingue Q&A,
avaliações e histórico com `sprint.qa.read`, `sprint.evaluations.read` e
`sprint.history_read`. O arquivo bruto inclui todas essas seções e não pode ser
entregue integralmente apenas por `board.read`. F3 exige retirar permissões
operacionais Sprint; F2B.6 proíbe ampliar autoridade na migração.

Reprodução `f2a-archive-authority-complete-flags.log`, com flags completas todas
negadas e grants explícitos: Board-only não lê nenhuma seção Sprint;
entity-only não lê Q&A/avaliações/histórico; entity+Q&A não lê avaliações/histórico.
Primeira tentativa com documentos esparsos (`f2a-archive-authority-reproduction.log`)
demonstrou a compatibilidade histórica absent=True de `PermissionSet.has`, não
Board-only. Não usar aquela tentativa como evidência de negação nem alterar esse
fallback silenciosamente; a preservação deve comparar decisões efetivas, não
somente copiar keys presentes.

Proposta para decisão: introduzir permissões genéricas de leitura de arquivo por
seção (identidade/conteúdo, Q&A, avaliações, histórico), com grants de migração
limitados à origem arquivada e ao Board correspondente. Materializar somente as
decisões efetivas anteriores, incluindo negações; exigir também ACL atual de
Board. Nenhuma permissão de executar/avaliar Sprint vira permissão de escrita,
avaliação de Spec ou acesso a arquivos de outras origens. Novas concessões ficam
no fluxo autorizado de administração; permissões operacionais Sprint saem em F3.
Não manter um serviço Legacy Sprint. Os nomes finais de leaves/escopos devem ser
fechados com testes de não ampliação, revogação e presets/overrides antes do corte.

Alternativa rejeitada sem autorização: servir arquivo completo por board.read,
que amplia acesso para os três casos reproduzidos. Restringir tudo a owner também
não preserva acesso dos leitores atuais. Esta é uma decisão de autoridade, nos
termos da política de investigação do pacote, não somente escolha de storage.
A consulta pública e a migração de grants ficam isoladas; captura, integridade,
reconciliação e demais frentes podem prosseguir. Até decisão e implementação
verificada, o verificador do arquivo é somente interno à migração.

As seções de Cards, cenários e regras no export atual também exigem, respectivamente,
`card.entity.read`, `spec.tests.read` e `spec.rules.read`; essas autoridades
remanescentes devem continuar aplicadas. A proposta não substitui todo o contrato
de disclosure por quatro flags nem autoriza exposição integral de rows SQL.

### 2026-09-20 — F2A, captura histórica relacional em storage/audit existente

Partida: Core `92f3926a`, Community `bf1d88d`, árvores limpas; turno anterior foi
progresso. Community `adapters/sprint_retirement_archive.py` adiciona captura
interna SQLite no storage de Board existente, com um `DomainEventRow` genérico
`historical_archive.created` por Board. Nenhuma entidade Legacy Sprint, novo
handler, endpoint, CLI, grant ou startup hook. IDs originais são dados opacos no
arquivo; a referência de auditoria tem somente FK de Board, sem FK para Sprint
ou Card. Board erasure continua dono da retenção no storage existente.

Captura todas as colunas físicas de `sprints`, `sprint_history`, `sprint_qa_items`
e `sprint_activation_baselines`, com PK/schema e vínculos anteriores de Cards.
Formato `historical-relational-archive/v1`, origem, Board, migração, contagens por
tabela, tamanho e SHA256. Células SQL tipadas preservam texto JSON original,
Unicode decomposto, CRLF, null, blobs e inteiros além da precisão de JSON/JS;
não aplica normalização canônica de domínio à evidência. Budget agregado de
linhas/bytes falha fechado; query de tamanho rejeita row SQL grande antes de
transferência para Python. Dados são separados fisicamente por Board.

`BEGIN IMMEDIATE` mantém reserva de escrita SQLite durante preflight, captura,
save/verificação de blobs e commit da auditoria. Replay determinístico do mesmo
migration_id exige mesma população de Boards e mesmo conteúdo; conteúdo alterado,
origem desaparecida ou blob adulterado não são recapturados silenciosamente.
Falha anterior à tentativa de commit reverte todas as referências e tenta limpar
somente blobs desta operação. Commit de resultado incerto retém blobs para
reconciliação, evitando apagar conteúdo que pode ter sido commitado. Blobs sem
referência commitada não têm rota de produto; falha de cleanup pode deixá-los para
reconciliação operacional. Não há afirmação de transação distribuída com storage.

Teste prova que arquivo continua verificável depois de a origem ser removida
em fixture, sem exigir uma Sprint operacional. Captura não muda Card, parecer,
estado ou Q&A; não dá aprovação a Spec. Préflight de relações e escopos precisa
passar antes de qualquer save. Work items desconhecidos ainda podem ser preservados
sem processamento; o gate de classificação permanece obrigatório antes de cutover.

Evidências em `PULSE_REFACTOR/.validation-v040`:
- `community-f2a-archive.log`: **55 passed**, 81,09 s, com captura, inventário,
  eventos/jobs e referências. Casos incluem Sprint vazia, dois Boards, autoria,
  respostas null, avaliação rejeitada, replay/população, adulteração, falhas de
  storage e INSERT de auditoria, rollback/retry, limites, órfão, escritor
  concorrente recusado durante save e tipos físicos adicionais preservados.
- Nova asserção de leitura independente da origem e rodada focada final em
  `community-f2a-archive-final.log`: **12 passed**, 25,49 s (subconjunto dos 55,
  não somar como 67 testes distintos).
- `provenance-f2a-archive-final.json`: **803/317 .py**, **868/401 membros**,
  source→wheel→install idênticos, PYTHONPATH pareado, processos novos. SHA256
  Core wheel `fb25e3f62af61b2ca694d6342efd09db8b8dd89c449011b39a9e766ac5e854ce`;
  Community `94f66e8fbaf827bbaaaad900addd17c76ec512c5c527aa4257451a384190e9c3`.
  A reconstrução final mudou somente metadados README; payload Community mantém
  `93cfd3b86affa9fc39e9dea4f5327fba728651f74c8d2f2959932efd40f634b2`.
- Primeira closure: sem findings de código, budgets zero; somente matriz README
  com contagem anterior. Ambos fragmentos regenerados pelo renderer oficial;
  nova contagem **7.618/1.250 imports**, 25 dependências. Ruff e diff-check passam.
  `closure-f2a-archive-final.json`: **ok=true**, findings de código/documentação
  vazios e todos os oito budgets **0/0**, verificados no par final reconstruído.

Limites: ainda não é o arquivo completo exigido por F2A. Não copia bytes de
attachments existentes, referências polimórficas/JSON externas, projeções Grafx
ou toda a trilha de permissões; esses dados permanecem intactos nas fontes atuais.
Não há reader público autorizado, cutover, remoção de tabelas ou mecanismo de
transferência de perguntas/achados substantivos. A captura usa o log append-only
existente e verificação de hash; a integração final deve fechar proteção,
disclosure, retenção e reconciliação antes de declarar o arquivo suficiente para
remover fontes. PostgreSQL e perda de energia não foram exercitados. Frontend e
MCP não mudaram. Nenhuma operação em banco/runtime real.

Próximo passo: resolver a decisão de autoridade acima; continuar independentemente
com reconciliação das referências/JSON/KG e conteúdo substantivo, preservação de
permissões efetivas e integração transacional de F2B/F2C/F3. Iniciativa permanece
em andamento e requer a auditoria integral de todos os documentos.

Community publicado por push normal em `feature/v0.4.0`, commit `8db7093`.
Core deste checkpoint altera ledger e matriz README gerada, sem código de domínio.
A pergunta sobre autoridade de leitura foi enviada; nenhuma resposta/aceite foi
inferido por passagem de tempo. Somente essa decisão e suas mutações dependentes
ficam pendentes; não há bloqueio da iniciativa inteira.

### 2026-09-20 — F2A, arquivo de referências e trabalho durável relacionado

Partida: Core `456a8385`, Community `8db7093`, árvores limpas. Turno anterior
classificado como progresso; a decisão de leitura pública continua pendente,
sem inferir autorização pelo tempo. Este incremento não altera permissões.

Community `sprint_retirement_archive.py` passa a produzir
`historical-relational-archive/v2`. Além das quatro tabelas próprias e vínculos
de Cards, copia as linhas físicas selecionadas pelo censo de referências
polimórficas/filhos e pelo inventário de eventos/jobs na mesma transação reservada.
Deduplica por tabela e PK completa, guarda todas as colunas sem decodificar/
normalizar JSON SQL e registra todos os papéis/proveniências da linha. A mesma
fila detectada por artifact_type e durable_work entra uma única vez, com ambos
os papéis. As decisões preserve/supersede/review são fatos do plano de migração,
não processamento nem novo estado dos jobs.

Leitura parametrizada em lotes de 16 chaves, incluindo chaves compostas, com
streaming, limite de bytes antes da transferência de payload e conferência de
todas as chaves esperadas. Não faz uma query por linha nem interpola IDs no SQL.
O budget agregado contabiliza dados e metadados; excesso não publica arquivo
parcial. Contagens/hash abrangem todas as tabelas incluídas e a lista de papéis.
Row ausente ou proprietário divergente entre plano e leitura falha fechado.

O Board proprietário observado no censo define o arquivo. Um Board que só tem
referências históricas a uma origem já removida recebe seu próprio arquivo, sem
Sprint fabricada; não transferir a linha ao Board da origem. KBs sem board_id
mantêm o proprietário derivado da Spec/entidade pai. Referência cross-board
inválida impede captura antes do storage. Conteúdos, authors, hashes, estados e
payloads originais permanecem inalterados nas tabelas vivas.

Verificador continua lendo v1 com suas contagens históricas e cobertura limitada;
não o promove a v2. Writer só gera v2. Replay de migration_id anteriormente
associado a conteúdo/formato diferente falha, preservando a evidência existente.
Nenhuma API/UI pública, handler ou serviço Legacy Sprint foi acrescentado.

Evidências desta versão em `PULSE_REFACTOR/.validation-v040`:
- `community-f2a-related-archive.log`: **62 passed**, 90,29 s. Inclui os testes
  de arquivo, preflight relacional, referências e eventos/jobs. Novos casos:
  recibo + filho de PK composta sem copiar sujeito Spec não relacionado;
  fila com dois papéis e evento misto + execução pending sem processamento;
  Board sem Sprint ativa, origem histórica ausente, alteração de recibo que
  invalida replay, overflow incluindo payload relacionado, compatibilidade v1,
  19 recibos além do lote inicial + KB com autoria/Unicode/CRLF preservados e
  referência cross-board recusada antes de qualquer arquivo/evento novo.
  Fixtures de recibos demonstram preservação/linkage, não validade semântica
  dos digests de guideline nem aprovação de conteúdo por serviços de domínio.
- `provenance-f2a-related-archive.json`: **803/317 .py**, **868/401 membros**,
  source→wheel→install idênticos antes dos testes, PYTHONPATH pareado, processos
  novos. Core wheel SHA256
  `fb25e3f62af61b2ca694d6342efd09db8b8dd89c449011b39a9e766ac5e854ce`;
  Community `d85778cf19213ee1a0e33cee9afdd7f512b2e38869b670fdd5ace20ab340b6b3`.
- `closure-f2a-related-archive.json`: **ok=true**, findings de código e docs
  vazios, oito budgets **0/0**, **7.618/1.250 imports**, 25 dependências. Ruff e
  diff-check aprovados. Sem mudanças em código Core, frontend ou catálogo MCP.

Limites: a seleção ainda é o censo declarado de raízes/filhos e eventos/jobs,
não fechamento transitivo universal de JSON, todos os pais relacionados, Grafx
ou arquivos externos. Não interpretar cópia integral de uma row como autorização
para divulgá-la integralmente. As permissões específicas de cada família e seção
continuam necessárias na futura leitura pública. Conteúdo substantivo não foi
transferido automaticamente para Spec/Card; fontes vivas não foram apagadas.
PostgreSQL e runtime real não foram tocados. F2A/F2C/F3 ainda não concluídos.

Investigação que orienta o próximo passo: `QualityEvidenceRefInput` e `EvidenceRef`
admitem source_type/source_id/source_version/content_hash dentro de evidence_refs.
O sujeito de uma finding pode ser Spec e a referência embutida ter origem Sprint;
um censo filtrado apenas pelo discriminador da row não cobre esse caminho. Isso
é uma lacuna de cobertura identificada por leitura de contrato, não reprodução
de um bug no runtime real. Inspecionar esses payloads/owners e source_ref textual
com limites e localização exata, sem substituir referências por busca de texto
ou modificar evidência assinada. Completar essa reconciliação, Grafx e conteúdo
substantivo antes do writer de cutover, mantendo a decisão de leitura isolada.

Community publicado por push normal em `feature/v0.4.0`, commit `c5c866f`.
Core deste checkpoint altera somente o ledger. A iniciativa continua ativa.

### 2026-09-20 — F2A, referências embutidas com localização e proprietário

Partida: Core `beb3dc48`, Community `c5c866f`, árvores limpas. Turno anterior
classificado como progresso. A decisão de leitura pública continua pendente;
nenhuma nova permissão ou concessão foi aplicada.

Core `domain/sprint_retirement_embedded.py` adiciona detector puro de identidades
explícitas: sprint_id/origin_sprint_id/source_sprint_id; pares artifact/subject/
entity/source type+id, incluindo entity_type/subject_id; valores históricos e
filtros de field=sprint_id; source_ref/evidence_ref e arrays correspondentes.
Guarda caminho exato (keys e índices), ID opaco e hint de Board separado. Texto
comum não vira referência por conter a palavra Sprint ou prefixo em título.
Limites de 5.000 nós e profundidade 32 falham sem retornar resultado parcial.
Detector não valida hash/autoridade/currentness, não transforma conteúdo e não
confere crédito a evidência. Classificação antiga de eventos não foi alterada.

Community `adapters/sprint_retirement_embedded.py` integra inspeção à mesma
transação de preflight: JSON físico, campos JSON declarados no ORM mesmo quando
legados em TEXT e colunas textuais source_ref/membership_source_ref/evidence_ref.
Cada localização contém tabela, PK física, coluna, caminho, origem e escopo.
Streaming de 16 linhas, budget de leitura compartilhado com os outros censos,
1 MiB por célula e 64 MiB agregados; tamanho validado no SQL antes de materializar.
JSON inválido, chaves duplicadas, não finitos, IDs malformados e estrutura acima
do limite impedem uma inspeção aparentemente completa.

Proprietário vem de board_id, Board.id ou relações de pai explicitamente
catalogadas. Para referências encontradas em filhos, a FK física correspondente
também deve existir, incluindo pares compostos. Board declarado no payload é
hint de referência, nunca substitui proprietário. Owner ausente/desconhecido,
hint para outro Board ou origem atual em outro Board exigem investigação.
Registros globais não são copiados para o Board da Sprint por suposição.

Arquivo passa a `historical-relational-archive/v3`, incluindo linhas localizadas
por referências embutidas e todos os seus papéis. Linhas próprias já capturadas
(por exemplo sprint_history) não são duplicadas. Conteúdo JSON SQL original e
autoria permanecem intactos. Verificador mantém v1/v2 com sua cobertura anterior;
não declara esses formatos como se já contivessem o novo censo. Captura exige
escopos embutidos resolvidos antes de qualquer save. Sem rota, frontend ou MCP.

Investigação/testes falhos, preservados como evidência: primeira rodada
`community-f2a-embedded.log` teve **8 failed, 66 passed**, 125,28 s. As oito falhas
ocorreram na preparação da fixture: `ck_spec_source_context` recusou manifest sem
origem de refinamento válida. Não relaxar esse CHECK. As fixtures foram corrigidas
para payload histórico de `kg_cognitive_sources`, e um teste negativo confirma
que o CHECK original continua ativo. O teste de domínio adicional usa o contrato
real `EvidenceRef` para demonstrar que source_type Sprint é representável;
fixtures de payload físico não afirmam admissão de uma nova conclusão cognitiva.

Validação final em `PULSE_REFACTOR/.validation-v040`:
- `core-f2a-embedded-final.log`: **50 passed**, 4,29 s; detector + regressão da
  classificação de eventos. A primeira rodada Core havia passado 49 testes.
- `community-f2a-embedded-final.log`: **93 passed**, 162,18 s; censo embutido,
  arquivos v1/v2/v3, inventário relacional, referências, eventos/jobs e snapshot/
  restauração. Cobre dump imutável antes/depois do censo, raw JSON preservado,
  replay, histórico próprio sem duplicação, KB com parent ownership, dois Boards,
  owner global desconhecido, 25 referências além da primeira página, JSON
  ambíguo/malformado, limite de célula/budget compartilhado, source_ref textual,
  drift da FK do pai e CHECK original. Não somar as rodadas como casos distintos.
- `provenance-f2a-embedded-final.json`: **804/318 .py**, **869/402 membros**,
  source→wheel→install byte a byte, antes da rodada final, PYTHONPATH pareado e
  processos novos. SHA256 Core wheel
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `56c9f68a16882c616340181ba728eaf02b05e69e8cdfbe7173f4d4d851b6ff45`.
- Closure inicial acusou apenas contagem antiga das matrizes README. Fragments
  regenerados pelo renderer oficial, par reconstruído e reinstalado.
  `closure-f2a-embedded-final.json`: **ok=true**, findings de código/docs vazios,
  oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências. Ruff e diff-check
  aprovados. Nenhuma implementação concreta foi adicionada ao Core.

Limites: detector cobre as formas declaradas acima, não interpreta todo texto,
URL arbitrária, JSON serializado dentro de prosa ou representação desconhecida
como vínculo. Isso não é autorização para apagar fontes, reescrever evidência
assinada ou declarar toda a iniciativa concluída. Ainda faltam reconciliação
completa de pais/autoridades, arquivos externos, projeções Grafx, coordenação de
backup/cutover, destino do conteúdo substantivo e leitor público autorizado.
PostgreSQL e ambiente real não foram exercitados. Frontend não mudou.

Próximo passo independente da decisão de leitura: revisar a captura/projeção
Grafx e seu vínculo com snapshot relacional sob fence, além de reconciliar
pendências substantivas sem novas aprovações. Integrar F2B/F2C/F3 e concluir os
demais requisitos DEI/ARQ/VER/KG com auditoria integral continuam necessários.
Community publicado em `feature/v0.4.0`, commit `9456bd5`; Core inclui o detector
puro, testes, matriz README gerada e este ledger. A iniciativa permanece ativa.

### 2026-09-20 — F2A/KG §8, janela offline e investigação da captura conjunta

Partida: Core `872238db`, Community `9456bd5`, árvores limpas. Turno anterior
classificado como progresso. Autenticação GitHub validada como `jpbraga`; não foi
necessário executar a troca de conta autorizada. Decisão F2A de leitura pública
continua pendente, sem alteração de permissões.

Investigação: `CommunityGrafxLogicalSnapshotSource` abre uma transação read MVCC
e mantém um snapshot consistente daquele grafo. `backup_logical_graph_file`
fecha esse snapshot antes da publicação; o publisher existente usa replace e
pode substituir um destino anterior. Portanto, ainda faltam identidade imutável
do conjunto, publicação sem overwrite e coordenação entre os stores para chamar
isso de backup de migração do ambiente. `graph_operation_guards` drena pins do
processo, não escritores nativos externos. A recuperação/quarentena existente
usa o mutex de aquisição do serve-lock para excluir novos runtimes.

Premissa descartada com reprodução real: `database.begin("write")` no Grafx não
é um fence de escrita. A lease é adquirida no commit, e outra transação consegue
commitar enquanto aquela continua aberta. O teste novo também mantém um read
snapshot antigo, vazio, e demonstra que uma leitura posterior vê o nó novo.
Isso é comportamento esperado de MVCC; não é bug do Grafx. Inspeção do pacote
instalado confirmou `okto-grafx 0.0.7` e a documentação de `Database.begin`.
O código local do Grafx foi consultado somente para investigação, sem alteração.
Não usar uma transação write vazia para alegar exclusão conjunta SQL+grafo.

Community `adapters/migration_runtime_fence.py` adiciona `offline_migration_window`
para a futura composição interna do instalador. Recebe 1–8 diretórios explícitos,
absolutos e existentes; rejeita aliases/junctions/reparse points de ancestralidade
e dos arquivos de lock, além de segmentos `..`. Deduplica os roots e adquire os
mutexes existentes em ordem canônica. Só admite o corpo depois de adquirir todos
e inspecionar seus owners, mantendo todos os mutexes até o fim. Reutiliza a regra
existente: PID vivo, mesmo com heartbeat antigo, heartbeat fresco de PID morto e
payload ilegível impedem entrada. Um servidor do próprio processo também impede
entrada. Registro comprovadamente antigo/morto é preservado, nunca apagado pelo
helper. Falha no segundo mutex ou no corpo libera todos os já adquiridos.

Esta janela cobre início de runtimes que respeitam o protocolo Community. Não
drena ferramentas CLI que já passaram por um preflight pontual, nem impede um
writer nativo que ignora esse protocolo. Não é uma transação SQL, uma lease
Grafx ou certificado de captura conjunta. O chamador futuro deverá identificar
todos os DATA_DIR/KG_BASE_DIR efetivos, excluir esses demais participantes e
manter captura/verificação/cutover autorizado dentro da janela. Nenhum hook de
startup, endpoint, comando público, parada de processo ou migração real foi
adicionado. A ausência de integração é uma pendência explícita, não um gate verde
de F2A/F2D/KG §8.

Validação em `PULSE_REFACTOR/.validation-v040`:
- `community-f2a-fence.log`: **48 passed**, 18,08 s; novo módulo e regressão de
  `serve_lock`. Inclui processos Python separados tentando iniciar nos dois
  roots durante a janela e conseguindo iniciar depois; owner vivo no próprio
  processo; heartbeat velho/vivo e fresco/morto; JSON/PID inválidos; contenção
  no segundo mutex; exceção no corpo; roots inválidos/ausentes; aliases de root,
  owner e mutex; reprodução Grafx real. Nenhum teste foi pulado.
- Antes dos testes, par reconstruído/reinstalado e comprovado por
  `provenance-f2a-fence.json`: **804/319 .py**, **869/403 membros** de payload,
  source→wheel→install byte a byte, PYTHONPATH pareado e processos novos.
  Core wheel SHA256 `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `1ab7974d58136957485994e3501918653387581f4b52a6b826b3227b53878dc1`.
- `closure-f2a-fence.json`: **ok=true**, findings de código/documentação vazios,
  oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências. Nenhuma mudança
  de matriz README necessária. Ruff e diff-check aprovados. Sem mudança de UI,
  de modo que não há teste frontend novo aplicável neste incremento.

Community commit `57cbbc9`, `feat(migration): hold startup fences across offline
recovery windows`. Core deste checkpoint altera somente este ledger. Próximo
passo: vincular snapshots SQL/Grafx e identidades/gerações ao manifest de backup,
provar consistência conjunta e restauração sem promoção nem overwrite, cobrindo
escritores que não são excluídos pelo mutex de startup; então integrar ao fluxo
interno resumível. Continuam pendentes conteúdo substantivo, reader autorizado,
cutover F2B/F2C/F3 e o restante da matriz DEI/ARQ/VER/KG. Iniciativa ativa; este
turno é progresso, não conclusão.

### 2026-09-20 — F2A/KG §8, conjunto de recuperação SQL/Grafx consistente

Partida: Core `af923ff1`, Community `57cbbc9`, árvores limpas. Turno anterior
classificado como progresso. Sem resposta adicional à decisão F2A de leitura
pública; esta frente independente não altera permissões nem leitores públicos.

Community `adapters/joint_recovery_snapshot.py` implementa captura, verificação
e restauração de um conjunto **explicitamente selecionado** de SQLite e grafos
Grafx Board/global_discovery. Não presume que a seleção representa todos os
Boards, bindings e arquivos do ambiente. API exclusivamente interna, sem rota,
CLI, bootstrap automático ou alteração de dados reais.

Consistência: mantém a janela offline de startup e uma conexão SQLite `mode=rw`
com `BEGIN IMMEDIATE`. Antes de qualquer export, coleta UUID e LSN publicado de
todos os handles Grafx. Exporta SQLite pela API de backup, incluindo WAL, e cada
grafo pelo codec lógico existente. Depois de **todos** os exports, obtém novas
views públicas de `database.transactions`, verifica recovery_required e compara
os UUIDs/LSNs. Não reutiliza a view imutável inicial como se fosse leitura atual.
Qualquer alteração impede a publicação do conjunto, inclusive ABA: valores que
mudam e voltam ao anterior continuam tendo LSN diferente. A reserva SQL impede
escritas no relacional durante esse intervalo. Com identidades estáveis e os
LSNs monotônicos inalterados, os stores selecionados têm um intervalo comum de
captura; isso não afirma transação distribuída ou quiescência para cutover.

O manifesto `joint-recovery-snapshot/v1` registra contrato de captura, par exato
de revisões/wheels informado pelo gate de proveniência do instalador, versão
Grafx, hashes relacionais, seleção de escopos/Boards, paths observados, UUID/LSN,
hashes dos arquivos lógicos e certificados (schema, contagens, fingerprint e
checksum). `RecoveryBuildPair` exige hashes fechados; o mecanismo não inventa a
proveniência dos binários a partir desses valores. Composição futura deve chamar
o gate real antes de fornecê-los. Limites: 256 grafos, manifesto 1 MiB, batch de
1–5.000, deadline verificado entre operações. Isso não é cancelamento preemptivo
de uma chamada nativa longa. Roots explícitos/absolutos, aliases e segmentos `..`
recusados, paths de sidecars SQLite checados, identidade de escopo/database
duplicada recusada. Nenhum handle Grafx do chamador é fechado ou promovido.

Publicação usa diretório parcial privado e mutex do root de recuperação, com
verificação de destino inexistente antes do rename; IDs existentes não são
reutilizados. Esse protocolo serializa os publishers cooperantes no diretório
protegido do operador, não escritores arbitrários de filesystem que o violem.
Falha deixa o conjunto sem publicação e limpa o staging contido. Artefatos são
verificados pelo digest retido pelo chamador; nunca se obtém o hash esperado do
próprio arquivo. Arquivos de grafo têm nomes gerados e fechados no verificador,
sem aceitar traversal trazido pelo manifesto.

Restauração verifica o conjunto, exige o par de builds registrado e usa um novo
diretório inteiro. SQL recuperado não recebe in-place overwrite. Grafos são
restaurados em gerações novas pelo sink existente, com checkpoint/reabertura
fria/certificação e comparação dos fingerprints/contagens/schema. UUID e LSN
nativos novos são esperados: recuperação lógica não transplanta o commit log.
Falha no segundo grafo limpa também o primeiro candidato ainda não publicado.
Não há troca de binding, reinício de runtime ou promoção automática.

Validação em `PULSE_REFACTOR/.validation-v040`:
- Primeira rodada `community-f2a-joint.log`: **44 passed**, 152,32 s.
- Após ampliar cobertura e exigir o par na restauração,
  `community-f2a-joint-final.log`: **46 passed**, 172,60 s. Inclui conjunto real
  com os dois esquemas Grafx, campos/vetores tipados e source_artifact_ref Sprint
  preservado; histórico SQL em WAL com bytes exatos; contenção de writer SQL;
  commit antes do primeiro export, depois de um grafo já exportado, ABA e commit
  de **outro processo Python**; nova captura após recusa; falha no segundo
  export/restore; adulteração de manifesto/grafo/SQL; traversal com digest
  estruturalmente confiado; recusa de par diferente e de destinos existentes.
  Inclui regressões dos snapshots SQLite e da janela offline. Não somar as
  rodadas como casos distintos. Nenhum teste pulado ou falho.
- Par final reconstruído/reinstalado antes dos testes:
  `provenance-f2a-joint-final.json`, **804/320 .py**, **869/404 membros**, todos
  source→wheel→install byte a byte; PYTHONPATH pareado, processos novos.
  Core SHA256 `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `0fb1cb7e29ec97c035a401b7b480851dc89854ec4da38454773d740ac8638d5a`.
- `closure-f2a-joint-final.json`: **ok=true**, findings de código/documentação
  vazios, oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências. Ruff e
  diff-check aprovados. Core não recebeu mecanismo concreto. Nenhuma mudança
  de frontend, MCP ou matriz README neste incremento.

Limites e retomada: falta reconciliar a seleção com o censo completo do ambiente,
autenticar os bindings/gerações e preservar arquivos externos. O algoritmo não
certifica trocas externas de diretórios/bindings nem coerência de negócio entre
stores (por exemplo outbox pendente). Não autoriza usar um snapshot já capturado
como fence de futuras mutações. Cutover ainda exige impedir escritores e binários
incompatíveis, relacionar o backup ao intent/audit, revalidar as fontes e coordenar
rollback compatível. Esta etapa não altera fontes Sprint ou dá destino automático
a conteúdo substantivo. PostgreSQL e ambiente real não foram exercitados.

Investigação para o próximo passo: `CommunityGraphBackendBindingStore` dispõe de
`inspect_board_binding`/`inspect_global_binding`, que autenticam o documento sem
exigir storage existente; `acquire_*` acrescenta a exigência de storage físico.
Ambos são leitores, sem inicialização automática. Usar essas evidências para
reconciliar Boards/bindings/gerações sob a janela, recusando ausência/drift sem
criá-los para fazer o censo passar. Ainda pendentes integração ao instalador,
conteúdo substantivo, leitor autorizado, cutover F2B/F2C/F3 e toda a matriz restante
DEI/ARQ/VER/KG. Community commit `1d59a86`; Core deste checkpoint altera o ledger.
Iniciativa ativa; progresso, não conclusão.

### 2026-09-20 — F2A/KG §8, seleção de recuperação reconciliada com routing

Partida: Core `b84c5403`, Community `1d59a86`, árvores limpas. Turno anterior
classificado como progresso. A decisão F2A de leitura pública continua pendente;
nenhuma permissão, concessão ou rota foi alterada.

Investigação de premissa: o resolver inicializa uma rota ausente apenas pela
operação explícita `_initialize_missing`, com callback de criação física. Seus
leitores e o store de bindings distinguem ausência de binding de uma rota
autenticada. Não transformar ausência de binding em corrupção por suposição,
nem inicializar grafos vazios para fazer um censo passar. O store pode inspecionar
um binding cujo storage está ausente; o preflight deve registrar essa diferença.

Community `adapters/recovery_graph_inventory.py` lê a tabela física `boards`
dentro da transação SQLite do chamador, exigindo PK id sem drift. Reconciliam-se
todos os IDs com `boards/<id>` e o escopo global no KG root explícito. Diretorias
de Board sem proprietário relacional, storage sem binding, geração ativa ausente
ou sem `grafx.meta`, backend vinculado incompatível, aliases e binding corrompido
impedem aceitar a seleção como resolvida. Ausência de binding e de geração/storage
fica explicitamente registrada como `binding_absent_storage_absent`; não é sinal
genérico de prontidão da migração. Arquivos de lock conhecidos não viram dados de
grafo nem são removidos. Nenhum router/init/CAS é chamado pelo censo.

Cada rota vinculada registra Board/escopo, geração, path físico, page size,
digest autenticado do binding e SHA256 observado do arquivo de identidade nativo.
O hash do arquivo detecta drift observado; o censo não interpreta seu conteúdo
como uma nova implementação do formato Grafx. Leitura de identidade limitada a
1 MiB, budget agregado padrão de 100.000 entradas (IDs e entradas de diretório),
sem truncar população silenciosamente. Gerações não selecionadas e outros paths
de storage são listados separadamente como **conteúdo ainda a preservar**; suas
bytes não são incluídas apenas porque seu path apareceu no censo.

`create_joint_recovery_snapshot(..., kg_base_dir=...)` agora exige que o root KG
também esteja na janela de startup. Antes da captura, sob a reserva SQL, exige
igualdade exata entre rotas ativas e seleção (escopo, Board, path, page size), sem
omissões, duplicação ou troca de dono. Após todos os exports/LSNs, relê o censo e
recusa drift. Gera `joint-recovery-snapshot/v2` com `routing_inventory` autenticado
pelo manifesto. Verificação offline valida a estrutura/população e a cobertura
pelos registros de grafo sem reabrir paths vivos. Restauração suporta v1/v2;
captura de seleção explícita sem esse argumento permanece v1 com sua cobertura
anterior, não recebe retroativamente a garantia nova. A futura composição do
instalador ainda deve exigir o caminho com censo, em vez de omitir o argumento.

O mecanismo detecta mudanças observadas antes/depois; não é um lock de todos os
writers de binding/filesystem, nem prova que nunca houve uma troca ABA externa de
diretório/binding. A exclusão desses mutadores no cutover segue pendente. Tampouco
um digest de identidade substitui admissão nativa/validação dos handles: exports
continuam usando os adapters Grafx e seus checks. Não promover nem descartar
automaticamente gerações inativas, dados órfãos ou conteúdo substantivo.

Validação em `PULSE_REFACTOR/.validation-v040`:
- `community-f2a-routing.log`: **63 passed**, 237,50 s; censo, snapshots conjuntos,
  reserva SQLite e janela offline. Inclui arquivo/dump SQL intactos após o censo,
  ausência sem criação, paths de retenção, seleção omitida/duplicada/page size
  divergente, owner órfão, storage não vinculado, geração/identidade ausentes,
  binding adulterado, budget compartilhado, drift de PK, aliases e manifesto que
  omite Board. Fixtures físicas de censo usam placeholders explicitamente sem
  abri-los como Grafx; integração usa grafos reais nos paths canônicos.
- Integração v2 captura e restaura Board+global e mantém Board sem grafo ausente;
  recusa troca real por CAS para uma geração g2 admitida, storage novo durante
  export e alteração da observação do hash de identidade. Este último teste
  injeta o digest observado; não corrompe um banco ativo para testar comparação.
  Regressões v1, LSN/ABA/commit de outro processo e restauração permanecem verdes.
  Nenhum teste falho ou pulado.
- `provenance-f2a-routing.json`: par reconstruído/reinstalado antes dos testes,
  **804/321 .py**, **869/405 membros**, source→wheel→install byte a byte, PYTHONPATH
  pareado e processos novos. Core wheel SHA256
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `f1fb36ad9d2b3603e378792b77d31fb37c11ed0cb058d9121cda8281939360b0`.
- `closure-f2a-routing.json`: **ok=true**, findings de código/docs vazios, oito
  budgets **0/0**, **7.619/1.251 imports**, 25 dependências. Ruff/diff-check
  aprovados. Sem novo mecanismo no Core, mudança de UI/MCP ou matriz README.

Próxima frente independente: preservar os arquivos externos e controles de
lifecycle no conjunto de recuperação. Leitura de `CommunityFileSystemStorage`
confirmou namespace por Board, mutexes em `.board_lifecycle/<hash>.lock` e marcador
`.erased`, usado por save/restore/purge para impedir recriação após apagamento.
Backup/rollback não pode ignorar esses controles ou reviver conteúdo apagado.
Ainda é necessário identificar todos os consumidores/paths, coordenar o fence
com SQL e distinguir prova de conteúdo de mera listagem, antes de implementar
a preservação. Não inferir que a enumeração do root KG cobre uploads separados.

Continuam pendentes arquivos/gerações não selecionadas, exclusão completa de
writers/binding changes, integração interna do instalador e rollback, conteúdo
substantivo, leitor autorizado, F2B/F2C/F3 e demais requisitos DEI/ARQ/VER/KG.
Community commit `4988f58`; Core deste checkpoint altera somente este ledger.
Dados reais e PostgreSQL não foram exercitados. Iniciativa ativa; progresso,
não conclusão.

### 2026-09-20 — F2A, recuperação física de uploads com autoridade de apagamento

Partida: Core `594e9772`, Community `4988f58`, árvores limpas. Turno anterior
classificado como progresso. A decisão de leitura pública do arquivo histórico
continua pendente, sem novas permissões ou concessões.

Investigação: `AttachmentService.upload_attachment` salva pelo StorageProvider
antes de adicionar a row relacional e compensa em caso de falha. O adapter de
arquivo histórico também salva seus blobs pelo mesmo provider. `Attachment.path`
e `historical_archive.created.payload_json.storage_path` são referências físicas;
copiar apenas as rows de attachments omite os blobs históricos e objetos ainda
sem referência. A composição usa `settings.upload_dir`, que pode ser separado do
KG root. O provider possui layout plano por Board e controles `.board_lifecycle`:
mutex por SHA256 do ID e marcador `.erased` cuja existência bloqueia save/restore.

Community `adapters/storage_recovery_snapshot.py` implementa captura, verificação
e restauração **isoladas do storage**, formato `community-storage-recovery/v1`.
Recebe roots explícitos e os IDs relacionais do chamador; a composição futura
deve obtê-los sob reserva SQL. Além desses IDs, cerca namespaces físicos e hashes
de lifecycle observados, inclusive controles opacos de Boards já ausentes. Usa
os paths de lock do provider existente, sem novo protocolo concorrente. Novos
namespaces antes da aquisição completa impedem uma captura com fence parcial.
Somente sidecars de mutex podem ser criados na origem; objetos não são alterados.

Captura todos os objetos do namespace plano, incluindo arquivos sem referência,
sem inventar proprietário relacional ou visibilidade de produto. Copia flags de
apagamento, mas não transplanta arquivos `.lock`. Namespace com marker de erasure
e diretório residual impede a captura; não copiar nem apagar silenciosamente esse
resíduo. Layout desconhecido/nested, entradas especiais e aliases/junctions são
recusados. Os hashes por objeto e o manifesto são autenticados pelo digest retido
pelo chamador. Streaming de 1 MiB, limite padrão de 100.000 objetos/16 GiB e
manifesto 16 MiB; crescimento além do tamanho observado é recusado antes de
escrever o chunk excedente. Deadline verificado nos chunks e entre operações,
com tempo restante compartilhado na aquisição de locks; não é cancelamento
preemptivo de qualquer chamada de filesystem. Releitura de namespace, stat e
hashes detecta mutações observadas de writers que ignoram o protocolo, sem alegar
exclusão universal desses writers.

Restaura somente em novo root, com os mesmos nomes, bytes e mtime, sem substituir
destino existente. Exige o storage ATUAL no mesmo path autoritativo registrado
no snapshot; apontar para um root vazio alternativo não contorna a proteção.
Qualquer erasure posterior ao snapshot recusa a restauração inteira, e a mudança
de flags durante a cópia impede sua publicação. Os locks de lifecycle ficam
retidos até a publicação desse componente. Flags já capturadas continuam a
impedir save no storage restaurado. Referências SQL absolutas não são reescritas;
este componente não produz sozinho um runtime relocado pronto para servir.
Tudo permanece em diretório protegido do operador, sem API/CLI/reader público.

Revisão Windows: nomes de controle/markers são reconhecidos segundo a semântica
de case do sistema, preservando seus nomes físicos. IDs declarados que colidem
por case e namespace observado com case divergente do ID conhecido são recusados;
não inferir que dois IDs lógicos são o mesmo Board porque o filesystem os aliasa.
Esse tratamento recebeu regressões locais específicas, incluindo apagamento
posterior com diretório de controle em maiúsculas. Não afirmar bug em ambiente
real: a revisão e as reproduções usam apenas fixtures descartáveis.

Validação em `PULSE_REFACTOR/.validation-v040`:
- Primeira rodada `community-f2a-storage.log`: **36 passed**, 59,13 s.
- Após revisão Windows/deadline, `community-f2a-storage-final.log`: **39 passed**,
  60,27 s, incluindo regressões de compensação e download pelo provider. Cobre
  bytes binários/Unicode/CRLF, blobs históricos, objeto sem referência, markers,
  mtime, não sobrescrita, save real em outro processo bloqueado durante captura e
  liberado depois, erasure posterior, root falso, resíduo de erasure, adulteração,
  falha na segunda cópia com cleanup/liberação, mudança de origem, marker novo
  durante restore, limites, aliases, traversal de manifesto e case no Windows.
  Nenhum teste falho ou pulado. Não somar rodadas como casos distintos.
- `provenance-f2a-storage-final.json`: par reconstruído/reinstalado antes dos
  testes, **804/322 .py**, **869/406 membros**, source→wheel→install byte a byte,
  PYTHONPATH pareado e processos novos. Core wheel SHA256
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `8e293787fbc0e6fa32ca0237417bf64f2492682c0ba5f6632401e270832be60e`.
- `closure-f2a-storage-final.json`: **ok=true**, findings de código/docs vazios,
  oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências. Ruff e diff-check
  aprovados. Sem mudança de UI/MCP/README ou novo mecanismo concreto no Core.

Integração ainda pendente: incorporar este componente ao conjunto SQL/Grafx sob
a reserva SQL e o intervalo de LSNs estáveis, reconciliando attachments e blobs
históricos com owners/paths. Na restauração conjunta, verificar erasures atuais
ANTES de copiar SQL/objetos e manter o guard de lifecycle até publicar **todo** o
conjunto. Não basta chamar o restore de arquivos, liberar seus locks, restaurar
grafos e depois publicar: isso reabriria uma janela para erasure posterior. Será
necessária composição do guard com a publicação externa, sem reacquirir locks
não reentrantes nem manter um lockfile de staging aberto durante rename no
Windows. Preservar paths/proveniência sem reescrever audit histórico por conveniência.

Gerações KG inativas/arquivos fora desse provider, retenção dos backups, exclusão
completa de writers, integração do instalador/rollback, conteúdo substantivo,
leitor autorizado, F2B/F2C/F3 e demais requisitos DEI/ARQ/VER/KG continuam pendentes.
Community commit `78b4ab8`; Core deste checkpoint altera o ledger. Dados reais,
PostgreSQL e runtime relocado não foram exercitados. Iniciativa ativa; progresso,
não conclusão.

### 2026-09-20 — F2A, recuperação conjunta SQL/Grafx/uploads e guard de apagamento

Partida: Core `0f586d04`, Community `78b4ab80`, árvores limpas. Turno anterior
classificado como progresso. Autenticação GitHub confirmada na conta `jpbraga`;
não foi necessário usar a troca de conta autorizada pelo usuário. A decisão de
autoridade do leitor histórico continua pendente e isolada desta frente.

O coordenador interno `joint_recovery_snapshot` aceita o root de uploads
explicitamente, exige o censo de roteamento KG e produz
`joint-recovery-snapshot/v3`. Captura os arquivos usando os IDs de Boards obtidos
sob a reserva SQLite, ainda dentro do intervalo de LSNs Grafx estáveis. Verifica
o artefato de storage e repete os checks de LSN/roteamento antes de publicar o
conjunto. O manifesto autentica o digest do componente de storage e seu verificador
exige a mesma população de Boards do censo. v1/v2 mantêm sua cobertura anterior;
não recebem retroativamente a garantia de uploads.

A restauração v3 exige o root ATUAL de storage. A janela de lifecycle recusa
apagamentos posteriores antes de criar o staging ou reconstruir SQL. O guard
permanece válido e seus locks de origem continuam retidos durante SQL, grafos,
arquivos e a publicação FINAL do diretório conjunto. Revalida markers/namespace
antes de publicar. Não há aquisição duplicada de locks não reentrantes nem
lockfile aberto dentro do staging que impeça o rename no Windows. O guard expira
ao sair do contexto e não pode ser reutilizado após liberar a exclusão.

O restore isolado de storage usa a mesma janela. Revisão identificou que a
recusa de destino dentro da origem precisava ocorrer antes de criar o mutex do
publicador: caso contrário, a chamada inválida poderia poluir o namespace com
um arquivo de lock. A verificação antecipada cobre tanto uploads quanto o
próprio artefato de backup, com testes de igualdade dos arquivos antes/depois.
O restore conjunto também rejeita essa sobreposição antes de criar seu mutex.

Validação em `PULSE_REFACTOR/.validation-v040`:
- Rodada anterior à correção adicional de sobreposição:
  `community-f2a-joint-storage.log`, **108 passed**, 365,61 s. Inclui recuperação
  conjunta/storage/censo KG/SQLite/janela offline e regressões de compensação e
  download de attachments. Nenhuma falha ou skip.
- Rodada final `community-f2a-joint-storage-final.log`: **46 passed**, 324,37 s,
  joint/storage incluindo as duas novas regressões de sobreposição. Nenhuma falha
  ou skip. Não contar as duas rodadas como populações distintas.
- `provenance-f2a-joint-storage-final.json`: par reconstruído/reinstalado após a
  última mudança de produção, **804/322 .py**, **869/406 membros**, comparação
  source→wheel→install byte a byte, PYTHONPATH pareado e processos novos.
  Core wheel SHA256
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `2da9bc8e4fc0f42ad858c3fc4325accca1fb1c940a078314b47cbd14772d3ca9`.
- `closure-f2a-joint-storage-final.json`: **ok=true**, findings de código/docs
  vazios, oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências.
  Ruff/diff-check aprovados. Sem mudança de UI/MCP ou mecanismo concreto no Core.

Os testes conjuntos exercitam grafos reais, SQLite e bytes de uploads. Um writer
SQL concorrente é bloqueado durante a captura de storage; commit Grafx durante
essa captura recusa o backup inteiro. Restore verifica SQL/ambos os grafos/bytes
e markers. No instante da publicação externa, um segundo processo tenta apagar
o Board pelo provider real e fica bloqueado; o rename final funciona no Windows
com locks de ORIGEM retidos. Erasure posterior recusa antes da cópia SQL. Injeção
de marker por um writer que ignora o protocolo, após a cópia de arquivos, impede
a publicação de todo o conjunto e remove o staging privado.

Próxima lacuna: reconciliar as referências relacionais com os objetos físicos.
Leitura confirmou `AttachmentService` em `core/services/main.py`: save precede
staging SQL, delete físico precede delete relacional e ambos possuem compensação.
Portanto, o fence SQL sozinho não prova ausência de arquivo faltante ou de save
ainda sem referência. O censo deve cruzar `attachments.path/size`, dono via
`attachments.card_id → cards.board_id`, e eventos `historical_archive.created`
com `payload_json.storage_path/sha256/size` e Board do evento. Validar o schema/FKs
real, limites agregados e ownership sem inferir dono para arquivos sem referência.
O provider atual retorna paths absolutos; não resolver paths legados relativos
contra cwd por conveniência. Esta rodada preserva objetos históricos como bytes,
mas sua fixture conjunta ainda não reconcilia rows de attachment/archive com eles.

Não há reescrita de paths SQL absolutos, runtime relocado pronto para servir,
promoção de bindings, migração real ou API pública nova. Exclusão de todos os
writers/binding changes, gerações inativas/outros arquivos, retenção dos backups,
integração interna do instalador/rollback, conteúdo substantivo, leitor autorizado,
F2B/F2C/F3 e demais requisitos BASE/KG/DEI/ARQ/VER continuam pendentes. Iniciativa
ativa; este checkpoint é progresso, não conclusão.

Community commit `d0b9336411552fb9ebd5579d72a75d7d247d3495`; Core deste checkpoint
altera somente este ledger. Ambos serão enviados à `feature/v0.4.0`, sem release
ou merge. A próxima retomada começa pela reconciliação SQL/objetos descrita acima.

### 2026-09-20 — F2A, referências SQL reconciliadas com uploads preservados

Partida: Core `336af08c`, Community `d0b9336`, árvores limpas; turno anterior
classificado como progresso verificado. Sem mudança da decisão pendente de
autoridade do leitor histórico.

Community `adapters/recovery_storage_references.py` cruza os dois consumidores
confirmados: `attachments` (owner via Card/Board) e eventos
`historical_archive.created` (owner do evento). Exige transação relacional do
chamador, tabelas físicas, PKs e FKs esperadas. Usa LEFT JOIN para não apagar
órfãos do resultado e recusa owner ausente, população de Boards divergente,
paths relativos/fora do root/nested/de outro Board, objeto ausente, tamanho
divergente e hash histórico divergente. Não repara rows ou bytes para passar.

O manifesto do storage é autenticado/verificado antes do cruzamento. Paths são
comparados lexicalmente ao root registrado; o verificador não abre a origem
viva nem resolve paths relativos contra cwd. Attachments não possuem hash SQL:
o certificado registra o hash observado no objeto preservado, sem inventar um
hash histórico autoritativo. Arquivos históricos possuem hash e tamanho na row
do evento, que precisam coincidir com o objeto. Valida os formatos históricos
v1/v2/v3 e shape fechado do payload, rejeitando chaves duplicadas/non-finite e
tipos inválidos. O hash do texto JSON bruto participa do certificado; não há
reescrita/canonicalização das rows históricas armazenadas.

Limites compartilhados de 100.000 rows (Boards/referências), 64 MiB de campos
selecionados e 1 MiB por célula. Tamanhos e contagens são medidos em SQL antes
da materialização; leitura iterada e deadline cooperativo. Não é cancelamento
preemptivo de uma consulta SQL ou syscall. Arquivos sem referência continuam
copiados: certificado separado de contagem/hash, sem conceder owner/ACL. Markers
de lifecycle não são classificados como objetos de conteúdo sem referência.

Captura com uploads agora produz `joint-recovery-snapshot/v4`, com certificado
compacto `relational-storage-reconciliation/v1`. Executa o cruzamento sob a mesma
reserva SQL e intervalo de LSNs estáveis. Verificação offline reabre somente a
cópia SQL autenticada, em modo read-only/immutable, e recalcula o certificado
contra o storage preservado. Compara serialização tipada, distinguindo `true`
de `1`. v1/v2/v3 continuam legíveis com suas garantias anteriores; a futura
composição do instalador deve exigir a evidência vigente, sem omitir uploads ou
aceitar formato antigo como se tivesse a garantia nova.

Validação em `PULSE_REFACTOR/.validation-v040`:
- `provenance-f2a-storage-references.json`: par reconstruído/reinstalado após a
  última mudança de produção; **804/323 .py**, **869/407 membros**, fontes/wheels/
  install byte a byte, PYTHONPATH pareado e processos novos. Core wheel SHA256
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `be17bdd5d09b25b5573c14f490625a3fb3fbf199fd726bbde69208f6abc438d7`.
- Testes: `test_recovery_storage_references`, `test_joint_recovery_snapshot`,
  `test_storage_recovery_snapshot`, `test_sprint_retirement_archive`.
  `community-f2a-storage-references.log`: **94 passed**, 384,94 s, nenhuma falha
  ou skip. Inclui origem offline, schema/writer histórico reais, igualdade SQL
  antes/depois, owners órfãos/cross-Board, paths ilegítimos/ausentes, hash/tamanho,
  payload fechado, limites, drift físico de FK/tabela, certificado adulterado
  (inclusive boolean no lugar de count), leitura v3 compatível e recusa de todo
  o conjunto v4 quando referências divergem. Guards de apagamento continuam verdes.
- `closure-f2a-storage-references.json`: **ok=true**, findings de código/docs
  vazios, oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências.
  Ruff/diff-check aprovados.

As fixtures de reconciliação distinguem consistência física de semântica do
conteúdo. A integração de schema completo usa o writer real do arquivo histórico
e um attachment real; os cenários mínimos usam blobs opacos para isolar paths,
hashes, limites e owners. O certificado não substitui a validação substantiva do
arquivo histórico, sua imutabilidade nem autorização de leitura por seção.

Continuam pendentes exclusão completa de writers/bindings, preservação de gerações
KG inativas/outros arquivos, retenção dos backups, integração interna do instalador
e rollback, conteúdo substantivo, leitor autorizado, F2B/F2C/F3 e demais requisitos
BASE/KG/DEI/ARQ/VER. Sem alteração de UI/MCP ou permissão, migração de dados reais,
promoção de bindings ou runtime relocado pronto. Iniciativa ativa; progresso,
não conclusão.

Investigação para a próxima frente: `CommunityGraphBackendBindingStore` publica
inicialização e CAS sob `FileLock(<binding>.lock)` em `_publish_initial` e
`_publish_compare_and_swap`; a inicialização cria o diretório pai antes do lock.
Portanto, compor exclusão de publicação requer considerar tanto rotas existentes
quanto ausentes e evitar criar uma rota vazia apenas para tomar um lock. Essa
leitura não prova exclusão dos writers nativos ou substituição arbitrária de
diretórios. Revalidar guards/lifecycle e consumidores antes de escolher a janela
de cutover; não promover a detecção atual de drift a uma garantia de exclusão.

Community commit `1c754d8d1c12325eb7b5cef2c452d05d077976cd`; Core deste checkpoint
altera apenas este ledger. Pushes normais à `feature/v0.4.0`, sem release/merge.

### 2026-09-20 — F2/KG, exclusão de publicação de bindings durante captura

Partida: Core `2da053d8`, Community `1c754d8`, árvores limpas. Turno anterior
classificado como progresso verificado. A iniciativa permanece integralmente
ativa; não confundir esta exclusão parcial com cutover completo.

Investigação de consumers: `graph_operation_guards` usa condições/locks de
thread, portanto drena somente participantes do processo. `Database.coordinator`
do Grafx instalado retorna uma view imutável; o backup físico nativo usa um
fence interno durante sua própria captura, não fornece ao Pulse uma janela
pública reutilizável de cutover. Nenhuma importação de internals Grafx ou Core
foi adicionada para simular essa capacidade. A CLI atual também possui preflight
pontual de serve-lock; isso sozinho não cerca uma operação já iniciada.

Leitura de `grafx_board_privacy_storage_present` confirmou que sidecars de
binding dentro de um Board são resíduo físico observado. Criar `<binding>.lock`
em cada Board ausente apenas para obter exclusão mudaria a ausência de privacidade.
A solução local é `CommunityGraphBackendBindingStore.publication_window`, com
um mutex na raiz KG (`.graph-binding-publication.lock`), sem criar diretórios de
Board/global, binding ou banco. Raiz ausente e alias do mutex são recusados.

Inicialização e CAS agora entram nessa janela antes da seção de publicação
existente, mantendo o lock por binding, admissão e digest esperado. O lock de
raiz serializa apenas essas publicações curtas; não envolve queries/transações
normais nem construção de candidatos. Raízes KG distintas continuam independentes.
Um coordenador interno pode manter a janela e publicar pelo MESMO store/thread:
reentrância do próprio lock, sem pular checks de CAS/admissão. Outro store não
herda a exclusão. Falha no corpo libera o lock; contenção tem erro tipado.

A captura conjunta obtém primeiro a reserva SQL e depois a janela de publicação,
mantida até a publicação final do artefato. O censo ignora o mutex de raiz como
controle, sem omitir storage de negócio. Ainda repete inventário e LSNs: writers
nativos, apagamento físico e substituição arbitrária não são cercados por esse
mutex. Um teste que antes observava CAS durante export agora exige sua recusa
antes de publicar e verifica que o binding original permanece; não se relaxou
o gate de drift. A prova nova corresponde à implementação atual; formatos de
artefatos antigos não passam a atestar retroativamente essa janela.

Validação em `PULSE_REFACTOR/.validation-v040`:
- Primeira rodada `community-f2a-binding-publication.log`: **112 passed,
  1 failed, 1 skipped**, 422,63 s. A falha era a expectativa do teste de que o
  sidecar vazio continuaria no disco: FileLock no Windows pode removê-lo ao
  liberar. Teste ajustado para aceitar ausência ou somente esse sidecar vazio.
  O skip é o teste legado de roteamento já removido, não prova de Grafx atual.
- Suíte inicial: janela de publicação, CAS, censo KG, recuperação conjunta,
  resolver, startup e lifecycle roteado. Não somar rodadas como casos distintos.
- Segunda rodada `community-f2a-binding-publication-final.log`: **77 passed,
  1 skipped**, 195,06 s, zero falhas. Reporta explicitamente o skip de
  `test_global_legacy_binding_stays_on_anchor_across_pointer_cutovers`, cujo
  backend foi retirado do Community. Sem novos skips ou gates relaxados.
- `provenance-f2a-binding-publication-checked.json`: par reconstruído/reinstalado
  após o último ajuste do censo, **804/323 .py**, **869/407 membros**, comparação
  source→wheel→install byte a byte, PYTHONPATH pareado e processos novos.
  Core wheel SHA256
  `cb870d60059e5c4308e4fab09e1a84cefc69fb558e2591502337bbd1fd58eadf`;
  Community `773204c009d1f59e49bd46de94c1ab3b8b3d932a92c71ec4ef21bbb9b0a9defd`.
- `community-f2a-binding-publication-checked.log`: **19 passed**, 53,72 s,
  nenhuma falha ou skip; censo, colisões e integração de captura conjunta após
  a última mudança.
- `closure-f2a-binding-publication-checked.json`: **ok=true**, findings de
  código/docs vazios, oito budgets **0/0**, **7.619/1.251 imports**, 25 dependências.
- Ruff/diff-check aprovados. Sem alteração de UI/MCP ou nova mecânica no Core.

Revisão com `reproduce_binding_mutex_collision.py` em diretório temporário
confirmou que o FileLock instalado pode remover um arquivo com conteúdo existente
no path escolhido para o mutex. Não houve acesso a dados reais. A entrada agora
recusa arquivo não vazio ou diretório nessa posição antes de obter o lock;
regressões preservam exatamente os bytes (inclusive um único byte) e o diretório.
Não depender de kwargs recentes do FileLock ausentes no mínimo declarado do
pacote (`>=3.16,<4`); a solução usa a API já suportada.
O próprio censo também recusa conteúdo/diretório no nome reservado do mutex;
não pode omitir esses bytes tratando-os como controle. Só um sidecar vazio é
reconhecido como tal. A mudança final recebeu três regressões específicas.

Testes novos usam processos reais para lock/publicação, com stubs de identidade
explicitamente sem abrir engine nos cenários unitários de binding. A integração
conjunta usa Grafx real e verifica exclusão imediatamente antes/depois do rename
final, além de liberação ao retornar. Testa ausência de Board/global e ausência
de resíduo de privacidade, init/CAS de Board e global, raízes independentes,
reentrância sem aceitar digest stale, falhas, raiz inexistente e aliases.

Próxima investigação: combinar a participação de processos/entrypoints e os
mutadores físicos com essa exclusão. A janela atual não protege contra binários
anteriores que desconhecem o mutex, CLI já passada pelo preflight ou erasure
física; não autoriza avançar cutover com essas lacunas. Também permanecem gerações
inativas/outros arquivos, retenção/rollback/instalador, conteúdo substantivo e
leitor histórico autorizado, F2B/F2C/F3 e demais BASE/KG/DEI/ARQ/VER. Não há
migração de dados reais, parada do runtime ou publicação de interface de manutenção.

Community commit `f3897db267eacc7aa6ebc6cf1c3b7c738cca39e1`; Core deste checkpoint
atualiza somente este ledger. Pushes normais à `feature/v0.4.0`, sem release/merge.
Próxima retomada: participação de entrypoints durante toda a operação e mutadores
físicos, preservando os fences de autoridade existentes. Iniciativa não concluída.

### 2026-09-20 — F4, retirada de reset e diagnóstico CLI com inicialização

Partida: Core `cdd5bb0b`, Community `f3897db2`, ambos limpos e publicados.
Autenticação GitHub confirmada em `jpbraga`; não foi necessário `gh auth switch`.
O mandato F4 do plano-base permite esta frente em paralelo à retirada de Sprint,
com release condicionado à compatibilização da migração. A iniciativa permanece
integralmente ativa; este checkpoint não encerra F4 ou a exclusão de writers.

Classificação por efeito e consumidores reais:
- `reset`: apaga SQLite/uploads/grafos e re-semeia. Removidos parser, handler e
  `commands/reset_graphs.py`; o módulo só era consumido por esse handler e seus
  testes. Não restou alias/flag/handler escondido de reset.
- `verify-pipeline`: apesar de descrito como diagnóstico, chamava `init_db`,
  registrava o lifecycle relacional e compunha o KG antes das cinco consultas.
  Removidos parser e handler. As funções de `core.kg.health` permanecem porque
  `services/queue_health_service.py` ainda as consome; sua pureza completa exige
  auditoria própria, não foi inferida da classificação de reader.
- `cmd_init(..., owned_serve_lock=...)`: o único consumidor da exceção era reset.
  Uma hipótese inicial de uso por serve foi refutada pela busca em src/tests.
  Removidos parâmetro e ramo exclusivo; init sempre passa pelo guard original.
  Testes cobrem recusa com lock real e rejeição do antigo argumento. Não houve
  ampliação de autoridade nem relaxamento do guard.

Incompatibilidade da CLI: comando desconhecido, exit 2, antes de settings ou
qualquer dispatch, inclusive opções antigas e `--help`. Testes novos exercitam
reset simples/--yes/--help e verify-pipeline com Board/--json/--help; o launcher
instalado roda em processo novo, sem PYTHONPATH, contra diretório ausente ou
fixtures opacas de SQL/uploads/binding/grafo. Comparam presença, bytes e mtimes
antes/depois. Fixtures opacas não pretendem certificar um banco real: demonstram
que o comando recusado sequer precisa abri-lo. Help preserva setup, serve,
status, investigação de código, métricas e credenciais.

Testes antigos de execução de reset foram retirados por especificarem uma
capacidade agora proibida; não foram transformados em skips nem contados como
aprovações. Regressões de status/métricas/migração no arquivo misto de closeout
foram preservadas. README/CLAUDE não oferecem os comandos retirados. O documento
histórico de setembro 13 conserva os resultados antigos, com aviso explícito de
que reset foi retirado. O guia corrente de health foi substituído por observação
local com limites explícitos: status não certifica integridade/currency do KG.
Saíram receitas de UPDATE de filas, remoção de locks e chamada direta de workers,
além da premissa obsoleta Kùzu. Nenhuma mudança de frontend ou contratos MCP/REST;
não há alegação de ausência de manutenção nesses outros transportes.

Pré-validação em `PULSE_REFACTOR/.validation-v040`:
- `closure-f4-cli-retirement.json`: nenhum finding de código, oito budgets 0/0;
  somente duas matrizes README divergentes após a redução de imports.
  Regeneradas com o renderer oficial; nenhuma exceção/budget foi alterada.
- `provenance-f4-cli-retirement-final.json`: reconstrução/reinstalação após a
  remoção do parâmetro exclusivo; 804/322 arquivos .py, 869/406 membros de payload,
  source→wheel→install idênticos byte a byte, inclusive ausência do módulo apagado.
- `community-f4-cli-retirement.log`: **130 passed**, 91,37 s, nenhuma falha
  ou skip. Suíte: comandos retirados, admissão init, closeout misto, init/serve/
  status/version, métricas, serve-lock, Code Traceability CLI, contributor contract
  e credential handoff. Inclui init com Grafx real na suíte existente.
- `closure-f4-cli-retirement-final.json`: **ok=true**, findings de código/docs
  vazios, oito budgets **0/0**, 7.619 imports Core, 1.249 Community→Core,
  25 dependências. A remoção não introduziu mecanismo no Core.
- Wheels finais: Core SHA256
  `b55cb83e8f1635a4ed503dfc94af834ee2566721dd739239cd6f756d667a687d`;
  Community `43ea2ce595b37d30cfadec8e0394198b37c569134aed4fa0baf7e9d788c52140`.
- Ruff e diff-check aprovados. Nenhuma alteração de fonte produtiva durante os
  testes finais; todos os processos de validação encerraram com exit 0.

Próxima frente F4, inventário já investigado mas ainda sem retirada:
`kg migrate-schema/backfill/dedup-entities/proposals/unmerge/export/subtype declare/restore`,
`commands/kg_migrate_schema.py`, executor `kg_recovery_only.py` e seu console-script.
Proposals aqui é curadoria de manutenção; export é backup JSON-LD de grafo.
O executor não tem consumidor src demonstrado além de sua entrada própria;
`api/kg_rebuild.py` e resources operacionais ainda instruem seu uso. Retirá-lo
exige coordenar a remoção/tombstone sem ação de preflight/confirm/run, testes de
REST e instruções na UI; features que toquem frontend devem ter testes frontend.
Não basta apagar apenas o console-script deixando a feature chamável.

Dependências de testes do executor a revisar sem apagar cobertura independente:
`test_community_grafx_only` tem um teste de recusa de arquivo legado do executor,
mas também testes reais de resolver/privacidade que permanecem;
`test_r16b_relational_schema_migrator` importa o executor dentro da certificação
terminal e precisa conservar a prova de schema;
`test_global_discovery_recovery_installed_e2e` usa seu EXPECTED_GRAFX_VERSION
(a dependência declarada atual é okto-grafx[accel]==0.0.7).
Também foi encontrado `python -m ...commands.materialize_legacy_fr_ac`: aceita
--dry-run=false e escreve FR/AC por actor de migração, sem fence de runtime.
Esse entrypoint distribuído deve entrar na classificação por efeito, preservando
materialização interna legítima quando houver consumidor demonstrado.

Continuam pendentes todas as demais superfícies F4, migração F2A/B/C/D e remoção
atômica F3, leitor histórico autorizado, writers/mutadores físicos/gerações inativas,
instalador/rollback e requisitos BASE/KG/DEI/ARQ/VER. Nenhuma migração de dados reais,
parada/restart de runtime, promoção de binding, release ou mudança de permissão.

Community commit `5b7d56a`; Core altera somente este ledger e a matriz README
gerada. Pushes normais à feature/v0.4.0; sem release/merge. Retomar pelo inventário
F4 acima, mantendo o objetivo integral e as pendências de migração.

### 2026-09-20 — F4, retirada do grupo KG da CLI e das rotinas exclusivas

Partida: Core `a55ca732`, Community `5b7d56a`, limpos/publicados. Turno anterior
classificado como progresso verificado. Mandato permanece o pacote integral.

Inventário por efeito: `kg migrate-schema`, `backfill`, `dedup-entities`,
`proposals`, `unmerge`, `export`, `subtype declare` e `restore` eram manutenção.
`proposals` listava planos de dedup/curadoria, não propostas de Decision do produto;
`export` era backup integral JSON-LD. Removidos o grupo/subparsers, oito handlers,
serializadores de backfill, `_apply_backfill`, cold registry exclusivo de restore,
`_json_field`/import copy exclusivos e `commands/kg_migrate_schema.py`, incluindo
sua entrada por `python -m`. `_field` e `_result_records` continuam usados pela
exportação autorizada de credenciais; não foram retirados por proximidade.

A busca de consumidores demonstrou que `core.kg.dedup_migration` só era chamado
pela CLI. Removido o módulo inteiro (propose/approve/confirm/dedup/unmerge, modos
físicos e formatter), sem guardá-lo atrás de flag. Removida sua superfície no
manifest público e na proveniência dos adaptadores. Também saiu o wrapper de
export e reexports de erros em `application/kg_operations`, exclusivos da CLI.
O exportador `kg.graph_export` permanece porque o MCP o consome diretamente;
a retirada desse transporte ainda é uma pendência F4, não prova de término.

Removidas do inventário CurationPolicy as operações CLI aposentadas, preservando
semântica de default desconhecido, autoridade dos consumidores restantes e recusa
incondicional de hard-delete. Mensagens não mandam mais reexecutar com flags de
CLI. `schema_layer_guard` conserva código, Board, contexto do erro, limites e
recusa; substitui instruções migrate/reprocess pela indisponibilidade do componente
sem comando de reparo. Subtipo não declarado continua recusado, sem direcionar ao
comando administrativo extinto. Nenhum gate foi afrouxado.

Histórico: ledger de equivalências e dados persistidos não foram apagados. Os
testes de fold, cache e query preservam leitura de registros históricos ativos e
revogados; fixtures agora criam explicitamente nós/records históricos, sem chamar
o dedup/unmerge aposentado para preparar os cenários. Regressões exclusivas de
execução da antiga manutenção foram excluídas, não convertidas em skips ou
contadas como aprovações. Arquivos mistos conservam testes de CORS, consentimento,
status, métricas e restore interno roteado. A retirada não prova que todos os
stores/ports antes usados por manutenção já foram limpos.

Incompatibilidade mantém o padrão do checkpoint anterior: `kg`, seus comandos,
opções de execução/planejamento/confirmação e --help recebem unknown-command,
exit 2, antes de configuração/dispatch. Testes ampliados exercitam o launcher
instalado contra diretório ausente e fixtures opacas de SQL/uploads/grafo/binding,
comparando bytes/mtimes/estrutura. Testes de ausência cobrem módulos, handlers,
helpers e contrato público retirados.

README/resources não oferecem mais a CLI KG; documentos de closeout/E2E anteriores
mantêm sua evidência com aviso histórico. A ajuda frontend substitui o bloco de
tripleta de migração por indisponibilidade de schema e autoridade do processo de
release. `HelpPanel.maintenanceCli` verifica a renderização, ausência da instrução
retirada e preservação da explicação de consolidação semântica.
- Build `frontend-f4-kg-cli-build.log`: TypeScript + Vite + sync aprovados;
  78 arquivos, árvore SHA256
  `7acfb0409c0930c3cd420bbdb62cbb9ed2e2cf2a3377e0063f3354d47ec9674b`.
  Warnings de tamanho de chunk/plugin timing, sem erro.
- `frontend-f4-kg-cli-tests.log`: **11 arquivos, 19 testes passaram**, 45,58 s.
- `frontend-f4-kg-cli-dist-verify.log`: verificação de sincronismo aprovada.
- `provenance-f4-kg-cli.json`: **803/321 .py, 868/405 membros de payload**,
  source→wheel→install idênticos, incluindo ausência dos dois módulos apagados;
  PYTHONPATH pareado nos testes e launcher filho instalado sem PYTHONPATH.
- `closure-f4-kg-cli.json`: sem findings de código; apenas matrizes README
  desatualizadas com a redução para **7.596 imports Core/1.226 Community→Core**.
  Registrar a recertificação final após o renderer oficial e reconstrução.
- `community-f4-kg-cli.log`: **192 passed**, 167,32 s, nenhuma falha ou skip.
  Cobertura: ausência/launcher, closeouts mistos, restore interno, proveniência
  dos adaptadores, init/serve/status e Code Traceability.
- `core-f4-kg-cli.log`: **121 passed, 1 failed**, 262,13 s. Única falha:
  `test_kg_graph_export::test_s7_surfaces_contract` ainda exigia o handler CLI
  retirado. Atualizado para exigir ausência, preservando contratos MCP e REST
  remanescentes; não houve mudança produtiva após essa rodada.
- `provenance-f4-kg-cli-final.json`: mesmo conjunto .py/payload byte a byte
  após matrizes README regeneradas e par reconstruído/reinstalado. SHA256 dos
  wheels: Core `9653ee34e54cc0498561e8a1dedb493fe95ccff0615457fce0fdb63b0e0bbe6b`;
  Community `8a8657209ce68da1327679faabb5451ccd04103c8d3e5caa07e9e93882d19ef0`.
- `closure-f4-kg-cli-final.json`: **ok=true**, nenhum finding de código/docs,
  budgets zero; contagens 7.596/1.226, 25 dependências.
- `core-f4-kg-cli-final.log`: **13 passed**, 102,10 s, nenhuma falha ou skip;
  reexecução dirigida de export, contratos retirados e manifesto público.
  Não somar rodadas sobrepostas como casos únicos. Todos os handles encerrados.
- Ruff e diff-check aprovados. Fonte produtiva congelada durante testes; os
  resultados frontend permanecem aplicáveis à mesma árvore de assets.

Dependência revelada para próxima limpeza: `kg_curation_proposals` não tem mais
consumidor de produto em src; persistem o port, adaptador e registro em
`adapters/composition.py`. Retirar código sem consumidor requer separar schema/
dados históricos da composição viva. A tabela/model KGCurationProposal também
participa do apagamento autorizado por Board em sqlalchemy_kg_governance e em
test_board_relational_erasure; preservar esse contrato e os dados preexistentes,
sem remover schema histórico junto com o adaptador morto. O ledger de equivalências, em contraste,
tem consumidor de leitura `equivalence_fold`; preservá-lo é necessário.

Continuam executor recovery-only, CLI materialize_legacy_fr_ac e demais MCP/REST/UI
operacionais, providers de health sem efeitos, stores exclusivos remanescentes,
F2 migração/backup/rollback/autoridade histórica e remoção atômica F3, além de todo
BASE/KG/DEI/ARQ/VER não comprovado. Não houve migração real, edição de dados do
usuário, parada de runtime, release ou mudança de permissões de leitura.

Community commit `96bf16753a60283fc4dea868f144c70d4ec1e501`; Core contém retirada
da implementação exclusiva, ajustes de mensagens/contratos/testes, matriz gerada
e este ledger. Publicação incremental na feature/v0.4.0, sem release/merge.
Próxima retomada: stores/ports exclusivos de curadoria e entrypoints distribuídos
remanescentes, seguida da retirada coordenada MCP/REST/UI. Objetivo integral ativo.

### 2026-09-20 — F4, stores exclusivos e materialização manual FR/TR/AC

Partida: Core `e1052beb`, Community `96bf167`, limpos e publicados. Turno anterior
classificado como progresso verificado. O objetivo continua o pacote consolidado
inteiro; esta retirada não encerra F4 nem a migração.

Investigação de consumidores confirmou duas cadeias exclusivas do controle público:
1. Após a retirada de dedup/propose/approve, `kg_curation_proposals` era referenciado
   em produção apenas pelo próprio adaptador e sua composição. Removidos port/DTO/
   registro de runtime, adaptador SQLAlchemy e fake de testes sem consumidores.
2. `commands/materialize_legacy_fr_ac.py` oferecia --dry-run=false para enumerar
   Specs de um Board e escrever FR/TR/AC com actor de sistema. Era o único
   consumidor de produção do planner dedicado, orchestration, port e adaptador.
   Retirada a cadeia inteira, inclusive alias do planner e actor exclusivo; não
   manter um comando escondido chamável após apagar apenas a CLI.

A conversão de requisitos no fluxo normal continua em
`services/spec_entity_canonicalization.py` e seus consumidores de create/update.
Não houve mudança nesses algoritmos, na autoria/versionamento de edits normais,
nos gates de Spec ou na resolução de links por texto. Os testes antigos de
execução da materialização manual foram retirados junto com a capacidade; testes
mistos de canonicalização preservam cenários do produto. Nada foi convertido em
skip. O inventário vivo F05 passou de 53 a 52 paths, retirando apenas o comando
extinto; o teste continua exigindo cobertura completa e zero import relacional.
O mapa RETIRED_CORE_ORM_IMPORT_ALLOWLIST permanece como evidência histórica
congelada, sem reativar exceção; não é um comando nem um import vivo.

História e privacidade: `KGCurationProposal`/`kg_curation_proposals`, seu schema,
índice e participação no erasure continuam. Remover a feature de aprovação não
é licença para apagar propostas existentes. O docstring do model agora explica
esse papel histórico. O teste de erasure foi ampliado com uma proposta de outro
Board e verifica preservação de plano, hash, autor e estado após apagar o alvo.
O novo teste de composição cria propostas históricas pending/resolved usando o
model real, compõe duas vezes sem permitir abertura de sessão e compara bytes
integrais do SQLite antes/depois. Confirma também ausência do registro de writer.
A fixture usa somente essa tabela para isolar a composição; a suíte existente de
erasure usa o schema completo e os permits reais. Não confundir as duas provas.

CLI por módulo: --help, --dry-run=true e --dry-run=false do módulo retirado agora
falham no Python instalado com módulo inexistente (exit 1), antes de abrir dados.
Não é tombstone/alias do parser `okto-pulse`: a entrada Python não existe no wheel.
Testes rodam sem PYTHONPATH em processo novo e preservam bytes/mtimes de SQLite
opaco. Testes de ausência verificam os módulos Core/Community retirados.

Docs PORTS/ARCHITECTURE deixaram de oferecer as duas portas e os adaptadores
retirados; inventários introdutórios deixam de fixar contagens antigas sem prova.
A composição passa a descrever Grafx por portas, sem a falsa exceção de runtime
embutido no Core. Não houve alteração frontend, MCP ou novos mecanismos no Core.

Pré-validação em `PULSE_REFACTOR/.validation-v040`:
- `provenance-f4-retired-stores.json`: **799/318 .py, 864/402 membros de payload**,
  source→wheel→install idênticos byte a byte, inclusive ausência dos sete módulos
  de produção retirados; testes com PYTHONPATH pareado e processos novos.
- Ruff e diff-check aprovados.
- `core-f4-retired-stores.log`: **45 passed**, 67,55 s; nenhuma falha ou skip.
  Inclui canonicalização FR/TR/AC normal, IDs, links por texto, fold histórico,
  manifesto público, ausência e gate F05.
- `community-f4-retired-stores.log`: **68 passed, 1 failed**, 217,41 s.
  Erasure completo/cross-Board, composição, autoria semântica e init passaram.
  Falha somente na fixture nova que publicou include_graph=False: registry
  corretamente recusou providers obrigatórios ausentes. Corrigido o teste para
  usar a composição completa lazy; nenhum gate/default foi relaxado.
- `closure-f4-retired-stores.json`: sem finding de código; somente duas matrizes
  README divergentes, regeneradas pelo renderer oficial. Oito budgets 0/0.
- `provenance-f4-retired-stores-final.json`: par reconstruído/reinstalado após
  matrizes; 799/318 .py e 864/402 payloads ainda byte-idênticos. SHA256:
  Core `489cd484ea8259fdd59763469ba4b656317629437a57370607251e8a26c0325d`;
  Community `a84550254e951bfbe8908aeb0eae0cba48722b82e576d4623b5da35a6b0ccae7`.
- `community-f4-retired-stores-final.log`: **7 passed**, 8,06 s, nenhuma falha
  ou skip; composição completa lazy, bytes históricos e ausência instalada.
  Não somar rodadas sobrepostas como casos únicos.
- `closure-f4-retired-stores-final.json`: **ok=true**, nenhum finding de código/
  docs, oito budgets **0/0**, 7.580 imports Core/1.219 Community→Core e
  25 dependências. Fonte produtiva não mudou após a primeira rodada de testes.
  Todos os processos de validação encerraram; nenhum runtime real foi reiniciado.

Próxima frente confirmada: `kg_recovery_only.py`/console-script, suas instruções
em api/kg_rebuild e resources, e UI/REST/MCP operacionais. No teste misto
`test_r16b_relational_schema_migrator`, a parte que usa o executor é fingerprint/
budgets auxiliares após a prova do schema; preservar schema_objects, contracts,
replay e índices/triggers ao retirar os checks específicos da CLI. O E2E instalado
de Global Discovery importa apenas EXPECTED_GRAFX_VERSION do executor, mas seus
fluxos MCP de manutenção exigem revisão F4 própria; não apagar cobertura de
produto por conveniência. A versão deve derivar da dependência pinada, não de
um módulo de manutenção que está saindo.

Permanecem demais F4, health passivo, F2A/B/C/D, leitor histórico autorizado,
exclusão completa de writers/cutover/rollback, remoção atômica F3 e requisitos
BASE/KG/DEI/ARQ/VER ainda sem prova. Nenhuma migração de dados reais, restart,
publicação de release, remoção de história ou nova permissão foi executada.

Community commit `692eb188ec18e6fcca526f589bc7dd631439c2e4`; Core remove os ports/
planner/orchestration exclusivos e fixtures correspondentes, mantendo canonicalização
de produto, inventário zero-relacional e evidência neste ledger. Pushes normais
na feature/v0.4.0, sem release/merge. Próxima retomada: retirada coordenada do
executor recovery-only e de suas recomendações, seguindo o inventário F4 por
efeito. Iniciativa integralmente ativa.

### 2026-09-20 — F4: controles de manutenção retirados de KG Health

Retomada a partir de Core 3570289e / Community 692eb188, ambos limpos e publicados.
A investigação da retirada do executor confirmou recomendações também em MCP,
health, resources e API, além do frontend. Para fechar uma alteração verificável
sem deixar o painel chamando uma API parcialmente removida, este incremento
retira primeiro os controles e o cliente exclusivos de KG Health. A retirada do
executor/REST/MCP segue pendente e continua na mesma iniciativa; o objetivo
integral não foi reduzido e F4 não está concluída.

Community frontend:
- KGHealthView perdeu RecoveryPanel, HistoricalRecoveryControl, seus helpers,
  formulário de razão, preflight/confirm/run, polling de histórico e cancelamento.
  Também saíram triggerKGTick, cooldown/estado de execução, botão de tick,
  callback/evento de abertura de Runtime Settings e permissões desses controles.
  O painel não renderiza recommended_action operacional recebido de payload antigo.
- kg-health-api perdeu os DTOs/funções de rebuild e postJSON exclusivo, além dos
  reexports de histórico. Não há outro consumidor frontend desses DTOs/funções.
  kg-api/EmptyState/KnowledgeGraphPage ainda possuem histórico: não confundir a
  remoção deste cliente/painel com a retirada completa da feature.
- Overview conserva estados healthy/at_risk/unknown/unavailable e indicação de
  snapshot antigo; o link de diagnóstico funciona também em recovery_needed.
  Não recomenda preflight nem apresenta outro caminho para o painel retirado.
  Os sinais de scheduler, filas, métricas, footprint e dívida continuam visíveis.
  Contagens de consolidação cognitiva permanecem no painel próprio com permissão
  original, sem o resumo duplicado de RecoveryPanel.
- Não houve alteração de backend, permissão efetiva, schema, autoridade ou dado
  histórico. O inspector de partition integrity e demais superfícies de manutenção
  ainda exigem sua própria retirada F4; este incremento não os declara passivos.

Verificação em PULSE_REFACTOR/.validation-v040:
- frontend-f4-health-controls-build.log: TypeScript detectou duas props boardId
  removidas junto com o trecho de tick. Corrigidas antes dos testes; nenhum gate
  foi relaxado. frontend-f4-health-controls-build-final.log: tsc/Vite/sync aprovados.
- 78 assets, tree SHA256
  bd904a265b896f5532a1adcf9d0b822566cd4aaf55ca2d8bb6412236fb47af5b.
- provenance-f4-health-controls.json: par reconstruído/reinstalado, **799/318 .py**
  e **864/402 payloads** source→wheel→install byte-idênticos antes dos testes.
  Wheels SHA256: Core
  489cd484ea8259fdd59763469ba4b656317629437a57370607251e8a26c0325d;
  Community 951e88b0b2204c2ddd592fc5d502948315678868acb31c36672ac3b6e81bf435.
- frontend-f4-health-controls-tests.log: **66 passed**, 3 arquivos, 6,48 s:
  KGHealthView, KGHealthOverview e KGHealthCognitivePendingPanel. Inclui
  polling/aba oculta/abort no unmount, refresh, erro/snapshot, permissão de health,
  telemetria ausente versus zero e ausência de controles mesmo com grants antigos.
  Os testes de execução da feature retirada foram substituídos por ausência;
  testes do painel cognitivo independente continuam verdes.
- frontend-f4-health-controls-e2e.log: **1 passed**, Chromium, 9,0 s. Servidor
  estático novo em porta efêmera, iniciado depois do mtime dos assets instalados,
  sem PYTHONPATH; serve diretamente frontend_dist do site-packages verificado.
  APIs só por fixtures: requests não mockados são recusados, inclusive leituras,
  sem proxy para Pulse real. Navegação 360/768/1440 px, tooltips, axe sem violações
  critical/serious em claro/escuro, refresh e zero chamadas de manutenção.
  Removida a antiga exceção que tolerava POST rebuild/preflight. Nenhum runtime
  Pulse real foi iniciado/parado. O servidor de teste encerrou no finally.
- frontend-f4-health-controls-lint.log: **0 errors**, 401 warnings históricos,
  dentro do ratchet por regra (total baseline 402, inalterado).
- frontend-f4-health-controls-dist.log: 78 assets sincronizados, hash acima.
- closure-f4-health-controls.json: **ok=true**, findings/docs vazios, todos os
  oito budgets **0/0**. Código Python não mudou; nenhuma suíte Python de domínio
  foi repetida sem necessidade. diff-check aprovado.

Próximo passo concreto: retirar console-script/kg_recovery_only.py e REST
kg_rebuild, os três handlers MCP e suas permissões/presets/descrições. Regenerar
catálogo por tools_catalog_generator após mudar registry. MCP ainda contém
_run_rebuild_service_cooperatively (uso exclusivo em teste), constantes de
remediação e os três nomes em _TOOLS_WITH_LAZY_COMPACT_DESCRIPTION. A classe
RebuildAdmissionGateUseCase só é consumida por esse preflight; revisar remoção
junto a seus exports e testes de autorização, sem enfraquecer outros gates.
Health service e readiness ainda recomendam o executor e preflight: coordenar
remoção das instruções com o transporte, mantendo componente/motivo/limitação.
Testes mistos adicionais localizados: r2a_rebuild_admission_gate, r10a/r10b,
af16 generation/storage, kg_board_rebuild_adapter, kg_global_discovery_recovery,
kg_operations_*_authorization, kg_direct_mcp_acl, kg_rebuild_preflight/sources/
service; revisar casos exclusivos, preservando cobertura interna legítima.
O E2E instalado Global Discovery ainda importa somente a constante Grafx do
executor: derivar do pin e preservar sua cobertura de produto. Teste R16B mantém
schema/replay/contracts/índices e perde apenas checks exclusivos do executor.

F2A/B/C/D, cutover/rollback, remoção atômica F3, restante F4, demais BASE/KG/DEI/
ARQ/VER e decisão pendente de leitura histórica permanecem abertos. Sem migração
real, release, merge, novas permissões ou declaração de conclusão integral.

Community commit d511302e837475d9398cf5fc7ccb05c5b293e30a. Core registra este incremento e a retomada no ledger; pushes normais em feature/v0.4.0, sem release/merge. Iniciativa integralmente ativa.


### 2026-09-20 — F4: retirada pública de board rebuild (validação em andamento)

Base Core 28f536bf / Community d511302, limpa/publicada. Removidos executable
kg_recovery_only.py, console-script e router kg_rebuild completo, três handlers
MCP e helper cooperativo exclusivo, RebuildAdmissionGate DTO/use case/exports,
invoke_rebuild_admission da porta e implementação e os helpers de application/
kg_rebuild sem consumidor. build_source_store permanece: kg_health_service o
consome para o censo, com recusa de snapshot incompleto preservada.

Removidas as três permissões da árvore, manifesto de introdução/presets e
registry MCP; nenhuma concessão nova. Registros persistidos não são reescritos.
Health preserva diagnóstico recovery_needed e componente/motivo, mas deixa de
mandar executar a CLI retirada: operator_action=none, limitation explícita.
Removidas instruções da família em agent_instructions/resources/HelpPanel;
catálogo regenerado oficialmente. Contagem observada anterior instalada **344**,
posterior **341**: os testes tinham um ratchet antigo de 340. Conferir diferença
exata de nomes no par durante validação; não aceitar contagem como prova única.

Testes exclusivos de CLI/transportes retirados; testes mistos preservam schema,
replay, enumeração, manifestos, serviço interno e outros gates. Revisão detectou
que uma edição por delimitador tinha englobado o teste R16B de falha parcial:
restaurado integralmente de HEAD antes de testar, limitando remoção aos 18 lines
específicos do fingerprint/budget da CLI. Novo teste test_retired_public_rebuild
cobre router real/OpenAPI, host MCP Community materializado, permissões/presets,
metadata de instalação e python -m retirado sem tocar bytes/mtimes históricos.

Artefatos em .validation-v040/*f4-public-rebuild*. Produção editada e par de
wheels construído; instalação/proveniência em andamento. Ainda sem resultado
comportamental deste incremento. Não commitar/push antes da validação e closure.


#### Fechamento deste incremento F4 — prova, falha conhecida e retomada

A superfície pública de **board rebuild** foi retirada nesta alteração. REST:
sem router/handlers/schemas registrados, requests antigos recebem 404 padrão
sem ler Board/UoW; o fallback SPA não intercepta /api/. MCP: três tools ausentes
também no host FastMCP materializado e chamadas retornam Unknown tool antes de
autoridade/providers. CLI: sem console entrypoint/launcher nem módulo no wheel;
python -m recusa help/inspect/execute/rehearsal antes de acessar dados existentes.
O teste preserva bytes/mtimes de SQLite, recibo, metadata Grafx e arquivo legado
opaco. Não faz upgrade nem interpreta esses arquivos como databases válidos.
Não há tombstone que redireciona para manutenção alternativa.

A resposta de health mantém recovery_needed e causa, sem autorização de reparo.
Revisão de transporte detectou que HealthIssue REST ignoraria o campo novo
limitation: retirado desse issue e incorporado à description já tipada. O teste
usa a resposta real do serviço e KGHealthResponse para provar componente,
reason, descrição da limitação e operator_action=none após serialização. Na
projeção de readiness em dict, next_action=none/limitation não oferece executor.
Histórico/geração/auditoria e censo de fontes continuam; nenhum registro alterado.

Validação (sem somar rodadas sobrepostas):
- provenance-f4-public-rebuild.json: 799/316 .py, 864/400 payloads idênticos
  source→wheel→install antes da rodada inicial.
- core-f4-public-rebuild.log: **585 passed, 6 failed**, 187,28 s. Falhas:
  dois testes ainda exigiam rotas retiradas, uma menção de diagnóstico retirada
  da documentação, contagens antigas de tools/permissões e budget de metadata.
  As rotas foram cobertas por ausência central; o serviço interno de preflight
  conserva seus testes. A menção CONTRADICT_PENALTY continua como diagnóstico,
  sem recomendar tick/rebuild.
- community-f4-public-rebuild.log: **171 passed, 1 failed**, 148,71 s. Inclui
  schema R16B/upgrade/replay/falha parcial, regras Grafx/privacidade, demais ACLs,
  ausência REST/CLI/MCP. Única falha: inventário de schemas fechados esperava45,
  mas o par anterior já tinha46 (confirmado abaixo); ratchet corrigido para46.
- frontend-f4-public-rebuild-tests.log: **19 passed**, 11 arquivos, 10,00 s.
  build tsc/Vite e verify:frontend-dist aprovados: 78 arquivos, tree
  83c38d44540ddb1b3f61de8995f3e50855fc81ffdf6bd2c78b21672ea9288034.
- Matrizes README regeneradas pelo renderer oficial, após closure inicialmente
  reportar somente drift documental. Nenhum budget/exception relaxado.
- core-f4-public-rebuild-final.log: **194 passed, 3 failed**, 42,46 s. Restavam
  metadata e duas contagens secundárias (policies338 e ALL_FLAGS596).
  Rodada counts.log repetiu essas duas falhas porque o script de edição usou cwd
  incorreto; nenhum arquivo foi modificado por ele. Correção aplicada depois do
  término: counts-final.log **2 passed**, 1,91 s. As contagens novas preservam
  igualdade registry/permissions e o teste de ausência verifica as três folhas.
- community-f4-public-rebuild-final.log: **30 passed**, 72,76 s. Nenhum skip;
  inclui o host MCP real via Client em memória, não chamada direta de .fn.
  Não confundir essa prova com E2E HTTP de um runtime Pulse completo.
- Após ajuste de transporte da descrição, reconstruído/reinstalado o par:
  provenance-f4-public-rebuild-transport.json **799/316 .py, 864/400 payloads**
  byte-idênticos. Wheels finais em wheels-f4-public-rebuild-transport:
  Core SHA256 08b9fb4b8d38045c1eec76861027fdce0286d65f2e00cd851b578602579554b0;
  Community SHA256 25260b1aeafd0d82bbdc5e5893c279a94982bf902ab72793f95bb2f03100f3d8.
- core-f4-public-rebuild-transport.log: **7 passed**, 6,21 s. Diagnóstico/DTO REST,
  causa persistente e recusa de snapshot incompleto pelos consumidores restantes.
- closure-f4-public-rebuild-transport.json: **ok=true**, findings/docs vazios,
  oito budgets **0/0**, 7.563 imports Core/1.143 Community→Core/25 dependências.
  Ruff e diff-check aprovados. Todos os processos de teste encerrados.

**Gate ainda vermelho, explicitamente não relaxado:**
`test_mcp_resources.py::test_initial_footprint_under_budget`: metadata medida
**57.457 tokens**, limite **50.800**. Foi reproduzida a situação anterior em venv
isolada, com os wheels do incremento anterior verificados por SHA e todos os .py
instalados byte a byte contra o wheel (os mesmos artefatos com source→wheel já
provado no checkpoint anterior). baseline-f4-public-rebuild-metadata.json:
**344 tools, 57.705 tokens de metadata, 2.493 de instructions, 46 closed schemas**.
O par atual tem341 tools. mcp-f4-public-rebuild-delta.json prova diferença exata:
saíram somente rebuild_preflight/confirm/run e nenhum nome entrou. Não interpretar
as antigas constantes340/45 como estado real do par anterior. A queda de248 tokens
é uma medida isolada de metadata, **não benchmark do fluxo completo**. Não houve
corte de schema tipado, aumento de limite nem remoção do teste para fazê-lo passar.
A retirada restante de manutenção e a prova de custo do pacote continuam abertas;
esse gate impede declarar a iniciativa/otimização concluída.

O harness instalado de Global Discovery agora deriva Grafx do pin do pyproject,
sem importar a CLI removida, e seu inventário esperado foi ajustado ao registry
observado (341/333, hash b88574861a237b1159358930b705b3d30592bc01b5cf0c61f30a55841edd2412).
Esse E2E completo **não foi executado** neste incremento. Ainda contém expectativas
históricas de versão0.3.3 e fluxos de manutenção a retirar; sua revisão faz parte
própria de F4/validação instalada, não apagar testes de produto para resolver drift.

Próxima frente: família pública Global Discovery recovery (preflight/confirm/run/
status/cancel/resume) e quarantine restore, depois demais controls/readers por efeito.
Revisar simultaneamente código interno exclusivo: KGRebuildService não tem mais
caller produtivo direto; board_rebuild_ingestion/rebuild_effects ainda compõem
providers e importam legacy_rebuild_reconciliation. Separar esse executor morto
de tipos/receipts/audit/journals e consumidores de histórico/privacidade/health
antes de remover. Não manter feature antiga só porque testes a instanciam; não
apagar schema ou história porque writer saiu. Continuação deve manter o gate de
metadata visivelmente pendente até resolvê-lo sem reduzir provas/autoridade.

Nenhuma migração real, restart de Pulse ativo, release/merge, alteração de conta
ou nova permissão. F2A/B/C/D, F3, restante F4 e BASE/KG/DEI/ARQ/VER continuam em
andamento; este incremento não é conclusão integral nem suspensão do objetivo.

Na conferência anterior ao push, frontend_dist continha 78 arquivos, mas o Git
versionava somente 76: a regra global *.png excluía os dois logos gerados já
presentes nos wheels verificados. Acrescentada exceção restrita a
src/okto_pulse/community/frontend_dist/assets/*.png e incluídos ambos os logos.
Comparação árvore física versus git ls-tree: 78/78, nenhuma diferença. Não houve
mudança nos bytes do pacote instalado nem nos hashes/provas acima. O commit
Community ainda não publicado foi ajustado; nenhum histórico remoto reescrito.

Community commit 84136882b55647e6e297ba1dc1dfddfe291c0c83. Core registra a retirada MCP/ports/permissões, diagnóstico e provas. Pushes normais em feature/v0.4.0; sem release/merge. O gate de metadata permanece aberto, conforme reprodução acima. Iniciativa integralmente ativa.

#### F4 em execução — retirada pública de Global Discovery recovery e quarantine restore

Checkpoint anterior publicado e confirmado por ls-remote: Core26e75111 e
Community8413688, ambos limpos. Progresso efetivo do ciclo anterior: push pareado
e correção da distribuição versionada dos logos. Autenticação válida sem troca
de conta. Nova prova provenance-before-f4-global-retirement.json confirma o par
instalado anterior byte a byte: 799/316 .py e 864/400 payloads.

Inventário desta frente: seis tools global_discovery_recovery
(preflight/confirm/run/status/cancel/resume) e quarantine_restore no registry MCP.
Busca das rotas REST e clientes frontend não encontrou equivalente dessas sete
operações. A ajuda geral ainda recomenda o painel de rebuild já retirado e health
ainda manda executar Global Discovery recovery; corrigir ambas as orientações.
Remover sete permissões operacionais/presets e policies das tools, sem alterar
administração de Boards/identidade/privacidade nem dados persistidos.

Dependências investigadas: helpers privados do server de confirmação/dispatch/
delivery/status são exclusivos do controle retirado; autorização global é
compartilhada pelas três tools de DLQ ainda vivas e precisa conservar exatamente
suas verificações. O controle/worker interno continua composto por main.py;
contratos públicos do mesmo módulo também servem ao writer lease usado por
outbox, CLI init e Board erasure. Não remover esse módulo em bloco por nome.
Quarantine restore também é chamado pela compensação de rebuild_effects, cuja
retirada interna é frente dependente já registrada. Esta mudança elimina os
entrypoints e helpers exclusivos do MCP; poda de workers/adapters e dos demais
controles segue obrigatória para concluir F4. Nenhum uso por teste conta como
justificativa permanente para manter executor morto.

Validação e commits deste incremento ainda pendentes. Gate de metadata anterior
permanece aberto com teto inalterado; não confundir retirada parcial com F4 completa.

#### Evidências do incremento F4 Global Discovery/quarantine

Retiradas as sete tools do registry vivo, handlers, classificação reader,
descriptions, documentação distribuída Core/Community, catálogo gerado e sete
folhas de permissão/presets. Permanecem 334 tools, 331 policies mais três exceções
humanas já existentes; 589 flags e 100 folhas dos manifests de introdução. Nenhuma
nova autoridade nem edição de permissões persistidas. Os helpers exclusivos do
server para service/control/status/dispatch/delivery e cancelamento foram
removidos. O helper compartilhado de autorização foi renomeado para
_global_outbox_authorize, com operação explícita e a mesma verificação de duas
autoridades; os testes dos três consumidores restantes passaram.

Health conserva recovery_needed, componente, causa e limitação de discovery,
mas usa operator_action=none. O teste do serviço serializa a resposta com o DTO
REST real para provar que a descrição e a ausência de comando sobrevivem ao
transporte. Ajuda deixa de orientar rebuild/quarantine restore e distingue Board
saudável de Discovery indisponível. Os controles de tick/DLQ ainda vivos não foram
declarados removidos: são próximos itens obrigatórios do inventário F4.

Validação em processos novos, após instalação pareada e prova byte a byte:
- provenance-f4-global-retirement.json: 799/316 .py e 864/400 payloads idênticos
  source→wheel→install antes dos testes; sem runtime Pulse ativo reiniciado.
- core-f4-global-retirement.log: **463 passed, 1 failed**, 34,43 s. Inclui catálogo,
  registry de permissões, reconciliação de presets, autorizações Core/MCP,
  diagnóstico/DTO health, composição restante, contratos internos de recovery,
  global outbox e writer lease. Única falha: budget de metadata, descrito abaixo.
- community-f4-global-retirement.log: **138 passed**, 95,17 s. Ausência comprovada
  no host FastMCP materializado, list_tools e call_tool (sem argumentos e com IDs
  históricos/apply=true), antes de resolver contexto global/Board/UoW/providers.
  Mantida prova REST/CLI do board rebuild anterior; não inventadas rotas públicas
  para Global Discovery/quarantine que a investigação não encontrou. Inclui
  composição roteada, demais ACLs REST e os 46 contratos MCP de schema fechado.
- frontend-f4-global-retirement.log: **19 passed**, 11 arquivos, 9,61 s. Teste da
  ajuda renderizada exige limitação por componente, ausência da cerimônia antiga
  e preservação do conteúdo de consolidação semântica.
- Build tsc/Vite, sync e verify:frontend-dist: **78 arquivos**, tree SHA256
  0625513f62392485ab1cdda64044af3197c8ea00580c8351902ed45753ebaabe.
  Sem novo E2E de browser neste incremento de texto; cobertura de controles
  retirados da tela health foi registrada no incremento anterior.
- Ruff dos Python alterados aprovado. Testes exclusivos dos handlers/helpers
  retirados foram substituídos pela ausência via transporte; testes mistos de
  locks/outbox, contratos internos e composição efetivamente usada foram mantidos.
- closure-f4-global-retirement.json: findings vazios, oito budgets 0/0; apenas
  dois readme_closure_matrix_mismatch. Matrizes regeneradas pelo renderer oficial.
- Par final reconstruído/reinstalado após essa alteração documental:
  provenance-f4-global-retirement-final.json confirma novamente 799/316 .py e
  864/400 payloads idênticos. Wheels em wheels-f4-global-retirement-final:
  Core SHA256 04338307f15e179b6e09cb223d79e750267d7b4873be5b11289ca04be0b995e3;
  Community SHA256 01092efb7a8b13bbd764570c893882572b6b4aaf974ccc2d945db52932384eef.

**Falha conhecida ainda aberta:** metadata passou de 57.457 para **57.028 tokens**
(menos 429), com limite **50.800** inalterado. A medição instalada
mcp-f4-global-retirement-delta.json prova 341→334 tools: exatamente as seis de
Global Discovery recovery e quarantine_restore saíram, nenhuma entrou; os
**46 schemas fechados** continuam. Instructions atuais: 2.444 tokens. Isto é
medição de metadata, não o benchmark de fluxo completo exigido pelo pacote.
Nenhum teto aumentado, schema truncado ou teste ignorado para produzir verde.

Retomada obrigatória: retirar controles/readers de manutenção DLQ/outbox, tick,
schema, histórico operacional e tuning nos transportes reais; concluir poda
interna. O runtime de recovery ainda inicia preparation_poller/recovery_worker
em build_community_recovery_runtime e é composto por main.py; não há mais caller
produtivo de resolve_recovery_control_plane após esta mudança. A retirada desse
runtime deve investigar jobs duráveis preexistentes, fences de writers e história
antes de eliminar módulos/tabelas. Não considerar a mera composição justificativa
final para preservar manutenção. O E2E instalado antigo de Global Discovery
continua não executado e ainda exige versão0.3.3, inventário antigo e as tools
retiradas; precisa ser substituído por provas instaladas do produto remanescente,
ausência, histórico e upgrade, sem fingir que ajustar só contagens o valida.

Closure final **ok=true**, findings e documentation_findings vazios, oito budgets
0/0, 7.544 imports Core, 1.143 Community→Core e 25 dependências. Todos os processos
de validação encerrados. Community commit 2e5eead11e7f37c3818ecb8bb1349126022d38b1;
78/78 assets físicos e versionados, nenhuma diferença. Core reúne retirada das
tools, permissões, diagnóstico, catálogo e este ledger. Push pareado normal em
feature/v0.4.0; nenhuma migração real, release/merge ou relaxamento de gates.
Objetivo completo permanece ativo.

#### Decisão F2A — autorização recebida em 2026-09-20

O usuário respondeu **“sim”** à proposta expressa de criar permissões genéricas
de leitura de arquivo histórico por seção, limitadas ao Board e à origem
arquivada, preservando exatamente os acessos e as negações efetivos anteriores.
Está resolvida a “Decisão F2A pendente” registrada anteriormente: implementação
desse contrato autorizada, sem precisar perguntar novamente. Isso não autoriza
usar somente board.read, ampliar Q&A/avaliações/histórico, conservar permissões
Sprint ativas, conceder acesso por default ausente ou migrar dados reais. Captura
da autoridade anterior, projeção por seção, negações explícitas, autenticação,
isolamento de Board/origem e testes de paridade são parte da implementação ainda
pendente; autorização não é evidência de implementação concluída.

#### F4 em execução — DLQ/outbox e inspeção de fila pública

Partida limpa e publicada: Core d31d2ea2 / Community 2e5eead. Prova anterior ao
incremento: provenance-before-f4-dlq-retirement.json, 799/316 .py e 864/400 payloads
idênticos. Ciclo anterior classificado como progresso: sete tools retiradas,
transporte/health/frontend testados e commits publicados; F2A autorizada.

Inventário fechado deste incremento: dead_letter_list/reprocess, queue_drilldown,
connectivity_dlq_diagnose/reprocess/verify e global_outbox_dead_letter_list/
reprocess/verify (nove tools MCP). REST: GET queue/dead-letter, POST
queue/dead-letter/redrive, GET queue/health e queue/drilldown. Frontend: modal
DeadLetterInspector, cliente dedicado, botão em RuntimeSettings/EventQueue e
ação em CognitiveActionCenter; retirar polling do endpoint de queue/health.
Preservar bloqueio técnico/readiness e processamento cognitivo legítimo; nenhuma
falha de infraestrutura vira skip/waiver. Tuning runtime/tick ainda é outra frente
F4 obrigatória, não declarar removido por apagar seu painel de observação de fila.

Dependências: consolidation.py chama reprocess_dead_letter_rows no retry interno;
health/readiness consomem list_dead_letter_rows e queue_health_service. Preservar
esses serviços compartilhados, retirando use cases/wrappers exclusivos da
superfície pública. kg.operations.queue.read/reprocess ainda têm consumidores em
kg_routes_crud, operational_rest e deterministic_projection_repair; não retirar
as folhas antes desses consumidores. Três folhas global_outbox são exclusivas
dos handlers retirados e podem sair. Health deve manter contagens/causas, sem
instruir chamadas para tools extintas.

#### F4 — retirada implementada de DLQ/outbox e inspeção pública de fila

Retiradas as nove tools inventariadas, quatro rotas REST, três use cases
exclusivos e os wrappers correspondentes. Removidos modal, clientes, botões e
polling da fila no frontend; preservados bloqueios técnicos, consolidação
semântica e gates de evidência. Health/readiness mantêm contagens, causas e
severidade, com limitação de disponibilidade e sem receita de reparo. O runbook
de novos alertas de takedown também fica vazio; alertas já persistidos não são
reescritos. Não há mudança em dados/grants reais ou no retry interno compartilhado.

As três folhas exclusivas global_outbox saíram; queue.read/reprocess permanecem
pelos consumidores identificados acima, ainda sujeitos à retirada F4. Registry
atual: 325 tools, 322 policies e três exceções humanas já existentes; 586 flags e
97 folhas de introdução. Catálogo regenerado exclusivamente pelo gerador oficial.
Documentação distribuída deixou de recomendar as nove tools e a cerimônia manual
de limpar o KG antes do trabalho. Tuning e tick ainda presentes não foram dados
como concluídos.

Evidência e correções, em PULSE_REFACTOR/.validation-v040:
- frontend-f4-dlq-retirement-final.log: **55 passed**, 13 arquivos, 10,63 s.
  Testa ausência do Inspector e ausência de fetch após alternar abas, avançar
  timers e desmontar. Mantém bloqueio técnico e ausência de skip/waiver.
  A primeira rodada falhou numa expectativa antiga de botão; corrigida. O
  primeiro build detectou import Database sem uso; removido, sem ignorar TSC.
- frontend-f4-dlq-retirement-build-final.log: tsc/Vite, sync e verificação,
  **78 arquivos**, tree SHA256
  b4071f8f03d5c17a319c2491be5ef82f017045e7b2024e8bce84725dd4870038.
  Lint frontend: zero erros, 397 warnings dentro do baseline 402 inalterado.
- core-f4-dlq-retirement.log: **433 passed, 19 failed**. Uma falha é o budget de
  metadata ainda aberto; 16 eram expectativas da navegação/remediação retirada;
  duas fixtures de paridade criavam Board relacional sem rota Grafx. Inicialização
  real isolada do Grafx foi acrescentada nessas fixtures; sem fallback produtivo.
- core-f4-dlq-retirement-final.log: **106 passed**, 58,40 s, nos dez arquivos
  corrigidos; mantidas contagens, classificação, severidade, autoridade e gates.
  core-f4-dlq-retirement-alert.log: **46 passed**, 2,92 s, após a última alteração
  do runbook, preservando thresholds e dívida. Ruff de todos os Python alterados
  aprovado. Nenhum teste de telemetria foi dispensado para retirar a recomendação.
- A limpeza automatizada inicial removeu indevidamente a tupla externa de rotas
  no teste de offsets e o trecho seguinte do teste de registro MCP. Revisão do diff
  detectou e restaurou os testes não relacionados. As primeiras coletas Community
  falharam por IDs de parametrização antigos; corrigidas sem remover casos vivos.
  Esses logs não são evidência de comportamento aprovado.
- closure-f4-dlq-retirement-alert.json: **ok=true**, findings e
  documentation_findings vazios, oito budgets **0/0**. Matrizes README atualizadas
  pelo renderer oficial após o primeiro relatório apontar somente drift documental.
- Par final em wheels-f4-dlq-retirement-alert, reinstalado antes dos últimos testes:
  provenance-f4-dlq-retirement-alert.json confirma **796/314 .py**, **861/398
  payloads**, source→wheel→install byte a byte e origem instalada. Core SHA256
  afe1f12320fb1182807014c812be526e8bd48164edc25dab49e6443330079c84;
  Community SHA256
  f8aaa8175a093252b368332c704d80aefc2a19b8751aac3040231bf03d4f44e1.
  Processos de validação novos; nenhum runtime Pulse real reiniciado.

**Gate ainda vermelho:** metadata **55.926 tokens > 50.800**, limite inalterado.
mcp-f4-dlq-retirement-delta.json prova 334→325 tools, exatamente nove removidas,
nenhuma adicionada, 46 schemas fechados e 2.444 tokens de instructions. Redução
de 1.102 tokens de metadata; não equivale ao benchmark de fluxo completo.

Retomada: concluir validação Community e publicar este incremento; depois retirar
tick manual, schema/histórico operacional, tuning e demais relatórios/reparos F4.
Poda interna deve preservar processamento automático e jobs/história existentes.
F2A está autorizado e ainda requer implementação de grants por seção/Board/origem,
paridade efetiva, revogação e captura das negações. F2B/F2C/F2D/F3, UI/analytics,
provas instaladas de upgrade/rollback e matriz integral dos quatro documentos
continuam pendentes. Este checkpoint não conclui a iniciativa.

Conclusão da validação Community: community-f4-dlq-retirement-verified.log teve
**147 passed, 2 failed**, 124,95 s. As falhas detectaram uma referência residual à
tool global_outbox_dead_letter_list numa tabela do workflow efetivamente servido
pelo Community; substituída pela limitação de disponibilidade. Nova instalação
pareada e prova provenance-f4-dlq-retirement-complete.json mantêm 796/314 .py e
861/398 payloads idênticos. community-f4-dlq-retirement-resources.log: **8 passed**,
9,15 s, incluindo os dois cenários antes falhos e o catálogo efetivo de resources.
Os 147 casos aprovados incluem ausência MCP/REST antes de storage/autoridade,
schemas remanescentes, permissões e limites de paginação. Não é E2E do runtime
Pulse real nem prova de upgrade/cutover.

Par definitivo deste incremento: wheels-f4-dlq-retirement-complete. Core SHA256
afe1f12320fb1182807014c812be526e8bd48164edc25dab49e6443330079c84 (inalterado);
Community SHA256 520221dbc35560084b127b6f3dc61597ade39ac37d1bd1c2893b4cfc6fabd270.
Os 78 assets físicos estão todos no índice Git, inclusive os novos nomes hash.
closure-f4-dlq-retirement-complete.json confirma **ok=true**, findings e
documentation_findings vazios, oito budgets 0/0, 7.482 imports Core, 1.131
Community→Core e 25 dependências. Todos os processos deste incremento terminaram.
Community commit dbf7706947133d32b6eb6474eb16f5b9664d0005; Core reúne o contrato,
catálogo, permissões, testes e ledger correspondentes. Publicação por push normal
em feature/v0.4.0; autenticação jpbraga válida, sem troca de conta necessária.

#### F4 em execução — tick manual

Partida publicada limpa: Core fc0a46bd / Community dbf7706. Prova do par instalado
permanece provenance-f4-dlq-retirement-complete.json. Inventário: tool
okto_pulse_kg_tick_run_now, POST /kg/tick/run-now, cliente kg-tick-api e botão
Save & run now no RuntimeSettingsPanel, incluindo polling exclusivamente usado
para habilitá-lo. A folha kg.operations.tick.run não tem outro consumidor
produtivo; sua introdução/presets devem sair sem conceder outra autoridade.

Entrada MCP/REST autoriza, adquire lease, consulta health, publica evento pelo
wrapper dispatch_manual_tick e commita. Esse wrapper só tem esses dois callers
produtivos e pode sair com refuse_tick_if_degraded/get_kg_health exclusivos dele.
O restante de application/kg_tick.py tem consumidores em eventos: admission,
fence de publicação, reset seguro e handler de KGFullRebuildTick. Preservar
esquemas/eventos duráveis preexistentes, processamento periódico, commit e locks;
retirada da entrada manual não é autorização para apagar eventos pendentes.
Testes mistos devem manter gates cognitivos F16, efeitos periódicos, barreiras
de escrita e resiliência. Tuning de settings permanece frente F4 separada.

#### F4 — tick manual retirado, processamento interno preservado

Retirados tool, rota, cliente, botão, polling de habilitação, wrapper/porta e
helpers exclusivos descritos acima; removida a folha tick.run de registry,
introduções e presets. Catálogo regenerado oficialmente: **324 tools**, **321
policies**, três exceções humanas, **585 flags**, **96 folhas de introdução**.
Nenhuma nova autoridade ou comando alternativo. A ajuda e os resources efetivos
não anunciam tick manual. O painel de tuning ainda existe e sua retirada continua
obrigatória; não foi declarada concluída pela retirada do botão.

Testes exclusivos da entrada eliminada foram substituídos por ausência nos
transportes registrados. Mantidos os casos de fan-out por Board real (FK ligada),
idempotência, recuperação parcial da frota, admission/fence de publicação,
eventos KGFullRebuildTick duráveis, reset seguro e cancelamento com writer lock.
Mantidos todos os gates cognitivos F16; a asserção do predicado compartilhado
continua no consumidor cognitivo. O teste S02 deixou de abrir os arquivos
kg_tick.py e kg_rebuild.py já eliminados, preservando o gate dos routers vivos.
Não há mudança no handler/schemas de eventos, dados, leases ou runtime real.

Validação em PULSE_REFACTOR/.validation-v040:
- provenance-f4-tick-retirement.json antes dos testes: **796/313 .py**, **861/397
  payloads** idênticos source→wheel→install. Processos novos, PYTHONPATH pareado.
- core-f4-tick-retirement.log: **364 passed, 1 failed**, 93,23 s, 20 arquivos.
  A única falha é o gate de metadata abaixo. Passaram autorizações, reconciliação
  de presets, catálogo, barreiras arquiteturais, gates cognitivos, eventos,
  fences, resiliência, clock/ownership e health do tick.
- community-f4-tick-retirement.log: **137 passed**, 142,94 s. A tool é desconhecida
  no host MCP efetivamente materializado antes de contexto, UoW, lease ou grafo;
  a rota responde 404 uniforme para IDs owned/foreign/missing, inclusive payload
  force_full_rebuild=true, e não aparece no OpenAPI. Recursos efetivos e schemas
  fechados também verificados. Prova de ausência, não E2E de migração real.
- frontend-f4-tick-retirement.log: **34 passed**, 12 arquivos, 49,27 s. Aba inicial
  Decay Tick com Board conhecido e permissões presentes, troca de abas, timers
  até 45 s, desmontagem e mais 15 s sem fetch, save ou botão manual. Ajuda
  renderizada não contém instruções de run-now; tuning remanescente continua
  com seus testes de permissão e drafts.
- frontend-f4-tick-retirement-build.log: build tsc/Vite, sync e verify de
  **78 assets**, tree SHA256
  49eacdd4db5129f52604bf01d5eb165f39b82558b42c6c304849f5893fdae072.
  Lint: zero erros, 394 warnings <= baseline 402. Ruff e diff --check aprovados.
- closure-f4-tick-retirement.json: findings vazios, oito budgets **0/0**; somente
  duas matrizes README desatualizadas, regeneradas pelo renderer oficial.
  Contagens observadas: 7.470 imports Core, 1.123 Community→Core, 25 dependências.
- Par final reconstruído/reinstalado após README: wheels-f4-tick-retirement-final;
  provenance-f4-tick-retirement-final.json repete 796/313 .py e 861/397 payloads
  byte a byte. Core SHA256
  697e0f4e7abfd08716afec08b51cf3952e1d463f09cf7cd39229819c2db9af66;
  Community SHA256
  0f445fe2743463a0847227428866d2669dd89bbbd2c082e57617752ecceee754.

**Gate de metadata aberto:** 55.859 > 50.800, limite inalterado. Medição instalada
mcp-f4-tick-retirement-delta.json confirma 325→324, somente tick_run_now retirado,
zero tools adicionadas, 46 schemas fechados e 2.444 tokens de instructions.
Menos 67 tokens; não substitui o benchmark de fluxo completo. Nenhuma evidência
de conclusão integral é inferida destes testes.

Próximo trabalho: F2A autorizado — captura de decisões efetivas por seção/origem,
grants limitados ao Board e leitura com ACL atual, sem expor o arquivo SQL bruto.
Investigação adicional já localizou a materialização de decisões no use case
GetEntityExportBundleUseCase e o adapter de export por seção; reutilizar a
autoridade canônica por porta pública. AgentBoard tem overrides restritivos;
PermissionPreset tem linhagem; REST usa Principal/claims e ACL de Board. Captura
não pode resumir essas fontes a keys presentes ou somente owner. Não foi definido
nem implementado novo contrato F2A neste incremento de tick. Retomada inclui
demais frentes F2/F3, F4 (histórico operacional/schema/tuning/relatórios/poda),
DEI/ARQ/VER/KG, UI/analytics, upgrade/rollback e benchmark, conforme matriz global.

closure-f4-tick-retirement-final.json: **ok=true**, findings/documentation_findings
vazios, oito budgets 0/0 e contagens 7.470/1.123/25 confirmadas sobre o par final.
Todos os processos de validação deste incremento encerrados. Community commit
3a4b6cd1d97f1d9331543ebcbc59be6ed3d070ea, 78/78 assets físicos/versionados. Core
inclui catálogo, permissões, wrappers retirados, testes e este ledger. Push normal
pareado em feature/v0.4.0. A iniciativa completa continua ativa; nenhum dado real
migrado, processo Pulse reiniciado, release ou relaxamento de gate realizado.

#### F2A em execução — autoridade por seção no arquivo histórico

Ciclo anterior classificado como progresso: DLQ/fila e tick manual retirados,
testados e publicados; oito budgets arquiteturais zero, metadata ainda acima do
teto. Partida limpa: Core 11296e2b / Community 3a4b6cd. Prova antes dos testes:
provenance-before-f2a-access.json, 796/313 .py e 861/397 payloads idênticos.

Implementação em curso da decisão autorizada: contrato público puro de seções
content/qa/evaluations/history e grant por realm/Board/origem/identidade, com
negação por ausência e interseção com ACL/permissão atuais. A captura chama a
política canônica para cada autoridade antiga, incluindo absent=True e revisão
obrigatória, e aplica o acesso à raiz antes das seções. Não copia flags por
presença, não cria autoridade de escrita e não converte board.read em leitura.

Arquivo relacional v4 adiciona decisões fechadas e fingerprint das fontes de
autoridade na mesma transação BEGIN IMMEDIATE que já protege conteúdo e publicação.
Leitura privilegiada de v1/v2/v3 continua possível sem inventar grants ausentes.
Community inventaria seu principal humano local (LocalAuthProvider) e agentes
dos AgentBoard, usando CommunityAgentAuthenticationGateway para preset, linhagem,
review, atividade e overrides. Proprietário/criador de agente não é identidade
autenticada por inferência. O contrato é genérico; inventário de outros provedores
de autenticação pertence às respectivas edições. Nenhuma credencial ou hash de
API key deve entrar no fingerprint/arquivo. Testes de paridade e replay pendentes.

Este passo ainda não publica reader REST/MCP/UI nem instala grants ativos. A
introdução das permissões genéricas no registry, administração/revogação, reader
de projeção por seção e corte de Sprint continuam necessários antes da conclusão
F2A/F3; não tratar o snapshot bruto como produto autorizado de histórico.

#### F2A — captura de autoridade implementada no arquivo v4

Implementados ArchiveSection/ArchiveReadSections/ArchiveSourceScope/ArchiveReadGrant,
parser fechado e política pura na porta pública historical_archive. A captura
aceita apenas PermissionSet já resolvido e autoridades registradas; herda review
e negações da política existente. A leitura de uma seção exige grant exato mais
acesso atual ao Board e permissão atual à seção; grant ausente, outra identidade,
realm, Board ou origem não autoriza. Este é contrato puro, ainda sem reader público.

O adapter sprint_retirement_access invoca o mesmo gateway de agentes usado pelo
MCP, dentro de AsyncSession vinculada à conexão/transação de captura. O principal
humano é LocalAuthProvider, confirmado como provider concreto em main.py e CLI.
Board com realm legado NULL continua local, como load_accessible_board; realm
estrangeiro é rejeitado pelo adapter Community. Sem inferir usuário autenticado
de owner_id/created_by. Agente inativo e revisão obrigatória produzem negações.
Limites de linhas/bytes são verificados antes da carga das fontes de policy; grant
órfão bloqueia publicação. Credenciais e seus hashes ficam fora do fingerprint.

O arquivo v4 registra esse snapshot por origem original e mantém tabelas/refs
históricas intactas. _verify_access exige formato, fingerprint, shape, escopo e
ausência de grants duplicados. A mesma reserva SQLite permanece até salvar,
verificar e commitar todos os audit refs. Alterar permissão invalida replay;
rotacionar credenciais não altera o arquivo. v1/v2/v3 continuam verificáveis sem
fabricar acesso. O reconciliador de storage/backup agora aceita v4 com os mesmos
gates de hash, tamanho, ownership e contagem; a dependência foi corrigida antes
dos testes de comportamento. Não há Attachment criado para expor o dump: o
download existente exige metadata de Attachment/Card; o único StaticFiles mount
localizado/inspecionado em main.py serve assets do frontend.

**Premissa de teste corrigida com evidência:** a lista legada vazia NÃO nega todas
as leituras. map_legacy_permissions habilita read/_read por compatibilidade, mesmo
sem tokens de escrita. O arquivo capturou corretamente quatro leituras True para
esse agente, mantendo board.admin.delete e card.entity.edit_fields negados no
resolver original. Negar essas leituras teria endurecido autoridade na migração,
contrariando a decisão autorizada. O primeiro teste as esperava False e foi
corrigido; nenhuma semântica produtiva foi alterada para satisfazê-lo. Uma segunda
asserção usava o nome inexistente card.entity.edit (has admite ausência no legado);
corrigida para a folha registrada edit_fields. O novo capturador, ao contrário
desse teste incorreto, valida as quatro autoridades contra ALL_FLAGS antes de
avaliá-las e nunca aceita autoridade desconhecida.

Evidência em PULSE_REFACTOR/.validation-v040:
- provenance-f2a-access-scope.json antes dos testes: **797/314 .py**, **862/398
  payloads** idênticos source→wheel→install. Todos os processos são novos.
- core-f2a-access-verified.log: **46 passed**, 4,52 s. Matriz das 16 combinações
  raiz/seções, absent=True/negação/review, identidade/realm/Board/origem, revogação
  atual, boolean estrito, shape fechado, porta de policy e contrato de export.
  A primeira invocação Core apontou arquivo de teste inexistente e não executou
  testes (core-f2a-access.log); seleção corrigida para test_entity_export_contract.
- community-f2a-access.log: **89 passed, 1 failed**, 385,44 s. Única falha foi a
  expectativa de leitura negada para lista legada vazia. Cobertura restante inclui
  arquivo lossless, embedded refs, replay, corrupção, limites, rollback, locks,
  storage reconciliation e snapshot/restore conjunto.
- community-f2a-access-focused.log: sete aprovados e a mesma falha; a inspeção do
  blob da fixture comprovou empty-list=True, inactive/review=False, e os overrides
  do preset herdado preservados. community-f2a-access-parity.log falhou apenas na
  folha de teste inexistente, descrita acima. Correção final:
  community-f2a-access-parity-final.log: **8 passed**, 17,04 s. Inclui rotação de
  credencial sem drift, limites/orfandade sem publicação, replay após revogação,
  duas origens no mesmo Board, Board estrangeiro e write reservation durante save.
- Ruff dos Python alterados/novos e diff --check aprovados. Sem mudança frontend
  ou novos transportes neste incremento; assets anteriores preservados no wheel.
- closure-f2a-access.json: findings vazios, oito budgets 0/0; somente matrizes
  README desatualizadas, regeneradas oficialmente. Contagens 7.475 imports Core,
  1.127 Community→Core e 25 dependências.
- Par final reinstalado após README: wheels-f2a-access-final;
  provenance-f2a-access-final.json confirma novamente 797/314 .py e 862/398
  payloads byte a byte. Core SHA256
  f4e0644cc819d00ef576cfd11ad6a49e109e82c982cc07861742579fcfba1bbc;
  Community SHA256
  00fd9681fc4be801e66d3ef8a71ce005b54ad50be9ee35878aa1eee31b26be46.

Limites/retomada: snapshots v3 existentes não são reescritos como v4. Replay do
mesmo migration_id com formato/autoridade diferente falha; o coordenador de
cutover deve selecionar/reconciliar a operação correta. A captura usa o resolver
e registry anteriores, ainda ativos nesta fase: **F3 precisa preservar a avaliação
antiga no caminho de migração antes de eliminar suas folhas vivas**, sem criar um
serviço Legacy Sprint. Não presumir que o capturador funcionará depois de retirar
Sprint de ALL_FLAGS sem coordenar essa dependência. Faltam instalar grants
genéricos ativos/revogação/administração, reader por projeção segura (evaluations
não pode vazar pela linha raiz de sprints), transportes/UI histórica, transferência
substantiva, imutabilidade/cutover e provas de upgrade/rollback. F2A não está
concluída. A iniciativa e o gate de metadata já aberto permanecem em andamento.

closure-f2a-access-final.json confirma **ok=true**, findings/documentation_findings
vazios, oito budgets 0/0, 7.475 imports Core, 1.127 Community→Core, 25 dependências.
Todos os processos deste incremento terminaram. Community commit
9a2832e093ffd5931b1cb9c9faa283a11270bccc; Core publica a porta pura, testes e ledger
correspondentes. Push pareado normal em feature/v0.4.0, sem migração real,
reinício de Pulse, release ou mudança de permissões de contas reais.

#### F2A — persistência genérica dos grants e revogação (em implementação)

A decisão autorizada por seção passa a ter uma porta pública de persistência e
estado tipado: teto capturado, seções atuais, referência/hash do arquivo e revisão.
O mecanismo SQL fica exclusivamente em Community/adapters. Chave completa:
realm, Board, tipo/ID opacos da origem e tipo/ID autenticados do sujeito. Não há FK
para a entidade aposentada, lane, workflow ou permissão de escrever/avaliar.

O instalador é interno e explícito, separado do create_all: exige o audit ref
commitado, verifica o blob e recaptura a autoridade sob BEGIN IMMEDIATE antes da
primeira instalação. Mudança de policy desde a captura bloqueia a instalação.
Replay exige o marcador de instalação e a população/proveniência exatas; não
ressuscita grant apagado nem restaura seção revogada. Outro arquivo para a mesma
origem exige reconciliação, em vez de substituir silenciosamente o anterior.

A porta de revogação só estreita seções, usa CAS de revisão e grava audit no mesmo
savepoint/transação do chamador. No SQLite exige transação física já estabelecida
pelo UoW antes do savepoint: RELEASE de savepoint externo em legacy transaction
mode poderia confirmar a mudança antecipadamente. Casos de rollback externo e
falha de audit fazem parte da validação pendente. Nenhum transporte chama a porta
sem use case; ela é persistência, não uma nova autorização administrativa.

A composição UoW foi estendida. Falta validar este incremento; não declarar gates
verdes ainda. O reader público, projeções seguras, registry/administração atual,
REST/MCP/UI e cutover continuam pendentes. O instalador histórico também depende
da avaliação antiga: coordenar com F3 antes de retirar as folhas Sprint.

Provas iniciais deste incremento: provenance-f2a-grants.json confirma **797/316
.py e 862/400 payloads** byte a byte entre fonte, wheels e install antes dos
processos novos. core-f2a-grants.log: **115 passed**, 4,52 s;
community-f2a-grants.log: **31 passed**, 42,40 s (grants, captura de acesso e
snapshot UoW). Regressão ampliada ainda em execução; inclui arquivo, inventário,
embedded refs, storage refs, snapshot/restore conjunto e ownership do ORM.

closure-f2a-grants.json: findings vazios, oito budgets 0/0; apenas as duas matrizes
README precisam regeneração oficial. Contagens 7.476 imports Core, 1.130 imports
Community→Core e 25 dependências. Não aumentar nenhum baseline para acomodar a
porta/tabela nova.

A ampliação para testes de contrato UoW encontrou **14 passed, 3 failed** em
core-f2a-grants-uow.log: dois doubles sem a nova capacidade obrigatória e três
helpers anteriores sem a anotação PulseUnitOfWork exigida pelo gate (code_traceability
2093; delivery_evidence 88/129). Correções pontuais e nova prova pendentes; não
relaxar o Protocol nem o gate. O relatório SaaS closure e esse gate verificam
coisas diferentes, portanto closure sem findings não encobre a falha encontrada.

Próximo leitor: conferir identidade/atividade e acesso atual ao Board dentro do
snapshot, não apenas confiar no ActorContext MCP previamente resolvido. Cruzar
as seções vigentes com a autoridade corrente e o grant capturado exato, e projetar
somente campos autorizados. O export atual separa evaluations, test_scenario_ids
e business_rule_ids da raiz; cenários/BR continuam sob spec.tests.read/spec.rules.read.
Não expor storage_path, fingerprints da população, manifests de outros sujeitos
ou tabelas brutas. Esse reader e a introdução coordenada das folhas genéricas no
registry ainda não foram implementados neste incremento.

Regressão ampliada: community-f2a-grants-regression.log terminou com **128 passed,
1 failed**, 463,76 s. A única falha é o contrato F01 de schema: faltavam no
inventário as duas tabelas anteriores de classificação arquitetural e a nova
historical_archive_grants. A comparação AST com b466038 identifica exatamente
architecture_candidate_decisions, architecture_classification_receipts e
historical_archive_grants como adições, sem remoção de tabela.

A prova schema-f2a-grants-proof.json reconstitui exatamente o hash governado
8b43b7a2... removendo somente três colunas nullable JSON implementadas antes deste
incremento: cards.migrated_validation_policy e specs.architecture_adoption /
execution_contract. Mantidas as 65 tabelas herdadas e o hash pré-extração imutável;
o hash corrente correspondente às três adições é 6ca27edf72476258f107e4411466efe6aed99769f3406a14804d680e80bb1c9d.
Inventário/hash atualizados com essa prova, não por troca cega da expectativa.

Corrigidos os doubles para falhar explicitamente quando a capacidade de grants
não estiver configurada; três assinaturas passaram a declarar a porta já
importada. Nenhuma nova lógica/adaptador concreto de infraestrutura no Core.
Matrizes README regeneradas pelo renderer oficial. Build pareado e repetição
dos gates corrigidos pendentes abaixo.

#### F2A — validação final dos grants persistidos

Após as correções, par final reconstruído/reinstalado em wheels-f2a-grants-final;
provenance-f2a-grants-final.json confirma novamente **797/316 .py e 862/400
payloads** fonte→wheel→install idênticos. SHA256 Core:
b04be03491fca4f7ddcf8c34eec273e7a279417ed54f1d11a712cdb05c191700;
Community: 3533fd83a36a9babfe4e2ceccfe42eb08a200cd3e91468066be40a01a98d3851.

- core-f2a-grants-final.log: **132 passed**, 22,06 s. Inclui a matriz pura de
  revogação, captura de autoridade, permission policy port e os três arquivos
  de gates/contrato UoW. Os três failures da primeira rodada foram corrigidos.
- community-f2a-grants-final.log: **30 passed**, 51,77 s. Inclui persistência real,
  instalação/replay, corrupção de população/proveniência, escopo exato, negações,
  rollback externo, falha de audit inclusive durante instalação, retomada, CAS de
  revisão e no-op, múltiplas origens, create_all idempotente sem conceder acesso,
  ownership e snapshot UoW. A única falha F01 da rodada de 129 casos foi corrigida.
- A regressão ampliada anterior já aprovou os outros **128 casos** de arquivo,
  inventário, embedded refs, storage refs e backup/restauração. Não somar as
  rodadas como testes únicos: há sobreposição. As mudanças posteriores no Python
  foram os doubles/anotações e o contrato governado de schema, sem mudar o reader
  de arquivo ou o mecanismo de backup aprovado.
- Ruff dos módulos novos/contratos e git diff --check aprovados. Sem alteração
  frontend/MCP/REST, portanto sem novo catálogo nem build/teste de frontend neste
  incremento. Os assets existentes permanecem no payload verificado.

Limites: a instalação continua operação de migração interna explícita. A nova
tabela não recebe grants pelo bootstrap comum; a porta de revogação é persistência
sob autorização futura do use case, não uma API administrativa. Não executada
contra dados reais. Reader autorizado e projeções, introdução de permissões
correntes, administração pública/transportes/UI e F2/F3 cutover seguem pendentes.
A evidência histórica continua no blob original. Proteção de imutabilidade contra
escritas SQL externas e a coordenação completa de migração/rollback ainda exigem
suas etapas próprias. Não interpretar esta validação parcial como conclusão da
F2A ou da iniciativa; o gate de metadata global anterior permanece aberto.

closure-f2a-grants-final.json: **ok=true**, findings e documentation_findings
vazios, oito budgets 0/0; 7.476/1.130 imports e 25 dependências. No staging, o check
incluindo arquivos novos encontrou uma linha vazia extra no EOF do adapter de
grants; removida sem alteração semântica. Wheel Community reconstruído e par
reinstalado novamente: provenance-f2a-grants-publish.json mantém **797/316 .py e
862/400 payloads** idênticos. SHA final Community substitui o anterior:
91794f9f669a3f572e958f4b120ff276a5f80467f3bffb55715fa2dae704d8ce.
Core permanece b04be03491fca4f7ddcf8c34eec273e7a279417ed54f1d11a712cdb05c191700.
Staged diff --check passa nos dois repos. Nova closure do par exato em andamento;
não houve necessidade de repetir testes de comportamento por remoção de EOF.

closure-f2a-grants-publish.json confirma **ok=true**, findings/documentation_findings
vazios e os oito budgets 0/0 no par final exato. Todos os processos de validação
terminaram. Community commit ece12b7914e7c1ee5d2ce8e6969a22bfff496656;
Core publica a porta, os testes de contrato e este ledger. Publicação pareada por
push normal de feature/v0.4.0. Sem migração real, restart de Pulse, release, merge
ou alteração de permissões de contas reais. Retomar pelo reader autorizado e
introdução coordenada das permissões genéricas, conforme dependências acima.

#### F2A — leitor autorizado e projeção histórica (em implementação)

O incremento anterior é progresso publicado e validado. Repos limpos em
3eaacd15/ece12b7 na retomada. Implementados contrato de leitura paginada, caso de
uso Core, reader/projeção Community e GET REST por Board/origem/seção, ainda sem
prova comportamental desta rodada. Snapshot precede ACL/grants; identidade/realm
incorretos não iniciam leitura. Falta de grant, seção revogada e origem ausente
usam o mesmo envelope. O mecanismo revalida estado e fonte commitada/hash antes
de devolver uma projeção fechada. Nenhum caminho de storage vai para a resposta.

A autoridade corrente por seção é o grant genérico escopado já persistido, junto
à ACL atual do Board, atividade e revisão do agente. A captura é seu teto. O
reader não converte esse grant em quatro booleans globais de agente/preset: o
registry plano existente não representa origem, e seu manifest exige também uma
autoridade histórica explícita. Copiar a decisão para flags globais perderia o
escopo; exigir Sprint no reader criaria dependência ativa proibida por F3.
Portanto, nesta etapa a checagem usa a porta de grants da origem; a integração de
capabilities/listagem de MCP e a administração na UI permanecem dependências
explícitas, sem criar alias ou fallback de board.read. Não declarar concluída a
introdução em todas as superfícies. A instalação continua exclusiva do cutover
interno; bootstrap normal não popula grants nem migra dados reais.

A projeção de v4 interpreta tabelas históricas conhecidas sem importar o modelo
Sprint vivo. Conteúdo exclui evaluations, IDs de cenários/BR, Cards, jobs e
manifest de permissões. Q&A/histórico filtram a FK opaca da origem; avaliações
ficam em sua seção. Valores originais de texto/JSON são preservados, sem
normalização editorial. Paginação de 1–200 registros, offset até 100 mil, arquivo
até 64 MiB (contrato existente), registros da página até 25 MiB; exceder limite
não trunca silenciosamente. Outras seções que precisam de card.entity.read,
spec.tests.read/spec.rules.read ainda exigem seus próprios caminhos autorizados.

O histórico preserva o conteúdo do audit original (inclusive changes/summary),
como o acesso histórico existente; não é uma nova aprovação. Não suprimir nem
reinterpretar audit com base em policy de outra seção sem caracterização/decisão.
A nova rota é read-only, no-store, com 404 genérico para ausência/negação e 503
sanitizado para fonte indisponível/corrompida. DTOs e UoW continuam sem SQLAlchemy,
filesystem ou FastAPI no Core. Testes e closure deste incremento pendentes.

Primeira validação: provenance-f2a-reader.json confirma **799/319 .py e 864/403
payloads** fonte→wheel→install idênticos. core-f2a-reader.log: **208 passed**,
33,29 s; community-f2a-reader.log: **45 passed**, 109,96 s. A captura, os grants
persistidos e a nova leitura coexistem sem alterar a autoridade anterior.
closure-f2a-reader.json: findings vazios, oito budgets 0/0; apenas matrizes README
foram regeneradas pelo renderer oficial. Contagens 7.485/1.143 imports, 25 deps.

Revisão posterior identificou que arrays de avaliações podem exceder a quantidade
de linhas físicas do arquivo. Acrescentado limite de 100 mil registros por seção
antes da primeira página para não emitir cursor além do offset suportado; segue
413 explícito, sem truncamento. Testes novos cobrem também limite agregado de
25 MiB, remoção das quatro tabelas antigas em fixture com foreign_keys=ON e
revogação concorrente: request em andamento usa seu snapshot único; o próximo
request precisa observar a revogação. Par final e essas provas ainda pendentes.

#### F2A — leitor/REST validados

Par final wheels-f2a-reader-final reinstalado e comprovado por
provenance-f2a-reader-final.json: **799/319 .py e 864/403 payloads** idênticos
fonte→wheel→install. Core SHA256
1b67d2a01fbef866bf57115dbef2f0cf3ed4077743b8d3c6d857a914493ded43;
Community SHA256
6ea0d9c73f984d180bd1c3f0b8bdb01db2820595e1df6b4d1dd591176f4396fd.

- core-f2a-reader.log: **208 passed**, 33,29 s. Sem mudança Python Core posterior;
  somente README gerado. Contratos de reader, grants, autoridade e UoW aprovados.
- community-f2a-reader-final.log: **75 passed**, 97,46 s. Inclui as provas de
  limites de seção/página, grants e read snapshot, source grant/corrupção,
  root/seções sem vazamento, isolamento de origem, atividade/revisão/Board ACL,
  identidade nova mesmo Full Control sem acesso herdado, paginação/revogação,
  REST, export existente e ownership. Remoção das quatro tabelas antigas passou
  em fixture com foreign_keys=ON, sem desabilitar FKs para fazer o teste passar.
- O teste concorrente confirmou uma única visão consistente: revogação commitada
  durante o stat não mistura snapshots no request já iniciado; o próximo request
  recebe negação sem abrir storage. O objeto histórico permaneceu byte a byte.
- closure-f2a-reader-final.json: **ok=true**, findings/documentation_findings
  vazios, oito budgets 0/0; **7.485/1.143 imports, 25 dependências**.
- Ruff e staged/worktree diff --check aprovados. Sem mudança frontend neste
  incremento; assets antigos seguem no payload provado. A futura UI permanece
  obrigada a testes frontend pela instrução do usuário.

Documentada a leitura em Community/docs/historical-archive-read.md. Inspeção da
composição real confirmou prefixo /api/v1 (community/app.py inclui api_router,
que define esse prefixo). O teste HTTP foi ampliado do router isolado para esse
router principal; rodada específica pendente abaixo. Nenhum processo real de
Pulse foi reiniciado nem recebeu migração/permissão nova.

Retomada: descoberta/listagem e administração autorizadas, capability MCP com
escopo de origem, UI histórica e testes frontend, seções adicionais de Card/Spec,
transferência substantiva e cutover completo F2/F3. O reader já evita depender de
folhas/serviços Sprint vivos, mas o capturador/instalador ainda precisam da
avaliação anterior congelada no caminho de migração antes da remoção dessas
folhas. Não tratar os grants capturados como permissão global de preset nem
implementar reach-in no core. F2A e a iniciativa seguem abertas, assim como o
gate global de metadata registrado anteriormente.

community-f2a-reader-mounted.log: **1 passed**, 12,16 s, usando o api_router real
sob /api/v1. Revisão arquitetural antes do commit extraiu a regra owner/share para
board_membership_allows_read, helper puro da porta pública permission_policy.
O preflight Core existente, o capturador e o reader passam a consumir essa mesma
regra. Evita duplicar política em Community sem importar helper privado do Core.
Nenhuma regra de realm, atividade/revisão, binding MCP ou restrição de share foi
relaxada. Casos novos cobrem viewer/editor/admin, owner, binding verificado
estrito e share humano por realm. Reconstrução pareada e regressão após essa
centralização pendentes; hashes finais anteriores serão substituídos.

Validação da policy pública centralizada: provenance-f2a-reader-policy.json
comprova o par instalado antes dos testes. core-f2a-reader-policy.log: **143
passed**, 9,79 s (membership, porta de policy, hardening REST por Board, reader e
gate UoW). community-f2a-reader-policy.log: **54 passed**, 82,82 s (reader/REST no
router real, captura, share por realm, limites e export). Nenhuma falha nesta
rodada. closure-f2a-reader-policy.json manteve findings vazios e oito budgets 0/0;
matrizes README regeneradas oficialmente para **7.487/1.144 imports, 25 deps**.

Staging inclui todos os arquivos novos e diff --check passa nos dois repos.
Par de publicação reconstruído em wheels-f2a-reader-publish após as matrizes.
Somente comprovação final do install/closure e commits/pushes pendentes abaixo.

provenance-f2a-reader-publish.json confirma o par final **799/319 .py e 864/403
payloads** byte a byte. SHA256 finais (substituem os pares intermediários):
Core d5de67df0822a4fcf8e9ed418fb815844e195a69b85fa9f40408c8049f1b61ca;
Community 1fda2ca359199cabc000ed1a96b9aec595c030efe6ef96cd30a46661d20ece60.
Somente matrizes README mudaram após a rodada de policy; código Python testado
permanece igual. closure-f2a-reader-publish.json executa contra esses wheels.

closure-f2a-reader-publish.json confirmou **ok=true**, findings/documentation_findings
vazios e os oito budgets 0/0 no par final. Todos os processos de teste/auditoria
terminaram. Community commit bb99ba233257b9af5cdd8c0a9b7e21d5dbe713a4;
Core publica contrato/use case, policy pública compartilhada, testes e ledger.
Push pareado normal em feature/v0.4.0, sem migração real, release, merge, restart
ou alteração de permissões de contas reais. Próximas dependências de F2A/F3
permanecem descritas acima; iniciativa ativa, sem declarar conclusão parcial como
cumprimento do pacote inteiro.

### F2A — descoberta autorizada e UI histórica (em implementação)

A partir do par publicado bc2a9610 / bb99ba23, acrescentada descoberta genérica
por Board, realm e identidade. O use case Core aplica a mesma decisão de seção
antes de ordenar/paginar; não expõe contagens de origens negadas. Community lê
somente grants instalados na mesma transação consistente e revalida atividade,
review e ACL física. Limites fechados: 100.000 candidatos / 64 MiB de autoridade,
200 itens por página. A descoberta é metadado de autoridade instalado: não lê
blobs nem atesta disponibilidade/integridade do conteúdo. O reader verifica a
origem imutável ao abrir cada seção. Esta separação evita repetir payload pesado
na listagem; títulos não são copiados para uma nova fonte de verdade.

REST GET /api/v1/boards/{board_id}/historical-archives retorna origem opaca,
archive_id, seções autorizadas e next_offset. Nova navegação Archives no Board,
com conteúdo sob demanda e referências históricas passivas. Sem CRUD antigo,
links operacionais de Sprint, nova aprovação, flags globais ou alteração real de
permissões. UI limpa conteúdo ao trocar Board/seção/página, cancela requisições
anteriores, ignora respostas atrasadas e distingue negação/limite/falha de vazio.

Testes adicionados para matriz de seção, paginação após negação, contratos
malformados/escopo estrangeiro, revogação, identidade e ACL atuais, REST no router
real, frontend, respostas tardias e HTML histórico inerte. Validação ainda
pendente: build frontend, par wheels/install/proveniência, pytest, Vitest, lint e
closure. Não considerar este trecho concluído antes das evidências abaixo.

Validação do incremento de descoberta/UI:
- provenance-f2a-discovery.json comprovou fonte → wheel → install antes dos
  testes: 799/319 .py, 864/403 payloads iguais, imports em site-packages.
- core-f2a-discovery.log: **238 passed**, 25,37 s (descoberta, reader, grants,
  matriz de autoridade, membership e UoW).
- community-f2a-discovery.log: **65 passed**, 133,03 s (SQLite/storage reais
  descartáveis, ACL/identidade/revogação, REST no router real, capture e F01).
- frontend-f2a-discovery-tests.log: **32 passed / 3 arquivos**, 30,18 s. Inclui
  contrato de resposta por Board/origem/arquivo/seção, offsets, cancelamento,
  respostas atrasadas, revogação, referências passivas e HTML histórico inerte.
- frontend-f2a-discovery-e2e-final.log: **1 passed**, 6,4 s. Chromium servido do
  frontend_dist instalado; API integralmente interceptada por fixtures (não é
  E2E de Pulse real). Larguras 360/768/1440, ambos temas, axe sem violações
  serious/critical; nenhum write API nem leitura de seção não oferecida.
  A primeira execução falhou com sidebar de 256 px aberta em viewport de 360:
  o teste foi completado com a ação real Hide sidebar, mantendo o assert de
  overflow e exigindo área de leitura >280 px. Nenhuma alteração no produto
  para mascarar esse comportamento existente. Captura archives-360.png revisada.
  Servidor descartável iniciado depois da instalação e encerrado após o teste.
- TypeScript/build passaram após corrigir replaceAll incompatível com o target
  e opção exact inválida no teste Testing Library. frontend_dist sincronizado e
  verificado: **78 arquivos**, árvore
  4260d177499ba174212cebeceec52a2b68125147faddd8964e2d2c39fb05378a.
- Lint: zero erros, 394 avisos existentes <=402. Ruff dos arquivos Python
  alterados/novos aprovado; fixture importada explicitamente por módulo.
- closure-f2a-discovery.json: findings vazios, oito budgets 0/0; somente drift
  das matrizes README. Renderer oficial executado: **7.487/1.145 imports,
  25 dependências**. Par final reconstruído em wheels-f2a-discovery-publish;
  prova de instalação/auditoria final e publicação pendentes abaixo.

A enumeração usa offsets somente de itens autorizados; revogação pode deslocar
páginas, portanto a UI oferece reinício/refresh e reconsulta ao voltar do detalhe.
Isso não dá autoridade ao offset. A descoberta não verifica a disponibilidade
do blob: falha de integridade fica explícita na leitura; não vira lista vazia.
A UI não deduz permissões de flags globais/Sprint nem permite mudar grants.

Retomada: integração MCP/capabilities com grants por origem, administração pública
sem ampliar direitos antigos, seções próprias de Card/Spec, transferência
substantiva e cutover F2/F3. Imutabilidade DB dos grants e congelamento da policy
antiga no migrador seguem pendentes. Gate global de metadata permanece vermelho
55.859 > 50.800 (não alterado nem relaxado neste incremento); nenhuma alegação de
conclusão de F2A ou da iniciativa inteira. Não executada migração de dados reais.

Par final comprovado por provenance-f2a-discovery-publish.json: **799/319 .py,
864/403 payloads**, igualdade fonte/wheel/install. SHA256:
Core d0c87c34f28b584f0cae7c63a4f6a40f5b1a148041bd6312e4bb68b505dc50a6;
Community 54f0ba97c504a55cfe800153295235e88f33c295bca0b65d2c23694757c30a9f.
closure-f2a-discovery-publish.json: **ok=true**, findings/documentation_findings
vazios, oito budgets 0/0; **7.487/1.145 imports, 25 dependências**. Somente README
foi alterado entre o par testado e o par final; Python e frontend ficaram iguais.
ESLint direto dos arquivos novos e verify:frontend-dist aprovados. Os 78 assets
estão no índice Git; staged diff --check passou em ambos. Processos descartáveis
de testes/servidor/auditoria encerrados.

Community commit **ab61d21956544bffdd6195e0a6447f7a0f34fed3**. Core publica neste
commit as portas/use case de descoberta, testes e este ledger. Publicação normal
na feature/v0.4.0 dos dois repos, sem alteração de dados/permissões reais,
restart do Pulse, tag, release ou merge. Continuidade conforme pendências acima;
o gate global de metadata e o restante do pacote seguem abertos.

### F2A/F3 — congelamento de autoridade de origem e regressão F4 (em implementação)

Turno anterior: progresso concreto publicado e verificado (Core e3bbce9f /
Community ab61d21). Investigação seguinte encontrou dependência que impede
retirar Sprint com segurança: capturador/instalador consultavam a policy viva.
Além disso, reprodução contra par instalado byte-identical confirmou que um
snapshot Full Control anterior à remoção de kg.operations.tick.run passa a
owner_review_required/unrecognized_direct_permissions. O normalizador tratava a
folha removida como extensão desconhecida. Não é autorização para relaxar review.

Prova de origem: permissions.py no commit-base local 20707250 e no pai de
26e75111 têm o mesmo blob Git 74101618064a1e50a1e9e11c012f7ff6f1f8f7a9;
SHA256 textual f43dbc442765160acefc45093994eb48460c0611100a6f2c04552db13236463b.
Foi extraída a dependência pura necessária do avaliador original, com dados e
funções versionados em historical_permission_policy_v034.py. A cópia histórica
é deliberadamente imutável e não é registry/preset vivo: nenhuma importação de
mecanismo, registry atual ou lifecycle. Somente lookups originais de transições/
status são substituídos pelos valores literais do mesmo baseline. A porta
pública historical_archive_authority retorna apenas quatro decisões históricas,
sem conferir acesso a Board nem operações ativas. Community continua dono do
carregamento SQL/identidade/ACL e da transação; não duplica merge/policy.

A captura/primeira instalação passa a avaliar fatos originais nessa versão,
inclusive snapshots ambíguos que parecem Full Control no registry menor atual.
Eles continuam negados se eram parciais no registry original. Grants/arquivos já
instalados não são reescritos ou ampliados; replay divergente continua fechado.
Não executada migração real. Se houver arquivo antigo capturado pela avaliação
incorreta, sua divergência exige reconciliação explícita, não recaptura silenciosa.

No normalizador vivo, reconhecer somente as quatro gerações exatas all-True
publicadas nas retiradas de F4. False, valores não booleanos, geração parcial e
extensões desconhecidas continuam reconhecíveis como review/negação. Folhas
retiradas não voltam a ALL_FLAGS, manifest público, MCP, preset seed ou rota.

316 casos golden foram calculados pelo módulo original do commit-base (256
combinações de seções/teto Board + 60 documentos diretos/legacy). Fixture inclui
proveniência e hash do avaliador congelado. Testes adicionados de independência
do registry/lifecycle atuais, ancestria inválida, fingerprints de retirada e
SQLite real: captura, instalação, leitura e replay após revogação. Build pareado,
proveniência e execução ainda pendentes abaixo. Integração MCP/cutover continuam
no escopo; esta dependência de autoridade deve ser fechada primeiro.

Rodada inicial: provenance-f2a-frozen-authority.json comprovou **801/319 .py e
866/403 payloads** antes dos testes. Os 316 casos golden passaram; Core total
550 passed / 3 failed. Falhas investigadas: hash da fixture calculado com CRLF
versus comparação canônica LF; normalização de folha retirada null/objeto vazio
podia desaparecer no delta. Corrigido somente o hash normalizado da fixture (o
avaliador congelado não mudou) e mantido documento não reconhecido íntegro no
normalizador vivo. A validação de shape também preserva os tipos antigos das
folhas retiradas, inclusive com preset/Board override, sem reativá-las. Novos
casos cobrem isso. Community inicial: **65 passed**, 124,10 s. Frontend:
**31 passed / 2 arquivos**, 3,00 s (estado de permissão e UI de arquivo).

Validação adicional de provenance do gerador: sdlc_registry.py é o mesmo blob
na base e HEAD (**004739b93ec92ffa4b98dfb4bcfd4de291711fe7**); idem
code_traceability_kg.py (**4680e1e97ced6299c7306bb6f93ba91ea2e664a2**).
Portanto os valores literais congelados vieram efetivamente da mesma policy
baseline, sem incorporar lifecycle posterior. SHA256 LF do avaliador congelado:
**3856be16963e571e5282ca2e6cb6c150c9458b48a17cdec9965b1a5818f23506**.

closure-f2a-frozen-authority.json: findings vazios, oito budgets 0/0, apenas
matrizes README desatualizadas. Regeneradas pelo renderer oficial para
**7.504 imports Core / 1.146 Community→Core / 25 dependências**. Par final
reconstruído em wheels-f2a-frozen-authority-final e instalação provada por
provenance-f2a-frozen-authority-final.json antes da repetição: **801/319 .py,
866/403 payloads**, todos byte a byte, origins em site-packages. Core final:
**559 passed**, 23,59 s. Rodada Community e closure final ainda em andamento.
Nenhum arquivo frontend mudou neste incremento; os 78 assets publicados no
incremento anterior permanecem no payload byte-identical.

Revisão final de autoridade restringiu o reconhecedor vivo: **somente o snapshot
completo original v0.3.4 com as 14 folhas retiradas explicitamente True**, junto
a toda a geração viva, pode perder o fingerprint retirado e normalizar para
Full Control. A proposta intermediária de reconhecer quatro etapas de F4 foi
rejeitada antes de commit/publicação: essas formas também representam documentos
parciais da versão original e não possuem provenance persistida suficiente para
distingui-los. Testes convertidos para exigir review nessas formas ambíguas.
Isto substitui a descrição anterior das quatro gerações; não há relaxamento.
A fonte congelada continua negando documentos parciais da base, inclusive um
snapshot menor idêntico ao registry atual. A migração F3 ainda precisa preservar
essa classificação/proveniência ao limpar os flags operacionais persistidos,
antes de o gateway vivo operar com o registro reduzido; não considerar a simples
remoção de folhas como migração de policy concluída.

Rodada anterior final, antes desse estreitamento adicional: Core 559 passed,
Community 65 passed em 121,53 s; closure ok=true com oito budgets 0/0. Nova prova
pareada e repetição dos testes de autoridade pendentes após a revisão acima.

Validação de publicação após o estreitamento:
- provenance-f2a-frozen-authority-publish.json: **801/319 .py, 866/403 payloads**
  byte-identical fonte/wheel/install; imports em site-packages.
- core-f2a-frozen-authority-publish.log: **559 passed**, 24,40 s.
- community-f2a-frozen-authority-publish.log: **65 passed**, 123,34 s.
- closure-f2a-frozen-authority-publish.json: **ok=true**, findings e
  documentation_findings vazios, oito budgets 0/0, **7.504/1.146 imports e
  25 dependências**. Ruff e staged diff --check aprovados nos dois repos.
- SHA256 final dos wheels (substitui pares intermediários):
  Core 02621903a34d579a9028c727de9ebda6a0421536dd8a548eb53e4a0eaa1a35c0;
  Community 9f697695a9460e10808a5d9db8c2f08367d9d296fd6b87cddeadcc6faad5b8e5.
  Frontend manteve o payload anterior provado; testes de regressão frontend
  registrados acima. Não houve mudanças em tools/catalog/ALL_FLAGS.

Community commit **8a6432239420946dd3f63a260270379973b4c636**; Core publica
neste commit a policy histórica, facade pública, normalização restrita e provas
golden do baseline. Todos os processos de validação encerrados. Push normal
pareado em feature/v0.4.0, sem migração real, restart, release, tag ou merge.

Retomada obrigatória: antes do cutover F3, persistir/preservar a classificação de
review e proveniência dos documentos antigos ao retirar flags operacionais. O
avaliador congelado e a captura agora independem de registry/lifecycle vivos;
a limpeza/migração de dados ainda não está implementada. Seguem MCP/capabilities
por origem, administração histórica sem ampliação de autoridade, imutabilidade
DB dos grants, seções próprias de Card/Spec, transformação e cutover completos.
Não tratar esta correção como conclusão de F2/F3. Metadata ainda **55.859 >
50.800**; o escopo completo DEI/ARQ/VER/KG permanece no objetivo ativo.

### F2/F3 — checkpoint de classificação e gate de paridade (em implementação)

Investigados os três resolvedores Community e a reconciliação de bootstrap.
permission_introduction_audit já registra review, mas gateways não o consultam:
um recibo antigo sozinho não impede reinterpretar um documento após retirar
folhas. Não converter esse log implicitamente em override de autoridade.

Adicionada porta pública permission_retirement: captura imutável das 599 decisões
originais, owner_review_required e motivo, usando o mesmo avaliador congelado
v0.3.4 da captura de arquivo. Hash/blob de origem explícito, parser fechado,
booleanos estritos. Gate exige identidade de cada decisão sobrevivente e da
classificação de review; a lista declarada de retiradas deve explicar exatamente
a diferença do registry vivo. Não aceita comparar somente flags convenientes,
introduzir flags novas ou substituir review por um documento all-False comum.
O módulo congelado/hash não foi alterado; facade compartilha sua resolução.

Community captura um checkpoint no journal permission_introduction_audit existente,
fase separada permission_retirement_capture, sem nova tabela/modelo no Core.
BEGIN IMMEDIATE engloba fonte e journal. Inclui contexto global de cada agente
(também inativo/sem Board) e contexto de cada binding, identidade/creator/realm,
classificação de review e fonte sem credenciais. Projeções SQL não carregam
api_key/hash/nome. Limites de entrada, contextos e bytes de evidência; UUIDs
determinísticos, manifesto e hashes. Retomada com recibo externo detecta journal
parcial, alterado ou completamente removido, sem recriar evidência. Leitura
verifica recibo mesmo após a fonte mudar; recaptura com fonte distinta falha.
Rotação de credencial não muda provenance; falha de insert reverte tudo.

Não conectado a bootstrap nem usado para executar migração de dados reais.
Este checkpoint é evidência pré-cutover, não uma permissão ou override ativo.
A limpeza ainda precisa consumir a classificação persistida e preservá-la em
todos os resolvedores, incluindo agentes inativos reativados e revisão de
preset/Board, antes de remover os dados antigos. Nenhuma decisão de autoridade
nova, edição de policy real, redução de gate ou autorização por board.read.

Testes adicionados para ambiguidade de snapshot menor, legacy vazio, review
por shape retirado inválido, separação global/Board, perda de grants/negações,
parser, provenance/replay, exclusão total com recibo, limites e rollback SQLite.
Ruff aprovado. Build pareado, prova byte a byte, testes e closure pendentes.

Validação deste incremento:
- provenance-permission-checkpoint.json e, após matrizes README geradas,
  provenance-permission-checkpoint-publish.json: **802/320 .py, 867/404
  payloads**, fonte/wheel/install byte a byte; origens em site-packages.
- Core inicial: 358 passed / 2 failed. Corrigidas expectativas novas que
  passavam preset incompleto ao resolvedor efetivo e ignoravam as negações de
  capacidades introduzidas ausentes num teto Board esparso. Nenhuma mudança
  na policy para acomodar testes. core-permission-checkpoint-final.log:
  **360 passed**, 12,51 s, incluindo os golden cases da captura existente.
- Community inicial: 10 passed / 12 failed por fixture sem api_key_hash
  obrigatório. Após corrigir fixture, 21 passed / 1 failed: assert confundia
  nome de permissão agent.api_key.rotate com campo de credencial. Corrigido
  para testar campos/valores reais, mantendo o nome canônico na evidência.
  Acrescentadas provas de writer concorrente bloqueado pelo BEGIN IMMEDIATE,
  binding órfão/realm estrangeiro sem publicação parcial. Rodada final:
  community-permission-checkpoint-publish.log **25 passed**, 43,90 s.
- Frontend: primeira seleção tinha caminhos incorretos (nenhum teste rodou).
  Caminhos reais descobertos; frontend-permission-checkpoint.log: **31 passed /
  2 arquivos**, 3,67 s, usePermissions.stateful e HistoricalArchivesPanel.
  Nenhuma alteração de frontend/SPA neste incremento; payload preservado.
- closure-permission-checkpoint.json: findings vazios, budgets 0/0, somente
  drift documental. Renderer oficial atualizou ambos READMEs para **7.508
  imports Core / 1.149 Community→Core / 25 dependências**. Closure final pendente.
- Wheels finais em wheels-permission-checkpoint-publish, SHA256:
  Core 10d1b36ee96cc48eaf5aab570ecac7863c4dd5aef455da5e7f66aacb134a8b7a;
  Community 385217ce181724e31d2002d91cf817c2bedcfdfe3f03c027259b5119dc46b9ca.
  Python permaneceu idêntico entre o par inicial testado e o par final; só
  README mudou no payload. Testes SQLite finais executados após prova final.

Retomada: integrar o checkpoint e o gate ao coordenador F2/F3, com recibo
persistido fora da mutação candidata. A classificação ainda deve ser consumida
pelos três resolvedores e pelo fluxo de revisão do owner, antes da limpeza de
flags/presets/ceilings. Não liberar review por diferença de hash nem por simples
reconciliação de bootstrap. Preservar identidade/atividade, vínculos e contexto
global/Board separadamente; o PermissionSet isolado não prova essas propriedades.
Grants históricos/arquivos e suas pendências de administração/MCP/imutabilidade
continuam no mesmo plano. Metadata permanece 55.859 >50.800; objetivo ativo.

closure-permission-checkpoint-publish.json final: **ok=true**, findings e
documentation_findings vazios, oito budgets 0/0; 7.508/1.149 imports e 25
dependências. Ruff e staged diff --check aprovados. Todos os processos de build,
testes e auditoria encerrados. Community commit
**7426c5e699e4489f1c911775263393c83b8a701e**; Core publica neste commit a porta,
gate de paridade, refactor da facade congelada, testes e ledger. Push normal
pareado na feature/v0.4.0; sem migração de dados reais, mudança de policy real,
restart do Pulse, release, tag ou merge. Pendências de integração acima mantidas.

### F2/F3 — revisão persistida nas camadas e resolvedores (em implementação)

Continuação após progresso verificado e publicado: Core e60f1d7d / Community
7426c5e. Worktrees limpas no início. Investigados os writers existentes: update
de flags/preset do agente, teto Board e flags do preset já são decisões de policy
autorizadas; descrição, nome, atividade e rotação de credencial não são substituição
da policy. Não conceder ao executor um campo novo nem liberar review em bootstrap.

Implementação atual ainda não validada:
- Value object puro PermissionMigrationReview, por camada agent/preset/board,
  motivo antigo e hashes do checkpoint/fonte. Parser fail-closed para marcador
  inválido ou de outra camada. Não é flag nem endpoint.
- PermissionPresetLineageNode carrega review do próprio preset; filhos continuam
  herdando review do pai mesmo quando editados. Facade pública
  resolve_agent_permission_facts concentra classificação direta, legacy vazio,
  lineage, teto e precedência original dos motivos. Os três resolvedores
  Community usam a mesma policy Core; classificação antes duplicada na edição
  virou reexport da porta pública.
- Três colunas JSON nullable permission_migration_review, somente nos modelos
  Community. Step pre_create_all adiciona-as sem mudar dados, com transação física
  SQLite e idempotência. create_all cria o mesmo shape; schema callable/ledger
  registrados. Bootstrap mantém camada direta guardada intacta e lê review dos
  presets; reconciliação automática não tem autoridade para limpar review.
- Writers existentes limpam somente o marcador da camada explicitamente
  substituída. AgentUpdate e payloads de produto não expõem o marcador. Não
  considerar mudança de descrição/atividade ou diferença de hash como aprovação.
- Instalador interno, fora de bootstrap/transports: verifica checkpoint e fonte
  sob BEGIN IMMEDIATE, classifica cada camada pelo avaliador original, grava
  marcadores e exige paridade de todas as decisões sobreviventes/motivos antes
  do commit. Preserva também identidade/atividade/contextos inativos. Falha
  reverte marcadores e recibo. Replay de instalação não reinstala marcador
  removido por edição de policy posterior; verifica recibo de instalação.

Testes novos escritos: ambiguidade, herança/motivos, marcadores malformados,
três gateways reais, edição/ativação/bootstrap, rollback de paridade, fonte
alterada, esquema antigo e payload sem campo interno. Ainda pendem Ruff,
build/install byte a byte, execução Python/frontend e closure. Flags antigas
não foram removidas pelo instalador; cutover de dados e coordenador F2/F3 ainda
necessários. Nenhuma execução de migração/instalador em banco real.

Observação para revisão: fallback genérico de _load_effective_permissions_for_user
em Core/services/main.py é anterior à lineage atual; Community usa o resolver
compacto registrado, que foi integrado. Não ampliar a alegação para outros
adapters/edições sem verificar seus caminhos. O seam público agora permite que
eles consumam a mesma policy, sem importar mecanismo Community.

Validação/investigação até aqui:
- provenance-permission-review.json e provenance-permission-review-corrected.json:
  **803/321 .py, 868/405 payloads** byte-identical fonte/wheel/install; imports
  em site-packages. Nenhuma execução comportamental contra install antigo.
- Core inicial: 423 passed / 1 failed no catálogo explícito de imports puros.
  Incluído o novo value object canônico e teste adicional que exige somente
  dataclasses, sem importação de mecanismo. Rodada ampliada 448 passed; após
  correção da projeção de extensões e regressão nova: **449 passed**, 37,02 s,
  core-permission-review-corrected.log.
- Community inicial 39 passed / 1 failed: fixture do writer não tinha RealmScope.
  Rodada ampliada 98 passed / 4 failed encontrou uma regressão real: o facade
  reconstruía a árvore negada e perdia vendor_extension. Corrigido preservando
  o PermissionSet existente e clonando sua projeção ao aplicar review adicional.
  A fixture também tinha Board.realm_id NULL, compatível com captura histórica
  mas não com writer ativo; agora usa realm local explícito e exige row encontrada.
  Nenhuma flexibilização do predicado de realm do produto.
- Ledger de schema agora tem 78 migrations (nova etapa explícita). A contagem
  antiga de 876 objetos estava atrasada desde F2A: identificados e exigidos pelo
  nome historical_archive_grants e ix_historical_archive_grants_archive_id,
  resultando em 878 objetos. Fixture v0.3.0/hash, paridade de replay, constraints,
  guards e demais asserts mantidos. Rodada corrigida **102 passed**, 115,04 s,
  community-permission-review-corrected.log, incluindo migração real de cópia
  descartável da fixture instalada v0.3.0, replay, bootstrap e projeção REST.
- Frontend: 49 passed / 7 failed expuseram o hook que só negava flags introduzidas
  quando owner_review_required. Corrigido para negar todas, igual à policy Core.
  Os dois asserts que proibiam Reset to Base em base válida eram inadequados:
  o reset é rascunho de policy permitido ao owner; agora testes exigem aviso
  persistente e nenhuma gravação antes de Save. Não removido o fluxo existente.
  Rodada corrigida 56 passed / 4 arquivos. TypeScript/build/verify aprovados;
  tree intermediária 383afd5327f62b39d951e99c9fd627637c4a8d34135ab29f05e6d4972150b597.
- Chromium e2e-permission-review.log: **1 passed**, 10,5 s, SPA instalada, APIs
  integralmente em fixtures, navegação de arquivo/seções/revogação, 360/768/1440
  e acessibilidade. Não é E2E contra dados reais. Servidor descartável encerrado.
- Lint frontend: zero erros, 394 avisos <=402. Closure corrected: findings
  vazios, oito budgets 0/0, somente matrizes README. Renderer oficial atualizou
  **7.513 imports Core / 1.151 Community→Core / 25 dependências**.

Revisão adicional do fluxo real de edição encontrou que PresetEditorModal
submetia flags mesmo em edição somente de nome/descrição. A tela agora envia
flags apenas após interação no editor, Enable/Disable All ou Reset to Base;
criação continua enviando flags. Isso mantém a revisão pendente em edição de
metadados e usa a autoridade de policy já existente, sem novo campo público.
frontend-permission-review-publish.log: **63 passed / 6 arquivos**, 10,43 s,
incluindo os novos casos de metadata-only e decisão explícita antes do Save.
Novo frontend build e par final ainda sendo preparados; repetir a prova do
payload/SPA e closure antes de publicar. Python não mudou após 449/102 aprovados.

Pendências de continuidade: remoção das flags e transformação F2/F3, coordenador
que instale o checkpoint/review antes de normalizações, recibo de retomada,
restante de arquivo/grants/MCP e matriz completa do pacote. Verificar também
projeção readonly de review direto/do binding na administração de agentes: os
DTOs atuais não expõem o marcador nem seus hashes; não inferir conclusão de
toda a UX administrativa a partir dos testes de usePermissions/preset.

Publicação deste incremento:
- provenance-permission-review-publish.json: **803/321 .py, 868/405 payloads**,
  byte a byte entre fonte/wheel/install. Python idêntico à rodada 449/102;
  o par final incorpora README e a correção adicional da UI, testada separadamente.
- frontend_dist final: **78 arquivos**, todos no índice Git, árvore
  **e9d02144d1ce7e4a58affbf6cc96c01c33d3ae85ce9f264a79d6621a65cf90b7**.
  TypeScript/build/verify aprovados. 63 testes frontend e lint direto dos arquivos
  finais aprovados (2 avisos preexistentes de any; nenhum erro).
- e2e-permission-review-publish.log: **1 passed**, 10,1 s, SPA final instalada,
  servidor iniciado após instalação/prova e encerrado após execução. APIs em
  fixtures; sem tráfego de escrita para Pulse real.
- closure-permission-review-publish.json: **ok=true**, findings e
  documentation_findings vazios; todos os oito budgets 0/0, 7.513/1.151 imports
  e 25 dependências. Ruff de todos os Python alterados e staged diff --check
  aprovados. Nenhum processo de build/teste/auditoria/servidor descartável ativo.
- SHA256 dos wheels finais:
  Core af3ff410366514527b9ef789098867a51c7a4d1d5a462a5a9ffafaed31b30a11;
  Community 89d379a4c66cb1cbc7300c11f4b802ea6505446590785c3ff5d8e6e69254e45e.

Community commit **eb6ec25c5438f4174c383979eabf168e65854485**; Core publica neste
commit value object, policy canônica/lineage com review persistido, classificação
de origem, writers via records, testes e ledger. Push normal em feature/v0.4.0
dos dois repos. Nenhuma migração de banco real, mudança de policy real, restart,
release, tag ou merge. Metadata global segue 55.859 >50.800; nenhuma conclusão
de F2/F3 ou da iniciativa inteira. Retomada pelas pendências registradas acima.

### 2026-09-20 — F2/F3: limpeza transacional dos flags já retirados (em execução)

Continuidade após Core 3b2e9b9e / Community eb6ec25. Implementando operação
interna, ainda sem ligação ao bootstrap ou execução em banco real, que instala
os reviews capturados e remove somente os 14 leaves KG já ausentes do registry.
O Core fornece a transformação pura; SQL, fence BEGIN IMMEDIATE e journal
permanecem em Community. Preservar extensões desconhecidas, sentinelas e todos
os valores sobreviventes. Reter antes/depois dos documentos alterados e recibo
externo de conclusão, inclusive quando nenhuma linha mudar. Retomada concluída
verifica evidência sem reaplicar alterações sobre decisões posteriores do owner.

Gates planejados: fonte/população idênticas ao checkpoint antes da primeira
limpeza; paridade de todas as decisões sobreviventes e razões de review antes
e depois; releitura do estado persistido; rollback conjunto de flags, marcadores
e journal em qualquer divergência. Testar corrupção/remoção do journal, replay
após edição autorizada, fontes alteradas, limites e falha durante a transação.
Não declarar removido Sprint nem concluídos F2/F3. A integração do coordenador,
a remoção do registry Sprint e as demais dependências continuam pendentes.
Build/proveniência, testes e auditoria deste incremento ainda não executados.

Validação do incremento de limpeza:
- provenance-permission-cleanup.json: 803/322 .py e 868/406 payloads,
  fonte/wheel/install byte a byte; imports em site-packages e processos novos.
- Core: 392 passed (15,27 s), incluindo goldens v0.3.4, review, transformação
  pura, paridade e contrato da porta. Fonte e extensões desconhecidas preservadas.
- Community primeira rodada: 36 passed e 1 falha na asserção do teste, que
  confundia o leaf legítimo agent.api_key.rotate com material de credencial.
  Corrigida para verificar os valores SECRET e o campo api_key_hash; nenhuma
  remoção de permissões legítimas. Rodada corrigida: 39 passed (47,53 s), com
  rollback após gravações/antes do commit, corrupção e remoção do journal,
  fonte/review alterados, replay após decisões posteriores e identidade nova,
  competição de writer bloqueada e preservação de ramo de extensão malformado.
- Frontend: 63 passed / 6 arquivos (12,71 s), inclusive deny durante review,
  edição de preset apenas após interação e leitura do arquivo histórico.
  Nenhuma alteração no bundle nesta etapa; SPA continua a versão já verificada.
- Closure inicial: findings=[], oito budgets 0/0; somente matrizes README
  divergentes pelo novo módulo. Renderer oficial aplicado para 7.514 imports
  Core / 1.152 Community→Core / 25 dependências. Preparar par final e repetir
  auditoria sobre o payload publicado, sem relaxar baseline ou budget.

Dependência de cutover que deve permanecer explícita: capturar/verificar os
arquivos e instalar seus grants históricos ANTES de transformar documentos de
permissão. O avaliador histórico v0.3.4 reconhece o Full Control original com
599 leaves; não recapturar autoridade histórica a partir da árvore já reduzida.
O coordenador ainda deve impor essa ordem, reter os recibos de checkpoint e
cleanup (também no caso zero alterações) e só então integrar a transformação
Card/Sprint e o corte de schema. Esta operação continua interna, sem chamada no
bootstrap; seu teste isolado não prova a ordenação do cutover completo.

Mecânica final: transaction helper compartilhado para instalação dos reviews;
releitura compara o documento persistido integralmente e verifica todas as
permissões sobreviventes; um trigger que altere policy/identidade durante a
limpeza causa rollback. Journal único, delimitado a 64 MiB, com árvores antes/
depois e caminhos removidos, hash e contagens. Replay valida o journal e o
recibo de instalação sem reclassificar agentes nem reaplicar reviews liberados.

Publicação da limpeza:
- Par final wheels-permission-cleanup-publish instalado e comprovado em
  provenance-permission-cleanup-publish.json: 803/322 .py e 868/406 payloads.
  O Python é idêntico ao validado; apenas as matrizes README mudaram no par.
- closure-permission-cleanup-publish.json: ok=true, findings=[] e
  documentation_findings=[], todos os oito budgets 0/0. Ruff e staged
  diff --check aprovados; nenhum servidor/teste/build/auditoria em execução.
- Wheels SHA256:
  Core 45906054ca024d938319e694ccf74016d275b2cb230506dfafcad4b8de672b1e;
  Community ab1cf0c3fbed0f7d86f1866fb3cfe420e8bc699c2aa90329c1e7000d672e9835.
- Community commit 9f75493d3cbc3151fc483d63de5417d72300c94a. Core publica neste
  commit a transformação pura, testes, matriz gerada e ledger. Push normal dos
  dois repositórios em feature/v0.4.0 após verificação, sem release/tag/merge.

Próxima continuidade: coordenar a materialização dos overrides Card e a
preservação histórica antes da limpeza/corte, impondo dependências executáveis
em F2D. Ainda faltam as superfícies genéricas de grants/MCP, contextos
substantivos, retirada de Sprint e matriz completa dos complementos. Não
executada migração de banco real nem restart do Pulse. Metadata global continua
55.859 >50.800; este incremento não encerra F2/F3 nem a iniciativa.

### 2026-09-20 — F2B: materialização por Card com pré-condição histórica (em execução)

Continuidade após Core adc40f70 / Community 9f75493, limpos e publicados.
Implementando a gravação dos overrides autorizados e a remoção conjunta de
Card.sprint_id, em transação SQLite reservada. Core fornece fachada pública de
planejamento/paridade; Community só carrega fatos e persiste. O deprecation
warning permanece no contrato e na operação: compatibilidade migrada, sem
nova autoria pelo executor, sem expiração automática.

Pré-condições executáveis: referências completas do arquivo v4, blobs íntegros,
evento histórico confirmado, grants já instalados, recaptura exatamente igual
antes de alterar vínculos. Arquivo ausente, stale ou com escopo incompleto
bloqueia a etapa. O journal por Board retém facts, resultados antes/depois,
origem da compatibilidade e digests das linhas completas. Somente diferenças
por campo são materializadas. SQL bruto evita onupdate de timestamp ou eventos
operacionais: IDs, Spec, estado, responsável e histórico permanecem intactos.

Esta etapa ainda não está no bootstrap e não substitui o coordenador F2D/F3:
transferência de contexto substantivo, jobs, referências KG e corte de schema
continuam dependências antes de habilitar o novo runtime. Não executado banco
real. Testes/build/proveniência desta etapa ainda pendentes.

Evidência intermediária e ajuste de integração:
- provenance-card-retirement.json: 804/323 .py e 869/407 payloads,
  fonte/wheel/install byte a byte. Core 39 passed (4,50 s); Community 53 passed
  (113,81 s); frontend 69 passed / 3 arquivos (34,02 s), incluindo CardModal.
- Ensaio encadeado community-card-retirement-sequence.log: 2 passed (13,56 s).
  Arquivo+grants → Card policy/link → permission cleanup preserva leitura por
  agente com Full Control original. Antecipar permission cleanup bloqueia a
  recaptura do arquivo; ambas as etapas retomam depois pelo recibo original.
- Revisão encontrou integração inadequada no journal inicialmente inline:
  fatos de todos os Cards no DomainEvent poderiam exceder 131.072 caracteres /
  5.000 nós do inventário e gerar referência Sprint não classificada. Antes de
  publicar, alterar para artefato de auditoria no StorageProvider já existente,
  com evento leve contendo referência, SHA256, tamanho e contagens. Não alterar
  o classificador para ignorar um novo payload desconhecido nem elevar limites.
- Rollback deve limpar blobs criados antes de tentar commit; commit incerto
  conserva blobs para reconciliação, igual à captura histórica existente.
  Testes adicionados para falha no segundo blob, limite da camada de policy,
  FK órfã/cross-Board e inventário após transformação. Novo par audit em curso;
  os verdes intermediários não substituem a validação desse formato final.
- Closure intermediário: findings=[], oito budgets 0/0, apenas matrizes README.
  Evidência observada: 7.519 imports Core, 1.154 Community→Core, 25 dependências.

Formato final e revisão:
- Journal card-validation-retirement/v1 gravado como artefato privado pelo
  StorageProvider; evento migration.card_validation_preserved guarda somente
  referência, hash, tamanho e contagens. Sem linha de attachment público ou
  endpoint novo. Verificada rota attachments: GetCardAttachmentUseCase exige
  registro/autorização; main monta somente assets da SPA, não uploads.
- Rodada do formato audit: 59 passed (131,83 s), incluindo inventário de trabalho
  após a transformação e as duas sequências de cutover. A auditoria não cria
  referência Sprint ativa nem exige relaxar classificadores/limites existentes.
- Ajuste final: comparar novamente todas as linhas e camadas de policy DEPOIS
  dos inserts do journal, antes do commit. Um trigger no próprio evento que
  tente alterar assignee_id também deve causar rollback de SQL e blobs. Mais
  um teste adicionado; rodada final em execução.
- provenance-card-retirement-publish.json: par final instalado, 804/323 .py,
  869/407 payloads, igualdade fonte/wheel/install e origem em site-packages.
- closure-card-retirement-publish.json: ok=true, findings=[],
  documentation_findings=[], oito budgets 0/0; 7.519/1.154 imports e 25 deps.
  READMEs atualizados pelo renderer oficial. Ruff aprovado.
- Wheels finais SHA256:
  Core 0e5862a21e8fab28b70bc0140f23fcf1b1500fecd5e8938a82420c7709e015e8;
  Community a1e7f9141d70a19211a6ce35bf0b5c0ca9138034331cca201d682fbb54d232ff.

Publicação F2B materialização por Card:
- community-card-retirement-publish.log: **60 passed**, 131,32 s, todos os
  processos encerrados. Inclui gravação e paridade por campo; proteção de todos
  os demais bytes das linhas; arquivo/grants obrigatórios e origem completa;
  hotfix cross-Spec; False/zero e herança dinâmica; vazio sem override; corrupção
  e perda de journal; rollback SQL/artefatos e triggers antes e depois dos
  eventos; limite e writer concorrente; FK inválida; sequência F2A/F2B/cleanup.
- Core: **39 passed**, contrato público e regras/DTOs migration-only; Python do
  Core idêntico ao validado. Frontend: **69 passed**, CardModal, thresholds e
  painel histórico; bundle sem alterações nesta etapa. Total selecionado 168.
- Ruff e staged diff --check aprovados. Community commit
  **33853f430ad05467103c37a97c0cecbfd4f5bc01**. Core publica neste commit porta
  pública pura, testes, matriz e ledger. Push normal de ambos em feature/v0.4.0.

Próxima continuidade concreta: integrar essas etapas no coordenador F2D e
resolver transferência dos contextos substantivos / supersedência auditada de
trabalho exclusivo antes de retirar o schema/runtime Sprint. Não chamar captura
histórica novamente depois da transformação: retomar pelos recibos verificados.
Os arquivos originais continuam necessários e seus grants atuais/revogações são
preservados. Não criar novo baseline de policy sobre Cards já migrados.
Permanecem também superfícies genéricas de grants/MCP, F3/F4/F5, complementos e
matriz integral. Metadata global segue 55.859 >50.800. Nenhum banco real migrado,
restart, release, tag ou merge; iniciativa continua ativa e incompleta.

### 2026-09-20 — F2C: supersedência de trabalho exclusivo (investigação/implementação)

Continuidade após Core 6ec6ee34 / Community 33853f4, limpos e publicados.
A classificação pura já distingue eventos Sprint exclusivos, eventos Card
mistos e execução/queue desconhecida ou em voo. Falta materializar o estado
superseded com proveniência, sem marcar done/processed_at nem alterar tentativas,
erros e payloads originais. Investigação encontrou requeue/upsert que reabre
qualquer terminal; protegê-los antes de instalar esse estado.

Ordem integrada: arquivo+grants → recibo de preservação por Card → supersedência
→ limpeza de permissões. Comparar as linhas de trabalho com o arquivo original,
pois recaptura total após detach dos Cards conflita corretamente com card_links
arquivados. Exigir o recibo Card e validar os arquivos/grants sob fence SQLite.
Mixed Card facts e execuções done ficam íntegros; claimed/processing, contrato
novo ou handler desconhecido bloqueiam. Não inventar processamento de Sprint.

Journal planejado: eventos pequenos por origem de trabalho, com hash da linha
antes/depois e referência ao arquivo, mais fechamento por Board com contagem/
hash agregado. Os dados originais já estão no arquivo imutável; não embutir
snapshot grande no DomainEvent nem relaxar limites do inventário. Retomada deve
verificar evidência e não reexecutar mutações. Sem bootstrap ou banco real nesta
etapa; testes e validação ainda não executados.

Implementação e investigação adicional:
- Porta pública pura `ports/work_retirement.py`: classificadores históricos,
  status terminal e contrato do journal. Nenhuma mecânica SQL no Core ou novo
  reach-in Community; os classificadores não são prova de proveniência sozinhos.
- `sprint_work_retirement.py` valida arquivos/grants já instalados e recibo Card,
  compara população e bytes SQL originais sob BEGIN IMMEDIATE, muda somente
  status de trabalho exclusivo e grava journal `archived-work-retirement/v1`.
  Payloads, tentativas, erros, timestamps e eventos originais ficam intactos.
  Recibo vincula hashes/contagens ao arquivo original e à etapa Card. Replay
  verifica evidência e terminais sem impedir progresso do trabalho misto válido.
- Investigação do caminho DLQ: `route_to_dead_letter` remove a linha da queue.
  Portanto status terminal sozinho não impede recriação. Journal inclui
  `migration.work_origin_retired` para CADA origem arquivada, inclusive Sprint
  vazia/sem queue. Fence SQL por Board+tipo+ID barra upsert/reconcile/retry/DLQ
  de origem retirada, sem converter falha histórica em sucesso.
- DLQ retirada permanece armazenada, fora de contagens/listas operacionais e
  da limpeza automática de poison. Replay explícito misto falha antes de mudar
  qualquer linha; seleção automática ignora histórico e continua entregando
  Cards válidos. Health global, por Board e census usam o mesmo predicado.
- Testes: 39 Core aprovados; 14 novos Community aprovados inicialmente. Mais
  35 regressões aprovadas e duas falhas na fixture mínima de segurança CT por
  ausência de `domain_events`. Incluir a tabela consultada, sem alterar as
  asserções de autorização. Rodada final de migração+CT: **16 passed**, 51,70 s.
  Inclui rollback, concorrência, drift/remoção de journal, mixed delivery,
  callbacks tardios, origem vazia, reconcile e DLQ sem linha de queue.
- Frontend: **87 passed**, quatro arquivos KG Health/pending; teste de contagem
  inclui zero após retirada e separação do outbox. Nenhum bundle alterado.
- Prova `provenance-work-retirement-origin.json`: 805/325 .py e 870/409 payloads,
  fontes/wheels/install idênticos e imports de site-packages antes dos testes.
- Closure intermediário: findings=[], oito budgets 0/0; somente matrizes README
  regeneradas pelo renderer oficial. Observado 7.520/1.160 imports e 25 deps.
  Par final/documentação ainda em validação; nenhum commit desta etapa ainda.

Limites de integração: etapa interna, não registrada no bootstrap. Coordenador
F2D deverá validar TODOS os blockers antes de qualquer transformação e drenar
processos/handlers em voo; código F3 e schema precisam ser promovidos juntos.
Não recapturar baseline histórico depois da etapa Card. Não supor que este passo
remove produtores Sprint, refs do grafo/outbox ou transfere contexto substantivo.
Esses trabalhos e toda a matriz integral permanecem pendentes. Nenhuma alteração
de permissões/grants atuais, banco real, restart, release, tag ou merge.

Fechamento/publicação deste incremento F2C:
- Fixture DLQ reforçada: origem retirada com 9 tentativas (limite poison=4),
  origem Card com 3. Teste dirigido aprovado, **1 passed**, 9,64 s; histórico
  acima do limite permanece, Card reprocessa, census/health reportam zero DLQ
  operacional e uma entrada ativa. Não somar essa repetição ao total.
- `community-work-sequence.log`: **2 passed**, 17,70 s, sequência integrada
  agora inclui work retirement e replay depois da limpeza de permissões.
- Total selecionado sem duplicar repetições: **177 testes aprovados** — 39 Core,
  51 Community, 87 frontend. Os 35 verdes da rodada de regressão são reutilizados;
  as duas falhas de fixture foram resolvidas e aprovadas na rodada final. Ruff
  dos arquivos alterados e staged diff --check aprovados; processos encerrados.
- `provenance-work-retirement-final.json`: 805/325 .py, 870/409 payloads, mesma
  prova byte a byte e origem instalada. README alterou metadata do wheel, não
  código Python. `closure-work-final.json`: ok=true, findings=[],
  documentation_findings=[], oito budgets 0/0; 7.520/1.160 imports, 25 deps.
- Wheels finais SHA256:
  Core 34c5990ae666578a6bc92bc1c4630ffe5d8b7d14a0cab81d9e237690d0d490cb;
  Community e14e338e2d634c7a06314f4f512af279a9ac3355495415cd5bac97dba8462155.
- Frontend empacotado: 78 arquivos intactos, árvore
  e9d02144d1ce7e4a58affbf6cc96c01c33d3ae85ce9f264a79d6621a65cf90b7.
- Community commit **15469e7f9ae0b0b06afdc37b7eb0bc3b835e1e3b**. Core publica
  neste commit contrato puro, classificador, testes, README e ledger; push
  normal dos dois em feature/v0.4.0, sem tag/release/merge.

Retomada concreta: preflight único F2D deve identificar blockers de contexto
substantivo e referências remanescentes ANTES da etapa Card. Não habilitar a
migração interna enquanto F3 ainda produz Sprint. Integrar checkpoint original,
arquivo/grants, Card receipt, work receipt e permission receipt sob coordenação
durável que impeça inicialização parcial e não recapture fonte transformada.
Remoção real do schema, upgrade=clean, rollback conjunto e todos os demais
complementos continuam pendentes. Metadata global permanece 55.859 >50.800.
A iniciativa está ativa e incompleta; estes verdes cobrem somente o incremento.

### 2026-09-20 — F2A/F2D: preflight de contexto antes de transformar Cards

Turno anterior: progresso verificado/publicado, Core 5417c017 / Community
15469e7, worktrees limpas e remotos iguais. Iniciar esta etapa por investigação
do contrato real, sem procurar a ideação original.

Evidência: `SprintQAItem` identifica pergunta aberta por answered_at IS NULL;
answer_question pode gravar selected ou mesmo resposta nula com timestamp, logo
truthiness de answer não reproduz o contrato. `Sprint.evaluations` é JSON com
recomendação, dimensões/justificativas, autoria, stale e data; não há identidade
ou resolução individual de achado. Approve/stale/closed não provam que texto
substantivo foi transferido ou deixou de ser aplicável. Description/objective/
expected_outcome, Q&A respondido e histórico também podem conter decisões.

Próxima alteração: inventário privilegiado e limitado desses conteúdos, com
origem/campo/hash e Spec estrutural, sem inferir destino Card a partir de prosa.
Preflight reúne relações, trabalho, escopos de referências e contextos antes
do primeiro UPDATE de Card. Conteúdo que ainda exige disposição explícita
impede transformação; captura histórica continua possível para investigação.
Nenhum novo gate de produto, cópia para Q&A público de Spec, aprovação sintética
ou mudança de ACL. Transferência concreta e disposição semântica permanecem a
resolver; um preflight sem candidatos não certifica todo o cutover F2D/F3.

Detalhamento da investigação/implementação:
- A ausência de resolução individual acima é específica de Sprint.evaluations.
  Há findings estruturados separados de policy compliance e semantic guidelines;
  QualityFinding atual limita subject/anchor a ideation/refinement/spec. Censos
  físicos existentes continuam necessários para variantes históricas. Nenhum
  recibo/waiver preso a Sprint pode virar autoridade de Spec por remapeamento.
- `ports/retirement_context.py` faz triagem pura de texto, avaliações, Q&A e
  histórico. Não decide aplicabilidade por approve/stale/score/status. Q&A
  respondido também pode conter decisão; apenas o rótulo aberto segue o relógio
  de resposta, incluindo respostas somente por escolha.
- `sprint_retirement_preflight.py` combina os inventários no mesmo snapshot;
  inclui referências de governança e fontes cognitivas embutidas. Apenas fontes
  mecânicas já classificadas têm tratamento separado. Diagnósticos contêm
  origem, chave, caminho, Spec estrutural e hash, sem copiar prosa para logs.
- Primeiro Card transform chama esse preflight sob o próprio BEGIN IMMEDIATE,
  antes de recaptura/mutações. Replay concluído continua verificando recibo
  original, sem reexecutar etapa por mudanças posteriores. Captura de arquivo
  permanece permitida para investigação mesmo quando contexto impede migração.
- Testes de trabalho desconhecido/em voo agora verificam impedimento ANTES
  da etapa Card, com bytes de Cards e filas intactos. Acrescentada reprodução
  de trabalho que entra em voo depois do Card receipt, ainda bloqueado em F2C.
- Par inicial instalado/provado: 806/326 .py, 871/410 payloads. Core: 60 passed,
  4,16 s. Community e closure ainda em execução; sem publicação nesta etapa.

Revisão e segunda validação:
- Primeira rodada Community: **45 passed**, 172,75 s, nos testes de preflight,
  supersedência, preservação de Card e sequência integrada. Closure intermediário
  sem findings e com oito budgets 0/0, apenas matrizes README; renderer oficial
  atualizou 7.522 imports Core / 1.161 Community→Core / 25 dependências.
- Reforçar source_sha256 para incluir TODAS as colunas físicas da linha de
  origem, incluindo autoria, datas e versão. Não aceitar coluna desconhecida
  nas tabelas próprias de Sprint como se não pudesse conter uma decisão.
  Testes adicionais alteram somente a autoria e introduzem coluna não mapeada.
- Par final instalado e comprovado em provenance-context-preflight-final.json:
  806/326 .py, 871/410 payloads, fonte/wheel/install byte a byte. Pytest do repo
  ativa as duas árvores locais em conftest; elas são idênticas ao par instalado
  comprovado antes da execução. Não editar código nem reinstalar durante testes.
- Wheels finais SHA256:
  Core 69ee3b3c756e198b4e70e16b7b056f8841ea38d2a81fdeb9b32d7347e0c0c796;
  Community b82503727d650a7661e443e1a3b5572c079c2bf1ed71b8afab5ed46ab349df94.
- Rodada final Community e closure em curso. Core Python permanece idêntico ao
  validado; frontend não foi alterado por este preflight interno.

Publicação deste incremento:
- `community-context-final.log`: **47 passed**, 175,08 s. Cobertura inclui
  perguntas abertas/choice respondido, avaliações stale/approve, prosa e história,
  contexto externo por recibo/ref cognitiva, hashes sensíveis a autoria/conteúdo,
  limites compartilhados de linhas/bytes, schema desconhecido/incompleto,
  replay e bloqueio anterior à mutação de Card. Nenhum conteúdo foi promovido
  a aprovação ou copiado para uma superfície de leitura mais permissiva.
- Core **60 passed**; total selecionado **107**, sem duplicar rodada inicial.
  Ruff e staged diff --check aprovados. Todos os processos de teste encerrados.
- `closure-context-final.json`: ok=true, findings=[], documentation_findings=[],
  oito budgets em 0/0. Mesmo par final/hash/proveniência acima.
- Community commit **d2f8230f5915906e078ceb90135791f7ac0b4103**; Core publica
  neste commit contrato puro, testes, README e ledger. Push normal em ambos.

Próxima continuidade: implementar disposição explícita vinculada ao arquivo,
hash e origem do candidato, distinguindo contexto aplicável a Spec/Card de
histórico administrativo. Reutilizar as autorizações por seção já aprovadas;
não despejar conteúdo em SpecQA/KB, não transformar parecer de Sprint em parecer
de Spec, não alterar resolução de finding/waiver. As quatro projeções históricas
atuais cobrem content/qa/evaluations/history; registros de governança e IDs de
Cards/cenários/regras não estão automaticamente autorizados por essas seções.
Investigar e manter essa separação ao concretizar o vínculo de contexto.

Preflight não é conclusão de F2D: disposição/transferência, coordenação durável,
corte de schema com F3, fontes/outbox/grafo e a matriz inteira seguem pendentes.
Metadata global continua 55.859 >50.800. Sem banco real migrado, restart, release,
tag ou merge. Objetivo integral permanece ativo e incompleto.

### 2026-09-20 — F2A: disposição explícita e vínculos de contexto (em implementação)

Turno anterior classificado como progresso: Core dd7b2b74 / Community d2f8230,
limpos e publicados. Estado atual conferido antes de continuar.

Contrato planejado: entrada privada do operador de migração, com referência da
decisão, justificativa e conjunto exato de candidatos/hash. Não é endpoint de
executor, credencial, nova aprovação nem alegação de revisão autenticada. Cada
candidato recebe retenção histórica explícita ou vínculo de contexto a Spec/Card
do mesmo Board; não existe default automático baseado em score/stale/status.
Arquivo e grants originais devem estar verificados antes de instalar disposições.

Não copiar conteúdo para SpecQA/KB nem alterar finding, waiver, estado ou policy.
Vínculos só podem apontar às quatro projeções já autorizadas; fontes adicionais
continuam exigindo tratamento próprio. Justificativas/entrada completa ficam em
artefato privado pelo StorageProvider, evitando introduzir prosa em DomainEvent
e repetir o problema de payload/ref Sprint observado na etapa Card. Eventos
leves guardam hashes/contagens e vínculos opacos; futuros leitores devem autorizar
o destino e a seção original ANTES de abrir esse artefato. Integração com leitura,
preflight e recibo Card ainda em implementação; nenhum teste desta etapa rodou.

Implementação/validação desta etapa interna:
- Core `ports/context_disposition.py`: contrato fechado/frozen para candidato,
  ação retain_history/bind_context, justificativa e destinos Spec/Card; rejeita
  aprovação sintética, população duplicada, binding sem destino, destinos
  repetidos e escopo cross-Board. Referência da decisão é evidência submetida
  pelo operador, não uma alegação de autenticação/revisão que o sistema produziu.
- Community `context_disposition_retirement.py`: BEGIN IMMEDIATE; verifica
  arquivos e grants já instalados, conjunto exato de candidatos, hashes das
  linhas arquivadas e destinos no Board. Recaptura original antes e depois;
  snapshots completos de destinos detectam triggers que alterem title/estado/
  autoria/policy. Não muda fontes, destinos, permissões, findings ou waivers.
- Artefato privado por Board contém decisões/justificativas e registros de
  auditoria completos. DomainEvents leves: migration.context_dispositions_committed
  e historical_context.bound. Não criar Attachment público. A referência e
  justificativa de teste começam com `sprint:` para provar que prosa privada
  não entra no inventário de eventos/fatos ativos. Nenhum classificador relaxado.
- Replay verifica arquivo/journal original e o plano fornecido, não recaptura
  fonte transformada nem reaplica decisões após edições legítimas. Ausência/
  corrupção de journal, binding, blob ou recibo falha; não reparar evidência.
  Falha antes de commit desfaz SQL e remove somente blobs criados na tentativa;
  commit incerto conserva artefatos para reconciliação.
- `core-disposition.log`: **28 passed**, 3,68 s. `community-disposition.log`:
  **17 passed**, 102,33 s. Total selecionado 45; todos os processos encerrados.
  Inclui não mutação, população incompleta/extra, destino inexistente/cross-Board,
  replay/drift, falhas SQL/target/source/segundo blob, writer concorrente e
  impedimento de expor recibo de governança pelas quatro seções existentes.
- Par inicial provado antes de testar: 807/327 .py, 872/411 payloads idênticos
  fonte/wheel/install. Ruff aprovado. Closure intermediário sem findings,
  oito budgets 0/0; READMEs regenerados oficialmente para 7.524/1.162 imports e
  25 deps. Par final está em instalação/prova/auditoria de documentação.
- Wheels finais SHA256:
  Core 48e34e5191f427a05e2c1b7e6e759fd72a5b508e012da41991d6e7a9893e6b5e;
  Community 746742e91bc4f4079883f822eb1bb421210cdbfc5e696c2a04e6053a5066a13f.

Limite deliberado desta publicação: não ligar disposition receipt ao Card
transform antes de existir leitura autorizada dos vínculos no destino. O
preflight continua exigindo disposição; registrar journal sozinho NÃO declara
concluída a transferência para o usuário. Nada é exposto em REST/MCP/frontend
nesta etapa; não há UI modificada. Próxima ação é o leitor que primeiro autoriza
Spec/Card, depois a origem/seção original, verifica artifact+binding e só então
projeta o conteúdo com autoria/proveniência. Integrar esse leitor e seus testes
de frontend, e então consumir recibo no preflight/Card sob o fence existente.

Fechamento desta etapa interna: par final reinstalado/provado em
`provenance-context-disposition-final.json` (807/327 .py; 872/411 payloads).
O processo verificador importou site-packages; pytest ativa checkouts cujos bytes
foram provados idênticos antes dos testes. Closure final
`closure-disposition-final.json`: ok=true, findings e documentation_findings
vazios, oito budgets 0/0. Community commit 7dbedfc; publicação do par a seguir.
Objetivo integral permanece ativo; leitor e integração Card ainda pendentes.

### 2026-09-20 — F2A: leitor interno de contexto vinculado (em implementação)

Publicação anterior confirmada: Core d76a54f46e4fb5eebe1697fa111ffa3ea5bd4443 /
Community 7dbedfc56e93a90f1a4c69daa44e0fdf1983c36a; HEADs iguais ao remoto,
ambas as working trees limpas antes desta etapa.

Porta Core e caso de uso adicionados para ler vínculos de um destino Spec/Card.
O destino usa o acesso existente de entidade/Board; para agentes, a resolução
atual deve também satisfazer a folha entity.read do destino. Snapshot único,
Board atual, grant original e revogações por seção precedem conteúdo. Filtragem
antes da paginação evita revelar contagens/IDs de seções negadas. Limites de
100.000 candidatos/64 MiB de metadados e 25 MiB de saída agregada.

Adaptador Community registrado no UoW: somente eventos leves na descoberta;
leitura revalida destino e grant antes de abrir arquivo original ou audit privado.
Binding é reconstruído a partir da decisão tipada e linha original hashada;
seleção passa pelas mesmas allowlists do leitor histórico. Objetivo/descrição/
resultado esperado vêm com autoria/datas originais; Q&A e avaliações permanecem
históricas, sem resposta/aprovação nova. Fontes e destinos não são escritos.

Nenhum endpoint, catálogo MCP ou frontend alterado ainda. Não consumir receipt
no Card transform até a integração REST/MCP/UI e respectivos testes de frontend.
Esta etapa está em build/prova do par antes dos testes; validação ainda pendente.

Investigação da primeira rodada: 146 testes Core passaram; Community terminou
com 57 passed / 10 failed (269,80 s). As falhas novas eram EntityNotFound no
lookup de destino, antes da autorização histórica. Evidência: fixture de
inventário cria Board.realm_id NULL; application_persistence filtra destinos
com Board.realm_id == realm local, enquanto o leitor de origem mantém sua
compatibilidade histórica explícita com NULL. Correção: preparar o realm local
na fixture ANTES da captura; manter o filtro de entidade existente. O adaptador
de contexto também exige realm explícito na consulta de destino, inclusive para
chamadas diretas. Nenhuma ampliação de ACL nem alteração na compatibilidade de
leitura da origem. Nova prova do par e reexecução dos casos novos pendentes.

Fechamento do leitor interno:
- `core-context-reader.log`: 146 passed (6,79 s).
- Primeira rodada Community: 49 regressões históricas/disposições aprovadas;
  os 18 casos do leitor novo foram integralmente reexecutados após a correção,
  mais dois novos casos (histórico cross-Spec e realm legado no port direto).
- `community-context-reader-final.log`: 20 passed (139,65 s). Total distinto
  desta etapa: 146 Core + 49 regressões Community + 20 leitor = **215**.
  Não somar os oito casos novos que já passaram na rodada inicial novamente.
- Todos os processos de teste/build/install/closure encerrados. Ruff e diff
  --check aprovados. Nenhuma fonte/reinstalação alterada durante testes ativos.
- `provenance-context-reader-final.json`: 809/328 .py e 874/412 payloads com
  igualdade byte a byte fonte/wheel/install. Verificador importa site-packages;
  pytest usa checkouts ativados pela fixture, provados idênticos previamente.
- `closure-context-reader-final.json`: ok=true, findings=[] e
  documentation_findings=[]; oito budgets 0/0. READMEs regenerados pelo renderer
  oficial: 7.538 imports Core, 1.167 Community->Core, 25 dependências.
- Wheels em `.validation-v040/wheels-context-reader-final`, SHA256:
  Core 20406245eb01217ecc62c563fb88ac581c56e382f3a6ed12eec23fc09bc3c600;
  Community 12f56629a96b8bc5e64ea7ff09e6eb7e77a08c71149877c4da77316442783f03.
- Community commit 95b08d3; Core acompanha este registro. Push normal do par
  será conferido por HEAD == ls-remote, sem merge/tag/release/migração real.

Próximo incremento concreto: API em `api/historical_archives.py` com envelope
fechado/no-store para o caso de uso; cliente/painel de contexto nos destinos
`frontend/src/components/specs/SpecModal.tsx` e `components/kanban/CardModal.tsx`,
com teste de parser, revogação/erro/paginação/troca de destino e integração dos
dois modais. Em seguida superfície MCP e testes de autoridade/contexto, geração
oficial de catálogo se registry mudar. Só depois ligar disposition receipt ao
Card transform sob o fence já existente. Não inferir que links ausentes no
runtime provam completude da migração: a conferência integral de população e
journal continua sendo responsabilidade do verificador de disposições.

Nenhum frontend alterado nesta etapa interna. Integração visível, autorização
administrativa de grants, imutabilidade operacional e coordenador F2D continuam
pendentes, assim como as demais frentes do plano. Objetivo integral ativo.

### 2026-09-20 — F2A: contexto histórico nos destinos REST/frontend (em implementação)

Turno anterior classificado como progresso: Core d12b01c3 / Community 95b08d3,
HEADs/working trees conferidos limpos antes da edição. Rota somente GET, fechada
em Spec/Card, usa o caso de uso publicado e envelope sem caminho privado,
justificativa de operador ou permissão nova. Respostas no-store; erros públicos
sem conteúdo parcial ou detalhe interno. Aba Historical context nos dois modais,
sem contagem antecipada de fontes negadas, campos históricos inertes e sem ação
de escrita/aprovação. Troca de Board/destino cancela consulta e desmonta conteúdo.

Testes planejados: rota real com UoW, paginação/revogação/erro/integridade;
parser e hook, painel (corridas, troca de destino, revogação, refresh, HTML inerte),
integração dos modais Spec e três tipos Card; Playwright contra SPA instalada,
fixtures REST fechadas para não escrever dados reais, light/dark e 360/768/1440.
Frontend em build; ainda nenhum teste comportamental rodado nesta etapa. Provar
par fonte/wheel/install e runtime novo antes dos testes. MCP e integração do
receipt no Card continuam pendentes; o preflight não foi liberado.

Primeira rodada: 22 testes Community/API passaram (173,81 s); frontend 116
passed / 1 failed, exclusivamente a lista exata de abas da Spec (agora inclui
Historical context). Corrigida expectativa para a nova aba. E2E Spec passou;
Card não chegou ao modal porque a fixture de colunas omitia columns_meta.columns
exigido pelo store real. Fixture corrigida segundo ColumnsOptInResponse; nenhum
código de produto alterado por essas duas falhas. Screenshot mobile da Spec
inspecionado: origem/autoria/texto inerte legíveis, sem overflow no painel.

Lint global: 0 erros, 394 warnings <= baseline 402 (baseline não modificado).
Closure inicial: findings=[] e budgets 0/0, somente matriz README desatualizada;
renderer oficial executado. Build frontend: 78 arquivos, tree SHA256
45a4b8fb4b8d938d389574939010505976913141eaebe29ae609307e59c496be.
Par inicial provado: 809/328 .py e 874/412 payloads. Servidor estático instalado
5176 iniciou 15:33:27/28 depois do mtime instalado (verificação executável); não
é runtime Pulse nem acessa banco real. Encerrar antes da reinstalação final.

Fechamento REST/frontend:
- Community e4916cc: GET `/boards/{board_id}/historical-context/{spec|card}/{id}`,
  cliente/painel, abas Spec/Card, testes e SPA regenerada. Nenhuma escrita nova.
- `community-context-ui.log`: 22 passed (173,81 s), API com UoW/ACL/arquivo real
  em fixture descartável + regressões do leitor.
- `frontend-context-tests.log` + `frontend-context-spec-final.log`: 117 casos
  distintos aprovados (25 Spec reexecutados integralmente, 18,31 s); não duplicar
  os 24 casos da Spec já verdes na primeira rodada. Inclui parser/hook, painel,
  tab routing e modais Spec/Card (normal, bug, test).
- `frontend-context-e2e-final.log`: 2 passed (12,8 s), Spec e Card reais na SPA
  instalada, três larguras x dois temas, verificação axe sem serious/critical,
  leitura sem overflow, autoria/proveniência/texto inerte, negação na próxima
  página remove conteúdo anterior. Nenhum request de escrita nem pageerror.
  Screenshots `spec-context-360.png` e `card-context-360.png` inspecionados.
  HTTP de API interceptado por fixtures: comprova integração da SPA instalada,
  não é alegação de um cutover/migração real end-to-end. API real validada acima.
- Total distinto selecionado desta etapa: **141** (22 Python + 117 Vitest + 2 E2E).
- Build, typecheck final, verify:frontend-dist, lint global, Ruff e staged diff
  --check aprovados. 78 arquivos da SPA versionados pelo sync oficial.
- `provenance-context-ui-final.json`: 809/328 .py, 874/412 payloads idênticos
  fonte/wheel/install. `frontend-context-server-final.json` prova servidor novo
  (15:38:51) posterior ao índice instalado (15:37:57), servindo site-packages.
  Ambos os servidores descartáveis foram encerrados por Ctrl+C após os testes;
  todos os processos de teste/build/install/closure terminaram.
- `closure-context-ui-final.json`: ok=true, findings/documentation_findings
  vazios, oito budgets 0/0, matriz 7.538/1.170 imports e 25 dependências.
- Wheels finais SHA256 (diretório `wheels-context-ui-final`):
  Core 93dd910cb02deff15832c2939c5cc9db182781ee10c426755d55b81966e74dd2;
  Community 59d1edcb2b880ecd2fdfa05fd8d8b5b23d23961b16c5588ff8b6e865b5db966f.

Próximo trabalho: expor leitura autorizada de contexto histórico ao MCP e
verificar consumidores de contexto existentes sem substituir origens por
aprovação. Catálogo só pelo gerador oficial se registry mudar. Depois consumir
disposition receipt no Card transform e testar a sequência arquivo/grants ->
disposições -> leitura nos destinos -> Card -> trabalho, com replay/rollback e
revogações preservadas. MCP/cutover ainda não implementados nesta publicação;
preflight continua bloqueando contexto pendente. Objetivo integral permanece
ativo, incluindo F2D/F3/F4/F5 e os gates ainda pendentes registrados acima.

### 2026-09-20 — F2A: leitura MCP de contexto histórico (em implementação)

Base publicada conferida limpa: Core baf4637d / Community e4916cc. Nova tool
okto_pulse_get_historical_context reutiliza caso de uso/UoW/autoridade da rota
REST; esquema fechado, paginação limitada e nenhuma permissão Sprint ativa.
Política exata condicional spec.entity.read/card.entity.read; admissão reader.
Envelope compartilhado puro no inbound Core, sem mecanismo concreto. Erros
públicos sanitizados; somente a projeção autorizada sai do arquivo privado.

Contextos Spec/Card não-legacy recebem ponteiro not_queried: não consulta fontes
nem revela existência/contagem. Projeção reserva bytes antes do orçamento para
preservar argumentos exatos, inclusive offset=0. Legacy permanece sem o bloco.
Catálogo regenerado pelo módulo oficial. Contagens explícitas 325 tools,
322 políticas/3 exceções humanas, 47 esquemas fechados; limites de tokens e
budgets arquiteturais não foram elevados. Medição e validação ainda pendentes.

Testes preparados: schema/admissão/projeção sob orçamento, consumidores existentes,
MCP com transporte Community e leitor real, revogação entre leituras, negação
sem IO privado, corrupção/limites sem resposta parcial e argumentos inválidos.
REST terá regressão do envelope compartilhado. Nenhuma UI alterada nesta etapa.
Card transform continua bloqueado até receipt verificado ser integrado depois.

Investigação/validação inicial MCP:
- Core 51 passed (7,10 s); Community 17 passed / 3 failed exclusivamente por
  helper tentando chamar CoreMcpTool, corrigido para tool.fn. Quatro casos novos
  reexecutados: 4 passed (30,59 s); não houve mudança de produto por essa falha.
- Prova inicial: 810/328 .py e 875/412 payloads fonte/wheel/install idênticos.
- Gate footprint continua vermelho: 56.024 > 50.800 tokens (+165 vs 55.859);
  limite e schemas fechados preservados. Não alegar validação global verde.
- Closure detectou o import Community->core.inbound.historical_context como
  privado e dois bridges derivados. Corrigido sem exceção: to_payload() exposto
  em HistoricalContextPage no contrato público já consumido; REST/MCP usam esse
  método. Inbound conserva apenas ponteiro interno de contexto MCP. Nova prova
  pareada, testes dos consumidores e closure pendentes após essa correção.

A closure após uso do contrato público retornou findings=[] e oito budgets 0/0;
restava apenas atualizar as matrizes README, feito pelo renderer oficial.
Core: 87 passed (8,37 s), incluindo 36 casos do leitor de domínio. REST/MCP:
6 passed (53,27 s). Frontend consumidor do envelope: 24 passed (3,15 s), parser/
hook e painel, sem alteração da SPA. Os 14 casos host Community passaram na
primeira rodada; nenhum produto host mudou. Não duplicar testes reexecutados.
Revisão final ajustou omitted_count do contexto gate para não contar o ponteiro
retido como conteúdo omitido; acrescentado um caso específico. Rebuild pareado,
prova final e reexecução Core em andamento antes do commit.

Fechamento MCP de contexto histórico:
- Core `core-context-mcp-final.log`: **88 passed** (6,79 s), schema/admissão,
  projeções/conservação legacy, consumidores Spec/Card, 36 casos de domínio,
  registry e igualdade byte a byte do catálogo gerado.
- Community: **20 casos distintos** aprovados (14 host na primeira rodada +
  6 REST/MCP em `community-context-mcp-public-final.log`, 53,27 s). Os quatro
  MCP foram reexecutados integralmente após corrigir o helper; não duplicados.
- Frontend `frontend-context-mcp.log`: **24 passed** (3,15 s), parser/hook/painel
  consumidor do mesmo envelope REST. Total selecionado distinto **132**.
- `provenance-context-mcp-final.json`: 810/328 .py, 875/412 payloads idênticos
  fonte/wheel/install; verificador importa site-packages, pytest usa checkouts
  ativados pelas fixtures, previamente provados idênticos. Sem servidor novo
  nesta etapa; nenhum processo Pulse ativo reiniciado nem dado real migrado.
- `closure-context-mcp-final.json`: ok=true, findings=[] e
  documentation_findings=[]; oito budgets 0/0. Matrizes regeneradas oficialmente:
  7.548 imports Core, 1.170 Community->Core, 25 dependências.
- Wheels finais em `wheels-context-mcp-final`, SHA256:
  Core 7e1c21ea8e64a84ee6925b5ecc4d1e4a1f1a21904de05037b9f50c6d19354e65;
  Community 7dd7a9bf477aa4a23059e18b1bc7d41acc6f0c7b3dc0470cdf8a6650068a5543.
- Ruff e diff --check aprovados; testes/build/install/closure encerrados antes
  dos commits. Community aaae057; Core acompanha este registro. Push normal
  do par será conferido por HEAD == ls-remote, sem merge/tag/release.
- Gate global de footprint segue vermelho: 56.024 tokens > 50.800. Acréscimo
  medido de 165 tokens; não elevar limite nem enfraquecer schema/autoridade.

Próximo incremento concreto: integrar `ContextDispositionReceipt` verificado em
`materialize_archived_card_policies`, mantendo BEGIN IMMEDIATE. Não liberar
`SprintContextDispositionRequired` pela mera existência de journal. Exigir a
população exata e arquivo original antes da transformação; vincular receipt à
prova durável dos Cards (formato/compatibilidade explícitos), inclusive na
verificação usada pelo work retirement. Revalidar journal/fontes/destinos sob
triggers antes do commit. Replay deve verificar a evidência vinculada, detectar
perda de journal e preservar edições/revogações posteriores legítimas, sem
recapturar fontes transformadas nem rebasear policy. Testar sequência real em
fixture: arquivo/grants -> disposições -> leitura REST/MCP/destinos -> Card ->
trabalho, falha/rollback/replay e adulteração. Helper `_verify_events` é usado
por `sprint_work_retirement`; tratar ambos como a mesma dependência.

Preflight Card continua bloqueando contexto nesta publicação. Coordenador F2D,
remoção operacional F3, F4/F5 restantes e matriz integral de critérios/rollout/
medição seguem pendentes conforme registros anteriores. Etapa classificada como
progresso verificado; objetivo integral permanece ativo.

### 2026-09-20 — F2A/F2B: vínculo do recibo de contexto no Card transform (em implementação)

Turno anterior: progresso verificado, Core 0a84a0df / Community aaae057,
working trees limpas na retomada. Integração mantém o gate antigo sem receipt,
mesmo se existir journal. Com receipt explícito, exige mecânica resolvida,
população exata, arquivo/grants e alvos válidos antes de escrever qualquer Card.

Evidência v2 dos Cards vincula ContextDispositionReceipt tanto no manifesto SQL
quanto no blob privado; v1 permanece válido para etapas anteriores sem contexto.
Replay e work retirement revalidam o vínculo contra arquivos/journal originais,
sem recapturar fontes/destinos vivos ou restaurar grants revogados. Triggers no
journal de trabalho também não podem invalidar a prova depois da verificação.

Fence posterior compara população, fontes, referências e alvos. A única
normalização prevista corresponde aos dois campos de Card já verificados byte
a byte (sprint_id e migrated_validation_policy), incluindo a nova proveniência
source_sprint_id gerada pelo plano autorizado. Nenhuma exceção por nome de
campo/port/permissão está sendo aberta. Testes de integração/rollback/replay e
prova do par antes da validação ainda pendentes; nenhum dado real alterado.

Validação em curso: prova `provenance-card-context.json` e rechecagem antes da
segunda rodada confirmam 810/328 .py e 875/412 payloads idênticos. Closure
`closure-card-context.json` retornou ok=true, findings/documentation_findings
vazios e oito budgets 0/0; matrizes README não mudaram. Frontend consumidor:
24 passed (3,02 s), sem alterar/rebuildar SPA.

Primeira rodada nova: 5 passed / 1 failed com -x (77,37 s). As duas variantes
completas passaram (inclusive Card com contexto embutido), mais ausência de
receipt, receipt incorreto e fonte alterada. Falha exclusivamente da expectativa
do teste: Spec movida de Board falha antes em SprintRetirementRelationsInvalid,
não no verificador de contexto. Expectativa corrigida para esse gate existente,
sem alterar produto. Suite nova inteira reiniciada após término do processo e
nova prova do par; acrescidos perda do journal Card, writer concorrente e falha
de segundo blob. Regressões independentes anteriores continuam no mesmo handle.

Fechamento da integração de recibos F2A/F2B/F2C:
- Community **87a582d**: Card transform aceita receipt tipado explícito e
  verificado para a população original; sem receipt conserva o bloqueio antigo.
  Evidência privada/manifesto v2 vinculam o receipt, inclusive Boards sem Cards.
  v1 continua aceito em replay; não há upgrade silencioso nem rebase de policy.
- Fence posterior verifica fontes, população, alvos e referências, normalizando
  apenas a proveniência produzida pelos dois campos Card já verificados. Origem
  embutida anterior em Card mantém disposição explícita; caminho desconhecido
  novo continua bloqueado. Nenhuma exceção arquitetural ou de autoridade.
- Card replay e work retirement verificam o contexto pelo arquivo/journal
  original. Journal Card/contexto ausente, binding/blob adulterado e manifesto
  v2 sem vínculo falham. Replay não escreve sobre edições posteriores legítimas,
  não transforma avaliação histórica em aprovação e não restaura grants.
- `card-context-regression.log`: **62 passed** (298,76 s), Card v1, disposições,
  preflight e trabalho. `card-context-new-final.log`: **21 passed** (203,51 s).
  Inclui sequência completa em banco descartável, Card com contexto embutido,
  policies 90/60 preservadas, leitura REST/MCP pós-desvínculo, revogação antes da
  transformação e depois do replay, writer SQLite bloqueado, rollback de SQL/
  arquivos novos em falha e triggers, e adulteração antes/depois dos recibos.
- `frontend-card-context.log`: **24 passed** (3,02 s), consumidores do envelope
  histórico. Sem mudança da SPA. Total selecionado distinto: **107**; os cinco
  casos novos já verdes na primeira execução -x não são contados novamente.
- `provenance-card-context.json` / `provenance-card-context-retest.json`: 810/328
  .py e 875/412 payloads idênticos fonte/wheel/install. Core wheel não mudou em
  relação ao incremento anterior. Pytest usa checkouts provados idênticos, não
  alegar que ele importa site-packages; isso é comprovado pelo verificador.
- `closure-card-context.json`: ok=true, findings/documentation_findings=[] e
  oito budgets 0/0. Matrizes permanecem 7.548/1.170 imports e 25 dependências.
- Wheels em `wheels-card-context`, SHA256:
  Core 7e1c21ea8e64a84ee6925b5ecc4d1e4a1f1a21904de05037b9f50c6d19354e65;
  Community d46dc869aef3351016c0e0a852f354d6436655aacbb3f92a676a7c6e6121bc4a.
- Ruff e staged diff --check aprovados. Todos os processos desta etapa
  (build/install/testes/closure) terminaram antes do commit; nenhum runtime
  Pulse reiniciado e nenhuma migração aplicada a dados reais. Push normal do
  par será comparado com ls-remote, sem merge/tag/release.

Próxima continuidade: tratar a retenção destes receipts no coordenador durável
F2D, em conjunto com checkpoints/cleanup de permissões, fence de runtime e
backup/restore pareado já existentes. Nunca invocar captura original depois do
Card transform: a retomada verifica os receipts armazenados. Investigar fontes
KG/global outbox e remoção dirigida antes de declarar F2C concluída; coordenar
corte pre_create_all/post_create_all e remoção F3 para impedir recriação de
Sprint ou schema novo servido por código antigo. As etapas internas agora
compõem um fluxo de prova, mas ainda não estão no bootstrap e não substituem
esse coordenador/corte. Imutabilidade operacional, seções históricas próprias e
administração de grants permanecem pendências explicitadas anteriormente.

Gate global de metadata MCP segue pendente (última medição 56.024 > 50.800;
nenhuma tool/schema mudou nesta etapa). F3/F4/F5, matriz integral BASE/KG/DEI/
ARQ/VER/ADV, upgrade/rollback instalados e benchmark completo continuam fora da
alegação de conclusão. Progresso verificado; objetivo integral permanece ativo.

### 2026-09-20 — F2D: checkpoints transacionais do trecho de preservação de dados

Retomada: Core 0766983f / Community 87a582d, branches/working trees conferidas
limpas. Turno anterior classificado como progresso. Investigação do lifecycle
confirma pre_create_all -> create_all -> post_create_all -> bootstrap. O journal
de eventos exige Board e não serve como âncora global de retomada multi-Board.

Em implementação: journal interno cross-Board com quatro registros encadeados
(prepared/context/cards/work), âncora de inputs fechados e hashes, receipts
existentes tipados, limites e triggers de imutabilidade. Não é entidade ativa
Sprint, API, permissão ou novo diário de Delivery. O instalador externo deverá
reter RetirementDataRun com seu backup/plano; o journal não certifica sozinho
um backup ou a exclusão de todos os writers.

Checkpoints entram na MESMA transação de cada etapa, antes das verificações
finais de fonte/alvo/journal. Falha de checkpoint reverte a etapa e seus blobs
novos; perda após commit deixa receipt e efeito juntos. Replay exige checkpoint
já existente para efeito já existente; não adota uma etapa avulsa nem reconstrói
journal apagado. Retomada chama verificadores originais com receipts retidos,
sem recapturar fontes transformadas. Tabela genérica pertence apenas ao Community.

Este é o trecho durável contexto -> Cards -> trabalho a ser composto pelo
coordenador de cutover integral. Não foi registrado no bootstrap; backup,
checkpoints/cleanup de permissões, exclusão de writers/KG, corte de schema e F3
continuam necessários. Testes preparados de interrupção/reabertura, input
alterado, etapa fora de ordem, corrupção coerentemente rehashada, imutabilidade,
falha por triggers e dados antigos sem a tabela nova. Build/prova e testes
comportamentais ainda pendentes. Nenhuma migração real executada.

Atualização de validação: `provenance-data-journal.json` comprovou 810/329 .py e
875/413 payloads idênticos fonte/wheel/install antes dos testes. Suite nova:
**13 passed** (201,30 s), `data-journal-new.log`. Regressões anteriores seguem
no mesmo processo, sem reinstalação nem alteração de fontes durante a execução.
Closure inicial: findings=[], oito budgets 0/0; somente os dois READMEs divergiam
na contagem Community -> Core (1.170 -> 1.171, nova porta pública consumida).
Matrizes atualizadas pelo renderer oficial, sem relaxar gates; rebuild final do
par aprovado. Instalação/prova/closure finais aguardam término das regressões.

Continuidade investigada: `create_joint_recovery_snapshot` já adquire
`offline_migration_window` internamente. O instalador deverá manter a mesma
janela durante backup e transformações; envolver a chamada atual em outra
instância do mesmo mutex não compõe essa garantia e pode causar contenção.
Separar a captura sob janela possuída requer contrato de lifetime e teste real
de concorrência, não um booleano declarando exclusão. A reserva SQLite do
backup termina após a captura; stamps Grafx detectam commits durante export,
mas não excluem writes nativos depois. O teste existente
`test_native_grafx_write_transaction_is_not_a_migration_fence` reproduz esse
limite com o runtime real. Não tratar estes checkpoints como fence de Grafx,
certificado de backup atual ou conclusão de F2D/F3.

Fechamento do trecho durável de preservação:
- Community **3b1e490b7d4e8e380f03351d351d41598b2df55c**: journal genérico
  cross-Board, âncora de inputs e checkpoints na mesma transação dos efeitos.
  Retomada verifica os receipts de contexto/Card/trabalho existentes; não
  adota efeitos avulsos, não reconstrói journal perdido e não recaptura a
  fonte original depois do Card transform. Resultado é `data_preserved`.
- `data-journal-new.log`: **13 passed** (201,30 s). Interrupção após cada
  commit com reabertura por outro engine, schema anterior sem tabela, replay
  após edições legítimas, inputs/dependências incorretos, journal incompleto,
  receipt adulterado com cadeia rehashada, triggers de imutabilidade e rollback
  de efeitos/blobs quando o checkpoint provoca drift de fonte/alvo/prova.
- `data-journal-regression.log`: **83 passed** (503,74 s), nas suites
  `test_card_context_retirement.py`, `test_context_disposition_retirement.py`,
  `test_card_validation_retirement.py`, `test_sprint_work_retirement.py` e
  `test_sprint_retirement_preflight.py`. Total distinto deste incremento: **96**.
  Nenhuma mudança de contrato/renderização frontend; não há novos testes de
  frontend nesta etapa interna. Os 24 casos do consumidor histórico foram
  executados no incremento imediatamente anterior, não recontados aqui.
- Build final e force-install pareados concluídos somente após os testes.
  `provenance-data-journal-final.json`: 810/329 .py e 875/413 payloads idênticos
  fonte/wheel/install; origens verificadas em site-packages. As suites pytest
  usam checkouts provados idênticos pelo preflight anterior, não são alegadas
  como execução contra imports instalados. Nenhuma fonte de produto mudou
  depois desse preflight; o rebuild final inclui as matrizes README atualizadas.
- `closure-data-journal-final.json`: ok=true, findings/documentation_findings=[];
  oito budgets 0/0. Matrizes: 7.548 imports Core / 1.171 Community -> Core,
  25 dependências. Não há novo adapter, mapeamento relacional ou router no Core.
- Wheels finais em `.validation-v040/wheels-data-journal-final`, SHA256:
  Core dcbaff8dd1bffcee358fa236e407c26c3f64cf0d459329319fe3f1ff4ab01363;
  Community c6db807f60eecd2231424fa47d2292229216764406cef189ff6596f2100a26c1.
- Ruff do módulo/testes novos e staged diff --check aprovados. Todos os
  processos de build/install/testes/closure terminaram. Nenhum runtime foi
  iniciado/reiniciado, nenhum dado real migrado, nenhum bootstrap registrado.
  Push normal do par em feature/v0.4.0 será conferido contra ls-remote.

Próximo trabalho: integrar preparação/retomada com backup pareado, retenção
externa da âncora, janela contínua de exclusão, checkpoint e cleanup de
permissões; investigar/remover referências KG/global outbox de forma dirigida.
A integração de lifecycle e corte de schema depende da retirada F3 para impedir
recriação por create_all/seeds ou execução do código antigo sobre schema novo.
Não confundir o journal interno com conclusão desse coordenador nem expor
operação pública de manutenção. F2A/F2C restantes, F3/F4/F5 e matriz integral
BASE/KG/DEI/ARQ/VER/ADV continuam pendentes. Gate global MCP segue com última
medição 56.024 > 50.800, sem mudança de tools/schema nem aumento do limite.
Objetivo integral permanece ativo; classificação desta etapa: progresso.

### 2026-09-20 — F2D: mesma janela de startup para backup e transformações

Retomada conferida: Core 8c206ef8 / Community 3b1e490, árvores limpas.
Turno anterior: progresso verificado e publicado. O helper de backup soltava
os mutexes de startup ao retornar; não permitia ao instalador manter a mesma
janela durante transformações sem tentar adquirir novamente os locks.

Em implementação: `joint_recovery_window`, contexto interno Community que
adquire a janela uma vez, captura e verifica integralmente o recovery set,
então cede o artefato mantendo os mutexes até saída do corpo (inclusive falha).
O helper avulso existente delega a esse contexto e conserva a liberação ao
retornar. A captura privada não cria token reutilizável ou flag de confiança.
Reservas SQL/locks de publicação da captura são liberados antes do corpo,
permitindo transações posteriores; a exclusão de startup continua ativa.

Testes preparados: outro processo tenta adquirir ServeInstanceLock durante
publicação, verificação e corpo; mutações SQL/Grafx/arquivo seguidas de restore
do backup v4 em destino novo, com saída normal e falha; falha de export ou
artefato adulterado antes de yield impede o corpo; runtime vivo impede captura.
Ainda sem testes comportamentais antes do build/install/prova byte a byte.

Limites explícitos: não exclui writers nativos Grafx/raw SQL; não faz rollback
automático, não preserva mutex após morte do processo e não implementa admission
durável de startup com cutover incompleto. Esses requisitos permanecem no
coordenador F2D/F3. Nenhuma API/CLI/manutenção pública, mudança de autoridade,
frontend ou dado real; toda a mecânica continua no Community.

Validação em curso: `provenance-recovery-window.json` aprovado antes dos testes
(810/329 .py, 875/413 payloads; fontes/wheels/install idênticos). Os cinco casos
novos passaram em 127,66 s, `recovery-window-new.log`, incluindo restauração
real do conjunto v4 após modificar SQL/Grafx/arquivo no corpo. Closure
`closure-recovery-window.json`: ok=true, findings/documentation_findings=[],
oito budgets 0/0 e matrizes README inalteradas. Regressões existentes ainda em
execução no mesmo processo; sem alteração/reinstalação de produto em paralelo.

Próxima integração identificada no código atual: `cli.cmd_serve` e `main.run`
adquirem `acquire_serve_lock` antes dos servidores, enquanto
`CommunityRelationalSchemaLifecycle.initialize_schema` executa a região de
schema antes dos seeds. Nenhum desses caminhos consulta ainda uma admissão
durável de cutover incompleto. Ela terá de preceder inicialização/mutação e
coordenar os caminhos de instalação/retomada; uma exceção no corpo do contexto
libera o mutex e não equivale a autorização para servir o ambiente parcial.

Fechamento da composição backup/janela:
- Community **f8f624c52e534f3542a2e923056434a556412b7a**: contexto
  `joint_recovery_window` mantém a mesma exclusão de startup entre captura,
  verificação integral e corpo do instalador. `create_joint_recovery_snapshot`
  usa esse caminho e devolve um backup verificado, liberando ao retornar como
  antes. Não duplica aquisição dos mutexes nem deixa a reserva SQL da captura
  impedir as transações do corpo. Nenhuma alteração no Core de produto.
- `recovery-window-new-final.log`: **5 passed** (122,54 s). Após a primeira
  rodada verde, revisão acrescentou asserção explícita de propagação da falha
  do corpo (não apenas liberação dos locks); nenhum código de produto mudou.
  A primeira execução de cinco casos não entra de novo na contagem.
- `recovery-window-regression.log`: **42 passed** (391,04 s), suites
  `test_joint_recovery_snapshot.py` e `test_migration_runtime_fence.py`.
  Total selecionado distinto: **47**. Sem alteração de superfície frontend.
- Os casos novos usam processos reais tentando `ServeInstanceLock` durante
  publicação/verificação e antes/depois das transformações. Ainda dentro do
  corpo, novas transações SQL/Grafx e aquisição de publicação são possíveis;
  runtime cooperante permanece bloqueado. Restore v4 em destino novo recupera
  o SQL original, fingerprints dos dois escopos Grafx e bytes dos arquivos.
  Falha antes de yield impede o corpo; falha no corpo propaga e preserva o
  backup, sem fingir rollback automático. Runtime vivo impede iniciar captura.
- `provenance-recovery-window.json`: 810/329 .py e 875/413 payloads idênticos
  fonte/wheel/install antes de testes; imports do verificador em site-packages.
  Pytest usa checkouts provados idênticos; subprocessos usam o mesmo Python/par.
  Sem reinstalação ou alteração de fonte de produto durante os checks.
- `closure-recovery-window.json`: ok=true, findings/documentation_findings=[],
  oito budgets 0/0; matrizes README inalteradas (7.548/1.171 imports, 25 deps).
- Wheels em `.validation-v040/wheels-recovery-window`, SHA256:
  Core dcbaff8dd1bffcee358fa236e407c26c3f64cf0d459329319fe3f1ff4ab01363;
  Community b8de85003b3513ecc1bad2a3cdf878777dfe727bad7e38d8e489ba7c5e09b5b1.
- Ruff e staged diff --check aprovados. Todos os processos desta etapa
  terminaram antes do commit. Sem runtime real iniciado/reiniciado, alteração
  de dados reais ou registro de migração no bootstrap. Push normal do par será
  conferido com ls-remote em feature/v0.4.0, sem merge/tag/release.

Continuidade: compor este contexto com checkpoint de permissões, captura/
grants e `RetirementDataRun`, retenção externa dos receipts/backup e admissão
durável que impeça servir cutover interrompido. Retomada não deve criar outro
backup da fonte parcialmente transformada como se fosse o original. Exclusão
de writers Grafx, fontes KG/global outbox, cleanup de permissões, corte de
schema e remoção F3 permanecem no mesmo fluxo; não registrar parcialmente no
lifecycle. F2A/F2C restantes, F3/F4/F5, matriz integral e gate global MCP
(última medição 56.024 > 50.800, sem alteração nesta etapa) continuam pendentes.
Objetivo integral ativo; esta publicação é progresso, não conclusão de F2D.

### 2026-09-20 — F2D: admissão durável antes de migrações e seeds

Estado retomado e conferido limpo: Core 778ecc13 / Community f8f624c.
Turno anterior: progresso. Investigação confirmou que Community `init_db`
possui `_serialized_schema_lifecycle`, lock por arquivo SQLite; o lifespan
composto chama init_db antes de iniciar workers e demais bootstrap. O journal
de dados permanece no banco após fim/morte do processo e já identifica um
run de instalador. Nenhum dos seus quatro estados é certificado de corte
integral; `work`/`data_preserved` também não pode permitir startup.

Em implementação: gate Community somente de leitura, antes de planejamento
de migrações/create_all/seeds, tanto no init_db Community (sob lock existente)
quanto na factory concreta do orquestrador (para chamadas diretas/Core seam).
Journal ausente ou vazio com estrutura esperada admite o caminho anterior.
Qualquer run presente bloqueia com SchemaMigrationError estruturado; view ou
schema desconhecido também bloqueiam. Consulta limitada à estrutura/presença,
sem desserializar JSON, confiar em flag runtime_ready ou vazar IDs privados.
init_db Community verifica mesmo um orquestrador alternativo registrado.

Testes preparados incluem prefixes até work, self-asserted complete em JSON,
schema adulterado, recusa antes de plan/seeds, processo terminado com os._exit
após commit, fluxo real de preservação seguido de novo processo e restauração
do conjunto original v4 em outro destino. Build/install/prova precederão testes.

Limites: este gate não exclui raw writers concorrentes, não cria certificado
terminal e não integra ainda o coordenador ao lifecycle. A janela contínua do
instalador e a composição de seu lock de schema continuam necessárias contra
corridas entre o check e uma nova transformação. Não há desbloqueio por
remoção de markers nem interpretação de receipt parcial como conclusão.
Nenhuma autoridade/governança/frontend alterada; mecânica somente Community.

Primeira validação: par provado em `provenance-retirement-admission.json`
(810/330 .py, 875/414 payloads idênticos). Suite nova: 11 passed (68,94 s).
Revisão identificou journal perdido com recibos de transformação remanescentes.
Reprodução executada contra esse mesmo par, sem alterar produto:
`retirement-admission-missing-journal-repro.log`, 1 failed / 12 deselected
(12,49 s), DID NOT RAISE. O check retornava ao não encontrar a tabela, embora
`migration.context_dispositions_committed` ainda estivesse em domain_events.
Correção planejada: verificar presença dos cinco tipos internos de recibo,
sem ler payload, mesmo com journal ausente/vazio; arquivo histórico original
e eventos comuns não sinalizam início do corte. Regressões iniciais seguem no
mesmo processo; nenhuma fonte alterada durante esses checks.

Regressões iniciais: 31 passed / 1 failed (215,30 s). Falha da asserção que
exigia `_migrate_agent_permissions` como última etapa, embora
`_migrate_add_skip_delivery_evidence` já venha depois. Reproduzida antes de
atribuir ao baseline: worktrees isolados em `.validation-v040/admission-baseline`
com Core 778ecc13 / Community f8f624c, rebuild/force-install do par e
`provenance-admission-baseline.json` aprovados (810/329 .py, 875/413 payloads).
`admission-order-baseline-repro.log`: o mesmo teste falha (14,91 s) no par
anterior. Nenhum teste contra install divergente foi usado como prova.

Correção da premissa antiga: nenhuma etapa foi reordenada. Metadata
runs_at_schema_tail passa a refletir a posição real, e a dependência
runs_before_data_bootstrap fica explícita; teste continua exigindo migração de
permissões antes de TODO o bootstrap. Assert de allowlist destrutiva preservado.
Docstrings de lifecycle/migrator deixam de afirmar uma posição que não existe.
Gate de admissão agora verifica também os cinco tipos de recibo remanescentes,
sem carregar payloads. Casos de journal ausente/vazio, histórico comum e perda
do journal no fluxo real acrescentados. Novo rebuild/prova/suites pendentes.

Fechamento da admissão de runtime:
- Community **6aba3d97fc61aac3edc6cb56463f81d5f78c04fe**: gate de leitura
  ligado ao init_db Community sob o lock de schema existente e ao orquestrador
  concreto antes de plan/migrações/seeds. Recusa qualquer prefixo de preservação,
  journal com estrutura desconhecida e recibos de efeito sem journal. Não lê
  payload histórico nem concede autoridade com um flag JSON. Arquivo original
  e eventos comuns não são classificados como corte iniciado.
- `retirement-admission-new-final.log`: **22 passed** (76,58 s). A revisão final
  fortaleceu o caso de restore: prepara TODO o conteúdo fonte/arquivo/grants
  antes de capturar o backup, depois inicia o run. Após transformação, startup
  em outro processo recusa; após perda privilegiada do journal na fixture,
  os recibos remanescentes ainda recusam. O restore v4 em destino novo reproduz
  o dump SQL inteiro original e permite init_db. Caso atualizado aprovado em
  `retirement-admission-restore-final.log`: **1 passed** (42,98 s), não recontado.
- Teste com os._exit(73) após commit demonstra que morte do processo não apaga
  a recusa. Os três caminhos de lifecycle (concreto, Core seam e Community)
  bloqueiam antes do planejamento. O entrypoint Community também recusa um
  orquestrador alternativo registrado. Estrutura inválida, flag complete
  falsificado e dez variantes de recibos sem journal têm teste negativo.
- `retirement-admission-regression-final.log`: **33 passed** (196,66 s),
  `test_r01c_imp4_schema_lifecycle_orchestrator.py`,
  `test_sqlalchemy_database_lifecycle_lock.py`,
  `test_global_discovery_recovery_schema_lifecycle.py`,
  `test_retirement_data_journal.py` e
  `test_r16b_relational_schema_migrator.py::test_ts_7aacc71a_destructive_steps_are_explicitly_allowlisted`.
  Total distinto final deste incremento: **55**; execuções iniciais/reproduções
  não somadas novamente. Sem mudança de contrato/renderização frontend.
- A falha de posição da migração de permissões foi reproduzida no par anterior
  em worktrees isolados, após build/install/prova. Correção apenas da descrição,
  metadata e expectativa obsoletos: ordem de passos e allowlist destrutiva
  inalteradas, obrigação de preceder todo data-bootstrap preservada.
- `provenance-retirement-admission-final.json`: 810/330 .py e 875/414 payloads
  idênticos fonte/wheel/install; origens do verificador em site-packages. Pytest
  usa checkouts provados idênticos. Par final reinstalado após a reprodução
  isolada; nenhum teste final executado contra os wheels do baseline.
- `closure-retirement-admission-final.json`: ok=true,
  findings/documentation_findings=[], oito budgets 0/0. READMEs gerados com a
  matriz 7.548/1.172 imports e 25 dependências; único novo consumo Core é o
  erro público do port de migração. Nenhuma implementação concreta no Core.
- Wheels finais em `.validation-v040/wheels-retirement-admission-final`, SHA256:
  Core ab4a2d7aa51433b25cd18462df2db1d6e15546d6473ea2e649093c7f9ecddde9;
  Community 0dc29295e435d05ac9935270c854fe4b659406dc73a67f20e88d7f7b049324d6.
- Ruff dos arquivos alterados e staged diff --check aprovados. Todos os handles
  de build/install/testes/closure/reprodução terminaram antes do commit. Os
  processos encerrados abruptamente pertencem exclusivamente às fixtures.
  Nenhum runtime real iniciado/parado nem dado real migrado. Worktrees de
  baseline permanecem na pasta de validação para reproduzir a evidência.
  Push normal do par será verificado contra ls-remote em feature/v0.4.0.

Próximo: coordenador offline deve compor a janela de runtime com a exclusão de
lifecycle, backup original verificado, checkpoint/cleanup de permissões,
arquivo/grants e âncora externa do RetirementDataRun. O gate atual não exclui
writers raw, não cobre apagamento privilegiado de TODAS as provas e não possui
ramo terminal: nenhum estado parcial pode ser admitido como F2D completo.
Definir/verificar conclusão integral juntamente com corte de schema/F3 antes
de habilitar startup pós-cutover; nunca liberar removendo o journal. Composição
direta do orquestrador não ganha uma nova garantia de exclusão concorrente
apenas por executar este check de leitura. F2A/F2C restantes, fontes KG/global
outbox, F3/F4/F5 e matriz integral permanecem pendentes. Metadata MCP segue com
última medição 56.024 > 50.800, sem alteração de tools/schema/limite nesta etapa.
Objetivo integral ativo; classificação: progresso verificado.

### 2026-09-20 — F2D: exclusão de lifecycle composta com recuperação offline

Retomada conferida: Core f30f6999 / Community 6aba3d9, árvores limpas.
Turno anterior classificado como progresso. A admissão durável precede DDL/
seeds, mas uma chamada direta à factory concreta ainda não mantinha o lock
de schema durante todo o lifecycle. O contexto de backup também segurava
apenas os mutexes de startup, não o mutex SQLite de inicialização.

Em implementação: reentrada de `_serialized_schema_lifecycle` limitada ao
mesmo caminho canônico, PID, task e lock OS ainda possuído. ContextVar copiado
para task filha não concede posse; lease expirado é invalidado antes de
liberar o mutex. Factory concreta mantém a janela durante admissão, schema
e seeds; `init_db` Community pode compor essa chamada sem aquisição duplicada.

Novo contexto interno `joint_recovery_lifecycle_window` recebe o runtime
Community explícito, deriva dele a fonte SQL, exige captura de routing/storage,
adquire exclusão de schema antes do backup e mantém schema/startup durante o
corpo offline. Não é coordenador completo nem token reutilizável de autoridade.
Writers raw SQL/Grafx, retenção externa de receipts, retomada e certificado
terminal continuam necessários. Nenhuma API/CLI/frontend/Core de produto muda.

Testes preparados de reentrada e bloqueio por outro processo, tasks filhas com
contexto vivo/expirado, banco diferente, cancelamento, três entrypoints reais
de lifecycle e recuperação v4 com erro no corpo. Nenhum teste comportamental
antes de rebuild/install/prova do par. Nenhum dado real alterado.

Validação em curso: `provenance-lifecycle-window.json` aprovado (810/330 .py,
875/414 payloads idênticos fonte/wheel/install). `closure-lifecycle-window.json`
ok=true, findings/documentation_findings=[], oito budgets 0/0; matrizes não
mudaram. Primeira suite nova: 8 passed / 1 failed (48,96 s). Os oito casos de
posse/reentrada/cancelamento/entrypoints passaram; falha na preparação do loop
do teste integrado, antes do corpo: fixture síncrona existente usa asyncio.run
e fecha o loop que pytest-asyncio tentava reutilizar no Python 3.13. Corrigido
somente o teste para dirigir sua coroutine com asyncio.run após a fixture.
Produto sem alterações desde o preflight; nova suite completa e regressões
existentes em execução, sem reinstalar enquanto os processos estão vivos.

Fechamento da janela compartilhada de lifecycle:
- Community **b4ff0790675f50900fe38e1412f6237acbb250ed**: mutex de schema
  reentrante somente para a task/processo que mantém o lock OS no mesmo caminho.
  Posse não é herdada por ContextVar nem preservada depois da saída. O
  orquestrador concreto agora mantém esse mutex durante admissão, região de
  schema e bootstrap, inclusive quando chamado diretamente pelo Core seam;
  o entrypoint Community continua usando o mesmo lock sem aquisição duplicada.
- `joint_recovery_lifecycle_window` deriva o SQL do runtime Community explícito
  e mantém a exclusão de lifecycle antes/durante backup verificado v4, além
  da exclusão de startup durante o corpo. Exige routing/storage; runtime em
  memória não pode ser apresentado como fonte de backup de arquivo durável.
- `lifecycle-window-new-final.log`: **11 passed** (81,98 s), nas suites
  `test_schema_lifecycle_reentry.py` e `test_joint_recovery_lifecycle_window.py`.
  Cobrem reentrada sem liberação antecipada, task filha com contexto vivo e
  expirado, banco diferente, cancelamento da task proprietária, mutex mantido
  por todos os três entrypoints até os seeds e janela integrada v4 com saída
  normal/erro. Outro processo tenta adquirir os locks reais nos pontos críticos.
- `lifecycle-window-regression.log`: **42 passed** (335,63 s), suites
  `test_sqlalchemy_database_lifecycle_lock.py`,
  `test_r01c_imp4_schema_lifecycle_orchestrator.py`,
  `test_retirement_runtime_admission.py` e `test_joint_recovery_window.py`.
  Total selecionado distinto: **53**; os oito casos iniciais verdes não são
  recontados. Sem mudança de UI/REST/MCP ou necessidade de rebuild da SPA.
- `provenance-lifecycle-window.json`: 810/330 .py e 875/414 payloads idênticos
  fonte/wheel/install antes dos testes, origens do verificador em site-packages.
  Pytest usa checkouts provados idênticos. Correção posterior foi somente do
  loop de teste, sem alteração de produto ou reinstall com checks ativos.
- `closure-lifecycle-window.json`: ok=true, findings/documentation_findings=[],
  oito budgets 0/0. Matrizes READMEs permanecem 7.548/1.172 imports e 25 deps.
  Core de produto inalterado; nenhum mecanismo concreto deslocado para ele.
- Wheels em `.validation-v040/wheels-lifecycle-window`, SHA256:
  Core ab4a2d7aa51433b25cd18462df2db1d6e15546d6473ea2e649093c7f9ecddde9;
  Community 082d72b7168449734132efb46e87554fd1987cf5bf1c9bb6b38f93244fe854cc.
- Ruff e staged diff --check aprovados. Todos os handles de build/install/
  testes/closure terminaram antes do commit. Somente fixtures descartáveis;
  nenhum runtime real ou dado de usuário alterado. Push normal do par em
  feature/v0.4.0 será conferido com ls-remote e working trees limpas.

Continuidade: o coordenador pode agora manter a mesma exclusão de schema e
startup desde a captura original até as etapas offline. Próximo trabalho é
compor essa janela com checkpoint/cleanup de permissões, arquivo/grants, plano
de disposições e journal de dados, retendo backup/inputs/receipts externamente
para retomada sem recapturar estado transformado. Não chamar a captura de um
novo backup como substituto do original em replay. Os contexts não são um
certificado terminal nem uma exclusão de writers raw SQL/Grafx. Remoção de
fontes KG/global outbox, corte de schema e F3 devem validar a conclusão integral
antes de admitir startup. F2A/F2C restantes, F3/F4/F5, matriz integral e gate
global MCP (última medição 56.024 > 50.800, inalterado) continuam pendentes.
Objetivo integral permanece ativo; classificação desta etapa: progresso.

### 2026-09-20 — F2D: preparação selada e retomada dos dados offline

Retomada conferida: Core 12624598 / Community b4ff079, feature/v0.4.0.
Em implementação: composição interna Community com backup original v4, plano
explícito de contexto, captura de permissões, arquivos/grants e journal de
contexto/Card/work. Registro externo privado publicado sem overwrite; seu
SHA retido pelo operador é verificado antes de seguir caminhos e recibos.
Preparação não transforma Cards; retomada não recaptura backup nem autoridade.
A mesma exclusão de schema/startup cobre preparação e cada retomada.

A saída continua data_preserved, insuficiente para liberar runtime. Cleanup
não pode remover flags Sprint ainda registradas: sua composição depende da
retirada real no F3 e de retenção atômica de seu recibo de conclusão. Nenhuma
exceção ao registry ou budget foi introduzida. Falha antes de publicar o handle
exige recuperar o backup original; SQL parcial não fabrica handle perdido.
Testes em preparação: interrupção/reabertura, replay sem recaptura, adulteração
de arquivo/backup/checkpoints, vínculo ao runtime/build e falha de publicação.
Somente fixtures descartáveis; nenhum dado real ou frontend alterado.

Primeiro preflight aprovado: provenance-offline-run.json, 810/331 .py e
875/415 payloads idênticos fonte/wheel/install. Primeiro teste integrado falhou
antes de retornar a preparação: leitura Python do plano estrito recusava listas
vindas do JSON onde o contrato usa tuplas. Correção no adapter para chamar a
validação JSON do mesmo contrato, sem relaxar strict/extra/limites no Core.
Closure inicial: findings=[], oito budgets 0/0; apenas matrizes README ficaram
desatualizadas pelo novo import público. Regeneradas pelo renderer oficial.
Processos iniciais encerrados antes dessas edições. Rebuild pareado em curso.

Fechamento da preparação/retomada offline:
- Community **d6188d78e0a92af21e9338d1f55eaf565d14773d** adiciona
  `adapters/retirement_offline_run.py`. Preparação captura/valida o backup v4
  original sob exclusão de schema/startup, instala arquivos/grants, valida o
  plano contra população e destinos atuais, prepara o journal SQL e publica
  um registro privado externo sem overwrite. Inputs limitados a 64 MiB.
- O handle externo retém SHA256 do registro, vinculando caminhos canônicos,
  par de builds original/de migração, backup, plano e recibos de permissões e
  dados. Retomada exige mesmo SQL/storage/build, verifica o backup original e
  os recibos antes de executar contexto/Card/work e revalida os recibos ao fim.
  Não recaptura autoridade/backup depois das transformações. Ausência de prova
  não é reparada por inferência. A captura continua restrita a exclusão dos
  entrypoints cooperantes; não certifica exclusão de writers raw SQL/Grafx.
- `offline-run-new-final.log`: **14 passed** (223,95 s). Cobertura integrada de
  locks OS mantidos em preparação/retomada (outro processo tenta entrar),
  preparação sem detach de Card, backup original preservado, create-only,
  interrupção após cada etapa confirmada e reabertura por outro AsyncEngine,
  replay sem recaptura preservando edição posterior da Spec, adulteração de
  registro/backup, perda de checkpoint de permissão/journal, SQL/storage/build
  incompatível, plano incompleto, falha de publicação com restore original e
  recusa de backup substituto, além de trigger que apaga a prova de autoridade
  na última etapa: retorno falha e startup continua bloqueado.
- `offline-run-regression.log`: **53 passed** (323,74 s), suites
  `test_retirement_data_journal.py`, `test_permission_retirement_checkpoint.py`,
  `test_retirement_runtime_admission.py` e
  `test_joint_recovery_lifecycle_window.py`. Total selecionado distinto: **67**.
  A primeira tentativa vermelha de leitura estrita do plano foi corrigida e
  toda a suite nova repetida; não foi omitida nem contada como aprovada.
- `provenance-offline-run-final.json`: 810/331 .py e 875/415 payloads idênticos
  fonte/wheel/install antes dos testes. Origens do verificador em site-packages;
  pytest utiliza checkouts provados idênticos. Nenhuma edição de produto ou
  reinstall durante as verificações ativas. Todos os handles terminaram.
- `closure-offline-run-final.json`: ok=true, findings/documentation_findings=[],
  oito budgets 0/0. READMEs gerados com 7.548/1.173 imports e 25 dependências.
  Único novo vínculo com Core: contrato público ContextDispositionPlan. Nenhum
  mecanismo concreto, exceção temporária ou reach-in privado no Core.
- Wheels em `.validation-v040/wheels-offline-run-final`, SHA256:
  Core a86c4e4cfbebb3287348bb91f29400c240a6aa4063c3c5708a9b896cf0beaf45;
  Community eee4e15e7e161fe5aa009f008ac8a62b9e0a967fd6434f9b1e68bcd5df579db2.
- Ruff e staged diff --check aprovados. Sem mudanças em frontend/REST/MCP;
  testes de frontend não necessários nesta etapa. Nenhum runtime real foi
  iniciado/parado, nenhum dado real migrado. Push normal do par em
  feature/v0.4.0 será conferido contra ls-remote e working trees limpas.

Continuidade: data_preserved NÃO é conclusão de F2D nem autorização de startup.
Preparação interrompida antes de publicar handle mantém backup para rollback;
retomada não reconstrói handle a partir do SQL candidato. Cleanup de permissões
precisa de registro atômico de conclusão e da retirada efetiva das flags Sprint
no F3; não abrir exceção ao guard do registry para antecipá-lo. Compor também
remoção dirigida das fontes KG/global outbox e corte real de schema/F3 antes
de criar certificado terminal e integrar admissão de upgrade/instalação limpa.
F2A/F2C restantes, F3/F4/F5, matriz integral e gate global de metadata MCP
(última medição 56.024 > 50.800, inalterado) continuam pendentes. Objetivo
integral ativo; classificação desta etapa: progresso verificado.

### 2026-09-20 — F2C/F3: remoção dirigida de projeções de Sprint no grafo

Retomada conferida: Core 38df839a / Community d6188d78, árvores limpas.
Turno anterior: progresso verificado (67 testes e pushes confirmados).
Investigação atual: deterministic_kg.process_sprint emite Entity raiz e
Criterion de expected_outcome com source_artifact_ref exato sprint:<id>.
Card→Sprint e Sprint→Spec são relações incidentes; a sessão que atesta uma
raiz não transfere sua propriedade. Não usar delete-by-session como migração.

Em implementação: plano puro de remoção por identidade lógica e origem
arquivada, com fingerprint completo antes/depois, preservação de nós vizinhos
e relações sobreviventes (inclusive paralelas). Referência parcial/descendente,
origem não arquivada ou produtor/tipo sem classificação comprovada falha fechado.
Adapter Grafx interno aplicará apenas o plano retido, verificando antes/depois
na mesma transação; falha anterior ao commit deve desfazer a remoção.
Não é rebuild nem exclusão universal de writers. Ainda não ligar ao coordenador
sem recibo durável, backup original e tratamento conjunto de global/outbox/F3.

Primeira execução: proveniência 811/332 .py e 876/416 payloads idênticos.
Planejamento/fingerprint do grafo real passou até a aplicação, mas o primeiro
caso falhou porque scan_rows_v1 só aceita transação READ no Grafx instalado.
Nenhuma remoção chegou a ocorrer. Adapter corrigido para montar a visão lógica
limitada através de Transaction.execute na mesma transação WRITE, usando o
schema físico fechado e o codec de valores Community existente. Verificação
antes/depois não foi transferida para snapshots independentes. Closure inicial
teve apenas drift das matrizes README; findings=[] e oito budgets 0/0.
Todos os processos anteriores terminaram antes de alterar fontes/reinstalar.

A segunda execução passou: 10 testes reais de planejamento/removal/rollback/
replay e reabertura do arquivo Grafx (104,86 s). Mantidos o UUID físico do banco,
os nós Card/Spec e a multiplicidade das relações sobreviventes. Falhas injetadas
após DELETE e alteração indevida de sobrevivente desfazem toda a transação.
A visão WRITE usa consultas com limite agregado de registros, payload lógico
limitado e validação exata do catálogo/schema; nenhum scan read-only é chamado
na transação de escrita. Closure r2: findings=[], oito budgets 0/0; apenas
matrizes de documentação requerem renderer. Reforço final dos testes cobre
payload sobrevivente com vetores/timestamps/scores e budgets de registros/bytes.

Fechamento da remoção dirigida de projeções Board:
- Core `ports/retirement_graph.py`: plano puro GraphRetirementPlan sobre a
  representação lógica existente. Seleciona exclusivamente Entity/Criterion
  com origem exata sprint:<id> pertencente ao conjunto arquivado informado e
  produtor determinístico segundo o classificador Core existente. Não usa
  sessão, título, conteúdo textual ou prefixo curto de ID como propriedade.
  Origem desconhecida/derivada e produtor não comprovado exigem investigação.
- Relações incidentes admitidas são belongs_to determinísticas do produtor
  atual (sprint_outcome, sprint_to_spec, card_to_sprint, sprint_to_board).
  Outra semântica incidente falha fechado, inclusive contribuição cognitiva
  que não possa ser apagada como hierarquia exclusiva. Nenhum nó vizinho é
  removido por transitividade. Não representa todos os resíduos possíveis de
  Sprint no KG nem uma autorização para classificar casos desconhecidos.
- Plano guarda identidades tipadas, fingerprints integrais antes/depois e
  multiplicidade de relações removidas. Medição valida schema, chaves únicas,
  endpoints e census; limites de 100.000 registros / 64 MiB de payload lógico.
  A identidade inclui tipo+chave: Entity e Criterion com mesmo ID não colidem.
- Community **e7c06d98c31463b9b6343f446d7ec31c4c0fe96d**:
  `grafx_sprint_retirement.py` prepara em snapshot fixo e aplica o plano retido
  na mesma transação WRITE que verifica o estado anterior e o resultado.
  Transação não utiliza scan_rows_v1, que o driver restringe a READ: queries
  nativas usam somente nomes do schema fechado e valores parametrizados; codec
  Community existente preserva tipos físicos, vetores e timestamps. Todo
  Cypher/Okto Grafx/vida da transação permanece em Community/adapters.
- Replay do estado esperado não emite escrita nem muda published_lsn. Estado
  divergente ou seleção/contagem incoerente é recusado. Erro depois de DELETE
  ou alteração indevida de sobrevivente desfaz o conjunto inteiro antes do
  commit. Não há rebuild/troca de geração; UUID físico permanece idêntico.
- `graph-retirement-new-final.log`: **12 passed** (134,49 s). Grafo físico real
  cobre seleção por tipo+ID, preservação integral de Card/Spec com valores
  ricos (incluindo vetores/timestamps/scores), relações paralelas, replay sem
  publicação, reabertura, rollback após DELETE, drift anterior, plano errado,
  origem não arquivada, ref descendente, writer cognitivo, regra incidente
  desconhecida, menção textual/sessão compartilhada e limites de leitura WRITE.
- `graph-retirement-regression.log`: **75 passed** (63,28 s), suites
  `test_logical_transfer_grafx.py`, `test_logical_transfer_factories.py` e
  `test_grafx_projection_active_set.py`. Total selecionado distinto: **87**.
  Dez casos intermediários foram reforçados/repetidos, sem contagem dupla.
- `provenance-graph-retirement-final.json`: 811/332 .py e 876/416 payloads
  idênticos entre fontes, wheels e instalação antes dos testes. Verificador
  resolve site-packages; pytest usa checkouts provados idênticos. Sem mudanças
  de produto/reinstalação com checks ativos. Todos os handles encerrados.
- `closure-graph-retirement-final.json`: ok=true, findings/documentation_findings=[],
  oito budgets 0/0. Matrizes oficiais READMEs: 7.552/1.175 imports, 25 deps.
  Sem mecanismo concreto novo no Core, reach-in privado ou exceção transitória.
- Wheels finais `.validation-v040/wheels-graph-retirement-final`, SHA256:
  Core baf97d8e1568a32629b101b259b5d1ca854025fade0e061456a02a89d4c3cad4;
  Community dda4d46e3774035f554cd98cb21c340eb2ad6fb268ac9eeb86ac4f0061884d1a.
- Ruff e staged diff --check aprovados. Sem efeito em frontend/REST/MCP; testes
  de frontend não necessários nesta etapa. Apenas bancos descartáveis; nenhum
  grafo real/dado de usuário alterado, nenhum runtime real iniciado/parado.
  Push normal pareado em feature/v0.4.0 será conferido com ls-remote.

Continuidade: o adapter é primitiva interna, ainda NÃO ligado à preparação/
retomada offline. O chamador deverá provar Board/routing, vincular o plano ao
backup original e retê-lo externamente; construir recibo durável/replay conjunto
com Global Discovery e outbox antes de integrar ao coordenador. O commit nativo
com conflito otimista é propagado; possuir WRITE não é exclusão universal de
writers raw e este helper não é certificado terminal de cutover. Investigar
resíduos fora da projeção determinística classificada, sem apagar nós cognitivos
ou reinterpretar fontes compartilhadas por conveniência. Continuam necessários
cleanup real das permissões no F3, corte de schema/instalação limpa, F2A/F2C
restantes, F3/F4/F5 e matriz integral. Metadata MCP permanece na última medição
56.024 > 50.800, sem mudança nesta etapa. Objetivo integral ativo; progresso
verificado, não conclusão da iniciativa.

### 2026-09-20 — F2C/F3: Global Discovery ligado à origem removida

Retomada: Core f94c75c5 / Community e7c06d98, árvores limpas. Turno anterior
classificado como progresso (87 testes, ambos os pushes conferidos).
Investigação: GlobalOutboxProcessor deriva identidade de (Board, node id),
ignora node_type do digest como autoridade e recusa ID ambíguo entre tabelas.
Board.decision_count é o total absoluto de fontes vetorizadas, não revogadas/
supersedidas, do grafo Board; não equivale ao número de digests persistidos.
O outbox consulta o grafo de origem antes de publicar; payload/session ref
isolado não recria um nó ausente. Ainda é necessário classificar/superseder
trabalho histórico exclusivo sem simular entrega nem descartar sessões mistas.

Em implementação: fatos derivados do snapshot original e plano Board retido,
plano global por identidade exata, preservação de outros Boards/Entity/Topic e
relações sobreviventes, ajuste somente do contador dos Boards afetados contra
a população autoritativa comprovada. Aplicação global exige fingerprints de
Board pós-remoção, antes de mudar digests. Recibo durável e composição com
outbox/coordenador permanecem etapas conjuntas necessárias ao cutover.

Validação inicial: provenance-global-retirement.json aprovado (812/333 .py,
877/417 payloads idênticos fonte/wheel/install). Primeira tentativa falhou
na fixture, antes do produto: vetor Entity fornecido a Criterion foi recusado
pelo codec/schema existentes. Fixture corrigida para espaço vetorial de cada
tipo, sem mudar produto nem relaxar validação. Suite r2: 14 passed (154,49 s).
Closure inicial: findings=[], budgets 0/0; somente matrizes README a regenerar.
Refinamento final limita a composição a 256 Boards e 100.000 chaves removidas
no agregado; novo teste cobre dois Boards afetados e recusa Global antes de
ambos terminarem. Nenhuma edição/reinstalação com testes anteriores ativos.

Fechamento da primitiva de remoção global:
- Core `ports/global_retirement_graph.py`: fatos de origem derivados do snapshot
  Board original, verificado contra o plano retido. Seleção de DecisionDigest
  por (board_id, original_node_id), sem confiar no tipo cacheado. Identidade
  ambígua na origem falha fechado. Contadores usam a população publicável
  autoritativa (embedding presente, sem revogação/supersessão), nunca subtração
  de linhas do cache. Limites de 256 Boards e 100.000 chaves agregadas.
- Community `grafx_global_retirement.py`: exige estado pós-remoção de todos os
  Boards afetados; verifica plano, remove digests/relações incidentes e atualiza
  apenas decision_count na mesma transação WRITE do grafo global. Verifica
  fingerprint integral depois e revalida os Boards antes do commit. Erros
  desfazem a transação global; não há alegação de transação entre os bancos.
  Replay verificado não escreve nem publica LSN. UUID físico permanece igual.
- `global-retirement-new-final.log`: **15 passed** (221,85 s). Cobre múltiplos
  digests da mesma origem, tipo cacheado divergente, outro Board com mesmo ID,
  preservação integral de Entity/Topic e relações paralelas, contagem sem
  digests correspondentes, revogação/supersessão/embedding nulo na origem,
  rollback após remoção, drift global/Board, plano adulterado, Board ausente,
  identidade ambígua, replay/reabertura e dois Boards afetados em conjunto.
- `global-retirement-regression.log`: **25 passed** (209,95 s), suites
  `test_grafx_sprint_retirement.py`, `test_grafx_global_discovery_providers.py`,
  `test_grafx_global_digest_link_batching.py` e
  `test_grafx_global_visibility_batching.py`. Total distinto: **40**. Os 14
  casos intermediários não são somados novamente.
- `provenance-global-retirement-final.json`: **812/333 .py**, **877/417 payloads**
  idênticos entre fontes, wheels e instalação antes dos testes. Verificador
  resolve site-packages; pytest utiliza checkouts provados idênticos. Nenhuma
  alteração/reinstalação de produto durante os checks; handles encerrados.
- `closure-global-retirement-final.json`: ok=true, findings=[] e
  documentation_findings=[]; oito budgets 0/0. Renderer oficial atualizou
  READMEs para 7.557/1.177 imports e 25 dependências. Ruff e diff --check passam.
- Wheels em `.validation-v040/wheels-global-retirement-final`, SHA256:
  Core 541ae6414f9016f3d40b1dc1b1790adbfe782cb4e24622081e4914ac26436721;
  Community b41ee3259dd6c4a0c899568251b45005372b40cb3eab5ea5869c44ab04029d41.
- Sem impacto em frontend/REST/MCP; apenas bancos descartáveis. Nenhum runtime
  real ou dado de usuário alterado. Push pareado normal em feature/v0.4.0.

Continuidade: estas primitivas Board/global ainda precisam de planos externos
selados e recibos duráveis integrados ao coordenador, backup original, routing
e exclusão de writers. A verificação final dos Boards não elimina uma corrida
com writer raw após a leitura. A investigação seguinte é a aposentadoria do
outbox exclusivo de Sprint, preservando sessões mistas, estado histórico e
semântica de entrega. O outbox atual usa processed_at/retry_count; não existe
estado explícito de aposentadoria. Não marcar como entregue nem reutilizar
sentinela sem verificar todos os consumidores/redrives. Permissões F3, schema,
certificado terminal/admissão, F2A/F2C restantes, F3/F4/F5 e matriz integral
continuam pendentes. Metadata MCP permanece na última medição 56.024 > 50.800.
Objetivo integral ativo; esta etapa é progresso verificado.

### 2026-09-20 — F2C: outbox global exclusivo, sem ACK fabricado

Par anterior publicado e conferido limpo: Core
336f608b8003e91fc236eeaadf8ac67a9bc89af5 / Community
ee3363882e832df962a591dd409f746c08498a7e (feature/v0.4.0).
Investigação do produtor `kg.primitives`: o payload de consolidação tem seis
campos; as referências persistidas descrevem nós adicionados, mas os contadores
de update/supersessão não têm correspondência suficiente nessas referências.
Por isso, sessões com esses efeitos exigem investigação. Outbox também executa
reconciliação de Board; não basta inferir exclusividade pelo nome da sessão.

Em implementação: classificação Core exige auditoria Sprint arquivada, mesma
sessão/Board, contrato atual exato, census de referências e inclusão de todas
as identidades tipadas no plano de remoção. Sessões mistas e histórico concluído
permanecem intactos. Contratos desconhecidos falham fechado. Adapter Community
retém snapshot privado integral de consolidation_audit/global_update_outbox/
kuzu_node_refs (o nome SQL legado não significa runtime Kuzu), limitado a
100.000 linhas/64 MiB, incluindo outros Boards para detectar referências cruzadas.

Estado reservado de migração: retry_count=-2, sem alterar processed_at,
last_error ou payload. Claim/health/DLQ existentes já excluem esse valor; reforços
explícitos impedem save de worker antigo, requeue e falsa classificação como
queued/applied. A aplicação exige fingerprints pós-remoção de Board e Global,
usa BEGIN IMMEDIATE e verifica snapshot integral após UPDATE antes do commit.
Snapshot original mantém contadores/diagnósticos anteriores como evidência
privada; não é uma nova seção de histórico público nem uma concessão de acesso.
Plano/backup ainda deverão ser selados pelo coordenador e receber checkpoint
durável; nenhum cutover/admissão completa é alegado por este helper.

Validação de outbox:
- Primeira suíte: `outbox-retirement-new.log`, **14 passed**, 22,19 s.
  Reforços posteriores acrescentam drift anterior no snapshot, proteção de
  redrive direto, origem de sessão não Sprint e claim/marker antigos.
- `outbox-retirement-core-final.log`: **43 passed**, 5,58 s, suites
  `test_global_outbox_retired_work.py`, `test_global_outbox_dead_letter_operations.py`,
  `test_global_outbox_blocking_execution.py`, `test_global_outbox_visibility_batches.py`
  e `test_global_outbox_edge_inventory_cost.py`.
- `outbox-retirement-community-final.log`: **31 passed**, 37,96 s, suites
  `test_global_outbox_retirement.py`, `test_global_outbox_dead_letter_operations.py`,
  `test_queue_health_global_outbox_adapter.py`, `test_materialization_health_adapters.py`
  e `test_terminal_debt_readers.py`.
- Revisão após esses checks encontrou a necessidade de proibir também a
  atribuição direta de -2 em requeue_terminal_events. A guarda foi incluída;
  novo teste tenta aposentar uma linha DLQ viva por esse método e exige erro
  com todos os valores originais preservados. Rebuild/reinstall dos DOIS wheels
  após encerramento dos checks; `outbox-retirement-community-r2.log`:
  **20 passed**, 25,08 s (16 casos de retirement + 4 de redrive/CAS). Total
  distinto selecionado: **75** = 43 Core + 12 demais regressões Community + 20.
  Nenhuma contagem dupla das execuções intermediárias. Payload Core r2 tem
  hash agregado idêntico ao Core testado nos 43 casos.
- Prova antes das suítes: `provenance-outbox-retirement-final.json` e
  `provenance-outbox-retirement-r2.json`: **813/334 .py**, **878/418 payloads**
  idênticos em fontes/wheels/site-packages. Verificador importa instalação;
  pytest usa checkouts provados idênticos. A primeira tentativa de comparação
  inicial foi antecipada enquanto pip ainda instalava e falhou por arquivos
  temporariamente ausentes; aguardado o MESMO processo terminar, a comparação
  completa passou antes de qualquer teste comportamental. Nenhum teste rodou
  durante install nem houve edição/reinstall de produto com testes ativos.
- SQL real e Grafx real cobrem ordem Board→Global→SQL, mudança anterior recusada,
  trigger que corrompe referência após primeiro UPDATE com rollback integral,
  replay após fechar conexões, tentativa de recapturar estado já transformado,
  falsificação da seleção, mesma sessão com referências fora da remoção,
  referências de outro Board, auditoria/referências ausentes ou duplicadas,
  contadores de update não comprovados, origem não arquivada, contrato/payload
  desconhecido, limites de linhas/bytes, exclusão de claim/DLQ/health, seleção
  stale do worker e rollback de todo save batch quando uma linha foi aposentada.
- Preservar referências fora do plano não certifica que todos esses nós ainda
  existem; simplesmente não há prova para superseder esse trabalho. A
  classificação completa de resíduos continua necessária ao cutover. Um
  worker que já tinha uma cópia anterior à aposentadoria pode tentar efeitos
  de grafo antes de chegar ao save SQL; a exclusão offline de writers continua
  obrigatória. As guardas SQL não são uma promessa de atomicidade entre stores.
- Novo estado usa o valor reservado -2 no mecanismo legado de retry_count.
  O número anterior fica no snapshot original retido; erro, payload, tempos,
  auditoria e referências não mudam. O verificador interno usa o estado
  `superseded` já existente e não inventa um evento substituto/ACK. Nenhuma
  porta de delivery normal pode atribuir -2. Permissões e schemas públicos,
  frontend, REST e catálogo MCP não foram alterados; sem teste de frontend
  necessário para esta etapa. Nenhum banco ou processo real de usuário tocado.
- Wheels finais `.validation-v040/wheels-outbox-retirement-r2`, SHA256:
  Core d66c761ad5ca8377fbd07d591ab1415129673ca1749cd9a67c3b5b1601f802e5;
  Community aa4d7c7802c3a9dc0e3332c3eb1eb9b661677f0572abe76835427447105ebc3c.
- `closure-outbox-retirement-r2.json`: **ok=true**, findings=[] e
  documentation_findings=[], oito budgets **0/0**. Matrizes oficiais dos
  READMEs: **7.560/1.181 imports, 25 dependências**. Nenhum reach-in privado,
  mecanismo concreto novo no Core ou exceção transitória. Ruff e diff --check
  aprovados; todos os processos de validação encerrados antes do commit.
- Commit Community desta etapa: **f0f5458ada48fc2dc01bbdc7c9c887aff9410c48**.
  Publicação normal do par em feature/v0.4.0, com comparação de HEAD/ls-remote
  e confirmação de árvores limpas após os pushes.

Próxima integração: ampliar o manifesto privado offline para reter os planos
originais de Board/global/outbox, vinculá-los ao backup original e aos Boards
arquivados, registrar intenção/checkpoints duráveis e retomar sem recaptura
mesmo após commits de grafo anteriores ao ACK SQL. O manifesto v1 atual cobre
apenas dados preservados; estes helpers ainda não foram ligados a ele. Cleanup
de permissões, schema F3, certificado terminal de admissão e demais fases da
matriz seguem pendentes. Metadata MCP mantém o bloqueio já conhecido
56.024 > 50.800. Objetivo integral ativo; progresso, não conclusão.

### 2026-09-20 — F2D: composição offline de materialização

Retomada conferida: Core af2b6b7831e6b06b212c0a99c5fa97df318d5665 /
Community f0f5458ada48fc2dc01bbdc7c9c887aff9410c48, árvores limpas.
Turno anterior: progresso verificado (remoção global e outbox, pushes normais).
O fluxo v1 existente só preservava dados relacionais. A integração agora sela
planos Board/global/outbox no manifesto privado v2, antes das transformações,
comparando os fingerprints de origem e o snapshot SQL com o backup original.

Preparação/retomada mantém os fences de schema, startup e publicação de bindings.
Rotas, geração, caminho físico, page size e UUID devem corresponder ao backup;
nenhuma inicialização ou troca de geração é inferida. Global ausente permanece
ausente, inclusive quando há remoção em Board e outbox. Board ausente com
trabalho/digests que podem depender de Sprint exige investigação, pois não há
prova para distinguir efeitos mistos. Ausência total sem trabalho é no-op.

O journal existente recebe dois estágios depois de work: graph_intent e graphs.
O primeiro ancora o plano externo e o backup antes da primeira remoção; o último
confirma grafos verificados e outbox na MESMA transação SQL. Crash depois do
commit de Board ou Global e antes do ACK retoma pelo plano original; um intent
perdido com grafo já transformado não é reconstruído a partir do candidato.
O schema de quatro colunas do journal não muda. Nenhum checkpoint concede
admissão do runtime: cleanup de permissões e corte de schema seguem necessários.

Validação inicial da composição:
- `provenance-materialization-coordinator.json`: 813/336 .py e 878/420 payloads
  idênticos antes dos testes. Closure inicial sem finding, budgets 0/0; apenas
  matrizes README a regenerar ao encerrar as alterações.
- Primeira execução recusou corretamente fixture com digest do Board B e Sprint
  arquivada, mas sem grafo de origem B (1 failed, 28,24 s). O caso foi mantido
  como teste negativo; o caminho positivo passa a fornecer os DOIS Boards.
- Segunda execução chegou à escrita de graph_intent e revelou a constraint
  física real `ordinal <= 3` (1 failed, 41,56 s), não observada na integração
  inicial. Não se tratou como sucesso nem se removeu a proteção do journal.
  A correção expande o contrato fechado para 0..5, com cópia transacional de
  células SQL cruas, igualdade byte a byte do conteúdo JSON, restauração dos
  triggers de imutabilidade e recusa de dependentes não classificados. Sem
  PRAGMA foreign_keys=OFF. Falha entre DROP/RENAME deve restaurar integralmente
  a tabela anterior pelo rollback. O número de colunas permanece quatro.
- Novos testes físicos cobrem expansão de journal antigo, replay sem alterações,
  limites 5/6, imutabilidade, rollback após DROP e trigger desconhecido. A prova
  de data-only continua exigindo os quatro estágios originais; estágios de grafo
  pertencem ao coordenador e não são simulados pelo helper relacional.

Resultado da integração após correção da constraint:
- `materialization-coordinator-new-r3.log`: **12 passed**, 453,86 s. Três
  testes do upgrade físico do journal e nove da composição offline: dois
  Boards reais, com/sem Global Discovery, preservação do Board não afetado,
  fences de schema/startup/binding provados por processos separados,
  crash após commits de Board, Global e SQL, reabertura de SQL/Grafx sem
  recaptura, replay sem nova publicação LSN, corrupção no ACK com rollback,
  perda de intent recusada, ausência total preservada e cache sem origem recusado.
- `materialization-coordinator-regression.log`: **65 passed**, 417,28 s,
  suites `test_retirement_offline_run.py`, `test_retirement_data_journal.py`,
  `test_global_outbox_retirement.py`, `test_retirement_runtime_admission.py`.
- `materialization-coordinator-guards.log`: **3 passed**, 128,33 s. Handle de
  Board trocado e grafo alterado após selagem são recusados antes de transformar
  Cards; uma conclusão fabricada no SQL, apesar da cadeia de hashes coerente,
  não certifica grafos ainda originais. A admissão do runtime continua bloqueada.
- `provenance-materialization-coordinator-r3.json` e
  `provenance-materialization-coordinator-final.json`: **813/337 .py**,
  **878/421 payloads** idênticos entre fontes, wheels e site-packages.
  Após os 80 testes, só os READMEs foram regenerados pelo renderer oficial;
  hashes agregados dos payloads Core E Community da reconstrução final são
  idênticos aos da revisão r3 testada. Não se apresenta pytest como consumidor
  exclusivo do install: seus checkouts foram provados byte a byte idênticos.
- Wheels finais `.validation-v040/wheels-materialization-coordinator-final`:
  Core SHA256 6042d6c2f2e30a777c08ddcb7ed527e94da626bea87ae7ac6f608f3aa8771f3b;
  Community SHA256 bc02d22f45053a0c03fc54d19af8f3e4ff0c64f2f8b3dd231c57d6915f7940d4.

O manifesto v1 continua legível e sua retomada de dados permanece suportada;
não se inventa um plano de grafo ausente nesse artefato. A nova retomada exige
v2. O resultado `materialization_retired` só confirma esta fase e mantém o
journal bloqueando bootstrap. Não é certificado de schema/permissões concluídos.
SQL e Grafx continuam commits separados; os fingerprints retidos e o intent
durável tratam a janela entre commit de grafo e ACK. A exclusão cooperativa não
é uma garantia contra escritores raw/old binaries externos aos fences.

Fechamento desta etapa:
- `materialization-coordinator-graph-regression.log`: **15 passed**, 186,33 s,
  suíte `test_grafx_global_retirement.py`. Total distinto da seleção: **95**
  (12 novos de integração/schema + 3 adversariais + 65 regressões + 15 de grafo).
  As duas tentativas inicialmente reprovadas estão registradas acima e não são
  ocultadas ou contadas como aprovação.
- `closure-materialization-coordinator-final.json`: **ok=true**, findings=[] e
  documentation_findings=[]; oito budgets **0/0**. Matrizes oficiais:
  **7.560 imports Core / 1.183 Community, 25 dependências**. Nenhuma exceção
  temporária, adaptador no Core ou acesso Community a privado do Core.
- Ruff e diff --check aprovados. Todos os handles de teste/closure/install
  encerrados. Sem alterações ou reinstalações de produto com testes ativos;
  nenhum processo real de usuário iniciado/parado ou dado real migrado.
- Sem impacto em frontend/REST/MCP e sem alteração de gates de acesso; não
  houve necessidade de testes de frontend nesta composição interna offline.

Continuidade: integrar a limpeza real de permissões com a remoção dos contratos
vivos de Sprint (F3), o corte de schema e o certificado terminal que admite o
runtime. Preservar v1 como leitura/retomada de dados, sem promover implicitamente
artefatos antigos a uma prova de materialização. F2A/F2C residuais, F3/F4/F5 e a
matriz integral continuam pendentes; metadata MCP mantém a última medição
56.024 > 50.800. Objetivo integral ativo; este turno trouxe progresso verificado.

Commit Community: **09c6cce672c6f6fb719e689bf69c3859eef02509**. Publicação normal
do par em feature/v0.4.0, com comparação HEAD/ls-remote e árvores limpas.

### 2026-09-20 — F3: retirada da família MCP dedicada de Sprint (em validação)

Retomada do par fdd195ed/09c6cce6, árvores limpas. A limpeza de permissões
continua exigindo registry exatamente igual ao snapshot menos folhas retiradas;
não será antecipada afrouxando esse gate enquanto REST/domínio consumirem Sprint.
A remoção autorizada pelo plano F3 começa nesta etapa pelas 14 tools dedicadas
de Sprint, seus aliases, policies de catálogo e classificação de reader.
Q&A consolidado também perde o destino Sprint: o caso de uso deixa de encaminhar
qualquer target desconhecido ao serviço Sprint. Nenhuma permissão Sprint passa
a autorizar Spec/Card. Recursos exclusivos e instruções das tools retiradas
saem do catálogo; a geração oficial continua obrigatória.

Investiguei entrada → autorização/UoW → serviço → commit/log dos handlers e
o fallback de McpAskQuestionUseCase. Foram preservados os testes compartilhados
de escopo de Board e as provas de negação/estado das entidades remanescentes.
Os testes exclusivos dos handlers removidos são substituídos por provas de
ausência no catálogo e recusa de invocação/alias, inclusive no FastMCP Community.
Sem mudança de frontend/REST nesta etapa. A investigação também identificou
policies de reader órfãs na primeira geração: o guard recusou importação e as
cinco entradas retiradas foram removidas, sem alterar o guard.

Estado parcial deliberadamente não liberável: leituras genéricas, contexto,
REST, UI, gates e contratos de domínio ainda têm Sprint. Não representa F3
completa nem cutover de schema. Permissões, backup, journal e admissão do
runtime permanecem com as garantias anteriores.

Evidência desta etapa (scratch `.validation-v040`):
- A primeira geração recusou readers sem policy, conforme descrito acima.
  O primeiro build também recusou o `force-include` de sprints.md já removido;
  o mapeamento exclusivo saiu do pyproject. Nenhum fallback de packaging.
- `provenance-sprint-mcp.json`: **813/337 .py**, **876/421 payloads** idênticos
  entre os dois fontes, wheels e site-packages, com origens no venv comprovadas
  antes de executar comportamento. Pytest usa checkouts provados idênticos.
- `sprint-mcp-core.log`: inicialmente **124 passed, 4 failed**, 21,32 s.
  As quatro falhas estavam no teste novo invocando wrapper keyword-only com
  argumentos posicionais. Corrigida apenas a chamada do teste, sem alteração
  do produto; `sprint-mcp-core-new-r2.log`: **23 passed**, 2,24 s.
  União distinta deste grupo: **128 aprovados**, sem contar os replays duas vezes.
- `sprint-mcp-transport.log`: **26 passed**, 15,38 s: transporte novo mais
  regressões de host/admission. FastMCP lista exatamente o catálogo atual,
  recusa as 14 invocações antigas e o ask/Sprint não alcança UoW, mesmo com `*`.
- `sprint-mcp-resources.log`: **222 passed, 1 failed**, 13,37 s. A única falha
  é `test_initial_footprint_under_budget`: **54.349 > 50.800 tokens**. A medição
  anterior era 56.024; redução de 1.675 tokens nesta superfície. Não é benchmark
  de fluxo completo e o budget continua intacto/vermelho. Catálogo: **311 tools,
  308 policies e 3 exemptions humanos**. Sem remover tipagem para ganhar tokens.
- Total distinto selecionado: **376 aprovados, 1 falha conhecida de metadata**.
  Preservadas as negativas de autorização/Board e os controles das entidades
  remanescentes. Nenhuma suite de frontend foi necessária: não há alteração
  de UI/REST ou dos seus contratos nesta retirada de transport MCP.
- Closure inicial: findings=[], oito budgets 0/0, apenas matrizes dos dois
  READMEs divergentes. Regeneradas pelo renderer oficial (7.512 imports Core /
  1.183 Community, 25 dependências). Ruff e diff --check aprovados.
- `provenance-sprint-mcp-final.json` confirma novamente **813/337 .py** e
  **876/421 payloads**. Após regenerar os READMEs, o hash agregado do conteúdo
  dos DOIS pacotes é igual ao da revisão testada; nenhuma mudança de produto
  ou reinstalação ocorreu com testes ativos.
- Wheels finais `wheels-sprint-mcp-final`:
  Core SHA256 cad901474b93a103f4227be0fff9480ff270749d8c880aa0f5a02a871efe523a;
  Community SHA256 81323392ddde28b8b193cd10e004d86eb8d1542cca67850c9d5291e92b80d323.

Retomada F3: retirar destinos Sprint das leituras genéricas/contextos MCP e
dos casos de uso exportados restantes, seguindo seus consumidores REST/UI;
compor a remoção de `sprint_assignment_block` e da exigência de Sprint encerrada
na conclusão de Spec com hotfix A/B, sem mexer nos demais gates. Só depois do
registry efetivamente reduzido executar cleanup com recibo durável e corte de
schema, mantendo o bootstrap bloqueado até prova terminal. O E2E Community
`test_global_discovery_recovery_installed_e2e.py` ainda congela inventário antigo
341/333 e alias Sprint; precisa revisão com a superfície final e execução real
pareada, não pode ser citado como prova deste catálogo. F2A/F2C residuais,
F3/F4/F5, matriz integral e rollout permanecem pendentes. Objetivo integral ativo.

Fechamento da etapa: `closure-sprint-mcp-final.json` **ok=true**, findings=[] e
documentation_findings=[], oito budgets **0/0**. Todos os handles de testes,
build/install e closure encerrados; sem intervenção em runtime/dados reais.
Commit Community **30ce1e0964a6d4a65a4da9a7418d959a842461cc**. O par segue para
push normal em feature/v0.4.0, com verificação HEAD/ls-remote ao encerrar.

### 2026-09-20 — Decisão F3 pendente: tarefa normal em Spec Done

Retomada do par publicado f84def2e/30ce1e0, árvores inicialmente limpas. Turno
anterior classificado como progresso verificado. Investigação dos consumidores
de `sprint_assignment_block` encontrou divergência material com a premissa F3:
`CardService.create_card` aceita explicitamente SpecStatus.DONE também para
normal; update_card não bloqueia por estado da Spec; spec_maturity_block usa
apenas limite mínimo. A exceção de bug, portanto, não é o único acesso atual.

`tests/test_f3_done_spec_characterization.py` reproduz com SQL descartável e
serviços reais: em Spec Done, criar e editar tarefa normal são aceitos; sem
Sprint ela avança para Started, enquanto uma Sprint fechada produz
`sprint_required`. A Spec permanece Done nos dois casos. **2 passed** em
`done-spec-characterization.log`, após `provenance-done-spec-characterization.json`
provar 813/337 .py e 876/421 payloads idênticos ao par instalado/testado anterior.
Não é premissa inferida de comentário nem autorização para perpetuar o resultado.

F3 determina não liberar escrita arbitrária normal em Spec concluída e testar
cada tipo; simplesmente apagar o bloqueio Sprint ampliaria o conjunto efetivo
de execuções. Pela política 10.2/10.4 do plano, isso requer decisão explícita
antes de introduzir/restringir um gate que a base não possui. Proposta: recusar
criação/alteração de conteúdo e início/reabertura de execução de tarefas normais
em Spec Done; revisão autorizada da Spec volta a permitir o fluxo. Preservar
leitura/histórico/colaboração e os fluxos legítimos de bug e teste de regressão
com seus controles próprios; não reabrir Spec automaticamente nem afrouxar
evidência/amendment. A retirada dos gates Sprint que afeta esse caminho fica
isolada aguardando a decisão; leituras genéricas MCP continuam independentes.

### 2026-09-20 — F3: retirada de Sprint das leituras genéricas MCP

Retirado Sprint de BoardEntityType, filtros Board/Q&A, listagem MCP por Board,
paginação mcp_sprint_list e descoberta MCP de transições. A chamada interna de
listagem também rejeita destinos desconhecidos antes de acessar persistência,
impedindo o antigo fallback para Topics. Demais entidades mantêm paginação,
contagem, escopo Board e filtros de arquivamento. REST e gates de execução não
foram alterados nesta etapa; a decisão F3 acima continua pendente.

Atualizados os recursos de listagem/erros e o override Community; manifest de
resources e catálogo regenerados pelos geradores oficiais. Catálogo permanece
com 311 tools. Teste no host Community real verifica o enum publicado e a
rejeição de Sprint antes de autenticação/UoW, sem retornar itens históricos.

Evidências em PULSE_REFACTOR/.validation-v040:
- Ambos os wheels reconstruídos e instalados, processos encerrados antes dos
  testes. provenance-sprint-mcp-reads.json: 813/337 .py e 876/421 payloads
  source/wheel/install byte-a-byte idênticos; origens no venv instalado.
- sprint-mcp-reads-core.log: 146 passed e um erro no novo teste (PageRequest
  sem offset/limit). Corrigido somente o teste; sprint-mcp-reads-core-r2.log:
  9 passed. União dessa seleção: 147 testes distintos aprovados.
- sprint-mcp-reads-contracts.log: 187 passed e a falha preexistente de footprint:
  54.330 > 50.800 tokens (antes 54.349). Teto preservado; não é gate verde.
- sprint-mcp-reads-transport.log: 16 passed. Total: 350 testes distintos
  aprovados, uma falha conhecida. Ruff e git diff --check aprovados.
- closure-sprint-mcp-reads.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0; 7.512 imports Core / 1.183 Community / 25 dependências.
- Wheels wheels-sprint-mcp-reads: Core SHA256
  cee3786bc33628404910d7f9ae2639ff38e84415ac53e1da569b77e6524b87da;
  Community 34377cb08c27122b33b9b957c100f8ee38839d716af60cd320db1fcd2344a60f.
  Não houve alteração de produto após a prova de identidade. Todos os handles
  de testes/closure/build/install terminaram; sem runtime ou dados reais tocados.

Retomada: investigar/remover agregação Sprint em get_spec_context e projeções
relacionadas, preservando controles e conteúdo das outras entidades; ainda há
consultas Sprint no resolver de validação de Card e rastreabilidade. Não tratar
esta etapa como eliminação integral de Sprint MCP. A retirada dos gates depende
apenas da decisão F3 registrada; demais frentes independentes continuam. Sem
alteração de frontend nesta etapa. Objetivo integral ativo e incompleto.

Commit Community e3ab8d12e338b45ed7b9ae526fcfccc282a30c00; par preparado para push normal em feature/v0.4.0 e verificação de igualdade remota.

### 2026-09-20 — F3: contexto Spec e remoção dos casos MCP exclusivos de Sprint

O checkpoint anterior foi publicado e verificado: Core 15965c308001956a0100b7734cf7c2c4b9da0d1a
/ Community e3ab8d12e338b45ed7b9ae526fcfccc282a30c00; HEAD=ls-remote e árvores limpas.

Investigação B (§10.4): get_spec_context buscava Sprints pelo serviço, devolvia
seus dados e fazia commit dentro de try/except amplo. SpecService.get_spec usa
somente leitura com includes de cards/knowledge/QA/architecture; o UoW Community
faz rollback de transação ainda ativa na saída. Retirado o bloco Sprint inteiro,
o campo sprints e sprint_id dos Cards dessa resposta, sem mudar a leitura de
requisitos/recursos, readiness ou ponte para arquivo histórico. Os quatro perfis
(summary/detail/full/legacy) não reintroduzem o campo eliminado.

Também removido mcp_sprint_crud.py e seus 24 exports (8 trios command/result/use
case). Busca nos DOIS src comprovou ausência de consumidores de produto depois
da retirada das tools dedicadas. Removidos apenas casos de teste exclusivos
desses use cases. Permanecem as negativas de Board dos use cases REST ativos e
as negativas de autoridade das outras entidades. Teste novo confirma ausência
do módulo instalado/exportado; isso não equivale à retirada de sprints_crud.py,
SprintService ou registry/permissões operacionais, ainda pendentes para F3.

Verificação em PULSE_REFACTOR/.validation-v040:
- Primeira seleção sprint-context-behavior.log: 136 passed e 4 erros no import
  do novo teste; corrigido para services.main. Reexecução específica: 4 passed.
  Foi erro de harness, sem alteração de semântica ou relaxamento de assert.
- Após remover o módulo morto, reconstruídos/reinstalados os DOIS wheels.
  provenance-sprint-context-r2.json: 812/337 .py e 875/421 payloads source/wheel/
  install idênticos. Reinstalação encerrou antes de iniciar a seleção final.
- sprint-context-final-behavior.log: 205 passed, incluindo 4 perfis com Sprint e
  Card persistidos, nenhuma consulta Sprint/commit e registros finais intactos;
  permissões, contexto, readiness e isolamento dos use cases remanescentes.
- sprint-context-final-contracts.log: 164 passed e a única falha conhecida:
  tools/list 54.330 > 50.800 tokens; limite mantido. Catálogo gerado oficial.
- sprint-context-final-transport.log: 16 passed. Total final: 385 testes distintos
  aprovados, uma falha de footprint. Ruff e git diff --check aprovados.

Retomada independente: a rastreabilidade viva ainda agrega Sprint no adapter
Community sqlalchemy_traceability_read_model.py (_spec_summary, resolve_lineage_root,
build_lineage_graph e overlays); LineageGraphModal.tsx renderiza a etapa e os
links. Essa frente exige alteração coordenada do report/REST/UI e testes de
frontend, evitando quebrar os Cards/bugs de origem durante a retirada. Há cópia
legada tests/sqlalchemy_test_traceability_read_model.py no Core: confirmar o
provider realmente executado antes de usar seus testes como evidência. O resolver
de validação ainda consulta Sprint; integrá-lo com a compatibilidade migrada
por Card na retirada operacional. Decisão F3 sobre normal em Spec Done segue
pendente; não houve mudança dos gates. Nenhum frontend alterado nesta etapa.
Objetivo integral permanece ativo, sem declaração de F3/F4/F5 ou rollout completos.

Confirmação da investigação para a próxima frente: tests/conftest.py:217–229
registra explicitamente sqlalchemy_test_traceability_read_model como provider
de teste no Core. Usar testes Community com adapter real para a retirada de
Sprint em lineage/report; os testes Core existentes isoladamente não bastam.

Closure r2: findings=[] e oito budgets 0/0; somente as duas matrizes README
estavam divergentes (7.501 imports Core após remover 11 imports). Regeneradas
pelo renderer oficial. Após isso, reconstrução/reinstalação final dos DOIS
wheels e provenance-sprint-context-final.json: 812/337 .py, 875/421 payloads;
hashes agregados de ambos iguais aos da revisão r2 testada. Wheels finais:
Core bf606db7ab190ab4a7fe2c987f99aff25b9d82ca2da6059c8717e91104cd8f74;
Community 6820dcd6b9c73af5394488579494d58367fc7f43e94eb6d5ab42009a63485f7e.
Nenhuma mudança de produto/reinstalação ocorreu com testes ativos.

Fechamento: closure-sprint-context-final.json ok=true, findings=[] e
documentation_findings=[], oito budgets 0/0. Todos os processos desta etapa
encerrados; Community 071b0ebda57ba4f5379cdb90cf1d4a25d0e6ca6f. Commit e push normal
em feature/v0.4.0, com verificação HEAD/ls-remote. Sem dados/runtime reais tocados.

### 2026-09-20 — F2B/F3: corrigir policy migrada na leitura compacta de gate

Checkpoint anterior publicado e verificado com árvores limpas: Core
3620a24754575e4bc23aef43b295527495410ad2 / Community
071b0ebda57ba4f5379cdb90cf1d4a25d0e6ca6f. A investigação do resolver encontrou
uma integração faltante: _TASK_GATE_CARD_SELECT_FIELDS não selecionava
migrated_validation_policy. Não altera a decisão F3 pendente; é preservação da
compatibilidade por Card já autorizada na F2B.

Reprodução com o par comprovadamente instalado em provenance-sprint-context-final:
6 casos MCP; summary/all e full/all mantinham 90/60, full/gate devolvia 70 nos
dois casos (migrated-context-before.log: 4 passed / 2 failed, assert 70==90/60).
Foi acrescentado somente o campo de policy à projeção limitada de Card, com
comentário de deprecation/migration-only. O resolver, a precedência e a autoridade
de escrita não mudam. A representação não deve ser removida enquanto existirem
overrides necessários, salvo revisão humana autorizada (ver decisão F2B).

Testes de regressão cobrem três perfis/escopos, valores 90/60, False/zero,
proveniência por campo e recusa de policy com Board divergente; a leitura não
muta o registro. Teste Community exercita projeção SQL pelo adapter real,
sem carregar os demais campos de Card, e entrega o resultado ao resolver Core.
Sem alteração de UI/REST; testes frontend não se aplicam a essa correção MCP.

Prova antes dos testes: ambos os wheels reconstruídos/reinstalados;
provenance-migrated-context.json confirma 812/337 .py e 875/421 payloads idênticos
source/wheel/install. Wheels-migrated-context:
Core a361ab520d3c6f40379e18891bb0ea239abbee3d916c1dcea5fecf3116dcc961;
Community 6820dcd6b9c73af5394488579494d58367fc7f43e94eb6d5ab42009a63485f7e.
Seleção Core: migrated-context-core.log, 77 passed. Community inicial:
12 passed e 2 falhas do novo harness sem CommunitySemanticSession; ajustado
somente o teste para a composição obrigatória usada pelo adapter, sem relaxar
esse guard nem alterar produto. Reexecução e closure registradas abaixo.

Fechamento: migrated-context-community-r2.log 2 passed; união da seleção
Community: 14 testes distintos aprovados. Total da correção: 91 testes distintos
aprovados. Ruff/diff --check aprovados. closure-migrated-context.json ok=true,
findings=[] e documentation_findings=[], oito budgets 0/0. Nenhuma mudança de
produto após a prova de identidade. Todos os handles encerrados.
A falha conhecida de footprint MCP 54.330 > 50.800 permanece da seleção anterior;
não foi reclassificada como verde nem seu limite alterado por esta correção.
Retomada: rastreabilidade viva Sprint e demais pendências coordenadas F2–F5,
matriz integral e rollout; decisão F3 sobre normal em Spec Done ainda pendente.
Objetivo integral ativo e incompleto; nenhum runtime ou dado real alterado.

Commit Community 26283b6acece2ab7a13ddd89d89e22dc14a27efe; push normal do par em feature/v0.4.0 com verificação HEAD/ls-remote.

### 2026-09-20 — F3/F5: rastreabilidade e grafo frontend sem Sprint

Retomada do par publicado 3eb30a47/26283b6, árvores limpas; turno anterior
classificado como progresso verificado. Investigada a cadeia MCP report → porta
Traceability → adapter SQL Community → grafo REST → LineageGraphModal. A retirada
é da projeção operacional, sem executar migração ou alterar os registros legados.

No adapter Community, removidos preload/summary de Sprint, resolução de raiz
Sprint e sprint_id dos reports e overlays de dependência. Tarefas/testes ligam-se
diretamente à Spec por has_card; bugs preservam originates_bug e regression_test.
Sem origem, o bug continua vinculado à Spec. Seleção de raiz Sprint falha
unsupported_entity_type antes de lookup. Escopo por Board, demais raízes, limites,
dependências e conteúdo de artefatos preservados. O fixture adapter Core foi
atualizado ao mesmo contrato; a comprovação principal usa o adapter real Community.

Frontend: removidos etapa/estilo/ícone/rota de detalhes Sprint do modal de lineage,
o tipo Sprint do contrato LineageEntityType e o botão de lineage de SprintModal.
Tarefas/testes são etapa 3, bugs etapa 4; estágios relativos de dependências
continuam independentes. Reescrita a fixture de ramos para ligar a Spec aos Cards
sem eliminar a cobertura de layout e dependências. SprintModal/REST/analytics
operacionais ainda existem fora desta superfície; sua retirada integral continua
pendente, sem declaração de F3/F5 concluídas.

Evidências em PULSE_REFACTOR/.validation-v040:
- lineage-frontend-build.log: tsc + Vite aprovados; sincronizados 78 arquivos em
  frontend_dist, árvore SHA256 b0ba4e4c9ebaf77c4bcf73e3b4d1610b6f920997069c58dfb952e9319e9ba3a8.
- Ambos os wheels reconstruídos e instalados. provenance-sprint-lineage.json:
  812/337 .py e 875/421 payloads source/wheel/install idênticos antes dos testes.
  Wheels-sprint-lineage: Core
  3d621e2a4101aacb49cee4eb094d0854cc4d71a9d9b439459e45967a8fbcf642;
  Community 0394f7e818bbadb1856db03ce410aaa64823941b448f24b8a3fd2682970613bc.
- sprint-lineage-core.log: 18 passed. sprint-lineage-community.log: 21 passed.
  Fixture real com Sprint fechada e três Cards confirma ausência de consulta à
  tabela Sprint e de exposição no report/lineage/overlays, vínculos Card/bug/teste
  preservados e negações cross-Board; registros originais permanecem intactos.
- sprint-lineage-resources.log: 149 passed e uma falha já conhecida de footprint:
  54.328 > 50.800 tokens. Limite mantido; catálogo/manifest regenerados oficialmente.
- sprint-lineage-frontend.log: 46 passed e a legenda antiga esperando Sprint;
  ajustada somente essa expectativa exclusiva da entidade removida.
  sprint-lineage-frontend-r2.log: 24 passed. União: 47 Vitest aprovados.
- sprint-lineage-browser-r4.log: 1 Chromium aprovado na SPA empacotada, servida por
  processo temporário em 127.0.0.1:5189, iniciado depois do build/prova de identidade.
  Todas as APIs interceptadas, nenhuma escrita nem erro de browser: Spec → grafo
  com aresta has_card → tarefa → abertura de detalhes. Não é E2E do runtime real.
  Falhas iniciais do harness: seletor pegava botão atrás do modal; depois fixture
  inventava root_ideation=null apesar do adapter preservar objeto compatível para
  Spec. Corrigidos escopo do seletor e fixture conforme contrato real; nenhuma
  flexibilização de assert de navegação ou mudança de produto para mascarar erro.
- Total: 188 backend + 47 Vitest + 1 Chromium = 236 testes distintos aprovados,
  uma falha conhecida de footprint. Closure-sprint-lineage.json ok=true,
  findings=[] e documentation_findings=[], oito budgets 0/0 (7.501/1.183 imports).
- Nenhuma mudança de produto após a prova de identidade; ajustes posteriores
  foram exclusivamente em testes. Testes/build/install/closure terminaram; servidor
  estático temporário encerrado pelo seu handle. Nenhum runtime/dado real tocado.

Retomada: remover demais superfícies Sprint de UI/REST/analytics em conjunto com
contratos e rejeição explícita de inputs legados; completar schema/registry e
cleanup de permissões com o coordenador offline. A decisão F3 sobre normal em
Spec Done continua pendente; gates de execução não foram alterados. F2A/F2C/F2D,
F4, matriz integral e rollout ainda exigem conclusão. Objetivo integral ativo.

Ruff/diff --check e verify:frontend-dist aprovados. Commit Community 5f31e85c008aad6192bfe8dd3361ae0eb9ffc4b4; par para push normal em feature/v0.4.0, com HEAD/ls-remote conferidos ao encerrar.

### 2026-09-20 — Configuração canônica de validação na leitura do Card (pré-requisito F3)

Retomada do par fff6f1a2/5f31e85c, preservando o objetivo integral. A investigação
das superfícies REST/UI de Sprint encontrou um consumidor legítimo remanescente:
CardModal buscava Sprint para recalcular limites de validação. Remover essa rota
diretamente quebraria a validação e a preservação F2B. Resolvido o pré-requisito
pela projeção tipada validation_config no GetCardUseCase; a política e sua
precedência continuam no resolver existente do Core, sem mudar gates de execução.

Autoridade: a leitura passa por card -> Board -> ator antes da projeção, usando a
mesma cadeia de acesso das antigas leituras de Spec/Sprint. As fontes vinculadas
devem existir no mesmo Board; ausência ou vínculo cross-Board mantém o Card
legível, mas retorna configuração indisponível e impede envio pela UI. A redação
de histórico pela permissão card.validation.read continua intacta. A projeção é
somente leitura, não concede ao executor edição de policy nem grava/commita.
O campo está no DTO de resposta; somente o GET autorizado o preenche nesta etapa.

Frontend: usa o valor/proveniência do Core, deixa de buscar Sprint e de duplicar
o algoritmo de precedência. Removida a informação operacional Sprint do painel
de referências do Card. Permanecem o controle de respostas fora de ordem, o
bloqueio durante atualização, retry e limites históricos capturados na avaliação.
Os quatro testes da antiga função JS foram substituídos pela cobertura do
resolver canônico, da leitura real e do modal; não há mais algoritmo JS a testar.
Os campos migrados por Card continuam de compatibilidade, com proveniência e
deprecation já autorizadas, sem novo campo editável ou alteração na política.

Evidências em PULSE_REFACTOR/.validation-v040:
- card-policy-ui-build.log: tsc/Vite aprovados; 78 arquivos sincronizados,
  SHA256 77f76159d8d967617a6d36a9b189a6ea0891da244cb4452014bb787f15f758c5.
  verify:frontend-dist confirmou o mesmo payload ao final.
- provenance-card-policy.json e provenance-card-policy-final.json: 812/337 .py e
  875/421 payloads byte a byte idênticos entre árvores, wheels e instalação, antes
  dos testes. O par foi reinstalado após regeneração oficial dos READMEs; ambos
  os agregados finais são iguais aos inicialmente testados. Não houve mudança de
  produto depois da primeira prova, somente fixtures/testes e matriz documental.
- Wheels finais: Core
  8153698bcd263679aed02ea649d65adc267239d6dc4b70d46b6d17f28782ff2f;
  Community 17d81b6fd4a9011af88e57a9f0351c032033cc33154bc2526bfd38c58041af3f.
- card-policy-core.log: 70 passed (configuração, CRUD, autorização de validação,
  política migrada e contexto MCP). Inclui 90/60, herança atual, false/zero,
  rejeição de Board alheio e ausência de commit.
- card-policy-community-r3.log: 5 passed com CommunityUnitOfWorkFactory e
  persistência SQL real descartável; leitura preserva fonte, valores e negações,
  e não grava. Os primeiros ensaios falharam por fixture sem realm_id e depois
  porta de conhecimento não registrada; corrigida a composição da fixture,
  sem mudar código de produto ou contornar autorização. Os 9 testes de rotas de
  autorização passaram nas execuções anteriores (card-policy-community-r2.log).
- card-policy-community-final.log: repetidos os mesmos 5 casos com observadores
  de SQL/commit do engine, confirmando zero INSERT/UPDATE/DELETE/REPLACE e zero
  commits após a seed, além da comparação do estado persistido original.
- card-policy-ui-r2.log: 69 passed. A primeira rodada teve 56 passes e 3 falhas;
  faltava fornecer config canônica no caso de polling, causando falhas em cascata.
  Atualizada somente a fixture de respostas, preservadas as verificações de
  ordem, histórico e navegação. Todo o arquivo CardModal e três suites adjacentes
  passaram. Total backend/frontend desta etapa: 153 testes distintos aprovados.
- card-policy-catalog.log: mais 4 testes de drift byte a byte do catálogo passaram,
  totalizando 157 testes distintos aprovados nesta etapa. Nenhuma edição manual
  do catálogo nem alteração no registry de tools.
- closure-card-policy-final.json: ok=true, findings=[] e documentation_findings=[],
  oito budgets 0/0, 7.502 imports Core/1.183 Community -> Core, 25 dependências.
  A primeira closure apontou somente a matriz README desatualizada; regenerada
  pelo renderer oficial, reconstruído/reinstalado/provado o par e repetida closure.
- Ruff e diff --check aprovados. Nenhum processo de runtime/dado real tocado.
  Estes testes não representam E2E integral instalado nem conclusão do pacote.

Retomada: agora a retirada das rotas/telas dedicadas a Sprint não depende de
getSprint no modal de Card. Permanecem as demais superfícies operacionais,
analytics, contratos/schema/registry, conclusão do coordenador offline, F2A/C/D,
F4 e matriz/rollout integrais. A decisão F3 sobre tarefas normais em Spec Done
continua pendente; não confundir o sim para F2A histórico com essa autorização.
O gate de footprint MCP previamente vermelho (54.328 > 50.800) permanece uma
pendência conhecida; seu limite não foi alterado. Objetivo integral ativo.

Commit Community desta etapa: a12f496a2c7d202c022634f7c888dc22978f5043.
Todas as execuções acima encerradas; par preparado para push normal em
feature/v0.4.0 e conferência de HEAD/ls-remote ao encerrar o checkpoint.

### 2026-09-20 — Retirada das superfícies dedicadas REST/UI de Sprint (F3)

Retomada verificada do par 9f230a83/a12f496a com árvores limpas. Turno anterior
classificado como progresso publicado. Removidos o router Community api/sprints.py
e seu registro (12 operações HTTP), cliente CRUD/sugestão/histórico no frontend,
SprintsPanel/SprintModal/SprintSuggestionModal, ambas as implementações da aba
Sprint de Spec, sugestões automáticas pós-validação e entrada no Board. Removidos
os DTOs/constantes exclusivos de Sprint no frontend; TaskValidationGateOverride
foi preservado por continuar compartilhado por Spec/Card.

Investigação de consumidores encontrou também links oriundos de KG, Discovery e
readiness cognitivo. Removida a navegação/enriquecimento para a entidade retirada;
referências históricas permanecem legíveis, sem reinterpretar ID Sprint como
Card/Spec. ModalStack recusa destinos não suportados, preservando a pilha atual.
GetCard e sua projeção canônica de policy permanecem; gates de execução e decisão
F3 sobre tarefa normal em Spec Done não foram alterados.

Testes exclusivos da operação retirada (SprintModal, SprintsTab e REST Sprint)
foram substituídos por negativos de inexistência e navegação legada. Nas suites
compartilhadas, removidos somente casos da entidade retirada; limites SQL,
negações, QA, paginação e demais entidades permanecem cobertos. O teste genérico
de RequestValidationError foi preservado com rota de prova independente de Sprint.
As use cases Core sprints_crud e testes diretos de autoridade ainda existem nesta
etapa: não são mais expostas pelo router retirado; remoção interna e superfícies
genéricas/analytics/export/registry/schema continuam como trabalho seguinte.

Evidências em PULSE_REFACTOR/.validation-v040:
- sprint-surfaces-build-ui-final-r2.log: tsc/Vite passaram, 78 arquivos,
  árvore 95cb892eff039db70643f56bc19581a10052e2dbd46e677c1adf0fd4055d6ffe.
  Builds intermediários revelaram consumidores KG/Discovery e fixtures incompletas;
  corrigidos tipos/consumidores e onClose da fixture antes dos testes.
- provenance-sprint-surfaces.json: 812/336 .py e 875/420 payloads source/wheel/install
  idênticos, incluindo prova de ausência do módulo Python removido na instalação.
- sprint-surfaces-core.log: 44 passed; sprint-surfaces-ui.log: 175 passed (15 suites).
- sprint-surfaces-browser.log: 1 Chromium aprovado sobre frontend_dist instalado,
  com preferência localStorage e URL legadas de Sprint: Board/Spec sem aba Sprint,
  Spec -> lineage -> Card, zero requests Sprint, zero escritas/erros de browser.
  Todas as APIs foram interceptadas; não é E2E do runtime integral. Servidor
  descartável 127.0.0.1:5189 iniciado após a instalação/prova e encerrado pelo handle.
- sprint-surfaces-community.log: 108 passed, incluindo 24 chamadas HTTP diretas
  às 12 rotas removidas, ausência do módulo e schemas de request no OpenAPI,
  paginação/QA/autorização/limites SQL das entidades preservadas e policy do Card.
  Total desta etapa: 152 backend + 175 Vitest + 1 Chromium = 328 testes distintos.
- A closure inicial terminou com findings=[] e oito budgets 0/0; apontou somente
  matriz README desatualizada após redução dos imports Community -> Core de
  1.183 para 1.175. Matriz regenerada pelo renderer oficial e wheels reconstruídos
  para a prova final. Core continua em 7.502 imports e 25 dependências no par.
- Todas as suites e o servidor estático encerraram antes de reinstalar o par
  final. Nenhum código de produto alterado após a primeira prova de identidade;
  somente ledger e matriz README. Ruff/diff --check e verify:frontend-dist passaram.
- provenance-sprint-surfaces-final.json confirma identidade integral do par final
  e agregados iguais aos testados. Wheels finais: Core
  44ab7796781f27452184cae46ddd13531349430283b52f574eaa1b2c2aea6518;
  Community ef17d2d7fce32e9dfc2a375184c0a439da9a6bb00bdfcf53cd0b43636b7541ae.
  Agregado Core 22c4f84d7ddfe4645b7a3e754d127a91b3240cdbb6390ba60df2a2f0c597612b;
  Community 6fb0d304a0521ade0957d7625757bcbdf4698cfd52b94ff2b9613dae83abe0ee.

Retomada investigada: sprints_crud.py (12 use cases, 36 contratos/exportações)
agora só é importado em produção por application/use_cases/__init__.py. A remoção
interna é independente do gate de execução pendente. Permanecem explicitamente
analytics.py (painéis/per-Sprint e filtros sprint_id), boards.py/cards_pagination.py,
policy_governance.py, canais genéricos e serializers/export; specs.py ainda traduz
SprintOperationError do gate de fechamento atual. Não declarar zero capacidade
operacional Sprint em todo o produto até retirar também essas superfícies e
coordenar schema/registry/cleanup offline. A decisão F3 continua isolada.

Objetivo integral ativo. Permanecem F2A/C/D, F4/F5, matriz/rollout completos,
coordenador offline terminal e pendência de footprint; nenhum dado real migrado.

Fechamento deste checkpoint: closure-sprint-surfaces-final.json ok=true,
findings=[] e documentation_findings=[], oito budgets 0/0. Builds, instalação,
provas, testes e closure encerrados. Commit Community
a39f7c261416f6186536889801a195ed7d1f211b. Par preparado para push normal em
feature/v0.4.0 e conferência final de HEAD/ls-remote/árvores limpas.

### 2026-09-20 — Decisão F3 autorizada: tarefas normais em Spec Done

O usuário respondeu explicitamente à proposta pendente: "Autorizar o bloqueio
proposto (recomendado)". Está autorizado bloquear criação/alteração de conteúdo
e início/reabertura de tarefas normais em Spec Done, preservando leitura,
histórico, colaboração e bugs/testes de regressão pelos controles próprios.
Esta decisão resolve a divergência reproduzida na seção "Decisão F3 pendente";
não é inferida da autorização F2A nem de tempo decorrido. As referências anteriores
a F3 pendente são histórico dos checkpoints; o bloqueio de decisão está resolvido.

Implementação seguinte: aplicar o gate no Core por todos os writers e previews
pertinentes, com reprodução negativa/positiva, preservando autoridade e demais
gates substantivos ao retirar a dependência operacional de Sprint. Nenhuma
permissão de executor adicional, reabertura automática de histórico, migração
real ou relaxamento de budget foi autorizada por esta decisão.

### 2026-09-20 — Retirada interna das use cases dedicadas de Sprint

Partida: Core 8f12660eb1bc7892c3fef49574cb4fa74bd6d1a1 e Community
a39f7c261416f6186536889801a195ed7d1f211b, ambos publicados em feature/v0.4.0.
Removidos sprints_crud.py (12 use cases), seus 36 contratos/exportações e os
dois helpers exclusivos de autorização de Sprint. Permissões, grants e gates
de Card/Spec não foram relaxados. Inventário AST em src/tests/scripts confirma
zero imports remanescentes dos contratos retirados. Os contratos negativos
novos verificam ausência do módulo, exports e helpers. As suites compartilhadas
mantêm os casos Card/Spec; retirados somente os casos da operação eliminada.

A coleta integral encontrou três imports por alias da antiga rota REST Sprint
que o inventário anterior não identificara. Corrigidos os consumidores em
test_permission_denied_rest_routes, test_card_rejected_rest_contract e
test_sprint_origin_integrity_health, preservando seus testes das operações vivas.
Não houve alteração de frontend nesta etapa; nenhuma nova execução frontend é
alegada. O catálogo MCP gerado permanece idêntico e seu gate passou.

Evidências em PULSE_REFACTOR/.validation-v040:
- sprint-usecases-core.log: 102 passed; sprint-usecases-community.log: 49 passed.
  Total de 151 testes comportamentais distintos aprovados.
- sprint-usecases-collect-core.log: 13.213 coletados; collect-community: 5.792.
  São 19.005 testes apenas coletados, sem erros de import, não testes executados.
- provenance-sprint-usecases.json e provenance-sprint-usecases-final.json:
  811/336 arquivos .py e 874/420 payloads source/wheel/install idênticos, inclusive
  ausência física do módulo removido. Agregados iniciais/finais iguais:
  Core a0780658e11ed80e21e3dab42c987c3c19949574d20151ecf4356c7449f465c9;
  Community 6fb0d304a0521ade0957d7625757bcbdf4698cfd52b94ff2b9613dae83abe0ee.
- A closure inicial apontou somente drift das matrizes README. Renderer oficial
  aplicado: imports Core 7.502 -> 7.492; Community 1.175, dependências 25.
  Reconstruídos/reinstalados ambos os wheels após término de todos os testes.
- Wheels finais: Core e04baca3fab1cff9b48bac84d2971bbbdbdb57c9ba555589567c925f2d2f0e50;
  Community 59dc5906f64e5bcfcf5008c2d1387a3b9762cc49dd6e595ec6c8d2d124b1a7ba.
- closure-sprint-usecases-final.json: ok=true, findings=[] e
  documentation_findings=[], todos os oito budgets 0/0. Ruff e diff --check
  passaram. Todos os processos de validação encerrados antes do commit.

Commit Community bca12e1ea0b9ca53442b6faec094c2d86488b15a. Próxima etapa é o
gate F3 agora autorizado: impedir conteúdo normal na Spec Done, inclusive troca
de vínculo de/para essa Spec, e execução inicial/retomada/reabertura. Investigar
todos os writers/previews, preservando colaboração e os controles bug/test.
Permanecem o restante do corte operacional Sprint e a integração offline/schema,
F4/F5, matriz integral, rollout e footprint MCP. Objetivo integral ativo;
nenhuma migração de dados reais ou conclusão da iniciativa é alegada.

### 2026-09-20 — F3 autorizado: admissão principal e vínculo de tarefa normal

Partida publicada: Core ed59d325c3173f86f4fa0e11b4c1e2200c53638a / Community
bca12e1ea0b9ca53442b6faec094c2d86488b15a. A decisão F3 acima foi aplicada,
sem nova solicitação de aprovação. Este checkpoint cobre criação, update_card,
link/unlink de Spec e início/retomada/reabertura pelo move_card e seu preview;
não é ainda a declaração de fechamento de todos os escritores de conteúdo.

Política pura completed_spec_normal_work_block/completed_spec_execution_block:
normal_card_spec_done com remediation revise_spec_before_normal_work. Somente
tipo Normal em Spec Done é atingido; Bug/Test conservam os controles próprios.
As arestas exatas de início/retomada já definidas em spec_dependency são usadas,
incluindo Done/Rejected/On Hold -> In Progress. Cancelamento, pausa e ordenação
não são convertidos em início. A execução mantém a revalidação transacional de
edition/status/archived existente; o novo bloqueio não elimina outros gates.

require_normal_card_spec_content_allowed usa exclusivamente portas públicas.
Verifica os pais atual/proposto antes de reparenting e recusa Done antes de
qualquer escrita. Para admissão positiva, usa a trava de Board já compartilhada
com lifecycle/dependências, snapshots de Spec sob trava e CAS do vínculo/tipo do
Card, na mesma transação do chamador. Não há commit no guard, SQL no Core,
novo adaptador, exceção de import ou alteração de grants. O mecanismo permanece
no Community. Pais referenciados ausentes/fora do Board não são tratados como
abertos. Legado sem vínculo mantém o contrato existente; nova criação continua
exigindo Spec. CardUpdate já proíbe mudar card_type/origin_task_id.

Frontend: CardModal bloqueia edição de campos e atribuição da tarefa Normal ao
carregar a Spec Done, informa o motivo e respeita o blocker de execução vindo do
Core. Leituras/abas de colaboração permanecem acessíveis. CreateCardModal permite
selecionar Done para regressão (Test) e Bug, e limpa seleção Done ao trocar para
Normal, impedindo conservar um pai oculto. A escrita continua revalidada no Core.

Evidências em PULSE_REFACTOR/.validation-v040:
- f3-done-core.log: 101 passed; f3-done-regressions.log: 241 passed;
  f3-done-catalog.log: 4 passed. Incluem CRUD/autorização, previews, lifecycle,
  dependências, vínculo de cenários e regressão/Path B. A antiga caracterização
  pré-decisão foi substituída pelo contrato autorizado em test_f3_done_spec_admission;
  a reprodução histórica permanece no ledger e no Git.
- test_f3_done_spec_admission mede zero DML para recusas de criação/edição/
  vínculo de entrada/saída, com e sem Sprint histórica; compara preview/mutação
  em quatro arestas; preserva edição Bug/Test e a Spec Done original.
- f3-done-community.log: 14 passed nos contratos REST de rejeição/permissão;
  os três testes novos falharam inicialmente no setup porque faltava a sessão
  semântica composta. Corrigida a fixture para CommunitySemanticSession, sem
  contornar o guard. f3-done-community-fence-r2.log: 3 passed. Portas reais e
  transação independente no intervalo leitura/trava demonstram recusa quando
  a Spec fecha ou o vínculo do Card muda; o caso positivo conserva admissão.
- f3-done-ui.log: 72 passed em CardModal/CreateCardModal, incluindo os três
  tipos na Spec Done e a troca Test -> Normal -> Bug sem envio indevido.
  Total: 346 Core + 17 Community + 72 frontend = 435 testes distintos aprovados.
- f3-done-build-ui.log: tsc/Vite e sincronização passaram; 78 arquivos,
  árvore 2cffc946610da1623cb76d6a884ea5a57fdf5c2d00438d4be82eda6af2d4cedd.
  verify:frontend-dist, Ruff e diff --check passaram. Não há alegação de E2E
  do runtime nem de nova execução Playwright neste checkpoint.
- provenance-f3-done.json e provenance-f3-done-final.json: 811/336 .py e
  874/420 payloads source/wheel/install byte a byte; agregados finais iguais
  aos testados. Core 7df31198c7a31569585921347fab43d3226af9af3089527e1fd5bbbe31d1a733;
  Community 25b3d461864ae2519b3b97a542864bcac65d895049436f54341b69d52953a055.
- Closure inicial: findings=[], oito budgets 0/0, somente drift README;
  renderer oficial atualizou imports Core 7.492 -> 7.496, Community 1.175,
  dependências 25. Ambos wheels finais reconstruídos/reinstalados após término
  das suites; somente README mudou no produto distribuído após a primeira prova.
  Wheels: Core 6d4a760d156f54896faceb35674049d3edb59a4dcbeca4e8b292b11278e5d691;
  Community a78ae41ff275b9c25568644bed433eb5b801578537ecac66d0cfc4d0c2cd6ab3.

Retomada F3 — ainda não declarar o bloqueio exaustivo:
- services/main: add/remove_dependency, attachments, delete_card, backlinks de
  traceability, delete_spec_unlink_card e demais writers diretos precisam da
  mesma admissão quando alterarem conteúdo normal. Archive/restore devem ser
  classificados pelo efeito, sem bloquear leitura/histórico/colaboração por
  analogia automática com o congelamento de Rejected. Writers exclusivamente
  Sprint serão retirados no corte, não convertidos em nova operação.
- usecases: card_traceability, architecture_crud.copy_architecture_to_card,
  knowledge_propagation._load_card_for_v2_write, mcp_resource_stories,
  operational_rest (resource waivers), spec_crud (backlinks IR/OR) e
  code_traceability (evidence/overlap/waiver) ainda têm guard somente Rejected.
  MCP server ~12530 também tem o guard local de link_card_traceability.
- Porta existente ApplicationServiceCatalog.cards retorna o serviço Core
  CardService; uma fachada pública de admissão nele pode reutilizar o guard
  transacional para use cases sem extrair contexto de edição nem importar
  privados. Confirmar escritores nos serviços de propagação e recursos além
  dos callers do guard Rejected, e adicionar testes negativos antes do primeiro
  efeito e positivos de colaboração/bug/test nos adaptadores reais.
- Manter o preview/UI consistente ao ampliar cobertura. Sprint assignment,
  Path C, completion de Spec, analytics/generic APIs/registry/schema e todo o
  coordenador offline terminal seguem pendentes. A autorização F3 está resolvida;
  não voltar a perguntar a mesma decisão. Objetivo integral permanece ativo.

Fechamento deste checkpoint: closure-f3-done-final.json ok=true, findings=[] e
documentation_findings=[], oito budgets 0/0. Todas as suites, builds, instalação,
provas e closure terminaram. Commit Community
ba050beed80f0efa4be9344411ecb91284b914f6; preparar par Core/Community para push
normal em feature/v0.4.0 e conferir HEAD remoto e árvores limpas. Nenhum runtime
do usuário foi parado/reiniciado, nem dados reais migrados.

### 2026-09-20 — F3: conteúdo auxiliar e propagação automática

Partida publicada e limpa: Core 22666b6000037a67048f38bdf39e578e1f2806a1 /
Community ba050beed80f0efa4be9344411ecb91284b914f6. Criada a fachada pública
CardService.require_content_mutation_allowed, acessível pelo catálogo tipado
existente, para compor Rejected + F3 sem extrair contexto de edição nas use cases.
Aplicada a dependências do Card (add/remove), deleção do Card, upload/delete de
anexos, replace/drop/refresh de Knowledge e cópia de Architecture pela use case.
Recusas precedem storage, persistência de conteúdo e commit. Replays sem efeito
continuam seguindo o comportamento idempotente existente.

SpecResourcePropagationService agora pula explicitamente Normal em Spec Done,
preservando seus snapshots, e mantém fanout para Bug/Test elegíveis. Não captura
conflitos de vínculo, falhas de porta ou outros erros como se fossem sucesso.
Reutiliza a trava e o CAS públicos do checkpoint anterior, sem adaptador no Core.

Divergência real investigada: ResourcePropagationCardRecord não transportava
status; o guard card_is_rejected, já existente, recebia um record sem esse fato
e retornava falso. Completada a porta com spec_id, status e card_type tipados e
obrigatórios, preenchidos pelo adaptador Community e pelo adapter de testes Core.
Não foi inventada nova autoridade para Rejected: corrigiu-se a perda dos fatos
necessários ao gate que já existia. As três fixtures unitárias de transformação
de snapshots declaram explicitamente seu Card sintético legado sem vínculo;
elas não são usadas como prova de isolamento ou de admissão em Spec real.
A prova de gates usa os adaptadores reais Community e fixtures SQL completas.

Frontend: anexos, deleção, dependências, Knowledge e Architecture respeitam o
estado de conteúdo congelado no CardModal/CardResourcesPanel. As abas e leituras
continuam disponíveis. Os testes exercitam os três tipos de Card, conferindo
affordances de anexos/delete e os props readOnly/locked dos recursos.

Classificação que evita ampliar a autorização por analogia: as implementações
SpecService de append FR/TR/Decision e unlink de scenario backlink documentam
esses vínculos como metadata de rastreabilidade, fora do snapshot semântico.
O adapter de versionamento também exclui lifecycle, conclusions, validations,
rejection records, archive e position do conteúdo semântico do Card. Portanto,
não aplicar cegamente o guard de conteúdo em todo caller do antigo freeze de
Rejected. Esses caminhos precisam manter os controles próprios e ser testados
como história/evidência/colaboração conforme seu efeito; não há autorização
genérica para proibir append histórico ou alterar gates de validação/waiver.
Essa classificação refina a lista investigativa anterior, não reabre a decisão F3.

Evidências em PULSE_REFACTOR/.validation-v040:
- f3-content-core.log: 166 passed, 1 falha de fixture de compensação de anexo
  (Card sintético sem card_type/spec_id). Corrigida a fixture, sem default novo
  no serviço. f3-content-core-r2.log: 48 passed e uma falha no novo teste de Q&A:
  a própria regra pré-existente recusou self-answer. O teste foi corrigido para
  provar essa recusa e então aceitar resposta de outro principal, sem mudar
  policy. f3-content-collaboration-r3.log: 1 passed.
  União sem duplicatas: 176 testes Core aprovados; inclui nove casos novos
  adicionados após a primeira execução (quatro use cases x duas origens Sprint
  e a prova de colaboração), além da fixture de compensação corrigida.
- Negativos F3 cobrem 16 operações com/sem Sprint histórica, medindo zero DML
  e usando sentinela que falha se o storage for acessado antes da recusa.
  Q&A, comentários e leitura funcionam em Card Normal Done/Spec Done; o mesmo
  principal continua proibido de responder à própria pergunta pela policy.
- f3-content-community.log: 23 passed, incluindo os 3 testes de corrida/CAS,
  6 casos de projeção/propagação real e 14 contratos REST de rejeição/permissão.
  Normal Done e Rejected (inclusive Bug) não fazem DML de propagação; Normal
  aberto e Bug/Test Done conservam admissão e snapshots existentes. Os casos
  positivos usam origem sem novo mockup; não alegam validação de design system
  nem cópia de novo mockup nesse teste. As suites Core cobrem a cópia de recursos.
- f3-content-ui.log: 79 passed (CardModal + CardKnowledgeTab).
  Total deste checkpoint: 176 + 23 + 79 = 278 testes distintos aprovados.
- tsc/Vite, sync e verify:frontend-dist: 78 arquivos,
  árvore 524dac680074d4492a6673c8960bc247d4f68b355535d035135eae1dc5db1d8c.
  Ruff e diff --check passaram. Sem nova execução Playwright/E2E integral.
- provenance-f3-content.json e provenance-f3-content-final.json provam 811/336
  .py e 874/420 payloads source/wheel/install idênticos. Agregados iniciais/finais:
  Core bd75683b67a59ef69d6191fceff0d17cc3a5b23710f71dc61336a0e15aad0262;
  Community c05da1cc94bb8d77b842b524c12bac71a486ecdbaab02e07290ad641ebdfdc8a.
- Closure inicial findings=[], oito budgets 0/0, somente matrizes README em drift.
  Renderer oficial: Core 7.497 imports, Community 1.175, dependências 25.
  Ambos wheels finais reconstruídos/reinstalados após encerrar testes; payloads
  iguais aos testados. Core b2c0e19d753ab1fb72c3dec30eeaa5ff8839cd280227c0564718f60b64f9c2fe;
  Community e4f60ee6290bd35aff638da7704af5e34ed24f33acdcef4c934110c7e1343eb8.

Retomada: auditar efeitos de restore_tree/import e delete_spec_unlink_card,
além dos entry points de recursos abaixo das use cases. Restore repõe
pre_archive_status; precisa de reprodução para distinguir restauração histórica
de retomada de execução normal em Spec Done, sem reescrever histórico. A lista
anterior de evidências/waivers/backlinks continua como inventário a classificar,
nunca como autorização automática de bloqueio mais amplo. Ainda falta retirar
Sprint dos gates/domínio/superfícies genéricas e completar o corte offline/schema,
F4/F5, matriz integral, footprint e rollout. Objetivo integral ativo; nenhuma
migração real, nova permissão ou aumento de budget realizado.

Fechamento: closure-f3-content-final.json ok=true, findings=[] e
documentation_findings=[], oito budgets 0/0. Todos os processos de validação
encerrados. Commit Community f76bb240e888b09bf12090cde698101eb43a65cd;
par pronto para push normal e conferência de sincronização em feature/v0.4.0.

### 2026-09-20 — F3: restauração, admissão sem Sprint e retirada de Path C

Partida publicada: Core a229289b4b4aa8a76551fe235424d4988d114c41 /
Community f76bb240e888b09bf12090cde698101eb43a65cd. A autorização de bloquear
conteúdo/início/reabertura Normal em Spec Done permanece resolvida; não repetir
pedido. Não houve nova autoridade, migração de dados reais ou aumento de budget.

Reprodução adicional do desvio de restore_tree: provenance-f3-restore-baseline.json
comprovou identidade source/wheel/install do par de partida. O primeiro teste
usou root `card`, que a operação não suporta: suas 36 falhas não provam o bug.
Corrigido para root `spec` (forma já suportada para restaurar descendentes),
f3-restore-baseline-r2.log demonstrou 2 falhas/34 passes: Normal arquivado em
Started/In Progress voltou a executar sob Spec Done. ArchiveService agora faz
preflight antes de qualquer restauração, usando a fachada pública transacional
existente. Restauração de histórico/pausa e Bug/Test conserva os controles próprios.
A matriz cobre 2 estados de Spec x 3 tipos de Card x 6 estados históricos.

F3 itens 2/3/5: retirados sprint_assignment_block, os sete fatos de Sprint em
CardTransitionFacts, o gate de lane no avanço e no avaliador de conclusão do Card,
e a exigência de Sprints fechadas/mínimo de uma fechada na conclusão da Spec.
Preview e mutação foram alterados juntos. Requisitos, maturidade, dependências,
validação, cenários, provas de entrega, cobertura e política continuam nos seus
caminhos. O read de policy legada por Sprint continua nesta etapa, até o cutover
preservar os overrides por Card já autorizado em F2B; não zerar esse read antes
da migração fiel das policies. Sprint ainda NÃO está totalmente retirado do domínio.

A investigação encontrou mais um gate exclusivo: Spec Done -> Draft era recusada
se uma hotfix lane dependesse da Spec permanecer Done. Retirado conforme F3;
o teste passa a exigir a nova edição e seu histórico sem fabricar mudanças no
histórico da lane antiga. As demais operações de SprintService serão retiradas
na frente restante, não declaradas concluídas neste checkpoint.

Retirados Path C/standard_sprint, ações assign/activate lane, hotfix_lane_status,
formatter e helpers exclusivos do contrato de remediation. Atualizados serializer
de CardOperationError, export público e allowlist de métricas para não acessar
campo inexistente. Path A e Path B (inclusive validator coverage) preservados.
Frontend remove o conceito/contagem de lane nos painéis, alinha os tipos ao Path B
amendment já existente e reconhece esse caminho mesmo antes da lista de revisions
carregar; coverage_pending continua sem indicar fechamento pronto.

Resources servidos de cards/errors/tool-docs atualizados. tools_catalog.md NÃO
foi editado; seu teste de drift passou no primeiro lote. tool-docs/card.md tem
origem histórica nos scripts R1 de captura/compactação, mas o parágrafo retirado
não está nos sidecars nem no docstring compacto vivo. Portanto a correção foi no
resource long-form canônico, sem regenerar arquivos pela captura antiga e sem
reintroduzir tools removidas. Testes de conteúdo servido preservam lineage,
SpecLockedError e checklist de prova, e proíbem as instruções de lane retiradas.
Dois asserts antigos foram reconciliados ao código já existente: SpecLockedError
vive em domain/spec_content_lock e literais concatenados são lidos pelo AST;
a documentação usa DLQ para dead-letter. Nenhum gate foi relaxado por esses ajustes.

Validação parcial até aqui (logs em PULSE_REFACTOR/.validation-v040):
- f3-lanes-core.log: 194 passed/28 failed. Fixtures novas tentavam aresta Normal
  NotStarted -> InProgress (deve passar por Started), faltava origin_task_id no
  Bug antes mascarado pelo bloqueio de lane e havia expectations de Path C.
- f3-lanes-core-r2.log: 43 passed/7 failed. Spec sintética sem contexto autoritativo
  foi barrada por code_delivery_context_required antes do gate em teste; corrigida
  a fixture usando o manifest/proveniência de contexto direto existente. A última
  falha documental era concatenação de literal. f3-lanes-core-r3.log: 30 passed.
  União do primeiro conjunto: 222 testes distintos aprovados após as correções.
- f3-lanes-ui.log: 73 passed (CardModal e PathBRemediationPanel).
- tsc/Vite + sync + verify:frontend-dist: 78 arquivos; árvore
  639c408cac92294bc31b6aa0e0f3988edf5380c516dc22a9d9afeb16a3a2bf52.
- provenance-f3-lanes.json e provenance-f3-lanes-final.json: 811/336 .py,
  874/420 payloads source/wheel/install byte a byte. Par final reconstruído após
  remover o gate de reabertura; todos os processos anteriores foram encerrados
  antes da reinstalação. Nenhum processo de runtime do usuário foi reiniciado.
- closure-f3-lanes.json ok=true, findings=[], documentation_findings=[], oito
  budgets 0/0. Ruff/diff --check passaram. Validação final do par em andamento:
  f3-lanes-final-core.log, f3-lanes-community.log, closure-f3-lanes-final.json.

Retomada integral: finalizar estes resultados/commit/push e continuar F3 no
service catalog, modelos/DTOs, registry/permissões, APIs genéricas, analytics,
KG e schema/cutover offline. Inventariar outros guards de vínculo de origem
em SprintService/Card update/delete antes de afirmar ausência completa. F2A/B/C/D
coordenador terminal, F4/F5, matriz DEI/ARQ/VER/ADV, rollout/upgrade/rollback,
footprint MCP e E2E do runtime pareado continuam pendentes. O journal offline não
é certificado runtime_ready e não pode ser apagado para liberar bootstrap.
Este incremento não encerra a iniciativa nem afirma novo E2E integral/Playwright.

Fechamento do incremento:
- f3-lanes-final-core.log: 305 passed/2 failed. O teste novo de reabertura
  esquecia o commit da transação pertencente ao chamador: corrigido no teste;
  o histórico da nova edição e a ausência de novo histórico de Sprint passaram.
- A suite antiga de ownership da validação tinha uma fixture sem adoption do
  execution contract. O teste agora primeiro prova a recusa específica e a
  preservação de Current, depois usa planning aceito como colaborador isolado
  (como já fazia para lint e delivery) para testar ownership do ponteiro.
  Não constitui prova de um plano completo. A contagem esperada da chamada foi
  corrigida para uma verificação sob fence, conforme o serviço real.
- f3-lanes-final-core-r2.log: 1 passed/1 failed (contagem da chamada acima);
  f3-lanes-final-core-r3.log: 1 passed. f3-lanes-spec-closure.log: 6 passed,
  ampliando esse mesmo contrato de conclusão para None e todos os cinco estados
  históricos de Sprint, sem alterar seus estados nem Current/edition da Spec.
- União final sem duplicatas: 421 Core + 18 Community (f3-lanes-community.log,
  incluindo portas reais/fence F3 e contratos REST) + 73 frontend = 512 testes.
  Os testes de catalog drift, Path B, restore/resequence, dependências, gates
  de validação, links de requisitos e policy migrada integram os lotes descritos.
- closure-f3-lanes-final.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0; 7.497 imports Core, 1.175 Community, 25 dependências.
- Agregados finais source/wheel/install: Core
  e2c31ac4cadd8522cb9a941041d7dbc0369b2146f235eb28ef5449c3eec7b534;
  Community 28857c2434a26cb4748b6efdadfe338fce8038860c5e7e1ddf07aaedefeb1e9c.
  Wheels finais: Core e88e7c8743d6f48c341b874dfbb1ebef23afd082eee2947a5848f1c75eb36df2;
  Community 42fb3859242735a5fe06e7e36a05b099c9f7667db930b5762b50ee2a3f936b45.
- Todos os processos de validação encerrados. Alterações após a prova final
  restritas a testes/ledger; nenhum payload de produto alterado. Sem novo E2E
  integral/Playwright, sem release/tag/deploy/migração de dados reais.
- Community commit 5aa4dda2a272084de497be8a0bb6952f7f024c0a; preparar commit
  Core e push normal do par. Objetivo integral continua pendente conforme a
  lista de retomada acima, com o ledger como ponto de continuidade.

### 2026-09-21 — F3: retirada do serviço operacional de Sprint

Partida publicada e limpa: Core 286845abc22f18262c4b7318b4b0aa9e36e27a3d /
Community 5aa4dda2a272084de497be8a0bb6952f7f024c0a. O turno anterior foi progresso:
commits/pushes verificados e 512 testes distintos, não mera atualização de status.

Retirados SprintService (22 métodos, incluindo CRUD, transições, assignment,
evaluations, histórico operacional e sugestão), SprintOperationError, o helper
exclusivo de critical action e sua exposição no catálogo/Protocol de serviços.
Removidas as duas entradas de cobertura de mutation guard exclusivas do serviço
apagado; o inventário das demais operações continua exato. Community specs.py
não importa/captura mais o erro de lane que a reabertura da Spec já não produz.
SprintQAService, analytics, modelos e contratos de persistência ainda existem;
não confundir a retirada deste serviço com conclusão integral de F3.

Preview de Card agora pede validation_config_for_card ao serviço público de Card,
sem acessar SprintService nem extrair contexto de edição. Essa fachada apenas
preserva o read legado de policy existente até o offline F2B materializar os
campos por Card. Comentário explícito de depreciação: remover o read após o
cutover fiel, nunca antecipar perda de overrides nem conceder writes ao executor.
Quatro testes com porta real comparam 0/60/90/100 antes e depois da captura por
Card e conservam a origem histórica. O DTO do executor permanece sem esse writer.

ListAllowedTransitions removeu o avaliador/load de Sprint e recusa esse tipo
na autoridade de descoberta; REST e MCP convergem para a operação retirada.
Tipo AllowedTransitionEntityType do frontend deixou de oferecer Sprint. Os
metadados da conclusão de Spec e reference/transitions.md não exigem mais Sprint.

Dependência material confirmada por execução, não omitida: retirar simultaneamente
a entrada Sprint de SDLC_REGISTRY quebrou o import da policy em
permissions.py:771 (historical transition fingerprint). Essa mesma definição
alimenta fingerprints e grants versionados ainda usados até o corte de permissões.
A tentativa inicial f3-service-core.log / f3-service-collect-community.log falhou
no carregamento, antes de testes. A entrada foi preservada nesta etapa, com recusa
explícita de descoberta e sem serviço operacional; não foi alterado nenhum
fingerprint, grant, deny ou budget para fazer o import passar. Retirar a entrada
junto com permissões/presets/normalização na próxima frente. O avaliador puro
historical_permission_policy_v034.py já está congelado e independente de lifecycle;
usá-lo para provar os ceilings do arquivo, sem substituir autoridade viva por ele.
Esse é um vínculo de cutover do plano único, não uma nova exceção arquitetural.

Testes: excluídas apenas classes/casos exclusivos das operações Sprint apagadas
(e o arquivo test_sprint_origin_invariants, só create/update desse serviço).
Preservados casos compartilhados de Card/Spec, cancellation, architecture, evidência,
contexto e analytics restante. O antigo teste de composição Path B/Path C agora
exercita diretamente início de Bug com Test cross-spec: cobertura confirmada passa,
linhagem sem confirmação continua coverage_pending. A sentinela de Spec context
foi movida do serviço retirado para a porta de persistência, proibindo a consulta
real a Sprint, além de verificar ausência no payload. O hash exato de
submit_spec_validation foi mantido; só a verificação exclusiva do método de
Sprint evaluation apagado foi retirada.

Evidências até aqui em PULSE_REFACTOR/.validation-v040:
- provenance-f3-service-r2.json: 811/336 .py e 874/420 payloads source/wheel/install
  idênticos; reconstrução/reinstalação após término dos processos do lote inicial.
- f3-service-core-r2.log: 162 passed (admissão, preview/REST/MCP, Path B, policy,
  context projection, mutation inventory e catálogo gerado).
- f3-service-community.log: 51 passed (REST de dependências de Spec, fence real F3,
  rejected/permissões de Card). f3-service-ui.log: 103 passed em CardModal e
  hooks/projeções de lifecycle/policy. tsc/Vite/sync passaram; a mudança só de
  tipo não alterou os 78 payloads SPA, árvore permanece
  639c408cac92294bc31b6aa0e0f3988edf5380c516dc22a9d9afeb16a3a2bf52.
- Coleta integral, não execução integral: 13.222 Core e 5.801 Community, sem
  falha de import/collection após a correção do acoplamento histórico.
- closure-f3-service.json: findings=[], todos os oito budgets 0/0; somente
  matrizes README precisam do renderer oficial (Core imports 7.497 -> 7.482,
  Community 1.175, dependências 25). Não afrouxar o gate de documentação.
- f3-service-regressions.log ainda em execução; finalizar o lote e a distribuição
  pareada antes do commit/push. Sem nova execução Playwright/E2E integral.

Retomada: além de fechar estas provas, retirar os consumidores restantes de Sprint
(analytics/filtros, QA/contextos/histórico operacional, DTOs/catalog/repos/KG),
coordenar permissões/registry e captura histórica no corte offline, então schema e
certificado terminal. A lista integral anterior F2/F3/F4/F5, DEI/ARQ/VER/ADV,
footprint, upgrade/rollback, benchmark e rollout permanece ativa. Nenhum release,
merge, tag, deploy, migração de dados reais ou restart do runtime do usuário.

Fechamento das verificações do incremento:
- f3-service-regressions.log: 107 passed nos oito arquivos compartilhados
  alterados (architecture, paginação MCP, cancellation, hardening, Evidence V2,
  reabertura/origem, analytics legado e autoridade/escopo). Soma sem duplicatas:
  269 Core + 51 Community + 103 frontend = 423 testes aprovados.
- Ruff e diff --check passaram nos dois repos. verify:frontend-dist confirmou
  os 78 payloads. Catálogo MCP preservado byte a byte pelo teste de drift.
- Renderer oficial atualizou somente a contagem Core da matriz README para
  7.482. Ambos wheels finais reconstruídos/reinstalados depois de encerrar todas
  as suites. provenance-f3-service-final.json prova novamente as árvores completas
  source/wheel/install; os agregados são iguais aos do par r2 testado.
  Core 6e608e0af71a021bcc179faf9faae47450d494ac906d2b9cecb96cca413c2b30;
  Community a21882b434e19d16bb71e9d833db67f236cace860ba46006789fe36de0f9c71b.
  SHA256 wheels: Core b78b19a5537f537f19bd2001fdf0ff2481c3e8b271ba3ae2649aaf67c1d3e482;
  Community 99dc504eec6898d2fe196c122fe5a1909a03825fc549811b4787649b7dc7601e.
- Uma tentativa PowerShell de imprimir a comparação usou pipeline inválido
  após foreach; falhou no parse e não executou verificação/mutação. Corrigida
  com captura em array, a prova e comparação efetivamente executaram e passaram.
- Aguardando somente closure-f3-service-final.json e publicação pareada. Não
  declarar a iniciativa completa: o registry histórico ainda depende do corte
  de permissões; Sprint QA/analytics/persistência e as demais frentes do plano
  continuam explicitamente no inventário de retomada.

Resultado final: closure-f3-service-final.json concluído com ok=true,
findings=[], documentation_findings=[] e oito budgets em 0/0. O relatório
persistido é a evidência terminal; o handle de execução já estava encerrado
na retomada da observação. Community commit
67c1d58ed902954550b36d94b07933bed6f6cbfc. Preparar commit Core e publicar o par
por push normal; verificar HEAD remoto e árvore limpa nos dois repositórios.
Nenhuma alteração de produto após a prova de identidade final.

### 2026-09-21 — F3: retirar writers de Q&A de Sprint

Par anterior publicado por push normal e verificado com ls-remote igual a HEAD,
árvores limpas: Core c2ee6cbac015111b596add912e714a0e6aa76d37 / Community
67c1d58ed902954550b36d94b07933bed6f6cbfc. Iniciativa integral segue ativa.

Rastreamento de consumidores confirmou que McpAskQuestionUseCase já recusa
Sprint antes de resolver serviços, mas SprintQAService ainda existia no catálogo
e no Protocol. Retirados o serviço, a propriedade sprint_qa, alias de record
exclusivo e DTOs de escrita SprintQACreate/SprintQAAnswer (incluindo exports).
Não há novo caminho de escrita ou concessão de autoridade. DTO de resposta
Sprint e modelos históricos ainda aguardam a retirada coordenada dos demais
consumidores; captura offline e reader por seção não dependem desse serviço.

Reprodução instalada antes de editar: provenance-f3-qa-before.json confirma
811/336 arquivos Python e 874/420 payloads idênticos ao par publicado.
f3-qa-baseline.log: 3 passed / 1 failed / 5 deselected. O único caso falho
exigia lookup/guard/writer para Sprint no teste de colaboração, embora o
use case já retornasse unsupported_target_type sem tocar serviços. Corrigida
essa expectativa aposentada: os três tipos ativos preservam os checks de
escopo/estado/permissão; Sprint agora prova recusa sem lookup, writer ou commit,
com nenhum grant, grant qa:create e wildcard. Não restaurar o caminho retirado
para satisfazer o teste antigo. O status real é o resumo pytest, não a linha
de teardown do logger que equivocadamente imprimiu PASSED no caso falho.

Testes de self-answering continuam cobrindo os quatro serviços ativos e wrappers
REST/MCP. Retirados somente fixture/caso exclusivos do serviço Sprint; a prova
conjunta ainda verifica cada handler restante. Provas de arquivo histórico vão
exercitar conteúdo/QA/avaliações/history, ausência das tabelas vivas, isolamento
de Board/origem/seção e revogação. Nenhuma alteração de frontend neste incremento:
entrada e UI de Sprint já haviam sido retiradas, não há contrato de tela novo.

Ruff passou; par wheels-f3-qa construído e instalado. Antes de testes, nova
reconstrução/reinstalação apenas para remover linhas vazias de declaração, sem
processo de comportamento vivo; provenance-f3-qa.json será a prova efetiva.
Próximo: executar regressões de colaboração/arquivo, drift do catálogo e closure,
atualizar matrizes pelo renderer oficial se necessário, publicar o incremento.
Pendências integrais F2/F3/F4/F5 e complementos permanecem as registradas acima.

Fechamento deste segundo incremento:
- f3-qa-core.log: 597 passed; f3-qa-community.log: 42 passed. Total deste lote:
  639, sem somar aos 423 anteriores como se fossem todos testes distintos.
  Catálogo MCP sem drift. Coleta Core completa: 13.223 testes, sem erro de import;
  coleta não é execução integral. Não houve novo Playwright/E2E integral.
- closure-f3-qa.json: findings=[] e oito budgets 0/0; apenas duas matrizes README
  desatualizadas. Renderer oficial aplicado: Core imports 7.482 -> 7.481,
  Community 1.175 e dependências 25 mantidos.
- Todos os processos de teste/coleta encerrados antes de reconstruir e instalar
  wheels-f3-qa-final. provenance-f3-qa-final.json confirma 811/336 Python e
  874/420 payloads integralmente idênticos entre source/wheel/install. Agregados
  efetivamente comparados com o par testado e iguais:
  Core b67bd71ab63fa152220c6912e9c8cbfd1c7d295c78a002c5e664306dd4cb1f8d;
  Community a21882b434e19d16bb71e9d833db67f236cace860ba46006789fe36de0f9c71b.
  Wheels SHA256 Core 567ca4398857a1c79b2badd197fcddf65b974628bb39cd5ed9a23cf261932fa7;
  Community c0039a63ac76a800c674eb3f3844f2881be2cacee996ce964f5db97d77a1cdae.
  A primeira impressão de comparação tentou a chave inexistente source_tree_sha256
  e mostrou null; não é prova de equivalência. A comparação válida foi repetida
  com StrictMode, aggregate_sha256 existente e exigência de valor não vazio.
- Auditoria final closure-f3-qa-final.json em execução; aguardar resultado antes
  da publicação. Nenhuma mudança de produto após a prova final de identidade.

Próxima dependência crítica de F3: permissões/registry devem sair juntas. Além
das folhas vivas Sprint, revisar manifestações históricas, reconhecimento de
snapshot Full Control, grants de presets, aliases de cancelamento e reconciliação.
A porta permission_retirement já captura o vetor original de 599 decisões e
owner_review_required/review_reason; o gate compara todas as decisões sobreviventes.
Não reduzir o vetor nem usar remoção de folhas como prova de equivalência. Usar
as fixtures originais, negações explícitas, documentos parciais/malformados,
ancestralidade de preset e overrides de Board. A captura/classificação histórica
é independente da policy viva; a prova de paridade deve continuar até o adapter.
Depois do corte, ainda restam analytics/compromisso, modelos/DTOs, persistência,
offline/schema/certificado terminal e demais critérios integrais do pacote.

Auditoria final concluída: closure-f3-qa-final.json ok=true, findings=[],
documentation_findings=[], oito budgets 0/0. Ruff e diff --check aprovados.
Community commit f67e77e5c7e652bad57f0f5c37eb040137933ac2 atualiza somente a
matriz de distribuição; produto Community permanece igual ao par anterior.
Preparar commit Core e push normal nos dois repos, verificando igualdade remota.
Este turno produziu dois incrementos com código, provas e publicação; não é
turno bloqueado. Nenhum dado real migrado nem runtime do usuário reiniciado.

### 2026-09-21 — F3: corte coordenado do registry e permissões Sprint (em revisão)

Partida limpa confirmada: Core ea8b62a83be565b8a66b5fdd40c5f86021b9587b /
Community f67e77e5c7e652bad57f0f5c37eb040137933ac2. Turno anterior foi progresso,
com dois incrementos publicados e provas; objetivo integral permanece ativo.

Antes de alterar produto, provenance-f3-permission-before.json comprovou o par
instalado wheels-f3-qa-final byte a byte. Captura pela porta pública em
tests/fixtures/sprint_permission_retirement_baseline.json guarda registry/presets
anteriores e 336 contextos, cada um com 552 decisões sobreviventes + revisão/motivo.
População determinística: identidade confiável, snapshot atual e original v0.3.4,
sete presets anteriores, cada uma das 33 folhas Sprint ausente/False/integer nas
camadas agente/preset/Board, estruturas malformadas/extensões e presets derivados
com negações. O fixture registra o commit de origem; não regenerar contra o código
novo para fazer a comparação passar.

Implementação ainda não publicada: Sprint removido de SDLC_REGISTRY e do registry
de permissões vivo (585 -> 552 folhas); nenhuma nova instalação recebe Sprint
Manager. Presets remanescentes e mapa legacy deixam de conceder operações Sprint.
PermissionSet e wrappers recusam a namespace mesmo com None confiável/documento
antigo permissivo. Operações dos demais tipos mantêm suas regras.

Normalização separa classificação histórica de autoridade viva: shape Sprint
congelado e gerações originais são usados somente quando o documento ainda traz
Sprint. Manifests públicos projetam folhas sobreviventes. Valores False, gerações
parciais e extensões não se tornam o sentinel Full Control por apagar dados.
Shapes inválidos continuam exigindo revisão. O avaliador original v0.3.4 e seus
599 fatos não foram alterados; captura/paridade offline continuam pela porta.
Comentários de depreciação delimitam a retenção até o corte offline fiel.

Frontend: hook nega Sprint inclusive com cache/resposta antiga; editor não oferece
controles Sprint; labels retirados. f3-permissions-ui.log: 145 passed em 11 arquivos
(hooks, editor, camadas, presets, labels e diff). Build tsc/Vite/sync em andamento.

Evidências intermediárias:
- Primeiro lote de paridade: 336 passed / 1 error. Erro no logger Windows por
  nome de caso com dois-pontos, antes da execução daquele caso. IDs substituídos
  por policy-NNN, mantendo as mesmas entradas; não é falha de autoridade.
- f3-permissions-regressions.log: 435 passed / 46 failed. Os 336 contextos de
  paridade e a ausência operacional passaram. Falhas foram inventário aposentado
  ainda limitado às 14 operações KG, expectativas de operações/preset Sprint e
  uma fixture pre-registry fabricada a partir do registry já reduzido. A fixture
  histórica agora parte do evaluator congelado; lista de retirement inclui as
  33 folhas Sprint originais, sem afrouxar validate_permission_retirement_registry.
- Testes exclusivos de grants Sprint aposentados; provas de presets ativos e
  compartilhadas preservadas. Nova comparação confronta todos os grants dos seis
  builtins restantes com os snapshots capturados. Paridade não é inferida da
  simples ausência de erro/import ou da redução de contagem.
- Scripts de edição tiveram duas falhas antes de escrever permissions.py: caminho
  de fixture errado e seletor AST ambíguo. Registry já retirado no segundo caso;
  retomada explícita, sem reaplicar a remoção. Leitura sem encoding num script
  produziu mojibake em comentários/textos; reparada antes da próxima instalação,
  com UTF-8 explícito e inspeção do diff. Não reaproveitar esses scripts one-shot.

Retomada imediata: finalizar frontend, reconstruir/reinstalar par e provar bytes;
rodar paridade/regressões corrigidas, testes Community de checkpoint/review/cleanup,
histórico, manifests/contratos e coleta; resolver qualquer divergência efetiva
antes de publicar. Closure e matrizes README, catálogo gerado, validação da SPA e
push pareado seguem obrigatórios. Ainda não há prova de corte offline/schema
terminal nem conclusão F3/F2 ou dos complementos integrais.

Verificações concluídas sobre o código de backend r2:
- provenance-f3-permissions-r2.json confirma source/wheel/install (811/336 Python,
  874/420 payloads). Reinstalação após encerrar os processos anteriores. Refinos
  de comentários/formatação ocorreram antes desta geração efetivamente testada.
- f3-permissions-core-r2.log: 806 passed. f3-permissions-manifests.log: 57 passed
  em cinco arquivos adicionais (SK-A, SK-B, namespaces, registry e serviço).
  Os 336 contextos de paridade e os seis builtins restantes passaram; todos os
  552 bits sobreviventes, owner_review_required e review_reason são comparados.
- Community: f3-permissions-community-r2.log teve 60 passed e uma contagem de
  removed_entries ainda em 18; agora são 51 (incluem 33 Sprint). Correção pontual,
  reexecução f3-permissions-cleanup-recheck.log: 1 passed. As demais asserts desse
  caso provaram bytes before/after, autoridade, motivos de revisão e replay.
- Lote adicional de adapters: inicialmente 4 failed/18 passed por fixtures que
  ainda removiam Sprint do registry já sem Sprint, esperavam sete seeds e
  sprint.tasks.assign na projeção REST. Após corrigir essas expectativas, 21 passed
  e uma segunda contagem de auditoria [0,7] falhou; [0,6] reexecutado em
  f3-permissions-reconcile-recheck.log: 1 passed. Total Community distinto: 83.
  O arquivo histórico continua usando a policy congelada com registry vivo
  removido e gateway vivo explicitamente proibido na captura/primeira instalação.
- UI revisada também nos contadores e diff de permissões: folhas Sprint antigas
  não geram concessão, contador ou controle; toggle em lote não reescreve a origem
  antiga. f3-permissions-ui-final.log: 145 passed/1 failed apenas no seletor do
  novo teste: havia três contadores iguais (resumo/base/efetivo). Seletor corrigido
  após inspeção do DOM; f3-permissions-diff-recheck-final.log: 3 passed. Total UI
  distinto: 146. Soma do incremento: 863 Core + 83 Community + 146 UI = 1.092.
- Coletas completas sem falha de import: 13.540 Core / 5.801 Community. Isto não
  é execução integral das suites. Catálogo MCP sem drift; não editado à mão.
- closure-f3-permissions.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0. Matriz permaneceu 7.481 imports Core / 1.175 Community / 25 deps;
  não houve necessidade de reescrever os READMEs.

Build final da SPA em execução após encerrar os testes. Reconstruir o par final,
verificar source/wheel/install e igualdade dos payloads Python com r2; a SPA
mudou pelos contadores/diff já testados. Concluir closure final antes de publicar.
Não houve Playwright/E2E integral novo, migração real, release, tag ou restart.
O avanço remove registry/autoridade operacional; analytics/compromisso, DTOs,
persistência, corte offline/schema/certificado e complementos continuam pendentes.

Distribuição final preparada:
- tsc/Vite/sync e verify:frontend-dist aprovados para 78 arquivos; SPA final
  b3c2691fdf2bd7d33b4b457d4e2214785793e09eddd5c0b07c14fbecc6fcb9a2.
- provenance-f3-permissions-final.json: ambos source/wheel/install idênticos.
  f3-permissions-final-python-parity.json compara todos os 811/336 payloads Python
  dos wheels finais com os wheels r2 testados, sem divergência. Somente a SPA foi
  regenerada após os testes Python, a partir da UI já verificada.
- Agregados: Core ded76bab84e11f6dadbe7c4dd30573ced21b66d6f9f1983118de56e53dd15797;
  Community 133ed53dcc2c4b43351e7d8c70d468761601f9880e31c3f3eec547e98e8fd3f6.
  Wheels SHA256 Core 536a2f16d3be6085d11f7e9385a63dcd312c9e80f308f33d2b7d80fc4c14db2c;
  Community 32739b9b4501007fd7bfc83e945c7275f8e8dd62cb348b69f7c648b5b585687f.
- Ruff e diff --check passaram. A auditoria closure-f3-permissions-final.json é
  a única verificação ainda em execução antes dos commits/pushes deste incremento.

Fechamento: closure-f3-permissions-final.json concluiu com ok=true, findings=[],
documentation_findings=[] e todos os budgets 0/0. Community commit
49a79840fefcd0822b179bc4037a160cff5114a4. Commit Core e push normal pareado a seguir;
verificar HEAD local/remoto e árvores limpas antes de encerrar o checkpoint.

Retomada prioritária: remover consumidores Sprint remanescentes de analytics e
compromisso (inclusive filtros), DTOs/enums/modelos e persistência; integrar a
limpeza de permissões já provada ao coordenador offline junto do schema e do
certificado terminal. Não eliminar o journal nem admitir runtime intermediário.
Documentos/presets antigos ainda são entrada histórica classificada, nunca
autoridade operacional Sprint. Leituras de policy por Card mantêm a compatibilidade
autorizada até materialização F2B. Lista integral anterior de F2/F3/F4/F5,
DEI/ARQ/VER/ADV, footprint, E2E/upgrade/rollback, benchmark e rollout segue ativa.
Este turno produziu código, baseline executável, provas e commits; não é bloqueio.


### F5 — retirada de Sprint dos agregados compartilhados (em execução)

Base publicada e limpa: Core 0e7839d2 / Community 49a79840. A autorização
F3 de Spec Done permanece aplicada, sem mudança adicional de autoridade.
Plano-base F5.6 exige eliminar métricas Sprint sem produzir zeros artificiais.
Investigação confirmou queries/contadores independentes nos agregados de
funnel, overview REST/MCP, validações, velocidade, detalhe de Spec e Card.
Avaliação de Spec, validações, denominadores de Cards, ownership e filtros
continuam com seus cálculos próprios. Compromisso/forecast e Delivery
Intelligence têm consumidores adicionais: continuam na fila, não são
declarados retirados por este incremento.

Proveniência anterior: provenance-f5-aggregates-before.json, 811/336 Python e
874/420 payloads source/wheel/install idênticos. Captura em processo novo do
par instalado: tests/fixtures/analytics_sprint_retirement_baseline.json,
19 casos (seis readers × três janelas, mais usuário sem Board), relógio fixo,
população mista com rejeições/sucessos, Bug/Test/Normal, arquivo e Board alheio.
O teste compara integralmente os campos sobreviventes com essa baseline
pré-alteração e proíbe consultas vivas de Sprint nos readers migrados.
Captura não deve ser regenerada com a implementação nova.

Resultado do incremento F5 (2026-09-21):
- Core: removidos queries, métricas, chaves aninhadas, campo Sprint de Card e
  séries de eventos Sprint exclusivamente dos agregados compartilhados citados.
  O detalhe de Spec preserva obrigações/cenários/decisões; Card preserva as
  validações projetadas e conclusões. Nenhum cálculo sobrevivente foi redefinido.
- Community: overview visual sem KPI, ciclo ou avaliação Sprint; três painéis
  de governança restantes preservados, inclusive Spec Evaluation. Tipos dos
  consumidores e docstrings REST alinhados. Não há novo placeholder de zero.
- Baseline executável: 19 comparações integrais passaram, além de quatro provas
  de detalhe/escopo. O fake do contrato público remove a fonte Sprint e o campo
  Card.sprint_id, falhando se algum agregado ainda os consultar. Testes com
  persistência real continuam no lote REST/MCP e SQLite Community.
- f5-aggregates-core.log: 232 passed/1 failed. A falha era a expectativa antiga
  de sprints no funil (e ciclo Sprint na mesma asserção), atualizada para ausência
  explícita; f5-aggregates-core-recheck.log: 1 passed. Total Core distinto: 233.
- f5-aggregates-community.log: 47 passed, cobrindo adapters SQLite, transportes,
  contratos analíticos e hardening CSV. f5-aggregates-ui.log: 32 passed em seis
  arquivos, incluindo quatro novos casos de overview: payload novo, campos
  antigos residuais ignorados, navegação/janela de datas e falha de leitura.
  Total distinto deste incremento: 312 testes. Não foi execução integral das
  suites nem novo Playwright/E2E de upgrade.
- tsc/Vite/sync/verify aprovados. 78 arquivos SPA, agregado
  97ace487c8064bd2ef61c79375511dac4875a28a2a18108042e900f328dd724b.
- provenance-f5-aggregates.json prova antes dos testes os 811/336 Python e os
  874/420 payloads source/wheel/install idênticos. Agregados Core
  f95bd6c7f31dd1e2b2a0f6c8b387aaac9fd8eb9c0f7685bc1e0c8d19b36ad50b e Community
  0472643d1ff4a9c6eaeb682c6a41111970c7acf1f5c1917d7fa0dfd974675fbd.
  Wheels em wheels-f5-aggregates: Core SHA256
  4489d27d56601ad9450f8712d355ffe03d3762c8776283423b3b21126ae9cfb5;
  Community c3cf1969cc4bc42c5c956731464c4c0e58dca0850fad4d4ad82c20f8bf4d34ab.
  Somente testes/ledger foram ajustados após essa prova; código empacotado está
  inalterado. Processos de teste/build/closure encerrados antes dos commits.
- closure-f5-aggregates.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0. Nenhuma exceção introduzida. Catálogo MCP sem drift e sem
  edição manual. Ruff e diff --check aprovados nos arquivos alterados.

Próxima frente: retirar rotas e readers exclusivos de Sprint, delivery commitment,
forecast de Sprint e resolver de escopo; preservar contribuição de agentes e
demais métricas legítimas usadas por Delivery Intelligence. Os filtros precisam
ser retirados junto de seus fingerprints, paginação, export e UI. Persistência,
schema/certificado offline, DTOs e todos os critérios F2/F3/F4/F5, DEI/ARQ/VER/ADV,
footprint, E2E/upgrade/rollback, benchmark e rollout anteriores permanecem na fila.
Nenhuma migração real, release, tag, restart ou liberação de runtime intermediário.

Distribuição pareada: Community commit 213eeed565732f1f26cbf9fed84725b2cc887723.
Commit Core e pushes normais a seguir; confirmar árvores limpas e HEAD remoto
antes de encerrar o checkpoint. O objetivo integral permanece ativo.


### F3/F5 — retirada do forecast exclusivo de Sprint (em execução)

Retomada do par limpo/publicado Core 7ff0722c / Community 213eeed. Turno anterior
classificado como progresso: agregados migrados, 312 testes, closure zero, pushes
confirmados. Objetivo integral permanece ativo.
Investigação: ForecastReadinessQuery tem horizonte next_sprint; o adapter lê
Sprint Closed + activation baseline + Card.sprint_id. Não há previsão autônoma
de Board/Spec/Card que deva ser preservada sob o mesmo contrato. Plano-base F3.6,
F5.6 autorizam eliminar esse forecast, não renomeá-lo ou devolver zeros.
Remoção coordenada da porta/serviço/use case/catálogo, DTOs/rotas JSON e CSV,
seam e implementação Community, clientes/UI de previsão. Adapter de KG e
contribuições de agentes são independentes e mantidos. O painel de resumo de
Sprint que existia apenas junto do forecast sai do Board; navegação às métricas
de entrega remanescentes continua disponível. Leitores/tabelas de compromisso
e Delivery Intelligence Sprint ainda exigem retirada subsequente; arquivo
histórico e captura de activation baseline não são alterados neste passo.
Proveniência anterior: provenance-f5-forecast-before.json, par instalado idêntico.

Validação do incremento (2026-09-21):
- Core: removidos três módulos operacionais de forecast, exports e os métodos
  dos contratos AnalyticsOperations/RelationalApplicationAdapter e do catálogo.
  Fake SaaS segue conforme às portas remanescentes. Não foi criado estimador,
  horizonte ou entidade substituta. Os dois arquivos de testes exclusivos do
  forecast retirado foram substituídos por provas negativas de ausência.
- Community: removidos routes JSON/CSV, DTO union, adapter de evidência e seam;
  o arquivo compartilhado mantém a implementação de Board KG. Retirados client,
  componente de previsão, fetch/state/exports e tipos forecast (incluindo o tipo
  SprintForecastProjection residual). Board mantém a navegação Delivery
  Intelligence sem buscar Sprints/forecast para um painel removido.
- Testes mistos A5/A6 preservam todos os casos KG/Delivery Intelligence restantes.
  Novos negativos provam 404 sem UoW para previsão e export, independentemente
  do Board, ausência no OpenAPI/DTO/adapters e ausência de módulos/portas Core.
  UI prova que previsão não é buscada/renderizada, mantendo contribuições,
  filtros, navegação e export de métricas ainda existentes.
- f5-forecast-core.log: 306 passed; f5-forecast-community.log: 63 passed;
  f5-forecast-ui.log: 97 passed em 17 arquivos. Total: 466 testes distintos.
  Coletas completas: 13.553 Core / 5.795 Community, sem erros de import. Coleta
  não é execução integral das suites; não houve novo Playwright/E2E de upgrade.
- Primeiro tsc falhou apenas em import fireEvent ocioso no teste misto após
  remoção dos casos de previsão; corrigido. Builds r2/final e verify:frontend-dist
  aprovados. 78 arquivos SPA, agregado
  0211bcd3b9ca655ea1296c0cf9fe7644a321643c2e5e58d5f78635640cb716f9.
- Antes dos testes: provenance-f5-forecast.json provou os 808/336 Python e
  871/420 payloads source/wheel/install. Todos os processos iniciados após
  reinstalação; o runtime real do usuário não foi alterado.
- closure-f5-forecast.json: findings=[], oito budgets 0/0; somente duas
  divergências de matriz README. Regenerador oficial usado a partir do relatório:
  Core imports 7.481→7.450, Community imports 1.175→1.169, dependências 25.
  Não houve nova exceção ou relaxamento de gate.
- Após READMEs: wheels-f5-forecast-final reconstruídos e reinstalados.
  provenance-f5-forecast-final.json: source/wheel/install idênticos. A prova
  f5-forecast-final-payload-parity.json compara TODOS os 871/420 payloads finais
  com os testados (inclusive SPA), sem diferença. Só metadados externos aos
  payloads mudaram pelos READMEs; testes comportamentais não foram repetidos.
  Agregados Core c6e3ab4d4ff741ed8cd4f717646099ca58642c8d39a568f4ccb1df4d32b75db7;
  Community 91b5597b4eeb503735bef9013c21b62d2f0fff4ec2877e6e7fbef960de27f0a0.
  Wheels SHA256 Core 6523262531cd41c5816a0ca9d6a083f4f77036c0cfb168c243133c4ea70b51a6;
  Community 4e15a27c23df77f611fd771019c98f0f8a8962cf370e287058c3be4c1234027e.
- Ruff/diff --check aprovados; MCP catálogo sem drift, sem edição manual.
  closure-f5-forecast-final.json está em execução antes de commits/pushes.

Próximo passo: separar os cálculos de contribuição (autoria, revisão, amostra
mínima, visibilidade própria/operador/agregado) da população Sprint em
compute_delivery_intelligence; retirar métricas de compromisso/lane e filtros,
fingerprints/paginação/export/UI correspondentes. Só então eliminar o reader
compute_sprints_analytics, SprintScopeResolver, DeliveryCommitmentService e
baseline port/adapter (captura histórica usa tabelas brutas, não esses readers).
As demais pendências integrais continuam ativas, inclusive rotas/detalhes Sprint,
DTOs/persistência/schema/certificado offline, manutenção e complementos,
E2E/upgrade/rollback, custo, rollout e footprint. Não declarar F3/F5 completas.

Fechamento: closure-f5-forecast-final.json concluiu com ok=true, findings=[],
documentation_findings=[] e oito budgets 0/0. Todos os handles de testes,
coletas, builds, instalações e closure encerrados. Community commit
bef42ee98204800adca9c8a132b6f824181402e0. Commit Core e pushes normais pareados
a seguir; conferir igualdade HEAD/remoto e árvores limpas. O objetivo integral
permanece ativo, com o próximo trabalho indicado acima.

### F3/F5 — Delivery Intelligence sem unidade Sprint (em execução)

Retomada do par Core 3ca28415 / Community bef42eeed. Investigação ponta a ponta:
use case mantém load_accessible_board e visibilidade de operador somente ao dono;
serviço calcula autoria/revisão por Card, mas selecionava população por Sprint.
Plano F5.6 autoriza retirar essa unidade e atualizar denominadores/filtros/export.
A população remanescente reutiliza exatamente o recorte de compute_agents:
Cards não arquivados do Board, created_at >= from e < to. Não é replay histórico;
validações e estado são observados atualmente. Proveniência e UI explicitam isso.
Nenhuma mudança nas fórmulas, papéis, anonimização ou mínimos por métrica.
Antes da alteração, provenance-f5-delivery-before.json provou ambos os pacotes
idênticos ao source/wheel/install. Captura congelada de 48 cenários de contribuição
em tests/fixtures/delivery_contribution_baseline.json, fonte Core 3ca28415;
mesmos Cards/atores/visibilidade/filtros antes/depois, sem regenerar o baseline.
Contrato DI v2 elimina summary/sprints, pagina contributions; prefixo de cursor
contributions-v2 impede reinterpretar cursores Sprint antigos. Paginação continua
sendo leitura corrente, sem alegar snapshot entre requisições. CSV drena páginas
com o mesmo instante de observação interno; as_of histórico externo segue negado.
Filtros Sprint/lane rejeitados explicitamente no REST antes da projeção; estado
salvo UI ignora esses campos e preserva papel/visibilidade/período. Nenhuma nova
porta de mecanismo, exceção arquitetural ou mudança de autoridade.
Rotas/readers de analytics Sprint independentes ainda pendentes. Não declarar F5
ou objetivo integral concluídos. Validações do incremento e hashes serão anexados.

Validação e fechamento do incremento (2026-09-21):
- 186 testes Core (inclui comparação integral dos 48 cenários congelados,
  rejeição de oito formas de filtro retirado, cursor legado, paginação e gates
  MCP/analytics compartilhados); 42 Community distintos; 37 frontend em seis
  arquivos. Total: 265 testes distintos. Reexecuções seletivas não somadas.
- Fonte/wheel/install comprovados antes das execuções: 808/336 arquivos Python,
  871/420 payloads completos. Última prova: provenance-f5-delivery-final.json,
  wheels-f5-delivery-final. Core agregado
  d095530905d2333b88f01d151bc45b5e05131c489806cfe66eacad7741a03463;
  Community agregado
  4629925151c71ebf170f52089abe821c84c1f42da3aa6b26d64a79121556805a.
  Wheels SHA256 Core f0fd621176f0f6e32303186ea6cbd10336b790dd9521cafbe622b94969e3af25;
  Community c410f5f162a403fdbbd65fdd4b4da65aee08aaf1a564880cd4b3c53802658a04.
- tsc/Vite/sync e verify:frontend-dist passaram. SPA final 78 arquivos,
  b9603a25ce08bb5d4dde1718d205a28ef0c637c7a6cc37fa5eabbd3f28f37437.
  Revisão encontrou papéis que poderiam desaparecer do seletor ao paginar;
  opções canônicas agora permanecem disponíveis, com teste de frontend.
- Integração SQLite real pela UoW comprova Cards sem Sprint, inclusão no limite
  inferior e exclusão no limite superior, fora do período, arquivados e outro
  Board. Primeira execução falhou na fixture por sessão sem composição semântica;
  corrigida para CommunitySemanticSession, sem relaxar o guard de produto.
- Primeira tentativa Community apontou nome inexistente de teste; não executou
  testes e não foi contada. Seleção corrigida e suites executadas. Logs:
  f5-delivery-{core,community,ui}.log; f5-delivery-{core,community,ui}-final.log;
  f5-delivery-cohort.log. O último contém o novo caso SQLite aprovado.
- closure-f5-delivery-final.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0. Não precisou regenerar matriz README. Catálogo MCP sem drift.
  Ruff e git diff --check aprovados. Todos os processos de teste/build/install/
  closure encerrados; nenhum teste contra processo em memória anterior.
- Não houve E2E de upgrade/rollback, benchmark final, migração real, release,
  tag, restart do runtime do usuário ou alteração de dados reais neste incremento.

Próxima frente: retirar analytics Sprint independente (rotas JSON/CSV/entity
view e clientes), compute_sprint_analytics/compute_sprints_analytics/_sprint_detail,
serviço de compromisso e resolver, respeitando usos compartilhados de currentness.
Depois completar DTOs/persistência e fechamento offline atômico já descritos.
Todas as pendências F2/F3/F4/F5, complementos DEI/ARQ/VER/ADV, footprint, E2E,
upgrade/rollback, benchmark e rollout continuam ativas. Este incremento não
conclui F5 nem o objetivo integral.

Distribuição pareada: Community commit 559d583652e0a273178e635f94f448c9bbe50864.
Commit Core e pushes normais a seguir; verificar HEAD remoto e árvores limpas.
Objetivo integral permanece ativo; a próxima frente está definida acima.

### F3/F5 — retirada dos leitores e superfícies exclusivas de analytics Sprint

Em execução sobre Core 4fc70bba / Community 559d583 (par limpo/publicado).
Turno anterior foi progresso: DI por Card, 265 testes, closure zero, pushes.
Investigação confirmou que compute_sprints_analytics era o último consumidor
operacional do resolver de escopo e DeliveryCommitmentService; o compartilhado
scenario_has_required_evidence continua em test_scenario_lifecycle, inalterado.
Remoção coordenada: endpoints analytics/sprints e analytics/sprint/{id}, detalhe
polimórfico Sprint e CSV, use cases/exports/Protocols/catálogo, cinco leitores/
agregadores Sprint, portas de compromisso/baseline, serviço/resolver, adapter e
registro de activation baseline. O reader SQL de analytics deixa de aceitar Sprint.
UI remove renderer/client/tipos de Sprint; URL antiga usa fallback existente ao
Board e conserva o período. Testes de frontend obrigatórios incluídos.
Tabelas/ORM de baseline ainda ficam para a captura histórica bruta e para o corte
atômico F2; não há perda de história nem porta operacional substituta.
Testes exclusivos dos módulos retirados substituídos por provas de ausência.
Os dois testes mistos dos escritores estreitos de Spec foram preservados: leitura
fresca de status draft/passed/failed e links com versão sem bump; retiradas apenas
as asserções do cache Sprint que deixa de existir. Reviewer separation e os testes
de evidência/currentness compartilhados continuam exigidos. Nenhum gate relaxado.
Validação de wheels, suites, UI e closure pendente antes de commits.

Validação do incremento (2026-09-21):
- Core: f5-sprint-analytics-core.log, 208 passed. Complemento extra: 9 passed,
  incluindo dois casos já contados, duas novas provas CSV de tipo inválido/Sprint
  e cinco gates do manifesto público. Total Core distinto: 215.
- Community: f5-sprint-analytics-community.log, 81 passed, incluindo archive
  capture, leitura histórica sem origem operacional, UoW de contribuição por Card,
  REST/CSV e adapter provenance. Frontend: 98 passed em todos os 17 arquivos de
  analytics, incluindo fallback da URL Sprint ao Board e visões remanescentes.
  Total distinto: 394 testes. Nenhum teste de evidência compartilhada relaxado.
- Coletas completas: 13.596 Core / 5.808 Community, sem erro de import; coleta Core
  antecedeu os dois casos adicionais de CSV executados no log extra. Isto não é
  execução completa das suites nem novo E2E instalado de upgrade/rollback.
- tsc/Vite/sync e verify:frontend-dist passaram. SPA 78 arquivos; agregado
  95640fb1eb409412e98a7acdeea6d587b2455b4d9c186bbeba2cc50d83ee0954.
- Preflight source/wheel/install antes dos testes: 804/335 Python e 867/419
  payloads completos, idênticos. Módulos removidos ausentes também no install;
  novas provas find_spec e contratos/rotas não permitem permanência acidental.
- Primeiro closure: findings=[], oito budgets 0/0; apenas matrizes README
  divergentes. Gerador oficial usado: Core imports 7450→7418, Community 1169→1167,
  dependências 25. Manifestos de contratos públicos Core/Community deixam de
  anunciar o serviço retirado; seus gates passaram.
- Após READMEs, wheels-f5-sprint-analytics-final reconstruídos e reinstalados.
  provenance-f5-sprint-analytics-final.json confirma source/wheel/install. A prova
  f5-sprint-analytics-final-payload-parity.json compara todos os 867/419 payloads
  finais aos testados, inclusive SPA, sem diferença. Só metadados README externos
  ao payload mudaram; comportamento não foi repetido sem necessidade.
  Core agregado 707fae50cea66d3cc3e4d7a10652d13e381ff12b1bb136abaf07658bf1d6e782;
  Community 24c4403fe68b2ca04b6a1566a081529340576d9c3c1e1288f6bd41378c9b3672.
  Wheel Core 16443e9dfe2c964f020ea6fb949b015ad773dc5d2040c70e4311549dfbe2ac37;
  Community d992606732ca478d54612585cc6077f711ae5c1297dd9a6501c83525ba4695bf.
- Ruff e diff --check aprovados. Catálogo MCP sem drift (gate executado, nenhuma
  edição manual). Final closure está em execução antes de commits/pushes.

Próxima frente: retirar as superfícies Sprint polimórficas restantes, começando
por entity_pagination e kg_node_source, e seguir getters/repositórios/UoW, DTOs,
Card.sprint_id/origens, arquivo/export/descendentes. A leitura de policy Sprint em
card_crud ainda é compatibilidade F2B pré-captura, não deve ser simplesmente
apagada antes do corte coordenado de migração. Schema/ORM legados, encerramento
atômico offline e certificado runtime_ready continuam pendentes. Preservação
histórica e os demais critérios F2/F3/F4/F5, DEI/ARQ/VER/ADV, footprint,
E2E/upgrade/rollback, benchmark e rollout permanecem ativos. Nenhuma migração real,
release, tag, restart ou liberação de runtime intermediário neste incremento.

Fechamento: closure-f5-sprint-analytics-final.json terminou com ok=true,
findings=[], documentation_findings=[] e oito budgets 0/0. Todos os handles
encerrados. Community commit e748c00d4e556fcace37f5f16cb4687d5dc15966.
Commit Core e pushes normais pareados a seguir; conferir árvores limpas e
igualdade HEAD/remoto. Objetivo integral continua ativo, não concluído.

### F3/F5 — superfícies polimórficas, DTOs Sprint e paginação de Cards

Em execução sobre Core acfa5345 / Community e748c00, par limpo/publicado.
Turno anterior foi progresso: analytics Sprint retirado, 394 testes, closure zero.
Investigação: ResolveKGNodeSourceUseCase ainda continha um destino operacional
para serviço sprints já removido. Agora referências diretas/indiretas cujo dono
é Sprint retornam unsupported com proveniência intacta, sem procurar serviço ou
abrir entidade. Board/realm e acesso a evidências indiretas mantidos. UI já tinha
fallback sem navegação; novo teste cobre a resposta unsupported do backend.
Entity pagination ainda declarava sprint_list e filtro/projeção Card.sprint_id.
Retirados a superfície, catálogo enum Sprint, campo filtrável e projeção da lista.
REST rejeita sprint_id explicitamente (inclusive vazio), sem ignorar pedido antigo.
CardPageItem deixa de emitir a chave; Cards de diferentes Sprints históricos agora
compõem a mesma população quando passam pelos filtros remanescentes. Fixture C7
mantém o registro d-sprint para provar essa inclusão, total_filtered 30→31; os
outros filtros, contagem geral, ordenação, paginação, limite SQL e privacidade
continuam exigidos, sem apagar o registro que revela a mudança autorizada.
Removidos nove DTOs exclusivos Sprint e exports SDK; testes mistos de overrides
Spec e badges Q&A continuam. O normalizador inbound de lane_type era exclusivo
Sprint: retirado junto do caller REST, preservando scenario_type e fallback 422.
As mutações/CardCreate/CardUpdate e CardResponse completos ainda exigem retirada
coordenada de vínculos/origens; a leitura interna de policy legada F2B permanece
até a captura da migração. Não alegar conclusão F3/F5 ou de toda persistência.
Validação pareada, testes Core/Community/frontend e closure pendentes.
Validação final do incremento (2026-09-21):
- Core: 123 testes distintos aprovados. O primeiro log registrou 118 passed,
  quatro falhas por offset obrigatório ausente em fixtures novas e um erro de
  setup por dois-pontos no ID parametrizado (nome de log inválido no Windows).
  Corrigidas somente essas fixtures; f5-sprint-surfaces-core-fixed.log: 54 passed,
  incluindo todos os cinco casos. Nenhum gate ou comportamento afrouxado.
- Community: 147 passed (f5-sprint-surfaces-community.log), incluindo paginação
  SQLite real, mesma população C7, contagens, privacidade e rejeição do filtro.
- Frontend: 38 passed em seis arquivos (f5-sprint-surfaces-ui.log), cobrindo
  fallback de proveniência Sprint e paginação/contagens/refresh. Total: 308
  testes distintos. Coleta integral sem erros: Core 13.606 / Community 5.810;
  coleta não equivale à execução completa nem a E2E de upgrade/rollback.
- Antes dos testes: source/wheel/install byte a byte nos dois pacotes,
  provenance-f5-sprint-surfaces.json: 804/335 Python e 867/419 payloads.
- Ruff, diff --check e gate do catálogo MCP passaram. Nenhuma alteração manual
  no catálogo. SPA produtiva inalterada, verify:frontend-dist aprovado:
  78 arquivos, 95640fb1eb409412e98a7acdeea6d587b2455b4d9c186bbeba2cc50d83ee0954.
- Primeiro closure: findings=[], budgets zero; apenas matrizes README divergiam.
  Gerador oficial atualizou Core imports 7418→7417; Community 1167, dependências25.
  Wheels finais reconstruídos/instalados após término de todos os testes.
  f5-sprint-surfaces-final-payload-parity.json confirma todos os payloads finais
  iguais aos testados; somente metadados README externos ao payload mudaram.
- provenance-f5-sprint-surfaces-final.json: fonte/wheel/install idênticos.
  Core agregado 6a6892f64071150155d134ad98769e388476e9703ccbd7a4bcae74de98972472;
  Community 9ff159672ab1d86174e13762d461c7f0df3a14bdb300c3cec09c428770dcee85.
  Wheel Core 4f3adc4f1848f5a5daf2a68d30563e35b7753535f296fa6c0fe62371f949f645;
  Community 16778b7a8354536043147e86a28e5f1a105095ea829a0330e3c6c4227da4257e.
- closure-f5-sprint-surfaces-final.json: ok=true, findings=[], documentação sem
  drift, todos os oito budgets 0/0. Observação do handle se perdeu na compactação;
  relatório final íntegro e ausência de processo confirmados, sem repetir o job.
- Community commit 31b359e; commit Core e pushes pareados a seguir.

Próxima frente: contratos completos e mutações Card.sprint_id/origens, mantendo
Spec/Board, gates críticos, Spec Done, regressão e captura F2B. Leitura interna
legada de policy exige coordenação com o corte offline. Demais pendências do
ledger (schema, certificado runtime_ready, histórico, E2E/upgrade/rollback,
benchmark, footprint e matriz integral) continuam ativas. Sem migração real,
restart de runtime, release ou tag. Objetivo integral ainda não concluído.

Complemento de verificação C7 (2026-09-21): o inventário posterior encontrou
um teste de lista exata de campos não incluído na seleção anterior. Atualizado
somente o contrato retirado: test_c7_card_page_item_schema deixa de exigir
sprint_id e verifica sua ausência na serialização; preserva todas as asserções
de enums, métricas, nulidade e privacidade Q&A. Prova pareada imediatamente antes:
provenance-f5-sprint-surfaces-c7.json, 804/335 Python e 867/419 payloads idênticos.
f5-sprint-surfaces-c7.log: 13 passed. Total distinto do incremento: 321.
Nenhum payload produtivo mudou; não houve rebuild ou reinstalação adicional.
Os commits 3ef0377fa67100a20b3c05dc92f1324d4a1b306e /
31b359eff6f6cec89654e548b2c45076a96dece1 foram publicados e HEAD=remoto
confirmado nos dois repositórios. Este complemento terá commit próprio no Core.

Investigação para retomada dos contratos completos (sem alteração produtiva):
- CardResponse usa validator AFTER com read_migrated_validation_policy(self).
  Remover sprint_id sem validar a entrada bruta antes esconderia a combinação
  inválida de override migrado e vínculo Sprint ainda ativo. Preservar a prova
  de rejeição dessa combinação ao retirar o campo da saída.
- CardCreate/CardUpdate aceitam extras por omissão: campo retirado deve causar
  erro explícito, inclusive null/vazio, e não desaparecer silenciosamente.
- main.update_card ainda verifica par Spec/Sprint e origem de hotfix; delete_card
  ainda verifica Sprint.origin_bug_id. Coordenar retirada com testes reais de
  preflight/CAS, Spec Done, cross-Board, regressão e captura F2B. Não apagar
  simplesmente a leitura de policy anterior ao corte de migração.
- Além de DTOs, restam refs em discovery_selector_catalog, context_projection,
  campos MCP e _CARD_ASSIGN_FIELDS. Frontend mantém tipos CardSummaryForSpec,
  Card e UpdateCardRequest; source_sprint_id do override é proveniência opaca.
- Testes conhecidos a revisar: test_card_relation_preflight_invariants,
  test_sprint_origin_lifecycle_guards, test_card_cross_board_hardening,
  test_card_validation_config_read, tests de discovery e CardModal.
O teste C7 está encerrado (exit 0); nenhum processo de validação desta etapa
permanece ativo. Objetivo integral continua ativo; esta é uma entrega parcial.

### F3/F5 — contratos completos de Card sem vínculo público de Sprint

Em execução sobre Core 42c0869f / Community 31b359e, ambos limpos/publicados.
Turno anterior foi progresso: superfícies/DTOs e C7, 321 testes, closure zero.
Retirados sprint_id de CardCreate/CardUpdate/CardResponse/CardSummaryForSpec e
seus tipos TypeScript. Antes de extra=ignore, requests com a chave legada falham
explicitamente (também null/vazio). Adicionadas provas REST de 422 sem alcançar
persistência e contratos OpenAPI, além dos testes Core tipados.
CardResponse valida a entrada bruta antes de perder sprint_id na projeção;
continua rejeitando override migrado com vínculo ainda ativo, escopo errado ou
contrato inválido. Proveniência source_sprint_id e deprecation F2B preservadas;
nenhuma nova capacidade para o executor. Validador after também permanece.
Fixtures de frontend agora consomem o DTO real sem o campo removido; os cenários
de policy resolvida, overrides históricos e retry continuam. Testes reais de
relações preservam preflight cross-Board e zero-write; casos de atribuição Sprint
agora esperam incompatibilidade. Sucessos de alteração/limpeza de Spec usam
fixture explicitamente pós-desvinculação; não simulam migração pelo endpoint.
Guards internos de relação/origem e leitura de policy legada ainda pendentes do
corte coordenado: esta entrega não é schema novo pronto para deploy. Nenhum dado
real alterado, nenhum runtime reiniciado. Validação pareada/UI/closure pendente.

Validação deste incremento (2026-09-21):
- f5-card-wire-core.log: 142 passed, incluindo DTOs, override migrado, preflight
  relacional real, Spec Done, autorização central e UoW MCP, catálogo sem drift.
- f5-card-wire-community.log: 38 passed e três falhas exclusivamente de fixture
  nova: POST usa require_principal, PATCH usa require_user. A fixture cobria só
  o segundo e falhava antes da validação. Corrigido override da dependência de
  POST; f5-card-wire-community-fixed.log: sete passed, incluindo os três casos.
  Community total distinto 41. Nenhum handler, gate ou autenticação alterados.
- Frontend: 74 passed em CardModal, CreateCardModal.knowledgePropagation e
  api.taskValidation; f5-card-wire-ui.log. Total distinto: 257 testes.
- tsc/Vite/sync aprovados; mudança só de tipos não mudou o payload SPA.
  verify:frontend-dist: 78 arquivos, hash
  95640fb1eb409412e98a7acdeea6d587b2455b4d9c186bbeba2cc50d83ee0954.
- Antes de qualquer teste comportamental: wheels-f5-card-wire instalados juntos;
  provenance-f5-card-wire.json confirma todos os 804/335 Python e 867/419 payloads
  iguais byte a byte entre fonte/wheel/install. Nenhuma edição de produto ou
  reinstalação ocorreu com testes ativos. Todos os handles agora encerrados.
  Core agregado 785a285c7a12a6b57a20cdeff9aab42fa2e066874a9cbd2d769d7a976a72c4d5;
  Community 9ff159672ab1d86174e13762d461c7f0df3a14bdb300c3cec09c428770dcee85.
  Wheel Core eb90a2a037ae8c4d1b6b9585848e013348e8071d851f565561b86a3311401118;
  Community 16778b7a8354536043147e86a28e5f1a105095ea829a0330e3c6c4227da4257e.
- closure-f5-card-wire.json: ok=true, findings=[], documentation_findings=[],
  oito budgets 0/0. Sem drift de matriz README. Gerador oficial do catálogo MCP
  executado: saída sem alteração. Ruff e diff --check aprovados.
- Community commit 00d44db; commit Core e pushes normais pareados a seguir.

Retomada: remover relações/origens operacionais internas (main.update_card,
main.delete_card, CardCreated, context_projection, discovery e consolidação) com
schema/migração F2 coordenados. Ainda não anunciar encerramento F3/F5: DTOs públicos
não equivalem a retirada do ORM/UoW, policy reader antigo ou certificado offline.
Preservar captura histórica e paridade F2B antes do corte; não implementar
reatribuição pela API para facilitar o upgrade. As demais pendências integrais
(schema/terminal runtime_ready, E2E instalado/upgrade/rollback, footprint,
benchmark, rollout e matriz DEI/ARQ/VER/ADV) continuam no objetivo ativo.
Não houve migração real, release, tag, merge nem reinício de runtime do usuário.

### F3 — retirar vínculos/origens Sprint das mutações de Card

Em execução sobre Core 64eb1c80 / Community 00d44db, par limpo/publicado.
Turno anterior foi progresso: DTOs públicos Card, 257 testes, closure zero.
Retirados preflight de par Spec/Sprint e dependentes Sprint.origin_bug_id em
update_card e delete_card. Mantidos Spec no mesmo Board, autorização crítica,
freeze, conteúdo de Spec Done e guards/reescrita dos vínculos de regressão Bug.
Não há escrita de desvinculação em endpoint; captura F2B continua tarefa offline.
CardCreated novo não emite sprint_id. Fixtures de migração agora introduzem
explicitamente o campo do payload histórico v0.3.4, sem depender da classe atual
para inventar eventos antigos. Classificador bruto e histórico/outbox continuam
preservando os fatos do Card e os bytes originais; novo teste separa os contratos.
Testes exclusivos do veto da lane substituídos por exclusão MCP/REST real com
sentinela contra consulta Sprint; demais negações cross-Board permanecem.
Reparentar Bug não fabrica história de Spec/Sprint nem reescreve a linha antiga.
Prova de arquivo Community cobre origem de Bug preservada byte a byte mesmo
quando DELETE relacional aciona FK SET NULL na antiga tabela de Sprint.
Schema/ORM e terminal offline ainda pendentes: nenhuma liberação de runtime
intermediário, migração real ou restart autorizado por esta alteração.
Validação pareada, regressão/arquivo/eventos e closure pendentes.

Validação do incremento (2026-09-21):
- f3-card-lineage-core.log: 244 passed, cobrindo relações reais, negações entre
  Boards, REST/MCP delete sem consulta Sprint, reparent de Bug, Spec Done,
  regressão A/B/locked, autorização central, eventos/dispatcher e catálogo MCP.
- f3-card-lineage-community.log: 94 passed (358 s). Arquivo físico imutável com
  origem preservada após FK SET NULL, context/work retirement, eventos mistos,
  sequência de captura e bloqueio de runtime com journal intermediário.
- f3-card-lineage-ui.log: 62 passed em CardModal. Total: 400 testes distintos.
  Nenhuma falha neste incremento. Não equivale às suites integrais nem ao E2E
  instalado de upgrade/rollback. Nenhum teste não Sprint relaxado.
- Proveniência antes dos testes: source/wheel/install idênticos, 804/335 Python e
  867/419 payloads (provenance-f3-card-lineage.json). Nenhum arquivo produtivo ou
  install foi alterado enquanto os testes estavam em execução.
- closure-f3-card-lineage.json aprovado: ok=true, findings=[], sem drift de docs,
  oito budgets 0/0. Ruff/diff --check e gate do catálogo MCP passaram.
- Depois do encerramento de todos os testes, events/README.md foi corrigido para
  deixar de anunciar Sprint publishers removidos e sprint_id no evento novo.
  A documentação distingue payload bruto histórico e contrato atual.
- Wheels finais reconstruídos/instalados juntos. Comparação integral em
  f3-card-lineage-final-payload-parity.json: somente events/README.md mudou;
  todo Python, recursos de runtime e SPA são idênticos ao par testado. Community
  inteira inalterada no payload; teste comportamental não repetido sem motivo.
- provenance-f3-card-lineage-final.json confirma fonte/wheel/install exatos.
  Core agregado 80bb326c13bcc1b12976e59437a83492820dd7fd42c0a61385a1b8a323fb0cdb;
  Community 9ff159672ab1d86174e13762d461c7f0df3a14bdb300c3cec09c428770dcee85.
  Wheel Core 468e72c3aed063e5b246dfff12c8e32a8397c9820d88f3363ba0b19413d51998;
  Community 16778b7a8354536043147e86a28e5f1a105095ea829a0330e3c6c4227da4257e.
- Closure final está em execução sobre esse par antes do push.

Próxima frente: discovery ainda depende de Sprints ativas em _exec_blockers e
list_cards_for_sprints; consolidação contém _sprint_to_dict/process_sprint e
_card_to_dict emite sprint_id. Retirar esses caminhos e seus adapters/payloads,
preservando blockers reais e raw audit histórico. Leituras de policy F2B, archive/
restore, ORM/UoW e corte schema/terminal offline ainda exigem coordenação.
Nenhum dado real, processo Pulse ativo, release ou tag foi alterado. Todas as
pendências integrais anteriores continuam ativas; objetivo não concluído.

Fechamento: closure-f3-card-lineage-final.json terminou (exit 0) com ok=true,
findings=[], documentation_findings=[] e oito budgets 0/0. Todos os handles
encerrados. Community commit 010f7eb; commit Core e pushes pareados a seguir.
Conferir árvores limpas e HEAD=remoto; objetivo integral permanece ativo.

### F3/F5 — discovery de bloqueios por Card, sem Sprint

Em execução sobre Core e350d9be / Community 010f7eb, par limpo/publicado.
Turno anterior foi progresso: gates/eventos de Card, 400 testes, closure zero.
Investigação mostrou um intent vivo blockers_current_sprint com leitor exclusivo
por Sprint ativa. Substituído por blocked_cards sobre Cards não arquivados do
Board, usando list_board_cards já existente; preservados estados, dependências,
on_hold, rejected, limiar stale de 72h, redação de causa e min_permission original.
O intent antigo não vira alias: catálogo de leitura o oculta mesmo em registro
antigo active=true; dispatcher o rejeita explicitamente. Bootstrap desativa a
identidade antiga sem apagá-la e cria o novo seed. Buscas salvas/histórico mantêm
os IDs originais, sem reexecutar silenciosamente uma população diferente.
Retirados DiscoverySprintFact, campo Sprint do DiscoveryCardFact, Protocol/leitores
SQL exclusivos e resolução operacional de títulos Sprint. Atividade histórica
conserva detalhes/origem, sem navegação viva para Sprint. Adapters continuam só
na Community; porta compartilhada continua no Core. Testes semânticos mantêm a
mesma população de Cards e asserções de bloqueios; novo SQL prova escopo Board e
exclusão de arquivados/estrangeiros, seed idempotente e preservação das referências.
Novo teste frontend cobre executar o intent sem parâmetro Sprint e abrir o Card.
Validação pareada, suites, bootstrap e closure pendentes. Consolidação, leitores
F2B, schema/ORM e terminal offline continuam pendentes; sem deploy intermediário.

Validação do incremento (2026-09-21):
- Core: seleção inicial 104 passed / quatro falhas, todas em fixtures/expectativas:
  adapter de teste ainda construía DiscoveryCardFact com sprint_id (dois casos),
  teste estático exigia SprintStatus.ACTIVE, novo teste de histórico esperava zero
  chamadas onde o helper envia refs=[]. Corrigido o adapter; mantidas as asserções
  de cobertura, links canônicos e separação de uncovered_scenario; nenhum gate
  afrouxado. f5-discovery-cards-core-fixed.log: 40 passed. Total Core distinto108.
- Community: seleção inicial 33 passed / uma falha: o teste de replay ainda
  esperava sete presets, embora Sprint Manager já tivesse sido retirado em etapa
  anterior. Ajustado para seis, mantendo igualdade antes/depois e replay do mesmo
  e de outro bootstrapper. f5-discovery-cards-community-fixed.log: 31 passed.
  Total Community distinto34, incluindo seed antigo inativo, identidade/histórico
  preservados, novo seed único após dois replays e escopo SQL real.
- Frontend: f5-discovery-cards-ui.log, 16 passed; abrir resultado Card, executar
  intent sem Sprint e demais parâmetros/navegação/avisos. Total distinto158.
- Revisão final retirou também SelectorCardFact.sprint_id e refs Sprint, nos dois
  adapters e na projeção Core. Após todos os testes anteriores terminarem, wheels
  finais reconstruídos/instalados; contratos de seletores retestados no install:
  f5-discovery-cards-selectors-final.log: 52 passed; adapters-final: três passed,
  incluindo Community real para opções de Card sem campo Sprint. Repetições não
  somadas ao total distinto. Nenhuma edição/reinstall com teste ativo.
- Proveniência antes das execuções: provenance-f5-discovery-cards.json e
  provenance-f5-discovery-cards-final.json, 804/335 Python e 867/419 payloads
  idênticos byte a byte entre fonte/wheel/site-packages.
  Final Core agregado 148a4660d5e8f772f0cf534e2f9ee033b137c8ac46fd825c8d86f23786e78ddb;
  Community 0547000f949e2eb0611b419190285a0ceca73f91e88afc23d7c875904481f49c.
  Wheel Core 7bda5c66a155de7ef516d1e5582ebc95438e0a7844ad56ed4d1d0def8ec5c43d;
  Community 7514e1b4e36a22f8e916c0ef13906996dde8a22cacd406b25aa255750350340f.
- Primeiro closure: findings=[], oito budgets 0/0; só matriz README divergente.
  Gerador oficial executado para ambos READMEs. Closure final em execução.
- Ruff/diff --check e gate do catálogo MCP passaram. SPA produtiva não alterada;
  verify:frontend-dist confirma 78 arquivos e
  95640fb1eb409412e98a7acdeea6d587b2455b4d9c186bbeba2cc50d83ee0954.

Retomada: consolidação ainda materializa Sprint e card.sprint_id; remover seus
handlers/workers/DTOs de projeção viva com os testes de outbox histórico e não
ressurreição. Em seguida continuar archive/restore/UoW e leituras F2B até schema/
terminal offline atômico. Demais pendências integrais de matriz, footprint,
E2E instalado/upgrade/rollback, benchmark e rollout continuam. Este incremento
não migrou dados reais, não executou release/tag/merge e não reiniciou o Pulse.

Fechamento: closure-f5-discovery-cards-final.json terminou com exit0, ok=true,
findings=[], documentation_findings=[] e oito budgets 0/0. Handles encerrados.
Community commit38a258f; commit Core e pushes pareados a seguir, com verificação
HEAD=remoto e árvores limpas. Objetivo integral continua ativo, não concluído.
