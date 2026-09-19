# Pulse v0.4.0 — ledger integrado de implementação

## Estado para retomada

Iniciativa **em andamento**. Etapa atual: implementação integrada de P1 sobre a
caracterização conjunta F0/F1 + K0 + I0 + P0, com política pura e adoção prospectiva:
correção F09/porta publicada, F11 caracterizado; compatibilidade F2B por Card
autorizada e depreciada. Preparação de IRs, contrato de classificação,
armazenamento atômico, coordenador autorizado, writers REST/MCP, revisão de
atualidade/histórico e autoria em lote na UI disponíveis, com sugestões
determinísticas e testes frontend. P2 iniciado pela autoria de perfil e vínculos
tipados nos critérios existentes, incluindo UI e integridade no writer.
Integração do gate, adoção de revisão legada, inventário e suites amplas
pendentes. Nenhuma migração real autorizada.
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
