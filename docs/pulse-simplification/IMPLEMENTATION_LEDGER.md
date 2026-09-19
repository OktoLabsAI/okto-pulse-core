# Pulse v0.4.0 — ledger integrado de implementação

## Estado para retomada

Iniciativa **em andamento**. Etapa atual: caracterização conjunta F0/F1 + K0 +
I0 + P0, com política pura, adoção prospectiva e leitura P1 em REST/MCP/frontend:
correção F09/porta publicada, F11 caracterizado; compatibilidade F2B por Card
autorizada e depreciada. Classificação/promoção, adoção de revisão legada,
inventário e suites amplas pendentes. Nenhuma migração real autorizada.
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
