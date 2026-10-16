# Aceite — validação unificada da Spec

Fechamento de U1–U4 em 15/10/2026. Escopo e aceites do
[plano](SPEC_UNIFIED_VALIDATION_PLAN.md) preservados. O relatório
[executável de conferência](evidence-u4-final-20261015.json) registra 107
artefatos com hashes, resultados, processo e limites. As provas abaixo são
complementares; não se afirma que cada negativo foi repetido no navegador.

## Resultado de produto

Spec, Task e Test podem ser preparados em Draft, mantendo Cards Not Started.
A conclusão do planejamento reúne Spec Validation e decomposição, consulta
os demais gates nativos e só promove quando todos os componentes aplicáveis
permitem. Avaliações, dimensões, rubricas, severidades, thresholds, autores,
permissões e históricos permanecem distintos. Não existe média entre avaliações
diferentes. Contribuições de outro avaliador podem ser reutilizadas sem cópia.

Zero Decisions não obriga criar uma decisão artificial; Decision existente
continua exigindo plano válido. Início, execução e entrega preservam seus
controles. A interface aberta acompanha alterações MCP, protege rascunhos e
remove conteúdo após perda confirmada de acesso. Não há migração ou contrato
alternativo para armazenamento antigo.

## Fontes de prova

- **U1:** `u1-core.xml` (209 passes), `u1-execution-negative.xml` (2);
  regressão U4 Core (221) inclui autoria, início precoce e reassociação.
- **U2:** [mapa finito](evidence-u2-finite-case-map-20261014.json), 208 entradas
  de casos passados reconferidas por nome e hash. São evidências anteriores,
  não 208 execuções novas. Três falhas antigas de fixtures em
  `u2-checklist-parity-final.xml` foram localizadas como passadas em
  `u2-checklist-regression.xml`; o XML antigo não foi apresentado como verde.
- **U3:** [fechamento UI](evidence-u3-authority-ui-closeout-20261015.json),
  37 hashes reconferidos; 122 testes frontend distintos, tipos e build já
  qualificados. Nenhum frontend mudou depois desse fechamento.
- **U4:** recibos imutáveis em
  `PULSE_REFACTOR/.validation-v040/spec-unified-20261014/u4-e2e/`,
  runtime instalado API8240/MCP8241; seis capturas finais inspecionadas em
  `okto_labs_pulse_community/output/playwright/u4-final-r3-*.png`.

## Matriz UV01–UV24

Todos os aceites abaixo foram qualificados no escopo indicado. Fixtures de
permissão/indisponibilidade exercitam fronteiras reais, mas não representam
uma capacidade multiusuário nova na edição local.

| Aceite | Evidência que prova o comportamento | Resultado / limite |
| --- | --- | --- |
| UV01 | U1 criação normal/test em Draft; U4 Task criada pela UI, Test por MCP; `test-create`, `allocation-final-context`, `allocation-tree-*`. | Not Started, cenários e alocação nativos; sem reabrir a Spec para criar Cards. |
| UV02 | U1/U4 `test_draft_card_cannot_be_created_started`, `test_planned_card_cannot_start_until_spec_is_mature`, `test_reassigning_execution_to_draft_is_rejected_before_mutation`. | Início/reassociação prematuros negados antes de efeitos. |
| UV03 | U1 ausência de Decision; U2 cobertura Decision; `u4-final-decision-plan.xml` (15 passes); U4 Done sem Decision. | Ausência aceita; plano ausente/inválido recusado; inspeção aceita sem teste artificial. |
| UV04 | U2 `test_composition_keeps_native_results_and_promotes_only_joint_success`: decomposição ausente/reject/request_changes/score 40. | Resultado de qualidade preservado, promoção bloqueada. |
| UV05 | U2 recommendation reject; `u4-final-quality-dimensions.xml` (5 passes), uma dimensão insuficiente por caso com decomposição 100. | Confidence, clarity, assertiveness, decidability e ambiguity bloqueiam independentemente; ambos os pareceres ficam no histórico. |
| UV06 | U2 `test_decomposition_uses_only_current_approvals_and_preserves_native_average`, rejeição Current e request_changes. | Média interna/rejeição nativas, sem compensação entre avaliações. |
| UV07 | U2 famílias lint nativo (12) e Checklist (16); U4 lint aceito e Checklist Blocking executado. | Ausência, findings, off/advisory/blocking conservam efeitos próprios. |
| UV08 | U2 Checklist congelado confrontado com promoções REST/MCP. | Snapshot da edição prevalece; ausência obrigatória falha fechada. |
| UV09 | U2 policy (28), cobertura (32), Code Evidence público (36), resources nativos e queries. | Falha/indisponibilidade impede avanço com razões nativas; fixture U4 sem Guidelines é N/A, não prova de aprovação de policy. |
| UV10 | U2 combinações auto_validate/skip, Require Spec Validation e resources. | Só componentes aplicáveis exigidos; promoção direta usa o mesmo predicado. |
| UV11 | U2 MCP autenticado, contextos críticos (4), escopo e permissões compostas. | Nenhum envio de parecer sem autoridade própria; Board/ator/realm preservados. |
| UV12 | U2 consumo de parecer de outro reviewer; U3 conclusão UI e contribuição MCP posterior. | Identidades/históricos separados, sem reenvio obrigatório do parecer alheio. |
| UV13 | U2 currentness (15), reabertura e replay após nova edição. | Previous/Current por edição; revisão técnica não apaga parecer; rejeição da mesma edição permanece. |
| UV14 | U2 alterações de Card, autoridade, head e manifesto; persistência real. | Conflito sem promoção/escrita parcial. |
| UV15 | U2 replay exato, payload divergente, chamadas simultâneas, falha após flush e leitura após perda de acesso. | Recibo estável, rollback e reautorização; sem segunda promoção. |
| UV16 | U2 conclusão negativa conjunta e falha técnica REST/MCP. | Parecer negativo válido persistido, Spec Approved; erro técnico não vira avaliação. |
| UV17 | U2 `u2-dependency-parity-r9.xml`, REST/MCP. | Validação com predecessor pendente; início negado/liberado sem novo parecer. Estado Done do predecessor é entrada persistida do fixture, não execução externa simulada. |
| UV18 | U3 live sync/conclusão/progresso nativo, snapshots e testes dos consumidores. | Atualizações MCP e Delivery convergem sem Refresh, inclusive com Spec.version constante. |
| UV19 | U3 watcher, conflitos, offline/retomada e troca de principal controlado. | Respostas obsoletas descartadas, consultas deduplicadas e recuperação visível; tempos específicos são testados em componente. |
| UV20 | U3 rascunhos, autoridade MCP/HTTP e revogação visual. | Campos autorizados preservados; conflito explícito; conteúdo protegido retirado. Fixture da AuthenticationPort, não alteração do LocalAuthProvider. |
| UV21 | U3 histórico, pinpoints, teclado, loading/erro/retry; U4 screenshot final Validation. | Cinco componentes legíveis, resultados independentes e ação integrada; sem restauração do antigo quadro isolado. |
| UV22 | U2 paridade REST/MCP e transições; U3 conclusão UI; U4 MCP instalado; 168 testes Core + 17 Community de resources/catálogos. | Predicado e contratos comuns, schemas/documentação efetiva qualificados. |
| UV23 | Recibos U4 Ideação → Refinamento → Spec → Cards/Test → Done; resultados autenticados; MCP final e capturas Coverage/Delivery. | Spec/Task 100% implementação e verificação; Test somente verificação 100%; nenhuma obrigação pendente. |
| UV24 | `closeout-cognitive-commit`, `closeout-exact-*`, `closeout-pending-after-update`, `final-installed-kg-query` e screenshot KG. | Alternative canonical com origem real e dois vínculos; item consolidated, fila pendente zero. A saúde geral possui a limitação descrita abaixo. |

## Percurso instalado e distribuição

Board `9f3f3f78-140d-4344-9951-7b929648eccd`.
Spec `dcfb20fb-db6d-5628-b861-831d6a2d140d`, edição 1/versão 17, Done.
Task `5e946696-db4d-5205-a47a-9b46f1115157`, Done, 4/4 obrigações.
Test `ee0caea9-97dd-5b9a-93a1-cf87bf0961ee`, Done, 2/2 cenários.
Mock no commit `c7109a980d2d5954081e97deab6e5faaeaf1a52b`:
dois métodos unittest, sete entradas; testes executados por runners autenticados.
Implementação chegou a 100% antes da verificação; esta avançou 0 → 25 → 100
na Task e 0 → 50 → 100 no Test. Os percentuais não foram autodeclarados.

Wheels finais em `u4-final-r3-wheels`:

| Artefato | SHA256 |
| --- | --- |
| Core 0.4.0 | `fe273f66a2c8f1baf7c07a6ddaef6cfb8c814bdcd5354543e4e53897162603d0` |
| Community 0.4.0 | `7a44bf805affc7f6e6233a8ea5fc6d31783d9c5c640fa498f7be744e3c074cad` |

Paridade exata: 867 Python Core, 326 Community, 79 frontend, 58 resources Core
e 5 Community. Imports vêm da venv isolada; PID3776 iniciou depois do mtime
instalado e possui ambas as portas. Recursos MCP servidos conferidos contra
o dono efetivo, incluindo overlay Community. Catálogo e manifest regenerados
oficialmente, sem diff. Closure final `ok=true`, sem findings e oito budgets ZERO.
O closure R2 reprovado por documentação permanece como evidência anterior.

Este fechamento acrescentou 168 + 17 testes de resources, 15 de plano de Decision
e 5 de dimensões, todos passados. A correção anterior de vínculo de ator passou
em 78 + 4 testes e na chamada MCP instalada. Contagens de suítes sobrepostas
não são somadas como número de testes distintos. Frontend/tipos/build U3
reutilizados por ausência de novo diff visual; UI final foi inspecionada.

## Limites observados

Após refresh concluído, o último `kg_health` informa grafo e discovery healthy,
grafo consultável, nenhuma recuperação exigida, porém **overall_state=at_risk**.
Probes `digest_layer_parity` e `stale_canonical_parity` continuam indisponíveis:
`global_discovery_read_failed` e cache expirado/refresh agendado. Observação de
footprint também registrou timeout. O adapter limita cada escopo de observação
a 350 ms; as consultas de conteúdo e consolidação tiveram sucesso. Isso delimita
a falha de observabilidade, não prova a causa de cada probe nem saúde integral.
Não foram ampliados budgets, desativados sensores ou alterados gates para obter
verde. Correlation ID final: `b84b150b27ce4740bb171efe625d2a7e`.

UV24 exige consulta/proveniência após consolidação, comprovadas por leitura
direta e UI; não exige redesign da telemetria. A limitação fica explícita para
operação, sem transformar cache/indisponibilidade em ausência de dívida.
Não há dívida canonical aberta nem item cognitivo pendente no recibo observado.

A primeira navegação após reinstalação ocorreu antes do término da inicialização
da API e recebeu connection refused; após o log Startup complete, a navegação
e as telas finais funcionaram. Erros históricos do browser incluem reinícios
controlados e ensaios negativos; não se declara console integral sem erros.
Home e runtime padrão do usuário não foram alterados por U4.
