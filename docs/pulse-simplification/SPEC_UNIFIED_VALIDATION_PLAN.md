# Validação unificada do planejamento da Spec

## Roteiro vigente para execução — conferência de 15/10/2026

Esta seção é a única fila operacional. Os roteiros de checkpoints anteriores
abaixo documentam a evolução; não reabrem trabalho concluído. Permanecem
normativos os contratos de §3–§5 e os aceites UV01–UV24 de §7.

**Base conferida e publicada:** Core `ec775fe1` /
Community `135454ca`, ambos em `feature/v0.4.0`. Nenhum diff de produto
rastreado encontrado. Capturas locais não rastreadas não integram o produto.
U1 implementado; U2 qualificado no backend; consumidores previstos de U3
integrados, incluindo Resources; qualificação integrada de U3 ainda parcial;
U4 pendente. Não recriar contratos, watcher, editores ou fluxos já entregues.

**Avanço nativo posterior à conferência:** conclusão composta UI confirmada pelo
MCP; parecer MCP sucessor reutilizou a decomposição do Owner sem duplicá-la;
histórico completo e pinpoint inspecionados, incluindo expansão por teclado.
Spec iniciou sem novo parecer. No Card aberto, prova de implementação elevou
0% → 100% automaticamente, com Spec.version 20 → 20 e verificação ainda 0%.
Ver [conclusão e entrega integradas](evidence-u3-integrated-conclusion-delivery-20261015.json).
UV18 teve avanço posterior: corrigida a divergência factual de Coverage;
cenários autenticados elevaram verificação Spec/Task 0 → 25 → 100 e Test
0 → 50 → 100, sem bump de Spec.version. Coverage e capas Kanban visíveis
acompanharam as mutações sem Refresh. Ver
[Coverage e progresso nativo](evidence-u3-coverage-native-progress-20261015.json).
Revogação MCP também foi comprovada na mesma sessão: leitura e escrita negadas,
estado preservado e agentes descartáveis desativados. Os 11 hashes dos artefatos
foram conferidos nesta revisão; trata-se de evidência anterior, não de novo ensaio.
Ver [autoridade MCP](evidence-u3-native-authority-20261015.json).
Restam autoridade/identidade **na UI** e reconciliação UV19–UV21; depois U4. Não repetir os submits
ou as mutações desse fixture para reconstruir evidência já registrada.

**Decisão de produto preservada:** Spec e Cards são preparados juntos; existe
um ponto de conclusão da validação do planejamento. Spec Validation,
decomposição, lint, Checklist, Policy Compliance e coberturas continuam
individualmente identificáveis, com os mesmos critérios, severidades,
thresholds, permissões, autores e históricos. A conclusão agrega impedimentos,
não scores. A ausência de Decision não bloqueia; uma Decision existente
continua sujeita aos seus controles. Execução e entrega mantêm os próprios gates.

| Ordem dentro dos milestones existentes | Procedimento delimitado | Evidência exigida para encerrar |
| --- | --- | --- |
| U3 — reconciliar UV18–UV21 | Relacionar cada aceite às provas já publicadas e aos artefatos locais. Separar componente, persistência, UI/MCP e instalação. Reutilizar somente provas que observem o comportamento exigido. | Matriz com caso, evidência, resultado e limite; nenhum passe inferido de mera existência de código ou de contagem global de testes. |
| U3 — atualização de progresso / UV18, comprovada | Reutilizar a evidência Coverage/progresso nativo já publicada; preservar a regressão factual sem relaxar lifecycle. | Implementação/verificação observadas na UI com Spec.version constante. Não repetir bindings, runners ou mutações para obter nova captura do mesmo caso. |
| U3 — recuperação e autoridade / UV19–UV20 | Reutilizar negativa MCP nativa e provas de rascunho/conflito/reconexão. Completar troca de identidade e revogação UI/HTTP no fixture isolado da porta de autenticação; correlacionar requisição, resposta e tela. | Respostas antigas não contaminam outro contexto; edição autorizada é preservada; revogação retira conteúdo protegido e impede escrita. Não atribuir ao LocalAuthProvider capacidade multiusuário que ele não possui. |
| U3 — conclusão e apresentação / UV21 | Reutilizar conclusão composta, contribuição independente, Previous versions e expansão por teclado já observadas. Mapear loading, erro/retry e foco às provas existentes e executar apenas os casos sem evidência suficiente. | Resultado registrado distinto de promoção; bloqueios visíveis; nenhuma avaliação desaparece ou é copiada artificialmente. Testes frontend afetados, tipos/build e inspeção visual aprovados. Ledger e commit/push de U3. |
| U4 — artefato e regressão | Executar regressões pertinentes, catálogo gerado e closure; construir frontend e wheels pareados. Instalar em ambiente/home novos, comparar arquivos Python/frontend byte a byte e iniciar processo posterior à instalação. | Commits, hashes, origem dos imports e identidade do processo registrados; oito budgets ZERO. Runtime de checkout não qualifica distribuição instalada. |
| U4 — percurso completo / UV01–UV24 | Criar nova iniciativa mock via UI/MCP: Ideação → Refinamento → Spec com Cards Not Started em Draft → conclusão unificada → execução → Tasks/Test/Spec Done → consulta/consolidação KG. | Sem reabrir só para criar Cards, Decision artificial ou repetição de avaliação ainda válida. Provas realmente executadas, percentuais aplicáveis em 100%, canonical/proveniência conferidos. Matriz final, ledger e commit/push. |

### Detalhamento da retomada — conferência documental de 15/10/2026

Esta revisão atende ao pedido de planejamento. Não executa novas mutações,
testes, instalação ou correção de produto. Fetch confirmou os dois HEADs iguais
aos remotos; os artefatos abaixo foram relidos, não produzidos novamente.
[Registro delimitado da conferência](evidence-unified-planning-reconciliation-20261015.json).
Os passos seguintes detalham exclusivamente U3/U4 e UV18–UV24 já previstos.

1. **Concluído: divergência de progresso em Coverage, dentro de UV18.**
   A execução posterior comprovou igualdade dos snapshots, corrigiu a projeção
   e qualificou UI/MCP/persistência. Os detalhes abaixo preservam a investigação;
   não representam trabalho ainda pendente.
   O registro REST `coverage-native-before.json` apresenta implementação 0/4;
   `spec-readiness-before-tests.response.json` apresenta 4/4, na mesma Spec,
   edição 2/versão 20, com verificação 0/4. Coverage rejeita inclusive o binding
   atual `card_delivery_6b543882c22b45e6bf5347e4d25e9812`.
   `services/spec_coverage_query.py` chama `evaluate_delivery_coverage`, cujo
   caminho efetivo exige lifecycle concluído por padrão; `EvaluatedDeliverySnapshot`
   separa prova factual de lifecycle. Isso identifica uma diferença concreta
   de avaliação, ainda sem excluir divergência adicional nos snapshots dos adapters.
   Comparar primeiro os fatos/escopos admitidos por ambas as leituras; reproduzir
   no teste de persistência o mesmo Card In Progress com prova atual. Corrigir a
   projeção de progresso usando a avaliação factual pública existente, sem mudar
   a elegibilidade de fechamento nem duplicar fórmula no frontend. Ampliar as
   suítes existentes de Spec Coverage/readiness e o painel afetado: prova válida
   antes de Done conta; prova obsoleta/revogada/incompleta não ganha crédito;
   Spec continua impedida de fechar enquanto houver pendências. Se os fatos
   mostrarem outro defeito, corrigir a causa reproduzida, não presumir este diagnóstico.
2. **Concluído: evolução real de UV18 e consumidores abertos.** Os dois runners
   foram executados e os resultados associados; Coverage e capas acompanharam
   progresso real com versão20 constante. A descrição seguinte registra o
   procedimento realizado, sem autorizar repetição das mutações. Após a correção,
   reiniciar somente o runtime isolado quando necessário e conferir origem/mtime.
   Recuperar o estado dos recibos, sem recriá-los. Os runners locais dos dois
   cenários estão registrados, mas os artefatos relidos não comprovam execução.
   Executar cada teste pelo caminho autenticado, associar apenas as obrigações
   que efetivamente verifica e observar progresso parcial/final na capa Kanban,
   Delivery e Coverage. Conferir REST/MCP, denominadores e Spec.version antes/depois;
   manter separadas atualização automática, prova de implementação e verificação.
3. **Fechar UV19–UV21 sem repetir os ensaios qualificados.** Mapear evidência
   existente por caso e completar somente lacunas. Para autoridade, o
   `LocalAuthProvider` resolve sempre `local-user` com controle total: trocar
   Bearer não reproduz revogação humana. Reutilizar a negativa MCP já comprovada
   e completar o fixture isolado da porta pública de autenticação para exercitar UI/HTTP
   com troca/revogação controlada. Demonstrar resposta tardia descartada, limpeza
   de conteúdo sem acesso, escrita negada e rascunho autorizado preservado.
   Identificar explicitamente prova de fixture versus comportamento da edição local;
   não alterar a autenticação do produto nem as permissões reais para obter o teste.
   Reaproveitar conclusão composta, autoria independente, histórico e teclado já
   registrados; completar loading/erro/retry e os demais negativos ainda sem prova.
4. **Encerrar U3 antes de U4.** Executar testes pertinentes aos diffs, frontend,
   tipos/build e inspeção visual dos consumidores afetados. Publicar matriz
   UV18–UV21 com resultado, arquivo/recibo e limite; ledger, commit e push nos
   repositórios alterados. Não declarar U3 completo com a divergência 0/100 aberta.
5. **Executar U4 conforme a tabela e §6.2.** Regressões, catálogo gerado, closure
   com oito budgets ZERO, frontend/wheels pareados, instalação em home nova e
   comparação byte a byte antecedem o novo percurso UI/MCP. A iniciativa nova
   inclui a BR necessária já na autoria, Cards em Draft e todos os gates reais;
   termina em Tasks/Test/Spec Done e KG canonical com proveniência conferida.
   Publicar o mapa final UV01–UV24 e os commits. Não converter a home do usuário
   nem apresentar o ensaio de checkout como validação do pacote instalado.

**Retomada concreta:** conferir saúde/origem do runtime isolado e reler o
contexto completo da Spec `38ce74e1-4d5c-4e1b-8a3b-9c8160dd40b2`, Board
`7bbd65c3-8738-45a2-8fab-a5216ab7e98c`. Há Task
`462da79f-33b8-57d5-94ad-351345d35eb8` e Test
`ba5cf224-0da8-50fb-be37-4cc572d11e2c` no registro local de autoria.
Não repetir criação nem presumir o estado atual a partir desses IDs.
Nas últimas consultas nativas a Spec está In Progress, edição 2/versão 20,
Task e Test In Progress; Coverage/readiness concordam em implementação 100% e
verificação 100%. O Test tem somente verificação aplicável, 100%. Project Structure,
aplicabilidade de recursos, lint, Checklist Blocking e conclusão foram
qualificados. Os recibos e o workspace mock estão identificados no ledger.

As capturas locais `u3-integration-offline.yml` e `u3-integration-online.yml`
registram rascunho preservado e Add desabilitado/habilitado na perda/retomada
de conexão. Foram relidas nesta conferência; não são nova execução, nem
encerram todos os casos de UV19. As provas acima demonstram conclusão,
implementação e verificação autenticada; ainda não demonstram Done ou KG canonical.

### Procedimento restante de U3 — saída verificável por caso

Esta tabela detalha somente UV19–UV21. A revisão é documental: não presume
que processos anteriores continuam vivos nem que fixtures locais já estão prontos.
Antes de executar, conferir identidade, configuração, dados e origem dos imports.
Os arquivos locais `u3_auth_runtime.py`, `u3_authority_vite.mjs` e
`u3-authority-state.json`, sob `.validation-v040/spec-unified-20261014/`,
documentam a preparação do fixture; sua existência não constitui prova positiva.

| Caso existente | Procedimento mínimo | Prova de saída |
| --- | --- | --- |
| UV19 — resposta antiga e identidade | Abrir uma Spec autorizada no fixture, manter uma leitura pendente e trocar o contexto/identidade. Liberar a resposta anterior. Reutilizar testes de componente para variações já demonstradas. | Conteúdo anterior não aparece no contexto novo; resposta obsoleta é descartada. Registrar ação, resposta e estado visual, sem alegar autenticação humana multiusuário na edição local. |
| UV20 — revogação UI/HTTP | Abrir conteúdo e formulário autorizado; revogar no fixture de autenticação e provocar a rechecagem existente. Tentar escrita pela entrada pública. | HTTP nega, UI retira conteúdo protegido e escrita não altera persistência. A prova MCP anterior complementa este caso, mas não substitui a inspeção visual. |
| UV19/20 — recuperação autorizada | Reconciliar as provas de rede e conflito já existentes. Se faltar prova para um consumidor afetado, interromper/restaurar apenas o transporte do fixture com rascunho aberto; revalidar antes de liberar escrita. | Campos autorizados preservados, base antiga sinalizada, retry sem duplicar entidade/parecer. Não apresentar simples Reload como recuperação automática. |
| UV21 — fechamento da conferência visual | Relacionar conclusão, resultados individuais, histórico integral, pinpoints, teclado/foco, pulso de loading e erro/retry às evidências existentes. Executar somente lacunas. | Cada aspecto tem artefato e limite declarado. Nenhum resultado ou permissão foi removido para simplificar a tela. |

**Registro para encerrar U3:** uma linha por aceite UV18–UV21 com superfície,
caso, commit, artefato, resultado e limite; distinguir teste de componente,
persistência, runtime de checkout e runtime instalado. Corrigir apenas falha
reproduzida nesses aceites, testar os caminhos afetados e registrar ledger,
commit/push. Evidência parcial conserva o aceite aberto.

**Dependência para U4:** fechar esse registro antes de construir o par final.
No E2E novo, incluir BR/FR/AC, cenários, critérios e Cards responsáveis já na
autoria; exercer a conclusão composta e o reaproveitamento dos pareceres,
depois execução e fechamento pelos controles próprios. O ponto unificado
decide sobre o planejamento; não aprova automaticamente Tasks, testes ou entrega.
O teste termina somente com os estados Done e a proveniência canonical do KG
observados. Ausência de uma evidência externa necessária é pendência explícita,
nunca um resultado mockado declarado como observado.

Corrigir somente defeito reproduzido que impeça estes aceites, no item
correspondente. Não adicionar milestones, migração, nova avaliação, redesign
opcional ou infraestrutura geral. Divergência material de gate/autoridade/
histórico não autorizada segue a investigação do pacote. O próximo trabalho
é executar esta fila; não realizar outra rodada de planejamento sem fato novo.

## Histórico dos checkpoints — revisão anterior de 15/10/2026

Os relatos desta seção conservam o estado e as limitações de cada checkpoint.
A fila vigente e o par atual estão no início do documento; pendências aqui
registradas podem ter sido resolvidas pelas evidências posteriores ali indicadas.

**U1 implementado; U2 qualificado no backend; U3 parcial; U4 pendente.**
Checkpoint de produto: Core `537eea6` / Community `ccc76df`, na branch
`feature/v0.4.0`. Resources está integrado e qualificado: Architecture/Mockups
recebem mudanças MCP sem perder rascunhos; Knowledge recupera leitura após
create/delete sem repetir escrita. 83 testes backend e 101 frontend distintos,
tipos/lint e closure aprovados. Provas e limites em
[Resources integrado](evidence-u3-resources-parent-20261015.json).
Retomar UV18–UV21 → U4; Resources não equivale a U3 concluído.

Knowledge e Targets têm agora prova integrada UI/MCP: rascunhos preservados,
revisão explícita da base e escrita UI confirmada por leitura MCP. Conjunto
frontend: 158 passes; últimos 41 testes de Targets e tipos aprovados. Evidências,
capturas inspecionadas e limites em
[Knowledge/Targets](evidence-u3-knowledge-targets-20261014.json).

Leitores de recursos de Card (2e.3): Architecture v1 → v2 e remoção de KB
selecionada comprovadas por UI/MCP, sem Refresh nem escrita automática.
Project Structure passou a carregar somente visível e descartar respostas
antigas; dependências comprovadas sem bump do timestamp do Card. Último
conjunto: 129 passes e tipos exit 0. Evidência e limites do runtime antigo em
[leitores de recursos](evidence-u3-resource-readers-live-20261014.json).
Autoria nativa de Project Structure e inspeção do par instalado permanecem U4.

U2 está reconciliado no [mapa finito](evidence-u2-finite-case-map-20261014.json):
208 casos selecionados em 15 grupos, incluindo provas anteriores verificadas.
Recursos: 16 passes; Code Evidence: 36; contexto crítico dos dois pareceres: 4.
Detalhes e limites em [evidência U2](evidence-u2-resources-code-20261014.json).
A auditoria de fonte registrada tem 9.036 linhas, nenhum finding/drift e oito
budgets ZERO. Esses resultados não qualificam o par instalado nem encerram U4.

Editores FR/AC, critério e qualificação têm prova nativa UI/MCP em `b0a497f4`:
rascunhos preservados, revisão explícita e escrita UI confirmada pelo MCP.
Conflito de versão agora é identificado pelo código nativo, distinto de outras
recusas 409. Corrigido o fechamento involuntário da Spec aninhada no Card.
137 testes dos editores e 92 de CardModal passaram, com tipos aprovados.
Detalhamento, screenshots e limites do runtime em
[editores UI/MCP](evidence-u3-spec-editors-live-20261014.json).

O writer de coleções BR/TR/IR/OR/Contracts/Decisions agora conserva a versão
original, espera confirmação e propaga conflitos nativos. Escrita confirmada
com falha de leitura não oferece novo create; sequência parcialmente confirmada
exige releitura e reabertura, sem retry automático. Prova UI/MCP de BR: versões
18 → 19 → 20, rascunho preservado, revisão explícita e conteúdo final confirmado.
222 casos finais distintos aprovados, tipos exit 0 e lint sem erros (28 warnings).
Detalhes em [coleções da Spec](evidence-u3-collection-editors-20261015.json).
Refresh habilitado em Business/Contracts/Technical/Observability, além de
Validation/FR/AC. Integration foi integrada em `81af8c41`: classificação
preserva rascunho, exige revisão explícita e mantém replay da base original.
141 casos finais distintos e prova UI/MCP de promoção de IR, com arquitetura
v2 → v3, contrato revisto e persistência confirmada. Ver
[classificação arquitetural](evidence-u3-classification-editors-20261015.json).
O componente de inspeção de Decisions conserva a base e o replay exato.
`DecisionsTab` agora preserva a observação durante recarga, erro recuperável e
remoção do item; separa leitura atual de cache e limpa dados em recusa de acesso.
A assinatura de Decisions está implementada em `7566db17` e qualificada por
browser/MCP: versão 8 → 9, rascunho preservado, revisão explícita e escrita UI
confirmada por leitura MCP, com histórico expandido inspecionado. O XML integrado
registra 154 passes; tipos/lint aprovados no checkpoint. 5b está encerrado nos
limites de [inspeção integrada](evidence-u3-decision-inspection-parent-20261015.json).
Evidência do componente em
[inspeção de Decision](evidence-u3-decision-inspection-component-20261015.json);
checkpoint do pai, resultado integrado e limites no ledger.
O componente de método de cenário também conserva a base: 15 testes aprovados,
tipos/lint aprovados. O pai agora aguarda confirmação de escrita, preserva o
formulário em recusa e descarta resposta de outra Spec (`3057e84e`): 43 passes
integrados e criação UI confirmada por MCP. Ver
[confirmação de cenários](evidence-u3-scenario-confirmation-20261015.json).
A base autorada de versão e a recuperação explícita de conflito foram
qualificadas em `4755e369` / `3c4df986`. A substituição da coleção agora também
confere snapshot operacional sob fence, com 102 testes backend, 48 frontend e
prova UI/MCP de mudança sem bump da versão. Ver
[snapshot operacional](evidence-u3-scenario-operational-snapshot-20261015.json).
Criação/exclusão com resposta incerta e retenção do método em cenário removido
foram qualificadas em `aeebd6df`: 54 testes, tipos/lint e prova UI/MCP 13 → 14 → 15.
Ver [reconciliação de cenários](evidence-u3-scenario-reconciliation-20261015.json).
Método, vínculo/desvínculo, skip, transições e relatórios foram integrados em
`28ec1871`; Tests agora assina o watcher existente. O conjunto final tem 166
passes, tipos/lint aprovados e prova UI/MCP de atualização automática com dois
rascunhos preservados, revisão explícita e criação única na versão 17. Ver
[Tests e writers](evidence-u3-tests-watch-20261015.json). Reconciliação de recibo
de relatório tem teste de componente; prova nativa assinada permanece no U4.
Preservar também [método de cenário](evidence-u3-scenario-method-20261015.json).

Project Structure/5d foi integrado em `5ac5ce06`: base autorada, replay exato,
identidade/acesso e recuperação da leitura completa protegidos. Watcher comprovado
por UI/MCP inclusive em vínculo de Task sem bump de Spec.version; revisão explícita
e gravação UI confirmadas na versão 20/revisão 4. 138 testes frontend passaram,
tipos/lint aprovados. Details permanece leitura, sem editores de requisitos.
Ver [Project Structure integrado](evidence-u3-project-structure-parent-20261015.json).
Contexto completo do fixture ainda recusa snapshot de Checklist ausente; autoria
instalada e percurso integral permanecem U4. A conferência local abaixo é histórica.

Coverage/References foram integrados em `be799c37`: mudança somente de Card
invalida Coverage sem bump de versão/timestamp da Spec; referências abertas
convergem sem trocar a subaba. Prova UI/MCP e 96 casos distintos aprovados,
com rechecagem isolada de um timeout, em
[Coverage e References](evidence-u3-coverage-references-20261015.json).
Prova de entrega indisponível não recebeu crédito; evolução nativa dos percentuais
permanece na qualificação integrada.

Dependencies foi integrado em `5c1d8f73`: base autorada/revisão explícita,
replay exato, confirmação separada da leitura e proteção de acesso/identidade.
145 testes frontend passaram; tipos/lint aprovados. UI/MCP comprovou inclusão
concorrente, revisão e gravação únicas, além de recuperação de leitura após
remoção sem repetir DELETE, versões 20 → 21 → 22 → 23. Ver
[Dependencies](evidence-u3-dependencies-20261015.json). Resources permanece aberto.

**Próxima ação de implementação:** reconciliar/fechar UV18–UV21 → U4.
O WIP de update autorado de Architecture foi concluído neste par: 80 casos Core
distintos, 3 de adaptador real, 26 frontend, tipos, catálogo e closure com oito
budgets ZERO. Browser/MCP comprovou rascunho preservado, revisão explícita e
recuperação de leitura após Save sem repetir PATCH, v1 → v2 → v3 → v3.
Foi usado Refresh manual do componente nesse checkpoint de update. Ver
[Architecture: update e recuperação](evidence-u3-architecture-authored-update-20261015.json).
Create/delete/import foram posteriormente qualificados em `826d5dfa` / `d58ee70d`:
120 testes backend, 34 frontend, tipos/lint e closure ZERO. UI/MCP comprovou
criação, conflito/revisão de importação e recuperação após import/delete sem
repetir escrita. Ver [operações Architecture](evidence-u3-architecture-operations-20261015.json).
Mockups/wrapper Knowledge e integração automática de Resources foram qualificados
no par vigente, conforme a evidência acima. Não reimplementar esse incremento.
A fila vigente está em “Roteiro vigente para execução”, no início. Não reimplementar
U1/U2, Project Structure ou Coverage/References já qualificados.
Histórico completo dos checkpoints: [ledger](IMPLEMENTATION_LEDGER.md).

### Histórico de planejamento — conferência sucedida por `5ac5ce06`

**Base conferida:** Core `9cb255a3` (produto `8291a648`) / Community
`28ec1871`, ambos publicados em `feature/v0.4.0`. Tests/5c já foi integrado;
não permanece na fila. O único diff de produto encontrado nesta conferência
está em `ProjectStructureTab.tsx` e `ProjectStructureTab.test.tsx`.
Ele acrescenta base autorada, recuperação da tentativa original, retenção de
rascunho e proteção de identidade/acesso. Preservá-lo e revisá-lo, sem reaplicar.
O XML local `u3-project-structure-guards.xml` registra 14 testes, zero falhas,
erros ou skips. Foi lido nesta conferência; não é nova execução nem prova de
tipos, integração do pai, browser ou instalação.

| Ordem restante | Trabalho concreto e dependência | Evidência de saída |
| --- | --- | --- |
| 5d.1 — concluir o diff local | Revisar base autorada versus tokens atuais, replay exato e resposta de outra identidade. Conferir apresentação do rascunho de nó removido inclusive com itens removidos visíveis, sem dois editores para o mesmo nó. Preservar a API transacional própria da árvore. | Testes dirigidos de refresh/conflito, resposta incerta, remoção, troca de Spec e acesso; tipos/lint pertinentes. Resultados locais existentes não encerram a integração. |
| 5d.2 — integrar Project Structure/Details | Em `SpecModal`, incluir a aba no watcher existente e fornecer revisão, atualidade e perda de acesso. Proteger `onStructureChange` por Board/Spec e revisões, sem regredir o estado do pai. Recuperar falha de leitura do workspace por releitura completa: carregar só a árvore não comprova atualidade de toda a Spec. Classificar os controles restantes de Details em leitura/escrita, sem reintroduzir requisitos por essa aba. | Testes do pai e componente; UI aberta com rascunho + mudança MCP → atualização, revisão explícita e escrita confirmada por MCP. Autoria no par instalado continua no U4. |
| 3d — leitores restantes, após proteger os editores | Conferir Coverage, recursos, referências e dependências contra o watcher já existente. Para cada leitor restante, registrar token autoritativo consumido, condição de visibilidade e descarte de resposta antiga; conectar somente o que falta. Reutilizar integrações de Cards/Delivery, SpecsPanel e Kanban. | Mudança apenas de Card/Delivery, sem bump de Spec, atualiza leitores/capas abertos; painel fechado consulta ao abrir. Sem polling pesado periódico, cálculo de gates no cliente ou novo mecanismo de sincronização. |
| UV18–UV21 — encerrar U3 | Conferir atualização externa, rede/retomada, troca de identidade, revogação, formulário sujo, resultados e histórico. Exercitar a conclusão composta com permissões distintas e inspecionar visualmente a interface. Reusar evidências que já demonstram exatamente o aceite. | Todos os consumidores previstos cobertos; avaliações separadamente legíveis; histórico, pinpoints, loading, foco/teclado e recuperação verificados. Ledger, testes/tipos/build pertinentes e commit/push de U3. |
| U4.1 — qualificar o artefato | Executar regressões afetadas, catálogo gerado e closure; construir wheels dos dois repos e frontend, instalar em ambiente/home isolados, comparar arquivos byte a byte e iniciar processo posterior à instalação. | Par exato e origem dos imports registrados; frontend empacotado conferido; oito budgets ZERO. Não confundir runtime de checkout com artefato instalado. |
| U4.2 — concluir o percurso | Nova iniciativa mock via UI/MCP: Ideação → Refinamento → Spec com Cards Not Started em Draft → conclusão unificada → execução → Cards/Test/Spec Done → consulta e consolidação KG. | UV01–UV24 ligados a evidências observadas; nenhum teste/prova/aprovação inventado; percentuais aplicáveis em 100%, KG canonical/proveniência, ledger e commits/pushes finais. |

Esta tabela conserva a decomposição de §6.2; 5d.1/5d.2 foram concluídos no
checkpoint acima. A fila vigente de 3d está em “Retomada executável de 3d”;
esta conferência permanece como histórico. Não acrescenta milestones nem
novos contratos. As avaliações e regras da tabela de §3 permanecem integrais.
A conferência é documental: não alterou produto nem executou novos testes,
instalou runtime ou qualificou o diff local. Melhorias opcionais não entram na
fila; falha que impede um aceite existente é corrigida no item correspondente.

### Leitura executiva do plano

**Resultado:** preparar Spec e Cards juntos e concluir a validação do planejamento
em um único ponto. Todas as avaliações continuam existindo, com seus critérios,
severidades, thresholds, permissões, autores e históricos próprios. A conclusão
consulta o conjunto aplicável; não substitui os resultados por uma média geral.
Contribuições de pessoas/agentes diferentes continuam possíveis antes da conclusão.

**Escopo fechado:** unificar a conclusão, retirar somente a obrigação de existir
Decision e corrigir a atualização da interface após operações MCP. Preservar os
gates de execução/entrega. Não redesenhar Ideação/Refinamento, percentuais ou KG.

| Sequência de retomada | Trabalho restante concreto | Condição para avançar |
| --- | --- | --- |
| U1 — preservar | Reutilizar autoria de Cards Not Started em Draft e Decision opcional já implementadas. | Não reimplementar; confirmar ambos no percurso U4, incluindo recusa de execução prematura. |
| U2 — preservar qualificação | Paridade de backend reconciliada no mapa finito, incluindo recursos/Code Evidence e os dois guards de contexto crítico. | Preservar provas e regressões afetadas; autenticação wire e percurso instalado permanecem U4. Não repetir famílias sem impacto novo. |
| U3 — completar qualificação | Preservar consumidores já integrados, progresso nativo, conclusão/histórico e negativa MCP. Completar autoridade/identidade na UI e reconciliar recuperação/apresentação nos aceites existentes. | Provas de UV18–UV21 identificadas por caso, incluindo limites de fixture, persistência e UI/MCP; nenhum consumidor precisa ser reimplementado por causa desta revisão. |
| U4 — qualificar entrega | Executar regressões e auditoria, construir/instalar o par exato isoladamente e tramitar uma iniciativa mock nova até Done e KG canonical. | Matriz UV01–UV24 com evidências, oito budgets ZERO, paridade do pacote instalado, ledger e commits/pushes completos. |

Cada linha usa as tarefas já discriminadas em §6.1/§6.2; não cria milestones
adicionais. O fim é U4 comprovado. Melhorias opcionais encontradas durante o
trabalho não ampliam este escopo. Divergência material de autoridade/gate/histórico
segue a investigação do pacote antes de qualquer mudança semântica não autorizada.

### Referências dos checkpoints

O ledger conserva os pares publicados, reproduções, correções e limitações.
As evidências parciais de [Validation](evidence-u3-live-validation-sync-20261014.json),
[Kanban](evidence-u3-kanban-sync-20261014.json) e
[rascunhos de Cards](evidence-u3-card-drafts-20261014.json) continuam válidas
dentro dos limites registrados; não equivalem ao aceite integral de U3/U4.

## 1. Resultado e limites acordados

Preparar a Spec, Tasks e Test Cards na mesma fase de autoria e concluir a
validação do planejamento em um único ponto. Preservar todas as avaliações,
dimensões, rubricas, escalas, severidades, thresholds, autoria e permissões.
O resultado agregado informa se o planejamento pode avançar; não substitui os
resultados individuais nem calcula uma média entre avaliações diferentes.

Este plano inclui exatamente os três atritos discutidos com o usuário:

1. Eliminar a volta Approved → Draft necessária apenas para criar Cards e
   preencher a alocação do implementation_plan; reunir Spec Validation e
   avaliação da decomposição antes da promoção para Validated.
2. Retirar a exigência de existir ao menos uma Decision ativa. Uma Decision
   existente continua sujeita ao seu plano de verificação e à aderência na entrega.
3. Sincronizar a interface aberta com alterações confirmadas pelo MCP, sem
   perder edição local nem mostrar resultados de uma edição como se fossem de outra.

Não alterar avaliação da Ideação/Refinamento, Task Validation, métodos de
verificação, evidência autenticada, percentuais, gates de entrega, permissões
de configuração do Board ou regras de publicação canonical do KG. Não criar
Decision, tarefa, teste, prova ou aprovação para satisfazer artificialmente um gate.
Não adicionar um framework geral de workflows ou uma nova hierarquia de permissões.

A v0.4.0 permanece contrato único: sem migração, backfill, conversor, alias de
compatibilidade ou leitura alternativa de resultados antigos. Histórico nativo
da própria versão continua obrigatório. Se a persistência precisar de novo
formato, atualizar o contrato de armazenamento e recusar bases incompatíveis com
diagnóstico acionável, sem conversão ou exclusão automática.

## 2. Evidência e precedência

Evidência operacional: [ensaio completo UI/MCP](evidence-live-sdlc-e2e-20261014.json)
e checkpoint de 14/10 no [ledger](IMPLEMENTATION_LEDGER.md). Spec do ensaio:
`3fbbb126-b9d6-54b3-a11c-70d05082a7cc`, Done, edição 4, versão 37.
As voltas de lifecycle incluíram também correções autorais; não atribuir todas
exclusivamente à restrição de criação de Cards.

Fontes normativas: pacote v1.3, começando por INICIAR_NO_CODEX_ENTREGA.md;
PLANO_PULSE_CODEX.md, especialmente §§2.1, 5.2–5.3, 6B–6C, 7–8 e 10;
COMPLEMENTO_KG_RASTREABILIDADE.md, §§3.2, 4 e 5.5;
COMPLEMENTO_ENTREGA_INCREMENTAL.md, §§4.5, 5.2 e 5.6–5.7;
ESPECIFICACAO_ARQUITETURA_VERIFICABILIDADE.md, §§1.3 e 4.
Localização: `D:/Projetos/Techridy/PULSE_REFACTOR/Pulse_Plano_Codex_v1_3_Arquitetura_Verificabilidade/`.

Aplicar conjuntamente o [corte de legado](CLEAN_BREAK_040_REASSESSMENT.md), o
[plano de verificação de Decisions](DECISION_VERIFICATION_PLAN.md) já implementado
e as decisões posteriores do usuário registradas na seção 1. A antecipação da
avaliação da decomposição é uma alteração deliberada do ponto do gate, autorizada
pela unificação; não uma alegação de que o código atual já faz isso.

Este assessment usou leitura do documento de entrada, conferência dirigida das
seções relacionadas, código e evidência existente. Não declara nova leitura
integral dos quatro documentos extensos nem nova execução do E2E. Antes de editar
contratos compartilhados, cumprir a leitura integral exigida pelo pacote, sem
buscar a ideação original e sem reabrir decisões já tomadas pelo usuário.

### Achados do baseline anterior à implementação

A tabela descreve o diagnóstico que originou o plano, não o estado atual de
todos os caminhos. O checkpoint de execução está na seção 6.2 e no ledger.

Os caminhos Core abaixo são relativos a `src/okto_pulse/core/`; os de frontend
ao repositório Community.

| Situação atual | Fonte | Mudança delimitada |
| --- | --- | --- |
| Card normal/Test não pode nascer em Draft; Spec só permite autoria de conteúdo em Draft. | `services/main.py`, `CardService.create_card` e edição de Spec | Permitir criação/autoria planejada em Draft, sempre Not Started; manter gates de execução. |
| Spec Validation exige Approved e pode promover imediatamente a Validated. | `services/main.py`, `submit_spec_validation`; `application/use_cases/submit_spec_validation.py` | A promoção passa a depender também da avaliação de decomposição aplicável. |
| Decomposição só aceita avaliação em Validated e bloqueia o início. | `services/main.py`, `submit_spec_evaluation`, `_enforce_spec_evaluation_gate` | Aceitar a avaliação em Approved; reaproveitar exatamente o mesmo julgamento no ponto unificado. |
| Existência de Decision é exigida em três caminhos. | `services/main.py`, `check_decision_presence`, `move_spec`, `submit_spec_validation` | Remover apenas o predicado de existência e seus consumidores/diagnósticos. |
| Decision existente exige plano próprio; inspeção pode substituir teste artificial quando adequada. | `check_decisions_coverage`; `domain/decision_verification.py` | Preservar integralmente. |
| Approved admite o ciclo de avaliação; Draft abre nova edição. | `domain/sdlc_registry.py`; `move_spec`, ação `admit_validation`; `domain/spec_evaluation.py` | Preservar estados, fechamento de conteúdo e Current/Previous. |
| SpecModal recarrega na montagem e callbacks locais; SpecsPanel depende de filtros/reloadKey. | `frontend/src/components/specs/SpecModal.tsx`, `SpecsPanel.tsx` | Incluir detecção de alteração externa e invalidar as projeções afetadas. |
| SSE encontrado é específico do KG. | `frontend/src/hooks/useKgLiveEvents.ts` | Não usá-lo como autoridade de atualidade relacional. |

## 3. Fluxo final

| Estado/ação | Comportamento pretendido |
| --- | --- |
| Draft | Editar definição, recursos, requisitos, critérios, cenários, plano, Tasks e Test Cards Not Started. Vincular cenários no cadastro do Test, sem operação redundante. Permissões de autoria continuam específicas. |
| Draft → Review → Approved | Preservar transições, autorização, critical context, content lock e admissão/snapshot do ciclo. A interface explica que Approved admite a avaliação; não declara planejamento validado. |
| Approved: workspace Validation | Reunir avaliações, resultados determinísticos e pendências da mesma edição. Registrar lint, Checklist e Policy Compliance pelos seus contratos nativos; registrar também a decomposição nesta fase. |
| Concluir validação | Uma ação UI/MCP avalia todos os gates aplicáveis, registra os pareceres fornecidos pelo ator autorizado e promove Approved → Validated somente se o conjunto permitir. |
| Validated → In Progress | Ação de início continua própria. Revalidar dependências, permissões, controles de início e resultados atuais; não pedir novo parecer se o existente continua válido. |
| Execução → Done | Preservar validações de Cards, provas, verificações, cobertura e gates de fechamento. Planejamento aprovado não concede crédito de implementação. |

Unificar a conclusão não elimina contribuições de avaliadores diferentes nem
significa uma única chamada de rede para produzir toda a análise. Há um ponto
de conclusão e promoção; cada evidência/parecer pode ser preparado pelo seu
responsável no mesmo workspace. Não executar automaticamente Review → Approved
em nome de quem só possui autoridade para avaliar.

### Preservação das avaliações e gates

| Componente | Regra preservada | Integração |
| --- | --- | --- |
| Spec Validation | Confidence, clarity, assertiveness, decidability e ambiguity; escala 0–100, justificativas, thresholds resolvidos, recomendação e pinpoints. Ambiguity mantém direção inversa. | Resultado individual e histórico próprios; nenhum score alto compensa outra dimensão abaixo do threshold. |
| Decomposição / Spec Evaluation | Breakdown completeness, granularity, dependency coherence, test coverage quality e overall_score; justificativas; approve/request_changes/reject. | Mesmo parecer, agora aceito em Approved e exibido dentro da validação integrada. |
| Gate da decomposição | Sem `auto_validate`/skip aplicável: ao menos uma aprovação, média dos overall_score das aprovações Current suficiente, nenhuma rejeição Current. | Preservar a média nativa **dentro** desta avaliação. Não criar média com Spec Validation ou qualquer outra avaliação. `request_changes` mantém o efeito atual, sem virar veto novo implicitamente. |
| Requirement lint | Relatório aceito da edição é pré-requisito quando exigido; findings permanecem advisory, com severidade nativa. | Mostrar ausência separadamente de findings. Não transformar score/count em veto novo. |
| Checklist | Binding/snapshot da edição, itens e métodos atuais; off/advisory/blocking. Snapshot obrigatório ausente continua falha fechada. | Executar/registrar via contrato nativo. Não permitir ao executor mudar policy, nem usar configuração nova do Board para reinterpretar edição congelada. |
| Policy Compliance | Guidelines efetivas, métricas, thresholds, autoridade, resultados, exceções autorizadas e história próprios. | Consumir o mesmo julgamento de transição; indisponibilidade/autoridade desconhecida nunca significa sucesso. |
| Coberturas estruturais | AC/cenário/Test, FR/BR transitiva, TR, IR, OR, Contracts, vínculos de Tasks, plano, recursos e Code Evidence conforme gates atuais. | Expor bloqueios com destino de correção; não antecipar exigência de código implementado ou teste passing. |
| Decisions | Zero Decisions não bloqueia. Cada Decision ativa deve ter plano válido; sua aderência continua verificada na entrega. | Vazio é não aplicável, não aprovação artificial e não dispensa de requisitos de negócio. |
| Arquitetura/verificabilidade | Classificação e planejamento exigidos antes do início; gates anteriores de Spec Validation preservados. | Exibir readiness da fase; não mover para a validação todos os gates de início ou fechamento indiscriminadamente. |
| Dependências de execução | Prontidão dos predecessores continua obrigatória no início/resume pertinente. | Pode haver planejamento validado aguardando predecessores. Não exigir predecessor Done para redigir/avaliar a Spec. |

Não mudar defaults, modos ou skips para facilitar o ensaio. Componentes off,
dispensados por policy existente, advisory, bloqueados e indisponíveis devem ser
distinguíveis. A classificação Problem/Opportunity, severidade e dimensões dos
pinpoints permanecem intactas. Não inferir novos vetos só da cor de um badge.

## 4. Contratos e atomicidade

### 4.1 Leitura integrada

Estender a projeção pública de ciclo de validação e readiness existente, usando
`ports/validation_cycle.py` e o read model pertinente, em vez de criar um segundo
ledger de aprovação. A resposta deve conter:

- Identidade do Board/Spec, edição, versão técnica e estado.
- Para cada componente: tipo, aplicabilidade, modo, resultado nativo, efeito no
  avanço, IDs dos resultados Current, bloqueios e próxima ação permitida.
- Manifesto técnico delimitado de revisões/heads usados na conclusão; mantê-lo
  fora do resumo humano. Fences são concorrência, não critério de obsolescência humana.
- Conclusão derivada `ready`, `blocked` ou `unavailable`, sem score agregado;
  readiness de início separado da conclusão da avaliação.
- Histórico paginado pelos resultados nativos; não duplicar corpos nem carregar
  todas as versões para desenhar o resumo.

Erro ou acesso insuficiente a um componente obrigatório resulta em unavailable,
não em ausência de bloqueios. O resumo não pode expor conteúdo que a permissão
de leitura daquele componente não autoriza.

### 4.2 Escrita: ampliar a fachada existente

Usar `SubmitSpecValidationUseCase`, REST `/specs/{id}/validation` e
`okto_pulse_submit_spec_validation` como conclusão canônica. Manter os campos
nativos de Spec Validation e acrescentar um payload **opcional e tipado** de
decomposição com as mesmas dimensões já existentes. Se esse payload não for
enviado, consumir os pareceres Current já registrados; nunca inventar aprovação.
Retornar resultado de Spec Validation, referências dos pareceres de decomposição,
resultado agregado e estado após a operação, distinguindo registro de promoção.

`submit_spec_evaluation` continua útil como registro independente por outro
avaliador. Passa a aceitar Approved além de Validated; não promove a Spec por
conta própria. Não criar permissão genérica de superavaliador. Para enviar os
dois pareceres juntos, o ator precisa tanto de `spec.validation.submit` quanto
de `spec.evaluations.submit`, além de escopo e critical context de cada ação.
Identidade/autoria são do servidor, não campos livres do payload.

Quando `require_spec_validation` estiver desligado, preservar a autorização da
transição direta existente, mas fazê-la consultar o mesmo predicado agregado
para os componentes ainda exigidos. Não exigir um parecer desativado nem deixar
o caminho direto contornar decomposição ativa. Todas as entradas para Validated
devem compartilhar esse predicado, inclusive REST, MCP e transições genéricas.

Extrair preparação, validação e append dos resultados de suas rotinas atuais,
sem chamar em sequência dois writers que efetuam commits/promovem isoladamente.
O caso de uso será responsável por uma única unidade de trabalho:

1. Autorizar leitura/escrita e cada tipo de parecer solicitado; validar schema,
   escopo e idempotency key antes de efeitos.
2. Sob fence da Spec/edição, conferir versão e heads nativos do manifesto,
   policies efetivas e referências. Rejeitar conflito sem gravar parecer parcial.
3. Preparar e selar anchors dos pareceres com as regras atuais; resolver demais
   resultados/gates na fonte autoritativa e conferir a decomposição prospectiva.
4. Persistir os resultados válidos e seus históricos. Resultado avaliativo
   negativo é fato rastreável; não é exceção de transporte a ser apagada.
5. Promover somente se todos os gates aplicáveis permitirem. Caso contrário,
   manter Approved e retornar os bloqueios. Nunca tornar Validated visível
   entre a gravação da Spec Validation e a checagem da decomposição.
6. Gravar atividade/eventos/recibo da operação na mesma transação, com os IDs
   nativos; commit único. Falha de infraestrutura reverte o conjunto.

Contrato de replay: chave por ator/Spec/operação; mesma chave e mesmo request
canônico devolvem o mesmo recibo, sem novo parecer/evento; mesma chave com outro
conteúdo conflita. Reautorizar leitura no replay e não reexecutar promoção de
recibo antigo. Reutilizar os mecanismos transacionais existentes; se faltar
capacidade pública, acrescentar porta tipada no Core e adapter no Community,
sem reach-in privado. Não adicionar um segundo histórico semântico.

Fences devem abranger também mudança de Card/plano que não incremente
`Spec.version`, heads de resultados e mudança de autoridade pertinente. Não
confiar só na versão da Spec ou no preflight do cliente. Read-your-writes e
rechecagem final de concorrência precisam de testes com persistência real.

### 4.3 Edição e correção

Preservar o modelo atual: reabrir para Draft cria nova edição e os pareceres
humanos anteriores ficam Previous, com conteúdo íntegro. Alteração de revisão
técnica, por si só, não transforma toda avaliação em Previous.

Retornar Validated → Approved continua permitindo parecer sucessor na mesma
edição sem destravar conteúdo. Uma rejeição de decomposição ativa nessa edição
continua bloqueando; nova aprovação não apaga rejeição. Corrigir conteúdo exige
reabertura autorizada. Este plano **não promete** reaproveitamento seletivo de
pareceres entre edições nem cria regra nova de invalidação por campo.

## 5. Interface e atualização após MCP

A aba Validation terá um resumo compacto da edição, conclusão geral e pendências
acionáveis. Preservar o padrão visual e navegação atuais, reunindo Spec Validation,
decomposição, Policy Compliance, lint e Checklist como seções da mesma área.
Não restaurar o antigo quadro isolado “Decomposition evaluations” retirado a
pedido do usuário. A decomposição aparece como componente do ciclo integrado,
com detalhes e histórico acessíveis.

Uma ação primária de conclusão explica quais componentes bloqueiam. Formulários
apresentam somente o que o ator pode enviar; contribuições já aceitas são
referenciadas, sem copiar scores, justificativas ou provas. Os resultados mantêm
seus círculos de score, pinpoints expansíveis e Previous versions completas.
Falhas de rede são recuperáveis; loading usa o pulso do Pulse. Não impor que a
mesma pessoa produza todos os pareceres.

Para a sincronização externa, usar polling delimitado de metadados autoritativos
dos objetos visíveis, a cada 5 segundos enquanto a página estiver visível, e
checagem imediata ao recuperar foco/conexão. Um hook compartilhado por workspace
deve deduplicar assinaturas, cancelar requests obsoletos e aplicar backoff em
falhas, sem disparar um timer por painel.

O contrato de metadados deve identificar mudanças de Spec, Cards visíveis,
avaliações, Checklist, policy, lint e heads de entrega mesmo sem bump na versão
da Spec. Reutilizar as leituras resumidas em lote do ciclo de validação; expor
metadados faltantes por porta pública e query no Community. Não usar polling de
Coverage completo, hash de todos os textos, varredura de histórico ou SSE do KG
como substituto. O mecanismo físico de consulta deve usar projeções delimitadas
e revisões persistidas; comprovar o custo por lote, sem N+1 por painel.

Quando houver alteração, invalidar apenas os consumidores correspondentes:
Spec, allowed transitions, Validation, Coverage/Delivery, Cards/References,
dependências e listas/capas visíveis. Preservar aba, filtros, scroll e paginação
quando ainda válidos; resetar cursor quando sua fonte mudou. Adiar recargas de
painéis fechados até abri-los. Resposta velha não pode substituir a mais nova.

Se houver formulário sujo, atualizar indicadores de atualidade e avisar que a
base mudou, sem sobrescrever campos. A submissão mantém o expected version/head
original e recebe conflito quando necessário; não reenviar automaticamente
sobre a versão nova. Revogação de acesso deve remover dados não autorizados,
mesmo que haja edição local. Falha de atualização mostra estado desatualizado,
sem apresentar cache como sucesso atual.

## 6. Milestones e dependências

Executar U1 → U2 → U3 → U4. Cada milestone encerra com ledger atualizado,
testes pertinentes, commit e push nos repositórios alterados. Não instalar
estados intermediários incompatíveis no runtime do usuário.

| Milestone | Entrega concreta | Arquivos/superfícies principais | Critério de saída |
| --- | --- | --- | --- |
| U1 — autoria e caracterização | Fixar testes do comportamento a preservar; permitir Task/Test Not Started em Draft; remover Decision obrigatória; sincronizar allowed transitions, readiness e documentação. | Core `services/main.py`, `domain/card_transition.py`, `services/gate_contracts.py`, use cases de Cards/transições; Community API e formulários de criação. | Spec sem Decision e com Cards/plano completos chega a Approved sem voltar a Draft; tentativa de iniciar Cards antes da maturidade é negada; Decision ativa incompleta continua bloqueando. |
| U2 — conclusão unificada | Antecipar registro de decomposição, ampliar comando de validação, predicado compartilhado, resumo de componentes, atomicidade/replay/fences; preservar histórico nativo e todos os gates. | Core `application/use_cases/submit_spec_validation.py`, `spec_crud.py`, `services/main.py`, `domain/spec_evaluation.py`, `domain/validation_cycle.py`, portas públicas; Community adapters relacionais e `api/specs.py`; Core MCP. | Um envio autorizado conclui ambos os pareceres; falha em qualquer componente exigido impede promoção em todas as entradas; paridade REST/MCP e concorrência comprovadas. |
| U3 — workspace e sincronização | Compor Validation, histórico e ação única; atualizar UI aberta após MCP com proteção de formulários; ampliar metadados delimitados quando necessário. | Community `SpecModal.tsx`, `SpecValidationPanel.tsx`, `SubmitSpecValidationModal.tsx`, `SpecValidationHistoryPanel.tsx`, `SpecsPanel.tsx`, hooks/client API e painéis consumidores. | Browser aberto acompanha mutações MCP sem Refresh manual; preserva edição não salva; avaliações continuam individualmente inspecionáveis e permissões não são ampliadas. |
| U4 — qualificação integrada | Documentação e catálogo gerado; auditoria arquitetural, build/paridade, E2E novo UI+MCP até Done e inspeção visual. | Ambos os pacotes, frontend_dist, resources/workflows MCP, testes e ledger/evidência. | Ensaio sem reabertura causada só pela criação/alocação de Cards, sem Decision artificial, sem repetição de avaliação válida no início e sem redução dos controles de entrega. |

U1 exige leitura integral do pacote, inventário finito dos callers
e testes de caracterização; não é uma nova auditoria geral do projeto. Se surgir
divergência que exija alterar autoridade ou efeito histórico além deste plano,
registrar reprodução e a decisão exata necessária segundo §10.2/10.4. Não criar
metas novas para melhorias opcionais descobertas no caminho.

### 6.1 Ordem operacional e entregas verificáveis

O detalhamento abaixo desdobra U1–U4; não adiciona features ou critérios ao escopo.
Retomar pelo checkpoint da seção 6.2, sem reaplicar U1. Não tratar alterações
existentes ou testes escritos como comprovação do ensaio integrado U4.

| Ordem | Trabalho delimitado | Evidência para encerrar |
| --- | --- | --- |
| U1.1 | Completar a caracterização da criação de Cards, transições e Decision vazia versus Decision sem plano. Conferir isolamento dos fixtures que alteram status. | Testes demonstram a restrição anterior e os controles que permanecem; UV01–UV03 mapeados. |
| U1.2 | Concluir criação em Draft no serviço, use cases e formulário. Auditar associação/reassociação a Spec para impedir entrada de Card já em execução em Draft, sem ampliar estados por conveniência. | UI e backend admitem somente autoria planejada; cenários são vinculados atomicamente; execução precoce e payload inválido não deixam efeitos. |
| U1.3 | Remover somente a obrigatoriedade de existir Decision em todos os consumidores; atualizar instruções MCP e testes frontend de seleção/tipo/status. | Decision ausente não bloqueia; Decision ativa incompleta bloqueia; instrução não manda criar ou vincular duas vezes o mesmo artefato. |
| U2.1 | Extrair cálculo puro da decomposição e preparação dos pareceres, preservando dimensões, recomendações e semântica Current/Previous. | UV04–UV06 e UV13; mesma regra usada por resumo e writers. |
| U2.2 | Definir DTOs fechados da conclusão e do manifesto, estender portas existentes e implementar persistência/recibo no Community. | Contrato de autorização, identidade, concorrência e replay testado com persistência real; nenhum mecanismo concreto no Core. |
| U2.3 | Integrar pareceres e promoção em uma UoW; encaminhar transição direta e allowed transitions ao mesmo predicado de avanço. | UV07–UV17 e UV22; sem promoção intermediária, bypass ou resultado parcial em falha técnica. |
| U2.4 | Expor leitura integrada e metadados delimitados em lote, com permissões por componente e identificadores de revisão. | Resumo distingue ready/blocked/unavailable; atualização detecta mudanças sem bump da Spec; custo por lote demonstrado. |
| U3.1 | Consumir o contrato integrado na Validation, mantendo formulários, resultados e históricos nativos e uma ação principal de conclusão. | UV21; componentes legíveis, permissões próprias, pinpoints/score/loading consistentes e nenhuma avaliação removida. |
| U3.2 | Implementar hook compartilhado de atualização externa e integrar os consumidores visíveis. | UV18–UV20; sem sobrescrever formulário sujo, sem resposta fora de ordem e sem consulta pesada periódica de Coverage/KG. |
| U4.1 | Rodar regressões pertinentes, frontend, catálogo e closure; construir o par de wheels e frontend. | Resultados registrados e oito budgets ZERO; paridade de instalação e origem de imports comprovadas. |
| U4.2 | Executar iniciativa mock nova com dados isolados, UI e MCP, até Done e consolidação. | UV23–UV24 e negativos pertinentes dos demais UV; IDs, recibos, screenshots e limitações no ledger. |

Na operação composta, distinguir explicitamente três situações: entrada inválida,
autoridade insuficiente ou conflito impedem a escrita; parecer válido negativo
é registrado e mantém Approved; pareceres positivos só promovem quando os demais
gates também permitem. Um bloqueio de pré-requisito não deve fabricar um parecer
negativo. O retorno diferencia pareceres efetivamente registrados de componentes
apenas consultados. Os testes precisam verificar persistência e estado, além do
status HTTP ou da mensagem MCP.

O checkpoint de cada milestone identifica o par exato Core/Community, os UV
demonstrados e os ainda pendentes. Nenhum UV é concluído somente porque uma suíte
genérica passou. U4 usa uma home descartável nova; não apagar, converter ou
reabrir as iniciativas existentes do usuário para acomodar alterações de schema.

### 6.2 Planejamento detalhado sobre o checkpoint atual

**Checkpoint de produto vigente:** Core `ec775fe1` / Community `135454ca`.
O WIP Resources descrito historicamente abaixo foi concluído no par anterior
`537eea6` / `ccc76df`; o checkpoint atual inclui Coverage factual qualificada.
5c foi integrado e qualificado após os checkpoints históricos abaixo: Tests
assina atualizações externas, preserva cenários/métodos/relatórios e possui
recuperação protegida de escrita/leitura. Evidência `evidence-u3-tests-watch-20261015.json`.
5d integrado e qualificado conforme evidência Project Structure acima.
Coverage/References, Dependencies e Resources integrados; retomar
UV18–UV21 → U4; as referências a Tests fora do watcher
nos relatos anteriores descrevem os respectivos checkpoints, não o estado vigente.
Branch `feature/v0.4.0`, incremento de 15/10/2026. A revisão documental
a partir deste par acompanha a execução parcial de 3d. O checkpoint inclui
a prova UI/MCP de 5b, a confirmação de cenários e a proteção da versão autorada
com recuperação explícita de conflito e snapshot operacional da coleção.
Testes, tipos, lint e prova nativa de
cada incremento estão nas respectivas evidências e no ledger, sem contagem
dupla. A matriz integral U3/U4 continua aberta.
Knowledge/Targets têm prova integrada; leitores têm testes aprovados e provas
UI/MCP dirigidas de Architecture e KB. Os limites da projeção Project Structure
e do runtime anterior permanecem explícitos para U4. Pares anteriores são
históricos, conservados no ledger.
A qualificação U2 já está encerrada no backend. As reproduções e a sequência
U2 abaixo são histórico de execução, não uma fila a reabrir.

O reach-in privado anterior foi corrigido e a closure de fonte voltou a ZERO.
Não repetir essa correção nem reconstruir watcher, metadados ou pareceres já
implementados. U3 completo e U4 instalado permanecem pendentes.

A conferência atual encontrou as correções dos dois impedimentos anteriores:
o teste REST separa a verificação de leitura da escrita negada e a formatação
usa expressão regular compatível com o target. O artefato
`u3-opinions-core.xml` registra 48 passes. Não reaplicar essas correções.

O diagnóstico de Checklist não configurado foi qualificado após este checkpoint:
o fixture passou a informar realm nativo e a reprodução revelou import ausente
de GateContractError, corrigido. `u3-checklist-native.xml` registra 20 passes,
incluindo recusa acionável REST e rollback integral; browser confirmou 409 sem
avanço. O refresh da Validation preservou o rascunho do Checklist no browser;
40 testes frontend passaram. Não criar binding automático nem dispensar gate.
Integração automática e reautorização já têm implementação/prova parcial na
Validation; a admissão sem Checklist também foi comprovada no MCP em UoW real.
A expansão aos demais consumidores segue pendente. A paridade de backend das
outras famílias foi qualificada posteriormente e está no mapa finito U2.
`closure-u3-interface.json` registra oito budgets ZERO e nenhum finding/drift.
Essas evidências não encerram o milestone nem substituem o E2E.

| Frente | Já existe | Falta comprovar ou implementar |
| --- | --- | --- |
| U1 | Cards Not Started em Draft e remoção da exigência de existir Decision. | Percurso real UI/MCP no U4; não refazer a implementação. |
| U2 — conclusão | Payload composto, permissões independentes, pareceres/históricos nativos, promoção condicionada, recibo/replay e manifesto obrigatório. | Preservar a matriz U2 já qualificada; comprovar autenticação wire e percurso instalado em U4. |
| U2 — leitura | `include_planning`, 17 componentes, composição pública sem escrita, revisões leves em lote, permissões por componente, paginação REST/MCP e cliente tipado. Revisões de coleções Spec/Card por Board; 11 passes do contrato, 79 de manifesto/leitura/transporte. Admissão sem Checklist comprovada por REST/browser e rollback nativo. | Consumir os contratos existentes em U3 e comprovar o percurso UI/MCP instalado em U4; não recriar metadados de coleção. |
| U3 | Resumo, histórico Current/Previous, contribuição independente, payload opcional, registro/promoção distintos e revisão explícita da base. Watcher e consumidores previstos integrados. Conclusão composta, progresso nativo sem bump da Spec e negativa MCP têm evidências publicadas/reconciliadas. | Completar autoridade/identidade UI e reconciliar UV19–UV21 conforme a fila vigente. Autoria instalada de Project Structure permanece U4. |
| U4 | Regressões parciais registradas no ledger. | Wheels, runtime compatível, browser e iniciativa nova até Done/KG; matriz UV01–UV24. |

#### Histórico de retomada de 3d — sucedido pela qualificação Resources

**Base documental atual:** Core `c0167eb6` (produto `826d5dfa`) / Community
`d58ee70d`, ambos confirmados no remoto `feature/v0.4.0` nesta conferência.
A sequência abaixo conserva o planejamento daquele checkpoint; suas linhas
1–3 foram concluídas. A fila atual está no início deste documento, sem novo
milestone ou mudança nos critérios de §7.

O WIP de Dependencies identificado na conferência foi concluído e publicado em
`5c1d8f73`, com 145 testes finais e prova UI/MCP. O XML inicial de 12 casos/uma
falha permanece como histórico: sua expectativa de rebase automático foi
substituída por revisão explícita, preservando asserções de concorrência e
replay exato. Retomar pela linha 2 abaixo; a linha 1 registra o aceite concluído.

| Ordem / item existente | Alteração ou conferência concreta | Saída verificável |
| --- | --- | --- |
| 1 — Dependencies / 3d | Concluído em `5c1d8f73`: preservar base, replay, confirmação/leitura, piso de versão, identidade/acesso e invalidação de paginação. | 145 passes, tipos/lint e prova UI/MCP em `evidence-u3-dependencies-20261015.json`; não repetir sem impacto novo. |
| 2 — Architecture / Resources / 3d | Preservar update/create/delete/import publicados. Revisar e qualificar o WIP do pai que transmite `revision/active/readCurrent` e aguarda a releitura completa, sem remontar a autoria. | Mutação MCP com formulário aberto chega automaticamente; rascunho permanece, revisão é explícita e escrita UI é confirmada pelo MCP. Não repetir as provas de operações já publicadas sem alteração relevante. |
| 3 — Mockups e Knowledge / Resources / 3d | Revisar o WIP de snapshot da coleção e concorrência no writer de Mockups, wrapper Knowledge e callbacks do pai. Concluir qualificação do conjunto Resources conforme roteiro abaixo. | Provas de mesma versão com coleção alterada, recuperação sem segunda escrita, subaba oculta/reaberta e acesso. Testes existentes só são repetidos por mudança ou falha; auditoria/catálogo e browser/MCP ainda pendentes. Ledger e commit/push após qualificação. |
| 4 — UV18–UV21 / fim de U3 | Reconciliar evidências existentes por aceite e executar somente os casos ainda não demonstrados: evolução real de Card/Delivery sem bump da Spec, recuperação e autoridade nos consumidores alterados; conclusão composta, resultados individuais e histórico completo. Inspecionar loading, pinpoints, teclado e foco. | Matriz UV → evidência → resultado → limite sem consumidores previstos pendentes. Testes frontend afetados e build aprovados, screenshots inspecionadas, ledger e commit/push de U3. |
| 5 — U4 / artefato | Regressões pertinentes, catálogo gerado, closure e build dos dois wheels/frontend. Instalar par exato em ambiente/home novos, comparar `.py` e frontend byte a byte e iniciar processo posterior à instalação. | Origens dos imports, par de commits, hashes e processos registrados; oito budgets ZERO. Não usar runtime de checkout como prova do artefato. |
| 6 — U4 / percurso | Nova iniciativa mock: Ideação → Refinamento → Spec e Cards Not Started em Draft → avaliações preservadas → conclusão única → execução → Tasks/Test/Spec Done → busca e consolidação KG. | UV01–UV24 com evidência observada, percentuais aplicáveis em 100%, canonical/proveniência, ledger e commits/pushes finais. Não inventar execução nem afrouxar policy para concluir. |

**Diagnóstico anterior ao WIP atual:** a leitura de
`ArchitectureTab.tsx` mostra que `readScope` limpa seleção/design; `revision`
recarrega listas, e a mudança de `selectedSummary` pode substituir `design`
pelo detalhe recebido. Portanto, não é correto atribuir a limpeza diretamente
ao efeito de `revision`. A proteção contra sobrescrita do rascunho foi
qualificada no componente em `b1b83138`, inclusive por browser/Refresh manual.
No pai, Mockups usa `updateSpec({screen_mockups})` sem base explícita nessa
chamada; Knowledge combina apenas sua geração local; Architecture não recebe
`revision/active`. São constatações de fonte, não novos ensaios de browser nem
prova de falha do backend.

**Limites da intervenção:** não criar infraestrutura genérica de edição ou
sincronização. Se a reprodução demonstrar falta de precondição indispensável
em um contrato nativo, corrigir somente esse contrato/caller/testes no mesmo
item, mantendo portas públicas e a regra de investigação do pacote. Não
inventar replay onde a API não o garante. Melhoria opcional fica fora da fila.

Esta seção começou como planejamento; a linha 1 e as operações de Architecture
foram sucedidas pelos incrementos publicados. A integração das linhas 2–3 tem
WIP e provas parciais abaixo; as linhas 4–6 continuam pendentes.
O fixture atual de Spec tem snapshot de Checklist ausente no contexto full;
não usar essa limitação como motivo para afrouxar gate. U4 usa a iniciativa nova
prevista, configurada e admitida pelos caminhos normais.

#### Execução restante de Resources — detalhamento das linhas 2 e 3

**Checkpoint que sucede o WIP desta seção:** Core `537eea6` / Community
`ccc76df`, com [29 artefatos de evidência](evidence-u3-resources-parent-20261015.json).
Architecture v3 → MCP v4 → revisão/UI v5; Mockups mantém inclusão concorrente
e rascunho oculto; Knowledge tem criação/exclusão confirmadas com GET posterior
interrompido e recuperação sem segundo POST/DELETE. Disclosure atualizado.
Todos os budgets ZERO, catálogo e tipos aprovados; lint sem erros. A prova
nativa de Mockups avançou Spec 30 → 31, portanto não comprova mesma versão:
essa garantia está nos testes REST/persistência/frontend. Não alterar a semântica
de versão para adequar o probe. Resposta incerta/acesso/identidade têm provas de
componentes; as lacunas integradas UV18–UV21 permanecem explícitas.
Próxima ação é a linha UV18–UV21 da tabela, seguida de U4.

**Histórico da conferência do WIP de Mockups/Knowledge:** as operações
Architecture estão publicadas; não reaplicar 2a–2c. O diff local já conecta
Resources ao watcher no `SpecModal`, transmite visibilidade/atualidade, aguarda
callbacks de releitura e extrai `SpecKnowledgeResources`. Mockups conserva base,
snapshot e identidade da tentativa. O backend compara o snapshot sob o fence
transacional existente; os writers MCP capturam a coleção antes da mutação.
Isso não modifica o significado de `Spec.version`; o snapshot protege também
os caminhos em que ela permanece igual. Não cria aprovação, coluna persistida
ou migração.

**Provas existentes conferidas, não reexecutadas neste planejamento:**
`u3-mockups-snapshot-core-final.xml` (77 passes),
`u3-mockups-snapshot-race.xml` (1 pass), `u3-resources-parent.xml` (98 passes) e
`u3-resources-parent-qualified-cases.xml` (3 passes e 79 skips por seleção).
Total distinto nesses relatórios: **78 backend e 101 frontend**. A sessão
TypeScript já iniciada terminou com exit 0. As primeiras reproduções falharam:
o schema recusava a base; a tentativa baseada somente em versão ainda aceitava
sobrescrita. Preservar esses registros. Nada disso demonstra browser/MCP do WIP,
catálogo/closure finais ou par instalado.

| Ordem imediata | Trabalho delimitado sobre o diff existente | Condição de saída |
| --- | --- | --- |
| 3a/3b/2d — revisar e qualificar fonte | Conferir normalização do snapshot, guarda após releitura sob fence, captura antes de mutações nos quatro writers MCP e resposta REST de conflito. Revisar callbacks de Resources, respostas de outra identidade, leitura com a mesma versão e snapshot diferente, perda de acesso e confirmação de escrita separada da leitura. Preservar Design System e demais policies. | Sem sobrescrita silenciosa nem efeitos parciais no conflito. Ajustar somente falhas concretas; executar regressões afetadas, tipos/lint, gerador oficial do catálogo e teste de drift. Closure sem findings/drift e oito budgets ZERO; atualizar matrizes README somente pelo renderer oficial se necessário. |
| 3a/3b/2d — provar integração nativa | No runtime isolado com ambos os checkouts comprovados, abrir rascunho de Architecture e alterar via MCP; repetir para Mockups mantendo a versão da Spec e alterando a coleção. Revisar explicitamente e salvar pela UI, conferindo IDs/conteúdo pelo MCP. Em Knowledge, criar pela UI, interromper uma releitura posterior e recuperar. Exercitar ocultar/reabrir subabas com rascunhos. | Atualização automática sem Refresh manual; campos preservados; snapshot antigo recusado; conteúdo concorrente retido; nenhuma segunda escrita após confirmação. Disclosure reflete a leitura autoritativa. Registrar requests, IDs, versões/snapshots e screenshots inspecionadas; não inferir sucesso por título. |
| 3d — publicar incremento | Confrontar resultados com os casos já existentes de create/delete, resposta incerta, identidade e acesso. Reutilizar provas suficientes; executar somente lacunas reais. Registrar alcance e limites no ledger e evidência. | Commit/push pareados do incremento Resources, sem incluir snapshots operacionais indiscriminadamente. Resources qualificado não encerra sozinho U3. |
| UV18–UV21 — fechar U3 | Reconciliar a matriz com evidências existentes. Executar os casos ainda ausentes de Card/Delivery sem bump da Spec, rede, autoridade, conclusão composta, resultados individuais/histórico, teclado/foco/loading. | Todos os consumidores previstos demonstrados, testes/build pertinentes aprovados e conferência visual real. Sem novos consumidores ou redesign opcional na fila. Ledger e commit/push. |
| U4 — fechar entrega | Qualificar wheels/frontend pareados em instalação/home novas, comprovar arquivos e processo; executar o percurso mock novo previsto em §7/§8 até Cards/Test/Spec Done e consulta/consolidação KG. | UV01–UV24 ligados a prova e limite, oito budgets ZERO, percentuais aplicáveis comprovados e KG canonical. Nenhum gate relaxado ou resultado inventado. Ledger e commit/push finais. |

**Ponto de partida:** o diff local descrito acima, não nova implementação dos
editores. O processo isolado iniciado anteriormente precisa ter saúde e origem
conferidas antes do ensaio. Não reiniciar mutações antigas nem usar o runtime da
home padrão. A Spec antiga sem snapshot de Checklist não qualifica o percurso
integral; U4 usa a iniciativa nova já prevista, admitida pelos caminhos normais.
O critério final continua U1–U4 e UV01–UV24; não há milestone adicional.

**Histórico que fundamentou essa retomada:** os parágrafos e itens 2a–3b abaixo
conservam a decomposição original. Seus relatos de WIP Architecture foram
sucedidos pelo par publicado; a fila operacional é a tabela imediatamente acima.

**Estado publicado:** update autorado, fence, retenção/revisão de
rascunho e recuperação de Save estão qualificados em `867ef70e` / `b1b83138`;
evidência e limites no início do plano. Preservar essas mudanças.

**WIP Architecture da revisão anterior, hoje publicado:** seis arquivos Core e cinco
Community estendem a precondição às operações delete/import/payload e protegem
create/delete/import no componente. O serviço reutiliza a porta de fence;
composição de diagramas conserva a versão da leitura inicial. A UI distingue
resultado incerto de escrita confirmada com leitura pendente, conserva entrada
local e não infere sucesso pelo título. `readCurrent` já existe no componente,
mas Resources ainda não está integrado ao watcher do pai.

Relatórios anteriores, lidos nesta conferência: `u3-architecture-operations-before.xml`
registra três falhas de caracterização; `u3-architecture-operations-core-final.xml`
registra **120 passes**, incluindo catálogo; `u3-architecture-operation-recovery-ui.xml`
registra **34 passes** (31 ArchitectureTab e três ExcalidrawImportDialog).
Nenhuma nova execução foi feita neste planejamento. Esses resultados não
comprovam browser, integração do pai, closure final ou artefato instalado.

**Checkpoint que sucede o WIP acima:** operações publicáveis em `826d5dfa` /
`d58ee70d`, com 120 testes backend, 34 frontend, tipos/lint e closure aprovados.
Create v1 foi testado com seleção anterior v3. Prova UI/MCP de import v1 recusado
após v2, revisão explícita, v3, falha de GET e recuperação sem escrita; delete
v3 confirmado, listagem interrompida e recuperação sem segundo DELETE.
Evidência e limitações no início do plano. Retomar em 3a/3b → 2d.

**Ação de conferência anterior, agora concluída nos limites da evidência:** revisar o diff e a suficiência das asserções de 2c,
especialmente criação v1 quando o design antes selecionado tinha versão maior,
falha do callback de leitura do pai e respostas após troca de identidade.
Corrigir somente lacunas demonstradas; executar os testes afetados e provar
create/import/delete por UI/MCP, incluindo recuperação sem segunda escrita.
Depois concluir 3a/3b antes de ativar o watcher de Resources em 2d: os painéis
mantidos montados também recebem props e não podem perder rascunhos ocultos.
A tabela continua decompondo o mesmo item 3d, sem novo milestone.

**Ponto de partida histórico da conferência de planejamento:** existia
um diff local de oito arquivos Python (seis Core, dois Community). Ele acrescenta
`ArchitectureDesignUpdate.expected_version`, conflito de domínio, porta pública
`fence_version`, implementações de persistência e transporte REST/MCP. O frontend
ainda não enviava a versão autorada. Esse diff não era integração concluída;
os resultados de 67 casos abaixo foram sucedidos pela qualificação acima.

O teste `test_rest_update_rejects_the_authored_version_after_another_writer`
reproduziu PATCH baseado em v1 aceito depois de outra gravação produzir v2;
o título antigo sobrescreveu o novo em v3. O XML anterior registra uma falha;
o XML posterior `u3-architecture-version-core.xml` registra 67 passes, zero
falhas/erros/skips em REST, repository e MCP. São execuções anteriores lidas
nesta conferência, não novos ensaios. O arquivo `.environment.json` comprova
imports dos dois checkouts e divergência de 25 arquivos Core/12 Community em
relação à instalação: não é qualificação do pacote instalado.

| Passo dentro de 3d | Intervenção delimitada | Prova necessária antes de avançar |
| --- | --- | --- |
| 2a — concluir a precondição de Architecture | Revisar o diff existente em schema, serviço, porta, adaptadores e REST/MCP. Conservar a versão autorada quando fornecida; chamadas sem ela só protegem a versão lida pelo serviço. Verificar create, delete e import separadamente, sem atribuir a eles a garantia de update. Atualizar descrição da tool e regenerar o catálogo pelo gerador oficial. | Versão inválida recusada; conflito preserva conteúdo, versão, snapshots, findings e eventos; duas transações reais não sobrescrevem silenciosamente. Exercitar o adaptador Community, além do adapter de testes Core. Paridade REST/MCP, rollback e drift do catálogo aprovados. |
| 2b — proteger o editor antes do watcher | Em `components/architecture/ArchitectureTab.tsx`, separar último estado lido, base autorada e rascunho; incluir edições de entidade/interface/diagrama. Atualização externa não substitui campos nem troca silenciosamente item removido. Revisão explícita permite nova tentativa; perda de acesso limpa o conteúdo protegido. Resposta atrasada não alcança outro Board/parent/design. | Testes de refresh com rascunho, item removido, conflito, troca de identidade e recusa de acesso. O request de update usa a base do rascunho, nunca a última versão recebida silenciosamente. Preservar leitura e restrições de snapshots herdados em Cards. |
| 2c — separar escrita e recuperação | Para save/create/delete/import, distinguir recusa, resultado incerto e escrita confirmada seguida de leitura falha. Capturar a intenção original; impedir duplo envio. Após confirmação, repetir somente a leitura. Sem recibo/idempotência nativos, não oferecer replay como se fosse seguro: reconciliar pela leitura autoritativa e manter incerteza quando ela não resolver. Se faltar capacidade indispensável, acrescentá-la apenas ao contrato afetado e demonstrá-la. | Falha de resposta e falha do GET posterior não duplicam criação/importação nem repetem exclusão. IDs/versões confirmados por leitura; confirmação tardia de outro contexto ignorada. Não declarar identidade pelo título ou igualdade de conteúdo. |
| 2d — integrar Architecture na Spec | Em `components/specs/SpecModal.tsx`, transmitir `active`, revisão e atualidade pelo watcher existente somente após 2b/2c. Substituir callbacks que remontam o workspace por releitura protegida, conservando seleção, rascunhos e a maior versão já observada. | Browser com formulário aberto + mutação MCP: dados atuais chegam, rascunho permanece, revisão é explícita e gravação UI é confirmada pelo MCP. Testes do componente e pai, tipos/lint pertinentes. Não basta screenshot estático. |
| 3a — Mockups | Proteger substituição de `screen_mockups` no writer do pai com base autorada e precondições nativas. Conservar ID e payload da tentativa; revisar explicitamente coleção alterada, sem restaurar item removido por composição silenciosa. Aplicar confirmação/recuperação e proteção de identidade/acesso. | Concorrência em coleção, inclusão/exclusão, resposta incerta e leitura falha sem perda/duplicação. Mudança MCP com formulário aberto, escrita UI revisada e releitura MCP; testes do pai/editor afetados. |
| 3b — wrapper Knowledge e disclosure | Reutilizar `KnowledgeWorkspace` já qualificado; proteger somente criação e callbacks restantes no wrapper da Spec. Conectar visibilidade/revisão sem remontar editor. Atualizar `ResourceGateDisclosure` pela fonte autoritativa e sinalizar indisponibilidade, sem calcular gates no cliente. | Criação única, resposta tardia descartada, rascunho preservado, reabertura de subaba atualizada e dados retirados em perda de acesso. Prova UI/MCP das escritas alteradas; regressões pertinentes dos leitores existentes. |

**Dependências vigentes:** preservar 2a–2c publicados; revisar/qualificar o WIP
3a/3b/2d → publicar Resources → UV18–UV21 → U4.
O watcher de Resources só é ativado quando todos os formulários que permanecem
montados conservarem base e rascunho. 3a/3b reutilizam os contratos já presentes,
sem exigir outro framework. Todas essas linhas encerram o mesmo item 3d.
O backend não ganha autoridade para julgar avaliações; a correção de
concorrência impede uma gravação sobre base ultrapassada. Não introduzir
migração, compatibilidade antiga ou novo requisito de aprovação humana.

**Fechamento de U3, após Resources:** mapear UV18–UV21 às evidências existentes
e executar somente lacunas reais. Conferir conclusão composta com atores de
permissões distintas, resultados individuais/Previous, progresso de Card sem
bump da Spec, recuperação de rede, acesso, foco/teclado e animação de loading.
Cada caso registra operação, identidade, versão anterior/posterior, resultado
persistido e observação UI; testes de componente não equivalem à prova nativa.
Preservar resultados negativos e indisponibilidade, sem compensação por média.

**Fechamento de U4:** qualificar o par exato construído e instalado conforme
§8, depois tramitar iniciativa mock nova até Tasks/Test/Spec Done e busca/KG
canonical. Configuração necessária do Board é feita pelos caminhos autorizados;
nenhuma prova externa, avaliação ou inspeção humana será declarada sem execução.
Não reinstalar ou limpar a home do usuário como efeito deste planejamento.

**Controle de escopo e checkpoint:** commits/pushes por incremento qualificado,
ledger com evidências e próximo passo; não declarar milestone completo com
writer/leitor previsto pendente. Só acrescentar correção que bloqueie um aceite
já definido, com reprodução. Melhorias opcionais ficam fora. A matriz continua
UV01–UV24, os milestones continuam U1–U4 e o resultado final continua uma única
conclusão com todas as avaliações, critérios, severidades e autoridades atuais.

#### U2 — encerrar backend e contratos antes da interface

Sequência já executada, mantida para rastreabilidade de U2.3/U2.4. Não é
trabalho pendente; acrescentar regressão somente por impacto ou falha nova.

1. **Preservar a qualificação nativa e fechar leitura/histórico.** A investigação
   anterior está resolvida: coleções IR/OR omitidas pelo seed relacional, plano
   de Decision incompleto, aprovação de decomposição não preparada e expectativas
   de transporte anteriores ao contrato composto. O writer público já inicializa
   as coleções; não foi acrescentado fallback `None → []` ao produto. O fixture
   usa parecer nativo explícito, sem aprovação no helper genérico de submissão.
   Um caso adicional prova qualidade positiva sem decomposição: mantém Approved.

   `u2-native-validation-final.xml` registra **90 passes** nas quatro suítes
   (`test_spec_validation_native_gate.py`, `test_spec_validation_lifecycle_gate.py`,
   `test_spec_validation_five_metric_transport.py`,
   `test_skb_b10_policy_transition_rest.py`). Não reabrir essa triagem nem repetir
   a suíte sem mudança, falha ou preocupação nova que o justifique. Ela precedeu
   a extensão das coleções; a regressão final do par em U4 continua obrigatória.
   A projeção/metadados após essa extensão tem 79 passes próprios registrados.

   A composição arquitetural já
   está corrigida; preservar catálogo público e sessão/snapshot únicos.
   Preservar as regressões de permissões por componente em
   `spec_planning_readiness.py` e da listagem em `spec_crud.py`, `services/main.py`,
   MCP e REST. Mapear os testes existentes antes de acrescentar casos de leitura
   negada, Board distinto, paginação e Current/Previous. Conferir tipos separados
   para append e listagem: `lifecycle_state` é projeção de leitura, não campo
   garantido pelo recibo de `submitSpecEvaluation`. A listagem não recalcula
   julgamentos nem aplica a antiga flag stale. A paginação atual limita a
   resposta, mas ainda lê a coleção JSON nativa: não alegar paginação física
   indexada nem abrir uma migração para este incremento.
2. **Fechar paridade resumo/promoção.** Conferir `get_planning_readiness`,
   `submit_spec_validation`, `move_spec` e `allowed_transitions` contra os
   predicados nativos. A lista de coberturas já é compartilhada; isso não prova
   paridade dos demais componentes. Cobrir policy off/advisory/blocking,
   autoridade indisponível, snapshot de Checklist, leitura negada e resultados
   malformados. Lint/AC/recursos exclusivos da avaliação habilitada não viram
   obrigações novas no caminho direto. `ready` não concede permissão nem muda
   lifecycle; bloqueio de promoção não impede registrar parecer válido negativo.
   A recusa de admissão sem Checklist já tem fixture com realm e dois passes
   REST/MCP em `u2-admission-rest-mcp.xml`, sem alteração em estado, versão,
   edição, avaliações, snapshots, atividade ou eventos. Referenciar essa prova;
   não refazer o diagnóstico. A autenticação wire continua parte de U4.
   Saída: UV04–UV17/UV22 pertinentes com persistência real, sem escrita parcial.
3. **Consumir os metadados leves já implementados.** O contrato por Board
   aceita 1–50 referências Spec/Card ou coleção Spec/Card e retorna revisões,
   sem corpos de avaliações ou execução de gates. Coleções usam o próprio ID
   do Board e exigem leitura da entidade; token ausente é indisponibilidade.
   Testes existentes demonstram custo limitado, insert/edit/delete/rollback e
   mudanças sem bump da Spec; completar a prova integrada, sem recriar a API.
   `include_planning` executa gates: reservá-lo para carga explícita/invalidação,
   nunca para polling. Revisão de autoridade não equivale a revisão da coleção:
   `spec_collection` já permite descobrir Specs desconhecidas no SpecsPanel.
   Usar `card_collection` no Kanban, em vez de presumir que a assinatura de
   IDs já conhecidos descobre novos Cards. Tokens são invalidação, não aprovação.
4. **Publicar U2 qualificado.** Rodar regressões afetadas, REST/MCP, frontend de
   contratos, catálogo gerado e closure. Registrar resultados e UV demonstrados
   no ledger; commit/push pareados. Não declarar browser/E2E concluídos por
   testes de serviço ou pelo fixture MCP que isola a aceitação externa de lint.

Ponto de partida da conferência, localizado no checkout desta revisão:
`tests/test_spec_validation_composition.py` já contém casos de resultados
negativos, reuso de outro avaliador, rejeição corrente, promoção direta, REST,
rollback, replay e conflito de manifesto; `tests/test_spec_planning_readiness.py`
cobre resumo, componentes ocultos, indisponibilidade, modos e ausência de score
combinado; `tests/test_workspace_revisions_contract.py` cobre população limitada
e escopo. No Community, `tests/test_spec_planning_manifest.py` cobre o adapter.
Localizar esses casos não equivale a executá-los nem encerra a matriz. Mapear
UV → caso → resultado registrado antes de acrescentar testes; ampliar as suítes
somente nas lacunas. Conferir os métodos `get_planning_readiness`,
`submit_spec_validation`, `move_spec` e os consumidores de ações permitidas,
sem criar outra implementação de gates.

#### Matriz finita para encerrar a paridade U2

**Encerramento backend desta matriz:** as tarefas abaixo foram executadas.
Os 16 casos de recursos, 36 de Code Evidence e quatro de contexto crítico
passaram; o mapa reúne 208 casos selecionados e registra as fronteiras dos
fixtures. Os testes de domínio não substituem a prova integrada U3/U4. Preservar
o histórico abaixo para explicar a reprodução e sua correção, não como fila aberta.

**Primeiro trabalho reproduzido e agora corrigido:**
`Community/tests/test_spec_planning_resource_parity.py`, preservado no commit
`f7e910d4`. O JUnit existente `u2-resource-parity.xml` terminou com 16 casos:
11 passes, 5 falhas, nenhum erro/skip (77,404 s). A revisão deste plano leu o
artefato; não reexecutou a suíte. Evidência e hashes em
[reprodução de recursos](evidence-u2-resource-parity-pending-20261014.json).

1. Em `Core/services/main.py`, preservar o diagnóstico tipado de
   `ResourceGateViolation` na conclusão composta; o tratamento genérico atual
   substitui seus códigos por `spec_validation_gate_not_ready`. Quatro falhas
   cobrem arquitetura ausente e mockup sem alocação, em REST e MCP.
2. Em `Core/mcp/server.py`, conferir o tratamento de `ResourceGateError` de
   `move_spec` e reutilizar o projetor de recursos existente. A quinta falha é
   a transição direta com qualidade desligada: a mensagem vira texto em `error`
   e perde o envelope estruturado. Conferir os adaptadores de submissão e REST
   para não trocar uma perda de diagnóstico por erro não tratado.
3. Manter aplicabilidade, permissões, N/A com justificativa e policies atuais.
   O ajuste é de propagação do erro, não de aprovação. Atualizar a regressão
   antiga que espera o wrapper genérico em `test_e2e_regression_hardening.py`.
   Critério de saída: os 16 casos passam com razões nativas e rollback; nenhuma
   promoção indevida. Os cinco casos falhos interrompem antes da asserção final
   de persistência, portanto ainda não comprovam rollback nesses caminhos.
4. Depois, completar apenas a lacuna pública de Code Evidence: usar o predicado
   real com projeção válida, incompleta e indisponível, incluindo skips admitidos
   e qualidade ligada/desligada nos caminhos aplicáveis. Injetar falha na porta
   de leitura quando necessário, sem mockar aprovação do gate. Comparar resumo,
   preview, escritor, diagnóstico e estado persistido. Reutilizar os casos
   existentes de cobertura; não exigir implementação antes de iniciar.
5. Reconciliar os 120 casos do mapa existente e esses resultados com UV04–17/22;
   executar apenas regressões impactadas e lacunas demonstradas. Publicar U2
   somente após as falhas conhecidas estarem resolvidas e os limites registrados.

Esta ordem detalha a família já prevista de arquitetura/recursos/Code Evidence;
não acrescenta milestone, avaliação, gate ou mudança de autoridade.

Primeiro relacionar os casos já executados aos UV; acrescentar somente a
combinação ainda não demonstrada. Cada confronto usa a mesma fonte/policy para
readiness, conclusão composta, transição direta quando admitida e ações
permitidas. Comparar razão, estado e efeitos persistidos; não somente HTTP 2xx.

| Família / UV | Ponto de partida existente | Conferência restante delimitada |
| --- | --- | --- |
| Qualidade/decomposição; UV04–06/10/12/13 | Core `test_spec_validation_composition.py`, `test_spec_evaluation_currentness.py`; Community `test_spec_validation_native_gate.py` | Relacionar os casos de aprovação, rejeição ativa, média nativa, autoria distinta e qualidade off aos caminhos públicos. Não duplicar os casos já provados. |
| Lint/Checklist; UV07/08 | Community `test_spec_planning_lint_parity.py` (12 passes), `test_spec_admission_missing_checklist.py` (REST/MCP, dois passes), `test_checklist_frozen_gate_characterization.py`, `test_sqlalchemy_checklist.py`; Core `test_frozen_checklist_readiness_matches_public_promotions` (16 passes) | Lint ausente/aceito com findings agora confrontado com preflight/admissão nativos, resumo/preview/writers REST/MCP. Checklist congelado e snapshot ausente preservados. Referenciar as duas evidências; acrescentar somente lacuna demonstrável. |
| Policy; UV09/10/11 | Community `test_spec_planning_policy_parity.py` (28 passes), `test_skb_b10_policy_transition_rest.py`; Core `test_spec_planning_readiness.py` | Modos aplicáveis, recibo ausente/negativo/positivo e leitura negada confrontados com persistência nativa e escritores públicos; indisponibilidade por injeção tipada depois da resolução real da autoridade. Recusas compostas preservam razões nativas. Autenticação wire e percurso instalado permanecem no U4. |
| Arquitetura/Code Evidence/alocação; UV09/22 | Core `test_spec_validation_composition.py`, `test_spec_planning_readiness.py`; Community `test_spec_validation_lifecycle_gate.py` | Reaproveitar os 32 casos de cobertura; fechar somente diferenças não demonstradas com os mesmos dados. Skip de cobertura não oculta projeção inválida; não exigir código implementado antes da execução. |
| Concorrência/replay/permissões; UV11/14–16 | Core `test_spec_validation_composition.py`; Community `test_spec_planning_manifest.py`, `test_validation_cycle_routes_authorization.py` | Mapear fence, rollback, chave/recibo, escopo e reautorização. Falha técnica não deixa parecer parcial; replay não promove novamente nem ignora acesso atual. |
| Início; UV17 | Community `test_spec_planning_dependency_parity.py` (REST/MCP, dois passes); regressão Core de dependências/projetor/catálogo (84 passes) | Comprovado: planejamento validado com predecessor pendente, início negado e depois liberado preservando edição/head/pareceres. Completar o próprio lifecycle do predecessor pertence ao percurso U4; no teste sua conclusão é entrada persistida de fixture. |

Entregável de U2: evidência UV → caso → comando/artefato → resultado → limite.
Caso já coberto recebe referência, não um teste duplicado. Não marcar UV completo
quando só uma superfície foi exercitada; o percurso autenticado UI/MCP de U4
continua complementar à prova de serviço/transportes.

#### U3 — compor Validation e sincronizar a interface

Depende dos contratos U2 qualificados; o frontend não recalcula gates.

1. **Workspace único.** Em `SpecModal`/painéis de Validation, topo compacto com
   edição, estado geral e pendências; seções de Spec Validation, decomposição,
   Checklist, Requirement lint e Policy Compliance. Coberturas apontam à seção
   de correção existente. Os 17 componentes técnicos não viram 17 abas novas.
   Manter círculos de score, pinpoints expansíveis, autoria, dimensões,
   severidade e histórico completo. Não restaurar o antigo quadro isolado
   Decomposition evaluations.
2. **Uma ação principal de conclusão.** Reutilizar pareceres Current de outros
   avaliadores; oferecer o envio conjunto apenas a quem possui ambas as
   permissões. Distinguir registro negativo, pré-requisito pendente, conflito e
   falha técnica. Preservar os formulários nativos; nenhum parecer é fabricado
   pela ação. O retorno informa resultados gravados e se houve promoção.
3. **Um hook compartilhado.** Consultar metadados a cada 5 s com a página
   visível, ao recuperar foco e conexão; deduplicar, cancelar e aplicar backoff.
   Invalidar os consumidores afetados, carregar painéis fechados sob demanda e
   preservar aba/filtros/scroll. Mudança externa com formulário sujo mantém a
   base original e exige revisão explícita antes de nova submissão; não fazer
   retry automático com manifesto atualizado. Revogação limpa dados protegidos.
4. **Qualificação visual e funcional.** Testes frontend e browser com mutações
   MCP simultâneas: UV18–UV21, troca de Spec, respostas fora de ordem, dirty form,
   histórico, foco, loading Pulse, erro/retry e permissões. Commit/push e ledger.

#### Ordem concreta de integração de U3

Os passos abaixo são tarefas do mesmo milestone U3, não novas frentes de produto.

| Ordem | Arquivos/pontos de integração | Entrega e prova exigida |
| --- | --- | --- |
| 1 | `SpecPlanningSummary.tsx`, cliente `services/api.ts`, tipos de avaliações | Preservar a correção de formatação; conferir diferença append/listagem e rodar tipos/build. Resultado ausente continua unavailable. |
| 2 | `SpecValidationPanel`, `SpecDecompositionHistory`, `SpecDecompositionPanel` e `SpecModal` | Qualificar o WIP de pareceres Current/Previous com autoria, dimensões, justificativas e scores próprios; navegação respeita leitura e abre destino real. UV12/UV13/UV21. |
| 3 | `SubmitSpecValidationModal` e resposta da API composta | Qualificar campo opcional com permissão pertinente, reuso de Current e distinção registro/promoção já implementados. Testar negativo, bloqueio, conflito e retry. UV11/UV12/UV15/UV16/UV22. |
| 4 | `useWorkspaceRevisions`, `SpecModal`, `SpecsPanel`, `KanbanBoard`, `CardModal` e Delivery | Preservar a integração existente em Validation/conclusão/SpecsPanel. Conectar os demais consumidores na ordem abaixo; deduplicar por cliente autenticado/Board, invalidar allowed transitions e Coverage mesmo sem mudança de Spec.version. Painéis fechados carregam sob demanda. UV18/UV19. |
| 5 | Formulários da Validation, inclusive Checklist, e submissão composta | Preservar campos e manifesto original; apresentar mudança externa e revisão explícita antes de aceitar nova base. Não incrementar chaves que remontam formulários sujos a cada polling. Revogação limpa conteúdo protegido. UV20. |
| 6 | Testes dos componentes e browser contra runtime isolado | Observar mutação MCP e convergência visual, histórico completo, foco, navegação, offline/reconexão e perda de acesso. Não substituir esse ensaio pelos testes isolados do watcher. UV18–UV21. |

A revisão explícita de conflito apresenta a necessidade de reler a edição e
seus resultados; preservar o rascunho não significa validar automaticamente
seus julgamentos contra a nova base. O usuário pode descartar o rascunho ou
revisá-lo e confirmar a nova base. A confirmação gera nova tentativa, sem
reutilizar a identidade de um request cujo conteúdo mudou. Não criar fusão
semântica automática de pareceres.

**Sequência de código restante no passo 4:**

1. Em `KanbanBoard.tsx`, assinar `card_collection` pelo watcher existente e
   atualizar as colunas/capas após mutação externa. Separar invalidação de dados
   da escolha visual: hoje `columnsReloadToken` também fecha View all. Preservar
   coluna expandida, filtros e página quando válidos, descartando cursor inválido.
   Reutilizar a geração/AbortController existentes; limpar store e respostas
   pendentes quando o acesso for perdido.
   **Implementado e parcialmente qualificado:** descoberta UI/MCP de Cards e
   expansão preservada, testes de filtro, resposta antiga, corrida de autoridade
   e recuperação de página inválida. Não reaplicar; completar a prova de barras
   com entrega real e os cenários integrados restantes junto de CardModal/U4.
2. Qualificar o WIP de `CardModal.tsx`, `EditableField.tsx` e
   `CardDeliveryDoDPanel.tsx`. A substituição do timer pesado de 10 s pelo watcher,
   a fila de invalidação e a base congelada do formulário já estão escritas.
   Fechar os casos abaixo antes de considerá-las entregues. Invalidar Delivery,
   readiness e dependências afetadas; leitura completa só após mudança ou ação
   explícita. Preservar aba/rascunho e os controles nativos de cada operação.
   Não manter ambos os timers nem recarregar painéis fechados.
3. Em `SpecModal.tsx`, concluir primeiro a proteção dos editores. O checkpoint
   `e2880609` já retira `spec.version` das chaves de `TextRequirementsTab`,
   `CriterionEditor` e `RequirementVerificationPanel`, conserva a base e limita
   a expansão do watcher a FR/AC. Não reimplementar essa parte; concluir as
   pendências discriminadas abaixo e comprovar a integração. Depois expandir
   além dessa assinatura restrita, incluindo
   Coverage, recursos, referências e dependências. A revisão de Coverage precisa
   refletir mudança de Card; `${spec.edition}:${spec.version}:${spec.updated_at}`
   isoladamente é insuficiente. Usar token recebido, sem cálculo de gate na UI.
4. Qualificar o conjunto aberto por mutações MCP: novo Card, progresso/Delivery
   sem bump da Spec, parecer, formulário sujo, troca de Board/Spec, resposta
   atrasada, acesso revogado e recuperação de rede. Reaproveitar a prova parcial
   de SpecsPanel; confirmar recuperação de acesso sem restaurar cache antigo.
   Completar a inspeção visual de resultados/histórico e ação de conclusão.

Esses passos encerram a integração já prevista; não criam serviço de eventos,
framework de formulários ou redesign de outras áreas. Um problema reproduzido
que impeça um desses aceites é corrigido no próprio passo, com evidência.

**Fechamento delimitado do WIP CardModal/Delivery (passo 2):**

| Ordem | Trabalho concreto | Prova de saída |
| --- | --- | --- |
| 2a | Atualizar os quatro testes que ainda procuram contenteditable no título, agora renderizado por EditableField. Preservar as populações normal/bug/test em Spec Done e rework Rejected. | Testar interação: quem pode editar abre o campo e salva; quem não pode não abre nem chama updateCard. Não basta trocar a asserção de atributo ou excluir cenários. |
| 2b | Separar falha de leitura de Delivery de erro de submissão. O WIP conserva dados antigos após erro e encerra loading; isso não permite confirmar uma base atual. | Refresh falho mantém rascunho, sinaliza desatualização e bloqueia envio/revisão da base até leitura bem-sucedida. Retry de escrita continua disponível quando a leitura está válida. Testar falha inicial, refresh falho e resposta atrasada. |
| 2c | Conferir fila de invalidação durante carregamento de thresholds, mudança de autoridade e troca de Card/Board. | Revisão pendente produz recarga; resposta antiga não repovoa dados; perda de acesso fecha/limpa conteúdo; nenhuma leitura pesada periódica sem mudança. |
| 2d | Qualificar preservação de texto e seleção de evidências, inclusive obrigações/recibos removidos na fonte. | Formulário não perde campos nem troca expected_card_version, expected_spec_edition ou delivery_revision silenciosamente. Revisão explícita apresenta escolhas atuais; nenhuma submissão automática. Testes de Delivery e EditableField mais interação no CardModal. |
| 2e | Conferir os consumidores montados de CardModal: Delivery/readiness, dependências, recursos/KB e histórico. Propagar a revisão somente onde faltar. | Mutação MCP aparece no leitor visível; painel fechado atualiza ao abrir. Browser mostra título remoto com rascunho local preservado e sem escrita involuntária. Registrar quais superfícies foram efetivamente exercitadas. |

Avanço `2a6324d3`: as quatro falhas de `u3-card-live.xml` foram resolvidas com
o editor real. `u3-card-delivery-final.xml` registra 137 passes (CardModal 83,
Delivery 33, Progress 19, EditableField 2), incluindo os cenários de 2a–2d;
tipos passaram e lint terminou sem erros, com warnings registrados. Browser
UI/MCP confirmou o título remoto com rascunho preservado. Não repetir essas
provas sem alteração/falha nova. **Naquele checkpoint, 2e estava pendente; a
tabela seguinte registra as provas posteriores.** Community `77394617` já adia Delivery/readiness oculta,
atualiza Activity e reconsulta autoridade de transição sem depender do timestamp
do Card. Cinco suítes/146 casos com resultado final aprovado, tipos e lint
registrados no ledger. A arquitetura do Card é somente leitura; os writers de
Knowledge e Targets exigem preservação da base antes de conectar invalidação.
Essa prova parcial não encerra U3 nem substitui o E2E instalado U4.

**Fila restante de 2e, conferida no código deste checkpoint:**

| Ordem | Alteração delimitada | Aceite específico |
| --- | --- | --- |
| 2e.1 — Knowledge | Implementado em `e34aa415`; integração CardModal/Resources e prova UI/MCP concluídas no checkpoint `ae265bdc`. | Revisões 1 → 2 via MCP e 3 via UI; rascunho preservado, revisão explícita e leitura MCP confirmatória. Preservar testes e prova, sem reimplementar. |
| 2e.2 — Targets | Implementado em `ae265bdc`: base da Spec/recibo congelada, revisão explícita, erro/visibilidade/escopo protegidos. | 41 testes finais aprovados; browser preservou rascunho após Spec 2 → 3 via MCP e gravou Target com source_spec_version=3 após revisão explícita. Recibo da fonte mock observado/aceito nativamente. Não é aceite instalado U4. |
| 2e.3 — leitores restantes | KB herdada, snapshot de `ArchitectureTab` e Resource Gate em `b30ad347`; Project Structure adiada/protegida e mensagem de seleção removida em `a8704e96`. Dependências/histórico conferidos. | 129 testes finais e tipos aprovados; Architecture v1 → v2 e remoção de KB selecionada observadas no browser após MCP. Não repetir as mutações. Project Structure tem prova frontend; autoria nativa/instalação ficam no U4 devido à recusa de contexto completo no runtime antigo. |

Knowledge e Targets já foram reproduzidos, corrigidos e qualificados conforme
a evidência acima. Para KB herdada/Architecture, seis falhas foram reproduzidas
antes da correção; 107 casos passaram depois, incluindo respostas antigas,
leitura negada e aba oculta. Preservar os cenários existentes. Conferir somente
os leitores ainda sem evidência; acrescentar correção/teste se houver lacuna
reproduzida. Não repetir as suítes aprovadas sem impacto.
O browser deve comprovar ao menos a atualização do recurso visível por MCP e
a preservação de um rascunho do writer alterado, no mesmo runtime isolado.

**Limite de concorrência:** CardUpdate não possui CAS genérico. A proteção de
EditableField detecta mudança recebida pelo cliente e preserva rascunho; não
garante ausência de corrida entre leitura e escrita no servidor. Não usar
Spec.version como versão do Card nem declarar CAS implementado. Os fences da
conclusão composta e de Delivery continuam obrigatórios e devem ser testados
pelos seus contratos reais; não ampliar esta revisão para um CAS global.

Não encerrar U3 enquanto existir qualquer uma destas lacunas: consumidor previsto
sem atualização; botão de conclusão que não oferece o payload composto permitido;
histórico reduzido a título/status; frontend recalculando gate; atualização que
apaga rascunho; dados protegidos mantidos após revogação; build ou teste pertinente
vermelho. O trabalho de U4 permanece obrigatório após esse fechamento.

#### Detalhamento executável dos editores da Spec — item 3 de U3

São tarefas do mesmo milestone. 3a e a prova principal de 3b foram executadas
em `b0a497f4`, conforme evidência abaixo. Retomar em 3c, preservando os testes
de erro/remoção/autoridade e acrescentando somente lacunas reproduzidas.

| Ordem | Trabalho delimitado | Aceite e evidência |
| --- | --- | --- |
| 3a — finalizar o incremento existente | Em `TextRequirementsTab`, `CriterionVerificationPanel` e `RequirementVerificationPanel`, distinguir o conflito nativo de versão de outras respostas 409. Conferir também a resposta estruturada `success=false`; não perder seu código ao construir uma exceção. Corrigir o warning de dependência de `scope` sem provocar recargas por identidade instável. | Conflito real preserva campos e impede adoção silenciosa da base; recusa de negócio permite correção conforme o contrato e não prende o editor esperando um bump inexistente. Testes dirigidos de ambas as respostas, tipos e lint dos arquivos afetados. |
| 3b — concluir a prova dos editores protegidos | Exercitar FR/AC, plano do critério e qualificação: alteração externa, requisito/vínculo removido, leitura indisponível, perda de acesso e sucesso de escrita seguido de falha de refresh. Preservar os testes existentes e acrescentar somente lacunas. | Browser com mutação MCP no mesmo runtime: texto local preservado, envio bloqueado enquanto a base estiver inválida, revisão explícita e escrita UI confirmada por leitura nativa. Não repetir create já confirmado quando apenas o refresh falha. |
| 3c — fechar o inventário de consumidores | Conferir os editores já montados de Details, BR/TR/IR/OR, Contracts/Decisions, arquitetura, cenários e Project Structure antes de ativar refresh global. Registrar por consumidor: identidade, base do writer, revisão recebida, comportamento de rascunho e autorização. Alterar somente onde o refresh produzir perda ou escrita sobre base trocada. | Nenhum formulário sujo é remontado ou atualizado silenciosamente; nenhuma resposta de outra Spec repovoa a atual. Leitor sem editor apenas invalida sua projeção; não criar framework genérico de formulários. |
| 3d — ampliar a assinatura e fechar integração | Integrar os consumidores previstos ao watcher existente; Coverage recebe mudança de Card mesmo sem alteração de `Spec.version`. Manter recarga sob demanda e cancelar gerações antigas. | UV18–UV20 demonstrados com modal e Board abertos; histórico/ação composta qualificados em UV21. Só então encerrar U3 e passar a U4. |

O checkpoint anterior dos XMLs (164/68 passes) está no ledger. O incremento
seguinte reproduziu cinco falhas em oito casos e corrigiu o tratamento de 409;
137 testes passaram. Tipos exit 0 e lint sem o warning novo de qualificação.
Prova UI/MCP: FR versões 9 → 10 → 11, AC 13 → 14 → 15 e qualificação
15 → 16 → 17, com rascunhos preservados, revisão explícita e leitura MCP final.
Uma queda do Vite interrompeu o primeiro ensaio do AC; a repetição foi separada
e a recuperação após recarga inteira da página não foi contabilizada.

O fechamento da Spec aninhada foi reproduzido: cliques propagavam ao overlay
do Card. Corrigido o handler para reconhecer somente o próprio fundo; 92
testes de CardModal e prova visual de navegação/fechamento aprovados. Não reabrir
essa investigação sem nova evidência. Limites e hashes em
[evidência dos editores](evidence-u3-spec-editors-live-20261014.json).

#### Execução delimitada de 3c — base, confirmação e consumidores

Diagnóstico anterior a `58eb6399`, conservado para rastreabilidade:
`RulesTab`, `TechnicalRequirementsTab`, `IntegrationRequirementsTab`,
`ObservabilityRequirementsTab`, `ContractsTab` e `DecisionsTab` limpavam os campos
sem aguardar `onUpdate`; o pai usava a versão corrente e absorvia falhas. Esses
caminhos foram corrigidos e qualificados conforme a evidência das coleções.
`validationLive` já inclui Validation/FR/AC/Business/Contracts/Technical/Observability.
Os itens 1–4 e a prova das coleções no item 6 abaixo foram executados. Retomar
pelo item 5 e suas provas, sem reaplicar a correção dos seis editores.

| Ordem | Intervenção | Resultado verificável |
| --- | --- | --- |
| 1 | Acrescentar regressões nas suítes existentes de identidade de requisitos, Contracts, Decisions e edição estruturada de SpecModal. Exercitar a interação, incluindo confirmação assíncrona. | Reproduzir perda de rascunho após recusa e adoção silenciosa de uma revisão externa; distinguir comportamento observado de risco identificado apenas por leitura. |
| 2 | Fazer cada editor capturar identidade Board/Spec, entidade e base original ao abrir criação/edição. Passar a base explicitamente ao writer do pai e aguardar seu resultado. Reutilizar o tratamento nativo de conflito já aplicado a FR/AC. | Nenhuma gravação usa automaticamente a versão recebida por refresh. Falha conserva os campos; pendência impede envio duplicado; conflito requer leitura e revisão explícita. Não criar versão fictícia para entidade sem CAS próprio. |
| 3 | Propagar falha de escrita ao editor e separar escrita confirmada de falha posterior de leitura em `syncStructuredCollection`. Manter previews, confirmações de impacto, estados e comandos nativos. | Falha de refresh após create confirmado não oferece novo create. Se uma sequência tiver efeito parcial, reler o estado autoritativo e informar o que foi confirmado antes de qualquer nova tentativa; não declarar atomicidade que o caminho não possui nem repetir a sequência automaticamente. |
| 4 | Preservar rascunho quando o item ou referência selecionada desaparecer, distinguindo remoção de perda de permissão. Conferir efeitos de foco, chaves e respostas pendentes. | Item removido não é recriado silenciosamente; referência inválida exige correção. Perda de acesso limpa conteúdo protegido; troca de Board/Spec não reaproveita campos nem respostas de outra identidade. |
| 5 | Conferir os writers de cenários, classificação arquitetural e Project Structure, além dos controles editáveis restantes de Details. Registrar quais superfícies são apenas leitura. | Aplicar proteção somente onde faltar; não transformar leitores em editores nem criar um framework. A autoria de Project Structure ainda precisa de prova nativa em U4, conforme o limite já registrado. |
| 6 | Executar testes frontend afetados, tipos e lint; observar no browser rascunho local, mudança MCP, revisão explícita e gravação UI confirmada por leitura MCP. | Registrar família/ação efetivamente exercitada e limites. Uma prova visual de um editor não substitui os testes das seis famílias. Só então avançar para 3d. |

Casos mínimos das seis famílias: criar/editar com sucesso, recusa de negócio,
conflito nativo, refresh durante edição, item/referência removido, leitura
indisponível, perda de acesso, troca de identidade e escrita confirmada seguida
de falha de refresh. Cobrir caminhos próprios de Decisions (supersessão) e de
operações com impacto sem mudar sua autoridade. Não repetir testes de famílias
inalteradas já qualificadas, salvo quando a alteração no writer compartilhado
atingir seu comportamento.

Suítes de partida: `RequirementTabIdentity.test.tsx`, `ContractsTab.test.tsx`,
`DecisionVerificationEditor.test.tsx`, `SpecModal.structuredEditing.test.ts`,
`SpecModal.validationTabs.test.tsx`, `ScenarioVerificationMethodEditor.test.tsx`,
`ArchitectureClassificationAuthoring.test.tsx` e `ProjectStructureTab.test.tsx`.
Ampliar o teste apropriado ao comportamento; não criar uma suíte por item desta
tabela nem contar testes planejados como executados.

Após 3c, 3d conecta os consumidores ao watcher existente. A prova decisiva é
alterar Card/Delivery pelo MCP sem mudar `Spec.version` e observar Coverage,
capas e leitores abertos atualizarem, sem apagar um formulário em edição.
UV18–UV21 encerram U3; U4 continua obrigatório e distinto dessa prova de frontend.

**Diagnóstico histórico deste detalhamento em 15/10:** concluída a proteção do writer de
coleções e dos seis editores, com regressões e prova visual de BR. A leitura
dos consumidores aninhados confirmou a próxima intervenção, ainda no item 5:
`ArchitectureClassificationsPanel` usa `specVersion` nas chaves do formulário;
`DecisionReviewSection` monta a inspeção com `data.version` corrente, e o pai
zera `reviewData` durante recarga; `ScenarioVerificationMethodEditor` inclui
versão/método na chave no baseline. **Este último foi corrigido em `6fe9129f` e
qualificado em 15 testes do componente; falta integração/prova visual.**
Naquele diagnóstico, `TestScenariosTab` ainda limpava o formulário sem aguardar
seu writer; isso foi corrigido em `3057e84e`. Classificação e Decisions também
foram integradas e qualificadas posteriormente (5a/5b). Tests continua sem
refresh automático até concluir base, método e resultados incertos de 5c.
`ProjectStructureTab` já possui revisão/estado de conflito próprios, mas precisa
ser conferido contra atualização externa e perda de acesso; não substituí-los
pelo helper das coleções. Details exibe descrição/contexto por `MarkdownContent`.
Esses achados são conferência de código, não novas reproduções de runtime.

#### Próximas intervenções de 3c — sequência de execução

**Atualização vigente:** 5c concluído nos limites dirigidos de `28ec1871` e da
evidência de Tests referenciada no início. A tabela abaixo conserva as etapas
e seus aceites; continuar em 5d. U3 ainda depende da integração dos leitores e
de UV18–UV21; U4 não foi executado por este incremento.

**5a executado em `81af8c41`**, com 141 casos distintos e prova UI/MCP registrada
acima. A linha 5a abaixo conserva o escopo e aceite atendidos. Em 5b, componente,
pai e assinatura estão implementados em `7566db17`; a prova UI/MCP foi concluída
e registrada em `evidence-u3-decision-inspection-parent-20261015.json`.
Retomar por 5c, sem refazer 5b. Os 154 testes integrados complementam essa prova;
os limites do backend isolado continuam parte da qualificação U4.

Esta é a decomposição do item 5 existente, não uma ampliação do produto. Cada
linha deve fechar implementação, regressão pertinente e evidência antes de
ser qualificada. A assinatura de Decisions já tem aceite dirigido UI/MCP;
o quadro conserva a sequência e os critérios das frentes. A investigação abaixo foi
reconfirmada no checkpoint; falhas ainda sem reprodução não são bugs de runtime
declarados. Preservar as 222 provas distintas das coleções e os 15 testes do
componente de método, repetindo apenas os afetados pela integração.

| Ordem / superfície | Intervenção delimitada | Prova de saída |
| --- | --- | --- |
| 5a — classificação arquitetural em Integration | Concluído nos limites registrados: preservar `81af8c41`, a integração de `ArchitectureClassificationsPanel`/`ArchitectureClassificationAuthoring`, a base original e o replay exato. | `evidence-u3-classification-editors-20261015.json`: 141 casos finais e prova UI/MCP com arquitetura v2 → v3, revisão explícita e promoção de IR confirmada. Não repetir sem impacto novo. |
| 5b — inspeção de Decision | Concluído nos limites registrados: preservar a integração publicada de `DecisionsTab`/`DecisionReviewSection` e a prova UI/MCP da versão 8 → 9, revisão explícita, gravação e leitura do histórico. | `evidence-u3-decision-inspection-parent-20261015.json`, capturas inspecionadas e 154 testes integrados. Não repetir a prova sem impacto novo; ela não substitui o par instalado de U4. |
| 5c — cenários e método em Tests | Preservar confirmação e base de versão já qualificadas. O método agora usa a recarga protegida do pai; falta preservar seleção diante de cenário removido e integrar atualidade externa. Conferir mudanças sem bump da versão, resultado incerto e writers de vínculo/policy, sem CAS global. | A prova 10 → 12 qualifica recusa de versão, conservação do texto e revisão explícita. Restam os casos operacionais, reconciliação e método removido antes de assinar Tests; 92 testes backend e 46 frontend não encerram essa integração. |
| 5d — Project Structure e controles restantes | Conferir a revisão própria, `snapshotGeneration`, conflito e revisão explícita de `ProjectStructureTab` contra refresh externo e permissões. Reutilizar esse contrato; não adaptar a árvore ao helper das coleções. Conferir controles editáveis de Details sem reintroduzir edição de requisitos pela descrição. | Testes de leitura fora de ordem, versão/structure revision, rascunho e perda de acesso; registrar quais controles são somente leitura. A prova nativa de autoria da árvore continua no runtime compatível de U4, sem tratar a recusa do runtime antigo como sucesso. |

Depois de 5a–5d, executar 3d usando os metadados e o watcher existentes. Para
cada consumidor de Coverage, recursos, referências e dependências, registrar o
token que o invalida, leitura sob demanda e descarte de resposta antiga. O
critério central permanece UV18: alteração apenas de Card/Delivery, sem bump
da Spec, refletida no modal e nas capas abertas. Não adicionar polling pesado.

Encerrar U3 somente após UV18–UV21 integrados: atualização externa, formulário
sujo, perda de acesso, rede/retomada, troca de identidade, resultados/históricos
completos e conclusão composta com permissões próprias. As provas de UI devem
identificar ações realmente observadas, separadas dos mocks de componentes.
U4 permanece a qualificação do par exato instalado e da iniciativa nova até
Done/KG canonical; não converter uma prova isolada de editor em aceite final.

#### Histórico do roteiro — checkpoint publicado `8291a648` / `47434062`

Esta fila é histórica; a retomada vigente está no início deste documento. Não acrescenta
milestone, gate, avaliação ou critério à matriz UV01–UV24. O planejamento foi
sucedido pela execução parcial de 5c. Preservar U1/U2, 5a/5b, confirmação e
proteção da versão autorada de cenários, sem tratar esses incrementos como U3
concluído. O ledger distingue cada checkpoint documental de execução de produto.

**Incremento executado:** a substituição de cenários aceita a versão autorada,
confere o fence nativo e recusa conflito por REST; o formulário conserva a base,
recarrega sem apagar texto e exige revisão explícita. Prova UI/MCP: versão
10 → alteração concorrente → conflito UI → recarga/revisão → versão 12, com
um único cenário novo e a alteração concorrente preservada. 92 testes backend,
46 frontend e cinco do catálogo passaram; tipos/lint aprovados e closure ZERO.
Ver [base autorada de cenários](evidence-u3-scenario-authored-base-20261015.json).
Tests ainda não está no watcher. A atualidade operacional da substituição da
coleção foi qualificada posteriormente pelo snapshot, como registrado abaixo.
Restam qualificação/publicação da reconciliação e retenção locais, demais
writers e integração da aba. Não repetir a prova de versão nem reduzir esses
aceites à precondição nova.

**Conferência de planejamento de 15/10, sobre Core `868818e9` e Community
`3c4df986`, ambos publicados:** o relatório local
`PULSE_REFACTOR/.validation-v040/spec-unified-20261014/u3-scenario-operational-before.xml`
já contém a reprodução de 5c.1: dois casos, um aprovado (status) e um falho
(task_link), sem erros. Após remover o vínculo pelo serviço nativo, a substituição
da coleção antiga restaura `linked_task_ids`, mesmo com a mesma versão esperada.
É uma falha observada na fronteira de serviço; não uma falha de todas as
operações de cenário. A reprodução foi sucedida pela correção descrita abaixo;
o teste integra `tests/test_scenario_verification_method.py`.

Para fechar 5c.1, preservar a versão autorada já implementada e acrescentar
proteção delimitada da base operacional da coleção: identidade e revisão usadas
na leitura devem ser verificadas no writer, dentro da unidade de trabalho,
inclusive quando `Spec.version` não mudou. Primeiro conferir se a revisão pública
existente identifica esse estado e é obtida junto da leitura; não reutilizar um
token lido depois do rascunho. Se ela não oferecer essa garantia, definir a
precondição específica da coleção na porta pública, sem criar CAS global.
Não incrementar a versão semântica para disfarçar mudança operacional nem trocar
a exclusão por uma cascata diferente. Aceite: o vínculo removido continua removido,
o status continua preservado, a escrita conflitante não deixa efeito parcial e
uma tentativa explicitamente revista consegue salvar. Estender a prova aos
transportes afetados e à UI antes de habilitar Tests no watcher.

**Execução posterior desta fronteira:** `scenario_snapshot` é calculado junto da
resposta REST da coleção, conservado na UI e comparado após refresh sob o fence
escalar público. Não usa a revisão do watcher lida separadamente. Um conflito
sem bump exige recarga/revisão; writers nativos de add/corpo/método/delete capturam
a base da própria leitura. 102 testes backend e 48 frontend aprovados, tipos/lint
aprovados e prova UI/MCP 12 → 12 → 13: mudança operacional preservada, rascunho
retido e uma única criação após revisão. Ver a evidência do snapshot operacional.
Conferir ainda writers operacionais e os demais casos 5c.2–5c.4 antes de ligar
Tests ao watcher; esse incremento não encerra U3/U4. A ordem restante é a tabela
abaixo; seus aceites e UV01–UV24 delimitam o término do trabalho.

| Ordem / dependência | Trabalho e fronteira exatos | Aceite para encerrar |
| --- | --- | --- |
| 5c.1 — antes de ligar Tests ao watcher | Reproduzir concorrência na substituição de cenários usada por `scenarioWriteGuard.ts` → `api.updateSpec` → `SpecUpdate` → `SpecService.update_spec`. Fechar a proteção da base nessa fronteira, usando o fence público existente; delimitar a alteração aos cenários. Conferir seus callers de autoria e status antes de mudar o contrato. Não substituir a exclusão atual pela exclusão MCP em cascata: os efeitos são distintos. | Duas escritas sobre a mesma base não apagam silenciosamente a primeira; conflito não deixa efeito parcial. REST/MCP afetados preservam permissões, Draft/content lock, vínculos e regras de evidência. Controle somente no cliente não conta como prova de concorrência no servidor. |
| 5c.2 — após definir a fronteira de 5c.1 | Em `TestScenariosTab`/`SpecModal`, capturar identidade e base do rascunho; propagar leitura atual ao `ScenarioVerificationMethodEditor`; preservar seleção quando cenário for removido, impedindo escrita até resolução. Guardar geração/identidade nas releituras de `onSaved` e `onSpecRefreshed`. Conferir link/unlink, controle de cobertura e Policy Compliance montados nessa aba, sem mudar suas permissões. | Refresh conserva texto e seleção; revisão da base é explícita; respostas antigas não restauram outra Spec nem versão anterior. Revogação retira conteúdo protegido; indisponibilidade não habilita escrita sobre cache apresentado como atual. |
| 5c.3 — com 5c.1/2 | Distinguir recusa definitiva, gravação confirmada com falha de releitura e resultado desconhecido por transporte. Conservar identidade/payload da tentativa. Reusar replay somente onde o contrato nativo o suporta; caso contrário, reconciliar por leitura autoritativa antes de permitir nova operação. Não criar outra entidade ou histórico de aprovação. | Timeout não gera segundo cenário; sucesso seguido de falha de leitura não oferece novo create. Se a leitura não permite determinar o resultado, a UI informa a incerteza e não repete a escrita automaticamente. |
| 5c.4 — saída de 5c | Ampliar `SpecModal.activity.test.tsx`, `SpecModal.validationTabs.test.tsx` e `ScenarioVerificationMethodEditor.test.tsx` nos casos afetados; acrescentar regressão nativa da fronteira alterada. Só então habilitar Tests no watcher. Browser aberto + mutação MCP: rascunho, conflito, revisão explícita, gravação UI e confirmação MCP. | Tipos e testes afetados aprovados; captura realmente inspecionada; ledger identifica operação, versões, resultado e limites. Os 43 passes já registrados não encerram esta integração. |
| 5d — após 5c | Conferir `ProjectStructureTab.tsx`: `snapshotGeneration`, versão da Spec, `structure_revision`, replay e revisão explícita. Integrar invalidação externa sem remontar autoria nem substituir o contrato da árvore pelo writer de coleções. Classificar os controles restantes de Details em leitura/escrita. | `ProjectStructureTab.test.tsx` e testes dos controles afetados cobrem recarga, resposta atrasada, rascunho e acesso. Não reintroduzir edição de requisitos em Details. Prova nativa de autoria da árvore no par compatível fica explicitamente em U4. |
| 3d — após proteger os editores | Ligar Coverage, recursos, referências e dependências restantes ao watcher existente. Para cada leitor, registrar token autoritativo, condição de visibilidade e descarte de resposta antiga. Reusar CardModal/Delivery, SpecsPanel e Kanban já implementados. | Alteração MCP somente em Card/Delivery, sem bump de `Spec.version`, atualiza Coverage e capas abertas; painel fechado consulta ao abrir. Sem polling periódico de Coverage completo e sem novo cálculo de gate no frontend. |
| UV18–UV21 — fechamento de U3 | Exercitar atualização externa, formulário sujo, troca de identidade, rede/retomada e revogação; inspecionar resultados individuais, histórico completo e conclusão composta com permissões distintas. Reusar provas existentes que cubram exatamente o caso. | UI e MCP sobre o mesmo runtime, ações observadas e limites registrados. Nenhum consumidor previsto pendente; testes/tipos/build pertinentes aprovados. Commit/push de U3 com ledger. |
| U4 — após U3 | Executar o roteiro instalado abaixo: regressões, catálogo, closure, wheels/frontend pareados, home isolada nova, paridade byte a byte e processo posterior à instalação. Tramitar iniciativa mock nova até Cards/Test/Spec Done e consultar/consolidar KG. | Matriz UV01–UV24 preenchida com evidências reais e limites; oito budgets ZERO; percentuais aplicáveis comprovados, KG canonical, ledger e commits/pushes finais. |

**Regra de execução para 5c.1:** a investigação tem inventário finito: substituição
da coleção, criação, edição de corpo/método, exclusão, vínculo/desvínculo e status
de cenário. Registrar entrada pública, autorização, versão/fence, efeitos e
commit de cada caminho tocado. `update_test_scenario` já aceitava
`expected_spec_version`; `SpecUpdate` agora o aceita somente com cenários, e a
UI envia sua base original. Isso não prova atualidade de status/vínculos sem
bump da versão. Reutilizar
garantias comprovadas, sem apresentar add/delete como protegidos por versão só
porque edit está protegido. Não ampliar essa correção para CAS global. Se a
correção exigir mudar cascata, autoridade ou efeito de evidência/histórico,
aplicar a investigação e decisão do pacote; não fazer essa mudança como efeito
colateral da sincronização da UI.

Cada incremento termina com resultado reproduzível, ledger e commit/push nos
repositórios alterados. Um teste falho ou aceitação não exercitada mantém o item
aberto. Novas melhorias de produto não entram nesta fila; somente correções de
falhas que impeçam os aceites existentes. Nenhuma etapa remove avaliações ou
reduz seus critérios, severidades, thresholds ou permissões.

#### Histórico da sequência de fechamento anterior

Esta tabela conserva a fila anterior à conclusão de 5d e Coverage/References.
Não usá-la como fila ativa. A retomada vigente está em “Retomada executável de
3d”, nesta seção; as provas e limitações anteriores continuam preservadas.

| Ordem / item existente | Ação concreta | Saída registrada no ledger |
| --- | --- | --- |
| 1 — U3/2e.1 | Concluído: preservar a prova Knowledge em `evidence-u3-knowledge-targets-20261014.json`. | Revisões 1 → 2 → 3, rascunho preservado, revisão explícita e escrita UI lida pelo MCP. Nenhuma repetição necessária sem alteração. |
| 2 — U3/2e.2–3 | Implementado e qualificado nos limites de `evidence-u3-resource-readers-live-20261014.json`. Preservar Targets e leitores. | Architecture/KB observadas após MCP e captura inspecionada; Project Structure/dependências em testes frontend. Autoria de árvore nativa recusada pelo contexto no runtime anterior não é prova positiva e permanece no ensaio U4. |
| 3 — U3/SpecModal | Retomar sobre `28ec1871`: concluir o diff local de Project Structure e sua integração/Details (5d). Preservar 5a/5b/5c, editores e coleções qualificados. Depois invalidar Coverage/recursos/referências/dependências restantes pelo watcher existente (3d). | Rascunho e base preservados; mudança só de Card atualiza Coverage; respostas de outra Spec descartadas. Testes dos editores e consumidores efetivamente alterados e prova browser/MCP. |
| 4 — U3/UV18–21 | Conferir o conjunto aberto: atualização MCP, acesso revogado, rede interrompida/recuperada, troca de contexto, histórico e ação de conclusão. Reusar provas parciais pertinentes. | UI legível e funcional, sem cache protegido após revogação, sem submissão duplicada, sem perda silenciosa de edição. U3 pode ser encerrado somente aqui. |
| 5 — U4/UV01–24 | Qualificar o par exato de wheels/frontend e executar iniciativa mock nova até Cards/Test/Spec Done e KG canonical, conforme roteiro abaixo. | Matriz final com provas reais UI/MCP, paridade byte a byte, processo novo, oito budgets ZERO, commits/pushes pareados. |

O planejamento foi sucedido pelos checkpoints descritos no estado vigente.
Não reiniciar etapas concluídas nem uma rodada genérica de assessment. Uma lacuna
crítica reproduzida pode exigir correção no item afetado; melhorias opcionais
ficam fora do escopo. Não declarar pronto com teste falho ou critério não exercitado.

#### Integração delimitada da atualização externa

Esta tabela concretiza os passos 4–6; não amplia os critérios UV18–UV20.

| Mudança observada | Consumidor/ação | Proteção e prova |
| --- | --- | --- |
| Revisão da Spec ou resultados do ciclo | `SpecModal`, `SpecValidationPanel`, histórico e allowed transitions | Recarregar leituras afetadas sem remontar modal/formulários. Resposta de outra Spec/edição é descartada; histórico fechado só recarrega ao abrir. |
| Card/alocação/Delivery sem mudança de `Spec.version` | `CardModal`, `KanbanBoard`, `SpecsPanel`, Coverage/Delivery e referências visíveis | Invalidar projeções pertinentes a partir das revisões autoritativas, sem calcular gates ou percentuais no frontend. Provar convergência por mutação MCP. |
| Formulário de avaliação ou Checklist em edição | `SubmitSpecValidationModal`, `SpecDecompositionPanel`, `SpecChecklistPanel` | Conservar campos e base original; mostrar conflito e exigir revisão explícita. Não usar `workspaceReloadKey` para remontar formulário sujo. Testar também execução de Checklist aberta e resposta atrasada. |
| Revisão de autoridade ou acesso negado | Watcher, `usePermissions` e leitores protegidos | Invalidar cache de permissões e cancelar respostas antigas; reautorizar antes de voltar a exibir conteúdo. O TTL atual de 60 s não comprova revogação. Preservar rascunhos ainda autorizados; retirar conteúdo cujo acesso foi perdido. |
| Novo Card/Spec ainda não assinado | Listagens existentes de Board/Spec | Tokens de coleção integrados em SpecsPanel e Kanban, descoberta comprovada parcialmente por UI/MCP. Preservar essa implementação; completar os cenários integrados sem novo metadado ou varredura de todos os objetos. |
| Perda de conexão, troca de Board, retorno de foco | Watcher compartilhado e consumidores | Cancelar geração antiga, sinalizar dados desatualizados, retomar sem polling duplicado e sem repetir submissão. |

O aceite no navegador exige duas superfícies simultâneas: UI aberta e mutação
MCP autorizada sobre o mesmo runtime isolado. Conferir explicitamente a URL da
API: `.env` do frontend pode apontar ao runtime do usuário apesar do proxy local.
Registrar versões/processos, mutação, revisão observada e atualização visual;
um teste isolado do watcher ou screenshot estático não encerra a sincronização.

#### U4 — comprovar o percurso completo

1. Regressões afetadas, frontend/tipos/build, catálogo e closure de checkout e
   wheels. Oito budgets ZERO. Instalar par exato em ambiente isolado/home nova;
   comparar Python/frontend byte a byte e iniciar processo posterior ao build.
2. Nova iniciativa mock por UI e MCP: Ideação → Refinamento → Spec com Cards
   criados em Draft → conclusão unificada → execução → Cards/Test/Spec Done.
   Sem Decision artificial, reabertura só para criar Cards ou novo parecer
   desnecessário no início. Provas externas devem ser observadas; 100% apenas
   onde aplicável. Não reduzir gates, alterar policies ou inventar resultados.
3. Consultar/consolidar KG e verificar estado canonical/proveniência, sem usar
   atraso da projeção como autoridade. Registrar IDs, recibos, screenshots,
   superfícies exercitadas e evidência por UV01–UV24. Commit/push final pareado.

**Fim do escopo:** U1–U4 e matriz UV01–UV24 comprovados, ledger atualizado,
checkout publicado e limitações explícitas. A simplificação mantém todas as
avaliações, critérios, severidades, thresholds, permissões e históricos; reúne
o ponto de conclusão. Não inclui mudanças de Ideação/Refinamento, verificação
da entrega, percentuais, política do Board ou lifecycle canonical do KG.

## 7. Matriz mínima de aceite

| ID | Ensaio | Resultado exigido |
| --- | --- | --- |
| UV01 | Criar Task e Test em Draft pela UI e MCP, vincular cenários e allocation. | Not Started e vínculo nativo; sem ciclo de reabertura. |
| UV02 | Criar diretamente Started/In Progress ou iniciar Card com Spec imatura. | Negado inclusive por caminhos alternativos; sem efeitos parciais. |
| UV03 | Spec completa sem Decision; com Decision ativa sem plano; com plano por inspeção. | Respectivamente não bloqueia por existência, bloqueia por plano, aceita planejamento sem teste artificial. |
| UV04 | Spec Validation passa; decomposição falta/rejeita/tem média insuficiente. | Não promove. Resultado de qualidade permanece distinto do estado agregado. |
| UV05 | Decomposição passa; uma dimensão da Spec falha ou recommendation=reject. | Não promove; scores altos alheios não compensam. |
| UV06 | Duas aprovações de decomposição, request_changes e rejeição ativa. | Mesma média/regra do baseline; rejeição não some com aprovação posterior. |
| UV07 | Lint ausente versus aceito com findings; Checklist off/advisory/blocking. | Preservar efeito de cada configuração e separar ausência de achados. |
| UV08 | Board muda Checklist após snapshot; snapshot obrigatório ausente. | Edição usa snapshot; ausência falha fechada. |
| UV09 | Policy Compliance falha/indisponível; cobertura/arquitetura/code evidence bloqueia. | Mesmos gates e razões; nenhum caminho de promoção contorna o componente. |
| UV10 | Require Spec Validation desligado; auto_validate/skip de decomposição nas combinações admitidas. | Apenas componentes aplicáveis exigidos; transição direta usa predicado comum. |
| UV11 | Ator só autor, só validador, só avaliador; outro Board; contexto crítico inválido. | Sem ampliação de autoridade. Ator sem uma permissão não envia ambos os pareceres. |
| UV12 | Avaliadores diferentes preparam resultados; validador conclui. | Autoria independente preservada; sem cópia/reenvio obrigatório nem impersonação. |
| UV13 | Reabrir Draft e avaliar nova edição; revisão técnica na mesma edição. | Previous/Current seguem contrato atual; histórico íntegro e rejeições delimitadas por edição. |
| UV14 | Mudança concorrente de Spec, Card/alocação, head de parecer ou autoridade entre preflight e commit. | Conflito/falha fechada; nenhuma promoção ou escrita parcial. |
| UV15 | Retry idêntico, chave com payload diferente, falha antes do commit, replay após perda de acesso. | Recibo estável, conflito, rollback e autorização de leitura, respectivamente. |
| UV16 | Conclusão conjunta com parecer válido negativo. | Histórico dos pareceres preservado, Spec Approved; erro técnico não vira parecer negativo. |
| UV17 | Planejamento validado com predecessor incompleto; iniciar após predecessor concluir. | Validação não exige predecessor Done; início bloqueia/libera conforme regra real sem novo parecer desnecessário. |
| UV18 | Registrar via MCP parecer/Checklist/lint e mudar Card/Delivery com modal e Board abertos. | Conteúdo, Current, barras e ações convergem sem Refresh manual, inclusive sem bump de Spec.version. |
| UV19 | Respostas fora de ordem, troca rápida de Spec, aba oculta/reaberta, perda de rede. | Sem contaminação entre Specs, regressão de versão ou polling duplicado; recuperação visível. |
| UV20 | Alteração externa durante formulário sujo; revogação de acesso. | Campos não sobrescritos, conflito explícito, nenhum dado continua exposto indevidamente. |
| UV21 | Histórico completo, pinpoints, teclado, foco, loading e erro/retry na Validation. | Padrão visual existente, avaliações separadamente legíveis e ação principal compreensível. |
| UV22 | UI/REST/MCP chamam promoção direta e composta. | Mesmo efeito de gates, permissões, retorno e histórico. Catálogo/documentação refletem contrato único. |
| UV23 | Nova iniciativa mock percorre Ideação → Refinamento → Spec → Cards/Test → Done. | Todos os gates reais satisfeitos, provas observadas, implementação/verificação 100% quando aplicáveis, Test sem implementação mantém só verificação. |
| UV24 | Consulta KG após fechamento e consolidação. | Estado/proveniência coerentes; projeção atrasada não substitui autoridade nem concede aprovação. |

Ampliar suítes existentes em vez de duplicar testes que espelhem a implementação:
Core `test_spec_evaluation_currentness.py`, `test_spec_evaluation_rest_parity.py`,
`test_spec_validation_current_input.py`, `test_spec_validation_native_history.py`,
`test_spec_validation_pinpoint_snapshots.py`, `test_card_lifecycle.py`,
`test_quality_assessment_lifecycle.py`, `test_checklist_governance.py` e suites
de decisões/cobertura afetadas. Acrescentar testes integrados específicos para
operação composta, concorrência e replay que ainda não tenham cobertura.

Frontend: ampliar `SpecModal.validationTabs.test.tsx`, `SpecModal.qualityTab.test.tsx`,
`SpecValidationPanel.test.tsx`, `SpecValidationHistoryPanel.test.tsx`, testes de
criação de Cards e dos consumidores de refresh; testar API client e hook de
sincronização. Executar build/checagem de tipos e ensaio real no browser. Mock de
componentes não substitui a conferência visual nem a mutação MCP concorrente.

## 8. Fechamento e handoff

Antes de testes de comportamento, provar qual código será importado. Usando a
instalação, comparar byte a byte todos os `.py` instalados com ambas as árvores
src; usando checkout, fixar `PYTHONPATH=core/src;community/src` com caminhos reais
dos dois repos e registrar resolução. Para o E2E instalado, conferir também o
frontend empacotado e reiniciar processo posterior aos arquivos instalados.

Executar `okto-pulse-saas-closure`: todos os budgets continuam ZERO
(`import_boundary_baseline`, `singleton_baseline`, `dependency_temporary_exceptions`,
`graph_runtime_compatibility`, `rebuild_artifact_compatibility`,
`community_private_reach_ins`, `community_adapter_bridges`, `af35_relational_residue`).
Regenerar catálogo com `python -m okto_pulse.core.mcp.tools_catalog_generator`
e passar `tests/test_mcp_tools_catalog_drift.py`; nunca editar catálogo à mão.
Nenhum router, ORM, filesystem, scheduler, HTTP client ou mecanismo Grafx entra
no Core; adapters Community consomem portas públicas.

Registrar por milestone: commits Core/Community, arquivos e contratos alterados,
comandos de testes/resultados, limitações, instalação/runtime quando aplicável,
e próximo passo exato. No E2E, registrar IDs/edições e evidências UI/MCP; não
alterar policies, scopes, skips ou percentuais para obter verde. Testes externos
mockados precisam de execução observada para gerar prova autenticada.

**Sequência fechada de execução a partir deste checkpoint:**

1. Preservar a matriz finita U2 qualificada em §6.2 e as correções de diagnóstico.
   Os 208 casos mapeados não são 208 novos testes; distinguir evidência anterior
   de nova execução e repetir somente regressões pertinentes a alterações futuras.
2. Preservar U3/2e.3, Knowledge/Targets e 2a–2d já qualificados nos limites
   registrados. Não repetir as mutações mock. O percurso instalado de Project
   Structure continua parte do U4; o runtime antigo não o qualificou.
3. Preservar Resources qualificado conforme §6.2, além dos
   editores/coleções, Tests, Project Structure,
   Coverage/References e Dependencies. Não repetir o trabalho
   publicado nem as mutações dos fixtures sem impacto novo.
4. Qualificar ação composta, resultados/histórico e sincronização U3 por
   testes frontend e browser/MCP: conflito, troca de entidade, rede e autoridade.
   Não promover o milestone com consumidor previsto ainda sem atualização.
5. Executar U4: regressões/closure, par instalado comprovado, iniciativa mock
   nova até Cards/Test/Spec Done, métricas aplicáveis em 100% e consulta/KG canonical.
   Atualizar UV → evidência → resultado → limite, ledger e commits/pushes.

Não refazer composição pública, API de revisões,
coleções ou SpecsPanel já implementados. Esta revisão documental confirmou os
checkpoints publicados e delimitou a integração restante de 3c–3d;
não acrescentou implementação nem reinstalou o runtime do usuário.
**Critério de conclusão:** U1–U4 e
UV01–UV24 demonstrados, ledger atualizado e par de repositórios publicado. Esta
sequência mantém o escopo acordado; os incrementos implementados e as provas
efetivamente obtidas permanecem identificados no ledger.
