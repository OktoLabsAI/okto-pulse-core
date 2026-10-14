# Validação unificada do planejamento da Spec

Plano de 14 de outubro de 2026. **U1 implementado; U2 parcial; U3 parcial; U4 pendente.**
Revisão de planejamento sobre Core `1ce49fda` / Community `93769ff1`,
ambos publicados em `feature/v0.4.0`. Esse checkpoint foi documental; a execução
foi retomada posteriormente. U2 ganhou 32 casos de paridade de cobertura REST/MCP
e correção da projeção de resultado malformado: ver
[evidência parcial U2](evidence-u2-read-write-parity-20261014.json).
As demais famílias de gates, U3 completo e U4 permanecem pendentes.

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
| U2 — fechar paridade | Confrontar leitura, conclusão composta, transição direta e ações permitidas para os mesmos dados/policies. Completar somente casos não demonstrados de gates, permissões, concorrência e replay. | Resultados nativos e estado persistido equivalentes nas entradas REST/MCP; nenhum bypass ou efeito parcial. Detalhamento em §6.2 e UV04–UV17/UV22. |
| U3 — completar integração | Expandir o watcher existente para listas/novos objetos, Cards/Delivery, Coverage e demais leitores previstos; preservar editores e revalidar acesso. Qualificar resumo, formulário e histórico existentes. | Mutação MCP observada na UI aberta, inclusive mudança só de Card; formulário preservado, perda de acesso tratada e recuperação de rede comprovada. UV18–UV21. |
| U4 — qualificar entrega | Executar regressões e auditoria, construir/instalar o par exato isoladamente e tramitar uma iniciativa mock nova até Done e KG canonical. | Matriz UV01–UV24 com evidências, oito budgets ZERO, paridade do pacote instalado, ledger e commits/pushes completos. |

Cada linha usa as tarefas já discriminadas em §6.1/§6.2; não cria milestones
adicionais. O fim é U4 comprovado. Melhorias opcionais encontradas durante o
trabalho não ampliam este escopo. Divergência material de autoridade/gate/histórico
segue a investigação do pacote antes de qualquer mudança semântica não autorizada.

### Histórico dos checkpoints

Base do checkpoint de UI: Core `dee6610b` / Community `fc92616a`, publicado.
Avanço posterior de Checklist: Community `264610a1` e registro pareado no ledger.
Último par com composição e revisões leves qualificados:
Core `3dce9233` / Community `204c7f4a`.
Conclusão composta, replay e manifesto já possuem implementação e regressões.
A leitura integrada e a consulta leve de revisões estão implementadas.
O bloqueio arquitetural do WIP foi corrigido pelo catálogo público; closure
voltou a ZERO nos oito budgets. Provas e par publicado constam no ledger.
Há resumo, histórico e formulário composto na interface, ainda em qualificação.
Admissão sem Checklist já tem recusa acionável comprovada; refresh da Validation
preserva o rascunho do Checklist. Evidência em
[ensaio parcial U3](evidence-u3-checklist-20261014.json).
Validation/conclusão já consomem revisões: parecer MCP apareceu sem Refresh e
mudança concorrente preservou rascunho com revisão explícita da base. Ver
[prova parcial de sincronização](evidence-u3-live-validation-sync-20261014.json).
Expansão aos demais consumidores e ensaio final permanecem pendentes.
A seção 6.2 define a sequência concreta sem ampliar o escopo.

Base: Core `57599196174a247510cef6b299ee51c0f50ce10f` e Community
`34fcf7ec8a617151df32ca403478cd39594cee56`, branch `feature/v0.4.0`.
Os dois repositórios estavam limpos e publicados antes deste planejamento.

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

**Checkpoint atual:** Core `1ce49fda` / Community `93769ff1`, branch
`feature/v0.4.0`, publicados antes desta revisão documental. Inclui a prova
parcial de sincronização indicada no cabeçalho; **não encerra U2/U3 nem qualifica
release instalável**. O checkpoint de UI anterior foi
Core `dee6610b` / Community `fc92616a`.
O reach-in privado do checkpoint antigo foi corrigido no par
`3dce9233` / `204c7f4a`, cuja closure confirmou oito budgets ZERO. Não repetir
essa correção. O WIP posterior precisa de nova qualificação.

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
Validation; a expansão aos demais consumidores e a paridade MCP completa seguem
pendentes, conforme a tabela abaixo.
`closure-u3-interface.json` registra oito budgets ZERO e nenhum finding/drift.
Essas evidências não encerram o milestone nem substituem o E2E.

| Frente | Já existe | Falta comprovar ou implementar |
| --- | --- | --- |
| U1 | Cards Not Started em Draft e remoção da exigência de existir Decision. | Percurso real UI/MCP no U4; não refazer a implementação. |
| U2 — conclusão | Payload composto, permissões independentes, pareceres/históricos nativos, promoção condicionada, recibo/replay e manifesto obrigatório. | Paridade completa de todos os caminhos com os gates aplicáveis. |
| U2 — leitura | `include_planning`, 17 componentes, composição pública sem escrita, revisões leves em lote, permissões por componente, paginação REST/MCP e cliente tipado. Regressão de opiniões: 48 passes. Admissão sem Checklist comprovada por REST/browser e rollback nativo. | Completar matriz de paridade dos gates e superfície MCP. |
| U3 | Resumo, histórico Current/Previous, contribuição independente, payload opcional, registro/promoção distintos e revisão explícita da base. Watcher integrado à Validation/conclusão; refresh de permissões com proteção de respostas antigas. Provas MCP/UI de atualização e rascunho. | Expandir aos demais consumidores, proteger os demais formulários e comprovar descoberta de objetos novos, Card sem bump da Spec, revogação/offline/reconexão no fluxo completo. |
| U4 | Regressões parciais registradas no ledger. | Wheels, runtime compatível, browser e iniciativa nova até Done/KG; matriz UV01–UV24. |

#### U2 — encerrar backend e contratos antes da interface

Executar na ordem abaixo. Os itens detalham U2.3/U2.4, sem novos milestones.

1. **Qualificar o WIP de leitura e histórico.** A composição arquitetural já
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
   Qualificar também a recusa de admissão sem Checklist: fixture com realm,
   diagnóstico acionável REST/MCP e nenhuma alteração em estado, versão, edição,
   avaliações, snapshots, atividade ou eventos. Reiniciar somente o runtime
   isolado para observar a correção pelo browser; processo antigo não é prova.
   Saída: UV04–UV17/UV22 pertinentes com persistência real, sem escrita parcial.
3. **Consumir os metadados leves já implementados.** O contrato por Board
   aceita 1–50 IDs Spec/Card e retorna revisões, sem corpos de avaliações ou
   execução de gates. Testes existentes demonstram custo limitado e mudanças
   sem bump da Spec; completar a prova integrada, sem recriar a API.
   `include_planning` executa gates: reservá-lo para carga explícita/invalidação,
   nunca para polling. Revisão de autoridade não equivale a revisão da coleção:
   ao integrar a listagem, verificar também descoberta de novos Cards/Specs
   relevantes pelos caminhos existentes, sem presumir que IDs ainda não
   assinados serão descobertos pelo hook.
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
| 4 | `useWorkspaceRevisions`, `SpecModal`, `SpecsPanel` e consumidores Card/Delivery visíveis | Expandir o hook já integrado à Validation/conclusão. Deduplicar por cliente autenticado/Board; invalidar dados afetados, inclusive allowed transitions e Coverage sem mudança de Spec.version. Painéis fechados carregam sob demanda. UV18/UV19. |
| 5 | Formulários da Validation, inclusive Checklist, e submissão composta | Preservar campos e manifesto original; apresentar mudança externa e revisão explícita antes de aceitar nova base. Não incrementar chaves que remontam formulários sujos a cada polling. Revogação limpa conteúdo protegido. UV20. |
| 6 | Testes dos componentes e browser contra runtime isolado | Observar mutação MCP e convergência visual, histórico completo, foco, navegação, offline/reconexão e perda de acesso. Não substituir esse ensaio pelos testes isolados do watcher. UV18–UV21. |

A revisão explícita de conflito apresenta a necessidade de reler a edição e
seus resultados; preservar o rascunho não significa validar automaticamente
seus julgamentos contra a nova base. O usuário pode descartar o rascunho ou
revisá-lo e confirmar a nova base. A confirmação gera nova tentativa, sem
reutilizar a identidade de um request cujo conteúdo mudou. Não criar fusão
semântica automática de pareceres.

Não encerrar U3 enquanto existir qualquer uma destas lacunas: consumidor previsto
sem atualização; botão de conclusão que não oferece o payload composto permitido;
histórico reduzido a título/status; frontend recalculando gate; atualização que
apaga rascunho; dados protegidos mantidos após revogação; build ou teste pertinente
vermelho. O trabalho de U4 permanece obrigatório após esse fechamento.

#### Integração delimitada da atualização externa

Esta tabela concretiza os passos 4–6; não amplia os critérios UV18–UV20.

| Mudança observada | Consumidor/ação | Proteção e prova |
| --- | --- | --- |
| Revisão da Spec ou resultados do ciclo | `SpecModal`, `SpecValidationPanel`, histórico e allowed transitions | Recarregar leituras afetadas sem remontar modal/formulários. Resposta de outra Spec/edição é descartada; histórico fechado só recarrega ao abrir. |
| Card/alocação/Delivery sem mudança de `Spec.version` | `CardModal`, `KanbanBoard`, `SpecsPanel`, Coverage/Delivery e referências visíveis | Invalidar projeções pertinentes a partir das revisões autoritativas, sem calcular gates ou percentuais no frontend. Provar convergência por mutação MCP. |
| Formulário de avaliação ou Checklist em edição | `SubmitSpecValidationModal`, `SpecDecompositionPanel`, `SpecChecklistPanel` | Conservar campos e base original; mostrar conflito e exigir revisão explícita. Não usar `workspaceReloadKey` para remontar formulário sujo. Testar também execução de Checklist aberta e resposta atrasada. |
| Revisão de autoridade ou acesso negado | Watcher, `usePermissions` e leitores protegidos | Invalidar cache de permissões e cancelar respostas antigas; reautorizar antes de voltar a exibir conteúdo. O TTL atual de 60 s não comprova revogação. Preservar rascunhos ainda autorizados; retirar conteúdo cujo acesso foi perdido. |
| Novo Card/Spec ainda não assinado | Listagens existentes de Board/Spec | Provar descoberta pelo fluxo de atualização da coleção; lote de IDs conhecidos não basta. Reutilizar mecanismos existentes e documentar qualquer lacuna antes de definir metadado adicional. |
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

**Próxima ação de execução:** completar a paridade U2 das famílias de gates ainda
não demonstradas em conjunto, sem repetir os 32 casos de cobertura; expandir
revisões além da Validation/conclusão,
incluindo listas/novos objetos, Cards/Delivery, Coverage e demais leitores,
protegendo os demais editores e comprovando os cenários restantes de §6.2;
depois executar U4. Não refazer composição pública ou API de revisões já provadas.
**Critério de conclusão:** U1–U4 e
UV01–UV24 demonstrados, ledger atualizado e par de repositórios publicado. Esta
sequência mantém o escopo acordado; os incrementos implementados e as provas
efetivamente obtidas permanecem identificados no ledger.
