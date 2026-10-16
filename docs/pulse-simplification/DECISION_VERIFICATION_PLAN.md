# Verificação proporcional de decisões

Assessment de 10 de outubro de 2026. Implementação DV1–DV4 concluída e qualificada;
estado e evidências de cada milestone no IMPLEMENTATION_LEDGER.md.
Alteração de schema com breaking change expressamente autorizada pelo usuário
em 10 de outubro de 2026; não é uma decisão pendente para executar este plano.
A v0.4.0 ainda não foi publicada e já prevê breaking change. Implementar somente
o contrato nativo final, sem compatibilidade, conversores, backfills ou migrações.

Decisões devem demonstrar aderência à escolha aprovada. Quando seus efeitos já
estão expressos em requisitos, a verificação deve reutilizar as provas desses
requisitos. Quando a decisão delimita escopo ou outra condição documental, deve
aceitar uma inspeção rastreável sobre a entrega. Nenhuma dessas situações exige
inventar código, uma task ou um teste exclusivo para a Decision.

## Conferência da implementação atual

Base inspecionada: Core `0de0a08597c55dbb050ce2d59395085f791901b9` e Community
`d4c1caa7e5702b263ee158a47dd038290035464b`, ambos em `feature/v0.4.0`.
Conferência estática dos caminhos abaixo e evidência operacional SIM-05 já
registrada. Não foram executados novos testes de comportamento neste assessment.

| Achado | Evidência no código | Consequência |
| --- | --- | --- |
| Decision representa escolha contextual, com rationale, contexto, alternativas e supersedência; não possui contrato de verificação. | Core `models/schemas.py`, `Decision`. | Não há distinção entre uma decisão de escopo e uma decisão com efeito executável. |
| Toda Decision ativa entra no inventário de entrega. | Core `domain/delivery_inventory.py`, `COLLECTIONS` e `_collection_obligations`. | A decisão contextual recebe o mesmo tratamento de implementação das obrigações executáveis. |
| O inventário suplementar exige atribuição a Card normal/bug. Um único link vira responsabilidade integral. | Core `domain/effective_delivery_inventory.py`, `resolve_effective_delivery_inventory`. | Vincular uma task apenas para passar o gate cria responsabilidade artificial. |
| O fechamento exige implementação aceita e verificação que nomeie seus IDs exatos. | Core `domain/effective_delivery_coverage.py`, `evaluate_effective_delivery_coverage`. | Uma inspeção documental não tem um caminho próprio para uma Decision sem implementação. |
| Existe também um gate anterior de vínculo direto a task para cada Decision ativa. | Core `services/main.py`, `check_decisions_coverage`; `application/use_cases/allowed_transitions.py`. | Corrigir somente o fechamento deixaria a burocracia no planejamento. |
| Inspeção, análise estática e demonstração já têm modelos e admissão concreta. | Core `domain/verification_report.py`; Community `adapters/test_evidence.py`, `CommunityTestEvidenceWriteVerifier` e `CommunityTestVerificationReportIssuer`. | O problema não é ausência de métodos não automatizados. Atualmente eles se vinculam a cenário, critérios e Test Card. |
| A UI e analytics contam Decisions com tasks ligadas. | Community `frontend/src/components/specs/DecisionsTab.tsx`; Core `services/analytics_service.py`. | Sem atualização conjunta, a interface continuaria cobrando vínculos que o gate deixaria de exigir. |
| O ledger de Spec admite fisicamente apenas waiver/revoke. | Community `adapters/sqlalchemy_models.py`, `DeliveryEvidenceRecordRow`, constraint `ck_delivery_evidence_kind`. | Registrar uma inspeção direta nesse ledger exige mudança real de contrato e schema, não apenas um campo na UI. |

O caso SIM-05 confirma o efeito: `decision:dec_1d23379a`, ainda ativa na Spec
`2372d8a2-68c8-57f4-a609-d809c92d55c6`, edição 4/version 290, diz que o exercício
não implementará reservas. Ela está ligada a `fr_res_ux` e à task de domínio.
O mock foi implementado após autorização posterior do usuário. A decisão deve
ser substituída; obter um teste passing para a afirmação antiga seria incorreto.
O gate observado inclui essa decisão em `delivery_test_result_missing`.

Há dois problemas distintos: a decisão está desatualizada na iniciativa; o Pulse
generaliza indevidamente a exigência de implementação/verificação para qualquer
Decision. Corrigir um deles não resolve automaticamente o outro.

## Refinamento da proposta

“Escopo” é assunto de uma decisão; “teste” e “inspeção” são métodos. Portanto,
não introduzir o enum misto `teste | inspeção | escopo`, nem obrigar o usuário
a preencher uma taxonomia adicional de decisões.

A pergunta na autoria será: **Como demonstrar que esta decisão foi respeitada?**

| Resposta | Planejamento | Fechamento |
| --- | --- | --- |
| Pelas obrigações vinculadas | Selecionar obrigações exatas que materializam a escolha. | Derivar o resultado de suas provas atuais, sem nova execução ou associação duplicada. |
| Por inspeção direta | Declarar a condição observável e o escopo a conferir. | Registrar observação, referências versionadas e conclusão por avaliador autorizado. |
| Por ambas | Declarar as duas partes. | Exigir ambas, nunca aceitar uma como alternativa à outra. |

Exemplos: idempotência remete ao requisito/critério correspondente; isolamento
hexagonal remete ao TR e à análise/inspeção que já o verifica; limite de entrega
do mock usa inspeção direta da entrega e da documentação. “Usar SQLite” não é
comprovado por qualquer teste funcional passing: exige obrigação que identifique
essa escolha ou inspeção do adaptador/composição. Essa adequação é julgada na
validação da Spec; o backend verifica estrutura, identidade e atualidade.

Uma Decision não pode ser o único lugar onde se exige comportamento do produto.
Se a escolha impõe idempotência, segurança ou outra obrigação executável, essa
obrigação continua formalizada no requisito/contrato apropriado e participa das
referências de verificação. A inspeção direta cobre a condição contextual
restante; selecionar inspeção não permite esconder um requisito dentro do texto
da Decision. A validação semântica deve apontar essa inadequação concretamente.

## Contrato proposto

Adicionar `Decision.verification`, com objeto fechado contendo:

- `obligation_refs`: lista sem duplicatas de referências tipadas e exatas a
  FR/TR/BR/IR/OR/AC/APIContract ativos da mesma Spec, até 100 referências.
- `inspection`: opcional, com `condition` observável e `scope_refs` tipadas para
  artefatos/partes da Spec a conferir. Condição até 2.000 caracteres; até 20
  referências. O alvo da inspeção não pode ser apenas a própria conclusão.

Ao menos uma parte deve existir antes da validação/início. Em Draft, ausência é
planejamento pendente, não dispensa. O modo mostrado na UI é derivado dessas
partes, evitando armazenar um seletor redundante. `linked_requirements` e
`linked_task_ids` continuam rastreabilidade contextual; não se tornam prova nem
seleção de verificação implicitamente. A UI permite selecionar referências já
existentes sem recopiá-las ou converter todos os links automaticamente.

As referências de verificação não aceitam outra Decision, texto livre, IDs de
outro Board/Spec ou uma task genérica. Isso evita ciclos e cadeias arbitrárias.
A resolução usa a fonte relacional e os avaliadores efetivos já existentes.
Um vínculo não prova adequação semântica: a avaliação da Spec verifica se as
obrigações escolhidas realmente materializam a decisão.

Para a inspeção direta, acrescentar um registro tipado `decision_review` no
ledger de entrega da Spec, com domínio e porta no Core e persistência no
Community. Reutilizar os conceitos de observação esperada/observada, fonte
versionada, resultado e autoria do relatório de inspeção existente; seu envelope
atual é específico de cenário e não pode receber um cenário fictício.

Uma submissão pode conferir várias decisões, com observação e resultado separados
por decisão, em lote atômico limitado. Não criar uma task, Test Card ou novo
ciclo de aprovação por decisão. O registro canônico é único; Decisions, Coverage
e contexto MCP apresentam projeções dele. Não usar Q&A, notas, KG ou flags do
cliente como fonte de aprovação.

Expor a submissão por um comando específico de conferência de decisões, compartilhado
por REST/MCP. O writer atual de prova por Card mantém seu `card_id` obrigatório;
não ampliar esse writer para aceitar ausência de Card ou autoridade de waiver.
Limitar o lote a 50 decisões e 128 KiB, com até 20 fontes por observação. Registrar
o lote uma vez e derivar seu detalhamento por decisão.

## Autoridade e atualidade propostas

1. Autoria de condição/referências segue permissões existentes de Decision e
   content lock. Alteração depois do congelamento segue a reabertura/revisão
   governada da Spec. Esta entrega não introduz edição sem revalidação.
2. Submeter inspeção exige `spec.validation.submit`, além das leituras necessárias
   dos objetos citados e interação permitida no estado. Permissão para editar
   uma Decision, executar uma task ou escrever progresso não concede esse poder.
   A operação de inspeção não submete nem substitui uma Spec Validation.
3. Aplicar explicitamente a configuração `reviewer_separation_mode` também à
   inspeção direta: `enforce` recusa autor/executor do escopo; `warn` registra o
   conflito; `off` não exige outro ator. Hoje o helper está aplicado a tasks;
   estendê-lo a esse novo julgamento é parte explícita desta proposta. Resolver
   autores da decisão e contribuições por registros do servidor; autoria
   desconhecida não demonstra independência. Não criar novo papel obrigatório.
4. Autoria, instante de registro e digest do recibo são definidos pelo servidor.
   Autenticar a submissão comprova quem declarou a observação, não que o servidor
   executou uma inspeção. A interface deve distinguir essas duas coisas.
5. Vincular a inspeção ao Board, Spec, edição, decisão/condição, revisões das
   fontes e ao conjunto de entrega relevante. Para uma condição que abrange toda
   a entrega, selar o conjunto inteiro; para escopo delimitado, selar seus alvos.
   Mudança material no escopo observado exige nova conferência. Comentários,
   leituras e atualizações fora desse escopo não invalidam o recibo.
6. Manter CAS da versão na escrita, revisão do conjunto de reviews, idempotência
   por ator e rechecagem transacional na transição Done. A própria submissão não
   pode invalidar sua prova. Novos recibos preservam os anteriores; falha atual
   não pode ser escondida escolhendo um passing antigo. Mesmo estado material
   com conclusões conflitantes fica pendente até reconciliação explícita.
7. `failed`, `inconclusive`, `unavailable`, fonte ausente, resultado revogado ou
   desatualizado não satisfazem o gate. Nova edição torna a conferência anterior
   histórica. Substituição/revogação de decisão preserva seu histórico e exige
   contrato válido para a sucessora ativa.
8. Waivers e revogações continuam com autoridade humana própria. Inspeção não
   altera um waiver, não dispensa requisito/teste e não autoriza registrar como
   observada uma inspeção humana que ainda não ocorreu.

## Gates e apresentação

Separar no inventário único obrigações executáveis e aderência de decisões.
Toda Decision ativa permanece visível, com estado e motivo acionável. Ela deixa
de exigir implementação própria só por ser Decision.

Antes da validação/início, exigir contrato de verificação completo e adequado;
as obrigações referenciadas continuam exigindo seu planejamento e responsáveis.
Uma inspeção de escopo não exige resultado antecipado. Remover a obrigação de
task direta nesse caso e na verificação derivada; links úteis continuam visíveis.

No fechamento, todas as obrigações selecionadas precisam estar efetivamente
verificadas, e toda inspeção declarada precisa de resultado satisfatório atual.
Uma obrigação dispensada continua identificada como dispensada; não transforma
a Decision em verificada automaticamente. Qualquer exceção de conclusão usa o
caminho humano existente, sem ampliação implícita de seu alcance.

O mesmo resolver alimenta preview de transições, gate real, contexto, Coverage,
analytics e UI. `skip_decisions_coverage` não vira bypass da nova verificação;
seu alcance estrutural deve permanecer explícito. Não introduzir outro skip.

Em Requirements & Decisions > Decisions, manter linhas expansíveis e incluir
forma de verificação, condição ou referências selecionadas, estado, resultado,
autor e evidências. Registrar/rever inspeção nesse contexto. Coverage apresenta
o resumo e navega para a mesma conferência, sem formulário duplicado. Substituir
a interpretação de “Decision Task Coverage” por prontidão de verificação e
aderência, distinguindo planejamento de resultado. Manter links de tasks como
rastreabilidade, sem classificá-los como aprovação.

Decisões conferidas não acrescentam pontos de implementação aos Cards nem
duplicam o peso das obrigações já verificadas. A capa de task continua medindo
seu trabalho executável; prontidão da Spec também considera as Decisions.
Nenhum percentual agregado pode ocultar decisão pendente ou fonte indisponível.

## Implementação em quatro milestones

| Milestone | Alterações delimitadas | Critério de saída |
| --- | --- | --- |
| DV1 Contrato e planejamento | Modelo de Decision, validação de referências, writers MCP/REST/structured/bulk e promoção de candidatos; inventário, execução planejada e gate de Decisions. | Scope Decision tem plano válido sem task artificial; referências erradas/inativas e contrato ausente são recusados com remediação. Nenhuma classificação por palavras do título. |
| DV2 Evidência e fechamento | Registro `decision_review` no ledger existente, porta pública, adapter, comando tipado e paridade REST/MCP; permissões, separação, CAS, replay, revogação e resolução de atualidade; gate e allowed transitions. | Inspeção atual libera apenas a decisão observada; provas das obrigações são reutilizadas; falha/revogação/staleness e corrida bloqueiam. Testes de requisitos continuam obrigatórios. |
| DV3 Interface e consumidores | Decisions, Coverage/Implementation, progressos, contexto do agente, analytics e documentação; exposição da cadeia de prova sem JSON cru. Preservar identidade e supersedência no KG, sem novo tipo físico de nó/aresta nem autoridade derivada do grafo. | Mesma resposta em UI, MCP, REST e gate; preenchimento único e expansão acessível; testes frontend e inspeção real em navegador. |
| DV4 Aceitação e distribuição | Cenários abaixo, gates arquiteturais com budgets zero, catálogo MCP regenerado se alterado, build dos dois repos e parity byte-a-byte do install; instalação de teste isolada. | Fluxo novo completo, negativos verificados, documentação/ledger e commits/pushes por milestone. Entrega funcional demonstrada antes de qualquer promoção na home real. |

DV2 depende de DV1; DV3 consome ambos; DV4 valida o conjunto. Não publicar
parcialmente um contrato que a UI, gate ou adapter não consiga cumprir.

Arquivos centrais Core: `models/schemas.py`, `models/delivery_evidence.py`,
`domain/{delivery_inventory,effective_delivery_inventory,effective_delivery_coverage,execution_plan,delivery_completeness}.py`,
`services/{spec_structured_entities,main,analytics_service,coverage_traceability_read_model,reviewer_separation}.py`,
use cases de autoria/entrega/transição, portas de entrega e MCP.
Community: `adapters/{sqlalchemy_models,sqlalchemy_delivery_evidence,test_evidence}.py`,
rotas de Spec/Code Traceability e promoção de candidatos, componentes Decisions,
Coverage/Delivery, serviços/tipos frontend e seus testes.

## Aceitação obrigatória

- DV-T01: decisão de escopo com inspeção válida fecha sem task, código, cenário
  ou Test Card artificial; vínculo contextual a task não muda essa regra.
- DV-T02: decisão por obrigações reutiliza exatamente as provas atuais dessas
  obrigações; várias decisões podem referenciá-las sem duplicar execução/crédito.
- DV-T03: combinação de obrigações e inspeção exige ambas.
- DV-T04: teste funcional alheio à escolha, comentário “aprovado”, digest
  inventado, assinatura forjada e fonte inacessível não produzem verificação.
- DV-T05: FR/TR/BR/AC/API/IR/OR continuam descobertos quando lhes falta prova,
  mesmo com todas as Decisions conferidas. T33 humano continua pendente. Uma
  Decision que esconde obrigação executável é reprovada na avaliação semântica,
  ainda que seu contrato de inspeção seja estruturalmente válido.
- DV-T06: o plano não aceita referências cruzadas, inativas, ambíguas ou ciclos;
  ausência de contrato não ganha modo/default retrospectivo.
- DV-T07: content lock vale para todos os writers, inclusive CRUD genérico,
  bulk e promoção de candidato. Executor sem autoridade de avaliar é recusado.
- DV-T08: separação off/warn/enforce e autoria desconhecida têm resultados
  explícitos; ator não pode se apresentar como outra pessoa.
- DV-T09: nova entrega relevante, nova edição, substituição, revogação e fonte
  alterada invalidam a conferência aplicável; mudança irrelevante a preserva.
- DV-T10: replay idêntico é único; mesma chave com payload diferente falha;
  corrida entre review, revisão e Done não aprova base diferente da observada.
- DV-T11: resultado failed/inconclusive/unavailable continua visível; histórico
  passing não apaga falha atual nem resolve divergência por ordem de chegada.
- DV-T12: UI/REST/MCP/analytics e preview/transição real concordam; leituras
  parciais ou timeout não viram 100%; a paginação não omite decisões pendentes.
- DV-T13: formulário, expansão, referências, permissões, estados anteriores,
  erros e carregamento são testados no frontend e no navegador real.
- DV-T14: KG mantém a decisão substituída e a sucessora com proveniência; atraso
  de projeção não satisfaz nem duplica a prova relacional.
- DV-T15: Core não ganha SQLAlchemy, routers, filesystem ou runtime concreto;
  Community consome portas públicas; budgets arquiteturais permanecem zero.
- DV-T16: instalação nova usa contrato único; armazenamento incompatível é
  recusado descritivamente, sem converter nem apagar dados.

## Contrato nativo da versão ainda não publicada

A proposta de `decision_review` amplia a constraint SQL do ledger da Spec. Isso
muda o schema aceito pelo contrato estrito de armazenamento. O usuário autorizou
expressamente a mudança mesmo com breaking change. Atualizar o contrato de schema
e as fixtures nativas; não criar caminho de compatibilidade ou migração.
Compatibilidade com builds intermediários da v0.4.0 não é requisito de aceitação,
frente de desenvolvimento ou decisão pendente.

Validar primeiro em data home isolada, com instalações e dados novos. Preservar
a home atual e seus relatórios. Sob a diretriz vigente de não criar migrações,
bases incompatíveis serão recusadas; nenhuma conversão ou exclusão automática
será adicionada. A quebra autorizada permite seguir com a implementação e a
validação sem nova confirmação de schema. Ela não autoriza apagar a home atual
nem apresentar a sessão SIM-05 anterior como convertida. A aceitação usa dados
nativos do novo contrato; o histórico anterior permanece preservado. Não
contornar a constraint armazenando review como waiver, nota ou falso Test Card.
Nenhuma alteração da home faz parte deste assessment.

## Aplicação ao mock e limites de escopo

No cenário de aceitação do novo contrato, representar a substituição da decisão
conceitual pela decisão autorizada de implementar um mock para exercitar o Pulse,
com autoria e histórico nativos, sem importar resultados antigos como atuais.
A inspeção deve conferir os limites efetivamente
declarados, citando a entrega e documentação versionadas. Ela não pode afirmar
ausência de uso em produção sem delimitar o ambiente observado.

Na sessão atual, a decisão antiga permanece histórica somente depois de uma
supersedência governada; não editar silenciosamente a edição congelada. A
sobrealocação de critérios entre as tasks é outra pendência e segue sua proposta
própria. Este plano não redesenha a cobertura transitiva geral, não implementa
revisão parcial de Spec e não remove a inspeção humana T33.

O fechamento e a consolidação canonical só poderão ser declarados depois de
todos os gates efetivos, com provas verdadeiras. Este assessment não moveu Cards,
Spec ou nós do KG e não modificou código executável.
