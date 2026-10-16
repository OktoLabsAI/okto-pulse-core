# KG: mapa de execução integrado v1.3

Este documento acompanha `feature/v0.4.0` nos dois repositórios irmãos. A
especificação é o pacote `Pulse_Plano_Codex_v1_3_Arquitetura_Verificabilidade`,
começando por `INICIAR_NO_CODEX_ENTREGA.md` e seus quatro documentos normativos.
O complemento KG aplica-se junto ao plano-base, à entrega incremental e à
arquitetura/verificabilidade; não é um projeto independente.

Esta revisão substitui as instruções operacionais antigas deste arquivo.
O histórico do documento permanece no Git. Números de boards, registros e
diagnósticos antigos não são evidência do runtime atual. Não é necessário
consultar a ideação original para executar a iniciativa.

O estado de implementação, decisões autorizadas, SHAs, falhas e provas está no
[ledger](pulse-simplification/IMPLEMENTATION_LEDGER.md). Este mapa descreve o
resultado exigido; uma linha aqui não significa que esteja implementada.
O escopo Community está no documento irmão `KG_TRACEABILITY_GAPS_COMMUNITY.md`.

## Autoridade e dependências

- Contratos Protocol, semântica e gates pertencem ao Core. Grafx, SQLAlchemy,
  filesystem, scheduler, HTTP e demais mecanismos pertencem aos adapters
  Community. Adapters usam portas públicas; ausência de porta não autoriza
  reach-in nem duplicação da regra.
- A fonte relacional decide planejamento, classificação arquitetural,
  obrigações diretas/herdadas e admissão de Delivery Evidence. O KG projeta
  essas fontes; não cria aprovação, critério, prova passing ou waiver.
- Target é intenção, receipt é execução técnica, claim é declaração. Relações
  gráficas e proxies não promovem nenhum desses elementos a prova verificada.
- Captura semântica durável precede materialização de Learning. Filas, DLQ,
  endpoints ainda não projetados e dívida técnica não substituem uma pendência
  semântica atual nem autorizam bloquear a entrega por manutenção do grafo.
- A retirada de Sprint preserva conteúdo substantivo, permissões, proveniência
  e histórico pelos contratos aprovados. Não projetar Sprint ativa nem inventar
  origem Spec/Board para eliminar órfãos.

## Decisões D1–D20 aplicáveis

| ID | Tratamento no pacote consolidado |
|---|---|
| D1 | Card possui card→filhos, com referências estáveis e inputs completos pela porta. |
| D2 | Timestamps da fonte são distintos de `created_at` da projeção. |
| D3 | Proxy `violates` de origem, confiança 0.8, explicitamente inferido; não é causa raiz comprovada nem prova de gate. |
| D4 | Bug conserva tipo e proveniência; conectividade real remanescente, sem Sprint fictícia. |
| D5 | Evoluir o schema real; versão e fingerprint inequívocos, nunca dois contratos incompatíveis sob 0.6.0. |
| D6 | Learning advisory/blocking: quando exigida, capturar semântica durável válida; não exigir projeção canônica antecipada. |
| D7 | Integrar dependências e trabalho local já incorporado, sem rebase destrutivo. |
| D8 | Upgrade explícito interno, com preflight, backup e autorização; nenhuma interface pública de manutenção. |
| D9 | Candidato e reconsolidação internos, retomáveis e combinados à retirada de Sprint. |
| D10 | Preservar status cru da fonte, separado de tipo. |
| D11 | Último `done` documentado e limpeza ao reabrir; desconhecido é NULL. |
| D12 | Remover coocorrência possuída; pendência contextual durável, correção pela fonte, interpretação semântica auditada sem autoridade automática. |
| D13 | Similaridade sugere candidatos; reuso/supersedência requer intenção autorizada, exceto dedup idêntico/replay. |
| D14 | Consulta recomendada, sem rito adicional; autoridade relacional, suficiência/currentness e frescor explícitos. |
| D15 | Sem limite de perguntas por contagem; preservar isolamento e capacidade concorrente. |
| D16 | UI completa: Impacto na Decision, Cobertura na Spec, Clusters em Analytics. Health continua operacional. |
| D17 | Policy humana em Board settings, default advisory; executor não desliga seu gate nem migração ativa blocking. |
| D18 | Consulta com prazo real 15/30 s e linhas 200/1000, inclusive agregações e execução nativa. |
| D19 | Reutilizar telas contextuais; sem wizard, checklist ou leitura obrigatória para abrir painel. |
| D20 | Missing-link blocking somente para lacuna semântica atual/aplicável, não por atraso técnico, endpoint ausente, DLQ ou ledger antigo. |

## Famílias de projeção e consumidores

| ID | Fonte e resultado requerido | Qualificação necessária |
|---|---|---|
| G1 | `linked_criteria`: cenário→critério por identidade canônica. | IDs primeiro; legado inequívoco; remove/restaura; ambiguidade explícita. |
| G2/G4 | `linked_task_ids`, cenários do Card e evidência admitida; card→filhos. | Dono Card, Entity/Bug correto, contribuição específica e referências externas estáveis. |
| G3 | Dependências de Card, pré-requisito→dependente. | Ciclos, pares reais necessários, eventos dos dois lados, nenhuma duplicação de Bug como Entity. |
| G5 | Bug, origem e vínculos declarados/inferidos. | Distinguir proxy, declaração e evidência; confiança/regra/origem visíveis. |
| G6 | Supersedência de Decision. | Histórico caminhável, dono exato, sem apagar predecessor ou aprovar conteúdo. |
| G7/G8 | API→BR, BR→FR, IR→FR, OR→IR e referências admitidas reais. | Direção/par correto, remoção refletida, domínio investigado antes de ampliar schema. |
| G-origins | Decision→FR/TR explícitos. | Sem fan-out por coocorrência; preservar autores/regras alheios e narrativa histórica. |
| G9 | `kind_of` por tipo de artefato. | Sem Sprint, sem confundir tipo com status/severidade. |
| G10/G-cols | Status, severidade e datas da fonte. | Rebuild/replay preservam cronologia; ausência não vira data atual. |
| G11 | Amendments, revisões e regressões. | Preservar identidade e origem; eventos alcançam consumidores. |
| G12 | Referências não resolvidas e interpretações. | Separar fonte incompleta, atraso, permissão, ambiguidade e ausência; corrigir domínio, sem fila obrigatória de reparo. |
| G13/G15 | Conjuntos ativos e identidade de arestas. | Writer/regra/dono/seção, before-images, compensação e confirmação de remoção. |
| G14 | Eventos e enfileiramento. | Releitura atual, eventos atrasados/repetidos, concorrência, commit/ACK e paridade com rebuild. |
| G16 | Consultas e UI. | Autorização, completude, frescor, paginação, direção e evidência; vazio não implica ausência. |
| G17 | Recursos, documentação e distribuição. | Contratos publicados e catálogo gerado coerentes com runtime e par instalado. |

Não inferir um vínculo normativo por proximidade, similaridade ou coocorrência.
Não remover relações de outros writers/owners para satisfazer uma contagem.
IDs duplicados ou referências ambíguas exigem recusa/limitação explícita.
Zero-órfão exige fonte/conectividade legítima, sem perda de história.

## Perguntas Q01–Q20

| ID | Pergunta preservada, sem assumir resposta completa hoje |
|---|---|
| Q01 | Impacto de uma Decision em requisitos, cards e cenários. |
| Q02 | Cards por requisito e requisitos por Card. |
| Q03 | Cenários por critério e critérios sem cobertura. |
| Q04 | Test Cards por cenário e cenários sem automação. |
| Q05 | Bugs em uma janela e associações comuns, distinguindo hipótese de causa. |
| Q06 | Bugs por origem e áreas afetadas. |
| Q07 | O que regrediu e quais cenários são pertinentes à correção. |
| Q08 | Decisions relacionadas a requisito ou Card. |
| Q09 | Efeito da supersedência, com história preservada. |
| Q10 | Constraints aplicáveis e associações de Bug. |
| Q11 | Dependências e consequências de atraso, sem inventar causalidade. |
| Q12 | Mudanças no Board pela cronologia da fonte. |
| Q13 | Learnings aplicáveis ao escopo e seus limites. |
| Q14 | Contratos por requisito e cards envolvidos. |
| Q15 | Requisitos sem Card e progresso por Spec/conjunto relacionado; sem dimensão Sprint. |
| Q16 | Code Evidence autorizada e sua relação com requisitos. |
| Q17 | Cobertura estrutural coerente com obrigações relacionais efetivas. |
| Q18 | Amendments e escopo afetado. |
| Q19 | Bugs por status/severidade e tempo de resolução com limites temporais explícitos. |
| Q20 | Linhagem de Card nos dois sentidos, incluindo limites da projeção. |

Cada resposta precisa delimitar fonte, camada, frescor, completude, autorização e
inferência. Ligar nós não comprova execução nem substitui os verifiers dos
métodos de evidência definidos na especificação de arquitetura/verificabilidade.

## Learnings e fechamento

Submeter uma intenção semântica com conteúdo, limites de aplicação, origem,
evidências e intenção de criar/reusar/superseder. Reutilizar revisões/selo de
`kg_cognitive_sources` quando aptos. Validar autoria, escopo, idempotência e base
de versões antes de admitir; não criar banco de memória paralelo.

No blocking, a policy exige captura válida e atual. Não aceitar fila vazia,
provider falho, `not_applicable` inventado ou projeção ausente como dispensa.
No advisory, não obrigar todo Bug a produzir Learning. Reabertura/mudança da
base exige reavaliação de aplicabilidade, preservando a história.

O worker materializa quando os alvos forem elegíveis. Não exigir que o agente
descubra IDs canônicos, mova para Done antes da captura, faça polling e vincule
novamente. Não registrar worker com LLM interna. R7/holds mistos precisam de
classificação exata; elegibilidade de um Bug não libera indiscriminadamente
toda dívida. Categorias de evidência existentes só mudam por contrato explícito.

## Evolução, validação e entrega

Ver [contrato 0.7.0 e sua qualificação](pulse-simplification/GRAPH_SCHEMA_070.md).
Upgrade integra retirada de Sprint, snapshot autoritativo, candidato, memória
cognitiva, reconciliação, histórico, checkpoint, cutover e rollback. Não há
override genérico de hash nem migração de dados reais implícita neste plano.

O aceite cobre KG-01–KG-66 junto aos gates do plano-base, DEI e arquitetura/
verificabilidade. Medir conjuntos, não só contagens. Testar autorização, fontes
parciais, ambiguidade, remoção, replay, falha após delete, compensação, processos
frescos, upgrade/rollback, frontend e wheels pareadas. Comparar todos os `.py`
instalados byte a byte com os dois checkouts antes de validar comportamento.

O catálogo MCP vem do registry pelo gerador oficial. Recursos alterados exigem
os manifests aplicáveis; não editar catálogo manualmente. A auditoria
`okto-pulse-saas-closure` deve manter os oito budgets em ZERO nos dois sentidos.

Publicar evidências e limites por requisito, sem promover testes focados a
aceite global. Benchmark deve medir fluxo completo e populações/permissões
equivalentes, incluindo custos novos e evitados. Não cortar gates para obter
ganho. Merge, release, parada de runtime e migração real continuam sujeitos à
autorização específica; commits e pushes de progresso foram autorizados.
