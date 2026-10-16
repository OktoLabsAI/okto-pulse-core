# Inventário integrado — fatos revisados e limites

Estado e decisões de execução ficam no [ledger único](IMPLEMENTATION_LEDGER.md).
Este inventário complementa F0/K0/I0/P0; não marca essas fases como concluídas.
Não usar ocorrências por nome como classificação final por efeito.

## Superfícies capturadas em 2026-09-19

Par de código: Core `54832726`, Community `cac95ef`. Fontes e wheels comprovadas
byte a byte conforme o ledger. Captura por `mcp.get_tools()` e
`create_community_app().openapi()`, com data/upload/KG/database limitados ao
diretório descartável. O lifespan não foi iniciado: sem servidor, workers,
seed ou migração. MCP carregado após a composição de settings da Community.

| Superfície real | Observação |
| --- | --- |
| Catálogo MCP completo, sem filtro de papel | 340 tools; JSON compacto com descrição/schema/admission class: 244.028 bytes |
| OpenAPI da aplicação Community composta | 372 paths, 460 operações HTTP; JSON compacto: 1.012.988 bytes |
| CLI, parser `--help` | init, serve, status, code-traceability, metrics, api-key, reset, verify-pipeline, kg |
| CLI `kg --help` | migrate-schema, backfill, dedup-entities, proposals, unmerge, export, subtype, restore |
| Ocorrências de Sprint no catálogo | 14 nomes de tools, 22 schemas com ocorrência de `sprint`; 11 paths REST com `sprint` |
| Coleta pytest do par atual | Core 12.088, Community 5.265; coleta sem erro, não equivale a execução |

Hashes e SHAs exatos em [surface-inventory-2026-09-19.json](surface-inventory-2026-09-19.json).
JSONs brutos, script `capture_surfaces.py`, logs e help estão em
`PULSE_REFACTOR/.validation-v040`. Esses bytes são medição de schemas, não
benchmark de tokens, latência ou fluxo completo; negociação MCP de transporte
e custo completo antes/depois ainda não medidos.

## Cadeias revisadas para os próximos incrementos

| Assunto | Fonte e caminho real | Consequência para a implementação conjunta |
| --- | --- | --- |
| Writers de Delivery | Community `api/code_traceability.py` → Core `application/use_cases/delivery_evidence.py` → `CommunityDeliveryEvidenceStore` | Provas por card; exceções humanas por Spec. F09 fechado com erro explícito, sem migração histórica. |
| Inventário | `domain/delivery_inventory.py`, porta `ports/delivery_inventory.py`, gate `services/delivery_evidence.py` | Uma política pura compartilhada. Atualmente links diretos; o futuro resolver efetivo precisa alimentar planejamento/contexto/admissão/rollup. |
| F11/edição de card | `UpdateCardUseCase` → `CardService.update_card` → ApplicationPersistence Community → `CommunitySemanticSession` → snapshot Delivery | Mudança de description/details aumenta `policy_version`, mas não altera digest fallback nem o predicado de Delivery. Mudança de título altera o digest. Ver reprodução abaixo. |
| Prova de implementação | `sqlalchemy_delivery_evidence._implementation` | Confere board/card/Spec, Target ativo e revisão, último execution, receipt aceito, não revogado, source/revision e commit limpo. Não compara a versão do card com o conteúdo observado. |
| Prova de teste | `_test` → `TestEvidenceWriteVerifier` → `CommunityTestEvidenceWriteVerifier` → `verify_community_evidence_v2` | Usa cenário atual, Test Card e IDs exatos de implementação. Não presumir suporte de static_analysis/inspection/demonstration a partir de rótulos. |
| Cobertura planejada | `coverage_traceability_read_model._derived_links` | Já deriva vínculos AC via cenário e FR via BR; isso não torna o inventário Delivery transitivo nem comprova suficiência semântica da BR. |
| Autoria FR/TR/AC | `SpecCreate/SpecUpdate`, `spec_entity_canonicalization`, `StructuredSpecEntityService.mutate` | FR/TR/AC aceitam strings/dicts legados; canonização preserva IDs/texto/extras. Verificação nova exige schema fechado e compatibilidade explícita, sem descartar legado. |
| Autoria IR/OR | `IntegrationRequirement`, `ObservabilityRequirement` em `models/schemas.py` | IR já tem contract_ref/data_contract/linked_api_contracts. Não inventar HTTP para contrato de evento nem criar APIContract intermediário obrigatório. |
| Lock da Spec | `require_draft_mutation`, `spec_is_content_locked`, `_require_spec_unlocked` | Current validation com sucesso na edição bloqueia conteúdo. Voltar a draft inicia nova edição e preserva histórico. Promoção/reclassificação normativa deve obedecer esse fluxo. |
| Início de execução | `SpecService.move_spec`, validated → in_progress | Reexecuta cobertura de testes/regras/TR/API/IR/OR, vínculos das tasks, decisões, Code Evidence e avaliação qualitativa. Novos predicados integram esse gate, sem exigir provas passing antecipadas. |
| Herança de validação | `CardService._resolve_validation_config` | Resolve cada campo por coalescência de null Sprint → Spec → Board; `False` e zero não são ausência. required, min_confidence, min_completeness e max_drift têm `resolved_sources` próprios. A migração não pode copiar só `resolved_from`. |
| Universo arquitetural | `ArchitectureDesignRepository.list`, `ArchitecturePropagationService`, `ResolvedResourceLineageService`/`resolve_effective_card_copy_plan` | Designs diretos não são toda a população: há herança efetiva. Reusar resolução canônica e falhar fechado se a origem não resolve. |
| Identidade arquitetural | `ArchitecturePropagationService._payload_from_source` | Copia interfaces preservando seus IDs e carrega `source_design_id` raiz; diagramas recebem IDs físicos novos. Candidato não pode usar ID do diagrama/cópia como identidade lógica. |
| Contrato arquitetural | `ArchitectureInterface` | IDs podem faltar no legado; schema dict vazio difere de None. Não gerar IDs na leitura nem excluir contrato incompleto do denominador. |
| Atualização de cópia | `copy_from_parent`, `_find_existing_copy` | Re-sync é write explícito e versionado; não introduzir atualização de snapshot por leitura dos candidatos. |

O exame dos métodos de prova ainda precisa cobrir todo o admission path e suas
autoridades, não só a interface `TestEvidenceWriteVerifier`. P0 também precisa
dos fixtures de content lock/classificação/gate inicial e do rollout por estado.

Verifier Community revisado: `verify_community_evidence_v2` normaliza o formato,
valida a semântica Core, producer/adapter, bytes/hash e identidade do manifesto,
e consulta o ledger autenticado. `validate_replay_manifest` admite somente
`okto-pulse-http-replay/v1`, com escopo/scenario hash e assertions
`json_equals`/`body_contains`. O executor concreto faz GET por HTTP e registra
observações. Portanto, a existência do verifier não comprova admissão dos
novos métodos static_analysis/inspection/demonstration. A evolução precisa
receber resultados externos e verificar autoridade/base/critério conforme
ARQ/VER §3.6, sem acrescentar execução de testes ao backend nem reclassificar
receipts antigos por simples rótulo. As leituras e migrações atuais preservam
legado não verificado; não inventar fatos para promovê-lo.

A suite ampla revelou um drift adicional na infraestrutura de testes Core:
o fixture `specs` não possuía `skip_delivery_evidence`. Corrigido com default
false igual ao adapter; 39 testes AF23 passaram. Comparação de metadata também
localizou cinco colunas ausentes no fixture `quality_assessment_receipts`
(`event_id`, `history_id`, `idempotency_key`, `outbox_id`, `request_digest`).
Esse segundo achado ainda requer análise dos consumidores; não é conclusão
sobre o schema de produção nem autorização para relaxar seus constraints.

Inspeção da metadata SQLAlchemy Community (sem conectar banco): tabelas diretas
`sprints`, `sprint_activation_baselines`, `sprint_history`, `sprint_qa_items`;
`cards.sprint_id` e `sprints.origin_sprint_id` carregam FKs operacionais.
Isso não inclui todos os documentos JSON, subjects genéricos, eventos, grants,
attachments ou source refs; o inventário histórico ainda precisa cobri-los.

## Reprodução F11 e preservação histórica

Teste Community `tests/test_delivery_fallback_characterization.py`, sem mocks
de autorização, guard de contexto ou persistência. Usa SQLite descartável,
prova de implementação com receipt aceito, `UpdateCardUseCase`, UoW e sessão
semântica reais. Registra prova fallback para task sem links em `in_progress`,
edita title/description/details como proprietário, confirma o incremento de
versão e reavalia o snapshot e `require_card_delivery` em modo blocking.

- `title`: digest muda e Delivery recusa a prova anterior.
- `description`/`details`: digest permanece e Delivery aceita a prova anterior.

Os três testes passaram e passaram também junto dos 26 testes de integração
Delivery. Isso demonstra a insuficiência desse predicado fallback; não demonstra
que todos os outros gates de validação/avaliação/Done aprovariam a transição.
Não foram alterados digests, receipts, políticas nem dados históricos. A correção
prospectiva pertence ao contrato/rollout novo de DEI §11.4 + ARQ/VER §11, junto
da resolução efetiva e das migrações; não invalidar retroativamente tudo usando
apenas `policy_version`.

## Frentes ainda abertas

- Sprint: inventário por efeito de heranças por campo, Path A/B, exports,
  histórico, jobs e referências; depois fixtures de migração com equivalência
  de policies. Os contadores acima não autorizam remoção por grep.
- Manutenção: seguir handlers e efeitos de health, DLQ, tick, rebuild/recovery,
  digest reconcile e CLI; separar diagnóstico de write antes do corte.
- KG: mapear fontes relacionais, owners/active sets e replay já incorporados
  na v0.3.4/Grafx; nenhum novo dialeto concreto no Core.
- P1/P2/I1: identidade/população/classificação e resolução explícita/herdada
  precisam preceder o congelamento de I2/P3 (schema, batch, retomada e fechamento).
- Full suites, migração/rollback, UI/E2E e medição completa permanecem pendentes
  conforme resultados atualizados no ledger.
