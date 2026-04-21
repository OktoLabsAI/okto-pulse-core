# Audit Estrutural: Duplicação MCP ↔ REST em Analytics

**Ideação**: #9 (`aa9e6cee-da1e-44c1-a13b-e65226794f30`) — Fase 1
**Data**: 2026-04-19
**Escopo**: `src/okto_pulse/core/mcp/server.py` vs `src/okto_pulse/core/api/analytics.py` + `api/specs.py`

---

## Sumário Executivo

8 duplicações identificadas entre tools MCP e endpoints REST:
- 1 **drift ativo documentado** (D-1 coverage)
- 2 **divergências potenciais** (D-4 funnel, D-5 velocity)
- 3 **paridades com gaps menores** (D-2 task validation, D-3 spec validation, D-6 blockers)
- 2 **helpers MCP-only** que REST poderia adotar (D-7 spec coverage, D-8 decisions stats)

---

## D-1: Coverage AC Deduplication (DRIFT ATIVO)

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:1276-1350` | `mcp/server.py:6617-6649` |
| Função | `board_coverage()` | `okto_pulse_get_analytics(metric_type="coverage")` |
| Risco drift | **ALTO — histórico de divergência** |

**Diff**:
- REST retorna 9 campos (inclui BR/contract counts + FR coverage %); MCP retorna 6.
- Campos ausentes no MCP: `business_rules_count`, `api_contracts_count`, `fr_with_rules_pct`, `fr_with_contracts_pct`.
- AC dedup via `_resolve_linked_criteria_to_indices()` (ambos usam, mas MCP duplica o import inline).
- Clamp `min(len, total_ac)` em ambos, mas MCP não clampa FR coverage.

**Fix recente**: commits `3abd2d3` (REST) + `c0db81d` (MCP hotfix).
**Prioridade**: CRÍTICA.

---

## D-2: Task Validation Gate Aggregation

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:272-344` | `mcp/server.py:6413-6456` |
| Função | `_aggregate_task_validation_gate(cards)` helper | inline em `get_analytics(metric_type="overview")` |

**Diff**: MCP falta `avg_attempts_per_card`, `first_pass_rate`, `rejection_reasons` (via `_classify_task_violation()`).
**Prioridade**: MÉDIA.

---

## D-3: Spec Validation Gate Aggregation

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:205-269` | `mcp/server.py:6413-6456` (inline overview) |
| Função | `_aggregate_spec_validation_gate(specs)` helper | não existe helper MCP dedicado |

**Diff**: MCP não expõe spec validation como `metric_type` separado; embutido em overview. Sem `_classify_spec_violation()`.
**Prioridade**: BAIXO-MÉDIA.

---

## D-4: Funnel Metrics (Status Breakdown)

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:970-1148` | `mcp/server.py:6542-6564` |
| Função | `board_funnel()` (14 campos + breakdowns) | inline (6 contadores) |

**Diff**:
- REST retorna `spec_status_breakdown` e `card_status_breakdown`; MCP não.
- REST diferencia `cards_impl` vs `cards_test` vs `cards_bug`; MCP só tem `done`.
- REST computa `rules_count`, `contracts_count`, `specs_with_rules`, `specs_with_contracts`; MCP omite.

**Prioridade**: ALTA.

---

## D-5: Velocity Binning (Weekly vs Daily)

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:1224-1275` + helpers em `2098-2288` | `mcp/server.py:6586-6615` |
| Função | `board_velocity()` com `granularity` param | hardcoded weekly (`timedelta(weeks=i)`) |

**Diff**: MCP hardcoded 12 semanas; REST aceita `granularity=week|day` + `days` param + séries extra (`spec_done`, `sprint_done`, `refinement_done`, `ideation_done` — entregues na ideação #6).
**Prioridade**: MÉDIO-ALTA.

---

## D-6: Blockers Triage (PARIDADE PRÓXIMA)

| Aspecto | REST | MCP |
|---|---|---|
| Arquivo:linha | `api/analytics.py:778-961` | `mcp/server.py:6684-6871` |
| Função | `board_blockers(stale_hours=72)` | `okto_pulse_list_blockers(stale_hours=72)` |

**Diff**: REST não retorna `filter_type` no response; MCP retorna. Lógica idêntica nos 6 tipos de bloqueio.
**Prioridade**: BAIXA (cosmética).

---

## D-7: Spec Context Coverage Summary (MCP-only)

| Aspecto | MCP |
|---|---|
| Arquivo:linha | `mcp/server.py:5233-5250` |
| Função | inline em `okto_pulse_get_spec_context` via `_spec_coverage()` (L451-535) |

**Diff**: REST não expõe `coverage_summary` em `GET /specs/{id}`. Helper `_spec_coverage()` é reutilizado por `add_test_scenario`, `add_business_rule`, `add_api_contract`.
**Prioridade**: BAIXO-MÉDIA.

---

## D-8: Decisions Stats & Filtering (MCP-only)

| Aspecto | MCP |
|---|---|
| Arquivo:linha | `mcp/server.py:404-448` (helpers), L5170-5176 + L1771-1777 (uso) |
| Função | `_filter_decisions_by_status()`, `_decisions_stats()` em `get_spec_context` / `get_task_context` |

**Diff**: REST `GET /specs/{id}` não implementa `include_superseded`. `decisions_stats` breakdown útil para dashboards.
**Prioridade**: BAIXA. (Ideação #10 reabre esse tema com coverage obrigatória.)

---

## Tabela de Prioridades de Refactor

| # | Duplicação | Risco | Ação | Esforço |
|---|---|---|---|---|
| D-1 | AC Coverage Dedup | **CRÍTICO** | Extrair helper compartilhado + adicionar BR/contract ao MCP | Alto |
| D-4 | Funnel Breakdowns | ALTO | MCP adicionar status_breakdown + cards_by_type | Médio |
| D-5 | Velocity Granularity | MÉDIO-ALTO | MCP aceitar `granularity` + `days` + séries extras | Médio |
| D-2 | Task Validation Gate | MÉDIO | MCP reutilizar helper REST + rejection_reasons | Médio |
| D-3 | Spec Validation Gate | BAIXO-MÉDIO | MCP extrair helper dedicado | Médio |
| D-7 | Spec Coverage Summary | BAIXO-MÉDIO | Extrair `_spec_coverage()` como canônico; REST usa | Baixo |
| D-6 | Blockers Triage | BAIXO | REST adicionar `filter_type` ao response | Baixo |
| D-8 | Decisions Stats | BAIXO | REST adota pattern (casará com ideação #10) | Baixo |

---

## Evidências de Drift Documentadas

1. **Commit c0db81d** (2026-04-19): MCP coverage retornava `covered_ac > total_ac`. Hotfix de dedup aplicado, mas BR/contract coverage continua defasada.
2. **Commit 3abd2d3**: REST fix original de AC double-counting. MCP exigiu 2º commit.
3. **Ideações #2, #6**: `cycle_time_by_phase` + `granularity=day` adicionados ao REST. MCP ficou stale.

---

## Decisão do Checkpoint (Fase 1 → Fase 2)

Opções:

**Opção A — Refactor completo (8 duplicações)**: ~10h, extrai todos os agregadores REST para `services/analytics_service.py`, MCP delega. Alto valor estrutural mas trabalho extenso.

**Opção B — Refactor escopado a drift-ativo + D-10 touchpoints** (D-1, D-4, D-5, D-7, D-8): ~6h. Prioriza o que já causou drift + prepara terreno para ideação #10 (decisions first-class exige helper compartilhado).

**Opção C — Só D-1 + D-8** (crítico + ideação #10 dependency): ~3h. Mínimo viável para desbloquear #10.

**Recomendação**: **Opção B**. A Opção A agrega mais valor estrutural mas sem urgência imediata. A Opção C adia trabalho que vai ser feito de qualquer jeito quando as próximas duplicações divergirem.
