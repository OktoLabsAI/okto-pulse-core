# Analytics Data Flow — Service Layer Canônico

**Origem**: Ideação #9 (`aa9e6cee`) — refactor de 8 duplicações MCP vs REST.
**Commit inicial**: `d82e00d` (D-1 coverage piloto).

## Princípio

Toda agregação de analytics passa por **um único ponto** em
`src/okto_pulse/core/services/analytics_service.py`. REST endpoints e MCP
tools são *thin adapters* que delegam ao service.

```
                   ┌──────────────────────────────────────┐
                   │  services/analytics_service.py       │
                   │  (fonte canônica, funções puras)     │
                   └──────────────────────────────────────┘
                           ▲                        ▲
                           │                        │
          ┌────────────────┴──────┐     ┌───────────┴────────────┐
          │ api/analytics.py      │     │ mcp/server.py          │
          │ FastAPI endpoints     │     │ MCP tools              │
          │ (auth + HTTP params)  │     │ (auth + JSON encoding) │
          └───────────────────────┘     └────────────────────────┘
                   │                                ▲
                   │                                │
                   ▼                                │
          GET /analytics/X               okto_pulse_get_analytics(metric_type=X)
```

## Funções do service

| Função | Uso REST | Uso MCP | Status |
|---|---|---|---|
| `compute_coverage()` | `board_coverage` | `get_analytics(coverage)` | D-1 ✓ |
| `compute_funnel()` | `board_funnel` | `get_analytics(funnel)` | D-4 ✓ |
| `compute_velocity()` | `board_velocity` | `get_analytics(velocity)` | D-5 ✓ |
| `spec_coverage_summary()` | (futuro) | `_spec_coverage` (re-export) | D-7 ✓ |
| `filter_decisions_by_status()` | (futuro) | `get_spec_context`, `get_task_context` | D-8 ✓ |
| `decisions_stats()` | (futuro) | `get_spec_context`, `get_task_context` | D-8 ✓ |
| `resolve_linked_criteria_to_indices()` | `board_coverage` | (via `compute_coverage`) | ✓ |
| `resolve_linked_fr_indices()` | `board_coverage` | (via `compute_coverage`) | ✓ |
| `aggregate_task_validation_gate()` | `board_overview` / `board_validations` | `get_analytics(overview)` | D-2 ✓ |
| `aggregate_spec_validation_gate()` | `board_overview` / `board_validations` | `get_analytics(overview)` | D-3 ✓ |
| `compute_blockers()` | `board_blockers` | `list_blockers` | D-6 ✓ |

## Invariante do contrato

Sempre que REST e MCP expõem a mesma métrica, o shape do payload JSON é
bit-a-bit idêntico (mesmo conjunto de chaves, mesmos tipos). Divergência
é considerada regressão e travada por parity tests em
`tests/test_mcp_rest_parity.py` (a implementar).

## Fluxo de dados — exemplo `coverage`

1. Cliente HTTP: `GET /boards/{id}/analytics/coverage`
2. `api/analytics.py::board_coverage` chama `_ensure_board` (auth) +
   delega a `compute_coverage(db, board_id, dt_from, dt_to)`.
3. `services/analytics_service.py::compute_coverage` lê specs, chama
   `_coverage_row_for_spec()` para cada um, retorna lista.
4. Resposta FastAPI serializa para JSON.

Cliente MCP: `okto_pulse_get_analytics(metric_type="coverage")`.
- `mcp/server.py::get_analytics` valida auth + chama exatamente o mesmo
  `compute_coverage()` do service.
- Converte saída para JSON via `json.dumps(result, default=str)`.

**Efeito**: fix aplicado ao service propaga automaticamente para REST e
MCP. O histórico de drift (c0db81d hotfix MCP após 3abd2d3 REST) não se
repete.

## Migração incremental

Cada duplicação é migrada em commit separado:

- `d82e00d` — D-1 coverage piloto
- `4aac1a9` — D-4 funnel
- `e165bc6` — D-5 velocity
- `b965e1b` — D-7 spec_coverage + D-8 decisions helpers
- `5a3a2f9` — parity test suite (tests/test_mcp_rest_parity.py)
- `e02c75f` — D-2 task validation gate + D-3 spec validation gate
- D-6 blockers — commit final (mesma release)

Todas as 8 duplicações mapeadas no audit `docs/duplications_audit.md`
estão migradas. Parity test suite bloqueia regressão.

## Testes

- **Unit do service**: `tests/test_analytics_service.py` (17 testes,
  cobre funções puras isoladamente).
- **Integration existentes**: `test_analytics_coverage.py`,
  `test_analytics_cycle_time_phase.py`, `test_analytics_velocity_daily.py`,
  `test_spec_context_active_decisions.py` — zero regressão após migração.
- **Parity test suite (futuro)**: `tests/test_mcp_rest_parity.py` valida
  igualdade de payload REST vs MCP para cada duplicação.
