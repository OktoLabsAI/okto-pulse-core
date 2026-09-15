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

### Board KG Analytics: disponibilidade independente por componente

O contrato v2 de Board KG Analytics usa `BoardKgAnalyticsUseCase` e
`BoardKgEffectivenessService`, com evidências fornecidas pelo port de Analytics
da edição. O Core não depende de Grafx nem de qualquer backend concreto.

`metric_status` de KG Health é um campo legado binário: telemetria parcial é
publicada como `unavailable`. Esse campo **não significa**, isoladamente, que
o grafo ou a autoridade de Analytics estejam indisponíveis. A fachada pública
`read_board_kg_health_evidence` usa os probes explícitos de graph/discovery,
sua atualidade e a indicação de consultabilidade para distinguir:

- `available`: evidências completas e atuais;
- `partial`: componentes ou indicadores válidos coexistem com telemetria
  incompleta, snapshots desatualizados ou outro componente indisponível;
- `unavailable`: nenhuma evidência utilizável das autoridades de saúde;
- `restricted` / `error`: preservados, sem substituição por sucesso parcial.

Payloads antigos sem probes continuam conservadores. Probes desconhecidos,
ausentes ou indisponíveis não são convertidos em resultados vazios. Um board
explicitamente confirmado como ainda não materializado não precisa abrir um
grafo para publicar seus fatos vazios confirmados. `recovery_needed`,
`quarantined` e `backpressure` continuam com classificação `blocking`, mesmo
quando faltam métricas. Inventário, contagens, taxas e amostras ausentes nunca
são inventados ou preenchidos com zero.

A UI mantém os indicadores válidos visíveis, distingue os componentes e
explica que falta de telemetria, por si só, não exige rebuild. O Community
publica diagnósticos de saúde por componente e um drill-down somente leitura.
Não há nova configuração nem alteração dos contratos de escrita/recuperação.

Regressão: `test_board_kg_analytics.py`, `test_board_kg_effectiveness.py` (Core),
`test_analytics_a5_a6_adapter.py`, testes de transporte Analytics e
`KgEffectivenessFullView.test.tsx` (Community).

Validação da correção em 2026-09-14: **362 testes Core**, **69 testes Community
de Analytics**, **84 testes frontend de Analytics** e **42 testes Community de
inicialização/recursos** passaram (regressão selecionada, não toda a suíte do
produto). O build TypeScript/Vite e a sincronização do frontend empacotado
passaram. Três testes de Health desatualizados foram adaptados aos contratos
atuais, preservando a prova de isolamento de probes com handles reais Grafx.
Pulse 0.3.3 atualizado localmente, mantendo Grafx 0.0.6, e validado via HTTP e
browser nos modos resumido/completo: `partial`, componentes independentes e
indicadores válidos visíveis. Não foi executado rebuild/redrive/consolidação
como parte dessa validação.

Sempre que REST e MCP expõem a mesma métrica, o shape do payload JSON é
bit-a-bit idêntico (mesmo conjunto de chaves, mesmos tipos). Divergência
é considerada regressão e travada por parity tests em
`tests/test_mcp_rest_parity.py` (a implementar).

### Capacidade não aplicável e ausência legítima de amostras

O contrato agnóstico `GraphStorageFootprint.percentage_applicable` distingue
ausência explícita de quota (`False`) de leitura incompleta (`True`, padrão).
O adaptador só declara `False` quando conhece essa condição. O Core não infere
isso de `percentage=None`. `GraphTelemetry.high_water_mark_applicable=False`
exclui apenas esse percentual do cálculo de disponibilidade, nunca erros de
WAL/commit nem as outras métricas. Valores incompatíveis com essa declaração
são recusados pelos contratos.

REST/MCP Health expõem `storage_footprint_proxy.percentage_status` como
`available`, `not_applicable` ou `unavailable`, mais `percentage_reason`.
Sem quota, o motivo é `no_capacity_limit_configured`: os bytes medidos continuam
visíveis, mas `percentage` e `high_water_mark_pct` permanecem `null`. A UI não
desenha uma barra de 0% nem recomenda recovery por essa condição.

Em Analytics, zero amostras de tempo sem um resultado materializado que exija
timestamp produz `effectiveness.timing.state=empty`, `sample_count=0`, percentis
`null` e motivo `no_consolidation_timing_samples`. A UI apresenta **No samples**;
isso não transforma o resultado global em `partial`. Um resultado materializado
sem timestamp permanece `unavailable / insufficient_consolidation_timing_evidence`,
inclusive quando existem outras amostras válidas. Percentis entre páginas não
são combinados sem as amostras brutas; páginas inteiramente vazias preservam
`empty`. Dívidas reais continuam influenciando o estado de saúde, independentemente
da disponibilidade dos dados.

O orçamento do runtime é fornecido pelo adaptador da edição. O Community captura
os valores validados usados para construir seus pools (buffer por participante e
número de leitores), sem abrir grafos nem resolver bindings para consultar esse
metadado. Não é RSS observado nem limite de memória de todo o processo. Não há
nova configuração a preencher ou operação de recuperação necessária.

Testes específicos: `test_kg_telemetry_applicability.py`,
`test_board_kg_effectiveness.py`; no Community, testes de composição/fachadas
routed, providers Grafx, contratos HTTP e UI de Analytics/KG Health.

Regressão selecionada dessa complementação (2026-09-14): **386 Core + 231
Community + 130 frontend = 747 testes aprovados**, incluindo falhas reais,
aplicabilidade, ausência de amostras, composição, transportes e recursos de
inicialização. Build TypeScript/Vite e verificação dos 78 arquivos do frontend
empacotado aprovados. Não equivale à execução de toda a suíte do produto.
O gate de isolamento do Core também passou; duas descrições antigas foram
neutralizadas, sem mudar comportamento ou introduzir exceções na verificação.

Validação instalada, 2026-09-14 18:40 UTC: ambos os ambientes locais (Python
user-site e ferramenta uv) conferidos contra os wheels, mantendo Grafx 0.0.6.
No board Okto Neuron, HTTP 200 com `result_state=available`, componentes graph e
discovery disponíveis, quatro itens cognitivos e timing `empty`; UI completa
confirmada com **Available** e **No samples**. A API Health informou capacidade
`not_applicable` e orçamento 64 MiB por participante, dois leitores (envelope
192 MiB por board). O card Health em execução também mostrou a distinção.
`at_risk / orphan_integrity_warning` foi preservado: disponibilidade não elimina
o aviso real de órfãos. Durante aquecimento/renovação de snapshots, `partial`
continua legítimo até uma consulta obter evidência fresca. Nenhum rebuild,
redrive ou consolidação foi disparado para produzir essas evidências.

O frontend reconsulta snapshots stale/não prontos até três vezes consecutivas,
a intervalos de 10 s, para não manter um aviso antigo após a coleta terminar.
Não reinicia recovery, não substitui páginas adicionais carregadas manualmente,
não repete requisições concorrentes e para diante de erro/restrição/bloqueio.
O limite não é reiniciado por `as_of` ou fingerprints novos. O Refresh manual
permanece disponível caso as três tentativas não obtenham evidência fresca.

Prova a frio na UI instalada (2026-09-14, 18:55 UTC), sem Refresh manual:
HTTP 200 `partial` aos 3,1 s (graph snapshot ainda indisponível), `partial` aos
13,7 s (grafo pronto, paridade/telemetria pendente), `available` aos 24,4 s.
Foram duas reconsultas automáticas, sem ultrapassar o limite. Ambos os percentis
continuaram **No samples** e o aviso real de órfãos permaneceu. Esses tempos
descrevem essa execução local, não são SLA nem benchmark de latência do Grafx.

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
