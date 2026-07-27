---
title: "Auditoria de descontaminação do core — oportunidades remanescentes rumo a lib comum (community + SaaS)"
date: 2026-07-12
status: Findings — auditoria de leitura; nenhum código alterado
method: workflow multi-agente adversarial (4 analistas + verificadores céticos), + verificação manual dos achados de maior impacto
scope: okto-pulse-core 0.3.0 vs okto-pulse-community; objetivo = core agnóstico (hexagonal) como lib comum ao community e ao futuro SaaS
---

# Auditoria de descontaminação do core (2026-07-12)

## Base da análise (fonte de verdade = CÓDIGO)

Fundamentada **exclusivamente no código-fonte e nos gates executáveis**, não em prosa de
documentação. **`CLAUDE.md` e `ARCHITECTURE.md` estão DEFASADOS em relação ao código e NÃO foram
usados como autoridade** — o código mudou e esses docs não acompanharam; aparecem aqui apenas como
*alvo* do achado P6. As classificações de "débito já ledgerado" se apoiam no **código dos gates**
(`core_settings_defaults_gate.py`, `graph_runtime_surface_gate.py`, `adapter_readiness_inventory.py`),
verificados diretamente. Âncoras reproduzidas no código 0.3.0: `src/okto_pulse/core` tem **0 imports
reais** de sqlalchemy/fastapi/starlette/aiosqlite/mcp/uvicorn (as 3 ocorrências de "sqlalchemy" são
docstrings do `core_orm_import_gate.py`); único import real de vendor em `src/` fora de `core/` é
`tools/kg_migrate_schema.py:68` (o tool quebrado do P2); o ledger executável dá **17/2/2**, não os
"11/7/3" que o `ARCHITECTURE.md` afirma.

## Método

4 analistas paralelos (varredura de imports/strings técnicas; inventário de portas vs
implementações concretas; contrato core↔community lido do lado do community; gates/ADRs/débito),
cada achado submetido a um verificador cético independente. O verificador esgotou créditos a meio
da fase (20 dos ~50 achados ficaram `NÃO-VERIFICADO`), então os achados de maior impacto entre
esses foram **verificados manualmente** e estão marcados abaixo. Referências `file:line` relativas
a `src/okto_pulse/` salvo indicação.

## Veredito executivo

**A descontaminação está avançada e os grandes movimentos já foram feitos.** O core src não tem
nenhum import de `sqlalchemy`/`fastapi`/`mcp` (verificado: 0 hits reais), não existem mais
`core/app.py`, `core/repositories/sqlalchemy/*` nem `core/kg/scheduler_singleton.py`, as deps
declaradas são só `pydantic/pydantic-settings/PyYAML`, e há um aparato de fronteira executável
(portas + `RuntimeComposition` + ledger de prontidão 17 ready / 2 blocked / 2 deferred + o auditor
`okto-pulse-core-boundary` com 8 gates). Isso é um core genuinamente hexagonal na dimensão
**relacional e de transporte**.

**Resta uma frente grande e real, e várias pequenas.** A frente grande é o **subsistema de
Knowledge Graph**: o core ainda fixa o dialeto proprietário do grafo (Cypher/Kùzu, `CALL
QUERY_VECTOR_INDEX`, índices HNSW), classifica erros do vendor por substring e coordena locks com
estado global de processo — e **nenhum gate cobre isso** (o AF17 só varre `kg/interfaces` +
tokens de storage físico, não dialeto de query). É exatamente o que bloqueia o swap
"LadybugDB/Kuzu → Neptune" que o próprio capstone declara como alvo. As frentes pequenas são
resíduo de `CoreSettings` (a maior parte já ledgerada como R17/AF37, aguardando execução),
singletons de processo hostis a multi-tenant (parte em R01C), código quebrado empacotado na wheel,
gates vermelhos por wheel stale, e documentação/CHANGELOG desatualizados.

## Mapa por área

| Área | Status | O que resta |
|---|---|---|
| Persistência relacional (SQLAlchemy/UoW) | **débito-aceito** | Strangler estreito; serviços (`core/services`, 53 módulos) ainda recebem sessão relacional duck-typed como `AsyncSession`. Ledgerado (ADR-01 `legacy_application_transitional_debt`). |
| Transporte HTTP/MCP | **débito-aceito** | 0 imports de transporte no core; resta `host/port/mcp_port/cors_origins` em `CoreSettings` (classificados `keep` pelo R17 — a auditoria contesta essa classificação) e o catálogo MCP montado como singleton em import-time. |
| **KG / graph storage (Cypher/HNSW)** | **contaminado** | Dialeto Kùzu emitido pelo core; classificação de erro por substring do vendor; coordenação de lock por estado global. **Sem gate.** É a frente principal. |
| Embeddings / ML stack | **débito-aceito** | `kg_embedding_model = all-MiniLM-L6-v2` + `dim=384` como default do core; community já registra o seu. Ledgerado R17 (register_before_remove). |
| Runtime / estado de processo / multi-tenancy | **porta-ausente (parcial)** | `os.environ` lido em import-time (write_barrier, decay_tick), `@lru_cache` em `get_mcp_settings`, contadores module-level. Parte em R01C. |
| Empacotamento / wheel | **contaminado (housekeeping)** | `tools/kg_migrate_schema.py` quebrado empacotado; wheel instalada stale (deps antigas) deixa 3 gates VERMELHOS; artefatos de runtime no repo root. |
| Contrato core↔community + docs | **contrato-vazado** | 208 module-paths importados (alcança internals do KG); allowlist SaaS obsoleta; ARCHITECTURE.md/CLAUDE.md/CHANGELOG defasados. |
| Testing fakes (`core.testing`) | **limpo** | `fake_saas_relational`/`fake_saas_uow` são fakes in-memory puros com sessão opaca — legítimos, provam substituibilidade SaaS. Só faltam classificação no layer_resolver e decisão de empacotamento. |

---

## Prioridades (ordenadas por impacto no objetivo lib-comum)

### P1 — Descontaminar o subsistema KG do dialeto e da semântica do vendor (a frente principal)

Corroborado independentemente por 3 analistas (todos CONFIRMADO): import-scan F1, ports-providers
F4, community-contract F1; + ports-providers F1/F2/F3, import-scan F2, community-contract F2.

- **Dialeto de query proprietário no core.** `core/kg/search.py:179` e `core/kg/kg_service.py:1301`
  compõem `CALL QUERY_VECTOR_INDEX(...)` (procedure exclusiva Kùzu/Ladybug + extensão VECTOR +
  índices HNSW per-table). As portas que transportam isso (`CypherExecutor.execute_read_only`,
  `GlobalDiscoveryRuntime.execute`) são **passthrough de string**. A inversão é completa:
  `QUERY_VECTOR_INDEX` **não aparece** no repo community — `CommunityKuzuGraphStore.vector_search`
  delega **de volta** para `core.kg.search`, e `adapter_provenance` abençoa `core.kg.search` como
  superfície de contrato **pública**. Falseável (verificado no código): num adapter openCypher sem
  essa procedure, `query_global`/`find_similar_nodes_by_type` retornam `[]` **silenciosamente** (os
  handlers de exceção retornam antes dos fallbacks lineares).
- **Semântica de erro do vendor como lógica de negócio.** `global_outbox.py:35-49` decide
  dead-letter/reset comparando texto de exceção contra marcadores literais do LadybugDB
  (`wal_record.cpp`, `.lbug`, `not a valid lbug database file`); `commit_coordinator.py:41,112-119`
  faz retry classificando `RuntimeError` por substring `"Could not set lock on file"`;
  `primitives.py:2932` congela embeddings no dedup porque o HNSW do Kùzu rejeita `SET` em coluna
  indexada. São decisões de fluxo do core derivadas de limitações de UM produto.
- **Nenhum gate cobre.** O `graph_runtime_surface_gate` (AF17) escaneia só `kg/interfaces/*.py` +
  `schema_contract.py` com tokens de storage/nome (`kuzu`, `ladybug`, `.lbug`, paths), não dialeto
  de query nem classificação de erro; o `adapter_readiness_inventory` marca a migração R-P2-05 como
  "ready", dando a impressão de que já está limpo.

**Proposta.** Extrair uma capacidade de busca vetorial na porta de grafo
(`SemanticGraphStore.vector_search` já existe — usar/estender) e mover a emissão de
`CALL QUERY_VECTOR_INDEX` + o fallback cosseno para o adapter da edição; rerotear os call-sites
internos do core (`search.py`, `kg_service.py` caminho global, `primitives.py`) pela porta. Definir
uma **taxonomia tipada de erro de grafo** (`GraphOpenCorruptionError`, `TransientLockContention`,
`GraphSourceReadError`) na porta e exigir que o adapter traduza as exceções do vendor; o core passa
a fazer `isinstance`. Expor `supports_indexed_column_update` como capability para o core decidir
freeze-vs-update. **Estender o AF17** para caçar `QUERY_VECTOR_INDEX|CREATE_VECTOR_INDEX|VECTOR
extension` e substrings de erro de vendor fora de adapters — senão a descontaminação regride sem o
gate ver.

### P2 — Quick wins de higiene: código quebrado, wheel stale, gates vermelhos (VERIFICADO)

- **Código quebrado empacotado na wheel** (ports-providers F5 / import-scan F5, CONFIRMADO +
  verificado manualmente): `tools/kg_migrate_schema.py:68,74` faz `from sqlalchemy import select`
  (dep não declarada) e `from okto_pulse.core.models.db import Board` — **módulo inexistente**
  (`find_spec` → `None`, `ModuleNotFoundError` garantido). Escapa de todos os gates porque `tools/`
  fica fora dos scan roots. Corrigir reescrevendo `_list_local_boards` pelo padrão de ports (como o
  twin MCP faz) ou remover o caminho `--all-boards`.
- **Wheel instalada stale → 3 gates VERMELHOS** (gates-debt F1, verificado manualmente): a metadata
  da wheel 0.3.0 instalada no venv declara `aiosqlite, authlib, fastapi, mcp, numpy,
  python-multipart, sqlalchemy` enquanto o `pyproject` declara só `pydantic/pydantic-settings/
  PyYAML`. Por isso `dependency-conformance` (R05-E), `conformance-matrix` (FCC-07A) e
  `packaging-ownership` (FCC-07C) saem `exit 1`. **Rebuild + reinstall** da wheel torna os gates
  verdes. (`af41` mcp-runtime-ownership já passa.)
- **Artefatos de runtime no repo root** (gates-debt F12): `dashboard.db{,-shm,-wal}`, dumps de DLQ
  `dlq-*.json`, `dist/`/`build/` antigos. Gitignore + limpar; reconstruir a wheel resolve junto.

### P3 — Executar a retirada R17/AF37 do `CoreSettings` (débito já ledgerado)

Corroborado por import-scan F4/F5/F7, ports-providers F7/F9/F10, community-contract F3/F8,
gates-debt F2. Estado misto — a maior parte é **débito reconhecido aguardando execução**:

- **Tuning do vendor (AF37 planejado, não executado):** `kg_kuzu_buffer_pool_mb`,
  `kg_kuzu_max_db_size_gb` (com validator que codifica "Ladybug requires max_db_size to be a power
  of 2"), `kg_wal_salvage_enabled`/`kg_wal_only_recovery_enabled`. Introduzir aliases neutros
  `graph_runtime_*` consumidos pela porta, mover o validator power-of-2 para o adapter, depreciar os
  nomes `kg_kuzu_*`/`kg_wal_*` na community.
- **ML stack (R17 register_before_remove):** `kg_embedding_model`/`kg_embedding_dim` — default vazio
  no core, community registra MiniLM, `dim` derivada do provider (`provider.dim`). Metade já feita.
- **Layout de disco:** `kg_base_dir = "~/.okto-pulse"` (single-tenant) — já `edition_default_community`
  no R17; concluir a remoção fixando o literal legado no `_derive_paths` da community antes.
- **Transporte (a auditoria CONTESTA a classificação atual):** `host/port/mcp_port/cors_origins`
  estão classificados `core_contract_required/keep` ("No planned removal") no gate R17, mas o core
  **não sobe listener** (`run_mcp_server` é `RuntimeError`) e **não consome** nenhum desses campos —
  os únicos consumidores estão na community (que ainda sobrescreve `cors_origins='*'`). Reclassificar
  para `edition_default_community` com `PublicSettingAlias` + plano de migração (preservando as env
  vars públicas do deploy Docker) e remover `env_file='.env'` da leitura implícita do cwd.

### P4 — Neutralizar singletons de processo hostis a multi-tenant

import-scan F3/F10 (CONFIRMADO), ports-providers F8/F12/F13 (CONFIRMADO), community-contract
F6/F9/F11, gates-debt F8/F10. Parte em R01C.

- **`os.environ` em import-time** congela config por processo, fora do settings port:
  `kg_decay_tick.py:42-43` (`KG_DECAY_TICK_BATCH_SIZE/STALENESS_DAYS` — e **duplica**
  `CoreSettings.kg_decay_tick_staleness_days`, que o tick **ignora**: o knob da UI é hoje inócuo),
  `write_barrier.py:73-76` (modo do barrier). Resolver lazy no call-site via `CoreSettings`/provider.
- **`get_mcp_settings()` `@lru_cache`** + `mcp = CoreMcpCatalog(get_settings()...)` montado em
  import-time (`mcp/server.py:253-261`): singletons de processo que ignoram a `RuntimeComposition`;
  duas composições/tenants no mesmo processo compartilham nome/versão/instructions e a config de auth
  MCP single-tenant (`agent_keys_env`, hoje código morto). Introduzir `build_core_catalog(settings)`
  no composition root (mantendo `mcp` module-level como compat ledgerada) e remover o `lru_cache`.
- **Contadores module-level** em `single_writer_lock.py:31-32` e `commit_coordinator.py:58,72`:
  emitir pela `TelemetryPort` da composição; manter só fallback de teste. Rotear a serialização de
  commit pela `WriteLockPort` (já existe, o caminho de commit a bypassa).
- **Switches soltos** `os.getenv` em `mcp/server.py:2363,1425` e `telemetry/settings.py:252`
  (`OKTO_PULSE_LEGACY_OFFSET`, `_LEGACY_COVERAGE`, `OKTO_PULSE_METRICS_MODE`) — rotear por settings
  para entrarem no inventário R17.

### P5 — Promover o gate `import_boundary` a enforcing (fechar o buraco dos 9 unclassified)

import-scan F8 (CONFIRMADO), ports-providers F14, community-contract F12, gates-debt F5.

O `import_boundary` roda em `mode=bootstrap` (xfail_advisory) com **0 violations** em 541 arquivos,
mas não pode ser promovido porque 9 arquivos são `unclassified_layers` (`core/__init__.py`,
`discovery_intent_catalog.py`, `observability/*`, `runtime_context.py`, `runtime_registry.py`,
`testing/*`). Pior: um arquivo unclassified **não recebe regra nenhuma** (`rule_for` → `None`, o scan
pula) — então esses paths poderiam importar `sqlalchemy` amanhã sem virar blocking. **Mapear os 9 no
`layer_resolver` (current-tree-v4):** runtime_context/runtime_registry/`core/__init__` → composition;
`observability/*` → application; `discovery_intent_catalog` → domain; `testing/*` → nova camada
`test-support` (regra: só ports+domain). Depois flipar o default do CLI para `blocking` e tratar
unclassified como blocking. Fechar também os 2 `blocked` do ledger (storage R02, mcp_auth_context R06).

### P6 — Formalizar e versionar o contrato core↔community; atualizar docs

community-contract F4/F5 (NÃO-VERIF), gates-debt F7/F11 (verificados manualmente).

- **Superfície pública não versionada:** a community importa **208 module-paths distintos** do core,
  alcançando internals de KG (`kg_service`, `cypher_templates`, `single_writer_lock`,
  `dedup_migration`...); o `CORE_ALLOWLIST_V1` do SaaS tem ~40 entradas e referencia
  `core.models.db.PermissionPreset` — **módulo inexistente**. Publicar uma superfície pública
  explícita (`okto_pulse.core.public` / `__all__` ledgerado) como fonte única para os dois
  consumidores + gate simétrico no community.
- **Contaminação inversa (Resource Gate):** as regras de negócio do Resource Gate (~1504 linhas
  auto-denominadas "domain service") vivem em `community/adapters` misturadas a SQLAlchemy, e o core
  as consome por **herança de classe concreta** dependendo de métodos protegidos (`_load_parent_refs`,
  `_load_active_marks`). A "porta" é herança de implementação — um SaaS teria de reimplementar as
  regras. Subir as regras para `core/services` e reduzir a porta a um repositório de leitura estreito.
- **Docs defasadas (verificado):** `ARCHITECTURE.md:58,206` diz "SQLAlchemy adapters still live in
  core" e "21 seams: 11 ready, 7 blocked, 3 deferred" (o ledger executável dá **17/2/2**);
  `CLAUDE.md:6-7` descreve o core como "SQLAlchemy models, FastAPI routes, MCP server". Renderizar as
  tabelas do ARCHITECTURE.md a partir do ledger executável (como já se faz com
  `af35_relational_ownership_matrix`) + drift-test; reescrever a abertura do CLAUDE.md.
- **CHANGELOG mudo:** `[Unreleased]` vazio, última entrada 0.2.5, mas o pacote é **0.3.0** — todo o
  programa R01-R17/AF24-AF41/FCC-07 invisível para consumidores. Escrever a entrada 0.3.0 (deps do
  core reduzidas; extração de SQLAlchemy/FastAPI/MCP-runtime; ports novos; gates de fronteira) — é a
  notificação de breaking-change que a lib deve aos consumidores.

## Proveniência

Workflow `wf_dbeac5dc-b2a` (2026-07-12, 55 agentes, ~3,6M tokens; 4 analistas + verificadores
céticos). O verificador esgotou créditos: 16 CONFIRMADO + 14 PARCIAL vieram com verificação
adversarial completa; 20 NÃO-VERIFICADO (dos quais os de maior impacto — gates vermelhos, tool
quebrado, drift de docs — foram verificados manualmente e confirmados). Dados brutos por achado
(claim/evidência/proposta/veredito): sessão Claude Code `769fbe42`, scratchpad
`decontam_result.json` / `decontam_findings.txt`.
