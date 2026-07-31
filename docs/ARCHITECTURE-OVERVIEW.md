# Architecture — okto-pulse-core

How the core is layered, what it owns, and the boundary rules an edition must respect.

> For the deep hexagonal reference — port-by-port adapter readiness ledger and provider registry —
> see [`ARCHITECTURE.md`](../ARCHITECTURE.md) at the repository root.
> For the full extension-point catalogue, see [`PORTS.md`](./PORTS.md).


For the backend hexagonal refactor, the full port inventory and the executable
adapter-readiness ledger, see [`ARCHITECTURE.md`](../ARCHITECTURE.md). It names
the runtime, storage/MCP, initialization, telemetry, repository/UoW, Knowledge
Graph and inbound adapter ports, plus terminal boundary checks that prevent
concrete adapters from returning to Core. This README keeps only the runtime
topology summary.

```text
Single Python process
|-- uvicorn :api
|   `-- FastAPI REST API under /api/v1
`-- uvicorn :mcp
    `-- MCP ASGI app under /mcp using streamable HTTP

Shared by composition:
- SQLAlchemy session factory registered by the edition lifespan
- RuntimeComposition providers for settings, auth, storage, events and scheduler control
- KGProviderRegistry providers supplied by the active edition
- MCP credential resolved by the edition-owned MCP host request context
```

The Community package mounts the bundled React SPA and owns the local-first
runtime adapters. Core keeps the REST/MCP contracts and the application rules
that those transports expose.

Relational dependency cleanup is terminal. `asyncpg`, SQLAlchemy mappings,
engines and session factories are absent from Core and audited across source,
package metadata, lock data and wheel metadata. `core.infra.database` is now an
adapter-neutral compatibility facade over relational runtime ports; concrete
SQLite/SQLAlchemy lifecycle and persistence live in Community.

AF35 adds the final relational ownership source map in
[`docs/architecture/af35_relational_ownership_matrix.md`](./architecture/af35_relational_ownership_matrix.md).
That document is rendered from `run_af35_s5_relational_final_gate()` and guards
the current matrix for `adapter_owned`, `migrated_clean`,
`temporary_core_exception`, `uow_seam`, `test_only`,
`non_productive_reference` and `unowned` residues. The executable gate, not this
README prose, is the source of truth for counts and stale-exception failures.

The moved Community-owned surfaces include relational mappings and repositories,
sentence-transformers embeddings, cross-encoder rerank, Ladybug/Kuzu board graph
adapters, global discovery runtime, board source reads, rebuild ingestion and the
APScheduler runtime. Core publishes domain/application dependencies and ports;
concrete local runtimes are owned by Community and guarded by executable boundary
checks.

AF-05/AF40 dependency owner matrix. The source of truth is
`dependency_ledger.py`, `CANONICAL_AF40_DEPENDENCY_TOKENS`,
`CANONICAL_TEMPORARY_EXCEPTION_TOKENS` and `conformance_matrix.py`; README text
must follow those gates, not the other way around.
F14 dependency ownership keeps concrete runtime dependencies out of the
published Core distribution and assigns local implementations to Community.

| Dependency | Status | Current owner and evidence | Community packaging note |
| --- | --- | --- | --- |
| `aiofiles` | `removed` | AF-05 removed the orphaned core runtime dependency. It must stay absent from the core manifest, lock, wheel metadata and runtime imports; `dependency_conformance` fails closed if it reappears and `conformance_matrix` emits `removed_dependency_absent`. | Not Community-owned and not moved to Community without a direct adapter consumer. A stale Community lock can still show the published core dependency until the isolated artifact smoke resolves against the local core build. |
| `requests` | `community_owned` | AF40-R1 moved the concrete telemetry HTTP transport ownership to Community. `dependency_conformance` now blocks core manifest, lock, wheel or runtime-import reintroduction. | Declared directly by Community and used by `community/adapters/telemetry_sender.py`; `community_packaging_audit` requires the declaration for `local_telemetry_store`. |
| `chardet` | `community_owned` | AF40-R1 moves the requests/telemetry charset companion with the transport pair. It is no longer in `CANONICAL_TEMPORARY_EXCEPTION_TOKENS`; it is covered by `CANONICAL_AF40_DEPENDENCY_TOKENS`. | Declared directly by Community together with `requests`; moving only one token fails the ownership matrix/oracle expectations. |
| `aiosqlite` | `community_owned` | F14 removed the local SQLite driver from the Core source, manifest, lock and wheel. Core exposes relational ports only. | Declared directly by Community because its SQLAlchemy adapter selects `sqlite+aiosqlite` URLs at runtime. |
| `numpy` | `community_owned` | F14 removed the numeric implementation dependency from the Core source, manifest, lock and wheel. Core exposes embedding/rerank ports and deterministic policy only. | Resolved transitively by the Community `sentence-transformers` implementation stack; Community wheel smoke proves the installed runtime. |
| `apscheduler` | `community_owned` | AF31-S1R moved the concrete scheduler runtime out of core. Core keeps only `JobSpec`/`SchedulerControl` and the KG daily tick policy; `dependency_conformance` now blocks manifest, lock, wheel or runtime-import reintroduction. | Declared by Community and mapped in `community/adapters/scheduler.py` from core `JobSpec` to APScheduler/`IntervalTrigger`. |

`CANONICAL_TEMPORARY_EXCEPTION_TOKENS` is empty. F16 treats any new accepted
dependency exception as a blocking nonzero budget.

AF41 MCP runtime ownership: `fastmcp`, `uvicorn[standard]` and `wsproto` are
Community serving dependencies. Core exposes `build_mcp_asgi_app()` and `mount_mcp()`
as port-backed compatibility facades, but its manifest, lock,
wheel metadata and source must not declare or import the concrete server
runtime. `mcp_runtime_ownership_gate.py` enforces that boundary; the deprecated
`okto_pulse.core.mcp.server.run_mcp_server` shim always rejects listener startup.

AF41 provider preservation: this change does not recreate the delivered MCP
instruction, resource, version, auth or trace adapters. Core keeps only the
provider seams (`register_instruction_provider`, `register_resource_catalog`,
`register_package_version_provider`, `McpAuthenticator` and `McpTraceSink`);
Community supplies the concrete local providers through its composition root.

AF37 graph runtime compatibility ledger. The source of truth is
`LEGACY_GRAPH_RUNTIME_COMPATIBILITY_LEDGER` in
`graph_runtime_surface_gate.py`; README text follows that executable gate.
These names remain public compatibility surfaces only. New core code should
consume `GraphRuntimeStore`, `GraphLifecycle`, `GraphTransaction`,
`GraphSchemaManager` or `SemanticGraphStore`.

| Legacy token | Preferred neutral surface | Owner | Removal criterion |
| --- | --- | --- | --- |
| `board_kuzu_path` | `GraphRuntimeStore.exists/graph_state/footprint` | `okto-pulse-core/kg + okto-pulse-community/adapters` | Remove after Community and fixtures stop importing board path symbols and startup/lifecycle checks use runtime ports. |
| `KuzuNodeRef` / `kuzu_node_id` | graph node reference audit ledger / `graph_node_id` | `okto-pulse-core/kg-governance` and `okto-pulse-core/kg-api-compat` | Add neutral aliases and prove payload/table parity before any versioned removal. |
| `kg_kuzu_*` | `graph_runtime_*` settings aliases | `okto-pulse-core/settings + okto-pulse-community/settings` | Add API/UI/env alias parity, then retire legacy names through public config stability gates. |
| `graph_lbug_bytes` | `storage_footprint_proxy.total_bytes/primary_bytes` | `okto-pulse-core/kg-health` | Move UI/API consumers to neutral footprint fields before removing the legacy byte field. |
| `kuzu_error` / `kuzu_lock_retries_5m` | `graph_backend_error` / `graph_lock_retries_5m` | `okto-pulse-core/kg-api-compat` and `okto-pulse-core/kg-operations` | Add neutral fields/types and keep legacy mappings for one compatibility window before deprecation. |

AF33 capstone ownership matrix. The marked table is rendered from
`CAPSTONE_OWNERSHIP_MATRIX` and must stay byte-identical to the Community
README block. The gates listed here are executable; README prose follows them.

<!-- AF33-CAPSTONE-MATRIX:BEGIN -->
| Surface | Core contract | Community/local adapter | SaaS swap target | Executable gates |
| --- | --- | --- | --- | --- |
| Relational runtime | repository/UoW and schema lifecycle ports; no ad-hoc dialect or engine/session factory bypass | SQLite/SQLAlchemy adapters in community.adapters.sqlalchemy_* and relational_schema_lifecycle | SQLite -> Aurora/Postgres | run_relational_residue_gate, audit_dependency_conformance, audit_community_core_import_boundary |
| KG graph runtime | KG interfaces, policies and adapter-neutral schema compatibility helpers | LadybugDB/Kuzu adapters in community.adapters.kuzu_* and global_discovery_runtime | LadybugDB/Kuzu -> Neptune | audit_dependency_conformance, ImportBoundaryGate, audit_community_core_import_boundary |
| Durable files and artifacts | StorageProvider, RebuildAuditArtifactStore and CognitivePendingWorkProvider contracts | filesystem storage, upload_dir, rebuild audit storage and cognitive-pending providers | filesystem -> S3 | run_rebuild_audit_storage_gate, run_core_settings_defaults_gate, run_public_config_stability_gate |
| Telemetry effects | TelemetryPort contracts, event schema and privacy policy | local JSONL store, state files, beacon sender and product telemetry adapters | local telemetry files/API -> AWS telemetry API | run_telemetry_store_ownership_gate, run_telemetry_sender_ownership_gate, run_telemetry_product_ownership_gate |
| Scheduler/runtime effects | JobSpec, SchedulerControl and KG daily tick policy | APScheduler-backed SingletonSchedulerControl | APScheduler local runtime -> runtime scheduler adapter | SchedulerControlSymbolGate, scheduler_signal_conformance |
| MCP resources and versions | MCP instruction/resource/version provider ports and stable public catalog | Community resource catalog, capability descriptors and package version wiring | local catalog/version reads -> deployment provider | run_public_config_stability_gate, register_instruction_provider, register_package_version_provider |
<!-- AF33-CAPSTONE-MATRIX:END -->

The AF-11 import-boundary pass is application-first. Its done criterion is
`ImportBoundaryGate(mode="bootstrap").observed_value == 0` for blocking
application-layer violations in the real source tree. It does not claim to retire
the inbound/outbound/legacy baseline debt above; those remain governed by the
adapter readiness ledger and their existing gates.

| Category removed from application use cases | Application-facing contract | Current implementation/owner | Evidence |
| --- | --- | --- | --- |
| KG consumers (22 historical imports) | `okto_pulse.core.services.application_kg` facade for governance, dashboard readers, consolidation primitives, cognitive readiness/closeout and canonical parity | Core owns the application/KG rules; the facade delegates to the current `core.kg` services while Community continues to provide local graph/runtime adapters | R01A KG parity suites, `tests/test_boundary_audit_12.py`, `tests/test_conformance_suite_15.py` |
| Transport/schema DTOs (17 historical imports) | `okto_pulse.core.services.application_schemas` sanctioned DTO facade | Core owns REST/MCP contracts and DTO compatibility; this pass removes direct `core.models.schemas` imports from application use cases without changing payload semantics | R01A spec/card/MCP parity suites, boundary/conformance oracles |
| Permission checks (6 historical imports) | `okto_pulse.core.services.permission_policy` application permission facade | Core owns transition and authorization policy; the facade preserves the existing `core.infra.permissions` evaluator and its patchable test seam | R01A permission/spec/story/ideation parity suites |
| Mutable persistence marking (5 historical imports) | `ApplicationPersistencePort` mutation methods | Core owns mutation intent and application rules; Community's SQLAlchemy persistence adapter owns mutable-column tracking and flush mechanics | Card/ideation mutation parity suites and boundary/conformance oracles |
| Ratchet evidence | `ImportBoundaryGate` violation evidence now includes `category`; AF-11 tests pin the eliminated application import inventory | Core boundary gate owns regression detection. The ratchet fails on real-tree reintroduction and on negative fixtures rather than by rebaselining or downgrading violations | `test_af11_application_import_ratchet_real_tree_stays_zero`, `test_af11_application_import_ratchet_negative_fixture_reports_categories` |

AF-20 hardens the same boundary policy for non-relational import-boundary
baseline debt. New non-relational baselines are fail-closed unless they are
declared in `IMPORT_BOUNDARY_BASELINE_LEDGER` with per-item owner, reason,
removal criterion, source spec/wave and risk. Relational debt remains governed
by the existing R01B/R01C ratchet and is not rebaselined by AF-20. The existing
`okto_pulse/core/ports` package is a real pure `ports` layer, not a
`future_target`, so its imports are governed like domain/application code.
Runtime singleton baselines must have owner, target provider and retirement
criterion in `SINGLETON_LEDGER` or the per-occurrence
`RUNTIME_SINGLETON_BASELINE_LEDGER`; counters, metric totals and guard flags are
explicit non-runtime exemptions. Adapter-specific debt belongs in Community when
the concrete dependency is local-first; core keeps the rule, gate and transition
logic that future SaaS editions must share.

AF-21 and AF-28 define the public surface that Community adapters may consume
while the remaining reach-ins are retired. Public entry points include
`okto_pulse.core.services.application_kg` for KG/governance orchestration,
`okto_pulse.core.services.application_agents` for agent credential/auth/ACL
primitives,
`okto_pulse.core.services.application_startup` for startup self-heal helpers,
`okto_pulse.core.mcp.build_mcp_asgi_app` and the other `okto_pulse.core.mcp`
composition hooks for MCP mounting/session binding,
`okto_pulse.core.ports.relational_runtime` for edition-owned relational
lifecycle/session binding,
`okto_pulse.core.ports.runtime_workers` for edition-owned worker composition,
and adapter-neutral KG facades in `core.kg.board_source_store`,
`core.kg.board_rebuild_adapter`, `core.kg.tier_power`, `core.kg.scoring`,
`core.kg.session_manager` and `core.kg.global_discovery.schema`.
These facades intentionally expose rules, policy helpers and normalized values,
not concrete database, graph, scheduler, file or telemetry implementations. They
must not require a future SaaS adapter to emulate SQLAlchemy sessions, LadybugDB
DDL/vector-index layout, filesystem reopen/fsync behavior, concrete worker
classes or private MCP server symbols.

AF-28 also makes global-discovery DDL ownership explicit. Core keeps the
adapter-neutral schema contract and compatibility helpers such as
`ensure_decision_digest_layer_column`; concrete node/relationship DDL and
vector-index definitions are edition-owned and live in the active graph runtime
adapter. AF-29 follows the same rule for durability mechanics: core owns the
outbox, cognitive closeout and drain policy, while edition adapters own durable
artifact flush/probe behavior and pending-work enumeration through
`GlobalDiscoveryRuntime.flush_after_write_batch` and
`CognitivePendingWorkProvider`.

`build_mcp_asgi_app(trace_sink=None)` and `mount_mcp(app, trace_sink=None)` are the two helpers
exposed from `okto_pulse.core.mcp`. Pick `build_mcp_asgi_app()` to drive a separate uvicorn `Server`
(the community edition does this for the `--mcp-port` listener) or `mount_mcp(app)` to mount the MCP
sub-app under an arbitrary path on an existing FastAPI app. The optional `trace_sink` is a core port
(`McpTraceSink`); core never resolves environment variables, creates directories or writes trace
files itself — tracing only happens when an edition injects a sink.

---

[← Back to README](../README.md)

---

## Glossary

For a complete list of domain terms and definitions, refer to the [Glossary](./GLOSSARY.md).