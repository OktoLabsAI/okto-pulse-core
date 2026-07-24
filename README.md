# okto-pulse-core

Domain and application engine for [Okto Pulse](https://github.com/OktoLabsAI/okto-pulse), with transport-neutral ports, contracts and MCP commands.

> **Ship with AI. Stay in control.**

> **You probably want to install [`okto-pulse`](https://pypi.org/project/okto-pulse/) instead.**
> This package is the internal engine. The `okto-pulse` package provides the CLI, frontend, and everything you need to get started.

## What's inside

- **0 SQLAlchemy models** — Core owns no concrete relational mappings. This is checked by scanning for `__tablename__` assignments anywhere under `core/`; the Community edition owns the SQLAlchemy model and repository adapters.
- **34 service classes** — Full business logic with governance rules, board agent governance, resource propagation + lineage, bug-regression workflow, archive/restore, traceability and board-level resource readiness. Source: classes ending in `Service` under `core/services`.
- **0 API route modules** — Core owns application contracts and use cases, not concrete FastAPI routers. The count scans `core/api/*.py`; Community owns the REST adapter and route modules.
- **17 governance gates** — Resource readiness, resource-to-task coverage, spec coverage, validation, evaluation, task completion, cognitive closeout, architecture-findings, evidence, bug traceability and sprint health controls.
- **281 MCP tools** — Complete Model Context Protocol command catalog for AI agent integration, counted from the transport-neutral Core catalog after importing the server, including:
  - Pipeline CRUD (Ideation, Refinement, Spec, Sprint, Card)
  - Q&A and choice questions across every entity
  - Mockups (HTML+Tailwind, sanitised) and Knowledge Bases at spec/refinement/card scope
  - Decisions with supersedence and coverage gates
  - Per-card Knowledge attachment lifecycle (`add_card_knowledge` and friends)
  - 62 Knowledge Graph tools (consolidation, query primary/power, health, dead-letter, schema-migrate, decay tick controllability, board rebuild and global discovery recovery preflight/confirm/run)
  - Community runtime exposure: 281 core MCP tools, 0 community-only MCP tools
- **Application composition contracts** — edition-neutral runtime, auth, storage, persistence, graph, telemetry and transport ports; concrete app construction belongs to the edition
- **Hexagonal backend ports** — runtime, telemetry, repository/UoW and KG provider seams, plus the adapter readiness ledger, documented in [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- **Knowledge Graph contracts and orchestration** — graph schema vocabulary, query/consolidation semantics, deterministic + cognitive workers, 11 node types and **13 relationship types**. Source: `len(KGEdgeType)` in `core/kg/schemas.py`; the concrete LadybugDB/Kuzu board and global graph runtimes are supplied by the active edition
- **Bounded operational metric samples** — governance, architecture, bug-regression, resource-lineage and global-discovery observability keep capped diagnostic samples. Global-discovery count APIs remain monotonic totals and do not derive totals from the retained sample ring.

## Governance Gate Surface

Okto Pulse currently documents and enforces **17 named governance gates**:

| Gate family | Gates |
| --- | --- |
| Resource readiness | Resource readiness; resource-to-task coverage |
| Spec coverage | Scenario/test coverage; functional requirement/business rule coverage; technical requirement/task coverage; API contract/task coverage; active decision/task coverage |
| Validation and evaluation | Spec validation; spec qualitative evaluation; task validation |
| Execution quality | Task start/spec readiness; task conclusion; cognitive closeout; architecture-findings done; test evidence; bug test-first/traceability |
| Sprint health | Sprint closure/evaluation |

The two execution-quality additions introduced in 0.2.3 — **cognitive closeout** (a `done` transition is blocked while active cognitive-consolidation items remain) and the **architecture-findings done gate** (active architecture warnings block `spec`/card `done`) — remain enforced in the current release.

## Architecture

For the backend hexagonal refactor, the full port inventory and the executable
adapter-readiness ledger, see [`ARCHITECTURE.md`](./ARCHITECTURE.md). It names
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
[`docs/architecture/af35_relational_ownership_matrix.md`](./docs/architecture/af35_relational_ownership_matrix.md).
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

`build_mcp_asgi_app(trace_sink=None)` and `mount_mcp(app, trace_sink=None)` are the two helpers exposed from `okto_pulse.core.mcp`. Pick `build_mcp_asgi_app()` to drive a separate uvicorn `Server` (the community edition does this for the `--mcp-port` listener) or `mount_mcp(app)` to mount the MCP sub-app under an arbitrary path on an existing FastAPI app. The optional `trace_sink` is a core port; core never resolves envir…2219 tokens truncated…egrity (KG-ZO-01/02)** — a node-connectivity pre-commit guard that refuses to commit orphans, plus orphan backfill, health reporting and rebuild visibility.
- **KG cognitive consolidation & source governance (KG-03/03A)** — cognitive item control + candidate-decision promotion (`candidate_decision_store`, `cognitive_badge_resolver`), per-concept `source_ref`, and dedup granularity with SUPERSEDE wiring + counted/audited merge.
- **KG health honesty & degraded-mode resilience (F3/F4/F16/F17, R2c)** — signal clarity (scheduler/decay debt ≠ corruption; footprint = file-size proxy), a resilient/observable decay tick, a uniform `graph_unavailable` envelope, a health-aware closeout gate + tick admission, real memory-pressure instrumentation and opt-in DLQ auto-drain.
- **Governance, lineage & gates (BG-01, RG-01, AFG)** — `critical_context_guard` (critical mutations resolve + fingerprint full entity context first), the `resource_lineage` provenance resolver with N/A inheritance, and the Architecture Finding Done Gate wired into `spec → done`. Two gates moved from defined to enforced (15 → 17): **Cognitive Closeout** and **Architecture Findings**.
- **MCP token-optimization & projection (R1–R5)** — `payload_budget`, `payload_compaction`, `projection_envelope`, context/copy projection, `kg_query_safety`, `tool_family_registry`; schema honesty (`anyOf array|string`), positional → canonical id/ref migration (`linked_requirements → FR`, `linked_criteria → AC`), and the pre-flight checklist as a real `okto-pulse://workflows/preflight` resource. Surface grew to **215 tools**.
- **Bug-regression workflow** — scenario reuse + test-gate remediation, operator-facing bug guidance/error remediation, and a post-closure hotfix lane.
- **Structured spec entities + API-contract hardening** — structured editing + `test_scenario` CRUD (closes the NC-9 bypass), `contract_type` discriminator + HTTP-method enum, granular per-requirement N/A for IR/OR/contract, and structured choice fields.
- **Analytics & telemetry** — IR/OR coverage calculator with a cancelled-card filter, Decision-coverage surfacing, and beacon-off metrics modes.

See `CHANGELOG.md` for the per-subsystem diff-level rationale.

### 0.2.2

Patch release rolling up the post-0.2.1 fixes. Same surface as `0.2.1` plus:

- **SDLC E2E gate polish (4 issues from the 2026-05-17 ceremonial run)** —
  - `submit_spec_validation` now runs the AC → test-scenario coverage check as its first pre-requisite, so a spec with uncovered ACs fails BEFORE the validation locks the content (previously the move → done gate raised the same error but only after the spec was already locked).
  - The "FR has no linked business rule" error now uses an `[i]` index marker rather than the duplicated `FR{i}:` prefix that collided with the author's own `FRN:` label and produced strings like `"FR1: FR2: ..."`.
  - `okto_pulse_link_task target_type='decision'` now spreads the saturation envelope into its success JSON, in parity with the other six target types. A parametrised dispatcher test pins the contract so a future eighth helper can't regress it silently.
  - `okto_pulse_evaluate_ideation` docstring now states the `status='evaluating'` pre-requisite and the full `draft → review → approved → evaluating → done` flow up front. The tool deliberately does not auto-promote — each transition is an explicit gate decision.
- **Agent instructions split + reference catalogue** — `agent_instructions.md` trimmed by extracting the static reference material into three new MCP resources (`okto-pulse://reference/list_tools`, `tools_catalog`, `transitions`). Workflow docs (`refinements`, `specs`, `stories`) refreshed in lockstep.
- **MCP server module slim-down** — `core/mcp/server.py` lost ~970 lines of helpers that now live in supporting modules. Public tool surface is unchanged; this is purely an organisational refactor on top of `0.2.1`.

Anti-regression tests added for each of the four E2E fixes (`tests/test_spec_validation_gate.py::TestAcScenarioPrecheck`, `TestFrCoverageMessageFormat`, `tests/test_link_task_dispatcher.py::test_link_helper_returns_saturation_envelope`). The `submit_spec_validation` baseline hash in `tests/.cache/validation_gates_baseline.txt` was bumped to reflect the intentional addition of the new AC → scenario pre-check.

See `CHANGELOG.md` for the diff-level rationale and the per-fix bug card references.

### 0.2.1

#### Branch changelog (`feature/0.2.1`)

This branch turns 0.2.1 into the IR/OR, telemetry, resource-propagation and MCP-surface optimization release.

- Added local-first telemetry and metrics infrastructure: product event schema, settings model, local event store, sender, privacy-aware service layer, metrics REST API and tests for local-only, disabled and anonymous-beacon modes.
- Added first-class Integration Requirements (IR) and Observability Requirements (OR) across database models, Pydantic schemas, REST responses, MCP handlers, permission registry and presets.
- Extended spec context, sprint context and coverage summaries so agents and UI callers can see IR/OR items alongside technical requirements, business rules, API contracts, decisions and test scenarios.
- Added service-layer spec resource propagation. Knowledge Base entries, architecture designs and mockups can be copied from specs to cards automatically when board settings enable auto-derive resources.
- Hardened propagation triggers on card creation/linking, spec resource edits and architecture updates so downstream task cards stay self-contained without relying on "see the spec" references.
- Added granular IR/OR permissions (`read`, `create`, `link_task`) and enforced them consistently across API, MCP and permission presets.
- Added four consolidated MCP list handlers: `okto_pulse_list_by_board`, `okto_pulse_list_qa`, `okto_pulse_list_knowledge` and `okto_pulse_list_snapshots`.
- Added server-side MCP filter validation and JSON-string filter decoding so tool transports can pass either dict objects or JSON-encoded filter strings.
- Split the large root MCP agent instructions into a compact pre-flight plus 12 lazy MCP resources under `okto-pulse://workflows/...` and `okto-pulse://reference/...`.
- Added a runtime MCP schema-generation pilot for card CRUD tools, backed by Pydantic v2 model schemas and a snapshot fixture.
- Added minimal-envelope response modes and token-optimization refinements for agent workflows, including tighter list/context payloads and refreshed workflow documentation.
- Added cursor-based keyset pagination to `okto_pulse_get_activity_log`, including opaque `next_cursor`, invalid-cursor structured errors and SQLite timestamp normalization for microsecond-safe pagination.
- Improved activity-log summaries and card-move logging by covering more action shapes and de-duplicating noisy `card_moved` entries.
- Hardened architecture services with semantic normalization and additional validation coverage used by the community Architecture UI and Excalidraw import flow.
- Added focused regression coverage for spec resource propagation, telemetry, IR/OR requirements, consolidated MCP handlers, MCP resources, schema generation, activity-log pagination and story/refinement regressions.

#### Post-release polish already on the branch

- Aligned authoritative handler signatures for the four consolidated MCP list handlers.
- Unified list handler defaults to `limit=100` for consistency across old and consolidated paths.
- Clarified that the implementation keeps `board_id` for ACL/auth and uses `entity_type/entity_id` naming for knowledge listings.

#### SDLC E2E gate polish (4 issues from end-to-end run 2026-05-17)

A full ceremonial E2E run (Story → Ideation → Refinement → Spec → Sprint → Cards → Sprint closeout) on the `E2E` board surfaced four small but recurring issues across the spec validation gates, error messages and tool response shapes. All four were fixed in the same `Unreleased` cycle and validated in-vivo against the live MCP server. See `CHANGELOG.md` for the full diff and rationale; the short summary:

- `submit_spec_validation` now runs the AC → test-scenario coverage check as the first pre-requisite, so a spec with uncovered ACs fails BEFORE the validation locks it. The error message also reminds the caller that the spec is locked after a successful validation.
- The "FR has no linked business rule" error message now uses an `[i]` index marker instead of `FR{i}:`, removing the confusing `FR1: FR2: ...` duplication that occurred whenever the FR text already started with its own label.
- `okto_pulse_link_task` with `target_type='decision'` now returns the same `saturation` envelope as the other six target types. Previously only the decision branch returned the bare `{success, decision_id, card_id, linked_tasks}` shape, breaking agents that drive "continue linking vs submit validation" off the saturation signal.
- The `okto_pulse_evaluate_ideation` MCP docstring now states the `status='evaluating'` pre-requisite and the full `draft → review → approved → evaluating → done` flow up front, so agents stop discovering the requirement by trial and error.

Anti-regression tests were added for each fix (`test_spec_validation_gate.py::TestAcScenarioPrecheck`, `TestFrCoverageMessageFormat`, and `test_link_task_dispatcher.py::test_link_helper_returns_saturation_envelope`). The `submit_spec_validation` baseline hash in `tests/.cache/validation_gates_baseline.txt` was bumped to reflect the intentional addition of the new coverage call.

### 0.2.0

#### Branch changelog (`feature/0.2.0`)

This branch turns 0.2.0 into the governed SDLC + Knowledge Graph release.

- Added Stories and Topics as pre-ideation intake primitives, including REST/MCP services, permissions, lifecycle rules, story-to-ideation traceability and the rule that a Story can reference only one Ideation while an Ideation can reference many Stories.
- Added Resource Gate readiness across Architecture, Mockups and Knowledge Base, with reversible N/A justification, entity-level readiness summaries and MCP guardrails that keep deterministic resource checks out of ad-hoc agent judgement.
- Hardened agent instructions for ambiguity handling: agents are directed to ask more clarification questions, prefer multiple-choice questions with recommendations when possible, and preserve an additional comment path for user nuance.
- Added Ideation Knowledge Base support and propagation, plus lineage/reporting improvements so specs, sprints, tasks, tests and bugs remain traceable even when a flow intentionally starts at Spec without a root Ideation.
- Expanded deterministic KG ingestion for specs, cards, bugs, tests, outcomes, requirements, criteria, constraints, API contracts and decisions, including resolved Bug `originates_from` and `covered_by` edges and schema migration coverage for those relationship tables.
- Strengthened KG schema lifecycle and graph runtime resilience: per-board schema bootstrap/migration, edge metadata migration, entity dedup support, Kuzu memory/runtime settings, vector-extension loading on hot-path graph connections and richer health/dead-letter diagnostics.
- Improved KG query/display contracts: `/kg/boards/{board_id}/graph` now accepts a node `type` filter, `/nodes` total hints remain filter-aware, graph stats expose node/edge histograms and tests cover pagination, type filtering and schema edge counts.
- Fixed guideline creation/parsing paths that could reject inline guideline additions with 422 responses.
- Preserved test scenario evidence in REST response schemas, including `latest_evidence` fallback data, so UI audit surfaces can expose recorded execution proof for Test cards.
- Added and expanded focused tests for Stories, Topic permissions, Resource Gate, Ideation KB, guidelines, deterministic KG workers, graph pagination, schema migration, traceability reports, presets and MCP registration contracts.

#### Fix C: single-process, dual-port serve (Kùzu lock contention)

`okto-pulse serve` now runs API/UI **and** MCP from a **single Python process** but on **two different ports** (`--api-port` defaults to 8100, `--mcp-port` defaults to 8101). Two `uvicorn.Server` instances run concurrently inside one `asyncio.gather` — the embedded graph runtime is owned by exactly one OS process (no inter-process lock contention), and the two listeners share the registered session factory plus the runtime/KG registries supplied by the active edition.

What you get:
- **No Kùzu file-lock thrash** — the embedded DB does not support multiple writers, so a single Python process is the only safe topology. The `kg.db_open.lock_retry path=... attempt=N/5` warnings disappear.
- **Independent ports** — keep `:8100` for the SPA fetches and `:8101` for the MCP HTTP transport, unchanged from earlier releases.
- **One lifespan** — `init_db`, KG worker startup, scheduler boot, and `register_session_factory` all run once on the API listener; the MCP sub-app picks up the registered factory automatically.

Public surface:
- `okto_pulse.core.mcp.build_mcp_asgi_app(trace_sink=None)` — delegates the Core command catalog and optional `okto_pulse.core.ports.McpTraceSink` to the MCP host selected by the edition composition root. Core does not construct HTTP middleware or an ASGI listener.
- `okto_pulse.core.mcp.mount_mcp(app, mount_path="/mcp", trace_sink=None)` — delegates mounting of the same command catalog to the selected edition host.
- `okto_pulse.core.mcp.register_session_factory(factory)` — call from the API lifespan so the MCP sub-app finds the DB. Idempotent.

#### Spec Skills entity removed in its entirety

The experimental "skills" feature on the spec entity is gone. Adoption was zero in real boards and knowledge entries already cover the reusable-context use case more naturally — the dedicated tab, MCP tools, REST endpoints and ORM table were paying recurring maintenance cost without return.

What goes away:
- **5 MCP tools removed** — `okto_pulse_create_spec_skill`, `okto_pulse_delete_spec_skill`, `okto_pulse_spec_skill_retrieve`, `okto_pulse_spec_skill_inspect`, `okto_pulse_spec_skill_load`.
- **4 REST endpoints removed** — `GET / POST / PATCH / DELETE /api/v1/specs/{spec_id}/skills` (and the `{skill_id}` variants).
- **5 permission flags removed** — `spec.skills.{read,load,create,delete,recall}` from the registry and from every preset.
- **Database table dropped** — `spec_skills`. Migration is idempotent (`DROP TABLE IF EXISTS`); no downgrade — the data is gone.
- **Pydantic schemas removed** — `SkillSectionSchema`, `SpecSkillCreate`, `SpecSkillUpdate`, `SpecSkillResponse`, `SpecSkillSummary`. The `skills` field is gone from `SpecResponse`.
- **`agent_instructions.md` scrubbed** — Quick Navigation, the dedicated Spec Skills section, the spec-authoring workflow step and the destructive-operations row no longer reference skills.

Reader-side defensive handling: `BaseSchema` now sets `extra="ignore"` so historical payloads still carrying a `skills` field validate silently — no warning, no log, no error. There is nothing to migrate; the field is dropped on read.

Use **knowledge entries** (`spec_knowledge`, `card_knowledge`) and **decisions** for the same use case.

#### Agent instructions overhaul

`agent_instructions.md` was reviewed end-to-end. Three behavioural sections were added in response to repeated drift patterns observed across production sessions:

- **§ 2.1a Ambiguity-killer protocol** — at ideation, the agent must scan the user's request against a table of ambiguity symptoms (vague verbs, undefined nouns, multiple plausible interpretations, implicit success criteria, implicit scope) and post Q&A items for every gap before advancing the ideation. "Just make a reasonable choice" is permission, not silence — it must be recorded explicitly.
- **§ 2.2a Investigação profunda obrigatória (refinement)** — refinement is research, not paraphrasing. The agent must exhaust all applicable sources (project files, source code, KE, Knowledge Graph, mockups, web docs, online discussions, runtime evidence, stakeholder context) and the refinement body must cite each finding with `path:line`, KE titles, KG node ids or URLs.
- **§ 2.8 Card-level artifact attachment (MANDATORY)** — every card must be self-contained. KE/mockup dependencies must be attached **directly to the card** via `copy_knowledge_to_card` / `copy_mockups_to_card` / `add_card_knowledge` / `add_screen_mockup(entity_type="card")`. Vague references to "see the spec" are a protocol violation.

Cleanup:
- Quick Navigation header *Multi-value Parameters — Two Accepted Formats* corrected to *Three Input Shapes* (the section was extended to native `list[str]` in 0.1.4).
- Obsolete `delete_task_validation` reference removed from the *Available Tools → Evaluations & Validations* table (the tool never shipped).
- `okto_pulse_create_sprint` parameter list aligned with the schema (`objective?` and `expected_outcome?` were missing).
- Duplicate "Startup Protocol" subsection deleted — Pre-Flight Checklist is the single source of truth.

#### Other improvements

- **MCP `ApiKeySessionMiddleware`** rewritten on top of `ContextVar` — required because the FastAPI process serves multiple concurrent requests and the previous module-level global would leak identities across requests. Token-based set/reset pattern protects against exception leaks.
- **Legacy debug shim:** `okto_pulse.core.mcp.server.run_mcp_server` is not part of the normal `okto_pulse.core.mcp` facade. Productive serving is owned by Community runtime composition; the shim emits a deprecation warning, lazy-loads the concrete server runtime, and fails closed when the edition has not configured the relational runtime/dependency. Removal criterion: no supported caller imports this shim from core after the AF41 ownership gates are enforced.

To upgrade an existing install: `pip install -U okto-pulse okto-pulse-core` and then `okto-pulse init --agents` to regenerate `.mcp.json` (the URL still points at port 8101 by default; override with `--mcp-port` if you remapped). No downstream contract changes for MCP clients — the wire protocol and tool catalog (sans 5 skills tools) are unchanged.

### 0.1.3 — previous stable (PyPI)

First hardening pass on the card lifecycle, the analytics contract, and the MCP instruction set.

- **`CardService.delete_card` cascades** through every spec-side JSON list (`test_scenarios[].linked_task_ids`, `business_rules[]`, `api_contracts[]`, `technical_requirements[]`, `decisions[]`) and through bug cards' `linked_test_task_ids`. The transactional cascade unblocks the delete→recreate flow that previously tripped `_validate_spec_linked_refs`.
- **Analytics card-type classifier** uses enum identity instead of `str(card.card_type).endswith(...)`. `total_cards_impl/test/bug`, `task_validation_gate.total_submitted`, `velocity[].test/bug`, and `bug_rate_per_spec` now report real counts.
- **`parse_multi_value` helper** consolidated the scattered `.split("|")` pattern; pipe-separated and JSON-array inputs are autodetected.
- **MCP agent instructions** rewritten (1830 → 2050 lines) with new sections for Multi-value Parameters, Destructive Operations, Versioning & Concurrent Edits, Security, Analytics-Driven Closure.

### 0.1.1 — initial PyPI release

26+1 SQLAlchemy models, 17+1 service classes, 11 API route modules, 119 MCP tools, embedded Kùzu Knowledge Graph with deterministic workers. (Spec Skills shipped here and was removed in 0.2.0.)

(Version 0.1.2 was published to TestPyPI only as a release candidate for 0.1.3.)

## SaaS Closure Audit

The executable ownership matrix is generated by `okto-pulse-saas-closure`. Every transitional budget must remain zero; the command fails closed on import, dependency, adapter, wheel, or documentation drift.

<!-- F16-SAAS-CLOSURE:BEGIN -->
| F16 executable surface | Owner | Observed | Terminal target |
| --- | --- | ---: | ---: |
| Core import rows | Core | 5213 | classified |
| Community-to-Core import rows | Community | 701 | classified |
| Direct dependency rows | Distribution owner | 22 | classified |
| `import_boundary_baseline` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `singleton_baseline` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `dependency_temporary_exceptions` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `graph_runtime_compatibility` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `rebuild_artifact_compatibility` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `community_private_reach_ins` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `community_adapter_bridges` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
| `af35_relational_residue` budget | `675c43ee-7d91-4cc3-8f87-44eeb293f90c` | 0 | 0 |
<!-- F16-SAAS-CLOSURE:END -->

## License

[Elastic License 2.0](./LICENSE) — free for personal and commercial use. Cannot be offered as a hosted/managed service.
