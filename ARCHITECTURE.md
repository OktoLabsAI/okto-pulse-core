# Okto Pulse Core Architecture

This document names the backend ports exposed by `okto-pulse-core`, the
edition-owned adapters expected behind those ports, and the current migration
state of the backend hexagonal refactor.

The architectural target is explicit:

- `okto-pulse-core` owns domain models, application services, REST/MCP
  contracts, pure port definitions, compatibility shims and conformance gates.
- Edition packages own runtime composition and concrete technology choices.
  Today the production local edition is `okto-pulse` Community; a future SaaS
  edition should satisfy the same ports with different adapters.
- Community behavior must remain functionally identical while implementations
  move out of core.

Important distinction: a port existing in core does not guarantee that every
legacy call site has already been migrated to it. The executable source of
truth for adapter extraction is
`okto_pulse.core.application.boundary.adapter_readiness_inventory.build_adapter_inventory()`.

## Port Family Index

Core exposes ports at a few different levels. Treat the table below as the
navigation index for the hexagonal boundary: if a concrete runtime concern does
not fit one of these families, it should either become a new explicit port or
remain documented as migration debt.

| Port family | Core modules | What the core owns | What the edition owns today |
| --- | --- | --- | --- |
| Runtime composition | `core.composition`, `core.ports.runtime_*`, `core.ports.scheduler` | Provider keys, lifecycle contract and fail-closed validation for required runtime providers. | Community builds `RuntimeComposition`, lifecycle hooks, settings/session/auth/storage providers and scheduler bridge. |
| HTTP, storage and MCP support | `core.infra.auth`, `core.infra.storage`, `core.ports.mcp_auth`, `core.ports.mcp_resources`, `core.ports.capability_descriptor` | Transport-neutral contracts and catalog hygiene checks. | Community supplies local auth, filesystem storage, MCP authentication, operational resources and capability descriptors. |
| Initialization | `core.ports.relational_schema_migrator`, `core.ports.data_bootstrapper` | Declarative migration/bootstrap step contracts and validation semantics. | Community runs the SQLite schema migration/bootstrap plan used by `okto-pulse init` and startup. |
| Telemetry | `core.ports.telemetry` | Event, state, sink, product aggregation and publish-health contracts. | Community owns local JSONL persistence, HTTP beacon sender, product aggregation and publish-health descriptors. |
| Relational persistence | `core.repositories.interfaces` | Repository and Unit-of-Work Protocols for the first migrated aggregates. | SQLAlchemy adapters still live in core for now; broad extraction to Community is pending. |
| Knowledge Graph | `core.kg.interfaces`, `core.kg.interfaces.registry` | KG provider Protocols and the fail-closed provider registry. | Community supplies KG config, memory cache/rate/session adapters, embedding/rerank adapters, audit/event adapters and Ladybug/Kuzu graph adapters. |
| Inbound adapters | `core.inbound.*`, `core.api.*`, `core.mcp.*` | REST and MCP protocol surfaces, validation envelopes and tool/resource contracts. | Community mounts the core REST/MCP surfaces and provides the concrete runtime behind them. |
| Boundary/conformance | `core.application.boundary.*` | Executable migration ledgers, import/dependency audits and removal gates. | Community provides evidence adapters and smoke coverage used by those gates. |

## Boundary Rules

- Core must not import `okto_pulse.community`.
- Core port modules must stay pure: no SQLAlchemy, Ladybug/Kuzu,
  sentence-transformers, requests, filesystem persistence or edition imports at
  module import time.
- Concrete adapters are supplied by an edition composition root.
- Real runtime composition must fail closed when a required provider slot is
  missing. Tests may use sanctioned fake providers.
- Compatibility shims are allowed only to preserve old import paths while they
  delegate to a port or registry slot.
- Adapter readiness is executable. When an adapter moves, update the readiness
  ledger and its conformance tests together with the docs.

## Runtime Composition

The runtime is assembled through `okto_pulse.core.composition` and the edition
entry point.

| Contract | Module | Purpose |
| --- | --- | --- |
| `LifecycleHook` | `core.composition` | Async startup/shutdown hook implemented by runtime infrastructure. |
| `RuntimeComposition` | `core.composition` | Provider bag used by the composition root. |
| `PulseRuntime` | `core.composition` | Minimal runtime facade: `startup`, `shutdown`, `app_lifespan`, `require_provider`. |
| `CompositionRuntime` | `core.composition` | Default runtime executor for a `RuntimeComposition`. |

Community builds the local runtime in:

- `okto_pulse.community.main.create_community_app()`
- `okto_pulse.community.adapters.composition.configure_community_kg_registry()`

The local serve topology remains a single Python process with two uvicorn
listeners: API/UI and MCP. The shared runtime state is no longer an MCP auth
`ContextVar`; MCP credentials ride the ASGI/FastMCP request scope and the KG
runtime is resolved through `KGProviderRegistry`.

## Core Ports

### HTTP, Storage and Runtime Ports

| Port | Module | Contract | Current Community adapter |
| --- | --- | --- | --- |
| `AuthProvider` | `core.infra.auth` | Resolves current user, user id and realm from a FastAPI request. | `community.auth.LocalAuthProvider`. |
| `StorageProvider` | `core.infra.storage` | Saves, loads and deletes binary attachments. | `community.adapters.storage.CommunityFileSystemStorage`; core now keeps only the port and fail-closed registry. |
| `RuntimeSettingsPort` | `core.ports.runtime_settings` | Loads/persists runtime settings and applies runtime side effects. | Runtime settings still use core settings/services; scheduler effects are routed through Community composition. |
| `SchedulerControl` | `core.ports.scheduler` | Controls the `kg_daily_tick` scheduler job and shutdown. | `core.services.scheduler_control_adapter.SingletonSchedulerControl`, currently supplied by Community composition. |
| `RuntimeEventBusPort` | `core.ports.runtime_events` | Publishes runtime/observability events and flushes pending work. | Community runtime routes runtime telemetry through registered telemetry adapters. |
| `RuntimeControl` | `core.ports.runtime_control` | Starts/stops a runtime composition and returns providers. | Community composition root drives the concrete runtime. |
| `RelationalSchemaMigrator` | `core.ports.relational_schema_migrator` | Plans, validates and executes relational schema migration steps. | `community.adapters.relational_schema_migrator.CommunityRelationalSchemaMigrator`. |
| `DataBootstrapper` | `core.ports.data_bootstrapper` | Plans, validates and executes initial data/bootstrap steps. | `community.adapters.data_bootstrapper.CommunityDataBootstrapper`. |
| `McpAuthenticator` / `AuthSession` | `core.ports.mcp_auth` | Authenticates MCP credentials and returns an agent session. | `community.adapters.mcp_auth.CommunityMcpAuthenticator`. |
| `McpResourceCatalog` | `core.ports.mcp_resources` | Supplies common or operational MCP resources and validates catalog hygiene. | `community.adapters.resources.build_community_resource_catalog`. |
| `CapabilityDescriptorSource` | `core.ports.capability_descriptor` | Describes edition/provider capabilities without importing the edition. | `community.adapters.capability_descriptors.CommunityCapabilityDescriptorSource`. |

### Telemetry Ports

Telemetry contracts are in `okto_pulse.core.ports.telemetry`.

| Port | Contract | Current Community adapter |
| --- | --- | --- |
| `TelemetrySink` | Sends pending usage batches and publishes product snapshots. | `community.adapters.telemetry_sender.CommunityTelemetryBeaconSender`. |
| `TelemetryStateStore` | Loads/saves the narrow consent-state view. | No dedicated Community class; broader `state.json` persistence lives in `community.adapters.telemetry_state`, while core still owns consent/mode resolution helpers. |
| `TelemetryEventStore` | Appends local events, sent records and snapshots; exports/purges local telemetry. | `community.adapters.telemetry_store.CommunityLocalTelemetryStore`. |
| `PublishHealthSource` | Provides one publish-health signal source. | `community.adapters.publish_health_sources.*Source`. |
| `ProductAggregationPort` | Aggregates product metrics from local persisted data. | `community.adapters.product_telemetry.CommunityProductTelemetryAggregator`. |
| `TelemetryPort` | Facade for record, summary and publish-health operations. | `community.adapters.telemetry_port.build_community_telemetry_port`. |

Telemetry is one of the most complete extractions: the event store, sender,
product aggregation and watermark/failure-state persistence are Community-owned.
The remaining core-local telemetry surface is narrow: consent/mode settings
resolution in `core.telemetry.settings`, plus the `core.telemetry.store`
compatibility/ledger module that records the removed local store and is tracked
as a deferred readiness item rather than an active JSONL runtime adapter.

### Repository and Unit-of-Work Ports

Repository contracts live under `okto_pulse.core.repositories.interfaces`.

| Port | Contract | Current adapter |
| --- | --- | --- |
| `BoardRepository` | `get(board_id)` and `add(board)`. | `core.repositories.sqlalchemy.SQLAlchemyBoardRepository`. |
| `IdeationRepository` | `get(ideation_id)` and `add(ideation)`. | `core.repositories.sqlalchemy.SQLAlchemyIdeationRepository`. |
| `SpecRepository` | `get(spec_id)` and `add(spec)`. | `core.repositories.sqlalchemy.SQLAlchemySpecRepository`. |
| `RepositoryCatalog` | Groups aggregate repositories behind one object. | SQLAlchemy unit of work. |
| `PulseUnitOfWork` | Async context manager with commit/rollback/close. | `core.repositories.sqlalchemy.SQLAlchemyUnitOfWork`. |
| `UnitOfWorkFactory` | Creates unit-of-work instances for application use cases. | `core.repositories.sqlalchemy.SQLAlchemyUnitOfWorkFactory`. |

This is the first-cut relational strangler seam. The current coverage is narrow;
many services, routes and MCP handlers still accept `AsyncSession` directly.

### Knowledge Graph Provider Ports

The Knowledge Graph runtime is assembled by
`okto_pulse.core.kg.interfaces.registry.KGProviderRegistry`.

| Port or slot | Module | Contract | Current Community adapter |
| --- | --- | --- | --- |
| `KGConfig` | `kg.interfaces.kg_config` | Reads KG base path, embedding settings, session TTL and cleanup settings. | `community.adapters.data.CommunityKGConfig`. |
| `CacheBackend` | `kg.interfaces.cache_backend` | Query cache `get`, `put`, board invalidation and stats. | `community.adapters.memory.CommunityInMemoryCache`. |
| `RateLimiter` | `kg.interfaces.rate_limiter` | Per-agent rate limiting and reset. | `community.adapters.memory.CommunityInMemoryRateLimiter`. |
| `EmbeddingProvider` | `kg.interfaces.embedding` | Encodes one text or a batch into vectors. | `CommunitySentenceTransformerProvider` or `CommunityStubEmbeddingProvider`. |
| `SessionStore` | `kg.interfaces.session_store` | Creates, fetches, removes and sweeps consolidation sessions. | `community.adapters.memory.CommunityInMemorySessionStore`. |
| `AuditRepository` | `kg.interfaces.audit_repository` | Persists consolidation audit rows, node refs and outbox events. | `community.adapters.data.CommunityAuditRepository`. |
| `AuthContext` | `kg.interfaces.auth_context` | Supplies agent id, accessible boards and admin status for KG query authorization. | Community MCP auth bridge registered as `auth_context_factory`. |
| `EventBus` | `kg.interfaces.event_bus` | Publishes KG events and manages subscribers. | `community.adapters.data.CommunityOutboxEventBus`. |
| `SemanticGraphStore` | `kg.interfaces.graph_store` | Semantic graph queries and node/edge writes. | `community.adapters.kg.CommunityKuzuGraphStore`. |
| `CypherExecutor` | `kg.interfaces.cypher_executor` | Safe read-only Cypher execution for power queries. | `community.adapters.kg.CommunityKuzuCypherExecutor`. |
| `GraphTransaction` | `kg.interfaces.graph_transaction` | Begins scoped graph write transactions. | `community.adapters.kg.CommunityKuzuGraphTransaction`. |
| `GraphSchemaManager` | `kg.interfaces.graph_schema_manager` | Bootstrap, migrate, inspect and validate graph schema. | `community.adapters.kg.CommunityKuzuGraphSchemaManager`. |
| `GraphLifecycle` | `kg.interfaces.graph_lifecycle` | Open, close, rebuild and purge graph storage. | `community.adapters.kg.CommunityKuzuGraphLifecycle`. |
| `GraphPathResolver` | `kg.interfaces.graph_path_resolver` | Resolves board graph paths and storage state. | `community.adapters.kg.CommunityKuzuGraphPathResolver`. |
| `BoardGraphRuntime` | `kg.interfaces.board_graph_runtime` | Compatibility facade for the historical `core.kg.schema` API. | `community.adapters.board_graph_runtime.CommunityBoardGraphRuntime`. |
| `safe_write_step_adapter` | registry slot | Executes the Ladybug safe-write lifecycle step. | `community.adapters.kg_runtime.apply_ladybug_lifecycle_step`. |
| `HopPlanner` | `kg.interfaces.hop_planner` | Chooses traversal depth for graph expansion. | Core fixed/iterative/LLM planners; edition-specific planners may satisfy the protocol. |
| `LLMProvider` | `kg.interfaces.llm` | Runtime-agnostic LLM completion provider for optional cognitive helpers. | No Community provider is required for the deterministic local runtime. |
| `QueryRewriter` | `kg.interfaces.query_rewriter` | Rewrites natural queries before retrieval. | Core noop/rule-based implementations; edition-specific rewriters may satisfy the protocol. |
| `Reranker` | `kg.interfaces.reranker` | Re-ranks top-K search candidates. | `community.adapters.rerank.CommunityCrossEncoderReranker`; core token-overlap remains the deterministic fallback. |

The board-level Ladybug/Kuzu runtime has moved to Community. Core keeps
`core.kg.schema` as an import-compatible facade that delegates to
`KGProviderRegistry.board_graph_runtime`. Global discovery still has a core
module-level `_global_db` handle and is tracked as a ledgered exception.

### Inbound Adapter Surface

Core exposes two inbound adapters for the same application/service layer:

| Adapter | Module | Role |
| --- | --- | --- |
| REST | `core.inbound.rest_adapter` and `core.api.*` | HTTP/JSON endpoints. |
| MCP | `core.inbound.mcp_adapter` and `core.mcp.*` | Agent tool/resource surface. |

The inbound layer translates transport-specific errors into canonical envelopes
before reaching domain services. MCP auth now uses request-scoped credentials
instead of an Okto-owned auth `ContextVar`.

## Canonical Adapter Readiness Ledger

The executable ledger currently catalogues 21 adapter or dependency seams:
8 `ready`, 10 `blocked`, and 3 `deferred`.

| Adapter key | Port reference | Status | Target / current owner |
| --- | --- | --- | --- |
| `filesystem_storage_provider` | `StorageProvider` | `blocked` | Community storage provider is wired; ledger still blocks removal until all evidence is complete. |
| `sentence_transformer_embedding_provider` | `EmbeddingProvider` | `ready` | Moved to `community.adapters.embedding` by R-P2-07. |
| `stub_embedding_provider` | `EmbeddingProvider` | `deferred` | Core last-resort deterministic fallback. |
| `cross_encoder_reranker` | `Reranker` | `ready` | Moved to `community.adapters.rerank` by R-P2-07. |
| `inmemory_cache_backend` | `CacheBackend` | `blocked` | Community registers a real runtime cache; core keeps test/fallback implementation. |
| `inmemory_token_bucket_rate_limiter` | `RateLimiter` | `blocked` | Community registers a real runtime rate limiter; core keeps test/fallback implementation. |
| `inmemory_session_store` | `SessionStore` | `blocked` | Community registers a real runtime session store; core keeps test/fallback implementation. |
| `mcp_auth_context` | `AuthContext / McpAuthenticator` | `blocked` | MCP credential carrier moved to request scope; embedded `MCPAuthContext` bridge still remains. |
| `kuzu_graph_store` | `SemanticGraphStore` | `ready` | Moved to `community.adapters.kuzu_graph_store` by R-P2-05. |
| `kuzu_cypher_executor` | `CypherExecutor` | `ready` | Moved to `community.adapters.kuzu_cypher_executor` by R-P2-05. |
| `kuzu_graph_schema_manager` | `GraphSchemaManager` | `ready` | Moved to `community.adapters.kuzu_graph_schema_manager` by R-P2-05. |
| `kuzu_graph_lifecycle` | `GraphLifecycle` | `ready` | Moved to `community.adapters.kuzu_graph_lifecycle` by R-P2-05. |
| `kuzu_graph_path_resolver` | `GraphPathResolver` | `ready` | Moved to `community.adapters.kuzu_graph_path_resolver` by R-P2-05. |
| `kuzu_graph_transaction` | `GraphTransaction` | `ready` | Moved to `community.adapters.kuzu_graph_transaction` by R-P2-05. |
| `global_discovery_db` | `kg_registry` global discovery handle | `blocked` | Core still owns the module-level global discovery DB handle. |
| `settings_kg_config` | `KGConfig` | `blocked` | Community supplies `CommunityKGConfig`; core settings-backed default remains tracked. |
| `singleton_scheduler_control` | `SchedulerControl` | `blocked` | Community composition supplies the singleton bridge; implementation still lives in core. |
| `local_telemetry_store` | `#10 telemetry` | `deferred` | Event store moved to Community; ledger item remains governed by telemetry boundary. |
| `asyncpg_postgres_driver` | relational driver dependency | `deferred` | `asyncpg` is removed from core defaults; the broader SQLAlchemy/PostgreSQL seam remains gated by R01B/R01C. |
| `board_source_store` | no port yet | `blocked` | Raw SQLite rebuild source reader still in core. |
| `board_rebuild_ingestion_adapter` | no port yet | `blocked` | Raw SQLite rebuild ingestion still in core. |

Relational dependency status is tracked on two axes. The `asyncpg` package is a
removed dependency: it must not appear in core dependencies, lock metadata, wheel
metadata or runtime imports. The residual SQLAlchemy/PostgreSQL branch in
`core.infra.database` is different: it is a deferred relational seam owned by
the R01B/R01C migration path, so documentation and gates must keep it visible
instead of treating it as already removed or as permission to reintroduce
`asyncpg`.

Removal must pass the bounded evidence fields in the inventory:
`port_closed`, `community_registered`, `oracle_passed`, `import_audit_passed`,
`dependency_audit_passed` and `register_before_remove_passed`.

## Narrow Internal Ports

Several services define local `Protocol` seams for tests, metrics sinks or
strategy injection. They are not edition-level adapter contracts yet:

- `UseCase` in `core.application.use_cases.base`.
- `CandidateLogSource` in `core.services.cognitive_effectiveness_service`.
- `FullContextResolver` in `core.services.critical_context_guard`.
- `SelectorAccessPolicy` and `SelectorMetricsSinkProtocol` in
  `core.services.discovery_selector_catalog`.
- `ResourceLineageProvider` in `core.services.resource_lineage`.
- `StructuredSpecEntityMetricsSink` and `StructuredSpecEntityAckStore` in
  `core.services.spec_structured_entities`.
- `CognitiveCandidatePersister` in `core.kg.cognitive_closeout_production`.
- `CognitiveItemStoreProtocol` in `core.kg.cognitive_closeout_gate`.
- `MetricSinkProtocol` in `core.kg.connectivity_guard`.
- `OrphanMetricSinkProtocol` and `OrphanAuditSinkProtocol` in
  `core.kg.orphan_integrity`.
- `LearningSummariser` in `core.kg.agent.extractors.learnings`.
- `HeuristicLLM` in `core.kg.agent.heuristics.llm_protocol`.
- `VectorSeedProvider` and `GraphExpander` in `core.kg.hybrid_search.hybrid`.
- `ReindexAdapter` in `core.kg.global_discovery_reindex`.
- `StorageStepAdapter` in `core.kg.safe_write_lifecycle`.
- `RebuildStepAdapter`, `KGRebuiltEventEmitter` and `OrphanScanProvider` in
  `core.kg.rebuild_service`.
- `KGRebuiltPublishAdapter`, `CognitivePendingAdapter` and `SourceSetResolver`
  in `core.kg.rebuild_audit`.
- `SourceMaterialiser` in `core.kg.rebuild_deterministic`.
- `SourceStore` in `core.kg.rebuild_sources`.
- `SourceProbe`, `HealthProbe` and `RebuildStatusProbe` in
  `core.kg.rebuild_preflight`.
- `HyDELLMFn`, `FusionLLMFn` and `DecomposeLLMFn` in
  `core.kg.query_rewrite`.
- `CriticFn`, `RetrievalFn` and `AuditSink` in
  `core.kg.retrieve_critic.orchestrator`.
- `LLMHopFn` in `core.kg.adaptive_hops.llm`.
- `LLMRankerFn` in `core.kg.rerank.llm`.
- `ExtractorFn` and `GrounderFn` in `core.kg.grounding.grounding`.
- `CanonicalBugProbe` in `core.kg.cognitive_source_ref_resolver`.
- `CommandRunner` in `core.application.boundary.community_smoke`.
- `CoroFactory` and `RunBlocking` in `core.kg.startup_schema_sweep`.
- `ChaosExecutor` in `core.kg.stress_runner`.

Promote one of these seams into `core.ports` or `core.kg.interfaces` before an
edition starts depending on it as an adapter contract.

## Current Ledgered Exceptions

The backend is not fully agnostic yet. These concrete concerns still appear in
core and should remain visible during the next refactor cycle:

| Area | Current core dependency | Why it remains |
| --- | --- | --- |
| Relational database | SQLAlchemy models, `AsyncSession` service APIs, `core.infra.database` migrations and SQLAlchemy repositories/UoW. | The repository/UoW strangler is narrow. Broad service and route migration is still pending. Any PostgreSQL dialect branch here is R01B/R01C debt, not a license to restore `asyncpg` to the core default. |
| Raw SQLite rebuild reads/writes | `core.kg.board_source_store` and `core.kg.board_rebuild_adapter` use `sqlite3.connect`. | No relational source-reader/rebuild-ingestion port exists yet. |
| Global discovery DB handle | `core.kg.global_discovery.schema._global_db` owns the global `discovery.lbug` handle. | Board graph moved to Community, but global discovery DB lifecycle still needs a composition-owned slot. |
| KG test/default providers | `core.kg.providers.embedded.memory_*` and `providers.testing.*`. | Needed for sanctioned test defaults and deterministic fakes; not production runtime ownership. |
| MCP AuthContext bridge | `core.kg.providers.embedded.mcp_auth_context`. | Credential carrier moved to request scope, but the concrete AuthContext bridge has not moved to Community. |
| Scheduler control implementation | `core.services.scheduler_control_adapter.SingletonSchedulerControl`. | Community composition supplies it, but the concrete singleton bridge still lives in core. |
| Telemetry consent/mode state and store ledger | `core.telemetry.settings` still persists the narrow consent/mode view in `state.json`; `core.telemetry.store` remains as a compatibility/ledger module for the removed local store. | Broader event, sender, product and watermark/failure-state persistence moved to Community; consent/mode settings and the deferred store ledger remain core-local. |
| Boundary gates | `core.application.boundary.*` references readiness ledgers and Community smoke-test concepts. | These are conformance/refactor controls, not product runtime adapters. |

## Adding a New Adapter

1. Define or reuse a Protocol in `core.ports` or `core.kg.interfaces`.
2. Keep DTOs serializable and independent from concrete runtimes.
3. Do not import a concrete backend from core services.
4. Implement the adapter in the edition package.
5. Register it in the edition composition root before core consumers run.
6. Add a conformance or replay test proving the adapter satisfies the Protocol.
7. Update `adapter_readiness_inventory` and this document.
8. Document the adapter in the edition README.

## Related Files

- `src/okto_pulse/core/ports/`
- `src/okto_pulse/core/kg/interfaces/`
- `src/okto_pulse/core/repositories/interfaces/`
- `src/okto_pulse/core/composition.py`
- `src/okto_pulse/core/application/boundary/adapter_readiness_inventory.py`
- `src/okto_pulse/core/application/boundary/`
- `../okto_labs_pulse_community/src/okto_pulse/community/adapters/`
