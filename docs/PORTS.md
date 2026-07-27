# Ports & Adapter Interfaces — okto-pulse-core

The complete catalogue of extension points an edition must fill.


Core defines **~100 `Protocol` contracts across 78 modules in `okto_pulse/core/ports/`** plus **30
Knowledge-Graph interfaces in `okto_pulse/core/kg/interfaces/`**. Every one of them is a seam an
edition must fill: core declares *what* it needs and *what the contract guarantees*; it never
constructs a database, opens a file, resolves an environment variable or speaks a wire protocol.

**How to read this catalogue.** Method counts are the public surface of each protocol. Anything not
listed here is internal to core and not an extension point. The reference implementations shipped in
`core/kg/providers/testing/` exist for tests only — they are **not** production adapters.

**Composition rule (AF-20/21/22).** Core must never import an edition. Editions register their
implementations at startup; unfilled slots **fail closed** (`R-P2-03A-D`) rather than falling back to
a silent default.

### Persistence & relational

| Port | Protocol (methods) | Contract |
|---|---|---|
| `application_persistence` | `ApplicationPersistencePort` (12) | Generic record CRUD + paginated reads for every SDLC entity. Owns the `bounded_page_offset()`/`PAGE_OFFSET_MAX` window contract. |
| `application_services` | `ApplicationServiceCatalog` (70) · `AnalyticsOperations` (15) · `KnowledgeGraphOperations` (54) · `BoardErasureLease` (1) | The aggregate service facade the use cases consume. The largest seam in the codebase. |
| `relational_application` | `RelationalApplicationAdapter` (3) · `PermissionPresetGateway` (6) · `AgentAuthenticationGateway` (4) | Relational access for permissions and agent auth, without core knowing SQL. |
| `relational_effects` | `RelationalEffectsPort` (7) | Side effects that must land in the relational store within a governed transaction. |
| `relational_runtime` | `RelationalRuntime` (2) | Engine/session lifecycle. |
| `relational_schema_migrator` | `RelationalSchemaMigrator` (3) | Schema migration execution. |
| `relational_schema_lifecycle` | `RelationalSchemaLifecycleOrchestrator` (1) | Ordering guarantee between migration, seed and worker startup. |
| `relational_services` | `ResourceGateRelationalAdapter` (10) · `ResourceGateAdapterFactory` | Resource-gate reads over the relational store. |
| `card_repository` · `structured_spec` · `spec_materialization` | (1 / 2 / 2) | Focused read/write seams for cards, structured spec entities and materialization. |
| `architecture_persistence` · `architecture_legacy` | (8 / 1) | Architecture designs and the legacy snapshot read path. |
| `amendment_revision` | `AmendmentRevisionStore` (3) | Path B amendment persistence. |
| `default_board_configuration` | `DefaultBoardConfigurationStore` (11) | Versioned board-config templates. |
| `design_system` | `DesignSystemStore` (12) | Global/inline design-system catalogue. |
| `effective_resource` · `spec_resource_propagation` | (2 / 8) | Effective-resource resolution and spec→card propagation. |
| `board_relational_cleanup` | `BoardRelationalCleanupPort` (1) | Relational side of governed board erasure. |

### Knowledge Graph — storage & runtime

| Interface (`core/kg/interfaces/`) | Methods | Contract |
|---|---|---|
| `SemanticGraphStore` | 24 | The graph itself: node/edge CRUD, similarity, layer-aware reads. |
| `GraphTransactionScope` · `GraphTransaction` | 16 / 1 | Transactional scope with lease revalidation. Owns the atomic tombstone swap. |
| `GlobalDiscoveryRuntime` | 19 | Cross-board discovery graph, including `flush_after_write_batch`. |
| `GraphLifecycle` · `GraphRuntimeStore` · `GraphSchemaManager` | 5 / 5 / 4 | Open/close/checkpoint, runtime state, edition-owned DDL and vector indexes. |
| `GraphRecovery` · `QuarantineRestore` · `GlobalDiscoveryRecovery` | 1 / 2 / 4 | WAL salvage, quarantine restore and discovery recovery (KGD-01). |
| `CypherExecutor` | 2 | Read-only Cypher execution behind the safety rails. |
| `AuditRepository` | 6 | Consolidation audit — the nothing-changed authority. |
| `SessionStore` | 6 | Transactional consolidation sessions. |
| `RebuildAuditArtifactStore` (+ resolver) | 15 / 1 | Deterministic rebuild artifacts. |
| `EmbeddingProvider` · `Reranker` · `LLMProvider` | 2 / 1 / 2 | Model-backed capabilities. Core never loads a model. |
| `ReflectiveRetrievalPort` · `ReflectiveCriticPort` · `ReflectiveTelemetryPort` | 1 each | Reflective query loop. |
| `HopPlanner` · `QueryRewriter` | 1 / 1 | Traversal planning and query rewriting. |
| `CacheBackend` · `RateLimiter` · `EventBus` | 4 / 2 / 4 | Cross-cutting runtime services. |
| `KGConfig` · `AuthContext` · `CognitivePendingWorkProvider` | 7 / 4 / 1 | Configuration, auth context and pending-work enumeration. |

### Knowledge Graph — governance & operations

| Port | Protocol (methods) | Contract |
|---|---|---|
| `kg_governance` | `KGGovernanceStore` (21) | Governance state: layers, maturity, revocation, takedown. |
| `consolidation` | `ConsolidationPersistencePort` (18) | Consolidation sessions, candidates and commits. |
| `kg_operational` | `KGOperationalReadModelPort` (9) · `KGGovernanceEffectsPort` (5) · `KGWorkerQueuePort` (5) · `KGWorkerAuditPort` (2) | Queue, DLQ, worker claim/audit and governance effects. |
| `kg_cognitive_source` | `CognitiveSourceStore` (4) | Durable cognitive source of truth with `generation` (MKG-A). |
| `kg_equivalence_ledger` | `EquivalenceLedger` (4) | Reversible dedup — a wrong merge can be undone (MKG-C). |
| `kg_curation_proposals` | `CurationProposalStore` (4) | Curation proposals under an autonomy policy. |
| `kg_subtype_registry` | `NodeSubtypeRegistry` (3) | Declarative `kind_of` subtypes (MKG-E). |
| `canonical_debt` | `CanonicalDebtStore` (7) | Failed/deferred canonical promotion with replay. |
| `kg_health` · `kg_events` | `KGHealthReadPort` (4) · `KGEventsReaderPort` (2) | Health projection and event reads. |
| `materialization_health` | `MaterializationEvidencePort` (2) | Materialization generation evidence. |
| `tombstone` · `stale_sweep` · `reconcile_intent` | 1 / 3 / 1 | Governed erasure, stale sweep and reconcile intent. |
| `bug_cognitive_context` | `BugCognitiveContextAssembler` (1) · `CanonicalBugNodeReadPort` (1) | Bug→KG context assembly. |
| `cognitive_effectiveness` | `CognitiveEffectivenessReadPort` (1) | Effectiveness metrics. |

### Delivery, coordination & workers

| Port | Protocol (methods) | Contract |
|---|---|---|
| `coordination` | `LeaseProvider` (3) · `WriteLockPort` (6) · `ClaimRepository` (3) · `RuntimeSettingsProvider` (1) · `ConfigValidationPort` (1) | Single-writer leases, write locks and work claims. The fence behind every graph mutation. |
| `runtime_workers` | `WorkerClockPort` · `BlockingExecutionPort` (2) · `QueueWorkPort` · `OutboxWorkPort` · `LeaseRecoveryPort` · `DeliverySignalPort` | Worker execution seams — core owns the policy, the edition owns the thread. |
| `global_outbox` | `GlobalOutboxStore` (8) | Cross-board delivery outbox. |
| `delivery_ledger` | `DeliveryLedgerPort` (5) | Delivery attempts, debt and circuit-breaker state. |
| `domain_event_delivery` | `DomainEventDeliveryStore` (7) · `DomainEventPublisher` (1) · `DomainEventFactReader` (4) | Domain-event persistence, publication and fact reads. |
| `runtime_events` | `RuntimeEventBusPort` (2) | In-process event bus. |
| `scheduler` | `SchedulerControl` (5) | Periodic work. No implicit singleton (`R-P2-06B`). |
| `runtime_control` | `RuntimeControl` (3) · `RuntimeCompositionLike` (1) | Composition handle and lifecycle control. |
| `runtime_settings` | `RuntimeSettingsPort` (3) · `ActorContextLike` | Effective settings with a persisted overlay. |

### MCP & inbound

| Port | Protocol (methods) | Contract |
|---|---|---|
| `mcp_host` | `McpHostProvider` (4) | ASGI host for the MCP listener. |
| `mcp_auth` | `McpAuthenticator` (1) · `AuthSession` | Credential authentication for MCP sessions. |
| `mcp_resources` | `McpResourceCatalog` (2) | Extends the `okto-pulse://` catalogue without core importing the edition. |
| `mcp_instructions` | `McpInstructionProvider` (2) | Agent instruction delivery. |
| `mcp_trace` | `McpTraceSink` (1) | Optional call tracing. Core writes no files. |
| `authentication` · `realm_access` · `permission_policy` | 1 / 1 / 2 | Identity, realm scoping and granular permission resolution. |
| `permission_preset_reconciliation` | `PermissionPresetReconciliationRepository` (2) | Preset drift reconciliation. |
| `capability_descriptor` | `CapabilityDescriptorSource` (1) | Edition capability advertisement. |

### Read models & reporting

| Port | Protocol (methods) | Contract |
|---|---|---|
| `analytics_read` | `AnalyticsReadPort` (2) | Analytics aggregates. |
| `traceability` | `TraceabilityReadPort` (2) | Traceability report. |
| `discovery_catalog` · `discovery_execution` · `discovery_selector` | 5 / 13 / 3 | Discovery catalogue, execution and selector read models. |
| `queue_health` · `takedown_telemetry` | 3 / 2 | Queue health and takedown timeline. |
| `critical_context` | `CriticalContextReadPort` (1) | Full-entity context for the critical-mutation guard. |
| `bug_regression_preview` | `BugRegressionPreviewReadPort` (3) | Bug regression preview. |
| `parent_artifact` · `skip_overrides` | 1 / 1 | Parent resolution and skip overrides. |

### Content, storage & telemetry

| Port | Protocol (methods) | Contract |
|---|---|---|
| `content_ingestion` | `ContentIngestionResolver` (2) | Secure remote-content resolution (SSRF-guarded). |
| `test_evidence` | `TestEvidenceWriteVerifier` (1) · `TestEvidenceExecutionIssuer` (1) | Test-evidence verification and re-executable issuance. |
| `knowledge_propagation` | `KnowledgePropagationPort` (5) · `KnowledgeMutationAuditSink` (1) | Selective knowledge propagation v2 with audited mutations. |
| `telemetry` | `TelemetrySink` · `TelemetryStateStore` · `TelemetryStateCarrier` · `TelemetryEffectConfigProvider` · `TelemetryEventStore` (9) · `PublishHealthSource` (3) · `ProductAggregationPort` · `TelemetryPort` (3) | Telemetry emission, local state, publish health and product aggregation. |
| `data_bootstrapper` | `DataBootstrapper` (3) | First-run seeding. |
| `package_version` · `f13` | `PackageVersionProvider` (1) · `EditionPort` (1) | Version and edition identity. |

---

[← Back to README](../README.md)

