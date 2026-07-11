"""AntiSingletonGate — block NEW module-global singletons in core (spec #15).

fr_95d98ef5: no new module-global singleton may be introduced in the core; a new
one detected blocks. fr_531b74f3: the existing singletons live in a
register-before-remove ledger with owner, target provider, expected adapter and
a retirement criterion — headlined by ``_mcp_session_factory`` and
``_permission_cache``.

Detection is deterministic and NARROW (AST, no import): a module-global is a
singleton when it is reassigned via a ``global`` statement (mutated process
state), is a ``ContextVar``, or is provider-bridge cache/lock state in a
``llm_provider_bridges.py`` module. Module constants — ``__all__``, lookup
tables, metric sample buffers — are NOT singletons and are never flagged.

The current inventory of such singletons is frozen in ``BASELINE_SINGLETONS``
(register-before-remove): introducing a NEW one fails the gate until it is
either injected through a composition provider/port or consciously added to the
baseline with justification.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

#: register-before-remove ledger of the HEADLINE core singletons (fr_531b74f3).
SINGLETON_LEDGER: dict[str, dict[str, str]] = {
    "_mcp_session_factory": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_session_factory",
        "expected_adapter": "RuntimeComposition.mcp_session_factory",
        "retirement_criterion": (
            "Composition root provides the MCP session factory; remove the global "
            "after the inbound MCP server resolves it from composition."
        ),
    },
    "_mcp_authenticator": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_authenticator",
        "expected_adapter": (
            "Edition-owned McpAuthenticator port registered by the composition "
            "root while auth_bootstrap migrates away from direct DB lookup."
        ),
        "retirement_criterion": (
            "Remove the module global once the inbound MCP server resolves "
            "McpAuthenticator from RuntimeComposition instead of a process-wide "
            "register-before-remove bridge."
        ),
    },
    "_permission_cache": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "auth",
        "expected_adapter": "Auth provider-owned permission cache",
        "retirement_criterion": (
            "Auth provider owns permission caching; remove the module dict once the "
            "provider exposes a scoped cache."
        ),
    },
    "_factory": {
        "file": "okto_pulse/core/telemetry/event_store_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetryEventStore factory (R10-B) — the composition root (Community) "
            "registers the concrete adapter behind the port; this is the same "
            "register-before-remove pattern as kg/interfaces/registry.py::_registry."
        ),
        "retirement_criterion": (
            "Remove the module global once the telemetry runtime resolves its "
            "EventStore from RuntimeComposition.telemetry instead of the process-wide "
            "factory registry (and the core LocalTelemetryStore shim is deleted)."
        ),
    },
    "_product_aggregator_factory": {
        "file": "okto_pulse/core/telemetry/product_aggregator_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "ProductAggregationPort factory (R10-D) — the composition root "
            "(Community) registers the concrete sqlite3 aggregator behind the port; "
            "same register-before-remove pattern as the R10-B event-store factory."
        ),
        "retirement_criterion": (
            "Remove the module global (and the core ProductTelemetryAggregator shim) "
            "once every edition composes its ProductAggregationPort in R10-E."
        ),
    },
    "_publish_health_source_provider": {
        "file": "okto_pulse/core/telemetry/publish_health_source_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "PublishHealthSource external-descriptor provider (R10-D) — the "
            "composition root (Community) registers the aws_ingest/report_athena "
            "descriptors (default GAP, never healthy) behind the port."
        ),
        "retirement_criterion": (
            "Remove the module global once the publish-health sources are composed "
            "via RuntimeComposition in R10-E."
        ),
    },
    "_telemetry_sender_factory": {
        "file": "okto_pulse/core/telemetry/sender_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetrySink factory (R10-C) — the composition root (Community) "
            "registers the concrete beacon sender (requests/HMAC/handshake/usage) "
            "behind the port; same register-before-remove pattern as the R10-B "
            "event-store factory."
        ),
        "retirement_criterion": (
            "Remove the module global (and the core TelemetryBeaconSender shim) "
            "once every edition composes its TelemetrySink in R10-E."
        ),
    },
    "_telemetry_port_factory": {
        "file": "okto_pulse/core/telemetry/telemetry_port_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "TelemetryPort facade factory (R10-E, Stage A) — the composition root "
            "(Community) registers the composed TelemetryService facade behind the "
            "port; same register-before-remove pattern as the R10-B/C/D factories."
        ),
        "retirement_criterion": (
            "Remove the module global once every edition composes its TelemetryPort "
            "via RuntimeComposition and the call-sites stop constructing the facade "
            "directly (R10-E Stage D / IMP03)."
        ),
    },
    "_provider": {
        "file": "okto_pulse/core/telemetry/effect_config_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry_effect_config",
        "expected_adapter": (
            "TelemetryEffectConfig provider (AF31-S3) - Community registers "
            "local metrics dir and beacon defaults behind the port while core "
            "keeps telemetry configuration neutral."
        ),
        "retirement_criterion": (
            "Remove the module global once telemetry effect config is resolved "
            "from RuntimeComposition instead of the process-wide registry."
        ),
    },
    "_carrier": {
        "file": "okto_pulse/core/telemetry/telemetry_state_registry.py",
        "owner": "okto-pulse-core/telemetry",
        "target_provider": "telemetry",
        "expected_adapter": (
            "Full-dict TelemetryStateCarrier (R12 FR1) - a composition root "
            "(Community) registers CommunityTelemetryStateCarrier behind the port; "
            "core load/save_telemetry_state fail closed until registration, matching "
            "the R10-B/C/D/E register-before-remove factory pattern."
        ),
        "retirement_criterion": (
            "Remove the module global once every edition composes its "
            "TelemetryStateCarrier via RuntimeComposition and core.telemetry.settings "
            "no longer resolves state through the process-wide bridge."
        ),
    },
    "_effective_resource_catalog": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Effective MCP resource catalog (R11-A) — the composition root injects "
            "edition catalogs behind McpResourceCatalog before the catalog is frozen."
        ),
        "retirement_criterion": (
            "Remove the module global once the inbound MCP server resolves the "
            "effective resource catalog from RuntimeComposition instead of the "
            "process-wide register-before-freeze catalog bridge."
        ),
    },
    "_instruction_providers": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_instruction_provider",
        "expected_adapter": (
            "Effective MCP instruction providers (AF31-S2) - Community injects "
            "edition-owned instructions before MCP startup freezes the registry."
        ),
        "retirement_criterion": (
            "Remove the module global once MCP instructions are resolved from "
            "RuntimeComposition instead of the process-wide provider bridge."
        ),
    },
    "_instruction_providers_frozen": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_instruction_provider",
        "expected_adapter": (
            "Instruction-provider freeze guard (AF31-S2) - late instruction "
            "provider registration fails closed after edition composition."
        ),
        "retirement_criterion": (
            "Remove the module global when MCP instruction provider lifecycle "
            "and freeze state move behind RuntimeComposition."
        ),
    },
    "_resource_catalog_frozen": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Catalog freeze guard (R11-A) — late resource-catalog injection fails "
            "closed after the composition root finishes registering providers."
        ),
        "retirement_criterion": (
            "Remove the module global when resource catalog lifecycle/freeze state is "
            "owned by RuntimeComposition or a scoped inbound MCP lifecycle provider."
        ),
    },
    "_RESOURCE_REGISTRY": {
        "file": "okto_pulse/core/mcp/server.py",
        "owner": "okto-pulse-core/inbound-mcp",
        "target_provider": "mcp_resource_catalog",
        "expected_adapter": (
            "Read-only legacy projection (R11-A) derived from the effective MCP "
            "resource catalog for compatibility with existing resource consumers."
        ),
        "retirement_criterion": (
            "Remove the projection when all MCP resource consumers read from "
            "effective_resource_catalog() or RuntimeComposition directly."
        ),
    },
    "_worker": {
        "file": "okto_pulse/core/kg/workers/cognitive_closeout.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "cognitive_closeout_worker",
        "expected_adapter": (
            "CognitiveCloseoutWorker runtime handle — started/stopped by app "
            "lifespan while the cognitive closeout worker is being moved behind "
            "composition-owned lifecycle control."
        ),
        "retirement_criterion": (
            "Remove the module global when the cognitive closeout worker is owned "
            "by RuntimeComposition/lifespan provider and resolved through the KG "
            "worker lifecycle port."
        ),
    },
    "_unit_of_work_factory": {
        "file": "okto_pulse/core/runtime_registry.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "uow_factory",
        "expected_adapter": (
            "Edition relational UnitOfWorkFactory (R01B FR3) — the composition root "
            "(Community) registers its concrete factory behind the port; the core "
            "REST (api/deps.get_unit_of_work) and MCP "
            "(mcp/server.get_unit_of_work_factory_for_mcp) inbounds resolve it here "
            "instead of constructing SQLAlchemyUnitOfWorkFactory. Same "
            "register-before-remove pattern as the telemetry registries."
        ),
        "retirement_criterion": (
            "Remove the module global once the core relational concretes "
            "(SQLAlchemyUnitOfWork/Factory) are physically removed (R01C) and every "
            "edition composes its UnitOfWorkFactory via RuntimeComposition."
        ),
    },
    "_relational_runtime_factory": {
        "file": "okto_pulse/core/runtime_registry.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "relational_runtime_factory",
        "expected_adapter": (
            "Edition relational runtime factory (AF30-3a) — transitional callers "
            "that still enter through core.infra.database.create_database must use "
            "an explicitly registered adapter factory. Productive Community startup "
            "builds and injects the runtime directly from its SQLAlchemy adapter."
        ),
        "retirement_criterion": (
            "Remove the compatibility factory once all legacy/tooling callers use "
            "edition-owned configure_database_runtime injection directly."
        ),
    },
    "_content_ingestion_resolver": {
        "file": "okto_pulse/core/runtime_registry.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "content_ingestion_resolver",
        "expected_adapter": (
            "Edition MCP content-ingestion resolver (AF12) — core tools accept "
            "inline payloads or an abstract content_reference, while Community "
            "owns local file/URL resolution, root confinement, size limits, and "
            "SSRF policy behind this optional seam."
        ),
        "retirement_criterion": (
            "Remove the module global when content ingestion is supplied through "
            "RuntimeComposition for every MCP runtime entrypoint."
        ),
    },
    "_lease_provider": {
        "file": "okto_pulse/core/ports/coordination.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "coordination_lease_provider",
        "expected_adapter": (
            "Edition coordination lease provider (AF15) — the composition root "
            "registers local-first or managed lease ownership behind the port so "
            "core daily-tick and leadership paths do not construct process-local "
            "coordination primitives directly. Same register-before-remove "
            "pattern as runtime_registry seams."
        ),
        "retirement_criterion": (
            "Remove the module global once coordination providers are supplied "
            "through RuntimeComposition or another explicit edition-scoped "
            "composition object for every runtime entrypoint."
        ),
    },
    "_write_lock_port": {
        "file": "okto_pulse/core/ports/coordination.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "coordination_write_lock_port",
        "expected_adapter": (
            "Edition write-lock provider (AF15) — advisory lock and consolidation "
            "flows resolve write locks through this port while Community owns the "
            "local-first locking adapter and future editions can supply managed "
            "implementations outside core."
        ),
        "retirement_criterion": (
            "Remove the module global once write-lock composition is owned by "
            "RuntimeComposition or an equivalent edition-scoped provider registry."
        ),
    },
    "_claim_repository": {
        "file": "okto_pulse/core/ports/coordination.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "coordination_claim_repository",
        "expected_adapter": (
            "Edition claim repository (AF15) — dispatcher and outbox workers use "
            "the port for claim ownership while concrete local-first persistence "
            "or managed claim semantics remain outside common core."
        ),
        "retirement_criterion": (
            "Remove the module global once claim repository composition is owned "
            "by RuntimeComposition or another explicit edition-scoped provider "
            "object."
        ),
    },
    "_runtime_settings_provider": {
        "file": "okto_pulse/core/ports/coordination.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "coordination_runtime_settings_provider",
        "expected_adapter": (
            "Edition runtime-settings provider (AF15) — settings reads resolve "
            "through the port so Community can preserve local settings behavior "
            "and future SaaS editions can provide distributed/runtime settings "
            "without core singletons."
        ),
        "retirement_criterion": (
            "Remove the module global once runtime-settings provider composition "
            "is owned by RuntimeComposition or an equivalent edition-scoped "
            "configuration provider."
        ),
    },
    "_config_validation_port": {
        "file": "okto_pulse/core/ports/coordination.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "coordination_config_validation_port",
        "expected_adapter": (
            "Edition config-validation port (AF15) — settings writes can be "
            "validated through an edition-owned boundary while core keeps the "
            "domain validation rules and avoids concrete runtime adapters."
        ),
        "retirement_criterion": (
            "Remove the module global once config-validation composition is owned "
            "by RuntimeComposition or an equivalent edition-scoped configuration "
            "provider."
        ),
    },
    "_relational_effects_port": {
        "file": "okto_pulse/core/ports/relational_effects.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "relational_effects_port",
        "expected_adapter": (
            "Edition relational side-effects adapter (AF30-3cR) — core runtime "
            "handlers request queue/tick persistence through this port while "
            "Community owns SQLAlchemy dialect conflict handling and model access."
        ),
        "retirement_criterion": (
            "Remove the module global once relational side-effect providers are "
            "supplied through RuntimeComposition or another edition-scoped "
            "composition object for every runtime entrypoint."
        ),
    },
    "_kg_operational_read_model_port": {
        "file": "okto_pulse/core/ports/kg_operational.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_operational_read_model_port",
        "expected_adapter": (
            "Edition KG operational read-model adapter (AF35-S2) — core readers "
            "depend on the narrow port while Community owns SQLAlchemy queries "
            "and concrete local-first operational projections."
        ),
        "retirement_criterion": (
            "Remove the module global once KG operational read models are supplied "
            "through RuntimeComposition or another edition-scoped composition object "
            "for every runtime entrypoint."
        ),
    },
    "_kg_governance_effects_port": {
        "file": "okto_pulse/core/ports/kg_operational.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_governance_effects_port",
        "expected_adapter": (
            "Edition KG governance effects adapter (AF35-S2) — core governance "
            "commands depend on this port while Community owns concrete relational "
            "state transitions."
        ),
        "retirement_criterion": (
            "Remove the module global once KG governance effects are supplied through "
            "RuntimeComposition or another edition-scoped composition object."
        ),
    },
    "_kg_worker_queue_port": {
        "file": "okto_pulse/core/ports/kg_operational.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_worker_queue_port",
        "expected_adapter": (
            "Edition KG worker queue adapter (AF35-S2) — workers route and retry "
            "queue entries through the port while Community owns concrete "
            "persistence and dialect behavior."
        ),
        "retirement_criterion": (
            "Remove the module global once KG worker queue composition is owned by "
            "RuntimeComposition or another edition-scoped provider registry."
        ),
    },
    "_kg_worker_audit_port": {
        "file": "okto_pulse/core/ports/kg_operational.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_worker_audit_port",
        "expected_adapter": (
            "Edition KG worker audit adapter (AF35-S2) — workers emit outbox and "
            "audit records through the port while Community owns SQLAlchemy-backed "
            "writes."
        ),
        "retirement_criterion": (
            "Remove the module global once KG worker audit composition is owned by "
            "RuntimeComposition or another edition-scoped provider registry."
        ),
    },
    "_orchestrator": {
        "file": "okto_pulse/core/infra/schema_lifecycle.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "relational_schema_lifecycle_orchestrator",
        "expected_adapter": (
            "Edition relational schema-lifecycle orchestrator (R01C FR3) — the "
            "composition root (Community) registers its concrete init_db / migration "
            "/ bootstrap orchestrator behind the seam so core init_db delegates "
            "schema creation to the edition. When unregistered the core stays "
            "FAIL-OPEN and runs its in-tree schema bootstrap; the seam is DORMANT in "
            "IMP1 (never registered in production main.py / cli.py). Same "
            "register-before-remove pattern as the R01B uow_factory / "
            "sqlite_pragma_installer seams."
        ),
        "retirement_criterion": (
            "Remove the module global once init_db / migrations / bootstrap "
            "ownership physically moves to Community (R01C FR3 activation, IMP2+) and "
            "the core no longer hosts the relational schema bootstrap."
        ),
    },
    "_bridge_cache_registry": {
        "file": "okto_pulse/core/kg/llm_provider_bridge_cache.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "llm_provider_bridge_cache",
        "expected_adapter": (
            "Core-owned bounded LLMProvider bridge cache. Provider bridges use "
            "namespaced entries instead of per-module _bridge_cache/_bridge_lock "
            "singletons while preserving stable callable identity for downstream "
            "id(llm_fn) caches."
        ),
        "retirement_criterion": (
            "Remove the module global once provider bridge cache lifecycle is "
            "owned by RuntimeComposition or a scoped KG provider cache service."
        ),
    },
    "_version_provider": {
        "file": "okto_pulse/core/infra/config.py",
        "owner": "okto-pulse-core/settings",
        "target_provider": "package_version_provider",
        "expected_adapter": (
            "PackageVersionProvider (AF31-S2) - runtime/package metadata owns "
            "version resolution so core settings never read source checkout files."
        ),
        "retirement_criterion": (
            "Remove the module global once version resolution is supplied by "
            "edition/runtime composition or immutable installed metadata only."
        ),
    },
    "_IMPL_CLS": {
        "file": "okto_pulse/core/services/resource_gate.py",
        "owner": "okto-pulse-core/resource-gate",
        "target_provider": "resource_gate_service_facade",
        "expected_adapter": (
            "Lazy ResourceGateService facade over the adapter-owned implementation "
            "while preserving the historical core.services.resource_gate import path."
        ),
        "retirement_criterion": (
            "Remove the module global once ResourceGateService is provided through "
            "composition or all call sites import the adapter-owned implementation "
            "through an explicit port/factory."
        ),
    },
}

#: Equivalent per-occurrence runtime ledger for singleton names that are reused
#: across modules (for example ``_singleton``). The historical
#: ``SINGLETON_LEDGER`` stays keyed by name for public compatibility; this keyed
#: ledger removes ambiguity when enforcing BASELINE_SINGLETONS coverage.
RUNTIME_SINGLETON_BASELINE_LEDGER: dict[str, dict[str, str]] = {
    "okto_pulse/core/ports/mcp_host.py::_mcp_host_provider": {
        "file": "okto_pulse/core/ports/mcp_host.py",
        "owner": "okto-pulse-core/mcp-host-boundary",
        "target_provider": "mcp_host_provider",
        "retirement_criterion": (
            "Remove the process registry when inbound MCP host composition is "
            "carried by RuntimeComposition rather than a global bridge."
        ),
    },
    "okto_pulse/core/ports/kg_events.py::_kg_events_reader_port": {
        "file": "okto_pulse/core/ports/kg_events.py",
        "owner": "okto-pulse-core/kg-events-boundary",
        "target_provider": "kg_events_reader_port",
        "retirement_criterion": (
            "Remove the process registry when request/application composition "
            "passes the edition-owned KG events reader directly."
        ),
    },
    "okto_pulse/core/ports/relational_application.py::_adapter": {
        "file": "okto_pulse/core/ports/relational_application.py",
        "owner": "okto-pulse-core/relational-application-boundary",
        "target_provider": "relational_application_adapter",
        "retirement_criterion": (
            "Remove the process registry when every inbound/application entry "
            "receives the edition-owned relational adapter bundle directly from "
            "RuntimeComposition or its scoped UnitOfWork."
        ),
    },
    "okto_pulse/core/application/kg_events_hub.py::_hub": {
        "file": "okto_pulse/core/application/kg_events_hub.py",
        "owner": "okto-pulse-core/inbound-events",
        "target_provider": "kg_events_hub",
        "retirement_criterion": "Move the process-wide KG event hub behind an inbound event-bus provider.",
    },
    "okto_pulse/core/events/dispatcher.py::_dispatcher": {
        "file": "okto_pulse/core/events/dispatcher.py",
        "owner": "okto-pulse-core/events",
        "target_provider": "domain_event_dispatcher",
        "retirement_criterion": "Compose the dispatcher through RuntimeComposition or an edition-scoped event bus.",
    },
    "okto_pulse/core/infra/auth.py::_auth_provider": {
        "file": "okto_pulse/core/infra/auth.py",
        "owner": "okto-pulse-core/auth-boundary",
        "target_provider": "auth_provider",
        "retirement_criterion": "Inject auth providers through composition instead of a process-global setter.",
    },
    "okto_pulse/core/infra/config.py::_settings_instance": {
        "file": "okto_pulse/core/infra/config.py",
        "owner": "okto-pulse-core/settings",
        "target_provider": "settings_provider",
        "retirement_criterion": "Resolve settings from an edition-scoped provider rather than a module singleton.",
    },
    "okto_pulse/core/infra/database.py::_engine": {
        "file": "okto_pulse/core/infra/database.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "relational_engine",
        "retirement_criterion": "Remove after relational engine ownership is fully composed by each edition.",
    },
    "okto_pulse/core/infra/database.py::_session_factory": {
        "file": "okto_pulse/core/infra/database.py",
        "owner": "okto-pulse-core/runtime",
        "target_provider": "relational_session_factory",
        "retirement_criterion": "Remove after UnitOfWork/session factory composition replaces direct core storage.",
    },
    "okto_pulse/core/infra/storage.py::_storage_provider": {
        "file": "okto_pulse/core/infra/storage.py",
        "owner": "okto-pulse-core/storage",
        "target_provider": "storage_provider",
        "retirement_criterion": "Resolve storage providers through composition for every edition.",
    },
    "okto_pulse/core/kg/backpressure.py::_default_gate": {
        "file": "okto_pulse/core/kg/backpressure.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_backpressure_gate",
        "retirement_criterion": "Move the default KG backpressure gate behind KG runtime composition.",
    },
    "okto_pulse/core/kg/global_discovery/outbox_worker.py::_singleton": {
        "file": "okto_pulse/core/kg/global_discovery/outbox_worker.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "global_discovery_outbox_worker",
        "retirement_criterion": "Move the global discovery outbox worker lifecycle behind runtime composition.",
    },
    "okto_pulse/core/kg/interfaces/registry.py::_registry": {
        "file": "okto_pulse/core/kg/interfaces/registry.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_provider_registry",
        "retirement_criterion": "Replace the process KG registry with edition-scoped graph runtime providers.",
    },
    "okto_pulse/core/kg/interfaces/registry.py::_configured": {
        "file": "okto_pulse/core/kg/interfaces/registry.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_provider_registry",
        "retirement_criterion": "Move KG registry configured state into the edition-scoped provider lifecycle.",
    },
    "okto_pulse/core/kg/kg_service.py::_default_service": {
        "file": "okto_pulse/core/kg/kg_service.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_service",
        "retirement_criterion": "Resolve KG service instances through application/runtime composition.",
    },
    "okto_pulse/core/kg/primitives.py::_graph_io_executor": {
        "file": "okto_pulse/core/kg/primitives.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "semantic_graph_executor",
        "retirement_criterion": "Replace the graph IO executor global with a graph runtime store capability.",
    },
    "okto_pulse/core/kg/session_manager.py::_singleton": {
        "file": "okto_pulse/core/kg/session_manager.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_session_manager",
        "retirement_criterion": "Move KG session manager lifecycle behind composition-owned KG providers.",
    },
    "okto_pulse/core/kg/workers/cleanup.py::_singleton": {
        "file": "okto_pulse/core/kg/workers/cleanup.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_cleanup_worker",
        "retirement_criterion": "Move cleanup worker lifecycle behind the KG worker lifecycle port.",
    },
    "okto_pulse/core/kg/workers/consolidation.py::_singleton": {
        "file": "okto_pulse/core/kg/workers/consolidation.py",
        "owner": "okto-pulse-core/kg",
        "target_provider": "kg_consolidation_worker",
        "retirement_criterion": "Move consolidation worker lifecycle behind the KG worker lifecycle port.",
    },
}

#: Frozen inventory (``file::name``) of EXISTING global-mutation / ContextVar
#: singletons in core at spec #15. Anything detected outside this set is NEW and
#: blocks (register-before-remove). Headline global/ContextVar singletons appear
#: here; ``_permission_cache`` is an in-place dict cache tracked by name in
#: SINGLETON_LEDGER, not by this detector.
BASELINE_SINGLETONS: frozenset[str] = frozenset(
    {
        "okto_pulse/core/ports/mcp_host.py::_mcp_host_provider",
        "okto_pulse/core/ports/kg_events.py::_kg_events_reader_port",
        "okto_pulse/core/application/kg_events_hub.py::_hub",
        "okto_pulse/core/events/dispatcher.py::_dispatcher",
        "okto_pulse/core/infra/auth.py::_auth_provider",
        "okto_pulse/core/infra/config.py::_settings_instance",
        "okto_pulse/core/infra/config.py::_version_provider",
        "okto_pulse/core/infra/database.py::_engine",
        "okto_pulse/core/infra/database.py::_session_factory",
        "okto_pulse/core/infra/database.py::_last_stale_warn_at",
        "okto_pulse/core/infra/schema_lifecycle.py::_orchestrator",
        "okto_pulse/core/infra/storage.py::_storage_provider",
        "okto_pulse/core/kg/backpressure.py::_default_gate",
        "okto_pulse/core/kg/global_discovery/outbox_worker.py::_singleton",
        "okto_pulse/core/kg/interfaces/registry.py::_registry",
        "okto_pulse/core/kg/interfaces/registry.py::_configured",
        "okto_pulse/core/kg/kg_service.py::_default_service",
        "okto_pulse/core/kg/primitives.py::_graph_io_executor",
        "okto_pulse/core/kg/session_manager.py::_singleton",
        "okto_pulse/core/kg/workers/cleanup.py::_singleton",
        "okto_pulse/core/kg/workers/consolidation.py::_singleton",
        "okto_pulse/core/kg/workers/cognitive_closeout.py::_worker",
        "okto_pulse/core/kg/workers/deterministic_worker.py::_whitelist_cache",
        "okto_pulse/core/kg/llm_provider_bridge_cache.py::_bridge_cache_registry",
        "okto_pulse/core/kg/write_barrier.py::_current_mode",
        "okto_pulse/core/kg/write_barrier.py::_active_guards",
        "okto_pulse/core/mcp/server.py::_mcp_authenticator",
        "okto_pulse/core/mcp/server.py::_mcp_session_factory",
        "okto_pulse/core/ports/coordination.py::_lease_provider",
        "okto_pulse/core/ports/coordination.py::_write_lock_port",
        "okto_pulse/core/ports/coordination.py::_claim_repository",
        "okto_pulse/core/ports/coordination.py::_runtime_settings_provider",
        "okto_pulse/core/ports/coordination.py::_config_validation_port",
        "okto_pulse/core/ports/kg_operational.py::_kg_operational_read_model_port",
        "okto_pulse/core/ports/kg_operational.py::_kg_governance_effects_port",
        "okto_pulse/core/ports/kg_operational.py::_kg_worker_queue_port",
        "okto_pulse/core/ports/kg_operational.py::_kg_worker_audit_port",
        "okto_pulse/core/ports/relational_effects.py::_relational_effects_port",
        "okto_pulse/core/ports/relational_application.py::_adapter",
        "okto_pulse/core/runtime_registry.py::_unit_of_work_factory",
        "okto_pulse/core/runtime_registry.py::_relational_runtime_factory",
        "okto_pulse/core/runtime_registry.py::_sqlite_pragma_installer",
        "okto_pulse/core/runtime_registry.py::_content_ingestion_resolver",
        "okto_pulse/core/mcp/server.py::_effective_resource_catalog",
        "okto_pulse/core/mcp/server.py::_instruction_providers",
        "okto_pulse/core/mcp/server.py::_instruction_providers_frozen",
        "okto_pulse/core/mcp/server.py::_resource_catalog_frozen",
        "okto_pulse/core/mcp/server.py::_RESOURCE_REGISTRY",
        "okto_pulse/core/mcp/server.py::_XML_SAFETY_DECORATED_COUNT",
        "okto_pulse/core/services/resource_gate.py::_IMPL_CLS",
        "okto_pulse/core/services/queue_health_service.py::_ALERT_FIRED_TOTAL",
        "okto_pulse/core/telemetry/event_store_registry.py::_factory",
        "okto_pulse/core/telemetry/product_aggregator_registry.py::_product_aggregator_factory",
        "okto_pulse/core/telemetry/publish_health_source_registry.py::_publish_health_source_provider",
        "okto_pulse/core/telemetry/sender_registry.py::_telemetry_sender_factory",
        "okto_pulse/core/telemetry/effect_config_registry.py::_provider",
        "okto_pulse/core/telemetry/telemetry_port_registry.py::_telemetry_port_factory",
        "okto_pulse/core/telemetry/telemetry_state_registry.py::_carrier",
    }
)

BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER: frozenset[str] = frozenset(
    {
        "okto_pulse/core/infra/database.py::_last_stale_warn_at",
        "okto_pulse/core/kg/write_barrier.py::_current_mode",
        "okto_pulse/core/kg/write_barrier.py::_active_guards",
        "okto_pulse/core/kg/workers/deterministic_worker.py::_whitelist_cache",
        "okto_pulse/core/mcp/server.py::_XML_SAFETY_DECORATED_COUNT",
        "okto_pulse/core/services/queue_health_service.py::_ALERT_FIRED_TOTAL",
    }
)

# F16 terminal state. Historical entries above remain as migration evidence, but
# no longer authorize runtime state. These ContextVars isolate values per task
# and are therefore architectural mechanisms, not shared mutable singletons.
SAFE_CONTEXT_LOCAL_STATE: frozenset[str] = frozenset(
    {
        "okto_pulse/core/composition.py::_active_runtime_composition",
        "okto_pulse/core/kg/write_barrier.py::_active_guards",
        "okto_pulse/core/runtime_context.py::_active_runtime_values",
    }
)
SINGLETON_LEDGER = {}
RUNTIME_SINGLETON_BASELINE_LEDGER = {}
BASELINE_SINGLETONS = frozenset()
BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER = frozenset()

_REQUIRED_RUNTIME_LEDGER_FIELDS = ("owner", "target_provider", "retirement_criterion")


@dataclass(frozen=True)
class SingletonOccurrence:
    """A module-global singleton found by the scanner."""

    name: str
    file: str
    kind: str  # "global_mutation" | "contextvar" | "provider_bridge_global_state"

    @property
    def key(self) -> str:
        return f"{self.file}::{self.name}"


@dataclass(frozen=True)
class AntiSingletonGateInput:
    source_root: Path | None = None
    #: extra ``file::name`` keys to treat as already-baselined (tests).
    extra_baseline: tuple[str, ...] = ()
    #: scan only these files (relative posix); () = whole core tree.
    only_files: tuple[str, ...] = ()


def _default_source_root() -> Path:
    # src/okto_pulse/core/application/boundary/singleton_gate.py -> src/
    return Path(__file__).resolve().parents[4]


def _is_contextvar(value: ast.expr | None) -> bool:
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    return (isinstance(func, ast.Name) and func.id == "ContextVar") or (
        isinstance(func, ast.Attribute) and func.attr == "ContextVar"
    )


def _module_level_targets(tree: ast.Module) -> dict[str, ast.expr | None]:
    out: dict[str, ast.expr | None] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id.startswith("_"):
                    out[tgt.id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id.startswith("_"):
                out[node.target.id] = node.value
    return out


_PROVIDER_BRIDGE_STATE_NAMES = frozenset({"_bridge_cache", "_bridge_lock"})
_MUTATING_METHODS = frozenset(
    {
        "add",
        "append",
        "clear",
        "discard",
        "extend",
        "pop",
        "popitem",
        "remove",
        "setdefault",
        "update",
    }
)


def _is_provider_bridge_file(rel: str) -> bool:
    return rel.startswith("okto_pulse/core/") and rel.endswith("/llm_provider_bridges.py")


def _is_module_constant_name(name: str) -> bool:
    stripped = name.strip("_")
    return bool(stripped) and stripped.upper() == stripped


def _call_name(expr: ast.expr | None) -> str:
    if not isinstance(expr, ast.Call):
        return ""
    func = expr.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _is_mutable_or_lock_value(value: ast.expr | None) -> bool:
    if isinstance(value, (ast.Dict, ast.List, ast.Set)):
        return True
    call_name = _call_name(value)
    return call_name in {"Lock", "RLock", "defaultdict", "OrderedDict"}


def _root_name(expr: ast.AST) -> str | None:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Subscript):
        return _root_name(expr.value)
    if isinstance(expr, ast.Attribute):
        return _root_name(expr.value)
    return None


def _mutated_module_names(tree: ast.Module) -> set[str]:
    mutated: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets: list[ast.AST]
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            else:
                targets = [node.target]
            for target in targets:
                if isinstance(target, (ast.Subscript, ast.Attribute)):
                    name = _root_name(target)
                    if name:
                        mutated.add(name)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = _root_name(node.func.value)
            if name and node.func.attr in _MUTATING_METHODS:
                mutated.add(name)
    return mutated


def _is_provider_bridge_state_name(name: str) -> bool:
    if name in _PROVIDER_BRIDGE_STATE_NAMES:
        return True
    if _is_module_constant_name(name):
        return False
    lower = name.lower()
    return name.startswith("_") and "bridge" in lower and (
        "cache" in lower or "lock" in lower
    )


def _provider_bridge_occurrences(
    rel: str,
    tree: ast.Module,
    module_names: dict[str, ast.expr | None],
) -> list[SingletonOccurrence]:
    if not _is_provider_bridge_file(rel):
        return []
    mutated_names = _mutated_module_names(tree)
    found: list[SingletonOccurrence] = []
    for name, value in module_names.items():
        if not _is_provider_bridge_state_name(name):
            continue
        if name in _PROVIDER_BRIDGE_STATE_NAMES:
            found.append(
                SingletonOccurrence(
                    name=name,
                    file=rel,
                    kind="provider_bridge_global_state",
                )
            )
            continue
        if name in mutated_names or _is_mutable_or_lock_value(value):
            found.append(
                SingletonOccurrence(
                    name=name,
                    file=rel,
                    kind="provider_bridge_global_state",
                )
            )
    return found


def _scan_module(rel: str, tree: ast.Module) -> list[SingletonOccurrence]:
    module_names = _module_level_targets(tree)
    global_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            global_names.update(node.names)
    found: list[SingletonOccurrence] = []
    for name, value in module_names.items():
        if name in global_names:
            kind = "global_mutation"
        elif _is_contextvar(value):
            kind = "contextvar"
        else:
            continue
        occurrence = SingletonOccurrence(name=name, file=rel, kind=kind)
        if occurrence.key not in SAFE_CONTEXT_LOCAL_STATE:
            found.append(occurrence)
    found.extend(_provider_bridge_occurrences(rel, tree, module_names))
    return found


def _runtime_ledger_entry(o: SingletonOccurrence) -> dict[str, str] | None:
    keyed = RUNTIME_SINGLETON_BASELINE_LEDGER.get(o.key)
    if keyed is not None:
        return keyed if keyed.get("file") == o.file else None
    named = SINGLETON_LEDGER.get(o.name)
    if named is not None and named.get("file") == o.file:
        return named
    return None


def _missing_runtime_ledger_entries(
    occurrences: list[SingletonOccurrence],
    baseline: set[str],
) -> list[dict[str, str]]:
    missing: list[dict[str, str]] = []
    for occurrence in occurrences:
        if occurrence.key not in baseline:
            continue
        if occurrence.key in BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER:
            continue
        entry = _runtime_ledger_entry(occurrence)
        reason = ""
        if entry is None:
            reason = "missing_runtime_ledger"
        elif any(not entry.get(field) for field in _REQUIRED_RUNTIME_LEDGER_FIELDS):
            reason = "missing_runtime_ledger_metadata"
        if reason:
            missing.append(
                {
                    "key": occurrence.key,
                    "name": occurrence.name,
                    "file": occurrence.file,
                    "kind": occurrence.kind,
                    "reason": reason,
                }
            )
    return sorted(missing, key=lambda item: item["key"])


class AntiSingletonGate:
    """Blocks new module-global singletons; ledgers the known ones."""

    gate_id = "anti_singleton"

    def run(self, gate_input: AntiSingletonGateInput | None = None) -> GateReport:
        gate_input = gate_input or AntiSingletonGateInput()
        root = gate_input.source_root or _default_source_root()
        core = root / "okto_pulse" / "core"
        baseline = set(BASELINE_SINGLETONS) | set(gate_input.extra_baseline)

        occurrences: list[SingletonOccurrence] = []
        for py in sorted(core.rglob("*.py")):
            rel = py.relative_to(root).as_posix()
            if gate_input.only_files and rel not in gate_input.only_files:
                continue
            try:
                tree = ast.parse(py.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            occurrences.extend(_scan_module(rel, tree))

        new_singletons = [o for o in occurrences if o.key not in baseline]
        missing_runtime_ledger = _missing_runtime_ledger_entries(occurrences, baseline)
        ledger_view = {
            name: {**meta, "status": "ledgered"}
            for name, meta in SINGLETON_LEDGER.items()
        }
        runtime_ledger_view = {
            key: {**meta, "status": "ledgered"}
            for key, meta in RUNTIME_SINGLETON_BASELINE_LEDGER.items()
        }
        evidence = {
            "ledger": ledger_view,
            "runtime_baseline_ledger": runtime_ledger_view,
            "non_runtime_baseline_exemptions": sorted(
                BASELINE_SINGLETONS_WITHOUT_RUNTIME_LEDGER
            ),
            "baseline_count": len(baseline),
            "detected_count": len(occurrences),
            "new_singletons": [
                {"name": o.name, "file": o.file, "kind": o.kind}
                for o in sorted(new_singletons, key=lambda o: o.key)
            ],
            "missing_runtime_ledger": missing_runtime_ledger,
            "scanned_root": core.relative_to(root).as_posix(),
        }

        if missing_runtime_ledger:
            return GateReport(
                gate_id=self.gate_id,
                subject="core module-global singletons",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "missing_singleton_ledger"},
                observed_value=[entry["key"] for entry in missing_runtime_ledger],
                expected_value=[],
                remediation_hint=(
                    "A baselined runtime singleton is missing owner, target provider "
                    "or retirement criterion. Add it to SINGLETON_LEDGER or the "
                    "per-occurrence RUNTIME_SINGLETON_BASELINE_LEDGER before accepting "
                    "the baseline."
                ),
            )

        if new_singletons:
            return GateReport(
                gate_id=self.gate_id,
                subject="core module-global singletons",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "new_singleton"},
                observed_value=sorted(o.key for o in new_singletons),
                expected_value=[],
                remediation_hint=(
                    "A new module-global singleton was introduced. Inject the "
                    "dependency through a RuntimeComposition provider/port instead. "
                    "If it is unavoidable transitional debt, register it in "
                    "BASELINE_SINGLETONS (and SINGLETON_LEDGER when it owns a runtime "
                    "resource) with owner, target provider and retirement criterion "
                    "(register-before-remove)."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="core module-global singletons",
            status="passed",
            severity="medium",
            owner=None,
            evidence={**evidence, "safe_context_local_state": sorted(SAFE_CONTEXT_LOCAL_STATE)},
            observed_value=0,
            expected_value=0,
        )
