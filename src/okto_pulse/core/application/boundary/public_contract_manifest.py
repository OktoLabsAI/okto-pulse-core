"""Stable, edition-neutral manifest of supported Core import surfaces.

Downstream editions import this manifest from the installed Core distribution and
gate every static or dynamic Core dependency against it.  Keeping the declaration
in Core prevents Community/SaaS allowlists from silently drifting apart.
"""

from __future__ import annotations

import hashlib

PUBLIC_CORE_CONTRACT_MANIFEST_VERSION = "1.0"

PUBLIC_CORE_CONTRACT_SURFACES: tuple[str, ...] = tuple(sorted((
    "okto_pulse.core.AuthProvider",
    "okto_pulse.core.AuthenticationError",
    "okto_pulse.core.AuthenticationPort",
    "okto_pulse.core.AuthorizationDenied",
    "okto_pulse.core.CoreSettings",
    "okto_pulse.core.Credential",
    "okto_pulse.core.DEFAULT_STREAM_CHUNK_SIZE",
    "okto_pulse.core.InvalidCredential",
    "okto_pulse.core.MissingCredential",
    "okto_pulse.core.Principal",
    "okto_pulse.core.RelationalSchemaLifecycleOrchestrator",
    "okto_pulse.core.StorageObjectStat",
    "okto_pulse.core.StorageProvider",
    "okto_pulse.core.application",
    "okto_pulse.core.composition",
    "okto_pulse.core.composition.isolated_runtime_provider_scope",
    "okto_pulse.core.configure_auth",
    "okto_pulse.core.configure_settings",
    "okto_pulse.core.configure_storage",
    "okto_pulse.core.discovery_intent_catalog",
    "okto_pulse.core.domain",
    "okto_pulse.core.events",
    "okto_pulse.core.events.types.QualityAssessmentRecorded",
    "okto_pulse.core.events.types.QualityClarificationChanged",
    "okto_pulse.core.get_auth_provider",
    "okto_pulse.core.get_settings",
    "okto_pulse.core.get_storage_provider",
    "okto_pulse.core.inbound.enum_error_envelope",
    "okto_pulse.core.inbound.guideline_policy_cursor.policy_cursor_codec_from_settings",
    "okto_pulse.core.inbound.guideline_policy_error.guideline_policy_http_status",
    "okto_pulse.core.inbound.guideline_policy_error.project_guideline_policy_error",
    "okto_pulse.core.inbound.policy_transition_error.project_policy_transition_rejection",
    "okto_pulse.core.inbound.quality_assessment_error",
    "okto_pulse.core.inbound.ska_contract_error",
    "okto_pulse.core.kg.async_bridge",
    "okto_pulse.core.kg.board_rebuild_adapter",
    "okto_pulse.core.kg.board_source_store",
    "okto_pulse.core.kg.canonical_cognitive_preservation",
    "okto_pulse.core.kg.candidate_decision_store",
    "okto_pulse.core.kg.cognitive_action_center",
    "okto_pulse.core.kg.cognitive_badge_resolver",
    "okto_pulse.core.kg.cognitive_readiness",
    "okto_pulse.core.kg.config_guard",
    "okto_pulse.core.kg.cypher_templates",
    "okto_pulse.core.kg.cursor_codec",
    "okto_pulse.core.kg.data_provider_ownership_gate",
    "okto_pulse.core.kg.dedup_migration",
    "okto_pulse.core.kg.embedding",
    "okto_pulse.core.kg.global_discovery.schema",
    "okto_pulse.core.kg.graph_availability",
    "okto_pulse.core.kg.health",
    "okto_pulse.core.kg.health_state",
    "okto_pulse.core.kg.hybrid_search.hybrid",
    "okto_pulse.core.kg.interfaces",
    "okto_pulse.core.kg.kg_service",
    "okto_pulse.core.kg.orphan_integrity",
    "okto_pulse.core.kg.primitives",
    "okto_pulse.core.kg.quarantine",
    "okto_pulse.core.kg.rebuild_audit",
    "okto_pulse.core.kg.rebuild_confirmation",
    "okto_pulse.core.kg.rebuild_deterministic",
    "okto_pulse.core.kg.rebuild_generation",
    "okto_pulse.core.kg.rebuild_preflight",
    "okto_pulse.core.kg.rebuild_report",
    "okto_pulse.core.kg.rebuild_service",
    "okto_pulse.core.kg.rebuild_sources",
    "okto_pulse.core.kg.relational_projection",
    "okto_pulse.core.kg.rerank.factory",
    "okto_pulse.core.kg.rerank.token_overlap",
    "okto_pulse.core.kg.safe_write_lifecycle",
    "okto_pulse.core.kg.schema_contract",
    "okto_pulse.core.kg.schemas",
    "okto_pulse.core.kg.scoring",
    "okto_pulse.core.kg.search",
    "okto_pulse.core.kg.session_manager",
    "okto_pulse.core.kg.single_writer_lock",
    "okto_pulse.core.kg.stress_runner",
    "okto_pulse.core.kg.tier_power",
    "okto_pulse.core.kg.write_barrier",
    "okto_pulse.core.mcp",
    "okto_pulse.core.models",
    "okto_pulse.core.ports",
    "okto_pulse.core.register_package_version_provider",
    "okto_pulse.core.register_relational_schema_lifecycle_orchestrator",
    "okto_pulse.core.repositories",
    "okto_pulse.core.repositories.interfaces",
    "okto_pulse.core.reset_auth_for_tests",
    "okto_pulse.core.reset_package_version_provider_for_tests",
    "okto_pulse.core.reset_relational_schema_lifecycle_orchestrator",
    "okto_pulse.core.resolve_relational_schema_lifecycle_orchestrator",
    "okto_pulse.core.runtime_registry",
    "okto_pulse.core.services.amendment_revision",
    "okto_pulse.core.services.amendment_revision_api",
    "okto_pulse.core.services.analytics_contract",
    "okto_pulse.core.services.analytics_service",
    "okto_pulse.core.services.application_agents",
    "okto_pulse.core.services.application_kg",
    "okto_pulse.core.services.application_startup",
    "okto_pulse.core.services.ambiguity_assessment",
    "okto_pulse.core.services.architecture",
    "okto_pulse.core.services.architecture_observability",
    "okto_pulse.core.services.bug_regression_preview",
    "okto_pulse.core.services.cancellation",
    "okto_pulse.core.services.checklist",
    "okto_pulse.core.services.cognitive_effectiveness_service",
    "okto_pulse.core.services.default_board_config_api",
    "okto_pulse.core.services.default_board_configuration",
    "okto_pulse.core.services.design_system",
    "okto_pulse.core.services.discovery_executor",
    "okto_pulse.core.services.discovery_selector_catalog",
    "okto_pulse.core.services.effective_resource_propagation",
    "okto_pulse.core.services.gate_contracts",
    "okto_pulse.core.services.kg_health_readiness_service",
    "okto_pulse.core.services.knowledge_propagation",
    "okto_pulse.core.services.quality_assessment",
    "okto_pulse.core.services.quality_assessment_legacy_import",
    "okto_pulse.core.services.quality_assessment_lifecycle",
    "okto_pulse.core.services.quality_projection_currentness",
    "okto_pulse.core.services.reference_resolution",
    "okto_pulse.core.services.requirement_lint_assessment",
    "okto_pulse.core.services.requirement_lint_writer",
    "okto_pulse.core.services.research_decision_ledger",
    "okto_pulse.core.services.resource_gate",
    "okto_pulse.core.services.resource_gate_contracts",
    "okto_pulse.core.services.spec_entity_canonicalization",
    "okto_pulse.core.services.resource_lineage",
    "okto_pulse.core.services.ska_observability",
    "okto_pulse.core.services.spec_structured_entities",
    "okto_pulse.core.services.test_scenario_lifecycle",
    "okto_pulse.core.telemetry",
)))

PUBLIC_CORE_CONTRACT_MANIFEST_DIGEST = hashlib.sha256(
    "\n".join(PUBLIC_CORE_CONTRACT_SURFACES).encode("utf-8")
).hexdigest()

_PRIVATE_CORE_IMPLEMENTATION_PREFIXES: tuple[str, ...] = (
    "okto_pulse.core.infra.database",
    "okto_pulse.core.kg.governance",
    "okto_pulse.core.kg.interfaces.registry",
    "okto_pulse.core.kg.workers",
    "okto_pulse.core.mcp.server",
    "okto_pulse.core.models.db",
    "okto_pulse.core.repositories.sqlalchemy",
    "okto_pulse.core.services.main",
    "okto_pulse.core.services.settings_service",
)


def is_public_core_contract(reference: str) -> bool:
    """Return whether a fully-qualified reference is covered by the manifest."""

    if any(
        reference == private or reference.startswith(private + ".")
        for private in _PRIVATE_CORE_IMPLEMENTATION_PREFIXES
    ):
        return False
    return any(
        reference == allowed or reference.startswith(allowed + ".")
        for allowed in PUBLIC_CORE_CONTRACT_SURFACES
    )


__all__ = [
    "PUBLIC_CORE_CONTRACT_MANIFEST_DIGEST",
    "PUBLIC_CORE_CONTRACT_MANIFEST_VERSION",
    "PUBLIC_CORE_CONTRACT_SURFACES",
    "is_public_core_contract",
]
