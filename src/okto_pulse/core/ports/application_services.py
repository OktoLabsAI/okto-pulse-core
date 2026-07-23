"""Typed application-service catalog exposed by a UnitOfWork.

Application use cases depend on this catalog instead of extracting an edition
transaction handle. The services remain Core-owned because they contain gates
and transition policy; edition composition supplies their persistence-backed
construction.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from okto_pulse.core.application.use_cases.entity_pagination import (
        EntityPageService,
    )
    from okto_pulse.core.ports.relational_application import PermissionPresetGateway
    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiService,
    )
    from okto_pulse.core.services.architecture import (
        ArchitectureDesignRepository,
        ArchitectureDiagramStore,
        ArchitecturePropagationService,
    )
    from okto_pulse.core.services.bug_regression_preview import (
        BugRegressionScenarioPreviewService,
    )
    from okto_pulse.core.services.default_board_config_api import (
        DefaultBoardConfigApiService,
    )
    from okto_pulse.core.services.design_system import (
        DesignSystemService,
        MockupDesignSystemGate,
    )
    from okto_pulse.core.services.discovery_catalog_reader import DiscoveryCatalogReader
    from okto_pulse.core.services.main import (
        AgentService,
        ArchiveService,
        AttachmentService,
        BoardService,
        CardService,
        CommentService,
        GuidelineService,
        IdeationKnowledgeService,
        IdeationQAService,
        IdeationService,
        QAService,
        RefinementKnowledgeService,
        RefinementQAService,
        RefinementService,
        ShareService,
        SpecKnowledgeService,
        SpecQAService,
        SpecService,
        SprintQAService,
        SprintService,
        StoryService,
    )
    from okto_pulse.core.services.resource_gate import ResourceGateService
    from okto_pulse.core.services.permission_policy import PermissionSet
    from okto_pulse.core.services.spec_structured_entities import (
        StructuredSpecEntityService,
    )


class ApplicationServiceCatalog(Protocol):
    """Core application capabilities scoped to one transaction."""

    @property
    def agent_authentication(self) -> object: ...

    @property
    def agents(self) -> "AgentService": ...

    @property
    def analytics(self) -> "AnalyticsOperations": ...

    @property
    def amendments(self) -> "AmendmentRevisionApiService": ...

    @property
    def architecture_designs(self) -> "ArchitectureDesignRepository": ...

    @property
    def architecture_diagrams(self) -> "ArchitectureDiagramStore": ...

    @property
    def architecture_propagation(self) -> "ArchitecturePropagationService": ...

    @property
    def archives(self) -> "ArchiveService": ...

    @property
    def attachments(self) -> "AttachmentService": ...

    @property
    def boards(self) -> "BoardService": ...

    @property
    def bug_regression_preview(self) -> "BugRegressionScenarioPreviewService": ...

    @property
    def cards(self) -> "CardService": ...

    @property
    def comments(self) -> "CommentService": ...

    @property
    def entity_pages(self) -> "EntityPageService": ...

    @property
    def default_board_config(self) -> "DefaultBoardConfigApiService": ...

    @property
    def design_systems(self) -> "DesignSystemService": ...

    @property
    def discovery_catalog(self) -> "DiscoveryCatalogReader": ...

    @property
    def guidelines(self) -> "GuidelineService": ...

    @property
    def ideation_knowledge(self) -> "IdeationKnowledgeService": ...

    @property
    def ideation_qa(self) -> "IdeationQAService": ...

    @property
    def ideations(self) -> "IdeationService": ...

    @property
    def knowledge_propagation(self) -> object: ...

    @property
    def kg(self) -> "KnowledgeGraphOperations": ...

    @property
    def mockup_design_gate(self) -> "MockupDesignSystemGate": ...

    @property
    def permission_presets(self) -> "PermissionPresetGateway": ...

    @property
    def qa(self) -> "QAService": ...

    @property
    def refinement_knowledge(self) -> "RefinementKnowledgeService": ...

    @property
    def refinement_qa(self) -> "RefinementQAService": ...

    @property
    def refinements(self) -> "RefinementService": ...

    @property
    def resource_gate(self) -> "ResourceGateService": ...

    @property
    def shares(self) -> "ShareService": ...

    @property
    def spec_knowledge(self) -> "SpecKnowledgeService": ...

    @property
    def spec_qa(self) -> "SpecQAService": ...

    @property
    def specs(self) -> "SpecService": ...

    @property
    def sprint_qa(self) -> "SprintQAService": ...

    @property
    def sprints(self) -> "SprintService": ...

    @property
    def stories(self) -> "StoryService": ...

    @property
    def structured_specs(self) -> "StructuredSpecEntityService": ...

    async def resolve_user_permissions(
        self, user_id: str, board_id: str
    ) -> "PermissionSet | dict[str, object]": ...

    async def resolve_actor_name(
        self, actor_id: str, board_id: str | None = None
    ) -> str: ...

    async def log_card_collaboration_activity(
        self,
        card_id: str,
        action: str,
        *,
        actor_id: str,
        actor_type: str,
        actor_name: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None: ...

    async def card_belongs_to_board(self, board_id: str, card_id: str) -> bool: ...

    async def comment_card_id(self, comment_id: str) -> str | None: ...

    async def resolve_choice_comment_actor_name(
        self, comment_id: str, actor_id: str
    ) -> str | None: ...

    async def qa_card_id(self, qa_id: str) -> str | None: ...

    async def compute_card_activity(
        self, card_id: str, *, limit: int = 50
    ) -> list[object]: ...

    async def compute_card_seen_status(self, card_id: str) -> object: ...

    async def build_propagation_legacy_report(
        self,
        *,
        board_id: str,
        limit: int = 100,
        offset: int = 0,
        include_clean: bool = False,
        parent_type_filter: str | None = None,
        surface: str = "service",
    ) -> dict[str, object]: ...

    async def resolve_effective_card_copy_plan(
        self, *, board_id: str, spec_id: str, resource_type: str
    ) -> dict[str, object]: ...

    async def load_effective_kb_items(
        self, source_entity_type: str, source_entity_id: str
    ) -> list[dict[str, object]]: ...

    async def load_effective_mockup_items(
        self, source_entity_type: str, source_entity_id: str
    ) -> list[dict[str, object]]: ...

    async def propagate_effective_resources_to_spec(
        self,
        *,
        board_id: str,
        spec: object,
        refinement_id: str,
        user_id: str,
        mockup_ids: list[str] | None = None,
        kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
        resolved_lineage: object | None = None,
    ) -> dict[str, object]: ...

    async def resolve_effective_spec_parent_lineage(
        self,
        *,
        board_id: str,
        ideation_id: str | None = None,
        refinement_id: str | None = None,
    ) -> object | None: ...

    async def list_my_mentions(
        self,
        *,
        board_id: str,
        agent_id: str,
        agent_name: str | None,
        include_seen: bool,
    ) -> tuple[list[dict[str, object]], bool]: ...

    async def mark_mentions_seen(
        self,
        *,
        board_id: str,
        agent_id: str,
        agent_name: str | None,
        item_ids: list[str],
    ) -> tuple[int, int]: ...

    async def get_unseen_summary(
        self, *, board_id: str, agent_id: str, agent_name: str | None
    ) -> dict[str, object]: ...

    async def get_activity_log_rows(
        self,
        *,
        board_id: str,
        limit: int,
        cursor_pair: tuple[datetime, str] | None,
        effective_offset: int,
        action: str | None,
        card_id: str | None,
        include_details: bool,
    ) -> tuple[list[object], tuple[datetime, str] | None]: ...

    async def update_resource_gate_board_settings(
        self,
        board_id: str,
        user_id: str,
        *,
        require_spec_resource_task_coverage: bool,
    ) -> dict[str, object] | None: ...

    async def get_runtime_settings(self) -> dict[str, object]: ...

    async def put_runtime_settings(
        self,
        values: dict[str, int],
        *,
        actor_id: str,
        migration_plan_ref: str | None,
        restart_policy: str | None,
        scheduler_control: object | None,
    ) -> dict[str, object]: ...

    async def build_lineage_graph(
        self,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool,
    ) -> dict[str, object]: ...

    async def list_discovery_selector_options(
        self,
        *,
        board_id: str,
        selector_kind: str,
        identity: str,
        spec_id: str | None,
        child_type: str | None,
        status: str | None,
        q: str | None,
        limit: int,
        offset: int,
        include_superseded: bool,
    ) -> tuple[dict[str, object], str]: ...

    async def execute_discovery_intent(
        self,
        *,
        identity: str,
        board_id: str,
        intent: object,
        params: dict[str, object],
    ) -> dict[str, object]: ...

    async def get_application_record(
        self,
        *,
        entity: str,
        record_id: str,
        includes: tuple[str, ...] = (),
    ) -> object | None: ...

    async def list_application_records(self, query: object) -> tuple[object, ...]: ...

    async def ideation_skip_overrides(
        self, ideation: object, board_id: str
    ) -> list[dict[str, object]]: ...

    async def spec_skip_overrides(
        self, spec: object, board_id: str
    ) -> list[dict[str, object]]: ...

    async def build_traceability_report(
        self,
        board_id: str,
        *,
        ideation_id: str = "",
        spec_id: str = "",
        include_artifacts: bool = True,
    ) -> dict[str, object]: ...

    async def get_default_board_template(self, template_id: str) -> object | None: ...

    async def resolve_active_default_board_template(
        self, scope: str
    ) -> object | None: ...


class AnalyticsOperations(Protocol):
    async def board_is_owned_by(self, board_id: str, user_id: str) -> bool: ...

    async def blockers(
        self,
        board_id: str,
        *,
        stale_hours: int = 72,
        filter_type: str | None = None,
    ) -> object: ...

    async def mcp_board_analytics(
        self,
        board_id: str,
        *,
        metric_type: str,
        dt_from: datetime | None,
        dt_to: datetime | None,
    ) -> object: ...

    async def funnel(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def velocity(
        self,
        board_id: str,
        *,
        granularity: str,
        weeks: int,
        days: int,
        dt_from: datetime | None,
        dt_to: datetime | None,
    ) -> object: ...

    async def coverage(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def overview(
        self, user_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def quality(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def validations(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def spec(self, board_id: str, spec_id: str) -> object | None: ...

    async def sprint(self, board_id: str, sprint_id: str) -> object | None: ...

    async def sprints(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def agents(
        self, board_id: str, *, dt_from: datetime | None, dt_to: datetime | None
    ) -> object: ...

    async def entities(
        self,
        entity_type: str,
        board_id: str,
        *,
        offset: int,
        limit: int,
        search: str,
        dt_from: datetime | None,
        dt_to: datetime | None,
    ) -> object: ...

    async def entity_detail(
        self, entity_type: str, board_id: str, entity_id: str
    ) -> object | None: ...


class KnowledgeGraphOperations(Protocol):
    async def dispatch_manual_tick(
        self,
        *,
        tick_id: str,
        board_id: str | None,
        force_full_rebuild: bool,
    ) -> None: ...

    async def evaluate_bug_cognitive_closure(
        self, readiness_service: object, **request: object
    ) -> dict[str, object]: ...

    async def list_cognitive_signals(
        self, readiness_service: object, **query: object
    ) -> dict[str, object]: ...

    async def evaluate_cognitive_readiness(
        self, readiness_service: object, **request: object
    ) -> object: ...

    async def cognitive_enforcement_active(self, board_id: str) -> bool: ...

    async def record_cognitive_skip(
        self, readiness_service: object, **request: object
    ) -> object: ...

    async def clear_cognitive_skip(
        self, readiness_service: object, **request: object
    ) -> object: ...

    async def cognitive_readiness_metrics(
        self,
        readiness_service: object,
        *,
        board_id: str,
        kg_generation_id: str | None,
    ) -> dict[str, object]: ...

    async def cognitive_effectiveness_inventory(
        self,
        board_id: str,
        *,
        artifact_id: str | None,
        include_candidate_logs: bool,
        graph_layer: str,
        metric_status: object,
    ) -> dict[str, object]: ...

    async def invoke_health_reader(
        self,
        reader: object,
        board_id: str,
        *,
        scheduler_control: object | None,
    ) -> dict[str, object]: ...

    async def begin_consolidation(self, request: object, *, agent_id: str) -> object: ...

    async def propose_reconciliation(
        self, request: object, *, agent_id: str
    ) -> object: ...

    async def commit_consolidation(
        self,
        request: object,
        *,
        board_id: str,
        agent_id: str,
        defer_session_finalization: bool = False,
    ) -> object: ...

    async def health(
        self, board_id: str, *, scheduler_control: object | None = None
    ) -> dict[str, object]: ...

    async def health_readiness(
        self,
        board_id: str,
        *,
        profile: str,
        surface: str,
        artifact_ref: str | None,
        scheduler_control: object | None = None,
    ) -> dict[str, object]: ...

    async def list_consolidation_audit(
        self, board_id: str, *, limit: int
    ) -> object: ...

    async def start_historical_consolidation(self, board_id: str) -> object: ...

    async def cancel_historical(self, board_id: str) -> object: ...

    async def get_historical_progress(self, board_id: str) -> object: ...

    async def right_to_erasure(self, board_id: str) -> object: ...

    async def list_pending_entries(self, board_id: str) -> object: ...

    async def build_pending_tree(self, board_id: str, *, depth: int) -> object: ...

    async def retry_pending_entry(
        self, board_id: str, queue_entry_id: str, *, recursive: bool
    ) -> object: ...

    async def boost_node(
        self, board_id: str, node_id: str, *, actor_id: str
    ) -> object: ...

    async def reprocess_dead_letter_rows(
        self,
        board_id: str,
        *,
        dead_letter_ids: list[str] | None,
        limit: int,
    ) -> dict[str, object]: ...

    async def diagnose_connectivity_guard_dlq(
        self, board_id: str
    ) -> dict[str, object]: ...

    async def reprocess_connectivity_guard_dlq(
        self, board_id: str, dead_letter_ids: list[str]
    ) -> dict[str, object]: ...

    async def verify_connectivity_class_cleared(
        self, board_id: str, *, artifact_refs: list[str] | None
    ) -> dict[str, object]: ...

    async def list_cognitive_dlq_rows(
        self, board_id: str, *, limit: int, offset: int
    ) -> object: ...

    async def list_dead_letter_rows(
        self, board_id: str, *, limit: int, offset: int
    ) -> dict[str, object]: ...

    async def list_stale_canonical_parity(
        self, board_id: str, *, limit: int, offset: int
    ) -> object: ...

    async def query_takedown_telemetry(
        self,
        *,
        board_id: str,
        delete_event_id: str | None = None,
        delivery_key: str | None = None,
    ) -> dict[str, object]: ...

    async def list_canonical_debt(
        self,
        *,
        board_id: str,
        artifact_type: str | None,
        state: str | None,
        limit: int,
        offset: int,
    ) -> object: ...

    async def schedule_canonical_debt_retry(
        self,
        *,
        board_id: str,
        debt_id: str,
        actor_id: str,
        kg_health_state: str,
    ) -> dict[str, object]: ...

    async def list_canonical_partition_integrity(
        self, *, board_id: str, **filters: object
    ) -> object: ...

    async def canonical_partition_integrity_detail(
        self, *, board_id: str, node_id: str
    ) -> object: ...

    async def list_digest_layer_mismatches(
        self, *, board_id: str, limit: int, offset: int
    ) -> object: ...

    async def enqueue_digest_layer_reconciliation(
        self, *, board_id: str, reason: str
    ) -> dict[str, object]: ...

    async def build_global_discovery_recovery_seeds(
        self,
        *,
        boards: list[tuple[str, str, str]],
        captured_cognitive_pending_exclusions: Mapping[
            str, Mapping[str, str]
        ],
    ) -> tuple[object, ...]: ...

    async def recover_global_discovery_delivery(
        self, *, run_id: str, board_ids: list[str], dead_letter_limit: int
    ) -> dict[str, object]: ...

    async def queue_health(self) -> dict[str, object]: ...

    async def queue_drilldown(self, board_id: str | None) -> dict[str, object]: ...

    async def invoke_rebuild_admission(
        self,
        refusal_check: object,
        board_id: str,
        *,
        scheduler_control: object | None,
    ) -> object: ...

    async def read_latest_kg_tick_completed_at(self) -> datetime | None: ...

    async def list_board_ids(self) -> list[str]: ...

    async def publish_kg_tick_events(self, *, scheduled_at: str) -> list[str]: ...

    async def publish_domain_event(self, event: object) -> None: ...


__all__ = [
    "AnalyticsOperations",
    "ApplicationServiceCatalog",
    "KnowledgeGraphOperations",
]
