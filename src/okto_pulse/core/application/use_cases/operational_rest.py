"""Operational REST use cases for AF35-S3 C4.

These use cases keep REST handlers transport-only while remaining
operational/KG services are strangled behind UoW-backed application calls.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
    commit,
)
from okto_pulse.core.ports.application_services import KnowledgeGraphOperations
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True)
class DataResult:
    data: Any


class BoardNotFoundError(EntityNotFoundError):
    def __init__(self, board_id: str) -> None:
        super().__init__("board", board_id)


class BugNotFoundError(EntityNotFoundError):
    def __init__(self, bug_id: str) -> None:
        super().__init__("bug", bug_id)


async def _require_board_access(
    uow: PulseUnitOfWork,
    board_id: str,
    actor: ActorContext,
    *,
    allowed_share_permissions: set[str] | None = None,
) -> Any:
    board = await load_accessible_board(
        uow,
        board_id,
        actor,
        allowed_share_permissions=allowed_share_permissions,
    )
    if board is None:
        raise BoardNotFoundError(board_id)
    return board


async def _require_resource_gate_entity(
    uow: PulseUnitOfWork,
    board_id: str,
    entity_type: str,
    entity_id: str,
) -> Any:
    """Resolve a Resource Gate child and prove it belongs to ``board_id``."""
    if entity_type == "ideation":
        entity = await uow.services.ideations.get_ideation(entity_id)
    elif entity_type == "refinement":
        entity = await uow.services.refinements.get_refinement(entity_id)
    elif entity_type == "spec":
        entity = await uow.services.specs.get_spec(entity_id)
    elif entity_type == "card":
        entity = await uow.services.cards.get_card(entity_id)
    else:
        entity = None
    if entity is None or getattr(entity, "board_id", None) != board_id:
        raise BoardNotFoundError(board_id)
    return entity


_RUNTIME_SETTINGS_WRITE_PERMISSIONS = (
    "runtime.settings.write",
    "settings.runtime.write",
)


def _permission_enabled(permissions: Any, required: str) -> bool:
    if isinstance(permissions, Mapping):
        if permissions.get("*") is True or permissions.get(required) is True:
            return True
        cursor: Any = permissions
        for part in required.split("."):
            if not isinstance(cursor, Mapping) or part not in cursor:
                return False
            cursor = cursor[part]
        return cursor is True
    checker = getattr(permissions, "check", None)
    if callable(checker):
        try:
            return checker(required) is None
        except Exception:
            return False
    if isinstance(permissions, (list, tuple, set, frozenset)):
        return required in permissions or "*" in permissions
    return False


def _require_runtime_settings_admin(actor: ActorContext) -> None:
    roles = {str(role).lower() for role in actor.roles}
    if roles.intersection({"admin", "operator"}):
        return
    if any(
        _permission_enabled(actor.permissions, permission)
        for permission in _RUNTIME_SETTINGS_WRITE_PERMISSIONS
    ):
        return
    raise PermissionDeniedError(
        "Runtime settings write requires an admin or operator capability"
    )


@dataclass(frozen=True)
class ResourceGateTaskCoverageCommand:
    board_id: str
    spec_id: str


@dataclass(frozen=True)
class ResourceGateEntityCommand:
    board_id: str
    entity_type: str
    entity_id: str


@dataclass(frozen=True)
class MarkResourceNotApplicableCommand(ResourceGateEntityCommand):
    resource_type: str
    justification: str | None
    source_channel: str


@dataclass(frozen=True)
class ClearResourceNotApplicableCommand(ResourceGateEntityCommand):
    resource_type: str
    reason: str | None


@dataclass(frozen=True)
class UpdateResourceGateBoardSettingsCommand:
    board_id: str
    require_spec_resource_task_coverage: bool


class GetSpecResourceTaskCoverageUseCase:
    async def execute(
        self, command: ResourceGateTaskCoverageCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        board = await _require_board_access(uow, command.board_id, actor)
        await _require_resource_gate_entity(
            uow,
            command.board_id,
            "spec",
            command.spec_id,
        )
        service = uow.services.resource_gate
        data = await service.validate_spec_resource_task_coverage(
            command.board_id,
            command.spec_id,
            enabled=service.is_spec_resource_task_coverage_required(board),
        )
        return DataResult(data)


class GetResourceGateSummaryUseCase:
    async def execute(
        self, command: ResourceGateEntityCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_board_access(uow, command.board_id, actor)
        await _require_resource_gate_entity(
            uow,
            command.board_id,
            command.entity_type,
            command.entity_id,
        )
        return DataResult(
            await uow.services.resource_gate.get_summary(
                command.board_id, command.entity_type, command.entity_id
            )
        )


class GetEffectiveResourcesUseCase:
    async def execute(
        self, command: ResourceGateEntityCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_board_access(uow, command.board_id, actor)
        await _require_resource_gate_entity(
            uow,
            command.board_id,
            command.entity_type,
            command.entity_id,
        )
        return DataResult(
            await uow.services.resource_gate.get_effective_resources(
                command.board_id, command.entity_type, command.entity_id
            )
        )


class MarkResourceNotApplicableUseCase:
    async def execute(
        self, command: MarkResourceNotApplicableCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        await _require_resource_gate_entity(
            uow,
            command.board_id,
            command.entity_type,
            command.entity_id,
        )
        data = await uow.services.resource_gate.mark_not_applicable(
            command.board_id,
            command.entity_type,
            command.entity_id,
            command.resource_type,
            actor.actor_id,
            justification=command.justification,
            source_channel=command.source_channel,
        )
        await commit(uow)
        return DataResult(data)


class ClearResourceNotApplicableUseCase:
    async def execute(
        self, command: ClearResourceNotApplicableCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:

        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        await _require_resource_gate_entity(
            uow,
            command.board_id,
            command.entity_type,
            command.entity_id,
        )
        data = await uow.services.resource_gate.clear_not_applicable(
            command.board_id,
            command.entity_type,
            command.entity_id,
            command.resource_type,
            actor.actor_id,
            reason=command.reason,
        )
        await commit(uow)
        return DataResult(data)


class UpdateResourceGateBoardSettingsUseCase:
    async def execute(
        self,
        command: UpdateResourceGateBoardSettingsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        settings = await uow.services.update_resource_gate_board_settings(
            command.board_id,
            actor.actor_id,
            require_spec_resource_task_coverage=(
                command.require_spec_resource_task_coverage
            ),
        )
        if settings is None:
            raise BoardNotFoundError(command.board_id)
        await commit(uow)
        return DataResult({"board_id": command.board_id, "settings": settings})


@dataclass(frozen=True)
class GetRuntimeSettingsCommand:
    pass


@dataclass(frozen=True)
class PutRuntimeSettingsCommand:
    values: dict[str, int]
    migration_plan_ref: str | None
    restart_policy: str | None
    scheduler_control: SchedulerControl | None


class GetRuntimeSettingsUseCase:
    async def execute(
        self, command: GetRuntimeSettingsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        return DataResult(await uow.services.get_runtime_settings())


class PutRuntimeSettingsUseCase:
    async def execute(
        self, command: PutRuntimeSettingsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        _require_runtime_settings_admin(actor)
        return DataResult(
            await uow.services.put_runtime_settings(
                command.values,
                actor_id=actor.actor_id,
                migration_plan_ref=command.migration_plan_ref,
                restart_policy=command.restart_policy,
                scheduler_control=command.scheduler_control,
            )
        )


@dataclass(frozen=True)
class GetLineageGraphCommand:
    board_id: str
    entity_type: str
    entity_id: str
    include_artifacts: bool


class GetLineageGraphUseCase:
    async def execute(
        self, command: GetLineageGraphCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise BoardNotFoundError(command.board_id)
        return DataResult(
            await uow.services.build_lineage_graph(
                command.board_id,
                entity_type=command.entity_type,
                entity_id=command.entity_id,
                include_artifacts=command.include_artifacts,
            )
        )


@dataclass(frozen=True)
class EvaluateBugCognitiveClosureByBugIdCommand:
    bug_id: str
    evidence: dict[str, Any]
    requested_action: str
    reason_code: str | None
    justification: str | None
    evidence_refs: list[str] | None
    revisit_at: str | None


class EvaluateBugCognitiveClosureByBugIdUseCase:
    async def execute(
        self,
        command: EvaluateBugCognitiveClosureByBugIdCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        from okto_pulse.core.application.use_cases.cognitive_readiness import (
            EvaluateBugCognitiveClosureCommand,
            EvaluateBugCognitiveClosureUseCase,
        )

        card = await uow.services.cards.get_card(command.bug_id)
        board_id = getattr(card, "board_id", None) if card is not None else None
        if not board_id:
            raise BugNotFoundError(command.bug_id)
        if await load_accessible_board(uow, board_id, actor) is None:
            raise BugNotFoundError(command.bug_id)
        result = await EvaluateBugCognitiveClosureUseCase().execute(
            EvaluateBugCognitiveClosureCommand(
                board_id,
                command.bug_id,
                evidence=command.evidence,
                requested_action=command.requested_action,
                reason_code=command.reason_code,
                justification=command.justification,
                evidence_refs=command.evidence_refs,
                revisit_at=command.revisit_at,
            ),
            actor=actor,
            uow=uow,
        )
        return DataResult(result.data)


@dataclass(frozen=True)
class CognitiveSkipCommand:
    board_id: str
    source_ref: str
    reason_code: str
    justification: str | None
    evidence_refs: list[str] | None
    revisit_at: str | None
    kg_generation_id: str | None


@dataclass(frozen=True)
class CognitiveClearCommand:
    board_id: str
    source_ref: str
    kg_generation_id: str | None


class RecordCognitiveSkipUseCase:
    def __init__(self, readiness_service_factory=None) -> None:
        self._readiness_service_factory = readiness_service_factory

    def _readiness_service(self):
        if self._readiness_service_factory is not None:
            return self._readiness_service_factory()
        from okto_pulse.core.services.application_kg import (
            build_cognitive_readiness_service,
        )

        return build_cognitive_readiness_service()

    async def execute(
        self, command: CognitiveSkipCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        service = self._readiness_service()
        item = await uow.services.kg.record_cognitive_skip(
            service,
            board_id=command.board_id,
            source_ref=command.source_ref,
            reason_code=command.reason_code,
            actor=actor.actor_id,
            actor_is_human=True,
            justification=command.justification,
            evidence_refs=command.evidence_refs,
            revisit_at=command.revisit_at,
            kg_generation_id=command.kg_generation_id,
        )
        verdict = await uow.services.kg.evaluate_cognitive_readiness(
            service,
            board_id=command.board_id,
            source_ref=command.source_ref,
            kg_generation_id=command.kg_generation_id,
        )
        enforcement_active = await uow.services.kg.cognitive_enforcement_active(
            command.board_id
        )
        return DataResult(
            {
                "item": item,
                "verdict": verdict,
                "enforcement_active": enforcement_active,
            }
        )


class ClearCognitiveSkipUseCase:
    def __init__(self, readiness_service_factory=None) -> None:
        self._readiness_service_factory = readiness_service_factory

    def _readiness_service(self):
        if self._readiness_service_factory is not None:
            return self._readiness_service_factory()
        from okto_pulse.core.services.application_kg import (
            build_cognitive_readiness_service,
        )

        return build_cognitive_readiness_service()

    async def execute(
        self, command: CognitiveClearCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        service = self._readiness_service()
        item = await uow.services.kg.clear_cognitive_skip(
            service,
            board_id=command.board_id,
            source_ref=command.source_ref,
            actor=actor.actor_id,
            actor_is_human=True,
            kg_generation_id=command.kg_generation_id,
        )
        verdict = await uow.services.kg.evaluate_cognitive_readiness(
            service,
            board_id=command.board_id,
            source_ref=command.source_ref,
            kg_generation_id=command.kg_generation_id,
        )
        enforcement_active = await uow.services.kg.cognitive_enforcement_active(
            command.board_id
        )
        return DataResult(
            {
                "item": item,
                "verdict": verdict,
                "enforcement_active": enforcement_active,
            }
        )


@dataclass(frozen=True)
class CognitiveReadinessMetricsCommand:
    board_id: str
    kg_generation_id: str | None


class GetCognitiveReadinessMetricsUseCase:
    def __init__(self, readiness_service_factory=None) -> None:
        self._readiness_service_factory = readiness_service_factory

    def _readiness_service(self):
        if self._readiness_service_factory is not None:
            return self._readiness_service_factory()
        from okto_pulse.core.services.application_kg import (
            build_cognitive_readiness_service,
        )

        return build_cognitive_readiness_service()

    async def execute(
        self,
        command: CognitiveReadinessMetricsCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            await uow.services.kg.cognitive_readiness_metrics(
                self._readiness_service(),
                board_id=command.board_id,
                kg_generation_id=command.kg_generation_id,
            )
        )


@dataclass(frozen=True)
class CognitiveEffectivenessInventoryCommand:
    board_id: str
    artifact_id: str | None
    include_candidate_logs: bool
    graph_layer: str
    scheduler_control: SchedulerControl | None


class GetCognitiveEffectivenessInventoryUseCase:
    async def execute(
        self,
        command: CognitiveEffectivenessInventoryCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        health = await uow.services.kg.health(
            command.board_id,
            scheduler_control=command.scheduler_control,
        )
        return DataResult(
            await uow.services.kg.cognitive_effectiveness_inventory(
                command.board_id,
                artifact_id=command.artifact_id,
                include_candidate_logs=command.include_candidate_logs,
                graph_layer=command.graph_layer,
                metric_status=health.get("metric_status"),
            )
        )


@dataclass(frozen=True)
class CanonicalDebtListCommand:
    board_id: str
    artifact_type: str | None
    state: str | None
    limit: int
    offset: int


@dataclass(frozen=True)
class CanonicalDebtRetryCommand:
    board_id: str
    debt_id: str
    scheduler_control: SchedulerControl | None


class ListCanonicalDebtUseCase:
    async def execute(
        self, command: CanonicalDebtListCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            await uow.services.kg.list_canonical_debt(
                board_id=command.board_id,
                artifact_type=command.artifact_type,
                state=command.state,
                limit=command.limit,
                offset=command.offset,
            )
        )


class RetryCanonicalDebtUseCase:
    async def execute(
        self, command: CanonicalDebtRetryCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        health = await uow.services.kg.health(
            command.board_id,
            scheduler_control=command.scheduler_control,
        )
        return DataResult(
            await uow.services.kg.schedule_canonical_debt_retry(
                board_id=command.board_id,
                debt_id=command.debt_id,
                actor_id=actor.actor_id,
                kg_health_state=str(
                    health.get("overall_state") or health.get("graph_state")
                ),
            )
        )


@dataclass(frozen=True)
class CanonicalPartitionListCommand:
    board_id: str
    reason_code: str | None
    graph_layer: str | None
    source_ref: str | None
    node_id: str | None
    status: str | None
    limit: int
    offset: int


@dataclass(frozen=True)
class CanonicalPartitionDetailCommand:
    board_id: str
    node_id: str


class ListCanonicalPartitionIntegrityUseCase:
    async def execute(
        self,
        command: CanonicalPartitionListCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            await uow.services.kg.list_canonical_partition_integrity(
                board_id=command.board_id,
                reason_code=command.reason_code,
                graph_layer=command.graph_layer,
                source_ref=command.source_ref,
                node_id=command.node_id,
                status=command.status,
                limit=command.limit,
                offset=command.offset,
            )
        )


class GetCanonicalPartitionIntegrityDetailUseCase:
    async def execute(
        self,
        command: CanonicalPartitionDetailCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            await uow.services.kg.canonical_partition_integrity_detail(
                board_id=command.board_id,
                node_id=command.node_id,
            )
        )


@dataclass(frozen=True)
class DigestLayerMismatchListCommand:
    board_id: str
    limit: int
    offset: int


class ListDigestLayerMismatchUseCase:
    async def execute(
        self,
        command: DigestLayerMismatchListCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            await uow.services.kg.list_digest_layer_mismatches(
                board_id=command.board_id,
                limit=command.limit,
                offset=command.offset,
            )
        )


@dataclass(frozen=True)
class OrphanIntegrityReportCommand:
    board_id: str
    generation_id: str | None
    limit: int


class GetOrphanIntegrityReportUseCase:
    def __init__(self, *, scanner_factory=None) -> None:
        self._scanner_factory = scanner_factory

    def _scanner(self):
        if self._scanner_factory is not None:
            return self._scanner_factory()
        from okto_pulse.core.kg.orphan_integrity import OrphanNodeScanner

        return OrphanNodeScanner()

    async def execute(
        self,
        command: OrphanIntegrityReportCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DataResult:
        await _require_board_access(uow, command.board_id, actor)
        return DataResult(
            self._scanner().scan(
                board_id=command.board_id,
                generation_id=command.generation_id,
                limit=command.limit,
            )
        )


@dataclass(frozen=True)
class OrphanBackfillCommand:
    board_id: str
    generation_id: str | None
    dry_run: bool
    node_ids: list[str] | None
    limit: int
    scheduler_control: SchedulerControl | None


class RunOrphanBackfillUseCase:
    def __init__(self, *, health_reader=None, reconciler_factory=None) -> None:
        self._health_reader = health_reader
        self._reconciler_factory = reconciler_factory

    async def _get_health(
        self,
        command: OrphanBackfillCommand,
        kg: KnowledgeGraphOperations,
    ) -> dict:
        if self._health_reader is not None:
            return await kg.invoke_health_reader(
                self._health_reader,
                command.board_id,
                scheduler_control=command.scheduler_control,
            )
        return await kg.health(
            command.board_id,
            scheduler_control=command.scheduler_control,
        )

    def _reconciler(self):
        if self._reconciler_factory is not None:
            return self._reconciler_factory()
        from okto_pulse.core.services.application_kg import (
            create_orphan_backfill_reconciler,
        )

        return create_orphan_backfill_reconciler()

    async def execute(
        self, command: OrphanBackfillCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DataResult:
        from okto_pulse.core.services.application_kg import max_orphan_sample_limit

        await _require_board_access(
            uow,
            command.board_id,
            actor,
            allowed_share_permissions={"editor", "admin"},
        )
        health = await self._get_health(command, uow.services.kg)
        state = str(health.get("overall_state") or health.get("graph_state") or "")
        if state in {"recovery_needed", "quarantined"}:
            return DataResult(
                {
                    "refused_by_health": {
                        "error": "kg_orphan_backfill_refused_by_health",
                        "board_id": command.board_id,
                        "overall_state": health.get("overall_state"),
                        "graph_state": health.get("graph_state"),
                        "operator_action": "inspect_kg_health_recovery_flow",
                    }
                }
            )
        limit = max(0, min(int(command.limit), max_orphan_sample_limit()))
        result = self._reconciler().run(
            board_id=command.board_id,
            generation_id=command.generation_id,
            dry_run=command.dry_run,
            node_ids=command.node_ids,
            limit=limit,
        )
        return DataResult(
            {
                "board_id": command.board_id,
                "generation_id": command.generation_id,
                "dry_run": command.dry_run,
                "backfill_summary": result.to_safe_dict(),
                "correlation_id": result.correlation_id,
            }
        )
