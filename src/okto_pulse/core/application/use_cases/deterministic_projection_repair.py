"""Authorized exact-source deterministic replay, independent of graph backend."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement, require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext, CommandValidationError, ConflictError, EntityNotFoundError, commit,
)
from okto_pulse.core.application.use_cases.operational_rest import _require_board_access
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True)
class RepairSpecProjectionCommand:
    board_id: str
    spec_ids: tuple[str, ...]
    reason: str
    scheduler_control: object = None


class RepairSpecProjectionUseCase:
    async def execute(
        self, command: RepairSpecProjectionCommand, *, actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> dict[str, object]:
        if not 1 <= len(command.spec_ids) <= 25 or not 3 <= len(command.reason.strip()) <= 1000:
            raise CommandValidationError("Select 1-25 specs and provide a 3-1000 character audit reason")
        try:
            spec_ids = tuple(dict.fromkeys(str(UUID(item)) for item in command.spec_ids))
        except (ValueError, TypeError, AttributeError) as exc:
            raise CommandValidationError("Spec IDs must be UUIDs") from exc
        await _require_board_access(
            uow, command.board_id, actor, allowed_share_permissions={"editor", "admin"},
        )
        await require_authorization(
            actor,
            PermissionRequirement("kg.operations.queue.reprocess", legacy_operation="kg.admin.settings_write"),
            uow=uow, board_id=command.board_id,
        )
        health = await uow.services.kg.health(
            command.board_id, scheduler_control=command.scheduler_control,
        )
        # Unknown/new/missing states are not evidence that replay is safe.
        # AT_RISK remains eligible for the historical missing-projection case;
        # the worker independently revalidates authority before any graph write.
        if (
            not isinstance(health, Mapping)
            or health.get("graph_state") != "healthy"
            or health.get("overall_state") not in ("healthy", "at_risk")
        ):
            raise ConflictError("deterministic_projection_repair_health", command.board_id)
        # Validate ALL sources before any enqueue. A foreign ID remains a 404,
        # not a source-content disclosure and never a partial accepted batch.
        for spec_id in spec_ids:
            spec = await uow.specs.get(spec_id)
            if spec is None or spec.board_id != command.board_id:
                raise EntityNotFoundError("spec", spec_id)
            status = getattr(spec.status, "value", spec.status)
            if spec.archived or status not in {"done", "approved", "validated"}:
                raise ConflictError("spec_not_canonical_eligible", spec_id)
        result = await uow.services.kg.stage_spec_projection_repair(
            board_id=command.board_id, spec_ids=spec_ids,
            actor_id=actor.actor_id, reason=command.reason.strip(),
        )
        await commit(uow)
        if result["queued_count"]:
            from okto_pulse.core.application.runtime_workers import signal_runtime_worker
            # Durable admission succeeded even if no local worker is running.
            try:
                signal_runtime_worker("consolidation_worker")
            except Exception:
                pass
        return result
