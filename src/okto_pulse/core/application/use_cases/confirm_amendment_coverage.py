"""Authorize and persist the validator-only amendment coverage attestation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext, commit
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True)
class ConfirmAmendmentCoverageCommand:
    board_id: str
    amendment_id: str
    regression_test_task_id: str
    regression_scenario_id: str


@dataclass(frozen=True)
class ConfirmAmendmentCoverageResult:
    coverage_confirmation: dict[str, Any]


class ConfirmAmendmentCoverageUseCase:
    """Apply the canonical policy before invoking the sole coverage writer."""

    async def execute(
        self,
        command: ConfirmAmendmentCoverageCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ConfirmAmendmentCoverageResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "amendment.coverage.confirm",
                legacy_operation="card.validation.submit",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        confirmation = await uow.services.cards.confirm_amendment_coverage(
            expected_board_id=command.board_id,
            amendment_id=command.amendment_id,
            regression_test_task_id=command.regression_test_task_id,
            regression_scenario_id=command.regression_scenario_id,
            reviewer_id=actor.actor_id,
            reviewer_name=actor.actor_name or actor.actor_id,
        )
        await commit(uow)
        return ConfirmAmendmentCoverageResult(confirmation)


__all__ = [
    "ConfirmAmendmentCoverageCommand",
    "ConfirmAmendmentCoverageResult",
    "ConfirmAmendmentCoverageUseCase",
]
