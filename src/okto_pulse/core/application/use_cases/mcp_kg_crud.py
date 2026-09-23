"""Transport-neutral canonical debt reader using the application unit of work."""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)


class ListCanonicalDebtCommand:
    """Input for :class:`ListCanonicalDebtUseCase`."""

    __slots__ = ("board_id", "artifact_type", "state", "limit", "offset")

    def __init__(
        self,
        board_id: str,
        *,
        artifact_type: str | None = None,
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> None:
        self.board_id = board_id
        self.artifact_type = artifact_type
        self.state = state
        self.limit = limit
        self.offset = offset


class ListCanonicalDebtResult:
    """Output — carries the reader result object (``.items``/``.counts``/``.total``)."""

    __slots__ = ("data",)

    def __init__(self, data: Any) -> None:
        self.data = data


class ListCanonicalDebtUseCase:
    """List canonical-debt ledger rows without any transport dependency."""

    async def execute(
        self, command: ListCanonicalDebtCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListCanonicalDebtResult:
        from okto_pulse.core.services.canonical_debt_service import (
            validate_canonical_debt_filters,
        )

        validate_canonical_debt_filters(
            artifact_type=command.artifact_type,
            state=command.state,
        )
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.integrity.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
            EvaluateCodeTraceabilityKGReadAccessUseCase,
        )

        ct_access = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
            actor=actor,
            board_id=command.board_id,
            uow=uow,
        )
        kwargs = {
            "board_id": command.board_id,
            "artifact_type": command.artifact_type,
            "state": command.state,
            "limit": command.limit,
            "offset": command.offset,
        }
        if not ct_access.allowed:
            kwargs["include_code_traceability"] = False
        data = await uow.services.kg.list_canonical_debt(**kwargs)
        return ListCanonicalDebtResult(data)
