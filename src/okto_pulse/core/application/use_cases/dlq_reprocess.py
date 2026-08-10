"""KG DLQ reprocess / connectivity use cases (SaaS Refactor spec R01A MCP-FU2).

Transport-free reimplementations of the four KG DLQ/connectivity MCP tools that
open a relational session — ``dead_letter_reprocess``, ``connectivity_dlq_diagnose``,
``connectivity_dlq_reprocess``, ``connectivity_dlq_verify`` — so ``mcp/server.py``
no longer opens a raw ``get_db_for_mcp()`` session for them. Each delegates to the
existing service so payloads/errors are byte-identical.

Commit semantics are preserved EXACTLY from the legacy tools (proven by the golden
suite, not assumed):
- ``dead_letter_reprocess`` committed explicitly → the use case commits.
- ``connectivity_dlq_reprocess`` committed only when NOT blocked → the use case
  commits only when ``data["blocked"]`` is falsey (fail-closed selections remove
  no DLQ and never commit).
- ``diagnose`` / ``verify`` are read-only → no commit.

The public signatures are transport-neutral (no ``AsyncSession``); the worker
``process_now`` signalling stays in the adapter (transport/orchestration).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext, commit
from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
    EvaluateCodeTraceabilityKGReadAccessUseCase,
    require_code_traceability_safe_arbitrary_query,
)
from okto_pulse.core.domain.code_traceability_kg import (
    KGDeadLetterReprocessScope,
)


class ReprocessDeadLetterRowsCommand:
    __slots__ = ("board_id", "dead_letter_ids", "limit", "scope")

    def __init__(
        self,
        board_id: str,
        *,
        dead_letter_ids: list[str] | None = None,
        limit: int = 50,
        scope: KGDeadLetterReprocessScope = KGDeadLetterReprocessScope.GENERIC,
    ) -> None:
        self.board_id = board_id
        self.dead_letter_ids = dead_letter_ids
        self.limit = limit
        self.scope = KGDeadLetterReprocessScope(scope)


class ReprocessDeadLetterRowsResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class ReprocessDeadLetterRowsUseCase:
    """Requeue dead-lettered KG consolidation rows (WRITE, commits)."""

    async def execute(
        self, command: ReprocessDeadLetterRowsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ReprocessDeadLetterRowsResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.queue.reprocess",
                legacy_operation="kg.admin.settings_write",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        kwargs: dict[str, Any] = {
            "dead_letter_ids": command.dead_letter_ids,
            "limit": command.limit,
        }
        if command.scope is KGDeadLetterReprocessScope.CODE_TRACEABILITY:
            access = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
                actor=actor,
                board_id=command.board_id,
                uow=uow,
            )
            require_code_traceability_safe_arbitrary_query(access)
            kwargs["scope"] = command.scope
        data = await uow.services.kg.reprocess_dead_letter_rows(
            command.board_id,
            **kwargs,
        )
        if (
            command.scope is KGDeadLetterReprocessScope.GENERIC
            or bool(data.get("mutated"))
        ):
            await commit(uow)
        return ReprocessDeadLetterRowsResult(data)


class DiagnoseConnectivityDlqCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class DiagnoseConnectivityDlqResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class DiagnoseConnectivityDlqUseCase:
    """Diagnose the connectivity-guard technical_dlq class (READ-ONLY)."""

    async def execute(
        self, command: DiagnoseConnectivityDlqCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> DiagnoseConnectivityDlqResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.queue.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.diagnose_connectivity_guard_dlq(
            command.board_id
        )
        return DiagnoseConnectivityDlqResult(data)


class ReprocessConnectivityDlqCommand:
    __slots__ = ("board_id", "dead_letter_ids")

    def __init__(self, board_id: str, dead_letter_ids: list[str]) -> None:
        self.board_id = board_id
        self.dead_letter_ids = dead_letter_ids


class ReprocessConnectivityDlqResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class ReprocessConnectivityDlqUseCase:
    """Fail-closed reprocess of the connectivity-guard technical_dlq class (WRITE).

    Commits ONLY when the service did not block — a blocked (fail-closed) selection
    removes no DLQ and must not commit, exactly as the legacy tool did.
    """

    async def execute(
        self, command: ReprocessConnectivityDlqCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ReprocessConnectivityDlqResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.queue.reprocess",
                legacy_operation="kg.admin.settings_write",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.reprocess_connectivity_guard_dlq(
            command.board_id,
            command.dead_letter_ids,
        )
        if not data.get("blocked"):
            await commit(uow)
        return ReprocessConnectivityDlqResult(data)


class VerifyConnectivityClassCommand:
    __slots__ = ("board_id", "artifact_refs")

    def __init__(self, board_id: str, *, artifact_refs: list[str] | None = None) -> None:
        self.board_id = board_id
        self.artifact_refs = artifact_refs


class VerifyConnectivityClassResult:
    __slots__ = ("data",)

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data


class VerifyConnectivityClassUseCase:
    """Confirm the connectivity-guard class is cleared (READ-ONLY)."""

    async def execute(
        self, command: VerifyConnectivityClassCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> VerifyConnectivityClassResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.queue.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        data = await uow.services.kg.verify_connectivity_class_cleared(
            command.board_id,
            artifact_refs=command.artifact_refs,
        )
        return VerifyConnectivityClassResult(data)
