"""list_cognitive_dlq use case (SaaS Refactor spec R01A MCP-FU3B, read-only).

Transport-free reimplementation of the relational read behind the
``okto_pulse_kg_list_cognitive_dlq`` MCP tool. The tool used to issue SQL inline
(``select(ConsolidationDeadLetter)``); that query is now a dedicated reader
(``dead_letter_inspector_service.list_cognitive_dlq_rows``) so the use case stays
free of ``select``/ORM. Read-only: no commit. The public signature is
transport-neutral; the row projection (normalized artifact id, technical_dlq
framing, response envelope) stays in the adapter so the payload is unchanged.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext


class ListCognitiveDlqCommand:
    __slots__ = ("board_id", "limit", "offset")

    def __init__(self, board_id: str, *, limit: int, offset: int) -> None:
        self.board_id = board_id
        self.limit = limit
        self.offset = offset


class ListCognitiveDlqResult:
    """Output — the total count + the page of DLQ rows for the adapter to project."""

    __slots__ = ("total", "rows")

    def __init__(self, total: int, rows: list[Any]) -> None:
        self.total = total
        self.rows = rows


class ListCognitiveDlqUseCase:
    """List the board's technical-DLQ rows, transport-free."""

    async def execute(
        self, command: ListCognitiveDlqCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListCognitiveDlqResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.cognitive.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        total, rows = await uow.services.kg.list_cognitive_dlq_rows(
            command.board_id,
            limit=command.limit,
            offset=command.offset,
        )
        return ListCognitiveDlqResult(total, rows)
