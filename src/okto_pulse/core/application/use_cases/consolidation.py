"""KG consolidation write use cases (SaaS Refactor spec R01A MCP-FU1, MCP writes).

Transport-free reimplementations of the three KG consolidation MCP tools that open
a relational session — ``begin_consolidation``, ``propose_reconciliation``,
``commit_consolidation`` — so ``kg_tools`` no longer opens a raw ``get_db()``
session for them. Each delegates to the existing primitive so payloads and errors
are byte-identical; ``KGPrimitiveError`` propagates for the MCP adapter to map.

Write parity is asserted empirically (the success golden proves persistence, the
rollback oracle proves no-partial-persist on mid-flow failure). The per-board
commit lock/retry coordinator is preserved for ``commit_consolidation``. The
session ownership pre-check (``_require_open_session``) stays in the adapter so the
error envelopes (with/without the R7 cognitive-hold side effect) are unchanged.
The public signatures are transport-neutral — they never expose ``AsyncSession``.
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import ActorContext


class BeginConsolidationCommand:
    __slots__ = ("req",)

    def __init__(self, req: Any) -> None:
        self.req = req


class BeginConsolidationResult:
    __slots__ = ("resp",)

    def __init__(self, resp: Any) -> None:
        self.resp = resp


class BeginConsolidationUseCase:
    """Open a transactional consolidation session, transport-free."""

    async def execute(
        self, command: BeginConsolidationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> BeginConsolidationResult:
        resp = await uow.services.kg.begin_consolidation(
            command.req,
            agent_id=actor.actor_id,
        )
        return BeginConsolidationResult(resp)


class ProposeReconciliationCommand:
    __slots__ = ("req",)

    def __init__(self, req: Any) -> None:
        self.req = req


class ProposeReconciliationResult:
    __slots__ = ("resp",)

    def __init__(self, resp: Any) -> None:
        self.resp = resp


class ProposeReconciliationUseCase:
    """Compute deterministic reconciliation hints, transport-free."""

    async def execute(
        self, command: ProposeReconciliationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ProposeReconciliationResult:
        resp = await uow.services.kg.propose_reconciliation(
            command.req,
            agent_id=actor.actor_id,
        )
        return ProposeReconciliationResult(resp)


class CommitConsolidationCommand:
    __slots__ = ("req", "board_id")

    def __init__(self, req: Any, *, board_id: str) -> None:
        self.req = req
        # Resolved by the adapter via _require_open_session (ownership pre-check),
        # used to key the per-board commit lock.
        self.board_id = board_id


class CommitConsolidationResult:
    __slots__ = ("resp",)

    def __init__(self, resp: Any) -> None:
        self.resp = resp


class CommitConsolidationUseCase:
    """Atomically commit a consolidation session under the per-board commit lock,
    transport-free. ``KGPrimitiveError`` propagates for the adapter to map (and to
    drive the R7 cognitive-hold side effect)."""

    async def execute(
        self, command: CommitConsolidationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> CommitConsolidationResult:
        resp = await uow.services.kg.commit_consolidation(
            command.req,
            board_id=command.board_id,
            agent_id=actor.actor_id,
        )
        return CommitConsolidationResult(resp)
