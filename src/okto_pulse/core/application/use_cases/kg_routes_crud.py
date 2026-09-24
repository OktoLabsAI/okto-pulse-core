"""Transport-neutral KG readers and governed privacy/semantic operations.

F4 retires the historical backfill start/cancel use cases. Existing privacy
erasure and cognitive product operations retain their original authorization.
"""

from __future__ import annotations


from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
    require_authorization,
    resolve_actor_permissions,
)
from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
    EvaluateCodeTraceabilityKGReadAccessUseCase,
)
from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog


async def _require_board_access(
    services: ApplicationServiceCatalog,
    actor: ActorContext,
    board_id: str,
) -> None:
    """Fail closed unless the actor can see the target board through QueryScope."""
    actor_scope = ActorScope.from_context(actor)
    query_scope = actor_scope.query_scope(target_board_id=board_id)
    access_reader = getattr(services.boards, "get_board_access_record", None)
    if not callable(access_reader):
        # Compatibility for edition/test service implementations that have not
        # adopted the additive bounded access projection yet.
        access_reader = services.boards.get_board
    board = await access_reader(
        board_id,
        actor_scope.actor_id,
        query_scope=query_scope,
    )
    if board is None:
        raise PermissionDeniedError("Not authorized to access this board")


async def _visible_board_ids(
    services: ApplicationServiceCatalog,
    actor: ActorContext,
) -> list[str]:
    """Resolve the actor's global KG search scope using the board service."""
    actor_scope = ActorScope.from_context(actor)
    boards, _total = await services.boards.list_boards(
        actor_scope.actor_id,
        offset=0,
        limit=10_000_000,
        realm_id=actor_scope.realm_id,
        view="all",
        query_scope=actor_scope.query_scope(),
    )
    scoped = actor_scope.query_scope(
        allowed_board_ids=[board.id for board in boards],
        require_ownership=False,
    )
    return sorted(scoped.allowed_board_ids or ())


async def _authorized_global_query_board_ids(
    uow: PulseUnitOfWork,
    actor: ActorContext,
) -> list[str]:
    """Filter visible boards through the same compound policy used by MCP."""

    visible_board_ids = await _visible_board_ids(uow.services, actor)
    authorized: list[str] = []
    requirements = (
        PermissionRequirement("board.read", legacy_operation="board:read"),
        PermissionRequirement("kg.query.global", legacy_operation="board:read"),
    )
    for board_id in visible_board_ids:
        permissions = await resolve_actor_permissions(actor, uow, board_id)
        if all(
            decide_authorization(actor, requirement, permissions).allowed
            for requirement in requirements
        ):
            authorized.append(board_id)
    return authorized


# --- list audit (reader) ----------------------------------------------------


class ListAuditCommand:
    __slots__ = ("board_id", "limit")

    def __init__(self, board_id: str, *, limit: int) -> None:
        self.board_id = board_id
        self.limit = limit


class ListAuditResult:
    __slots__ = ("entries",)

    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self.entries = entries


class ListAuditUseCase:
    """List committed consolidation-audit entries for a board (read, no commit).
    Delegates the ``select`` + projection to ``dashboard_readers`` so the use case
    holds no relational symbol; the adapter wraps the entries in the legacy
    ``{"entries": ..., "next_cursor": None}`` envelope."""

    async def execute(
        self, command: ListAuditCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> ListAuditResult:

        await _require_board_access(uow.services, actor, command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.audit.read",
                legacy_operation="kg.admin.settings_read",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        ct_access = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
            actor=actor,
            board_id=command.board_id,
            uow=uow,
        )
        if ct_access.allowed:
            entries = await uow.services.kg.list_consolidation_audit(
                command.board_id,
                limit=command.limit,
            )
        else:
            entries = await uow.services.kg.list_consolidation_audit(
                command.board_id,
                limit=command.limit,
                include_code_traceability=False,
            )
        return ListAuditResult(entries)


# --- global search (reader + KG service) ------------------------------------


class GlobalSearchCommand:
    __slots__ = ("q", "limit", "min_similarity", "graph_layer")

    def __init__(
        self,
        *,
        q: str,
        limit: int,
        min_similarity: float,
        graph_layer: str,
    ) -> None:
        self.q = q
        self.limit = limit
        self.min_similarity = min_similarity
        self.graph_layer = graph_layer


class GlobalSearchResult:
    __slots__ = ("results", "graph_layer")

    def __init__(self, results: list[Any], graph_layer: str) -> None:
        self.results = results
        self.graph_layer = graph_layer


class GlobalSearchUseCase:
    """Cross-board global discovery search (read, no commit).

    Resolves the caller's board visibility through ``ActorScope``/``QueryScope``
    and ``BoardService`` before delegating to ``kg_service.query_global``. An
    unresolved scope is never treated as "all boards".
    """

    async def execute(
        self, command: GlobalSearchCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> GlobalSearchResult:
        from okto_pulse.core.services.application_kg import (
            normalize_graph_layer,
            query_global,
        )

        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.query.global",
                legacy_operation="board:read",
            ),
            uow=uow,
        )
        user_board_ids = await _authorized_global_query_board_ids(uow, actor)
        layer = normalize_graph_layer(command.graph_layer)
        results = query_global(
            command.q,
            user_boards=user_board_ids,
            top_k=command.limit,
            min_similarity=command.min_similarity,
            graph_layer=layer,
        )
        return GlobalSearchResult(results, layer)


# --- right to erasure (write) -----------------------------------------------


class DeleteBoardKgCommand:
    __slots__ = ("board_id",)

    def __init__(self, board_id: str) -> None:
        self.board_id = board_id


class DeleteBoardKgResult:
    __slots__ = ("counts",)

    def __init__(self, counts: dict[str, Any]) -> None:
        self.counts = counts


class DeleteBoardKgUseCase:
    """Wipe all KG data for a board — right-to-erasure (write). Delegates to
    ``governance.right_to_erasure`` (best-effort cascade + SQLite purge, commits
    internally) and returns its counts; the adapter still answers 204 No Content
    regardless of the counts, exactly as the legacy endpoint did."""

    async def execute(
        self,
        command: DeleteBoardKgCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> DeleteBoardKgResult:

        await _require_board_access(uow.services, actor, command.board_id)
        await require_authorization(
            actor,
            PermissionRequirement(
                "kg.operations.board.erase",
                legacy_operation="kg.admin.wipe_board",
            ),
            uow=uow,
            board_id=command.board_id,
        )
        # The standalone KG erasure endpoint is an administrative operation,
        # just like full Board deletion.  Hold the shared orchestration
        # reservation and exact board/global writer fences for the complete
        # physical purge so it cannot overlap a rebuild's writer-free drain.
        async with uow.services.kg.board_erasure_scope(
            command.board_id,
            actor_id=actor.actor_id,
        ) as erasure:
            erasure.ensure_owned()
            counts = await uow.services.kg.right_to_erasure(
                command.board_id,
                global_writer_guarded=True,
            )
            erasure.ensure_owned()
        return DeleteBoardKgResult(counts)
