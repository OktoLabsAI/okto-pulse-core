"""Canonical authorization contracts for dedicated KG operational use cases."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases import (
    cognitive_readiness,
    kg_health,
    kg_routes_crud,
    list_cognitive_dlq,
    operational_rest,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.permissions import (
    KG_OPERATIONS_PERMISSION_INTRODUCTION_V1,
    PermissionSet,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


BOARD_ID = "board-kg-operations"


_NAMESPACE_REQUIREMENTS = (
    ("kg.operations.health.read", "kg.admin.settings_read"),
    ("kg.operations.cognitive.read", "kg.admin.settings_read"),
    ("kg.operations.cognitive.skip", "kg.admin.settings_write"),
    ("kg.operations.cognitive.clear", "kg.admin.settings_write"),
    ("kg.operations.queue.read", "kg.admin.settings_read"),
    ("kg.operations.queue.reprocess", "kg.admin.settings_write"),
    ("kg.operations.audit.read", "kg.admin.settings_read"),
    (
        "kg.operations.historical.read",
        "kg.admin.historical_consolidation",
    ),
    (
        "kg.operations.historical.start",
        "kg.admin.historical_consolidation",
    ),
    (
        "kg.operations.historical.cancel",
        "kg.admin.historical_consolidation",
    ),
    ("kg.operations.node.boost", "kg.admin.settings_write"),
    ("kg.operations.settings.read", "kg.admin.settings_read"),
    ("kg.operations.settings.write", "kg.admin.settings_write"),
    ("kg.operations.board.erase", "kg.admin.wipe_board"),
)


def _permission_set(operation: str, historical_authority: str) -> PermissionSet:
    document: dict[str, Any] = {}
    for path in (operation, historical_authority):
        cursor = document
        parts = path.split(".")
        for part in parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[parts[-1]] = True
    return PermissionSet(document)


def test_authorization_matrix_covers_the_complete_kg_operations_namespace() -> None:
    assert tuple(operation for operation, _legacy in _NAMESPACE_REQUIREMENTS) == (
        KG_OPERATIONS_PERMISSION_INTRODUCTION_V1.leaves
    )


@pytest.mark.parametrize(("operation", "historical_authority"), _NAMESPACE_REQUIREMENTS)
def test_each_kg_operation_declares_and_accepts_its_two_authorities(
    operation: str,
    historical_authority: str,
) -> None:
    assert (
        KG_OPERATIONS_PERMISSION_INTRODUCTION_V1.historical_authority_for(operation)
        == historical_authority
    )
    requirement = PermissionRequirement(
        operation,
        legacy_operation=historical_authority,
    )

    canonical = decide_authorization(
        ActorContext(
            "operator",
            "mcp",
            permissions=_permission_set(operation, historical_authority),
        ),
        requirement,
    )
    historical = decide_authorization(
        ActorContext("legacy-operator", "mcp", permissions=[historical_authority]),
        requirement,
    )
    canonical_without_historical_ceiling = decide_authorization(
        ActorContext(
            "partial-operator",
            "mcp",
            permissions=_permission_set(operation, "kg.admin.unrelated"),
        ),
        requirement,
    )

    assert canonical.allowed is True
    assert historical.allowed is True
    assert canonical_without_historical_ceiling.allowed is False


class _BoardService:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get_board(
        self,
        board_id: str,
        actor_id: str,
        *,
        query_scope: object,
    ) -> object:
        assert actor_id == "operator"
        assert getattr(query_scope, "target_board_id") == board_id
        self._events.append(f"lookup:{board_id}")
        return SimpleNamespace(id=board_id, owner_id=actor_id)


class _BoardRepository:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get(self, board_id: str) -> object:
        self._events.append(f"lookup:{board_id}")
        return SimpleNamespace(
            id=board_id,
            owner_id="operator",
            realm_id=LOCAL_REALM_ID,
        )


class _KgWriterSpy:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def _write(self, name: str) -> None:
        self._events.append(f"write:{name}")

    async def start_historical_consolidation(self, _board_id: str) -> dict[str, Any]:
        self._write("historical.start")
        return {}

    async def cancel_historical(self, _board_id: str) -> dict[str, Any]:
        self._write("historical.cancel")
        return {}

    @asynccontextmanager
    async def board_erasure_scope(self, _board_id: str, *, actor_id: str):
        del actor_id
        yield SimpleNamespace(ensure_owned=lambda: None)

    async def right_to_erasure(
        self,
        _board_id: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self._write("board.erase")
        return {}

    async def retry_pending_entry(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        self._write("queue.retry")
        return {}

    async def mutate_boost_node_graph(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("node.boost")
        return {}

    async def enqueue_digest_layer_reconciliation(
        self, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("integrity.reconcile")
        return {}

    async def reprocess_dead_letter_rows(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("queue.dead_letter_reprocess")
        return {}

    async def reprocess_connectivity_guard_dlq(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self._write("queue.connectivity_reprocess")
        return {}


class _Uow:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.boards = _BoardRepository(self.events)
        self.services = SimpleNamespace(
            boards=_BoardService(self.events),
            kg=_KgWriterSpy(self.events),
        )
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


_WRITE_CASES: tuple[
    tuple[
        Any,
        Callable[[], Any],
        str,
        str,
        bool,
    ],
    ...,
] = (
    (
        kg_routes_crud.StartHistoricalUseCase(),
        lambda: kg_routes_crud.StartHistoricalCommand(BOARD_ID),
        "kg.operations.historical.start",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud.CancelHistoricalUseCase(),
        lambda: kg_routes_crud.CancelHistoricalCommand(BOARD_ID),
        "kg.operations.historical.cancel",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud.DeleteBoardKgUseCase(),
        lambda: kg_routes_crud.DeleteBoardKgCommand(BOARD_ID),
        "kg.operations.board.erase",
        "kg.admin.wipe_board",
        True,
    ),
    (
        kg_routes_crud.RetryPendingEntryUseCase(),
        lambda: kg_routes_crud.RetryPendingEntryCommand(BOARD_ID, "queue-1"),
        "kg.operations.queue.reprocess",
        "kg.admin.settings_write",
        True,
    ),
    (
        kg_routes_crud.BoostNodeUseCase(),
        lambda: kg_routes_crud.BoostNodeCommand(BOARD_ID, "node-1"),
        "kg.operations.node.boost",
        "kg.admin.settings_write",
        True,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command_factory", "operation", "legacy", "expects_lookup"),
    _WRITE_CASES,
    ids=(
        "historical-start",
        "historical-cancel",
        "board-erase",
        "pending-retry",
        "node-boost",
    ),
)
async def test_each_dedicated_kg_writer_authorizes_after_lookup_and_before_write(
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    expects_lookup: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = kg_routes_crud
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs["board_id"] == BOARD_ID
    assert kwargs["uow"] is uow
    assert uow.events == ([f"lookup:{BOARD_ID}"] if expects_lookup else [])
    assert uow.commits == 0
    assert uow.rollbacks == 0


_REST_OPERATION_CASES: tuple[
    tuple[Any, Callable[[], Any], str, str],
    ...,
] = (
    (
        operational_rest.RecordCognitiveSkipUseCase(),
        lambda: operational_rest.CognitiveSkipCommand(
            BOARD_ID,
            "spec:1",
            "operator_decision",
            None,
            None,
            None,
            None,
        ),
        "kg.operations.cognitive.skip",
        "kg.admin.settings_write",
    ),
    (
        operational_rest.ClearCognitiveSkipUseCase(),
        lambda: operational_rest.CognitiveClearCommand(BOARD_ID, "spec:1", None),
        "kg.operations.cognitive.clear",
        "kg.admin.settings_write",
    ),
    (
        operational_rest.GetCognitiveReadinessMetricsUseCase(),
        lambda: operational_rest.CognitiveReadinessMetricsCommand(BOARD_ID, None),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
    ),
    (
        operational_rest.GetCognitiveEffectivenessInventoryUseCase(),
        lambda: operational_rest.CognitiveEffectivenessInventoryCommand(
            BOARD_ID,
            None,
            False,
            "canonical",
            None,
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command_factory", "operation", "legacy"),
    _REST_OPERATION_CASES,
    ids=(
        "cognitive-skip",
        "cognitive-clear",
        "cognitive-metrics",
        "cognitive-inventory",
    ),
)
async def test_operational_rest_authorizes_before_any_kg_service_call(
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(operational_rest, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "rest",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs == {"uow": uow, "board_id": BOARD_ID}
    assert uow.events == [f"lookup:{BOARD_ID}"]
    assert uow.commits == 0
    assert uow.rollbacks == 0


_READ_CASES: tuple[
    tuple[Any, Any, Callable[[], Any], str, str, bool],
    ...,
] = (
    (
        kg_health,
        kg_health.GetKgHealthUseCase(),
        lambda: kg_health.GetKgHealthCommand(BOARD_ID),
        "kg.operations.health.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_health,
        kg_health.GetKgHealthReadinessUseCase(),
        lambda: kg_health.GetKgHealthReadinessCommand(BOARD_ID),
        "kg.operations.health.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.EvaluateBugCognitiveClosureUseCase(),
        lambda: cognitive_readiness.EvaluateBugCognitiveClosureCommand(
            BOARD_ID,
            "bug-1",
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.ListCognitiveReadinessItemsUseCase(),
        lambda: cognitive_readiness.ListCognitiveReadinessItemsCommand(BOARD_ID),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        cognitive_readiness,
        cognitive_readiness.EvaluateCognitiveReadinessUseCase(),
        lambda: cognitive_readiness.EvaluateCognitiveReadinessCommand(
            BOARD_ID,
            source_ref="spec:1",
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListAuditUseCase(),
        lambda: kg_routes_crud.ListAuditCommand(BOARD_ID, limit=50),
        "kg.operations.audit.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.GetHistoricalProgressUseCase(),
        lambda: kg_routes_crud.GetHistoricalProgressCommand(BOARD_ID),
        "kg.operations.historical.read",
        "kg.admin.historical_consolidation",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListPendingUseCase(),
        lambda: kg_routes_crud.ListPendingCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        kg_routes_crud,
        kg_routes_crud.ListPendingTreeUseCase(),
        lambda: kg_routes_crud.ListPendingTreeCommand(BOARD_ID),
        "kg.operations.queue.read",
        "kg.admin.settings_read",
        True,
    ),
    (
        list_cognitive_dlq,
        list_cognitive_dlq.ListCognitiveDlqUseCase(),
        lambda: list_cognitive_dlq.ListCognitiveDlqCommand(
            BOARD_ID,
            limit=50,
            offset=0,
        ),
        "kg.operations.cognitive.read",
        "kg.admin.settings_read",
        False,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "module",
        "use_case",
        "command_factory",
        "operation",
        "legacy",
        "expects_lookup",
    ),
    _READ_CASES,
    ids=(
        "health",
        "health-readiness",
        "bug-cognitive",
        "cognitive-list",
        "cognitive-evaluate",
        "audit-list",
        "historical-progress",
        "pending-list",
        "pending-tree",
        "cognitive-dlq",
    ),
)
async def test_each_dedicated_kg_reader_checks_the_specific_operation(
    module: Any,
    use_case: Any,
    command_factory: Callable[[], Any],
    operation: str,
    legacy: str,
    expects_lookup: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[PermissionRequirement, dict[str, Any]]] = []

    async def _deny(
        _actor: ActorContext,
        requirement: PermissionRequirement,
        **kwargs: Any,
    ) -> None:
        captured.append((requirement, kwargs))
        raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", _deny)
    uow = _Uow()
    actor = ActorContext(
        "operator",
        "mcp",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError, match="denied"):
        await use_case.execute(command_factory(), actor=actor, uow=uow)

    requirement, kwargs = captured[0]
    assert requirement == PermissionRequirement(operation, legacy_operation=legacy)
    assert kwargs["board_id"] == BOARD_ID
    assert kwargs["uow"] is uow
    assert uow.events == ([f"lookup:{BOARD_ID}"] if expects_lookup else [])
    assert uow.commits == 0
