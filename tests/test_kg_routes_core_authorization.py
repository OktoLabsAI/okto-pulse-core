"""Central authorization contracts for KG dashboard writer use cases."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.kg_routes_crud import (
    CancelHistoricalCommand,
    CancelHistoricalUseCase,
    DeleteBoardKgCommand,
    DeleteBoardKgUseCase,
    StartHistoricalCommand,
    StartHistoricalUseCase,
)
from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


BOARD_ID = "board-kg-auth"


class _Boards:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def get_board(
        self,
        board_id: str,
        actor_id: str,
        *,
        query_scope: object,
    ) -> object:
        assert actor_id == "actor-kg"
        assert getattr(query_scope, "target_board_id") == board_id
        self._events.append(f"lookup:{board_id}")
        return SimpleNamespace(id=board_id)


class _Kg:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start_historical_consolidation(self, board_id: str) -> dict[str, str]:
        self._events.append(f"writer:start:{board_id}")
        return {"status": "queueing"}

    async def cancel_historical(self, board_id: str) -> dict[str, str]:
        self._events.append(f"writer:cancel:{board_id}")
        return {"status": "cancelled"}

    async def right_to_erasure(self, board_id: str) -> dict[str, int]:
        self._events.append(f"writer:delete:{board_id}")
        return {"nodes": 0}


class _Services:
    def __init__(
        self,
        events: list[str],
        resolved_permissions: PermissionSet | dict[str, object] | None = None,
    ) -> None:
        self.boards = _Boards(events)
        self.kg = _Kg(events)
        self._events = events
        self._resolved_permissions = resolved_permissions

    async def resolve_user_permissions(
        self, actor_id: str, board_id: str
    ) -> PermissionSet | dict[str, object] | None:
        self._events.append(f"resolve:{actor_id}:{board_id}")
        return self._resolved_permissions


class _Uow:
    def __init__(
        self,
        events: list[str],
        resolved_permissions: PermissionSet | dict[str, object] | None = None,
    ) -> None:
        self.services = _Services(events, resolved_permissions)
        self.commit = AsyncMock(side_effect=lambda: events.append("commit"))


_WRITERS = (
    (
        StartHistoricalUseCase,
        StartHistoricalCommand,
        "kg.admin.historical_consolidation",
        "start",
    ),
    (
        CancelHistoricalUseCase,
        CancelHistoricalCommand,
        "kg.admin.historical_consolidation",
        "cancel",
    ),
    (DeleteBoardKgUseCase, DeleteBoardKgCommand, "kg.admin.wipe_board", "delete"),
)


def _permission_set(operation: str, *, allowed: bool) -> PermissionSet:
    leaf = operation.rsplit(".", 1)[-1]
    return PermissionSet(
        {
            "board": {"read": True},
            "kg": {"admin": {leaf: allowed}},
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case_type", "command_type", "operation", "writer_name"), _WRITERS
)
async def test_kg_writer_denial_happens_after_lookup_and_before_writer_or_commit(
    use_case_type: type,
    command_type: type,
    operation: str,
    writer_name: str,
) -> None:
    events: list[str] = []
    uow = _Uow(events)
    actor = ActorContext(
        "actor-kg",
        "rest",
        board_id=BOARD_ID,
        realm_id=LOCAL_REALM_ID,
        permissions=_permission_set(operation, allowed=False),
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        await use_case_type().execute(
            command_type(BOARD_ID),
            actor=actor,
            uow=uow,
        )

    denial = json.loads(str(exc_info.value))
    assert denial["required_permission"] == operation
    assert events == [f"lookup:{BOARD_ID}"]
    assert f"writer:{writer_name}:{BOARD_ID}" not in events
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case_type", "command_type", "operation", "writer_name"), _WRITERS
)
async def test_kg_writer_uses_rest_permissions_resolved_for_target_board(
    use_case_type: type,
    command_type: type,
    operation: str,
    writer_name: str,
) -> None:
    events: list[str] = []
    uow = _Uow(events, _permission_set(operation, allowed=True))
    actor = ActorContext(
        "actor-kg",
        "rest",
        realm_id=LOCAL_REALM_ID,
        permissions=None,
    )

    await use_case_type().execute(
        command_type(BOARD_ID),
        actor=actor,
        uow=uow,
    )

    assert events == [
        f"lookup:{BOARD_ID}",
        f"resolve:actor-kg:{BOARD_ID}",
        f"writer:{writer_name}:{BOARD_ID}",
    ]
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case_type", "command_type", "operation", "writer_name"), _WRITERS
)
async def test_kg_writer_rejects_cross_board_actor_before_writer(
    use_case_type: type,
    command_type: type,
    operation: str,
    writer_name: str,
) -> None:
    events: list[str] = []
    uow = _Uow(events)
    actor = ActorContext(
        "actor-kg",
        "mcp",
        board_id="board-other",
        realm_id=LOCAL_REALM_ID,
        permissions=_permission_set(operation, allowed=True),
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        await use_case_type().execute(
            command_type(BOARD_ID),
            actor=actor,
            uow=uow,
        )

    assert json.loads(str(exc_info.value))["reason"] == "board_scope_mismatch"
    assert events == [f"lookup:{BOARD_ID}"]
    assert f"writer:{writer_name}:{BOARD_ID}" not in events
    uow.commit.assert_not_awaited()
