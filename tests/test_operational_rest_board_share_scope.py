"""Shared-board reads must not silently grant operational mutations."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.operational_rest import (
    BoardNotFoundError,
    GetResourceGateSummaryUseCase,
    MarkResourceNotApplicableCommand,
    MarkResourceNotApplicableUseCase,
    ResourceGateEntityCommand,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID


@pytest.mark.asyncio
async def test_shared_resource_gate_read_does_not_grant_write() -> None:
    board_id = "shared-operational-board"
    board = SimpleNamespace(
        id=board_id,
        owner_id="owner",
        realm_id=LOCAL_REALM_ID,
    )
    resource_gate = SimpleNamespace(
        get_summary=AsyncMock(return_value={"ok": True}),
        mark_not_applicable=AsyncMock(),
    )
    services = SimpleNamespace(
        boards=SimpleNamespace(get_board=AsyncMock(return_value=None)),
        shares=SimpleNamespace(get_user_permission=AsyncMock(return_value="viewer")),
        specs=SimpleNamespace(
            get_spec=AsyncMock(
                return_value=SimpleNamespace(id="spec-id", board_id=board_id)
            )
        ),
        resource_gate=resource_gate,
    )
    uow = SimpleNamespace(
        services=services,
        boards=SimpleNamespace(get=AsyncMock(return_value=board)),
    )
    actor = ActorContext("viewer", "rest", realm_id=LOCAL_REALM_ID)

    read = await GetResourceGateSummaryUseCase().execute(
        ResourceGateEntityCommand(board_id, "spec", "spec-id"),
        actor=actor,
        uow=uow,
    )
    assert read.data == {"ok": True}

    with pytest.raises(BoardNotFoundError):
        await MarkResourceNotApplicableUseCase().execute(
            MarkResourceNotApplicableCommand(
                board_id,
                "spec",
                "spec-id",
                "architecture",
                "not applicable",
                "rest",
            ),
            actor=actor,
            uow=uow,
        )
    resource_gate.mark_not_applicable.assert_not_awaited()
