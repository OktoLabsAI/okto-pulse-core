"""Authorization precedes every candidate body read; no mutation or approval."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.architecture_candidates import (
    GetArchitectureCandidatesCommand, GetArchitectureCandidatesUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.domain.architecture_candidates import project_architecture_candidates


def context(*, owner="actor", share=None, source="rest", permissions=None, actor_board="board"):
    events = []

    async def snapshot():
        events.append("snapshot")

    async def get_spec(*, entity, record_id, includes):
        assert entity == "spec" and record_id == "spec" and includes == ()
        assert events == ["snapshot"]
        events.append("spec")
        return SimpleNamespace(id="spec", board_id="board", version=4, edition=2)

    population = project_architecture_candidates(
        board_id="board", spec_id="spec", spec_edition=2, designs=(), source_complete=True,
    )
    uow = SimpleNamespace(
        begin_consistent_read=snapshot,
        boards=SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(owner_id=owner, realm_id="local"))),
        services=SimpleNamespace(
            get_application_record=get_spec,
            shares=SimpleNamespace(get_share_permission=AsyncMock(return_value=share)),
            resolve_user_permissions=AsyncMock(return_value={}),
            load_spec_architecture_candidates=AsyncMock(return_value=population),
        ),
        commit=AsyncMock(), rollback=AsyncMock(),
    )
    actor = ActorContext("actor", source, board_id=actor_board, permissions=permissions if permissions is not None else {
        "spec": {"entity": {"read": True}, "architecture": {"read": True}},
    })
    return uow, actor


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["rest", "mcp"])
async def test_authorized_read_is_complete_empty_without_claiming_approval(source):
    uow, actor = context(source=source)
    result = await GetArchitectureCandidatesUseCase().execute(
        GetArchitectureCandidatesCommand("board", "spec"), actor=actor, uow=uow,
    )
    assert result["population_state"] == "complete" and result["total"] == 0
    assert result["spec_version"] == 4 and result["spec_edition"] == 2
    assert "approved" not in result and "ready" not in result
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_population_does_not_publish_a_zero_total():
    uow, actor = context()
    uow.services.load_spec_architecture_candidates.return_value = project_architecture_candidates(
        board_id="board", spec_id="spec", spec_edition=2, designs=(), source_complete=False,
    )
    result = await GetArchitectureCandidatesUseCase().execute(
        GetArchitectureCandidatesCommand("board", "spec"), actor=actor, uow=uow,
    )
    assert result["population_state"] == "unavailable" and result["total"] is None
    assert result["candidates"] == []
    assert result["issues"][0]["code"] == "architecture_sources_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [
    [], {"spec": {"entity": {"read": True}, "architecture": {"read": False}}},
    {"spec": {"entity": {"read": False}, "architecture": {"read": True}}},
])
async def test_missing_either_read_permission_never_loads_contracts(permissions):
    uow, actor = context(permissions=permissions)
    with pytest.raises(PermissionDeniedError):
        await GetArchitectureCandidatesUseCase().execute(
            GetArchitectureCandidatesCommand("board", "spec"), actor=actor, uow=uow,
        )
    uow.services.load_spec_architecture_candidates.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_rest_permission_resolution_is_not_trusted_access():
    uow, actor = context()
    actor.permissions = None
    uow.services.resolve_user_permissions.return_value = None
    with pytest.raises(PermissionDeniedError):
        await GetArchitectureCandidatesUseCase().execute(
            GetArchitectureCandidatesCommand("board", "spec"), actor=actor, uow=uow,
        )
    uow.services.load_spec_architecture_candidates.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("options,board_id", [
    ({"owner": "someone-else"}, "board"), ({}, "other-board"),
    ({"source": "mcp", "actor_board": "other-board"}, "board"),
])
async def test_denied_board_never_loads_contracts(options, board_id):
    uow, actor = context(**options)
    with pytest.raises(EntityNotFoundError):
        await GetArchitectureCandidatesUseCase().execute(
            GetArchitectureCandidatesCommand(board_id, "spec"), actor=actor, uow=uow,
        )
    uow.services.load_spec_architecture_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_mcp_reader_uses_the_same_authorization_and_envelope(monkeypatch):
    from okto_pulse.core.mcp import server

    uow, actor = context(source="mcp")
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=SimpleNamespace(
        agent_id="actor", agent_name="Actor", permissions=actor.permissions,
    )))

    @asynccontextmanager
    async def factory(**kwargs):
        yield uow

    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: factory)
    result = json.loads(await server.okto_pulse_list_architecture_candidates.fn(board_id="board", spec_id="spec"))
    assert result["success"] and result["total"] == 0
    assert result["contract_version"] == "architecture-candidates/v1"
    uow.commit.assert_not_awaited()
