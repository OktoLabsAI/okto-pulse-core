"""Spec 9df814bc / card 7da43521 — administrative API + MCP for default board
configuration (FR7), scenario ts_dc86816b.

REST orchestrator (DefaultBoardConfigApiService) + the MCP twin tools expose the
active template, version history, and the board default-config field-level diff,
with structured errors — sharing the umbrella DefaultBoardConfigurationService.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_config_api.py
"""

from __future__ import annotations

import json
import uuid

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_config_api import DefaultBoardConfigApiService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-api-user"


class _Ctx:
    agent_id = USER_ID
    permissions: list = []


async def _call(name: str, **kwargs) -> dict:
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _seed_versions_and_board(db):
    """v1 active -> board created under v1 with an override -> v2 active."""
    svc = DefaultBoardConfigurationService(db)
    v1 = await svc.create_version(
        settings_payload=BoardSettings(max_scenarios_per_card=3), actor=USER_ID, activate=True
    )
    board = await BoardService(db).create_board(
        USER_ID,
        BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}", settings=BoardSettings(max_scenarios_per_card=8)),
    )
    v2 = await svc.create_version(
        settings_payload=BoardSettings(max_scenarios_per_card=5), actor=USER_ID, activate=True
    )
    return v1, v2, board


async def test_ts_dc86816b_admin_surface_active_history_and_diff():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        v1, v2, board = await _seed_versions_and_board(db)
        api = DefaultBoardConfigApiService(db)

        active = await api.get_active()
        assert active["active"]["id"] == v2.id
        assert active["active"]["version"] == v2.version
        assert active["active"]["status"] == "active"

        versions = await api.list_versions()
        assert versions["active_id"] == v2.id
        assert {v["version"] for v in versions["versions"]} == {1, 2}

        diff = await api.get_board_diff(board_id=board.id)
        assert diff["snapshot_state"] == "applied"
        assert diff["applied_template_version"] == v1.version  # board applied v1
        assert diff["active_template_version"] == v2.version
        assert diff["is_outdated"] is True
        fields = {f["field"]: (f["template_value"], f["current_value"]) for f in diff["fields"]}
        # the override (8) vs the applied template value (3) is a field-level diff.
        assert fields["max_scenarios_per_card"] == (3, 8)
        assert all(f["state"] == "overridden" for f in diff["fields"])

        # structured error for a missing/inaccessible board.
        with pytest.raises(DefaultBoardConfigurationError) as exc:
            await api.get_board_diff(board_id="does-not-exist")
        assert exc.value.code == "board_not_found"


async def test_get_active_none_when_no_template():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        active = await DefaultBoardConfigApiService(db).get_active()
        assert active == {"scope": "global", "active": None}


async def test_diff_legacy_board_reports_no_snapshot():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        # board created with no active template -> no snapshot (legacy-like).
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        diff = await DefaultBoardConfigApiService(db).get_board_diff(board_id=board.id)
        assert diff["snapshot_state"] == "legacy_no_snapshot"
        assert diff["fields"] == []


async def test_mcp_twin_active_versions_and_diff():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        v1, v2, board = await _seed_versions_and_board(db)
        await db.commit()
        ids = {"board": board.id, "v1": v1.version, "v2": v2.version}

    active = await _call("okto_pulse_get_active_default_board_config", board_id=ids["board"])
    assert active["active"]["version"] == ids["v2"]

    versions = await _call("okto_pulse_list_default_board_config_versions", board_id=ids["board"])
    assert {v["version"] for v in versions["versions"]} == {ids["v1"], ids["v2"]}

    diff = await _call("okto_pulse_get_board_default_config_diff", board_id=ids["board"])
    assert diff["snapshot_state"] == "applied"
    assert diff["applied_template_version"] == ids["v1"]

    # structured error twin (board not found), never a raw exception.
    err = await _call("okto_pulse_get_board_default_config_diff", board_id="does-not-exist")
    assert err["code"] == "board_not_found"


async def test_mcp_twin_create_and_activate_version():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        await db.commit()
        board_id = board.id

    created = await _call(
        "okto_pulse_create_default_board_config_version",
        board_id=board_id,
        settings_payload={"max_scenarios_per_card": 7},
        activate=True,
    )
    assert created["status"] == "active" and created["is_active"] is True
    assert created["settings_payload"]["max_scenarios_per_card"] == 7

    deactivated = await _call(
        "okto_pulse_deactivate_default_board_config_version",
        board_id=board_id, template_id=created["id"],
    )
    assert deactivated["is_active"] is False and deactivated["status"] == "inactive"
