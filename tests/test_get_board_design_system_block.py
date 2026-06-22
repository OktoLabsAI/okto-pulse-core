"""Spec 24f2e786 / card d0f6b62b — the okto_pulse_get_board overview surfaces the board's
EFFECTIVE Design System + canonical gate mode + mandate (FR1-FR4), so an agent learns the
mockup mandate from the board summary instead of only by being rejected at the gate.

Scenarios:
* ts_ff3cef78 (TS1): board_link DS + gate blocking -> effective filled + gate_mode=blocking
  + mandate=true;
* ts_0fd51a8b (TS2): DS + gate off -> effective filled + mandate=false;
* ts_18089a6a (TS3): no effective DS -> effective=null + mandate=false;
* ts_4288636b (TS4 / TEETH): source=default_snapshot with BoardSettings.gate_mode='advisory'
  DIFFERENT from the snapshot's gate_mode -> effective.title=None (normalized) +
  effective.source='default_snapshot' + gate_mode='advisory' (read ONLY from BoardSettings,
  never the snapshot mirror) + mandate=false.

The block is read-only — it never touches the MockupDesignSystemGate (FR6). The MCP-twin
test commits its seed (the tool opens its own session) and cleans it up in a finally so
nothing leaks (gotcha ts_cdb70cc0).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_get_board_design_system_block.py
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import delete
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, BoardDesignSystem, DesignSystem
from okto_pulse.core.models.schemas import BoardCreate
from okto_pulse.core.services.design_system import DesignSystemService
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "ds-overview-user"


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


async def _cleanup(board_ids: list[str], ds_ids: list[str]) -> None:
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        for bid in board_ids:
            await db.execute(delete(BoardDesignSystem).where(BoardDesignSystem.board_id == bid))
            await db.execute(delete(Board).where(Board.id == bid))
        for dsid in ds_ids:
            await db.execute(delete(DesignSystem).where(DesignSystem.id == dsid))
        await db.commit()


async def _seed_board(db, *, gate_mode: str) -> Board:
    board = await BoardService(db).create_board(
        USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
    )
    board.settings = {**(board.settings or {}), "design_system_gate_mode": gate_mode}
    flag_modified(board, "settings")
    return board


async def test_ts_ff3cef78_board_link_blocking_mandate_true():
    from okto_pulse.core.infra.database import get_session_factory

    board_id = ds_id = None
    try:
        async with get_session_factory()() as db:
            board = await _seed_board(db, gate_mode="blocking")
            board_id = board.id
            ds = await DesignSystemService(db).create_design_system(
                USER_ID, title="Teste DS", scope="global", payload={"tokens": {}}
            )
            ds_id = ds.id
            await DesignSystemService(db).link_design_system_to_board(board_id, ds_id)
            await db.commit()

        out = await _call("okto_pulse_get_board", board_id=board_id)
        block = out["design_system"]
        assert block["effective"] is not None
        assert block["effective"]["design_system_id"] == ds_id
        assert block["effective"]["title"] == "Teste DS"
        assert block["effective"]["source"] == "board_link"
        assert block["effective"]["version"] is not None
        assert block["gate_mode"] == "blocking"
        assert block["mandate"] is True
    finally:
        await _cleanup([board_id] if board_id else [], [ds_id] if ds_id else [])


async def test_ts_0fd51a8b_board_link_off_mandate_false():
    from okto_pulse.core.infra.database import get_session_factory

    board_id = ds_id = None
    try:
        async with get_session_factory()() as db:
            board = await _seed_board(db, gate_mode="off")
            board_id = board.id
            ds = await DesignSystemService(db).create_design_system(
                USER_ID, title="Teste DS", scope="global", payload={"tokens": {}}
            )
            ds_id = ds.id
            await DesignSystemService(db).link_design_system_to_board(board_id, ds_id)
            await db.commit()

        out = await _call("okto_pulse_get_board", board_id=board_id)
        block = out["design_system"]
        assert block["effective"] is not None
        assert block["effective"]["design_system_id"] == ds_id
        assert block["gate_mode"] == "off"
        assert block["mandate"] is False
    finally:
        await _cleanup([board_id] if board_id else [], [ds_id] if ds_id else [])


async def test_ts_18089a6a_no_effective_ds_effective_null_mandate_false():
    from okto_pulse.core.infra.database import get_session_factory

    board_id = None
    try:
        async with get_session_factory()() as db:
            board = await _seed_board(db, gate_mode="blocking")
            board_id = board.id
            # explicit: no board link, no design_system in default_config_snapshot.
            board.default_config_snapshot = None
            flag_modified(board, "default_config_snapshot")
            await db.commit()

        out = await _call("okto_pulse_get_board", board_id=board_id)
        block = out["design_system"]
        assert block["effective"] is None
        # mandate is false even under blocking when there is no effective Design System.
        assert block["mandate"] is False
    finally:
        await _cleanup([board_id] if board_id else [], [])


async def test_ts_4288636b_default_snapshot_normalizes_title_null_and_gate_from_settings():
    """TEETH: source=default_snapshot carries gate_mode but NOT title; the block must
    normalize title=None AND read gate_mode ONLY from BoardSettings (advisory), never the
    snapshot's own gate_mode (blocking)."""
    from okto_pulse.core.infra.database import get_session_factory

    board_id = ds_id = None
    try:
        async with get_session_factory()() as db:
            board = await _seed_board(db, gate_mode="advisory")
            board_id = board.id
            ds = await DesignSystemService(db).create_design_system(
                USER_ID, title="Snapshot DS", scope="global", payload={"tokens": {}}
            )
            ds_id = ds.id
            # NO board link — only a default_config_snapshot whose gate_mode (blocking)
            # deliberately DIFFERS from BoardSettings.design_system_gate_mode (advisory).
            board.default_config_snapshot = {
                "design_system": {
                    "design_system_id": ds_id,
                    "version": 1,
                    "gate_mode": "blocking",
                }
            }
            flag_modified(board, "default_config_snapshot")
            await db.commit()

        out = await _call("okto_pulse_get_board", board_id=board_id)
        block = out["design_system"]
        assert block["effective"] is not None
        assert block["effective"]["source"] == "default_snapshot"
        assert block["effective"]["design_system_id"] == ds_id
        assert block["effective"]["title"] is None  # normalized (snapshot has no title)
        assert block["gate_mode"] == "advisory"  # from BoardSettings, NOT the snapshot mirror
        assert block["gate_mode"] != "blocking"
        assert block["mandate"] is False  # advisory is not blocking
    finally:
        await _cleanup([board_id] if board_id else [], [ds_id] if ds_id else [])
