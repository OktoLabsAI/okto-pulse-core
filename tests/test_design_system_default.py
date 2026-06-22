"""Spec 3a006f65 / card 96f76a5f — Design System default via the umbrella +
BoardSettings.design_system_gate_mode (FR3/FR4/FR5, AC3/AC4/AC5), scenarios
ts_53be91fd + ts_a52e5ee5.

The Design System default is a consumer of DefaultBoardConfigurationService (no
parallel store): the IDENTITY lives in design_system_default_ref, the CANONICAL gate
mode in settings_payload.design_system_gate_mode (Q1=B; the gate_mode mirrored inside
the ref is never a second authoritative source). create_board materializes a REAL
BoardDesignSystem link + the effective gate mode in the same transaction; setting a
new default is forward-only (existing boards unchanged).

create_board materializes from scope='global'; create_board tests use a single
session WITHOUT commit (rolled back at close — gotcha ts_cdb70cc0). The MCP-twin test
commits its seed under a UNIQUE template scope and cleans it up in a finally.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_design_system_default.py
"""

from __future__ import annotations

import json
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import delete

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    Board,
    BoardDesignSystem,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    DesignSystem,
)
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_config_api import DefaultBoardConfigApiService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.design_system import DesignSystemService
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "ds-default-user"


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


async def _global_ds(db, title: str) -> DesignSystem:
    return await DesignSystemService(db).create_design_system(USER_ID, title=title, scope="global")


async def _active_global_template(db):
    return await DefaultBoardConfigurationService(db).create_version(
        settings_payload=BoardSettings(), actor=USER_ID, scope="global", activate=True
    )


async def _board(db, name: str | None = None):
    return await BoardService(db).create_board(
        USER_ID, BoardCreate(name=name or f"b-{uuid.uuid4().hex[:8]}")
    )


# ---------------------------------------------------------------------------
# ts_a52e5ee5 — BoardSettings gate modes + legacy compatibility (unit)
# ---------------------------------------------------------------------------


async def test_ts_a52e5ee5_boardsettings_gate_modes_and_legacy():
    for mode in ("off", "advisory", "blocking"):
        assert BoardSettings(design_system_gate_mode=mode).design_system_gate_mode == mode
    # legacy board data without the field validates with the compatible default 'off'.
    legacy = BoardSettings.model_validate({"max_scenarios_per_card": 3})
    assert legacy.design_system_gate_mode == "off"
    # an invalid explicit mode is rejected.
    with pytest.raises(ValidationError):
        BoardSettings(design_system_gate_mode="loud")


# ---------------------------------------------------------------------------
# ts_53be91fd — default applied through the umbrella; forward-only
# ---------------------------------------------------------------------------


async def test_ts_53be91fd_default_applied_via_umbrella_and_forward_only():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        cfg = DefaultBoardConfigApiService(db)
        ds_svc = DesignSystemService(db)
        gds1 = await _global_ds(db, "DS1")
        gds2 = await _global_ds(db, "DS2")
        v1 = await _active_global_template(db)

        # set DS default = gds1 with gate advisory (copy-on-write new active version).
        await cfg.set_template_design_system(
            template_id=v1.id, design_system_id=gds1.id, actor=USER_ID,
            version=gds1.version, gate_mode="advisory",
        )

        # a new board gets the REAL effective link + the canonical gate mode + snapshot.
        board_a = await _board(db)
        eff_a = await ds_svc.get_board_effective_design_system(board_a.id)
        assert eff_a["source"] == "board_link" and eff_a["design_system_id"] == gds1.id
        assert board_a.settings["design_system_gate_mode"] == "advisory"
        assert board_a.default_config_snapshot["design_system"]["design_system_id"] == gds1.id

        # change the default to gds2/blocking for FUTURE boards.
        active = (await cfg.get_active())["active"]
        await cfg.set_template_design_system(
            template_id=active["id"], design_system_id=gds2.id, actor=USER_ID,
            version=gds2.version, gate_mode="blocking",
        )

        # board A is UNCHANGED (forward-only — no live inheritance).
        eff_a2 = await ds_svc.get_board_effective_design_system(board_a.id)
        assert eff_a2["design_system_id"] == gds1.id
        assert board_a.settings["design_system_gate_mode"] == "advisory"

        # a board created AFTER the change receives gds2/blocking.
        board_b = await _board(db)
        eff_b = await ds_svc.get_board_effective_design_system(board_b.id)
        assert eff_b["design_system_id"] == gds2.id
        assert board_b.settings["design_system_gate_mode"] == "blocking"


# ---------------------------------------------------------------------------
# set-default: canonical gate + no divergence + copy-on-write + fail-closed
# ---------------------------------------------------------------------------


async def test_set_default_sets_canonical_gate_with_no_divergence():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        cfg = DefaultBoardConfigApiService(db)
        svc = DefaultBoardConfigurationService(db)
        ds = await _global_ds(db, "DS")
        v1 = await _active_global_template(db)

        result = await cfg.set_template_design_system(
            template_id=v1.id, design_system_id=ds.id, actor=USER_ID,
            version=ds.version, gate_mode="blocking",
        )
        # copy-on-write: a NEW active version.
        assert result["id"] != v1.id and result["is_active"] is True
        # CANONICAL gate mode lives in settings_payload.
        assert result["settings_payload"]["design_system_gate_mode"] == "blocking"
        # the ref carries identity; its mirrored gate_mode matches the canonical (no divergence).
        assert result["design_system_default_ref"]["design_system_id"] == ds.id
        assert result["design_system_default_ref"]["gate_mode"] == "blocking"
        # the prior version is intact (forward-only).
        assert (await svc._require(v1.id)).design_system_default_ref is None


async def test_set_default_rejects_inline_and_invalid_gate():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        cfg = DefaultBoardConfigApiService(db)
        board = await _board(db)
        inline = await DesignSystemService(db).create_design_system(
            USER_ID, title="Inline", scope="inline", board_id=board.id
        )
        gds = await _global_ds(db, "G")
        v1 = await _active_global_template(db)

        # inline can never be a global default.
        with pytest.raises(DefaultBoardConfigurationError) as e1:
            await cfg.set_template_design_system(
                template_id=v1.id, design_system_id=inline.id, actor=USER_ID, gate_mode="advisory"
            )
        assert e1.value.code == "design_system_default_not_global"

        # an invalid gate mode is rejected (before the ref check).
        with pytest.raises(DefaultBoardConfigurationError) as e2:
            await cfg.set_template_design_system(
                template_id=v1.id, design_system_id=gds.id, actor=USER_ID, gate_mode="loud"
            )
        assert e2.value.code == "design_system_gate_mode_invalid"


# ---------------------------------------------------------------------------
# MCP twin — shared orchestrator
# ---------------------------------------------------------------------------


async def test_mcp_twin_set_default_design_system():
    from okto_pulse.core.infra.database import get_session_factory

    scope = f"t-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        board = await _board(db)
        ds = await _global_ds(db, "MCP DS")
        v1 = await DefaultBoardConfigurationService(db).create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=True
        )
        await db.commit()
        ids = {"board": board.id, "v1": v1.id, "ds": ds.id, "dsver": ds.version}

    try:
        res = await _call(
            "okto_pulse_set_default_design_system",
            board_id=ids["board"], template_id=ids["v1"],
            design_system_id=ids["ds"], gate_mode="advisory", version=ids["dsver"],
        )
        assert res["settings_payload"]["design_system_gate_mode"] == "advisory"
        assert res["design_system_default_ref"]["design_system_id"] == ids["ds"]
        assert res["is_active"] is True

        # structured error twin (never a raw exception).
        err = await _call(
            "okto_pulse_set_default_design_system",
            board_id=ids["board"], template_id=res["id"],
            design_system_id="missing", gate_mode="off",
        )
        assert err["code"] == "design_system_not_found"
    finally:
        async with get_session_factory()() as db:
            await db.execute(
                delete(DefaultBoardConfigurationAudit).where(
                    DefaultBoardConfigurationAudit.scope == scope
                )
            )
            await db.execute(
                delete(DefaultBoardConfiguration).where(DefaultBoardConfiguration.scope == scope)
            )
            await db.execute(delete(BoardDesignSystem).where(BoardDesignSystem.board_id == ids["board"]))
            await db.execute(delete(DesignSystem).where(DesignSystem.owner_id == USER_ID))
            await db.execute(delete(Board).where(Board.id == ids["board"]))
            await db.commit()
