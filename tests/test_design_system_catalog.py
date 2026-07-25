"""Spec 3a006f65 / card 1392f59d — Design System catalog + board link (FR1/FR2),
scenario ts_1054bf42, plus the umbrella default-ref entity validation (Q3).

Scope of this card: the DesignSystem entity (global catalog + board-inline,
versioned), the singular board<->design-system link + effective read, and the
umbrella `_validate_design_system_default_ref` enrichment that rejects a synthetic /
inline / non-active default fail-closed. The default-set endpoint + gate_mode (card
#2) and the mockup gate (card #3) are NOT here.

Catalog rows of inline scope require board_id; create_board flows use scope='global'.
Service/orchestrator tests use a single session WITHOUT commit (rolled back at close
— gotcha ts_cdb70cc0); the MCP-twin test commits its seed and cleans it up in a
finally so nothing leaks.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_design_system_catalog.py
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid

import pytest
from sqlalchemy import delete, select

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, BoardDesignSystem, DesignSystem
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.design_system import DesignSystemError, DesignSystemService
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "ds-catalog-user"


class _Ctx:
    agent_id = USER_ID
    permissions: list = []


async def _call(name: str, **kwargs) -> dict:
    from unittest.mock import AsyncMock, patch

    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


async def _board(db, name: str | None = None):
    return await BoardService(db).create_board(
        USER_ID, BoardCreate(name=name or f"b-{uuid.uuid4().hex[:8]}")
    )


# ---------------------------------------------------------------------------
# ts_1054bf42 — global + inline catalog
# ---------------------------------------------------------------------------


async def test_ts_1054bf42_catalog_supports_global_and_inline():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DesignSystemService(db)
        # a global DesignSystem is versioned (v1) and listed in the catalog.
        g = await svc.create_design_system(
            USER_ID, title="Aaa DS", scope="global", payload={"tokens": {"c": 1}}
        )
        assert g.scope == "global" and g.board_id is None and g.version == 1
        assert g.id in {d.id for d in await svc.list_catalog(scope="global")}

        # inline WITHOUT board_id fails.
        with pytest.raises(DesignSystemError) as e1:
            await svc.create_design_system(USER_ID, title="Inline", scope="inline")
        assert e1.value.code == "design_system_inline_requires_board"

        # inline WITH board_id is board-scoped and NOT in the global catalog.
        board = await _board(db)
        inline = await svc.create_design_system(
            USER_ID, title="Inline", scope="inline", board_id=board.id
        )
        assert inline.scope == "inline" and inline.board_id == board.id
        assert inline.id not in {d.id for d in await svc.list_catalog(scope="global")}
        assert inline.id in {d.id for d in await svc.list_catalog(scope="inline", board_id=board.id)}

        # inline can NOT be selected as a global default (umbrella rejects it).
        cfg = DefaultBoardConfigurationService(db)
        with pytest.raises(DefaultBoardConfigurationError) as e2:
            await cfg.create_version(
                settings_payload=BoardSettings(), actor=USER_ID, activate=True,
                design_system_default_ref={"design_system_id": inline.id, "gate_mode": "advisory"},
            )
        assert e2.value.code == "design_system_default_not_global"


# ---------------------------------------------------------------------------
# version lifecycle
# ---------------------------------------------------------------------------


async def test_update_bumps_version_on_title_or_payload_change():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DesignSystemService(db)
        ds = await svc.create_design_system(USER_ID, title="DS", scope="global", payload={"a": 1})
        assert ds.version == 1
        assert (await svc.update_design_system(ds.id, USER_ID, payload={"a": 2})).version == 2
        # a no-op payload update does NOT bump.
        assert (await svc.update_design_system(ds.id, USER_ID, payload={"a": 2})).version == 2
        # a title change bumps.
        assert (await svc.update_design_system(ds.id, USER_ID, title="DS2")).version == 3
        # status-only change does NOT bump version.
        assert (await svc.update_design_system(ds.id, USER_ID, status="archived")).version == 3


# ---------------------------------------------------------------------------
# Q3 — umbrella default-ref entity validation (real, global, active)
# ---------------------------------------------------------------------------


async def test_umbrella_default_ref_requires_real_global_active_design_system():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        cfg = DefaultBoardConfigurationService(db)
        svc = DesignSystemService(db)

        # missing design_system_id -> design_system_not_found.
        with pytest.raises(DefaultBoardConfigurationError) as e1:
            await cfg.create_version(
                settings_payload=BoardSettings(), actor=USER_ID, activate=True,
                design_system_default_ref={"design_system_id": "nope", "gate_mode": "off"},
            )
        assert e1.value.code == "design_system_not_found"

        # a non-active (draft) global DS -> design_system_default_not_active.
        draft = await svc.create_design_system(
            USER_ID, title="Draft DS", scope="global", status="draft"
        )
        with pytest.raises(DefaultBoardConfigurationError) as e2:
            await cfg.create_version(
                settings_payload=BoardSettings(), actor=USER_ID, activate=True,
                design_system_default_ref={"design_system_id": draft.id, "gate_mode": "off"},
            )
        assert e2.value.code == "design_system_default_not_active"

        # a real, global, active DS -> activation succeeds.
        ok = await svc.create_design_system(USER_ID, title="Active DS", scope="global")
        tmpl = await cfg.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, activate=True,
            design_system_default_ref={
                "design_system_id": ok.id, "version": ok.version, "gate_mode": "advisory"
            },
        )
        assert tmpl.is_active is True


# ---------------------------------------------------------------------------
# board link (singular) + effective read
# ---------------------------------------------------------------------------


async def test_board_link_is_singular_and_effective_read_is_real():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        svc = DesignSystemService(db)
        board = await _board(db)
        g1 = await svc.create_design_system(USER_ID, title="DS1", scope="global")
        g2 = await svc.create_design_system(USER_ID, title="DS2", scope="global")

        link1 = await svc.link_design_system_to_board(board.id, g1.id)
        assert link1.design_system_id == g1.id and link1.design_system_version == g1.version
        eff = await svc.get_board_effective_design_system(board.id)
        assert eff["source"] == "board_link" and eff["design_system_id"] == g1.id

        # re-link to g2 is an upsert: still exactly ONE row (singular per board).
        await svc.link_design_system_to_board(board.id, g2.id)
        rows = (
            await db.execute(select(BoardDesignSystem).where(BoardDesignSystem.board_id == board.id))
        ).scalars().all()
        assert len(rows) == 1 and rows[0].design_system_id == g2.id

        # an inline DS of ANOTHER board cannot be linked here.
        other = await _board(db)
        inline_other = await svc.create_design_system(
            USER_ID, title="Inline", scope="inline", board_id=other.id
        )
        with pytest.raises(DesignSystemError) as e:
            await svc.link_design_system_to_board(board.id, inline_other.id)
        assert e.value.code == "design_system_inline_other_board"

        # unlink clears the effective Design System.
        assert await svc.unlink_design_system_from_board(board.id) is True
        assert await svc.get_board_effective_design_system(board.id) is None


# ---------------------------------------------------------------------------
# MCP twins — shared service, structured errors
# ---------------------------------------------------------------------------


async def test_mcp_twins_catalog_link_and_effective():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        board = await _board(db)
        g = await DesignSystemService(db).create_design_system(
            USER_ID, title="MCP DS", scope="global"
        )
        await db.commit()
        ids = {"board": board.id, "g": g.id}

    try:
        listed = await _call("okto_pulse_list_design_systems", board_id=ids["board"], scope="global")
        assert listed["profile"] == "summary"
        assert ids["g"] in {d["id"] for d in listed["items"]}
        assert all("payload" not in item for item in listed["items"])

        # MCP requires a board_id even for the actor's global catalog. An owned
        # item returned by list must therefore be retrievable before it is
        # linked as the board's effective Design System.
        unlinked = await _call(
            "okto_pulse_get_design_system",
            board_id=ids["board"],
            design_system_id=ids["g"],
        )
        assert unlinked.get("id") == ids["g"], unlinked

        linked = await _call(
            "okto_pulse_link_board_design_system",
            board_id=ids["board"],
            design_system_id=ids["g"],
        )
        assert linked["design_system_id"] == ids["g"]
        got = await _call("okto_pulse_get_design_system", board_id=ids["board"], design_system_id=ids["g"])
        assert got["id"] == ids["g"] and got["version"] == 1

        inline = await _call(
            "okto_pulse_create_design_system", board_id=ids["board"], title="Inline", scope="inline"
        )
        assert inline["scope"] == "inline" and inline["board_id"] == ids["board"]

        eff = await _call("okto_pulse_get_board_design_system", board_id=ids["board"])
        assert eff["effective"]["design_system_id"] == ids["g"]

        # structured error twin (never a raw exception).
        err = await _call(
            "okto_pulse_get_design_system", board_id=ids["board"], design_system_id="missing"
        )
        assert err["code"] == "design_system_not_found"
    finally:
        async with get_session_factory()() as db:
            await db.execute(delete(BoardDesignSystem).where(BoardDesignSystem.board_id == ids["board"]))
            await db.execute(delete(DesignSystem).where(DesignSystem.owner_id == USER_ID))
            await db.execute(delete(Board).where(Board.id == ids["board"]))
            await db.commit()


async def test_ts_9c7f3ee0_mcp_catalog_summary_and_profile_aware_get():
    from okto_pulse.core.infra.database import get_session_factory

    large_payload = {"tokens": "x" * 50_000}
    async with get_session_factory()() as db:
        board = await _board(db)
        service = DesignSystemService(db)
        created = []
        for title in ("A payload", "B payload", "C payload"):
            created.append(
                await service.create_design_system(
                    USER_ID,
                    title=title,
                    scope="global",
                    payload=large_payload,
                )
            )
        await db.commit()
        board_id = board.id

    created_ids = {item.id for item in created}
    try:
        page_one = await _call(
            "okto_pulse_list_design_systems",
            board_id=board_id,
            scope="global",
            limit=2,
        )
        assert page_one["count"] == 2
        assert page_one["next_cursor"]
        assert all("payload" not in item for item in page_one["items"])
        assert len(json.dumps(page_one)) < 5_000

        page_two = await _call(
            "okto_pulse_list_design_systems",
            board_id=board_id,
            scope="global",
            limit=2,
            cursor=page_one["next_cursor"],
        )
        ids = {item["id"] for item in page_one["items"] + page_two["items"]}
        assert ids == created_ids
        assert page_two["next_cursor"] is None

        await _call(
            "okto_pulse_link_board_design_system",
            board_id=board_id,
            design_system_id=created[0].id,
        )
        full = await _call(
            "okto_pulse_get_design_system",
            board_id=board_id,
            design_system_id=created[0].id,
            profile="full",
        )
        assert full["payload"] == large_payload
        assert full["profile"] == "full"

        detail = await _call(
            "okto_pulse_get_design_system",
            board_id=board_id,
            design_system_id=created[0].id,
            profile="detail",
        )
        assert detail["payload"] == large_payload
        assert detail["profile"] == "detail"

        summary = await _call(
            "okto_pulse_get_design_system",
            board_id=board_id,
            design_system_id=created[0].id,
            profile="summary",
        )
        assert summary["profile"] == "summary"
        assert summary["payload_available"] is True
        assert "payload" not in summary
    finally:
        async with get_session_factory()() as db:
            await db.execute(delete(DesignSystem).where(DesignSystem.id.in_(created_ids)))
            await db.execute(delete(Board).where(Board.id == board_id))
            await db.commit()
