"""Spec 8a2fad91 / card 5cb88511 — admin API + MCP for GLOBAL guideline defaults
on the umbrella template (FR1/FR2/FR6, TR1/TR6), scenarios ts_c949f743 +
ts_ed08c1b1 (+ light no-parallel-store guard ts_18ab5dd2).

Validator criteria reproduced here:
  1) updating guideline_default_refs on an ACTIVE template creates a NEW version
     (copy-on-write / version-bump, Q1=B);
  2) the previous version keeps its content intact + reconstituible by id/version;
  3) updating a DRAFT template mutates it in place;
  4) inline/missing/board-scoped refs are rejected fail-closed, template unchanged;
  5) default state is derived FROM the template, never a Guideline.is_default flag.

Isolation: the test DB persists committed rows across the whole session (the
conftest resets singletons/caches, not SQL). Template version numbering is
per-scope, so every test uses a UNIQUE template scope — version numbers reset to
1 and an activate() in one test never deactivates another scope's active template.
Guideline catalog rows must still be scope='global' (catalog eligibility).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_config_guidelines.py
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import delete

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    Board,
    BoardGuideline,
    DefaultBoardConfiguration,
    DefaultBoardConfigurationAudit,
    Guideline,
)
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_config_api import DefaultBoardConfigApiService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-guidelines-user"


def _scope() -> str:
    return f"t-{uuid.uuid4().hex[:8]}"


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


async def _global_guideline(db, title: str) -> Guideline:
    g = Guideline(
        title=title, content="c", scope="global", board_id=None, owner_id=USER_ID, version=1
    )
    db.add(g)
    await db.flush()
    return g


# ---------------------------------------------------------------------------
# ts_c949f743 + validator criteria 1/2 — active update is copy-on-write
# ---------------------------------------------------------------------------


async def test_active_update_creates_new_version_and_keeps_old_intact():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        svc = DefaultBoardConfigurationService(db)
        api = DefaultBoardConfigApiService(db)
        v1 = await svc.create_version(
            settings_payload=BoardSettings(max_scenarios_per_card=3),
            actor=USER_ID, scope=scope, activate=True,
        )
        g1 = await _global_guideline(db, "Aaa coding standard")

        # update the ACTIVE template -> a NEW version is created + activated.
        updated = await api.update_template_guidelines(
            template_id=v1.id,
            guideline_default_refs=[{"guideline_id": g1.id, "priority": 1}],
            actor=USER_ID,
        )
        assert updated["id"] != v1.id
        assert updated["version"] == v1.version + 1
        assert updated["scope"] == scope
        assert updated["is_active"] is True and updated["status"] == "active"
        assert updated["guideline_default_refs"][0]["guideline_id"] == g1.id
        assert updated["guideline_default_refs"][0]["priority"] == 1

        # the prior version is intact + reconstituible by id/version: its content
        # (settings + guideline refs) is unchanged; only its activation flag flipped.
        reloaded_v1 = await svc._require(v1.id)
        assert reloaded_v1.version == 1
        assert reloaded_v1.guideline_default_refs is None  # never carried the new ref
        assert reloaded_v1.settings_payload == v1.settings_payload
        assert reloaded_v1.is_active is False

        # candidates derive is_default + priority from the active template.
        cands = await api.list_default_candidates(scope=scope)
        assert cands["template_version"] == updated["version"]
        by_id = {c["guideline_id"]: c for c in cands["candidates"]}
        assert by_id[g1.id]["is_default"] is True
        assert by_id[g1.id]["priority"] == 1
        assert by_id[g1.id]["eligible"] is True


async def test_active_update_each_change_bumps_version():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        svc = DefaultBoardConfigurationService(db)
        api = DefaultBoardConfigApiService(db)
        v1 = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=True
        )
        g1 = await _global_guideline(db, "G1")
        g2 = await _global_guideline(db, "G2")

        v2 = await api.update_template_guidelines(
            template_id=v1.id,
            guideline_default_refs=[{"guideline_id": g1.id, "priority": 1}],
            actor=USER_ID,
        )
        v3 = await api.update_template_guidelines(
            template_id=v2["id"],
            guideline_default_refs=[
                {"guideline_id": g1.id, "priority": 1},
                {"guideline_id": g2.id, "priority": 2},
            ],
            actor=USER_ID,
        )
        # a fresh scope numbers versions deterministically from 1.
        assert (v1.version, v2["version"], v3["version"]) == (1, 2, 3)

        # every prior version stays reconstituible with its own refs.
        r1 = await svc._require(v1.id)
        r2 = await svc._require(v2["id"])
        assert r1.guideline_default_refs is None
        assert [r["guideline_id"] for r in r2.guideline_default_refs] == [g1.id]
        assert {r["guideline_id"] for r in v3["guideline_default_refs"]} == {g1.id, g2.id}
        # only the newest version is active.
        versions = await api.list_versions(scope=scope)
        assert versions["active_id"] == v3["id"]


# ---------------------------------------------------------------------------
# validator criterion 3 — draft update mutates in place
# ---------------------------------------------------------------------------


async def test_draft_update_mutates_in_place():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        svc = DefaultBoardConfigurationService(db)
        api = DefaultBoardConfigApiService(db)
        draft = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=False
        )
        assert draft.status == "draft" and draft.is_active is False
        g1 = await _global_guideline(db, "Draft default")

        updated = await api.update_template_guidelines(
            template_id=draft.id,
            guideline_default_refs=[{"guideline_id": g1.id, "priority": 5}],
            actor=USER_ID,
        )
        # SAME template id + version: mutated in place, no version-bump.
        assert updated["id"] == draft.id
        assert updated["version"] == draft.version
        assert updated["status"] == "draft"
        assert updated["guideline_default_refs"][0]["guideline_id"] == g1.id

        versions = await api.list_versions(scope=scope)
        assert len(versions["versions"]) == 1  # no new version created


# ---------------------------------------------------------------------------
# ts_ed08c1b1 + validator criterion 4 — fail-closed rejection, template unchanged
# ---------------------------------------------------------------------------


async def test_inline_missing_and_boardscoped_refs_rejected_failclosed():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        scope = _scope()
        svc = DefaultBoardConfigurationService(db)
        api = DefaultBoardConfigApiService(db)
        v1 = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=True
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        # an inline / board-scoped guideline (board_id set) is NOT a global default.
        inline = Guideline(
            title="Inline", content="c", scope="inline", board_id=board.id, owner_id=USER_ID, version=1
        )
        db.add(inline)
        await db.flush()

        cases = [
            ([{"priority": 1}], "default_guideline_inline_not_allowed"),  # no guideline_id
            ([{"guideline_id": "does-not-exist"}], "default_guideline_not_found"),
            ([{"guideline_id": inline.id}], "default_guideline_not_global"),
        ]
        for refs, expected_code in cases:
            with pytest.raises(DefaultBoardConfigurationError) as exc:
                await api.update_template_guidelines(
                    template_id=v1.id, guideline_default_refs=refs, actor=USER_ID
                )
            assert exc.value.code == expected_code

        # the template is unchanged: still v1 active, still no refs, no new version.
        versions = await api.list_versions(scope=scope)
        assert len(versions["versions"]) == 1
        assert versions["active_id"] == v1.id
        reloaded = await svc._require(v1.id)
        assert reloaded.guideline_default_refs is None
        assert reloaded.is_active is True


# ---------------------------------------------------------------------------
# ts_18ab5dd2 (light) — default state comes from the template, not a Guideline flag
# ---------------------------------------------------------------------------


async def test_default_state_derived_from_template_not_guideline_flag():
    from okto_pulse.core.infra.database import get_session_factory

    # The Guideline model carries NO authoritative is_default column — the only
    # source of truth for default membership is the umbrella template.
    assert not hasattr(Guideline, "is_default")

    async with get_session_factory()() as db:
        scope = _scope()
        svc = DefaultBoardConfigurationService(db)
        api = DefaultBoardConfigApiService(db)
        g1 = await _global_guideline(db, "Derived")
        v1 = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=True
        )

        # not a default yet (template has no ref).
        before = {
            c["guideline_id"]: c
            for c in (await api.list_default_candidates(scope=scope))["candidates"]
        }
        assert before[g1.id]["is_default"] is False

        # becomes default purely by the template ref change.
        await api.update_template_guidelines(
            template_id=v1.id,
            guideline_default_refs=[{"guideline_id": g1.id, "priority": 1}],
            actor=USER_ID,
        )
        after = {
            c["guideline_id"]: c
            for c in (await api.list_default_candidates(scope=scope))["candidates"]
        }
        assert after[g1.id]["is_default"] is True


# ---------------------------------------------------------------------------
# MCP twins — shared orchestrator, structured errors
# ---------------------------------------------------------------------------


async def test_mcp_twin_list_candidates_and_update_refs():
    from okto_pulse.core.infra.database import get_session_factory

    scope = _scope()
    async with get_session_factory()() as db:
        svc = DefaultBoardConfigurationService(db)
        v1 = await svc.create_version(
            settings_payload=BoardSettings(), actor=USER_ID, scope=scope, activate=True
        )
        board = await BoardService(db).create_board(
            USER_ID, BoardCreate(name=f"b-{uuid.uuid4().hex[:8]}")
        )
        g1 = await _global_guideline(db, "MCP default")
        # MCP twins open a separate session, so the seed must be committed.
        await db.commit()
        ids = {"board": board.id, "v1": v1.id, "v1ver": v1.version, "g1": g1.id}

    try:
        # read twin — g1 is eligible, not yet default.
        cands = await _call(
            "okto_pulse_list_default_guideline_candidates", board_id=ids["board"], scope=scope
        )
        by_id = {c["guideline_id"]: c for c in cands["candidates"]}
        assert by_id[ids["g1"]]["eligible"] is True
        assert by_id[ids["g1"]]["is_default"] is False

        # write twin on the ACTIVE template -> copy-on-write new version.
        updated = await _call(
            "okto_pulse_update_default_guideline_refs",
            board_id=ids["board"],
            template_id=ids["v1"],
            guideline_default_refs=[{"guideline_id": ids["g1"], "priority": 1}],
        )
        assert updated["id"] != ids["v1"]
        assert updated["version"] == ids["v1ver"] + 1
        assert updated["is_active"] is True

        # structured error twin (never a raw exception).
        err = await _call(
            "okto_pulse_update_default_guideline_refs",
            board_id=ids["board"],
            template_id=updated["id"],
            guideline_default_refs=[{"guideline_id": "missing"}],
        )
        assert err["code"] == "default_guideline_not_found"
    finally:
        # remove the committed rows so nothing leaks into later tests/files.
        async with get_session_factory()() as db:
            await db.execute(
                delete(DefaultBoardConfigurationAudit).where(
                    DefaultBoardConfigurationAudit.scope == scope
                )
            )
            await db.execute(
                delete(DefaultBoardConfiguration).where(DefaultBoardConfiguration.scope == scope)
            )
            await db.execute(delete(BoardGuideline).where(BoardGuideline.board_id == ids["board"]))
            await db.execute(delete(Guideline).where(Guideline.id == ids["g1"]))
            await db.execute(delete(Board).where(Board.id == ids["board"]))
            await db.commit()
