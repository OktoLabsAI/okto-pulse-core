"""Spec 8a2fad91 / card 5c73293c — forward-only semantics + absence of a parallel
default store for guideline defaults (FR4, TR5/TR7, AC5/AC6/AC8), scenarios
ts_8b1455b1 + ts_18ab5dd2.

This is a PROOF / regression card: the invariants are guaranteed BY DESIGN — the
materialization is snapshot-at-creation, card #1's template update only mutates the
template refs and never touches BoardGuideline, and default membership lives ONLY in
the umbrella template refs (there is no authoritative Guideline.is_default flag and
no independent default-guideline store). No production code is added; these
behavioral tests prove the invariants through the REAL services + model
introspection (not a fragile textual grep).

create_board materializes from scope='global'. Each test establishes its OWN active
global template and never commits (single session rolled back at close — gotcha
ts_cdb70cc0).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_config_forward_only.py
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.models.db import (
    Base,
    BoardGuideline,
    DefaultBoardConfiguration,
    Guideline,
)
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings, GuidelineCreate
from okto_pulse.core.services.default_board_config_api import DefaultBoardConfigApiService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService, GuidelineService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-forward-only-user"


async def _global_guideline(db, title: str, version: int = 2) -> Guideline:
    g = Guideline(
        title=title, content="c", scope="global", board_id=None, owner_id=USER_ID, version=version
    )
    db.add(g)
    await db.flush()
    return g


async def _active_global_template_with(db, refs):
    return await DefaultBoardConfigurationService(db).create_version(
        settings_payload=BoardSettings(),
        actor=USER_ID,
        scope="global",
        guideline_default_refs=refs,
        activate=True,
    )


async def _board(db, name: str | None = None):
    return await BoardService(db).create_board(
        USER_ID, BoardCreate(name=name or f"b-{uuid.uuid4().hex[:8]}")
    )


async def _link_gids(db, board_id) -> set:
    return set(
        (
            await db.execute(
                select(BoardGuideline.guideline_id).where(BoardGuideline.board_id == board_id)
            )
        ).scalars().all()
    )


async def _get_link(db, board_id, gid):
    return (
        await db.execute(
            select(BoardGuideline).where(
                BoardGuideline.board_id == board_id, BoardGuideline.guideline_id == gid
            )
        )
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# forward-only (ts_8b1455b1, FR4, AC5, AC6)
# ---------------------------------------------------------------------------


async def test_existing_links_survive_default_removal_and_new_board_excludes_it():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        api = DefaultBoardConfigApiService(db)
        await _active_global_template_with(
            db, [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}]
        )

        # board A created while g1 is a default.
        board_a = await _board(db)
        before = await _link_gids(db, board_a.id)
        assert g1.id in before
        link_a = await _get_link(db, board_a.id, g1.id)
        assert link_a.priority == 1 and link_a.template_version is not None

        # remove g1 from the active template (copy-on-write new version).
        active = (await api.get_active())["active"]
        await api.update_template_guidelines(
            template_id=active["id"], guideline_default_refs=[], actor=USER_ID
        )

        # AC5/AC6/FR4: board A is UNCHANGED — no backfill / no live inheritance / the
        # removal never unlinks an already-materialized BoardGuideline.
        assert await _link_gids(db, board_a.id) == before
        link_a_after = await _get_link(db, board_a.id, g1.id)
        assert link_a_after is not None and link_a_after.priority == 1

        # forward-only: board B created AFTER the removal does NOT receive g1.
        board_b = await _board(db)
        assert g1.id not in await _link_gids(db, board_b.id)


async def test_future_template_change_does_not_mutate_existing_links():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        g2 = await _global_guideline(db, "G2")
        api = DefaultBoardConfigApiService(db)
        await _active_global_template_with(
            db, [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}]
        )
        board_a = await _board(db)

        # bump g1 priority + add g2 in a NEW template version.
        active = (await api.get_active())["active"]
        await api.update_template_guidelines(
            template_id=active["id"],
            guideline_default_refs=[
                {"guideline_id": g1.id, "priority": 9, "guideline_version": g1.version},
                {"guideline_id": g2.id, "priority": 2, "guideline_version": g2.version},
            ],
            actor=USER_ID,
        )

        # the future template change did NOT propagate to the existing board.
        link = await _get_link(db, board_a.id, g1.id)
        assert link.priority == 1  # unchanged priority
        assert g2.id not in await _link_gids(db, board_a.id)  # added default not inherited


# ---------------------------------------------------------------------------
# TR5 — legacy / template-independent effective read
# ---------------------------------------------------------------------------


async def test_board_reads_effective_guidelines_via_links_and_inline():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        await _active_global_template_with(db, [])  # no materialized defaults
        board = await _board(db)
        gsvc = GuidelineService(db)
        linked = await _global_guideline(db, "Linked global")
        await gsvc.link_guideline_to_board(board.id, linked.id, priority=1)
        inline = await gsvc.create_guideline(
            USER_ID,
            GuidelineCreate(title="Inline", content="c", scope="inline", board_id=board.id),
        )

        # effective guidelines = BoardGuideline links + inline guidelines, with no
        # dependency on the default template (TR5, no destructive migration).
        effective = await gsvc.get_board_guidelines(board.id)
        gids = {item["guideline"]["id"] for item in effective}
        assert linked.id in gids
        assert inline.id in gids


# ---------------------------------------------------------------------------
# no parallel store (ts_18ab5dd2, TR7)
# ---------------------------------------------------------------------------


async def test_no_parallel_default_store_via_model_introspection():
    # Default membership lives ONLY on the umbrella template — the Guideline model
    # carries no authoritative is_default column/attribute (introspection, not grep).
    assert "is_default" not in Guideline.__table__.columns
    assert not hasattr(Guideline, "is_default")
    # the default set is a JSON column on the umbrella template, not a separate table.
    assert "guideline_default_refs" in DefaultBoardConfiguration.__table__.columns
    parallel = [
        name for name in Base.metadata.tables
        if "guideline_default" in name or "default_guideline" in name
    ]
    assert parallel == [], f"unexpected parallel default-guideline store table(s): {parallel}"


async def test_default_state_is_derived_from_template_refs_not_a_flag():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "Derived")
        api = DefaultBoardConfigApiService(db)
        tmpl = await _active_global_template_with(db, [])

        cand = {
            c["guideline_id"]: c
            for c in (await api.list_default_candidates(scope="global"))["candidates"]
        }
        assert cand[g1.id]["is_default"] is False

        # flipping the template ref (the single source of truth) flips the state.
        await api.update_template_guidelines(
            template_id=tmpl.id,
            guideline_default_refs=[
                {"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}
            ],
            actor=USER_ID,
        )
        cand2 = {
            c["guideline_id"]: c
            for c in (await api.list_default_candidates(scope="global"))["candidates"]
        }
        assert cand2[g1.id]["is_default"] is True
