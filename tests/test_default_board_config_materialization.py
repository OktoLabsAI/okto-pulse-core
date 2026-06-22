"""Spec 8a2fad91 / card 2803c136 — transactional materialization of BoardGuideline
defaults during board creation (FR3, TR3/TR4, AC3/AC4), scenario ts_a48e70ee.

Validator criteria reproduced:
  1) the migration adds template_id/template_version/guideline_version NULLABLE to
     BoardGuideline; manual/legacy links keep NULL (no backfill);
  2) a default-materialized BoardGuideline carries priority + template_id +
     template_version + guideline_version;
  3) a forced GuidelineService.apply_default_guidelines failure -> structured
     default_materialization_failed; after a clean-session rollback the board does
     not exist and no orphan BoardGuideline remains;
  4) idempotent per uq_board_guideline (no duplicate); intra-template duplicate
     guideline_ids de-duped deterministically (first wins);
  5) the applied_to_board audit row is queryable (actor/template_version/board_id);
     the materialization_failed event carries the same fields in the structured error.

create_board materializes from scope='global' (hardcoded). Each test first
creates + activates ITS OWN global template (deactivating any leaked active
template in-session) and never commits, so the single session is rolled back at
close and nothing leaks (gotcha ts_cdb70cc0). Template versions are read relatively.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_default_board_config_materialization.py
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from okto_pulse.core.models.db import (
    Board,
    BoardGuideline,
    DefaultBoardConfigurationAudit,
    Guideline,
)
from okto_pulse.core.models.schemas import BoardCreate, BoardSettings
from okto_pulse.core.services.default_board_configuration import (
    EVENT_GUIDELINE_APPLIED_TO_BOARD,
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService, GuidelineService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-materialize-user"


async def _global_guideline(db, title: str, version: int = 3) -> Guideline:
    g = Guideline(
        title=title, content="c", scope="global", board_id=None, owner_id=USER_ID, version=version
    )
    db.add(g)
    await db.flush()
    return g


async def _active_global_template_with(db, refs):
    """Create + activate a global template (deactivating any other active global
    template in this session) whose guideline_default_refs are ``refs``."""
    return await DefaultBoardConfigurationService(db).create_version(
        settings_payload=BoardSettings(),
        actor=USER_ID,
        scope="global",
        guideline_default_refs=refs,
        activate=True,
    )


async def _board(db, name: str | None = None) -> Board:
    return await BoardService(db).create_board(
        USER_ID, BoardCreate(name=name or f"b-{uuid.uuid4().hex[:8]}")
    )


# ---------------------------------------------------------------------------
# criterion 1 — nullable provenance, no backfill for manual links
# ---------------------------------------------------------------------------


async def test_manual_link_keeps_null_provenance():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        await _active_global_template_with(db, [])  # empty defaults -> create_board links nothing
        g = await _global_guideline(db, "Manual")
        board = await _board(db)
        link = await GuidelineService(db).link_guideline_to_board(board.id, g.id, priority=2)
        await db.refresh(link)
        # the migration added the columns (else this would error) and they are NULL
        # for a manually-linked guideline — forward-only, no backfill (TR5).
        assert link.priority == 2
        assert link.template_id is None
        assert link.template_version is None
        assert link.guideline_version is None


# ---------------------------------------------------------------------------
# criterion 2 + AC3 — board creation materializes links with provenance
# ---------------------------------------------------------------------------


async def test_board_creation_materializes_links_with_provenance():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1", version=4)
        g2 = await _global_guideline(db, "G2", version=7)
        tmpl = await _active_global_template_with(db, [
            {"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version},
            {"guideline_id": g2.id, "priority": 5, "guideline_version": g2.version},
        ])
        board = await _board(db)

        links = (
            await db.execute(select(BoardGuideline).where(BoardGuideline.board_id == board.id))
        ).scalars().all()
        by_gid = {link.guideline_id: link for link in links}
        assert set(by_gid) == {g1.id, g2.id}  # exactly two, no duplicate
        assert by_gid[g1.id].priority == 1 and by_gid[g2.id].priority == 5  # preserved
        for gid, gver in ((g1.id, g1.version), (g2.id, g2.version)):
            assert by_gid[gid].template_id == tmpl.id
            assert by_gid[gid].template_version == tmpl.version
            assert by_gid[gid].guideline_version == gver


# ---------------------------------------------------------------------------
# criterion 5 — applied_to_board audit row is queryable
# ---------------------------------------------------------------------------


async def test_applied_to_board_audit_is_queryable():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        tmpl = await _active_global_template_with(
            db, [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}]
        )
        board = await _board(db)

        rows = (
            await db.execute(
                select(DefaultBoardConfigurationAudit).where(
                    DefaultBoardConfigurationAudit.event_type == EVENT_GUIDELINE_APPLIED_TO_BOARD,
                    DefaultBoardConfigurationAudit.template_id == tmpl.id,
                )
            )
        ).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.template_version == tmpl.version
        assert row.actor_id == USER_ID
        assert row.payload["board_id"] == board.id
        assert g1.id in row.payload["guideline_ids"]


# ---------------------------------------------------------------------------
# criterion 3 + AC4 + ts_a48e70ee — forced failure aborts transactionally
# ---------------------------------------------------------------------------


async def test_forced_apply_failure_aborts_transactionally():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        tmpl = await _active_global_template_with(
            db, [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}]
        )
        name = f"b-{uuid.uuid4().hex[:8]}"

        with patch.object(
            GuidelineService, "apply_default_guidelines", side_effect=RuntimeError("boom")
        ):
            with pytest.raises(DefaultBoardConfigurationError) as exc:
                await _board(db, name)
        assert exc.value.code == "default_materialization_failed"
        # criterion 5: the failure event carries actor/template_version/board_id/cause.
        d = exc.value.details
        assert d["cause"] == "RuntimeError"
        assert d["detail"] == "boom"
        assert d["template_id"] == tmpl.id and d["template_version"] == tmpl.version
        assert d["actor"] == USER_ID and d["board_id"]

        # criterion 3: clean-session rollback — no partial board, no orphan links.
        await db.rollback()
        boards = (
            await db.execute(select(Board).where(Board.name == name))
        ).scalars().all()
        assert boards == []
        orphans = (
            await db.execute(
                select(func.count()).select_from(BoardGuideline).where(
                    BoardGuideline.template_id == tmpl.id
                )
            )
        ).scalar()
        assert orphans == 0


# ---------------------------------------------------------------------------
# criterion 4 + TR4 — idempotency + intra-template dedup (writer)
# ---------------------------------------------------------------------------


async def test_apply_default_guidelines_idempotent_and_dedups():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        await _active_global_template_with(db, [])  # create_board links nothing
        g1 = await _global_guideline(db, "G1")
        g2 = await _global_guideline(db, "G2")
        board = await _board(db)
        gsvc = GuidelineService(db)

        refs = [
            {"guideline_id": g1.id, "priority": 1, "guideline_version": 1},
            {"guideline_id": g1.id, "priority": 9, "guideline_version": 9},  # intra-batch dup
            {"guideline_id": g2.id, "priority": 2, "guideline_version": 2},
        ]
        created = await gsvc.apply_default_guidelines(
            board.id, refs, template_id="t1", template_version=7
        )
        # intra-template dup de-duped first-wins: g1 once (priority 1), g2 once.
        assert {c.guideline_id for c in created} == {g1.id, g2.id}
        assert len(created) == 2
        g1link = next(c for c in created if c.guideline_id == g1.id)
        assert g1link.priority == 1 and g1link.guideline_version == 1  # first wins

        # idempotent re-run: existing board/guideline links preserved, none created.
        again = await gsvc.apply_default_guidelines(
            board.id, refs, template_id="t2", template_version=8
        )
        assert again == []
        total = (
            await db.execute(
                select(func.count()).select_from(BoardGuideline).where(
                    BoardGuideline.board_id == board.id
                )
            )
        ).scalar()
        assert total == 2  # uq_board_guideline upheld; no duplicate, provenance unchanged
        preserved = (
            await db.execute(
                select(BoardGuideline).where(
                    BoardGuideline.board_id == board.id, BoardGuideline.guideline_id == g1.id
                )
            )
        ).scalar_one()
        assert preserved.template_id == "t1" and preserved.priority == 1  # untouched
