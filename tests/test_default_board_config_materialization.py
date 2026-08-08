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
from sqlalchemy import select

from sqlalchemy_test_models import (
    Board,
    DefaultBoardConfigurationAudit,
)
from okto_pulse.core.models.schemas import (
    BoardCreate,
    BoardSettings,
    GuidelineCreate,
    GuidelineUpdate,
)
from okto_pulse.core.services.default_board_configuration import (
    EVENT_GUIDELINE_APPLIED_TO_BOARD,
    DefaultBoardConfigurationError,
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import BoardService, GuidelineService

pytestmark = pytest.mark.asyncio

USER_ID = "dbc-materialize-user"


async def _global_guideline(db, title: str, version: int = 3):
    service = GuidelineService(db)
    guideline = await service.create_guideline(
        USER_ID,
        GuidelineCreate(
            title=title,
            content="c-1",
            scope="global",
            board_id=None,
        ),
    )
    for revision_number in range(2, version + 1):
        guideline = await service.update_guideline(
            guideline.id,
            USER_ID,
            GuidelineUpdate(content=f"c-{revision_number}"),
        )
        assert guideline is not None
    return guideline


async def _active_global_template_with(db, refs):
    """Create + activate a global template (deactivating any other active global
    template in this session) whose guideline_default_refs are ``refs``."""
    return await DefaultBoardConfigurationService(db).create_version(
        settings_payload=BoardSettings(),
        actor=USER_ID,
        scope="global",
        guideline_default_refs=refs,
        activate=True,
        compatibility_import=True,
    )


async def _board(db, name: str | None = None) -> Board:
    return await BoardService(db).create_board(
        USER_ID, BoardCreate(name=name or f"b-{uuid.uuid4().hex[:8]}")
    )


# ---------------------------------------------------------------------------
# criterion 1 — direct manual links fail closed before provenance is persisted
# ---------------------------------------------------------------------------


async def test_manual_link_requires_impact_preview_and_persists_nothing():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.ports.guideline_policy import (
        GuidelinePolicyBindingConflict,
    )
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    async with get_session_factory()() as db:
        await _active_global_template_with(
            db, []
        )  # empty defaults -> create_board links nothing
        g = await _global_guideline(db, "Manual")
        board = await _board(db)
        with pytest.raises(
            GuidelinePolicyBindingConflict,
            match="guideline_impact_preview_required",
        ) as exc:
            await GuidelineService(db).link_guideline_to_board(
                board.id,
                g.id,
                priority=2,
            )

        assert dict(exc.value.details) == {
            "board_id": board.id,
            "guideline_id": g.id,
            "remediation": "preview_then_adopt",
        }
        persisted = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .get_binding(board_id=board.id, guideline_id=g.id)
        )
        assert persisted is None


# ---------------------------------------------------------------------------
# criterion 2 + AC3 — board creation materializes links with provenance
# ---------------------------------------------------------------------------


async def test_board_creation_materializes_links_with_provenance():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1", version=4)
        g2 = await _global_guideline(db, "G2", version=7)
        await _active_global_template_with(
            db,
            [
                {"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version},
                {"guideline_id": g2.id, "priority": 5, "guideline_version": g2.version},
            ],
        )
        board = await _board(db)

        links = (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .list_bindings(board_id=board.id)
        )
        by_gid = {link.guideline_id: link for link in links}
        assert set(by_gid) == {g1.id, g2.id}  # exactly two, no duplicate
        assert by_gid[g1.id].priority == 1 and by_gid[g2.id].priority == 5  # preserved
        for guideline in (g1, g2):
            binding = by_gid[guideline.id]
            assert binding.revision_id == guideline.revision_id
            assert binding.semantic_version == guideline.semantic_version
            assert binding.revision_digest == guideline.revision_digest


# ---------------------------------------------------------------------------
# criterion 5 — applied_to_board audit row is queryable
# ---------------------------------------------------------------------------


async def test_applied_to_board_audit_is_queryable():
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        tmpl = await _active_global_template_with(
            db,
            [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}],
        )
        board = await _board(db)

        rows = (
            (
                await db.execute(
                    select(DefaultBoardConfigurationAudit).where(
                        DefaultBoardConfigurationAudit.event_type
                        == EVENT_GUIDELINE_APPLIED_TO_BOARD,
                        DefaultBoardConfigurationAudit.template_id == tmpl.id,
                    )
                )
            )
            .scalars()
            .all()
        )
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
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    async with get_session_factory()() as db:
        g1 = await _global_guideline(db, "G1")
        tmpl = await _active_global_template_with(
            db,
            [{"guideline_id": g1.id, "priority": 1, "guideline_version": g1.version}],
        )
        name = f"b-{uuid.uuid4().hex[:8]}"

        with patch.object(
            GuidelineService,
            "apply_default_guidelines",
            side_effect=RuntimeError("boom"),
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
            (await db.execute(select(Board).where(Board.name == name))).scalars().all()
        )
        assert boards == []
        assert (
            await require_relational_application_adapter()
            .guideline_policy(db)
            .list_bindings(board_id=d["board_id"])
            == ()
        )


# ---------------------------------------------------------------------------
# criterion 4 + TR4 — idempotency + intra-template dedup (writer)
# ---------------------------------------------------------------------------


async def test_apply_default_guidelines_idempotent_and_dedups():
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.ports.relational_application import (
        require_relational_application_adapter,
    )

    async with get_session_factory()() as db:
        await _active_global_template_with(db, [])  # create_board links nothing
        g1 = await _global_guideline(db, "G1")
        g2 = await _global_guideline(db, "G2")
        board = await _board(db)
        gsvc = GuidelineService(db)

        refs = [
            {"guideline_id": g1.id, "priority": 1, "guideline_version": 1},
            {
                "guideline_id": g1.id,
                "priority": 9,
                "guideline_version": 9,
            },  # intra-batch dup
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
        policy = require_relational_application_adapter().guideline_policy(db)
        bindings = await policy.list_bindings(board_id=board.id)
        assert len(bindings) == 2
        preserved = await policy.get_binding(
            board_id=board.id,
            guideline_id=g1.id,
        )
        assert preserved is not None
        assert preserved.priority == 1
        assert preserved.binding_revision == 1
