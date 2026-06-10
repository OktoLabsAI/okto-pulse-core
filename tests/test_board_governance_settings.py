"""BG-01.1 — Board governance settings and resolver."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select


pytestmark = pytest.mark.asyncio


USER_ID = "bg-01-user"


async def _create_board(db, *, settings: dict | None = None):
    from okto_pulse.core.models.db import Board

    board = Board(
        id=f"bg-board-{uuid.uuid4().hex[:8]}",
        name="BG Board",
        owner_id=USER_ID,
        settings=settings,
    )
    db.add(board)
    await db.flush()
    return board


async def test_defaults_resolve_without_mutating_stored_settings(db_factory):
    from okto_pulse.core.models.db import Board
    from okto_pulse.core.models.schemas import BoardSettings
    from okto_pulse.core.services.board_governance import BoardGovernanceService

    assert BoardSettings().allow_agent_self_answering is False
    assert BoardSettings().require_full_context_for_critical_actions is True

    async with db_factory() as db:
        board = await _create_board(db, settings={})
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        resolved = await BoardGovernanceService(db).resolve(board_id)
        assert resolved.allow_agent_self_answering is False
        assert resolved.require_full_context_for_critical_actions is True
        assert resolved.settings["allow_agent_self_answering"] is False
        assert resolved.settings["require_full_context_for_critical_actions"] is True

        stored = await db.get(Board, board_id)
        assert stored is not None
        assert stored.settings == {}


async def test_null_settings_resolve_to_safe_defaults(db_factory):
    from okto_pulse.core.services.board_governance import BoardGovernanceService

    async with db_factory() as db:
        board = await _create_board(db, settings=None)
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        resolved = await BoardGovernanceService(db).resolve(board_id)
        assert resolved.allow_agent_self_answering is False
        assert resolved.require_full_context_for_critical_actions is True


async def test_legacy_role_separation_does_not_grant_self_answering(db_factory):
    from okto_pulse.core.services.board_governance import BoardGovernanceService

    for legacy_value in (False, True):
        async with db_factory() as db:
            board = await _create_board(
                db,
                settings={"qa_require_role_separation": legacy_value},
            )
            board_id = board.id
            await db.commit()

        async with db_factory() as db:
            resolved = await BoardGovernanceService(db).resolve(board_id)
            assert resolved.allow_agent_self_answering is False
            assert resolved.qa_require_role_separation is legacy_value


async def test_board_update_merges_partial_governance_settings_and_logs_safe_event(db_factory):
    from okto_pulse.core.models.db import ActivityLog, Board
    from okto_pulse.core.models.schemas import BoardUpdate
    from okto_pulse.core.services.main import BoardService

    async with db_factory() as db:
        board = await _create_board(
            db,
            settings={
                "skip_test_coverage_global": True,
                "require_full_context_for_critical_actions": True,
            },
        )
        board_id = board.id
        await db.commit()

    async with db_factory() as db:
        service = BoardService(db)
        update = BoardUpdate.model_validate(
            {"settings": {"allow_agent_self_answering": True}}
        )
        updated = await service.update_board(board_id, USER_ID, update)
        assert updated is not None
        await db.commit()

    async with db_factory() as db:
        stored = await db.get(Board, board_id)
        assert stored is not None
        assert stored.settings["skip_test_coverage_global"] is True
        assert stored.settings["allow_agent_self_answering"] is True
        assert stored.settings["require_full_context_for_critical_actions"] is True

        rows = (
            await db.execute(
                select(ActivityLog)
                .where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "board_governance_setting_changed",
                )
                .order_by(ActivityLog.created_at.asc())
            )
        ).scalars().all()
        assert len(rows) == 1
        details = rows[0].details
        assert details == {
            "metric_name": "board_governance_setting_changed_total",
            "board_id": board_id,
            "actor_id": USER_ID,
            "setting_key": "allow_agent_self_answering",
            "old_effective_value": False,
            "new_effective_value": True,
            "surface": "board_patch",
            "outcome": "changed",
        }
