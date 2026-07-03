"""Tests for GET /api/v1/kg/boards/{id}/pending/tree (spec f33eb9ca).

Migrated to the UnitOfWork transport (spec R01A REST-FU5-S3): the endpoint now
binds ``uow=Depends(get_unit_of_work)`` instead of ``db=Depends(get_db)``, so the
direct-call form wraps the request session in a ``SQLAlchemyUnitOfWork``. Every
assertion is preserved — only the persistence handle passed to the endpoint
changed.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.api.kg_routes import list_pending_tree
from okto_pulse.core.application.use_cases import ActorContext
from okto_pulse.core.repositories import SQLAlchemyUnitOfWork


def _actor(board_id: str) -> ActorContext:
    return ActorContext("u", "rest", board_id=board_id)


@pytest.mark.asyncio
async def test_pending_tree_empty_board(db_factory):
    from okto_pulse.core.models.db import Board

    factory = db_factory
    async with factory() as db:
        db.add(Board(id="bt-empty", name="b", description="", owner_id="u"))
        await db.commit()
        result = await list_pending_tree(
            "bt-empty", 5, actor=_actor("bt-empty"), uow=SQLAlchemyUnitOfWork(db)
        )
    assert result["board_id"] == "bt-empty"
    assert result["tree"] == []
    assert result["total_pending"] == 0
    assert set(result["levels"].keys()) == {
        "ideations", "refinements", "specs", "sprints", "cards",
    }


@pytest.mark.asyncio
async def test_pending_tree_hierarchical_shape(db_factory):
    """Seed a single ideation→refinement→spec→sprint→card chain and
    verify the tree produced by the endpoint matches the parent links."""
    from okto_pulse.core.models.db import (
        Board, Card, Ideation, Refinement, Spec, Sprint,
    )
    factory = db_factory
    async with factory() as db:
        board = Board(id="bt-1", name="b", description="", owner_id="u")
        db.add(board)
        await db.flush()
        idea = Ideation(id="i-1", board_id=board.id, title="Idea",
                        description="", problem_statement="",
                        proposed_approach="", created_by="u")
        db.add(idea)
        await db.flush()
        ref = Refinement(id="r-1", board_id=board.id, ideation_id=idea.id,
                         title="R", description="", created_by="u")
        db.add(ref)
        await db.flush()
        spec = Spec(id="s-1", board_id=board.id, refinement_id=ref.id,
                    title="S", description="", created_by="u")
        db.add(spec)
        await db.flush()
        sprint = Sprint(id="sp-1", board_id=board.id, spec_id=spec.id,
                        title="Sprint 1", created_by="u")
        db.add(sprint)
        await db.flush()
        card = Card(id="c-1", board_id=board.id, spec_id=spec.id,
                    sprint_id=sprint.id, title="Card", created_by="u")
        db.add(card)
        await db.commit()

    async with factory() as db:
        result = await list_pending_tree(
            "bt-1", 5, actor=_actor("bt-1"), uow=SQLAlchemyUnitOfWork(db)
        )

    assert len(result["tree"]) == 1
    ideation_node = result["tree"][0]
    assert ideation_node["id"] == "i-1"
    assert ideation_node["type"] == "ideation"
    assert len(ideation_node["children"]) == 1
    ref_node = ideation_node["children"][0]
    assert ref_node["type"] == "refinement"
    assert ref_node["id"] == "r-1"
    spec_node = ref_node["children"][0]
    assert spec_node["type"] == "spec"
    sprint_node = spec_node["children"][0]
    assert sprint_node["type"] == "sprint"
    card_node = sprint_node["children"][0]
    assert card_node["type"] == "card"
    assert card_node["id"] == "c-1"


@pytest.mark.asyncio
async def test_pending_tree_depth_limits_children(db_factory):
    """depth=2 should return ideations + refinements but no deeper."""
    from okto_pulse.core.models.db import (
        Board, Ideation, Refinement, Spec,
    )
    factory = db_factory
    async with factory() as db:
        board = Board(id="bt-2", name="b", description="", owner_id="u")
        idea = Ideation(id="i-2", board_id="bt-2", title="I2",
                        description="", problem_statement="",
                        proposed_approach="", created_by="u")
        ref = Refinement(id="r-2", board_id="bt-2", ideation_id="i-2",
                         title="R2", description="", created_by="u")
        spec = Spec(id="s-2", board_id="bt-2", refinement_id="r-2",
                    title="S2", description="", created_by="u")
        db.add_all([board, idea, ref, spec])
        await db.commit()

    async with factory() as db:
        result = await list_pending_tree(
            "bt-2", 2, actor=_actor("bt-2"), uow=SQLAlchemyUnitOfWork(db)
        )
    ideation_node = result["tree"][0]
    ref_node = ideation_node["children"][0]
    # depth=2 stops at refinement — no spec children.
    assert ref_node["children"] == []


@pytest.mark.asyncio
async def test_pending_tree_counters_track_queue_status(db_factory):
    """ConsolidationQueue statuses must flow into levels counters."""
    from okto_pulse.core.models.db import (
        Board, ConsolidationQueue, Ideation, Spec,
    )
    factory = db_factory
    async with factory() as db:
        board = Board(id="bt-3", name="b", description="", owner_id="u")
        idea = Ideation(id="i-3", board_id="bt-3", title="I3",
                        description="", problem_statement="",
                        proposed_approach="", created_by="u")
        spec = Spec(id="s-3", board_id="bt-3", title="S3",
                    description="", created_by="u")
        q = ConsolidationQueue(board_id="bt-3", artifact_type="spec",
                               artifact_id="s-3", status="pending",
                               priority="low", source="historical_backfill")
        db.add_all([board, idea, spec, q])
        await db.commit()

    async with factory() as db:
        result = await list_pending_tree(
            "bt-3", 5, actor=_actor("bt-3"), uow=SQLAlchemyUnitOfWork(db)
        )
    assert result["levels"]["specs"]["pending"] == 1
    assert result["total_pending"] == 1  # ideation=not_queued, spec=pending
