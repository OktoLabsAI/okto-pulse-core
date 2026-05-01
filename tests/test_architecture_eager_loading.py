from __future__ import annotations

import uuid

import pytest
from sqlalchemy import inspect as sqlalchemy_inspect

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import (
    BoardResponse,
    IdeationResponse,
    IdeationSummary,
    RefinementResponse,
    RefinementSummary,
    SpecSummary,
)
from okto_pulse.core.services.main import (
    BoardService,
    IdeationService,
    RefinementService,
    SpecService,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _architecture_design(
    *,
    board_id: str,
    parent_type: str,
    parent_id: str,
    title: str,
    created_by: str,
) -> ArchitectureDesign:
    parent_field = {
        "ideation": "ideation_id",
        "refinement": "refinement_id",
        "spec": "spec_id",
        "card": "card_id",
    }[parent_type]
    return ArchitectureDesign(
        id=_id("arch"),
        board_id=board_id,
        parent_type=parent_type,
        title=title,
        global_description=f"{title} global description",
        entities=[],
        interfaces=[],
        diagrams=[],
        created_by=created_by,
        **{parent_field: parent_id},
    )


def _assert_loaded(instance, relationship_name: str) -> None:
    assert relationship_name not in sqlalchemy_inspect(instance).unloaded


@pytest.mark.asyncio
async def test_board_get_preloads_card_architecture_for_response_serialization(db_factory):
    user_id = _id("user")
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Board with card architecture", owner_id=user_id))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec",
                status=SpecStatus.APPROVED,
                created_by=user_id,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Task",
                status=CardStatus.NOT_STARTED,
                created_by=user_id,
            )
        )
        db.add(
            _architecture_design(
                board_id=board_id,
                parent_type="card",
                parent_id=card_id,
                title="Task Architecture",
                created_by=user_id,
            )
        )
        await db.commit()

    async with db_factory() as db:
        board = await BoardService(db).get_board(board_id, user_id)

        assert board is not None
        assert len(board.cards) == 1
        card = board.cards[0]
        _assert_loaded(card, "architecture_designs")
        assert card.architecture_designs[0].title == "Task Architecture"

        response = BoardResponse.model_validate(board)
        assert response.cards[0].architecture_designs[0].title == "Task Architecture"


@pytest.mark.asyncio
async def test_architecture_summary_relationships_are_preloaded(db_factory):
    user_id = _id("user")
    board_id = _id("board")
    ideation_id = _id("ideation")
    refinement_id = _id("refinement")
    spec_id = _id("spec")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Architecture summary loading", owner_id=user_id))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Ideation",
                status=IdeationStatus.DONE,
                created_by=user_id,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="Refinement",
                status=RefinementStatus.DONE,
                created_by=user_id,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                ideation_id=ideation_id,
                refinement_id=refinement_id,
                title="Spec",
                status=SpecStatus.APPROVED,
                created_by=user_id,
            )
        )
        for parent_type, parent_id in (
            ("ideation", ideation_id),
            ("refinement", refinement_id),
            ("spec", spec_id),
        ):
            db.add(
                _architecture_design(
                    board_id=board_id,
                    parent_type=parent_type,
                    parent_id=parent_id,
                    title=f"{parent_type.title()} Architecture",
                    created_by=user_id,
                )
            )
        await db.commit()

    async with db_factory() as db:
        ideations = await IdeationService(db).list_ideations(board_id)
        assert len(ideations) == 1
        _assert_loaded(ideations[0], "architecture_designs")
        assert IdeationSummary.model_validate(ideations[0]).architecture_designs

        refinements = await RefinementService(db).list_refinements(ideation_id)
        assert len(refinements) == 1
        _assert_loaded(refinements[0], "architecture_designs")
        RefinementSummary.model_validate(refinements[0])

        specs = await SpecService(db).list_specs(board_id)
        assert len(specs) == 1
        _assert_loaded(specs[0], "architecture_designs")
        assert SpecSummary.model_validate(specs[0]).architecture_designs

    async with db_factory() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        assert ideation is not None
        _assert_loaded(ideation.refinements[0], "architecture_designs")
        _assert_loaded(ideation.specs[0], "architecture_designs")
        response = IdeationResponse.model_validate(ideation)
        assert response.specs[0].architecture_designs

    async with db_factory() as db:
        refinement = await RefinementService(db).get_refinement(refinement_id)
        assert refinement is not None
        _assert_loaded(refinement.specs[0], "architecture_designs")
        response = RefinementResponse.model_validate(refinement)
        assert response.specs[0].architecture_designs
