"""CreateSpecUseCase resolves explicit lineage before the Spec FK write."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from okto_pulse.core.application.use_cases import CreateSpecCommand, CreateSpecUseCase
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import SpecCreate
from okto_pulse.core.services.effective_resource_propagation import (
    ResourceLineageResolutionError,
)
from sqlalchemy_test_models import Board, Ideation, Refinement, Spec
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory


USER_ID = "create-spec-lineage-user"


@pytest.fixture
async def lineage_graph() -> dict[str, str]:
    board_id = f"spec-lineage-a-{uuid.uuid4().hex[:8]}"
    foreign_board_id = f"spec-lineage-b-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add_all(
            (
                Board(
                    id=board_id,
                    name="Spec lineage A",
                    owner_id=USER_ID,
                    realm_id=LOCAL_REALM_ID,
                ),
                Board(
                    id=foreign_board_id,
                    name="Spec lineage B",
                    owner_id=USER_ID,
                    realm_id=LOCAL_REALM_ID,
                ),
            )
        )
        await db.flush()

        idea_a = Ideation(
            board_id=board_id,
            title="Idea A",
            created_by=USER_ID,
        )
        idea_a_other = Ideation(
            board_id=board_id,
            title="Idea A other",
            created_by=USER_ID,
        )
        idea_b = Ideation(
            board_id=foreign_board_id,
            title="Idea B",
            created_by=USER_ID,
        )
        db.add_all((idea_a, idea_a_other, idea_b))
        await db.flush()

        refinement_a = Refinement(
            board_id=board_id,
            ideation_id=idea_a.id,
            title="Refinement A",
            created_by=USER_ID,
        )
        refinement_a_other = Refinement(
            board_id=board_id,
            ideation_id=idea_a_other.id,
            title="Refinement A other",
            created_by=USER_ID,
        )
        refinement_b = Refinement(
            board_id=foreign_board_id,
            ideation_id=idea_b.id,
            title="Refinement B",
            created_by=USER_ID,
        )
        db.add_all((refinement_a, refinement_a_other, refinement_b))
        await db.flush()

        graph = {
            "board_id": board_id,
            "foreign_board_id": foreign_board_id,
            "idea_a": idea_a.id,
            "idea_a_other": idea_a_other.id,
            "idea_b": idea_b.id,
            "refinement_a": refinement_a.id,
            "refinement_a_other": refinement_a_other.id,
            "refinement_b": refinement_b.id,
        }
        await db.commit()
    return graph


async def _spec_count(board_id: str) -> int:
    async with get_session_factory()() as db:
        return int(
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            or 0
        )


async def _create_spec(
    board_id: str,
    *,
    ideation_id: str | None = None,
    refinement_id: str | None = None,
):
    actor = ActorContext(USER_ID, "rest", realm_id=LOCAL_REALM_ID)
    uow_factory = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    async with uow_factory(actor=actor) as uow:
        return await CreateSpecUseCase().execute(
            CreateSpecCommand(
                board_id,
                SpecCreate(
                    title=f"Lineage spec {uuid.uuid4().hex[:8]}",
                    ideation_id=ideation_id,
                    refinement_id=refinement_id,
                ),
            ),
            actor=actor,
            uow=uow,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("no_parent", "ideation", "refinement", "both"))
async def test_create_spec_valid_parent_matrix_persists(lineage_graph, case):
    parent_args = {
        "no_parent": {},
        "ideation": {"ideation_id": lineage_graph["idea_a"]},
        "refinement": {"refinement_id": lineage_graph["refinement_a"]},
        "both": {
            "ideation_id": lineage_graph["idea_a"],
            "refinement_id": lineage_graph["refinement_a"],
        },
    }[case]
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    result = await _create_spec(board_id, **parent_args)

    assert result.spec is not None
    assert result.spec.ideation_id == parent_args.get("ideation_id")
    assert result.spec.refinement_id == parent_args.get("refinement_id")
    assert await _spec_count(board_id) == before + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "missing_ideation",
        "missing_refinement",
        "cross_board_ideation",
        "cross_board_refinement",
        "parent_mismatch",
    ),
)
async def test_create_spec_invalid_parent_matrix_is_governed_and_writes_nothing(
    lineage_graph,
    case,
):
    parent_args = {
        "missing_ideation": {"ideation_id": "missing-ideation"},
        "missing_refinement": {"refinement_id": "missing-refinement"},
        "cross_board_ideation": {"ideation_id": lineage_graph["idea_b"]},
        "cross_board_refinement": {
            "refinement_id": lineage_graph["refinement_b"]
        },
        "parent_mismatch": {
            "ideation_id": lineage_graph["idea_a"],
            "refinement_id": lineage_graph["refinement_a_other"],
        },
    }[case]
    board_id = lineage_graph["board_id"]
    before = await _spec_count(board_id)

    with pytest.raises(ResourceLineageResolutionError) as raised:
        await _create_spec(board_id, **parent_args)

    assert raised.value.to_error_dict()["error"] == (
        "resource_lineage_resolution_failed"
    )
    assert await _spec_count(board_id) == before
