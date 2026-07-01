"""Derivation-pending badge summary count projections.

The UI renders "Sem refinamento" and "Sem spec" from lightweight summary fields.
These tests keep the backend contract honest: only non-archived, non-cancelled
children count as active derivations, and nested refinement summaries get the
same active spec count used by list endpoints.
"""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.db import (
    Board,
    Ideation,
    IdeationComplexity,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import IdeationSummary, RefinementSummary
from okto_pulse.core.services.main import IdeationService, RefinementService

USER = "derivation-pending-badge-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _ideation(board_id: str, title: str) -> Ideation:
    return Ideation(
        id=_id("ideation"),
        board_id=board_id,
        title=title,
        status=IdeationStatus.DONE,
        complexity=IdeationComplexity.MEDIUM,
        created_by=USER,
    )


def _refinement(
    board_id: str,
    ideation_id: str,
    title: str,
    *,
    status: RefinementStatus = RefinementStatus.DRAFT,
    archived: bool = False,
) -> Refinement:
    return Refinement(
        id=_id("refinement"),
        board_id=board_id,
        ideation_id=ideation_id,
        title=title,
        status=status,
        archived=archived,
        created_by=USER,
    )


def _spec(
    board_id: str,
    ideation_id: str,
    refinement_id: str,
    title: str,
    *,
    status: SpecStatus = SpecStatus.DRAFT,
    archived: bool = False,
) -> Spec:
    return Spec(
        id=_id("spec"),
        board_id=board_id,
        ideation_id=ideation_id,
        refinement_id=refinement_id,
        title=title,
        status=status,
        archived=archived,
        functional_requirements=[],
        technical_requirements=[],
        acceptance_criteria=[],
        test_scenarios=[],
        business_rules=[],
        api_contracts=[],
        integration_requirements=[],
        observability_requirements=[],
        decisions=[],
        created_by=USER,
    )


@pytest.mark.asyncio
async def test_ideation_summary_counts_only_active_refinements() -> None:
    board_id = _id("board")
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Derivation counts", owner_id=USER))
        zero = _ideation(board_id, "zero")
        active = _ideation(board_id, "active")
        cancelled = _ideation(board_id, "cancelled")
        archived = _ideation(board_id, "archived")
        mixed = _ideation(board_id, "mixed")
        db.add_all([zero, active, cancelled, archived, mixed])
        db.add(_refinement(board_id, active.id, "active child"))
        db.add(
            _refinement(
                board_id,
                cancelled.id,
                "cancelled child",
                status=RefinementStatus.CANCELLED,
            )
        )
        db.add(_refinement(board_id, archived.id, "archived child", archived=True))
        db.add(_refinement(board_id, mixed.id, "mixed active child"))
        db.add(
            _refinement(
                board_id,
                mixed.id,
                "mixed cancelled child",
                status=RefinementStatus.CANCELLED,
            )
        )
        db.add(_refinement(board_id, mixed.id, "mixed archived child", archived=True))
        await db.commit()

        rows = await IdeationService(db).list_ideations(board_id)

    summaries = {row.title: IdeationSummary.model_validate(row) for row in rows}
    assert summaries["zero"].active_refinement_count == 0
    assert summaries["active"].active_refinement_count == 1
    assert summaries["cancelled"].active_refinement_count == 0
    assert summaries["archived"].active_refinement_count == 0
    assert summaries["mixed"].active_refinement_count == 1


@pytest.mark.asyncio
async def test_refinement_summary_counts_only_active_specs_in_list_and_nested_ideation() -> None:
    board_id = _id("board")
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Nested derivation counts", owner_id=USER))
        ideation = _ideation(board_id, "parent")
        db.add(ideation)
        zero = _refinement(board_id, ideation.id, "zero")
        active = _refinement(board_id, ideation.id, "active")
        cancelled = _refinement(board_id, ideation.id, "cancelled")
        archived = _refinement(board_id, ideation.id, "archived")
        mixed = _refinement(board_id, ideation.id, "mixed")
        db.add_all([zero, active, cancelled, archived, mixed])
        db.add(_spec(board_id, ideation.id, active.id, "active spec"))
        db.add(
            _spec(
                board_id,
                ideation.id,
                cancelled.id,
                "cancelled spec",
                status=SpecStatus.CANCELLED,
            )
        )
        db.add(_spec(board_id, ideation.id, archived.id, "archived spec", archived=True))
        db.add(_spec(board_id, ideation.id, mixed.id, "mixed active spec"))
        db.add(
            _spec(
                board_id,
                ideation.id,
                mixed.id,
                "mixed cancelled spec",
                status=SpecStatus.CANCELLED,
            )
        )
        db.add(_spec(board_id, ideation.id, mixed.id, "mixed archived spec", archived=True))
        await db.commit()

        listed = await RefinementService(db).list_refinements(ideation.id)
        nested = (await IdeationService(db).get_ideation(ideation.id)).refinements

    listed_summaries = {row.title: RefinementSummary.model_validate(row) for row in listed}
    nested_summaries = {row.title: RefinementSummary.model_validate(row) for row in nested}
    expected = {
        "zero": 0,
        "active": 1,
        "cancelled": 0,
        "archived": 0,
        "mixed": 1,
    }
    assert {title: item.active_spec_count for title, item in listed_summaries.items()} == expected
    assert {title: item.active_spec_count for title, item in nested_summaries.items()} == expected


@pytest.mark.asyncio
async def test_active_derivation_counts_return_after_archived_children_are_restored() -> None:
    board_id = _id("board")
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Restore derivation counts", owner_id=USER))
        ideation = _ideation(board_id, "restore parent")
        refinement = _refinement(board_id, ideation.id, "restored refinement", archived=True)
        db.add_all([ideation, refinement])
        spec = _spec(board_id, ideation.id, refinement.id, "restored spec", archived=True)
        db.add(spec)
        await db.commit()

        ideation_before = (await IdeationService(db).list_ideations(board_id))[0]
        refinement_before = await RefinementService(db).get_refinement(refinement.id)
        assert IdeationSummary.model_validate(ideation_before).active_refinement_count == 0
        assert RefinementSummary.model_validate(refinement_before).active_spec_count == 0

        refinement.archived = False
        spec.archived = False
        await db.commit()

        ideation_after = (await IdeationService(db).list_ideations(board_id))[0]
        refinement_after = (await RefinementService(db).list_refinements(ideation.id))[0]
        nested_refinement = (await IdeationService(db).get_ideation(ideation.id)).refinements[0]

    assert IdeationSummary.model_validate(ideation_after).active_refinement_count == 1
    assert RefinementSummary.model_validate(refinement_after).active_spec_count == 1
    assert RefinementSummary.model_validate(nested_refinement).active_spec_count == 1
