"""Explicit assessment contracts and independent native mutation transactions."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select

from okto_pulse.core.domain.enums import (
    IdeationComplexity,
    IdeationStatus,
    RefinementStatus,
)
from okto_pulse.core.infra.permissions import get_builtin_presets, resolve_permissions
from okto_pulse.core.models.schemas import SpecCreate, SpecUpdate
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecService,
)
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
    StructuredSpecEntityService,
)
from r3_scenario_helpers import freeze_refinement_completion_fixture
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    DomainEventRow,
    Ideation,
    Refinement,
    Spec,
    SpecHistory,
    QualityAssessmentReceiptFixture,
)

ACTOR_ID = "ska-writer-matrix-agent"
NOW = datetime(2026, 7, 27, 16, 30, tzinfo=UTC)


async def _seed_board(db_factory, *, suffix: str) -> str:
    board_id = f"board-ska-writer-{suffix}-{uuid4().hex[:8]}"
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name=f"SK-A writer {suffix}",
                owner_id=ACTOR_ID,
                settings={},
            )
        )
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )
    return board_id


async def test_bulk_create_and_update_remain_independent_of_automatic_lint(
    db_factory,
) -> None:
    board_id = await _seed_board(db_factory, suffix="bulk")

    async with db_factory() as db:
        service = SpecService(db)
        created = await service.create_spec(
            board_id,
            ACTOR_ID,
            SpecCreate(
                title="Bulk create",
                delivery_context="brownfield",
                functional_requirements=[{"text": "FR bulk"}],
                technical_requirements=[{"text": "TR bulk"}],
                acceptance_criteria=[{"text": "AC bulk"}],
            ),
        )
        assert created is not None
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )

        updated = await service.update_spec(
            created.id,
            ACTOR_ID,
            SpecUpdate(
                title="Bulk update",
                technical_requirements=[{"text": "TR bulk updated"}],
            ),
        )
        assert updated is not None
        assert updated.title == "Bulk update"
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )


async def test_ideation_and_refinement_derivation_remain_independent_of_automatic_lint(
    db_factory,
) -> None:
    board_id = await _seed_board(db_factory, suffix="derivation")
    async with db_factory() as db:
        ideation_direct = Ideation(
            board_id=board_id,
            title="Direct ideation",
            description="Derive directly.",
            created_by=ACTOR_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.SMALL,
        )
        ideation_refined = Ideation(
            board_id=board_id,
            title="Refined ideation",
            description="Derive through refinement.",
            created_by=ACTOR_ID,
            status=IdeationStatus.DONE,
            complexity=IdeationComplexity.MEDIUM,
        )
        db.add_all((ideation_direct, ideation_refined))
        await db.flush()
        refinement = Refinement(
            board_id=board_id,
            ideation_id=ideation_refined.id,
            title="Completed refinement",
            description="Refinement snapshot.",
            created_by=ACTOR_ID,
            status=RefinementStatus.DONE,
            delivery_context="brownfield",
        )
        db.add(refinement)
        await db.flush()
        await freeze_refinement_completion_fixture(db, refinement)
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )
        direct_id = ideation_direct.id
        refinement_id = refinement.id

    async with db_factory() as db:
        direct_spec = await IdeationService(db).derive_spec(
            direct_id,
            ACTOR_ID,
            skip_ownership_check=True,
            delivery_context="brownfield",
        )
        assert direct_spec is not None
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )

    async with db_factory() as db:
        refined_spec = await RefinementService(db).derive_spec(
            refinement_id,
            ACTOR_ID,
            skip_ownership_check=True,
        )
        assert refined_spec is not None
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )


def _spec_writer_permissions():
    preset = next(
        preset for preset in get_builtin_presets() if preset["name"] == "Spec"
    )
    return resolve_permissions(None, preset["flags"], None)


async def test_structured_crud_remains_independent_of_automatic_lint(
    db_factory,
) -> None:
    board_id = await _seed_board(db_factory, suffix="structured")
    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            ACTOR_ID,
            SpecCreate(
                title="Structured writer",
                delivery_context="brownfield",
            ),
        )
        assert spec is not None
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )

        result = await StructuredSpecEntityService(db).mutate(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec.id,
                actor_id=ACTOR_ID,
                entity_type="technical_requirement",
                operation="create",
                payload={
                    "id": "tr_writer_matrix",
                    "text": "Persist through the structured writer.",
                },
                expected_spec_version=1,
                permission_set=_spec_writer_permissions(),
            )
        )
        assert result.success is True
        persisted = await db.get(Spec, spec.id)
        assert persisted is not None
        assert any(
            requirement.get("id") == "tr_writer_matrix"
            for requirement in persisted.technical_requirements
        )
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )


async def test_scenario_body_update_and_delete_remain_independent_of_automatic_lint(
    db_factory,
) -> None:
    board_id = await _seed_board(db_factory, suffix="scenario")
    async with db_factory() as db:
        service = SpecService(db)
        spec = await service.create_spec(
            board_id,
            ACTOR_ID,
            SpecCreate(
                title="Scenario writer",
                delivery_context="brownfield",
                test_scenarios=[
                    {
                        "id": "ts_writer_matrix",
                        "title": "Scenario",
                        "scenario_type": "integration",
                        "given": "a draft spec",
                        "when": "the scenario changes",
                        "then": "lint observes the final body",
                    }
                ],
            ),
        )
        assert spec is not None
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )

        await service.update_test_scenario(
            spec.id,
            ACTOR_ID,
            "ts_writer_matrix",
            given="an updated draft spec",
        )
        updated = await service.get_spec(spec.id)
        assert updated is not None
        assert updated.test_scenarios[0]["given"] == "an updated draft spec"
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )

        await service.delete_test_scenario(
            spec.id,
            ACTOR_ID,
            "ts_writer_matrix",
        )
        deleted = await service.get_spec(spec.id)
        assert deleted is not None
        assert deleted.test_scenarios == []
        await db.commit()
        assert (
            await db.scalar(
                select(func.count())
                .select_from(QualityAssessmentReceiptFixture)
                .where(QualityAssessmentReceiptFixture.board_id == board_id)
            )
            == 0
        )


async def _assert_no_spec_side_effects(
    db_factory,
    board_id: str,
    spec_id: str,
) -> None:
    async with db_factory() as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            == 0
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(SpecHistory)
                .where(SpecHistory.spec_id == spec_id)
            )
            == 0
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(ActivityLog)
                .where(ActivityLog.board_id == board_id)
            )
            == 0
        )
        assert (
            await db.scalar(
                select(func.count())
                .select_from(DomainEventRow)
                .where(DomainEventRow.board_id == board_id)
            )
            == 0
        )


async def test_outer_commit_fault_rolls_back_native_writer_effects(
    db_factory,
) -> None:
    board_id = await _seed_board(db_factory, suffix="fault-commit")

    class CommitFailure(RuntimeError):
        pass

    async with db_factory() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            ACTOR_ID,
            SpecCreate(
                title="Commit must fail",
                delivery_context="brownfield",
                functional_requirements=[{"text": "No partial commit."}],
            ),
        )
        assert spec is not None
        spec_id = spec.id

        def _fail_commit(_session) -> None:
            raise CommitFailure("fault_injected")

        event.listen(db.sync_session, "before_commit", _fail_commit)
        try:
            with pytest.raises(CommitFailure, match="fault_injected"):
                await db.commit()
        finally:
            event.remove(db.sync_session, "before_commit", _fail_commit)
            await db.rollback()

    await _assert_no_spec_side_effects(
        db_factory,
        board_id,
        spec_id,
    )
