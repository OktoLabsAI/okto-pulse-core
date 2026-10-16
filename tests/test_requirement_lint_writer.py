from __future__ import annotations

from uuid import uuid4

from sqlalchemy import func, select

from okto_pulse.core.models.schemas import SpecCreate
from okto_pulse.core.services.main import SpecService
from sqlalchemy_test_models import Board, Spec, QualityAssessmentReceiptFixture


async def test_spec_creation_commits_without_automatic_lint(
    db_factory,
) -> None:
    board_id = f"board-lint-rollback-{uuid4().hex[:10]}"
    actor_id = "agent-lint-rollback"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint rollback", owner_id=actor_id, settings={}))
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
        created = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="External lint is independent",
                delivery_context="brownfield",
                functional_requirements=[{"text": "The API must return a result."}],
            ),
        )
        assert created is not None
        created_spec_id = created.id
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
        assert (
            await db.scalar(
                select(func.count()).select_from(Spec).where(Spec.board_id == board_id)
            )
            == 1
        )
        persisted = await db.get(Spec, created_spec_id)
        assert persisted is not None
        assert persisted.title == "External lint is independent"


async def test_create_spec_has_no_automatic_lint_route_coupling(
    db_factory,
) -> None:
    board_id = f"board-lint-route-{uuid4().hex[:10]}"
    actor_id = "agent-lint-route"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Lint route", owner_id=actor_id, settings={}))
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
        created = await SpecService(db).create_spec(
            board_id,
            actor_id,
            SpecCreate(
                title="External agent owns requirement lint",
                delivery_context="brownfield",
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
