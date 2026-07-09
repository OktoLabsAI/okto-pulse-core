"""Open-Q&A-count + ideation scope-score badge projection tests.

The frontend list cards render:
  * an "N open Q&A" badge driven by ``<Summary>.open_qa_count`` on ideation /
    refinement / spec / sprint / card, and
  * Domains/Ambiguity/Dependencies score badges driven by
    ``IdeationSummary.scope_assessment`` after evaluation.

These tests prove the backend projection actually emits those fields and that
"open" is defined as ``answered_at IS NULL`` — the only correct predicate, since
a *choice* answer leaves ``answer`` NULL (it stores ``selected``) yet always sets
``answered_at``. Counting by ``answer IS NULL`` would wrongly mark answered choice
questions as still open.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from okto_pulse.core.api.boards import get_board_columns
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationComplexity,
    IdeationQAItem,
    IdeationStatus,
    QAItem,
    Spec,
    SpecQAItem,
    SpecStatus,
)
from okto_pulse.core.models.schemas import IdeationSummary, SpecSummary
from okto_pulse.core.repositories.sqlalchemy.unit_of_work import SQLAlchemyUnitOfWork
from okto_pulse.core.services.main import IdeationService, SpecService

USER = "open-qa-badge-user"


def _id() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_ideation_open_qa_count_counts_only_unanswered_and_exposes_scope():
    """list_ideations -> open_qa_count counts ONLY answered_at-null items.

    Three Q&A: one unanswered, one text-answered, one choice-answered (answer is
    NULL but answered_at set). Only the unanswered one is open, so count == 1.
    scope_assessment rides through to the summary projection unchanged.
    """
    board_id, ideation_id = _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="QA Badge Board", owner_id=USER))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Ideation with mixed Q&A",
                status=IdeationStatus.EVALUATING,
                version=1,
                scope_assessment={"domains": 4, "ambiguity": 2, "dependencies": 3},
                complexity=IdeationComplexity.LARGE,
                created_by=USER,
            )
        )
        # OPEN: never answered.
        db.add(
            IdeationQAItem(
                ideation_id=ideation_id,
                question="Still open?",
                question_type="text",
                asked_by=USER,
            )
        )
        # ANSWERED (text): answer + answered_at set.
        db.add(
            IdeationQAItem(
                ideation_id=ideation_id,
                question="Text answered?",
                question_type="text",
                answer="Yes",
                asked_by=USER,
                answered_by=USER,
                answered_at=_now(),
            )
        )
        # ANSWERED (choice): answer stays NULL, selected + answered_at set.
        db.add(
            IdeationQAItem(
                ideation_id=ideation_id,
                question="Choice answered?",
                question_type="single_choice",
                choices=[{"id": "a", "label": "A"}],
                selected=["a"],
                asked_by=USER,
                answered_by=USER,
                answered_at=_now(),
            )
        )
        await db.commit()

        rows = await IdeationService(db).list_ideations(board_id)

    assert len(rows) == 1
    ideation = rows[0]
    # answered_at-null predicate: only the first Q&A is open (choice answer counts
    # as answered even though its `answer` column is NULL).
    assert ideation.open_qa_count == 1

    summary = IdeationSummary.model_validate(ideation)
    assert summary.open_qa_count == 1
    assert summary.scope_assessment == {"domains": 4, "ambiguity": 2, "dependencies": 3}


@pytest.mark.asyncio
async def test_ideation_summary_scope_absent_and_zero_count_when_clean():
    """A never-evaluated ideation with no Q&A -> scope_assessment None, count 0."""
    board_id, ideation_id = _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Clean Board", owner_id=USER))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Fresh ideation",
                status=IdeationStatus.DRAFT,
                version=1,
                created_by=USER,
            )
        )
        await db.commit()

        rows = await IdeationService(db).list_ideations(board_id)

    summary = IdeationSummary.model_validate(rows[0])
    assert summary.open_qa_count == 0
    assert summary.scope_assessment is None


@pytest.mark.asyncio
async def test_spec_open_qa_count():
    """list_specs -> open_qa_count via the shared helper (answered_at IS NULL)."""
    board_id, spec_id = _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Spec QA Board", owner_id=USER))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec with Q&A",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1"],
                functional_requirements=["FR1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
                integration_requirements=[],
                observability_requirements=[],
                created_by=USER,
            )
        )
        db.add(
            SpecQAItem(
                spec_id=spec_id,
                question="Open spec question?",
                question_type="text",
                asked_by=USER,
            )
        )
        db.add(
            SpecQAItem(
                spec_id=spec_id,
                question="Answered spec question?",
                question_type="text",
                answer="Done",
                asked_by=USER,
                answered_by=USER,
                answered_at=_now(),
            )
        )
        await db.commit()

        rows = await SpecService(db).list_specs(board_id)

    summary = SpecSummary.model_validate(rows[0])
    assert summary.open_qa_count == 1


@pytest.mark.asyncio
async def test_card_columns_open_qa_count():
    """get_board_columns embeds open_qa_count per kanban card (inline count)."""
    board_id, card_id = _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Card QA Board", owner_id=USER))
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                title="Card with Q&A",
                status=CardStatus.IN_PROGRESS,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=USER,
            )
        )
        db.add(QAItem(card_id=card_id, question="Open?", asked_by=USER))
        db.add(
            QAItem(
                card_id=card_id,
                question="Answered?",
                answer="Yes",
                asked_by=USER,
                answered_by=USER,
                answered_at=_now(),
            )
        )
        await db.commit()

        payload = await get_board_columns(
            board_id,
            user_id=USER,
            realm_id=None,
            uow=SQLAlchemyUnitOfWork(db),
        )

    cards = payload["columns"][CardStatus.IN_PROGRESS.value]
    assert len(cards) == 1
    assert cards[0]["open_qa_count"] == 1


@pytest.mark.asyncio
async def test_inherited_answered_qa_does_not_inflate_open_qa_count():
    """Campo 2026-06-10: a herança de Q&A copiava resposta/seleção sem
    ``answered_at`` — e o badge define "aberta" como ``answered_at IS NULL``,
    então TODA Q&A respondida herdada virava falso-aberta no derivado (100%
    dos badges de refinements/specs do board 0.2.3 eram falsos)."""
    from okto_pulse.core.models.db import (
        Refinement,
        RefinementQAItem,
        RefinementStatus,
    )
    from okto_pulse.core.services.main import propagate_artifacts
    from sqlalchemy import select

    board_id, ideation_id, refinement_id = _id(), _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="QA Inherit Board", owner_id=USER))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Parent ideation",
                status=IdeationStatus.APPROVED,
                version=1,
                created_by=USER,
            )
        )
        answered_text = IdeationQAItem(
            ideation_id=ideation_id,
            question="Text answered on parent?",
            question_type="text",
            answer="Yes — decided on the parent.",
            asked_by=USER,
            answered_by=USER,
            answered_at=_now(),
        )
        answered_choice = IdeationQAItem(
            ideation_id=ideation_id,
            question="Choice answered on parent?",
            question_type="single_choice",
            choices=[{"id": "a", "label": "A"}],
            selected=["a"],
            asked_by=USER,
            answered_by=USER,
            answered_at=_now(),
        )
        open_qa = IdeationQAItem(
            ideation_id=ideation_id,
            question="Never answered — must NOT be copied.",
            question_type="text",
            asked_by=USER,
        )
        db.add_all([answered_text, answered_choice, open_qa])
        refinement = Refinement(
            id=refinement_id,
            ideation_id=ideation_id,
            board_id=board_id,
            title="Derived refinement",
            status=RefinementStatus.DRAFT,
            version=1,
            created_by=USER,
        )
        db.add(refinement)
        await db.flush()

        await propagate_artifacts(
            db,
            source_mockups=None,
            source_qa_items=[answered_text, answered_choice, open_qa],
            source_knowledge_bases=None,
            target_entity=refinement,
            target_kb_class=None,
            user_id=USER,
        )
        await db.commit()

        copied = (
            await db.execute(
                select(RefinementQAItem).where(
                    RefinementQAItem.refinement_id == refinement_id
                )
            )
        ).scalars().all()

    # Só as respondidas são copiadas, e TODAS chegam com answered_at.
    assert len(copied) == 2
    for qa in copied:
        assert qa.answered_at is not None, (
            f"Q&A herdada respondida sem answered_at: {qa.question!r} — "
            "volta a inflar o badge open_qa_count"
        )


@pytest.mark.asyncio
async def test_backfill_qa_answered_at_stamps_only_answered_rows():
    """O backfill do boot carimba answered_at APENAS nas linhas respondidas
    órfãs (answer OU selected presentes); abertas reais permanecem NULL.
    Segunda execução é no-op (idempotente)."""
    from okto_pulse.core.services.main import backfill_qa_answered_at
    from sqlalchemy import select

    board_id, spec_id = _id(), _id()
    db_factory = get_session_factory()
    async with db_factory() as db:
        db.add(Board(id=board_id, name="QA Backfill Board", owner_id=USER))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec with orphan answered QA",
                status=SpecStatus.DRAFT,
                version=1,
                created_by=USER,
            )
        )
        # Linhas pré-fix: respondidas mas sem answered_at (herança antiga).
        orphan_text = SpecQAItem(
            spec_id=spec_id,
            question="Inherited text answer, no timestamp",
            question_type="text",
            answer="Inherited answer",
            asked_by=USER,
            answered_by=USER,
        )
        orphan_choice = SpecQAItem(
            spec_id=spec_id,
            question="Inherited choice answer, no timestamp",
            question_type="single_choice",
            choices=[{"id": "a", "label": "A"}],
            selected=["a"],
            asked_by=USER,
            answered_by=USER,
        )
        genuinely_open = SpecQAItem(
            spec_id=spec_id,
            question="Really open — must stay open",
            question_type="text",
            asked_by=USER,
        )
        db.add_all([orphan_text, orphan_choice, genuinely_open])
        await db.commit()

        fixed = await backfill_qa_answered_at(db)
        assert fixed.get("spec_qa_items", 0) >= 2

        rows = (
            await db.execute(
                select(SpecQAItem).where(SpecQAItem.spec_id == spec_id)
            )
        ).scalars().all()
        by_q = {r.question: r for r in rows}
        assert by_q["Inherited text answer, no timestamp"].answered_at is not None
        assert by_q["Inherited choice answer, no timestamp"].answered_at is not None
        assert by_q["Really open — must stay open"].answered_at is None

        # Idempotência: nada mais a corrigir nesta spec.
        again = await backfill_qa_answered_at(db)
        assert again.get("spec_qa_items", 0) == 0 or "spec_qa_items" not in again
