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

        payload = await get_board_columns(board_id, user_id=USER, db=db)

    cards = payload["columns"][CardStatus.IN_PROGRESS.value]
    assert len(cards) == 1
    assert cards[0]["open_qa_count"] == 1
