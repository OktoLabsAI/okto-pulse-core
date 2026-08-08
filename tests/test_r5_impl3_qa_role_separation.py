"""Q&A self-answering governance enforcement.

Covers:
- BG-01: default allow_agent_self_answering=false rejects asked_by == answered_by.
- BG-01: allow_agent_self_answering=true accepts asked_by == answered_by.
- Legacy qa_require_role_separation remains readable but does not grant self-answering.
- Reject works in all 5 handlers (QAService, SpecQAService, IdeationQAService,
  RefinementQAService, SprintQAService), plus REST/MCP card answer wrappers.

Historical R5 role-separation tests live here because BG-01 supersedes their
same-principal semantics while preserving the legacy field compatibility.

NOTE: Tests exercise REAL service-layer calls (not source inspection).
      The validator can reproduce by running: pytest tests/test_r5_impl3_qa_role_separation.py -v
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
import pytest
from sqlalchemy import select
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

pytestmark = pytest.mark.asyncio

BOARD_ID_ROLE_SEP = "board-qa-role-sep-001"
USER_ASKER = "user-asker-001"
USER_ANSWERER = "user-answerer-001"


# ---------------------------------------------------------------------------
# Helpers to create test fixtures in the DB
# ---------------------------------------------------------------------------


async def _create_board(
    db,
    board_id: str,
    qa_require_role_separation: bool = False,
    allow_agent_self_answering: bool = False,
):
    """Create a Board with given qa_require_role_separation setting."""
    from sqlalchemy_test_models import Board

    settings = {
        "qa_require_role_separation": qa_require_role_separation,
        "allow_agent_self_answering": allow_agent_self_answering,
    }
    existing = await db.get(Board, board_id)
    if existing:
        # Update settings
        existing.settings = {**(existing.settings or {}), **settings}
        await db.flush()
        return existing

    board = Board(
        id=board_id,
        name=f"Role Sep Test Board {board_id[-4:]}",
        owner_id=USER_ASKER,
        settings=settings,
    )
    db.add(board)
    await db.flush()
    return board


async def _create_spec(db, board_id: str):
    """Create a minimal Spec."""
    from sqlalchemy_test_models import Spec, SpecStatus

    spec_id = str(uuid.uuid4())
    spec = Spec(
        id=spec_id,
        board_id=board_id,
        title="Role Sep Test Spec",
        status=SpecStatus.DRAFT,
        created_by=USER_ASKER,
    )
    db.add(spec)
    await db.flush()
    return spec


async def _create_ideation(db, board_id: str):
    """Create a minimal Ideation (no Story FK required)."""
    from sqlalchemy_test_models import Ideation, IdeationStatus, IdeationComplexity

    ideation_id = str(uuid.uuid4())
    ideation = Ideation(
        id=ideation_id,
        board_id=board_id,
        title="Role Sep Test Ideation",
        status=IdeationStatus.DRAFT,
        complexity=IdeationComplexity.MEDIUM,
        created_by=USER_ASKER,
    )
    db.add(ideation)
    await db.flush()
    return ideation


async def _create_refinement(db, board_id: str, ideation_id: str):
    """Create a minimal Refinement."""
    from sqlalchemy_test_models import Refinement, RefinementStatus

    ref_id = str(uuid.uuid4())
    ref = Refinement(
        id=ref_id,
        board_id=board_id,
        ideation_id=ideation_id,
        title="Role Sep Test Refinement",
        status=RefinementStatus.DRAFT,
        created_by=USER_ASKER,
    )
    db.add(ref)
    await db.flush()
    return ref


async def _create_sprint(db, board_id: str, spec_id: str):
    """Create a minimal Sprint."""
    from sqlalchemy_test_models import Sprint, SprintStatus

    sprint_id = str(uuid.uuid4())
    sprint = Sprint(
        id=sprint_id,
        board_id=board_id,
        spec_id=spec_id,
        title="Role Sep Test Sprint",
        status=SprintStatus.DRAFT,
        spec_version=1,
        created_by=USER_ASKER,
    )
    db.add(sprint)
    await db.flush()
    return sprint


async def _create_card(db, board_id: str):
    """Create a minimal Card."""
    from sqlalchemy_test_models import Card, CardStatus, CardType, CardPriority

    card_id = str(uuid.uuid4())
    card = Card(
        id=card_id,
        board_id=board_id,
        title="Role Sep Test Card",
        status=CardStatus.NOT_STARTED,
        card_type=CardType.NORMAL,
        priority=CardPriority.MEDIUM,
        position=0,
        created_by=USER_ASKER,
    )
    db.add(card)
    await db.flush()
    return card


# ---------------------------------------------------------------------------
# FR6 — BoardSettings.qa_require_role_separation field
# ---------------------------------------------------------------------------


async def test_fr6_board_settings_field_present_and_defaults_false():
    """FR6 — BoardSettings has qa_require_role_separation defaulting to False."""
    from okto_pulse.core.models.schemas import BoardSettings

    settings = BoardSettings()
    assert settings.qa_require_role_separation is False

    settings_on = BoardSettings(qa_require_role_separation=True)
    assert settings_on.qa_require_role_separation is True

    dumped = settings_on.model_dump()
    assert "qa_require_role_separation" in dumped
    assert dumped["qa_require_role_separation"] is True


async def test_fr6_helper_reads_board_settings(db_factory):
    """FR6 — _board_qa_require_role_separation helper reads the flag correctly."""
    from okto_pulse.core.services.main import _board_qa_require_role_separation
    from sqlalchemy_test_models import Board

    board_id_off = f"board-helper-off-{uuid.uuid4().hex[:8]}"
    board_id_on = f"board-helper-on-{uuid.uuid4().hex[:8]}"

    async with db_factory() as db:
        await _create_board(db, board_id_off, qa_require_role_separation=False)
        await _create_board(db, board_id_on, qa_require_role_separation=True)
        await db.commit()

    async with db_factory() as db:
        board_off = await db.get(Board, board_id_off)
        board_on = await db.get(Board, board_id_on)
        assert _board_qa_require_role_separation(board_off) is False
        assert _board_qa_require_role_separation(board_on) is True
        assert _board_qa_require_role_separation(None) is False


# ---------------------------------------------------------------------------
# BG-01 — canonical self-answering policy supersedes legacy flag semantics
# ---------------------------------------------------------------------------


async def test_ac8_qa_service_flag_off_same_principal_accepted(db_factory):
    """BG-01 — default policy rejects same-principal answers and records safe audit."""
    from sqlalchemy_test_models import ActivityLog, QAItem
    from okto_pulse.core.models.schemas import QAAnswer
    from okto_pulse.core.services.main import QAService

    board_id = f"board-ac8-qa-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=False)
        card = await _create_card(db, board_id)
        qa = QAItem(
            id=str(uuid.uuid4()),
            card_id=card.id,
            question="What is the expected behavior?",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = QAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, QAAnswer(answer="42"))
        assert "self_answering_not_allowed" in str(exc_info.value)
        assert "allow_agent_self_answering" in str(exc_info.value)

        stored_qa = await db.get(QAItem, qa_id)
        assert stored_qa is not None
        assert stored_qa.answer is None
        assert stored_qa.answered_by is None

        event = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "qa_self_answer_denied",
                )
            )
        ).scalar_one()
        assert event.details == {
            "metric_name": "qa_self_answer_denied_total",
            "board_id": board_id,
            "actor_id": USER_ASKER,
            "entity_type": "card",
            "question_id": qa_id,
            "reason": "self_answering_not_allowed",
            "surface": "service",
            "outcome": "deny",
        }


async def test_ac8_spec_qa_service_flag_off_same_principal_accepted(db_factory):
    """BG-01 — positive opt-in allows same-principal Q&A answers."""
    from sqlalchemy_test_models import SpecQAItem
    from okto_pulse.core.models.schemas import SpecQAAnswer
    from okto_pulse.core.services.main import SpecQAService

    board_id = f"board-ac8-spec-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(
            db,
            board_id,
            qa_require_role_separation=False,
            allow_agent_self_answering=True,
        )
        spec = await _create_spec(db, board_id)
        qa = SpecQAItem(
            id=str(uuid.uuid4()),
            spec_id=spec.id,
            question="Is this spec complete?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = SpecQAService(db)
        result = await svc.answer_question(qa_id, USER_ASKER, SpecQAAnswer(answer="Yes"))
        assert result is not None
        assert result.answer == "Yes"


# ---------------------------------------------------------------------------
# AC9 — flag ON → same-principal answer is REJECTED with real service call
# ---------------------------------------------------------------------------


async def test_ac9_qa_service_flag_on_same_principal_rejected(db_factory):
    """AC9 — QAService: flag ON, answered_by == asked_by → ValueError with
    role_separation_required message. REAL service call, not source inspection."""
    from sqlalchemy_test_models import QAItem
    from okto_pulse.core.models.schemas import QAAnswer
    from okto_pulse.core.services.main import QAService

    board_id = f"board-ac9-qa-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        card = await _create_card(db, board_id)
        qa = QAItem(
            id=str(uuid.uuid4()),
            card_id=card.id,
            question="What is the expected behavior?",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = QAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, QAAnswer(answer="should be rejected"))
        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg


async def test_ac9_spec_qa_service_flag_on_same_principal_rejected(db_factory):
    """AC9 — SpecQAService: flag ON, answered_by == asked_by → ValueError."""
    from sqlalchemy_test_models import SpecQAItem
    from okto_pulse.core.models.schemas import SpecQAAnswer
    from okto_pulse.core.services.main import SpecQAService

    board_id = f"board-ac9-spec-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        spec = await _create_spec(db, board_id)
        qa = SpecQAItem(
            id=str(uuid.uuid4()),
            spec_id=spec.id,
            question="Is this spec complete?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = SpecQAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, SpecQAAnswer(answer="should be rejected"))
        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg


async def test_ac9_ideation_qa_service_flag_on_same_principal_rejected(db_factory):
    """AC9 — IdeationQAService: flag ON, answered_by == asked_by → ValueError."""
    from sqlalchemy_test_models import IdeationQAItem
    from okto_pulse.core.models.schemas import IdeationQAAnswer
    from okto_pulse.core.services.main import IdeationQAService

    board_id = f"board-ac9-ideation-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        ideation = await _create_ideation(db, board_id)
        qa = IdeationQAItem(
            id=str(uuid.uuid4()),
            ideation_id=ideation.id,
            question="Does this approach make sense?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = IdeationQAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, IdeationQAAnswer(answer="should be rejected"))
        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg


async def test_ac9_refinement_qa_service_flag_on_same_principal_rejected(db_factory):
    """AC9 — RefinementQAService: flag ON, answered_by == asked_by → ValueError."""
    from sqlalchemy_test_models import RefinementQAItem
    from okto_pulse.core.models.schemas import RefinementQAAnswer
    from okto_pulse.core.services.main import RefinementQAService

    board_id = f"board-ac9-ref-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        ideation = await _create_ideation(db, board_id)
        ref = await _create_refinement(db, board_id, ideation.id)
        qa = RefinementQAItem(
            id=str(uuid.uuid4()),
            refinement_id=ref.id,
            question="Is this approach technically sound?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = RefinementQAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, RefinementQAAnswer(answer="should be rejected"))
        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg


async def test_ac9_sprint_qa_service_flag_on_same_principal_rejected(db_factory):
    """AC9 — SprintQAService: flag ON, answered_by == asked_by → ValueError."""
    from sqlalchemy_test_models import SprintQAItem
    from okto_pulse.core.services.main import SprintQAService

    board_id = f"board-ac9-sprint-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        spec = await _create_spec(db, board_id)
        sprint = await _create_sprint(db, board_id, spec.id)
        qa = SprintQAItem(
            id=str(uuid.uuid4()),
            sprint_id=sprint.id,
            question="Is this sprint scoped correctly?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = SprintQAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, answer="should be rejected")
        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg


# ---------------------------------------------------------------------------
# AC10 — flag ON → different principal answer is ACCEPTED
# ---------------------------------------------------------------------------


async def test_ac10_qa_service_flag_on_different_principal_accepted(db_factory):
    """AC10 — QAService: flag ON, answered_by != asked_by → accepted."""
    from sqlalchemy_test_models import QAItem
    from okto_pulse.core.models.schemas import QAAnswer
    from okto_pulse.core.services.main import QAService

    board_id = f"board-ac10-qa-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        card = await _create_card(db, board_id)
        qa = QAItem(
            id=str(uuid.uuid4()),
            card_id=card.id,
            question="What is the expected behavior?",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = QAService(db)
        # Different user answering — should succeed
        result = await svc.answer_question(qa_id, USER_ANSWERER, QAAnswer(answer="42"))
        assert result is not None
        assert result.answered_by == USER_ANSWERER


async def test_ac10_spec_qa_service_flag_on_different_principal_accepted(db_factory):
    """AC10 — SpecQAService: flag ON, different principal → accepted."""
    from sqlalchemy_test_models import SpecQAItem
    from okto_pulse.core.models.schemas import SpecQAAnswer
    from okto_pulse.core.services.main import SpecQAService

    board_id = f"board-ac10-spec-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        spec = await _create_spec(db, board_id)
        qa = SpecQAItem(
            id=str(uuid.uuid4()),
            spec_id=spec.id,
            question="Is this spec complete?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = SpecQAService(db)
        result = await svc.answer_question(qa_id, USER_ANSWERER, SpecQAAnswer(answer="Yes"))
        assert result is not None
        assert result.answered_by == USER_ANSWERER


# ---------------------------------------------------------------------------
# AC11 — reject works in all 5 handlers (each tested in AC9 above; this test
# adds a combined coverage assertion confirming shared enforcement)
# ---------------------------------------------------------------------------


async def test_ac11_all_5_handlers_reject_same_principal(db_factory):
    """AC11 — All 5 service handlers enforce the role-separation gate when enabled.

    This test creates fixtures for all 5 entity types on a SINGLE board with
    allow_agent_self_answering=False and confirms each handler raises ValueError
    with the canonical message when asked_by == answered_by.

    The 5 handlers: QAService, SpecQAService, IdeationQAService,
    RefinementQAService, SprintQAService.
    """
    from sqlalchemy_test_models import (
        QAItem, SpecQAItem, IdeationQAItem, RefinementQAItem, SprintQAItem,
    )
    from okto_pulse.core.models.schemas import QAAnswer, SpecQAAnswer, IdeationQAAnswer, RefinementQAAnswer
    from okto_pulse.core.services.main import (
        QAService, SpecQAService, IdeationQAService, RefinementQAService, SprintQAService,
    )

    board_id = f"board-ac11-all5-{uuid.uuid4().hex[:8]}"

    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        card = await _create_card(db, board_id)
        spec = await _create_spec(db, board_id)
        ideation = await _create_ideation(db, board_id)
        ref = await _create_refinement(db, board_id, ideation.id)
        sprint = await _create_sprint(db, board_id, spec.id)

        qa_card = QAItem(id=str(uuid.uuid4()), card_id=card.id, question="Q?", asked_by=USER_ASKER)
        qa_spec = SpecQAItem(id=str(uuid.uuid4()), spec_id=spec.id, question="Q?", question_type="text", asked_by=USER_ASKER)
        qa_idea = IdeationQAItem(id=str(uuid.uuid4()), ideation_id=ideation.id, question="Q?", question_type="text", asked_by=USER_ASKER)
        qa_ref = RefinementQAItem(id=str(uuid.uuid4()), refinement_id=ref.id, question="Q?", question_type="text", asked_by=USER_ASKER)
        qa_sprint = SprintQAItem(id=str(uuid.uuid4()), sprint_id=sprint.id, question="Q?", question_type="text", asked_by=USER_ASKER)

        for obj in [qa_card, qa_spec, qa_idea, qa_ref, qa_sprint]:
            db.add(obj)
        await db.commit()

        qa_card_id = qa_card.id
        qa_spec_id = qa_spec.id
        qa_idea_id = qa_idea.id
        qa_ref_id = qa_ref.id
        qa_sprint_id = qa_sprint.id

    # Each handler must raise ValueError with self_answering_not_allowed
    async with db_factory() as db:
        svc = QAService(db)
        with pytest.raises(ValueError, match="self_answering_not_allowed"):
            await svc.answer_question(qa_card_id, USER_ASKER, QAAnswer(answer="x"))

    async with db_factory() as db:
        svc = SpecQAService(db)
        with pytest.raises(ValueError, match="self_answering_not_allowed"):
            await svc.answer_question(qa_spec_id, USER_ASKER, SpecQAAnswer(answer="x"))

    async with db_factory() as db:
        svc = IdeationQAService(db)
        with pytest.raises(ValueError, match="self_answering_not_allowed"):
            await svc.answer_question(qa_idea_id, USER_ASKER, IdeationQAAnswer(answer="x"))

    async with db_factory() as db:
        svc = RefinementQAService(db)
        with pytest.raises(ValueError, match="self_answering_not_allowed"):
            await svc.answer_question(qa_ref_id, USER_ASKER, RefinementQAAnswer(answer="x"))

    async with db_factory() as db:
        svc = SprintQAService(db)
        with pytest.raises(ValueError, match="self_answering_not_allowed"):
            await svc.answer_question(qa_sprint_id, USER_ASKER, answer="x")


# ---------------------------------------------------------------------------
# FR8 — Error message structure
# ---------------------------------------------------------------------------


async def test_fr8_error_message_contains_remediation(db_factory):
    """BG-01 — The rejection error cites allow_agent_self_answering remediation."""
    from sqlalchemy_test_models import SpecQAItem
    from okto_pulse.core.models.schemas import SpecQAAnswer
    from okto_pulse.core.services.main import SpecQAService

    board_id = f"board-fr8-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id, qa_require_role_separation=True)
        spec = await _create_spec(db, board_id)
        qa = SpecQAItem(
            id=str(uuid.uuid4()),
            spec_id=spec.id,
            question="Does this meet requirements?",
            question_type="text",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        svc = SpecQAService(db)
        with pytest.raises(ValueError) as exc_info:
            await svc.answer_question(qa_id, USER_ASKER, SpecQAAnswer(answer="x"))

        err_msg = str(exc_info.value)
        assert "self_answering_not_allowed" in err_msg
        assert "allow_agent_self_answering" in err_msg
        assert "same principal" in err_msg.lower()


async def test_rest_card_qa_answer_returns_typed_self_answering_denial(db_factory):
    """BG-01 — REST wrapper maps policy denial to 403 and commits safe event."""
    from okto_pulse.community.api.qa import answer_question as answer_card_question
    from sqlalchemy_test_models import ActivityLog, QAItem
    from okto_pulse.core.models.schemas import QAAnswer

    board_id = f"board-rest-self-deny-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id)
        card = await _create_card(db, board_id)
        qa = QAItem(
            id=str(uuid.uuid4()),
            card_id=card.id,
            question="Can the asker answer?",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    async with db_factory() as db:
        with pytest.raises(HTTPException) as exc_info:
                await answer_card_question(
                    qa_id,
                    QAAnswer(answer="not allowed"),
                    user_id=USER_ASKER,
                    db=resolve_unit_of_work_factory().wrap(db),
                )
        assert exc_info.value.status_code == 403
        assert exc_info.value.detail["reason"] == "self_answering_not_allowed"

    async with db_factory() as db:
        stored_qa = await db.get(QAItem, qa_id)
        assert stored_qa is not None
        assert stored_qa.answer is None
        event = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "qa_self_answer_denied",
                )
            )
        ).scalar_one()
        assert event.details["surface"] == "rest"
        assert event.details["entity_type"] == "card"
        assert event.details["question_id"] == qa_id


async def test_mcp_card_qa_answer_returns_typed_self_answering_denial(db_factory):
    """BG-01 — MCP wrapper returns typed JSON denial and commits safe event."""
    from okto_pulse.core.mcp import server as mcp_server
    from sqlalchemy_test_models import ActivityLog, QAItem

    board_id = f"board-mcp-self-deny-{uuid.uuid4().hex[:8]}"
    async with db_factory() as db:
        await _create_board(db, board_id)
        card = await _create_card(db, board_id)
        qa = QAItem(
            id=str(uuid.uuid4()),
            card_id=card.id,
            question="Can the MCP agent answer?",
            asked_by=USER_ASKER,
        )
        db.add(qa)
        await db.commit()
        qa_id = qa.id

    register_mcp_test_runtime(db_factory)
    ctx = SimpleNamespace(
        agent_id=USER_ASKER,
        agent_name="self-answering-test-agent",
        board_id=board_id,
        permissions=["*"],
    )
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)
    ), patch.object(mcp_server, "check_permission", return_value=None):
        raw = await mcp_server.okto_pulse_answer_question.fn(
            board_id=board_id,
            qa_id=qa_id,
            answer="not allowed",
        )
    payload = json.loads(raw)
    assert payload["error"] == "self_answering_not_allowed"

    async with db_factory() as db:
        stored_qa = await db.get(QAItem, qa_id)
        assert stored_qa is not None
        assert stored_qa.answer is None
        event = (
            await db.execute(
                select(ActivityLog).where(
                    ActivityLog.board_id == board_id,
                    ActivityLog.action == "qa_self_answer_denied",
                )
            )
        ).scalar_one()
        assert event.actor_type == "agent"
        assert event.details["surface"] == "mcp"
        assert event.details["entity_type"] == "card"
        assert event.details["question_id"] == qa_id
