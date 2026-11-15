"""Pending planning questions block new authority, with native SQL persistence."""

from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from test_e2e043_source_context import native_source_factory as _native_source_factory
from okto_pulse.community.adapters.sqlalchemy_models import (
    Board,
    Ideation,
    IdeationQAItem,
    IdeationSnapshot,
    Refinement,
    RefinementQAItem,
    RefinementSnapshot,
)
from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import (
    bind_semantic_subject_actor,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import IdeationStatus, RefinementStatus
from okto_pulse.core.domain.pending_qa import (
    PendingQuestionsError,
    planning_qa_readiness,
)
from okto_pulse.core.models.schemas import (
    IdeationMove,
    RefinementMove,
    IdeationQAAnswer,
    RefinementQAAnswer,
    RefinementCreate,
)
from okto_pulse.core.services.main import (
    IdeationService,
    RefinementService,
    SpecService,
    IdeationQAService,
    RefinementQAService,
)
from okto_pulse.core.services.reference_resolution import (
    serialize_parent_ideation_context,
    compile_ideation_parent_context,
)

native_source_factory = _native_source_factory


def _bind(db, actor_kind="user"):
    bind_semantic_subject_actor(
        db, ActorContext("owner", "test", actor_kind=actor_kind, board_id="board")
    )


async def _seed(factory, subject_type, status, *, count=1, answered=False):
    async with factory() as db:
        _bind(db)
        db.add(
            Board(
                id="board",
                realm_id="local",
                name="Pending Q&A",
                owner_id="owner",
                settings={"allow_agent_self_answering": True},
            )
        )
        db.add(
            Ideation(
                id="idea",
                board_id="board",
                title="Intent",
                status=IdeationStatus(status)
                if subject_type == "ideation"
                else IdeationStatus.DONE,
                created_by="owner",
            )
        )
        if subject_type == "refinement":
            db.add(
                Refinement(
                    id="refinement",
                    ideation_id="idea",
                    board_id="board",
                    title="Design",
                    in_scope=["Pending questions"],
                    delivery_context="greenfield",
                    status=RefinementStatus(status),
                    created_by="owner",
                )
            )
        model, field, subject_id = (
            (IdeationQAItem, "ideation_id", "idea")
            if subject_type == "ideation"
            else (RefinementQAItem, "refinement_id", "refinement")
        )
        for i in range(count):
            db.add(
                model(
                    id=f"question-{i:03d}",
                    **{field: subject_id},
                    question="Which option?",
                    question_type="single_choice",
                    choices=[{"id": "recommended", "label": "Recommended"}],
                    selected=["recommended"] if answered else None,
                    asked_by="owner",
                    answered_by="owner" if answered else None,
                    answered_at=datetime.now(timezone.utc) if answered else None,
                )
            )
        await db.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("actor_kind", ["user", "agent"])
@pytest.mark.parametrize(
    "subject_type,before,after",
    [
        ("ideation", "review", "approved"),
        ("ideation", "approved", "evaluating"),
        ("ideation", "evaluating", "done"),
        ("refinement", "review", "approved"),
        ("refinement", "approved", "done"),
    ],
)
async def test_pending_questions_block_authority_before_mutation(
    native_source_factory, subject_type, before, after, actor_kind
):
    await _seed(native_source_factory, subject_type, before)
    async with native_source_factory() as db:
        _bind(db, actor_kind)
        with pytest.raises(PendingQuestionsError) as error:
            if subject_type == "ideation":
                await IdeationService(db).move_ideation(
                    "idea", "owner", IdeationMove(status=after), actor_name=actor_kind
                )
            else:
                await RefinementService(db).move_refinement(
                    "refinement",
                    "owner",
                    RefinementMove(status=after),
                    actor_name=actor_kind,
                )
        assert error.value.details["pending_ids"] == ["question-000"]
        await db.rollback()
    async with native_source_factory() as db:
        model, subject_id = (
            (Ideation, "idea")
            if subject_type == "ideation"
            else (Refinement, "refinement")
        )
        assert (await db.get(model, subject_id)).status.value == before
        assert not (await db.execute(select(IdeationSnapshot))).scalars().all()
        assert not (await db.execute(select(RefinementSnapshot))).scalars().all()


@pytest.mark.asyncio
async def test_diagnostics_bound_ids_but_keep_exact_pending_count(
    native_source_factory,
):
    await _seed(native_source_factory, "ideation", "review", count=62)
    async with native_source_factory() as db:
        _bind(db)
        with pytest.raises(PendingQuestionsError) as error:
            await IdeationService(db).move_ideation(
                "idea", "owner", IdeationMove(status="approved")
            )
        assert error.value.details["pending_count"] == 62
        assert len(error.value.details["pending_ids"]) == 50
        assert error.value.details["truncated"] is True
        assert error.value.details["next_action"]


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_type", ["ideation", "refinement"])
async def test_review_accepts_choice_answer_and_allows_approval(
    native_source_factory, subject_type
):
    await _seed(native_source_factory, subject_type, "review")
    async with native_source_factory() as db:
        _bind(db)
        if subject_type == "ideation":
            answer = await IdeationQAService(db).answer_question(
                "question-000", "owner", IdeationQAAnswer(selected=["recommended"])
            )
        else:
            answer = await RefinementQAService(db).answer_question(
                "question-000", "owner", RefinementQAAnswer(selected=["recommended"])
            )
        assert answer.answer is None
        assert answer.answered_at is not None
        await db.commit()
    async with native_source_factory() as db:
        _bind(db)
        if subject_type == "ideation":
            result = await IdeationService(db).move_ideation(
                "idea", "owner", IdeationMove(status="approved")
            )
        else:
            result = await RefinementService(db).move_refinement(
                "refinement", "owner", RefinementMove(status="approved")
            )
        assert result.status.value == "approved"


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_type", ["ideation", "refinement"])
async def test_entering_review_does_not_require_answers(
    native_source_factory, subject_type
):
    await _seed(native_source_factory, subject_type, "draft")
    async with native_source_factory() as db:
        _bind(db)
        if subject_type == "ideation":
            result = await IdeationService(db).move_ideation(
                "idea", "owner", IdeationMove(status="review")
            )
        else:
            result = await RefinementService(db).move_refinement(
                "refinement", "owner", RefinementMove(status="review")
            )
        assert result.status.value == "review"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route", ["ideation_spec", "refinement_create", "refinement_spec", "direct_spec"]
)
async def test_historical_pending_source_refuses_derivation_without_rewriting(
    native_source_factory, route
):
    subject_type = "refinement" if route == "refinement_spec" else "ideation"
    await _seed(native_source_factory, subject_type, "done")
    async with native_source_factory() as db:
        _bind(db)
        with pytest.raises(PendingQuestionsError) as error:
            if route == "ideation_spec":
                await IdeationService(db).derive_spec("idea", "owner")
            elif route == "refinement_create":
                await RefinementService(db).create_refinement(
                    "idea",
                    "owner",
                    RefinementCreate(
                        ideation_id="idea",
                        title="Child",
                        in_scope=["scope"],
                        delivery_context="greenfield",
                    ),
                    skip_ownership_check=True,
                )
            elif route == "refinement_spec":
                await RefinementService(db).derive_spec("refinement", "owner")
            else:
                await SpecService(db)._validate_lineage(
                    "board", ideation_id="idea", refinement_id=None
                )
        assert error.value.details["state"] == "historical_source_incomplete"
        assert "new edition" in error.value.details["next_action"]
        await db.rollback()
    async with native_source_factory() as db:
        model, subject_id = (
            (Ideation, "idea")
            if subject_type == "ideation"
            else (Refinement, "refinement")
        )
        assert (await db.get(model, subject_id)).status.value == "done"


def test_parent_context_never_drops_unanswered_or_promotes_proposed_answer():
    parent = SimpleNamespace(
        id="idea",
        status="done",
        title="History",
        qa_items=[
            SimpleNamespace(
                id="pending",
                question="Still open?",
                answer="Proposed by assessment",
                selected=None,
                answered_at=None,
            ),
            SimpleNamespace(
                id="choice",
                question="Choice?",
                answer=None,
                selected=["yes"],
                answered_at=datetime.now(timezone.utc),
            ),
        ],
    )
    projected = serialize_parent_ideation_context(parent)
    assert [q["id"] for q in projected["qa_items"]] == ["pending", "choice"]
    assert projected["qa_readiness"]["pending_ids"] == ["pending"]
    assert "Pending" in compile_ideation_parent_context(parent)
    assert "Proposed by assessment" in compile_ideation_parent_context(parent)


def test_native_mcp_and_rest_mapping_preserve_actionable_bounded_details():
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.community.inbound.rest_adapter import RESTAdapterContract

    source = SimpleNamespace(
        id="idea", status="review", qa_items=[{"id": "open", "answered_at": None}]
    )
    error = PendingQuestionsError(
        planning_qa_readiness(source, subject_type="ideation")
    )
    mcp = json.loads(MCPAdapterContract.error(error))
    rest = RESTAdapterContract.http_error(error)
    assert rest.status_code == 409
    assert mcp["details"] == rest.detail["details"] == error.details


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_type", ["ideation", "refinement"])
async def test_transition_preview_reports_the_same_pending_ids(
    native_source_factory, subject_type
):
    from okto_pulse.core.application.use_cases.allowed_transitions import (
        AllowedTransition,
        ListAllowedTransitionsUseCase,
    )

    await _seed(native_source_factory, subject_type, "review")
    async with native_source_factory() as db:
        service = (
            IdeationService(db) if subject_type == "ideation" else RefinementService(db)
        )
        entity = await (
            service.get_ideation("idea")
            if subject_type == "ideation"
            else service.get_refinement("refinement")
        )
        transition = await ListAllowedTransitionsUseCase()._preview_entity_transition(
            SimpleNamespace(),
            subject_type,
            entity,
            AllowedTransition(to_status="approved", label="Approved", gate="none"),
        )
        assert transition.blocked_reason.startswith("planning_pending_questions:")
        assert transition.blocked_facts["pending_ids"] == ["question-000"]


@pytest.mark.asyncio
async def test_inherited_snapshot_questions_keep_their_recorded_state(
    native_source_factory,
):
    from okto_pulse.core.application.artifact_propagation import propagate_artifacts
    from okto_pulse.core.ports.application_persistence import ApplicationRecord
    from okto_pulse.community.adapters.sqlalchemy_models import Spec, SpecQAItem

    await _seed(native_source_factory, "ideation", "done", answered=True)
    stamp = "2026-11-04T12:00:00+00:00"
    source = [
        {"question": "Open", "answer": None, "answered_at": None},
        {
            "question": "Unconfirmed historical answer",
            "answer": "Proposed",
            "created_at": stamp,
        },
        {
            "question": "Recorded choice",
            "question_type": "single_choice",
            "selected": ["yes"],
            "answered_at": stamp,
        },
    ]
    original = json.dumps(source, sort_keys=True)
    async with native_source_factory() as db:
        _bind(db)
        db.add(
            Spec(
                id="child-spec",
                board_id="board",
                title="Derived",
                created_by="owner",
                architecture_adoption={},
            )
        )
        await db.flush()
        count = await propagate_artifacts(
            db=db,
            source_mockups=[],
            source_qa_items=source,
            source_knowledge_bases=[],
            target_entity=ApplicationRecord(entity="spec", values={"id": "child-spec"}),
            target_kb_entity=None,
            user_id="owner",
        )
        await db.commit()
    async with native_source_factory() as db:
        rows = {q.question: q for q in (await db.execute(select(SpecQAItem))).scalars()}
        assert count["qa"] == len(rows) == 3
        assert rows["Open"].answered_at is None
        assert rows["Unconfirmed historical answer"].answered_at is None
        assert rows["Recorded choice"].answered_at is not None
        assert rows["Recorded choice"].answer is None
    assert json.dumps(source, sort_keys=True) == original
