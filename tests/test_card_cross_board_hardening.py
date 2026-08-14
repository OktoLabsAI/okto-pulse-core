"""Cross-board security matrix for the remaining MCP card relation flows."""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select, text

from mcp_runtime_testing import register_mcp_test_runtime
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.card_crud import (
    DeleteTaskValidationCommand,
    DeleteTaskValidationUseCase,
    GetCardActivityCommand,
    GetCardActivityUseCase,
    GetCardSeenStatusCommand,
    GetCardSeenStatusUseCase,
)
from okto_pulse.core.application.use_cases.mcp_card_crud import (
    McpCopyKnowledgeToCardCommand,
    McpCopyKnowledgeToCardUseCase,
    McpCreateCardCommand,
    McpCreateCardUseCase,
    McpDeleteCardCommand,
    McpDeleteCardUseCase,
    McpGetCardCommand,
    McpGetCardUseCase,
    McpMoveCardCommand,
    McpMoveCardUseCase,
    McpUpdateCardCommand,
    McpUpdateCardUseCase,
)
from okto_pulse.core.application.use_cases.mcp_collaboration import (
    McpAnswerQuestionCommand,
    McpAnswerQuestionUseCase,
    McpDeleteCommentCommand,
    McpDeleteCommentUseCase,
    McpDeleteAttachmentCommand,
    McpDeleteAttachmentUseCase,
    McpDeleteQuestionCommand,
    McpDeleteQuestionUseCase,
    McpGetChoiceResponsesCommand,
    McpGetChoiceResponsesUseCase,
    McpRespondToChoiceCommand,
    McpRespondToChoiceUseCase,
    McpUploadAttachmentCommand,
    McpUploadAttachmentUseCase,
)
from okto_pulse.core.domain.enums import (
    BugSeverity,
    CardType,
    SprintLaneType,
    SprintStatus,
)
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgePropagationScope,
    register_knowledge_propagation_port,
    reset_knowledge_propagation_port_for_tests,
)
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Attachment,
    Board,
    Card,
    CardDependency,
    Comment,
    QAItem,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
    Sprint,
)
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory


class _LegacyKnowledgeScopePort:
    def __init__(self, *, v2_active: bool = False):
        self.v2_active = v2_active

    async def load_scope(self, _context, request):
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=1 if self.v2_active else 0,
            v2_active=self.v2_active,
            selection_state=("omitted" if self.v2_active else None),
        )


@pytest.fixture(autouse=True)
def _register_legacy_knowledge_scope_port():
    register_knowledge_propagation_port(_LegacyKnowledgeScopePort())
    try:
        yield
    finally:
        reset_knowledge_propagation_port_for_tests()


USER_ID = "card-cross-board-agent"


@pytest.fixture
async def _graph(db_factory):
    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board_a": f"card-scope-a-{suffix}",
        "board_b": f"card-scope-b-{suffix}",
        "spec_a": f"card-scope-spec-a-{suffix}",
        "spec_b": f"card-scope-spec-b-{suffix}",
        "source_a": f"card-scope-source-a-{suffix}",
        "target_a": f"card-scope-target-a-{suffix}",
        "target_b": f"card-scope-target-b-{suffix}",
        "local_edge": f"card-scope-local-edge-{suffix}",
        "foreign_edge": f"card-scope-foreign-edge-{suffix}",
        "kb_a": f"card-scope-kb-a-{suffix}",
        "kb_b": f"card-scope-kb-b-{suffix}",
        "comment_a": f"card-scope-comment-a-{suffix}",
        "choice_a": f"card-scope-choice-a-{suffix}",
        "qa_a": f"card-scope-qa-a-{suffix}",
        "attachment_a": f"card-scope-attachment-a-{suffix}",
        "validation_a": f"card-scope-validation-a-{suffix}",
        "fr_a": f"fr-a-{suffix}",
        "fr_b": f"fr-b-{suffix}",
    }

    async with db_factory() as db:
        db.add_all(
            [
                Board(id=ids["board_a"], name="Card scope A", owner_id=USER_ID),
                Board(id=ids["board_b"], name="Card scope B", owner_id=USER_ID),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Spec(
                    id=ids["spec_a"],
                    board_id=ids["board_a"],
                    title="Card scope spec A",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=[
                        {
                            "id": ids["fr_a"],
                            "text": "Scoped requirement A",
                            "linked_task_ids": [],
                        }
                    ],
                    acceptance_criteria=[],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
                Spec(
                    id=ids["spec_b"],
                    board_id=ids["board_b"],
                    title="Card scope spec B",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=[
                        {
                            "id": ids["fr_b"],
                            "text": "Scoped requirement B",
                            "linked_task_ids": [],
                        }
                    ],
                    acceptance_criteria=[],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Card(
                    id=ids["source_a"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a"],
                    title="Scoped source A",
                    created_by=USER_ID,
                    validations=[
                        {
                            "id": ids["validation_a"],
                            "reviewer_id": "reviewer-a",
                            "recommendation": "approve",
                        }
                    ],
                ),
                Card(
                    id=ids["target_a"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a"],
                    title="Scoped target A",
                    created_by=USER_ID,
                ),
                Card(
                    id=ids["target_b"],
                    board_id=ids["board_b"],
                    spec_id=ids["spec_b"],
                    title="SECRET foreign target B",
                    created_by=USER_ID,
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                CardDependency(
                    id=ids["local_edge"],
                    card_id=ids["source_a"],
                    depends_on_id=ids["target_a"],
                ),
                # Deliberately corrupt legacy edge: the database FK checks IDs,
                # not board co-tenancy. Readers/writers must still contain it.
                CardDependency(
                    id=ids["foreign_edge"],
                    card_id=ids["source_a"],
                    depends_on_id=ids["target_b"],
                ),
                SpecKnowledgeBase(
                    id=ids["kb_a"],
                    spec_id=ids["spec_a"],
                    title="Knowledge A",
                    content="Scoped knowledge A",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                SpecKnowledgeBase(
                    id=ids["kb_b"],
                    spec_id=ids["spec_b"],
                    title="Knowledge B",
                    content="SECRET scoped knowledge B",
                    mime_type="text/markdown",
                    created_by=USER_ID,
                ),
                Comment(
                    id=ids["comment_a"],
                    card_id=ids["source_a"],
                    content="SECRET scoped comment A",
                    author_id=USER_ID,
                ),
                Comment(
                    id=ids["choice_a"],
                    card_id=ids["source_a"],
                    content="SECRET scoped choice A",
                    author_id=USER_ID,
                    comment_type="choice",
                    choices=[{"id": "opt_0", "label": "Scoped option"}],
                    responses=[],
                    allow_free_text=True,
                ),
                QAItem(
                    id=ids["qa_a"],
                    card_id=ids["source_a"],
                    question="SECRET scoped question A",
                    asked_by="different-asker",
                ),
                Attachment(
                    id=ids["attachment_a"],
                    card_id=ids["source_a"],
                    filename="secret-a.txt",
                    original_filename="secret-a.txt",
                    mime_type="text/plain",
                    size=8,
                    path=f"scope/{suffix}/secret-a.txt",
                    uploaded_by=USER_ID,
                ),
            ]
        )
        await db.commit()
    return ids


async def _call(db_factory, tool_name: str, **kwargs) -> dict:
    board_id = kwargs["board_id"]
    ctx = type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["*"],
        },
    )()
    register_mcp_test_runtime(db_factory)
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=ctx)),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        tool = await mcp_server.mcp.get_tool(tool_name)
        return json.loads(await tool.fn(**kwargs))


async def _edge_ids(db_factory, source_id: str) -> set[str]:
    async with db_factory() as db:
        rows = await db.execute(
            select(CardDependency.id).where(CardDependency.card_id == source_id)
        )
        return set(rows.scalars().all())


async def _traceability_activity_count(db_factory, card_ids: list[str]) -> int:
    async with db_factory() as db:
        rows = await db.execute(
            select(ActivityLog.id).where(
                ActivityLog.card_id.in_(card_ids),
                ActivityLog.action == "card_traceability_linked",
            )
        )
        return len(rows.scalars().all())


async def _activity_ids(db_factory, card_id: str) -> list[str]:
    async with db_factory() as db:
        rows = await db.execute(
            select(ActivityLog.id).where(ActivityLog.card_id == card_id)
        )
        return list(rows.scalars().all())


async def _card_child_snapshot(db_factory, ids: dict[str, str]) -> dict:
    async with db_factory() as db:
        qa_rows = await db.execute(
            select(QAItem.id).where(QAItem.card_id == ids["source_a"])
        )
        comment_rows = await db.execute(
            select(Comment.id).where(Comment.card_id == ids["source_a"])
        )
        activity_rows = await db.execute(
            select(ActivityLog.id).where(ActivityLog.card_id == ids["source_a"])
        )
        card = await db.get(Card, ids["source_a"])
        qa = await db.get(QAItem, ids["qa_a"])
        comment = await db.get(Comment, ids["comment_a"])
        choice = await db.get(Comment, ids["choice_a"])
        attachment_rows = await db.execute(
            select(Attachment.id).where(Attachment.card_id == ids["source_a"])
        )
        return {
            "qa_ids": list(qa_rows.scalars().all()),
            "qa_answer": qa.answer if qa else None,
            "comment_ids": list(comment_rows.scalars().all()),
            "comment_content": comment.content if comment else None,
            "choice_responses": list(choice.responses or []) if choice else None,
            "activity_ids": list(activity_rows.scalars().all()),
            "validations": list(card.validations or []),
            "attachment_ids": list(attachment_rows.scalars().all()),
        }


@pytest.mark.asyncio
async def test_dependencies_hide_foreign_source_and_corrupt_foreign_edges(
    db_factory, _graph
):
    ids = _graph
    foreign_source = await _call(
        db_factory,
        "okto_pulse_get_card_dependencies",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    assert foreign_source == {"error": "Card not found"}
    missing_source = await _call(
        db_factory,
        "okto_pulse_get_card_dependencies",
        board_id=ids["board_a"],
        card_id=f"missing-{uuid.uuid4().hex}",
    )
    assert missing_source == foreign_source

    local_source = await _call(
        db_factory,
        "okto_pulse_get_card_dependencies",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )
    assert [item["id"] for item in local_source["depends_on"]] == [ids["target_a"]]
    assert local_source["blocking_titles"] == ["Scoped target A"]
    assert "SECRET" not in json.dumps(local_source)


@pytest.mark.asyncio
async def test_remove_dependency_contains_source_and_target_without_enumeration(
    db_factory, _graph
):
    ids = _graph
    before = await _edge_ids(db_factory, ids["source_a"])

    foreign_source = await _call(
        db_factory,
        "okto_pulse_remove_card_dependency",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        depends_on_id=ids["target_a"],
    )
    assert foreign_source == {"error": "Card not found"}
    missing_source = await _call(
        db_factory,
        "okto_pulse_remove_card_dependency",
        board_id=ids["board_a"],
        card_id=f"missing-{uuid.uuid4().hex}",
        depends_on_id=ids["target_a"],
    )
    assert missing_source == foreign_source

    foreign_target = await _call(
        db_factory,
        "okto_pulse_remove_card_dependency",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        depends_on_id=ids["target_b"],
    )
    missing_target = await _call(
        db_factory,
        "okto_pulse_remove_card_dependency",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        depends_on_id=f"missing-{uuid.uuid4().hex}",
    )
    assert foreign_target == missing_target == {"success": False}
    assert await _edge_ids(db_factory, ids["source_a"]) == before

    same_board = await _call(
        db_factory,
        "okto_pulse_remove_card_dependency",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        depends_on_id=ids["target_a"],
    )
    assert same_board == {"success": True}
    assert await _edge_ids(db_factory, ids["source_a"]) == {ids["foreign_edge"]}


@pytest.mark.asyncio
async def test_copy_knowledge_requires_source_and_target_on_command_board(
    db_factory, _graph
):
    ids = _graph
    matrix = (
        (
            ids["board_a"],
            f"missing-{uuid.uuid4().hex}",
            ids["source_a"],
            "Spec not found",
        ),
        (
            ids["board_a"],
            ids["spec_a"],
            f"missing-{uuid.uuid4().hex}",
            "Card not found",
        ),
        (ids["board_b"], ids["spec_a"], ids["target_b"], "Spec not found"),
        (ids["board_a"], ids["spec_b"], ids["source_a"], "Spec not found"),
        (ids["board_a"], ids["spec_a"], ids["target_b"], "Card not found"),
    )
    for board_id, spec_id, card_id, error in matrix:
        result = await _call(
            db_factory,
            "okto_pulse_copy_knowledge_to_card",
            board_id=board_id,
            spec_id=spec_id,
            card_id=card_id,
        )
        assert result == {"error": error}

    async with db_factory() as db:
        assert (await db.get(Card, ids["source_a"])).knowledge_bases is None
        assert (await db.get(Card, ids["target_b"])).knowledge_bases is None

    same_board = await _call(
        db_factory,
        "okto_pulse_copy_knowledge_to_card",
        board_id=ids["board_a"],
        spec_id=ids["spec_a"],
        card_id=ids["source_a"],
    )
    assert same_board["success"] is True
    assert same_board["copied"] == 1


@pytest.mark.asyncio
async def test_copy_knowledge_returns_stable_v2_legacy_write_error(
    db_factory,
    _graph,
):
    ids = _graph
    register_knowledge_propagation_port(_LegacyKnowledgeScopePort(v2_active=True))

    result = await _call(
        db_factory,
        "okto_pulse_copy_knowledge_to_card",
        board_id=ids["board_a"],
        spec_id=ids["spec_a"],
        card_id=ids["source_a"],
    )

    assert result["error"] == "knowledge_propagation_legacy_write_forbidden"
    assert result["code"] == "knowledge_propagation_legacy_write_forbidden"
    assert result["retryable"] is False
    async with db_factory() as db:
        assert (await db.get(Card, ids["source_a"])).knowledge_bases is None


@pytest.mark.asyncio
async def test_copy_knowledge_rejects_actor_command_board_mismatch(db_factory, _graph):
    ids = _graph
    actor = ActorContext(USER_ID, "mcp", board_id=ids["board_b"])
    uow_factory = SQLAlchemyUnitOfWorkFactory(db_factory)
    with pytest.raises(EntityNotFoundError) as exc_info:
        async with uow_factory(actor=actor) as uow:
            await McpCopyKnowledgeToCardUseCase().execute(
                McpCopyKnowledgeToCardCommand(
                    ids["board_a"], ids["spec_a"], ids["source_a"], None
                ),
                actor=actor,
                uow=uow,
            )
    assert exc_info.value.entity_type == "spec"
    async with db_factory() as db:
        assert (await db.get(Card, ids["source_a"])).knowledge_bases is None


@pytest.mark.asyncio
async def test_traceability_requires_spec_card_and_actor_board_with_zero_audit(
    db_factory, _graph
):
    ids = _graph
    matrix = (
        (
            ids["board_a"],
            f"missing-{uuid.uuid4().hex}",
            ids["source_a"],
            "Spec not found",
        ),
        (
            ids["board_a"],
            ids["spec_a"],
            f"missing-{uuid.uuid4().hex}",
            "Card not found",
        ),
        (ids["board_b"], ids["spec_a"], ids["target_b"], "Spec not found"),
        (ids["board_a"], ids["spec_b"], ids["source_a"], "Spec not found"),
        (ids["board_a"], ids["spec_a"], ids["target_b"], "Card not found"),
    )
    for board_id, spec_id, card_id, error in matrix:
        result = await _call(
            db_factory,
            "okto_pulse_link_task",
            board_id=board_id,
            target_type="fr",
            target_id=ids["fr_a"] if spec_id == ids["spec_a"] else ids["fr_b"],
            card_id=card_id,
            spec_id=spec_id,
        )
        assert result == {"error": error}

    async with db_factory() as db:
        spec_a = await db.get(Spec, ids["spec_a"])
        spec_b = await db.get(Spec, ids["spec_b"])
        assert spec_a.functional_requirements[0]["linked_task_ids"] == []
        assert spec_b.functional_requirements[0]["linked_task_ids"] == []
    assert (
        await _traceability_activity_count(
            db_factory, [ids["source_a"], ids["target_b"]]
        )
        == 0
    )

    same_board = await _call(
        db_factory,
        "okto_pulse_link_task",
        board_id=ids["board_a"],
        target_type="fr",
        target_id=ids["fr_a"],
        card_id=ids["source_a"],
        spec_id=ids["spec_a"],
    )
    assert same_board["success"] is True
    async with db_factory() as db:
        spec_a = await db.get(Spec, ids["spec_a"])
        assert spec_a.functional_requirements[0]["linked_task_ids"] == [ids["source_a"]]
    assert await _traceability_activity_count(db_factory, [ids["source_a"]]) == 1


@pytest.mark.asyncio
async def test_card_children_cross_board_are_not_read_written_or_audited(
    db_factory, _graph
):
    ids = _graph
    before = await _card_child_snapshot(db_factory, ids)

    ask = await _call(
        db_factory,
        "okto_pulse_ask_question",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        question="foreign question",
    )
    add_comment = await _call(
        db_factory,
        "okto_pulse_add_comment",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        content="foreign comment",
    )
    list_comments = await _call(
        db_factory,
        "okto_pulse_list_comments",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    list_attachments = await _call(
        db_factory,
        "okto_pulse_list_attachments",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    delete_attachment = await _call(
        db_factory,
        "okto_pulse_delete_attachment",
        board_id=ids["board_b"],
        attachment_id=ids["attachment_a"],
    )

    assert ask == {"error": "Failed to create question (card not found)"}
    assert add_comment == {"error": "Failed to create comment (card not found)"}
    assert list_comments == {"error": "Card not found"}
    assert list_attachments == {"error": "Card not found"}
    assert delete_attachment == {"error": "Attachment not found"}
    assert await _card_child_snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_qa_choice_comment_and_upload_children_are_parent_card_scoped(
    db_factory, _graph
):
    ids = _graph
    before = await _card_child_snapshot(db_factory, ids)

    answer = await _call(
        db_factory,
        "okto_pulse_answer_question",
        board_id=ids["board_b"],
        qa_id=ids["qa_a"],
        answer="must not reach foreign Q&A",
    )
    missing_answer = await _call(
        db_factory,
        "okto_pulse_answer_question",
        board_id=ids["board_b"],
        qa_id=f"missing-{uuid.uuid4().hex}",
        answer="missing",
    )
    delete_question = await _call(
        db_factory,
        "okto_pulse_delete_question",
        board_id=ids["board_b"],
        qa_id=ids["qa_a"],
    )
    missing_delete_question = await _call(
        db_factory,
        "okto_pulse_delete_question",
        board_id=ids["board_b"],
        qa_id=f"missing-{uuid.uuid4().hex}",
    )
    add_choice = await _call(
        db_factory,
        "okto_pulse_add_choice_comment",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        question="must not create a foreign choice",
        options=["one"],
    )
    respond = await _call(
        db_factory,
        "okto_pulse_respond_to_choice",
        board_id=ids["board_b"],
        comment_id=ids["choice_a"],
        selected=["opt_0"],
    )
    missing_respond = await _call(
        db_factory,
        "okto_pulse_respond_to_choice",
        board_id=ids["board_b"],
        comment_id=f"missing-{uuid.uuid4().hex}",
        selected=["opt_0"],
    )
    responses = await _call(
        db_factory,
        "okto_pulse_get_choice_responses",
        board_id=ids["board_b"],
        comment_id=ids["choice_a"],
    )
    missing_responses = await _call(
        db_factory,
        "okto_pulse_get_choice_responses",
        board_id=ids["board_b"],
        comment_id=f"missing-{uuid.uuid4().hex}",
    )
    update = await _call(
        db_factory,
        "okto_pulse_update_comment",
        board_id=ids["board_b"],
        comment_id=ids["comment_a"],
        content="must not update foreign comment",
    )
    missing_update = await _call(
        db_factory,
        "okto_pulse_update_comment",
        board_id=ids["board_b"],
        comment_id=f"missing-{uuid.uuid4().hex}",
        content="missing",
    )
    delete_comment = await _call(
        db_factory,
        "okto_pulse_delete_comment",
        board_id=ids["board_b"],
        comment_id=ids["comment_a"],
    )
    missing_delete_comment = await _call(
        db_factory,
        "okto_pulse_delete_comment",
        board_id=ids["board_b"],
        comment_id=f"missing-{uuid.uuid4().hex}",
    )
    with patch(
        "okto_pulse.core.services.main.get_storage_provider"
    ) as storage_provider:
        upload = await _call(
            db_factory,
            "okto_pulse_upload_attachment",
            board_id=ids["board_b"],
            card_id=ids["source_a"],
            filename="must-not-save.txt",
            content_base64="bm90IHNhdmVk",
        )
    storage_provider.assert_not_called()

    assert (
        answer == missing_answer == {"error": "Failed to answer question (not found)"}
    )
    assert delete_question == missing_delete_question == {"error": "Q&A item not found"}
    assert add_choice == {"error": "Failed to create choice comment (card not found)"}
    assert (
        respond
        == missing_respond
        == {"error": "Choice comment not found or invalid selection"}
    )
    assert responses == missing_responses == {"error": "Choice comment not found"}
    assert (
        update
        == missing_update
        == {"error": "Comment not found or not owned by this agent"}
    )
    assert (
        delete_comment
        == missing_delete_comment
        == {"error": "Comment not found or not owned by this agent"}
    )
    assert upload == {"error": "Failed to upload attachment (card not found)"}
    assert "SECRET" not in json.dumps(
        [
            answer,
            delete_question,
            add_choice,
            respond,
            responses,
            update,
            delete_comment,
            upload,
        ]
    )
    assert await _card_child_snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_qa_and_comment_child_flows_keep_same_board_success_envelopes(
    db_factory, _graph
):
    ids = _graph

    answer = await _call(
        db_factory,
        "okto_pulse_answer_question",
        board_id=ids["board_a"],
        qa_id=ids["qa_a"],
        answer="same-board answer",
    )
    update = await _call(
        db_factory,
        "okto_pulse_update_comment",
        board_id=ids["board_a"],
        comment_id=ids["comment_a"],
        content="same-board update",
    )
    add_choice = await _call(
        db_factory,
        "okto_pulse_add_choice_comment",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        question="same-board choice",
        options=["ship", "wait"],
    )
    new_choice_id = add_choice["comment"]["id"]
    respond = await _call(
        db_factory,
        "okto_pulse_respond_to_choice",
        board_id=ids["board_a"],
        comment_id=new_choice_id,
        selected=["opt_0"],
    )
    responses = await _call(
        db_factory,
        "okto_pulse_get_choice_responses",
        board_id=ids["board_a"],
        comment_id=new_choice_id,
    )
    delete_comment = await _call(
        db_factory,
        "okto_pulse_delete_comment",
        board_id=ids["board_a"],
        comment_id=ids["comment_a"],
    )
    delete_question = await _call(
        db_factory,
        "okto_pulse_delete_question",
        board_id=ids["board_a"],
        qa_id=ids["qa_a"],
    )

    assert answer["success"] is True
    assert answer["qa"]["answer"] == "same-board answer"
    assert update["success"] is True
    assert update["comment"]["content"] == "same-board update"
    assert add_choice["success"] is True
    assert respond["success"] is True
    assert responses["response_count"] == 1
    assert responses["responses"][0]["selected"] == ["opt_0"]
    assert delete_comment == {"success": True}
    assert delete_question == {"success": True}

    async with db_factory() as db:
        assert await db.get(Comment, ids["comment_a"]) is None
        assert await db.get(QAItem, ids["qa_a"]) is None


@pytest.mark.asyncio
async def test_task_reads_and_validation_are_card_scoped_before_child_access(
    db_factory, _graph
):
    ids = _graph
    before = await _card_child_snapshot(db_factory, ids)

    context = await _call(
        db_factory,
        "okto_pulse_get_task_context",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    conclusions = await _call(
        db_factory,
        "okto_pulse_get_task_conclusions",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    validations = await _call(
        db_factory,
        "okto_pulse_list_task_validations",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
    )
    validation = await _call(
        db_factory,
        "okto_pulse_get_task_validation",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        validation_id=ids["validation_a"],
    )
    submit = await _call(
        db_factory,
        "okto_pulse_submit_task_validation",
        board_id=ids["board_b"],
        card_id=ids["source_a"],
        expected_subject_version=1,
        idempotency_key="cross-board-validation-must-not-write",
        confidence=90,
        confidence_justification="Scoped validation confidence",
        estimated_completeness=90,
        completeness_justification="Scoped validation completeness",
        estimated_drift=5,
        drift_justification="Scoped validation drift",
        general_justification="Must not reach the foreign card",
        recommendation="approve",
    )

    for payload in (context, conclusions, validations, validation, submit):
        assert payload == {"error": "Card not found"}
        assert "SECRET" not in json.dumps(payload)
    assert await _card_child_snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_create_card_foreign_spec_matches_missing_and_writes_nothing(
    db_factory, _graph
):
    ids = _graph
    before = await _card_child_snapshot(db_factory, ids)
    async with db_factory() as db:
        cards_before = len((await db.execute(select(Card.id))).scalars().all())

    foreign = await _call(
        db_factory,
        "okto_pulse_create_card",
        board_id=ids["board_a"],
        title="Must not exist",
        spec_id=ids["spec_b"],
    )
    missing_id = f"missing-{uuid.uuid4().hex}"
    missing = await _call(
        db_factory,
        "okto_pulse_create_card",
        board_id=ids["board_a"],
        title="Must not exist either",
        spec_id=missing_id,
    )
    assert foreign == {"error": f"Spec '{ids['spec_b']}' not found"}
    assert missing == {"error": f"Spec '{missing_id}' not found"}

    async with db_factory() as db:
        cards_after = len((await db.execute(select(Card.id))).scalars().all())
    assert cards_after == cards_before
    assert await _card_child_snapshot(db_factory, ids) == before


@pytest.mark.asyncio
async def test_mcp_card_use_cases_reject_actor_command_board_spoof_before_services():
    cards = SimpleNamespace(
        create_card=AsyncMock(),
        get_card=AsyncMock(),
        update_card=AsyncMock(),
        move_card=AsyncMock(),
        delete_card=AsyncMock(),
    )
    specs = SimpleNamespace(get_spec=AsyncMock())
    uow = SimpleNamespace(services=SimpleNamespace(cards=cards, specs=specs))
    actor = ActorContext(USER_ID, "mcp", board_id="board-b")

    with pytest.raises(ValueError, match="Board not found"):
        await McpCreateCardUseCase().execute(
            McpCreateCardCommand("board-a", "spec-a", object(), None, {}),
            actor=actor,
            uow=uow,
        )

    matrix = (
        (McpGetCardUseCase(), McpGetCardCommand("card-a", "board-a")),
        (
            McpUpdateCardUseCase(),
            McpUpdateCardCommand("card-a", "board-a", object(), {}),
        ),
        (McpMoveCardUseCase(), McpMoveCardCommand("card-a", "board-a", object())),
        (McpDeleteCardUseCase(), McpDeleteCardCommand("card-a", "board-a")),
    )
    for use_case, command in matrix:
        with pytest.raises(EntityNotFoundError):
            await use_case.execute(command, actor=actor, uow=uow)

    specs.get_spec.assert_not_awaited()
    cards.create_card.assert_not_awaited()
    cards.get_card.assert_not_awaited()
    cards.update_card.assert_not_awaited()
    cards.move_card.assert_not_awaited()
    cards.delete_card.assert_not_awaited()


@pytest.mark.asyncio
async def test_collaboration_commands_reject_actor_command_board_spoof_before_children():
    cards = SimpleNamespace(get_card=AsyncMock())
    qa = SimpleNamespace(
        get_question=AsyncMock(),
        answer_question=AsyncMock(),
        delete_question=AsyncMock(),
    )
    comments = SimpleNamespace(
        get_comment=AsyncMock(),
        respond_to_choice=AsyncMock(),
        delete_comment=AsyncMock(),
    )
    attachments = SimpleNamespace(upload_attachment=AsyncMock())
    uow = SimpleNamespace(
        services=SimpleNamespace(
            cards=cards,
            qa=qa,
            comments=comments,
            attachments=attachments,
        ),
        commit=AsyncMock(),
    )
    actor = ActorContext(USER_ID, "mcp", board_id="board-b")

    answer = await McpAnswerQuestionUseCase().execute(
        McpAnswerQuestionCommand("board-a", "qa-a", "answer"),
        actor=actor,
        uow=uow,
    )
    delete_question = await McpDeleteQuestionUseCase().execute(
        McpDeleteQuestionCommand("board-a", "qa-a"),
        actor=actor,
        uow=uow,
    )
    respond = await McpRespondToChoiceUseCase().execute(
        McpRespondToChoiceCommand("board-a", "comment-a", ["opt_0"], ""),
        actor=actor,
        uow=uow,
    )
    responses = await McpGetChoiceResponsesUseCase().execute(
        McpGetChoiceResponsesCommand("board-a", "comment-a"),
        actor=actor,
        uow=uow,
    )
    delete_comment = await McpDeleteCommentUseCase().execute(
        McpDeleteCommentCommand("board-a", "comment-a"),
        actor=actor,
        uow=uow,
    )
    upload = await McpUploadAttachmentUseCase().execute(
        McpUploadAttachmentCommand(
            "board-a",
            "card-a",
            "proof.txt",
            b"proof",
            "text/plain",
        ),
        actor=actor,
        uow=uow,
    )

    assert answer.payload == {"error": "Failed to answer question (not found)"}
    assert delete_question.payload == {"error": "Q&A item not found"}
    assert respond.payload == {"error": "Choice comment not found or invalid selection"}
    assert responses.payload == {"error": "Choice comment not found"}
    assert delete_comment.payload == {
        "error": "Comment not found or not owned by this agent"
    }
    assert upload.payload == {"error": "Failed to upload attachment (card not found)"}
    cards.get_card.assert_not_awaited()
    qa.get_question.assert_not_awaited()
    qa.answer_question.assert_not_awaited()
    qa.delete_question.assert_not_awaited()
    comments.get_comment.assert_not_awaited()
    comments.respond_to_choice.assert_not_awaited()
    comments.delete_comment.assert_not_awaited()
    attachments.upload_attachment.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_board_card_children_keep_legacy_success_envelopes(
    db_factory, _graph
):
    ids = _graph
    ask = await _call(
        db_factory,
        "okto_pulse_ask_question",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        question="same-board question",
    )
    add_comment = await _call(
        db_factory,
        "okto_pulse_add_comment",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        content="same-board comment",
    )
    comments = await _call(
        db_factory,
        "okto_pulse_list_comments",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )
    attachments = await _call(
        db_factory,
        "okto_pulse_list_attachments",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )
    validations = await _call(
        db_factory,
        "okto_pulse_list_task_validations",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )
    validation = await _call(
        db_factory,
        "okto_pulse_get_task_validation",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        validation_id=ids["validation_a"],
    )
    conclusions = await _call(
        db_factory,
        "okto_pulse_get_task_conclusions",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )

    assert ask["success"] is True
    assert add_comment["success"] is True
    assert {item["id"] for item in comments} >= {ids["comment_a"]}
    assert [item["id"] for item in attachments] == [ids["attachment_a"]]
    assert validations["total"] == 1
    assert validation["id"] == ids["validation_a"]
    for projected in (*validations["validations"], validation):
        assert {"response", "request_digest", "idempotency_key"}.isdisjoint(projected)
    assert conclusions["id"] == ids["source_a"]


@pytest.mark.asyncio
async def test_mcp_task_validation_submit_list_and_get_hide_ledger_plumbing(
    db_factory,
    _graph,
) -> None:
    from sqlalchemy_test_models import CardStatus

    ids = _graph
    async with db_factory() as db:
        card = await db.get(Card, ids["source_a"])
        assert card is not None
        card.status = CardStatus.VALIDATION
        card.validations = []
        await db.commit()
        await db.refresh(card)
        expected_subject_version = int(card.policy_version)

    submitted = await _call(
        db_factory,
        "okto_pulse_submit_task_validation",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        expected_subject_version=expected_subject_version,
        idempotency_key=f"mcp-public-projection-{ids['source_a']}",
        confidence=90,
        confidence_justification="Independent review established high confidence.",
        estimated_completeness=95,
        completeness_justification="All required implementation behavior is present.",
        estimated_drift=2,
        drift_justification="Only the explicitly approved implementation choices differ.",
        general_justification="The reviewed implementation satisfies the task contract.",
        recommendation="reject",
    )
    listed = await _call(
        db_factory,
        "okto_pulse_list_task_validations",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
    )
    fetched = await _call(
        db_factory,
        "okto_pulse_get_task_validation",
        board_id=ids["board_a"],
        card_id=ids["source_a"],
        validation_id=submitted["id"],
    )

    assert listed["total"] == 1
    for projected in (submitted, *listed["validations"], fetched):
        assert {"response", "request_digest", "idempotency_key"}.isdisjoint(projected)
        assert projected["id"] == submitted["id"]
        assert projected["validation_outcome"] == "failed"
        assert projected["completion_outcome"] == "rejected"


@pytest.mark.asyncio
async def test_delete_attachment_same_board_still_calls_writer_and_commits():
    card = SimpleNamespace(id="card-a", board_id="board-a")
    attachment = SimpleNamespace(id="attachment-a", card_id=card.id)
    attachments = SimpleNamespace(
        get_attachment=AsyncMock(return_value=attachment),
        delete_attachment=AsyncMock(return_value=True),
    )
    cards = SimpleNamespace(get_card=AsyncMock(return_value=card))
    uow = SimpleNamespace(
        services=SimpleNamespace(
            attachments=attachments,
            cards=cards,
            boards=SimpleNamespace(_log_activity=AsyncMock()),
        ),
        commit=AsyncMock(),
    )
    actor = ActorContext(USER_ID, "mcp", board_id="board-a")

    result = await McpDeleteAttachmentUseCase().execute(
        McpDeleteAttachmentCommand("board-a", attachment.id),
        actor=actor,
        uow=uow,
    )

    assert result.payload == {"success": True}
    attachments.delete_attachment.assert_awaited_once_with(attachment.id)
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_upload_attachment_preloads_scoped_card_before_storage_writer():
    card = SimpleNamespace(id="card-a", board_id="board-a")
    attachment = SimpleNamespace(
        id="attachment-a",
        original_filename="proof.txt",
        mime_type="text/plain",
        size=5,
    )
    cards = SimpleNamespace(get_card=AsyncMock(return_value=card))
    attachments = SimpleNamespace(upload_attachment=AsyncMock(return_value=attachment))
    uow = SimpleNamespace(
        services=SimpleNamespace(
            cards=cards,
            attachments=attachments,
            boards=SimpleNamespace(_log_activity=AsyncMock()),
        ),
        commit=AsyncMock(),
    )
    foreign_actor = ActorContext(USER_ID, "mcp", board_id="board-b")
    command = McpUploadAttachmentCommand(
        "board-a",
        card.id,
        "proof.txt",
        b"proof",
        "text/plain",
    )

    foreign = await McpUploadAttachmentUseCase().execute(
        command,
        actor=foreign_actor,
        uow=uow,
    )
    assert foreign.payload == {"error": "Failed to upload attachment (card not found)"}
    attachments.upload_attachment.assert_not_awaited()
    uow.commit.assert_not_awaited()

    same_board_actor = ActorContext(USER_ID, "mcp", board_id="board-a")
    same_board = await McpUploadAttachmentUseCase().execute(
        command,
        actor=same_board_actor,
        uow=uow,
    )
    assert same_board.payload["success"] is True
    attachments.upload_attachment.assert_awaited_once()
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_task_validation_checks_actor_card_scope_before_writer():
    card = SimpleNamespace(id="card-a", board_id="board-a")
    cards = SimpleNamespace(
        get_card=AsyncMock(return_value=card),
        get_task_validation=AsyncMock(return_value={"id": "validation-a"}),
        delete_task_validation=AsyncMock(return_value=True),
    )
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(id="board-a"))
        ),
        services=SimpleNamespace(cards=cards),
        commit=AsyncMock(),
    )
    command = DeleteTaskValidationCommand(card.id, "validation-a")

    with pytest.raises(EntityNotFoundError):
        await DeleteTaskValidationUseCase().execute(
            command,
            actor=ActorContext(USER_ID, "mcp", board_id="board-b"),
            uow=uow,
        )
    cards.delete_task_validation.assert_not_awaited()
    uow.commit.assert_not_awaited()

    cards.get_card.return_value = None
    with pytest.raises(ValueError, match="Card not found"):
        await DeleteTaskValidationUseCase().execute(
            command,
            actor=ActorContext(USER_ID, "rest"),
            uow=uow,
        )
    cards.delete_task_validation.assert_not_awaited()

    cards.get_card.return_value = card
    await DeleteTaskValidationUseCase().execute(
        command,
        actor=ActorContext(USER_ID, "mcp", board_id="board-a"),
        uow=uow,
    )
    cards.delete_task_validation.assert_awaited_once_with(
        card.id,
        "validation-a",
        USER_ID,
    )
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_card_activity_and_seen_scope_parent_before_aggregate_readers():
    card = SimpleNamespace(id="card-a", board_id="board-a")
    cards = SimpleNamespace(get_card=AsyncMock(return_value=card))
    compute_activity = AsyncMock(return_value=["activity-a"])
    compute_seen = AsyncMock(return_value={"items": {"comment-a": []}})
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(id="board-a"))
        ),
        services=SimpleNamespace(
            cards=cards,
            compute_card_activity=compute_activity,
            compute_card_seen_status=compute_seen,
        ),
    )
    foreign_actor = ActorContext(USER_ID, "mcp", board_id="board-b")

    with pytest.raises(EntityNotFoundError):
        await GetCardActivityUseCase().execute(
            GetCardActivityCommand(card.id),
            actor=foreign_actor,
            uow=uow,
        )
    with pytest.raises(EntityNotFoundError):
        await GetCardSeenStatusUseCase().execute(
            GetCardSeenStatusCommand(card.id),
            actor=foreign_actor,
            uow=uow,
        )
    compute_activity.assert_not_awaited()
    compute_seen.assert_not_awaited()

    cards.get_card.return_value = None
    with pytest.raises(EntityNotFoundError):
        await GetCardActivityUseCase().execute(
            GetCardActivityCommand("missing"),
            actor=foreign_actor,
            uow=uow,
        )
    with pytest.raises(EntityNotFoundError):
        await GetCardSeenStatusUseCase().execute(
            GetCardSeenStatusCommand("missing"),
            actor=foreign_actor,
            uow=uow,
        )
    compute_activity.assert_not_awaited()
    compute_seen.assert_not_awaited()

    cards.get_card.return_value = card
    same_board_actor = ActorContext(USER_ID, "mcp", board_id="board-a")
    activity = await GetCardActivityUseCase().execute(
        GetCardActivityCommand(card.id, limit=7),
        actor=same_board_actor,
        uow=uow,
    )
    seen = await GetCardSeenStatusUseCase().execute(
        GetCardSeenStatusCommand(card.id),
        actor=same_board_actor,
        uow=uow,
    )
    assert activity.activity == ["activity-a"]
    assert seen.data == {"items": {"comment-a": []}}
    compute_activity.assert_awaited_once_with(card.id, limit=7)
    compute_seen.assert_awaited_once_with(card.id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "foreign_keys_enabled",
    [True, False],
    ids=["fresh-fk", "legacy-no-fk-enforcement"],
)
async def test_delete_bug_referenced_by_hotfix_is_governed_zero_write(
    db_factory, _graph, foreign_keys_enabled
):
    ids = _graph
    bug_id = f"card-scope-bug-{uuid.uuid4().hex[:8]}"
    sprint_id = f"card-scope-hotfix-{uuid.uuid4().hex[:8]}"
    if not foreign_keys_enabled:
        async with db_factory() as db:
            await db.execute(text("PRAGMA foreign_keys=OFF"))
            assert await db.scalar(text("PRAGMA foreign_keys")) == 0
            await db.commit()

    try:
        await _assert_hotfix_origin_blocks_delete(
            db_factory,
            ids,
            bug_id=bug_id,
            sprint_id=sprint_id,
        )
    finally:
        if not foreign_keys_enabled:
            async with db_factory() as db:
                await db.execute(text("PRAGMA foreign_keys=ON"))
                assert await db.scalar(text("PRAGMA foreign_keys")) == 1
                await db.commit()


async def _assert_hotfix_origin_blocks_delete(
    db_factory,
    ids: dict[str, str],
    *,
    bug_id: str,
    sprint_id: str,
) -> None:
    await _seed_hotfix_origin(
        db_factory,
        ids,
        bug_id=bug_id,
        sprint_id=sprint_id,
    )

    before_activity = await _activity_ids(db_factory, bug_id)
    result = await _call(
        db_factory,
        "okto_pulse_delete_card",
        board_id=ids["board_a"],
        card_id=bug_id,
    )

    assert result["error"] == "hotfix_origin_bug_delete_conflict"
    assert result["code"] == "hotfix_origin_bug_delete_conflict"
    assert result["referencing_sprint_ids"] == [sprint_id]
    assert result["next_action"] == "remove_or_relineage_hotfix_before_bug_delete"
    assert "relineage" in result["remediation"]
    async with db_factory() as db:
        assert await db.get(Card, bug_id) is not None
        sprint = await db.get(Sprint, sprint_id)
        assert sprint.origin_bug_id == bug_id
    assert await _activity_ids(db_factory, bug_id) == before_activity


async def _seed_hotfix_origin(
    db_factory,
    ids: dict[str, str],
    *,
    bug_id: str,
    sprint_id: str,
) -> None:
    async with db_factory() as db:
        db.add(
            Card(
                id=bug_id,
                board_id=ids["board_a"],
                spec_id=ids["spec_a"],
                title="Bug that anchors a hotfix",
                created_by=USER_ID,
                card_type=CardType.BUG,
                origin_task_id=ids["target_a"],
                severity=BugSeverity.MAJOR,
                expected_behavior="Expected",
                observed_behavior="Observed",
            )
        )
        await db.flush()
        db.add(
            Sprint(
                id=sprint_id,
                board_id=ids["board_a"],
                spec_id=ids["spec_a"],
                title="Hotfix anchored to bug",
                spec_version=1,
                status=SprintStatus.DRAFT,
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=bug_id,
                created_by=USER_ID,
            )
        )
        await db.commit()


@pytest.mark.asyncio
async def test_rest_delete_bug_hotfix_conflict_maps_to_409(db_factory, _graph):
    ids = _graph
    bug_id = f"card-scope-rest-bug-{uuid.uuid4().hex[:8]}"
    sprint_id = f"card-scope-rest-hotfix-{uuid.uuid4().hex[:8]}"
    await _seed_hotfix_origin(
        db_factory,
        ids,
        bug_id=bug_id,
        sprint_id=sprint_id,
    )
    before_activity = await _activity_ids(db_factory, bug_id)

    app = FastAPI()
    app.include_router(cards_router, prefix="/api/v1/cards")
    app.dependency_overrides[require_user] = lambda: USER_ID
    register_mcp_test_runtime(db_factory)
    response = TestClient(app).delete(f"/api/v1/cards/{bug_id}")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "hotfix_origin_bug_delete_conflict"
    assert detail["facts"]["referencing_sprint_ids"] == [sprint_id]
    async with db_factory() as db:
        assert await db.get(Card, bug_id) is not None
        sprint = await db.get(Sprint, sprint_id)
        assert sprint.origin_bug_id == bug_id
    assert await _activity_ids(db_factory, bug_id) == before_activity
