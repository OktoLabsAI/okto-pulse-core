"""F3 authorized contract; pre-decision reproduction remains in the ledger."""
from uuid import uuid4

import pytest
from sqlalchemy import event

from sqlalchemy_test_models import Attachment, Board, Card, CardDependency, CardStatus, CardType, Spec, SpecStatus, Sprint, SprintStatus
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate
from okto_pulse.core.services.main import AttachmentService, CardOperationError, CardService, SpecService
from okto_pulse.core.application.use_cases.allowed_transitions import ListAllowedTransitionsCommand, ListAllowedTransitionsUseCase
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.application.use_cases.architecture_crud import CopyArchitectureFromSpecToCardCommand, CopyArchitectureFromSpecToCardUseCase
from okto_pulse.core.application.use_cases.knowledge_propagation import (
    DropCardKnowledgeAssignmentsCommand, DropCardKnowledgeAssignmentsUseCase,
    RefreshCardKnowledgeAssignmentsCommand, RefreshCardKnowledgeAssignmentsUseCase,
    ReplaceCardKnowledgeAssignmentsCommand, ReplaceCardKnowledgeAssignmentsUseCase,
)
from okto_pulse.core.models.knowledge_propagation import (
    KnowledgeAssignmentDropRequest, KnowledgeAssignmentRefreshRequest, KnowledgeAssignmentReplaceRequest,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_closed_sprint", (False, True))
@pytest.mark.parametrize("operation", ("create", "edit", "unlink", "link_out", "link_in", "reparent_out", "reparent_in",
    "delete", "upload", "delete_attachment", "add_dependency", "remove_dependency",
    "knowledge_drop", "knowledge_replace", "knowledge_refresh", "architecture_copy"))
async def test_done_spec_blocks_normal_content_without_writes(has_closed_sprint, operation, monkeypatch):
    suffix = uuid4().hex[:8]
    board_id, spec_id, open_id, owner = f"board-{suffix}", f"done-{suffix}", f"open-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id=owner))
        db.add_all([Spec(id=spec_id, board_id=board_id, title="Delivered", status=SpecStatus.DONE, created_by=owner),
                    Spec(id=open_id, board_id=board_id, title="Open", status=SpecStatus.IN_PROGRESS, created_by=owner)])
        source_id = open_id if operation.endswith("_in") else spec_id
        db.add(Card(id=f"card-{suffix}", board_id=board_id, spec_id=source_id, title="Historical normal task", created_by=owner))
        db.add(Card(id=f"dependency-{suffix}", board_id=board_id, spec_id=open_id, title="Dependency", created_by=owner))
        if operation == "remove_dependency":
            db.add(CardDependency(card_id=f"card-{suffix}", depends_on_id=f"dependency-{suffix}"))
        if operation == "delete_attachment":
            db.add(Attachment(id=f"attachment-{suffix}", card_id=f"card-{suffix}", filename="f.txt",
                original_filename="f.txt", path="never-read", mime_type="text/plain", size=1, uploaded_by=owner))
        if has_closed_sprint:
            db.add(Sprint(id=f"sprint-{suffix}", board_id=board_id, spec_id=spec_id, title="Historical sprint", status=SprintStatus.CLOSED, created_by=owner))
        await db.commit()
        statements = []

        def capture(_conn, _cursor, sql, _params, _context, _many):
            if sql.lstrip().split()[0].lower() in {"insert", "update", "delete", "replace"}:
                statements.append(sql)

        event.listen(db.bind.sync_engine, "before_cursor_execute", capture)
        service, specs = CardService(db), SpecService(db)
        card_id = f"card-{suffix}"
        def forbidden_storage():
            raise AssertionError("Storage reached before content admission")
        monkeypatch.setattr("okto_pulse.core.services.main.get_storage_provider", forbidden_storage)
        try:
            with pytest.raises(CardOperationError) as error:
                if operation == "create":
                    await service.create_card(board_id, owner, CardCreate(title="New", spec_id=spec_id))
                elif operation == "edit":
                    await service.update_card(card_id, owner, CardUpdate(title="Changed"))
                elif operation == "unlink":
                    await specs.unlink_card(card_id, owner)
                elif operation == "delete":
                    await service.delete_card(card_id, owner)
                elif operation == "upload":
                    await AttachmentService(db).upload_attachment(card_id, owner, "new.txt", b"x", "text/plain")
                elif operation == "delete_attachment":
                    await AttachmentService(db).delete_attachment(f"attachment-{suffix}")
                elif operation == "add_dependency":
                    await service.add_dependency(card_id, f"dependency-{suffix}")
                elif operation == "remove_dependency":
                    await service.remove_dependency(card_id, f"dependency-{suffix}")
                elif operation.startswith("knowledge_") or operation == "architecture_copy":
                    if operation == "knowledge_drop":
                        use_case = DropCardKnowledgeAssignmentsUseCase()
                        command = DropCardKnowledgeAssignmentsCommand(card_id, KnowledgeAssignmentDropRequest(
                            knowledge_ids=[], justification="Remove assignments", idempotency_key="drop", expected_revision=0))
                    elif operation == "knowledge_replace":
                        use_case = ReplaceCardKnowledgeAssignmentsUseCase()
                        command = ReplaceCardKnowledgeAssignmentsCommand(card_id, KnowledgeAssignmentReplaceRequest(
                            knowledge_ids=["root-1"], mode="snapshot", justification="Relevant",
                            idempotency_key="replace", expected_revision=0, linkage=[]))
                    elif operation == "knowledge_refresh":
                        use_case = RefreshCardKnowledgeAssignmentsUseCase()
                        command = RefreshCardKnowledgeAssignmentsCommand(card_id, KnowledgeAssignmentRefreshRequest(
                            knowledge_ids=["root-1"], idempotency_key="refresh", expected_revision=0))
                    else:
                        use_case = CopyArchitectureFromSpecToCardUseCase()
                        command = CopyArchitectureFromSpecToCardCommand(card_id, spec_id, None, None, board_id)
                    await use_case.execute(command, actor=ActorContext(owner, "test", board_id=board_id),
                        uow=resolve_unit_of_work_factory().wrap(db))
                elif operation.startswith("link_"):
                    await specs.link_card(spec_id if operation.endswith("_in") else open_id, card_id, owner)
                else:
                    await service.update_card(card_id, owner, CardUpdate(spec_id=spec_id if operation.endswith("_in") else open_id))
            assert error.value.code == "normal_card_spec_done"
            assert statements == []
            row = await service.get_card(card_id)
            assert row.title == "Historical normal task" and row.spec_id == source_id
            assert (await db.get(Spec, spec_id)).status is SpecStatus.DONE
        finally:
            event.remove(db.bind.sync_engine, "before_cursor_execute", capture)


@pytest.mark.asyncio
@pytest.mark.parametrize("old_status,target", (
    (CardStatus.NOT_STARTED, CardStatus.STARTED), (CardStatus.STARTED, CardStatus.IN_PROGRESS),
    (CardStatus.ON_HOLD, CardStatus.IN_PROGRESS), (CardStatus.DONE, CardStatus.IN_PROGRESS),
))
async def test_done_spec_execution_preview_matches_mutation(old_status, target):
    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id, owner = f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id=owner))
        db.add(Spec(id=spec_id, board_id=board_id, title="Delivered", status=SpecStatus.DONE, created_by=owner))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, status=old_status, title="Existing", created_by=owner))
        await db.commit()
        result = await ListAllowedTransitionsUseCase().execute(
            ListAllowedTransitionsCommand(board_id, "card", entity_id=card_id),
            actor=ActorContext(owner, "test", board_id=board_id), uow=resolve_unit_of_work_factory().wrap(db))
        edge = next(item for item in result.read_model.allowed_transitions if item.to_status == target.value)
        assert "normal_card_spec_done" in edge.blocked_reason
        with pytest.raises(CardOperationError, match="Normal tasks") as error:
            await CardService(db).move_card(card_id, owner, CardMove(status=target))
        assert error.value.code == "normal_card_spec_done"
        assert (await db.get(Card, card_id)).status == old_status


@pytest.mark.asyncio
@pytest.mark.parametrize("card_type", (CardType.BUG, CardType.TEST))
async def test_done_spec_preserves_bug_and_test_content(card_type):
    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id, owner = f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id=owner))
        db.add(Spec(id=spec_id, board_id=board_id, title="Delivered", status=SpecStatus.DONE, created_by=owner))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, card_type=card_type, title="Existing", created_by=owner))
        await db.commit()
        result = await CardService(db).update_card(card_id, owner, CardUpdate(title="Legitimate correction"))
        assert result.title == "Legitimate correction"
        assert (await db.get(Spec, spec_id)).status is SpecStatus.DONE


@pytest.mark.asyncio
async def test_done_spec_normal_card_keeps_collaboration_and_reads():
    from okto_pulse.core.models.schemas import QACreate, QAAnswer, CommentCreate
    from okto_pulse.core.ports.application_persistence import get_application_persistence_port
    from okto_pulse.core.services.main import QAService, CommentService
    from okto_pulse.core.services.board_governance import QASelfAnsweringNotAllowedError

    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id, owner = f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id=owner))
        db.add(Spec(id=spec_id, board_id=board_id, title="Delivered", status=SpecStatus.DONE, created_by=owner))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, status=CardStatus.DONE, title="Historical", created_by=owner))
        await db.commit()
        qa, comments = QAService(db), CommentService(db)
        question = await qa.create_question(card_id, owner, QACreate(question="Historical clarification?"))
        comment = await comments.create_comment(card_id, owner, CommentCreate(content="Preserved collaboration"))
        await get_application_persistence_port().flush(db)
        with pytest.raises(QASelfAnsweringNotAllowedError):
            await qa.answer_question(question.id, owner, QAAnswer(answer="Self answer"))
        answered = await qa.answer_question(question.id, "f3-reviewer", QAAnswer(answer="Clarified"))
        assert answered.answer == "Clarified"
        assert (await comments.get_comment(comment.id)).content == "Preserved collaboration"
        card = await CardService(db).get_card(card_id)
        assert card.status is CardStatus.DONE and card.title == "Historical"
        assert (await db.get(Spec, spec_id)).status is SpecStatus.DONE
