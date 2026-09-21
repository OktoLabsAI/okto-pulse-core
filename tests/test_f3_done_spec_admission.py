"""F3 authorized contract; pre-decision reproduction remains in the ledger."""
from uuid import uuid4

import pytest
from sqlalchemy import event

from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus, Sprint, SprintStatus
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate
from okto_pulse.core.services.main import CardOperationError, CardService, SpecService
from okto_pulse.core.application.use_cases.allowed_transitions import ListAllowedTransitionsCommand, ListAllowedTransitionsUseCase
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("has_closed_sprint", (False, True))
@pytest.mark.parametrize("operation", ("create", "edit", "unlink", "link_out", "link_in", "reparent_out", "reparent_in"))
async def test_done_spec_blocks_normal_content_without_writes(has_closed_sprint, operation):
    suffix = uuid4().hex[:8]
    board_id, spec_id, open_id, owner = f"board-{suffix}", f"done-{suffix}", f"open-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3", owner_id=owner))
        db.add_all([Spec(id=spec_id, board_id=board_id, title="Delivered", status=SpecStatus.DONE, created_by=owner),
                    Spec(id=open_id, board_id=board_id, title="Open", status=SpecStatus.IN_PROGRESS, created_by=owner)])
        source_id = open_id if operation.endswith("_in") else spec_id
        db.add(Card(id=f"card-{suffix}", board_id=board_id, spec_id=source_id, title="Historical normal task", created_by=owner))
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
        try:
            with pytest.raises(CardOperationError) as error:
                if operation == "create":
                    await service.create_card(board_id, owner, CardCreate(title="New", spec_id=spec_id))
                elif operation == "edit":
                    await service.update_card(card_id, owner, CardUpdate(title="Changed"))
                elif operation == "unlink":
                    await specs.unlink_card(card_id, owner)
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
