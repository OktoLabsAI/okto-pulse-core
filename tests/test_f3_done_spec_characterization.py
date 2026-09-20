"""Observed pre-cutover discrepancy with F3's completed-Spec premise.

This records current behavior for the authority decision; it does not authorize
preserving it in the final contract. See the implementation ledger.
"""

from uuid import uuid4

import pytest

from sqlalchemy_test_models import Board, CardStatus, Spec, SpecStatus, Sprint, SprintStatus
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate
from okto_pulse.core.services.main import CardOperationError, CardService


@pytest.mark.asyncio
@pytest.mark.parametrize("has_closed_sprint", (False, True))
async def test_current_done_spec_accepts_normal_card_writes_and_only_sprint_blocks_start(has_closed_sprint):
    suffix = uuid4().hex[:8]
    board_id, spec_id, owner = f"f3-board-{suffix}", f"f3-spec-{suffix}", "f3-owner"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="F3 investigation", owner_id=owner))
        db.add(Spec(id=spec_id, board_id=board_id, title="Completed Spec",
            status=SpecStatus.DONE, created_by=owner))
        if has_closed_sprint:
            db.add(Sprint(id=f"sprint-{suffix}", board_id=board_id, spec_id=spec_id,
                title="Historical closed sprint", status=SprintStatus.CLOSED, created_by=owner))
        await db.commit()
        service = CardService(db)
        card = await service.create_card(board_id, owner,
            CardCreate(title="New normal work", spec_id=spec_id))
        assert card is not None
        updated = await service.update_card(card.id, owner, CardUpdate(title="Changed normal work"))
        assert updated.title == "Changed normal work"
        if has_closed_sprint:
            with pytest.raises(CardOperationError) as exc:
                await service.move_card(card.id, owner, CardMove(status=CardStatus.STARTED))
            assert exc.value.code == "sprint_required"
        else:
            moved = await service.move_card(card.id, owner, CardMove(status=CardStatus.STARTED))
            assert moved.status is CardStatus.STARTED
        assert (await db.get(Spec, spec_id)).status is SpecStatus.DONE
