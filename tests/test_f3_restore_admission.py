"""Restoration may expose history, but cannot resume normal work in a Done Spec."""
from uuid import uuid4

import pytest

from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.services.main import ArchiveService, CardOperationError, CardService


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_status", (SpecStatus.DONE, SpecStatus.IN_PROGRESS))
@pytest.mark.parametrize("card_type", list(CardType))
@pytest.mark.parametrize("status", (CardStatus.STARTED, CardStatus.IN_PROGRESS, CardStatus.NOT_STARTED, CardStatus.ON_HOLD, CardStatus.DONE, CardStatus.CANCELLED))
async def test_restore_keeps_history_without_resuming_frozen_normal_work(spec_status, card_type, status):
    suffix = uuid4().hex[:8]
    board_id, spec_id, card_id = f"board-{suffix}", f"spec-{suffix}", f"card-{suffix}"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="Restore", owner_id="owner"))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", status=spec_status, created_by="owner"))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Historical task",
                    status=status, card_type=card_type, archived=True, pre_archive_status=status.value, created_by="owner"))
        await db.commit()
        blocked = card_type is CardType.NORMAL and spec_status is SpecStatus.DONE and status in {CardStatus.STARTED, CardStatus.IN_PROGRESS}
        if blocked:
            with pytest.raises(CardOperationError) as error:
                await ArchiveService(db).restore_tree("spec", spec_id)
            assert error.value.code == "normal_card_spec_done"
        else:
            result = await ArchiveService(db).restore_tree("spec", spec_id)
            assert result["cards"] == 1
        restored = await CardService(db).get_card(card_id)
        assert restored.status is status
        assert restored.archived is blocked
        assert restored.pre_archive_status == (status.value if blocked else None)
        assert restored.title == "Historical task"
        assert (await db.get(Spec, spec_id)).status is spec_status
