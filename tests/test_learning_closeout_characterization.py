"""Characterize pre-KG7 lifecycle behavior; not acceptance of the target gate.

The real cognitive gates and SQL lifecycle run. Healthy graph observation and
independent Delivery admission are fixtures, not claims of graph/E2E admission.
"""

from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import CardService
from sqlalchemy_test_models import CardStatus, CardType
from test_cognitive_closeout_service_wiring import (
    USER_ID,
    _card_row,
    _mark_card_resources_na,
    _seed_card,
    accepted_card_delivery as _accepted_delivery_fixture,
    isolated_closeout_kg_dir as _isolated_kg_fixture,
)

accepted_card_delivery = _accepted_delivery_fixture
isolated_closeout_kg_dir = _isolated_kg_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("policy,global_enabled", [
    ("advisory", False), ("advisory", True),
    ("blocking", False), ("blocking", True),
])
async def test_characterize_empty_queue_bug_done_without_learning(
    policy, global_enabled, monkeypatch, isolated_closeout_kg_dir,
    accepted_card_delivery,
):
    monkeypatch.setattr(get_settings(), "cognitive_readiness_blocking_enabled", global_enabled)
    monkeypatch.setattr(main_service, "_resolve_closeout_graph_state", AsyncMock(return_value="healthy"))
    board_id, _, card_id = await _seed_card(
        CardType.BUG, CardStatus.VALIDATION,
        board_settings={
            "require_task_validation": False,
            "skip_test_coverage_global": True,
            "cognitive_readiness_policy": policy,
        },
    )
    async with get_session_factory()() as db:
        await _mark_card_resources_na(db, board_id, card_id)
        await db.commit()
    async with get_session_factory()() as db:
        # No gate factory replacement: exercise legacy and readiness together.
        await CardService(db).move_card(
            card_id=card_id, user_id=USER_ID, actor_name=USER_ID,
            data=CardMove(
                status=CardStatus.DONE,
                conclusion="Characterization: correction validated, no Learning submitted.",
                completeness=100, completeness_justification="Independent fixture admits delivery.",
                drift=0, drift_justification="No scope drift in this fixture.",
            ),
        )
        await db.commit()
    card = await _card_row(card_id)
    assert card.status == CardStatus.DONE
    assert len(card.conclusions) == 1
    accepted_card_delivery.load_card_snapshot.assert_awaited()
