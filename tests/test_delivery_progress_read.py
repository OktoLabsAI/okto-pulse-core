from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.application.use_cases.delivery_evidence import GetDeliveryEvidenceUseCase
from okto_pulse.core.models.delivery_evidence import DeliveryEvidenceReadQuery


@pytest.mark.parametrize("patch", [{"cursor": "abc"}, {"record_id": "id"}, {"limit": 1},
    {"card_id": "c", "limit": 21}, {"card_id": "c", "cursor": "x", "record_id": "r"},
    {"card_id": "c", "limit": True}, {"card_id": "c", "verified": True},
    {"edition": 1}, {"card_id": "c", "edition": True},
    {"card_id": "c", "view": "resume", "edition": 1}])
def test_read_rejects_unbounded_or_ambiguous_scope(patch):
    with pytest.raises(ValidationError):
        DeliveryEvidenceReadQuery(board_id="b", spec_id="s", **patch)


@pytest.mark.asyncio
async def test_each_page_reauthorizes_and_does_not_borrow_write_authority():
    store = SimpleNamespace(progress_history=AsyncMock(return_value={"items": []}), projection=AsyncMock())
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store))
    actor = ActorContext(actor_id="reader", source="mcp", actor_kind="agent",
        board_id="b", permissions=["code_traceability.evidence.read"])
    query = DeliveryEvidenceReadQuery(board_id="b", spec_id="s", card_id="c")
    await GetDeliveryEvidenceUseCase().execute(query, actor=actor, uow=uow)
    store.progress_history.assert_awaited_once_with(query, actor_id="reader")
    store.projection.assert_not_awaited()
    for permissions in (["board.read"], ["card.conclusion.write"], []):
        denied = ActorContext(actor_id="reader", source="mcp", actor_kind="agent", board_id="b", permissions=permissions)
        with pytest.raises(PermissionDeniedError):
            await GetDeliveryEvidenceUseCase().execute(query.model_copy(update={"cursor": "old"}), actor=denied, uow=uow)
    assert store.progress_history.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("write,state,expected", [(True, True, True), (False, True, False), (True, False, False)])
async def test_resume_actions_combine_current_authority_and_execution_state(write, state, expected):
    store = SimpleNamespace(card_resume=AsyncMock(side_effect=lambda *args, **kwargs: {"progress_state_eligible": state}))
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store))
    permissions = ["code_traceability.evidence.read"] + (["card.conclusion.write"] if write else [])
    actor = ActorContext(actor_id="reader", source="mcp", actor_kind="agent", board_id="b", permissions=permissions)
    result = await GetDeliveryEvidenceUseCase().execute(DeliveryEvidenceReadQuery(
        board_id="b", spec_id="s", card_id="c", view="resume"), actor=actor, uow=uow)
    assert result["actions"]["record_progress"] is expected
    assert result["actions"]["final_transitions"] == "not_evaluated"
    assert result["actions"]["mutation_reauthorization_required"]
    assert "progress_state_eligible" not in result
