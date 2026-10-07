"""KG-06: observed unilateral links cannot satisfy the real completion gate."""
import copy

import pytest

from sqlalchemy_test_models import Card, CardStatus, Spec
from okto_pulse.core.domain.card_scenario_references import analyze_card_scenario_references
from test_r4_test2_test_card_gate_behavior import _seed_test_card, _call


@pytest.mark.parametrize("origin", ["card", "spec"])
@pytest.mark.parametrize("status", ["ready", "passed"])
async def test_unilateral_observation_never_substitutes_completion_evidence(db_factory, origin, status):
    board_id, spec_id, card_id = await _seed_test_card(db_factory, scenarios=[{
        "id": "scenario", "title": "Observed scenario", "given": "g", "when": "w",
        "then": "t", "status": status,
    }])
    async with db_factory() as session:
        card = await session.get(Card, card_id)
        spec = await session.get(Spec, spec_id)
        card.test_scenario_ids = ["scenario"] if origin == "card" else []
        spec.test_scenarios = [{**spec.test_scenarios[0],
                               "linked_task_ids": [card_id] if origin == "spec" else []}]
        await session.commit()
        before = copy.deepcopy((card.test_scenario_ids, spec.test_scenarios))
        analysis = analyze_card_scenario_references(
            board_id=board_id, card_id=card_id, spec_id=spec_id,
            card_links=card.test_scenario_ids, parent_exists=True, scenarios=spec.test_scenarios,
        )
    assert len(analysis.links) == 1
    assert analysis.snapshot.findings[0].reason_code == "source_disagreement"
    response = await _call("okto_pulse_move_card", board_id=board_id, card_id=card_id, status="done")
    assert response.get("error_code") == "test_card_completion_blocked", response
    details = response["data"]["details"]
    assert details["gate_type"] == "test_card_completion"
    async with db_factory() as session:
        card = await session.get(Card, card_id)
        spec = await session.get(Spec, spec_id)
        assert card.status == CardStatus.IN_PROGRESS
        assert (card.test_scenario_ids, spec.test_scenarios) == before
