"""KG28: current authoritative facts, independent of projection recovery state."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus
from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope
from okto_pulse.core.domain.execution_contract import new_execution_contract
from okto_pulse.core.models.schemas import CardMove, SpecMove
from okto_pulse.core.services import missing_link_gate as gate
from okto_pulse.core.services.gate_contracts import GateContractError
from okto_pulse.core.services.main import CardService, SpecService
from sqlalchemy_test_models import Card, Spec
from test_allowed_transitions_mutation_parity_regressions import (
    USER_ID, _board, _direct_spec_context_fields, _id, _persist, _preview_transition,
)


async def seed(db_factory, *, mode="blocking", entity_type="spec"):
    board_id, spec_id, card_id = _id("link-board"), _id("link-spec"), _id("link-card")
    spec = Spec(id=spec_id, **_direct_spec_context_fields(spec_id), board_id=board_id,
                execution_contract=new_execution_contract(board_id=board_id, spec_id=spec_id,
                    edition=1, actor_id=USER_ID, origin='new_spec'),
                architecture_adoption=ArchitectureAdoptionScope(board_id=board_id, spec_id=spec_id,
                    adopted_in_edition=1, actor_id=USER_ID, inherited_resource_ids=()).model_dump(mode='json'),
                title="Reference source", status=SpecStatus.IN_PROGRESS, created_by=USER_ID,
                decisions=[{"id": "decision", "title": "Declared choice", "rationale": "Explicit reference",
                            "linked_requirements": ["absent-fr"]}])
    card = Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Reference owner",
                status=CardStatus.IN_PROGRESS, card_type=CardType.TEST, created_by=USER_ID,
                test_scenario_ids=["absent-scenario"])
    await _persist(db_factory, _board(board_id, settings={"missing_link_gate": mode}), spec, card)
    return board_id, spec_id if entity_type == "spec" else card_id


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", ["spec", "card"])
async def test_blocking_preview_and_real_move_refuse_current_reference_gap(db_factory, entity_type):
    board_id, subject_id = await seed(db_factory, entity_type=entity_type)
    async with db_factory() as db:
        preview = await _preview_transition(db, board_id=board_id, entity_type=entity_type,
                                            entity_id=subject_id, to_status="done")
        with pytest.raises(GateContractError) as caught:
            if entity_type == "spec":
                await SpecService(db).move_spec(subject_id, USER_ID, SpecMove(status=SpecStatus.DONE))
            else:
                await CardService(db).move_card(subject_id, USER_ID, CardMove(status=CardStatus.DONE))
        assert preview.blocked_reason.startswith("missing_links_open:")
        assert caught.value.code == "missing_links_open"
        assert caught.value.details["nature"] == "semantic_reference"
        assert "absent-" not in str(caught.value.to_dict())
        stored = await gate.get_application_persistence_port().get(db, entity=entity_type, record_id=subject_id)
        assert stored.status.value == "in_progress"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["advisory", "blocking"])
async def test_re_evaluation_uses_current_source_and_never_persists_diagnostics(db_factory, mode):
    _, subject_id = await seed(db_factory, mode=mode)
    async with db_factory() as db:
        port = gate.get_application_persistence_port()
        subject = await port.get(db, entity="spec", record_id=subject_id)
        before = dict(subject.values)
        first = await gate.evaluate_missing_links(db, subject=subject, entity_type="spec",
                                                   settings={"missing_link_gate": mode})
        assert first.status == "available" and len(first.findings) == 1
        assert first.blocked == (mode == "blocking")
        assert subject.values == before and not subject.dirty_fields
        # Normative correction, not clearing a saved finding or waiting on KG.
        row = await db.get(Spec, subject_id)
        row.decisions = [{"id": "decision", "linked_requirements": []}]
        await db.flush()
        current = await gate.evaluate_missing_links(db, subject=subject, entity_type="spec",
                                                     settings={"missing_link_gate": mode})
        assert current.status == "available" and not current.findings and not current.blocked
        assert first.findings  # Prior diagnostic remains immutable, never authoritative.


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["advisory", "blocking"])
async def test_unavailable_source_is_not_an_observation_of_zero(monkeypatch, mode):
    port = SimpleNamespace(get=AsyncMock(side_effect=RuntimeError("private provider text")))
    monkeypatch.setattr(gate, "get_application_persistence_port", lambda: port)
    subject = SimpleNamespace(id="card", board_id="board", status=CardStatus.IN_PROGRESS)
    result = await gate.evaluate_missing_links(None, subject=subject, entity_type="card",
                                               settings={"missing_link_gate": mode})
    assert result.status == "unavailable" and result.to_payload()["finding_count"] is None
    assert "private provider text" not in str(result.to_payload())
    if mode == "blocking":
        with pytest.raises(GateContractError) as caught:
            gate.require_missing_links_closed(result, entity_type="card", subject=subject)
        assert caught.value.code == "missing_link_source_unavailable"
    else:
        gate.require_missing_links_closed(result, entity_type="card", subject=subject)


@pytest.mark.asyncio
async def test_same_board_cross_spec_scenario_is_valid_without_projection(db_factory):
    board_id, subject_id = await seed(db_factory, entity_type="card")
    other_id = _id("other-spec")
    await _persist(db_factory, Spec(id=other_id, **_direct_spec_context_fields(other_id),
        board_id=board_id, title="Cross Spec source", status=SpecStatus.DONE, created_by=USER_ID,
        test_scenarios=[{"id": "absent-scenario", "title": "Declared scenario"}]))
    async with db_factory() as db:
        subject = await gate.get_application_persistence_port().get(db, entity="card", record_id=subject_id)
        result = await gate.evaluate_missing_links(db, subject=subject, entity_type="card",
                                                   settings={"missing_link_gate": "blocking"})
        assert result.status == "available" and not result.findings and not result.blocked


@pytest.mark.asyncio
async def test_foreign_board_scenario_is_not_a_valid_target(db_factory):
    _, subject_id = await seed(db_factory, entity_type="card")
    foreign_id, spec_id = _id("foreign-board"), _id("foreign-spec")
    await _persist(db_factory, _board(foreign_id), Spec(id=spec_id, **_direct_spec_context_fields(spec_id),
        board_id=foreign_id, title="Private source", status=SpecStatus.DONE, created_by=USER_ID,
        test_scenarios=[{"id": "absent-scenario", "title": "Private content"}]))
    async with db_factory() as db:
        subject = await gate.get_application_persistence_port().get(db, entity="card", record_id=subject_id)
        result = await gate.evaluate_missing_links(db, subject=subject, entity_type="card",
                                                   settings={"missing_link_gate": "blocking"})
        assert result.blocked and result.findings[0].reason == "target_absent"
        assert "Private" not in str(result.to_payload()) and foreign_id not in str(result.to_payload())
