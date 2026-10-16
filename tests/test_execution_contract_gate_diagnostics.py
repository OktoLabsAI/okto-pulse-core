"""AC-ARQ-15 diagnostics, including blockers outside the first summary page."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.execution_contract import new_execution_contract
from okto_pulse.core.services import main
from okto_pulse.core.services.architecture_classification import ArchitectureClassificationService
from okto_pulse.core.services.gate_contracts import GateContractError
from test_architecture_classification_review import decision, interface, review, sources


def test_blocking_ids_are_global_bounded_and_independent_of_summary_filter():
    population = sources(*(interface(str(i)) for i in range(60)))
    records = tuple(decision(item) for item in population.candidates[:30])
    result = review(population, records, limit=1, state="current")
    expected = [item.id for item in population.candidates[30:]]
    assert result["items"][0]["state"] == "current"
    assert result["blocking_candidate_count"] == 30
    assert len(result["blocking_candidate_ids"]) == 25
    assert set(result["blocking_candidate_ids"]) <= set(expected)
    assert result["blocking_candidates_truncated"] is True
    assert not result["classification_complete"]


@pytest.mark.asyncio
async def test_start_gate_returns_ids_and_remediation_without_changing_admission(monkeypatch):
    population = sources(*(interface(str(i)) for i in range(31)))
    records = tuple(decision(item) for item in population.candidates[:30])
    classification = AsyncMock(return_value=review(population, records, limit=1))
    monkeypatch.setattr(ArchitectureClassificationService, "review", classification)
    monkeypatch.setattr(main, "_application_list", AsyncMock(return_value=[]))
    # Isolate diagnostics from plan qualification; no claim of end-to-end admission.
    from okto_pulse.core.ports import delivery_inventory
    monkeypatch.setattr(delivery_inventory, "default_delivery_inventory_policy", lambda:
        SimpleNamespace(execution_plan=lambda **kwargs: SimpleNamespace(complete=True)))
    spec = SimpleNamespace(id="spec", board_id="board", status="validated",
        execution_contract=new_execution_contract(board_id="board", spec_id="spec",
            edition=2, actor_id="author", origin="new_spec"), edition=2)
    service = main.SpecService(object())
    with pytest.raises(GateContractError) as caught:
        await service.require_execution_contract_ready(spec)
    error = caught.value.to_dict()
    assert error["code"] == "spec_architecture_classification_incomplete"
    assert error["details"]["blocking_candidate_ids"] == [population.candidates[-1].id]
    assert error["details"]["blocked_transition"] == "in_progress"
    assert error["details"]["required_tool"] == "okto_pulse_list_architecture_classifications"
    assert "Draft" in error["message"]
    assert spec.status == "validated"
    classification.return_value = review(population, tuple(decision(item) for item in population.candidates))
    await service.require_execution_contract_ready(spec)
    assert spec.status == "validated"  # Readiness never starts the Spec automatically.
