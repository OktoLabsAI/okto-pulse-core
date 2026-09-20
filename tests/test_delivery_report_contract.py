from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.models.delivery_report import CardDeliveryReportInput
from okto_pulse.core.models.delivery_evidence import card_delivery_command
from okto_pulse.core.application.use_cases.delivery_evidence import RecordCardDeliveryEvidenceUseCase
from okto_pulse.core.application.use_cases.base import PermissionDeniedError


def body():
    return dict(contract_version="card-delivery-report/v1", expected_card_status="in_progress",
        batch=dict(contract_version="card-delivery-batch/v1", expected_card_version=1, expected_spec_edition=1,
            expected_delivery_revision=0, idempotency_key="final", entries=[dict(client_ref="last", kind="progress",
                justification="Last checkpoint", progress=dict(source_state=dict(workspace_state="unknown", recoverability="unknown"), remaining="Review"))]),
        report=dict(status="validation", conclusion="Completed work", completeness=100,
            completeness_justification="All assigned work", drift=0, drift_justification="Within scope"))


@pytest.mark.parametrize("mutation", ["target", "state", "trust", "selection", "duplicates", "limit"])
def test_composed_contract_is_closed_and_bounded(mutation):
    value = deepcopy(body())
    if mutation == "target":
        value["report"]["status"] = "in_progress"
    elif mutation == "state":
        value["expected_card_status"] = "validation"
    elif mutation == "trust":
        value["report"]["gate_passed"] = True
    elif mutation == "selection":
        value["report"]["delivery_selection"] = dict(expected_card_version=1, expected_spec_edition=1,
            expected_delivery_revision=0, record_ids=[])
    elif mutation == "duplicates":
        value["existing_record_ids"] = ["one", "one"]
    else:
        value["existing_record_ids"] = [str(i) for i in range(200)]
    with pytest.raises(ValueError):
        CardDeliveryReportInput.model_validate(value)


@pytest.mark.asyncio
async def test_every_constituent_permission_is_checked_before_atomic_store(monkeypatch):
    from okto_pulse.core.application.use_cases import delivery_evidence as module
    command = card_delivery_command(board_id="b", card_id="c", spec_id="s", evidence=body())
    calls = []

    async def authorize(actor, requirement, **_):
        calls.append(requirement.operation)
        if len(calls) == 2:
            raise PermissionDeniedError("denied")

    monkeypatch.setattr(module, "require_authorization", authorize)
    store = SimpleNamespace(record_card=AsyncMock(), record_card_report=AsyncMock())
    uow = SimpleNamespace(services=SimpleNamespace(delivery_evidence=store), commit=AsyncMock(), rollback=AsyncMock())
    with pytest.raises(PermissionDeniedError):
        await RecordCardDeliveryEvidenceUseCase().execute(command,
            actor=SimpleNamespace(actor_id="agent", actor_kind="agent"), uow=uow)
    assert calls[0] == "card.conclusion.write" and len(calls) == 2
    store.record_card_report.assert_not_called()
    uow.commit.assert_not_called()
