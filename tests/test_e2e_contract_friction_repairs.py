from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.services.main import CardService


def _board() -> SimpleNamespace:
    return SimpleNamespace(settings={})


@pytest.mark.asyncio
async def test_not_applicable_api_contract_does_not_block_spec_coverage() -> None:
    spec = SimpleNamespace(
        title="Contract waiver",
        api_contracts=[
            {
                "id": "contract-na",
                "method": "GET",
                "path": "/retired",
                "status": "not_applicable",
                "notes": "Endpoint was retired before implementation.",
                "linked_task_ids": [],
            }
        ],
        skip_contract_coverage=False,
    )

    await CardService(None).check_contract_coverage(spec, _board())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_active_api_contract_still_requires_a_task_and_names_public_tool() -> None:
    spec = SimpleNamespace(
        title="Active contract",
        api_contracts=[
            {
                "id": "contract-active",
                "method": "GET",
                "path": "/health",
                "status": "active",
                "linked_task_ids": [],
            }
        ],
        skip_contract_coverage=False,
    )

    with pytest.raises(ValueError) as exc_info:
        await CardService(None).check_contract_coverage(spec, _board())  # type: ignore[arg-type]

    message = str(exc_info.value)
    assert "okto_pulse_link_task(target_type='contract'" in message
    assert "okto_pulse_link_task_to_contract" not in message


@pytest.mark.asyncio
async def test_legacy_decision_gate_fallback_is_enforced_and_names_public_tool() -> None:
    # Deliberately omit skip_decisions_coverage to model a legacy object whose
    # persisted row predates the field.
    spec = SimpleNamespace(
        title="Legacy decision",
        decisions=[
            {
                "id": "decision-active",
                "title": "Use the consolidated MCP tool",
                "status": "active",
                "linked_task_ids": [],
            }
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        await CardService(None).check_decisions_coverage(spec, _board())  # type: ignore[arg-type]

    message = str(exc_info.value)
    assert "okto_pulse_link_task(target_type='decision'" in message
    assert "okto_pulse_link_task_to_decision" not in message
