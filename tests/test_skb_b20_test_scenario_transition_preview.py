"""B20 parity for test-scenario transition previews and scoped mutation locks."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.allowed_transitions import (
    AllowedTransition,
    ListAllowedTransitionsUseCase,
    _TestScenarioTransitionEntity,
)
from okto_pulse.core.domain.enums import (
    SpecStatus,
    TestScenarioStatus as ScenarioStatus,
)


def _services(*, spec_status: SpecStatus, executable: bool):
    spec = SimpleNamespace(
        id="spec-b20",
        board_id="board-b20",
        status=spec_status,
    )
    specs = SimpleNamespace(
        get_spec=AsyncMock(return_value=spec),
        _has_executable_test_card_for_scenario=AsyncMock(
            return_value=executable,
        ),
    )
    return SimpleNamespace(specs=specs), spec


def _scenario() -> _TestScenarioTransitionEntity:
    return _TestScenarioTransitionEntity(
        id="scenario-b20",
        board_id="board-b20",
        spec_id="spec-b20",
        status=ScenarioStatus.READY,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_status", (SpecStatus.VALIDATED, SpecStatus.DONE))
async def test_locked_parent_spec_blocks_preview_without_executable_test_card(
    spec_status: SpecStatus,
) -> None:
    services, spec = _services(spec_status=spec_status, executable=False)

    blocked = await ListAllowedTransitionsUseCase()._blocked_reason(
        services,
        "test_scenario",
        _scenario(),
        AllowedTransition(
            to_status="draft",
            label="Draft",
            gate="none",
        ),
    )

    assert blocked is not None
    assert "Cannot change test scenario status" in blocked
    services.specs.get_spec.assert_awaited_once_with("spec-b20")
    services.specs._has_executable_test_card_for_scenario.assert_awaited_once_with(
        spec,
        "scenario-b20",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_status", (SpecStatus.VALIDATED, SpecStatus.DONE))
async def test_executable_test_card_preserves_post_lock_status_path(
    spec_status: SpecStatus,
) -> None:
    services, spec = _services(spec_status=spec_status, executable=True)

    blocked = await ListAllowedTransitionsUseCase()._blocked_reason(
        services,
        "test_scenario",
        _scenario(),
        AllowedTransition(
            to_status="passed",
            label="Passed",
            gate="test_scenario_progression",
            preconditions=("authenticated_test_evidence",),
            policy_compliance=True,
        ),
    )

    assert blocked is None
    services.specs._has_executable_test_card_for_scenario.assert_awaited_once_with(
        spec,
        "scenario-b20",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec_status",
    (
        SpecStatus.DRAFT,
        SpecStatus.REVIEW,
        SpecStatus.APPROVED,
        SpecStatus.IN_PROGRESS,
    ),
)
async def test_mutable_parent_spec_does_not_manufacture_a_preview_blocker(
    spec_status: SpecStatus,
) -> None:
    services, _spec = _services(spec_status=spec_status, executable=False)

    blocked = await ListAllowedTransitionsUseCase()._blocked_reason(
        services,
        "test_scenario",
        _scenario(),
        AllowedTransition(
            to_status="draft",
            label="Draft",
            gate="none",
        ),
    )

    assert blocked is None
    services.specs._has_executable_test_card_for_scenario.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_parent_spec_fails_closed() -> None:
    services, _spec = _services(
        spec_status=SpecStatus.IN_PROGRESS,
        executable=False,
    )
    services.specs.get_spec.return_value = None

    blocked = await ListAllowedTransitionsUseCase()._blocked_reason(
        services,
        "test_scenario",
        _scenario(),
        AllowedTransition(
            to_status="draft",
            label="Draft",
            gate="none",
        ),
    )

    assert blocked is not None
    assert "test_scenario_parent_spec_not_found" in blocked
