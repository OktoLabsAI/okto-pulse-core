"""Repair admission stays backend-neutral and refuses unproven health states."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from okto_pulse.core.application.use_cases import deterministic_projection_repair as module
from okto_pulse.core.application.use_cases.base import ActorContext, ConflictError


@pytest.mark.asyncio
@pytest.mark.parametrize("health,allowed", [
    ({"graph_state": "healthy", "overall_state": "healthy"}, True),
    ({"graph_state": "healthy", "overall_state": "at_risk"}, True),
    ({"graph_state": "healthy"}, False),
    ({"graph_state": "healthy", "overall_state": None}, False),
    ({"graph_state": "healthy", "overall_state": "future_state"}, False),
    ({"graph_state": "healthy", "overall_state": []}, False),
    *[({"graph_state": "healthy", "overall_state": state}, False)
      for state in ("recovery_needed", "quarantined", "backpressure", "unavailable")],
    ({"graph_state": "unavailable", "overall_state": "healthy"}, False),
    ({"overall_state": "healthy"}, False),
    (None, False),
    ([], False),
])
async def test_repair_requires_positive_health_evidence(monkeypatch, health, allowed):
    monkeypatch.setattr(module, "_require_board_access", AsyncMock())
    monkeypatch.setattr(module, "require_authorization", AsyncMock())
    kg = SimpleNamespace(
        health=AsyncMock(return_value=health),
        stage_spec_projection_repair=AsyncMock(return_value={"queued_count": 0}),
    )
    specs = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(
        board_id="board", status="done", archived=False,
    )))
    uow = SimpleNamespace(services=SimpleNamespace(kg=kg), specs=specs, commit=AsyncMock())
    command = module.RepairSpecProjectionCommand(
        "board", (str(uuid4()),), "Repair verified missing root",
    )
    operation = module.RepairSpecProjectionUseCase()
    if allowed:
        assert await operation.execute(command, actor=ActorContext("operator", "rest"), uow=uow) == {"queued_count": 0}
        specs.get.assert_awaited_once()
        kg.stage_spec_projection_repair.assert_awaited_once()
        uow.commit.assert_awaited_once()
    else:
        with pytest.raises(ConflictError):
            await operation.execute(command, actor=ActorContext("operator", "rest"), uow=uow)
        specs.get.assert_not_awaited()
        kg.stage_spec_projection_repair.assert_not_awaited()
        uow.commit.assert_not_awaited()
