"""Behavioral coverage for fail-closed scenario_type centralization
(spec ac16b3c9, IMP card 58844a26).

Exercises the REAL write surfaces — the ``okto_pulse_add_test_scenario`` /
``okto_pulse_update_test_scenario`` MCP tools and the ``SpecService``
create/update persistence gates — and proves:

* an unsupported scenario_type is rejected BEFORE any mutation, with a
  structured error naming the allowed values (no silent normalization to
  ``integration``);
* supported values persist EXACTLY;
* an omitted value still defaults to ``integration`` (a default, not a coercion
  of an invalid value);
* unchanged legacy/invalid values are GRANDFATHERED so the whole-list update
  path (UI full-list / REST PUT) keeps re-serializing historical data, while a
  new or changed invalid value on that same path still fails closed.

Validator can reproduce with:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_scenario_type_fail_closed.py
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import SpecCreate, SpecUpdate
from okto_pulse.core.models.schemas import TestScenario as _TestScenario
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services.test_scenario_lifecycle import (
    InvalidScenarioTypeError,
    VALID_SCENARIO_TYPES,
)

pytestmark = pytest.mark.asyncio

USER_ID = "scenario-type-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read", "specs:update"],
        },
    )()


async def _seed(db_factory, scenarios=None) -> tuple[str, str]:
    board_id, spec_id = _id("st-board"), _id("st-spec")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Scenario-Type Board",
                owner_id=USER_ID,
                settings={"skip_test_coverage_global": False},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Scenario-Type Spec",
                status=SpecStatus.DRAFT,
                created_by=USER_ID,
                acceptance_criteria=[],
                test_scenarios=scenarios or [],
                functional_requirements=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _seed_board(db_factory) -> str:
    board_id = _id("st-board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Scenario-Type Board", owner_id=USER_ID, settings={}))
        await db.commit()
    return board_id


async def _stored(db_factory, spec_id) -> list:
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        return list(spec.test_scenarios or [])


async def _call_tool(db_factory, tool_name, **kwargs):
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(kwargs["board_id"]))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(tool_name)
        return json.loads(await tool.fn(**kwargs))


# ---------------------------------------------------------------------------
# MCP add tool (server.py okto_pulse_add_test_scenario)
# ---------------------------------------------------------------------------


async def test_add_invalid_scenario_type_fails_closed_no_mutation(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S",
        given="g", when="w", then="t", scenario_type="regression",
    )
    assert payload.get("error") == "invalid_scenario_type", payload
    for t in VALID_SCENARIO_TYPES:
        assert t in payload["message"], payload
    assert "regression" in payload["message"]
    assert "No scenario was appended" in payload["message"]
    # fail-closed: nothing persisted, NOT silently normalized to integration.
    assert await _stored(db_factory, spec_id) == []


async def test_add_valid_scenario_type_persists_exactly(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S",
        given="g", when="w", then="t", scenario_type="negative",
    )
    assert payload.get("success") is True, payload
    assert payload["scenario"]["scenario_type"] == "negative"
    assert [s["scenario_type"] for s in await _stored(db_factory, spec_id)] == ["negative"]


async def test_add_omitted_scenario_type_defaults_integration(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S", given="g", when="w", then="t",
    )
    assert payload.get("success") is True, payload
    assert payload["scenario"]["scenario_type"] == "integration"


# ---------------------------------------------------------------------------
# MCP update tool (server.py okto_pulse_update_test_scenario)
# ---------------------------------------------------------------------------


async def test_update_invalid_scenario_type_fails_closed(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_keep", "title": "keep", "scenario_type": "unit", "status": "draft"}],
    )
    payload = await _call_tool(
        db_factory, "okto_pulse_update_test_scenario",
        board_id=board_id, spec_id=spec_id, scenario_id="ts_keep", scenario_type="regression",
    )
    assert payload.get("error") == "invalid_scenario_type", payload
    assert "No scenario was updated" in payload["message"]
    # unchanged
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "unit"


async def test_update_valid_scenario_type_persists(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_x", "title": "x", "scenario_type": "unit", "status": "draft"}],
    )
    payload = await _call_tool(
        db_factory, "okto_pulse_update_test_scenario",
        board_id=board_id, spec_id=spec_id, scenario_id="ts_x", scenario_type="manual",
    )
    assert payload.get("success") is True, payload
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "manual"


# ---------------------------------------------------------------------------
# Service whole-list update_spec (UI full-list / REST PUT bypass)
# ---------------------------------------------------------------------------


async def test_update_spec_new_invalid_scenario_rejected(db_factory):
    board_id, spec_id = await _seed(db_factory)
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.update_spec(
                spec_id, USER_ID,
                SpecUpdate(test_scenarios=[
                    {"id": "ts_new", "title": "new", "scenario_type": "bogus", "status": "draft"}
                ]),
            )
    assert await _stored(db_factory, spec_id) == []  # rejected before mutation


async def test_update_spec_grandfathers_unchanged_legacy(db_factory):
    # legacy invalid value inserted out-of-band (as historical data would be).
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        # whole-list re-serialize (legacy unchanged) + append a valid new one → OK.
        await svc.update_spec(
            spec_id, USER_ID,
            SpecUpdate(test_scenarios=[
                {"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"},
                {"id": "ts_ok", "title": "ok", "scenario_type": "unit", "status": "draft"},
            ]),
        )
        await db.commit()
    stored = {s["id"]: s["scenario_type"] for s in await _stored(db_factory, spec_id)}
    assert stored == {"ts_legacy": "regression", "ts_ok": "unit"}


async def test_update_spec_changing_legacy_to_invalid_rejected(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.update_spec(
                spec_id, USER_ID,
                SpecUpdate(test_scenarios=[
                    {"id": "ts_legacy", "title": "legacy", "scenario_type": "still_bad", "status": "draft"}
                ]),
            )


async def test_update_spec_changing_legacy_to_valid_ok(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        await svc.update_spec(
            spec_id, USER_ID,
            SpecUpdate(test_scenarios=[
                {"id": "ts_legacy", "title": "legacy", "scenario_type": "e2e", "status": "draft"}
            ]),
        )
        await db.commit()
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "e2e"


# ---------------------------------------------------------------------------
# Service create_spec
# ---------------------------------------------------------------------------


async def test_create_spec_invalid_scenario_type_rejected(db_factory):
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.create_spec(
                board_id, USER_ID,
                SpecCreate(
                    title="S",
                    test_scenarios=[_TestScenario(id="ts_c", title="c", scenario_type="bogus")],
                ),
            )


async def test_create_spec_valid_scenario_type_ok(db_factory):
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.create_spec(
            board_id, USER_ID,
            SpecCreate(
                title="S",
                test_scenarios=[_TestScenario(id="ts_c", title="c", scenario_type="manual")],
            ),
        )
        await db.commit()
        assert spec is not None
        assert spec.test_scenarios[0]["scenario_type"] == "manual"
