"""MCP-tool-layer tests for okto_pulse_update_test_scenario /
okto_pulse_delete_test_scenario (spec 6f1e75bf).

The service methods are covered in test_test_scenario_lifecycle.py; these
exercise the actual MCP tool wrappers via ``tool.fn`` (auth, param parsing,
clear pipe-list, error mapping) — the layer that could not be exercised live
because this Claude Code harness's deferred-tool index does not surface the two
new tools (a harness staleness artifact; the server DOES register them).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Card, CardType, Spec, SpecStatus
from okto_pulse.core.services.main import SpecService

pytestmark = pytest.mark.asyncio

USER = "udts-agent"
_EV = {
    "evidence_class": "automated_test_pointer",
    "test_file_path": "tests/test_update_delete_test_scenario_mcp.py",
    "test_function": "test_update_tool_cosmetic_edit_preserves_evidence",
    "last_run_at": "2026-05-30T00:00:00",
    "output_snippet": "1 passed",
}

db_factory_ref: list = [None]


@pytest.fixture(autouse=True)
def _bind(db_factory):
    db_factory_ref[0] = db_factory
    yield
    db_factory_ref[0] = None


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4().hex[:8]}"


def _ctx(board_id: str):
    return type("Ctx", (), {
        "agent_id": USER, "agent_name": USER, "board_id": board_id,
        "permissions": ["board:read", "specs:update"],
    })()


async def _seed(*, status=SpecStatus.IN_PROGRESS, scenarios=None, locked=False,
                card_scenarios=None):
    board_id = _id("udts-board")
    spec_id = _id("udts-spec")
    card_id = _id("udts-card")
    async with db_factory_ref[0]() as db:
        db.add(Board(id=board_id, name="B", owner_id=USER, settings={}))
        kw = dict(
            id=spec_id, board_id=board_id, title="S", status=status, created_by=USER,
            acceptance_criteria=[{"id": "ac_one", "text": "AC one", "status": "active"}],
            test_scenarios=scenarios or [],
        )
        if locked:
            kw["validations"] = [{"id": "val_x", "outcome": "success"}]
            kw["current_validation_id"] = "val_x"
        db.add(Spec(**kw))
        if card_scenarios is not None:
            db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="TC",
                        card_type=CardType.TEST, created_by=USER,
                        test_scenario_ids=card_scenarios))
        await db.commit()
    return board_id, spec_id, card_id


async def _call(tool_name: str, board_id: str, **kw) -> dict:
    register_mcp_test_runtime(db_factory_ref[0])
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(tool_name)
        raw = await tool.fn(board_id=board_id, **kw)
    return json.loads(raw)


async def _scenarios(spec_id):
    async with db_factory_ref[0]() as db:
        return list((await SpecService(db).get_spec(spec_id)).test_scenarios or [])


# ====================================================================
# okto_pulse_update_test_scenario (MCP wrapper)
# ====================================================================


async def test_update_tool_edits_body_and_clears(db_factory):
    _b, spec, _c = await _seed(scenarios=[{
        "id": "ts_a", "title": "S", "given": "old g", "when": "w", "then": "t",
        "notes": "keepme", "status": "draft",
    }])
    out = await _call("okto_pulse_update_test_scenario", _b, spec_id=spec,
                      scenario_id="ts_a", given="new g", title="S2", clear="notes")
    assert out.get("success") is True, out
    assert set(out["updated_fields"]) >= {"given", "title", "notes"}
    sc = (await _scenarios(spec))[0]
    assert sc["given"] == "new g" and sc["title"] == "S2" and sc["notes"] == ""


async def test_update_tool_has_no_status_param():
    import inspect
    tool = await mcp_server.mcp.get_tool("okto_pulse_update_test_scenario")
    assert "status" not in inspect.signature(tool.fn).parameters


async def test_update_tool_semantic_edit_invalidates_evidence(db_factory):
    _b, spec, _c = await _seed(scenarios=[{
        "id": "ts_a", "title": "S", "given": "g", "when": "w", "then": "t",
        "status": "passed", "evidence": dict(_EV),
    }])
    out = await _call("okto_pulse_update_test_scenario", _b, spec_id=spec,
                      scenario_id="ts_a", given="semantic change")
    assert out.get("success") is True, out
    assert out["evidence_invalidated"] is True
    sc = (await _scenarios(spec))[0]
    assert sc["status"] == "ready" and not sc.get("evidence")


async def test_update_tool_cosmetic_edit_preserves_evidence(db_factory):
    _b, spec, _c = await _seed(scenarios=[{
        "id": "ts_a", "title": "S", "given": "g", "when": "w", "then": "t",
        "status": "passed", "evidence": dict(_EV),
    }])
    out = await _call("okto_pulse_update_test_scenario", _b, spec_id=spec,
                      scenario_id="ts_a", title="renamed")
    assert out.get("success") is True and out["evidence_invalidated"] is False, out
    sc = (await _scenarios(spec))[0]
    assert sc["status"] == "passed" and sc["evidence"]["last_run_at"] == _EV["last_run_at"]


async def test_update_tool_respects_content_lock(db_factory):
    _b, spec, _c = await _seed(locked=True, scenarios=[{"id": "ts_a", "title": "S",
        "given": "g", "when": "w", "then": "t", "status": "draft"}])
    out = await _call("okto_pulse_update_test_scenario", _b, spec_id=spec,
                      scenario_id="ts_a", title="nope")
    assert out.get("error") == "spec_locked", out


async def test_update_tool_scenario_not_found(db_factory):
    _b, spec, _c = await _seed(scenarios=[])
    out = await _call("okto_pulse_update_test_scenario", _b, spec_id=spec,
                      scenario_id="ts_ghost", title="x")
    assert out.get("error") == "scenario_not_found", out


# ====================================================================
# okto_pulse_delete_test_scenario (MCP wrapper)
# ====================================================================


async def test_delete_tool_cascade(db_factory):
    _b, spec, card = await _seed(
        status=SpecStatus.APPROVED,
        scenarios=[{"id": "ts_a", "title": "A", "given": "g", "when": "w", "then": "t",
                    "status": "draft"},
                   {"id": "ts_b", "title": "B", "given": "g", "when": "w", "then": "t",
                    "status": "draft"}],
        card_scenarios=["ts_a", "ts_b"],
    )
    out = await _call("okto_pulse_delete_test_scenario", _b, spec_id=spec, scenario_id="ts_a")
    assert out.get("success") is True, out
    assert out["cards_unlinked"] == [card]
    assert [s["id"] for s in await _scenarios(spec)] == ["ts_b"]
    async with db_factory_ref[0]() as db:
        c = await db.get(Card, card)
    assert c.test_scenario_ids == ["ts_b"]  # no orphan


async def test_delete_tool_not_found(db_factory):
    _b, spec, _c = await _seed(status=SpecStatus.APPROVED, scenarios=[])
    out = await _call("okto_pulse_delete_test_scenario", _b, spec_id=spec, scenario_id="ts_ghost")
    assert out.get("error") == "scenario_not_found", out


async def test_delete_tool_respects_content_lock(db_factory):
    _b, spec, _c = await _seed(locked=True, scenarios=[{"id": "ts_a", "title": "S",
        "given": "g", "when": "w", "then": "t", "status": "draft"}])
    out = await _call("okto_pulse_delete_test_scenario", _b, spec_id=spec, scenario_id="ts_a")
    assert out.get("error") == "spec_locked", out
