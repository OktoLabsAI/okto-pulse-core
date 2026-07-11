"""Integration tests for the okto_pulse_add_test_scenario MCP tool against
STRUCTURED acceptance criteria.

Spec aafcc73f / KB 26b0e005. These exercise the real MCP tool (not just the
helper): write resolution is strict (index / ac_id / exact text), persists
canonical ac_id strings, is fail-closed and atomic on unresolved tokens, and
keeps the tolerant read resolver for coverage. Covers the 9 spec scenarios
(TC-1..TC-5).
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.services.main import SpecService

pytestmark = pytest.mark.asyncio

USER_ID = "add-scenario-agent"

# Structured ACs (dict + id) — the shape produced by StructuredSpecEntityService.
_STRUCTURED_ACS = [
    {"id": "ac_aaaa1111", "text": "User can log in with valid token", "status": "active"},
    {"id": "ac_bbbb2222", "text": "Session expires after timeout", "status": "active"},
    {"id": "ac_cccc3333", "text": "Invalid token is rejected", "status": "active"},
]

_LEGACY_ACS = ["AC0 legacy behavior", "AC1 legacy behavior"]


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


async def _seed(db_factory, acs: list) -> tuple[str, str]:
    board_id = _id("ats-board")
    spec_id = _id("ats-spec")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Add-Scenario Board",
                owner_id=USER_ID,
                settings={"skip_test_coverage_global": False},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Add-Scenario Spec",
                status=SpecStatus.DRAFT,
                created_by=USER_ID,
                acceptance_criteria=acs,
                test_scenarios=[],
                functional_requirements=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _add_scenario(db_factory, board_id, spec_id, *, linked_criteria, title="Scenario"):
    mcp_server.register_session_factory(db_factory)
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_add_test_scenario")
        raw = await tool.fn(
            board_id=board_id,
            spec_id=spec_id,
            title=title,
            given="Given a structured spec",
            when="When add_test_scenario is called",
            then="Then linked_criteria resolves",
            linked_criteria=linked_criteria,
        )
    return json.loads(raw)


async def _scenarios(db_factory, spec_id) -> list:
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        return list(spec.test_scenarios or [])


# --- TC-1: index / ac_id -> ac_id -----------------------------------------

async def test_index_persists_ac_id(db_factory):
    """ts_dba06b3b — structured AC + 0-based index -> [ac_id] as list[str]."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="0")

    assert payload.get("success") is True, payload
    assert payload["scenario"]["linked_criteria"] == ["ac_aaaa1111"]
    assert all(isinstance(x, str) for x in payload["scenario"]["linked_criteria"])


async def test_ac_id_persists_ac_id(db_factory):
    """ts_ba7be759 — structured AC + ac_id token -> [ac_id] (no 'not found')."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="ac_bbbb2222")

    assert payload.get("success") is True, payload
    assert payload["scenario"]["linked_criteria"] == ["ac_bbbb2222"]


# --- TC-2: exact text -> ac_id; legacy regression -------------------------

async def test_exact_text_persists_ac_id(db_factory):
    """ts_51c8fe5e — exact AC text -> the ac_id, not the text nor the dict."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(
        db_factory, board_id, spec_id, linked_criteria="Invalid token is rejected"
    )

    assert payload.get("success") is True, payload
    assert payload["scenario"]["linked_criteria"] == ["ac_cccc3333"]


async def test_legacy_string_ac_regression(db_factory):
    """ts_abe407c5 — legacy string AC by index/text resolves to its text; no error."""
    board_id, spec_id = await _seed(db_factory, _LEGACY_ACS)

    by_index = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="0")
    assert by_index.get("success") is True, by_index
    assert by_index["scenario"]["linked_criteria"] == ["AC0 legacy behavior"]

    by_text = await _add_scenario(
        db_factory, board_id, spec_id, linked_criteria="AC1 legacy behavior"
    )
    assert by_text.get("success") is True, by_text
    assert by_text["scenario"]["linked_criteria"] == ["AC1 legacy behavior"]


# --- TC-3: fail-closed / atomic -------------------------------------------

async def test_unresolved_token_structured_error(db_factory):
    """ts_b5a4dbbb — unresolved token -> structured error, nothing appended."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="99")

    assert "error" in payload, payload
    assert "Available ac_ids" in payload["error"]
    assert "ac_aaaa1111" in payload["error"]
    assert "No scenario was appended" in payload["error"]
    assert await _scenarios(db_factory, spec_id) == []


async def test_mixed_valid_invalid_is_atomic(db_factory):
    """ts_e87a5fc0 — '0|ghost' fails closed: error mentions ghost, nothing partial."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="0|ghost")

    assert "error" in payload, payload
    assert "ghost" in payload["error"]
    # atomic: no scenario appended and no partial ac_aaaa1111 persisted.
    assert await _scenarios(db_factory, spec_id) == []


# --- TC-4: multi-value shapes + coverage round-trip -----------------------

async def test_json_array_and_pipe_resolve_equal(db_factory):
    """ts_69b44f10 — JSON-array and pipe inputs yield the same ordered list[str]."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)

    via_json = await _add_scenario(
        db_factory, board_id, spec_id, linked_criteria='["0","1"]', title="json"
    )
    via_pipe = await _add_scenario(
        db_factory, board_id, spec_id, linked_criteria="0|1", title="pipe"
    )

    assert via_json.get("success") is True, via_json
    assert via_pipe.get("success") is True, via_pipe
    assert via_json["scenario"]["linked_criteria"] == ["ac_aaaa1111", "ac_bbbb2222"]
    assert (
        via_pipe["scenario"]["linked_criteria"]
        == via_json["scenario"]["linked_criteria"]
    )


async def test_coverage_round_trip_after_create(db_factory):
    """ts_8a4e5a5f — after creating via the tool, list_test_scenarios sees the AC covered."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    created = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="ac_cccc3333")
    assert created.get("success") is True, created

    mcp_server.register_session_factory(db_factory)
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool("okto_pulse_list_test_scenarios")
        listed = json.loads(await tool.fn(board_id=board_id, spec_id=spec_id))

    # AC index 2 (ac_cccc3333) is now covered by the scenario we just created.
    assert 2 not in listed["coverage"]["uncovered_indices"]
    assert listed["coverage"]["details"].get("2"), listed["coverage"]


# --- TC-5: prefix is read-only --------------------------------------------

async def test_prefix_unresolved_on_write_but_read_tolerant(db_factory):
    """ts_32748b43 — prefix token is unresolved on write; read resolver still tolerates it."""
    board_id, spec_id = await _seed(db_factory, _STRUCTURED_ACS)
    payload = await _add_scenario(db_factory, board_id, spec_id, linked_criteria="User can log")

    assert "error" in payload, payload
    assert await _scenarios(db_factory, spec_id) == []

    # read-path tolerance is intact (prefix still maps to index 0).
    from okto_pulse.core.services.analytics_service import resolve_linked_criteria_to_indices

    assert resolve_linked_criteria_to_indices(["User can log"], _STRUCTURED_ACS) == {0}
