"""Regression: okto_pulse_add_business_rule / okto_pulse_add_api_contract must
accept ``linked_requirements`` against STRUCTURED functional requirements.

Bug (found in the 0.2.3 live E2E): once FR/AC canonicalization made
``functional_requirements`` a list of dicts ``{id,text,status}``, the inline
index resolver in add/update_business_rule and add/update_api_contract did
``req_list.append(frs[idx])`` — appending the whole FR dict — which violates
``SpecUpdate.{business_rules,api_contracts}[].linked_requirements: list[str]``
and crashed with ``Input should be a valid string ... input_type=dict``. The
text path (``token in frs``) also always failed (a list of dicts never contains
the bare text). FR->BR/contract linkage was therefore impossible, which blocked
fr_coverage and the spec validation gate. Fix: resolve via the shared
``_parse_linked_requirements`` helper (structured-FR safe), matching how
decisions/IR/OR already resolve.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Spec, SpecStatus
from okto_pulse.core.services.analytics_service import resolve_linked_fr_indices
from okto_pulse.core.services.main import SpecService

pytestmark = pytest.mark.asyncio

USER_ID = "br-fr-agent"

_STRUCTURED_FRS = [
    {"id": "fr_1111aaaa", "text": "User can log in", "status": "active"},
    {"id": "fr_2222bbbb", "text": "Session expires after timeout", "status": "active"},
]


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


async def _seed(db_factory) -> tuple[str, str]:
    board_id = _id("brfr-board")
    spec_id = _id("brfr-spec")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="BR-FR Board", owner_id=USER_ID, settings={}))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="BR-FR Spec",
                status=SpecStatus.DRAFT,
                created_by=USER_ID,
                functional_requirements=[dict(f) for f in _STRUCTURED_FRS],
                acceptance_criteria=[],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _call(tool_name: str, board_id: str, **kwargs) -> dict:
    mcp_server.register_session_factory(db_factory_ref[0])
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(tool_name)
        raw = await tool.fn(board_id=board_id, **kwargs)
    return json.loads(raw)


db_factory_ref: list = [None]


@pytest.fixture(autouse=True)
def _bind_db_factory(db_factory):
    db_factory_ref[0] = db_factory
    yield
    db_factory_ref[0] = None


async def _read_spec(spec_id: str):
    async with db_factory_ref[0]() as db:
        return await SpecService(db).get_spec(spec_id)


# ====================================================================
# add_business_rule — the crash repro + every link form
# ====================================================================


async def test_add_business_rule_index_structured_fr(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_business_rule",
        board_id,
        spec_id=spec_id,
        title="Cap login attempts",
        rule="max 5",
        when="bad password",
        then="lock 15m",
        linked_requirements="0",
    )
    assert payload.get("success") is True, payload  # no Pydantic crash
    spec = await _read_spec(spec_id)
    assert len(spec.business_rules) == 1
    linked = spec.business_rules[0]["linked_requirements"]
    # Persisted as list[str], NOT a dict — normalized to the canonical FR index.
    assert linked == ["0"], linked
    assert resolve_linked_fr_indices(linked, spec.functional_requirements) == {0}


async def test_add_business_rule_text_structured_fr(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_business_rule",
        board_id,
        spec_id=spec_id,
        title="R",
        rule="r",
        when="w",
        then="t",
        linked_requirements="User can log in",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.business_rules[0]["linked_requirements"]
    # text token normalized to the canonical FR index "0"
    assert linked == ["0"], linked
    assert resolve_linked_fr_indices(linked, spec.functional_requirements) == {0}


async def test_add_business_rule_fr_id_structured_fr(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_business_rule",
        board_id,
        spec_id=spec_id,
        title="R",
        rule="r",
        when="w",
        then="t",
        linked_requirements="fr_2222bbbb",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.business_rules[0]["linked_requirements"]
    # fr_id token normalized to the canonical FR index "1"
    assert linked == ["1"], linked
    assert resolve_linked_fr_indices(linked, spec.functional_requirements) == {1}


async def test_add_business_rule_index_out_of_range(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_business_rule",
        board_id,
        spec_id=spec_id,
        title="R",
        rule="r",
        when="w",
        then="t",
        linked_requirements="9",
    )
    assert "error" in payload and "not found" in payload["error"], payload


# ====================================================================
# add_api_contract — same crash, same fix
# ====================================================================


async def test_add_api_contract_index_structured_fr(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_api_contract",
        board_id,
        spec_id=spec_id,
        method="POST",
        path="/api/login",
        linked_requirements="1",
    )
    assert payload.get("success") is True, payload  # no Pydantic crash
    spec = await _read_spec(spec_id)
    assert len(spec.api_contracts) == 1
    linked = spec.api_contracts[0]["linked_requirements"]
    assert linked == ["1"], linked  # normalized to canonical FR index
    assert resolve_linked_fr_indices(linked, spec.functional_requirements) == {1}
