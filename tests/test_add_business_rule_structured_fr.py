"""Regression: structured ``linked_requirements`` must resolve canonically.

Bug (found in the 0.2.3 live E2E): once FR/AC canonicalization made
``functional_requirements`` a list of dicts ``{id,text,status}``, the inline
index resolver in add/update_business_rule and add/update_api_contract did
``req_list.append(frs[idx])`` — appending the whole FR dict — which violates
``SpecUpdate.{business_rules,api_contracts}[].linked_requirements: list[str]``
and crashed with ``Input should be a valid string ... input_type=dict``. The
text path (``token in frs``) also always failed (a list of dicts never contains
the bare text). FR->BR/contract linkage was therefore impossible, which blocked
fr_coverage and the spec validation gate.

IMPL-1 (spec 9d66847f): write-path now resolves to canonical requirement IDs
(not indices). BR/Decision stay FR-scoped; API contracts, IR, and OR accept
FR/TR references. Fail-closed: any unresolved token aborts before persistence.
The tolerant read resolver (``resolve_linked_fr_indices``) is unchanged — it
still handles both fr_ids and indices on read.
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

_STRUCTURED_TRS = [
    {"id": "tr_3333cccc", "text": "Login API must emit audit events", "linked_task_ids": []},
    {"id": "tr_4444dddd", "text": "Session timeout must be configurable", "linked_task_ids": []},
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
                technical_requirements=[dict(t) for t in _STRUCTURED_TRS],
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
    # IMPL-1: persisted as canonical fr_id, not index. Index "0" resolves to
    # fr_1111aaaa. The tolerant read resolver still maps fr_ids back to indices.
    assert linked == ["fr_1111aaaa"], linked
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
    # IMPL-1: text token resolves to canonical fr_id "fr_1111aaaa".
    # The tolerant read resolver maps fr_ids back to indices.
    assert linked == ["fr_1111aaaa"], linked
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
    # IMPL-1: fr_id token persisted as canonical fr_id directly.
    # The tolerant read resolver maps fr_ids back to indices.
    assert linked == ["fr_2222bbbb"], linked
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
    # IMPL-1: fail-closed; unresolved token aborts before persistence.
    assert "error" in payload and "Unresolved" in payload["error"], payload
    # ...AND the write is ATOMIC: re-read the spec and prove the business_rule
    # was NOT persisted (a bug that returns the error but still appends the BR
    # would be caught here — the atomicity clause of the `then`).
    spec = await _read_spec(spec_id)
    assert (spec.business_rules or []) == [], (
        "fail-closed must be atomic: nothing persisted on an unresolved FR ref, "
        f"got {spec.business_rules!r}"
    )


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
    # IMPL-1: index "1" resolves to canonical fr_id "fr_2222bbbb".
    # The tolerant read resolver maps fr_ids back to indices.
    assert linked == ["fr_2222bbbb"], linked
    assert resolve_linked_fr_indices(linked, spec.functional_requirements) == {1}


async def test_add_api_contract_tr_id_structured_requirement(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_api_contract",
        board_id,
        spec_id=spec_id,
        method="POST",
        path="/api/login",
        linked_requirements="tr_3333cccc",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.api_contracts[0]["linked_requirements"]
    assert linked == ["tr_3333cccc"], linked

    listed = await _call(
        "okto_pulse_list_api_contracts",
        board_id,
        spec_id=spec_id,
    )
    contract = listed["api_contracts"][0]
    assert contract["linked_requirements"] == ["tr_3333cccc"], contract
    assert "unresolved_requirements" not in contract
    assert contract["resolved_requirements"] == [
        "[TR-tr_3333cccc] Login API must emit audit events"
    ]


async def test_update_api_contract_tr_text_structured_requirement(db_factory):
    board_id, spec_id = await _seed(db_factory)
    created = await _call(
        "okto_pulse_add_api_contract",
        board_id,
        spec_id=spec_id,
        method="POST",
        path="/api/login",
    )
    assert created.get("success") is True, created
    contract_id = created["api_contract"]["id"]

    payload = await _call(
        "okto_pulse_update_api_contract",
        board_id,
        spec_id=spec_id,
        contract_id=contract_id,
        linked_requirements="Session timeout must be configurable",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.api_contracts[0]["linked_requirements"]
    assert linked == ["tr_4444dddd"], linked


# ====================================================================
# add_integration/observability_requirement — linked_requirements accept TRs
# ====================================================================


async def test_add_integration_requirement_tr_id_structured_requirement(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_integration_requirement",
        board_id,
        spec_id=spec_id,
        title="Audit export",
        integration_type="external_service",
        linked_requirements="tr_3333cccc",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.integration_requirements[0]["linked_requirements"]
    assert linked == ["tr_3333cccc"], linked


async def test_add_observability_requirement_tr_text_structured_requirement(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_observability_requirement",
        board_id,
        spec_id=spec_id,
        title="Session timeout configuration metric",
        signal_type="metric",
        linked_requirements="Session timeout must be configurable",
    )
    assert payload.get("success") is True, payload
    spec = await _read_spec(spec_id)
    linked = spec.observability_requirements[0]["linked_requirements"]
    assert linked == ["tr_4444dddd"], linked


async def test_add_integration_requirement_unknown_requirement_fails_closed(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call(
        "okto_pulse_add_integration_requirement",
        board_id,
        spec_id=spec_id,
        title="Broken link",
        integration_type="api",
        linked_requirements="tr_missing",
    )
    assert "error" in payload and "Unresolved" in payload["error"], payload
    assert "Available tr_ids" in payload["error"], payload
    spec = await _read_spec(spec_id)
    assert (spec.integration_requirements or []) == []
