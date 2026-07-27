"""R3a — MCP schema honesty + uniform error envelope (spec d41c7209, FR1-FR8).

Behavioural tests backing TEST-A..D:
- AC1 ts_0f235833, AC2 ts_6a0accab (TEST-A) — anyOf snapshot of the multi-value cluster
- AC3 ts_995e7ee1, AC4 ts_feb570a8 (TEST-B) — *_json anyOf + native dict/list acceptance
- AC5 ts_652ff72f, AC6 ts_e32062e6 (TEST-C) — {error,detail} envelope on comma-only (REAL handler)
- AC7 ts_6768b99c, AC8 ts_36a86f39 (TEST-D) — backward-compat + scope guard

Schema tests read the REAL FastMCP ``FunctionTool.parameters`` (the client-facing
JSON schema), NOT source text. Handler tests call the REAL handler via ``.fn`` with
stubbed auth and the conftest DB factory (mirrors tests/test_card_knowledge_mcp.py).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus

CORE = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core"
HELPERS_PY = CORE / "mcp" / "helpers.py"
SERVER_PY = CORE / "mcp" / "server.py"

BOARD_ID = "r3a-board-001"
USER_ID = "r3a-agent-001"


# ---------------------------------------------------------------------------
# Schema helpers — read the REAL FastMCP tool schema (not source)
# ---------------------------------------------------------------------------


def _field_schema(tool_name: str, field: str) -> dict:
    tool = getattr(mcp_server, tool_name)
    params = tool.parameters
    props = params["properties"]
    assert field in props, f"{tool_name} has no field '{field}' in schema"
    return props[field]


def _anyof_branches(sch: dict) -> list[dict]:
    return sch.get("anyOf") or []


def _is_anyof_array_string(sch: dict) -> bool:
    b = _anyof_branches(sch)
    has_array = any(x.get("type") == "array" and x.get("items", {}).get("type") == "string" for x in b)
    has_string = any(x.get("type") == "string" for x in b)
    return has_array and has_string


def _is_anyof_object_string(sch: dict) -> bool:
    b = _anyof_branches(sch)
    has_object = any(x.get("type") == "object" for x in b)
    has_string = any(x.get("type") == "string" for x in b)
    return has_object and has_string


def _is_anyof_arrayobject_string(sch: dict) -> bool:
    b = _anyof_branches(sch)
    has_array_obj = any(
        x.get("type") == "array" and x.get("items", {}).get("type") == "object" for x in b
    )
    has_string = any(x.get("type") == "string" for x in b)
    return has_array_obj and has_string


# ---------------------------------------------------------------------------
# AC1 (ts_0f235833) — refinement multi-value fields are anyOf[array, string]
# ---------------------------------------------------------------------------


def test_ac1_refinement_fields_anyof_array_string():
    for tool in ("okto_pulse_create_refinement", "okto_pulse_update_refinement"):
        for field in ("in_scope", "out_of_scope", "decisions"):
            sch = _field_schema(tool, field)
            assert _is_anyof_array_string(sch), f"{tool}.{field} not anyOf[array,string]: {sch}"
    # Control: a genuine single-value field stays a bare string (proves anyOf is field-specific).
    bid = _field_schema("okto_pulse_create_refinement", "board_id")
    assert bid.get("type") == "string" and "anyOf" not in bid, bid


# ---------------------------------------------------------------------------
# AC2 (ts_6a0accab) — expanded cluster fields are anyOf[array, string]
# ---------------------------------------------------------------------------


def test_ac2_expanded_cluster_anyof_array_string():
    assert _is_anyof_array_string(_field_schema("okto_pulse_create_spec", "functional_requirements"))
    assert _is_anyof_array_string(_field_schema("okto_pulse_create_spec", "technical_requirements"))
    assert _is_anyof_array_string(_field_schema("okto_pulse_create_spec", "acceptance_criteria"))
    assert _is_anyof_array_string(_field_schema("okto_pulse_add_decision", "alternatives_considered"))


# ---------------------------------------------------------------------------
# AC3 (ts_995e7ee1) — *_json fields: object vs array (LIST-vs-OBJECT asymmetry)
# ---------------------------------------------------------------------------


def test_ac3_json_fields_object_and_array_asymmetry():
    # OBJECT-typed
    assert _is_anyof_object_string(_field_schema("okto_pulse_add_api_contract", "request_body_json"))
    assert _is_anyof_object_string(_field_schema("okto_pulse_add_api_contract", "response_success_json"))
    assert _is_anyof_object_string(_field_schema("okto_pulse_add_integration_requirement", "data_contract_json"))
    assert _is_anyof_object_string(_field_schema("okto_pulse_update_spec_entity", "payload_json"))
    # LIST-typed (the asymmetry): response_errors_json is array-of-object, NOT object
    resp = _field_schema("okto_pulse_add_api_contract", "response_errors_json")
    assert _is_anyof_arrayobject_string(resp), f"response_errors_json must be anyOf[array-of-object,string]: {resp}"
    assert not _is_anyof_object_string(resp), "response_errors_json must NOT be object-typed (asymmetry)"


# ---------------------------------------------------------------------------
# DB-backed handler fixtures
# ---------------------------------------------------------------------------


def _stub_ctx():
    return type("Ctx", (), {
        "agent_id": USER_ID,
        "agent_name": "r3a-agent",
        "permissions": ["*"],
    })()


@pytest.fixture(autouse=True)
def _stub_auth():
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        register_mcp_test_runtime(__import__("okto_pulse.core.infra.database", fromlist=["get_session_factory"]).get_session_factory())
        yield


@pytest.fixture
async def _seed_spec():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    spec_id = str(uuid.uuid4())
    async with factory() as db:
        if await db.get(Board, BOARD_ID) is None:
            db.add(Board(id=BOARD_ID, name="R3a board", owner_id=USER_ID))
            await db.flush()
        db.add(Spec(
            id=spec_id, board_id=BOARD_ID, title="R3a spec",
            status=SpecStatus.APPROVED, created_by=USER_ID,
            functional_requirements=["FR1"], acceptance_criteria=["AC1"],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        await db.commit()
    return spec_id


async def _call(_tool_name: str, /, **kwargs):
    tool = getattr(mcp_server, _tool_name)
    raw = await tool.fn(**kwargs)
    return json.loads(raw)


# ---------------------------------------------------------------------------
# AC4 (ts_feb570a8) — *_json handler accepts NATIVE dict/list (isinstance branch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac4_json_handler_accepts_native_dict_and_list(_seed_spec):
    spec_id = _seed_spec
    res = await _call(
        "okto_pulse_add_api_contract",
        board_id=BOARD_ID, spec_id=spec_id,
        method="POST", path="/users", description="Create user",
        request_body_json={"type": "object", "properties": {"email": {"type": "string"}}},
        response_errors_json=[{"code": 400, "message": "bad"}],
    )
    assert res.get("error") is None, res
    # The native dict/list survived (no json.loads on a dict → no crash, persisted as-is).
    contract = res.get("contract") or res.get("api_contract") or res
    body = json.dumps(contract)
    assert "email" in body and "400" in body, res


# ---------------------------------------------------------------------------
# AC5 (ts_652ff72f) — comma-only on create_refinement → {error,detail}, no leak
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac5_create_refinement_comma_only_returns_envelope():
    # coerce_to_list_str runs at the top of the handler, before the ideation lookup,
    # so a fake ideation_id is fine — the envelope must return without a raised ValueError.
    raw = await mcp_server.okto_pulse_create_refinement.fn(
        board_id=BOARD_ID, ideation_id="ideation-fake",
        title="x", in_scope="alpha, beta, gamma",
    )
    res = json.loads(raw)  # no exception propagated == no raw ValueError leak
    assert res.get("error") == "invalid_multi_value_input", res
    assert "detail" in res and res["detail"], res


# ---------------------------------------------------------------------------
# AC6 (ts_e32062e6) — comma-only on a cluster field → uniform {error,detail}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac6_add_decision_comma_only_uniform_envelope(_seed_spec):
    spec_id = _seed_spec
    raw = await mcp_server.okto_pulse_add_decision.fn(
        board_id=BOARD_ID, spec_id=spec_id,
        title="Pick a store", rationale="because",
        alternatives_considered="Postgres, DuckDB, SQLite",
    )
    res = json.loads(raw)
    assert res.get("error") == "invalid_multi_value_input", res
    assert "detail" in res and res["detail"], res


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "kwargs"),
    [
        (
            "okto_pulse_create_card",
            {
                "title": "Card envelope",
                "spec_id": "spec-fake",
                "labels": "alpha,beta",
            },
        ),
        (
            "okto_pulse_update_card",
            {"card_id": "card-fake", "labels": "alpha,beta"},
        ),
        (
            "okto_pulse_update_card",
            {"card_id": "card-fake", "test_scenario_ids": "ts_a,ts_b"},
        ),
        (
            "okto_pulse_update_card",
            {"card_id": "card-fake", "linked_test_task_ids": "a,b"},
        ),
    ],
)
async def test_card_multi_value_errors_use_uniform_envelope(tool_name, kwargs):
    raw = await getattr(mcp_server, tool_name).fn(board_id=BOARD_ID, **kwargs)
    res = json.loads(raw)
    assert res.get("error") == "invalid_multi_value_input", res
    assert res.get("detail"), res


# ---------------------------------------------------------------------------
# AC7 (ts_6768b99c) — legacy string forms still accepted (ADDITIVE backward-compat)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac7_legacy_forms_equivalent_to_native(_seed_spec):
    spec_id = _seed_spec
    forms = {
        "native_list": ["Postgres", "DuckDB"],
        "pipe_string": "Postgres|DuckDB",
        "json_array_string": '["Postgres", "DuckDB"]',
    }
    persisted = {}
    for label, value in forms.items():
        res = await _call(
            "okto_pulse_add_decision",
            board_id=BOARD_ID, spec_id=spec_id,
            title=f"Decision {label}", rationale="r",
            alternatives_considered=value,
        )
        assert res.get("error") is None, (label, res)
        dec = res.get("decision") or res
        persisted[label] = dec.get("alternatives_considered")
    assert persisted["native_list"] == ["Postgres", "DuckDB"], persisted
    assert persisted["pipe_string"] == ["Postgres", "DuckDB"], persisted
    assert persisted["json_array_string"] == ["Postgres", "DuckDB"], persisted


# ---------------------------------------------------------------------------
# AC8 (ts_36a86f39) — scope guard: asymmetry + no sweep + strict_mode intact
# ---------------------------------------------------------------------------


def test_ac8_scope_guard_no_sweep_strict_mode_intact():
    # (a) Asymmetry preserved in the real schema.
    req = _field_schema("okto_pulse_add_api_contract", "request_body_json")
    resp = _field_schema("okto_pulse_add_api_contract", "response_errors_json")
    assert _is_anyof_object_string(req), req
    assert _is_anyof_arrayobject_string(resp), resp

    # (b) _auth_error / _perm_error untouched — still the bare error-string shape.
    server_src = SERVER_PY.read_text(encoding="utf-8")
    assert '"error": "Authentication failed or board access denied"' in server_src
    # No global sweep: the error-string idiom still dominates (auth/perm kept their shape).
    assert "def _auth_error()" in server_src and "def _perm_error(" in server_src

    # (c) strict_mode default is still True (not flipped) in helpers.py.
    helpers_src = HELPERS_PY.read_text(encoding="utf-8")
    assert "strict_mode: bool = True" in helpers_src, "strict_mode default must remain True"

    # (d) The uniform multi-value envelope is scoped, not a 353-site sweep: it is a
    # distinct code string from the auth/perm error-string family.
    assert '"invalid_multi_value_input"' in server_src
