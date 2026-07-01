"""R01A MCP-FU6 (family: spec) — strangler oracle.

Proves the 33 spec MCP tools were migrated off ``get_db_for_mcp`` / direct service
construction onto the MCP UnitOfWork + the transport-free ``mcp_spec_crud`` use
cases (Codex-corrected: domain in the use case, transport/envelopes in the adapter),
WITHOUT behavior drift.

Consolidated proofs (Codex-mandated):
- AST: all 33 spec tools strangled (no ``get_db_for_mcp``).
- AST purity: ``mcp_spec_crud`` imports neither ``okto_pulse.core.mcp`` nor a
  ``server.py`` helper.
- AST commit-map: ``McpAddTestScenario`` commits in the use case; ``McpUpdate`` /
  ``McpDelete`` ``TestScenario`` do NOT (the service self-commits — no double commit).
- Unit: the resolver moved to core (opt-C) resolves FR-then-TR + dedup + unresolved,
  and ``available_structured_ids`` extracts the canonical ids.
- Unit: the api_contract F9/F10 — an invalid http method becomes the canonical
  ``invalid_api_contract`` with no ``errors.pydantic.dev`` leak.
- Runtime: add business_rule / add api_contract round-trip; the board-scope asymmetry
  (IR is board-scoped, business_rule is not); decision SOFT-remove (status=revoked).
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import Board, Spec, SpecStatus

BOARD_ID = "r01a-mcpspec"
OTHER_BOARD_ID = "r01a-mcpspec-other"
USER_ID = "r01a-mcpspec-agent"

# The 33 spec-family MCP tools (inventory w1ahn926e).
_SPEC_TOOLS = (
    "get_spec", "delete_spec", "get_spec_history", "update_test_scenario_status",
    "move_spec", "update_spec", "create_spec", "get_spec_context",
    "derive_spec_from_ideation", "derive_spec_from_refinement",
    "add_business_rule", "update_business_rule", "remove_business_rule",
    "list_business_rules",
    "add_api_contract", "update_api_contract", "remove_api_contract",
    "list_api_contracts",
    "add_decision", "update_decision", "remove_decision",
    "add_integration_requirement", "list_integration_requirements",
    "add_observability_requirement", "list_observability_requirements",
    "add_test_scenario", "list_test_scenarios", "update_test_scenario",
    "delete_test_scenario",
    "update_spec_entity", "update_spec_api_contract", "remove_spec_entity",
    "migrate_spec_decisions",
)


def _spec_func_blocks() -> dict[str, str]:
    src = Path(mcp_server.__file__).read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.AsyncFunctionDef):
            short = n.name.replace("okto_pulse_", "")
            if short in _SPEC_TOOLS:
                out[short] = ast.get_source_segment(src, n) or ""
    return out


# --- AST proofs (no DB) -----------------------------------------------------


def test_all_spec_tools_strangled():
    blocks = _spec_func_blocks()
    assert set(blocks) == set(_SPEC_TOOLS), (
        f"missing spec tools: {set(_SPEC_TOOLS) - set(blocks)}"
    )
    still = [nm for nm, b in blocks.items() if "async with get_db_for_mcp" in b]
    assert not still, f"spec tools still open get_db_for_mcp: {still}"


def test_mcp_spec_crud_is_transport_free():
    from okto_pulse.core.application.use_cases import mcp_spec_crud

    src = Path(mcp_spec_crud.__file__).read_text(encoding="utf-8")
    bad = [
        (getattr(n, "module", None))
        for n in ast.walk(ast.parse(src))
        if isinstance(n, (ast.Import, ast.ImportFrom))
        and (getattr(n, "module", None) or "").startswith("okto_pulse.core.mcp")
    ]
    assert not bad, f"mcp_spec_crud must not import the MCP transport package: {bad}"


def test_test_scenario_commit_map_no_double_commit():
    """add commits in the use case; update/delete rely on SpecService self-commit and
    must NOT add a UoW commit (double-commit)."""
    from okto_pulse.core.application.use_cases import mcp_spec_crud

    src = Path(mcp_spec_crud.__file__).read_text(encoding="utf-8")
    bodies = {
        n.name: ast.get_source_segment(src, n) or ""
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.ClassDef)
    }
    assert "await commit(uow)" in bodies["McpAddTestScenarioUseCase"]
    assert "await commit(uow)" not in bodies["McpUpdateTestScenarioUseCase"]
    assert "await commit(uow)" not in bodies["McpDeleteTestScenarioUseCase"]


# --- unit proofs (no DB) ----------------------------------------------------


def test_core_resolver_fr_then_tr_dedup_unresolved():
    from okto_pulse.core.services.analytics_service import (
        available_structured_ids,
        resolve_linked_requirement_tokens_to_fr_or_tr_ids,
    )

    frs = [{"id": "fr_a", "text": "login"}, {"id": "fr_b", "text": "logout"}]
    trs = [{"id": "tr_x", "text": "postgres"}]
    resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
        ["0", "tr_x", "fr_a", "nope"], frs, trs
    )
    assert "fr_a" in resolved and "tr_x" in resolved
    assert resolved.count("fr_a") == 1, "dedup failed"
    assert unresolved == ["nope"]
    assert available_structured_ids(frs) == ["fr_a", "fr_b"]
    assert available_structured_ids(trs) == ["tr_x"]


def test_api_contract_f9_f10_canonical_no_pydantic_url():
    from pydantic import ValidationError

    from okto_pulse.core.mcp.server import _canonical_api_contract_error
    from okto_pulse.core.models.schemas import ApiContract

    bad = {
        "id": "api_x", "method": "CALL", "path": "/x", "description": "",
        "request_body": None, "response_success": None, "response_errors": None,
        "linked_requirements": None, "linked_rules": None, "notes": None,
    }
    with pytest.raises(ValidationError) as ei:
        ApiContract.model_validate(bad, context={"on_write": True})
    out = _canonical_api_contract_error(ei.value)
    assert "invalid_api_contract" in out
    assert "errors.pydantic.dev" not in out


# --- runtime harness --------------------------------------------------------


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {"agent_id": USER_ID, "agent_name": "mcp-spec-test", "permissions": ["*"]},
    )()


@pytest.fixture(autouse=True)
def _auth():
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())
    ), patch.object(mcp_server, "check_permission", return_value=None), patch.object(
        mcp_server, "_mcp_check_permission", return_value=None
    ):
        yield


@pytest.fixture
async def _seed():
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    async with factory() as db:
        for bid in (BOARD_ID, OTHER_BOARD_ID):
            if await db.get(Board, bid) is None:
                db.add(Board(id=bid, name="MCP Spec", owner_id=USER_ID))
        await db.flush()
        spec = Spec(
            board_id=BOARD_ID,
            title="MCP Spec Strangler",
            status=SpecStatus.DRAFT,
            created_by=USER_ID,
            functional_requirements=["the system logs in"],
            acceptance_criteria=["login returns a token"],
        )
        db.add(spec)
        await db.flush()
        spec_id = spec.id
        await db.commit()
    return spec_id


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


@pytest.mark.asyncio
async def test_add_business_rule_roundtrip(_seed):
    out = await _call(
        "okto_pulse_add_business_rule",
        board_id=BOARD_ID, spec_id=_seed,
        title="Clamp", rule="MUST clamp", when="score > 1.5", then="clamp to 1.5",
    )
    assert out["success"] is True
    assert out["business_rule"]["id"].startswith("br_")
    listed = await _call(
        "okto_pulse_list_business_rules", board_id=BOARD_ID, spec_id=_seed
    )
    assert any(r["id"] == out["business_rule"]["id"] for r in listed["business_rules"])


@pytest.mark.asyncio
async def test_add_api_contract_roundtrip(_seed):
    out = await _call(
        "okto_pulse_add_api_contract",
        board_id=BOARD_ID, spec_id=_seed, method="get", path="/login",
    )
    assert out["success"] is True
    assert out["api_contract"]["method"] == "GET"


@pytest.mark.asyncio
async def test_add_api_contract_invalid_method_is_canonical(_seed):
    out = await _call(
        "okto_pulse_add_api_contract",
        board_id=BOARD_ID, spec_id=_seed, method="CALL", path="/x",
    )
    assert out["error"] == "invalid_api_contract"
    assert "errors.pydantic.dev" not in json.dumps(out)


@pytest.mark.asyncio
async def test_integration_requirement_is_board_scoped(_seed):
    """IR is board-scoped: the same spec read through the WRONG board is not found."""
    ok = await _call(
        "okto_pulse_list_integration_requirements", board_id=BOARD_ID, spec_id=_seed
    )
    assert ok["spec_id"] == _seed
    cross = await _call(
        "okto_pulse_list_integration_requirements",
        board_id=OTHER_BOARD_ID, spec_id=_seed,
    )
    assert cross == {"error": "Spec not found"}


@pytest.mark.asyncio
async def test_business_rule_is_not_board_scoped(_seed):
    """business_rule is NOT board-scoped (asymmetry preserved): the cross-board read
    still resolves the spec (no "Spec not found")."""
    cross = await _call(
        "okto_pulse_list_business_rules", board_id=OTHER_BOARD_ID, spec_id=_seed
    )
    assert "error" not in cross
    assert "business_rules" in cross


@pytest.mark.asyncio
async def test_decision_remove_is_soft_delete(_seed):
    added = await _call(
        "okto_pulse_add_decision",
        board_id=BOARD_ID, spec_id=_seed,
        title="Pick LadybugDB", rationale="embedded + single-writer",
    )
    dec_id = added["decision"]["id"]
    removed = await _call(
        "okto_pulse_remove_spec_entity",
        board_id=BOARD_ID, spec_id=_seed, target_type="decision", entity_id=dec_id,
    )
    assert removed["success"] is True and removed["revoked"] == dec_id
    assert removed["decision"]["status"] == "revoked"
