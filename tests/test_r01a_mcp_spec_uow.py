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
- Runtime: add business_rule / add api_contract round-trip; every legacy JSON-list
  Spec read/write is board-scoped; decision SOFT-remove (status=revoked).
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import ast
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import event, func, inspect, select
from sqlalchemy.orm import Session

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    Spec,
    SpecHistory,
    SpecKnowledgeBase,
    SpecQAItem,
    SpecStatus,
)
from knowledge_governance_test_data import valid_governance_metadata

BOARD_ID = "r01a-mcpspec"
OTHER_BOARD_ID = "r01a-mcpspec-other"
USER_ID = "r01a-mcpspec-agent"
KNOWLEDGE_ID_PREFIX = "r01a-mcpspec-kb"
EVALUATION_ID = "eval_r01a_mcp"
CONTRACT_ID = "api_scope"
TECHNICAL_REQUIREMENT_ID = "tr_scope"
DECISION_ID = "dec_scope"
INTEGRATION_REQUIREMENT_ID = "ir_scope"
OBSERVABILITY_REQUIREMENT_ID = "or_scope"


def _seed_card_id(spec_id: str) -> str:
    return f"card-scope-{spec_id[:8]}"


def _seed_foreign_card_id(spec_id: str) -> str:
    return f"card-foreign-{spec_id[:8]}"


def _seed_qa_id(spec_id: str) -> str:
    return f"qa-scope-{spec_id[:8]}"


# The 33 spec-family MCP tools (inventory w1ahn926e).
_SPEC_TOOLS = (
    "get_spec",
    "delete_spec",
    "get_spec_history",
    "update_test_scenario_status",
    "move_spec",
    "update_spec",
    "create_spec",
    "get_spec_context",
    "derive_spec_from_ideation",
    "derive_spec_from_refinement",
    "add_business_rule",
    "update_business_rule",
    "remove_business_rule",
    "list_business_rules",
    "add_api_contract",
    "update_api_contract",
    "remove_api_contract",
    "list_api_contracts",
    "add_decision",
    "update_decision",
    "remove_decision",
    "add_integration_requirement",
    "list_integration_requirements",
    "add_observability_requirement",
    "list_observability_requirements",
    "add_test_scenario",
    "list_test_scenarios",
    "update_test_scenario",
    "delete_test_scenario",
    "update_spec_entity",
    "update_spec_api_contract",
    "remove_spec_entity",
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


@pytest.mark.asyncio
async def test_move_spec_refreshes_inside_transaction_before_commit() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.mcp_spec_crud import (
        McpMoveSpecCommand,
        McpMoveSpecUseCase,
    )

    spec = SimpleNamespace(
        id="spec-transaction-order",
        board_id=BOARD_ID,
        status=SpecStatus.DRAFT,
        edition=2,
        version=5,
    )

    class Specs:
        async def get_spec(self, _spec_id: str):
            return spec

        async def move_spec(self, *_args, **_kwargs):
            spec.status = SpecStatus.REVIEW
            return spec

    class UnitOfWork:
        services = SimpleNamespace(specs=Specs())

        def __init__(self, *, fail_reload: bool = False) -> None:
            self.events: list[str] = []
            self.fail_reload = fail_reload

        async def synchronize(self) -> None:
            self.events.append("synchronize")

        async def reload(self, entity: object, *, fields: tuple[str, ...] = ()) -> None:
            assert fields == ("status", "edition", "version")
            self.events.append("reload")
            if self.fail_reload:
                raise RuntimeError("refresh failed")
            entity.version = 6

        async def commit(self) -> None:
            self.events.append("commit")

    actor = ActorContext(
        USER_ID,
        "mcp",
        board_id=BOARD_ID,
        permissions=["specs:move"],
    )
    successful = UnitOfWork()
    result = await McpMoveSpecUseCase().execute(
        McpMoveSpecCommand(
            spec.id,
            BOARD_ID,
            SimpleNamespace(status=SpecStatus.REVIEW),
        ),
        actor=actor,
        uow=successful,
    )
    assert successful.events == ["synchronize", "reload", "commit"]
    assert result.spec.version == 6

    spec.status = SpecStatus.DRAFT
    failing = UnitOfWork(fail_reload=True)
    with pytest.raises(RuntimeError, match="refresh failed"):
        await McpMoveSpecUseCase().execute(
            McpMoveSpecCommand(
                spec.id,
                BOARD_ID,
                SimpleNamespace(status=SpecStatus.REVIEW),
            ),
            actor=actor,
            uow=failing,
        )
    assert failing.events == ["synchronize", "reload"]


def test_api_contract_f9_f10_canonical_no_pydantic_url():
    from pydantic import ValidationError

    from okto_pulse.core.mcp.server import _canonical_api_contract_error
    from okto_pulse.core.models.schemas import ApiContract

    bad = {
        "id": "api_x",
        "method": "CALL",
        "path": "/x",
        "description": "",
        "request_body": None,
        "response_success": None,
        "response_errors": None,
        "linked_requirements": None,
        "linked_rules": None,
        "notes": None,
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
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(mcp_server, "_mcp_check_permission", return_value=None),
    ):
        yield


@pytest.fixture
async def _seed():
    from okto_pulse.core.domain.code_traceability import (
        DeliveryContext,
        DirectSpecDeliveryContextProvenance,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.main import (
        _direct_spec_source_context_manifest,
    )

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
            edition=4,
            created_by=USER_ID,
            functional_requirements=["the system logs in"],
            technical_requirements=[
                {
                    "id": TECHNICAL_REQUIREMENT_ID,
                    "text": "Use scoped storage",
                    "linked_task_ids": [],
                }
            ],
            acceptance_criteria=["login returns a token"],
            api_contracts=[
                {
                    "id": CONTRACT_ID,
                    "method": "GET",
                    "path": "/scope",
                    "linked_task_ids": [],
                }
            ],
            decisions=[
                {
                    "id": DECISION_ID,
                    "title": "Keep scope explicit",
                    "rationale": "Prevent cross-board mutation",
                    "linked_task_ids": [],
                }
            ],
            integration_requirements=[
                {
                    "id": INTEGRATION_REQUIREMENT_ID,
                    "title": "Scoped integration",
                    "linked_task_ids": [],
                }
            ],
            observability_requirements=[
                {
                    "id": OBSERVABILITY_REQUIREMENT_ID,
                    "title": "Scoped observability",
                    "linked_task_ids": [],
                }
            ],
            evaluations=[
                {
                    "id": EVALUATION_ID,
                    "evaluator_id": USER_ID,
                    "evaluator_name": USER_ID,
                    "evaluator_type": "agent",
                    "overall_score": 80,
                    "recommendation": "approve",
                    "stale": False,
                }
            ],
        )
        db.add(spec)
        await db.flush()
        spec_id = spec.id
        spec.delivery_context = "brownfield"
        provenance = DirectSpecDeliveryContextProvenance(
            value=DeliveryContext.BROWNFIELD,
            source_spec_id=spec_id,
            source_spec_version=1,
        )
        spec.delivery_context_provenance = {
            "value": provenance.value.value,
            "source_spec_id": provenance.source_spec_id,
            "source_spec_version": provenance.source_spec_version,
        }
        (
            spec.source_context_manifest,
            spec.source_context_sha256,
        ) = _direct_spec_source_context_manifest(
            spec_id=spec_id,
            delivery_context=DeliveryContext.BROWNFIELD,
            provenance=provenance,
        )
        db.add(
            Card(
                id=_seed_card_id(spec_id),
                board_id=BOARD_ID,
                title="Scoped link card",
                created_by=USER_ID,
            )
        )
        db.add(
            Card(
                id=_seed_foreign_card_id(spec_id),
                board_id=OTHER_BOARD_ID,
                title="Foreign link card",
                created_by=USER_ID,
            )
        )
        db.add(
            SpecQAItem(
                id=_seed_qa_id(spec_id),
                spec_id=spec_id,
                question="Can another agent answer?",
                question_type="text",
                asked_by="different-asker",
            )
        )
        db.add(
            SpecKnowledgeBase(
                id=f"{KNOWLEDGE_ID_PREFIX}-{spec_id}",
                spec_id=spec_id,
                title="Scoped KB",
                content="private board content",
                mime_type="text/markdown",
                created_by=USER_ID,
            )
        )
        await db.commit()
    return spec_id


@pytest.mark.asyncio
async def test_update_spec_exposes_delivery_context_bootstrap(_seed) -> None:
    updated = await _call(
        "okto_pulse_update_spec",
        board_id=BOARD_ID,
        spec_id=_seed,
        delivery_context="greenfield",
    )

    assert updated["success"] is True
    assert updated["spec"]["delivery_context"] == "greenfield"
    assert updated["spec"]["delivery_context_provenance"] == {
        "value": "greenfield",
        "source_spec_id": _seed,
        "source_spec_version": 2,
    }


async def _call(tool: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    t = await mcp_server.mcp.get_tool(tool)
    return json.loads(await t.fn(**kwargs))


async def _spec_mutation_state(spec_id: str) -> dict:
    """Capture persisted Spec state plus both mutation audit surfaces."""
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        history_count = await db.scalar(
            select(func.count())
            .select_from(SpecHistory)
            .where(SpecHistory.spec_id == spec_id)
        )
        activity_count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.board_id == BOARD_ID)
        )
        foreign_activity_count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.board_id == OTHER_BOARD_ID)
        )
        knowledge_ids = list(
            await db.scalars(
                select(SpecKnowledgeBase.id).where(SpecKnowledgeBase.spec_id == spec_id)
            )
        )
        qa_items = list(
            await db.scalars(
                select(SpecQAItem)
                .where(SpecQAItem.spec_id == spec_id)
                .order_by(SpecQAItem.id)
            )
        )
        card = await db.get(Card, _seed_card_id(spec_id))
        foreign_card = await db.get(Card, _seed_foreign_card_id(spec_id))
        state = {
            "exists": spec is not None,
            "knowledge_ids": knowledge_ids,
            "qa_items": [
                {
                    "id": item.id,
                    "answer": item.answer,
                    "selected": deepcopy(item.selected),
                    "answered_by": item.answered_by,
                    "answered_at": item.answered_at,
                }
                for item in qa_items
            ],
            "card_spec_id": card.spec_id if card is not None else None,
            "foreign_card_spec_id": (
                foreign_card.spec_id if foreign_card is not None else None
            ),
            "history_count": history_count,
            "activity_count": activity_count,
            "foreign_activity_count": foreign_activity_count,
        }
        if spec is None:
            return state
        state.update(
            {
                "title": spec.title,
                "description": spec.description,
                "context": spec.context,
                "edition": spec.edition,
                "version": spec.version,
                "updated_at": spec.updated_at,
                "functional_requirements": deepcopy(spec.functional_requirements),
                "technical_requirements": deepcopy(spec.technical_requirements),
                "acceptance_criteria": deepcopy(spec.acceptance_criteria),
                "business_rules": deepcopy(spec.business_rules),
                "api_contracts": deepcopy(spec.api_contracts),
                "decisions": deepcopy(spec.decisions),
                "test_scenarios": deepcopy(spec.test_scenarios),
                "validations": deepcopy(spec.validations),
                "current_validation_id": spec.current_validation_id,
                "evaluations": deepcopy(spec.evaluations),
            }
        )
        return state


@pytest.mark.asyncio
async def test_move_spec_returns_committed_version(_seed) -> None:
    # The production relational adapter advances this fence in before_flush.
    # Core's lightweight test adapter deliberately omits that cross-cutting
    # listener, so install the equivalent behavior for this regression.
    def bump_version_on_flush(session, _flush_context, _instances) -> None:
        for entity in tuple(session.dirty):
            if (
                isinstance(entity, Spec)
                and inspect(entity).attrs.status.history.has_changes()
            ):
                entity.version += 1

    event.listen(Session, "before_flush", bump_version_on_flush)
    try:
        payload = await _call(
            "okto_pulse_move_spec",
            board_id=BOARD_ID,
            spec_id=_seed,
            status="review",
        )
    finally:
        event.remove(Session, "before_flush", bump_version_on_flush)

    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        persisted = await db.get(Spec, _seed)

    assert payload["success"] is True
    assert payload["from_status"] == "draft"
    assert payload["to_status"] == persisted.status.value == "review"
    assert payload["version"] == persisted.version == 2


@pytest.mark.asyncio
async def test_add_business_rule_roundtrip(_seed):
    out = await _call(
        "okto_pulse_add_business_rule",
        board_id=BOARD_ID,
        spec_id=_seed,
        title="Clamp",
        rule="MUST clamp",
        when="score > 1.5",
        then="clamp to 1.5",
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
        board_id=BOARD_ID,
        spec_id=_seed,
        method="get",
        path="/login",
    )
    assert out["success"] is True
    assert out["api_contract"]["method"] == "GET"


@pytest.mark.asyncio
async def test_get_spec_exposes_structured_families_and_archive_state(_seed):
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        spec = await db.get(Spec, _seed)
        spec.business_rules = [
            {
                "id": "br_get_spec",
                "title": "Expose structured state",
                "rule": "MUST preserve structured families",
                "linked_task_ids": [],
            }
        ]
        spec.test_scenarios = [
            {
                "id": "ts_get_spec",
                "title": "Read projection",
                "status": "pending",
                "acceptance_criteria_ids": [],
            }
        ]
        spec.archived = True
        spec.pre_archive_status = "draft"
        await db.commit()

    payload = await _call(
        "okto_pulse_get_spec",
        board_id=BOARD_ID,
        spec_id=_seed,
    )

    assert payload["business_rules"][0]["id"] == "br_get_spec"
    assert payload["api_contracts"][0]["id"] == CONTRACT_ID
    assert payload["decisions"][0]["id"] == DECISION_ID
    assert payload["test_scenarios"][0]["id"] == "ts_get_spec"
    assert payload["integration_requirements"][0]["id"] == (INTEGRATION_REQUIREMENT_ID)
    assert payload["observability_requirements"][0]["id"] == (
        OBSERVABILITY_REQUIREMENT_ID
    )
    assert payload["archived"] is True
    assert payload["pre_archive_status"] == "draft"
    assert payload["edition"] == 4
    assert payload["version"] == 1


@pytest.mark.asyncio
async def test_add_api_contract_invalid_method_is_canonical(_seed):
    out = await _call(
        "okto_pulse_add_api_contract",
        board_id=BOARD_ID,
        spec_id=_seed,
        method="CALL",
        path="/x",
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
        board_id=OTHER_BOARD_ID,
        spec_id=_seed,
    )
    assert cross == {"error": "Spec not found"}


@pytest.mark.asyncio
async def test_business_rule_read_is_board_scoped(_seed):
    cross = await _call(
        "okto_pulse_list_business_rules", board_id=OTHER_BOARD_ID, spec_id=_seed
    )
    assert cross == {"error": "Spec not found"}


_CROSS_BOARD_JSON_SPEC_CASES = (
    ("okto_pulse_update_spec", {"title": "cross-board write"}),
    (
        "okto_pulse_add_business_rule",
        {"title": "BR", "rule": "MUST", "when": "x", "then": "y"},
    ),
    ("okto_pulse_update_business_rule", {"rule_id": "br_missing", "title": "x"}),
    ("okto_pulse_remove_business_rule", {"rule_id": "br_missing"}),
    ("okto_pulse_list_business_rules", {}),
    ("okto_pulse_add_api_contract", {"method": "GET", "path": "/cross"}),
    (
        "okto_pulse_update_api_contract",
        {"contract_id": "api_missing", "path": "/cross"},
    ),
    ("okto_pulse_remove_api_contract", {"contract_id": "api_missing"}),
    ("okto_pulse_list_api_contracts", {}),
    (
        "okto_pulse_add_decision",
        {"title": "Decision", "rationale": "cross-board"},
    ),
    (
        "okto_pulse_update_decision",
        {"decision_id": "dec_missing", "title": "cross-board"},
    ),
    ("okto_pulse_remove_decision", {"decision_id": "dec_missing"}),
    ("okto_pulse_migrate_spec_decisions", {}),
    (
        "okto_pulse_add_test_scenario",
        {"title": "Scenario", "given": "g", "when": "w", "then": "t"},
    ),
    ("okto_pulse_list_test_scenarios", {}),
    (
        "okto_pulse_update_test_scenario",
        {"scenario_id": "ts_missing", "title": "cross-board"},
    ),
    ("okto_pulse_delete_test_scenario", {"scenario_id": "ts_missing"}),
    (
        "okto_pulse_update_test_scenario_status",
        {"scenario_id": "ts_missing", "status": "ready"},
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "tool_args"),
    _CROSS_BOARD_JSON_SPEC_CASES,
    ids=[case[0].removeprefix("okto_pulse_") for case in _CROSS_BOARD_JSON_SPEC_CASES],
)
async def test_json_spec_operations_fail_closed_cross_board_without_audit(
    _seed, tool: str, tool_args: dict
) -> None:
    before = await _spec_mutation_state(_seed)

    result = await _call(
        tool,
        board_id=OTHER_BOARD_ID,
        spec_id=_seed,
        **tool_args,
    )

    assert "error" in result, result
    assert result["error"] in {"Spec not found", "scenario_not_found"}, result
    assert await _spec_mutation_state(_seed) == before


_CROSS_BOARD_SPEC_PARENT_CASES = (
    ("okto_pulse_get_spec", {}, "Spec not found"),
    ("okto_pulse_delete_spec", {}, "Spec not found"),
    ("okto_pulse_get_spec_history", {}, "Spec not found"),
    ("okto_pulse_list_spec_validations", {}, "Spec not found"),
    (
        "okto_pulse_submit_spec_validation",
        {
            "expected_validation_edition": 1,
            "expected_spec_version": 1,
            "expected_head_revision": 0,
            "confidence": 90,
            "confidence_justification": "Evaluator inspected the complete Spec",
            "clarity": 90,
            "clarity_justification": "Problem and solution are explicit",
            "assertiveness": 85,
            "assertiveness_justification": "Requirements use measurable language",
            "decidability": 90,
            "decidability_justification": "Requirements direct concrete choices",
            "ambiguity": 15,
            "ambiguity_justification": "Terms are explicitly and clearly defined",
            "recommendation": "approve",
        },
        "Spec not found",
    ),
    ("okto_pulse_list_spec_evaluations", {}, "Spec not found"),
    (
        "okto_pulse_get_spec_evaluation",
        {"evaluation_id": EVALUATION_ID},
        "Spec not found",
    ),
    (
        "okto_pulse_delete_spec_evaluation",
        {"evaluation_id": EVALUATION_ID},
        "Spec not found",
    ),
    (
        "okto_pulse_submit_spec_evaluation",
        {
            "breakdown_completeness": 80,
            "breakdown_justification": "complete",
            "granularity": 80,
            "granularity_justification": "granular",
            "dependency_coherence": 80,
            "dependency_justification": "coherent",
            "test_coverage_quality": 80,
            "test_coverage_justification": "covered",
            "overall_score": 80,
            "overall_justification": "good",
            "recommendation": "approve",
        },
        "Spec not found",
    ),
    (
        "okto_pulse_get_spec_knowledge",
        {"knowledge_id": "__seed_knowledge__"},
        "Knowledge base item not found",
    ),
    (
        "okto_pulse_add_spec_knowledge",
        {"title": "cross-board", "content": "must not persist"},
        "Failed to create knowledge base item — spec not found",
    ),
    (
        "okto_pulse_delete_spec_knowledge",
        {"knowledge_id": "__seed_knowledge__"},
        "Knowledge base item not found",
    ),
    (
        "okto_pulse_ask_spec_choice_question",
        {"question": "Choose safely", "options": ["A", "B"]},
        "Spec not found",
    ),
    (
        "okto_pulse_answer_spec_question",
        {"qa_id": "__seed_qa__", "answer": "must not persist"},
        "qa_not_found",
    ),
    (
        "okto_pulse_delete_spec_question",
        {"qa_id": "__seed_qa__"},
        "Q&A item not found",
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "tool_args", "expected_error"),
    _CROSS_BOARD_SPEC_PARENT_CASES,
    ids=[
        case[0].removeprefix("okto_pulse_") for case in _CROSS_BOARD_SPEC_PARENT_CASES
    ],
)
async def test_spec_parent_operations_fail_closed_cross_board_without_audit(
    _seed, tool: str, tool_args: dict, expected_error: str
) -> None:
    before = await _spec_mutation_state(_seed)
    resolved_args = {
        key: (
            f"{KNOWLEDGE_ID_PREFIX}-{_seed}"
            if value == "__seed_knowledge__"
            else _seed_qa_id(_seed)
            if value == "__seed_qa__"
            else value
        )
        for key, value in tool_args.items()
    }
    resolved_expected_error = expected_error.replace("__seed_spec__", _seed)

    result = await _call(
        tool,
        board_id=OTHER_BOARD_ID,
        spec_id=_seed,
        **resolved_args,
    )

    if expected_error == "qa_not_found":
        assert result == {
            "error": "qa_not_found",
            "code": "qa_not_found",
            "message": "Q&A item not found",
            "mutation_applied": False,
        }
    else:
        assert result == {"error": resolved_expected_error}
    assert await _spec_mutation_state(_seed) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_type", "target_id", "expected_error"),
    (
        (
            "spec",
            "__seed_spec__",
            "Spec or card not found, or they belong to different boards",
        ),
        ("contract", CONTRACT_ID, "Spec not found"),
        ("tr", TECHNICAL_REQUIREMENT_ID, "Spec not found"),
        ("decision", DECISION_ID, "Spec not found"),
        ("ir", INTEGRATION_REQUIREMENT_ID, "Spec not found"),
        ("or", OBSERVABILITY_REQUIREMENT_ID, "Spec not found"),
    ),
)
async def test_link_task_parents_fail_closed_cross_board_without_audit(
    _seed, target_type: str, target_id: str, expected_error: str
) -> None:
    before = await _spec_mutation_state(_seed)
    resolved_target_id = _seed if target_id == "__seed_spec__" else target_id
    args = {
        "board_id": OTHER_BOARD_ID,
        "target_type": target_type,
        "target_id": resolved_target_id,
        "card_id": _seed_card_id(_seed),
    }
    if target_type != "spec":
        args["spec_id"] = _seed

    result = await _call("okto_pulse_link_task", **args)

    assert result == {"error": expected_error}
    assert await _spec_mutation_state(_seed) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_type", "target_id", "expected_error"),
    (
        (
            "spec",
            "__seed_spec__",
            "Spec or card not found, or they belong to different boards",
        ),
        ("contract", CONTRACT_ID, "Card not found"),
        ("tr", TECHNICAL_REQUIREMENT_ID, "Card not found"),
        ("decision", DECISION_ID, "Card not found"),
        ("ir", INTEGRATION_REQUIREMENT_ID, "Card not found"),
        ("or", OBSERVABILITY_REQUIREMENT_ID, "Card not found"),
    ),
)
async def test_link_task_foreign_card_fails_closed_without_audit(
    _seed, target_type: str, target_id: str, expected_error: str
) -> None:
    before = await _spec_mutation_state(_seed)
    resolved_target_id = _seed if target_id == "__seed_spec__" else target_id
    args = {
        "board_id": BOARD_ID,
        "target_type": target_type,
        "target_id": resolved_target_id,
        "card_id": _seed_foreign_card_id(_seed),
    }
    if target_type != "spec":
        args["spec_id"] = _seed

    result = await _call("okto_pulse_link_task", **args)

    assert result == {"error": expected_error}
    assert await _spec_mutation_state(_seed) == before


@pytest.mark.asyncio
async def test_link_task_parents_same_board_roundtrip(_seed) -> None:
    from okto_pulse.core.infra.database import get_session_factory

    async with get_session_factory()() as db:
        spec = await db.get(Spec, _seed)
        # The operational card-to-Spec association retains its existing
        # Approved support; structured requirement links below are Spec content
        # and are switched to Draft before they are mutated.
        spec.status = SpecStatus.APPROVED
        await db.commit()

    card_id = _seed_card_id(_seed)
    linked_spec = await _call(
        "okto_pulse_link_task",
        board_id=BOARD_ID,
        target_type="spec",
        target_id=_seed,
        card_id=card_id,
    )
    assert linked_spec["success"] is True

    async with get_session_factory()() as db:
        spec = await db.get(Spec, _seed)
        spec.status = SpecStatus.DRAFT
        await db.commit()

    for target_type, target_id, result_key in (
        ("contract", CONTRACT_ID, "contract_id"),
        ("tr", TECHNICAL_REQUIREMENT_ID, "tr_id"),
        ("decision", DECISION_ID, "decision_id"),
        ("ir", INTEGRATION_REQUIREMENT_ID, "requirement_id"),
        ("or", OBSERVABILITY_REQUIREMENT_ID, "requirement_id"),
    ):
        linked = await _call(
            "okto_pulse_link_task",
            board_id=BOARD_ID,
            target_type=target_type,
            target_id=target_id,
            card_id=card_id,
            spec_id=_seed,
        )
        assert linked.get("success") is True, linked
        assert linked[result_key] == target_id


@pytest.mark.asyncio
async def test_spec_qa_same_board_choice_answer_delete(_seed) -> None:
    answered = await _call(
        "okto_pulse_answer_spec_question",
        board_id=BOARD_ID,
        spec_id=_seed,
        qa_id=_seed_qa_id(_seed),
        answer="Scoped answer",
    )
    assert answered["success"] is True, answered
    assert answered["qa"]["answer"] == "Scoped answer"

    choice = await _call(
        "okto_pulse_ask_spec_choice_question",
        board_id=BOARD_ID,
        spec_id=_seed,
        question="Choose one",
        options=["A", "B"],
    )
    assert choice["success"] is True, choice
    deleted = await _call(
        "okto_pulse_delete_spec_question",
        board_id=BOARD_ID,
        spec_id=_seed,
        qa_id=choice["qa"]["id"],
    )
    assert deleted == {"success": True}


@pytest.mark.asyncio
async def test_spec_qa_child_must_belong_to_requested_spec_without_audit(_seed) -> None:
    from okto_pulse.core.infra.database import get_session_factory

    other_spec_id = f"spec-qa-parent-{_seed[:8]}"
    other_qa_id = f"qa-other-parent-{_seed[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Spec(
                id=other_spec_id,
                board_id=BOARD_ID,
                title="Other Q&A parent",
                status=SpecStatus.DRAFT,
                created_by=USER_ID,
                functional_requirements=[],
                acceptance_criteria=[],
            )
        )
        db.add(
            SpecQAItem(
                id=other_qa_id,
                spec_id=other_spec_id,
                question="Belongs to another spec",
                question_type="text",
                asked_by="different-asker",
            )
        )
        await db.commit()

    async def _other_qa_state() -> tuple:
        async with get_session_factory()() as db:
            qa = await db.get(SpecQAItem, other_qa_id)
            activity_count = await db.scalar(
                select(func.count())
                .select_from(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
            )
            return (
                qa is not None,
                qa.answer if qa else None,
                qa.answered_by if qa else None,
                qa.answered_at if qa else None,
                activity_count,
            )

    before = await _other_qa_state()
    answered = await _call(
        "okto_pulse_answer_spec_question",
        board_id=BOARD_ID,
        spec_id=_seed,
        qa_id=other_qa_id,
        answer="must not persist",
    )
    deleted = await _call(
        "okto_pulse_delete_spec_question",
        board_id=BOARD_ID,
        spec_id=_seed,
        qa_id=other_qa_id,
    )

    assert answered == {
        "error": "qa_not_found",
        "code": "qa_not_found",
        "message": "Q&A item not found",
        "mutation_applied": False,
    }
    assert deleted == {"error": "Q&A item not found"}
    assert await _other_qa_state() == before


@pytest.mark.asyncio
async def test_spec_knowledge_same_board_lifecycle(_seed) -> None:
    seeded_id = f"{KNOWLEDGE_ID_PREFIX}-{_seed}"
    got = await _call(
        "okto_pulse_get_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        knowledge_id=seeded_id,
    )
    assert got["id"] == seeded_id
    assert got["content"] == "private board content"

    added = await _call(
        "okto_pulse_add_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        title="same-board KB",
        content="same-board content",
        governance_metadata=valid_governance_metadata(),
    )
    assert added["success"] is True
    assert added["knowledge"]["governance"]["metadata_status"] == "complete"
    added_id = added["knowledge"]["id"]

    governed = await _call(
        "okto_pulse_get_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        knowledge_id=added_id,
    )
    assert governed["governance"] == added["knowledge"]["governance"]

    rejected = await _call(
        "okto_pulse_add_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        title="Invalid",
        content="body",
        governance_metadata={},
    )
    assert rejected["code"] == "knowledge_governance_invalid_metadata"

    from okto_pulse.core.infra.database import get_session_factory

    async def kb_count() -> int:
        async with get_session_factory()() as db:
            return int(
                await db.scalar(
                    select(func.count())
                    .select_from(SpecKnowledgeBase)
                    .where(SpecKnowledgeBase.spec_id == _seed)
                )
                or 0
            )

    before_blank = await kb_count()
    blank = await _call(
        "okto_pulse_add_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        title="Blank metadata",
        content="body",
        governance_metadata="",
    )
    assert blank["code"] == "knowledge_governance_invalid_metadata"
    assert blank["issues"][0]["code"] == "invalid_json"
    assert await kb_count() == before_blank

    deleted = await _call(
        "okto_pulse_delete_spec_knowledge",
        board_id=BOARD_ID,
        spec_id=_seed,
        knowledge_id=added_id,
    )
    assert deleted == {"success": True}


@pytest.mark.asyncio
async def test_spec_evaluation_same_board_read_and_delete(_seed) -> None:
    listed = await _call(
        "okto_pulse_list_spec_evaluations",
        board_id=BOARD_ID,
        spec_id=_seed,
    )
    assert EVALUATION_ID in {item["id"] for item in listed["evaluations"]}

    got = await _call(
        "okto_pulse_get_spec_evaluation",
        board_id=BOARD_ID,
        spec_id=_seed,
        evaluation_id=EVALUATION_ID,
    )
    assert got["evaluation"]["id"] == EVALUATION_ID

    deleted = await _call(
        "okto_pulse_delete_spec_evaluation",
        board_id=BOARD_ID,
        spec_id=_seed,
        evaluation_id=EVALUATION_ID,
    )
    assert deleted == {
        "success": True,
        "deleted_evaluation_id": EVALUATION_ID,
    }


@pytest.mark.asyncio
async def test_decision_remove_is_soft_delete(_seed):
    added = await _call(
        "okto_pulse_add_decision",
        board_id=BOARD_ID,
        spec_id=_seed,
        title="Pick LadybugDB",
        rationale="embedded + single-writer",
    )
    dec_id = added["decision"]["id"]
    removed = await _call(
        "okto_pulse_remove_spec_entity",
        board_id=BOARD_ID,
        spec_id=_seed,
        target_type="decision",
        entity_id=dec_id,
    )
    assert removed["success"] is True and removed["revoked"] == dec_id
    assert removed["decision"]["status"] == "revoked"


@pytest.mark.asyncio
async def test_get_spec_history_accepts_explicit_limit(_seed):
    out = await _call(
        "okto_pulse_get_spec_history",
        board_id=BOARD_ID,
        spec_id=_seed,
        limit="7",
    )
    assert out["spec_id"] == _seed
    assert out["count"] <= 7
    assert isinstance(out["history"], list)


@pytest.mark.asyncio
async def test_migrate_decisions_preserves_prose_after_contiguous_bullets(_seed):
    updated = await _call(
        "okto_pulse_update_spec",
        board_id=BOARD_ID,
        spec_id=_seed,
        context=(
            "Intro.\r\n\r\n## decisions\r\n"
            "- Keep writes local\r\n"
            "* Require idempotency\r\n\r\n"
            "Trailing prose must survive."
        ),
    )
    assert updated["success"] is True
    assert updated["spec"]["edition"] == 4
    assert updated["spec"]["version"] == 2

    migrated = await _call(
        "okto_pulse_migrate_spec_decisions", board_id=BOARD_ID, spec_id=_seed
    )
    assert migrated["decisions_added"] == 2
    assert migrated["context_modified"] is True

    spec = await _call("okto_pulse_get_spec", board_id=BOARD_ID, spec_id=_seed)
    assert "## decisions" not in spec["context"].lower()
    assert "Trailing prose must survive." in spec["context"]

    second = await _call(
        "okto_pulse_migrate_spec_decisions", board_id=BOARD_ID, spec_id=_seed
    )
    assert second["decisions_added"] == 0
    assert second["context_modified"] is False
