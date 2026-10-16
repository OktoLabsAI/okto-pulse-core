"""R4 — MCP tool-family consolidation (ToolFamilyRegistry + the two
assertiveness-gate-eligible families: spec_entity_remove and qa_ask).

Covers spec ``MCP Tool Family Consolidation and Alias Compatibility``:
- TC-R4.1: current registry families and per-target behavior.
- TC-R4.2 superseded: removed aliases cannot dispatch.
- TC-R4.3: unsupported target_type structured errors + safe alias telemetry.

Owner gate honored: ONLY the two homogeneous-signature families are consolidated;
the six heterogeneous families are kept separate with a rejected_reason.
"""

from __future__ import annotations

from okto_pulse.core.domain.permissions import PermissionSet

from mcp_runtime_testing import register_mcp_test_runtime

import json
import logging
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select

from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.tool_family_registry import (
    REGISTRY,
    ConsolidationMode,
    ToolFamilyRegistry,
    emit_alias_usage,
)
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationQAItem,
    IdeationStatus,
    Refinement,
    RefinementQAItem,
    RefinementStatus,
    Spec,
    SpecQAItem,
    SpecStatus,
)

USER_ID = "r4-agent"


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4()}"


def _stub_ctx(board_id: str, permissions=None):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": permissions if permissions is not None else PermissionSet({
                "board": {"read": True}, "card": {"qa": {"ask": True}},
                "ideation": {"qa": {"ask": True}}, "refinement": {"qa": {"ask": True}},
                "spec": {"qa": {"ask": True}},
            }),
        },
    )()


async def _call(name: str, **kwargs) -> dict:
    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool(name)
    return json.loads(await tool.fn(**kwargs))


# ===========================================================================
# Registry — eligible / excluded families with reasons (TC-R4.1, ac_e4d9ba74)
# ===========================================================================


def test_registry_lists_exactly_the_two_eligible_families():
    eligible = [f.family_id for f in REGISTRY.eligible()]
    assert eligible == ["spec_entity_remove", "qa_ask"]
    for f in REGISTRY.eligible():
        assert f.mode is ConsolidationMode.DEDICATED_ROUTING  # never a flat payload
        assert f.consolidated_tool and f.consolidated_tool.startswith("okto_pulse_")
        assert not f.dedicated_tools and f.routing_notes


def test_registry_excludes_six_families_each_with_a_reason():
    excluded = {f.family_id: f for f in REGISTRY.excluded()}
    assert set(excluded) == {
        "spec_entity_add", "test_scenario", "qa_answer",
        "move_workitem", "kg_query", "move_card",
    }
    for f in excluded.values():
        assert f.eligible is False
        assert f.consolidated_tool is None
        assert f.rejected_reason and len(f.rejected_reason) > 40  # a real reason


def test_registry_marks_kg_query_and_move_card_excluded_no_replacement(ac_599ad903=None):
    # ac_599ad903: the two spec non-goals are excluded with a documented reason and
    # no consolidated replacement is registered.
    for fid in ("kg_query", "move_card"):
        fam = REGISTRY.get(fid)
        assert fam is not None and not fam.eligible
        assert fam.consolidated_tool is None
        assert fam.rejected_reason


def test_validate_target_type_structured_and_excluded(ac_c0c8f0f3=None):
    assert REGISTRY.validate_target_type("spec_entity_remove", "business_rule") is None
    assert REGISTRY.validate_target_type("spec_entity_remove", "decision") is None
    bad = REGISTRY.validate_target_type("spec_entity_remove", "functional_requirement")
    assert bad and "Allowed" in bad and "business_rule" in bad
    # excluded family never validates a target_type for consolidation
    excl = REGISTRY.validate_target_type("spec_entity_add", "business_rule")
    assert excl and "not consolidated" in excl


def test_registry_resolves_only_current_tools():
    reg = ToolFamilyRegistry()
    assert reg.resolve_tool("okto_pulse_remove_business_rule") is None
    assert reg.resolve_tool("ask") is None
    family, kind = reg.resolve_tool("okto_pulse_remove_spec_entity")
    assert family.family_id == "spec_entity_remove"
    assert kind.value == "consolidated"
    family, kind = reg.resolve_tool("okto_pulse_add_business_rule")
    assert kind.value == "dedicated"


@pytest.mark.asyncio
async def test_removed_family_aliases_have_no_handler_or_policy(monkeypatch):
    from okto_pulse.core.domain.mcp_permission_registry import MCP_TOOL_PERMISSION_POLICIES
    def forbidden(*args, **kwargs):
        pytest.fail("removed alias reached runtime")
    monkeypatch.setattr(mcp_server, "_get_agent_ctx", forbidden)
    for name in ('okto_pulse_ask_question', 'okto_pulse_ask_ideation_question', 'okto_pulse_ask_refinement_question', 'okto_pulse_ask_spec_question', 'okto_pulse_remove_business_rule', 'okto_pulse_remove_api_contract', 'okto_pulse_remove_decision'):
        assert not hasattr(mcp_server, name)
        with pytest.raises(KeyError):
            await mcp_server.mcp.get_tool(name)
        assert name not in {policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES}
        assert REGISTRY.resolve_tool(name) is None


# ===========================================================================
# Telemetry — safe labels only, fail-closed (TC-R4.3, ac_54ef117f / or_4e57890f)
# ===========================================================================


def test_alias_usage_telemetry_records_safe_labels(caplog):
    with caplog.at_level(logging.INFO, logger="okto_pulse.mcp.tool_family"):
        labels = emit_alias_usage(
            family_id="spec_entity_remove", alias_kind="consolidated",
            tool_name="okto_pulse_remove_spec_entity", operation="remove",
            target_type="decision", outcome="ok",
        )
    assert labels  # accepted
    assert set(labels) == {
        "family_id", "alias_kind", "tool_name", "operation", "target_type", "outcome",
    }
    rec = next(r for r in caplog.records if r.getMessage() == "mcp_tool_alias_usage_total")
    assert rec.alias_usage["outcome"] == "ok"


def test_alias_usage_telemetry_fails_closed_on_unsafe_value():
    # A body-length value (e.g. the question text) is rejected fail-closed.
    rejected = emit_alias_usage(
        family_id="qa_ask", alias_kind="consolidated", tool_name="okto_pulse_ask",
        operation="ask", target_type="x" * 200, outcome="ok",
    )
    assert rejected == {}


# ===========================================================================
# Parity — consolidated vs legacy remove (TC-R4.1, ac_cea034cf / ac_e0476d43)
# ===========================================================================


async def _seed_spec(db_factory, *, board_id, spec_id, **spec_kwargs):
    async with db_factory() as db:
        db.add(Board(id=board_id, name="R4", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="Spec",
            status=SpecStatus.DRAFT, created_by=USER_ID, **spec_kwargs,
        ))
        await db.commit()


@pytest.mark.asyncio
async def test_canonical_removal_preserves_hard_and_soft_delete():
    db_factory = get_session_factory()
    board_id, spec_id = _id("r4-board"), _id("r4-spec")
    await _seed_spec(
        db_factory, board_id=board_id, spec_id=spec_id,
        business_rules=[
            {"id": "br_legacy", "title": "L", "rule": "r", "when": "w", "then": "t", "status": "active"},
            {"id": "br_consol", "title": "C", "rule": "r", "when": "w", "then": "t", "status": "active"},
        ],
        decisions=[
            {"id": "dec_legacy", "title": "L", "rationale": "x", "status": "active"},
            {"id": "dec_consol", "title": "C", "rationale": "x", "status": "active"},
        ],
    )

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        consol_br = await _call("okto_pulse_remove_spec_entity", board_id=board_id, spec_id=spec_id, target_type="business_rule", entity_id="br_consol")
        consol_dec = await _call("okto_pulse_remove_spec_entity", board_id=board_id, spec_id=spec_id, target_type="decision", entity_id="dec_consol")

    # business_rule hard-remove parity: identical key shape.
    assert consol_br["success"] is True and consol_br["removed"] == "br_consol"

    # decision SOFT-delete parity: both revoke (status=revoked), same shape.
    assert consol_dec["revoked"] == "dec_consol" and consol_dec["decision"]["status"] == "revoked"


@pytest.mark.asyncio
async def test_remove_spec_entity_unsupported_target_type_no_mutation():
    db_factory = get_session_factory()
    board_id, spec_id = _id("r4b-board"), _id("r4b-spec")
    await _seed_spec(
        db_factory, board_id=board_id, spec_id=spec_id,
        business_rules=[{"id": "br_x", "title": "X", "rule": "r", "when": "w", "then": "t", "status": "active"}],
    )
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        bad = await _call("okto_pulse_remove_spec_entity", board_id=board_id, spec_id=spec_id, target_type="functional_requirement", entity_id="br_x")
        # no mutation: the business_rule is still removable afterwards
        still = await _call("okto_pulse_remove_spec_entity", board_id=board_id, spec_id=spec_id, target_type="business_rule", entity_id="br_x")

    assert bad["error"] == "unsupported_target_type"
    assert set(bad["allowed"]) == {"business_rule", "api_contract", "decision"}
    assert still["success"] is True and still["removed"] == "br_x"


# ===========================================================================
# Parity — consolidated vs legacy ask (TC-R4.1/R4.2)
# ===========================================================================


@pytest.mark.asyncio
async def test_canonical_ask_creates_scoped_questions_and_rejects_unknown_target():
    db_factory = get_session_factory()
    board_id, spec_id, card_id = _id("r4q-board"), _id("r4q-spec"), _id("r4q-card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="R4Q", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", status=SpecStatus.DRAFT, created_by=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()

    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(board_id))), \
         patch.object(mcp_server, "check_permission", return_value=None):
        consol = await _call("okto_pulse_ask", board_id=board_id, target_type="card", parent_id=card_id, question="Q consolidated")
        spec_consol = await _call("okto_pulse_ask", board_id=board_id, target_type="spec", parent_id=spec_id, question="Q spec")
        bad = await _call("okto_pulse_ask", board_id=board_id, target_type="bogus", parent_id=card_id, question="Q")

    # card ask parity: identical key shape, both create a qa.
    assert consol.get("success") is True, consol
    assert consol["qa"]["question"] == "Q consolidated"
    assert spec_consol["success"] is True
    # unsupported target_type → structured error, no qa.
    assert bad["error"] == "unsupported_target_type"
    assert bad["allowed"] == ["card", "ideation", "refinement", "spec"]


@pytest.mark.asyncio
async def test_ask_uses_target_specific_permission_for_card_and_spec():
    # A Board read permission cannot authorize Q&A writes on either target.
    db_factory = get_session_factory()
    board_id, card_id, spec_id = (
        _id("r4s-board"),
        _id("r4s-card"),
        _id("r4s-spec"),
    )
    async with db_factory() as db:
        db.add(Board(id=board_id, name="R4S", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", status=SpecStatus.IN_PROGRESS, created_by=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="Card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_stub_ctx(board_id, permissions=PermissionSet({"board": {"read": True}, "card": {"qa": {"ask": False}}, "spec": {"qa": {"ask": False}}}))),
    ):
        card_res = await _call("okto_pulse_ask", board_id=board_id, target_type="card", parent_id=card_id, question="Q")
        spec_res = await _call("okto_pulse_ask", board_id=board_id, target_type="spec", parent_id=spec_id, question="Q")

    assert "card.qa.ask" in card_res["error"]
    assert "spec.qa.ask" in spec_res["error"]


@pytest.mark.asyncio
async def test_ask_non_card_parents_are_board_scoped_before_create_or_log():
    db_factory = get_session_factory()
    board_id, foreign_board_id = _id("r4scope-board"), _id("r4scope-foreign")
    async with db_factory() as db:
        db.add_all(
            (
                Board(id=board_id, name="R4 scope", owner_id=USER_ID),
                Board(id=foreign_board_id, name="R4 foreign", owner_id=USER_ID),
            )
        )
        await db.flush()
        local_ideation = Ideation(
            board_id=board_id,
            title="Local ideation",
            status=IdeationStatus.DRAFT,
            created_by=USER_ID,
        )
        foreign_ideation = Ideation(
            board_id=foreign_board_id,
            title="Foreign ideation",
            status=IdeationStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add_all((local_ideation, foreign_ideation))
        await db.flush()
        local_refinement = Refinement(
            board_id=board_id,
            ideation_id=local_ideation.id,
            title="Local refinement",
            status=RefinementStatus.DRAFT,
            created_by=USER_ID,
        )
        foreign_refinement = Refinement(
            board_id=foreign_board_id,
            ideation_id=foreign_ideation.id,
            title="Foreign refinement",
            status=RefinementStatus.DRAFT,
            created_by=USER_ID,
        )
        local_spec = Spec(
            board_id=board_id,
            ideation_id=local_ideation.id,
            title="Local spec",
            status=SpecStatus.DRAFT,
            created_by=USER_ID,
        )
        foreign_spec = Spec(
            board_id=foreign_board_id,
            ideation_id=foreign_ideation.id,
            title="Foreign spec",
            status=SpecStatus.DRAFT,
            created_by=USER_ID,
        )
        db.add_all(
            (
                local_refinement,
                foreign_refinement,
                local_spec,
                foreign_spec,
            )
        )
        await db.commit()
        local_ids = {
            "ideation": local_ideation.id,
            "refinement": local_refinement.id,
            "spec": local_spec.id,
        }
        foreign_ids = {
            "ideation": foreign_ideation.id,
            "refinement": foreign_refinement.id,
            "spec": foreign_spec.id,
        }

    async def _counts() -> tuple[int, int, int, int]:
        async with db_factory() as db:
            return (
                int(await db.scalar(select(func.count()).select_from(IdeationQAItem)) or 0),
                int(await db.scalar(select(func.count()).select_from(RefinementQAItem)) or 0),
                int(await db.scalar(select(func.count()).select_from(SpecQAItem)) or 0),
                int(await db.scalar(select(func.count()).select_from(ActivityLog)) or 0),
            )

    before = await _counts()
    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_stub_ctx(board_id)),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        cross = {
            entity_type: await _call(
                "okto_pulse_ask",
                board_id=board_id,
                target_type=entity_type,
                parent_id=parent_id,
                question="must-not-create",
            )
            for entity_type, parent_id in foreign_ids.items()
        }

    assert cross == {
        "ideation": {"error": "Ideation not found"},
        "refinement": {"error": "Refinement not found"},
        "spec": {"error": "Spec not found"},
    }
    assert await _counts() == before

    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=_stub_ctx(board_id)),
    ), patch.object(mcp_server, "check_permission", return_value=None):
        same_board = {
            entity_type: await _call(
                "okto_pulse_ask",
                board_id=board_id,
                target_type=entity_type,
                parent_id=parent_id,
                question="same-board question",
            )
            for entity_type, parent_id in local_ids.items()
        }

    assert all(result.get("success") is True for result in same_board.values()), same_board
    after = await _counts()
    assert tuple(after[i] - before[i] for i in range(4)) == (1, 1, 1, 3)


# ===========================================================================
# Current tool docs — tools/list points to the canonical lazy documentation
# instead of embedding migration prose (TC-R4.3, ac_ac75da3a / AC6)
# ===========================================================================


@pytest.mark.asyncio
async def test_consolidated_tool_descriptions_point_to_lazy_family_docs():
    # ac_ac75da3a (AC6): the consolidated tools' tools/list descriptions REFERENCE
    # canonical lazy documentation instead of embedding long migration prose. The
    # current routing guidance must remain reachable and
    # stay out of the compact description.
    tools = await mcp_server.mcp.get_tools()
    load = mcp_server._load_resource_file

    cases = [
        (
            "okto_pulse_remove_spec_entity",
            "okto-pulse://reference/tool-docs/spec",
            "reference/tool-docs/spec.md",
            "reference/tool-families/spec_entity_remove.md",
            # Full legacy aliases live in the family resource reached via the
            # compact tool-docs landing page.
            "target_type",
        ),
        (
            "okto_pulse_ask",
            "okto-pulse://reference/tool-docs/qa",
            "reference/tool-docs/qa.md",
            "reference/tool-families/qa_ask.md",
            # Full aliases are doc-only.
            "target_type",
        ),
    ]

    for tool_name, docs_uri, landing_path, detail_path, moved_detail in cases:
        assert tool_name in tools, f"{tool_name} is not registered"
        desc = tools[tool_name].description or ""

        # 1) the compact tools/list description points to the canonical lazy doc...
        assert docs_uri in desc, f"{tool_name} desc does not reference {docs_uri}"
        # 2) ...and stays within the R1 compaction budget (no embedded long prose).
        assert len(desc) <= 900, f"{tool_name} desc {len(desc)} > 900-char budget"

        # 3) the long migration detail is MOVED to the lazy resource, not deleted...
        landing_doc = load(landing_path)
        if detail_path != landing_path:
            detail_uri = "okto-pulse://" + detail_path.removesuffix(".md")
            assert detail_uri in landing_doc
        doc = load(detail_path)
        assert doc and "R4 consolidation" in doc
        for section in ("Consolidated tool", "Telemetry"):
            assert section in doc, f"{section} missing from {detail_path}"
        assert moved_detail in doc, f"{moved_detail} missing from {detail_path}"
        # 4) ...and is NOT re-embedded into the compact tools/list description.
        assert "Legacy aliases" not in desc, (
            f"{tool_name} embeds migration detail '{moved_detail}' "
            f"instead of linking the lazy doc"
        )
