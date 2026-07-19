"""R3-IMP2 (card 67eb2096) — copy tools fall back to the EFFECTIVE inherited
resource when a manual/legacy spec has no direct resource, with an identity the
Resource Gate reads; a provided-but-unresolvable resource yields a structured
actionable error (never a generic "no resources to copy").

Anti-test-theater: the copy is the REAL MCP tool over a REAL spec→refinement
lineage; the end-to-end teeth is that the Resource Gate's spec→task coverage flips
to satisfied after the fallback copy (proving the copied identity is the one the
gate matches), and that WITHOUT a resource the tool returns an honest empty (not a
generic error) while a provided-but-unresolvable obligation returns the structured
error.
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    Refinement,
    RefinementKnowledgeBase,
    ResourceNotApplicable,
    Spec,
)
from okto_pulse.core.mcp.server import _effective_empty_copy_response
from okto_pulse.core.services.effective_resource_propagation import _dedupe_effective_refs
from okto_pulse.core.services.resource_gate import ResourceGateService

USER_ID = "user-r3-imp2"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r3 tester"
        self.permissions = object()


def test_architecture_fallback_refs_dedupe_by_source_design_identity():
    refs = _dedupe_effective_refs(
        "architecture",
        [
            {
                "id": "arch-refinement-snapshot",
                "source_design_id": "arch-ideation-root",
                "source_entity_type": "refinement",
                "source_entity_id": "ref-1",
            },
            {
                "id": "arch-ideation-root",
                "source_entity_type": "ideation",
                "source_entity_id": "idea-1",
            },
        ],
    )

    assert [item["id"] for item in refs] == ["arch-refinement-snapshot"]


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_architecture_copy_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _legacy_spec_inheriting(db_factory, *, with_kb=False, with_mockup=False,
                                  with_architecture=False,
                                  with_ideation_architecture=False,
                                  with_kb_na_mark=False,
                                  with_mockup_na_mark=False,
                                  with_architecture_na_mark=False):
    """A manual/legacy spec (NO direct resources) linked to a refinement that
    DOES carry the requested resource(s) — the effective inherited case."""
    board_id = _id("board")
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    spec_id = _id("spec")
    card_id = _id("card")
    ref_kb_id = _id("refkb")
    ref_mockup_id = _id("refmock")
    ref_design_id = _id("refdesign")
    ideation_design_id = _id("ideadesign")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r3 imp2", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="idea", created_by=USER_ID))
        if with_ideation_architecture:
            db.add(ArchitectureDesign(
                id=ideation_design_id, board_id=board_id, parent_type="ideation",
                ideation_id=ideation_id, title="Shared architecture",
                global_description="ideation architecture", entities=[],
                interfaces=[], diagrams=[], created_by=USER_ID,
            ))
        db.add(Refinement(
            id=refinement_id, board_id=board_id, ideation_id=ideation_id,
            title="refinement", created_by=USER_ID,
            screen_mockups=(
                [{"id": ref_mockup_id, "title": "Ref mockup", "screen_type": "form",
                  "html_content": "<div/>"}] if with_mockup else []
            ),
        ))
        if with_kb:
            db.add(RefinementKnowledgeBase(
                id=ref_kb_id, refinement_id=refinement_id, title="Ref KB",
                description="d", content="ref content", mime_type="text/markdown",
                created_by=USER_ID,
            ))
        for resource_type, marked in (
            ("knowledge_base", with_kb_na_mark),
            ("mockup", with_mockup_na_mark),
            ("architecture", with_architecture_na_mark),
        ):
            if not marked:
                continue
            db.add(ResourceNotApplicable(
                board_id=board_id,
                entity_type="refinement",
                entity_id=refinement_id,
                resource_type=resource_type,
                justification=f"Historical mark superseded by provided {resource_type}",
                source_channel="mcp",
                created_by=USER_ID,
            ))
        if with_architecture:
            db.add(ArchitectureDesign(
                id=ref_design_id, board_id=board_id, parent_type="refinement",
                refinement_id=refinement_id, title="Shared architecture",
                global_description="refinement architecture", entities=[],
                interfaces=[], diagrams=[], created_by=USER_ID,
            ))
        # The spec is linked to the refinement but has NO direct resources.
        db.add(Spec(id=spec_id, board_id=board_id, refinement_id=refinement_id,
                    ideation_id=ideation_id, title="Legacy manual spec",
                    created_by=USER_ID))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="impl card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    return {"board_id": board_id, "refinement_id": refinement_id, "spec_id": spec_id,
            "card_id": card_id, "ref_kb_id": ref_kb_id, "ref_mockup_id": ref_mockup_id,
            "ref_design_id": ref_design_id, "ideation_design_id": ideation_design_id}


# ===========================================================================
# AC: knowledge fallback to effective inherited + gate coverage flips satisfied
# ===========================================================================


@pytest.mark.asyncio
async def test_copy_knowledge_falls_back_to_effective_and_gate_is_covered(db_factory):
    seed = await _legacy_spec_inheriting(db_factory, with_kb=True)

    result = await _call(
        "okto_pulse_copy_knowledge_to_card",
        board_id=seed["board_id"], spec_id=seed["spec_id"], card_id=seed["card_id"],
    )
    assert result.get("success") is True, result
    assert result["fallback"] is True
    assert result["copied"] >= 1

    # The card KB carries the gate identity == the effective refinement kb id.
    async with db_factory() as db:
        card = await db.get(Card, seed["card_id"])
        kbs = list(card.knowledge_bases or [])
    assert any(kb.get("source_kb_id") == seed["ref_kb_id"] for kb in kbs), kbs

    # END-TO-END TEETH: the spec's inherited KB obligation is now covered by the
    # card (the copied identity is exactly the one the gate matches).
    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board_id"], seed["spec_id"],
        )
    kb_uncovered = [
        r for r in coverage["uncovered_resources"]
        if r["resource_type"] == "knowledge_base"
    ]
    assert kb_uncovered == [], coverage["uncovered_resources"]


@pytest.mark.asyncio
async def test_copy_knowledge_ignores_ineffective_inherited_na_mark(db_factory):
    seed = await _legacy_spec_inheriting(
        db_factory,
        with_kb=True,
        with_kb_na_mark=True,
    )

    result = await _call(
        "okto_pulse_copy_knowledge_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
    )

    assert result.get("success") is True, result
    assert result.get("reason") != "not_applicable", result
    assert result["fallback"] is True
    assert result["copied"] >= 1

    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board_id"], seed["spec_id"]
        )
    assert not [
        item
        for item in coverage["uncovered_resources"]
        if item["resource_type"] == "knowledge_base"
    ]


@pytest.mark.asyncio
async def test_all_copy_tools_ignore_ineffective_inherited_na_marks(db_factory):
    seed = await _legacy_spec_inheriting(
        db_factory,
        with_kb=True,
        with_mockup=True,
        with_architecture=True,
        with_kb_na_mark=True,
        with_mockup_na_mark=True,
        with_architecture_na_mark=True,
    )

    knowledge = await _call(
        "okto_pulse_copy_knowledge_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
    )
    mockup = await _call(
        "okto_pulse_copy_mockups_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
    )
    architecture = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
    )

    for result in (knowledge, mockup, architecture):
        assert result.get("success") is True, result
        assert result.get("reason") != "not_applicable", result
        assert int(result.get("total_on_card") or 0) >= 1, result

    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board_id"], seed["spec_id"]
        )
    assert coverage["allowed"] is True, coverage
    assert coverage["uncovered_resources"] == [], coverage


@pytest.mark.asyncio
async def test_copy_mockups_falls_back_to_effective(db_factory):
    seed = await _legacy_spec_inheriting(db_factory, with_mockup=True)

    result = await _call(
        "okto_pulse_copy_mockups_to_card",
        board_id=seed["board_id"], spec_id=seed["spec_id"], card_id=seed["card_id"],
    )
    assert result.get("success") is True, result
    assert result["fallback"] is True and result["copied"] >= 1

    async with db_factory() as db:
        card = await db.get(Card, seed["card_id"])
        mockups = list(card.screen_mockups or [])
    # Mockup keeps its id (the gate identity) == the effective refinement mockup.
    assert any(m.get("id") == seed["ref_mockup_id"] for m in mockups), mockups


@pytest.mark.asyncio
async def test_copy_knowledge_rejects_mixed_valid_and_foreign_ids_atomically(db_factory):
    seed = await _legacy_spec_inheriting(db_factory, with_kb=True)
    foreign_id = _id("foreign-kb")

    result = await _call(
        "okto_pulse_copy_knowledge_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
        knowledge_ids=[seed["ref_kb_id"], foreign_id],
    )

    assert result["error"] == "resource_selection_invalid", result
    assert result["resource_type"] == "knowledge_base"
    assert result["requested"] == [seed["ref_kb_id"], foreign_id]
    assert result["matched"] == [seed["ref_kb_id"]]
    assert result["missing"] == [foreign_id]
    assert result["retryable"] is False

    # Validation happens before any card write: a mixed valid/foreign request
    # cannot leave the valid half copied behind.
    async with db_factory() as db:
        card = await db.get(Card, seed["card_id"])
        assert list(card.knowledge_bases or []) == []


@pytest.mark.asyncio
async def test_copy_mockups_rejects_mixed_valid_and_foreign_ids_atomically(db_factory):
    seed = await _legacy_spec_inheriting(db_factory, with_mockup=True)
    foreign_id = _id("foreign-mockup")

    result = await _call(
        "okto_pulse_copy_mockups_to_card",
        board_id=seed["board_id"],
        spec_id=seed["spec_id"],
        card_id=seed["card_id"],
        screen_ids=[seed["ref_mockup_id"], foreign_id],
    )

    assert result["error"] == "resource_selection_invalid", result
    assert result["resource_type"] == "mockup"
    assert result["requested"] == [seed["ref_mockup_id"], foreign_id]
    assert result["matched"] == [seed["ref_mockup_id"]]
    assert result["missing"] == [foreign_id]
    assert result["retryable"] is False

    async with db_factory() as db:
        card = await db.get(Card, seed["card_id"])
        assert list(card.screen_mockups or []) == []


@pytest.mark.asyncio
async def test_copy_architecture_falls_back_to_effective(db_factory):
    seed = await _legacy_spec_inheriting(db_factory, with_architecture=True)

    result = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board_id"], spec_id=seed["spec_id"], card_id=seed["card_id"],
    )
    assert "error" not in result, result

    async with db_factory() as db:
        designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card",
                ArchitectureDesign.card_id == seed["card_id"],
            )
        )).scalars().all()
    # The card design carries source_design_id == the effective refinement design.
    assert any(getattr(d, "source_design_id", None) == seed["ref_design_id"]
               for d in designs), [getattr(d, "source_design_id", None) for d in designs]


@pytest.mark.asyncio
async def test_copy_architecture_covers_multiple_inherited_effective_designs(db_factory):
    seed = await _legacy_spec_inheriting(
        db_factory,
        with_architecture=True,
        with_ideation_architecture=True,
    )

    result = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board_id"], spec_id=seed["spec_id"], card_id=seed["card_id"],
        profile="full",
    )
    assert result.get("success") is True, result
    copied_source_ids = {
        item.get("source_design_id") for item in result["architecture_designs"]
    }
    assert copied_source_ids >= {
        seed["ref_design_id"],
        seed["ideation_design_id"],
    }

    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board_id"], seed["spec_id"],
        )

    arch_uncovered = [
        item for item in coverage["uncovered_resources"]
        if item["resource_type"] == "architecture"
    ]
    assert arch_uncovered == [], coverage["uncovered_resources"]


# ===========================================================================
# Point 6 — honest empty vs structured error (never generic "no resources")
# ===========================================================================


@pytest.mark.asyncio
async def test_copy_knowledge_no_resource_required_is_clean_empty(db_factory):
    # Neither the spec nor the refinement has any KB -> no obligation.
    seed = await _legacy_spec_inheriting(db_factory)  # no resources anywhere

    result = await _call(
        "okto_pulse_copy_knowledge_to_card",
        board_id=seed["board_id"], spec_id=seed["spec_id"], card_id=seed["card_id"],
    )
    # Honest empty — NOT a generic "No knowledge bases to copy" error.
    assert result.get("success") is True, result
    assert result.get("copied") == 0
    assert result.get("reason") == "no_resource_required"
    assert "error" not in result


def test_effective_empty_copy_response_branches():
    # N/A -> success not_applicable (no error).
    na = json.loads(_effective_empty_copy_response(
        "knowledge_base", {"not_applicable": True, "has_obligation": True}))
    assert na["success"] is True and na["reason"] == "not_applicable"

    # No obligation -> honest empty success.
    none = json.loads(_effective_empty_copy_response(
        "mockup", {"not_applicable": False, "has_obligation": False}))
    assert none["success"] is True and none["reason"] == "no_resource_required"

    # Provided obligation but unresolvable -> structured actionable error.
    err = json.loads(_effective_empty_copy_response(
        "architecture",
        {"not_applicable": False, "has_obligation": True,
         "coverage_obligation_id": "architecture:design-x",
         "accepted_identity_fields": ["source_design_id", "source_ref", "id"]},
    ))
    assert err["error"] == "resource_propagation_failed"
    assert err["resource_type"] == "architecture"
    assert err["coverage_obligation_id"] == "architecture:design-x"
    assert "source_design_id" in err["accepted_identity_fields"]
    assert err["retryable"] is True
