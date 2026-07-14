"""R3-IMP1 (card 2748de1e) — create_spec(refinement_id) propagates EFFECTIVE
resources (direct or inherited) so the Resource Gate sees them as provided.

Anti-test-theater: the spec is created through the REAL MCP tool
``okto_pulse_create_spec`` over a REAL refinement/ideation lineage; the propagated
rows are read back from the DB and matched to the gate identity fields. Failure
modes (lineage resolution failure, propagation failure) are proven to fail
transactionally — NO spec persisted, never a silent half-resourced spec.
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
    Ideation,
    IdeationKnowledgeBase,
    Refinement,
    RefinementKnowledgeBase,
    Spec,
    SpecKnowledgeBase,
)

USER_ID = "user-r3-imp1"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r3 tester"
        self.permissions = object()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _board(db_factory) -> str:
    board_id = _id("board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r3 imp1", owner_id=USER_ID))
        await db.commit()
    return board_id


async def _refinement_with_direct_resources(db_factory, board_id):
    """Refinement carrying a DIRECT KB + mockup + architecture design."""
    refinement_id = _id("ref")
    ideation_id = _id("idea")
    kb_id = _id("refkb")
    mockup_id = _id("refmock")
    design_id = _id("refdesign")
    async with db_factory() as db:
        db.add(Ideation(id=ideation_id, board_id=board_id, title="R3 parent ideation",
                        created_by=USER_ID))
        db.add(Refinement(
            id=refinement_id, board_id=board_id, ideation_id=ideation_id,
            title="R3 refinement", created_by=USER_ID,
            screen_mockups=[{"id": mockup_id, "title": "Ref mockup",
                             "screen_type": "form", "html_content": "<div/>"}],
        ))
        db.add(RefinementKnowledgeBase(
            id=kb_id, refinement_id=refinement_id, title="Ref KB",
            description="d", content="ref content", mime_type="text/markdown",
            created_by=USER_ID,
        ))
        db.add(ArchitectureDesign(
            id=design_id, board_id=board_id, parent_type="refinement",
            refinement_id=refinement_id, title="Ref design",
            global_description="g", entities=[], interfaces=[], diagrams=[],
            created_by=USER_ID,
        ))
        await db.commit()
    return {"refinement_id": refinement_id, "kb_id": kb_id,
            "mockup_id": mockup_id, "design_id": design_id}


async def _spec_rows(db_factory, spec_id):
    async with db_factory() as db:
        kbs = (await db.execute(
            select(SpecKnowledgeBase).where(SpecKnowledgeBase.spec_id == spec_id)
        )).scalars().all()
        designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "spec",
                ArchitectureDesign.spec_id == spec_id,
            )
        )).scalars().all()
        spec = await db.get(Spec, spec_id)
        mockups = list(spec.screen_mockups or []) if spec else []
    return kbs, designs, mockups


# ===========================================================================
# AC1/AC3 — direct refinement resources propagate to the manual spec
# ===========================================================================


@pytest.mark.asyncio
async def test_create_spec_inherits_direct_refinement_resources(db_factory):
    board_id = await _board(db_factory)
    seed = await _refinement_with_direct_resources(db_factory, board_id)

    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Manual spec from refinement",
        refinement_id=seed["refinement_id"],
    )
    assert result.get("success") is True, result
    spec_id = result["spec"]["id"]

    rp = result["resource_propagation"]
    assert rp["status"] == "created_with_resources", rp
    assert rp["by_type"]["knowledge_base"]["status"] == "created_with_resources"
    assert rp["by_type"]["mockup"]["status"] == "created_with_resources"
    assert rp["by_type"]["architecture"]["status"] == "created_with_resources"

    kbs, designs, mockups = await _spec_rows(db_factory, spec_id)
    # KB carries a gate-readable identity == the effective refinement kb id.
    assert any(getattr(kb, "source_kb_id", None) == seed["kb_id"] for kb in kbs), [
        getattr(kb, "source_kb_id", None) for kb in kbs
    ]
    # Mockup carries origin_id == the effective mockup id (the gate identity;
    # propagate_artifacts re-keys id to sm_* and records origin_id).
    assert any(m.get("origin_id") == seed["mockup_id"] for m in mockups), mockups
    # Architecture design carries source_design_id == the refinement design id.
    assert any(getattr(d, "source_design_id", None) == seed["design_id"] for d in designs)


# ===========================================================================
# Effective fallback — refinement empty, ideation has the resource
# ===========================================================================


@pytest.mark.asyncio
async def test_create_spec_effective_fallback_from_ideation(db_factory):
    board_id = await _board(db_factory)
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    ideation_kb_id = _id("ideakb")
    async with db_factory() as db:
        db.add(Ideation(id=ideation_id, board_id=board_id, title="R3 ideation",
                        created_by=USER_ID))
        db.add(IdeationKnowledgeBase(
            id=ideation_kb_id, ideation_id=ideation_id, title="Ideation KB",
            description="d", content="idea content", mime_type="text/markdown",
            created_by=USER_ID,
        ))
        # Refinement linked to the ideation but with NO direct KB of its own.
        db.add(Refinement(id=refinement_id, board_id=board_id, ideation_id=ideation_id,
                          title="Empty refinement", created_by=USER_ID))
        await db.commit()

    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Spec inheriting from ideation",
        refinement_id=refinement_id,
    )
    assert result.get("success") is True, result
    spec_id = result["spec"]["id"]

    rp = result["resource_propagation"]
    # Aggregate is effective_fallback because the KB came from the ancestor.
    assert rp["status"] == "created_with_effective_fallback", rp
    assert rp["by_type"]["knowledge_base"]["status"] == "created_with_effective_fallback"
    assert rp["by_type"]["knowledge_base"]["source_entity_type"] == "ideation"

    kbs, _designs, _mockups = await _spec_rows(db_factory, spec_id)
    # The effective KB id (the ideation kb) is the gate-readable identity.
    assert any(getattr(kb, "source_kb_id", None) == ideation_kb_id for kb in kbs)


@pytest.mark.asyncio
async def test_create_spec_without_resources_reports_without_required(db_factory):
    board_id = await _board(db_factory)
    refinement_id = _id("ref")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(Ideation(id=ideation_id, board_id=board_id, title="Bare ideation",
                        created_by=USER_ID))
        db.add(Refinement(id=refinement_id, board_id=board_id, ideation_id=ideation_id,
                          title="Bare refinement", created_by=USER_ID))
        await db.commit()

    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Spec with no resources",
        refinement_id=refinement_id,
    )
    assert result.get("success") is True, result
    assert result["resource_propagation"]["status"] == "created_without_required_resources"


@pytest.mark.asyncio
async def test_create_spec_without_refinement_has_no_propagation_field(db_factory):
    board_id = await _board(db_factory)
    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Plain manual spec",
    )
    assert result.get("success") is True, result
    assert "resource_propagation" not in result  # unchanged legacy behavior


# ===========================================================================
# Failure modes — fail closed, NO spec persisted (no masking)
# ===========================================================================


@pytest.mark.asyncio
async def test_lineage_resolution_failure_persists_no_spec(db_factory):
    board_id = await _board(db_factory)
    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Spec with bad refinement",
        refinement_id="refinement-does-not-exist",
    )
    assert result.get("error") == "resource_lineage_resolution_failed", result
    # TEETH: the create rolled back — no spec was persisted on the board.
    async with db_factory() as db:
        specs = (await db.execute(
            select(Spec).where(Spec.board_id == board_id)
        )).scalars().all()
    assert specs == []


@pytest.mark.asyncio
async def test_propagation_failure_is_structured_and_rolls_back(db_factory, monkeypatch):
    board_id = await _board(db_factory)
    seed = await _refinement_with_direct_resources(db_factory, board_id)

    import okto_pulse.core.services.effective_resource_propagation as propagation_module

    async def _boom(*args, **kwargs):
        raise RuntimeError("simulated propagate_artifacts failure")

    monkeypatch.setattr(propagation_module, "propagate_artifacts", _boom)

    result = await _call(
        "okto_pulse_create_spec", board_id=board_id, title="Spec that fails propagation",
        refinement_id=seed["refinement_id"],
    )
    assert result.get("error") == "resource_propagation_failed", result
    assert result["failed_resource_type"] == "knowledge_base"
    assert "spec_id" in result and result["retryable"] is True
    assert "source_kb_id" in result["accepted_identity_fields"]
    # TEETH: transactional rollback — no spec persisted.
    async with db_factory() as db:
        specs = (await db.execute(
            select(Spec).where(Spec.board_id == board_id)
        )).scalars().all()
    assert specs == []
