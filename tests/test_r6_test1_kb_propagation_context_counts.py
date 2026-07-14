"""R6-TEST1 (card c2e0d161) — multi-hop KB propagation + unambiguous lineage counts
through the REAL MCP context tools.

* ts_0143209c: across ideation -> refinement -> spec the propagated KB shows AT MOST
  one ``[propagated from parent]`` prefix and preserves origin metadata
  (source_type / source_id / source_kb_id / source_title / source_version) — read
  back from okto_pulse_get_refinement_context / okto_pulse_get_spec_context.
* ts_3c459332: the spec context's lineage counts are unambiguous — raw/attachment
  total, direct, inherited, unique_effective + by_unique_resource — and a raw total
  is never presented as effective coverage (coverage_basis == "unique_effective").

Anti-test-theater: the chain is built through the REAL ``propagate_artifacts`` (the
single propagation path, R6-IMP1/R6-IMP4) and the assertions read the live payloads
of the REAL MCP context tools.
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
    Board,
    Ideation,
    IdeationKnowledgeBase,
    Refinement,
    RefinementKnowledgeBase,
    Spec,
    SpecKnowledgeBase,
    SpecStatus,
)
from okto_pulse.core.services.main import propagate_artifacts

USER_ID = "r6-test1-user"
PREFIX = "[propagated from parent]"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "r6 test1 agent"
        self.permissions = set()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_chain(db_factory):
    """ideation(+KB) -> refinement -> spec, propagated through propagate_artifacts."""
    board = _id("board")
    ideation_id, refinement_id, spec_id = _id("idea"), _id("ref"), _id("spec")
    ideation_kb_id = _id("kb")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 test1", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board, title="Idea",
                        created_by=USER_ID, version=1))
        db.add(Refinement(id=refinement_id, board_id=board, ideation_id=ideation_id,
                          title="Refinement", created_by=USER_ID, version=2))
        db.add(Spec(id=spec_id, board_id=board, title="Spec", status=SpecStatus.IN_PROGRESS,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[], version=3))
        db.add(IdeationKnowledgeBase(
            id=ideation_kb_id, ideation_id=ideation_id, title="Glossary",
            description="domain terms", content="body", created_by=USER_ID))
        await db.commit()

    # Hop 1: ideation -> refinement
    async with db_factory() as db:
        ideation_kb = await db.get(IdeationKnowledgeBase, ideation_kb_id)
        refinement = await db.get(Refinement, refinement_id)
        await propagate_artifacts(
            db, None, None, [ideation_kb], refinement, RefinementKnowledgeBase, USER_ID,
            source_type="ideation", source_id=ideation_id, source_title="Idea",
            source_version=1,
        )
        await db.commit()
    # Hop 2: refinement -> spec
    async with db_factory() as db:
        ref_kb = (await db.execute(select(RefinementKnowledgeBase).where(
            RefinementKnowledgeBase.refinement_id == refinement_id))).scalar_one()
        spec = await db.get(Spec, spec_id)
        await propagate_artifacts(
            db, None, None, [ref_kb], spec, SpecKnowledgeBase, USER_ID,
            source_type="refinement", source_id=refinement_id, source_title="Refinement",
            source_version=2,
        )
        await db.commit()
    return {"board": board, "ideation_id": ideation_id, "refinement_id": refinement_id,
            "spec_id": spec_id, "ideation_kb_id": ideation_kb_id, "ref_kb_id": ref_kb.id}


def _find_propagated_kb(kbs: list[dict]) -> dict:
    for kb in kbs:
        if PREFIX in (kb.get("description") or ""):
            return kb
    raise AssertionError(f"no propagated KB found in {kbs}")


# ===========================================================================
# ts_0143209c — KB propagation: at-most-one prefix + origin metadata
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_0143209c_refinement_context_kb_single_prefix_and_metadata(db_factory):
    ids = await _seed_chain(db_factory)
    ctx = await _call("okto_pulse_get_refinement_context", board_id=ids["board"],
                      refinement_id=ids["refinement_id"])
    kbs = ctx.get("knowledge_bases") or []
    kb = _find_propagated_kb(kbs)
    # Hop 1: exactly one prefix; origin metadata points at the ideation KB.
    assert kb["description"].count(PREFIX) == 1
    assert kb["source_type"] == "ideation"
    assert kb["source_id"] == ids["ideation_id"]
    assert kb["source_kb_id"] == ids["ideation_kb_id"]
    assert kb["source_title"] == "Idea"
    assert kb["source_version"] == 1
    assert "domain terms" in kb["description"]


@pytest.mark.asyncio
async def test_ts_0143209c_spec_context_kb_single_prefix_after_two_hops(db_factory):
    ids = await _seed_chain(db_factory)
    ctx = await _call("okto_pulse_get_spec_context", board_id=ids["board"],
                      spec_id=ids["spec_id"], profile="full")
    kbs = ctx.get("knowledge_bases") or []
    kb = _find_propagated_kb(kbs)
    # After TWO hops the prefix is still applied AT MOST ONCE (R6-IMP1 idempotency).
    assert kb["description"].count(PREFIX) == 1
    # Origin metadata is the immediate parent (the refinement KB) — preserved.
    assert kb["source_type"] == "refinement"
    assert kb["source_id"] == ids["refinement_id"]
    assert kb["source_kb_id"] == ids["ref_kb_id"]
    assert kb["source_title"] == "Refinement"
    assert kb["source_version"] == 2
    assert "domain terms" in kb["description"]


# ===========================================================================
# ts_3c459332 — unambiguous lineage counts (raw vs effective)
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_3c459332_spec_context_lineage_counts_unambiguous(db_factory):
    ids = await _seed_chain(db_factory)
    ctx = await _call("okto_pulse_get_spec_context", board_id=ids["board"],
                      spec_id=ids["spec_id"], profile="full")
    counts = ctx["resource_gate_summary"]["lineage_counts"]
    # Raw-vs-effective labels are explicit and consistent.
    assert counts["unique_effective_count"] == counts["unique_resources_count"]
    assert counts["raw_attachment_count"] == counts["attachment_count"]
    assert counts["coverage_basis"] == "unique_effective"
    # Direct/inherited/by_unique_resource present and unambiguous.
    assert "direct_resources_count" in counts
    assert "inherited_references_count" in counts
    assert isinstance(counts["by_unique_resource"], list)
    # The propagated KB is an effective resource (>=1); raw total is labelled
    # distinctly so it is never read as effective coverage.
    assert counts["unique_effective_count"] >= 1
    assert counts["raw_attachment_count"] >= 1
