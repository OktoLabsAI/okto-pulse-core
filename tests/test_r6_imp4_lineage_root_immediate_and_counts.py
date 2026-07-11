"""R6-IMP4 (card 4afe0124, FR4/AC4) — multi-hop KB lineage (root vs immediate
parent) + unambiguous raw-vs-effective lineage count labels.

* Across ideation -> refinement -> spec, ``root_source_kb_id`` stays the INITIAL
  canonical origin (never overwritten by the immediate parent), while
  ``immediate_parent_kb_id`` is the direct parent of each hop; ``source_kb_id``
  remains the immediate parent (back-compat). The [propagated from parent] prefix
  is applied at most once (R6-IMP1).
* The lineage counts expose explicit ``unique_effective_count`` /
  ``raw_attachment_count`` / ``coverage_basis`` so a raw attachment total is never
  read as effective coverage.

Anti-test-theater: the chain runs through the REAL ``propagate_artifacts`` over
real KB rows; the counts are read from the REAL ``ResourceGateService.get_summary``.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

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
from okto_pulse.core.services.resource_gate import ResourceGateService

USER_ID = "r6-imp4-user"
PREFIX = "[propagated from parent]"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ===========================================================================
# Part 2 — multi-hop root vs immediate parent (root never overwritten)
# ===========================================================================


@pytest.mark.asyncio
async def test_multihop_preserves_root_and_tracks_immediate_parent(db_factory):
    board = _id("board")
    ideation_id, refinement_id, spec_id = _id("idea"), _id("ref"), _id("spec")
    ideation_kb_id = _id("kb")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp4", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board, title="Idea",
                        created_by=USER_ID, version=1))
        db.add(Refinement(id=refinement_id, board_id=board, ideation_id=ideation_id,
                          title="Refinement", created_by=USER_ID, version=2))
        db.add(Spec(id=spec_id, board_id=board, title="Spec", status=SpecStatus.DRAFT,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[], version=3))
        db.add(IdeationKnowledgeBase(
            id=ideation_kb_id, ideation_id=ideation_id, title="Glossary",
            description="domain terms", content="body", created_by=USER_ID))
        await db.commit()

    # Hop 1: ideation -> refinement (origin hop: root == immediate == parent).
    async with db_factory() as db:
        ideation_kb = await db.get(IdeationKnowledgeBase, ideation_kb_id)
        refinement = await db.get(Refinement, refinement_id)
        await propagate_artifacts(
            db, None, None, [ideation_kb], refinement, RefinementKnowledgeBase, USER_ID,
            source_type="ideation", source_id=ideation_id, source_title="Idea",
            source_version=1,
        )
        await db.commit()

    async with db_factory() as db:
        ref_kb = (await db.execute(select(RefinementKnowledgeBase).where(
            RefinementKnowledgeBase.refinement_id == refinement_id))).scalar_one()
    assert ref_kb.source_kb_id == ideation_kb_id
    assert ref_kb.immediate_parent_kb_id == ideation_kb_id
    assert ref_kb.root_source_kb_id == ideation_kb_id      # origin hop
    assert ref_kb.description.count(PREFIX) == 1

    # Hop 2: refinement -> spec. root MUST stay the ideation KB (the canonical
    # origin); immediate parent becomes the refinement KB.
    async with db_factory() as db:
        ref_kb = await db.get(RefinementKnowledgeBase, ref_kb.id)
        spec = await db.get(Spec, spec_id)
        await propagate_artifacts(
            db, None, None, [ref_kb], spec, SpecKnowledgeBase, USER_ID,
            source_type="refinement", source_id=refinement_id, source_title="Refinement",
            source_version=2,
        )
        await db.commit()

    async with db_factory() as db:
        spec_kb = (await db.execute(select(SpecKnowledgeBase).where(
            SpecKnowledgeBase.spec_id == spec_id))).scalar_one()
    # THE KEY TEETH: root preserved (NOT overwritten with the immediate parent).
    assert spec_kb.root_source_kb_id == ideation_kb_id
    assert spec_kb.immediate_parent_kb_id == ref_kb.id
    assert spec_kb.source_kb_id == ref_kb.id               # back-compat = immediate
    assert spec_kb.root_source_kb_id != spec_kb.immediate_parent_kb_id
    # Prefix still applied exactly once after 2 hops (R6-IMP1 coordination).
    assert spec_kb.description.count(PREFIX) == 1
    assert "domain terms" in spec_kb.description


# ===========================================================================
# Part 1 — unambiguous raw-vs-effective count labels
# ===========================================================================


@pytest.mark.asyncio
async def test_lineage_counts_label_raw_vs_effective(db_factory):
    board = _id("board")
    spec_id = _id("spec")
    async with db_factory() as db:
        db.add(Board(id=board, name="r6 imp4", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board, title="Spec", status=SpecStatus.DRAFT,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[]))
        db.add(SpecKnowledgeBase(
            id=_id("kb"), spec_id=spec_id, title="Direct KB",
            description="d", content="c", created_by=USER_ID))
        await db.commit()

    async with db_factory() as db:
        summary = await ResourceGateService(db).get_summary(board, "spec", spec_id)

    counts = summary["lineage_counts"]
    # New explicit labels are present and consistent with the legacy keys.
    assert counts["unique_effective_count"] == counts["unique_resources_count"]
    assert counts["raw_attachment_count"] == counts["attachment_count"]
    assert counts["coverage_basis"] == "unique_effective"
    # The direct KB shows up as an effective resource (>=1) — raw total is labelled
    # distinctly so it is never mistaken for effective coverage.
    assert counts["unique_effective_count"] >= 1
    assert "attachment_count" in counts and "unique_resources_count" in counts
