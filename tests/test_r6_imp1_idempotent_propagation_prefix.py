"""R6-IMP1 (card 939bc943, FR1/AC1) — the ``[propagated from parent]`` KB marker is
applied AT MOST ONCE across a multi-hop propagation chain, and origin metadata is
preserved.

Every propagation hop (ideation -> refinement -> spec -> card) copies the parent's
KB description through the single ``propagate_artifacts`` path. Before the fix the
prefix stacked on each hop; now it is idempotent.

Anti-test-theater: the multi-hop chain runs through the REAL ``propagate_artifacts``
over REAL KB rows, and the assertion counts the marker occurrences on the final hop.
"""

from __future__ import annotations

import uuid

import pytest

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
from okto_pulse.core.services.main import (
    _PROPAGATED_KB_PREFIX,
    _propagated_kb_description,
    propagate_artifacts,
)

USER_ID = "r6-imp1-user"
PREFIX = _PROPAGATED_KB_PREFIX


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ===========================================================================
# Unit — the idempotent marker helper
# ===========================================================================


def test_prefix_applied_once_on_plain_description():
    out = _propagated_kb_description("original body")
    assert out == f"{PREFIX} original body"
    assert out.count(PREFIX) == 1


def test_prefix_not_stacked_on_already_prefixed():
    once = _propagated_kb_description("original body")
    twice = _propagated_kb_description(once)
    thrice = _propagated_kb_description(twice)
    assert twice == once == thrice
    assert thrice.count(PREFIX) == 1


def test_prefix_handles_empty_and_none():
    assert _propagated_kb_description(None) == PREFIX
    assert _propagated_kb_description("") == PREFIX
    assert _propagated_kb_description("   ") == PREFIX
    # re-propagating the empty-but-marked description does not stack.
    assert _propagated_kb_description(PREFIX).count(PREFIX) == 1


# ===========================================================================
# Integration — multi-hop chain through the REAL propagate_artifacts
# ===========================================================================


@pytest.mark.asyncio
async def test_multihop_chain_applies_prefix_once_and_preserves_metadata(db_factory):
    board_id = _id("board")
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    spec_id = _id("spec")
    ideation_kb_id = _id("kb")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="r6 imp1", owner_id=USER_ID))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="Idea",
                        created_by=USER_ID, version=1))
        db.add(Refinement(id=refinement_id, board_id=board_id, ideation_id=ideation_id,
                          title="Refinement", created_by=USER_ID, version=2))
        db.add(Spec(id=spec_id, board_id=board_id, title="Spec", status=SpecStatus.DRAFT,
                    created_by=USER_ID, functional_requirements=[], acceptance_criteria=[],
                    test_scenarios=[], business_rules=[], api_contracts=[], version=3))
        db.add(IdeationKnowledgeBase(
            id=ideation_kb_id, ideation_id=ideation_id, title="Domain glossary",
            description="canonical domain terms", content="body", created_by=USER_ID))
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

    # Hop 2: refinement -> spec (source KB description is ALREADY prefixed)
    async with db_factory() as db:
        from sqlalchemy import select
        ref_kbs = (await db.execute(
            select(RefinementKnowledgeBase).where(
                RefinementKnowledgeBase.refinement_id == refinement_id))).scalars().all()
        assert len(ref_kbs) == 1
        ref_kb = ref_kbs[0]
        # the first hop applied the marker exactly once.
        assert ref_kb.description.count(PREFIX) == 1
        spec = await db.get(Spec, spec_id)
        await propagate_artifacts(
            db, None, None, [ref_kb], spec, SpecKnowledgeBase, USER_ID,
            source_type="refinement", source_id=refinement_id, source_title="Refinement",
            source_version=2,
        )
        await db.commit()

    # Final hop assertion: the spec KB carries the marker EXACTLY ONCE (no stacking).
    async with db_factory() as db:
        from sqlalchemy import select
        spec_kbs = (await db.execute(
            select(SpecKnowledgeBase).where(
                SpecKnowledgeBase.spec_id == spec_id))).scalars().all()
        assert len(spec_kbs) == 1
        spec_kb = spec_kbs[0]
        assert spec_kb.description.count(PREFIX) == 1, spec_kb.description
        assert spec_kb.description.startswith(PREFIX)
        # the original body survived (not truncated by the prefix logic).
        assert "canonical domain terms" in spec_kb.description
        # origin metadata preserved on the last hop.
        assert spec_kb.source_type == "refinement"
        assert spec_kb.source_id == refinement_id
        assert spec_kb.source_title == "Refinement"
        assert spec_kb.source_version == 2
        assert spec_kb.source_kb_id == ref_kb.id
        assert spec_kb.title == "Domain glossary"
        assert spec_kb.content == "body"
