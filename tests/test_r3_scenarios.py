"""R3 scenario coverage (cards R3-TEST1..4) — one test per spec scenario_id.

Anti-test-theater: every scenario drives the REAL MCP tools (create_spec / copy_*)
over a REAL board->ideation->refinement->spec/card lineage and the REAL Resource
Gate / lineage services. Identity is matched with the gate's own
``_resource_identity_values``; the end-to-end scenarios assert the gate's
spec->task coverage actually flips to satisfied (no disable / skip / auto-N/A).
"""

from __future__ import annotations

import os
import sys
import tempfile

import pytest
from sqlalchemy import select

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_r3scn_"))

from r3_scenario_helpers import (
    USER_ID,
    add_card,
    call_tool,
    new_board,
    seed_legacy_spec_with_card,
    seed_refinement,
)

from sqlalchemy_test_models import (
    ArchitectureDesign,
    Card,
    Ideation,
    IdeationStatus,
    ResourceNotApplicable,
    Spec,
    SpecKnowledgeBase,
)
from okto_pulse.core.services.resource_gate import ResourceGateService


async def _no_na_marks(db_factory, board_id) -> bool:
    async with db_factory() as db:
        rows = (await db.execute(
            select(ResourceNotApplicable).where(ResourceNotApplicable.board_id == board_id)
        )).scalars().all()
    return len(rows) == 0


async def _seed_done_derive_parent(db_factory, board_id, source: str) -> str:
    if source == "refinement":
        seed = await seed_refinement(db_factory, board_id, status="done")
        return seed["refinement_id"]

    from r3_scenario_helpers import sid

    ideation_id = sid("idea")
    async with db_factory() as db:
        db.add(Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Done derive parent",
            status=IdeationStatus.DONE,
            created_by=USER_ID,
        ))
        await db.commit()
    return ideation_id


def _derive_tool(source: str) -> str:
    return f"okto_pulse_derive_spec_from_{source}"


def _derive_id_argument(source: str, source_id: str) -> dict[str, str]:
    return {f"{source}_id": source_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["ideation", "refinement"])
async def test_derive_spec_rejects_cross_board_parent_before_write(db_factory, source):
    authenticated_board_id = await new_board(db_factory)
    foreign_board_id = await new_board(db_factory)
    source_id = await _seed_done_derive_parent(db_factory, foreign_board_id, source)

    result = await call_tool(
        _derive_tool(source),
        board_id=authenticated_board_id,
        **_derive_id_argument(source, source_id),
    )

    assert result == {"error": f"{source.title()} not found"}
    source_column = Spec.ideation_id if source == "ideation" else Spec.refinement_id
    async with db_factory() as db:
        specs = (await db.execute(
            select(Spec).where(source_column == source_id)
        )).scalars().all()
    assert specs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["ideation", "refinement"])
async def test_derive_spec_missing_parent_is_governed_and_writes_nothing(
    db_factory, source
):
    board_id = await new_board(db_factory)
    source_id = f"missing-{source}"

    result = await call_tool(
        _derive_tool(source),
        board_id=board_id,
        **_derive_id_argument(source, source_id),
    )

    assert result == {"error": f"{source.title()} not found"}
    async with db_factory() as db:
        specs = (await db.execute(
            select(Spec).where(Spec.board_id == board_id)
        )).scalars().all()
    assert specs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["ideation", "refinement"])
async def test_derive_spec_accepts_same_board_parent(db_factory, source):
    board_id = await new_board(db_factory)
    source_id = await _seed_done_derive_parent(db_factory, board_id, source)

    result = await call_tool(
        _derive_tool(source),
        board_id=board_id,
        **_derive_id_argument(source, source_id),
    )

    assert result.get("success") is True, result
    async with db_factory() as db:
        spec = await db.get(Spec, result["spec"]["id"])
    assert spec is not None and spec.board_id == board_id
    assert getattr(spec, f"{source}_id") == source_id


# ===========================================================================
# R3-TEST1 — create_spec resolves effective resources; gate & copy share the list
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_d4ddaf4e_create_spec_materializes_effective_before_cards(db_factory):
    """ts_d4ddaf4e: create_spec(refinement_id) materializes/resolves the effective
    resources up front (so cards derived later can carry them)."""
    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, kb=True, mockup=True)

    result = await call_tool(
        "okto_pulse_create_spec", board_id=board_id, title="manual spec",
        refinement_id=ref["refinement_id"],
    )
    assert result.get("success") is True, result
    assert result["resource_propagation"]["status"] == "created_with_resources"

    # The KB is MATERIALIZED on the spec (a real row) before any card derivation.
    async with db_factory() as db:
        kbs = (await db.execute(
            select(SpecKnowledgeBase).where(SpecKnowledgeBase.spec_id == result["spec"]["id"])
        )).scalars().all()
    assert any(getattr(kb, "source_kb_id", None) == ref["kb_id"] for kb in kbs)


@pytest.mark.asyncio
async def test_ts_6e228232_gate_summary_and_copy_share_effective_list(db_factory):
    """ts_6e228232: the gate summary and the copy tool consume the SAME effective
    list — the gate reports knowledge_base provided (inherited) and the copy tool
    falls back to that exact resource (no generic "No resources to copy")."""
    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, kb=True)
    legacy = await seed_legacy_spec_with_card(db_factory, board_id, ref)

    # Gate summary sees the inherited KB as provided, keyed by the effective kb id.
    async with db_factory() as db:
        summary = await ResourceGateService(db).get_summary(board_id, "spec", legacy["spec_id"])
    kb_state = next(r for r in summary["resources"] if r["resource_type"] == "knowledge_base")
    assert kb_state["state"] == "provided", summary["resources"]
    gate_ids = {
        a.get("id") for a in summary["resource_lineage"]["attachments"]
        if a.get("resource_type") == "knowledge_base"
    }
    assert ref["kb_id"] in gate_ids, gate_ids

    # The copy tool consumes the SAME effective resource (no generic error).
    copy = await call_tool(
        "okto_pulse_copy_knowledge_to_card",
        board_id=board_id, spec_id=legacy["spec_id"], card_id=legacy["card_id"],
    )
    assert copy.get("success") is True and copy["fallback"] is True
    assert "error" not in copy
    async with db_factory() as db:
        card = await db.get(Card, legacy["card_id"])
        card_kbs = list(card.knowledge_bases or [])
    assert any(kb.get("source_kb_id") == ref["kb_id"] for kb in card_kbs)


# ===========================================================================
# R3-TEST2 — inherited copy with gate identity; no auto-N/A, no provenance loss
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_3524d4ce_card_receives_all_inherited_with_gate_identity(db_factory):
    """ts_3524d4ce: a card receives KB + mockup + Architecture inherited via the
    copy tools, each with an identity ResourceGateService._resource_identity_values
    accepts (proven by the gate coverage flipping to satisfied)."""
    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, kb=True, mockup=True, architecture=True)
    legacy = await seed_legacy_spec_with_card(db_factory, board_id, ref)

    for tool in (
        "okto_pulse_copy_knowledge_to_card",
        "okto_pulse_copy_mockups_to_card",
        "okto_pulse_copy_architecture_to_card",
    ):
        res = await call_tool(tool, board_id=board_id, spec_id=legacy["spec_id"],
                              card_id=legacy["card_id"])
        assert "error" not in res, (tool, res)

    async with db_factory() as db:
        card = await db.get(Card, legacy["card_id"])
        card_kbs = list(card.knowledge_bases or [])
        card_mockups = list(card.screen_mockups or [])
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            board_id, legacy["spec_id"],
        )

    # Identity the gate reads is present for KB + mockup.
    assert any(ref["kb_id"] in ResourceGateService._resource_identity_values(kb) for kb in card_kbs)
    assert any(ref["mockup_id"] in ResourceGateService._resource_identity_values(m) for m in card_mockups)
    # End-to-end: every inherited spec obligation is covered by the card.
    assert coverage["allowed"] is True, coverage["uncovered_resources"]


@pytest.mark.asyncio
async def test_ts_7cc0dcf9_no_auto_na_and_no_provenance_loss(db_factory):
    """ts_7cc0dcf9: the fallback copy never creates an auto-N/A mark and never
    loses provenance (the copied resource keeps a gate-readable source identity)."""
    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, kb=True)
    legacy = await seed_legacy_spec_with_card(db_factory, board_id, ref)

    copy = await call_tool(
        "okto_pulse_copy_knowledge_to_card",
        board_id=board_id, spec_id=legacy["spec_id"], card_id=legacy["card_id"],
    )
    assert copy["fallback"] is True and copy["copied"] >= 1

    # No auto-N/A mark was created anywhere on the board.
    assert await _no_na_marks(db_factory, board_id)
    # Provenance preserved: the copied KB carries source + source_kb_id == effective id.
    async with db_factory() as db:
        card = await db.get(Card, legacy["card_id"])
        kbs = list(card.knowledge_bases or [])
    copied = [kb for kb in kbs if kb.get("source_kb_id") == ref["kb_id"]]
    assert copied and str(copied[0].get("source") or "").startswith("copied_from_refinement:")


# ===========================================================================
# R3-TEST3 — derive_spec no regression; legacy fallback or actionable error
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_e59fe6ad_derive_spec_from_refinement_still_propagates(db_factory):
    """ts_e59fe6ad: the existing derive_spec_from_refinement path is unchanged —
    it still propagates the refinement's resources into the derived spec."""
    from okto_pulse.core.services.main import RefinementService

    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, status="done", kb=True, mockup=True)

    async with db_factory() as db:
        spec = await RefinementService(db).derive_spec(
            ref["refinement_id"], USER_ID, skip_ownership_check=True,
        )
        await db.commit()
        assert spec is not None
        spec_id = spec.id
        kbs = (await db.execute(
            select(SpecKnowledgeBase).where(SpecKnowledgeBase.spec_id == spec_id)
        )).scalars().all()
        derived = await db.get(Spec, spec_id)
        mockups = list(derived.screen_mockups or [])

    assert any(getattr(kb, "source_kb_id", None) == ref["kb_id"] for kb in kbs), [
        getattr(kb, "source_kb_id", None) for kb in kbs
    ]
    assert any(m.get("origin_id") == ref["mockup_id"] for m in mockups), mockups


@pytest.mark.asyncio
async def test_derive_spec_returns_canonical_resource_propagation_summary(db_factory):
    board_id = await new_board(db_factory)
    ref = await seed_refinement(
        db_factory,
        board_id,
        status="done",
        kb=True,
        mockup=True,
        architecture=True,
    )

    result = await call_tool(
        "okto_pulse_derive_spec_from_refinement",
        board_id=board_id,
        refinement_id=ref["refinement_id"],
    )

    assert result.get("success") is True, result
    propagation = result["resource_propagation"]
    assert propagation["status"] == "created_with_resources"
    assert propagation["source"] == {
        "entity_type": "refinement",
        "entity_id": ref["refinement_id"],
    }
    assert propagation["target"]["entity_type"] == "spec"
    assert propagation["target"]["entity_id"] == result["spec"]["id"]
    assert propagation["counts"] == {
        "mockup": 1,
        "knowledge_base": 1,
        "architecture": 1,
    }
    assert propagation["by_type"]["architecture"]["source_design_ids"] == [
        ref["design_id"]
    ]

    # Reopen the target so the assertion covers persisted snapshots, not only
    # the transient write response.
    async with db_factory() as db:
        spec = await db.get(Spec, result["spec"]["id"])
        mockups = list(spec.screen_mockups or [])
        kbs = (await db.execute(
            select(SpecKnowledgeBase).where(
                SpecKnowledgeBase.spec_id == result["spec"]["id"]
            )
        )).scalars().all()
        designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "spec",
                ArchitectureDesign.spec_id == result["spec"]["id"],
            )
        )).scalars().all()

    assert len(mockups) == 1
    assert mockups[0]["origin_id"] == ref["mockup_id"]
    assert mockups[0]["source_mockup_id"] == ref["mockup_id"]
    assert len(kbs) == 1
    assert kbs[0].root_source_kb_id == ref["kb_id"]
    assert kbs[0].immediate_parent_kb_id == ref["kb_id"]
    assert len(designs) == 1
    assert designs[0].source_design_id == ref["design_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("argument_name", "resource_type", "valid_id_key", "foreign_prefix"),
    [
        ("mockup_ids", "mockup", "mockup_id", "foreign-mockup"),
        ("kb_ids", "knowledge_base", "kb_id", "foreign-kb"),
    ],
)
async def test_derive_spec_rejects_mixed_foreign_selection_before_target_write(
    db_factory,
    argument_name,
    resource_type,
    valid_id_key,
    foreign_prefix,
):
    board_id = await new_board(db_factory)
    ref = await seed_refinement(
        db_factory,
        board_id,
        status="done",
        kb=True,
        mockup=True,
    )
    foreign_id = f"{foreign_prefix}-outside-source"

    result = await call_tool(
        "okto_pulse_derive_spec_from_refinement",
        board_id=board_id,
        refinement_id=ref["refinement_id"],
        **{argument_name: [ref[valid_id_key], foreign_id]},
    )

    assert result["error"] == "resource_selection_invalid", result
    assert result["resource_type"] == resource_type
    assert result["requested"] == [ref[valid_id_key], foreign_id]
    assert result["matched"] == [ref[valid_id_key]]
    assert result["missing"] == [foreign_id]
    assert result["source_parent_type"] == "refinement"
    assert result["source_parent_id"] == ref["refinement_id"]
    assert result["retryable"] is False

    async with db_factory() as db:
        specs = (await db.execute(
            select(Spec).where(Spec.refinement_id == ref["refinement_id"])
        )).scalars().all()
    assert specs == []


@pytest.mark.asyncio
async def test_ts_2e4169c2_legacy_spec_falls_back_or_actionable_error(db_factory):
    """ts_2e4169c2: a legacy spec without direct resources copies via the effective
    fallback; when nothing is required it is an honest empty (NOT a generic error),
    and the structured actionable error shape carries
    resource_type/coverage_obligation_id/accepted_identity_fields."""
    board_id = await new_board(db_factory)
    # (a) fallback path — refinement carries the KB.
    ref = await seed_refinement(db_factory, board_id, kb=True)
    legacy = await seed_legacy_spec_with_card(db_factory, board_id, ref)
    copy = await call_tool(
        "okto_pulse_copy_knowledge_to_card",
        board_id=board_id, spec_id=legacy["spec_id"], card_id=legacy["card_id"],
    )
    assert copy.get("success") is True and copy["fallback"] is True
    assert "error" not in copy

    # (b) nothing required anywhere -> honest empty, never generic "No X to copy".
    bare_ref = await seed_refinement(db_factory, board_id)
    bare = await seed_legacy_spec_with_card(db_factory, board_id, bare_ref)
    empty = await call_tool(
        "okto_pulse_copy_knowledge_to_card",
        board_id=board_id, spec_id=bare["spec_id"], card_id=bare["card_id"],
    )
    assert empty.get("success") is True and empty.get("copied") == 0
    assert empty.get("reason") == "no_resource_required" and "error" not in empty

    # (c) the structured actionable error shape (provided-but-unresolvable).
    from okto_pulse.core.mcp.server import _effective_empty_copy_response
    import json
    err = json.loads(_effective_empty_copy_response(
        "knowledge_base",
        {"not_applicable": False, "has_obligation": True,
         "coverage_obligation_id": "knowledge_base:kb-x",
         "accepted_identity_fields": ["source_kb_id", "id", "source", "source_ref"]},
    ))
    assert err["error"] == "resource_propagation_failed"
    assert err["resource_type"] == "knowledge_base"
    assert err["coverage_obligation_id"] == "knowledge_base:kb-x"
    assert "source_kb_id" in err["accepted_identity_fields"]


# ===========================================================================
# R3-TEST4 — E2E without disable / skip / auto-N/A
# ===========================================================================


@pytest.mark.asyncio
async def test_ts_a69e5f5a_e2e_create_spec_card_copy_gate_clean(db_factory):
    """ts_a69e5f5a: create_spec manual -> card -> copy tools -> Resource Gate
    coverage satisfied, with NO disable, NO skip and NO auto-N/A mark anywhere."""
    board_id = await new_board(db_factory)
    ref = await seed_refinement(db_factory, board_id, kb=True)

    # 1. Create the spec manually from the refinement (effective propagation).
    created = await call_tool(
        "okto_pulse_create_spec", board_id=board_id, title="E2E manual spec",
        refinement_id=ref["refinement_id"],
    )
    assert created.get("success") is True, created
    spec_id = created["spec"]["id"]

    # 2. A task card under the spec ...
    card_id = await add_card(db_factory, board_id, spec_id)

    # 3. ... carries the spec's (now direct) KB via the copy tool ...
    copy = await call_tool(
        "okto_pulse_copy_knowledge_to_card",
        board_id=board_id, spec_id=spec_id, card_id=card_id,
    )
    assert copy.get("success") is True and copy["copied"] >= 1

    # 4. ... and the Resource Gate spec->task coverage is satisfied WITHOUT any
    #    disable / skip / auto-N/A.
    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            board_id, spec_id,
        )
    assert coverage["allowed"] is True, coverage["uncovered_resources"]
    assert await _no_na_marks(db_factory, board_id)
