"""
Tests for spec a8580f49 (Refinement A of ideation 41425528 — F8 + F6).

F8: a parent's N/A mark must inherit parent→child in the Resource Gate, so a
single mark at the ideation resolves the same resource to not_applicable at the
refinement/spec without a re-mark. Provided refs still win (precedence).
F6: auto_derive_spec_resources_enabled is documented as Spec→Card-only.

Integration tests (AC1/AC2/AC3) exercise ResourceGateService.get_summary against a
real DB via db_factory, mirroring tests/test_resource_gate_service.py. Doc/code-content
tests (AC4/AC5/AC6) read the reference doc + source. AC5 is load-bearing via
negative-wiring (control cases that would fail if the behavior/doc regressed).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Ideation,
    Refinement,
)
from okto_pulse.core.services.resource_gate import ResourceGateService

# ---------------------------------------------------------------------------
# Paths (AC4/AC5/AC6)
# ---------------------------------------------------------------------------

CORE_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"
ERRORS_MD = CORE_DIR / "mcp" / "resources" / "reference" / "errors.md"
RESOURCE_GATE_PY = CORE_DIR / "services" / "resource_gate.py"
SPEC_PROP_PY = CORE_DIR / "services" / "spec_resource_propagation.py"

SPEC_CARD_ONLY = "Spec→Card-only"  # "Spec→Card-only"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _doc_states_spec_card_only(text: str) -> bool:
    """The F6 doc predicate (load-bearing): names the setting, its Spec→Card-only
    scope, the propagate_for_card mechanism, and the no-auto-fill caveat."""
    return (
        "auto_derive_spec_resources_enabled" in text
        and SPEC_CARD_ONLY in text
        and "propagate_for_card" in text
        and "does NOT auto-fill" in text
    )


# ---------------------------------------------------------------------------
# AC1 + AC3 + AC5 (no-inheritance control) — parent N/A resolves child to not_applicable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac1_parent_na_inherits_to_child(db_factory) -> None:
    board_id, actor_id = _id("board"), _id("agent")
    ideation_id, refinement_id = _id("idea"), _id("ref")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="RG N/A inheritance", owner_id=actor_id))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="Idea", created_by=actor_id))
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Ref",
                created_by=actor_id,
            )
        )
        await db.commit()
        service = ResourceGateService(db)

        # Negative-wiring (no-inheritance control): before any parent mark, the
        # child's mockup is `missing` — so a later `not_applicable` must come from
        # the inherited mark, not a vacuous default.
        before = await service.get_summary(board_id, "refinement", refinement_id)
        before_by = {i["resource_type"]: i for i in before["resources"]}
        assert before_by["mockup"]["state"] == "missing"

        # Mark Mockup N/A on the IDEATION (the parent) only.
        await service.mark_not_applicable(
            board_id,
            "ideation",
            ideation_id,
            "mockup",
            actor_id,
            justification="Mockup not applicable at the ideation level.",
            source_channel="ui",
        )

        # AC1: the child refinement now resolves Mockup to not_applicable BY
        # INHERITANCE, with no re-mark on the refinement.
        after = await service.get_summary(board_id, "refinement", refinement_id)
        after_by = {i["resource_type"]: i for i in after["resources"]}
        assert after_by["mockup"]["state"] == "not_applicable"
        na = after_by["mockup"]["na_mark"]
        assert na is not None
        assert na["effective"] is True
        # AC3: the inherited mark exposes its source.
        assert na["inherited"] is True
        assert na["source_entity_type"] == "ideation"
        assert na["source_entity_id"] == ideation_id
        lineage_attachment = next(
            item for item in after["resource_lineage"]["attachments"]
            if item["resource_type"] == "mockup"
            and item["attachment_kind"] == "not_applicable"
        )
        assert lineage_attachment["coverage_state"] == "not_applicable"
        assert lineage_attachment["effective"] is True
        assert lineage_attachment["inherited"] is True
        assert lineage_attachment["source_entity_type"] == "ideation"
        assert lineage_attachment["source_entity_id"] == ideation_id


# ---------------------------------------------------------------------------
# AC2 + AC5 (precedence control) — provided beats N/A
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac2_provided_beats_inherited_na(db_factory) -> None:
    board_id, actor_id = _id("board"), _id("agent")
    ideation_id, refinement_id = _id("idea"), _id("ref")

    async with db_factory() as db:
        db.add(Board(id=board_id, name="RG precedence", owner_id=actor_id))
        db.add(Ideation(id=ideation_id, board_id=board_id, title="Idea", created_by=actor_id))
        # The ideation PROVIDES architecture.
        db.add(
            ArchitectureDesign(
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Idea architecture",
                global_description="Architecture context",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=actor_id,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Ref",
                created_by=actor_id,
            )
        )
        await db.commit()
        service = ResourceGateService(db)

        # Mark BOTH architecture (provided) and mockup (not provided) N/A at the ideation.
        for rt in ("architecture", "mockup"):
            await service.mark_not_applicable(
                board_id, "ideation", ideation_id, rt, actor_id,
                justification=f"{rt} marked at ideation for the precedence test.",
                source_channel="ui",
            )

        summary = await service.get_summary(board_id, "refinement", refinement_id)
        by = {i["resource_type"]: i for i in summary["resources"]}
        # AC2: architecture is inherited-PROVIDED, so it wins over the inherited N/A.
        assert by["architecture"]["state"] == "provided"
        inherited_arch_na = by["architecture"]["na_mark"]
        assert inherited_arch_na["inherited"] is True
        assert inherited_arch_na["effective"] is False
        lineage_arch_na = next(
            item for item in summary["resource_lineage"]["attachments"]
            if item["resource_type"] == "architecture"
            and item["attachment_kind"] == "not_applicable"
        )
        assert lineage_arch_na["coverage_state"] == "not_applicable"
        assert lineage_arch_na["effective"] is False
        assert lineage_arch_na["inherited"] is True
        assert lineage_arch_na["source_entity_type"] == "ideation"
        assert lineage_arch_na["source_entity_id"] == ideation_id
        # Negative-wiring (precedence control): mockup has the SAME inherited N/A but
        # no provided ref → not_applicable. Same mark, different outcome ⇒ provided>N/A
        # is real, not "always provided".
        assert by["mockup"]["state"] == "not_applicable"
        assert by["mockup"]["na_mark"]["inherited"] is True


# ---------------------------------------------------------------------------
# AC4 — F6 doc states the Spec→Card-only scope
# ---------------------------------------------------------------------------


def test_ac4_f6_doc_states_spec_card_only() -> None:
    assert _doc_states_spec_card_only(ERRORS_MD.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# AC5 — the F6 doc guard is load-bearing (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac5_doc_guard_is_load_bearing() -> None:
    assert _doc_states_spec_card_only(ERRORS_MD.read_text(encoding="utf-8"))
    # Synthetic stale doc (omits the Spec→Card-only scope / promises auto-fill) FAILS.
    synthetic = "auto_derive_spec_resources_enabled auto-fills resources everywhere."
    assert not _doc_states_spec_card_only(synthetic)


# ---------------------------------------------------------------------------
# AC6 — machinery byte-unchanged + setting not renamed
# ---------------------------------------------------------------------------


def test_ac6_machinery_unchanged_and_setting_not_renamed() -> None:
    gate = RESOURCE_GATE_PY.read_text(encoding="utf-8")
    # _resolve_state precedence is unchanged.
    assert "def _resolve_state(" in gate
    assert 'if list(direct) or list(inherited):' in gate
    assert '            return "provided"' in gate
    assert "if na_mark is not None:" in gate
    assert '            return "not_applicable"' in gate
    # Parent traversal crosses the adapter through a public port method.
    assert "async def load_parent_refs(" in gate
    assert "async def _load_parent_refs(" not in gate
    assert "async def load_active_marks(" in gate
    assert "async def _load_active_marks(" not in gate

    prop = SPEC_PROP_PY.read_text(encoding="utf-8")
    assert "class SpecResourcePropagationService:" in prop
    assert "async def propagate_for_card(" in prop
    # The board setting key is NOT renamed.
    assert "auto_derive_spec_resources_enabled" in prop
