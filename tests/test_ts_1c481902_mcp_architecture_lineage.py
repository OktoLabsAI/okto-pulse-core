"""Executable evidence for Pulse scenario ``ts_1c481902``.

The test drives the real MCP create-refinement, derive-spec and create-card
tools.  It proves that an Architecture snapshot keeps one canonical root while
``source_ref`` names the immediate physical parent at every hop.  It also
exercises a mixed valid/foreign explicit selection before target creation and
asserts that the failure leaves no partial spec, card, or Architecture row.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from r3_scenario_helpers import USER_ID, call_tool, sid
from sqlalchemy_test_models import (
    ArchitectureDesign,
    Board,
    Card,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)

from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.resource_lineage import ResolvedResourceLineageService


async def _seed_done_ideation_with_architecture(db_factory) -> dict[str, str]:
    board_id = sid("board-ts-1c481902")
    ideation_id = sid("idea-ts-1c481902")
    root_design_id = sid("arch-root-ts-1c481902")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="ts_1c481902 Architecture lineage",
                owner_id=USER_ID,
                settings={
                    "auto_derive_spec_resources_enabled": True,
                    "auto_derive_spec_resource_types": ["architecture"],
                },
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Architecture lineage root",
                status=IdeationStatus.DONE,
                created_by=USER_ID,
            )
        )
        db.add(
            ArchitectureDesign(
                id=root_design_id,
                board_id=board_id,
                parent_type="ideation",
                ideation_id=ideation_id,
                title="Canonical Architecture root",
                global_description="Root design propagated through every ceremony hop.",
                entities=[],
                interfaces=[],
                diagrams=[],
                created_by=USER_ID,
            )
        )
        await db.commit()
    return {
        "board_id": board_id,
        "ideation_id": ideation_id,
        "root_design_id": root_design_id,
    }


async def _architecture_rows(db_factory, board_id: str) -> list[ArchitectureDesign]:
    async with db_factory() as db:
        return list(
            (
                await db.execute(
                    select(ArchitectureDesign)
                    .where(ArchitectureDesign.board_id == board_id)
                    .order_by(ArchitectureDesign.parent_type, ArchitectureDesign.id)
                )
            )
            .scalars()
            .all()
        )


@pytest.mark.asyncio
async def test_ts_1c481902_mcp_multihop_and_atomic_mixed_selection(
    db_factory,
) -> None:
    seed = await _seed_done_ideation_with_architecture(db_factory)
    board_id = seed["board_id"]
    root_design_id = seed["root_design_id"]

    created_refinement = await call_tool(
        "okto_pulse_create_refinement",
        board_id=board_id,
        ideation_id=seed["ideation_id"],
        title="Refinement with governed Architecture",
        in_scope=["Preserve Architecture lineage"],
        architecture_design_ids=[root_design_id],
        architecture_propagation_mode="copy",
    )
    assert created_refinement.get("success") is True, created_refinement
    refinement_id = created_refinement["refinement"]["id"]

    refinement_designs = [
        row
        for row in await _architecture_rows(db_factory, board_id)
        if row.parent_type == "refinement" and row.refinement_id == refinement_id
    ]
    assert len(refinement_designs) == 1
    refinement_design = refinement_designs[0]
    assert refinement_design.source_design_id == root_design_id
    assert refinement_design.source_ref == f"architecture_design:{root_design_id}"

    # Derivation requires a completed parent; the transition itself is outside
    # this lineage scenario, so only the prerequisite status is established.
    async with db_factory() as db:
        refinement = await db.get(Refinement, refinement_id)
        assert refinement is not None
        refinement.status = RefinementStatus.DONE
        await db.commit()

    foreign_design_id = sid("arch-foreign-ts-1c481902")
    invalid = await call_tool(
        "okto_pulse_derive_spec_from_refinement",
        board_id=board_id,
        refinement_id=refinement_id,
        architecture_design_ids=[root_design_id, foreign_design_id],
        architecture_propagation_mode="copy",
    )
    assert invalid["error"] == "architecture_design_selection_invalid", invalid
    assert invalid["code"] == "architecture_design_selection_invalid"
    assert invalid["requested"] == [root_design_id, foreign_design_id]
    assert invalid["matched"] == [root_design_id]
    assert invalid["missing"] == [foreign_design_id]
    assert invalid["source_parent_type"] == "refinement"
    assert invalid["source_parent_id"] == refinement_id
    assert invalid["retryable"] is False

    # The invalid selection is preflighted before target/resource writes.
    async with db_factory() as db:
        assert list(
            (
                await db.execute(
                    select(Spec).where(Spec.refinement_id == refinement_id)
                )
            )
            .scalars()
            .all()
        ) == []
        assert list(
            (
                await db.execute(select(Card).where(Card.board_id == board_id))
            )
            .scalars()
            .all()
        ) == []
        partial_architecture = list(
            (
                await db.execute(
                    select(ArchitectureDesign).where(
                        ArchitectureDesign.board_id == board_id,
                        ArchitectureDesign.parent_type.in_(("spec", "card")),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert partial_architecture == []

    derived_spec = await call_tool(
        "okto_pulse_derive_spec_from_refinement",
        board_id=board_id,
        refinement_id=refinement_id,
        architecture_design_ids=[root_design_id],
        architecture_propagation_mode="copy",
    )
    assert derived_spec.get("success") is True, derived_spec
    spec_id = derived_spec["spec"]["id"]

    spec_designs = [
        row
        for row in await _architecture_rows(db_factory, board_id)
        if row.parent_type == "spec" and row.spec_id == spec_id
    ]
    assert len(spec_designs) == 1
    spec_design = spec_designs[0]
    assert spec_design.source_design_id == root_design_id
    assert spec_design.source_ref == f"architecture_design:{refinement_design.id}"

    # Card creation requires an approved-or-later spec.  Status setup is kept
    # outside the propagation oracle so the test remains about ts_1c481902.
    async with db_factory() as db:
        spec = await db.get(Spec, spec_id)
        assert spec is not None
        spec.status = SpecStatus.APPROVED
        await db.commit()

    created_card = await call_tool(
        "okto_pulse_create_card",
        board_id=board_id,
        spec_id=spec_id,
        title="Implementation card consuming the Architecture root",
    )
    assert created_card.get("success") is True, created_card
    card_id = created_card["card"]["id"]

    card_designs = [
        row
        for row in await _architecture_rows(db_factory, board_id)
        if row.parent_type == "card" and row.card_id == card_id
    ]
    assert len(card_designs) == 1
    card_design = card_designs[0]
    assert card_design.source_design_id == root_design_id
    assert card_design.source_ref == f"architecture_design:{spec_design.id}"

    async with db_factory() as db:
        lineage = await ResolvedResourceLineageService(
            ResourceGateService(db)
        ).resolve(
            board_id,
            "card",
            card_id,
            include_coverage=False,
        )

    architecture_attachments = [
        item
        for item in lineage.attachments
        if item.resource_type == "architecture" and item.effective
    ]
    assert {
        item.source_entity_type for item in architecture_attachments
    } == {"ideation", "refinement", "spec", "card"}
    assert {
        item.unique_resource_id for item in architecture_attachments
    } == {f"architecture:{root_design_id}"}
    assert lineage.counts["unique_effective_count"] == 1
    assert lineage.counts["raw_attachment_count"] == 4
