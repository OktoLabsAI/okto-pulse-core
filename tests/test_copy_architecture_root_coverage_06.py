"""Card 5c43a364 - copy_architecture_to_card must preserve the ROOT origin so the
Resource Gate recognizes coverage of a MULTI-HOP inherited Architecture.

Reproduces the real #12 topology (validated 2026-06-23): an ideation Architecture
(root) is COPIED to the refinement (refinement.source_design_id == ideation.id),
the spec inherits it (no direct), and the card copy must end up covered. The
existing fallback tests only cover single-level inheritance or two SEPARATE roots,
not the copy-chain that left #12 blocked.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    Refinement,
    Spec,
)
from okto_pulse.core.services.resource_gate import ResourceGateService

USER_ID = "user-5c43a364"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class _Ctx:
    def __init__(self):
        self.agent_id = USER_ID
        self.agent_name = "5c43a364 tester"
        self.permissions = object()


async def _call(name: str, **kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())), \
         patch.object(mcp_server, "check_permission", return_value=None), \
         patch.object(mcp_server, "_mcp_check_architecture_copy_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(name)
        raw = await tool.fn(**kwargs)
    return json.loads(raw)


async def _seed_copy_chain(db_factory) -> dict:
    """ideation arch A (root) --copied--> refinement arch B (source_design_id=A);
    spec inherits (no direct); in-progress card."""
    ids = {k: _id(k) for k in ("board", "idea", "ref", "spec", "card")}
    root_id = _id("arch-root-ideation")
    mid_id = _id("arch-mid-refinement")
    async with db_factory() as db:
        db.add(Board(id=ids["board"], name="5c43a364", owner_id=USER_ID))
        db.add(Ideation(id=ids["idea"], board_id=ids["board"], title="idea", created_by=USER_ID))
        # ROOT architecture at the ideation level.
        db.add(ArchitectureDesign(
            id=root_id, board_id=ids["board"], parent_type="ideation",
            ideation_id=ids["idea"], title="Shared architecture",
            global_description="root", entities=[], interfaces=[], diagrams=[],
            created_by=USER_ID,
        ))
        db.add(Refinement(
            id=ids["ref"], board_id=ids["board"], ideation_id=ids["idea"],
            title="refinement", created_by=USER_ID,
        ))
        # Refinement architecture is a COPY of the ideation root (the #12 topology).
        db.add(ArchitectureDesign(
            id=mid_id, board_id=ids["board"], parent_type="refinement",
            refinement_id=ids["ref"], title="Shared architecture",
            global_description="root", entities=[], interfaces=[], diagrams=[],
            created_by=USER_ID,
            source_design_id=root_id,
            source_ref=f"architecture_design:{root_id}",
        ))
        # Legacy/manual spec: inherits, no direct architecture.
        db.add(Spec(id=ids["spec"], board_id=ids["board"], refinement_id=ids["ref"],
                    ideation_id=ids["idea"], title="Legacy spec", created_by=USER_ID))
        db.add(Card(id=ids["card"], board_id=ids["board"], spec_id=ids["spec"],
                    title="impl card", status=CardStatus.IN_PROGRESS,
                    card_type=CardType.NORMAL, created_by=USER_ID))
        await db.commit()
    ids["root_id"] = root_id
    ids["mid_id"] = mid_id
    return ids


@pytest.mark.asyncio
async def test_copy_architecture_multihop_chain_satisfies_task_coverage(db_factory):
    seed = await _seed_copy_chain(db_factory)

    result = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board"], spec_id=seed["spec"], card_id=seed["card"],
        profile="full",
    )
    assert "error" not in result, result

    # END-TO-END TEETH: after the copy, the spec's inherited Architecture
    # obligation must be covered by the (non-cancelled) card.
    async with db_factory() as db:
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board"], seed["spec"],
        )
    arch_uncovered = [
        item for item in coverage["uncovered_resources"]
        if item["resource_type"] == "architecture"
    ]
    assert arch_uncovered == [], coverage["uncovered_resources"]

    # And the card carries an Architecture whose identity reaches the ROOT origin.
    async with db_factory() as db:
        designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card",
                ArchitectureDesign.card_id == seed["card"],
            )
        )).scalars().all()
    identity_values = {getattr(d, "source_design_id", None) for d in designs}
    identity_values |= {getattr(d, "root_source_design_id", None) for d in designs}
    assert seed["root_id"] in identity_values, (
        f"card architecture must reach root {seed['root_id']}; got {identity_values}"
    )


@pytest.mark.asyncio
async def test_copy_architecture_multihop_is_idempotent_no_duplicate(db_factory):
    # Re-sync teeth: copying twice must not duplicate the card Architecture and must
    # keep coverage satisfied (re-sync keys on source_ref, not the root identity).
    seed = await _seed_copy_chain(db_factory)
    first = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board"], spec_id=seed["spec"], card_id=seed["card"], profile="full",
    )
    assert "error" not in first, first

    async with db_factory() as db:
        before = (await db.execute(
            select(ArchitectureDesign).where(ArchitectureDesign.card_id == seed["card"])
        )).scalars().all()
    before_ids = {d.id for d in before}

    second = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board"], spec_id=seed["spec"], card_id=seed["card"], profile="full",
    )
    assert "error" not in second, second

    async with db_factory() as db:
        after = (await db.execute(
            select(ArchitectureDesign).where(ArchitectureDesign.card_id == seed["card"])
        )).scalars().all()
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board"], seed["spec"],
        )
    # No improper duplication: the second copy re-synced the same snapshot(s).
    assert {d.id for d in after} == before_ids, (before_ids, {d.id for d in after})
    assert [
        item for item in coverage["uncovered_resources"]
        if item["resource_type"] == "architecture"
    ] == [], coverage["uncovered_resources"]


@pytest.mark.asyncio
async def test_copy_architecture_design_ids_root_id_resolves_multihop(db_factory):
    # Original board symptom: copy_architecture_to_card(design_ids=[ROOT_ID]) returned
    # copied=0 in the multi-hop case because the fallback filter only compared the
    # immediate ref id. The effective ref's representative id may be the intermediate
    # (refinement) snapshot while the inherited obligation's canonical identity is the
    # ROOT (ideation) in source_design_id - so a design_ids filter must match the root.
    seed = await _seed_copy_chain(db_factory)

    result = await _call(
        "okto_pulse_copy_architecture_to_card",
        board_id=seed["board"], spec_id=seed["spec"], card_id=seed["card"],
        design_ids=[seed["root_id"]],
        profile="full",
    )
    assert "error" not in result, result
    # Must NOT be a silent copied=0; the root design_id resolved the multi-hop ref.
    designs_payload = result.get("architecture_designs") or []
    assert designs_payload, result

    async with db_factory() as db:
        card_designs = (await db.execute(
            select(ArchitectureDesign).where(ArchitectureDesign.card_id == seed["card"])
        )).scalars().all()
        coverage = await ResourceGateService(db).validate_spec_resource_task_coverage(
            seed["board"], seed["spec"],
        )
    assert card_designs, "design_ids=[root_id] must copy at least one design (not copied=0)"
    assert [
        item for item in coverage["uncovered_resources"]
        if item["resource_type"] == "architecture"
    ] == [], coverage["uncovered_resources"]
