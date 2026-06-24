"""R3 bug eded2f0e — architecture warning-ack regression, updated for Spec B.

Authoring gate (UNCHANGED): a DIRECT repo.create / repo.update of a warning-bearing
design WITHOUT an acknowledgement still raises ArchitectureWarningAcknowledgementRequired.

Spec B (architecture propagation eligibility) SUPERSEDES the old R3 "Option B"
behavior: a system/copy-scoped acknowledgement is audit-only and NO LONGER authorizes
copying or propagating a warning-bearing source (one carrying active critic findings).
Both copy_from_parent and the SDLC internal-snapshot path (propagate_architecture_designs)
now fail closed with the canonical ArchitecturePropagationBlocked error
(architecture_propagation_blocked). See FR 67f3545a / TR 392cd7aa.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.models.db import (
    ArchitectureDesign,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignUpdate,
    ArchitectureWarningAcknowledgementRequest,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitecturePropagationBlocked,
    ArchitecturePropagationService,
    ArchitectureWarningAcknowledgementRequired,
)

USER_ID = "user-r3-bug"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _warning_payload(**extra) -> ArchitectureDesignCreate:
    """A design the architecture critic flags with structured warnings (an API
    entity without supporting interface/diagram)."""
    return ArchitectureDesignCreate(
        title="Warning-bearing arch",
        global_description="Design that the critic flags with structured warnings.",
        entities=[
            {"id": "svc-api", "name": "Demo API", "entity_type": "api",
             "responsibility": "Handles demo traffic."}
        ],
        interfaces=[],
        diagrams=[],
        **extra,
    )


def _ack() -> ArchitectureWarningAcknowledgementRequest:
    return ArchitectureWarningAcknowledgementRequest(
        accepted=True, statement="author acknowledges the warnings")


async def _seed(db_factory):
    board_id = _id("board")
    spec_id = _id("spec")
    card_id = _id("card")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="r3 bug", owner_id=USER_ID))
        db.add(Spec(id=spec_id, board_id=board_id, title="spec", status=SpecStatus.APPROVED,
                    created_by=USER_ID, functional_requirements=["FR"],
                    acceptance_criteria=["AC"], test_scenarios=[], business_rules=[],
                    api_contracts=[]))
        db.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title="card",
                    status=CardStatus.IN_PROGRESS, card_type=CardType.NORMAL,
                    created_by=USER_ID))
        await db.commit()
    return board_id, spec_id, card_id


# ===========================================================================
# NEGATIVE PROOF — the authoring gate is unchanged
# ===========================================================================


@pytest.mark.asyncio
async def test_direct_create_without_ack_still_raises(db_factory):
    _board_id, spec_id, _card_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        with pytest.raises(ArchitectureWarningAcknowledgementRequired):
            await repo.create("spec", spec_id, _warning_payload(), USER_ID)


@pytest.mark.asyncio
async def test_direct_update_without_ack_still_raises(db_factory):
    _board_id, spec_id, _card_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        # Author the design WITH an acknowledgement first ...
        design = await repo.create(
            "spec", spec_id,
            _warning_payload(architecture_warning_acknowledgement=_ack()), USER_ID,
        )
        await db.commit()
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        # ... a DIRECT update that re-introduces warnings WITHOUT an ack still raises.
        with pytest.raises(ArchitectureWarningAcknowledgementRequired):
            await repo.update(
                design.id,
                ArchitectureDesignUpdate(
                    entities=[{"id": "svc-api", "name": "Demo API v2",
                               "entity_type": "api", "responsibility": "refreshed"}],
                    change_summary="no ack on this authoring update",
                ),
                USER_ID,
            )


# ===========================================================================
# SPEC B — system/copy-scoped ack is audit-only; warning-bearing copy is blocked
# ===========================================================================


@pytest.mark.asyncio
async def test_copy_from_parent_blocks_warning_bearing_source(db_factory):
    """Spec B: copy_from_parent of a warning-bearing source (active findings) is blocked
    by the canonical eligibility policy — even a copy-scoped acknowledgement does NOT
    authorize the copy (the ack is audit-only)."""
    _board_id, spec_id, card_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        await repo.create(
            "spec", spec_id,
            _warning_payload(architecture_warning_acknowledgement=_ack()), USER_ID,
        )
        await db.commit()
    async with db_factory() as db:
        service = ArchitecturePropagationService(db)
        with pytest.raises(ArchitecturePropagationBlocked) as excinfo:
            await service.copy_from_parent(
                source_parent_type="spec", source_parent_id=spec_id,
                target_parent_type="card", target_parent_id=card_id, actor_id=USER_ID,
                architecture_warning_acknowledgement=_ack(),
            )
    assert excinfo.value.to_payload()["code"] == "architecture_propagation_blocked"


@pytest.mark.asyncio
async def test_internal_snapshot_propagation_blocks_warning_bearing_source(db_factory):
    """Spec B / TR 392cd7aa (replaces the old R3 positive proof): the SDLC
    internal-snapshot path (propagate_architecture_designs) supplies a SYSTEM
    acknowledgement, but that ack is audit-only and no longer authorizes copying a
    warning-bearing source. The propagation fails closed and NO design is copied."""
    from okto_pulse.core.services.main import propagate_architecture_designs

    _board_id, spec_id, card_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        await repo.create(
            "spec", spec_id,
            _warning_payload(architecture_warning_acknowledgement=_ack()), USER_ID,
        )
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(ArchitecturePropagationBlocked):
            await propagate_architecture_designs(
                db, source_parent_type="spec", source_parent_id=spec_id,
                target_parent_type="card", target_parent_id=card_id, actor_id=USER_ID,
                mode="copy",
            )

    # No partial / laundered snapshot: the card received no copied design.
    async with db_factory() as db:
        card_designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card",
                ArchitectureDesign.card_id == card_id,
            )
        )).scalars().all()
    assert card_designs == []
