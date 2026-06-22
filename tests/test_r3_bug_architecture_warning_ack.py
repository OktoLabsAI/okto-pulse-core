"""R3 bug eded2f0e (test card eb314eef) — architecture warning-ack regression.

Option B fix: an INTERNAL snapshot copy (copy_from_parent / auto-propagation
re-sync) of an already-acknowledged source architecture design carries a system
acknowledgement, so legitimate propagation is not blocked. This must NOT weaken
the authoring gate.

Negative proof: a DIRECT repo.create / repo.update of a warning-bearing design
WITHOUT an acknowledgement still raises ArchitectureWarningAcknowledgementRequired.
Positive proof: the internal snapshot copy succeeds WITHOUT an interactive ack.
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
# POSITIVE PROOF — internal snapshot copy needs no interactive ack
# ===========================================================================


@pytest.mark.asyncio
async def test_raw_copy_from_parent_still_requires_explicit_ack(db_factory):
    """The copy gate itself is intact: copy_from_parent of a warning-bearing source
    WITHOUT an acknowledgement still raises (copies need a copy-scoped ack — the
    contract in test_copy_warning_design_requires_copy_scoped_acknowledgement)."""
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
        with pytest.raises(ArchitectureWarningAcknowledgementRequired):
            await service.copy_from_parent(
                source_parent_type="spec", source_parent_id=spec_id,
                target_parent_type="card", target_parent_id=card_id, actor_id=USER_ID,
            )


@pytest.mark.asyncio
async def test_internal_snapshot_propagation_does_not_require_interactive_ack(db_factory):
    """The FIX: the SDLC internal-snapshot path (propagate_architecture_designs)
    copies a warning-bearing, already-acknowledged source WITHOUT a human ack — the
    system supplies the copy-scoped acknowledgement, so legitimate propagation is
    not blocked."""
    from okto_pulse.core.services.main import propagate_architecture_designs

    _board_id, spec_id, card_id = await _seed(db_factory)
    async with db_factory() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.create(
            "spec", spec_id,
            _warning_payload(architecture_warning_acknowledgement=_ack()), USER_ID,
        )
        await db.commit()

    async with db_factory() as db:
        copied = await propagate_architecture_designs(
            db, source_parent_type="spec", source_parent_id=spec_id,
            target_parent_type="card", target_parent_id=card_id, actor_id=USER_ID,
            mode="copy",
        )
        await db.commit()
        assert len(copied) == 1
        card_designs = (await db.execute(
            select(ArchitectureDesign).where(
                ArchitectureDesign.parent_type == "card",
                ArchitectureDesign.card_id == card_id,
            )
        )).scalars().all()
    assert any(getattr(d, "source_design_id", None) == design.id for d in card_designs)
