"""Card 6e5d18cf — enforce Max ambiguity gate on the ideation done transition.

Covers TR6/TR8/TR9, FR6-FR10, BR2/BR4/BR6 and scenarios ts_e07509da,
ts_466eea4b, ts_2df24199, ts_4bd19944 (gate scope, precedence before
ResourceGate, missing/above-threshold errors, skip bypass, disabled no-op).
"""

from __future__ import annotations

import uuid

import pytest

from sqlalchemy_test_models import Board, Ideation, IdeationStatus
from okto_pulse.core.models.schemas import IdeationMove
from okto_pulse.core.services.main import AmbiguityGateError, IdeationService
from okto_pulse.core.services.resource_gate import (
    ResourceGateService,
    ResourceGateViolation,
)

ACTOR = "ambiguity-gate-actor"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed(
    db,
    *,
    gate_enabled: bool,
    threshold: int = 3,
    ambiguity=None,
    skip: bool = False,
    status: IdeationStatus = IdeationStatus.EVALUATING,
    settings_override=None,
) -> tuple[str, str]:
    board_id = _id("board")
    ideation_id = _id("idea")
    if settings_override is not None:
        settings = settings_override
    else:
        settings = {
            "require_ideation_ambiguity_gate": gate_enabled,
            "max_ideation_ambiguity": threshold,
        }
    scope = None if ambiguity is None else {"ambiguity": ambiguity}
    db.add(Board(id=board_id, name="Gate", owner_id=ACTOR, settings=settings))
    db.add(
        Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Gate ideation",
            created_by=ACTOR,
            status=status,
            scope_assessment=scope,
            skip_ambiguity_gate=skip,
        )
    )
    await db.flush()
    return board_id, ideation_id


async def _satisfy_resource_gate(db, board_id: str, ideation_id: str) -> None:
    """Mark every ideation resource type N/A so ResourceGate allows done."""
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            board_id,
            "ideation",
            ideation_id,
            resource_type,
            ACTOR,
            justification=f"{resource_type} not applicable in this test.",
            source_channel="ui",
        )


# ---------------------------------------------------------------------------
# Missing / invalid ambiguity (TR8, FR8, AC6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_ambiguity_blocks_done_with_actionable_error(db_factory):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, ambiguity=None)
        with pytest.raises(
            AmbiguityGateError,
            match="ambiguity has not been evaluated",
        ):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["high", 0, 7, 3.5, True])
async def test_non_numeric_or_out_of_range_ambiguity_treated_as_missing(db_factory, bad):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, ambiguity=bad)
        with pytest.raises(AmbiguityGateError, match="ambiguity has not been evaluated"):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


# ---------------------------------------------------------------------------
# Above / equal threshold (TR9, FR9, AC7, AC8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_above_threshold_blocks_with_score_and_max_in_error(db_factory):
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, threshold=3, ambiguity=4)
        with pytest.raises(
            AmbiguityGateError,
            match="ambiguity score 4 exceeds configured max 3",
        ):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


@pytest.mark.asyncio
async def test_equal_threshold_passes_gate_and_reaches_done(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db, gate_enabled=True, threshold=3, ambiguity=3
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


# ---------------------------------------------------------------------------
# Precedence: ambiguity gate runs BEFORE ResourceGate (BR4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ambiguity_error_precedes_resource_gate(db_factory):
    # Bare ideation (ResourceGate WOULD block) + gate enabled + missing score.
    # The ambiguity error must surface, proving it runs first.
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, ambiguity=None)
        with pytest.raises(AmbiguityGateError):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


# ---------------------------------------------------------------------------
# Skip bypasses ONLY the Max ambiguity gate (FR10, BR3, AC9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_bypasses_only_ambiguity_gate_resource_gate_still_runs(db_factory):
    # skip=True with missing ambiguity: ambiguity gate is bypassed, but the
    # bare ideation still trips ResourceGate (not AmbiguityGateError).
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, ambiguity=None, skip=True)
        with pytest.raises(ResourceGateViolation):
            await IdeationService(db).move_ideation(
                ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
            )


@pytest.mark.asyncio
async def test_skip_lets_done_complete_when_resources_satisfied(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db, gate_enabled=True, ambiguity=5, threshold=3, skip=True
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


# ---------------------------------------------------------------------------
# Gate only on evaluating -> done (FR6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gate_does_not_fire_on_non_done_transitions(db_factory):
    # evaluating -> approved with gate enabled + missing ambiguity must succeed.
    async with db_factory() as db:
        _, ideation_id = await _seed(db, gate_enabled=True, ambiguity=None)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.APPROVED)
        )
        assert moved.status == IdeationStatus.APPROVED


# ---------------------------------------------------------------------------
# Disabled gate + legacy defaults are no-ops (FR7, FR13, BR1, BR6, AC5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_gate_does_not_block_done(db_factory):
    async with db_factory() as db:
        board_id, ideation_id = await _seed(db, gate_enabled=False, ambiguity=None)
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE


def test_ambiguity_gate_error_is_valueerror_for_mcp_surfacing():
    # okto_pulse_move_ideation surfaces gate failures through its existing
    # `except ValueError -> {"error": str(e)}`; the subclass relationship is
    # the contract that keeps the MCP detail equivalent to REST (TR9).
    assert issubclass(AmbiguityGateError, ValueError)


@pytest.mark.asyncio
async def test_legacy_board_without_settings_defaults_gate_off(db_factory):
    # No gate settings at all -> _resolve_ideation_ambiguity_config defaults to
    # disabled; high ambiguity does not block (only ResourceGate governs).
    async with db_factory() as db:
        board_id, ideation_id = await _seed(
            db, gate_enabled=False, ambiguity=5, settings_override={}
        )
        await _satisfy_resource_gate(db, board_id, ideation_id)
        moved = await IdeationService(db).move_ideation(
            ideation_id, ACTOR, IdeationMove(status=IdeationStatus.DONE)
        )
        assert moved.status == IdeationStatus.DONE
