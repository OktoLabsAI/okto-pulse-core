"""Refinement ambiguity-gate human override contract (SK-A C5/API06)."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.refinements_crud import (
    SetRefinementAmbiguityGateSkipCommand,
    SetRefinementAmbiguityGateSkipUseCase,
)
from okto_pulse.core.models.schemas import RefinementAmbiguityGateSkipUpdate
from okto_pulse.core.services.main import RefinementService
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
)

USER_ID = "refinement-ambiguity-owner"
ACTION = "refinement.ambiguity_gate_skip_updated"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_refinement(
    db_factory,
    *,
    status: RefinementStatus = RefinementStatus.APPROVED,
    version: int = 7,
    archived: bool = False,
) -> tuple[str, str]:
    board_id = _id("board")
    ideation_id = _id("idea")
    refinement_id = _id("ref")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Refinement ambiguity gate",
                owner_id=USER_ID,
                settings={
                    "require_refinement_ambiguity_gate": True,
                    "max_refinement_ambiguity": 3,
                },
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Parent",
                status=IdeationStatus.DONE,
                version=1,
                created_by=USER_ID,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                ideation_id=ideation_id,
                board_id=board_id,
                title="Refinement",
                in_scope=["ambiguity governance"],
                status=status,
                version=version,
                archived=archived,
                created_by=USER_ID,
            )
        )
        await db.commit()
    return board_id, refinement_id


async def _activity_rows(db, refinement_id: str) -> list[ActivityLog]:
    rows = (
        await db.execute(
            select(ActivityLog).where(ActivityLog.action == ACTION)
        )
    ).scalars()
    return [
        row
        for row in rows
        if (row.details or {}).get("refinement_id") == refinement_id
    ]


@pytest.mark.asyncio
async def test_human_skip_is_version_fenced_audited_and_does_not_bump_version(
    db_factory,
) -> None:
    _, refinement_id = await _seed_refinement(db_factory)

    async with db_factory() as db:
        result = await RefinementService(db).set_ambiguity_gate_skip(
            refinement_id,
            USER_ID,
            True,
            reason="  Accepted residual ambiguity for this release.  ",
            expected_refinement_version=7,
            expected_refinement_edition=1,
            source="rest",
            actor_name="Owner",
        )
        await db.commit()

        assert result is not None
        assert result.skipped is True
        assert result.version == 7
        assert result.refinement.version == 7

    async with db_factory() as db:
        refinement = await RefinementService(db).get_refinement(refinement_id)
        assert refinement is not None
        assert refinement.skip_ambiguity_gate is True
        assert refinement.version == 7

        rows = await _activity_rows(db, refinement_id)
        assert len(rows) == 1
        assert rows[0].actor_type == "user"
        assert rows[0].actor_id == USER_ID
        assert rows[0].details == {
            "refinement_id": refinement_id,
            "source": "rest",
            "reason": "Accepted residual ambiguity for this release.",
            "old_value": False,
            "new_value": True,
            "state_changed": True,
            "expected_refinement_version": 7,
            "refinement_version": 7,
            "edition": 1,
        }


@pytest.mark.asyncio
async def test_skip_rejects_non_human_source_before_reading_or_writing(
    db_factory,
) -> None:
    _, refinement_id = await _seed_refinement(db_factory)

    async with db_factory() as db:
        with pytest.raises(ValueError, match="human_actor_required"):
            await RefinementService(db).set_ambiguity_gate_skip(
                refinement_id,
                USER_ID,
                True,
                reason="Agent cannot grant this override.",
                expected_refinement_version=7,
                expected_refinement_edition=1,
                source="mcp",
            )
        assert await _activity_rows(db, refinement_id) == []


@pytest.mark.asyncio
async def test_skip_conflict_and_wrong_lifecycle_have_zero_side_effects(
    db_factory,
) -> None:
    _, approved_id = await _seed_refinement(db_factory)
    _, draft_id = await _seed_refinement(
        db_factory,
        status=RefinementStatus.DRAFT,
    )

    async with db_factory() as db:
        service = RefinementService(db)
        with pytest.raises(ValueError, match="version_conflict"):
            await service.set_ambiguity_gate_skip(
                approved_id,
                USER_ID,
                True,
                reason="Stale modal.",
                expected_refinement_version=6,
                expected_refinement_edition=1,
                source="rest",
            )
        with pytest.raises(
            ValueError,
            match="refinement_ambiguity_skip_status_conflict",
        ):
            await service.set_ambiguity_gate_skip(
                draft_id,
                USER_ID,
                True,
                reason="Wrong lifecycle.",
                expected_refinement_version=7,
                expected_refinement_edition=1,
                source="rest",
            )
        assert await _activity_rows(db, approved_id) == []
        assert await _activity_rows(db, draft_id) == []


def test_skip_request_is_closed_and_normalizes_reason() -> None:
    parsed = RefinementAmbiguityGateSkipUpdate(
        skip_ambiguity_gate=False,
        reason="  ambiguity resolved  ",
        expected_refinement_version=4,
        expected_refinement_edition=1,
    )
    assert parsed.reason == "ambiguity resolved"

    with pytest.raises(ValidationError):
        RefinementAmbiguityGateSkipUpdate(
            skip_ambiguity_gate=True,
            reason=" ",
            expected_refinement_version=4,
            expected_refinement_edition=1,
        )
    with pytest.raises(ValidationError):
        RefinementAmbiguityGateSkipUpdate.model_validate(
            {
                "skip_ambiguity_gate": True,
                "reason": "smuggled edit",
                "expected_refinement_version": 4,
                "expected_refinement_edition": 1,
                "analysis": "must not be writable here",
            }
        )


@pytest.mark.asyncio
async def test_use_case_rejects_mcp_actor_before_touching_unit_of_work() -> None:
    class _UntouchableUnitOfWork:
        @property
        def services(self):
            raise AssertionError("MCP rejection must precede all persistence access")

    with pytest.raises(PermissionDeniedError, match="human_actor_required"):
        await SetRefinementAmbiguityGateSkipUseCase().execute(
            SetRefinementAmbiguityGateSkipCommand(
                "refinement-1",
                skip=True,
                reason="Only a human may authorize this.",
                expected_refinement_version=1,
                expected_refinement_edition=1,
            ),
            actor=ActorContext(
                actor_id="agent-1",
                source="mcp",
                board_id="board-1",
            ),
            uow=_UntouchableUnitOfWork(),  # type: ignore[arg-type]
        )
