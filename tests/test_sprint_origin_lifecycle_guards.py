from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from okto_pulse.core.models.schemas import CardUpdate, SpecMove
from okto_pulse.core.services.main import CardService, SpecService
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardType,
    Spec,
    SpecHistory,
    SpecStatus,
    Sprint,
    SprintHistory,
    SprintLaneType,
    SprintStatus,
)


ACTOR = "sprint-origin-lifecycle-agent"


def _id(label: str) -> str:
    return f"{label}-{uuid.uuid4().hex[:12]}"


async def _seed_lineage(
    db_factory,
    *,
    spec_status: SpecStatus,
    with_origin: bool = True,
) -> dict[str, str]:
    ids = {
        "board": _id("board"),
        "spec": _id("spec"),
        "other_spec": _id("other-spec"),
        "bug": _id("bug"),
        "origin": _id("origin"),
        "hotfix": _id("hotfix"),
    }
    async with db_factory() as db:
        db.add(Board(id=ids["board"], name="Lineage board", owner_id=ACTOR))
        await db.flush()
        db.add_all(
            [
                Spec(
                    id=ids["spec"],
                    board_id=ids["board"],
                    title="Lineage spec",
                    status=spec_status,
                    created_by=ACTOR,
                ),
                Spec(
                    id=ids["other_spec"],
                    board_id=ids["board"],
                    title="Other spec",
                    status=SpecStatus.DONE,
                    created_by=ACTOR,
                ),
            ]
        )
        await db.flush()
        db.add(
            Card(
                id=ids["bug"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Origin bug",
                card_type=CardType.BUG,
                created_by=ACTOR,
            )
        )
        db.add(
            Sprint(
                id=ids["origin"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Closed origin",
                status=SprintStatus.CLOSED,
                lane_type=SprintLaneType.NORMAL,
                created_by=ACTOR,
            )
        )
        await db.flush()
        db.add(
            Sprint(
                id=ids["hotfix"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Dependent hotfix",
                status=SprintStatus.DRAFT,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["origin"] if with_origin else None,
                origin_bug_id=ids["bug"],
                created_by=ACTOR,
            )
        )
        await db.commit()
    return ids


async def _audit_counts(db_factory, ids: dict[str, str]) -> tuple[int, int, int]:
    async with db_factory() as db:
        activities = int(
            await db.scalar(
                select(func.count())
                .select_from(ActivityLog)
                .where(ActivityLog.board_id == ids["board"])
            )
            or 0
        )
        sprint_history = int(
            await db.scalar(
                select(func.count())
                .select_from(SprintHistory)
                .where(
                    SprintHistory.sprint_id.in_([ids["origin"], ids["hotfix"]])
                )
            )
            or 0
        )
        spec_history = int(
            await db.scalar(
                select(func.count())
                .select_from(SpecHistory)
                .where(SpecHistory.spec_id == ids["spec"])
            )
            or 0
        )
        return activities, sprint_history, spec_history










@pytest.mark.asyncio
async def test_reopening_done_spec_preserves_retired_lane_history(db_factory):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.DONE, with_origin=False)
    before = await _audit_counts(db_factory, ids)
    async with db_factory() as db:
        spec = await SpecService(db).move_spec(ids["spec"], ACTOR, SpecMove(status=SpecStatus.DRAFT))
        assert spec.status is SpecStatus.DRAFT
        assert spec.edition == 2
        lane = await db.get(Sprint, ids["hotfix"])
        assert lane.status is SprintStatus.DRAFT
        assert lane.origin_sprint_id is None
        assert lane.origin_bug_id == ids["bug"]
        await db.commit()
    after = await _audit_counts(db_factory, ids)
    assert after[1] == before[1]  # No new Sprint history is manufactured.
    assert after[2] > before[2]  # The authorized Spec revision is recorded.


@pytest.mark.asyncio
async def test_reparenting_bug_preserves_retired_lane_history(db_factory):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.DONE)
    before = await _audit_counts(db_factory, ids)
    async with db_factory() as db:
        bug = await CardService(db).update_card(
            ids["bug"], ACTOR, CardUpdate(spec_id=ids["other_spec"])
        )
        assert bug.spec_id == ids["other_spec"]
        lane = await db.get(Sprint, ids["hotfix"])
        assert lane.spec_id == ids["spec"]
        assert lane.origin_bug_id == ids["bug"]
        await db.commit()
    after = await _audit_counts(db_factory, ids)
    assert after[0] > before[0]  # Actual Card mutation remains audited.
    assert after[1:] == before[1:]  # No Sprint/Spec history is manufactured.
