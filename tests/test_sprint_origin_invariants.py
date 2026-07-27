"""Fail-closed Sprint lane/origin invariants with relational FKs enabled."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select, text

from okto_pulse.core.models.schemas import SprintCreate, SprintUpdate
from okto_pulse.core.services.main import SprintOperationError, SprintService
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintHistory,
    SprintLaneType,
    SprintStatus,
)

ACTOR_ID = "sprint-origin-invariant-agent"


def _id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
async def origin_graph(db_factory):
    ids = {
        "board": _id(),
        "foreign_board": _id(),
        "spec": _id(),
        "progress_spec": _id(),
        "other_spec": _id(),
        "foreign_spec": _id(),
        "origin": _id(),
        "active_origin": _id(),
        "other_spec_origin": _id(),
        "foreign_origin": _id(),
        "bug": _id(),
        "progress_bug": _id(),
        "other_spec_bug": _id(),
        "foreign_bug": _id(),
        "non_bug": _id(),
        "target": _id(),
    }
    async with db_factory() as db:
        db.add_all(
            [
                Board(id=ids["board"], name="Sprint origin board", owner_id=ACTOR_ID),
                Board(
                    id=ids["foreign_board"],
                    name="Foreign sprint origin board",
                    owner_id=ACTOR_ID,
                ),
            ]
        )
        db.add_all(
            [
                Spec(
                    id=ids["spec"],
                    board_id=ids["board"],
                    title="Done Spec",
                    status=SpecStatus.DONE,
                    created_by=ACTOR_ID,
                ),
                Spec(
                    id=ids["progress_spec"],
                    board_id=ids["board"],
                    title="In-progress Spec",
                    status=SpecStatus.IN_PROGRESS,
                    created_by=ACTOR_ID,
                ),
                Spec(
                    id=ids["other_spec"],
                    board_id=ids["board"],
                    title="Other Spec",
                    status=SpecStatus.DONE,
                    created_by=ACTOR_ID,
                ),
                Spec(
                    id=ids["foreign_spec"],
                    board_id=ids["foreign_board"],
                    title="Foreign Spec",
                    status=SpecStatus.DONE,
                    created_by=ACTOR_ID,
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Sprint(
                    id=ids["origin"],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title="Closed Origin",
                    status=SprintStatus.CLOSED,
                    created_by=ACTOR_ID,
                ),
                Sprint(
                    id=ids["active_origin"],
                    board_id=ids["board"],
                    spec_id=ids["progress_spec"],
                    title="Active Origin",
                    status=SprintStatus.ACTIVE,
                    created_by=ACTOR_ID,
                ),
                Sprint(
                    id=ids["other_spec_origin"],
                    board_id=ids["board"],
                    spec_id=ids["other_spec"],
                    title="Other-spec Origin",
                    status=SprintStatus.CLOSED,
                    created_by=ACTOR_ID,
                ),
                Sprint(
                    id=ids["foreign_origin"],
                    board_id=ids["foreign_board"],
                    spec_id=ids["foreign_spec"],
                    title="Foreign Origin",
                    status=SprintStatus.CLOSED,
                    created_by=ACTOR_ID,
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Card(
                    id=ids["bug"],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title="Valid origin bug without sprint membership",
                    status=CardStatus.NOT_STARTED,
                    card_type=CardType.BUG,
                    created_by=ACTOR_ID,
                ),
                Card(
                    id=ids["progress_bug"],
                    board_id=ids["board"],
                    spec_id=ids["progress_spec"],
                    title="In-progress spec bug",
                    status=CardStatus.NOT_STARTED,
                    card_type=CardType.BUG,
                    created_by=ACTOR_ID,
                ),
                Card(
                    id=ids["other_spec_bug"],
                    board_id=ids["board"],
                    spec_id=ids["other_spec"],
                    title="Other-spec Bug",
                    status=CardStatus.NOT_STARTED,
                    card_type=CardType.BUG,
                    created_by=ACTOR_ID,
                ),
                Card(
                    id=ids["foreign_bug"],
                    board_id=ids["foreign_board"],
                    spec_id=ids["foreign_spec"],
                    title="Foreign Bug",
                    status=CardStatus.NOT_STARTED,
                    card_type=CardType.BUG,
                    created_by=ACTOR_ID,
                ),
                Card(
                    id=ids["non_bug"],
                    board_id=ids["board"],
                    spec_id=ids["spec"],
                    title="Not a Bug",
                    status=CardStatus.NOT_STARTED,
                    card_type=CardType.NORMAL,
                    created_by=ACTOR_ID,
                ),
            ]
        )
        await db.flush()
        db.add(
            Sprint(
                id=ids["target"],
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Draft Target",
                status=SprintStatus.DRAFT,
                lane_type=SprintLaneType.NORMAL,
                created_by=ACTOR_ID,
            )
        )
        await db.commit()
    return ids


async def _board_counts(db_factory, board_id: str) -> tuple[int, int, int]:
    async with db_factory() as db:
        sprint_count = int(
            await db.scalar(
                select(func.count()).select_from(Sprint).where(Sprint.board_id == board_id)
            )
            or 0
        )
        history_count = int(
            await db.scalar(
                select(func.count())
                .select_from(SprintHistory)
                .join(Sprint, Sprint.id == SprintHistory.sprint_id)
                .where(Sprint.board_id == board_id)
            )
            or 0
        )
        activity_count = int(
            await db.scalar(
                select(func.count())
                .select_from(ActivityLog)
                .where(ActivityLog.board_id == board_id)
            )
            or 0
        )
        return sprint_count, history_count, activity_count


async def _snapshot(db_factory, sprint_id: str) -> tuple[object, ...]:
    async with db_factory() as db:
        sprint = await db.get(Sprint, sprint_id)
        assert sprint is not None
        return (
            sprint.title,
            sprint.lane_type,
            sprint.origin_sprint_id,
            sprint.origin_bug_id,
            sprint.version,
        )


def _invalid_create(case: str, ids: dict[str, str]) -> tuple[SprintCreate, str]:
    common = {"title": f"invalid-{case}-{_id()}", "spec_id": ids["spec"]}
    cases: dict[str, tuple[SprintCreate, str]] = {
        "normal_origin_sprint": (
            SprintCreate(**common, origin_sprint_id=ids["origin"]),
            "normal_lane_lineage_forbidden",
        ),
        "normal_origin_bug": (
            SprintCreate(**common, origin_bug_id=ids["bug"]),
            "normal_lane_lineage_forbidden",
        ),
        "normal_empty_origin": (
            SprintCreate(**common, origin_bug_id=""),
            "normal_lane_lineage_forbidden",
        ),
        "hotfix_missing_bug": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["origin"],
            ),
            "hotfix_lineage_required",
        ),
        "hotfix_unknown_bug": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=_id(),
            ),
            "origin_bug_not_found",
        ),
        "hotfix_cross_board_bug": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["foreign_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_cross_spec_bug": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["other_spec_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_non_bug": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["non_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_unknown_sprint": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=_id(),
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_empty_sprint": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id="",
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_cross_board_sprint": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["foreign_origin"],
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_cross_spec_sprint": (
            SprintCreate(
                **common,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["other_spec_origin"],
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_ineligible": (
            SprintCreate(
                title=common["title"],
                spec_id=ids["progress_spec"],
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["active_origin"],
                origin_bug_id=ids["progress_bug"],
            ),
            "hotfix_lane_not_eligible",
        ),
    }
    return cases[case]


@pytest.mark.parametrize(
    "case",
    [
        "normal_origin_sprint",
        "normal_origin_bug",
        "normal_empty_origin",
        "hotfix_missing_bug",
        "hotfix_unknown_bug",
        "hotfix_cross_board_bug",
        "hotfix_cross_spec_bug",
        "hotfix_non_bug",
        "hotfix_unknown_sprint",
        "hotfix_empty_sprint",
        "hotfix_cross_board_sprint",
        "hotfix_cross_spec_sprint",
        "hotfix_ineligible",
    ],
)
@pytest.mark.asyncio
async def test_create_origin_preflight_is_governed_and_zero_write(
    db_factory,
    origin_graph,
    case: str,
):
    data, expected_code = _invalid_create(case, origin_graph)
    before = await _board_counts(db_factory, origin_graph["board"])

    async with db_factory() as db:
        assert await db.scalar(text("PRAGMA foreign_keys")) == 1
        with pytest.raises(SprintOperationError) as exc:
            await SprintService(db).create_sprint(
                origin_graph["board"], ACTOR_ID, data, skip_ownership_check=True
            )

    assert exc.value.code == expected_code
    assert await _board_counts(db_factory, origin_graph["board"]) == before
    async with db_factory() as db:
        assert (
            await db.scalar(select(func.count()).where(Sprint.title == data.title)) or 0
        ) == 0


def _invalid_update(case: str, ids: dict[str, str]) -> tuple[SprintUpdate, str]:
    cases: dict[str, tuple[SprintUpdate, str]] = {
        "normal_origin_sprint": (
            SprintUpdate(origin_sprint_id=ids["origin"]),
            "normal_lane_lineage_forbidden",
        ),
        "normal_origin_bug": (
            SprintUpdate(origin_bug_id=ids["bug"]),
            "normal_lane_lineage_forbidden",
        ),
        "hotfix_missing_bug": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["origin"],
            ),
            "hotfix_lineage_required",
        ),
        "hotfix_unknown_bug": (
            SprintUpdate(lane_type=SprintLaneType.HOTFIX, origin_bug_id=_id()),
            "origin_bug_not_found",
        ),
        "hotfix_cross_board_bug": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["foreign_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_cross_spec_bug": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["other_spec_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_non_bug": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=ids["non_bug"],
            ),
            "origin_bug_not_found",
        ),
        "hotfix_unknown_sprint": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=_id(),
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_cross_board_sprint": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["foreign_origin"],
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_cross_spec_sprint": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["other_spec_origin"],
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "hotfix_self_origin": (
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=ids["target"],
                origin_bug_id=ids["bug"],
            ),
            "origin_sprint_not_found",
        ),
        "null_lane": (SprintUpdate(lane_type=None), "invalid_lane_type"),
    }
    return cases[case]


@pytest.mark.parametrize(
    "case",
    [
        "normal_origin_sprint",
        "normal_origin_bug",
        "hotfix_missing_bug",
        "hotfix_unknown_bug",
        "hotfix_cross_board_bug",
        "hotfix_cross_spec_bug",
        "hotfix_non_bug",
        "hotfix_unknown_sprint",
        "hotfix_cross_board_sprint",
        "hotfix_cross_spec_sprint",
        "hotfix_self_origin",
        "null_lane",
    ],
)
@pytest.mark.asyncio
async def test_update_validates_resulting_lineage_before_any_mutation(
    db_factory,
    origin_graph,
    case: str,
):
    data, expected_code = _invalid_update(case, origin_graph)
    target_id = origin_graph["target"]
    before = await _snapshot(db_factory, target_id)
    counts_before = await _board_counts(db_factory, origin_graph["board"])

    async with db_factory() as db:
        assert await db.scalar(text("PRAGMA foreign_keys")) == 1
        with pytest.raises(SprintOperationError) as exc:
            await SprintService(db).update_sprint(target_id, ACTOR_ID, data)

    assert exc.value.code == expected_code
    assert await _snapshot(db_factory, target_id) == before
    assert await _board_counts(db_factory, origin_graph["board"]) == counts_before


@pytest.mark.asyncio
async def test_valid_normal_and_hotfix_create_paths_remain_supported(
    db_factory,
    origin_graph,
):
    async with db_factory() as db:
        service = SprintService(db)
        normal = await service.create_sprint(
            origin_graph["board"],
            ACTOR_ID,
            SprintCreate(title="Valid Normal", spec_id=origin_graph["spec"]),
            skip_ownership_check=True,
        )
        bug_only = await service.create_sprint(
            origin_graph["board"],
            ACTOR_ID,
            SprintCreate(
                title="Valid Bug-only Hotfix",
                spec_id=origin_graph["spec"],
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=origin_graph["bug"],
            ),
            skip_ownership_check=True,
        )
        full = await service.create_sprint(
            origin_graph["board"],
            ACTOR_ID,
            SprintCreate(
                title="Valid Full-lineage Hotfix",
                spec_id=origin_graph["spec"],
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=origin_graph["origin"],
                origin_bug_id=origin_graph["bug"],
            ),
            skip_ownership_check=True,
        )
        # The executable contract permits repeated lineage; no synthetic unique-pair
        # restriction is introduced by the preflight.
        repeated = await service.create_sprint(
            origin_graph["board"],
            ACTOR_ID,
            SprintCreate(
                title="Valid Repeated-lineage Hotfix",
                spec_id=origin_graph["spec"],
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id=origin_graph["origin"],
                origin_bug_id=origin_graph["bug"],
            ),
            skip_ownership_check=True,
        )
        await db.commit()

    assert normal is not None and normal.lane_type == SprintLaneType.NORMAL
    assert bug_only is not None and bug_only.origin_sprint_id is None
    assert full is not None and full.origin_sprint_id == origin_graph["origin"]
    assert repeated is not None and repeated.origin_bug_id == origin_graph["bug"]


@pytest.mark.asyncio
async def test_valid_partial_updates_are_checked_as_complete_resulting_state(
    db_factory,
    origin_graph,
):
    target_id = origin_graph["target"]
    async with db_factory() as db:
        service = SprintService(db)
        hotfix = await service.update_sprint(
            target_id,
            ACTOR_ID,
            SprintUpdate(
                lane_type=SprintLaneType.HOTFIX,
                origin_bug_id=origin_graph["bug"],
            ),
        )
        assert hotfix is not None
        assert hotfix.lane_type == SprintLaneType.HOTFIX
        assert hotfix.origin_sprint_id is None

        with_origin = await service.update_sprint(
            target_id,
            ACTOR_ID,
            SprintUpdate(origin_sprint_id=origin_graph["origin"]),
        )
        assert with_origin is not None
        assert with_origin.origin_bug_id == origin_graph["bug"]
        assert with_origin.origin_sprint_id == origin_graph["origin"]

        normal = await service.update_sprint(
            target_id,
            ACTOR_ID,
            SprintUpdate(lane_type=SprintLaneType.NORMAL),
        )

    assert normal is not None
    assert normal.lane_type == SprintLaneType.NORMAL
    assert normal.origin_sprint_id is None
    assert normal.origin_bug_id is None
