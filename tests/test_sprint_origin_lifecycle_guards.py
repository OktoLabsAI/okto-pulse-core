from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select, text

from okto_pulse.core.models.schemas import CardUpdate, SpecMove, SprintMove, SprintUpdate
from okto_pulse.core.services.main import (
    CardOperationError,
    CardService,
    SpecService,
    SprintOperationError,
    SprintService,
)
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


async def _set_foreign_keys(db_factory, enabled: bool) -> None:
    async with db_factory() as db:
        await db.execute(text(f"PRAGMA foreign_keys={'ON' if enabled else 'OFF'}"))
        assert await db.scalar(text("PRAGMA foreign_keys")) == int(enabled)
        await db.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_keys_enabled", [True, False], ids=["fk-on", "fk-off"])
async def test_delete_origin_explicitly_nulls_done_dependents_with_fk_parity(
    db_factory, foreign_keys_enabled
):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.DONE)
    await _set_foreign_keys(db_factory, foreign_keys_enabled)
    try:
        async with db_factory() as db:
            assert await SprintService(db).delete_sprint(ids["origin"], ACTOR) is True

        async with db_factory() as db:
            assert await db.get(Sprint, ids["origin"]) is None
            dependent = await db.get(Sprint, ids["hotfix"])
            assert dependent is not None
            assert dependent.origin_sprint_id is None
    finally:
        await _set_foreign_keys(db_factory, True)


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_keys_enabled", [True, False], ids=["fk-on", "fk-off"])
async def test_delete_origin_blocks_ineligible_dependents_with_zero_audit(
    db_factory, foreign_keys_enabled
):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.IN_PROGRESS)
    await _set_foreign_keys(db_factory, foreign_keys_enabled)
    before = await _audit_counts(db_factory, ids)
    try:
        async with db_factory() as db:
            with pytest.raises(SprintOperationError) as exc:
                await SprintService(db).delete_sprint(ids["origin"], ACTOR)
        assert exc.value.code == "origin_sprint_delete_conflict"

        async with db_factory() as db:
            assert await db.get(Sprint, ids["origin"]) is not None
            dependent = await db.get(Sprint, ids["hotfix"])
            assert dependent is not None
            assert dependent.origin_sprint_id == ids["origin"]
        assert await _audit_counts(db_factory, ids) == before
    finally:
        await _set_foreign_keys(db_factory, True)


@pytest.mark.asyncio
async def test_reopening_closed_origin_is_blocked_before_mutation_or_audit(db_factory):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.IN_PROGRESS)
    before = await _audit_counts(db_factory, ids)

    async with db_factory() as db:
        with pytest.raises(SprintOperationError) as exc:
            await SprintService(db).move_sprint(
                ids["origin"], ACTOR, SprintMove(status=SprintStatus.DRAFT)
            )

    assert exc.value.code == "origin_sprint_reopen_conflict"
    async with db_factory() as db:
        origin = await db.get(Sprint, ids["origin"])
        assert origin is not None and origin.status == SprintStatus.CLOSED
    assert await _audit_counts(db_factory, ids) == before


@pytest.mark.asyncio
async def test_reopening_done_spec_with_bug_only_hotfix_is_zero_write_conflict(db_factory):
    ids = await _seed_lineage(
        db_factory, spec_status=SpecStatus.DONE, with_origin=False
    )
    before = await _audit_counts(db_factory, ids)

    async with db_factory() as db:
        with pytest.raises(SprintOperationError) as exc:
            await SpecService(db).move_spec(
                ids["spec"], ACTOR, SpecMove(status=SpecStatus.DRAFT)
            )

    assert exc.value.code == "hotfix_spec_reopen_conflict"
    async with db_factory() as db:
        spec = await db.get(Spec, ids["spec"])
        assert spec is not None and spec.status == SpecStatus.DONE
    assert await _audit_counts(db_factory, ids) == before


@pytest.mark.asyncio
async def test_reparenting_origin_bug_across_specs_is_zero_write_conflict(db_factory):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.DONE)
    before = await _audit_counts(db_factory, ids)

    async with db_factory() as db:
        with pytest.raises(CardOperationError) as exc:
            await CardService(db).update_card(
                ids["bug"], ACTOR, CardUpdate(spec_id=ids["other_spec"])
            )

    assert exc.value.code == "hotfix_origin_bug_reparent_conflict"
    async with db_factory() as db:
        bug = await db.get(Card, ids["bug"])
        assert bug is not None and bug.spec_id == ids["spec"]
    assert await _audit_counts(db_factory, ids) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["update", "move"])
async def test_invalid_legacy_lineage_is_revalidated_on_update_and_move(
    db_factory, operation
):
    ids = await _seed_lineage(db_factory, spec_status=SpecStatus.DONE)
    invalid_id = _id("legacy-invalid")
    async with db_factory() as db:
        db.add(
            Sprint(
                id=invalid_id,
                board_id=ids["board"],
                spec_id=ids["spec"],
                title="Legacy invalid normal lane",
                status=SprintStatus.DRAFT,
                lane_type=SprintLaneType.NORMAL,
                origin_bug_id=ids["bug"],
                created_by=ACTOR,
            )
        )
        await db.commit()
    before = await _audit_counts(db_factory, ids)

    async with db_factory() as db:
        with pytest.raises(SprintOperationError) as exc:
            if operation == "update":
                await SprintService(db).update_sprint(
                    invalid_id, ACTOR, SprintUpdate(title="must not persist")
                )
            else:
                await SprintService(db).move_sprint(
                    invalid_id, ACTOR, SprintMove(status=SprintStatus.ACTIVE)
                )

    assert exc.value.code == "normal_lane_lineage_forbidden"
    async with db_factory() as db:
        invalid = await db.get(Sprint, invalid_id)
        assert invalid is not None
        assert invalid.title == "Legacy invalid normal lane"
        assert invalid.status == SprintStatus.DRAFT
    assert await _audit_counts(db_factory, ids) == before
