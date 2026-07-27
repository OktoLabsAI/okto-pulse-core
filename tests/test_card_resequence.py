"""Card C2 — CardService.resequence_columns + move_card position semantics.

Covers the v17 item-7 batch contract (atomic pre-validation, deterministic
(position ASC, id DESC) order, ops applied in list order) and the density
invariant per (board_id, status): active cards dense 0..n-1, archived n..m.
Also proves the authorized CardMove narrowing at the service layer: -1/None
append to the END OF THE ACTIVE RANGE (never past archived cards) and
position < -1 is rejected without persisting anything (QA 6afdc547).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

pytestmark = pytest.mark.asyncio

USER_ID = "resequence-test-user"


async def _seed_board(db_factory):
    """Board + in-progress spec + two columns with an archived straggler each.

    not_started: c0..c3 active (positions 0..3), ca archived at position 9.
    started:     s0, s1 active (positions 0, 1), sa archived at position 7.
    The sparse archived positions replicate the legacy state the resequencer
    must normalize into the n..m tail.
    """
    from sqlalchemy_test_models import Board, Card, CardStatus, Spec, SpecStatus

    token = uuid.uuid4().hex[:8]
    board_id = f"rsq-board-{token}"
    spec_id = f"rsq-spec-{token}"
    ids = {
        "board": board_id,
        "spec": spec_id,
        **{name: f"rsq-{name}-{token}" for name in ("c0", "c1", "c2", "c3", "ca", "s0", "s1", "sa")},
    }

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resequence Board", owner_id=USER_ID, settings={}))
        await db.flush()
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Resequence Spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                skip_decisions_coverage=True,
                evaluations=[],
            )
        )
        await db.flush()
        def _card(name: str, status, position: int, archived: bool = False) -> Card:
            return Card(
                id=ids[name],
                board_id=board_id,
                spec_id=spec_id,
                title=f"Card {name}",
                status=status,
                position=position,
                archived=archived,
                created_by=USER_ID,
            )

        db.add_all(
            [
                _card("c0", CardStatus.NOT_STARTED, 0),
                _card("c1", CardStatus.NOT_STARTED, 1),
                _card("c2", CardStatus.NOT_STARTED, 2),
                _card("c3", CardStatus.NOT_STARTED, 3),
                _card("ca", CardStatus.NOT_STARTED, 9, archived=True),
                _card("s0", CardStatus.STARTED, 0),
                _card("s1", CardStatus.STARTED, 1),
                _card("sa", CardStatus.STARTED, 7, archived=True),
            ]
        )
        await db.commit()
    return ids


async def _snapshot(db_factory, board_id: str) -> dict[str, tuple[str, int, bool]]:
    """id -> (status value, position, archived) for every card on the board."""
    from sqlalchemy_test_models import Card

    async with db_factory() as db:
        rows = (
            (await db.execute(select(Card).where(Card.board_id == board_id)))
            .scalars()
            .all()
        )
        return {
            row.id: (
                row.status.value if hasattr(row.status, "value") else str(row.status),
                row.position,
                bool(row.archived),
            )
            for row in rows
        }


async def test_positional_move_resequences_both_columns(db_factory) -> None:
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        moved = await CardService(db).move_card(
            ids["c2"], USER_ID, CardMove(status=CardStatus.STARTED, position=0)
        )
        assert moved is not None
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    # Target column: c2 inserted at index 0, actives dense, archived at tail.
    assert snap[ids["c2"]] == ("started", 0, False)
    assert snap[ids["s0"]] == ("started", 1, False)
    assert snap[ids["s1"]] == ("started", 2, False)
    assert snap[ids["sa"]] == ("started", 3, True)
    # Source column: gap closed, archived straggler pulled into the dense tail.
    assert snap[ids["c0"]] == ("not_started", 0, False)
    assert snap[ids["c1"]] == ("not_started", 1, False)
    assert snap[ids["c3"]] == ("not_started", 2, False)
    assert snap[ids["ca"]] == ("not_started", 3, True)


async def test_minus_one_and_none_append_to_end_of_active_range(db_factory) -> None:
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        # Legacy defect: max(position)+1 over the whole column would yield 8
        # (after archived sa@7). The contract lands at the end of the ACTIVES.
        await CardService(db).move_card(
            ids["c0"], USER_ID, CardMove(status=CardStatus.STARTED, position=-1)
        )
        await db.commit()
    async with db_factory() as db:
        await CardService(db).move_card(
            ids["c1"], USER_ID, CardMove(status=CardStatus.STARTED, position=None)
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["c0"]] == ("started", 2, False)  # after s0, s1
    assert snap[ids["c1"]] == ("started", 3, False)  # after c0
    assert snap[ids["sa"]] == ("started", 4, True)  # archived tail renumbered
    assert all(position >= 0 for (_, position, _) in snap.values())


async def test_position_below_minus_one_is_rejected_without_writes(db_factory) -> None:
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    before = await _snapshot(db_factory, ids["board"])
    async with db_factory() as db:
        with pytest.raises(ValueError, match="position_out_of_range"):
            await CardService(db).move_card(
                ids["c3"], USER_ID, CardMove(status=CardStatus.NOT_STARTED, position=-2)
            )
    assert await _snapshot(db_factory, ids["board"]) == before


async def test_batch_prevalidation_is_atomic(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    before = await _snapshot(db_factory, ids["board"])

    duplicate = [
        ColumnResequenceOp(ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, None),
        ColumnResequenceOp(ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, None),
    ]
    stale_from = [
        ColumnResequenceOp(ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, None),
        ColumnResequenceOp(ids["c1"], CardStatus.STARTED, CardStatus.STARTED, None),
    ]
    negative_index = [
        ColumnResequenceOp(ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, -1),
    ]
    for ops, message in (
        (duplicate, "resequence_duplicate_card"),
        (stale_from, "resequence_stale_from"),
        (negative_index, "resequence_negative_index"),
    ):
        async with db_factory() as db:
            with pytest.raises(ValueError, match=message):
                await CardService(db).resequence_columns(ids["board"], ops)
        assert await _snapshot(db_factory, ids["board"]) == before

    async with db_factory() as db:
        with pytest.raises(ValueError, match="resequence_wrong_board"):
            await CardService(db).resequence_columns(
                "another-board",
                [ColumnResequenceOp(ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, None)],
            )
    assert await _snapshot(db_factory, ids["board"]) == before


async def test_archive_and_restore_keep_columns_dense(db_factory) -> None:
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import Card, CardStatus

    ids = await _seed_board(db_factory)

    # Archive c1 (as archive_tree does: flip + flush), then renormalize.
    async with db_factory() as db:
        row = await db.get(Card, ids["c1"])
        row.archived = True
        await db.flush()
        await CardService(db).resequence_columns(
            ids["board"], [], extra_columns=(CardStatus.NOT_STARTED,)
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["c0"]] == ("not_started", 0, False)
    assert snap[ids["c2"]] == ("not_started", 1, False)
    assert snap[ids["c3"]] == ("not_started", 2, False)
    # Archived tail n..m, deterministic (position ASC, id DESC) among ca/c1.
    assert {snap[ids["c1"]], snap[ids["ca"]]} == {
        ("not_started", 3, True),
        ("not_started", 4, True),
    }

    # Restore c1 (flip back, position still in the archived range) — the dense
    # rewrite must land it at the END of the active range (ts_b2e972e7).
    async with db_factory() as db:
        row = await db.get(Card, ids["c1"])
        row.archived = False
        await db.flush()
        await CardService(db).resequence_columns(
            ids["board"], [], extra_columns=(CardStatus.NOT_STARTED,)
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["c1"]] == ("not_started", 3, False)  # end of active range
    assert snap[ids["ca"]] == ("not_started", 4, True)


async def test_same_column_reposition_is_dense_and_ordered(db_factory) -> None:
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        await CardService(db).move_card(
            ids["c3"], USER_ID, CardMove(status=CardStatus.NOT_STARTED, position=0)
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["c3"]] == ("not_started", 0, False)
    assert snap[ids["c0"]] == ("not_started", 1, False)
    assert snap[ids["c1"]] == ("not_started", 2, False)
    assert snap[ids["c2"]] == ("not_started", 3, False)
    assert snap[ids["ca"]] == ("not_started", 4, True)
