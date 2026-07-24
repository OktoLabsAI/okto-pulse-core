"""Card C2 (round 2) — full ColumnResequenceOp contract (matriz v13, item 5).

Adversarial coverage demanded by the codex REJECT (val_82566cd5): anchor and
placement selectors, same-insertion-point preorder stability, invalid-anchor
variants rejecting the WHOLE batch atomically (including a missing card mixed
with a valid op), a real (position) collision resolved by the deterministic
``id DESC`` tie-break, and the ArchiveService end-to-end archive/restore flow
— including the exact legacy repro: an archived card carrying ``position=-1``
must restore to the END of the active range, never to the front.
"""

from __future__ import annotations

import pytest

from test_card_resequence import _seed_board, _snapshot

pytestmark = pytest.mark.asyncio


async def test_anchor_and_placement_selectors(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        service = CardService(db)
        await service.resequence_columns(
            ids["board"],
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    before_id=ids["s1"],
                )
            ],
        )
        await service.resequence_columns(
            ids["board"],
            [
                ColumnResequenceOp(
                    ids["c1"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    after_id=ids["s0"],
                )
            ],
        )
        await service.resequence_columns(
            ids["board"],
            [
                ColumnResequenceOp(
                    ids["c2"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    placement="start",
                )
            ],
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    # Final order: c2 (start), s0, c1 (after s0), c0 (before s1), s1; sa tail.
    assert snap[ids["c2"]] == ("started", 0, False)
    assert snap[ids["s0"]] == ("started", 1, False)
    assert snap[ids["c1"]] == ("started", 2, False)
    assert snap[ids["c0"]] == ("started", 3, False)
    assert snap[ids["s1"]] == ("started", 4, False)
    assert snap[ids["sa"]] == ("started", 5, True)


async def test_same_insertion_point_preserves_batch_order(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        await CardService(db).resequence_columns(
            ids["board"],
            [
                ColumnResequenceOp(
                    ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, 0
                ),
                ColumnResequenceOp(
                    ids["c1"], CardStatus.NOT_STARTED, CardStatus.STARTED, 0
                ),
                ColumnResequenceOp(
                    ids["c2"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    after_id=ids["s0"],
                ),
                ColumnResequenceOp(
                    ids["c3"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    after_id=ids["s0"],
                ),
            ],
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    # c0 then c1 at the head (batch order preserved at the equal index),
    # then s0, then c2 then c3 (batch order after the same anchor), then s1.
    assert snap[ids["c0"]] == ("started", 0, False)
    assert snap[ids["c1"]] == ("started", 1, False)
    assert snap[ids["s0"]] == ("started", 2, False)
    assert snap[ids["c2"]] == ("started", 3, False)
    assert snap[ids["c3"]] == ("started", 4, False)
    assert snap[ids["s1"]] == ("started", 5, False)
    assert snap[ids["sa"]] == ("started", 6, True)


async def test_mixed_start_and_index_zero_preserve_batch_order(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        # Round-3 REJECT repro: placement="start" followed by target_index=0
        # resolve to the same physical index — the uniform step-past rule must
        # keep batch order across DIFFERENT selector kinds.
        await CardService(db).resequence_columns(
            ids["board"],
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    placement="start",
                ),
                ColumnResequenceOp(
                    ids["c1"], CardStatus.NOT_STARTED, CardStatus.STARTED, 0
                ),
            ],
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["c0"]] == ("started", 0, False)  # batch order kept
    assert snap[ids["c1"]] == ("started", 1, False)
    assert snap[ids["s0"]] == ("started", 2, False)
    assert snap[ids["s1"]] == ("started", 3, False)


async def test_empty_anchor_is_structurally_rejected(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    before = await _snapshot(db_factory, ids["board"])
    # Round-3 REJECT repro: an empty-string anchor must fail STRUCTURAL
    # pre-validation (never reach apply), for both anchor kinds.
    for kwargs in ({"before_id": ""}, {"after_id": "   "}):
        async with db_factory() as db:
            with pytest.raises(ValueError, match="resequence_anchor_invalid"):
                await CardService(db).resequence_columns(
                    ids["board"],
                    [
                        ColumnResequenceOp(
                            ids["c0"],
                            CardStatus.NOT_STARTED,
                            CardStatus.STARTED,
                            **kwargs,
                        )
                    ],
                )
        assert await _snapshot(db_factory, ids["board"]) == before


async def test_move_card_forwards_anchor_selectors(db_factory) -> None:
    from okto_pulse.core.models.schemas import CardMove
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        # CardMove now exposes the anchors; move_card forwards them.
        await CardService(db).move_card(
            ids["c0"],
            "resequence-test-user",
            CardMove(status=CardStatus.STARTED, before_id=ids["s1"]),
        )
        await db.commit()
    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["s0"]] == ("started", 0, False)
    assert snap[ids["c0"]] == ("started", 1, False)  # immediately before s1
    assert snap[ids["s1"]] == ("started", 2, False)

    # Conflicting selectors are now rejected at PARSE time by the CardMove
    # model validator — preflight BEFORE any service effect (round-3 fix):
    # position=-1 counts as explicit positional intent too.
    from pydantic import ValidationError

    before = await _snapshot(db_factory, ids["board"])
    for kwargs in (
        {"position": 0, "before_id": ids["s1"]},
        {"position": -1, "before_id": ids["s1"]},
        {"placement": "bogus"},
        {"before_id": "   "},
    ):
        with pytest.raises(ValidationError, match="card_move_"):
            CardMove(status=CardStatus.STARTED, **kwargs)
    assert await _snapshot(db_factory, ids["board"]) == before


async def test_invalid_anchor_variants_reject_whole_batch(db_factory) -> None:
    from okto_pulse.core.services.main import CardService, ColumnResequenceOp
    from sqlalchemy_test_models import CardStatus

    ids = await _seed_board(db_factory)
    before = await _snapshot(db_factory, ids["board"])

    cases = [
        (
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    before_id=ids["sa"],  # archived anchor
                )
            ],
            "resequence_anchor_invalid",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    after_id=ids["c1"],  # anchor from another column
                )
            ],
            "resequence_anchor_invalid",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    before_id="rsq-missing-anchor",
                )
            ],
            "resequence_anchor_invalid",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.NOT_STARTED,
                    before_id=ids["c0"],  # anchor is the card itself
                )
            ],
            "resequence_anchor_self",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["s0"],
                    CardStatus.STARTED,
                    CardStatus.STARTED,
                    placement="end",
                ),
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    before_id=ids["s0"],  # anchor moved in the same batch
                ),
            ],
            "resequence_anchor_in_batch",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["c0"],
                    CardStatus.NOT_STARTED,
                    CardStatus.STARTED,
                    1,
                    before_id=ids["s0"],  # two selectors at once
                )
            ],
            "resequence_conflicting_placement",
        ),
        (
            [
                ColumnResequenceOp(
                    ids["c0"], CardStatus.NOT_STARTED, CardStatus.STARTED, 0
                ),
                ColumnResequenceOp(
                    "rsq-missing-card", CardStatus.NOT_STARTED, CardStatus.STARTED
                ),  # atomicity: valid op + missing card => nothing applies
            ],
            "resequence_card_not_found",
        ),
    ]
    for ops, message in cases:
        async with db_factory() as db:
            with pytest.raises(ValueError, match=message):
                await CardService(db).resequence_columns(ids["board"], ops)
        assert await _snapshot(db_factory, ids["board"]) == before


async def test_tree_ops_follow_dfs_preorder_with_roots_siblings_and_chain(
    db_factory,
) -> None:
    from okto_pulse.core.services.main import ArchiveService
    from sqlalchemy_test_models import Card, CardStatus, CardType

    ids = await _seed_board(db_factory)

    def _bug(bug_id: str, origin: str, position: int) -> Card:
        return Card(
            id=bug_id,
            board_id=ids["board"],
            spec_id=ids["spec"],
            title=f"Bug {bug_id}",
            status=CardStatus.NOT_STARTED,
            position=position,
            card_type=CardType.BUG,
            origin_task_id=origin,
            severity="major",
            expected_behavior="expected",
            observed_behavior="observed",
            created_by="resequence-test-user",
        )

    # Round-4 REJECT repro (val_3a016df4) plus siblings: roots A=c0, B=c1;
    # bugs A1→A, A3→A (siblings; A3 at lane position 0 would come FIRST in
    # BFS/lane order), chain A2→A1 (bug of bug — product-legal), B1→B.
    a1, a2, a3, b1 = (
        f"rsq-bug-{k}-{ids['board'][-8:]}" for k in ("a1", "a2", "a3", "b1")
    )
    async with db_factory() as db:
        db.add_all(
            [
                _bug(a1, ids["c0"], 5),
                _bug(a2, a1, 6),
                _bug(a3, ids["c0"], 0),
                _bug(b1, ids["c1"], 7),
            ]
        )
        await db.commit()

    async with db_factory() as db:
        await ArchiveService(db).archive_tree("spec", ids["spec"])
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    order = sorted(
        (
            (position, card_id)
            for card_id, (status, position, archived) in snap.items()
            if status == "not_started" and archived
        )
    )
    sequence = [card_id for _position, card_id in order]
    # DFS preorder (FR11): the pre-existing archived resident (ca) keeps its
    # place at the head of the archived range; the batch appends after it as
    # each root immediately followed by its FULL bug subtree — A, A3(sib,
    # lane-first), A1, A2(chain), then B, B1 — NEVER the breadth-first
    # A, B, A1, B1, A2.
    assert sequence == [
        ids["ca"],
        ids["c0"],
        a3,
        a1,
        a2,
        ids["c1"],
        b1,
        ids["c2"],
        ids["c3"],
    ]


async def test_position_collision_resolved_by_id_desc(db_factory) -> None:
    from okto_pulse.core.services.main import CardService
    from sqlalchemy_test_models import Card, CardStatus

    ids = await _seed_board(db_factory)
    async with db_factory() as db:
        # Force a REAL collision: c0 and c1 both at position 5.
        for name in ("c0", "c1"):
            row = await db.get(Card, ids[name])
            row.position = 5
        await db.flush()
        await CardService(db).resequence_columns(
            ids["board"], [], extra_columns=(CardStatus.NOT_STARTED,)
        )
        await db.commit()

    snap = await _snapshot(db_factory, ids["board"])
    # c2(2) and c3(3) first; the collided pair at 5 resolves id DESC — the
    # ids share the prefix "rsq-c" so "rsq-c1-..." > "rsq-c0-..." puts c1
    # before c0; archived straggler renumbers into the tail.
    assert snap[ids["c2"]] == ("not_started", 0, False)
    assert snap[ids["c3"]] == ("not_started", 1, False)
    assert snap[ids["c1"]] == ("not_started", 2, False)
    assert snap[ids["c0"]] == ("not_started", 3, False)
    assert snap[ids["ca"]] == ("not_started", 4, True)


async def test_archive_tree_and_restore_tree_e2e(db_factory) -> None:
    from okto_pulse.core.services.main import ArchiveService
    from sqlalchemy_test_models import Card

    ids = await _seed_board(db_factory)

    # Legacy corruption: an archived card carrying position -1 (the exact
    # codex repro — it must NOT restore to the front of the column).
    async with db_factory() as db:
        row = await db.get(Card, ids["ca"])
        row.position = -1
        row.pre_archive_status = "not_started"
        await db.commit()

    # Phase A — DIRECT restore with actives present (the codex repro): the
    # legacy -1 card restores via explicit placement=end, landing AFTER the
    # existing actives, never at the front.
    async with db_factory() as db:
        counts = await ArchiveService(db).restore_tree("spec", ids["spec"])
        await db.commit()
    assert counts["cards"] == 2  # only the archived stragglers ca and sa
    snap = await _snapshot(db_factory, ids["board"])
    assert snap[ids["ca"]] == ("not_started", 4, False)  # end, after c0..c3
    assert snap[ids["sa"]] == ("started", 2, False)  # end, after s0..s1
    assert not any(archived for (_, _, archived) in snap.values())

    # Phase B — archive the whole tree: every column densely archived 0..m,
    # relative (position ASC, id DESC) order preserved by the op preorder.
    async with db_factory() as db:
        counts = await ArchiveService(db).archive_tree("spec", ids["spec"])
        await db.commit()
    assert counts["cards"] == 8
    snap = await _snapshot(db_factory, ids["board"])
    assert all(archived for (_, _, archived) in snap.values())
    for status in ("not_started", "started"):
        positions = sorted(
            position for (s, position, _) in snap.values() if s == status
        )
        assert positions == list(range(len(positions)))  # dense 0..m

    # Phase C — restore the whole tree: dense actives, deterministic column
    # order preserved through the full cycle (ca stays at the END).
    async with db_factory() as db:
        counts = await ArchiveService(db).restore_tree("spec", ids["spec"])
        await db.commit()
    assert counts["cards"] == 8
    snap = await _snapshot(db_factory, ids["board"])
    assert not any(archived for (_, _, archived) in snap.values())
    for status in ("not_started", "started"):
        positions = sorted(
            position for (s, position, _) in snap.values() if s == status
        )
        assert positions == list(range(len(positions)))
    assert snap[ids["c0"]] == ("not_started", 0, False)
    assert snap[ids["ca"]] == ("not_started", 4, False)  # still the tail
    assert snap[ids["sa"]] == ("started", 2, False)


async def test_archive_restore_tree_cascades_to_sprint(db_factory) -> None:
    """spec -> sprint -> card: archive_tree / restore_tree cascade to the Sprint
    (a first-class descendant of Spec) — counts include it and the reversal
    restores the pre-archive status."""
    import uuid

    from okto_pulse.core.services.main import ArchiveService
    from sqlalchemy_test_models import (
        Board,
        Card,
        CardStatus,
        Spec,
        SpecStatus,
        Sprint,
        SprintStatus,
    )

    token = uuid.uuid4().hex[:8]
    board_id = f"arch-sprint-board-{token}"
    spec_id = f"arch-sprint-spec-{token}"
    sprint_id = f"arch-sprint-sprint-{token}"
    card_id = f"arch-sprint-card-{token}"

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Archive Sprint Board",
                owner_id="arch-user",
                settings={},
            )
        )
        await db.flush()
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Spec with a sprint",
                status=SpecStatus.IN_PROGRESS,
                created_by="arch-user",
                skip_decisions_coverage=True,
                evaluations=[],
            )
        )
        await db.flush()
        db.add(
            Sprint(
                id=sprint_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Sprint under spec",
                status=SprintStatus.ACTIVE,
                created_by="arch-user",
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                sprint_id=sprint_id,
                title="Card under sprint",
                status=CardStatus.NOT_STARTED,
                position=0,
                created_by="arch-user",
            )
        )
        await db.commit()

    # Archive from the spec: the sprint (and card) cascade with it.
    async with db_factory() as db:
        counts = await ArchiveService(db).archive_tree("spec", spec_id)
        await db.commit()
    assert counts["specs"] == 1
    assert counts["sprints"] == 1
    assert counts["cards"] == 1

    async with db_factory() as db:
        sprint = await db.get(Sprint, sprint_id)
        assert sprint.archived is True
        assert sprint.pre_archive_status == SprintStatus.ACTIVE.value
        assert (await db.get(Spec, spec_id)).archived is True
        assert (await db.get(Card, card_id)).archived is True

    # Restore from the spec: the sprint reverses cleanly (flag + status + marker).
    async with db_factory() as db:
        counts = await ArchiveService(db).restore_tree("spec", spec_id)
        await db.commit()
    assert counts["sprints"] == 1

    async with db_factory() as db:
        sprint = await db.get(Sprint, sprint_id)
        assert sprint.archived is False
        assert sprint.pre_archive_status is None
        assert sprint.status == SprintStatus.ACTIVE
        assert (await db.get(Spec, spec_id)).archived is False
        assert (await db.get(Card, card_id)).archived is False
