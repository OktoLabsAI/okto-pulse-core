"""Remaining legacy Sprint analytics fixtures until F5 retires their projections.

Operational CRUD/lifecycle/evaluation tests were retired with SprintService.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select


from sqlalchemy_test_models import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintLaneType,
    SprintStatus,
)


BOARD_ID = "sprint-crud-board-001"
AGENT_ID = "sprint-crud-agent-001"
SPEC_ID = "sprint-crud-spec-001"
CARD_1_ID = "sprint-crud-card-1"
CARD_2_ID = "sprint-crud-card-2"
CARD_3_ID = "sprint-crud-card-3"  # Different spec
HOTFIX_BUG_CARD_ID = "sprint-crud-hotfix-bug-card"
HOTFIX_TEST_CARD_ID = "sprint-crud-hotfix-test-card"
HOTFIX_NORMAL_CARD_ID = "sprint-crud-hotfix-normal-card"
HOTFIX_CROSS_SPEC_BUG_CARD_ID = "sprint-crud-hotfix-cross-spec-bug-card"
HOTFIX_ORIGIN_TASK_CARD_ID = "sprint-crud-hotfix-origin-task-card"
TS_1_ID = "sprint-crud-ts-1"
TS_2_ID = "sprint-crud-ts-2"
BR_1_ID = "sprint-crud-br-1"


async def _seed_board(db_factory) -> None:
    """Create a board, agent, spec, 2 cards, and test scenarios.

    Idempotent — skips if board already seeded by a prior test in this session.
    """
    async with db_factory() as db:
        existing = (
            await db.execute(select(Board).where(Board.id == BOARD_ID))
        ).scalar_one_or_none()
        if existing is not None:
            return

        board = Board(id=BOARD_ID, name="Sprint CRUD Board", owner_id=AGENT_ID)
        db.add(board)
        db.add(
            Spec(
                id=SPEC_ID,
                board_id=BOARD_ID,
                title="Sprint CRUD Spec",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1", "AC2", "AC3"],
                functional_requirements=["FR1", "FR2", "FR3"],
                test_scenarios=[
                    {
                        "id": TS_1_ID,
                        "title": "Test Scenario 1",
                        "linked_criteria": [0],
                        "status": "draft",
                        "linked_task_ids": [],
                    },
                    {
                        "id": TS_2_ID,
                        "title": "Test Scenario 2",
                        "linked_criteria": [1],
                        "status": "draft",
                        "linked_task_ids": [],
                    },
                ],
                business_rules=[
                    {
                        "id": BR_1_ID,
                        "title": "Business Rule 1",
                        "linked_requirements": [0],
                        "linked_task_ids": [],
                    },
                ],
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
                created_by=AGENT_ID,
            )
        )
        yesterday = datetime.now(timezone.utc) - timedelta(hours=12)
        db.add(
            Card(
                id=CARD_1_ID,
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Card 1",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=AGENT_ID,
                created_at=yesterday,
                updated_at=yesterday,
            )
        )
        db.add(
            Card(
                id=CARD_2_ID,
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Card 2",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=AGENT_ID,
                created_at=yesterday,
                updated_at=yesterday,
            )
        )
        await db.commit()


async def _seed_different_spec(db_factory) -> None:
    """Create a second spec with a card for cross-spec assignment tests."""
    async with db_factory() as db:
        existing = (
            await db.execute(select(Spec).where(Spec.id == "sprint-crud-spec-diff"))
        ).scalar_one_or_none()
        if existing is not None:
            return

        db.add(
            Spec(
                id="sprint-crud-spec-diff",
                board_id=BOARD_ID,
                title="Different Spec",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1"],
                functional_requirements=["FR1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
                created_by=AGENT_ID,
            )
        )
        db.add(
            Card(
                id=CARD_3_ID,
                board_id=BOARD_ID,
                spec_id="sprint-crud-spec-diff",
                title="Card from different spec",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=AGENT_ID,
            )
        )
        await db.commit()


async def _put_card(
    db,
    *,
    card_id: str,
    spec_id: str,
    title: str,
    card_type: CardType,
) -> Card:
    """Create or reset a card row for assignment-focused tests."""
    existing = await db.get(Card, card_id)
    if existing is not None:
        existing.spec_id = spec_id
        existing.title = title
        existing.status = CardStatus.NOT_STARTED
        existing.card_type = card_type
        existing.sprint_id = None
        existing.archived = False
        return existing

    card = Card(
        id=card_id,
        board_id=BOARD_ID,
        spec_id=spec_id,
        title=title,
        status=CardStatus.NOT_STARTED,
        card_type=card_type,
        archived=False,
        created_by=AGENT_ID,
    )
    db.add(card)
    return card


async def _clean_sprints(db_factory, board_id: str) -> None:
    """Delete all existing sprints for a board to ensure test isolation."""
    async with db_factory() as db:
        from sqlalchemy import delete

        stmt = delete(Sprint).where(Sprint.board_id == board_id)
        await db.execute(stmt)
        await db.commit()


# ============================================================================
# Sprint Creation Tests
# ============================================================================




# ============================================================================
# Sprint Update Tests
# ============================================================================




# ============================================================================
# Sprint State Machine Tests
# ============================================================================




# ============================================================================
# Sprint Evaluation Tests
# ============================================================================




# ============================================================================
# Sprint Card Assignment Tests
# ============================================================================




# ============================================================================
# Sprint Listing Tests
# ============================================================================




# ============================================================================
# Sprint Retrieval Tests
# ============================================================================




# ============================================================================
# Sprint History Tests
# ============================================================================




class TestSprintAnalyticsLaneBreakdown:
    """Tests for `/boards/{board_id}/analytics/sprints` lane separation."""

    async def test_sprint_analytics_separates_normal_and_hotfix_lanes(self, db_factory):
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            spec = await db.get(Spec, SPEC_ID)
            spec.status = SpecStatus.DONE
            await _put_card(
                db,
                card_id=HOTFIX_BUG_CARD_ID,
                spec_id=SPEC_ID,
                title="Analytics hotfix bug",
                card_type=CardType.BUG,
            )
            normal_one = Sprint(
                id="closed-origin",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Normal delivery sprint 1",
                status=SprintStatus.CLOSED,
                lane_type=SprintLaneType.NORMAL,
                created_by=AGENT_ID,
            )
            normal_two = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Normal delivery sprint 2",
                status=SprintStatus.ACTIVE,
                lane_type=SprintLaneType.NORMAL,
                created_by=AGENT_ID,
            )
            hotfix = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Hotfix lane",
                status=SprintStatus.ACTIVE,
                lane_type=SprintLaneType.HOTFIX,
                origin_sprint_id="closed-origin",
                origin_bug_id=HOTFIX_BUG_CARD_ID,
                created_by=AGENT_ID,
            )
            # Persist the self-referenced origin before its hotfix child so the
            # SQLite FK does not depend on insert-many ordering.
            db.add(normal_one)
            await db.flush()
            db.add_all([normal_two, hotfix])
            await db.flush()
            normal_card_one = await db.get(Card, CARD_1_ID)
            normal_card_one.sprint_id = normal_one.id
            normal_card_two = await db.get(Card, CARD_2_ID)
            normal_card_two.sprint_id = normal_two.id
            bug = await _put_card(
                db,
                card_id=HOTFIX_BUG_CARD_ID,
                spec_id=SPEC_ID,
                title="Bug in hotfix lane",
                card_type=CardType.BUG,
            )
            bug.sprint_id = hotfix.id
            await db.commit()

            from okto_pulse.core.services.analytics_service import (
                compute_sprints_analytics,
            )

            payload = await compute_sprints_analytics(db, BOARD_ID)

        summary = payload["summary"]
        assert summary["total_sprints"] == 3
        assert summary["normal_sprints_total"] == 2
        assert summary["hotfix_lanes_total"] == 1
        assert summary["active_hotfix_lanes"] == 1

        by_id = {row["sprint_id"]: row for row in payload["sprints"]}
        normal_rows = [
            row for row in payload["sprints"] if row["lane_type"] == "normal"
        ]
        hotfix_rows = [
            row for row in payload["sprints"] if row["lane_type"] == "hotfix"
        ]
        assert {row["sprint_id"] for row in normal_rows} == {
            normal_one.id,
            normal_two.id,
        }
        assert {row["sprint_id"] for row in hotfix_rows} == {hotfix.id}
        assert by_id[normal_one.id]["normal_sprint_created"] is True
        assert by_id[normal_two.id]["normal_sprint_created"] is True
        assert by_id[hotfix.id]["lane_type"] == "hotfix"
        assert by_id[hotfix.id]["origin_sprint_id"] == "closed-origin"
        assert by_id[hotfix.id]["origin_bug_id"] == HOTFIX_BUG_CARD_ID
        assert by_id[hotfix.id]["normal_sprint_created"] is False
        assert by_id[normal_one.id]["commitment"] == {
            "sprint_id": normal_one.id,
            "state": "unavailable_legacy",
            "baseline_ref": None,
            "unavailable_reason": "activation_baseline_not_persisted",
        }

    async def test_sprint_analytics_window_keeps_active_sprint_and_full_membership(
        self, db_factory
    ):
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=60)
        recent = now - timedelta(days=2)
        async with db_factory() as db:
            active_old = Sprint(
                id="analytics-active-before-window",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Active before window",
                status=SprintStatus.ACTIVE,
                lane_type=SprintLaneType.NORMAL,
                created_at=old,
                created_by=AGENT_ID,
            )
            closed_old = Sprint(
                id="analytics-closed-before-window",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Closed before window",
                status=SprintStatus.CLOSED,
                lane_type=SprintLaneType.NORMAL,
                created_at=old,
                created_by=AGENT_ID,
            )
            closed_recent = Sprint(
                id="analytics-closed-in-window",
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Closed in window",
                status=SprintStatus.CLOSED,
                lane_type=SprintLaneType.NORMAL,
                created_at=recent,
                created_by=AGENT_ID,
            )
            db.add_all([active_old, closed_old, closed_recent])
            await db.flush()
            old_member = await _put_card(
                db,
                card_id="analytics-old-active-member",
                spec_id=SPEC_ID,
                title="Member created before the window",
                card_type=CardType.NORMAL,
            )
            old_member.created_at = old
            old_member.sprint_id = active_old.id
            await db.commit()

            from okto_pulse.core.services.analytics_service import (
                compute_sprints_analytics,
            )

            payload = await compute_sprints_analytics(
                db,
                BOARD_ID,
                dt_from=now - timedelta(days=7),
                dt_to=now + timedelta(days=1),
            )

        by_id = {row["sprint_id"]: row for row in payload["sprints"]}
        assert set(by_id) == {active_old.id, closed_recent.id}
        assert by_id[active_old.id]["total_cards"] == 1
        assert closed_old.id not in by_id


# ============================================================================
# Sprint Deletion Tests
# ============================================================================




# ============================================================================
# Skip Flags Tests
# ============================================================================
