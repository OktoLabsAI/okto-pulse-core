"""Comprehensive tests for card lifecycle operations.

Covers the full card state machine, CRUD, dependencies, and activity logging.

State machine:
    not_started → started → in_progress → validation → done

Rules:
    - Moving to 'done' requires: conclusion, completeness (0-100),
      completeness_justification, drift (0-100), drift_justification
    - Circular dependencies are blocked
    - Bug cards require origin_task_id, severity, expected_behavior, observed_behavior
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from okto_pulse.core.models.db import (
    ActivityLog,
    Board,
    Card,
    CardDependency,
    CardPriority,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate
from okto_pulse.core.services.main import CardService


BOARD_ID = "card-lifecycle-board-001"
AGENT_ID = "card-lifecycle-agent-001"
USER_ID = AGENT_ID


# ============================================================================
# Seed helpers
# ============================================================================


async def _seed_board(db_factory) -> None:
    """Create minimal fixture: 1 board, 1 agent, 2 cards, 1 spec.

    Idempotent — skips if board already seeded by a prior test.
    Returns a dict with created entities for convenience.
    """
    async with db_factory() as db:
        existing = await db.get(Board, BOARD_ID)
        if existing is not None:
            return

        board = Board(id=BOARD_ID, name="Card Lifecycle Board", owner_id=USER_ID)
        db.add(board)

        spec_id = str(uuid.uuid4())
        spec = Spec(
            id=spec_id,
            board_id=BOARD_ID,
            title="Lifecycle Spec",
            status=SpecStatus.APPROVED,
            created_by=USER_ID,
            functional_requirements=["FR1", "FR2"],
            acceptance_criteria=["AC1", "AC2"],
            test_scenarios=[
                {"id": "ts-001", "title": "Scenario 1", "given": "g", "when": "w",
                 "then": "t", "scenario_type": "integration", "linked_criteria": [0],
                 "linked_task_ids": [], "status": "draft"},
                {"id": "ts-002", "title": "Scenario 2", "given": "g", "when": "w",
                 "then": "t", "scenario_type": "unit", "linked_criteria": [1],
                 "linked_task_ids": [], "status": "draft"},
            ],
            business_rules=[],
            api_contracts=[],
            technical_requirements=[],
            decisions=[],
        )
        db.add(spec)

        card1_id = str(uuid.uuid4())
        card1 = Card(
            id=card1_id,
            board_id=BOARD_ID,
            spec_id=spec_id,
            title="Card One",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            priority=CardPriority.MEDIUM,
            position=0,
            created_by=USER_ID,
            labels=["label-a"],
        )
        db.add(card1)

        card2_id = str(uuid.uuid4())
        card2 = Card(
            id=card2_id,
            board_id=BOARD_ID,
            spec_id=spec_id,
            title="Card Two",
            status=CardStatus.STARTED,
            card_type=CardType.NORMAL,
            priority=CardPriority.HIGH,
            position=0,
            created_by=USER_ID,
            labels=["label-b"],
        )
        db.add(card2)

        await db.commit()


# ============================================================================
# 1. Card creation: normal card with all fields
# ============================================================================


@pytest.mark.asyncio
class TestCardCreation:
    """AC-1: Card creation with various field combinations."""

    async def test_create_normal_card_with_all_fields(self, db_factory):
        """Create a normal card with title, description, details, priority, labels."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            data = CardCreate(
                title="New Task Card",
                description="A task to implement feature X",
                details="Implementation notes here",
                status=CardStatus.NOT_STARTED,
                priority=CardPriority.HIGH,
                assignee_id="assignee-1",
                spec_id="spec-lifecycle-001",  # will be auto-set from seed
            )
            # Fetch the actual spec_id from the seeded board
            board = await db.get(Board, BOARD_ID)
            spec = await db.get(Spec, board.id)
            # The seed creates a spec with a random UUID; get it
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id if specs else None
            assert actual_spec_id is not None

            data.spec_id = actual_spec_id
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            assert card is not None
            assert card.title == "New Task Card"
            assert card.description == "A task to implement feature X"
            assert card.details == "Implementation notes here"
            assert card.status == CardStatus.NOT_STARTED
            assert card.priority == CardPriority.HIGH
            assert card.assignee_id == "assignee-1"
            assert card.spec_id == actual_spec_id
            assert card.card_type == CardType.NORMAL

    async def test_create_card_with_test_scenario_ids(self, db_factory):
        """Create a test card linked to specific test scenarios."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title="Test Card",
                status=CardStatus.NOT_STARTED,
                card_type="test",
                spec_id=actual_spec_id,
                test_scenario_ids=["ts-001", "ts-002"],
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            assert card is not None
            assert card.card_type == CardType.TEST
            assert card.test_scenario_ids == ["ts-001", "ts-002"]

    async def test_create_test_card_without_scenarios_raises(self, db_factory):
        """Test card creation without test_scenario_ids must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title="Bad Test Card",
                status=CardStatus.NOT_STARTED,
                card_type="test",
                spec_id=actual_spec_id,
            )
            with pytest.raises(ValueError, match="test_scenario_ids is required"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_create_card_without_spec_raises(self, db_factory):
        """Card creation without spec_id must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            data = CardCreate(
                title="Orphan Card",
                status=CardStatus.NOT_STARTED,
            )
            with pytest.raises(ValueError, match="Every task must be linked to a spec"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_create_card_with_invalid_scenario_raises(self, db_factory):
        """Test card with non-existent scenario IDs must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title="Bad Test Card",
                status=CardStatus.NOT_STARTED,
                card_type="test",
                spec_id=actual_spec_id,
                test_scenario_ids=["ts-999"],
            )
            with pytest.raises(ValueError, match="not found in spec"):
                await svc.create_card(BOARD_ID, USER_ID, data)


# ============================================================================
# 2. Card status transitions
# ============================================================================


@pytest.mark.asyncio
class TestCardStatusTransitions:
    """AC-2: Card status transitions through the state machine."""

    async def _create_card_for_transition(self, db_factory, status=CardStatus.NOT_STARTED):
        """Helper: create a card in a given status for transition testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title=f"Transition Card ({status.value})",
                status=status,
                spec_id=actual_spec_id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            return card, actual_spec_id

    async def test_transition_not_started_to_started(self, db_factory):
        """not_started → started is a valid forward transition."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.NOT_STARTED)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.STARTED),
            )
            assert moved is not None
            assert moved.status == CardStatus.STARTED

    async def test_transition_started_to_in_progress(self, db_factory):
        """started → in_progress is a valid forward transition."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.STARTED)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            assert moved is not None
            assert moved.status == CardStatus.IN_PROGRESS

    async def test_transition_in_progress_to_validation(self, db_factory):
        """in_progress → validation is a valid forward transition."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.IN_PROGRESS)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.VALIDATION),
            )
            assert moved is not None
            assert moved.status == CardStatus.VALIDATION

    async def test_transition_validation_to_done_with_required_fields(self, db_factory):
        """validation → done requires conclusion, completeness, drift."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(
                    status=CardStatus.DONE,
                    conclusion="Implemented feature X with unit tests",
                    completeness=100,
                    completeness_justification="All planned work completed",
                    drift=0,
                    drift_justification="No deviation from plan",
                ),
            )
            assert moved is not None
            assert moved.status == CardStatus.DONE
            assert moved.conclusions is not None
            assert len(moved.conclusions) == 1
            assert moved.conclusions[0]["completeness"] == 100
            assert moved.conclusions[0]["drift"] == 0

    async def test_transition_done_backward_to_not_started(self, db_factory):
        """done → not_started is a valid backward transition (reset)."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.DONE)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.NOT_STARTED),
            )
            assert moved is not None
            assert moved.status == CardStatus.NOT_STARTED

    async def test_invalid_transition_not_started_to_done_raises(self, db_factory):
        """not_started → done without required fields must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.NOT_STARTED)
        async with db_factory() as db:
            svc = CardService(db)
            # Move to in_progress first (required before done)
            await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            # Now try to go directly to done without going through validation
            # (validation gate may block, but definitely missing required fields)
            with pytest.raises(ValueError, match="conclusion"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(status=CardStatus.DONE),
                )

    async def test_missing_conclusion_raises(self, db_factory):
        """Moving to done without conclusion must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="conclusion"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        completeness=100,
                        completeness_justification="Complete",
                        drift=0,
                        drift_justification="No deviation",
                    ),
                )

    async def test_missing_completeness_raises(self, db_factory):
        """Moving to done without completeness must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="completeness"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        conclusion="Done",
                        drift=0,
                        drift_justification="No deviation",
                    ),
                )

    async def test_completeness_out_of_range_raises(self, db_factory):
        """Completeness outside 0-100 must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="between 0 and 100"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        conclusion="Done",
                        completeness=101,
                        completeness_justification="Complete",
                        drift=0,
                        drift_justification="No deviation",
                    ),
                )

    async def test_missing_drift_raises(self, db_factory):
        """Moving to done without drift must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="drift"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        conclusion="Done",
                        completeness=100,
                        completeness_justification="Complete",
                        drift_justification="No deviation",
                    ),
                )

    async def test_drift_out_of_range_raises(self, db_factory):
        """Drift outside 0-100 must raise ValueError."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="between 0 and 100"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        conclusion="Done",
                        completeness=100,
                        completeness_justification="Complete",
                        drift=101,
                        drift_justification="No deviation",
                    ),
                )

    async def test_multiple_conclusions_accumulate(self, db_factory):
        """Multiple moves to done should accumulate conclusion entries."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            # First completion
            await svc.move_card(
                card.id, USER_ID,
                CardMove(
                    status=CardStatus.DONE,
                    conclusion="First completion",
                    completeness=100,
                    completeness_justification="Complete",
                    drift=0,
                    drift_justification="No deviation",
                ),
            )
            # Reset to validation
            await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.VALIDATION),
            )
            # Second completion
            await svc.move_card(
                card.id, USER_ID,
                CardMove(
                    status=CardStatus.DONE,
                    conclusion="Second completion",
                    completeness=80,
                    completeness_justification="Mostly complete",
                    drift=10,
                    drift_justification="Minor deviation",
                ),
            )
            await db.refresh(card)
            assert len(card.conclusions) == 2
            assert card.conclusions[0]["text"] == "First completion"
            assert card.conclusions[1]["text"] == "Second completion"


# ============================================================================
# 3. Card updates
# ============================================================================


@pytest.mark.asyncio
class TestCardUpdates:
    """AC-3: Card field updates."""

    async def test_update_title_and_description(self, db_factory):
        """Update card title and description."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()
            assert card is not None

            updated = await svc.update_card(
                card.id, USER_ID,
                CardUpdate(
                    title="Updated Title",
                    description="Updated description text",
                ),
            )
            assert updated is not None
            assert updated.title == "Updated Title"
            assert updated.description == "Updated description text"

    async def test_update_priority(self, db_factory):
        """Update card priority."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            updated = await svc.update_card(
                card.id, USER_ID,
                CardUpdate(priority=CardPriority.CRITICAL),
            )
            assert updated.priority == CardPriority.CRITICAL

    async def test_update_labels(self, db_factory):
        """Update card labels."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            updated = await svc.update_card(
                card.id, USER_ID,
                CardUpdate(labels=["new-label-1", "new-label-2"]),
            )
            assert updated.labels == ["new-label-1", "new-label-2"]

    async def test_update_assignee(self, db_factory):
        """Change card assignee."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            updated = await svc.update_card(
                card.id, USER_ID,
                CardUpdate(assignee_id="new-assignee-123"),
            )
            assert updated.assignee_id == "new-assignee-123"

    async def test_update_nonexistent_card_returns_none(self, db_factory):
        """Updating a non-existent card returns None."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            result = await svc.update_card(
                "nonexistent-card-id", USER_ID,
                CardUpdate(title="Ghost"),
            )
            assert result is None


# ============================================================================
# 4. Card dependencies
# ============================================================================


@pytest.mark.asyncio
class TestCardDependencies:
    """AC-4: Card dependency management."""

    async def _setup_dependency_test(self, db_factory):
        """Helper: seed board and return the two cards for dependency testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            cards = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().all()
            assert len(cards) >= 2
            return cards[0], cards[1]

    async def test_add_dependency(self, db_factory):
        """Card A depends on Card B — dependency is created."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            dep = await svc.add_dependency(card_a.id, card_b.id)
            assert dep is not None
            assert dep.card_id == card_a.id
            assert dep.depends_on_id == card_b.id

            # Verify via get_dependencies
            deps = await svc.get_dependencies(card_a.id)
            dep_ids = [d.id for d in deps]
            assert card_b.id in dep_ids

    async def test_circular_dependency_detection(self, db_factory):
        """A→B then B→A should be blocked (circular)."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            # First: A depends on B
            dep1 = await svc.add_dependency(card_a.id, card_b.id)
            assert dep1 is not None

            # Second: B depends on A — should be blocked
            dep2 = await svc.add_dependency(card_b.id, card_a.id)
            assert dep2 is None

            # Verify: only A→B exists
            deps_a = await svc.get_dependencies(card_a.id)
            deps_b = await svc.get_dependencies(card_b.id)
            assert len(deps_a) == 1
            assert len(deps_b) == 0

    async def test_circular_dependency_long_chain(self, db_factory):
        """A→B→C→A should be blocked at the final link."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            # Create a third card
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            card_c = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=specs[0].id,
                title="Card Three",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(card_c)
            await db.commit()

            cards = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().all()
            ca, cb, cc = cards[0], cards[1], card_c

            # A→B
            assert await svc.add_dependency(ca.id, cb.id) is not None
            # B→C
            assert await svc.add_dependency(cb.id, cc.id) is not None
            # C→A — should be blocked (creates A→B→C→A cycle)
            assert await svc.add_dependency(cc.id, ca.id) is None

    async def test_remove_dependency(self, db_factory):
        """Adding and then removing a dependency."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            # Add
            dep = await svc.add_dependency(card_a.id, card_b.id)
            assert dep is not None

            # Remove
            removed = await svc.remove_dependency(card_a.id, card_b.id)
            assert removed is True

            # Verify removed
            deps = await svc.get_dependencies(card_a.id)
            assert len(deps) == 0

    async def test_self_reference_detection(self, db_factory):
        """A card depending on itself should be blocked."""
        card_a, _ = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            dep = await svc.add_dependency(card_a.id, card_a.id)
            assert dep is None

    async def test_block_forward_move_on_unmet_dependency(self, db_factory):
        """Forward move should be blocked if dependencies are not met."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            # Advance spec to in_progress so cards can move
            spec = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().first()
            spec.status = SpecStatus.IN_PROGRESS
            await db.commit()

            svc = CardService(db)
            # Make card_b depend on card_a (so card_a is the blocker)
            await svc.add_dependency(card_b.id, card_a.id)

            # Try to move card_b forward — should be blocked because card_a is not done
            with pytest.raises(ValueError, match="Dependências"):
                await svc.move_card(
                    card_b.id, USER_ID,
                    CardMove(status=CardStatus.IN_PROGRESS),
                )

    async def test_forward_move_unblocked_after_dependency_done(self, db_factory):
        """Forward move should succeed after dependency card is moved to done."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            # Advance spec to in_progress so cards can move
            spec = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().first()
            spec.status = SpecStatus.IN_PROGRESS
            await db.commit()

            svc = CardService(db)
            # Make card_b depend on card_a
            await svc.add_dependency(card_b.id, card_a.id)

            # Move card_a to done first
            await svc.move_card(
                card_a.id, USER_ID,
                CardMove(
                    status=CardStatus.DONE,
                    conclusion="Dependency done",
                    completeness=100,
                    completeness_justification="Complete",
                    drift=0,
                    drift_justification="No deviation",
                ),
            )

            # Now card_b should be able to move forward
            moved = await svc.move_card(
                card_b.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            assert moved.status == CardStatus.IN_PROGRESS


# ============================================================================
# 5. Card deletion
# ============================================================================


@pytest.mark.asyncio
class TestCardDeletion:
    """AC-5: Card deletion behavior."""

    async def test_delete_normal_card(self, db_factory):
        """Deleting a normal card removes it from the database."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()
            assert card is not None

            # Count before deletion (database is shared across tests)
            count_before = len((await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().all())

            deleted = await svc.delete_card(card.id, USER_ID)
            assert deleted is True

            # Verify card is gone (delete_card does delete but we query
            # in the same session since delete_card doesn't explicitly commit)
            await db.flush()
            remaining = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().all()
            assert len(remaining) == count_before - 1

    async def test_delete_nonexistent_card_returns_false(self, db_factory):
        """Deleting a non-existent card returns False."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            deleted = await svc.delete_card("nonexistent-card", USER_ID)
            assert deleted is False

    async def test_delete_card_unlinks_from_spec(self, db_factory):
        """Deleting a card should clean linked_task_ids from spec containers."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            # Add the card to a spec's test scenario linked_task_ids
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec = specs[0]
            spec.test_scenarios[0]["linked_task_ids"] = [card.id]
            from sqlalchemy.orm.attributes import flag_modified
            flag_modified(spec, "test_scenarios")
            await db.commit()

            # Delete the card
            await svc.delete_card(card.id, USER_ID)
            await db.commit()

            # Verify spec is cleaned
            await db.refresh(spec)
            assert card.id not in spec.test_scenarios[0]["linked_task_ids"]

    async def test_delete_test_card_cleans_bug_linked_test_task_ids(self, db_factory):
        """Deleting a test card should remove it from bug card's linked_test_task_ids."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            # Create a test card
            test_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Test Card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.TEST,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(test_card)

            # Create a bug card that references the test card
            bug_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Bug Card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.BUG,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
                linked_test_task_ids=[test_card.id],
            )
            db.add(bug_card)
            await db.commit()

            # Delete the test card
            await svc.delete_card(test_card.id, USER_ID)
            await db.commit()

            # Verify bug card's linked_test_task_ids is cleaned
            await db.refresh(bug_card)
            assert test_card.id not in (bug_card.linked_test_task_ids or [])


# ============================================================================
# 6. Bug card creation
# ============================================================================


@pytest.mark.asyncio
class TestBugCardCreation:
    """AC-6: Bug card specific requirements."""

    async def test_create_bug_card_with_required_fields(self, db_factory):
        """Bug card creation requires origin_task_id, severity, expected/observed behavior."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            # Create an origin task first (the bug card depends on it)
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Origin Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            data = CardCreate(
                title="Bug: Login Fails",
                card_type="bug",
                origin_task_id=origin_card.id,
                severity="critical",
                expected_behavior="User should be able to log in with valid credentials",
                observed_behavior="User receives 500 error on login attempt",
                spec_id=spec_id,  # auto-resolved from origin_task, but also provided
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            assert card is not None
            assert card.card_type == CardType.BUG
            assert card.severity == "critical"
            assert card.origin_task_id == origin_card.id
            assert card.expected_behavior == data.expected_behavior
            assert card.observed_behavior == data.observed_behavior

    async def test_create_bug_card_without_origin_task_raises(self, db_factory):
        """Bug card without origin_task_id must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            data = CardCreate(
                title="Bad Bug",
                card_type="bug",
                severity="critical",
                expected_behavior="Should work",
                observed_behavior="Broken",
            )
            with pytest.raises(ValueError, match="origin_task_id"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_create_bug_card_without_severity_raises(self, db_factory):
        """Bug card without severity must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            # Create a valid origin task so we get past origin_task_id validation
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Origin Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            svc = CardService(db)
            data = CardCreate(
                title="Bad Bug",
                card_type="bug",
                origin_task_id=origin_card.id,
                # missing severity
            )
            with pytest.raises(ValueError, match="severity"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_create_bug_card_without_expected_behavior_raises(self, db_factory):
        """Bug card without expected_behavior must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            # Create a valid origin task
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Origin Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            svc = CardService(db)
            data = CardCreate(
                title="Bad Bug",
                card_type="bug",
                origin_task_id=origin_card.id,
                severity="critical",
                # missing expected_behavior
                observed_behavior="Broken",
            )
            with pytest.raises(ValueError, match="expected_behavior"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_create_bug_card_without_observed_behavior_raises(self, db_factory):
        """Bug card without observed_behavior must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            # Create a valid origin task
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Origin Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            svc = CardService(db)
            data = CardCreate(
                title="Bad Bug",
                card_type="bug",
                origin_task_id=origin_card.id,
                severity="critical",
                expected_behavior="Should work",
                # missing observed_behavior
            )
            with pytest.raises(ValueError, match="observed_behavior"):
                await svc.create_card(BOARD_ID, USER_ID, data)

    async def test_bug_card_limited_initial_status(self, db_factory):
        """Bug cards can only be created with not_started or started status."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec_id,
                title="Origin Task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            # Bug card with in_progress status should be rejected
            data = CardCreate(
                title="Bad Bug Status",
                card_type="bug",
                origin_task_id=origin_card.id,
                severity="critical",
                expected_behavior="Should work",
                observed_behavior="Broken",
                status=CardStatus.IN_PROGRESS,
            )
            with pytest.raises(ValueError, match="not_started.*started"):
                await svc.create_card(BOARD_ID, USER_ID, data)


# ============================================================================
# 7. Priority tests
# ============================================================================


@pytest.mark.asyncio
class TestCardPriorities:
    """AC-7: Card priority levels."""

    async def test_create_critical_priority_card(self, db_factory):
        """Create a card with critical priority."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()

            data = CardCreate(
                title="Critical Task",
                priority=CardPriority.CRITICAL,
                spec_id=specs[0].id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            assert card.priority == CardPriority.CRITICAL

    async def test_update_priority_to_critical(self, db_factory):
        """Update a card's priority to critical."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            updated = await svc.update_card(
                card.id, USER_ID,
                CardUpdate(priority=CardPriority.CRITICAL),
            )
            assert updated.priority == CardPriority.CRITICAL

    async def test_all_priority_levels(self, db_factory):
        """Test creating cards with all priority levels."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            for priority in [
                CardPriority.CRITICAL,
                CardPriority.VERY_HIGH,
                CardPriority.HIGH,
                CardPriority.MEDIUM,
                CardPriority.LOW,
                CardPriority.NONE,
            ]:
                data = CardCreate(
                    title=f"Priority-{priority.value}",
                    priority=priority,
                    spec_id=spec_id,
                )
                card = await svc.create_card(BOARD_ID, USER_ID, data)
                assert card.priority == priority


# ============================================================================
# 8. Multiple status cards
# ============================================================================


@pytest.mark.asyncio
class TestMultipleStatusCards:
    """AC-8: Create and manage cards in different statuses."""

    async def test_create_cards_in_different_statuses(self, db_factory):
        """Create cards in various statuses on the same board."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            statuses = [
                CardStatus.NOT_STARTED,
                CardStatus.STARTED,
                CardStatus.IN_PROGRESS,
                CardStatus.VALIDATION,
            ]
            created_cards = []
            for status in statuses:
                data = CardCreate(
                    title=f"Card in {status.value}",
                    status=status,
                    spec_id=spec_id,
                )
                card = await svc.create_card(BOARD_ID, USER_ID, data)
                created_cards.append(card)

            # Verify all cards exist with correct statuses
            all_cards = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().all()
            assert len(all_cards) >= 4  # seed cards + created cards

            for card in created_cards:
                assert card.status in statuses

    async def test_card_positions_per_status(self, db_factory):
        """Cards in different statuses should have positions within their column."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            # Create two cards in not_started
            data1 = CardCreate(title="NS Card 1", status=CardStatus.NOT_STARTED, spec_id=spec_id)
            data2 = CardCreate(title="NS Card 2", status=CardStatus.NOT_STARTED, spec_id=spec_id)
            c1 = await svc.create_card(BOARD_ID, USER_ID, data1)
            c2 = await svc.create_card(BOARD_ID, USER_ID, data2)

            # Create one card in started
            data3 = CardCreate(title="Started Card", status=CardStatus.STARTED, spec_id=spec_id)
            c3 = await svc.create_card(BOARD_ID, USER_ID, data3)

            assert c1.position >= 0
            assert c2.position >= 0
            assert c3.position >= 0


# ============================================================================
# 9. Activity log
# ============================================================================


@pytest.mark.asyncio
class TestActivityLog:
    """AC-9: Activity logging on card operations."""

    async def _create_card_for_move(self, db_factory, status=CardStatus.NOT_STARTED):
        """Helper: create a card in a given status for transition testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title=f"Move Card ({status.value})",
                status=status,
                spec_id=actual_spec_id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            await db.commit()
            return card, actual_spec_id

    async def test_activity_logged_on_card_creation(self, db_factory):
        """Creating a card should log a card_created activity."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            data = CardCreate(title="Activity Test Card", spec_id=spec_id)
            await svc.create_card(BOARD_ID, USER_ID, data)
            await db.commit()

            # Check activity log
            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_created")
                .order_by(ActivityLog.created_at.desc())
            )).scalars().all()
            assert len(logs) >= 1
            assert logs[0].card_id is not None
            assert logs[0].actor_id == USER_ID
            assert logs[0].details is not None

    async def test_activity_logged_on_status_change(self, db_factory):
        """Moving a card should log a card_moved activity."""
        card, _ = await self._create_card_for_move(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.STARTED),
            )
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_moved")
                .where(ActivityLog.card_id == card.id)
            )).scalars().all()
            assert len(logs) >= 1
            assert logs[0].details is not None
            assert logs[0].details.get("from_status") == "not_started"
            assert logs[0].details.get("to_status") == "started"

    async def test_activity_logged_on_card_update(self, db_factory):
        """Updating a card should log a card_updated activity."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            await svc.update_card(
                card.id, USER_ID,
                CardUpdate(title="Updated via test"),
            )
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_updated")
                .where(ActivityLog.card_id == card.id)
            )).scalars().all()
            assert len(logs) >= 1

    async def test_activity_logged_on_card_deletion(self, db_factory):
        """Deleting a card should log a card_deleted activity."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            await svc.delete_card(card.id, USER_ID)
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_deleted")
                .where(ActivityLog.card_id == card.id)
            )).scalars().all()
            assert len(logs) >= 1

    async def test_activity_has_actor_info(self, db_factory):
        """Activity log entries should contain actor_id and actor_name."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()

            data = CardCreate(title="Actor Test", spec_id=specs[0].id)
            await svc.create_card(BOARD_ID, USER_ID, data)
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_created")
            )).scalars().all()
            assert logs
            assert logs[0].actor_id == USER_ID
            assert logs[0].actor_type == "user"
            assert logs[0].actor_name != ""

    async def test_multiple_status_changes_log_each(self, db_factory):
        """Each status change should create a separate activity log entry."""
        card, _ = await self._create_card_for_move(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            # Move through the chain
            await svc.move_card(card.id, USER_ID, CardMove(status=CardStatus.STARTED))
            await svc.move_card(card.id, USER_ID, CardMove(status=CardStatus.IN_PROGRESS))
            await svc.move_card(card.id, USER_ID, CardMove(status=CardStatus.VALIDATION))
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_moved")
                .where(ActivityLog.card_id == card.id)
            )).scalars().all()
            assert len(logs) == 3


# ============================================================================
# Helper for move tests
# ============================================================================


@pytest.mark.asyncio
class TestCardStatusTransitions:
    """Re-include helper for tests that need a card ready for moving."""

    async def _create_card_for_move(self, db_factory, status=CardStatus.NOT_STARTED):
        """Helper: create a card in a given status for transition testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id

            data = CardCreate(
                title=f"Move Card ({status.value})",
                status=status,
                spec_id=actual_spec_id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            await db.commit()
            return card, actual_spec_id
