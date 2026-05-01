"""Comprehensive tests for sprint CRUD operations.

Covers:
- Sprint creation with various configurations
- Sprint updates (title, description, labels, test_scenario_ids, business_rule_ids)
- Sprint state machine transitions with gates
- Sprint evaluation submission
- Sprint card assignment
- Sprint listing and retrieval
- Sprint deletion
- Skip flags (skip_test_coverage, skip_rules_coverage)
- Sprint history logging
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)
from okto_pulse.core.models.schemas import SprintCreate, SprintMove, SprintUpdate


BOARD_ID = "sprint-crud-board-001"
AGENT_ID = "sprint-crud-agent-001"
SPEC_ID = "sprint-crud-spec-001"
CARD_1_ID = "sprint-crud-card-1"
CARD_2_ID = "sprint-crud-card-2"
CARD_3_ID = "sprint-crud-card-3"  # Different spec
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
        db.add(Spec(
            id=SPEC_ID,
            board_id=BOARD_ID,
            title="Sprint CRUD Spec",
            status=SpecStatus.IN_PROGRESS,
            archived=False,
            acceptance_criteria=["AC1", "AC2", "AC3"],
            functional_requirements=["FR1", "FR2", "FR3"],
            test_scenarios=[
                {"id": TS_1_ID, "title": "Test Scenario 1", "linked_criteria": [0], "status": "draft", "linked_task_ids": []},
                {"id": TS_2_ID, "title": "Test Scenario 2", "linked_criteria": [1], "status": "draft", "linked_task_ids": []},
            ],
            business_rules=[
                {"id": BR_1_ID, "title": "Business Rule 1", "linked_requirements": [0], "linked_task_ids": []},
            ],
            api_contracts=[],
            technical_requirements=[],
            decisions=[],
            created_by=AGENT_ID,
        ))
        yesterday = datetime.now(timezone.utc) - timedelta(hours=12)
        db.add(Card(
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
        ))
        db.add(Card(
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
        ))
        await db.commit()


async def _seed_different_spec(db_factory) -> None:
    """Create a second spec with a card for cross-spec assignment tests."""
    async with db_factory() as db:
        existing = (
            await db.execute(select(Spec).where(Spec.id == "sprint-crud-spec-diff"))
        ).scalar_one_or_none()
        if existing is not None:
            return

        db.add(Spec(
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
        ))
        db.add(Card(
            id=CARD_3_ID,
            board_id=BOARD_ID,
            spec_id="sprint-crud-spec-diff",
            title="Card from different spec",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.NORMAL,
            archived=False,
            created_by=AGENT_ID,
        ))
        await db.commit()


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


class TestSprintCreation:
    """Tests for sprint creation with various configurations."""

    async def test_creation_minimal(self, db_factory):
        """Test 1: Minimal sprint creation (title only)."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Minimal Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        assert sprint.title == "Minimal Sprint"
        assert sprint.status == SprintStatus.DRAFT
        assert sprint.description is None
        assert sprint.objective is None
        assert sprint.expected_outcome is None
        assert sprint.labels is None
        assert sprint.test_scenario_ids is None
        assert sprint.business_rule_ids is None
        assert sprint.start_date is None
        assert sprint.end_date is None
        assert sprint.version == 1
        assert sprint.archived is False

    async def test_creation_with_all_fields(self, db_factory):
        """Test 2: Sprint creation with all fields populated."""
        await _seed_board(db_factory)
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=14)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Full Sprint",
                description="A comprehensive sprint description",
                objective="Deliver core features",
                expected_outcome="Working demo",
                start_date=now,
                end_date=end,
                labels=["sprint", "v1", "priority"],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        assert sprint.title == "Full Sprint"
        assert sprint.description == "A comprehensive sprint description"
        assert sprint.objective == "Deliver core features"
        assert sprint.expected_outcome == "Working demo"
        assert sprint.start_date is not None
        assert sprint.end_date is not None
        assert sprint.labels == ["sprint", "v1", "priority"]
        assert sprint.version == 1

    async def test_creation_with_test_scenarios(self, db_factory):
        """Test 3: Sprint creation linked to test scenarios."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="TS Sprint",
                test_scenario_ids=[TS_1_ID, TS_2_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        assert sprint.test_scenario_ids == [TS_1_ID, TS_2_ID]

    async def test_creation_with_business_rules(self, db_factory):
        """Test 4: Sprint creation linked to business rules."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="BR Sprint",
                business_rule_ids=[BR_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        assert sprint.business_rule_ids == [BR_1_ID]

    async def test_creation_via_schema_minimal(self, db_factory):
        """Sprint creation using SprintCreate schema — title only."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="Schema Sprint", spec_id=SPEC_ID)
            sprint = await service.create_sprint(BOARD_ID, AGENT_ID, data)

        assert sprint is not None
        assert sprint.title == "Schema Sprint"
        assert sprint.spec_id == SPEC_ID
        assert sprint.status == SprintStatus.DRAFT
        assert sprint.board_id == BOARD_ID

    async def test_creation_via_schema_full(self, db_factory):
        """Sprint creation using SprintCreate schema — all fields."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=14)
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(
                title="Full Schema Sprint",
                description="Full description",
                objective="Full objective",
                expected_outcome="Full outcome",
                spec_id=SPEC_ID,
                test_scenario_ids=[TS_1_ID],
                business_rule_ids=[BR_1_ID],
                start_date=now,
                end_date=end,
                labels=["full"],
            )
            sprint = await service.create_sprint(BOARD_ID, AGENT_ID, data)

        assert sprint is not None
        assert sprint.title == "Full Schema Sprint"
        assert sprint.description == "Full description"
        assert sprint.objective == "Full objective"
        assert sprint.expected_outcome == "Full outcome"
        assert sprint.test_scenario_ids == [TS_1_ID]
        assert sprint.business_rule_ids == [BR_1_ID]
        assert sprint.labels == ["full"]

    async def test_creation_invalid_test_scenario_ids(self, db_factory):
        """Sprint creation with test scenario IDs not in spec should raise."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="Bad TS", spec_id=SPEC_ID, test_scenario_ids=["nonexistent-ts"])
            with pytest.raises(ValueError, match="Test scenario IDs not found in spec"):
                await service.create_sprint(BOARD_ID, AGENT_ID, data)

    async def test_creation_invalid_business_rule_ids(self, db_factory):
        """Sprint creation with business rule IDs not in spec should raise."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="Bad BR", spec_id=SPEC_ID, business_rule_ids=["nonexistent-br"])
            with pytest.raises(ValueError, match="Business rule IDs not found in spec"):
                await service.create_sprint(BOARD_ID, AGENT_ID, data)

    async def test_creation_invalid_spec(self, db_factory):
        """Sprint creation with non-existent spec should return None."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="No spec", spec_id="nonexistent-spec")
            sprint = await service.create_sprint(BOARD_ID, AGENT_ID, data)
            assert sprint is None

    async def test_creation_invalid_board(self, db_factory):
        """Sprint creation with non-existent board should return None."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="No board", spec_id=SPEC_ID)
            sprint = await service.create_sprint("nonexistent-board", AGENT_ID, data)
            assert sprint is None


# ============================================================================
# Sprint Update Tests
# ============================================================================


class TestSprintUpdate:
    """Tests for sprint updates."""

    async def _create_sprint(self, db_factory, title="Test Sprint"):
        """Helper to create a sprint for update tests."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title=title,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        return sprint.id

    async def test_update_title_description_labels(self, db_factory):
        """Test 5: Update title, description, and labels."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(
                title="Updated Title",
                description="Updated description",
                labels=["updated", "label"],
            )
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.title == "Updated Title"
        assert sprint.description == "Updated description"
        assert sprint.labels == ["updated", "label"]
        assert sprint.version == 2  # title is a content field

    async def test_update_test_scenario_ids_add(self, db_factory):
        """Test 6: Add test_scenario_ids to sprint."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(test_scenario_ids=[TS_1_ID, TS_2_ID])
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.test_scenario_ids == [TS_1_ID, TS_2_ID]
        assert sprint.version == 2

    async def test_update_test_scenario_ids_remove(self, db_factory):
        """Test 6b: Remove test_scenario_ids from sprint."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="With TS",
                test_scenario_ids=[TS_1_ID, TS_2_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(test_scenario_ids=[TS_1_ID])
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.test_scenario_ids == [TS_1_ID]
        assert sprint.version == 2

    async def test_update_test_scenario_ids_clear(self, db_factory):
        """Test 6c: Clear all test_scenario_ids from sprint."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="With TS",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(test_scenario_ids=[])
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.test_scenario_ids == []
        assert sprint.version == 2

    async def test_update_business_rule_ids_add(self, db_factory):
        """Test 7: Add business_rule_ids to sprint."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(business_rule_ids=[BR_1_ID])
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.business_rule_ids == [BR_1_ID]
        assert sprint.version == 2

    async def test_update_business_rule_ids_remove(self, db_factory):
        """Test 7b: Remove business_rule_ids from sprint."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="With BR",
                business_rule_ids=[BR_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(business_rule_ids=[])
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.business_rule_ids == []
        assert sprint.version == 2

    async def test_update_nonexistent_sprint(self, db_factory):
        """Update a non-existent sprint should return None."""
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(title="Nope")
            sprint = await service.update_sprint("nonexistent-sprint", AGENT_ID, data)
            assert sprint is None

    async def test_update_invalid_test_scenario_ids(self, db_factory):
        """Update with test scenario IDs not in spec should raise."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(test_scenario_ids=["invalid-ts"])
            with pytest.raises(ValueError, match="Test scenario IDs not found in spec"):
                await service.update_sprint(sprint_id, AGENT_ID, data)

    async def test_update_invalid_business_rule_ids(self, db_factory):
        """Update with business rule IDs not in spec should raise."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(business_rule_ids=["invalid-br"])
            with pytest.raises(ValueError, match="Business rule IDs not found in spec"):
                await service.update_sprint(sprint_id, AGENT_ID, data)

    async def test_update_skip_flags(self, db_factory):
        """Update skip_test_coverage and skip_rules_coverage flags."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(
                skip_test_coverage=True,
                skip_rules_coverage=True,
                skip_qualitative_validation=True,
                validation_threshold=85,
            )
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.skip_test_coverage is True
        assert sprint.skip_rules_coverage is True
        assert sprint.skip_qualitative_validation is True
        assert sprint.validation_threshold == 85
        assert sprint.version == 1  # skip flags are not content fields


# ============================================================================
# Sprint State Machine Tests
# ============================================================================


class TestSprintStateMachine:
    """Tests for sprint state machine transitions with gates."""

    async def _create_sprint(self, db_factory, title="Test Sprint", **kwargs):
        """Helper to create a sprint and return its ID."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title=title,
                created_by=AGENT_ID,
                **kwargs,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        return sprint.id

    async def test_transition_draft_to_active_no_cards_fails(self, db_factory):
        """Test 8: draft → active should fail without cards assigned."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintMove(status=SprintStatus.ACTIVE)
            with pytest.raises(ValueError, match="no cards assigned"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_draft_to_active_with_cards_succeeds(self, db_factory):
        """Test 9: draft → active should succeed with cards assigned."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign cards first
            count = await service.assign_tasks(sprint_id, [CARD_1_ID, CARD_2_ID], AGENT_ID)
            assert count == 2
            # Now move to active
            data = SprintMove(status=SprintStatus.ACTIVE)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.ACTIVE

    async def test_transition_active_to_review_no_coverage_fails(self, db_factory):
        """Test 10: active → review should fail without test coverage."""
        sprint_id = await self._create_sprint(
            db_factory,
            test_scenario_ids=[TS_1_ID],
        )
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign a card so we can activate
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            # Move to active
            data = SprintMove(status=SprintStatus.ACTIVE)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)
            assert sprint.status == SprintStatus.ACTIVE

            # Now try to move to review — should fail because TS_1_ID is not passed
            data = SprintMove(status=SprintStatus.REVIEW)
            with pytest.raises(ValueError, match="not passed"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_active_to_review_with_coverage_succeeds(self, db_factory):
        """Test 11: active → review should succeed when scoped test scenarios are passed."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Coverage Sprint",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign card and activate
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Update the test scenario to "passed" status
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                   ts["status"] = "passed"
            flag_modified(spec, "test_scenarios")
            await db.commit()

            # Move to review — should succeed
            data = SprintMove(status=SprintStatus.REVIEW)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.REVIEW

    async def test_transition_review_to_closed_no_evaluation_fails(self, db_factory):
        """Test 12: review → closed should fail without evaluation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Review Sprint",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Activate
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Update scenario to passed
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"

            flag_modified(spec, "test_scenarios")
            await db.commit()

            # Move to review
            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Try to close without evaluation — should fail
            data = SprintMove(status=SprintStatus.CLOSED)
            with pytest.raises(ValueError, match="no evaluation"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_review_to_closed_with_approve_succeeds(self, db_factory):
        """Test 13: review → closed should succeed with approve evaluation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Close Sprint",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Activate
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Update scenario to passed
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"

            flag_modified(spec, "test_scenarios")
            await db.commit()

            # Move to review
            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Submit evaluation with approve
            evaluation = {
                "breakdown_completeness": 90,
                "breakdown_justification": "Tasks cover the sprint well",
                "granularity": 85,
                "granularity_justification": "Tasks are properly sized",
                "dependency_coherence": 80,
                "dependency_justification": "Dependencies make sense",
                "test_coverage_quality": 85,
                "test_coverage_justification": "Tests cover happy path",
                "overall_score": 85,
                "overall_justification": "Good overall quality",
                "recommendation": "approve",
            }
            await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

            # Now close — should succeed
            data = SprintMove(status=SprintStatus.CLOSED)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.CLOSED

    async def test_transition_review_to_closed_reject_prevents_close(self, db_factory):
        """Closing with a reject evaluation should fail."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Reject Sprint",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"

            flag_modified(spec, "test_scenarios")
            await db.commit()

            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            evaluation = {
                "breakdown_completeness": 30,
                "breakdown_justification": "Poor coverage",
                "granularity": 30,
                "granularity_justification": "Tasks too big",
                "dependency_coherence": 30,
                "dependency_justification": "Dependencies messy",
                "test_coverage_quality": 30,
                "test_coverage_justification": "No tests",
                "overall_score": 30,
                "overall_justification": "Needs work",
                "recommendation": "reject",
            }
            await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

            data = SprintMove(status=SprintStatus.CLOSED)
            with pytest.raises(ValueError, match="reject"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_invalid_draft_to_review(self, db_factory):
        """draft → review directly should fail (must go through active)."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintMove(status=SprintStatus.REVIEW)
            with pytest.raises(ValueError, match="Cannot move sprint"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_invalid_active_to_closed(self, db_factory):
        """active → closed directly should fail (must go through review)."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            data = SprintMove(status=SprintStatus.CLOSED)
            with pytest.raises(ValueError, match="Cannot move sprint"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_transition_draft_to_cancelled(self, db_factory):
        """draft → cancelled should always succeed."""
        sprint_id = await self._create_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintMove(status=SprintStatus.CANCELLED)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.CANCELLED

    async def test_transition_review_to_active(self, db_factory):
        """review → active should succeed (re-open sprint)."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Reopen Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            data = SprintMove(status=SprintStatus.ACTIVE)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.ACTIVE


# ============================================================================
# Sprint Evaluation Tests
# ============================================================================


class TestSprintEvaluation:
    """Tests for sprint evaluation submission."""

    async def _create_review_sprint(self, db_factory):
        """Helper: create and move a sprint to review status."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            # Ensure TS_1_ID is "passed" for review gate
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"
            flag_modified(spec, "test_scenarios")
            await db.commit()

            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Eval Sprint",
                test_scenario_ids=[TS_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Verify the sprint is in review status
            sprint = await db.get(Sprint, sprint_id)
            print(f"DEBUG helper: sprint_id={sprint_id}, status_after_review={sprint.status}")
            assert sprint.status == SprintStatus.REVIEW, f"Sprint is {sprint.status}, expected REVIEW"

        return sprint_id

    async def test_evaluation_submit_all_dimensions(self, db_factory):
        """Test 14: Submit evaluation with all dimensions."""
        sprint_id = await self._create_review_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            # Debug: check sprint status before submit_evaluation
            sprint_before = await db.get(Sprint, sprint_id)
            print(f"DEBUG: sprint_id={sprint_id}, status_before={sprint_before.status if sprint_before else 'NOT FOUND'}")
            service = SprintService(db)
            evaluation = {
                "breakdown_completeness": 90,
                "breakdown_justification": "Tasks cover the sprint scope well",
                "granularity": 85,
                "granularity_justification": "Tasks are properly sized",
                "dependency_coherence": 80,
                "dependency_justification": "Dependencies are logical",
                "test_coverage_quality": 85,
                "test_coverage_justification": "Tests cover happy path and edge cases",
                "overall_score": 85,
                "overall_justification": "High quality sprint with good coverage",
                "recommendation": "approve",
            }
            await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)
            # Refresh to get the evaluations field
            sprint = await db.get(Sprint, sprint_id)

        assert sprint is not None
        assert len(sprint.evaluations) == 1
        ev = sprint.evaluations[0]
        assert ev["breakdown_completeness"] == 90
        assert ev["granularity"] == 85
        assert ev["dependency_coherence"] == 80
        assert ev["test_coverage_quality"] == 85
        assert ev["overall_score"] == 85
        assert ev["recommendation"] == "approve"
        assert ev["evaluator_id"] == AGENT_ID
        assert ev["stale"] is False

    async def test_evaluation_submit_low_scores(self, db_factory):
        """Test 15: Submit evaluation with low scores — should not auto-close."""
        sprint_id = await self._create_review_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            evaluation = {
                "breakdown_completeness": 30,
                "breakdown_justification": "Incomplete",
                "granularity": 20,
                "granularity_justification": "Tasks too coarse",
                "dependency_coherence": 25,
                "dependency_justification": "Messy dependencies",
                "test_coverage_quality": 15,
                "test_coverage_justification": "Minimal test coverage",
                "overall_score": 25,
                "overall_justification": "Needs significant work",
                "recommendation": "reject",
            }
            await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

            # Sprint should still be in review, not auto-closed
            sprint = await db.get(Sprint, sprint_id)
            assert sprint.status == SprintStatus.REVIEW

            # Closing should fail because of reject recommendation
            data = SprintMove(status=SprintStatus.CLOSED)
            with pytest.raises(ValueError, match="reject"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_evaluation_submit_below_threshold(self, db_factory):
        """Evaluation with overall_score below threshold should prevent closing."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Threshold Sprint",
                test_scenario_ids=[TS_1_ID],
                validation_threshold=80,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"

            flag_modified(spec, "test_scenarios")
            await db.commit()

            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Submit approve but below threshold
            evaluation = {
                "breakdown_completeness": 60,
                "breakdown_justification": "OK",
                "granularity": 60,
                "granularity_justification": "OK",
                "dependency_coherence": 60,
                "dependency_justification": "OK",
                "test_coverage_quality": 60,
                "test_coverage_justification": "OK",
                "overall_score": 60,
                "overall_justification": "Below threshold",
                "recommendation": "approve",
            }
            await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

            # Closing should fail — score 60 < threshold 80
            data = SprintMove(status=SprintStatus.CLOSED)
            with pytest.raises(ValueError, match="below threshold"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_evaluation_only_in_review_status(self, db_factory):
        """Submitting evaluation to non-review sprint should raise."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Draft Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            evaluation = {
                "breakdown_completeness": 90,
                "breakdown_justification": "OK",
                "granularity": 90,
                "granularity_justification": "OK",
                "dependency_coherence": 90,
                "dependency_justification": "OK",
                "test_coverage_quality": 90,
                "test_coverage_justification": "OK",
                "overall_score": 90,
                "overall_justification": "OK",
                "recommendation": "approve",
            }
            with pytest.raises(ValueError, match="review"):
                await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

    async def test_evaluation_multiple(self, db_factory):
        """Multiple evaluations can be submitted for the same sprint."""
        sprint_id = await self._create_review_sprint(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            for i in range(3):
                evaluation = {
                    "breakdown_completeness": 80 + i * 5,
                    "breakdown_justification": f"Justification {i}",
                    "granularity": 80 + i * 5,
                    "granularity_justification": f"Justification {i}",
                    "dependency_coherence": 80 + i * 5,
                    "dependency_justification": f"Justification {i}",
                    "test_coverage_quality": 80 + i * 5,
                    "test_coverage_justification": f"Justification {i}",
                    "overall_score": 80 + i * 5,
                    "overall_justification": f"Justification {i}",
                    "recommendation": "approve",
                }
                await service.submit_evaluation(sprint_id, AGENT_ID, evaluation)

            sprint = await db.get(Sprint, sprint_id)
            assert len(sprint.evaluations) == 3
            for ev in sprint.evaluations:
                assert ev["evaluator_id"] == AGENT_ID
                assert ev["stale"] is False


# ============================================================================
# Sprint Card Assignment Tests
# ============================================================================


class TestSprintCardAssignment:
    """Tests for sprint card assignment."""

    async def test_assign_cards_to_sprint(self, db_factory):
        """Test 16: Assign cards to sprint."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Assignment Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            count = await service.assign_tasks(sprint_id, [CARD_1_ID, CARD_2_ID], AGENT_ID)
            assert count == 2

            # Verify cards are linked
            card1 = await db.get(Card, CARD_1_ID)
            card2 = await db.get(Card, CARD_2_ID)
            assert card1.sprint_id == sprint_id
            assert card2.sprint_id == sprint_id

    async def test_assign_cards_from_different_spec_fails(self, db_factory):
        """Test 17: Assign cards from a different spec should fail."""
        await _seed_board(db_factory)
        await _seed_different_spec(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Cross-spec Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            with pytest.raises(ValueError, match="different spec"):
                await service.assign_tasks(sprint_id, [CARD_3_ID], AGENT_ID)

    async def test_assign_nonexistent_card(self, db_factory):
        """Assigning a non-existent card should be silently skipped."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Missing Card Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            count = await service.assign_tasks(sprint_id, ["nonexistent-card"], AGENT_ID)
            assert count == 0

    async def test_assign_mixed_valid_invalid_cards(self, db_factory):
        """Assigning a mix of valid and non-existent cards — valid ones succeed."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Mixed Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            count = await service.assign_tasks(sprint_id, [CARD_1_ID, "nonexistent-card"], AGENT_ID)
            assert count == 1

            card = await db.get(Card, CARD_1_ID)
            assert card.sprint_id == sprint_id


# ============================================================================
# Sprint Listing Tests
# ============================================================================


class TestSprintListing:
    """Tests for sprint listing."""

    async def test_list_sprints_by_status(self, db_factory):
        """Test 18: List sprints filtered by status."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            sprint1 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Draft Sprint",
                status=SprintStatus.DRAFT,
                created_by=AGENT_ID,
            )
            sprint2 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Active Sprint",
                status=SprintStatus.ACTIVE,
                created_by=AGENT_ID,
            )
            sprint3 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Review Sprint",
                status=SprintStatus.REVIEW,
                created_by=AGENT_ID,
            )
            db.add_all([sprint1, sprint2, sprint3])
            await db.commit()

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            draft_sprints = await service.list_board_sprints(BOARD_ID, status_filter="draft")
            assert len(draft_sprints) == 1
            assert draft_sprints[0].title == "Draft Sprint"

            active_sprints = await service.list_board_sprints(BOARD_ID, status_filter="active")
            assert len(active_sprints) == 1
            assert active_sprints[0].status == SprintStatus.ACTIVE

    async def test_list_sprints_by_spec_id(self, db_factory):
        """Test 19: List sprints by spec_id filter."""
        await _seed_board(db_factory)
        await _seed_different_spec(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            sprint1 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Sprint for Spec 1",
                created_by=AGENT_ID,
            )
            sprint2 = Sprint(
                board_id=BOARD_ID,
                spec_id="sprint-crud-spec-diff",
                title="Sprint for Spec 2",
                created_by=AGENT_ID,
            )
            db.add_all([sprint1, sprint2])
            await db.commit()

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # List by first spec
            spec1_sprints = await service.list_board_sprints(BOARD_ID, spec_id=SPEC_ID)
            assert len(spec1_sprints) == 1
            assert spec1_sprints[0].title == "Sprint for Spec 1"

            # List by second spec
            spec2_sprints = await service.list_board_sprints(BOARD_ID, spec_id="sprint-crud-spec-diff")
            assert len(spec2_sprints) == 1
            assert spec2_sprints[0].title == "Sprint for Spec 2"

    async def test_list_sprints_empty(self, db_factory):
        """List sprints when none exist."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            sprints = await service.list_board_sprints(BOARD_ID)
            assert sprints == []

    async def test_list_sprints_ordered_by_created_at(self, db_factory):
        """Sprints should be ordered by created_at ascending."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            sprint1 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="First Sprint",
                created_by=AGENT_ID,
            )
            sprint2 = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Second Sprint",
                created_by=AGENT_ID,
            )
            db.add_all([sprint1, sprint2])
            await db.commit()

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            sprints = await service.list_board_sprints(BOARD_ID, spec_id=SPEC_ID)
            assert len(sprints) == 2
            assert sprints[0].title == "First Sprint"
            assert sprints[1].title == "Second Sprint"


# ============================================================================
# Sprint Retrieval Tests
# ============================================================================


class TestSprintRetrieval:
    """Tests for sprint retrieval."""

    async def test_get_sprint_full_details(self, db_factory):
        """Test 24: Retrieve sprint with full details."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Get Sprint",
                description="A sprint for testing retrieval",
                objective="Test get",
                expected_outcome="Success",
                start_date=datetime.now(timezone.utc),
                end_date=datetime.now(timezone.utc) + timedelta(days=14),
                labels=["test", "retrieval"],
                test_scenario_ids=[TS_1_ID],
                business_rule_ids=[BR_1_ID],
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)

        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            sprint = await service.get_sprint(sprint_id)

        assert sprint is not None
        assert sprint.id == sprint_id
        assert sprint.title == "Get Sprint"
        assert sprint.description == "A sprint for testing retrieval"
        assert sprint.objective == "Test get"
        assert sprint.expected_outcome == "Success"
        assert sprint.labels == ["test", "retrieval"]
        assert sprint.test_scenario_ids == [TS_1_ID]
        assert sprint.business_rule_ids == [BR_1_ID]
        assert sprint.spec_id == SPEC_ID
        assert sprint.board_id == BOARD_ID

    async def test_get_nonexistent_sprint(self, db_factory):
        """Test 25: Retrieve non-existent sprint should return None."""
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            sprint = await service.get_sprint("nonexistent-sprint-id")
            assert sprint is None


# ============================================================================
# Sprint History Tests
# ============================================================================


class TestSprintHistory:
    """Tests for sprint history logging."""

    async def test_history_logged_on_status_changes(self, db_factory):
        """Test 26: Verify history is logged on status changes."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="History Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign card for activation
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)

            # Move to active
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # History should have entries
            history = await service.list_history(sprint_id)
            action_types = [h.action for h in history]
            assert "status_changed" in action_types
            assert "tasks_assigned" in action_types

            # Move to cancelled
            data = SprintMove(status=SprintStatus.CANCELLED)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            history = await service.list_history(sprint_id)
            action_types = [h.action for h in history]
            assert "status_changed" in action_types

            # Check history entry details
            status_changes = [h for h in history if h.action == "status_changed"]
            assert len(status_changes) >= 2
            for change in status_changes:
                assert change.sprint_id == sprint_id
                assert change.actor_id == AGENT_ID
                assert change.actor_type == "user"
                assert change.summary is not None
                assert "→" in change.summary

    async def test_history_logged_on_update(self, db_factory):
        """History should be logged on sprint updates."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Original Title",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(title="Updated Title", description="New desc")
            await service.update_sprint(sprint_id, AGENT_ID, data)

            history = await service.list_history(sprint_id)
            action_types = [h.action for h in history]
            assert "updated" in action_types

            update_entry = [h for h in history if h.action == "updated"][0]
            assert update_entry.summary is not None
            assert "Updated" in update_entry.summary

    async def test_history_logged_on_creation(self, db_factory):
        """History should be logged on sprint creation."""
        await _seed_board(db_factory)
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintCreate(title="History Test Sprint", spec_id=SPEC_ID)
            sprint = await service.create_sprint(BOARD_ID, AGENT_ID, data)

            history = await service.list_history(sprint.id)
            action_types = [h.action for h in history]
            assert "created" in action_types


# ============================================================================
# Sprint Deletion Tests
# ============================================================================


class TestSprintDeletion:
    """Tests for sprint deletion."""

    async def test_delete_sprint_in_draft(self, db_factory):
        """Test 20: Delete sprint in draft status."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Delete Draft Sprint",
                status=SprintStatus.DRAFT,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            result = await service.delete_sprint(sprint_id, AGENT_ID)
            assert result is True

            # Verify deletion
            retrieved = await service.get_sprint(sprint_id)
            assert retrieved is None

    async def test_delete_sprint_in_active(self, db_factory):
        """Test 21: Delete sprint in active status."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Delete Active Sprint",
                status=SprintStatus.ACTIVE,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            result = await service.delete_sprint(sprint_id, AGENT_ID)
            assert result is True

            retrieved = await service.get_sprint(sprint_id)
            assert retrieved is None

    async def test_delete_sprint_in_review(self, db_factory):
        """Test 22: Delete sprint in review status."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Delete Review Sprint",
                status=SprintStatus.REVIEW,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            result = await service.delete_sprint(sprint_id, AGENT_ID)
            assert result is True

            retrieved = await service.get_sprint(sprint_id)
            assert retrieved is None

    async def test_delete_nonexistent_sprint(self, db_factory):
        """Test 23: Delete non-existent sprint should return False."""
        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            result = await service.delete_sprint("nonexistent-sprint-id", AGENT_ID)
            assert result is False

    async def test_delete_unlinks_cards(self, db_factory):
        """Deleting a sprint should unlink cards (set sprint_id to null)."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Unlink Sprint",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign cards
            await service.assign_tasks(sprint_id, [CARD_1_ID, CARD_2_ID], AGENT_ID)

            card1 = await db.get(Card, CARD_1_ID)
            assert card1.sprint_id == sprint_id

            # Delete sprint
            await service.delete_sprint(sprint_id, AGENT_ID)

            # Cards should be unlinked
            card1 = await db.get(Card, CARD_1_ID)
            card2 = await db.get(Card, CARD_2_ID)
            assert card1.sprint_id is None
            assert card2.sprint_id is None


# ============================================================================
# Skip Flags Tests
# ============================================================================


class TestSprintSkipFlags:
    """Tests for skip_test_coverage and skip_rules_coverage flags."""

    async def test_skip_test_coverage_allows_review(self, db_factory):
        """Test 27: Sprint can move to review without test coverage if skip_test_coverage is set."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Skip Coverage Sprint",
                test_scenario_ids=[TS_1_ID],
                skip_test_coverage=True,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            # Assign card and activate
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # TS_1_ID is still "draft" (not passed), but skip flag should allow review
            data = SprintMove(status=SprintStatus.REVIEW)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.REVIEW

    async def test_skip_rules_coverage_does_not_affect_test_gate(self, db_factory):
        """skip_rules_coverage should not bypass the test coverage gate."""
        await _seed_board(db_factory)
        await _clean_sprints(db_factory, BOARD_ID)
        async with db_factory() as db:
            # Ensure TS_1_ID is "draft" (not passed)
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "draft"
            flag_modified(spec, "test_scenarios")
            await db.commit()

            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Skip Rules Sprint",
                test_scenario_ids=[TS_1_ID],
                skip_rules_coverage=True,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # TS_1_ID is not passed — should still fail even with skip_rules_coverage
            data = SprintMove(status=SprintStatus.REVIEW)
            with pytest.raises(ValueError, match="not passed"):
                await service.move_sprint(sprint_id, AGENT_ID, data)

    async def test_skip_qualitative_validation_allows_close(self, db_factory):
        """skip_qualitative_validation should allow closing without evaluation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Skip Qual Sprint",
                test_scenario_ids=[TS_1_ID],
                skip_qualitative_validation=True,
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            await service.assign_tasks(sprint_id, [CARD_1_ID], AGENT_ID)
            data = SprintMove(status=SprintStatus.ACTIVE)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Update scenario to passed
            spec = await db.get(Spec, SPEC_ID)
            for ts in (spec.test_scenarios or []):
                if ts.get("id") == TS_1_ID:
                    ts["status"] = "passed"

            flag_modified(spec, "test_scenarios")
            await db.commit()

            # Move to review
            data = SprintMove(status=SprintStatus.REVIEW)
            await service.move_sprint(sprint_id, AGENT_ID, data)

            # Close without evaluation — should succeed because skip_qualitative_validation
            data = SprintMove(status=SprintStatus.CLOSED)
            sprint = await service.move_sprint(sprint_id, AGENT_ID, data)

        assert sprint.status == SprintStatus.CLOSED

    async def test_skip_flags_via_update(self, db_factory):
        """Skip flags can be set via SprintUpdate."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            sprint = Sprint(
                board_id=BOARD_ID,
                spec_id=SPEC_ID,
                title="Update Skip Flags",
                created_by=AGENT_ID,
            )
            db.add(sprint)
            await db.commit()
            await db.refresh(sprint)
        sprint_id = sprint.id

        from okto_pulse.core.services.main import SprintService
        async with db_factory() as db:
            service = SprintService(db)
            data = SprintUpdate(
                skip_test_coverage=True,
                skip_rules_coverage=True,
                skip_qualitative_validation=True,
            )
            sprint = await service.update_sprint(sprint_id, AGENT_ID, data)

        assert sprint.skip_test_coverage is True
        assert sprint.skip_rules_coverage is True
        assert sprint.skip_qualitative_validation is True
