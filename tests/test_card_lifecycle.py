"""Comprehensive tests for card lifecycle operations.

Covers the full card state machine, CRUD, dependencies, and activity logging.

State machine:
    not_started → started → in_progress → validation → done

Rules:
    - Moving execution work to 'validation' or 'done' requires: conclusion, completeness (0-100),
      completeness_justification, drift (0-100), drift_justification
    - Circular dependencies are blocked
    - Bug cards require origin_task_id, severity, expected_behavior, observed_behavior
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardPriority,
    CardStatus,
    CardType,
    DomainEventRow,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate
from okto_pulse.core.domain.knowledge_selection import KnowledgeSelectionState
from okto_pulse.core.ports.knowledge_propagation import KnowledgePropagationScope
from okto_pulse.core.services import main as main_service
from okto_pulse.core.services.main import (
    CardOperationError,
    CardResourceReadOnlyError,
    CardService,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgePropagationServiceError,
)
from okto_pulse.core.services.resource_gate import ResourceGateService


BOARD_ID = "card-lifecycle-board-001"
AGENT_ID = "card-lifecycle-agent-001"
USER_ID = AGENT_ID


class _CardKnowledgeScopePort:
    def __init__(self, *, v2_active: bool) -> None:
        self.v2_active = v2_active

    async def load_scope(self, _context, request):
        return KnowledgePropagationScope(
            target=request.target,
            scope_revision=1 if self.v2_active else 0,
            v2_active=self.v2_active,
            selection_state=(
                KnowledgeSelectionState.OMITTED if self.v2_active else None
            ),
        )


async def _mark_all_resources_na(db, entity_type: str, entity_id: str) -> None:
    service = ResourceGateService(db)
    for resource_type in ("architecture", "mockup", "knowledge_base"):
        await service.mark_not_applicable(
            BOARD_ID,
            entity_type,
            entity_id,
            resource_type,
            USER_ID,
            justification=f"{resource_type} is intentionally not applicable in this lifecycle test.",
            source_channel="ui",
        )


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

    @pytest.mark.parametrize("card_type", ["normal", "test"])
    async def test_all_card_types_reject_advanced_initial_status(
        self,
        db_factory,
        card_type,
    ):
        """Normal and test cards cannot bypass lifecycle gates during creation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            spec = (
                await db.execute(select(Spec).where(Spec.board_id == BOARD_ID))
            ).scalars().first()
            data = CardCreate.model_construct(
                title=f"Invalid initial {card_type} card",
                status=CardStatus.DONE,
                card_type=card_type,
                spec_id=spec.id,
                test_scenario_ids=["ts-001"] if card_type == "test" else None,
            )

            with pytest.raises(CardOperationError) as exc_info:
                await CardService(db).create_card(BOARD_ID, USER_ID, data)

            assert exc_info.value.code == "card_initial_status_invalid"
            assert exc_info.value.facts["requested_status"] == "done"


# ============================================================================
# 2. Card status transitions
# ============================================================================


@pytest.mark.asyncio
class TestCardStatusTransitionMatrix:
    """AC-2: Card status transitions through the state machine."""

    async def _create_card_for_transition(
        self,
        db_factory,
        status=CardStatus.NOT_STARTED,
        *,
        require_task_validation: bool = False,
    ):
        """Helper: create a card in a given status for transition testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            actual_spec_id = specs[0].id
            specs[0].status = SpecStatus.IN_PROGRESS
            specs[0].require_task_validation = require_task_validation

            data = CardCreate(
                title=f"Transition Card ({status.value})",
                status=CardStatus.NOT_STARTED,
                spec_id=actual_spec_id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            # The transition suite needs fixtures at every source state. Seed
            # that state directly so production creation stays fail-closed.
            card.status = status
            await db.commit()
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

    async def test_same_status_move_is_idempotent_reorder(self, db_factory):
        """A lateral move preserves status while accepting a new position."""
        card, _ = await self._create_card_for_transition(
            db_factory, CardStatus.NOT_STARTED
        )
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id,
                USER_ID,
                CardMove(status=CardStatus.NOT_STARTED, position=0),
            )
            assert moved is not None
            assert moved.status == CardStatus.NOT_STARTED
            assert moved.position == 0

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
        """in_progress → validation persists the executor's completion report."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.IN_PROGRESS)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Implemented the planned task and prepared it for validation",
                    completeness=95,
                    completeness_justification="All required behavior was implemented; polishing remains",
                    drift=5,
                    drift_justification="Minor naming adjustment during implementation",
                ),
            )
            assert moved is not None
            assert moved.status == CardStatus.VALIDATION
            assert moved.conclusions is not None
            assert len(moved.conclusions) == 1
            assert moved.conclusions[0]["source"] == "move_to_validation"
            assert moved.conclusions[0]["completeness"] == 95
            assert moved.conclusions[0]["drift"] == 5

    async def test_transition_in_progress_to_validation_requires_conclusion(self, db_factory):
        """in_progress → validation without an execution report is rejected."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.IN_PROGRESS)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="conclusion"):
                await svc.move_card(
                    card.id, USER_ID,
                    CardMove(status=CardStatus.VALIDATION),
                )

    async def test_successful_task_validation_does_not_duplicate_executor_report(self, db_factory):
        """Reviewer approval keeps the executor report as the single conclusion entry."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.IN_PROGRESS)
        async with db_factory() as db:
            svc = CardService(db)
            await svc.move_card(
                card.id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Executor claim for validator review",
                    completeness=100,
                    completeness_justification="All acceptance criteria implemented",
                    drift=0,
                    drift_justification="No deviation from plan",
                ),
            )
            await db.commit()
            await _mark_all_resources_na(db, "card", card.id)
            result = await svc.submit_task_validation(
                card.id,
                "reviewer-1",
                "Reviewer One",
                {
                    "confidence": 95,
                    "confidence_justification": "Reviewed implementation and tests",
                    "estimated_completeness": 100,
                    "completeness_justification": "Everything requested is present",
                    "estimated_drift": 0,
                    "drift_justification": "Implementation follows the plan",
                    "general_justification": "Approved after reviewing the executor report and delivered changes.",
                    "recommendation": "approve",
                },
            )
            await db.commit()

            persisted = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.id == card.id)
            )).scalar_one()
            assert result["outcome"] == "success"
            assert persisted.status == CardStatus.DONE
            assert len(persisted.validations or []) == 1
            assert persisted.validations[0]["resolved_thresholds"] == {
                "required": False,
                "min_confidence": 70,
                "min_completeness": 80,
                "max_drift": 50,
                "resolved_from": "spec",
                "resolved_sources": {
                    "required": "spec",
                    "min_confidence": "board",
                    "min_completeness": "board",
                    "max_drift": "board",
                },
            }
            assert result["resolved_thresholds"] == persisted.validations[0][
                "resolved_thresholds"
            ]
            assert len(persisted.conclusions or []) == 1
            assert persisted.conclusions[0]["source"] == "move_to_validation"
            assert persisted.conclusions[0]["text"] == "Executor claim for validator review"

    async def test_validation_config_tracks_mixed_override_sources(self, db_factory):
        """Every independently resolved threshold retains accurate provenance."""
        from types import SimpleNamespace

        async with db_factory() as db:
            config = CardService(db)._resolve_validation_config(
                SimpleNamespace(),
                SimpleNamespace(
                    require_task_validation=False,
                    validation_min_confidence=75,
                    validation_min_completeness=None,
                    validation_max_drift=30,
                ),
                SimpleNamespace(
                    require_task_validation=None,
                    validation_min_confidence=90,
                    validation_min_completeness=88,
                    validation_max_drift=None,
                ),
                {
                    "require_task_validation": True,
                    "min_confidence": 70,
                    "min_completeness": 80,
                    "max_drift": 50,
                },
            )

        assert config == {
            "required": False,
            "min_confidence": 90,
            "min_completeness": 88,
            "max_drift": 30,
            "resolved_from": "spec",
            "resolved_sources": {
                "required": "spec",
                "min_confidence": "sprint",
                "min_completeness": "sprint",
                "max_drift": "spec",
            },
        }

    async def test_transition_validation_to_done_with_required_fields(self, db_factory):
        """validation → done remains available when task validation is disabled."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.VALIDATION)
        async with db_factory() as db:
            svc = CardService(db)
            await _mark_all_resources_na(db, "card", card.id)
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

    async def test_direct_validation_to_done_cannot_bypass_required_gate(
        self,
        db_factory,
    ):
        card, _ = await self._create_card_for_transition(
            db_factory,
            CardStatus.VALIDATION,
            require_task_validation=True,
        )
        async with db_factory() as db:
            with pytest.raises(ValueError, match="submit_task_validation"):
                await CardService(db).move_card(
                    card.id,
                    USER_ID,
                    CardMove(
                        status=CardStatus.DONE,
                        conclusion="Attempted direct completion",
                        completeness=100,
                        completeness_justification="Complete",
                        drift=0,
                        drift_justification="No deviation",
                    ),
                )

    async def test_transition_done_backward_to_in_progress(self, db_factory):
        """done → in_progress is the canonical backward transition."""
        card, _ = await self._create_card_for_transition(db_factory, CardStatus.DONE)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            assert moved is not None
            assert moved.status == CardStatus.IN_PROGRESS

    async def test_invalid_transition_not_started_to_done_raises(self, db_factory):
        """not_started → done without required fields must raise ValueError."""
        card, _ = await self._create_card_for_transition(
            db_factory,
            CardStatus.IN_PROGRESS,
        )
        async with db_factory() as db:
            svc = CardService(db)
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
            await _mark_all_resources_na(db, "card", card.id)
            # First completion
            moved = await svc.move_card(
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
            # Reopen through the canonical reverse path.
            await svc.move_card(
                card.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            # Second completion
            moved = await svc.move_card(
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
            assert moved is not None
            assert len(moved.conclusions) == 2
            assert moved.conclusions[0]["text"] == "First completion"
            assert moved.conclusions[1]["text"] == "Second completion"


# ============================================================================
# 3. Card updates
# ============================================================================


@pytest.mark.asyncio
class TestCardValidationReportGate:
    """Regression coverage for executor reports before task validation."""

    async def _create_in_progress_card(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            card = await svc.create_card(
                BOARD_ID,
                USER_ID,
                CardCreate(
                    title="Validation report gate card",
                    status=CardStatus.STARTED,
                    spec_id=specs[0].id,
                ),
            )
            card.status = CardStatus.IN_PROGRESS
            await db.commit()
            return card

    async def test_move_to_validation_requires_executor_report(self, db_factory):
        card = await self._create_in_progress_card(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            with pytest.raises(ValueError, match="conclusion"):
                await svc.move_card(card.id, USER_ID, CardMove(status=CardStatus.VALIDATION))

    async def test_move_to_validation_persists_executor_report(self, db_factory):
        card = await self._create_in_progress_card(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            moved = await svc.move_card(
                card.id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Executor claim for validation",
                    completeness=95,
                    completeness_justification="All required behavior was implemented",
                    drift=5,
                    drift_justification="Minor adjustment during implementation",
                ),
            )

            assert moved is not None
            assert moved.status == CardStatus.VALIDATION
            assert len(moved.conclusions or []) == 1
            assert moved.conclusions[0]["source"] == "move_to_validation"
            assert moved.conclusions[0]["text"] == "Executor claim for validation"

    async def test_successful_validation_does_not_duplicate_executor_report(self, db_factory):
        card = await self._create_in_progress_card(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            await svc.move_card(
                card.id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Executor claim for validator review",
                    completeness=100,
                    completeness_justification="All acceptance criteria implemented",
                    drift=0,
                    drift_justification="No deviation from plan",
                ),
            )
            await db.commit()
            await _mark_all_resources_na(db, "card", card.id)
            result = await svc.submit_task_validation(
                card.id,
                "reviewer-1",
                "Reviewer One",
                {
                    "confidence": 95,
                    "confidence_justification": "Reviewed implementation and tests",
                    "estimated_completeness": 100,
                    "completeness_justification": "Everything requested is present",
                    "estimated_drift": 0,
                    "drift_justification": "Implementation follows the plan",
                    "general_justification": "Approved after reviewing the executor report and delivered changes.",
                    "recommendation": "approve",
                },
            )
            await db.commit()

            persisted = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.id == card.id)
            )).scalar_one()
            assert result["outcome"] == "success"
            assert persisted.status == CardStatus.DONE
            assert len(persisted.validations or []) == 1
            assert len(persisted.conclusions or []) == 1
            assert persisted.conclusions[0]["source"] == "move_to_validation"


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

    async def test_update_card_rejects_direct_status_change(self, db_factory):
        """CRUD updates cannot bypass move_card transition gates."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            card = (
                await db.execute(select(Card).where(Card.board_id == BOARD_ID))
            ).scalars().first()
            original_status = card.status

            with pytest.raises(CardOperationError) as exc_info:
                await CardService(db).update_card(
                    card.id,
                    USER_ID,
                    CardUpdate(status=CardStatus.DONE),
                )

            assert exc_info.value.code == "card_status_update_requires_move"
            assert card.status == original_status

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

    async def test_direct_resource_field_update_is_read_only_without_internal_flag(self, db_factory):
        """Card KB/mockup snapshots can only be refreshed by propagation/copy paths."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(
                db,
                knowledge_propagation_port=_CardKnowledgeScopePort(
                    v2_active=False
                ),
            )
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()

            with pytest.raises(CardResourceReadOnlyError):
                await svc.update_card(
                    card.id,
                    USER_ID,
                    CardUpdate(knowledge_bases=[{"id": "cardkb_direct"}]),
                )

            updated = await svc.update_card(
                card.id,
                USER_ID,
                CardUpdate(knowledge_bases=[{"id": "cardkb_copied"}]),
                allow_card_resource_write=True,
            )
            assert updated.knowledge_bases == [{"id": "cardkb_copied"}]

    async def test_internal_resource_write_is_forbidden_for_v2_target(
        self,
        db_factory,
    ):
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(
                db,
                knowledge_propagation_port=_CardKnowledgeScopePort(
                    v2_active=True
                ),
            )
            card = (
                await db.execute(
                    __import__("sqlalchemy")
                    .select(Card)
                    .where(Card.board_id == BOARD_ID)
                )
            ).scalars().first()
            before = list(card.knowledge_bases or [])

            with pytest.raises(KnowledgePropagationServiceError) as caught:
                await svc.update_card(
                    card.id,
                    USER_ID,
                    CardUpdate(
                        knowledge_bases=[{"id": "cardkb_forbidden"}]
                    ),
                    allow_card_resource_write=True,
                )

            assert (
                caught.value.code
                == "knowledge_propagation_legacy_write_forbidden"
            )
            await db.refresh(card)
            assert list(card.knowledge_bases or []) == before

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
# 4. Task validation evidence deletion
# ============================================================================


@pytest.mark.asyncio
class TestTaskValidationDeletion:
    async def test_completed_card_retains_last_required_success(self, db_factory):
        """Admitted validation attempts remain append-only causal history."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            card = (
                await db.execute(select(Card).where(Card.board_id == BOARD_ID))
            ).scalars().first()
            spec = await db.get(Spec, card.spec_id)
            spec.require_task_validation = True
            card.status = CardStatus.DONE
            card.validations = [
                {
                    "id": "validation-success",
                    "outcome": "success",
                    "reviewer_id": "reviewer-1",
                },
                {
                    "id": "validation-failed",
                    "outcome": "failed",
                    "reviewer_id": "reviewer-2",
                },
            ]

            service = CardService(db)
            for validation_id in ("validation-failed", "validation-success"):
                with pytest.raises(CardOperationError) as exc_info:
                    await service.delete_task_validation(
                        card.id,
                        validation_id,
                        USER_ID,
                    )
                assert (
                    exc_info.value.code
                    == "task_validation_history_append_only"
                )

            persisted = await service.get_card(card.id)
            assert [item["id"] for item in persisted.validations] == [
                "validation-success",
                "validation-failed",
            ]


# ============================================================================
# 5. Card dependencies
# ============================================================================


@pytest.mark.asyncio
class TestCardDependencies:
    """AC-4: Card dependency management."""

    async def _setup_dependency_test(self, db_factory):
        """Helper: seed board and return the two cards for dependency testing."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            spec = (
                await db.execute(
                    __import__("sqlalchemy")
                    .select(Spec)
                    .where(Spec.board_id == BOARD_ID)
                )
            ).scalars().first()
            card_a = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec.id,
                title=f"Dependency A {uuid.uuid4().hex[:8]}",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            card_b = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec.id,
                title=f"Dependency B {uuid.uuid4().hex[:8]}",
                status=CardStatus.STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add_all([card_a, card_b])
            await db.commit()
            return card_a, card_b

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
            with pytest.raises(CardOperationError) as caught:
                await svc.add_dependency(card_b.id, card_a.id)
            assert caught.value.code == "dependency_cycle_detected"

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
            with pytest.raises(CardOperationError) as caught:
                await svc.add_dependency(cc.id, ca.id)
            assert caught.value.code == "dependency_cycle_detected"

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
            with pytest.raises(CardOperationError) as caught:
                await svc.add_dependency(card_a.id, card_a.id)
            assert caught.value.code == "dependency_self_reference"

    async def test_duplicate_dependency_is_idempotent(self, db_factory):
        """A repeated edge returns the original record without duplication."""
        card_a, card_b = await self._setup_dependency_test(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            created = await svc.add_dependency(card_a.id, card_b.id)
            duplicate = await svc.add_dependency(card_a.id, card_b.id)

            assert duplicate.id == created.id
            assert len(await svc.get_dependencies(card_a.id)) == 1

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
            with pytest.raises(CardOperationError) as caught:
                await svc.move_card(
                    card_b.id, USER_ID,
                    CardMove(status=CardStatus.IN_PROGRESS),
                )
            assert caught.value.code == "dependencies_incomplete"
            assert caught.value.facts["blocking_dependencies"] == [card_a.title]

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
            await _mark_all_resources_na(db, "card", card_a.id)

            # Move card_a through the validation gate first.
            await svc.move_card(
                card_a.id, USER_ID,
                CardMove(status=CardStatus.STARTED),
            )
            await svc.move_card(
                card_a.id, USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )
            await svc.move_card(
                card_a.id, USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Dependency done",
                    completeness=100,
                    completeness_justification="Complete",
                    drift=0,
                    drift_justification="No deviation",
                ),
            )
            await db.commit()
            await svc.submit_task_validation(
                card_a.id,
                "reviewer-1",
                "Reviewer One",
                {
                    "confidence": 95,
                    "confidence_justification": "Dependency work reviewed",
                    "estimated_completeness": 100,
                    "completeness_justification": "Dependency is complete",
                    "estimated_drift": 0,
                    "drift_justification": "No deviation from plan",
                    "general_justification": "Approved so dependent work can advance.",
                    "recommendation": "approve",
                },
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

    async def test_v2_bug_creation_fences_origin_parent_change(self, db_factory):
        """A stale preflight parent cannot be replaced during target staging."""

        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            original_spec = (
                await db.execute(select(Spec).where(Spec.board_id == BOARD_ID))
            ).scalars().first()
            assert original_spec is not None
            current_spec = Spec(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                title="Origin moved here",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
            )
            db.add(current_spec)
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=current_spec.id,
                title="Moved origin task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.flush()

            with pytest.raises(KnowledgePropagationServiceError) as raised:
                await svc.create_card(
                    BOARD_ID,
                    USER_ID,
                    CardCreate(
                        title="Stale governed bug",
                        card_type="bug",
                        origin_task_id=origin_card.id,
                        severity="major",
                        expected_behavior="Expected",
                        observed_behavior="Observed",
                        spec_id=original_spec.id,
                    ),
                    target_id=str(uuid.uuid4()),
                    knowledge_propagation_v2=True,
                )

            assert raised.value.code == "knowledge_propagation_parent_changed"
            assert raised.value.details["expected_spec_id"] == original_spec.id
            assert raised.value.details["actual_spec_id"] == current_spec.id

    async def test_v2_bug_creation_fails_closed_when_origin_fence_loses(
        self,
        db_factory,
        monkeypatch,
    ):
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            spec = (
                await db.execute(select(Spec).where(Spec.board_id == BOARD_ID))
            ).scalars().first()
            assert spec is not None
            origin_card = Card(
                id=str(uuid.uuid4()),
                board_id=BOARD_ID,
                spec_id=spec.id,
                title="Origin changed after the fresh read",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.flush()

            async def _lost_fence(*_args, **_kwargs):
                return False

            monkeypatch.setattr(
                main_service,
                "_application_fence",
                _lost_fence,
            )
            with pytest.raises(KnowledgePropagationServiceError) as raised:
                await svc.create_card(
                    BOARD_ID,
                    USER_ID,
                    CardCreate(
                        title="Fenced governed bug",
                        card_type="bug",
                        origin_task_id=origin_card.id,
                        severity="major",
                        expected_behavior="Expected",
                        observed_behavior="Observed",
                        spec_id=spec.id,
                    ),
                    target_id=str(uuid.uuid4()),
                    knowledge_propagation_v2=True,
                )

            assert raised.value.code == "knowledge_propagation_parent_changed"

    async def test_create_bug_card_inherits_origin_traceability_links(self, db_factory):
        """Bug card inherits spec item links from the origin task."""
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
                title="Origin Task With Traceability",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                priority=CardPriority.NONE,
                position=0,
                created_by=USER_ID,
            )
            db.add(origin_card)
            await db.commit()

            spec = await db.get(Spec, spec_id)
            assert spec is not None
            spec.test_scenarios = [
                {
                    "id": "ts-origin-link",
                    "title": "Origin scenario",
                    "given": "g",
                    "when": "w",
                    "then": "t",
                    "scenario_type": "integration",
                    "linked_task_ids": [origin_card.id],
                    "status": "draft",
                },
                {
                    "id": "ts-other-link",
                    "title": "Other scenario",
                    "given": "g",
                    "when": "w",
                    "then": "t",
                    "scenario_type": "unit",
                    "linked_task_ids": [],
                    "status": "draft",
                },
            ]
            spec.business_rules = [
                {"id": "br-origin-link", "title": "Origin BR", "linked_task_ids": [origin_card.id]},
                {"id": "br-other-link", "title": "Other BR", "linked_task_ids": []},
            ]
            spec.api_contracts = [
                {"id": "api-origin-link", "method": "GET", "path": "/origin", "linked_task_ids": [origin_card.id]},
            ]
            spec.technical_requirements = [
                {"id": "tr-origin-link", "title": "Origin TR", "linked_task_ids": [origin_card.id]},
                "Legacy TR without link metadata",
            ]
            spec.decisions = [
                {"id": "dec-origin-link", "title": "Origin decision", "status": "active", "linked_task_ids": [origin_card.id]},
            ]
            await db.flush()

            bug = await svc.create_card(
                BOARD_ID,
                USER_ID,
                CardCreate(
                    title="Bug: inherited traceability",
                    card_type="bug",
                    origin_task_id=origin_card.id,
                    severity="major",
                    expected_behavior="Traceability should follow the origin task",
                    observed_behavior="Traceability is missing on the bug",
                ),
            )
            await db.commit()

            assert bug is not None
            assert bug.spec_id == spec_id
            assert bug.test_scenario_ids == ["ts-origin-link"]

            def linked_ids(field: str, item_id: str) -> list[str]:
                for item in getattr(spec, field) or []:
                    if isinstance(item, dict) and item.get("id") == item_id:
                        return item.get("linked_task_ids") or []
                raise AssertionError(f"{item_id} not found in {field}")

            assert bug.id in linked_ids("test_scenarios", "ts-origin-link")
            assert bug.id not in linked_ids("test_scenarios", "ts-other-link")
            assert bug.id in linked_ids("business_rules", "br-origin-link")
            assert bug.id not in linked_ids("business_rules", "br-other-link")
            assert bug.id in linked_ids("api_contracts", "api-origin-link")
            assert bug.id in linked_ids("technical_requirements", "tr-origin-link")
            assert bug.id in linked_ids("decisions", "dec-origin-link")

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
            data = CardCreate.model_construct(
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
        """Creation accepts only lifecycle entry states."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            specs = (await db.execute(
                __import__("sqlalchemy").select(Spec).where(Spec.board_id == BOARD_ID)
            )).scalars().all()
            spec_id = specs[0].id

            statuses = [CardStatus.NOT_STARTED, CardStatus.STARTED]
            created_cards = []
            for status in statuses:
                data = CardCreate(
                    title=f"Card in {status.value}",
                    status=status,
                    spec_id=spec_id,
                )
                card = await svc.create_card(BOARD_ID, USER_ID, data)
                created_cards.append(card)

            for card in created_cards:
                assert card.status in statuses

            for status in (CardStatus.IN_PROGRESS, CardStatus.VALIDATION):
                with pytest.raises(CardOperationError) as exc_info:
                    await svc.create_card(
                        BOARD_ID,
                        USER_ID,
                        CardCreate.model_construct(
                            title=f"Rejected {status.value}",
                            status=status,
                            spec_id=spec_id,
                        ),
                    )
                assert exc_info.value.code == "card_initial_status_invalid"

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
            spec = specs[0]
            actual_spec_id = spec.id

            data = CardCreate(
                title=f"Move Card ({status.value})",
                status=CardStatus.NOT_STARTED,
                spec_id=actual_spec_id,
            )
            card = await svc.create_card(BOARD_ID, USER_ID, data)
            # The helper must be order-independent: forward card transitions
            # require an execution-ready spec, rather than relying on an
            # earlier test to have promoted the shared fixture.
            spec.status = SpecStatus.IN_PROGRESS
            if status != CardStatus.NOT_STARTED:
                await svc.move_card(
                    card.id,
                    USER_ID,
                    CardMove(status=CardStatus.STARTED),
                )
            if status in (
                CardStatus.IN_PROGRESS,
                CardStatus.VALIDATION,
                CardStatus.DONE,
            ):
                await svc.move_card(
                    card.id,
                    USER_ID,
                    CardMove(status=CardStatus.IN_PROGRESS),
                )
            await db.commit()
            return card, actual_spec_id

    async def test_successful_task_validation_emits_card_moved_event(self, db_factory):
        """Validation auto-route to done must re-enqueue KG consolidation."""
        card, spec_id = await self._create_card_for_move(db_factory, CardStatus.IN_PROGRESS)
        async with db_factory() as db:
            svc = CardService(db)
            await svc.move_card(
                card.id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Executor claim for validator review",
                    completeness=100,
                    completeness_justification="All acceptance criteria implemented",
                    drift=0,
                    drift_justification="No deviation from plan",
                ),
            )
            await db.commit()
            await _mark_all_resources_na(db, "card", card.id)
            await db.execute(
                DomainEventRow.__table__.delete().where(
                    DomainEventRow.board_id == BOARD_ID
                )
            )
            result = await svc.submit_task_validation(
                card.id,
                "reviewer-1",
                "Reviewer One",
                {
                    "confidence": 95,
                    "confidence_justification": "Reviewed implementation and tests",
                    "estimated_completeness": 100,
                    "completeness_justification": "Everything requested is present",
                    "estimated_drift": 0,
                    "drift_justification": "Implementation follows the plan",
                    "general_justification": "Approved after reviewing the executor report.",
                    "recommendation": "approve",
                },
            )
            events = (
                await db.execute(
                    select(DomainEventRow).where(DomainEventRow.board_id == BOARD_ID)
                )
            ).scalars().all()

        assert result["card_status"] == "done"
        by_type = {event.event_type: event.payload_json for event in events}
        assert by_type["card.moved"] == {
            "card_id": card.id,
            "from_status": "validation",
            "to_status": "done",
            "spec_id": spec_id,
            "moved_by": "reviewer-1",
        }

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
        """Card updates preserve legacy details and add field-level diffs."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            svc = CardService(db)
            card = (await db.execute(
                __import__("sqlalchemy").select(Card).where(Card.board_id == BOARD_ID)
            )).scalars().first()
            old_title = card.title
            old_priority = card.priority.value
            old_labels = list(card.labels or [])
            new_due_date = datetime(2026, 8, 15, 12, 30, tzinfo=timezone.utc)

            await svc.update_card(
                card.id, USER_ID,
                CardUpdate(
                    title="Updated via test",
                    priority=CardPriority.CRITICAL,
                    labels=["label-a", "activity-diff"],
                    due_date=new_due_date,
                ),
            )
            await db.commit()

            logs = (await db.execute(
                __import__("sqlalchemy").select(ActivityLog)
                .where(ActivityLog.board_id == BOARD_ID)
                .where(ActivityLog.action == "card_updated")
                .where(ActivityLog.card_id == card.id)
            )).scalars().all()
            assert len(logs) >= 1
            details = logs[0].details
            assert details is not None
            # Backward compatibility: existing consumers still receive the
            # submitted values at the top level.
            assert details["title"] == "Updated via test"
            assert details["priority"] == "critical"
            assert details["labels"] == ["label-a", "activity-diff"]
            assert details["due_date"] == new_due_date.isoformat()

            changes = {
                change["field"]: {"old": change["old"], "new": change["new"]}
                for change in details["changes"]
            }
            assert changes == {
                "title": {"old": old_title, "new": "Updated via test"},
                "priority": {"old": old_priority, "new": "critical"},
                "labels": {
                    "old": old_labels,
                    "new": ["label-a", "activity-diff"],
                },
                "due_date": {"old": None, "new": new_due_date.isoformat()},
            }

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
            await svc.move_card(
                card.id,
                USER_ID,
                CardMove(
                    status=CardStatus.VALIDATION,
                    conclusion="Ready for validation after implementation",
                    completeness=100,
                    completeness_justification="All planned work completed",
                    drift=0,
                    drift_justification="No deviation from plan",
                ),
            )
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
class TestCardStatusTransitions:  # noqa: F811
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
