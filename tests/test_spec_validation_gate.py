"""Comprehensive tests for the Spec Validation Gate workflow.

Covers:
- State guard: spec must be in "approved" to submit validation
- Threshold pass/fail for completeness, assertiveness, ambiguity
- Recommendation reject overrides passing thresholds
- Append-only validation history
- Content lock: edits blocked after successful validation
- Lock release: backward move clears current_validation_id
- list_validations: returns all with active flag
- Input validation: score ranges, justification lengths
- Board-level custom thresholds

Test patterns:
- Async tests with pytest
- Class-based organization with pytest.mark.asyncio
- _seed_board() fixture pattern for setup
- db_factory fixture for DB operations
- BOARD_ID / SPEC_ID constants
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from okto_pulse.core.models.db import (
    Board,
    Card,
    CardStatus,
    CardType,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import SpecMove, SpecUpdate
from okto_pulse.core.services.main import (
    SpecLockedError,
    SpecService,
)


BOARD_ID = "validation-board-001"
SPEC_ID = "validation-spec-001"
USER_ID = "user-test-001"


# ===========================================================================
# Seed fixture
# ===========================================================================


async def _seed_board(db_factory, board_id=None, spec_id=None) -> None:
    """Create a board with ideation → refinement → spec chain.

    Idempotent — skips if board already seeded.
    Uses global BOARD_ID/SPEC_ID if not provided.
    """
    if board_id is None:
        board_id = BOARD_ID
    if spec_id is None:
        spec_id = SPEC_ID
    return await _seed_board_with_ids(db_factory, board_id, spec_id)


async def _seed_board_with_ids(db_factory, board_id, spec_id) -> None:
    """Create a board with ideation → refinement → spec chain.

    Idempotent — skips if board already seeded.
    """
    async with db_factory() as db:
        existing = await db.get(Board, board_id)
        if existing is not None:
            return

    ideation_id = str(uuid.uuid4())
    ref_id = str(uuid.uuid4())
    card_impl_id = str(uuid.uuid4())
    card_test_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(
            id=board_id,
            name="Validation Gate Board",
            owner_id=USER_ID,
            settings={
                "require_spec_validation": True,
                "min_spec_completeness": 80,
                "min_spec_assertiveness": 80,
                "max_spec_ambiguity": 30,
            },
        ))
        db.add(Ideation(
            id=ideation_id,
            board_id=board_id,
            title="Validation Gate Ideation",
            status=IdeationStatus.DONE,
            archived=False,
            created_by=USER_ID,
        ))
        db.add(Refinement(
            id=ref_id,
            ideation_id=ideation_id,
            board_id=board_id,
            title="Validation Gate Refinement",
            status=RefinementStatus.DONE,
            archived=False,
            created_by=USER_ID,
        ))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            ideation_id=ideation_id,
            refinement_id=ref_id,
            title="Validation Gate Spec",
            status=SpecStatus.APPROVED,
            archived=False,
            skip_test_coverage=True,
            acceptance_criteria=[
                "AC1: System returns 200 on health check",
                "AC2: System returns 401 on invalid token",
                "AC3: System returns 404 on unknown resource",
            ],
            functional_requirements=[
                "FR1: Health endpoint exists",
                "FR2: Authentication required",
                "FR3: Resource not found handling",
            ],
            test_scenarios=[
                {
                    "id": "ts_health",
                    "title": "Health check returns 200",
                    "given": "Server is running",
                    "when": "GET /health",
                    "then": "Returns 200 OK",
                    "scenario_type": "integration",
                    "linked_criteria": [0],
                    "linked_task_ids": [card_impl_id],
                },
                {
                    "id": "ts_auth",
                    "title": "Invalid token returns 401",
                    "given": "Client sends invalid token",
                    "when": "GET /resource",
                    "then": "Returns 401 Unauthorized",
                    "scenario_type": "integration",
                    "linked_criteria": [1],
                    "linked_task_ids": [card_impl_id],
                },
                {
                    "id": "ts_notfound",
                    "title": "Unknown resource returns 404",
                    "given": "Client requests unknown path",
                    "when": "GET /unknown",
                    "then": "Returns 404 Not Found",
                    "scenario_type": "integration",
                    "linked_criteria": [2],
                    "linked_task_ids": [card_impl_id],
                },
            ],
            business_rules=[
                {
                    "id": "br_health",
                    "title": "Health endpoint exists",
                    "rule": "Health endpoint must return 200",
                    "when": "GET /health is called",
                    "then": "Return 200 OK",
                    "linked_requirements": [0],
                    "linked_task_ids": [card_impl_id],
                },
                {
                    "id": "br_auth",
                    "title": "Authentication required",
                    "rule": "All endpoints require valid token",
                    "when": "Request is made without token",
                    "then": "Return 401",
                    "linked_requirements": [1],
                    "linked_task_ids": [card_impl_id],
                },
                {
                    "id": "br_notfound",
                    "title": "Resource not found handling",
                    "rule": "Unknown resources return 404",
                    "when": "Resource does not exist",
                    "then": "Return 404 with message",
                    "linked_requirements": [2],
                    "linked_task_ids": [card_impl_id],
                },
            ],
            technical_requirements=[
                {"id": "tr_1", "text": "Must use JWT auth", "linked_task_ids": [card_impl_id]},
                {"id": "tr_2", "text": "Response time < 200ms", "linked_task_ids": [card_impl_id]},
            ],
            api_contracts=[
                {
                    "id": "api_1",
                    "method": "GET",
                    "path": "/health",
                    "description": "Health check endpoint",
                    "request_body": None,
                    "response_success": {"status": 200, "message": "ok"},
                    "response_errors": [{"status": 500, "detail": "internal error"}],
                    "linked_requirements": [0],
                    "linked_rules": [],
                    "linked_task_ids": [card_impl_id],
                },
            ],
            decisions=[
                {"id": "dec_1", "title": "Use JWT", "status": "active", "linked_task_ids": [card_impl_id]},
            ],
            created_by=USER_ID,
        ))
        yesterday = datetime.now(timezone.utc)
        db.add(Card(
            id=card_impl_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Implementation card",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            archived=False,
            created_by=USER_ID,
            created_at=yesterday,
            updated_at=yesterday,
        ))
        # Test card — required by check_test_coverage for scenarios with linked_task_ids
        db.add(Card(
            id=card_test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Test card",
            status=CardStatus.DONE,
            card_type=CardType.TEST,
            archived=False,
            created_by=USER_ID,
            created_at=yesterday,
            updated_at=yesterday,
        ))
        await db.commit()


# ===========================================================================
# Helper: build a valid submit payload
# ===========================================================================


def _valid_submit_data(
    completeness: int = 90,
    assertiveness: int = 85,
    ambiguity: int = 15,
    recommendation: str = "approve",
) -> dict:
    """Return a minimal valid submit payload."""
    return {
        "completeness": completeness,
        "completeness_justification": "All ACs are covered with detailed test plans",
        "assertiveness": assertiveness,
        "assertiveness_justification": "FRs are measurable with no weasel words",
        "ambiguity": ambiguity,
        "ambiguity_justification": "Glossary added and terms defined clearly",
        "general_justification": "Spec is ready for execution with high confidence",
        "recommendation": recommendation,
    }


# ===========================================================================
# 1. State guard
# ===========================================================================


@pytest.mark.asyncio
class TestStateGuard:
    """Spec must be in 'approved' status to submit validation."""

    async def test_approved_status_allows_submit(self, db_factory):
        """Spec in 'approved' status should accept validation submission."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"

    async def test_draft_status_rejects_submit(self, db_factory):
        """Spec in 'draft' status must raise ValueError."""
        spec_id = str(uuid.uuid4())
        board_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Validation Gate Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 80,
                    "min_spec_assertiveness": 80,
                    "max_spec_ambiguity": 30,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Draft Spec",
                status=SpecStatus.DRAFT,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="'draft'"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_in_progress_status_rejects_submit(self, db_factory):
        """Spec in 'in_progress' status must raise ValueError."""
        spec_id = str(uuid.uuid4())
        board_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Validation Gate Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 80,
                    "min_spec_assertiveness": 80,
                    "max_spec_ambiguity": 30,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="In Progress Spec",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="'in_progress'"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_done_status_rejects_submit(self, db_factory):
        """Spec in 'done' status must raise ValueError."""
        spec_id = str(uuid.uuid4())
        board_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Validation Gate Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 80,
                    "min_spec_assertiveness": 80,
                    "max_spec_ambiguity": 30,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Done Spec",
                status=SpecStatus.DONE,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="'done'"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_validated_status_rejects_submit(self, db_factory):
        """Spec already in 'validated' status must raise ValueError."""
        spec_id = str(uuid.uuid4())
        board_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Validation Gate Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 80,
                    "min_spec_assertiveness": 80,
                    "max_spec_ambiguity": 30,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Validated Spec",
                status=SpecStatus.VALIDATED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="'validated'"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_review_status_rejects_submit(self, db_factory):
        """Spec in 'review' status must raise ValueError."""
        spec_id = str(uuid.uuid4())
        board_id = str(uuid.uuid4())
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Validation Gate Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 80,
                    "min_spec_assertiveness": 80,
                    "max_spec_ambiguity": 30,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Review Spec",
                status=SpecStatus.REVIEW,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="'review'"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_nonexistent_spec_rejects_submit(self, db_factory):
        """Submitting validation for a nonexistent spec must raise ValueError."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="not found"):
                await service.submit_spec_validation(
                    spec_id="nonexistent-spec",
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )


# ===========================================================================
# 2. Threshold pass — all scores meet thresholds + approve → success
# ===========================================================================


@pytest.mark.asyncio
class TestThresholdPass:
    """All scores meet thresholds + recommendation=approve → spec becomes validated."""

    async def test_all_thresholds_pass(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(
                    completeness=90,
                    assertiveness=85,
                    ambiguity=15,
                ),
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"
        assert result["threshold_violations"] == []
        assert result["recommendation"] == "approve"

    async def test_boundary_scores_pass(self, db_factory):
        """Scores exactly at threshold boundaries should pass."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 80,  # exactly at min
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 80,  # exactly at min
                    "assertiveness_justification": "FRs are measurable with no weasel words",
                    "ambiguity": 30,  # exactly at max
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"

    async def test_max_scores_pass(self, db_factory):
        """Maximum scores (100/100/0) should pass."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 100,
                    "completeness_justification": "Perfect completeness across all areas",
                    "assertiveness": 100,
                    "assertiveness_justification": "Every requirement is measurable",
                    "ambiguity": 0,
                    "ambiguity_justification": "Zero ambiguity — all terms defined",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"

    async def test_spec_persists_validated_status(self, db_factory):
        """After successful validation, spec status is persisted as 'validated'."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            # Re-fetch spec to verify persistence
            spec = await service.get_spec(SPEC_ID)
        assert spec.status == SpecStatus.VALIDATED
        assert spec.current_validation_id is not None


# ===========================================================================
# 3. Threshold fail — completeness < 80
# ===========================================================================


@pytest.mark.asyncio
class TestThresholdFailCompleteness:
    """Completeness below threshold → validation fails, spec stays approved."""

    async def test_completeness_below_threshold(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 72,
                    "completeness_justification": "Edge case scenarios are still missing",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 15,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Scores are low in completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"
        assert len(result["threshold_violations"]) == 1
        assert "completeness" in result["threshold_violations"][0]
        assert "72" in result["threshold_violations"][0]

    async def test_completeness_zero_fails(self, db_factory):
        """Completeness of 0 should fail."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 0,
                    "completeness_justification": "No coverage at all",
                    "assertiveness": 100,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 0,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Zero completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"

    async def test_spec_status_unchanged_on_failure(self, db_factory):
        """Spec status remains 'approved' after a failed validation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 50,
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 100,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 0,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Half coverage but I approve overall",
                    "recommendation": "approve",
                },
            )
            spec = await service.get_spec(SPEC_ID)
        assert spec.status == SpecStatus.APPROVED


# ===========================================================================
# 4. Threshold fail — assertiveness < 80
# ===========================================================================


@pytest.mark.asyncio
class TestThresholdFailAssertiveness:
    """Assertiveness below threshold → validation fails, spec stays approved."""

    async def test_assertiveness_below_threshold(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 70,
                    "assertiveness_justification": "Some FRs use vague language",
                    "ambiguity": 15,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Good completeness but assertiveness is low",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"
        assert len(result["threshold_violations"]) == 1
        assert "assertiveness" in result["threshold_violations"][0]

    async def test_assertiveness_zero_fails(self, db_factory):
        """Assertiveness of 0 should fail."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 100,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 0,
                    "assertiveness_justification": "No measurable criteria",
                    "ambiguity": 0,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Perfect completeness but no assertiveness",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"


# ===========================================================================
# 5. Threshold fail — ambiguity > 30
# ===========================================================================


@pytest.mark.asyncio
class TestThresholdFailAmbiguity:
    """Ambiguity above threshold → validation fails, spec stays approved."""

    async def test_ambiguity_above_threshold(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 45,
                    "ambiguity_justification": "Many terms are undefined",
                    "general_justification": "Good scores but ambiguity is too high",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"
        assert len(result["threshold_violations"]) == 1
        assert "ambiguity" in result["threshold_violations"][0]
        assert "45" in result["threshold_violations"][0]

    async def test_ambiguity_at_boundary_passes(self, db_factory):
        """Ambiguity of exactly 30 should pass (it's the max)."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 30,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"

    async def test_ambiguity_one_above_boundary_fails(self, db_factory):
        """Ambiguity of 31 should fail (one above max)."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 31,
                    "ambiguity_justification": "Some terms are still vague",
                    "general_justification": "Almost perfect but ambiguity is slightly too high",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"


# ===========================================================================
# 6. Recommendation reject
# ===========================================================================


@pytest.mark.asyncio
class TestRecommendationReject:
    """Even with passing thresholds, recommendation=reject → fails."""

    async def test_reject_override_with_passing_thresholds(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 95,
                    "completeness_justification": "Excellent completeness across all areas",
                    "assertiveness": 95,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 5,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Scores are excellent but I still reject this spec",
                    "recommendation": "reject",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"
        assert result["threshold_violations"] == []  # no threshold violations
        assert result["recommendation"] == "reject"

    async def test_reject_preserves_spec_status(self, db_factory):
        """Spec remains in 'approved' after a reject validation."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 95,
                    "completeness_justification": "Excellent completeness across all areas",
                    "assertiveness": 95,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 5,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Scores are excellent but I still reject this spec",
                    "recommendation": "reject",
                },
            )
            spec = await service.get_spec(SPEC_ID)
        assert spec.status == SpecStatus.APPROVED
        assert spec.current_validation_id is not None  # pointer still set


# ===========================================================================
# 7. Append-only history
# ===========================================================================


@pytest.mark.asyncio
class TestAppendOnlyHistory:
    """Multiple submissions create multiple validation records."""

    async def test_multiple_submissions_append(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            # First submission: fails (low completeness)
            result1 = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 60,
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 10,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Low completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
            assert result1["outcome"] == "failed"

            # Second submission: passes
            result2 = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            assert result2["outcome"] == "success"

            # Move back to approved for third submission
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.REVIEW),
            )
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.APPROVED),
            )

            # Third submission: fails again (reject)
            result3 = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 95,
                    "completeness_justification": "Excellent completeness across all areas",
                    "assertiveness": 95,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 5,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Scores are excellent but I still reject this spec",
                    "recommendation": "reject",
                },
            )
            assert result3["outcome"] == "failed"

            # Verify all three records exist
            spec = await service.get_spec(SPEC_ID)
        assert len(spec.validations) == 3

    async def test_validation_ids_are_unique(self, db_factory):
        """Each validation record gets a unique ID."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            results = []
            for i in range(3):
                result = await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 90,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "reject",  # always reject to stay in approved
                    },
                )
                results.append(result)
        ids = [r["id"] for r in results]
        assert len(set(ids)) == 3  # all unique

    async def test_current_validation_id_points_to_latest(self, db_factory):
        """After multiple submissions, current_validation_id points to the last one."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 60,
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 10,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Low completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
            result2 = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(SPEC_ID)
        assert spec.current_validation_id == result2["id"]

    async def test_validation_records_preserved_on_status_change(self, db_factory):
        """Moving spec back to draft preserves all validation records."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            # Submit a successful validation → spec becomes validated
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(SPEC_ID)
            validation_count = len(spec.validations)

            # Move back to draft
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )

            # Move back through review to approved so we can submit again
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.REVIEW),
            )
            await service.move_spec(
                SPEC_ID,
                USER_ID,
                SpecMove(status=SpecStatus.APPROVED),
            )

            # Submit another validation
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 10,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "reject",
                },
            )

            spec = await service.get_spec(SPEC_ID)
        assert len(spec.validations) == validation_count + 1


# ===========================================================================
# 8. Content lock
# ===========================================================================


@pytest.mark.asyncio
class TestContentLock:
    """After successful validation, spec edits raise SpecLockedError."""

    async def test_update_spec_blocked_after_success(self, db_factory):
        content_lock_board_id = str(uuid.uuid4())
        content_lock_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=content_lock_board_id, spec_id=content_lock_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # First, pass validation
            await service.submit_spec_validation(
                spec_id=content_lock_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            # Now try to update — should be blocked
            with pytest.raises(SpecLockedError) as exc_info:
                await service.update_spec(
                    content_lock_spec_id,
                    USER_ID,
                    SpecUpdate(description="New description after validation"),
                )
        assert "locked" in str(exc_info.value).lower()

    async def test_update_title_blocked_after_success(self, db_factory):
        """Updating the title should also be blocked."""
        content_lock_board_id = str(uuid.uuid4())
        content_lock_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=content_lock_board_id, spec_id=content_lock_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=content_lock_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            with pytest.raises(SpecLockedError):
                await service.update_spec(
                    content_lock_spec_id,
                    USER_ID,
                    SpecUpdate(title="New title"),
                )

    async def test_update_functional_requirements_blocked(self, db_factory):
        """Updating functional requirements should be blocked."""
        cl_board_id = str(uuid.uuid4())
        cl_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=cl_board_id, spec_id=cl_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=cl_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            with pytest.raises(SpecLockedError):
                await service.update_spec(
                    cl_spec_id,
                    USER_ID,
                    SpecUpdate(functional_requirements=["New FR"]),
                )

    async def test_get_spec_allowed_after_success(self, db_factory):
        """Reading the spec should still be allowed after validation."""
        cl_board_id = str(uuid.uuid4())
        cl_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=cl_board_id, spec_id=cl_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=cl_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(cl_spec_id)
        assert spec is not None
        assert spec.status == SpecStatus.VALIDATED
        assert spec.validations is not None
        assert len(spec.validations) == 1

    async def test_spec_locked_error_has_correct_message(self, db_factory):
        """SpecLockedError should have a meaningful message."""
        cl_board_id = str(uuid.uuid4())
        cl_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=cl_board_id, spec_id=cl_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=cl_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            try:
                await service.update_spec(
                    SPEC_ID,
                    USER_ID,
                    SpecUpdate(description="attempted edit"),
                )
            except SpecLockedError as exc:
                assert "locked" in str(exc).lower()
                assert "validation passed" in str(exc).lower() or "move" in str(exc).lower()


# ===========================================================================
# 9. Lock release
# ===========================================================================


@pytest.mark.asyncio
class TestLockRelease:
    """Moving spec back to approved/draft clears the lock."""

    async def test_backward_move_clears_lock(self, db_factory):
        """Moving from validated → draft clears current_validation_id."""
        lr_board_id = str(uuid.uuid4())
        lr_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lr_board_id, spec_id=lr_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # Pass validation
            await service.submit_spec_validation(
                spec_id=lr_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(lr_spec_id)
            assert spec.current_validation_id is not None

            # Move back to draft
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )
            spec = await service.get_spec(lr_spec_id)
        assert spec.current_validation_id is None
        assert spec.status == SpecStatus.DRAFT
        assert len(spec.validations) == 1  # history preserved

    async def test_lock_released_allows_edit(self, db_factory):
        """After lock is released, spec edits should work."""
        lr_board_id = str(uuid.uuid4())
        lr_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lr_board_id, spec_id=lr_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # Pass validation
            await service.submit_spec_validation(
                spec_id=lr_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            # Move back to draft (releases lock)
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )
            # Move back through review to approved
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.REVIEW),
            )
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.APPROVED),
            )
            # Now edit should work
            spec = await service.update_spec(
                lr_spec_id,
                USER_ID,
                SpecUpdate(description="Updated after lock release"),
            )
        assert spec.description == "Updated after lock release"

    async def test_validations_history_preserved_after_release(self, db_factory):
        """Validation history is preserved after lock release."""
        lr_board_id = str(uuid.uuid4())
        lr_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lr_board_id, spec_id=lr_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # Submit multiple validations
            await service.submit_spec_validation(
                spec_id=lr_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 60,
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 10,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Low completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
            await service.submit_spec_validation(
                spec_id=lr_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(lr_spec_id)
            assert len(spec.validations) == 2

            # Move back to draft
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )
            spec = await service.get_spec(lr_spec_id)
        assert len(spec.validations) == 2  # history preserved
        assert spec.current_validation_id is None

    async def test_move_to_approved_clears_lock(self, db_factory):
        """Moving from validated → approved also clears the lock."""
        lr_board_id = str(uuid.uuid4())
        lr_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lr_board_id, spec_id=lr_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=lr_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(lr_spec_id)
            assert spec.current_validation_id is not None

            # Move back to approved (not draft)
            await service.move_spec(
                lr_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.APPROVED),
            )
            spec = await service.get_spec(lr_spec_id)
        assert spec.current_validation_id is None
        assert spec.status == SpecStatus.APPROVED


# ===========================================================================
# 10. list_validations
# ===========================================================================


@pytest.mark.asyncio
class TestListValidations:
    """list_validations returns all validations with current one marked active."""

    async def test_list_returns_validations_in_reverse_chronological_order(self, db_factory):
        lv_board_id = str(uuid.uuid4())
        lv_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lv_board_id, spec_id=lv_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # Submit 3 validations
            for i in range(3):
                await service.submit_spec_validation(
                    spec_id=lv_spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 90,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "reject",
                    },
                )
            result = await service.list_spec_validations(lv_spec_id)
        assert len(result["validations"]) == 3
        # First item in result should be the latest (reversed)
        assert result["validations"][0]["id"] == result["current_validation_id"]

    async def test_list_marks_active_validation(self, db_factory):
        """The validation pointed to by current_validation_id should have active=True."""
        lv_board_id = str(uuid.uuid4())
        lv_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lv_board_id, spec_id=lv_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            # Submit 2 validations
            result1 = await service.submit_spec_validation(
                spec_id=lv_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 60,
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 10,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Low completeness but I approve overall",
                    "recommendation": "approve",
                },
            )
            result2 = await service.submit_spec_validation(
                spec_id=lv_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            list_result = await service.list_spec_validations(lv_spec_id)
        # Latest should be active
        latest = list_result["validations"][0]
        assert latest["id"] == result2["id"]
        assert latest["active"] is True
        # Previous should be inactive
        prev = list_result["validations"][1]
        assert prev["id"] == result1["id"]
        assert prev["active"] is False

    async def test_list_after_lock_release(self, db_factory):
        """After lock release, current_validation_id is None and no active flag."""
        lv_board_id = str(uuid.uuid4())
        lv_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lv_board_id, spec_id=lv_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=lv_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
            # Move back to draft
            await service.move_spec(
                lv_spec_id,
                USER_ID,
                SpecMove(status=SpecStatus.DRAFT),
            )
            result = await service.list_spec_validations(lv_spec_id)
        assert result["current_validation_id"] is None
        for v in result["validations"]:
            assert v["active"] is False

    async def test_list_returns_all_record_fields(self, db_factory):
        """Each validation record should include all expected fields."""
        lv_board_id = str(uuid.uuid4())
        lv_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lv_board_id, spec_id=lv_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=lv_spec_id,
                reviewer_id=USER_ID,
                reviewer_name="TestReviewer",
                data=_valid_submit_data(),
            )
            result = await service.list_spec_validations(lv_spec_id)
        v = result["validations"][0]
        for field in (
            "id", "spec_id", "board_id", "reviewer_id", "reviewer_name",
            "completeness", "completeness_justification",
            "assertiveness", "assertiveness_justification",
            "ambiguity", "ambiguity_justification",
            "general_justification", "recommendation",
            "outcome", "threshold_violations",
            "resolved_thresholds", "created_at",
        ):
            assert field in v, f"Missing field: {field}"

    async def test_list_empty_validations(self, db_factory):
        """Spec with no validations returns empty list."""
        lv_board_id = str(uuid.uuid4())
        lv_spec_id = str(uuid.uuid4())
        await _seed_board(db_factory, board_id=lv_board_id, spec_id=lv_spec_id)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.list_spec_validations(lv_spec_id)
        assert result["validations"] == []
        assert result["current_validation_id"] is None


# ===========================================================================
# 11. Input validation
# ===========================================================================


@pytest.mark.asyncio
class TestInputValidation:
    """Invalid score ranges and missing justification fields."""

    async def test_negative_completeness_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="completeness"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": -1,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 80,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )

    async def test_completeness_over_100_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="completeness"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 101,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 80,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )

    async def test_negative_assertiveness_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="assertiveness"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": -5,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )

    async def test_negative_ambiguity_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="ambiguity"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 90,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": -10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )

    async def test_ambiguity_over_100_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="ambiguity"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 90,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 150,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )

    async def test_invalid_recommendation_raises(self, db_factory):
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises(ValueError, match="recommendation"):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        "assertiveness": 90,
                        "assertiveness_justification": "FRs are measurable and testable",
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "maybe",
                    },
                )

    async def test_missing_justification_field_raises(self, db_factory):
        """Omitting a required justification field should raise."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            with pytest.raises((KeyError, TypeError, ValueError)):
                await service.submit_spec_validation(
                    spec_id=SPEC_ID,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data={
                        "completeness": 90,
                        "completeness_justification": "All ACs are covered with detailed test plans",
                        # Missing assertiveness_justification
                        "assertiveness": 90,
                        "ambiguity": 10,
                        "ambiguity_justification": "Glossary added and terms defined clearly",
                        "general_justification": "Spec is ready for execution with high confidence",
                        "recommendation": "approve",
                    },
                )


# ===========================================================================
# 12. Board-level config with custom thresholds
# ===========================================================================


@pytest.mark.asyncio
class TestBoardLevelConfig:
    """Test with custom threshold values at the board level."""

    async def test_custom_thresholds_applied(self, db_factory):
        """Board with custom thresholds should enforce them."""
        board_id = "custom-threshold-board"
        spec_id = "custom-threshold-spec"
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Custom Threshold Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 95,  # higher default
                    "min_spec_assertiveness": 90,  # higher default
                    "max_spec_ambiguity": 10,  # lower default (stricter)
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Custom Threshold Spec",
                status=SpecStatus.APPROVED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            # With default thresholds (80/80/30) this would pass, but with
            # custom thresholds (95/90/10) it should fail on completeness.
            result = await service.submit_spec_validation(
                spec_id=spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,  # below custom min 95
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 95,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 5,
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "failed"
        assert result["spec_status"] == "approved"
        assert any("completeness" in v for v in result["threshold_violations"])

    async def test_custom_thresholds_pass_with_higher_scores(self, db_factory):
        """With scores meeting custom thresholds, validation should pass."""
        board_id = "custom-pass-board"
        spec_id = "custom-pass-spec"
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Custom Pass Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": True,
                    "min_spec_completeness": 95,
                    "min_spec_assertiveness": 90,
                    "max_spec_ambiguity": 10,
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Custom Pass Spec",
                status=SpecStatus.APPROVED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 96,  # above custom min 95
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 95,  # above custom min 90
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 5,  # below custom max 10
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result["outcome"] == "success"
        assert result["spec_status"] == "validated"

    async def test_default_thresholds_when_not_configured(self, db_factory):
        """Board without explicit threshold settings should use defaults."""
        board_id = "default-threshold-board"
        spec_id = "default-threshold-spec"
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Default Threshold Board",
                owner_id=USER_ID,
                settings={},  # no threshold settings
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Default Threshold Spec",
                status=SpecStatus.APPROVED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            # Board doesn't have require_spec_validation → should raise
            with pytest.raises(ValueError, match="does not require"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_opt_in_required(self, db_factory):
        """Board without require_spec_validation should reject all submissions."""
        board_id = "opt-out-board"
        spec_id = "opt-out-spec"
        async with db_factory() as db:
            db.add(Board(
                id=board_id,
                name="Opt Out Board",
                owner_id=USER_ID,
                settings={
                    "require_spec_validation": False,  # explicitly disabled
                },
            ))
            db.add(Spec(
                id=spec_id,
                board_id=board_id,
                title="Opt Out Spec",
                status=SpecStatus.APPROVED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            with pytest.raises(ValueError, match="does not require"):
                await service.submit_spec_validation(
                    spec_id=spec_id,
                    reviewer_id=USER_ID,
                    reviewer_name="Tester",
                    data=_valid_submit_data(),
                )

    async def test_multiple_threshold_violations_reported(self, db_factory):
        """When multiple thresholds are violated, all should be reported."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 50,  # below 80
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 50,  # below 80
                    "assertiveness_justification": "FRs are vague",
                    "ambiguity": 60,  # above 30
                    "ambiguity_justification": "Many undefined terms",
                    "general_justification": "Very poor spec quality",
                    "recommendation": "reject",
                },
            )
        assert result["outcome"] == "failed"
        assert len(result["threshold_violations"]) == 3
        violation_types = {v.split()[0] for v in result["threshold_violations"]}
        assert violation_types == {"completeness", "assertiveness", "ambiguity"}


# ===========================================================================
# 13. Validation record structure
# ===========================================================================


@pytest.mark.asyncio
class TestValidationRecordStructure:
    """Verify the structure and content of validation records."""

    async def test_validation_record_has_all_fields(self, db_factory):
        """Each validation record should contain all expected fields."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="TestReviewer",
                data=_valid_submit_data(),
            )
        expected_fields = {
            "id", "spec_id", "board_id", "reviewer_id", "reviewer_name",
            "completeness", "completeness_justification",
            "assertiveness", "assertiveness_justification",
            "ambiguity", "ambiguity_justification",
            "general_justification", "recommendation",
            "outcome", "threshold_violations",
            "resolved_thresholds", "created_at", "spec_status", "active",
        }
        assert set(result.keys()) >= expected_fields

    async def test_validation_record_resolved_thresholds(self, db_factory):
        """resolved_thresholds should contain the thresholds in effect."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
        rt = result["resolved_thresholds"]
        assert rt["min_spec_completeness"] == 80
        assert rt["min_spec_assertiveness"] == 80
        assert rt["max_spec_ambiguity"] == 30

    async def test_validation_record_preserves_reviewer_info(self, db_factory):
        """Validation record should preserve reviewer_id and reviewer_name."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id="reviewer-123",
                reviewer_name="JaneReviewer",
                data=_valid_submit_data(),
            )
            spec = await service.get_spec(SPEC_ID)
        v = spec.validations[0]
        assert v["reviewer_id"] == "reviewer-123"
        assert v["reviewer_name"] == "JaneReviewer"

    async def test_validation_record_has_timestamp(self, db_factory):
        """Validation record should have a created_at timestamp."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
        assert "created_at" in result
        assert result["created_at"] is not None
        # Should be a valid ISO format
        datetime.fromisoformat(result["created_at"])

    async def test_validation_id_format(self, db_factory):
        """Validation ID should follow the 'val_' prefix format."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
        assert result["id"].startswith("val_")
        assert len(result["id"]) == 12  # "val_" + 8 hex chars


# ===========================================================================
# 14. Edge cases
# ===========================================================================


@pytest.mark.asyncio
class TestEdgeCases:
    """Edge cases and boundary conditions."""

    async def test_justification_stripped(self, db_factory):
        """Justification fields should be stripped of leading/trailing whitespace."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "  All ACs are covered with detailed test plans  ",
                    "assertiveness": 90,
                    "assertiveness_justification": "  FRs are measurable and testable  ",
                    "ambiguity": 10,
                    "ambiguity_justification": "  Glossary added and terms defined clearly  ",
                    "general_justification": "  Spec is ready for execution with high confidence  ",
                    "recommendation": "approve",
                },
            )
            spec = await service.get_spec(SPEC_ID)
        v = spec.validations[0]
        assert v["completeness_justification"] == "All ACs are covered with detailed test plans"
        assert v["assertiveness_justification"] == "FRs are measurable and testable"
        assert v["ambiguity_justification"] == "Glossary added and terms defined clearly"
        assert v["general_justification"] == "Spec is ready for execution with high confidence"

    async def test_multiple_violations_with_reject(self, db_factory):
        """Both threshold violations AND reject recommendation are recorded."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 50,  # below 80
                    "completeness_justification": "Half the ACs are covered",
                    "assertiveness": 50,  # below 80
                    "assertiveness_justification": "FRs are vague",
                    "ambiguity": 60,  # above 30
                    "ambiguity_justification": "Many undefined terms",
                    "general_justification": "Very poor spec quality",
                    "recommendation": "reject",
                },
            )
        assert result["outcome"] == "failed"
        assert len(result["threshold_violations"]) == 3
        assert result["recommendation"] == "reject"

    async def test_ambiguity_is_max_not_min(self, db_factory):
        """Ambiguity is checked as a maximum (lower is better), not minimum."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            service = SpecService(db)
            # Low ambiguity should pass
            result_pass = await service.submit_spec_validation(
                spec_id=SPEC_ID,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data={
                    "completeness": 90,
                    "completeness_justification": "All ACs are covered with detailed test plans",
                    "assertiveness": 90,
                    "assertiveness_justification": "FRs are measurable and testable",
                    "ambiguity": 0,  # perfect — no ambiguity
                    "ambiguity_justification": "Glossary added and terms defined clearly",
                    "general_justification": "Spec is ready for execution with high confidence",
                    "recommendation": "approve",
                },
            )
        assert result_pass["outcome"] == "success"

    async def test_spec_with_no_coverage_passes_threshold_test(self, db_factory):
        """Spec with empty coverage arrays should pass coverage pre-checks."""
        await _seed_board(db_factory)
        async with db_factory() as db:
            # Create a spec with no coverage items (empty arrays)
            spec_id = "no-coverage-spec"
            db.add(Spec(
                id=spec_id,
                board_id=BOARD_ID,
                title="No Coverage Spec",
                status=SpecStatus.APPROVED,
                archived=False,
                acceptance_criteria=[],
                functional_requirements=[],
                test_scenarios=[],
                business_rules=[],
                technical_requirements=[],
                api_contracts=[],
                decisions=[],
                created_by=USER_ID,
            ))
            await db.commit()

            service = SpecService(db)
            result = await service.submit_spec_validation(
                spec_id=spec_id,
                reviewer_id=USER_ID,
                reviewer_name="Tester",
                data=_valid_submit_data(),
            )
        # Empty coverage should pass the pre-checks (nothing to cover)
        assert result["outcome"] == "success"
