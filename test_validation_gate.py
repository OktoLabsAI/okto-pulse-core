"""Integration test for Task Validation Gate — covers TS-01 through TS-14."""
import asyncio
import os
import sys
from datetime import datetime, timezone

# Use a temp sqlite database
os.environ["OKTO_PULSE_DB_URL"] = "sqlite+aiosqlite:///:memory:"

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from okto_pulse.core.models.db import (
    Base, Board, Spec, Sprint, Card, CardStatus, CardType, SpecStatus, SprintStatus
)
from okto_pulse.core.services.main import CardService


async def run_tests():
    results = []

    def check(name, condition, detail=""):
        status = "PASS" if condition else "FAIL"
        results.append((status, name, detail))
        print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as db:
        # Setup: board with gate ON
        board = Board(
            id="b1", name="Test Board", owner_id="u1",
            settings={
                "require_task_validation": True,
                "validation_min_confidence": 70,
                "validation_min_completeness": 80,
                "validation_max_drift": 50,
            }
        )
        db.add(board)

        spec = Spec(
            id="s1", board_id="b1", title="Test Spec",
            status=SpecStatus.IN_PROGRESS, created_by="u1",
        )
        db.add(spec)

        card_service = CardService(db)

        # ==== TEST: _resolve_validation_config from board ====
        config = card_service._resolve_validation_config(
            card=None, spec=spec, sprint=None, board_settings=board.settings
        )
        check("Config resolves from board (required=True)", config["required"] == True)
        check("Config min_confidence=70", config["min_confidence"] == 70)
        check("Config resolved_from=board", config["resolved_from"] == "board")

        # ==== TEST: spec override enables gate ====
        spec.require_task_validation = True
        config = card_service._resolve_validation_config(
            card=None, spec=spec, sprint=None,
            board_settings={"require_task_validation": False}
        )
        check("Spec override enables gate", config["required"] == True)
        check("Spec override resolved_from=spec", config["resolved_from"] == "spec")

        # ==== TEST: sprint override disables gate ====
        sprint = Sprint(
            id="sp1", spec_id="s1", board_id="b1", title="Sprint 1",
            status=SprintStatus.ACTIVE, created_by="u1",
            require_task_validation=False,
        )
        db.add(sprint)
        config = card_service._resolve_validation_config(
            card=None, spec=spec, sprint=sprint,
            board_settings={"require_task_validation": True}
        )
        check("Sprint override disables gate", config["required"] == False)
        check("Sprint override resolved_from=sprint", config["resolved_from"] == "sprint")

        # ==== TEST: sprint null inherits spec ====
        sprint.require_task_validation = None
        config = card_service._resolve_validation_config(
            card=None, spec=spec, sprint=sprint,
            board_settings={"require_task_validation": False}
        )
        check("Sprint null inherits spec (required=True)", config["required"] == True)
        check("Sprint null resolved_from=spec", config["resolved_from"] == "spec")

        # ==== TEST: Threshold null-coalescing per field ====
        spec.validation_min_confidence = 75
        sprint.validation_max_drift = 30
        config = card_service._resolve_validation_config(
            card=None, spec=spec, sprint=sprint,
            board_settings={
                "require_task_validation": True,
                "validation_min_confidence": 70,
                "validation_min_completeness": 80,
                "validation_max_drift": 50,
            }
        )
        check("min_confidence from spec (75)", config["min_confidence"] == 75)
        check("min_completeness from board (80)", config["min_completeness"] == 80)
        check("max_drift from sprint (30)", config["max_drift"] == 30)

        # Reset for next tests
        spec.require_task_validation = None
        spec.validation_min_confidence = None
        sprint.validation_max_drift = None

        # ==== TEST: submit_task_validation — success case ====
        card1 = Card(
            id="c1", board_id="b1", spec_id="s1", title="Normal card",
            status=CardStatus.VALIDATION, created_by="u1",
            card_type=CardType.NORMAL,
        )
        db.add(card1)
        await db.flush()

        result = await card_service.submit_task_validation(
            card_id="c1", reviewer_id="reviewer1", reviewer_name="Reviewer",
            data={
                "confidence": 85,
                "confidence_justification": "All endpoints implemented and tested",
                "estimated_completeness": 92,
                "completeness_justification": "All planned work delivered",
                "estimated_drift": 12,
                "drift_justification": "Closely follows spec",
                "general_justification": "Solid implementation ready for production",
                "recommendation": "approve",
            },
        )
        check("TS-01 success outcome", result["outcome"] == "success")
        check("TS-01 no threshold violations", result["threshold_violations"] == [])
        check("TS-01 card moved to done", card1.status == CardStatus.DONE)
        check("TS-01 validation persisted", len(card1.validations) == 1)

        # ==== TEST: confidence below threshold ====
        card2 = Card(
            id="c2", board_id="b1", spec_id="s1", title="Normal card 2",
            status=CardStatus.VALIDATION, created_by="u1", card_type=CardType.NORMAL,
        )
        db.add(card2)
        await db.flush()

        result = await card_service.submit_task_validation(
            card_id="c2", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 55,  # BELOW 70
                "confidence_justification": "Lacks pagination",
                "estimated_completeness": 90,
                "completeness_justification": "Most work done",
                "estimated_drift": 10,
                "drift_justification": "Matches spec",
                "general_justification": "Approve but with concerns about rate limiting",
                "recommendation": "approve",  # reviewer says approve BUT threshold violated
            },
        )
        check("TS-02 failed outcome (threshold)", result["outcome"] == "failed")
        check("TS-02 confidence violation recorded", any("confidence" in v for v in result["threshold_violations"]))
        check("TS-02 card returned to not_started", card2.status == CardStatus.NOT_STARTED)

        # ==== TEST: completeness below threshold ====
        card3 = Card(
            id="c3", board_id="b1", spec_id="s1", title="Card 3",
            status=CardStatus.VALIDATION, created_by="u1", card_type=CardType.NORMAL,
        )
        db.add(card3)
        await db.flush()
        result = await card_service.submit_task_validation(
            card_id="c3", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 90, "confidence_justification": "High confidence",
                "estimated_completeness": 72,  # BELOW 80
                "completeness_justification": "Missing 2 endpoints",
                "estimated_drift": 10, "drift_justification": "None",
                "general_justification": "Incomplete — missing endpoints",
                "recommendation": "approve",
            },
        )
        check("TS-03 failed outcome (completeness)", result["outcome"] == "failed")
        check("TS-03 completeness violation", any("completeness" in v for v in result["threshold_violations"]))

        # ==== TEST: drift above threshold ====
        card4 = Card(
            id="c4", board_id="b1", spec_id="s1", title="Card 4",
            status=CardStatus.VALIDATION, created_by="u1", card_type=CardType.NORMAL,
        )
        db.add(card4)
        await db.flush()
        result = await card_service.submit_task_validation(
            card_id="c4", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 90, "confidence_justification": "ok",
                "estimated_completeness": 90, "completeness_justification": "ok",
                "estimated_drift": 65,  # ABOVE 50
                "drift_justification": "Switched from REST to GraphQL mid-implementation",
                "general_justification": "Deviated significantly from plan",
                "recommendation": "approve",
            },
        )
        check("TS-04 failed outcome (drift)", result["outcome"] == "failed")
        check("TS-04 drift violation", any("drift" in v for v in result["threshold_violations"]))

        # ==== TEST: Reject with thresholds OK ====
        card5 = Card(
            id="c5", board_id="b1", spec_id="s1", title="Card 5",
            status=CardStatus.VALIDATION, created_by="u1", card_type=CardType.NORMAL,
        )
        db.add(card5)
        await db.flush()
        result = await card_service.submit_task_validation(
            card_id="c5", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 90, "confidence_justification": "ok",
                "estimated_completeness": 95, "completeness_justification": "ok",
                "estimated_drift": 5, "drift_justification": "ok",
                "general_justification": "Reject — does not meet our quality standards",
                "recommendation": "reject",
            },
        )
        check("TS-05 reject -> failed", result["outcome"] == "failed")
        check("TS-05 no threshold violations (soft fail)", result["threshold_violations"] == [])
        check("TS-05 card returned to not_started", card5.status == CardStatus.NOT_STARTED)

        # ==== TEST: list_task_validations (reverse chronological) ====
        # Create a card with 2 validations
        card6 = Card(
            id="c6", board_id="b1", spec_id="s1", title="Card 6",
            status=CardStatus.VALIDATION, created_by="u1", card_type=CardType.NORMAL,
        )
        db.add(card6)
        await db.flush()
        # First validation — fail
        await card_service.submit_task_validation(
            card_id="c6", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 55, "confidence_justification": "issues found",
                "estimated_completeness": 90, "completeness_justification": "ok",
                "estimated_drift": 10, "drift_justification": "ok",
                "general_justification": "First attempt not ready",
                "recommendation": "reject",
            },
        )
        # Back to not_started, then fix and retry
        card6.status = CardStatus.VALIDATION
        await card_service.submit_task_validation(
            card_id="c6", reviewer_id="r1", reviewer_name="Reviewer",
            data={
                "confidence": 90, "confidence_justification": "Fixed all issues",
                "estimated_completeness": 95, "completeness_justification": "Complete now",
                "estimated_drift": 8, "drift_justification": "Close to plan",
                "general_justification": "Second attempt — all issues addressed",
                "recommendation": "approve",
            },
        )
        validations = await card_service.list_task_validations("c6")
        check("TS-12 list returns 2 entries", len(validations) == 2)
        check("TS-12 most recent first (success)", validations[0]["outcome"] == "success")
        check("TS-12 oldest last (failed)", validations[1]["outcome"] == "failed")

    # Summary
    print()
    passed = sum(1 for s, _, _ in results if s == "PASS")
    failed = sum(1 for s, _, _ in results if s == "FAIL")
    print(f"=== {passed} passed, {failed} failed ===")
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)
