"""Regression coverage for bug-card test gates on already validated specs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import uuid

import pytest

from okto_pulse.core.models.db import (
    Board,
    BugSeverity,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.services.main import CardService


pytestmark = pytest.mark.asyncio

USER_ID = "bug-regression-agent"


async def test_bug_gate_allows_existing_scenario_with_new_test_card():
    """A locked spec can reuse an existing scenario if the test card is new."""
    from okto_pulse.core.infra.database import get_session_factory

    factory = get_session_factory()
    board_id = f"bug-gate-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"bug-gate-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    test_id = f"test-{uuid.uuid4().hex[:8]}"
    scenario_id = "ts-existing-regression"
    now = datetime.now(timezone.utc)

    async with factory() as db:
        db.add(Board(id=board_id, name="Bug Gate Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id,
            board_id=board_id,
            title="Validated regression spec",
            status=SpecStatus.IN_PROGRESS,
            created_by=USER_ID,
            functional_requirements=["FR1"],
            acceptance_criteria=["AC1"],
            test_scenarios=[{
                "id": scenario_id,
                "title": "Existing regression scenario",
                "linked_criteria": [0],
                "linked_task_ids": [test_id],
                "status": "passed",
                "evidence": {
                    "last_run_at": "2026-05-01T12:00:00Z",
                    "output_snippet": "passed",
                },
            }],
            business_rules=[],
            api_contracts=[],
        ))
        db.add(Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin implementation",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            created_by=USER_ID,
            created_at=now - timedelta(minutes=5),
        ))
        db.add(Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug needing regression",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            severity=BugSeverity.MAJOR,
            expected_behavior="request succeeds",
            observed_behavior="request fails",
            linked_test_task_ids=[test_id],
            created_by=USER_ID,
            created_at=now,
        ))
        db.add(Card(
            id=test_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Regression test created after bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.TEST,
            test_scenario_ids=[scenario_id],
            created_by=USER_ID,
            created_at=now + timedelta(seconds=1),
        ))
        await db.flush()

        moved = await CardService(db).move_card(
            bug_id,
            USER_ID,
            CardMove(status=CardStatus.IN_PROGRESS),
        )

    assert moved is not None
    assert moved.status == CardStatus.IN_PROGRESS
