"""B14 OR4 — policy-constraint projection health/readiness integration."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.kg.health_state import HealthState
from okto_pulse.core.ports.kg_health import (
    KGHealthQueueSnapshot,
    register_kg_health_read_port,
)
from okto_pulse.core.services.kg_health_readiness_service import (
    build_health_readiness,
)
from okto_pulse.core.services.kg_health_service import (
    _build_policy_constraint_projection_health,
    get_kg_health,
)
from sqlalchemy_kg_health_reader import TestSqlAlchemyKGHealthReader
from sqlalchemy_test_models import Board


_STATE_RANK = {
    HealthState.HEALTHY.value: 0,
    HealthState.AT_RISK.value: 1,
    HealthState.BACKPRESSURE.value: 2,
    HealthState.RECOVERY_NEEDED.value: 3,
    HealthState.QUARANTINED.value: 4,
}


class _PolicyProjectionHealthReader(TestSqlAlchemyKGHealthReader):
    def __init__(self, **values: object) -> None:
        self._values = values

    async def queue_snapshot(
        self,
        context: object,
        *,
        board_id: str,
    ) -> KGHealthQueueSnapshot:
        snapshot = await super().queue_snapshot(context, board_id=board_id)
        return replace(snapshot, **self._values)


async def _seed_board(db_factory) -> str:
    board_id = f"skb-b14-health-{uuid.uuid4().hex[:10]}"
    async with db_factory() as db:
        db.add(Board(id=board_id, name="B14 OR4", owner_id="owner"))
        await db.commit()
    return board_id


def _snapshot(**values: object) -> KGHealthQueueSnapshot:
    return replace(
        KGHealthQueueSnapshot(
            board_exists=True,
            queue_depth=0,
            oldest_triggered_at=None,
            dead_letter_count=0,
        ),
        **values,
    )


def test_policy_projection_snapshot_is_backward_compatible_and_separate():
    snapshot = _snapshot()

    assert snapshot.policy_constraint_projection_pending_count == 0
    assert snapshot.policy_constraint_projection_processing_count == 0
    assert snapshot.policy_constraint_projection_retry_scheduled_count == 0
    assert snapshot.policy_constraint_projection_dlq_count == 0
    assert snapshot.policy_constraint_projection_max_attempt_count == 0
    assert snapshot.queue_depth == 0
    assert snapshot.dead_letter_count == 0


def test_policy_projection_normal_retry_is_info_but_stuck_retry_is_warning():
    now = datetime(2026, 7, 29, 12, tzinfo=timezone.utc)
    common = {
        "policy_constraint_projection_retry_scheduled_count": 2,
        "policy_constraint_projection_max_attempt_count": 3,
        "policy_constraint_projection_oldest_retry_scheduled_at": (
            now - timedelta(minutes=2)
        ),
    }
    normal, normal_issues, normal_dlq = (
        _build_policy_constraint_projection_health(
            _snapshot(
                **common,
                policy_constraint_projection_oldest_retry_due_at=(
                    now + timedelta(seconds=30)
                ),
            ),
            now=now,
            stuck_after_seconds=300,
        )
    )
    stuck, stuck_issues, stuck_dlq = (
        _build_policy_constraint_projection_health(
            _snapshot(
                **common,
                policy_constraint_projection_oldest_retry_due_at=(
                    now - timedelta(seconds=301)
                ),
            ),
            now=now,
            stuck_after_seconds=300,
        )
    )

    assert normal["classification"] == "retry_scheduled"
    assert normal["severity"] == "info"
    assert len(normal_issues) == 1
    assert (
        normal_issues[0]["code"]
        == "policy_constraint_projection_retry_scheduled"
    )
    assert normal_issues[0]["severity"] == "info"
    assert normal_dlq is False
    assert stuck["classification"] == "retry_stuck"
    assert stuck["severity"] == "warning"
    assert stuck["retry_overdue_age_seconds"] == 301.0
    assert len(stuck_issues) == 1
    assert (
        stuck_issues[0]["code"]
        == "policy_constraint_projection_retry_stuck"
    )
    assert stuck_issues[0]["severity"] == "warning"
    assert stuck_dlq is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("projection_values", "expected_issue"),
    [
        (
            {
                "policy_constraint_projection_retry_scheduled_count": 1,
                "policy_constraint_projection_max_attempt_count": 2,
                "policy_constraint_projection_oldest_retry_scheduled_at": (
                    datetime.now(timezone.utc) - timedelta(minutes=20)
                ),
                "policy_constraint_projection_oldest_retry_due_at": (
                    datetime.now(timezone.utc) - timedelta(minutes=10)
                ),
            },
            "policy_constraint_projection_retry_stuck",
        ),
        (
            {
                "policy_constraint_projection_dlq_count": 1,
                "policy_constraint_projection_max_attempt_count": 5,
                "policy_constraint_projection_oldest_dlq_at": (
                    datetime.now(timezone.utc) - timedelta(minutes=10)
                ),
            },
            "policy_constraint_projection_dlq",
        ),
    ],
)
async def test_health_payload_promotes_policy_projection_debt_to_at_risk(
    db_factory,
    projection_values,
    expected_issue,
):
    board_id = await _seed_board(db_factory)
    register_kg_health_read_port(
        _PolicyProjectionHealthReader(**projection_values)
    )

    async with db_factory() as db:
        health = await get_kg_health(board_id, db)

    domain = health["operational_domains"]["policy_constraint_projection"]
    assert domain["classification"] in {"retry_stuck", "dead_letter"}
    assert domain["severity"] == "warning"
    assert any(
        issue["code"] == expected_issue
        for issue in health["health_issues"]
    )
    assert _STATE_RANK[health["overall_state"]] >= _STATE_RANK["at_risk"]
    assert expected_issue.split("policy_constraint_projection_", 1)[1] in (
        health["classification_reason"]
    )
    # Existing domains retain exact values; policy delivery is never folded in.
    assert health["queue_depth"] == 0
    assert health["dead_letter_count"] == 0
    assert health["global_outbox_dead_letter_count"] == 0
    assert health["active_queue"]["total_active_depth"] == 0


@pytest.mark.asyncio
async def test_readiness_derives_non_maskable_policy_dlq_from_health_domain(
    db_factory,
):
    board_id = await _seed_board(db_factory)
    oldest_dlq = datetime.now(timezone.utc) - timedelta(minutes=7)
    register_kg_health_read_port(
        _PolicyProjectionHealthReader(
            policy_constraint_projection_pending_count=2,
            policy_constraint_projection_processing_count=1,
            policy_constraint_projection_retry_scheduled_count=3,
            policy_constraint_projection_dlq_count=4,
            policy_constraint_projection_max_attempt_count=5,
            policy_constraint_projection_oldest_dlq_at=oldest_dlq,
        )
    )

    async with db_factory() as db:
        readiness = await build_health_readiness(
            board_id,
            db,
            profile="summary",
            artifact_ref="spec:unrelated",
        )

    signals = readiness["technical_signals"]
    # Established counters remain exact and do not absorb policy DLQ.
    assert signals["dead_letter_count"] == 0
    assert signals["global_outbox_dead_letter_count"] == 0
    assert signals["technical_dlq_count"] == 0
    assert signals["active_queue_count"] == 0
    assert signals["policy_constraint_projection_pending_count"] == 2
    assert signals["policy_constraint_projection_processing_count"] == 1
    assert signals["policy_constraint_projection_retry_scheduled_count"] == 3
    assert signals["policy_constraint_projection_dlq_count"] == 4
    assert signals["policy_constraint_projection_max_attempt_count"] == 5

    items = [
        item
        for item in readiness["non_maskable_items"]
        if item["signal"] == "policy_constraint_projection_dlq"
    ]
    assert len(items) == 1
    assert items[0]["artifact_ref"] == f"board:{board_id}"
    assert items[0]["count"] == 4
    assert "last_error" not in items[0]
    assert "error_text" not in items[0]
    assert readiness["readiness"]["blocking"] is True
    assert (
        "policy_constraint_projection_dlq"
        in readiness["readiness"]["reasons"]
    )
