"""Terminal GlobalUpdateOutbox health remains separate from the active queue."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from sqlalchemy_test_models import Board, GlobalUpdateOutbox

from okto_pulse.core.services.kg_health_readiness_service import (
    build_health_readiness,
)
from okto_pulse.core.services.kg_health_service import get_kg_health
from okto_pulse.core.services.queue_health_service import (
    get_active_queue_drilldown,
    get_global_outbox_dead_letter_drilldown,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _outbox(
    board_id: str,
    *,
    retry_count: int,
    processed: bool = False,
    last_error: str | None = None,
    age_seconds: int = 30,
) -> GlobalUpdateOutbox:
    now = datetime.now(timezone.utc)
    return GlobalUpdateOutbox(
        id=_id("outbox"),
        event_id=_id("event"),
        board_id=board_id,
        session_id=_id("session"),
        event_type="node_upsert",
        payload={"must_not_leak": "payload"},
        retry_count=retry_count,
        processed_at=now if processed else None,
        created_at=now - timedelta(seconds=age_seconds),
        last_error=last_error,
    )


@pytest.mark.asyncio
async def test_global_outbox_dead_letter_drilldown_is_bounded_and_read_only(
    db_factory,
):
    board_id = _id("global-dlq-board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="global dlq", owner_id="health-user"))
        await db.flush()
        db.add(_outbox(board_id, retry_count=0, last_error="active"))
        db.add(
            _outbox(
                board_id,
                retry_count=5,
                last_error="MemoryError: bad allocation",
                age_seconds=120,
            )
        )
        db.add(
            _outbox(
                board_id,
                retry_count=-1,
                last_error=(
                    "board_read failed at C:\\Users\\operator\\graph.lbug "
                    "postgresql://user:secret@localhost/pulse"
                ),
                age_seconds=90,
            )
        )
        db.add(_outbox(board_id, retry_count=5, processed=True))
        await db.commit()

        active = await get_active_queue_drilldown(db, board_id)
        drilldown = await get_global_outbox_dead_letter_drilldown(
            db,
            board_id,
            limit=1,
        )
        full_drilldown = await get_global_outbox_dead_letter_drilldown(
            db,
            board_id,
            limit=2,
        )

    assert active["total_active_depth"] == 1
    assert drilldown["read_only"] is True
    assert drilldown["total_count"] == 2
    assert drilldown["returned_count"] == 1
    assert drilldown["truncated"] is True
    item = drilldown["items"][0]
    assert item["classification"] == "global_open_failure"
    assert item["next_action"] == (
        "inspect_global_discovery_health_before_requeue"
    )
    assert len(item["last_error"]) <= 240
    assert "payload" not in item
    redacted = full_drilldown["items"][1]
    assert redacted["classification"] == "board_source_failure"
    assert redacted["last_error"] == "global_outbox_error_redacted"
    assert "operator" not in redacted["last_error"]
    assert "postgresql" not in redacted["last_error"]


@pytest.mark.asyncio
async def test_global_outbox_dead_letter_is_separate_non_maskable_readiness(
    db_factory,
):
    board_id = _id("global-readiness-board")
    async with db_factory() as db:
        db.add(
            Board(id=board_id, name="global readiness", owner_id="health-user")
        )
        await db.flush()
        db.add(
            _outbox(
                board_id,
                retry_count=-1,
                last_error="graph_corruption while opening global discovery",
            )
        )
        await db.commit()

        health = await get_kg_health(board_id, db)
        readiness = await build_health_readiness(
            board_id,
            db,
            profile="summary",
        )

    domains = health["operational_domains"]
    assert health["dead_letter_count"] == 0
    assert health["global_outbox_dead_letter_count"] == 1
    assert domains["dead_letter"]["count"] == 0
    assert domains["global_outbox_dead_letter"]["count"] == 1
    assert domains["active_queue"]["count"] == 0
    assert domains["global_outbox_dead_letter"]["drill_down_tool"] == (
        "okto_pulse_kg_global_outbox_dead_letter_list"
    )
    diagnostic = next(
        item
        for item in health["health_issues"]
        if item["code"] == "global_outbox_dead_letter_backlog"
    )
    assert diagnostic["drill_down_tool"] == (
        "okto_pulse_kg_global_outbox_dead_letter_list"
    )

    signals = readiness["technical_signals"]
    assert signals["dead_letter_count"] == 0
    assert signals["global_outbox_dead_letter_count"] == 1
    assert signals["technical_dlq_count"] == 1
    assert readiness["readiness"]["blocking"] is True
    assert "global_outbox_dead_letter" in readiness["readiness"]["reasons"]
    outbox_items = [
        item
        for item in readiness["non_maskable_items"]
        if item["signal"] == "global_outbox_dead_letter"
    ]
    assert len(outbox_items) == 1
    assert outbox_items[0]["artifact_ref"].startswith(
        "global_update_outbox:"
    )
    assert outbox_items[0]["drill_down_tool"] == (
        "okto_pulse_kg_global_outbox_dead_letter_list"
    )
