"""Backend tests for SSE events endpoint + retry-from-here (cards e17717a6 + b5a5cc73)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest


@pytest.mark.asyncio
async def test_sse_endpoint_rejects_invalid_since(db_factory):
    from okto_pulse.core.api.kg_routes import stream_kg_events
    from fastapi import HTTPException

    factory = db_factory
    async with factory() as db:
        with pytest.raises(HTTPException) as exc:
            await stream_kg_events(
                board_id="b", since="not-an-iso", db=db,
            )
        assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_sse_endpoint_returns_streaming_response(db_factory):
    from okto_pulse.core.api.kg_routes import stream_kg_events
    from fastapi.responses import StreamingResponse

    factory = db_factory
    async with factory() as db:
        resp = await stream_kg_events(board_id="b1", since=None, db=db)
    assert isinstance(resp, StreamingResponse)
    assert resp.media_type == "text/event-stream"
    assert resp.headers.get("cache-control") == "no-cache"


@pytest.mark.asyncio
async def test_sse_endpoint_streams_outbox_events(db_factory):
    """Seed GlobalUpdateOutbox rows and assert the stream emits them."""
    from okto_pulse.core.api.kg_routes import stream_kg_events
    from okto_pulse.core.models.db import GlobalUpdateOutbox

    factory = db_factory
    async with factory() as db:
        db.add(GlobalUpdateOutbox(
            event_id="evt_1",
            board_id="b_sse",
            session_id="ses_1",
            event_type="kg.session.committed",
            payload={"node_count": 3, "edge_count": 2},
        ))
        await db.commit()

    async with factory() as db:
        resp = await stream_kg_events(
            board_id="b_sse",
            since=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            db=db,
        )
        body = resp.body_iterator
        collected: list[str] = []
        async for chunk in body:
            collected.append(chunk)
            # Two events (hello + committed) + keepalive is enough to stop.
            if len(collected) >= 3:
                break

    blob = "".join(collected)
    assert "event: hello" in blob
    assert "event: kg.session.committed" in blob
    assert "evt_1" in blob


# ---------------------------------------------------------------------------
# Retry endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_endpoint_404_when_entry_missing(db_factory):
    from fastapi import HTTPException

    from okto_pulse.core.api.kg_routes import retry_pending_entry

    factory = db_factory
    async with factory() as db:
        with pytest.raises(HTTPException) as exc:
            await retry_pending_entry(
                board_id="b", queue_entry_id="does-not-exist",
                recursive=False, db=db,
            )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_retry_endpoint_resets_failed_entry(db_factory):
    from okto_pulse.core.api.kg_routes import retry_pending_entry
    from okto_pulse.core.models.db import ConsolidationQueue

    factory = db_factory
    async with factory() as db:
        entry = ConsolidationQueue(
            board_id="b_retry",
            artifact_type="spec",
            artifact_id="spec_r_1",
            status="failed",
            source="historical_backfill",
            claimed_by_session_id="prev_session",
        )
        db.add(entry)
        await db.commit()
        entry_id = entry.id

    async with factory() as db:
        result = await retry_pending_entry(
            board_id="b_retry", queue_entry_id=entry_id,
            recursive=False, db=db,
        )

    assert result["reopened_count"] == 1
    assert result["reopened_ids"] == [entry_id]
    async with factory() as db:
        refreshed = await db.get(ConsolidationQueue, entry_id)
        assert refreshed.status == "pending"
        assert refreshed.claimed_at is None
        assert refreshed.claimed_by_session_id is None
        assert refreshed.source == "retry_from_ui"


@pytest.mark.asyncio
async def test_retry_endpoint_recursive_reopens_descendants(db_factory):
    """When replaying a spec, cards/sprints under it must also be reopened."""
    from okto_pulse.core.api.kg_routes import retry_pending_entry
    from okto_pulse.core.models.db import (
        Board, Card, ConsolidationQueue, Spec, Sprint,
    )

    factory = db_factory
    async with factory() as db:
        board = Board(id="b_rec", name="n", description="", owner_id="u")
        spec = Spec(id="spec_rec", board_id="b_rec", title="t",
                    description="", created_by="u")
        sprint = Sprint(id="sprint_rec", board_id="b_rec",
                        spec_id="spec_rec", title="s", created_by="u")
        card = Card(id="card_rec", board_id="b_rec", spec_id="spec_rec",
                    sprint_id="sprint_rec", title="c", created_by="u")
        spec_entry = ConsolidationQueue(
            board_id="b_rec", artifact_type="spec", artifact_id="spec_rec",
            status="failed", source="initial",
        )
        sprint_entry = ConsolidationQueue(
            board_id="b_rec", artifact_type="sprint", artifact_id="sprint_rec",
            status="done", source="initial",
        )
        card_entry = ConsolidationQueue(
            board_id="b_rec", artifact_type="card", artifact_id="card_rec",
            status="done", source="initial",
        )
        db.add_all([board, spec, sprint, card, spec_entry, sprint_entry, card_entry])
        await db.commit()
        spec_entry_id = spec_entry.id
        sprint_entry_id = sprint_entry.id
        card_entry_id = card_entry.id

    async with factory() as db:
        result = await retry_pending_entry(
            board_id="b_rec", queue_entry_id=spec_entry_id,
            recursive=True, db=db,
        )

    assert result["recursive"] is True
    # Spec itself + sprint + card should be reopened (min 3).
    assert result["reopened_count"] >= 3
    assert spec_entry_id in result["reopened_ids"]
    assert sprint_entry_id in result["reopened_ids"]
    assert card_entry_id in result["reopened_ids"]

    async with factory() as db:
        for entry_id in (spec_entry_id, sprint_entry_id, card_entry_id):
            refreshed = await db.get(ConsolidationQueue, entry_id)
            assert refreshed.status == "pending"
