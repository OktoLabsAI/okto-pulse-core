"""Backend tests for SSE events endpoint + retry-from-here (cards e17717a6 + b5a5cc73)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application.use_cases import ActorContext
from okto_pulse.core.domain.realm import RealmScope


def _actor(board_id: str) -> ActorContext:
    return ActorContext(
        "u",
        "rest",
        board_id=board_id,
        realm_scope=RealmScope.local(),
    )


@pytest.mark.asyncio
async def test_sse_endpoint_rejects_invalid_since(db_factory):
    from okto_pulse.community.api.kg_routes import stream_kg_events
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await stream_kg_events(board_id="b", since="not-an-iso")
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_sse_endpoint_returns_streaming_response(db_factory):
    from okto_pulse.community.api.kg_routes import stream_kg_events
    from fastapi.responses import StreamingResponse

    resp = await stream_kg_events(board_id="b1", since=None)
    assert isinstance(resp, StreamingResponse)
    assert resp.media_type == "text/event-stream"
    assert resp.headers.get("cache-control") == "no-cache"


@pytest.mark.asyncio
async def test_sse_endpoint_streams_outbox_events(db_factory):
    """Seed GlobalUpdateOutbox rows and assert the stream emits them."""
    from okto_pulse.community.api.kg_routes import stream_kg_events
    from sqlalchemy_test_models import GlobalUpdateOutbox

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

    resp = await stream_kg_events(
        board_id="b_sse",
        since=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
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
