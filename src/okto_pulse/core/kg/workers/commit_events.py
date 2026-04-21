"""Emit `kg.session.committed` events to the outbox table.

The SSE stream (separate card `e17717a6`) consumes these events to push
canvas updates to the React IDE. Keeping the emission wrapped in a small
helper lets us decouple the primitives commit path from the serialisation
shape — the SSE route reads whatever we drop here.

Payload contract (frozen for the SSE side):
    {
      "event_type": "kg.session.committed",
      "board_id": str,
      "session_id": str,
      "artifact_type": str,
      "artifact_id": str,
      "nodes_added": int,
      "edges_added": int,
      "content_hash": str,
      "committed_at": ISO8601 string,
    }
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import GlobalUpdateOutbox

logger = logging.getLogger("okto_pulse.kg.commit_events")


EVENT_TYPE_SESSION_COMMITTED = "kg.session.committed"
EVENT_TYPE_BOARD_CLEARED = "kg.board.cleared"


async def emit_session_committed(
    session: AsyncSession,
    *,
    board_id: str,
    session_id: str,
    artifact_type: str,
    artifact_id: str,
    nodes_added: int,
    edges_added: int,
    content_hash: str,
) -> str:
    """Insert a kg.session.committed event. Returns the event_id.

    Must be called inside the same SQLAlchemy transaction as the KG commit
    so the outbox row lands atomically with the audit row (standard
    transactional outbox pattern).
    """
    event_id = str(uuid.uuid4())
    payload = {
        "event_type": EVENT_TYPE_SESSION_COMMITTED,
        "board_id": board_id,
        "session_id": session_id,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "nodes_added": nodes_added,
        "edges_added": edges_added,
        "content_hash": content_hash,
        "committed_at": datetime.now(timezone.utc).isoformat(),
    }
    row = GlobalUpdateOutbox(
        event_id=event_id,
        board_id=board_id,
        session_id=session_id,
        event_type=EVENT_TYPE_SESSION_COMMITTED,
        payload=payload,
    )
    session.add(row)
    logger.info(
        "commit_events.session_committed board=%s session=%s nodes=%d edges=%d",
        board_id, session_id, nodes_added, edges_added,
        extra={"event": "commit_events.session_committed",
               "board_id": board_id, "session_id": session_id,
               "artifact_id": artifact_id,
               "nodes_added": nodes_added, "edges_added": edges_added},
    )
    return event_id


async def emit_board_cleared(
    session: AsyncSession,
    *,
    board_id: str,
    reason: str = "",
) -> str:
    """Insert a kg.board.cleared event (right-to-erasure, reset, etc)."""
    event_id = str(uuid.uuid4())
    payload: dict[str, Any] = {
        "event_type": EVENT_TYPE_BOARD_CLEARED,
        "board_id": board_id,
        "reason": reason,
        "cleared_at": datetime.now(timezone.utc).isoformat(),
    }
    row = GlobalUpdateOutbox(
        event_id=event_id,
        board_id=board_id,
        session_id="",
        event_type=EVENT_TYPE_BOARD_CLEARED,
        payload=payload,
    )
    session.add(row)
    logger.info(
        "commit_events.board_cleared board=%s reason=%s",
        board_id, reason,
        extra={"event": "commit_events.board_cleared",
               "board_id": board_id, "reason": reason},
    )
    return event_id
