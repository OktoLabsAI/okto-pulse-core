"""Application-facing startup/self-heal helpers.

Public facade for startup maintenance that Community must replay when it owns
the concrete application lifespan.
"""

from __future__ import annotations

from typing import Any


async def backfill_qa_answered_at(db: Any) -> dict[str, int]:
    """Run the Q&A answered_at self-heal without exposing ``services.main``."""

    from okto_pulse.core.services.main import backfill_qa_answered_at as _backfill

    return await _backfill(db)


__all__ = ["backfill_qa_answered_at"]
