"""Bounded, backend-neutral observations of consolidation's await boundaries.

No payload, query, error message or candidate identifier is recorded. These are
elapsed phases, not storage timings or proof of relational durability.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from time import perf_counter
from typing import Literal, TypeVar

_T = TypeVar("_T")
_Phase = Literal[
    "health_admission",
    "graph_dispatch",
    "cognitive_source_append",
    "audit_outbox_stage",
    "session_finalize",
]
logger = logging.getLogger(__name__)


async def observe_consolidation_phase(
    phase: _Phase, session_id: str, operation: Awaitable[_T]
) -> _T:
    """Await exactly once; preserve result, exception and cancellation semantics.

    The caller still owns shielding, locks, compensation and finalization. In
    particular ``graph_dispatch`` includes executor queuing; ``audit_outbox_stage``
    does not assert that a caller-owned relational transaction has committed.
    Ordinary telemetry failures must not turn an applied write into an apparent
    failure or replace the original exception.
    """
    started = None
    try:
        if logger.isEnabledFor(logging.INFO):
            started = perf_counter()
    except Exception:
        # Diagnostics are optional; the operation must still be awaited.
        pass
    outcome = "raised"
    try:
        result = await operation
        outcome = "returned"
        return result
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    finally:
        if started is not None:
            try:
                elapsed = max(0.0, perf_counter() - started)
                logger.info(
                    "kg.consolidation.phase session=%s phase=%s outcome=%s elapsed_s=%.6f",
                    session_id,
                    phase,
                    outcome,
                    elapsed,
                    extra={
                        "event": "kg.consolidation.phase",
                        "session_id": session_id,
                        "phase": phase,
                        "outcome": outcome,
                        "elapsed_s": elapsed,
                    },
                )
            except Exception:
                pass
