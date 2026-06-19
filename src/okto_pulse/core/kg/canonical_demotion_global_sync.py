"""R2-IMP5 — synchronize deterministic stale-canonical demotion with Global
Discovery via the R1 parity contract.

After R2-IMP1 demotes a stale canonical DETERMINISTIC node in the board graph
(canonical -> working), the Global Discovery ``DecisionDigest`` for that node must
converge to the same layer truth so ``query_global(graph_layer=canonical)`` stops
returning the now-obsolete canonical fact (``all`` still surfaces it as working,
diagnostic). This module does NOT reimplement dedup / embeddings /
expected_digest_layer — it simply ENQUEUES a Global Discovery outbox event so the
GD outbox worker runs the EXISTING R1-IMP1 parity reconciler
(``OutboxWorker._reconcile_board_digest_layers``) which recomputes the expected
publication layer from the board graph and corrects each digest in place
(preserving ``original_node_id``/``source_artifact_ref``; the R7 carve-out keeps
cognitive canonical digests intact).

Why a ``nodes_added=0`` event is safe (no fragile empty-event side effects): in
``_apply_event`` such an event resolves to ZERO ``KuzuNodeRef operation=add`` rows,
so it (a) upserts the Board summary with ``+0`` decision_count (only refreshes
``last_sync_at`` — benign metadata), (b) prunes nothing (the demoted node still
EXISTS, now ``working``, so it stays in the digestable-id set), (c) runs the R1
parity reconciler (the intended convergence, which runs BEFORE the empty-refs
early return), then returns. No new digest is created and no extra node is touched.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import GlobalUpdateOutbox

logger = logging.getLogger("okto_pulse.kg.canonical_demotion_global_sync")


async def sync_stale_demotion_to_global_discovery(
    db: AsyncSession, *, board_id: str
) -> dict[str, Any]:
    """Enqueue a Global Discovery outbox event so the GD worker's R1 parity
    reconciler converges DecisionDigest layers to the (post-demotion) board graph
    truth. Returns the enqueued event identity. Idempotent at the convergence
    layer: the R1 reconciler only rewrites a digest whose layer actually drifted,
    so a repeated sync after convergence is a no-op."""
    session_id = f"stalesync_{uuid.uuid4().hex[:16]}"
    event_id = str(uuid.uuid4())
    db.add(GlobalUpdateOutbox(
        event_id=event_id,
        board_id=board_id,
        session_id=session_id,
        event_type="consolidation_committed",
        payload={"session_id": session_id, "nodes_added": 0,
                 "reason": "r2_stale_demotion_global_sync"},
    ))
    await db.flush()
    logger.info(
        "kg.stale_demotion.global_sync_enqueued board=%s event=%s",
        board_id, event_id,
        extra={"event": "kg.stale_demotion.global_sync_enqueued",
               "board_id": board_id, "event_id": event_id},
    )
    return {"enqueued": True, "event_id": event_id, "session_id": session_id}


__all__ = ["sync_stale_demotion_to_global_discovery"]
