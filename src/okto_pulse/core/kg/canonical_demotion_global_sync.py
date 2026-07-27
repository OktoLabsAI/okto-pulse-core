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
``_apply_event`` such an event resolves to ZERO ``graph reference operation=add`` rows,
so it (a) upserts the Board summary with the absolute authoritative digestable
source count (retry-idempotent, while also refreshing ``last_sync_at``), (b)
prunes nothing (the demoted node still
EXISTS, now ``working``, so it stays in the digestable-id set), (c) runs the R1
parity reconciler (the intended convergence, which runs BEFORE the empty-refs
early return), then returns. No new digest is created and no extra node is touched.
"""

from __future__ import annotations

import logging
import hashlib
import re
import uuid
from typing import Any

from okto_pulse.core.ports.kg_operational import get_kg_worker_audit_port

logger = logging.getLogger("okto_pulse.kg.canonical_demotion_global_sync")

_AUDIT_REASON_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{2,127}$")


async def enqueue_digest_layer_reconciliation(
    context: object,
    *,
    board_id: str,
    reason: str,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Enqueue a board-scoped Global Discovery layer reconciliation.

    The event deliberately contains no graph references (``nodes_added=0``).
    The Global Discovery worker therefore runs its existing board/digest parity
    reconciler without publishing a new digest. Repeating the command is safe:
    each request has a unique audit/event identity, while the materialized effect
    only updates DecisionDigest rows whose layer currently differs from the board
    graph truth.

    ``reason`` is a required, bounded audit code rather than free-form prose. This
    keeps logs and the outbox payload useful for incident review without turning
    an administrative control into an arbitrary-text persistence surface.
    """
    normalized_board_id = str(board_id or "").strip()
    if not normalized_board_id:
        raise ValueError("board_id is required")

    normalized_reason = str(reason or "").strip().lower()
    if not _AUDIT_REASON_RE.fullmatch(normalized_reason):
        raise ValueError(
            "reason must be a 3-128 character audit code using only "
            "lowercase letters, digits, '.', ':', '_' or '-'"
        )

    if idempotency_key is None:
        session_id = f"digestreconcile_{uuid.uuid4().hex[:16]}"
        event_id = str(uuid.uuid4())
    else:
        normalized_key = str(idempotency_key).strip()
        if not normalized_key or len(normalized_key) > 256:
            raise ValueError("idempotency_key must contain 1..256 characters")
        digest = hashlib.sha256(
            f"{normalized_board_id}\x00{normalized_key}".encode("utf-8")
        ).hexdigest()
        session_id = f"digestreconcile_{digest[:16]}"
        event_id = str(
            uuid.uuid5(
                uuid.UUID("7d857bce-1c07-4d51-9cda-fb0e4b1aaf38"),
                f"{normalized_board_id}:{normalized_key}",
            )
        )
    await get_kg_worker_audit_port().emit_outbox_event(
        context,
        event_id=event_id,
        board_id=normalized_board_id,
        session_id=session_id,
        event_type="consolidation_committed",
        payload={
            "session_id": session_id,
            "nodes_added": 0,
            "reason": normalized_reason,
        },
    )
    logger.info(
        "kg.digest_layer.reconcile_enqueued board=%s event=%s reason=%s",
        normalized_board_id,
        event_id,
        normalized_reason,
        extra={
            "event": "kg.digest_layer.reconcile_enqueued",
            "board_id": normalized_board_id,
            "event_id": event_id,
            "reason": normalized_reason,
        },
    )
    return {
        "enqueued": True,
        "board_id": normalized_board_id,
        "event_id": event_id,
        "session_id": session_id,
        "reason": normalized_reason,
        "effect_idempotent": True,
        "idempotency_key": idempotency_key,
    }


async def sync_stale_demotion_to_global_discovery(
    context: object, *, board_id: str
) -> dict[str, Any]:
    """Enqueue a Global Discovery outbox event so the GD worker's R1 parity
    reconciler converges DecisionDigest layers to the (post-demotion) board graph
    truth. Returns the enqueued event identity. Idempotent at the convergence
    layer: the R1 reconciler only rewrites a digest whose layer actually drifted,
    so a repeated sync after convergence is a no-op."""
    result = await enqueue_digest_layer_reconciliation(
        context,
        board_id=board_id,
        reason="r2_stale_demotion_global_sync",
    )
    logger.info(
        "kg.stale_demotion.global_sync_enqueued board=%s event=%s",
        board_id,
        result["event_id"],
        extra={
            "event": "kg.stale_demotion.global_sync_enqueued",
            "board_id": board_id,
            "event_id": result["event_id"],
        },
    )
    return result


__all__ = [
    "enqueue_digest_layer_reconciliation",
    "sync_stale_demotion_to_global_discovery",
]
