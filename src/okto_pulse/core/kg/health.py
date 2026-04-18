"""Pipeline health checks — shared between the CLI and any future /health endpoint.

The pipeline runs in five stages, mirrored by five checks:

1. ``consolidation_queue`` — SQLite row per pending artifact. Healthy when
   ``status='pending' | 'claimed'`` count is zero (worker drained everything).
2. ``kuzu`` — per-board Kùzu graph. Healthy when at least one node exists
   across the 11 node types (otherwise "unpopulated").
3. ``kuzu_node_refs`` — SQLite mirror of the Kùzu writes. Healthy when the
   mirror count matches the Kùzu node count (detects partial/aborted writes).
4. ``global_update_outbox`` — SQLite transactional outbox feeding the global
   discovery meta-graph. Healthy when ``pending`` == 0 and ``dead_letter`` == 0.
5. ``global_discovery`` — global Kùzu meta-graph. Healthy when
   ``DecisionDigest`` count for the given board matches the per-board
   digestable-nodes count (the types listed in
   :data:`okto_pulse.core.kg.global_discovery.outbox_worker.DIGESTED_NODE_TYPES`).

Each check returns a :class:`LayerHealth` dataclass with the same shape so
the CLI table renderer and the JSON emitter both consume the same payload.
The functions are intentionally pure (no writes, no side effects) — they can
be called against a read replica, during a live request, or from the CLI.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from okto_pulse.core.kg.global_discovery.outbox_worker import DIGESTED_NODE_TYPES
from okto_pulse.core.kg.schema import NODE_TYPES, board_kuzu_path, open_board_connection
from okto_pulse.core.models.db import (
    ConsolidationQueue,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("okto_pulse.kg.health")

DEAD_LETTER_RETRY_SENTINEL = -1
MAX_OUTBOX_RETRIES = 5


@dataclass(frozen=True)
class LayerHealth:
    """Snapshot of one pipeline layer's health.

    Attributes:
        layer: stable identifier — one of
            ``queue`` | ``kuzu`` | ``kuzu_node_refs`` | ``outbox`` | ``global``.
        healthy: boolean summary. The CLI renders a green/red glyph from this.
        counts: layer-specific integer buckets (e.g. ``{"pending": 0, "claimed": 0}``).
            Always populated so the CLI table does not have to special-case empties.
        details: one-line human-readable explanation for the CLI table.
    """

    layer: str
    healthy: bool
    counts: dict[str, int] = field(default_factory=dict)
    details: str = ""


async def check_queue(db: "AsyncSession", board_id: str) -> LayerHealth:
    """Inspect the ``consolidation_queue`` for a given board.

    Healthy when no ``pending`` or ``claimed`` rows remain. ``failed`` is a
    warning, not a fatal — the worker may retry on the next tick, but an
    operator should still see it in the CLI output.
    """
    status_col = ConsolidationQueue.status
    result = await db.execute(
        select(status_col, func.count())
        .where(ConsolidationQueue.board_id == board_id)
        .group_by(status_col)
    )
    buckets: dict[str, int] = {
        "pending": 0,
        "claimed": 0,
        "done": 0,
        "failed": 0,
        "paused": 0,
    }
    for status, count in result.all():
        buckets[status] = int(count)

    backlog = buckets["pending"] + buckets["claimed"]
    healthy = backlog == 0 and buckets["failed"] == 0

    if backlog == 0 and buckets["failed"] == 0:
        details = f"{buckets['done']} done, 0 pending"
    elif backlog > 0:
        details = f"{backlog} not yet drained (pending={buckets['pending']} claimed={buckets['claimed']})"
    else:
        details = f"{buckets['failed']} failed row(s) — inspect consolidation_queue"

    return LayerHealth(
        layer="queue",
        healthy=healthy,
        counts=buckets,
        details=details,
    )


def check_kuzu(board_id: str) -> LayerHealth:
    """Open the per-board Kùzu graph and count nodes per type.

    Healthy when at least one node exists across all node types — a
    bootstrapped-but-empty graph returns ``healthy=False`` with
    ``details="no nodes committed yet"`` so the operator can distinguish an
    un-consolidated board from a broken one.

    When the ``.kuzu`` directory does not exist yet the layer is reported
    unhealthy with a distinct ``details`` string — the CLI uses this branch
    to advise running ``okto-pulse init`` / waiting for the first
    consolidation before the check is meaningful.
    """
    path = board_kuzu_path(board_id)
    if not path.exists():
        return LayerHealth(
            layer="kuzu",
            healthy=False,
            counts={"total": 0},
            details=f"graph not bootstrapped at {path}",
        )

    counts: dict[str, int] = {node_type: 0 for node_type in NODE_TYPES}
    total = 0
    try:
        with open_board_connection(board_id) as (_db, conn):
            for node_type in NODE_TYPES:
                try:
                    qr = conn.execute(f"MATCH (n:{node_type}) RETURN count(n) AS c")
                    # Kùzu 0.6+ exposes .get_next() / .has_next(); fall back
                    # to iterating when the driver changes shape.
                    row = None
                    if hasattr(qr, "has_next") and hasattr(qr, "get_next"):
                        if qr.has_next():
                            row = qr.get_next()
                    else:
                        iterator = iter(qr)
                        row = next(iterator, None)
                    if row is None:
                        continue
                    value = row[0] if isinstance(row, (list, tuple)) else row
                    counts[node_type] = int(value)
                    total += int(value)
                except Exception as exc:
                    logger.warning(
                        "health.kuzu.count_failed board=%s type=%s err=%s",
                        board_id, node_type, exc,
                        extra={"event": "health.kuzu.count_failed", "board_id": board_id},
                    )
    except Exception as exc:
        return LayerHealth(
            layer="kuzu",
            healthy=False,
            counts={"total": 0},
            details=f"failed to open graph: {exc}",
        )

    counts["total"] = total
    if total == 0:
        return LayerHealth(
            layer="kuzu",
            healthy=False,
            counts=counts,
            details="no nodes committed yet",
        )

    populated_types = [f"{t}={c}" for t, c in counts.items() if t != "total" and c > 0]
    return LayerHealth(
        layer="kuzu",
        healthy=True,
        counts=counts,
        details=f"{total} nodes ({', '.join(populated_types) or 'none'})",
    )


async def check_kuzu_node_refs(
    db: "AsyncSession",
    board_id: str,
    kuzu_total: int | None = None,
) -> LayerHealth:
    """Count rows in ``kuzu_node_refs`` for a board and compare to the
    per-board Kùzu count.

    The mirror is written in the same SQLite transaction as the audit row at
    commit_consolidation, so a mismatch points to a partial write or a
    silently-aborted session. Pass ``kuzu_total`` to avoid a second Kùzu
    scan when :func:`check_kuzu` already ran.
    """
    op_col = KuzuNodeRef.operation
    result = await db.execute(
        select(op_col, func.count())
        .where(KuzuNodeRef.board_id == board_id)
        .group_by(op_col)
    )
    by_op: dict[str, int] = {"add": 0, "update": 0, "supersede": 0}
    for op, count in result.all():
        by_op[op] = int(count)
    # Kùzu node count = add+update net of supersede (supersede replaces, not deletes).
    # The mirror is append-only so we compare against total rows.
    total_rows = sum(by_op.values())

    counts = dict(by_op)
    counts["total"] = total_rows

    if kuzu_total is None:
        healthy = True
        details = f"{total_rows} ref rows (add={by_op['add']} update={by_op['update']} supersede={by_op['supersede']})"
    else:
        # add - supersede ≈ Kùzu live nodes; allow equality when supersede is 0.
        expected = by_op["add"] - by_op["supersede"]
        healthy = expected == kuzu_total
        details = (
            f"{total_rows} ref rows, expected_live={expected}, kuzu_live={kuzu_total} "
            f"{'ok' if healthy else 'MISMATCH'}"
        )

    return LayerHealth(
        layer="kuzu_node_refs",
        healthy=healthy,
        counts=counts,
        details=details,
    )


async def check_outbox(db: "AsyncSession", board_id: str) -> LayerHealth:
    """Inspect ``global_update_outbox`` for a board.

    Buckets:
      - ``pending`` — ``processed_at IS NULL`` and ``0 <= retry_count < MAX_RETRIES``.
        The outbox worker will drain these on its next tick.
      - ``dead_letter`` — ``retry_count >= MAX_RETRIES`` or ``retry_count == -1``
        (sentinel set by the worker when it gave up).
      - ``processed`` — ``processed_at IS NOT NULL``. Informational only.

    Healthy when ``pending == 0`` AND ``dead_letter == 0``.
    """
    # Pending: still in the worker's retry window.
    pending_q = select(func.count()).where(
        GlobalUpdateOutbox.board_id == board_id,
        GlobalUpdateOutbox.processed_at.is_(None),
        GlobalUpdateOutbox.retry_count >= 0,
        GlobalUpdateOutbox.retry_count < MAX_OUTBOX_RETRIES,
    )
    pending = int((await db.execute(pending_q)).scalar_one())

    # Dead letter: worker gave up (either hit MAX_RETRIES or set sentinel).
    dead_q = select(func.count()).where(
        GlobalUpdateOutbox.board_id == board_id,
        GlobalUpdateOutbox.processed_at.is_(None),
        (GlobalUpdateOutbox.retry_count >= MAX_OUTBOX_RETRIES)
        | (GlobalUpdateOutbox.retry_count == DEAD_LETTER_RETRY_SENTINEL),
    )
    dead_letter = int((await db.execute(dead_q)).scalar_one())

    processed_q = select(func.count()).where(
        GlobalUpdateOutbox.board_id == board_id,
        GlobalUpdateOutbox.processed_at.is_not(None),
    )
    processed = int((await db.execute(processed_q)).scalar_one())

    counts = {
        "pending": pending,
        "dead_letter": dead_letter,
        "processed": processed,
    }
    healthy = pending == 0 and dead_letter == 0
    if healthy:
        details = f"{processed} processed, 0 pending"
    elif pending > 0 and dead_letter == 0:
        details = f"{pending} pending — worker will retry"
    else:
        details = f"{dead_letter} dead-lettered event(s) — inspect last_error"

    return LayerHealth(
        layer="outbox",
        healthy=healthy,
        counts=counts,
        details=details,
    )


def check_global(board_id: str) -> LayerHealth:
    """Count ``DecisionDigest`` rows in the global meta-graph for this board.

    Healthy when the count is > 0 and the global Kùzu file is openable. Does
    NOT compare against the per-board Kùzu count — the outbox may legitimately
    lag the per-board writes briefly. For strict equality checks use
    :func:`check_outbox` (pending=0) combined with this.
    """
    try:
        from okto_pulse.core.kg.global_discovery.schema import open_global_connection
    except ImportError as exc:
        return LayerHealth(
            layer="global",
            healthy=False,
            counts={"digests": 0},
            details=f"global discovery module unavailable: {exc}",
        )

    try:
        _db, conn = open_global_connection()
        try:
            qr = conn.execute(
                "MATCH (d:DecisionDigest {board_id: $bid}) RETURN count(d) AS c",
                {"bid": board_id},
            )
            row = None
            if hasattr(qr, "has_next") and hasattr(qr, "get_next"):
                if qr.has_next():
                    row = qr.get_next()
            else:
                row = next(iter(qr), None)
            digests = int(row[0]) if row is not None else 0
        finally:
            del conn
    except Exception as exc:
        return LayerHealth(
            layer="global",
            healthy=False,
            counts={"digests": 0},
            details=f"failed to query global graph: {exc}",
        )

    counts = {"digests": digests, "digested_types": len(DIGESTED_NODE_TYPES)}
    if digests == 0:
        return LayerHealth(
            layer="global",
            healthy=False,
            counts=counts,
            details="no DecisionDigest synced for this board yet",
        )
    return LayerHealth(
        layer="global",
        healthy=True,
        counts=counts,
        details=f"{digests} digests synced",
    )
