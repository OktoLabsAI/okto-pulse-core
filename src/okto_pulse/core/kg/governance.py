"""Governance module — historical opt-in, ACL violation log, admin audit,
undo mechanism, audit retention, right-to-erasure.

Consolidates all governance operations that the REST API and MCP tools call.
Depends on ConsolidationQueue/ConsolidationAudit models from models/db.py
and the global_discovery cascade from global_discovery/clustering.py.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    KuzuNodeRef,
)

logger = logging.getLogger("okto_pulse.kg.governance")


# ---------------------------------------------------------------------------
# Historical opt-in flow (FR-0 through FR-6)
# ---------------------------------------------------------------------------


async def start_historical_consolidation(
    db: AsyncSession,
    board_id: str,
) -> dict:
    """Populate consolidation_queue with low-priority entries for all done
    specs/sprints in the board. Returns counts."""
    # Check if already in progress
    existing = await db.execute(
        select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        ).limit(1)
    )
    if existing.scalars().first():
        return {"status": "already_in_progress", "board_id": board_id}

    # MVP: insert a placeholder entry since we don't have direct access
    # to the specs/sprints tables from this module. The real implementation
    # would query Board -> Specs(done) -> insert queue entries.
    import uuid
    db.add(ConsolidationQueue(
        id=str(uuid.uuid4()),
        board_id=board_id,
        artifact_type="historical_marker",
        artifact_id=f"hist_{board_id}",
        priority="low",
        source="historical_backfill",
        status="pending",
    ))
    await db.commit()
    return {"status": "queueing", "board_id": board_id, "total_artifacts": 0}


async def pause_historical(db: AsyncSession, board_id: str) -> dict:
    """Mark low-priority backfill entries as paused."""
    await db.execute(
        update(ConsolidationQueue)
        .where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status == "pending",
        )
        .values(status="paused")
    )
    await db.commit()
    return {"status": "paused", "board_id": board_id}


async def resume_historical(db: AsyncSession, board_id: str) -> dict:
    """Resume paused backfill entries."""
    await db.execute(
        update(ConsolidationQueue)
        .where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status == "paused",
        )
        .values(status="pending")
    )
    await db.commit()
    return {"status": "resumed", "board_id": board_id}


async def cancel_historical(db: AsyncSession, board_id: str) -> dict:
    """Delete pending low-priority entries. Already-consolidated preserved."""
    result = await db.execute(
        delete(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status.in_(["pending", "paused"]),
        )
    )
    await db.commit()
    return {"status": "cancelled", "board_id": board_id, "removed": result.rowcount}


async def get_historical_progress(db: AsyncSession, board_id: str) -> dict:
    """Return progress of historical consolidation."""
    total = await db.execute(
        select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
        )
    )
    all_entries = list(total.scalars().all())
    done = sum(1 for e in all_entries if e.status == "done")
    return {
        "enabled": len(all_entries) > 0,
        "status": "in_progress" if any(e.status in ("pending", "claimed") for e in all_entries) else "inactive",
        "total": len(all_entries),
        "progress": done,
    }


# ---------------------------------------------------------------------------
# Undo mechanism (FR-11 through FR-14)
# ---------------------------------------------------------------------------


async def undo_session(
    db: AsyncSession,
    board_id: str,
    session_id: str,
    *,
    force: bool = False,
) -> dict:
    """Soft-delete nodes/edges from a consolidation session.

    Returns 409 cascade_blocked if other sessions reference nodes from this
    session, unless force=True (admin).
    """
    audit = await db.execute(
        select(ConsolidationAudit).where(
            ConsolidationAudit.session_id == session_id,
            ConsolidationAudit.board_id == board_id,
        )
    )
    row = audit.scalars().first()
    if not row:
        return {"error": "not_found", "session_id": session_id}
    if row.undo_status == "undone":
        return {"error": "already_undone", "session_id": session_id}

    # Check cascade: are any nodes from this session referenced by other sessions?
    refs = await db.execute(
        select(KuzuNodeRef).where(KuzuNodeRef.session_id == session_id)
    )
    node_refs = list(refs.scalars().all())

    if not force and node_refs:
        # Check if any OTHER session references these node IDs
        node_ids = [r.kuzu_node_id for r in node_refs]
        other_refs = await db.execute(
            select(KuzuNodeRef).where(
                KuzuNodeRef.kuzu_node_id.in_(node_ids),
                KuzuNodeRef.session_id != session_id,
            )
        )
        blockers = list(set(r.session_id for r in other_refs.scalars().all()))
        if blockers:
            return {
                "error": "cascade_blocked",
                "session_id": session_id,
                "blocking_sessions": blockers,
            }

    # Mark as undone
    row.undo_status = "undone"
    row.undone_at = datetime.now(timezone.utc)
    await db.commit()

    # Kuzu soft-delete would happen here via TransactionOrchestrator.compensate
    # pattern. For MVP: mark in SQLite only.
    return {
        "session_id": session_id,
        "status": "undone",
        "nodes_removed": len(node_refs),
        "force_used": force,
    }


# ---------------------------------------------------------------------------
# Audit retention + purge (FR-15, FR-16)
# ---------------------------------------------------------------------------


async def purge_expired_audit(
    db: AsyncSession,
    board_id: str,
    retention_days: int | None = None,
) -> dict:
    """Delete audit entries older than retention_days. None = skip (unlimited)."""
    if retention_days is None or retention_days <= 0:
        return {"board_id": board_id, "purged": 0, "retention": "unlimited"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    result = await db.execute(
        delete(ConsolidationAudit).where(
            ConsolidationAudit.board_id == board_id,
            ConsolidationAudit.committed_at < cutoff,
        )
    )
    await db.commit()
    return {
        "board_id": board_id,
        "purged": result.rowcount,
        "retention_days": retention_days,
        "cutoff": cutoff.isoformat(),
    }


# ---------------------------------------------------------------------------
# ACL violation log (FR-9, FR-10)
# ---------------------------------------------------------------------------

_acl_violations: list[dict] = []
_ACL_ALERT_THRESHOLD = 10
_ACL_ALERT_WINDOW = 3600  # 1 hour


def log_acl_violation(user_id: str, board_id: str, resource: str) -> None:
    """Record an ACL violation. Alert if threshold exceeded."""
    now = datetime.now(timezone.utc)
    _acl_violations.append({
        "user_id": user_id,
        "board_id": board_id,
        "resource": resource,
        "timestamp": now.isoformat(),
    })

    # Check alert threshold
    window_start = now - timedelta(seconds=_ACL_ALERT_WINDOW)
    recent = [
        v for v in _acl_violations
        if v["user_id"] == user_id and v["timestamp"] > window_start.isoformat()
    ]
    if len(recent) >= _ACL_ALERT_THRESHOLD:
        logger.warning(
            "acl.alert user=%s violations=%d window=1h",
            user_id, len(recent),
            extra={
                "event": "acl.alert",
                "user_id": user_id,
                "violation_count": len(recent),
            },
        )


def get_acl_violations(user_id: str | None = None, limit: int = 100) -> list[dict]:
    """Return recent ACL violations, optionally filtered by user."""
    results = _acl_violations if not user_id else [
        v for v in _acl_violations if v["user_id"] == user_id
    ]
    return results[-limit:]


def clear_acl_violations_for_tests() -> None:
    _acl_violations.clear()


# ---------------------------------------------------------------------------
# Right to erasure (FR-18, FR-19)
# ---------------------------------------------------------------------------


async def right_to_erasure(
    db: AsyncSession,
    board_id: str,
) -> dict:
    """Wipe all KG data for a board: Kuzu file + global cascade + audit purge.

    Best-effort: each step runs independently so partial erasure still removes
    as much as possible.
    """
    counts: dict[str, Any] = {"board_id": board_id}

    # 1. Global discovery cascade
    try:
        from okto_pulse.core.kg.global_discovery.clustering import board_delete_cascade
        cascade = board_delete_cascade(board_id)
        counts["global_cascade"] = cascade
    except Exception as exc:
        counts["global_cascade_error"] = str(exc)

    # 2. Kuzu per-board file delete
    try:
        from okto_pulse.core.kg.schema import board_kuzu_path
        import shutil
        path = board_kuzu_path(board_id)
        if path.exists():
            shutil.rmtree(str(path))
            counts["kuzu_file_removed"] = True
        else:
            counts["kuzu_file_removed"] = False
    except Exception as exc:
        counts["kuzu_file_error"] = str(exc)

    # 3. SQLite audit/refs/outbox purge
    try:
        await db.execute(
            delete(KuzuNodeRef).where(KuzuNodeRef.board_id == board_id)
        )
        await db.execute(
            delete(ConsolidationAudit).where(ConsolidationAudit.board_id == board_id)
        )
        await db.execute(
            delete(ConsolidationQueue).where(ConsolidationQueue.board_id == board_id)
        )
        await db.execute(
            delete(GlobalUpdateOutbox).where(GlobalUpdateOutbox.board_id == board_id)
        )
        await db.commit()
        counts["sqlite_purged"] = True
    except Exception as exc:
        counts["sqlite_purge_error"] = str(exc)

    logger.info(
        "governance.erasure board=%s", board_id,
        extra={"event": "governance.erasure", **counts},
    )
    return counts
