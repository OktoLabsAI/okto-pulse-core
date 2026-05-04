"""Governance module — historical opt-in, ACL violation log, admin audit,
undo mechanism, audit retention, right-to-erasure.

Consolidates all governance operations that the REST API and MCP tools call.
Depends on ConsolidationQueue/ConsolidationAudit models from models/db.py
and the global_discovery cascade from global_discovery/clustering.py.
"""

from __future__ import annotations

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.models.db import (
    Board,
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    KuzuNodeRef,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)

logger = logging.getLogger("okto_pulse.kg.governance")

HISTORICAL_PROGRESS_SETTINGS_KEY = "kg_historical_consolidation"


def _historical_progress_state(board: Board | None) -> dict[str, Any]:
    if board is None or not isinstance(board.settings, dict):
        return {}
    value = board.settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
    return value if isinstance(value, dict) else {}


def _set_historical_progress_state(
    board: Board | None,
    *,
    total: int,
    status: str,
) -> None:
    if board is None:
        return
    settings = dict(board.settings or {})
    current = settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
    current_state = current if isinstance(current, dict) else {}
    settings[HISTORICAL_PROGRESS_SETTINGS_KEY] = {
        **current_state,
        "total": max(0, int(total)),
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": current_state.get("started_at")
        or datetime.now(timezone.utc).isoformat(),
    }
    board.settings = settings
    flag_modified(board, "settings")


async def _historical_queue_counts(
    db: AsyncSession,
    board_id: str,
) -> dict[str, int]:
    rows = (
        await db.execute(
            select(ConsolidationQueue.status, func.count())
            .where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.source == "historical_backfill",
            )
            .group_by(ConsolidationQueue.status)
        )
    ).all()
    counts = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
    for status, count in rows:
        if status in counts:
            counts[status] = int(count)
    return counts


async def _has_materialized_kg_nodes(board_id: str) -> bool:
    """Best-effort check that the per-board KG still contains user nodes.

    Historical progress is persisted in board settings, while the graph file
    can be wiped/recreated independently. If the persisted state says a prior
    backfill completed but the graph has no materialized nodes, callers must be
    allowed to run historical consolidation again.
    """
    try:
        from okto_pulse.core.kg.kg_service import get_kg_service

        rows = await asyncio.to_thread(
            get_kg_service().get_all_nodes,
            board_id,
            min_confidence=0.0,
            min_relevance=0.0,
            max_rows=1,
        )
        return bool(rows)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        logger.debug(
            "governance.historical_progress_graph_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return True


async def _purge_stale_metadata_if_graph_empty(
    db: AsyncSession,
    board_id: str,
) -> bool:
    """Drop SQLite KG mirrors when the physical board graph has no user nodes."""
    has_nodes = await _has_materialized_kg_nodes(board_id)
    if has_nodes:
        return False

    await db.execute(
        delete(KuzuNodeRef).where(KuzuNodeRef.board_id == board_id)
    )
    await db.execute(
        delete(ConsolidationAudit).where(ConsolidationAudit.board_id == board_id)
    )
    await db.execute(
        delete(GlobalUpdateOutbox).where(GlobalUpdateOutbox.board_id == board_id)
    )
    logger.info(
        "governance.historical_start.purged_stale_metadata board=%s",
        board_id,
        extra={
            "event": "governance.historical_start.purged_stale_metadata",
            "board_id": board_id,
        },
    )
    return True


# ---------------------------------------------------------------------------
# Historical opt-in flow (FR-0 through FR-6)
# ---------------------------------------------------------------------------


async def start_historical_consolidation(
    db: AsyncSession,
    board_id: str,
) -> dict:
    """Populate consolidation_queue with low-priority entries for all done
    specs/sprints in the board. Returns counts."""
    import uuid

    board = await db.get(Board, board_id)

    # Check if already in progress
    existing = await db.execute(
        select(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status.in_(["pending", "claimed"]),
        ).limit(1)
    )
    if existing.scalars().first():
        counts = await _historical_queue_counts(db, board_id)
        live_total = sum(counts.values())
        current_total = int(_historical_progress_state(board).get("total") or 0)
        if live_total > 0 and current_total < live_total:
            _set_historical_progress_state(
                board,
                total=live_total,
                status="in_progress",
            )
            await db.commit()
        return {"status": "already_in_progress", "board_id": board_id}

    await _purge_stale_metadata_if_graph_empty(db, board_id)

    # Query done/approved specs for this board
    spec_result = await db.execute(
        select(Spec).where(
            Spec.board_id == board_id,
            Spec.status.in_([SpecStatus.DONE, SpecStatus.APPROVED, SpecStatus.VALIDATED]),
            Spec.archived.is_(False),
        )
    )
    specs = list(spec_result.scalars().all())

    # Query closed sprints for this board
    sprint_result = await db.execute(
        select(Sprint).where(
            Sprint.board_id == board_id,
            Sprint.status == SprintStatus.CLOSED,
            Sprint.archived.is_(False),
        )
    )
    sprints = list(sprint_result.scalars().all())

    # Query cards (any status — Layer 1 worker materialises every card so
    # the hierarchy backbone Spec→Sprint→Card stays consistent in the KG).
    from okto_pulse.core.models.db import Card
    card_result = await db.execute(
        select(Card).where(Card.board_id == board_id)
    )
    cards = list(card_result.scalars().all())

    # Remove completed/failed entries so they can be re-queued.
    # NOTE: we purposely do NOT filter by source — terminal rows from
    # event-driven enqueues (event:card.created, retry_from_ui, …) must
    # also be cleared so the historical pass can reprocess every artifact.
    # The UNIQUE constraint (board_id, artifact_type, artifact_id) means
    # only one row per artifact can exist, so deleting all terminal rows
    # is equivalent to clearing the slot for re-queueing.
    await db.execute(
        delete(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["done", "failed"]),
        )
    )

    # Collect live entries only — pending, claimed, or paused. Terminal
    # rows (done/failed) have just been deleted above, so dedup against
    # them would be incorrect. Including `paused` covers the case where
    # a prior historical run was paused and is still reachable via
    # resume_historical.
    existing_result = await db.execute(
        select(
            ConsolidationQueue.artifact_type,
            ConsolidationQueue.artifact_id,
            ConsolidationQueue.source,
        ).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.status.in_(["pending", "claimed", "paused"]),
        )
    )
    existing_rows = list(existing_result.all())
    already_queued = {(row[0], row[1]) for row in existing_rows}
    existing_historical = {
        (row[0], row[1])
        for row in existing_rows
        if row[2] == "historical_backfill"
    }

    total = 0

    # Insert queue entries for each spec
    for spec in specs:
        if ("spec", spec.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="spec",
            artifact_id=spec.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

    # Insert queue entries for each sprint
    for sprint in sprints:
        if ("sprint", sprint.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="sprint",
            artifact_id=sprint.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

    # Insert queue entries for each card. We deliberately enqueue cards AFTER
    # specs+sprints so the deterministic worker can resolve Card→Sprint /
    # Card→Spec hierarchy edges via the cross-session lookup (the parent
    # Entity is already committed when the card session opens).
    for card in cards:
        if ("card", card.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="card",
            artifact_id=card.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

    run_total = total + len(existing_historical)
    _set_historical_progress_state(
        board,
        total=run_total,
        status="in_progress" if run_total > 0 else "inactive",
    )

    await db.commit()

    logger.info(
        "governance.historical_start board=%s specs=%d sprints=%d cards=%d total=%d",
        board_id, len(specs), len(sprints), len(cards), total,
    )

    if total > 0:
        # Fase 4 — wake the background worker immediately so the freshly
        # enqueued rows start processing without waiting for a heartbeat.
        try:
            from okto_pulse.core.kg.workers.consolidation import (
                signal_consolidation_worker,
            )
            signal_consolidation_worker()
        except Exception:  # pragma: no cover — signal is best-effort
            pass

    return {"status": "queueing", "board_id": board_id, "total_artifacts": run_total}


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
    board = await db.get(Board, board_id)
    result = await db.execute(
        delete(ConsolidationQueue).where(
            ConsolidationQueue.board_id == board_id,
            ConsolidationQueue.source == "historical_backfill",
            ConsolidationQueue.status.in_(["pending", "paused"]),
        )
    )
    current_total = int(_historical_progress_state(board).get("total") or 0)
    _set_historical_progress_state(board, total=current_total, status="cancelled")
    await db.commit()
    return {"status": "cancelled", "board_id": board_id, "removed": result.rowcount}


async def get_historical_progress(db: AsyncSession, board_id: str) -> dict:
    """Return progress of historical consolidation."""
    board = await db.get(Board, board_id)
    state = _historical_progress_state(board)
    counts = await _historical_queue_counts(db, board_id)
    live_total = sum(counts.values())
    total = max(int(state.get("total") or 0), live_total)
    remaining = counts["pending"] + counts["claimed"] + counts["paused"]
    processed = max(0, min(total, total - remaining))
    if state.get("status") == "cancelled" and remaining == 0:
        status = "cancelled"
    elif counts["pending"] or counts["claimed"]:
        status = "in_progress"
    elif counts["paused"]:
        status = "paused"
    elif total > 0 and counts["failed"] > 0:
        status = "completed_with_errors"
    elif total > 0 and processed >= total:
        status = "completed"
    else:
        status = "inactive"

    stale = False
    if status in {"completed", "completed_with_errors"} and total > 0 and remaining == 0:
        has_nodes = await _has_materialized_kg_nodes(board_id)
        if not has_nodes:
            stale = True
            total = 0
            processed = 0
            status = "inactive"

    return {
        "enabled": total > 0,
        "status": status,
        "total": total,
        "progress": processed,
        "pending": counts["pending"],
        "claimed": counts["claimed"],
        "paused": counts["paused"],
        "failed": counts["failed"],
        "stale": stale,
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


_RMTREE_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (0.1, 0.3, 1.0)


def _rmtree_with_retry(path, board_id: str) -> None:
    """Remove a Kùzu board path on any platform without leaking file locks.

    Kùzu 0.11 stores the graph as a single file (``graph.kuzu`` plus a sibling
    ``.wal``), while older/newer versions may use a directory. This helper
    handles both layouts — it also sweeps any ``graph.kuzu.*`` siblings
    (WAL/shadow) that Kùzu may have left behind.

    Windows holds an OS-level lock on any mmap'd file as long as the owning
    process has a live handle. Before the remove can succeed we must:

    1. Close every pooled + global :class:`BoardConnection` that might still
       hold a handle on this board (via :func:`close_all_connections`).
    2. Force a ``gc.collect()`` to drop any stray references.
    3. Sleep 50ms so the OS flushes the handle table.

    After the preamble, the remove runs with up to 3 retries on
    ``PermissionError`` (backoff 0.1s / 0.3s / 1.0s). If all 4 attempts fail,
    re-raise enriched with a ``diagnostic`` line listing still-open handles
    on the path (via psutil when available — best-effort).

    The preamble + retries are critical for the right-to-erasure path: a
    WinError 32 here is user-visible and blocks GDPR compliance.
    """
    import gc
    import time

    from okto_pulse.core.kg.schema import close_all_connections

    close_all_connections(board_id)
    gc.collect()
    time.sleep(0.05)

    last_exc: Exception | None = None
    for attempt in range(len(_RMTREE_RETRY_BACKOFF_SECONDS) + 1):
        try:
            _remove_path(path)
            if attempt:
                logger.info(
                    "governance.rmtree_recovered board=%s attempts=%d",
                    board_id, attempt + 1,
                    extra={
                        "event": "governance.rmtree_recovered",
                        "board_id": board_id,
                        "attempts": attempt + 1,
                    },
                )
            return
        except PermissionError as exc:
            last_exc = exc
            if attempt >= len(_RMTREE_RETRY_BACKOFF_SECONDS):
                break
            backoff = _RMTREE_RETRY_BACKOFF_SECONDS[attempt]
            logger.warning(
                "governance.rmtree_retry board=%s attempt=%d backoff=%.2f err=%s",
                board_id, attempt + 1, backoff, exc,
                extra={
                    "event": "governance.rmtree_retry",
                    "board_id": board_id,
                    "attempt": attempt + 1,
                    "backoff_seconds": backoff,
                },
            )
            close_all_connections(board_id)
            gc.collect()
            time.sleep(backoff)

    assert last_exc is not None
    diag = _diagnose_open_handles(path)
    raise PermissionError(
        f"rmtree failed for {path} after "
        f"{len(_RMTREE_RETRY_BACKOFF_SECONDS) + 1} attempts: {last_exc}. "
        f"Open handles diagnostic: {diag}"
    ) from last_exc


def _remove_path(path) -> None:
    """Delete a Kùzu path whether it's a file or a directory.

    Also sweeps WAL/shadow siblings (``{stem}.wal``, ``{stem}-shm``, etc.)
    that Kùzu may have left outside the primary file.
    """
    import os
    import shutil
    from pathlib import Path

    p = Path(path)
    if p.is_dir():
        shutil.rmtree(str(p))
    elif p.is_file():
        os.remove(str(p))
        # Kùzu 0.11 emits sibling WAL/shadow files (e.g. graph.kuzu.wal).
        # Sweep any that survived so the board dir is truly empty.
        for sibling in p.parent.glob(p.name + ".*"):
            try:
                if sibling.is_file():
                    os.remove(str(sibling))
                elif sibling.is_dir():
                    shutil.rmtree(str(sibling))
            except Exception as exc:
                logger.debug(
                    "governance.sibling_cleanup_skipped sibling=%s err=%s",
                    sibling, exc,
                )


def _diagnose_open_handles(path) -> str:
    """Best-effort list of processes with open handles on ``path``.

    Uses psutil when available — returns a short string suitable for log
    context. Errors (psutil missing, access denied, etc.) collapse to a
    descriptive placeholder so the rmtree error message always carries
    *some* context.
    """
    try:
        import psutil  # type: ignore
    except ImportError:
        return "psutil unavailable"

    target = str(path).lower()
    holders: list[str] = []
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            try:
                for f in proc.open_files() or []:
                    if f.path.lower().startswith(target):
                        holders.append(
                            f"pid={proc.info.get('pid')} "
                            f"name={proc.info.get('name')} file={f.path}"
                        )
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
    except Exception as exc:
        return f"psutil scan failed: {exc}"

    if not holders:
        return "no process reported open handles (lock may be stale)"
    return "; ".join(holders[:10])





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
        cascade = await asyncio.to_thread(board_delete_cascade, board_id)
        counts["global_cascade"] = cascade
    except Exception as exc:
        counts["global_cascade_error"] = str(exc)

    # 2. Kuzu per-board file delete
    try:
        from okto_pulse.core.kg.schema import board_kuzu_path
        path = board_kuzu_path(board_id)
        if path.exists():
            _rmtree_with_retry(path, board_id)
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
        board = await db.get(Board, board_id)
        if board is not None and isinstance(board.settings, dict):
            settings = dict(board.settings or {})
            if HISTORICAL_PROGRESS_SETTINGS_KEY in settings:
                settings.pop(HISTORICAL_PROGRESS_SETTINGS_KEY, None)
                board.settings = settings
                flag_modified(board, "settings")
        await db.commit()
        counts["sqlite_purged"] = True
    except Exception as exc:
        counts["sqlite_purge_error"] = str(exc)

    logger.info(
        "governance.erasure board=%s", board_id,
        extra={"event": "governance.erasure", **counts},
    )
    return counts
