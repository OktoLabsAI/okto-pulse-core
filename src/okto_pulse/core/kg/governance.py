"""Governance module — historical opt-in, ACL violation log, admin audit,
undo mechanism, audit retention, right-to-erasure.

Consolidates all governance operations that the REST API and MCP tools call.
Depends on ConsolidationQueue/ConsolidationAudit models from models/db.py
and the global_discovery cascade from global_discovery/clustering.py.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from okto_pulse.core.ports.kg_events import HISTORICAL_PROGRESS_SETTINGS_KEY
from okto_pulse.core.ports.kg_governance import (
    BoostAuditRecord,
    HistoricalBoardRecord,
    HistoricalQueueInsert,
    get_kg_governance_store,
)

logger = logging.getLogger("okto_pulse.kg.governance")

def _historical_progress_state(
    board: HistoricalBoardRecord | None,
) -> dict[str, Any]:
    if board is None or not isinstance(board.settings, dict):
        return {}
    value = board.settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
    return value if isinstance(value, dict) else {}


def _set_historical_progress_state(
    board: HistoricalBoardRecord | None,
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


async def _historical_queue_counts(
    db: Any,
    board_id: str,
) -> dict[str, int]:
    counts = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
    counts.update(await get_kg_governance_store().queue_counts(db, board_id=board_id))
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
    db: Any,
    board_id: str,
) -> bool:
    """Drop SQLite KG mirrors when the physical board graph has no user nodes."""
    has_nodes = await _has_materialized_kg_nodes(board_id)
    if has_nodes:
        return False

    await get_kg_governance_store().purge_stale_metadata(
        db,
        board_id=board_id,
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
    db: Any,
    board_id: str,
) -> dict:
    """Populate consolidation_queue with low-priority entries for all done
    specs/sprints in the board. Returns counts."""
    import uuid

    store = get_kg_governance_store()
    board = await store.get_board(db, board_id=board_id)

    # Check if already in progress
    live_queue = await store.list_live_queue(db, board_id=board_id)
    if any(
        row.source == "historical_backfill"
        and row.status in {"pending", "claimed"}
        for row in live_queue
    ):
        counts = await _historical_queue_counts(db, board_id)
        live_total = sum(counts.values())
        current_total = int(_historical_progress_state(board).get("total") or 0)
        if live_total > 0 and current_total < live_total:
            _set_historical_progress_state(
                board,
                total=live_total,
                status="in_progress",
            )
            if board is not None:
                await store.save_board(db, board)
            await store.commit(db)
        return {"status": "already_in_progress", "board_id": board_id}

    await _purge_stale_metadata_if_graph_empty(db, board_id)

    artifacts = await store.list_historical_artifacts(db, board_id=board_id)
    by_type = {
        artifact_type: [row for row in artifacts if row.artifact_type == artifact_type]
        for artifact_type in ("story", "ideation", "refinement", "spec", "sprint", "card")
    }

    # Remove completed/failed entries so they can be re-queued.
    # NOTE: we purposely do NOT filter by source — terminal rows from
    # event-driven enqueues (event:card.created, retry_from_ui, …) must
    # also be cleared so the historical pass can reprocess every artifact.
    # The UNIQUE constraint (board_id, artifact_type, artifact_id) means
    # only one row per artifact can exist, so deleting all terminal rows
    # is equivalent to clearing the slot for re-queueing.
    await store.delete_terminal_queue(db, board_id=board_id)

    # Collect live entries only — pending, claimed, or paused. Terminal
    # rows (done/failed) have just been deleted above, so dedup against
    # them would be incorrect. Including `paused` covers the case where
    # a prior historical run was paused and is still reachable via
    # resume_historical.
    existing_rows = await store.list_live_queue(db, board_id=board_id)
    already_queued = {(row.artifact_type, row.artifact_id) for row in existing_rows}
    existing_historical = {
        (row.artifact_type, row.artifact_id)
        for row in existing_rows
        if row.source == "historical_backfill"
    }

    entries = [
        HistoricalQueueInsert(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact.artifact_id,
        )
        for artifact_type in ("story", "ideation", "refinement", "spec", "sprint", "card")
        for artifact in by_type[artifact_type]
        if (artifact_type, artifact.artifact_id) not in already_queued
    ]
    total = len(entries)
    await store.add_queue_entries(db, entries)

    run_total = total + len(existing_historical)
    _set_historical_progress_state(
        board,
        total=run_total,
        status="in_progress" if run_total > 0 else "inactive",
    )
    if board is not None:
        await store.save_board(db, board)
    await store.commit(db)

    logger.info(
        "governance.historical_start board=%s stories=%d ideations=%d "
        "refinements=%d specs=%d sprints=%d cards=%d total=%d",
        board_id, len(by_type["story"]), len(by_type["ideation"]),
        len(by_type["refinement"]), len(by_type["spec"]),
        len(by_type["sprint"]), len(by_type["card"]), total,
    )

    if total > 0:
        # Fase 4 — wake the background worker immediately so the freshly
        # enqueued rows start processing without waiting for a heartbeat.
        try:
            from okto_pulse.core.application.runtime_workers import (
                signal_runtime_worker,
            )
            signal_runtime_worker("consolidation_worker")
        except Exception:  # pragma: no cover — signal is best-effort
            pass

    return {"status": "queueing", "board_id": board_id, "total_artifacts": run_total}


async def pause_historical(db: Any, board_id: str) -> dict:
    """Mark low-priority backfill entries as paused."""
    store = get_kg_governance_store()
    await store.update_historical_status(
        db,
        board_id=board_id,
        old_status="pending",
        new_status="paused",
    )
    await store.commit(db)
    return {"status": "paused", "board_id": board_id}


async def resume_historical(db: Any, board_id: str) -> dict:
    """Resume paused backfill entries."""
    store = get_kg_governance_store()
    await store.update_historical_status(
        db,
        board_id=board_id,
        old_status="paused",
        new_status="pending",
    )
    await store.commit(db)
    return {"status": "resumed", "board_id": board_id}


async def cancel_historical(db: Any, board_id: str) -> dict:
    """Delete pending low-priority entries. Already-consolidated preserved."""
    store = get_kg_governance_store()
    board = await store.get_board(db, board_id=board_id)
    removed = await store.delete_historical_pending(db, board_id=board_id)
    current_total = int(_historical_progress_state(board).get("total") or 0)
    _set_historical_progress_state(board, total=current_total, status="cancelled")
    if board is not None:
        await store.save_board(db, board)
    await store.commit(db)
    return {"status": "cancelled", "board_id": board_id, "removed": removed}


async def retry_pending_entry(
    db: Any,
    board_id: str,
    queue_entry_id: str,
    *,
    recursive: bool = False,
) -> dict | None:
    """Re-queue a failed/done ConsolidationQueue entry so the worker reprocesses
    it (write, commits internally). ``recursive=True`` also re-enqueues
    descendants below the artifact in the Ideation→Refinement→Spec→Sprint→Card
    hierarchy.

    Returns ``None`` when the entry does not exist (so this module stays
    transport-free — the use case maps that to ``EntityNotFoundError`` → HTTP
    404). Reproduces the legacy ``retry_pending_entry`` endpoint byte-for-byte:
    the mutation, the recursive descendant sweep, the single ``commit`` and the
    best-effort worker signal all live here, exactly as the endpoint relied on
    when it passed ``db`` straight through.

    Idempotency: content_hash BR still owns "nothing actually changed" no-op
    behaviour downstream, so retrying an unchanged artifact is a cheap round-trip
    that touches the outbox once.
    """
    from okto_pulse.core.ports.kg_operational import get_kg_worker_queue_port

    result = await get_kg_worker_queue_port().retry_pending_entry(
        db,
        board_id=board_id,
        queue_entry_id=queue_entry_id,
        recursive=recursive,
    )
    if result is None:
        return None

    # Fase 4 — wake the background worker so retried rows are picked up
    # immediately instead of waiting for the heartbeat tick.
    try:
        from okto_pulse.core.application.runtime_workers import signal_runtime_worker
        signal_runtime_worker("consolidation_worker")
    except Exception:  # pragma: no cover — signal is best-effort
        pass

    return dict(result)


async def get_historical_progress(db: Any, board_id: str) -> dict:
    """Return progress of historical consolidation."""
    board = await get_kg_governance_store().get_board(db, board_id=board_id)
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
    db: Any,
    board_id: str,
    session_id: str,
    *,
    force: bool = False,
) -> dict:
    """Soft-delete nodes/edges from a consolidation session.

    Returns 409 cascade_blocked if other sessions reference nodes from this
    session, unless force=True (admin).
    """
    store = get_kg_governance_store()
    fact = await store.get_undo_fact(
        db,
        board_id=board_id,
        session_id=session_id,
    )
    if fact is None:
        return {"error": "not_found", "session_id": session_id}
    if fact.undo_status == "undone":
        return {"error": "already_undone", "session_id": session_id}
    if not force and fact.blocking_sessions:
        return {
            "error": "cascade_blocked",
            "session_id": session_id,
            "blocking_sessions": list(fact.blocking_sessions),
        }

    # Mark as undone
    await store.mark_session_undone(
        db,
        session_id=session_id,
        undone_at=datetime.now(timezone.utc),
    )
    await store.commit(db)

    # Kuzu soft-delete would happen here via TransactionOrchestrator.compensate
    # pattern. For MVP: mark in SQLite only.
    return {
        "session_id": session_id,
        "status": "undone",
        "nodes_removed": len(fact.node_ids),
        "force_used": force,
    }


# ---------------------------------------------------------------------------
# Audit retention + purge (FR-15, FR-16)
# ---------------------------------------------------------------------------


async def purge_expired_audit(
    db: Any,
    board_id: str,
    retention_days: int | None = None,
) -> dict:
    """Delete audit entries older than retention_days. None = skip (unlimited)."""
    if retention_days is None or retention_days <= 0:
        return {"board_id": board_id, "purged": 0, "retention": "unlimited"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    store = get_kg_governance_store()
    purged = await store.purge_expired_audit(
        db,
        board_id=board_id,
        cutoff=cutoff,
    )
    await store.commit(db)
    return {
        "board_id": board_id,
        "purged": purged,
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
    db: Any,
    board_id: str,
) -> dict:
    """Wipe all KG data for a board via logical runtime and audit purges.

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

    # 2. Per-board graph purge through the logical runtime capability.
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        purge = get_kg_registry().graph_runtime_store.purge_board_graph(
            board_id,
            reason="right_to_erasure",
        )
        counts["graph_purge"] = asdict(purge)
    except Exception as exc:
        counts["graph_purge_error"] = str(exc)

    # 3. SQLite audit/refs/outbox purge
    try:
        store = get_kg_governance_store()
        await store.purge_board_metadata(db, board_id=board_id)
        await store.commit(db)
        counts["sqlite_purged"] = True
    except Exception as exc:
        counts["sqlite_purge_error"] = str(exc)

    logger.info(
        "governance.erasure board=%s", board_id,
        extra={"event": "governance.erasure", **counts},
    )
    return counts


# ===========================================================================
# Node relevance boost (spec R01A REST-FU5-S4 — kg_routes.boost_node)
# ===========================================================================


class BoostPersistError(Exception):
    """Raised when the graph SET that persists a node boost fails.

    The legacy ``api/kg_routes.boost_node`` endpoint caught the SET exception
    inline and returned a 500 ``kuzu_error`` RFC 7807 problem; the REST adapter
    catches this and reproduces that exact problem body."""


BOOST_DELTA = 0.3
BOOST_CLAMP_MIN = 0.0
BOOST_CLAMP_MAX = 1.5


async def boost_node(
    db: Any, board_id: str, node_id: str, *, actor_id: str
) -> dict[str, Any] | None:
    """Boost a node's ``relevance_score`` by +0.3 (clamp [0, 1.5]) and STAGE the
    ``ConsolidationAudit`` row on ``db`` (write; the caller owns the commit via the
    UnitOfWork).

    The graph read/SET runs through the #06 ``GraphTransaction`` port (the embedded
    store auto-commits each statement), and idempotency is NOT enforced — each call
    stacks another +0.3 until the clamp is reached. The boost response body and the
    +0.3/clamp arithmetic reproduce the legacy ``api/kg_routes.boost_node``
    byte-for-byte.

    Returns the response dict on success. Returns ``None`` when the node is
    absent in every node type of the board graph (this module stays
    transport-free — the use case maps that to ``EntityNotFoundError`` → the
    adapter's 404 problem). A failure to persist the SET raises
    :class:`BoostPersistError` (adapter → 500 ``kuzu_error``).

    The staged audit row carries ALL required NOT-NULL columns (``artifact_type``,
    ``started_at``, …) so it persists on a successful boost — bug 547a2aa8 fix; the
    legacy row omitted those columns, so its commit always raised IntegrityError and
    was silently swallowed (200 with no audit row). The caller (``BoostNodeUseCase``)
    commits this row best-effort: the commit guard now only covers a genuinely
    unexpected failure on the already-mutated graph (split-brain), not a deterministic
    schema violation."""
    import uuid

    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    score_before: float | None = None
    node_type: str | None = None
    # Stamp the audit start before the graph read/SET so the persisted
    # ConsolidationAudit row carries a truthful ``started_at`` (bug 547a2aa8 fix).
    started_at = datetime.now(timezone.utc)
    # Read+write through the #06 GraphTransaction port — behaviour-identical to
    # the legacy direct (db, conn) tuple (embedded auto-commits per statement on
    # the SET); the relational ``db`` only carries the audit row.
    async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
        for ntype in NODE_TYPES:
            try:
                res = scope.execute(
                    f"MATCH (n:{ntype} {{id: $nid}}) RETURN n.relevance_score",
                    {"nid": node_id},
                )
            except Exception:
                continue
            if res.has_next():
                row = res.get_next()
                score_before = float(row[0]) if row[0] is not None else 0.5
                node_type = ntype
                break

        if node_type is None or score_before is None:
            return None

        score_after = max(
            BOOST_CLAMP_MIN, min(BOOST_CLAMP_MAX, score_before + BOOST_DELTA)
        )
        try:
            scope.execute(
                f"MATCH (n:{node_type} {{id: $nid}}) "
                f"SET n.relevance_score = $score",
                {"nid": node_id, "score": score_after},
            )
        except Exception as exc:
            raise BoostPersistError(f"Failed to persist boost: {exc}") from exc

    boosted_at = datetime.now(timezone.utc)
    boosted_by = actor_id

    # Persist the boost audit row with ALL required NOT-NULL columns populated
    # (bug 547a2aa8 fix): the legacy staging omitted ``artifact_type`` and
    # ``started_at``, so its commit always raised IntegrityError and was swallowed,
    # dropping the row while the boost still returned 200. ``artifact_type="boost"``
    # is intentionally OUTSIDE ``CONSOLIDABLE_ARTIFACT_TYPES`` — a boost bumps an
    # existing node's relevance_score, it is not an artifact consolidation, so the
    # rebuild/health counters must not treat it as one (and nodes_added/edges_added=0
    # keep it out of every count). The caller (BoostNodeUseCase) commits this row;
    # that commit stays best-effort only for the already-mutated-graph split-brain
    # case, NOT to mask a deterministic schema violation.
    get_kg_governance_store().add_boost_audit(
        db,
        BoostAuditRecord(
            session_id=(
                f"boost-{node_id[:8]}-{int(boosted_at.timestamp())}"
                f"-{uuid.uuid4().hex[:8]}"
            ),
            board_id=board_id,
            artifact_id=node_id,
            agent_id=boosted_by,
            started_at=started_at,
            committed_at=boosted_at,
        ),
    )

    return {
        "node_id": node_id,
        "node_type": node_type,
        "score_before": round(score_before, 4),
        "score_after": round(score_after, 4),
        "boosted_at": boosted_at.isoformat(),
        "boosted_by": boosted_by,
    }
