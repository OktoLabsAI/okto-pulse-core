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

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.domain.enums import SpecStatus, SprintStatus
from okto_pulse.core.models.db import (
    Board,
    ConsolidationAudit,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    Ideation,
    KuzuNodeRef,
    Refinement,
    Spec,
    Sprint,
    Story,
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

    story_result = await db.execute(
        select(Story).where(
            Story.board_id == board_id,
            Story.archived.is_(False),
        )
    )
    stories = list(story_result.scalars().all())

    ideation_result = await db.execute(
        select(Ideation).where(
            Ideation.board_id == board_id,
            Ideation.archived.is_(False),
        )
    )
    ideations = list(ideation_result.scalars().all())

    refinement_result = await db.execute(
        select(Refinement).where(
            Refinement.board_id == board_id,
            Refinement.archived.is_(False),
        )
    )
    refinements = list(refinement_result.scalars().all())

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

    # Insert pre-spec entries first so lineage targets exist before specs.
    for story in stories:
        if ("story", story.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="story",
            artifact_id=story.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

    for ideation in ideations:
        if ("ideation", ideation.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="ideation",
            artifact_id=ideation.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

    for refinement in refinements:
        if ("refinement", refinement.id) in already_queued:
            continue
        db.add(ConsolidationQueue(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type="refinement",
            artifact_id=refinement.id,
            priority="low",
            source="historical_backfill",
            status="pending",
        ))
        total += 1

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
        "governance.historical_start board=%s stories=%d ideations=%d "
        "refinements=%d specs=%d sprints=%d cards=%d total=%d",
        board_id, len(stories), len(ideations), len(refinements),
        len(specs), len(sprints), len(cards), total,
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


async def retry_pending_entry(
    db: AsyncSession,
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
    from sqlalchemy import and_
    from sqlalchemy import select as _select

    from okto_pulse.core.models.db import (
        Card,
        ConsolidationQueue,
        Refinement,
        Spec,
        Sprint,
    )

    entry = (await db.execute(
        _select(ConsolidationQueue).where(and_(
            ConsolidationQueue.id == queue_entry_id,
            ConsolidationQueue.board_id == board_id,
        ))
    )).scalars().first()
    if entry is None:
        return None

    entry.status = "pending"
    entry.claimed_at = None
    entry.claimed_by_session_id = None
    entry.source = "retry_from_ui"
    reopened = [queue_entry_id]

    if recursive:
        descendants: list[tuple[str, str]] = []
        if entry.artifact_type == "ideation":
            rows = (await db.execute(
                _select(Refinement.id).where(Refinement.ideation_id == entry.artifact_id)
            )).scalars().all()
            descendants.extend(("refinement", r) for r in rows)
        if entry.artifact_type in ("ideation", "refinement"):
            refinement_ids: list[str]
            if entry.artifact_type == "ideation":
                refinement_ids = [r for _, r in descendants]
            else:
                refinement_ids = [entry.artifact_id]
            specs = (await db.execute(
                _select(Spec.id).where(Spec.refinement_id.in_(refinement_ids))
            )).scalars().all()
            descendants.extend(("spec", s) for s in specs)
        if entry.artifact_type in ("ideation", "refinement", "spec"):
            spec_ids = [s for t, s in descendants if t == "spec"] or [entry.artifact_id]
            sprints = (await db.execute(
                _select(Sprint.id).where(Sprint.spec_id.in_(spec_ids))
            )).scalars().all()
            descendants.extend(("sprint", sp) for sp in sprints)
            cards = (await db.execute(
                _select(Card.id).where(Card.spec_id.in_(spec_ids))
            )).scalars().all()
            descendants.extend(("card", c) for c in cards)

        for artifact_type, artifact_id in descendants:
            row = (await db.execute(
                _select(ConsolidationQueue).where(and_(
                    ConsolidationQueue.board_id == board_id,
                    ConsolidationQueue.artifact_type == artifact_type,
                    ConsolidationQueue.artifact_id == artifact_id,
                ))
            )).scalars().first()
            if row is None:
                continue
            row.status = "pending"
            row.claimed_at = None
            row.claimed_by_session_id = None
            row.source = "retry_from_ui_recursive"
            reopened.append(row.id)

    await db.commit()

    # Fase 4 — wake the background worker so retried rows are picked up
    # immediately instead of waiting for the heartbeat tick.
    try:
        from okto_pulse.core.kg.workers.consolidation import (
            signal_consolidation_worker,
        )
        signal_consolidation_worker()
    except Exception:  # pragma: no cover — signal is best-effort
        pass

    return {
        "board_id": board_id,
        "queue_entry_id": queue_entry_id,
        "recursive": recursive,
        "reopened_count": len(reopened),
        "reopened_ids": reopened,
    }


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


async def right_to_erasure(
    db: AsyncSession,
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
    db: AsyncSession, board_id: str, node_id: str, *, actor_id: str
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
    db.add(ConsolidationAudit(
        # session_id is the audit PK. Now that the row PERSISTS (bug 547a2aa8 fix),
        # the legacy ``boost-{node[:8]}-{epoch_s}`` template would collide on a second
        # boost of the same node within the same second and the duplicate would be
        # silently swallowed by the best-effort commit guard — re-dropping the audit
        # row. A uuid suffix makes every boost's audit row unique (stays ≤ String(36):
        # 6 + 8 + 1 + 10 + 1 + 8 = 34). The ``boost-`` prefix is preserved.
        session_id=(
            f"boost-{node_id[:8]}-{int(boosted_at.timestamp())}"
            f"-{uuid.uuid4().hex[:8]}"
        ),
        board_id=board_id,
        artifact_id=node_id,
        artifact_type="boost",
        agent_id=boosted_by,
        started_at=started_at,
        committed_at=boosted_at,
        nodes_added=0,
        edges_added=0,
    ))

    return {
        "node_id": node_id,
        "node_type": node_type,
        "score_before": round(score_before, 4),
        "score_after": round(score_after, 4),
        "boosted_at": boosted_at.isoformat(),
        "boosted_by": boosted_by,
    }
