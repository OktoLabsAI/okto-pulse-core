"""Relational readers for the KG dashboard "light" REST endpoints (spec R01A
REST-FU5-S2) and the pending-queue readers (spec R01A REST-FU5-S3).

The reader endpoints in ``api/kg_routes.py`` — ``list_audit`` / ``global_search``
(S2) and ``list_pending`` / ``list_pending_tree`` (S3) — used to bind a raw
request session and run their ``select`` inline in the handler. The strangler
moves the endpoint onto a transport-free use case + ``PulseUnitOfWork``; the
inline SQL (the only thing that may legitimately touch the relational layer)
lands here, in a service-reader module, so the use case stays free of ``select``
/ ``AsyncSession`` / ORM imports (the relational ratchet gate over
``application/use_cases`` would fail otherwise).

Each function takes the request ``AsyncSession`` and reproduces the legacy query +
projection byte-for-byte. They are reads only — no commit.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    Board,
    Card,
    ConsolidationAudit,
    ConsolidationQueue,
    Ideation,
    Refinement,
    Spec,
    Sprint,
)


async def list_consolidation_audit(
    db: AsyncSession, board_id: str, *, limit: int
) -> list[dict[str, Any]]:
    """Return committed consolidation-audit entries for a board, newest first.

    Reproduces the legacy ``list_audit`` query (committed rows only, ordered by
    ``committed_at`` desc, capped at ``limit``) and its per-row projection exactly.
    """
    query = (
        select(ConsolidationAudit)
        .where(
            ConsolidationAudit.board_id == board_id,
            ConsolidationAudit.committed_at.is_not(None),
        )
        .order_by(ConsolidationAudit.committed_at.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    rows = result.scalars().all()

    return [
        {
            "session_id": r.session_id,
            "board_id": r.board_id,
            "artifact_id": r.artifact_id,
            "artifact_type": getattr(r, "artifact_type", ""),
            "agent_id": r.agent_id,
            "committed_at": r.committed_at.isoformat() if r.committed_at else None,
            "nodes_added": r.nodes_added or 0,
            "nodes_updated": r.nodes_updated or 0,
            "nodes_superseded": r.nodes_superseded or 0,
            "edges_added": r.edges_added or 0,
            "summary_text": r.summary_text,
            "undo_status": r.undo_status or "none",
        }
        for r in rows
    ]


async def list_all_board_ids(db: AsyncSession, *, limit: int = 100) -> list[str]:
    """Return the ids of all boards (community edition has no per-user filter).

    Reproduces the legacy ``global_search`` board lookup: ``select(Board)`` capped
    at 100, projected to ids. For the SaaS edition this is where the
    accessible-board filter would be applied.
    """
    result = await db.execute(select(Board).limit(limit))
    boards = result.scalars().all()
    return [b.id for b in boards]


# ---------------------------------------------------------------------------
# Pending queue readers (spec R01A REST-FU5-S3 — list_pending / list_pending_tree)
# ---------------------------------------------------------------------------


async def list_pending_entries(
    db: AsyncSession, board_id: str
) -> list[dict[str, Any]]:
    """Return the board's pending consolidation-queue entries, newest first.

    Reproduces the legacy ``list_pending`` query (board-scoped, ordered by
    ``triggered_at`` desc, capped at 100) and its per-row projection exactly.
    Reads only — no commit. The legacy endpoint swallowed any error and returned
    an empty envelope; that fallback is preserved by the adapter (not here), so
    this reader surfaces failures to the caller's ``except`` wrapper.
    """
    query = (
        select(ConsolidationQueue)
        .where(ConsolidationQueue.board_id == board_id)
        .order_by(ConsolidationQueue.triggered_at.desc())
        .limit(100)
    )
    result = await db.execute(query)
    rows = result.scalars().all()

    return [
        {
            "id": r.id,
            "board_id": r.board_id,
            "artifact_id": r.artifact_id,
            "artifact_type": r.artifact_type,
            "priority": r.priority,
            "source": r.source,
            "status": r.status,
            "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
            "claimed_by_session_id": r.claimed_by_session_id,
        }
        for r in rows
    ]


async def build_pending_tree(
    db: AsyncSession, board_id: str, *, depth: int = 5
) -> dict[str, Any]:
    """Build the hierarchical pending-queue view (spec f33eb9ca — Layer 4 UI).

    Reproduces the legacy ``list_pending_tree`` handler byte-for-byte: fetches the
    ``consolidation_queue`` state once and joins it in-Python against the
    Ideation→Refinement→Spec→Sprint→Card hierarchy, annotating each level with
    aggregate status counters. Reads only — no commit. The returned payload shape
    is the stable contract consumed by ``PendingQueueTree.tsx``.
    """
    # Fetch queue state once, then join in-Python against the hierarchy.
    # A recursive CTE across SQLite + JSON test_scenarios would be
    # brittle; per-table fetches keep the code portable and cover the
    # unique-constraint-indexed case (per-board lookups are cheap).
    q_rows = (await db.execute(
        select(ConsolidationQueue).where(ConsolidationQueue.board_id == board_id)
    )).scalars().all()
    q_by_artifact: dict[tuple[str, str], ConsolidationQueue] = {
        (r.artifact_type, r.artifact_id): r for r in q_rows
    }

    def _queue_meta(art_type: str, art_id: str) -> dict:
        entry = q_by_artifact.get((art_type, art_id))
        if entry is None:
            return {"status": "not_queued", "queued_age_seconds": None,
                    "retry_count": 0, "layer": None, "last_error": None}
        age = None
        if entry.triggered_at is not None:
            # SQLite may return naive datetimes; normalise both sides to UTC.
            trig = entry.triggered_at
            if trig.tzinfo is None:
                trig = trig.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - trig).total_seconds()
        return {
            "status": entry.status,
            "queued_age_seconds": int(age) if age is not None else None,
            "retry_count": 0,  # placeholder until retry counter lands
            "layer": entry.source or "unknown",
            "last_error": None,
        }

    ideas = (await db.execute(
        select(Ideation).where(Ideation.board_id == board_id)
    )).scalars().all()
    refs = (await db.execute(
        select(Refinement).where(Refinement.board_id == board_id)
    )).scalars().all()
    specs = (await db.execute(
        select(Spec).where(Spec.board_id == board_id)
    )).scalars().all()
    sprints = (await db.execute(
        select(Sprint).where(Sprint.board_id == board_id)
    )).scalars().all()
    cards = (await db.execute(
        select(Card).where(Card.board_id == board_id)
    )).scalars().all()

    refs_by_ideation: dict[str, list] = defaultdict(list)
    for r in refs:
        refs_by_ideation[r.ideation_id or ""].append(r)
    specs_by_refinement: dict[str, list] = defaultdict(list)
    specs_orphan: list = []
    for s in specs:
        if s.refinement_id:
            specs_by_refinement[s.refinement_id].append(s)
        else:
            specs_orphan.append(s)
    sprints_by_spec: dict[str, list] = defaultdict(list)
    for sp in sprints:
        sprints_by_spec[sp.spec_id].append(sp)
    cards_by_sprint: dict[str, list] = defaultdict(list)
    cards_by_spec_direct: dict[str, list] = defaultdict(list)
    for c in cards:
        if getattr(c, "sprint_id", None):
            cards_by_sprint[c.sprint_id].append(c)
        else:
            cards_by_spec_direct[c.spec_id].append(c)

    levels_counter = {
        lvl: {"pending": 0, "in_progress": 0, "done": 0, "failed": 0,
              "not_queued": 0}
        for lvl in ("ideations", "refinements", "specs", "sprints", "cards")
    }

    def _tally(level: str, art_type: str, art_id: str) -> str:
        status = _queue_meta(art_type, art_id)["status"]
        levels_counter[level][status] = levels_counter[level].get(status, 0) + 1
        return status

    def _card_node(c) -> dict:
        meta = _queue_meta("card", c.id)
        _tally("cards", "card", c.id)
        return {
            "id": c.id, "type": "card",
            "title": c.title,
            "card_type": str(c.card_type) if getattr(c, "card_type", None) else "normal",
            **meta,
            "children": [],
        }

    def _sprint_node(sp) -> dict:
        meta = _queue_meta("sprint", sp.id)
        _tally("sprints", "sprint", sp.id)
        children = [_card_node(c) for c in cards_by_sprint.get(sp.id, [])]
        if depth < 5:
            children = []
        return {
            "id": sp.id, "type": "sprint", "title": sp.title,
            **meta, "children": children,
        }

    def _spec_node(s) -> dict:
        meta = _queue_meta("spec", s.id)
        _tally("specs", "spec", s.id)
        sp_children = [_sprint_node(sp) for sp in sprints_by_spec.get(s.id, [])]
        direct_cards = [_card_node(c) for c in cards_by_spec_direct.get(s.id, [])]
        if depth < 4:
            sp_children = []
            direct_cards = []
        return {
            "id": s.id, "type": "spec", "title": s.title,
            **meta,
            "children": sp_children + direct_cards,
        }

    def _refinement_node(r) -> dict:
        meta = _queue_meta("refinement", r.id)
        _tally("refinements", "refinement", r.id)
        spec_children = [_spec_node(s) for s in specs_by_refinement.get(r.id, [])]
        if depth < 3:
            spec_children = []
        return {
            "id": r.id, "type": "refinement", "title": r.title,
            **meta, "children": spec_children,
        }

    tree: list[dict] = []
    for idea in ideas:
        meta = _queue_meta("ideation", idea.id)
        _tally("ideations", "ideation", idea.id)
        ref_children = [_refinement_node(r) for r in refs_by_ideation.get(idea.id, [])]
        if depth < 2:
            ref_children = []
        tree.append({
            "id": idea.id, "type": "ideation", "title": idea.title,
            **meta, "children": ref_children,
        })

    # Orphan specs (no refinement) go to the root alongside ideations.
    for s in specs_orphan:
        tree.append(_spec_node(s))

    total_pending = sum(
        sum(
            v for k, v in counts.items()
            if k in ("pending", "in_progress")
        )
        for counts in levels_counter.values()
    )

    return {
        "board_id": board_id,
        "depth": depth,
        "total_pending": total_pending,
        "levels": levels_counter,
        "tree": tree,
    }
