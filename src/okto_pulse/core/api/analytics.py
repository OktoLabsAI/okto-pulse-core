"""Analytics API endpoints — cross-board and per-board KPIs, funnels, quality, velocity, coverage, agents."""

import csv
import io
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from starlette.responses import StreamingResponse
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    Ideation,
    IdeationQAItem,
    IdeationStatus,
    Refinement,
    Spec,
    SpecStatus,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str | None, end_of_day: bool = False) -> datetime | None:
    """Parse an ISO date string, returning None if absent or invalid.
    When end_of_day=True, sets time to 23:59:59.999999 so the entire day is included.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        # If only a date was provided (no time component) and end_of_day requested,
        # set to end of day so filters include cards created during that day
        if end_of_day and "T" not in value:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        return dt
    except (ValueError, TypeError):
        return None


def _extract_conclusion(card) -> dict | None:
    """Return the last conclusion entry from a card's conclusions JSON list."""
    conclusions = card.conclusions
    if not conclusions or not isinstance(conclusions, list):
        return None
    last = conclusions[-1]
    if not isinstance(last, dict):
        return None
    return last


def _is_test_card(card) -> bool:
    """True if the card is a test card (has non-empty test_scenario_ids)."""
    ids = card.test_scenario_ids
    return bool(ids and isinstance(ids, list) and len(ids) > 0)


def _resolve_linked_fr_indices(linked_refs: list, frs: list[str]) -> set[int]:
    """Resolve linked_requirements (which can be indices or FR text) to FR indices."""
    indices: set[int] = set()
    for ref in linked_refs:
        ref_str = str(ref)
        try:
            idx = int(ref_str)
            if 0 <= idx < len(frs):
                indices.add(idx)
                continue
        except (ValueError, TypeError):
            pass
        # Try matching by text content
        for i, fr_text in enumerate(frs):
            if ref_str in fr_text or fr_text in ref_str:
                indices.add(i)
                break
    return indices


# ---------------------------------------------------------------------------
# 1) GET /analytics/overview — Cross-board KPIs
# ---------------------------------------------------------------------------


@router.get("/analytics/overview")
async def analytics_overview(
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Cross-board KPIs: totals, averages, funnel, velocity, board list."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    # Fetch boards owned by user
    boards_q = select(Board).where(Board.owner_id == user_id)
    boards_result = await db.execute(boards_q)
    boards = list(boards_result.scalars().all())
    board_ids = [b.id for b in boards]

    if not board_ids:
        return {
            "total_ideations": 0,
            "total_specs": 0,
            "total_cards_impl": 0,
            "total_cards_test": 0,
            "avg_completeness": None,
            "avg_drift": None,
            "total_business_rules": 0,
            "total_api_contracts": 0,
            "specs_with_rules": 0,
            "specs_with_contracts": 0,
            "funnel": {"ideations": 0, "refinements": 0, "specs": 0, "cards": 0, "done": 0},
            "velocity": [],
            "boards": [],
        }

    # --- Ideations ---
    ideation_q = select(Ideation).where(Ideation.board_id.in_(board_ids))
    if dt_from:
        ideation_q = ideation_q.where(Ideation.created_at >= dt_from)
    if dt_to:
        ideation_q = ideation_q.where(Ideation.created_at <= dt_to)
    ideations = list((await db.execute(ideation_q)).scalars().all())

    # --- Refinements ---
    refinement_q = select(Refinement).where(Refinement.board_id.in_(board_ids))
    if dt_from:
        refinement_q = refinement_q.where(Refinement.created_at >= dt_from)
    if dt_to:
        refinement_q = refinement_q.where(Refinement.created_at <= dt_to)
    refinements = list((await db.execute(refinement_q)).scalars().all())

    # --- Specs ---
    spec_q = select(Spec).where(Spec.board_id.in_(board_ids))
    if dt_from:
        spec_q = spec_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_q = spec_q.where(Spec.created_at <= dt_to)
    specs = list((await db.execute(spec_q)).scalars().all())

    # --- Cards ---
    card_q = select(Card).where(Card.board_id.in_(board_ids))
    if dt_from:
        card_q = card_q.where(Card.created_at >= dt_from)
    if dt_to:
        card_q = card_q.where(Card.created_at <= dt_to)
    cards = list((await db.execute(card_q)).scalars().all())

    impl_cards = [c for c in cards if not _is_test_card(c)]
    test_cards = [c for c in cards if _is_test_card(c)]

    # Avg completeness / drift from last conclusion of done cards
    # Legacy conclusions (before completeness/drift fields) default to 100/0
    completeness_vals: list[float] = []
    drift_vals: list[float] = []
    for c in cards:
        concl = _extract_conclusion(c)
        if concl:
            completeness_vals.append(concl.get("completeness", 100))
            drift_vals.append(concl.get("drift", 0))

    avg_completeness = round(sum(completeness_vals) / len(completeness_vals), 1) if completeness_vals else None
    avg_drift = round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None

    # Funnel
    done_cards = [c for c in cards if c.status == CardStatus.DONE]
    funnel = {
        "ideations": len(ideations),
        "refinements": len(refinements),
        "specs": len(specs),
        "cards": len(cards),
        "done": len(done_cards),
    }

    # Velocity: cards done per week, last 12 weeks
    velocity = _compute_velocity(done_cards, 12)

    # Per-board stats
    board_stats = []
    for b in boards:
        b_cards = [c for c in cards if c.board_id == b.id]
        b_done = [c for c in b_cards if c.status == CardStatus.DONE]
        b_bugs = [c for c in b_cards if getattr(c, "card_type", "normal") == "bug"]
        board_stats.append({
            "board_id": b.id,
            "board_name": b.name,
            "ideations": sum(1 for i in ideations if i.board_id == b.id),
            "refinements": sum(1 for r in refinements if r.board_id == b.id),
            "specs": sum(1 for s in specs if s.board_id == b.id),
            "cards": len(b_cards),
            "cards_done": len(b_done),
            "bugs": len(b_bugs),
        })

    # Status breakdowns
    ideations_done = sum(1 for i in ideations if i.status == IdeationStatus.DONE)
    specs_done = sum(1 for s in specs if s.status == SpecStatus.DONE)
    specs_with_tests = sum(1 for s in specs if s.test_scenarios and len(s.test_scenarios) > 0)

    # Business Rules & API Contracts aggregation
    total_brs = sum(len(s.business_rules or []) for s in specs)
    total_contracts = sum(len(s.api_contracts or []) for s in specs)
    specs_with_rules = sum(1 for s in specs if s.business_rules and len(s.business_rules) > 0)
    specs_with_contracts = sum(1 for s in specs if s.api_contracts and len(s.api_contracts) > 0)

    # --- Bug metrics ---
    bug_cards = [c for c in cards if getattr(c, "card_type", "normal") == "bug"]
    total_bugs = len(bug_cards)
    bugs_open = sum(1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED))
    bugs_done = sum(1 for c in bug_cards if c.status == CardStatus.DONE)
    bugs_by_severity = {
        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
    }

    # Bugs per spec
    bugs_per_spec: dict[str, int] = {}
    for c in bug_cards:
        sid = c.spec_id or "unlinked"
        bugs_per_spec[sid] = bugs_per_spec.get(sid, 0) + 1

    # Bug rate per spec (bugs / total tasks in that spec)
    bug_rate_per_spec = []
    for s in specs:
        s_cards = [c for c in cards if c.spec_id == s.id]
        s_bugs = [c for c in s_cards if getattr(c, "card_type", "normal") == "bug"]
        if s_cards:
            bug_rate_per_spec.append({
                "spec_id": s.id,
                "spec_title": s.title,
                "total_tasks": len(s_cards),
                "bugs": len(s_bugs),
                "rate": round(len(s_bugs) / len(s_cards) * 100, 1),
            })

    # Avg triage time (bug created -> first test task linked)
    triage_times: list[float] = []
    for c in bug_cards:
        linked = getattr(c, "linked_test_task_ids", None) or []
        if linked and c.created_at:
            # Find earliest linked test task creation
            earliest = None
            for tid in linked:
                tt = next((tc for tc in cards if tc.id == tid), None)
                if tt and tt.created_at:
                    if earliest is None or tt.created_at < earliest:
                        earliest = tt.created_at
            if earliest:
                delta_hours = (earliest - c.created_at).total_seconds() / 3600
                triage_times.append(delta_hours)

    avg_triage_hours = round(sum(triage_times) / len(triage_times), 1) if triage_times else None

    return {
        "total_ideations": len(ideations),
        "ideations_done": ideations_done,
        "total_specs": len(specs),
        "specs_done": specs_done,
        "specs_with_tests": specs_with_tests,
        "total_business_rules": total_brs,
        "total_api_contracts": total_contracts,
        "specs_with_rules": specs_with_rules,
        "specs_with_contracts": specs_with_contracts,
        "total_cards_impl": len(impl_cards),
        "total_cards_test": len(test_cards),
        "avg_completeness": avg_completeness,
        "avg_drift": avg_drift,
        "funnel": funnel,
        "velocity": velocity,
        "boards": board_stats,
        # Bug metrics
        "total_bugs": total_bugs,
        "bugs_open": bugs_open,
        "bugs_done": bugs_done,
        "bugs_by_severity": bugs_by_severity,
        "bug_rate_per_spec": bug_rate_per_spec,
        "avg_triage_hours": avg_triage_hours,
    }


# ---------------------------------------------------------------------------
# 2) GET /boards/{board_id}/analytics/funnel — Board funnel
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/funnel")
async def board_funnel(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Funnel for a single board: ideations -> refinements -> specs -> cards -> done."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    counts: dict[str, int] = {}
    for model, key in [
        (Ideation, "ideations"),
        (Refinement, "refinements"),
        (Spec, "specs"),
        (Card, "cards"),
    ]:
        q = select(func.count(model.id)).where(model.board_id == board_id)
        if dt_from:
            q = q.where(model.created_at >= dt_from)
        if dt_to:
            q = q.where(model.created_at <= dt_to)
        counts[key] = (await db.execute(q)).scalar() or 0

    # Done cards
    done_q = (
        select(func.count(Card.id))
        .where(Card.board_id == board_id, Card.status == CardStatus.DONE)
    )
    if dt_from:
        done_q = done_q.where(Card.created_at >= dt_from)
    if dt_to:
        done_q = done_q.where(Card.created_at <= dt_to)
    counts["done"] = (await db.execute(done_q)).scalar() or 0

    # Status breakdowns for KPIs
    ideations_done_q = select(func.count(Ideation.id)).where(
        Ideation.board_id == board_id, Ideation.status == IdeationStatus.DONE
    )
    specs_done_q = select(func.count(Spec.id)).where(
        Spec.board_id == board_id, Spec.status == SpecStatus.DONE
    )
    if dt_from:
        ideations_done_q = ideations_done_q.where(Ideation.created_at >= dt_from)
        specs_done_q = specs_done_q.where(Spec.created_at >= dt_from)
    if dt_to:
        ideations_done_q = ideations_done_q.where(Ideation.created_at <= dt_to)
        specs_done_q = specs_done_q.where(Spec.created_at <= dt_to)

    counts["ideations_done"] = (await db.execute(ideations_done_q)).scalar() or 0
    counts["specs_done"] = (await db.execute(specs_done_q)).scalar() or 0

    # Impl vs test cards — use Python-side filtering (JSON column can't be compared with SQL strings)
    all_cards_q = select(Card).where(Card.board_id == board_id)
    if dt_from:
        all_cards_q = all_cards_q.where(Card.created_at >= dt_from)
    if dt_to:
        all_cards_q = all_cards_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(all_cards_q)).scalars().all())
    counts["cards_impl"] = sum(1 for c in all_cards if not _is_test_card(c))
    counts["cards_test"] = sum(1 for c in all_cards if _is_test_card(c))

    # Business Rules & API Contracts for the board
    spec_objs_q = select(Spec).where(Spec.board_id == board_id)
    if dt_from:
        spec_objs_q = spec_objs_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_objs_q = spec_objs_q.where(Spec.created_at <= dt_to)
    spec_objs = list((await db.execute(spec_objs_q)).scalars().all())

    counts["rules_count"] = sum(len(s.business_rules or []) for s in spec_objs)
    counts["contracts_count"] = sum(len(s.api_contracts or []) for s in spec_objs)
    counts["specs_with_rules"] = sum(1 for s in spec_objs if s.business_rules and len(s.business_rules) > 0)
    counts["specs_with_contracts"] = sum(1 for s in spec_objs if s.api_contracts and len(s.api_contracts) > 0)

    # Bug metrics for board
    bug_cards = [c for c in all_cards if getattr(c, "card_type", "normal") == "bug"]
    counts["bugs_total"] = len(bug_cards)
    counts["bugs_open"] = sum(1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED))
    counts["bugs_by_severity"] = {
        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
    }

    return counts


# ---------------------------------------------------------------------------
# 3) GET /boards/{board_id}/analytics/quality — Scatter data
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/quality")
async def board_quality(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Scatter: for each done card with conclusions, return completeness + drift."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    q = select(Card).where(Card.board_id == board_id, Card.status == CardStatus.DONE)
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)

    cards = list((await db.execute(q)).scalars().all())
    result = []
    for c in cards:
        concl = _extract_conclusion(c)
        if concl:
            result.append({
                "card_id": c.id,
                "title": c.title,
                "completeness": concl.get("completeness", 100),
                "drift": concl.get("drift", 0),
            })
    return result


# ---------------------------------------------------------------------------
# 4) GET /boards/{board_id}/analytics/velocity — Weekly velocity
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/velocity")
async def board_velocity(
    board_id: str,
    weeks: int = Query(12, ge=1, le=52),
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Cards done per week, stacked by impl/test."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    q = select(Card).where(Card.board_id == board_id, Card.status == CardStatus.DONE)
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)

    cards = list((await db.execute(q)).scalars().all())
    return _compute_velocity(cards, weeks)


# ---------------------------------------------------------------------------
# 5) GET /boards/{board_id}/analytics/coverage — Test coverage per spec
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/coverage")
async def board_coverage(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Test coverage per spec: AC count, covered ACs, test scenario status counts."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    spec_q = select(Spec).where(Spec.board_id == board_id)
    if dt_from:
        spec_q = spec_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_q = spec_q.where(Spec.created_at <= dt_to)
    specs = list((await db.execute(spec_q)).scalars().all())

    result = []
    for s in specs:
        ac_list = s.acceptance_criteria or []
        total_ac = len(ac_list)

        scenarios = s.test_scenarios or []
        # Covered ACs: ACs referenced in at least one test scenario's linked_criteria
        covered_ac_ids: set[str] = set()
        status_counts: dict[str, int] = {}
        for ts in scenarios:
            if isinstance(ts, dict):
                for crit in (ts.get("linked_criteria") or []):
                    covered_ac_ids.add(crit)
                ts_status = ts.get("status", "unknown")
                status_counts[ts_status] = status_counts.get(ts_status, 0) + 1

        # Business rules & API contracts coverage
        brs = s.business_rules or []
        contracts = s.api_contracts or []
        frs = s.functional_requirements or []
        total_frs = len(frs)

        # FRs with at least one BR linked
        fr_indices_with_rules: set[int] = set()
        for br in brs:
            if isinstance(br, dict):
                fr_indices_with_rules |= _resolve_linked_fr_indices(br.get("linked_requirements") or [], frs)
        fr_with_rules_pct = round(len(fr_indices_with_rules) / total_frs * 100, 1) if total_frs > 0 else 0

        # FRs with at least one contract linked
        fr_indices_with_contracts: set[int] = set()
        for ct in contracts:
            if isinstance(ct, dict):
                fr_indices_with_contracts |= _resolve_linked_fr_indices(ct.get("linked_requirements") or [], frs)
        fr_with_contracts_pct = round(len(fr_indices_with_contracts) / total_frs * 100, 1) if total_frs > 0 else 0

        result.append({
            "spec_id": s.id,
            "title": s.title,
            "total_ac": total_ac,
            "covered_ac": len(covered_ac_ids),
            "total_scenarios": len(scenarios),
            "scenario_status_counts": status_counts,
            "business_rules_count": len(brs),
            "api_contracts_count": len(contracts),
            "fr_with_rules_pct": fr_with_rules_pct,
            "fr_with_contracts_pct": fr_with_contracts_pct,
        })
    return result


# ---------------------------------------------------------------------------
# 6) GET /boards/{board_id}/analytics/agents — Agent ranking
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/agents")
async def board_agents(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Agent ranking: group cards by created_by, count done, avg completeness/drift."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    q = select(Card).where(Card.board_id == board_id)
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)
    cards = list((await db.execute(q)).scalars().all())

    # Group by created_by
    groups: dict[str, list] = {}
    for c in cards:
        groups.setdefault(c.created_by, []).append(c)

    # Resolve agent names
    from okto_pulse.core.services.main import resolve_actor_name

    result = []
    for actor_id, actor_cards in groups.items():
        done = [c for c in actor_cards if c.status == CardStatus.DONE]
        comp_vals = []
        drift_vals = []
        for c in done:
            concl = _extract_conclusion(c)
            if concl:
                comp_vals.append(concl.get("completeness", 100))
                drift_vals.append(concl.get("drift", 0))

        actor_name = await resolve_actor_name(db, actor_id, board_id)
        result.append({
            "actor_id": actor_id,
            "actor_name": actor_name,
            "total_cards": len(actor_cards),
            "done_cards": len(done),
            "avg_completeness": round(sum(comp_vals) / len(comp_vals), 1) if comp_vals else None,
            "avg_drift": round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None,
        })

    # Sort by done_cards desc
    result.sort(key=lambda x: x["done_cards"], reverse=True)
    return result


# ---------------------------------------------------------------------------
# 7) GET /boards/{board_id}/analytics/entities — Entity list (paginated)
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/entities")
async def board_entities(
    board_id: str,
    type: str = Query(..., description="Entity type: ideation, spec, or card"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query("", description="Search by title (case-insensitive)"),
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Paginated entity list with relevant metrics. When search is provided, date filters are ignored to search the entire base."""
    # When searching, ignore date filters to search the entire dataset
    dt_from = None if search else _parse_date(date_from)
    dt_to = None if search else _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    if type == "ideation":
        return await _list_ideation_entities(db, board_id, offset, limit, dt_from, dt_to, search)
    elif type == "spec":
        return await _list_spec_entities(db, board_id, offset, limit, dt_from, dt_to, search)
    elif type == "card":
        return await _list_card_entities(db, board_id, offset, limit, dt_from, dt_to, search)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="type must be one of: ideation, spec, card",
        )


# ---------------------------------------------------------------------------
# 8) GET /boards/{board_id}/analytics/entity/{entity_type}/{entity_id} — Detail
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/entity/{entity_type}/{entity_id}")
async def board_entity_detail(
    board_id: str,
    entity_type: str,
    entity_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Detailed analytics for a single entity."""
    await _ensure_board(db, board_id, user_id)

    if entity_type == "spec":
        return await _spec_detail(db, board_id, entity_id)
    elif entity_type == "ideation":
        return await _ideation_detail(db, board_id, entity_id)
    elif entity_type == "card":
        return await _card_detail(db, board_id, entity_id)
    elif entity_type == "refinement":
        return await _refinement_detail(db, board_id, entity_id)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type must be one of: spec, ideation, card, refinement",
        )


# ---------------------------------------------------------------------------
# 9) GET /analytics/overview/export — CSV export of overview data
# ---------------------------------------------------------------------------


@router.get("/analytics/overview/export")
async def analytics_overview_export(
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Export cross-board overview KPIs as CSV."""
    data = await analytics_overview(date_from=date_from, date_to=date_to, user_id=user_id, db=db)

    output = io.StringIO()
    writer = csv.writer(output)

    # Summary row
    writer.writerow(["Metric", "Value"])
    writer.writerow(["Total Ideations", data["total_ideations"]])
    writer.writerow(["Total Specs", data["total_specs"]])
    writer.writerow(["Total Cards (Impl)", data["total_cards_impl"]])
    writer.writerow(["Total Cards (Test)", data["total_cards_test"]])
    writer.writerow(["Avg Completeness", data["avg_completeness"]])
    writer.writerow(["Avg Drift", data["avg_drift"]])

    # Funnel
    writer.writerow([])
    writer.writerow(["Funnel Stage", "Count"])
    for stage, count in data["funnel"].items():
        writer.writerow([stage, count])

    # Velocity
    writer.writerow([])
    writer.writerow(["Week", "Impl", "Test"])
    for v in data["velocity"]:
        writer.writerow([v["week"], v["impl"], v["test"]])

    # Board stats
    writer.writerow([])
    writer.writerow(["Board ID", "Board Name", "Ideations", "Specs", "Cards", "Cards Done"])
    for b in data["boards"]:
        writer.writerow([b["board_id"], b["board_name"], b["ideations"], b["specs"], b["cards"], b["cards_done"]])

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="analytics-overview-{today}.csv"'},
    )


# ---------------------------------------------------------------------------
# 10) GET /boards/{board_id}/analytics/export — CSV export of board analytics
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/export")
async def board_analytics_export(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Export board analytics (funnel, quality, velocity) as CSV."""
    await _ensure_board(db, board_id, user_id)

    funnel = await board_funnel(board_id=board_id, date_from=date_from, date_to=date_to, user_id=user_id, db=db)
    quality = await board_quality(board_id=board_id, date_from=date_from, date_to=date_to, user_id=user_id, db=db)
    velocity = await board_velocity(board_id=board_id, date_from=date_from, date_to=date_to, user_id=user_id, db=db)

    output = io.StringIO()
    writer = csv.writer(output)

    # Funnel
    writer.writerow(["Funnel Stage", "Count"])
    for stage, count in funnel.items():
        writer.writerow([stage, count])

    # Quality scatter
    writer.writerow([])
    writer.writerow(["Card ID", "Title", "Completeness", "Drift"])
    for item in quality:
        writer.writerow([item["card_id"], item["title"], item["completeness"], item["drift"]])

    # Velocity
    writer.writerow([])
    writer.writerow(["Week", "Impl", "Test"])
    for v in velocity:
        writer.writerow([v["week"], v["impl"], v["test"]])

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="analytics-board-{today}.csv"'},
    )


# ---------------------------------------------------------------------------
# 11) GET /boards/{board_id}/analytics/entity/{entity_type}/{entity_id}/export
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/entity/{entity_type}/{entity_id}/export")
async def board_entity_detail_export(
    board_id: str,
    entity_type: str,
    entity_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Export entity detail analytics as CSV."""
    data = await board_entity_detail(
        board_id=board_id, entity_type=entity_type, entity_id=entity_id,
        user_id=user_id, db=db,
    )

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(["Field", "Value"])
    for key, value in data.items():
        if isinstance(value, list):
            # Write list items as sub-table
            writer.writerow([])
            if value and isinstance(value[0], dict):
                headers = list(value[0].keys())
                writer.writerow([key] + headers)
                for item in value:
                    writer.writerow([""] + [item.get(h, "") for h in headers])
            else:
                writer.writerow([key, str(value)])
        elif isinstance(value, dict):
            writer.writerow([key, str(value)])
        else:
            writer.writerow([key, value])

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="analytics-entity-{today}.csv"'},
    )


# ============================================================================
# Internal helpers
# ============================================================================


async def _ensure_board(db: AsyncSession, board_id: str, user_id: str):
    """Raise 404 if board not found or not accessible by user."""
    q = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
    result = await db.execute(q)
    if not result.scalars().first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")


def _compute_velocity(done_cards: list, weeks: int) -> list[dict]:
    """Compute weekly velocity (impl + test + bug) for the last N weeks."""
    now = datetime.now(timezone.utc)
    # Build week buckets (Monday-aligned)
    buckets: dict[str, dict[str, int]] = {}
    for i in range(weeks):
        week_start = now - timedelta(weeks=i)
        # Align to Monday
        week_start = week_start - timedelta(days=week_start.weekday())
        key = week_start.strftime("%Y-%m-%d")
        buckets[key] = {"impl": 0, "test": 0, "bug": 0}

    for c in done_cards:
        if not c.updated_at:
            continue
        updated = c.updated_at
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        # Align to Monday of that week
        week_start = updated - timedelta(days=updated.weekday())
        key = week_start.strftime("%Y-%m-%d")
        if key in buckets:
            if getattr(c, "card_type", "normal") == "bug":
                buckets[key]["bug"] += 1
            elif _is_test_card(c):
                buckets[key]["test"] += 1
            else:
                buckets[key]["impl"] += 1

    # Return sorted oldest first
    result = [
        {"week": k, "impl": v["impl"], "test": v["test"], "bug": v["bug"]}
        for k, v in sorted(buckets.items())
    ]
    return result


async def _list_ideation_entities(
    db: AsyncSession,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    q = select(Ideation).where(Ideation.board_id == board_id)
    if dt_from:
        q = q.where(Ideation.created_at >= dt_from)
    if dt_to:
        q = q.where(Ideation.created_at <= dt_to)
    if search:
        q = q.where(Ideation.title.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
    items_q = q.order_by(Ideation.created_at.desc()).offset(offset).limit(limit)
    ideations = list((await db.execute(items_q)).scalars().all())

    # For each ideation, count derived refinements and specs
    result_items = []
    for i in ideations:
        ref_count = (
            await db.execute(
                select(func.count(Refinement.id)).where(Refinement.ideation_id == i.id)
            )
        ).scalar() or 0
        spec_count = (
            await db.execute(
                select(func.count(Spec.id)).where(Spec.ideation_id == i.id)
            )
        ).scalar() or 0
        result_items.append({
            "id": i.id,
            "title": i.title,
            "status": i.status.value if i.status else None,
            "complexity": i.complexity.value if i.complexity else None,
            "created_at": i.created_at.isoformat() if i.created_at else None,
            "refinement_count": ref_count,
            "spec_count": spec_count,
        })

    return {"total": total, "offset": offset, "limit": limit, "items": result_items}


async def _list_spec_entities(
    db: AsyncSession,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    q = select(Spec).where(Spec.board_id == board_id)
    if dt_from:
        q = q.where(Spec.created_at >= dt_from)
    if dt_to:
        q = q.where(Spec.created_at <= dt_to)
    if search:
        q = q.where(Spec.title.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
    items_q = q.order_by(Spec.created_at.desc()).offset(offset).limit(limit)
    specs = list((await db.execute(items_q)).scalars().all())

    result_items = []
    for s in specs:
        ac_list = s.acceptance_criteria or []
        scenarios = s.test_scenarios or []
        card_count = (
            await db.execute(
                select(func.count(Card.id)).where(Card.spec_id == s.id)
            )
        ).scalar() or 0

        result_items.append({
            "id": s.id,
            "title": s.title,
            "status": s.status.value if s.status else None,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "ac_count": len(ac_list),
            "scenario_count": len(scenarios),
            "card_count": card_count,
            "rules_count": len(s.business_rules or []),
            "contracts_count": len(s.api_contracts or []),
        })

    return {"total": total, "offset": offset, "limit": limit, "items": result_items}


async def _list_card_entities(
    db: AsyncSession,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    q = select(Card).where(Card.board_id == board_id)
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)
    if search:
        q = q.where(Card.title.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
    items_q = q.order_by(Card.created_at.desc()).offset(offset).limit(limit)
    cards = list((await db.execute(items_q)).scalars().all())

    result_items = []
    for c in cards:
        concl = _extract_conclusion(c)
        ct = getattr(c, "card_type", "normal") or "normal"
        result_items.append({
            "id": c.id,
            "title": c.title,
            "status": c.status.value if c.status else None,
            "is_test": _is_test_card(c),
            "card_type": ct if hasattr(ct, "value") else ct,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "completeness": concl.get("completeness") if concl else None,
            "drift": concl.get("drift") if concl else None,
        })

    return {"total": total, "offset": offset, "limit": limit, "items": result_items}


async def _spec_detail(db: AsyncSession, board_id: str, spec_id: str) -> dict:
    """Spec detail: AC coverage, scenario statuses, cards with conclusions, cycle time, derivation chain."""
    q = select(Spec).where(Spec.id == spec_id, Spec.board_id == board_id)
    spec = (await db.execute(q)).scalars().first()
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    ac_list = spec.acceptance_criteria or []
    scenarios = spec.test_scenarios or []

    # Coverage
    covered_ac_ids: set[str] = set()
    scenario_statuses: list[dict] = []
    for ts in scenarios:
        if isinstance(ts, dict):
            for crit in (ts.get("linked_criteria") or []):
                covered_ac_ids.add(crit)
            scenario_statuses.append({
                "id": ts.get("id"),
                "title": ts.get("title"),
                "status": ts.get("status", "unknown"),
            })

    # Cards linked to this spec
    cards_q = select(Card).where(Card.spec_id == spec_id)
    cards = list((await db.execute(cards_q)).scalars().all())
    card_data = []
    for c in cards:
        concl = _extract_conclusion(c)
        ct = getattr(c, "card_type", "normal") or "normal"
        card_data.append({
            "id": c.id,
            "title": c.title,
            "status": c.status.value if c.status else None,
            "is_test": _is_test_card(c),
            "card_type": ct if hasattr(ct, "value") else ct,
            "completeness": concl.get("completeness") if concl else None,
            "drift": concl.get("drift") if concl else None,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "updated_at": c.updated_at.isoformat() if c.updated_at else None,
        })

    # Cycle time: created_at to updated_at for done cards
    done_cards = [c for c in cards if c.status == CardStatus.DONE]
    cycle_times = []
    for c in done_cards:
        if c.created_at and c.updated_at:
            delta = (c.updated_at - c.created_at).total_seconds() / 3600.0
            cycle_times.append(round(delta, 1))
    avg_cycle_hours = round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None

    # Derivation chain
    derivation: dict = {"ideation_id": spec.ideation_id, "refinement_id": spec.refinement_id}

    # Business rules & API contracts
    brs = spec.business_rules or []
    contracts = spec.api_contracts or []
    frs = spec.functional_requirements or []
    total_frs = len(frs)

    fr_indices_with_rules: set[int] = set()
    for br in brs:
        if isinstance(br, dict):
            fr_indices_with_rules |= _resolve_linked_fr_indices(br.get("linked_requirements") or [], frs)
    rules_coverage = round(len(fr_indices_with_rules) / total_frs * 100, 1) if total_frs > 0 else 0

    fr_indices_with_contracts: set[int] = set()
    for ct in contracts:
        if isinstance(ct, dict):
            fr_indices_with_contracts |= _resolve_linked_fr_indices(ct.get("linked_requirements") or [], frs)
    contracts_coverage = round(len(fr_indices_with_contracts) / total_frs * 100, 1) if total_frs > 0 else 0

    # AC details with names and coverage status
    ac_details = []
    for idx, ac_text in enumerate(ac_list):
        ac_details.append({
            "index": idx,
            "text": ac_text,
            "covered": str(idx) in covered_ac_ids or ac_text in covered_ac_ids,
        })

    # FR details with coverage status (rules + contracts)
    fr_details = []
    for idx, fr_text in enumerate(frs):
        fr_details.append({
            "index": idx,
            "text": fr_text,
            "has_rule": idx in fr_indices_with_rules,
            "has_contract": idx in fr_indices_with_contracts,
        })

    # Bug stats for this spec
    bug_cards = [c for c in cards if getattr(c, "card_type", "normal") == "bug"]

    return {
        "spec_id": spec.id,
        "title": spec.title,
        "status": spec.status.value if spec.status else None,
        "total_ac": len(ac_list),
        "covered_ac": len(covered_ac_ids),
        "ac_details": ac_details,
        "total_fr": total_frs,
        "fr_details": fr_details,
        "scenario_statuses": scenario_statuses,
        "cards": card_data,
        "avg_cycle_hours": avg_cycle_hours,
        "derivation": derivation,
        "business_rules": brs,
        "api_contracts": contracts,
        "rules_coverage": rules_coverage,
        "contracts_coverage": contracts_coverage,
        "bugs_count": len(bug_cards),
    }


async def _ideation_detail(db: AsyncSession, board_id: str, ideation_id: str) -> dict:
    """Ideation detail: scope assessment, derived refinements/specs, QA count."""
    q = select(Ideation).where(Ideation.id == ideation_id, Ideation.board_id == board_id)
    ideation = (await db.execute(q)).scalars().first()
    if not ideation:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ideation not found")

    ref_count = (
        await db.execute(
            select(func.count(Refinement.id)).where(Refinement.ideation_id == ideation_id)
        )
    ).scalar() or 0

    spec_count = (
        await db.execute(
            select(func.count(Spec.id)).where(Spec.ideation_id == ideation_id)
        )
    ).scalar() or 0

    qa_count = (
        await db.execute(
            select(func.count(IdeationQAItem.id)).where(IdeationQAItem.ideation_id == ideation_id)
        )
    ).scalar() or 0

    return {
        "ideation_id": ideation.id,
        "title": ideation.title,
        "status": ideation.status.value if ideation.status else None,
        "complexity": ideation.complexity.value if ideation.complexity else None,
        "scope_assessment": ideation.scope_assessment,
        "refinement_count": ref_count,
        "spec_count": spec_count,
        "qa_count": qa_count,
        "created_at": ideation.created_at.isoformat() if ideation.created_at else None,
    }


async def _card_detail(db: AsyncSession, board_id: str, card_id: str) -> dict:
    """Card detail: conclusions, cycle time, spec link."""
    q = select(Card).where(Card.id == card_id, Card.board_id == board_id)
    card = (await db.execute(q)).scalars().first()
    if not card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")

    concl = _extract_conclusion(card)
    cycle_hours = None
    if card.status == CardStatus.DONE and card.created_at and card.updated_at:
        cycle_hours = round((card.updated_at - card.created_at).total_seconds() / 3600.0, 1)

    return {
        "card_id": card.id,
        "title": card.title,
        "status": card.status.value if card.status else None,
        "is_test": _is_test_card(card),
        "card_type": getattr(card, "card_type", "normal") or "normal",
        "spec_id": card.spec_id,
        "completeness": concl.get("completeness") if concl else None,
        "drift": concl.get("drift") if concl else None,
        "conclusions": card.conclusions,
        "cycle_hours": cycle_hours,
        "created_at": card.created_at.isoformat() if card.created_at else None,
        "updated_at": card.updated_at.isoformat() if card.updated_at else None,
    }


async def _refinement_detail(db: AsyncSession, board_id: str, refinement_id: str) -> dict:
    """Refinement detail: scope, KBs, derived specs."""
    q = select(Refinement).where(Refinement.id == refinement_id, Refinement.board_id == board_id)
    refinement = (await db.execute(q)).scalars().first()
    if not refinement:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Refinement not found")

    # Derived specs
    specs_q = select(Spec).where(Spec.refinement_id == refinement_id)
    specs = (await db.execute(specs_q)).scalars().all()

    # Knowledge bases count
    from okto_pulse.core.models.db import RefinementKnowledgeBase
    kb_q = select(func.count()).select_from(RefinementKnowledgeBase).where(
        RefinementKnowledgeBase.refinement_id == refinement_id
    )
    kb_count = (await db.execute(kb_q)).scalar() or 0

    return {
        "refinement_id": refinement.id,
        "title": refinement.title,
        "description": refinement.description,
        "status": refinement.status.value if refinement.status else None,
        "version": refinement.version,
        "ideation_id": refinement.ideation_id,
        "in_scope": refinement.in_scope,
        "out_of_scope": refinement.out_of_scope,
        "analysis": refinement.analysis,
        "decisions": refinement.decisions,
        "knowledge_base_count": kb_count,
        "derived_specs": [
            {"id": s.id, "title": s.title, "status": s.status.value if s.status else None}
            for s in specs
        ],
        "created_at": refinement.created_at.isoformat() if refinement.created_at else None,
        "updated_at": refinement.updated_at.isoformat() if refinement.updated_at else None,
    }
