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
    CardDependency,
    CardStatus,
    CardType,
    Ideation,
    IdeationQAItem,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
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
    """True if the card is explicitly typed as a test card (card_type=test)."""
    ct = getattr(card, "card_type", None)
    if ct is not None and str(ct).endswith("test"):
        return True
    return False


def _is_normal_card(card) -> bool:
    """True if card_type is normal (not bug, not test)."""
    ct = getattr(card, "card_type", None)
    if ct is None:
        return not _is_test_card(card)
    return str(ct).endswith("normal")


def _is_bug_card(card) -> bool:
    ct = getattr(card, "card_type", None)
    return ct is not None and str(ct).endswith("bug")


def _resolve_linked_criteria_to_indices(
    linked_list: list | None, ac_list: list[str]
) -> set[int]:
    """Normalize heterogeneous `linked_criteria` entries into a deduplicated set of
    0-based AC indices.

    Scenarios in the wild store entries in three shapes (historical drift across
    code paths): `int`, numeric `str` (e.g. ``"3"``), or full AC text. Without
    normalization, a set over raw values double-counts the same AC when multiple
    shapes coexist in one spec — producing `covered_ac > total_ac`.

    Out-of-range indices and unmatched texts are dropped silently so the invariant
    `covered_ac <= total_ac` holds even for degenerate inputs.
    """
    if not linked_list or not ac_list:
        return set()
    valid_range = range(len(ac_list))
    resolved: set[int] = set()
    for entry in linked_list:
        if isinstance(entry, bool):
            # bool is a subclass of int in Python; reject to avoid True→1 coincidences
            continue
        if isinstance(entry, int):
            if entry in valid_range:
                resolved.add(entry)
            continue
        if isinstance(entry, str):
            stripped = entry.strip()
            if not stripped:
                continue
            # numeric-string index
            try:
                idx = int(stripped)
            except ValueError:
                pass
            else:
                if idx in valid_range:
                    resolved.add(idx)
                continue
            # text match — tolerant, aligned with mcp/server.py::_spec_coverage
            for i, ac in enumerate(ac_list):
                if stripped == ac or ac.startswith(stripped) or stripped.startswith(ac):
                    resolved.add(i)
                    break
    return resolved


# ---------------------------------------------------------------------------
# Validation Gate aggregation helpers
# ---------------------------------------------------------------------------
#
# Spec Validation Gate records (spec.validations) shape:
#   {id, completeness, assertiveness, ambiguity, recommendation (approve|reject),
#    outcome (success|failed), threshold_violations: [str], resolved_thresholds, ...}
#
# Task Validation Gate records (card.validations) shape:
#   {id, confidence, estimated_completeness | completeness, estimated_drift | drift,
#    recommendation, outcome, threshold_violations: [str], ...}
#
# D3 (multi-count): a single failed record can contribute to multiple rejection
# reason buckets (e.g. completeness_below + ambiguity_above). Total rejection
# reasons per gate may exceed total failed count.
#
# D4 (all history): aggregations walk the full array regardless of active flag.


def _classify_spec_violation(violations: list[str], recommendation: str) -> list[str]:
    """Map a spec validation's threshold_violations + recommendation to reason
    buckets. Returns a list of 0..N reasons from:
    {completeness_below, assertiveness_below, ambiguity_above, reject_recommendation}.
    """
    reasons: list[str] = []
    for v in violations or []:
        v_lower = str(v).lower()
        if "completeness" in v_lower:
            reasons.append("completeness_below")
        elif "assertiveness" in v_lower:
            reasons.append("assertiveness_below")
        elif "ambiguity" in v_lower:
            reasons.append("ambiguity_above")
    if recommendation == "reject":
        reasons.append("reject_recommendation")
    return reasons


def _classify_task_violation(violations: list[str], recommendation: str) -> list[str]:
    """Map a task validation's threshold_violations + recommendation to reason
    buckets: {confidence_below, completeness_below, drift_above, reject_recommendation}.
    """
    reasons: list[str] = []
    for v in violations or []:
        v_lower = str(v).lower()
        if "confidence" in v_lower:
            reasons.append("confidence_below")
        elif "completeness" in v_lower:
            reasons.append("completeness_below")
        elif "drift" in v_lower:
            reasons.append("drift_above")
    if recommendation == "reject":
        reasons.append("reject_recommendation")
    return reasons


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 1) if values else None


def _aggregate_spec_validation_gate(specs: list) -> dict:
    """Aggregate Spec Validation Gate metrics across a collection of specs.

    Returns:
        {
          total_submitted, total_success, total_failed, success_rate (0-100),
          avg_attempts_per_spec, avg_scores: {completeness, assertiveness, ambiguity},
          rejection_reasons: {completeness_below, assertiveness_below,
                              ambiguity_above, reject_recommendation},
          specs_with_validation: int,
        }
    """
    total_submitted = 0
    total_success = 0
    total_failed = 0
    completeness_vals: list[float] = []
    assertiveness_vals: list[float] = []
    ambiguity_vals: list[float] = []
    reasons: dict[str, int] = {
        "completeness_below": 0,
        "assertiveness_below": 0,
        "ambiguity_above": 0,
        "reject_recommendation": 0,
    }
    specs_with_validation = 0
    attempts_per_spec: list[int] = []

    for s in specs:
        vals = getattr(s, "validations", None) or []
        if not isinstance(vals, list) or len(vals) == 0:
            continue
        specs_with_validation += 1
        attempts_per_spec.append(len(vals))
        for v in vals:
            if not isinstance(v, dict):
                continue
            total_submitted += 1
            outcome = v.get("outcome")
            if outcome == "success":
                total_success += 1
            elif outcome == "failed":
                total_failed += 1
                for r in _classify_spec_violation(
                    v.get("threshold_violations") or [],
                    v.get("recommendation", ""),
                ):
                    reasons[r] = reasons.get(r, 0) + 1
            completeness_vals.append(_safe_int(v.get("completeness")))
            assertiveness_vals.append(_safe_int(v.get("assertiveness")))
            ambiguity_vals.append(_safe_int(v.get("ambiguity")))

    return {
        "total_submitted": total_submitted,
        "total_success": total_success,
        "total_failed": total_failed,
        "success_rate": round(total_success / total_submitted * 100, 1) if total_submitted else None,
        "avg_attempts_per_spec": round(sum(attempts_per_spec) / len(attempts_per_spec), 2) if attempts_per_spec else None,
        "avg_scores": {
            "completeness": _avg(completeness_vals),
            "assertiveness": _avg(assertiveness_vals),
            "ambiguity": _avg(ambiguity_vals),
        },
        "rejection_reasons": reasons,
        "specs_with_validation": specs_with_validation,
    }


def _aggregate_task_validation_gate(cards: list) -> dict:
    """Aggregate Task Validation Gate metrics across a collection of cards.

    Returns similar shape as _aggregate_spec_validation_gate but for
    confidence/completeness/drift dimensions.
    """
    total_submitted = 0
    total_success = 0
    total_failed = 0
    confidence_vals: list[float] = []
    completeness_vals: list[float] = []
    drift_vals: list[float] = []
    reasons: dict[str, int] = {
        "confidence_below": 0,
        "completeness_below": 0,
        "drift_above": 0,
        "reject_recommendation": 0,
    }
    cards_with_validation = 0
    attempts_per_card: list[int] = []
    first_pass_count = 0  # cards where the FIRST validation had outcome=success

    for c in cards:
        vals = getattr(c, "validations", None) or []
        if not isinstance(vals, list) or len(vals) == 0:
            continue
        cards_with_validation += 1
        attempts_per_card.append(len(vals))
        if isinstance(vals[0], dict) and vals[0].get("outcome") == "success":
            first_pass_count += 1
        for v in vals:
            if not isinstance(v, dict):
                continue
            total_submitted += 1
            outcome = v.get("outcome")
            verdict = v.get("verdict")
            is_success = outcome == "success" or verdict == "pass"
            is_failed = outcome == "failed" or verdict == "fail"
            if is_success:
                total_success += 1
            elif is_failed:
                total_failed += 1
                for r in _classify_task_violation(
                    v.get("threshold_violations") or [],
                    v.get("recommendation", ""),
                ):
                    reasons[r] = reasons.get(r, 0) + 1
            # Dual-naming support: completeness/drift were renamed from estimated_*
            confidence_vals.append(_safe_int(v.get("confidence")))
            completeness_vals.append(_safe_int(
                v.get("completeness") if v.get("completeness") is not None
                else v.get("estimated_completeness")
            ))
            drift_vals.append(_safe_int(
                v.get("drift") if v.get("drift") is not None
                else v.get("estimated_drift")
            ))

    return {
        "total_submitted": total_submitted,
        "total_success": total_success,
        "total_failed": total_failed,
        "success_rate": round(total_success / total_submitted * 100, 1) if total_submitted else None,
        "avg_attempts_per_card": round(sum(attempts_per_card) / len(attempts_per_card), 2) if attempts_per_card else None,
        "first_pass_rate": round(first_pass_count / cards_with_validation * 100, 1) if cards_with_validation else None,
        "avg_scores": {
            "confidence": _avg(confidence_vals),
            "completeness": _avg(completeness_vals),
            "drift": _avg(drift_vals),
        },
        "rejection_reasons": reasons,
        "cards_with_validation": cards_with_validation,
    }


def _aggregate_spec_evaluation(specs: list) -> dict:
    """Aggregate Spec Evaluation (qualitative gate for validated→in_progress).

    Different from Spec Validation Gate — this is spec.evaluations, the
    breakdown-quality gate submitted by reviewers against validated specs.
    """
    total = 0
    total_approve = 0
    total_reject = 0
    total_request_changes = 0
    overall_vals: list[float] = []
    dimension_avgs: dict[str, list[float]] = {}
    specs_with_eval = 0

    for s in specs:
        evals = getattr(s, "evaluations", None) or []
        if not isinstance(evals, list) or len(evals) == 0:
            continue
        specs_with_eval += 1
        for e in evals:
            if not isinstance(e, dict):
                continue
            total += 1
            rec = e.get("recommendation", "")
            if rec == "approve":
                total_approve += 1
            elif rec == "reject":
                total_reject += 1
            elif rec == "request_changes":
                total_request_changes += 1
            if e.get("overall_score") is not None:
                overall_vals.append(_safe_int(e.get("overall_score")))
            dims = e.get("dimensions", {})
            if isinstance(dims, dict):
                for k, v in dims.items():
                    score = v.get("score") if isinstance(v, dict) else v
                    if score is not None:
                        dimension_avgs.setdefault(k, []).append(_safe_int(score))

    return {
        "total_submitted": total,
        "total_approve": total_approve,
        "total_reject": total_reject,
        "total_request_changes": total_request_changes,
        "approve_rate": round(total_approve / total * 100, 1) if total else None,
        "avg_overall_score": _avg(overall_vals),
        "avg_dimension_scores": {k: _avg(v) for k, v in dimension_avgs.items()},
        "specs_with_evaluation": specs_with_eval,
    }


def _aggregate_sprint_evaluation(sprints: list) -> dict:
    """Aggregate Sprint Evaluation gate across sprints."""
    total = 0
    total_approve = 0
    total_reject = 0
    overall_vals: list[float] = []
    sprints_with_eval = 0

    for sp in sprints:
        evals = getattr(sp, "evaluations", None) or []
        if not isinstance(evals, list) or len(evals) == 0:
            continue
        sprints_with_eval += 1
        for e in evals:
            if not isinstance(e, dict):
                continue
            total += 1
            rec = e.get("recommendation", "")
            if rec == "approve":
                total_approve += 1
            elif rec == "reject":
                total_reject += 1
            if e.get("overall_score") is not None:
                overall_vals.append(_safe_int(e.get("overall_score")))

    return {
        "total_submitted": total,
        "total_approve": total_approve,
        "total_reject": total_reject,
        "approve_rate": round(total_approve / total * 100, 1) if total else None,
        "avg_overall_score": _avg(overall_vals),
        "sprints_with_evaluation": sprints_with_eval,
    }


def _spec_status_breakdown(specs: list) -> dict[str, int]:
    """Count specs per status, aware of all SpecStatus values."""
    out = {s.value: 0 for s in SpecStatus}
    for s in specs:
        st = s.status.value if hasattr(s.status, "value") else str(s.status)
        out[st] = out.get(st, 0) + 1
    return out


def _card_status_breakdown(cards: list) -> dict[str, int]:
    out = {s.value: 0 for s in CardStatus}
    for c in cards:
        st = c.status.value if hasattr(c.status, "value") else str(c.status)
        out[st] = out.get(st, 0) + 1
    return out


def _sprint_status_breakdown(sprints: list) -> dict[str, int]:
    out = {s.value: 0 for s in SprintStatus}
    for sp in sprints:
        st = sp.status.value if hasattr(sp.status, "value") else str(sp.status)
        out[st] = out.get(st, 0) + 1
    return out


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
    """Cross-board KPIs: totals, lifecycle status breakdowns, validation gates,
    sprint summary, funnel, velocity, board list."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    # Fetch boards owned by user
    boards_q = select(Board).where(Board.owner_id == user_id)
    boards_result = await db.execute(boards_q)
    boards = list(boards_result.scalars().all())
    board_ids = [b.id for b in boards]

    if not board_ids:
        return {
            "total_ideations": 0, "total_specs": 0, "total_sprints": 0,
            "total_cards_impl": 0, "total_cards_test": 0, "total_cards_bug": 0,
            "spec_status_breakdown": {},
            "sprint_status_breakdown": {},
            "card_status_breakdown": {},
            "total_business_rules": 0, "total_api_contracts": 0,
            "specs_with_rules": 0, "specs_with_contracts": 0,
            "spec_validation_gate": _aggregate_spec_validation_gate([]),
            "task_validation_gate": _aggregate_task_validation_gate([]),
            "spec_evaluation": _aggregate_spec_evaluation([]),
            "sprint_evaluation": _aggregate_sprint_evaluation([]),
            "funnel": {"ideations": 0, "refinements": 0, "specs": 0, "sprints": 0, "cards": 0, "tests": 0, "bugs": 0, "done": 0},
            "velocity": [],
            "boards": [],
            "total_bugs": 0, "bugs_open": 0, "bugs_done": 0,
            "bugs_by_severity": {"critical": 0, "major": 0, "minor": 0},
            "bug_rate_per_spec": [],
            "avg_triage_hours": None,
        }

    # --- Ideations ---
    ideation_q = select(Ideation).where(
        Ideation.board_id.in_(board_ids),
        Ideation.archived.is_(False),
    )
    if dt_from:
        ideation_q = ideation_q.where(Ideation.created_at >= dt_from)
    if dt_to:
        ideation_q = ideation_q.where(Ideation.created_at <= dt_to)
    ideations = list((await db.execute(ideation_q)).scalars().all())

    # --- Refinements ---
    refinement_q = select(Refinement).where(
        Refinement.board_id.in_(board_ids),
        Refinement.archived.is_(False),
    )
    if dt_from:
        refinement_q = refinement_q.where(Refinement.created_at >= dt_from)
    if dt_to:
        refinement_q = refinement_q.where(Refinement.created_at <= dt_to)
    refinements = list((await db.execute(refinement_q)).scalars().all())

    # --- Specs ---
    spec_q = select(Spec).where(
        Spec.board_id.in_(board_ids),
        Spec.archived.is_(False),
    )
    if dt_from:
        spec_q = spec_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_q = spec_q.where(Spec.created_at <= dt_to)
    specs = list((await db.execute(spec_q)).scalars().all())

    # --- Cards ---
    card_q = select(Card).where(
        Card.board_id.in_(board_ids),
        Card.archived.is_(False),
    )
    if dt_from:
        card_q = card_q.where(Card.created_at >= dt_from)
    if dt_to:
        card_q = card_q.where(Card.created_at <= dt_to)
    cards = list((await db.execute(card_q)).scalars().all())

    impl_cards = [c for c in cards if _is_normal_card(c)]
    test_cards = [c for c in cards if _is_test_card(c)]
    bug_cards_all = [c for c in cards if _is_bug_card(c)]

    # --- Sprints ---
    sprint_q = select(Sprint).where(
        Sprint.board_id.in_(board_ids),
        Sprint.archived.is_(False),
    )
    if dt_from:
        sprint_q = sprint_q.where(Sprint.created_at >= dt_from)
    if dt_to:
        sprint_q = sprint_q.where(Sprint.created_at <= dt_to)
    sprints = list((await db.execute(sprint_q)).scalars().all())

    # Self-reported scores (from card.conclusions — the implementer's report)
    concl_completeness: list[float] = []
    concl_drift: list[float] = []
    for c in cards:
        concl = _extract_conclusion(c)
        if concl:
            concl_completeness.append(concl.get("completeness", 100))
            concl_drift.append(concl.get("drift", 0))

    avg_completeness = _avg(concl_completeness)
    avg_drift = _avg(concl_drift)

    # Reviewer-reported scores come from _aggregate_task_validation_gate.

    # Funnel
    done_cards = [c for c in cards if c.status == CardStatus.DONE]
    funnel = {
        "ideations": len(ideations),
        "refinements": len(refinements),
        "specs": len(specs),
        "sprints": len(sprints),
        "cards": len(cards),
        "tests": len(test_cards),
        "bugs": len(bug_cards_all),
        "done": len(done_cards),
    }

    # Velocity: cards done per week, last 12 weeks (stacked by type)
    velocity = _compute_velocity(done_cards, 12)

    # Per-board stats
    board_stats = []
    for b in boards:
        b_cards = [c for c in cards if c.board_id == b.id]
        b_done = [c for c in b_cards if c.status == CardStatus.DONE]
        b_bugs = [c for c in b_cards if _is_bug_card(c)]
        b_sprints = [sp for sp in sprints if sp.board_id == b.id]
        board_stats.append({
            "board_id": b.id,
            "board_name": b.name,
            "ideations": sum(1 for i in ideations if i.board_id == b.id),
            "refinements": sum(1 for r in refinements if r.board_id == b.id),
            "specs": sum(1 for s in specs if s.board_id == b.id),
            "sprints": len(b_sprints),
            "cards": len(b_cards),
            "cards_done": len(b_done),
            "bugs": len(b_bugs),
        })

    # Status breakdowns (full)
    ideations_done = sum(1 for i in ideations if i.status == IdeationStatus.DONE)
    spec_status_breakdown = _spec_status_breakdown(specs)
    sprint_status_breakdown = _sprint_status_breakdown(sprints)
    card_status_breakdown = _card_status_breakdown(cards)
    specs_done = spec_status_breakdown.get("done", 0)
    specs_with_tests = sum(1 for s in specs if s.test_scenarios and len(s.test_scenarios) > 0)

    # Business Rules & API Contracts aggregation
    total_brs = sum(len(s.business_rules or []) for s in specs)
    total_contracts = sum(len(s.api_contracts or []) for s in specs)
    specs_with_rules = sum(1 for s in specs if s.business_rules and len(s.business_rules) > 0)
    specs_with_contracts = sum(1 for s in specs if s.api_contracts and len(s.api_contracts) > 0)

    # --- Bug metrics ---
    bug_cards = bug_cards_all
    total_bugs = len(bug_cards)
    bugs_open = sum(1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED))
    bugs_done = sum(1 for c in bug_cards if c.status == CardStatus.DONE)
    bugs_by_severity = {
        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
    }

    # --- Validation Gate aggregations ---
    spec_validation_gate = _aggregate_spec_validation_gate(specs)
    task_validation_gate = _aggregate_task_validation_gate(
        [c for c in cards if _is_normal_card(c) or _is_bug_card(c)]
    )
    spec_evaluation = _aggregate_spec_evaluation(specs)
    sprint_evaluation = _aggregate_sprint_evaluation(sprints)

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

    # Fallback: use validation scores if conclusion-based averages are empty
    if avg_completeness is None and task_validation_gate["avg_scores"]["completeness"] is not None:
        avg_completeness = task_validation_gate["avg_scores"]["completeness"]
    if avg_drift is None and task_validation_gate["avg_scores"]["drift"] is not None:
        avg_drift = task_validation_gate["avg_scores"]["drift"]

    # Cycle time: avg hours from created_at to updated_at for done cards
    cycle_times: list[float] = []
    for c in done_cards:
        if c.created_at and c.updated_at:
            ct = round((c.updated_at - c.created_at).total_seconds() / 3600.0, 1)
            cycle_times.append(ct)
    avg_cycle_hours = round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None

    def _lifecycle_ct(items, done_status_str: str) -> float | None:
        times = []
        for item in items:
            if str(item.status) == done_status_str and item.created_at and item.updated_at:
                times.append((item.updated_at - item.created_at).total_seconds() / 3600.0)
        return round(sum(times) / len(times), 1) if times else None

    cycle_time_by_level = {
        "ideation": _lifecycle_ct(ideations, str(IdeationStatus.DONE)),
        "refinement": _lifecycle_ct(refinements, str(RefinementStatus.DONE)),
        "spec": _lifecycle_ct(specs, str(SpecStatus.DONE)),
        "sprint": _lifecycle_ct(sprints, str(SprintStatus.CLOSED)),
        "card": avg_cycle_hours,
    }

    return {
        "total_ideations": len(ideations),
        "ideations_done": ideations_done,
        "total_specs": len(specs),
        "specs_done": specs_done,
        "specs_with_tests": specs_with_tests,
        "total_sprints": len(sprints),
        "spec_status_breakdown": spec_status_breakdown,
        "sprint_status_breakdown": sprint_status_breakdown,
        "card_status_breakdown": card_status_breakdown,
        "total_business_rules": total_brs,
        "total_api_contracts": total_contracts,
        "specs_with_rules": specs_with_rules,
        "specs_with_contracts": specs_with_contracts,
        "total_cards_impl": len(impl_cards),
        "total_cards_test": len(test_cards),
        "total_cards_bug": len(bug_cards_all),
        # Self-reported quality (with validation fallback)
        "avg_completeness": avg_completeness,
        "avg_drift": avg_drift,
        # Cycle time
        "avg_cycle_hours": avg_cycle_hours,
        "cycle_time": cycle_time_by_level,
        # Validation gates — reviewer-reported metrics
        "spec_validation_gate": spec_validation_gate,
        "task_validation_gate": task_validation_gate,
        "spec_evaluation": spec_evaluation,
        "sprint_evaluation": sprint_evaluation,
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
# 2b) GET /boards/{board_id}/analytics/blockers — Triage with root-cause
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/blockers")
async def board_blockers(
    board_id: str,
    stale_hours: int = Query(
        72,
        description="Cards unchanged for more than this many hours while in an active state are flagged as stale.",
        ge=1,
    ),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Triage endpoint: every artifact blocking the funnel, classified by
    root cause so an agent can act directly instead of scanning each level.

    Categories (non-overlapping; a card can appear in multiple):

    - ``dependency_blocked`` — card is not_started/started while at least one
      of its `depends_on_id` targets has NOT reached the DONE status.
    - ``on_hold`` — card is explicitly paused (status=on_hold).
    - ``stale`` — card is in_progress/started/validation but
      ``now() - updated_at > stale_hours``.
    - ``spec_pending_validation`` — spec is approved but lacks an 'approve'
      validation gate (unable to promote to in_progress).
    - ``spec_no_cards`` — spec is validated/in_progress but has ZERO
      non-archived cards linked to it.
    - ``uncovered_scenarios`` — scenarios with no linked test cards.
    """
    await _ensure_board(db, board_id, user_id)

    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(hours=stale_hours)

    blockers: list[dict] = []

    # --- Dependency blocked -------------------------------------------------
    cards_q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
    cards = list((await db.execute(cards_q)).scalars().all())
    card_by_id = {c.id: c for c in cards}

    deps_q = select(CardDependency).where(
        CardDependency.card_id.in_([c.id for c in cards])
    ) if cards else None
    deps = list((await db.execute(deps_q)).scalars().all()) if cards else []
    deps_by_card: dict[str, list[str]] = {}
    for d in deps:
        deps_by_card.setdefault(d.card_id, []).append(d.depends_on_id)

    active_states = {
        CardStatus.NOT_STARTED,
        CardStatus.STARTED,
        CardStatus.IN_PROGRESS,
        CardStatus.VALIDATION,
        CardStatus.ON_HOLD,
    }
    for c in cards:
        if c.status not in active_states:
            continue
        blocking = []
        for dep_id in deps_by_card.get(c.id, []):
            target = card_by_id.get(dep_id)
            if target is None or target.status != CardStatus.DONE:
                blocking.append({
                    "id": dep_id,
                    "title": getattr(target, "title", None),
                    "status": target.status.value if target and target.status else None,
                })
        if blocking:
            blockers.append({
                "type": "dependency_blocked",
                "card_id": c.id,
                "card_title": c.title,
                "card_status": c.status.value,
                "reason": f"Depends on {len(blocking)} unfinished card(s)",
                "evidence": {"blocking_cards": blocking},
            })

    # --- On hold ------------------------------------------------------------
    for c in cards:
        if c.status == CardStatus.ON_HOLD:
            blockers.append({
                "type": "on_hold",
                "card_id": c.id,
                "card_title": c.title,
                "card_status": c.status.value,
                "reason": "Card explicitly paused via status=on_hold",
                "evidence": {"updated_at": c.updated_at.isoformat() if c.updated_at else None},
            })

    # --- Stale --------------------------------------------------------------
    stale_states = {
        CardStatus.STARTED,
        CardStatus.IN_PROGRESS,
        CardStatus.VALIDATION,
    }
    for c in cards:
        if c.status in stale_states and c.updated_at:
            # Ensure timezone-aware compare (sqlite may return naive datetimes)
            upd = c.updated_at
            if upd.tzinfo is None:
                upd = upd.replace(tzinfo=timezone.utc)
            if upd < stale_cutoff:
                age_h = round((now - upd).total_seconds() / 3600.0, 1)
                blockers.append({
                    "type": "stale",
                    "card_id": c.id,
                    "card_title": c.title,
                    "card_status": c.status.value,
                    "reason": f"No update in {age_h}h while in active state",
                    "evidence": {"last_updated": upd.isoformat(), "age_hours": age_h},
                })

    # --- Spec pending validation -------------------------------------------
    specs_q = select(Spec).where(
        Spec.board_id == board_id,
        Spec.archived.is_(False),
    )
    specs = list((await db.execute(specs_q)).scalars().all())
    for s in specs:
        if s.status != SpecStatus.APPROVED:
            continue
        evals = s.evaluations or []
        approved = [e for e in evals if isinstance(e, dict) and e.get("recommendation") == "approve"]
        if not approved:
            blockers.append({
                "type": "spec_pending_validation",
                "spec_id": s.id,
                "spec_title": s.title,
                "reason": "Spec is approved but has no 'approve' evaluation — cannot promote to in_progress",
                "evidence": {"total_evaluations": len(evals)},
            })

    # --- Spec no cards ------------------------------------------------------
    spec_card_counts: dict[str, int] = {}
    for c in cards:
        if c.spec_id:
            spec_card_counts[c.spec_id] = spec_card_counts.get(c.spec_id, 0) + 1
    for s in specs:
        if s.status in (SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS):
            if spec_card_counts.get(s.id, 0) == 0:
                blockers.append({
                    "type": "spec_no_cards",
                    "spec_id": s.id,
                    "spec_title": s.title,
                    "reason": "Spec has zero linked cards — implementation hasn't started",
                    "evidence": {"status": s.status.value},
                })

    # --- Uncovered scenarios ------------------------------------------------
    test_card_scenarios: set[str] = set()
    for c in cards:
        if _is_test_card(c):
            for sid in (c.test_scenario_ids or []):
                test_card_scenarios.add(sid)
    for s in specs:
        for ts in (s.test_scenarios or []):
            if not isinstance(ts, dict):
                continue
            ts_id = ts.get("id")
            if ts_id and ts_id not in test_card_scenarios:
                blockers.append({
                    "type": "uncovered_scenario",
                    "spec_id": s.id,
                    "spec_title": s.title,
                    "scenario_id": ts_id,
                    "scenario_title": ts.get("title"),
                    "reason": "Test scenario has no linked test card — coverage gate will fail",
                    "evidence": {"scenario_status": ts.get("status")},
                })

    summary: dict[str, int] = {}
    for b in blockers:
        summary[b["type"]] = summary.get(b["type"], 0) + 1

    return {
        "board_id": board_id,
        "summary": summary,
        "total": len(blockers),
        "stale_hours_threshold": stale_hours,
        "blockers": blockers,
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

    counts: dict = {}
    for model, key in [
        (Ideation, "ideations"),
        (Refinement, "refinements"),
        (Spec, "specs"),
        (Sprint, "sprints"),
        (Card, "cards"),
    ]:
        q = select(func.count(model.id)).where(
            model.board_id == board_id,
            model.archived.is_(False),
        )
        if dt_from:
            q = q.where(model.created_at >= dt_from)
        if dt_to:
            q = q.where(model.created_at <= dt_to)
        counts[key] = (await db.execute(q)).scalar() or 0

    # Done cards
    done_q = (
        select(func.count(Card.id))
        .where(
            Card.board_id == board_id,
            Card.status == CardStatus.DONE,
            Card.archived.is_(False),
        )
    )
    if dt_from:
        done_q = done_q.where(Card.created_at >= dt_from)
    if dt_to:
        done_q = done_q.where(Card.created_at <= dt_to)
    counts["done"] = (await db.execute(done_q)).scalar() or 0

    # Status breakdowns for KPIs
    ideations_done_q = select(func.count(Ideation.id)).where(
        Ideation.board_id == board_id,
        Ideation.status == IdeationStatus.DONE,
        Ideation.archived.is_(False),
    )
    specs_done_q = select(func.count(Spec.id)).where(
        Spec.board_id == board_id,
        Spec.status == SpecStatus.DONE,
        Spec.archived.is_(False),
    )
    if dt_from:
        ideations_done_q = ideations_done_q.where(Ideation.created_at >= dt_from)
        specs_done_q = specs_done_q.where(Spec.created_at >= dt_from)
    if dt_to:
        ideations_done_q = ideations_done_q.where(Ideation.created_at <= dt_to)
        specs_done_q = specs_done_q.where(Spec.created_at <= dt_to)

    counts["ideations_done"] = (await db.execute(ideations_done_q)).scalar() or 0
    counts["specs_done"] = (await db.execute(specs_done_q)).scalar() or 0

    # Impl / test / bug cards (Python-side filtering — JSON-backed card_type)
    all_cards_q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
    if dt_from:
        all_cards_q = all_cards_q.where(Card.created_at >= dt_from)
    if dt_to:
        all_cards_q = all_cards_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(all_cards_q)).scalars().all())
    counts["cards_impl"] = sum(1 for c in all_cards if _is_normal_card(c))
    counts["cards_test"] = sum(1 for c in all_cards if _is_test_card(c))
    counts["cards_bug"] = sum(1 for c in all_cards if _is_bug_card(c))

    # Business Rules & API Contracts for the board
    spec_objs_q = select(Spec).where(
        Spec.board_id == board_id,
        Spec.archived.is_(False),
    )
    if dt_from:
        spec_objs_q = spec_objs_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_objs_q = spec_objs_q.where(Spec.created_at <= dt_to)
    spec_objs = list((await db.execute(spec_objs_q)).scalars().all())

    counts["rules_count"] = sum(len(s.business_rules or []) for s in spec_objs)
    counts["contracts_count"] = sum(len(s.api_contracts or []) for s in spec_objs)
    counts["specs_with_rules"] = sum(1 for s in spec_objs if s.business_rules and len(s.business_rules) > 0)
    counts["specs_with_contracts"] = sum(1 for s in spec_objs if s.api_contracts and len(s.api_contracts) > 0)

    # Status breakdowns — full lifecycle visibility
    counts["spec_status_breakdown"] = _spec_status_breakdown(spec_objs)
    counts["card_status_breakdown"] = _card_status_breakdown(all_cards)

    # Sprint breakdown
    sprint_objs_q = select(Sprint).where(
        Sprint.board_id == board_id,
        Sprint.archived.is_(False),
    )
    if dt_from:
        sprint_objs_q = sprint_objs_q.where(Sprint.created_at >= dt_from)
    if dt_to:
        sprint_objs_q = sprint_objs_q.where(Sprint.created_at <= dt_to)
    sprint_objs = list((await db.execute(sprint_objs_q)).scalars().all())
    counts["sprint_status_breakdown"] = _sprint_status_breakdown(sprint_objs)

    # Bug metrics for board
    bug_cards = [c for c in all_cards if _is_bug_card(c)]
    counts["bugs_total"] = len(bug_cards)
    counts["bugs_open"] = sum(1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED))
    counts["bugs_by_severity"] = {
        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
    }

    # Avg cycle time for done cards
    done_cards_board = [c for c in all_cards if c.status == CardStatus.DONE]
    cycle_times_board: list[float] = []
    for c in done_cards_board:
        if c.created_at and c.updated_at:
            ct = round((c.updated_at - c.created_at).total_seconds() / 3600.0, 1)
            cycle_times_board.append(ct)
    counts["avg_cycle_hours"] = round(sum(cycle_times_board) / len(cycle_times_board), 1) if cycle_times_board else None

    # Cycle time by lifecycle phase — created_at → updated_at for items in the
    # "done" terminal state of each level. Mirrors the overview endpoint so
    # per-board dashboards can show the same funnel-phase breakdown.
    board_ideations_q = select(Ideation).where(
        Ideation.board_id == board_id,
        Ideation.archived.is_(False),
    )
    board_refinements_q = select(Refinement).where(
        Refinement.board_id == board_id,
        Refinement.archived.is_(False),
    )
    if dt_from:
        board_ideations_q = board_ideations_q.where(Ideation.created_at >= dt_from)
        board_refinements_q = board_refinements_q.where(Refinement.created_at >= dt_from)
    if dt_to:
        board_ideations_q = board_ideations_q.where(Ideation.created_at <= dt_to)
        board_refinements_q = board_refinements_q.where(Refinement.created_at <= dt_to)
    board_ideations = list((await db.execute(board_ideations_q)).scalars().all())
    board_refinements = list((await db.execute(board_refinements_q)).scalars().all())

    def _phase_ct(items, done_status_str: str) -> float | None:
        times = []
        for it in items:
            if str(it.status) == done_status_str and it.created_at and it.updated_at:
                times.append((it.updated_at - it.created_at).total_seconds() / 3600.0)
        return round(sum(times) / len(times), 1) if times else None

    counts["cycle_time_by_phase"] = {
        "ideation": _phase_ct(board_ideations, str(IdeationStatus.DONE)),
        "refinement": _phase_ct(board_refinements, str(RefinementStatus.DONE)),
        "spec": _phase_ct(spec_objs, str(SpecStatus.DONE)),
        "sprint": _phase_ct(sprint_objs, str(SprintStatus.CLOSED)),
        "card": counts["avg_cycle_hours"],
    }
    counts["refinements_done"] = sum(
        1 for r in board_refinements if str(r.status) == str(RefinementStatus.DONE)
    )

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
    """Quality scatters with dual sources:
    - conclusion_reported: self-reported from card.conclusions (implementer)
    - validation_reported: reviewer-reported from card.validations (validator)

    When the task validation gate is active, validation_reported is the
    authoritative source. Both are returned so the UI can show the delta.
    """
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    q = select(Card).where(
        Card.board_id == board_id,
        Card.status == CardStatus.DONE,
        Card.archived.is_(False),
    )
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)

    cards = list((await db.execute(q)).scalars().all())
    conclusion_reported: list[dict] = []
    validation_reported: list[dict] = []
    for c in cards:
        concl = _extract_conclusion(c)
        if concl:
            conclusion_reported.append({
                "card_id": c.id,
                "title": c.title,
                "card_type": str(getattr(c, "card_type", "normal")).replace("CardType.", "").lower(),
                "completeness": concl.get("completeness", 100),
                "drift": concl.get("drift", 0),
            })
        vals = getattr(c, "validations", None) or []
        if isinstance(vals, list) and vals:
            # Use the latest success-outcome validation if any; else last record
            success_vals = [v for v in vals if isinstance(v, dict) and (v.get("outcome") == "success" or v.get("verdict") == "pass")]
            v = success_vals[-1] if success_vals else (vals[-1] if isinstance(vals[-1], dict) else None)
            if v:
                validation_reported.append({
                    "card_id": c.id,
                    "title": c.title,
                    "card_type": str(getattr(c, "card_type", "normal")).replace("CardType.", "").lower(),
                    "confidence": _safe_int(v.get("confidence")),
                    "completeness": _safe_int(
                        v.get("completeness") if v.get("completeness") is not None
                        else v.get("estimated_completeness")
                    ),
                    "drift": _safe_int(
                        v.get("drift") if v.get("drift") is not None
                        else v.get("estimated_drift")
                    ),
                    "outcome": v.get("outcome") or v.get("verdict"),
                })
    return {
        "conclusion_reported": conclusion_reported,
        "validation_reported": validation_reported,
    }


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
    """Cards done per week, stacked by impl/test/bug + validation_bounce series."""
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    all_q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
    if dt_from:
        all_q = all_q.where(Card.created_at >= dt_from)
    if dt_to:
        all_q = all_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(all_q)).scalars().all())
    done_cards = [c for c in all_cards if c.status == CardStatus.DONE]
    return _compute_velocity(done_cards, weeks, all_cards=all_cards)


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

    spec_q = select(Spec).where(
        Spec.board_id == board_id,
        Spec.archived.is_(False),
    )
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
        # Covered ACs: ACs referenced in at least one test scenario's linked_criteria.
        # Normalize to int indices so mixed-type entries (idx / str-idx / text) dedup.
        covered_ac_indices: set[int] = set()
        status_counts: dict[str, int] = {}
        for ts in scenarios:
            if isinstance(ts, dict):
                covered_ac_indices |= _resolve_linked_criteria_to_indices(
                    ts.get("linked_criteria"), ac_list
                )
                ts_status = ts.get("status", "unknown")
                status_counts[ts_status] = status_counts.get(ts_status, 0) + 1
        covered_ac_count = min(len(covered_ac_indices), total_ac)

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
            "covered_ac": covered_ac_count,
            "total_scenarios": len(scenarios),
            "scenario_status_counts": status_counts,
            "business_rules_count": len(brs),
            "api_contracts_count": len(contracts),
            "fr_with_rules_pct": fr_with_rules_pct,
            "fr_with_contracts_pct": fr_with_contracts_pct,
        })
    return result


# ---------------------------------------------------------------------------
# NEW: /boards/{board_id}/analytics/validations — Validation gate panel
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/validations")
async def board_validations(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Validation Gate panel for a board.

    Returns:
    - spec_validation_gate: aggregate across all specs + per-spec breakdown
    - task_validation_gate: aggregate across all cards + per-card breakdown
    - spec_evaluation: aggregate (different gate — qualitative breakdown quality)
    - sprint_evaluation: aggregate
    """
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    spec_q = select(Spec).where(
        Spec.board_id == board_id,
        Spec.archived.is_(False),
    )
    if dt_from:
        spec_q = spec_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_q = spec_q.where(Spec.created_at <= dt_to)
    specs = list((await db.execute(spec_q)).scalars().all())

    card_q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
    if dt_from:
        card_q = card_q.where(Card.created_at >= dt_from)
    if dt_to:
        card_q = card_q.where(Card.created_at <= dt_to)
    cards = list((await db.execute(card_q)).scalars().all())

    sprint_q = select(Sprint).where(
        Sprint.board_id == board_id,
        Sprint.archived.is_(False),
    )
    if dt_from:
        sprint_q = sprint_q.where(Sprint.created_at >= dt_from)
    if dt_to:
        sprint_q = sprint_q.where(Sprint.created_at <= dt_to)
    sprints = list((await db.execute(sprint_q)).scalars().all())

    # Per-spec breakdown for Spec Validation Gate — walks full history (D4)
    per_spec: list[dict] = []
    for s in specs:
        vals = getattr(s, "validations", None) or []
        if not isinstance(vals, list) or len(vals) == 0:
            continue
        agg = _aggregate_spec_validation_gate([s])
        last = vals[-1] if isinstance(vals[-1], dict) else None
        per_spec.append({
            "spec_id": s.id,
            "title": s.title,
            "status": s.status.value if hasattr(s.status, "value") else str(s.status),
            "attempts": len(vals),
            "last_outcome": last.get("outcome") if last else None,
            "last_completeness": _safe_int(last.get("completeness")) if last else None,
            "last_assertiveness": _safe_int(last.get("assertiveness")) if last else None,
            "last_ambiguity": _safe_int(last.get("ambiguity")) if last else None,
            "success_count": agg["total_success"],
            "failed_count": agg["total_failed"],
            "rejection_reasons": agg["rejection_reasons"],
            "current_validation_id": getattr(s, "current_validation_id", None),
        })
    per_spec.sort(key=lambda x: x["failed_count"], reverse=True)

    # Per-card breakdown for Task Validation Gate
    per_card: list[dict] = []
    for c in cards:
        vals = getattr(c, "validations", None) or []
        if not isinstance(vals, list) or len(vals) == 0:
            continue
        agg = _aggregate_task_validation_gate([c])
        last = vals[-1] if isinstance(vals[-1], dict) else None
        per_card.append({
            "card_id": c.id,
            "title": c.title,
            "card_type": str(getattr(c, "card_type", "normal")).replace("CardType.", "").lower(),
            "spec_id": c.spec_id,
            "sprint_id": c.sprint_id,
            "status": c.status.value if hasattr(c.status, "value") else str(c.status),
            "attempts": len(vals),
            "last_outcome": (last.get("outcome") or last.get("verdict")) if last else None,
            "last_confidence": _safe_int(last.get("confidence")) if last else None,
            "last_completeness": _safe_int(
                last.get("completeness") if last and last.get("completeness") is not None
                else (last.get("estimated_completeness") if last else None)
            ) if last else None,
            "last_drift": _safe_int(
                last.get("drift") if last and last.get("drift") is not None
                else (last.get("estimated_drift") if last else None)
            ) if last else None,
            "success_count": agg["total_success"],
            "failed_count": agg["total_failed"],
            "rejection_reasons": agg["rejection_reasons"],
        })
    per_card.sort(key=lambda x: x["failed_count"], reverse=True)

    return {
        "spec_validation_gate": {
            **_aggregate_spec_validation_gate(specs),
            "per_spec": per_spec,
        },
        "task_validation_gate": {
            **_aggregate_task_validation_gate(cards),
            "per_card": per_card,
        },
        "spec_evaluation": _aggregate_spec_evaluation(specs),
        "sprint_evaluation": _aggregate_sprint_evaluation(sprints),
    }


# ---------------------------------------------------------------------------
# NEW: /boards/{board_id}/analytics/sprints — Sprint panel
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/sprints")
async def board_sprints_analytics(
    board_id: str,
    date_from: str | None = Query(None, alias="from"),
    date_to: str | None = Query(None, alias="to"),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Sprint panel for a board.

    Returns:
    - summary: counts by status + evaluation aggregate
    - sprints: per-sprint breakdown with cards, completion, last eval
    """
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    sprint_q = select(Sprint).where(
        Sprint.board_id == board_id,
        Sprint.archived.is_(False),
    )
    if dt_from:
        sprint_q = sprint_q.where(Sprint.created_at >= dt_from)
    if dt_to:
        sprint_q = sprint_q.where(Sprint.created_at <= dt_to)
    sprints = list((await db.execute(sprint_q)).scalars().all())

    card_q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
    if dt_from:
        card_q = card_q.where(Card.created_at >= dt_from)
    if dt_to:
        card_q = card_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(card_q)).scalars().all())

    per_sprint: list[dict] = []
    for sp in sprints:
        sp_cards = [c for c in all_cards if c.sprint_id == sp.id]
        done_cards = [c for c in sp_cards if c.status == CardStatus.DONE]
        total = len(sp_cards)
        done = len(done_cards)
        completion_rate = round(done / total * 100, 1) if total else 0.0
        evals = getattr(sp, "evaluations", None) or []
        last_eval = None
        if isinstance(evals, list) and evals and isinstance(evals[-1], dict):
            last_eval = {
                "overall_score": evals[-1].get("overall_score"),
                "recommendation": evals[-1].get("recommendation"),
                "evaluator_name": evals[-1].get("evaluator_name"),
                "created_at": evals[-1].get("created_at"),
            }
        task_gate = _aggregate_task_validation_gate(sp_cards)

        # Self-reported quality from card.conclusions on this sprint's cards.
        # Falls back to the validation gate's reviewer-reported avg_scores when
        # no implementer conclusions exist (e.g. validation-gate-only flow).
        sp_completeness: list[float] = []
        sp_drift: list[float] = []
        for c in sp_cards:
            concl = _extract_conclusion(c)
            if concl:
                if concl.get("completeness") is not None:
                    sp_completeness.append(concl["completeness"])
                if concl.get("drift") is not None:
                    sp_drift.append(concl["drift"])
        avg_completeness = _avg(sp_completeness)
        avg_drift = _avg(sp_drift)
        if avg_completeness is None:
            avg_completeness = task_gate["avg_scores"].get("completeness")
        if avg_drift is None:
            avg_drift = task_gate["avg_scores"].get("drift")

        per_sprint.append({
            "sprint_id": sp.id,
            "title": sp.title,
            "status": sp.status.value if hasattr(sp.status, "value") else str(sp.status),
            "spec_id": sp.spec_id,
            "total_cards": total,
            "done_cards": done,
            "completion_rate": completion_rate,
            "avg_completeness": avg_completeness,
            "avg_drift": avg_drift,
            "card_status_breakdown": _card_status_breakdown(sp_cards),
            "evaluations_count": len(evals),
            "last_evaluation": last_eval,
            "task_validation_gate": {
                "total_submitted": task_gate["total_submitted"],
                "total_success": task_gate["total_success"],
                "total_failed": task_gate["total_failed"],
                "rejection_reasons": task_gate["rejection_reasons"],
                "first_pass_rate": task_gate["first_pass_rate"],
            },
        })
    per_sprint.sort(key=lambda x: x["total_cards"], reverse=True)

    return {
        "summary": {
            "total_sprints": len(sprints),
            "status_breakdown": _sprint_status_breakdown(sprints),
            "avg_completion_rate": round(
                sum(p["completion_rate"] for p in per_sprint) / len(per_sprint), 1
            ) if per_sprint else None,
            "sprint_evaluation": _aggregate_sprint_evaluation(sprints),
        },
        "sprints": per_sprint,
    }


# ---------------------------------------------------------------------------
# NEW: /boards/{board_id}/analytics/spec/{spec_id} — Per-spec detail
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/spec/{spec_id}")
async def board_spec_analytics(
    board_id: str,
    spec_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-spec analytics: validation timeline, task gate summary, coverage."""
    await _ensure_board(db, board_id, user_id)

    spec = (await db.execute(
        select(Spec).where(Spec.id == spec_id, Spec.board_id == board_id)
    )).scalar_one_or_none()
    if not spec:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Spec not found")

    cards = list((await db.execute(
        select(Card).where(Card.spec_id == spec_id, Card.archived.is_(False))
    )).scalars().all())

    # Validation timeline: all submissions (D4), oldest first
    validation_timeline: list[dict] = []
    for v in (spec.validations or []):
        if not isinstance(v, dict):
            continue
        validation_timeline.append({
            "id": v.get("id"),
            "reviewer_id": v.get("reviewer_id"),
            "reviewer_name": v.get("reviewer_name"),
            "completeness": _safe_int(v.get("completeness")),
            "assertiveness": _safe_int(v.get("assertiveness")),
            "ambiguity": _safe_int(v.get("ambiguity")),
            "recommendation": v.get("recommendation"),
            "outcome": v.get("outcome"),
            "threshold_violations": v.get("threshold_violations") or [],
            "rejection_reasons": _classify_spec_violation(
                v.get("threshold_violations") or [], v.get("recommendation", "")
            ),
            "resolved_thresholds": v.get("resolved_thresholds"),
            "created_at": v.get("created_at"),
            "active": v.get("id") == getattr(spec, "current_validation_id", None),
        })

    return {
        "spec_id": spec_id,
        "title": spec.title,
        "status": spec.status.value if hasattr(spec.status, "value") else str(spec.status),
        "version": spec.version,
        "gate_status": {
            "current_validation_id": getattr(spec, "current_validation_id", None),
            "locked": getattr(spec, "current_validation_id", None) is not None
                and spec.status
                in (SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE),
            "total_submissions": len(validation_timeline),
        },
        "validation_timeline": validation_timeline,
        "task_validation_summary": _aggregate_task_validation_gate(cards),
        "spec_evaluation": _aggregate_spec_evaluation([spec]),
        "cards_summary": {
            "total": len(cards),
            "by_status": _card_status_breakdown(cards),
            "by_type": {
                "normal": sum(1 for c in cards if _is_normal_card(c)),
                "test": sum(1 for c in cards if _is_test_card(c)),
                "bug": sum(1 for c in cards if _is_bug_card(c)),
            },
        },
    }


# ---------------------------------------------------------------------------
# NEW: /boards/{board_id}/analytics/sprint/{sprint_id} — Per-sprint detail
# ---------------------------------------------------------------------------


@router.get("/boards/{board_id}/analytics/sprint/{sprint_id}")
async def board_sprint_analytics(
    board_id: str,
    sprint_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-sprint analytics: kanban distribution, task gate, evaluation timeline."""
    await _ensure_board(db, board_id, user_id)

    sprint = (await db.execute(
        select(Sprint).where(Sprint.id == sprint_id, Sprint.board_id == board_id)
    )).scalar_one_or_none()
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")

    cards = list((await db.execute(
        select(Card).where(Card.sprint_id == sprint_id, Card.archived.is_(False))
    )).scalars().all())
    done_cards = [c for c in cards if c.status == CardStatus.DONE]

    # Evaluation timeline (append-only) — oldest first
    eval_timeline: list[dict] = []
    for e in (sprint.evaluations or []):
        if not isinstance(e, dict):
            continue
        eval_timeline.append({
            "id": e.get("id"),
            "evaluator_id": e.get("evaluator_id"),
            "evaluator_name": e.get("evaluator_name"),
            "dimensions": e.get("dimensions"),
            "overall_score": e.get("overall_score"),
            "recommendation": e.get("recommendation"),
            "stale": e.get("stale", False),
            "created_at": e.get("created_at"),
        })

    # Weekly velocity during the sprint window (or last 4 weeks if window missing)
    velocity = _compute_velocity(done_cards, 4, all_cards=cards)

    return {
        "sprint_id": sprint_id,
        "title": sprint.title,
        "status": sprint.status.value if hasattr(sprint.status, "value") else str(sprint.status),
        "spec_id": sprint.spec_id,
        "kanban_distribution": _card_status_breakdown(cards),
        "cards_summary": {
            "total": len(cards),
            "done": len(done_cards),
            "completion_rate": round(len(done_cards) / len(cards) * 100, 1) if cards else 0.0,
            "by_type": {
                "normal": sum(1 for c in cards if _is_normal_card(c)),
                "test": sum(1 for c in cards if _is_test_card(c)),
                "bug": sum(1 for c in cards if _is_bug_card(c)),
            },
        },
        "task_validation_gate": _aggregate_task_validation_gate(cards),
        "evaluation_timeline": eval_timeline,
        "velocity": velocity,
    }


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
    """Agent activity ranking with role-aware metrics.

    Groups by `created_by` (cards) plus reviewer_id (validations) so both
    implementers and validators show up. Emits:
    - total_cards, done_cards (implementer side)
    - avg_completeness, avg_drift (self-reported from conclusions)
    - task_validations_submitted, task_validation_success_rate
    - spec_validations_submitted, spec_validation_success_rate
    - first_pass_acceptance: % of own cards that passed validation on 1st try
    """
    dt_from = _parse_date(date_from)
    dt_to = _parse_date(date_to, end_of_day=True)

    await _ensure_board(db, board_id, user_id)

    q = select(Card).where(Card.board_id == board_id, Card.archived.is_(False))
    if dt_from:
        q = q.where(Card.created_at >= dt_from)
    if dt_to:
        q = q.where(Card.created_at <= dt_to)
    cards = list((await db.execute(q)).scalars().all())

    specs = list((await db.execute(
        select(Spec).where(Spec.board_id == board_id, Spec.archived.is_(False))
    )).scalars().all())

    # Collect actors from both cards and validations
    actors: set[str] = set()
    for c in cards:
        if c.created_by:
            actors.add(c.created_by)
    for c in cards:
        for v in (getattr(c, "validations", None) or []):
            if isinstance(v, dict):
                rid = v.get("reviewer_id") or v.get("evaluator_id")
                if rid:
                    actors.add(rid)
    for s in specs:
        for v in (getattr(s, "validations", None) or []):
            if isinstance(v, dict):
                rid = v.get("reviewer_id")
                if rid:
                    actors.add(rid)

    from okto_pulse.core.services.main import resolve_actor_name

    result = []
    for actor_id in actors:
        actor_cards = [c for c in cards if c.created_by == actor_id]
        done = [c for c in actor_cards if c.status == CardStatus.DONE]
        comp_vals: list[float] = []
        drift_vals: list[float] = []
        for c in done:
            concl = _extract_conclusion(c)
            if concl:
                comp_vals.append(concl.get("completeness", 100))
                drift_vals.append(concl.get("drift", 0))

        # Task validations submitted BY this actor (as reviewer)
        task_sub = 0
        task_sub_success = 0
        for c in cards:
            for v in (getattr(c, "validations", None) or []):
                if not isinstance(v, dict):
                    continue
                if (v.get("reviewer_id") or v.get("evaluator_id")) == actor_id:
                    task_sub += 1
                    if v.get("outcome") == "success" or v.get("verdict") == "pass":
                        task_sub_success += 1

        # Spec validations submitted BY this actor
        spec_sub = 0
        spec_sub_success = 0
        for s in specs:
            for v in (getattr(s, "validations", None) or []):
                if not isinstance(v, dict):
                    continue
                if v.get("reviewer_id") == actor_id:
                    spec_sub += 1
                    if v.get("outcome") == "success":
                        spec_sub_success += 1

        # First-pass acceptance on own cards
        own_with_vals = [c for c in actor_cards if getattr(c, "validations", None)]
        first_pass = 0
        for c in own_with_vals:
            vals = c.validations or []
            if vals and isinstance(vals[0], dict) and (
                vals[0].get("outcome") == "success" or vals[0].get("verdict") == "pass"
            ):
                first_pass += 1
        first_pass_rate = round(first_pass / len(own_with_vals) * 100, 1) if own_with_vals else None

        actor_name = await resolve_actor_name(db, actor_id, board_id)
        result.append({
            "actor_id": actor_id,
            "actor_name": actor_name,
            "total_cards": len(actor_cards),
            "done_cards": len(done),
            "avg_completeness": round(sum(comp_vals) / len(comp_vals), 1) if comp_vals else None,
            "avg_drift": round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None,
            "task_validations_submitted": task_sub,
            "task_validation_success_rate": round(task_sub_success / task_sub * 100, 1) if task_sub else None,
            "spec_validations_submitted": spec_sub,
            "spec_validation_success_rate": round(spec_sub_success / spec_sub * 100, 1) if spec_sub else None,
            "first_pass_acceptance_rate": first_pass_rate,
        })

    # Sort by most active (combined activity)
    result.sort(
        key=lambda x: (x["done_cards"] + x["task_validations_submitted"] + x["spec_validations_submitted"]),
        reverse=True,
    )
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
    elif entity_type == "sprint":
        return await _sprint_detail(db, board_id, entity_id)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type must be one of: spec, ideation, card, refinement, sprint",
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


def _compute_velocity(done_cards: list, weeks: int, all_cards: list | None = None) -> list[dict]:
    """Compute weekly velocity for the last N weeks, stacked by card type.

    Series:
    - impl: normal cards moved to done that week
    - test: test cards moved to done that week
    - bug:  bug cards moved to done that week
    - validation_bounce: count of task validations with outcome=failed created
      that week (approximates "how many revalidations happened"). Uses
      `all_cards` (not just done_cards) to catch failures on cards still
      in-progress. When all_cards is None, falls back to done_cards only.
    """
    now = datetime.now(timezone.utc)
    # Build week buckets (Monday-aligned)
    buckets: dict[str, dict[str, int]] = {}
    for i in range(weeks):
        week_start = now - timedelta(weeks=i)
        week_start = week_start - timedelta(days=week_start.weekday())
        key = week_start.strftime("%Y-%m-%d")
        buckets[key] = {"impl": 0, "test": 0, "bug": 0, "validation_bounce": 0}

    for c in done_cards:
        if not c.updated_at:
            continue
        updated = c.updated_at
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        week_start = updated - timedelta(days=updated.weekday())
        key = week_start.strftime("%Y-%m-%d")
        if key in buckets:
            if _is_bug_card(c):
                buckets[key]["bug"] += 1
            elif _is_test_card(c):
                buckets[key]["test"] += 1
            else:
                buckets[key]["impl"] += 1

    # validation_bounce: count failed task validations by week
    pool = all_cards if all_cards is not None else done_cards
    for c in pool:
        vals = getattr(c, "validations", None) or []
        if not isinstance(vals, list):
            continue
        for v in vals:
            if not isinstance(v, dict):
                continue
            if v.get("outcome") == "failed" or v.get("verdict") == "fail":
                created_at = v.get("created_at")
                if not created_at:
                    continue
                try:
                    dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                week_start = dt - timedelta(days=dt.weekday())
                key = week_start.strftime("%Y-%m-%d")
                if key in buckets:
                    buckets[key]["validation_bounce"] += 1

    # Return sorted oldest first
    result = [
        {
            "week": k,
            "impl": v["impl"], "test": v["test"], "bug": v["bug"],
            "validation_bounce": v["validation_bounce"],
        }
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
    q = select(Ideation).where(
        Ideation.board_id == board_id,
        Ideation.archived.is_(False),
    )
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
                select(func.count(Refinement.id)).where(
                    Refinement.ideation_id == i.id,
                    Refinement.archived.is_(False),
                )
            )
        ).scalar() or 0
        spec_count = (
            await db.execute(
                select(func.count(Spec.id)).where(
                    Spec.ideation_id == i.id,
                    Spec.archived.is_(False),
                )
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
    q = select(Spec).where(
        Spec.board_id == board_id,
        Spec.archived.is_(False),
    )
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
                select(func.count(Card.id)).where(
                    Card.spec_id == s.id,
                    Card.archived.is_(False),
                )
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
    q = select(Card).where(
        Card.board_id == board_id,
        Card.archived.is_(False),
    )
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

    # Coverage — normalize mixed linked_criteria formats (int / str-idx / AC text)
    # to int indices so the `covered_ac <= total_ac` invariant holds.
    covered_ac_indices: set[int] = set()
    scenario_statuses: list[dict] = []
    for ts in scenarios:
        if isinstance(ts, dict):
            covered_ac_indices |= _resolve_linked_criteria_to_indices(
                ts.get("linked_criteria"), ac_list
            )
            scenario_statuses.append({
                "id": ts.get("id"),
                "title": ts.get("title"),
                "status": ts.get("status", "unknown"),
            })
    covered_ac_count = min(len(covered_ac_indices), len(ac_list))

    # Cards linked to this spec
    cards_q = select(Card).where(
        Card.spec_id == spec_id,
        Card.archived.is_(False),
    )
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
            "covered": idx in covered_ac_indices,
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

    # Sprint breakdown
    sprints_q = select(Sprint).where(Sprint.spec_id == spec_id, Sprint.archived.is_(False))
    sprints = list((await db.execute(sprints_q)).scalars().all())
    sprint_summaries = []
    for sp in sprints:
        sp_cards = [c for c in cards if getattr(c, "sprint_id", None) == sp.id]
        sp_done = [c for c in sp_cards if c.status == CardStatus.DONE]
        sp_concls = [_extract_conclusion(c) for c in sp_done if _extract_conclusion(c)]
        sp_completeness = [cn.get("completeness") for cn in sp_concls if cn.get("completeness") is not None]
        sp_drift = [cn.get("drift") for cn in sp_concls if cn.get("drift") is not None]
        sp_cycle = []
        for c in sp_done:
            if c.created_at and c.updated_at:
                sp_cycle.append(round((c.updated_at - c.created_at).total_seconds() / 3600.0, 1))
        sprint_summaries.append({
            "sprint_id": sp.id, "title": sp.title, "status": sp.status.value,
            "tasks_total": len(sp_cards), "tasks_done": len(sp_done),
            "progress": round(len(sp_done) / len(sp_cards) * 100, 1) if sp_cards else 0,
            "avg_completeness": round(sum(sp_completeness) / len(sp_completeness), 1) if sp_completeness else None,
            "avg_drift": round(sum(sp_drift) / len(sp_drift), 1) if sp_drift else None,
            "avg_cycle_hours": round(sum(sp_cycle) / len(sp_cycle), 1) if sp_cycle else None,
            "evaluations_count": len(sp.evaluations or []),
        })

    return {
        "spec_id": spec.id,
        "title": spec.title,
        "status": spec.status.value if spec.status else None,
        "total_ac": len(ac_list),
        "covered_ac": covered_ac_count,
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
        "sprints": sprint_summaries,
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
    """Card detail: conclusions, validations history, cycle time, spec link."""
    q = select(Card).where(Card.id == card_id, Card.board_id == board_id)
    card = (await db.execute(q)).scalars().first()
    if not card:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Card not found")

    concl = _extract_conclusion(card)
    cycle_hours = None
    if card.status == CardStatus.DONE and card.created_at and card.updated_at:
        cycle_hours = round((card.updated_at - card.created_at).total_seconds() / 3600.0, 1)

    # Normalize card_type to lowercase string (enum or raw).
    ct = getattr(card, "card_type", "normal")
    card_type = str(ct).replace("CardType.", "").lower() or "normal"

    return {
        "card_id": card.id,
        "title": card.title,
        "status": card.status.value if card.status else None,
        "is_test": _is_test_card(card),
        "card_type": card_type,
        "spec_id": card.spec_id,
        "sprint_id": card.sprint_id,
        "completeness": concl.get("completeness") if concl else None,
        "drift": concl.get("drift") if concl else None,
        "conclusions": card.conclusions,
        "validations": getattr(card, "validations", None) or [],
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


async def _sprint_detail(db: AsyncSession, board_id: str, sprint_id: str) -> dict:
    """Sprint detail: tasks done/total, completeness avg, drift avg, cycle time, evaluations, comparison."""
    q = select(Sprint).where(Sprint.id == sprint_id, Sprint.board_id == board_id)
    sprint = (await db.execute(q)).scalars().first()
    if not sprint:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sprint not found")

    # Cards in this sprint (skip archived to keep counts honest)
    cards_q = select(Card).where(
        Card.sprint_id == sprint_id,
        Card.archived.is_(False),
    )
    cards = list((await db.execute(cards_q)).scalars().all())

    done_cards = [c for c in cards if c.status == CardStatus.DONE]
    cancelled = [c for c in cards if c.status == CardStatus.CANCELLED]
    in_progress = [c for c in cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED)]

    # Completeness and drift: prefer self-reported conclusions, fall back to
    # the validation gate's reviewer score when no conclusion exists. This
    # ensures sprints that use the task validation gate flow still surface
    # quality metrics instead of showing "--".
    completeness_vals: list[float] = []
    drift_vals: list[float] = []
    cycle_times: list[float] = []
    card_metrics = []
    for c in cards:
        concl = _extract_conclusion(c)
        comp = concl.get("completeness") if concl else None
        dr = concl.get("drift") if concl else None
        if comp is None or dr is None:
            vals = getattr(c, "validations", None) or []
            last_val = next(
                (v for v in reversed(vals) if isinstance(v, dict)),
                None,
            )
            if last_val:
                if comp is None:
                    comp = last_val.get("completeness") or last_val.get("estimated_completeness")
                if dr is None:
                    dr = last_val.get("drift") or last_val.get("estimated_drift")
        ct_hours = None
        if c.status == CardStatus.DONE and c.created_at and c.updated_at:
            ct_hours = round((c.updated_at - c.created_at).total_seconds() / 3600.0, 1)
            cycle_times.append(ct_hours)
        if comp is not None:
            completeness_vals.append(comp)
        if dr is not None:
            drift_vals.append(dr)
        card_metrics.append({
            "id": c.id, "title": c.title,
            "status": c.status.value if c.status else None,
            "card_type": getattr(c, "card_type", "normal"),
            "completeness": comp, "drift": dr,
            "cycle_hours": ct_hours,
        })

    # Evaluations summary
    evaluations = sprint.evaluations or []
    non_stale = [e for e in evaluations if not e.get("stale")]
    approvals = [e for e in non_stale if e.get("recommendation") == "approve"]

    # Scoped test scenario coverage
    spec = await db.get(Spec, sprint.spec_id) if sprint.spec_id else None
    scoped_scenarios = []
    if spec and sprint.test_scenario_ids:
        all_scenarios = {s.get("id"): s for s in (spec.test_scenarios or [])}
        for ts_id in sprint.test_scenario_ids:
            sc = all_scenarios.get(ts_id)
            if sc:
                scoped_scenarios.append({
                    "id": sc.get("id"), "title": sc.get("title"),
                    "status": sc.get("status", "unknown"),
                })
    passed = [s for s in scoped_scenarios if s["status"] == "passed"]

    # Sibling sprints for comparison
    comparison = []
    if sprint.spec_id:
        siblings_q = select(Sprint).where(
            Sprint.spec_id == sprint.spec_id, Sprint.archived.is_(False),
        )
        siblings = list((await db.execute(siblings_q)).scalars().all())
        for sib in siblings:
            sib_cards_q = select(Card).where(
                Card.sprint_id == sib.id,
                Card.archived.is_(False),
            )
            sib_cards = list((await db.execute(sib_cards_q)).scalars().all())
            sib_done = [c for c in sib_cards if c.status == CardStatus.DONE]
            sib_concls = [_extract_conclusion(c) for c in sib_done if _extract_conclusion(c)]
            sib_comp = [cn.get("completeness") for cn in sib_concls if cn.get("completeness") is not None]
            sib_dr = [cn.get("drift") for cn in sib_concls if cn.get("drift") is not None]
            comparison.append({
                "sprint_id": sib.id, "title": sib.title, "status": sib.status.value,
                "tasks_total": len(sib_cards), "tasks_done": len(sib_done),
                "avg_completeness": round(sum(sib_comp) / len(sib_comp), 1) if sib_comp else None,
                "avg_drift": round(sum(sib_dr) / len(sib_dr), 1) if sib_dr else None,
                "is_current": sib.id == sprint_id,
            })

    return {
        "sprint_id": sprint.id,
        "title": sprint.title,
        "status": sprint.status.value,
        "spec_id": sprint.spec_id,
        "spec_version": sprint.spec_version,
        "tasks_total": len(cards),
        "tasks_done": len(done_cards),
        "tasks_cancelled": len(cancelled),
        "tasks_in_progress": len(in_progress),
        "progress": round(len(done_cards) / len(cards) * 100, 1) if cards else 0,
        "avg_completeness": round(sum(completeness_vals) / len(completeness_vals), 1) if completeness_vals else None,
        "avg_drift": round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None,
        "avg_cycle_hours": round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None,
        "cards": card_metrics,
        "evaluations_total": len(evaluations),
        "evaluations_non_stale": len(non_stale),
        "approvals": len(approvals),
        "avg_eval_score": round(sum(e.get("overall_score", 0) for e in approvals) / len(approvals), 1) if approvals else None,
        "scoped_scenarios": scoped_scenarios,
        "scenario_coverage": round(len(passed) / len(scoped_scenarios) * 100, 1) if scoped_scenarios else 0,
        "comparison": comparison,
        "created_at": sprint.created_at.isoformat() if sprint.created_at else None,
        "updated_at": sprint.updated_at.isoformat() if sprint.updated_at else None,
    }
