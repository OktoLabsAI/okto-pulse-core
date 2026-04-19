"""Analytics service layer — pure aggregation functions shared by REST + MCP.

Ideação #9 (aa9e6cee): eliminar duplicação entre api/analytics.py e
mcp/server.py extraindo agregadores para funções puras. Ambos os call-sites
delegam a este módulo, garantindo paridade de contrato por construção.

Cada função é assíncrona (AsyncSession como primeiro argumento) e retorna
o mesmo shape que o endpoint REST correspondente — MCP converge para REST.

Migração incremental em commits separados (1 duplicação por commit) para
preservar bisectability.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    Card,
    CardStatus,
    Ideation,
    IdeationStatus,
    Refinement,
    RefinementStatus,
    Spec,
    SpecStatus,
    Sprint,
    SprintStatus,
)


# ---------------------------------------------------------------------------
# Normalization helpers (duplicados em api/analytics.py — manter sincronizados
# via re-export para backwards compat; nova lógica vai direto daqui)
# ---------------------------------------------------------------------------


def resolve_linked_criteria_to_indices(
    linked_list: list | None, ac_list: list[str]
) -> set[int]:
    """Normalize heterogeneous `linked_criteria` entries into a deduplicated set
    of 0-based AC indices.

    Scenarios in the wild store entries in three shapes: `int`, numeric `str`
    (e.g. ``"3"``), or full AC text. Without normalization, a set over raw
    values double-counts the same AC when multiple shapes coexist.

    Out-of-range indices and unmatched texts are dropped silently so the
    invariant `covered_ac <= total_ac` holds even for degenerate inputs.
    """
    if not linked_list or not ac_list:
        return set()
    valid_range = range(len(ac_list))
    resolved: set[int] = set()
    for entry in linked_list:
        if isinstance(entry, bool):
            continue
        if isinstance(entry, int):
            if entry in valid_range:
                resolved.add(entry)
            continue
        if isinstance(entry, str):
            stripped = entry.strip()
            if not stripped:
                continue
            try:
                idx = int(stripped)
            except ValueError:
                pass
            else:
                if idx in valid_range:
                    resolved.add(idx)
                continue
            for i, ac in enumerate(ac_list):
                if stripped == ac or ac.startswith(stripped) or stripped.startswith(ac):
                    resolved.add(i)
                    break
    return resolved


def resolve_linked_fr_indices(linked_refs: list, frs: list[str]) -> set[int]:
    """Resolve linked_requirements (indices or FR text) to FR indices."""
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
        for i, fr_text in enumerate(frs):
            if ref_str in fr_text or fr_text in ref_str:
                indices.add(i)
                break
    return indices


# ---------------------------------------------------------------------------
# D-1 · Coverage per spec
# ---------------------------------------------------------------------------


def _coverage_row_for_spec(spec: Spec) -> dict:
    """Build a single coverage row for one Spec ORM row.

    Output shape matches REST /analytics/coverage exactly. MCP converges to
    this shape (previously omitted BR/contract counts + FR coverage %).
    """
    ac_list = spec.acceptance_criteria or []
    total_ac = len(ac_list)

    scenarios = spec.test_scenarios or []
    covered_ac_indices: set[int] = set()
    status_counts: dict[str, int] = {}
    for ts in scenarios:
        if isinstance(ts, dict):
            covered_ac_indices |= resolve_linked_criteria_to_indices(
                ts.get("linked_criteria"), ac_list
            )
            ts_status = ts.get("status", "unknown")
            status_counts[ts_status] = status_counts.get(ts_status, 0) + 1
    covered_ac_count = min(len(covered_ac_indices), total_ac)

    brs = spec.business_rules or []
    contracts = spec.api_contracts or []
    frs = spec.functional_requirements or []
    total_frs = len(frs)

    fr_indices_with_rules: set[int] = set()
    for br in brs:
        if isinstance(br, dict):
            fr_indices_with_rules |= resolve_linked_fr_indices(
                br.get("linked_requirements") or [], frs
            )
    fr_with_rules_pct = (
        round(len(fr_indices_with_rules) / total_frs * 100, 1) if total_frs > 0 else 0
    )

    fr_indices_with_contracts: set[int] = set()
    for ct in contracts:
        if isinstance(ct, dict):
            fr_indices_with_contracts |= resolve_linked_fr_indices(
                ct.get("linked_requirements") or [], frs
            )
    fr_with_contracts_pct = (
        round(len(fr_indices_with_contracts) / total_frs * 100, 1) if total_frs > 0 else 0
    )

    return {
        "spec_id": spec.id,
        "title": spec.title,
        "total_ac": total_ac,
        "covered_ac": covered_ac_count,
        "total_scenarios": len(scenarios),
        "scenario_status_counts": status_counts,
        "business_rules_count": len(brs),
        "api_contracts_count": len(contracts),
        "fr_with_rules_pct": fr_with_rules_pct,
        "fr_with_contracts_pct": fr_with_contracts_pct,
    }


async def compute_coverage(
    db: AsyncSession,
    board_id: str,
    *,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    include_archived: bool = False,
) -> list[dict]:
    """Compute test coverage per spec for a board.

    Returns a list of coverage rows (one per spec), each with:
      - spec_id, title, total_ac, covered_ac, total_scenarios,
        scenario_status_counts, business_rules_count, api_contracts_count,
        fr_with_rules_pct, fr_with_contracts_pct.

    Parameters
    ----------
    include_archived : bool
        REST path sets this False (only non-archived). MCP path historically
        did not filter — set True to replicate legacy MCP behavior. Default
        matches REST (stricter).
    """
    spec_q = select(Spec).where(Spec.board_id == board_id)
    if not include_archived:
        spec_q = spec_q.where(Spec.archived.is_(False))
    if dt_from:
        spec_q = spec_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_q = spec_q.where(Spec.created_at <= dt_to)
    specs = list((await db.execute(spec_q)).scalars().all())
    return [_coverage_row_for_spec(s) for s in specs]


# ---------------------------------------------------------------------------
# D-4 · Funnel metrics
# ---------------------------------------------------------------------------


def _is_test_card(card) -> bool:
    ct = getattr(card, "card_type", None)
    return ct is not None and str(ct).endswith("test")


def _is_bug_card(card) -> bool:
    ct = getattr(card, "card_type", None)
    return ct is not None and str(ct).endswith("bug")


def _is_normal_card(card) -> bool:
    ct = getattr(card, "card_type", None)
    if ct is None:
        return not _is_test_card(card)
    return str(ct).endswith("normal")


def _status_breakdown(items: list, enum_cls) -> dict[str, int]:
    """Count items per status, aware of all enum values (zeros preserved)."""
    out = {s.value: 0 for s in enum_cls}
    for it in items:
        st = it.status.value if hasattr(it.status, "value") else str(it.status)
        out[st] = out.get(st, 0) + 1
    return out


async def compute_funnel(
    db: AsyncSession,
    board_id: str,
    *,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    include_archived: bool = False,
) -> dict:
    """Compute the full funnel for a board.

    Returns the same rich shape as REST `/boards/{id}/analytics/funnel`:
      - Per-level counts: ideations, refinements, specs, sprints, cards.
      - Done counts: done, ideations_done, specs_done, refinements_done.
      - Card type breakdown: cards_impl, cards_test, cards_bug.
      - BR/Contract aggregation: rules_count, contracts_count,
        specs_with_rules, specs_with_contracts.
      - Status breakdowns: spec_status_breakdown, card_status_breakdown,
        sprint_status_breakdown.
      - Bug metrics: bugs_total, bugs_open, bugs_by_severity.
      - Cycle time: avg_cycle_hours + cycle_time_by_phase{ideation, refinement,
        spec, sprint, card}.

    MCP previously returned only 6 keys (ideations/refinements/specs/cards/done)
    — migration unifies to the full shape.
    """
    counts: dict = {}

    # Per-level counts
    archived_filter = (lambda m: m.archived.is_(False)) if not include_archived else (lambda m: None)
    for model, key in [
        (Ideation, "ideations"),
        (Refinement, "refinements"),
        (Spec, "specs"),
        (Sprint, "sprints"),
        (Card, "cards"),
    ]:
        q = select(func.count(model.id)).where(model.board_id == board_id)
        if not include_archived:
            q = q.where(model.archived.is_(False))
        if dt_from:
            q = q.where(model.created_at >= dt_from)
        if dt_to:
            q = q.where(model.created_at <= dt_to)
        counts[key] = (await db.execute(q)).scalar() or 0

    # Done cards
    done_q = select(func.count(Card.id)).where(
        Card.board_id == board_id,
        Card.status == CardStatus.DONE,
    )
    if not include_archived:
        done_q = done_q.where(Card.archived.is_(False))
    if dt_from:
        done_q = done_q.where(Card.created_at >= dt_from)
    if dt_to:
        done_q = done_q.where(Card.created_at <= dt_to)
    counts["done"] = (await db.execute(done_q)).scalar() or 0

    # Lifecycle done counts
    ideations_done_q = select(func.count(Ideation.id)).where(
        Ideation.board_id == board_id,
        Ideation.status == IdeationStatus.DONE,
    )
    specs_done_q = select(func.count(Spec.id)).where(
        Spec.board_id == board_id,
        Spec.status == SpecStatus.DONE,
    )
    if not include_archived:
        ideations_done_q = ideations_done_q.where(Ideation.archived.is_(False))
        specs_done_q = specs_done_q.where(Spec.archived.is_(False))
    if dt_from:
        ideations_done_q = ideations_done_q.where(Ideation.created_at >= dt_from)
        specs_done_q = specs_done_q.where(Spec.created_at >= dt_from)
    if dt_to:
        ideations_done_q = ideations_done_q.where(Ideation.created_at <= dt_to)
        specs_done_q = specs_done_q.where(Spec.created_at <= dt_to)
    counts["ideations_done"] = (await db.execute(ideations_done_q)).scalar() or 0
    counts["specs_done"] = (await db.execute(specs_done_q)).scalar() or 0

    # Card types (Python-side on JSON column)
    all_cards_q = select(Card).where(Card.board_id == board_id)
    if not include_archived:
        all_cards_q = all_cards_q.where(Card.archived.is_(False))
    if dt_from:
        all_cards_q = all_cards_q.where(Card.created_at >= dt_from)
    if dt_to:
        all_cards_q = all_cards_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(all_cards_q)).scalars().all())
    counts["cards_impl"] = sum(1 for c in all_cards if _is_normal_card(c))
    counts["cards_test"] = sum(1 for c in all_cards if _is_test_card(c))
    counts["cards_bug"] = sum(1 for c in all_cards if _is_bug_card(c))

    # Specs (para BR/Contract + breakdown)
    spec_objs_q = select(Spec).where(Spec.board_id == board_id)
    if not include_archived:
        spec_objs_q = spec_objs_q.where(Spec.archived.is_(False))
    if dt_from:
        spec_objs_q = spec_objs_q.where(Spec.created_at >= dt_from)
    if dt_to:
        spec_objs_q = spec_objs_q.where(Spec.created_at <= dt_to)
    spec_objs = list((await db.execute(spec_objs_q)).scalars().all())

    counts["rules_count"] = sum(len(s.business_rules or []) for s in spec_objs)
    counts["contracts_count"] = sum(len(s.api_contracts or []) for s in spec_objs)
    counts["specs_with_rules"] = sum(
        1 for s in spec_objs if s.business_rules and len(s.business_rules) > 0
    )
    counts["specs_with_contracts"] = sum(
        1 for s in spec_objs if s.api_contracts and len(s.api_contracts) > 0
    )

    counts["spec_status_breakdown"] = _status_breakdown(spec_objs, SpecStatus)
    counts["card_status_breakdown"] = _status_breakdown(all_cards, CardStatus)

    # Sprints
    sprint_objs_q = select(Sprint).where(Sprint.board_id == board_id)
    if not include_archived:
        sprint_objs_q = sprint_objs_q.where(Sprint.archived.is_(False))
    if dt_from:
        sprint_objs_q = sprint_objs_q.where(Sprint.created_at >= dt_from)
    if dt_to:
        sprint_objs_q = sprint_objs_q.where(Sprint.created_at <= dt_to)
    sprint_objs = list((await db.execute(sprint_objs_q)).scalars().all())
    counts["sprint_status_breakdown"] = _status_breakdown(sprint_objs, SprintStatus)

    # Bug metrics
    bug_cards = [c for c in all_cards if _is_bug_card(c)]
    counts["bugs_total"] = len(bug_cards)
    counts["bugs_open"] = sum(
        1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED)
    )
    counts["bugs_by_severity"] = {
        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
    }

    # Avg cycle (cards done)
    done_cards_board = [c for c in all_cards if c.status == CardStatus.DONE]
    cycle_times_board: list[float] = []
    for c in done_cards_board:
        if c.created_at and c.updated_at:
            cycle_times_board.append(
                (c.updated_at - c.created_at).total_seconds() / 3600.0
            )
    counts["avg_cycle_hours"] = (
        round(sum(cycle_times_board) / len(cycle_times_board), 1)
        if cycle_times_board
        else None
    )

    # Ideations/Refinements para cycle_time_by_phase
    board_ideations_q = select(Ideation).where(Ideation.board_id == board_id)
    board_refinements_q = select(Refinement).where(Refinement.board_id == board_id)
    if not include_archived:
        board_ideations_q = board_ideations_q.where(Ideation.archived.is_(False))
        board_refinements_q = board_refinements_q.where(Refinement.archived.is_(False))
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
# D-5 · Velocity (weekly + daily granularities)
# ---------------------------------------------------------------------------


async def compute_velocity(
    db: AsyncSession,
    board_id: str,
    *,
    granularity: str = "week",
    weeks: int = 12,
    days: int = 30,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    include_archived: bool = False,
) -> list[dict]:
    """Compute velocity buckets (week|day) with lifecycle overlays.

    Returns list of dicts, one per bucket, keyed by ``week`` or ``day``:
      - impl / test / bug — cards done in the bucket (by card_type)
      - validation_bounce — failed task validations in the bucket
      - spec_done — spec_moved events where new_status == 'done'
      - sprint_done — sprint_moved events where new_status == 'closed'

    MCP previously hardcoded weekly 12 buckets and only returned impl/test.
    Service delega para os builders existentes em api/analytics.py.
    """
    if granularity not in ("week", "day"):
        raise ValueError(f"granularity must be 'week' or 'day', got {granularity!r}")

    # Lazy-import helpers já testados em api/analytics.py
    from okto_pulse.core.api.analytics import (
        _build_velocity_buckets,
        _load_lifecycle_moves,
    )

    all_q = select(Card).where(Card.board_id == board_id)
    if not include_archived:
        all_q = all_q.where(Card.archived.is_(False))
    if dt_from:
        all_q = all_q.where(Card.created_at >= dt_from)
    if dt_to:
        all_q = all_q.where(Card.created_at <= dt_to)
    all_cards = list((await db.execute(all_q)).scalars().all())
    done_cards = [c for c in all_cards if c.status == CardStatus.DONE]

    spec_moves = await _load_lifecycle_moves(db, board_id, "spec_moved")
    sprint_moves = await _load_lifecycle_moves(db, board_id, "sprint_moved")

    periods = days if granularity == "day" else weeks
    return _build_velocity_buckets(
        done_cards=done_cards,
        all_cards=all_cards,
        periods=periods,
        granularity=granularity,
        spec_moves=spec_moves,
        sprint_moves=sprint_moves,
    )
