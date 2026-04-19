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

from datetime import datetime as _dt, timezone as _tz, timedelta as _td

from okto_pulse.core.models.db import (
    Card,
    CardDependency,
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


# ---------------------------------------------------------------------------
# D-7 · Spec coverage summary (per-spec detailed breakdown)
# ---------------------------------------------------------------------------


def spec_coverage_summary(
    spec, *, scenarios=None, rules=None, contracts=None, trs=None
) -> dict:
    """Compute coverage stats for a single spec — used by validation gate + UI.

    Move canônico do antigo `mcp/server.py::_spec_coverage`. Ambos REST e MCP
    passam a consumir daqui.

    Override args (scenarios/rules/contracts/trs) suportam chamadas in-flight
    onde o spec ainda não foi persistido com a nova coleção.
    """
    acs = spec.acceptance_criteria or []
    frs = spec.functional_requirements or []
    _ts = scenarios if scenarios is not None else (spec.test_scenarios or [])
    _brs = rules if rules is not None else (spec.business_rules or [])
    _contracts = contracts if contracts is not None else (spec.api_contracts or [])
    _trs = trs if trs is not None else (spec.technical_requirements or [])

    covered_ac = set()
    for ts in _ts:
        for val in (ts.get("linked_criteria") or []):
            if isinstance(val, int):
                covered_ac.add(val)
            elif isinstance(val, str):
                for i, ac in enumerate(acs):
                    if val == ac or ac.startswith(val) or val.startswith(ac):
                        covered_ac.add(i)
                        break
    ac_total = len(acs)
    ac_covered = len(covered_ac & set(range(ac_total)))

    covered_fr = set()
    for br in _brs:
        for val in (br.get("linked_requirements") or []):
            if isinstance(val, int):
                covered_fr.add(val)
            elif isinstance(val, str):
                for i, fr in enumerate(frs):
                    if val == fr or fr.startswith(val) or val.startswith(fr):
                        covered_fr.add(i)
                        break
    fr_total = len(frs)
    fr_covered = len(covered_fr & set(range(fr_total)))

    ts_total = len(_ts)
    ts_linked = sum(1 for ts in _ts if ts.get("linked_task_ids"))

    br_total = len(_brs)
    br_linked = sum(1 for br in _brs if br.get("linked_task_ids"))

    c_total = len(_contracts)
    c_linked = sum(1 for c in _contracts if c.get("linked_task_ids"))

    struct_trs = [t for t in _trs if isinstance(t, dict)]
    tr_total = len(struct_trs)
    tr_linked = sum(1 for t in struct_trs if t.get("linked_task_ids"))

    def _pct(n, d):
        return round((n / d * 100) if d > 0 else 100, 1)

    return {
        "ac_coverage_pct": _pct(ac_covered, ac_total),
        "ac_covered": ac_covered,
        "ac_total": ac_total,
        "ac_uncovered_indices": sorted(set(range(ac_total)) - covered_ac),
        "fr_coverage_pct": _pct(fr_covered, fr_total),
        "fr_covered": fr_covered,
        "fr_total": fr_total,
        "fr_uncovered_indices": sorted(set(range(fr_total)) - covered_fr),
        "scenario_task_linkage_pct": _pct(ts_linked, ts_total),
        "scenarios_linked": ts_linked,
        "scenarios_total": ts_total,
        "br_task_linkage_pct": _pct(br_linked, br_total),
        "brs_linked": br_linked,
        "brs_total": br_total,
        "contract_task_linkage_pct": _pct(c_linked, c_total),
        "contracts_linked": c_linked,
        "contracts_total": c_total,
        "tr_task_linkage_pct": _pct(tr_linked, tr_total),
        "trs_linked": tr_linked,
        "trs_total": tr_total,
        "skip_test_coverage": getattr(spec, "skip_test_coverage", False),
        "skip_rules_coverage": getattr(spec, "skip_rules_coverage", False),
    }


# ---------------------------------------------------------------------------
# D-8 · Decisions filtering / stats
# ---------------------------------------------------------------------------


def filter_decisions_by_status(
    decisions: list | None, *, include_superseded: bool = False
) -> list:
    """Return only `status="active"` decisions by default; all when flag set.

    Legacy rows sem campo status são tratadas como active (não são dropadas
    silenciosamente).
    """
    if not decisions:
        return []
    if include_superseded:
        return list(decisions)
    kept = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        status_val = d.get("status")
        if status_val is None or status_val == "active":
            kept.append(d)
    return kept


# ---------------------------------------------------------------------------
# D-6 · Blockers triage
# ---------------------------------------------------------------------------


async def compute_blockers(
    db: AsyncSession,
    board_id: str,
    *,
    stale_hours: int = 72,
    filter_type: str | None = None,
) -> dict:
    """Triage blockers across a board. Returns payload compatible with both
    REST (GET /analytics/blockers) and MCP (list_blockers).

    Categories (non-overlapping per blocker entry):
    - dependency_blocked — card is active but has unfinished dependencies
    - on_hold — card explicitly paused (status=on_hold)
    - stale — card in active state and stuck beyond stale_hours
    - spec_pending_validation — spec approved without approve-evaluation
    - spec_no_cards — spec validated/in_progress with zero linked cards
    - uncovered_scenario — scenario has no linked test card

    Returns::

        {
          board_id, summary: {<type>: count}, total,
          stale_hours_threshold, filter_type, blockers: [...]
        }
    """
    if stale_hours < 1:
        raise ValueError("stale_hours must be >= 1")

    now = _dt.now(_tz.utc)
    stale_cutoff = now - _td(hours=stale_hours)

    cards = list(
        (await db.execute(
            select(Card).where(Card.board_id == board_id, Card.archived.is_(False))
        )).scalars().all()
    )
    card_by_id = {c.id: c for c in cards}

    deps: list = []
    if cards:
        deps = list((await db.execute(
            select(CardDependency).where(CardDependency.card_id.in_([c.id for c in cards]))
        )).scalars().all())
    deps_by_card: dict[str, list[str]] = {}
    for d in deps:
        deps_by_card.setdefault(d.card_id, []).append(d.depends_on_id)

    blockers: list[dict] = []
    active_states = {
        CardStatus.NOT_STARTED,
        CardStatus.STARTED,
        CardStatus.IN_PROGRESS,
        CardStatus.VALIDATION,
        CardStatus.ON_HOLD,
    }
    stale_states = {CardStatus.STARTED, CardStatus.IN_PROGRESS, CardStatus.VALIDATION}

    for c in cards:
        if c.status in active_states:
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

        if c.status == CardStatus.ON_HOLD:
            blockers.append({
                "type": "on_hold",
                "card_id": c.id,
                "card_title": c.title,
                "card_status": c.status.value,
                "reason": "Card explicitly paused via status=on_hold",
                "evidence": {"updated_at": c.updated_at.isoformat() if c.updated_at else None},
            })

        if c.status in stale_states and c.updated_at:
            upd = c.updated_at
            if upd.tzinfo is None:
                upd = upd.replace(tzinfo=_tz.utc)
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

    specs = list((await db.execute(
        select(Spec).where(Spec.board_id == board_id, Spec.archived.is_(False))
    )).scalars().all())
    spec_card_counts: dict[str, int] = {}
    for c in cards:
        if c.spec_id:
            spec_card_counts[c.spec_id] = spec_card_counts.get(c.spec_id, 0) + 1

    for s in specs:
        if s.status == SpecStatus.APPROVED:
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
        if s.status in (SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS):
            if spec_card_counts.get(s.id, 0) == 0:
                blockers.append({
                    "type": "spec_no_cards",
                    "spec_id": s.id,
                    "spec_title": s.title,
                    "reason": "Spec has zero linked cards — implementation hasn't started",
                    "evidence": {"status": s.status.value},
                })

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

    if filter_type:
        blockers = [b for b in blockers if b["type"] == filter_type]

    summary: dict[str, int] = {}
    for b in blockers:
        summary[b["type"]] = summary.get(b["type"], 0) + 1

    return {
        "board_id": board_id,
        "summary": summary,
        "total": len(blockers),
        "stale_hours_threshold": stale_hours,
        "filter_type": filter_type or None,
        "blockers": blockers,
    }


# ---------------------------------------------------------------------------
# D-2 / D-3 · Validation gate aggregators (Task + Spec)
# ---------------------------------------------------------------------------


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 1) if values else None


def classify_spec_violation(violations: list[str], recommendation: str) -> list[str]:
    """Map a spec validation's threshold_violations + recommendation to reason
    buckets: {completeness_below, assertiveness_below, ambiguity_above,
    reject_recommendation}. A record may hit multiple reasons (D3 multi-count).
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


def classify_task_violation(violations: list[str], recommendation: str) -> list[str]:
    """Map a task validation's threshold_violations + recommendation to reason
    buckets: {confidence_below, completeness_below, drift_above,
    reject_recommendation}.
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


def aggregate_spec_validation_gate(specs: list) -> dict:
    """Aggregate Spec Validation Gate metrics across a collection of specs.

    Walks ALL spec.validations records (D4 all-history). A single failed record
    can contribute to multiple rejection buckets (D3 multi-count).

    Returns::

        {
          total_submitted, total_success, total_failed,
          success_rate, avg_attempts_per_spec,
          avg_scores: {completeness, assertiveness, ambiguity},
          rejection_reasons: {
            completeness_below, assertiveness_below,
            ambiguity_above, reject_recommendation,
          },
          specs_with_validation,
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
                for r in classify_spec_violation(
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


def aggregate_task_validation_gate(cards: list) -> dict:
    """Aggregate Task Validation Gate metrics across a collection of cards.

    Walks ALL card.validations records. Supports both legacy naming
    (``estimated_completeness``/``estimated_drift``) and new naming
    (``completeness``/``drift``).

    Returns shape mirrors :func:`aggregate_spec_validation_gate` but for
    confidence/completeness/drift dimensions, plus ``first_pass_rate`` and
    ``avg_attempts_per_card``.
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
    first_pass_count = 0

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
                for r in classify_task_violation(
                    v.get("threshold_violations") or [],
                    v.get("recommendation", ""),
                ):
                    reasons[r] = reasons.get(r, 0) + 1
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


def decisions_stats(decisions: list | None) -> dict:
    """Breakdown de decisions por status (total, active, superseded, revoked, other)."""
    out = {"total": 0, "active": 0, "superseded": 0, "revoked": 0, "other": 0}
    for d in decisions or []:
        if not isinstance(d, dict):
            continue
        out["total"] += 1
        status_val = d.get("status") or "active"
        if status_val == "active":
            out["active"] += 1
        elif status_val == "superseded":
            out["superseded"] += 1
        elif status_val == "revoked":
            out["revoked"] += 1
        else:
            out["other"] += 1
    return out
