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

from datetime import datetime, timedelta, timezone
from datetime import datetime as _dt
from datetime import timedelta as _td
from datetime import timezone as _tz
from typing import Any

from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
    SprintLaneType,
    SprintStatus,
    StoryStatus,
)
from okto_pulse.core.ports.analytics_read import (
    AnalyticsFilter,
    AnalyticsQuery,
    get_analytics_read_port,
)
from okto_pulse.core.services.analytics_contract import (
    classify_analytics_card,
    partition_analytics_cards,
)
from okto_pulse.core.services.coverage_calculator import (
    spec_saturation_envelope_from_coverage,
)


def _af(field: str, operator: str, value: Any = None) -> AnalyticsFilter:
    return AnalyticsFilter(field=field, operator=operator, value=value)  # type: ignore[arg-type]


async def _analytics_list(
    db: Any,
    entity: str,
    *,
    filters: tuple[AnalyticsFilter, ...] = (),
    search: str = "",
    search_fields: tuple[str, ...] = (),
    order_by: str | None = None,
    descending: bool = False,
    offset: int = 0,
    limit: int | None = None,
) -> list[Any]:
    rows = await get_analytics_read_port().list(
        db,
        AnalyticsQuery(
            entity=entity,
            filters=filters,
            search=search,
            search_fields=search_fields,
            order_by=order_by,
            descending=descending,
            offset=offset,
            limit=limit,
        ),
    )
    return list(rows)


async def _analytics_count(
    db: Any,
    entity: str,
    *,
    filters: tuple[AnalyticsFilter, ...] = (),
    search: str = "",
    search_fields: tuple[str, ...] = (),
) -> int:
    return await get_analytics_read_port().count(
        db,
        AnalyticsQuery(
            entity=entity,
            filters=filters,
            search=search,
            search_fields=search_fields,
        ),
    )


def _artifact_filters(
    board_id: str,
    *,
    include_archived: bool,
    dt_from: datetime | None,
    dt_to: datetime | None,
    extra: tuple[AnalyticsFilter, ...] = (),
) -> tuple[AnalyticsFilter, ...]:
    filters = [_af("board_id", "eq", board_id), *extra]
    if not include_archived:
        filters.append(_af("archived", "is_false"))
    if dt_from:
        filters.append(_af("created_at", "gte", dt_from))
    if dt_to:
        filters.append(_af("created_at", "lt", dt_to))
    return tuple(filters)


async def board_is_owned_by(db: Any, board_id: str, user_id: str) -> bool:
    """True when ``board_id`` exists and is owned by ``user_id``. Transport-free
    reader for the analytics board-access guard (spec R01A REST-FU2a) — the same
    strict owner-only query the inline ``_ensure_board`` in ``api/analytics.py``
    runs for the not-yet-migrated endpoints."""
    return bool(
        await _analytics_count(
            db,
            "board",
            filters=(
                _af("id", "eq", board_id),
                _af("owner_id", "eq", user_id),
            ),
        )
    )


# ---------------------------------------------------------------------------
# Normalization helpers (duplicados em api/analytics.py — manter sincronizados
# via re-export para backwards compat; nova lógica vai direto daqui)
# ---------------------------------------------------------------------------


def _structured_ref_text(item) -> str:
    if isinstance(item, dict):
        return str(item.get("text") or item.get("title") or item.get("description") or "")
    return str(item)


def _structured_ref_id(item) -> str | None:
    if isinstance(item, dict):
        raw = item.get("id")
        return str(raw) if raw not in (None, "") else None
    return None


def _utc_datetime(value) -> _dt | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = _dt.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if not isinstance(value, _dt):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=_tz.utc)
    return value.astimezone(_tz.utc)


def _hours_between(start, end) -> float | None:
    start_dt = _utc_datetime(start)
    end_dt = _utc_datetime(end)
    if not start_dt or not end_dt:
        return None
    return (end_dt - start_dt).total_seconds() / 3600.0


def resolve_linked_criteria_to_indices(
    linked_list: list | None, ac_list: list
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
                ac_text = _structured_ref_text(ac)
                ac_id = _structured_ref_id(ac)
                if (
                    stripped == ac_id
                    or stripped == ac_text
                    or (ac_text and ac_text.startswith(stripped))
                    or (ac_text and stripped.startswith(ac_text))
                ):
                    resolved.add(i)
                    break
    return resolved


def _resolve_one_linked_criterion_to_id(entry, ac_list: list) -> str | None:
    """Resolve ONE ``linked_criteria`` token to a canonical ac_id (write-path, STRICT).

    Accepts a 0-based index (``int`` or numeric ``str``), an exact ``ac_id``, or
    the exact AC text. Unlike :func:`resolve_linked_criteria_to_indices`
    (read-path), this does NO prefix matching — write resolution must be
    deterministic. Returns the ac_id (or the AC text when the AC is legacy and
    has no id), else ``None``.
    """
    # bool is a subclass of int — reject explicitly so True/False never index.
    if isinstance(entry, bool):
        return None

    idx: int | None = None
    if isinstance(entry, int):
        idx = entry
    elif isinstance(entry, str):
        stripped = entry.strip()
        if stripped.lstrip("-").isdigit():
            idx = int(stripped)
    if idx is not None:
        if 0 <= idx < len(ac_list):
            ac = ac_list[idx]
            return _structured_ref_id(ac) or _structured_ref_text(ac)
        return None

    token = str(entry).strip()
    if not token:
        return None
    for ac in ac_list:
        ac_id = _structured_ref_id(ac)
        ac_text = _structured_ref_text(ac)
        if token == ac_id or token == ac_text:
            return ac_id or ac_text
    return None


def resolve_linked_criteria_to_ids(
    linked_list: list | None, ac_list: list
) -> tuple[list[str], list[str]]:
    """Write-path resolver for ``linked_criteria``. Mirrors the read resolver but
    projects to canonical ids instead of indices.

    Returns ``(resolved_ids, unresolved_tokens)``:

    * ``resolved_ids`` — canonical ``ac_id`` values (or the AC text for legacy
      ACs without an id), deduplicated while preserving first-seen order.
    * ``unresolved_tokens`` — tokens that did not resolve. These are NEVER
      dropped silently, so the caller can fail closed.

    Never emits a dict. The tolerant read resolver
    :func:`resolve_linked_criteria_to_indices` is intentionally left untouched —
    its prefix/index leniency must not leak into write persistence.
    """
    resolved: list[str] = []
    unresolved: list[str] = []
    seen: set[str] = set()
    for entry in linked_list or []:
        rid = _resolve_one_linked_criterion_to_id(entry, ac_list)
        if rid is None:
            unresolved.append(str(entry))
        elif rid not in seen:
            seen.add(rid)
            resolved.append(rid)
    return resolved, unresolved


def resolve_linked_requirements_to_ids(
    linked_list: list | None, fr_list: list
) -> tuple[list[str], list[str]]:
    """Write-path resolver for ``linked_requirements`` — the FR analog of
    :func:`resolve_linked_criteria_to_ids` (spec 9d66847f).

    Returns ``(resolved_ids, unresolved_tokens)``: canonical ``fr_id`` values
    (or the FR text for legacy FRs without an id), deduplicated in first-seen
    order, plus the tokens that did not resolve (never dropped silently). Token
    resolution is identical to the AC write-path — strict, exact match, no
    prefix leniency. The tolerant read resolver
    :func:`resolve_linked_fr_indices` is intentionally left untouched.
    """
    resolved: list[str] = []
    unresolved: list[str] = []
    seen: set[str] = set()
    for entry in linked_list or []:
        rid = _resolve_one_linked_criterion_to_id(entry, fr_list)
        if rid is None:
            unresolved.append(str(entry))
        elif rid not in seen:
            seen.add(rid)
            resolved.append(rid)
    return resolved, unresolved


def resolve_linked_fr_indices(linked_refs: list, frs: list) -> set[int]:
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
        for i, fr in enumerate(frs):
            fr_text = _structured_ref_text(fr)
            fr_id = _structured_ref_id(fr)
            if ref_str == fr_id or (fr_text and (ref_str in fr_text or fr_text in ref_str)):
                indices.add(i)
                break
    return indices


# ---------------------------------------------------------------------------
# D-1 · Coverage per spec
# ---------------------------------------------------------------------------


def _coverage_row_for_spec(spec: Any, cards: list | None = None) -> dict:
    """Build a single coverage row for one Spec ORM row.

    Output shape matches REST /analytics/coverage exactly. MCP converges to
    this shape (previously omitted BR/contract counts + FR coverage %).

    Spec 233eaad3: extends shape with 4 fields (decisions_coverage_pct,
    decisions_total, tr_task_linkage_pct, trs_total) sourced from
    ``spec_coverage_summary``. Backward compatible — pre-existing fields
    preserved bit-for-bit; ``cards`` defaults to None.
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

    # FR coverage is semantic: a Functional Requirement is covered only when a
    # Business Rule references it. FR.linked_task_ids is direct task traceability
    # and intentionally does not satisfy the FR->BR coverage gate.
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

    cov = spec_coverage_summary(spec, cards=cards)

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
        # Bug 6f152627: AC/FR coverage explícitos no nível 2 (eram inferidos de
        # covered_ac/total_ac e fr_with_rules_pct, com labels confusos no UI).
        "ac_coverage_pct": cov["ac_coverage_pct"],
        "fr_coverage_pct": cov["fr_coverage_pct"],
        "decisions_coverage_pct": cov["decisions_coverage_pct"],
        "decisions_total": cov["decisions_total"],
        # Bug 42e78332: decisions parity with IR/OR (linked + uncovered_ids + skip).
        "decisions_linked": cov["decisions_linked"],
        "decisions_uncovered_ids": cov["decisions_uncovered_ids"],
        "skip_decisions_coverage": cov["skip_decisions_coverage"],
        "tr_task_linkage_pct": cov["tr_task_linkage_pct"],
        "trs_total": cov["trs_total"],
        "irs_linked": cov["irs_linked"],
        "irs_total": cov["irs_total"],
        "ir_task_linkage_pct": cov["ir_task_linkage_pct"],
        "irs_uncovered_ids": cov["irs_uncovered_ids"],
        "skip_ir_coverage": cov["skip_ir_coverage"],
        "ors_linked": cov["ors_linked"],
        "ors_total": cov["ors_total"],
        "or_task_linkage_pct": cov["or_task_linkage_pct"],
        "ors_uncovered_ids": cov["ors_uncovered_ids"],
        "skip_or_coverage": cov["skip_or_coverage"],
    }


async def compute_coverage(
    db: Any,
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
        fr_with_rules_pct, fr_with_contracts_pct,
      - decisions_coverage_pct, decisions_total, tr_task_linkage_pct, trs_total
        (spec 233eaad3 — sourced from ``spec_coverage_summary`` so cancelled
        cards are excluded from linkage counts).

    Parameters
    ----------
    include_archived : bool
        REST path sets this False (only non-archived). MCP path historically
        did not filter — set True to replicate legacy MCP behavior. Default
        matches REST (stricter).
    """
    from collections import defaultdict

    filters = _artifact_filters(
        board_id,
        include_archived=include_archived,
        dt_from=dt_from,
        dt_to=dt_to,
    )
    specs = await _analytics_list(db, "spec", filters=filters)

    # Spec 233eaad3: 1 query batch para cards do board, group by spec_id
    # — evita N+1 e permite que _coverage_row_for_spec aplique cancelled filter.
    card_filters = [_af("board_id", "eq", board_id)]
    if not include_archived:
        card_filters.append(_af("archived", "is_false"))
    all_cards = await _analytics_list(
        db,
        "card",
        filters=tuple(card_filters),
    )
    cards_by_spec: dict[str, list] = defaultdict(list)
    for c in all_cards:
        if c.spec_id:
            cards_by_spec[c.spec_id].append(c)

    return [_coverage_row_for_spec(s, cards=cards_by_spec.get(s.id, [])) for s in specs]


# ---------------------------------------------------------------------------
# D-4 · Funnel metrics
# ---------------------------------------------------------------------------


def _is_test_card(card) -> bool:
    return getattr(card, "card_type", None) == CardType.TEST


def _is_bug_card(card) -> bool:
    return getattr(card, "card_type", None) == CardType.BUG


def _is_normal_card(card) -> bool:
    return getattr(card, "card_type", None) == CardType.NORMAL


def _status_breakdown(items: list, enum_cls) -> dict[str, int]:
    """Count items per status, aware of all enum values (zeros preserved)."""
    out = {s.value: 0 for s in enum_cls}
    for it in items:
        st = it.status.value if hasattr(it.status, "value") else str(it.status)
        out[st] = out.get(st, 0) + 1
    return out


async def compute_funnel(
    db: Any,
    board_id: str,
    *,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
    include_archived: bool = False,
) -> dict:
    """Compute the full funnel for a board.

    Returns the same rich shape as REST `/boards/{id}/analytics/funnel`:
      - Per-level counts: stories, ideations, refinements, specs, sprints, cards.
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

    artifact_filters = _artifact_filters(
        board_id,
        include_archived=include_archived,
        dt_from=dt_from,
        dt_to=dt_to,
    )
    board_stories = await _analytics_list(db, "story", filters=artifact_filters)
    board_ideations = await _analytics_list(db, "ideation", filters=artifact_filters)
    board_refinements = await _analytics_list(
        db,
        "refinement",
        filters=artifact_filters,
    )
    spec_objs = await _analytics_list(db, "spec", filters=artifact_filters)
    sprint_objs = await _analytics_list(db, "sprint", filters=artifact_filters)
    all_cards = await _analytics_list(db, "card", filters=artifact_filters)
    counts.update(
        {
            "stories": len(board_stories),
            "ideations": len(board_ideations),
            "refinements": len(board_refinements),
            "specs": len(spec_objs),
            "sprints": len(sprint_objs),
            "cards": len(all_cards),
        }
    )

    counts["stories_converted"] = sum(
        1 for story in board_stories if story.status == StoryStatus.CONVERTED
    )
    counts["story_conversion_pct"] = (
        round((counts["stories_converted"] / counts["stories"]) * 100, 1)
        if counts.get("stories")
        else 0
    )
    counts["story_ideation_links"] = await _analytics_count(
        db,
        "story_ideation_link",
        filters=(_af("board_id", "eq", board_id),),
    )

    # Done cards
    counts["done"] = sum(1 for card in all_cards if card.status == CardStatus.DONE)

    # Lifecycle done counts
    counts["ideations_done"] = sum(
        1 for item in board_ideations if item.status == IdeationStatus.DONE
    )
    counts["specs_done"] = sum(
        1 for item in spec_objs if item.status == SpecStatus.DONE
    )

    # Card types (Python-side on JSON column)
    card_partitions = partition_analytics_cards(all_cards)
    counts["cards_impl"] = len(card_partitions["implementation"])
    counts["cards_test"] = len(card_partitions["test"])
    counts["cards_bug"] = len(card_partitions["bug"])

    # Specs (para BR/Contract + breakdown)
    counts["rules_count"] = sum(len(s.business_rules or []) for s in spec_objs)
    counts["contracts_count"] = sum(len(s.api_contracts or []) for s in spec_objs)
    counts["specs_with_rules"] = sum(
        1 for s in spec_objs if s.business_rules and len(s.business_rules) > 0
    )
    counts["specs_with_contracts"] = sum(
        1 for s in spec_objs if s.api_contracts and len(s.api_contracts) > 0
    )
    counts["specs_with_tests"] = sum(
        1 for s in spec_objs if s.test_scenarios and len(s.test_scenarios) > 0
    )

    counts["spec_status_breakdown"] = _status_breakdown(spec_objs, SpecStatus)
    counts["card_status_breakdown"] = _status_breakdown(all_cards, CardStatus)

    # Sprints
    counts["sprint_status_breakdown"] = _status_breakdown(sprint_objs, SprintStatus)

    # Bug metrics
    bug_cards = card_partitions["bug"]
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
        hours = _hours_between(c.created_at, c.updated_at)
        if hours is not None:
            cycle_times_board.append(hours)
    counts["avg_cycle_hours"] = (
        round(sum(cycle_times_board) / len(cycle_times_board), 1)
        if cycle_times_board
        else None
    )

    def _phase_ct(items, done_status_str: str) -> float | None:
        times = []
        for it in items:
            if str(it.status) == done_status_str and it.created_at and it.updated_at:
                hours = _hours_between(it.created_at, it.updated_at)
                if hours is not None:
                    times.append(hours)
        return round(sum(times) / len(times), 1) if times else None

    counts["cycle_time_by_phase"] = {
        "story": _phase_ct(board_stories, str(StoryStatus.CONVERTED)),
        "ideation": _phase_ct(board_ideations, str(IdeationStatus.DONE)),
        "refinement": _phase_ct(board_refinements, str(RefinementStatus.DONE)),
        "spec": _phase_ct(spec_objs, str(SpecStatus.DONE)),
        "sprint": _phase_ct(sprint_objs, str(SprintStatus.CLOSED)),
        "card": counts["avg_cycle_hours"],
    }
    counts["refinements_done"] = sum(
        1 for r in board_refinements if str(r.status) == str(RefinementStatus.DONE)
    )
    counts["story_status_breakdown"] = _status_breakdown(board_stories, StoryStatus)
    topics = await _analytics_list(
        db,
        "topic",
        filters=(_af("board_id", "eq", board_id),),
        order_by="name",
    )
    stories_per_topic: dict[str, int] = {}
    for story in board_stories:
        if story.topic_id:
            stories_per_topic[story.topic_id] = stories_per_topic.get(story.topic_id, 0) + 1
    counts["stories_by_topic"] = [
        {
            "topic_id": topic.id,
            "topic": topic.name,
            "stories": stories_per_topic.get(topic.id, 0),
        }
        for topic in topics
    ]

    return counts


# ---------------------------------------------------------------------------
# D-5 · Velocity (weekly + daily granularities)
# ---------------------------------------------------------------------------


async def compute_velocity(
    db: Any,
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

    all_cards = await _analytics_list(
        db,
        "card",
        filters=_artifact_filters(
            board_id,
            include_archived=include_archived,
            dt_from=dt_from,
            dt_to=dt_to,
        ),
    )
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


def _card_status_value(card: Any) -> str:
    status_attr = getattr(card, "status", None)
    return getattr(status_attr, "value", str(status_attr) if status_attr is not None else "")


def _card_coverage_counts(cards: list | None) -> dict[str, int]:
    raw_cards = list(cards or [])
    effective_cards = [
        card
        for card in raw_cards
        if not getattr(card, "archived", False) and _card_status_value(card) != "cancelled"
    ]
    return {
        "cards_total_raw": len(raw_cards),
        "cards_done_raw": sum(1 for card in raw_cards if _card_status_value(card) == "done"),
        "cards_total_effective": len(effective_cards),
        "cards_done_effective": sum(
            1 for card in effective_cards if _card_status_value(card) == "done"
        ),
    }


def spec_coverage_summary(
    spec, *, scenarios=None, rules=None, contracts=None, trs=None, decisions=None,
    integration_requirements=None, observability_requirements=None, cards=None,
) -> dict:
    """Compute coverage stats for a single spec — used by validation gate + UI.

    Move canônico do antigo `mcp/server.py::_spec_coverage`. Ambos REST e MCP
    passam a consumir daqui.

    Override args (scenarios/rules/contracts/trs/decisions) suportam chamadas
    in-flight onde o spec ainda não foi persistido com a nova coleção.

    Ideação #10 Fase 1: adiciona decisions_coverage_pct + decisions_uncovered_ids
    paralelo ao TR/BR/Contract linkage, paridade first-class.

    Spec 233eaad3 (Analytics cancelled-card filter): aceita ``cards`` opcional
    (lista de Card) e exclui IDs de cards em status ``cancelled`` do cálculo
    de linkage de TS/BR/Contract/TR/Decision via set difference. AC e FR
    coverage permanecem inalterados (são estruturais via TS.linked_criteria
    e BR.linked_requirements). Backward compat: cards=None mantém o
    comportamento histórico bit-a-bit.
    """
    acs = spec.acceptance_criteria or []
    frs = spec.functional_requirements or []
    _ts = scenarios if scenarios is not None else (spec.test_scenarios or [])
    _brs = rules if rules is not None else (spec.business_rules or [])
    _contracts = contracts if contracts is not None else (spec.api_contracts or [])
    _trs = trs if trs is not None else (spec.technical_requirements or [])
    _decisions = decisions if decisions is not None else (getattr(spec, "decisions", None) or [])
    _irs = (
        integration_requirements
        if integration_requirements is not None
        else (getattr(spec, "integration_requirements", None) or [])
    )
    _ors = (
        observability_requirements
        if observability_requirements is not None
        else (getattr(spec, "observability_requirements", None) or [])
    )

    card_counts = _card_coverage_counts(cards)
    cancelled_card_ids: set = set()
    if cards:
        for c in cards:
            if _card_status_value(c) == "cancelled" or getattr(c, "archived", False):
                cancelled_card_ids.add(c.id)

    covered_ac = set()
    for ts in _ts:
        if isinstance(ts, dict):
            covered_ac |= resolve_linked_criteria_to_indices(
                ts.get("linked_criteria"), acs
            )
    ac_total = len(acs)
    ac_covered = len(covered_ac & set(range(ac_total)))

    # FR coverage is driven by BR.linked_requirements. Direct FR task links are
    # traceability only and must not satisfy the FR->BR coverage gate.
    covered_fr = set()
    for br in _brs:
        if isinstance(br, dict):
            covered_fr |= resolve_linked_fr_indices(
                br.get("linked_requirements") or [], frs
            )
    fr_total = len(frs)
    fr_covered = len(covered_fr & set(range(fr_total)))

    ts_total = len(_ts)
    ts_linked = sum(
        1 for ts in _ts
        if (set(ts.get("linked_task_ids") or []) - cancelled_card_ids)
    )

    br_total = len(_brs)
    br_linked = sum(
        1 for br in _brs
        if (set(br.get("linked_task_ids") or []) - cancelled_card_ids)
    )

    # F13: exclude not_applicable (and superseded/revoked) contracts from coverage,
    # mirroring active_irs/active_ors. A not_applicable contract is a justified waiver.
    active_contracts = [
        c for c in _contracts
        if isinstance(c, dict) and c.get("status", "active") == "active"
    ]
    c_total = len(active_contracts)
    c_linked = sum(
        1 for c in active_contracts
        if (set(c.get("linked_task_ids") or []) - cancelled_card_ids)
    )

    struct_trs = [t for t in _trs if isinstance(t, dict)]
    tr_total = len(struct_trs)
    tr_linked = sum(
        1 for t in struct_trs
        if (set(t.get("linked_task_ids") or []) - cancelled_card_ids)
    )

    active_decisions = [
        d for d in _decisions
        if isinstance(d, dict) and d.get("status", "active") == "active"
    ]
    d_total = len(active_decisions)
    d_linked = sum(
        1 for d in active_decisions
        if (set(d.get("linked_task_ids") or []) - cancelled_card_ids)
    )
    d_uncovered_ids = [
        d.get("id") for d in active_decisions
        if not (set(d.get("linked_task_ids") or []) - cancelled_card_ids) and d.get("id")
    ]
    active_irs = [
        ir for ir in _irs
        if isinstance(ir, dict) and ir.get("status", "active") == "active"
    ]
    ir_total = len(active_irs)
    ir_linked = sum(
        1 for ir in active_irs
        if (set(ir.get("linked_task_ids") or []) - cancelled_card_ids)
    )
    ir_uncovered_ids = [
        ir.get("id") for ir in active_irs
        if not (set(ir.get("linked_task_ids") or []) - cancelled_card_ids) and ir.get("id")
    ]

    active_ors = [
        req for req in _ors
        if isinstance(req, dict) and req.get("status", "active") == "active"
    ]
    or_total = len(active_ors)
    or_linked = sum(
        1 for req in active_ors
        if (set(req.get("linked_task_ids") or []) - cancelled_card_ids)
    )
    or_uncovered_ids = [
        req.get("id") for req in active_ors
        if not (set(req.get("linked_task_ids") or []) - cancelled_card_ids) and req.get("id")
    ]

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
        "decisions_coverage_pct": _pct(d_linked, d_total),
        "decisions_linked": d_linked,
        "decisions_total": d_total,
        "decisions_uncovered_ids": d_uncovered_ids,
        "ir_task_linkage_pct": _pct(ir_linked, ir_total),
        "irs_linked": ir_linked,
        "irs_total": ir_total,
        "irs_uncovered_ids": ir_uncovered_ids,
        "or_task_linkage_pct": _pct(or_linked, or_total),
        "ors_linked": or_linked,
        "ors_total": or_total,
        "ors_uncovered_ids": or_uncovered_ids,
        # Raw/historical aliases are preserved for legacy clients; effective
        # counts mirror the gate surface by excluding cancelled/archived cards.
        "cards_total": card_counts["cards_total_raw"],
        "cards_done": card_counts["cards_done_raw"],
        **card_counts,
        "skip_test_coverage": getattr(spec, "skip_test_coverage", False),
        "skip_rules_coverage": getattr(spec, "skip_rules_coverage", False),
        "skip_decisions_coverage": getattr(spec, "skip_decisions_coverage", False),
        "skip_ir_coverage": getattr(spec, "skip_ir_coverage", False),
        "skip_or_coverage": getattr(spec, "skip_or_coverage", False),
    }


def spec_saturation_envelope(coverage: dict[str, Any]) -> dict[str, Any]:
    """Return the minimal saturation envelope: {pct, blocking[]}.

    Ideação MCP-token-optimization Story 1: replaces the verbose coverage{}
    block on write/link responses. Aggregates the per-dimension percentages
    weighted equally; blocking[] lists dimension keys whose percentage is
    below 100 (or whose linkage is short) and that aren't skip-flagged.
    """
    return spec_saturation_envelope_from_coverage(coverage)


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
    db: Any,
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

    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("board_id", "eq", board_id),
            _af("archived", "is_false"),
        ),
    )
    card_by_id = {c.id: c for c in cards}

    deps = await _analytics_list(
        db,
        "card_dependency",
        filters=(_af("card_id", "in", [card.id for card in cards]),),
    ) if cards else []
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

    specs = await _analytics_list(
        db,
        "spec",
        filters=(
            _af("board_id", "eq", board_id),
            _af("archived", "is_false"),
        ),
    )
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
        if classify_analytics_card(c) == "test":
            for sid in (c.test_scenario_ids or []):
                test_card_scenarios.add(sid)
    for s in specs:
        if s.status == SpecStatus.CANCELLED:
            continue
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


async def compute_mcp_board_analytics(
    db: Any,
    board_id: str,
    *,
    metric_type: str = "overview",
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
) -> Any:
    """Legacy board-scoped MCP analytics dispatcher.

    Keeps the historical MCP envelopes while ORM querying remains in the
    analytics service layer.
    """
    boards = await _analytics_list(
        db,
        "board",
        filters=(_af("id", "eq", board_id),),
        limit=1,
    )
    board = boards[0] if boards else None
    if not board:
        return {"error": "Board not found"}

    def _last_conclusion(card) -> dict | None:
        conclusions = card.conclusions
        if not conclusions or not isinstance(conclusions, list):
            return None
        last = conclusions[-1]
        return last if isinstance(last, dict) else None

    if metric_type == "overview":
        filters = _artifact_filters(
            board_id,
            include_archived=True,
            dt_from=dt_from,
            dt_to=dt_to,
        )
        ideations = await _analytics_list(db, "ideation", filters=filters)
        refinements = await _analytics_list(db, "refinement", filters=filters)
        specs = await _analytics_list(db, "spec", filters=filters)
        sprints = await _analytics_list(db, "sprint", filters=filters)
        cards = await _analytics_list(db, "card", filters=filters)

        card_partitions = partition_analytics_cards(cards)
        impl_cards = card_partitions["implementation"]
        test_cards = card_partitions["test"]
        done_cards = [card for card in cards if card.status == CardStatus.DONE]
        bug_cards = card_partitions["bug"]

        comp_vals = []
        drift_vals = []
        for card in cards:
            conclusion = _last_conclusion(card)
            if conclusion and "completeness" in conclusion:
                comp_vals.append(conclusion["completeness"])
            if conclusion and "drift" in conclusion:
                drift_vals.append(conclusion["drift"])

        avg_completeness = round(sum(comp_vals) / len(comp_vals), 1) if comp_vals else None
        avg_drift = round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None

        task_validation_gate = aggregate_task_validation_gate(cards)
        spec_validation_gate = aggregate_spec_validation_gate(specs)
        if (
            avg_completeness is None
            and task_validation_gate["avg_scores"]["completeness"] is not None
        ):
            avg_completeness = task_validation_gate["avg_scores"]["completeness"]
        if avg_drift is None and task_validation_gate["avg_scores"]["drift"] is not None:
            avg_drift = task_validation_gate["avg_scores"]["drift"]

        cycle_times = []
        for card in done_cards:
            hours = _hours_between(card.created_at, card.updated_at)
            if hours is not None:
                cycle_times.append(round(hours, 1))
        avg_cycle_hours = round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None

        def _lifecycle_cycle_time(items, done_status) -> float | None:
            times = []
            for item in items:
                if str(getattr(item, "status", "")) == str(done_status):
                    hours = _hours_between(item.created_at, item.updated_at)
                    if hours is not None:
                        times.append(round(hours, 1))
            return round(sum(times) / len(times), 1) if times else None

        sprint_evals_total = 0
        sprint_eval_scores = []
        for sprint in sprints:
            evaluations = getattr(sprint, "evaluations", None) or []
            if isinstance(evaluations, list):
                sprint_evals_total += len(evaluations)
                for evaluation in evaluations:
                    if isinstance(evaluation, dict) and evaluation.get("overall_score") is not None:
                        sprint_eval_scores.append(int(evaluation["overall_score"]))

        funnel = {
            "ideations": len(ideations),
            "refinements": len(refinements),
            "specs": len(specs),
            "sprints": len(sprints),
            "cards": len(cards),
            "done": len(done_cards),
        }
        bugs_open = sum(
            1 for card in bug_cards if card.status not in (CardStatus.DONE, CardStatus.CANCELLED)
        )

        return {
            "board_id": board_id,
            "ideation_count": len(ideations),
            "refinement_count": len(refinements),
            "spec_count": len(specs),
            "sprint_count": len(sprints),
            "task_count": {
                "total": len(cards),
                "impl": len(impl_cards),
                "tests": len(test_cards),
                "bugs": len(bug_cards),
            },
            "avg_completeness": avg_completeness,
            "avg_drift": avg_drift,
            "avg_cycle_hours": avg_cycle_hours,
            "cycle_time": {
                "ideation": _lifecycle_cycle_time(ideations, "done"),
                "refinement": _lifecycle_cycle_time(refinements, "done"),
                "spec": _lifecycle_cycle_time(specs, "done"),
                "sprint": _lifecycle_cycle_time(sprints, "closed"),
                "card": avg_cycle_hours,
            },
            "task_validation_gate": task_validation_gate,
            "spec_validation_gate": spec_validation_gate,
            "sprint_evaluation": {
                "total_submitted": sprint_evals_total,
                "avg_overall_score": (
                    round(sum(sprint_eval_scores) / len(sprint_eval_scores), 1)
                    if sprint_eval_scores
                    else None
                ),
            },
            "funnel": funnel,
            "bugs": {
                "total": len(bug_cards),
                "open": bugs_open,
                "done": sum(1 for card in bug_cards if card.status == CardStatus.DONE),
                "by_severity": {
                    "critical": sum(
                        1 for card in bug_cards if getattr(card, "severity", None) == "critical"
                    ),
                    "major": sum(
                        1 for card in bug_cards if getattr(card, "severity", None) == "major"
                    ),
                    "minor": sum(
                        1 for card in bug_cards if getattr(card, "severity", None) == "minor"
                    ),
                },
            },
        }

    if metric_type == "funnel":
        return await compute_funnel(
            db, board_id, dt_from=dt_from, dt_to=dt_to, include_archived=True
        )

    if metric_type == "quality":
        cards = await _analytics_list(
            db,
            "card",
            filters=_artifact_filters(
                board_id,
                include_archived=True,
                dt_from=dt_from,
                dt_to=dt_to,
                extra=(_af("status", "eq", CardStatus.DONE),),
            ),
        )

        result = []
        for card in cards:
            conclusion = _last_conclusion(card)
            if conclusion and "completeness" in conclusion and "drift" in conclusion:
                result.append(
                    {
                        "card_id": card.id,
                        "title": card.title,
                        "completeness": conclusion["completeness"],
                        "drift": conclusion["drift"],
                    }
                )
        return result

    if metric_type == "velocity":
        return await compute_velocity(
            db,
            board_id,
            granularity="week",
            weeks=12,
            dt_from=dt_from,
            dt_to=dt_to,
            include_archived=True,
        )

    if metric_type == "coverage":
        return await compute_coverage(
            db, board_id, dt_from=dt_from, dt_to=dt_to, include_archived=True
        )

    if metric_type == "agents":
        cards = await _analytics_list(
            db,
            "card",
            filters=_artifact_filters(
                board_id,
                include_archived=True,
                dt_from=dt_from,
                dt_to=dt_to,
            ),
        )

        groups: dict[str, list] = {}
        for card in cards:
            groups.setdefault(card.created_by, []).append(card)

        result = []
        for actor_id, actor_cards in groups.items():
            done = [card for card in actor_cards if card.status == CardStatus.DONE]
            conclusions = [_last_conclusion(card) for card in done]
            comp = [
                conclusion["completeness"]
                for conclusion in conclusions
                if conclusion and "completeness" in conclusion
            ]
            drift = [
                conclusion["drift"]
                for conclusion in conclusions
                if conclusion and "drift" in conclusion
            ]
            result.append(
                {
                    "actor_id": actor_id,
                    "total_cards": len(actor_cards),
                    "done_cards": len(done),
                    "avg_completeness": round(sum(comp) / len(comp), 1) if comp else None,
                    "avg_drift": round(sum(drift) / len(drift), 1) if drift else None,
                }
            )
        result.sort(key=lambda item: item["done_cards"], reverse=True)
        return result

    return {
        "error": (
            f"Unknown metric_type: {metric_type}. "
            "Use one of: overview, funnel, quality, velocity, coverage, agents"
        )
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


def render_decisions_markdown(
    decisions: list | None, *, include_superseded: bool = False
) -> str:
    """Ideação #10 Fase 2 — render structured markdown for agent consumption.

    Produces a ``## Decisions`` block with one section per decision (title,
    status, rationale, alternatives, linked FRs, linked tasks). Supersedes
    relationship (supersedes_decision_id) is surfaced inline.

    Parameters
    ----------
    decisions : list[dict] | None
        Raw decisions list from spec.decisions.
    include_superseded : bool
        When False (default), status='superseded' entries are omitted.
        When True, they are included with their supersedes chain visible.

    Returns
    -------
    str
        Empty string when there are no decisions to render, otherwise a
        markdown block. Each decision renders in ~200 tokens to avoid
        inflating get_task_context payload for specs with many decisions.
    """
    if not decisions:
        return ""

    kept = []
    for d in decisions:
        if not isinstance(d, dict):
            continue
        status = d.get("status") or "active"
        if not include_superseded and status == "superseded":
            continue
        kept.append(d)

    if not kept:
        return ""

    lines: list[str] = ["## Decisions", ""]
    for d in kept:
        title = d.get("title") or d.get("id") or "Untitled"
        status = d.get("status") or "active"
        lines.append(f"### {title} ({status})")

        if sup := d.get("supersedes_decision_id"):
            lines.append(f"- **Supersedes**: `{sup}`")

        if rationale := d.get("rationale"):
            lines.append(f"- **Rationale**: {rationale}")
        if context := d.get("context"):
            lines.append(f"- **Context**: {context}")

        alternatives = d.get("alternatives_considered") or []
        if alternatives:
            alts = ", ".join(str(a) for a in alternatives)
            lines.append(f"- **Alternatives**: {alts}")

        linked_frs = d.get("linked_requirements") or []
        if linked_frs:
            frs = ", ".join(f"FR{x}" if str(x).isdigit() else str(x) for x in linked_frs)
            lines.append(f"- **Linked FRs**: {frs}")

        linked_tasks = d.get("linked_task_ids") or []
        if linked_tasks:
            tasks = ", ".join(str(t) for t in linked_tasks)
            lines.append(f"- **Linked tasks**: {tasks}")

        if notes := d.get("notes"):
            lines.append(f"- **Notes**: {notes}")

        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


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


# ===========================================================================
# R01A REST-FU2b: pure aggregation helpers moved from api/analytics.py (re-exported
# there during the strangle) + the cross-board overview reader.
# ===========================================================================


def _extract_conclusion(card) -> dict | None:
    """Return the last conclusion entry from a card's conclusions JSON list."""
    conclusions = card.conclusions
    if not conclusions or not isinstance(conclusions, list):
        return None
    last = conclusions[-1]
    if not isinstance(last, dict):
        return None
    return last

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
    """Aggregate Sprint Evaluation gate across sprints.

    Shape harmonizado com _aggregate_spec_evaluation — inclui
    avg_dimension_scores (vazio quando não há dimensões) para que
    consumers possam processar ambos os evaluation types com o mesmo
    código.
    """
    total = 0
    total_approve = 0
    total_reject = 0
    overall_vals: list[float] = []
    dimension_avgs: dict[str, list[float]] = {}
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
        "approve_rate": round(total_approve / total * 100, 1) if total else None,
        "avg_overall_score": _avg(overall_vals),
        "avg_dimension_scores": {k: _avg(v) for k, v in dimension_avgs.items()},
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

def _compute_velocity(done_cards: list, weeks: int, all_cards: list | None = None) -> list[dict]:
    """Weekly velocity — backward-compat shim for callers that don't need
    spec/sprint overlays. Delegates to the bucket builder with spec/sprint
    event dicts empty."""
    return _build_velocity_buckets(
        done_cards=done_cards,
        all_cards=all_cards,
        periods=weeks,
        granularity="week",
        spec_moves=[],
        sprint_moves=[],
    )

def _bucket_key(dt: datetime, granularity: str) -> str:
    """Build the bucket key for a datetime under the chosen granularity."""
    if granularity == "day":
        return dt.strftime("%Y-%m-%d")
    # week — Monday-aligned
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")

def _build_velocity_buckets(
    *,
    done_cards: list,
    all_cards: list | None,
    periods: int,
    granularity: str,
    spec_moves: list[tuple[datetime, str]],
    sprint_moves: list[tuple[datetime, str]],
) -> list[dict]:
    """Shared bucket builder for week and day granularities.

    Series per bucket:
    - ``impl`` / ``test`` / ``bug`` — cards of that type moved to done in the bucket.
    - ``validation_bounce`` — task validations that failed in the bucket.
    - ``spec_done`` — spec_moved events where details.new_status == 'done'.
    - ``sprint_done`` — sprint_moved events where details.new_status == 'closed'.
    """
    now = datetime.now(timezone.utc)
    buckets: dict[str, dict[str, int]] = {}
    # Seed buckets backwards so the axis has a stable shape even when no
    # events landed in a given period.
    for i in range(periods):
        anchor = now - (timedelta(days=i) if granularity == "day" else timedelta(weeks=i))
        key = _bucket_key(anchor, granularity)
        buckets[key] = {
            "impl": 0, "test": 0, "bug": 0,
            "validation_bounce": 0,
            "spec_done": 0, "sprint_done": 0,
        }

    for c in done_cards:
        if not c.updated_at:
            continue
        updated = c.updated_at
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        key = _bucket_key(updated, granularity)
        if key in buckets:
            category = classify_analytics_card(c)
            if category == "bug":
                buckets[key]["bug"] += 1
            elif category == "test":
                buckets[key]["test"] += 1
            else:
                buckets[key]["impl"] += 1

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
                key = _bucket_key(dt, granularity)
                if key in buckets:
                    buckets[key]["validation_bounce"] += 1

    for dt, status_val in spec_moves:
        if status_val != "done":
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        key = _bucket_key(dt, granularity)
        if key in buckets:
            buckets[key]["spec_done"] += 1

    for dt, status_val in sprint_moves:
        if status_val != "closed":
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        key = _bucket_key(dt, granularity)
        if key in buckets:
            buckets[key]["sprint_done"] += 1

    period_label = "day" if granularity == "day" else "week"
    return [
        {
            period_label: k,
            "impl": v["impl"], "test": v["test"], "bug": v["bug"],
            "validation_bounce": v["validation_bounce"],
            "spec_done": v["spec_done"], "sprint_done": v["sprint_done"],
        }
        for k, v in sorted(buckets.items())
    ]


async def compute_overview(
    db: Any,
    user_id: str,
    *,
    dt_from: datetime | None = None,
    dt_to: datetime | None = None,
) -> dict:
    """Cross-board KPIs reader (spec R01A REST-FU2b) — the transport-free body of
    the legacy ``analytics_overview`` endpoint, verbatim. Date parsing + the HTTP
    envelope stay in the adapter; the validation-gate aggregators live in this
    module."""
    # Fetch boards owned by user
    boards = await _analytics_list(
        db,
        "board",
        filters=(_af("owner_id", "eq", user_id),),
    )
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
            "spec_validation_gate": aggregate_spec_validation_gate([]),
            "task_validation_gate": aggregate_task_validation_gate([]),
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

    filters = [
        _af("board_id", "in", board_ids),
        _af("archived", "is_false"),
    ]
    if dt_from:
        filters.append(_af("created_at", "gte", dt_from))
    if dt_to:
        filters.append(_af("created_at", "lt", dt_to))
    artifact_filters = tuple(filters)

    ideations = await _analytics_list(db, "ideation", filters=artifact_filters)
    refinements = await _analytics_list(db, "refinement", filters=artifact_filters)
    specs = await _analytics_list(db, "spec", filters=artifact_filters)
    cards = await _analytics_list(db, "card", filters=artifact_filters)

    card_partitions = partition_analytics_cards(cards)
    impl_cards = card_partitions["implementation"]
    test_cards = card_partitions["test"]
    bug_cards_all = card_partitions["bug"]

    sprints = await _analytics_list(db, "sprint", filters=artifact_filters)

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

    # Reviewer-reported scores come from aggregate_task_validation_gate.

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
        b_bugs = [c for c in b_cards if classify_analytics_card(c) == "bug"]
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
    spec_validation_gate = aggregate_spec_validation_gate(specs)
    task_validation_gate = aggregate_task_validation_gate(
        [c for c in cards if classify_analytics_card(c) != "test"]
    )
    spec_evaluation = _aggregate_spec_evaluation(specs)
    sprint_evaluation = _aggregate_sprint_evaluation(sprints)

    # Bugs per spec
    bugs_per_spec: dict[str, int] = {}
    for c in bug_cards:
        sid = c.spec_id or "unlinked"
        bugs_per_spec[sid] = bugs_per_spec.get(sid, 0) + 1

    # Bug rate per spec (bugs / total tasks in that spec) — only specs
    # with at least one bug. Specs com rate=0 poluem o payload sem trazer
    # sinal; consumer que precisa da lista completa usa /boards/{id}/specs.
    bug_rate_per_spec = []
    for s in specs:
        s_cards = [c for c in cards if c.spec_id == s.id]
        s_bugs = [c for c in s_cards if classify_analytics_card(c) == "bug"]
        if s_cards and s_bugs:
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
                delta_hours = _hours_between(c.created_at, earliest)
                if delta_hours is not None:
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
        ct_raw = _hours_between(c.created_at, c.updated_at)
        if ct_raw is not None:
            ct = round(ct_raw, 1)
            cycle_times.append(ct)
    avg_cycle_hours = round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None

    def _lifecycle_ct(items, done_status_str: str) -> float | None:
        times = []
        for item in items:
            if str(item.status) == done_status_str and item.created_at and item.updated_at:
                hours = _hours_between(item.created_at, item.updated_at)
                if hours is not None:
                    times.append(hours)
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


# R01A REST-FU2b rework: lifecycle-move reader moved here from api/analytics.py
# to remove the service->api coupling (Clean Core).
async def _load_lifecycle_moves(
    db: Any, board_id: str, action: str,
) -> list[tuple[datetime, str]]:
    """Read ActivityLog rows for a lifecycle action (spec_moved / sprint_moved)
    and return (created_at, new_status) tuples for the aggregator."""
    rows = await _analytics_list(
        db,
        "activity_log",
        filters=(
            _af("board_id", "eq", board_id),
            _af("action", "eq", action),
        ),
    )
    out: list[tuple[datetime, str]] = []
    for row in rows:
        details = row.details or {}
        if not isinstance(details, dict):
            continue
        # Different writers stamp the terminal-state field under slightly
        # different keys — accept the full set we've observed in the wild.
        for key in ("to_status", "new_status", "status"):
            val = details.get(key)
            if isinstance(val, str):
                out.append((row.created_at, val))
                break
    return out


async def compute_quality(db, board_id: str, *, dt_from=None, dt_to=None) -> dict:
    """Quality scatters reader (spec R01A REST-FU2c) — conclusion-reported vs
    validation-reported completeness/drift for done cards. Transport-free body of
    the legacy board_quality endpoint."""
    cards = await _analytics_list(
        db,
        "card",
        filters=_artifact_filters(
            board_id,
            include_archived=False,
            dt_from=dt_from,
            dt_to=dt_to,
            extra=(_af("status", "eq", CardStatus.DONE),),
        ),
    )
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


async def compute_validations(db, board_id: str, *, dt_from=None, dt_to=None) -> dict:
    """Validation-gate panel reader (spec R01A REST-FU2c) — spec/task validation
    gates + spec/sprint evaluations with per-spec/per-card breakdown. Transport-free
    body of the legacy board_validations endpoint."""
    filters = _artifact_filters(
        board_id,
        include_archived=False,
        dt_from=dt_from,
        dt_to=dt_to,
    )
    specs = await _analytics_list(db, "spec", filters=filters)
    cards = await _analytics_list(db, "card", filters=filters)
    sprints = await _analytics_list(db, "sprint", filters=filters)

    # Per-spec breakdown for Spec Validation Gate — walks full history (D4)
    per_spec: list[dict] = []
    for s in specs:
        vals = getattr(s, "validations", None) or []
        if not isinstance(vals, list) or len(vals) == 0:
            continue
        agg = aggregate_spec_validation_gate([s])
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
        agg = aggregate_task_validation_gate([c])
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
            **aggregate_spec_validation_gate(specs),
            "per_spec": per_spec,
        },
        "task_validation_gate": {
            **aggregate_task_validation_gate(cards),
            "per_card": per_card,
        },
        "spec_evaluation": _aggregate_spec_evaluation(specs),
        "sprint_evaluation": _aggregate_sprint_evaluation(sprints),
    }


async def compute_spec_analytics(db, board_id: str, spec_id: str) -> dict | None:
    """Per-spec analytics reader (spec R01A REST-FU2c) — transport-free
    body of the legacy board_spec_analytics endpoint; returns None when the spec is not found."""
    specs = await _analytics_list(
        db,
        "spec",
        filters=(
            _af("id", "eq", spec_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    spec = specs[0] if specs else None
    if not spec:
        return None

    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("spec_id", "eq", spec_id),
            _af("archived", "is_false"),
        ),
    )

    # Spec 233eaad3: coverage_summary com cancelled-card filter — mesmo
    # cálculo do validation gate (SSOT), agora visível no 3º nível do
    # Analytics dashboard.
    from okto_pulse.core.services.analytics_service import spec_coverage_summary
    coverage_summary = spec_coverage_summary(spec, cards=cards)

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
            "rejection_reasons": classify_spec_violation(
                v.get("threshold_violations") or [], v.get("recommendation", "")
            ),
            "resolved_thresholds": v.get("resolved_thresholds"),
            "created_at": v.get("created_at"),
            "active": v.get("id") == getattr(spec, "current_validation_id", None),
        })

    card_partitions = partition_analytics_cards(cards)
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
        "task_validation_summary": aggregate_task_validation_gate(cards),
        "spec_evaluation": _aggregate_spec_evaluation([spec]),
        "coverage_summary": coverage_summary,
        "integration_requirements": getattr(spec, "integration_requirements", None) or [],
        "observability_requirements": getattr(spec, "observability_requirements", None) or [],
        # Bug 42e78332: surface decisions for the entity-detail drilldown (parity with
        # _spec_detail + the IR/OR pattern). EntityDetail.tsx reads data.decisions /
        # data.decisions_coverage / data.decisions_uncovered_ids; sourced from the
        # already-computed coverage_summary (SSOT spec_coverage_summary).
        "decisions": getattr(spec, "decisions", None) or [],
        "decisions_coverage": coverage_summary["decisions_coverage_pct"],
        "decisions_uncovered_ids": coverage_summary["decisions_uncovered_ids"],
        "cards_summary": {
            "total": len(cards),
            "by_status": _card_status_breakdown(cards),
            "by_type": {
                "normal": len(card_partitions["implementation"]),
                "test": len(card_partitions["test"]),
                "bug": len(card_partitions["bug"]),
            },
        },
    }



async def compute_sprint_analytics(db, board_id: str, sprint_id: str) -> dict | None:
    """Per-sprint analytics reader (spec R01A REST-FU2c) — transport-free
    body of the legacy board_sprint_analytics endpoint; returns None when the sprint is not found."""
    sprints = await _analytics_list(
        db,
        "sprint",
        filters=(
            _af("id", "eq", sprint_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    sprint = sprints[0] if sprints else None
    if not sprint:
        return None

    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("sprint_id", "eq", sprint_id),
            _af("archived", "is_false"),
        ),
    )
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

    card_partitions = partition_analytics_cards(cards)
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
                "normal": len(card_partitions["implementation"]),
                "test": len(card_partitions["test"]),
                "bug": len(card_partitions["bug"]),
            },
        },
        "task_validation_gate": aggregate_task_validation_gate(cards),
        "evaluation_timeline": eval_timeline,
        "velocity": velocity,
    }


async def compute_sprints_analytics(db, board_id: str, *, dt_from=None, dt_to=None) -> dict:
    """Board-level analytics reader (spec R01A REST-FU2d) — transport-free body of
    the legacy board_sprints_analytics endpoint."""
    filters = _artifact_filters(
        board_id,
        include_archived=False,
        dt_from=dt_from,
        dt_to=dt_to,
    )
    sprints = await _analytics_list(db, "sprint", filters=filters)
    all_cards = await _analytics_list(db, "card", filters=filters)
    specs = await _analytics_list(db, "spec", filters=filters)
    specs_by_id = {spec.id: spec for spec in specs}
    from okto_pulse.core.services.sprint_scope import SprintScopeResolver

    per_sprint: list[dict] = []
    normal_sprints_total = 0
    hotfix_lanes_total = 0
    active_hotfix_lanes = 0
    for sp in sprints:
        lane_type = (
            sp.lane_type.value
            if getattr(sp.lane_type, "value", None)
            else str(sp.lane_type or SprintLaneType.NORMAL.value)
        )
        if lane_type == SprintLaneType.HOTFIX.value:
            hotfix_lanes_total += 1
            if sp.status == SprintStatus.ACTIVE:
                active_hotfix_lanes += 1
        else:
            normal_sprints_total += 1
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
        task_gate = aggregate_task_validation_gate(sp_cards)
        spec = specs_by_id.get(sp.spec_id)
        scope = (
            SprintScopeResolver.resolve(sprint=sp, spec=spec, cards=sp_cards)
            if spec is not None
            else None
        )

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
            "lane_type": lane_type,
            "origin_sprint_id": getattr(sp, "origin_sprint_id", None),
            "origin_bug_id": getattr(sp, "origin_bug_id", None),
            "normal_sprint_created": getattr(sp, "normal_sprint_created", lane_type == "normal"),
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
            "scope": (
                {
                    "sprint_version": scope.sprint_version,
                    "spec_version": scope.spec_version,
                    "counts": {
                        name: len(items) for name, items in scope.items.items()
                    },
                }
                if scope is not None
                else None
            ),
        })
    per_sprint.sort(key=lambda x: x["total_cards"], reverse=True)

    return {
        "summary": {
            "total_sprints": len(sprints),
            "normal_sprints_total": normal_sprints_total,
            "hotfix_lanes_total": hotfix_lanes_total,
            "active_hotfix_lanes": active_hotfix_lanes,
            "status_breakdown": _sprint_status_breakdown(sprints),
            "avg_completion_rate": round(
                sum(p["completion_rate"] for p in per_sprint) / len(per_sprint), 1
            ) if per_sprint else None,
            "sprint_evaluation": _aggregate_sprint_evaluation(sprints),
        },
        "sprints": per_sprint,
    }



async def compute_agents(db, board_id: str, *, dt_from=None, dt_to=None) -> dict:
    """Board-level analytics reader (spec R01A REST-FU2d) — transport-free body of
    the legacy board_agents endpoint."""
    cards = await _analytics_list(
        db,
        "card",
        filters=_artifact_filters(
            board_id,
            include_archived=False,
            dt_from=dt_from,
            dt_to=dt_to,
        ),
    )

    specs = await _analytics_list(
        db,
        "spec",
        filters=(
            _af("board_id", "eq", board_id),
            _af("archived", "is_false"),
        ),
    )

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



# R01A REST-FU2e: entity-list readers moved verbatim from api/analytics.py.


async def _list_ideation_entities(
    db: Any,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    filters = [
        _af("board_id", "eq", board_id),
        _af("archived", "is_false"),
    ]
    if dt_from:
        filters.append(_af("created_at", "gte", dt_from))
    if dt_to:
        filters.append(_af("created_at", "lt", dt_to))

    total = await _analytics_count(
        db,
        "ideation",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
    )
    ideations = await _analytics_list(
        db,
        "ideation",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
        order_by="created_at",
        descending=True,
        offset=offset,
        limit=limit,
    )

    # For each ideation, count derived refinements and specs
    result_items = []
    for i in ideations:
        ref_count = await _analytics_count(
            db,
            "refinement",
            filters=(
                _af("ideation_id", "eq", i.id),
                _af("archived", "is_false"),
            ),
        )
        spec_count = await _analytics_count(
            db,
            "spec",
            filters=(
                _af("ideation_id", "eq", i.id),
                _af("archived", "is_false"),
            ),
        )
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
    db: Any,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    filters = [
        _af("board_id", "eq", board_id),
        _af("archived", "is_false"),
    ]
    if dt_from:
        filters.append(_af("created_at", "gte", dt_from))
    if dt_to:
        filters.append(_af("created_at", "lt", dt_to))

    total = await _analytics_count(
        db,
        "spec",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
    )
    specs = await _analytics_list(
        db,
        "spec",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
        order_by="created_at",
        descending=True,
        offset=offset,
        limit=limit,
    )

    result_items = []
    for s in specs:
        ac_list = s.acceptance_criteria or []
        scenarios = s.test_scenarios or []
        card_count = await _analytics_count(
            db,
            "card",
            filters=(
                _af("spec_id", "eq", s.id),
                _af("archived", "is_false"),
            ),
        )

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
    db: Any,
    board_id: str,
    offset: int,
    limit: int,
    dt_from: datetime | None,
    dt_to: datetime | None,
    search: str = "",
) -> dict:
    filters = [
        _af("board_id", "eq", board_id),
        _af("archived", "is_false"),
    ]
    if dt_from:
        filters.append(_af("created_at", "gte", dt_from))
    if dt_to:
        filters.append(_af("created_at", "lt", dt_to))

    total = await _analytics_count(
        db,
        "card",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
    )
    cards = await _analytics_list(
        db,
        "card",
        filters=tuple(filters),
        search=search,
        search_fields=("title",),
        order_by="created_at",
        descending=True,
        offset=offset,
        limit=limit,
    )

    result_items = []
    for c in cards:
        concl = _extract_conclusion(c)
        ct = getattr(c, "card_type", "normal") or "normal"
        result_items.append({
            "id": c.id,
            "title": c.title,
            "status": c.status.value if c.status else None,
            "is_test": classify_analytics_card(c) == "test",
            "card_type": ct if hasattr(ct, "value") else ct,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "completeness": concl.get("completeness") if concl else None,
            "drift": concl.get("drift") if concl else None,
        })

    return {"total": total, "offset": offset, "limit": limit, "items": result_items}



# R01A REST-FU2f: entity-detail readers moved from api/analytics.py.
# HTTPException(404) -> return None (adapter maps via EntityNotFoundError).


def _resolve_linked_fr_indices(linked_refs: list, frs: list) -> set[int]:
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
        # Try matching by structured id or text content.
        for i, fr in enumerate(frs):
            fr_text = _structured_ref_text(fr)
            fr_id = _structured_ref_id(fr)
            if ref_str == fr_id or (fr_text and (ref_str in fr_text or fr_text in ref_str)):
                indices.add(i)
                break
    return indices

async def _spec_detail(db: Any, board_id: str, spec_id: str) -> dict:
    """Spec detail: AC coverage, scenario statuses, cards with conclusions, cycle time, derivation chain."""
    specs = await _analytics_list(
        db,
        "spec",
        filters=(
            _af("id", "eq", spec_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    spec = specs[0] if specs else None
    if not spec:
        return None

    ac_list = spec.acceptance_criteria or []
    scenarios = spec.test_scenarios or []

    # Coverage — normalize mixed linked_criteria formats (int / str-idx / AC text)
    # to int indices so the `covered_ac <= total_ac` invariant holds.
    covered_ac_indices: set[int] = set()
    scenario_statuses: list[dict] = []
    for ts in scenarios:
        if isinstance(ts, dict):
            covered_ac_indices |= resolve_linked_criteria_to_indices(
                ts.get("linked_criteria"), ac_list
            )
            scenario_statuses.append({
                "id": ts.get("id"),
                "title": ts.get("title"),
                "status": ts.get("status", "unknown"),
            })
    covered_ac_count = min(len(covered_ac_indices), len(ac_list))

    # Cards linked to this spec
    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("spec_id", "eq", spec_id),
            _af("archived", "is_false"),
        ),
    )
    from okto_pulse.core.services.analytics_service import spec_coverage_summary
    coverage_summary = spec_coverage_summary(spec, cards=cards)
    card_data = []
    for c in cards:
        concl = _extract_conclusion(c)
        ct = getattr(c, "card_type", "normal") or "normal"
        card_data.append({
            "id": c.id,
            "title": c.title,
            "status": c.status.value if c.status else None,
            "is_test": classify_analytics_card(c) == "test",
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
        delta = _hours_between(c.created_at, c.updated_at)
        if delta is not None:
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
    for idx, ac in enumerate(ac_list):
        ac_details.append({
            "index": idx,
            "id": _structured_ref_id(ac),
            "text": _structured_ref_text(ac),
            "covered": idx in covered_ac_indices,
        })

    # FR details with coverage status (rules + contracts)
    fr_details = []
    for idx, fr in enumerate(frs):
        fr_details.append({
            "index": idx,
            "id": _structured_ref_id(fr),
            "text": _structured_ref_text(fr),
            "has_rule": idx in fr_indices_with_rules,
            "has_contract": idx in fr_indices_with_contracts,
        })

    # Bug stats for this spec
    bug_cards = [c for c in cards if classify_analytics_card(c) == "bug"]

    # Sprint breakdown
    sprints = await _analytics_list(
        db,
        "sprint",
        filters=(
            _af("spec_id", "eq", spec_id),
            _af("archived", "is_false"),
        ),
    )
    sprint_summaries = []
    for sp in sprints:
        sp_cards = [c for c in cards if getattr(c, "sprint_id", None) == sp.id]
        sp_done = [c for c in sp_cards if c.status == CardStatus.DONE]
        sp_concls = [_extract_conclusion(c) for c in sp_done if _extract_conclusion(c)]
        sp_completeness = [cn.get("completeness") for cn in sp_concls if cn.get("completeness") is not None]
        sp_drift = [cn.get("drift") for cn in sp_concls if cn.get("drift") is not None]
        sp_cycle = []
        for c in sp_done:
            delta = _hours_between(c.created_at, c.updated_at)
            if delta is not None:
                sp_cycle.append(round(delta, 1))
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
        "coverage_summary": coverage_summary,
        "integration_requirements": getattr(spec, "integration_requirements", None) or [],
        "observability_requirements": getattr(spec, "observability_requirements", None) or [],
        # Bug 42e78332: surface decisions for the entity-detail drilldown. EntityDetail.tsx
        # reads top-level data.decisions / data.decisions_coverage / data.decisions_uncovered_ids
        # (KPI + "Decisions Coverage" panel). Sourced from spec.decisions + the already-computed
        # coverage_summary (SSOT spec_coverage_summary) — additive, mirrors IR/OR (spec 233eaad3).
        "decisions": getattr(spec, "decisions", None) or [],
        "decisions_coverage": coverage_summary["decisions_coverage_pct"],
        "decisions_uncovered_ids": coverage_summary["decisions_uncovered_ids"],
        "bugs_count": len(bug_cards),
        "sprints": sprint_summaries,
    }

async def _ideation_detail(db: Any, board_id: str, ideation_id: str) -> dict:
    """Ideation detail: scope assessment, derived refinements/specs, QA count."""
    ideations = await _analytics_list(
        db,
        "ideation",
        filters=(
            _af("id", "eq", ideation_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    ideation = ideations[0] if ideations else None
    if not ideation:
        return None

    ref_count = await _analytics_count(
        db,
        "refinement",
        filters=(_af("ideation_id", "eq", ideation_id),),
    )
    spec_count = await _analytics_count(
        db,
        "spec",
        filters=(_af("ideation_id", "eq", ideation_id),),
    )
    qa_count = await _analytics_count(
        db,
        "ideation_qa_item",
        filters=(_af("ideation_id", "eq", ideation_id),),
    )

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

async def _card_detail(db: Any, board_id: str, card_id: str) -> dict:
    """Card detail: conclusions, validations history, cycle time, spec link."""
    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("id", "eq", card_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    card = cards[0] if cards else None
    if not card:
        return None

    concl = _extract_conclusion(card)
    cycle_hours = None
    if card.status == CardStatus.DONE and card.created_at and card.updated_at:
        delta = _hours_between(card.created_at, card.updated_at)
        cycle_hours = round(delta, 1) if delta is not None else None

    # Normalize card_type to lowercase string (enum or raw).
    ct = getattr(card, "card_type", "normal")
    card_type = str(ct).replace("CardType.", "").lower() or "normal"

    return {
        "card_id": card.id,
        "title": card.title,
        "status": card.status.value if card.status else None,
        "is_test": classify_analytics_card(card) == "test",
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

async def _refinement_detail(db: Any, board_id: str, refinement_id: str) -> dict:
    """Refinement detail: scope, KBs, derived specs."""
    refinements = await _analytics_list(
        db,
        "refinement",
        filters=(
            _af("id", "eq", refinement_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    refinement = refinements[0] if refinements else None
    if not refinement:
        return None

    # Derived specs
    specs = await _analytics_list(
        db,
        "spec",
        filters=(_af("refinement_id", "eq", refinement_id),),
    )

    # Knowledge bases count
    kb_count = await _analytics_count(
        db,
        "refinement_knowledge_base",
        filters=(_af("refinement_id", "eq", refinement_id),),
    )

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

async def _sprint_detail(db: Any, board_id: str, sprint_id: str) -> dict:
    """Sprint detail: tasks done/total, completeness avg, drift avg, cycle time, evaluations, comparison."""
    sprints = await _analytics_list(
        db,
        "sprint",
        filters=(
            _af("id", "eq", sprint_id),
            _af("board_id", "eq", board_id),
        ),
        limit=1,
    )
    sprint = sprints[0] if sprints else None
    if not sprint:
        return None

    # Cards in this sprint (skip archived to keep counts honest)
    cards = await _analytics_list(
        db,
        "card",
        filters=(
            _af("sprint_id", "eq", sprint_id),
            _af("archived", "is_false"),
        ),
    )

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
            delta = _hours_between(c.created_at, c.updated_at)
            if delta is not None:
                ct_hours = round(delta, 1)
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
    spec_rows = (
        await _analytics_list(
            db,
            "spec",
            filters=(_af("id", "eq", sprint.spec_id),),
            limit=1,
        )
        if sprint.spec_id
        else []
    )
    spec = spec_rows[0] if spec_rows else None
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
        siblings = await _analytics_list(
            db,
            "sprint",
            filters=(
                _af("spec_id", "eq", sprint.spec_id),
                _af("archived", "is_false"),
            ),
        )
        for sib in siblings:
            sib_cards = await _analytics_list(
                db,
                "card",
                filters=(
                    _af("sprint_id", "eq", sib.id),
                    _af("archived", "is_false"),
                ),
            )
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


def resolve_linked_requirement_tokens_to_fr_or_tr_ids(
    linked_tokens: list | None,
    frs: list,
    trs: list,
) -> tuple[list[str], list[str]]:
    """Resolve requirement refs against FRs first, then TRs (MCP-FU6: moved here
    from the MCP server so MCP-scoped use cases can resolve without importing the
    transport package).

    ``linked_requirements`` is used by agents as a generic requirement-link
    surface on API contracts, integration requirements, observability
    requirements, and decisions. Keep FR behavior unchanged and add strict TR
    support for structured technical requirements.
    """
    resolved: list[str] = []
    unresolved: list[str] = []
    seen: set[str] = set()

    fr_ids, fr_unresolved = resolve_linked_requirements_to_ids(linked_tokens, frs)
    for rid in fr_ids:
        if rid not in seen:
            seen.add(rid)
            resolved.append(rid)

    if not fr_unresolved:
        return resolved, unresolved

    tr_ids, tr_unresolved = resolve_linked_requirements_to_ids(fr_unresolved, trs)
    for rid in tr_ids:
        if rid not in seen:
            seen.add(rid)
            resolved.append(rid)
    unresolved.extend(tr_unresolved)
    return resolved, unresolved


def available_structured_ids(items: list) -> list[str]:
    """Canonical structured ids present in a FR/TR list (MCP-FU6: core twin of the
    MCP server's ``_available_structured_ids``, for unresolved-token envelopes)."""
    return [rid for rid in (_structured_ref_id(item) for item in items) if rid]
