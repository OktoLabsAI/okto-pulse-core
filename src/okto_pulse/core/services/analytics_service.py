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

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import Spec


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
