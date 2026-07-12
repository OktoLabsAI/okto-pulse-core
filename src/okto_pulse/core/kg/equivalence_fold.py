"""Query-time equivalence fold (spec MKG-C-S1 — FR6, D5, BR4).

Active equivalence records (off-graph ledger, D1) are applied to recall
results as a PURE post-fetch step in the core: member node ids are
rewritten to their survivor and the resulting duplicate rows are folded.
The graph itself is never touched — un-merge (ledger revoke) makes the
members reappear immediately because the per-board mapping cache is
invalidated on every ledger write.

Composition note (D5): the graph_layer canonical/working scoping stays
IN-CYPHER at the store ("not filtered post-hoc") — the fold composes on
top of it because equivalences live OUTSIDE the graph and the store
cannot know them. ``kg_query_cypher`` (raw Cypher) is deliberately NOT
intercepted: raw cypher results may expose members until the physical
materialization inside the deterministic rebuild (documented in
tool-docs/kg.md).

Titles/attrs of folded rows may still show the member's values when the
survivor was not part of the same result set — members of a dedup group
share the same (type, source_artifact_ref) and near-identical content by
construction, so the id rewrite is the semantically load-bearing part.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping

from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.ports.kg_equivalence_ledger import (
    resolve_equivalence_ledger,
)

logger = logging.getLogger("okto_pulse.kg.equivalence_fold")

__all__ = [
    "fold_id",
    "fold_pair_rows",
    "fold_result_values",
    "fold_rows",
    "invalidate_equivalence_fold_cache",
    "load_equivalence_mapping",
]

# Per-board member_id -> survivor_id mapping cache. Invalidated on every
# ledger write (dedup append / unmerge revoke) — the marginalia reloads on
# every query, acceptable only at N=1 (D5).
_FOLD_CACHE: dict[str, Mapping[str, str]] = {}


def invalidate_equivalence_fold_cache(board_id: str | None = None) -> None:
    """Drop the cached mapping for ``board_id`` (or all boards)."""

    if board_id is None:
        _FOLD_CACHE.clear()
    else:
        _FOLD_CACHE.pop(board_id, None)


def load_equivalence_mapping(board_id: str) -> Mapping[str, str]:
    """Return the ACTIVE member->survivor mapping for ``board_id``.

    Read-path is deliberately tolerant (unlike the WRITE path, which is
    fail-closed via ``require_equivalence_ledger``): an edition without a
    registered ledger, or a transient ledger read failure, degrades to an
    empty mapping — recall keeps working, it just doesn't fold.
    """

    cached = _FOLD_CACHE.get(board_id)
    if cached is not None:
        return cached
    ledger = resolve_equivalence_ledger()
    if ledger is None:
        return {}
    try:
        records = run_async_blocking(ledger.active_for_board(board_id))
    except Exception as exc:
        logger.warning(
            "kg.equivalence.fold_load_failed board=%s err=%s", board_id, exc,
        )
        return {}
    mapping: dict[str, str] = {}
    for record in records:
        for member in record.merged_ids:
            mapping[member] = record.survivor_id
    frozen: Mapping[str, str] = dict(mapping)
    _FOLD_CACHE[board_id] = frozen
    return frozen


def fold_id(node_id: Any, mapping: Mapping[str, str]) -> Any:
    """Rewrite a member id to its survivor; anything else passes through."""

    if isinstance(node_id, str) and node_id in mapping:
        return mapping[node_id]
    return node_id


def fold_rows(
    rows: list[dict],
    mapping: Mapping[str, str],
    *,
    id_keys: Iterable[str],
    dedupe_key: str | None = None,
    score_key: str | None = None,
) -> list[dict]:
    """Fold member ids in dict rows (pure — FR6/TR5).

    Rewrites every key in ``id_keys`` through ``mapping``. When
    ``dedupe_key`` is given, rows that collapse onto the same value keep
    only the best one (highest ``score_key`` when provided, else the first
    — input order preserved otherwise).
    """

    if not mapping:
        return rows
    folded: list[dict] = []
    best_by_key: dict[Any, int] = {}
    for row in rows:
        new_row = dict(row)
        for key in id_keys:
            if key in new_row:
                new_row[key] = fold_id(new_row[key], mapping)
        if dedupe_key is None:
            folded.append(new_row)
            continue
        dk = new_row.get(dedupe_key)
        if dk not in best_by_key:
            best_by_key[dk] = len(folded)
            folded.append(new_row)
        elif score_key is not None:
            idx = best_by_key[dk]
            if (new_row.get(score_key) or 0) > (folded[idx].get(score_key) or 0):
                folded[idx] = new_row
    return folded


def fold_pair_rows(
    rows: list[dict],
    mapping: Mapping[str, str],
    *,
    key_a: str,
    key_b: str,
) -> list[dict]:
    """Fold contradiction-style pair rows: rewrite both endpoints, drop
    pairs that collapse onto themselves (a member never "contradicts" its
    own survivor) and dedupe the folded pairs order-insensitively."""

    if not mapping:
        return rows
    out: list[dict] = []
    seen: set[frozenset] = set()
    for row in rows:
        new_row = dict(row)
        new_row[key_a] = fold_id(new_row.get(key_a), mapping)
        new_row[key_b] = fold_id(new_row.get(key_b), mapping)
        if new_row[key_a] == new_row[key_b]:
            continue
        pair = frozenset((new_row[key_a], new_row[key_b]))
        if pair in seen:
            continue
        seen.add(pair)
        out.append(new_row)
    return out


def fold_result_values(
    rows: list[list],
    mapping: Mapping[str, str],
) -> list[list]:
    """Value-level fold for tabular results (natural query): any cell whose
    value is a member id is rewritten to the survivor; rows that become
    byte-identical after the rewrite are deduped preserving order."""

    if not mapping:
        return rows
    out: list[list] = []
    seen: set[tuple] = set()
    for row in rows:
        new_row = [fold_id(cell, mapping) for cell in row]
        key = tuple(str(c) for c in new_row)
        if key in seen:
            continue
        seen.add(key)
        out.append(new_row)
    return out
