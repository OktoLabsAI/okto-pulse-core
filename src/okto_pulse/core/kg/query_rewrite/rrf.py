"""Reciprocal Rank Fusion helper.

Cormack, Clarke, Buettcher (2009). Given N rankings of the same
universe, produces a combined ranking where the score of each item is
the sum of 1/(k + rank_i) across every ranking it appears in. Items
appearing well-ranked across many rankings dominate; items appearing
once with a low rank fall to the tail.

Accepts heterogeneous candidate shapes: objects with `.node_id` or
dicts with key 'node_id' — picks the first it finds.
"""

from __future__ import annotations

from typing import Iterable, Sequence


def _id_of(item: object) -> str | None:
    """Extract node_id from a candidate. Returns None when nothing
    usable is found (the item is silently dropped from the merge)."""
    if item is None:
        return None
    nid = getattr(item, "node_id", None)
    if isinstance(nid, str) and nid:
        return nid
    if isinstance(item, dict):
        nid = item.get("node_id")
        if isinstance(nid, str) and nid:
            return nid
    return None


def merge_rrf(
    rankings: Iterable[Sequence[object]],
    *,
    k: int = 60,
) -> list[object]:
    """Merge N rankings via Reciprocal Rank Fusion.

    Args:
        rankings: An iterable of ranked sequences. Each sequence is
            treated as a ranking in the order given (position 0 = best).
        k: Smoothing constant of the RRF formula. The Cormack paper
            uses 60; smaller values give more weight to top ranks.

    Returns:
        A list of items sorted by descending RRF score. Ties are
        broken by first-appearance order (stable for snapshot tests).
        Items without a node_id are dropped.
    """
    scores: dict[str, float] = {}
    first_seen: dict[str, object] = {}
    order_seen: dict[str, int] = {}
    _counter = 0

    for ranking in rankings:
        for rank, item in enumerate(ranking, start=1):
            nid = _id_of(item)
            if nid is None:
                continue
            scores[nid] = scores.get(nid, 0.0) + 1.0 / (k + rank)
            if nid not in first_seen:
                first_seen[nid] = item
                order_seen[nid] = _counter
                _counter += 1

    ordered_ids = sorted(
        scores.keys(),
        key=lambda nid: (-scores[nid], order_seen[nid]),
    )
    return [first_seen[nid] for nid in ordered_ids]
