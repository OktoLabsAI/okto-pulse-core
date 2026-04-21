# KG Health & Observability

## Relevance Scoring (v0.3.0)

The Knowledge Graph layer assigns every node a continuous
``relevance_score`` in ``[0.0, 1.5]`` that replaces the binary
``validation_status`` enum removed in R1.

### Formula

```
relevance = clamp(
    0.4 * source_confidence
  + 0.3 * log(1 + degree) / log(100)
  + 0.3 * decayed_hits
  - contradict_penalty
, 0.0, 1.5)
```

where:

| Input                | Source                                                  |
|----------------------|---------------------------------------------------------|
| `source_confidence`  | Set by the extraction worker on insert (e.g. 0.9)       |
| `degree`             | `in_degree + out_degree` at query time                  |
| `decayed_hits`       | `query_hits * exp(-ln(2) * days_since_last_query / 30)` |
| `contradict_penalty` | `SUM(COALESCE(r.confidence, 0.5))` for incoming :contradicts |

### Colour buckets (UI)

| Bucket | Range      | Colour | Meaning                         |
|--------|------------|--------|---------------------------------|
| high   | `>= 0.7`   | green  | Highly relevant / often queried |
| mid    | `0.3..0.7` | amber  | Neutral — new or moderately used |
| low    | `< 0.3`    | red    | Demoted — contradicted or stale |

### Boosting

Endpoint ``POST /api/v1/kg/boards/{board_id}/nodes/{node_id}/boost`` adds a
fixed ``+0.3`` to the current score (clamped at 1.5) and writes an audit
row to ``ConsolidationAudit`` with ``event_type='kg.node.boosted'``.
Calls stack — two clicks sum to ``+0.6`` — because repeat intent should
reflect repeat trust.

The UI exposes a **Boost** button on ``NodeDetailPanel`` (green, next to
**Find Similar** and **Show History**) that updates the badge
optimistically and reverts on network error.

### Decay

The decay factor is applied *on read* inside
``kg_service._recompute_relevance`` — the score persisted in Kùzu is
always the "raw" value. Ranking queries (R3 adds ``ORDER BY
relevance_score DESC``) therefore reflect the instantaneous decayed
ordering, but the UI ``RelevanceBadge`` shows the persisted score.

Half-life: 30 days. After 30 days a node with 10 hits decays to ~5; after
60d to ~2.5; after 90d to ~1.25.

### Filtering

Every read-side tool (Cypher templates, ``hybrid_search``,
``find_similar``, ``get_decision_history``, etc.) filters by
``n.relevance_score >= $min_relevance`` with a default threshold of
``0.3``. Callers that need to retrieve every node (including newly
created nodes with score 0.5) can pass ``min_relevance=0.0``.

### Related specs

* R1 — ``a3023ed2`` — removed ``validation_status`` from the DDL and
  introduced the three new columns ``relevance_score / query_hits /
  last_queried_at``
* R2 — ``620fa859`` — added the scoring formula, hit counter with lazy
  flush (10-hit batch or 24-hour age trigger), UNWIND batch recompute
  and the in-process histogram
* R3 — ``7deb9ec8`` — swapped every Cypher filter, plugged the hit hook
  into ``hybrid_search`` top-K, created ``POST /boost`` and the
  ``RelevanceBadge`` UI

## Metrics

The in-process histogram ``RELEVANCE_HISTOGRAM`` (in
``okto_pulse.core.kg.scoring``) carries bucket counters keyed by
``(board_id, node_type)``. Buckets ``[0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2,
1.5]`` cover the full clamped range. Use
``get_histogram_snapshot()`` from an admin/metrics endpoint to export.
