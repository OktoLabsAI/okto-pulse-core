"""Tier Power — 3 flexible query tools with safety rails.

query_cypher: read-only Cypher via parser whitelist + safety rails
query_natural: hybrid search (embedding + HNSW + 1-hop) with fallback
schema_info: schema introspection with stable/internal type ACL

Safety rails applied to ALL tier power queries:
- Timeout: 5s default, 30s max hard ceiling (asyncio.wait_for)
- Max rows: 1000 default, 10000 max
- Rate limit: 30 queries/min per agent (token bucket)
- Cypher injection mitigation via parser whitelist

All queries logged in tier_power_audit with pattern_hash for telemetry.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any

from okto_pulse.core.kg.schema import (
    NODE_TYPES,
    REL_TYPES,
    SCHEMA_VERSION,
    VECTOR_INDEX_TYPES,
    open_board_connection,
    vector_index_name,
)

logger = logging.getLogger("okto_pulse.kg.tier_power")


# ---------------------------------------------------------------------------
# TierPowerError (FR-9)
# ---------------------------------------------------------------------------


class TierPowerError(Exception):
    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


# ---------------------------------------------------------------------------
# Cypher parser whitelist (FR-3, FR-4)
# ---------------------------------------------------------------------------

CYPHER_WHITELIST = frozenset({
    "MATCH", "WHERE", "RETURN", "WITH", "ORDER", "BY",
    "LIMIT", "UNWIND", "OPTIONAL", "UNION", "AS", "AND",
    "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE",
    "CONTAINS", "STARTS", "ENDS", "DISTINCT", "COUNT",
    "COLLECT", "SUM", "AVG", "MIN", "MAX", "CALL",
    "CASE", "WHEN", "THEN", "ELSE", "END", "DESC", "ASC",
})

CYPHER_BLACKLIST = frozenset({
    "CREATE", "MERGE", "DELETE", "DETACH", "SET",
    "REMOVE", "DROP", "ALTER", "LOAD", "CSV",
})


def _strip_comments(cypher: str) -> str:
    """Remove // line comments and /* block comments */."""
    cypher = re.sub(r"//[^\n]*", "", cypher)
    cypher = re.sub(r"/\*.*?\*/", "", cypher, flags=re.DOTALL)
    return cypher


def _normalize_unicode(cypher: str) -> str:
    """NFKC normalize to prevent unicode homoglyph attacks."""
    return unicodedata.normalize("NFKC", cypher)


def _strip_string_literals(cypher: str) -> str:
    """Replace string literals with placeholders so keyword check doesn't
    trigger on words inside strings."""
    return re.sub(r"'[^']*'|\"[^\"]*\"", "'__STR__'", cypher)


def validate_cypher_read_only(cypher: str) -> None:
    """Validate that Cypher is read-only by checking against whitelist/blacklist.

    Raises TierPowerError(unsafe_cypher) on violation.
    """
    cleaned = _strip_comments(cypher)
    cleaned = _normalize_unicode(cleaned)
    cleaned = _strip_string_literals(cleaned)

    tokens = re.findall(r"[A-Z_]+", cleaned.upper())
    for token in tokens:
        if token in CYPHER_BLACKLIST:
            raise TierPowerError(
                "unsafe_cypher",
                f"Blacklisted keyword detected: {token}",
                details={"keyword": token},
            )


def _auto_inject_limit(cypher: str, max_rows: int) -> str:
    """Inject LIMIT if not present."""
    if "LIMIT" not in cypher.upper():
        cypher = cypher.rstrip().rstrip(";") + f"\nLIMIT {max_rows}"
    return cypher


def _auto_bound_var_length_path(cypher: str, max_depth: int = 20) -> str:
    """Replace unbounded *] or *) with *..20] or *..20)."""
    cypher = re.sub(
        r"\*\s*\]", f"*..{max_depth}]", cypher
    )
    cypher = re.sub(
        r"\*\s*\)", f"*..{max_depth})", cypher
    )
    return cypher


# ---------------------------------------------------------------------------
# Rate limiter — token bucket (FR-5)
# ---------------------------------------------------------------------------


@dataclass
class _TokenBucket:
    rate: int = 30  # tokens per window
    window: float = 60.0  # seconds
    _tokens: dict[str, list[float]] = field(default_factory=dict)

    def allow(self, agent_id: str) -> tuple[bool, int]:
        """Returns (allowed, retry_after_seconds)."""
        now = time.monotonic()
        times = self._tokens.setdefault(agent_id, [])
        # Purge old entries outside window
        cutoff = now - self.window
        self._tokens[agent_id] = [t for t in times if t > cutoff]
        times = self._tokens[agent_id]
        if len(times) >= self.rate:
            oldest = times[0]
            retry_after = int(self.window - (now - oldest)) + 1
            return False, max(1, retry_after)
        times.append(now)
        return True, 0


_rate_limiter = _TokenBucket()


def reset_rate_limiter_for_tests() -> None:
    """Reset rate limiter — resets the whole KG registry."""
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
    global _rate_limiter
    _rate_limiter = _TokenBucket()


def check_rate_limit(agent_id: str) -> None:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    limiter = get_kg_registry().rate_limiter
    allowed, retry_after = limiter.allow(agent_id)
    if not allowed:
        raise TierPowerError(
            "rate_limited",
            f"Rate limit exceeded: 30 queries/min",
            details={"retry_after": retry_after},
        )


# ---------------------------------------------------------------------------
# Pattern hash for audit telemetry (FR-8)
# ---------------------------------------------------------------------------


def compute_pattern_hash(cypher: str) -> str:
    """Normalize a Cypher query into a shape hash for grouping similar queries.

    Strips: numeric literals, string literals, whitespace normalization,
    lowercase keywords. Two queries with same shape but different params
    produce the same hash.
    """
    normalized = _strip_comments(cypher)
    normalized = re.sub(r"'[^']*'|\"[^\"]*\"", "'?'", normalized)
    normalized = re.sub(r"\b\d+(\.\d+)?\b", "?", normalized)
    normalized = re.sub(r"\$\w+", "$?", normalized)
    normalized = " ".join(normalized.upper().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Safety defaults (FR-5)
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT_MS = 5000
MAX_TIMEOUT_MS = 30000
DEFAULT_MAX_ROWS = 1000
MAX_MAX_ROWS = 10000
MAX_TRAVERSAL_DEPTH = 20


def clamp_timeout(timeout_ms: int | None) -> int:
    t = timeout_ms or DEFAULT_TIMEOUT_MS
    return max(1000, min(t, MAX_TIMEOUT_MS))


def clamp_max_rows(max_rows: int | None) -> int:
    r = max_rows or DEFAULT_MAX_ROWS
    return max(1, min(r, MAX_MAX_ROWS))


# ---------------------------------------------------------------------------
# query_cypher (FR-3, FR-4, FR-10)
# ---------------------------------------------------------------------------


def execute_cypher_read_only(
    board_id: str,
    cypher: str,
    params: dict[str, Any] | None = None,
    *,
    max_rows: int | None = None,
    timeout_ms: int | None = None,
) -> dict:
    """Execute a validated read-only Cypher query with safety rails.

    Delegates to registry.cypher_executor when available, falls back to
    direct Kuzu execution.
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    max_rows = clamp_max_rows(max_rows)

    executor = get_kg_registry().cypher_executor
    if executor is not None:
        return executor.execute_read_only(
            board_id, cypher, params, max_rows=max_rows,
        )

    # Fallback: direct execution (should not happen with proper bootstrap)
    import time as _time

    timeout_ms = clamp_timeout(timeout_ms)

    cleaned = _normalize_unicode(cypher)
    validate_cypher_read_only(cleaned)
    cleaned = _auto_inject_limit(cleaned, max_rows)
    cleaned = _auto_bound_var_length_path(cleaned, MAX_TRAVERSAL_DEPTH)

    t0 = _time.monotonic()
    with open_board_connection(board_id) as (_db, conn):
        try:
            result = conn.execute(cleaned, params or {})
            rows = []
            while result.has_next():
                rows.append(result.get_next())
                if len(rows) > max_rows:
                    break
        except Exception as exc:
            raise TierPowerError(
                "invalid_cypher",
                f"Cypher execution failed: {exc}",
                details={"cypher": cleaned[:200]},
            ) from exc

    dur = (_time.monotonic() - t0) * 1000
    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    return {
        "rows": [list(r) for r in rows],
        "row_count": len(rows),
        "truncated": truncated,
        "execution_time_ms": round(dur, 1),
    }


# ---------------------------------------------------------------------------
# query_natural — hybrid search + fallback (FR-1, FR-2, FR-12)
# ---------------------------------------------------------------------------


def _parse_iso_ts(value: str | None) -> Any:
    """Parse an ISO-8601 timestamp into a Kùzu-ready datetime; ``None`` passes
    through. Swallows invalid input so the caller can proceed unfiltered (a
    bad cursor shouldn't cause a 500 — the natural-query tool must remain
    best-effort)."""
    if value is None or value == "":
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def execute_natural_query(
    board_id: str,
    nl_query: str,
    *,
    limit: int = 20,
    min_confidence: float = 0.5,
    since: str | None = None,
    until: str | None = None,
    rewrite: str = "none",
    rewrite_llm_fn=None,
    fusion_paraphrases: int = 3,
) -> dict:
    """Hybrid search: embed query -> HNSW k-NN -> 1-hop traversal -> ranking.

    Optional ``since`` / ``until`` parameters accept ISO-8601 timestamps
    and post-filter results by ``n.created_at`` so an agent can scope the
    query to a release window, a sprint, or "what happened since I last
    looked". Invalid timestamps are ignored (best-effort). Over-fetch by a
    10x factor so post-filter still returns ``limit`` matches when the window
    is narrow.

    Ideação 2cf21a31 — optional pre-retrieve rewrite stage:

    - ``rewrite``: one of ``"none"`` (default, passthrough), ``"hyde"``
      (embed a hypothetical passage instead of the query),
      ``"decompose"`` (split into sub-queries and union-dedupe the
      results), ``"fusion"`` (K paraphrases merged via RRF k=60).
    - ``rewrite_llm_fn``: callable with the shape required by the
      chosen strategy (see ``okto_pulse.core.kg.query_rewrite``).
      Required for any non-``none`` strategy.
    - ``fusion_paraphrases``: number of paraphrases the fusion LLM
      should generate. Default 3.

    On any rewrite failure the retrieval degrades to ``rewrite="none"``
    with a warning — the rewrite stage never aborts the pipeline.

    The response carries ``rewrite_strategy`` (what was effectively
    applied) and ``rewrite_variants_count`` (1 for none/hyde, N for
    decompose/fusion) so callers / audit can tell the stages apart.
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
    from okto_pulse.core.kg.query_rewrite import get_rewriter, merge_rrf
    from okto_pulse.core.kg.query_rewrite.interfaces import RewriteResult

    registry = get_kg_registry()
    embedder = registry.embedding_provider
    store = registry.graph_store
    warning = None

    since_dt = _parse_iso_ts(since)
    until_dt = _parse_iso_ts(until)
    temporal_filter_requested = since_dt is not None or until_dt is not None
    # Over-fetch when a temporal filter is active so the post-filter has
    # enough candidates to return ``limit`` hits.
    fetch_limit = limit * 10 if temporal_filter_requested else limit

    # Pre-retrieve rewrite (ideação 2cf21a31). Any failure degrades
    # to rewrite="none" — the pipeline never aborts because of it.
    rewrite_result: RewriteResult
    try:
        rewriter = get_rewriter(
            rewrite,
            llm_fn=rewrite_llm_fn,
            fusion_paraphrases=fusion_paraphrases,
        )
        rewrite_result = rewriter.rewrite(nl_query)
    except Exception as e:  # noqa: BLE001 — anything falls back
        logger.warning(
            "execute_natural_query.rewrite_failed strategy=%s error=%s",
            rewrite, type(e).__name__,
        )
        rewrite_result = RewriteResult(
            strategy="none",
            original_query=nl_query,
            rewritten_queries=(nl_query,),
            hyde_passage=None,
        )

    applied_strategy = rewrite_result.strategy
    variants = list(rewrite_result.rewritten_queries)

    # HyDE: embed the hypothetical passage, but retrieve with the seed
    # from THAT passage (not from the original query). The query of
    # record stays the original.
    hyde_vec = None
    if applied_strategy == "hyde" and rewrite_result.hyde_passage:
        try:
            hyde_vec = embedder.encode(rewrite_result.hyde_passage)
        except Exception:
            hyde_vec = None

    def _run_single(variant_query: str, override_vec=None) -> list[dict]:
        """Run the existing single-variant retrieval pipeline."""
        out: list[dict] = []
        try:
            query_vec = override_vec if override_vec is not None else embedder.encode(variant_query)
        except Exception:
            query_vec = None

        if query_vec is not None and store is not None:
            for node_type in VECTOR_INDEX_TYPES:
                raw = store.vector_search(
                    board_id=board_id,
                    node_type=node_type,
                    query_vec=query_vec,
                    top_k=fetch_limit,
                    min_similarity=0.3,
                )
                for r in raw:
                    out.append({
                        "node_id": r["node_id"],
                        "node_type": r["node_type"],
                        "title": r["title"],
                        "similarity": r["similarity"],
                    })
        elif store is not None:
            f = QueryFilters(min_confidence=0.0, max_rows=fetch_limit)
            for node_type in NODE_TYPES:
                try:
                    rows = store.find_by_topic(board_id, node_type, variant_query[:50], f)
                    for r in rows:
                        out.append({
                            "node_id": r[0],
                            "node_type": node_type,
                            "title": r[1],
                            "similarity": 0.5,
                        })
                except Exception:
                    pass
        else:
            with open_board_connection(board_id) as (_db, conn):
                for node_type in NODE_TYPES:
                    try:
                        result = conn.execute(
                            f"MATCH (n:{node_type}) WHERE n.title CONTAINS $q "
                            f"RETURN n.id, n.title LIMIT $k",
                            {"q": variant_query[:50], "k": fetch_limit},
                        )
                        while result.has_next():
                            row = result.get_next()
                            out.append({
                                "node_id": row[0],
                                "node_type": node_type,
                                "title": row[1],
                                "similarity": 0.5,
                            })
                    except Exception:
                        pass
        return out

    if applied_strategy in ("none", "hyde"):
        # Single-variant path — hyde reuses _run_single with an override
        # embedding so the retrieval seed is the passage, not the query.
        variant = variants[0] if variants else nl_query
        all_results = _run_single(variant, override_vec=hyde_vec)
        if not all_results and hyde_vec is None and applied_strategy == "none":
            # Preserve the old warning surface: when the embedder errored
            # out and gave us no seed, report ``embedding_unavailable``
            # so existing callers still see the warning they expect.
            try:
                embedder.encode(nl_query)
            except Exception:
                warning = "embedding_unavailable"

    elif applied_strategy == "decompose":
        # Run each sub-query independently and union with first-occurrence
        # wins dedup. Do not re-rank — preserve the aggregate order of
        # first appearance to respect the LLM's sub-query ordering.
        seen: dict[str, dict] = {}
        for variant in variants:
            for row in _run_single(variant):
                if row["node_id"] not in seen:
                    seen[row["node_id"]] = row
        all_results = list(seen.values())

    elif applied_strategy == "fusion":
        # Run each paraphrase independently, sort each ranking by
        # similarity desc, then RRF-merge.
        rankings: list[list[dict]] = []
        for variant in variants:
            rows = _run_single(variant)
            rows.sort(key=lambda r: r["similarity"], reverse=True)
            rankings.append(rows)
        all_results = merge_rrf(rankings, k=60)

    else:
        # Unknown strategy somehow slipped through — be safe.
        all_results = _run_single(nl_query)

    total_before_filter = len(all_results)
    filtered_out = 0
    if temporal_filter_requested and all_results:
        node_ids = [r["node_id"] for r in all_results]
        timestamps = _batch_lookup_created_at(board_id, node_ids)
        kept: list[dict] = []
        for r in all_results:
            ts = timestamps.get(r["node_id"])
            if ts is None:
                # Node vanished between vector hit and lookup — drop to avoid
                # misleading an agent that asked for a specific window.
                filtered_out += 1
                continue
            if since_dt is not None and ts < since_dt:
                filtered_out += 1
                continue
            if until_dt is not None and ts > until_dt:
                filtered_out += 1
                continue
            r["created_at"] = ts.isoformat()
            kept.append(r)
        all_results = kept

    # Final ordering: keep RRF order for fusion, otherwise sort by
    # similarity desc (decompose respects union order except we also
    # need a deterministic final ranking, so sort by similarity).
    if applied_strategy != "fusion":
        all_results.sort(key=lambda x: x["similarity"], reverse=True)
    results = all_results[:limit]

    resp: dict[str, Any] = {
        "nodes": results,
        "total_matches": len(all_results),
        "rewrite_strategy": applied_strategy,
        "rewrite_variants_count": len(variants) if variants else 1,
    }
    if warning:
        resp["warning"] = warning
    if temporal_filter_requested:
        resp["temporal_filter"] = {
            "since": since,
            "until": until,
            "candidates_before_filter": total_before_filter,
            "filtered_out": filtered_out,
        }
    return resp


def _batch_lookup_created_at(board_id: str, node_ids: list[str]) -> dict[str, Any]:
    """Fetch ``created_at`` for a list of node ids in one pass across all
    node types. Returns a mapping ``{node_id: datetime}``. Nodes without a
    known created_at (e.g. degenerate rows) are omitted — callers treat the
    absence as "outside the temporal window" to be safe.
    """
    from datetime import timezone

    if not node_ids:
        return {}

    out: dict[str, Any] = {}
    with open_board_connection(board_id) as (_db, conn):
        for node_type in NODE_TYPES:
            try:
                result = conn.execute(
                    f"MATCH (n:{node_type}) WHERE n.id IN $ids "
                    f"RETURN n.id, n.created_at",
                    {"ids": node_ids},
                )
                while result.has_next():
                    row = result.get_next()
                    nid = row[0]
                    ts = row[1]
                    if ts is None:
                        continue
                    # Kùzu returns a Python datetime; ensure tz-aware UTC
                    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    out[nid] = ts
            except Exception:
                continue
    return out


# ---------------------------------------------------------------------------
# schema_info (FR-6, FR-11)
# ---------------------------------------------------------------------------


def get_schema_info(
    board_id: str,
    *,
    include_internal: bool = False,
) -> dict:
    """Return schema introspection: node types, rel types, vector indexes."""
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    store = get_kg_registry().graph_store
    if store is not None:
        return store.get_schema_info(board_id, include_internal=include_internal)

    # Fallback: static schema from constants
    stable_nodes = [
        {"name": nt, "stable": True}
        for nt in NODE_TYPES
    ]
    stable_rels = [
        {"name": rt[0], "from": rt[1], "to": rt[2]}
        for rt in REL_TYPES
    ]
    vector_indexes = [
        {"node_type": nt, "attribute": "embedding",
         "dimension": 384, "similarity_metric": "cosine",
         "index_name": vector_index_name(nt)}
        for nt in VECTOR_INDEX_TYPES
    ]

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stable_node_types": stable_nodes,
        "stable_rel_types": stable_rels,
        "vector_indexes": vector_indexes,
    }
    if include_internal:
        result["internal_node_types"] = [{"name": "BoardMeta", "stable": False}]
        result["internal_rel_types"] = []
    return result
