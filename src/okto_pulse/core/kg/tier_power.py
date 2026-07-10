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
import unicodedata
from typing import Any

from okto_pulse.core.kg.schema_contract import (
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
    VECTOR_INDEX_TYPES,
    stable_rel_type_entries,
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


def normalize_cypher_unicode(cypher: str) -> str:
    """Public facade for Cypher unicode normalization."""

    return _normalize_unicode(cypher)


def _strip_string_literals(cypher: str) -> str:
    """Replace string literals with placeholders so keyword check doesn't
    trigger on words inside strings."""
    return re.sub(r"'[^']*'|\"[^\"]*\"", "'__STR__'", cypher)


def _mask_literals_and_comments(cypher: str) -> str:
    """Return a same-length string with comments/literals replaced by spaces."""

    chars = list(cypher)
    index = 0
    quote: str | None = None
    in_line_comment = False
    in_block_comment = False

    while index < len(chars):
        char = chars[index]
        next_char = chars[index + 1] if index + 1 < len(chars) else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            else:
                chars[index] = " "
            index += 1
            continue

        if in_block_comment:
            chars[index] = " "
            if char == "*" and next_char == "/":
                chars[index + 1] = " "
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if quote:
            chars[index] = " "
            if char == "\\":
                if index + 1 < len(chars):
                    chars[index + 1] = " "
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue

        if char in {"'", '"'}:
            chars[index] = " "
            quote = char
            index += 1
            continue
        if char == "/" and next_char == "/":
            chars[index] = " "
            chars[index + 1] = " "
            in_line_comment = True
            index += 2
            continue
        if char == "/" and next_char == "*":
            chars[index] = " "
            chars[index + 1] = " "
            in_block_comment = True
            index += 2
            continue

        index += 1

    return "".join(chars)


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
    searchable = _strip_string_literals(_strip_comments(cypher)).upper()
    if not re.search(r"\bLIMIT\b", searchable):
        cypher = cypher.rstrip().rstrip(";") + f"\nLIMIT {max_rows}"
    return cypher


def auto_inject_limit(cypher: str, max_rows: int) -> str:
    """Public facade for Tier Power LIMIT injection policy."""

    return _auto_inject_limit(cypher, max_rows)


def _bound_relationship_segment(segment: str, max_depth: int) -> str:
    """Bound variable-length relationship syntax inside one [...] segment."""
    star_index = segment.find("*")
    if star_index == -1:
        return segment

    cursor = star_index + 1
    while cursor < len(segment) and segment[cursor].isspace():
        cursor += 1

    if cursor >= len(segment):
        return f"{segment[:cursor]}..{max_depth}{segment[cursor:]}"

    if segment[cursor] == "." and cursor + 1 < len(segment) and segment[cursor + 1] == ".":
        upper_start = cursor + 2
        cursor = upper_start
        while cursor < len(segment) and segment[cursor].isspace():
            cursor += 1
        upper_digits_start = cursor
        while cursor < len(segment) and segment[cursor].isdigit():
            cursor += 1
        if cursor > upper_digits_start:
            return segment
        return f"{segment[:upper_start]}{max_depth}{segment[upper_start:]}"

    if segment[cursor].isdigit():
        while cursor < len(segment) and segment[cursor].isdigit():
            cursor += 1
        while cursor < len(segment) and segment[cursor].isspace():
            cursor += 1
        if cursor + 1 < len(segment) and segment[cursor:cursor + 2] == "..":
            upper_start = cursor + 2
            cursor = upper_start
            while cursor < len(segment) and segment[cursor].isspace():
                cursor += 1
            upper_digits_start = cursor
            while cursor < len(segment) and segment[cursor].isdigit():
                cursor += 1
            if cursor == upper_digits_start:
                return f"{segment[:upper_start]}{max_depth}{segment[upper_start:]}"
        return segment

    return f"{segment[:star_index + 1]}..{max_depth}{segment[star_index + 1:]}"


def _auto_bound_var_length_path(cypher: str, max_depth: int = 20) -> str:
    """Bound unbounded variable-length relationships without touching functions.

    Cypher uses ``*`` for both aggregate arguments (``count(*)``) and
    relationship traversal lengths (``-[*]->``). Only the latter appears inside
    relationship brackets, so this walks bracket segments instead of applying a
    broad ``*)`` regex.
    """
    parts: list[str] = []
    start = 0
    index = 0
    quote: str | None = None
    in_line_comment = False
    in_block_comment = False

    while index < len(cypher):
        char = cypher[index]
        next_char = cypher[index + 1] if index + 1 < len(cypher) else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if quote:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if char == "/" and next_char == "/":
            in_line_comment = True
            index += 2
            continue
        if char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue
        if char != "[":
            index += 1
            continue

        close_index = cypher.find("]", index + 1)
        if close_index == -1:
            index += 1
            continue

        parts.append(cypher[start:index + 1])
        segment = cypher[index + 1:close_index]
        parts.append(_bound_relationship_segment(segment, max_depth))
        start = close_index
        index = close_index + 1

    if not parts:
        return cypher
    parts.append(cypher[start:])
    return "".join(parts)


def auto_bound_var_length_path(cypher: str, max_depth: int = 20) -> str:
    """Public facade for Tier Power variable-length path bounding."""

    return _auto_bound_var_length_path(cypher, max_depth)


# ---------------------------------------------------------------------------
# Rate limiter — token bucket (FR-5)
# ---------------------------------------------------------------------------
#
# R03 IMP1 (FR2/AC2): the in-memory token bucket lives ONLY behind the
# RateLimiter port — the canonical concrete is
# ``community.adapters.memory.CommunityInMemoryRateLimiter`` (the Community
# edition composes ``CommunityInMemoryRateLimiter``). The duplicate module-global
# ``_TokenBucket`` / ``_rate_limiter`` that used to live here was vestigial — the
# runtime resolves the slot through ``require_rate_limiter()`` — so it is removed,
# together with its ``BASELINE_SINGLETONS`` ledger entry. Token-bucket
# semantics (30 tokens / 60s window per agent) are unchanged.


def reset_rate_limiter_for_tests() -> None:
    """Reset the rate limiter — drops the whole KG registry (tests only)."""
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()


def check_rate_limit(agent_id: str) -> None:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    # AC3 (base fail-closed): read the rate limiter through the required port —
    # an unregistered slot raises ``runtime_provider_missing`` instead of a late
    # ``AttributeError`` on ``None`` or a silent concrete fallback.
    limiter = get_kg_registry().require_rate_limiter()
    allowed, retry_after = limiter.allow(agent_id)
    if not allowed:
        raise TierPowerError(
            "rate_limited",
            "Rate limit exceeded: 30 queries/min",
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


def _extract_match_node_vars(pattern_part: str) -> tuple[list[str], bool]:
    """Return node variables found in a MATCH pattern and whether any node is anonymous.

    This intentionally covers the supported Tier Power subset. When a query uses
    anonymous nodes under canonical-only mode we fail closed instead of returning
    possibly working rows or counts.
    """

    variables: list[str] = []
    anonymous = False
    for match in re.finditer(r"\(([^()]*)\)", pattern_part):
        content = match.group(1).strip()
        if not content or content.startswith(":") or content.startswith("{"):
            anonymous = True
            continue
        first = re.split(r"[\s:{]", content, maxsplit=1)[0].strip()
        if not first or "." in first or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", first):
            continue
        variables.append(first)
    return sorted(set(variables)), anonymous


def _find_clause_end(cypher: str, start: int) -> int:
    masked = _mask_literals_and_comments(cypher)
    boundary_re = re.compile(
        r"\b(OPTIONAL\s+MATCH|MATCH|WITH|RETURN|UNION|ORDER\s+BY|ORDER|LIMIT|CALL)\b",
        flags=re.IGNORECASE,
    )
    for boundary in boundary_re.finditer(masked, start):
        keyword = " ".join(boundary.group(1).upper().split())
        if keyword == "WITH":
            prefix = masked[:boundary.start()].rstrip()
            previous = re.search(r"([A-Za-z_][A-Za-z0-9_]*)$", prefix)
            if previous and previous.group(1).upper() in {"STARTS", "ENDS"}:
                continue
        return boundary.start()
    return len(cypher)


def _canonical_filter_for_vars(variables: list[str]) -> str:
    return " AND ".join(f"{var}.graph_layer = 'canonical'" for var in variables)


def _rewrite_cypher_canonical_only(cypher: str) -> tuple[str, str]:
    """Inject graph_layer predicates into supported MATCH clauses.

    If the query shape cannot be filtered safely, raise instead of allowing a
    working leak. This is deliberately stricter than row projection because
    arbitrary ``MATCH (n) RETURN n`` rows may not expose ``graph_layer`` after
    driver normalisation.
    """

    match_iter = list(re.finditer(r"\b(?:OPTIONAL\s+MATCH|MATCH)\b", cypher, re.IGNORECASE))
    if not match_iter:
        return cypher, "no_match"

    out: list[str] = []
    cursor = 0
    for idx, match in enumerate(match_iter):
        out.append(cypher[cursor:match.end()])
        body_start = match.end()
        next_match_start = match_iter[idx + 1].start() if idx + 1 < len(match_iter) else len(cypher)
        clause_end = min(_find_clause_end(cypher, body_start), next_match_start)
        clause_body = cypher[body_start:clause_end]
        where_match = re.search(r"\bWHERE\b", clause_body, re.IGNORECASE)
        pattern_part = clause_body[:where_match.start()] if where_match else clause_body
        variables, has_anonymous = _extract_match_node_vars(pattern_part)
        if re.search(r"\[[^\]]*\*", pattern_part):
            raise TierPowerError(
                "canonical_filter_unenforceable",
                "Canonical-only query uses variable-length traversal; name and bound every traversed node or pass include_working=true.",
                details={"filter_mode": "cypher_rewrite"},
            )
        if has_anonymous:
            raise TierPowerError(
                "canonical_filter_unenforceable",
                "Canonical-only query uses anonymous node patterns; name every node or pass include_working=true.",
                details={"filter_mode": "cypher_rewrite"},
            )
        if not variables:
            out.append(clause_body)
        else:
            canonical_filter = _canonical_filter_for_vars(variables)
            if where_match:
                original_where = clause_body[where_match.end():].strip()
                out.append(
                    f"{pattern_part}WHERE {canonical_filter} AND ({original_where}) "
                )
            else:
                out.append(f"{pattern_part}WHERE {canonical_filter} ")
        cursor = clause_end

    out.append(cypher[cursor:])
    return "".join(out), "cypher_rewrite"


def _apply_canonical_projection(
    result: dict,
    *,
    include_working: bool,
    canonical_filter_mode: str | None = None,
) -> dict:
    rows = list(result.get("rows") or [])
    if include_working:
        return {
            **result,
            "query_state": "canonical_and_working",
            "layers_included": ["canonical", "working"],
            "canonical_filter_enforced": False,
            "working_omitted_count": 0,
        }

    kept: list[Any] = []
    omitted = 0
    saw_layer = False
    for row in rows:
        layer: str | None = None
        if isinstance(row, dict):
            raw = row.get("graph_layer") or row.get("layer")
            if raw is not None:
                layer = str(raw)
        if layer is None:
            kept.append(row)
            continue
        saw_layer = True
        if layer == "working":
            omitted += 1
            continue
        kept.append(row)

    return {
        **result,
        "rows": kept,
        "row_count": len(kept),
        "query_state": "canonical_only",
        "layers_included": ["canonical"],
        "canonical_filter_enforced": bool(canonical_filter_mode or saw_layer),
        "canonical_filter_mode": (
            canonical_filter_mode
            or ("row_projection" if saw_layer else "partial_no_layer_column")
        ),
        "working_omitted_count": omitted,
    }


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
    include_working: bool = False,
) -> dict:
    """Execute a validated read-only Cypher query with safety rails.

    Delegates to registry.cypher_executor. A missing executor is a composition
    error: business-facing query code must never open the graph backend
    directly.
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    logger.debug("[KG] execute_cypher_read_only board_id=%s cypher_len=%d params=%s",
                 board_id, len(cypher), "yes" if params else "no")
    logger.debug("[KG] execute_cypher_read_only cypher=%s", cypher[:200])

    max_rows = clamp_max_rows(max_rows)
    cleaned = _normalize_unicode(cypher)
    validate_cypher_read_only(cleaned)
    cleaned = _auto_inject_limit(cleaned, max_rows)
    cleaned = _auto_bound_var_length_path(cleaned, MAX_TRAVERSAL_DEPTH)
    canonical_filter_mode = None
    if not include_working:
        cleaned, canonical_filter_mode = _rewrite_cypher_canonical_only(cleaned)

    executor = getattr(get_kg_registry(), "cypher_executor", None)
    if executor is not None:
        logger.debug("[KG] execute_cypher_read_only delegating to registry.cypher_executor")
        result = executor.execute_read_only(
            board_id, cleaned, params, max_rows=max_rows,
        )
        return _apply_canonical_projection(
            result,
            include_working=include_working,
            canonical_filter_mode=canonical_filter_mode,
        )

    raise TierPowerError(
        "graph_backend_unconfigured",
        "KG registry is missing cypher_executor; configure the graph query port "
        "in the composition root before using Tier Power.",
        details={"board_id": board_id, "cypher": cleaned[:200]},
    )


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


def _find_literal_node_matches(
    board_id: str,
    query_text: str,
    *,
    limit: int,
) -> list[dict]:
    """Exact/text fallback across every node type.

    Vector search is intentionally index-scoped, so operational nodes such as
    Bug/TestScenario must still be discoverable by exact title, id, or source
    reference even if an HNSW index is absent/stale.
    """
    query_text = (query_text or "").strip()
    if not query_text:
        return []

    out: list[dict] = []
    seen: set[str] = set()

    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    executor = getattr(get_kg_registry(), "cypher_executor", None)
    if executor is None:
        return []

    def _append(row: list[Any], node_type: str, similarity: float) -> None:
        node_id = row[0]
        if not node_id or node_id in seen:
            return
        seen.add(node_id)
        out.append({
            "node_id": node_id,
            "node_type": node_type,
            "title": row[1],
            "source_artifact_ref": row[2] if len(row) > 2 else None,
            "similarity": similarity,
        })

    try:
        for node_type in NODE_TYPES:
            if len(out) >= limit:
                break
            try:
                result = executor.execute_read_only(
                    board_id,
                    f"MATCH (n:{node_type}) "
                    "WHERE n.id = $q "
                    "OR n.title = $q "
                    "OR n.source_artifact_ref = $q "
                    "RETURN n.id, n.title, n.source_artifact_ref "
                    "LIMIT $k",
                    {"q": query_text, "k": max(1, limit - len(out))},
                    max_rows=max(1, limit - len(out)),
                )
                for row in result.get("rows") or []:
                    _append(row, node_type, 1.0)
            except Exception:
                pass

        for node_type in NODE_TYPES:
            if len(out) >= limit:
                break
            try:
                result = executor.execute_read_only(
                    board_id,
                    f"MATCH (n:{node_type}) "
                    "WHERE n.title CONTAINS $q "
                    "OR n.content CONTAINS $q "
                    "OR n.source_artifact_ref CONTAINS $q "
                    "RETURN n.id, n.title, n.source_artifact_ref "
                    "LIMIT $k",
                    {"q": query_text[:200], "k": max(1, limit - len(out))},
                    max_rows=max(1, limit - len(out)),
                )
                for row in result.get("rows") or []:
                    _append(row, node_type, 0.65)
            except Exception:
                pass
    except Exception as exc:
        logger.debug(
            "kg.natural.literal_fallback_failed board=%s err=%s",
            board_id, exc,
        )
    return out[:limit]


def _dedupe_natural_results(rows: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    order: list[str] = []
    for row in rows:
        node_id = row.get("node_id")
        if not node_id:
            continue
        current = best.get(node_id)
        if current is None:
            best[node_id] = row
            order.append(node_id)
            continue
        if row.get("similarity", 0.0) > current.get("similarity", 0.0):
            best[node_id] = {**current, **row}
    return [best[node_id] for node_id in order]


def execute_natural_query(
    board_id: str,
    nl_query: str,
    *,
    limit: int = 20,
    min_confidence: float = 0.5,
    since: str | None = None,
    until: str | None = None,
    graph_layer: str = "canonical",
    rewrite: str = "none",
    rewrite_llm_fn=None,
    fusion_paraphrases: int = 3,
    include_parent_context: bool = False,
    parent_db=None,
    compress_if_over_tokens: int = 0,
    compress_llm_fn=None,
    approx_token_count_fn=None,
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
    # graph_layer contract (spec e2598178): fail-closed at the boundary via the
    # shared normalizer — an invalid value raises BEFORE any retrieval. Reuses
    # normalize_graph_layer (the single layer allowlist) so there is no second
    # vocabulary to drift from query_global/get_related_context.
    from okto_pulse.core.kg.kg_service import KGToolError, normalize_graph_layer

    try:
        graph_layer = normalize_graph_layer(graph_layer)
    except KGToolError as exc:
        raise TierPowerError(exc.code, exc.message, details=exc.details) from exc

    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.interfaces.graph_store import QueryFilters
    from okto_pulse.core.kg.query_rewrite import get_rewriter, merge_rrf
    from okto_pulse.core.kg.query_rewrite.interfaces import RewriteResult

    logger.debug("[KG] execute_natural_query board_id=%s query=%r limit=%d min_confidence=%.2f",
                 board_id, nl_query[:80], limit, min_confidence)

    registry = get_kg_registry()
    embedder = registry.require_embedding_provider()
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
        out: list[dict] = _find_literal_node_matches(
            board_id,
            variant_query,
            limit=fetch_limit,
        )
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
                        "source_artifact_ref": r.get("source_artifact_ref"),
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
                            "source_artifact_ref": None,
                            "similarity": 0.5,
                        })
                except Exception:
                    pass
        else:
            executor = getattr(registry, "cypher_executor", None)
            if executor is not None:
                for node_type in NODE_TYPES:
                    try:
                        result = executor.execute_read_only(
                            board_id,
                            f"MATCH (n:{node_type}) WHERE n.title CONTAINS $q "
                            f"RETURN n.id, n.title LIMIT $k",
                            {"q": variant_query[:50], "k": fetch_limit},
                            max_rows=fetch_limit,
                        )
                        for row in result.get("rows") or []:
                            out.append({
                                "node_id": row[0],
                                "node_type": node_type,
                                "title": row[1],
                                "source_artifact_ref": None,
                                "similarity": 0.5,
                            })
                    except Exception:
                        pass
        return _dedupe_natural_results(out)

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
    all_results = _dedupe_natural_results(all_results)
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

    # graph_layer contract (spec e2598178): attach each result's layer, audit
    # leakage across layers, then filter to the requested layer. Fail-closed —
    # legacy_unknown/metadata only surface under graph_layer='all'.
    all_results, layer_audit = _apply_graph_layer_to_natural_results(
        board_id, all_results, graph_layer
    )

    # Final ordering: fusion preserves RRF; others sort by similarity
    # desc (decompose respects union order except for the final
    # deterministic sort by similarity).
    if applied_strategy != "fusion":
        all_results.sort(key=lambda x: x["similarity"], reverse=True)
    results = all_results[:limit]

    # Parent-doc enrichment (ideação fe55ff7c). Runs AFTER the ordering
    # so we only hit the DB for the top-limit rows. Orphans / malformed
    # refs produce parent_artifact=None without removing the row.
    if include_parent_context:
        try:
            from okto_pulse.core.kg.parent_doc import resolve_parent_artifacts
            import asyncio as _asyncio

            refs = [r.get("source_artifact_ref", "") for r in results]
            refs = [r for r in refs if r]
            parent_map: dict[str, dict] = {}
            if parent_db is not None and refs:
                try:
                    parent_map = _asyncio.get_event_loop().run_until_complete(
                        resolve_parent_artifacts(parent_db, refs)
                    )
                except RuntimeError:
                    # Already inside an event loop — schedule as a task
                    # via a nested run. This happens in async callers.
                    loop = _asyncio.new_event_loop()
                    try:
                        parent_map = loop.run_until_complete(
                            resolve_parent_artifacts(parent_db, refs)
                        )
                    finally:
                        loop.close()
            for r in results:
                r["parent_artifact"] = parent_map.get(
                    r.get("source_artifact_ref", ""),
                )
        except Exception as e:  # noqa: BLE001 — never break on parent lookup
            logger.warning(
                "execute_natural_query.parent_lookup_failed error=%s",
                type(e).__name__,
            )
            for r in results:
                r["parent_artifact"] = None

    # Context compression (ideação fe55ff7c). Gate by opt-in threshold.
    compressed_summary: dict | None = None
    compression_applied = False
    if compress_if_over_tokens > 0 and compress_llm_fn is not None:
        try:
            from okto_pulse.core.kg.context_compress import compress_if_needed

            comp = compress_if_needed(
                results,
                compress_llm_fn=compress_llm_fn,
                max_tokens=compress_if_over_tokens,
                approx_token_count_fn=approx_token_count_fn,
            )
            if comp.applied:
                compressed_summary = {
                    "summary": comp.summary,
                    "compressed_from_nodes": comp.compressed_from_nodes,
                    "approx_original_tokens": comp.approx_original_tokens,
                    "approx_compressed_tokens": comp.approx_compressed_tokens,
                }
                compression_applied = True
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "execute_natural_query.compression_failed error=%s",
                type(e).__name__,
            )

    resp: dict[str, Any] = {
        "nodes": results,
        "total_matches": len(all_results),
        "applied_graph_layer": graph_layer,
        "layer_audit": layer_audit,
        "rewrite_strategy": applied_strategy,
        "rewrite_variants_count": len(variants) if variants else 1,
        "parent_context_included": include_parent_context,
        "compression_applied": compression_applied,
    }
    if compressed_summary is not None:
        resp["compressed_summary"] = compressed_summary
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


# Natural-query layer-audit buckets (spec e2598178, ac_6fabaaec). canonical /
# working are artifact layers; BoardMeta (internal singleton) and any
# NULL/unknown layer are NON-artifact and never count as canonical/working
# leakage.
_NATURAL_LAYER_BUCKETS: tuple[str, ...] = (
    "canonical",
    "working",
    "legacy_unknown",
    "metadata",
)


def _classify_natural_layer(node_type: str, raw_layer: str | None) -> str:
    """Bucket a natural-query result into the layer-audit vocabulary.

    BoardMeta (the internal schema-version singleton) is non-artifact metadata;
    canonical/working pass through; anything else (NULL, 'none', unknown) is the
    conservative ``legacy_unknown`` bucket — never silently canonical/working.
    """
    if node_type == "BoardMeta":
        return "metadata"
    layer = (raw_layer or "").strip().lower()
    if layer == "canonical":
        return "canonical"
    if layer == "working":
        return "working"
    return "legacy_unknown"


def _batch_lookup_graph_layer(board_id: str, node_ids: list[str]) -> dict[str, str]:
    """Fetch graph_layer for node ids in one pass across all node types.

    Reuses the fail-safe projection ``cypher_templates.layer_label_projection``:
    a NULL/absent graph_layer is reported as ``'legacy_unknown'`` — never
    coerced to canonical — so the natural-query layer filter stays fail-closed.
    """
    if not node_ids:
        return {}
    from okto_pulse.core.kg import cypher_templates as tpl
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    out: dict[str, str] = {}
    executor = getattr(get_kg_registry(), "cypher_executor", None)
    if executor is None:
        return out
    for node_type in NODE_TYPES:
        try:
            result = executor.execute_read_only(
                board_id,
                f"MATCH (n:{node_type}) WHERE n.id IN $ids "
                f"RETURN n.id, {tpl.layer_label_projection('n')}",
                {"ids": node_ids},
                max_rows=max(len(node_ids), 1),
            )
            for row in result.get("rows") or []:
                out[row[0]] = str(row[1] or "legacy_unknown")
        except Exception:
            continue
    return out


def _apply_graph_layer_to_natural_results(
    board_id: str, rows: list[dict], graph_layer: str
) -> tuple[list[dict], dict]:
    """Attach each result's graph_layer bucket, audit leakage across layers, and
    filter to the requested layer.

    Fail-closed: under ``canonical``/``working`` only that artifact layer is
    kept; ``legacy_unknown``/``metadata`` surface ONLY under ``all``. The
    ``layer_audit.counts_by_layer`` is computed over the FULL retrieved set
    (pre-filter) so an agent can see what was matched across layers; metadata
    and legacy_unknown are reported separately and never folded into the
    canonical/working leakage counts.
    """
    counts = {bucket: 0 for bucket in _NATURAL_LAYER_BUCKETS}
    if not rows:
        return rows, {"applied_graph_layer": graph_layer, "counts_by_layer": counts}
    layers = _batch_lookup_graph_layer(board_id, [r["node_id"] for r in rows])
    for r in rows:
        bucket = _classify_natural_layer(r.get("node_type", ""), layers.get(r["node_id"]))
        r["graph_layer"] = bucket
        counts[bucket] += 1
    if graph_layer == "all":
        kept = rows
    else:
        kept = [r for r in rows if r["graph_layer"] == graph_layer]
    audit = {
        "applied_graph_layer": graph_layer,
        "counts_by_layer": counts,
        # metadata / legacy_unknown are non-artifact and never leak into the
        # canonical or working result set.
        "non_artifact_excluded": counts["legacy_unknown"] + counts["metadata"],
    }
    return kept, audit


def _batch_lookup_created_at(board_id: str, node_ids: list[str]) -> dict[str, Any]:
    """Fetch ``created_at`` for a list of node ids in one pass across all
    node types. Returns a mapping ``{node_id: datetime}``. Nodes without a
    known created_at (e.g. degenerate rows) are omitted — callers treat the
    absence as "outside the temporal window" to be safe.
    """
    from datetime import timezone
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    if not node_ids:
        return {}

    out: dict[str, Any] = {}
    executor = getattr(get_kg_registry(), "cypher_executor", None)
    if executor is None:
        return out
    for node_type in NODE_TYPES:
        try:
            result = executor.execute_read_only(
                board_id,
                f"MATCH (n:{node_type}) WHERE n.id IN $ids "
                f"RETURN n.id, n.created_at",
                {"ids": node_ids},
                max_rows=max(len(node_ids), 1),
            )
            for row in result.get("rows") or []:
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

    logger.debug("[KG] get_schema_info board_id=%s include_internal=%s", board_id, include_internal)

    store = get_kg_registry().graph_store
    if store is not None:
        result = store.get_schema_info(board_id, include_internal=include_internal)
    else:
        # Fallback: static schema from constants
        stable_nodes = [
            {"name": nt, "stable": True}
            for nt in NODE_TYPES
        ]
        stable_rels = stable_rel_type_entries()
        vector_indexes = [
            {"node_type": nt, "attribute": "embedding",
             "dimension": 384, "similarity_metric": "cosine",
             "index_name": vector_index_name(nt)}
            for nt in VECTOR_INDEX_TYPES
        ]

        result = {
            "schema_version": SCHEMA_VERSION,
            "stable_node_types": stable_nodes,
            "stable_rel_types": stable_rels,
            "vector_indexes": vector_indexes,
        }
        if include_internal:
            result["internal_node_types"] = [{"name": "BoardMeta", "stable": False}]
            result["internal_rel_types"] = []

    # R6-IMP3 (FR3/AC3): additive per-label stable-property map so agents introspect
    # which properties are schema-safe instead of assuming a universal property.
    # Every canonical node label shares _COMMON_NODE_ATTRS, so stable_properties is
    # the same guaranteed set on each label; per-label variation is carried by
    # has_vector_index. Additive — the global keys above are unchanged.
    result["label_properties"] = _label_properties_map()
    return result


def _label_properties_map() -> dict[str, Any]:
    """Per-label stable-property map for schema_info (R6-IMP3). Derived from the
    canonical schema constants — never assumes an ad-hoc/universal property."""
    return {
        nt: {
            "stable_properties": list(STABLE_NODE_PROPERTIES),
            "has_vector_index": nt in VECTOR_INDEX_TYPES,
        }
        for nt in NODE_TYPES
    }
