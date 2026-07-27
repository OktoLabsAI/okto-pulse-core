"""MCP KG query-safety quick-wins (spec R3 — MCP KG Query Safety and Quick-Win
Payload Bounds, board Okto Pulse 0.2.3).

These components bound and sanitize KG query-tool responses at the MCP
serialization boundary. Their reason for existing is concrete: an agent that
runs ``RETURN n`` or ``RETURN n.embedding`` against a board graph would
otherwise pull 384-float embedding vectors per node straight into its context
window — a single ``LIMIT 1000`` query can emit ~1.5M tokens of unusable float
noise and blow the window. None of these helpers touch authorization,
persistence, or the underlying query semantics; they only shape the response.

Components (named in the spec test scenarios):
- ``KGNaturalQueryBoundaryGuard`` (FR0): reject oversize natural-query text
  BEFORE the embedding provider is invoked.
- ``KGCypherResponseSanitizer`` (FR1): strip embedding/vector columns and
  nested embedding fields, reporting every removed path.
- Cypher row bounds (FR2): agent-safe default + hard cap with truncation
  metadata, while a caller may still request the bounded full set explicitly.
- Numeric rounding (FR3): round score/distance/timing floats at the response
  boundary only, never reordering server-sorted results.
"""

from __future__ import annotations

from typing import Any

# --- FR0: natural-query boundary -------------------------------------------
# Sentence-transformers (all-MiniLM-L6-v2) truncates at 256 word-pieces; far
# below that, a multi-thousand-char "query" is never a real search intent and
# only burns the embedding call. Reject before resolving the provider.
NATURAL_QUERY_MAX_CHARS = 2000

# --- FR2: agent-safe Cypher row bounds -------------------------------------
# The MCP surface defaults to a small, agent-safe page. A caller that genuinely
# needs more may pass max_rows up to the hard cap (FR9 full/legacy access);
# anything above the hard cap is rejected structurally rather than silently
# dumping an unbounded result into the context window.
CYPHER_DEFAULT_ROWS = 50
CYPHER_HARD_CAP_ROWS = 1000

# --- FR1: embedding/vector detection ---------------------------------------
_EMBEDDING_KEY = "embedding"
# A list of >= this many plain numbers has the shape of an embedding vector
# (the board model emits 384-dim vectors); short numeric lists (e.g. a degree
# pair) are left untouched.
_VECTOR_MIN_LEN = 16

# --- FR3: numeric rounding -------------------------------------------------
NUMERIC_ROUND_DIGITS = 4


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _looks_like_vector(value: Any) -> bool:
    """True when ``value`` has the shape of an embedding vector."""
    return (
        isinstance(value, (list, tuple))
        and len(value) >= _VECTOR_MIN_LEN
        and all(_is_number(x) for x in value)
    )


# ===========================================================================
# FR0 — KGNaturalQueryBoundaryGuard
# ===========================================================================
class KGNaturalQueryBoundaryGuard:
    """Reject an oversize natural-language query before any embedding call.

    Pure/deterministic: ``check`` returns ``None`` when the query is within
    bounds, or a structured rejection dict (``query_too_large``) when it is
    not. The MCP tool returns that dict directly so the embedding provider is
    never resolved — see scenario ts_27ecfd9d (the embedding spy records zero
    calls on rejection).
    """

    def __init__(self, max_chars: int = NATURAL_QUERY_MAX_CHARS) -> None:
        self.max_chars = max_chars

    def check(self, nl_query: str) -> dict | None:
        length = len(nl_query or "")
        if length <= self.max_chars:
            return None
        return {
            "error": "query_too_large",
            "detail": (
                f"natural query is {length} chars; max is {self.max_chars}. "
                "Shorten the query — embeddings are computed over a small "
                "window and oversize text only wastes the call."
            ),
            "rejection": {
                "reason": "query_too_large",
                "query_chars": length,
                "max_chars": self.max_chars,
                "embedding_invoked": False,
            },
        }


# ===========================================================================
# FR1 — KGCypherResponseSanitizer
# ===========================================================================
class KGCypherResponseSanitizer:
    """Strip embedding/vector data from a Cypher result before serialization.

    Handles every shape an agent can produce (scenario ts_5f85add5):
    - a column named ``embedding`` or ``*.embedding`` (when ``columns`` are
      present, matched by name);
    - a bare vector value (``RETURN n.embedding`` — a list of 384 floats with
      no column name), matched by shape;
    - a node-like dict carrying an ``embedding`` key (``RETURN n``), stripped
      recursively at any depth.

    Non-vector data is preserved. Every removed location is recorded in
    ``result['sanitization']['stripped_fields']``.
    """

    def sanitize(self, result: dict) -> dict:
        if not isinstance(result, dict):
            return result
        stripped: list[str] = []
        columns = result.get("columns")
        # Column-name match (when the executor surfaces column names).
        vector_col_idx: set[int] = set()
        if isinstance(columns, list):
            for idx, name in enumerate(columns):
                if isinstance(name, str) and (
                    name == _EMBEDDING_KEY or name.endswith("." + _EMBEDDING_KEY)
                ):
                    vector_col_idx.add(idx)
                    stripped.append(f"columns[{idx}]:{name}")

        rows = result.get("rows")
        if isinstance(rows, list):
            new_rows = []
            for r_i, row in enumerate(rows):
                if isinstance(row, list):
                    new_row = []
                    for c_i, cell in enumerate(row):
                        if c_i in vector_col_idx:
                            new_row.append(None)
                            continue
                        cleaned, paths = self._strip_value(cell, f"rows[{r_i}][{c_i}]")
                        stripped.extend(paths)
                        new_row.append(cleaned)
                    new_rows.append(new_row)
                else:
                    cleaned, paths = self._strip_value(row, f"rows[{r_i}]")
                    stripped.extend(paths)
                    new_rows.append(cleaned)
            result = {**result, "rows": new_rows}

        result["sanitization"] = {
            "stripped_fields": stripped,
            "stripped_count": len(stripped),
        }
        return result

    def _strip_value(self, value: Any, path: str) -> tuple[Any, list[str]]:
        """Recursively remove embedding keys + bare vectors. Returns (cleaned, paths)."""
        paths: list[str] = []
        if _looks_like_vector(value):
            return None, [path]
        if isinstance(value, dict):
            out: dict[Any, Any] = {}
            for k, v in value.items():
                if k == _EMBEDDING_KEY or (
                    isinstance(k, str) and k.endswith("." + _EMBEDDING_KEY)
                ):
                    paths.append(f"{path}.{k}")
                    continue
                cleaned, sub = self._strip_value(v, f"{path}.{k}")
                paths.extend(sub)
                out[k] = cleaned
            return out, paths
        if isinstance(value, list):
            out_list = []
            for i, item in enumerate(value):
                cleaned, sub = self._strip_value(item, f"{path}[{i}]")
                paths.extend(sub)
                out_list.append(cleaned)
            return out_list, paths
        return value, paths


# ===========================================================================
# FR2 — Cypher row bounds
# ===========================================================================
def resolve_cypher_max_rows(
    requested: int | None,
    *,
    default: int = CYPHER_DEFAULT_ROWS,
    hard_cap: int = CYPHER_HARD_CAP_ROWS,
) -> tuple[int | None, dict | None]:
    """Resolve the effective max_rows for a Cypher MCP call (FR2/FR9).

    Returns ``(effective_limit, error)``:
    - ``requested`` None/<=0  -> use the agent-safe ``default``.
    - ``0 < requested <= hard_cap`` -> use ``requested`` (explicit bounded full
      access, FR9).
    - ``requested > hard_cap`` -> ``(None, {error: 'max_rows_exceeds_hard_cap', ...})``.
    """
    if requested is None or requested <= 0:
        return default, None
    if requested > hard_cap:
        return None, {
            "error": "max_rows_exceeds_hard_cap",
            "detail": (
                f"max_rows={requested} exceeds the agent-safe hard cap "
                f"{hard_cap}. Page your query or lower max_rows."
            ),
            "requested_max_rows": requested,
            "hard_cap": hard_cap,
        }
    return requested, None


def annotate_truncation(result: dict, effective_limit: int | None) -> dict:
    """Ensure the cypher result carries explicit row-bound metadata (FR2).

    The executor already sets ``truncated`` when it stops at the limit; this
    records the effective limit applied so the agent knows it received a bounded
    page rather than the full set.
    """
    if not isinstance(result, dict):
        return result
    result = {**result}
    result.setdefault("truncated", False)
    result["row_bounds"] = {
        "effective_limit": effective_limit,
        "hard_cap": CYPHER_HARD_CAP_ROWS,
        "truncated": bool(result.get("truncated")),
    }
    return result


# ===========================================================================
# FR3 — response-only numeric rounding
# ===========================================================================
def round_kg_numbers(obj: Any, *, ndigits: int = NUMERIC_ROUND_DIGITS) -> Any:
    """Recursively round float values to ``ndigits`` for response JSON (FR3).

    Order-preserving by construction: this only rewrites scalar float values in
    place; it never reorders lists, so server-side ordering (already applied
    before serialization) is untouched. Ints and bools pass through unchanged;
    a bare embedding vector, if any survived sanitization, is left as-is (it is
    sanitized upstream, not rounded).
    """
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, dict):
        return {k: round_kg_numbers(v, ndigits=ndigits) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_kg_numbers(v, ndigits=ndigits) for v in obj]
    if isinstance(obj, tuple):
        return [round_kg_numbers(v, ndigits=ndigits) for v in obj]
    return obj


# ===========================================================================
# FR4 — KGHealthMCPProjection
# ===========================================================================
# The full kg_health payload carries ~38 fields built for the dashboard
# (triple-aliased state, duplicated schema-version strings, verbose
# health_issues prose, a nested diagnostics block). An LLM agent polling
# health only needs the operational stop-rule fields. The slim default keeps
# those; the full diagnostics stay available behind profile=full/legacy (FR9).

# Operational stop-rule fields that MUST survive the slim projection
# (scenario ts_860cb8a4). ``recent_events`` carries the "recent critical
# signals"; the *_state + metric_status + classification_reason gate whether an
# agent may proceed with a KG mutation, so they are never dropped.
KG_HEALTH_STOP_FIELDS = (
    "board_id",
    "health_schema_version",
    "materialization_state",
    "materialization_generation",
    "probe_reason_codes",
    "graph_state",
    "discovery_state",
    "overall_state",
    "metric_status",
    "classification_reason",
    "correlation_id",
    "memory_pressure_status",
    "recent_events",
    "checked_at",
)

# Small, decision-relevant operational fields kept in the slim view (cheap,
# and what the docstring historically advertised). Scheduler diagnostics and
# storage footprint proxy are intentionally included because they explain
# at_risk-like operator debt without implying graph recovery. Verbose
# diagnostics, state aliases, duplicate schema-version strings and prose issue
# descriptions are omitted until the full profile is requested.
KG_HEALTH_SLIM_EXTRA = (
    "queue_depth",
    "dead_letter_count",
    "global_outbox_dead_letter_count",
    "total_nodes",
    "default_score_ratio",
    "avg_relevance",
    "contradict_warn_count",
    "last_tick_status",
    "last_decay_tick_at",
    # FR6 (spec R2b TR5, IMPL-3): board counters in MCP summary profile so
    # agents can distinguish tick-global-failed from board-stale without
    # requesting profile=full. Default 0 per BR5.
    "nodes_recomputed_in_last_tick",
    "boards_processed_in_last_tick",
    "boards_failed_in_last_tick",
    "decay_scheduler_diagnostics",
    "storage_footprint_proxy",
    "probe_diagnostics",
    # RKG-05 (fr_3fbb564c / tr_1db056de / tr_22d4434d / br_df421357): technical
    # signals are NON-MASKABLE. The slim view may omit prose, but the
    # domain-separated scalar counts + drill_down_tool MUST survive so an agent
    # can locate the items without a manual DB query (ac_2c86f666). active_queue,
    # dead_letter and canonical_debt stay distinct operational domains; one count
    # is never inferred from another.
    "operational_domains",
    "canonical_debt",
)

_KG_HEALTH_SLIM_KEEP = frozenset(KG_HEALTH_STOP_FIELDS) | frozenset(
    KG_HEALTH_SLIM_EXTRA
)


class KGHealthMCPProjection:
    """Project a full kg_health payload to a slim MCP default (FR4).

    ``profile in {'full', 'legacy'}`` returns the payload untouched (full
    diagnostics). The default ``'summary'`` keeps the stop-rule fields + a few
    operational scalars and drops everything else, recording which keys were
    omitted so a caller can ask for ``profile=full`` when it needs them.
    """

    def project(self, payload: dict, *, profile: str = "summary") -> dict:
        if not isinstance(payload, dict):
            return payload
        from okto_pulse.core.mcp.projection_envelope import (
            resolve_profile,
            unsupported_projection_error,
        )

        resolved_profile = resolve_profile(profile)
        if resolved_profile is None:
            return unsupported_projection_error(profile)
        if resolved_profile in ("full", "legacy"):
            return payload
        slim = {k: v for k, v in payload.items() if k in _KG_HEALTH_SLIM_KEEP}
        omitted = sorted(k for k in payload if k not in _KG_HEALTH_SLIM_KEEP)
        slim["profile"] = resolved_profile
        slim["full_profile_available"] = bool(omitted)
        slim["omitted_count"] = len(omitted)
        return slim
