"""Parametrized Cypher templates for the 9 tier primario tools.

SECURITY: ALL templates use $-prefixed params — NEVER string interpolation.
This mitigates Cypher injection (FR-10). Default filters (min_confidence,
max_rows) are injected by the service layer at query time.

Templates return dicts from graph backend `RETURN` projections. The service layer
wraps results into typed Pydantic models.

v0.3.0: validation_status filter removed from every template; R3 adds a
relevance_score threshold filter in its place. During the R1→R3 window the
queries are intentionally permissive — stubs return the full graph so the
server stays operational while the scoring pipeline lands.
"""

# Default filter clause injected into every read query.
# The service layer replaces $min_confidence, $min_relevance, and $max_rows
# at call time. v0.3.0 R3 adds the relevance threshold — default 0.3 (below
# the neutral 0.5 so newly created nodes still pass).
_DEFAULT_FILTERS = (
    "AND n.source_confidence >= $min_confidence "
    "AND n.relevance_score >= $min_relevance "
)

# Tombstones that must never surface through active-memory reads.  This rule is
# intentionally independent from ``include_superseded``: that opt-in exposes
# historical versions, not nodes whose governed source disappeared or whose
# projection was removed.
ACTIVE_READ_TOMBSTONE_REASONS = frozenset(
    {
        "source_deleted",
        "source_projection_removed",
    }
)


def is_visible_in_active_reads(revocation_reason: object) -> bool:
    """Return whether ``revocation_reason`` is visible to active read surfaces."""

    return str(revocation_reason or "") not in ACTIVE_READ_TOMBSTONE_REASONS


# ---------------------------------------------------------------------------
# Canonical-only layer scoping — SINGLE SOURCE OF TRUTH (bug 07bdf670).
# Every canonical-only read surface (these templates, kg_service inline
# queries, the embedded graph-store subgraph, jobs) MUST build its graph_layer
# predicate from `layer_filter_clause` and its label from
# `layer_label_projection` so the fail-closed contract can never drift.
# ---------------------------------------------------------------------------


def layer_filter_clause(var: str, *, param: str = "$graph_layer") -> str:
    """Fail-CLOSED graph_layer filter for node alias ``var`` (bug 07bdf670).

    A node whose ``graph_layer`` is NULL/absent is NOT treated as canonical: it
    is excluded from BOTH canonical-only and working-only reads and surfaces
    ONLY under ``<param> = 'all'``. The OLD form
    ``coalesce(var.graph_layer, 'canonical') = <param>`` was fail-OPEN — a
    missing layer defaulted to canonical and leaked. The authoritative place
    that stamps legacy/un-stamped nodes is the migration backfill
    (``migrate_kg_layer`` / global-discovery layer backfill), never this read.

    ``var.graph_layer = <param>`` is already NULL-safe in Cypher (``NULL =``
    anything yields NULL → the row is dropped), so this is fail-closed by
    construction.
    """
    return f"({param} = 'all' OR {var}.graph_layer = {param})"


def superseded_filter_clause(var: str, *, param: str = "$include_superseded") -> str:
    """Fail-closed active-memory filter for node alias ``var`` (spec
    MKG-D-S1 FR7, decision D5 — same single-source pattern as
    ``layer_filter_clause``, born from bug 07bdf670).

    Default recall EXCLUDES superseded nodes; the caller opts in by binding
    ``<param> = true``. ``superseded_by IS NULL`` is NULL-safe by
    construction.
    """
    return f"({param} = true OR {var}.superseded_by IS NULL)"


def active_read_filter_clause(var: str) -> str:
    """Exclude permanent source/projection tombstones from active reads.

    The values are trusted constants rather than caller parameters so every
    template and adapter applies the exact same closed set.  Explicit
    comparisons keep the predicate compatible with Kùzu while ``coalesce``
    preserves legacy rows that have no revocation reason.
    """

    clauses = " AND ".join(
        f"coalesce({var}.revocation_reason, '') <> '{reason}'"
        for reason in sorted(ACTIVE_READ_TOMBSTONE_REASONS)
    )
    return f"({clauses})"


def layer_label_projection(var: str, *, alias: str = "graph_layer") -> str:
    """Fail-SAFE label projection for ``var.graph_layer`` (bug 07bdf670).

    A NULL/absent layer is reported as ``'legacy_unknown'`` — NEVER mislabeled
    ``'canonical'`` and never a raw ``null`` — so an ``all`` / include_working
    read can never present an un-stamped node as canonical. ``graph_layer`` is
    not a strictly binary canonical|working field (it also carries ``'none'``
    per source_maturity), so the conservative label is the explicit
    ``legacy_unknown`` bucket, matching ``MATURITY_LEGACY_UNKNOWN`` (codex
    contract 29611d06 / bug 07bdf670, item 2).
    """
    return f"coalesce({var}.graph_layer, 'legacy_unknown') AS {alias}"

# ---------------------------------------------------------------------------
# 1. get_decision_history — FR-11
# Variable-length path on :supersedes up to depth 10.
# ---------------------------------------------------------------------------

GET_DECISION_HISTORY = f"""
MATCH (d:Decision)
WHERE d.title CONTAINS $topic
  AND d.source_confidence >= $min_confidence
  AND d.relevance_score >= $min_relevance
  AND {superseded_filter_clause('d')}
  AND {active_read_filter_clause('d')}
RETURN d.id, d.title, d.content, d.created_at, d.source_confidence,
       d.relevance_score, d.superseded_by, d.source_artifact_ref
ORDER BY d.relevance_score DESC, d.created_at DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 2. get_related_context — FR-12
# 2-hop neighborhood + entity co-occurrence from an artifact_id.
# ---------------------------------------------------------------------------

GET_RELATED_CONTEXT = f"""
MATCH (center)-[r1]-(hop1)
WHERE center.source_artifact_ref = $artifact_id
  AND center.source_confidence >= $min_confidence
  AND {active_read_filter_clause('center')}
  AND {layer_filter_clause('hop1')}
  AND {superseded_filter_clause('hop1')}
  AND {active_read_filter_clause('hop1')}
OPTIONAL MATCH (hop1)-[r2]-(hop2)
WHERE (hop2 IS NULL
       OR ({layer_filter_clause('hop2')}
           AND {superseded_filter_clause('hop2')}
           AND {active_read_filter_clause('hop2')}))
RETURN center.id AS center_id, center.title AS center_title,
       hop1.id AS hop1_id, hop1.title AS hop1_title,
       hop2.id AS hop2_id, hop2.title AS hop2_title,
       label(r1) AS rel1_type, label(r2) AS rel2_type
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 3. get_supersedence_chain — FR-15
# Variable-length path *..10 on :supersedes for a specific decision_id.
# ---------------------------------------------------------------------------

GET_SUPERSEDENCE_CHAIN = f"""
MATCH (current:Decision {{id: $decision_id}})-[:supersedes]->(next:Decision)
WHERE {active_read_filter_clause('current')}
  AND {active_read_filter_clause('next')}
RETURN next.id, next.title, next.created_at,
       next.superseded_by, next.superseded_at
"""


def supersedence_chain_template(node_type: str) -> str:
    """Label-parametrized supersedence chain (spec MKG-D-S1 FR6).

    ``node_type`` is validated against the NODE_TYPES allowlist BEFORE any
    string interpolation — an unknown label fails closed with ValueError,
    never a free-form Cypher injection. Decision keeps the shared named
    template so its visibility contract cannot drift from the generic path.
    """
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    if node_type not in NODE_TYPES:
        raise ValueError(
            f"invalid_node_type: {node_type!r} (allowed: {NODE_TYPES})"
        )
    if node_type == "Decision":
        return GET_SUPERSEDENCE_CHAIN
    return (
        f"\nMATCH (current:{node_type} {{id: $decision_id}})"
        f"-[:supersedes]->(next:{node_type})\n"
        f"WHERE {active_read_filter_clause('current')}\n"
        f"  AND {active_read_filter_clause('next')}\n"
        "RETURN next.id, next.title, next.created_at,\n"
        "       next.superseded_by, next.superseded_at\n"
    )

# ---------------------------------------------------------------------------
# 4. find_contradictions — FR-14
# Pairs via :contradicts rel. Optional node_id filter.
# ---------------------------------------------------------------------------

FIND_CONTRADICTIONS_BY_NODE = f"""
MATCH (a:Decision)-[r:contradicts]->(b:Decision)
WHERE (a.id = $node_id OR b.id = $node_id)
  AND {active_read_filter_clause('a')}
  AND {active_read_filter_clause('b')}
RETURN a.id AS id_a, a.title AS title_a,
       b.id AS id_b, b.title AS title_b,
       r.confidence AS confidence
LIMIT $max_rows
"""

FIND_CONTRADICTIONS_ALL = f"""
MATCH (a:Decision)-[r:contradicts]->(b:Decision)
WHERE {active_read_filter_clause('a')}
  AND {active_read_filter_clause('b')}
RETURN a.id AS id_a, a.title AS title_a,
       b.id AS id_b, b.title AS title_b,
       r.confidence AS confidence
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 5. find_similar_decisions — FR-13
# HNSW vector search via the indexed similarity adapter. Handled by kg/search.py,
# but we define the fallback text-match template here.
# ---------------------------------------------------------------------------

FIND_SIMILAR_DECISIONS_TEXT_FALLBACK = f"""
MATCH (d:Decision)
WHERE d.title CONTAINS $topic
  AND d.source_confidence >= $min_confidence
  AND d.relevance_score >= $min_relevance
  AND {active_read_filter_clause('d')}
RETURN d.id, d.title, d.content, d.source_confidence,
       d.source_artifact_ref, d.created_at
ORDER BY d.relevance_score DESC, d.source_confidence DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 6. explain_constraint — FR-16
# Constraint + origin via :derives_from + :violates.
# ---------------------------------------------------------------------------

EXPLAIN_CONSTRAINT = f"""
MATCH (c:Constraint {{id: $constraint_id}})
WHERE {active_read_filter_clause('c')}
RETURN c.id, c.title, c.content, c.justification,
       c.source_artifact_ref, c.source_confidence
"""

EXPLAIN_CONSTRAINT_ORIGINS = f"""
MATCH (c:Constraint {{id: $constraint_id}})<-[:derives_from]-(origin:Decision)
WHERE {active_read_filter_clause('c')}
  AND {active_read_filter_clause('origin')}
RETURN origin.id, origin.title
"""

EXPLAIN_CONSTRAINT_VIOLATIONS = f"""
MATCH (c:Constraint {{id: $constraint_id}})<-[:violates]-(bug:Bug)
WHERE {active_read_filter_clause('c')}
  AND {active_read_filter_clause('bug')}
RETURN bug.id, bug.title
"""

# ---------------------------------------------------------------------------
# 7. list_alternatives — FR-17
# Alternative nodes via :relates_to from a Decision.
# ---------------------------------------------------------------------------

LIST_ALTERNATIVES = f"""
MATCH (d:Decision {{id: $decision_id}})-[:relates_to]->(alt:Alternative)
WHERE {active_read_filter_clause('d')}
  AND {active_read_filter_clause('alt')}
RETURN alt.id, alt.title, alt.content, alt.justification,
       alt.source_confidence, alt.source_artifact_ref
ORDER BY alt.source_confidence DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 8. get_learning_from_bugs — FR-18
# Learning nodes via :derives_from to Bug in a filtered area.
# Area filtering via Entity :mentions on the Bug.
# ---------------------------------------------------------------------------

GET_LEARNING_FROM_BUGS = f"""
MATCH (l:Learning)-[:validates]->(b:Bug)
WHERE l.source_confidence >= $min_confidence
  AND l.relevance_score >= $min_relevance
  AND (b.title CONTAINS $area OR b.content CONTAINS $area)
  AND {active_read_filter_clause('l')}
  AND {active_read_filter_clause('b')}
RETURN l.id AS learning_id, l.title AS learning_title,
       l.content AS learning_content, l.justification,
       l.source_confidence,
       b.id AS bug_id, b.title AS bug_title
ORDER BY l.relevance_score DESC, l.source_confidence DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 9. query_global — delegates to global discovery layer
# No Cypher template here — handled by kg/search.py against the global
# discovery.graph meta-graph. Placeholder for the service layer.
# ---------------------------------------------------------------------------

# (handled in kg_service.py via search.find_similar_nodes_by_type on global)

# ---------------------------------------------------------------------------
# 10. get_all_nodes — visualization helper (no type filter)
# ---------------------------------------------------------------------------

GET_ALL_NODES = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
RETURN n.id, label(n) AS node_type, n.title, n.content,
       n.created_at, n.source_confidence, n.relevance_score,
       n.source_artifact_ref, {layer_label_projection('n')},
       n.maturity_status
ORDER BY n.created_at DESC, n.id DESC
LIMIT $max_rows
"""

GET_ALL_NODES_BY_TYPE = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
  AND label(n) = $node_type
RETURN n.id, label(n) AS node_type, n.title, n.content,
       n.created_at, n.source_confidence, n.relevance_score,
       n.source_artifact_ref, {layer_label_projection('n')},
       n.maturity_status
ORDER BY n.created_at DESC, n.id DESC
LIMIT $max_rows
"""

# Cursor-keyset variant — Spec 8 / S1.3. The WHERE clause applies a
# strict tuple comparison so the next page starts immediately after the
# last row of the previous page. Mirrors the ORDER BY above so the page
# boundaries are stable across calls.
GET_ALL_NODES_AFTER_CURSOR = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
  AND (n.created_at < $cursor_ts
       OR (n.created_at = $cursor_ts AND n.id < $cursor_id))
RETURN n.id, label(n) AS node_type, n.title, n.content,
       n.created_at, n.source_confidence, n.relevance_score,
       n.source_artifact_ref, {layer_label_projection('n')},
       n.maturity_status
ORDER BY n.created_at DESC, n.id DESC
LIMIT $max_rows
"""

GET_ALL_NODES_BY_TYPE_AFTER_CURSOR = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
  AND label(n) = $node_type
  AND (n.created_at < $cursor_ts
       OR (n.created_at = $cursor_ts AND n.id < $cursor_id))
RETURN n.id, label(n) AS node_type, n.title, n.content,
       n.created_at, n.source_confidence, n.relevance_score,
       n.source_artifact_ref, {layer_label_projection('n')},
       n.maturity_status
ORDER BY n.created_at DESC, n.id DESC
LIMIT $max_rows
"""

COUNT_ALL_NODES = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
RETURN count(n)
"""

COUNT_ALL_NODES_BY_TYPE = f"""
MATCH (n)
WHERE n.source_confidence >= $min_confidence
  AND n.relevance_score >= $min_relevance
  AND {layer_filter_clause('n')}
  AND {active_read_filter_clause('n')}
  AND label(n) = $node_type
RETURN count(n)
"""
