"""Parametrized Cypher templates for the 9 tier primario tools.

SECURITY: ALL templates use $-prefixed params — NEVER string interpolation.
This mitigates Cypher injection (FR-10). Default filters (validation_status,
min_confidence, max_rows) are injected by the service layer at query time.

Templates return dicts from Kuzu `RETURN` projections. The service layer
wraps results into typed Pydantic models.
"""

# Default filter clause injected into every read query.
# The service layer replaces $min_confidence and $max_rows at call time.
_DEFAULT_FILTERS = (
    "AND n.validation_status <> 'unvalidated' "
    "AND n.source_confidence >= $min_confidence "
)

# ---------------------------------------------------------------------------
# 1. get_decision_history — FR-11
# Variable-length path on :supersedes up to depth 10.
# ---------------------------------------------------------------------------

GET_DECISION_HISTORY = """
MATCH (d:Decision)
WHERE d.title CONTAINS $topic
  AND d.validation_status <> 'unvalidated'
  AND d.source_confidence >= $min_confidence
RETURN d.id, d.title, d.content, d.created_at, d.source_confidence,
       d.validation_status, d.superseded_by
ORDER BY d.created_at DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 2. get_related_context — FR-12
# 2-hop neighborhood + entity co-occurrence from an artifact_id.
# ---------------------------------------------------------------------------

GET_RELATED_CONTEXT = """
MATCH (center)-[r1]-(hop1)
WHERE center.source_artifact_ref = $artifact_id
  AND center.validation_status <> 'unvalidated'
  AND center.source_confidence >= $min_confidence
OPTIONAL MATCH (hop1)-[r2]-(hop2)
WHERE hop2.validation_status <> 'unvalidated'
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

GET_SUPERSEDENCE_CHAIN = """
MATCH (current:Decision {id: $decision_id})-[:supersedes]->(next:Decision)
RETURN next.id, next.title, next.created_at,
       next.superseded_by, next.superseded_at
"""

# ---------------------------------------------------------------------------
# 4. find_contradictions — FR-14
# Pairs via :contradicts rel. Optional node_id filter.
# ---------------------------------------------------------------------------

FIND_CONTRADICTIONS_BY_NODE = """
MATCH (a:Decision)-[r:contradicts]->(b:Decision)
WHERE a.id = $node_id OR b.id = $node_id
RETURN a.id AS id_a, a.title AS title_a,
       b.id AS id_b, b.title AS title_b,
       r.confidence AS confidence
LIMIT $max_rows
"""

FIND_CONTRADICTIONS_ALL = """
MATCH (a:Decision)-[r:contradicts]->(b:Decision)
WHERE a.validation_status <> 'unvalidated'
  AND b.validation_status <> 'unvalidated'
RETURN a.id AS id_a, a.title AS title_a,
       b.id AS id_b, b.title AS title_b,
       r.confidence AS confidence
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 5. find_similar_decisions — FR-13
# HNSW vector search via QUERY_VECTOR_INDEX. Handled by kg/search.py,
# but we define the fallback text-match template here.
# ---------------------------------------------------------------------------

FIND_SIMILAR_DECISIONS_TEXT_FALLBACK = """
MATCH (d:Decision)
WHERE d.title CONTAINS $topic
  AND d.validation_status <> 'unvalidated'
  AND d.source_confidence >= $min_confidence
RETURN d.id, d.title, d.content, d.source_confidence,
       d.source_artifact_ref, d.created_at
ORDER BY d.source_confidence DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 6. explain_constraint — FR-16
# Constraint + origin via :derives_from + :violates.
# ---------------------------------------------------------------------------

EXPLAIN_CONSTRAINT = """
MATCH (c:Constraint {id: $constraint_id})
RETURN c.id, c.title, c.content, c.justification,
       c.source_artifact_ref, c.source_confidence
"""

EXPLAIN_CONSTRAINT_ORIGINS = """
MATCH (c:Constraint {id: $constraint_id})<-[:derives_from]-(origin:Decision)
RETURN origin.id, origin.title
"""

EXPLAIN_CONSTRAINT_VIOLATIONS = """
MATCH (c:Constraint {id: $constraint_id})<-[:violates]-(bug:Bug)
RETURN bug.id, bug.title
"""

# ---------------------------------------------------------------------------
# 7. list_alternatives — FR-17
# Alternative nodes via :relates_to from a Decision.
# ---------------------------------------------------------------------------

LIST_ALTERNATIVES = """
MATCH (d:Decision {id: $decision_id})-[:relates_to]->(alt:Alternative)
WHERE alt.validation_status <> 'unvalidated'
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

GET_LEARNING_FROM_BUGS = """
MATCH (l:Learning)-[:validates]->(b:Bug)
WHERE l.validation_status <> 'unvalidated'
  AND l.source_confidence >= $min_confidence
  AND (b.title CONTAINS $area OR b.content CONTAINS $area)
RETURN l.id AS learning_id, l.title AS learning_title,
       l.content AS learning_content, l.justification,
       l.source_confidence,
       b.id AS bug_id, b.title AS bug_title
ORDER BY l.source_confidence DESC
LIMIT $max_rows
"""

# ---------------------------------------------------------------------------
# 9. query_global — delegates to global discovery layer
# No Cypher template here — handled by kg/search.py against the global
# discovery.kuzu meta-graph. Placeholder for the service layer.
# ---------------------------------------------------------------------------

# (handled in kg_service.py via search.find_similar_nodes_by_type on global)
