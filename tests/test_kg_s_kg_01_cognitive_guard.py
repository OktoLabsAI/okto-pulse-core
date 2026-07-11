"""S-KG-01 / BR-KG-02 — cognitive_provenance_connectivity vs
deterministic_operational_connectivity in the KG connectivity guard.

The guard separates cognitive provenance (Learning/Alternative/Assumption: a
resolved source ref + a cognitive-taxonomy relation) from deterministic
operational provenance (Requirement/Constraint/APIContract/TestScenario/
Criterion/Bug/Entity: the strict belongs_to backbone). The taxonomy reuses the
EXISTING edge names ``validates`` and ``relates_to`` — no new edge type is
introduced.

Scenarios (TS-KG-01..07):
  01 Learning validates a canonical Bug                  -> accepted
  02 Learning validates a working/non-canonical Bug      -> blocked, no leak
  03 Cognitive artifact relates_to canonical types       -> accepted (no belongs_to)
  04 Learning relates_to a type OUTSIDE the taxonomy     -> structured failure
  05 cognitive writer emits Learning belongs_to          -> forbidden_deterministic_edge
  06 operational artifact w/o deterministic connectivity -> still blocked
  07 schema/policy diff: no new edge names; reuse        -> unit

Reproduce:
  uv run pytest -q tests/test_kg_s_kg_01_cognitive_guard.py
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.cognitive_policy import (
    COGNITIVE_EDGE_TYPES,
    COGNITIVE_PROVENANCE_NODE_TYPES,
    DETERMINISTIC_EDGE_TYPES,
    FORBIDDEN_DETERMINISTIC_EDGE_REASON,
    LEARNING_RELATES_TO_TARGETS,
    LayerViolationError,
    check_cognitive_edge_allowed,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
    MISSING_COGNITIVE_PROVENANCE_REASON,
    KGConnectivityOutcome,
    KGConnectivityRuleRegistry,
    KGNodeConnectivityGuard,
    KGNodeRef,
)
from okto_pulse.core.kg.primitives import (
    KGPrimitiveError,
    add_edge_candidate,
    add_node_candidate,
    begin_consolidation,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    AddNodeCandidateRequest,
    BeginConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
)


def _node(candidate_id: str, node_type: str, source_ref: str = ""):
    return SimpleNamespace(
        candidate_id=candidate_id,
        node_type=node_type,
        source_artifact_ref=source_ref,
    )


def _edge(candidate_id: str, edge_type: str, source: str, target: str):
    return SimpleNamespace(
        candidate_id=candidate_id,
        edge_type=edge_type,
        from_candidate_id=source,
        to_candidate_id=target,
    )


def _validate(guard, *, nodes, edges, existing=(), writer="commit_consolidation"):
    return guard.validate(
        board_id="board-skg01",
        writer_path=writer,
        kg_health_state="healthy",
        generation_id="gen-skg01",
        nodes=nodes,
        edges=edges,
        existing_node_refs=list(existing),
    )


# ---------------------------------------------------------------------------
# TS-KG-01 — Learning validates a CANONICAL Bug is accepted (canonical publish)
# ---------------------------------------------------------------------------


def test_ts_kg_01_learning_validates_canonical_bug_accepted():
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        nodes=[_node("learn-1", "Learning", "card:bug:bug-1:learning:0")],
        edges=[_edge("e1", "validates", "learn-1", "kg:bug-1")],
        existing=[KGNodeRef(ref_id="bug-1", node_type="Bug", graph_layer="canonical")],
    )

    assert result.passed is True
    assert result.outcome == KGConnectivityOutcome.PASSED
    assert result.violations == ()


# ---------------------------------------------------------------------------
# TS-KG-02 — Learning validates a WORKING Bug is held, never a canonical leak
# ---------------------------------------------------------------------------


def test_ts_kg_02_learning_validates_working_bug_blocked_no_canonical_leak():
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        nodes=[_node("learn-1", "Learning", "card:bug:bug-1:learning:0")],
        edges=[_edge("e1", "validates", "learn-1", "kg:bug-1")],
        existing=[KGNodeRef(ref_id="bug-1", node_type="Bug", graph_layer="working")],
    )

    # Fail-closed: a working Bug never satisfies canonical completeness, so the
    # canonical Learning is NOT published (no canonical leak).
    assert result.passed is False
    assert result.outcome == KGConnectivityOutcome.REJECTED
    violation = result.violations[0]
    assert violation.reason == CANONICAL_LEARNING_WORKING_ONLY_REASON
    # The working endpoint is surfaced (bounded "<ref>@<layer>"), never counted.
    assert any("@working" in ep for ep in violation.observed_endpoints)


# ---------------------------------------------------------------------------
# TS-KG-03 — cognitive artifact relates_to canonical endpoints, no belongs_to
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("node_type", COGNITIVE_PROVENANCE_NODE_TYPES)
@pytest.mark.parametrize("endpoint_type", LEARNING_RELATES_TO_TARGETS)
def test_ts_kg_03_cognitive_relates_to_canonical_endpoint_accepted(
    node_type, endpoint_type
):
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        # NOT bug-derived (spec: source) — exercises the cognitive_provenance
        # group rather than the bug_learning group.
        nodes=[_node("cog-1", node_type, f"spec:spec-1:{node_type.lower()}:0")],
        edges=[_edge("e1", "relates_to", "cog-1", "kg:endpoint-1")],
        existing=[
            KGNodeRef(
                ref_id="endpoint-1",
                node_type=endpoint_type,
                graph_layer="canonical",
            )
        ],
    )

    assert result.passed is True, (node_type, endpoint_type)
    assert result.violations == ()


def test_ts_kg_03_learning_relates_to_does_not_require_belongs_to():
    """The same Learning would FAIL under the old deterministic-only rule (its
    spec: source ref never resolves to a root Entity, so no belongs_to can be
    auto-attached); under cognitive_provenance the relates_to relation alone
    establishes connectivity."""
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        nodes=[_node("learn-1", "Learning", "spec:spec-1:learning:0")],
        edges=[_edge("e1", "relates_to", "learn-1", "kg:decision-1")],
        existing=[
            KGNodeRef(ref_id="decision-1", node_type="Decision", graph_layer="canonical"),
        ],
    )
    assert result.passed is True
    # No belongs_to edge was present or required.


# ---------------------------------------------------------------------------
# TS-KG-04 — Learning relates_to a type OUTSIDE the taxonomy fails structurally
# ---------------------------------------------------------------------------


def test_ts_kg_04_learning_relates_to_off_taxonomy_fails_before_mutation():
    guard = KGNodeConnectivityGuard()
    # relates_to -> Alternative is NOT in the taxonomy (Alternative is reachable
    # from a Decision, never the target of a Learning relates_to). It must fail
    # closed with the bounded missing_cognitive_provenance reason.
    result = _validate(
        guard,
        nodes=[_node("learn-1", "Learning", "spec:spec-1:learning:0")],
        edges=[_edge("e1", "relates_to", "learn-1", "kg:alt-1")],
        existing=[KGNodeRef(ref_id="alt-1", node_type="Alternative", graph_layer="canonical")],
    )

    assert result.passed is False
    assert result.outcome == KGConnectivityOutcome.REJECTED
    violation = result.violations[0]
    assert violation.reason == MISSING_COGNITIVE_PROVENANCE_REASON
    assert violation.node_type == "Learning"


def test_ts_kg_04_learning_with_no_cognitive_relation_is_missing_provenance():
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        nodes=[_node("learn-1", "Learning", "spec:spec-1:learning:0")],
        edges=[],
    )
    assert result.passed is False
    assert result.violations[0].reason == MISSING_COGNITIVE_PROVENANCE_REASON


# ---------------------------------------------------------------------------
# TS-KG-05 — a cognitive writer must never emit a deterministic belongs_to edge
# ---------------------------------------------------------------------------


def test_ts_kg_05_policy_belongs_to_is_forbidden_deterministic_edge():
    with pytest.raises(LayerViolationError) as exc:
        check_cognitive_edge_allowed("belongs_to")
    assert exc.value.edge_type == "belongs_to"
    assert exc.value.reason == FORBIDDEN_DETERMINISTIC_EDGE_REASON


@pytest.mark.asyncio
async def test_ts_kg_05_primitive_blocks_cognitive_belongs_to_with_reason(
    board_id, agent_id, db_factory
):
    async with db_factory() as db:
        begin = await begin_consolidation(
            BeginConsolidationRequest(
                board_id=board_id,
                artifact_type="spec",
                artifact_id="s-kg-01-belongs-to",
                raw_content="s-kg-01 cognitive belongs_to is forbidden",
            ),
            agent_id=agent_id,
            db=db,
        )

    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="learn-1",
                node_type=KGNodeType.LEARNING,
                title="S-KG-01 learning",
                source_artifact_ref="spec:spec-1:learning:0",
                source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )
    await add_node_candidate(
        AddNodeCandidateRequest(
            session_id=begin.session_id,
            candidate=NodeCandidate(
                candidate_id="ent-1",
                node_type=KGNodeType.ENTITY,
                title="Root entity",
                source_confidence=0.9,
            ),
        ),
        agent_id=agent_id,
    )

    with pytest.raises(KGPrimitiveError) as exc:
        await add_edge_candidate(
            AddEdgeCandidateRequest(
                session_id=begin.session_id,
                candidate=EdgeCandidate(
                    candidate_id="bad-belongs-to",
                    edge_type=KGEdgeType.BELONGS_TO,
                    from_candidate_id="learn-1",
                    to_candidate_id="ent-1",
                    confidence=0.9,
                ),
            ),
            agent_id=agent_id,
        )

    assert exc.value.code == "layer_violation"
    assert exc.value.details["reason"] == FORBIDDEN_DETERMINISTIC_EDGE_REASON
    assert exc.value.details["edge_type"] == "belongs_to"


# ---------------------------------------------------------------------------
# TS-KG-06 — operational artifacts stay on strict deterministic provenance
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "node_type, source_ref",
    [
        ("Requirement", "spec:spec-1:fr:0"),
        ("APIContract", "spec:spec-1:contract:0"),
    ],
    # Clean ids (no ':') so the per-test log filename stays valid on Windows.
    ids=["Requirement", "APIContract"],
)
def test_ts_kg_06_operational_node_without_provenance_still_blocked(node_type, source_ref):
    guard = KGNodeConnectivityGuard()
    # The deterministic worker owns these node types. A cognitive-style
    # relates_to edge (the "cognitive metadata") does NOT satisfy the strict
    # belongs_to provenance the operational artifact requires.
    result = _validate(
        guard,
        writer="deterministic_worker",
        nodes=[_node("op-1", node_type, source_ref)],
        edges=[_edge("e1", "relates_to", "op-1", "kg:dec-1")],
        existing=[KGNodeRef(ref_id="dec-1", node_type="Decision", graph_layer="canonical")],
    )

    assert result.passed is False
    violation = result.violations[0]
    assert violation.required_edge == "belongs_to:outgoing:Entity|Bug"
    assert violation.reason == "missing_required_edge"
    # The cognitive reason never leaks into the operational path.
    assert violation.reason != MISSING_COGNITIVE_PROVENANCE_REASON


def test_ts_kg_06_operational_node_with_belongs_to_passes():
    """Control: the same operational artifact passes once it carries the
    deterministic belongs_to backbone — proving the block above is about the
    missing deterministic provenance, not the node type per se."""
    guard = KGNodeConnectivityGuard()
    result = _validate(
        guard,
        writer="deterministic_worker",
        nodes=[_node("req-1", "Requirement", "spec:spec-1:fr:0")],
        edges=[_edge("e1", "belongs_to", "req-1", "kg:entity-spec-1")],
        existing=[KGNodeRef(ref_id="entity-spec-1", node_type="Entity")],
    )
    assert result.passed is True


# ---------------------------------------------------------------------------
# TS-KG-07 — schema/policy diff: no new edge names; validates/relates_to reused
# ---------------------------------------------------------------------------

_FORBIDDEN_NEW_EDGE_NAMES = frozenset(
    {"learned_from", "informs", "constrains", "refines", "warns_about"}
)


def test_ts_kg_07_no_new_edge_names_introduced():
    from kg_schema_testing import MULTI_REL_TYPES, REL_TYPES
    from okto_pulse.core.kg.schemas import KGEdgeType

    rel_names = {r[0] for r in REL_TYPES} | {m[0] for m in MULTI_REL_TYPES}
    dto_names = {e.value for e in KGEdgeType}
    policy_names = set(COGNITIVE_EDGE_TYPES) | set(DETERMINISTIC_EDGE_TYPES)

    assert _FORBIDDEN_NEW_EDGE_NAMES.isdisjoint(rel_names)
    assert _FORBIDDEN_NEW_EDGE_NAMES.isdisjoint(dto_names)
    assert _FORBIDDEN_NEW_EDGE_NAMES.isdisjoint(policy_names)


def test_ts_kg_07_taxonomy_reuses_validates_and_relates_to():
    from kg_schema_testing import relationship_endpoint_pairs
    from okto_pulse.core.kg.schemas import KGEdgeType

    dto_names = {e.value for e in KGEdgeType}
    assert "validates" in dto_names
    assert "relates_to" in dto_names

    relates_pairs = relationship_endpoint_pairs("relates_to")
    # Historical Decision -> Alternative pair is unchanged.
    assert ("Decision", "Alternative") in relates_pairs
    # The cognitive taxonomy pairs are ADDITIVELY present, reusing relates_to.
    for source in COGNITIVE_PROVENANCE_NODE_TYPES:
        for target in LEARNING_RELATES_TO_TARGETS:
            assert (source, target) in relates_pairs

    # validates Learning -> Bug is unchanged (reused, not renamed).
    assert ("Learning", "Bug") in relationship_endpoint_pairs("validates")


def test_ts_kg_07_registry_keeps_operational_strict_and_cognitive_taxonomy():
    registry = KGConnectivityRuleRegistry()

    # Operational artifacts keep the deterministic belongs_to provenance group.
    for node_type in ("Requirement", "Constraint", "APIContract", "TestScenario", "Criterion"):
        groups = registry.get_rule(node_type).required_edge_groups
        labels = {alt.edge_type for g in groups for alt in g.alternatives}
        assert labels == {"belongs_to"}, node_type

    # Cognitive artifacts use the cognitive_provenance group (belongs_to OR
    # relates_to) — and never invent a new edge name. validates is the exclusive
    # bug-derived proof and lives in the bug_learning branch, not in this static
    # rule group (see test_ts_kg_01 for the bug-derived validates path).
    for node_type in ("Learning", "Alternative", "Assumption"):
        groups = registry.get_rule(node_type).required_edge_groups
        assert [g.name for g in groups] == ["cognitive_provenance"], node_type
        edge_types = {alt.edge_type for g in groups for alt in g.alternatives}
        assert edge_types == {"belongs_to", "relates_to"}, node_type
        assert _FORBIDDEN_NEW_EDGE_NAMES.isdisjoint(edge_types)
