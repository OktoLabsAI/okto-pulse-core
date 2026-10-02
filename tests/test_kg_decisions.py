"""Native structured Decision models and deterministic projection contract."""

from __future__ import annotations

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.models.schemas import Decision, DecisionStatus


# ---------------------------------------------------------------------------
# TS5 — Pydantic model defaults + status validation
# ---------------------------------------------------------------------------


def test_ts5_decision_defaults():
    d = Decision(id="dec_abc12345", title="Use Grafx", rationale="embedded graph DB")
    assert d.status == "active"
    assert d.context is None
    assert d.alternatives_considered is None
    assert d.supersedes_decision_id is None
    assert d.linked_requirements is None
    assert d.linked_task_ids is None
    assert d.notes is None


def test_ts5_decision_status_literal_roundtrip():
    for status in ("active", "superseded", "revoked"):
        d = Decision(id="dec_1", title="t", rationale="r", status=status)
        assert d.status == status


def test_ts5_decision_rejects_invalid_status():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Decision(id="dec_1", title="t", rationale="r", status="bogus")  # type: ignore[arg-type]


def test_ts5_decision_status_literal_type_exports():
    # DecisionStatus Literal alias is re-exported for external consumers
    # (e.g. MCP server type hints).
    assert DecisionStatus is not None


# ---------------------------------------------------------------------------
# TS9 — DeterministicWorker emits Decision nodes from spec.decisions[]
# ---------------------------------------------------------------------------


def _spec_with_formalized_decisions() -> dict:
    return {
        "id": "11111111-aaaa-4444-bbbb-222222222222",
        "title": "TS9 Spec",
        "description": "desc",
        "context": "plain context without markdown Decisions",
        "functional_requirements": [
            {"id": "fr-a", "text": "FR A"},
            {"id": "fr-b", "text": "FR B"},
        ],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "test_scenarios": [],
        "business_rules": [],
        "api_contracts": [],
        "decisions": [
            {
                "id": "dec_ts9_one",
                "title": "Use Grafx",
                "rationale": "embedded graph DB fits our use case",
                "status": "active",
                "linked_requirements": ["fr-a"],
            },
            {
                "id": "dec_ts9_two",
                "title": "Cache em Redis",
                "rationale": "streaks need low-latency reads",
                "status": "active",
            },
            {
                "id": "dec_ts9_revoked",
                "title": "Use DuckDB",
                "rationale": "was the first idea — revoked",
                "status": "revoked",
            },
        ],
    }


def test_ts9_process_spec_emits_formalized_decisions():
    result = DeterministicWorker().process_spec(_spec_with_formalized_decisions())
    decision_nodes = [n for n in result.nodes if n.node_type == "Decision"]
    titles = {n.title for n in decision_nodes}
    assert "Use Grafx" in titles
    assert "Cache em Redis" in titles
    # Revoked decisions are NOT emitted (superseded/revoked are excluded).
    assert "Use DuckDB" not in titles
    refs = {n.title: n.source_artifact_ref for n in decision_nodes}
    spec_ref = "spec:11111111-aaaa-4444-bbbb-222222222222"
    assert refs == {
        "Use Grafx": f"{spec_ref}:decision:dec_ts9_one",
        "Cache em Redis": f"{spec_ref}:decision:dec_ts9_two",
    }
    for n in decision_nodes:
        assert n.source_confidence == 1.0
        assert n.priority_boost == 0.0


def test_ts9_explicit_linked_requirements_use_high_confidence_edge():
    """Only declared native requirement IDs produce decision links."""
    result = DeterministicWorker().process_spec(_spec_with_formalized_decisions())
    derives = [e for e in result.edges if e.edge_type == "derives_from"]
    assert derives  # sanity

    # Explicit fr-a ID produces exactly one derives_from edge with confidence=1.0.
    grafx_cids = [
        n.candidate_id for n in result.nodes
        if n.node_type == "Decision" and n.title == "Use Grafx"
    ]
    assert len(grafx_cids) == 1
    grafx_cid = grafx_cids[0]
    grafx_edges = [e for e in derives if e.from_candidate_id == grafx_cid]
    assert len(grafx_edges) == 1
    assert grafx_edges[0].confidence == 1.0

    # Unlinked decisions do not invent requirement relationships.
    cache_cids = [
        n.candidate_id for n in result.nodes
        if n.node_type == "Decision" and n.title == "Cache em Redis"
    ]
    assert len(cache_cids) == 1
    cache_cid = cache_cids[0]
    cache_edges = [e for e in derives if e.from_candidate_id == cache_cid]
    assert cache_edges == []


@pytest.mark.parametrize("structured", [True, False])
def test_context_markdown_is_not_converted_into_decisions(structured):
    spec = _spec_with_formalized_decisions()
    if not structured:
        spec["decisions"] = []
    context = "## Decisions\n- Use Grafx\n- Markdown only\n"
    spec["context"] = context
    result = DeterministicWorker().process_spec(spec)
    titles = {n.title for n in result.nodes if n.node_type == "Decision"}
    assert titles == ({"Use Grafx", "Cache em Redis"} if structured else set())
    assert spec["context"] == context
    assert not any(":decision_legacy:" in n.source_artifact_ref for n in result.nodes)
