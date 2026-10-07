"""Native rebuild identity comes from verified history, never an empty graph."""
from dataclasses import replace

import pytest
from okto_pulse.core.kg.cognitive_rebuild import select_decision_heads, matches_literal_decision
from okto_pulse.core.kg.schemas import NodeCandidate
from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord


def candidate():
    return NodeCandidate(candidate_id="candidate", node_type="Decision", title="Choice",
        content="Current", source_artifact_ref="spec:spec:decision:d")


def record(node_id, generation, **values):
    return CognitiveSourceRecord(board_id="board", node_id=node_id, node_type="Decision",
        generation=generation, payload=dict(source_artifact_ref="spec:spec:decision:d",
            generation=generation, title="Choice", content="Current",
            graph_layer="canonical", maturity_status="canonical_eligible", **values))


def test_selects_current_identity_without_recreating_generation_zero():
    current = record("new", 1)
    old = record("old", 0, superseded_by="new")
    assert select_decision_heads("board", {"candidate": candidate()}, (old, current)) == {
        "spec:spec:decision:d": current}
    assert matches_literal_decision(candidate(), current)


@pytest.mark.parametrize("records", [
    (record("old", 0), record("new", 1)),
    (record("old", 0, superseded_by="missing"),),
])
def test_ambiguous_or_missing_active_identity_is_refused(records):
    with pytest.raises(ValueError, match="active_identity_ambiguous"):
        select_decision_heads("board", {"candidate": candidate()}, records)


def test_foreign_board_is_refused():
    foreign = replace(record("node", 0), board_id="other", record_fingerprint="")
    with pytest.raises(ValueError, match="cross_board"):
        select_decision_heads("board", {"candidate": candidate()}, (foreign,))


def test_new_source_without_history_still_has_no_recovered_identity():
    assert select_decision_heads("board", {"candidate": candidate()}, ()) == {}


def test_changed_source_must_follow_normal_supersedence():
    changed = candidate().model_copy(update={"content": "Changed"})
    assert not matches_literal_decision(changed, record("node", 1))


def test_curated_content_is_preserved_but_maturity_change_is_not_ignored():
    curated = record("node", 1, human_curated=True)
    assert matches_literal_decision(candidate().model_copy(update={"content": "New source"}), curated)
    assert not matches_literal_decision(candidate().model_copy(update={"graph_layer": "working"}), curated)

@pytest.mark.asyncio
@pytest.mark.parametrize("agent,context,deferred,overrides", [
    ("agent:executor", object(), True, {}),
    ("system:historical_consolidation", None, True, {}),
    ("system:historical_consolidation", object(), False, {}),
    ("system:historical_consolidation", object(), True, {"candidate": {"candidate_id": "candidate", "operation": "ADD", "confidence": 1.0, "reason": "Override"}}),
])
async def test_recovery_cannot_bypass_worker_transaction_boundary(
    monkeypatch, agent, context, deferred, overrides,
):
    from types import SimpleNamespace
    from okto_pulse.core.kg import primitives
    from okto_pulse.core.kg.schemas import CommitConsolidationRequest
    async def session(*args, **kwargs):
        return SimpleNamespace(artifact_type="spec")
    monkeypatch.setattr(primitives, "get_kg_registry", lambda: object())
    monkeypatch.setattr(primitives, "_require_open_session", session)
    with pytest.raises(ValueError, match="cognitive_rebuild_worker_transaction_required"):
        await primitives.commit_consolidation(
            CommitConsolidationRequest(session_id="test-session", summary_text="Recovery",
                agent_overrides=overrides),
            agent_id=agent, db=context, defer_session_finalization=deferred,
            rebuild_cognitive=True,
        )


@pytest.mark.parametrize("field,value", [
    ("content", "Wrong"), ("generation", 0), ("superseded_by", "another"),
    ("source_artifact_ref", "spec:foreign:decision:d"), ("human_curated", True),
])
def test_existing_graph_identity_conflict_is_refused(field, value):
    from okto_pulse.core.kg.cognitive_rebuild import require_existing_decision_matches
    head = record("node", 1)
    with pytest.raises(ValueError, match="existing_identity_conflict"):
        require_existing_decision_matches(head, {**head.payload, field: value})


def test_existing_identity_retains_ephemeral_recovery_session():
    from okto_pulse.core.kg.cognitive_rebuild import require_existing_decision_matches
    head = record("node", 1)
    require_existing_decision_matches(head, {**head.payload, "source_session_id": "recovery"})
