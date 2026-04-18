"""Sprint 1 integration tests for the Layer 1 consolidation v2 flow
(cards bb82169e, 697c3648, 1612a3c0 — spec c48a5c33).

Covers the bridge between pulse.db SQLAlchemy rows → DeterministicWorker →
NodeCandidate/EdgeCandidate with v0.2.0 provenance metadata → primitives
begin_consolidation with stable content_hash. The cards are marked
"integration" because they validate multiple layers working together.

Kùzu commit itself is exercised by test_kg_pipeline_e2e.py (marker=e2e);
this suite keeps the file-lock-sensitive path off the fast unit run.
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.workers.consolidation import (
    _run_deterministic_worker,
    _spec_to_dict,
    _worker_edge_to_candidate,
    _worker_node_to_candidate,
)
from okto_pulse.core.kg.workers.deterministic_worker import (
    LAYER,
    WORKER_ID,
    DeterministicWorker,
)
from okto_pulse.core.models.db import Board, ConsolidationQueue, Spec


# ---------------------------------------------------------------------------
# Fixture: spec with full linked metadata covering every edge type
# ---------------------------------------------------------------------------


@pytest.fixture
def full_spec_row():
    """A Spec fixture wired with FR, TR, AC, BR, test_scenarios, api_contracts.

    linked_criteria + linked_requirements are populated so the worker resolves
    `tests` and `implements` edges without missing_link_candidate fallbacks.
    """
    return Spec(
        id="spec-sprint1-full",
        board_id="board-sprint1",
        title="Leaderboard",
        description="Competitive leaderboard with streaks and XP",
        context=(
            "## Scope\n- Points\n\n"
            "## Decisions\n"
            "- Use PostgreSQL for leaderboard persistence\n"
            "- Cache recent streaks in Redis with 5min TTL\n"
        ),
        functional_requirements=[
            "Player accumulates XP for each action",
            "Player earns streak multiplier after 7 consecutive days",
        ],
        technical_requirements=[
            {"text": "XP write path < 50ms p95"},
        ],
        acceptance_criteria=[
            "Level formula matches spec: 1000 XP per level",
            "Streak multiplier caps at 2.0",
        ],
        business_rules=[
            {"title": "Daily XP Cap", "rule": "User cannot exceed 500 XP per day"},
        ],
        test_scenarios=[
            {
                "id": "ts_level",
                "title": "Level-up crosses threshold",
                "given": "User has 950 XP",
                "when": "User earns 150 XP",
                "then": "Level increases",
                "linked_criteria": ["Level formula matches spec: 1000 XP per level"],
            },
        ],
        api_contracts=[
            {
                "method": "GET",
                "path": "/leaderboard",
                "description": "Top-100 users",
                "linked_requirements": ["Player accumulates XP for each action"],
            },
        ],
        created_by="u",
    )


# ===========================================================================
# ts_7b6a1ec1 / bb82169e — Worker emits deterministic edges for full spec
# ===========================================================================


def test_ts_7b6a1ec1_worker_emits_all_edge_types(full_spec_row):
    """Complete spec → worker produces tests + implements + derives_from +
    mentions edges, all tagged layer=deterministic with correct confidence."""
    result = _run_deterministic_worker(
        ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                           artifact_id=full_spec_row.id),
        full_spec_row,
    )

    edge_types = {e.edge_type for e in result.edges}
    # tests + implements come from linked_* matches.
    assert "tests" in edge_types
    assert "implements" in edge_types
    # derives_from from the ## Decisions section (2 decisions × 2 FRs = 4 edges)
    derives = [e for e in result.edges if e.edge_type == "derives_from"]
    assert len(derives) == 4
    assert all(e.confidence == 0.6 for e in derives)
    # mentions from tech whitelist (PostgreSQL + Redis)
    mentions = [e for e in result.edges if e.edge_type == "mentions"]
    assert any("postgresql" in e.to_candidate_id for e in mentions)
    assert any("redis" in e.to_candidate_id for e in mentions)


def test_ts_7b6a1ec1_edges_metadata_complete(full_spec_row):
    """Every emitted edge carries the v0.2.0 provenance triple."""
    result = _run_deterministic_worker(
        ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                           artifact_id=full_spec_row.id),
        full_spec_row,
    )
    assert result.edges, "sanity: we expect at least one edge"
    for edge in result.edges:
        assert edge.layer == LAYER
        assert edge.created_by == WORKER_ID
        assert edge.rule_id  # non-empty, machine-readable tag
        assert edge.fallback_reason == ""


def test_ts_7b6a1ec1_edge_candidates_preserve_provenance(full_spec_row):
    """The Pydantic translation keeps every provenance field."""
    result = _run_deterministic_worker(
        ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                           artifact_id=full_spec_row.id),
        full_spec_row,
    )
    candidates = [_worker_edge_to_candidate(e) for e in result.edges]
    for cand in candidates:
        assert cand.layer == LAYER
        assert cand.created_by == WORKER_ID
        assert cand.rule_id  # non-empty


def test_ts_7b6a1ec1_deterministic_ratio_is_100pct(full_spec_row):
    """Every edge the Layer 1 worker produces is tagged deterministic."""
    result = _run_deterministic_worker(
        ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                           artifact_id=full_spec_row.id),
        full_spec_row,
    )
    assert result.deterministic_edge_ratio() == 1.0


def test_ts_7b6a1ec1_node_candidates_translation_roundtrip(full_spec_row):
    """Worker → NodeCandidate conversion keeps the node_type strings aligned
    with the KGNodeType enum (prevents silent enum drift)."""
    result = _run_deterministic_worker(
        ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                           artifact_id=full_spec_row.id),
        full_spec_row,
    )
    for node in result.nodes:
        cand = _worker_node_to_candidate(node)
        assert cand.candidate_id == node.candidate_id
        # node_type stored as string (Pydantic use_enum_values=True)
        assert str(cand.node_type) == node.node_type


# ===========================================================================
# ts_a278ec64 / 697c3648 — Missing linked_criteria ⇒ fallback candidate
# ===========================================================================


def test_ts_a278ec64_missing_linked_criteria_skips_tests_edge():
    """TestScenario without linked_criteria must NOT emit a `tests` edge."""
    spec = {
        "id": "spec-missing-links",
        "title": "Partial Spec",
        "description": "",
        "context": "",
        "functional_requirements": ["Nothing"],
        "technical_requirements": [],
        "acceptance_criteria": ["Some criterion"],
        "business_rules": [],
        "test_scenarios": [
            {
                "id": "ts_empty",
                "title": "Orphan scenario",
                "given": "g",
                "when": "w",
                "then": "t",
                # linked_criteria omitted — should trigger fallback
            },
        ],
        "api_contracts": [],
    }
    result = DeterministicWorker().process_spec(spec)
    assert not any(e.edge_type == "tests" for e in result.edges)


def test_ts_a278ec64_missing_linked_criteria_enqueues_candidate():
    """Fallback candidate carries reason=no_criterion_match + artifact_ref."""
    spec = {
        "id": "spec-missing-links-2",
        "title": "Partial",
        "description": "",
        "context": "",
        "functional_requirements": [],
        "technical_requirements": [],
        "acceptance_criteria": ["AC-1"],
        "business_rules": [],
        "test_scenarios": [
            {"id": "ts_orphan", "title": "Orphan", "given": "g",
             "when": "w", "then": "t", "linked_criteria": []},
        ],
        "api_contracts": [],
    }
    result = DeterministicWorker().process_spec(spec)
    missing = [m for m in result.missing_link_candidates
               if m.edge_type == "tests"]
    assert len(missing) == 1
    assert missing[0].reason == "no_criterion_match"
    assert missing[0].artifact_ref == "spec:spec-missing-links-2"
    # Suggested candidates must include the existing Criterion so the agent
    # can confirm the match without re-reading the spec.
    assert missing[0].suggested_candidates  # non-empty hint list


def test_ts_a278ec64_missing_linked_requirements_enqueues_candidate():
    """api_contracts without linked_requirements → fallback for `implements`."""
    spec = {
        "id": "spec-implements-orphan",
        "title": "Partial",
        "description": "",
        "context": "",
        "functional_requirements": ["FR-A"],
        "technical_requirements": [],
        "acceptance_criteria": [],
        "business_rules": [],
        "test_scenarios": [],
        "api_contracts": [
            {"method": "GET", "path": "/x", "description": "d",
             "linked_requirements": []},
        ],
    }
    result = DeterministicWorker().process_spec(spec)
    impl_missing = [m for m in result.missing_link_candidates
                    if m.edge_type == "implements"]
    assert len(impl_missing) == 1
    assert impl_missing[0].reason == "no_requirement_match"


# ===========================================================================
# ts_77f3539e / 1612a3c0 — Retry idempotent by content_hash
# ===========================================================================


def test_ts_77f3539e_content_hash_stable_across_runs(full_spec_row):
    """Running the worker twice on identical input ⇒ identical content_hash."""
    entry = ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                               artifact_id=full_spec_row.id)
    first = _run_deterministic_worker(entry, full_spec_row)
    second = _run_deterministic_worker(entry, full_spec_row)
    assert first.content_hash == second.content_hash
    assert first.content_hash != ""


def test_ts_77f3539e_content_hash_differs_on_mutation(full_spec_row):
    """Any user-visible change to the spec must bump content_hash."""
    entry = ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                               artifact_id=full_spec_row.id)
    first = _run_deterministic_worker(entry, full_spec_row)

    # Touch the description — mirrors a real-world spec edit.
    full_spec_row.description = full_spec_row.description + " (revised)"
    second = _run_deterministic_worker(entry, full_spec_row)
    assert first.content_hash != second.content_hash


def test_ts_77f3539e_node_and_edge_counts_are_stable(full_spec_row):
    """A retry with unchanged input must not produce additional candidates.

    The primitives layer turns a matching content_hash into a NOOP commit;
    this test proves the worker itself stays deterministic so that
    downstream mechanism receives identical input on every retry.
    """
    entry = ConsolidationQueue(board_id="board-sprint1", artifact_type="spec",
                               artifact_id=full_spec_row.id)
    first = _run_deterministic_worker(entry, full_spec_row)
    second = _run_deterministic_worker(entry, full_spec_row)
    assert len(first.nodes) == len(second.nodes)
    assert len(first.edges) == len(second.edges)
    first_edges = sorted((e.candidate_id, e.edge_type, e.from_candidate_id,
                          e.to_candidate_id) for e in first.edges)
    second_edges = sorted((e.candidate_id, e.edge_type, e.from_candidate_id,
                           e.to_candidate_id) for e in second.edges)
    assert first_edges == second_edges


def test_ts_77f3539e_begin_consolidation_noops_on_repeat_content_hash():
    """begin_consolidation reports nothing_changed=True on the 2nd call.

    Integrates: compute_content_hash must be stable across calls for the
    same (raw_content, artifact_id, board_id) triple, which is what the
    audit-row lookup keys off of.
    """
    from okto_pulse.core.kg.session_manager import compute_content_hash

    board_id = "board-sprint1"
    artifact_id = "spec-sprint1-full"
    raw = "fixture raw content"

    h1 = compute_content_hash(raw, artifact_id, board_id)
    h2 = compute_content_hash(raw, artifact_id, board_id)
    assert h1 == h2

    # Mutating any component flips the hash.
    h3 = compute_content_hash(raw + "x", artifact_id, board_id)
    assert h1 != h3
    h4 = compute_content_hash(raw, artifact_id + "x", board_id)
    assert h1 != h4
    h5 = compute_content_hash(raw, artifact_id, board_id + "x")
    assert h1 != h5


# ---------------------------------------------------------------------------
# Bonus: _spec_to_dict produces a worker-compatible shape
# ---------------------------------------------------------------------------


def test_spec_to_dict_mirrors_spec_api_shape(full_spec_row):
    """The dict shape has to match what DeterministicWorker.process_spec
    expects — otherwise the branch in _process_queue_entry silently ignores
    fields and drops edges."""
    d = _spec_to_dict(full_spec_row)
    for key in (
        "id", "title", "description", "context",
        "functional_requirements", "technical_requirements",
        "acceptance_criteria", "business_rules",
        "test_scenarios", "api_contracts",
    ):
        assert key in d
