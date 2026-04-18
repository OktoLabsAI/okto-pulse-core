"""Unit tests for the Layer 1 Deterministic Worker (spec c48a5c33).

Covers:
- Functional + technical requirements, acceptance criteria, business rules
  mapped to correct KG node types.
- `tests` edges fire on test_scenarios[].linked_criteria matches; missing
  linked_criteria produces a missing_link_candidate (fallback trigger).
- `implements` edges for api_contracts → FR via linked_requirements.
- `derives_from` low-confidence co-occurrence edges from `## Decisions`.
- `mentions` via tech_entities.yml whitelist.
- Content hash deterministic across runs on identical input.
- `layer=deterministic` + `rule_id` populated on every edge.
"""

from __future__ import annotations

from okto_pulse.core.kg.workers.deterministic_worker import (
    DeterministicWorker,
    WORKER_ID,
    _extract_decisions_from_context,
    _extract_tech_mentions,
    reset_tech_whitelist_cache,
)


def _spec_fixture() -> dict:
    return {
        "id": "11111111-aaaa-4444-bbbb-222222222222",
        "title": "Demo Spec",
        "description": "A spec to test the Layer 1 extractor",
        "context": (
            "## In Scope\n- Foo\n- Bar\n\n"
            "## Decisions\n"
            "- Use PostgreSQL for leaderboard\n"
            "- Cache streaks in Redis with 5min TTL\n"
            "- Badge rules as JSON in DB\n"
        ),
        "functional_requirements": [
            "User earns XP for eco-actions",
            "User level increases based on XP threshold",
            "Badge awarded for achievements",
        ],
        "technical_requirements": [
            {"text": "XP calc <100ms"},
            {"text": "Indexed leaderboard table"},
        ],
        "acceptance_criteria": [
            "Level formula: level * 1000 XP",
            "7-day streak gives 1.5x XP multiplier",
        ],
        "test_scenarios": [
            {
                "id": "ts_1",
                "title": "Level up correctness",
                "given": "User has 900 XP",
                "when": "Earns 200 XP",
                "then": "Level becomes 2",
                "linked_criteria": ["Level formula: level * 1000 XP"],
            },
            {
                "id": "ts_2",
                "title": "Scenario without criterion",
                "given": "Given",
                "when": "When",
                "then": "Then",
                "linked_criteria": [],  # triggers missing_link_candidate
            },
        ],
        "business_rules": [
            {
                "id": "br_xp_cap",
                "title": "XP Cap Per Day",
                "rule": "User cannot earn more than 500 XP per day",
            },
        ],
        "api_contracts": [
            {
                "method": "GET",
                "path": "/leaderboard",
                "description": "Top 100 users",
                "linked_requirements": ["User earns XP for eco-actions"],
            },
            {
                "method": "POST",
                "path": "/streaks",
                "description": "Streak reset endpoint",
                "linked_requirements": [],  # triggers missing_link_candidate
            },
        ],
    }


def test_extract_decisions_from_context_tolerates_case():
    ctx = "# Header\n## DECISIONS\n- Use PG\n- Use Redis\n## Out"
    assert _extract_decisions_from_context(ctx) == ["Use PG", "Use Redis"]


def test_extract_decisions_from_context_supports_star_bullets():
    ctx = "## Decisions\n* First\n* Second"
    assert _extract_decisions_from_context(ctx) == ["First", "Second"]


def test_extract_decisions_returns_empty_when_header_missing():
    assert _extract_decisions_from_context("## Other\n- x") == []


def test_tech_mentions_matches_canonical_and_alias():
    reset_tech_whitelist_cache()
    assert _extract_tech_mentions("Use PostgreSQL and pg") == ["PostgreSQL"]
    assert _extract_tech_mentions("Redis is fast") == ["Redis"]


def test_tech_mentions_case_insensitive():
    reset_tech_whitelist_cache()
    assert _extract_tech_mentions("use REDIS cache") == ["Redis"]


def test_tech_mentions_ignores_non_whitelisted():
    reset_tech_whitelist_cache()
    assert _extract_tech_mentions("MyCustomDB is great") == []


def test_process_spec_emits_all_node_types():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    types = {n.node_type for n in result.nodes}
    assert {"Entity", "Requirement", "Constraint", "Criterion",
            "TestScenario", "APIContract", "Decision"}.issubset(types)


def test_process_spec_edges_carry_v2_metadata():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    for edge in result.edges:
        assert edge.layer == "deterministic"
        assert edge.rule_id  # non-empty
        assert edge.created_by == WORKER_ID
        assert edge.fallback_reason == ""
    assert result.deterministic_edge_ratio() == 1.0


def test_process_spec_tests_edge_resolves_linked_criterion_by_text():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    tests_edges = [e for e in result.edges if e.edge_type == "tests"]
    assert len(tests_edges) == 1
    edge = tests_edges[0]
    assert edge.confidence == 1.0
    # from=TS_1 (first test scenario), to=AC_0 (level formula)
    assert edge.from_candidate_id.endswith("_ts_0")
    assert edge.to_candidate_id.endswith("_ac_0")


def test_process_spec_missing_linked_criteria_generates_candidate():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    missing = [c for c in result.missing_link_candidates
               if c.edge_type == "tests"]
    assert len(missing) == 1
    assert missing[0].reason == "no_criterion_match"
    assert missing[0].from_candidate_title == "Scenario without criterion"


def test_process_spec_implements_edge_to_fr():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    impl_edges = [e for e in result.edges if e.edge_type == "implements"]
    assert len(impl_edges) == 1
    assert impl_edges[0].confidence == 1.0


def test_process_spec_missing_linked_requirements_generates_candidate():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    missing_impl = [c for c in result.missing_link_candidates
                    if c.edge_type == "implements"]
    assert len(missing_impl) == 1
    assert missing_impl[0].reason == "no_requirement_match"


def test_process_spec_derives_from_cooccurrence():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    derives = [e for e in result.edges if e.edge_type == "derives_from"]
    # 3 decisions × 3 FRs = 9 edges, confidence 0.6 each.
    assert len(derives) == 9
    assert all(e.confidence == 0.6 for e in derives)


def test_process_spec_mentions_tech_whitelist_entities():
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_fixture())
    mentions = [e for e in result.edges if e.edge_type == "mentions"]
    # 2 decisions reference PG/Redis → 2 mentions (JSON in DB decision
    # doesn't mention a whitelisted entity, only Redis decision does).
    targets = {e.to_candidate_id for e in mentions}
    assert any("postgresql" in t for t in targets)
    assert any("redis" in t for t in targets)


def test_content_hash_stable_across_runs():
    spec = _spec_fixture()
    r1 = DeterministicWorker().process_spec(spec)
    r2 = DeterministicWorker().process_spec(spec)
    assert r1.content_hash == r2.content_hash


def test_content_hash_changes_when_spec_mutated():
    spec = _spec_fixture()
    r1 = DeterministicWorker().process_spec(spec)
    spec["title"] = "Demo Spec v2"
    r2 = DeterministicWorker().process_spec(spec)
    assert r1.content_hash != r2.content_hash


def test_process_sprint_emits_entity_and_outcome_criterion():
    sprint = {
        "id": "sprint-1234-5678",
        "title": "Sprint 1",
        "description": "First sprint",
        "objective": "Ship the worker",
        "expected_outcome": "Worker processes 3 specs successfully",
    }
    result = DeterministicWorker().process_sprint(sprint)
    types = [n.node_type for n in result.nodes]
    assert "Entity" in types
    assert "Criterion" in types


def test_process_sprint_emits_hierarchy_edge_via_belongs_to():
    """When the sprint has a spec_id FK, the worker emits a `belongs_to`
    edge from the sprint Entity to the parent Spec Entity (resolved cross-
    session at commit time)."""
    sprint = {
        "id": "sprint-1234-5678",
        "title": "Sprint 1",
        "spec_id": "spec-abcdef-1234-uuid",
    }
    result = DeterministicWorker().process_sprint(sprint)
    hierarchy = [e for e in result.edges if e.edge_type == "belongs_to"
                 and e.rule_id.startswith("belongs_to/sprint_to_spec")]
    assert len(hierarchy) == 1
    # Worker truncates the spec_id to the first 8 chars when building the
    # cross-session candidate id pointer.
    assert hierarchy[0].to_candidate_id == "spec_spec-abc_entity"


def test_process_card_bug_emits_violates_missing_link():
    bug = {
        "id": "bug-1234-5678",
        "title": "Silent rate limit",
        "description": "Fails silently",
        "card_type": "bug",
        "origin_task_id": "task-zzz",
    }
    result = DeterministicWorker().process_card(bug)
    assert any(n.node_type == "Bug" for n in result.nodes)
    missing = [c for c in result.missing_link_candidates if c.edge_type == "violates"]
    assert len(missing) == 1
    assert missing[0].reason == "origin_task_requires_cross_artifact_resolution"


def test_process_card_bug_without_origin_marks_no_origin_task():
    bug = {
        "id": "bug-x",
        "title": "Orphan bug",
        "card_type": "bug",
        # no origin_task_id
    }
    result = DeterministicWorker().process_card(bug)
    missing = [c for c in result.missing_link_candidates if c.edge_type == "violates"]
    assert len(missing) == 1
    assert missing[0].reason == "no_origin_task"


def test_process_artifact_dispatches_by_type():
    worker = DeterministicWorker()
    assert worker.process_artifact("spec", _spec_fixture()).nodes
    assert worker.process_artifact("sprint", {"id": "s-1", "title": "s"}).nodes
    assert worker.process_artifact("card", {"id": "c-1", "title": "c"}).nodes


def test_process_artifact_raises_on_unknown_type():
    import pytest as _pytest
    with _pytest.raises(ValueError, match="unknown artifact_type"):
        DeterministicWorker().process_artifact("ideation", {"id": "x"})
