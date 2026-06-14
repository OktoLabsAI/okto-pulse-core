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

from types import SimpleNamespace

from okto_pulse.core.kg.primitives import _validate_local_edge_pair
from okto_pulse.core.kg.workers.deterministic_worker import (
    DeterministicWorker,
    WORKER_ID,
    WorkerResult,
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


def test_process_pre_spec_lineage_entities():
    worker = DeterministicWorker()
    story = worker.process_story({
        "id": "story-abcdef-123",
        "title": "Capture intake",
        "description": "Story details",
    })
    ideation = worker.process_ideation({
        "id": "idea-abcdef-123",
        "title": "Shape intake",
        "story_ids": ["story-abcdef-123"],
    })
    refinement = worker.process_refinement({
        "id": "ref-abcdef-123",
        "title": "Refine intake",
        "ideation_id": "idea-abcdef-123",
    })
    spec = worker.process_spec({
        **_spec_fixture(),
        "refinement_id": "ref-abcdef-123",
    })

    assert story.nodes[0].source_artifact_ref == "story:story-abcdef-123"
    assert any(
        e.from_candidate_id == "story_story-ab_entity"
        and e.to_candidate_id == "ideation_idea-abc_entity"
        for e in ideation.edges
    )
    assert any(
        e.from_candidate_id == "refinement_ref-abcd_entity"
        and e.to_candidate_id == "ideation_idea-abc_entity"
        for e in refinement.edges
    )
    assert any(
        e.from_candidate_id == "spec_11111111_entity"
        and e.to_candidate_id == "refinement_ref-abcd_entity"
        for e in spec.edges
    )


def test_process_spec_draft_marks_non_root_nodes_working():
    spec = {**_spec_fixture(), "status": "draft", "board_id": "board-layer"}
    result = DeterministicWorker().process_spec(spec)

    assert result.nodes
    assert all(
        node.graph_layer == "working"
        for node in result.nodes
        if not node.source_artifact_ref.startswith("board:")
        and node.source_artifact_ref != "tech_entities.yml"
    )
    assert all(
        node.maturity_status == "working_immature"
        for node in result.nodes
        if not node.source_artifact_ref.startswith("board:")
        and node.source_artifact_ref != "tech_entities.yml"
    )


def test_process_story_ready_is_working_only():
    result = DeterministicWorker().process_story({
        "id": "story-layer-123",
        "title": "Story layer",
        "description": "Working intake",
        "status": "ready",
    })

    story_node = next(
        node for node in result.nodes
        if node.source_artifact_ref == "story:story-layer-123"
    )
    assert story_node.graph_layer == "working"
    assert story_node.maturity_status == "working_immature"


def test_consolidation_lineage_nodes_keep_source_maturity_layer():
    from okto_pulse.core.kg.workers.consolidation import (
        _append_ideation_entity_node,
        _append_refinement_entity_node,
    )

    result = WorkerResult()
    _append_ideation_entity_node(
        result,
        SimpleNamespace(
            id="idea-layer-123",
            title="Done ideation",
            description="Ideation remains working",
            problem_statement="Problem",
            proposed_approach="Approach",
            status="done",
        ),
    )
    _append_refinement_entity_node(
        result,
        SimpleNamespace(
            id="ref-draft-123",
            title="Draft refinement",
            description="Draft refinement",
            analysis="Analysis",
            status="draft",
        ),
    )
    _append_refinement_entity_node(
        result,
        SimpleNamespace(
            id="ref-done-123",
            title="Done refinement",
            description="Done refinement",
            analysis="Analysis",
            status="done",
        ),
    )

    nodes = {node.source_artifact_ref: node for node in result.nodes}
    assert nodes["ideation:idea-layer-123"].graph_layer == "working"
    assert nodes["ideation:idea-layer-123"].maturity_status == "working_immature"
    assert nodes["refinement:ref-draft-123"].graph_layer == "working"
    assert nodes["refinement:ref-draft-123"].maturity_status == "working_immature"
    assert nodes["refinement:ref-done-123"].graph_layer == "canonical"
    assert nodes["refinement:ref-done-123"].maturity_status == "canonical_eligible"


def test_process_first_line_artifacts_attach_to_board_root_when_board_id_exists():
    worker = DeterministicWorker()
    board_id = "board-12345678"
    results = [
        (
            "story",
            worker.process_story({
                "id": "story-abcdef-123",
                "board_id": board_id,
                "title": "Capture intake",
            }),
            "story_story-ab_entity",
        ),
        (
            "ideation",
            worker.process_ideation({
                "id": "idea-abcdef-123",
                "board_id": board_id,
                "title": "Shape intake",
            }),
            "ideation_idea-abc_entity",
        ),
        (
            "refinement",
            worker.process_refinement({
                "id": "ref-abcdef-123",
                "board_id": board_id,
                "title": "Refine intake",
            }),
            "refinement_ref-abcd_entity",
        ),
        (
            "spec",
            worker.process_spec({
                **_spec_fixture(),
                "board_id": board_id,
            }),
            "spec_11111111_entity",
        ),
        (
            "sprint",
            worker.process_sprint({
                "id": "sprint-abcdef-123",
                "board_id": board_id,
                "title": "Sprint",
            }),
            "sprint_sprint-a_entity",
        ),
        (
            "card",
            worker.process_card({
                "id": "card-abcdef-123",
                "board_id": board_id,
                "title": "Card",
                "card_type": "normal",
            }),
            "card_card-abc_entity",
        ),
    ]

    for artifact_type, result, root_candidate_id in results:
        board_roots = [
            node for node in result.nodes
            if node.node_type == "Entity"
            and node.source_artifact_ref == f"board:{board_id}"
        ]
        assert len(board_roots) == 1, artifact_type
        assert any(
            edge.edge_type == "belongs_to"
            and edge.from_candidate_id == root_candidate_id
            and edge.to_candidate_id == board_roots[0].candidate_id
            and edge.rule_id == f"belongs_to/{artifact_type}_to_board@v2.0"
            for edge in result.edges
        ), artifact_type


def test_process_spec_child_source_refs_are_granular():
    spec = _spec_fixture()
    result = DeterministicWorker().process_spec(spec)
    spec_ref = f"spec:{spec['id']}"

    root_nodes = [
        n for n in result.nodes
        if n.node_type == "Entity" and n.source_artifact_ref == spec_ref
    ]
    assert len(root_nodes) == 1

    child_types = {
        "Requirement", "Constraint", "Criterion",
        "TestScenario", "APIContract", "Decision",
    }
    child_refs = [
        n.source_artifact_ref for n in result.nodes if n.node_type in child_types
    ]
    assert spec_ref not in child_refs
    assert len(child_refs) == len(set(child_refs))
    assert {
        f"spec:{spec['id']}:fr:0",
        f"spec:{spec['id']}:fr:1",
        f"spec:{spec['id']}:tr:0",
        f"spec:{spec['id']}:tr:1",
        f"spec:{spec['id']}:ac:0",
        f"spec:{spec['id']}:ac:1",
        f"spec:{spec['id']}:business_rule:br_xp_cap",
        f"spec:{spec['id']}:test_scenario:ts_1",
        f"spec:{spec['id']}:test_scenario:ts_2",
        f"spec:{spec['id']}:api_contract:0",
        f"spec:{spec['id']}:api_contract:1",
        f"spec:{spec['id']}:decision_legacy:0",
        f"spec:{spec['id']}:decision_legacy:1",
        f"spec:{spec['id']}:decision_legacy:2",
    }.issubset(set(child_refs))


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


def test_process_spec_projects_architecture_designs_without_new_node_types():
    spec = _spec_fixture()
    spec["architecture_designs"] = [
        {
            "id": "arch_runtime",
            "title": "Runtime Architecture",
            "global_description": "Checkout runs through a local API boundary.",
            "entities": [
                {
                    "id": "entity_api",
                    "name": "Checkout API",
                    "entity_type": "service",
                    "responsibility": "Own checkout orchestration.",
                    "technologies": ["FastAPI"],
                }
            ],
            "interfaces": [
                {
                    "id": "iface_checkout",
                    "name": "POST /checkout",
                    "description": "Accepts checkout requests.",
                    "protocol": "REST",
                    "request_schema": {"cart_id": "string"},
                }
            ],
            "diagrams": [
                {
                    "id": "diagram_context",
                    "title": "Context",
                    "diagram_type": "context",
                    "format": "excalidraw_json",
                    "content_hash": "abc",
                }
            ],
        }
    ]

    result = DeterministicWorker().process_spec(spec)

    by_title = {node.title: node for node in result.nodes}
    assert by_title["Runtime Architecture"].node_type == "Entity"
    assert by_title["Checkout API"].node_type == "Entity"
    assert by_title["POST /checkout"].node_type == "APIContract"
    architecture_refs = {
        by_title["Runtime Architecture"].source_artifact_ref,
        by_title["Checkout API"].source_artifact_ref,
        by_title["POST /checkout"].source_artifact_ref,
    }
    assert architecture_refs == {
        "architecture_design:arch_runtime",
        "architecture_design:arch_runtime:entity:entity_api",
        "architecture_design:arch_runtime:interface:iface_checkout",
    }
    assert "Checkout runs through" in result.raw_content
    assert "cart_id" in result.raw_content
    assert any(
        edge.rule_id.startswith("belongs_to/architecture_interface")
        for edge in result.edges
    )


def test_process_card_projects_architecture_designs():
    card = {
        "id": "card-arch-123",
        "title": "Implement checkout boundary",
        "description": "Task details",
        "card_type": "normal",
        "spec_id": "spec-arch-123",
        "architecture_designs": [
            {
                "id": "arch_card",
                "title": "Task Architecture",
                "global_description": "Card snapshot from the spec.",
                "entities": [{"name": "Payment Adapter"}],
                "interfaces": [{"name": "PaymentGateway.authorize"}],
            }
        ],
    }

    result = DeterministicWorker().process_card(card)

    assert any(node.title == "Task Architecture" for node in result.nodes)
    assert any(node.title == "Payment Adapter" for node in result.nodes)
    assert any(node.title == "PaymentGateway.authorize" for node in result.nodes)
    assert "Card snapshot from the spec." in result.raw_content


def test_process_bug_card_architecture_belongs_to_bug_is_valid():
    bug = {
        "id": "bug-arch-123",
        "title": "Regression in checkout boundary",
        "description": "Bug details",
        "card_type": "bug",
        "architecture_designs": [
            {
                "id": "arch_bug",
                "title": "Bug Architecture Evidence",
                "global_description": "Diagnostic architecture attached to the bug.",
                "entities": [{"name": "Checkout Adapter"}],
            }
        ],
    }

    result = DeterministicWorker().process_card(bug)

    node_types = {node.candidate_id: node.node_type for node in result.nodes}
    architecture_edge = next(
        edge
        for edge in result.edges
        if edge.rule_id.startswith("belongs_to/architecture_design")
    )
    assert node_types[architecture_edge.from_candidate_id] == "Entity"
    assert node_types[architecture_edge.to_candidate_id] == "Bug"
    for edge in result.edges:
        if (
            edge.from_candidate_id in node_types
            and edge.to_candidate_id in node_types
        ):
            _validate_local_edge_pair(
                edge.edge_type,
                node_types[edge.from_candidate_id],
                node_types[edge.to_candidate_id],
                session_id="test-bug-architecture",
            )


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


def test_process_sprint_serializes_lane_metadata_into_context_and_hash():
    base = {
        "id": "sprint-lane-1234",
        "title": "Post-closure lane",
        "description": "Fix a closed delivery bug",
        "objective": "Close regression",
        "spec_id": "spec-abcdef-1234-uuid",
    }
    normal = DeterministicWorker().process_sprint({**base, "lane_type": "normal"})
    hotfix = DeterministicWorker().process_sprint({
        **base,
        "lane_type": "hotfix",
        "origin_sprint_id": "closed-sprint-1",
        "origin_bug_id": "bug-1",
        "normal_sprint_created": False,
    })

    assert normal.content_hash != hotfix.content_hash
    hotfix_entity = next(n for n in hotfix.nodes if n.node_type == "Entity")
    assert "lane_type=hotfix" in hotfix_entity.context
    assert "origin_sprint_id=closed-sprint-1" in hotfix.raw_content
    assert "origin_bug_id=bug-1" in hotfix.raw_content
    assert "normal_sprint_created=false" in hotfix.raw_content


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


def test_process_card_bug_linked_test_tasks_are_observable():
    bug = {
        "id": "bug-1234-5678",
        "title": "Silent rate limit",
        "description": "Fails silently",
        "card_type": "bug",
        "origin_task_id": "task-zzz",
        "linked_test_task_ids": ["test-card-1", "test-card-2"],
    }
    result = DeterministicWorker().process_card(bug)
    missing = [
        c for c in result.missing_link_candidates
        if c.reason == "linked_test_task_requires_cross_artifact_resolution"
    ]
    assert len(missing) == 2
    assert all(c.edge_type == "tests" for c in missing)
    assert {tuple(c.suggested_candidates) for c in missing} == {
        ("test_task:test-card-1",),
        ("test_task:test-card-2",),
    }
    assert "Origin task: task-zzz" in result.raw_content
    assert "Linked test tasks" in result.raw_content


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
    assert worker.process_artifact("story", {
        "id": "story-1",
        "title": "s",
        "description": "d",
    }).nodes
    assert worker.process_artifact("ideation", {
        "id": "idea-1",
        "title": "i",
        "story_ids": ["story-1"],
    }).nodes
    assert worker.process_artifact("refinement", {
        "id": "ref-1",
        "title": "r",
        "ideation_id": "idea-1",
    }).nodes
    assert worker.process_artifact("spec", _spec_fixture()).nodes
    assert worker.process_artifact("sprint", {"id": "s-1", "title": "s"}).nodes
    assert worker.process_artifact("card", {"id": "c-1", "title": "c"}).nodes


def test_process_artifact_raises_on_unknown_type():
    import pytest as _pytest
    with _pytest.raises(ValueError, match="unknown artifact_type"):
        DeterministicWorker().process_artifact("unknown", {"id": "x"})


# ---------------------------------------------------------------------------
# Priority boost propagation — v0.3.1 (spec 0eb51d3e) — TS6 / TS7
# ---------------------------------------------------------------------------


def test_ts6_process_card_critical_sets_boost_020():
    """TS6 (AC1): card.priority=critical → root EmittedNode boost=0.20."""
    card = {
        "id": "card-ts6-critical",
        "title": "Crit card",
        "description": "body",
        "card_type": "normal",
        "priority": "critical",
    }
    result = DeterministicWorker().process_card(card)
    root = [n for n in result.nodes if n.source_artifact_ref == f"card:{card['id']}"]
    assert len(root) == 1
    assert root[0].priority_boost == 0.20


def test_ts6_process_card_none_priority_zero_boost():
    """TS6 (AC2): card.priority=None → boost=0.0."""
    card = {
        "id": "card-ts6-none",
        "title": "No-priority card",
        "card_type": "normal",
        "priority": None,
    }
    result = DeterministicWorker().process_card(card)
    assert result.nodes[0].priority_boost == 0.0


def test_ts6_process_card_boost_only_on_root_not_hierarchy():
    """TS6 BR3: hierarchy edges' belongs_to target nodes don't carry boost.

    process_card emits the root Entity/Bug node plus belongs_to edges. No
    *node* other than the root is created by process_card, so the only
    node carrying a non-zero boost is the root. (Sprint/Spec anchor nodes
    are created by process_sprint / process_spec, which we test below.)
    """
    card = {
        "id": "card-ts6-hier",
        "title": "Hierarchical card",
        "card_type": "normal",
        "priority": "high",
        "sprint_id": "sprint-xyz",
        "spec_id": "spec-xyz",
    }
    result = DeterministicWorker().process_card(card)
    boosted = [n for n in result.nodes if n.priority_boost > 0.0]
    assert len(boosted) == 1
    assert boosted[0].candidate_id.endswith("_entity")
    assert boosted[0].priority_boost == 0.10


def test_ts6_process_card_bug_with_priority_propagates():
    """process_card for bug cards also propagates the boost to the Bug node."""
    bug = {
        "id": "bug-ts6",
        "title": "High-sev bug",
        "card_type": "bug",
        "priority": "very_high",
        "origin_task_id": "task-abc",
    }
    result = DeterministicWorker().process_card(bug)
    bug_nodes = [n for n in result.nodes if n.node_type == "Bug"]
    assert len(bug_nodes) == 1
    assert bug_nodes[0].priority_boost == 0.15


def test_ts6_process_card_unknown_priority_zero_boost():
    """process_card with unknown priority falls back to boost=0.0."""
    card = {
        "id": "card-ts6-unknown",
        "title": "Mystery",
        "card_type": "normal",
        "priority": "urgent",  # not in enum
    }
    result = DeterministicWorker().process_card(card)
    assert result.nodes[0].priority_boost == 0.0


def test_ts7_process_spec_emits_zero_boost_everywhere():
    """TS7 (AC4): every EmittedNode from process_spec has priority_boost=0.0.

    Specs carry no priority field in the domain model, so all derived nodes
    (Entity anchor, Requirement, Constraint, Criterion, TestScenario,
    APIContract) must start at 0.0 — boosting only the root card's node.
    """
    result = DeterministicWorker().process_spec(_spec_fixture())
    assert result.nodes  # sanity: spec fixture produces nodes
    assert all(n.priority_boost == 0.0 for n in result.nodes), (
        f"non-zero boost leaked into spec-derived nodes: "
        f"{[(n.node_type, n.priority_boost) for n in result.nodes if n.priority_boost]}"
    )


def test_ts7_process_sprint_emits_zero_boost():
    """Sprints also have no priority source → all derived nodes boost=0.0."""
    sprint = {
        "id": "sprint-ts7",
        "title": "Sprint 99",
        "description": "d",
        "objective": "o",
        "expected_outcome": "e",
    }
    result = DeterministicWorker().process_sprint(sprint)
    assert all(n.priority_boost == 0.0 for n in result.nodes)


# ---------------------------------------------------------------------------
# IMPL-3: fr_id-aware edge resolution (spec R3b cc8e9252)
# AC4 behavioral tests — validator reproduces each case.
# ---------------------------------------------------------------------------


def _spec_with_fr_ids() -> dict:
    """Spec where functional_requirements carry canonical fr_id fields
    (as persisted by IMPL-1).  linked_requirements on the api_contract and
    decision reference the fr_id strings, NOT the full text, so the
    pre-IMPL-3 text/int lookups would both fail to resolve them.
    """
    return {
        "id": "aaaabbbb-1111-4444-cccc-ddddeeee0000",
        "title": "FR-id-aware Spec",
        "description": "Tests fr_id resolution in implements and derives_from",
        "context": "## In Scope\n- FR-id resolution\n",
        "functional_requirements": [
            {
                "id": "fr-canonical-001",
                "text": "System shall emit a deterministic edge when fr_id is referenced",
            },
            {
                "id": "fr-canonical-002",
                "text": "System shall fall back to co-occurrence for unlinked decisions",
            },
        ],
        "api_contracts": [
            {
                "method": "POST",
                "path": "/edge-resolution",
                "description": "Endpoint that implements the first FR by id",
                # (a): linked_requirements contains the fr_id, not the FR text.
                "linked_requirements": ["fr-canonical-001"],
            },
        ],
        "decisions": [
            {
                "title": "Use fr_id for cross-section links",
                "rationale": "Canonical ids are stable; text changes over time.",
                "status": "active",
                # (b): linked_requirements contains the fr_id, not an int index.
                "linked_requirements": ["fr-canonical-002"],
            },
        ],
    }


def test_impl3_api_contract_implements_resolves_via_fr_id():
    """(a) api_contract.linked_requirements = [fr_id] → deterministic
    `implements` edge with confidence 1.0.  The edge must NOT appear in
    missing_link_candidates.
    """
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_with_fr_ids())

    impl_edges = [e for e in result.edges if e.edge_type == "implements"]
    assert len(impl_edges) == 1, (
        f"Expected exactly 1 implements edge; got {len(impl_edges)}: {impl_edges}"
    )
    edge = impl_edges[0]
    assert edge.confidence == 1.0
    # The target must resolve to the node for fr-canonical-001 (first FR, index 0).
    assert edge.to_candidate_id.endswith("_fr_0"), (
        f"Expected target to end with '_fr_0', got: {edge.to_candidate_id}"
    )

    # Must NOT produce a missing_link_candidate for this api_contract.
    impl_missing = [
        c for c in result.missing_link_candidates
        if c.edge_type == "implements"
    ]
    assert impl_missing == [], (
        f"fr_id resolution produced unexpected missing_link_candidates: {impl_missing}"
    )


def test_impl3_decision_derives_from_resolves_via_fr_id():
    """(b) decision.linked_requirements = [fr_id] → deterministic
    `derives_from` edge with confidence 1.0 (explicit_link), targeting only
    the one referenced FR.  The other FR must NOT receive a derives_from edge
    (no fallback co-occurrence when the explicit set is non-empty).
    """
    worker = DeterministicWorker()
    result = worker.process_spec(_spec_with_fr_ids())

    derives_edges = [e for e in result.edges if e.edge_type == "derives_from"]

    # Exactly one derives_from edge: fr-canonical-002 (index 1).
    assert len(derives_edges) == 1, (
        f"Expected exactly 1 derives_from edge; got {len(derives_edges)}: {derives_edges}"
    )
    edge = derives_edges[0]
    assert edge.confidence == 1.0, (
        f"Expected confidence 1.0 (explicit_link), got {edge.confidence}"
    )
    assert "explicit_link" in edge.rule_id, (
        f"Expected rule_id to contain 'explicit_link', got: {edge.rule_id}"
    )
    # Target must be the second FR (fr-canonical-002, index 1).
    assert edge.to_candidate_id.endswith("_fr_1"), (
        f"Expected target to end with '_fr_1', got: {edge.to_candidate_id}"
    )


def test_impl3_legacy_text_resolution_still_works():
    """Regression: specs written before IMPL-1 (no id on FR dict, text refs)
    must still resolve the `implements` edge via text lookup.
    """
    spec = {
        "id": "legacy-spec-0000-1111-2222",
        "title": "Legacy spec",
        "description": "Pre-IMPL-1 spec with text-based linked_requirements",
        "context": "",
        "functional_requirements": [
            "User can log in",  # plain string, no id field
        ],
        "api_contracts": [
            {
                "method": "POST",
                "path": "/login",
                "description": "Login endpoint",
                "linked_requirements": ["User can log in"],  # text ref
            },
        ],
    }
    result = DeterministicWorker().process_spec(spec)
    impl_edges = [e for e in result.edges if e.edge_type == "implements"]
    assert len(impl_edges) == 1
    assert impl_edges[0].confidence == 1.0
    assert not [c for c in result.missing_link_candidates if c.edge_type == "implements"]


def test_impl3_legacy_int_index_resolution_still_works():
    """Regression: specs written before IMPL-1 (no id on FR dict, int index
    refs on decisions) must still resolve `derives_from` via positional int.
    """
    spec = {
        "id": "legacy-spec-int-3333-4444",
        "title": "Legacy int-index spec",
        "description": "Pre-IMPL-1 spec with int-index linked_requirements on decision",
        "context": "",
        "functional_requirements": [
            "FR zero",
            "FR one",
        ],
        "decisions": [
            {
                "title": "Only link to first FR by int index",
                "rationale": "Legacy int-index reference",
                "status": "active",
                "linked_requirements": [0],  # positional int
            },
        ],
    }
    result = DeterministicWorker().process_spec(spec)
    derives_edges = [e for e in result.edges if e.edge_type == "derives_from"]
    # Only one edge (index 0 resolves to fr_0); fr_1 is skipped.
    assert len(derives_edges) == 1
    edge = derives_edges[0]
    assert edge.confidence == 1.0
    assert "explicit_link" in edge.rule_id
    assert edge.to_candidate_id.endswith("_fr_0")
