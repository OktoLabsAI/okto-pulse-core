from __future__ import annotations

from types import SimpleNamespace

from okto_pulse.core.kg.connectivity_guard import (
    CONNECTIVITY_ERROR_CODE,
    SAFE_CONNECTIVITY_METRIC_LABELS,
    InMemoryConnectivityMetricSink,
    KGConnectivityOutcome,
    KGConnectivityRuleRegistry,
    KGNodeConnectivityGuard,
    KGNodeRef,
    SourceResolutionStatus,
    WriterClass,
    classify_writer_path,
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


def test_registry_exposes_required_resolution_statuses_and_owner_table():
    registry = KGConnectivityRuleRegistry()

    assert {
        item.value for item in SourceResolutionStatus
    } == {
        "resolved_in_batch",
        "resolved_existing_node",
        "unresolved_source_ref",
        "ambiguous_source_ref",
        "source_type_not_supported",
        "deferred_degraded_graph",
        "allowlisted_technical_root",
    }

    entity_rule = registry.get_rule("Entity", "deterministic_worker")
    requirement_rule = registry.get_rule("Requirement", "deterministic_worker")
    decision_rule = registry.get_rule("Decision", "commit_consolidation")
    learning_rule = registry.get_rule("Learning", "commit_consolidation")
    alternative_rule = registry.get_rule("Alternative", "commit_consolidation")
    assumption_rule = registry.get_rule("Assumption", "commit_consolidation")
    constraint_rule = registry.get_rule("Constraint", "deterministic_worker")
    bug_rule = registry.get_rule("Bug", "deterministic_worker")

    assert entity_rule.allowed_new_node_writers == (WriterClass.DETERMINISTIC,)
    assert requirement_rule.allowed_new_node_writers == (WriterClass.DETERMINISTIC,)
    assert constraint_rule.allowed_new_node_writers == (WriterClass.DETERMINISTIC,)
    assert bug_rule.allowed_new_node_writers == (WriterClass.DETERMINISTIC,)
    assert WriterClass.COGNITIVE in decision_rule.allowed_new_node_writers
    assert WriterClass.COGNITIVE in learning_rule.allowed_new_node_writers
    assert alternative_rule.allowed_new_node_writers == (WriterClass.COGNITIVE,)
    assert assumption_rule.semantic_target_policy == "provenance_only_v1"


def test_isolated_learning_candidate_fails_with_safe_violation_and_metric():
    sink = InMemoryConnectivityMetricSink()
    guard = KGNodeConnectivityGuard(metric_sink=sink)

    result = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        generation_id="gen-1",
        nodes=[_node("learn-1", "Learning", "card:bug:bug-1")],
        edges=[],
    )

    assert result.passed is False
    assert result.outcome == KGConnectivityOutcome.REJECTED
    assert len(result.violations) == 1
    violation = result.violations[0]
    assert violation.error_code == CONNECTIVITY_ERROR_CODE
    assert violation.node_type == "Learning"
    assert violation.candidate_id == "learn-1"
    assert violation.source_artifact_ref == "card:bug:bug-1"
    assert violation.source_resolution_status == SourceResolutionStatus.UNRESOLVED_SOURCE_REF
    assert violation.safe_for_api() is True
    assert violation.safe_for_metric() is True
    assert "card:bug:bug-1" in violation.to_safe_dict().values()
    assert "title" not in violation.to_safe_dict()
    assert "content" not in violation.to_safe_dict()

    assert len(sink.events) == 1
    labels = sink.events[0].labels()
    assert tuple(labels.keys()) == SAFE_CONNECTIVITY_METRIC_LABELS
    assert labels == {
        "board_id": "board-1",
        "node_type": "Learning",
        "writer_path": "commit_consolidation",
        "outcome": "rejected",
        "reason": "missing_required_edge",
        "source_resolution_status": "unresolved_source_ref",
        "generation_id": "gen-1",
    }


def test_connectivity_metrics_emit_safe_labels_for_all_outcomes():
    sink = InMemoryConnectivityMetricSink()
    guard = KGNodeConnectivityGuard(metric_sink=sink)

    guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        generation_id="gen-pass",
        nodes=[_node("req-1", "Requirement", "spec:spec-1:fr:0")],
        edges=[_edge("e-1", "belongs_to", "req-1", "kg:entity_spec_1")],
        existing_node_refs=[
            KGNodeRef(ref_id="entity_spec_1", node_type="Entity"),
        ],
    )
    guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        generation_id="gen-reject",
        nodes=[
            _node(
                "learn-1",
                "Learning",
                "card:bug:bug-1:email:secret@example.com:payload:{hidden}",
            )
        ],
        edges=[],
    )
    guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="recovery_needed",
        generation_id="gen-deferred",
        nodes=[
            _node(
                "learn-2",
                "Learning",
                "card:bug:bug-2:description:private incident text",
            )
        ],
        edges=[],
    )
    guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        generation_id="gen-allowlisted",
        nodes=[_node("redis", "Entity", "tech_entities.yml")],
        edges=[],
    )

    assert [event.outcome for event in sink.events] == [
        "passed",
        "rejected",
        "deferred",
        "allowlisted",
    ]
    assert [event.generation_id for event in sink.events] == [
        "gen-pass",
        "gen-reject",
        "gen-deferred",
        "gen-allowlisted",
    ]

    forbidden_fragments = (
        "title",
        "content",
        "description",
        "payload",
        "secret@example.com",
        "private incident text",
        "source_artifact_ref",
        "{hidden}",
    )
    for event in sink.events:
        labels = event.labels()
        assert tuple(labels.keys()) == SAFE_CONNECTIVITY_METRIC_LABELS
        label_blob = " ".join(f"{key}={value}" for key, value in labels.items())
        assert not any(fragment in label_blob for fragment in forbidden_fragments)


def test_same_batch_entity_provenance_edge_passes_guard():
    guard = KGNodeConnectivityGuard()

    result = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[
            _node("board-root", "Entity", "board:board-1"),
            _node("card-root", "Entity", "card:card-1"),
        ],
        edges=[_edge("e-1", "belongs_to", "card-root", "board-root")],
    )

    assert result.passed is True
    assert result.violations == ()
    assert result.allowlisted_roots == ("board-root",)

    result = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("card-root", "Entity", "card:card-1")],
        edges=[
            _edge("e-1", "belongs_to", "card-root", "kg:entity_board_1"),
        ],
        existing_node_refs=[
            KGNodeRef(ref_id="entity_board_1", node_type="Entity", source_artifact_ref="board:board-1"),
        ],
    )

    assert result.passed is True
    assert result.violations == ()


def test_unresolved_required_endpoint_fails_contextually():
    guard = KGNodeConnectivityGuard()

    result = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("req-1", "Requirement", "spec:spec-1:fr:0")],
        edges=[_edge("e-1", "belongs_to", "req-1", "kg:missing-entity")],
        existing_node_refs=[],
    )

    assert result.passed is False
    violation = result.violations[0]
    assert violation.candidate_id == "req-1"
    assert violation.required_edge == "belongs_to:outgoing:Entity|Bug"
    assert violation.source_resolution_status == SourceResolutionStatus.UNRESOLVED_SOURCE_REF
    assert violation.reason == "unresolved_required_endpoint"


def test_degraded_graph_defers_without_passing_connectivity():
    guard = KGNodeConnectivityGuard()

    result = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="recovery_needed",
        nodes=[_node("learn-1", "Learning", "card:bug:bug-1")],
        edges=[],
    )

    assert result.passed is False
    assert result.outcome == KGConnectivityOutcome.DEFERRED
    assert result.violations[0].source_resolution_status == (
        SourceResolutionStatus.DEFERRED_DEGRADED_GRAPH
    )


def test_writer_owner_precedence_blocks_wrong_creation_path():
    guard = KGNodeConnectivityGuard()

    result = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        nodes=[_node("req-1", "Requirement", "spec:spec-1:fr:0")],
        edges=[_edge("e-1", "belongs_to", "req-1", "kg:entity_spec_1")],
        existing_node_refs=[KGNodeRef(ref_id="entity_spec_1", node_type="Entity")],
    )

    assert result.passed is False
    assert result.violations[0].reason == "writer_not_connectivity_owner"
    assert result.violations[0].source_resolution_status == (
        SourceResolutionStatus.SOURCE_TYPE_NOT_SUPPORTED
    )


def test_dual_origin_owner_table_enforces_writer_polarity_for_core_types():
    guard = KGNodeConnectivityGuard()
    registry = KGConnectivityRuleRegistry()

    assert registry.get_rule("Entity").allowed_new_node_writers == (
        WriterClass.DETERMINISTIC,
    )
    assert registry.get_rule("Requirement").allowed_new_node_writers == (
        WriterClass.DETERMINISTIC,
    )
    assert registry.get_rule("Decision").allowed_new_node_writers == (
        WriterClass.COGNITIVE,
        WriterClass.DETERMINISTIC,
    )
    assert registry.get_rule("Learning").allowed_new_node_writers == (
        WriterClass.COGNITIVE,
        WriterClass.DETERMINISTIC,
    )

    for node_type in ("Entity", "Requirement"):
        wrong_writer = guard.validate(
            board_id="board-1",
            writer_path="commit_consolidation",
            kg_health_state="healthy",
            nodes=[_node("candidate-1", node_type, f"{node_type.lower()}:1")],
            edges=[],
            existing_node_refs=[],
        )

        assert wrong_writer.passed is False
        assert wrong_writer.violations[0].reason == "writer_not_connectivity_owner"
        assert wrong_writer.violations[0].node_type == node_type

    deterministic_requirement = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("req-1", "Requirement", "spec:spec-1:fr:0")],
        edges=[_edge("e-1", "belongs_to", "req-1", "kg:entity_spec_1")],
        existing_node_refs=[
            KGNodeRef(ref_id="entity_spec_1", node_type="Entity"),
        ],
    )

    assert deterministic_requirement.passed is True

    for writer_path in ("commit_consolidation", "deterministic_worker"):
        decision = guard.validate(
            board_id="board-1",
            writer_path=writer_path,
            kg_health_state="healthy",
            nodes=[_node("decision-1", "Decision", "spec:spec-1:decision:0")],
            edges=[
                _edge("e-1", "belongs_to", "decision-1", "kg:entity_spec_1"),
                _edge("e-2", "derives_from", "decision-1", "kg:req_1"),
            ],
            existing_node_refs=[
                KGNodeRef(ref_id="entity_spec_1", node_type="Entity"),
                KGNodeRef(ref_id="req_1", node_type="Requirement"),
            ],
        )

        assert decision.passed is True

    bug_learning = guard.validate(
        board_id="board-1",
        writer_path="commit_consolidation",
        kg_health_state="healthy",
        nodes=[_node("learning-1", "Learning", "card:bug:bug-1:learning:0")],
        edges=[_edge("e-1", "validates", "learning-1", "kg:bug_1")],
        existing_node_refs=[
            # R7: a bug-derived canonical Learning only reaches completeness
            # through a CANONICAL Bug, so the endpoint is stamped canonical
            # (fixture correction, not a relaxation of the invariant).
            KGNodeRef(ref_id="bug_1", node_type="Bug", graph_layer="canonical"),
        ],
    )

    assert bug_learning.passed is True


def test_technical_root_allowlist_is_exact():
    guard = KGNodeConnectivityGuard()

    allowed = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("redis", "Entity", "tech_entities.yml")],
        edges=[],
    )
    denied = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("card-root", "Entity", "card:card-1")],
        edges=[],
    )

    assert allowed.passed is True
    assert allowed.outcome == KGConnectivityOutcome.ALLOWLISTED
    assert allowed.allowlisted_roots == ("redis",)
    assert denied.passed is False


def test_board_root_allowlist_accepts_board_source_ref_prefix_only():
    guard = KGNodeConnectivityGuard()

    allowed = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("board-root", "Entity", "board:board-1")],
        edges=[],
    )
    denied = guard.validate(
        board_id="board-1",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=[_node("almost-board-root", "Entity", "card:board-1")],
        edges=[],
    )

    assert allowed.passed is True
    assert allowed.outcome == KGConnectivityOutcome.ALLOWLISTED
    assert allowed.allowlisted_roots == ("board-root",)
    assert denied.passed is False


def test_writer_path_classifier_is_deterministic():
    assert classify_writer_path("deterministic_worker") == WriterClass.DETERMINISTIC
    assert classify_writer_path("commit_consolidation") == WriterClass.COGNITIVE
    assert classify_writer_path("custom") == WriterClass.UNKNOWN
