from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.connectivity_guard import KGConnectivityRuleRegistry
from okto_pulse.core.kg.orphan_integrity import (
    get_orphan_audit_fields,
    get_orphan_metric_labels,
    InMemoryOrphanAuditSink,
    SAFE_ORPHAN_SAMPLE_FIELDS,
    ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL,
    build_orphan_integrity_projection,
    InMemoryOrphanMetricSink,
    OrphanBackfillReconciler,
    OrphanNodeScanner,
    SAFE_ORPHAN_AUDIT_FIELDS,
    SAFE_ORPHAN_METRIC_LABELS,
    schema_node_types_for_orphan_scanner,
    schema_relationship_pairs_for_orphan_scanner,
)
from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
from okto_pulse.core.kg.schema import (
    MULTI_REL_TYPES,
    NODE_TYPES,
    REL_TYPES,
    open_board_connection,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(cypher_executor=RealBoardCypherExecutorForTests())


def _seed_node(
    kconn,
    orch: TransactionOrchestrator,
    node_type: str,
    node_id: str,
    source_ref: str,
    *,
    created_by_agent: str = "test",
    title: str | None = None,
    content: str | None = None,
) -> None:
    _apply_kuzu_node_create_with_timestamp(
        orch,
        node_type,
        node_id,
        {
            "title": title or f"Raw title must not leak {node_type}",
            "content": content or "Raw content must not leak",
            "context": "",
            "justification": "",
            "source_artifact_ref": source_ref,
            "created_at": "2026-06-08T00:00:00+00:00",
            "created_by_agent": created_by_agent,
            "source_confidence": 1.0,
            "relevance_score": 0.5,
            "query_hits": 0,
            "last_queried_at": None,
            "last_recomputed_at": None,
            "priority_boost": 0.0,
            "superseded_by": None,
            "superseded_at": None,
            "revocation_reason": "",
            "human_curated": False,
            "embedding": [0.0] * 384,
        },
    )


def test_orphan_scanner_uses_schema_node_and_relationship_catalogs() -> None:
    assert schema_node_types_for_orphan_scanner() == NODE_TYPES

    expected = list(REL_TYPES)
    for rel_name, endpoint_pairs in MULTI_REL_TYPES:
        expected.extend((rel_name, from_type, to_type) for from_type, to_type in endpoint_pairs)

    assert schema_relationship_pairs_for_orphan_scanner() == tuple(expected)


def test_scanner_detects_only_zero_degree_learning_with_safe_samples() -> None:
    board_id = f"orphan-scan-{uuid.uuid4()}"
    learning_id = f"learning_orphan_{uuid.uuid4().hex[:12]}"
    decision_id = f"decision_connected_{uuid.uuid4().hex[:12]}"
    entity_id = f"entity_connected_{uuid.uuid4().hex[:12]}"
    source_ref = f"card:bug:{uuid.uuid4()}:learning:0"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Learning", learning_id, source_ref)
        _seed_node(kconn, orch, "Decision", decision_id, "spec:abc:decision:0")
        _seed_node(kconn, orch, "Entity", entity_id, "spec:abc")
        orch.create_edge(
            "belongs_to",
            decision_id,
            entity_id,
            attrs={"confidence": 1.0},
            from_type="Decision",
            to_type="Entity",
        )

        metric_sink = InMemoryOrphanMetricSink()
        report = OrphanNodeScanner(metric_sink=metric_sink).scan(
            board_id=board_id,
            generation_id="gen-1",
            limit=5,
            connection=kconn,
        )

    assert report.orphan_count == 1
    assert report.orphan_count_by_type == {"Learning": 1}
    assert report.orphan_count_by_writer_path == {"unknown": 1}
    assert report.allowlisted_root_count == 0
    assert len(report.samples) == 1

    sample = report.samples[0].to_safe_dict()
    assert set(sample) == set(SAFE_ORPHAN_SAMPLE_FIELDS)
    assert sample["node_id"] == learning_id
    assert sample["node_type"] == "Learning"
    assert sample["source_artifact_ref"] == source_ref
    assert sample["reason"] == "zero_graph_degree"
    assert "Raw title" not in str(report.to_safe_dict())
    assert "Raw content" not in str(report.to_safe_dict())

    assert len(metric_sink.events) == 1
    event = metric_sink.events[0]
    assert event.metric_name == "kg_orphan_node_detected_total"
    assert event.labels() == {
        "board_id": board_id,
        "node_type": "Learning",
        "writer_path": "unknown",
        "outcome": "detected",
        "reason": "zero_graph_degree",
        "source_resolution_status": "unresolved_source_ref",
        "generation_id": "gen-1",
    }

    projection = build_orphan_integrity_projection(report).to_safe_dict()
    assert projection["classification_delta"] == "at_risk"
    assert projection["integrity_warning"] is True
    assert projection["orphan_count"] == 1
    assert (
        projection["zero_orphan_validation"]
        == ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL
    )
    assert set(projection["samples"][0]) == set(SAFE_ORPHAN_SAMPLE_FIELDS)


def test_scanner_does_not_report_allowlisted_board_root_entity() -> None:
    board_id = f"orphan-root-{uuid.uuid4()}"
    board_root_id = f"entity_board_root_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"bootstrap_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Entity",
            board_root_id,
            f"board:{board_id}",
            created_by_agent="system:deterministic_worker",
        )

        report = OrphanNodeScanner().scan(
            board_id=board_id,
            generation_id="gen-root",
            limit=5,
            connection=kconn,
        )

    assert report.orphan_count == 0
    assert report.allowlisted_root_count == 1
    assert report.samples == ()


def test_scanner_allowlists_final_report_root_from_kg_session_id() -> None:
    board_id = f"orphan-final-report-{uuid.uuid4()}"
    assumption_id = f"assumption_final_report_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"kgses_{uuid.uuid4().hex[:16]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Assumption",
            assumption_id,
            "final_report:saas-refactor-rkg-closeout-2026-06-25",
            created_by_agent=str(uuid.uuid4()),
        )

        report = OrphanNodeScanner().scan(
            board_id=board_id,
            generation_id="gen-final-report",
            limit=5,
            connection=kconn,
        )

    assert report.orphan_count == 0
    assert report.allowlisted_root_count == 1
    assert report.samples == ()


def _edge_count(
    board_id: str,
    *,
    edge_type: str,
    from_type: str,
    to_type: str,
    from_id: str,
    to_id: str,
) -> int:
    with open_board_connection(board_id) as (_db, kconn):
        result = kconn.execute(
            f"MATCH (a:{from_type})-[r:{edge_type}]->(b:{to_type}) "
            "WHERE a.id = $from_id AND b.id = $to_id RETURN count(r)",
            {"from_id": from_id, "to_id": to_id},
        )
        try:
            if result.has_next():
                return int(result.get_next()[0])
        finally:
            result.close()
    return 0


def _node_exists(board_id: str, node_type: str, node_id: str) -> bool:
    with open_board_connection(board_id) as (_db, kconn):
        result = kconn.execute(
            f"MATCH (n:{node_type}) WHERE n.id = $node_id RETURN count(n)",
            {"node_id": node_id},
        )
        try:
            if result.has_next():
                return int(result.get_next()[0]) == 1
        finally:
            result.close()
    return False


def _edge_count_with_connection(
    kconn: object,
    *,
    edge_type: str,
    from_type: str,
    to_type: str,
    from_id: str,
    to_id: str,
) -> int:
    result = kconn.execute(
        f"MATCH (a:{from_type})-[r:{edge_type}]->(b:{to_type}) "
        "WHERE a.id = $from_id AND b.id = $to_id RETURN count(r)",
        {"from_id": from_id, "to_id": to_id},
    )
    try:
        if result.has_next():
            return int(result.get_next()[0])
    finally:
        result.close()
    return 0


def test_backfill_creates_one_provenance_edge_and_rerun_noop() -> None:
    board_id = f"orphan-backfill-{uuid.uuid4()}"
    spec_entity_id = f"entity_spec_{uuid.uuid4().hex[:12]}"
    requirement_id = f"requirement_orphan_{uuid.uuid4().hex[:12]}"
    source_root = f"spec:{uuid.uuid4()}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", spec_entity_id, source_root)
        _seed_node(kconn, orch, "Requirement", requirement_id, f"{source_root}:fr:0")

        dry_run = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[requirement_id],
            generation_id="gen-backfill",
            dry_run=True,
            connection=kconn,
        )
        dry_run_edge_count = _edge_count_with_connection(
            kconn,
            edge_type="belongs_to",
            from_type="Requirement",
            to_type="Entity",
            from_id=requirement_id,
            to_id=spec_entity_id,
        )

        result = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[requirement_id],
            generation_id="gen-backfill",
            connection=kconn,
        )
        rerun = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[requirement_id],
            generation_id="gen-backfill",
            connection=kconn,
        )

    assert dry_run.detected == 1
    assert dry_run.connected == 1
    assert dry_run.samples[0].edge_type == "belongs_to"
    assert dry_run.samples[0].target_node_type == "Entity"
    assert dry_run_edge_count == 0
    assert result.detected == 1
    assert result.connected == 1
    assert result.samples[0].edge_type == "belongs_to"
    assert result.samples[0].target_node_type == "Entity"
    assert rerun.noop == 1
    assert _edge_count(
        board_id,
        edge_type="belongs_to",
        from_type="Requirement",
        to_type="Entity",
        from_id=requirement_id,
        to_id=spec_entity_id,
    ) == 1


def test_bug_derived_learning_backfill_validates_resolved_bug() -> None:
    board_id = f"orphan-bug-learning-{uuid.uuid4()}"
    bug_id = f"bug_{uuid.uuid4().hex[:12]}"
    learning_id = f"learning_bug_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Bug", bug_id, f"bug:{bug_id}")
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            f"card:bug:{bug_id}:learning:0",
            created_by_agent="agent:cognitive",
        )

        result = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[learning_id],
            connection=kconn,
        )
        rerun = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[learning_id],
            connection=kconn,
        )

    assert result.connected == 1
    assert result.samples[0].reason == "bug_learning_validates_bug"
    assert result.samples[0].edge_type == "validates"
    assert rerun.noop == 1
    assert _edge_count(
        board_id,
        edge_type="validates",
        from_type="Learning",
        to_type="Bug",
        from_id=learning_id,
        to_id=bug_id,
    ) == 1


def test_backfill_preserves_ambiguous_orphan_without_fabricated_edge() -> None:
    board_id = f"orphan-ambiguous-{uuid.uuid4()}"
    source_root = f"spec:{uuid.uuid4()}"
    requirement_id = f"requirement_ambiguous_{uuid.uuid4().hex[:12]}"
    entity_a = f"entity_a_{uuid.uuid4().hex[:12]}"
    entity_b = f"entity_b_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", entity_a, source_root)
        _seed_node(kconn, orch, "Entity", entity_b, source_root)
        _seed_node(kconn, orch, "Requirement", requirement_id, f"{source_root}:fr:0")

        result = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[requirement_id],
            connection=kconn,
        )

    assert result.ambiguous == 1
    assert result.samples[0].reason == "ambiguous_source_ref"
    assert _node_exists(board_id, "Requirement", requirement_id)
    assert _edge_count(
        board_id,
        edge_type="belongs_to",
        from_type="Requirement",
        to_type="Entity",
        from_id=requirement_id,
        to_id=entity_a,
    ) == 0
    assert _edge_count(
        board_id,
        edge_type="belongs_to",
        from_type="Requirement",
        to_type="Entity",
        from_id=requirement_id,
        to_id=entity_b,
    ) == 0


def test_backfill_keeps_fuzzy_or_prose_only_learning_semantic_pending() -> None:
    board_id = f"orphan-fuzzy-{uuid.uuid4()}"
    learning_id = f"learning_fuzzy_{uuid.uuid4().hex[:12]}"
    bug_id = f"bug_fuzzy_{uuid.uuid4().hex[:12]}"
    fuzzy_title = "Only title/prose points at this bug"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Bug",
            bug_id,
            f"bug:{bug_id}",
            title=fuzzy_title,
            content="Structured bug node exists but is not referenced by source_artifact_ref.",
        )
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            "",
            created_by_agent="agent:cognitive",
            title=fuzzy_title,
            content=f"Prose mentions {bug_id}, but no structured source reference exists.",
        )

        result = OrphanBackfillReconciler().run(
            board_id=board_id,
            node_ids=[learning_id],
            connection=kconn,
        )

    assert result.semantic_pending == 1
    assert result.samples[0].reason == "fuzzy_or_prose_only_evidence"
    assert result.samples[0].edge_type is None
    assert result.samples[0].source_resolution_status == "missing_source_artifact_ref"
    assert _node_exists(board_id, "Learning", learning_id)
    assert _edge_count(
        board_id,
        edge_type="validates",
        from_type="Learning",
        to_type="Bug",
        from_id=learning_id,
        to_id=bug_id,
    ) == 0


def test_backfill_metrics_and_audit_use_safe_fields_only() -> None:
    board_id = f"orphan-safe-obs-{uuid.uuid4()}"
    requirement_id = f"requirement_unresolved_{uuid.uuid4().hex[:12]}"
    unsafe_source_ref = (
        r"C:\Users\jpamb\secret\payload.json:email:user@example.com:title:raw"
    )
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Requirement", requirement_id, unsafe_source_ref)

        result = OrphanBackfillReconciler(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        ).run(
            board_id=board_id,
            node_ids=[requirement_id],
            generation_id="gen-safe-obs",
            connection=kconn,
        )

    assert result.unresolved == 1
    assert len(metric_sink.events) == 1
    metric = metric_sink.events[0]
    assert metric.metric_name == "kg_orphan_backfill_total"
    assert tuple(metric.labels()) == get_orphan_metric_labels()
    assert tuple(metric.labels()) == SAFE_ORPHAN_METRIC_LABELS
    assert metric.labels() == {
        "board_id": board_id,
        "node_type": "Requirement",
        "writer_path": "unknown",
        "outcome": "unresolved",
        "reason": "unresolved_source_ref",
        "source_resolution_status": "unresolved_source_ref",
        "generation_id": "gen-safe-obs",
    }

    assert len(audit_sink.records) == 1
    record = audit_sink.records[0].to_safe_dict()
    assert tuple(record) == get_orphan_audit_fields()
    assert tuple(record) == SAFE_ORPHAN_AUDIT_FIELDS
    assert record == {
        "event_name": "kg_orphan_backfill_audit_record",
        "board_id": board_id,
        "node_id": requirement_id,
        "node_type": "Requirement",
        "writer_path": "unknown",
        "outcome": "unresolved",
        "reason": "unresolved_source_ref",
        "source_resolution_status": "unresolved_source_ref",
        "generation_id": "gen-safe-obs",
        "correlation_id": result.correlation_id,
        "sample_count": 1,
    }

    rendered_metric = str(metric.labels())
    rendered_audit = str(record)
    for unsafe_fragment in (
        "payload.json",
        "user@example.com",
        "title:raw",
        r"C:\Users",
        "Raw title",
        "Raw content",
    ):
        assert unsafe_fragment not in rendered_metric
        assert unsafe_fragment not in rendered_audit


def test_scanner_audit_uses_safe_fields_only() -> None:
    board_id = f"orphan-scan-audit-{uuid.uuid4()}"
    learning_id = f"learning_audit_{uuid.uuid4().hex[:12]}"
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            "card:bug:bug-audit:learning:0",
            created_by_agent="agent:cognitive",
        )

        report = OrphanNodeScanner(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        ).scan(
            board_id=board_id,
            generation_id="gen-scan-audit",
            limit=5,
            connection=kconn,
        )

    assert report.orphan_count == 1
    assert len(metric_sink.events) == 1
    assert tuple(metric_sink.events[0].labels()) == SAFE_ORPHAN_METRIC_LABELS

    assert len(audit_sink.records) == 1
    record = audit_sink.records[0].to_safe_dict()
    assert tuple(record) == SAFE_ORPHAN_AUDIT_FIELDS
    assert record["event_name"] == "kg_orphan_node_detected"
    assert record["node_id"] == learning_id
    assert record["outcome"] == "detected"
    assert record["sample_count"] == 1
    assert "Raw title" not in str(record)
    assert "Raw content" not in str(record)


def test_backfill_metrics_cover_connected_noop_and_unresolved_safe_labels() -> None:
    board_id = f"orphan-backfill-outcomes-{uuid.uuid4()}"
    source_root = f"spec:{uuid.uuid4()}"
    entity_id = f"entity_metric_{uuid.uuid4().hex[:12]}"
    connected_req_id = f"requirement_metric_connected_{uuid.uuid4().hex[:12]}"
    unresolved_req_id = f"requirement_metric_unresolved_{uuid.uuid4().hex[:12]}"
    unsafe_source_ref = (
        r"C:\Users\jpamb\secret\payload.json:email:user@example.com:title:raw"
    )
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", entity_id, source_root)
        _seed_node(kconn, orch, "Requirement", connected_req_id, f"{source_root}:fr:0")
        _seed_node(kconn, orch, "Requirement", unresolved_req_id, unsafe_source_ref)

        reconciler = OrphanBackfillReconciler(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        )
        first_run = reconciler.run(
            board_id=board_id,
            node_ids=[connected_req_id, unresolved_req_id],
            generation_id="gen-safe-outcomes",
            connection=kconn,
        )
        second_run = reconciler.run(
            board_id=board_id,
            node_ids=[connected_req_id],
            generation_id="gen-safe-outcomes",
            connection=kconn,
        )

    assert first_run.connected == 1
    assert first_run.unresolved == 1
    assert second_run.noop == 1
    assert {event.outcome for event in metric_sink.events} == {
        "connected",
        "noop",
        "unresolved",
    }
    assert {record.outcome for record in audit_sink.records} == {
        "connected",
        "noop",
        "unresolved",
    }
    for metric in metric_sink.events:
        assert tuple(metric.labels()) == SAFE_ORPHAN_METRIC_LABELS
    for record in audit_sink.records:
        safe_record = record.to_safe_dict()
        assert tuple(safe_record) == SAFE_ORPHAN_AUDIT_FIELDS
        assert "kg_orphan" in safe_record["event_name"]

    rendered = f"{[event.labels() for event in metric_sink.events]} {audit_sink.records}"
    for unsafe_fragment in (
        "payload.json",
        "user@example.com",
        "title:raw",
        r"C:\Users",
        "Raw title",
        "Raw content",
    ):
        assert unsafe_fragment not in rendered


def test_scan_and_backfill_audit_records_cover_outcomes_with_safe_fields_only() -> None:
    board_id = f"orphan-audit-outcome-matrix-{uuid.uuid4()}"
    source_root = f"spec:{uuid.uuid4()}"
    entity_id = f"entity_audit_{uuid.uuid4().hex[:12]}"
    connected_req_id = f"requirement_connected_{uuid.uuid4().hex[:12]}"
    noop_req_id = f"requirement_noop_{uuid.uuid4().hex[:12]}"
    unresolved_req_id = f"requirement_unresolved_{uuid.uuid4().hex[:12]}"
    ambiguous_req_id = f"requirement_ambiguous_{uuid.uuid4().hex[:12]}"
    board_root_id = f"entity_board_root_{uuid.uuid4().hex[:12]}"
    entity_ambiguous_a = f"entity_ambiguous_a_{uuid.uuid4().hex[:12]}"
    entity_ambiguous_b = f"entity_ambiguous_b_{uuid.uuid4().hex[:12]}"
    unsafe_source_ref = (
        r"C:\Users\jpamb\secret\payload.json:email:user@example.com:title:raw"
    )
    unsafe_title = "Raw title must not leak user@example.com"
    unsafe_content = "Raw content must not leak payload body or JWT eyJhbGci"
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Entity",
            board_root_id,
            f"board:{board_id}",
            created_by_agent="system:deterministic_worker",
        )
        _seed_node(kconn, orch, "Entity", entity_id, source_root)
        orch.create_edge(
            "belongs_to",
            entity_id,
            board_root_id,
            attrs={"confidence": 1.0},
            from_type="Entity",
            to_type="Entity",
        )
        _seed_node(
            kconn,
            orch,
            "Requirement",
            connected_req_id,
            f"{source_root}:fr:0",
            title=unsafe_title,
            content=unsafe_content,
        )
        _seed_node(
            kconn,
            orch,
            "Requirement",
            noop_req_id,
            f"{source_root}:fr:1",
            title=unsafe_title,
            content=unsafe_content,
        )
        orch.create_edge(
            "belongs_to",
            noop_req_id,
            entity_id,
            attrs={"confidence": 1.0},
            from_type="Requirement",
            to_type="Entity",
        )
        _seed_node(
            kconn,
            orch,
            "Requirement",
            unresolved_req_id,
            unsafe_source_ref,
            title=unsafe_title,
            content=unsafe_content,
        )
        _seed_node(kconn, orch, "Entity", entity_ambiguous_a, "spec:ambiguous")
        _seed_node(kconn, orch, "Entity", entity_ambiguous_b, "spec:ambiguous")
        for entity_candidate_id in (entity_ambiguous_a, entity_ambiguous_b):
            orch.create_edge(
                "belongs_to",
                entity_candidate_id,
                board_root_id,
                attrs={"confidence": 1.0},
                from_type="Entity",
                to_type="Entity",
            )
        _seed_node(
            kconn,
            orch,
            "Requirement",
            ambiguous_req_id,
            "spec:ambiguous:fr:0",
            title=unsafe_title,
            content=unsafe_content,
        )

        scanner = OrphanNodeScanner(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        )
        report = scanner.scan(
            board_id=board_id,
            generation_id="gen-audit-matrix",
            limit=10,
            connection=kconn,
        )
        result = OrphanBackfillReconciler(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        ).run(
            board_id=board_id,
            node_ids=[
                connected_req_id,
                noop_req_id,
                unresolved_req_id,
                ambiguous_req_id,
            ],
            generation_id="gen-audit-matrix",
            connection=kconn,
        )

    assert report.orphan_count == 3
    assert result.connected == 1
    assert result.noop == 1
    assert result.unresolved == 1
    assert result.ambiguous == 1

    outcomes = {record.outcome for record in audit_sink.records}
    assert outcomes == {"detected", "connected", "noop", "unresolved", "ambiguous"}
    for record in audit_sink.records:
        safe_record = record.to_safe_dict()
        assert tuple(safe_record) == SAFE_ORPHAN_AUDIT_FIELDS
        assert safe_record["event_name"].startswith("kg_orphan_")
        assert safe_record["board_id"] == board_id
        assert safe_record["generation_id"] == "gen-audit-matrix"

    rendered = (
        f"{[event.labels() for event in metric_sink.events]} "
        f"{[record.to_safe_dict() for record in audit_sink.records]}"
    )
    for unsafe_fragment in (
        "payload.json",
        "user@example.com",
        "title:raw",
        r"C:\Users",
        "Raw title",
        "Raw content",
        "payload body",
        "eyJhbGci",
    ):
        assert unsafe_fragment not in rendered


def test_mcp_orphan_report_returns_bounded_safe_samples_behavioral(monkeypatch) -> None:
    board_id = f"orphan-mcp-report-{uuid.uuid4()}"
    learning_id = f"learning_mcp_{uuid.uuid4().hex[:12]}"
    requirement_id = f"requirement_mcp_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            kuzu_conn=kconn,
            sqlite_session=None,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            "card:bug:bug-mcp:learning:0",
            created_by_agent="agent:cognitive",
        )
        _seed_node(
            kconn,
            orch,
            "Requirement",
            requirement_id,
            "spec:mcp:fr:0",
            created_by_agent="system:deterministic_worker",
        )

    import okto_pulse.core.mcp.server as mcp_server

    async def _fake_get_agent_ctx(_board_id: str):
        return SimpleNamespace(agent=SimpleNamespace(id="agent-mcp-test"))

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_get_agent_ctx)

    payload = json.loads(
        asyncio.run(
            mcp_server.okto_pulse_kg_orphan_report.fn(
                board_id=board_id,
                generation_id="gen-mcp",
                limit=1,
            )
        )
    )

    assert payload["board_id"] == board_id
    assert payload["generation_id"] == "gen-mcp"
    assert payload["orphan_count"] == 2
    assert payload["orphan_count_by_type"] == {"Learning": 1, "Requirement": 1}
    assert len(payload["samples"]) == 1
    assert set(payload["samples"][0]) == set(SAFE_ORPHAN_SAMPLE_FIELDS)
    assert payload["backfill_summary"]["status"] == "not_run"
    assert payload["correlation_id"].startswith("kg-orphan-scan-")
    assert "Raw title" not in str(payload)
    assert "Raw content" not in str(payload)
    assert "description" not in payload["samples"][0]
    assert "payload" not in payload["samples"][0]


def test_backfill_and_guard_rule_registry_cannot_drift() -> None:
    registry = KGConnectivityRuleRegistry()
    scanner = OrphanNodeScanner(registry=registry)
    reconciler = OrphanBackfillReconciler(registry=registry)

    assert scanner.rule_node_types() == registry.rule_node_types()
    assert reconciler.rule_node_types() == registry.rule_node_types()
    assert set(scanner.rule_node_types()) == {
        "APIContract",
        "Alternative",
        "Assumption",
        "Bug",
        "Constraint",
        "Criterion",
        "Decision",
        "Entity",
        "Learning",
        "Requirement",
        "TestScenario",
    }

    source = Path("src/okto_pulse/core/kg/orphan_integrity.py").read_text(
        encoding="utf-8"
    )
    assert "_ORPHAN_CONNECTIVITY_RULES" not in source
    assert "_build_default_rules" not in source
