from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.orphan_integrity import (
    InMemoryOrphanAuditSink,
    SAFE_ORPHAN_SAMPLE_FIELDS,
    ZERO_ORPHAN_VALIDATION_PENDING_BACKFILL,
    build_orphan_integrity_projection,
    InMemoryOrphanMetricSink,
    OrphanNodeScanner,
    OrphanScanQueryError,
    SAFE_ORPHAN_AUDIT_FIELDS,
    SAFE_ORPHAN_METRIC_LABELS,
    SOURCE_REF_MISSING,
    SOURCE_REF_RESOLVED,
    SOURCE_REF_UNRESOLVED,
    schema_node_types_for_orphan_scanner,
    schema_relationship_pairs_for_orphan_scanner,
)
from okto_pulse.core.kg.primitives import _apply_graph_node_create
from kg_schema_testing import (
    MULTI_REL_TYPES,
    NODE_TYPES,
    REL_TYPES,
    open_materialized_board_connection as open_board_connection,
    resolve_relationship_endpoint_pair,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    configure_test_kg_registry,
)
from sqlalchemy_test_models import Board, Card, Spec, SpecStatus


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(cypher_executor=RealBoardCypherExecutorForTests())


class _BatchScannerConnection:
    def __init__(
        self,
        node_rows: tuple[tuple[object, ...], ...],
        *,
        connected_degrees: dict[str, tuple[int, int]] | None = None,
        fail_phase: str | None = None,
    ) -> None:
        self.node_rows = node_rows
        self.connected_degrees = connected_degrees or {}
        self.fail_phase = fail_phase
        self.queries: list[str] = []

    def execute(self, query: str, _params=None):
        self.queries.append(query)
        if "n.source_artifact_ref" in query:
            if self.fail_phase == "enumeration":
                raise RuntimeError("enumeration unavailable")
            return SimpleNamespace(rows=self.node_rows)
        if "OPTIONAL MATCH (n)-[r_out]->()" in query:
            if self.fail_phase == "connectivity":
                raise RuntimeError("connectivity unavailable")
            return SimpleNamespace(
                rows=tuple(
                    (node_id, degrees[0], degrees[1])
                    for node_id, degrees in self.connected_degrees.items()
                )
            )
        raise AssertionError(f"unexpected scanner query: {query}")


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
    _apply_graph_node_create(
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


def test_boost_audit_relates_to_endpoints_are_schema_valid() -> None:
    """The handler's Decision anchor must be materializable for every card root."""

    assert resolve_relationship_endpoint_pair(
        "relates_to",
        from_type="Decision",
        to_type="Entity",
    ) == ("Decision", "Entity")
    assert resolve_relationship_endpoint_pair(
        "relates_to",
        from_type="Decision",
        to_type="Bug",
    ) == ("Decision", "Bug")


def test_boost_audit_edges_materialize_and_clear_zero_degree_diagnostics() -> None:
    """Both normal and bug-card audit Decisions receive real graph degree."""

    board_id = f"boost-audit-schema-{uuid.uuid4()}"
    decision_entity_id = f"decision-entity-{uuid.uuid4().hex[:12]}"
    decision_bug_id = f"decision-bug-{uuid.uuid4().hex[:12]}"
    entity_id = f"entity-{uuid.uuid4().hex[:12]}"
    bug_id = f"bug-{uuid.uuid4().hex[:12]}"
    with open_board_connection(board_id) as (_db, kconn):
        orchestrator = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"boost-schema-{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(
            kconn,
            orchestrator,
            "Decision",
            decision_entity_id,
            "card:card-normal:boost_audit:normal",
        )
        _seed_node(kconn, orchestrator, "Entity", entity_id, "card:card-normal")
        _seed_node(
            kconn,
            orchestrator,
            "Decision",
            decision_bug_id,
            "card:card-bug:boost_audit:bug",
        )
        _seed_node(kconn, orchestrator, "Bug", bug_id, "card:card-bug")
        orchestrator.create_edge(
            "relates_to",
            decision_entity_id,
            entity_id,
            attrs={"layer": "deterministic", "rule_id": "boost_audit"},
            from_type="Decision",
            to_type="Entity",
        )
        orchestrator.create_edge(
            "relates_to",
            decision_bug_id,
            bug_id,
            attrs={"layer": "deterministic", "rule_id": "boost_audit"},
            from_type="Decision",
            to_type="Bug",
        )

        report = OrphanNodeScanner().scan(
            board_id=board_id,
            node_type="Decision",
            connection=kconn,
        )

    assert report.orphan_count == 0
    assert build_orphan_integrity_projection(
        report
    ).classification_delta == "none"


def test_scanner_batches_connectivity_with_constant_query_count_for_many_nodes() -> None:
    node_count = 250
    connected_count = 100
    rows = tuple(
        (
            f"learning-{index}",
            f"card:bug:bulk-{index}:learning:0",
            f"kgses_bulk_{index}",
            "agent:cognitive",
        )
        for index in range(node_count)
    )
    connection = _BatchScannerConnection(
        rows,
        connected_degrees={
            f"learning-{index}": (1, 0) for index in range(connected_count)
        },
    )

    report = OrphanNodeScanner().scan(
        board_id="board-batch-query-bound",
        node_type="Learning",
        limit=3,
        connection=connection,
    )

    assert len(connection.queries) == 2
    assert sum("n.source_artifact_ref" in query for query in connection.queries) == 1
    assert sum("OPTIONAL MATCH" in query for query in connection.queries) == 1
    assert report.orphan_count == node_count - connected_count
    assert report.orphan_count_by_type == {"Learning": node_count - connected_count}
    assert len(report.samples) == 3


@pytest.mark.asyncio
async def test_scanner_resolves_sources_from_board_scoped_relational_truth(
    db_factory,
) -> None:
    """Live, absent, malformed and cross-board refs keep a strict tri-state."""

    token = uuid.uuid4().hex
    board_id = f"orphan-source-board-{token}"
    other_board_id = f"orphan-source-other-{token}"
    live_spec_id = f"orphan-source-live-{token}"
    live_card_id = f"orphan-source-card-{token}"
    cross_board_spec_id = f"orphan-source-cross-{token}"
    actor_id = f"orphan-source-actor-{token}"
    async with db_factory() as db:
        db.add_all(
            [
                Board(id=board_id, name="Source board", owner_id=actor_id),
                Board(
                    id=other_board_id,
                    name="Other source board",
                    owner_id=actor_id,
                ),
                Spec(
                    id=live_spec_id,
                    board_id=board_id,
                    title="Live source",
                    status=SpecStatus.DRAFT,
                    created_by=actor_id,
                ),
                Spec(
                    id=cross_board_spec_id,
                    board_id=other_board_id,
                    title="Cross-board source",
                    status=SpecStatus.DRAFT,
                    created_by=actor_id,
                ),
                Card(
                    id=live_card_id,
                    board_id=board_id,
                    title="Live boost source",
                    created_by=actor_id,
                ),
            ]
        )
        await db.commit()

    rows = (
        ("learning-live", f"spec:{live_spec_id}", None, "agent:cognitive"),
        (
            "learning-absent",
            f"spec:absent-{token}",
            None,
            "agent:cognitive",
        ),
        ("learning-malformed", "spec::broken", None, "agent:cognitive"),
        (
            "learning-cross-board",
            f"spec:{cross_board_spec_id}",
            None,
            "agent:cognitive",
        ),
        ("learning-missing-ref", None, None, "agent:cognitive"),
    )
    report = OrphanNodeScanner().scan(
        board_id=board_id,
        node_type="Learning",
        limit=10,
        connection=_BatchScannerConnection(rows),
    )

    status_by_node = {
        sample.node_id: sample.source_resolution_status
        for sample in report.samples
    }
    assert status_by_node == {
        "learning-live": SOURCE_REF_RESOLVED,
        "learning-absent": SOURCE_REF_UNRESOLVED,
        "learning-malformed": SOURCE_REF_UNRESOLVED,
        "learning-cross-board": SOURCE_REF_UNRESOLVED,
        "learning-missing-ref": SOURCE_REF_MISSING,
    }
    assert report.unresolved_reasons == {
        SOURCE_REF_RESOLVED: 1,
        SOURCE_REF_UNRESOLVED: 3,
        SOURCE_REF_MISSING: 1,
    }

    # Relational resolution is diagnostic only: even a live source cannot
    # excuse a zero-degree graph node or hide it from the Health projection.
    projection = build_orphan_integrity_projection(report).to_safe_dict()
    assert report.orphan_count == 5
    assert projection["integrity_warning"] is True
    assert projection["classification_delta"] == "at_risk"
    assert projection["orphan_count"] == 5
    assert projection["unresolved_reasons"] == report.unresolved_reasons

    # A boost audit can have a live relational source and still be a genuine
    # graph orphan: source resolution enriches the diagnostic, never degree.
    boost_report = OrphanNodeScanner().scan(
        board_id=board_id,
        node_type="Decision",
        connection=_BatchScannerConnection(
            (
                (
                    "decision-boost-orphan",
                    f"card:{live_card_id}:boost_audit:deadbeef",
                    None,
                    "system:card_boost_recompute_handler",
                ),
            )
        ),
    )
    boost_projection = build_orphan_integrity_projection(boost_report).to_safe_dict()
    assert boost_report.orphan_count == 1
    assert boost_report.samples[0].source_resolution_status == SOURCE_REF_RESOLVED
    assert boost_projection["classification_delta"] == "at_risk"
    assert boost_projection["integrity_warning"] is True


@pytest.mark.parametrize("fail_phase", ["enumeration", "connectivity"])
def test_scanner_query_failures_are_fail_closed(fail_phase: str) -> None:
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()
    connection = _BatchScannerConnection(
        (("learning-1", "card:bug:bug-1:learning:0", None, "agent:cognitive"),),
        fail_phase=fail_phase,
    )

    with pytest.raises(OrphanScanQueryError) as exc_info:
        OrphanNodeScanner(
            metric_sink=metric_sink,
            audit_sink=audit_sink,
        ).scan(
            board_id="board-fail-closed",
            node_type="Learning",
            connection=connection,
        )

    assert exc_info.value.phase == fail_phase
    assert exc_info.value.node_type == "Learning"
    assert len(connection.queries) == (1 if fail_phase == "enumeration" else 2)
    assert metric_sink.events == []
    assert audit_sink.records == []


def test_scanner_detects_only_zero_degree_learning_with_safe_samples() -> None:
    board_id = f"orphan-scan-{uuid.uuid4()}"
    learning_id = f"learning_orphan_{uuid.uuid4().hex[:12]}"
    decision_id = f"decision_connected_{uuid.uuid4().hex[:12]}"
    entity_id = f"entity_connected_{uuid.uuid4().hex[:12]}"
    source_ref = f"card:bug:{uuid.uuid4()}:learning:0"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

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


def test_scanner_batch_connectivity_counts_both_edge_directions() -> None:
    board_id = f"orphan-directions-{uuid.uuid4()}"
    decision_id = f"decision_outgoing_{uuid.uuid4().hex[:12]}"
    entity_id = f"entity_incoming_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id=f"seed_{uuid.uuid4().hex[:8]}",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Decision", decision_id, "spec:directions:decision")
        _seed_node(kconn, orch, "Entity", entity_id, "spec:directions")
        orch.create_edge(
            "belongs_to",
            decision_id,
            entity_id,
            attrs={"confidence": 1.0},
            from_type="Decision",
            to_type="Entity",
        )

        outgoing_report = OrphanNodeScanner().scan(
            board_id=board_id,
            node_type="Decision",
            connection=kconn,
        )
        incoming_report = OrphanNodeScanner().scan(
            board_id=board_id,
            node_type="Entity",
            connection=kconn,
        )

    assert outgoing_report.orphan_count == 0
    assert incoming_report.orphan_count == 0


def test_scanner_does_not_report_allowlisted_board_root_entity() -> None:
    board_id = f"orphan-root-{uuid.uuid4()}"
    board_root_id = f"entity_board_root_{uuid.uuid4().hex[:12]}"

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

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
            graph_scope=kconn,

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
        if result.rows:
            return int(result.rows[0][0])
    return 0


def test_scanner_audit_uses_safe_fields_only() -> None:
    board_id = f"orphan-scan-audit-{uuid.uuid4()}"
    learning_id = f"learning_audit_{uuid.uuid4().hex[:12]}"
    metric_sink = InMemoryOrphanMetricSink()
    audit_sink = InMemoryOrphanAuditSink()

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,

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
