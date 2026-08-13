"""Closed event and deterministic KG contracts for Code Traceability."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.events.types import (
    EVENT_TYPES,
    CodeEvidenceCreated,
    CodeEvidenceDispositionChanged,
    CodeEvidenceLinked,
    CodeEvidenceRevoked,
    CodeEvidenceSuperseded,
    CodeEvidenceUnlinked,
    CodeInvestigationReceiptRevoked,
    CodeInvestigationReceiptSubmitted,
    CodeInvestigationRequested,
    CodeTraceabilityWaiverCleared,
    CodeTraceabilityWaiverCreated,
    ImplementationOverlapAcknowledged,
    ImplementationTargetCreated,
    ImplementationTargetExecutionReceiptSubmitted,
    ImplementationTargetResolutionSubmitted,
    ImplementationTargetRevoked,
    ImplementationTargetUpdated,
    resolve_event_class,
)
from okto_pulse.core.kg.rebuild_deterministic import (
    default_source_materialiser,
)
from okto_pulse.core.kg.schema_contract import (
    CODE_TRACEABILITY_COLUMNS,
    CODE_TRACEABILITY_ENTITY_SUBTYPES,
    NODE_TYPES,
    SCHEMA_VERSION,
    STABLE_NODE_PROPERTIES,
    relationship_endpoint_pairs,
)


_SHA_A = "a" * 64
_SHA_B = "b" * 64

_TRACEABILITY_EVENT_CLASSES = (
    CodeInvestigationRequested,
    CodeInvestigationReceiptSubmitted,
    CodeInvestigationReceiptRevoked,
    CodeEvidenceCreated,
    CodeEvidenceSuperseded,
    CodeEvidenceRevoked,
    CodeEvidenceLinked,
    CodeEvidenceUnlinked,
    CodeEvidenceDispositionChanged,
    ImplementationTargetCreated,
    ImplementationTargetUpdated,
    ImplementationTargetRevoked,
    ImplementationTargetResolutionSubmitted,
    ImplementationTargetExecutionReceiptSubmitted,
    ImplementationOverlapAcknowledged,
    CodeTraceabilityWaiverCreated,
    CodeTraceabilityWaiverCleared,
)

_TRACEABILITY_EVENT_TYPES = {
    "code_investigation.requested",
    "code_investigation.receipt_submitted",
    "code_investigation.receipt_revoked",
    "code_evidence.created",
    "code_evidence.superseded",
    "code_evidence.revoked",
    "code_evidence.linked",
    "code_evidence.unlinked",
    "code_evidence.disposition_changed",
    "implementation_target.created",
    "implementation_target.updated",
    "implementation_target.revoked",
    "implementation_target.resolution_submitted",
    "implementation_target.execution_receipt_submitted",
    "implementation_overlap.acknowledged",
    "code_traceability.waiver_created",
    "code_traceability.waiver_cleared",
}


def _receipt() -> dict[str, object]:
    return {
        "id": "receipt-1",
        "board_id": "board-1",
        "status": "accepted",
        "investigation_source_ref": "source:opaque-1",
        "attestor_actor_id": "agent-1",
        "declared_revision": "revision-1",
        "workspace_state_id": "workspace-state-1",
        "trust_level": "single_attestation",
        "outcome": "accessible",
        "generation": 1,
        "payload_sha256": _SHA_A,
        "content_hash": _SHA_A,
    }


def _evidence() -> dict[str, object]:
    return {
        "id": "evidence-1",
        "board_id": "board-1",
        "lifecycle_status": "active",
        "investigation_receipt_id": "receipt-1",
        "investigation_source_ref": "source:opaque-1",
        "declared_revision": "revision-1",
        "workspace_state_id": "workspace-state-1",
        "relative_path": "src/orders/service.py",
        "qualified_symbol": "orders.service.create_order",
        "symbol_kind": "function",
        "selector_kind": "symbol",
        "snapshot_line_start": 10,
        "snapshot_line_end": 24,
        "declared_source_content_sha256": _SHA_A,
        "evidence_type": "behavior",
        "claim": "Order creation is delegated to the domain service.",
        "supersedes_evidence_id": "evidence-0",
        "content_hash": _SHA_B,
        "spec_links": [
            {
                "id": "link-1",
                "spec_id": "spec-1",
                "entity_type": "functional_requirement",
                "entity_id": "fr-1",
                "relation_type": "supports",
            }
        ],
    }


def _target() -> dict[str, object]:
    return {
        "id": "target-1",
        "board_id": "board-1",
        "card_id": "card-1",
        "card_node_type": "Entity",
        "investigation_source_ref": "source:opaque-1",
        "selector_kind": "symbol",
        "relative_path_hint": "src/orders/service.py",
        "qualified_symbol": "orders.service.create_order",
        "symbol_kind": "function",
        "role": "modify",
        "intent": "Apply the accepted order-validation rule.",
        "lifecycle_status": "active",
        "revision": 2,
        "baseline_evidence_id": "evidence-1",
        "resolution_state": "resolved",
        "investigation_receipt_id": "receipt-1",
        "declared_revision": "revision-1",
        "workspace_state_id": "workspace-state-1",
        "selector_fingerprint": _SHA_A,
        "resolved_relative_path": "src/orders/service.py",
        "resolved_qualified_symbol": "orders.service.create_order",
        "resolved_symbol_kind": "function",
        "resolved_line_start": 10,
        "resolved_line_end": 24,
        "payload_sha256": _SHA_B,
        "content_hash": _SHA_B,
        "evidence_links": [
            {
                "id": "target-evidence-1",
                "evidence_id": "evidence-1",
                "relation_type": "derived_from",
            }
        ],
        "overlap_target_ids": ["target-2"],
    }


def test_event_registry_adds_exactly_the_seventeen_closed_event_names():
    actual = {event.event_type for event in _TRACEABILITY_EVENT_CLASSES}
    assert actual == _TRACEABILITY_EVENT_TYPES
    assert len(EVENT_TYPES) == 62
    assert _TRACEABILITY_EVENT_TYPES.issubset(EVENT_TYPES)
    for event_class in _TRACEABILITY_EVENT_CLASSES:
        assert resolve_event_class(event_class.event_type) is event_class


def test_event_schemas_have_no_operational_code_or_secret_fields():
    common = {"event_id", "board_id", "actor_id", "actor_type", "occurred_at"}
    prohibited = (
        "path",
        "symbol",
        "snippet",
        "excerpt",
        "challenge",
        "secret",
        "credential",
        "locator",
        "repository",
        "clone_url",
    )
    for event_class in _TRACEABILITY_EVENT_CLASSES:
        payload_fields = set(event_class.model_fields).difference(common)
        assert not {
            field
            for field in payload_fields
            if any(token in field.lower() for token in prohibited)
        }


def test_event_payload_is_bounded_frozen_and_rejects_unknown_fields():
    event = CodeInvestigationReceiptRevoked(
        board_id="board-1",
        actor_id="agent-1",
        actor_type="agent",
        investigation_receipt_id="receipt-1",
        revocation_id="revocation-1",
        reason_code="operator_revoked",
        head_state="revoked",
    )
    assert set(event.payload_for_storage()) == {
        "investigation_receipt_id",
        "revocation_id",
        "reason_code",
        "head_state",
    }
    with pytest.raises(ValidationError):
        CodeInvestigationReceiptRevoked(
            board_id="board-1",
            investigation_receipt_id="receipt-1",
            revocation_id="revocation-1",
            reason_code="operator_revoked",
            head_state="revoked",
            challenge="must-not-enter-the-event",  # type: ignore[call-arg]
        )
    with pytest.raises(ValidationError):
        event.reason_code = "changed"


def test_kg_schema_is_additive_semantic_subtyping_only():
    assert SCHEMA_VERSION == "0.5.0"
    assert len(NODE_TYPES) == 11
    assert not set(CODE_TRACEABILITY_ENTITY_SUBTYPES).intersection(NODE_TYPES)
    assert CODE_TRACEABILITY_ENTITY_SUBTYPES == (
        "code_investigation_receipt",
        "code_evidence",
        "implementation_target",
    )
    expected_columns = {
        "investigation_receipt_id",
        "source_ref",
        "attestor_actor_id",
        "declared_revision",
        "workspace_state_id",
        "code_path",
        "symbol_qualified_name",
        "symbol_kind",
        "selector_kind",
        "selector_fingerprint",
        "resolution_state",
    }
    assert {name for name, column_type in CODE_TRACEABILITY_COLUMNS} == (
        expected_columns
    )
    assert all(column_type == "STRING" for _, column_type in CODE_TRACEABILITY_COLUMNS)
    assert expected_columns.issubset(STABLE_NODE_PROPERTIES)


def test_kg_relationship_catalog_has_only_closed_physical_endpoint_pairs():
    assert set(relationship_endpoint_pairs("supports")) == {
        ("Entity", "Requirement"),
        ("Entity", "Constraint"),
        ("Entity", "Criterion"),
        ("Entity", "APIContract"),
        ("Entity", "Decision"),
        ("Entity", "TestScenario"),
        ("Entity", "Entity"),
    }
    assert ("Entity", "Entity") in relationship_endpoint_pairs("derives_from")
    assert ("Entity", "Entity") in relationship_endpoint_pairs("overlaps")
    assert ("Entity", "Entity") in relationship_endpoint_pairs("belongs_to")
    assert ("Entity", "Bug") in relationship_endpoint_pairs("belongs_to")
    assert ("Entity", "Entity") in relationship_endpoint_pairs("supersedes")


def test_receipt_projection_is_deterministic_connected_and_never_dynamic():
    worker = DeterministicWorker()
    first = worker.process_code_investigation_receipt(_receipt())
    second = worker.process_code_investigation_receipt(_receipt())

    assert first == second
    assert {candidate.node_type for candidate in first.nodes} == {"Entity"}
    trace_nodes = [
        candidate
        for candidate in first.nodes
        if candidate.kind_of == "code_investigation_receipt"
    ]
    assert len(trace_nodes) == 1
    node = trace_nodes[0]
    assert node.node_type == "Entity"
    assert node.kind_of == "code_investigation_receipt"
    assert node.source_artifact_ref == "code_investigation_receipt:receipt-1"
    assert node.source_ref == "source:opaque-1"
    assert any(node.candidate_id in {edge.from_candidate_id, edge.to_candidate_id} for edge in first.edges)


def test_conflicted_accepted_receipt_projects_as_typed_working_knowledge():
    worker = DeterministicWorker()
    explicit = worker.process_code_investigation_receipt(
        {**_receipt(), "status": "conflicted", "trust_level": "conflicted"}
    )
    acceptance_fallback = worker.process_code_investigation_receipt(
        {
            key: value
            for key, value in {
                **_receipt(),
                "status": None,
                "acceptance_status": "conflicted",
                "trust_level": "conflicted",
            }.items()
            if value is not None
        }
    )

    assert explicit == acceptance_fallback
    assert '"status":"conflicted"' in explicit.raw_content
    receipt_node = next(
        node
        for node in explicit.nodes
        if node.kind_of == "code_investigation_receipt"
    )
    assert receipt_node.graph_layer == "working"


def test_evidence_projection_uses_metadata_only_and_closed_relationships():
    worker = DeterministicWorker()
    result = worker.process_code_evidence(_evidence())

    assert {node.node_type for node in result.nodes} == {"Entity"}
    node = result.nodes[0]
    assert node.kind_of == "code_evidence"
    assert node.source_artifact_ref == "code_evidence:evidence-1"
    assert node.source_ref == "source:opaque-1"
    assert node.code_path == "src/orders/service.py"
    assert node.symbol_qualified_name == "orders.service.create_order"
    assert {edge.edge_type for edge in result.edges} == {
        "belongs_to",
        "supports",
        "supersedes",
    }
    assert "excerpt" not in result.raw_content
    with pytest.raises(
        ValueError,
        match="code_traceability_projection_fields_invalid",
    ):
        worker.process_code_evidence({**_evidence(), "excerpt": "secret source"})


def test_target_projects_current_resolution_as_properties_not_as_a_node():
    worker = DeterministicWorker()
    result = worker.process_implementation_target(_target())

    assert len(result.nodes) == 1
    node = result.nodes[0]
    assert node.node_type == "Entity"
    assert node.kind_of == "implementation_target"
    assert node.source_artifact_ref == "implementation_target:target-1"
    assert node.source_ref == "source:opaque-1"
    assert node.resolution_state == "resolved"
    assert node.selector_fingerprint == _SHA_A
    assert {edge.edge_type for edge in result.edges} == {
        "belongs_to",
        "derives_from",
        "overlaps",
    }
    assert all(candidate.kind_of != "implementation_target_resolution" for candidate in result.nodes)


def test_projection_rejects_malformed_digest_before_graph_staging():
    worker = DeterministicWorker()
    with pytest.raises(ValueError, match="selector_fingerprint_invalid"):
        worker.process_implementation_target(
            {**_target(), "selector_fingerprint": "a" * 63}
        )
    with pytest.raises(ValueError, match="payload_sha256_invalid"):
        worker.process_code_investigation_receipt(
            {**_receipt(), "payload_sha256": "not-a-sha"}
        )
    with pytest.raises(ValueError, match="code_traceability_outcome_invalid"):
        worker.process_code_investigation_receipt(
            {**_receipt(), "outcome": "available"}
        )


def test_rebuild_identity_keeps_row_source_ref_separate_from_semantic_mapping():
    nodes, edges, decisions = default_source_materialiser(
        (
            {
                "artifact_type": "code_evidence",
                "id": "evidence-1",
                "source_ref": "code_evidence:evidence-1",
                "content_hash": _SHA_A,
            },
        )
    )
    assert nodes == (
        {
            "type": "Entity",
            "id": "evidence-1",
            "source_ref": "code_evidence:evidence-1",
            "content_hash": _SHA_A,
            "kind_of": "code_evidence",
        },
    )
    assert edges == ()
    assert decisions == ()
