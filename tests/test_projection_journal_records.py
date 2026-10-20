"""Compensation before-images survive the JSON journal exactly."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from okto_pulse.core.kg.interfaces.graph_transaction import (
    GraphNodePropertyBeforeImage,
    LearningBugAssociationReceipt,
    ProjectionActiveSetIntent,
    ProjectionActiveSetReceipt,
    ProjectionNodeBeforeImage,
    SpecLineageEdgeSnapshot,
    SpecLineageReconciliationReceipt,
)
from okto_pulse.core.kg.projection_journal_records import (
    ProjectionJournalRecordsError,
    graph_write_records_from_payload,
    graph_write_records_to_payload,
)
from okto_pulse.core.kg.transaction import GraphWriteRecord


def test_graph_write_before_images_round_trip_through_json() -> None:
    records = [
        GraphWriteRecord(kind="node", entity_type="Spec", entity_id="new-node"),
        GraphWriteRecord(
            kind="property", entity_type="Spec", entity_id="existing-node",
            property_before_image=GraphNodePropertyBeforeImage(
                node_type="Spec", node_id="existing-node",
                attrs={
                    "updated_at": datetime(2026, 10, 20, tzinfo=UTC),
                    "score": Decimal("1.25"),
                    "source_id": UUID("00000000-0000-0000-0000-000000000001"),
                    "raw": b"before",
                },
            ),
        ),
        GraphWriteRecord(
            kind="edge", entity_type="belongs_to", entity_id="parent-edge",
            lineage_receipt=SpecLineageReconciliationReceipt(
                source_id="spec-child", target_id="spec-parent",
                target_rule_id="spec_parent.v1", target_attrs={},
                new_edge_created=True,
                removed_edges=(SpecLineageEdgeSnapshot(
                    source_id="spec-child", target_id="old-parent",
                    rule_id="spec_parent.v1", attrs={"weight": 1},
                ),),
            ),
        ),
        GraphWriteRecord(
            kind="projection", entity_type="spec", entity_id="owner",
            projection_receipt=ProjectionActiveSetReceipt(
                intent=ProjectionActiveSetIntent(
                    owner_type="spec", owner_id="owner", namespace="dependencies"
                ),
                before_images=(ProjectionNodeBeforeImage(
                    node_type="Requirement", node_id="old-node",
                    source_session_id="old-session", attrs={"title": "Before"},
                ),),
            ),
        ),
        GraphWriteRecord(
            kind="learning_association", entity_type="validates",
            entity_id="old-learning",
            learning_association_receipt=LearningBugAssociationReceipt(
                board_id="board", previous_learning_id="old-learning",
                replacement_learning_id="new-learning", bug_id="bug",
            ),
        ),
    ]

    payload = json.loads(json.dumps(graph_write_records_to_payload(records)))
    assert graph_write_records_from_payload(payload) == records


def test_missing_or_unknown_compensation_records_fail_closed() -> None:
    with pytest.raises(ProjectionJournalRecordsError):
        graph_write_records_from_payload(None)
    with pytest.raises(ProjectionJournalRecordsError):
        graph_write_records_from_payload([{"type": "arbitrary.Class", "fields": {}}])
