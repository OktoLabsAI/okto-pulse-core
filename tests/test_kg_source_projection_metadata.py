"""KG-13/15: chronology comes from the source and cannot be client-attested."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.kg.schemas import BeginConsolidationRequest, NodeCandidate
from okto_pulse.core.kg.source_projection_metadata import SourceProjectionMetadata


def test_source_chronology_preserves_instant_and_raw_status():
    source = SimpleNamespace(
        created_at=datetime(2001, 1, 2, tzinfo=timezone.utc),
        updated_at="2002-03-04T05:06:07-03:00", status="rejected", severity="critical",
    )
    first = SourceProjectionMetadata.from_source(source, is_bug=True)
    assert first == SourceProjectionMetadata.from_source(source, is_bug=True)
    assert first.graph_attributes() == {
        "source_created_at": "2001-01-02T00:00:00+00:00",
        "source_updated_at": "2002-03-04T08:06:07+00:00",
        "source_status": "rejected", "severity": "critical",
    }
    assert SourceProjectionMetadata.from_source({}, is_bug=False).graph_attributes() == {
        "source_created_at": None, "source_updated_at": None,
        "source_status": None, "severity": None,
    }


@pytest.mark.parametrize("field", [
    "source_created_at", "source_updated_at", "source_status", "severity",
    "_source_projection_metadata",
])
def test_client_cannot_supply_source_metadata(field):
    with pytest.raises(ValidationError):
        NodeCandidate(candidate_id="c", node_type="Bug", title="Bug", **{field: "forged"})


def test_internal_metadata_survives_session_request_without_entering_wire_contract():
    node = NodeCandidate(candidate_id="c", node_type="Bug", title="Bug")
    metadata = SourceProjectionMetadata.from_source({"status": "done"}, is_bug=True)
    node._source_projection_metadata = metadata
    request = BeginConsolidationRequest(
        board_id="b", artifact_type="card", artifact_id="c", raw_content="Bug",
        deterministic_candidates=[node],
    )
    assert request.deterministic_candidates[0]._source_projection_metadata is metadata
    assert "_source_projection_metadata" not in node.model_dump()
    assert "source_created_at" not in NodeCandidate.model_json_schema()["properties"]


@pytest.mark.asyncio
async def test_non_worker_cannot_submit_internal_attestation():
    from okto_pulse.core.kg.primitives import KGPrimitiveError, begin_consolidation

    node = NodeCandidate(candidate_id="c", node_type="Bug", title="Bug")
    node._source_projection_metadata = SourceProjectionMetadata.from_source(
        {"status": "done"}, is_bug=True
    )
    with pytest.raises(KGPrimitiveError, match="Only the deterministic worker"):
        await begin_consolidation(
            BeginConsolidationRequest(
                board_id="b", artifact_type="card", artifact_id="c", raw_content="Bug",
                deterministic_candidates=[node],
            ), agent_id="external-agent",
        )
