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
        "resolved_at": None,
    }
    assert SourceProjectionMetadata.from_source({}, is_bug=False).graph_attributes() == {
        "source_created_at": None, "source_updated_at": None,
        "source_status": None, "severity": None,
        "resolved_at": None,
    }


@pytest.mark.parametrize("field", [
    "source_created_at", "source_updated_at", "source_status", "severity", "resolved_at",
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


def test_resolution_uses_last_done_and_clears_on_reopen():
    from okto_pulse.core.kg.source_projection_metadata import latest_resolution_time
    from okto_pulse.core.ports.consolidation import CardLifecycleTransition

    def transition(day, old, new):
        return CardLifecycleTransition(str(day), datetime(2001, 1, day), old, new)

    first = transition(2, "in_progress", "done")
    reopened = transition(3, "done", "in_progress")
    last = transition(4, "in_progress", "done")
    assert latest_resolution_time("done", (first,)) == "2001-01-02T00:00:00+00:00"
    assert latest_resolution_time("in_progress", (first,)) is None
    assert latest_resolution_time("in_progress", (reopened, first)) is None
    assert latest_resolution_time("done", (last, reopened)) == "2001-01-04T00:00:00+00:00"
    assert latest_resolution_time("done", ()) is None
    assert latest_resolution_time("done", (reopened, first)) is None
    assert latest_resolution_time("done", (last, transition(4, "done", "in_progress"))) is None
    assert latest_resolution_time("done", (transition(4, None, "done"),)) is None
    assert latest_resolution_time("done", (transition(4, ["in_progress"], "done"),)) is None
