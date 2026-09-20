import copy

import pytest

from okto_pulse.core.domain.mcp_permission_registry import McpAdmissionClass
from okto_pulse.core.inbound.historical_context import historical_context_follow_up
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.context_projection import project_spec_context, project_task_context
from okto_pulse.core.mcp.projection_envelope import _stable_payload_bytes


def test_historical_reader_is_closed_bounded_and_admitted_as_read():
    tool = next(tool for tool in server.mcp.iter_tools() if tool.name == "okto_pulse_get_historical_context")
    assert tool.admission_class is McpAdmissionClass.READER
    assert tool.parameters["additionalProperties"] is False
    fields = tool.parameters["properties"]
    assert set(fields) == {"board_id", "target_kind", "target_id", "offset", "limit"}
    assert fields["target_kind"]["enum"] == ["spec", "card"]
    assert fields["board_id"]["maxLength"] == 255
    assert fields["target_id"]["maxLength"] == 128
    assert fields["offset"]["maximum"] == 100_000
    assert fields["limit"]["maximum"] == 200


def test_gate_does_not_count_retained_pointer_as_omitted_content():
    source = {"card": {"id": "c"}, "unused": {str(i): i for i in range(30)}}
    before = project_task_context(source, card_id="c", profile="full", context_scope="gate")
    source["historical_context_read"] = historical_context_follow_up("b", "card", "c")
    after = project_task_context(source, card_id="c", profile="full", context_scope="gate")
    assert after["projection"]["omitted_count"] == before["projection"]["omitted_count"]


@pytest.mark.parametrize("kind,profile,scope", [
    ("card", "summary", "all"), ("card", "detail", "all"),
    ("card", "full", "all"), ("card", "full", "gate"),
    ("spec", "summary", "all"), ("spec", "detail", "all"), ("spec", "full", "all"),
])
def test_lazy_pointer_survives_compaction_and_adversarial_budget_without_source_claim(kind, profile, scope):
    pointer = historical_context_follow_up("界" * 255, kind, "é" * 128)
    # Force nested semantic blocks past initial budget bounds. Routing metadata
    # must remain complete, including offset=0, with no archive materialization.
    source = {"card": {"id": "c"}, "spec": {"id": "s"},
        "gate_readiness": {str(i): {str(j): "x" * 900 for j in range(6)} for i in range(12)},
        "historical_context_read": pointer}
    before = copy.deepcopy(source)
    result = (project_task_context(source, card_id="c", profile=profile, context_scope=scope)
        if kind == "card" else project_spec_context(source, profile=profile))
    assert result["historical_context_read"] == pointer
    assert source == before
    assert pointer["availability"] == "not_queried"
    assert set(pointer) == {"availability", "tool", "arguments", "pagination", "historical_only"}
    if "projection" in result:
        meta = result["projection"]
        assert meta["payload_bytes"] == _stable_payload_bytes(result)
        if "budget_bytes" in meta:
            assert meta["payload_bytes"] <= meta["budget_bytes"]
