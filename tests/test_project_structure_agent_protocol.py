"""The agent-facing protocol and copyable examples stay discoverable and valid."""

import inspect
import json
from pathlib import Path
import re

import pytest

from okto_pulse.core.domain.project_structure import apply_project_structure_batch


MCP = Path(__file__).resolve().parents[1] / "src/okto_pulse/core/mcp"
REFERENCE = MCP / "resources/reference/project_structure.md"


@pytest.mark.parametrize("relative", [
    "agent_instructions.md",
    "resources/workflows/specs.md",
    "resources/workflows/preflight.md",
    "resources/reference/spec_gates.md",
    "resources/reference/tool-docs/spec.md",
])
def test_authoring_and_review_entry_points_require_explicit_persisted_decision(relative):
    text = (MCP / relative).read_text(encoding="utf-8")
    assert "okto-pulse://reference/project-structure" in text
    assert "not applicable" in text
    assert "context" in text


def test_protocol_does_not_misrepresent_empty_data_as_na_or_claim_a_server_gate():
    text = REFERENCE.read_text(encoding="utf-8")
    for fragment in (
        "the applicability decision", "mandatory for every Spec",
        "No structure (`null`)", "empty structure (`[]`)", "chat-only statement",
        "not a newly implemented server gate", "not a machine-validated",
        "Coordinate with other", "not an atomic append",
        "not automatically reopened", "scope requires reassessment",
    ):
        assert fragment in text
    from okto_pulse.core.services.resource_gate_contracts import RESOURCE_TYPES
    assert "project_structure" not in RESOURCE_TYPES


def _examples():
    text = REFERENCE.read_text(encoding="utf-8")
    return [(name, json.loads(payload)) for name, payload in re.findall(
        r"<!-- tested-example: (\w+) -->\s*```json\s*(.*?)\s*```", text, re.S
    )]


def test_examples_cover_all_three_delivery_situations():
    assert {name for name, _ in _examples()} == {"brownfield", "greenfield", "scaffold"}


@pytest.mark.parametrize("name,arguments", _examples())
def test_documented_batches_match_real_mcp_arguments_and_apply_atomically(name, arguments):
    from okto_pulse.core.mcp import server

    inspect.signature(server.okto_pulse_update_spec_entity.fn).bind(**arguments)
    assert arguments["entity_type"] == "project_structure_node"
    assert arguments["operation"] == "batch"
    assert arguments["expected_spec_version"] > 0
    assert arguments["expected_structure_revision"] == 0
    assert arguments["idempotency_key"]
    nodes, ids, changed = apply_project_structure_batch(None, arguments["payload_json"])
    assert changed and len(nodes) == len(ids)
    expected = {"brownfield": "as_is", "greenfield": "to_be", "scaffold": "reference_scaffold"}[name]
    assert {node["classification"] for node in nodes} == {expected}
    assert all(not node["evidence_ids"] for node in nodes)
    if name == "scaffold":
        assert nodes[0]["note"] == nodes[0]["interpretation_limit"]
