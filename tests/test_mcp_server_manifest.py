"""Server identity and deterministic installed MCP inventory contract."""

from __future__ import annotations

import json

from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.manifest import (
    build_server_manifest,
    tool_inventory_document,
    tool_inventory_sha256,
)


def test_manifest_count_hash_and_aliases_match_live_catalog():
    document = tool_inventory_document(server.mcp)
    manifest = build_server_manifest(server.mcp, include_tool_names=True)
    inventory = manifest["tool_inventory"]

    assert manifest["manifest_version"] == "1.0"
    assert manifest["server"]["version"] == "0.3.3"
    assert inventory["count"] == len(document["tools"]) == 338
    assert inventory["tools"] == document["tools"]
    assert "okto_pulse_execute_test_scenario_evidence" in inventory["tools"]
    assert inventory["sha256"] == tool_inventory_sha256(document)
    assert inventory["aliases"]["okto_pulse_ask_question"] == "okto_pulse_ask"
    assert (
        inventory["aliases"]["okto_pulse_remove_business_rule"]
        == "okto_pulse_remove_spec_entity"
    )
    assert "ask" not in inventory["aliases"]
    assert "remove_spec_entity" not in inventory["aliases"]


def test_server_manifest_resource_is_compact_and_dynamic():
    spec = next(
        item
        for item in server.effective_resource_catalog().specs()
        if item.uri == "okto-pulse://server-manifest"
    )
    body = json.loads(spec.read())
    assert body["tool_inventory"]["count"] == len(server.mcp.iter_tools())
    assert "tools" not in body["tool_inventory"]
    assert len(body["tool_inventory"]["sha256"]) == 64
