from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.mcp.kg_tools import register_kg_tools


class _MCPRegistryDouble:
    def __init__(self) -> None:
        self.tools: dict = {}

    def tool(self):
        def decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return decorator


class _NullDb:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


def _registered_tools() -> dict:
    async def get_agent():
        return SimpleNamespace(id="agent-validation")

    registry = _MCPRegistryDouble()
    register_kg_tools(
        registry,
        get_agent=get_agent,
        get_uow=lambda: _NullDb(),
    )
    return registry.tools


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "candidate", "field"),
    [
        (
            "okto_pulse_kg_add_node_candidate",
            {
                "candidate_id": "node-1",
                "node_type": "Decision",
                "title": "Decision",
                "confidence": 0.91,
            },
            "candidate.confidence",
        ),
        (
            "okto_pulse_kg_add_edge_candidate",
            {
                "candidate_id": "edge-1",
                "edge_type": "relates_to",
                "from_candidate_id": "node-1",
                "to_candidate_id": "node-2",
                "unknown_edge_field": "secret-input-value",
            },
            "candidate.unknown_edge_field",
        ),
    ],
)
async def test_candidate_validation_error_is_strict_and_sanitized(
    tool_name,
    candidate,
    field,
):
    raw = await _registered_tools()[tool_name](
        session_id="session-not-read",
        candidate=candidate,
    )
    payload = json.loads(raw)

    assert payload["error"]["code"] == "invalid_candidate"
    assert field in payload["error"]["message"]
    assert "Extra inputs are not permitted" in payload["error"]["message"]
    for leak in (
        "input_value",
        "input_type",
        "errors.pydantic.dev",
        "secret-input-value",
    ):
        assert leak not in raw
