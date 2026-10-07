"""Native MCP booleans have no textual or numeric compatibility branch."""
import json
import typing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import StrictBool, TypeAdapter, ValidationError
from okto_pulse.core.mcp import server


@pytest.mark.parametrize("value", ["true", "false", "1", "0", "yes", "no", "", 0, 1, None, {}, []])
def test_boolean_validator_refuses_non_boolean(value):
    with pytest.raises(ValueError, match="native boolean"):
        server._require_boolean(value)


@pytest.mark.parametrize("value", [True, False])
def test_boolean_validator_preserves_native_value(value):
    assert server._require_boolean(value) is value


def test_all_native_boolean_tool_annotations_reject_strings_and_numbers():
    checked = 0
    for tool in server.mcp.iter_tools():
        for field, annotation in typing.get_type_hints(tool.fn, include_extras=True).items():
            if annotation not in (StrictBool, StrictBool | None):
                continue
            adapter = TypeAdapter(annotation)
            assert adapter.validate_python(True) is True
            assert adapter.validate_python(False) is False
            for invalid in ("true", "false", "yes", "no", "1", "0", 1, 0):
                with pytest.raises(ValidationError):
                    adapter.validate_python(invalid)
            schema = tool.parameters["properties"][field]
            branches = schema.get("anyOf", [schema])
            assert {branch["type"] for branch in branches} <= {"boolean", "null"}
            checked += 1
    assert checked >= 30


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["true", "false", "yes", "no", "1", "0", "", 0, 1, None])
async def test_string_filter_flag_rejected_before_use_case(monkeypatch, value):
    monkeypatch.setattr(server, "_get_agent_ctx", AsyncMock(return_value=SimpleNamespace(permissions=["*"])))
    monkeypatch.setattr(server, "check_permission", lambda *_: None)

    def no_uow():
        pytest.fail("invalid filters must not open a unit of work")
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", no_uow)
    result = json.loads(await server.okto_pulse_list_by_board.fn(
        board_id="board-1", entity_type="story", filters={"converted": value},
    ))
    assert result["error_code"] == "invalid_filter"
    assert result["supported"] == [True, False]


def test_story_link_has_no_ignored_flag():
    assert "mark_converted" not in server.okto_pulse_link_story_to_ideation.parameters["properties"]
