"""The v0.4 MCP collection contract has no string conversion path."""
from __future__ import annotations

import inspect
import typing

import pytest
from pydantic import TypeAdapter, ValidationError

from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.helpers import validate_choice_options, validate_string_list


@pytest.mark.parametrize("value", ["", "one", "a|b", "a,b", '["a"]', 0, False, {}, ("a",), ["a", 1]])
def test_string_collection_refuses_other_shapes(value):
    with pytest.raises(ValueError):
        validate_string_list(value)


def test_native_strings_preserve_content_order_and_duplicates():
    assert validate_string_list([" A, B | C ", "", "x", "x", r"a\nb"]) == [
        "A, B | C", "x", "x", r"a\nb",
    ]
    assert validate_string_list(None) == []
    assert validate_string_list([]) == []


@pytest.mark.parametrize("value", [
    None, [], "", '[{"label":"A"}]', ["A"], [{"label": 1}],
    [{"label": " "}], [{"label": "A", "recommended": "false"}],
    [{"label": "A", "recommended": 0}], [{"label": "A", "tradeoff": 1}],
    [{"label": "A", "unknown": True}],
])
def test_options_refuse_converters_and_invalid_objects(value):
    with pytest.raises(ValueError):
        validate_choice_options(value)


def test_native_options_keep_metadata_and_defaults():
    assert validate_choice_options([
        {"label": " A, B | C ", "recommended": True, "tradeoff": "cost"},
        {"label": "other"},
    ]) == [
        {"label": "A, B | C", "recommended": True, "tradeoff": "cost"},
        {"label": "other", "recommended": False, "tradeoff": None},
    ]


CHOICE_TOOLS = [
    "okto_pulse_add_choice_comment",
    "okto_pulse_ask_ideation_choice_question",
    "okto_pulse_ask_refinement_choice_question",
    "okto_pulse_ask_spec_choice_question",
]


@pytest.mark.parametrize("name", CHOICE_TOOLS)
def test_choice_wire_contract_is_one_required_array(name):
    tool = getattr(server, name)
    schema = tool.parameters
    assert "options_json" not in schema["properties"]
    assert "options" in schema["required"]
    assert schema["properties"]["options"]["type"] == "array"
    adapter = TypeAdapter(typing.get_type_hints(tool.fn)["options"])
    assert adapter.validate_python([{"label": "A"}]) == [{"label": "A"}]
    for value in ('[{"label":"A"}]', ["A"], [{"label": "A", "recommended": "false"}],
                  [{"label": "A", "tradeoff": 1}], [{"label": "A", "unknown": 1}]):
        with pytest.raises(ValidationError):
            adapter.validate_python(value)


def test_string_list_tools_do_not_publish_string_alternatives():
    checked = 0
    for name in dir(server):
        tool = getattr(server, name)
        fn = getattr(tool, "fn", None)
        if not name.startswith("okto_pulse_") or fn is None:
            continue
        if "validate_string_list(" not in inspect.getsource(fn):
            continue
        hints = typing.get_type_hints(fn)
        for field, annotation in hints.items():
            if annotation not in (list[str], list[str] | None):
                continue
            adapter = TypeAdapter(annotation)
            assert adapter.validate_python(["a|b", "c,d"]) == ["a|b", "c,d"]
            for invalid in ("a", "a|b", '["a"]'):
                with pytest.raises(ValidationError):
                    adapter.validate_python(invalid)
            checked += 1
    assert checked >= 45
