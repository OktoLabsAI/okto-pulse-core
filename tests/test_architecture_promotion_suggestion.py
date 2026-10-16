"""AC-ARQ-07/08: suggestions retain declared contracts without inventing HTTP."""

import copy

import pytest

from okto_pulse.core.domain.architecture_promotion_suggestion import (
    architecture_promotion_suggestion,
)


def test_event_suggestion_preserves_every_declared_element_and_no_roles_or_method():
    contract = {
        "name": "Order event",
        "contract_type": "event",
        "description": "Declared meaning",
        "event_schema": {},
        "error_contract": [{"code": "BUSY"}],
        "request_schema": {"type": "object"},
        "response_schema": {},
        "direction": "publish",
        "protocol": "AMQP",
        "participants": ["first", "second"],
        "schema_ref": "https://do-not-fetch.invalid/v3",
        "notes": "Declared note",
    }
    before = copy.deepcopy(contract)
    result = architecture_promotion_suggestion(contract)
    ir = result["proposed_ir"]
    assert ir["integration_type"] == "event" and ir["title"] == contract["name"]
    assert ir["contract_ref"] == contract["schema_ref"]
    assert all(
        ir["data_contract"][field] == contract[field]
        for field in (
            "request_schema",
            "response_schema",
            "event_schema",
            "error_contract",
            "direction",
            "protocol",
            "participants",
        )
    )
    assert not ({"provider", "consumer", "method", "endpoint"} & ir.keys())
    assert result["requires_author_review"] and result["scope_paths"] == [""]
    ir["data_contract"]["participants"].append("new")
    assert contract == before


@pytest.mark.parametrize(
    "kind", [None, "mcp", "in_process", "grpc", "HTTP maybe", "unknown"]
)
def test_unknown_type_is_missing_not_silently_converted_to_api(kind):
    result = architecture_promotion_suggestion(
        {
            "contract_type": kind,
            "endpoint": "https://example.invalid/orders",
            "protocol": "HTTP",
        }
    )
    assert (
        "integration_type" not in result["proposed_ir"]
        and "method" not in result["proposed_ir"]
    )
    assert result["missing_required_fields"] == ["title", "integration_type"]


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("http", "api"),
        ("API", "api"),
        ("event", "event"),
        ("queue", "queue"),
        ("file", "file"),
        ("stored_procedure", "stored_procedure"),
        ("data_contract", "data_contract"),
        ("other", "other"),
    ],
)
def test_only_explicit_discriminators_supply_the_existing_ir_type(kind, expected):
    result = architecture_promotion_suggestion(
        {"name": "Declared", "contract_type": kind}
    )
    assert result["proposed_ir"] == {"title": "Declared", "integration_type": expected}
    assert result["missing_required_fields"] == []
