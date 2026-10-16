"""Published MCP schema, safe errors and malformed-input preflight."""

import copy
import json
from unittest.mock import AsyncMock

import jsonschema
import pytest
from pydantic import BaseModel, ConfigDict

from okto_pulse.core.domain.architecture_classification import (
    ArchitectureClassificationError,
)
from okto_pulse.core.domain.mcp_permission_registry import McpAdmissionClass
from okto_pulse.core.inbound.architecture_classification import classification_error
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.catalog import CoreMcpCatalog, closed_mcp_schema


def request():
    return {
        "board_id": "board",
        "spec_id": "spec",
        "batch": {
            "expected_spec_version": 1,
            "expected_spec_edition": 2,
            "idempotency_key": "intent-1",
            "decisions": [
                {
                    "candidate_ref": "candidate",
                    "expected_source_digest": "a" * 64,
                    "disposition": "context_only",
                    "reason": "External context",
                }
            ],
        },
    }


def test_public_schema_resolves_nested_references_and_keeps_bounds_and_writer_admission():
    tool = server.okto_pulse_classify_architecture_candidates
    assert tool.admission_class == McpAdmissionClass.WRITER
    validator = jsonschema.Draft202012Validator(tool.parameters)
    validator.validate(request())
    for case in (
        "extra_root",
        "extra_batch",
        "extra_decision",
        "bad_disposition",
        "many",
        "fractional_version",
    ):
        raw = request()
        if case == "extra_root":
            raw["trusted"] = True
        elif case == "extra_batch":
            raw["batch"]["approved"] = True
        elif case == "extra_decision":
            raw["batch"]["decisions"][0]["waiver"] = True
        elif case == "bad_disposition":
            raw["batch"]["decisions"][0]["disposition"] = "approve"
        elif case == "many":
            raw["batch"]["decisions"] *= 51
        else:
            raw["batch"]["expected_spec_version"] = 1.5
        assert not validator.is_valid(raw), case


class _Child(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: str


class _Parent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    child: _Child


def test_embedding_two_model_parameters_keeps_each_definition_in_its_own_scope():
    catalog = CoreMcpCatalog(name="schema", version="test")

    @catalog.tool()
    @closed_mcp_schema
    async def example(left: _Parent, right: _Parent):
        return None

    schema = example.parameters
    validator = jsonschema.Draft202012Validator(schema)
    validator.validate(
        {"left": {"child": {"value": "one"}}, "right": {"child": {"value": "two"}}}
    )
    assert not validator.is_valid(
        {"left": {"child": {"value": 7}}, "right": {"child": {"value": "two"}}}
    )
    assert (
        schema["properties"]["left"]["properties"]["child"]["$ref"]
        == "#/properties/left/$defs/_Child"
    )
    assert (
        schema["properties"]["right"]["properties"]["child"]["$ref"]
        == "#/properties/right/$defs/_Child"
    )


def test_authored_ir_schema_is_closed_while_contract_json_remains_data():
    from okto_pulse.core.domain.architecture_classification import (
        ArchitectureClassificationBatch,
    )

    raw = request()
    raw["batch"]["decisions"][0] = {
        **raw["batch"]["decisions"][0],
        "disposition": "promote_to_ir",
        "reason": None,
        "integration_requirements": [
            {
                "title": "Publish orders",
                "integration_type": "event",
                "data_contract": {
                    "properties": {"approved": {"type": "boolean"}},
                    "examples": [True, None, 1],
                },
            }
        ],
    }
    validator = jsonschema.Draft202012Validator(
        server.okto_pulse_classify_architecture_candidates.parameters
    )
    validator.validate(raw)
    batch = ArchitectureClassificationBatch.model_validate(raw["batch"])
    assert (
        batch.decisions[0].integration_requirements[0]
        == raw["batch"]["decisions"][0]["integration_requirements"][0]
    )
    raw["batch"]["decisions"][0]["integration_requirements"][0]["approved"] = True
    assert not validator.is_valid(raw)
    with pytest.raises(ValueError):
        ArchitectureClassificationBatch.model_validate(raw["batch"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,status", [("bool", 422), ("unknown", 422), ("size", 413), ("last_ir", 422)]
)
async def test_invalid_mcp_batch_never_authenticates_or_opens_a_uow(
    monkeypatch, case, status
):
    raw = request()
    if case == "bool":
        raw["batch"]["expected_spec_version"] = True
    elif case == "unknown":
        raw["batch"]["private_secret"] = "DO_NOT_ECHO_THIS"
    elif case == "size":
        raw["batch"]["decisions"][0]["reason"] = "DO_NOT_ECHO_THIS" * 20000
    else:
        raw["batch"]["decisions"][0] = {
            **raw["batch"]["decisions"][0],
            "disposition": "promote_to_ir",
            "integration_requirements": [{"title": "DO_NOT_ECHO_THIS"}],
            "reason": None,
        }
    before = copy.deepcopy(raw)
    auth = AsyncMock(side_effect=AssertionError("authentication must not run"))
    monkeypatch.setattr(server, "_get_agent_ctx", auth)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: pytest.fail("UOW must not open"),
    )
    result = json.loads(
        await server.okto_pulse_classify_architecture_candidates.fn(**raw)
    )
    assert result["success"] is False and result["status_code"] == status
    assert "DO_NOT_ECHO_THIS" not in json.dumps(result)
    assert raw == before
    auth.assert_not_awaited()


def test_unrecognized_classification_error_never_echoes_provider_diagnostics():
    result = classification_error(
        ArchitectureClassificationError("private-provider-diagnostic")
    )
    assert result.status_code == 422
    assert result.payload() == {
        "error": "architecture_classification_invalid",
        "message": "Classification request validation failed.",
    }
