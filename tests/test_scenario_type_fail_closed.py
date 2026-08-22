"""Behavioral coverage for fail-closed scenario_type centralization
(spec ac16b3c9, IMP card 58844a26).

Exercises the REAL write surfaces — the ``okto_pulse_add_test_scenario`` /
``okto_pulse_update_test_scenario`` MCP tools and the ``SpecService``
create/update persistence gates — and proves:

* an unsupported scenario_type is rejected BEFORE any mutation, with a
  structured error naming the allowed values (no silent normalization to
  ``integration``);
* supported values persist EXACTLY;
* an omitted value still defaults to ``integration`` (a default, not a coercion
  of an invalid value);
* unchanged legacy/invalid values are GRANDFATHERED so the whole-list update
  path (UI full-list / REST PUT) keeps re-serializing historical data, while a
  new or changed invalid value on that same path still fails closed.

Validator can reproduce with:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_scenario_type_fail_closed.py
"""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Spec, SpecStatus
from okto_pulse.core.models.schemas import (
    SpecCreate,
    SpecResponse,
    SpecUpdate,
    TestScenario as ScenarioRead,
    TestScenarioWrite as ScenarioWrite,
)
from okto_pulse.core.services.application_schemas import (
    PersistedTestScenarioSpecUpdate,
)
from okto_pulse.core.services.main import SpecService
from okto_pulse.core.services.test_scenario_lifecycle import (
    InvalidScenarioTypeError,
    VALID_SCENARIO_TYPES,
)

pytestmark = pytest.mark.asyncio

USER_ID = "scenario-type-agent"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _stub_ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": USER_ID,
            "board_id": board_id,
            "permissions": ["board:read", "specs:update"],
        },
    )()


async def _seed(db_factory, scenarios=None) -> tuple[str, str]:
    board_id, spec_id = _id("st-board"), _id("st-spec")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Scenario-Type Board",
                owner_id=USER_ID,
                settings={"skip_test_coverage_global": False},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Scenario-Type Spec",
                status=SpecStatus.DRAFT,
                created_by=USER_ID,
                acceptance_criteria=[],
                test_scenarios=scenarios or [],
                functional_requirements=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        await db.commit()
    return board_id, spec_id


async def _seed_board(db_factory) -> str:
    board_id = _id("st-board")
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Scenario-Type Board", owner_id=USER_ID, settings={}))
        await db.commit()
    return board_id


async def _stored(db_factory, spec_id) -> list:
    async with db_factory() as db:
        spec = await SpecService(db).get_spec(spec_id)
        return list(spec.test_scenarios or [])


async def _call_tool(db_factory, tool_name, **kwargs):
    register_mcp_test_runtime(db_factory)
    with patch.object(
        mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx(kwargs["board_id"]))
    ), patch.object(mcp_server, "check_permission", return_value=None):
        tool = await mcp_server.mcp.get_tool(tool_name)
        return json.loads(await tool.fn(**kwargs))


# ---------------------------------------------------------------------------
# MCP add tool (server.py okto_pulse_add_test_scenario)
# ---------------------------------------------------------------------------


async def test_add_invalid_scenario_type_fails_closed_no_mutation(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S",
        given="g", when="w", then="t", scenario_type="regression",
    )
    assert payload.get("error") == "invalid_scenario_type", payload
    for t in VALID_SCENARIO_TYPES:
        assert t in payload["message"], payload
    assert "regression" in payload["message"]
    assert "No scenario was appended" in payload["message"]
    # fail-closed: nothing persisted, NOT silently normalized to integration.
    assert await _stored(db_factory, spec_id) == []


async def test_add_valid_scenario_type_persists_exactly(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S",
        given="g", when="w", then="t", scenario_type="negative",
    )
    assert payload.get("success") is True, payload
    assert payload["scenario"]["scenario_type"] == "negative"
    assert [s["scenario_type"] for s in await _stored(db_factory, spec_id)] == ["negative"]


async def test_add_omitted_scenario_type_defaults_integration(db_factory):
    board_id, spec_id = await _seed(db_factory)
    payload = await _call_tool(
        db_factory, "okto_pulse_add_test_scenario",
        board_id=board_id, spec_id=spec_id, title="S", given="g", when="w", then="t",
    )
    assert payload.get("success") is True, payload
    assert payload["scenario"]["scenario_type"] == "integration"


async def test_add_preserves_an_existing_unknown_legacy_type(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[
            {
                "id": "ts_legacy",
                "title": "historical",
                "scenario_type": "regression",
                "status": "draft",
            }
        ],
    )
    payload = await _call_tool(
        db_factory,
        "okto_pulse_add_test_scenario",
        board_id=board_id,
        spec_id=spec_id,
        title="New negative path",
        given="invalid input",
        when="the request is submitted",
        then="it is rejected",
        scenario_type="negative",
    )
    assert payload.get("success") is True, payload
    stored = {
        scenario["id"]: scenario["scenario_type"]
        for scenario in await _stored(db_factory, spec_id)
    }
    assert stored == {
        "ts_legacy": "regression",
        payload["scenario"]["id"]: "negative",
    }


# ---------------------------------------------------------------------------
# MCP update tool (server.py okto_pulse_update_test_scenario)
# ---------------------------------------------------------------------------


async def test_update_invalid_scenario_type_fails_closed(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_keep", "title": "keep", "scenario_type": "unit", "status": "draft"}],
    )
    payload = await _call_tool(
        db_factory, "okto_pulse_update_test_scenario",
        board_id=board_id, spec_id=spec_id, scenario_id="ts_keep", scenario_type="regression",
    )
    assert payload.get("error") == "invalid_scenario_type", payload
    assert "No scenario was updated" in payload["message"]
    # unchanged
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "unit"


async def test_update_valid_scenario_type_persists(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_x", "title": "x", "scenario_type": "unit", "status": "draft"}],
    )
    payload = await _call_tool(
        db_factory, "okto_pulse_update_test_scenario",
        board_id=board_id, spec_id=spec_id, scenario_id="ts_x", scenario_type="manual",
    )
    assert payload.get("success") is True, payload
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "manual"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("title", "cosmetic rename"),
        ("given", "semantic body edit"),
    ),
)
async def test_update_body_without_type_preserves_unknown_legacy_type(
    db_factory,
    field,
    value,
):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[
            {
                "id": "ts_legacy",
                "title": "historical",
                "scenario_type": "regression",
                "given": "old given",
                "when": "old when",
                "then": "old then",
                "status": "draft",
            }
        ],
    )
    payload = await _call_tool(
        db_factory,
        "okto_pulse_update_test_scenario",
        board_id=board_id,
        spec_id=spec_id,
        scenario_id="ts_legacy",
        **{field: value},
    )
    assert payload.get("success") is True, payload
    assert payload["updated_fields"] == [field]
    stored = (await _stored(db_factory, spec_id))[0]
    assert stored[field] == value
    assert stored["scenario_type"] == "regression"


# ---------------------------------------------------------------------------
# Service whole-list update_spec (UI full-list / REST PUT bypass)
# ---------------------------------------------------------------------------


async def test_update_spec_new_invalid_scenario_rejected(db_factory):
    board_id, spec_id = await _seed(db_factory)
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.update_spec(
                spec_id,
                USER_ID,
                PersistedTestScenarioSpecUpdate.from_iterable(
                    [
                        {
                            "id": "ts_new",
                            "title": "new",
                            "scenario_type": "bogus",
                            "status": "draft",
                        }
                    ]
                ),
            )
    assert await _stored(db_factory, spec_id) == []  # rejected before mutation


async def test_update_spec_omitted_type_preserves_existing_and_defaults_new(
    db_factory,
):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[
            {
                "id": "ts_keep",
                "title": "keep",
                "scenario_type": "unit",
                "status": "draft",
            }
        ],
    )
    async with db_factory() as db:
        await SpecService(db).update_spec(
            spec_id,
            USER_ID,
            SpecUpdate.model_validate(
                {
                    "test_scenarios": [
                        {"id": "ts_keep", "title": "renamed", "status": "draft"},
                        {"id": "ts_new", "title": "new", "status": "draft"},
                    ]
                }
            ),
        )
        await db.commit()

    stored = {
        scenario["id"]: scenario["scenario_type"]
        for scenario in await _stored(db_factory, spec_id)
    }
    assert stored == {"ts_keep": "unit", "ts_new": "integration"}


async def test_update_spec_omitted_type_preserves_unknown_legacy_value(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[
            {
                "id": "ts_legacy",
                "title": "legacy",
                "scenario_type": "regression",
                "status": "draft",
            }
        ],
    )
    async with db_factory() as db:
        await SpecService(db).update_spec(
            spec_id,
            USER_ID,
            SpecUpdate.model_validate(
                {
                    "test_scenarios": [
                        {
                            "id": "ts_legacy",
                            "title": "legacy renamed",
                            "status": "draft",
                        }
                    ]
                }
            ),
        )
        await db.commit()

    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "regression"


async def test_update_spec_grandfathers_unchanged_legacy(db_factory):
    # legacy invalid value inserted out-of-band (as historical data would be).
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        # Internal read-modify-write flows use the narrow persisted carrier;
        # public API/MCP request DTOs remain closed to historical values.
        await svc.update_spec(
            spec_id,
            USER_ID,
            PersistedTestScenarioSpecUpdate.from_iterable(
                [
                    {
                        "id": "ts_legacy",
                        "title": "legacy",
                        "scenario_type": "regression",
                        "status": "draft",
                    },
                    ScenarioWrite(
                        id="ts_ok",
                        title="ok",
                        scenario_type="unit",
                        status="draft",
                    ),
                ]
            ),
        )
        await db.commit()
    stored = {s["id"]: s["scenario_type"] for s in await _stored(db_factory, spec_id)}
    assert stored == {"ts_legacy": "regression", "ts_ok": "unit"}


async def test_update_spec_changing_legacy_to_invalid_rejected(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.update_spec(
                spec_id,
                USER_ID,
                PersistedTestScenarioSpecUpdate.from_iterable(
                    [
                        {
                            "id": "ts_legacy",
                            "title": "legacy",
                            "scenario_type": "still_bad",
                            "status": "draft",
                        }
                    ]
                ),
            )


async def test_update_spec_changing_legacy_to_valid_ok(db_factory):
    board_id, spec_id = await _seed(
        db_factory,
        scenarios=[{"id": "ts_legacy", "title": "legacy", "scenario_type": "regression", "status": "draft"}],
    )
    async with db_factory() as db:
        svc = SpecService(db)
        await svc.update_spec(
            spec_id, USER_ID,
            SpecUpdate(test_scenarios=[
                {"id": "ts_legacy", "title": "legacy", "scenario_type": "e2e", "status": "draft"}
            ]),
        )
        await db.commit()
    assert (await _stored(db_factory, spec_id))[0]["scenario_type"] == "e2e"


# ---------------------------------------------------------------------------
# Service create_spec
# ---------------------------------------------------------------------------


async def test_create_spec_invalid_scenario_type_rejected(db_factory):
    with pytest.raises(ValidationError):
        SpecCreate.model_validate(
            {
                "title": "S",
                "test_scenarios": [
                    {"id": "ts_c", "title": "c", "scenario_type": "bogus"}
                ],
            }
        )

    # Defense in depth remains in the service for non-transport callers that
    # bypass normal Pydantic validation.
    board_id = await _seed_board(db_factory)
    invalid = ScenarioWrite.model_construct(
        id="ts_c",
        title="c",
        scenario_type="bogus",
    )
    async with db_factory() as db:
        svc = SpecService(db)
        with pytest.raises(InvalidScenarioTypeError):
            await svc.create_spec(
                board_id, USER_ID,
                SpecCreate.model_construct(
                    title="S",
                    delivery_context="brownfield",
                    test_scenarios=[invalid],
                ),
            )


async def test_create_spec_valid_scenario_type_ok(db_factory):
    board_id = await _seed_board(db_factory)
    async with db_factory() as db:
        svc = SpecService(db)
        spec = await svc.create_spec(
            board_id, USER_ID,
            SpecCreate(
                title="S",
                delivery_context="brownfield",
                test_scenarios=[
                    ScenarioWrite(
                        id="ts_c",
                        title="c",
                        scenario_type="manual",
                    )
                ],
            ),
        )
        await db.commit()
        assert spec is not None
        assert spec.test_scenarios[0]["scenario_type"] == "manual"


async def test_write_schemas_publish_exact_enum_while_response_reads_legacy():
    schema = SpecUpdate.model_json_schema()
    write_schema = schema["$defs"]["TestScenarioWrite"]
    scenario_schema = write_schema["properties"]["scenario_type"]
    assert scenario_schema["enum"] == list(VALID_SCENARIO_TYPES)
    assert scenario_schema["default"] == "integration"
    assert write_schema["additionalProperties"] is False
    assert "invalid, forbidden, or denial paths" in scenario_schema["description"]

    response_field = SpecResponse.model_json_schema()["$defs"]["TestScenario"][
        "properties"
    ]["scenario_type"]
    assert response_field["type"] == "string"
    assert "enum" not in response_field

    legacy = ScenarioRead.model_validate(
        {
            "id": "ts_legacy",
            "title": "historical",
            "scenario_type": "regression",
        }
    )
    assert legacy.scenario_type == "regression"


async def test_write_schema_rejects_legacy_type_alias_field():
    with pytest.raises(ValidationError) as exc_info:
        SpecUpdate.model_validate(
            {
                "test_scenarios": [
                    {
                        "id": "ts_alias",
                        "title": "wrong field name",
                        "type": "negative",
                    }
                ]
            }
        )
    assert {
        (error["type"], tuple(error["loc"]))
        for error in exc_info.value.errors()
    } == {
        ("extra_forbidden", ("test_scenarios", 0, "type")),
    }


def _schema_enum(schema: dict) -> list[str] | None:
    if isinstance(schema.get("enum"), list):
        return schema["enum"]
    for branch in schema.get("anyOf", []):
        found = _schema_enum(branch)
        if found is not None:
            return found
    return None


async def test_mcp_write_tools_publish_closed_enum_but_list_filter_stays_raw():
    add_tool = await mcp_server.mcp.get_tool("okto_pulse_add_test_scenario")
    update_tool = await mcp_server.mcp.get_tool("okto_pulse_update_test_scenario")
    list_tool = await mcp_server.mcp.get_tool("okto_pulse_list_test_scenarios")

    assert _schema_enum(
        add_tool.parameters["properties"]["scenario_type"]
    ) == list(VALID_SCENARIO_TYPES)
    assert (
        "invalid, forbidden, or denial paths"
        in add_tool.parameters["properties"]["scenario_type"]["description"]
    )
    assert _schema_enum(
        update_tool.parameters["properties"]["scenario_type"]
    ) == list(VALID_SCENARIO_TYPES)
    assert (
        update_tool.parameters["properties"]["scenario_type"]["default"] is None
    )
    assert (
        _schema_enum(list_tool.parameters["properties"]["scenario_type"])
        is None
    )


async def test_real_fastmcp_transport_rejects_invalid_value_and_type_alias():
    from fastmcp import Client

    from okto_pulse.community.adapters.mcp_host import CommunityMcpHostProvider
    from okto_pulse.core.ports.mcp_resources import (
        StaticMcpResourceCatalog,
        freeze_mcp_resource_catalog,
    )

    frozen = freeze_mcp_resource_catalog(
        StaticMcpResourceCatalog("scenario-type-transport", (), precedence=1)
    )
    host = CommunityMcpHostProvider().materialize_catalog(
        mcp_server.mcp.resolve(),
        resource_catalog=frozen,
        projection_identity=frozen.identity,
    )
    base_args = {
        "board_id": "never-reached",
        "spec_id": "never-reached",
        "title": "schema validation",
        "given": "g",
        "when": "w",
        "then": "t",
    }
    context_lookup = AsyncMock(
        side_effect=AssertionError("transport validation must precede context")
    )
    with (
        patch.object(mcp_server, "_get_agent_ctx", context_lookup),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            side_effect=AssertionError("transport validation must precede UoW"),
        ) as uow_factory,
    ):
        async with Client(host) as client:
            invalid_value = await client.call_tool(
                "okto_pulse_add_test_scenario",
                {**base_args, "scenario_type": "regression"},
                raise_on_error=False,
            )
            invalid_alias = await client.call_tool(
                "okto_pulse_add_test_scenario",
                {**base_args, "type": "negative"},
                raise_on_error=False,
            )
            empty_update_type = await client.call_tool(
                "okto_pulse_update_test_scenario",
                {
                    "board_id": "never-reached",
                    "spec_id": "never-reached",
                    "scenario_id": "never-reached",
                    "scenario_type": "",
                },
                raise_on_error=False,
            )
            null_update_type = await client.call_tool(
                "okto_pulse_update_test_scenario",
                {
                    "board_id": "never-reached",
                    "spec_id": "never-reached",
                    "scenario_id": "never-reached",
                    "scenario_type": None,
                },
                raise_on_error=False,
            )

    context_lookup.assert_not_awaited()
    uow_factory.assert_not_called()

    assert invalid_value.is_error is True
    assert invalid_value.structured_content["error_code"] == "validation_failed"
    assert invalid_value.structured_content["data"]["issues"][0]["type"] == (
        "literal_error"
    )
    assert invalid_value.structured_content["data"]["issues"][0]["loc"] == [
        "scenario_type"
    ]

    assert invalid_alias.is_error is True
    assert invalid_alias.structured_content["error_code"] == "validation_failed"
    alias_issue = invalid_alias.structured_content["data"]["issues"][0]
    assert alias_issue["type"] == "unexpected_keyword_argument"
    assert alias_issue["loc"] == ["type"]

    assert empty_update_type.is_error is True
    assert empty_update_type.structured_content["error_code"] == "validation_failed"
    empty_issue = empty_update_type.structured_content["data"]["issues"][0]
    assert empty_issue["type"] == "literal_error"
    assert empty_issue["loc"] == ["scenario_type"]

    assert null_update_type.is_error is True
    assert null_update_type.structured_content["error_code"] == "validation_failed"
    null_issue = null_update_type.structured_content["data"]["issues"][0]
    assert null_issue["type"] == "literal_error"
    assert null_issue["loc"] == ["scenario_type"]
