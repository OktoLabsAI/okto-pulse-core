"""AF24 - REST/MCP adapter parity for shared application rules.

The scenarios in this file cover AF24 TS1..TS4:

* REST and MCP task-to-scenario linking persist equivalent bidirectional state.
* MCP no longer mutates scenario/card JSON fields directly.
* Metrics/summary residuals are recorded with concrete ownership.
* Existing REST/MCP public shapes stay compatible while sharing application code.
"""

from __future__ import annotations

import ast
import inspect
import json
import textwrap
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Card, CardType, Spec, SpecStatus
from okto_pulse.core.services.activity_log import activity_log_summary
from okto_pulse.core.services.analytics_service import spec_coverage_summary

USER = "af24-parity-agent"
PREFIX = "/api/v1"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER,
            "agent_name": USER,
            "board_id": board_id,
            "permissions": ["board:read", "cards:update", "specs:update"],
        },
    )()


async def _seed_link_fixture() -> tuple[str, str, str, str]:
    board_id = _id("af24-board")
    spec_id = _id("af24-spec")
    rest_card_id = _id("af24-rest-card")
    mcp_card_id = _id("af24-mcp-card")
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="AF24", owner_id=USER, settings={}))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF24 spec",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[
                    {
                        "id": "ts_rest",
                        "title": "REST scenario",
                        "linked_task_ids": [],
                        "linked_criteria": [],
                    },
                    {
                        "id": "ts_mcp",
                        "title": "MCP scenario",
                        "linked_task_ids": [],
                        "linked_criteria": [],
                    },
                    {
                        "id": "ts_missing_card",
                        "title": "Missing card scenario",
                        "linked_task_ids": [],
                        "linked_criteria": [],
                    },
                ],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        db.add(
            Card(
                id=rest_card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="REST task",
                created_by=USER,
                test_scenario_ids=[],
            )
        )
        db.add(
            Card(
                id=mcp_card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="MCP task",
                created_by=USER,
                test_scenario_ids=[],
            )
        )
        await db.commit()
    return board_id, spec_id, rest_card_id, mcp_card_id


async def _seed_wrong_board_fixture() -> tuple[str, str, str]:
    board_id = _id("af24-board")
    other_board_id = _id("af24-other-board")
    spec_id = _id("af24-spec")
    other_card_id = _id("af24-other-card")
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="AF24", owner_id=USER, settings={}))
        db.add(Board(id=other_board_id, name="AF24 other", owner_id=USER, settings={}))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF24 spec",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[
                    {
                        "id": "ts_wrong_board",
                        "title": "Wrong board scenario",
                        "linked_task_ids": [],
                    }
                ],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        db.add(
            Card(
                id=other_card_id,
                board_id=other_board_id,
                spec_id=None,
                title="Wrong board task",
                created_by=USER,
                test_scenario_ids=[],
            )
        )
        await db.commit()
    return board_id, spec_id, other_card_id


async def _seed_saturation_fixture() -> tuple[str, str, str]:
    board_id = _id("af24-board")
    spec_id = _id("af24-spec")
    card_id = _id("af24-test-card")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF24",
                owner_id=USER,
                settings={"max_scenarios_per_card": 1},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF24 spec",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=[
                    {
                        "id": "ts_existing",
                        "title": "Existing scenario",
                        "linked_task_ids": [card_id],
                    },
                    {
                        "id": "ts_saturation",
                        "title": "Saturation scenario",
                        "linked_task_ids": [],
                    },
                ],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="AF24 test task",
                created_by=USER,
                card_type=CardType.TEST,
                test_scenario_ids=["ts_existing"],
            )
        )
        await db.commit()
    return board_id, spec_id, card_id


async def _get_state(spec_id: str, *card_ids: str) -> tuple[dict[str, dict], dict[str, list[str]]]:
    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        scenarios = {s["id"]: dict(s) for s in spec.test_scenarios or []}
        cards = {}
        for card_id in card_ids:
            card = await db.get(Card, card_id)
            cards[card_id] = list(card.test_scenario_ids or [])
        return scenarios, cards


async def _call_mcp_link(
    board_id: str,
    spec_id: str,
    scenario_id: str,
    card_id: str,
) -> dict:
    mcp_server.register_session_factory(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))), patch.object(
        mcp_server, "check_permission", return_value=None
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_link_task")
        raw = await tool.fn(
            board_id=board_id,
            target_type="scenario",
            spec_id=spec_id,
            target_id=scenario_id,
            card_id=card_id,
        )
    return json.loads(raw)


@pytest.mark.asyncio
async def test_rest_and_mcp_link_task_to_scenario_persist_same_bidirectional_shape(
    client: TestClient,
) -> None:
    board_id, spec_id, rest_card_id, mcp_card_id = await _seed_link_fixture()

    for _ in range(2):
        resp = client.post(f"{PREFIX}/specs/{spec_id}/scenarios/ts_rest/link-task/{rest_card_id}")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "success": True,
            "spec_id": spec_id,
            "scenario_id": "ts_rest",
            "card_id": rest_card_id,
        }

    for _ in range(2):
        payload = await _call_mcp_link(board_id, spec_id, "ts_mcp", mcp_card_id)
        assert payload["success"] is True, payload
        assert payload["scenario_id"] == "ts_mcp"
        assert payload["card_id"] == mcp_card_id
        assert "saturation" in payload

    scenarios, cards = await _get_state(spec_id, rest_card_id, mcp_card_id)
    assert scenarios["ts_rest"]["linked_task_ids"] == [rest_card_id]
    assert scenarios["ts_mcp"]["linked_task_ids"] == [mcp_card_id]
    assert cards[rest_card_id] == ["ts_rest"]
    assert cards[mcp_card_id] == ["ts_mcp"]


@pytest.mark.asyncio
async def test_rest_and_mcp_missing_card_errors_do_not_leave_orphan_scenario_links(
    client: TestClient,
) -> None:
    board_id, spec_id, _rest_card_id, _mcp_card_id = await _seed_link_fixture()
    missing_rest = _id("missing-rest-card")
    missing_mcp = _id("missing-mcp-card")

    rest = client.post(f"{PREFIX}/specs/{spec_id}/scenarios/ts_missing_card/link-task/{missing_rest}")
    assert rest.status_code == 404
    assert rest.json()["detail"] == (
        f"Card '{missing_rest}' not found \u2014 cannot link a non-existent card."
    )

    mcp_payload = await _call_mcp_link(board_id, spec_id, "ts_missing_card", missing_mcp)
    assert mcp_payload == {
        "error": f"Card '{missing_mcp}' not found \u2014 cannot link a non-existent card."
    }

    scenarios, _cards = await _get_state(spec_id)
    assert scenarios["ts_missing_card"]["linked_task_ids"] == []


@pytest.mark.asyncio
async def test_rest_and_mcp_missing_scenario_errors_are_equivalent_and_non_mutating(
    client: TestClient,
) -> None:
    board_id, spec_id, rest_card_id, mcp_card_id = await _seed_link_fixture()

    rest = client.post(f"{PREFIX}/specs/{spec_id}/scenarios/nope/link-task/{rest_card_id}")
    assert rest.status_code == 404
    assert rest.json()["detail"] == "Scenario 'nope' not found in spec."

    mcp_payload = await _call_mcp_link(board_id, spec_id, "missing_mcp", mcp_card_id)
    assert mcp_payload == {"error": "Scenario 'missing_mcp' not found in spec."}

    scenarios, cards = await _get_state(spec_id, rest_card_id, mcp_card_id)
    assert all(not (scenario.get("linked_task_ids") or []) for scenario in scenarios.values())
    assert cards[rest_card_id] == []
    assert cards[mcp_card_id] == []


@pytest.mark.asyncio
async def test_rest_and_mcp_wrong_board_errors_are_equivalent_and_non_mutating(
    client: TestClient,
) -> None:
    board_id, spec_id, other_card_id = await _seed_wrong_board_fixture()
    expected = f"Card '{other_card_id}' belongs to a different board than spec '{spec_id}'."

    rest = client.post(f"{PREFIX}/specs/{spec_id}/scenarios/ts_wrong_board/link-task/{other_card_id}")
    assert rest.status_code == 422
    assert rest.json()["detail"] == expected

    mcp_payload = await _call_mcp_link(board_id, spec_id, "ts_wrong_board", other_card_id)
    assert mcp_payload == {"error": expected}

    scenarios, cards = await _get_state(spec_id, other_card_id)
    assert scenarios["ts_wrong_board"]["linked_task_ids"] == []
    assert cards[other_card_id] == []


@pytest.mark.asyncio
async def test_rest_and_mcp_saturation_errors_are_equivalent_and_non_mutating(
    client: TestClient,
) -> None:
    board_id, spec_id, card_id = await _seed_saturation_fixture()

    rest = client.post(f"{PREFIX}/specs/{spec_id}/scenarios/ts_saturation/link-task/{card_id}")
    assert rest.status_code == 409
    rest_detail = rest.json()["detail"]
    assert rest_detail["code"] == "max_scenarios_per_card_exceeded"
    assert rest_detail["facts"] == {"provided_count": 2, "max_scenarios_per_card": 1}

    mcp_payload = await _call_mcp_link(board_id, spec_id, "ts_saturation", card_id)
    assert mcp_payload["error"] == "max_scenarios_per_card_exceeded"
    assert mcp_payload["code"] == "max_scenarios_per_card_exceeded"
    assert mcp_payload["facts"] == {"provided_count": 2, "max_scenarios_per_card": 1}

    scenarios, cards = await _get_state(spec_id, card_id)
    assert scenarios["ts_existing"]["linked_task_ids"] == [card_id]
    assert scenarios["ts_saturation"]["linked_task_ids"] == []
    assert cards[card_id] == ["ts_existing"]


def test_mcp_task_scenario_link_delegates_without_direct_json_or_orm_mutation() -> None:
    tree = ast.parse(textwrap.dedent(inspect.getsource(mcp_server._link_task_to_scenario_internal)))

    call_names = set()
    referenced_names = set()
    assigned_json_keys = set()
    assigned_attrs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced_names.add(node.id)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                call_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                call_names.add(node.func.attr)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant):
                    assigned_json_keys.add(target.slice.value)
                if isinstance(target, ast.Attribute):
                    assigned_attrs.add(target.attr)

    assert "LinkTaskToScenarioUseCase" in referenced_names
    assert "LinkTaskToScenarioCommand" in referenced_names
    assert "get_unit_of_work_factory_for_mcp" in call_names
    assert "get_db_for_mcp" not in call_names
    assert "SpecService" not in referenced_names
    assert "CardService" not in referenced_names
    assert "CardUpdate" not in referenced_names
    assert "flag_modified" not in referenced_names
    assert "linked_task_ids" not in assigned_json_keys
    assert "test_scenario_ids" not in assigned_json_keys
    assert "test_scenarios" not in assigned_attrs
    assert "test_scenario_ids" not in assigned_attrs


def test_metric_summary_wrappers_delegate_to_shared_core_services() -> None:
    assert mcp_server._activity_log_summary("card_updated", {"title": "X"}) == activity_log_summary(
        "card_updated", {"title": "X"}
    )
    assert mcp_server._spec_coverage is spec_coverage_summary
    assert "activity_log_summary(action, details)" in inspect.getsource(mcp_server._activity_log_summary)
    assert "_spec_coverage(spec, cards=cards)" in inspect.getsource(mcp_server._mcp_spec_coverage_summary)


def test_af24_residual_inventory_documents_metrics_and_summary_ownership() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    inventory_path = repo_root / "docs" / "af24_rest_mcp_residual_inventory.md"
    assert inventory_path.exists()
    inventory = inventory_path.read_text(encoding="utf-8")

    required_fragments = (
        "src/okto_pulse/core/services/activity_log.py:67",
        "src/okto_pulse/core/services/analytics_service.py:737",
        "owner: core",
        "REST/MCP",
        "activity_log_summary",
        "spec_coverage_summary",
        "no open AF24 residual",
    )
    for fragment in required_fragments:
        assert fragment in inventory
