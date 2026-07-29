"""REST fail-closed scenario_type contract (spec ac16b3c9, scenario ts_7c2b7ec5).

A stale REST/API client that PATCHes a spec with an unsupported scenario_type
must fail closed (422) BEFORE any mutation, returning the SAME allowed-value
contract as the MCP path, and must not persist a partial scenario. Valid types
persist exactly. Complements the MCP coverage in
``test_scenario_type_fail_closed.py`` (ts_7c89bf9e).

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_scenario_type_rest_contract.py
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.community.api import deps as _deps_mod
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.core.infra.database import get_db
from sqlalchemy_test_models import Board, Spec, SpecStatus

USER_ID = "scenario-type-rest-user"
EXPECTED_INVALID_TYPE = {
    "error": "invalid_scenario_type",
    "value": "regression",
    "allowed": ["unit", "integration", "e2e", "manual", "negative"],
    "message": (
        "Invalid scenario_type 'regression'. "
        "Allowed values: unit, integration, e2e, manual, negative."
    ),
    "mutated": False,
}


async def _build_rest_spec(db_factory, *, scenarios: list[dict]):
    board_id = f"st-rest-board-{uuid.uuid4().hex[:8]}"
    spec_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="ST REST Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="ST REST Spec",
            status=SpecStatus.DRAFT, created_by=USER_ID,
            functional_requirements=[], acceptance_criteria=[],
            test_scenarios=scenarios, business_rules=[], api_contracts=[],
        ))
        await db.commit()

    app = FastAPI()
    app.include_router(specs_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    return TestClient(app), board_id, spec_id


@pytest_asyncio.fixture
async def rest_spec(db_factory):
    return await _build_rest_spec(db_factory, scenarios=[])


@pytest_asyncio.fixture
async def rest_legacy_spec(db_factory):
    return await _build_rest_spec(
        db_factory,
        scenarios=[
            {
                "id": "ts_legacy",
                "title": "historical",
                "given": "legacy given",
                "when": "legacy when",
                "then": "legacy then",
                "scenario_type": "regression",
                "status": "draft",
                "linked_criteria": None,
                "linked_task_ids": None,
            }
        ],
    )


def _scenario(
    scenario_type: str | None,
    *,
    scenario_id: str = "ts_rest",
    title: str = "rest",
) -> dict:
    payload = {
        "id": scenario_id, "title": title, "given": "g", "when": "w", "then": "t",
        "status": "draft",
        "linked_criteria": None, "linked_task_ids": None,
    }
    if scenario_type is not None:
        payload["scenario_type"] = scenario_type
    return payload


@pytest.mark.parametrize(
    ("method", "path", "body"),
    (
        (
            "post",
            "/api/v1/boards/never-reached/specs",
            {
                "title": "Invalid scenario type",
                "test_scenarios": [_scenario("regression")],
            },
        ),
        (
            "patch",
            "/api/v1/specs/never-reached",
            {"test_scenarios": [_scenario("regression")]},
        ),
    ),
)
def test_rest_invalid_scenario_type_is_rejected_before_dependencies(
    method,
    path,
    body,
):
    calls = {"auth": 0, "uow": 0}
    app = FastAPI()
    app.include_router(specs_router, prefix="/api/v1")

    def _counting_user():
        calls["auth"] += 1
        return USER_ID

    async def _counting_uow():
        calls["uow"] += 1
        yield object()

    app.dependency_overrides[_auth_mod.require_user] = _counting_user
    app.dependency_overrides[_deps_mod.get_unit_of_work] = _counting_uow

    response = TestClient(app).request(method, path, json=body)

    assert response.status_code == 422, response.text
    assert response.json() == EXPECTED_INVALID_TYPE
    assert "detail" not in response.json()
    assert calls == {"auth": 0, "uow": 0}


def test_rest_patch_spec_invalid_scenario_type_fails_closed(rest_spec):
    # ts_7c2b7ec5 — stale REST client submits an unsupported scenario_type.
    client, _board, spec_id = rest_spec
    resp = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={"test_scenarios": [_scenario("regression")]},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json() == EXPECTED_INVALID_TYPE
    assert "detail" not in resp.json()
    # fail-closed: NO mutation — the spec still has zero scenarios.
    got = client.get(f"/api/v1/specs/{spec_id}")
    assert got.status_code == 200, got.text
    assert (got.json().get("test_scenarios") or []) == []


def test_rest_patch_spec_valid_scenario_type_persists(rest_spec):
    client, _board, spec_id = rest_spec
    resp = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={"test_scenarios": [_scenario("negative")]},
    )
    assert resp.status_code == 200, resp.text
    scenarios = resp.json().get("test_scenarios") or []
    assert [s["scenario_type"] for s in scenarios] == ["negative"]


def test_rest_patch_rejects_type_alias_without_mutation(rest_spec):
    client, _board, spec_id = rest_spec
    scenario = _scenario(None)
    scenario["type"] = "negative"
    resp = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={"test_scenarios": [scenario]},
    )
    assert resp.status_code == 422, resp.text
    assert any(
        issue["type"] == "extra_forbidden"
        and issue["loc"][-2:] == [0, "type"]
        for issue in resp.json()["detail"]
    )
    got = client.get(f"/api/v1/specs/{spec_id}")
    assert got.status_code == 200, got.text
    assert (got.json().get("test_scenarios") or []) == []


def test_rest_patch_omitted_type_preserves_existing_and_defaults_new(rest_spec):
    client, _board, spec_id = rest_spec
    first = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={"test_scenarios": [_scenario("unit")]},
    )
    assert first.status_code == 200, first.text

    second = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={
            "test_scenarios": [
                _scenario(None, title="renamed"),
                _scenario(None, scenario_id="ts_new", title="new"),
            ]
        },
    )
    assert second.status_code == 200, second.text
    assert {
        scenario["id"]: scenario["scenario_type"]
        for scenario in second.json()["test_scenarios"]
    } == {
        "ts_rest": "unit",
        "ts_new": "integration",
    }


def test_rest_patch_omitted_type_preserves_unknown_legacy_and_defaults_new(
    rest_legacy_spec,
):
    client, _board, spec_id = rest_legacy_spec
    response = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={
            "test_scenarios": [
                _scenario(
                    None,
                    scenario_id="ts_legacy",
                    title="historical renamed",
                ),
                _scenario(None, scenario_id="ts_new", title="new"),
            ]
        },
    )
    assert response.status_code == 200, response.text
    assert {
        scenario["id"]: scenario["scenario_type"]
        for scenario in response.json()["test_scenarios"]
    } == {
        "ts_legacy": "regression",
        "ts_new": "integration",
    }
