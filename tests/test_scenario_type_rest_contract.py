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

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.specs import router as specs_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, Spec, SpecStatus

USER_ID = "scenario-type-rest-user"


@pytest_asyncio.fixture
async def rest_spec(db_factory):
    board_id = f"st-rest-board-{uuid.uuid4().hex[:8]}"
    spec_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="ST REST Board", owner_id=USER_ID))
        db.add(Spec(
            id=spec_id, board_id=board_id, title="ST REST Spec",
            status=SpecStatus.DRAFT, created_by=USER_ID,
            functional_requirements=[], acceptance_criteria=[],
            test_scenarios=[], business_rules=[], api_contracts=[],
        ))
        await db.commit()

    app = FastAPI()
    app.include_router(specs_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    return TestClient(app), board_id, spec_id


def _scenario(scenario_type: str) -> dict:
    return {
        "id": "ts_rest", "title": "rest", "given": "g", "when": "w", "then": "t",
        "scenario_type": scenario_type, "status": "draft",
        "linked_criteria": None, "linked_task_ids": None,
    }


def test_rest_patch_spec_invalid_scenario_type_fails_closed(rest_spec):
    # ts_7c2b7ec5 — stale REST client submits an unsupported scenario_type.
    client, _board, spec_id = rest_spec
    resp = client.patch(
        f"/api/v1/specs/{spec_id}",
        json={"test_scenarios": [_scenario("regression")]},
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    # same allowed-value contract as MCP: the supported types are named.
    for t in ("unit", "integration", "e2e", "manual", "negative"):
        assert t in detail, detail
    assert "regression" in detail
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
