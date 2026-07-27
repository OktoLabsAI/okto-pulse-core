from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi.testclient import TestClient

from okto_pulse.community.api.resource_gate import router as resource_gate_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db
from sqlalchemy_test_models import Board, Card, CardStatus, CardType, Ideation, Spec
from okto_pulse.core.services.resource_gate import ResourceGateService


USER_ID = "resource-gate-api-user"


@pytest_asyncio.fixture
async def _client_and_entities():
    from fastapi import FastAPI
    from okto_pulse.core.infra.database import get_session_factory

    db_factory = get_session_factory()
    board_id = f"resource-gate-api-board-{uuid.uuid4()}"
    ideation_id = str(uuid.uuid4())
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Resource Gate API", owner_id=USER_ID))
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="API resource gate ideation",
                created_by=USER_ID,
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="API resource gate spec",
                created_by=USER_ID,
                screen_mockups=[{"id": "mock-api-1", "title": "API flow"}],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Task without copied resources",
                created_by=USER_ID,
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(resource_gate_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"

    return TestClient(app), {
        "board_id": board_id,
        "ideation_id": ideation_id,
        "spec_id": spec_id,
        "card_id": card_id,
    }


def test_resource_gate_summary_mark_and_clear_na(_client_and_entities):
    client, ids = _client_and_entities
    board_id = ids["board_id"]
    ideation_id = ids["ideation_id"]

    summary = client.get(
        f"/api/v1/resource-gate/ideation/{ideation_id}",
        params={"board_id": board_id},
    )
    assert summary.status_code == 200
    body = summary.json()
    assert body["blocking"] is True
    assert {item["state"] for item in body["resources"]} == {"missing"}

    missing_justification = client.post(
        f"/api/v1/resource-gate/ideation/{ideation_id}/not-applicable",
        params={"board_id": board_id},
        json={"resource_type": "architecture", "source_channel": "api"},
    )
    assert missing_justification.status_code == 400
    assert missing_justification.json()["detail"]["code"] == "justification_required"

    ui_mark = client.post(
        f"/api/v1/resource-gate/ideation/{ideation_id}/not-applicable",
        params={"board_id": board_id},
        json={"resource_type": "architecture", "source_channel": "ui"},
    )
    assert ui_mark.status_code == 200
    ui_body = ui_mark.json()
    assert ui_body["warning"] is None
    arch = next(
        item
        for item in ui_body["summary"]["resources"]
        if item["resource_type"] == "architecture"
    )
    assert arch["state"] == "not_applicable"

    api_mark = client.post(
        f"/api/v1/resource-gate/ideation/{ideation_id}/not-applicable",
        params={"board_id": board_id},
        json={
            "resource_type": "mockup",
            "source_channel": "api",
            "justification": "Mockup is intentionally not applicable.",
        },
    )
    assert api_mark.status_code == 200
    assert api_mark.json()["warning"]

    cleared = client.request(
        "DELETE",
        f"/api/v1/resource-gate/ideation/{ideation_id}/not-applicable/architecture",
        params={"board_id": board_id},
        json={"reason": "Architecture became applicable."},
    )
    assert cleared.status_code == 200
    assert cleared.json()["cleared"] == 1
    arch_after = next(
        item
        for item in cleared.json()["summary"]["resources"]
        if item["resource_type"] == "architecture"
    )
    assert arch_after["state"] == "missing"


def test_resource_gate_spec_coverage_respects_board_setting(_client_and_entities):
    client, ids = _client_and_entities
    board_id = ids["board_id"]
    spec_id = ids["spec_id"]

    coverage = client.get(
        f"/api/v1/resource-gate/specs/{spec_id}/task-coverage",
        params={"board_id": board_id},
    )
    assert coverage.status_code == 200
    coverage_body = coverage.json()
    assert coverage_body["enabled"] is True
    assert coverage_body["allowed"] is False
    assert coverage_body["uncovered_resources"][0]["resource_type"] == "mockup"

    settings = client.patch(
        f"/api/v1/boards/{board_id}/settings/resource-gate",
        json={"require_spec_resource_task_coverage": False},
    )
    assert settings.status_code == 200
    assert settings.json()["settings"]["require_spec_resource_task_coverage"] is False

    coverage_disabled = client.get(
        f"/api/v1/resource-gate/specs/{spec_id}/task-coverage",
        params={"board_id": board_id},
    )
    assert coverage_disabled.status_code == 200
    assert coverage_disabled.json()["enabled"] is False
    assert coverage_disabled.json()["allowed"] is True


def test_resource_gate_identity_values_include_copied_card_kb_source():
    values = ResourceGateService._resource_identity_values(
        {
            "id": "cardkb_kb-original-123",
            "source": "copied_from_spec:spec-456:kb-original-123",
        }
    )

    assert "cardkb_kb-original-123" in values
    assert "kb-original-123" in values


def test_resource_gate_coverage_obligations_prefer_direct_snapshots():
    refs = ResourceGateService._coverage_obligation_refs(
        {
            "direct_refs": [{"id": "spec-snapshot"}],
            "inherited_refs": [{"id": "parent-original"}],
        }
    )

    assert refs == [{"id": "spec-snapshot"}]


def test_resource_gate_coverage_obligations_use_inherited_when_no_direct_snapshot():
    refs = ResourceGateService._coverage_obligation_refs(
        {
            "direct_refs": [],
            "inherited_refs": [{"id": "parent-original"}],
        }
    )

    assert refs == [{"id": "parent-original"}]
