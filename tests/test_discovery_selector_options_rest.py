from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import discovery as discovery_api
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.db import Board, Spec, SpecStatus
from okto_pulse.core.services.discovery_selector_catalog import (
    SAFE_SELECTOR_OPTION_FIELDS,
    SUPPORTED_SPEC_CHILD_TYPES,
    get_default_discovery_selector_cache,
)


OWNER_ID = "selector-rest-owner"
OTHER_USER_ID = "selector-rest-other"


def _make_spec(board_id: str) -> Spec:
    return Spec(
        id=str(uuid.uuid4()),
        board_id=board_id,
        title="Selector REST Spec",
        description="description must never appear in selector response",
        context="context must never appear in selector response",
        status=SpecStatus.APPROVED,
        version=3,
        created_by=OWNER_ID,
        functional_requirements=["FR one", "FR ten"],
        technical_requirements=[{"id": "tr-1", "title": "TR one"}],
        acceptance_criteria=[{"id": "ac-1", "title": "AC one"}],
        test_scenarios=[],
        business_rules=[
            {
                "id": "br-1",
                "title": "BR one",
                "rule": "rule body remains server-side",
                "linked_requirements": ["FR1"],
            }
        ],
        api_contracts=[
            {
                "id": "api-1",
                "method": "GET",
                "path": "/api/v1/discovery/boards/{board_id}/selector-options",
                "description": "api body remains server-side",
                "request_body": {"secret": "nope"},
            }
        ],
        integration_requirements=[
            {"id": "ir-1", "title": "IR one", "provider": "REST", "consumer": "UI"}
        ],
        observability_requirements=[
            {
                "id": "or-1",
                "title": "OR one",
                "metric_name": "discovery_selector_options_latency_ms",
            }
        ],
        decisions=[
            {"id": "dec-1", "title": "Active decision", "status": "active"},
            {"id": "dec-2", "title": "Old decision", "status": "superseded"},
        ],
        labels=[],
    )


@pytest_asyncio.fixture
async def selector_rest_client():
    get_default_discovery_selector_cache().clear()
    db_factory = get_session_factory()
    board_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Selector REST Board", owner_id=OWNER_ID))
        spec = _make_spec(board_id)
        db.add(spec)
        await db.commit()
        spec_id = spec.id

    app = FastAPI()
    app.include_router(discovery_api.router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    def _set_user(user_id: str) -> None:
        app.dependency_overrides[discovery_api.require_user] = lambda: user_id

    app.dependency_overrides[get_db] = _override_db
    _set_user(OWNER_ID)

    with TestClient(app) as client:
        yield client, board_id, spec_id, _set_user
    get_default_discovery_selector_cache().clear()


def test_selector_options_lists_specs_with_metadata_only(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={"selector_kind": "spec"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["source"] == "board_db_spec_json"
    assert body["cache_status"] == "miss"
    assert body["global_refs_used"] is False
    assert body["options"][0]["id"] == spec_id
    assert body["options"][0]["label"] == "Selector REST Spec"
    assert "description" not in body["options"][0]
    assert "context" not in body["options"][0]

    hit_response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={"selector_kind": "spec"},
    )
    assert hit_response.status_code == 200, hit_response.text
    assert hit_response.json()["cache_status"] == "hit"


def test_selector_options_lists_spec_child_metadata_only(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": spec_id,
            "child_type": "api_contract",
        },
    )

    assert response.status_code == 200, response.text
    option = response.json()["options"][0]
    assert option["child_type"] == "api_contract"
    assert option["child_ref"] == f"spec:{spec_id}:api_contract:api-1"
    assert option["label"] == "GET /api/v1/discovery/boards/{board_id}/selector-options"
    assert "description" not in option
    assert "request_body" not in option
    assert "response_success" not in option


def test_selector_options_rest_covers_all_eight_child_types_with_safe_fields(
    selector_rest_client,
):
    client, board_id, spec_id, _set_user = selector_rest_client

    seen_child_types: set[str] = set()
    for child_type in SUPPORTED_SPEC_CHILD_TYPES:
        response = client.get(
            f"/api/v1/discovery/boards/{board_id}/selector-options",
            params={
                "selector_kind": "spec_child",
                "spec_id": spec_id,
                "child_type": child_type,
            },
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["source"] == "board_db_spec_json"
        assert body["global_refs_used"] is False
        assert body["options"], f"{child_type} should return a seeded option"
        option = body["options"][0]
        assert set(option) <= SAFE_SELECTOR_OPTION_FIELDS
        assert option["entity_type"] == "spec_child"
        assert option["spec_id"] == spec_id
        assert option["child_type"] == child_type
        assert option["child_ref"].startswith(f"spec:{spec_id}:{child_type}:")
        assert "description" not in option
        assert "context" not in option
        assert "request_body" not in option
        seen_child_types.add(child_type)

    assert seen_child_types == set(SUPPORTED_SPEC_CHILD_TYPES)


def test_selector_options_invalid_spec_dependency_returns_400(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={"selector_kind": "spec", "child_type": "decision"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "invalid_selector_dependency"}}


def test_selector_options_unsupported_child_type_returns_400(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": spec_id,
            "child_type": "test_scenario",
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "unsupported_child_type"}}


def test_selector_options_forbidden_body_does_not_reveal_resource(selector_rest_client):
    client, board_id, spec_id, set_user = selector_rest_client
    set_user(OTHER_USER_ID)

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": spec_id,
            "child_type": "decision",
        },
    )

    assert response.status_code == 403
    assert response.json() == {"detail": {"error": "selector_access_denied"}}
    raw = response.text
    assert spec_id not in raw
    assert "Selector REST Spec" not in raw
    assert "dec-1" not in raw


def test_selector_options_missing_spec_returns_404_for_authorized_board(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": str(uuid.uuid4()),
            "child_type": "decision",
        },
    )

    assert response.status_code == 404
    assert response.json() == {"detail": {"error": "selector_spec_not_found"}}


def test_selector_options_superseded_decisions_require_explicit_opt_in(selector_rest_client):
    client, board_id, spec_id, _set_user = selector_rest_client

    active_response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": spec_id,
            "child_type": "decision",
        },
    )
    assert [item["child_id"] for item in active_response.json()["options"]] == ["dec-1"]

    all_response = client.get(
        f"/api/v1/discovery/boards/{board_id}/selector-options",
        params={
            "selector_kind": "spec_child",
            "spec_id": spec_id,
            "child_type": "decision",
            "status": "all",
            "include_superseded": "true",
        },
    )
    assert [item["child_id"] for item in all_response.json()["options"]] == [
        "dec-1",
        "dec-2",
    ]
