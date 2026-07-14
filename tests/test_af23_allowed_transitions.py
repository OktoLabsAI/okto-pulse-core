"""AF23 - allowed transition read model for UI and MCP actions."""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.allowed_transitions import router as allowed_transitions_router
from okto_pulse.core.application.use_cases.allowed_transitions import (
    ALLOWED_TRANSITIONS_DRIFT_METRIC,
    ALLOWED_TRANSITIONS_SOURCE,
    ListAllowedTransitionsCommand,
    ListAllowedTransitionsUseCase,
    allowed_transitions_for_status,
    calculate_allowed_transition_drift,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import IdeationStatus, RefinementStatus, SpecStatus
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, Ideation, Refinement, Spec
from okto_pulse.core.models.schemas import SpecMove
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services import IdeationService, RefinementService, SpecService

USER = "af23-allowed-transitions"
PREFIX = "/api/v1"


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(allowed_transitions_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wrap_uow(db):
    return resolve_unit_of_work_factory().wrap(db)


def _ctx(board_id: str):
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER,
            "agent_name": USER,
            "board_id": board_id,
            "realm_id": LOCAL_REALM_ID,
            "permissions": ["board:read"],
        },
    )()


def _status_values(entity_type: str, status: str) -> list[str]:
    return [item.to_status for item in allowed_transitions_for_status(entity_type, status)]


@pytest.mark.parametrize(
    ("entity_type", "authority"),
    [
        ("ideation", IdeationService._IDEATION_TRANSITIONS),
        ("refinement", RefinementService._REFINEMENT_TRANSITIONS),
        ("spec", SpecService._SPEC_TRANSITIONS),
    ],
)
def test_read_model_projects_the_same_runtime_transition_authority(entity_type, authority) -> None:
    for from_status, to_statuses in authority.items():
        assert _status_values(entity_type, from_status.value) == [
            to_status.value for to_status in to_statuses
        ]


def test_read_model_is_not_a_parallel_static_map(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        SpecService,
        "_SPEC_TRANSITIONS",
        {SpecStatus.DRAFT: [SpecStatus.DONE]},
    )

    assert _status_values("spec", "draft") == ["done"]


def test_docs_only_forward_subset_reports_reverse_and_unlock_drift() -> None:
    docs_only_subset = {
        "ideation": {
            "draft": ["review", "cancelled"],
            "review": ["approved", "cancelled"],
            "approved": ["evaluating", "cancelled"],
            "evaluating": ["done", "cancelled"],
            "done": [],
            "cancelled": [],
        },
        "refinement": {
            "draft": ["review", "cancelled"],
            "review": ["approved", "cancelled"],
            "approved": ["done", "cancelled"],
            "done": [],
            "cancelled": [],
        },
        "spec": {
            "draft": ["review", "cancelled"],
            "review": ["approved", "cancelled"],
            "approved": ["validated", "cancelled"],
            "validated": ["in_progress", "cancelled"],
            "in_progress": ["done", "cancelled"],
            "done": [],
            "cancelled": [],
        },
    }

    report = calculate_allowed_transition_drift(docs_only_subset)

    assert report.metric_name == ALLOWED_TRANSITIONS_DRIFT_METRIC
    assert report.drift_total > 0
    assert ("ideation", "review", "draft") in report.missing_edges
    assert ("refinement", "done", "draft") in report.missing_edges
    assert ("spec", "approved", "draft") in report.missing_edges
    assert ("spec", "validated", "draft") in report.missing_edges
    assert ("spec", "done", "draft") in report.missing_edges


async def _seed_fixture() -> tuple[str, str, str, str]:
    board_id = _id("af23-board")
    ideation_id = _id("af23-ideation")
    refinement_id = _id("af23-refinement")
    spec_id = _id("af23-spec")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="AF23 ideation",
                status=IdeationStatus.REVIEW,
                created_by=USER,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=ideation_id,
                title="AF23 refinement",
                status=RefinementStatus.DONE,
                created_by=USER,
                in_scope=["scope"],
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                ideation_id=ideation_id,
                refinement_id=refinement_id,
                title="AF23 spec",
                status=SpecStatus.VALIDATED,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        await db.commit()
    return board_id, ideation_id, refinement_id, spec_id


@pytest.mark.asyncio
async def test_use_case_resolves_entity_status_and_exposes_unlock_edges() -> None:
    board_id, _ideation_id, _refinement_id, spec_id = await _seed_fixture()

    async with get_session_factory()() as db:
        result = await ListAllowedTransitionsUseCase().execute(
            ListAllowedTransitionsCommand(board_id, "spec", entity_id=spec_id),
            actor=ActorContext(
                USER, "rest", board_id=board_id, realm_id=LOCAL_REALM_ID
            ),
            uow=_wrap_uow(db),
        )

    payload = result.read_model.to_dict()
    assert payload["source"] == ALLOWED_TRANSITIONS_SOURCE
    assert payload["current_status"] == "validated"
    assert [item["to_status"] for item in payload["allowed_transitions"]] == [
        "approved",
        "in_progress",
        "draft",
        "cancelled",
    ]
    assert any(item["gate"] == "unlock_content" for item in payload["allowed_transitions"])


@pytest.mark.asyncio
async def test_rest_endpoint_and_mcp_tool_return_the_same_contract(client: TestClient) -> None:
    board_id, _ideation_id, _refinement_id, spec_id = await _seed_fixture()

    rest = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "spec", "entity_id": spec_id},
    )
    assert rest.status_code == 200, rest.text
    rest_payload = rest.json()
    assert rest_payload["source"] == ALLOWED_TRANSITIONS_SOURCE

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))), patch.object(
        mcp_server, "check_permission", return_value=None
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_get_allowed_transitions")
        raw = await tool.fn(board_id=board_id, entity_type="spec", entity_id=spec_id)
    mcp_payload = json.loads(raw)

    assert mcp_payload == rest_payload


@pytest.mark.asyncio
async def test_mcp_board_context_allows_authorized_non_owner_agent() -> None:
    board_id = _id("af23-board")
    spec_id = _id("af23-spec")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 MCP board",
                owner_id="board-owner",
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF23 MCP spec",
                status=SpecStatus.REVIEW,
                created_by="board-owner",
                functional_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        await db.commit()

    register_mcp_test_runtime(get_session_factory())
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))), patch.object(
        mcp_server, "check_permission", return_value=None
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_get_allowed_transitions")
        raw = await tool.fn(board_id=board_id, entity_type="spec", entity_id=spec_id)

    payload = json.loads(raw)
    assert payload["source"] == ALLOWED_TRANSITIONS_SOURCE
    assert [item["to_status"] for item in payload["allowed_transitions"]] == [
        "draft",
        "approved",
        "cancelled",
    ]


@pytest.mark.asyncio
async def test_read_model_does_not_enforce_invalid_backend_moves() -> None:
    board_id = _id("af23-board")
    spec_id = _id("af23-spec")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 enforcement",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF23 enforcement spec",
                status=SpecStatus.DRAFT,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        await db.commit()

    async with get_session_factory()() as db:
        result = await ListAllowedTransitionsUseCase().execute(
            ListAllowedTransitionsCommand(board_id, "spec", entity_id=spec_id),
            actor=ActorContext(
                USER, "rest", board_id=board_id, realm_id=LOCAL_REALM_ID
            ),
            uow=_wrap_uow(db),
        )
        assert "done" not in [
            item.to_status for item in result.read_model.allowed_transitions
        ]
        with pytest.raises(ValueError, match="Cannot move spec from 'draft' to 'done'"):
            await SpecService(db).move_spec(spec_id, USER, SpecMove(status=SpecStatus.DONE))


def test_rest_endpoint_rejects_invalid_type_or_missing_status(client: TestClient) -> None:
    board_id = _id("af23-board")
    response = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "task", "current_status": "draft"},
    )
    assert response.status_code == 400
    assert "Invalid entity_type" in response.json()["detail"]
