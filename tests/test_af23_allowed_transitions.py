"""AF23 - allowed transition read model for UI and MCP actions."""

from __future__ import annotations

from mcp_runtime_testing import register_mcp_test_runtime

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.allowed_transitions import (
    router as allowed_transitions_router,
)
from okto_pulse.core.application.use_cases.allowed_transitions import (
    ALLOWED_TRANSITIONS_DRIFT_METRIC,
    ALLOWED_TRANSITIONS_SOURCE,
    ListAllowedTransitionsCommand,
    ListAllowedTransitionsUseCase,
    allowed_transitions_for_status,
    calculate_allowed_transition_drift,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import (
    CardStatus,
    IdeationStatus,
    RefinementStatus,
    SpecStatus,
)
from okto_pulse.core.domain.sdlc_registry import is_transition_allowed
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.mcp import server as mcp_server
from sqlalchemy_test_models import Board, BoardShare, Ideation, Refinement, Spec
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
            "permissions": [
                "board:read",
                "guidelines.assessments.read",
            ],
        },
    )()


def _status_values(entity_type: str, status: str) -> list[str]:
    return [
        item.to_status for item in allowed_transitions_for_status(entity_type, status)
    ]


@pytest.mark.parametrize(
    ("entity_type", "authority"),
    [
        ("ideation", IdeationService._IDEATION_TRANSITIONS),
        ("refinement", RefinementService._REFINEMENT_TRANSITIONS),
        ("spec", SpecService._SPEC_TRANSITIONS),
    ],
)
def test_read_model_projects_the_same_runtime_transition_authority(
    entity_type, authority
) -> None:
    for from_status, to_statuses in authority.items():
        assert _status_values(entity_type, from_status.value) == [
            to_status.value for to_status in to_statuses
        ]


def test_service_and_read_model_derive_from_registry_not_runtime_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        SpecService,
        "_SPEC_TRANSITIONS",
        {SpecStatus.DRAFT: [SpecStatus.DONE]},
    )

    # The registry is the authority; a consumer monkeypatch cannot create a
    # second lifecycle contract for UI/resources.
    assert _status_values("spec", "draft") == ["review", "cancelled"]


@pytest.mark.parametrize("card_type", ["normal", "test", "bug"])
def test_card_read_model_and_mutation_admission_share_every_typed_edge(
    card_type: str,
) -> None:
    for current in CardStatus:
        projected = {
            item.to_status
            for item in allowed_transitions_for_status(
                "card",
                current.value,
                card_type=card_type,
            )
        }
        admitted = {
            target.value
            for target in CardStatus
            if is_transition_allowed(
                "card",
                current.value,
                target.value,
                card_type=card_type,
            )
        }
        assert projected == admitted

    direct = {
        item.to_status
        for item in allowed_transitions_for_status(
            "card",
            CardStatus.NOT_STARTED.value,
            card_type=card_type,
        )
    }
    assert (CardStatus.IN_PROGRESS.value in direct) is (card_type in {"test", "bug"})


def test_refinement_scope_metadata_is_not_projected_on_ideation() -> None:
    ideation_review = next(
        item
        for item in allowed_transitions_for_status("ideation", "draft")
        if item.to_status == "review"
    )
    refinement_review = next(
        item
        for item in allowed_transitions_for_status("refinement", "draft")
        if item.to_status == "review"
    )

    assert ideation_review.gate == "none"
    assert "in_scope_present" not in ideation_review.preconditions
    assert refinement_review.gate == "refinement_scope"
    assert "in_scope_present" in refinement_review.preconditions


def test_spec_same_edition_reverse_moves_do_not_advertise_content_unlock() -> None:
    validated = {
        item.to_status: item
        for item in allowed_transitions_for_status("spec", "validated")
    }
    in_progress = {
        item.to_status: item
        for item in allowed_transitions_for_status("spec", "in_progress")
    }

    assert validated["approved"].gate == "none"
    assert "reopen" not in validated["approved"].capabilities
    assert "current_validations_cleared" not in validated["approved"].effects
    assert in_progress["validated"].gate == "none"
    assert "reopen" not in in_progress["validated"].capabilities
    assert "current_validations_cleared" not in in_progress["validated"].effects
    assert in_progress["draft"].gate == "unlock_content"
    assert "current_validations_cleared" in in_progress["draft"].effects


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
                decisions=[
                    {
                        "id": "decision-af23",
                        "title": "Exercise transition preview",
                        "status": "active",
                    }
                ],
                skip_decisions_coverage=True,
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
    transitions = {item["to_status"]: item for item in payload["allowed_transitions"]}
    assert transitions["draft"]["gate"] == "unlock_content"
    assert "reopen" in transitions["draft"]["capabilities"]
    assert "current_validations_cleared" in transitions["draft"]["effects"]
    assert transitions["approved"]["gate"] == "none"
    assert "reopen" not in transitions["approved"]["capabilities"]
    assert "current_validations_cleared" not in transitions["approved"]["effects"]
    in_progress = next(
        item
        for item in payload["allowed_transitions"]
        if item["to_status"] == "in_progress"
    )
    assert "spec_evaluation_required" in in_progress["blocked_reason"]


@pytest.mark.asyncio
async def test_entity_scoped_read_model_previews_resource_gate_blocker() -> None:
    board_id = _id("af23-gated-board")
    ideation_id = _id("af23-gated-ideation")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 gated",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={"skip_cognitive_consolidation": True},
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="AF23 gated ideation",
                status=IdeationStatus.EVALUATING,
                created_by=USER,
            )
        )
        await db.commit()

    async with get_session_factory()() as db:
        result = await ListAllowedTransitionsUseCase().execute(
            ListAllowedTransitionsCommand(
                board_id,
                "ideation",
                entity_id=ideation_id,
            ),
            actor=ActorContext(
                USER,
                "rest",
                board_id=board_id,
                realm_id=LOCAL_REALM_ID,
            ),
            uow=_wrap_uow(db),
        )

    done = next(
        transition
        for transition in result.read_model.allowed_transitions
        if transition.to_status == "done"
    )
    assert done.blocked_reason is not None
    assert "resource_gate_missing_resources" in done.blocked_reason


@pytest.mark.asyncio
async def test_rest_endpoint_and_mcp_tool_return_the_same_contract(
    client: TestClient,
) -> None:
    board_id, _ideation_id, _refinement_id, spec_id = await _seed_fixture()

    rest = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "spec", "entity_id": spec_id},
    )
    assert rest.status_code == 200, rest.text
    rest_payload = rest.json()
    assert rest_payload["source"] == ALLOWED_TRANSITIONS_SOURCE

    register_mcp_test_runtime(get_session_factory())
    with (
        patch.object(
            mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_get_allowed_transitions")
        raw = await tool.fn(board_id=board_id, entity_type="spec", entity_id=spec_id)
    mcp_payload = json.loads(raw)

    assert mcp_payload == rest_payload


@pytest.mark.asyncio
async def test_rest_and_mcp_current_status_policy_subject_required_parity(
    client: TestClient,
) -> None:
    board_id = _id("af23-policy-subject-board")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 policy subject",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        await db.commit()

    rest = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "spec", "current_status": "approved"},
    )
    assert rest.status_code == 200, rest.text
    rest_payload = rest.json()

    register_mcp_test_runtime(get_session_factory())
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=_ctx(board_id)),
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_get_allowed_transitions")
        raw = await tool.fn(
            board_id=board_id,
            entity_type="spec",
            current_status="approved",
        )
    mcp_payload = json.loads(raw)

    assert mcp_payload == rest_payload
    validated = next(
        item
        for item in rest_payload["allowed_transitions"]
        if item["to_status"] == "validated"
    )
    assert validated["policy_compliance"] is True
    assert validated["policy_compliance_decision"]["state"] == "policy_subject_required"
    assert validated["policy_compliance_decision"]["allowed"] is None


@pytest.mark.asyncio
async def test_rest_viewer_share_can_read_allowed_transitions(
    client: TestClient,
) -> None:
    board_id = _id("af23-shared-board")
    spec_id = _id("af23-shared-spec")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 shared board",
                owner_id="af23-board-owner",
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        db.add(
            BoardShare(
                board_id=board_id,
                user_id=USER,
                realm_id=LOCAL_REALM_ID,
                permission="viewer",
                shared_by="af23-board-owner",
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF23 shared spec",
                status=SpecStatus.APPROVED,
                created_by="af23-board-owner",
                functional_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        await db.commit()

    response = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "spec", "entity_id": spec_id},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["current_status"] == "approved"


@pytest.mark.asyncio
async def test_policy_preview_is_redacted_without_assessment_leaf_and_full_with_it() -> (
    None
):
    board_id = _id("af23-redaction-board")
    spec_id = _id("af23-redaction-spec")
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="AF23 redaction",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
                settings={},
            )
        )
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="AF23 redaction spec",
                status=SpecStatus.APPROVED,
                created_by=USER,
                functional_requirements=[],
                acceptance_criteria=[],
                business_rules=[],
                api_contracts=[],
                technical_requirements=[],
            )
        )
        await db.commit()

    async def read(permissions: list[str]):
        async with get_session_factory()() as db:
            return await ListAllowedTransitionsUseCase().execute(
                ListAllowedTransitionsCommand(
                    board_id,
                    "spec",
                    entity_id=spec_id,
                ),
                actor=ActorContext(
                    USER,
                    "mcp",
                    board_id=board_id,
                    realm_id=LOCAL_REALM_ID,
                    permissions=permissions,
                ),
                uow=_wrap_uow(db),
            )

    limited = await read(["board:read", "spec.entity.read", "spec.validation.read"])
    full = await read(
        [
            "board:read",
            "spec.entity.read",
            "spec.validation.read",
            "guidelines.assessments.read",
        ]
    )
    limited_row = next(
        item
        for item in limited.read_model.to_dict()["allowed_transitions"]
        if item["policy_compliance"]
    )
    full_row = next(
        item
        for item in full.read_model.to_dict()["allowed_transitions"]
        if item["policy_compliance"]
    )

    assert limited_row["policy_compliance_decision"]["projection"] == "redacted"
    assert set(limited_row["policy_compliance_decision"]) == {
        "projection",
        "state",
        "allowed",
        "policy_compliance_required",
    }
    assert full_row["policy_compliance_decision"]["projection"] == "full"
    assert "decision_digest" in full_row["policy_compliance_decision"]
    assert "binding_decisions" in full_row["policy_compliance_decision"]


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
    with (
        patch.object(
            mcp_server, "_get_agent_ctx", AsyncMock(return_value=_ctx(board_id))
        ),
        patch.object(mcp_server, "check_permission", return_value=None),
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
            await SpecService(db).move_spec(
                spec_id, USER, SpecMove(status=SpecStatus.DONE)
            )


def test_rest_endpoint_rejects_invalid_type_or_missing_status(
    client: TestClient,
) -> None:
    board_id = _id("af23-board")
    response = client.get(
        f"{PREFIX}/boards/{board_id}/allowed-transitions",
        params={"entity_type": "task", "current_status": "draft"},
    )
    assert response.status_code == 400
    assert "Invalid entity_type" in response.json()["detail"]
