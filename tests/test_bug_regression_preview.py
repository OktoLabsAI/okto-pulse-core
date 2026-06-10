"""REST/MCP preview coverage for bug regression scenario reuse."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.cards import router as cards_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models.db import Board, BugSeverity, Card, CardStatus, CardType, Spec, SpecStatus
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.services.bug_regression_observability import (
    METRIC_RESOLVE_LATENCY_MS,
    METRIC_RESOLVE_TOTAL,
    METRIC_SEMANTIC_GAP_TOTAL,
    METRIC_WORKFLOW_REMEDIATION_TOTAL,
    assert_bug_regression_payload_is_safe,
    get_bug_regression_metric_samples,
    reset_bug_regression_observability_for_tests,
)
from okto_pulse.core.services.bug_regression_preview import BugRegressionScenarioPreviewService


USER_ID = "bug-regression-preview-agent"


@pytest.fixture(autouse=True)
def _reset_bug_regression_metrics():
    reset_bug_regression_observability_for_tests()
    yield
    reset_bug_regression_observability_for_tests()


@pytest_asyncio.fixture
async def bug_preview_seed(db_factory):
    board_id = f"preview-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"preview-spec-{uuid.uuid4().hex[:8]}"
    foreign_spec_id = f"foreign-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    affected_id = f"affected-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Bug Preview Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Preview spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[
                    {
                        "id": "ts-origin-direct",
                        "title": "Origin direct scenario",
                        "linked_criteria": [0],
                        "status": "passed",
                    },
                    {
                        "id": "ts-affected-direct",
                        "title": "Affected direct scenario",
                        "linked_criteria": [0],
                        "status": "passed",
                    },
                    {
                        "id": "ts-affected-linked",
                        "title": "Affected linked scenario",
                        "linked_criteria": [0],
                        "linked_task_ids": [affected_id],
                        "status": "passed",
                    },
                    {
                        "id": "ts-unrelated",
                        "title": "Similar looking but unrelated",
                        "linked_criteria": [0],
                        "linked_task_ids": [],
                        "status": "passed",
                    },
                    {
                        "id": "ts-deleted",
                        "title": "Deleted candidate",
                        "linked_criteria": [0],
                        "status": "deleted",
                    },
                ],
                business_rules=[],
                api_contracts=[],
                current_validation_id="val-preview",
                validations=[{"id": "val-preview", "outcome": "success"}],
            )
        )
        db.add(
            Spec(
                id=foreign_spec_id,
                board_id=board_id,
                title="Foreign spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[
                    {
                        "id": "ts-foreign",
                        "title": "Foreign regression scenario",
                        "linked_criteria": [0],
                        "status": "passed",
                    }
                ],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=origin_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Origin task",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                test_scenario_ids=["ts-origin-direct"],
                created_by=USER_ID,
                created_at=now,
            )
        )
        db.add(
            Card(
                id=affected_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Affected task",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                test_scenario_ids=["ts-affected-direct"],
                created_by=USER_ID,
                created_at=now,
            )
        )
        db.add(
            Card(
                id=bug_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Bug spanning affected task",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.BUG,
                origin_task_id=origin_id,
                severity=BugSeverity.MAJOR,
                expected_behavior="request succeeds",
                observed_behavior="request fails",
                created_by=USER_ID,
                created_at=now,
            )
        )
        await db.commit()

    return {
        "board_id": board_id,
        "spec_id": spec_id,
        "foreign_spec_id": foreign_spec_id,
        "origin_id": origin_id,
        "affected_id": affected_id,
        "bug_id": bug_id,
    }


@pytest_asyncio.fixture
async def bug_semantic_gap_seed(db_factory):
    board_id = f"gap-board-{uuid.uuid4().hex[:8]}"
    spec_id = f"gap-spec-{uuid.uuid4().hex[:8]}"
    origin_id = f"origin-{uuid.uuid4().hex[:8]}"
    bug_id = f"bug-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)

    async with db_factory() as db:
        db.add(Board(id=board_id, name="Bug Gap Board", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Gap spec",
                status=SpecStatus.IN_PROGRESS,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[
                    {
                        "id": "ts-unrelated-gap",
                        "title": "Unrelated gap scenario",
                        "linked_criteria": [0],
                        "status": "passed",
                    }
                ],
                business_rules=[],
                api_contracts=[],
                current_validation_id="val-gap",
                validations=[{"id": "val-gap", "outcome": "success"}],
            )
        )
        db.add(
            Card(
                id=origin_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Origin without regression scenario",
                status=CardStatus.DONE,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
                created_at=now,
            )
        )
        db.add(
            Card(
                id=bug_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Bug with semantic gap",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.BUG,
                origin_task_id=origin_id,
                severity=BugSeverity.MAJOR,
                expected_behavior="request succeeds",
                observed_behavior="request fails",
                created_by=USER_ID,
                created_at=now,
            )
        )
        await db.commit()

    return {"board_id": board_id, "spec_id": spec_id, "bug_id": bug_id}


async def test_preview_service_resolves_mixed_origin_affected_and_rejections(
    db_factory,
    bug_preview_seed,
):
    async with db_factory() as db:
        payload = await BugRegressionScenarioPreviewService(db).resolve(
            board_id=bug_preview_seed["board_id"],
            bug_id=bug_preview_seed["bug_id"],
            affected_task_ids=[bug_preview_seed["affected_id"]],
            candidate_scenario_ids=[
                "ts-origin-direct",
                "ts-affected-linked",
                "ts-unrelated",
                "ts-foreign",
                "ts-missing",
                "ts-deleted",
            ],
        )

    eligible = {row["scenario_id"]: row for row in payload["eligible_scenarios"]}
    rejected = {row["scenario_id"]: row for row in payload["rejected_scenarios"]}

    assert payload["semantic_gap_required"] is False
    assert payload["spec_mutation_required"] is False
    assert payload["next_action"] == "create_regression_test_card"
    assert payload["remediation"]["reason_code"] == "origin_task_direct"
    assert payload["remediation"]["remediation_path"] == "path_a_reuse_existing_scenario"
    assert payload["remediation"]["next_action"] == "create_regression_test_card"
    assert payload["remediation"]["actions"][0]["primary"] is True
    assert eligible["ts-origin-direct"]["reason"] == "origin_task_direct"
    assert eligible["ts-origin-direct"]["source_task_id"] == bug_preview_seed["origin_id"]
    assert eligible["ts-affected-linked"]["reason"] == "affected_task_linked_scenario"
    assert eligible["ts-affected-linked"]["source_task_id"] == bug_preview_seed["affected_id"]
    assert rejected["ts-unrelated"]["reason"] == "unrelated_scenario"
    assert rejected["ts-foreign"]["reason"] == "cross_spec_scenario"
    assert rejected["ts-foreign"]["detail"] == bug_preview_seed["foreign_spec_id"]
    assert rejected["ts-missing"]["reason"] == "scenario_not_found"
    assert rejected["ts-deleted"]["reason"] == "deleted_scenario"


async def test_preview_service_semantic_gap_does_not_mutate_locked_spec(
    db_factory,
    bug_semantic_gap_seed,
):
    async with db_factory() as db:
        before = await db.get(Spec, bug_semantic_gap_seed["spec_id"])
        before_scenarios = list(before.test_scenarios or [])
        before_validation_id = before.current_validation_id

        payload = await BugRegressionScenarioPreviewService(db).resolve(
            board_id=bug_semantic_gap_seed["board_id"],
            bug_id=bug_semantic_gap_seed["bug_id"],
        )
        after = await db.get(Spec, bug_semantic_gap_seed["spec_id"])

    assert payload["eligible_scenarios"] == []
    assert payload["rejected_scenarios"] == []
    assert payload["semantic_gap_required"] is True
    assert payload["spec_mutation_required"] is True
    assert payload["next_action"] == "escalate_semantic_gap"
    assert payload["remediation"]["reason_code"] == "no_eligible_scenarios"
    assert payload["remediation"]["remediation_path"] == "path_b_semantic_gap"
    assert payload["remediation"]["semantic_gap_required"] is True
    assert after.test_scenarios == before_scenarios
    assert after.current_validation_id == before_validation_id

    samples = get_bug_regression_metric_samples()
    metric_names = {sample["metric_name"] for sample in samples}
    assert {
        METRIC_RESOLVE_TOTAL,
        METRIC_RESOLVE_LATENCY_MS,
        METRIC_SEMANTIC_GAP_TOTAL,
        METRIC_WORKFLOW_REMEDIATION_TOTAL,
    } <= metric_names
    for sample in samples:
        assert_bug_regression_payload_is_safe(sample["labels"])
    semantic_gap = next(
        sample for sample in samples if sample["metric_name"] == METRIC_SEMANTIC_GAP_TOTAL
    )
    assert semantic_gap["labels"]["outcome"] == "semantic_gap"
    assert semantic_gap["labels"]["reason_code"] == "no_eligible_scenarios"
    assert semantic_gap["labels"]["next_action"] == "escalate_semantic_gap"
    remediation_metric = next(
        sample
        for sample in samples
        if sample["metric_name"] == METRIC_WORKFLOW_REMEDIATION_TOTAL
    )
    assert remediation_metric["labels"]["surface"] == "preview"
    assert remediation_metric["labels"]["remediation_path"] == "path_b_semantic_gap"


@pytest_asyncio.fixture
async def bug_preview_client(db_factory, bug_preview_seed):
    app = FastAPI()
    app.include_router(cards_router, prefix="/api/v1/cards")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    return TestClient(app), bug_preview_seed


def test_rest_preview_route_uses_same_candidate_payload(bug_preview_client):
    client, ids = bug_preview_client
    response = client.get(
        f"/api/v1/cards/{ids['bug_id']}/regression-scenario-candidates",
        params=[
            ("board_id", ids["board_id"]),
            ("affected_task_ids", ids["affected_id"]),
            ("candidate_scenario_ids", "ts-affected-direct"),
            ("candidate_scenario_ids", "ts-unrelated"),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["affected_task_ids"] == [ids["affected_id"]]
    assert payload["eligible_scenarios"] == [
        {
            "scenario_id": "ts-affected-direct",
            "title": "Affected direct scenario",
            "reason": "affected_task_direct",
            "source_task_id": ids["affected_id"],
        }
    ]
    assert payload["rejected_scenarios"] == [
        {"scenario_id": "ts-unrelated", "reason": "unrelated_scenario", "detail": None}
    ]
    assert payload["remediation"]["next_action"] == "create_regression_test_card"
    assert payload["remediation"]["eligible_scenarios_count"] == 1


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "bug-preview-agent",
            "permissions": ["board.entity.read"],
        },
    )()


@pytest.mark.asyncio
async def test_mcp_preview_tool_matches_rest_shape(db_factory, bug_preview_seed):
    mcp_server.register_session_factory(db_factory)
    with patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())), patch.object(
        mcp_server, "check_permission", return_value=None
    ):
        tool = await mcp_server.mcp.get_tool("okto_pulse_resolve_bug_regression_scenarios")
        payload = json.loads(
            await tool.fn(
                board_id=bug_preview_seed["board_id"],
                bug_id=bug_preview_seed["bug_id"],
                affected_task_ids=[bug_preview_seed["affected_id"]],
                candidate_scenario_ids="ts-affected-linked|ts-missing",
            )
        )

    assert payload["eligible_scenarios"] == [
        {
            "scenario_id": "ts-affected-linked",
            "title": "Affected linked scenario",
            "reason": "affected_task_linked_scenario",
            "source_task_id": bug_preview_seed["affected_id"],
        }
    ]
    assert payload["rejected_scenarios"] == [
        {"scenario_id": "ts-missing", "reason": "scenario_not_found", "detail": None}
    ]
    assert payload["next_action"] == "create_regression_test_card"
    assert payload["remediation"]["remediation_path"] == "path_a_reuse_existing_scenario"
    assert payload["remediation"]["actions"][0]["action_id"] == "create_regression_test_card"
