"""REST regressions for Story conversion and rich Refinement creation."""

from __future__ import annotations

import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.refinements import router as refinements_router
from okto_pulse.community.api.stories import router as stories_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from sqlalchemy_test_models import Board, Ideation, IdeationStatus, Story, StoryStatus, Topic


USER_ID = "rest-regression-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


@pytest_asyncio.fixture
async def _client_and_board():
    db_factory = get_session_factory()
    board_id = _id("rest-reg-board")
    topic_id = _id("rest-reg-topic")
    story_id = _id("rest-reg-story")
    ideation_id = _id("rest-reg-ideation")

    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="REST Regression Board",
                owner_id=USER_ID,
                realm_id="local",
            )
        )
        db.add(
            Topic(
                id=topic_id,
                board_id=board_id,
                name="REST regression topic",
                created_by=USER_ID,
            )
        )
        db.add(
            Story(
                id=story_id,
                board_id=board_id,
                topic_id=topic_id,
                title="Capture product usage signals",
                description="As a product lead, I want usage signals to understand adoption.",
                actor="Product lead",
                goal="understand adoption",
                benefit="prioritize improvements",
                labels=["metrics", "kg"],
                status=StoryStatus.READY,
                created_by=USER_ID,
                screen_mockups=[
                    {
                        "id": "story-mockup-rest-1",
                        "title": "Usage overview",
                        "screen_type": "page",
                        "html_content": "<main>Usage overview</main>",
                        "order": 0,
                    }
                ],
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Rich Refinement Parent",
                description="Parent ideation for rich refinement creation.",
                problem_statement="We need a fully described refinement payload to persist cleanly.",
                proposed_approach="Keep all structured fields JSON serializable.",
                status=IdeationStatus.DONE,
                labels=["metrics"],
                screen_mockups=[
                    {
                        "id": "ideation-mockup-rest-1",
                        "title": "Parent mockup",
                        "screen_type": "modal",
                        "html_content": "<section>Parent</section>",
                        "order": 0,
                    }
                ],
                created_by=USER_ID,
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(stories_router, prefix="/api/v1")
    app.include_router(refinements_router, prefix="/api/v1")

    async def _override_uow():
        async with db_factory() as session:
            try:
                yield resolve_unit_of_work_factory().wrap(session)
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    app.dependency_overrides[get_unit_of_work] = _override_uow
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"

    return TestClient(app), {
        "board_id": board_id,
        "story_id": story_id,
        "ideation_id": ideation_id,
    }


def test_story_conversion_endpoint_returns_success_after_persisting(_client_and_board):
    client, ids = _client_and_board

    response = client.post(
        f"/api/v1/boards/{ids['board_id']}/stories/convert-to-ideation",
        json={
            "story_ids": [ids["story_id"]],
            "title": "Product usage and KG reporting",
            "description": "Created from selected Stories.",
            "problem_statement": "Selected story should become an Ideation without returning 500.",
            "proposed_approach": "Preserve Story links and propagated mockups.",
            "mockup_ids": ["story-mockup-rest-1"],
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["ideation"]["title"] == "Product usage and KG reporting"
    assert body["links"][0]["story_id"] == ids["story_id"]
    assert body["propagated_mockups"] == 1


def test_rich_refinement_create_endpoint_serializes_complete_payload(_client_and_board):
    client, ids = _client_and_board

    response = client.post(
        f"/api/v1/ideations/{ids['ideation_id']}/refinements",
        json={
            "ideation_id": ids["ideation_id"],
            "title": "Backend and client refinement for metrics reporting",
            "description": "Detailed refinement with manually supplied UI mockup.",
            "in_scope": [
                "Client usage event taxonomy",
                "Backend ingestion and KG consolidation",
            ],
            "out_of_scope": ["QuickSight dashboards"],
            "analysis": "The payload combines text, lists, labels and mockup objects.",
            "decisions": [
                "Use the existing metrics payload version.",
                "Keep KG inclusion explicit in the event contract.",
            ],
            "labels": ["metrics", "kg", "regression"],
            "screen_mockups": [
                {
                    "id": "refinement-mockup-rest-1",
                    "title": "Metrics opt-in modal",
                    "description": "The user can confirm collection options.",
                    "screen_type": "modal",
                    "html_content": "<section>Metrics opt-in</section>",
                    "annotations": [
                        {
                            "id": "annotation-rest-1",
                            "text": "Save button persists choices.",
                        }
                    ],
                    "order": 0,
                }
            ],
                "architecture_propagation_mode": "none",
                "delivery_context": "brownfield",
            },
    )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["title"] == "Backend and client refinement for metrics reporting"
    assert body["screen_mockups"][0]["id"] == "refinement-mockup-rest-1"
    assert body["screen_mockups"][0]["annotations"][0]["id"] == "annotation-rest-1"
    assert "## Parent Ideation Context" in body["description"]
