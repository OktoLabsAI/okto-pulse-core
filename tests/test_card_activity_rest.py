"""REST coverage for readable card activity DTOs."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardStatus,
    CardType,
    Spec,
    SpecStatus,
)


USER_ID = "card-activity-rest-user"


@pytest_asyncio.fixture
async def activity_client(db_factory):
    board_id = f"board-{uuid.uuid4()}"
    spec_id = str(uuid.uuid4())
    card_id = str(uuid.uuid4())
    async with db_factory() as db:
        db.add(Board(id=board_id, name="Card Activity REST", owner_id=USER_ID))
        db.add(
            Spec(
                id=spec_id,
                board_id=board_id,
                title="Readable Activity",
                status=SpecStatus.APPROVED,
                created_by=USER_ID,
                functional_requirements=["FR1"],
                acceptance_criteria=["AC1"],
                test_scenarios=[],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                spec_id=spec_id,
                title="Activity Card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                created_by=USER_ID,
            )
        )
        db.add(
            ActivityLog(
                id=str(uuid.uuid4()),
                board_id=board_id,
                card_id=card_id,
                action="structured_entity_updated",
                actor_type="agent",
                actor_id="agent-1",
                actor_name="Agent",
                details={
                    "trigger": "structured_entity_updated",
                    "entity_type": "functional_requirement",
                    "entity_id": "fr_abcdef123456",
                    "operation": "updated",
                    "field": "description",
                    "password": "do-not-return",
                    "after": {"nested": "value"},
                },
                created_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(cards_router, prefix="/api/v1/cards")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    return TestClient(app), card_id


def test_card_activity_rest_returns_explicit_readable_dto(activity_client):
    client, card_id = activity_client
    resp = client.get(f"/api/v1/cards/{card_id}/activity")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body) == 1

    row = body[0]
    assert row["summary"].startswith("structured_entity updated")
    assert row["trigger"] == "structured_entity_updated"
    assert row["details"]["password"] == "[redacted]"
    assert row["details"]["after"] == {"nested": "value"}
    assert "[object Object]" not in row["summary"]
    assert "{'nested'" not in row["summary"]
