"""Card 53b75084 — dedicated skip write path, MCP mirror and audit log.

Covers TR5/TR14/TR15, FR5/FR14/FR15, BR3/BR7 and scenarios ts_6eeb8ad3,
ts_2b024c93 (skip persistence while evaluating, draft-guard preservation,
auditable activity entry, REST/MCP shared service path).
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select

from okto_pulse.community.api.ideations import router as ideations_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Ideation,
    IdeationStatus,
)
from okto_pulse.core.models.schemas import IdeationUpdate
from okto_pulse.core.services.main import IdeationService

USER_ID = "ambiguity-skip-user"
SKIP_ACTION = "ideation.ambiguity_gate_skip_updated"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


async def _seed_ideation(
    db_factory,
    *,
    status: IdeationStatus = IdeationStatus.EVALUATING,
    archived: bool = False,
    gate_enabled: bool = True,
) -> tuple[str, str]:
    board_id = _id("board")
    ideation_id = _id("idea")
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Ambiguity Skip",
                owner_id=USER_ID,
                settings={
                    "require_ideation_ambiguity_gate": gate_enabled,
                    "max_ideation_ambiguity": 3,
                },
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title="Skip path ideation",
                created_by=USER_ID,
                status=status,
                archived=archived,
            )
        )
        await db.commit()
    return board_id, ideation_id


async def _skip_logs(db, ideation_id: str) -> list[ActivityLog]:
    rows = (
        await db.execute(
            select(ActivityLog).where(ActivityLog.action == SKIP_ACTION)
        )
    ).scalars().all()
    return [r for r in rows if (r.details or {}).get("ideation_id") == ideation_id]


# ---------------------------------------------------------------------------
# Service-level (the single path REST and MCP both call)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_persists_while_evaluating_and_writes_audit(db_factory):
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    async with db_factory() as db:
        service = IdeationService(db)
        result = await service.set_ambiguity_gate_skip(
            ideation_id, USER_ID, True, source="rest"
        )
        await db.commit()
        assert result is not None
        assert result.skip_ambiguity_gate is True
        # status is untouched — the skip write path is not a status transition.
        assert result.status == IdeationStatus.EVALUATING

    async with db_factory() as db:
        reloaded = await IdeationService(db).get_ideation(ideation_id)
        assert reloaded.skip_ambiguity_gate is True
        assert reloaded.status == IdeationStatus.EVALUATING

        logs = await _skip_logs(db, ideation_id)
        assert len(logs) == 1
        details = logs[0].details
        assert details["source"] == "rest"
        assert details["old_value"] is False
        assert details["new_value"] is True
        assert logs[0].actor_id == USER_ID


@pytest.mark.asyncio
async def test_mcp_source_is_audited_distinctly(db_factory):
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    async with db_factory() as db:
        await IdeationService(db).set_ambiguity_gate_skip(
            ideation_id, USER_ID, True, source="mcp"
        )
        await db.commit()

    async with db_factory() as db:
        logs = await _skip_logs(db, ideation_id)
        assert logs[0].details["source"] == "mcp"
        assert logs[0].details["new_value"] is True


@pytest.mark.asyncio
async def test_skip_rejects_archived_ideation(db_factory):
    _, ideation_id = await _seed_ideation(db_factory, archived=True)

    async with db_factory() as db:
        service = IdeationService(db)
        with pytest.raises(
            ValueError, match="Cannot update ambiguity gate skip for archived ideation"
        ):
            await service.set_ambiguity_gate_skip(ideation_id, USER_ID, True, source="rest")


@pytest.mark.asyncio
async def test_skip_missing_ideation_returns_none(db_factory):
    async with db_factory() as db:
        result = await IdeationService(db).set_ambiguity_gate_skip(
            _id("missing"), USER_ID, True, source="rest"
        )
        assert result is None


@pytest.mark.asyncio
async def test_generic_update_ideation_still_rejects_non_draft_edits(db_factory):
    """Preservation (R1): the dedicated skip path must NOT relax the
    draft-only guard on the generic update_ideation flow."""
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    async with db_factory() as db:
        service = IdeationService(db)
        with pytest.raises(ValueError, match="Cannot edit ideation in 'evaluating' status"):
            await service.update_ideation(
                ideation_id, USER_ID, IdeationUpdate(description="sneaky non-draft edit")
            )


# ---------------------------------------------------------------------------
# REST endpoint
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def _client():
    db_factory = get_session_factory()
    app = FastAPI()
    app.include_router(ideations_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    return TestClient(app), db_factory


@pytest.mark.asyncio
async def test_rest_skip_endpoint_persists_in_evaluating(_client):
    client, db_factory = _client
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    resp = client.patch(
        f"/api/v1/ideations/{ideation_id}/ambiguity-gate-skip",
        json={"skip_ambiguity_gate": True},
    )
    assert resp.status_code == 200
    assert resp.json()["skip_ambiguity_gate"] is True


@pytest.mark.asyncio
async def test_rest_skip_endpoint_rejects_extra_fields(_client):
    client, db_factory = _client
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    resp = client.patch(
        f"/api/v1/ideations/{ideation_id}/ambiguity-gate-skip",
        json={"skip_ambiguity_gate": True, "description": "sneaky"},
    )
    assert resp.status_code == 422  # extra='forbid' blocks smuggled edits


@pytest.mark.asyncio
async def test_rest_skip_endpoint_missing_field_is_422(_client):
    client, db_factory = _client
    _, ideation_id = await _seed_ideation(db_factory, status=IdeationStatus.EVALUATING)

    resp = client.patch(
        f"/api/v1/ideations/{ideation_id}/ambiguity-gate-skip", json={}
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_rest_skip_endpoint_archived_is_400(_client):
    client, db_factory = _client
    _, ideation_id = await _seed_ideation(db_factory, archived=True)

    resp = client.patch(
        f"/api/v1/ideations/{ideation_id}/ambiguity-gate-skip",
        json={"skip_ambiguity_gate": True},
    )
    assert resp.status_code == 400
    assert "archived" in resp.json()["detail"].lower()
