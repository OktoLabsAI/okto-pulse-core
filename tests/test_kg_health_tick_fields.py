"""Behavioral tests for FR5/FR6 — boards_processed_in_last_tick and
boards_failed_in_last_tick fields (spec R2b, IMPL-3).

Test scenarios:
- AC5 (ts_e7f794df): KGTickRun with boards_processed=5, boards_failed=1 →
  get_kg_health returns the correct counters.
- AC6 (ts_b5a676d2): GET /kg/health endpoint JSON contains both new fields
  without removing any existing required fields.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api.kg_health import router as kg_health_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db
from sqlalchemy_test_models import Board, KGTickRun
from okto_pulse.core.services.kg_health_service import get_kg_health


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_USER_ID = "user-tick-fields-test"


@pytest_asyncio.fixture
async def tick_fields_board(db_factory):
    """Create a Board row and clean up KGTickRun rows for isolation."""
    board_id = f"board-tick-fields-{uuid.uuid4().hex[:8]}"
    async with db_factory() as session:
        session.add(Board(id=board_id, name="tick-fields-test", owner_id=_USER_ID))
        await session.commit()

    yield board_id

    async with db_factory() as session:
        await session.execute(
            KGTickRun.__table__.delete().where(
                KGTickRun.tick_id.isnot(None)
            )
        )
        await session.commit()


# ---------------------------------------------------------------------------
# AC5 (ts_e7f794df): service layer returns correct board counters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ac5_boards_processed_and_failed_from_tick_run(
    db_factory, tick_fields_board
):
    """AC5 (ts_e7f794df) — Given a KGTickRun with boards_processed=5 and
    boards_failed=1, get_kg_health (called for any board) returns
    boards_processed_in_last_tick==5 and boards_failed_in_last_tick==1.

    The query is global (no board_id filter — BR6), so the tick run
    inserted here is picked up regardless of which board_id is passed to
    get_kg_health.
    """
    now = datetime.now(timezone.utc)
    tick_id = f"tick-ac5-{uuid.uuid4().hex}"

    async with db_factory() as session:
        session.add(
            KGTickRun(
                tick_id=tick_id,
                started_at=now,
                completed_at=now,
                nodes_recomputed=12,
                boards_processed=5,
                boards_failed=1,
            )
        )
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(tick_fields_board, session)

    assert result["boards_processed_in_last_tick"] == 5, (
        f"Expected 5, got {result['boards_processed_in_last_tick']}"
    )
    assert result["boards_failed_in_last_tick"] == 1, (
        f"Expected 1, got {result['boards_failed_in_last_tick']}"
    )


@pytest.mark.asyncio
async def test_ac5_zero_defaults_when_no_tick_run(db_factory, tick_fields_board):
    """AC5 extension (BR5) — When no completed KGTickRun exists, both counters
    default to 0. This guards the case before any tick has run.
    """
    # Ensure no tick runs exist (fixture already clears at teardown, but the
    # session-scoped DB may have rows from other tests — delete explicitly).
    async with db_factory() as session:
        await session.execute(KGTickRun.__table__.delete())
        await session.commit()

    async with db_factory() as session:
        result = await get_kg_health(tick_fields_board, session)

    assert result["boards_processed_in_last_tick"] == 0
    assert result["boards_failed_in_last_tick"] == 0


# ---------------------------------------------------------------------------
# AC6 (ts_b5a676d2): REST endpoint JSON shape
# ---------------------------------------------------------------------------

# Required existing fields per contract api_3ed9037f + legacy dashboard surface.
_REQUIRED_EXISTING_FIELDS = {
    # KG-01 REST contract
    "board_id",
    "graph_state",
    "discovery_state",
    "overall_state",
    "metric_status",
    "classification_reason",
    "correlation_id",
    "recent_events",
    "checked_at",
    # Tick / dashboard fields asserted by the spec
    "last_tick_status",
    "last_decay_tick_at",
    "nodes_recomputed_in_last_tick",
    "tick_in_progress",
}


@pytest_asyncio.fixture
def kg_health_test_client(db_factory, tick_fields_board):
    """FastAPI TestClient wired to the kg_health router with auth bypassed."""
    app = FastAPI()
    app.include_router(kg_health_router, prefix="/api/v1")

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: _USER_ID

    return TestClient(app), tick_fields_board


def test_ac6_rest_endpoint_json_contains_new_fields_without_removing_existing(
    kg_health_test_client,
):
    """AC6 (ts_b5a676d2) — GET /api/v1/kg/health returns JSON that:

    1. Contains boards_processed_in_last_tick and boards_failed_in_last_tick.
    2. Does NOT remove any of the required existing fields (last_tick_status,
       last_decay_tick_at, nodes_recomputed_in_last_tick, tick_in_progress,
       and the KG-01 REST contract fields).

    Uses FastAPI TestClient so the real serialization path is exercised.
    """
    client, board_id = kg_health_test_client

    response = client.get("/api/v1/kg/health", params={"board_id": board_id})
    assert response.status_code == 200, f"Unexpected status: {response.status_code}"

    body = response.json()

    # New fields present with correct types and defaults.
    assert "boards_processed_in_last_tick" in body, (
        "boards_processed_in_last_tick missing from response"
    )
    assert "boards_failed_in_last_tick" in body, (
        "boards_failed_in_last_tick missing from response"
    )
    assert isinstance(body["boards_processed_in_last_tick"], int)
    assert isinstance(body["boards_failed_in_last_tick"], int)
    # No completed tick_run for this board → defaults are 0.
    assert body["boards_processed_in_last_tick"] == 0
    assert body["boards_failed_in_last_tick"] == 0

    # Existing required fields are NOT removed (additive contract, br_2a8cdfdc).
    missing = _REQUIRED_EXISTING_FIELDS - set(body.keys())
    assert not missing, f"Existing fields were removed: {missing}"
