"""Card 2c0483bb (ts_4d20c0d3, ts_6d9624ef) — board settings defaults,
1-5 validation, normalization, and the legacy skip_ambiguity_gate migration.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from okto_pulse.community.api.boards import router as boards_router
from okto_pulse.community.api import auth_deps as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import Board
from okto_pulse.core.models.schemas import BoardSettings
from okto_pulse.core.services.main import IdeationService

USER_ID = "ambiguity-settings-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# BoardSettings model — defaults + 1-5 validation (ts_4d20c0d3, AC1/AC3)
# ---------------------------------------------------------------------------


def test_board_settings_defaults_gate_off_threshold_three():
    s = BoardSettings()
    assert s.require_ideation_ambiguity_gate is False
    assert s.max_ideation_ambiguity == 3


@pytest.mark.parametrize("value", [1, 2, 3, 4, 5])
def test_board_settings_accepts_threshold_in_range(value):
    s = BoardSettings(require_ideation_ambiguity_gate=True, max_ideation_ambiguity=value)
    assert s.max_ideation_ambiguity == value


@pytest.mark.parametrize("value", [0, 6, -1, 99])
def test_board_settings_rejects_threshold_out_of_range(value):
    with pytest.raises(ValidationError):
        BoardSettings(max_ideation_ambiguity=value)


# ---------------------------------------------------------------------------
# Normalization helper — legacy settings resolve to defaults (TR7, BR1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_config_defaults_for_legacy_board(db_factory):
    async with db_factory() as db:
        board = Board(id=_id("board"), name="Legacy", owner_id=USER_ID, settings={})
        cfg = IdeationService(db)._resolve_ideation_ambiguity_config(board)
        assert cfg == {"require_ideation_ambiguity_gate": False, "max_ideation_ambiguity": 3}


@pytest.mark.asyncio
async def test_resolve_config_clamps_out_of_range_legacy_value(db_factory):
    async with db_factory() as db:
        board = Board(
            id=_id("board"),
            name="Legacy",
            owner_id=USER_ID,
            settings={"require_ideation_ambiguity_gate": True, "max_ideation_ambiguity": 99},
        )
        cfg = IdeationService(db)._resolve_ideation_ambiguity_config(board)
        assert cfg["require_ideation_ambiguity_gate"] is True
        assert cfg["max_ideation_ambiguity"] == 5  # defensively clamped to 1-5


# ---------------------------------------------------------------------------
# Legacy migration — idempotent ADD COLUMN, existing rows read false (ts_6d9624ef)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_migration_adds_column_idempotent_reads_false(tmp_path):
    steps = pytest.importorskip("okto_pulse.community.adapters.relational_schema_steps")

    db_path = tmp_path / "legacy.db"
    eng = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        # Legacy ideations table WITHOUT skip_ambiguity_gate.
        async with eng.begin() as conn:
            await conn.execute(text("CREATE TABLE ideations (id TEXT PRIMARY KEY, title TEXT)"))
            await conn.execute(text("INSERT INTO ideations (id, title) VALUES ('i1', 'legacy')"))

        with patch.object(steps, "get_engine", lambda: eng):
            await steps._migrate_add_ideation_skip_ambiguity_gate()
            # Idempotent: a second run must not raise.
            await steps._migrate_add_ideation_skip_ambiguity_gate()

        async with eng.begin() as conn:
            rows = (await conn.execute(text("SELECT id, skip_ambiguity_gate FROM ideations"))).fetchall()
        assert rows == [("i1", 0)]  # existing row reads false (0)
    finally:
        await eng.dispose()


# ---------------------------------------------------------------------------
# REST round-trip — persist + reload, reject invalid payload (AC2/AC3)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def _board_client():
    df = get_session_factory()
    board_id = _id("board")
    async with df() as db:
        db.add(
            Board(
                id=board_id,
                name="Gate Settings",
                owner_id=USER_ID,
                realm_id="local",
                settings={},
            )
        )
        await db.commit()

    app = FastAPI()
    app.include_router(boards_router, prefix="/api/v1/boards")

    async def _odb():
        async with df() as session:
            yield session

    app.dependency_overrides[get_db] = _odb
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID
    app.dependency_overrides[_auth_mod.get_realm_id] = lambda: "local"
    return TestClient(app), board_id


def test_rest_persists_gate_settings_and_reloads(_board_client):
    client, board_id = _board_client
    resp = client.patch(
        f"/api/v1/boards/{board_id}",
        json={"settings": {"require_ideation_ambiguity_gate": True, "max_ideation_ambiguity": 4}},
    )
    assert resp.status_code == 200
    settings = resp.json()["settings"]
    assert settings["require_ideation_ambiguity_gate"] is True
    assert settings["max_ideation_ambiguity"] == 4


def test_rest_rejects_out_of_range_threshold(_board_client):
    client, board_id = _board_client
    resp = client.patch(
        f"/api/v1/boards/{board_id}",
        json={"settings": {"require_ideation_ambiguity_gate": True, "max_ideation_ambiguity": 9}},
    )
    assert resp.status_code == 422
