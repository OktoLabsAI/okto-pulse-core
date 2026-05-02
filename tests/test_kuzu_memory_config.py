"""Tests for the Kùzu memory-safety patch shipped in 0.1.4.

Covers test scenarios ts_8f050d96, ts_4d36e7f2, ts_88753a17, ts_5efbeef6,
ts_31f323aa and the version-bump scenario ts_4f154d21. Paired with the
spec 849ca9ae-f744-439e-95f8-9083e2148c0a.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings


@pytest.fixture(autouse=True)
def _restore_core_settings():
    """Snapshot + restore the CoreSettings singleton around each test.

    Without this, tests that `configure_settings(CoreSettings(...))` mutate
    the process-wide singleton, which in turn changes `_open_kuzu_db` behavior
    for unrelated downstream tests and can surface as Kùzu file-lock flakes.
    """
    original = get_settings()
    yield
    configure_settings(original)


# ----------------------------------------------------------------------
# AC2, AC3 — CoreSettings defaults + _open_kuzu_db passes kwargs in bytes
# ----------------------------------------------------------------------

def test_core_settings_defaults_are_safe():
    """AC3: fresh CoreSettings exposes the 0.1.4 safe defaults."""
    s = CoreSettings()
    assert s.kg_kuzu_buffer_pool_mb == 256
    assert s.kg_kuzu_max_db_size_gb == 1
    assert s.kg_connection_pool_size == 8


def test_open_kuzu_db_passes_kwargs_in_bytes(tmp_path):
    """AC2: _open_kuzu_db multiplies MB/GB by 1024^2 / 1024^3 correctly."""
    from okto_pulse.core.kg import schema as schema_module

    # Pin CoreSettings to explicit values so the test is deterministic even
    # if the suite-wide fixture wiggled the singleton.
    configure_settings(CoreSettings(
        kg_kuzu_buffer_pool_mb=256,
        kg_kuzu_max_db_size_gb=1,
        kg_connection_pool_size=8,
    ))

    captured: dict = {}

    class _FakeDatabase:
        def __init__(self, path, *, buffer_pool_size, max_db_size):
            captured["path"] = path
            captured["buffer_pool_size"] = buffer_pool_size
            captured["max_db_size"] = max_db_size

    class _FakeKuzuModule:
        Database = _FakeDatabase

    fake_path = tmp_path / "graph.lbug"
    # Monkeypatch the lazy-imported LadybugDB module.
    with patch.dict("sys.modules", {"ladybug": _FakeKuzuModule}):
        schema_module._open_kuzu_db(fake_path)

    assert captured["path"] == str(fake_path)
    assert captured["buffer_pool_size"] == 256 * 1024 * 1024  # 268_435_456
    assert captured["max_db_size"] == 1 * 1024 * 1024 * 1024  # 1_073_741_824


# ----------------------------------------------------------------------
# AC1 — Zero direct kuzu.Database() call sites outside the helper
# ----------------------------------------------------------------------

def test_zero_direct_kuzu_database_call_sites():
    """AC1: the only `kuzu.Database(...)` call is inside _open_kuzu_db.

    Walks the entire core/kg source tree and fails if any other module
    bypasses the helper. Guards against future regressions when adding new
    code paths that open Kùzu.
    """
    import okto_pulse.core.kg as kg_pkg

    pkg_root = Path(kg_pkg.__file__).parent
    offenders: list[str] = []
    helper_file = pkg_root / "schema.py"

    pattern = re.compile(r"\bkuzu\.Database\s*\(")
    for py_file in pkg_root.rglob("*.py"):
        if py_file == helper_file:
            continue  # the single authorised call lives in the helper
        text = py_file.read_text(encoding="utf-8", errors="ignore")
        if pattern.search(text):
            offenders.append(str(py_file.relative_to(pkg_root)))

    assert not offenders, (
        f"Direct kuzu.Database(...) calls outside _open_kuzu_db: {offenders}"
    )


# ----------------------------------------------------------------------
# AC4 — connection_pool reads CoreSettings + honours env var override
# ----------------------------------------------------------------------

def test_pool_cap_reads_core_settings(monkeypatch, caplog):
    """AC4: without env var, pool cap comes from CoreSettings; env var
    overrides with a deprecation warning."""
    from okto_pulse.core.kg import connection_pool

    monkeypatch.delenv("KG_CONNECTION_POOL_SIZE", raising=False)
    configure_settings(CoreSettings(kg_connection_pool_size=8))
    assert connection_pool._read_cap_from_env() == 8

    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "16")
    with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.connection_pool"):
        cap = connection_pool._read_cap_from_env()
    assert cap == 16
    assert any("env_override_detected" in rec.message for rec in caplog.records)


def test_pool_cap_env_invalid_falls_back_to_settings(monkeypatch):
    """Invalid env var doesn't crash — falls back to CoreSettings."""
    from okto_pulse.core.kg import connection_pool

    configure_settings(CoreSettings(kg_connection_pool_size=12))
    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "not-a-number")
    assert connection_pool._read_cap_from_env() == 12


# ----------------------------------------------------------------------
# AC5, AC6, AC7 — REST GET/PUT /settings/runtime
# ----------------------------------------------------------------------

@pytest_asyncio.fixture
async def settings_client():
    """Minimal ASGI client wrapping just the settings router for fast tests."""
    from fastapi import FastAPI

    from okto_pulse.core.api.settings import router
    from okto_pulse.core.infra.auth import require_user
    from okto_pulse.core.infra.database import get_db, get_session_factory

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")

    # Bypass auth for tests — inject a fixed user id.
    async def _fake_user():
        return "user-test"

    # Bypass the @Depends(get_db) with a per-request session.
    async def _override_db():
        factory = get_session_factory()
        async with factory() as session:
            yield session

    app.dependency_overrides[require_user] = _fake_user
    app.dependency_overrides[get_db] = _override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.mark.asyncio
async def test_settings_runtime_get_returns_defaults(settings_client):
    """AC5: GET on a fresh install returns the safe defaults."""
    # Ensure CoreSettings is the fresh default. The test suite's autouse
    # fixture may have left an instance with other values — force a reset.
    configure_settings(CoreSettings())

    response = await settings_client.get("/api/v1/settings/runtime")
    assert response.status_code == 200
    data = response.json()
    assert data["kg_kuzu_buffer_pool_mb"] == 256
    assert data["kg_kuzu_max_db_size_gb"] == 1
    assert data["kg_connection_pool_size"] == 8
    assert isinstance(data["restart_required"], bool)


@pytest.mark.asyncio
async def test_settings_runtime_put_persists_and_flips_restart(settings_client):
    """AC6: PUT with a valid value persists and sets restart_required=true."""
    # Baseline GET establishes the boot snapshot.
    configure_settings(CoreSettings())
    await settings_client.get("/api/v1/settings/runtime")

    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_kuzu_buffer_pool_mb": 64},
    )
    assert put_resp.status_code == 200
    put_data = put_resp.json()
    assert put_data["kg_kuzu_buffer_pool_mb"] == 256  # effective still the boot value
    assert put_data["restart_required"] is True

    # Second GET confirms persistence.
    get_resp = await settings_client.get("/api/v1/settings/runtime")
    assert get_resp.status_code == 200
    assert get_resp.json()["restart_required"] is True


@pytest.mark.asyncio
async def test_settings_runtime_put_422_on_out_of_range(settings_client):
    """AC7: PUT rejects values below the Pydantic minimum and doesn't mutate state."""
    before = await settings_client.get("/api/v1/settings/runtime")
    baseline = before.json()["kg_kuzu_buffer_pool_mb"]

    bad = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_kuzu_buffer_pool_mb": 8},  # below min=16
    )
    assert bad.status_code == 422
    # Pydantic v2 emits "greater_than_or_equal" in the error type.
    assert "greater_than_or_equal" in bad.text or "ge" in bad.text

    after = await settings_client.get("/api/v1/settings/runtime")
    assert after.json()["kg_kuzu_buffer_pool_mb"] == baseline


# ----------------------------------------------------------------------
# AC13 — Version bump across every runtime surface
# ----------------------------------------------------------------------

def test_version_is_consistent_across_runtime_surfaces():
    """AC13 (refactored): CoreSettings + pyproject.toml carry the SAME version
    string. The test reads pyproject as the single source of truth and asserts
    that every other surface mirrors it — so future version bumps only need
    to update pyproject + CoreSettings (the test verifies they stay in sync).
    """
    import re

    core_pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    text = core_pyproject.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    assert match, "pyproject.toml must declare a top-level version"
    pyproject_version = match.group(1)

    s = CoreSettings()
    assert s.app_version == pyproject_version, (
        f"CoreSettings.app_version={s.app_version!r} drifted from "
        f"pyproject.toml version={pyproject_version!r}"
    )
    assert s.mcp_server_version == pyproject_version, (
        f"CoreSettings.mcp_server_version={s.mcp_server_version!r} drifted "
        f"from pyproject.toml version={pyproject_version!r}"
    )
