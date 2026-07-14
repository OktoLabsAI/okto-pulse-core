"""Tests for the Kùzu memory-safety patch shipped in 0.1.4.

Covers test scenarios ts_8f050d96, ts_4d36e7f2, ts_88753a17, ts_5efbeef6,
ts_31f323aa and the version-bump scenario ts_4f154d21. Paired with the
spec 849ca9ae-f744-439e-95f8-9083e2148c0a.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from okto_pulse.core.infra.config import CoreSettings, configure_settings, get_settings

_community_config = pytest.importorskip(
    "okto_pulse.community.config",
    reason="graph-runtime configuration belongs to the Community edition",
)
CommunitySettings = _community_config.CommunitySettings


@pytest.fixture(autouse=True)
def _restore_core_settings():
    """Snapshot and restore the configured settings around each test.

    Without this, tests that call ``configure_settings`` mutate
    the process-wide singleton, which in turn changes `_open_kuzu_db` behavior
    for unrelated downstream tests and can surface as graph file-lock flakes.
    """
    original = get_settings()
    yield
    configure_settings(original)


# ----------------------------------------------------------------------
# AC2, AC3 — Community graph defaults + adapter kwargs in bytes
# ----------------------------------------------------------------------

def test_community_settings_graph_defaults_are_safe():
    """AC3: Community owns safe defaults for its graph database adapter."""
    s = CommunitySettings()
    assert s.kg_kuzu_buffer_pool_mb == 512
    assert s.kg_kuzu_max_db_size_gb == 2
    assert s.kg_connection_pool_size == 8


def test_community_settings_rejects_unsupported_max_db_size():
    """Ladybug requires max_db_size to be a power of two in bytes."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        CommunitySettings(kg_kuzu_max_db_size_gb=6)
    msg = str(exc_info.value)
    assert "2, 4, 8, 16, 32, 64" in msg
    assert "power of 2" in msg


def test_open_kuzu_db_passes_kwargs_in_bytes(tmp_path):
    """AC2: _open_kuzu_db multiplies MB/GB by 1024^2 / 1024^3 correctly."""
    import kg_schema_testing as schema_module

    # Pin the Community settings so the test is deterministic even
    # if the suite-wide fixture wiggled the singleton.
    configure_settings(CommunitySettings(
        kg_kuzu_buffer_pool_mb=512,
        kg_kuzu_max_db_size_gb=2,
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
    assert captured["buffer_pool_size"] == 512 * 1024 * 1024  # 536_870_912
    assert captured["max_db_size"] == 2 * 1024 * 1024 * 1024  # 2_147_483_648


def test_open_kuzu_db_failure_message_includes_graph_settings(tmp_path):
    """Ladybug open failures include the active graph settings and fix hint."""
    import kg_schema_testing as schema_module

    configure_settings(CommunitySettings(
        kg_kuzu_buffer_pool_mb=512,
        kg_kuzu_max_db_size_gb=2,
        kg_connection_pool_size=8,
    ))

    class _FakeDatabase:
        def __init__(self, path, *, buffer_pool_size, max_db_size):
            raise RuntimeError(
                "Buffer manager exception: The given max db size should be a power of 2."
            )

    class _FakeKuzuModule:
        Database = _FakeDatabase

    with patch.dict("sys.modules", {"ladybug": _FakeKuzuModule}):
        with pytest.raises(RuntimeError) as exc_info:
            schema_module._open_kuzu_db(tmp_path / "graph.lbug")

    msg = str(exc_info.value)
    assert "kg_kuzu_buffer_pool_mb=512MB" in msg
    assert "kg_kuzu_max_db_size_gb=2GB" in msg
    assert "2, 4, 8, 16, 32 or 64 GB" in msg


def test_open_kuzu_db_retries_pybind_when_capi_shared_lib_is_missing(tmp_path):
    """A missing C-API shared library is a backend issue, not KG corruption."""
    import kg_schema_testing as schema_module
    import okto_pulse.community.adapters.kg_runtime as kg_runtime

    configure_settings(CommunitySettings(
        kg_kuzu_buffer_pool_mb=512,
        kg_kuzu_max_db_size_gb=2,
        kg_connection_pool_size=8,
    ))

    previous_backend = os.environ.get("LBUG_PYTHON_BACKEND")
    os.environ["LBUG_PYTHON_BACKEND"] = "capi"
    calls: list[tuple[str | None, str | None]] = []

    class _FakeDatabase:
        def __init__(
            self,
            path,
            *,
            buffer_pool_size,
            max_db_size,
            backend=None,
        ):
            calls.append((os.environ.get("LBUG_PYTHON_BACKEND"), backend))
            if os.environ.get("LBUG_PYTHON_BACKEND") != "pybind":
                raise RuntimeError(
                    "Could not find lbug C API shared library. "
                    "Set LBUG_C_API_LIB_PATH or download a shared lib."
                )
            self.path = path

    class _FakeKuzuModule:
        Database = _FakeDatabase

    try:
        with (
            patch.dict("sys.modules", {"ladybug": _FakeKuzuModule}),
            patch.object(kg_runtime, "_ladybug_pybind_available", return_value=True),
        ):
            db = schema_module._open_kuzu_db(tmp_path / "graph.lbug")
    finally:
        if previous_backend is None:
            os.environ.pop("LBUG_PYTHON_BACKEND", None)
        else:
            os.environ["LBUG_PYTHON_BACKEND"] = previous_backend

    assert db.path == str(tmp_path / "graph.lbug")
    assert calls == [("capi", None), ("pybind", "pybind")]
    assert os.environ.get("LBUG_PYTHON_BACKEND") == previous_backend


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


def test_open_kuzu_db_controls_wal_salvage_flag():
    """AC1 extension (KGD-01 FR1/TR2/BR1): the single open factory carries
    ``throw_on_wal_replay_failure`` on every open path.

    The primary open is fail-closed (``throw_on_wal_replay_failure=True``);
    the salvage retry (``False``) only runs on a corruption-marked failure and
    is gated by ``kg_wal_salvage_enabled``. Both the C-API path and the pybind
    fallback must carry the flag so no open bypasses the salvage contract.
    """
    import inspect

    import okto_pulse.community.adapters.kg_runtime as kg_runtime

    factory_src = inspect.getsource(kg_runtime._open_kuzu_db)
    assert "throw_on_wal_replay_failure" in factory_src, (
        "_open_kuzu_db must pass throw_on_wal_replay_failure on its opens"
    )
    assert "kg_wal_salvage_enabled" in factory_src, (
        "_open_kuzu_db must gate the salvage retry on kg_wal_salvage_enabled"
    )

    pybind_src = inspect.getsource(kg_runtime._open_ladybug_database_forced_pybind)
    assert "throw_on_wal_replay_failure" in pybind_src, (
        "the pybind fallback must propagate throw_on_wal_replay_failure"
    )

    salvage_src = inspect.getsource(kg_runtime._try_open_with_wal_salvage)
    assert "throw_on_wal_replay_failure=False" in salvage_src, (
        "the salvage retry must open with throw_on_wal_replay_failure=False"
    )

    # Recovery policy is adapter-specific and therefore Community-owned.
    assert CommunitySettings().kg_wal_salvage_enabled is True


# ----------------------------------------------------------------------
# AC4 — connection_pool reads configured edition settings + env override
# ----------------------------------------------------------------------

def test_pool_cap_reads_core_settings(monkeypatch, caplog):
    """AC4: without env var, the pool cap comes from edition settings; env var
    overrides with a deprecation warning."""
    from okto_pulse.community.adapters import graph_connection_pool as connection_pool

    monkeypatch.delenv("KG_CONNECTION_POOL_SIZE", raising=False)
    configure_settings(CommunitySettings(kg_connection_pool_size=8))
    assert connection_pool._read_cap_from_env() == 8

    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "16")
    with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.connection_pool"):
        cap = connection_pool._read_cap_from_env()
    assert cap == 16
    assert any("env_override_detected" in rec.message for rec in caplog.records)


def test_pool_cap_env_invalid_falls_back_to_settings(monkeypatch):
    """Invalid env var falls back to the configured edition settings."""
    from okto_pulse.community.adapters import graph_connection_pool as connection_pool

    configure_settings(CommunitySettings(kg_connection_pool_size=12))
    monkeypatch.setenv("KG_CONNECTION_POOL_SIZE", "not-a-number")
    assert connection_pool._read_cap_from_env() == 12


# ----------------------------------------------------------------------
# AC5, AC6, AC7 — REST GET/PUT /settings/runtime
# ----------------------------------------------------------------------

@pytest_asyncio.fixture
async def settings_client():
    """Minimal ASGI client wrapping just the settings router for fast tests."""
    from fastapi import FastAPI

    from okto_pulse.community.api.settings import router
    from okto_pulse.community.api.auth_deps import require_user
    from okto_pulse.core.infra.database import get_db, get_session_factory

    configure_settings(CommunitySettings())

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
    # Materialize Community adapter settings before serving the endpoint.
    configure_settings(CommunitySettings())

    response = await settings_client.get("/api/v1/settings/runtime")
    assert response.status_code == 200
    data = response.json()
    assert data["kg_kuzu_buffer_pool_mb"] == 512
    assert data["kg_kuzu_max_db_size_gb"] == 2
    assert data["kg_connection_pool_size"] == 8
    assert isinstance(data["restart_required"], bool)


@pytest.mark.asyncio
async def test_settings_runtime_put_persists_and_flips_restart(settings_client):
    """AC6: PUT with a valid value persists and sets restart_required=true."""
    # Baseline GET establishes the boot snapshot.
    configure_settings(CommunitySettings())
    await settings_client.get("/api/v1/settings/runtime")

    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_kuzu_buffer_pool_mb": 256},
    )
    assert put_resp.status_code == 200
    put_data = put_resp.json()
    assert put_data["kg_kuzu_buffer_pool_mb"] == 512  # effective still the boot value
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
        json={"kg_kuzu_buffer_pool_mb": 48},  # below min=128
    )
    assert bad.status_code == 422
    # Pydantic v2 emits "greater_than_or_equal" in the error type.
    assert "greater_than_or_equal" in bad.text or "ge" in bad.text

    after = await settings_client.get("/api/v1/settings/runtime")
    assert after.json()["kg_kuzu_buffer_pool_mb"] == baseline


@pytest.mark.asyncio
async def test_settings_runtime_put_422_on_unsupported_max_db_size(settings_client):
    """PUT rejects even but non-power-of-two max DB sizes before restart."""
    bad = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_kuzu_max_db_size_gb": 6},
    )
    assert bad.status_code == 422
    assert "2, 4, 8, 16, 32, 64" in bad.text


def test_commit_error_context_mentions_buffer_pool_settings():
    """Commit errors report the concrete graph knobs operators must change."""
    from okto_pulse.community.adapters.graph_error_mapping import map_graph_error
    from okto_pulse.core.kg.primitives import _contextualize_graph_commit_error

    configure_settings(CommunitySettings(
        kg_kuzu_buffer_pool_mb=128,
        kg_kuzu_max_db_size_gb=2,
        kg_connection_pool_size=8,
    ))

    mapped = map_graph_error(
        RuntimeError(
            "Buffer manager exception: Unable to allocate memory! "
            "The buffer pool is full and no memory could be freed!"
        ),
        operation="commit",
    )
    msg, details = _contextualize_graph_commit_error(mapped)

    assert "buffer manager exception" in msg.lower()
    assert "512 MB" in details["remediation"]
    assert details["graph_buffer_pool_mb"] == 128
    assert details["graph_max_db_size_gb"] == 2


# ----------------------------------------------------------------------
# AC13 — Version bump across every runtime surface
# ----------------------------------------------------------------------

def test_version_is_consistent_across_runtime_surfaces():
    """AC13: Core policy and Community MCP surfaces mirror package version."""
    import re

    core_pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    text = core_pyproject.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    assert match, "pyproject.toml must declare a top-level version"
    pyproject_version = match.group(1)

    core_settings = CoreSettings()
    assert core_settings.app_version == pyproject_version, (
        f"CoreSettings.app_version={core_settings.app_version!r} drifted from "
        f"pyproject.toml version={pyproject_version!r}"
    )
    community_settings = CommunitySettings()
    assert community_settings.mcp_server_version == pyproject_version, (
        f"CommunitySettings.mcp_server_version={community_settings.mcp_server_version!r} drifted "
        f"from pyproject.toml version={pyproject_version!r}"
    )
