"""KG-01.5 endpoint integration — IR ir_4039d470 (Grafx rework).

Proves the real settings endpoint (PUT /api/v1/settings/runtime) persists the
Grafx graph-runtime knobs fail-closed. The Grafx runtime has no in-place
storage migration: constructor options are restart-required and existing
storage geometry is immutable, so the endpoint no longer runs the
storage-grow / shrink / migration-plan branches of ``KGConfigChangeGuard``.
The retired engine's ``kg_kuzu_max_db_size_gb`` / ``kg_connection_pool_size``
knobs have no Grafx equivalent and were dropped with it. Tests cover:

* Allowed changes persist (buffer + read participants, restart_required).
* Retired engine keys are refused by the payload contract (extra=forbid).
* Invalid Grafx values are refused before persistence.
* Event-queue keys are hot-reload and never bump the guard counter.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from okto_pulse.community.config import CommunitySettings
from okto_pulse.core.infra.config import configure_settings, get_settings
from okto_pulse.core.kg.config_guard import (
    SETTING_GROUP_BUFFER,
    SETTING_GROUP_CONNECTION_POOL,
    SETTING_GROUP_STORAGE,
    get_config_block_count,
    reset_config_block_counter,
)


@pytest.fixture(autouse=True)
def _restore_core_settings_and_counter():
    original = get_settings()
    reset_config_block_counter()
    yield
    configure_settings(original)
    reset_config_block_counter()


@pytest_asyncio.fixture(autouse=True)
async def _reset_app_settings():
    """Wipe persisted AppSetting rows so tests don't leak state via the
    shared DB factory. Without this, a test that persists
    kg_grafx_buffer_pool_mb=128 makes the next test's "value_not_changed"
    path fire spuriously."""
    from okto_pulse.core.infra.database import get_session_factory
    from sqlalchemy_test_models import AppSetting

    factory = get_session_factory()
    async with factory() as session:
        await session.execute(AppSetting.__table__.delete())
        await session.commit()
    yield
    async with factory() as session:
        await session.execute(AppSetting.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
async def settings_client():
    from fastapi import FastAPI

    from okto_pulse.community.api.settings import router
    from okto_pulse.community.api.auth_deps import require_principal, require_user
    from okto_pulse.core.domain.realm import LOCAL_REALM_ID
    from okto_pulse.core.infra.database import get_db, get_session_factory
    from okto_pulse.core.ports.authentication import Principal

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")

    async def _fake_user():
        return "user-kg01-5"

    async def _override_db():
        factory = get_session_factory()
        async with factory() as session:
            yield session

    app.dependency_overrides[require_user] = _fake_user
    app.dependency_overrides[require_principal] = lambda: Principal(
        "user-kg01-5",
        realm_id=LOCAL_REALM_ID,
        claims={"roles": ["admin"]},
    )
    app.dependency_overrides[get_db] = _override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


# --- Allowed paths ------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_buffer_change_persists_with_implicit_restart_required(
    settings_client,
):
    """Buffer change with no explicit restart_policy persists as a desired
    value; the effective snapshot keeps the boot value until restart."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_grafx_buffer_pool_mb": 128},
    )
    assert put_resp.status_code == 200
    body = put_resp.json()
    assert body["kg_grafx_buffer_pool_mb"] == 64
    assert body["desired_values"]["kg_grafx_buffer_pool_mb"] == 128
    assert body["restart_required"] is True


@pytest.mark.asyncio
async def test_put_read_participants_change_persists_with_implicit_restart_required(
    settings_client,
):
    """Read participants replaced the retired connection-pool knob: it is a
    Grafx constructor option, so it is restart-required just the same."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_grafx_read_participants": 4},
    )
    assert put_resp.status_code == 200
    body = put_resp.json()
    assert body["kg_grafx_read_participants"] == 2
    assert body["desired_values"]["kg_grafx_read_participants"] == 4
    assert body["restart_required"] is True


@pytest.mark.asyncio
async def test_put_descriptor_revalidation_persists_with_restart_required(
    settings_client,
):
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_grafx_descriptor_revalidation": "strict"},
    )
    assert put_resp.status_code == 200
    body = put_resp.json()
    assert body["desired_values"]["kg_grafx_descriptor_revalidation"] == "strict"
    assert body["restart_required"] is True


# --- Fail-closed paths --------------------------------------------------------


@pytest.mark.asyncio
async def test_retired_graph_engine_keys_are_refused_by_the_payload_contract(
    settings_client,
):
    """The retired engine's storage/connection knobs have no Grafx equivalent.
    ``RuntimeSettingsPayload`` forbids extras, so they can never be persisted
    through a stale client."""
    configure_settings(CommunitySettings())
    await settings_client.get("/api/v1/settings/runtime")

    for retired_key, value in (
        ("kg_kuzu_max_db_size_gb", 4),
        ("kg_kuzu_buffer_pool_mb", 128),
        ("kg_connection_pool_size", 12),
    ):
        resp = await settings_client.put(
            "/api/v1/settings/runtime",
            json={retired_key: value},
        )
        assert resp.status_code == 422, retired_key

    get_resp = await settings_client.get("/api/v1/settings/runtime")
    assert get_resp.status_code == 200
    assert retired_key not in get_resp.json()


@pytest.mark.asyncio
async def test_put_out_of_range_grafx_value_is_refused_before_persistence(
    settings_client,
):
    """Grafx knobs are bounded by the payload validators; an out-of-range
    value never reaches the persistence layer."""
    configure_settings(CommunitySettings())
    await settings_client.get("/api/v1/settings/runtime")

    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_grafx_read_participants": 99},
    )
    assert put_resp.status_code == 422

    get_resp = await settings_client.get("/api/v1/settings/runtime")
    assert get_resp.json()["desired_values"]["kg_grafx_read_participants"] == 2


@pytest.mark.asyncio
async def test_put_non_power_of_two_page_size_is_refused(settings_client):
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_grafx_page_size": 12288},
    )
    assert put_resp.status_code == 422


# --- Non-graph-runtime keys bypass the guard ---------------------------------


@pytest.mark.asyncio
async def test_event_queue_key_change_does_not_go_through_guard(
    settings_client,
):
    """Event queue keys are hot-reloadable. The guard should NOT bump
    its counter for them."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_queue_max_concurrent_workers": 4},
    )
    assert put_resp.status_code == 200

    # No guard counter bumps for event-queue keys.
    assert get_config_block_count(SETTING_GROUP_STORAGE) == 0
    assert get_config_block_count(SETTING_GROUP_BUFFER) == 0
    assert get_config_block_count(SETTING_GROUP_CONNECTION_POOL) == 0
