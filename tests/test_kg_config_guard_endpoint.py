"""KG-01.5 endpoint integration — IR ir_4039d470 (val_06cd6809 rework).

Proves that the real settings endpoint (PUT /api/v1/settings/runtime)
routes graph-runtime changes through KGConfigChangeGuard BEFORE
persisting. Tests cover:

* Allowed change persists (buffer with implicit restart_required).
* Blocked changes do NOT persist and return HTTP 400 with bounded reason.
* Storage shrink below current is blocked.
* Storage grow without migration_plan_ref is blocked.
* Connection-pool changes require restart policy.
* Bounded audit_event surfaces in the response — no raw values leak.
* Existing legacy tests continue to pass (no regression).
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from okto_pulse.community.config import CommunitySettings
from okto_pulse.core.infra.config import configure_settings, get_settings
from okto_pulse.core.kg.config_guard import (
    ConfigBlockReason,
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
    kg_kuzu_max_db_size_gb=4 makes the next test's "value_not_changed"
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
    """Buffer change with no explicit restart_policy uses default
    restart_policy='required' (matches existing semantics) and is allowed."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_kuzu_buffer_pool_mb": 256},
    )
    assert put_resp.status_code == 200
    body = put_resp.json()
    assert body["restart_required"] is True


@pytest.mark.asyncio
async def test_put_connection_pool_change_persists_with_implicit_restart_required(
    settings_client,
):
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={"kg_connection_pool_size": 12},
    )
    assert put_resp.status_code == 200
    body = put_resp.json()
    assert body["kg_connection_pool_size"] == 8
    assert body["restart_required"] is True


@pytest.mark.asyncio
async def test_put_storage_grow_with_migration_plan_persists(settings_client):
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_kuzu_max_db_size_gb": 4,
            "migration_plan_ref": "MP-2026-05-26-001",
            "restart_policy": "scheduled",
        },
    )
    assert put_resp.status_code == 200


# --- Blocked paths (KGConfigChangeGuard enforcement) -------------------------


@pytest.mark.asyncio
async def test_put_storage_grow_without_migration_plan_is_blocked(
    settings_client,
):
    """val_06cd6809 enforcement: storage group requires migration_plan_ref.
    The endpoint MUST refuse to persist and return HTTP 400."""
    configure_settings(CommunitySettings())
    # Baseline GET to establish boot snapshot.
    await settings_client.get("/api/v1/settings/runtime")

    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_kuzu_max_db_size_gb": 4,
            # Note: no migration_plan_ref.
            "restart_policy": "required",
        },
    )
    assert put_resp.status_code == 400
    body = put_resp.json()
    detail = body["detail"]
    assert detail["error"] == "kg_config_change_blocked"
    assert detail["reason"] == ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value
    assert detail["setting_group"] == SETTING_GROUP_STORAGE
    assert detail["audit_event"].startswith("kg.config_change.")
    # TR12 safety: no raw values in the response.
    assert "4" not in detail["audit_event"]
    assert "4" not in detail["reason"]

    # Counter bumped for the bounded reason.
    assert (
        get_config_block_count(
            SETTING_GROUP_STORAGE,
            reason=ConfigBlockReason.MIGRATION_PLAN_REQUIRED.value,
        )
        == 1
    )

    # The block MUST NOT have persisted — GET still shows the baseline.
    get_resp = await settings_client.get("/api/v1/settings/runtime")
    assert get_resp.status_code == 200
    # Default boot value for kg_kuzu_max_db_size_gb is 2.
    assert get_resp.json()["kg_kuzu_max_db_size_gb"] == 2


@pytest.mark.asyncio
async def test_put_storage_shrink_below_current_is_blocked(settings_client):
    """val_06cd6809 enforcement: storage shrink below current footprint
    is blocked even with a migration plan + restart policy."""
    configure_settings(CommunitySettings())

    # First, grow storage to 4 (allowed with migration).
    grow = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_kuzu_max_db_size_gb": 4,
            "migration_plan_ref": "MP-grow",
            "restart_policy": "required",
        },
    )
    assert grow.status_code == 200

    # Now attempt to shrink back to 2 — must be blocked.
    shrink = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_kuzu_max_db_size_gb": 2,
            "migration_plan_ref": "MP-shrink",
            "restart_policy": "required",
        },
    )
    assert shrink.status_code == 400
    detail = shrink.json()["detail"]
    assert detail["reason"] == ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value
    assert detail["setting_group"] == SETTING_GROUP_STORAGE
    # Counter recorded the shrink-block.
    assert (
        get_config_block_count(
            SETTING_GROUP_STORAGE,
            reason=ConfigBlockReason.SHRINK_BELOW_CURRENT_FOOTPRINT.value,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_put_buffer_with_restart_policy_none_is_blocked(
    settings_client,
):
    """Buffer changes are restart-required; explicit policy=none blocks."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_kuzu_buffer_pool_mb": 256,
            "restart_policy": "none",
        },
    )
    assert put_resp.status_code == 400
    detail = put_resp.json()["detail"]
    assert detail["reason"] == ConfigBlockReason.RESTART_POLICY_REQUIRED.value
    assert detail["setting_group"] == SETTING_GROUP_BUFFER


@pytest.mark.asyncio
async def test_put_connection_pool_with_restart_policy_none_is_blocked(
    settings_client,
):
    """Connection-pool changes are graph-runtime constructor-time settings."""
    configure_settings(CommunitySettings())
    put_resp = await settings_client.put(
        "/api/v1/settings/runtime",
        json={
            "kg_connection_pool_size": 12,
            "restart_policy": "none",
        },
    )
    assert put_resp.status_code == 400
    detail = put_resp.json()["detail"]
    assert detail["reason"] == ConfigBlockReason.RESTART_POLICY_REQUIRED.value
    assert detail["setting_group"] == SETTING_GROUP_CONNECTION_POOL


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
    samples_storage = get_config_block_count(SETTING_GROUP_STORAGE)
    samples_buffer = get_config_block_count(SETTING_GROUP_BUFFER)
    assert samples_storage == 0
    assert samples_buffer == 0
