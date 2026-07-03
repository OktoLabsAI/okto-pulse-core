from __future__ import annotations

from pathlib import Path

import pytest

from coordination_fakes import (
    FakeLeaseProvider,
    FakeRuntimeSettingsProvider,
    FakeWriteLockPort,
)
from okto_pulse.core.kg.workers import advisory_lock as advisory_lock_module
from okto_pulse.core.kg.workers.advisory_lock import advisory_lock
from okto_pulse.core.ports.coordination import (
    CoordinationProviderMissing,
    get_write_lock_port,
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)


@pytest.fixture(autouse=True)
def _coordination_registry():
    reset_coordination_providers_for_tests()
    yield
    reset_coordination_providers_for_tests()


@pytest.mark.asyncio
async def test_af15_lease_provider_allows_single_holder() -> None:
    lease_provider = FakeLeaseProvider()
    register_coordination_providers(lease_provider=lease_provider)

    first = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=30)
    assert first is not None
    assert lease_provider.is_held("kg_daily_tick")
    assert await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=30) is None

    await lease_provider.release(first)
    assert not lease_provider.is_held("kg_daily_tick")
    assert await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=30) is not None


@pytest.mark.asyncio
async def test_af15_advisory_lock_delegates_to_write_lock_port() -> None:
    port = FakeWriteLockPort()
    register_coordination_providers(write_lock_port=port)

    async with advisory_lock("board", "artifact"):
        assert port.is_locked("board", "artifact")
    assert not port.is_locked("board", "artifact")


def test_af15_missing_write_lock_provider_fails_closed() -> None:
    with pytest.raises(CoordinationProviderMissing):
        get_write_lock_port()


def test_af15_advisory_lock_facade_has_no_concrete_lock_primitives() -> None:
    source = Path(advisory_lock_module.__file__).read_text(encoding="utf-8")
    assert "asyncio.Lock(" not in source
    assert "threading.Lock(" not in source


@pytest.mark.asyncio
async def test_af15_runtime_settings_read_uses_provider() -> None:
    from okto_pulse.core.infra.config import get_settings
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.settings_service import (
        RUNTIME_KEYS,
        get_runtime_settings,
    )

    payload = {key: int(getattr(get_settings(), key)) for key in RUNTIME_KEYS}
    payload["kg_decay_tick_interval_minutes"] = 123
    provider = FakeRuntimeSettingsProvider(payload)
    register_coordination_providers(runtime_settings_provider=provider)

    factory = get_session_factory()
    async with factory() as db:
        snapshot = await get_runtime_settings(db)

    assert provider.read_scopes == ["global"]
    assert snapshot["kg_decay_tick_interval_minutes"] == 123


@pytest.mark.asyncio
async def test_af15_runtime_settings_partial_provider_merges_defaults() -> None:
    from okto_pulse.core.infra.config import get_settings
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.settings_service import (
        RUNTIME_KEYS,
        get_runtime_settings,
    )

    provider = FakeRuntimeSettingsProvider({"kg_decay_tick_interval_minutes": 123})
    register_coordination_providers(runtime_settings_provider=provider)

    factory = get_session_factory()
    async with factory() as db:
        snapshot = await get_runtime_settings(db)

    assert set(RUNTIME_KEYS).issubset(snapshot)
    assert snapshot["kg_decay_tick_interval_minutes"] == 123
    assert snapshot["kg_queue_min_interval_ms"] == int(
        get_settings().kg_queue_min_interval_ms
    )


@pytest.mark.asyncio
async def test_af15_runtime_settings_write_uses_ports() -> None:
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.services.settings_service import (
        get_runtime_settings,
        put_runtime_settings,
    )

    write_lock = FakeWriteLockPort()
    validation = FakeRuntimeSettingsProvider()
    register_coordination_providers(
        write_lock_port=write_lock,
        config_validation_port=validation,
    )

    factory = get_session_factory()
    async with factory() as db:
        current = await get_runtime_settings(db)
        value = int(current["kg_decay_tick_interval_minutes"])
        await put_runtime_settings(db, {"kg_decay_tick_interval_minutes": value})

    assert validation.validated_values[-1] == {
        "kg_decay_tick_interval_minutes": value
    }
    assert write_lock.acquired_async == [("_runtime", "settings")]
    assert write_lock.released_async == [("_runtime", "settings")]
