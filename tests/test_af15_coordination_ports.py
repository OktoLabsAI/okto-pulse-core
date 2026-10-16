from __future__ import annotations

from pathlib import Path

import pytest

from coordination_fakes import (
    FakeLeaseProvider,
    FakeWriteLockPort,
)
from okto_pulse.core.ports import advisory_lock as advisory_lock_module
from okto_pulse.core.ports.advisory_lock import advisory_lock
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
