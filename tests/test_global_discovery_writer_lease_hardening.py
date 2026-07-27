from __future__ import annotations

import inspect
from contextvars import ContextVar
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.kg import global_discovery_writer as writer_module
from okto_pulse.core.kg.global_discovery_recovery import (
    GlobalDiscoveryRecoveryService,
)
from okto_pulse.core.kg.global_discovery_writer import (
    DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
    MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
    GlobalDiscoveryWriterFenceLost,
    GlobalDiscoveryWriterLease,
    assert_global_discovery_writer_fence,
    global_discovery_writer_scope,
)


_composition_marker: ContextVar[str] = ContextVar(
    "test_global_writer_composition",
    default="missing",
)


class _ObservedLock:
    def __init__(
        self,
        *,
        renew_result: bool = True,
        renew_error: Exception | None = None,
        release_result: bool = True,
    ) -> None:
        from threading import Event

        self.renew_result = renew_result
        self.renew_error = renew_error
        self.release_result = release_result
        self.acquired_ttls: list[int] = []
        self.renewed_ttls: list[int] = []
        self.renew_contexts: list[str] = []
        self.renew_threads: list[Any] = []
        self.renew_attempted = Event()
        self.owner = True
        self.release_calls = 0

    def acquire(self, **kwargs: Any) -> SimpleNamespace:
        self.acquired_ttls.append(int(kwargs["ttl_seconds"]))
        return SimpleNamespace(
            acquired=True,
            owner_token="writer-token",
            current_owner=None,
        )

    def is_owner(self, _board_id: str, _owner_token: str) -> bool:
        return self.owner

    def renew(self, **kwargs: Any) -> bool:
        from threading import current_thread

        self.renewed_ttls.append(int(kwargs["ttl_seconds"]))
        self.renew_contexts.append(_composition_marker.get())
        self.renew_threads.append(current_thread())
        self.renew_attempted.set()
        if self.renew_error is not None:
            raise self.renew_error
        return self.renew_result

    def release(self, **_kwargs: Any) -> bool:
        self.release_calls += 1
        self.owner = False
        return self.release_result


def _install_lock(monkeypatch: pytest.MonkeyPatch, lock: _ObservedLock) -> None:
    monkeypatch.setattr(writer_module, "KGSingleWriterLock", lambda: lock)


def test_global_writer_default_and_explicit_ttl_are_bounded_to_60_seconds() -> None:
    lock = _ObservedLock()

    lease = GlobalDiscoveryWriterLease.acquire(operation="test", lock=lock)  # type: ignore[arg-type]

    assert DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS == 60
    assert MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS == 60
    assert lease.ttl_seconds == 60
    assert lock.acquired_ttls == [60]
    assert lease.release() is True

    with pytest.raises(ValueError, match=r"1\.\.60"):
        GlobalDiscoveryWriterLease.acquire(  # type: ignore[arg-type]
            operation="too-long",
            ttl_seconds=61,
            lock=lock,
        )
    with pytest.raises(ValueError, match=r"1\.\.60"):
        GlobalDiscoveryWriterLease(  # type: ignore[arg-type]
            lock=lock,
            owner_token="token",
            operation="too-long-direct",
            ttl_seconds=3600,
        )


def test_scope_renews_with_copied_context_and_joins_before_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = _ObservedLock()
    _install_lock(monkeypatch, lock)
    marker = _composition_marker.set("community-composition")
    try:
        with global_discovery_writer_scope(
            operation="long-sync-operation",
            ttl_seconds=2,
            renew_interval_seconds=0.01,
        ):
            assert lock.renew_attempted.wait(1)
            assert_global_discovery_writer_fence()
    finally:
        _composition_marker.reset(marker)

    assert lock.acquired_ttls == [2]
    assert lock.renewed_ttls
    assert set(lock.renew_contexts) == {"community-composition"}
    assert lock.release_calls == 1
    assert all(not thread.is_alive() for thread in lock.renew_threads)


@pytest.mark.parametrize(
    ("lock", "expected_cause"),
    [
        (_ObservedLock(renew_result=False), None),
        (_ObservedLock(renew_error=RuntimeError("renew failed")), RuntimeError),
    ],
)
def test_scope_fails_closed_when_background_renewal_fails(
    monkeypatch: pytest.MonkeyPatch,
    lock: _ObservedLock,
    expected_cause: type[Exception] | None,
) -> None:
    _install_lock(monkeypatch, lock)

    with pytest.raises(GlobalDiscoveryWriterFenceLost) as excinfo:
        with global_discovery_writer_scope(
            operation="renewal-failure",
            ttl_seconds=2,
            renew_interval_seconds=0.01,
        ):
            assert lock.renew_attempted.wait(1)
            # ``renew`` holds the lease mutex while the fake signals.  This
            # checkpoint therefore waits for, then observes, the poisoned
            # lease even if physical ownership still appears present.
            assert_global_discovery_writer_fence()

    if expected_cause is None:
        assert excinfo.value.__cause__ is None
    else:
        assert isinstance(excinfo.value.__cause__, expected_cause)
    assert lock.release_calls == 1
    assert all(not thread.is_alive() for thread in lock.renew_threads)


def test_scope_preserves_body_error_and_cleans_up_renewer_and_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = _ObservedLock()
    _install_lock(monkeypatch, lock)
    original = RuntimeError("productive body failed")

    with pytest.raises(RuntimeError) as excinfo:
        with global_discovery_writer_scope(
            operation="body-failure",
            ttl_seconds=2,
            renew_interval_seconds=0.01,
        ):
            assert lock.renew_attempted.wait(1)
            raise original

    assert excinfo.value is original
    assert lock.release_calls == 1
    assert all(not thread.is_alive() for thread in lock.renew_threads)


def test_scope_treats_false_release_as_fence_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = _ObservedLock(release_result=False)
    _install_lock(monkeypatch, lock)

    with pytest.raises(GlobalDiscoveryWriterFenceLost):
        with global_discovery_writer_scope(operation="release-failure"):
            pass

    assert lock.release_calls == 1


def test_global_recovery_uses_the_short_renewable_writer_lease() -> None:
    source = inspect.getsource(GlobalDiscoveryRecoveryService.run)

    assert "ttl_seconds=DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS" in source
    assert "with lease.renewing_guard():" in source
    assert "ttl_seconds=3600" not in source
