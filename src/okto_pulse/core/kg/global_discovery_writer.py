"""One fenced writer lane shared by every Global Discovery mutation."""

from __future__ import annotations

import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Iterator

from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GLOBAL_DISCOVERY_WRITER_SCOPE,
)
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
from okto_pulse.core.kg.write_barrier import under_global_safe_write


class GlobalDiscoveryWriterContention(RuntimeError):
    """The durable edition-owned writer fence is held by another process."""

    code = "global_discovery_writer_contention"

    def __init__(self, current_owner: str | None) -> None:
        self.current_owner = current_owner
        super().__init__(self.code)


class GlobalDiscoveryWriterFenceLost(RuntimeError):
    """The durable token no longer owns the shared Global Discovery fence."""

    code = "global_discovery_writer_fence_lost"

    def __init__(self) -> None:
        super().__init__(self.code)


@dataclass(slots=True)
class GlobalDiscoveryWriterLease:
    """Acquired durable fence plus the exact token used by the write barrier."""

    lock: KGSingleWriterLock
    owner_token: str
    operation: str
    ttl_seconds: int = 3600
    released: bool = False
    _renew_lock: Any = field(
        default_factory=Lock,
        repr=False,
        compare=False,
    )

    @classmethod
    def acquire(
        cls,
        *,
        operation: str,
        owner_id: str | None = None,
        ttl_seconds: int = 3600,
        admin_lane: bool = False,
        lock: KGSingleWriterLock | None = None,
    ) -> "GlobalDiscoveryWriterLease":
        durable_lock = lock or KGSingleWriterLock()
        acquisition = durable_lock.acquire(
            board_id=GLOBAL_DISCOVERY_WRITER_SCOPE,
            operation=operation,
            owner_id=owner_id or f"{operation}:{secrets.token_urlsafe(12)}",
            ttl_seconds=ttl_seconds,
            admin_lane=admin_lane,
        )
        if not acquisition.acquired or not acquisition.owner_token:
            raise GlobalDiscoveryWriterContention(acquisition.current_owner)
        return cls(
            lock=durable_lock,
            owner_token=acquisition.owner_token,
            operation=operation,
            ttl_seconds=ttl_seconds,
        )

    @contextmanager
    def guard(self) -> Iterator[None]:
        if self.released:
            raise RuntimeError("global discovery writer lease already released")
        self.assert_fenced()
        lease_handle = _active_lease.set(self)
        try:
            with under_global_safe_write(self.owner_token, self.operation):
                yield
            self.assert_fenced()
        finally:
            _active_lease.reset(lease_handle)

    def assert_fenced(self) -> None:
        if self.released or not self.lock.is_owner(
            GLOBAL_DISCOVERY_WRITER_SCOPE, self.owner_token
        ):
            raise GlobalDiscoveryWriterFenceLost()

    def renew(self) -> None:
        with self._renew_lock:
            if self.released:
                raise GlobalDiscoveryWriterFenceLost()
            try:
                renewed = self.lock.renew(
                    board_id=GLOBAL_DISCOVERY_WRITER_SCOPE,
                    owner_token=self.owner_token,
                    ttl_seconds=self.ttl_seconds,
                )
            except OSError as exc:
                # Renewal that dies on an OS fault (e.g. a Windows sharing
                # violation that survived the port's bounded retry) leaves
                # ownership unproven.  A raw OSError crossing this boundary
                # would reach generic native-failure handlers and fabricate
                # FAILED truth after physical work; surface the exact
                # fence-loss type with the OS fault chained as its cause.
                raise GlobalDiscoveryWriterFenceLost() from exc
            if not renewed:
                raise GlobalDiscoveryWriterFenceLost()

    def release(self) -> bool:
        with self._renew_lock:
            if self.released:
                return True
            released = self.lock.release(
                board_id=GLOBAL_DISCOVERY_WRITER_SCOPE,
                owner_token=self.owner_token,
            )
            self.released = True
            return released


_active_lease: ContextVar[GlobalDiscoveryWriterLease | None] = ContextVar(
    "okto_pulse_global_discovery_writer_lease", default=None
)


def assert_global_discovery_writer_fence() -> None:
    """Fail closed unless the current guard still owns its durable token."""

    lease = _active_lease.get()
    if lease is None:
        raise GlobalDiscoveryWriterFenceLost()
    lease.assert_fenced()


@contextmanager
def global_discovery_writer_scope(
    *,
    operation: str,
    owner_id: str | None = None,
    ttl_seconds: int = 3600,
    admin_lane: bool = False,
) -> Iterator[GlobalDiscoveryWriterLease]:
    lease = GlobalDiscoveryWriterLease.acquire(
        operation=operation,
        owner_id=owner_id,
        ttl_seconds=ttl_seconds,
        admin_lane=admin_lane,
    )
    try:
        with lease.guard():
            yield lease
    finally:
        lease.release()


__all__ = [
    "GLOBAL_DISCOVERY_WRITER_SCOPE",
    "GlobalDiscoveryWriterContention",
    "GlobalDiscoveryWriterFenceLost",
    "GlobalDiscoveryWriterLease",
    "assert_global_discovery_writer_fence",
    "global_discovery_writer_scope",
]
