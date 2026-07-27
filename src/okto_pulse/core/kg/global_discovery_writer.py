"""One fenced writer lane shared by every Global Discovery mutation."""

from __future__ import annotations

import secrets
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, field
from math import isfinite
from threading import Event, Lock, Thread
from typing import Any, Iterator

from okto_pulse.core.kg.interfaces.global_discovery_runtime import (
    GLOBAL_DISCOVERY_WRITER_SCOPE,
)
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
from okto_pulse.core.kg.write_barrier import under_global_safe_write


# A process crash cannot release its durable token.  Keep the crash tombstone
# short and renew it while productive synchronous work is still alive instead
# of reserving the global writer lane for an hour.
DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS = 60
MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS = 60
DEFAULT_GLOBAL_DISCOVERY_WRITER_RENEW_INTERVAL_SECONDS = 15.0


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
    ttl_seconds: int = DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS
    released: bool = False
    _renew_lock: Any = field(
        default_factory=Lock,
        repr=False,
        compare=False,
    )
    _fence_lost: bool = field(default=False, repr=False, compare=False)
    _fence_failure: BaseException | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        self.ttl_seconds = _validate_writer_ttl(self.ttl_seconds)

    @classmethod
    def acquire(
        cls,
        *,
        operation: str,
        owner_id: str | None = None,
        ttl_seconds: int = DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
        admin_lane: bool = False,
        lock: KGSingleWriterLock | None = None,
    ) -> "GlobalDiscoveryWriterLease":
        bounded_ttl = _validate_writer_ttl(ttl_seconds)
        durable_lock = lock or KGSingleWriterLock()
        acquisition = durable_lock.acquire(
            board_id=GLOBAL_DISCOVERY_WRITER_SCOPE,
            operation=operation,
            owner_id=owner_id or f"{operation}:{secrets.token_urlsafe(12)}",
            ttl_seconds=bounded_ttl,
            admin_lane=admin_lane,
        )
        if not acquisition.acquired or not acquisition.owner_token:
            raise GlobalDiscoveryWriterContention(acquisition.current_owner)
        return cls(
            lock=durable_lock,
            owner_token=acquisition.owner_token,
            operation=operation,
            ttl_seconds=bounded_ttl,
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

    @contextmanager
    def renewing_guard(
        self,
        *,
        renew_interval_seconds: float | None = None,
    ) -> Iterator[None]:
        """Guard synchronous work and renew its short durable lease.

        The productive operation stays on the calling thread.  Only the lease
        heartbeat runs in a helper thread, under a copy of the current context
        so edition-owned coordination providers remain visible.  The helper is
        always stopped and joined before guard exit; a renewal failure poisons
        the lease and prevents a successful return.
        """

        interval = _validate_renew_interval(
            ttl_seconds=self.ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
        )
        stop = Event()
        renewal_failures: list[BaseException] = []

        def renew_until_stopped() -> None:
            while not stop.wait(interval):
                try:
                    self.renew()
                except BaseException as exc:  # pragma: no branch - terminal
                    renewal_failures.append(exc)
                    return

        guarded_error: BaseException | None = None
        with self.guard():
            # Capture inside ``guard`` so the exact active lease and the
            # edition's coordination composition both cross the thread edge.
            renewal_context = copy_context()
            renewal_thread = Thread(
                target=renewal_context.run,
                args=(renew_until_stopped,),
                name="pulse-global-discovery-writer-renewal",
                daemon=True,
            )
            renewal_thread.start()
            try:
                yield
            except BaseException as exc:
                guarded_error = exc
                raise
            finally:
                stop.set()
                # ``KGSingleWriterLock.renew`` is a bounded synchronous port
                # operation.  Joining here guarantees no heartbeat survives
                # guard exit and races a subsequent release/reacquire.
                renewal_thread.join()
                if guarded_error is None:
                    if renewal_failures:
                        failure = renewal_failures[0]
                        if isinstance(failure, GlobalDiscoveryWriterFenceLost):
                            raise failure
                        raise GlobalDiscoveryWriterFenceLost() from failure
                    self.assert_fenced()

    def assert_fenced(self) -> None:
        with self._renew_lock:
            if self.released or self._fence_lost:
                cause = self._fence_failure
                if cause is not None:
                    raise GlobalDiscoveryWriterFenceLost() from cause
                raise GlobalDiscoveryWriterFenceLost()
            try:
                owns_fence = self.lock.is_owner(
                    GLOBAL_DISCOVERY_WRITER_SCOPE, self.owner_token
                )
            except Exception as exc:
                self._fence_lost = True
                self._fence_failure = exc
                raise GlobalDiscoveryWriterFenceLost() from exc
            if not owns_fence:
                self._fence_lost = True
                raise GlobalDiscoveryWriterFenceLost()

    def renew(self) -> None:
        with self._renew_lock:
            if self.released or self._fence_lost:
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
                self._fence_lost = True
                self._fence_failure = exc
                raise GlobalDiscoveryWriterFenceLost() from exc
            except Exception as exc:
                # Keep semantic adapter/configuration errors visible to their
                # direct caller, but never treat the lease as healthy after a
                # renewal attempt whose outcome is unknown.
                self._fence_lost = True
                self._fence_failure = exc
                raise
            if not renewed:
                self._fence_lost = True
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
            if not released:
                self._fence_lost = True
            return released


_active_lease: ContextVar[GlobalDiscoveryWriterLease | None] = ContextVar(
    "okto_pulse_global_discovery_writer_lease", default=None
)


def _validate_writer_ttl(ttl_seconds: int) -> int:
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
        raise ValueError("ttl_seconds must be an integer")
    if not 1 <= ttl_seconds <= MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS:
        raise ValueError(
            "ttl_seconds must be within "
            f"1..{MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS}"
        )
    return ttl_seconds


def _validate_renew_interval(
    *,
    ttl_seconds: int,
    renew_interval_seconds: float | None,
) -> float:
    interval = (
        min(
            DEFAULT_GLOBAL_DISCOVERY_WRITER_RENEW_INTERVAL_SECONDS,
            ttl_seconds / 2,
        )
        if renew_interval_seconds is None
        else float(renew_interval_seconds)
    )
    if not isfinite(interval) or not 0 < interval <= ttl_seconds / 2:
        raise ValueError(
            "renew_interval_seconds must be positive and no greater than "
            "half ttl_seconds"
        )
    return interval


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
    ttl_seconds: int = DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS,
    renew_interval_seconds: float | None = None,
    admin_lane: bool = False,
) -> Iterator[GlobalDiscoveryWriterLease]:
    lease = GlobalDiscoveryWriterLease.acquire(
        operation=operation,
        owner_id=owner_id,
        ttl_seconds=ttl_seconds,
        admin_lane=admin_lane,
    )
    guarded_error: BaseException | None = None
    try:
        with lease.renewing_guard(
            renew_interval_seconds=renew_interval_seconds,
        ):
            yield lease
    except BaseException as exc:
        guarded_error = exc
        raise
    finally:
        try:
            released = lease.release()
        except BaseException:
            if guarded_error is None:
                raise
        else:
            if not released and guarded_error is None:
                raise GlobalDiscoveryWriterFenceLost()


__all__ = [
    "DEFAULT_GLOBAL_DISCOVERY_WRITER_RENEW_INTERVAL_SECONDS",
    "DEFAULT_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS",
    "GLOBAL_DISCOVERY_WRITER_SCOPE",
    "MAX_GLOBAL_DISCOVERY_WRITER_TTL_SECONDS",
    "GlobalDiscoveryWriterContention",
    "GlobalDiscoveryWriterFenceLost",
    "GlobalDiscoveryWriterLease",
    "assert_global_discovery_writer_fence",
    "global_discovery_writer_scope",
]
