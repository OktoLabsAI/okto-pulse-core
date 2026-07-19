"""Runtime worker registry contract.

The core owns only the generic lifecycle orchestration. Editions provide the
worker factories and decide which families are active in their composition root.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


WorkerFactory = Callable[[], Any | Awaitable[Any]]
WorkerStopper = Callable[[Any], None | Awaitable[None]]


@runtime_checkable
class WorkerClockPort(Protocol):
    """Clock used by deterministic worker processors."""

    def now(self) -> datetime:
        ...


@runtime_checkable
class BlockingExecutionPort(Protocol):
    """Edition-owned execution of a blocking durability operation.

    Callers may depend on active ``contextvars`` (notably graph write guards
    and composed runtime ownership). Core blocking bridges capture that
    context before invoking this port. Implementations must execute the
    supplied callable as received and must not replace it with an unwrapped
    original callable.
    """

    async def run(self, operation: Callable[[], Any]) -> Any:
        ...

    async def join(self, timeout: float) -> int:
        """Wait for owned operations and return the number still pending."""
        ...


class WorkerDrainIncomplete(RuntimeError):
    """A bounded worker shutdown could not prove native-operation quiescence.

    This is a fail-closed lifecycle signal: graph and relational runtimes must
    remain open because a retained worker task may still complete durability
    work against them.
    """

    code = "worker_native_drain_incomplete"

    def __init__(
        self,
        *,
        family: str,
        phase: str,
        pending_tasks: int,
        pending_operations: int,
        timeout_seconds: float,
    ) -> None:
        self.family = family
        self.phase = phase
        self.pending_tasks = max(0, int(pending_tasks))
        self.pending_operations = max(0, int(pending_operations))
        self.timeout_seconds = max(0.0, float(timeout_seconds))
        super().__init__(
            f"{self.code}: family={family} phase={phase} "
            f"pending_tasks={self.pending_tasks} "
            f"pending_operations={self.pending_operations} "
            f"timeout_seconds={self.timeout_seconds:g}"
        )


@runtime_checkable
class QueueWorkPort(Protocol):
    """Application processor consumed by a queue runner."""

    async def process_batch(self) -> int:
        ...


@runtime_checkable
class OutboxWorkPort(Protocol):
    """Application processor consumed by an outbox runner."""

    async def process_once(self) -> int:
        ...


@runtime_checkable
class LeaseRecoveryPort(Protocol):
    """Recovers work abandoned by a previous process or lease owner."""

    async def recover_stale_claims(self) -> int:
        ...


@runtime_checkable
class DeliverySignalPort(Protocol):
    """Wake-up boundary exposed by an edition-owned runner."""

    def notify(self) -> None:
        ...


@dataclass(frozen=True)
class RuntimeWorkerSpec:
    """One worker family registered by an edition composition root."""

    family: str
    start: WorkerFactory
    stop: WorkerStopper
    required: bool = True
    stop_priority: int = 0


@dataclass(frozen=True)
class RuntimeWorkerStartFailure:
    """A non-fatal start failure captured for an optional worker family."""

    family: str
    error_class: str
    message: str


@dataclass(frozen=True)
class RuntimeWorkerStopFailure:
    """A non-fatal stop failure captured during shutdown."""

    family: str
    error_class: str
    message: str
    resource_close_unsafe: bool = False


class RuntimeWorkerRegistry:
    """Idempotent start/stop registry for runtime worker families."""

    def __init__(self, specs: Iterable[RuntimeWorkerSpec] = ()) -> None:
        self._specs: dict[str, RuntimeWorkerSpec] = {}
        self._active: dict[str, Any] = {}
        self._start_counts: dict[str, int] = {}
        self._start_failures: list[RuntimeWorkerStartFailure] = []
        for spec in specs:
            self.register(spec)

    def register(self, spec: RuntimeWorkerSpec) -> None:
        if not spec.family:
            raise ValueError("worker family is required")
        if spec.family in self._specs:
            raise ValueError(f"worker family already registered: {spec.family}")
        self._specs[spec.family] = spec
        self._start_counts.setdefault(spec.family, 0)

    @property
    def families(self) -> tuple[str, ...]:
        return tuple(self._specs)

    @property
    def active_families(self) -> tuple[str, ...]:
        return tuple(self._active)

    def start_count(self, family: str) -> int:
        return self._start_counts.get(family, 0)

    def get_handle(self, family: str) -> Any | None:
        """Return an active edition-owned runner without constructing one."""

        return self._active.get(family)

    def is_running(self, family: str) -> bool:
        handle = self.get_handle(family)
        if handle is None:
            return False
        return bool(getattr(handle, "is_running", True))

    def notify(self, family: str) -> bool:
        handle = self.get_handle(family)
        notify = getattr(handle, "notify", None) if handle is not None else None
        if not callable(notify):
            return False
        notify()
        return True

    async def process_once(self, family: str) -> int:
        """Run one application-processor iteration through an active runner."""

        handle = self.get_handle(family)
        if handle is None:
            return 0
        operation = getattr(handle, "process_once", None)
        if not callable(operation):
            operation = getattr(handle, "process_batch", None)
        if not callable(operation):
            return 0
        return int(await _maybe_await(operation()))

    def snapshot(self, family: str, **context: Any) -> dict[str, Any]:
        handle = self.get_handle(family)
        if handle is None:
            return {}
        snapshot = getattr(handle, "snapshot", None)
        if not callable(snapshot):
            snapshot = getattr(handle, "snapshot_pool", None)
        if not callable(snapshot):
            value = {}
        else:
            try:
                value = snapshot(**context)
            except TypeError:
                value = snapshot()
        return dict(value or {})

    @property
    def start_failures(self) -> tuple[RuntimeWorkerStartFailure, ...]:
        return tuple(self._start_failures)

    async def start_family(self, family: str) -> Any:
        if family in self._active:
            return self._active[family]
        try:
            spec = self._specs[family]
        except KeyError as exc:
            raise KeyError(f"unknown worker family: {family}") from exc
        handle = await _maybe_await(spec.start())
        self._active[family] = handle
        self._start_counts[family] += 1
        return handle

    async def start_all(self) -> tuple[str, ...]:
        started: list[str] = []
        for family, spec in self._specs.items():
            try:
                if family not in self._active:
                    await self.start_family(family)
                    started.append(family)
            except Exception as exc:
                if spec.required:
                    await self.stop_families(reversed(started))
                    raise
                self._start_failures.append(
                    RuntimeWorkerStartFailure(
                        family=family,
                        error_class=exc.__class__.__name__,
                        message=str(exc),
                    )
                )
        return self.active_families

    async def stop_families(
        self, families: Iterable[str]
    ) -> tuple[RuntimeWorkerStopFailure, ...]:
        failures: list[RuntimeWorkerStopFailure] = []
        for family in families:
            if family not in self._active:
                continue
            spec = self._specs[family]
            handle = self._active[family]
            try:
                await _maybe_await(spec.stop(handle))
            except Exception as exc:  # noqa: BLE001 - shutdown must drain all families
                failures.append(
                    RuntimeWorkerStopFailure(
                        family=family,
                        error_class=exc.__class__.__name__,
                        message=str(exc),
                        resource_close_unsafe=isinstance(
                            exc,
                            WorkerDrainIncomplete,
                        ),
                    )
                )
            else:
                # A failed stop may retain in-flight native work. Keep the
                # edition handle observable/retryable until quiescence is
                # actually proven instead of reporting a false inactive state.
                if self._active.get(family) is handle:
                    self._active.pop(family, None)
        return tuple(failures)

    async def stop_all(self) -> tuple[RuntimeWorkerStopFailure, ...]:
        active = tuple(self._active)
        index = {family: position for position, family in enumerate(active)}
        stop_order = sorted(
            active,
            key=lambda family: (
                self._specs[family].stop_priority,
                index[family],
            ),
            reverse=True,
        )
        return await self.stop_families(stop_order)


async def _maybe_await(value: Any | Awaitable[Any]) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


__all__ = [
    "BlockingExecutionPort",
    "DeliverySignalPort",
    "LeaseRecoveryPort",
    "OutboxWorkPort",
    "QueueWorkPort",
    "RuntimeWorkerRegistry",
    "RuntimeWorkerSpec",
    "RuntimeWorkerStartFailure",
    "RuntimeWorkerStopFailure",
    "WorkerDrainIncomplete",
    "WorkerClockPort",
]
