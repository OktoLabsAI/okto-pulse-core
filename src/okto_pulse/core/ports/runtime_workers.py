"""Runtime worker registry contract.

The core owns only the generic lifecycle orchestration. Editions provide the
worker factories and decide which families are active in their composition root.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any


WorkerFactory = Callable[[], Any | Awaitable[Any]]
WorkerStopper = Callable[[Any], None | Awaitable[None]]


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
            handle = self._active.pop(family)
            try:
                await _maybe_await(spec.stop(handle))
            except Exception as exc:  # noqa: BLE001 - shutdown must drain all families
                failures.append(
                    RuntimeWorkerStopFailure(
                        family=family,
                        error_class=exc.__class__.__name__,
                        message=str(exc),
                    )
                )
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
    "RuntimeWorkerRegistry",
    "RuntimeWorkerSpec",
    "RuntimeWorkerStartFailure",
    "RuntimeWorkerStopFailure",
]
