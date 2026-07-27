"""Application-facing access to app-scoped runtime workers.

The helpers never create runners and never retain process-global state. They
resolve the registry owned by the active :class:`RuntimeComposition`; callers
outside an application scope receive an explicit inactive result.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.composition import current_runtime_composition
from okto_pulse.core.ports.runtime_workers import RuntimeWorkerRegistry


def current_runtime_worker_registry() -> RuntimeWorkerRegistry | None:
    composition = current_runtime_composition()
    if composition is None:
        return None
    registry = composition.worker_registry
    return registry if isinstance(registry, RuntimeWorkerRegistry) else None


def signal_runtime_worker(family: str) -> bool:
    registry = current_runtime_worker_registry()
    return registry.notify(family) if registry is not None else False


def runtime_worker_is_running(family: str) -> bool:
    registry = current_runtime_worker_registry()
    return registry.is_running(family) if registry is not None else False


async def process_runtime_worker_once(family: str) -> int:
    registry = current_runtime_worker_registry()
    return await registry.process_once(family) if registry is not None else 0


def runtime_worker_snapshot(family: str, **context: Any) -> dict[str, Any]:
    registry = current_runtime_worker_registry()
    return registry.snapshot(family, **context) if registry is not None else {}


__all__ = [
    "current_runtime_worker_registry",
    "process_runtime_worker_once",
    "runtime_worker_is_running",
    "runtime_worker_snapshot",
    "signal_runtime_worker",
]
