"""Context-local runtime values shared by Core composition facades.

The registry contains edition-owned adapters only. Each ``RuntimeComposition``
owns a registry instance, so two applications in the same process cannot leak
providers across tenants or editions.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping


@dataclass(slots=True)
class RuntimeValueRegistry:
    _values: dict[str, Any] = field(default_factory=dict)

    def register(self, key: str, value: Any) -> None:
        if value is None:
            raise ValueError(f"runtime_value_none:{key}")
        self._values[key] = value

    def resolve(self, key: str) -> Any | None:
        return self._values.get(key)

    def require(self, key: str, error: str | None = None) -> Any:
        value = self.resolve(key)
        if value is None:
            raise RuntimeError(error or f"runtime_value_not_configured:{key}")
        return value

    def discard(self, *keys: str) -> None:
        for key in keys:
            self._values.pop(key, None)

    def snapshot(self) -> Mapping[str, Any]:
        return dict(self._values)

    def copy(self) -> "RuntimeValueRegistry":
        return RuntimeValueRegistry(dict(self._values))


_active_runtime_values: ContextVar[RuntimeValueRegistry | None] = ContextVar(
    "okto_pulse_active_runtime_values",
    default=None,
)


def current_runtime_values(*, create: bool = False) -> RuntimeValueRegistry | None:
    registry = _active_runtime_values.get()
    if registry is None and create:
        registry = RuntimeValueRegistry()
        _active_runtime_values.set(registry)
    return registry


def snapshot_runtime_values() -> RuntimeValueRegistry:
    registry = current_runtime_values()
    return registry.copy() if registry is not None else RuntimeValueRegistry()


def capture_runtime_values_for_tests() -> RuntimeValueRegistry:
    """Capture the complete current registry for deterministic test restoration."""

    return snapshot_runtime_values()


def restore_runtime_values_for_tests(registry: RuntimeValueRegistry) -> None:
    """Replace the current test context with a previously captured registry."""

    _active_runtime_values.set(registry.copy())


def register_runtime_value(key: str, value: Any) -> None:
    registry = current_runtime_values(create=True)
    assert registry is not None
    registry.register(key, value)


def resolve_runtime_value(key: str) -> Any | None:
    registry = current_runtime_values()
    return registry.resolve(key) if registry is not None else None


def require_runtime_value(key: str, error: str | None = None) -> Any:
    registry = current_runtime_values()
    if registry is None:
        raise RuntimeError(error or f"runtime_value_not_configured:{key}")
    return registry.require(key, error)


def reset_runtime_values(*keys: str) -> None:
    registry = current_runtime_values()
    if registry is None:
        return
    if keys:
        registry.discard(*keys)
    else:
        _active_runtime_values.set(RuntimeValueRegistry())


@contextmanager
def runtime_value_scope(
    registry: RuntimeValueRegistry,
) -> Iterator[RuntimeValueRegistry]:
    token = _active_runtime_values.set(registry)
    try:
        yield registry
    finally:
        _active_runtime_values.reset(token)


__all__ = [
    "RuntimeValueRegistry",
    "capture_runtime_values_for_tests",
    "current_runtime_values",
    "register_runtime_value",
    "require_runtime_value",
    "reset_runtime_values",
    "restore_runtime_values_for_tests",
    "resolve_runtime_value",
    "runtime_value_scope",
    "snapshot_runtime_values",
]
