"""Context-local runtime values shared by Core composition facades.

The registry contains edition-owned adapters only. Each ``RuntimeComposition``
owns a registry instance, so two applications in the same process cannot leak
providers across tenants or editions.
"""

from __future__ import annotations

from collections import Counter, deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Callable, Iterator, Mapping


@dataclass(slots=True)
class RuntimeValueRegistry:
    _values: dict[str, Any] = field(default_factory=dict)
    _guard: RLock = field(
        default_factory=RLock,
        init=False,
        repr=False,
        compare=False,
    )

    def register(self, key: str, value: Any) -> None:
        if value is None:
            raise ValueError(f"runtime_value_none:{key}")
        with self._guard:
            self._values[key] = value

    def resolve(self, key: str) -> Any | None:
        with self._guard:
            return self._values.get(key)

    def resolve_or_create(self, key: str, factory: Callable[[], Any]) -> Any:
        """Resolve one value, creating it atomically for this runtime."""

        with self._guard:
            value = self._values.get(key)
            if value is None:
                value = factory()
                if value is None:
                    raise ValueError(f"runtime_value_none:{key}")
                self._values[key] = value
            return value

    def require(self, key: str, error: str | None = None) -> Any:
        value = self.resolve(key)
        if value is None:
            raise RuntimeError(error or f"runtime_value_not_configured:{key}")
        return value

    def discard(self, *keys: str) -> None:
        with self._guard:
            for key in keys:
                self._values.pop(key, None)

    def snapshot(self) -> Mapping[str, Any]:
        with self._guard:
            return dict(self._values)

    def copy(self) -> "RuntimeValueRegistry":
        copied: dict[str, Any] = {}
        for key, value in self.snapshot().items():
            clone = getattr(value, "clone_for_runtime", None)
            if callable(clone):
                copied[key] = clone()
            elif key.startswith("runtime.lock."):
                copied[key] = RLock()
            elif key.startswith("runtime.state."):
                if isinstance(value, Counter):
                    copied[key] = value.copy()
                elif isinstance(value, dict):
                    copied[key] = value.copy()
                elif isinstance(value, list):
                    copied[key] = list(value)
                elif isinstance(value, set):
                    copied[key] = set(value)
                elif isinstance(value, deque):
                    copied[key] = value.copy()
                else:
                    copied[key] = value
            else:
                copied[key] = value
        return RuntimeValueRegistry(copied)


@dataclass(frozen=True, slots=True)
class RuntimeLock:
    """Immutable descriptor for a lock owned by the active runtime context."""

    key: str
    _process_default_lock: RLock = field(
        default_factory=RLock,
        init=False,
        repr=False,
        compare=False,
    )

    def _resolve(self) -> RLock:
        binding = _current_runtime_binding(create=True)
        assert binding is not None
        factory = RLock if binding.scoped else lambda: self._process_default_lock
        return binding.registry.resolve_or_create(self.key, factory)

    def bind(self) -> RLock:
        """Bind the concrete lock before work crosses a thread boundary."""

        return self._resolve()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        return self._resolve().acquire(blocking, timeout)

    def release(self) -> None:
        self._resolve().release()

    def __enter__(self) -> "RuntimeLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()


def runtime_lock(key: str) -> RuntimeLock:
    """Create one module-scope descriptor for a runtime-owned lock key."""

    return RuntimeLock(f"runtime.lock.{key}")


@dataclass(frozen=True, slots=True)
class RuntimeState:
    """Immutable descriptor for mutable state owned by a runtime context."""

    key: str
    factory: Callable[[], Any]

    def resolve(self) -> Any:
        registry = current_runtime_values(create=True)
        assert registry is not None
        return registry.resolve_or_create(self.key, self.factory)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.resolve(), name)

    def __getitem__(self, key: Any) -> Any:
        return self.resolve()[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self.resolve()[key] = value

    def __delitem__(self, key: Any) -> None:
        del self.resolve()[key]

    def __iter__(self):
        return iter(self.resolve())

    def __len__(self) -> int:
        return len(self.resolve())

    def __contains__(self, item: Any) -> bool:
        return item in self.resolve()

    def __bool__(self) -> bool:
        return bool(self.resolve())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RuntimeState):
            other = other.resolve()
        return bool(self.resolve() == other)


def runtime_state(key: str, factory: Callable[[], Any]) -> RuntimeState:
    """Create a descriptor whose mutable value is runtime-context owned."""

    return RuntimeState(f"runtime.state.{key}", factory)


@dataclass(frozen=True, slots=True)
class _RuntimeValueBinding:
    registry: RuntimeValueRegistry
    scoped: bool


_active_runtime_values: ContextVar[_RuntimeValueBinding | None] = ContextVar(
    "okto_pulse_active_runtime_values",
    default=None,
)


def _current_runtime_binding(
    *,
    create: bool = False,
) -> _RuntimeValueBinding | None:
    binding = _active_runtime_values.get()
    if binding is None and create:
        binding = _RuntimeValueBinding(RuntimeValueRegistry(), scoped=False)
        _active_runtime_values.set(binding)
    return binding


def current_runtime_values(*, create: bool = False) -> RuntimeValueRegistry | None:
    binding = _current_runtime_binding(create=create)
    return binding.registry if binding is not None else None


def snapshot_runtime_values() -> RuntimeValueRegistry:
    registry = current_runtime_values()
    return registry.copy() if registry is not None else RuntimeValueRegistry()


def capture_runtime_values_for_tests() -> RuntimeValueRegistry:
    """Capture the complete current registry for deterministic test restoration."""

    return snapshot_runtime_values()


def restore_runtime_values_for_tests(registry: RuntimeValueRegistry) -> None:
    """Replace the current test context with a previously captured registry."""

    _active_runtime_values.set(_RuntimeValueBinding(registry.copy(), scoped=True))


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
    binding = _current_runtime_binding()
    if binding is None:
        return
    if keys:
        binding.registry.discard(*keys)
    else:
        _active_runtime_values.set(
            _RuntimeValueBinding(RuntimeValueRegistry(), scoped=binding.scoped)
        )


@contextmanager
def runtime_value_scope(
    registry: RuntimeValueRegistry,
) -> Iterator[RuntimeValueRegistry]:
    token = _active_runtime_values.set(_RuntimeValueBinding(registry, scoped=True))
    try:
        yield registry
    finally:
        _active_runtime_values.reset(token)


__all__ = [
    "RuntimeLock",
    "RuntimeState",
    "RuntimeValueRegistry",
    "capture_runtime_values_for_tests",
    "current_runtime_values",
    "register_runtime_value",
    "require_runtime_value",
    "reset_runtime_values",
    "restore_runtime_values_for_tests",
    "resolve_runtime_value",
    "runtime_value_scope",
    "runtime_lock",
    "runtime_state",
    "snapshot_runtime_values",
]
