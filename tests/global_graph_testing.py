"""Test-owned Global Discovery lifecycle and statement helpers."""

from pathlib import Path

from okto_pulse.core.kg.global_discovery.schema import GLOBAL_SCHEMA_VERSION
from okto_pulse.core.kg.global_discovery_writer import (
    global_discovery_writer_scope,
)

__all__ = [
    "GLOBAL_SCHEMA_VERSION",
    "bootstrap_global_discovery",
    "execute_global_read",
    "execute_global_write",
    "ensure_global_discovery_layer_schema",
    "global_discovery_graph_path",
    "purge_global_discovery_storage",
    "reset_global_discovery_runtime_for_tests",
]


def _runtime():
    from okto_pulse.core.kg import interfaces

    return interfaces.get_kg_registry().require_global_discovery_runtime()


def _ensure_test_write_lock_port() -> None:
    """Install the test coordination fake before module-scoped fixtures run."""

    from coordination_fakes import FakeWriteLockPort
    from okto_pulse.core.ports.coordination import (
        CoordinationProviderMissing,
        get_write_lock_port,
        register_coordination_providers,
    )

    try:
        get_write_lock_port()
    except CoordinationProviderMissing:
        register_coordination_providers(write_lock_port=FakeWriteLockPort())


def bootstrap_global_discovery():
    runtime = _runtime()
    # Production Global Discovery mutations now require the durable edition
    # writer lease in addition to the ContextVar write barrier.  Keep the
    # shared test helper on the same public ownership path so every fixture
    # that bootstraps native storage exercises the real fail-closed contract.
    _ensure_test_write_lock_port()
    with global_discovery_writer_scope(operation="test_global_discovery_bootstrap"):
        result = runtime.bootstrap()
    if isinstance(result, Path):
        return result
    locator = getattr(runtime, "_global_graph_path", None)
    return locator() if callable(locator) else runtime.state().storage_ref


def execute_global_read(statement: str, params: dict | None = None):
    """Execute a test read through the lifecycle-safe public runtime port."""

    return _runtime().execute(statement, params)


def execute_global_write(
    statement: str,
    params: dict | None = None,
    *,
    operation: str,
):
    """Execute test-only fixture DML while owning the durable global fence."""

    _ensure_test_write_lock_port()
    with global_discovery_writer_scope(operation=operation):
        return _runtime().execute(statement, params)


def ensure_global_discovery_layer_schema():
    _ensure_test_write_lock_port()
    with global_discovery_writer_scope(
        operation="test_global_discovery_ensure_layer_schema"
    ):
        return _runtime().ensure_layer_schema()


def purge_global_discovery_storage(*, reason: str = "manual"):
    runtime = _runtime()
    locator = getattr(runtime, "_global_graph_path", None)
    targets = []
    if callable(locator):
        primary = locator()
        if primary.exists():
            targets.append(primary)
        if primary.parent.exists():
            targets.extend(sorted(primary.parent.glob(primary.name + ".*")))
    _ensure_test_write_lock_port()
    with global_discovery_writer_scope(operation="test_global_discovery_purge"):
        result = runtime.purge(reason=reason)
    if not callable(locator) and isinstance(result, list):
        return result
    return [str(path) for path in targets if not path.exists()]


def reset_global_discovery_runtime_for_tests() -> None:
    try:
        runtime = _runtime()
    except RuntimeError:
        return
    reset = getattr(runtime, "reset_for_tests", None)
    if callable(reset):
        reset()


def global_discovery_graph_path():
    locator = getattr(_runtime(), "_global_graph_path", None)
    if not callable(locator):
        raise RuntimeError("native_global_graph_locator_unavailable")
    return locator()
