"""Test-owned access to native Community Global Discovery mechanics."""

from okto_pulse.community.adapters.global_discovery_schema import (
    ensure_decision_digest_layer_column,
)
from okto_pulse.core.kg.global_discovery.schema import GLOBAL_SCHEMA_VERSION
from okto_pulse.core.kg.interfaces.registry import get_kg_registry


def _runtime():
    return get_kg_registry().require_global_discovery_runtime()


def bootstrap_global_discovery():
    runtime = _runtime()
    runtime.bootstrap()
    locator = getattr(runtime, "_global_graph_path", None)
    return locator() if callable(locator) else runtime.state().storage_ref


def open_global_connection():
    runtime = _runtime()
    native = getattr(runtime, "_open_native", None)
    if not callable(native):
        raise RuntimeError("native_global_graph_scope_unavailable")
    return native()


def ensure_global_discovery_layer_schema():
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
    runtime.purge(reason=reason)
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
