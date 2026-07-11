"""Composition slots for edition-owned relational service adapters.

The Core exposes stable application facades, but it neither imports nor builds a
relational implementation. Edition composition must register each adapter
explicitly; missing providers fail closed.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Any

_RESOURCE_GATE_KEY = "ports.relational_services.resource_gate"
_RUNTIME_SETTINGS_KEY = "ports.relational_services.runtime_settings"
_TRACEABILITY_KEY = "ports.relational_services.traceability"


def register_resource_gate_service_class(service_class: type[Any]) -> None:
    register_runtime_value(_RESOURCE_GATE_KEY, service_class)


def resolve_resource_gate_service_class() -> type[Any]:
    return require_runtime_value(_RESOURCE_GATE_KEY, "resource_gate_relational_adapter_not_configured")


def register_runtime_settings_adapter(adapter: Any) -> None:
    register_runtime_value(_RUNTIME_SETTINGS_KEY, adapter)


def resolve_runtime_settings_adapter() -> Any:
    return require_runtime_value(_RUNTIME_SETTINGS_KEY, "runtime_settings_relational_adapter_not_configured")


def register_traceability_adapter(adapter: Any) -> None:
    register_runtime_value(_TRACEABILITY_KEY, adapter)


def resolve_traceability_adapter() -> Any:
    return require_runtime_value(_TRACEABILITY_KEY, "traceability_relational_adapter_not_configured")


def reset_relational_service_adapters_for_tests() -> None:
    reset_runtime_values(_RESOURCE_GATE_KEY, _RUNTIME_SETTINGS_KEY, _TRACEABILITY_KEY)


__all__ = [
    "register_resource_gate_service_class",
    "register_runtime_settings_adapter",
    "register_traceability_adapter",
    "reset_relational_service_adapters_for_tests",
    "resolve_resource_gate_service_class",
    "resolve_runtime_settings_adapter",
    "resolve_traceability_adapter",
]
