"""Composition slots for edition-owned relational service adapters.

The Core exposes stable application facades, but it neither imports nor builds a
relational implementation. Edition composition must register each adapter
explicitly; missing providers fail closed.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, require_runtime_value, reset_runtime_values

from typing import Any, Protocol

_RESOURCE_GATE_KEY = "ports.relational_services.resource_gate"
_RUNTIME_SETTINGS_KEY = "ports.relational_services.runtime_settings"
_TRACEABILITY_KEY = "ports.relational_services.traceability"


class ResourceGateRelationalAdapter(Protocol):
    async def load_entity_ref(self, board_id: str, entity_type: str, entity_id: str) -> Any: ...
    async def load_parent_refs(self, board_id: str, root: Any) -> list[Any]: ...
    async def collect_refs(self, ref: Any) -> dict[str, list[dict]]: ...
    async def load_active_marks(self, board_id: str, entity_type: str, entity_id: str) -> dict[str, Any]: ...
    def serialize_na_mark(self, mark: Any, *, effective: bool, source: Any | None = None) -> dict | None: ...
    async def save_not_applicable(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        resource_type: str,
        actor_id: str,
        *,
        justification: str | None,
        source_channel: str,
    ) -> str: ...
    async def clear_not_applicable(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        resource_type: str,
        actor_id: str,
        *,
        reason: str,
    ) -> int: ...
    async def hydrate_effective_resource(self, **request: Any) -> dict[str, Any] | None: ...
    async def load_spec_task_cards(self, spec_id: str) -> list[Any]: ...
    async def collect_task_resource_id_coverage(
        self,
        task_cards: list[Any],
    ) -> dict[str, dict[str, set[str]]]: ...


class ResourceGateMetadataLineageAdapter(Protocol):
    """Origin-bounded read capability required by the gate projection.

    ResourceGateService discovers the complete quartet at runtime. A
    ``projection_profile='gate'`` request fails closed when any method is
    unavailable; it never falls back to the body-loading relational reads.
    Metadata refs carry only identity, lineage/effectivity and persisted
    revision fields — never KB/mockup/architecture bodies or computed hashes.
    """

    async def load_entity_ref_metadata(
        self, board_id: str, entity_type: str, entity_id: str
    ) -> Any: ...
    async def load_parent_refs_metadata(
        self, board_id: str, root: Any
    ) -> list[Any]: ...
    async def collect_refs_metadata(self, ref: Any) -> dict[str, list[dict]]: ...
    async def filter_inherited_refs_metadata(
        self,
        root: Any,
        parent: Any,
        refs: dict[str, list[dict]],
    ) -> dict[str, list[dict]]: ...


class ResourceGateAdapterFactory(Protocol):
    def __call__(self, relational_context: object) -> ResourceGateRelationalAdapter: ...


def register_resource_gate_adapter_factory(
    factory: ResourceGateAdapterFactory,
) -> None:
    register_runtime_value(_RESOURCE_GATE_KEY, factory)


def resolve_resource_gate_adapter_factory() -> ResourceGateAdapterFactory:
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
    "ResourceGateAdapterFactory",
    "ResourceGateMetadataLineageAdapter",
    "ResourceGateRelationalAdapter",
    "register_resource_gate_adapter_factory",
    "register_runtime_settings_adapter",
    "register_traceability_adapter",
    "reset_relational_service_adapters_for_tests",
    "resolve_resource_gate_adapter_factory",
    "resolve_runtime_settings_adapter",
    "resolve_traceability_adapter",
]
