"""Resource Gate service facade.

The SQLAlchemy-backed implementation is isolated behind the explicit adapter in
``okto_pulse.core.repositories.sqlalchemy.resource_gate_service``. This module
keeps the historical public import path while avoiding eager adapter imports
during core package startup.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.services.resource_gate_contracts import (
    ENTITY_TYPES,
    RESOURCE_TYPES,
    SOURCE_CHANNELS,
    EntityType,
    ResourceGateError,
    ResourceGateJustificationRequired,
    ResourceGateNotFound,
    ResourceGateViolation,
    ResourceState,
    ResourceType,
    SourceChannel,
)
from okto_pulse.core.services.resource_lineage import (
    ResourceLineageError,
    ResolvedResourceLineageService,
)

_IMPL_CLS: type | None = None


def _build_impl_class() -> type:
    global _IMPL_CLS
    if _IMPL_CLS is not None:
        return _IMPL_CLS

    from okto_pulse.core.repositories import sqlalchemy_resource_gate_service_class

    SQLAlchemyResourceGateService = sqlalchemy_resource_gate_service_class()

    class _ResolvedResourceGateService(SQLAlchemyResourceGateService):
        """Compatibility facade over the adapter-owned implementation."""

        async def _resolve_resource_lineage(
            self,
            board_id: str,
            entity_type: str,
            entity_id: str,
            *,
            include_coverage: bool,
            projection_profile: str,
        ):
            try:
                return await ResolvedResourceLineageService(self).resolve(
                    board_id,
                    entity_type,
                    entity_id,
                    include_coverage=include_coverage,
                    projection_profile=projection_profile,
                )
            except ResourceLineageError as exc:
                raise ResourceGateViolation(
                    "resource_lineage_resolution_failed",
                    (
                        "Resource Gate could not resolve resource lineage for "
                        f"{entity_type} '{entity_id}': {exc}"
                    ),
                    details={
                        "board_id": board_id,
                        "entity_type": entity_type,
                        "entity_id": entity_id,
                        "lineage_error_code": exc.code,
                        "lineage_error_details": exc.details,
                    },
                ) from exc

        async def _load_parent_refs(self, *args, **kwargs):
            return await super()._load_parent_refs(*args, **kwargs)

        async def _load_active_marks(self, *args, **kwargs):
            return await super()._load_active_marks(*args, **kwargs)

        @staticmethod
        def _resolve_state(direct, inherited, na_mark):
            if list(direct) or list(inherited):
                return "provided"
            if na_mark is not None:
                return "not_applicable"
            return "missing"

    _IMPL_CLS = _ResolvedResourceGateService
    return _IMPL_CLS


class _ResourceGateServiceMeta(type):
    def __getattr__(cls, name: str) -> Any:
        return getattr(_build_impl_class(), name)


class ResourceGateService(metaclass=_ResourceGateServiceMeta):
    """Lazy facade that instantiates the adapter-owned Resource Gate service."""

    def __new__(cls, *args, **kwargs):
        return _build_impl_class()(*args, **kwargs)


__all__ = [
    "ENTITY_TYPES",
    "RESOURCE_TYPES",
    "SOURCE_CHANNELS",
    "EntityType",
    "ResourceGateError",
    "ResourceGateJustificationRequired",
    "ResourceGateNotFound",
    "ResourceGateService",
    "ResourceGateViolation",
    "ResolvedResourceLineageService",
    "ResourceState",
    "ResourceType",
    "SourceChannel",
]
