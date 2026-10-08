"""Typed Discovery intent parameter schema helpers."""

from __future__ import annotations

from typing import Any, Literal

from typing_extensions import TypedDict

DiscoveryParamType = Literal["text", "entity_selector", "spec_child_selector"]

DISCOVERY_PARAM_TYPE_TEXT: DiscoveryParamType = "text"
DISCOVERY_PARAM_TYPE_ENTITY_SELECTOR: DiscoveryParamType = "entity_selector"
DISCOVERY_PARAM_TYPE_SPEC_CHILD_SELECTOR: DiscoveryParamType = "spec_child_selector"

SUPPORTED_DISCOVERY_PARAM_TYPES: tuple[DiscoveryParamType, ...] = (
    DISCOVERY_PARAM_TYPE_TEXT,
    DISCOVERY_PARAM_TYPE_ENTITY_SELECTOR,
    DISCOVERY_PARAM_TYPE_SPEC_CHILD_SELECTOR,
)


class DiscoveryParamSchema(TypedDict, total=False):
    type: DiscoveryParamType
    required: bool
    label: str
    entity_type: str
    child_types: list[str]
    depends_on: list[str]
    options_endpoint: str


DiscoveryParamsSchema = dict[str, dict[str, Any]]


def normalize_discovery_params_schema(
    params_schema: dict[str, Any] | None,
) -> DiscoveryParamsSchema | None:
    """Validate current metadata without inferring missing parameter types."""
    if params_schema is None:
        return None
    if not isinstance(params_schema, dict):
        raise ValueError("incompatible_discovery_params_schema: expected an object")
    if not params_schema:
        return None

    normalized: DiscoveryParamsSchema = {}
    for name, raw_meta in params_schema.items():
        if (
            not isinstance(raw_meta, dict)
            or raw_meta.get("type") not in SUPPORTED_DISCOVERY_PARAM_TYPES
        ):
            raise ValueError(
                "incompatible_discovery_params_schema: explicit supported type required"
            )
        normalized[name] = dict(raw_meta)
    return normalized
