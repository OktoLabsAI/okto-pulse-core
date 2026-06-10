"""Typed Discovery intent parameter schema helpers."""

from __future__ import annotations

from typing import Any, Literal, TypedDict


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
    """Normalize reader-side Discovery params metadata.

    Legacy params without an explicit ``type`` are text params. Unknown/future
    types are preserved so later validation layers can make the reject/handle
    decision without this compatibility helper hiding the original value.
    """
    if not params_schema:
        return None

    normalized: DiscoveryParamsSchema = {}
    for name, raw_meta in params_schema.items():
        meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}
        meta["type"] = meta.get("type") or DISCOVERY_PARAM_TYPE_TEXT
        normalized[name] = meta
    return normalized
