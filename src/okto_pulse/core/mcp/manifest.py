"""Deterministic identity and installed MCP tool inventory manifest."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.mcp.tool_family_registry import ELIGIBLE_FAMILIES
from okto_pulse.core.ports.mcp_resources import (
    FrozenMcpResourceCatalog,
    McpResourceCatalogError,
)


SERVER_MANIFEST_VERSION = "1.0"
TOOL_INVENTORY_CANONICALIZATION = "json-sort-keys-v1"


def installed_tool_names(catalog: Any) -> tuple[str, ...]:
    """Return enabled tool names exactly as exposed by ``tools/list``."""

    tools = getattr(catalog, "iter_tools", lambda: ())()
    return tuple(sorted(tool.name for tool in tools if getattr(tool, "enabled", True)))


def installed_aliases(tool_names: tuple[str, ...]) -> dict[str, str]:
    """Map only aliases that are actually installed to their canonical tool."""

    installed = frozenset(tool_names)
    aliases: dict[str, str] = {}
    for family in ELIGIBLE_FAMILIES:
        canonical = family.consolidated_tool
        if canonical is None or canonical not in installed:
            continue
        for alias in family.legacy_aliases:
            if alias in installed:
                aliases[alias] = canonical
    return dict(sorted(aliases.items()))


def tool_inventory_document(catalog: Any) -> dict[str, Any]:
    names = installed_tool_names(catalog)
    return {
        "tools": list(names),
        "aliases": installed_aliases(names),
    }


def tool_inventory_sha256(catalog_or_document: Any) -> str:
    """Hash the canonical installed-name + alias inventory."""

    if isinstance(catalog_or_document, Mapping) and {
        "tools",
        "aliases",
    }.issubset(catalog_or_document):
        document = dict(catalog_or_document)
    else:
        document = tool_inventory_document(catalog_or_document)
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_identity() -> dict[str, str]:
    return {
        "id": os.getenv("OKTO_PULSE_BUILD_ID", "unknown"),
        "commit": os.getenv(
            "OKTO_PULSE_BUILD_COMMIT",
            os.getenv("GIT_COMMIT", os.getenv("SOURCE_VERSION", "unknown")),
        ),
    }


def build_server_manifest(
    catalog: Any,
    *,
    include_tool_names: bool = False,
) -> dict[str, Any]:
    """Build a compact manifest whose hash can be checked against tools/list."""

    document = tool_inventory_document(catalog)
    inventory: dict[str, Any] = {
        "count": len(document["tools"]),
        "sha256": tool_inventory_sha256(document),
        "canonicalization": TOOL_INVENTORY_CANONICALIZATION,
        "aliases": document["aliases"],
    }
    if include_tool_names:
        inventory["tools"] = document["tools"]
    return {
        "manifest_version": SERVER_MANIFEST_VERSION,
        "server": {
            "name": str(getattr(catalog, "name", "okto-pulse")),
            "version": str(getattr(catalog, "version", "unknown")),
            "build": _build_identity(),
        },
        "tool_inventory": inventory,
    }


def render_server_manifest(catalog: Any) -> str:
    return json.dumps(
        build_server_manifest(catalog),
        sort_keys=True,
        separators=(",", ":"),
    )


def build_resource_manifest(catalog: Any) -> dict[str, Any]:
    """Return the canonical manifest for an already-frozen resource projection."""

    if not isinstance(catalog, FrozenMcpResourceCatalog):
        raise McpResourceCatalogError(
            "registry_unfrozen",
            "resource manifest requires a frozen effective projection",
        )
    return catalog.manifest.as_dict()


def render_resource_manifest(catalog: Any) -> str:
    """Render the canonical frozen-resource manifest as UTF-8 JSON text."""

    if not isinstance(catalog, FrozenMcpResourceCatalog):
        raise McpResourceCatalogError(
            "registry_unfrozen",
            "resource manifest requires a frozen effective projection",
        )
    return catalog.manifest.render()


__all__ = [
    "SERVER_MANIFEST_VERSION",
    "TOOL_INVENTORY_CANONICALIZATION",
    "build_server_manifest",
    "build_resource_manifest",
    "installed_aliases",
    "installed_tool_names",
    "render_server_manifest",
    "render_resource_manifest",
    "tool_inventory_document",
    "tool_inventory_sha256",
]
