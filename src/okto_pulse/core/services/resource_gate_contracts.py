"""Neutral Resource Gate contracts shared by facades and adapters."""

from __future__ import annotations

from typing import Any, Literal

EntityType = Literal["ideation", "refinement", "spec", "card"]
ResourceType = Literal["architecture", "mockup", "knowledge_base"]
ResourceState = Literal["provided", "not_applicable", "missing"]
SourceChannel = Literal["ui", "api", "mcp"]

ENTITY_TYPES: tuple[str, ...] = ("ideation", "refinement", "spec", "card")
RESOURCE_TYPES: tuple[str, ...] = ("architecture", "mockup", "knowledge_base")
SOURCE_CHANNELS: tuple[str, ...] = ("ui", "api", "mcp")


class ResourceGateError(ValueError):
    """Base exception carrying a stable machine-readable error code."""

    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class ResourceGateNotFound(ResourceGateError):
    """Raised when the target entity does not exist on the requested board."""

    def __init__(self, entity_type: str, entity_id: str, board_id: str):
        super().__init__(
            "entity_not_found",
            f"{entity_type} '{entity_id}' was not found on board '{board_id}'.",
            details={"entity_type": entity_type, "entity_id": entity_id, "board_id": board_id},
        )


class ResourceGateJustificationRequired(ResourceGateError):
    """Raised when API/MCP attempts to mark N/A without justification."""

    def __init__(self, source_channel: str):
        super().__init__(
            "justification_required",
            f"Justification is required when source_channel is '{source_channel}'.",
            details={"source_channel": source_channel},
        )


class ResourceGateViolation(ResourceGateError):
    """Raised when a Resource Gate blocks a transition."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(code, message, details=details)


__all__ = [
    "ENTITY_TYPES",
    "RESOURCE_TYPES",
    "SOURCE_CHANNELS",
    "EntityType",
    "ResourceGateError",
    "ResourceGateJustificationRequired",
    "ResourceGateNotFound",
    "ResourceGateViolation",
    "ResourceState",
    "ResourceType",
    "SourceChannel",
]
