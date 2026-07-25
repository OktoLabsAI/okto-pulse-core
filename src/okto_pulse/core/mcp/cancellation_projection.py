"""Shared MCP projection for reversible-cancellation audit fields."""

from __future__ import annotations

from typing import Any


def project_cancellation(entity: Any) -> dict[str, Any]:
    """Return the public cancellation record without assuming an ORM type."""

    cancelled_at = getattr(entity, "cancelled_at", None)
    return {
        "cancellation_reason": getattr(entity, "cancellation_reason", None),
        "cancelled_by": getattr(entity, "cancelled_by", None),
        "cancelled_at": (
            cancelled_at.isoformat()
            if cancelled_at is not None and hasattr(cancelled_at, "isoformat")
            else cancelled_at
        ),
    }


__all__ = ["project_cancellation"]
