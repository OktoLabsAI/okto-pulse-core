"""Shared MCP projection for reversible-cancellation audit fields."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.datetime_utils import isoformat_utc


def project_cancellation(entity: Any) -> dict[str, Any]:
    """Return the public cancellation record without assuming an ORM type."""

    cancelled_at = getattr(entity, "cancelled_at", None)
    return {
        "cancellation_reason": getattr(entity, "cancellation_reason", None),
        "cancelled_by": getattr(entity, "cancelled_by", None),
        "cancelled_at": isoformat_utc(cancelled_at),
    }


__all__ = ["project_cancellation"]
