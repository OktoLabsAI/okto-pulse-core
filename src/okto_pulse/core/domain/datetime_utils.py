"""Shared datetime normalization at persistence and projection boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def normalize_utc_datetime(value: datetime) -> datetime:
    """Return an aware UTC datetime, treating legacy naive values as UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def isoformat_utc(value: Any) -> Any:
    """Project datetimes with an explicit UTC offset; pass other values through."""

    if isinstance(value, datetime):
        return normalize_utc_datetime(value).isoformat()
    if value is not None and hasattr(value, "isoformat"):
        return value.isoformat()
    return value


__all__ = ["isoformat_utc", "normalize_utc_datetime"]
