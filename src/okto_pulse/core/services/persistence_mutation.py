"""Persistence mutation helpers used by application use cases."""

from __future__ import annotations

from typing import Any

__all__ = ["mark_mutable_field_modified"]


def mark_mutable_field_modified(entity: Any, field_name: str) -> None:
    """Replace a mutable field with an equal copy to expose the mutation."""
    value = getattr(entity, field_name)
    if isinstance(value, dict):
        value = dict(value)
    elif isinstance(value, list):
        value = list(value)
    elif isinstance(value, set):
        value = set(value)
    setattr(entity, field_name, value)
