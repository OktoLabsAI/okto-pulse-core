"""Persistence mutation helpers used by application use cases."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm.attributes import flag_modified

__all__ = ["mark_mutable_field_modified"]


def mark_mutable_field_modified(entity: Any, field_name: str) -> None:
    """Mark a mutable ORM-backed field as dirty without exposing ORM imports upstream."""
    flag_modified(entity, field_name)
