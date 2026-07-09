"""Small service payloads for application use cases.

Application use cases should not import transport/outbound Pydantic schemas from
``core.models``. Legacy services still expect attribute access and, for update
objects, ``model_dump(exclude_unset=True)``. This adapter keeps that contract
local to the application layer.
"""

from __future__ import annotations

from typing import Any, Iterable


class ServicePayload:
    """Attribute bag with the tiny subset of Pydantic's dump API services use."""

    def __init__(self, **fields: Any) -> None:
        self._fields = tuple(fields)
        for key, value in fields.items():
            setattr(self, key, value)

    def model_dump(self, *, exclude_unset: bool = False, **_: Any) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self._fields}


def payload(**fields: Any) -> ServicePayload:
    return ServicePayload(**fields)


def payload_choices(choices: Iterable[Any] | None) -> list[Any] | None:
    """Normalize choice dicts into objects exposing ``model_dump``."""

    if choices is None:
        return None
    normalized: list[Any] = []
    for choice in choices:
        if hasattr(choice, "model_dump"):
            normalized.append(choice)
        elif isinstance(choice, dict):
            normalized.append(ServicePayload(**choice))
        else:
            normalized.append(choice)
    return normalized


__all__ = ["ServicePayload", "payload", "payload_choices"]
