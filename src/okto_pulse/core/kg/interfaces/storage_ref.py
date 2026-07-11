"""Opaque graph storage references shared by neutral Core ports."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StorageRef:
    """Stable opaque token that Core may carry but never dereference."""

    token: str
    namespace: str = "graph"

    def __post_init__(self) -> None:
        token = str(self.token).strip()
        namespace = str(self.namespace).strip()
        if not token or not namespace:
            raise ValueError("storage_ref_requires_token_and_namespace")
        object.__setattr__(self, "token", token)
        object.__setattr__(self, "namespace", namespace)


__all__ = ["StorageRef"]
