"""Tenant scope value and fail-closed isolation outcomes."""

from __future__ import annotations

from dataclasses import dataclass

LOCAL_REALM_ID = "local"


class MissingRealmScope(ValueError):
    """A tenant-owned operation was invoked without an explicit realm."""


class RealmIsolationViolation(LookupError):
    """A resource is absent from the caller's realm.

    The bounded message deliberately does not disclose whether the identifier
    exists in another realm.
    """

    def __init__(self) -> None:
        super().__init__("tenant_resource_not_found")


@dataclass(frozen=True, slots=True)
class RealmScope:
    """Non-optional tenant boundary carried by application operations."""

    realm_id: str
    is_local: bool = False

    def __post_init__(self) -> None:
        normalized = str(self.realm_id).strip()
        if not normalized:
            raise MissingRealmScope("realm_scope_required")
        if self.is_local and normalized != LOCAL_REALM_ID:
            raise ValueError("local_realm_scope_must_use_local_id")
        object.__setattr__(self, "realm_id", normalized)

    @classmethod
    def local(cls) -> "RealmScope":
        return cls(LOCAL_REALM_ID, is_local=True)

    @classmethod
    def tenant(cls, realm_id: str) -> "RealmScope":
        return cls(realm_id, is_local=False)


def require_realm_scope(value: RealmScope | None) -> RealmScope:
    if not isinstance(value, RealmScope):
        raise MissingRealmScope("realm_scope_required")
    return value


__all__ = [
    "LOCAL_REALM_ID",
    "MissingRealmScope",
    "RealmIsolationViolation",
    "RealmScope",
    "require_realm_scope",
]
