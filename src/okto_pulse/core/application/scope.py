"""Transport-neutral actor and query scoping contracts.

This module is intentionally pure application code: it carries identity, realm,
and board visibility facts as data so inbound adapters can translate their
runtime-specific context before the service layer builds persistence queries.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from okto_pulse.core.domain.realm import (
    LOCAL_REALM_ID,
    RealmScope,
    require_realm_scope,
)


def _freeze_permissions(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return MappingProxyType(dict(value))
    return value


def _freeze_board_ids(value: set[str] | frozenset[str] | list[str] | tuple[str, ...] | None) -> frozenset[str] | None:
    if value is None:
        return None
    return frozenset(str(item) for item in value if item)


@dataclass(frozen=True, slots=True)
class QueryScope:
    """Board query constraints authorized for an actor.

    ``require_ownership`` preserves the existing owner-only REST semantics unless
    an adapter/use case intentionally supplies a non-owner allowed-board set.
    ``allow_all_boards`` is the explicit sentinel for admin/operator all-board
    reads; absent ``allowed_board_ids`` is never treated as all-boards by direct
    board checks.
    """

    actor_id: str
    source: str
    actor_name: str | None = None
    realm_id: str | None = None
    realm_scope: RealmScope | None = None
    target_board_id: str | None = None
    allowed_board_ids: frozenset[str] | None = None
    require_ownership: bool = True
    allow_all_boards: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_id", str(self.actor_id))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "allowed_board_ids", _freeze_board_ids(self.allowed_board_ids))
        scope = self.realm_scope
        if scope is None and self.realm_id:
            scope = RealmScope.local() if self.realm_id == LOCAL_REALM_ID else RealmScope.tenant(self.realm_id)
        object.__setattr__(self, "realm_scope", scope)
        object.__setattr__(self, "realm_id", scope.realm_id if scope else self.realm_id)

    def with_target_board(self, board_id: str | None) -> "QueryScope":
        return QueryScope(
            actor_id=self.actor_id,
            source=self.source,
            actor_name=self.actor_name,
            realm_id=self.realm_id,
            realm_scope=self.realm_scope,
            target_board_id=board_id,
            allowed_board_ids=self.allowed_board_ids,
            require_ownership=self.require_ownership,
            allow_all_boards=self.allow_all_boards,
        )

    def allows_board_id(self, board_id: str | None) -> bool:
        if board_id is None:
            return False
        if self.allow_all_boards:
            return True
        if self.allowed_board_ids is None:
            return False
        return board_id in self.allowed_board_ids


@dataclass(frozen=True, slots=True)
class ActorScope:
    """Transport-neutral actor facts used to derive query scopes."""

    actor_id: str
    source: str
    actor_name: str | None = None
    board_id: str | None = None
    realm_id: str | None = None
    realm_scope: RealmScope | None = None
    permissions: Any = None
    roles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_id", str(self.actor_id))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "permissions", _freeze_permissions(self.permissions))
        object.__setattr__(self, "roles", tuple(self.roles or ()))
        scope = self.realm_scope
        if scope is None and self.realm_id:
            scope = RealmScope.local() if self.realm_id == LOCAL_REALM_ID else RealmScope.tenant(self.realm_id)
        object.__setattr__(self, "realm_scope", scope)
        object.__setattr__(self, "realm_id", scope.realm_id if scope else self.realm_id)

    @classmethod
    def from_context(cls, actor: Any) -> "ActorScope":
        return cls(
            actor_id=getattr(actor, "actor_id"),
            source=getattr(actor, "source", "system"),
            actor_name=getattr(actor, "actor_name", None),
            board_id=getattr(actor, "board_id", None),
            realm_id=getattr(actor, "realm_id", None),
            realm_scope=getattr(actor, "realm_scope", None),
            permissions=getattr(actor, "permissions", None),
            roles=getattr(actor, "roles", ()),
        )

    def query_scope(
        self,
        *,
        target_board_id: str | None = None,
        allowed_board_ids: set[str] | frozenset[str] | list[str] | tuple[str, ...] | None = None,
        require_ownership: bool = True,
        allow_all_boards: bool = False,
    ) -> QueryScope:
        if allow_all_boards and not self.has_global_board_scope_capability():
            raise PermissionError("Global board scope requires an admin/operator capability")
        realm_scope = require_realm_scope(self.realm_scope)
        return QueryScope(
            actor_id=self.actor_id,
            source=self.source,
            actor_name=self.actor_name,
            realm_id=realm_scope.realm_id,
            realm_scope=realm_scope,
            target_board_id=target_board_id if target_board_id is not None else self.board_id,
            allowed_board_ids=_freeze_board_ids(allowed_board_ids),
            require_ownership=require_ownership,
            allow_all_boards=allow_all_boards,
        )

    def has_global_board_scope_capability(self) -> bool:
        role_set = {str(role).lower() for role in self.roles}
        if {"admin", "operator"} & role_set:
            return True
        permissions = self.permissions
        if isinstance(permissions, Mapping):
            for key in (
                "*",
                "admin",
                "operator",
                "board.read.all",
                "boards.read.all",
                "board.global",
                "boards.global",
                "kg.admin.historical_consolidation",
            ):
                if permissions.get(key) is True:
                    return True
        checker = getattr(permissions, "check", None)
        if callable(checker):
            for key in ("board.read.all", "boards.global", "kg.admin.historical_consolidation"):
                try:
                    checker(key)
                    return True
                except Exception:
                    continue
        return False
