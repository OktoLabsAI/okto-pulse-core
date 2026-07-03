"""Transport-neutral actor and query scoping contracts.

This module is intentionally pure application code: it carries identity, realm,
and board visibility facts as data so inbound adapters can translate their
runtime-specific context before the service layer builds persistence queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(value or {}))


def _freeze_board_ids(value: set[str] | frozenset[str] | list[str] | tuple[str, ...] | None) -> frozenset[str] | None:
    if value is None:
        return None
    return frozenset(str(item) for item in value if item)


@dataclass(frozen=True, slots=True)
class QueryScope:
    """Board query constraints authorized for an actor.

    ``require_ownership`` preserves the existing owner-only REST semantics unless
    an adapter/use case intentionally supplies a non-owner allowed-board set.
    """

    actor_id: str
    source: str
    actor_name: str | None = None
    realm_id: str | None = None
    target_board_id: str | None = None
    allowed_board_ids: frozenset[str] | None = None
    require_ownership: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_id", str(self.actor_id))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "allowed_board_ids", _freeze_board_ids(self.allowed_board_ids))

    def with_target_board(self, board_id: str | None) -> "QueryScope":
        return QueryScope(
            actor_id=self.actor_id,
            source=self.source,
            actor_name=self.actor_name,
            realm_id=self.realm_id,
            target_board_id=board_id,
            allowed_board_ids=self.allowed_board_ids,
            require_ownership=self.require_ownership,
        )

    def allows_board_id(self, board_id: str | None) -> bool:
        if board_id is None or self.allowed_board_ids is None:
            return True
        return board_id in self.allowed_board_ids


@dataclass(frozen=True, slots=True)
class ActorScope:
    """Transport-neutral actor facts used to derive query scopes."""

    actor_id: str
    source: str
    actor_name: str | None = None
    board_id: str | None = None
    realm_id: str | None = None
    permissions: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    roles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "actor_id", str(self.actor_id))
        object.__setattr__(self, "source", str(self.source))
        object.__setattr__(self, "permissions", _freeze_mapping(self.permissions))
        object.__setattr__(self, "roles", tuple(self.roles or ()))

    @classmethod
    def from_context(cls, actor: Any) -> "ActorScope":
        return cls(
            actor_id=getattr(actor, "actor_id"),
            source=getattr(actor, "source", "system"),
            actor_name=getattr(actor, "actor_name", None),
            board_id=getattr(actor, "board_id", None),
            realm_id=getattr(actor, "realm_id", None),
            permissions=getattr(actor, "permissions", None),
            roles=getattr(actor, "roles", ()),
        )

    def query_scope(
        self,
        *,
        target_board_id: str | None = None,
        allowed_board_ids: set[str] | frozenset[str] | list[str] | tuple[str, ...] | None = None,
        require_ownership: bool = True,
    ) -> QueryScope:
        return QueryScope(
            actor_id=self.actor_id,
            source=self.source,
            actor_name=self.actor_name,
            realm_id=self.realm_id,
            target_board_id=target_board_id if target_board_id is not None else self.board_id,
            allowed_board_ids=_freeze_board_ids(allowed_board_ids),
            require_ownership=require_ownership,
        )
