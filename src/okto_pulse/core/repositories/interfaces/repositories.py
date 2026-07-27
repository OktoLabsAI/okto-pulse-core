"""Repository ports for the migrated aggregates (SaaS Refactor spec #04).

One Protocol per migrated aggregate. Methods are domain operations;
``select()``/``AsyncSession`` and mapped classes never appear in this module.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.entities import Board, Ideation, Spec
from okto_pulse.core.domain.realm import RealmScope


@runtime_checkable
class BoardRepository(Protocol):
    """Persistence port for the Board aggregate."""

    realm_scope: RealmScope

    async def get(self, board_id: str) -> Board | None: ...

    async def add(self, board: Board) -> None: ...


@runtime_checkable
class IdeationRepository(Protocol):
    """Persistence port for the Ideation aggregate."""

    realm_scope: RealmScope

    async def get(self, ideation_id: str) -> Ideation | None: ...

    async def add(self, ideation: Ideation) -> None: ...


@runtime_checkable
class SpecRepository(Protocol):
    """Persistence port for the Spec aggregate."""

    realm_scope: RealmScope

    async def get(self, spec_id: str) -> Spec | None: ...

    async def add(self, spec: Spec) -> None: ...


@runtime_checkable
class RepositoryCatalog(Protocol):
    """Typed access to the repositories grouped by a unit of work.

    Grows one attribute per migrated aggregate; the first cut exposes the three
    aggregates the spec #09 flows touch.
    """

    realm_scope: RealmScope
    boards: BoardRepository
    ideations: IdeationRepository
    specs: SpecRepository
