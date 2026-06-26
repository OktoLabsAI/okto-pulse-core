"""Repository ports for the migrated aggregates (SaaS Refactor spec #04).

One Protocol per aggregate the first-cut (spec #09) flows traverse — Board
(create_board), Ideation (move_ideation), Spec (submit_spec_validation). Methods
are domain operations; ``select()``/``AsyncSession`` never appear in the
signature. Return types are the existing ORM models as an EXPLICIT, registered
transitional debt (fr_802078e4 / tr_cd0631cf) — see
``okto_pulse.core.repositories.debt``; the domain/ORM split is a later axis.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.models.db import Board, Ideation, Spec


@runtime_checkable
class BoardRepository(Protocol):
    """Persistence port for the Board aggregate."""

    async def get(self, board_id: str) -> Board | None:
        ...

    async def add(self, board: Board) -> None:
        ...


@runtime_checkable
class IdeationRepository(Protocol):
    """Persistence port for the Ideation aggregate."""

    async def get(self, ideation_id: str) -> Ideation | None:
        ...

    async def add(self, ideation: Ideation) -> None:
        ...


@runtime_checkable
class SpecRepository(Protocol):
    """Persistence port for the Spec aggregate."""

    async def get(self, spec_id: str) -> Spec | None:
        ...

    async def add(self, spec: Spec) -> None:
        ...


@runtime_checkable
class RepositoryCatalog(Protocol):
    """Typed access to the repositories grouped by a unit of work.

    Grows one attribute per migrated aggregate; the first cut exposes the three
    aggregates the spec #09 flows touch.
    """

    boards: BoardRepository
    ideations: IdeationRepository
    specs: SpecRepository
