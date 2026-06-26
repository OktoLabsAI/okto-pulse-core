"""SQLAlchemy Community adapters for the repository ports (spec #04, tr_7b951e01).

Each repository receives an ``AsyncSession`` by composition and never exposes it
to the caller. Returns the existing ORM models (registered transitional debt —
see ``okto_pulse.core.repositories.debt``); the relational semantics are
unchanged from the current services (fr_3a4879be).

``get(...)`` after a pending ``add(...)`` in the same transaction relies on the
session's autoflush (the Community ``session_factory`` uses the SQLAlchemy
default ``autoflush=True``). A downstream adapter that disables autoflush must
flush explicitly before reading to preserve read-after-write semantics.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import Board, Ideation, Spec


class SQLAlchemyBoardRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, board_id: str) -> Board | None:
        result = await self._session.execute(select(Board).where(Board.id == board_id))
        return result.scalar_one_or_none()

    async def add(self, board: Board) -> None:
        self._session.add(board)


class SQLAlchemyIdeationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, ideation_id: str) -> Ideation | None:
        result = await self._session.execute(
            select(Ideation).where(Ideation.id == ideation_id)
        )
        return result.scalar_one_or_none()

    async def add(self, ideation: Ideation) -> None:
        self._session.add(ideation)


class SQLAlchemySpecRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, spec_id: str) -> Spec | None:
        result = await self._session.execute(select(Spec).where(Spec.id == spec_id))
        return result.scalar_one_or_none()

    async def add(self, spec: Spec) -> None:
        self._session.add(spec)
