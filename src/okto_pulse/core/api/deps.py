"""Shared FastAPI inbound dependencies (SaaS Refactor spec #04).

``get_unit_of_work`` is the request-scoped persistence dependency for migrated
REST flows: it binds a :class:`PulseUnitOfWork` to the FastAPI request session
(``get_db``) so the existing session / dependency overrides keep working and the
request transaction scope is preserved (the use case commits via the UoW). The
standalone ``SQLAlchemyUnitOfWorkFactory`` produced by the same persistence port
serves non-request paths (workers, MCP, SaaS multi-tenant).
"""

from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.database import get_db
from okto_pulse.core.repositories import PulseUnitOfWork, SQLAlchemyUnitOfWork


async def get_unit_of_work(db: AsyncSession = Depends(get_db)) -> PulseUnitOfWork:
    """Provide a request-scoped :class:`PulseUnitOfWork` over the request session.

    Depending on ``get_db`` keeps the FastAPI ``dependency_overrides[get_db]``
    test hook intact and the request transaction scope unchanged; the UoW does
    NOT own the session lifecycle (``get_db`` closes it).
    """
    return SQLAlchemyUnitOfWork(db)
