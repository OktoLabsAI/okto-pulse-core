"""Shared FastAPI inbound dependencies (SaaS Refactor spec #04; R01B FR3).

``get_unit_of_work`` is the request-scoped persistence dependency for migrated
REST flows. R01B (FR3 / TR4): the core no longer constructs the relational
concrete — it resolves the edition-owned ``UnitOfWorkFactory`` from the runtime
composition (``request.app.state.runtime_composition.uow_factory``) or the
process-level registry seam (:mod:`okto_pulse.core.runtime_registry`), then wraps
the FastAPI request session (``get_db``) via ``.wrap(db)``. Binding the wrapped
session keeps the ``dependency_overrides[get_db]`` test hook + the request
transaction scope intact (the use case commits via the UoW; ``get_db`` owns the
session close). Fail-closed: if no edition provider is registered the resolution
raises — there is NO implicit fallback to a core ``SQLAlchemyUnitOfWork``.
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.database import get_db
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.repositories import PulseUnitOfWork
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory


def scheduler_control_from_request(request: Request) -> SchedulerControl | None:
    """Return the edition-owned scheduler port from the request composition."""
    composition = getattr(request.app.state, "runtime_composition", None)
    return (
        getattr(composition, "scheduler_control", None)
        if composition is not None
        else None
    )


async def get_unit_of_work(
    request: Request, db: AsyncSession = Depends(get_db)
) -> PulseUnitOfWork:
    """Provide a request-scoped :class:`PulseUnitOfWork` over the request session.

    Resolves the edition ``uow_factory`` (composition-preferred, else the
    process-level seam) and wraps the ``Depends(get_db)`` session; the UoW does
    NOT own the session lifecycle (``get_db`` closes it), preserving the request
    transaction scope and the ``dependency_overrides[get_db]`` hook. Fail-closed
    when no provider is registered (R01B FR3 — no implicit core concrete).
    """
    composition = getattr(request.app.state, "runtime_composition", None)
    preferred = (
        getattr(composition, "uow_factory", None) if composition is not None else None
    )
    try:
        factory = resolve_unit_of_work_factory(preferred=preferred)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "persistence_provider_not_configured",
                "message": str(exc),
            },
        ) from exc
    return factory.wrap(db)
