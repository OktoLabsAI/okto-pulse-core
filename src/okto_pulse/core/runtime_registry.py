"""Process-level registry for the edition-owned relational UnitOfWorkFactory
(SaaS Refactor R01B REPLAN-IMP2, FR3 / TR4).

The inbound consumers — REST ``api/deps.get_unit_of_work`` and MCP
``mcp/server.get_unit_of_work_factory_for_mcp`` — must resolve the relational
UnitOfWork provider WITHOUT the core importing or constructing the concrete
SQLAlchemy adapter (TR4: the dependency direction is core-contracts ->
Community-adapters, never the reverse). The edition composition root registers
ITS concrete factory here at boot — the SAME object it also exposes on
``app.state.runtime_composition.uow_factory`` — so the core resolves the edition
provider through this seam.

Register-before-remove, FAIL-CLOSED: this is an EXPLICIT, RESETTABLE registration
(``register_unit_of_work_factory`` / ``reset_unit_of_work_factory``). There is NO
default that constructs a core ``SQLAlchemyUnitOfWorkFactory`` — a consumer that
resolves before any edition registered gets a structured ``RuntimeError`` (never
an implicit core concrete in production). The test harness (``tests/conftest``)
may register a CORE-ONLY provider as EXPLICIT test/transitional-compat wiring;
that is test wiring, NOT a runtime fallback.

This module global is ledgered in
``application/boundary/singleton_gate`` (``SINGLETON_LEDGER`` +
``BASELINE_SINGLETONS``) so the ``AntiSingletonGate`` keeps it register-before-
remove (owner ``okto-pulse-core/runtime``, target provider ``uow_factory``,
retirement = R01C when the core relational concretes are physically removed).
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

#: The edition-registered relational UnitOfWorkFactory (None until a composition
#: root — or the test harness — registers one). NO implicit default.
_UOW_KEY = "runtime.unit_of_work_factory"
_CONTENT_KEY = "runtime.content_ingestion_resolver"
_RELATIONAL_KEY = "runtime.relational_runtime_factory"


def register_unit_of_work_factory(factory: Any) -> None:
    """Register the edition-owned relational ``UnitOfWorkFactory`` (R01B FR3).

    Called once by the edition composition root (Community ``main.py``) with the
    same object it puts on ``app.state.runtime_composition.uow_factory``. Last
    writer wins so a re-composed app / test can re-point the seam. ``factory`` is
    duck-typed: it must expose ``wrap(session)`` (request-scoped REST path) and be
    callable for the MCP / worker fresh-session path."""
    register_runtime_value(_UOW_KEY, factory)


def reset_unit_of_work_factory() -> None:
    """Drop the registered factory (test isolation / teardown). After a reset a
    resolve fails closed until something registers again — there is no fallback."""
    reset_runtime_values(_UOW_KEY)


def is_unit_of_work_factory_registered() -> bool:
    """Whether an edition provider is currently registered on the seam."""
    return resolve_runtime_value(_UOW_KEY) is not None


def resolve_unit_of_work_factory(*, preferred: Any = None) -> Any:
    """Return the relational ``UnitOfWorkFactory``, FAIL-CLOSED.

    ``preferred`` lets the REST request path pass the per-app composition provider
    (``request.app.state.runtime_composition.uow_factory``) so a composed app wins
    over the process-global seam. When neither ``preferred`` nor the registered
    seam is available, raise — there is NO implicit fallback to a core concrete
    (R01B FR3 / AC4 fail-closed)."""
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    scoped = composition.uow_factory if composition is not None else None
    factory = preferred if preferred is not None else scoped
    if factory is None:
        factory = resolve_runtime_value(_UOW_KEY)
    if factory is None:
        raise RuntimeError(
            "No relational UnitOfWorkFactory provider is registered (R01B FR3 "
            "fail-closed). The edition composition root must register one via "
            "okto_pulse.core.runtime_registry.register_unit_of_work_factory(); the "
            "core no longer constructs a SQLAlchemyUnitOfWorkFactory fallback."
        )
    return factory


# ---------------------------------------------------------------------------
# MCP content ingestion resolver seam (AF12)
# ---------------------------------------------------------------------------


def register_content_ingestion_resolver(resolver: Any) -> None:
    """Register the edition-owned resolver for MCP content references.

    The core MCP tools may accept direct inline payloads or an abstract
    ``content_reference``. Concrete local file/URL behavior is edition-owned, so
    Community registers a resolver here instead of the core importing pathlib or
    http clients in tool handlers.
    """
    register_runtime_value(_CONTENT_KEY, resolver)


def reset_content_ingestion_resolver() -> None:
    """Drop the registered content-ingestion resolver (test isolation)."""
    reset_runtime_values(_CONTENT_KEY)


def resolve_content_ingestion_resolver(*, preferred: Any = None) -> Any | None:
    """Return the registered content resolver, if any.

    This seam is optional because inline MCP payloads are a complete core path.
    A missing resolver only fails calls that provide ``content_reference``.
    """
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    scoped = (
        composition.content_ingestion_resolver if composition is not None else None
    )
    if preferred is not None:
        return preferred
    if scoped is not None:
        return scoped
    return resolve_runtime_value(_CONTENT_KEY)


# ---------------------------------------------------------------------------
# Relational runtime factory seam (AF30-3a)
# ---------------------------------------------------------------------------

def register_relational_runtime_factory(factory: Any) -> None:
    """Register an edition/test factory that builds a relational runtime port.

    The factory is called as ``factory(url, echo=False)`` and returns an object
    implementing ``RelationalRuntime``. It supports compatibility callers that
    still enter through ``core.infra.database.create_database``.
    """
    register_runtime_value(_RELATIONAL_KEY, factory)


def reset_relational_runtime_factory() -> None:
    """Drop the registered relational runtime factory."""
    reset_runtime_values(_RELATIONAL_KEY)


def is_relational_runtime_factory_registered() -> bool:
    """Whether a relational runtime factory is currently registered."""
    return resolve_runtime_value(_RELATIONAL_KEY) is not None


def resolve_relational_runtime_factory(*, preferred: Any = None) -> Any:
    """Return a registered relational runtime factory, fail-closed if absent."""
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    scoped = composition.relational_runtime_factory if composition is not None else None
    factory = preferred if preferred is not None else scoped
    if factory is None:
        factory = resolve_runtime_value(_RELATIONAL_KEY)
    if factory is None:
        raise RuntimeError(
            "No relational runtime factory is registered. The edition adapter must "
            "build and register a RelationalRuntime, or "
            "register a compatibility factory explicitly."
        )
    return factory


__all__ = [
    "register_unit_of_work_factory",
    "reset_unit_of_work_factory",
    "is_unit_of_work_factory_registered",
    "resolve_unit_of_work_factory",
    "register_content_ingestion_resolver",
    "reset_content_ingestion_resolver",
    "resolve_content_ingestion_resolver",
    "register_relational_runtime_factory",
    "reset_relational_runtime_factory",
    "is_relational_runtime_factory_registered",
    "resolve_relational_runtime_factory",
]
