"""Transport-neutral authorization for bounded operational actions.

Some edition-owned adapters expose operations that do not otherwise need a
domain use case (for example, reading local telemetry).  They still route the
authorization decision through Core so REST, MCP and UI-facing contracts use
the same canonical permission registry and introduction policy.
"""

from __future__ import annotations

from dataclasses import dataclass

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class AuthorizeOperationCommand:
    """One canonical operation and its pre-introduction compatibility token."""

    operation: str
    legacy_operation: str | None = None
    board_id: str | None = None


@dataclass(frozen=True, slots=True)
class AuthorizeOperationResult:
    """Evidence that Core admitted the exact requested operation."""

    operation: str


class AuthorizeOperationUseCase:
    """Require a registered permission without coupling Core to a transport."""

    async def execute(
        self,
        command: AuthorizeOperationCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork | None = None,
    ) -> AuthorizeOperationResult:
        operation = command.operation.strip()
        await require_authorization(
            actor,
            PermissionRequirement(
                operation,
                legacy_operation=command.legacy_operation,
            ),
            uow=uow,
            board_id=command.board_id,
        )
        return AuthorizeOperationResult(operation=operation)


__all__ = [
    "AuthorizeOperationCommand",
    "AuthorizeOperationResult",
    "AuthorizeOperationUseCase",
]
