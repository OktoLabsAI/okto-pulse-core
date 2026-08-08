from __future__ import annotations

import pytest

from okto_pulse.core.application.use_cases.authorize_operation import (
    AuthorizeOperationCommand,
    AuthorizeOperationUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)


@pytest.mark.asyncio
async def test_operational_authorization_requires_exact_and_historical_authority() -> None:
    use_case = AuthorizeOperationUseCase()
    command = AuthorizeOperationCommand(
        "metrics.settings.edit",
        legacy_operation="board.analytics_read",
    )

    for incomplete in (
        {"metrics": {"settings": {"edit": True}}},
        {"board": {"analytics_read": True}},
        {},
    ):
        with pytest.raises(PermissionDeniedError):
            await use_case.execute(
                command,
                actor=ActorContext(
                    "operator",
                    "rest",
                    actor_kind="human",
                    permissions=incomplete,
                ),
            )

    result = await use_case.execute(
        command,
        actor=ActorContext(
            "operator",
            "rest",
            actor_kind="human",
            permissions={
                "metrics": {"settings": {"edit": True}},
                "board": {"analytics_read": True},
            },
        ),
    )

    assert result.operation == "metrics.settings.edit"


@pytest.mark.asyncio
async def test_operational_authorization_rejects_unregistered_operation() -> None:
    with pytest.raises(PermissionDeniedError, match="unknown_permission"):
        await AuthorizeOperationUseCase().execute(
            AuthorizeOperationCommand("metrics.unregistered.action"),
            actor=ActorContext(
                "operator",
                "rest",
                actor_kind="human",
                permissions={"metrics": {"unregistered": {"action": True}}},
            ),
        )
