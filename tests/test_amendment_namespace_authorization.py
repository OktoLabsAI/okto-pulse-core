"""Canonical amendment policy is enforced once in transport-neutral use cases."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Awaitable, Callable
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases import admin_catalog
from okto_pulse.core.application.use_cases.admin_catalog import (
    AssociateAmendmentRevisionCommand,
    AssociateAmendmentRevisionUseCase,
    CreateAmendmentRevisionCommand,
    CreateAmendmentRevisionUseCase,
    GetAmendmentRevisionCommand,
    GetAmendmentRevisionUseCase,
    ListAmendmentRevisionsCommand,
    ListAmendmentRevisionsUseCase,
    TransitionAmendmentRevisionCommand,
    TransitionAmendmentRevisionUseCase,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.confirm_amendment_coverage import (
    ConfirmAmendmentCoverageCommand,
    ConfirmAmendmentCoverageUseCase,
)
from okto_pulse.core.domain.realm import RealmScope


class _Uow:
    def __init__(self, services: Any) -> None:
        self.services = services
        self.commit = AsyncMock()


def _actor(*permissions: str) -> ActorContext:
    return ActorContext(
        "actor-1",
        "mcp",
        actor_name="Agent One",
        board_id="board-1",
        realm_scope=RealmScope.local(),
        permissions=list(permissions),
    )


@dataclass(frozen=True)
class _Case:
    operation: str
    legacy: str
    writer: str | None
    invoke: Callable[[_Uow, ActorContext], Awaitable[Any]]
    commits: bool


async def _create(uow: _Uow, actor: ActorContext) -> Any:
    return await CreateAmendmentRevisionUseCase().execute(
        CreateAmendmentRevisionCommand("board-1", "bug-1", {}),
        actor=actor,
        uow=uow,
    )


async def _list(uow: _Uow, actor: ActorContext) -> Any:
    return await ListAmendmentRevisionsUseCase().execute(
        ListAmendmentRevisionsCommand("board-1", "bug-1"),
        actor=actor,
        uow=uow,
    )


async def _get(uow: _Uow, actor: ActorContext) -> Any:
    return await GetAmendmentRevisionUseCase().execute(
        GetAmendmentRevisionCommand("board-1", "bug-1", "amendment-1"),
        actor=actor,
        uow=uow,
    )


async def _associate(uow: _Uow, actor: ActorContext) -> Any:
    return await AssociateAmendmentRevisionUseCase().execute(
        AssociateAmendmentRevisionCommand(
            "board-1", "bug-1", "amendment-1", {}
        ),
        actor=actor,
        uow=uow,
    )


async def _transition(uow: _Uow, actor: ActorContext) -> Any:
    return await TransitionAmendmentRevisionUseCase().execute(
        TransitionAmendmentRevisionCommand(
            "board-1", "bug-1", "amendment-1", {"status": "review"}
        ),
        actor=actor,
        uow=uow,
    )


_CASES = (
    _Case(
        "amendment.revision.create",
        "card.entity.edit_bug_fields",
        "create",
        _create,
        True,
    ),
    _Case("amendment.revision.read", "card.entity.read", "list_for_bug", _list, False),
    _Case("amendment.revision.read", "card.entity.read", None, _get, False),
    _Case(
        "amendment.revision.associate",
        "card.entity.edit_bug_fields",
        "associate",
        _associate,
        True,
    ),
    _Case(
        "amendment.revision.transition",
        "card.entity.edit_bug_fields",
        "transition_lifecycle",
        _transition,
        True,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _CASES, ids=lambda case: case.operation)
async def test_amendment_use_cases_deny_before_writer_and_accept_legacy(
    case: _Case,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _accessible(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _preflight(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"id": "amendment-1"}

    monkeypatch.setattr(admin_catalog, "_require_amendment_bug_access", _accessible)
    monkeypatch.setattr(admin_catalog, "_preflight_amendment", _preflight)

    denied_service = SimpleNamespace(
        create=AsyncMock(return_value={}),
        list_for_bug=AsyncMock(return_value={}),
        get=AsyncMock(return_value={}),
        associate=AsyncMock(return_value={}),
        transition_lifecycle=AsyncMock(return_value={}),
    )
    denied_uow = _Uow(SimpleNamespace(amendments=denied_service))
    with pytest.raises(PermissionDeniedError, match=case.operation):
        await case.invoke(denied_uow, _actor())
    if case.writer is not None:
        getattr(denied_service, case.writer).assert_not_awaited()
    denied_uow.commit.assert_not_awaited()

    allowed_service = SimpleNamespace(
        create=AsyncMock(return_value={}),
        list_for_bug=AsyncMock(return_value={}),
        get=AsyncMock(return_value={}),
        associate=AsyncMock(return_value={}),
        transition_lifecycle=AsyncMock(return_value={}),
    )
    allowed_uow = _Uow(SimpleNamespace(amendments=allowed_service))
    await case.invoke(allowed_uow, _actor(case.legacy))
    if case.writer is not None:
        getattr(allowed_service, case.writer).assert_awaited_once()
    if case.commits:
        allowed_uow.commit.assert_awaited_once()
    else:
        allowed_uow.commit.assert_not_awaited()

    canonical_service = SimpleNamespace(
        create=AsyncMock(return_value={}),
        list_for_bug=AsyncMock(return_value={}),
        get=AsyncMock(return_value={}),
        associate=AsyncMock(return_value={}),
        transition_lifecycle=AsyncMock(return_value={}),
    )
    canonical_uow = _Uow(SimpleNamespace(amendments=canonical_service))
    await case.invoke(canonical_uow, _actor(case.operation))
    if case.writer is not None:
        getattr(canonical_service, case.writer).assert_awaited_once()


@pytest.mark.asyncio
async def test_confirm_coverage_denies_before_writer_and_binds_expected_board() -> None:
    cards = SimpleNamespace(confirm_amendment_coverage=AsyncMock(return_value={"ok": True}))
    denied_uow = _Uow(SimpleNamespace(cards=cards))
    command = ConfirmAmendmentCoverageCommand(
        "board-1", "amendment-1", "test-1", "scenario-1"
    )

    with pytest.raises(PermissionDeniedError, match="amendment.coverage.confirm"):
        await ConfirmAmendmentCoverageUseCase().execute(
            command, actor=_actor(), uow=denied_uow
        )
    cards.confirm_amendment_coverage.assert_not_awaited()
    denied_uow.commit.assert_not_awaited()

    allowed_cards = SimpleNamespace(
        confirm_amendment_coverage=AsyncMock(return_value={"ok": True})
    )
    allowed_uow = _Uow(SimpleNamespace(cards=allowed_cards))
    result = await ConfirmAmendmentCoverageUseCase().execute(
        command,
        actor=_actor("card.validation.submit"),
        uow=allowed_uow,
    )

    assert result.coverage_confirmation == {"ok": True}
    allowed_cards.confirm_amendment_coverage.assert_awaited_once_with(
        expected_board_id="board-1",
        amendment_id="amendment-1",
        regression_test_task_id="test-1",
        regression_scenario_id="scenario-1",
        reviewer_id="actor-1",
        reviewer_name="Agent One",
    )
    allowed_uow.commit.assert_awaited_once()
