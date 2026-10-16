"""P00/G07: delivery override uses the same authority as other coverage flags."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.spec_crud import _spec_update_permission_requirements
from okto_pulse.core.models.schemas import SpecUpdate
from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.application.use_cases.spec_crud import UpdateSpecCommand, UpdateSpecUseCase


@pytest.mark.parametrize("enabled", [False, True])
def test_G07_delivery_override_requires_coverage_authority_in_both_directions(enabled):
    current = SimpleNamespace(skip_delivery_evidence=not enabled)
    requested = SpecUpdate(skip_delivery_evidence=enabled)
    assert "spec.entity.edit_coverage_flags" in _spec_update_permission_requirements(current, requested)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_G07_unauthorized_override_never_reaches_writer(enabled):
    spec = SimpleNamespace(id="s", board_id="b", status="draft", skip_delivery_evidence=not enabled)
    service = SimpleNamespace(get_spec=AsyncMock(return_value=spec),
        update_spec=AsyncMock(side_effect=AssertionError("Unauthorized override reached the writer")))
    uow = SimpleNamespace(services=SimpleNamespace(specs=service,
        resolve_user_permissions=AsyncMock(return_value=[])), commit=AsyncMock())
    actor = ActorContext(actor_id="agent", actor_kind="agent", source="mcp", board_id="b")
    with pytest.raises(PermissionDeniedError, match="spec.entity.edit_coverage_flags"):
        await UpdateSpecUseCase().execute(UpdateSpecCommand("s", SpecUpdate(skip_delivery_evidence=enabled)),
            actor=actor, uow=uow)
    service.update_spec.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('enabled', [False, True])
async def test_G08_authorized_draft_override_reaches_native_writer(monkeypatch, enabled):
    from okto_pulse.core.ports.permission_policy import PermissionSet
    spec = SimpleNamespace(id='s', board_id='b', status='draft', skip_delivery_evidence=not enabled)
    monkeypatch.setattr('okto_pulse.core.application.use_cases.spec_crud.project_effective_knowledge',
        AsyncMock(return_value=spec))
    monkeypatch.setattr('okto_pulse.core.application.use_cases.missing_link_context.attach_missing_link_context',
        AsyncMock(return_value=spec))
    service = SimpleNamespace(get_spec=AsyncMock(return_value=spec), update_spec=AsyncMock(return_value=spec))
    uow = SimpleNamespace(services=SimpleNamespace(specs=service,
        resolve_user_permissions=AsyncMock(return_value=PermissionSet({'spec': {'entity': {'edit_coverage_flags': True}}}))),
        commit=AsyncMock())
    actor = ActorContext(actor_id='agent', actor_kind='agent', source='mcp', board_id='b')
    request = SpecUpdate(skip_delivery_evidence=enabled)
    await UpdateSpecUseCase().execute(UpdateSpecCommand('s', request), actor=actor, uow=uow)
    service.update_spec.assert_awaited_once_with('s', 'agent', request)
    uow.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['review', 'approved', 'validated', 'in_progress', 'done'])
@pytest.mark.parametrize('enabled', [False, True])
async def test_G09_native_spec_service_keeps_override_draft_only(monkeypatch, status, enabled):
    from okto_pulse.core.services.main import SpecService
    from okto_pulse.core.domain.human_validation_cycle import SubjectEditRequiresDraftError
    service = SpecService(None)
    monkeypatch.setattr(service, 'get_spec', AsyncMock(return_value=SimpleNamespace(
        id='s', board_id='b', status=status, archived=False, skip_delivery_evidence=not enabled)))
    with pytest.raises(SubjectEditRequiresDraftError):
        await service.update_spec('s', 'human', SpecUpdate(skip_delivery_evidence=enabled))
