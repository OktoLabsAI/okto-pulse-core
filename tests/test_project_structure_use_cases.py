from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import get_args

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.project_structure import (
    GetCardProjectStructureProjectionCommand,
    GetCardProjectStructureProjectionUseCase,
    GetProjectStructureCommand,
    GetProjectStructureUseCase,
    MutateProjectStructureCommand,
    MutateProjectStructureUseCase,
)


def _actor() -> ActorContext:
    return ActorContext("user-1", "rest", board_id="board-1")


@pytest.mark.asyncio
async def test_spec_read_preserves_authored_empty_and_requires_spec_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import project_structure as module

    spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=8,
        project_structure=[],
        project_structure_revision=3,
    )
    required: list[str] = []

    async def require_spec(*args, **kwargs):
        return spec

    async def require_permission(actor, requirement, **kwargs):
        required.append(requirement.operation)

    monkeypatch.setattr(module, "_require_actor_board_spec", require_spec)
    monkeypatch.setattr(module, "require_authorization", require_permission)

    result = await GetProjectStructureUseCase().execute(
        GetProjectStructureCommand("board-1", "spec-1"),
        actor=_actor(),
        uow=SimpleNamespace(),
    )
    assert result.structure.state == "authored_empty"
    assert result.structure.spec_version == 8
    assert required == ["spec.entity.read"]


@pytest.mark.asyncio
async def test_card_projection_requires_card_and_spec_read_and_rejects_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import project_structure as module

    card = SimpleNamespace(
        id="bug-1",
        board_id="board-1",
        spec_id="spec-1",
        card_type="bug",
    )
    spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=1,
        project_structure=[],
        project_structure_revision=1,
    )
    required: list[str] = []

    async def get_card(*args, **kwargs):
        return card

    async def get_spec(*args, **kwargs):
        return spec

    async def require_permission(actor, requirement, **kwargs):
        required.append(requirement.operation)

    monkeypatch.setattr(module, "_get_card_for_actor", get_card)
    monkeypatch.setattr(module, "_require_actor_board_spec", get_spec)
    monkeypatch.setattr(module, "require_authorization", require_permission)

    with pytest.raises(CommandValidationError, match="unsupported_card_type:bug"):
        await GetCardProjectStructureProjectionUseCase().execute(
            GetCardProjectStructureProjectionCommand("board-1", "bug-1"),
            actor=_actor(),
            uow=SimpleNamespace(),
        )
    assert required == ["card.entity.read", "spec.entity.read"]


@pytest.mark.asyncio
async def test_card_projection_fails_closed_when_spec_read_is_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import project_structure as module

    card = SimpleNamespace(
        id="task-1",
        board_id="board-1",
        spec_id="spec-1",
        card_type="normal",
    )
    spec = SimpleNamespace(
        id="spec-1",
        board_id="board-1",
        version=1,
        project_structure=[],
        project_structure_revision=1,
    )

    async def get_card(*args, **kwargs):
        return card

    async def get_spec(*args, **kwargs):
        return spec

    async def require_permission(actor, requirement, **kwargs):
        if requirement.operation == "spec.entity.read":
            raise PermissionDeniedError("spec read denied")

    monkeypatch.setattr(module, "_get_card_for_actor", get_card)
    monkeypatch.setattr(module, "_require_actor_board_spec", get_spec)
    monkeypatch.setattr(module, "require_authorization", require_permission)

    with pytest.raises(PermissionDeniedError, match="spec read denied"):
        await GetCardProjectStructureProjectionUseCase().execute(
            GetCardProjectStructureProjectionCommand("board-1", "task-1"),
            actor=_actor(),
            uow=SimpleNamespace(),
        )


@pytest.mark.asyncio
async def test_batch_use_case_commits_successful_noop_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import project_structure as module

    spec = SimpleNamespace(id="spec-1", board_id="board-1")
    applied = SimpleNamespace(success=True, changed_fields=[])

    async def require_spec(*args, **kwargs):
        return spec

    class StructuredService:
        async def apply(self, command):
            assert command.operation == "batch"
            assert command.entity_type == "project_structure_node"
            assert command.idempotency_key == "idem-1"
            return applied

    class Services:
        structured_specs = StructuredService()

        async def resolve_user_permissions(self, actor_id, board_id):
            return {"allow": True}

    class Uow:
        services = Services()
        committed = 0
        rolled_back = 0

        async def commit(self):
            self.committed += 1

        async def rollback(self):
            self.rolled_back += 1

    uow = Uow()
    monkeypatch.setattr(module, "_require_actor_board_spec", require_spec)

    result = await MutateProjectStructureUseCase().execute(
        MutateProjectStructureCommand(
            "board-1",
            "spec-1",
            operations=[
                {
                    "operation": "create",
                    "payload": {
                        "id": "psn_root",
                        "position": 0,
                        "kind": "folder",
                        "name": "src",
                        "classification": "to_be",
                    },
                }
            ],
            expected_spec_version=4,
            expected_structure_revision=0,
            idempotency_key="idem-1",
        ),
        actor=_actor(),
        uow=uow,
    )
    assert result.structured_result is applied
    assert uow.committed == 1
    assert uow.rolled_back == 0


def test_mcp_contract_exposes_project_structure_and_all_specific_inputs() -> None:
    from okto_pulse.core.mcp import server

    assert "project_structure_node" in get_args(server.StructuredSpecEntityType)
    operations = set(get_args(server.StructuredSpecOperation))
    assert {
        "batch",
        "link_test",
        "unlink_test",
        "link_evidence",
        "unlink_evidence",
    }.issubset(operations)
    parameters = inspect.signature(server._mcp_apply_structured_spec_entity).parameters
    assert {
        "task_role",
        "test_id",
        "test_role",
        "evidence_id",
        "idempotency_key",
        "expected_structure_revision",
    }.issubset(parameters)
