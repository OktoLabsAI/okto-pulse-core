"""Adversarial SK-M contracts for composite reads and REST board fences."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.spec_crud import (
    GetSpecCommand,
    GetSpecUseCase,
)
from okto_pulse.core.application.use_cases.spec_dependencies import (
    AddSpecDependencyCommand,
    AddSpecDependencyUseCase,
    RemoveSpecDependencyCommand,
    RemoveSpecDependencyUseCase,
)
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import (
    SpecDependencyReadiness,
    spec_dependency_readiness_projection,
)


@pytest.mark.asyncio
async def test_get_spec_starts_snapshot_before_every_composite_projection_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_crud as module

    events: list[str] = []
    spec = SimpleNamespace(
        id="source",
        board_id="board-1",
        status=SpecStatus.DRAFT,
    )
    readiness = SpecDependencyReadiness(
        spec_id="source",
        board_id="board-1",
        current_edition=2,
        last_started_edition=None,
        active_dependency_count=0,
        blocking_count=0,
        archived_blocking_count=0,
        unfinished_blocking_count=0,
        blockers_truncated=False,
    )

    async def load_spec(*_: object, **__: object) -> object:
        events.append("load_spec")
        return spec

    async def project(*_: object, **__: object) -> object:
        events.append("project_effective_knowledge")
        return SimpleNamespace(id="source")

    class DependencyReader:
        async def get_readiness(self, **_: object) -> SpecDependencyReadiness:
            events.append("get_readiness")
            return readiness

    class Uow:
        services = SimpleNamespace(spec_dependencies=DependencyReader())

        async def begin_consistent_read(self) -> None:
            events.append("begin_consistent_read")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "project_effective_knowledge", project)

    result = await GetSpecUseCase().execute(
        GetSpecCommand("source"),
        actor=ActorContext("user-1", "rest", actor_kind="user"),
        uow=Uow(),
    )

    assert result.spec.dependency_readiness == spec_dependency_readiness_projection(
        readiness
    )
    assert events == [
        "begin_consistent_read",
        "load_spec",
        "project_effective_knowledge",
        "get_readiness",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("add", "remove"))
async def test_mutation_use_case_enforces_transport_board_fence_before_authorization(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    events: list[str] = []

    async def load_spec(*_: object, **__: object) -> object:
        events.append("load_spec")
        return SimpleNamespace(
            id="source",
            board_id="board-2",
            status=SpecStatus.DRAFT,
        )

    async def unexpected_authorization(*_: object, **__: object) -> None:
        events.append("authorize")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(
        module,
        "_require_dependency_permission",
        unexpected_authorization,
    )
    if operation == "add":
        use_case = AddSpecDependencyUseCase()
        command = AddSpecDependencyCommand(
            spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="add-key",
            board_id="board-1",
        )
    else:
        use_case = RemoveSpecDependencyUseCase()
        command = RemoveSpecDependencyCommand(
            spec_id="source",
            dependency_id="dependency-1",
            reason="No longer required",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="remove-key",
            board_id="board-1",
        )

    with pytest.raises(EntityNotFoundError) as caught:
        await use_case.execute(
            command,
            actor=ActorContext("user-1", "rest", actor_kind="user"),
            uow=SimpleNamespace(services=SimpleNamespace()),
        )

    assert caught.value.entity_type == "spec"
    assert caught.value.entity_id == "source"
    assert events == ["load_spec"]
