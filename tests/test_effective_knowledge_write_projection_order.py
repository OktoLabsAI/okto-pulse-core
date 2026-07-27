"""Write responses must observe committed state before effective-KB projection."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases import card_crud, spec_crud
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)


_CASES = (
    (
        card_crud,
        card_crud.UpdateCardUseCase,
        card_crud.UpdateCardCommand,
        "cards",
        "update_card",
        "get_card",
        "_get_card_for_actor",
        "card",
    ),
    (
        card_crud,
        card_crud.MoveCardUseCase,
        card_crud.MoveCardCommand,
        "cards",
        "move_card",
        "get_card",
        "_get_card_for_actor",
        "card",
    ),
    (
        spec_crud,
        spec_crud.UpdateSpecUseCase,
        spec_crud.UpdateSpecCommand,
        "specs",
        "update_spec",
        "get_spec",
        "_require_actor_board_spec",
        "spec",
    ),
    (
        spec_crud,
        spec_crud.MoveSpecUseCase,
        spec_crud.MoveSpecCommand,
        "specs",
        "move_spec",
        "get_spec",
        "_require_actor_board_spec",
        "spec",
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "module",
        "use_case_type",
        "command_type",
        "service_name",
        "mutation_name",
        "refetch_name",
        "guard_name",
        "target_type",
    ),
    _CASES,
    ids=("update-card", "move-card", "update-spec", "move-spec"),
)
async def test_commit_precedes_refetch_and_effective_knowledge_projection(
    monkeypatch: pytest.MonkeyPatch,
    module,
    use_case_type,
    command_type,
    service_name: str,
    mutation_name: str,
    refetch_name: str,
    guard_name: str,
    target_type: str,
) -> None:
    events: list[str] = []
    entity = SimpleNamespace(id="entity-1", board_id="board-1")

    async def guard(*_args, **_kwargs):
        events.append("guard")
        return entity

    async def mutate(*_args, **_kwargs):
        events.append("mutate")
        return entity

    async def refetch(*_args, **_kwargs):
        assert events[-1] == "commit"
        events.append("refetch")
        return entity

    async def project(_services, refreshed, *, target_type: str):
        assert refreshed is entity
        assert events[-1] == "refetch"
        events.append("project")
        return SimpleNamespace(id=refreshed.id, projected_for=target_type)

    service = SimpleNamespace(**{mutation_name: mutate, refetch_name: refetch})
    services = SimpleNamespace(**{service_name: service})

    class UnitOfWork:
        async def commit(self) -> None:
            events.append("commit")

    uow = UnitOfWork()
    uow.services = services
    monkeypatch.setattr(module, guard_name, guard)
    monkeypatch.setattr(module, "project_effective_knowledge", project)

    data = SimpleNamespace(model_fields_set=set())
    result = await use_case_type().execute(
        command_type("entity-1", data),
        actor=ActorContext("actor-1", "rest"),
        uow=uow,
    )

    projected = getattr(result, target_type)
    assert projected.projected_for == target_type
    assert events == ["guard", "mutate", "commit", "refetch", "project"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "module",
        "use_case_type",
        "command_type",
        "service_name",
        "mutation_name",
        "refetch_name",
        "guard_name",
        "target_type",
    ),
    _CASES,
    ids=("update-card", "move-card", "update-spec", "move-spec"),
)
async def test_missing_post_commit_refetch_is_explicit_and_not_projected(
    monkeypatch: pytest.MonkeyPatch,
    module,
    use_case_type,
    command_type,
    service_name: str,
    mutation_name: str,
    refetch_name: str,
    guard_name: str,
    target_type: str,
) -> None:
    events: list[str] = []
    entity = SimpleNamespace(id="entity-1", board_id="board-1")

    async def guard(*_args, **_kwargs):
        events.append("guard")
        return entity

    async def mutate(*_args, **_kwargs):
        events.append("mutate")
        return entity

    async def refetch(*_args, **_kwargs):
        assert events[-1] == "commit"
        events.append("refetch")
        return None

    async def unexpected_projection(*_args, **_kwargs):
        raise AssertionError("projection must not run without a refetched entity")

    service = SimpleNamespace(**{mutation_name: mutate, refetch_name: refetch})
    services = SimpleNamespace(**{service_name: service})

    class UnitOfWork:
        async def commit(self) -> None:
            events.append("commit")

    uow = UnitOfWork()
    uow.services = services
    monkeypatch.setattr(module, guard_name, guard)
    monkeypatch.setattr(
        module,
        "project_effective_knowledge",
        unexpected_projection,
    )

    with pytest.raises(EntityNotFoundError) as raised:
        await use_case_type().execute(
            command_type("entity-1", SimpleNamespace(model_fields_set=set())),
            actor=ActorContext("actor-1", "rest"),
            uow=uow,
        )

    assert raised.value.entity_type == target_type
    assert raised.value.entity_id == "entity-1"
    assert events == ["guard", "mutate", "commit", "refetch"]
