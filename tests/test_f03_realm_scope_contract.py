from __future__ import annotations

import inspect

import pytest

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.domain.entities import Board, Ideation
from okto_pulse.core.domain.ownership import (
    AGGREGATE_OWNERSHIP,
    GLOBAL_AGGREGATES,
    TENANT_OWNED_AGGREGATES,
    aggregate_ownership,
)
from okto_pulse.core.domain.realm import (
    MissingRealmScope,
    RealmIsolationViolation,
    RealmScope,
    require_realm_scope,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import UnitOfWorkFactory
from okto_pulse.core.testing.fake_saas_uow import FakeSaaSUnitOfWorkFactory


def test_f03_realm_scope_is_required_and_normalized() -> None:
    with pytest.raises(MissingRealmScope, match="realm_scope_required"):
        RealmScope.tenant("")
    with pytest.raises(MissingRealmScope, match="realm_scope_required"):
        require_realm_scope(None)

    assert RealmScope.tenant("  acme  ").realm_id == "acme"
    assert RealmScope.local().is_local is True


def test_f03_aggregate_ownership_registry_is_complete_disjoint_and_fail_closed() -> None:
    assert set(AGGREGATE_OWNERSHIP) == TENANT_OWNED_AGGREGATES | GLOBAL_AGGREGATES
    assert TENANT_OWNED_AGGREGATES.isdisjoint(GLOBAL_AGGREGATES)
    assert {"board", "ideation", "spec"} <= TENANT_OWNED_AGGREGATES
    assert {"agent", "guideline", "permission_preset"} <= GLOBAL_AGGREGATES
    with pytest.raises(ValueError, match="unclassified_aggregate"):
        aggregate_ownership("future_unclassified_entity")


def test_f03_uow_port_and_fake_require_keyword_only_realm_scope() -> None:
    for callable_object in (UnitOfWorkFactory.__call__, FakeSaaSUnitOfWorkFactory.__call__):
        parameter = inspect.signature(callable_object).parameters["realm_scope"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty

    with pytest.raises(TypeError, match="realm_scope"):
        FakeSaaSUnitOfWorkFactory()()  # type: ignore[call-arg]


def test_f03_actor_query_scope_fails_closed_without_realm() -> None:
    actor = ActorScope(actor_id="agent", source="test")
    with pytest.raises(MissingRealmScope, match="realm_scope_required"):
        actor.query_scope(target_board_id="known-board")


@pytest.mark.asyncio
async def test_f03_fake_saas_adapter_isolates_known_ids_and_parent_writes() -> None:
    factory = FakeSaaSUnitOfWorkFactory()
    realm_a = RealmScope.tenant("realm-a")
    realm_b = RealmScope.tenant("realm-b")
    board = Board(id="known-board", name="A", owner_id="owner-a")

    async with factory(realm_scope=realm_a) as writer:
        await writer.boards.add(board)
        await writer.commit()

    async with factory(realm_scope=realm_b) as reader:
        assert await reader.boards.get(board.id) is None
        with pytest.raises(RealmIsolationViolation, match="tenant_resource_not_found"):
            await reader.ideations.add(
                Ideation(
                    id="cross-realm-child",
                    board_id=board.id,
                    title="Cross realm",
                    created_by="owner-b",
                )
            )
