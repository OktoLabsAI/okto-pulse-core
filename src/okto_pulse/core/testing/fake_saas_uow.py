"""Pure in-memory UnitOfWork used to verify SaaS adapter substitutability."""

from __future__ import annotations

import copy
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from okto_pulse.core.domain.entities import Board, Ideation, Spec
from okto_pulse.core.domain.realm import (
    RealmIsolationViolation,
    RealmScope,
    require_realm_scope,
)

if TYPE_CHECKING:
    from okto_pulse.core.application.use_cases.base import ActorContext


@dataclass
class FakeSaaSState:
    boards: dict[tuple[str, str], Board] = field(default_factory=dict)
    ideations: dict[tuple[str, str], Ideation] = field(default_factory=dict)
    specs: dict[tuple[str, str], Spec] = field(default_factory=dict)


class _Repository:
    def __init__(
        self,
        rows: dict[tuple[str, str], object],
        scope: RealmScope,
        *,
        parent_boards: dict[tuple[str, str], Board] | None = None,
    ) -> None:
        self._rows = rows
        self.realm_scope = require_realm_scope(scope)
        self._parent_boards = parent_boards

    async def get(self, entity_id: str):  # noqa: ANN201
        return self._rows.get((self.realm_scope.realm_id, entity_id))

    async def add(self, entity: object) -> None:
        entity_id = getattr(entity, "id", None)
        if not isinstance(entity_id, str) or not entity_id:
            raise TypeError("repository entities must expose a non-empty string id")
        entity_realm = getattr(entity, "realm_id", None)
        if entity_realm not in (None, self.realm_scope.realm_id):
            raise RealmIsolationViolation()
        board_id = getattr(entity, "board_id", None)
        if self._parent_boards is not None and (
            not isinstance(board_id, str)
            or (self.realm_scope.realm_id, board_id) not in self._parent_boards
        ):
            raise RealmIsolationViolation()
        self._rows[(self.realm_scope.realm_id, entity_id)] = entity


class _UnsupportedServiceCatalog:
    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(
            f"fake SaaS service capability {name!r} was not configured"
        )


class _UnsupportedSemanticAssessmentPort:
    async def resolve_semantic_anchor(self, request: object) -> None:
        del request
        raise NotImplementedError(
            "fake SaaS semantic subject projection was not configured"
        )

    async def save_semantic_assessment_v2(self, request: object) -> None:
        del request
        raise NotImplementedError(
            "fake SaaS semantic assessment persistence was not configured"
        )

    async def get_current_semantic_assessment_v2(
        self,
        *,
        board_id: str,
        entity_type: str,
        subject_id: str,
        binding_id: str,
        subject_edition: int | None = None,
    ) -> None:
        del board_id, entity_type, subject_id, binding_id, subject_edition
        raise NotImplementedError(
            "fake SaaS semantic assessment reader was not configured"
        )

    async def semantic_assessment_v2_capabilities(self) -> None:
        raise NotImplementedError(
            "fake SaaS semantic assessment capabilities were not configured"
        )


class FakeSaaSUnitOfWork:
    """Copy-on-write UnitOfWork with no native persistence handle."""

    def __init__(
        self,
        committed_state: FakeSaaSState,
        *,
        realm_scope: RealmScope,
        actor: "ActorContext | None",
        services: object | None = None,
    ) -> None:
        self._committed_state = committed_state
        self._working_state = copy.deepcopy(committed_state)
        self.realm_scope = require_realm_scope(realm_scope)
        self.realm_id = self.realm_scope.realm_id
        self.actor = actor
        self.boards = _Repository(self._working_state.boards, self.realm_scope)
        self.ideations = _Repository(
            self._working_state.ideations,
            self.realm_scope,
            parent_boards=self._working_state.boards,
        )
        self.specs = _Repository(
            self._working_state.specs,
            self.realm_scope,
            parent_boards=self._working_state.boards,
        )
        self.services = services or _UnsupportedServiceCatalog()
        unsupported_semantic_port = _UnsupportedSemanticAssessmentPort()
        self.semantic_subject_projection = unsupported_semantic_port
        self.semantic_assessment_v2 = unsupported_semantic_port
        self.semantic_assessment_v2_reader = unsupported_semantic_port
        self.semantic_assessment_v2_capability = unsupported_semantic_port
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0
        self.consistent_read_calls = 0
        self.closed = False

    async def __aenter__(self) -> "FakeSaaSUnitOfWork":
        return self

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> None:
        try:
            if exc is not None:
                await self.rollback()
        finally:
            await self.close()
        return None

    async def commit(self) -> None:
        self.commit_calls += 1
        self._committed_state.boards = copy.deepcopy(self._working_state.boards)
        self._committed_state.ideations = copy.deepcopy(self._working_state.ideations)
        self._committed_state.specs = copy.deepcopy(self._working_state.specs)

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self._working_state = copy.deepcopy(self._committed_state)
        self.boards = _Repository(self._working_state.boards, self.realm_scope)
        self.ideations = _Repository(
            self._working_state.ideations,
            self.realm_scope,
            parent_boards=self._working_state.boards,
        )
        self.specs = _Repository(
            self._working_state.specs,
            self.realm_scope,
            parent_boards=self._working_state.boards,
        )

    async def begin_consistent_read(self) -> None:
        # The fake takes a deep copy when the UoW is created, so every read in
        # this UoW already observes one immutable committed-state snapshot.
        self.consistent_read_calls += 1

    async def synchronize(
        self,
        *,
        conflict_error: Exception | None = None,
    ) -> None:
        del conflict_error
        return None

    async def reload(
        self, entity: object, *, fields: tuple[str, ...] = ()
    ) -> None:
        del entity, fields

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _FakeSaaSUnitOfWorkContext(AbstractAsyncContextManager[FakeSaaSUnitOfWork]):
    def __init__(self, uow: FakeSaaSUnitOfWork) -> None:
        self.uow = uow

    async def __aenter__(self) -> FakeSaaSUnitOfWork:
        return await self.uow.__aenter__()

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> None:
        return await self.uow.__aexit__(exc_type, exc, tb)


class FakeSaaSUnitOfWorkFactory:
    def __init__(
        self,
        state: FakeSaaSState | None = None,
        *,
        services: object | None = None,
        request_realm_scope: RealmScope | None = None,
    ) -> None:
        self.state = state or FakeSaaSState()
        self.services = services
        self.request_realm_scope = request_realm_scope
        self.created: list[FakeSaaSUnitOfWork] = []

    def resolve_realm_scope(self) -> RealmScope:
        if self.request_realm_scope is None:
            raise RuntimeError("realm_scope_not_configured")
        return require_realm_scope(self.request_realm_scope)

    def __call__(
        self,
        *,
        realm_scope: RealmScope,
        actor: "ActorContext | None" = None,
    ) -> AbstractAsyncContextManager[FakeSaaSUnitOfWork]:
        uow = FakeSaaSUnitOfWork(
            self.state,
            realm_scope=require_realm_scope(realm_scope),
            actor=actor,
            services=self.services,
        )
        self.created.append(uow)
        return _FakeSaaSUnitOfWorkContext(uow)


__all__ = [
    "FakeSaaSState",
    "FakeSaaSUnitOfWork",
    "FakeSaaSUnitOfWorkFactory",
]
