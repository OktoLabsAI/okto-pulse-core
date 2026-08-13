from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.spec_dependencies import (
    AddSpecDependencyCommand,
    AddSpecDependencyUseCase,
)
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.domain.spec_dependency import (
    SpecDependencyMutationReceipt,
    SpecDependencyOperationError,
    SpecDependencyRecord,
    SpecDependencySpecSnapshot,
)
from okto_pulse.core.services.spec_dependency import SpecDependencyService


def _snapshot(
    spec_id: str,
    *,
    board_id: str = "board-1",
    status: SpecStatus = SpecStatus.DRAFT,
    version: int = 7,
    edition: int = 2,
    archived: bool = False,
) -> SpecDependencySpecSnapshot:
    return SpecDependencySpecSnapshot(
        id=spec_id,
        board_id=board_id,
        title=f"Spec {spec_id}",
        status=status,
        version=version,
        edition=edition,
        archived=archived,
    )


def _entity(snapshot: SpecDependencySpecSnapshot) -> SimpleNamespace:
    return SimpleNamespace(
        id=snapshot.id,
        board_id=snapshot.board_id,
        title=snapshot.title,
        status=snapshot.status,
        version=snapshot.version,
        edition=snapshot.edition,
        archived=snapshot.archived,
        ideation_id=snapshot.ideation_id,
        last_started_edition=snapshot.last_started_edition,
    )


class _Persistence:
    def __init__(
        self,
        snapshots: dict[str, SpecDependencySpecSnapshot],
    ) -> None:
        self.snapshots = snapshots
        self.replay: SpecDependencyMutationReceipt | None = None
        self.locked: list[str] = []
        self.inserts = 0
        self.bumps = 0
        self.receipts = 0
        self.inserted: SpecDependencyRecord | None = None

    async def acquire_board_graph_lock(self, board_id: str) -> None:
        assert board_id == "board-1"

    async def lookup_mutation_replay(
        self, **_: object
    ) -> SpecDependencyMutationReceipt | None:
        return self.replay

    async def get_spec_snapshot(
        self,
        *,
        board_id: str,
        spec_id: str,
        for_update: bool = False,
    ) -> SpecDependencySpecSnapshot | None:
        assert board_id == "board-1"
        if for_update:
            self.locked.append(spec_id)
        return self.snapshots.get(spec_id)

    async def find_active_dependency(self, **_: object) -> None:
        return None

    async def list_active_board_edges(self, **_: object) -> tuple[()]:
        return ()

    async def insert_dependency(
        self,
        dependency: SpecDependencyRecord,
        **_: object,
    ) -> None:
        self.inserts += 1
        self.inserted = dependency

    async def bump_source_spec_version(
        self,
        *,
        spec_id: str,
        expected_version: int,
        **_: object,
    ) -> SpecDependencySpecSnapshot:
        self.bumps += 1
        updated = replace(self.snapshots[spec_id], version=expected_version + 1)
        self.snapshots[spec_id] = updated
        return updated

    async def store_mutation_receipt(
        self,
        receipt: SpecDependencyMutationReceipt,
        **_: object,
    ) -> None:
        self.receipts += 1
        self.replay = receipt


class _Uow:
    def __init__(self, service: SpecDependencyService) -> None:
        self.services = SimpleNamespace(spec_dependencies=service)
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


def _actor() -> ActorContext:
    return ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        actor_name="Agent",
        board_id="board-1",
        permissions=["*"],
    )


def _command() -> AddSpecDependencyCommand:
    return AddSpecDependencyCommand(
        spec_id="source",
        target_spec_id="private-target-secret",
        expected_spec_version=7,
        expected_spec_edition=2,
        idempotency_key="target-auth-fence",
        board_id="board-1",
    )


def _drift(
    snapshot: SpecDependencySpecSnapshot,
    field: str,
) -> SpecDependencySpecSnapshot:
    changes = {
        "board_id": {"board_id": "board-2"},
        "id": {"id": "replacement-target"},
        "version": {"version": snapshot.version + 1},
        "edition": {"edition": snapshot.edition + 1},
        "status": {"status": SpecStatus.REVIEW},
        "archived": {"archived": True},
    }
    return replace(snapshot, **changes[field])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_field",
    ("board_id", "id", "version", "edition", "status", "archived"),
)
async def test_add_rejects_target_changed_after_authorization_barrier(
    changed_field: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    source = _snapshot("source")
    target = _snapshot("private-target-secret", version=4, edition=3)
    entities = {"source": _entity(source), target.id: _entity(target)}
    persistence = _Persistence({"source": source, target.id: target})
    service = SpecDependencyService(persistence, object())
    service._record_effects = AsyncMock()  # type: ignore[method-assign]
    uow = _Uow(service)
    target_authorized = asyncio.Event()
    drift_applied = asyncio.Event()

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor_context: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        return entities[spec_id]

    async def authorize(
        _actor_context: ActorContext,
        _uow: object,
        spec: object,
        *,
        write: bool,
    ) -> None:
        if getattr(spec, "id") == target.id and not write:
            target_authorized.set()
            await drift_applied.wait()

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    operation = asyncio.create_task(
        AddSpecDependencyUseCase().execute(
            _command(),
            actor=_actor(),
            uow=uow,
        )
    )
    await asyncio.wait_for(target_authorized.wait(), timeout=1)
    persistence.snapshots[target.id] = _drift(target, changed_field)
    drift_applied.set()

    with pytest.raises(SpecDependencyOperationError) as caught:
        await operation

    assert caught.value.code == "dependency_target_unavailable"
    assert caught.value.facts == {"spec_id": "source"}
    assert "private-target-secret" not in str(caught.value.to_dict())
    assert persistence.inserts == persistence.bumps == persistence.receipts == 0
    assert uow.commits == 0
    service._record_effects.assert_not_awaited()


@pytest.mark.asyncio
async def test_add_accepts_the_unchanged_authorized_target_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    source = _snapshot("source")
    target = _snapshot("private-target-secret", version=4, edition=3)
    entities = {"source": _entity(source), target.id: _entity(target)}
    persistence = _Persistence({"source": source, target.id: target})
    service = SpecDependencyService(persistence, object())
    service._record_effects = AsyncMock()  # type: ignore[method-assign]
    uow = _Uow(service)

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor_context: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        return entities[spec_id]

    async def authorize(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    result = await AddSpecDependencyUseCase().execute(
        _command(),
        actor=_actor(),
        uow=uow,
    )

    assert result.receipt.dependency.target_spec_id == target.id
    assert persistence.inserted is not None
    assert persistence.inserted.target_version_on_create == target.version
    assert (persistence.inserts, persistence.bumps, persistence.receipts) == (1, 1, 1)
    assert uow.commits == 1
    service._record_effects.assert_awaited_once()


@pytest.mark.asyncio
async def test_target_fence_preserves_side_effect_free_idempotent_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    source = _snapshot("source")
    target = _snapshot("private-target-secret", version=4, edition=3)
    entities = {"source": _entity(source), target.id: _entity(target)}
    persistence = _Persistence({"source": source, target.id: target})
    service = SpecDependencyService(persistence, object())
    service._record_effects = AsyncMock()  # type: ignore[method-assign]
    uow = _Uow(service)

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor_context: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        return entities[spec_id]

    async def authorize(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    first = await AddSpecDependencyUseCase().execute(
        _command(), actor=_actor(), uow=uow
    )
    changed = replace(target, status=SpecStatus.REVIEW, version=5)
    persistence.snapshots[target.id] = changed
    entities[target.id] = _entity(changed)
    second = await AddSpecDependencyUseCase().execute(
        _command(), actor=_actor(), uow=uow
    )

    assert first.receipt.replayed is False
    assert second.receipt.replayed is True
    assert second.receipt.dependency.id == first.receipt.dependency.id
    assert (persistence.inserts, persistence.bumps, persistence.receipts) == (1, 1, 1)
    assert uow.commits == 2
    service._record_effects.assert_awaited_once()


def test_mcp_unknown_spec_dependency_logs_omit_exception_payload_and_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okto_pulse.core.mcp.server import _log_unhandled_spec_dependency_error

    secret = "postgresql://user:SECRET-DO-NOT-LEAK@private/db"
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="okto_pulse.mcp.spec_dependency"):
        for operation in ("add", "remove", "list"):
            _log_unhandled_spec_dependency_error(operation, RuntimeError(secret))

    records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "spec_dependency.unhandled_error"
    ]
    assert len(records) == 3
    assert {getattr(record, "operation") for record in records} == {
        "add",
        "remove",
        "list",
    }
    assert all(getattr(record, "error_type") == "RuntimeError" for record in records)
    assert all(record.exc_info is None for record in records)
    assert secret not in caplog.text
