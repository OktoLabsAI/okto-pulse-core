from __future__ import annotations

import copy
import inspect
import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from okto_pulse.core.application.use_cases.allowed_transitions import (
    AllowedTransition,
    ListAllowedTransitionsUseCase,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    decide_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
)
from okto_pulse.core.application.use_cases.spec_dependencies import (
    AddSpecDependencyCommand,
    AddSpecDependencyUseCase,
    GetSpecDependencyReadinessCommand,
    GetSpecDependencyReadinessUseCase,
    ListSpecDependenciesCommand,
    ListSpecDependenciesUseCase,
    RemoveSpecDependencyCommand,
    RemoveSpecDependencyUseCase,
)
from okto_pulse.core.domain.card_transition import (
    CardTransitionFacts,
    evaluate_card_transition,
)
from okto_pulse.core.domain.enums import CardStatus, SpecStatus
from okto_pulse.core.domain.mcp_permission_registry import (
    MCP_READER_TOOL_NAMES,
    MCP_TOOL_PERMISSION_POLICIES,
)
from okto_pulse.core.domain.permissions import (
    PERMISSION_INTRODUCTION_MANIFESTS,
    SKM_PERMISSION_INTRODUCTION_V1,
    PermissionSet,
)
from okto_pulse.core.domain.sdlc_registry import transition_contracts
from okto_pulse.core.domain.spec_dependency import (
    SPEC_DEPENDENCY_CURSOR_MAX_LENGTH,
    SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH,
    SpecDependencyBlocker,
    SpecDependencyCapabilities,
    SpecDependencyDirection,
    SpecDependencyListItem,
    SpecDependencyListQuery,
    SpecDependencyMutationReceipt,
    SpecDependencyOperationError,
    SpecDependencyPage,
    SpecDependencyReadiness,
    SpecDependencyRecord,
    SpecDependencySpecSnapshot,
    normalize_spec_dependency_blocker_limit,
    spec_dependency_blocked_guidance,
    spec_dependency_blocking_facts,
    spec_dependency_cycle_path,
    spec_dependency_is_satisfied,
    spec_dependency_readiness_projection,
    spec_dependency_would_create_cycle,
    transition_starts_card_execution,
    transition_starts_spec_execution,
)
from okto_pulse.core.services.gate_contracts import (
    GATE_SPEC_DEPENDENCIES,
    spec_gate_readiness,
)
from okto_pulse.core.services.spec_dependency import SpecDependencyService
from okto_pulse.core.services.spec_dependency_observability import (
    METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS,
    METRIC_SPEC_DEPENDENCY_GATE_TOTAL,
    METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS,
    METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL,
    METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
    SpecDependencyMetricEvent,
    get_spec_dependency_metric_samples,
    mark_spec_dependency_critical_section_started,
    observe_spec_dependency_projection_lag,
    reset_spec_dependency_observability_for_tests,
    sanitize_spec_dependency_metric_event,
)


def _manage_dependency_requirement() -> PermissionRequirement:
    return PermissionRequirement(
        "spec.entity.manage_dependencies",
        legacy_operation="specs:update",
        entity="spec",
        state="draft",
    )


def _snapshot(
    spec_id: str,
    *,
    status: SpecStatus = SpecStatus.DRAFT,
    version: int = 7,
    edition: int = 2,
    last_started_edition: int | None = None,
    archived: bool = False,
) -> SpecDependencySpecSnapshot:
    return SpecDependencySpecSnapshot(
        id=spec_id,
        board_id="board-1",
        title=f"Spec {spec_id}",
        status=status,
        edition=edition,
        version=version,
        last_started_edition=last_started_edition,
        archived=archived,
    )


def _record(
    *,
    source_id: str = "source",
    target_id: str = "target",
) -> SpecDependencyRecord:
    return SpecDependencyRecord(
        id="dependency-1",
        board_id="board-1",
        source_spec_id=source_id,
        target_spec_id=target_id,
        created_at=datetime.now(timezone.utc),
        created_by="actor-1",
        source_version_on_create=7,
        source_status_on_create=SpecStatus.DRAFT,
        target_status_on_create=SpecStatus.DONE,
        target_version_on_create=3,
        resolved_on_create=True,
    )


class _FakeDependencyPersistence:
    def __init__(
        self,
        snapshots: dict[str, SpecDependencySpecSnapshot],
        *,
        existing: SpecDependencyRecord | None = None,
        replay_by_actor_type: dict[str, SpecDependencyMutationReceipt] | None = None,
        readiness: SpecDependencyReadiness | None = None,
        incoming: tuple[SpecDependencyRecord, ...] = (),
    ) -> None:
        self.snapshots = snapshots
        self.existing = existing
        self.replay_by_actor_type = replay_by_actor_type or {}
        self.snapshot_lock_order: list[str] = []
        self.lookup_actor_types: list[str] = []
        self.write_calls: list[str] = []
        self.board_lock_calls: list[str] = []
        self.readiness = readiness
        self.incoming = incoming
        self.mark_started_calls: list[int] = []
        self.incoming_calls: list[dict[str, object]] = []
        self.readiness_calls: list[dict[str, object]] = []

    async def acquire_board_graph_lock(self, board_id: str) -> None:
        assert board_id == "board-1"
        self.board_lock_calls.append(board_id)

    async def lookup_mutation_replay(
        self,
        *,
        board_id: str,
        operation: str,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
    ) -> SpecDependencyMutationReceipt | None:
        self.lookup_actor_types.append(actor_type)
        return self.replay_by_actor_type.get(actor_type)

    async def get_spec_snapshot(
        self,
        *,
        board_id: str,
        spec_id: str,
        for_update: bool = False,
    ) -> SpecDependencySpecSnapshot | None:
        if for_update:
            self.snapshot_lock_order.append(spec_id)
        return self.snapshots.get(spec_id)

    async def find_active_dependency(self, **_: object) -> SpecDependencyRecord | None:
        return self.existing

    async def list_active_board_edges(self, **_: object) -> tuple[()]:
        return ()

    async def insert_dependency(self, *_: object, **__: object) -> None:
        self.write_calls.append("insert")

    async def bump_source_spec_version(self, **_: object) -> None:
        self.write_calls.append("bump")
        return None

    async def store_mutation_receipt(self, *_: object, **__: object) -> None:
        self.write_calls.append("receipt")

    async def get_readiness(self, **kwargs: object) -> SpecDependencyReadiness:
        self.readiness_calls.append(dict(kwargs))
        assert self.readiness is not None
        return self.readiness

    async def mark_spec_edition_started(
        self,
        *,
        board_id: str,
        spec_id: str,
        expected_edition: int,
    ) -> SpecDependencySpecSnapshot | None:
        assert board_id == "board-1"
        self.mark_started_calls.append(expected_edition)
        assert self.readiness is not None
        if self.readiness.current_edition != expected_edition:
            return None
        self.readiness = replace(
            self.readiness,
            last_started_edition=expected_edition,
        )
        current = self.snapshots[spec_id]
        return replace(current, last_started_edition=expected_edition)

    async def list_incoming_active(
        self, **kwargs: object
    ) -> tuple[SpecDependencyRecord, ...]:
        self.incoming_calls.append(dict(kwargs))
        return self.incoming


class _InjectedAtomicFailure(RuntimeError):
    """Failure raised after one staged relational write in the memory UoW."""


@dataclass
class _AtomicDependencyState:
    """Committed relational facts used by the AC19 transaction contract test."""

    snapshots: dict[str, SpecDependencySpecSnapshot]
    dependencies: dict[str, SpecDependencyRecord] = field(default_factory=dict)
    receipts: dict[tuple[str, str, str, str, str], SpecDependencyMutationReceipt] = (
        field(default_factory=dict)
    )
    events: list[str] = field(default_factory=list)
    application_records: list[tuple[str, dict[str, object]]] = field(
        default_factory=list
    )


class _AtomicDependencyTransaction:
    """Copy-on-write Core UoW + dependency persistence test adapter.

    The production database transaction belongs to an edition adapter. This
    memory implementation exercises the Core contract: every dependency row,
    source-version CAS, replay receipt, event and audit row is staged against
    one relational context; ``__aexit__`` discards the whole working copy when
    any stage raises.
    """

    def __init__(
        self,
        committed: _AtomicDependencyState,
        *,
        fail_after: str,
    ) -> None:
        self.committed = committed
        self.working = copy.deepcopy(committed)
        self.fail_after = fail_after
        self.completed_stages: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def _stage_completed(self, stage: str) -> None:
        self.completed_stages.append(stage)
        if stage == self.fail_after:
            raise _InjectedAtomicFailure(stage)

    async def __aenter__(self) -> "_AtomicDependencyTransaction":
        return self

    async def __aexit__(
        self,
        exc_type: object,
        _exc: object,
        _tb: object,
    ) -> None:
        if exc_type is not None:
            await self.rollback()

    async def commit(self) -> None:
        self.commit_calls += 1
        # A failed adapter commit must not publish a partially committed copy.
        self._stage_completed("commit")
        self.committed.snapshots = copy.deepcopy(self.working.snapshots)
        self.committed.dependencies = copy.deepcopy(self.working.dependencies)
        self.committed.receipts = copy.deepcopy(self.working.receipts)
        self.committed.events = copy.deepcopy(self.working.events)
        self.committed.application_records = copy.deepcopy(
            self.working.application_records
        )

    async def rollback(self) -> None:
        self.rollback_calls += 1
        self.working = copy.deepcopy(self.committed)

    async def acquire_board_graph_lock(self, board_id: str) -> None:
        assert board_id == "board-1"

    async def lookup_mutation_replay(
        self,
        *,
        board_id: str,
        operation: str,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
    ) -> SpecDependencyMutationReceipt | None:
        return self.working.receipts.get(
            (board_id, operation, idempotency_key, actor_id, actor_type)
        )

    async def get_spec_snapshot(
        self,
        *,
        board_id: str,
        spec_id: str,
        for_update: bool = False,
    ) -> SpecDependencySpecSnapshot | None:
        del for_update
        snapshot = self.working.snapshots.get(spec_id)
        if snapshot is None or snapshot.board_id != board_id:
            return None
        return snapshot

    async def find_active_dependency(
        self,
        *,
        board_id: str,
        source_spec_id: str,
        target_spec_id: str,
    ) -> SpecDependencyRecord | None:
        return next(
            (
                dependency
                for dependency in self.working.dependencies.values()
                if dependency.board_id == board_id
                and dependency.source_spec_id == source_spec_id
                and dependency.target_spec_id == target_spec_id
                and dependency.active
            ),
            None,
        )

    async def list_active_board_edges(
        self,
        *,
        board_id: str,
    ) -> tuple[SpecDependencyRecord, ...]:
        return tuple(
            dependency
            for dependency in self.working.dependencies.values()
            if dependency.board_id == board_id and dependency.active
        )

    async def insert_dependency(
        self,
        dependency: SpecDependencyRecord,
        *,
        request_digest: str,
    ) -> None:
        assert request_digest
        self.working.dependencies[dependency.id] = dependency
        self._stage_completed("insert_dependency")

    async def bump_source_spec_version(
        self,
        *,
        board_id: str,
        spec_id: str,
        expected_version: int,
        expected_edition: int,
    ) -> SpecDependencySpecSnapshot | None:
        current = self.working.snapshots.get(spec_id)
        if (
            current is None
            or current.board_id != board_id
            or current.version != expected_version
            or current.edition != expected_edition
        ):
            return None
        updated = replace(current, version=current.version + 1)
        self.working.snapshots[spec_id] = updated
        self._stage_completed("bump_source_spec_version")
        return updated

    async def store_mutation_receipt(
        self,
        receipt: SpecDependencyMutationReceipt,
        *,
        idempotency_key: str,
        actor_id: str,
        actor_type: str,
        actor_name: str | None,
    ) -> None:
        del actor_name
        self.working.receipts[
            (
                receipt.source_spec.board_id,
                receipt.operation,
                idempotency_key,
                actor_id,
                actor_type,
            )
        ] = receipt
        self._stage_completed("store_mutation_receipt")

    async def get_dependency(
        self,
        *,
        board_id: str,
        dependency_id: str,
    ) -> SpecDependencyRecord | None:
        dependency = self.working.dependencies.get(dependency_id)
        if dependency is None or dependency.board_id != board_id:
            return None
        return dependency

    async def tombstone_dependency(
        self,
        *,
        board_id: str,
        dependency_id: str,
        removed_at: datetime,
        removed_by: str,
        removed_by_type: str,
        removed_by_name: str | None,
        removal_reason: str,
        source_version_on_remove: int,
        idempotency_key: str,
        request_digest: str,
    ) -> SpecDependencyRecord:
        assert idempotency_key and request_digest
        current = self.working.dependencies[dependency_id]
        assert current.board_id == board_id
        removed = replace(
            current,
            removed_at=removed_at,
            removed_by=removed_by,
            removed_by_type=removed_by_type,
            removed_by_name=removed_by_name,
            removal_reason=removal_reason,
            source_version_on_remove=source_version_on_remove,
            remove_idempotency_key=idempotency_key,
        )
        self.working.dependencies[dependency_id] = removed
        self._stage_completed("tombstone_dependency")
        return removed

    async def stage_event(self, event: object) -> None:
        event_type = str(getattr(event, "event_type"))
        self.working.events.append(event_type)
        self._stage_completed(event_type)

    async def stage_application_record(self, record: object) -> None:
        entity = str(getattr(record, "entity"))
        values = copy.deepcopy(getattr(record, "values"))
        self.working.application_records.append((entity, values))
        self._stage_completed(entity)


def _install_atomic_effect_ports(
    monkeypatch: pytest.MonkeyPatch,
    transaction: _AtomicDependencyTransaction,
) -> None:
    import okto_pulse.core.services.spec_dependency as dependency_module

    async def _publish(event: object, *, session: object) -> None:
        assert session is transaction
        await transaction.stage_event(event)

    class _ApplicationPersistence:
        async def add(self, context: object, record: object) -> None:
            assert context is transaction
            await transaction.stage_application_record(record)

    monkeypatch.setattr(dependency_module, "event_publish", _publish)
    monkeypatch.setattr(
        dependency_module,
        "get_application_persistence_port",
        lambda: _ApplicationPersistence(),
    )


def test_manage_dependency_permission_is_a_fail_closed_introduction() -> None:
    from okto_pulse.core.ports.permission_policy import (
        permission_introduction_manifests,
        skm_permission_introduction_v1,
    )

    requirement = _manage_dependency_requirement()
    legacy_actor = ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        board_id="board-1",
        permissions=["specs:update"],
    )
    assert not decide_authorization(legacy_actor, requirement).allowed

    canonical_without_historical = PermissionSet(
        {
            "spec": {
                "entity": {"manage_dependencies": True},
                "interact_in": {"draft": True},
            }
        }
    )
    assert not decide_authorization(
        legacy_actor,
        requirement,
        permissions=canonical_without_historical,
    ).allowed

    explicit_pair = PermissionSet(
        {
            "spec": {
                "entity": {
                    "manage_dependencies": True,
                    "edit_fields": True,
                },
                "interact_in": {"draft": True},
            }
        }
    )
    assert decide_authorization(
        legacy_actor,
        requirement,
        permissions=explicit_pair,
    ).allowed
    assert SKM_PERMISSION_INTRODUCTION_V1.legacy_compatible is False
    assert SKM_PERMISSION_INTRODUCTION_V1.leaves == ("spec.entity.manage_dependencies",)
    assert PERMISSION_INTRODUCTION_MANIFESTS[-1] is SKM_PERMISSION_INTRODUCTION_V1
    assert skm_permission_introduction_v1() is SKM_PERMISSION_INTRODUCTION_V1
    assert permission_introduction_manifests()[-1] is SKM_PERMISSION_INTRODUCTION_V1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "can_manage"),
    (("rest", False), ("mcp", False), ("rest", True)),
)
async def test_list_use_case_derives_and_enforces_remove_capability(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    can_manage: bool,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    assert (
        "can_manage_dependencies"
        not in ListSpecDependenciesCommand.__dataclass_fields__
    )
    spec = SimpleNamespace(
        id="source",
        board_id="board-1",
        status=SpecStatus.DRAFT,
    )

    events: list[str] = []

    async def load_spec(*_: object, **__: object) -> object:
        events.append("load_spec")
        return spec

    page = SpecDependencyPage(
        items=(
            SpecDependencyListItem(
                dependency=_record(),
                direction=SpecDependencyDirection.OUTGOING,
                related_spec=_snapshot("target", status=SpecStatus.DONE),
                satisfied=True,
                retrospective=False,
                same_ideation=False,
                # Deliberately over-permissive: the use case must still mask
                # this value for a reader-only principal.
                capabilities=SpecDependencyCapabilities(can_remove=True),
            ),
        ),
        total=1,
        next_cursor=None,
        readiness=SpecDependencyReadiness(
            spec_id="source",
            board_id="board-1",
            current_edition=2,
            last_started_edition=None,
            active_dependency_count=1,
            blocking_count=0,
            archived_blocking_count=0,
            unfinished_blocking_count=0,
            blockers_truncated=False,
        ),
    )

    class DependencyReader:
        query = None

        async def list_page(self, query: object) -> SpecDependencyPage:
            events.append("list_page")
            self.query = query
            return page

    dependency_reader = DependencyReader()
    permission_flags: dict[str, object] = {"spec": {"entity": {"read": True}}}
    if can_manage:
        permission_flags = {
            "spec": {
                "entity": {
                    "read": True,
                    "manage_dependencies": True,
                    "edit_fields": True,
                },
                "interact_in": {"draft": True},
            }
        }
    actor = ActorContext(
        "user-1",
        source,
        actor_kind="agent" if source == "mcp" else "user",
        board_id="board-1",
        permissions=(
            ["spec.entity.read"] if source == "mcp" else PermissionSet(permission_flags)
        ),
    )
    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)

    class Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(spec_dependencies=dependency_reader)

        async def begin_consistent_read(self) -> None:
            events.append("begin_consistent_read")

    result = await ListSpecDependenciesUseCase().execute(
        ListSpecDependenciesCommand(
            spec_id="source",
            board_id="board-1",
            direction=SpecDependencyDirection.OUTGOING,
        ),
        actor=actor,
        uow=Uow(),
    )

    assert events == ["begin_consistent_read", "load_spec", "list_page"]
    assert dependency_reader.query is not None
    assert dependency_reader.query.can_manage_dependencies is can_manage
    capability = result.page.items[0].capabilities
    assert capability.can_remove is can_manage
    assert capability.removal_blocked_reason == (
        None if can_manage else "permission_denied"
    )


@pytest.mark.asyncio
async def test_readiness_starts_consistent_read_before_spec_load_and_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

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

    async def authorize(*_: object, **__: object) -> None:
        events.append("authorize")

    class DependencyReader:
        async def get_readiness(self, **_: object) -> SpecDependencyReadiness:
            events.append("get_readiness")
            return readiness

    class Uow:
        services = SimpleNamespace(spec_dependencies=DependencyReader())

        async def begin_consistent_read(self) -> None:
            events.append("begin_consistent_read")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    result = await GetSpecDependencyReadinessUseCase().execute(
        GetSpecDependencyReadinessCommand(spec_id="source"),
        actor=ActorContext(
            "user-1",
            "rest",
            actor_kind="user",
            board_id="board-1",
            permissions=PermissionSet({"spec": {"entity": {"read": True}}}),
        ),
        uow=Uow(),
    )

    assert result.readiness is readiness
    assert events == [
        "begin_consistent_read",
        "load_spec",
        "authorize",
        "get_readiness",
    ]


@pytest.mark.asyncio
async def test_add_use_case_normalizes_only_missing_target_without_disclosure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    source = SimpleNamespace(
        id="source",
        board_id="board-1",
        status=SpecStatus.DRAFT,
    )

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        if spec_id == "source":
            return source
        raise EntityNotFoundError("spec", spec_id)

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)

    async def permit(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_dependency_permission", permit)
    command = AddSpecDependencyCommand(
        spec_id="source",
        target_spec_id="private-target",
        expected_spec_version=7,
        expected_spec_edition=2,
        idempotency_key="safe-key",
    )
    with pytest.raises(SpecDependencyOperationError) as caught:
        await AddSpecDependencyUseCase().execute(
            command,
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=["*"],
            ),
            uow=SimpleNamespace(),
        )

    assert caught.value.code == "dependency_target_unavailable"
    assert caught.value.facts == {"spec_id": "source"}
    assert "private-target" not in str(caught.value.to_dict())

    async def missing_source(*_: object, **__: object) -> object:
        raise EntityNotFoundError("spec", "source")

    monkeypatch.setattr(module, "_require_actor_board_spec", missing_source)
    with pytest.raises(EntityNotFoundError):
        await AddSpecDependencyUseCase().execute(
            command,
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=["*"],
            ),
            uow=SimpleNamespace(),
        )


@pytest.mark.asyncio
async def test_add_use_case_authorizes_source_before_target_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module
    from okto_pulse.core.application.use_cases.base import PermissionDeniedError

    calls: list[str] = []

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        calls.append(f"load:{spec_id}")
        return SimpleNamespace(
            id=spec_id,
            board_id="board-1",
            status=SpecStatus.DRAFT,
        )

    async def deny(*_: object, **__: object) -> None:
        calls.append("authorize")
        raise PermissionDeniedError("permission_missing")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", deny)
    with pytest.raises(PermissionDeniedError):
        await AddSpecDependencyUseCase().execute(
            AddSpecDependencyCommand(
                spec_id="source",
                target_spec_id="target",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="key",
            ),
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=(),
            ),
            uow=SimpleNamespace(),
        )

    assert calls == ["load:source", "authorize"]


@pytest.mark.asyncio
async def test_add_use_case_requires_both_endpoint_reads_and_masks_target_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module
    from okto_pulse.core.application.use_cases.base import PermissionDeniedError

    calls: list[str] = []

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        calls.append(f"load:{spec_id}")
        return SimpleNamespace(
            id=spec_id,
            board_id="board-1",
            status=SpecStatus.DRAFT,
        )

    async def authorize(
        _actor: ActorContext,
        _uow: object,
        spec: object,
        *,
        write: bool,
    ) -> None:
        spec_id = str(getattr(spec, "id"))
        calls.append(f"authorize:{spec_id}:{'write' if write else 'read'}")
        if spec_id == "private-target":
            raise PermissionDeniedError("spec.entity.read")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    with pytest.raises(SpecDependencyOperationError) as caught:
        await AddSpecDependencyUseCase().execute(
            AddSpecDependencyCommand(
                spec_id="source",
                target_spec_id="private-target",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="read-fence",
            ),
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=("spec.entity.manage_dependencies",),
            ),
            uow=SimpleNamespace(),
        )

    assert calls == [
        "load:source",
        "authorize:source:write",
        "authorize:source:read",
        "load:private-target",
        "authorize:private-target:read",
    ]
    assert caught.value.code == "dependency_target_unavailable"
    assert caught.value.facts == {"spec_id": "source"}
    assert "private-target" not in str(caught.value.to_dict())


@pytest.mark.asyncio
async def test_remove_use_case_requires_source_read_after_manage_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module
    from okto_pulse.core.application.use_cases.base import PermissionDeniedError

    calls: list[str] = []
    source = SimpleNamespace(
        id="source",
        board_id="board-1",
        status=SpecStatus.DRAFT,
    )

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        calls.append(f"load:{spec_id}:{'write' if write else 'read'}")
        return source

    async def authorize(
        _actor: ActorContext,
        _uow: object,
        spec: object,
        *,
        write: bool,
    ) -> None:
        calls.append(
            f"authorize:{getattr(spec, 'id')}:{'write' if write else 'read'}"
        )
        if not write:
            raise PermissionDeniedError("spec.entity.read")

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)

    with pytest.raises(PermissionDeniedError, match="spec.entity.read"):
        await RemoveSpecDependencyUseCase().execute(
            RemoveSpecDependencyCommand(
                spec_id="source",
                dependency_id="dependency-1",
                reason="No longer required",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="remove-read-fence",
            ),
            actor=ActorContext(
                "agent-1",
                "mcp",
                actor_kind="agent",
                board_id="board-1",
                permissions=("spec.entity.manage_dependencies",),
            ),
            uow=SimpleNamespace(),
        )

    assert calls == [
        "load:source:write",
        "authorize:source:write",
        "authorize:source:read",
    ]


@pytest.mark.asyncio
async def test_mutation_use_cases_bind_stateful_authorization_to_source_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    source = SimpleNamespace(
        id="source",
        board_id="board-1",
        status=SpecStatus.REVIEW,
    )
    target = SimpleNamespace(
        id="target",
        board_id="board-1",
        status=SpecStatus.DONE,
    )

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        return source if spec_id == source.id else target

    async def permit(*_: object, **__: object) -> None:
        return None

    calls: dict[str, dict[str, object]] = {}
    receipt = SpecDependencyMutationReceipt(
        operation="add",
        dependency=_record(),
        source_spec=_snapshot("source", status=SpecStatus.REVIEW, version=8),
        request_digest="digest",
        satisfied=True,
    )

    class DependencyService:
        async def add_dependency(
            self, **kwargs: object
        ) -> SpecDependencyMutationReceipt:
            calls["add"] = dict(kwargs)
            return receipt

        async def remove_dependency(
            self, **kwargs: object
        ) -> SpecDependencyMutationReceipt:
            calls["remove"] = dict(kwargs)
            return replace(receipt, operation="remove")

    class Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(spec_dependencies=DependencyService())
            self.commit_calls = 0

        async def commit(self) -> None:
            self.commit_calls += 1

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", permit)
    uow = Uow()
    actor = ActorContext(
        "agent-1",
        "mcp",
        actor_kind="agent",
        actor_name="Agent",
        board_id="board-1",
        permissions=["*"],
    )

    await AddSpecDependencyUseCase().execute(
        AddSpecDependencyCommand(
            spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="add-status-fence",
        ),
        actor=actor,
        uow=uow,
    )
    await RemoveSpecDependencyUseCase().execute(
        RemoveSpecDependencyCommand(
            spec_id="source",
            dependency_id="dependency-1",
            reason="No longer required",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="remove-status-fence",
        ),
        actor=actor,
        uow=uow,
    )

    assert calls["add"]["expected_spec_status"] is SpecStatus.REVIEW
    assert calls["remove"]["expected_spec_status"] is SpecStatus.REVIEW
    assert uow.commit_calls == 2


def test_mcp_dependency_list_projection_matches_closed_public_contract() -> None:
    from okto_pulse.core.mcp.server import (
        _spec_dependency_error_payload,
        _spec_dependency_page_payload,
    )

    record = replace(
        _record(),
        source_version_on_remove=11,
        removed_at=datetime.now(timezone.utc),
        removal_reason="obsolete",
    )
    page = SpecDependencyPage(
        items=(
            SpecDependencyListItem(
                dependency=record,
                direction=SpecDependencyDirection.OUTGOING,
                related_spec=_snapshot("target", status=SpecStatus.DONE),
                satisfied=True,
                retrospective=False,
                same_ideation=True,
                capabilities=SpecDependencyCapabilities(
                    can_remove=False,
                    can_navigate=True,
                    removal_blocked_reason="source_not_editable",
                ),
            ),
        ),
        total=1,
        next_cursor="opaque",
        readiness=SpecDependencyReadiness(
            spec_id="source",
            board_id="board-1",
            current_edition=2,
            last_started_edition=None,
            active_dependency_count=1,
            blocking_count=0,
            archived_blocking_count=0,
            unfinished_blocking_count=0,
            blockers_truncated=False,
        ),
    )

    payload = _spec_dependency_page_payload(page, direction="depends_on")
    assert payload["direction"] == "depends_on"
    item = payload["items"][0]
    assert item["direction"] == "depends_on"
    assert item["removed_at_spec_version"] == 11
    assert "source_version_on_remove" not in item
    assert item["capabilities"] == {
        "can_remove": False,
        "remove_reason_code": "source_not_editable",
        "can_navigate": True,
    }
    assert "removal_blocked_reason" not in item["capabilities"]
    source_missing = json.loads(
        _spec_dependency_error_payload(EntityNotFoundError("spec", "source"))
    )
    assert source_missing == {
        "error": "spec_not_found",
        "code": "spec_not_found",
        "message": "Spec was not found in the requested board.",
        "retryable": False,
    }


def test_full_spec_readiness_uses_same_public_shape_as_rest_and_mcp() -> None:
    from okto_pulse.core.models.schemas import SpecDependencyReadinessResponse

    readiness = SpecDependencyReadiness(
        spec_id="source",
        board_id="board-1",
        current_edition=2,
        last_started_edition=1,
        active_dependency_count=1,
        blocking_count=1,
        archived_blocking_count=1,
        unfinished_blocking_count=0,
        blockers_truncated=False,
        blockers=(
            SpecDependencyBlocker(
                dependency_id="dep-1",
                source_spec_id="source",
                target_spec_id="target",
                target_title="Target",
                target_status=SpecStatus.DONE,
                target_edition=3,
                target_version=9,
                target_archived=True,
            ),
        ),
    )
    projection = spec_dependency_readiness_projection(readiness)
    validated = SpecDependencyReadinessResponse.model_validate(projection)
    assert validated.model_dump(mode="json") == projection
    assert projection["archived_blocking_count"] == 1
    assert projection["unfinished_blocking_count"] == 0
    assert projection["blockers_truncated"] is False
    assert projection["blockers"][0]["dependent_spec_id"] == "source"
    assert projection["blockers"][0]["prerequisite_spec_id"] == "target"
    assert "source_spec_id" not in projection["blockers"][0]
    assert "target_spec_id" not in projection["blockers"][0]


@pytest.mark.asyncio
async def test_readiness_detail_limit_preserves_zero_and_bounds_other_values() -> None:
    readiness = SpecDependencyReadiness(
        spec_id="source",
        board_id="board-1",
        current_edition=2,
        last_started_edition=None,
        active_dependency_count=1,
        blocking_count=1,
        archived_blocking_count=0,
        unfinished_blocking_count=1,
        blockers_truncated=True,
    )
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source")},
        readiness=readiness,
    )

    result = await SpecDependencyService(port, object()).get_readiness(
        board_id="board-1",
        spec_id="source",
        blocker_limit=0,
    )

    assert result is readiness
    assert port.readiness_calls == [
        {"board_id": "board-1", "spec_id": "source", "blocker_limit": 0}
    ]
    assert normalize_spec_dependency_blocker_limit(-1) == 0
    assert normalize_spec_dependency_blocker_limit(101) == 100


def test_dependency_query_rejects_cursor_beyond_shared_core_bound() -> None:
    with pytest.raises(SpecDependencyOperationError) as caught:
        SpecDependencyListQuery(
            board_id="board-1",
            spec_id="source",
            direction=SpecDependencyDirection.OUTGOING,
            cursor="x" * (SPEC_DEPENDENCY_CURSOR_MAX_LENGTH + 1),
        )

    assert caught.value.code == "invalid_cursor"


@pytest.mark.asyncio
async def test_dependency_service_rejects_removal_reason_beyond_shared_core_bound(
) -> None:
    service = SpecDependencyService(object(), object())

    with pytest.raises(SpecDependencyOperationError) as caught:
        await service.remove_dependency(
            board_id="board-1",
            source_spec_id="source",
            dependency_id="dependency-1",
            reason="x" * (SPEC_DEPENDENCY_REMOVAL_REASON_MAX_LENGTH + 1),
            expected_spec_version=1,
            expected_spec_edition=1,
            idempotency_key="remove-key",
            actor_id="actor-1",
            actor_type="user",
            actor_name="User",
        )

    assert caught.value.code == "invalid_spec_dependency_request"


@pytest.mark.asyncio
async def test_active_duplicate_is_rejected_without_any_durable_write() -> None:
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source"), "target": _snapshot("target")},
        existing=_record(),
    )
    service = SpecDependencyService(port, object())

    with pytest.raises(SpecDependencyOperationError) as caught:
        await service.add_dependency(
            board_id="board-1",
            source_spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="new-key",
            actor_id="actor-1",
            actor_type="user",
            actor_name="User",
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts["conflict_kind"] == "active_duplicate"
    assert caught.value.to_dict()["retryable"] is False
    assert port.write_calls == []


@pytest.mark.asyncio
async def test_add_locks_both_endpoint_specs_in_lexicographic_order() -> None:
    port = _FakeDependencyPersistence(
        {
            "z-source": _snapshot(
                "z-source",
                status=SpecStatus.VALIDATED,
                last_started_edition=2,
            ),
            "a-target": _snapshot("a-target", status=SpecStatus.VALIDATED),
        }
    )
    service = SpecDependencyService(port, object())

    with pytest.raises(SpecDependencyOperationError) as caught:
        await service.add_dependency(
            board_id="board-1",
            source_spec_id="z-source",
            target_spec_id="a-target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="ordered-locks",
            actor_id="actor-1",
            actor_type="user",
            actor_name=None,
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert port.snapshot_lock_order == ["a-target", "z-source"]
    assert port.write_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("add", "remove"))
async def test_mutation_rejects_status_changed_after_stateful_authorization(
    operation: str,
) -> None:
    port = _FakeDependencyPersistence(
        {
            "source": _snapshot("source", status=SpecStatus.VALIDATED),
            "target": _snapshot("target", status=SpecStatus.DONE),
        }
    )
    service = SpecDependencyService(port, object())

    with pytest.raises(SpecDependencyOperationError) as caught:
        if operation == "add":
            await service.add_dependency(
                board_id="board-1",
                source_spec_id="source",
                target_spec_id="target",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="status-race-add",
                actor_id="actor-1",
                actor_type="user",
                actor_name="User",
                expected_spec_status=SpecStatus.REVIEW,
            )
        else:
            await service.remove_dependency(
                board_id="board-1",
                source_spec_id="source",
                dependency_id="dependency-1",
                reason="No longer required",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="status-race-remove",
                actor_id="actor-1",
                actor_type="user",
                actor_name="User",
                expected_spec_status=SpecStatus.REVIEW,
            )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts == {"spec_id": "source"}
    assert port.write_calls == []


@pytest.mark.asyncio
async def test_idempotency_lookup_is_scoped_by_actor_type() -> None:
    source = _snapshot("source")
    record = _record()
    replay = SpecDependencyMutationReceipt(
        operation="add",
        dependency=record,
        source_spec=source,
        request_digest="different-digest-is-never-read-for-agent",
        satisfied=True,
    )
    port = _FakeDependencyPersistence(
        {"source": source, "target": _snapshot("target")},
        existing=record,
        replay_by_actor_type={"user": replay},
    )
    service = SpecDependencyService(port, object())

    with pytest.raises(SpecDependencyOperationError) as caught:
        await service.add_dependency(
            board_id="board-1",
            source_spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="same-key",
            actor_id="same-id",
            actor_type="agent",
            actor_name=None,
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert port.lookup_actor_types == ["agent"]


@pytest.mark.asyncio
async def test_visible_cross_board_target_uses_explicit_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okto_pulse.core.application.use_cases.spec_dependencies as module

    source = SimpleNamespace(id="source", board_id="board-1", status=SpecStatus.DRAFT)
    target = SimpleNamespace(id="target", board_id="board-2", status=SpecStatus.DONE)

    async def resolve(
        _uow: object, spec_id: str, _actor: object, **_: object
    ) -> object:
        return source if spec_id == "source" else target

    async def authorize(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_actor_board_spec", resolve)
    monkeypatch.setattr(module, "_require_dependency_permission", authorize)
    actor = ActorContext("human-1", "rest", actor_kind="human")

    with pytest.raises(SpecDependencyOperationError) as caught:
        await AddSpecDependencyUseCase().execute(
            AddSpecDependencyCommand("source", "target", 7, 2, "key"),
            actor=actor,
            uow=SimpleNamespace(services=SimpleNamespace()),
        )

    assert caught.value.code == "cross_board_dependency_forbidden"
    assert caught.value.to_dict()["retryable"] is False


def test_done_is_the_only_satisfying_target_and_cycle_check_is_directed() -> None:
    assert spec_dependency_is_satisfied(SpecStatus.DONE)
    assert not spec_dependency_is_satisfied(
        SpecStatus.DONE,
        target_archived=True,
    )
    assert not spec_dependency_is_satisfied(SpecStatus.CANCELLED)
    assert spec_dependency_would_create_cycle(
        (("a", "b"), ("b", "c")),
        source_spec_id="c",
        target_spec_id="a",
    )
    assert not spec_dependency_would_create_cycle(
        (("a", "b"),),
        source_spec_id="a",
        target_spec_id="c",
    )
    assert spec_dependency_cycle_path(
        (("a", "b"), ("b", "c")),
        source_spec_id="c",
        target_spec_id="a",
    ) == ("c", "a", "b", "c")


def test_every_card_execution_start_or_resume_edge_uses_dependency_blocker() -> None:
    gated_edges = {
        (CardStatus.NOT_STARTED, CardStatus.STARTED),
        (CardStatus.NOT_STARTED, CardStatus.IN_PROGRESS),
        (CardStatus.STARTED, CardStatus.IN_PROGRESS),
        (CardStatus.ON_HOLD, CardStatus.STARTED),
        (CardStatus.ON_HOLD, CardStatus.IN_PROGRESS),
        (CardStatus.VALIDATION, CardStatus.IN_PROGRESS),
        (CardStatus.DONE, CardStatus.IN_PROGRESS),
    }
    blocker = SimpleNamespace(
        dependency_id="dep",
        target_spec_id="prerequisite",
        target_title="Prerequisite",
        target_status=SpecStatus.VALIDATED,
    )

    for old, new in gated_edges:
        assert transition_starts_card_execution(old, new)
        decision = evaluate_card_transition(
            CardTransitionFacts(
                card_id="card",
                old_status=old,
                new_status=new,
                spec_id="source",
                spec_dependency_blockers=(blocker,),
                spec_dependency_blocking_count=1,
                spec_dependency_archived_blocking_count=0,
                spec_dependency_unfinished_blocking_count=1,
            )
        )
        assert not decision.allowed
        assert decision.block is not None
        assert decision.block.code == "spec_dependencies_incomplete"

    # The registry's normal-card-only rollback edge is intentionally excluded
    # from Test/Bug execution semantics and therefore from precedence gating.
    to_started = [
        edge
        for edge in transition_contracts("card", "in_progress")
        if edge.to_status == "started"
    ]
    assert len(to_started) == 1
    assert to_started[0].card_types == ("normal",)


def test_spec_gate_readiness_surfaces_precedence_as_active_gate() -> None:
    result = spec_gate_readiness(
        spec_id="source",
        spec_status="validated",
        require_spec_validation=True,
        cognitive_enforcement_active=False,
        dependency_readiness={
            "ready": False,
            "blocking_count": 1,
            "archived_blocking_count": 0,
            "unfinished_blocking_count": 1,
            "blockers_truncated": False,
            "blockers": [{"target_spec_id": "prerequisite"}],
        },
    )

    assert result["active_gates"][0]["gate_type"] == GATE_SPEC_DEPENDENCIES
    assert result["active_gates"][0]["blocked_transition"] == (
        "validated_to_in_progress"
    )
    assert result["active_gates"][0]["archived_blocking_count"] == 0
    assert result["active_gates"][0]["unfinished_blocking_count"] == 1
    assert result["active_gates"][0]["blockers_truncated"] is False
    assert result["consistency"]["mismatch"] is False


def test_error_retryability_and_mcp_inventory_are_closed() -> None:
    assert (
        SpecDependencyOperationError(
            "spec_dependency_version_conflict", "changed"
        ).to_dict()["retryable"]
        is True
    )
    for code in (
        "spec_dependency_state_conflict",
        "spec_dependency_cycle",
        "invalid_spec_dependency_request",
        "dependency_target_unavailable",
    ):
        assert (
            SpecDependencyOperationError(code, "blocked").to_dict()["retryable"]
            is False
        )

    policies = {
        policy.tool_name: policy.permission_flags
        for policy in MCP_TOOL_PERMISSION_POLICIES
    }
    assert policies["okto_pulse_add_spec_dependency"] == (
        "spec.entity.manage_dependencies",
        "spec.entity.read",
    )
    assert policies["okto_pulse_remove_spec_dependency"] == (
        "spec.entity.manage_dependencies",
        "spec.entity.read",
    )
    assert policies["okto_pulse_list_spec_dependencies"] == ("spec.entity.read",)
    assert "okto_pulse_list_spec_dependencies" in MCP_READER_TOOL_NAMES
    assert "okto_pulse_get_spec_dependency_readiness" not in policies
    assert "okto_pulse_get_spec_dependency_readiness" not in MCP_READER_TOOL_NAMES


def test_spec_dependency_mcp_resource_documents_the_three_tool_contract() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    docs = mcp_server._load_resource_file("reference/tool-docs/spec.md")
    for tool_name in (
        "okto_pulse_add_spec_dependency",
        "okto_pulse_remove_spec_dependency",
        "okto_pulse_list_spec_dependencies",
    ):
        assert f"## `{tool_name}`" in docs

    normalized = " ".join(docs.split())
    assert "there is no separate readiness tool" in normalized
    assert "expected_spec_version" in normalized
    assert "expected_spec_edition" in normalized
    assert "idempotency_key_reuse" in normalized
    assert "opaque keyset pagination" in normalized
    assert "target_archived" in normalized
    assert "remove_reason_code" in normalized
    remove_docs = docs.split("## `okto_pulse_remove_spec_dependency`", 1)[1].split(
        "## `okto_pulse_list_spec_dependencies`", 1
    )[0]
    assert "`spec.entity.manage_dependencies`" in remove_docs
    assert "`spec.entity.read`" in remove_docs


def test_only_validated_to_in_progress_starts_spec_execution() -> None:
    assert transition_starts_spec_execution(
        SpecStatus.VALIDATED,
        SpecStatus.IN_PROGRESS,
    )
    for source_status in (
        SpecStatus.DRAFT,
        SpecStatus.REVIEW,
        SpecStatus.APPROVED,
        SpecStatus.DONE,
        SpecStatus.CANCELLED,
        SpecStatus.IN_PROGRESS,
    ):
        assert not transition_starts_spec_execution(
            source_status,
            SpecStatus.IN_PROGRESS,
        )
    assert not transition_starts_spec_execution(
        SpecStatus.VALIDATED,
        SpecStatus.DONE,
    )


@pytest.mark.asyncio
async def test_same_principal_same_request_is_a_side_effect_free_replay() -> None:
    import okto_pulse.core.services.spec_dependency as dependency_module

    source = _snapshot("source")
    request_digest = dependency_module._request_digest(
        "add",
        {
            "board_id": "board-1",
            "source_spec_id": "source",
            "target_spec_id": "target",
            "expected_spec_version": 7,
            "expected_spec_edition": 2,
        },
    )
    receipt = SpecDependencyMutationReceipt(
        operation="add",
        dependency=_record(),
        source_spec=source,
        request_digest=request_digest,
        satisfied=True,
    )
    port = _FakeDependencyPersistence(
        {"source": source, "target": _snapshot("target")},
        replay_by_actor_type={"agent": receipt},
    )
    service = SpecDependencyService(port, object())
    service._record_effects = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("a replay cannot emit durable effects")
    )

    result = await service.add_dependency(
        board_id="board-1",
        source_spec_id="source",
        target_spec_id="target",
        expected_spec_version=7,
        expected_spec_edition=2,
        idempotency_key="same-key",
        actor_id="same-id",
        actor_type="agent",
        actor_name="Agent",
    )

    assert result.replayed is True
    assert result.dependency.id == "dependency-1"
    assert port.board_lock_calls == ["board-1"]
    assert port.lookup_actor_types == ["agent"]
    assert port.snapshot_lock_order == []
    assert port.write_calls == []
    service._record_effects.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_idempotency_key_with_a_different_digest_is_closed_conflict() -> (
    None
):
    receipt = SpecDependencyMutationReceipt(
        operation="add",
        dependency=_record(),
        source_spec=_snapshot("source"),
        request_digest="digest-for-another-request",
        satisfied=True,
    )
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source"), "target": _snapshot("target")},
        replay_by_actor_type={"user": receipt},
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).add_dependency(
            board_id="board-1",
            source_spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="same-key",
            actor_id="same-id",
            actor_type="user",
            actor_name="User",
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts == {"conflict_kind": "idempotency_key_reuse"}
    assert caught.value.to_dict()["retryable"] is False
    assert port.snapshot_lock_order == []
    assert port.write_calls == []


@pytest.mark.asyncio
async def test_remove_receipt_snapshots_current_target_satisfaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependency = _record()
    state = _AtomicDependencyState(
        snapshots={
            "source": _snapshot("source"),
            # The immutable edge says Done on creation, while the current
            # prerequisite has since returned to Review.
            "target": _snapshot("target", status=SpecStatus.REVIEW, version=9),
        },
        dependencies={dependency.id: dependency},
    )
    transaction = _AtomicDependencyTransaction(state, fail_after="never")
    _install_atomic_effect_ports(monkeypatch, transaction)

    async with transaction:
        receipt = await SpecDependencyService(
            transaction,
            transaction,
        ).remove_dependency(
            board_id="board-1",
            source_spec_id="source",
            dependency_id=dependency.id,
            reason="No longer required",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="current-satisfaction",
            actor_id="actor-1",
            actor_type="user",
            actor_name="User",
        )
        await transaction.commit()

    assert receipt.dependency.target_status_on_create is SpecStatus.DONE
    assert receipt.satisfied is False
    assert next(iter(state.receipts.values())).satisfied is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fail_after",
    (
        "insert_dependency",
        "bump_source_spec_version",
        "store_mutation_receipt",
        "spec.version_bumped",
        "spec.dependency_added",
        "activity_log",
        "spec_history",
        "commit",
    ),
)
async def test_ac19_add_fault_injection_rolls_back_the_complete_mutation(
    monkeypatch: pytest.MonkeyPatch,
    fail_after: str,
) -> None:
    state = _AtomicDependencyState(
        snapshots={
            "source": _snapshot("source"),
            "target": _snapshot("target", status=SpecStatus.DONE, version=3),
        },
        events=["unrelated.baseline"],
        application_records=[("baseline", {"id": "unrelated"})],
    )
    before = copy.deepcopy(state)
    transaction = _AtomicDependencyTransaction(state, fail_after=fail_after)
    _install_atomic_effect_ports(monkeypatch, transaction)
    service = SpecDependencyService(transaction, transaction)

    with pytest.raises(_InjectedAtomicFailure, match=fail_after):
        async with transaction:
            await service.add_dependency(
                board_id="board-1",
                source_spec_id="source",
                target_spec_id="target",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="atomic-add",
                actor_id="actor-1",
                actor_type="user",
                actor_name="User",
            )
            await transaction.commit()

    # Dependency, version, replay receipt, outbox events and audit records are
    # one atomic relational mutation. No injected post-write failure may leave
    # any subset visible in committed state.
    assert state == before
    assert transaction.rollback_calls == 1
    assert transaction.completed_stages[-1] == fail_after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fail_after",
    (
        "tombstone_dependency",
        "bump_source_spec_version",
        "store_mutation_receipt",
        "spec.version_bumped",
        "spec.dependency_removed",
        "activity_log",
        "spec_history",
        "commit",
    ),
)
async def test_ac19_remove_fault_injection_rolls_back_the_complete_mutation(
    monkeypatch: pytest.MonkeyPatch,
    fail_after: str,
) -> None:
    dependency = _record()
    state = _AtomicDependencyState(
        snapshots={
            "source": _snapshot("source"),
            "target": _snapshot("target", status=SpecStatus.DONE, version=3),
        },
        dependencies={dependency.id: dependency},
        events=["unrelated.baseline"],
        application_records=[("baseline", {"id": "unrelated"})],
    )
    before = copy.deepcopy(state)
    transaction = _AtomicDependencyTransaction(state, fail_after=fail_after)
    _install_atomic_effect_ports(monkeypatch, transaction)
    service = SpecDependencyService(transaction, transaction)

    with pytest.raises(_InjectedAtomicFailure, match=fail_after):
        async with transaction:
            await service.remove_dependency(
                board_id="board-1",
                source_spec_id="source",
                dependency_id=dependency.id,
                reason="Prerequisite no longer applies",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="atomic-remove",
                actor_id="actor-1",
                actor_type="user",
                actor_name="User",
            )
            await transaction.commit()

    assert state == before
    assert state.dependencies[dependency.id].active is True
    assert transaction.rollback_calls == 1
    assert transaction.completed_stages[-1] == fail_after


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "legacy_status",
    (SpecStatus.IN_PROGRESS, SpecStatus.DONE, SpecStatus.CANCELLED),
)
async def test_legacy_terminal_source_without_marker_is_still_post_start(
    legacy_status: SpecStatus,
) -> None:
    port = _FakeDependencyPersistence(
        {
            "source": _snapshot(
                "source",
                status=legacy_status,
                last_started_edition=None,
            ),
            "target": _snapshot("target", status=SpecStatus.VALIDATED),
        }
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).add_dependency(
            board_id="board-1",
            source_spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key=f"legacy-{legacy_status.value}",
            actor_id="actor-1",
            actor_type="user",
            actor_name=None,
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts["target_status"] == "validated"
    assert port.write_calls == []


@pytest.mark.asyncio
async def test_execution_marker_is_written_once_per_lifecycle_edition() -> None:
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
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source", status=SpecStatus.VALIDATED)},
        readiness=readiness,
    )
    service = SpecDependencyService(port, object())

    await service.require_ready_for_execution(
        board_id="board-1",
        spec_id="source",
        mark_started=True,
        expected_edition=2,
    )
    await service.require_ready_for_execution(
        board_id="board-1",
        spec_id="source",
        mark_started=True,
        expected_edition=2,
    )
    port.readiness = replace(
        port.readiness,
        current_edition=3,
        last_started_edition=2,
    )
    port.snapshots["source"] = replace(
        port.snapshots["source"],
        edition=3,
        last_started_edition=2,
    )
    await service.require_ready_for_execution(
        board_id="board-1",
        spec_id="source",
        mark_started=True,
        expected_edition=3,
    )

    assert port.mark_started_calls == [2, 3]


@pytest.mark.asyncio
async def test_lifecycle_fence_can_be_reused_without_double_locking() -> None:
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
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source", status=SpecStatus.VALIDATED)},
        readiness=readiness,
    )
    service = SpecDependencyService(port, object())
    await service.acquire_lifecycle_write_fence(board_id="board-1")
    await service.require_ready_for_execution(
        board_id="board-1",
        spec_id="source",
        mark_started=True,
        expected_edition=2,
        acquire_graph_lock=False,
    )
    assert port.board_lock_calls == ["board-1"]
    assert port.mark_started_calls == [2]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("locked_snapshot", "expected_status", "expected_archived"),
    (
        (
            _snapshot("source", status=SpecStatus.VALIDATED),
            SpecStatus.IN_PROGRESS,
            False,
        ),
        (
            _snapshot("source", status=SpecStatus.IN_PROGRESS, archived=True),
            SpecStatus.IN_PROGRESS,
            False,
        ),
    ),
)
async def test_execution_gate_rejects_lifecycle_change_under_graph_fence(
    locked_snapshot: SpecDependencySpecSnapshot,
    expected_status: SpecStatus,
    expected_archived: bool,
) -> None:
    port = _FakeDependencyPersistence(
        {"source": locked_snapshot},
        readiness=SpecDependencyReadiness(
            spec_id="source",
            board_id="board-1",
            current_edition=2,
            last_started_edition=None,
            active_dependency_count=0,
            blocking_count=0,
            archived_blocking_count=0,
            unfinished_blocking_count=0,
            blockers_truncated=False,
        ),
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_ready_for_execution(
            board_id="board-1",
            spec_id="source",
            mark_started=True,
            expected_edition=2,
            expected_status=expected_status,
            expected_archived=expected_archived,
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert port.board_lock_calls == ["board-1"]
    assert port.snapshot_lock_order == ["source"]
    assert port.readiness_calls == []
    assert port.mark_started_calls == []


def test_lifecycle_and_card_writers_take_dependency_fence_at_safe_boundary() -> None:
    from okto_pulse.core.services.main import CardService, SpecService

    spec_source = inspect.getsource(SpecService.move_spec)
    spec_fence = spec_source.index("acquire_lifecycle_write_fence")
    row_fence = spec_source.index("await _application_fence")
    assert spec_fence < row_fence
    assert spec_source.index("require_ready_for_execution") > spec_fence
    assert "acquire_graph_lock=False" in spec_source

    card_source = inspect.getsource(CardService.move_card)
    capture = card_source.index("precedence_expected_edition = int")
    policy_gate = card_source.index("enforce_policy_transition")
    card_dependency_gate = card_source.index("check_dependencies_met")
    readiness_lock = card_source.index("require_ready_for_execution")
    report_mutation = card_source.index("card.conclusions = conclusions")
    cancellation_mutation = card_source.index("apply_cancellation_policy")
    assert capture < policy_gate < card_dependency_gate < readiness_lock
    assert readiness_lock < report_mutation < cancellation_mutation
    assert card_source.count("require_ready_for_execution") == 1
    assert "expected_status=precedence_expected_status" in card_source
    assert "expected_archived=precedence_expected_archived" in card_source


@pytest.mark.asyncio
async def test_execution_gate_rejects_a_changed_edition_even_if_already_marked() -> (
    None
):
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source", last_started_edition=2)},
        readiness=SpecDependencyReadiness(
            spec_id="source",
            board_id="board-1",
            current_edition=2,
            last_started_edition=2,
            active_dependency_count=0,
            blocking_count=0,
            archived_blocking_count=0,
            unfinished_blocking_count=0,
            blockers_truncated=False,
        ),
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_ready_for_execution(
            board_id="board-1",
            spec_id="source",
            mark_started=True,
            expected_edition=1,
        )

    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts["current_spec_edition"] == 2
    assert port.mark_started_calls == []


@pytest.mark.asyncio
async def test_allowed_transition_and_mutation_gate_share_exact_blocking_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocker = SpecDependencyBlocker(
        dependency_id="dep-1",
        source_spec_id="source",
        target_spec_id="target",
        target_title="Prerequisite",
        target_status=SpecStatus.DONE,
        target_edition=4,
        target_version=9,
        target_archived=True,
    )
    readiness = SpecDependencyReadiness(
        spec_id="source",
        board_id="board-1",
        current_edition=2,
        last_started_edition=None,
        active_dependency_count=1,
        blocking_count=1,
        archived_blocking_count=1,
        unfinished_blocking_count=0,
        blockers_truncated=False,
        blockers=(blocker,),
    )
    port = _FakeDependencyPersistence(
        {"source": _snapshot("source", status=SpecStatus.VALIDATED)},
        readiness=readiness,
    )
    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_ready_for_execution(
            board_id="board-1",
            spec_id="source",
            mark_started=True,
            expected_edition=2,
        )

    guidance, remediation = spec_dependency_blocked_guidance(
        archived_blocking_count=1,
        unfinished_blocking_count=0,
    )
    assert "Restore archived prerequisite Specs" in guidance
    assert "remove their dependencies" in guidance
    assert remediation == "restore_archived_prerequisites_or_remove_dependencies"
    assert caught.value.message == guidance
    assert caught.value.remediation == remediation

    async def noop(*_: object, **__: object) -> None:
        return None

    reason = await ListAllowedTransitionsUseCase()._spec_blocked_reason(
        SimpleNamespace(
            boards=SimpleNamespace(get_board=noop),
            cards=SimpleNamespace(
                check_test_coverage=noop,
                check_rules_coverage=noop,
                check_trs_coverage=noop,
                check_contract_coverage=noop,
                check_ir_coverage=noop,
                check_or_coverage=noop,
                check_task_requirement_links_for_spec=noop,
                check_decision_presence=noop,
                check_decisions_coverage=noop,
            ),
        ),
        SimpleNamespace(
            id="source",
            board_id="board-1",
            status=SpecStatus.VALIDATED,
            skip_qualitative_validation=True,
        ),
        "in_progress",
        dependency_readiness=readiness,
    )
    assert reason is not None
    assert reason.startswith(f"spec_dependencies_incomplete: {guidance}")

    card_decision = evaluate_card_transition(
        CardTransitionFacts(
            card_id="card-1",
            old_status=CardStatus.NOT_STARTED,
            new_status=CardStatus.STARTED,
            spec_id="source",
            spec_dependency_blockers=(blocker,),
            spec_dependency_blocking_count=1,
            spec_dependency_archived_blocking_count=1,
            spec_dependency_unfinished_blocking_count=0,
        )
    )
    assert card_decision.block is not None
    assert card_decision.block.detail == guidance
    assert card_decision.block.remediation == remediation

    async def get_readiness(**_: object) -> SpecDependencyReadiness:
        return readiness

    use_case = ListAllowedTransitionsUseCase()

    async def dependency_blocked(*_: object, **__: object) -> str:
        return f"spec_dependencies_incomplete: {guidance}"

    monkeypatch.setattr(use_case, "_blocked_reason", dependency_blocked)
    preview = await use_case._preview_entity_transition(
        SimpleNamespace(
            spec_dependencies=SimpleNamespace(get_readiness=get_readiness),
        ),
        "spec",
        SimpleNamespace(
            id="source",
            board_id="board-1",
            status=SpecStatus.VALIDATED,
        ),
        AllowedTransition(
            to_status="in_progress",
            label="Start",
            gate="execution_readiness",
        ),
    )

    expected = spec_dependency_blocking_facts(
        spec_id="source",
        blockers=(blocker,),
        blocking_count=1,
        archived_blocking_count=1,
        unfinished_blocking_count=0,
        blockers_truncated=False,
    )
    assert caught.value.facts == expected
    assert caught.value.facts["blocking_dependencies"] == [
        {
            "dependency_id": "dep-1",
            "target_spec_id": "target",
            "target_title": "Prerequisite",
            "target_status": "done",
            "target_archived": True,
        }
    ]
    assert preview.blocked_facts == expected
    assert preview.to_dict()["blocked_facts"] == expected


@pytest.mark.asyncio
async def test_readiness_overflow_uses_exact_categories_outside_blocker_sample() -> (
    None
):
    sampled_blockers = tuple(
        SpecDependencyBlocker(
            dependency_id=f"dep-{index:03d}",
            source_spec_id="source",
            target_spec_id=f"unfinished-{index:03d}",
            target_title=f"Unfinished {index:03d}",
            target_status=SpecStatus.VALIDATED,
            target_edition=1,
            target_version=1,
            target_archived=False,
        )
        for index in range(100)
    )
    readiness = SpecDependencyReadiness(
        spec_id="source",
        board_id="board-1",
        current_edition=2,
        last_started_edition=None,
        active_dependency_count=101,
        blocking_count=101,
        archived_blocking_count=1,
        unfinished_blocking_count=100,
        blockers_truncated=True,
        blockers=sampled_blockers,
    )
    assert all(not blocker.target_archived for blocker in readiness.blockers)

    projection = spec_dependency_readiness_projection(readiness)
    assert projection["blocking_count"] == 101
    assert projection["archived_blocking_count"] == 1
    assert projection["unfinished_blocking_count"] == 100
    assert projection["blockers_truncated"] is True
    assert len(projection["blockers"]) == 100

    port = _FakeDependencyPersistence(
        {"source": _snapshot("source", status=SpecStatus.VALIDATED)},
        readiness=readiness,
    )
    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_ready_for_execution(
            board_id="board-1",
            spec_id="source",
            mark_started=True,
            expected_edition=2,
        )

    assert caught.value.remediation == (
        "restore_archived_prerequisites_or_remove_dependencies"
    )
    assert "Restore archived prerequisite Specs" in caught.value.message
    assert "complete unfinished prerequisite Specs" in caught.value.message
    assert caught.value.facts["blocking_count"] == 101
    assert caught.value.facts["archived_blocking_count"] == 1
    assert caught.value.facts["unfinished_blocking_count"] == 100
    assert caught.value.facts["blockers_truncated"] is True
    assert len(caught.value.facts["blocking_dependencies"]) == 100

    gate = spec_gate_readiness(
        spec_id="source",
        spec_status="validated",
        require_spec_validation=True,
        cognitive_enforcement_active=False,
        dependency_readiness=projection,
    )["active_gates"][0]
    assert gate["archived_blocking_count"] == 1
    assert gate["unfinished_blocking_count"] == 100
    assert gate["blockers_truncated"] is True
    assert "Restore archived prerequisite Specs" in gate["operator_action"]


@pytest.mark.asyncio
async def test_archive_guard_excludes_only_internal_sources_and_returns_typed_facts() -> (
    None
):
    incoming = _record(source_id="external", target_id="target")
    port = _FakeDependencyPersistence(
        {"target": _snapshot("target")},
        incoming=(incoming,),
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_no_incoming_active(
            board_id="board-1",
            target_spec_ids=("target", "child"),
            exclude_source_spec_ids=("target", "child"),
            operation="archive Spec tree",
        )

    assert port.incoming_calls == [
        {
            "board_id": "board-1",
            "target_spec_ids": ("target", "child"),
            "exclude_source_spec_ids": ("target", "child"),
            "limit": 101,
        }
    ]
    assert caught.value.code == "spec_dependency_state_conflict"
    assert caught.value.facts["operation"] == "archive Spec tree"
    assert caught.value.facts["incoming_count"] == 1
    assert caught.value.facts["incoming_count_lower_bound"] == 1
    assert caught.value.facts["incoming_dependencies_truncated"] is False
    assert caught.value.facts["incoming_has_more"] is False
    assert caught.value.facts["incoming_dependencies"] == [
        {
            "dependency_id": "dependency-1",
            "source_spec_id": "external",
            "target_spec_id": "target",
        }
    ]
    assert caught.value.to_dict()["retryable"] is False


@pytest.mark.asyncio
async def test_archive_guard_reports_bounded_incoming_facts_without_false_total() -> (
    None
):
    incoming = tuple(
        replace(
            _record(source_id=f"external-{index}", target_id="target"),
            id=f"dependency-{index:03d}",
        )
        for index in range(101)
    )
    port = _FakeDependencyPersistence(
        {"target": _snapshot("target")},
        incoming=incoming,
    )

    with pytest.raises(SpecDependencyOperationError) as caught:
        await SpecDependencyService(port, object()).require_no_incoming_active(
            board_id="board-1",
            target_spec_ids=("target",),
            operation="delete Spec",
        )

    facts = caught.value.facts
    assert "incoming_count" not in facts
    assert facts["incoming_count_lower_bound"] == 101
    assert facts["incoming_dependencies_truncated"] is True
    assert facts["incoming_has_more"] is True
    assert len(facts["incoming_dependencies"]) == 100
    assert port.incoming_calls[0]["limit"] == 101


@pytest.mark.asyncio
async def test_observability_is_typed_timed_and_cardinality_safe(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    reset_spec_dependency_observability_for_tests()
    caplog.set_level(
        "INFO",
        logger="okto_pulse.core.services.spec_dependency_observability",
    )
    blocker = SpecDependencyBlocker(
        dependency_id="dep-secret",
        source_spec_id="source-secret",
        target_spec_id="target-secret",
        target_title="Secret title",
        target_status=SpecStatus.DRAFT,
        target_edition=1,
        target_version=1,
    )
    port = _FakeDependencyPersistence(
        {
            "source-secret": _snapshot("source-secret"),
            "target-secret": _snapshot("target-secret"),
        },
        existing=_record(source_id="source-secret", target_id="target-secret"),
        readiness=SpecDependencyReadiness(
            spec_id="source-secret",
            board_id="board-1",
            current_edition=2,
            last_started_edition=None,
            active_dependency_count=1,
            blocking_count=1,
            archived_blocking_count=0,
            unfinished_blocking_count=1,
            blockers_truncated=False,
            blockers=(blocker,),
        ),
    )
    service = SpecDependencyService(port, object())

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        # The authorization fence intentionally binds the complete canonical
        # target lifecycle identity.  Keep this integration-style fixture as a
        # real dependency snapshot rather than an incomplete transport stub.
        return _snapshot(spec_id)

    async def permit(*_: object, **__: object) -> None:
        return None

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", permit)
    uow = SimpleNamespace(
        services=SimpleNamespace(spec_dependencies=service),
    )

    with pytest.raises(SpecDependencyOperationError):
        await AddSpecDependencyUseCase().execute(
            AddSpecDependencyCommand(
                spec_id="source-secret",
                target_spec_id="target-secret",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="metric-key",
            ),
            actor=ActorContext(
                "actor-secret",
                "mcp",
                actor_kind="agent",
                actor_name="Agent",
                board_id="board-1",
                permissions=["*"],
            ),
            uow=uow,
        )
    with pytest.raises(SpecDependencyOperationError):
        await service.require_ready_for_execution(
            board_id="board-1",
            spec_id="source-secret",
            mark_started=True,
            expected_edition=2,
        )
    projected_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    observe_spec_dependency_projection_lag(
        event_type="spec.dependency_added",
        triggered_at=projected_at - timedelta(seconds=5),
        projected_at=projected_at,
    )

    samples = get_spec_dependency_metric_samples()
    names = {sample["metric_name"] for sample in samples}
    assert {
        METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL,
        METRIC_SPEC_DEPENDENCY_MUTATION_DURATION_MS,
        METRIC_SPEC_DEPENDENCY_CRITICAL_SECTION_DURATION_MS,
        METRIC_SPEC_DEPENDENCY_GATE_TOTAL,
        METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
    }.issubset(names)
    assert any(
        sample["metric_name"] == METRIC_SPEC_DEPENDENCY_MUTATION_TOTAL
        and sample["labels"]
        == {
            "operation": "add",
            "outcome": "policy_conflict",
            "reason_code": "spec_dependency_state_conflict",
        }
        for sample in samples
    )
    assert any(
        sample["metric_name"] == METRIC_SPEC_DEPENDENCY_GATE_TOTAL
        and sample["labels"]
        == {
            "surface": "execution",
            "outcome": "blocked",
            "reason_code": "spec_dependencies_incomplete",
        }
        for sample in samples
    )
    assert any(
        sample["metric_name"] == METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS
        and sample["value"] == 5.0
        for sample in samples
    )
    assert all(
        "id" not in key and "secret" not in value
        for sample in samples
        for key, value in sample["labels"].items()
    )
    mutation_logs = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "spec_dependency.mutation"
    ]
    gate_logs = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "spec_dependency.gate"
    ]
    assert len(mutation_logs) == 1
    assert len(gate_logs) == 1
    mutation_log = mutation_logs[0]
    assert mutation_log.board_id == "board-1"
    assert mutation_log.spec_id == "source-secret"
    assert mutation_log.outcome == "policy_conflict"
    assert mutation_log.reason_code == "spec_dependency_state_conflict"
    assert mutation_log.critical_section_duration_ms is not None
    assert mutation_log.dependency_id is None
    assert mutation_log.version_old is None
    assert mutation_log.version_new is None
    assert "target-secret" not in str(mutation_log.__dict__)
    assert "metric-key" not in str(mutation_log.__dict__)
    assert gate_logs[0].blocking_count == 1
    assert not hasattr(SpecDependencyService.add_dependency, "__wrapped__")
    assert hasattr(AddSpecDependencyUseCase.execute, "__wrapped__")
    with pytest.raises(ValueError, match="spec_dependency_metric_label_invalid"):
        sanitize_spec_dependency_metric_event(
            SpecDependencyMetricEvent(
                METRIC_SPEC_DEPENDENCY_GATE_TOTAL,
                labels={"spec_id": "source-secret"},
            )
        )


@pytest.mark.asyncio
async def test_mutation_observation_spans_commit_and_preserves_receipt(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okto_pulse.core.application.use_cases import spec_dependencies as module

    caplog.set_level(
        "INFO",
        logger="okto_pulse.core.services.spec_dependency_observability",
    )
    receipt = SpecDependencyMutationReceipt(
        operation="add",
        dependency=_record(),
        source_spec=_snapshot("source", version=8),
        request_digest="digest",
        satisfied=True,
        replayed=True,
    )

    async def load_spec(
        _uow: object,
        spec_id: str,
        _actor: ActorContext,
        *,
        write: bool = False,
    ) -> object:
        del write
        return SimpleNamespace(
            id=spec_id,
            board_id="board-1",
            status=SpecStatus.DRAFT,
        )

    async def permit(*_: object, **__: object) -> None:
        return None

    class DependencyService:
        async def add_dependency(self, **_: object) -> SpecDependencyMutationReceipt:
            mark_spec_dependency_critical_section_started()
            return receipt

    class Uow:
        def __init__(self) -> None:
            self.services = SimpleNamespace(spec_dependencies=DependencyService())
            self.committed = False

        async def commit(self) -> None:
            # The mutation context must remain active until commit completes.
            mark_spec_dependency_critical_section_started()
            self.committed = True

    monkeypatch.setattr(module, "_require_actor_board_spec", load_spec)
    monkeypatch.setattr(module, "_require_dependency_permission", permit)
    uow = Uow()
    result = await AddSpecDependencyUseCase().execute(
        AddSpecDependencyCommand(
            spec_id="source",
            target_spec_id="target",
            expected_spec_version=7,
            expected_spec_edition=2,
            idempotency_key="replay-key",
        ),
        actor=ActorContext(
            "agent-1",
            "mcp",
            actor_kind="agent",
            actor_name="Agent",
            board_id="board-1",
            permissions=["*"],
        ),
        uow=uow,
    )

    assert result.receipt is receipt
    assert uow.committed is True
    logs = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "spec_dependency.mutation"
    ]
    assert len(logs) == 1
    assert logs[0].outcome == "replayed"
    assert logs[0].replayed is True
    assert logs[0].dependency_id == "dependency-1"
    assert logs[0].version_old == 7
    assert logs[0].version_new == 8
    assert logs[0].critical_section_duration_ms is not None


class _UowContext:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_: object) -> bool:
        return False


class _UowFactory:
    def __call__(self, **_: object) -> _UowContext:
        return _UowContext()


@pytest.mark.asyncio
async def test_mcp_remove_projects_receipt_satisfaction_not_creation_status() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    removed = replace(
        _record(),
        removed_at=datetime.now(timezone.utc),
        removal_reason="No longer required",
        source_version_on_remove=8,
    )
    receipt = SpecDependencyMutationReceipt(
        operation="remove",
        dependency=removed,
        source_spec=_snapshot("source", version=8),
        request_digest="digest",
        satisfied=False,
    )
    context = SimpleNamespace(
        agent_id="agent-1",
        agent_name="Agent",
        permissions=["*"],
    )
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=context),
        ),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=_UowFactory(),
        ),
        patch.object(
            RemoveSpecDependencyUseCase,
            "execute",
            AsyncMock(return_value=SimpleNamespace(receipt=receipt)),
        ),
    ):
        payload = json.loads(
            await mcp_server.okto_pulse_remove_spec_dependency.fn(
                board_id="board-1",
                spec_id="source",
                dependency_id=removed.id,
                reason="No longer required",
                expected_spec_version=7,
                expected_spec_edition=2,
                idempotency_key="remove-key",
            )
        )

    assert payload["success"] is True
    assert payload["dependency"]["target_status_on_create"] == "done"
    assert payload["dependency"]["satisfied"] is False


@pytest.mark.asyncio
async def test_mcp_archive_and_delete_preserve_typed_dependency_error_envelope() -> (
    None
):
    from okto_pulse.core.application.use_cases.boards_crud import ArchiveTreeUseCase
    from okto_pulse.core.application.use_cases.spec_crud import DeleteSpecUseCase
    from okto_pulse.core.mcp import server as mcp_server

    error = SpecDependencyOperationError(
        "spec_dependency_state_conflict",
        "Incoming dependency blocks deletion.",
        remediation="remove_incoming_dependencies",
        facts={"incoming_count": 1},
    )
    context = SimpleNamespace(
        agent_id="agent-1",
        agent_name="Agent",
        permissions=["*"],
    )
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=context),
        ),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=_UowFactory(),
        ),
        patch.object(
            ArchiveTreeUseCase,
            "execute",
            AsyncMock(side_effect=error),
        ),
    ):
        archive = json.loads(
            await mcp_server.okto_pulse_archive_tree.fn(
                board_id="board-1",
                entity_type="spec",
                entity_id="target",
            )
        )
    with (
        patch.object(
            mcp_server,
            "_get_agent_ctx",
            AsyncMock(return_value=context),
        ),
        patch.object(
            mcp_server,
            "get_unit_of_work_factory_for_mcp",
            return_value=_UowFactory(),
        ),
        patch.object(
            DeleteSpecUseCase,
            "execute",
            AsyncMock(side_effect=error),
        ),
    ):
        delete = json.loads(
            await mcp_server.okto_pulse_delete_spec.fn(
                board_id="board-1",
                spec_id="target",
            )
        )

    expected = {
        "error": "spec_dependency_state_conflict",
        "code": "spec_dependency_state_conflict",
        "message": "Another active Spec depends on this target.",
        "retryable": False,
        "remediation": "remove_incoming_dependencies",
        "facts": {"incoming_count": 1},
    }
    assert archive == expected
    assert delete == expected
