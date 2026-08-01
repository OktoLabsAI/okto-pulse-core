"""SK-B B12 application tests for shared guideline import/export use cases."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.guideline_import_export import (
    ExportGuidelinePolicyCommand,
    ExportGuidelinePolicyV3UseCase,
    ImportGuidelinePolicyCommand,
    ImportGuidelinePolicyUseCase,
)
from okto_pulse.core.domain.guideline_import_export import (
    GuidelineBindingMaterialization,
    GuidelineExportAggregate,
    GuidelineExportBinding,
    GuidelineExportRevision,
    GuidelineExportSnapshot,
    GuidelineImportTransactionStatus,
    build_guideline_export_v3,
    canonical_guideline_sha256,
    guideline_export_payload,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    Guideline,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelineRevision,
    GuidelineScope,
    PolicyEntityType,
)


NOW = datetime(2026, 7, 29, 20, tzinfo=timezone.utc)
ACTOR = ActorContext(
    "actor-1",
    "rest",
    realm_id="local",
    permissions=(
        "guidelines.revisions.read",
        "guidelines.revisions.create",
        "guidelines.metrics.author",
        "guidelines.read",
        "spec.entity.edit_fields",
    ),
)
REVISION_ONLY_ACTOR = ActorContext(
    "revision-only-actor",
    "rest",
    realm_id="local",
    permissions=(
        "guidelines.revisions.create",
        "spec.entity.edit_fields",
    ),
)
MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "application"
    / "use_cases"
    / "guideline_import_export.py"
)


def _aggregate(
    *,
    guideline_id: str = "guideline-1",
    owner_id: str = "source-owner",
    board_id: str | None = None,
    content: str = "The policy body.",
    with_binding: bool = False,
) -> GuidelineExportAggregate:
    scope = GuidelineScope.INLINE if board_id is not None else GuidelineScope.GLOBAL
    revision_id = f"{guideline_id}-revision-1"
    metric = GuidelineMetric(
        metric_id="metric-1",
        code="policy.clarity",
        title="Policy clarity",
        description="Assess clarity of the policy.",
        evaluation_rubric="Score clarity from 0 to 100.",
        target_entity_types=(PolicyEntityType.SPEC,),
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=80,
    )
    revision = GuidelineRevision(
        revision_id=revision_id,
        guideline_id=guideline_id,
        revision_number=1,
        semantic_version="1.0.0",
        title="Policy",
        content=content,
        metrics=(metric,),
        created_by="source-actor",
        created_at=NOW,
    )
    bindings = (
        (
            GuidelineExportBinding(
                binding=BoardGuidelineBinding(
                    binding_id=f"{guideline_id}-binding",
                    board_id=board_id or "board-1",
                    guideline_id=guideline_id,
                    revision_id=revision_id,
                    semantic_version="1.0.0",
                    revision_digest=revision.revision_digest,
                    priority=0,
                    binding_revision=1,
                    adopted_by="source-actor",
                    adopted_at=NOW,
                    enforcement=GuidelineEnforcement.ADVISORY,
                    minimum_confidence=70,
                    metric_threshold_overrides={"policy.clarity": 85},
                ),
                physical_source_kind="guideline_policy_v1",
                binding_origin="native",
                materialization=GuidelineBindingMaterialization.LIVE,
            ),
        )
        if with_binding
        else ()
    )
    return GuidelineExportAggregate(
        identity=Guideline(
            guideline_id=guideline_id,
            owner_id=owner_id,
            scope=scope,
            board_id=board_id,
            created_at=NOW,
        ),
        revisions=(GuidelineExportRevision(revision=revision),),
        head=GuidelineHead(
            guideline_id=guideline_id,
            revision_id=revision_id,
            revision_number=1,
            semantic_version="1.0.0",
            head_revision=1,
            updated_at=NOW,
        ),
        bindings=bindings,
    )


def _payload(*aggregates: GuidelineExportAggregate) -> dict[str, object]:
    envelope = build_guideline_export_v3(
        GuidelineExportSnapshot(aggregates=aggregates),
        exported_at=NOW,
    )
    return guideline_export_payload(envelope)


class _Boards:
    def __init__(self, boards: dict[str, object]) -> None:
        self._boards = boards
        self.calls: list[str] = []

    async def get(self, board_id: str):
        self.calls.append(board_id)
        return self._boards.get(board_id)


class _Shares:
    def __init__(self, permissions: dict[tuple[str, str], str] | None = None) -> None:
        self._permissions = permissions or {}

    async def get_share_permission(self, board_id: str, actor_id: str):
        return self._permissions.get((board_id, actor_id))


class _Port:
    def __init__(
        self,
        *,
        snapshot: GuidelineExportSnapshot,
        apply_error: Exception | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.apply_error = apply_error
        self.export_calls: list[dict[str, object]] = []
        self.import_snapshot_calls: list[dict[str, object]] = []
        self.apply_calls: list[dict[str, object]] = []

    async def export_guideline_snapshot(self, **kwargs):
        self.export_calls.append(kwargs)
        return self.snapshot

    async def load_guideline_import_snapshot(self, **kwargs):
        self.import_snapshot_calls.append(kwargs)
        return self.snapshot

    async def apply_guideline_import_plan(self, plan, **kwargs) -> None:
        self.apply_calls.append({"plan": plan, **kwargs})
        if self.apply_error is not None:
            raise self.apply_error


class _Guidelines:
    def __init__(self, port: _Port) -> None:
        self.port = port
        self.policy_persistence_calls = 0

    def policy_persistence(self) -> _Port:
        self.policy_persistence_calls += 1
        return self.port


class _Uow:
    def __init__(
        self,
        port: _Port,
        *,
        boards: dict[str, object] | None = None,
        permissions: dict[tuple[str, str], str] | None = None,
        commit_error: Exception | None = None,
    ) -> None:
        self.boards = _Boards(boards or {})
        self.guidelines = _Guidelines(port)
        self.services = SimpleNamespace(
            guidelines=self.guidelines,
            shares=_Shares(permissions),
        )
        self.commit_error = commit_error
        self.commit_count = 0
        self.rollback_count = 0

    async def commit(self) -> None:
        self.commit_count += 1
        if self.commit_error is not None:
            raise self.commit_error

    async def rollback(self) -> None:
        self.rollback_count += 1


def _owned_board(board_id: str = "board-1") -> object:
    return SimpleNamespace(
        id=board_id,
        owner_id=ACTOR.actor_id,
        realm_id="local",
    )


@pytest.mark.asyncio
async def test_export_authorizes_board_before_obtaining_policy_adapter() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(
        port,
        boards={
            "board-1": SimpleNamespace(
                id="board-1",
                owner_id="other-owner",
                realm_id="local",
            )
        },
    )

    with pytest.raises(EntityNotFoundError):
        await ExportGuidelinePolicyV3UseCase(clock=lambda: NOW).execute(
            ExportGuidelinePolicyCommand(board_id="board-1"),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.guidelines.policy_persistence_calls == 0
    assert port.export_calls == []
    assert uow.commit_count == 0


@pytest.mark.asyncio
async def test_export_scopes_snapshot_by_actor_and_board_without_commit() -> None:
    aggregate = _aggregate(board_id="board-1")
    port = _Port(
        snapshot=GuidelineExportSnapshot(
            aggregates=(aggregate,),
            source_board_id="board-1",
        )
    )
    uow = _Uow(port, boards={"board-1": _owned_board()})

    output = await ExportGuidelinePolicyV3UseCase(clock=lambda: NOW).execute(
        ExportGuidelinePolicyCommand(
            board_id="board-1",
            guideline_ids=("guideline-1",),
        ),
        actor=ACTOR,
        uow=uow,
    )

    assert output.envelope.guidelines == (aggregate,)
    assert output.envelope.exported_at == NOW
    assert output.envelope.schema_version == "3"
    exported_revision = output.envelope.guidelines[0].revisions[0].revision
    assert exported_revision.metrics[0].metric_id == "metric-1"
    assert port.export_calls == [
        {
            "owner_id": ACTOR.actor_id,
            "board_id": "board-1",
            "guideline_ids": ("guideline-1",),
            "include_binding_history": True,
        }
    ]
    assert uow.commit_count == 0
    assert uow.rollback_count == 0


@pytest.mark.asyncio
async def test_metric_import_requires_author_capability_before_adapter_access() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    with pytest.raises(PermissionDeniedError, match="guidelines.metrics.author"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=_payload(_aggregate())),
            actor=REVISION_ONLY_ACTOR,
            uow=uow,
        )

    assert uow.boards.calls == []
    assert uow.guidelines.policy_persistence_calls == 0
    assert port.import_snapshot_calls == []
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_default_export_selects_actor_catalog_instead_of_empty_selection() -> (
    None
):
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    await ExportGuidelinePolicyV3UseCase(clock=lambda: NOW).execute(
        ExportGuidelinePolicyCommand(),
        actor=ACTOR,
        uow=uow,
    )

    assert port.export_calls == [
        {
            "owner_id": ACTOR.actor_id,
            "board_id": None,
            "guideline_ids": None,
            "include_binding_history": True,
        }
    ]


@pytest.mark.asyncio
async def test_import_authorizes_every_target_board_before_adapter_access() -> None:
    payload = _payload(
        _aggregate(
            guideline_id="guideline-1",
            board_id="board-1",
            with_binding=True,
        ),
        _aggregate(
            guideline_id="guideline-2",
            board_id="board-2",
            with_binding=True,
        ),
    )
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(
        port,
        boards={
            "board-1": _owned_board("board-1"),
            "board-2": SimpleNamespace(
                id="board-2",
                owner_id="other-owner",
                realm_id="local",
            ),
        },
    )

    with pytest.raises(EntityNotFoundError):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=payload),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.boards.calls == ["board-1", "board-2"]
    assert uow.guidelines.policy_persistence_calls == 0
    assert port.export_calls == []
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_import_validates_complete_envelope_before_board_or_adapter_access() -> (
    None
):
    payload = _payload(
        _aggregate(guideline_id="guideline-1"),
        _aggregate(guideline_id="guideline-2"),
    )
    guidelines = payload["guidelines"]
    assert isinstance(guidelines, list)
    second = guidelines[1]
    assert isinstance(second, dict)
    revisions = second["revisions"]
    assert isinstance(revisions, list)
    revisions[0]["content"] = "tampered after digest"
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    with pytest.raises(ValueError, match="digest"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=payload),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.boards.calls == []
    assert uow.guidelines.policy_persistence_calls == 0
    assert port.export_calls == []
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_import_rejects_durable_boundary_plus_one_before_any_adapter() -> None:
    payload = _payload(_aggregate())
    guidelines = payload["guidelines"]
    assert isinstance(guidelines, list)
    aggregate = guidelines[0]
    assert isinstance(aggregate, dict)
    identity = aggregate["identity"]
    assert isinstance(identity, dict)
    identity["guideline_id"] = "x" * 37
    payload["content_digest"] = canonical_guideline_sha256(
        {
            "contract_version": payload["contract_version"],
            "schema_version": payload["schema_version"],
            "kind": payload["kind"],
            "source_board_id": payload["source_board_id"],
            "guidelines": guidelines,
        }
    )
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    with pytest.raises(ValueError, match="guideline_id_required"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=payload),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.boards.calls == []
    assert uow.guidelines.policy_persistence_calls == 0
    assert port.import_snapshot_calls == []
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_import_rejects_sql_integer_overflow_before_any_adapter() -> None:
    payload = _payload(_aggregate())
    guidelines = payload["guidelines"]
    assert isinstance(guidelines, list)
    aggregate = guidelines[0]
    assert isinstance(aggregate, dict)
    revisions = aggregate["revisions"]
    assert isinstance(revisions, list)
    revision = revisions[0]
    assert isinstance(revision, dict)
    revision["revision_number"] = 2_147_483_648
    payload["content_digest"] = canonical_guideline_sha256(
        {
            "contract_version": payload["contract_version"],
            "schema_version": payload["schema_version"],
            "kind": payload["kind"],
            "source_board_id": payload["source_board_id"],
            "guidelines": guidelines,
        }
    )
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    with pytest.raises(ValueError, match="revision_number_invalid"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=payload),
            actor=ACTOR,
            uow=uow,
        )

    assert uow.boards.calls == []
    assert uow.guidelines.policy_persistence_calls == 0
    assert port.import_snapshot_calls == []
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_dry_run_remaps_owner_and_has_no_write_or_transaction_finalize() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(
            envelope=_payload(_aggregate()),
            dry_run=True,
        ),
        actor=ACTOR,
        uow=uow,
    )

    assert output.result.transaction_status is GuidelineImportTransactionStatus.DRY_RUN
    assert output.result.created_count == 0
    assert output.plan.entries[0].aggregate.identity.owner_id == ACTOR.actor_id
    assert output.plan.live_binding_writes == ()
    assert port.apply_calls == []
    assert uow.commit_count == 0
    assert uow.rollback_count == 0


@pytest.mark.asyncio
async def test_target_remap_stores_source_binding_as_inert_candidate_only() -> None:
    imported = _aggregate(
        board_id="source-board",
        with_binding=True,
    )
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(
        port,
        boards={"target-board": _owned_board("target-board")},
    )

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(
            envelope=_payload(imported),
            target_board_id="target-board",
            dry_run=True,
        ),
        actor=ACTOR,
        uow=uow,
    )

    entry = output.plan.entries[0]
    assert entry.aggregate.identity.owner_id == ACTOR.actor_id
    assert entry.aggregate.identity.board_id == "target-board"
    assert entry.aggregate.bindings[0].materialization.value == "candidate"
    assert entry.binding_candidates[0].source_board_id == "source-board"
    assert entry.binding_candidates[0].target_board_id == "target-board"
    assert output.plan.live_binding_writes == ()
    assert port.import_snapshot_calls == [{"guideline_ids": ("guideline-1",)}]
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_conflicting_semver_rolls_back_without_apply_or_overwrite() -> None:
    imported = _aggregate(content="new body")
    existing = _aggregate(
        owner_id=ACTOR.actor_id,
        content="different existing body",
    )
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=(existing,)))
    uow = _Uow(port)

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(envelope=_payload(imported)),
        actor=ACTOR,
        uow=uow,
    )

    assert (
        output.result.transaction_status is GuidelineImportTransactionStatus.ROLLED_BACK
    )
    assert output.result.error_code == "conflict"
    assert output.result.conflict_count == 1
    assert output.result.overwritten_row_count == 0
    assert port.apply_calls == []
    assert uow.commit_count == 0
    assert uow.rollback_count == 1


@pytest.mark.asyncio
async def test_valid_import_applies_once_and_reports_commit_only_after_commit() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(envelope=_payload(_aggregate())),
        actor=ACTOR,
        uow=uow,
    )

    assert (
        output.result.transaction_status is GuidelineImportTransactionStatus.COMMITTED
    )
    assert output.result.created_count == 1
    assert output.result.overwritten_row_count == 0
    assert len(port.apply_calls) == 1
    assert port.apply_calls[0]["imported_by"] == ACTOR.actor_id
    assert port.apply_calls[0]["imported_at"] == NOW
    assert port.apply_calls[0]["import_digest"] == output.plan.import_digest
    assert output.plan.entries[0].aggregate.identity.owner_id == ACTOR.actor_id
    assert uow.commit_count == 1
    assert uow.rollback_count == 0


@pytest.mark.asyncio
async def test_exact_round_trip_is_skip_identical_with_zero_overwrite() -> None:
    aggregate = _aggregate(owner_id=ACTOR.actor_id)
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=(aggregate,)))
    uow = _Uow(port)

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(envelope=_payload(aggregate)),
        actor=ACTOR,
        uow=uow,
    )

    assert (
        output.result.transaction_status is GuidelineImportTransactionStatus.COMMITTED
    )
    assert output.result.created_count == 0
    assert output.result.skip_identical_count == 1
    assert output.result.overwritten_row_count == 0
    assert len(port.apply_calls) == 1
    assert uow.commit_count == 1


@pytest.mark.asyncio
async def test_adapter_failure_rolls_back_and_never_returns_committed() -> None:
    failure = RuntimeError("adapter failed")
    port = _Port(
        snapshot=GuidelineExportSnapshot(aggregates=()),
        apply_error=failure,
    )
    uow = _Uow(port)

    with pytest.raises(RuntimeError, match="adapter failed"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=_payload(_aggregate())),
            actor=ACTOR,
            uow=uow,
        )

    assert len(port.apply_calls) == 1
    assert uow.commit_count == 0
    assert uow.rollback_count == 1


@pytest.mark.asyncio
async def test_commit_failure_rolls_back_and_cannot_report_committed() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port, commit_error=RuntimeError("commit failed"))

    with pytest.raises(RuntimeError, match="commit failed"):
        await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
            ImportGuidelinePolicyCommand(envelope=_payload(_aggregate())),
            actor=ACTOR,
            uow=uow,
        )

    assert len(port.apply_calls) == 1
    assert uow.commit_count == 1
    assert uow.rollback_count == 1


@pytest.mark.asyncio
async def test_legacy_v1_dispatch_is_contextual_unadopted_and_dry_run() -> None:
    port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
    uow = _Uow(port)
    payload = {
        "schema_version": "1",
        "kind": "guidelines",
        "exported_at": "2026-07-29T20:00:00Z",
        "items": [
            {
                "title": "Legacy policy",
                "content": "Legacy body",
                "tags": ["legacy"],
                "scope": "global",
                "legacy_version": "17",
                "blocking": True,
                "rules": [
                    {
                        "enforcement": "blocking",
                        "legacy_expression": "coverage < 100",
                    }
                ],
            }
        ],
    }

    output = await ImportGuidelinePolicyUseCase(clock=lambda: NOW).execute(
        ImportGuidelinePolicyCommand(envelope=payload, dry_run=True),
        actor=REVISION_ONLY_ACTOR,
        uow=uow,
    )

    aggregate = output.plan.entries[0].aggregate
    assert aggregate.identity.owner_id == REVISION_ONLY_ACTOR.actor_id
    assert aggregate.revisions[0].semantic_version == "1.0.0"
    assert aggregate.revisions[0].legacy_version == "17"
    assert aggregate.revisions[0].revision.metrics == ()
    assert aggregate.bindings == ()
    assert output.plan.live_binding_writes == ()
    assert "legacy_blocking_downgraded_to_advisory" in aggregate.migration_notes
    assert "legacy_rules_dropped_contextual_baseline" in aggregate.migration_notes
    assert port.apply_calls == []


@pytest.mark.asyncio
async def test_legacy_v1_without_timestamp_replays_with_deterministic_baseline() -> (
    None
):
    payload = {
        "schema_version": "1",
        "kind": "guidelines",
        "items": [
            {
                "title": "Legacy replay",
                "content": "Stable legacy body",
                "scope": "global",
            }
        ],
    }
    outputs = []
    for clock_value in (
        NOW,
        datetime(2027, 1, 1, tzinfo=timezone.utc),
    ):
        port = _Port(snapshot=GuidelineExportSnapshot(aggregates=()))
        output = await ImportGuidelinePolicyUseCase(
            clock=lambda value=clock_value: value
        ).execute(
            ImportGuidelinePolicyCommand(envelope=payload, dry_run=True),
            actor=ACTOR,
            uow=_Uow(port),
        )
        outputs.append(output)

    first, replay = outputs
    assert first.plan.entries[0].aggregate == replay.plan.entries[0].aggregate
    assert first.plan.import_digest == replay.plan.import_digest
    assert first.plan.entries[0].aggregate.identity.created_at == datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )


def test_application_module_is_transport_and_persistence_implementation_free() -> None:
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)

    forbidden = {
        "fastapi",
        "sqlalchemy",
        "okto_pulse.community",
        "okto_pulse.core.api",
        "okto_pulse.core.mcp",
    }
    assert not {
        module
        for module in modules
        if any(module == root or module.startswith(f"{root}.") for root in forbidden)
    }
