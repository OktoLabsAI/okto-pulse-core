"""B13 transport-free policy governance orchestration."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases import guideline_import_export
from okto_pulse.core.application.use_cases.guideline_import_export import (
    ExportGuidelinePolicyCommand,
    ExportGuidelinePolicyV3UseCase,
    ImportGuidelinePolicyCommand,
    ImportGuidelinePolicyUseCase,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    ASSESSMENTS_READ,
    ASSESSMENTS_RECORD,
    IMPACT_PREVIEW,
    METRICS_AUTHOR,
    POLICY_GOVERNANCE_CAPABILITIES,
    REVISIONS_CREATE,
    REVISIONS_READ,
    REVISIONS_RETIRE,
    AdoptGuidelineRevisionCommand,
    AdoptGuidelineRevisionUseCase,
    CreateGuidelineRevisionCommand,
    CreateGuidelineRevisionUseCase,
    GetGuidelineImpactCommand,
    GetGuidelineImpactUseCase,
    GetGuidelineRevisionCommand,
    GetGuidelineRevisionUseCase,
    ListGuidelineRevisionsCommand,
    ListGuidelineRevisionsUseCase,
    ListGuidelineImpactItemsCommand,
    ListGuidelineImpactItemsUseCase,
    PreviewGuidelineImpactCommand,
    PreviewGuidelineImpactUseCase,
    RecordSemanticGuidelineAssessmentUseCase,
    RetireGuidelineCommand,
    RetireGuidelineUseCase,
    GuidelineRevisionUnderBump,
    _require_capability,
    _write,
)
from okto_pulse.core.domain.guideline_compliance import PolicyProjection
from okto_pulse.core.domain.guideline_lifecycle import GuidelineRevisionPatch
from okto_pulse.core.domain.guideline_policy import (
    GUIDELINE_ID_MAX_LENGTH,
    GUIDELINE_RETIREMENT_ID_MAX_LENGTH,
    GUIDELINE_REVISION_ID_MAX_LENGTH,
    GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_IDEMPOTENCY_KEY_MAX_LENGTH,
    POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
    BoardGuidelineBinding,
    Guideline,
    GuidelineBindingState,
    GuidelineEnforcement,
    GuidelineHead,
    GuidelineLifecycleStatus,
    GuidelineRetirement,
    GuidelineRevision,
    GuidelineRevisionPage,
    GuidelineScope,
    GuidelineRevisionPageCursor,
    guideline_revision_digest_v2,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelineImpactListQuery,
    GuidelinePolicyIdempotencyConflict,
    GuidelinePolicyInvalidCursor,
    GuidelineRetirementReplay,
    GuidelineRevisionNoopReplay,
    GuidelineRevisionReplay,
    GuidelineRevisionListQuery,
)


NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
HISTORICAL_AUTHORITIES = (
    "guidelines.read",
    "guidelines.delete",
    "spec.entity.edit_fields",
    "spec.validation.submit",
)


class _UntouchedUow:
    """A denial must not even resolve a repository/service from the UoW."""

    def __init__(self) -> None:
        self.access_count = 0
        self.commit_count = 0
        self.rollback_count = 0

    @property
    def services(self):
        self.access_count += 1
        raise AssertionError("services must not be touched on permission denial")

    @property
    def boards(self):
        self.access_count += 1
        raise AssertionError("boards must not be touched on permission denial")

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1


def _actor_without(capability: str) -> ActorContext:
    return ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        permissions=tuple(
            item for item in POLICY_GOVERNANCE_CAPABILITIES if item != capability
        )
        + HISTORICAL_AUTHORITIES,
    )


class _CapabilityOnlyUseCase:
    def __init__(self, capability: str) -> None:
        self.capability = capability

    async def execute(self, _command, *, actor, uow) -> None:
        _require_capability(actor, self.capability)
        raise AssertionError("permission denial must happen first")


def _denial_case(capability: str):
    if capability == REVISIONS_READ:
        return (
            ListGuidelineRevisionsUseCase(),
            ListGuidelineRevisionsCommand("board-1", "guideline-1"),
        )
    if capability == REVISIONS_CREATE:
        return (
            CreateGuidelineRevisionUseCase(),
            CreateGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                GuidelineRevisionPatch(title="Next"),
                "revision-create",
            ),
        )
    if capability == METRICS_AUTHOR:
        return (
            CreateGuidelineRevisionUseCase(),
            CreateGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                GuidelineRevisionPatch(metrics=()),
                "blocking-change",
            ),
        )
    if capability == REVISIONS_RETIRE:
        return (
            RetireGuidelineUseCase(),
            RetireGuidelineCommand(
                "board-1",
                "guideline-1",
                "retirement-1",
                GuidelineLifecycleStatus.RETIRED,
                "No longer applicable",
                "retire-1",
            ),
        )
    if capability == IMPACT_PREVIEW:
        return (
            PreviewGuidelineImpactUseCase(),
            PreviewGuidelineImpactCommand(
                board_id="board-1",
                guideline_id="guideline-1",
                proposed_priority=10,
                proposed_enforcement=GuidelineEnforcement.ADVISORY,
                proposed_minimum_confidence=0,
                proposed_metric_threshold_overrides={},
                idempotency_key="preview-1",
            ),
        )
    if capability == ADOPTION_MANAGE:
        return (
            AdoptGuidelineRevisionUseCase(),
            AdoptGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                "impact-1",
                "a" * 64,
                "adopt-1",
            ),
        )
    if capability == ASSESSMENTS_READ:
        return _CapabilityOnlyUseCase(capability), object()
    if capability == ASSESSMENTS_RECORD:
        return RecordSemanticGuidelineAssessmentUseCase(), object()
    if capability.startswith("guidelines.waiver."):
        return _CapabilityOnlyUseCase(capability), object()
    raise AssertionError(capability)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", POLICY_GOVERNANCE_CAPABILITIES)
async def test_every_skb_capability_fails_before_uow_or_writer_access(
    capability: str,
) -> None:
    use_case, command = _denial_case(capability)
    uow = _UntouchedUow()

    with pytest.raises(PermissionDeniedError):
        await use_case.execute(
            command,
            actor=_actor_without(capability),
            uow=uow,
        )

    assert uow.access_count == 0
    assert uow.commit_count == 0
    assert uow.rollback_count == 0


def _bounded_mutation_commands():
    create = CreateGuidelineRevisionCommand(
        "board-1",
        "guideline-1",
        GuidelineRevisionPatch(title="Policy v2"),
        "create-key",
        next_revision_id="revision-2",
        declared_semantic_version="1.0.1",
    )
    retire = RetireGuidelineCommand(
        "board-1",
        "guideline-1",
        "retirement-1",
        GuidelineLifecycleStatus.RETIRED,
        "Withdrawn",
        "retire-key",
    )
    preview = PreviewGuidelineImpactCommand(
        board_id="board-1",
        guideline_id="guideline-1",
        proposed_priority=0,
        proposed_enforcement=GuidelineEnforcement.ADVISORY,
        proposed_minimum_confidence=0,
        proposed_metric_threshold_overrides={},
        idempotency_key="preview-key",
        to_revision_id="revision-1",
    )
    adopt = AdoptGuidelineRevisionCommand(
        "board-1",
        "guideline-1",
        "impact-1",
        "a" * 64,
        "adopt-key",
    )
    return (
        (create, "board_id", POLICY_BOARD_ID_MAX_LENGTH),
        (create, "guideline_id", GUIDELINE_ID_MAX_LENGTH),
        (create, "idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        (create, "next_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
        (
            create,
            "declared_semantic_version",
            GUIDELINE_SEMANTIC_VERSION_MAX_LENGTH,
        ),
        (retire, "board_id", POLICY_BOARD_ID_MAX_LENGTH),
        (retire, "guideline_id", GUIDELINE_ID_MAX_LENGTH),
        (retire, "retirement_id", GUIDELINE_RETIREMENT_ID_MAX_LENGTH),
        (retire, "idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        (retire, "superseded_by_guideline_id", GUIDELINE_ID_MAX_LENGTH),
        (preview, "board_id", POLICY_BOARD_ID_MAX_LENGTH),
        (preview, "guideline_id", GUIDELINE_ID_MAX_LENGTH),
        (preview, "idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
        (preview, "to_revision_id", GUIDELINE_REVISION_ID_MAX_LENGTH),
        (adopt, "board_id", POLICY_BOARD_ID_MAX_LENGTH),
        (adopt, "guideline_id", GUIDELINE_ID_MAX_LENGTH),
        (adopt, "impact_receipt_id", POLICY_IMPACT_RECEIPT_ID_MAX_LENGTH),
        (adopt, "idempotency_key", POLICY_IDEMPOTENCY_KEY_MAX_LENGTH),
    )


@pytest.mark.parametrize(
    ("command", "field_name", "maximum"),
    _bounded_mutation_commands(),
)
def test_mutation_command_durable_text_bounds_are_closed_before_uow(
    command,
    field_name: str,
    maximum: int,
) -> None:
    boundary = "x" * maximum
    assert getattr(replace(command, **{field_name: boundary}), field_name) == boundary
    with pytest.raises(ValueError):
        replace(command, **{field_name: "x" * (maximum + 1)})
    assert _UntouchedUow().access_count == 0


def test_sql_integer_boundaries_are_closed_before_uow() -> None:
    boundary_preview = PreviewGuidelineImpactCommand(
        board_id="board-1",
        guideline_id="guideline-1",
        proposed_priority=POLICY_SQL_INTEGER_MAX,
        proposed_enforcement=GuidelineEnforcement.ADVISORY,
        proposed_minimum_confidence=0,
        proposed_metric_threshold_overrides={},
        idempotency_key="preview-key",
    )
    assert boundary_preview.proposed_priority == POLICY_SQL_INTEGER_MAX
    with pytest.raises(ValueError, match="guideline_impact_priority_invalid"):
        replace(
            boundary_preview,
            proposed_priority=POLICY_SQL_INTEGER_MAX + 1,
        )

@pytest.mark.asyncio
async def test_actor_bound_is_rejected_before_uow_access() -> None:
    uow = _UntouchedUow()
    actor = ActorContext(
        "x" * 256,
        "mcp",
        permissions=(REVISIONS_READ,),
    )
    with pytest.raises(ValueError, match="policy_actor_id_invalid"):
        await ListGuidelineRevisionsUseCase().execute(
            ListGuidelineRevisionsCommand("board-1", "guideline-1"),
            actor=actor,
            uow=uow,
        )
    assert uow.access_count == 0


def test_flat_introduced_leaf_requires_its_historical_authority() -> None:
    leaf_only = ActorContext(
        "agent-1",
        "mcp",
        permissions=(REVISIONS_READ,),
    )
    with pytest.raises(PermissionDeniedError):
        _require_capability(leaf_only, REVISIONS_READ)

    bridged = ActorContext(
        "agent-1",
        "mcp",
        permissions=(REVISIONS_READ, "guidelines.read"),
    )
    _require_capability(bridged, REVISIONS_READ)


def test_revision_command_preserves_projection_and_rejects_cross_projection() -> None:
    summary = GuidelineRevisionListQuery(
        guideline_id="guideline-1",
        projection=PolicyProjection.SUMMARY,
    )
    cursor = GuidelineRevisionPageCursor(
        revision_number=3,
        item_id="revision-3",
        filter_digest=summary.filter_digest,
        projection_digest=summary.projection_digest,
    )

    command = ListGuidelineRevisionsCommand(
        "board-1",
        "guideline-1",
        projection=PolicyProjection.SUMMARY,
        cursor=cursor,
    )
    assert command.projection is PolicyProjection.SUMMARY
    with pytest.raises(
        GuidelinePolicyInvalidCursor,
        match="guideline_revision_cursor_context_mismatch",
    ):
        ListGuidelineRevisionsCommand(
            "board-1",
            "guideline-1",
            projection=PolicyProjection.DETAIL,
            cursor=cursor,
        )


@pytest.mark.asyncio
async def test_write_commits_once_and_rolls_back_late_failure() -> None:
    committed = SimpleNamespace(commit_count=0, rollback_count=0)

    async def commit() -> None:
        committed.commit_count += 1

    async def rollback() -> None:
        committed.rollback_count += 1

    committed.commit = commit
    committed.rollback = rollback

    async def success() -> str:
        return "ok"

    assert await _write(committed, success) == "ok"
    assert (committed.commit_count, committed.rollback_count) == (1, 0)

    failed = SimpleNamespace(commit_count=0, rollback_count=0)

    async def failing_commit() -> None:
        failed.commit_count += 1
        raise RuntimeError("late commit failure")

    async def failed_rollback() -> None:
        failed.rollback_count += 1

    failed.commit = failing_commit
    failed.rollback = failed_rollback

    with pytest.raises(RuntimeError, match="late commit failure"):
        await _write(failed, success)
    assert (failed.commit_count, failed.rollback_count) == (1, 1)


class _BoardRepo:
    async def get(self, board_id: str):
        return SimpleNamespace(
            id=board_id,
            owner_id="owner-2",
            realm_id="local",
        )


class _PolicyPort:
    def __init__(self, *, binding: BoardGuidelineBinding | None) -> None:
        digest = guideline_revision_digest_v2(
            semantic_version="1.0.0",
            title="Policy",
            content="Policy body",
            metrics=(),
        )
        self.identity = Guideline(
            guideline_id="guideline-1",
            owner_id="owner-2",
            scope=GuidelineScope.GLOBAL,
            board_id=None,
            created_at=NOW,
        )
        self.revision = GuidelineRevision(
            revision_id="revision-1",
            guideline_id="guideline-1",
            revision_number=1,
            semantic_version="1.0.0",
            title="Policy",
            content="Policy body",
            revision_digest=digest,
            metrics=(),
            created_by="owner-2",
            created_at=NOW,
        )
        self.head = GuidelineHead(
            guideline_id="guideline-1",
            revision_id="revision-1",
            revision_number=1,
            semantic_version="1.0.0",
            head_revision=1,
            updated_at=NOW,
        )
        self.binding = binding
        self.retirement = None

    async def get_guideline(self, **_kwargs):
        return self.identity

    async def get_binding(self, **_kwargs):
        return self.binding

    async def get_retirement(self, **_kwargs):
        return self.retirement

    async def get_head(self, **_kwargs):
        return self.head

    async def get_revision(self, **_kwargs):
        return self.revision

    async def list_revisions(self, _query):
        return GuidelineRevisionPage(
            items=(self.revision,),
            limit=50,
            next_cursor=None,
            has_more=False,
        )

    async def get_revision_result_by_idempotency(self, **_kwargs):
        return None

    async def get_retirement_result_by_idempotency(self, **_kwargs):
        return None


class _GovernanceUow:
    def __init__(self, port: _PolicyPort) -> None:
        self.boards = _BoardRepo()
        self.services = SimpleNamespace(
            guidelines=SimpleNamespace(policy_persistence=lambda: port)
        )
        self.commit_count = 0
        self.rollback_count = 0

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1


def _active_binding() -> BoardGuidelineBinding:
    digest = guideline_revision_digest_v2(
        semantic_version="1.0.0",
        title="Policy",
        content="Policy body",
        metrics=(),
    )
    return BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id="guideline-1",
        revision_id="revision-1",
        semantic_version="1.0.0",
        revision_digest=digest,
        priority=0,
        binding_revision=1,
        adopted_by="owner-2",
        adopted_at=NOW,
    )


@pytest.mark.asyncio
async def test_non_owner_global_revision_read_requires_exact_active_binding() -> None:
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        permissions=(REVISIONS_READ, "guidelines.read"),
    )
    bound = _GovernanceUow(_PolicyPort(binding=_active_binding()))

    summary = await ListGuidelineRevisionsUseCase().execute(
        ListGuidelineRevisionsCommand("board-1", "guideline-1"),
        actor=actor,
        uow=bound,
    )
    assert summary.projection is PolicyProjection.SUMMARY
    assert summary.page.items[0].content is None
    assert summary.page.items[0].metrics is None
    assert summary.page.items[0].tags is None

    detail = await ListGuidelineRevisionsUseCase().execute(
        ListGuidelineRevisionsCommand(
            "board-1",
            "guideline-1",
            projection=PolicyProjection.DETAIL,
        ),
        actor=actor,
        uow=bound,
    )
    assert detail.page.items[0].content == "Policy body"
    assert detail.page.items[0].metrics == ()
    assert detail.page.items[0].tags == ()

    with pytest.raises(EntityNotFoundError):
        await ListGuidelineRevisionsUseCase().execute(
            ListGuidelineRevisionsCommand("board-1", "guideline-1"),
            actor=actor,
            uow=_GovernanceUow(_PolicyPort(binding=None)),
        )


@pytest.mark.asyncio
async def test_global_owner_reads_revision_history_without_board_binding() -> None:
    actor = ActorContext(
        "owner-2",
        "mcp",
        board_id="board-1",
        permissions=(REVISIONS_READ, "guidelines.read"),
    )
    port = _PolicyPort(binding=None)
    uow = _GovernanceUow(port)

    listed = await ListGuidelineRevisionsUseCase().execute(
        ListGuidelineRevisionsCommand(
            "board-1",
            "guideline-1",
            projection=PolicyProjection.DETAIL,
        ),
        actor=actor,
        uow=uow,
    )
    fetched = await GetGuidelineRevisionUseCase().execute(
        GetGuidelineRevisionCommand(
            "board-1",
            "guideline-1",
            "revision-1",
        ),
        actor=actor,
        uow=uow,
    )

    assert listed.page.items[0].revision_id == port.revision.revision_id
    assert listed.page.items[0].content == port.revision.content
    assert fetched.guideline.owner_id == actor.actor_id
    assert fetched.revision == port.revision


@pytest.mark.asyncio
async def test_global_owner_cannot_use_a_different_board_authority_path() -> None:
    actor = ActorContext(
        "owner-2",
        "mcp",
        board_id="board-1",
        permissions=(REVISIONS_READ, "guidelines.read"),
    )

    with pytest.raises(EntityNotFoundError) as captured:
        await ListGuidelineRevisionsUseCase().execute(
            ListGuidelineRevisionsCommand("board-2", "guideline-1"),
            actor=actor,
            uow=_GovernanceUow(_PolicyPort(binding=None)),
        )

    assert captured.value.entity_type == "board"


@pytest.mark.asyncio
async def test_retired_global_history_remains_visible_on_exact_historical_binding() -> None:
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        permissions=(REVISIONS_READ, "guidelines.read"),
    )
    port = _PolicyPort(binding=_active_binding())
    port.binding = replace(
        port.binding,
        state=GuidelineBindingState.UNLINKED,
        binding_revision=2,
    )
    port.retirement = GuidelineRetirement(
        retirement_id="retirement-1",
        guideline_id="guideline-1",
        status=GuidelineLifecycleStatus.RETIRED,
        retired_revision_id=port.revision.revision_id,
        retired_revision_number=port.revision.revision_number,
        retired_semantic_version=port.revision.semantic_version,
        retired_revision_digest=port.revision.revision_digest,
        retired_head_revision=port.head.head_revision,
        reason="Historical policy",
        retired_by="owner-2",
        retired_at=NOW + timedelta(seconds=1),
    )
    uow = _GovernanceUow(port)

    listed = await ListGuidelineRevisionsUseCase().execute(
        ListGuidelineRevisionsCommand("board-1", "guideline-1"),
        actor=actor,
        uow=uow,
    )
    fetched = await GetGuidelineRevisionUseCase().execute(
        GetGuidelineRevisionCommand(
            "board-1",
            "guideline-1",
            "revision-1",
        ),
        actor=actor,
        uow=uow,
    )

    assert listed.page.items[0].revision_id == "revision-1"
    assert fetched.retirement == port.retirement

    port.binding = None
    with pytest.raises(EntityNotFoundError):
        await GetGuidelineRevisionUseCase().execute(
            GetGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                "revision-1",
            ),
            actor=actor,
            uow=uow,
        )


@pytest.mark.asyncio
async def test_non_owner_cannot_mutate_linked_global_revision() -> None:
    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        permissions=(REVISIONS_CREATE, "spec.entity.edit_fields"),
    )
    uow = _GovernanceUow(_PolicyPort(binding=_active_binding()))

    with pytest.raises(EntityNotFoundError):
        await CreateGuidelineRevisionUseCase(clock=lambda: NOW).execute(
            CreateGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                GuidelineRevisionPatch(title="Changed"),
                "revision-change-1",
            ),
            actor=actor,
            uow=uow,
        )
    assert uow.commit_count == 0


class _ReplayPolicyPort(_PolicyPort):
    def __init__(self) -> None:
        super().__init__(binding=_active_binding())
        self.revisions = {self.revision.revision_id: self.revision}
        self.revision_replays: dict[
            str,
            GuidelineRevisionReplay | GuidelineRevisionNoopReplay,
        ] = {}
        self.retirement_replays: dict[str, GuidelineRetirementReplay] = {}
        self.append_count = 0
        self.noop_count = 0
        self.retire_count = 0

    async def get_revision(self, *, revision_id: str, **_kwargs):
        return self.revisions.get(revision_id)

    async def get_revision_result_by_idempotency(
        self,
        *,
        idempotency_key: str,
        **_kwargs,
    ):
        return self.revision_replays.get(idempotency_key)

    async def get_retirement_result_by_idempotency(
        self,
        *,
        idempotency_key: str,
        **_kwargs,
    ):
        return self.retirement_replays.get(idempotency_key)

    async def append_revision_cas(
        self,
        *,
        revision: GuidelineRevision,
        next_head: GuidelineHead,
        idempotency_key: str,
        request_digest: str,
        **_kwargs,
    ):
        self.append_count += 1
        self.revisions[revision.revision_id] = revision
        self.revision = revision
        self.head = next_head
        self.revision_replays[idempotency_key] = GuidelineRevisionReplay(
            revision=revision,
            published_head=next_head,
            request_digest=request_digest,
        )
        return revision, next_head

    async def record_revision_noop_cas(
        self,
        *,
        replay: GuidelineRevisionNoopReplay,
        idempotency_key: str,
    ):
        assert replay.original_head == self.head
        self.noop_count += 1
        self.revision_replays[idempotency_key] = replay
        return replay

    async def retire_guideline_cas(
        self,
        *,
        retirement,
        idempotency_key: str,
        request_digest: str,
        **_kwargs,
    ):
        self.retire_count += 1
        self.retirement = retirement
        self.retirement_replays[idempotency_key] = GuidelineRetirementReplay(
            retirement=retirement,
            request_digest=request_digest,
        )
        self.binding = replace(
            self.binding,
            state=GuidelineBindingState.UNLINKED,
            binding_revision=self.binding.binding_revision + 1,
        )
        return retirement


class _UnboundGlobalPolicyPort(_ReplayPolicyPort):
    def __init__(self) -> None:
        super().__init__()
        self.binding = None

    async def retire_guideline_cas(
        self,
        *,
        retirement,
        idempotency_key: str,
        request_digest: str,
        **_kwargs,
    ):
        self.retire_count += 1
        self.retirement = retirement
        self.retirement_replays[idempotency_key] = GuidelineRetirementReplay(
            retirement=retirement,
            request_digest=request_digest,
        )
        return retirement


def _owner_actor(*capabilities: str) -> ActorContext:
    return ActorContext(
        "owner-2",
        "mcp",
        board_id="board-1",
        permissions=capabilities + HISTORICAL_AUTHORITIES,
    )


@pytest.mark.asyncio
async def test_global_owner_creates_revision_without_board_binding() -> None:
    port = _UnboundGlobalPolicyPort()
    uow = _GovernanceUow(port)

    result = await CreateGuidelineRevisionUseCase().execute(
        CreateGuidelineRevisionCommand(
            "board-1",
            "guideline-1",
            GuidelineRevisionPatch(title="Policy v2"),
            "unbound-owner-revision-key",
            occurred_at=NOW + timedelta(seconds=1),
        ),
        actor=_owner_actor(REVISIONS_CREATE),
        uow=uow,
    )

    assert result.status == "applied"
    assert result.revision is not None
    assert result.revision.revision_number == 2
    assert port.append_count == 1
    assert uow.commit_count == 1


@pytest.mark.asyncio
async def test_global_owner_retires_guideline_without_board_binding() -> None:
    port = _UnboundGlobalPolicyPort()
    uow = _GovernanceUow(port)

    result = await RetireGuidelineUseCase().execute(
        RetireGuidelineCommand(
            "board-1",
            "guideline-1",
            "unbound-retirement-1",
            GuidelineLifecycleStatus.RETIRED,
            "Global policy withdrawn",
            "unbound-owner-retirement-key",
            occurred_at=NOW + timedelta(seconds=1),
        ),
        actor=_owner_actor(REVISIONS_RETIRE),
        uow=uow,
    )

    assert result.retirement == port.retirement
    assert result.retirement.retired_by == "owner-2"
    assert port.retire_count == 1
    assert uow.commit_count == 1


@pytest.mark.asyncio
async def test_non_owner_cannot_mutate_unbound_global_guideline() -> None:
    actor = ActorContext(
        "owner-other",
        "mcp",
        board_id="board-1",
        permissions=(
            REVISIONS_CREATE,
            REVISIONS_RETIRE,
            "spec.entity.edit_fields",
            "guidelines.delete",
        ),
    )
    port = _UnboundGlobalPolicyPort()
    uow = _GovernanceUow(port)

    with pytest.raises(EntityNotFoundError):
        await CreateGuidelineRevisionUseCase().execute(
            CreateGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                GuidelineRevisionPatch(title="Forbidden"),
                "unbound-cross-owner-revision-key",
            ),
            actor=actor,
            uow=uow,
        )
    with pytest.raises(EntityNotFoundError):
        await RetireGuidelineUseCase().execute(
            RetireGuidelineCommand(
                "board-1",
                "guideline-1",
                "forbidden-retirement",
                GuidelineLifecycleStatus.RETIRED,
                "Forbidden",
                "unbound-cross-owner-retirement-key",
            ),
            actor=actor,
            uow=uow,
        )

    assert port.append_count == 0
    assert port.retire_count == 0
    assert uow.commit_count == 0


@pytest.mark.asyncio
async def test_revision_replay_uses_original_head_and_rejects_payload_drift() -> None:
    port = _ReplayPolicyPort()
    uow = _GovernanceUow(port)
    actor = _owner_actor(REVISIONS_CREATE)
    use_case = CreateGuidelineRevisionUseCase()
    original_command = CreateGuidelineRevisionCommand(
        "board-1",
        "guideline-1",
        GuidelineRevisionPatch(title="Policy v2"),
        "revision-key-1",
        occurred_at=NOW + timedelta(seconds=1),
    )

    original = await use_case.execute(original_command, actor=actor, uow=uow)
    await use_case.execute(
        CreateGuidelineRevisionCommand(
            "board-1",
            "guideline-1",
            GuidelineRevisionPatch(content="Policy body v3"),
            "revision-key-2",
            occurred_at=NOW + timedelta(seconds=2),
        ),
        actor=actor,
        uow=uow,
    )
    replay = await use_case.execute(original_command, actor=actor, uow=uow)

    assert replay == original
    assert replay.status == "applied"
    assert port.head.revision_number == 3
    assert port.append_count == 2
    assert uow.commit_count == 2

    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_revision_idempotency_payload_mismatch",
    ):
        await use_case.execute(
            replace(
                original_command,
                next_revision_id="different-revision-id",
            ),
            actor=actor,
            uow=uow,
        )
    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_revision_idempotency_payload_mismatch",
    ):
        await use_case.execute(
            replace(
                original_command,
                patch=GuidelineRevisionPatch(title="Different intent"),
            ),
            actor=actor,
            uow=uow,
        )
    assert port.append_count == 2
    assert uow.commit_count == 2


@pytest.mark.asyncio
async def test_revision_applied_key_cannot_be_reused_for_noop_payload() -> None:
    port = _ReplayPolicyPort()
    uow = _GovernanceUow(port)
    actor = _owner_actor(REVISIONS_CREATE)
    use_case = CreateGuidelineRevisionUseCase()
    command = CreateGuidelineRevisionCommand(
        "board-1",
        "guideline-1",
        GuidelineRevisionPatch(title="Policy v2"),
        "applied-then-noop-key",
        occurred_at=NOW + timedelta(seconds=1),
    )
    await use_case.execute(command, actor=actor, uow=uow)

    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_revision_idempotency_payload_mismatch",
    ):
        await use_case.execute(
            replace(
                command,
                patch=GuidelineRevisionPatch(title="Policy"),
            ),
            actor=actor,
            uow=uow,
        )

    assert port.append_count == 1
    assert port.noop_count == 0
    assert port.head.revision_number == 2
    assert uow.commit_count == 1


@pytest.mark.asyncio
async def test_revision_noop_replay_is_durable_after_head_advance() -> None:
    port = _ReplayPolicyPort()
    uow = _GovernanceUow(port)
    actor = _owner_actor(REVISIONS_CREATE)
    use_case = CreateGuidelineRevisionUseCase()
    noop_command = CreateGuidelineRevisionCommand(
        "board-1",
        "guideline-1",
        GuidelineRevisionPatch(title="  Policy  "),
        "revision-noop-key-1",
        occurred_at=NOW + timedelta(seconds=1),
    )

    original = await use_case.execute(noop_command, actor=actor, uow=uow)
    assert original.status == "noop"
    assert original.revision is None
    assert original.head is None
    assert port.noop_count == 1

    await use_case.execute(
        CreateGuidelineRevisionCommand(
            "board-1",
            "guideline-1",
            GuidelineRevisionPatch(title="Policy v2"),
            "revision-after-noop-key",
            occurred_at=NOW + timedelta(seconds=2),
        ),
        actor=actor,
        uow=uow,
    )
    replay = await use_case.execute(noop_command, actor=actor, uow=uow)

    assert replay == original
    assert port.head.revision_number == 2
    assert port.append_count == 1
    assert port.noop_count == 1
    assert uow.commit_count == 2

    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_revision_idempotency_payload_mismatch",
    ):
        await use_case.execute(
            replace(
                noop_command,
                patch=GuidelineRevisionPatch(title="Different intent"),
            ),
            actor=actor,
            uow=uow,
        )
    assert port.append_count == 1
    assert port.noop_count == 1
    assert uow.commit_count == 2


@pytest.mark.asyncio
async def test_retirement_replay_survives_terminal_unlink_and_rejects_drift() -> None:
    port = _ReplayPolicyPort()
    uow = _GovernanceUow(port)
    actor = _owner_actor(REVISIONS_RETIRE)
    use_case = RetireGuidelineUseCase()
    command = RetireGuidelineCommand(
        "board-1",
        "guideline-1",
        "retirement-1",
        GuidelineLifecycleStatus.RETIRED,
        "Policy withdrawn",
        "retirement-key-1",
        occurred_at=NOW + timedelta(seconds=1),
    )

    original = await use_case.execute(command, actor=actor, uow=uow)
    assert port.binding.state is GuidelineBindingState.UNLINKED
    replay = await use_case.execute(command, actor=actor, uow=uow)

    assert replay == original
    assert port.retire_count == 1
    assert uow.commit_count == 1

    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="guideline_retirement_idempotency_payload_mismatch",
    ):
        await use_case.execute(
            replace(command, reason="Different retirement intent"),
            actor=actor,
            uow=uow,
        )
    assert port.retire_count == 1
    assert uow.commit_count == 1


@pytest.mark.asyncio
async def test_declared_semver_under_bump_is_typed_and_never_written() -> None:
    port = _ReplayPolicyPort()
    uow = _GovernanceUow(port)

    with pytest.raises(GuidelineRevisionUnderBump) as captured:
        await CreateGuidelineRevisionUseCase().execute(
            CreateGuidelineRevisionCommand(
                "board-1",
                "guideline-1",
                GuidelineRevisionPatch(title="Changed"),
                "under-bump-key",
                declared_semantic_version="1.0.0",
                occurred_at=NOW + timedelta(seconds=1),
            ),
            actor=_owner_actor(REVISIONS_CREATE),
            uow=uow,
        )

    assert captured.value.code == "guideline_semver_below_minimum"
    assert captured.value.minimum_semantic_version == "1.0.1"
    assert port.append_count == 0
    assert uow.commit_count == 0
    assert uow.rollback_count == 0


class _ImpactScopePort:
    def __init__(self) -> None:
        self.list_count = 0

    async def get_impact_receipt(self, **_kwargs):
        return SimpleNamespace(guideline_id="guideline-x")

    async def list_impact_items(self, _query):
        self.list_count += 1
        raise AssertionError("cross-guideline items must not be listed")


@pytest.mark.asyncio
async def test_impact_receipt_is_bound_to_guideline_path_and_cursor_digest() -> None:
    x_query = GuidelineImpactListQuery(
        board_id="board-1",
        guideline_id="guideline-x",
        impact_receipt_id="impact-1",
    )
    y_query = GuidelineImpactListQuery(
        board_id="board-1",
        guideline_id="guideline-y",
        impact_receipt_id="impact-1",
    )
    assert x_query.filter_digest != y_query.filter_digest

    port = _ImpactScopePort()
    uow = _GovernanceUow(port)
    actor = _owner_actor(IMPACT_PREVIEW)
    with pytest.raises(EntityNotFoundError):
        await GetGuidelineImpactUseCase().execute(
            GetGuidelineImpactCommand(
                "board-1",
                "guideline-y",
                "impact-1",
            ),
            actor=actor,
            uow=uow,
        )
    with pytest.raises(EntityNotFoundError):
        await ListGuidelineImpactItemsUseCase().execute(
            ListGuidelineImpactItemsCommand("guideline-y", y_query),
            actor=actor,
            uow=uow,
        )
    assert port.list_count == 0


@pytest.mark.asyncio
async def test_import_export_capabilities_fail_before_uow_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uow = _UntouchedUow()
    with pytest.raises(PermissionDeniedError):
        await ExportGuidelinePolicyV3UseCase().execute(
            ExportGuidelinePolicyCommand(),
            actor=ActorContext("agent-1", "rest", permissions=()),
            uow=uow,
        )
    assert uow.access_count == 0

    monkeypatch.setattr(
        guideline_import_export,
        "parse_guideline_export",
        lambda _payload: SimpleNamespace(
            guidelines=(
                SimpleNamespace(
                    identity=SimpleNamespace(
                        scope=GuidelineScope.GLOBAL,
                        board_id=None,
                    ),
                    bindings=(),
                        revisions=(
                            SimpleNamespace(
                                revision=SimpleNamespace(
                                    metrics=(SimpleNamespace(),)
                                ),
                            ),
                    ),
                ),
            ),
        ),
    )
    with pytest.raises(PermissionDeniedError):
        await ImportGuidelinePolicyUseCase().execute(
            ImportGuidelinePolicyCommand(envelope={}),
            actor=ActorContext(
                "agent-1",
                "rest",
                permissions=(REVISIONS_CREATE, "spec.entity.edit_fields"),
            ),
            uow=uow,
        )
    assert uow.access_count == 0


@pytest.mark.asyncio
async def test_context_only_import_does_not_require_metric_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        guideline_import_export,
        "parse_guideline_export",
        lambda _payload: SimpleNamespace(
            guidelines=(
                SimpleNamespace(
                    identity=SimpleNamespace(
                        scope=GuidelineScope.GLOBAL,
                        board_id=None,
                    ),
                    bindings=(),
                        revisions=(
                            SimpleNamespace(
                                revision=SimpleNamespace(metrics=()),
                            ),
                    ),
                ),
            ),
        ),
    )
    uow = _UntouchedUow()
    with pytest.raises(
        AssertionError,
        match="services must not be touched",
    ):
        await ImportGuidelinePolicyUseCase().execute(
            ImportGuidelinePolicyCommand(envelope={}),
            actor=ActorContext(
                "agent-1",
                "rest",
                permissions=(REVISIONS_CREATE, "spec.entity.edit_fields"),
            ),
            uow=uow,
        )
    assert uow.access_count == 1
