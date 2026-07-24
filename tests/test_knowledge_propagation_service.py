"""Behavioral contract for selective Knowledge Base propagation orchestration."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib

import pytest

from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeAssignment,
    KnowledgeAssignmentState,
    KnowledgeOriginClass,
    KnowledgeRelevanceLink,
    KnowledgeSelection,
    KnowledgeSelectionState,
)
from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeLegacyAttachment,
    KnowledgeLocalAttachment,
    KnowledgeMutationKind,
    KnowledgeMutationLedgerEntry,
    KnowledgeMutationOutcome,
    KnowledgeMutationPlan,
    KnowledgeMutationReceipt,
    KnowledgeParentEvidence,
    KnowledgeParentKey,
    KnowledgePropagationScope,
    KnowledgePropagationPortError,
    KnowledgePropagationSnapshot,
    KnowledgePropagationTombstone,
    KnowledgeRecordKind,
    KnowledgeSelectableSource,
    KnowledgeTargetKey,
    KnowledgeTemporalWindow,
    TemporalKnowledgeAssignment,
)
from okto_pulse.core.services.knowledge_propagation import (
    KnowledgeCreationPreflightCommand,
    KnowledgeGrandfatherAttachment,
    KnowledgeGrandfatherCommand,
    KnowledgeGrandfatherEvidence,
    KnowledgeMutationCommand,
    KnowledgeMutationPreparation,
    KnowledgeMutationResultV2Projector,
    KnowledgePropagationService,
    KnowledgePropagationServiceError,
    KnowledgeRelinkResetCommand,
    KnowledgeRefreshCommand,
    KnowledgeRefreshByKnowledgeIdsCommand,
    classify_legacy_origin,
    deterministic_knowledge_target_id,
)
from okto_pulse.core.services.knowledge_governance_projection import (
    project_knowledge_governance_from_resource,
)


NOW = datetime(2026, 7, 23, 13, 0, tzinfo=timezone.utc)


def _target(
    *,
    board_id: str = "board-1",
    target_id: str = "card-1",
) -> KnowledgeTargetKey:
    return KnowledgeTargetKey(board_id, "card", target_id)


def _stamp(
    root_id: str,
    *,
    revision: str = "1",
    content: bytes = b"content",
) -> ResourceRevisionStamp:
    return ResourceRevisionStamp(
        root_id=root_id,
        immediate_parent_id=f"{root_id}-parent",
        source_revision=revision,
        source_content_sha256=hashlib.sha256(content).hexdigest(),
    )


def _source(
    knowledge_id: str,
    root_id: str,
    *,
    revision: str = "1",
    content: bytes | None = b"content",
    source_deleted: bool = False,
    governance_metadata: object | None = None,
) -> KnowledgeSelectableSource:
    digest_content = b"content" if content is None else content
    return KnowledgeSelectableSource(
        requested_knowledge_id=knowledge_id,
        source_knowledge_id=knowledge_id,
        revision_stamp=_stamp(
            root_id,
            revision=revision,
            content=digest_content,
        ),
        content_bytes=content,
        source_deleted=source_deleted,
        governance_metadata=governance_metadata,
    )


def _window(
    *,
    current: bool = True,
    superseded_by: str = "next-record",
) -> KnowledgeTemporalWindow:
    return KnowledgeTemporalWindow(
        effective_from=NOW - timedelta(minutes=1),
        effective_to=None if current else NOW - timedelta(seconds=1),
        superseded_by_id=None if current else superseded_by,
    )


def _assignment(
    assignment_id: str,
    source_id: str,
    root_id: str,
    *,
    mode: str = "reference",
    state: str = "active",
    revision: int = 1,
    stamp_revision: str = "1",
    stamp_content: bytes = b"content",
    current: bool = True,
    justification: str = "acceptance evidence",
    relevance_links: tuple[KnowledgeRelevanceLink, ...] = (),
) -> TemporalKnowledgeAssignment:
    return TemporalKnowledgeAssignment(
        assignment=KnowledgeAssignment(
            assignment_id=assignment_id,
            board_id="board-1",
            target_type="card",
            target_id="card-1",
            source_knowledge_id=source_id,
            revision_stamp=_stamp(
                root_id,
                revision=stamp_revision,
                content=stamp_content,
            ),
            mode=mode,
            state=state,
            origin_class="v2",
            actor_id="agent-1",
            revision=revision,
            justification=justification,
            relevance_links=relevance_links,
        ),
        temporal=_window(current=current),
    )


def _snapshot(
    snapshot_id: str,
    assignment_id: str,
    root_id: str,
    *,
    revision: str = "1",
    content: bytes = b"content",
    current: bool = True,
    governance_metadata: object | None = None,
) -> KnowledgePropagationSnapshot:
    return KnowledgePropagationSnapshot(
        snapshot_id=snapshot_id,
        assignment_id=assignment_id,
        revision_stamp=_stamp(root_id, revision=revision, content=content),
        content_bytes=content,
        temporal=_window(current=current),
        governance_metadata=governance_metadata,
    )


def _tombstone(
    tombstone_id: str,
    root_id: str | None,
) -> KnowledgePropagationTombstone:
    return KnowledgePropagationTombstone(
        tombstone_id=tombstone_id,
        target=_target(),
        root_id=root_id,
        actor_id="agent-1",
        justification="explicit suppression",
        temporal=_window(),
    )


def _scope(
    *,
    target: KnowledgeTargetKey | None = None,
    revision: int = 0,
    v2_active: bool = False,
    state: KnowledgeSelectionState | str | None = None,
    assignments: tuple[TemporalKnowledgeAssignment, ...] = (),
    tombstones: tuple[KnowledgePropagationTombstone, ...] = (),
    snapshots: tuple[KnowledgePropagationSnapshot, ...] = (),
    legacy: tuple[KnowledgeLegacyAttachment, ...] = (),
    sources: tuple[KnowledgeSelectableSource, ...] = (),
    local: tuple[KnowledgeLocalAttachment, ...] = (),
    v2_activated_at: datetime | None = None,
) -> KnowledgePropagationScope:
    return KnowledgePropagationScope(
        target=target or _target(),
        scope_revision=revision,
        v2_active=v2_active,
        selection_state=state,
        assignments=assignments,
        tombstones=tombstones,
        snapshots=snapshots,
        legacy_attachments=legacy,
        sources=sources,
        local_attachments=local,
        v2_activated_at=v2_activated_at,
    )


class _FakePort:
    def __init__(
        self,
        scope: KnowledgePropagationScope,
        *,
        replay_entry: KnowledgeMutationLedgerEntry | None = None,
        divergent_receipt: bool = False,
        parent_evidence: KnowledgeParentEvidence | None = None,
    ) -> None:
        self.scope = scope
        self.replay_entry = replay_entry
        self.divergent_receipt = divergent_receipt
        self.parent_evidence = parent_evidence
        self.idempotency_lookups = []
        self.scope_lookups = []
        self.parent_lookups = []
        self.staged: list[KnowledgeMutationPlan] = []
        self.attempts = []

    async def get_idempotency_entry(self, context, request):
        self.idempotency_lookups.append((context, request))
        return self.replay_entry

    async def load_scope(self, context, request):
        self.scope_lookups.append((context, request))
        return self.scope

    async def load_parent_evidence(self, context, request):
        self.parent_lookups.append((context, request))
        assert self.parent_evidence is not None
        return self.parent_evidence

    async def stage_mutation(self, context, plan):
        self.staged.append(plan)
        receipt = plan.ledger_entry.receipt
        if self.divergent_receipt:
            return replace(receipt, operation_id="adapter-divergence")
        return receipt

    async def stage_attempt(self, context, attempt):
        self.attempts.append((context, attempt))


def _service(port: _FakePort) -> KnowledgePropagationService:
    counters: Counter[str] = Counter()

    def _next_id(prefix: str) -> str:
        counters[prefix] += 1
        return f"{prefix}-{counters[prefix]}"

    return KnowledgePropagationService(
        port,
        now=lambda: NOW,
        id_factory=_next_id,
    )


@pytest.mark.asyncio
async def test_read_normalizes_persistence_port_error() -> None:
    class _FailingReadPort(_FakePort):
        async def load_scope(self, context, request):
            raise KnowledgePropagationPortError(
                "knowledge_read_unavailable",
                "read backend unavailable",
                details={"retryable": True},
            )

    service = _service(_FailingReadPort(_scope()))

    with pytest.raises(KnowledgePropagationServiceError) as caught:
        await service.read(object(), _target())

    assert caught.value.code == "knowledge_read_unavailable"
    assert caught.value.detail == "read backend unavailable"
    assert caught.value.details == {"retryable": True}


def _command(
    selection: KnowledgeSelection,
    *,
    expected_revision: int = 0,
    idempotency_key: str = "idem-1",
    justification: str | None = "acceptance evidence",
) -> KnowledgeMutationCommand:
    return KnowledgeMutationCommand(
        target=_target(),
        selection=selection,
        actor_id="agent-1",
        expected_revision=expected_revision,
        idempotency_key=idempotency_key,
        justification=justification,
    )


def _parent_evidence(
    parent: KnowledgeParentKey,
    *,
    sources: tuple[KnowledgeSelectableSource, ...] = (),
    linked_spec_id: str | None = None,
    functional_requirement_ids: tuple[str, ...] = (),
    acceptance_criterion_ids: tuple[str, ...] = (),
    test_scenario_ids: tuple[str, ...] = (),
) -> KnowledgeParentEvidence:
    return KnowledgeParentEvidence(
        parent=parent,
        parent_exists=True,
        same_board=True,
        parent_state="done",
        sources=sources,
        linked_spec_id=linked_spec_id,
        functional_requirement_ids=functional_requirement_ids,
        acceptance_criterion_ids=acceptance_criterion_ids,
        test_scenario_ids=test_scenario_ids,
    )


def _grandfather_attachment(
    source_id: str,
    *,
    evidence: KnowledgeGrandfatherEvidence | None = None,
    storage_kind: str = "card_json",
) -> KnowledgeGrandfatherAttachment:
    table = "cards" if storage_kind == "card_json" else "spec_knowledge_bases"
    return KnowledgeGrandfatherAttachment(
        source_knowledge_id=source_id,
        revision_stamp=_stamp(f"{source_id}-root"),
        evidence=evidence or KnowledgeGrandfatherEvidence(),
        physical_locator={
            "storage_kind": storage_kind,
            "table": table,
            "owner_id": "card-1",
            "attachment_id": source_id,
        },
    )


def _ledger_for_replay(
    command: KnowledgeMutationCommand,
    request_hash: str,
) -> KnowledgeMutationLedgerEntry:
    receipt = KnowledgeMutationReceipt(
        operation_id="original-operation",
        target=command.target,
        operation_kind=KnowledgeMutationKind.REPLACE,
        previous_revision=command.expected_revision,
        revision=command.expected_revision + 1,
        request_hash=request_hash,
        applied_at=NOW,
    )
    return KnowledgeMutationLedgerEntry(
        target=command.target,
        idempotency_key=command.idempotency_key,
        request_hash=request_hash,
        operation_kind=KnowledgeMutationKind.REPLACE,
        receipt=receipt,
        recorded_at=NOW,
        actor_id=command.actor_id,
    )


@pytest.mark.asyncio
async def test_identical_replay_precedes_revision_check_and_divergence_conflicts() -> (
    None
):
    command = _command(
        KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        expected_revision=0,
    )
    probe = _service(_FakePort(_scope()))
    request_hash = probe._mutation_request_hash(command)

    identical_port = _FakePort(
        _scope(revision=99),
        replay_entry=_ledger_for_replay(command, request_hash),
    )
    replay = await _service(identical_port).mutate(object(), command)

    assert replay.replayed is True
    assert replay.operation_id == "original-operation"
    assert identical_port.scope_lookups == []
    assert identical_port.staged == []
    assert len(identical_port.attempts) == 1
    replay_attempt = identical_port.attempts[0][1]
    assert replay_attempt.outcome is KnowledgeMutationOutcome.REPLAYED
    assert replay_attempt.original_operation_id == "original-operation"

    divergent_hash = "f" * 64
    divergent_port = _FakePort(
        _scope(revision=99),
        replay_entry=_ledger_for_replay(command, divergent_hash),
    )
    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(divergent_port).mutate(object(), command)

    assert raised.value.code == "knowledge_propagation_idempotency_conflict"
    assert raised.value.details["original_request_hash"] == divergent_hash
    assert raised.value.ledger_attempt is not None
    assert raised.value.ledger_attempt.outcome is KnowledgeMutationOutcome.REJECTED
    assert divergent_port.scope_lookups == []
    assert divergent_port.staged == []

    wrong_key_entry = replace(
        _ledger_for_replay(command, request_hash),
        idempotency_key="different-key",
    )
    wrong_key_port = _FakePort(
        _scope(revision=99),
        replay_entry=wrong_key_entry,
    )
    with pytest.raises(KnowledgePropagationServiceError) as wrong_key:
        await _service(wrong_key_port).mutate(object(), command)

    assert wrong_key.value.code == "knowledge_propagation_idempotency_conflict"
    assert wrong_key.value.ledger_attempt is not None
    assert wrong_key.value.ledger_attempt.outcome is KnowledgeMutationOutcome.REJECTED
    assert wrong_key_port.scope_lookups == []
    assert wrong_key_port.staged == []


@pytest.mark.asyncio
async def test_rejected_canonical_result_replays_the_same_error_for_audit() -> None:
    command = _command(
        KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        expected_revision=0,
    )
    probe = _service(_FakePort(_scope()))
    request_hash = probe._mutation_request_hash(command)
    rejected_receipt = KnowledgeMutationReceipt(
        operation_id="rejected-operation",
        target=command.target,
        operation_kind=KnowledgeMutationKind.REPLACE,
        previous_revision=0,
        revision=0,
        request_hash=request_hash,
        applied_at=NOW,
        outcome=KnowledgeMutationOutcome.REJECTED,
        reason_code="knowledge_selection_invalid",
        reason_detail="source could not be resolved",
        details={"missing": ["kb-1"]},
    )
    entry = KnowledgeMutationLedgerEntry(
        target=command.target,
        idempotency_key=command.idempotency_key,
        request_hash=request_hash,
        operation_kind=KnowledgeMutationKind.REPLACE,
        receipt=rejected_receipt,
        recorded_at=NOW,
        actor_id=command.actor_id,
    )
    port = _FakePort(_scope(revision=99), replay_entry=entry)

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).mutate(object(), command)

    assert raised.value.code == "knowledge_selection_invalid"
    assert raised.value.details == {"missing": ["kb-1"]}
    assert raised.value.ledger_attempt is not None
    assert raised.value.ledger_attempt.outcome is KnowledgeMutationOutcome.REPLAYED
    assert raised.value.ledger_attempt.original_operation_id == "rejected-operation"
    assert port.scope_lookups == []
    assert port.staged == []
    assert port.attempts == []


@pytest.mark.asyncio
async def test_revision_rejection_carries_a_post_rollback_ledger_attempt() -> None:
    port = _FakePort(
        _scope(
            revision=3,
            sources=(_source("kb-1", "root-1"),),
        )
    )
    command = _command(
        KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        expected_revision=2,
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).mutate(object(), command)

    assert raised.value.code == "knowledge_propagation_revision_conflict"
    assert raised.value.ledger_attempt is not None
    assert raised.value.ledger_attempt.outcome is KnowledgeMutationOutcome.REJECTED
    assert raised.value.ledger_attempt.reason_code == raised.value.code
    assert port.staged == []


@pytest.mark.asyncio
async def test_partial_or_foreign_selection_reports_requested_matched_missing() -> None:
    port = _FakePort(_scope(sources=(_source("kb-local", "root-local"),)))
    command = _command(
        KnowledgeSelection.explicit_ids(
            ["kb-local", "kb-foreign"],
            mode="reference",
        )
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).mutate(object(), command)

    assert raised.value.code == "knowledge_selection_invalid"
    assert raised.value.details == {
        "requested": ["kb-foreign", "kb-local"],
        "matched": ["kb-local"],
        "missing": ["kb-foreign"],
        "invalid": [],
        "ambiguous": [],
    }
    assert port.staged == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["reference", "snapshot"])
async def test_reference_and_snapshot_each_stage_one_complete_plan(mode: str) -> None:
    content = b"snapshot-source"
    source = _source("kb-1", "root-1", content=content)
    port = _FakePort(_scope(sources=(source,)))
    command = _command(KnowledgeSelection.explicit_ids(["kb-1"], mode=mode))

    receipt = await _service(port).mutate(object(), command)

    assert len(port.staged) == 1
    plan = port.staged[0]
    assert plan.operation_kind is KnowledgeMutationKind.REPLACE
    assert len(plan.assignments_to_open) == 1
    assert plan.assignments_to_open[0].assignment.mode.value == mode
    assert receipt == plan.ledger_entry.receipt
    if mode == "reference":
        assert plan.snapshots_to_open == ()
    else:
        assert len(plan.snapshots_to_open) == 1
        snapshot = plan.snapshots_to_open[0]
        assert snapshot.content_bytes == content
        assert snapshot.revision_stamp == source.revision_stamp
        assert snapshot.assignment_id == (
            plan.assignments_to_open[0].assignment.assignment_id
        )


@pytest.mark.asyncio
async def test_omitted_is_persisted_distinct_and_explicit_empty_suppresses_globally() -> (
    None
):
    current = _assignment("assignment-old", "kb-old", "root-old")
    omitted_port = _FakePort(
        _scope(
            revision=1,
            v2_active=True,
            state="explicit_ids",
            assignments=(current,),
        )
    )
    omitted = _command(
        KnowledgeSelection.omitted(),
        expected_revision=1,
        justification=None,
    )

    await _service(omitted_port).mutate(object(), omitted)
    omitted_plan = omitted_port.staged[0]
    assert omitted_plan.operation_kind is KnowledgeMutationKind.REPLACE_OMITTED
    assert omitted_plan.next_scope_selection_state is KnowledgeSelectionState.OMITTED
    assert omitted_plan.assignment_ids_to_close == ("assignment-old",)
    assert omitted_plan.tombstones_to_open == ()

    empty_port = _FakePort(
        _scope(
            revision=1,
            v2_active=True,
            state="explicit_ids",
            assignments=(current,),
        )
    )
    explicit_empty = _command(
        KnowledgeSelection.explicit_empty(),
        expected_revision=1,
    )

    await _service(empty_port).mutate(object(), explicit_empty)
    empty_plan = empty_port.staged[0]
    assert empty_plan.operation_kind is KnowledgeMutationKind.REPLACE_EMPTY
    assert empty_plan.next_scope_selection_state is (
        KnowledgeSelectionState.EXPLICIT_EMPTY
    )
    assert empty_plan.assignment_ids_to_close == ("assignment-old",)
    assert len(empty_plan.tombstones_to_open) == 1
    assert empty_plan.tombstones_to_open[0].root_id is None


@pytest.mark.asyncio
async def test_repeated_explicit_empty_supersedes_the_current_global_tombstone() -> (
    None
):
    current_global = _tombstone("tombstone-old", None)
    port = _FakePort(
        _scope(
            revision=2,
            v2_active=True,
            state="explicit_empty",
            tombstones=(current_global,),
        )
    )

    await _service(port).mutate(
        object(),
        _command(
            KnowledgeSelection.explicit_empty(),
            expected_revision=2,
            idempotency_key="empty-repeat",
        ),
    )

    plan = port.staged[0]
    assert plan.tombstone_ids_to_close == ("tombstone-old",)
    assert len(plan.tombstones_to_open) == 1
    replacement = plan.tombstones_to_open[0]
    assert replacement.root_id is None
    assert {
        (link.previous_id, link.successor_id)
        for link in plan.supersession_links
        if link.record_kind is KnowledgeRecordKind.TOMBSTONE
    } == {("tombstone-old", replacement.tombstone_id)}


@pytest.mark.asyncio
async def test_drop_delta_closes_only_selected_root_and_preserves_other_assignment() -> (
    None
):
    root_a = _assignment("assignment-a", "kb-a", "root-a")
    root_b = _assignment("assignment-b", "kb-b", "root-b")
    port = _FakePort(
        _scope(
            revision=2,
            v2_active=True,
            state="explicit_ids",
            assignments=(root_a, root_b),
            sources=(_source("kb-a", "root-a"),),
        )
    )
    command = _command(
        KnowledgeSelection.explicit_ids(["kb-a"], mode="drop"),
        expected_revision=2,
    )

    await _service(port).mutate(object(), command)
    plan = port.staged[0]

    assert plan.operation_kind is KnowledgeMutationKind.DROP_DELTA
    assert plan.assignment_ids_to_close == ("assignment-a",)
    assert "assignment-b" not in plan.assignment_ids_to_close
    assert len(plan.assignments_to_open) == 1
    assert plan.assignments_to_open[0].assignment.state is (
        KnowledgeAssignmentState.DROPPED
    )
    assert plan.tombstones_to_open[0].root_id == "root-a"


@pytest.mark.asyncio
async def test_drop_delta_closes_a_prior_global_tombstone_before_root_drop() -> None:
    port = _FakePort(
        _scope(
            revision=2,
            v2_active=True,
            state="explicit_empty",
            tombstones=(_tombstone("global-drop", None),),
            sources=(_source("kb-a", "root-a"),),
        )
    )

    await _service(port).mutate(
        object(),
        _command(
            KnowledgeSelection.explicit_ids(["kb-a"], mode="drop"),
            expected_revision=2,
            idempotency_key="drop-after-empty",
        ),
    )

    plan = port.staged[0]
    assert plan.tombstone_ids_to_close == ("global-drop",)
    assert tuple(item.root_id for item in plan.tombstones_to_open) == ("root-a",)


@pytest.mark.asyncio
async def test_invalid_source_or_hash_fails_before_any_stage() -> None:
    deleted_port = _FakePort(
        _scope(sources=(_source("kb-1", "root-1", source_deleted=True),))
    )
    with pytest.raises(KnowledgePropagationServiceError) as deleted:
        await _service(deleted_port).mutate(
            object(),
            _command(KnowledgeSelection.explicit_ids(["kb-1"], mode="reference")),
        )
    assert deleted.value.details["invalid"] == ["kb-1"]
    assert deleted_port.staged == []

    no_bytes_port = _FakePort(
        _scope(sources=(_source("kb-1", "root-1", content=None),))
    )
    with pytest.raises(KnowledgePropagationServiceError) as no_bytes:
        await _service(no_bytes_port).mutate(
            object(),
            _command(KnowledgeSelection.explicit_ids(["kb-1"], mode="snapshot")),
        )
    assert no_bytes.value.details["invalid"] == ["kb-1"]
    assert no_bytes_port.staged == []

    corrupt_source = _source("kb-1", "root-1", content=b"authentic")
    object.__setattr__(corrupt_source, "content_bytes", b"tampered-adapter-bytes")
    corrupt_port = _FakePort(_scope(sources=(corrupt_source,)))
    with pytest.raises(KnowledgePropagationServiceError) as corrupt:
        await _service(corrupt_port).mutate(
            object(),
            _command(KnowledgeSelection.explicit_ids(["kb-1"], mode="snapshot")),
        )
    assert corrupt.value.code == "knowledge_selection_invalid"
    assert corrupt.value.details["invalid"] == ["kb-1"]
    assert corrupt_port.staged == []


@pytest.mark.asyncio
async def test_refresh_replaces_only_current_snapshot_and_closes_old_records() -> None:
    old_content = b"old snapshot"
    new_content = b"new source"
    old_metadata = {"purpose": "frozen old governance"}
    new_metadata = {"purpose": "current source governance"}
    old_assignment = _assignment(
        "assignment-old",
        "kb-1",
        "root-1",
        mode="snapshot",
        stamp_revision="1",
        stamp_content=old_content,
    )
    old_snapshot = _snapshot(
        "snapshot-old",
        "assignment-old",
        "root-1",
        revision="1",
        content=old_content,
        governance_metadata=old_metadata,
    )
    source = _source(
        "kb-1",
        "root-1",
        revision="2",
        content=new_content,
        governance_metadata=new_metadata,
    )
    port = _FakePort(
        _scope(
            revision=4,
            v2_active=True,
            state="explicit_ids",
            assignments=(old_assignment,),
            snapshots=(old_snapshot,),
            sources=(source,),
        )
    )
    command = KnowledgeRefreshCommand(
        target=_target(),
        assignment_ids=("assignment-old",),
        actor_id="agent-2",
        justification="refresh verified source",
        expected_revision=4,
        idempotency_key="refresh-1",
    )

    await _service(port).refresh(object(), command)
    plan = port.staged[0]

    assert plan.operation_kind is KnowledgeMutationKind.REFRESH_SNAPSHOT
    assert plan.assignment_ids_to_close == ("assignment-old",)
    assert plan.snapshot_ids_to_close == ("snapshot-old",)
    assert len(plan.assignments_to_open) == 1
    assert len(plan.snapshots_to_open) == 1
    new_assignment = plan.assignments_to_open[0].assignment
    new_snapshot = plan.snapshots_to_open[0]
    assert new_assignment.assignment_id != "assignment-old"
    assert new_assignment.revision_stamp == source.revision_stamp
    assert new_snapshot.assignment_id == new_assignment.assignment_id
    assert new_snapshot.content_bytes == new_content
    assert new_snapshot.governance_metadata == new_metadata
    assert "governance_metadata" not in new_snapshot.to_dict()
    assert {
        (
            link.record_kind,
            link.previous_id,
            link.successor_id,
        )
        for link in plan.supersession_links
    } == {
        (
            KnowledgeRecordKind.ASSIGNMENT,
            "assignment-old",
            new_assignment.assignment_id,
        ),
        (
            KnowledgeRecordKind.SNAPSHOT,
            "snapshot-old",
            new_snapshot.snapshot_id,
        ),
    }
    assert len(port.staged) == 1

    source_without_metadata = _source(
        "kb-1",
        "root-1",
        revision="2",
        content=new_content,
    )
    assert source_without_metadata.revision_stamp == source.revision_stamp
    assert source_without_metadata == source
    without_metadata_port = _FakePort(
        _scope(
            revision=4,
            v2_active=True,
            state="explicit_ids",
            assignments=(old_assignment,),
            snapshots=(old_snapshot,),
            sources=(source_without_metadata,),
        )
    )
    await _service(without_metadata_port).refresh(object(), command)
    without_metadata_plan = without_metadata_port.staged[0]

    assert without_metadata_plan.request_hash == plan.request_hash
    assert without_metadata_plan.snapshots_to_open[0].governance_metadata is None
    assert isinstance(source.governance_metadata, dict)
    source.governance_metadata["purpose"] = "changed after refresh"
    assert new_snapshot.governance_metadata == {"purpose": "current source governance"}


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_shape", ["reference", "closed"])
async def test_refresh_rejects_non_snapshot_or_non_current_assignment(
    invalid_shape: str,
) -> None:
    assignment = _assignment(
        "assignment-1",
        "kb-1",
        "root-1",
        mode="reference" if invalid_shape == "reference" else "snapshot",
        current=invalid_shape != "closed",
    )
    port = _FakePort(
        _scope(
            revision=1,
            v2_active=True,
            state="explicit_ids",
            assignments=(assignment,),
            sources=(_source("kb-1", "root-1"),),
        )
    )
    command = KnowledgeRefreshCommand(
        target=_target(),
        assignment_ids=("assignment-1",),
        actor_id="agent-1",
        justification="refresh",
        expected_revision=1,
        idempotency_key="refresh-invalid",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).refresh(object(), command)

    assert raised.value.code == "knowledge_assignment_not_refreshable"
    if invalid_shape == "reference":
        assert raised.value.details["invalid"] == ["assignment-1"]
    else:
        assert raised.value.details["missing"] == ["assignment-1"]
    assert port.staged == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_state", "include_snapshot", "tombstone_root"),
    [
        ("omitted", True, "no-tombstone"),
        ("explicit_empty", True, "no-tombstone"),
        ("explicit_ids", False, "no-tombstone"),
        ("explicit_ids", True, "root-1"),
        ("explicit_ids", True, None),
    ],
)
async def test_refresh_rejects_non_selected_missing_or_tombstoned_snapshot(
    scope_state: str,
    include_snapshot: bool,
    tombstone_root: str | None,
) -> None:
    assignment = _assignment(
        "assignment-1",
        "kb-1",
        "root-1",
        mode="snapshot",
    )
    snapshots = (
        (_snapshot("snapshot-1", "assignment-1", "root-1"),) if include_snapshot else ()
    )
    tombstones = (
        ()
        if tombstone_root == "no-tombstone"
        else (_tombstone("tombstone-1", tombstone_root),)
    )
    port = _FakePort(
        _scope(
            revision=1,
            v2_active=True,
            state=scope_state,
            assignments=(assignment,),
            tombstones=tombstones,
            snapshots=snapshots,
            sources=(_source("kb-1", "root-1"),),
        )
    )
    command = KnowledgeRefreshCommand(
        target=_target(),
        assignment_ids=("assignment-1",),
        actor_id="agent-1",
        justification="refresh",
        expected_revision=1,
        idempotency_key=f"refresh-{scope_state}-{tombstone_root}",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).refresh(object(), command)

    assert raised.value.code == "knowledge_assignment_not_refreshable"
    assert port.staged == []


@pytest.mark.asyncio
async def test_dual_read_legacy_and_v2_resolution_preserves_history() -> None:
    legacy_effective = KnowledgeLegacyAttachment(
        source_knowledge_id="legacy-effective",
        revision_stamp=ResourceRevisionStamp(root_id="legacy-root-a"),
        origin_class="legacy_all",
        effective=True,
    )
    legacy_history = KnowledgeLegacyAttachment(
        source_knowledge_id="legacy-history",
        revision_stamp=ResourceRevisionStamp(root_id="legacy-root-b"),
        origin_class="selected_legacy",
        effective=False,
    )
    legacy_result = await _service(
        _FakePort(_scope(legacy=(legacy_effective, legacy_history)))
    ).read(object(), _target())

    assert legacy_result.v2_active is False
    assert legacy_result.effective_legacy_attachments == (legacy_effective,)
    assert legacy_result.history_legacy_attachments == (
        legacy_effective,
        legacy_history,
    )
    assert legacy_result.resolved_assignments == ()
    assert legacy_result.effective_count == 1

    old = b"old"
    new = b"new"
    assignments = (
        _assignment("a-ref", "kb-ref", "root-ref"),
        _assignment(
            "b-snapshot-stale",
            "kb-stale",
            "root-stale",
            mode="snapshot",
            stamp_revision="1",
            stamp_content=old,
        ),
        _assignment(
            "c-snapshot-deleted",
            "kb-snapshot-deleted",
            "root-snapshot-deleted",
            mode="snapshot",
            stamp_content=old,
        ),
        _assignment(
            "d-reference-deleted",
            "kb-reference-deleted",
            "root-reference-deleted",
        ),
    )
    snapshots = (
        _snapshot(
            "snapshot-stale",
            "b-snapshot-stale",
            "root-stale",
            revision="1",
            content=old,
        ),
        _snapshot(
            "snapshot-deleted",
            "c-snapshot-deleted",
            "root-snapshot-deleted",
            content=old,
        ),
    )
    sources = (
        _source("kb-ref", "root-ref"),
        _source(
            "kb-stale",
            "root-stale",
            revision="2",
            content=new,
        ),
        _source(
            "kb-snapshot-deleted",
            "root-snapshot-deleted",
            content=old,
            source_deleted=True,
        ),
        _source(
            "kb-reference-deleted",
            "root-reference-deleted",
            source_deleted=True,
        ),
    )
    v2_result = await _service(
        _FakePort(
            _scope(
                revision=3,
                v2_active=True,
                state="explicit_ids",
                assignments=assignments,
                snapshots=snapshots,
                legacy=(legacy_effective,),
                sources=sources,
            )
        )
    ).read(object(), _target())
    resolved = {
        item.assignment.assignment_id: item for item in v2_result.resolved_assignments
    }

    assert resolved["a-ref"].state is KnowledgeAssignmentState.ACTIVE
    assert resolved["a-ref"].effective is True
    assert resolved["b-snapshot-stale"].state is KnowledgeAssignmentState.STALE
    assert resolved["b-snapshot-stale"].effective is True
    assert resolved["b-snapshot-stale"].content_bytes == old
    assert (
        resolved["c-snapshot-deleted"].state is KnowledgeAssignmentState.SOURCE_DELETED
    )
    assert resolved["c-snapshot-deleted"].effective is False
    assert resolved["c-snapshot-deleted"].content_bytes == old
    assert (
        resolved["d-reference-deleted"].state is KnowledgeAssignmentState.SOURCE_DELETED
    )
    assert resolved["d-reference-deleted"].effective is False
    assert v2_result.effective_legacy_attachments == ()
    assert v2_result.history_legacy_attachments == (legacy_effective,)
    assert v2_result.effective_count == 2


@pytest.mark.asyncio
async def test_v2_read_falls_back_to_root_and_keeps_reference_snapshot_semantics() -> (
    None
):
    old_snapshot = b"immutable snapshot"
    current_reference = b"current reference"
    current_snapshot_source = b"changed snapshot source"
    reference_metadata = {"purpose": "current reference governance"}
    frozen_snapshot_metadata = {"purpose": "frozen snapshot governance"}
    current_snapshot_metadata = {"purpose": "changed source governance"}
    assignments = (
        _assignment(
            "assignment-reference",
            "obsolete-reference-row",
            "root-reference",
            stamp_content=b"old reference",
        ),
        _assignment(
            "assignment-snapshot",
            "obsolete-snapshot-row",
            "root-snapshot",
            mode="snapshot",
            stamp_content=old_snapshot,
        ),
    )
    snapshot = _snapshot(
        "snapshot-immutable",
        "assignment-snapshot",
        "root-snapshot",
        content=old_snapshot,
        governance_metadata=frozen_snapshot_metadata,
    )
    reference_source = KnowledgeSelectableSource(
        requested_knowledge_id="root-reference",
        source_knowledge_id="current-reference-row",
        revision_stamp=_stamp(
            "root-reference",
            revision="2",
            content=current_reference,
        ),
        content_bytes=current_reference,
        governance_metadata=reference_metadata,
    )
    snapshot_source = KnowledgeSelectableSource(
        requested_knowledge_id="root-snapshot",
        source_knowledge_id="current-snapshot-row",
        revision_stamp=_stamp(
            "root-snapshot",
            revision="2",
            content=current_snapshot_source,
        ),
        content_bytes=current_snapshot_source,
        governance_metadata=current_snapshot_metadata,
    )

    result = await _service(
        _FakePort(
            _scope(
                revision=5,
                v2_active=True,
                state="explicit_ids",
                assignments=assignments,
                snapshots=(snapshot,),
                sources=(reference_source, snapshot_source),
            )
        )
    ).read(object(), _target())
    resolved = {
        item.assignment.assignment_id: item for item in result.resolved_assignments
    }

    reference = resolved["assignment-reference"]
    assert reference.state is KnowledgeAssignmentState.ACTIVE
    assert reference.effective is True
    assert reference.revision_stamp == reference_source.revision_stamp
    assert reference.content_bytes == current_reference
    assert reference.resolved_source_knowledge_id == "current-reference-row"
    assert reference.governance_metadata == reference_metadata
    assert "governance_metadata" not in reference.to_dict()
    assert reference.to_dict()["resolved_source_knowledge_id"] == (
        "current-reference-row"
    )
    assert isinstance(reference_source.governance_metadata, dict)
    reference_source.governance_metadata["purpose"] = "changed after read"
    assert reference.governance_metadata == reference_metadata

    snapshot_result = resolved["assignment-snapshot"]
    assert snapshot_result.state is KnowledgeAssignmentState.STALE
    assert snapshot_result.effective is True
    assert snapshot_result.revision_stamp == snapshot.revision_stamp
    assert snapshot_result.content_bytes == old_snapshot
    assert snapshot_result.resolved_source_knowledge_id == "current-snapshot-row"
    assert snapshot_result.reason == "source_changed"
    assert snapshot_result.governance_metadata == frozen_snapshot_metadata
    assert snapshot_result.governance_metadata != current_snapshot_metadata
    assert "governance_metadata" not in snapshot_result.to_dict()


@pytest.mark.asyncio
async def test_legacy_snapshot_without_governance_metadata_projects_incomplete() -> (
    None
):
    content = b"legacy snapshot"
    assignment = _assignment(
        "assignment-legacy-snapshot",
        "kb-legacy",
        "root-legacy",
        mode="snapshot",
        stamp_content=content,
    )
    snapshot = _snapshot(
        "snapshot-legacy",
        "assignment-legacy-snapshot",
        "root-legacy",
        content=content,
    )
    source = _source(
        "kb-legacy",
        "root-legacy",
        content=content,
        governance_metadata={"purpose": "metadata added after snapshot"},
    )

    result = await _service(
        _FakePort(
            _scope(
                revision=1,
                v2_active=True,
                state="explicit_ids",
                assignments=(assignment,),
                snapshots=(snapshot,),
                sources=(source,),
            )
        )
    ).read(object(), _target())
    resolved = result.resolved_assignments[0]

    assert resolved.governance_metadata is None
    assert (
        project_knowledge_governance_from_resource(resolved)["metadata_status"]
        == "legacy_incomplete"
    )


@pytest.mark.asyncio
async def test_v2_read_includes_local_post_activation_attachments() -> None:
    local_content = b"target local knowledge"
    activation = NOW - timedelta(minutes=10)
    local = KnowledgeLocalAttachment(
        source_knowledge_id="local-kb",
        revision_stamp=_stamp(
            "local-root",
            revision="3",
            content=local_content,
        ),
        attached_at=NOW - timedelta(minutes=5),
        content_bytes=local_content,
    )
    legacy = KnowledgeLegacyAttachment(
        source_knowledge_id="legacy-kb",
        revision_stamp=ResourceRevisionStamp(root_id="legacy-root"),
        origin_class="legacy_all",
        effective=True,
    )

    result = await _service(
        _FakePort(
            _scope(
                revision=2,
                v2_active=True,
                state="omitted",
                legacy=(legacy,),
                local=(local,),
                v2_activated_at=activation,
            )
        )
    ).read(object(), _target())

    assert result.effective_assignments == ()
    assert result.effective_local_attachments == (local,)
    assert result.effective_legacy_attachments == ()
    assert result.v2_activated_at == activation
    assert result.effective_count == 1
    payload = result.to_dict()
    assert payload["v2_activated_at"] == activation.isoformat()
    assert payload["effective_local_attachments"] == [local.to_dict()]
    assert payload["effective_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tombstones", "expected_dropped"),
    [
        ((_tombstone("drop-root", "root-a"),), {"assignment-a"}),
        (
            (_tombstone("drop-all", None),),
            {"assignment-a", "assignment-b"},
        ),
    ],
)
async def test_current_tombstones_are_authoritative_but_preserve_history(
    tombstones: tuple[KnowledgePropagationTombstone, ...],
    expected_dropped: set[str],
) -> None:
    assignments = (
        _assignment("assignment-a", "kb-a", "root-a"),
        _assignment("assignment-b", "kb-b", "root-b"),
    )
    result = await _service(
        _FakePort(
            _scope(
                revision=2,
                v2_active=True,
                state="explicit_ids",
                assignments=assignments,
                tombstones=tombstones,
                sources=(
                    _source("kb-a", "root-a"),
                    _source("kb-b", "root-b"),
                ),
            )
        )
    ).read(object(), _target())

    resolved = {
        item.assignment.assignment_id: item for item in result.resolved_assignments
    }
    assert {
        assignment_id
        for assignment_id, item in resolved.items()
        if item.state is KnowledgeAssignmentState.DROPPED
    } == expected_dropped
    assert all(
        not resolved[assignment_id].effective for assignment_id in expected_dropped
    )
    assert result.history_assignments == assignments


@pytest.mark.asyncio
async def test_divergent_adapter_receipt_fails_closed_after_single_stage() -> None:
    port = _FakePort(
        _scope(sources=(_source("kb-1", "root-1"),)),
        divergent_receipt=True,
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).mutate(
            object(),
            _command(KnowledgeSelection.explicit_ids(["kb-1"], mode="reference")),
        )

    assert raised.value.code == "knowledge_propagation_stage_receipt_mismatch"
    assert len(port.staged) == 1


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (KnowledgeGrandfatherEvidence(), KnowledgeOriginClass.LEGACY_ALL),
        (
            KnowledgeGrandfatherEvidence(durable_selection_evidence=True),
            KnowledgeOriginClass.SELECTED_LEGACY,
        ),
        (
            KnowledgeGrandfatherEvidence(
                durable_selection_evidence=True,
                origin_missing=True,
            ),
            KnowledgeOriginClass.LEGACY_UNRESOLVED,
        ),
        (
            KnowledgeGrandfatherEvidence(origin_cycle=True),
            KnowledgeOriginClass.LEGACY_UNRESOLVED,
        ),
        (
            KnowledgeGrandfatherEvidence(content_divergent=True),
            KnowledgeOriginClass.LEGACY_UNRESOLVED,
        ),
    ],
)
def test_grandfather_classification_is_conservative(
    evidence: KnowledgeGrandfatherEvidence,
    expected: KnowledgeOriginClass,
) -> None:
    assert classify_legacy_origin(evidence) is expected


@pytest.mark.asyncio
async def test_grandfather_stages_canonical_non_activating_ledger_and_replays() -> None:
    unresolved = _grandfather_attachment(
        "kb-unresolved",
        evidence=KnowledgeGrandfatherEvidence(origin_missing=True),
    )
    selected = _grandfather_attachment(
        "kb-selected",
        evidence=KnowledgeGrandfatherEvidence(
            durable_selection_evidence=True,
        ),
    )
    legacy_all = _grandfather_attachment("kb-all")
    physical = tuple(
        item.to_legacy_attachment() for item in (legacy_all, selected, unresolved)
    )
    port = _FakePort(_scope(legacy=physical))
    service = _service(port)
    command = KnowledgeGrandfatherCommand(
        target=_target(),
        attachments=(unresolved, legacy_all, selected),
        actor_id="migration-v2",
        expected_revision=0,
        idempotency_key="grandfather:card-1",
    )

    receipt = await service.grandfather(object(), command)

    assert receipt.outcome is KnowledgeMutationOutcome.GRANDFATHERED
    assert receipt.previous_revision == 0
    assert receipt.revision == 1
    assert len(port.staged) == 1
    plan = port.staged[0]
    assert plan.operation_kind is KnowledgeMutationKind.GRANDFATHER
    assert plan.next_scope_v2_active is False
    assert plan.next_scope_selection_state is None
    assert plan.assignments_to_open == ()
    assert plan.snapshots_to_open == ()
    details = plan.ledger_entry.receipt.details
    assert details["legacy_content_preserved"] is True
    attachments = details["grandfathered_attachments"]
    assert [item["source_knowledge_id"] for item in attachments] == [
        "kb-all",
        "kb-selected",
        "kb-unresolved",
    ]
    assert [item["origin_class"] for item in attachments] == [
        "legacy_all",
        "selected_legacy",
        "legacy_unresolved",
    ]
    assert [item["effective"] for item in attachments] == [
        True,
        True,
        False,
    ]

    port.replay_entry = plan.ledger_entry
    replay = await service.grandfather(object(), command)
    assert replay.outcome is KnowledgeMutationOutcome.REPLAYED
    assert replay.original_outcome is KnowledgeMutationOutcome.GRANDFATHERED
    assert replay.revision == 1
    assert len(port.staged) == 1
    assert len(port.attempts) == 1


@pytest.mark.asyncio
async def test_grandfather_rejects_partial_inventory_with_audit_attempt() -> None:
    first = _grandfather_attachment("kb-1")
    second = _grandfather_attachment("kb-2")
    port = _FakePort(
        _scope(
            legacy=(
                first.to_legacy_attachment(),
                second.to_legacy_attachment(),
            )
        )
    )
    command = KnowledgeGrandfatherCommand(
        target=_target(),
        attachments=(first,),
        actor_id="migration-v2",
        expected_revision=0,
        idempotency_key="grandfather:partial",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).grandfather(object(), command)

    assert raised.value.code == "knowledge_propagation_grandfather_attachment_mismatch"
    assert raised.value.details["unclassified"] == ["kb-2"]
    assert raised.value.ledger_attempt is not None
    assert raised.value.ledger_attempt.outcome is KnowledgeMutationOutcome.REJECTED
    assert port.staged == []


@pytest.mark.asyncio
async def test_grandfather_never_replaces_an_active_v2_scope() -> None:
    attachment = _grandfather_attachment("kb-1")
    port = _FakePort(
        _scope(
            revision=4,
            v2_active=True,
            state="omitted",
            legacy=(attachment.to_legacy_attachment(),),
        )
    )
    command = KnowledgeGrandfatherCommand(
        target=_target(),
        attachments=(attachment,),
        actor_id="migration-v2",
        expected_revision=4,
        idempotency_key="grandfather:active",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).grandfather(object(), command)

    assert raised.value.code == "knowledge_propagation_grandfather_v2_active"
    assert raised.value.ledger_attempt is not None
    assert port.staged == []


@pytest.mark.asyncio
async def test_relink_reset_closes_current_records_without_legacy_fallback() -> None:
    reference = _assignment(
        "assignment-reference",
        "kb-reference",
        "root-reference",
    )
    snapshot_assignment = _assignment(
        "assignment-snapshot",
        "kb-snapshot",
        "root-snapshot",
        mode="snapshot",
    )
    snapshot = _snapshot(
        "snapshot-current",
        "assignment-snapshot",
        "root-snapshot",
    )
    tombstone = _tombstone("tombstone-current", "root-dropped")
    port = _FakePort(
        _scope(
            revision=7,
            v2_active=True,
            state="explicit_ids",
            assignments=(reference, snapshot_assignment),
            snapshots=(snapshot,),
            tombstones=(tombstone,),
        )
    )
    service = _service(port)
    command = KnowledgeRelinkResetCommand(
        target=_target(),
        previous_parent=KnowledgeParentKey("board-1", "spec", "spec-old"),
        next_parent=KnowledgeParentKey("board-1", "spec", "spec-new"),
        actor_id="agent-1",
        expected_revision=7,
        idempotency_key="relink:spec-old:spec-new",
    )

    receipt = await service.reset_for_relink(object(), command)

    assert receipt.operation_kind is KnowledgeMutationKind.RELINK_RESET
    assert receipt.previous_revision == 7
    assert receipt.revision == 8
    plan = port.staged[0]
    assert plan.next_scope_v2_active is True
    assert plan.next_scope_selection_state is KnowledgeSelectionState.OMITTED
    assert plan.parent == command.previous_parent
    assert plan.assignment_ids_to_close == (
        "assignment-reference",
        "assignment-snapshot",
    )
    assert plan.snapshot_ids_to_close == ("snapshot-current",)
    assert plan.tombstone_ids_to_close == ("tombstone-current",)
    assert plan.assignments_to_open == ()
    assert plan.snapshots_to_open == ()
    assert plan.tombstones_to_open == ()
    assert plan.supersession_links == ()
    assert receipt.details["relink"] == {
        "previous_parent": {
            "board_id": "board-1",
            "parent_type": "spec",
            "parent_id": "spec-old",
        },
        "next_parent": {
            "board_id": "board-1",
            "parent_type": "spec",
            "parent_id": "spec-new",
        },
    }
    result = KnowledgeMutationResultV2Projector.from_receipt(receipt)
    assert result.operation_kind is KnowledgeMutationKind.RELINK_RESET
    assert result.selection_state is KnowledgeSelectionState.OMITTED
    assert result.assignments == ()

    closed_at = NOW
    closed_reference = TemporalKnowledgeAssignment(
        assignment=reference.assignment,
        temporal=KnowledgeTemporalWindow(
            effective_from=reference.temporal.effective_from,
            effective_to=closed_at,
        ),
    )
    closed_snapshot_assignment = TemporalKnowledgeAssignment(
        assignment=snapshot_assignment.assignment,
        temporal=KnowledgeTemporalWindow(
            effective_from=snapshot_assignment.temporal.effective_from,
            effective_to=closed_at,
        ),
    )
    closed_snapshot = KnowledgePropagationSnapshot(
        snapshot_id=snapshot.snapshot_id,
        assignment_id=snapshot.assignment_id,
        revision_stamp=snapshot.revision_stamp,
        content_bytes=snapshot.content_bytes,
        temporal=KnowledgeTemporalWindow(
            effective_from=snapshot.temporal.effective_from,
            effective_to=closed_at,
        ),
    )
    closed_tombstone = KnowledgePropagationTombstone(
        tombstone_id=tombstone.tombstone_id,
        target=tombstone.target,
        root_id=tombstone.root_id,
        actor_id=tombstone.actor_id,
        justification=tombstone.justification,
        temporal=KnowledgeTemporalWindow(
            effective_from=tombstone.temporal.effective_from,
            effective_to=closed_at,
        ),
    )
    post_reset = await _service(
        _FakePort(
            _scope(
                revision=8,
                v2_active=True,
                state="omitted",
                assignments=(closed_reference, closed_snapshot_assignment),
                snapshots=(closed_snapshot,),
                tombstones=(closed_tombstone,),
                legacy=(
                    KnowledgeLegacyAttachment(
                        source_knowledge_id="legacy-json",
                        revision_stamp=ResourceRevisionStamp(root_id="legacy-root"),
                        origin_class="legacy_all",
                    ),
                ),
            )
        )
    ).read(object(), _target())
    assert post_reset.v2_active is True
    assert post_reset.selection_state is KnowledgeSelectionState.OMITTED
    assert post_reset.effective_count == 0
    assert post_reset.resolved_assignments == ()
    assert post_reset.effective_legacy_attachments == ()

    port.replay_entry = plan.ledger_entry
    replay = await service.reset_for_relink(object(), command)
    assert replay.outcome is KnowledgeMutationOutcome.REPLAYED
    assert replay.original_outcome is KnowledgeMutationOutcome.APPLIED
    assert replay.revision == 8
    assert len(port.scope_lookups) == 1
    assert len(port.staged) == 1
    assert len(port.attempts) == 1


@pytest.mark.asyncio
async def test_relink_reset_rejects_an_inactive_v2_scope_with_audit_attempt() -> None:
    port = _FakePort(_scope(revision=2))
    command = KnowledgeRelinkResetCommand(
        target=_target(),
        previous_parent=KnowledgeParentKey("board-1", "spec", "spec-old"),
        next_parent=KnowledgeParentKey("board-1", "spec", "spec-new"),
        actor_id="agent-1",
        expected_revision=2,
        idempotency_key="relink:inactive",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).reset_for_relink(object(), command)

    assert raised.value.code == "knowledge_propagation_relink_v2_inactive"
    assert raised.value.ledger_attempt is not None
    assert raised.value.ledger_attempt.operation_kind is (
        KnowledgeMutationKind.RELINK_RESET
    )
    assert port.staged == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "previous_parent", "next_parent"),
    [
        (
            KnowledgeTargetKey("board-1", "card", "card-1"),
            KnowledgeParentKey("board-1", "spec", "spec-old"),
            None,
        ),
        (
            KnowledgeTargetKey("board-1", "spec", "spec-target"),
            KnowledgeParentKey("board-1", "ideation", "ideation-old"),
            KnowledgeParentKey("board-1", "refinement", "refinement-new"),
        ),
        (
            KnowledgeTargetKey("board-1", "card", "card-1"),
            None,
            KnowledgeParentKey("board-1", "spec", "spec-repaired"),
        ),
    ],
)
async def test_relink_reset_supports_unlink_spec_reparent_and_repair(
    target: KnowledgeTargetKey,
    previous_parent: KnowledgeParentKey | None,
    next_parent: KnowledgeParentKey | None,
) -> None:
    port = _FakePort(
        _scope(
            target=target,
            revision=4,
            v2_active=True,
            state="omitted",
        )
    )
    command = KnowledgeRelinkResetCommand(
        target=target,
        previous_parent=previous_parent,
        next_parent=next_parent,
        actor_id="agent-1",
        expected_revision=4,
        idempotency_key=f"relink:{target.target_id}",
    )

    receipt = await _service(port).reset_for_relink(object(), command)

    assert receipt.revision == 5
    plan = port.staged[0]
    assert plan.parent == previous_parent
    assert plan.next_scope_v2_active is True
    assert plan.next_scope_selection_state is KnowledgeSelectionState.OMITTED
    assert receipt.details["relink"] == {
        "previous_parent": (
            None if previous_parent is None else previous_parent.to_dict()
        ),
        "next_parent": None if next_parent is None else next_parent.to_dict(),
    }


@pytest.mark.parametrize(
    ("target", "previous_parent", "next_parent", "error"),
    [
        (
            KnowledgeTargetKey("board-1", "card", "card-1"),
            KnowledgeParentKey("board-1", "refinement", "refinement-1"),
            None,
            "relink_parent_target_invalid",
        ),
        (
            KnowledgeTargetKey("board-1", "spec", "spec-1"),
            KnowledgeParentKey("board-1", "spec", "spec-parent"),
            None,
            "relink_parent_target_invalid",
        ),
        (
            KnowledgeTargetKey("board-1", "card", "card-1"),
            None,
            None,
            "relink_parents_missing",
        ),
    ],
)
def test_relink_reset_command_rejects_incoherent_parent_scope(
    target: KnowledgeTargetKey,
    previous_parent: KnowledgeParentKey | None,
    next_parent: KnowledgeParentKey | None,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        KnowledgeRelinkResetCommand(
            target=target,
            previous_parent=previous_parent,
            next_parent=next_parent,
            actor_id="agent-1",
            expected_revision=1,
            idempotency_key="relink:invalid",
        )


@pytest.mark.asyncio
async def test_creation_preflight_is_target_independent_revalidated_and_replay_safe() -> (
    None
):
    parent = KnowledgeParentKey("board-1", "refinement", "refinement-1")
    target_id = deterministic_knowledge_target_id(parent, "spec", "derive-1")
    semantic_hash = hashlib.sha256(b"spec semantic payload").hexdigest()
    command = KnowledgeCreationPreflightCommand(
        parent=parent,
        target_type="spec",
        selection=KnowledgeSelection.explicit_ids(
            ["kb-1"],
            mode="reference",
        ),
        actor_id="agent-1",
        idempotency_key="derive-1",
        expected_revision=None,
        justification="selected for this specification",
        semantic_creation_hash=semantic_hash,
        creation_result={
            "spec": {
                "id": target_id,
                "title": "Deterministic spec",
            }
        },
    )
    source = _source("kb-1", "root-1")
    target = command.target
    port = _FakePort(
        _scope(target=target, sources=(source,)),
        parent_evidence=_parent_evidence(parent, sources=(source,)),
    )
    service = _service(port)

    prepared = await service.preflight_creation(object(), command)

    assert isinstance(prepared, KnowledgeMutationPreparation)
    assert prepared.command.target == target
    assert target.target_id == target_id
    assert len(port.parent_lookups) == 1
    assert port.scope_lookups == []
    receipt = await service.mutate(object(), prepared)
    assert len(port.parent_lookups) == 2
    assert len(port.staged) == 1

    result = KnowledgeMutationResultV2Projector.from_receipt(receipt)
    assert result.operation_id == receipt.operation_id
    assert result.assignments[0].source_knowledge_id == "kb-1"
    assert result.creation_result["spec"]["id"] == target_id

    replay_port = _FakePort(
        _scope(target=target, revision=99),
        replay_entry=port.staged[0].ledger_entry,
    )
    replay = await _service(replay_port).preflight_creation(object(), command)
    assert isinstance(replay, KnowledgeMutationReceipt)
    assert replay.replayed is True
    assert replay.operation_id == receipt.operation_id
    assert replay.details == receipt.details
    assert replay_port.parent_lookups == []
    assert replay_port.scope_lookups == []


@pytest.mark.asyncio
async def test_preflight_evidence_change_rejects_mutation_without_staging() -> None:
    parent = KnowledgeParentKey("board-1", "refinement", "refinement-1")
    command = KnowledgeCreationPreflightCommand(
        parent=parent,
        target_type="spec",
        selection=KnowledgeSelection.explicit_ids(
            ["kb-1"],
            mode="reference",
        ),
        actor_id="agent-1",
        idempotency_key="derive-stale",
        justification="selected",
    )
    original = _source("kb-1", "root-1", revision="1")
    port = _FakePort(
        _scope(target=command.target, sources=(original,)),
        parent_evidence=_parent_evidence(parent, sources=(original,)),
    )
    service = _service(port)
    prepared = await service.preflight_creation(object(), command)
    assert isinstance(prepared, KnowledgeMutationPreparation)
    port.parent_evidence = _parent_evidence(
        parent,
        sources=(_source("kb-1", "root-1", revision="2"),),
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await service.mutate(object(), prepared)

    assert raised.value.code == "knowledge_propagation_preflight_stale"
    assert port.scope_lookups == []
    assert port.staged == []


@pytest.mark.asyncio
async def test_public_refresh_resolves_root_and_inherits_assignment_semantics() -> None:
    relevance = (KnowledgeRelevanceLink("acceptance_criterion", "ac-1"),)
    old_content = b"old snapshot"
    new_content = b"new source"
    old_assignment = _assignment(
        "assignment-old",
        "kb-old-revision",
        "root-1",
        mode="snapshot",
        stamp_content=old_content,
        justification="original relevance rationale",
        relevance_links=relevance,
    )
    port = _FakePort(
        _scope(
            revision=3,
            v2_active=True,
            state="explicit_ids",
            assignments=(old_assignment,),
            snapshots=(
                _snapshot(
                    "snapshot-old",
                    "assignment-old",
                    "root-1",
                    content=old_content,
                ),
            ),
            sources=(
                _source(
                    "kb-current",
                    "root-1",
                    revision="2",
                    content=new_content,
                ),
            ),
        )
    )
    command = KnowledgeRefreshByKnowledgeIdsCommand(
        target=_target(),
        knowledge_ids=("kb-current",),
        actor_id="agent-2",
        expected_revision=3,
        idempotency_key="refresh-root-1",
    )

    receipt = await _service(port).refresh_by_knowledge_ids(object(), command)
    plan = port.staged[0]
    refreshed = plan.assignments_to_open[0].assignment

    assert refreshed.justification == "original relevance rationale"
    assert refreshed.relevance_links == relevance
    assert plan.assignment_ids_to_close == ("assignment-old",)
    assert plan.snapshot_ids_to_close == ("snapshot-old",)
    result = KnowledgeMutationResultV2Projector.from_receipt(receipt)
    assert result.refreshed_knowledge_ids == ("root-1",)
    assert result.assignments == (refreshed,)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["reference", "missing", "ambiguous"])
async def test_public_refresh_invalid_resolution_is_zero_effect(
    failure: str,
) -> None:
    assignments = (
        _assignment(
            "assignment-1",
            "kb-1",
            "root-1",
            mode="reference" if failure == "reference" else "snapshot",
        ),
    )
    snapshots = (
        ()
        if failure == "reference"
        else (_snapshot("snapshot-1", "assignment-1", "root-1"),)
    )
    sources = (
        ()
        if failure == "missing"
        else (
            _source("kb-requested", "root-1"),
            *((_source("kb-alias", "root-1"),) if failure == "ambiguous" else ()),
        )
    )
    knowledge_ids = (
        ("kb-requested", "kb-alias") if failure == "ambiguous" else ("kb-requested",)
    )
    port = _FakePort(
        _scope(
            revision=2,
            v2_active=True,
            state="explicit_ids",
            assignments=assignments,
            snapshots=snapshots,
            sources=sources,
        )
    )
    command = KnowledgeRefreshByKnowledgeIdsCommand(
        target=_target(),
        knowledge_ids=knowledge_ids,
        actor_id="agent-2",
        expected_revision=2,
        idempotency_key=f"refresh-{failure}",
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).refresh_by_knowledge_ids(object(), command)

    assert raised.value.code == "knowledge_assignment_not_refreshable"
    assert port.staged == []


@pytest.mark.asyncio
async def test_existing_card_mutation_revalidates_relevance_against_linked_spec() -> (
    None
):
    parent = KnowledgeParentKey("board-1", "spec", "spec-1")
    source = _source("kb-1", "root-1")
    port = _FakePort(
        _scope(sources=(source,)),
        parent_evidence=_parent_evidence(
            parent,
            sources=(source,),
            linked_spec_id="spec-1",
            functional_requirement_ids=("fr-owned",),
        ),
    )
    command = KnowledgeMutationCommand(
        target=_target(),
        selection=KnowledgeSelection.explicit_ids(
            ["kb-1"],
            mode="reference",
        ),
        actor_id="agent-1",
        expected_revision=0,
        idempotency_key="put-card-kb",
        justification="claimed relevance",
        relevance_links=(
            KnowledgeRelevanceLink("functional_requirement", "fr-foreign"),
        ),
        parent=parent,
    )

    with pytest.raises(KnowledgePropagationServiceError) as raised:
        await _service(port).mutate(object(), command)

    assert raised.value.code == "knowledge_relevance_invalid"
    assert raised.value.details["missing"] == ["functional_requirement:fr-foreign"]
    assert port.scope_lookups == []
    assert port.staged == []
