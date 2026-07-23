"""Acceptance contract for the selective Knowledge Base propagation port."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import hashlib

import pytest

from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeAssignment,
    KnowledgeOriginClass,
    KnowledgeSelection,
    KnowledgeSelectionState,
)
from okto_pulse.core.domain.resource_revision import ResourceRevisionStamp
from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeLegacyAttachment,
    KnowledgeLocalAttachment,
    KnowledgeMutationKind,
    KnowledgeMutationLedgerEntry,
    KnowledgeMutationPlan,
    KnowledgeMutationReceipt,
    KnowledgePropagationScope,
    KnowledgePropagationSnapshot,
    KnowledgePropagationTombstone,
    KnowledgeRecordKind,
    KnowledgeSupersessionLink,
    KnowledgeTargetKey,
    KnowledgeTemporalWindow,
    TemporalKnowledgeAssignment,
    get_knowledge_propagation_port,
    register_knowledge_propagation_port,
    reset_knowledge_propagation_port_for_tests,
)


NOW = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
CONTENT = b"canonical snapshot bytes"
CONTENT_HASH = hashlib.sha256(CONTENT).hexdigest()
REQUEST_HASH = hashlib.sha256(b"request").hexdigest()


@pytest.fixture(autouse=True)
def _clean_runtime_registry():
    reset_knowledge_propagation_port_for_tests()
    yield
    reset_knowledge_propagation_port_for_tests()


def _target() -> KnowledgeTargetKey:
    return KnowledgeTargetKey("board-1", "card", "card-1")


def _stamp(
    *,
    root_id: str = "kb-root",
    content_hash: str = CONTENT_HASH,
) -> ResourceRevisionStamp:
    return ResourceRevisionStamp(
        root_id=root_id,
        immediate_parent_id="kb-parent",
        source_revision="7",
        source_content_sha256=content_hash,
    )


def _window(
    *,
    effective_from: datetime = NOW,
    effective_to: datetime | None = None,
    superseded_by_id: str | None = None,
) -> KnowledgeTemporalWindow:
    return KnowledgeTemporalWindow(
        effective_from=effective_from,
        effective_to=effective_to,
        superseded_by_id=superseded_by_id,
    )


def _assignment(
    *,
    assignment_id: str = "assignment-1",
    mode: str = "reference",
    state: str = "active",
    revision: int = 1,
    root_id: str = "kb-root",
    source_knowledge_id: str = "kb-1",
    actor_id: str = "agent-1",
    current: bool = True,
) -> TemporalKnowledgeAssignment:
    return TemporalKnowledgeAssignment(
        assignment=KnowledgeAssignment(
            assignment_id=assignment_id,
            board_id="board-1",
            target_type="card",
            target_id="card-1",
            source_knowledge_id=source_knowledge_id,
            revision_stamp=_stamp(root_id=root_id),
            mode=mode,
            state=state,
            origin_class="v2",
            actor_id=actor_id,
            revision=revision,
            justification="required by AC-B1",
        ),
        temporal=(
            _window()
            if current
            else _window(
                effective_to=NOW + timedelta(seconds=1),
                superseded_by_id="assignment-successor",
            )
        ),
    )


def _snapshot(
    *,
    snapshot_id: str = "snapshot-1",
    assignment_id: str = "assignment-1",
    content: bytes = CONTENT,
    content_hash: str = CONTENT_HASH,
    root_id: str = "kb-root",
    current: bool = True,
) -> KnowledgePropagationSnapshot:
    return KnowledgePropagationSnapshot(
        snapshot_id=snapshot_id,
        assignment_id=assignment_id,
        revision_stamp=_stamp(root_id=root_id, content_hash=content_hash),
        content_bytes=content,
        temporal=(
            _window()
            if current
            else _window(
                effective_to=NOW + timedelta(seconds=1),
                superseded_by_id="snapshot-successor",
            )
        ),
    )


def _tombstone(
    *,
    tombstone_id: str = "tombstone-1",
    root_id: str | None = "kb-root",
    actor_id: str = "agent-1",
    current: bool = True,
) -> KnowledgePropagationTombstone:
    return KnowledgePropagationTombstone(
        tombstone_id=tombstone_id,
        target=_target(),
        root_id=root_id,
        actor_id=actor_id,
        justification="no longer relevant",
        temporal=(
            _window()
            if current
            else _window(
                effective_to=NOW + timedelta(seconds=1),
                superseded_by_id="tombstone-successor",
            )
        ),
    )


def _global_tombstone() -> KnowledgePropagationTombstone:
    return _tombstone(
        tombstone_id="tombstone-global",
        root_id=None,
    )


def _receipt(
    kind: KnowledgeMutationKind,
    *,
    replayed: bool = False,
) -> KnowledgeMutationReceipt:
    return KnowledgeMutationReceipt(
        operation_id=f"operation-{kind.value}",
        target=_target(),
        operation_kind=kind,
        previous_revision=0,
        revision=1,
        request_hash=REQUEST_HASH,
        applied_at=NOW,
        replayed=replayed,
    )


def _ledger(kind: KnowledgeMutationKind) -> KnowledgeMutationLedgerEntry:
    return KnowledgeMutationLedgerEntry(
        target=_target(),
        idempotency_key=f"idempotency-{kind.value}",
        request_hash=REQUEST_HASH,
        operation_kind=kind,
        receipt=_receipt(kind),
        recorded_at=NOW,
        actor_id="agent-1",
    )


def _plan(
    kind: KnowledgeMutationKind,
    *,
    selection: KnowledgeSelection | None,
    next_state: KnowledgeSelectionState,
    assignments: tuple[TemporalKnowledgeAssignment, ...] = (),
    tombstones: tuple[KnowledgePropagationTombstone, ...] = (),
    snapshots: tuple[KnowledgePropagationSnapshot, ...] = (),
    assignment_ids_to_close: tuple[str, ...] = (),
    tombstone_ids_to_close: tuple[str, ...] = (),
    snapshot_ids_to_close: tuple[str, ...] = (),
    supersession_links: tuple[KnowledgeSupersessionLink, ...] = (),
) -> KnowledgeMutationPlan:
    return KnowledgeMutationPlan(
        operation_id=f"operation-{kind.value}",
        target=_target(),
        operation_kind=kind,
        selection=selection,
        expected_revision=0,
        next_revision=1,
        actor_id="agent-1",
        occurred_at=NOW,
        idempotency_key=f"idempotency-{kind.value}",
        request_hash=REQUEST_HASH,
        next_scope_selection_state=next_state,
        assignments_to_open=assignments,
        assignment_ids_to_close=assignment_ids_to_close,
        tombstones_to_open=tombstones,
        tombstone_ids_to_close=tombstone_ids_to_close,
        snapshots_to_open=snapshots,
        snapshot_ids_to_close=snapshot_ids_to_close,
        supersession_links=supersession_links,
        ledger_entry=_ledger(kind),
    )


def test_snapshot_requires_revision_evidence_and_matching_canonical_bytes() -> None:
    snapshot = _snapshot()

    assert snapshot.revision_stamp == _stamp()
    assert snapshot.to_dict()["contract_version"] == 2
    assert snapshot.to_dict()["content_sha256"] == CONTENT_HASH
    assert snapshot.to_dict()["content_size_bytes"] == len(CONTENT)

    with pytest.raises(
        ValueError, match="knowledge_propagation_snapshot_hash_mismatch"
    ):
        _snapshot(content=b"tampered")

    with pytest.raises(
        ValueError, match="knowledge_propagation_revision_evidence_required"
    ):
        KnowledgePropagationSnapshot(
            snapshot_id="snapshot-no-evidence",
            assignment_id="assignment-1",
            revision_stamp=ResourceRevisionStamp(root_id="kb-root"),
            content_bytes=CONTENT,
            temporal=_window(),
        )


def test_temporal_window_requires_utc_order_and_coherent_supersession() -> None:
    current = _window()
    closed = _window(
        effective_to=NOW + timedelta(seconds=1),
        superseded_by_id="assignment-2",
    )

    assert current.is_current is True
    assert closed.is_current is False
    assert closed.to_dict()["superseded_by_id"] == "assignment-2"

    with pytest.raises(ValueError, match="effective_from_invalid"):
        _window(effective_from=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="effective_window_invalid"):
        _window(
            effective_to=NOW - timedelta(microseconds=1),
            superseded_by_id="assignment-2",
        )
    closed_without_successor = _window(effective_to=NOW + timedelta(seconds=1))
    assert closed_without_successor.is_current is False
    with pytest.raises(ValueError, match="supersession_window_incoherent"):
        _window(superseded_by_id="assignment-2")
    with pytest.raises(FrozenInstanceError):
        current.effective_to = NOW  # type: ignore[misc]


def test_scope_v2_marker_disables_legacy_fallback_without_erasing_history() -> None:
    legacy = KnowledgeLegacyAttachment(
        source_knowledge_id="legacy-kb",
        revision_stamp=ResourceRevisionStamp(root_id="legacy-root"),
        origin_class=KnowledgeOriginClass.LEGACY_ALL,
        effective=False,
    )
    legacy_scope = KnowledgePropagationScope(
        target=_target(),
        scope_revision=0,
        v2_active=False,
        selection_state=None,
        legacy_attachments=(legacy,),
    )
    v2_empty = KnowledgePropagationScope(
        target=_target(),
        scope_revision=1,
        v2_active=True,
        selection_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
        legacy_attachments=(legacy,),
    )

    assert legacy_scope.v2_active is False
    assert legacy_scope.selection_state is None
    assert v2_empty.v2_active is True
    assert v2_empty.selection_state is KnowledgeSelectionState.EXPLICIT_EMPTY
    assert v2_empty.legacy_attachments == (legacy,)
    assert v2_empty.legacy_attachments[0].effective is False
    v2_omitted = KnowledgePropagationScope(
        target=_target(),
        scope_revision=1,
        v2_active=True,
        selection_state=KnowledgeSelectionState.OMITTED,
        legacy_attachments=(legacy,),
    )
    assert v2_omitted.selection_state is KnowledgeSelectionState.OMITTED

    with pytest.raises(ValueError, match="inactive_scope_state_invalid"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=0,
            v2_active=False,
            selection_state="explicit_ids",
        )
    with pytest.raises(ValueError, match="active_scope_state_invalid"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state=None,
        )


def test_scope_carries_immutable_activation_and_local_attachment_evidence() -> None:
    activation = NOW - timedelta(minutes=2)
    local = KnowledgeLocalAttachment(
        source_knowledge_id="local-kb",
        revision_stamp=_stamp(),
        attached_at=NOW - timedelta(minutes=1),
        content_bytes=CONTENT,
    )
    scope = KnowledgePropagationScope(
        target=_target(),
        scope_revision=3,
        v2_active=True,
        selection_state="omitted",
        local_attachments=(local,),
        v2_activated_at=activation,
    )

    assert scope.v2_activated_at == activation
    assert scope.local_attachments == (local,)
    local_payload = local.to_dict()
    assert local_payload["attached_at"] == local.attached_at.isoformat()
    assert local_payload["content_available"] is True
    assert local_payload["content_size_bytes"] == len(CONTENT)
    with pytest.raises(FrozenInstanceError):
        scope.v2_activated_at = NOW  # type: ignore[misc]

    with pytest.raises(
        ValueError,
        match="local_attachment_predates_v2_activation",
    ):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=3,
            v2_active=True,
            selection_state="omitted",
            local_attachments=(local,),
            v2_activated_at=NOW,
        )

    boundary_local = KnowledgeLocalAttachment(
        source_knowledge_id="local-at-boundary",
        revision_stamp=_stamp(),
        attached_at=activation,
        content_bytes=CONTENT,
    )
    with pytest.raises(
        ValueError,
        match="local_attachment_predates_v2_activation",
    ):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=3,
            v2_active=True,
            selection_state="omitted",
            local_attachments=(boundary_local,),
            v2_activated_at=activation,
        )

    with pytest.raises(
        ValueError,
        match="local_attachment_hash_mismatch",
    ):
        KnowledgeLocalAttachment(
            source_knowledge_id="tampered-local",
            revision_stamp=_stamp(),
            attached_at=NOW,
            content_bytes=b"tampered",
        )


def test_legacy_unresolved_is_always_history_only() -> None:
    unresolved = KnowledgeLegacyAttachment(
        source_knowledge_id="legacy-unresolved",
        revision_stamp=ResourceRevisionStamp(root_id="legacy-root"),
        origin_class=KnowledgeOriginClass.LEGACY_UNRESOLVED,
        effective=True,
    )

    assert unresolved.effective is False
    assert unresolved.to_dict()["effective"] is False


def test_scope_allows_only_one_current_assignment_per_root() -> None:
    with pytest.raises(ValueError, match="current_assignment_root_ambiguous"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            assignments=(
                _assignment(assignment_id="assignment-a"),
                _assignment(assignment_id="assignment-b"),
            ),
        )

    scope = KnowledgePropagationScope(
        target=_target(),
        scope_revision=1,
        v2_active=True,
        selection_state="explicit_ids",
        assignments=(
            _assignment(assignment_id="assignment-current"),
            _assignment(assignment_id="assignment-history", current=False),
        ),
    )
    assert len(scope.assignments) == 2


def test_scope_enforces_current_tombstone_uniqueness_and_global_exclusion() -> None:
    with pytest.raises(ValueError, match="current_tombstone_root_ambiguous"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            tombstones=(
                _tombstone(tombstone_id="tombstone-a"),
                _tombstone(tombstone_id="tombstone-b"),
            ),
        )

    with pytest.raises(ValueError, match="current_global_tombstone_conflict"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_empty",
            tombstones=(
                _global_tombstone(),
                _tombstone(tombstone_id="tombstone-root"),
            ),
        )

    scope = KnowledgePropagationScope(
        target=_target(),
        scope_revision=2,
        v2_active=True,
        selection_state="explicit_ids",
        tombstones=(
            _tombstone(
                tombstone_id="tombstone-global-history",
                root_id=None,
                current=False,
            ),
            _tombstone(tombstone_id="tombstone-current"),
        ),
    )
    assert len(scope.tombstones) == 2


def test_current_snapshot_requires_one_current_snapshot_assignment() -> None:
    with pytest.raises(ValueError, match="current_snapshot_assignment_missing"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            snapshots=(_snapshot(),),
        )

    with pytest.raises(
        ValueError,
        match="current_snapshot_assignment_mode_invalid",
    ):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            assignments=(_assignment(),),
            snapshots=(_snapshot(),),
        )

    snapshot_assignment = _assignment(mode="snapshot")
    with pytest.raises(
        ValueError,
        match="current_snapshot_assignment_ambiguous",
    ):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            assignments=(snapshot_assignment,),
            snapshots=(
                _snapshot(snapshot_id="snapshot-a"),
                _snapshot(snapshot_id="snapshot-b"),
            ),
        )

    with pytest.raises(ValueError, match="current_snapshot_revision_mismatch"):
        KnowledgePropagationScope(
            target=_target(),
            scope_revision=1,
            v2_active=True,
            selection_state="explicit_ids",
            assignments=(snapshot_assignment,),
            snapshots=(_snapshot(root_id="different-root"),),
        )

    scope = KnowledgePropagationScope(
        target=_target(),
        scope_revision=1,
        v2_active=True,
        selection_state="explicit_ids",
        assignments=(snapshot_assignment,),
        snapshots=(
            _snapshot(),
            _snapshot(snapshot_id="snapshot-history", current=False),
        ),
    )
    assert len(scope.snapshots) == 2


def test_atomic_plan_requires_a_coherent_ledger_and_complete_next_revision() -> None:
    plan = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(),),
    )

    assert plan.next_revision == plan.expected_revision + 1
    assert plan.ledger_entry is not None
    assert plan.ledger_entry.receipt.operation_id == plan.operation_id
    assert plan.assignments_to_open[0].assignment.revision == plan.next_revision

    wrong_ledger = KnowledgeMutationLedgerEntry(
        target=_target(),
        idempotency_key="different-key",
        request_hash=REQUEST_HASH,
        operation_kind=KnowledgeMutationKind.REPLACE,
        receipt=_receipt(KnowledgeMutationKind.REPLACE),
        recorded_at=NOW,
    )
    with pytest.raises(ValueError, match="ledger_entry_incoherent"):
        KnowledgeMutationPlan(
            operation_id="operation-replace",
            target=_target(),
            operation_kind="replace",
            selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
            expected_revision=0,
            next_revision=1,
            actor_id="agent-1",
            occurred_at=NOW,
            idempotency_key="idempotency-replace",
            request_hash=REQUEST_HASH,
            next_scope_selection_state="explicit_ids",
            assignments_to_open=(_assignment(),),
            ledger_entry=wrong_ledger,
        )


def test_all_mutation_kinds_have_distinct_valid_write_shapes() -> None:
    omitted = _plan(
        KnowledgeMutationKind.REPLACE_OMITTED,
        selection=KnowledgeSelection.omitted(),
        next_state=KnowledgeSelectionState.OMITTED,
        assignment_ids_to_close=("assignment-1",),
    )
    replace = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="snapshot"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(mode="snapshot"),),
        snapshots=(_snapshot(),),
    )
    drop_delta = _plan(
        KnowledgeMutationKind.DROP_DELTA,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="drop"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(mode="drop", state="dropped"),),
        tombstones=(_tombstone(),),
        assignment_ids_to_close=("assignment-previous",),
    )
    replace_empty = _plan(
        KnowledgeMutationKind.REPLACE_EMPTY,
        selection=KnowledgeSelection.explicit_empty(),
        next_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
        tombstones=(_global_tombstone(),),
        assignment_ids_to_close=("assignment-1",),
    )
    refresh = _plan(
        KnowledgeMutationKind.REFRESH_SNAPSHOT,
        selection=None,
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(
            _assignment(
                assignment_id="assignment-refreshed",
                mode="snapshot",
            ),
        ),
        snapshots=(
            _snapshot(
                snapshot_id="snapshot-refreshed",
                assignment_id="assignment-refreshed",
            ),
        ),
        assignment_ids_to_close=("assignment-previous",),
        snapshot_ids_to_close=("snapshot-previous",),
        supersession_links=(
            KnowledgeSupersessionLink(
                record_kind="assignment",
                previous_id="assignment-previous",
                successor_id="assignment-refreshed",
            ),
            KnowledgeSupersessionLink(
                record_kind="snapshot",
                previous_id="snapshot-previous",
                successor_id="snapshot-refreshed",
            ),
        ),
    )

    assert omitted.operation_kind is KnowledgeMutationKind.REPLACE_OMITTED
    assert omitted.assignments_to_open == ()
    assert replace.operation_kind is KnowledgeMutationKind.REPLACE
    assert drop_delta.tombstones_to_open == (_tombstone(),)
    assert replace_empty.assignments_to_open == ()
    assert replace_empty.tombstones_to_open[0].root_id is None
    assert refresh.selection is None
    assert refresh.snapshots_to_open[0].snapshot_id == "snapshot-refreshed"


def test_plan_rejects_actor_mismatch_for_every_actor_bearing_operation() -> None:
    replace_plan = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(
            ["kb-1"],
            mode="reference",
        ),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(),),
    )
    drop_plan = _plan(
        KnowledgeMutationKind.DROP_DELTA,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="drop"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(mode="drop", state="dropped"),),
        tombstones=(_tombstone(),),
    )
    empty_plan = _plan(
        KnowledgeMutationKind.REPLACE_EMPTY,
        selection=KnowledgeSelection.explicit_empty(),
        next_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
        tombstones=(_global_tombstone(),),
    )
    refresh_plan = _plan(
        KnowledgeMutationKind.REFRESH_SNAPSHOT,
        selection=None,
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(
            _assignment(
                assignment_id="assignment-refreshed",
                mode="snapshot",
            ),
        ),
        snapshots=(
            _snapshot(
                snapshot_id="snapshot-refreshed",
                assignment_id="assignment-refreshed",
            ),
        ),
        assignment_ids_to_close=("assignment-previous",),
        snapshot_ids_to_close=("snapshot-previous",),
        supersession_links=(
            KnowledgeSupersessionLink(
                record_kind="assignment",
                previous_id="assignment-previous",
                successor_id="assignment-refreshed",
            ),
            KnowledgeSupersessionLink(
                record_kind="snapshot",
                previous_id="snapshot-previous",
                successor_id="snapshot-refreshed",
            ),
        ),
    )

    for plan in (replace_plan, drop_plan, empty_plan, refresh_plan):
        with pytest.raises(ValueError, match="plan_actor_incoherent"):
            replace(plan, actor_id="different-agent")


def test_plan_rejects_extraneous_or_incomplete_operation_shapes() -> None:
    omitted = _plan(
        KnowledgeMutationKind.REPLACE_OMITTED,
        selection=KnowledgeSelection.omitted(),
        next_state=KnowledgeSelectionState.OMITTED,
    )
    with pytest.raises(ValueError, match="replace_omitted_plan_invalid"):
        replace(omitted, tombstones_to_open=(_global_tombstone(),))

    reference = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(
            ["kb-1"],
            mode="reference",
        ),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(),),
    )
    with pytest.raises(ValueError, match="replace_plan_invalid"):
        replace(reference, tombstones_to_open=(_tombstone(),))

    snapshot = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="snapshot"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(mode="snapshot"),),
        snapshots=(_snapshot(),),
    )
    with pytest.raises(ValueError, match="replace_plan_invalid"):
        replace(snapshot, snapshots_to_open=())

    drop = _plan(
        KnowledgeMutationKind.DROP_DELTA,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="drop"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(mode="drop", state="dropped"),),
        tombstones=(_tombstone(),),
    )
    with pytest.raises(ValueError, match="drop_delta_plan_invalid"):
        replace(drop, assignments_to_open=())

    explicit_empty = _plan(
        KnowledgeMutationKind.REPLACE_EMPTY,
        selection=KnowledgeSelection.explicit_empty(),
        next_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
        tombstones=(_global_tombstone(),),
    )
    with pytest.raises(ValueError, match="replace_empty_plan_invalid"):
        replace(explicit_empty, tombstones_to_open=(_tombstone(),))

    refresh = _plan(
        KnowledgeMutationKind.REFRESH_SNAPSHOT,
        selection=None,
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(
            _assignment(
                assignment_id="assignment-refreshed",
                mode="snapshot",
            ),
        ),
        snapshots=(
            _snapshot(
                snapshot_id="snapshot-refreshed",
                assignment_id="assignment-refreshed",
            ),
        ),
        assignment_ids_to_close=("assignment-previous",),
        snapshot_ids_to_close=("snapshot-previous",),
        supersession_links=(
            KnowledgeSupersessionLink(
                record_kind="assignment",
                previous_id="assignment-previous",
                successor_id="assignment-refreshed",
            ),
            KnowledgeSupersessionLink(
                record_kind="snapshot",
                previous_id="snapshot-previous",
                successor_id="snapshot-refreshed",
            ),
        ),
    )
    with pytest.raises(ValueError, match="refresh_snapshot_plan_invalid"):
        replace(refresh, supersession_links=())


def test_receipt_replay_preserves_the_original_committed_result() -> None:
    original = _receipt(KnowledgeMutationKind.REPLACE)
    replay = original.as_replay()

    assert replay.replayed is True
    assert replay.operation_id == original.operation_id
    assert replay.previous_revision == original.previous_revision
    assert replay.revision == original.revision
    assert replay.request_hash == original.request_hash
    assert replay.applied_at == original.applied_at
    assert replay.to_dict()["contract_version"] == 2
    assert original.replayed is False


def test_plan_validates_explicit_supersession_links_against_the_write_set() -> None:
    base = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(),),
    )
    link = KnowledgeSupersessionLink(
        record_kind=KnowledgeRecordKind.ASSIGNMENT,
        previous_id="assignment-old",
        successor_id="assignment-1",
    )
    plan = replace(
        base,
        assignment_ids_to_close=("assignment-old",),
        supersession_links=(link,),
    )

    assert plan.supersession_links == (link,)
    with pytest.raises(ValueError, match="supersession_link_incoherent"):
        replace(
            base,
            assignment_ids_to_close=("assignment-old",),
            supersession_links=(
                KnowledgeSupersessionLink(
                    record_kind="assignment",
                    previous_id="assignment-old",
                    successor_id="assignment-missing",
                ),
            ),
        )


@pytest.mark.asyncio
async def test_runtime_registry_fails_closed_and_port_stages_exactly_one_plan() -> None:
    with pytest.raises(RuntimeError, match="knowledge_propagation_port_not_configured"):
        get_knowledge_propagation_port()

    class FakePort:
        def __init__(self) -> None:
            self.staged: list[KnowledgeMutationPlan] = []

        async def get_idempotency_entry(self, context, request):
            return None

        async def load_scope(self, context, request):
            raise AssertionError("not used by this mutation test")

        async def stage_mutation(self, context, plan):
            self.staged.append(plan)
            assert context is unit_of_work
            return plan.ledger_entry.receipt

    unit_of_work = object()
    port = FakePort()
    register_knowledge_propagation_port(port)
    assert get_knowledge_propagation_port() is port

    plan = _plan(
        KnowledgeMutationKind.REPLACE,
        selection=KnowledgeSelection.explicit_ids(["kb-1"], mode="reference"),
        next_state=KnowledgeSelectionState.EXPLICIT_IDS,
        assignments=(_assignment(),),
    )
    receipt = await get_knowledge_propagation_port().stage_mutation(unit_of_work, plan)

    assert port.staged == [plan]
    assert receipt == plan.ledger_entry.receipt
    assert not hasattr(port, "commit")
    assert not hasattr(port, "rollback")
