"""Acceptance contract for the selective Knowledge Base propagation port."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
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
    KnowledgeMutationKind,
    KnowledgeMutationLedgerEntry,
    KnowledgeMutationPlan,
    KnowledgeMutationReceipt,
    KnowledgePropagationScope,
    KnowledgePropagationSnapshot,
    KnowledgePropagationTombstone,
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


def _stamp(*, content_hash: str = CONTENT_HASH) -> ResourceRevisionStamp:
    return ResourceRevisionStamp(
        root_id="kb-root",
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
) -> TemporalKnowledgeAssignment:
    return TemporalKnowledgeAssignment(
        assignment=KnowledgeAssignment(
            assignment_id=assignment_id,
            board_id="board-1",
            target_type="card",
            target_id="card-1",
            source_knowledge_id="kb-1",
            revision_stamp=_stamp(),
            mode=mode,
            state=state,
            origin_class="v2",
            actor_id="agent-1",
            revision=revision,
            justification="required by AC-B1",
        ),
        temporal=_window(),
    )


def _snapshot(
    *,
    snapshot_id: str = "snapshot-1",
    assignment_id: str = "assignment-1",
    content: bytes = CONTENT,
    content_hash: str = CONTENT_HASH,
) -> KnowledgePropagationSnapshot:
    return KnowledgePropagationSnapshot(
        snapshot_id=snapshot_id,
        assignment_id=assignment_id,
        revision_stamp=_stamp(content_hash=content_hash),
        content_bytes=content,
        temporal=_window(),
    )


def _tombstone() -> KnowledgePropagationTombstone:
    return KnowledgePropagationTombstone(
        tombstone_id="tombstone-1",
        target=_target(),
        root_id="kb-root",
        actor_id="agent-1",
        justification="no longer relevant",
        temporal=_window(),
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
        snapshots_to_open=snapshots,
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
    with pytest.raises(ValueError, match="supersession_window_incoherent"):
        _window(effective_to=NOW + timedelta(seconds=1))
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
            selection=KnowledgeSelection.explicit_ids(
                ["kb-1"], mode="reference"
            ),
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
        tombstones=(_tombstone(),),
        assignment_ids_to_close=("assignment-1",),
    )
    replace_empty = _plan(
        KnowledgeMutationKind.REPLACE_EMPTY,
        selection=KnowledgeSelection.explicit_empty(),
        next_state=KnowledgeSelectionState.EXPLICIT_EMPTY,
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
    )

    assert replace.operation_kind is KnowledgeMutationKind.REPLACE
    assert drop_delta.tombstones_to_open == (_tombstone(),)
    assert replace_empty.assignments_to_open == ()
    assert refresh.selection is None
    assert refresh.snapshots_to_open[0].snapshot_id == "snapshot-refreshed"


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


@pytest.mark.asyncio
async def test_runtime_registry_fails_closed_and_port_stages_exactly_one_plan() -> None:
    with pytest.raises(
        RuntimeError, match="knowledge_propagation_port_not_configured"
    ):
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
    receipt = await get_knowledge_propagation_port().stage_mutation(
        unit_of_work, plan
    )

    assert port.staged == [plan]
    assert receipt == plan.ledger_entry.receipt
    assert not hasattr(port, "commit")
    assert not hasattr(port, "rollback")
