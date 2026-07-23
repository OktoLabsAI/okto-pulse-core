"""Outcome vocabulary for the selective-propagation idempotency ledger."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from okto_pulse.core.ports.knowledge_propagation import (
    KnowledgeMutationAttempt,
    KnowledgeMutationKind,
    KnowledgeMutationLedgerEntry,
    KnowledgeMutationOutcome,
    KnowledgeMutationReceipt,
    KnowledgeTargetKey,
    get_knowledge_mutation_audit_sink,
    register_knowledge_mutation_audit_sink,
    reset_knowledge_mutation_audit_sink_for_tests,
)


NOW = datetime(2026, 7, 23, 14, 0, tzinfo=timezone.utc)
REQUEST_HASH = "a" * 64


def _target(
    *,
    board_id: str = "board-1",
    target_id: str = "card-1",
) -> KnowledgeTargetKey:
    return KnowledgeTargetKey(board_id, "card", target_id)


def _receipt(
    *,
    outcome: KnowledgeMutationOutcome | str | None = None,
    operation_kind: KnowledgeMutationKind | str = KnowledgeMutationKind.REPLACE,
    previous_revision: int = 4,
    revision: int = 5,
    reason_code: str | None = None,
    reason_detail: str | None = None,
    original_outcome: KnowledgeMutationOutcome | str | None = None,
    replayed: bool = False,
    target: KnowledgeTargetKey | None = None,
    request_hash: str = REQUEST_HASH,
) -> KnowledgeMutationReceipt:
    values: dict[str, object] = {
        "operation_id": "operation-1",
        "target": target or _target(),
        "operation_kind": operation_kind,
        "previous_revision": previous_revision,
        "revision": revision,
        "request_hash": request_hash,
        "applied_at": NOW,
        "replayed": replayed,
    }
    if outcome is not None:
        values["outcome"] = outcome
    if reason_code is not None:
        values["reason_code"] = reason_code
    if reason_detail is not None:
        values["reason_detail"] = reason_detail
    if original_outcome is not None:
        values["original_outcome"] = original_outcome
    return KnowledgeMutationReceipt(**values)  # type: ignore[arg-type]


def _entry(
    receipt: KnowledgeMutationReceipt,
    *,
    target: KnowledgeTargetKey | None = None,
    request_hash: str = REQUEST_HASH,
    actor_id: str = "agent-1",
) -> KnowledgeMutationLedgerEntry:
    return KnowledgeMutationLedgerEntry(
        target=target or _target(),
        idempotency_key="idempotency-1",
        request_hash=request_hash,
        operation_kind=receipt.operation_kind,
        actor_id=actor_id,
        receipt=receipt,
        recorded_at=NOW,
    )


def _attempt(
    *,
    outcome: KnowledgeMutationOutcome | str,
    original_operation_id: str | None = None,
    reason_code: str | None = None,
    reason_detail: str | None = None,
) -> KnowledgeMutationAttempt:
    return KnowledgeMutationAttempt(
        attempt_id="attempt-1",
        target=_target(),
        idempotency_key="idempotency-1",
        request_hash=REQUEST_HASH,
        operation_kind=KnowledgeMutationKind.REPLACE,
        actor_id="agent-1",
        outcome=outcome,
        recorded_at=NOW,
        original_operation_id=original_operation_id,
        reason_code=reason_code,
        reason_detail=reason_detail,
    )


def test_applied_is_the_backward_compatible_default_and_advances_revision() -> None:
    receipt = _receipt()

    assert receipt.outcome is KnowledgeMutationOutcome.APPLIED
    assert receipt.previous_revision == 4
    assert receipt.revision == 5
    assert receipt.replayed is False
    assert receipt.original_outcome is None
    assert receipt.to_dict()["outcome"] == "applied"

    with pytest.raises(
        ValueError, match="knowledge_propagation_receipt_revision_invalid"
    ):
        _receipt(revision=4)


def test_grandfathered_is_terminal_and_advances_revision() -> None:
    receipt = _receipt(
        outcome=KnowledgeMutationOutcome.GRANDFATHERED,
        operation_kind=KnowledgeMutationKind.GRANDFATHER,
    )

    assert receipt.outcome is KnowledgeMutationOutcome.GRANDFATHERED
    assert receipt.revision == receipt.previous_revision + 1
    assert receipt.to_dict()["outcome"] == "grandfathered"

    with pytest.raises(
        ValueError, match="knowledge_propagation_receipt_revision_invalid"
    ):
        _receipt(
            outcome="grandfathered",
            operation_kind="grandfather",
            revision=4,
        )


@pytest.mark.parametrize(
    "outcome",
    [KnowledgeMutationOutcome.NOOP, KnowledgeMutationOutcome.REJECTED],
)
def test_noop_and_rejected_keep_the_current_revision(
    outcome: KnowledgeMutationOutcome,
) -> None:
    reason = (
        {
            "reason_code": "selection_rejected",
            "reason_detail": "source could not be resolved",
        }
        if outcome is KnowledgeMutationOutcome.REJECTED
        else {}
    )
    receipt = _receipt(
        outcome=outcome,
        revision=4,
        **reason,
    )

    assert receipt.revision == receipt.previous_revision
    assert receipt.outcome is outcome

    with pytest.raises(
        ValueError, match="knowledge_propagation_receipt_revision_invalid"
    ):
        _receipt(
            outcome=outcome,
            revision=5,
            **reason,
        )


@pytest.mark.parametrize(
    ("reason_code", "reason_detail"),
    [
        (None, None),
        ("selection_rejected", None),
        (None, "source could not be resolved"),
        (" ", "source could not be resolved"),
        ("selection_rejected", " "),
    ],
)
def test_rejected_receipt_requires_stable_code_and_detail(
    reason_code: str | None,
    reason_detail: str | None,
) -> None:
    with pytest.raises(
        ValueError, match="knowledge_propagation_rejection_reason_required"
    ):
        _receipt(
            outcome="rejected",
            revision=4,
            reason_code=reason_code,
            reason_detail=reason_detail,
        )


def test_replayed_requires_terminal_original_outcome_and_true_compatibility_flag() -> (
    None
):
    replay = _receipt(
        outcome="replayed",
        original_outcome="noop",
        revision=4,
        replayed=True,
    )

    assert replay.outcome is KnowledgeMutationOutcome.REPLAYED
    assert replay.original_outcome is KnowledgeMutationOutcome.NOOP
    assert replay.replayed is True

    with pytest.raises(
        ValueError, match="knowledge_propagation_replay_original_outcome_invalid"
    ):
        _receipt(outcome="replayed", revision=4, replayed=True)
    with pytest.raises(
        ValueError, match="knowledge_propagation_replay_original_outcome_invalid"
    ):
        _receipt(
            outcome="replayed",
            original_outcome="replayed",
            revision=4,
            replayed=True,
        )
    with pytest.raises(ValueError, match="knowledge_propagation_replayed_invalid"):
        _receipt(
            outcome="replayed",
            original_outcome="applied",
            replayed=False,
        )


def test_as_replay_preserves_original_terminal_result_and_rejection_reason() -> None:
    original = _receipt(
        outcome="rejected",
        revision=4,
        reason_code="revision_conflict",
        reason_detail="expected revision differs from current revision",
    )

    replay = original.as_replay()

    assert replay.outcome is KnowledgeMutationOutcome.REPLAYED
    assert replay.original_outcome is KnowledgeMutationOutcome.REJECTED
    assert replay.replayed is True
    assert replay.operation_id == original.operation_id
    assert replay.previous_revision == original.previous_revision
    assert replay.revision == original.revision
    assert replay.request_hash == original.request_hash
    assert replay.reason_code == original.reason_code
    assert replay.reason_detail == original.reason_detail
    assert original.outcome is KnowledgeMutationOutcome.REJECTED
    assert original.replayed is False


def test_canonical_entry_rejects_replay_and_validates_actor_target_and_hash() -> None:
    applied = _receipt()
    entry = _entry(applied)

    assert entry.actor_id == "agent-1"
    assert entry.receipt.outcome is KnowledgeMutationOutcome.APPLIED
    assert entry.to_dict()["actor_id"] == "agent-1"

    with pytest.raises(
        ValueError, match="knowledge_propagation_ledger_receipt_incoherent"
    ):
        _entry(applied.as_replay())
    with pytest.raises(ValueError, match="knowledge_propagation_actor_id_invalid"):
        _entry(applied, actor_id=" ")
    with pytest.raises(
        ValueError, match="knowledge_propagation_ledger_receipt_incoherent"
    ):
        _entry(applied, target=_target(target_id="card-foreign"))
    with pytest.raises(
        ValueError, match="knowledge_propagation_ledger_receipt_incoherent"
    ):
        _entry(applied, request_hash="b" * 64)


def test_attempt_accepts_only_replayed_or_rejected() -> None:
    replay = _attempt(
        outcome=KnowledgeMutationOutcome.REPLAYED,
        original_operation_id="operation-1",
    )
    rejected = _attempt(
        outcome=KnowledgeMutationOutcome.REJECTED,
        reason_code="idempotency_conflict",
        reason_detail="key was reused with a divergent request hash",
    )

    assert replay.outcome is KnowledgeMutationOutcome.REPLAYED
    assert rejected.outcome is KnowledgeMutationOutcome.REJECTED
    assert replay.to_dict()["original_operation_id"] == "operation-1"
    assert rejected.to_dict()["reason_code"] == "idempotency_conflict"

    for invalid in (
        KnowledgeMutationOutcome.APPLIED,
        KnowledgeMutationOutcome.NOOP,
        KnowledgeMutationOutcome.GRANDFATHERED,
    ):
        with pytest.raises(
            ValueError, match="knowledge_propagation_attempt_outcome_invalid"
        ):
            _attempt(outcome=invalid)


def test_replay_attempt_references_original_operation() -> None:
    with pytest.raises(
        ValueError,
        match="knowledge_propagation_attempt_original_operation_id_required",
    ):
        _attempt(outcome="replayed")

    with pytest.raises(
        ValueError,
        match="knowledge_propagation_attempt_original_operation_id_invalid",
    ):
        _attempt(
            outcome="replayed",
            original_operation_id="operation-1",
            reason_code="unexpected",
            reason_detail="replay is not a rejection",
        )


@pytest.mark.parametrize(
    ("reason_code", "reason_detail"),
    [
        (None, None),
        ("source_invalid", None),
        (None, "source could not be resolved"),
    ],
)
def test_rejected_attempt_requires_reason(
    reason_code: str | None,
    reason_detail: str | None,
) -> None:
    with pytest.raises(
        ValueError, match="knowledge_propagation_attempt_rejection_reason_required"
    ):
        _attempt(
            outcome="rejected",
            reason_code=reason_code,
            reason_detail=reason_detail,
        )


@pytest.mark.asyncio
async def test_rejection_audit_sink_is_explicitly_post_rollback_and_runtime_bound() -> (
    None
):
    class FakeAuditSink:
        def __init__(self) -> None:
            self.appended: list[KnowledgeMutationAttempt] = []

        async def append_after_rollback(
            self,
            attempt: KnowledgeMutationAttempt,
        ) -> None:
            self.appended.append(attempt)

    reset_knowledge_mutation_audit_sink_for_tests()
    with pytest.raises(
        RuntimeError,
        match="knowledge_mutation_audit_sink_not_configured",
    ):
        get_knowledge_mutation_audit_sink()

    sink = FakeAuditSink()
    register_knowledge_mutation_audit_sink(sink)
    rejected = _attempt(
        outcome="rejected",
        reason_code="selection_rejected",
        reason_detail="domain unit of work was rolled back",
    )
    await get_knowledge_mutation_audit_sink().append_after_rollback(rejected)

    assert sink.appended == [rejected]
