"""Card 4 -- governed queue branching, fencing and acknowledgement semantics."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.ports import consolidation as consolidation_ports
from okto_pulse.core.application.processors.consolidation import (
    ConsolidationProcessor,
    _process_queue_entry,
)
from okto_pulse.core.kg.board_rebuild_adapter import (
    DETERMINISTIC_SOURCE_ARTIFACT_TYPES,
)
from okto_pulse.core.ports.consolidation import (
    ConsolidationClaimScope,
    ConsolidationProjectionInputs,
    ConsolidationQueueRecord,
    ExactConsolidationAckReceipt,
    ExactConsolidationAckIntegrityError,
    ExactConsolidationCompensationReceipt,
    ExactConsolidationCompensationError,
    ExactConsolidationCompensationResult,
    ExactConsolidationDisposition,
    ExactConsolidationMutationState,
    ExactConsolidationResultOrigin,
    exact_consolidation_ack_receipts_sha256,
    get_consolidation_persistence_port,
    register_consolidation_persistence_port,
)


def _entry(
    *,
    entry_id: str = "card4-entry",
    work_kind: str = "consolidate",
    generation: int = 0,
    delete_event_id: str | None = None,
    payload: dict[str, Any] | None = None,
    status: str = "pending",
    claim_token: str | None = None,
    board_id: str = "card4-board",
    artifact_id: str = "card4-spec",
    source: str = "state_transition",
) -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id=entry_id,
        board_id=board_id,
        artifact_type="spec",
        artifact_id=artifact_id,
        status=status,
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=(datetime.now(timezone.utc) if status == "claimed" else None),
        claim_timeout_at=None,
        worker_id=("old-worker" if status == "claimed" else None),
        claimed_by_session_id=("old-worker" if status == "claimed" else None),
        triggered_at=datetime.now(timezone.utc),
        priority="high",
        work_kind=work_kind,
        generation=generation,
        payload=payload,
        delete_event_id=delete_event_id,
        claim_token=claim_token,
        source=source,
    )


def _valid_reconcile_entry(**overrides: Any) -> ConsolidationQueueRecord:
    artifact_id = str(overrides.get("artifact_id", "deleted-spec"))
    delete_event_id = str(overrides.get("delete_event_id", "delete-event-g1"))
    values: dict[str, Any] = {
        "entry_id": "reconcile-g1",
        "work_kind": "stale_reconcile",
        "generation": 1,
        "delete_event_id": delete_event_id,
        "payload": {
            "schema_version": 1,
            "delete_event_id": delete_event_id,
            "source_refs": [f"spec:{artifact_id}"],
        },
        "artifact_id": artifact_id,
    }
    values.update(overrides)
    return _entry(**values)


def _exact_rebuild_entry(**overrides: Any) -> ConsolidationQueueRecord:
    source = str(overrides.get("source", "rebuild:run-card4-exact"))
    artifact_id = str(overrides.get("artifact_id", "card4-spec"))
    payload = {
        "_rebuild_membership": {
            "content_hash": "a" * 64,
            "run_id": source.removeprefix("rebuild:"),
            "source_ref": f"spec:{artifact_id}",
            "source_version": "7",
        }
    }
    values: dict[str, Any] = {
        "entry_id": "card4-exact-entry",
        "artifact_id": artifact_id,
        "source": source,
        "payload": payload,
    }
    values.update(overrides)
    return _entry(**values)


def test_queue_record_preserves_original_positional_work_kind_abi() -> None:
    now = datetime.now(timezone.utc)
    record = ConsolidationQueueRecord(
        "entry-positional",
        "board-positional",
        "spec",
        "spec-positional",
        "pending",
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        now,
        "high",
        "stale_sweep",
    )

    assert record.work_kind == "stale_sweep"
    assert record.source == "state_transition"


def test_exact_row_result_rejects_unemittable_public_state_combinations() -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-dto-matrix")
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="b" * 64,
    )
    source_ref, source_version, content_hash = consolidation._exact_rebuild_membership(
        entry, scope
    )
    ack_receipt = ExactConsolidationAckReceipt.create(
        queue_id=entry.id,
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="b" * 64,
        work_kind=entry.work_kind,
        artifact_type=entry.artifact_type,
        artifact_id=entry.artifact_id,
        generation=entry.generation,
        membership_source_ref=source_ref,
        membership_source_version=source_version,
        membership_content_hash=content_hash,
        audit_content_hash="f" * 64,
        consolidation_session_id="session-dto",
        outbox_event_id="outbox-dto",
        generation_event_id="generation-dto",
        previous_materialization_generation="mg-before",
        materialization_generation="mg-after",
        node_ref_count=0,
        node_refs_sha256="0" * 64,
    )
    ack = consolidation._exact_row_result(
        entry,
        scope,
        attempt_ordinal=1,
        disposition=ExactConsolidationDisposition.ACKED,
        origin=ExactConsolidationResultOrigin.NEW,
        mutation_state=ExactConsolidationMutationState.COMMITTED,
        ack_receipt=ack_receipt,
    )
    assert ack_receipt.membership_content_hash != ack_receipt.audit_content_hash
    assert ack_receipt.to_payload()["schema"] == "exact_consolidation_ack_receipt.v2"
    assert ack_receipt.to_payload()["audit_content_hash"] == "f" * 64

    invalid_variants = (
        {"origin": ExactConsolidationResultOrigin.REPLAYED},
        {"mutation_state": ExactConsolidationMutationState.AMBIGUOUS},
        {"error_code": "", "error_message": ""},
        {
            "disposition": ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS,
            "origin": ExactConsolidationResultOrigin.REPLAYED,
            "mutation_state": ExactConsolidationMutationState.UNCHANGED,
            "error_code": "fence_lost",
            "error_message": "fence lost",
        },
        {
            "disposition": ExactConsolidationDisposition.TERMINAL_FAILURE,
            "mutation_state": ExactConsolidationMutationState.COMMITTED,
            "error_code": "terminal",
            "error_message": "terminal",
        },
        {
            "disposition": ExactConsolidationDisposition.TERMINAL_FAILURE,
            "mutation_state": ExactConsolidationMutationState.UNCHANGED,
            "error_code": "terminal",
            "error_message": "terminal",
            "diagnostic_json": '{"z":1,"a":2}',
        },
        {
            "artifact_type": "foo",
            "membership_source_ref": f"foo:{entry.artifact_id}",
        },
    )
    for changes in invalid_variants:
        with pytest.raises((TypeError, ValueError)):
            replace(ack, **changes)


def test_exact_ack_v2_binds_separate_membership_and_audit_hash_domains() -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-v2")
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="b" * 64,
    )
    source_ref, source_version, membership_hash = (
        consolidation._exact_rebuild_membership(entry, scope)
    )
    receipt = ExactConsolidationAckReceipt.create(
        queue_id=entry.id,
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="b" * 64,
        work_kind=entry.work_kind,
        artifact_type=entry.artifact_type,
        artifact_id=entry.artifact_id,
        generation=entry.generation,
        membership_source_ref=source_ref,
        membership_source_version=source_version,
        membership_content_hash=membership_hash,
        audit_content_hash="e" * 64,
        consolidation_session_id="session-v2",
        outbox_event_id="outbox-v2",
        generation_event_id="generation-v2",
        previous_materialization_generation="mg-before",
        materialization_generation="mg-after",
        node_ref_count=0,
        node_refs_sha256="0" * 64,
    )

    payload = receipt.to_payload()
    assert receipt.membership_content_hash != receipt.audit_content_hash
    assert ExactConsolidationAckReceipt.from_payload(payload) == receipt
    alternate_values = {
        name: value
        for name, value in payload.items()
        if name not in {"schema", "receipt_sha256"}
    }
    alternate_values["audit_content_hash"] = "d" * 64
    alternate = ExactConsolidationAckReceipt.create(**alternate_values)
    assert exact_consolidation_ack_receipts_sha256((alternate,)) != (
        exact_consolidation_ack_receipts_sha256((receipt,))
    )
    for field in ("membership_content_hash", "audit_content_hash"):
        with pytest.raises(
            ValueError, match="exact_consolidation_ack_receipt_digest_mismatch"
        ):
            replace(receipt, **{field: "d" * 64})

    legacy_payload = dict(payload)
    legacy_payload["schema"] = "exact_consolidation_ack_receipt.v1"
    legacy_payload.pop("audit_content_hash")
    with pytest.raises(
        ValueError, match="exact_consolidation_ack_receipt_payload_invalid"
    ):
        ExactConsolidationAckReceipt.from_payload(legacy_payload)
    with pytest.raises(ValueError, match="exact_consolidation_ack_receipt_invalid"):
        replace(receipt, audit_content_hash="not-a-sha256")


def test_exact_ack_integrity_error_has_closed_public_codes() -> None:
    error = ExactConsolidationAckIntegrityError("exact_consolidation_ack_audit_invalid")
    assert error.code == "exact_consolidation_ack_audit_invalid"
    assert str(error) == error.code
    assert "ExactConsolidationAckIntegrityError" in consolidation_ports.__all__
    with pytest.raises(
        ValueError, match="exact_consolidation_ack_integrity_code_invalid"
    ):
        ExactConsolidationAckIntegrityError("arbitrary_adapter_runtime_error")


def test_exact_post_commit_error_is_part_of_public_port_contract() -> None:
    assert "ExactConsolidationPostCommitError" in consolidation_ports.__all__
    assert "ExactConsolidationAckReceipt" in consolidation_ports.__all__
    assert "ExactConsolidationCompensationError" in consolidation_ports.__all__
    assert (
        "build_exact_consolidation_compensation_binding" in consolidation_ports.__all__
    )
    assert consolidation_ports._EXACT_REBUILD_SOURCE_ARTIFACT_TYPES == (
        DETERMINISTIC_SOURCE_ARTIFACT_TYPES
    )


@pytest.mark.parametrize("source_artifact_type", ("task", "test", "bug", "card"))
@pytest.mark.asyncio
async def test_exact_card_queue_preserves_canonical_membership_source_alias(
    monkeypatch,
    source_artifact_type: str,
) -> None:
    entry = _exact_rebuild_entry(
        entry_id=f"card4-exact-{source_artifact_type}-membership",
    )
    entry.artifact_type = "card"
    assert entry.payload is not None
    entry.payload["_rebuild_membership"]["source_ref"] = (
        f"{source_artifact_type}:{entry.artifact_id}"
    )
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="c" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert len(result.terminal_failures) == 1
    assert result.terminal_failures[0].artifact_type == "card"
    assert result.terminal_failures[0].membership_source_ref == (
        f"{source_artifact_type}:{entry.artifact_id}"
    )


@pytest.mark.parametrize(
    "queue_artifact_type,source_ref",
    (
        ("card", "spec:card4-spec"),
        ("card", "task:different-card"),
        ("card", "task:card4-spec:forged"),
        ("spec", "task:card4-spec"),
    ),
    ids=("wrong-prefix", "wrong-id", "extra-separator", "wrong-queue-type"),
)
def test_exact_membership_rejects_queue_alias_and_identity_tampering(
    queue_artifact_type: str,
    source_ref: str,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-membership-tamper")
    entry.artifact_type = queue_artifact_type
    assert entry.payload is not None
    entry.payload["_rebuild_membership"]["source_ref"] = source_ref
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="d" * 64,
    )

    with pytest.raises(RuntimeError, match="exact_rebuild_membership_invalid"):
        consolidation._exact_rebuild_membership(entry, scope)


class _MemoryConsolidationStore:
    def __init__(
        self,
        entries: tuple[ConsolidationQueueRecord, ...] = (),
        *,
        fence_result: bool = True,
        ack_result: bool = True,
        reservation_sources: tuple[str | None, ...] = (),
    ) -> None:
        self.entries = {entry.id: entry for entry in entries}
        self.fence_result = fence_result
        self.ack_result = ack_result
        self.fence_calls: list[dict[str, Any]] = []
        self.ack_calls: list[dict[str, Any]] = []
        self.load_calls: list[tuple[str, str]] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.reservation_sources = list(reservation_sources)
        self.current_reservation_source: str | None = None
        self.repend_calls: list[dict[str, Any]] = []
        self.exact_disposition_calls: list[dict[str, Any]] = []
        self.exact_ack_receipts: list[ExactConsolidationAckReceipt] = []
        self.materialization_head = "mg-card4-baseline"
        self.exact_compensation_receipt: (
            ExactConsolidationCompensationReceipt | None
        ) = None

    async def load_artifact(self, _context, *, artifact_type, artifact_id):
        self.load_calls.append((artifact_type, artifact_id))
        return SimpleNamespace(title="Legacy spec")

    async def load_projection_inputs(self, _context, **_identity):
        return ConsolidationProjectionInputs()

    async def count_pending(self, _context) -> int:
        return sum(entry.status == "pending" for entry in self.entries.values())

    async def list_claimed_board_ids(self, _context) -> frozenset[str]:
        return frozenset(
            entry.board_id
            for entry in self.entries.values()
            if entry.status == "claimed"
        )

    async def list_ready_pending(self, _context, *, now):
        del now
        return tuple(
            entry for entry in self.entries.values() if entry.status == "pending"
        )

    async def list_ready_pending_exact(
        self,
        _context,
        *,
        now,
        board_id,
        source,
        work_kind,
    ):
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "pending"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
            and (entry.next_retry_at is None or entry.next_retry_at <= now)
        )

    async def list_pending_exact(
        self,
        _context,
        *,
        board_id,
        source,
        work_kind,
    ):
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "pending"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
        )

    async def list_claimed_exact(
        self,
        _context,
        *,
        board_id,
        source,
        work_kind,
    ):
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "claimed"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
        )

    async def claim_ready_pending_exact(
        self,
        _context,
        *,
        entry_id,
        board_id,
        source,
        work_kind,
        generation,
        now,
        claim_timeout_at,
        worker_id,
        claim_token,
    ):
        entry = self.entries.get(entry_id)
        if not (
            entry is not None
            and entry.status == "pending"
            and entry.board_id == board_id
            and entry.source == source
            and entry.work_kind == work_kind
            and entry.generation == generation
        ):
            return None
        entry.status = "claimed"
        entry.claimed_at = now
        entry.claim_timeout_at = claim_timeout_at
        entry.worker_id = worker_id
        entry.claimed_by_session_id = worker_id
        entry.claim_token = claim_token
        return entry

    async def board_administrative_rebuild_source(self, _context, *, board_id):
        del board_id
        if self.reservation_sources:
            self.current_reservation_source = self.reservation_sources.pop(0)
        return self.current_reservation_source

    async def list_stale_claims(self, _context, *, now, legacy_cutoff):
        del legacy_cutoff
        return tuple(
            entry
            for entry in self.entries.values()
            if entry.status == "claimed"
            and entry.claim_timeout_at is not None
            and entry.claim_timeout_at < now
        )

    async def save_queue_entries(self, _context, entries) -> None:
        for entry in entries:
            self.entries[entry.id] = entry

    async def get_queue_entry(self, _context, *, entry_id):
        return self.entries.get(entry_id)

    async def queue_claim_is_current_and_unfenced(self, _context, **identity):
        self.fence_calls.append(identity)
        return self.fence_result

    async def ack_claimed_queue_entry(self, _context, **identity):
        self.ack_calls.append(identity)
        if self.ack_result:
            self.entries.pop(str(identity["entry_id"]), None)
        return self.ack_result

    async def ack_exact_rebuild_commit(self, _context, **identity):
        self.ack_calls.append(identity)
        entry = self.entries.get(str(identity["entry_id"]))
        authority_probe = identity["reservation_authority_probe"]
        if not (
            self.ack_result
            and entry is not None
            and entry.status == "claimed"
            and entry.claim_token == identity["claim_token"]
            and entry.board_id == identity["board_id"]
            and entry.artifact_type == identity["artifact_type"]
            and entry.artifact_id == identity["artifact_id"]
            and entry.source == identity["source"]
            and entry.work_kind == identity["work_kind"]
            and entry.generation == identity["generation"]
            and entry.delete_event_id == identity["delete_event_id"]
            and entry.attempts == identity["expected_attempts"]
            and entry.last_error == identity["expected_last_error"]
            and entry.next_retry_at == identity["expected_next_retry_at"]
            and entry.payload == identity["expected_payload"]
            and callable(authority_probe)
            and authority_probe() is True
        ):
            return None
        receipt = ExactConsolidationAckReceipt.create(
            queue_id=entry.id,
            board_id=entry.board_id,
            source=entry.source,
            reservation_lineage_id=identity["reservation_lineage_id"],
            work_kind=entry.work_kind,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            generation=entry.generation,
            membership_source_ref=identity["membership_source_ref"],
            membership_source_version=identity["membership_source_version"],
            membership_content_hash=identity["membership_content_hash"],
            audit_content_hash="c" * 64,
            consolidation_session_id=identity["consolidation_session_id"],
            outbox_event_id=f"outbox-{entry.id}",
            generation_event_id=f"generation-{entry.id}",
            previous_materialization_generation=self.materialization_head,
            materialization_generation=f"mg-{entry.id}",
            node_ref_count=0,
            node_refs_sha256="0" * 64,
        )
        self.materialization_head = receipt.materialization_generation
        self.exact_ack_receipts.append(receipt)
        self.entries.pop(entry.id, None)
        return receipt

    async def list_exact_rebuild_ack_receipts(self, _context, **identity):
        return tuple(
            receipt
            for receipt in self.exact_ack_receipts
            if receipt.board_id == identity["board_id"]
            and receipt.source == identity["source"]
            and receipt.reservation_lineage_id == identity["reservation_lineage_id"]
        )

    async def compensate_exact_rebuild_commits(self, _context, **identity):
        receipts = identity["expected_receipts"]
        authority_probe = identity["reservation_authority_probe"]
        if not callable(authority_probe) or authority_probe() is not True:
            return None
        if self.exact_compensation_receipt is not None:
            return ExactConsolidationCompensationResult(
                receipt=self.exact_compensation_receipt,
                replayed=True,
            )
        terminal = receipts[-1]
        compensation_receipt = ExactConsolidationCompensationReceipt.create(
            board_id=identity["board_id"],
            source=identity["source"],
            reservation_lineage_id=identity["reservation_lineage_id"],
            baseline_materialization_generation=(
                receipts[0].previous_materialization_generation
            ),
            terminal_materialization_generation=(terminal.materialization_generation),
            ack_count=len(receipts),
            node_ref_count=sum(receipt.node_ref_count for receipt in receipts),
            ack_receipts_sha256=exact_consolidation_ack_receipts_sha256(receipts),
            audit_session_ids=tuple(
                receipt.consolidation_session_id for receipt in receipts
            ),
            outbox_event_ids=tuple(receipt.outbox_event_id for receipt in receipts),
            generation_event_ids=tuple(
                receipt.generation_event_id for receipt in receipts
            ),
            compensation_id="comp-card4",
            compensated_at=datetime.now(timezone.utc),
        )
        self.exact_compensation_receipt = compensation_receipt
        self.materialization_head = receipts[0].previous_materialization_generation
        return ExactConsolidationCompensationResult(
            receipt=compensation_receipt,
            replayed=False,
        )

    async def save_exact_rebuild_disposition(
        self,
        _context,
        **identity,
    ):
        self.exact_disposition_calls.append(identity)
        entry = self.entries.get(str(identity["entry_id"]))
        authority_probe = identity["reservation_authority_probe"]
        authority_valid = authority_probe() if callable(authority_probe) else False
        if not (
            entry is not None
            and entry.status == "claimed"
            and entry.claim_token == identity["claim_token"]
            and entry.board_id == identity["board_id"]
            and entry.artifact_type == identity["artifact_type"]
            and entry.artifact_id == identity["artifact_id"]
            and entry.source == identity["source"]
            and entry.work_kind == identity["work_kind"]
            and entry.generation == identity["generation"]
            and entry.delete_event_id == identity["delete_event_id"]
            and entry.attempts == identity["expected_attempts"]
            and entry.last_error == identity["expected_last_error"]
            and entry.next_retry_at == identity["expected_next_retry_at"]
            and entry.payload == identity["expected_payload"]
            and self.current_reservation_source == identity["source"]
            and type(authority_valid) is bool
            and authority_valid
        ):
            return None
        entry.payload = identity["payload"]
        entry.attempts = identity["attempts"]
        entry.last_error = identity["last_error"]
        entry.next_retry_at = identity["next_retry_at"]
        entry.status = "pending"
        entry.claimed_at = None
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_by_session_id = None
        entry.claim_token = None
        return entry

    async def repend_claimed_queue_entry(self, _context, **identity):
        self.repend_calls.append(identity)
        entry = self.entries.get(str(identity["entry_id"]))
        if not (
            entry is not None
            and entry.status == "claimed"
            and entry.claim_token == identity["claim_token"]
            and entry.board_id == identity["board_id"]
            and entry.source == identity["source"]
            and entry.work_kind == identity["work_kind"]
            and entry.generation == identity["generation"]
            and entry.delete_event_id == identity["delete_event_id"]
        ):
            return False
        entry.status = "pending"
        entry.claimed_at = None
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_by_session_id = None
        entry.claim_token = None
        return True

    async def delete_queue_entry(self, _context, *, entry_id):
        raise AssertionError(f"legacy non-CAS ACK used for {entry_id}")

    async def commit(self, _context) -> None:
        self.commit_count += 1

    async def rollback(self, _context) -> None:
        self.rollback_count += 1


@asynccontextmanager
async def _scope():
    yield object()


@contextmanager
def _registered(store: _MemoryConsolidationStore):
    previous = get_consolidation_persistence_port()
    register_consolidation_persistence_port(store)
    try:
        yield
    finally:
        register_consolidation_persistence_port(previous)


async def _process_exact(
    processor: ConsolidationProcessor,
    scope: ConsolidationClaimScope,
    *,
    reservation_authority_probe=None,
):
    if reservation_authority_probe is None:
        reservation_authority_probe = _always_authorized
    if scope.reservation_lineage_id is None:
        scope = ConsolidationClaimScope(
            board_id=scope.board_id,
            source=scope.source,
            work_kind=scope.work_kind,
            reservation_lineage_id="f" * 64,
        )
    return await processor.process_exact_batch(
        claim_scope=scope,
        reservation_authority_probe=reservation_authority_probe,
    )


def _always_authorized() -> bool:
    return True


def _patch_graph_write_shell(monkeypatch, *, lifecycle_calls: list[str]) -> None:
    monkeypatch.setattr(
        consolidation,
        "guarded_board_write",
        lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(
                durability_applied=True,
                ensure_owned=lambda **_kwargs: None,
            )
        ),
    )

    def _lifecycle(**_kwargs):
        lifecycle_calls.append("lifecycle")
        return SimpleNamespace()

    monkeypatch.setattr(
        consolidation,
        "_apply_board_graph_lifecycle_after_commit",
        _lifecycle,
    )


@pytest.mark.asyncio
async def test_stale_reconcile_branches_before_artifact_load(monkeypatch):
    """The governed-delete lane reconciles by source ref, never by deleted row."""

    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry(status="claimed", claim_token="claim-g1")
    store = _MemoryConsolidationStore((entry,))
    reconciliations: list[dict[str, Any]] = []
    lifecycle_calls: list[str] = []

    class _BlockingExecution:
        async def run(self, operation):
            return operation()

        async def join(self, _timeout: float) -> int:
            return 0

    blocking_execution = _BlockingExecution()

    async def _reconcile(_db, **kwargs):
        kwargs["before_graph_write"]()
        reconciliations.append(kwargs)
        kwargs.pop("before_graph_write")
        return SimpleNamespace(
            incomplete=False,
            failed_types=(),
            **_complete_target_contract(),
        )

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _reconcile,
    )
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    with _registered(store):
        assert await _process_queue_entry(
            object(),
            entry,
            blocking_execution=blocking_execution,
        )

    assert store.load_calls == []
    assert reconciliations == [
        {
            "board_id": entry.board_id,
            "source_refs": [f"spec:{entry.artifact_id}"],
            "correlation_id": entry.delete_event_id,
            "blocking_execution": blocking_execution,
        }
    ]
    assert lifecycle_calls == ["lifecycle"]


@pytest.mark.asyncio
async def test_invalid_stale_payload_has_no_graph_write_or_ack(monkeypatch):
    entry = _valid_reconcile_entry(
        payload={
            "schema_version": 1,
            "delete_event_id": "delete-event-g1",
            "source_refs": ["spec:some-other-artifact"],
        }
    )
    store = _MemoryConsolidationStore((entry,))
    lifecycle_calls: list[str] = []
    failure_calls: list[str] = []
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)
        failed_entry.status = "pending"

    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert store.load_calls == []
    # The only fence read is the batch's ownership check before recording the
    # operational failure. The malformed intent never reaches graph code.
    assert len(store.fence_calls) == 1
    assert lifecycle_calls == []
    assert store.ack_calls == []
    assert failure_calls == [entry.id]


def _complete_target_contract() -> dict[str, int]:
    return {
        "target_identity_count": 1,
        "target_found_count": 0,
        "target_demoted_count": 0,
        "target_already_converged_count": 0,
        "target_skipped_cognitive_count": 0,
        "target_preserved_canonical_count": 0,
    }


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (SimpleNamespace(), False),
        ({}, False),
        (
            SimpleNamespace(
                incomplete=False,
                failed_types=(),
                **_complete_target_contract(),
            ),
            True,
        ),
        (
            {
                "incomplete": False,
                "failed_types": [],
                **_complete_target_contract(),
            },
            True,
        ),
        (SimpleNamespace(incomplete=True, failed_types=()), False),
        ({"incomplete": False, "failed_types": ["Decision"]}, False),
    ],
)
def test_stale_reconcile_completeness_is_explicit_and_fail_closed(result, expected):
    assert consolidation._stale_reconcile_is_complete(result) is expected


def test_stale_reconcile_empty_retry_cannot_ack_prior_partial_graph_failure():
    result = SimpleNamespace(
        incomplete=False,
        failed_types=(),
        **_complete_target_contract(),
    )

    assert (
        consolidation._stale_reconcile_is_complete(
            result,
            previous_error="stale_reconcile_graph_partial:Decision",
        )
        is False
    )
    assert (
        consolidation._stale_reconcile_failure_error(
            existing_error=None,
            reconcile_details={"failed_types": ["Requirement", "Decision"]},
        )
        == "stale_reconcile_graph_partial:Decision,Requirement"
    )


@pytest.mark.asyncio
async def test_incomplete_stale_reconcile_is_not_acknowledged(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry()
    store = _MemoryConsolidationStore((entry,))
    lifecycle_calls: list[str] = []
    failure_calls: list[str] = []

    async def _incomplete(_db, **_kwargs):
        _kwargs["before_graph_write"]()
        return SimpleNamespace(incomplete=True, failed_types=("Decision",))

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)
        failed_entry.status = "pending"

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _incomplete,
    )
    _patch_graph_write_shell(monkeypatch, lifecycle_calls=lifecycle_calls)
    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert lifecycle_calls == ["lifecycle"]
    assert failure_calls == [entry.id]
    assert store.ack_calls == []
    assert entry.id in store.entries


@pytest.mark.asyncio
async def test_lost_claim_is_neutral_before_stale_reconcile_write(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry()
    store = _MemoryConsolidationStore((entry,), fence_result=False)
    failure_calls: list[str] = []

    async def _must_not_reconcile(*_args, **_kwargs):
        raise AssertionError("stale reconciliation ran after the claim was lost")

    async def _mark_failed(_db, failed_entry, **_kwargs):
        failure_calls.append(failed_entry.id)

    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _must_not_reconcile,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)
    monkeypatch.setattr(processor, "_mark_failed", _mark_failed)

    with _registered(store):
        assert await processor.process_batch() == 0

    assert len(store.fence_calls) == 1
    assert store.ack_calls == []
    assert failure_calls == []
    assert entry.status == "pending"
    assert entry.claim_token is None
    assert entry.attempts == 0


@pytest.mark.asyncio
async def test_stale_reconcile_claim_cas_runs_after_graph_writer(monkeypatch):
    from okto_pulse.core.kg import canonical_stale_reconciler

    entry = _valid_reconcile_entry(status="claimed", claim_token="claim-order")
    store = _MemoryConsolidationStore((entry,))
    events: list[str] = []

    async def _claim_cas(*_args, **_kwargs):
        events.append("claim-cas")
        return True

    async def _reconcile(_db, **kwargs):
        events.append("reconcile")
        kwargs["before_graph_write"]()
        return SimpleNamespace(
            incomplete=False,
            failed_types=(),
            **_complete_target_contract(),
        )

    async def _durable(**_kwargs):
        events.append("durable")

    lease = SimpleNamespace(
        ensure_owned=lambda **_kwargs: events.append("lease-check"),
        durability_applied=True,
    )

    def _enter(_mutation_ref):
        events.append("graph-writer")
        return lease

    monkeypatch.setattr(store, "queue_claim_is_current_and_unfenced", _claim_cas)
    monkeypatch.setattr(
        canonical_stale_reconciler,
        "reconcile_stale_canonical",
        _reconcile,
    )
    monkeypatch.setattr(
        consolidation,
        "_ensure_board_graph_durable",
        _durable,
    )

    with _registered(store):
        assert await consolidation._process_stale_reconcile_entry(
            object(),
            entry,
            enter_graph_write=_enter,
        )

    assert events == [
        "graph-writer",
        "claim-cas",
        "reconcile",
        "lease-check",
        "durable",
    ]


@pytest.mark.asyncio
async def test_delete_between_extraction_and_publish_blocks_legacy_commit(monkeypatch):
    """AC6/TS3: a tombstone winning at the final re-check publishes nothing."""

    entry = _entry(status="claimed", claim_token="claim-before-delete")
    store = _MemoryConsolidationStore((entry,), fence_result=False)
    observed: list[str] = []

    worker_result = SimpleNamespace(
        nodes=[object()],
        edges=[],
        missing_link_candidates=[],
        raw_content="legacy spec body",
        relational_projection_candidate_ids=(),
        relational_projection_active_set_intent=None,
    )

    def _extract(*_args, **_kwargs):
        observed.append("extract")
        return worker_result

    async def _passthrough(_db, _entry, _artifact, result):
        return result

    async def _resolve(_db, _board_id, result):
        return result

    async def _begin(*_args, **_kwargs):
        return SimpleNamespace(session_id="uncommitted-session")

    async def _propose(*_args, **_kwargs):
        return SimpleNamespace()

    async def _abort(**_kwargs):
        observed.append("abort")

    @contextmanager
    def _writer(*_args, **_kwargs):
        observed.append("graph-writer")
        yield SimpleNamespace()

    async def _claim_cas(*_args, **_kwargs):
        observed.append("claim-cas")
        return False

    monkeypatch.setattr(consolidation, "_run_deterministic_worker", _extract)
    monkeypatch.setattr(
        consolidation,
        "_materialize_lineage_endpoint_nodes",
        _passthrough,
    )
    monkeypatch.setattr(consolidation, "_resolve_missing_link_candidates", _resolve)
    monkeypatch.setattr(
        consolidation,
        "_worker_node_to_candidate",
        lambda _node: {
            "candidate_id": "candidate-before-delete",
            "node_type": "Requirement",
            "title": "Candidate before governed delete",
        },
    )
    monkeypatch.setattr(consolidation, "begin_consolidation", _begin)
    monkeypatch.setattr(consolidation, "propose_reconciliation", _propose)
    monkeypatch.setattr(
        consolidation,
        "_abort_open_consolidation_after_fence",
        _abort,
    )
    monkeypatch.setattr(consolidation, "guarded_board_write", _writer)
    monkeypatch.setattr(
        consolidation,
        "_queue_claim_is_current_and_unfenced",
        _claim_cas,
    )

    with _registered(store):
        with pytest.raises(consolidation._QueueClaimLostOrFenced):
            await _process_queue_entry(object(), entry)

    assert store.load_calls == [("spec", entry.artifact_id)]
    assert observed == ["extract", "graph-writer", "claim-cas", "abort"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ack_result", [False, True])
async def test_legacy_processed_count_requires_ack_cas_rowcount_one(
    monkeypatch,
    ack_result,
):
    entry = _entry(
        entry_id=f"ack-{ack_result}",
        work_kind="consolidate",
        generation=0,
        delete_event_id=None,
        payload=None,
    )
    store = _MemoryConsolidationStore((entry,), ack_result=ack_result)

    async def _success(*_args, **_kwargs):
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        processed = await processor.process_batch()

    assert processed == int(ack_result)
    assert len(store.ack_calls) == 1
    ack = store.ack_calls[0]
    assert ack == {
        "entry_id": entry.id,
        "claim_token": entry.claim_token,
        "board_id": entry.board_id,
        "source": entry.source,
        "work_kind": entry.work_kind,
        "generation": 0,
        "delete_event_id": None,
    }
    assert (entry.id not in store.entries) is ack_result


@pytest.mark.asyncio
async def test_live_claim_is_neutrally_repended_when_rebuild_reserves_before_step2(
    monkeypatch,
) -> None:
    rebuild_source = "rebuild:manifest-card4"
    live = _entry(entry_id="live-preclaimed-race")
    rebuild = _entry(
        entry_id="exact-rebuild-row",
        artifact_id="rebuild-spec",
        source=rebuild_source,
    )
    store = _MemoryConsolidationStore(
        (live, rebuild),
        reservation_sources=(
            None,  # Step 1 lists/claims the ordinary row.
            rebuild_source,  # Reservation appears before Step 2.
            rebuild_source,
            rebuild_source,
            rebuild_source,
        ),
    )

    async def _success(*_args, **_kwargs):
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        assert await processor.process_batch() == 0
        assert live.status == "pending"
        assert live.claim_token is None
        assert live.source == "state_transition"
        assert rebuild.status == "pending"

        assert await processor.process_batch() == 1

    assert rebuild.id not in store.entries
    assert live.id in store.entries
    assert live.status == "pending"
    assert len(store.repend_calls) == 1


def test_under_writer_reservation_probe_admits_only_exact_rebuild_source(
    monkeypatch,
) -> None:
    expires = datetime.now(timezone.utc).timestamp() + 60

    class _Reservation:
        def bind_write_lock_port(self):
            return object()

        def inspect(self, *, board_id):
            assert board_id == "card4-board"
            return SimpleNamespace(
                operation="kg02_rebuild_reservation:manifest-card4",
                expires_at_epoch=expires,
            )

    monkeypatch.setattr(
        consolidation,
        "KGAdministrativeOperationReservation",
        _Reservation,
    )

    exact = _entry(source="rebuild:manifest-card4")
    consolidation._ensure_entry_admitted_by_reservation_under_writer(exact)

    with pytest.raises(
        consolidation._QueueClaimLostOrFenced,
        match="rebuild_reservation_scope_mismatch_under_writer",
    ):
        consolidation._ensure_entry_admitted_by_reservation_under_writer(_entry())


@pytest.mark.asyncio
async def test_recovery_clears_token_and_reclaim_uses_fresh_token(monkeypatch):
    old_token = "token-from-crashed-worker"
    entry = _entry(
        status="claimed",
        claim_token=old_token,
    )
    entry.claim_timeout_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    store = _MemoryConsolidationStore((entry,))
    observed_tokens: list[str | None] = []

    async def _lose_after_reclaim(_db, claimed_entry, **_kwargs):
        observed_tokens.append(claimed_entry.claim_token)
        raise consolidation._QueueClaimLostOrFenced("simulated takeover")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _lose_after_reclaim,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        assert await processor.recover_stale_claims() == 1
        assert entry.claim_token is None
        assert await processor.process_batch() == 0

    assert len(observed_tokens) == 1
    assert observed_tokens[0]
    assert observed_tokens[0] != old_token
    assert store.ack_calls == []
    assert entry.attempts == 0


@pytest.mark.asyncio
async def test_exact_recovery_repends_killed_claim_and_replays_logical_commit_once(
    monkeypatch,
) -> None:
    source = "rebuild:manifest-crash-recovery"
    entry = _entry(
        entry_id="claimed-before-process-kill",
        status="claimed",
        claim_token="dead-process-token",
        source=source,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="6" * 64,
    )
    logical_graph_commits = {f"spec:{entry.artifact_id}"}

    async def _idempotent_replay(_db, claimed_entry, **_kwargs):
        logical_graph_commits.add(f"spec:{claimed_entry.artifact_id}")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _idempotent_replay,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        recovered = await processor.recover_exact_claims(
            claim_scope=scope,
            recovery_authority_probe=lambda: True,
        )
        assert recovered == 1
        assert entry.status == "pending"
        assert entry.claim_token is None
        assert entry.attempts == 0

        assert await processor.process_batch(claim_scope=scope) == 1

    assert entry.id not in store.entries
    assert logical_graph_commits == {f"spec:{entry.artifact_id}"}
    assert len(store.repend_calls) == 1
    assert store.repend_calls[0]["claim_token"] == "dead-process-token"


@pytest.mark.asyncio
@pytest.mark.parametrize("authority,reservation", [(False, "exact"), (True, None)])
async def test_exact_recovery_requires_offline_authority_and_matching_reservation(
    authority: bool,
    reservation: str | None,
) -> None:
    source = "rebuild:manifest-crash-recovery"
    entry = _entry(
        entry_id="claimed-fenced",
        status="claimed",
        claim_token="still-owned",
        source=source,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source if reservation == "exact" else reservation
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store), pytest.raises(RuntimeError):
        await processor.recover_exact_claims(
            claim_scope=ConsolidationClaimScope(
                board_id=entry.board_id,
                source=source,
                reservation_lineage_id="7" * 64,
            ),
            recovery_authority_probe=lambda: authority,
        )

    assert entry.status == "claimed"
    assert entry.claim_token == "still-owned"
    assert store.repend_calls == []


@pytest.mark.asyncio
async def test_stale_sweep_is_claimable_with_legacy_consolidate(monkeypatch):
    legacy = _entry(entry_id="legacy-consolidate", board_id="legacy-board")
    sweep = _entry(
        entry_id="card8-sweep",
        work_kind="stale_sweep",
        board_id="sweep-board",
        artifact_id="board-sweep",
    )
    store = _MemoryConsolidationStore((legacy, sweep))
    processed_ids: list[str] = []

    async def _success(_db, claimed_entry, **_kwargs):
        processed_ids.append(claimed_entry.id)
        return True

    monkeypatch.setattr(consolidation, "_process_queue_entry_serialized", _success)
    processor = ConsolidationProcessor(_scope, batch_size=5)

    with _registered(store):
        assert await processor.process_batch() == 2

    assert processed_ids == [legacy.id, sweep.id]
    assert legacy.id not in store.entries
    assert sweep.id not in store.entries


@pytest.mark.asyncio
async def test_exact_batch_persists_connectivity_terminal_and_replays_without_claim(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry()
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="8" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    process_calls = 0

    async def _connectivity_failure(_db, _entry, **_kwargs):
        nonlocal process_calls
        process_calls += 1
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "Alternative is outside the deterministic ownership contract.",
            details={
                "connectivity": {
                    "violations": [
                        {
                            "candidate_id": "forged-alternative",
                            "reason_code": "writer_not_connectivity_owner",
                        }
                    ]
                }
            },
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _connectivity_failure,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        first = await _process_exact(processor, scope)
        second = await _process_exact(processor, scope)

    assert process_calls == 1
    assert first.new_attempt_count == 1
    assert first.replayed_count == 0
    assert first.acked_count == 0
    assert len(first.terminal_failures) == 1
    row = first.terminal_failures[0]
    assert row.origin is ExactConsolidationResultOrigin.NEW
    assert row.mutation_state is ExactConsolidationMutationState.UNCHANGED
    assert row.error_code == consolidation.CONNECTIVITY_ERROR_CODE
    assert "writer_not_connectivity_owner" in (row.diagnostic_json or "")
    assert second.new_attempt_count == 0
    assert second.replayed_count == 1
    assert second.terminal_failures[0].origin is (
        ExactConsolidationResultOrigin.REPLAYED
    )
    assert entry.status == "pending"
    assert entry.attempts == 1
    assert entry.claim_token is None
    assert len(store.exact_disposition_calls) == 1
    assert store.ack_calls == []


@pytest.mark.asyncio
async def test_exact_retry_marker_waits_without_reclaim_then_replaces_after_due(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-retry")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    current = [datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)]
    clock = SimpleNamespace(now=lambda: current[0])
    process_calls = 0

    async def _retry_then_ack(_db, _entry, **kwargs):
        nonlocal process_calls
        process_calls += 1
        if process_calls == 1:
            raise consolidation.KGPrimitiveError(
                "relational_projection_endpoint_pending",
                "The prerequisite projection is not materialized yet.",
            )
        kwargs["deferred_session_ids"].append("session-retry-then-ack")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _retry_then_ack,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1, clock=clock)

    with _registered(store):
        first = await _process_exact(processor, scope)
        waiting = await _process_exact(processor, scope)
        current[0] += timedelta(seconds=2)
        completed = await _process_exact(processor, scope)

    assert process_calls == 2
    assert len(first.retry_scheduled) == 1
    assert first.retry_scheduled[0].attempt_ordinal == 1
    assert first.retry_scheduled[0].mutation_state is (
        ExactConsolidationMutationState.UNCHANGED
    )
    assert waiting.replayed_count == 1
    assert waiting.new_attempt_count == 0
    assert waiting.retry_scheduled[0].origin is ExactConsolidationResultOrigin.REPLAYED
    assert completed.acked_count == 1
    assert completed.rows[0].attempt_ordinal == 2
    assert completed.rows[0].disposition is ExactConsolidationDisposition.ACKED
    assert entry.id not in store.entries
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_ordinary_batch_refuses_durable_exact_disposition(monkeypatch) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-marker-ordinary")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        await _process_exact(processor, scope)
        with pytest.raises(
            RuntimeError,
            match="exact_rebuild_disposition_requires_process_exact_batch",
        ):
            await processor.process_batch(claim_scope=scope)

    assert entry.status == "pending"
    assert entry.claim_token is None
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_exact_batch_skips_global_claim_and_post_commit_maintenance(
    monkeypatch,
) -> None:
    from okto_pulse.core.services import queue_health_service

    entry = _exact_rebuild_entry(entry_id="card4-exact-no-maintenance")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    maintenance_calls: list[str] = []
    claim_metric_calls: list[datetime] = []

    async def _success_with_deferred_session(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-exact")
        return True

    async def _finalize(_session_id, **_kwargs):
        return None

    async def _maintenance(_db, *, entry, session_id):
        maintenance_calls.append(f"{entry.id}:{session_id}")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success_with_deferred_session,
    )
    monkeypatch.setattr(consolidation, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(consolidation, "_run_post_commit_maintenance", _maintenance)
    monkeypatch.setattr(
        queue_health_service,
        "record_claim",
        lambda *, now: claim_metric_calls.append(now),
    )

    processor = ConsolidationProcessor(_scope, batch_size=1)
    with _registered(store):
        result = await _process_exact(processor, scope)

    assert result.acked_count == 1
    assert maintenance_calls == []
    assert claim_metric_calls == []


@pytest.mark.asyncio
async def test_exact_ack_returns_durable_relational_receipt_and_compensates(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-journal")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="a" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _success(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-ack-journal")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        batch = await _process_exact(processor, scope)
        receipts = await processor.list_exact_rebuild_ack_receipts(
            claim_scope=scope,
            reservation_authority_probe=lambda: True,
        )
        compensated = await processor.compensate_exact_rebuild_commits(
            claim_scope=scope,
            reservation_authority_probe=lambda: True,
        )
        replayed = await processor.compensate_exact_rebuild_commits(
            claim_scope=scope,
            reservation_authority_probe=lambda: True,
        )

    assert batch.acked_count == 1
    assert batch.rows[0].ack_receipt == receipts[0]
    assert (
        ExactConsolidationAckReceipt.from_payload(receipts[0].to_payload())
        == receipts[0]
    )
    assert receipts[0].consolidation_session_id == "session-ack-journal"
    assert compensated.receipt.ack_count == 1
    assert compensated.receipt.audit_session_ids == ("session-ack-journal",)
    assert compensated.replayed is False
    assert replayed.replayed is True
    assert replayed.receipt == compensated.receipt
    assert (
        ExactConsolidationCompensationReceipt.from_payload(
            compensated.receipt.to_payload()
        )
        == compensated.receipt
    )
    with pytest.raises(
        ValueError,
        match="exact_consolidation_compensation_receipt_invalid",
    ):
        replace(
            compensated.receipt,
            terminal_materialization_generation=(
                compensated.receipt.baseline_materialization_generation
            ),
        )
    assert store.materialization_head == "mg-card4-baseline"


@pytest.mark.asyncio
async def test_exact_ack_journal_rejects_materialization_generation_cycle() -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-cycle-a")
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=entry.source,
        reservation_lineage_id="a" * 64,
    )
    store = _MemoryConsolidationStore(())
    store.current_reservation_source = entry.source

    def _receipt(
        *,
        queue_id: str,
        artifact_id: str,
        previous_generation: str,
        generation: str,
    ) -> ExactConsolidationAckReceipt:
        return ExactConsolidationAckReceipt.create(
            queue_id=queue_id,
            board_id=entry.board_id,
            source=entry.source,
            reservation_lineage_id="a" * 64,
            work_kind="consolidate",
            artifact_type="spec",
            artifact_id=artifact_id,
            generation=0,
            membership_source_ref=f"spec:{artifact_id}",
            membership_source_version="1",
            membership_content_hash="b" * 64,
            audit_content_hash="c" * 64,
            consolidation_session_id=f"session-{queue_id}",
            outbox_event_id=f"outbox-{queue_id}",
            generation_event_id=f"event-{queue_id}",
            previous_materialization_generation=previous_generation,
            materialization_generation=generation,
            node_ref_count=0,
            node_refs_sha256="0" * 64,
        )

    store.exact_ack_receipts = [
        _receipt(
            queue_id="cycle-a",
            artifact_id="cycle-artifact-a",
            previous_generation="generation-a",
            generation="generation-b",
        ),
        _receipt(
            queue_id="cycle-b",
            artifact_id="cycle-artifact-b",
            previous_generation="generation-b",
            generation="generation-a",
        ),
    ]

    processor = ConsolidationProcessor(_scope, batch_size=1)
    with _registered(store):
        with pytest.raises(
            ExactConsolidationCompensationError,
            match="exact_consolidation_ack_journal_invalid",
        ):
            await processor.list_exact_rebuild_ack_receipts(
                claim_scope=scope,
                reservation_authority_probe=lambda: True,
            )


@pytest.mark.asyncio
async def test_exact_compensation_reports_post_commit_authority_loss(
    monkeypatch,
) -> None:
    authority = [True]

    class PostCommitLossStore(_MemoryConsolidationStore):
        lose_on_commit = False

        async def commit(self, context) -> None:
            await super().commit(context)
            if self.lose_on_commit:
                authority[0] = False

    entry = _exact_rebuild_entry(entry_id="card4-exact-compensation-fence")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="a" * 64,
    )
    store = PostCommitLossStore((entry,))
    store.current_reservation_source = source

    async def _success(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-compensation-fence")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: authority[0],
        )
        store.lose_on_commit = True
        with pytest.raises(
            ExactConsolidationCompensationError,
            match="authority_lost_after_commit",
        ) as raised:
            await processor.compensate_exact_rebuild_commits(
                claim_scope=scope,
                reservation_authority_probe=lambda: authority[0],
            )

    assert raised.value.committed_result is not None
    assert raised.value.committed_result.receipt.ack_count == 1
    assert store.materialization_head == "mg-card4-baseline"


@pytest.mark.asyncio
async def test_exact_pre_acquire_lock_contention_is_retryable_and_unchanged(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-pre-acquire-contention")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="a" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    aborted: list[str] = []

    @contextmanager
    def _contended(*_args, **_kwargs):
        raise consolidation.GuardedWriteError(
            "lock_contention",
            "another writer currently owns the board graph",
            retryable=True,
        )
        yield  # pragma: no cover

    async def _attempt(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-pre-acquire")
        kwargs["enter_graph_write"]("mutation-pre-acquire")
        return True

    async def _abort(session_id, **_kwargs):
        aborted.append(session_id)

    monkeypatch.setattr(consolidation, "guarded_board_write", _contended)
    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _attempt,
    )
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert aborted == ["session-pre-acquire"]
    assert result.new_attempt_count == 1
    assert len(result.retry_scheduled) == 1
    assert result.retry_scheduled[0].mutation_state is (
        ExactConsolidationMutationState.UNCHANGED
    )
    assert result.retry_scheduled[0].error_code == "guarded_write_lock_contention"
    assert entry.status == "pending"
    assert entry.id in store.entries


@pytest.mark.asyncio
async def test_exact_pre_acquire_retry_authority_loss_is_neutral_unchanged(
    monkeypatch,
) -> None:
    authority = [True]

    class AuthorityLossStore(_MemoryConsolidationStore):
        async def save_exact_rebuild_disposition(self, context, **identity):
            authority[0] = False
            return await super().save_exact_rebuild_disposition(
                context,
                **identity,
            )

    entry = _exact_rebuild_entry(entry_id="card4-exact-pre-acquire-fence")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="a" * 64,
    )
    store = AuthorityLossStore((entry,))
    store.current_reservation_source = source

    @contextmanager
    def _contended(*_args, **_kwargs):
        raise consolidation.GuardedWriteError(
            "lock_contention",
            "another writer currently owns the board graph",
            retryable=True,
        )
        yield  # pragma: no cover

    async def _attempt(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-pre-acquire-fence")
        kwargs["enter_graph_write"]("mutation-pre-acquire-fence")
        return True

    async def _abort(_session_id, **_kwargs):
        return None

    monkeypatch.setattr(consolidation, "guarded_board_write", _contended)
    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _attempt,
    )
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: authority[0],
        )

    assert len(result.rows) == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert result.rows[0].mutation_state is (ExactConsolidationMutationState.UNCHANGED)
    assert entry.status == "claimed"
    assert entry.payload is not None
    assert "_exact_rebuild_disposition" not in entry.payload


@pytest.mark.asyncio
async def test_exact_batch_requires_crash_claim_recovery_before_processing() -> None:
    entry = _exact_rebuild_entry(
        entry_id="card4-exact-crash-claim",
        status="claimed",
        claim_token="dead-owner-token",
    )
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with (
        _registered(store),
        pytest.raises(
            RuntimeError,
            match="consolidation_exact_claimed_rows_require_recovery",
        ),
    ):
        await _process_exact(processor, scope)

    assert entry.status == "claimed"
    assert entry.claim_token == "dead-owner-token"
    assert store.commit_count == 0


@pytest.mark.asyncio
async def test_exact_batch_rejects_tampered_terminal_marker_before_claim(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-tampered-marker")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        await _process_exact(processor, scope)
        assert entry.payload is not None
        entry.payload["_exact_rebuild_disposition"]["queue_attempts"] = True
        with pytest.raises(
            RuntimeError,
            match="exact_rebuild_disposition_queue_state_invalid",
        ):
            await _process_exact(processor, scope)

    assert entry.status == "pending"
    assert entry.claim_token is None
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_exact_batch_reservation_loss_repends_without_disposition_or_effect(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-reservation-loss")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore(
        (entry,),
        reservation_sources=(source, source, source, None),
    )
    process_calls = 0

    async def _must_not_process(_db, _entry, **_kwargs):
        nonlocal process_calls
        process_calls += 1
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _must_not_process,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert process_calls == 0
    assert result.new_attempt_count == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert result.rows[0].mutation_state is ExactConsolidationMutationState.UNCHANGED
    assert entry.status == "pending"
    assert entry.claim_token is None
    assert entry.payload == {
        "_rebuild_membership": {
            "content_hash": "a" * 64,
            "run_id": source.removeprefix("rebuild:"),
            "source_ref": f"spec:{entry.artifact_id}",
            "source_version": "7",
        }
    }
    assert store.exact_disposition_calls == []


@pytest.mark.asyncio
async def test_exact_false_outcome_uses_terminal_marker_not_generic_failure_path(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-false")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _false(_db, _entry, **_kwargs):
        return False

    async def _generic_failure_forbidden(*_args, **_kwargs):
        raise AssertionError("exact processing entered generic debt/DLQ path")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _false,
    )
    monkeypatch.setattr(
        ConsolidationProcessor,
        "_mark_failed",
        _generic_failure_forbidden,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert len(result.terminal_failures) == 1
    assert result.terminal_failures[0].error_code == "consolidation_returned_false"
    assert result.terminal_failures[0].mutation_state is (
        ExactConsolidationMutationState.AMBIGUOUS
    )
    assert entry.status == "pending"
    assert entry.attempts == 1
    assert entry.claim_token is None
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_exact_ack_integrity_failure_is_terminal_unchanged_after_abort(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-integrity")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    aborted: list[str] = []

    async def _invalid_ack(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-ack-integrity")
        raise ExactConsolidationAckIntegrityError(
            "exact_consolidation_ack_audit_invalid"
        )

    async def _abort(session_id, **_kwargs):
        aborted.append(session_id)

    async def _generic_failure_forbidden(*_args, **_kwargs):
        raise AssertionError("exact integrity failure entered generic debt/DLQ path")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _invalid_ack,
    )
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    monkeypatch.setattr(
        ConsolidationProcessor,
        "_mark_failed",
        _generic_failure_forbidden,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert aborted == ["session-ack-integrity"]
    assert len(result.terminal_failures) == 1
    row = result.terminal_failures[0]
    assert row.error_code == "exact_consolidation_ack_audit_invalid"
    assert row.error_message == "exact_consolidation_ack_audit_invalid"
    assert row.diagnostic_json == (
        '{"integrity_code":"exact_consolidation_ack_audit_invalid"}'
    )
    assert row.mutation_state is ExactConsolidationMutationState.UNCHANGED
    assert entry.status == "pending"
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_exact_ack_integrity_abort_failure_is_terminal_ambiguous(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-integrity-abort-failed")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _invalid_ack(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-ack-integrity-abort-failed")
        raise ExactConsolidationAckIntegrityError(
            "exact_consolidation_ack_generation_head_invalid"
        )

    async def _abort_failed(_session_id, **_kwargs):
        raise RuntimeError("graph abort did not complete")

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _invalid_ack,
    )
    monkeypatch.setattr(
        consolidation,
        "abort_deferred_consolidation",
        _abort_failed,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert len(result.terminal_failures) == 1
    row = result.terminal_failures[0]
    assert row.error_code == "exact_consolidation_ack_generation_head_invalid"
    assert row.mutation_state is ExactConsolidationMutationState.AMBIGUOUS
    assert row.diagnostic_json == (
        '{"integrity_code":"exact_consolidation_ack_generation_head_invalid"}'
    )


@pytest.mark.asyncio
async def test_exact_ack_integrity_secondary_cas_loss_preserves_primary_diagnostic(
    monkeypatch,
) -> None:
    class _DispositionLossStore(_MemoryConsolidationStore):
        async def save_exact_rebuild_disposition(self, _context, **identity):
            self.exact_disposition_calls.append(identity)
            return None

    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-integrity-cas-loss")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _DispositionLossStore((entry,))
    store.current_reservation_source = source
    aborted: list[str] = []

    async def _invalid_ack(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-integrity-cas-loss")
        raise ExactConsolidationAckIntegrityError(
            "exact_consolidation_ack_outbox_invalid"
        )

    async def _abort(session_id, **_kwargs):
        aborted.append(session_id)

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _invalid_ack,
    )
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert aborted == ["session-integrity-cas-loss"]
    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.disposition is ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    assert row.mutation_state is ExactConsolidationMutationState.UNCHANGED
    assert row.error_code == "queue_claim_lost_or_fenced"
    assert "primary=exact_consolidation_ack_outbox_invalid" in row.error_message
    assert row.diagnostic_json == (
        '{"integrity_code":"exact_consolidation_ack_outbox_invalid"}'
    )


@pytest.mark.asyncio
async def test_exact_disposition_cas_rejects_payload_mutation_after_fresh_read(
    monkeypatch,
) -> None:
    class _PayloadRaceStore(_MemoryConsolidationStore):
        async def save_exact_rebuild_disposition(self, context, **identity):
            stored = self.entries[str(identity["entry_id"])]
            assert stored.payload is not None
            stored.payload["_rebuild_membership"]["content_hash"] = "b" * 64
            return await super().save_exact_rebuild_disposition(context, **identity)

    entry = _exact_rebuild_entry(entry_id="card4-exact-payload-race")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _PayloadRaceStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert len(result.rows) == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert result.rows[0].membership_content_hash == "a" * 64
    assert entry.status == "claimed"
    assert entry.claim_token is not None
    assert entry.payload is not None
    assert entry.payload["_rebuild_membership"]["content_hash"] == "b" * 64
    assert "_exact_rebuild_disposition" not in entry.payload


@pytest.mark.asyncio
async def test_exact_baseexception_leaves_claim_for_governed_recovery_then_acks(
    monkeypatch,
) -> None:
    class _SimulatedProcessCrash(BaseException):
        pass

    entry = _exact_rebuild_entry(entry_id="card4-exact-baseexception")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="9" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _crash(_db, _entry, **_kwargs):
        raise _SimulatedProcessCrash()

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _crash,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        with pytest.raises(_SimulatedProcessCrash):
            await _process_exact(processor, scope)
        assert entry.status == "claimed"
        assert entry.claim_token is not None
        assert (
            await processor.recover_exact_claims(
                claim_scope=scope,
                recovery_authority_probe=lambda: True,
            )
            == 1
        )
        assert entry.status == "pending"
        assert entry.claim_token is None

        async def _success(_db, _entry, **kwargs):
            kwargs["deferred_session_ids"].append("session-recovered")
            return True

        monkeypatch.setattr(
            consolidation,
            "_process_queue_entry_serialized",
            _success,
        )
        result = await _process_exact(processor, scope)

    assert result.acked_count == 1
    assert entry.id not in store.entries
    assert store.exact_disposition_calls == []


@pytest.mark.asyncio
async def test_exact_ack_cas_loss_returns_one_neutral_row_and_repends(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-loss")
    source = entry.source
    scope = ConsolidationClaimScope(board_id=entry.board_id, source=source)
    store = _MemoryConsolidationStore((entry,), ack_result=False)
    store.current_reservation_source = source

    async def _success(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-ack-loss")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(processor, scope)

    assert processor.last_attempted_count == 1
    assert result.new_attempt_count == 1
    assert len(result.rows) == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert result.rows[0].mutation_state is ExactConsolidationMutationState.UNCHANGED
    assert entry.status == "pending"
    assert entry.claim_token is None
    assert len(store.ack_calls) == 1
    assert len(store.repend_calls) == 1


@pytest.mark.asyncio
async def test_exact_marker_replays_under_successor_token_in_same_lineage(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-lineage-successor")
    source = entry.source
    lineage_id = "c" * 64
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id=lineage_id,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    current_authority = ["token-a"]
    process_calls = 0

    async def _terminal(_db, _entry, **_kwargs):
        nonlocal process_calls
        process_calls += 1
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        first = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )
        current_authority[0] = "token-b"
        resumed = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-b",
        )

    assert process_calls == 1
    assert first.new_attempt_count == 1
    assert resumed.new_attempt_count == 0
    assert resumed.replayed_count == 1
    assert resumed.terminal_failures[0].reservation_lineage_id == lineage_id


@pytest.mark.asyncio
async def test_exact_marker_rejects_foreign_lineage_with_same_rebuild_source(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-foreign-lineage")
    source = entry.source
    original_scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="d" * 64,
    )
    foreign_scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="e" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        await _process_exact(processor, original_scope)
        with pytest.raises(
            RuntimeError,
            match="exact_rebuild_disposition_binding_invalid",
        ):
            await _process_exact(processor, foreign_scope)

    assert entry.status == "pending"
    assert entry.claim_token is None
    assert len(store.exact_disposition_calls) == 1


@pytest.mark.asyncio
async def test_exact_same_source_authority_replacement_before_disposition_cas_is_neutral(
    monkeypatch,
) -> None:
    current_authority = ["token-a"]

    class _AuthorityReplacementStore(_MemoryConsolidationStore):
        async def save_exact_rebuild_disposition(self, context, **identity):
            current_authority[0] = "token-b"
            return await super().save_exact_rebuild_disposition(context, **identity)

    entry = _exact_rebuild_entry(entry_id="card4-exact-authority-pre-cas")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="1" * 64,
    )
    store = _AuthorityReplacementStore((entry,))
    store.current_reservation_source = source

    async def _terminal(_db, _entry, **_kwargs):
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )

    assert result.new_attempt_count == 1
    assert len(result.rows) == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert entry.status == "claimed"
    assert entry.claim_token is not None
    assert entry.payload is not None
    assert "_exact_rebuild_disposition" not in entry.payload


@pytest.mark.asyncio
async def test_exact_authority_is_reproved_under_graph_writer_before_effect(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-authority-under-writer")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="3" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    current_authority = ["token-a"]
    mutation_calls: list[str] = []

    monkeypatch.setattr(
        consolidation,
        "guarded_board_write",
        lambda *_args, **_kwargs: nullcontext(
            SimpleNamespace(
                durability_applied=False,
                ensure_owned=lambda **_kwargs: None,
            )
        ),
    )
    monkeypatch.setattr(
        consolidation,
        "_ensure_entry_admitted_by_reservation_under_writer",
        lambda _entry: None,
    )

    async def _attempt_graph_effect(_db, _entry, **kwargs):
        current_authority[0] = "token-b"
        kwargs["enter_graph_write"]("exact-authority-test")
        mutation_calls.append("mutated")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _attempt_graph_effect,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )

    assert mutation_calls == []
    assert result.new_attempt_count == 1
    assert len(result.rows) == 1
    assert result.rows[0].disposition is (
        ExactConsolidationDisposition.NEUTRAL_FENCE_LOSS
    )
    assert entry.status == "claimed"
    assert entry.claim_token is not None


@pytest.mark.asyncio
async def test_exact_authority_replacement_after_disposition_commit_returns_one_neutral_and_replays(
    monkeypatch,
) -> None:
    current_authority = ["token-a"]

    class _PostCommitReplacementStore(_MemoryConsolidationStore):
        async def commit(self, context) -> None:
            await super().commit(context)
            if self.commit_count == 2:
                current_authority[0] = "token-b"

    entry = _exact_rebuild_entry(entry_id="card4-exact-authority-post-commit")
    source = entry.source
    lineage_id = "2" * 64
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id=lineage_id,
    )
    store = _PostCommitReplacementStore((entry,))
    store.current_reservation_source = source
    process_calls = 0

    async def _terminal(_db, _entry, **_kwargs):
        nonlocal process_calls
        process_calls += 1
        raise consolidation.KGPrimitiveError(
            consolidation.CONNECTIVITY_ERROR_CODE,
            "deterministic terminal",
        )

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _terminal,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        fenced = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )
        resumed = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-b",
        )

    assert process_calls == 1
    assert fenced.new_attempt_count == 1
    assert len(fenced.terminal_failures) == 1
    assert fenced.rows[0].disposition is (
        ExactConsolidationDisposition.TERMINAL_FAILURE
    )
    assert resumed.new_attempt_count == 0
    assert resumed.replayed_count == 1
    assert resumed.terminal_failures[0].reservation_lineage_id == lineage_id


@pytest.mark.asyncio
async def test_exact_post_commit_authority_loss_finalizes_instead_of_compensating_graph(
    monkeypatch,
) -> None:
    current_authority = ["token-a"]

    class _PostCommitReplacementStore(_MemoryConsolidationStore):
        async def commit(self, context) -> None:
            await super().commit(context)
            if self.commit_count == 2:
                current_authority[0] = "token-b"

    entry = _exact_rebuild_entry(entry_id="card4-exact-graph-post-commit")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="4" * 64,
    )
    store = _PostCommitReplacementStore((entry,))
    store.current_reservation_source = source
    finalized: list[str] = []
    compensated: list[str] = []

    async def _success_with_deferred_graph(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-durable")
        return True

    async def _finalize(session_id, **_kwargs):
        finalized.append(session_id)

    async def _abort(session_id, **_kwargs):
        compensated.append(session_id)

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success_with_deferred_graph,
    )
    monkeypatch.setattr(consolidation, "finalize_deferred_consolidation", _finalize)
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )

    assert result.new_attempt_count == 1
    assert len(result.rows) == 1
    assert result.rows[0].disposition is ExactConsolidationDisposition.ACKED
    assert result.rows[0].mutation_state is (ExactConsolidationMutationState.COMMITTED)
    assert finalized == ["session-durable"]
    assert compensated == []
    assert entry.id not in store.entries


@pytest.mark.asyncio
async def test_exact_post_commit_authority_loss_without_graph_reports_durable_ack(
    monkeypatch,
) -> None:
    current_authority = ["token-a"]

    class _PostCommitReplacementStore(_MemoryConsolidationStore):
        async def commit(self, context) -> None:
            await super().commit(context)
            if self.commit_count == 2:
                current_authority[0] = "token-b"

    entry = _exact_rebuild_entry(entry_id="card4-exact-ack-post-commit")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="5" * 64,
    )
    store = _PostCommitReplacementStore((entry,))
    store.current_reservation_source = source

    async def _success(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-post-commit")
        return True

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success,
    )
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with _registered(store):
        result = await _process_exact(
            processor,
            scope,
            reservation_authority_probe=lambda: current_authority[0] == "token-a",
        )

    assert result.new_attempt_count == 1
    assert result.acked_count == 1
    assert len(result.rows) == 1
    assert result.rows[0].mutation_state is (ExactConsolidationMutationState.COMMITTED)
    assert entry.id not in store.entries


@pytest.mark.asyncio
async def test_exact_double_post_commit_finalize_failure_raises_typed_blocker(
    monkeypatch,
) -> None:
    entry = _exact_rebuild_entry(entry_id="card4-exact-finalize-blocker")
    source = entry.source
    scope = ConsolidationClaimScope(
        board_id=entry.board_id,
        source=source,
        reservation_lineage_id="a" * 64,
    )
    store = _MemoryConsolidationStore((entry,))
    store.current_reservation_source = source
    finalize_calls: list[str] = []
    compensated: list[str] = []

    async def _success_with_deferred_graph(_db, _entry, **kwargs):
        kwargs["deferred_session_ids"].append("session-unfinalized")
        return True

    async def _fail_finalize(session_id, **_kwargs):
        finalize_calls.append(session_id)
        raise RuntimeError("simulated finalize failure")

    async def _abort(session_id, **_kwargs):
        compensated.append(session_id)

    monkeypatch.setattr(
        consolidation,
        "_process_queue_entry_serialized",
        _success_with_deferred_graph,
    )
    monkeypatch.setattr(
        consolidation,
        "finalize_deferred_consolidation",
        _fail_finalize,
    )
    monkeypatch.setattr(consolidation, "abort_deferred_consolidation", _abort)
    processor = ConsolidationProcessor(_scope, batch_size=1)

    with (
        _registered(store),
        pytest.raises(
            consolidation.ExactConsolidationPostCommitError,
            match="exact_consolidation_post_commit_finalization_failed",
        ) as captured,
    ):
        await _process_exact(processor, scope)

    assert captured.value.failed_queue_id == entry.id
    assert captured.value.error_code == (
        "exact_consolidation_post_commit_finalization_failed"
    )
    assert captured.value.batch_result.new_attempt_count == 1
    assert captured.value.batch_result.acked_count == 1
    assert captured.value.batch_result.rows[0].queue_id == entry.id
    assert finalize_calls == ["session-unfinalized", "session-unfinalized"]
    assert compensated == []
    assert entry.id not in store.entries
