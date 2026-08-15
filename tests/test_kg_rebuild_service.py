"""KG-02.3 — Rebuild service admin lane tests.

Covers AC7/AC8/AC15, IR ir_03c2a132 + ir_73c3e169, TR10/TR12, OR
or_37cebd03.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from okto_pulse.core.kg.recovery_execution import (
    issue_recovery_execution_capability,
)
from okto_pulse.core.kg.rebuild_confirmation import (
    RebuildConfirmationStore,
)
from okto_pulse.core.kg.rebuild_service import (
    KGRebuildService,
    RebuildBlockReason,
    RebuildOutcome,
    RebuildStepInput,
    RebuildStepResult,
    get_rebuild_run_count,
    get_rebuild_run_counter_labels,
    get_rebuild_run_samples,
    reset_rebuild_run_counter,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebuildSourceEnumerator,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    HealthProbe,
    KGSafeWriteLifecycle,
    LifecycleStepResult,
    LockOwnerProbe,
)
from okto_pulse.core.kg.single_writer_lock import (
    KGAdministrativeOperationReservation,
    KGSingleWriterLock,
)
from coordination_fakes import FakeWriteLockPort
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphLifecycleForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
)


class _RecoveryEnabledKGRebuildService(KGRebuildService):
    """Test harness that proves the same opaque recovery authority as CLI.

    Individual capability-gate tests invoke ``KGRebuildService.run`` directly
    on this instance to bypass the harness and exercise fail-closed admission.
    """

    def run(self, *, board_id: str, **kwargs):
        with issue_recovery_execution_capability(
            board_id=board_id,
            lifetime_probe=lambda: True,
        ) as capability:
            return super().run(
                board_id=board_id,
                recovery_capability=capability,
                **kwargs,
            )


@pytest.fixture(autouse=True)
def _real_board_graph_registry(_kg_registry_test_fakes):
    configure_test_kg_registry(
        cypher_executor=RealBoardCypherExecutorForTests(),
        graph_transaction=RealBoardGraphTransactionForTests(),
        graph_lifecycle=RealBoardGraphLifecycleForTests(),
    )


# ---------------------------------------------------------------------------
# IMPL-2 compatibility helper
#
# kg_rebuild.py added FR10 (board scope check) + FR9 (real health probe) in
# IMPL-2.  Both gates use a DB session.  TestClient-based tests that bypass
# require_user MUST also override get_db (so the Board SELECT passes) and
# monkeypatch get_kg_health so the health probe returns a healthy state.
#
# Usage:
#   app = _make_rebuild_test_app()
#   app.dependency_overrides[require_user] = _fake_user
# ---------------------------------------------------------------------------


def _make_rebuild_test_app(board_id: str = "b-test"):
    """Return a FastAPI app with get_db overridden to satisfy FR10/FR9 gates.

    The fake DB session returns a mock Board row for the given board_id,
    bypassing the SQLite SELECT without touching real storage.
    """
    from types import SimpleNamespace

    from fastapi import FastAPI

    from okto_pulse.community.api.deps import get_unit_of_work
    from okto_pulse.community.api.router import api_router

    _fake_board = SimpleNamespace(id=board_id, owner_id="user-test")

    class _Boards:
        async def get(self, candidate_board_id):
            return _fake_board if candidate_board_id == board_id else None

    class _Shares:
        async def get_user_permission(self, candidate_board_id, _user_id):
            return "editor" if candidate_board_id == board_id else None

    async def _resolve_user_permissions(_user_id, candidate_board_id):
        if candidate_board_id != board_id:
            return []
        return [
            "kg.operations.rebuild.preflight",
            "kg.operations.rebuild.confirm",
            "kg.operations.rebuild.run",
            "kg.admin.settings_read",
            "kg.admin.settings_write",
        ]

    async def _fake_uow():
        yield SimpleNamespace(
            boards=_Boards(),
            services=SimpleNamespace(
                shares=_Shares(),
                resolve_user_permissions=_resolve_user_permissions,
            ),
        )

    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[get_unit_of_work] = _fake_uow
    return app


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_rebuild_run_counter()
    yield
    reset_rebuild_run_counter()


def _row():
    return {
        "artifact_type": "spec",
        "id": "id-1",
        "source_ref": "ref:id-1",
        "source_version": "v1",
        "content_hash": "h1",
        "created_at": "2026-05-01T00:00:00Z",
        "status": "validated",
    }


def _build_service(
    tmp_path: Path,
    *,
    step_adapter=None,
    source_rows=None,
    lifecycle_step_ok=True,
    lifecycle_step_failed_step="flush",
    single_writer_lock: KGSingleWriterLock | None = None,
) -> tuple[
    KGRebuildService,
    KGRebuildSourceManifest,
    RebuildConfirmationStore,
    KGSingleWriterLock,
]:
    """Wire a fully-functional rebuild service against tmp storage."""
    rows = source_rows if source_rows is not None else [_row()]
    lock = single_writer_lock or KGSingleWriterLock(
        base_dir=tmp_path / "locks",
        write_lock_port=FakeWriteLockPort(),
    )
    enumerator = RebuildSourceEnumerator(source_store=lambda _b: list(rows))
    manifest_store = KGRebuildSourceManifest(base_dir=tmp_path)
    confirmation_store = RebuildConfirmationStore(base_dir=tmp_path)

    def _owner_probe(board_id: str, owner_token: str) -> bool:
        manifest = lock.inspect(board_id=board_id)
        return manifest is not None and manifest.owner_token == owner_token

    def _step(b, g, step):
        if lifecycle_step_ok:
            return LifecycleStepResult(ok=True)
        if step == lifecycle_step_failed_step:
            return LifecycleStepResult(ok=False, detail="forced failure")
        return LifecycleStepResult(ok=True)

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=_step,
        owner_probe=LockOwnerProbe(is_active_owner=_owner_probe),
        health_probe=HealthProbe(classify=lambda b, g, status, step: "at_risk"),
    )

    service = _RecoveryEnabledKGRebuildService(
        base_dir=tmp_path,
        single_writer_lock=lock,
        safe_write_lifecycle=safe_lifecycle,
        quarantine_service=None,
        confirmation_store=confirmation_store,
        manifest_store=manifest_store,
        rebuild_step_adapter=step_adapter or (lambda req: RebuildStepResult(ok=True)),
        source_enumerator=enumerator,
        lock_ttl_seconds=60,
    )
    return service, manifest_store, confirmation_store, lock


def _issue_confirmation(
    confirmation_store: RebuildConfirmationStore,
    manifest_store: KGRebuildSourceManifest,
    enumerator: RebuildSourceEnumerator,
    *,
    board_id: str = "b1",
    actor_id: str = "user-1",
    operation: str = "rebuild",
) -> tuple[str, str, str]:
    """Mint a fresh manifest + confirmation; returns the args /run needs."""
    source_set = enumerator.enumerate(board_id=board_id)
    preflight_hash = "a" * 64
    manifest = manifest_store.build(
        source_set=source_set, preflight_hash=preflight_hash
    )
    token = confirmation_store.issue(
        board_id=board_id,
        actor_id=actor_id,
        operation=operation,
        preflight_hash=preflight_hash,
        manifest_ref=manifest.manifest_ref,
    )
    return token.confirmation_id, manifest.manifest_ref, preflight_hash


# --- Happy path -------------------------------------------------------------


@pytest.mark.parametrize(
    "forged_capability",
    [None, True, "offline", {"board_id": "b1"}, object()],
)
def test_run_requires_opaque_recovery_capability_before_confirmation_consume(
    tmp_path: Path,
    forged_capability,
):
    service, manifest_store, confirmation_store, _lock = _build_service(tmp_path)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    args = {
        "confirmation_id": confirmation_id,
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "offline recovery",
    }

    denied = KGRebuildService.run(
        service,
        recovery_capability=forged_capability,
        **args,
    )

    assert denied.outcome == RebuildOutcome.RECOVERY_EXECUTION_REQUIRED.value
    assert denied.reason == RebuildBlockReason.RECOVERY_EXECUTION_REQUIRED.value
    assert denied.audit_ref == ""
    # Admission happens before the one-shot confirmation consume. The exact
    # same confirmation therefore remains usable by a genuine offline scope.
    allowed = service.run(**args)
    assert allowed.outcome == RebuildOutcome.COMPLETED.value


def test_run_rejects_wrong_board_and_revoked_recovery_capabilities(tmp_path: Path):
    service, manifest_store, confirmation_store, _lock = _build_service(tmp_path)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    args = {
        "confirmation_id": confirmation_id,
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "offline recovery",
    }

    with issue_recovery_execution_capability(
        board_id="other-board",
        lifetime_probe=lambda: True,
    ) as wrong_board:
        mismatch = KGRebuildService.run(
            service,
            recovery_capability=wrong_board,
            **args,
        )
    assert mismatch.outcome == RebuildOutcome.RECOVERY_EXECUTION_REQUIRED.value

    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as revoked:
        pass
    expired = KGRebuildService.run(
        service,
        recovery_capability=revoked,
        **args,
    )
    assert expired.outcome == RebuildOutcome.RECOVERY_EXECUTION_REQUIRED.value

    # Both denials occurred before consume.
    assert service.run(**args).outcome == RebuildOutcome.COMPLETED.value


def test_recovery_capability_lifetime_loss_fences_terminal_completion(tmp_path: Path):
    alive = {"value": True}

    def _step(_req):
        alive["value"] = False
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=_step,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: alive["value"],
    ) as capability:
        result = KGRebuildService.run(
            service,
            confirmation_id=confirmation_id,
            board_id="b1",
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason="offline recovery",
            recovery_capability=capability,
        )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.current_kg_generation_id is None
    assert lock.inspect(board_id="b1") is None


def test_capability_loss_after_confirmation_consume_creates_resumable_receipt(
    tmp_path: Path,
    monkeypatch,
):
    service, manifest_store, confirmation_store, _lock = _build_service(tmp_path)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    alive = {"value": True}
    original_consume = confirmation_store.artifact_store.consume_json_with_receipt

    def _consume_then_expire(**kwargs):
        outcome = original_consume(**kwargs)
        alive["value"] = False
        return outcome

    monkeypatch.setattr(
        confirmation_store.artifact_store,
        "consume_json_with_receipt",
        _consume_then_expire,
    )

    args = {
        "confirmation_id": confirmation_id,
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "offline recovery",
    }
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: alive["value"],
    ) as capability:
        expired = KGRebuildService.run(
            service,
            recovery_capability=capability,
            **args,
        )

    assert expired.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert expired.reason == RebuildBlockReason.LEASE_LOST.value
    assert expired.audit_ref == ""
    # Unlike admission denial, this lifetime loss happened after the atomic
    # token->active receipt transition. Replaying the raw token is denied.
    replay = service.run(**args)
    assert replay.outcome == RebuildOutcome.CONFIRMATION_REQUIRED.value
    assert replay.reason == RebuildBlockReason.CONFIRMATION_INVALID.value

    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    active = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(active) == 1
    alive["value"] = True
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: alive["value"],
    ) as resume_capability:
        resumed = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=str(active[0]["run_id"]),
            recovery_capability=resume_capability,
            **{key: value for key, value in args.items() if key != "confirmation_id"},
        )
    assert resumed.outcome == RebuildOutcome.COMPLETED.value


def test_verified_active_receipt_loader_owns_schema_and_run_binding(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint
    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
    from okto_pulse.core.kg.rebuild_service import (
        RebuildConfirmationReceiptIntegrityError,
        load_verified_rebuild_confirmation_receipt,
        rebuild_active_confirmation_receipt_key,
        rebuild_operation_run_id,
    )

    store = RebuildConfirmationStore(base_dir=tmp_path)
    # Existing installs can contain legacy board artifacts from before the
    # active-receipt protocol.  They are not an oracle for a torn new receipt
    # and therefore do not block first admission.
    store.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="run_audit",
            board_id="b1",
            artifact_id="legacy-run",
        ),
        {"board_id": "b1", "run_id": "legacy-run"},
    )
    assert (
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )
        is None
    )
    binding = {
        "board_id": "b1",
        "operation": "rebuild",
        "preflight_hash": "a" * 64,
        "source_set_hash": "b" * 64,
        "manifest_ref": "rebuild_manifest_verified_loader",
    }
    receipt = {
        "schema_version": "kg_rebuild_confirmation_receipt.v1",
        "run_id": rebuild_operation_run_id(**binding),
        **binding,
        "actor_id": "user-1",
        "confirmation_ref": confirmation_fingerprint("opaque-token"),
        "user_reason": "verified recovery",
        "started_at": "2026-08-15T00:00:00+00:00",
        # Forward-compatible terminal journal fields are not part of the
        # authorization identity and therefore remain tolerated.
        "journal_extension": {"phase": "report_persisted"},
    }
    key = rebuild_active_confirmation_receipt_key(board_id="b1")
    store.artifact_store.write_json_atomic(key, receipt)
    assert (
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )
        == receipt
    )

    store.artifact_store.write_json_atomic(
        key,
        {**receipt, "run_id": "run_forged"},
    )
    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_active_receipt_integrity_invalid",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )


def test_verified_active_receipt_loader_rejects_history_without_active(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.kg.rebuild_service import (
        RebuildConfirmationReceiptIntegrityError,
        load_verified_rebuild_confirmation_receipt,
        rebuild_confirmation_receipt_key,
    )

    store = RebuildConfirmationStore(base_dir=tmp_path)
    store.artifact_store.write_json_atomic(
        rebuild_confirmation_receipt_key(board_id="b1", run_id="run_orphaned"),
        {"board_id": "b1", "run_id": "run_orphaned"},
    )

    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_active_missing_with_history",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )


def test_verified_active_receipt_loader_wraps_unverifiable_history_absence() -> None:
    from okto_pulse.core.kg.rebuild_service import (
        RebuildConfirmationReceiptIntegrityError,
        load_verified_rebuild_confirmation_receipt,
    )

    class HistoryReadFails:
        calls = 0

        def list_json_bounded(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
            self.calls += 1
            if self.calls == 1:
                return []
            raise RuntimeError("malformed or oversized history")

    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_active_missing_history_unverifiable",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=HistoryReadFails(),  # type: ignore[arg-type]
            board_id="b1",
        )


def test_verified_terminal_active_receipt_requires_exact_history(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint
    from okto_pulse.core.kg.rebuild_service import (
        RebuildConfirmationReceiptIntegrityError,
        load_verified_rebuild_confirmation_receipt,
        rebuild_active_confirmation_receipt_key,
        rebuild_confirmation_receipt_key,
        rebuild_operation_run_id,
    )

    store = RebuildConfirmationStore(base_dir=tmp_path)
    binding = {
        "board_id": "b1",
        "operation": "rebuild",
        "preflight_hash": "a" * 64,
        "source_set_hash": "b" * 64,
        "manifest_ref": "rebuild_manifest_terminal_loader",
    }
    receipt = {
        "schema_version": "kg_rebuild_confirmation_receipt.v1",
        "run_id": rebuild_operation_run_id(**binding),
        **binding,
        "actor_id": "user-1",
        "confirmation_ref": confirmation_fingerprint("opaque-terminal-token"),
        "user_reason": "verified terminal recovery",
        "started_at": "2026-08-15T00:00:00+00:00",
        "receipt_state": "terminal",
    }
    active_key = rebuild_active_confirmation_receipt_key(board_id="b1")
    history_key = rebuild_confirmation_receipt_key(
        board_id="b1",
        run_id=str(receipt["run_id"]),
    )
    store.artifact_store.write_json_atomic(active_key, receipt)

    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_terminal_history_mismatch",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )

    store.artifact_store.write_json_atomic(
        history_key,
        {**receipt, "user_reason": "forged reason"},
    )
    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_terminal_history_mismatch",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )

    store.artifact_store.write_json_atomic(history_key, receipt)
    assert (
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )
        == receipt
    )

    # Crash after history commit but before the active CAS leaves the older
    # authorized active alongside a terminal history. That state is resumable,
    # not corrupt; only terminal active claims require the exact witness.
    authorized = {**receipt, "receipt_state": "authorized"}
    store.artifact_store.write_json_atomic(active_key, authorized)
    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_authorized_history_audit_mismatch",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey

    closed_audit = {
        "run_id": receipt["run_id"],
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "manifest_ref": binding["manifest_ref"],
        "user_reason": authorized["user_reason"],
        "confirmation_ref": authorized["confirmation_ref"],
        "outcome": RebuildOutcome.REBUILD_FAILED.value,
        "reason": RebuildBlockReason.LIFECYCLE_FAILED.value,
        "same_run_resume_allowed": False,
    }
    store.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="run_audit",
            board_id="b1",
            artifact_id=str(receipt["run_id"]),
        ),
        closed_audit,
    )
    assert (
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )
        == authorized
    )

    store.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="run_audit",
            board_id="b1",
            artifact_id=str(receipt["run_id"]),
        ),
        {
            **closed_audit,
            "same_run_resume_allowed": True,
            "operator_action": "emit_terminal_event",
        },
    )
    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_authorized_history_audit_mismatch",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )

    store.artifact_store.write_json_atomic(
        history_key,
        {**receipt, "user_reason": "conflicting terminal history"},
    )
    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_authorized_history_mismatch",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=store.artifact_store,
            board_id="b1",
        )


def test_verified_authorized_active_receipt_wraps_history_read_error() -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint
    from okto_pulse.core.kg.rebuild_service import (
        RebuildConfirmationReceiptIntegrityError,
        load_verified_rebuild_confirmation_receipt,
        rebuild_operation_run_id,
    )

    binding = {
        "board_id": "b1",
        "operation": "rebuild",
        "preflight_hash": "a" * 64,
        "source_set_hash": "b" * 64,
        "manifest_ref": "rebuild_manifest_authorized_history_error",
    }
    active = {
        "schema_version": "kg_rebuild_confirmation_receipt.v1",
        "run_id": rebuild_operation_run_id(**binding),
        **binding,
        "actor_id": "user-1",
        "confirmation_ref": confirmation_fingerprint("opaque-authorized-token"),
        "user_reason": "verified authorized recovery",
        "started_at": "2026-08-15T00:00:00+00:00",
        "receipt_state": "authorized",
    }

    class HistoryReadFails:
        calls = 0

        def list_json_bounded(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
            self.calls += 1
            if self.calls == 1:
                return [active]
            raise RuntimeError("history unreadable")

    with pytest.raises(
        RebuildConfirmationReceiptIntegrityError,
        match="rebuild_confirmation_authorized_history_unverifiable",
    ):
        load_verified_rebuild_confirmation_receipt(
            artifact_store=HistoryReadFails(),  # type: ignore[arg-type]
            board_id="b1",
        )


def test_closed_reconciliation_classifier_requires_clean_or_compensated_f06() -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint
    from okto_pulse.core.kg.rebuild_service import (
        ClosedRebuildReconciliation,
        classify_closed_rebuild_reconciliation,
        rebuild_operation_run_id,
    )

    binding = {
        "board_id": "b1",
        "operation": "rebuild",
        "preflight_hash": "a" * 64,
        "source_set_hash": "b" * 64,
        "manifest_ref": "rebuild_manifest_closed_classifier",
    }
    receipt = {
        "schema_version": "kg_rebuild_confirmation_receipt.v1",
        "run_id": rebuild_operation_run_id(**binding),
        **binding,
        "actor_id": "user-1",
        "confirmation_ref": confirmation_fingerprint("closed-classifier-token"),
        "user_reason": "classify exact closed state",
        "started_at": "2026-08-15T00:00:00+00:00",
        "receipt_state": "authorized",
    }
    audit = {
        "run_id": receipt["run_id"],
        "board_id": receipt["board_id"],
        "actor_id": receipt["actor_id"],
        "operation": receipt["operation"],
        "manifest_ref": receipt["manifest_ref"],
        "user_reason": receipt["user_reason"],
        "confirmation_ref": receipt["confirmation_ref"],
        "outcome": RebuildOutcome.REBUILD_FAILED.value,
        "reason": RebuildBlockReason.LIFECYCLE_FAILED.value,
        "same_run_resume_allowed": False,
        "affected_files": [],
    }

    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit=audit,
            checkpoint=None,
        )
        is ClosedRebuildReconciliation.RECEIPT_ONLY
    )
    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit={**audit, "affected_files": ["quarantine/q1"]},
            checkpoint=None,
        )
        is ClosedRebuildReconciliation.AMBIGUOUS
    )
    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit={**audit, "report_ref": "rebuild/reports/r1.json"},
            checkpoint=None,
        )
        is ClosedRebuildReconciliation.AMBIGUOUS
    )

    f06_run_id = f"f06:{receipt['manifest_ref']}"
    actions = [
        "cancel_enqueued_sources",
        "discard_candidate_generation",
        "restore_quarantine",
    ]
    compensation_key = f"{f06_run_id}:compensate"
    checkpoint = {
        "state": "failed",
        "command": {
            "run_id": f06_run_id,
            "board_id": receipt["board_id"],
            "manifest_ref": receipt["manifest_ref"],
            "operation": receipt["operation"],
            "actor_id": receipt["actor_id"],
            "reason": receipt["user_reason"],
        },
        "compensation_actions": actions,
        "receipts": {
            compensation_key: {
                "effect_key": compensation_key,
                "effect": "compensate",
                "ok": True,
                "code": "compensated",
                "details": {"actions": actions},
            }
        },
    }
    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit=audit,
            checkpoint=checkpoint,
        )
        is ClosedRebuildReconciliation.FULLY_COMPENSATED
    )
    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit=audit,
            checkpoint={**checkpoint, "state": "enqueued"},
        )
        is ClosedRebuildReconciliation.AMBIGUOUS
    )
    failed_receipts = {
        compensation_key: {
            **checkpoint["receipts"][compensation_key],
            "ok": False,
        }
    }
    assert (
        classify_closed_rebuild_reconciliation(
            receipt=receipt,
            audit=audit,
            checkpoint={**checkpoint, "receipts": failed_receipts},
        )
        is ClosedRebuildReconciliation.AMBIGUOUS
    )


@pytest.mark.parametrize("manifest_failure", ("missing", "raises"))
def test_authorized_resume_routes_manifest_failure_to_compensation_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest_failure: str,
) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    observed_steps: list[RebuildStepInput] = []

    def _recovery_step(request: RebuildStepInput) -> RebuildStepResult:
        observed_steps.append(request)
        return RebuildStepResult(
            ok=False,
            detail="manifest_drift:existing checkpoint compensation complete",
        )

    service, manifest_store, confirmation_store, _lock = _build_service(
        tmp_path,
        step_adapter=_recovery_step,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    alive = {"value": True}
    original_consume = confirmation_store.artifact_store.consume_json_with_receipt

    def _consume_then_expire(**kwargs):
        result = original_consume(**kwargs)
        alive["value"] = False
        return result

    monkeypatch.setattr(
        confirmation_store.artifact_store,
        "consume_json_with_receipt",
        _consume_then_expire,
    )
    common = {
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "resume must compensate the old attempt",
    }
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: alive["value"],
    ) as first_capability:
        stopped = KGRebuildService.run(
            service,
            confirmation_id=confirmation_id,
            recovery_capability=first_capability,
            **common,
        )
    assert stopped.reason == RebuildBlockReason.LEASE_LOST.value

    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    receipt = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )[0]
    # A transient audit is not itself an irreversible terminal effect.  In
    # particular, writer/capability loss before report, promotion, or event
    # publication must not suppress governed compensation after source drift.
    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey

    service.artifact_store.write_json_atomic(
        RebuildAuditKey(
            namespace="run_audit",
            board_id="b1",
            artifact_id=str(receipt["run_id"]),
        ),
        {
            "run_id": receipt["run_id"],
            "board_id": "b1",
            "actor_id": "user-1",
            "operation": "rebuild",
            "manifest_ref": manifest_ref,
            "user_reason": common["reason"],
            "confirmation_ref": receipt["confirmation_ref"],
            "outcome": RebuildOutcome.REBUILD_FAILED.value,
            "reason": RebuildBlockReason.LEASE_LOST.value,
            "same_run_resume_allowed": True,
            "resume_phase": "reacquire_writer_and_resume",
            "operator_action": "reacquire_writer_and_resume",
            "report_ref": None,
            "current_kg_generation_id": None,
            "event_emitted": False,
        },
    )
    alive["value"] = True
    if manifest_failure == "missing":

        def load_manifest(_ref):
            return None

    else:

        def load_manifest(_ref):
            raise OSError("manifest unreadable")

    def _unexpected_enumeration(*, board_id: str):
        pytest.fail(f"resume compensation enumerated live sources for {board_id}")

    service = replace(
        service,
        manifest_store=SimpleNamespace(load=load_manifest),
        source_enumerator=SimpleNamespace(enumerate=_unexpected_enumeration),
    )
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: alive["value"],
    ) as resume_capability:
        result = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=str(receipt["run_id"]),
            recovery_capability=resume_capability,
            **common,
        )

    assert result.outcome == RebuildOutcome.MANIFEST_DRIFT.value
    assert len(observed_steps) == 1
    request = observed_steps[0]
    assert request.recovery_failure_code == RebuildOutcome.MANIFEST_DRIFT.value
    assert request.recovery_failure_detail is not None
    assert (
        "manifest_missing"
        if manifest_failure == "missing"
        else "manifest_load_exception"
    ) in request.recovery_failure_detail
    assert request.source_set_hash == receipt["source_set_hash"]


def test_run_completes_when_confirmation_lock_lifecycle_all_succeed(tmp_path: Path):
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="operator-initiated rebuild",
    )
    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.reason == RebuildBlockReason.OK.value
    assert result.run_id.startswith("run_")
    assert Path(result.audit_ref).exists()
    # Lock was released after the run.
    assert lock.inspect(board_id="b1") is None
    # Counter bumped.
    assert get_rebuild_run_count("b1", RebuildOutcome.COMPLETED.value) == 1


def test_nonterminal_legacy_rebaseline_completes_and_records_once(tmp_path: Path):
    from dataclasses import replace

    from okto_pulse.core.kg.rebuild_sources import (
        _compose_source_set_hash_v2,
        get_spec_manifest_rebaseline_count,
        read_spec_manifest_rebaseline_audit,
        reset_spec_manifest_rebaseline_counter,
    )

    board_id = "b-legacy-service"
    source_rows = [
        {
            "artifact_type": "spec",
            "id": "legacy-spec",
            "source_ref": "spec:legacy-spec",
            "source_version": "1",
            "content_hash": "hash-v3",
            "content_hash_v1": "hash-v1",
            "content_hash_v2": "hash-v2",
            "created_at": "2026-05-01T00:00:00+00:00",
            "status": "done",
        }
    ]
    step_calls = 0
    step_requests = []

    def _step(request):
        nonlocal step_calls
        step_calls += 1
        step_requests.append(request)
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        source_rows=source_rows,
        step_adapter=_step,
    )
    source_set = service.source_enumerator.enumerate(board_id=board_id)
    current = manifest_store.build(
        source_set=source_set,
        preflight_hash="a" * 64,
    )

    def _legacy_rows(rows):
        return tuple(
            replace(
                row,
                content_hash=(row.content_hash_v2 or row.content_hash),
                content_hash_v1="",
                content_hash_v2="",
            )
            for row in rows
        )

    legacy = replace(
        current,
        manifest_schema_version=2,
        source_set_hash=_compose_source_set_hash_v2(source_set),
        sources=_legacy_rows(current.sources),
        working_sources=_legacy_rows(current.working_sources),
        skipped_by_maturity=_legacy_rows(current.skipped_by_maturity),
        skipped_expired_working=_legacy_rows(current.skipped_expired_working),
        legacy_unknown=_legacy_rows(current.legacy_unknown),
        payload_digest="",
    )
    manifest_store.artifact_store.write_json_atomic(
        manifest_store._manifest_key(legacy.manifest_ref),
        legacy.to_dict(),
    )
    token = confirmation_store.issue(
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
    )
    reset_spec_manifest_rebaseline_counter()

    result = service.run(
        confirmation_id=token.confirmation_id,
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
        reason="governed legacy rebaseline",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert step_calls == 1
    assert len(step_requests) == 1
    projected = step_requests[0].rebaseline_source_rows
    assert projected is not None
    assert projected[0]["content_hash"] == "hash-v3"
    assert projected[0]["_rebuild_rebaseline_evidence_id"] == (
        step_requests[0].rebaseline_evidence_id
    )
    assert step_requests[0].rebaseline_target_source_set_hash
    assert len(step_requests[0].rebaseline_target_source_set_hash) == 64
    records = read_spec_manifest_rebaseline_audit(tmp_path, board_id)
    assert len(records) == 1
    assert records[0]["manifest_ref"] == legacy.manifest_ref
    assert records[0]["evidence_id"] == f"{result.run_id}:{legacy.manifest_ref}"
    assert get_spec_manifest_rebaseline_count(board_id) == 1
    assert lock.inspect(board_id=board_id) is None

    # A terminal replay classifies the legacy manifest purely and returns the
    # frozen/closed decision before the governed evidence writer or rebuild
    # step can run again.
    with issue_recovery_execution_capability(
        board_id=board_id,
        lifetime_probe=lambda: True,
    ) as capability:
        replay = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=result.run_id,
            recovery_capability=capability,
            board_id=board_id,
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=legacy.preflight_hash,
            manifest_ref=legacy.manifest_ref,
            reason="governed legacy rebaseline",
        )
    assert replay.outcome == result.outcome
    assert step_calls == 1
    assert len(read_spec_manifest_rebaseline_audit(tmp_path, board_id)) == 1
    assert get_spec_manifest_rebaseline_count(board_id) == 1


def test_legacy_rebaseline_blocks_v3_only_drift_after_pre_step(tmp_path: Path):
    from dataclasses import replace

    from okto_pulse.core.kg.rebuild_sources import _compose_source_set_hash_v2

    board_id = "b-legacy-v3-drift"

    def _source(content_hash: str):
        return {
            "artifact_type": "spec",
            "id": "legacy-spec",
            "source_ref": "spec:legacy-spec",
            "source_version": "1",
            "content_hash": content_hash,
            # The v2 projection is unchanged: only a v3-bound field moved.
            "content_hash_v1": "hash-v1",
            "content_hash_v2": "hash-v2-stable",
            "created_at": "2026-05-01T00:00:00+00:00",
            "status": "done",
        }

    initial_rows = [_source("hash-v3-before")]
    changed_rows = [_source("hash-v3-after")]
    inner_revalidations: list[bool] = []

    def _step(request):
        assert request.source_revalidate is not None
        unchanged = request.source_revalidate()
        inner_revalidations.append(unchanged)
        return RebuildStepResult(
            ok=unchanged,
            detail=None if unchanged else "v3 projection drift",
        )

    service, manifest_store, confirmation_store, _lock = _build_service(
        tmp_path,
        source_rows=initial_rows,
        step_adapter=_step,
    )
    initial_set = service.source_enumerator.enumerate(board_id=board_id)
    changed_set = RebuildSourceEnumerator(
        source_store=lambda _board: changed_rows
    ).enumerate(board_id=board_id)
    current = manifest_store.build(
        source_set=initial_set,
        preflight_hash="b" * 64,
    )

    def _legacy_rows(rows):
        return tuple(
            replace(
                row,
                content_hash=(row.content_hash_v2 or row.content_hash),
                content_hash_v1="",
                content_hash_v2="",
            )
            for row in rows
        )

    legacy = replace(
        current,
        manifest_schema_version=2,
        source_set_hash=_compose_source_set_hash_v2(initial_set),
        sources=_legacy_rows(current.sources),
        working_sources=_legacy_rows(current.working_sources),
        skipped_by_maturity=_legacy_rows(current.skipped_by_maturity),
        skipped_expired_working=_legacy_rows(current.skipped_expired_working),
        legacy_unknown=_legacy_rows(current.legacy_unknown),
        payload_digest="",
    )
    manifest_store.artifact_store.write_json_atomic(
        manifest_store._manifest_key(legacy.manifest_ref),
        legacy.to_dict(),
    )

    class _SequencedEnumerator:
        def __init__(self):
            self.calls = 0

        def enumerate(self, *, board_id: str):
            assert board_id == "b-legacy-v3-drift"
            self.calls += 1
            return initial_set if self.calls <= 2 else changed_set

    service = replace(service, source_enumerator=_SequencedEnumerator())
    token = confirmation_store.issue(
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
    )

    result = service.run(
        confirmation_id=token.confirmation_id,
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
        reason="prove v3 target cut",
    )

    assert inner_revalidations == [False]
    assert result.outcome != RebuildOutcome.COMPLETED.value
    assert result.current_kg_generation_id is None


def test_audit_trail_has_TR12_required_fields(tmp_path: Path):
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="initial cut",
    )
    body = json.loads(Path(result.audit_ref).read_text(encoding="utf-8"))
    # TR12 required fields. val_8fa8019d rework: confirmation_id MUST
    # NOT be persisted raw — replaced by confirmation_ref (SHA256
    # fingerprint).
    for field in (
        "run_id",
        "outcome",
        "reason",
        "board_id",
        "actor_id",
        "operation",
        "confirmation_ref",
        "manifest_ref",
        "user_reason",
        "started_at",
        "finished_at",
        "affected_files",
        "previous_kg_generation_id",
        "current_kg_generation_id",
    ):
        assert field in body, f"audit missing TR12 field {field}"
    assert "confirmation_id" not in body, (
        "raw confirmation_id leaked into legacy run audit (val_8fa8019d)"
    )
    assert body["board_id"] == "b1"
    assert body["actor_id"] == "user-1"
    assert body["user_reason"] == "initial cut"
    assert body["confirmation_ref"].startswith("conf_fp_")


# --- Confirmation invalid paths --------------------------------------------


def test_run_refuses_when_confirmation_missing(tmp_path: Path):
    service, manifest_store, _cs, lock = _build_service(tmp_path)
    preflight_hash = "a" * 64
    manifest = manifest_store.build(
        source_set=service.source_enumerator.enumerate(board_id="b1"),
        preflight_hash=preflight_hash,
    )
    result = service.run(
        confirmation_id="conf_does_not_exist",
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest.manifest_ref,
        reason="r",
    )
    assert result.outcome == RebuildOutcome.CONFIRMATION_REQUIRED.value
    assert result.reason == RebuildBlockReason.CONFIRMATION_INVALID.value
    assert result.audit_ref == ""
    # Lock NEVER acquired — no mutation.
    assert lock.inspect(board_id="b1") is None


def test_run_refuses_when_confirmation_replayed(tmp_path: Path):
    """Second consume of same token MUST be rejected as confirmation_required."""
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    first = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="first",
    )
    assert first.outcome == RebuildOutcome.COMPLETED.value

    second = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="second",
    )
    assert second.outcome == RebuildOutcome.CONFIRMATION_REQUIRED.value


# --- Manifest drift ---------------------------------------------------------


def test_run_aborts_on_manifest_drift(tmp_path: Path):
    """Source drift is detected under reservation+writer, before the step."""
    from dataclasses import replace

    initial_rows = [_row()]
    step_calls: list[str] = []
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        source_rows=initial_rows,
        step_adapter=lambda _req: (
            step_calls.append("step") or RebuildStepResult(ok=True)
        ),
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    reservation = KGAdministrativeOperationReservation(
        write_lock_port=lock.bind_write_lock_port()
    )

    class _FencedEnumerator:
        def enumerate(self, *, board_id: str):
            assert lock.inspect(board_id=board_id) is not None
            assert reservation.inspect(board_id=board_id) is not None
            return enumerator.enumerate(board_id=board_id)

    service = replace(
        service,
        source_enumerator=_FencedEnumerator(),
        operation_reservation=reservation,
    )
    # Mutate the source store — simulates spec added between preflight and run.
    initial_rows.append(
        {
            "artifact_type": "spec",
            "id": "id-2",
            "source_ref": "ref:id-2",
            "source_version": "v1",
            "content_hash": "h2",
            "created_at": "2026-05-02T00:00:00Z",
            "status": "validated",
        }
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="drift test",
    )
    assert result.outcome == RebuildOutcome.MANIFEST_DRIFT.value
    assert result.reason == RebuildBlockReason.MANIFEST_DRIFT.value
    assert step_calls == []
    # Both fences are released after the audit-only drift result.
    assert lock.inspect(board_id="b1") is None
    assert reservation.inspect(board_id="b1") is None


def test_run_revalidates_after_drain_before_lifecycle_or_promotion(
    tmp_path: Path,
) -> None:
    """A relational mutation during writer delegation cannot promote stale KG."""
    from dataclasses import replace

    source_rows = [_row()]
    lifecycle_calls: list[str] = []

    def _mutating_step(req: RebuildStepInput) -> RebuildStepResult:
        source_rows.append(
            {
                "artifact_type": "spec",
                "id": "id-during-drain",
                "source_ref": "ref:id-during-drain",
                "source_version": "v1",
                "content_hash": "drift-during-drain",
                "created_at": "2026-05-03T00:00:00Z",
                "status": "validated",
            }
        )
        assert req.source_revalidate is not None
        assert req.source_revalidate() is False
        return RebuildStepResult(
            ok=False,
            detail="manifest_drift:source_set_hash drift during rebuild drain",
            current_kg_generation_id=str(uuid.uuid4()),
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1},
        )

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        source_rows=source_rows,
        step_adapter=_mutating_step,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    original_lifecycle = service.safe_write_lifecycle

    class _TrackingLifecycle:
        def apply(self, **kwargs):
            lifecycle_calls.append("apply")
            return original_lifecycle.apply(**kwargs)

    service = replace(service, safe_write_lifecycle=_TrackingLifecycle())
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="mutate relational source during writer-free drain",
    )

    assert result.outcome == RebuildOutcome.MANIFEST_DRIFT.value
    assert result.reason == RebuildBlockReason.MANIFEST_DRIFT.value
    assert result.current_kg_generation_id is None
    assert result.report_ref is None
    assert lifecycle_calls == []
    assert lock.inspect(board_id="b1") is None


# --- Lock contention --------------------------------------------------------


def test_run_returns_lock_contention_when_admin_lane_blocked(tmp_path: Path):
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    # Acquire the lock with another owner first.
    pre = lock.acquire(
        board_id="b1",
        operation="other",
        owner_id="other-actor",
        ttl_seconds=60,
    )
    assert pre.acquired is True

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="lock test",
    )
    assert result.outcome == RebuildOutcome.LOCK_CONTENTION.value
    assert result.reason == RebuildBlockReason.LOCK_CONTENTION.value
    assert result.audit_ref == ""
    # The pre-existing lock is still held by the other owner.
    manifest = lock.inspect(board_id="b1")
    assert manifest is not None
    assert manifest.owner_id == "other-actor"


# --- Step exception / failure ----------------------------------------------


def test_run_returns_rebuild_failed_when_step_raises(tmp_path: Path):
    def boom(req: RebuildStepInput) -> RebuildStepResult:
        raise RuntimeError("structural rebuild blew up")

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=boom,
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="boom test",
    )
    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.STEP_EXCEPTION.value
    # Lock released even on failure.
    assert lock.inspect(board_id="b1") is None


def test_run_returns_failed_when_step_returns_ok_false(tmp_path: Path):
    def fail(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(ok=False, detail="step refused")

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=fail,
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="ok=false test",
    )
    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.reason == RebuildBlockReason.LIFECYCLE_FAILED.value
    assert lock.inspect(board_id="b1") is None


def test_run_returns_failed_when_safe_lifecycle_step_fails(tmp_path: Path):
    """Step OK but the lifecycle's flush step returns False → FAILED.
    Lock still released; lifecycle is the boundary KG-01.3 enforces."""
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        lifecycle_step_ok=False,
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="lifecycle fail",
    )
    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.reason == RebuildBlockReason.LIFECYCLE_FAILED.value
    assert lock.inspect(board_id="b1") is None


# --- AC7: admin lane semantics ---------------------------------------------


def test_run_acquires_lock_with_admin_lane_true(tmp_path: Path):
    """AC9 / TR7: rebuild acquires the lock under admin_lane=True."""
    observed = {}

    def step(req: RebuildStepInput) -> RebuildStepResult:
        # Snapshot lock state during the step.
        from okto_pulse.core.kg.rebuild_service import logger  # noqa

        observed["manifest_during_step"] = service.single_writer_lock.inspect(
            board_id=req.board_id
        )
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=step,
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="admin lane check",
    )
    captured = observed["manifest_during_step"]
    assert captured is not None
    assert captured.admin_lane is True
    assert captured.operation == "kg02_rebuild:rebuild"


def test_run_forwards_cancellation_probe_and_lease_renewal(tmp_path: Path):
    observed = {}

    def cancel_requested() -> bool:
        return False

    def step(req: RebuildStepInput) -> RebuildStepResult:
        observed["cancel_requested"] = req.cancel_requested
        observed["lease_renewed"] = req.lease_renew()
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=step,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="cooperative controls",
        cancel_requested=cancel_requested,
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert observed["cancel_requested"] is cancel_requested
    assert observed["lease_renewed"] is True
    assert lock.inspect(board_id="b1") is None


def test_run_renews_lease_while_blocking_step_executes(
    tmp_path: Path,
    monkeypatch,
):
    from dataclasses import replace
    from threading import Event, Lock

    heartbeat_observed = Event()
    count_lock = Lock()
    renew_count = 0

    def step(_req: RebuildStepInput) -> RebuildStepResult:
        assert heartbeat_observed.wait(1.0)
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=step,
    )
    original_renew = lock.renew

    def renew_spy(**kwargs) -> bool:  # noqa: ANN003
        nonlocal renew_count
        renewed = original_renew(**kwargs)
        with count_lock:
            renew_count += 1
            heartbeat_observed.set()
        return renewed

    monkeypatch.setattr(lock, "renew", renew_spy)
    service = replace(service, lease_heartbeat_interval_seconds=0.01)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="heartbeat during blocking step",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert renew_count >= 1
    assert lock.inspect(board_id="b1") is None


def test_run_binds_runtime_write_lock_port_for_raw_heartbeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from dataclasses import replace
    from threading import Event, current_thread

    from okto_pulse.core.ports.coordination import register_coordination_providers
    from okto_pulse.core.runtime_context import (
        runtime_value_scope,
        snapshot_runtime_values,
    )

    heartbeat_observed = Event()
    write_lock_port = FakeWriteLockPort()
    original_inspect = write_lock_port.inspect_single_writer_sync

    def inspect_spy(**kwargs):  # noqa: ANN003
        if current_thread().name.startswith("kg-rebuild-lease:"):
            heartbeat_observed.set()
        return original_inspect(**kwargs)

    monkeypatch.setattr(write_lock_port, "inspect_single_writer_sync", inspect_spy)

    def step(_req: RebuildStepInput) -> RebuildStepResult:
        assert heartbeat_observed.wait(1.0)
        return RebuildStepResult(ok=True)

    with runtime_value_scope(snapshot_runtime_values()):
        register_coordination_providers(write_lock_port=write_lock_port)
        runtime_lock = KGSingleWriterLock(base_dir=tmp_path / "runtime-locks")
        service, manifest_store, confirmation_store, lock = _build_service(
            tmp_path,
            step_adapter=step,
            single_writer_lock=runtime_lock,
        )
        service = replace(service, lease_heartbeat_interval_seconds=0.01)
        confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
            confirmation_store,
            manifest_store,
            service.source_enumerator,
        )

        result = service.run(
            confirmation_id=confirmation_id,
            board_id="b1",
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason="raw-thread lease renewal",
        )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert heartbeat_observed.is_set()
    assert lock is runtime_lock


def test_run_delegates_admin_lease_to_real_worker_guard_and_rebinds_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace
    from time import sleep

    from okto_pulse.core.kg.guarded_write import (
        GuardedWriteError,
        guarded_board_write,
    )
    from okto_pulse.core.kg.write_barrier import (
        BarrierMode,
        WriteLifecycleViolation,
        get_barrier_mode,
        require_write_token,
        set_barrier_mode,
    )

    write_lock_port = FakeWriteLockPort()
    lock = KGSingleWriterLock(
        base_dir=tmp_path / "shared-locks",
        write_lock_port=write_lock_port,
    )
    released_tokens: list[str] = []
    renew_events: list[tuple[str, bool]] = []
    drain_window = False
    stale_admin_renewed = False
    original_release = lock.release
    original_renew = lock.renew

    def release_spy(*, board_id: str, owner_token: str) -> bool:
        released_tokens.append(owner_token)
        return original_release(board_id=board_id, owner_token=owner_token)

    def renew_spy(*, board_id: str, owner_token: str, ttl_seconds: int) -> bool:
        nonlocal stale_admin_renewed
        if drain_window and owner_token == initial_token:
            stale_admin_renewed = True
        renewed = original_renew(
            board_id=board_id,
            owner_token=owner_token,
            ttl_seconds=ttl_seconds,
        )
        renew_events.append((owner_token, renewed))
        return renewed

    monkeypatch.setattr(lock, "release", release_spy)
    monkeypatch.setattr(lock, "renew", renew_spy)

    worker_lifecycle = KGSafeWriteLifecycle(
        step_adapter=lambda _board, _graph, _step: LifecycleStepResult(ok=True),
        owner_probe=LockOwnerProbe(is_active_owner=lock.is_owner),
    )
    initial_token = ""
    reacquired_token = ""

    def step(req: RebuildStepInput) -> RebuildStepResult:
        nonlocal drain_window, initial_token, reacquired_token
        assert req.release_writer_for_drain is not None
        assert req.reacquire_writer_after_drain is not None
        initial_token = req.owner_token

        with pytest.raises(GuardedWriteError, match="another writer"):
            with guarded_board_write(
                req.board_id,
                operation="consolidation_commit",
                owner_id="normal-worker-before-handoff",
                mutation_ref="worker-before-handoff",
                writer_lock=lock,
                lifecycle=worker_lifecycle,
            ):
                pass

        assert req.release_writer_for_drain()
        drain_window = True
        peer_admin = KGAdministrativeOperationReservation(
            base_dir=tmp_path / "shared-locks",
            write_lock_port=write_lock_port,
        ).acquire(
            board_id=req.board_id,
            operation="competing-board-erasure",
            owner_id="peer-admin",
            ttl_seconds=60,
            admin_lane=True,
        )
        assert peer_admin.acquired is False
        with pytest.raises(WriteLifecycleViolation, match="no_active_guard"):
            require_write_token(
                req.board_id,
                expected_owner_token=initial_token,
            )

        # Longer than the configured heartbeat interval: token A must stay
        # detached rather than being renewed/poisoned during the worker drain.
        sleep(0.05)
        with guarded_board_write(
            req.board_id,
            operation="consolidation_commit",
            owner_id="normal-worker",
            mutation_ref="worker-drain",
            writer_lock=lock,
            lifecycle=worker_lifecycle,
        ) as worker_lease:
            require_write_token(
                req.board_id,
                expected_owner_token=worker_lease.owner_token,
            )
            worker_lease.ensure_durable()

        reacquired_token = req.reacquire_writer_after_drain() or ""
        drain_window = False
        assert reacquired_token
        assert reacquired_token != initial_token
        require_write_token(
            req.board_id,
            expected_owner_token=reacquired_token,
        )
        with pytest.raises(WriteLifecycleViolation, match="owner_token_mismatch"):
            require_write_token(
                req.board_id,
                expected_owner_token=initial_token,
            )
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, _ = _build_service(
        tmp_path,
        step_adapter=step,
        single_writer_lock=lock,
    )
    service = replace(
        service,
        lease_heartbeat_interval_seconds=0.01,
        lease_reacquire_timeout_seconds=1.0,
        lease_reacquire_poll_interval_seconds=0.001,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    previous_mode = get_barrier_mode()
    set_barrier_mode(BarrierMode.STRICT)
    try:
        result = service.run(
            confirmation_id=confirmation_id,
            board_id="b1",
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason="exercise real worker handoff",
        )
    finally:
        set_barrier_mode(previous_mode)

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert stale_admin_renewed is False
    assert released_tokens.count(initial_token) == 1
    assert released_tokens.count(reacquired_token) == 1
    assert lock.inspect(board_id="b1") is None
    assert any(token == reacquired_token and ok for token, ok in renew_events)


def test_run_waits_for_inflight_worker_before_initial_admin_writer(
    tmp_path: Path,
) -> None:
    from dataclasses import replace
    from threading import Thread
    from time import sleep

    write_lock_port = FakeWriteLockPort()
    lock = KGSingleWriterLock(
        base_dir=tmp_path / "initial-retry-locks",
        write_lock_port=write_lock_port,
    )
    worker = lock.acquire(
        board_id="b1",
        operation="consolidation_commit",
        owner_id="worker",
        ttl_seconds=60,
    )
    assert worker.acquired and worker.owner_token

    def _release_worker() -> None:
        sleep(0.05)
        assert lock.release(board_id="b1", owner_token=worker.owner_token or "")

    releaser = Thread(target=_release_worker, daemon=True)
    releaser.start()
    service, manifest_store, confirmation_store, _ = _build_service(
        tmp_path,
        single_writer_lock=lock,
    )
    service = replace(
        service,
        lease_reacquire_timeout_seconds=1.0,
        lease_reacquire_poll_interval_seconds=0.005,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="wait for ordinary writer",
    )
    releaser.join(timeout=1)

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert lock.inspect(board_id="b1") is None


def test_initial_writer_timeout_releases_administrative_reservation(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    write_lock_port = FakeWriteLockPort()
    lock = KGSingleWriterLock(
        base_dir=tmp_path / "initial-timeout-locks",
        write_lock_port=write_lock_port,
    )
    worker = lock.acquire(
        board_id="b1",
        operation="consolidation_commit",
        owner_id="worker",
        ttl_seconds=60,
    )
    assert worker.acquired and worker.owner_token
    service, manifest_store, confirmation_store, _ = _build_service(
        tmp_path,
        single_writer_lock=lock,
    )
    service = replace(
        service,
        lease_reacquire_timeout_seconds=0.02,
        lease_reacquire_poll_interval_seconds=0.005,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="bounded initial contention",
    )

    assert result.outcome == RebuildOutcome.LOCK_CONTENTION.value
    peer_reservation = KGAdministrativeOperationReservation(
        base_dir=tmp_path / "initial-timeout-locks",
        write_lock_port=write_lock_port,
    ).acquire(
        board_id="b1",
        operation="peer-admin",
        owner_id="peer",
        ttl_seconds=60,
        admin_lane=True,
    )
    assert peer_reservation.acquired and peer_reservation.owner_token
    assert lock.release(board_id="b1", owner_token=worker.owner_token or "")


# --- OR or_37cebd03 counter labels ------------------------------------------


def test_counter_labels_match_or_shape():
    assert get_rebuild_run_counter_labels() == ("board_id", "status", "reason")


def test_counter_bumps_per_outcome(tmp_path: Path):
    # Happy run — actor_id MUST match the issuer (default "user-1").
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="r",
    )
    # Invalid confirmation.
    invalid_preflight_hash = "b" * 64
    invalid_manifest = manifest_store.build(
        source_set=enumerator.enumerate(board_id="b1"),
        preflight_hash=invalid_preflight_hash,
    )
    service.run(
        confirmation_id="conf_nope",
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=invalid_preflight_hash,
        manifest_ref=invalid_manifest.manifest_ref,
        reason="r",
    )
    samples = get_rebuild_run_samples()
    statuses = {s["status"] for s in samples}
    assert RebuildOutcome.COMPLETED.value in statuses
    assert RebuildOutcome.CONFIRMATION_REQUIRED.value in statuses
    for s in samples:
        for label in get_rebuild_run_counter_labels():
            assert label in s and isinstance(s[label], str) and s[label]


# --- Endpoint integration ---------------------------------------------------


# --- val_dfdff0b8 fail-closed: non-rebuild operations -----------------------


def test_run_returns_unsupported_operation_for_reset_before_lock(tmp_path: Path):
    """val_dfdff0b8: reset (and other non-rebuild ops) MUST fail-closed
    BEFORE the lock is taken or the step adapter runs. No silent
    completed=True via stub adapter."""
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator, operation="rebuild"
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="reset",  # NOT in SUPPORTED_REBUILD_OPERATIONS
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="should-be-rejected",
    )
    assert result.outcome == RebuildOutcome.UNSUPPORTED_OPERATION.value
    assert result.reason == RebuildBlockReason.OPERATION_PENDING_KG02_4.value
    # Lock NEVER acquired.
    assert lock.inspect(board_id="b1") is None
    # No affected_files, no current_kg_generation_id.
    assert result.affected_files == ()
    assert result.current_kg_generation_id is None


def test_run_unsupported_operation_does_not_consume_confirmation(tmp_path: Path):
    """Fail-closed happens BEFORE consume — the operator can retry with
    the correct operation using the same confirmation token (within TTL)."""
    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator, operation="rebuild"
    )
    # Try with wrong operation first.
    bad = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="quarantine",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="bad-op",
    )
    assert bad.outcome == RebuildOutcome.UNSUPPORTED_OPERATION.value

    # Token still usable for the supported operation.
    ok = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="retry-correct",
    )
    assert ok.outcome == RebuildOutcome.COMPLETED.value


@pytest.mark.parametrize(
    "operation",
    ["reset", "quarantine", "promote", "rollback", "reindex_discovery"],
)
def test_confirm_endpoint_rejects_non_rebuild_operations(
    tmp_path: Path, operation: str, monkeypatch
):
    """val_dfdff0b8: /confirm refuses to issue a token for any
    canonical operation other than 'rebuild' until KG-02.4 wires the
    full reset/quarantine/promote/rollback/reindex paths."""
    import okto_pulse.community.api.kg_rebuild as kg_rebuild_mod
    from fastapi.testclient import TestClient

    from okto_pulse.community.api.auth_deps import require_user

    async def _fake_health(board_id, db, scheduler_control=None):
        return {
            "graph_state": "healthy",
            "metric_status": "available",
            "current_kg_generation_id": None,
        }

    monkeypatch.setattr(kg_rebuild_mod, "get_kg_health", _fake_health)
    monkeypatch.setattr(
        kg_rebuild_mod,
        "_build_source_store",
        lambda: lambda _board_id: [_row()],
    )

    app = _make_rebuild_test_app(board_id="b-reset")

    async def _fake_user():
        return "u"

    app.dependency_overrides[require_user] = _fake_user

    with TestClient(app) as client:
        pre = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-reset"},
        )
        assert pre.status_code == 200
        preflight_hash = pre.json()["preflight_hash"]

        conf = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b-reset",
                "operation": operation,
                "preflight_hash": preflight_hash,
                "manifest_ref": "diagnostic-only",
            },
        )
    assert conf.status_code == 400, conf.text
    detail = conf.json()["detail"]
    assert detail["error"] == "operation_pending_implementation"
    assert operation in detail["reason"]


def test_post_rebuild_run_endpoint_is_registered_and_callable(
    tmp_path: Path, monkeypatch
):
    import okto_pulse.community.api.kg_rebuild as kg_rebuild_mod
    from fastapi.testclient import TestClient

    from okto_pulse.community.api.auth_deps import require_user

    async def _fake_health(board_id, db, scheduler_control=None):
        return {
            "graph_state": "healthy",
            "metric_status": "available",
            "current_kg_generation_id": None,
        }

    monkeypatch.setattr(kg_rebuild_mod, "get_kg_health", _fake_health)
    monkeypatch.setattr(
        kg_rebuild_mod,
        "_build_source_store",
        lambda: lambda _board_id: [_row()],
    )

    app = _make_rebuild_test_app(board_id="b-endpoint")
    paths = set(app.openapi()["paths"])
    assert "/api/v1/kg/rebuild/run" in paths

    async def _fake_user():
        return "user-run-test"

    app.dependency_overrides[require_user] = _fake_user

    with TestClient(app) as client:
        pre = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-endpoint"},
        )
        assert pre.status_code == 200
        preflight_hash = pre.json()["preflight_hash"]
        assert pre.json()["manifest_ref"] is None
        assert pre.json()["source_set_hash"] is None
        assert pre.json()["operator_action"] == (
            "run_local_offline_kg_recovery_executor"
        )

        conf = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b-endpoint",
                "operation": "rebuild",
                "preflight_hash": preflight_hash,
                "manifest_ref": "diagnostic-only",
            },
        )
        run = client.post(
            "/api/v1/kg/rebuild/run",
            json={
                "confirmation_id": "legacy-confirmation",
                "board_id": "b-endpoint",
                "operation": "rebuild",
                "preflight_hash": preflight_hash,
                "manifest_ref": "diagnostic-only",
                "reason": "endpoint integration smoke",
            },
        )

    for response in (conf, run):
        assert response.status_code == 409, response.text
        detail = response.json()["detail"]
        assert detail["error"] == "recovery_execution_required"
        assert detail["operator_action"] == ("run_local_offline_kg_recovery_executor")

    openapi = app.openapi()["paths"]
    for path in ("/api/v1/kg/rebuild/confirm", "/api/v1/kg/rebuild/run"):
        operation_schema = openapi[path]["post"]
        request_schema = operation_schema["requestBody"]["content"]["application/json"][
            "schema"
        ]
        assert "recovery_capability" not in json.dumps(request_schema)
        assert "200" not in operation_schema["responses"]
        assert "409" in operation_schema["responses"]


# --- KG-02.4 — Report-first terminal state + generation promotion ----------
#
# Wires the four KG-02.4 primitives (generation_repository,
# promotion_guard, report_store, terminal_state_guard) into the service
# and proves:
#   * TR16 / br_82deef11 — report persisted BEFORE promotion + audit.
#   * IR ir_6d092147 — promotion is impossible without a durable report.
#   * TR8 — kg.rebuilt event emitted with the canonical payload.
#   * br_82deef11 — when persistence fails, previous generation stays.


def _build_service_with_kg024(
    tmp_path: Path,
    *,
    step_adapter=None,
    report_store=None,
    event_sink=None,
    orphan_scan_provider=None,
    source_rows=None,
):
    from okto_pulse.core.kg.rebuild_generation import (
        KGGenerationPromotionGuard,
        KGGenerationRepository,
    )
    from okto_pulse.core.kg.rebuild_report import (
        RebuildReportStore,
        RebuildReportTerminalStateGuard,
    )

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=step_adapter,
        source_rows=source_rows,
    )
    generation_repo = KGGenerationRepository(base_dir=tmp_path)
    rep_store = report_store or RebuildReportStore(base_dir=tmp_path)
    event_log: list[dict] = [] if event_sink is None else event_sink

    def _emit(payload):
        event_log.append(payload)
        return True

    # KGRebuildService is frozen — rebuild via dataclasses.replace.
    from dataclasses import replace

    enriched = replace(
        service,
        generation_repository=generation_repo,
        promotion_guard=KGGenerationPromotionGuard,
        report_store=rep_store,
        terminal_state_guard=RebuildReportTerminalStateGuard,
        event_emitter=_emit,
        orphan_scan_provider=orphan_scan_provider,
    )
    return (
        enriched,
        manifest_store,
        confirmation_store,
        lock,
        generation_repo,
        rep_store,
        event_log,
    )


def test_completed_run_persists_report_and_promotes_generation(tmp_path: Path):
    from okto_pulse.core.kg.rebuild_report import (
        get_persist_count,
        get_report_count,
        get_terminal_count,
        ReportPersistOutcome,
    )

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 7, "edges": 3},
            drilldown={"specs": [{"id": "spec-1", "status": "ok"}]},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        lock,
        gen_repo,
        rep_store,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    assert gen_repo.get_current("b1") is None
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="report-first run",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.report_ref is not None
    assert Path(result.report_ref).exists()
    assert result.publishable_status == "completed"
    assert result.promotion_outcome == "promoted"
    assert result.current_kg_generation_id is not None
    assert result.event_emitted is True
    # Pointer advanced.
    assert gen_repo.get_current("b1") == result.current_kg_generation_id
    # Counters bumped.
    assert get_persist_count("b1", outcome=ReportPersistOutcome.STORED.value) == 1
    assert get_report_count("b1", event="created") == 1
    assert (
        get_terminal_count(
            "b1",
            candidate_terminal_status="completed",
            publishable_status="completed",
            with_report_ref=True,
        )
        == 1
    )
    # Audit row carries report_ref + publishable_status (TR8/TR16).
    audit_body = json.loads(Path(result.audit_ref).read_text(encoding="utf-8"))
    assert audit_body["report_ref"] == result.report_ref
    assert audit_body["publishable_status"] == "completed"
    assert audit_body["current_kg_generation_id"] == result.current_kg_generation_id
    assert audit_body["same_run_resume_allowed"] is False
    assert audit_body["resume_phase"] is None
    # kg.rebuilt event captured.
    assert events, "kg.rebuilt event was not emitted"
    event = events[0]
    assert event["event"] == "kg.rebuilt"
    assert event["board_id"] == "b1"
    assert event["previous_kg_generation_id"] is None
    assert event["kg_generation_id"] == result.current_kg_generation_id
    assert event["report_ref"] == result.report_ref
    assert event["status"] == "completed"
    assert event["triggered_by"] == "user-1"


def test_legacy_rebaseline_event_binds_live_v3_projection(tmp_path: Path) -> None:
    from dataclasses import replace

    from okto_pulse.core.kg.rebuild_sources import _compose_source_set_hash_v2

    board_id = "b-legacy-event"
    source_rows = [
        {
            "artifact_type": "spec",
            "id": "legacy-event-spec",
            "source_ref": "spec:legacy-event-spec",
            "source_version": "1",
            "content_hash": "hash-v3",
            "content_hash_v1": "hash-v1",
            "content_hash_v2": "hash-v2",
            "created_at": "2026-05-01T00:00:00+00:00",
            "status": "done",
        }
    ]

    def _step(request: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=request.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        _generation_repo,
        _report_store,
        events,
    ) = _build_service_with_kg024(
        tmp_path,
        step_adapter=_step,
        source_rows=source_rows,
    )
    source_set = service.source_enumerator.enumerate(board_id=board_id)
    current = manifest_store.build(
        source_set=source_set,
        preflight_hash="a" * 64,
    )

    def _legacy_rows(rows):
        return tuple(
            replace(
                row,
                content_hash=(row.content_hash_v2 or row.content_hash),
                content_hash_v1="",
                content_hash_v2="",
            )
            for row in rows
        )

    legacy = replace(
        current,
        manifest_schema_version=2,
        source_set_hash=_compose_source_set_hash_v2(source_set),
        sources=_legacy_rows(current.sources),
        working_sources=_legacy_rows(current.working_sources),
        skipped_by_maturity=_legacy_rows(current.skipped_by_maturity),
        skipped_expired_working=_legacy_rows(current.skipped_expired_working),
        legacy_unknown=_legacy_rows(current.legacy_unknown),
        payload_digest="",
    )
    manifest_store.artifact_store.write_json_atomic(
        manifest_store._manifest_key(legacy.manifest_ref),
        legacy.to_dict(),
    )
    token = confirmation_store.issue(
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
    )

    result = service.run(
        confirmation_id=token.confirmation_id,
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=legacy.preflight_hash,
        manifest_ref=legacy.manifest_ref,
        reason="legacy event projection binding",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert len(events) == 1
    event = events[0]
    assert event["rebaseline_evidence_id"] == (f"{result.run_id}:{legacy.manifest_ref}")
    assert len(event["rebaseline_target_source_set_hash"]) == 64
    assert event["rebaseline_target_source_set_hash"] != legacy.source_set_hash


def test_completed_terminal_replay_fails_closed_when_current_pointer_drifted(
    tmp_path: Path,
) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    def _step(request: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=request.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        _generation_repo,
        _report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    common = {
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "pointer drift must not replay success",
    }
    completed = service.run(confirmation_id=confirmation_id, **common)
    assert completed.outcome == RebuildOutcome.COMPLETED.value

    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    active = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(active) == 1
    assert active[0]["receipt_state"] == "terminal"
    service = replace(
        service,
        generation_repository=SimpleNamespace(
            get_current=lambda _board_id: "kggen_00000000-0000-4000-8000-000000000999"
        ),
    )

    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as capability:
        with pytest.raises(
            RuntimeError,
            match="rebuild_terminal_generation_pointer_conflict",
        ):
            KGRebuildService.run(
                service,
                confirmation_id="receipt_authorized_resume",
                _resume_run_id=completed.run_id,
                recovery_capability=capability,
                **common,
            )

    # Conflict is diagnostic: it neither rotates nor discards the active proof.
    assert (
        list_rebuild_confirmation_receipts(
            artifact_store=confirmation_store.artifact_store,
            board_id="b1",
        )
        == active
    )


def test_terminal_event_failure_preserves_durable_success_and_requests_retry(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        generation_repo,
        _report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    def _fail_event(_payload):  # noqa: ANN001, ANN202
        raise RuntimeError("event bus unavailable")

    service = replace(service, event_emitter=_fail_event)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="prove terminal event retry contract",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.report_ref is not None
    assert result.current_kg_generation_id == generation_repo.get_current("b1")
    assert result.event_emitted is False
    assert result.operator_action == "emit_terminal_event"
    audit = json.loads(Path(result.audit_ref).read_text(encoding="utf-8"))
    assert audit["same_run_resume_allowed"] is True
    assert audit["resume_phase"] == "emit_terminal_event"


def test_terminal_event_composite_rejection_overrides_nested_publish_success(
    tmp_path: Path,
) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        generation_repo,
        _report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    composite_rejection = SimpleNamespace(
        accepted=False,
        publish=SimpleNamespace(accepted=True),
    )
    service = replace(
        service,
        event_emitter=lambda _payload: composite_rejection,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="prove composite terminal event acceptance",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.current_kg_generation_id == generation_repo.get_current("b1")
    assert result.event_emitted is False
    assert result.operator_action == "emit_terminal_event"
    audit = json.loads(Path(result.audit_ref).read_text(encoding="utf-8"))
    assert audit["same_run_resume_allowed"] is True
    assert audit["resume_phase"] == "emit_terminal_event"


def test_real_terminal_handler_retries_marker_without_republishing_event(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from okto_pulse.core.kg.rebuild_audit import (
        CognitivePendingMarker,
        KGRebuiltEventPublisher,
        build_kg_rebuilt_event_handler,
    )
    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    step_calls = 0

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        nonlocal step_calls
        step_calls += 1
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        generation_repo,
        report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    published: list[dict[str, object]] = []
    marker_calls = 0

    def _publish(payload):  # noqa: ANN001, ANN202
        published.append(dict(payload))
        return True

    def _fail_first_marker(_board, _generation, sources):  # noqa: ANN001, ANN202
        nonlocal marker_calls
        marker_calls += 1
        if marker_calls == 1:
            raise RuntimeError("cognitive marker temporarily unavailable")
        return len(sources)

    handler = build_kg_rebuilt_event_handler(
        publisher=KGRebuiltEventPublisher(
            base_dir=tmp_path,
            publish_adapter=_publish,
        ),
        cognitive_marker=CognitivePendingMarker(
            base_dir=tmp_path,
            pending_adapter=_fail_first_marker,
        ),
        source_resolver=lambda _payload: (
            {"artifact_type": "spec", "source_ref": "spec:spec-1"},
        ),
    )
    service = replace(service, event_emitter=handler)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    common = {
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "retry cognitive marker under the same terminal journal",
    }

    first = service.run(confirmation_id=confirmation_id, **common)

    assert first.outcome == RebuildOutcome.COMPLETED.value
    assert first.event_emitted is False
    assert first.operator_action == "emit_terminal_event"
    promoted = generation_repo.get_current("b1")
    assert promoted == first.current_kg_generation_id
    assert len(published) == 1
    assert marker_calls == 1
    active = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(active) == 1
    assert active[0].get("receipt_state", "authorized") == "authorized"

    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as resume_capability:
        resumed = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=first.run_id,
            recovery_capability=resume_capability,
            **common,
        )

    assert resumed.outcome == RebuildOutcome.COMPLETED.value
    assert resumed.event_emitted is True
    assert resumed.current_kg_generation_id == promoted
    assert len(published) == 1
    assert marker_calls == 2
    assert report_store.inspect_for_run(board_id="b1", run_id=first.run_id) is not None
    terminal = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(terminal) == 1
    assert terminal[0]["receipt_state"] == "terminal"

    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as replay_capability:
        frozen = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=first.run_id,
            recovery_capability=replay_capability,
            **common,
        )

    assert frozen.outcome == RebuildOutcome.COMPLETED.value
    assert frozen.event_emitted is True
    assert frozen.current_kg_generation_id == promoted
    assert len(published) == 1
    assert marker_calls == 2
    assert step_calls == 2


@pytest.mark.parametrize("preserve_run_audit", (True, False))
def test_source_drift_after_terminal_effects_requires_reconciliation_not_compensation(
    tmp_path: Path,
    preserve_run_audit: bool,
) -> None:
    from dataclasses import replace

    def _step(request: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=request.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        generation_repo,
        report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    service = replace(service, event_emitter=lambda _payload: False)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    common = {
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": "terminal evidence outranks later source drift",
    }
    partial = service.run(confirmation_id=confirmation_id, **common)
    assert partial.operator_action == "emit_terminal_event"
    assert partial.report_ref is not None
    promoted = generation_repo.get_current("b1")
    assert promoted == partial.current_kg_generation_id
    report_before = report_store.inspect_for_run(board_id="b1", run_id=partial.run_id)
    assert report_before is not None

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    active_before = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(active_before) == 1
    if not preserve_run_audit:
        service.artifact_store.delete_json(
            RebuildAuditKey(
                namespace="run_audit",
                board_id="b1",
                artifact_id=partial.run_id,
            )
        )

    drifted = dict(_row())
    drifted["content_hash"] = "source-changed-after-promotion"
    drifted_enumerator = RebuildSourceEnumerator(source_store=lambda _board: [drifted])

    def _unexpected_step(_request):  # noqa: ANN001, ANN202
        pytest.fail("terminal evidence must block checkpoint compensation/replay")

    service = replace(
        service,
        source_enumerator=drifted_enumerator,
        rebuild_step_adapter=_unexpected_step,
    )
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as capability:
        reconciled = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=partial.run_id,
            recovery_capability=capability,
            **common,
        )

    assert reconciled.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert reconciled.reason == RebuildBlockReason.LEASE_LOST.value
    assert reconciled.operator_action == "terminal_reconciliation_required"
    assert generation_repo.get_current("b1") == promoted
    assert (
        report_store.inspect_for_run(
            board_id="b1",
            run_id=partial.run_id,
        )
        == report_before
    )
    assert (
        list_rebuild_confirmation_receipts(
            artifact_store=confirmation_store.artifact_store,
            board_id="b1",
        )
        == active_before
    )


@pytest.mark.parametrize(
    "terminal_evidence",
    (
        "report_before_guard",
        "history_before_pointer",
        "candidate_current_pointer",
        "third_current_pointer",
    ),
)
def test_source_drift_reconciles_each_partial_terminal_cut_without_compensation(
    tmp_path: Path,
    terminal_evidence: str,
) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    def _step(request: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=request.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        durable_generation_repo,
        report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    service = replace(service, event_emitter=lambda _payload: False)
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    common = {
        "board_id": "b1",
        "actor_id": "user-1",
        "operation": "rebuild",
        "preflight_hash": preflight_hash,
        "manifest_ref": manifest_ref,
        "reason": f"reconcile isolated terminal cut: {terminal_evidence}",
    }
    partial = service.run(confirmation_id=confirmation_id, **common)
    assert partial.operator_action == "emit_terminal_event"
    receipts = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(receipts) == 1
    receipt = receipts[0]
    candidate = str(receipt["candidate_kg_generation_id"])
    previous = receipt.get("previous_kg_generation_id")
    persisted_history = durable_generation_repo.load_history("b1", candidate)
    assert persisted_history is not None
    service.artifact_store.delete_json(
        RebuildAuditKey(
            namespace="run_audit",
            board_id="b1",
            artifact_id=partial.run_id,
        )
    )

    if terminal_evidence == "report_before_guard":
        # The deterministic report exists, but neither history nor current
        # pointer is observable at this crash cut.
        recovery_report_store = report_store
        recovery_generation_repo = SimpleNamespace(
            get_current=lambda _board_id: previous,
            load_history=lambda _board_id, _candidate: None,
        )
    elif terminal_evidence == "history_before_pointer":
        # Promotion history is durable and bound to this run/report, while the
        # current pointer still exposes the previous generation.
        recovery_report_store = None
        recovery_generation_repo = SimpleNamespace(
            get_current=lambda _board_id: previous,
            load_history=lambda _board_id, _candidate: dict(persisted_history),
        )
    elif terminal_evidence == "candidate_current_pointer":
        # The pointer CAS itself is irreversible even if the report/history
        # probes are unavailable at this exact crash boundary.
        recovery_report_store = None
        recovery_generation_repo = SimpleNamespace(
            get_current=lambda _board_id: candidate,
            load_history=lambda _board_id, _candidate: None,
        )
    else:
        # A third current value is never safe to compensate as though the
        # previous pointer still owned the board.
        recovery_report_store = None
        recovery_generation_repo = SimpleNamespace(
            get_current=lambda _board_id: "kggen_00000000-0000-4000-8000-000000000777",
            load_history=lambda _board_id, _candidate: None,
        )

    drifted = dict(_row())
    drifted["content_hash"] = f"drift-after-{terminal_evidence}"

    def _unexpected_step(_request):  # noqa: ANN001, ANN202
        pytest.fail("partial terminal evidence must block fail_existing/compensation")

    service = replace(
        service,
        source_enumerator=RebuildSourceEnumerator(
            source_store=lambda _board: [drifted]
        ),
        rebuild_step_adapter=_unexpected_step,
        report_store=recovery_report_store,
        generation_repository=recovery_generation_repo,
    )
    with issue_recovery_execution_capability(
        board_id="b1",
        lifetime_probe=lambda: True,
    ) as capability:
        reconciled = KGRebuildService.run(
            service,
            confirmation_id="receipt_authorized_resume",
            _resume_run_id=partial.run_id,
            recovery_capability=capability,
            **common,
        )

    assert reconciled.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert reconciled.reason == RebuildBlockReason.LEASE_LOST.value
    assert reconciled.operator_action == "terminal_reconciliation_required"
    assert durable_generation_repo.get_current("b1") == candidate
    assert (
        list_rebuild_confirmation_receipts(
            artifact_store=confirmation_store.artifact_store,
            board_id="b1",
        )
        == receipts
    )


def test_terminal_rebuild_effects_remain_under_board_writer_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        lock,
        _generation_repo,
        _report_store,
        _events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)
    observed: list[str] = []

    def _assert_fenced(label: str) -> None:
        assert lock.inspect(board_id="b1") is not None
        observed.append(label)

    service = replace(
        service,
        event_emitter=lambda _payload: (_assert_fenced("event"), True)[1],
    )
    original_emit = KGRebuildService._emit_audit_and_counter

    def _guarded_emit(self, **kwargs):
        _assert_fenced("audit")
        return original_emit(self, **kwargs)

    monkeypatch.setattr(
        KGRebuildService,
        "_emit_audit_and_counter",
        _guarded_emit,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="fence terminal effects",
    )

    assert result.event_emitted is True
    assert observed == ["event", "audit"]
    assert lock.inspect(board_id="b1") is None


def test_terminal_orphan_scan_losing_writer_blocks_report_promotion_and_event(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.kg.orphan_integrity import OrphanScanReport
    from okto_pulse.core.kg.rebuild_report import get_persist_count

    events: list[dict] = []
    lock_holder: dict[str, KGSingleWriterLock] = {}

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1},
        )

    def _orphan_scan(board_id: str, generation_id: str | None):
        lock = lock_holder["lock"]
        manifest = lock.inspect(board_id=board_id)
        assert manifest is not None
        assert lock.release(board_id=board_id, owner_token=manifest.owner_token)
        return OrphanScanReport(
            board_id=board_id,
            generation_id=generation_id,
            orphan_count=0,
            orphan_count_by_type={},
            orphan_count_by_writer_path={},
            samples=(),
            unresolved_reasons={},
            allowlisted_root_count=0,
            correlation_id="terminal-fence-loss",
        )

    (
        service,
        manifest_store,
        confirmation_store,
        lock,
        generation_repo,
        _report_store,
        _,
    ) = _build_service_with_kg024(
        tmp_path,
        step_adapter=_step,
        event_sink=events,
        orphan_scan_provider=_orphan_scan,
    )
    lock_holder["lock"] = lock
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    persist_count_before = get_persist_count("b1")

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="lose writer during orphan scan",
    )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.report_ref is None
    assert result.current_kg_generation_id is None
    assert generation_repo.get_current("b1") is None
    assert events == []
    assert get_persist_count("b1") == persist_count_before


def test_post_audit_reservation_loss_never_recreates_receipt_after_erasure_purge(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
        rebuild_active_confirmation_receipt_key,
    )

    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    real_reservation = KGAdministrativeOperationReservation(
        base_dir=tmp_path / "locks",
        write_lock_port=lock.bind_write_lock_port(),
    )

    class ReservationLostAfterAudit:
        alive = True

        def bind_write_lock_port(self):  # noqa: ANN201
            return real_reservation.bind_write_lock_port()

        def acquire(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.acquire(**kwargs)

        def renew(self, **kwargs):  # noqa: ANN003, ANN201
            return self.alive and real_reservation.renew(**kwargs)

        def release(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.release(**kwargs)

    reservation = ReservationLostAfterAudit()
    delegate = service.artifact_store

    class ErasurePurgeAfterAudit:
        def __getattr__(self, name):  # noqa: ANN001, ANN204
            return getattr(delegate, name)

        def write_json_atomic(self, key, payload):  # noqa: ANN001, ANN201
            result = delegate.write_json_atomic(key, payload)
            if key.namespace == "run_audit":
                # Model erasure taking over immediately after the audit write:
                # the old board artifacts are purged and the old reservation
                # can no longer be renewed.  The finalizer must not archive and
                # thereby recreate the active receipt after this cut.
                reservation.alive = False
                delegate.delete_json(key)
                delegate.delete_json(
                    rebuild_active_confirmation_receipt_key(board_id="b1")
                )
            return result

    service = replace(
        service,
        operation_reservation=reservation,
        artifact_store=ErasurePurgeAfterAudit(),
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="erasure wins after technical audit",
    )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.audit_ref == ""
    assert result.operator_action == "reacquire_writer_and_resume"
    assert (
        list_rebuild_confirmation_receipts(
            artifact_store=confirmation_store.artifact_store,
            board_id="b1",
        )
        == ()
    )
    assert (
        delegate.read_json(
            RebuildAuditKey(
                namespace="run_audit",
                board_id="b1",
                artifact_id=result.run_id,
            )
        )
        is None
    )
    assert lock.inspect(board_id="b1") is None
    assert real_reservation.inspect(board_id="b1") is None


def test_reservation_loss_before_terminal_audit_performs_no_board_audit_io(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from okto_pulse.core.kg.rebuild_service import (
        list_rebuild_confirmation_receipts,
    )

    reservation_alive = {"value": True}

    def _step(_request: RebuildStepInput) -> RebuildStepResult:
        reservation_alive["value"] = False
        return RebuildStepResult(ok=True)

    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        step_adapter=_step,
    )
    real_reservation = KGAdministrativeOperationReservation(
        base_dir=tmp_path / "locks",
        write_lock_port=lock.bind_write_lock_port(),
    )

    class LostReservation:
        def bind_write_lock_port(self):  # noqa: ANN201
            return real_reservation.bind_write_lock_port()

        def acquire(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.acquire(**kwargs)

        def renew(self, **kwargs):  # noqa: ANN003, ANN201
            return reservation_alive["value"] and real_reservation.renew(**kwargs)

        def release(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.release(**kwargs)

    delegate = service.artifact_store
    run_audit_writes: list[str] = []
    run_audit_deletes: list[str] = []

    class AuditIOSpy:
        def __getattr__(self, name):  # noqa: ANN001, ANN204
            return getattr(delegate, name)

        def write_json_atomic(self, key, payload):  # noqa: ANN001, ANN201
            if key.namespace == "run_audit":
                run_audit_writes.append(str(key.artifact_id))
            return delegate.write_json_atomic(key, payload)

        def delete_json(self, key):  # noqa: ANN001, ANN201
            if key.namespace == "run_audit":
                run_audit_deletes.append(str(key.artifact_id))
            return delegate.delete_json(key)

    service = replace(
        service,
        operation_reservation=LostReservation(),
        artifact_store=AuditIOSpy(),
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="reservation is lost before terminal audit",
    )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.audit_ref == ""
    assert run_audit_writes == []
    assert run_audit_deletes == []
    active = list_rebuild_confirmation_receipts(
        artifact_store=confirmation_store.artifact_store,
        board_id="b1",
    )
    assert len(active) == 1
    assert active[0].get("receipt_state", "authorized") == "authorized"
    assert lock.inspect(board_id="b1") is None
    assert real_reservation.inspect(board_id="b1") is None


def test_archive_history_reproves_fences_inside_artifact_transaction(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
    from okto_pulse.core.kg.rebuild_service import (
        rebuild_active_confirmation_receipt_key,
        rebuild_confirmation_receipt_key,
    )

    service, manifest_store, confirmation_store, lock = _build_service(tmp_path)
    real_reservation = KGAdministrativeOperationReservation(
        base_dir=tmp_path / "locks",
        write_lock_port=lock.bind_write_lock_port(),
    )

    class ReservationTakenByErasure:
        alive = True

        def bind_write_lock_port(self):  # noqa: ANN201
            return real_reservation.bind_write_lock_port()

        def acquire(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.acquire(**kwargs)

        def renew(self, **kwargs):  # noqa: ANN003, ANN201
            return self.alive and real_reservation.renew(**kwargs)

        def release(self, **kwargs):  # noqa: ANN003, ANN201
            return real_reservation.release(**kwargs)

    reservation = ReservationTakenByErasure()
    receipt_delegate = confirmation_store.artifact_store
    run_delegate = service.artifact_store
    attempted_run_id: list[str] = []

    class ErasureBeforeHistoryTransaction:
        def __getattr__(self, name):  # noqa: ANN001, ANN204
            return getattr(receipt_delegate, name)

        def replace_json(self, key, transform):  # noqa: ANN001, ANN201
            if (
                key.namespace == "rebuild_confirmation_receipt"
                and key.artifact_id not in (None, "active")
                and not attempted_run_id
            ):
                attempted_run_id.append(str(key.artifact_id))
                # The outer post-audit probe already succeeded, then this call
                # waited behind erasure's artifact transaction until R expired.
                # Purge finishes before the stale callback is invoked.
                reservation.alive = False
                receipt_delegate.delete_json(
                    rebuild_active_confirmation_receipt_key(board_id="b1")
                )
                receipt_delegate.delete_json(
                    rebuild_confirmation_receipt_key(
                        board_id="b1",
                        run_id=str(key.artifact_id),
                    )
                )
                run_delegate.delete_json(
                    RebuildAuditKey(
                        namespace="run_audit",
                        board_id="b1",
                        artifact_id=str(key.artifact_id),
                    )
                )
            return receipt_delegate.replace_json(key, transform)

    guarded_confirmation_store = replace(
        confirmation_store,
        artifact_store=ErasureBeforeHistoryTransaction(),
    )
    service = replace(
        service,
        confirmation_store=guarded_confirmation_store,
        operation_reservation=reservation,
    )
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        guarded_confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    with pytest.raises(
        RuntimeError,
        match="rebuild_confirmation_archive_fence_lost_before_history",
    ):
        service.run(
            confirmation_id=confirmation_id,
            board_id="b1",
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason="erasure wins before receipt history transaction",
        )

    assert len(attempted_run_id) == 1
    run_id = attempted_run_id[0]
    assert (
        receipt_delegate.read_json(
            rebuild_active_confirmation_receipt_key(board_id="b1")
        )
        is None
    )
    assert (
        receipt_delegate.read_json(
            rebuild_confirmation_receipt_key(board_id="b1", run_id=run_id)
        )
        is None
    )
    assert (
        run_delegate.read_json(
            RebuildAuditKey(
                namespace="run_audit",
                board_id="b1",
                artifact_id=run_id,
            )
        )
        is None
    )
    assert lock.inspect(board_id="b1") is None
    assert real_reservation.inspect(board_id="b1") is None


def test_terminal_fence_loss_after_report_persist_preserves_report_receipt(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        lock,
        generation_repo,
        report_store,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    class ReleaseAfterPersist:
        def persist(self, *, payload):  # noqa: ANN001, ANN201
            receipt = report_store.persist(payload=payload)
            manifest = lock.inspect(board_id=payload.summary.board_id)
            assert manifest is not None
            assert lock.release(
                board_id=payload.summary.board_id,
                owner_token=manifest.owner_token,
            )
            return receipt

    service = replace(service, report_store=ReleaseAfterPersist())
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="lose fence after report persist",
    )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.report_ref is not None
    assert Path(result.report_ref).exists()
    assert result.report_id is not None
    assert result.current_kg_generation_id is None
    assert result.event_emitted is False
    assert generation_repo.get_current("b1") is None
    assert events == []


def test_terminal_fence_loss_after_generation_promotion_preserves_pointer(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    def _step(req: RebuildStepInput) -> RebuildStepResult:
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        lock,
        generation_repo,
        _report_store,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    class ReleaseAfterPromotion:
        def get_current(self, board_id: str):  # noqa: ANN201
            return generation_repo.get_current(board_id)

        def promote_current(self, **kwargs):  # noqa: ANN003, ANN201
            receipt = generation_repo.promote_current(**kwargs)
            manifest = lock.inspect(board_id=kwargs["board_id"])
            assert manifest is not None
            assert lock.release(
                board_id=kwargs["board_id"],
                owner_token=manifest.owner_token,
            )
            return receipt

    service = replace(service, generation_repository=ReleaseAfterPromotion())
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="lose fence after promotion",
    )

    durable_current = generation_repo.get_current("b1")
    assert durable_current is not None
    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.LEASE_LOST.value
    assert result.report_ref is not None
    assert result.current_kg_generation_id == durable_current
    assert result.promotion_outcome == "promoted"
    assert result.event_emitted is False
    assert events == []


@pytest.mark.parametrize(
    "missing_component",
    (
        "generation_repository",
        "promotion_guard",
        "report_store",
        "terminal_state_guard",
    ),
)
def test_partial_kg024_terminal_bundle_fails_closed(
    tmp_path: Path,
    missing_component: str,
) -> None:
    from dataclasses import replace

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        generation_repo,
        _report_store,
        events,
    ) = _build_service_with_kg024(tmp_path)
    service = replace(service, **{missing_component: None})
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
    )

    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="partial KG-02.4 configuration",
    )

    assert result.outcome == RebuildOutcome.REBUILD_FAILED.value
    assert result.reason == RebuildBlockReason.GENERATION_STORE_UNAVAILABLE.value
    assert result.current_kg_generation_id is None
    assert result.operator_action == "configure_complete_kg02_4_bundle"
    assert events == []
    assert generation_repo.get_current("b1") is None


def test_completed_run_with_remaining_orphans_blocks_clean_success(tmp_path: Path):
    """KG-ZO-02.3: clean rebuild success requires zero non-allowlisted orphans."""

    from okto_pulse.core.kg.orphan_integrity import (
        OrphanNodeSample,
        OrphanScanReport,
    )
    from okto_pulse.core.kg.rebuild_report import get_terminal_count

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 7, "edges": 3},
        )

    def _orphan_scan(board_id: str, generation_id: str | None):
        return OrphanScanReport(
            board_id=board_id,
            generation_id=generation_id,
            orphan_count=1,
            orphan_count_by_type={"Learning": 1},
            orphan_count_by_writer_path={"cognitive_consolidation": 1},
            samples=(
                OrphanNodeSample(
                    node_id="learning_orphan_1",
                    node_type="Learning",
                    writer_path="cognitive_consolidation",
                    source_artifact_ref="bug:bug-1",
                    source_resolution_status="unresolved_source_ref",
                    generation_id=generation_id,
                    reason="zero_graph_degree",
                    correlation_id="corr-orphan-rebuild",
                ),
            ),
            unresolved_reasons={"unresolved_source_ref": 1},
            allowlisted_root_count=0,
            correlation_id="corr-orphan-rebuild",
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        rep_store,
        events,
    ) = _build_service_with_kg024(
        tmp_path,
        step_adapter=_step,
        orphan_scan_provider=_orphan_scan,
    )

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="orphan validation path",
    )

    assert result.outcome == RebuildOutcome.FAILED_ORPHAN_VALIDATION.value
    assert result.reason == RebuildBlockReason.ORPHAN_VALIDATION_FAILED.value
    assert result.publishable_status == "failed_orphan_validation"
    assert result.operator_action == "run_orphan_backfill"
    assert result.current_kg_generation_id is None
    assert gen_repo.get_current("b1") is None
    assert events and events[0]["status"] == "failed_orphan_validation"
    assert (
        get_terminal_count(
            "b1",
            candidate_terminal_status="failed_orphan_validation",
            publishable_status="failed_orphan_validation",
            with_report_ref=True,
        )
        == 1
    )
    report = rep_store.load(result.report_ref or "")
    assert report is not None
    validation = report["drilldown"]["zero_orphan_validation"]
    assert validation["zero_orphan_validation"] == "pending_backfill"
    assert validation["orphan_count"] == 1
    assert set(validation["samples"][0]) == {
        "node_id",
        "node_type",
        "writer_path",
        "source_artifact_ref",
        "source_resolution_status",
        "generation_id",
        "reason",
        "correlation_id",
    }


def test_completed_run_after_backfill_publishes_zero_orphan_validation(
    tmp_path: Path,
):
    """TC-KG-ZO-02.6: rebuild report is clean only after real orphan backfill."""

    from okto_pulse.core.kg.orphan_integrity import (
        OrphanBackfillReconciler,
        OrphanNodeScanner,
    )
    from okto_pulse.core.kg.primitives import _apply_graph_node_create
    from okto_pulse.core.kg.rebuild_report import get_terminal_count
    from kg_schema_testing import open_board_connection
    from okto_pulse.core.kg.transaction import TransactionOrchestrator

    board_id = f"zo02e2e{uuid.uuid4().hex[:12]}"
    source_root = f"spec:{board_id}"
    board_root_id = "entity_board_root_zero_orphan"
    entity_id = "entity_spec_zero_orphan"
    requirement_id = "requirement_zero_orphan"
    bug_id = "bug_zero_orphan"
    learning_id = "learning_zero_orphan"

    def _seed_node(kconn, orch, node_type: str, node_id: str, source_ref: str) -> None:
        _apply_graph_node_create(
            orch,
            node_type,
            node_id,
            {
                "title": f"Sensitive title must not leak {node_id}",
                "content": "Sensitive content must not leak user@example.com",
                "context": "",
                "justification": "",
                "source_artifact_ref": source_ref,
                "created_at": "2026-06-08T00:00:00+00:00",
                "created_by_agent": "agent:e2e",
                "source_confidence": 1.0,
                "relevance_score": 0.5,
                "query_hits": 0,
                "last_queried_at": None,
                "last_recomputed_at": None,
                "priority_boost": 0.0,
                "superseded_by": None,
                "superseded_at": None,
                "revocation_reason": "",
                "human_curated": False,
                "embedding": [0.0] * 384,
            },
        )

    with open_board_connection(board_id) as (_db, kconn):
        orch = TransactionOrchestrator(
            graph_scope=kconn,
            session_id="seed_zero_orphan_e2e",
            board_id=board_id,
        )
        _seed_node(kconn, orch, "Entity", board_root_id, f"board:{board_id}")
        _seed_node(kconn, orch, "Entity", entity_id, source_root)
        orch.create_edge(
            "belongs_to",
            entity_id,
            board_root_id,
            attrs={"confidence": 1.0},
            from_type="Entity",
            to_type="Entity",
        )
        _seed_node(kconn, orch, "Requirement", requirement_id, f"{source_root}:fr:0")
        _seed_node(kconn, orch, "Bug", bug_id, f"bug:{bug_id}")
        orch.create_edge(
            "belongs_to",
            bug_id,
            board_root_id,
            attrs={"confidence": 1.0},
            from_type="Bug",
            to_type="Entity",
        )
        _seed_node(
            kconn,
            orch,
            "Learning",
            learning_id,
            f"card:bug:{bug_id}:learning:0",
        )

    before = OrphanNodeScanner().scan(board_id=board_id, generation_id="gen-before")
    assert before.orphan_count == 2

    backfill = OrphanBackfillReconciler().run(
        board_id=board_id,
        generation_id="gen-backfill",
    )
    assert backfill.connected == 2

    after = OrphanNodeScanner().scan(board_id=board_id, generation_id="gen-after")
    assert after.orphan_count == 0

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="e" * 64,
            source_hash="f" * 64,
            counts={"nodes": 4, "edges": 2},
            drilldown={"fixture": "zero-orphan-e2e"},
        )

    def _orphan_scan(scan_board_id: str, generation_id: str | None):
        assert scan_board_id == board_id
        return OrphanNodeScanner().scan(
            board_id=scan_board_id,
            generation_id=generation_id,
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        rep_store,
        events,
    ) = _build_service_with_kg024(
        tmp_path,
        step_adapter=_step,
        orphan_scan_provider=_orphan_scan,
    )

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store,
        manifest_store,
        service.source_enumerator,
        board_id=board_id,
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id=board_id,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="zero-orphan validation e2e",
    )

    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert result.publishable_status == "completed"
    assert result.current_kg_generation_id is not None
    assert gen_repo.get_current(board_id) == result.current_kg_generation_id
    assert events and events[0]["status"] == "completed"
    assert (
        get_terminal_count(
            board_id,
            candidate_terminal_status="completed",
            publishable_status="completed",
            with_report_ref=True,
        )
        == 1
    )
    report = rep_store.load(result.report_ref or "")
    assert report is not None
    validation = report["drilldown"]["zero_orphan_validation"]
    assert validation["zero_orphan_validation"] == "passed"
    assert validation["orphan_count"] == 0
    assert validation["samples"] == []
    assert validation["reason"] == "zero_non_allowlisted_orphans"


def test_report_persist_failure_blocks_promotion_and_preserves_previous(
    tmp_path: Path,
):
    """br_82deef11: report fails -> outcome=report_persist_failed and the
    repository pointer must NOT advance."""

    from okto_pulse.core.kg.rebuild_report import (
        RebuildReportStore,
        ReportPersistOutcome,
        ReportPersistResult,
    )

    class _BrokenStore(RebuildReportStore):
        def persist(self, *, payload):  # type: ignore[override]
            return ReportPersistResult(
                outcome=ReportPersistOutcome.STORE_FAILED.value,
                report_ref=None,
                report_id=None,
                board_id=payload.summary.board_id,
                run_id=payload.summary.run_id,
                persisted_at=None,
                detail="forced",
            )

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
        )

    broken = _BrokenStore(base_dir=tmp_path)
    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        _rep,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step, report_store=broken)

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="report failure path",
    )

    assert result.outcome == RebuildOutcome.REPORT_PERSIST_FAILED.value
    assert result.reason == RebuildBlockReason.REPORT_PERSIST_STORE_FAILED.value
    assert result.publishable_status == "report_persist_failed"
    assert result.promotion_outcome is None
    assert result.current_kg_generation_id is None
    assert result.event_emitted is False
    assert result.operator_action == "retry_report_persist"
    # Pointer NEVER advanced.
    assert gen_repo.get_current("b1") is None
    # No kg.rebuilt event.
    assert events == []


def test_sensitive_payload_rejection_blocks_promotion(tmp_path: Path):
    """A step that leaks a secret-shaped value into drilldown must be
    rejected by the report store and must not promote the generation."""

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            drilldown={"specs": [{"api_key": "AKIA1234567890ABCDEF"}]},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        _rep,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="leaky drilldown",
    )

    assert result.outcome == RebuildOutcome.REPORT_PERSIST_FAILED.value
    assert result.reason == RebuildBlockReason.REPORT_PERSIST_SENSITIVE_REJECTED.value
    assert result.publishable_status == "report_persist_failed"
    assert result.operator_action == "redact_payload_and_retry"
    assert gen_repo.get_current("b1") is None
    assert events == []
    audit = json.loads(Path(result.audit_ref).read_text(encoding="utf-8"))
    assert audit["same_run_resume_allowed"] is False
    assert audit["resume_phase"] is None


def test_step_failure_persists_report_but_does_not_promote(tmp_path: Path):
    """FAILED outcome still needs a durable report (TR16), but the
    pointer must not advance — only COMPLETED ever promotes."""

    def _step(req):
        return RebuildStepResult(
            ok=False,
            detail="forced step failure",
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 0, "edges": 0},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        _rep,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="failure path persists report",
    )

    assert result.outcome == RebuildOutcome.FAILED.value
    assert result.report_ref is not None
    assert Path(result.report_ref).exists()
    assert result.publishable_status == "rebuild_failed"
    # Promotion NEVER attempted on failures.
    assert result.promotion_outcome is None
    assert result.current_kg_generation_id is None
    assert gen_repo.get_current("b1") is None
    # Event was emitted because the report persisted.
    assert events and events[0]["status"] == "rebuild_failed"


def test_e2e_no_raw_confirmation_id_in_any_audit_file(tmp_path: Path):
    """val_8fa8019d E2E regression: full KGRebuildService.run() with
    RebuildConfirmationStore wired to ConfirmationConsumptionAuditRecorder.
    Scan every JSON under base/rebuild/audit/** and prove the raw
    confirmation_id NEVER appears; the canonical fingerprint DOES."""

    from dataclasses import replace as dc_replace
    from okto_pulse.core.kg.rebuild_audit import (
        ConfirmationConsumptionAuditRecorder,
        confirmation_fingerprint,
    )
    from okto_pulse.core.kg.rebuild_confirmation import (
        RebuildConfirmationStore as _Store,
    )

    service, manifest_store, _orig_confirmation, lock = _build_service(tmp_path)
    enumerator = service.source_enumerator

    # Replace the confirmation_store with one wired to the audit recorder
    # so consume() also produces the KG-02.7 audit row alongside the
    # legacy run audit.
    audit_recorder = ConfirmationConsumptionAuditRecorder(base_dir=tmp_path)
    wired_store = _Store(base_dir=tmp_path, audit_recorder=audit_recorder)
    service = dc_replace(service, confirmation_store=wired_store)

    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        wired_store, manifest_store, enumerator
    )
    raw_confirmation_id = confirmation_id

    result = service.run(
        confirmation_id=raw_confirmation_id,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="e2e no-leak audit",
    )
    assert result.outcome == RebuildOutcome.COMPLETED.value

    audit_root = tmp_path / "rebuild" / "audit"
    audit_files = list(audit_root.rglob("*.json"))
    assert audit_files, "no audit files produced — wiring is broken"

    fingerprint = confirmation_fingerprint(raw_confirmation_id)
    fingerprint_seen = False
    for path in audit_files:
        body = path.read_text(encoding="utf-8")
        assert raw_confirmation_id not in body, (
            f"raw confirmation_id leaked into {path.relative_to(tmp_path)}"
        )
        if fingerprint in body:
            fingerprint_seen = True
    assert fingerprint_seen, (
        "canonical fingerprint should appear in at least one audit row "
        "to allow operator correlation"
    )


def test_second_completed_run_advances_previous_pointer(tmp_path: Path):
    """Two consecutive completed runs prove promote_current chains the
    previous pointer correctly (br_5c7c5dfa)."""

    def _step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": 1, "edges": 0},
        )

    (
        service,
        manifest_store,
        confirmation_store,
        _lock,
        gen_repo,
        _rep,
        events,
    ) = _build_service_with_kg024(tmp_path, step_adapter=_step)

    # First run.
    cid, mref, ph = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    first = service.run(
        confirmation_id=cid,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=ph,
        manifest_ref=mref,
        reason="first",
    )
    assert first.outcome == RebuildOutcome.COMPLETED.value
    first_gen = first.current_kg_generation_id

    # Second run — issue fresh manifest + confirmation.
    cid2, mref2, ph2 = _issue_confirmation(
        confirmation_store, manifest_store, service.source_enumerator
    )
    second = service.run(
        confirmation_id=cid2,
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=ph2,
        manifest_ref=mref2,
        reason="second",
    )
    assert second.outcome == RebuildOutcome.COMPLETED.value
    assert second.previous_kg_generation_id == first_gen
    assert second.current_kg_generation_id != first_gen
    assert gen_repo.get_current("b1") == second.current_kg_generation_id
    # Both runs emitted events.
    statuses = [e["status"] for e in events]
    assert statuses == ["completed", "completed"]
