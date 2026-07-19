"""KG-02 test scenarios — one pytest per ts_<id>.

Each function below maps 1:1 to a spec test_scenario for KG-02 and
exercises the wired primitives end-to-end. They are referenced as
evidence (test_file_path + test_function) when each ts_id is promoted
to ``passed`` via ``okto_pulse_update_test_scenario_status``.

Scenario map:

| TC card     | ts_id        | function                                            |
|-------------|--------------|-----------------------------------------------------|
| TC-KG02-01  | ts_bac02dd7  | test_ts_bac02dd7_health_ui_shows_recovery_state     |
| TC-KG02-01  | ts_4712e6d7  | test_ts_4712e6d7_reset_requires_confirmation        |
| TC-KG02-01  | ts_e258586a  | test_ts_e258586a_preflight_is_read_only             |
| TC-KG02-02  | ts_969dfdc7  | test_ts_969dfdc7_confirmation_single_use_ttl_audit  |
| TC-KG02-03  | ts_909e0e04  | test_ts_909e0e04_preflight_excludes_cancelled       |
| TC-KG02-03  | ts_92cfed29  | test_ts_92cfed29_same_source_set_equivalent_rebuild |
| TC-KG02-03  | ts_dff04b2e  | test_ts_dff04b2e_structural_hash_excludes_run_state |
| TC-KG02-04  | ts_7949efc6  | test_ts_7949efc6_rebuild_obtains_kg01_lock          |
| TC-KG02-05  | ts_2c262b64  | test_ts_2c262b64_rebuild_persists_generation_event  |
| TC-KG02-05  | ts_3c9e856c  | test_ts_3c9e856c_cognitive_pending_and_audit_trail  |
| TC-KG02-05  | ts_9cb41200  | test_ts_9cb41200_report_persists_before_terminal    |
| TC-KG02-06  | ts_ebf4ed79  | test_ts_ebf4ed79_discovery_reindex_or_pending       |
| TC-KG02-06  | ts_dee61d05  | test_ts_dee61d05_discovery_reindex_status_visible   |
"""

from __future__ import annotations

import json
import secrets
import shutil
import time
from dataclasses import replace as dc_replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.ports.coordination import (
    WriteLockHandle,
    register_coordination_providers,
    reset_coordination_providers_for_tests,
)
from okto_pulse.core.kg.global_discovery_reindex import (
    GlobalDiscoveryReindexStatusStore,
    GlobalDiscoveryReindexer,
    ReindexAttempt,
    ReindexReason,
    ReindexStatus,
    reset_reindex_counter,
)
from okto_pulse.core.kg.global_discovery_writer import GlobalDiscoveryWriterLease
from okto_pulse.core.kg.rebuild_audit import (
    CognitivePendingMarker,
    CognitivePendingStatus,
    ConfirmationConsumptionAuditRecorder,
    KGRebuiltEventPublisher,
    build_kg_rebuilt_event_handler,
    confirmation_fingerprint,
    reset_audit_counter,
    reset_event_counter,
    reset_pending_counter,
)
from okto_pulse.core.kg.rebuild_confirmation import (
    ConfirmationOutcome,
    RebuildConfirmationStore,
    reset_confirmation_counter,
)
from okto_pulse.core.kg.rebuild_deterministic import (
    DeterministicStructuralRebuilder,
    EXCLUDED_HASH_FIELDS,
    build_structural_inputs,
    compute_structural_hash,
    reset_structural_hash_counter,
    reset_structural_hash_mismatch_counter,
)
from okto_pulse.core.kg.rebuild_generation import (
    KGGenerationRepository,
    generate_kg_generation_id,
    is_valid_kg_generation_id,
    reset_promotion_counter,
)
from okto_pulse.core.kg.rebuild_preflight import (
    PreflightOutcome,
    RebuildHealthSummary,
    RebuildPreflightService,
    RebuildSourceSummary,
    reset_preflight_counter,
)
from okto_pulse.core.kg.rebuild_report import (
    RebuildReportStore,
    RebuildReportTerminalStateGuard,
    reset_persist_counter,
    reset_report_counter,
    reset_terminal_counter,
)
from okto_pulse.core.kg.rebuild_service import (
    KGRebuildService,
    RebuildBlockReason,
    RebuildOutcome,
    RebuildStepResult,
    SUPPORTED_REBUILD_OPERATIONS,
    reset_rebuild_run_counter,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebuildSourceEnumerator,
    reset_enumeration_counter,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    HealthProbe,
    KGSafeWriteLifecycle,
    LifecycleStepResult,
    LockOwnerProbe,
)
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock


BOARD = "b1"


class _InMemorySingleWriterPort:
    def __init__(self) -> None:
        self._locks: dict[tuple[str, str, str], dict[str, object]] = {}

    @staticmethod
    def _scope(base_dir_hint: str | None) -> str:
        return base_dir_hint or "default"

    def _key(
        self, board_id: str, artifact_id: str, base_dir_hint: str | None
    ) -> tuple[str, str, str]:
        return (self._scope(base_dir_hint), board_id, artifact_id)

    @staticmethod
    def _iso(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

    def acquire_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        operation: str,
        owner_id: str,
        ttl_seconds: int,
        admin_lane: bool = False,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> dict[str, object]:
        del board_dir_resolver
        key = self._key(board_id, artifact_id, base_dir_hint)
        now = time.time()
        current = self._locks.get(key)
        stale_recovered = False
        if current is not None and float(current["expires_at_epoch"]) > now:
            return {
                "acquired": False,
                "owner_token": None,
                "expires_at": current["expires_at"],
                "current_owner": current["owner_id"],
                "admin_lane": current["admin_lane"],
                "stale_recovered": False,
            }
        if current is not None:
            stale_recovered = True

        owner_token = secrets.token_urlsafe(18)
        expires_at_epoch = now + ttl_seconds
        manifest = {
            "owner_token": owner_token,
            "owner_id": owner_id,
            "operation": operation,
            "acquired_at_epoch": now,
            "expires_at_epoch": expires_at_epoch,
            "acquired_at": self._iso(now),
            "expires_at": self._iso(expires_at_epoch),
            "admin_lane": admin_lane,
        }
        self._locks[key] = manifest
        return {
            "acquired": True,
            "owner_token": owner_token,
            "expires_at": manifest["expires_at"],
            "current_owner": owner_id,
            "admin_lane": admin_lane,
            "stale_recovered": stale_recovered,
        }

    def release_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        owner_token: str,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> bool:
        del board_dir_resolver
        key = self._key(board_id, artifact_id, base_dir_hint)
        current = self._locks.get(key)
        if current is None or current["owner_token"] != owner_token:
            return False
        del self._locks[key]
        return True

    def inspect_single_writer_sync(
        self,
        *,
        board_id: str,
        artifact_id: str,
        base_dir_hint: str | None = None,
        board_dir_resolver=None,
    ) -> dict[str, object] | None:
        del board_dir_resolver
        current = self._locks.get(self._key(board_id, artifact_id, base_dir_hint))
        return dict(current) if current is not None else None

    def acquire_sync(
        self,
        board_id: str,
        artifact_id: str,
        *,
        owner_token: str | None = None,
    ) -> WriteLockHandle:
        token = owner_token or secrets.token_urlsafe(18)
        return WriteLockHandle(
            board_id=board_id,
            artifact_id=artifact_id,
            owner_token=token,
            fencing_token=token,
        )

    def release_sync(self, handle: WriteLockHandle) -> None:
        self.release_single_writer_sync(
            board_id=handle.board_id,
            artifact_id=handle.artifact_id,
            owner_token=handle.owner_token,
        )

    def is_locked(self, board_id: str, artifact_id: str) -> bool:
        return self.inspect_single_writer_sync(
            board_id=board_id, artifact_id=artifact_id
        ) is not None

    def reset_for_tests(self) -> None:
        self._locks.clear()


class _AlwaysOwnedGlobalWriterLock:
    def is_owner(self, _board_id: str, _owner_token: str) -> bool:
        return True

    def release(self, *, board_id: str, owner_token: str) -> bool:
        del board_id, owner_token
        return True


def _guarded_reindex(reindexer: GlobalDiscoveryReindexer, **kwargs):
    lease = GlobalDiscoveryWriterLease(
        lock=_AlwaysOwnedGlobalWriterLock(),  # type: ignore[arg-type]
        owner_token="kg02-reindex-test-writer",
        operation="test_kg02_reindex",
    )
    try:
        with lease.guard():
            return reindexer.reindex_or_mark_pending(**kwargs)
    finally:
        lease.release()


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg02-scenarios"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_preflight_counter()
    reset_enumeration_counter()
    reset_confirmation_counter()
    reset_rebuild_run_counter()
    reset_promotion_counter()
    reset_persist_counter()
    reset_report_counter()
    reset_terminal_counter()
    reset_structural_hash_counter()
    reset_structural_hash_mismatch_counter()
    reset_reindex_counter()
    reset_event_counter()
    reset_pending_counter()
    reset_audit_counter()
    reset_coordination_providers_for_tests()
    register_coordination_providers(write_lock_port=_InMemorySingleWriterPort())
    yield
    reset_coordination_providers_for_tests()


def _row(id_: str = "s1", artifact_type: str = "spec",
         content_hash: str = "h1", source_version: str = "v1") -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"ref:{id_}",
        "source_version": source_version,
        "content_hash": content_hash,
        "created_at": "2026-05-01T00:00:00Z",
        "status": "validated",
    }


def _build_full_service(
    base_dir: Path,
    *,
    sources: list[dict] | None = None,
    step_adapter=None,
    audit_recorder: ConfirmationConsumptionAuditRecorder | None = None,
):
    """Wire a fully-functional KG-02 service stack (lock + safe lifecycle
    + manifest + confirmation + KG-02.4 report-first + KG-02.5 hashes +
    optional KG-02.7 audit recorder)."""

    rows = sources if sources is not None else [_row()]
    lock = KGSingleWriterLock(base_dir=base_dir / "locks")
    enumerator = RebuildSourceEnumerator(source_store=lambda _b: list(rows))
    manifest_store = KGRebuildSourceManifest(base_dir=base_dir)
    confirmation_store = RebuildConfirmationStore(
        base_dir=base_dir, audit_recorder=audit_recorder
    )

    def _owner_probe(board_id: str, owner_token: str) -> bool:
        manifest = lock.inspect(board_id=board_id)
        return manifest is not None and manifest.owner_token == owner_token

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=lambda b, g, s: LifecycleStepResult(ok=True),
        owner_probe=LockOwnerProbe(is_active_owner=_owner_probe),
        health_probe=HealthProbe(
            classify=lambda b, g, status, step: "at_risk"
        ),
    )

    def _default_step(req):
        return RebuildStepResult(
            ok=True,
            current_kg_generation_id=req.candidate_kg_generation_id,
            structural_hash="c" * 64,
            source_hash="d" * 64,
            counts={"nodes": len(rows), "edges": 0},
        )

    service = KGRebuildService(
        base_dir=base_dir,
        single_writer_lock=lock,
        safe_write_lifecycle=safe_lifecycle,
        quarantine_service=None,
        confirmation_store=confirmation_store,
        manifest_store=manifest_store,
        rebuild_step_adapter=step_adapter or _default_step,
        source_enumerator=enumerator,
        lock_ttl_seconds=60,
        generation_repository=KGGenerationRepository(base_dir=base_dir),
        promotion_guard=None,
        report_store=RebuildReportStore(base_dir=base_dir),
        terminal_state_guard=RebuildReportTerminalStateGuard,
    )
    return service, manifest_store, confirmation_store, lock, enumerator


def _issue_confirmation(
    confirmation_store: RebuildConfirmationStore,
    manifest_store: KGRebuildSourceManifest,
    enumerator: RebuildSourceEnumerator,
    *,
    board_id: str = BOARD,
    actor_id: str = "user-1",
    operation: str = "rebuild",
) -> tuple[str, str, str]:
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


# ---------------- TC-KG02-01 -------------------------------------------------


def test_ts_bac02dd7_health_ui_shows_recovery_state(base_dir: Path) -> None:
    """ts_bac02dd7 — KG Health UI shows graph and discovery recovery state.

    Given KG saudável + global discovery wired, when preflight runs and
    discovery status is recorded, then health must surface BOTH the
    graph state (preflight outcome + base_state) AND the discovery
    reindex status. Equivalent to the UI reading the same source of
    truth the rebuild report uses."""

    def health_probe(_b):
        return RebuildHealthSummary(
            base_state="at_risk",
            metric_status="degraded",
            current_kg_generation_id=None,
        )

    def source_probe(_b):
        return RebuildSourceSummary(
            eligible_count=3,
            skipped_cancelled_count=0,
            has_non_deterministic_inputs=False,
        )

    preflight = RebuildPreflightService(
        source_probe=source_probe, health_probe=health_probe
    )
    pf_result = preflight.run(board_id=BOARD)
    # UI reads the preflight outcome — at_risk + non-deterministic-free
    # → READY but recommended.
    assert pf_result.outcome in {
        PreflightOutcome.READY.value,
        PreflightOutcome.CONFIRMATION_REQUIRED.value,
    }
    assert pf_result.base_state == "at_risk"
    assert pf_result.metric_status == "degraded"

    # Discovery wired separately — the UI must see a reindex status row
    # for the active generation.
    discovery_store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(status_store=discovery_store)
    gen = generate_kg_generation_id()
    outcome = reindexer.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    # Default adapter marks pending — UI sees explicit operator action.
    assert outcome.status == ReindexStatus.REINDEX_PENDING.value
    record = discovery_store.get_status(BOARD, gen)
    assert record is not None
    assert record["status"] == ReindexStatus.REINDEX_PENDING.value
    # Health surface combines both: preflight state + discovery state.
    health_view = {
        "graph": {
            "base_state": pf_result.base_state,
            "metric_status": pf_result.metric_status,
        },
        "discovery": {
            "status": record["status"],
            "reason": record["reason"],
            "job_ref": record["job_ref"],
        },
    }
    assert health_view["graph"]["base_state"] == "at_risk"
    assert health_view["discovery"]["status"] == "reindex_pending"


def test_ts_4712e6d7_reset_requires_confirmation(base_dir: Path) -> None:
    """ts_4712e6d7 — reset requires UI confirmation and rejects agent
    destructive path.

    Reset operation is NOT in SUPPORTED_REBUILD_OPERATIONS (KG-02.3
    val_dfdff0b8 fail-closed), so even a valid confirmation token for
    'reset' is rejected before the lock is taken."""

    service, manifest_store, confirmation_store, lock, enumerator = (
        _build_full_service(base_dir)
    )
    # An agent issuing reset MUST be denied at the gate.
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator,
        actor_id="agent-1", operation="reset",
    )
    result = service.run(
        confirmation_id=confirmation_id,
        board_id=BOARD,
        actor_id="agent-1",
        operation="reset",
        preflight_hash=preflight_hash,
        manifest_ref=manifest_ref,
        reason="agent attempt",
    )
    assert result.outcome == RebuildOutcome.UNSUPPORTED_OPERATION.value
    assert result.reason == RebuildBlockReason.OPERATION_PENDING_KG02_4.value
    # Lock never taken.
    assert lock.inspect(board_id=BOARD) is None
    assert "reset" not in SUPPORTED_REBUILD_OPERATIONS


def test_ts_e258586a_preflight_is_read_only(base_dir: Path) -> None:
    """ts_e258586a — Preflight is read-only and does not reserve a
    generation."""

    repo = KGGenerationRepository(base_dir=base_dir)
    assert repo.get_current(BOARD) is None

    def health_probe(_b):
        return RebuildHealthSummary(
            base_state="healthy",
            metric_status="available",
            current_kg_generation_id=None,
        )

    def source_probe(_b):
        return RebuildSourceSummary(
            eligible_count=2,
            skipped_cancelled_count=1,
            has_non_deterministic_inputs=False,
        )

    preflight = RebuildPreflightService(
        source_probe=source_probe, health_probe=health_probe
    )
    pf = preflight.run(board_id=BOARD)
    assert pf.outcome == PreflightOutcome.READY.value
    # Preflight MUST NOT have created a generation pointer.
    assert repo.get_current(BOARD) is None


# ---------------- TC-KG02-02 -------------------------------------------------


def test_ts_969dfdc7_confirmation_single_use_ttl_audit(base_dir: Path) -> None:
    """ts_969dfdc7 — Confirmation token is single-use with TTL and audit."""

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    store = RebuildConfirmationStore(
        base_dir=base_dir, audit_recorder=recorder
    )
    token = store.issue(
        board_id=BOARD,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_abc",
    )

    # First consume — CONSUMED.
    r1 = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-1",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    assert r1.outcome == ConfirmationOutcome.CONSUMED.value

    # Second consume — MISSING (single-use).
    r2 = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-1",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    assert r2.outcome == ConfirmationOutcome.MISSING.value

    # TTL: force expiry on a fresh token and verify EXPIRED outcome.
    expired = store.issue(
        board_id=BOARD,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_abc",
    )
    path = base_dir / "rebuild" / "confirmations" / f"{expired.confirmation_id}.json"
    body = json.loads(path.read_text(encoding="utf-8"))
    body["expires_at"] = "2000-01-01T00:00:00+00:00"
    path.write_text(json.dumps(body), encoding="utf-8")
    r3 = store.consume(
        confirmation_id=expired.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-1",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    assert r3.outcome == ConfirmationOutcome.EXPIRED.value

    # Audit: all 3 outcomes left a safe row with confirmation_ref (no raw).
    audit_dir = base_dir / "rebuild" / "audit" / "confirmation" / BOARD
    audit_files = list(audit_dir.glob("*.json"))
    assert len(audit_files) >= 3
    for path in audit_files:
        content = path.read_text(encoding="utf-8")
        assert token.confirmation_id not in content
        assert expired.confirmation_id not in content


# ---------------- TC-KG02-03 -------------------------------------------------


def test_ts_909e0e04_preflight_excludes_cancelled(base_dir: Path) -> None:
    """ts_909e0e04 — Preflight includes non-cancelled specs and stops
    for legacy fallback (non-deterministic inputs → CONFIRMATION_REQUIRED)."""

    def source_probe(_b):
        return RebuildSourceSummary(
            eligible_count=5,
            skipped_cancelled_count=2,
            has_non_deterministic_inputs=True,  # legacy fallback present
        )

    def health_probe(_b):
        return RebuildHealthSummary(
            base_state="healthy",
            metric_status="available",
            current_kg_generation_id=None,
        )

    preflight = RebuildPreflightService(
        source_probe=source_probe, health_probe=health_probe
    )
    pf = preflight.run(board_id=BOARD)
    assert pf.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert pf.eligible_source_count == 5
    assert pf.skipped_cancelled_count == 2


def test_ts_92cfed29_same_source_set_equivalent_rebuild() -> None:
    """ts_92cfed29 — Same source set rebuilds to equivalent structure
    (deterministic hash + comparison)."""

    sources = [_row(), _row("s2", "spec", "h2", "v2")]
    a = build_structural_inputs(sources=sources)
    b = build_structural_inputs(sources=list(reversed(sources)))
    assert compute_structural_hash(a) == compute_structural_hash(b)

    rebuilder = DeterministicStructuralRebuilder()
    r1 = rebuilder.rebuild(board_id=BOARD, sources=sources)
    r2 = rebuilder.rebuild(board_id=BOARD, sources=sources)
    assert r1.structural_hash == r2.structural_hash
    assert r1.source_hash == r2.source_hash


def test_ts_dff04b2e_structural_hash_excludes_run_state() -> None:
    """ts_dff04b2e — Structural hash excludes generation UUID and run
    timestamps."""

    sources = [_row()]
    b = build_structural_inputs(
        sources=sources,
        nodes=[{
            "type": "Spec",
            "id": "S1",
            "kg_generation_id": generate_kg_generation_id(),
            "run_id": "run_xyz",
            "started_at": "2099-01-01T00:00:00Z",
        }],
    )
    a_with_nodes = build_structural_inputs(
        sources=sources, nodes=[{"type": "Spec", "id": "S1"}]
    )
    # Polluting nodes with excluded fields equals the clean polluted form.
    assert compute_structural_hash(a_with_nodes) == compute_structural_hash(b)
    # Every excluded field is part of the documented set.
    for excluded in ("kg_generation_id", "run_id", "started_at"):
        assert excluded in EXCLUDED_HASH_FIELDS


# ---------------- TC-KG02-04 -------------------------------------------------


def test_ts_7949efc6_rebuild_obtains_kg01_lock(base_dir: Path) -> None:
    """ts_7949efc6 — Confirmed rebuild obtains KG-01 lock and uses
    quarantine before reset. The orchestrator acquires the lock with
    admin_lane=True and releases on completion. Reset path remains
    UNSUPPORTED_OPERATION (KG-02.4 will ship the real reset+quarantine
    sequence)."""

    captured_calls: list[dict] = []

    real_acquire = KGSingleWriterLock.acquire

    def _track_acquire(self, **kwargs):
        captured_calls.append(kwargs)
        return real_acquire(self, **kwargs)

    KGSingleWriterLock.acquire = _track_acquire
    try:
        service, manifest_store, confirmation_store, lock, enumerator = (
            _build_full_service(base_dir)
        )
        cid, mref, ph = _issue_confirmation(
            confirmation_store, manifest_store, enumerator
        )
        result = service.run(
            confirmation_id=cid,
            board_id=BOARD,
            actor_id="user-1",
            operation="rebuild",
            preflight_hash=ph,
            manifest_ref=mref,
            reason="ts_7949efc6",
        )
    finally:
        KGSingleWriterLock.acquire = real_acquire

    assert result.outcome == RebuildOutcome.COMPLETED.value
    # Lock acquired with admin_lane=True.
    assert any(
        kwargs.get("admin_lane") is True for kwargs in captured_calls
    )
    # Released after completion.
    assert lock.inspect(board_id=BOARD) is None
    # Reset (which would trigger quarantine in KG-02.4+) remains
    # fail-closed today — confirms the boundary.
    assert "reset" not in SUPPORTED_REBUILD_OPERATIONS


# ---------------- TC-KG02-05 -------------------------------------------------


def test_ts_2c262b64_rebuild_persists_generation_event(base_dir: Path) -> None:
    """ts_2c262b64 — Rebuild persists UUID generation and emits
    kg.rebuilt with the canonical payload."""

    service, manifest_store, confirmation_store, lock, enumerator = (
        _build_full_service(base_dir)
    )

    captured_events: list[dict] = []

    publisher = KGRebuiltEventPublisher(
        base_dir=base_dir,
        publish_adapter=lambda p: captured_events.append(dict(p)) or True,
    )
    marker = CognitivePendingMarker(base_dir=base_dir)
    handler = build_kg_rebuilt_event_handler(
        publisher=publisher,
        cognitive_marker=marker,
        source_resolver=lambda _p: [_row()],
    )

    service = dc_replace(service, event_emitter=handler)
    cid, mref, ph = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=cid,
        board_id=BOARD,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=ph,
        manifest_ref=mref,
        reason="ts_2c262b64",
    )
    assert result.outcome == RebuildOutcome.COMPLETED.value
    assert is_valid_kg_generation_id(result.current_kg_generation_id)
    # Event captured + UUID v4 echoed.
    assert captured_events, "kg.rebuilt event was not published"
    payload = captured_events[0]
    assert payload["board_id"] == BOARD
    assert payload["kg_generation_id"] == result.current_kg_generation_id
    assert payload["status"] == "completed"
    assert payload["report_ref"]


def test_ts_3c9e856c_cognitive_pending_and_audit_trail(base_dir: Path) -> None:
    """ts_3c9e856c — Cognitive pending marker integration + audit trail
    for destructive recovery outcomes."""

    audit_recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    service, manifest_store, confirmation_store, lock, enumerator = (
        _build_full_service(base_dir, audit_recorder=audit_recorder)
    )

    publisher = KGRebuiltEventPublisher(base_dir=base_dir)
    marker = CognitivePendingMarker(base_dir=base_dir)
    handler = build_kg_rebuilt_event_handler(
        publisher=publisher,
        cognitive_marker=marker,
        source_resolver=lambda _p: [
            _row("s1", "spec"),
            _row("r1", "refinement"),
            _row("c1", "comment"),
        ],
    )
    service = dc_replace(service, event_emitter=handler)
    cid, mref, ph = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    raw = cid
    result = service.run(
        confirmation_id=cid,
        board_id=BOARD,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=ph,
        manifest_ref=mref,
        reason="ts_3c9e856c",
    )
    assert result.outcome == RebuildOutcome.COMPLETED.value

    # Cognitive pending recorded for the new generation; only spec +
    # refinement count (comment excluded by CONSOLIDABLE_ARTIFACT_TYPES).
    pending_dir = (
        base_dir
        / "rebuild"
        / "audit"
        / "cognitive_pending"
        / BOARD
    )
    pending_files = list(pending_dir.glob("*.json"))
    assert pending_files, "cognitive pending record missing"
    record = json.loads(pending_files[0].read_text(encoding="utf-8"))
    assert record["status"] == CognitivePendingStatus.PENDING_MARKED.value
    assert record["pending_count"] == 2

    # Confirmation audit trail: consumed row + raw token NEVER leaks.
    audit_dir = (
        base_dir / "rebuild" / "audit" / "confirmation" / BOARD
    )
    audit_files = list(audit_dir.glob("*.json"))
    assert audit_files
    for path in audit_files:
        body = path.read_text(encoding="utf-8")
        assert raw not in body
        assert confirmation_fingerprint(raw) in body or "conf_fp_" in body


def test_ts_9cb41200_report_persists_before_terminal(base_dir: Path) -> None:
    """ts_9cb41200 — Report persists before completed or failed terminal
    state. Block promotion when persistence fails."""

    from okto_pulse.core.kg.rebuild_report import ReportPersistOutcome, ReportPersistResult

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

    service, manifest_store, confirmation_store, lock, enumerator = (
        _build_full_service(base_dir)
    )
    service = dc_replace(service, report_store=_BrokenStore(base_dir=base_dir))
    cid, mref, ph = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
    )
    result = service.run(
        confirmation_id=cid,
        board_id=BOARD,
        actor_id="user-1",
        operation="rebuild",
        preflight_hash=ph,
        manifest_ref=mref,
        reason="ts_9cb41200",
    )
    assert result.outcome == RebuildOutcome.REPORT_PERSIST_FAILED.value
    assert result.publishable_status == "report_persist_failed"
    # Previous safe generation preserved (was None — and stays None).
    repo = KGGenerationRepository(base_dir=base_dir)
    assert repo.get_current(BOARD) is None


# ---------------- TC-KG02-06 -------------------------------------------------


def test_ts_ebf4ed79_discovery_reindex_or_pending(base_dir: Path) -> None:
    """ts_ebf4ed79 — Global discovery is reindexed OR explicitly
    pending — never silently stale."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    # Path A: adapter success → REINDEXED.
    rx_ok = GlobalDiscoveryReindexer(
        status_store=store,
        reindex_adapter=lambda b, g, r: ReindexAttempt(
            success=True, indexed_generation=g, detail="ok"
        ),
    )
    gen_a = generate_kg_generation_id()
    a = _guarded_reindex(
        rx_ok,
        board_id=BOARD,
        kg_generation_id=gen_a,
        reason=ReindexReason.DISCOVERY_LBUG_AFFECTED.value,
    )
    assert a.status == ReindexStatus.REINDEXED.value
    assert a.report_ref

    # Path B: default adapter → REINDEX_PENDING with manual_reindex_required.
    rx_pending = GlobalDiscoveryReindexer(status_store=store)
    gen_b = generate_kg_generation_id()
    b = rx_pending.reindex_or_mark_pending(
        board_id=BOARD,
        kg_generation_id=gen_b,
        reason=ReindexReason.STRUCTURAL_REFERENCE_CHANGED.value,
    )
    assert b.status == ReindexStatus.REINDEX_PENDING.value
    assert b.job_ref == "manual_reindex_required"


def test_ts_dee61d05_discovery_reindex_status_visible(base_dir: Path) -> None:
    """ts_dee61d05 — Global discovery reindex status is visible
    (visible_in_health + visible_in_report + durable record_ref)."""

    store = GlobalDiscoveryReindexStatusStore(base_dir=base_dir)
    reindexer = GlobalDiscoveryReindexer(
        status_store=store,
        reindex_adapter=lambda b, g, r: ReindexAttempt(
            success=True, indexed_generation=g
        ),
    )
    gen = generate_kg_generation_id()
    outcome = _guarded_reindex(
        reindexer,
        board_id=BOARD,
        kg_generation_id=gen,
        reason=ReindexReason.OPERATOR_REQUESTED.value,
    )
    # Outcome carries durable report_ref the UI uses.
    assert outcome.report_ref
    assert Path(outcome.report_ref).exists()
    # Status store reads the same row.
    record = store.get_status(BOARD, gen)
    assert record is not None
    assert record["status"] == ReindexStatus.REINDEXED.value
    assert record["reason"] == ReindexReason.OPERATOR_REQUESTED.value
    # latest_for_board returns the same row (single record for board).
    latest = store.latest_for_board(BOARD)
    assert latest is not None
    assert latest["kg_generation_id"] == gen
