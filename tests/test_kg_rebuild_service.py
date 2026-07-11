"""KG-02.3 — Rebuild service admin lane tests.

Covers AC7/AC8/AC15, IR ir_03c2a132 + ir_73c3e169, TR10/TR12, OR
or_37cebd03.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

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
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
from coordination_fakes import FakeWriteLockPort
from kg_registry_testing import (
    RealBoardCypherExecutorForTests,
    RealBoardGraphLifecycleForTests,
    RealBoardGraphTransactionForTests,
    configure_test_kg_registry,
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
            return "owner" if candidate_board_id == board_id else None

    async def _fake_uow():
        yield SimpleNamespace(
            boards=_Boards(),
            services=SimpleNamespace(shares=_Shares()),
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
) -> tuple[
    KGRebuildService,
    KGRebuildSourceManifest,
    RebuildConfirmationStore,
    KGSingleWriterLock,
]:
    """Wire a fully-functional rebuild service against tmp storage."""
    rows = source_rows if source_rows is not None else [_row()]
    lock = KGSingleWriterLock(
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

    service = KGRebuildService(
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
    service, _ms, _cs, lock = _build_service(tmp_path)
    result = service.run(
        confirmation_id="conf_does_not_exist",
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_abc",
        reason="r",
    )
    assert result.outcome == RebuildOutcome.CONFIRMATION_REQUIRED.value
    assert result.reason == RebuildBlockReason.CONFIRMATION_INVALID.value
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
    """Source set changed between preflight and run → MANIFEST_DRIFT."""
    initial_rows = [_row()]
    service, manifest_store, confirmation_store, lock = _build_service(
        tmp_path,
        source_rows=initial_rows,
    )
    enumerator = service.source_enumerator
    confirmation_id, manifest_ref, preflight_hash = _issue_confirmation(
        confirmation_store, manifest_store, enumerator
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
    # Lock NEVER acquired (drift caught BEFORE lock).
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
    service.run(
        confirmation_id="conf_nope",
        board_id="b1",
        actor_id="user-1",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_x",
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
        manifest_ref = pre.json()["manifest_ref"]
        preflight_hash = pre.json()["preflight_hash"]

        conf = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b-reset",
                "operation": operation,
                "preflight_hash": preflight_hash,
                "manifest_ref": manifest_ref,
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
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    async def _fake_health(board_id, db, scheduler_control=None):
        return {
            "graph_state": "healthy",
            "metric_status": "available",
            "current_kg_generation_id": None,
        }

    monkeypatch.setattr(kg_rebuild_mod, "get_kg_health", _fake_health)

    app = _make_rebuild_test_app(board_id="b-endpoint")
    paths = set(app.openapi()["paths"])
    assert "/api/v1/kg/rebuild/run" in paths

    async def _fake_user():
        return "user-run-test"

    app.dependency_overrides[require_user] = _fake_user

    registry = get_kg_registry()
    original_step_adapter = registry.graph_lifecycle.apply_step

    def _missing_real_lifecycle_adapter(board_id: str, graph_type: str, step: str):
        return LifecycleStepResult(
            ok=False, detail="real lifecycle adapter unavailable"
        )

    registry.graph_lifecycle.apply_step = _missing_real_lifecycle_adapter

    # Full lifecycle via the real endpoints: preflight → confirm → run.
    try:
        with TestClient(app) as client:
            pre = client.post(
                "/api/v1/kg/rebuild/preflight",
                params={"board_id": "b-endpoint"},
            )
            assert pre.status_code == 200
            manifest_ref = pre.json()["manifest_ref"]
            preflight_hash = pre.json()["preflight_hash"]

            conf = client.post(
                "/api/v1/kg/rebuild/confirm",
                json={
                    "board_id": "b-endpoint",
                    "operation": "rebuild",
                    "preflight_hash": preflight_hash,
                    "manifest_ref": manifest_ref,
                },
            )
            assert conf.status_code == 200
            confirmation_id = conf.json()["confirmation_id"]

            run = client.post(
                "/api/v1/kg/rebuild/run",
                json={
                    "confirmation_id": confirmation_id,
                    "board_id": "b-endpoint",
                    "operation": "rebuild",
                    "preflight_hash": preflight_hash,
                    "manifest_ref": manifest_ref,
                    "reason": "endpoint integration smoke",
                },
            )
    finally:
        registry.graph_lifecycle.apply_step = original_step_adapter
    assert run.status_code == 200, run.text
    body = run.json()
    # The endpoint smoke test does not start the consolidation worker nor seed
    # a real Ladybug graph. Production wiring must therefore fail closed at the
    # real lifecycle boundary instead of using a fake ok=True adapter and
    # reporting a completed rebuild with no reopenable graph.
    assert body["outcome"] == RebuildOutcome.FAILED.value
    assert body["reason"] == RebuildBlockReason.LIFECYCLE_FAILED.value
    assert body["run_id"].startswith("run_")
    assert body["audit_ref"]


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
        tmp_path, step_adapter=step_adapter
    )
    generation_repo = KGGenerationRepository(base_dir=tmp_path)
    rep_store = report_store or RebuildReportStore(base_dir=tmp_path)
    event_log: list[dict] = [] if event_sink is None else event_sink

    def _emit(payload):
        event_log.append(payload)

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
    from okto_pulse.core.kg.primitives import _apply_kuzu_node_create_with_timestamp
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
        _apply_kuzu_node_create_with_timestamp(
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
            kuzu_conn=kconn,
            sqlite_session=None,
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
