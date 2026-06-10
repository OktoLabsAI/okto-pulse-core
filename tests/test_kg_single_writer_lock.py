"""KG-01.3 — KGSingleWriterLock + KGSafeWriteLifecycle (FR5, FR6).

Deterministic tests against the cross-process lock primitive and the
safe write lifecycle wrapper. No real LadybugDB — adapter callables
are fakes that exercise success, blocked and failed paths.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from okto_pulse.core.kg.safe_write_lifecycle import (
    CANONICAL_GRAPH_TYPES,
    CANONICAL_STEP_ORDER,
    DEFAULT_REQUIRED_STEPS,
    HealthProbe,
    KGSafeWriteLifecycle,
    LifecycleStepResult,
    LockOwnerProbe,
    SafeWriteLifecycleError,
    SafeWriteLifecycleErrorCode,
    SafeWriteLifecycleResponse,
    SafeWriteLifecycleStatus,
    STEP_CHECKPOINT,
    STEP_CLOSE_REOPEN_PROBE,
    STEP_FLUSH,
    STEP_FSYNC,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    get_lifecycle_counter,
    get_lifecycle_counter_labels,
    get_lifecycle_counter_samples,
    reset_lifecycle_counter,
)
from okto_pulse.core.kg.single_writer_lock import (
    DEFAULT_TTL_SECONDS,
    KGSingleWriterLock,
    LOCK_FILENAME,
    MAX_TTL_SECONDS,
    SingleWriterLockError,
    get_lock_counter,
    get_lock_counter_labels,
    get_lock_counter_samples,
    reset_lock_counter,
)
from okto_pulse.core.kg.write_barrier import (
    BarrierMode,
    WriteLifecycleViolation,
    get_barrier_mode,
    get_unguarded_count,
    require_write_token,
    reset_unguarded_counter,
    set_barrier_mode,
    under_safe_write,
)


@pytest.fixture(autouse=True)
def _reset_counters_and_mode():
    reset_lock_counter()
    reset_lifecycle_counter()
    reset_unguarded_counter()
    prior_mode = get_barrier_mode()
    yield
    reset_lock_counter()
    reset_lifecycle_counter()
    reset_unguarded_counter()
    set_barrier_mode(prior_mode)


# --- Helpers ------------------------------------------------------------------


@pytest.fixture
def lock_root(tmp_path: Path) -> Path:
    root = tmp_path / "kg-storage"
    root.mkdir()
    return root


@pytest.fixture
def lock(lock_root: Path) -> KGSingleWriterLock:
    return KGSingleWriterLock(base_dir=lock_root)


# --- FR5 / api_2f4b18e4: acquire / release ------------------------------------


def test_acquire_returns_owner_token_and_expires_at(lock: KGSingleWriterLock):
    result = lock.acquire(
        board_id="b1",
        operation="consolidate",
        owner_id="worker-1",
        ttl_seconds=30,
    )
    assert result.acquired is True
    assert isinstance(result.owner_token, str) and len(result.owner_token) >= 16
    assert isinstance(result.expires_at, str) and "T" in result.expires_at
    assert result.current_owner == "worker-1"
    assert result.admin_lane is False


def test_second_acquire_returns_contention(lock: KGSingleWriterLock):
    first = lock.acquire(
        board_id="b1", operation="op", owner_id="w1", ttl_seconds=60
    )
    assert first.acquired is True

    second = lock.acquire(
        board_id="b1", operation="op", owner_id="w2", ttl_seconds=60
    )
    assert second.acquired is False
    assert second.owner_token is None
    assert second.current_owner == "w1"
    assert second.expires_at == first.expires_at


def test_release_with_correct_token_frees_lock(lock: KGSingleWriterLock):
    first = lock.acquire(
        board_id="b1", operation="op", owner_id="w1", ttl_seconds=60
    )
    assert lock.release(board_id="b1", owner_token=first.owner_token) is True

    # Now another worker can acquire.
    second = lock.acquire(
        board_id="b1", operation="op", owner_id="w2", ttl_seconds=60
    )
    assert second.acquired is True
    assert second.owner_token != first.owner_token


def test_release_with_wrong_token_keeps_lock(lock: KGSingleWriterLock):
    first = lock.acquire(
        board_id="b1", operation="op", owner_id="w1", ttl_seconds=60
    )
    assert lock.release(board_id="b1", owner_token="forged") is False

    # Contention still present.
    second = lock.acquire(
        board_id="b1", operation="op", owner_id="w2", ttl_seconds=60
    )
    assert second.acquired is False
    assert second.current_owner == "w1"


def test_admin_lane_flag_propagates(lock: KGSingleWriterLock):
    result = lock.acquire(
        board_id="b1",
        operation="rebuild",
        owner_id="admin",
        ttl_seconds=60,
        admin_lane=True,
    )
    assert result.acquired is True
    assert result.admin_lane is True

    manifest = lock.inspect(board_id="b1")
    assert manifest is not None
    assert manifest.admin_lane is True
    assert manifest.operation == "rebuild"


def test_distinct_boards_have_independent_locks(lock: KGSingleWriterLock):
    a = lock.acquire(board_id="ba", operation="op", owner_id="w1", ttl_seconds=60)
    b = lock.acquire(board_id="bb", operation="op", owner_id="w2", ttl_seconds=60)
    assert a.acquired is True
    assert b.acquired is True
    assert a.owner_token != b.owner_token


# --- Stale recovery (TR7-ish): expired locks are reclaimable -----------------


def test_stale_lock_is_recovered_after_ttl(lock_root: Path):
    fast_lock = KGSingleWriterLock(base_dir=lock_root)
    first = fast_lock.acquire(
        board_id="b1", operation="op", owner_id="dead-worker", ttl_seconds=1
    )
    assert first.acquired is True
    time.sleep(1.2)  # let TTL expire

    second = fast_lock.acquire(
        board_id="b1", operation="op", owner_id="rescuer", ttl_seconds=60
    )
    assert second.acquired is True
    assert second.current_owner == "rescuer"
    assert second.owner_token != first.owner_token


def test_acquire_validates_ttl_bounds(lock: KGSingleWriterLock):
    with pytest.raises(ValueError):
        lock.acquire(board_id="b1", operation="op", owner_id="w", ttl_seconds=0)
    with pytest.raises(ValueError):
        lock.acquire(
            board_id="b1",
            operation="op",
            owner_id="w",
            ttl_seconds=MAX_TTL_SECONDS + 1,
        )


def test_manifest_file_is_human_readable_json(lock_root: Path, lock: KGSingleWriterLock):
    lock.acquire(
        board_id="b1", operation="op", owner_id="w1", ttl_seconds=60
    )
    manifest_path = lock_root / "b1" / LOCK_FILENAME
    raw = manifest_path.read_text(encoding="utf-8")
    decoded = json.loads(raw)
    for key in (
        "owner_token",
        "owner_id",
        "operation",
        "acquired_at_epoch",
        "expires_at_epoch",
        "acquired_at",
        "expires_at",
        "admin_lane",
    ):
        assert key in decoded


def test_inspect_returns_none_when_no_lock(lock: KGSingleWriterLock):
    assert lock.inspect(board_id="never-locked") is None


def test_inspect_returns_manifest_when_locked(lock: KGSingleWriterLock):
    lock.acquire(
        board_id="b1", operation="bulk", owner_id="w1", ttl_seconds=60
    )
    manifest = lock.inspect(board_id="b1")
    assert manifest is not None
    assert manifest.owner_id == "w1"
    assert manifest.operation == "bulk"


def test_release_when_no_lock_is_noop(lock: KGSingleWriterLock):
    assert lock.release(board_id="b1", owner_token="anything") is False


# --- FR6 / api_1c9d19e1: safe write lifecycle ----------------------------------


def _always_owner_probe() -> LockOwnerProbe:
    return LockOwnerProbe(is_active_owner=lambda board, token: True)


def _never_owner_probe() -> LockOwnerProbe:
    return LockOwnerProbe(is_active_owner=lambda board, token: False)


def _passing_step_adapter(
    board_id: str, graph_type: str, step: str
) -> LifecycleStepResult:
    return LifecycleStepResult(ok=True)


def _failing_step_adapter(failed_step: str):
    def adapter(board_id: str, graph_type: str, step: str) -> LifecycleStepResult:
        if step == failed_step:
            return LifecycleStepResult(ok=False, detail=f"forced failure at {step}")
        return LifecycleStepResult(ok=True)

    return adapter


def _raising_step_adapter(raising_step: str):
    def adapter(board_id: str, graph_type: str, step: str) -> LifecycleStepResult:
        if step == raising_step:
            raise RuntimeError(f"boom at {step}")
        return LifecycleStepResult(ok=True)

    return adapter


def test_apply_runs_default_steps_in_canonical_order():
    seen: list[str] = []

    def adapter(board_id: str, graph_type: str, step: str) -> LifecycleStepResult:
        seen.append(step)
        return LifecycleStepResult(ok=True)

    wrapper = KGSafeWriteLifecycle(
        step_adapter=adapter,
        owner_probe=_always_owner_probe(),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="bulk",
        owner_token="ok",
        mutation_ref="run-1",
        required_steps=None,
    )
    assert response.status is SafeWriteLifecycleStatus.APPLIED
    assert seen == list(CANONICAL_STEP_ORDER)
    assert response.applied_steps == CANONICAL_STEP_ORDER
    assert response.failed_step is None
    assert response.health_state_after in {
        "healthy", "at_risk", "backpressure", "recovery_needed",
    }
    assert response.correlation_id


def test_apply_respects_canonical_order_even_when_caller_reorders():
    seen: list[str] = []

    def adapter(board_id: str, graph_type: str, step: str) -> LifecycleStepResult:
        seen.append(step)
        return LifecycleStepResult(ok=True)

    wrapper = KGSafeWriteLifecycle(
        step_adapter=adapter,
        owner_probe=_always_owner_probe(),
    )
    wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="bulk",
        owner_token="ok",
        mutation_ref="run-1",
        required_steps=[STEP_FSYNC, STEP_CHECKPOINT, STEP_FLUSH],
    )
    assert seen == [STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC]


def test_apply_owner_token_required_when_missing():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
    )
    with pytest.raises(SafeWriteLifecycleError) as excinfo:
        wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="op",
            owner_token=None,
            mutation_ref="run-1",
        )
    assert excinfo.value.code is SafeWriteLifecycleErrorCode.OWNER_TOKEN_REQUIRED


def test_apply_owner_token_required_when_probe_rejects():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_never_owner_probe(),
    )
    with pytest.raises(SafeWriteLifecycleError) as excinfo:
        wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="op",
            owner_token="stale-token",
            mutation_ref="run-1",
        )
    assert excinfo.value.code is SafeWriteLifecycleErrorCode.OWNER_TOKEN_REQUIRED


def test_apply_boundary_violation_on_unknown_graph_type():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
    )
    with pytest.raises(SafeWriteLifecycleError) as excinfo:
        wrapper.apply(
            board_id="b1",
            graph_type="some_other_graph",
            operation="op",
            owner_token="ok",
            mutation_ref="run-1",
        )
    assert excinfo.value.code is SafeWriteLifecycleErrorCode.BOUNDARY_VIOLATION


def test_apply_boundary_violation_on_unknown_step():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
    )
    with pytest.raises(SafeWriteLifecycleError) as excinfo:
        wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="op",
            owner_token="ok",
            mutation_ref="run-1",
            required_steps=["sing", "dance"],
        )
    assert excinfo.value.code is SafeWriteLifecycleErrorCode.BOUNDARY_VIOLATION


def test_apply_returns_failed_when_step_returns_false():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_failing_step_adapter(STEP_FLUSH),
        owner_probe=_always_owner_probe(),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="op",
        owner_token="ok",
        mutation_ref="run-1",
    )
    assert response.status is SafeWriteLifecycleStatus.FAILED
    assert response.failed_step == STEP_FLUSH
    # Only checkpoint succeeded before flush failed.
    assert response.applied_steps == (STEP_CHECKPOINT,)
    assert response.health_state_after in {"recovery_needed", "at_risk"}


def test_apply_returns_failed_when_step_raises():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_raising_step_adapter(STEP_FSYNC),
        owner_probe=_always_owner_probe(),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="op",
        owner_token="ok",
        mutation_ref="run-1",
    )
    assert response.status is SafeWriteLifecycleStatus.FAILED
    assert response.failed_step == STEP_FSYNC
    assert response.applied_steps == (STEP_CHECKPOINT, STEP_FLUSH)


def test_apply_never_returns_quarantined_health_state():
    """Spec: health_state_after is restricted to 4 values, NOT quarantined.
    Quarantine is the quarantine service's call, not the lifecycle's."""

    def probe(board_id, graph_type, status, failed_step):
        return "quarantined"  # adversarial probe

    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
        health_probe=HealthProbe(classify=probe),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="op",
        owner_token="ok",
        mutation_ref="run-1",
    )
    assert response.health_state_after != "quarantined"
    assert response.health_state_after in {
        "healthy", "at_risk", "backpressure", "recovery_needed",
    }


def test_apply_handles_global_discovery_graph_type():
    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="global_discovery",
        operation="op",
        owner_token="ok",
        mutation_ref="run-1",
        required_steps=[STEP_CHECKPOINT, STEP_FSYNC],
    )
    assert response.status is SafeWriteLifecycleStatus.APPLIED
    assert response.applied_steps == (STEP_CHECKPOINT, STEP_FSYNC)


def test_canonical_constants_match_contract():
    assert CANONICAL_GRAPH_TYPES == frozenset({"board_graph", "global_discovery"})
    assert CANONICAL_STEP_ORDER == (
        STEP_CHECKPOINT, STEP_FLUSH, STEP_FSYNC, STEP_CLOSE_REOPEN_PROBE,
    )
    assert DEFAULT_REQUIRED_STEPS == CANONICAL_STEP_ORDER


# --- Integration sketch: lock + lifecycle ------------------------------------


def test_lock_token_can_drive_lifecycle(lock: KGSingleWriterLock):
    acquisition = lock.acquire(
        board_id="b1", operation="op", owner_id="w", ttl_seconds=60
    )

    def probe(board_id, owner_token):
        manifest = lock.inspect(board_id=board_id)
        return manifest is not None and manifest.owner_token == owner_token

    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=LockOwnerProbe(is_active_owner=probe),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="op",
        owner_token=acquisition.owner_token,
        mutation_ref="run-1",
    )
    assert response.status is SafeWriteLifecycleStatus.APPLIED

    # After releasing the lock the same lifecycle call MUST be refused.
    lock.release(board_id="b1", owner_token=acquisition.owner_token)
    with pytest.raises(SafeWriteLifecycleError) as excinfo:
        wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="op",
            owner_token=acquisition.owner_token,
            mutation_ref="run-2",
        )
    assert excinfo.value.code is SafeWriteLifecycleErrorCode.OWNER_TOKEN_REQUIRED


# --- Validator val_75dee856 rework: stale recovery race regression -----------


def test_stale_recovery_does_not_delete_fresh_lock_placed_by_other_recoverer(
    lock_root: Path, lock: KGSingleWriterLock, monkeypatch: pytest.MonkeyPatch,
):
    """val_75dee856 bloqueio 2: race entre (read stale) e (unlink stale)
    permitia apagar o lock fresco de outro recoverer. O fix faz CAS: re-le
    o manifest antes do unlink e aborta se mudou.

    Repro determinística: monkeypatch ``_read_manifest`` para retornar o
    stale na primeira chamada (deteccao do stale) e um manifest novo na
    segunda (revalidacao). O acquire deve abortar sem unlink e relatar
    contention com o novo owner.
    """
    # Step 1: place a stale lock.
    fast = KGSingleWriterLock(base_dir=lock_root)
    stale = fast.acquire(
        board_id="b1", operation="op", owner_id="stale-worker", ttl_seconds=1
    )
    assert stale.acquired is True
    time.sleep(1.2)

    import okto_pulse.core.kg.single_writer_lock as module

    # The on-disk manifest is currently stale. Build a synthetic
    # "fresh" manifest that the revalidation step will see — same path,
    # different owner_token / expires_at / owner_id.
    fresh_manifest = module.LockManifest(
        owner_token="other-token-xyz",
        owner_id="other-recoverer",
        operation="op",
        acquired_at_epoch=time.time(),
        expires_at_epoch=time.time() + 60,
        admin_lane=False,
    )

    real_read = module._read_manifest
    call_count = {"n": 0}

    def patched_read(path):
        # First call: return the actual stale manifest (so we go into
        # the stale recovery branch). Second call (revalidation inside
        # the recovery-lock): return the fresh manifest so the CAS
        # aborts. Subsequent reads (contention reporting, etc) return
        # the real manifest.
        call_count["n"] += 1
        if call_count["n"] == 1:
            return real_read(path)
        if call_count["n"] == 2:
            return fresh_manifest
        return real_read(path)

    primary_path = lock_root / "b1" / module.LOCK_FILENAME
    real_unlink = os.unlink
    unlink_called_on_primary = {"n": 0}

    def spy_unlink(path):
        if str(path) == str(primary_path):
            unlink_called_on_primary["n"] += 1
        return real_unlink(path)

    monkeypatch.setattr(module, "_read_manifest", patched_read)
    monkeypatch.setattr(module.os, "unlink", spy_unlink)

    result = lock.acquire(
        board_id="b1", operation="op", owner_id="original-recoverer", ttl_seconds=60
    )

    # MUST report contention against the synthetic fresh owner.
    assert result.acquired is False
    assert result.current_owner == "other-recoverer"
    # And MUST NOT have unlinked the PRIMARY sentinel (the recovery-lock
    # may be unlinked freely — that's the auxiliary sentinel).
    assert unlink_called_on_primary["n"] == 0
    # Counter records the aborted recovery as contention, not as a
    # successful stale recovery.
    assert (
        get_lock_counter("b1", "contention") >= 1
    )
    assert (
        get_lock_counter("b1", "acquired", stale_recovered=True) == 0
    )


# --- OR or_a6018284 counter shape --------------------------------------------


def test_kg_single_writer_lock_total_carries_required_or_labels(
    lock: KGSingleWriterLock,
):
    """OR or_a6018284: labels (board_id, operation, outcome, stale_recovered)."""
    assert get_lock_counter_labels() == (
        "board_id", "operation", "outcome", "stale_recovered",
    )

    a = lock.acquire(board_id="b1", operation="consolidate", owner_id="w", ttl_seconds=60)
    # contention
    lock.acquire(board_id="b1", operation="consolidate", owner_id="w2", ttl_seconds=60)
    # release happy path
    lock.release(board_id="b1", owner_token=a.owner_token)
    # release token mismatch
    fresh = lock.acquire(board_id="b1", operation="consolidate", owner_id="w3", ttl_seconds=60)
    lock.release(board_id="b1", owner_token="forged")
    lock.release(board_id="b1", owner_token=fresh.owner_token)

    samples = get_lock_counter_samples()
    outcomes = {(s["operation"], s["outcome"]) for s in samples}
    assert ("consolidate", "acquired") in outcomes
    assert ("consolidate", "contention") in outcomes
    assert ("consolidate", "released") in outcomes
    assert ("consolidate", "release_token_mismatch") in outcomes

    # Every sample carries the 5 required-ish columns + count.
    for s in samples:
        for label in get_lock_counter_labels():
            assert label in s
            assert isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


def test_kg_single_writer_lock_total_marks_stale_recovered_true(
    lock_root: Path,
):
    fast = KGSingleWriterLock(base_dir=lock_root)
    fast.acquire(board_id="b1", operation="op", owner_id="dead", ttl_seconds=1)
    time.sleep(1.2)
    fast.acquire(board_id="b1", operation="op", owner_id="rescuer", ttl_seconds=60)
    assert get_lock_counter("b1", "acquired", stale_recovered=True) == 1


# --- OR or_a0162af4: lifecycle counter ----------------------------------------


def test_kg_safe_write_lifecycle_total_carries_required_or_labels():
    assert get_lifecycle_counter_labels() == (
        "board_id", "operation", "graph_type", "outcome", "failed_step_bucket",
    )

    wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_always_owner_probe(),
    )
    wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="consolidate",
        owner_token="ok",
        mutation_ref="run-1",
    )
    # boundary violation: unknown graph_type
    with pytest.raises(SafeWriteLifecycleError):
        wrapper.apply(
            board_id="b1",
            graph_type="bogus",
            operation="consolidate",
            owner_token="ok",
            mutation_ref="run-2",
        )
    # owner_token_required
    bad_wrapper = KGSafeWriteLifecycle(
        step_adapter=_passing_step_adapter,
        owner_probe=_never_owner_probe(),
    )
    with pytest.raises(SafeWriteLifecycleError):
        bad_wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="consolidate",
            owner_token="x",
            mutation_ref="run-3",
        )
    # failed step
    failing_wrapper = KGSafeWriteLifecycle(
        step_adapter=_failing_step_adapter(STEP_FLUSH),
        owner_probe=_always_owner_probe(),
    )
    failing_wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="consolidate",
        owner_token="ok",
        mutation_ref="run-4",
    )

    samples = get_lifecycle_counter_samples()
    keys = {
        (s["operation"], s["graph_type"], s["outcome"], s["failed_step_bucket"])
        for s in samples
    }
    assert ("consolidate", "board_graph", "applied", "none") in keys
    assert ("consolidate", "bogus", "boundary_violation", "boundary") in keys
    assert ("consolidate", "board_graph", "owner_token_required", "boundary") in keys
    assert ("consolidate", "board_graph", "failed", STEP_FLUSH) in keys

    for s in samples:
        for label in get_lifecycle_counter_labels():
            assert label in s and isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


# --- Write barrier (FR5/FR6 enforcement) -------------------------------------


def test_require_write_token_strict_raises_when_no_guard():
    set_barrier_mode(BarrierMode.STRICT)
    with pytest.raises(WriteLifecycleViolation):
        require_write_token("b1")


def test_require_write_token_strict_passes_inside_guard():
    set_barrier_mode(BarrierMode.STRICT)
    with under_safe_write("b1", "token-x", "consolidate"):
        guard = require_write_token("b1")
        assert guard is not None
        assert guard.owner_token == "token-x"
        assert guard.operation == "consolidate"


def test_require_write_token_strict_rejects_token_mismatch():
    set_barrier_mode(BarrierMode.STRICT)
    with under_safe_write("b1", "good-token", "op"):
        with pytest.raises(WriteLifecycleViolation):
            require_write_token("b1", expected_owner_token="other-token")


def test_require_write_token_soft_logs_and_bumps_counter():
    set_barrier_mode(BarrierMode.SOFT)
    # No guard active.
    result = require_write_token("b1")
    assert result is None
    assert get_unguarded_count("b1", mode=BarrierMode.SOFT) >= 1


def test_safe_write_lifecycle_installs_guard_for_steps():
    """KGSafeWriteLifecycle.apply must enter under_safe_write so that
    downstream mutation primitives (commit_consolidation et al.) see the
    guard active for the duration of the lifecycle steps."""
    set_barrier_mode(BarrierMode.STRICT)

    observed_guard: dict[str, str | None] = {"board": None, "token": None}

    def probing_adapter(board_id: str, graph_type: str, step: str):
        # Inside a step, the barrier MUST see an active guard.
        guard = require_write_token(board_id)
        assert guard is not None
        observed_guard["board"] = guard.board_id
        observed_guard["token"] = guard.owner_token
        return LifecycleStepResult(ok=True)

    wrapper = KGSafeWriteLifecycle(
        step_adapter=probing_adapter,
        owner_probe=_always_owner_probe(),
    )
    response = wrapper.apply(
        board_id="b1",
        graph_type="board_graph",
        operation="consolidate",
        owner_token="lifecycle-token",
        mutation_ref="run-1",
    )
    assert response.status is SafeWriteLifecycleStatus.APPLIED
    assert observed_guard["board"] == "b1"
    assert observed_guard["token"] == "lifecycle-token"

    # After the lifecycle exits, the guard is gone — direct calls fail.
    with pytest.raises(WriteLifecycleViolation):
        require_write_token("b1")


def test_unguarded_write_outside_lifecycle_is_blocked_in_strict_mode():
    """val_75dee856 bloqueio 1: a barreira deve flagar/bloquear writes
    diretos. Em STRICT a primitiva require_write_token raise antes do
    write tocar storage."""
    set_barrier_mode(BarrierMode.STRICT)
    # A "direct call" would do: lock.acquire(...) then call commit_consolidation
    # without entering under_safe_write. We simulate the require check:
    with pytest.raises(WriteLifecycleViolation):
        require_write_token("b1")


def test_stale_recovery_lock_serialises_recoverers(
    lock_root: Path, monkeypatch: pytest.MonkeyPatch,
):
    """val_3f8803ab regression: even after CAS, a window existed between
    revalidation and unlink. The fix is an auxiliary recovery-lock
    (.write.lock.recovering) created via O_CREAT|O_EXCL. Only one
    recoverer at a time can run the unlink+create sequence — concurrent
    recoverers see the recovery-lock and bail to contention.

    Repro: hold the recovery-lock manually (simulating recoverer A in
    the critical section), then have recoverer B try to acquire on a
    stale primary lock. Recoverer B MUST refuse to touch the primary
    and report contention.
    """
    import okto_pulse.core.kg.single_writer_lock as module

    # Step 1: place a primary stale lock (TTL expired).
    fast = KGSingleWriterLock(base_dir=lock_root)
    fast.acquire(board_id="b1", operation="op", owner_id="stale", ttl_seconds=1)
    time.sleep(1.2)

    # Step 2: pretend recoverer A is currently inside the recovery
    # critical section by creating the auxiliary sentinel manually.
    board_dir = lock_root / "b1"
    recovery_path = board_dir / module.RECOVERY_LOCK_FILENAME
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(recovery_path, flags, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "owner_id": "recoverer-A",
                "operation": "op",
                "board_id": "b1",
                "acquired_at_epoch": time.time(),
                "expires_at_epoch": time.time() + 30,
            },
            fh,
        )

    primary_path = board_dir / module.LOCK_FILENAME
    real_unlink = os.unlink
    primary_unlink_count = {"n": 0}

    def spy_unlink(path):
        if str(path) == str(primary_path):
            primary_unlink_count["n"] += 1
        return real_unlink(path)

    monkeypatch.setattr(module.os, "unlink", spy_unlink)

    # Step 3: recoverer B tries to acquire — the primary is stale but
    # the recovery-lock is held by recoverer A. B MUST return contention
    # WITHOUT touching the primary sentinel.
    lock_b = KGSingleWriterLock(base_dir=lock_root)
    result = lock_b.acquire(
        board_id="b1", operation="op", owner_id="recoverer-B", ttl_seconds=60
    )

    assert result.acquired is False
    assert primary_unlink_count["n"] == 0
    # The stale lock is still there, untouched.
    assert primary_path.exists()
    # And the recovery-lock is still held by A.
    assert recovery_path.exists()


def test_stale_recovery_lock_blocks_with_explicit_error_no_auto_reclaim(
    lock_root: Path,
):
    """val_189fe864: auto-reclaim of .write.lock.recovering reintroduced
    the race it was meant to prevent (two recoverers could both unlink+
    create the recovery-lock in turn and then both enter the critical
    section). Option 1 (chosen): NEVER auto-reclaim. A stale
    recovery-lock raises STALE_LOCK_RECOVERY_FAILED with reason
    `recovery_lock_stale_manual_intervention_required`, leaving cleanup
    to an operator. Primary lock is NEVER touched."""
    import okto_pulse.core.kg.single_writer_lock as module

    # Place stale primary.
    fast = KGSingleWriterLock(base_dir=lock_root)
    fast.acquire(board_id="b1", operation="op", owner_id="dead-primary", ttl_seconds=1)
    time.sleep(1.2)

    # Place a stale recovery-lock by directly writing one with expires_at
    # already in the past.
    board_dir = lock_root / "b1"
    recovery_path = board_dir / module.RECOVERY_LOCK_FILENAME
    primary_path = board_dir / module.LOCK_FILENAME
    with recovery_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "owner_id": "dead-recoverer",
                "operation": "op",
                "board_id": "b1",
                "acquired_at_epoch": time.time() - 60,
                "expires_at_epoch": time.time() - 30,
            },
            fh,
        )

    lock = KGSingleWriterLock(base_dir=lock_root)
    with pytest.raises(SingleWriterLockError) as excinfo:
        lock.acquire(
            board_id="b1", operation="op", owner_id="rescuer", ttl_seconds=60
        )
    assert "recovery_lock_stale_manual_intervention_required" in excinfo.value.reason
    assert "dead-recoverer" in excinfo.value.reason
    # Primary and recovery sentinels are still both there — operator
    # decides whether to wipe them.
    assert recovery_path.exists()
    assert primary_path.exists()


def test_concurrent_recoverers_with_stale_recovery_lock_never_corrupt_primary(
    lock_root: Path,
):
    """val_189fe864 regression: two recoverers racing on a stale
    recovery-lock MUST NOT both delete it and end up corrupting the
    primary. With no auto-reclaim, every recoverer simply errors out
    explicitly and the primary stays whole.

    Sequence:
    1. Primary `.write.lock` exists but is stale.
    2. `.write.lock.recovering` exists but is stale too (crashed recoverer).
    3. Recoverer A and Recoverer B both call acquire.
    4. BOTH must raise STALE_LOCK_RECOVERY_FAILED.
    5. Neither recovery-lock nor primary is mutated by either.
    """
    import okto_pulse.core.kg.single_writer_lock as module

    fast = KGSingleWriterLock(base_dir=lock_root)
    primary_acq = fast.acquire(
        board_id="b1", operation="op", owner_id="dead-primary-owner",
        ttl_seconds=1,
    )
    assert primary_acq.acquired is True
    time.sleep(1.2)

    board_dir = lock_root / "b1"
    primary_path = board_dir / module.LOCK_FILENAME
    recovery_path = board_dir / module.RECOVERY_LOCK_FILENAME
    with recovery_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "owner_id": "dead-recoverer",
                "operation": "op",
                "board_id": "b1",
                "acquired_at_epoch": time.time() - 60,
                "expires_at_epoch": time.time() - 30,
            },
            fh,
        )

    primary_bytes_before = primary_path.read_bytes()
    recovery_bytes_before = recovery_path.read_bytes()

    lock_a = KGSingleWriterLock(base_dir=lock_root)
    lock_b = KGSingleWriterLock(base_dir=lock_root)

    with pytest.raises(SingleWriterLockError) as exc_a:
        lock_a.acquire(
            board_id="b1", operation="op", owner_id="recoverer-A", ttl_seconds=60
        )
    with pytest.raises(SingleWriterLockError) as exc_b:
        lock_b.acquire(
            board_id="b1", operation="op", owner_id="recoverer-B", ttl_seconds=60
        )

    assert "recovery_lock_stale_manual_intervention_required" in exc_a.value.reason
    assert "recovery_lock_stale_manual_intervention_required" in exc_b.value.reason

    # Neither sentinel was touched by either recoverer — the bytes on
    # disk are byte-identical to what we wrote.
    assert primary_path.read_bytes() == primary_bytes_before
    assert recovery_path.read_bytes() == recovery_bytes_before


def test_commit_consolidation_calls_require_write_token():
    """Sanity: the primitive imports and the call is present.

    Statically asserts the wiring without spinning up a Kùzu graph —
    full end-to-end runs in tests/test_kg_primitives.py (out of scope
    here; we only prove the barrier is hooked in).
    """
    import inspect as _inspect
    from okto_pulse.core.kg import primitives as _prim

    src = _inspect.getsource(_prim.commit_consolidation)
    assert "require_write_token" in src
    assert "session.board_id" in src
