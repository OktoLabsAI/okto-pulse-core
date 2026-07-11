"""KG-01.6 release stress entrypoint (FR8 / TR10 / IR ir_b3116b9c / ir_77b71dee).

OPT-IN: marked with ``@pytest.mark.stress`` so the default suite skips it.
Run explicitly via:

    pytest -m stress tests/stress/test_kg_ci_destructive_release.py

Or via the standalone script:

    python scripts/run_kg_ci_destructive_stress.py

This test is the canonical release-evidence path the validator
(val_67950216) required: it drives ``KGStressProfileRunner`` with
``profile=ci_destructive``, the release-qualified ``KGChaosExecutor``,
the full 1000-iteration floor and every canonical ``ChaosMode``.

Runtime budget: KGChaosExecutor's ``sigkill_during_write`` mode sleeps
1.1s per iteration to exercise the lock's TTL-based stale recovery. At
1000 iterations / 5 modes = 200 sigkill iterations × 1.1s ≈ 220s
total just for that one mode. Whole run lands around 4-5 minutes.

Release pass invariants asserted here:
* ``passed=True``
* ``iterations_completed == CI_DESTRUCTIVE_ITERATIONS_FLOOR``
* ``counters.is_release_clean() is True`` — four required counters at 0
* ``iterations_per_mode[mode] >= 1`` for every canonical mode
* ``per_mode_complete`` flag in the evidence file is True
* ``evidence_ref`` file exists on disk
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from coordination_fakes import (
    FakeRebuildAuditArtifactStore,
    FakeWriteLockPort,
)
from okto_pulse.community.adapters.kg_chaos_executor import KGChaosExecutor
from okto_pulse.core.kg.stress_runner import (
    CI_DESTRUCTIVE_ITERATIONS_FLOOR,
    ChaosMode,
    KGStressProfileRunner,
    StressProfile,
)


@pytest.mark.stress
def test_ci_destructive_release_evidence_against_primitives(tmp_path: Path):
    """The canonical release stress run — drives KGChaosExecutor against
    KG-01.1-5 primitives at the TR10 floor (1000 iterations) with all
    five canonical ChaosMode values."""
    chaos_dir = tmp_path / "chaos-scratch"
    evidence_dir = tmp_path / "evidence"

    executor = KGChaosExecutor(
        base_dir=chaos_dir,
        write_lock_port=FakeWriteLockPort(),
        artifact_store=FakeRebuildAuditArtifactStore(
            tmp_path / "chaos-artifacts"
        ),
    )
    # Defence in depth: the runner gate also enforces this, but the
    # release-evidence pipeline should fail loudly if the marker
    # regresses.
    assert getattr(executor, "release_evidence_executor", False) is True

    runner = KGStressProfileRunner(base_dir=evidence_dir, executor=executor)
    response = runner.run(
        profile=StressProfile.CI_DESTRUCTIVE.value,
        iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
        chaos_modes=[m.value for m in ChaosMode],
        seed="kg-01.6-release-evidence",
    )

    assert response.passed is True
    assert response.iterations_completed == CI_DESTRUCTIVE_ITERATIONS_FLOOR
    assert response.counters.is_release_clean() is True
    assert response.counters.corruption_detected == 0
    assert response.counters.wal_truncated == 0
    assert response.counters.orphaned_lock == 0
    assert response.counters.purge_without_quarantine == 0

    # Every canonical mode was actually exercised.
    for mode in ChaosMode:
        assert response.iterations_per_mode[mode.value] >= 1

    # Evidence file matches the contract.
    evidence_path = Path(response.evidence_ref)
    assert evidence_path.exists()
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["per_mode_complete"] is True
    assert payload["below_ci_floor"] is False
    assert payload["iterations_completed"] == CI_DESTRUCTIVE_ITERATIONS_FLOOR
    assert payload["profile"] == StressProfile.CI_DESTRUCTIVE.value
    assert payload["counters"]["corruption_detected"] == 0
    assert payload["counters"]["wal_truncated"] == 0
    assert payload["counters"]["orphaned_lock"] == 0
    assert payload["counters"]["purge_without_quarantine"] == 0
    for mode in ChaosMode:
        assert payload["iterations_per_mode"][mode.value] >= 1
