"""KG-01.6 — KGStressProfileRunner + KGStorageBackendContingency.

Covers contracts api_bb4ff8ae + api_8721ddb7, FR8/FR9, TR10/TR11/TR15/TR16,
and ORs or_bb640ee1, or_6daf13ec, or_ee5a98e4.

No real LadybugDB writes — chaos modes are simulated via injected
executors that return deterministic ChaosOutcomes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_pulse.core.kg.contingency import (
    CONTINGENCY_DIRNAME,
    CONTINGENCY_MANIFEST_FILENAME,
    ContingencyError,
    ContingencyErrorCode,
    KGStorageBackendContingency,
    get_contingency_count,
    get_contingency_counter_labels,
    get_contingency_samples,
    reset_contingency_counter,
)
from okto_pulse.core.kg.stress_runner import (
    CI_DESTRUCTIVE_ITERATIONS_FLOOR,
    CANONICAL_CHAOS_MODES,
    ChaosMode,
    ChaosOutcome,
    EVIDENCE_FILENAME,
    KGStressProfileRunner,
    STRESS_DIRNAME,
    StressError,
    StressErrorCode,
    StressProfile,
    get_stress_corruption_count,
    get_stress_corruption_labels,
    get_stress_corruption_samples,
    get_stress_evidence_count,
    get_stress_evidence_labels,
    get_stress_evidence_samples,
    reset_stress_counters,
)


@pytest.fixture(autouse=True)
def _reset_counters():
    reset_stress_counters()
    reset_contingency_counter()
    yield
    reset_stress_counters()
    reset_contingency_counter()


# --- KGStressProfileRunner ---------------------------------------------------


def _clean_executor(seed, iteration, chaos_mode):
    return ChaosOutcome()


def _every_iter_corrupt_executor(seed, iteration, chaos_mode):
    return ChaosOutcome(
        corruption_detected=True,
        failure_artifact=f"forced_corruption@{iteration}",
    )


def _wal_truncated_every_other(seed, iteration, chaos_mode):
    return ChaosOutcome(wal_truncated=(iteration % 2 == 0))


def test_local_smoke_clean_run_returns_passed_true(tmp_path: Path):
    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_clean_executor
    )
    response = runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=10,
        chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
        seed="deterministic-seed-1",
    )
    assert response.passed is True
    assert response.iterations_completed == 10
    assert response.counters.is_release_clean() is True
    assert response.profile == StressProfile.LOCAL_SMOKE.value
    assert response.seed == "deterministic-seed-1"
    # Evidence file persisted.
    evidence_path = Path(response.evidence_ref)
    assert evidence_path.exists()
    assert evidence_path.name == EVIDENCE_FILENAME
    data = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert data["passed"] is True
    assert data["iterations_completed"] == 10
    assert "corruption_detected" in data["counters"]


def test_local_smoke_with_corruption_returns_passed_false(tmp_path: Path):
    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_every_iter_corrupt_executor
    )
    response = runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=5,
        chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
        seed="seed-corrupt",
    )
    assert response.passed is False
    assert response.counters.corruption_detected == 5
    # Corruption counter bumped.
    assert (
        get_stress_corruption_count(
            StressProfile.LOCAL_SMOKE.value,
            counter="corruption_detected",
        )
        == 5
    )


class _FakeReleaseQualifiedExecutor:
    """Test-only executor that carries the release marker but stays fast.

    val_67950216 made the marker mandatory in CI. For unit-level
    coverage of below-floor / above-floor logic we still want a fast
    callable — KGChaosExecutor's sigkill mode sleeps 1.1s/iter which
    is unacceptable inside the regular suite. This class is the
    test-author's explicit attestation that the executor is "good
    enough for THIS unit test"; the real release evidence comes from
    the opt-in slow test that uses KGChaosExecutor directly.
    """

    release_evidence_executor = True

    def __call__(self, seed, iteration, chaos_mode):
        return ChaosOutcome()


def test_ci_destructive_below_floor_marks_passed_false(tmp_path: Path):
    """TR10: CI destructive needs >=1000 iterations to be release-grade.
    Below the floor we refuse to mark passed=True even on a clean run."""
    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_FakeReleaseQualifiedExecutor()
    )
    response = runner.run(
        profile=StressProfile.CI_DESTRUCTIVE.value,
        iterations=50,  # well below CI_DESTRUCTIVE_ITERATIONS_FLOOR
        chaos_modes=[m.value for m in ChaosMode],
        seed="seed-floor-test",
    )
    assert response.passed is False
    assert response.counters.is_release_clean() is True
    data = json.loads(Path(response.evidence_ref).read_text())
    assert data["below_ci_floor"] is True


def test_ci_destructive_above_floor_clean_marks_passed_true(tmp_path: Path):
    """val_67950216: this unit test uses a release-qualified fake to
    keep the suite fast. The opt-in stress test in
    tests/stress/test_kg_ci_destructive_release.py drives the real
    KGChaosExecutor against the primitives for actual release evidence.
    """
    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_FakeReleaseQualifiedExecutor()
    )
    response = runner.run(
        profile=StressProfile.CI_DESTRUCTIVE.value,
        iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
        chaos_modes=[m.value for m in ChaosMode],
        seed="seed-ci-clean",
    )
    assert response.passed is True
    assert response.iterations_completed == CI_DESTRUCTIVE_ITERATIONS_FLOOR
    assert response.counters.is_release_clean() is True
    for mode in ChaosMode:
        assert response.iterations_per_mode[mode.value] >= 1


def test_executor_exception_counts_as_corruption(tmp_path: Path):
    def boom(seed, iteration, chaos_mode):
        raise RuntimeError(f"chaos exec failed @ {iteration}")

    runner = KGStressProfileRunner(base_dir=tmp_path, executor=boom)
    response = runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=3,
        chaos_modes=[ChaosMode.SIGKILL_DURING_WRITE.value],
        seed="seed-boom",
    )
    assert response.passed is False
    assert response.counters.corruption_detected == 3
    data = json.loads(Path(response.evidence_ref).read_text())
    # Failure artifacts captured (bounded, no raw exception trace).
    assert any(
        "exec_exception=RuntimeError" in a for a in data["failure_artifacts"]
    )


def test_seed_determinism_picks_same_chaos_sequence(tmp_path: Path):
    """The runner uses seeded random.choice for chaos_mode rotation —
    same seed must produce the same sequence."""
    seen_modes: list[str] = []

    def recording_executor(seed, iteration, chaos_mode):
        seen_modes.append(chaos_mode)
        return ChaosOutcome()

    runner_a = KGStressProfileRunner(
        base_dir=tmp_path / "a", executor=recording_executor
    )
    runner_a.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=10,
        chaos_modes=[m.value for m in ChaosMode],
        seed="same-seed",
    )
    first_run = list(seen_modes)
    seen_modes.clear()

    runner_b = KGStressProfileRunner(
        base_dir=tmp_path / "b", executor=recording_executor
    )
    runner_b.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=10,
        chaos_modes=[m.value for m in ChaosMode],
        seed="same-seed",
    )
    second_run = list(seen_modes)

    assert first_run == second_run


def test_invalid_profile_raises(tmp_path: Path):
    runner = KGStressProfileRunner(base_dir=tmp_path)
    with pytest.raises(StressError) as excinfo:
        runner.run(
            profile="bogus",
            iterations=10,
            chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
            seed="x",
        )
    assert excinfo.value.code is StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE


def test_invalid_chaos_mode_raises(tmp_path: Path):
    runner = KGStressProfileRunner(base_dir=tmp_path)
    with pytest.raises(StressError) as excinfo:
        runner.run(
            profile=StressProfile.LOCAL_SMOKE.value,
            iterations=10,
            chaos_modes=["chaos_singularity"],
            seed="x",
        )
    assert excinfo.value.code is StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE


def test_empty_seed_raises(tmp_path: Path):
    runner = KGStressProfileRunner(base_dir=tmp_path)
    with pytest.raises(StressError):
        runner.run(
            profile=StressProfile.LOCAL_SMOKE.value,
            iterations=10,
            chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
            seed="",
        )


def test_zero_iterations_raises(tmp_path: Path):
    runner = KGStressProfileRunner(base_dir=tmp_path)
    with pytest.raises(StressError):
        runner.run(
            profile=StressProfile.LOCAL_SMOKE.value,
            iterations=0,
            chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
            seed="x",
        )


# --- OR or_bb640ee1 + or_6daf13ec counter shape ------------------------------


def test_stress_corruption_counter_carries_required_labels(tmp_path: Path):
    assert get_stress_corruption_labels() == ("profile", "chaos_mode", "counter")

    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_wal_truncated_every_other
    )
    runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=4,
        chaos_modes=[ChaosMode.SIGKILL_DURING_WRITE.value],
        seed="seed-walt",
    )
    samples = get_stress_corruption_samples()
    keys = {(s["profile"], s["chaos_mode"], s["counter"]) for s in samples}
    assert (
        StressProfile.LOCAL_SMOKE.value,
        ChaosMode.SIGKILL_DURING_WRITE.value,
        "wal_truncated",
    ) in keys


def test_stress_evidence_counter_carries_required_labels(tmp_path: Path):
    assert get_stress_evidence_labels() == ("profile", "outcome")

    runner = KGStressProfileRunner(base_dir=tmp_path, executor=_clean_executor)
    runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=5,
        chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
        seed="ok",
    )
    assert get_stress_evidence_count(
        StressProfile.LOCAL_SMOKE.value, outcome="passed"
    ) == 1

    runner2 = KGStressProfileRunner(
        base_dir=tmp_path, executor=_every_iter_corrupt_executor
    )
    runner2.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=3,
        chaos_modes=[ChaosMode.MEMORY_PRESSURE.value],
        seed="bad",
    )
    assert get_stress_evidence_count(
        StressProfile.LOCAL_SMOKE.value, outcome="failed"
    ) == 1


# --- KGStorageBackendContingency ---------------------------------------------


def test_prepare_happy_path_writes_manifest(tmp_path: Path):
    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    response = service.prepare(
        board_id="b1",
        corruption_timeline_ref="/snapshots/timeline-42",
        quarantine_ids=["q_abc", "q_def"],
        software_version="0.2.3",
    )
    assert response.ready_for_upstream_issue is True
    assert response.ready_for_hot_swap_decision is True

    manifest_path = Path(response.contingency_ref)
    assert manifest_path.exists()
    assert manifest_path.name == CONTINGENCY_MANIFEST_FILENAME
    body = json.loads(manifest_path.read_text())
    assert body["board_id"] == "b1"
    assert body["quarantine_ids"] == ["q_abc", "q_def"]
    assert body["software_version"] == "0.2.3"


def test_prepare_with_stale_version_blocks_hot_swap(tmp_path: Path):
    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    response = service.prepare(
        board_id="b1",
        corruption_timeline_ref="/snapshots/x",
        quarantine_ids=["q1"],
        software_version="0.1.0",  # stale
    )
    assert response.ready_for_upstream_issue is True
    assert response.ready_for_hot_swap_decision is False


def test_prepare_missing_quarantine_raises(tmp_path: Path):
    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    with pytest.raises(ContingencyError) as excinfo:
        service.prepare(
            board_id="b1",
            corruption_timeline_ref="/snapshots/x",
            quarantine_ids=[],
            software_version="0.2.3",
        )
    assert excinfo.value.code is ContingencyErrorCode.MISSING_QUARANTINE_EVIDENCE


def test_prepare_empty_timeline_ref_raises(tmp_path: Path):
    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    with pytest.raises(ContingencyError) as excinfo:
        service.prepare(
            board_id="b1",
            corruption_timeline_ref="",
            quarantine_ids=["q1"],
            software_version="0.2.3",
        )
    assert excinfo.value.code is ContingencyErrorCode.TIMELINE_UNAVAILABLE


def test_prepare_resolver_blocks_when_manifest_missing(tmp_path: Path):
    """quarantine_resolver returns None for unknown IDs → raises
    missing_quarantine_evidence so we never publish stale contingency
    referencing non-existent quarantines."""
    def resolver(qid):
        if qid == "q_existing":
            return {"quarantine_id": qid}
        return None

    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        quarantine_resolver=resolver,
    )
    with pytest.raises(ContingencyError) as excinfo:
        service.prepare(
            board_id="b1",
            corruption_timeline_ref="/x",
            quarantine_ids=["q_existing", "q_missing"],
            software_version="0.2.3",
        )
    assert excinfo.value.code is ContingencyErrorCode.MISSING_QUARANTINE_EVIDENCE


def test_contingency_counter_carries_required_or_labels(tmp_path: Path):
    assert get_contingency_counter_labels() == (
        "board_id", "outcome", "ready_for_upstream_issue", "ready_for_hot_swap_decision",
    )

    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    service.prepare(
        board_id="b1",
        corruption_timeline_ref="/x",
        quarantine_ids=["q1"],
        software_version="0.2.3",
    )
    samples = get_contingency_samples()
    keys = {(s["board_id"], s["outcome"]) for s in samples}
    assert ("b1", "prepared") in keys
    for s in samples:
        for label in get_contingency_counter_labels():
            assert label in s and isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int)


# --- val_209622e9 enforcement regression ------------------------------------


def test_ci_destructive_with_clean_fake_without_release_marker_is_rejected(
    tmp_path: Path,
):
    """val_67950216 regression: even a non-default clean callable that
    lacks the release_evidence_executor marker MUST be rejected for
    ci_destructive. CI release evidence can only come from explicitly
    qualified executors."""
    runner = KGStressProfileRunner(base_dir=tmp_path, executor=_clean_executor)
    with pytest.raises(StressError) as excinfo:
        runner.run(
            profile=StressProfile.CI_DESTRUCTIVE.value,
            iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
            chaos_modes=[m.value for m in ChaosMode],
            seed="seed-fake",
        )
    assert excinfo.value.code is StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE
    assert "release-qualified" in excinfo.value.reason


def test_kg_chaos_executor_has_release_marker():
    """KGChaosExecutor must carry the marker so CI accepts it."""
    from okto_pulse.core.kg.stress_chaos_executor import KGChaosExecutor

    assert getattr(KGChaosExecutor, "release_evidence_executor", False) is True


def test_ci_destructive_with_default_executor_is_rejected(tmp_path: Path):
    """val_209622e9 #1: the runner refuses ci_destructive when the
    default no-op executor is active. CI must wire a real executor."""
    runner = KGStressProfileRunner(base_dir=tmp_path)  # default executor
    with pytest.raises(StressError) as excinfo:
        runner.run(
            profile=StressProfile.CI_DESTRUCTIVE.value,
            iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
            chaos_modes=[m.value for m in ChaosMode],
            seed="seed-x",
        )
    assert excinfo.value.code is StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE
    assert "default no-op executor" in excinfo.value.reason


def test_ci_destructive_missing_canonical_mode_is_rejected(tmp_path: Path):
    """val_209622e9 #2: ci_destructive must exercise every canonical
    chaos mode. Subset is OK for local_smoke triage but not for release."""
    runner = KGStressProfileRunner(
        base_dir=tmp_path, executor=_FakeReleaseQualifiedExecutor()
    )
    with pytest.raises(StressError) as excinfo:
        runner.run(
            profile=StressProfile.CI_DESTRUCTIVE.value,
            iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
            chaos_modes=[
                ChaosMode.MEMORY_PRESSURE.value,
                # missing the other 4
            ],
            seed="seed-subset",
        )
    assert excinfo.value.code is StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE
    assert "missing" in excinfo.value.reason


def test_round_robin_distributes_iterations_deterministically(tmp_path: Path):
    """val_209622e9 #3: chaos_mode selection is round-robin, not random.
    With 10 iterations and 5 modes each mode runs exactly 2 times."""
    runner = KGStressProfileRunner(base_dir=tmp_path, executor=_clean_executor)
    response = runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=10,
        chaos_modes=[m.value for m in ChaosMode],
        seed="anything",
    )
    for mode in ChaosMode:
        assert response.iterations_per_mode[mode.value] == 2


def test_real_chaos_executor_runs_clean_against_primitives(tmp_path: Path):
    """val_209622e9 #2 production-style proof: the real KGChaosExecutor
    runs against the KG-01.1-5 primitives and reports zero corruption
    across all 5 canonical modes."""
    from okto_pulse.core.kg.stress_chaos_executor import KGChaosExecutor

    executor = KGChaosExecutor(base_dir=tmp_path / "chaos-scratch")
    runner = KGStressProfileRunner(
        base_dir=tmp_path / "evidence", executor=executor,
    )
    # local_smoke for the test (real executor's sigkill mode sleeps
    # 1.1s per iter — would be ~20 min for the full 1000-iter CI run).
    response = runner.run(
        profile=StressProfile.LOCAL_SMOKE.value,
        iterations=5,
        chaos_modes=[m.value for m in ChaosMode],
        seed="real-exec",
    )
    assert response.counters.is_release_clean() is True
    for mode in ChaosMode:
        assert response.iterations_per_mode[mode.value] == 1


# --- val_209622e9 #5: contingency default safety ----------------------------


def test_contingency_rejects_construction_without_resolver_or_flag(tmp_path: Path):
    """val_209622e9 #5: no more silent `treat every id as resolvable`
    default. Construction needs an explicit knob."""
    with pytest.raises(ValueError) as excinfo:
        KGStorageBackendContingency(
            base_dir=tmp_path, boot_software_version="0.2.3"
        )
    assert "quarantine_resolver" in str(excinfo.value)


def test_contingency_with_explicit_allow_flag_constructs_for_tests(tmp_path: Path):
    """The flag is explicit so the operator/test acknowledges they are
    bypassing manifest verification on purpose."""
    service = KGStorageBackendContingency(
        base_dir=tmp_path,
        boot_software_version="0.2.3",
        allow_unverified_quarantine_ids=True,
    )
    assert service is not None


# --- TR16: contingency module MUST NOT import any alternate backend ---------


def test_contingency_module_does_not_import_alternate_backend():
    """TR16: Plano B/hot-swap must remain backend-port based, no direct
    imports of alternative backends in this spec."""
    import okto_pulse.core.kg.contingency as module

    # Whitelisted imports — only what KG-01.6 legitimately needs.
    allowed_top_level = {
        "json",
        "logging",
        "secrets",
        "threading",
        "dataclasses",
        "datetime",
        "enum",
        "pathlib",
        "typing",
        "__future__",
    }
    import inspect
    src = inspect.getsource(module)
    # No known competitor backend imports.
    for forbidden in (
        "neo4j",
        "memgraph",
        "tigergraph",
        "kuzu_alternative",
        "arcadedb",
    ):
        assert forbidden not in src.lower()
