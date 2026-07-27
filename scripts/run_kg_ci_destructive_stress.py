"""Standalone entrypoint for the KG-01.6 destructive release stress run.

Usage::

    python scripts/run_kg_ci_destructive_stress.py [--evidence-dir DIR] [--seed STR]

Mirrors the pytest stress test (`tests/stress/test_kg_ci_destructive_release.py`)
but as a script the operator can run on demand or wire into a
`.github/workflows` job. Exits with code 0 only when every release
invariant holds — exit code 1 otherwise so CI can fail the build.

Invariants enforced (per KG-01 spec TR10 / AC12 / IR ir_b3116b9c /
ir_77b71dee):

* `passed=True`
* `iterations_completed == CI_DESTRUCTIVE_ITERATIONS_FLOOR` (1000)
* `counters.is_release_clean()` — all four required counters at 0
* `iterations_per_mode[mode] >= 1` for every canonical ChaosMode
* `per_mode_complete=True` in the persisted evidence file
* `evidence_ref` file persisted on disk
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the KG-01.6 release stress profile and emit evidence."
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=None,
        help="Directory where the stress runner persists evidence JSON.",
    )
    parser.add_argument(
        "--chaos-dir",
        type=Path,
        default=None,
        help="Scratch directory for KGChaosExecutor mode handlers.",
    )
    parser.add_argument(
        "--seed",
        type=str,
        default="kg-01.6-release-evidence",
        help="Deterministic seed for the stress run.",
    )
    args = parser.parse_args(argv)

    try:
        from okto_pulse.community.adapters.coordination import (
            CommunityLocalWriteLockPort,
        )
        from okto_pulse.community.adapters.rebuild_audit_storage import (
            CommunityFileSystemRebuildAuditArtifactStore,
        )
    except ModuleNotFoundError as exc:
        print(
            "FATAL: KG destructive stress requires edition-provided local "
            f"adapters; Community package is unavailable ({exc}).",
            file=sys.stderr,
        )
        return 1
    from okto_pulse.core.kg.stress_chaos_executor import KGChaosExecutor
    from okto_pulse.core.kg.stress_runner import (
        CI_DESTRUCTIVE_ITERATIONS_FLOOR,
        ChaosMode,
        KGStressProfileRunner,
        StressProfile,
    )

    tmp_root: Path
    if args.evidence_dir is None or args.chaos_dir is None:
        tmp_root = Path(tempfile.mkdtemp(prefix="kg-stress-"))
    evidence_dir = args.evidence_dir or (tmp_root / "evidence")
    chaos_dir = args.chaos_dir or (tmp_root / "chaos-scratch")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    chaos_dir.mkdir(parents=True, exist_ok=True)

    executor = KGChaosExecutor(
        base_dir=chaos_dir,
        write_lock_port=CommunityLocalWriteLockPort(),
        artifact_store=CommunityFileSystemRebuildAuditArtifactStore(
            chaos_dir / "artifacts"
        ),
    )
    if not getattr(executor, "release_evidence_executor", False):
        print(
            "FATAL: KGChaosExecutor is missing the release_evidence_executor "
            "marker — refusing to publish release evidence.",
            file=sys.stderr,
        )
        return 1

    runner = KGStressProfileRunner(base_dir=evidence_dir, executor=executor)
    print(
        f"Running KG-01.6 ci_destructive: iterations="
        f"{CI_DESTRUCTIVE_ITERATIONS_FLOOR}, chaos_modes="
        f"{[m.value for m in ChaosMode]}, seed={args.seed!r}"
    )
    response = runner.run(
        profile=StressProfile.CI_DESTRUCTIVE.value,
        iterations=CI_DESTRUCTIVE_ITERATIONS_FLOOR,
        chaos_modes=[m.value for m in ChaosMode],
        seed=args.seed,
    )

    print(f"evidence_ref: {response.evidence_ref}")
    print(f"iterations_completed: {response.iterations_completed}")
    print(f"counters: {response.counters.to_dict()}")
    print(f"iterations_per_mode: {response.iterations_per_mode}")
    print(f"passed: {response.passed}")

    failures: list[str] = []
    if not response.passed:
        failures.append("passed=False")
    if response.iterations_completed != CI_DESTRUCTIVE_ITERATIONS_FLOOR:
        failures.append(
            f"iterations_completed={response.iterations_completed} "
            f"!= floor={CI_DESTRUCTIVE_ITERATIONS_FLOOR}"
        )
    if not response.counters.is_release_clean():
        failures.append("counters not release-clean")
    for mode in ChaosMode:
        if response.iterations_per_mode.get(mode.value, 0) < 1:
            failures.append(f"mode {mode.value} not exercised")

    evidence_path = Path(response.evidence_ref)
    if not evidence_path.exists():
        failures.append(f"evidence_ref missing on disk: {evidence_path}")
    else:
        body = json.loads(evidence_path.read_text(encoding="utf-8"))
        if not body.get("per_mode_complete"):
            failures.append("evidence per_mode_complete is not True")
        if body.get("below_ci_floor"):
            failures.append("evidence below_ci_floor is True")

    if failures:
        print(
            "FAIL: KG-01.6 release stress invariants violated:\n  - "
            + "\n  - ".join(failures),
            file=sys.stderr,
        )
        return 1

    print("OK: KG-01.6 release stress invariants satisfied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
