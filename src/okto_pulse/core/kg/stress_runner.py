"""KG destructive stress runner (KG-01 FR8, contract api_bb4ff8ae).

Runs chaos profiles against the KG hardening primitives (lock,
backpressure, quarantine) and persists release-grade evidence so the
spec-validation gate can confirm zero corruption survives the hardening.

The runner is intentionally backend-agnostic: it does NOT call
embedded graph backend directly. Each chaos mode is a deterministic, seeded
simulator that exercises the primitives via a pluggable
``ChaosExecutor`` callable. The default executor uses
``unittest.mock``-style fakes; production CI wires a real executor
that hooks the primitives into graph backend's actual write paths.

TR10: CI pass requires every corruption counter == 0 across all
iterations. The runner refuses to mark ``passed=True`` otherwise.
TR15: persisted evidence carries seed, iterations, chaos_modes,
software_version, counters, and a per-failure artifact list — no
sensitive payload.
TR11: ``local_smoke`` is a fast feedback profile; it does NOT
substitute for the CI destructive profile and the response carries a
``profile`` field so consumers can refuse to gate on smoke runs.
"""

from __future__ import annotations

import logging
import random
import secrets
from okto_pulse.core.runtime_context import runtime_lock, runtime_state
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    REBUILD_AUDIT_GLOBAL_BOARD_ID,
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import resolve_rebuild_audit_artifact_store

logger = logging.getLogger("okto_pulse.kg.stress_runner")


STRESS_DIRNAME = "stress"
EVIDENCE_FILENAME = "evidence.json"
SOFTWARE_VERSION_FALLBACK = "unknown"

# TR10: CI destructive profile floor. Below this we refuse to mark
# the run as CI-grade evidence (releases gating off this number must
# see a real exercise of the hardening, not a 10-iteration smoke).
CI_DESTRUCTIVE_ITERATIONS_FLOOR = 1000


class StressProfile(str, Enum):
    CI_DESTRUCTIVE = "ci_destructive"
    LOCAL_SMOKE = "local_smoke"


class ChaosMode(str, Enum):
    MEMORY_PRESSURE = "memory_pressure"
    SIGKILL_DURING_WRITE = "sigkill_during_write"
    OOM_DURING_WRITE = "oom_during_write"
    RESTART_DURING_CONSOLIDATION = "restart_during_consolidation"
    TICK_CONSOLIDATION_REBUILD_CONCURRENCY = "tick_consolidation_rebuild_concurrency"


CANONICAL_CHAOS_MODES = frozenset(m.value for m in ChaosMode)


class StressErrorCode(str, Enum):
    """Typed errors per contract api_bb4ff8ae response_errors."""

    STRESS_ENVIRONMENT_UNAVAILABLE = "stress_environment_unavailable"
    STRESS_PROFILE_FAILED = "stress_profile_failed"


class StressError(Exception):
    def __init__(
        self,
        code: StressErrorCode,
        *,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.retryable = retryable
        self.reason = reason


@dataclass(frozen=True, slots=True)
class StressCounters:
    """Required counters per contract api_bb4ff8ae response.

    The contract explicitly mandates four — additional informational
    counters live in ``extras`` to keep the response shape predictable
    while preserving diagnostic richness.
    """

    corruption_detected: int
    wal_truncated: int
    orphaned_lock: int
    purge_without_quarantine: int
    extras: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "corruption_detected": self.corruption_detected,
            "wal_truncated": self.wal_truncated,
            "orphaned_lock": self.orphaned_lock,
            "purge_without_quarantine": self.purge_without_quarantine,
            **{f"extra_{k}": v for k, v in self.extras.items()},
        }

    def is_release_clean(self) -> bool:
        """TR10 / OR or_bb640ee1: release pass iff every required counter is 0."""
        return (
            self.corruption_detected == 0
            and self.wal_truncated == 0
            and self.orphaned_lock == 0
            and self.purge_without_quarantine == 0
        )


@dataclass(frozen=True, slots=True)
class StressResponse:
    """Frozen contract-shaped response per api_bb4ff8ae success body."""

    passed: bool
    iterations_completed: int
    counters: StressCounters
    evidence_ref: str
    # Additive: which profile + seed were used, for traceability.
    profile: str = ""
    seed: str = ""
    # val_209622e9: per-mode iteration count so the operator can audit
    # that CI actually exercised every canonical chaos mode (not just a
    # random subset). Required ≥1 per canonical mode for CI_DESTRUCTIVE.
    iterations_per_mode: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ChaosOutcome:
    """One iteration's outcome, returned by the executor."""

    corruption_detected: bool = False
    wal_truncated: bool = False
    orphaned_lock: bool = False
    purge_without_quarantine: bool = False
    extras: dict[str, int] = field(default_factory=dict)
    failure_artifact: str | None = None


# A chaos executor is a callable(seed, iteration_index, chaos_mode) → ChaosOutcome.
# Default executor: deterministic and always-clean (proves the runner's
# bookkeeping but never reports corruption). CI wires a real one.
ChaosExecutor = Callable[[str, int, str], ChaosOutcome]


def _default_executor(seed: str, iteration: int, chaos_mode: str) -> ChaosOutcome:
    """Deterministic no-op chaos — exercises the runner without storage.

    val_209622e9 enforcement: the runner refuses ``profile=ci_destructive``
    when this default is in use. CI must wire a real executor.
    """
    return ChaosOutcome()


# Identity-comparable sentinel: the runner uses ``is`` to detect "the
# caller didn't supply a real executor" and blocks CI runs accordingly.
_default_executor._is_default_chaos_executor = True  # type: ignore[attr-defined]


# --- Counters (OR or_bb640ee1 + or_6daf13ec) ---------------------------------

_STRESS_CORRUPTION_LABELS = ("profile", "chaos_mode", "counter")
_STRESS_EVIDENCE_LABELS = ("profile", "outcome")

_corruption_counter = runtime_state("kg.stress_runner.corruption_counter", dict)
_evidence_counter = runtime_state("kg.stress_runner.evidence_counter", dict)
_stress_counter_lock = runtime_lock("kg.stress_runner.counters")


def _bump_corruption(profile: str, chaos_mode: str, counter: str) -> None:
    key = (profile, chaos_mode, counter)
    with _stress_counter_lock:
        _corruption_counter[key] = _corruption_counter.get(key, 0) + 1


def _bump_evidence(profile: str, outcome: str) -> None:
    key = (profile, outcome)
    with _stress_counter_lock:
        _evidence_counter[key] = _evidence_counter.get(key, 0) + 1


def get_stress_corruption_count(
    profile: str,
    *,
    chaos_mode: str | None = None,
    counter: str | None = None,
) -> int:
    with _stress_counter_lock:
        total = 0
        for (p, cm, c), value in _corruption_counter.items():
            if p != profile:
                continue
            if chaos_mode is not None and cm != chaos_mode:
                continue
            if counter is not None and c != counter:
                continue
            total += value
        return total


def get_stress_evidence_count(profile: str, *, outcome: str | None = None) -> int:
    with _stress_counter_lock:
        total = 0
        for (p, out), value in _evidence_counter.items():
            if p != profile:
                continue
            if outcome is not None and out != outcome:
                continue
            total += value
        return total


def get_stress_corruption_samples() -> list[dict[str, Any]]:
    with _stress_counter_lock:
        return [
            {"profile": p, "chaos_mode": cm, "counter": c, "count": value}
            for (p, cm, c), value in _corruption_counter.items()
        ]


def get_stress_evidence_samples() -> list[dict[str, Any]]:
    with _stress_counter_lock:
        return [
            {"profile": p, "outcome": out, "count": value}
            for (p, out), value in _evidence_counter.items()
        ]


def reset_stress_counters() -> None:
    with _stress_counter_lock:
        _corruption_counter.clear()
        _evidence_counter.clear()


def get_stress_corruption_labels() -> tuple[str, ...]:
    return _STRESS_CORRUPTION_LABELS


def get_stress_evidence_labels() -> tuple[str, ...]:
    return _STRESS_EVIDENCE_LABELS


# --- Runner ------------------------------------------------------------------


def _read_software_version() -> str:
    try:
        from importlib.metadata import version

        return version("okto-pulse-core")
    except Exception:
        return SOFTWARE_VERSION_FALLBACK


@dataclass(frozen=True, slots=True)
class KGStressProfileRunner:
    """Runs a chaos profile and persists evidence.

    The runner is pure orchestration: it dispatches iterations to the
    supplied ``executor`` (a callable plugged by tests or by the CI
    runner that wires the primitives into the embedded graph backend write paths),
    aggregates outcomes into counters, and writes a single
    ``evidence.json`` per run. No graph storage is touched directly.
    """

    base_dir: object | None
    executor: ChaosExecutor = _default_executor
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )

    def run(
        self,
        *,
        profile: str,
        iterations: int,
        chaos_modes: list[str],
        seed: str,
    ) -> StressResponse:
        """Run the chaos profile and return the contract response.

        Raises ``StressError(STRESS_ENVIRONMENT_UNAVAILABLE)`` if the
        evidence directory cannot be created or the seed is empty;
        ``STRESS_PROFILE_FAILED`` is reserved for callers that need to
        surface a non-corruption infrastructure issue.
        """
        if not seed:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason="seed must be non-empty",
            )
        if iterations < 1:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason=f"iterations must be >= 1 (got {iterations})",
            )
        if profile not in {p.value for p in StressProfile}:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason=f"unknown profile: {profile}",
            )
        invalid = [m for m in chaos_modes if m not in CANONICAL_CHAOS_MODES]
        if invalid:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason=f"unknown chaos_modes: {sorted(invalid)}",
            )
        if not chaos_modes:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason="chaos_modes must be non-empty",
            )

        # val_209622e9 enforcement #1: CI_DESTRUCTIVE refuses the
        # no-op default executor — CI must wire a real executor that
        # exercises the KG-01.1-5 primitives (lock, backpressure,
        # quarantine, safe_lifecycle). Without this guard, a clean
        # 1000-iteration run with the default would be release-clean
        # by accident, defeating TR10/AC12.
        if profile == StressProfile.CI_DESTRUCTIVE.value and getattr(
            self.executor, "_is_default_chaos_executor", False
        ):
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason=(
                    "ci_destructive profile requires a real chaos executor; "
                    "the default no-op executor cannot produce release evidence"
                ),
            )

        # val_67950216 enforcement: CI_DESTRUCTIVE additionally requires
        # the executor to carry the explicit ``release_evidence_executor``
        # marker. The default no-op block above only covers ONE specific
        # callable; without this marker a generic fake-clean callable
        # could still produce release evidence by accident. The marker
        # is an opt-in attestation from the executor author: "I really
        # do exercise the KG-01.1-5 primitives in ways that can detect
        # corruption". KGChaosExecutor sets release_evidence_executor=True
        # on the class. Tests that need a fake CI-grade executor must
        # set the same marker explicitly and earn the right to gate
        # release on their callable.
        if profile == StressProfile.CI_DESTRUCTIVE.value and not getattr(
            self.executor, "release_evidence_executor", False
        ):
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=False,
                reason=(
                    "ci_destructive profile requires a release-qualified "
                    "executor (must set release_evidence_executor=True). "
                    "Use KGChaosExecutor in CI; fake callables can NOT "
                    "produce release evidence"
                ),
            )

        # val_209622e9 enforcement #2: CI_DESTRUCTIVE requires every
        # canonical chaos mode to be in the request. A subset is fine
        # for local_smoke triage but not for release evidence.
        if profile == StressProfile.CI_DESTRUCTIVE.value:
            missing = CANONICAL_CHAOS_MODES - set(chaos_modes)
            if missing:
                raise StressError(
                    StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                    retryable=False,
                    reason=(
                        "ci_destructive profile must exercise every canonical "
                        f"chaos mode; missing: {sorted(missing)}"
                    ),
                )

        evidence_id = f"stress_{secrets.token_urlsafe(12)}"
        evidence_key = RebuildAuditKey(
            namespace="stress_evidence",
            board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
            artifact_id=evidence_id,
        )
        if self.artifact_store.exists(evidence_key):
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=True,
                reason=f"evidence id collision: {evidence_id}",
            )

        corruption = 0
        wal_truncated = 0
        orphaned = 0
        purge_without = 0
        extras: dict[str, int] = {}
        failure_artifacts: list[str] = []
        iterations_completed = 0
        iterations_per_mode: dict[str, int] = {m: 0 for m in chaos_modes}

        # val_209622e9 enforcement #3: deterministic round-robin
        # selection. Release evidence cannot depend on RNG — every
        # canonical mode in `chaos_modes` is exercised in turn. For
        # LOCAL_SMOKE we keep the rng seed only for shuffle-stable
        # extras (e.g. picking a sub-scenario inside a mode).
        chaos_modes_list = list(chaos_modes)
        # Stable order regardless of caller's dict ordering.
        chaos_modes_list.sort()
        rng = random.Random(seed)  # noqa: F841 — kept for executor sub-decisions

        try:
            for i in range(iterations):
                mode = chaos_modes_list[i % len(chaos_modes_list)]
                iterations_per_mode[mode] = iterations_per_mode.get(mode, 0) + 1
                try:
                    outcome = self.executor(seed, i, mode)
                except Exception as exc:
                    failure_artifacts.append(
                        f"iter={i} mode={mode} exec_exception={type(exc).__name__}"
                    )
                    corruption += 1
                    _bump_corruption(profile, mode, "corruption_detected")
                    iterations_completed += 1
                    continue

                if outcome.corruption_detected:
                    corruption += 1
                    _bump_corruption(profile, mode, "corruption_detected")
                if outcome.wal_truncated:
                    wal_truncated += 1
                    _bump_corruption(profile, mode, "wal_truncated")
                if outcome.orphaned_lock:
                    orphaned += 1
                    _bump_corruption(profile, mode, "orphaned_lock")
                if outcome.purge_without_quarantine:
                    purge_without += 1
                    _bump_corruption(profile, mode, "purge_without_quarantine")
                for k, v in outcome.extras.items():
                    extras[k] = extras.get(k, 0) + v
                if outcome.failure_artifact:
                    failure_artifacts.append(
                        f"iter={i} mode={mode} {outcome.failure_artifact}"
                    )
                iterations_completed += 1
        except Exception as exc:
            # An exception escaped the executor wrapper — surface as
            # a typed profile failure so the operator can investigate.
            raise StressError(
                StressErrorCode.STRESS_PROFILE_FAILED,
                retryable=False,
                reason=f"orchestrator failure at iter={iterations_completed}: {exc}",
            ) from exc

        counters = StressCounters(
            corruption_detected=corruption,
            wal_truncated=wal_truncated,
            orphaned_lock=orphaned,
            purge_without_quarantine=purge_without,
            extras=extras,
        )
        below_ci_floor = (
            profile == StressProfile.CI_DESTRUCTIVE.value
            and iterations_completed < CI_DESTRUCTIVE_ITERATIONS_FLOOR
        )
        # val_209622e9 enforcement #4: CI_DESTRUCTIVE pass requires
        # every canonical mode to have been exercised at least once.
        # With deterministic round-robin this only fails when
        # iterations < len(canonical_modes) AND profile=CI — caught
        # above by the floor check, but we keep an independent
        # invariant here for defense in depth.
        per_mode_complete = True
        if profile == StressProfile.CI_DESTRUCTIVE.value:
            per_mode_complete = all(
                iterations_per_mode.get(m, 0) >= 1 for m in CANONICAL_CHAOS_MODES
            )
        passed = (
            counters.is_release_clean() and not below_ci_floor and per_mode_complete
        )
        outcome_label = "passed" if passed else "failed"

        evidence_payload = {
            "evidence_id": evidence_id,
            "profile": profile,
            "seed": seed,
            "iterations_requested": iterations,
            "iterations_completed": iterations_completed,
            "iterations_per_mode": dict(iterations_per_mode),
            "chaos_modes": sorted(set(chaos_modes_list)),
            "software_version": _read_software_version(),
            "counters": counters.to_dict(),
            "passed": passed,
            "below_ci_floor": below_ci_floor,
            "per_mode_complete": per_mode_complete,
            "failure_artifacts": failure_artifacts,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.artifact_store.write_json_atomic(evidence_key, evidence_payload)
        except Exception as exc:
            raise StressError(
                StressErrorCode.STRESS_ENVIRONMENT_UNAVAILABLE,
                retryable=True,
                reason=f"evidence write failed: {exc}",
            ) from exc

        _bump_evidence(profile, outcome_label)
        logger.warning(
            "kg.stress.run_completed profile=%s iterations=%d/%d passed=%s "
            "evidence=%s corruption=%d wal_trunc=%d orphaned=%d purge_without=%d",
            profile,
            iterations_completed,
            iterations,
            passed,
            self.artifact_store.reference(evidence_key),
            corruption,
            wal_truncated,
            orphaned,
            purge_without,
            extra={
                "event": "kg.stress.run_completed",
                "profile": profile,
                "iterations_completed": iterations_completed,
                "passed": passed,
                "evidence_ref": self.artifact_store.reference(evidence_key),
            },
        )
        return StressResponse(
            passed=passed,
            iterations_completed=iterations_completed,
            counters=counters,
            evidence_ref=self.artifact_store.reference(evidence_key),
            profile=profile,
            seed=seed,
            iterations_per_mode=dict(iterations_per_mode),
        )


__all__ = [
    "CANONICAL_CHAOS_MODES",
    "CI_DESTRUCTIVE_ITERATIONS_FLOOR",
    "ChaosExecutor",
    "ChaosMode",
    "ChaosOutcome",
    "EVIDENCE_FILENAME",
    "KGStressProfileRunner",
    "STRESS_DIRNAME",
    "StressCounters",
    "StressError",
    "StressErrorCode",
    "StressProfile",
    "StressResponse",
    "get_stress_corruption_count",
    "get_stress_corruption_labels",
    "get_stress_corruption_samples",
    "get_stress_evidence_count",
    "get_stress_evidence_labels",
    "get_stress_evidence_samples",
    "reset_stress_counters",
]
