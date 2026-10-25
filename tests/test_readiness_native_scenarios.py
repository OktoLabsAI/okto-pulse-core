"""Unittest entry points for Pulse's authenticated scenario runner.

The local runner admits only unittest methods. These methods execute the
existing pytest suites from the two immutable Git checkouts in a paired
profile; they do not turn a self-reported test result into evidence.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import unittest


_CORE = Path(__file__).resolve().parents[1]


def _run(*selectors: str) -> None:
    # Core and Community use different pytest composition fixtures. A fresh
    # interpreter for each suite prevents their global registries leaking
    # across scenario methods in the runner's long-lived unittest process.
    workspace = Path(selectors[0].split("::", 1)[0]).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *selectors],
        cwd=workspace, capture_output=True, text=True, timeout=180, check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"pytest returned {result.returncode} for {selectors}\n"
            f"{result.stdout[-6000:]}\n{result.stderr[-2000:]}"
        )
    print(result.stdout.strip())


class PersistedReadinessNativeScenarios(unittest.TestCase):
    def test_sc01_canonical_parity(self) -> None:
        suite = _CORE / "tests" / "test_persisted_delivery_readiness.py"
        _run(
            f"{suite}::test_generation_matches_canonical_spec_and_each_card",
            f"{suite}::test_generation_units_and_percentages_follow_canonical_cases",
            str(_CORE / "tests" / "test_delivery_readiness_metrics.py"),
        )

    def test_sc02_persisted_generation_integrity(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        tests = Path(community) / "tests"
        _run(
            str(tests / "test_readiness_invalidation.py"),
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_worker_publishes_one_complete_generation_after_source_commit",
        )

    def test_sc03_phase_and_history_semantics(self) -> None:
        suite = _CORE / "tests" / "test_persisted_delivery_readiness.py"
        _run(
            f"{suite}::test_generation_preserves_independent_local_progress_and_unknowns",
            f"{_CORE / 'tests' / 'test_delivery_readiness_metrics.py'}::"
            "test_M10_historical_waiver_is_risk_not_na_and_is_not_rewritten",
            f"{_CORE / 'tests' / 'test_delivery_readiness_metrics.py'}::"
            "test_M20_test_card_counts_assigned_scenarios_with_structural_na_implementation",
            f"{_CORE / 'tests' / 'test_delivery_readiness_metrics.py'}::"
            "test_phase_unknown_does_not_erase_known_implementation",
        )

    def test_sc07_closed_snapshot_and_coalescing(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        suite = Path(community) / "tests" / "test_readiness_projection_worker.py"
        _run(
            f"{suite}::test_projection_cpu_runs_after_snapshot_checkout_is_released",
            f"{suite}::test_many_source_revisions_coalesce_into_one_generation",
        )

    def test_sc08_external_churn_and_population_limit(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        suite = Path(community) / "tests" / "test_readiness_projection_worker.py"
        _run(
            f"{suite}::test_unrelated_board_commit_does_not_retry_projection_snapshot",
            f"{suite}::test_over_cap_scope_is_unknown_while_healthy_scope_progresses",
        )

    def test_sc09_deadline_and_lost_lease(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        tests = Path(community) / "tests"
        _run(f"{_CORE / 'tests' / 'test_persisted_delivery_readiness.py'}::"
            "test_generation_checks_deadline_before_and_during_evaluation")
        _run(
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_worker_publishes_one_complete_generation_after_source_commit",
            f"{tests / 'test_worker_runners.py'}::"
            "test_tracked_blocking_failure_is_drained_before_parent_cancel_returns",
        )

    def test_sc10_cas_and_monotonic_publication(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        suite = Path(community) / "tests" / "test_readiness_projection_worker.py"
        _run(
            f"{suite}::test_worker_publishes_one_complete_generation_after_source_commit",
            f"{suite}::test_older_snapshot_publishes_as_updating_without_losing_newer_debt",
        )

    def test_sc11_pinned_generation_cursor(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        suite = Path(community) / "tests" / "test_readiness_projection_access.py"
        _run(
            f"{suite}::test_projection_pages_are_actor_bound_and_reauthorized",
            f"{suite}::test_projection_cursor_keeps_original_generation_until_retention",
        )

    def test_sc12_staging_retention_and_evaluator_version(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        tests = Path(community) / "tests"
        _run(
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_invalid_staging_manifest_never_becomes_active",
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_incompatible_evaluator_generation_cannot_publish",
            f"{tests / 'test_readiness_gc.py'}::"
            "test_gc_keeps_active_and_three_recent_generations_in_bounded_batches",
        )

    def test_sc13_crash_replay_and_staging_visibility(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        suite = Path(community) / "tests" / "test_readiness_projection_worker.py"
        _run(
            f"{suite}::test_crash_replay_keeps_source_and_hides_unpublished_staging",
            f"{suite}::test_worker_publishes_one_complete_generation_after_source_commit",
        )

    def test_sc14_retry_fairness_and_governed_requeue(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        tests = Path(community) / "tests"
        _run(
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_claim_round_robin_survives_restart_and_failing_scope",
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_eight_failures_terminal_then_fenced_admin_requeue",
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_reconcile_cursor_resumes_bounded_pages_after_restart",
            f"{tests / 'test_readiness_projection_access.py'}::"
            "test_projection_pages_are_actor_bound_and_reauthorized",
            str(tests / "test_readiness_requeue_route.py"),
        )

    def test_sc15_storage_failure_and_reconciliation(self) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        if not community:
            raise AssertionError("paired Community checkout is required")
        tests = Path(community) / "tests"
        _run(
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_stage_storage_failure_keeps_previous_generation_until_repair",
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_reconcile_audits_each_active_generation_in_a_page",
            f"{tests / 'test_readiness_projection_worker.py'}::"
            "test_sustained_queue_lag_is_warned_without_exposing_payload",
        )
