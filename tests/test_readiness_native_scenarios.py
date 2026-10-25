"""Unittest entry points for Pulse's authenticated scenario runner.

The local runner admits only unittest methods. These methods execute the
existing pytest suites from the two immutable Git checkouts in a paired
profile; they do not turn a self-reported test result into evidence.
"""

from __future__ import annotations

import os
from pathlib import Path
import unittest

import pytest


_CORE = Path(__file__).resolve().parents[1]


def _run(*selectors: str) -> None:
    result = pytest.main(["-q", "-p", "no:cacheprovider", *selectors])
    if int(result) != 0:
        raise AssertionError(f"pytest returned {int(result)} for {selectors}")


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
