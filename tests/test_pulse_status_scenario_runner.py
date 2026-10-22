"""Authenticated Pulse runner entry points for the Started migration scenarios.

Each unittest method invokes the committed pytest cases for one Spec scenario.
The native runner extracts this tracked revision before calling the method.
"""

from __future__ import annotations

import inspect
import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PulseStatusScenarioTests(unittest.TestCase):
    def _run_cases(self, *selectors: str) -> None:
        import pytest

        previous_core = os.environ.get("OKTO_PULSE_CORE_REPO")
        os.environ["OKTO_PULSE_CORE_REPO"] = str(ROOT)
        try:
            result = pytest.main(["-q", "-p", "no:cacheprovider", *selectors])
            from okto_pulse.core.models.schemas import CardCreate

            self.assertTrue(Path(inspect.getfile(CardCreate)).resolve().is_relative_to(ROOT))
        finally:
            if previous_core is None:
                os.environ.pop("OKTO_PULSE_CORE_REPO", None)
            else:
                os.environ["OKTO_PULSE_CORE_REPO"] = previous_core
        self.assertEqual(int(result), 0, "One or more committed scenario checks failed")

    def test_sc01_create_only_not_started(self) -> None:
        self._run_cases(
            "tests/test_card_initial_status_contract.py::test_new_card_status_is_not_started_for_each_type",
            "tests/test_card_lifecycle.py::TestCardCreation::test_plan_cards_in_draft_without_starting_execution",
            "tests/test_card_lifecycle.py::TestCardCreation::test_create_normal_card_with_all_fields",
            "tests/test_card_lifecycle.py::TestCardCreation::test_create_card_with_test_scenario_ids",
        )

    def test_sc02_reject_non_planning_status(self) -> None:
        self._run_cases(
            "tests/test_card_initial_status_contract.py::test_new_card_cannot_skip_planning",
            "tests/test_mcp_card_type_validation.py::test_create_card_rejects_non_planning_status_without_writes",
            "tests/test_card_native_wire_contract.py::test_current_card_wire_status_excludes_historical_started",
        )

    def test_sc03_progress_and_historical_report(self) -> None:
        self._run_cases(
            "tests/test_delivery_progress.py::test_progress_cannot_bypass_execution_lifecycle",
            "tests/test_delivery_progress.py::test_archived_execution_card_cannot_record_new_progress",
            "tests/test_delivery_report_contract.py::test_legacy_started_report_is_parseable_only_for_exact_replay",
        )
