"""Authenticated Pulse runner entry points for the Started migration scenarios.

Each unittest method invokes the committed pytest cases for one Spec scenario.
The native runner extracts this tracked revision before calling the method.
"""

from __future__ import annotations

import inspect
import os
import subprocess
import sys
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

    def _run_companion_cases(self, *selectors: str) -> None:
        community = os.environ.get("OKTO_PULSE_COMMUNITY_REPO")
        self.assertIsNotNone(community, "Scenario requires an immutable paired Community checkout")
        companion_root = Path(community).resolve()
        self.assertTrue(companion_root.is_dir())
        result = subprocess.run(
            [sys.executable, "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider", *selectors],
            cwd=companion_root, env=os.environ.copy(), capture_output=True, text=True,
            timeout=90, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-4000:] + result.stderr[-4000:])

    def test_sc01_create_only_not_started(self) -> None:
        self._run_cases(
            "tests/test_card_initial_status_contract.py::test_new_card_status_is_not_started_for_each_type",
            "tests/test_card_lifecycle.py::TestCardCreation::test_plan_cards_in_draft_without_starting_execution",
            "tests/test_card_lifecycle.py::TestCardCreation::test_create_normal_card_with_all_fields",
            "tests/test_card_lifecycle.py::TestCardCreation::test_create_card_with_test_scenario_ids",
            "tests/test_card_lifecycle.py::TestBugCardCreation::test_create_bug_card_with_required_fields",
        )
        self._run_companion_cases(
            "tests/test_test_card_creation_surfaces.py::test_public_create_enters_planning_once",
        )

    def test_sc02_reject_non_planning_status(self) -> None:
        self._run_cases(
            "tests/test_card_initial_status_contract.py::test_new_card_cannot_skip_planning",
            "tests/test_mcp_card_type_validation.py::test_create_card_rejects_non_planning_status_without_writes",
            "tests/test_card_native_wire_contract.py::test_current_card_wire_status_excludes_historical_started",
            "tests/test_card_move_oneof_schema.py::test_historical_started_is_not_an_active_move_target",
            "tests/test_r01a_mcp_card_uow.py::test_move_tool_rejects_started_without_mutating_card",
            "tests/test_card_lifecycle.py::TestCardStatusTransitionMatrix::test_internal_move_cannot_reintroduce_started",
        )

    def test_sc03_progress_and_historical_report(self) -> None:
        self._run_cases(
            "tests/test_delivery_progress.py::test_progress_cannot_bypass_execution_lifecycle",
            "tests/test_delivery_progress.py::test_archived_execution_card_cannot_record_new_progress",
            "tests/test_delivery_report_contract.py::test_legacy_started_report_is_parseable_only_for_exact_replay",
            "tests/test_delivery_report_contract.py::test_active_wire_schema_does_not_advertise_started_report",
        )
        self._run_companion_cases(
            "tests/test_delivery_report.py::test_pre_upgrade_started_report_only_replays_identical_receipt",
        )

    def test_sc04_transition_gates_by_type(self) -> None:
        self._run_cases(
            "tests/test_card_lifecycle.py::TestCardStatusTransitionMatrix",
            "tests/test_af23_allowed_transitions.py::test_card_read_model_and_mutation_admission_share_every_typed_edge",
            "tests/test_skm_create_started_precedence.py::test_test_card_validation_rework_still_enforces_spec_precedence",
        )

    def test_sc05_migrated_cards_do_not_gain_qualification(self) -> None:
        self._run_cases(
            "tests/test_allowed_transitions_mutation_parity_regressions.py::test_migrated_in_progress_task_rechecks_link_before_validation_but_can_pause",
            "tests/test_allowed_transitions_mutation_parity_regressions.py::test_bug_execution_preview_blocks_missing_linked_test_task_id",
            "tests/test_skm_create_started_precedence.py::test_migrated_test_validation_rechecks_prerequisite_and_edition_fence",
        )

    def test_sc06_qualification_fences(self) -> None:
        self._run_cases(
            "tests/test_skm_create_started_precedence.py::test_migrated_test_validation_rechecks_prerequisite_and_edition_fence",
            "tests/test_skm_create_started_precedence.py::test_test_card_validation_rework_fences_concurrent_spec_edition_change",
            "tests/test_spec_dependencies_core.py::test_lifecycle_and_card_writers_take_dependency_fence_at_safe_boundary",
        )

    def test_sc07_permission_truth_table(self) -> None:
        self._run_cases(
            "tests/test_card_start_authority_routes.py::test_start_requires_an_entire_authorized_route",
            "tests/test_card_start_authority_routes.py::test_start_respects_state_denial_on_legacy_route",
        )
        self._run_companion_cases(
            "tests/test_card_status_permission_reconciliation.py::test_custom_same_name_direct_grants_and_board_ceilings_are_converted",
            "tests/test_card_status_migration_return.py::test_migrated_return_expires_after_governed_reentry",
        )

    def test_sc08_presets_and_reconciliation(self) -> None:
        self._run_cases(
            "tests/test_sdlc_transition_permissions.py::test_transition_grants_are_fail_closed_and_explicit_per_preset",
            "tests/test_card_start_authority_routes.py::test_migrated_test_bug_retains_only_its_former_return_right",
        )
        self._run_companion_cases(
            "tests/test_card_status_permission_reconciliation.py::test_custom_same_name_direct_grants_and_board_ceilings_are_converted",
            "tests/test_card_status_permission_reconciliation.py::test_concurrent_permission_startups_do_not_duplicate_conversion",
            "tests/test_card_status_permission_reconciliation.py::test_permission_conversion_failure_rolls_back_before_readiness",
        )

    def test_sc09_return_and_migration_provenance(self) -> None:
        self._run_cases(
            "tests/test_card_start_authority_routes.py::test_return_to_planning_needs_both_former_edges",
            "tests/test_card_start_authority_routes.py::test_return_to_planning_requires_materialized_new_capability",
            "tests/test_card_start_authority_routes.py::test_migrated_normal_retains_its_former_started_exit",
            "tests/test_card_start_authority_routes.py::test_migrated_test_bug_retains_only_its_former_return_right",
        )
        self._run_companion_cases(
            "tests/test_card_status_migration_return.py::test_migrated_return_expires_after_governed_reentry",
            "tests/test_card_status_migration_return.py::test_migrated_card_can_return_once_before_new_execution",
        )
