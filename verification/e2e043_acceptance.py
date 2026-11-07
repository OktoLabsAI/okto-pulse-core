"""Explicit unittest entry points for the installation's paired-source runner.

These methods execute committed regression tests in fresh child interpreters.
They never import an existing CI report or access a running Pulse instance.
The runner authenticates the two source revisions and this exact selector.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET


class _AcceptanceCase(unittest.TestCase):
    def _run(self, selections: dict[str, tuple[str, ...]], *, concurrent_groups: bool = False) -> None:
        # Aggregate runner budget for several sequential native graph fixtures;
        # individual tests keep their own stricter timeout markers and all
        # application lease/SQL/graph deadlines remain unchanged.
        deadline = time.monotonic() + 600
        roots = {
            role: Path(os.environ[f"OKTO_PULSE_{role.upper()}_REPO"]).resolve(strict=True)
            for role in ("core", "community")
        }
        self.assertEqual(roots["core"], Path(__file__).resolve().parents[1])
        self.assertNotEqual(roots["core"], roots["community"])
        if concurrent_groups:
            self._run_concurrent_groups(selections, roots, deadline)
            return
        for role, selectors in selections.items():
            with self.subTest(edition=role), tempfile.TemporaryDirectory(prefix="pulse-e2e043-test-") as tmp:
                output = Path(tmp)
                report = output / "results.xml"
                env = os.environ.copy()
                env.update(
                    DATA_DIR=str(output / "data"),
                    KG_BASE_DIR=str(output / "graphs"),
                    DATABASE_URL=f"sqlite+aiosqlite:///{output / 'isolated.db'}",
                    KG_EMBEDDING_MODE="stub",
                    PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
                )
                # Fixed, reviewed selectors; no caller-provided executable,
                # test expression, source path, report or acceptance override.
                process = subprocess.run(
                    [sys.executable, "-m", "pytest", "-q", "-p", "pytest_asyncio.plugin",
                     "-p", "pytest_timeout", "--timeout=180",
                     *selectors, f"--junitxml={report}", "-o", "addopts="],
                    cwd=roots[role], env=env, capture_output=True, text=True,
                    timeout=max(0.1, deadline - time.monotonic()),
                )
                self.assertEqual(process.returncode, 0, (process.stdout + process.stderr)[-12000:])
                suites = ET.parse(report).getroot().findall(".//testsuite")
                self.assertTrue(suites, "No native test results were produced")
                self.assertGreater(sum(int(s.attrib["tests"]) for s in suites), 0)
                for suite in suites:
                    for counter in ("failures", "errors", "skipped"):
                        self.assertEqual(int(suite.attrib[counter]), 0, f"Incomplete regression: {counter}")

    def _run_concurrent_groups(self, selections, roots, deadline) -> None:
        def text(value):
            return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else (value or "")

        def diagnostic(role, phase, stdout="", stderr=""):
            return (
                f"edition={role} phase={phase} aggregate_budget_seconds=600\n"
                f"cwd={roots[role]} selectors={selections[role]!r}\n"
                f"--- stdout ---\n{text(stdout)}\n--- stderr ---\n{text(stderr)}"
            )

        def run_group(role, selectors):
            with tempfile.TemporaryDirectory(prefix=f"pulse-e2e043-{role}-") as tmp:
                output = Path(tmp)
                report = output / "results.xml"
                env = os.environ.copy()
                env.update(
                    DATA_DIR=str(output / "data"),
                    KG_BASE_DIR=str(output / "graphs"),
                    DATABASE_URL=f"sqlite+aiosqlite:///{output / 'isolated.db'}",
                    KG_EMBEDDING_MODE="stub",
                    PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(diagnostic(role, "deadline_before_spawn"))
                try:
                    process = subprocess.run(
                        [sys.executable, "-m", "pytest", "-q", "-p", "pytest_asyncio.plugin",
                         "-p", "pytest_timeout", "--timeout=180",
                         *selectors, f"--junitxml={report}", "-o", "addopts="],
                        cwd=roots[role], env=env, capture_output=True, text=True,
                        timeout=remaining,
                    )
                except subprocess.TimeoutExpired as error:
                    # subprocess.run kills and drains its direct child before raising.
                    # Preserve captured output after the isolated directory is removed.
                    raise AssertionError(diagnostic(
                        role, "pytest_aggregate_deadline", error.stdout, error.stderr,
                    )) from error
                except Exception as error:
                    raise AssertionError(diagnostic(
                        role, f"pytest_launch:{type(error).__name__}", stderr=str(error),
                    )) from error
                if process.returncode:
                    return process, None
                try:
                    suites = ET.parse(report).getroot().findall(".//testsuite")
                except Exception as error:
                    raise AssertionError(diagnostic(
                        role, f"junit:{type(error).__name__}", process.stdout,
                        process.stderr + "\n" + str(error),
                    )) from error
                return process, suites

        # Collect every result before assertions: a failed group must not skip
        # observation/cleanup of its peer. Exiting the executor joins both workers.
        outcomes = []
        with ThreadPoolExecutor(max_workers=len(selections)) as executor:
            futures = [(role, executor.submit(run_group, role, selectors))
                       for role, selectors in selections.items()]
            for role, future in futures:
                try:
                    outcomes.append((role, future.result(), None))
                except Exception as error:
                    outcomes.append((role, None, error))

        # Emit both failure diagnostics before subTest/failfast can stop reporting.
        for role, result, error in outcomes:
            if error is not None:
                print(str(error), file=sys.stderr, flush=True)
            elif result[0].returncode:
                print(diagnostic(role, "pytest_exit", result[0].stdout, result[0].stderr),
                      file=sys.stderr, flush=True)
        for role, result, error in outcomes:
            with self.subTest(edition=role):
                if error is not None:
                    raise error
                process, suites = result
                self.assertEqual(process.returncode, 0, (process.stdout + process.stderr)[-12000:])
                self.assertTrue(suites, "No native test results were produced")
                self.assertGreater(sum(int(s.attrib["tests"]) for s in suites), 0)
                for suite in suites:
                    for counter in ("failures", "errors", "skipped"):
                        self.assertEqual(int(suite.attrib[counter]), 0, f"Incomplete regression: {counter}")


class R1Acceptance(_AcceptanceCase):
    def test_bug_lineage_and_provenance_repair(self):
        self._run({
            "core": ("tests/test_e2e043_bug_allocations.py",),
            "community": ("tests/test_e2e043_spec_version_fences.py",),
        })

    def test_pending_qa_and_historical_freeze(self):
        self._run({"core": ("tests/test_e2e043_pending_qa.py",)})

    def test_persisted_validation_and_regression_preview(self):
        self._run({
            "core": (
                "tests/test_e2e043_task_versions.py",
                "tests/test_bug_regression_preview.py",
                "tests/test_task_validation_public_contract.py",
            ),
            "community": ("tests/test_e2e043_spec_version_fences.py",),
        })


class R2Acceptance(_AcceptanceCase):
    def test_quality_audit_source_compatibility(self):
        self._run({
            "core": ("tests/test_e2e043_source_context.py",),
            "community": ("tests/test_delivery_readiness_source_context.py",),
        })

    def test_source_context_and_native_scenario_targets(self):
        self._run({
            "core": (
                "tests/test_e2e043_source_context.py",
                "tests/test_e2e043_contracts.py",
            ),
            "community": ("tests/test_delivery_readiness_source_context.py",),
        })

    def test_concurrent_receipts_and_integrity(self):
        self._run({
            "core": ("tests/test_evidence_v2_mcp_boundary.py",),
            "community": (
                "tests/test_e2e043_evidence_ledger.py",
                "tests/test_evidence_v2_adapter.py",
                "tests/test_evidence_recovery.py",
            ),
        })

    def test_transport_resource_contracts_and_human_receipts(self):
        self._run({
            "core": ("tests/test_e2e043_contracts.py",),
            "community": (
                "tests/test_e2e043_evidence.py",
                "tests/test_mcp_host_adapter.py",
                "tests/test_human_task_delivery_report.py",
            ),
        })


class R3Acceptance(_AcceptanceCase):
    def test_identity_population_and_partial_failure(self):
        self._run({
            "core": ("tests/test_e2e043_kg_boundaries.py",),
            "community": ("tests/test_e2e043_kg_transactions.py",),
        })

    def test_writer_cancellation_and_sql_isolation(self):
        self._run({
            "core": (
                "tests/test_e2e043_kg_boundaries.py",
                "tests/test_e2e043_cognitive_transactions.py",
                "tests/test_e2e043_outbox_observability.py",
                "tests/test_relational_runtime_cancel_safe_contract.py",
            ),
            "community": (
                "tests/test_e2e043_kg_transactions.py",
                "tests/test_code_traceability_persistence.py::test_receipt_submission_admits_writer_before_policy_read",
                "tests/test_worker_runners.py",
                "tests/test_retained_cleanup_shutdown.py",
                "tests/test_routed_graph_shutdown.py",
                "tests/test_worker_shutdown_fail_closed.py",
                "tests/test_shutdown_signal_ownership.py",
                "tests/test_board_runtime_activation_api.py",
                "tests/test_uow_begin_write_by_http_method.py",
                "tests/test_api_deps_error_translation.py",
                "tests/test_sqlite_stop_ownership.py",
                "tests/test_outbox_stop_admission.py",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_community_runtime_satisfies_cancel_safe_relational_port",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_scope_rolls_back_transaction_after_internally_caught_error",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_scope_does_not_rollback_after_explicit_commit",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_scope_cancellation_cannot_interrupt_rollback_then_close",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_background_cleanup_failure_is_logged_and_consumed",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_runtime_close_drains_pending_session_teardown_before_dispose",
                "tests/test_sqlalchemy_session_transaction_recovery.py::test_runtime_close_bounds_stuck_session_cleanup_drain",
            ),
        }, concurrent_groups=True)

    def test_owned_recovery_restart_and_truthful_health(self):
        self._run({
            "core": (
                "tests/test_e2e043_outbox_observability.py",
                "tests/test_global_outbox_dead_letter_operations.py",
                "tests/test_global_outbox_dead_letter_health.py",
                "tests/test_card7_delivery_outbox_results.py",
                "tests/test_r6_test2_kg_health_active_queue_vs_dlq.py",
            ),
            "community": (
                "tests/test_global_outbox_dead_letter_operations.py",
                "tests/test_card7_delivery_outbox_integration.py",
                "tests/test_queue_health_global_outbox_adapter.py",
                "tests/test_e2e043_shutdown_budgets.py",
                "tests/test_runtime.py",
                "tests/test_e2e043_kg_transactions.py::test_cancelled_test_card_replay_keeps_native_ack_truthful",
            ),
        })


if __name__ == "__main__":
    unittest.main()
