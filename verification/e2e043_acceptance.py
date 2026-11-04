"""Explicit unittest entry points for the installation's paired-source runner.

These methods execute committed regression tests in fresh child interpreters.
They never import an existing CI report or access a running Pulse instance.
The runner authenticates the two source revisions and this exact selector.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET


class _AcceptanceCase(unittest.TestCase):
    def _run(self, selections: dict[str, tuple[str, ...]]) -> None:
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
            ),
            "community": (
                "tests/test_e2e043_kg_transactions.py",
                "tests/test_worker_runners.py",
            ),
        })

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
