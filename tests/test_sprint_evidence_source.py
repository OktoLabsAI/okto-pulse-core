"""Unit coverage for the sprint/spec shared test evidence predicate."""

from __future__ import annotations

from okto_pulse.core.services.main import _test_scenario_has_required_evidence


def test_passed_scenario_accepts_latest_evidence():
    scenario = {
        "status": "passed",
        "latest_evidence": {
            "last_run_at": "2026-05-01T12:00:00Z",
            "output_snippet": "3 passed",
        },
    }

    assert _test_scenario_has_required_evidence(scenario) is True


def test_failed_scenario_requires_run_evidence():
    scenario = {
        "status": "failed",
        "evidence": {"output_snippet": "failed without timestamp"},
    }

    assert _test_scenario_has_required_evidence(scenario) is False


def test_automated_scenario_requires_test_pointer():
    scenario = {
        "status": "automated",
        "evidence": {
            "test_file_path": "tests/test_api.py",
            "test_function": "test_api_contract",
        },
    }

    assert _test_scenario_has_required_evidence(scenario) is True
