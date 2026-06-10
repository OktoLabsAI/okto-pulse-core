"""Wave 2 NC-9 — Test theater prevention gate (spec 873e98cc).

Cobre validation helper `_validate_evidence` e BoardSettings persistence.
Gate full integration (via MCP tool harness) é deferred — testar
helper + persistence garante a lógica core.
"""

from __future__ import annotations

import pytest


pytestmark = pytest.mark.asyncio


async def test_ts1_boardsettings_persists_skip_flag():
    """TS1 — BoardSettings.skip_test_evidence_global = True é persistido
    em Board.settings JSON e retornado via Pydantic.
    """
    from okto_pulse.core.models.schemas import BoardSettings

    s = BoardSettings(skip_test_evidence_global=True)
    assert s.skip_test_evidence_global is True
    # Default = False
    s_default = BoardSettings()
    assert s_default.skip_test_evidence_global is False
    assert s_default.require_spec_validation is True
    assert s_default.require_spec_resource_task_coverage is True
    assert s_default.require_task_validation is True
    assert s_default.skip_cognitive_consolidation is False
    # JSON dump preserves
    dumped = s.model_dump()
    assert dumped["skip_test_evidence_global"] is True


async def test_boardsettings_persists_cognitive_closeout_skip_flag():
    """CCG.1 — BoardSettings.skip_cognitive_consolidation defaults false
    and survives JSON serialization for existing PATCH settings flow.
    """
    from okto_pulse.core.models.schemas import BoardSettings

    settings = BoardSettings(skip_cognitive_consolidation=True)
    dumped = settings.model_dump()

    assert settings.skip_cognitive_consolidation is True
    assert dumped["skip_cognitive_consolidation"] is True
    assert BoardSettings().skip_cognitive_consolidation is False


async def test_ts2_gate_rejects_automated_without_evidence():
    """TS2 — _validate_evidence(status='automated', evidence=None) →
    not ok, missing inclui test_file_path E test_function.
    """
    from okto_pulse.core.services.test_scenario_lifecycle import (
        validate_test_scenario_evidence as _validate_evidence,
    )

    ok, missing = _validate_evidence("automated", None)
    assert not ok
    assert "test_file_path" in missing
    assert "test_function" in missing


async def test_ts3_gate_accepts_automated_with_complete_evidence():
    """TS3 — automated com test_file_path + test_function → ok."""
    from okto_pulse.core.services.test_scenario_lifecycle import (
        validate_test_scenario_evidence as _validate_evidence,
    )

    ok, missing = _validate_evidence(
        "automated",
        {"test_file_path": "tests/foo.py", "test_function": "test_bar"},
    )
    assert ok
    assert missing == []


async def test_ts4_passed_accepts_output_snippet_or_test_run_id():
    """TS4 — passed exige last_run_at + (output_snippet OR test_run_id).

    - output_snippet only → ok
    - test_run_id only → ok
    - neither → not ok
    - both → ok
    """
    from okto_pulse.core.services.test_scenario_lifecycle import (
        validate_test_scenario_evidence as _validate_evidence,
    )

    base = {"last_run_at": "2026-04-27T20:00:00"}

    # output_snippet only
    ok, _ = _validate_evidence("passed", {**base, "output_snippet": "1 passed"})
    assert ok

    # test_run_id only
    ok, _ = _validate_evidence("passed", {**base, "test_run_id": "ci-42"})
    assert ok

    # both
    ok, _ = _validate_evidence(
        "passed",
        {**base, "output_snippet": "1 passed", "test_run_id": "ci-42"},
    )
    assert ok

    # neither — should fail
    ok, missing = _validate_evidence("passed", base)
    assert not ok
    # one-of group reported as combined string
    assert any("output_snippet" in m and "test_run_id" in m for m in missing)


async def test_ts4b_failed_same_as_passed():
    """failed segue mesmas regras de passed."""
    from okto_pulse.core.services.test_scenario_lifecycle import (
        validate_test_scenario_evidence as _validate_evidence,
    )

    ok, _ = _validate_evidence(
        "failed",
        {"last_run_at": "2026-04-27T20:00:00", "output_snippet": "1 failed"},
    )
    assert ok

    ok, missing = _validate_evidence("failed", {"last_run_at": "..."})
    assert not ok


async def test_ts6_draft_and_ready_dont_require_evidence():
    """TS6 — status=draft ou ready não dispara o gate."""
    from okto_pulse.core.services.test_scenario_lifecycle import (
        validate_test_scenario_evidence as _validate_evidence,
    )

    ok, missing = _validate_evidence("draft", None)
    assert ok
    assert missing == []

    ok, missing = _validate_evidence("ready", None)
    assert ok
    assert missing == []


async def test_ts7_response_schema_preserves_scenario_evidence():
    """TS7 - REST response schema must not strip persisted scenario evidence."""
    from okto_pulse.core.models.schemas import TestScenario

    scenario = TestScenario.model_validate({
        "id": "ts-evidence",
        "title": "Scenario with execution evidence",
        "linked_criteria": ["0"],
        "scenario_type": "integration",
        "given": "Given a saved scenario",
        "when": "When the spec response is serialized",
        "then": "Then evidence remains visible to the UI",
        "status": "passed",
        "linked_task_ids": ["card-test"],
        "evidence": {
            "test_file_path": "tests/test_api.py",
            "test_function": "test_contract",
            "last_run_at": "2026-05-09T12:00:00Z",
            "output_snippet": "1 passed",
            "command": "python -m pytest tests/test_api.py -q",
        },
        "latest_evidence": {
            "last_run_at": "2026-05-09T12:05:00Z",
            "test_run_id": "ci-123",
        },
    })

    dumped = scenario.model_dump()

    assert dumped["evidence"]["test_file_path"] == "tests/test_api.py"
    assert dumped["evidence"]["test_function"] == "test_contract"
    assert dumped["evidence"]["last_run_at"] == "2026-05-09T12:00:00Z"
    assert dumped["evidence"]["output_snippet"] == "1 passed"
    assert dumped["evidence"]["command"] == "python -m pytest tests/test_api.py -q"
    assert dumped["latest_evidence"]["test_run_id"] == "ci-123"
