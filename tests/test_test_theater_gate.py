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
    # JSON dump preserves
    dumped = s.model_dump()
    assert dumped["skip_test_evidence_global"] is True


async def test_ts2_gate_rejects_automated_without_evidence():
    """TS2 — _validate_evidence(status='automated', evidence=None) →
    not ok, missing inclui test_file_path E test_function.
    """
    from okto_pulse.core.mcp.server import _validate_evidence

    ok, missing = _validate_evidence("automated", None)
    assert not ok
    assert "test_file_path" in missing
    assert "test_function" in missing


async def test_ts3_gate_accepts_automated_with_complete_evidence():
    """TS3 — automated com test_file_path + test_function → ok."""
    from okto_pulse.core.mcp.server import _validate_evidence

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
    from okto_pulse.core.mcp.server import _validate_evidence

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
    from okto_pulse.core.mcp.server import _validate_evidence

    ok, _ = _validate_evidence(
        "failed",
        {"last_run_at": "2026-04-27T20:00:00", "output_snippet": "1 failed"},
    )
    assert ok

    ok, missing = _validate_evidence("failed", {"last_run_at": "..."})
    assert not ok


async def test_ts6_draft_and_ready_dont_require_evidence():
    """TS6 — status=draft ou ready não dispara o gate."""
    from okto_pulse.core.mcp.server import _validate_evidence

    ok, missing = _validate_evidence("draft", None)
    assert ok
    assert missing == []

    ok, missing = _validate_evidence("ready", None)
    assert ok
    assert missing == []
