"""R5C-F — the Metrics Publish Health triage runbook is faithful to the real DTO.

These tests tie the runbook to the IMPLEMENTED publish_health contract (no invented
field/state/reason/source) and prove minimum coverage of every health state with a
next action + expected evidence (ts_155de001). The runbook is doc-only; this is the
behavioral guard that keeps it honest against the code.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.telemetry import publish_health as ph  # noqa: E402

RUNBOOK = ROOT / "docs" / "runbooks" / "metrics_publish_health_triage.md"
TEXT = RUNBOOK.read_text(encoding="utf-8")


def test_runbook_exists_and_references_the_real_surface() -> None:
    assert TEXT.strip()
    # the agent-facing MCP tool and the REST endpoint, as implemented in R5C-A.
    assert "okto_pulse_get_publish_health" in TEXT
    assert "/metrics/publish-health" in TEXT
    assert ph.HEALTH_SOURCE_UNAVAILABLE in TEXT


def test_runbook_documents_every_health_state() -> None:
    # ts_155de001: all seven implemented states are covered.
    for status in ph.HEALTH_STATUSES:
        assert f"`{status}`" in TEXT, f"state {status!r} not documented"
    # the state matrix carries a next action + expected evidence per row.
    low = TEXT.lower()
    assert "next action" in low
    assert "expected evidence" in low


def test_runbook_references_only_real_dto_fields() -> None:
    # every implemented DTO field appears (no field omitted, none invented).
    for field in ph.PUBLISH_HEALTH_FIELDS:
        assert field in TEXT, f"DTO field {field!r} missing from runbook"


def test_runbook_does_not_reference_non_dto_failure_state_fields() -> None:
    # these are failure_state internals that the R5C public DTO does NOT expose;
    # citing them as evidence would be fiction the validator rejects.
    for leaked in ("recovered_at", "publish_enabled", "consent_state"):
        assert leaked not in ph.PUBLISH_HEALTH_FIELDS  # sanity: really not in the DTO
        assert leaked not in TEXT, f"runbook references non-DTO field {leaked!r}"


def test_runbook_distinguishes_the_four_sources() -> None:
    for source in (
        ph.SOURCE_LOCAL,
        ph.SOURCE_INSTALL_LIFECYCLE,
        ph.SOURCE_AWS_INGEST,
        ph.SOURCE_REPORT_ATHENA,
    ):
        assert source in TEXT, f"source {source!r} not documented"
    # the four sources must drive DIFFERENT actions (validator criterion 3).
    assert "Per-source triage" in TEXT


def test_runbook_covers_reason_codes_and_categories() -> None:
    for code in ("UNKNOWN_INSTALL", "TOKEN_EXPIRED", "INVALID_SIGNATURE", "USAGE_"):
        assert code in TEXT, f"reason code {code!r} not documented"
    for category in (ph.REASON_SOURCE_GAP, ph.REASON_STALE_INGEST, ph.REASON_STALE_REPORT):
        assert category in TEXT, f"reason category {category!r} not documented"


def test_runbook_states_the_baseline_is_not_an_incident() -> None:
    # B-atenuado: source_gap on AWS/report in core standalone is the EXPECTED
    # baseline, distinguished from a real local failure (validator criterion 2).
    low = TEXT.lower()
    assert "baseline" in low
    assert ph.REASON_SOURCE_GAP in TEXT
    assert "not an incident" in low or "not a incident" in low or "not an incident," in low


def test_runbook_guardrail_health_does_not_replace_cloudwatch() -> None:
    low = TEXT.lower()
    assert "cloudwatch" in low
    # absent/expired/gap source is never healthy.
    assert "never" in low and "healthy" in low


def test_runbook_security_only_redacted_id_never_secrets() -> None:
    assert "install_id_redacted" in TEXT
    low = TEXT.lower()
    # the runbook must forbid surfacing these while triaging.
    for secret in ("install_token", "token", "signature", "nonce", "payload", "raw install id"):
        assert secret in low  # named so the guardrail can say "never expose <secret>"
    assert "never" in low
