"""[TEST] R5C — minimum health-state coverage in the contract/classifier (ts_155de001).

The six minimum states (healthy, degraded, failing, stale, disabled, unavailable)
must exist in the contract enum and each classify to its expected status + severity
+ actionable message. Tied explicitly to ts_155de001; the UI half lives in the
community MetricsHealthPanel.test.tsx.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.telemetry import failure_state as fs  # noqa: E402
from okto_pulse.core.telemetry import publish_health as ph  # noqa: E402

_NOW = datetime(2026, 6, 15, 13, 1, 0, tzinfo=timezone.utc)

# the SIX minimum health states the spec requires the surface to cover.
MIN_STATES = (
    ph.HEALTHY,
    ph.DEGRADED,
    ph.FAILING,
    ph.STALE,
    ph.HEALTH_DISABLED,
    ph.UNAVAILABLE,
)


def _projection(**overrides) -> dict:
    defaults = dict(
        status=fs.STATUS_OK,
        publish_enabled=True,
        consent_state=fs.CONSENT_GRANTED,
        last_success_at="2026-06-15T13:00:00Z",
    )
    defaults.update(overrides)
    return fs.FailureState(**defaults).to_public_dict()


# (label, projection, expected status, expected severity, expected reason_category)
_STATE_CASES = [
    ("healthy", _projection(status=fs.STATUS_OK), ph.HEALTHY, ph.SEVERITY_NONE, ph.REASON_NONE),
    (
        "degraded",
        _projection(status=fs.STATUS_DEGRADED, reason_code="USAGE_503", http_status=503),
        ph.DEGRADED,
        ph.SEVERITY_WARNING,
        ph.REASON_TRANSPORT_5XX,
    ),
    (
        "failing",
        _projection(status=fs.STATUS_FATAL, reason_code="INVALID_SIGNATURE", http_status=401),
        ph.FAILING,
        ph.SEVERITY_CRITICAL,
        ph.REASON_INVALID_SIGNATURE,
    ),
    (
        "stale",
        _projection(status=fs.STATUS_OK, last_success_at="2026-06-14T00:00:00Z"),
        ph.STALE,
        ph.SEVERITY_WARNING,
        ph.REASON_NONE,
    ),
    (
        "disabled",
        _projection(status=fs.STATUS_BLOCKED, publish_enabled=False, consent_state=fs.CONSENT_BLOCKED),
        ph.HEALTH_DISABLED,
        ph.SEVERITY_INFO,
        ph.REASON_DISABLED,
    ),
    (
        "unavailable",
        _projection(status=fs.STATUS_UNKNOWN, last_success_at=None),
        ph.UNAVAILABLE,
        ph.SEVERITY_WARNING,
        ph.REASON_NONE,
    ),
]


# --- criterion 1: the minimum states exist in the contract ---------------------

def test_ts155de001_min_states_exist_in_contract() -> None:
    for state in MIN_STATES:
        assert state in ph.HEALTH_STATUSES, f"minimum state {state!r} missing from the contract"
    assert len(set(MIN_STATES)) == 6


# --- criterion 2: each state classifies to status + severity + actionable msg --

@pytest.mark.parametrize("label,projection,exp_status,exp_severity,exp_reason", _STATE_CASES)
def test_ts155de001_state_classification(label, projection, exp_status, exp_severity, exp_reason) -> None:
    dto = ph.resolve_publish_health(projection, now=_NOW)
    assert dto.status == exp_status, label
    assert dto.severity == exp_severity, label
    assert dto.severity == ph.severity_for(exp_status), label  # coherent
    # actionable message is the specific catalog entry for the (status, reason).
    assert dto.message == ph.actionable_message(exp_status, exp_reason), label
    assert dto.message and "{" not in dto.message, label
    # a non-healthy state is never severity none (never a success indicator).
    if exp_status != ph.HEALTHY:
        assert dto.severity != ph.SEVERITY_NONE, label


def test_ts155de001_states_have_distinct_messages() -> None:
    messages = {
        ph.resolve_publish_health(projection, now=_NOW).message for _, projection, *_ in _STATE_CASES
    }
    # healthy/stale/unavailable use status-fallback messages; degraded/failing/disabled
    # use reason messages — all six remain distinct.
    assert len(messages) == 6


# --- criterion 4: teeth — dropping a state / collapsing severity breaks it ------

def test_ts155de001_teeth_dropping_a_state_breaks_the_contract(monkeypatch) -> None:
    monkeypatch.setattr(ph, "HEALTH_STATUSES", ph.HEALTH_STATUSES - {ph.FAILING})
    with pytest.raises(AssertionError):
        for state in MIN_STATES:
            assert state in ph.HEALTH_STATUSES


def test_ts155de001_teeth_collapsed_severity_is_detected(monkeypatch) -> None:
    fatal = _projection(status=fs.STATUS_FATAL, reason_code="INVALID_SIGNATURE", http_status=401)
    assert ph.resolve_publish_health(fatal, now=_NOW).severity == ph.SEVERITY_CRITICAL  # baseline
    # collapse the severity classifier -> a failing state is no longer critical, so the
    # per-state severity assertion would go red (direction of failure).
    monkeypatch.setattr(ph, "severity_for", lambda status: ph.SEVERITY_NONE)
    assert ph.resolve_publish_health(fatal, now=_NOW).severity != ph.SEVERITY_CRITICAL
