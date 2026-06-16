"""R5C-B — publish-health classifier: distinct/actionable reason+source states.

Proves the classifier maps each reason/source to a distinct, actionable
(status, severity, message); that the actionable messages are keyed by category
and are secret-free even over a secret-laden projection; and that the absence of
a source — or an unavailable/expired/stale mandatory source — can NEVER be
reported as healthy.

Spec R5C / card R5C-B / scenarios ts_a81de6bc, ts_60131b20.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.telemetry import failure_state as fs  # noqa: E402
from okto_pulse.core.telemetry import publish_health as ph  # noqa: E402

_NOW = datetime(2026, 6, 15, 13, 1, 0, tzinfo=timezone.utc)
_SECRET = "tok-supersecret-xyz"
_CATALOG = set(ph._REASON_MESSAGES.values()) | set(ph._STATUS_MESSAGES.values())


def _projection(**overrides) -> dict:
    defaults = dict(
        status=fs.STATUS_OK,
        publish_enabled=True,
        consent_state=fs.CONSENT_GRANTED,
        install_id_redacted="iid_abc123def456",
        last_success_at="2026-06-15T13:00:00Z",
    )
    defaults.update(overrides)
    return fs.FailureState(**defaults).to_public_dict()


# --- ts_a81de6bc: five distinct, actionable reason classifications -------------

def test_ts_a81de6bc_distinct_actionable_reasons() -> None:
    cases = {
        "UNKNOWN_INSTALL": (
            _projection(status=fs.STATUS_DEGRADED, reason_code="UNKNOWN_INSTALL", http_status=401),
            ph.DEGRADED,
            ph.REASON_UNKNOWN_INSTALL,
            ph.SEVERITY_WARNING,
        ),
        "TOKEN_EXPIRED": (
            _projection(status=fs.STATUS_DEGRADED, reason_code="TOKEN_EXPIRED", http_status=401),
            ph.DEGRADED,
            ph.REASON_TOKEN_EXPIRED,
            ph.SEVERITY_WARNING,
        ),
        "INVALID_SIGNATURE": (
            _projection(status=fs.STATUS_FATAL, reason_code="INVALID_SIGNATURE", http_status=401),
            ph.FAILING,
            ph.REASON_INVALID_SIGNATURE,
            ph.SEVERITY_CRITICAL,
        ),
        "TRANSPORT_5XX": (
            _projection(status=fs.STATUS_DEGRADED, reason_code="USAGE_503", http_status=503),
            ph.DEGRADED,
            ph.REASON_TRANSPORT_5XX,
            ph.SEVERITY_WARNING,
        ),
        "DISABLED": (
            _projection(status=fs.STATUS_BLOCKED, publish_enabled=False, consent_state=fs.CONSENT_BLOCKED),
            ph.HEALTH_DISABLED,
            ph.REASON_DISABLED,
            ph.SEVERITY_INFO,
        ),
    }

    reason_categories: set[str] = set()
    messages: set[str] = set()
    for label, (projection, exp_status, exp_reason, exp_sev) in cases.items():
        dto = ph.resolve_publish_health(projection, now=_NOW)
        assert dto.status == exp_status, label
        assert dto.reason_category == exp_reason, label
        assert dto.severity == exp_sev, label
        # actionable: non-empty, from the bounded catalog (keyed by category).
        assert dto.message and dto.message in _CATALOG, label
        assert "{" not in dto.message  # no unformatted placeholder
        reason_categories.add(dto.reason_category)
        messages.add(dto.message)

    # all five reason categories AND all five messages are pairwise distinct.
    assert len(reason_categories) == 5
    assert len(messages) == 5


# --- ts_60131b20: AWS/R5B unavailable/expired/stale -> never healthy -----------

def test_ts_60131b20_aws_unavailable_is_not_healthy() -> None:
    healthy_local = _projection(status=fs.STATUS_OK)
    dto = ph.resolve_publish_health(healthy_local, now=_NOW, aws_ingest={"availability": ph.SRC_UNAVAILABLE})
    assert dto.status != ph.HEALTHY
    assert dto.status in {ph.DEGRADED, ph.UNAVAILABLE, ph.STALE}
    assert dto.status == ph.UNAVAILABLE
    assert dto.source == ph.SOURCE_COMBINED
    aws = next(s for s in dto.sources if s["name"] == ph.SOURCE_AWS_INGEST)
    assert aws["available"] is False


def test_ts_60131b20_aws_expired_is_degraded_not_healthy() -> None:
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_OK), now=_NOW, aws_ingest={"availability": ph.SRC_EXPIRED}
    )
    assert dto.status != ph.HEALTHY
    assert dto.status == ph.DEGRADED
    assert dto.reason_category == ph.REASON_SOURCE_EXPIRED


def test_ts_60131b20_aws_stale_maps_stale_ingest() -> None:
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_OK), now=_NOW, aws_ingest={"availability": ph.SRC_STALE}
    )
    assert dto.status == ph.STALE
    assert dto.reason_category == ph.REASON_STALE_INGEST
    assert dto.message == ph._REASON_MESSAGES[ph.REASON_STALE_INGEST]


def test_report_athena_stale_maps_stale_report() -> None:
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_OK), now=_NOW, report_athena={"availability": ph.SRC_STALE}
    )
    assert dto.status == ph.STALE
    assert dto.reason_category == ph.REASON_STALE_REPORT


def test_missing_required_source_is_not_healthy() -> None:
    # a required source that was never provided -> unavailable, never healthy.
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_OK), now=_NOW, required_sources=(ph.SOURCE_AWS_INGEST,)
    )
    assert dto.status == ph.UNAVAILABLE
    assert dto.status != ph.HEALTHY
    names = {s["name"] for s in dto.sources}
    assert ph.SOURCE_AWS_INGEST in names


def test_all_sources_healthy_combines_healthy() -> None:
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_OK),
        now=_NOW,
        aws_ingest={"availability": ph.SRC_AVAILABLE},
        report_athena={"availability": ph.SRC_AVAILABLE},
    )
    assert dto.status == ph.HEALTHY
    assert dto.source == ph.SOURCE_COMBINED
    assert len(dto.sources) == 3


def test_local_disabled_short_circuits_over_stale_aws() -> None:
    # publishing deliberately off: stale AWS must NOT escalate the overall status.
    dto = ph.resolve_publish_health(
        _projection(status=fs.STATUS_BLOCKED, publish_enabled=False, consent_state=fs.CONSENT_BLOCKED),
        now=_NOW,
        aws_ingest={"availability": ph.SRC_STALE},
    )
    assert dto.status == ph.HEALTH_DISABLED
    assert dto.reason_category == ph.REASON_DISABLED


# --- messages are secret-free even over a secret-laden projection --------------

def test_messages_are_secret_free_over_secret_laden_state() -> None:
    # a state carrying real secrets next to the failure-state block; the public
    # projection is allowlisted, and the message is keyed by category -> no secret.
    state = {
        "mode": "anonymous_beacon",
        "install_token": _SECRET,
        "token_hash": "hash-" + _SECRET,
        "signature": "sig-" + _SECRET,
        "nonce": "nonce-" + _SECRET,
        fs.FAILURE_STATE_KEY: {
            "status": fs.STATUS_FATAL,
            "reason_code": "INVALID_SIGNATURE",
            "http_status": 401,
            "last_failure_at": "2026-06-15T13:00:00Z",
            "next_retry_at": "2026-06-15T13:05:00Z",
            "retry_count": 3,
            "publish_enabled": True,
            "consent_state": fs.CONSENT_GRANTED,
            "install_id_redacted": "iid_deadbeef0001",
        },
    }
    projection = fs.public_status_projection(state)
    dto = ph.resolve_publish_health(projection, now=_NOW, aws_ingest={"availability": ph.SRC_STALE})

    out = dto.to_dict()
    blob = json.dumps(out)
    assert _SECRET not in blob
    # the message is from the bounded catalog (keyed by category), not interpolated.
    assert dto.message in _CATALOG
    for source in out["sources"]:
        assert source["message"] in _CATALOG
    # no secret key surfaced anywhere in the DTO.
    for key in out:
        assert not fs.is_secret_key(key), key
    assert out["install_id_redacted"] == "iid_deadbeef0001"


def test_every_catalog_message_is_literal_and_carries_no_value() -> None:
    # The messages are static operator guidance: the WORD "token"/"signature" may
    # appear in advice (e.g. "re-handshake to obtain a new install token"), but no
    # message may carry a VALUE interpolated from the state — proven by the literal
    # form (no format placeholders) and the absence of a redacted-id value marker.
    for message in _CATALOG:
        assert "{" not in message and "}" not in message  # literal, no placeholders
        assert "iid_" not in message  # never embeds a (redacted) install id value
        assert _SECRET not in message
