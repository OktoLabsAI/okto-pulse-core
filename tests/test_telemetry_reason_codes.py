"""Behavioral tests for R1-C: testable /v1/usage reason-code recovery.

Covers test cards c67b7e3e (ts_bfa70eb6 — UNKNOWN_INSTALL with valid consent
recovers once), 9c9476ab (ts_3a1f7d14 — UNKNOWN_INSTALL without consent does not
re-handshake), bcf871c1 (ts_cc1bee08 — INVALID_SIGNATURE is fatal, no loop), and
867b7180 (ts_80e5b9a5 — DUPLICATE is idempotent, advances send-time seq, no
infinite replay). Reason codes are read from the backend JSON body
({"code": ...}), never from log text.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry import failure_state as fs
from okto_pulse.core.telemetry import sender as sender_mod
from okto_pulse.core.telemetry.sender import TelemetryBeaconSender
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.service import TelemetryService

FIXED_NOW = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
HANDSHAKE_URL = "/v1/handshake"
USAGE_URL = "/v1/usage"


class FakeResponse:
    def __init__(self, status_code: int, json_data: dict | None = None):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}

    def json(self) -> dict:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


class ScriptedSession:
    def __init__(self, *, handshake: FakeResponse | None = None, usage: list[FakeResponse] | None = None):
        self._handshake = handshake
        self._usage = list(usage or [])
        self.calls: list[str] = []

    def post(self, url, *args, **kwargs):
        if url.endswith(HANDSHAKE_URL):
            self.calls.append(HANDSHAKE_URL)
            assert self._handshake is not None, "unexpected handshake call"
            return self._handshake
        if url.endswith(USAGE_URL):
            self.calls.append(USAGE_URL)
            assert self._usage, "no scripted /v1/usage response left"
            return self._usage.pop(0)
        raise AssertionError(f"unexpected url {url}")


def _err(status: int, code: str) -> FakeResponse:
    return FakeResponse(status, {"accepted": False, "code": code, "message": code})


def _prepare(tmp_path: Path, monkeypatch, *, with_consent: bool = True) -> CoreSettings:
    monkeypatch.setattr(sender_mod, "_utcnow", lambda: FIXED_NOW)
    monkeypatch.setattr(sender_mod, "_backoff_jitter", lambda: 0.0)
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))

    settings = CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="")
    service = TelemetryService(settings)
    service.update_settings(
        mode="anonymous_beacon",
        source="cli",
        policy_version="2026-05-11",
        schema_version=CURRENT_SCHEMA_VERSION,
    )
    state_path = tmp_path / "metrics" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["install_token"] = "tok-current"
    # far from expiry so the R1-B preventive refresh never triggers here
    state["install_token_expires_at"] = (FIXED_NOW + timedelta(hours=72)).isoformat().replace("+00:00", "Z")
    state["next_batch_seq"] = 5
    if not with_consent:
        # anonymous_beacon mode but NO recorded policy acknowledgement -> consent
        # is not valid for a re-handshake (ts_3a1f7d14).
        state.pop("policy_version", None)
    state_path.write_text(json.dumps(state), encoding="utf-8")
    service.record_event("cli", {"command": "serve"})
    return settings


def _state(settings: CoreSettings) -> dict:
    return json.loads((Path(settings.metrics_dir) / "state.json").read_text(encoding="utf-8"))


def test_unknown_install_with_consent_rehandshakes_once_and_recovers(tmp_path, monkeypatch):
    """ts_bfa70eb6 — one re-handshake + one retry recovers the publish."""
    settings = _prepare(tmp_path, monkeypatch, with_consent=True)
    session = ScriptedSession(
        handshake=FakeResponse(200, {"install_token": "tok-new", "token_ttl_seconds": 2592000, "accepted_schema_version": CURRENT_SCHEMA_VERSION}),
        usage=[_err(401, "UNKNOWN_INSTALL"), FakeResponse(200, {"accepted": True})],
    )
    result = TelemetryBeaconSender(settings, session=session).send_once()  # type: ignore[arg-type]

    assert result["sent"] is True
    assert result.get("recovered") == "rehandshake"
    # exactly one re-handshake, two usage attempts, in order
    assert session.calls == [USAGE_URL, HANDSHAKE_URL, USAGE_URL]
    state = _state(settings)
    assert state["install_token"] == "tok-new"
    assert fs.read_failure_state(state).status == fs.STATUS_OK
    assert state["next_batch_seq"] == 6


def test_unknown_install_retry_still_unknown_backs_off_without_second_rehandshake(tmp_path, monkeypatch):
    """No infinite loop: a single re-handshake, then back off if still unknown."""
    settings = _prepare(tmp_path, monkeypatch, with_consent=True)
    session = ScriptedSession(
        handshake=FakeResponse(200, {"install_token": "tok-new", "token_ttl_seconds": 2592000, "accepted_schema_version": CURRENT_SCHEMA_VERSION}),
        usage=[_err(401, "UNKNOWN_INSTALL"), _err(401, "UNKNOWN_INSTALL")],
    )
    result = TelemetryBeaconSender(settings, session=session).send_once()  # type: ignore[arg-type]

    assert result == {"sent": False, "reason": "unknown_install_unresolved"}
    assert session.calls.count(HANDSHAKE_URL) == 1  # exactly one re-handshake
    assert session.calls.count(USAGE_URL) == 2  # initial + single retry
    fstate = fs.read_failure_state(_state(settings))
    assert fstate.reason_code == "UNKNOWN_INSTALL"
    assert fstate.next_retry_at is not None


def test_unknown_install_without_consent_does_not_rehandshake(tmp_path, monkeypatch):
    """ts_3a1f7d14 — no valid consent -> no /v1/handshake, actionable block."""
    settings = _prepare(tmp_path, monkeypatch, with_consent=False)
    session = ScriptedSession(usage=[_err(401, "UNKNOWN_INSTALL")])  # no handshake scripted

    result = TelemetryBeaconSender(settings, session=session).send_once()  # type: ignore[arg-type]

    assert result == {"sent": False, "reason": "consent_blocked"}
    assert HANDSHAKE_URL not in session.calls  # never re-handshaked
    assert session.calls == [USAGE_URL]
    fstate = fs.read_failure_state(_state(settings))
    assert fstate.status == fs.STATUS_BLOCKED
    assert fstate.consent_state == fs.CONSENT_BLOCKED
    assert fstate.publish_enabled is False
    assert fstate.reason_code == "UNKNOWN_INSTALL"


def test_invalid_signature_is_fatal_without_rehandshake_loop(tmp_path, monkeypatch):
    """ts_cc1bee08 — INVALID_SIGNATURE is fatal/actionable, no re-handshake."""
    settings = _prepare(tmp_path, monkeypatch, with_consent=True)
    session = ScriptedSession(usage=[_err(401, "INVALID_SIGNATURE")])

    result = TelemetryBeaconSender(settings, session=session).send_once()  # type: ignore[arg-type]

    assert result == {"sent": False, "reason": "invalid_signature"}
    assert HANDSHAKE_URL not in session.calls
    assert session.calls == [USAGE_URL]
    fstate = fs.read_failure_state(_state(settings))
    assert fstate.status == fs.STATUS_FATAL
    assert fstate.reason_code == "INVALID_SIGNATURE"


def test_duplicate_is_idempotent_advances_seq_and_not_fatal(tmp_path, monkeypatch):
    """ts_80e5b9a5 — DUPLICATE advances send-time seq, no fatal state, no replay."""
    settings = _prepare(tmp_path, monkeypatch, with_consent=True)
    session = ScriptedSession(usage=[_err(409, "DUPLICATE_NONCE_OR_BATCH_SEQ")])

    result = TelemetryBeaconSender(settings, session=session).send_once()  # type: ignore[arg-type]

    assert result == {"sent": False, "reason": "duplicate", "batch_seq": 5}
    assert session.calls == [USAGE_URL]  # no replay, no re-handshake
    state = _state(settings)
    assert state["next_batch_seq"] == 6  # send-time seq advanced as resolved
    fstate = fs.read_failure_state(state)
    assert fstate.status == fs.STATUS_OK  # not fatal
    assert "circuit_open_until" not in state
