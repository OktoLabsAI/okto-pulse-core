"""R5C-E — redaction guardrails for the Metrics Publish Health API/MCP/UI/logs.

ANTI-TAUTOLOGY (validator complement): it is not enough to run the guardrail over an
already-clean DTO. These tests INJECT a secret into a future field / tampered
sources[] / a value under an innocent key and prove the guardrail redacts it; cover
both forbidden KEY NAMES and sentinel VALUES recursively (sources[], freshness); and
prove the guardrail is non-destructive/idempotent on a legitimate DTO (descriptive
words like "token"/"install" in catalog messages are NOT redacted by false positive).

Spec R5C / card R5C-E / scenario ts_26974d8d / BR br_f38bf9a6.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.infra.config import CoreSettings  # noqa: E402
from okto_pulse.core.telemetry import failure_state as fs  # noqa: E402
from okto_pulse.core.telemetry import publish_health as ph  # noqa: E402
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION  # noqa: E402
from okto_pulse.core.telemetry.service import TelemetryService  # noqa: E402
from okto_pulse.core.telemetry.publish_health_source_registry import (  # noqa: E402
    register_external_source_provider,
)

_NOW = datetime(2026, 6, 15, 13, 1, 0, tzinfo=timezone.utc)
_SECRET = "oat_SEGREDOreal_supersecret_value_123456"
_FORBIDDEN_SUBSTRINGS = ("install_token", "token_hash", "signature", "oat_", "nonce_raw")


def _has_no_forbidden(blob: str) -> None:
    assert _SECRET not in blob
    assert "oat_" not in blob
    # full 32+ hex nonce/hash must not appear
    import re

    assert not re.search(r"\b[0-9a-fA-F]{32,}\b", blob)


# --- anti-tautology: secret injected under an INNOCENT key / future field ------

def test_redact_scrubs_secret_value_under_innocent_key() -> None:
    tampered = {
        "status": "degraded",
        "note": f"debug {_SECRET} trailing",  # secret VALUE under an innocent key
        "future_field": {"deep": [{"x": "oat_anotherLeak999"}]},
    }
    out = ph.redact_health_payload(tampered)
    blob = json.dumps(out)
    _has_no_forbidden(blob)
    assert out["status"] == "degraded"  # safe data preserved
    assert ph.REDACTED in out["note"]


def test_redact_scrubs_tampered_sources_and_forbidden_keys_recursively() -> None:
    tampered = {
        "status": "healthy",
        "install_token": _SECRET,  # forbidden key at top level
        "freshness": {"last_success_at": "2026-06-15T13:00:00Z", "token_hash": "deadbeef" * 8},
        "sources": [
            {"name": "local", "status": "healthy", "signature": _SECRET},
            {"name": "aws_ingest", "debug": {"nonce": "f" * 40, "note": _SECRET}},
        ],
    }
    out = ph.redact_health_payload(tampered)
    blob = json.dumps(out)
    _has_no_forbidden(blob)
    assert out["install_token"] == ph.REDACTED
    assert out["freshness"]["token_hash"] == ph.REDACTED
    assert out["freshness"]["last_success_at"] == "2026-06-15T13:00:00Z"  # safe preserved
    assert out["sources"][0]["signature"] == ph.REDACTED
    assert out["sources"][1]["debug"]["nonce"] == ph.REDACTED
    assert ph.REDACTED in out["sources"][1]["debug"]["note"]


def test_redact_scrubs_known_state_secret_value_anywhere() -> None:
    state = {"install_token": _SECRET, "failure_state": {"status": "ok"}}
    secret_values = ph.collect_health_secret_values(state)
    assert _SECRET in secret_values
    # the same secret value re-appears under an innocent key in the payload:
    payload = {"status": "ok", "echo": f"value={_SECRET}"}
    out = ph.redact_health_payload(payload, secret_values=secret_values)
    assert _SECRET not in json.dumps(out)


def test_redact_converts_raw_install_id_to_safe_form() -> None:
    raw = "install-12345-raw-id"
    out = ph.redact_health_payload({"install_id": raw, "status": "ok"})
    assert out["install_id"] != raw
    assert out["install_id"].startswith("iid_")
    assert out["install_id"] == fs.redact_install_id(raw)


def test_redact_scrubs_full_nonce_hex_under_any_key() -> None:
    out = ph.redact_health_payload({"correlation_id": "a1b2c3d4" * 5})  # 40 hex chars
    assert out["correlation_id"] == ph.REDACTED


# --- non-destructive / idempotent on a legitimate DTO --------------------------

def _clean_dto() -> dict:
    projection = fs.FailureState(
        status=fs.STATUS_OK,
        publish_enabled=True,
        consent_state=fs.CONSENT_GRANTED,
        last_success_at="2026-06-15T13:00:00Z",
        install_id_redacted="iid_abc123def456",
    ).to_public_dict()
    return ph.resolve_publish_health(
        projection,
        now=_NOW,
        install_lifecycle={"availability": ph.SRC_AVAILABLE},
        aws_ingest={"availability": ph.SRC_GAP},
        report_athena={"availability": ph.SRC_GAP},
    ).to_dict()


def test_redact_is_non_destructive_on_clean_dto() -> None:
    dto = _clean_dto()
    redacted = ph.redact_health_payload(dto)
    assert redacted == dto  # nothing mutilated
    assert redacted["install_id_redacted"] == "iid_abc123def456"  # safe id preserved
    assert redacted["freshness"]["last_success_at"] == "2026-06-15T13:00:00Z"


def test_redact_is_idempotent() -> None:
    dto = _clean_dto()
    once = ph.redact_health_payload(dto)
    twice = ph.redact_health_payload(once)
    assert once == twice


def test_redact_preserves_descriptive_words_in_catalog_messages() -> None:
    # words like "token"/"install"/"signature" appear in actionable guidance and must
    # NOT be redacted by a false positive (validator complement #4).
    for message in set(ph._REASON_MESSAGES.values()) | set(ph._STATUS_MESSAGES.values()):
        assert ph.redact_health_payload(message) == message


# --- service end-to-end (ts_26974d8d): API/MCP surface is redacted -------------

def _settings(tmp_path: Path, monkeypatch) -> CoreSettings:
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    register_external_source_provider(
        lambda _settings: (
            {"availability": ph.SRC_GAP},
            {"availability": ph.SRC_GAP},
        )
    )
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="anonymous_beacon")


def _enable_with_secrets(tmp_path: Path, settings: CoreSettings) -> None:
    TelemetryService(settings).update_settings(
        mode="anonymous_beacon", source="cli", policy_version="2026-05-11", schema_version=CURRENT_SCHEMA_VERSION
    )
    state_path = tmp_path / "metrics" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["install_token"] = _SECRET
    state["token_hash"] = "f" * 40
    state["signature"] = "sig_" + _SECRET
    state["nonce"] = "a1b2c3d4" * 5
    state["install_token_expires_at"] = "2027-01-01T00:00:00Z"
    state["last_handshake_at"] = "2026-06-15T12:30:00Z"
    state[fs.FAILURE_STATE_KEY] = {
        "status": fs.STATUS_OK,
        "last_success_at": "2026-06-15T13:00:00Z",
        "publish_enabled": True,
        "consent_state": fs.CONSENT_GRANTED,
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")


def test_ts_26974d8d_service_surface_redacts_secrets(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _enable_with_secrets(tmp_path, settings)

    # the API endpoint and the MCP tool both return this dict verbatim, so the
    # service chokepoint guarantees both surfaces are redacted.
    result = TelemetryService(settings).publish_health(now=_NOW)

    blob = json.dumps(result)
    _has_no_forbidden(blob)
    assert "sig_" not in blob
    for key in result:
        assert not fs.is_secret_key(key), key
    assert set(result) == set(ph.PUBLISH_HEALTH_FIELDS)


def test_publish_health_log_carries_no_secret(tmp_path: Path, monkeypatch, caplog) -> None:
    caplog.set_level("INFO", logger="okto_pulse.telemetry.service")
    settings = _settings(tmp_path, monkeypatch)
    _enable_with_secrets(tmp_path, settings)

    TelemetryService(settings).publish_health(now=_NOW)

    health_logs = [r for r in caplog.records if r.__dict__.get("metric_name") == "metrics_publish_health_total"]
    assert health_logs
    for record in health_logs:
        blob = json.dumps(record.__dict__, default=str)
        assert _SECRET not in blob
        assert "oat_" not in blob
