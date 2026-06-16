"""[TEST] R5C security — Health redacts secrets in DTO / API / MCP (ts_26974d8d).

Proves the three BACKEND surfaces of Metrics Publish Health (the service DTO, the
REST `GET /metrics/publish-health` handler, and the `okto_pulse_get_publish_health`
MCP tool) emit NO secret over a state loaded with every forbidden category — token,
install_token, token_hash, signature, full nonce, raw install_id, sensitive payload,
including a nested one. The scan is RECURSIVE over keys AND values (message, sources[],
freshness, an innocent key), keyed on distinctive sentinels.

Teeth (validator criterion A/F): the redaction is proven load-bearing — with the
redaction call replaced by identity the same composition LEAKS, so the assertion has
a real direction of failure. The UI surface is covered in the community repo
(MetricsHealthPanel.test.tsx).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.api import metrics as metrics_api  # noqa: E402
from okto_pulse.core.infra.config import CoreSettings  # noqa: E402
from okto_pulse.core.mcp import server as mcp_server  # noqa: E402
from okto_pulse.core.telemetry import failure_state as fs  # noqa: E402
from okto_pulse.core.telemetry import publish_health as ph  # noqa: E402
from okto_pulse.core.telemetry import service as service_module  # noqa: E402
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION  # noqa: E402
from okto_pulse.core.telemetry.service import TelemetryService  # noqa: E402

_NOW = datetime(2026, 6, 15, 13, 1, 0, tzinfo=timezone.utc)
_RAW_INSTALL_ID = "install-RAWID-sentinel-dddd4444"

# distinctive sentinels per forbidden category (NOT generic strings).
SENTINELS = {
    "install_token": "oat_INSTALLTOKENsentinel_aaaa1111",
    "token": "tok_TOKENsentinel_bbbb2222",
    "token_hash": "f0e1d2c3b4a5" * 4,  # 48 hex (also matches the 32+hex pattern)
    "signature": "sig_SIGNATUREsentinel_cccc3333",
    "nonce": "NONCEsentinel0000aaaa1111bbbb2222cccc3333",
    "raw_install_id": _RAW_INSTALL_ID,
    "payload": "PAYLOADsentinel_eeee5555_sensitive_body",
}


def _settings(tmp_path: Path, monkeypatch) -> CoreSettings:
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="anonymous_beacon")


def _seed_secret_laden_state(tmp_path: Path, settings: CoreSettings) -> None:
    TelemetryService(settings).update_settings(
        mode="anonymous_beacon", source="cli", policy_version="2026-05-11", schema_version=CURRENT_SCHEMA_VERSION
    )
    state_path = tmp_path / "metrics" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update(
        {
            "install_token": SENTINELS["install_token"],
            "token": SENTINELS["token"],
            "token_hash": SENTINELS["token_hash"],
            "signature": SENTINELS["signature"],
            "nonce": SENTINELS["nonce"],
            "install_id": SENTINELS["raw_install_id"],  # raw install id key
            "payload": SENTINELS["payload"],
            "install_token_expires_at": "2027-01-01T00:00:00Z",
            "last_handshake_at": "2026-06-15T12:30:00Z",
            # a secret nested under an innocent-looking key:
            "diagnostics": {"note": SENTINELS["install_token"], "trace": [SENTINELS["signature"]]},
            fs.FAILURE_STATE_KEY: {
                "status": fs.STATUS_DEGRADED,
                "reason_code": "USAGE_503",
                "http_status": 503,
                "last_failure_at": "2026-06-15T13:00:00Z",
                "next_retry_at": "2026-06-15T13:05:00Z",
                "retry_count": 2,
                "consent_state": fs.CONSENT_GRANTED,
                "install_id_redacted": "iid_safehash1234",
            },
        }
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")


def _iter_nodes(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield ("key", key)
            yield from _iter_nodes(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_nodes(item)
    elif isinstance(obj, str):
        yield ("value", obj)


def _assert_fully_redacted(result: dict) -> None:
    # recursive key + value scan (nested dicts/lists: sources[], freshness, diagnostics).
    for kind, item in _iter_nodes(result):
        if kind == "key":
            assert not fs.is_secret_key(item), f"forbidden key surfaced: {item!r}"
        else:
            for name, sentinel in SENTINELS.items():
                assert sentinel not in item, f"{name} sentinel leaked in a value"
    blob = json.dumps(result)
    # the RAW install id value never appears (an install_id key is fine only if its
    # value was converted to the safe iid_ form).
    assert _RAW_INSTALL_ID not in blob
    # the safe redacted id is allowed; redaction is flagged.
    assert result.get("redaction_applied") is True
    assert str(result.get("install_id_redacted", "")).startswith("iid_")


# --- surface 1: the service DTO composition ------------------------------------

def test_service_dto_redacts_every_secret_category(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_secret_laden_state(tmp_path, settings)
    result = TelemetryService(settings).publish_health(now=_NOW)
    _assert_fully_redacted(result)
    # the secret-laden state really does carry the sentinels (fixture sanity).
    assert SENTINELS["install_token"] in (tmp_path / "metrics" / "state.json").read_text(encoding="utf-8")


# --- surface 2: the REST GET /metrics/publish-health handler --------------------

async def test_rest_handler_redacts_every_secret_category(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_secret_laden_state(tmp_path, settings)
    monkeypatch.setattr(metrics_api, "get_settings", lambda: settings)
    result = await metrics_api.get_metrics_publish_health(_="test-user")
    assert isinstance(result, dict)  # success path returns the DTO dict
    _assert_fully_redacted(result)


# --- surface 3: the MCP okto_pulse_get_publish_health tool ----------------------

async def test_mcp_tool_redacts_every_secret_category(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_secret_laden_state(tmp_path, settings)
    monkeypatch.setattr(mcp_server, "get_settings", lambda: settings)
    with patch.object(mcp_server, "_get_authenticated_agent", AsyncMock(return_value=object())):
        raw = await mcp_server.okto_pulse_get_publish_health.fn()
    result = json.loads(raw)
    _assert_fully_redacted(result)


# --- TEETH: the redaction is load-bearing (direction of failure) ----------------

def test_service_redaction_is_load_bearing(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_secret_laden_state(tmp_path, settings)
    leak = "oat_LEAKsentinel_99998888"

    real_resolve = ph.resolve_publish_health

    def leaky_resolve(*args, **kwargs):
        dto = real_resolve(*args, **kwargs)
        # simulate a future code path injecting a secret into a rendered field.
        object.__setattr__(dto, "message", f"{dto.message} {leak}")
        return dto

    monkeypatch.setattr(service_module.publish_health_mod, "resolve_publish_health", leaky_resolve)

    # redaction ON -> the injected secret is scrubbed.
    guarded = TelemetryService(settings).publish_health(now=_NOW)
    assert leak not in json.dumps(guarded)

    # redaction OFF (identity) -> the SAME composition LEAKS (proves the guard works).
    monkeypatch.setattr(service_module.publish_health_mod, "redact_health_payload", lambda payload, **kw: payload)
    leaked = TelemetryService(settings).publish_health(now=_NOW)
    assert leak in json.dumps(leaked)


def test_redact_scrubs_nested_and_innocent_fields_directly() -> None:
    # direct recursive teeth over message / sources[] / freshness / an innocent key.
    payload = {
        "status": "degraded",
        "message": f"see {SENTINELS['install_token']}",
        "freshness": {"note": SENTINELS["nonce"]},
        "sources": [{"name": "local", "debug": SENTINELS["signature"], "deep": [{"x": SENTINELS['token']}]}],
        "innocent": SENTINELS["payload"],
        "install_id": _RAW_INSTALL_ID,
        "redaction_applied": True,
        "install_id_redacted": "iid_keepme",
    }
    secret_values = list(SENTINELS.values())
    # without redaction the secrets are present (direction of failure).
    assert any(s in json.dumps(payload) for s in secret_values)
    redacted = ph.redact_health_payload(payload, secret_values=secret_values)
    _assert_fully_redacted({**redacted, "redaction_applied": True})
    assert redacted["install_id"].startswith("iid_")  # raw install_id key -> iid_
