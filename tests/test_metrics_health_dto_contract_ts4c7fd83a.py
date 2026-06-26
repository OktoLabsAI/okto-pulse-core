"""[TEST] R5C — publish-health MCP/API returns the COMPLETE DTO (ts_4c7fd83a).

Given a realistic local failure-state (a prior success, a recent failure, and a
scheduled retry), When an agent calls the MCP tool okto_pulse_get_publish_health
or the REST GET /metrics/publish-health handler, Then the response carries the
full R5C DTO with values DERIVED FROM the seeded state.

The field set is tied to the runtime constant ph.PUBLISH_HEALTH_FIELDS (not a
hardcoded list); the seeded timestamps/retry/reason are asserted exactly; freshness
age is derived from last_success_at vs now; and the teeth tests prove the contract
goes RED if a required field disappears or comes back empty/None/0.

Note (flagged to coordination): freshness is `{last_success_at, age_seconds,
is_stale, stale_threshold_seconds}` as implemented (R5C-A/C) — the spec's
`ingest_lag_minutes`/`report_lag_minutes` are NOT named freshness fields here; the
per-source freshness lives in `sources[]`. The validator's R5C freshness shape
(criterion 4) matches the implemented one, so this test asserts the real contract
and does not reopen runtime.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

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


@pytest.fixture(autouse=True)
def _register_telemetry_port():
    """R10-E Pass 2: the port registry is fail-closed. REST/MCP handlers call
    get_telemetry_port(settings) — register a factory that builds TelemetryService."""
    from okto_pulse.core.telemetry.telemetry_port_registry import (
        register_telemetry_port_factory,
        reset_telemetry_port_factory_for_tests,
    )
    reset_telemetry_port_factory_for_tests()
    register_telemetry_port_factory(lambda s: TelemetryService(s))
    yield
    reset_telemetry_port_factory_for_tests()
_LAST_SUCCESS = datetime(2026, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
_RAW_INSTALL_ID = "install-RAWID-contract-7777"

# distinctive seeded values (non-empty, non-zero) so the contract proves the DTO
# carries the STATE, not a complete-but-empty shell.
SEED = {
    "last_success_at": "2026-06-15T09:00:00Z",
    "last_failure_at": "2026-06-15T12:55:00Z",
    "next_retry_at": "2026-06-15T13:10:00Z",
    "retry_count": 3,
    "reason_code": "USAGE_503",
    "http_status": 503,
    "install_id_redacted": "iid_contract7777",
}


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D401 - deterministic now for freshness
        return _NOW.astimezone(tz) if tz else _NOW


def _settings(tmp_path: Path, monkeypatch) -> CoreSettings:
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    monkeypatch.setattr(service_module, "datetime", _FixedDateTime)  # deterministic now
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="anonymous_beacon")


def _seed_realistic_failure_state(tmp_path: Path, settings: CoreSettings) -> None:
    TelemetryService(settings).update_settings(
        mode="anonymous_beacon", source="cli", policy_version="2026-05-11", schema_version=CURRENT_SCHEMA_VERSION
    )
    state_path = tmp_path / "metrics" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update(
        {
            "install_token": "oat_secret_should_not_surface",  # a real secret in state
            "install_id": _RAW_INSTALL_ID,
            "install_token_expires_at": "2027-01-01T00:00:00Z",
            "last_handshake_at": "2026-06-15T08:55:00Z",
            fs.FAILURE_STATE_KEY: {
                "status": fs.STATUS_DEGRADED,
                "reason_code": SEED["reason_code"],
                "http_status": SEED["http_status"],
                "last_success_at": SEED["last_success_at"],
                "last_failure_at": SEED["last_failure_at"],
                "next_retry_at": SEED["next_retry_at"],
                "retry_count": SEED["retry_count"],
                "consent_state": fs.CONSENT_GRANTED,
                "install_id_redacted": SEED["install_id_redacted"],
            },
        }
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")


def _assert_complete_contract(result: dict) -> None:
    # 1. exact field set, tied to the runtime constant (not a hardcoded list).
    assert set(result) == set(ph.PUBLISH_HEALTH_FIELDS)
    # 3. values derived from the seeded state, exact + non-empty/non-zero.
    assert result["last_success_at"] == SEED["last_success_at"]
    assert result["last_failure_at"] == SEED["last_failure_at"]
    assert result["next_retry_at"] == SEED["next_retry_at"]
    assert result["retry_count"] == SEED["retry_count"] and result["retry_count"] > 0
    assert result["reason_code"] == SEED["reason_code"]
    assert result["http_status"] == SEED["http_status"]
    # 4. nested freshness; age_seconds DERIVED from last_success_at vs now.
    fr = result["freshness"]
    assert set(fr) == {"last_success_at", "age_seconds", "is_stale", "stale_threshold_seconds"}
    assert fr["last_success_at"] == SEED["last_success_at"]
    assert fr["age_seconds"] == int((_NOW - _LAST_SUCCESS).total_seconds())  # 14460s, computed
    assert isinstance(fr["is_stale"], bool)
    assert isinstance(fr["stale_threshold_seconds"], int) and fr["stale_threshold_seconds"] > 0
    # 6. redaction: redacted id only, raw id absent, flagged.
    assert result["install_id_redacted"] == SEED["install_id_redacted"]
    assert result["install_id_redacted"].startswith("iid_")
    assert result["redaction_applied"] is True
    assert _RAW_INSTALL_ID not in json.dumps(result)
    assert "oat_secret_should_not_surface" not in json.dumps(result)
    # coherent enums + message + sources.
    assert result["source"] in ph.HEALTH_SOURCES
    assert result["status"] in ph.HEALTH_STATUSES
    assert result["severity"] in ph.SEVERITIES
    assert isinstance(result["message"], str) and result["message"]
    assert isinstance(result["sources"], list) and result["sources"]


# --- the three real surfaces return the complete, state-derived DTO ------------

def test_service_surface_returns_complete_dto(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_realistic_failure_state(tmp_path, settings)
    _assert_complete_contract(TelemetryService(settings).publish_health())


async def test_rest_handler_returns_complete_dto(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_realistic_failure_state(tmp_path, settings)
    monkeypatch.setattr(metrics_api, "get_settings", lambda: settings)
    result = await metrics_api.get_metrics_publish_health(_="test-user")
    assert isinstance(result, dict)
    _assert_complete_contract(result)


async def test_mcp_tool_returns_complete_dto(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, monkeypatch)
    _seed_realistic_failure_state(tmp_path, settings)
    monkeypatch.setattr(mcp_server, "get_settings", lambda: settings)
    with patch.object(mcp_server, "_get_authenticated_agent", AsyncMock(return_value=object())):
        raw = await mcp_server.okto_pulse_get_publish_health.fn()
    _assert_complete_contract(json.loads(raw))


# --- TEETH: a missing or empty required field makes the contract RED ------------

@pytest.fixture()
def _seeded_dto(tmp_path: Path, monkeypatch) -> dict:
    settings = _settings(tmp_path, monkeypatch)
    _seed_realistic_failure_state(tmp_path, settings)
    return TelemetryService(settings).publish_health()


@pytest.mark.parametrize(
    "field",
    ["status", "reason_code", "last_success_at", "last_failure_at", "next_retry_at", "retry_count", "freshness", "install_id_redacted"],
)
def test_missing_required_field_breaks_the_contract(_seeded_dto, field) -> None:
    broken = dict(_seeded_dto)
    broken.pop(field)
    with pytest.raises(AssertionError):
        _assert_complete_contract(broken)


@pytest.mark.parametrize(
    "field,bad_value",
    [("next_retry_at", None), ("retry_count", 0), ("last_success_at", ""), ("last_failure_at", None), ("reason_code", None)],
)
def test_empty_required_value_breaks_the_contract(_seeded_dto, field, bad_value) -> None:
    broken = dict(_seeded_dto)
    broken[field] = bad_value
    with pytest.raises(AssertionError):
        _assert_complete_contract(broken)
