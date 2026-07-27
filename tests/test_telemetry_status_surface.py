"""Behavioral tests for R1-D: sanitized publish-status surface for UI/MCP/CLI.

Covers test card dfeb73f7 (ts_c39b140e — status visible without secrets) and
closes the R1-A leftover 11c0ac9b (ts_c5b11103 — legacy state migrates and is
exposed via the service surface). All consumers (UI via the metrics API, MCP and
CLI) read TelemetryService.summary(), so asserting the service output covers the
contract. The projection is allowlist-based (R1-A), so secrets cannot leak.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry import failure_state as fs
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.service import TelemetryService
from okto_pulse.core.telemetry.settings import save_state


class _NullStore:
    """Minimal in-test TelemetryEventStore (R10-E Pass 2: the concrete core store
    was removed; ``summary()`` reads failure_state from state, not the store)."""

    def append_event(self, e): ...
    def append_sent(self, r, *, failed=False): ...
    def append_snapshot(self, r): ...
    def confirmed_event_ids(self):
        return set()

    def iter_events(self, *, since=None):
        return ()

    def summarize(self, *, window_days=30):
        return {}

    def prune_old(self, *, now=None):
        return {}

    def export_events(self, output_path=None): ...
    def purge_events(self):
        return {}


@pytest.fixture(autouse=True)
def _register_fake_store():
    """R10-E Pass 2: the core registries are fail-closed (no concrete fallback), so
    a TelemetryService round-trip needs a registered store factory."""
    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )

    reset_telemetry_event_store_factory_for_tests()
    register_telemetry_event_store_factory(lambda base, ret: _NullStore())
    yield
    reset_telemetry_event_store_factory_for_tests()


_SECRET_TOKEN = "SECRET-INSTALL-TOKEN"
_SECRET_HASH = "SECRET-TOKEN-HASH"
_AC_FIELDS = (
    "status",
    "reason_code",
    "last_success_at",
    "last_failure_at",
    "next_retry_at",
)


def _settings(tmp_path: Path) -> CoreSettings:
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="")


def _assert_summary_has_no_secret(summary: dict) -> None:
    blob = json.dumps(summary, default=str)
    assert _SECRET_TOKEN not in blob
    assert _SECRET_HASH not in blob

    def walk(node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                assert not fs.is_secret_key(key), f"secret key surfaced: {key}"
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(summary)


def test_summary_exposes_publish_status_without_secrets(tmp_path):
    """ts_c39b140e — failure-state fields are readable; secrets never appear."""
    settings = _settings(tmp_path)
    save_state(
        tmp_path / "metrics",
        {
            "mode": "anonymous_beacon",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "policy_version": "2026-05-11",
            "install_token": _SECRET_TOKEN,
            "token_hash": _SECRET_HASH,
            "install_token_expires_at": "2026-07-15T00:00:00Z",
            "next_batch_seq": 3,
            "failure_state": {
                "status": fs.STATUS_DEGRADED,
                "reason_code": "USAGE_503",
                "http_status": 503,
                "last_success_at": "2026-06-15T10:00:00Z",
                "last_failure_at": "2026-06-15T11:00:00Z",
                "next_retry_at": "2026-06-15T11:15:00Z",
                "retry_count": 2,
                "recovered_at": None,
                "publish_enabled": True,
                "consent_state": fs.CONSENT_GRANTED,
            },
        },
    )

    summary = TelemetryService(settings).summary()
    publish = summary["publish_status"]

    # AC ac_b2051d3c: the named fields are readable.
    for field in _AC_FIELDS:
        assert field in publish
    assert publish["status"] == fs.STATUS_DEGRADED
    assert publish["reason_code"] == "USAGE_503"
    assert publish["last_failure_at"] == "2026-06-15T11:00:00Z"
    assert publish["next_retry_at"] == "2026-06-15T11:15:00Z"
    # exactly the allowlisted schema fields, nothing else
    assert set(publish) == set(fs.PUBLIC_FAILURE_STATE_FIELDS)
    # no install_token / token_hash anywhere in the summary or its logs path
    _assert_summary_has_no_secret(summary)


def test_summary_migrates_legacy_state_with_safe_defaults_no_secrets(tmp_path):
    """ts_c5b11103 (R1-A leftover) — legacy state without a failure_state block
    is exposed via the service with safe defaults and no secrets."""
    settings = _settings(tmp_path)
    save_state(
        tmp_path / "metrics",
        {
            "install_id": "11111111-2222-3333-4444-555555555555",
            "install_token": _SECRET_TOKEN,
            "token_hash": _SECRET_HASH,
            "install_token_expires_at": "2026-07-01T00:00:00Z",
            "next_batch_seq": 7,
        },
    )

    summary = TelemetryService(settings).summary()
    publish = summary["publish_status"]

    assert set(publish) == set(fs.PUBLIC_FAILURE_STATE_FIELDS)
    # safe, actionable defaults for a legacy install with no recorded mode
    assert publish["status"] == fs.STATUS_UNKNOWN
    assert publish["reason_code"] is None
    assert publish["retry_count"] == 0
    assert publish["next_retry_at"] is None
    assert publish["consent_state"] == fs.CONSENT_UNKNOWN
    assert publish["publish_enabled"] is False
    _assert_summary_has_no_secret(summary)
