from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.telemetry_state_boundary_gate import (
    check_telemetry_state_boundary,
)
from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.service import TelemetryService
from okto_pulse.core.telemetry.settings import (
    LOCAL_ONLY_MIGRATION_NOTICE,
    load_state,
    mark_migration_notice_seen,
    record_consent,
    resolve_telemetry_config,
    save_state,
)
from okto_pulse.core.telemetry.telemetry_state_registry import (
    load_telemetry_state,
    register_telemetry_state_carrier,
    reset_telemetry_state_carrier_for_tests,
)


class _MemoryCarrier:
    def __init__(self) -> None:
        self.state: dict = {}

    def load_state(self, metrics_dir: Path) -> dict:
        return dict(self.state)

    def save_state(self, metrics_dir: Path, state: dict) -> None:
        self.state = dict(state)


def _settings(tmp_path: Path, **overrides) -> CoreSettings:
    values = {"metrics_dir": str(tmp_path / "metrics"), "metrics_mode": ""}
    values.update(overrides)
    return CoreSettings(**values)


def test_state_registry_is_fail_closed_without_carrier(tmp_path: Path) -> None:
    reset_telemetry_state_carrier_for_tests()
    with pytest.raises(RuntimeError, match="No TelemetryStateCarrier registered"):
        load_telemetry_state(tmp_path / "metrics")

    carrier = _MemoryCarrier()
    carrier.state = {"mode": "disabled", "unknown": {"kept": True}}
    register_telemetry_state_carrier(carrier)
    assert load_telemetry_state(tmp_path / "metrics") == {
        "mode": "disabled",
        "unknown": {"kept": True},
    }


def test_consent_and_migration_notice_preserve_full_dict_carrier(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    metrics_dir = tmp_path / "metrics"
    original = {
        "mode": "disabled",
        "source": "settings_ui",
        "history": [{"mode": "disabled", "changed_at": f"t{i}"} for i in range(55)],
        "migration_notices": {LOCAL_ONLY_MIGRATION_NOTICE: {"seen": False}},
        "watermark": {"cursor": "abc"},
        "failure_state": {"status": "degraded", "retry_count": 2},
        "install_token": "SECRET-TOKEN",
        "install_token_expires_at": "2026-07-01T00:00:00Z",
        "last_handshake_at": "2026-06-01T10:00:00Z",
        "last_send_at": "2026-06-01T10:01:00Z",
        "circuit_open_until": "2026-06-01T10:15:00Z",
        "schema_status": "current",
        "unknown_block": {"nested": ["must", "survive"]},
    }
    save_state(metrics_dir, original)

    record_consent(
        settings,
        mode="anonymous_beacon",
        source="settings_ui",
        policy_version="2026-05-11",
        schema_version=CURRENT_SCHEMA_VERSION,
        acknowledged_items=["privacy", "schema"],
    )
    mark_migration_notice_seen(settings, notice_key=LOCAL_ONLY_MIGRATION_NOTICE)
    reloaded = load_state(metrics_dir)

    assert reloaded["mode"] == "anonymous_beacon"
    assert len(reloaded["history"]) == 50
    assert reloaded["history"][-1]["acknowledged_items"] == ["privacy", "schema"]
    assert reloaded["migration_notices"][LOCAL_ONLY_MIGRATION_NOTICE]["seen"] is True
    for key in (
        "watermark",
        "failure_state",
        "install_token",
        "install_token_expires_at",
        "last_handshake_at",
        "last_send_at",
        "circuit_open_until",
        "schema_status",
        "unknown_block",
    ):
        assert reloaded[key] == original[key]


def test_resolve_telemetry_config_accepts_injected_state_snapshot(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    save_state(tmp_path / "metrics", {"mode": "disabled"})

    cfg = resolve_telemetry_config(
        settings,
        state_snapshot={
            "mode": "local_only",
            "migration_notices": {LOCAL_ONLY_MIGRATION_NOTICE: {"seen": False}},
        },
    )

    assert cfg.source == "persisted_consent"
    assert cfg.mode == "disabled"
    assert cfg.normalized_from == "local_only"
    assert cfg.migration_notice is not None
    assert cfg.migration_notice["pending"] is True


def test_record_event_schema_reject_writes_through_carrier_and_preserves_state(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    metrics_dir = tmp_path / "metrics"
    save_state(
        metrics_dir,
        {
            "mode": "anonymous_beacon",
            "policy_version": "2026-05-11",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "install_token": "SECRET-TOKEN",
            "token_hash": "SECRET-HASH",
            "watermark": {"cursor": "w1"},
            "failure_state": {"status": "healthy"},
            "last_handshake_at": "2026-06-01T10:00:00Z",
            "last_send_at": "2026-06-01T10:01:00Z",
            "circuit_open_until": "2026-06-01T10:15:00Z",
            "schema_status": "current",
            "unknown_block": {"kept": True},
        },
    )

    result = TelemetryService(settings).record_event("not_a_known_event", {})
    reloaded = load_state(metrics_dir)

    assert result["written"] is False
    assert result["rejected_fields_count"] == 1
    assert reloaded["schema_reject_count"] == 1
    assert reloaded["install_token"] == "SECRET-TOKEN"
    assert reloaded["token_hash"] == "SECRET-HASH"
    assert reloaded["watermark"] == {"cursor": "w1"}
    assert reloaded["failure_state"] == {"status": "healthy"}
    assert reloaded["last_handshake_at"] == "2026-06-01T10:00:00Z"
    assert reloaded["last_send_at"] == "2026-06-01T10:01:00Z"
    assert reloaded["circuit_open_until"] == "2026-06-01T10:15:00Z"
    assert reloaded["schema_status"] == "current"
    assert reloaded["unknown_block"] == {"kept": True}


def test_telemetry_state_boundary_gate_passes_current_core() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
    report = check_telemetry_state_boundary(root)
    assert report.ok, report.violations


def test_telemetry_state_boundary_gate_catches_direct_state_file_io(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"
    target = root / "telemetry" / "settings.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        "from pathlib import Path\n"
        "def save(metrics_dir: Path):\n"
        "    path = metrics_dir / 'state.json'\n"
        "    path.write_text('{}', encoding='utf-8')\n",
        encoding="utf-8",
    )

    report = check_telemetry_state_boundary(root)

    assert not report.ok
    assert any(v.reason == "direct_state_file_io:write_text" for v in report.violations)


def test_public_surfaces_do_not_emit_install_token_or_hash(tmp_path: Path) -> None:
    class _NullStore:
        def append_event(self, event): ...
        def append_sent(self, record, *, failed=False): ...
        def append_snapshot(self, record): ...
        def confirmed_event_ids(self): return set()
        def iter_events(self, *, since=None): return ()
        def summarize(self, *, window_days=30): return {}
        def prune_old(self, *, now=None): return {}
        def export_local(self, output_path=None): ...
        def purge_local(self): return {}

    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )

    reset_telemetry_event_store_factory_for_tests()
    register_telemetry_event_store_factory(lambda base, retention_days: _NullStore())
    try:
        settings = _settings(tmp_path)
        save_state(
            tmp_path / "metrics",
            {
                "mode": "anonymous_beacon",
                "schema_version": CURRENT_SCHEMA_VERSION,
                "install_token": "SECRET-TOKEN",
                "token_hash": "SECRET-HASH",
                "install_token_expires_at": "2026-07-01T00:00:00Z",
                "last_handshake_at": "2026-06-01T10:00:00Z",
                "last_send_at": "2026-06-01T10:01:00Z",
                "circuit_open_until": "2026-06-01T10:15:00Z",
                "schema_status": "current",
            },
        )

        summary = TelemetryService(settings).summary()
    finally:
        reset_telemetry_event_store_factory_for_tests()

    blob = json.dumps(summary, default=str)
    assert "SECRET-TOKEN" not in blob
    assert "SECRET-HASH" not in blob
    assert "install_token" not in blob
    assert "token_hash" not in blob
    assert summary["beacon_status"] == {
        "enabled": True,
        "last_handshake_at": "2026-06-01T10:00:00Z",
        "last_send_at": "2026-06-01T10:01:00Z",
        "circuit_open_until": "2026-06-01T10:15:00Z",
        "schema_status": "current",
    }
