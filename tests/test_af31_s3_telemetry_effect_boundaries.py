from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry.settings import (
    resolve_telemetry_config,
    state_ref_for,
)


class _MemoryStateCarrier:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}

    def load_state(self, state_ref: str) -> dict[str, Any]:
        return dict(self.state)

    def save_state(self, state_ref: str, state: dict[str, Any]) -> None:
        self.state = dict(state)


class _MemoryTelemetryStore:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.sent: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []

    def append_event(self, event: dict[str, Any]) -> str:
        self.events.append(dict(event))
        return "memory://events"

    def append_sent(self, record: dict[str, Any], *, failed: bool = False) -> str:
        self.sent.append({"failed": failed, **dict(record)})
        return "memory://sent"

    def append_snapshot(self, record: dict[str, Any]) -> str:
        self.snapshots.append(dict(record))
        return "memory://snapshots"

    def confirmed_event_ids(self) -> set[str]:
        return set()

    def iter_events(self, *, since: datetime | None = None):
        yield from self.events

    def summarize(self, *, window_days: int = 30) -> dict[str, Any]:
        return {
            "event_count": len(self.events),
            "by_event_type": {
                event["event_type"]: sum(
                    1 for e in self.events if e["event_type"] == event["event_type"]
                )
                for event in self.events
            },
            "by_day": {},
            "guided_help_counts": {},
            "files_count": 0,
        }

    def prune_old(self, *, now: datetime | None = None) -> dict[str, int]:
        return {}

    def export_events(self, destination_ref: str | None = None) -> str:
        return destination_ref or "memory://export"

    def purge_events(self) -> dict[str, int]:
        count = len(self.events)
        self.events.clear()
        return {"purged_files": count}


def _core_src(relative: str) -> str:
    return (
        Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / relative
    ).read_text(encoding="utf-8")


def test_core_telemetry_has_no_local_metrics_or_beacon_defaults() -> None:
    settings_src = _core_src("telemetry/settings.py")
    effect_config_src = _core_src("telemetry/effect_config_registry.py")
    config_src = _core_src("infra/config.py")

    assert "Path.home" not in settings_src
    assert "Path.home" not in effect_config_src
    assert "OKTO_PULSE_HOME" not in settings_src
    assert "OKTO_PULSE_HOME" not in effect_config_src
    assert '.okto-pulse" / "metrics' not in settings_src
    assert '.okto-pulse" / "metrics' not in effect_config_src
    assert "metrics.oktolabs.ai" not in config_src
    assert "metrics.oktolabs.ai" not in settings_src
    assert "metrics.oktolabs.ai" not in effect_config_src


def test_state_ref_requires_explicit_setting_or_provider(caplog) -> None:
    from okto_pulse.core.telemetry.effect_config_registry import (
        reset_telemetry_effect_config_provider_for_tests,
    )

    reset_telemetry_effect_config_provider_for_tests()
    settings = SimpleNamespace(telemetry_state_ref="")

    with caplog.at_level("ERROR", logger="okto_pulse.telemetry.effect_config"):
        with pytest.raises(
            RuntimeError, match="No telemetry state reference configured"
        ):
            state_ref_for(settings)

    assert any(
        getattr(record, "metric_name", None)
        == "telemetry_effect_config_no_state_ref_total"
        and getattr(record, "reason", None) == "no_state_ref_provider"
        and getattr(record, "outcome", None) == "fail_closed"
        for record in caplog.records
    )


def test_telemetry_effect_config_provider_supplies_runtime_defaults(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.telemetry.effect_config_registry import (
        register_telemetry_effect_config_provider,
        reset_telemetry_effect_config_provider_for_tests,
    )

    class _Provider:
        def state_ref(self, settings: Any) -> str:
            return "memory://provider-state"

        def delivery_target(self, settings: Any) -> str:
            return "edition://delivery-target"

    reset_telemetry_effect_config_provider_for_tests()
    register_telemetry_effect_config_provider(_Provider())
    try:
        cfg = resolve_telemetry_config(
            CoreSettings(metrics_dir="", metrics_beacon_url=""),
            state_snapshot={"mode": "disabled"},
        )
    finally:
        reset_telemetry_effect_config_provider_for_tests()

    assert cfg.state_ref == "memory://provider-state"
    assert cfg.delivery_target == "edition://delivery-target"


def test_core_telemetry_service_runs_with_in_memory_ports_without_community(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )
    from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
    from okto_pulse.core.telemetry.service import TelemetryService
    from okto_pulse.core.telemetry.effect_config_registry import (
        register_telemetry_effect_config_provider,
        reset_telemetry_effect_config_provider_for_tests,
    )
    from okto_pulse.core.telemetry.telemetry_state_registry import (
        register_telemetry_state_carrier,
        reset_telemetry_state_carrier_for_tests,
    )

    carrier = _MemoryStateCarrier()
    store = _MemoryTelemetryStore()
    reset_telemetry_state_carrier_for_tests()
    reset_telemetry_event_store_factory_for_tests()
    reset_telemetry_effect_config_provider_for_tests()
    register_telemetry_state_carrier(carrier)
    register_telemetry_event_store_factory(lambda base, retention_days: store)
    register_telemetry_effect_config_provider(
        SimpleNamespace(
            state_ref=lambda settings: "memory://app-a",
            delivery_target=lambda settings: "",
        )
    )
    try:
        settings = CoreSettings(
            metrics_dir=str(tmp_path / "metrics"),
            metrics_beacon_url="",
            metrics_mode="anonymous_beacon",
        )
        service = TelemetryService(settings)
        service.update_settings(
            mode="anonymous_beacon",
            source="cli",
            policy_version="2026-05-11",
            schema_version=CURRENT_SCHEMA_VERSION,
            acknowledged_items=["schema"],
        )
        result = service.record_event(
            "http",
            {
                "method": "GET",
                "route_template": "/api/v1/cards/{card_id}",
                "status_code": 200,
                "board_id": "secret-board",
            },
        )
    finally:
        reset_telemetry_event_store_factory_for_tests()
        reset_telemetry_state_carrier_for_tests()
        reset_telemetry_effect_config_provider_for_tests()

    assert result["written"] is True
    assert result["rejected_fields_count"] == 1
    assert len(store.events) == 1
    assert store.events[0]["event_type"] == "http"
    assert store.events[0]["payload"] == {
        "method": "GET",
        "route_template": "/api/v1/cards/{card_id}",
        "status_code": 200,
    }
