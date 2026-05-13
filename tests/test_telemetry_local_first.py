from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from okto_pulse.core.infra.config import CoreSettings, DEFAULT_METRICS_BEACON_URL
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.sender import TelemetryBeaconSender, get_or_create_install_id, sign_payload
from okto_pulse.core.telemetry.service import TelemetryService
from okto_pulse.core.telemetry.settings import resolve_telemetry_config


def _settings(tmp_path: Path, **overrides) -> CoreSettings:
    values = {"metrics_dir": str(tmp_path / "metrics"), "metrics_mode": ""}
    values.update(overrides)
    return CoreSettings(**values)


def test_fresh_install_resolves_local_only_without_network(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    cfg = resolve_telemetry_config(settings)

    assert cfg.mode == "local_only"
    assert cfg.source == "default"
    assert cfg.metrics_dir == (tmp_path / "metrics").resolve()
    assert cfg.beacon_url == DEFAULT_METRICS_BEACON_URL
    assert cfg.schema_version == CURRENT_SCHEMA_VERSION


def test_disabled_mode_does_not_write_events(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)
    service.update_settings(mode="disabled", source="cli")

    result = service.record_event("cli", {"command": "serve", "exit_code": 0})

    assert result["written"] is False
    assert not list((tmp_path / "metrics").glob("events/*.jsonl"))


def test_allowlist_drops_sensitive_payload_before_store(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    result = service.record_event(
        "http",
        {
            "method": "GET",
            "route_template": "/api/v1/cards/{card_id}",
            "status_code": 200,
            "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
            "title": "secret roadmap",
            "email": "dev@example.com",
            "path": "D:\\Projects\\private",
            "payload": {"raw": True},
        },
    )

    assert result["written"] is True
    assert result["rejected_fields_count"] >= 5
    event_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(event_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["payload"] == {
        "method": "GET",
        "route_template": "/api/v1/cards/{card_id}",
        "status_code": 200,
    }
    serialized = json.dumps(event)
    assert "secret roadmap" not in serialized
    assert "dev@example.com" not in serialized
    assert "9ec5f06f" not in serialized
    assert "D:\\Projects" not in serialized


def test_summary_export_and_purge_are_local_only(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)
    service.record_event("cli", {"command": "status", "exit_code": 0})

    summary = service.summary()
    assert summary["mode"] == "local_only"
    assert summary["summary"]["by_event_type"] == {"cli": 1}
    assert "payload" not in json.dumps(summary)

    exported = service.export_local()
    assert Path(exported["output_path"]).exists()
    purged = service.purge_local()
    assert purged["purged_files"] >= 2
    assert service.summary()["summary"]["event_count"] == 0


def test_install_id_is_stable_and_hmac_is_canonical(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))

    first = get_or_create_install_id(settings)
    second = get_or_create_install_id(settings)

    assert first == second
    payload_a = {"b": 2, "a": 1}
    payload_b = {"a": 1, "b": 2}
    assert sign_payload("token", "1", "nonce", 7, payload_a) == sign_payload(
        "token",
        "1",
        "nonce",
        7,
        payload_b,
    )


def test_sender_does_not_call_network_before_opt_in(tmp_path: Path) -> None:
    class ExplodingSession:
        def post(self, *args, **kwargs):  # pragma: no cover - should not run
            raise AssertionError("network call not allowed")

    settings = _settings(tmp_path)
    sender = TelemetryBeaconSender(settings, session=ExplodingSession())  # type: ignore[arg-type]

    assert sender.handshake() is None
    assert sender.send_once() == {"sent": False, "reason": "not_enabled"}


def test_hourly_batch_keeps_full_iso_bucket_start(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    service = TelemetryService(settings)
    service.record_event(
        "cli",
        {"command": "serve", "exit_code": 0, "duration_ms": 42},
    )

    batch = TelemetryBeaconSender(settings).hourly_batch()

    assert batch is not None
    assert batch["bucket_start"].endswith(":00:00Z")
    assert len(batch["bucket_start"]) == len("2026-05-11T01:00:00Z")


def test_beacon_rejects_legacy_schema_cutover(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    try:
        service.update_settings(
            mode="anonymous_beacon",
            source="cli",
            policy_version="2026-05-11",
            schema_version="1.0.0",
        )
    except ValueError as exc:
        assert str(exc) == "UNSUPPORTED_METRICS_SCHEMA"
    else:  # pragma: no cover
        raise AssertionError("legacy telemetry schema must be rejected")


def test_stale_persisted_beacon_consent_falls_back_to_local_only(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir(parents=True)
    (metrics_dir / "state.json").write_text(
        json.dumps(
            {
                "mode": "anonymous_beacon",
                "source": "settings_ui",
                "policy_version": "2026-05-11",
                "schema_version": "1.0.0",
            }
        ),
        encoding="utf-8",
    )

    service = TelemetryService(settings)
    summary = service.summary()

    assert summary["mode"] == "local_only"
    assert summary["source"] == "stale_persisted_consent"
    assert summary["beacon_status"]["enabled"] is False
    assert summary["beacon_status"]["schema_status"] == "stale_consent"


def test_hourly_batch_adds_product_aggregates_without_identifiers(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "pulse.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE domain_events (event_type TEXT, payload_json JSON);
        CREATE TABLE specs (
          id TEXT, status TEXT, ideation_id TEXT, refinement_id TEXT,
          test_scenarios JSON, decisions JSON
        );
        CREATE TABLE story_ideation_links (ideation_id TEXT);
        CREATE TABLE cards (status TEXT, card_type TEXT);
        CREATE TABLE sprints (status TEXT);
        CREATE TABLE architecture_designs (id TEXT);
        """
    )
    conn.execute(
        "INSERT INTO domain_events VALUES (?, ?)",
        ("spec.created", json.dumps({"spec_id": "secret-spec-id", "source": "derived_ideation"})),
    )
    conn.execute(
        "INSERT INTO domain_events VALUES (?, ?)",
        ("card.created", json.dumps({"card_id": "secret-card-id", "card_type": "bug"})),
    )
    conn.execute(
        "INSERT INTO specs VALUES (?, ?, ?, ?, ?, ?)",
        ("secret-spec-id", "done", "secret-ideation-id", None, json.dumps([{"id": "test-1"}]), json.dumps([{"id": "decision-1"}])),
    )
    conn.execute("INSERT INTO story_ideation_links VALUES (?)", ("secret-ideation-id",))
    conn.execute("INSERT INTO cards VALUES (?, ?)", ("done", "bug"))
    conn.execute("INSERT INTO sprints VALUES (?)", ("closed",))
    conn.execute("INSERT INTO architecture_designs VALUES (?)", ("secret-design-id",))
    conn.commit()
    conn.close()
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    settings = _settings(
        tmp_path,
        metrics_mode="anonymous_beacon",
        database_url=f"sqlite+aiosqlite:///{db_path}",
    )

    batch = TelemetryBeaconSender(settings).hourly_batch()

    assert batch is not None
    assert batch["schema_version"] == CURRENT_SCHEMA_VERSION
    metrics = batch["metrics"]
    assert metrics["product_flow_origin_counts"]["current.story"] == 1
    assert metrics["product_flow_completion_counts"]["story"] == 1
    assert metrics["product_work_item_type_counts"]["bug"] == 1
    assert metrics["product_quality_signal_counts"]["test_scenarios_total"] == 1
    serialized = json.dumps(batch)
    assert "secret-" not in serialized


def test_hourly_batch_fail_open_when_product_aggregation_fails(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    service = TelemetryService(settings)
    service.record_event("cli", {"command": "serve", "exit_code": 0})

    def boom(self):
        raise RuntimeError("local db busy")

    monkeypatch.setattr("okto_pulse.core.telemetry.sender.ProductTelemetryAggregator.aggregate", boom)

    batch = TelemetryBeaconSender(settings).hourly_batch()

    assert batch is not None
    assert batch["metrics"]["cli_counts"] == {"serve": 1}
    assert "product_flow_origin_counts" not in batch["metrics"]
