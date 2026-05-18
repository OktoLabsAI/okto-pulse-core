from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import metrics as metrics_api
from okto_pulse.core.infra.config import CoreSettings, DEFAULT_METRICS_BEACON_URL
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.sender import TelemetryBeaconSender, get_or_create_install_id, sign_payload
from okto_pulse.core.telemetry.service import TelemetryService
from okto_pulse.core.telemetry.settings import resolve_telemetry_config


def _settings(tmp_path: Path, **overrides) -> CoreSettings:
    values = {"metrics_dir": str(tmp_path / "metrics"), "metrics_mode": ""}
    values.update(overrides)
    return CoreSettings(**values)


def _metrics_client(tmp_path: Path, monkeypatch, **overrides) -> TestClient:
    settings = _settings(tmp_path, **overrides)
    monkeypatch.setattr(metrics_api, "get_settings", lambda: settings)
    app = FastAPI()
    app.include_router(metrics_api.router, prefix="/api/v1")
    app.dependency_overrides[metrics_api.require_user] = lambda: "test-user"
    return TestClient(app)


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


def test_guided_help_event_is_normalized_with_categorical_payload(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    result = service.record_event(
        "guided_help",
        {
            "action": "viewed",
            "tour_surface": "metrics",
            "step_kind": "navigation",
            "status": "success",
            "duration_ms": "42",
        },
    )

    assert result["written"] is True
    assert result["rejected_fields_count"] == 0
    event_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(event_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["schema_version"] == CURRENT_SCHEMA_VERSION
    assert event["event_type"] == "guided_help"
    assert event["payload"] == {
        "action": "viewed",
        "tour_surface": "metrics",
        "step_kind": "navigation",
        "status": "success",
        "duration_ms": 42,
    }


def test_guided_help_drops_forbidden_fields_before_store(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    result = service.record_event(
        "guided_help",
        {
            "action": "step_completed",
            "tour_surface": "specs",
            "step_kind": "feature",
            "status": "success",
            "duration_ms": 125,
            "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
            "spec_id": "secret-spec-id",
            "title": "private roadmap",
            "selector": "[data-tour-id='private']",
            "url": "https://example.test/specs?secret=1",
            "content": "sensitive popover body",
            "token": "secret-token",
            "tour_id": "guided-help-intro",
            "step_id": "metrics-menu",
            "skipped_all": True,
        },
    )

    assert result["written"] is True
    assert result["rejected_fields_count"] >= 10
    event_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(event_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["payload"] == {
        "action": "step_completed",
        "tour_surface": "specs",
        "step_kind": "feature",
        "status": "success",
        "duration_ms": 125,
    }
    serialized = json.dumps(event)
    assert "9ec5f06f" not in serialized
    assert "secret-spec-id" not in serialized
    assert "private roadmap" not in serialized
    assert "data-tour-id" not in serialized
    assert "example.test" not in serialized
    assert "sensitive popover body" not in serialized
    assert "secret-token" not in serialized
    assert "guided-help-intro" not in serialized
    assert "metrics-menu" not in serialized
    assert "skipped_all" not in serialized


def test_guided_help_rejects_unknown_event_type_and_invalid_payload(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    unknown = service.record_event("guided_help_raw", {"action": "viewed"})
    invalid = service.record_event(
        "guided_help",
        {
            "action": "raw_private_action",
            "tour_surface": "metrics",
            "step_kind": "navigation",
            "status": "success",
            "duration_ms": 42,
        },
    )

    assert unknown["written"] is False
    assert invalid["written"] is False
    assert not list((tmp_path / "metrics").glob("events/*.jsonl"))


def test_local_events_endpoint_accepts_safe_guided_help_payload(tmp_path: Path, monkeypatch) -> None:
    client = _metrics_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/metrics/local/events",
        json={
            "event_type": "guided_help",
            "payload": {
                "action": "viewed",
                "tour_surface": "metrics",
                "step_kind": "navigation",
                "status": "success",
                "duration_ms": 64,
                "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
                "selector": "[data-tour-id='private']",
                "path": "D:\\Projects\\private",
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "written": True,
        "rejected_fields_count": 3,
        "schema_version": CURRENT_SCHEMA_VERSION,
    }
    assert "file" not in body
    assert "payload" not in body
    assert "stacktrace" not in json.dumps(body).lower()

    event_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(event_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["event_type"] == "guided_help"
    assert event["payload"] == {
        "action": "viewed",
        "tour_surface": "metrics",
        "step_kind": "navigation",
        "status": "success",
        "duration_ms": 64,
    }
    serialized = json.dumps(event)
    assert "9ec5f06f" not in serialized
    assert "data-tour-id" not in serialized
    assert "D:\\Projects" not in serialized

    summary = client.get("/api/v1/metrics/local/summary").json()
    assert summary["mode"] == "local_only"
    assert summary["summary"]["by_event_type"] == {"guided_help": 1}
    assert summary["summary"]["guided_help_counts"] == {
        "action.viewed": 1,
        "status.success": 1,
        "step_kind.navigation": 1,
        "tour_surface.metrics": 1,
    }
    assert summary["beacon_status"]["enabled"] is False
    assert "payload" not in json.dumps(summary)


def test_local_events_endpoint_disabled_mode_does_not_write(tmp_path: Path, monkeypatch) -> None:
    client = _metrics_client(tmp_path, monkeypatch, metrics_mode="disabled")

    response = client.post(
        "/api/v1/metrics/local/events",
        json={
            "event_type": "guided_help",
            "payload": {
                "action": "viewed",
                "tour_surface": "help",
                "step_kind": "feature",
                "status": "disabled",
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "written": False,
        "rejected_fields_count": 0,
        "schema_version": CURRENT_SCHEMA_VERSION,
    }
    assert not list((tmp_path / "metrics").glob("events/*.jsonl"))


def test_local_events_endpoint_rejects_invalid_event_without_leaking_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = _metrics_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/metrics/local/events",
        json={
            "event_type": "guided_help_raw",
            "payload": {
                "action": "viewed",
                "title": "private roadmap",
                "path": "D:\\Projects\\private",
            },
        },
    )

    assert response.status_code == 400
    body = response.json()
    assert body == {
        "error": "INVALID_EVENT_TYPE",
        "written": False,
        "rejected_fields_count": 3,
        "schema_version": CURRENT_SCHEMA_VERSION,
    }
    serialized = json.dumps(body)
    assert "private roadmap" not in serialized
    assert "D:\\Projects" not in serialized
    assert "stack" not in serialized.lower()

    invalid_payload = client.post(
        "/api/v1/metrics/local/events",
        json={
            "event_type": "guided_help",
            "payload": {
                "action": "raw_private_action",
                "tour_surface": "metrics",
                "step_kind": "navigation",
                "status": "success",
            },
        },
    )
    assert invalid_payload.status_code == 400
    assert invalid_payload.json() == {
        "error": "INVALID_PAYLOAD",
        "written": False,
        "rejected_fields_count": 1,
        "schema_version": CURRENT_SCHEMA_VERSION,
    }
    assert not list((tmp_path / "metrics").glob("events/*.jsonl"))


def test_guided_help_does_not_persist_server_side_progress(tmp_path: Path, monkeypatch) -> None:
    client = _metrics_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/metrics/local/events",
        json={
            "event_type": "guided_help",
            "payload": {
                "action": "viewed",
                "tour_surface": "help",
                "step_kind": "feature",
                "status": "success",
                "tour_id": "guided-help-intro",
                "step_id": "metrics-menu",
                "completed": True,
                "skipped": False,
                "skipped_all": True,
                "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
            },
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "written": True,
        "rejected_fields_count": 6,
        "schema_version": CURRENT_SCHEMA_VERSION,
    }
    event_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(event_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["payload"] == {
        "action": "viewed",
        "tour_surface": "help",
        "step_kind": "feature",
        "status": "success",
    }

    summary = client.get("/api/v1/metrics/local/summary").json()
    assert summary["summary"]["guided_help_counts"] == {
        "action.viewed": 1,
        "status.success": 1,
        "step_kind.feature": 1,
        "tour_surface.help": 1,
    }
    serialized = json.dumps({"event": event, "summary": summary})
    for forbidden in (
        "guided-help-intro",
        "metrics-menu",
        "tour_id",
        "step_id",
        "completed",
        "skipped",
        "skipped_all",
        "9ec5f06f",
    ):
        assert forbidden not in serialized

    api_and_models = "\n".join(
        path.read_text(encoding="utf-8")
        for root in (Path("src/okto_pulse/core/api"), Path("src/okto_pulse/core/models"))
        for path in root.rglob("*.py")
    )
    for forbidden in ("tour_id", "step_id", "guided_help_progress", "GuidedHelpProgress", "TourProgress"):
        assert forbidden not in api_and_models


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


def test_hourly_batch_exports_guided_help_counts_without_identifiers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    monkeypatch.setenv("OKTO_PULSE_INSTALL_ID_PATH", str(tmp_path / "install_id"))
    service = TelemetryService(settings)
    service.record_event(
        "guided_help",
        {
            "action": "viewed",
            "tour_surface": "metrics",
            "step_kind": "navigation",
            "status": "success",
            "duration_ms": 125,
            "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
            "selector": "[data-tour-id='private']",
            "url": "https://example.test/specs?secret=1",
            "content": "private popover body",
        },
    )
    service.store().append_event(
        {
            "schema_version": CURRENT_SCHEMA_VERSION,
            "event_type": "guided_help",
            "occurred_at": "2026-05-16T18:00:00Z",
            "payload": {
                "action": "D:\\Projects\\private",
                "tour_surface": "metrics",
                "step_kind": "navigation",
                "status": "success",
                "selector": "[data-tour-id='tampered']",
                "content": "tampered private text",
            },
        }
    )

    batch = TelemetryBeaconSender(settings).hourly_batch()

    assert batch is not None
    metrics = batch["metrics"]
    assert metrics["guided_help_counts"] == {
        "action.viewed": 1,
        "status.success": 2,
        "step_kind.navigation": 2,
        "tour_surface.metrics": 2,
    }
    serialized = json.dumps(batch)
    assert "payload" not in serialized
    assert "9ec5f06f" not in serialized
    assert "data-tour-id" not in serialized
    assert "example.test" not in serialized
    assert "private popover body" not in serialized
    assert "D:\\Projects" not in serialized
    assert "tampered private text" not in serialized


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


def test_metrics_settings_persist_acknowledged_items(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    service = TelemetryService(settings)

    result = service.update_settings(
        mode="local_only",
        source="cli",
        acknowledged_items=["schema", "privacy_policy", "schema"],
    )

    assert result["acknowledged_items"] == ["schema", "privacy_policy"]
    assert service.summary()["consent"]["acknowledged_items"] == ["schema", "privacy_policy"]


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
