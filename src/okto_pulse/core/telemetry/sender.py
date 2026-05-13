"""Anonymous beacon handshake, aggregation and signed usage sender."""

from __future__ import annotations

import hashlib
import hmac
import os
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry.product import ProductTelemetryAggregator
from okto_pulse.core.telemetry.schema import canonical_json, now_utc
from okto_pulse.core.telemetry.settings import (
    resolve_telemetry_config,
    save_state,
)
from okto_pulse.core.telemetry.store import LocalTelemetryStore, parse_iso


def install_id_path(settings: CoreSettings) -> Path:
    override = os.environ.get("OKTO_PULSE_INSTALL_ID_PATH")
    if override:
        return Path(override).expanduser().resolve()
    docker_path = Path("/data/install_id")
    if docker_path.parent.exists():
        return docker_path
    return resolve_telemetry_config(settings).metrics_dir / "install_id"


def get_or_create_install_id(settings: CoreSettings) -> str:
    path = install_id_path(settings)
    try:
        existing = path.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    except OSError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    value = str(uuid.uuid4())
    path.write_text(value, encoding="utf-8")
    return value


def sign_payload(secret: str, timestamp: str, nonce: str, batch_seq: int, payload: dict[str, Any]) -> str:
    message = f"{timestamp}.{nonce}.{batch_seq}.{canonical_json(payload)}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


class TelemetryBeaconSender:
    def __init__(self, settings: CoreSettings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or requests.Session()

    def _store(self) -> LocalTelemetryStore:
        cfg = resolve_telemetry_config(self.settings)
        return LocalTelemetryStore(cfg.metrics_dir, cfg.retention_days)

    def handshake(self) -> dict[str, Any] | None:
        cfg = resolve_telemetry_config(self.settings)
        if cfg.mode != "anonymous_beacon":
            return None
        state = dict(cfg.state)
        payload = {
            "install_id": get_or_create_install_id(self.settings),
            "runtime": {
                "deployment": "docker" if Path("/data").exists() else "pypi",
                "python_version": f"{os.sys.version_info.major}.{os.sys.version_info.minor}",
                "os_family": os.name,
            },
            "app_version": getattr(self.settings, "app_version", "0.0.0+local"),
            "platform_arch": os.uname().machine if hasattr(os, "uname") else "unknown",
            "schema_version": cfg.schema_version,
        }
        try:
            resp = self.session.post(
                f"{cfg.beacon_url}/v1/handshake",
                json=payload,
                timeout=5,
            )
        except requests.RequestException:
            self._open_circuit(state, cfg, "HANDSHAKE_NETWORK")
            return None
        if resp.status_code in {410, 426}:
            state["mode"] = "local_only"
            state["schema_status"] = "gone" if resp.status_code == 410 else "sunset"
            save_state(cfg.metrics_dir, state)
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            self._open_circuit(state, cfg, f"HANDSHAKE_{resp.status_code}")
            return None
        resp.raise_for_status()
        data = resp.json()
        state.update(
            {
                "install_token": data["install_token"],
                "install_token_expires_at": (
                    datetime.now(timezone.utc)
                    + timedelta(seconds=int(data.get("token_ttl_seconds", 2592000)))
                ).isoformat().replace("+00:00", "Z"),
                "accepted_schema_version": data.get("accepted_schema_version", cfg.schema_version),
                "last_handshake_at": now_utc(),
                "limits": data.get("limits") or {},
            }
        )
        save_state(cfg.metrics_dir, state)
        return data

    def hourly_batch(self) -> dict[str, Any] | None:
        cfg = resolve_telemetry_config(self.settings)
        store = self._store()
        buckets: dict[str, Counter[str]] = defaultdict(Counter)
        duration_buckets: Counter[str] = Counter()
        error_class_counts: Counter[str] = Counter()
        for event in store.iter_events():
            occurred = parse_iso(str(event.get("occurred_at", "")))
            if not occurred:
                continue
            bucket = occurred.replace(minute=0, second=0, microsecond=0)
            key = bucket.isoformat().replace("+00:00", "Z")
            event_type = str(event.get("event_type", "unknown"))
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            label = str(
                payload.get("command")
                or payload.get("route_template")
                or payload.get("tool_name")
                or payload.get("operation")
                or payload.get("action")
                or payload.get("phase")
                or "unknown"
            )
            buckets[f"{event_type}:{key}"][label] += 1
            if "duration_ms" in payload:
                try:
                    ms = int(payload["duration_ms"])
                    duration_buckets["lt_100ms" if ms < 100 else "lt_1s" if ms < 1000 else "gte_1s"] += 1
                except (TypeError, ValueError):
                    pass
            if payload.get("error_class"):
                error_class_counts[str(payload["error_class"])] += 1
        product_metrics: dict[str, Any] = {}
        try:
            product_metrics = ProductTelemetryAggregator(self.settings, cfg.metrics_dir).aggregate()
        except Exception:
            product_metrics = {}
        if not buckets and not product_metrics:
            return None
        if buckets:
            first_key = sorted(buckets)[0]
            bucket_start = first_key.split(":", 1)[1]
        else:
            bucket_start = (
                datetime.now(timezone.utc)
                .replace(minute=0, second=0, microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        metrics: dict[str, Any] = {
            "cli_counts": {},
            "http_route_template_counts": {},
            "mcp_tool_counts": {},
            "kg_operation_counts": {},
            "duration_buckets": dict(duration_buckets),
            "error_class_counts": dict(error_class_counts),
        }
        metrics.update(product_metrics)
        for key, counts in buckets.items():
            event_type, _ = key.split(":", 1)
            if event_type == "cli":
                metrics["cli_counts"].update(counts)
            elif event_type == "http":
                metrics["http_route_template_counts"].update(counts)
            elif event_type == "mcp":
                metrics["mcp_tool_counts"].update(counts)
            elif event_type == "kg":
                metrics["kg_operation_counts"].update(counts)
        return {
            "schema_version": cfg.schema_version,
            "install_id": get_or_create_install_id(self.settings),
            "bucket_start": bucket_start,
            "bucket_duration_seconds": 3600,
            "metrics": metrics,
        }

    def send_once(self) -> dict[str, Any]:
        cfg = resolve_telemetry_config(self.settings)
        if cfg.mode != "anonymous_beacon":
            return {"sent": False, "reason": "not_enabled"}
        state = dict(cfg.state)
        circuit_until = parse_iso(str(state.get("circuit_open_until", "")))
        if circuit_until and circuit_until > datetime.now(timezone.utc):
            return {"sent": False, "reason": "circuit_open"}
        if not state.get("install_token"):
            self.handshake()
            cfg = resolve_telemetry_config(self.settings)
            state = dict(cfg.state)
        token = state.get("install_token")
        if not token:
            return {"sent": False, "reason": "missing_token"}
        batch = self.hourly_batch()
        if not batch:
            return {"sent": False, "reason": "empty"}
        batch_seq = int(state.get("next_batch_seq") or 1)
        timestamp = str(int(datetime.now(timezone.utc).timestamp()))
        nonce = str(uuid.uuid4())
        signature = sign_payload(str(token), timestamp, nonce, batch_seq, batch)
        headers = {
            "x-okto-signature": signature,
            "x-okto-timestamp": timestamp,
            "x-okto-nonce": nonce,
            "x-okto-batch-seq": str(batch_seq),
        }
        try:
            resp = self.session.post(
                f"{cfg.beacon_url}/v1/usage",
                json=batch,
                headers=headers,
                timeout=5,
            )
        except requests.RequestException:
            self._open_circuit(state, cfg, "USAGE_NETWORK")
            return {"sent": False, "reason": "network"}
        if resp.status_code in {410, 426}:
            state["mode"] = "local_only"
            state["schema_status"] = "gone" if resp.status_code == 410 else "sunset"
            save_state(cfg.metrics_dir, state)
            return {"sent": False, "reason": "schema_incompatible"}
        if resp.status_code == 429 or resp.status_code >= 500:
            self._open_circuit(state, cfg, f"USAGE_{resp.status_code}")
            return {"sent": False, "reason": "retryable"}
        resp.raise_for_status()
        state["last_send_at"] = now_utc()
        state["next_batch_seq"] = batch_seq + 1
        save_state(cfg.metrics_dir, state)
        self._store().append_sent(
            {
                "sent_at": state["last_send_at"],
                "batch_seq": batch_seq,
                "payload": batch,
                "response_status": resp.status_code,
            }
        )
        return {"sent": True, "batch_seq": batch_seq}

    def _open_circuit(self, state: dict[str, Any], cfg, code: str) -> None:
        state["circuit_open_until"] = (
            datetime.now(timezone.utc) + timedelta(minutes=15)
        ).isoformat().replace("+00:00", "Z")
        state["last_failure_code"] = code
        save_state(cfg.metrics_dir, state)
        self._store().append_sent({"failed_at": now_utc(), "code": code}, failed=True)
