"""Anonymous beacon handshake, aggregation and signed usage sender."""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import random
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
from okto_pulse.core.telemetry import failure_state as fs
from okto_pulse.core.telemetry.store import LocalTelemetryStore, add_guided_help_counts, parse_iso

logger = logging.getLogger("okto_pulse.telemetry.sender")

# R1-B: preventive token refresh + jittered exponential backoff for transient
# failures. Time and jitter go through small indirections so tests can simulate
# the clock and make backoff deterministic.
DEFAULT_TOKEN_REFRESH_MARGIN_HOURS = 24
_BACKOFF_BASE_SECONDS = 30
_BACKOFF_CAP_SECONDS = 3600
_BACKOFF_JITTER_RATIO = 0.5


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _backoff_jitter() -> float:
    """Jitter fraction in [0, _BACKOFF_JITTER_RATIO]; patched in tests."""
    return random.random() * _BACKOFF_JITTER_RATIO


def _backoff_delay_seconds(retry_count: int) -> float:
    """Exponential backoff base*2^(n-1), capped, with additive jitter."""
    steps = min(20, max(0, retry_count - 1))
    base = min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2**steps))
    return min(_BACKOFF_CAP_SECONDS, base * (1.0 + _backoff_jitter()))


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


def _log_runtime_skip(*, reason: str) -> None:
    logger.info(
        "metrics.runtime_skip",
        extra={
            "metric_name": "metrics_runtime_skip_total",
            "component": "beacon_sender",
            "outcome": "skipped",
            "reason": reason,
        },
    )


def _log_beacon_outcome(*, reason: str, outcome: str = "skipped") -> None:
    logger.info(
        "metrics.beacon_outcome",
        extra={
            "metric_name": "metrics_beacon_outcome_total",
            "outcome": outcome,
            "reason": reason,
        },
    )


class TelemetryBeaconSender:
    def __init__(self, settings: CoreSettings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or requests.Session()

    def _store(self) -> LocalTelemetryStore:
        cfg = resolve_telemetry_config(self.settings)
        return LocalTelemetryStore(cfg.metrics_dir, cfg.retention_days)

    def handshake(self, *, open_circuit_on_failure: bool = True) -> dict[str, Any] | None:
        cfg = resolve_telemetry_config(self.settings)
        if cfg.mode != "anonymous_beacon":
            _log_runtime_skip(reason="disabled")
            _log_beacon_outcome(reason="disabled")
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
            if open_circuit_on_failure:
                self._open_circuit(state, cfg, "HANDSHAKE_NETWORK")
            _log_beacon_outcome(reason="transport_failed")
            return None
        if resp.status_code in {410, 426}:
            state["mode"] = "disabled"
            state["schema_status"] = "gone" if resp.status_code == 410 else "sunset"
            save_state(cfg.metrics_dir, state)
            _log_beacon_outcome(reason="consent_stale")
            return None
        if resp.status_code == 429 or resp.status_code >= 500:
            if open_circuit_on_failure:
                self._open_circuit(state, cfg, f"HANDSHAKE_{resp.status_code}", http_status=resp.status_code)
            _log_beacon_outcome(reason="transport_failed")
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
        if cfg.mode != "anonymous_beacon":
            _log_runtime_skip(reason="disabled")
            _log_beacon_outcome(reason="disabled")
            return None
        store = self._store()
        buckets: dict[str, Counter[str]] = defaultdict(Counter)
        bucket_starts: list[str] = []
        guided_help_counts: Counter[str] = Counter()
        duration_buckets: Counter[str] = Counter()
        error_class_counts: Counter[str] = Counter()
        for event in store.iter_events():
            occurred = parse_iso(str(event.get("occurred_at", "")))
            if not occurred:
                continue
            bucket = occurred.replace(minute=0, second=0, microsecond=0)
            key = bucket.isoformat().replace("+00:00", "Z")
            bucket_starts.append(key)
            event_type = str(event.get("event_type", "unknown"))
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if event_type == "guided_help":
                add_guided_help_counts(guided_help_counts, payload)
                continue
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
        if not buckets and not product_metrics and not guided_help_counts:
            return None
        if bucket_starts:
            bucket_start = sorted(bucket_starts)[0]
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
        if guided_help_counts:
            metrics["guided_help_counts"] = dict(sorted(guided_help_counts.items()))
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
            _log_runtime_skip(reason="disabled")
            _log_beacon_outcome(reason="disabled")
            return {"sent": False, "reason": "not_enabled"}
        state = dict(cfg.state)
        circuit_until = parse_iso(str(state.get("circuit_open_until", "")))
        if circuit_until and circuit_until > datetime.now(timezone.utc):
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "circuit_open"}
        refresh_status: str | None = None
        refresh_next_retry_at: str | None = None
        if not state.get("install_token"):
            self.handshake()
            cfg = resolve_telemetry_config(self.settings)
            state = dict(cfg.state)
        else:
            # R1-B: preventive refresh when the current token is within the
            # configurable expiry margin (default 24h) BEFORE POST /v1/usage.
            expires_at = parse_iso(str(state.get("install_token_expires_at") or ""))
            margin = timedelta(
                hours=int(
                    getattr(
                        self.settings,
                        "metrics_token_refresh_margin_hours",
                        DEFAULT_TOKEN_REFRESH_MARGIN_HOURS,
                    )
                )
            )
            if expires_at and (expires_at - _utcnow()) <= margin:
                refreshed = self.handshake(open_circuit_on_failure=False)
                cfg = resolve_telemetry_config(self.settings)
                state = dict(cfg.state)
                if refreshed is None:
                    if expires_at <= _utcnow():
                        # token already expired and refresh failed -> cannot publish
                        self._open_circuit(state, cfg, "REFRESH_FAILED")
                        _log_beacon_outcome(reason="transport_failed")
                        return {"sent": False, "reason": "refresh_failed"}
                    # AC ac_7dc06c55: refresh failed by 5xx/transport but the
                    # current token is still valid -> degrade and publish with it,
                    # recording the refresh retry without blocking the publish path.
                    refresh_status = "degraded"
                    refresh_next_retry_at = _iso(_utcnow() + timedelta(seconds=_backoff_delay_seconds(1)))
                    logger.info(
                        "metrics.token_refresh",
                        extra={
                            "metric_name": "metrics_token_refresh_total",
                            "outcome": "degraded",
                            "reason": "refresh_failed_token_valid",
                        },
                    )
                else:
                    refresh_status = "refreshed"
        token = state.get("install_token")
        if not token:
            _log_beacon_outcome(reason="ack_missing")
            return {"sent": False, "reason": "missing_token"}
        batch = self.hourly_batch()
        if not batch:
            return {"sent": False, "reason": "empty"}
        batch_seq = int(state.get("next_batch_seq") or 1)
        try:
            resp = self._sign_and_post_usage(cfg, token, batch, batch_seq)
        except requests.RequestException:
            self._open_circuit(state, cfg, "USAGE_NETWORK")
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "network"}
        outcome = self._handle_usage_response(
            resp, state, cfg, batch=batch, batch_seq=batch_seq, allow_rehandshake=True
        )
        if outcome.get("sent") and refresh_status is not None:
            outcome["refresh"] = refresh_status
            if refresh_next_retry_at is not None:
                outcome["refresh_next_retry_at"] = refresh_next_retry_at
        return outcome

    def _open_circuit(
        self, state: dict[str, Any], cfg, code: str, *, http_status: int | None = None, status: str = fs.STATUS_DEGRADED
    ) -> None:
        # R1-B: jittered exponential backoff recorded in the R1-A failure-state
        # schema. circuit_open_until/last_failure_code stay in sync for the
        # existing send_once gate and backward compatibility. R1-C passes
        # status=FATAL for integrity failures (INVALID_SIGNATURE).
        current = fs.read_failure_state(state)
        retry_count = current.retry_count + 1
        now = _utcnow()
        next_retry_at = _iso(now + timedelta(seconds=_backoff_delay_seconds(retry_count)))
        updated = fs.merge(
            current,
            status=status,
            reason_code=code,
            http_status=http_status,
            last_failure_at=_iso(now),
            next_retry_at=next_retry_at,
            retry_count=retry_count,
            recovered_at=None,
        )
        state[fs.FAILURE_STATE_KEY] = updated.to_public_dict()
        state["circuit_open_until"] = next_retry_at
        state["last_failure_code"] = code
        save_state(cfg.metrics_dir, state)
        self._store().append_sent({"failed_at": now_utc(), "code": code}, failed=True)

    def _record_success(self, state: dict[str, Any], cfg, *, batch_seq: int) -> None:
        # R1-B: record a successful publish in the failure-state schema, marking
        # recovery when the previous state was failing, and clear the legacy
        # circuit gate. next_batch_seq/send-time seq stays here (R1 scope); event
        # watermark/delta is R3.
        current = fs.read_failure_state(state)
        now_iso = now_utc()
        was_failing = current.status in (fs.STATUS_DEGRADED, fs.STATUS_FATAL) or current.retry_count > 0
        updated = fs.merge(
            current,
            status=fs.STATUS_OK,
            reason_code=None,
            http_status=None,
            last_success_at=now_iso,
            next_retry_at=None,
            retry_count=0,
            recovered_at=now_iso if was_failing else current.recovered_at,
        )
        state[fs.FAILURE_STATE_KEY] = updated.to_public_dict()
        state["last_send_at"] = now_iso
        state["next_batch_seq"] = batch_seq + 1
        state.pop("circuit_open_until", None)
        state.pop("last_failure_code", None)
        save_state(cfg.metrics_dir, state)

    def _sign_and_post_usage(self, cfg, token, batch: dict[str, Any], batch_seq: int):
        timestamp = str(int(_utcnow().timestamp()))
        nonce = str(uuid.uuid4())
        signature = sign_payload(str(token), timestamp, nonce, batch_seq, batch)
        body = canonical_json(batch).encode("utf-8")
        headers = {
            "content-type": "application/json",
            "x-okto-signature": signature,
            "x-okto-timestamp": timestamp,
            "x-okto-nonce": nonce,
            "x-okto-batch-seq": str(batch_seq),
        }
        return self.session.post(f"{cfg.beacon_url}/v1/usage", data=body, headers=headers, timeout=5)

    @staticmethod
    def _response_code(resp) -> str | None:
        try:
            body = resp.json()
        except Exception:
            return None
        return body.get("code") if isinstance(body, dict) else None

    @staticmethod
    def _rehandshake_allowed(cfg, state: dict[str, Any]) -> bool:
        # R1-C / FR fr_07d36948: a re-handshake re-registers the install, so it is
        # only allowed while consent is valid — beacon opted-in AND a recorded
        # policy acknowledgement (policy_ack) present in local state.
        return cfg.mode == "anonymous_beacon" and bool(state.get("policy_version"))

    def _handle_usage_response(
        self,
        resp,
        state: dict[str, Any],
        cfg,
        *,
        batch: dict[str, Any],
        batch_seq: int,
        allow_rehandshake: bool,
    ) -> dict[str, Any]:
        if resp.status_code in {410, 426}:
            state["mode"] = "disabled"
            state["schema_status"] = "gone" if resp.status_code == 410 else "sunset"
            save_state(cfg.metrics_dir, state)
            _log_beacon_outcome(reason="consent_stale")
            return {"sent": False, "reason": "schema_incompatible"}
        if resp.status_code in {403, 429} or resp.status_code >= 500:
            self._open_circuit(state, cfg, f"USAGE_{resp.status_code}", http_status=resp.status_code)
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "retryable"}
        if 200 <= resp.status_code < 300:
            self._record_success(state, cfg, batch_seq=batch_seq)
            self._store().append_sent(
                {
                    "sent_at": state["last_send_at"],
                    "batch_seq": batch_seq,
                    "payload": batch,
                    "response_status": resp.status_code,
                }
            )
            _log_beacon_outcome(reason="sent", outcome="sent")
            return {"sent": True, "batch_seq": batch_seq}
        # R1-C: classify the named /v1/usage reason codes (testable, not log parsing).
        code = self._response_code(resp)
        if resp.status_code == 401 and code == "UNKNOWN_INSTALL":
            return self._recover_unknown_install(
                state, cfg, batch=batch, batch_seq=batch_seq, allow_rehandshake=allow_rehandshake
            )
        if resp.status_code == 401 and code == "INVALID_SIGNATURE":
            # Integrity/auth failure: actionable/fatal, never a blind re-handshake loop.
            self._open_circuit(state, cfg, "INVALID_SIGNATURE", http_status=401, status=fs.STATUS_FATAL)
            _log_beacon_outcome(reason="fatal")
            return {"sent": False, "reason": "invalid_signature"}
        if resp.status_code == 409 and code == "DUPLICATE_NONCE_OR_BATCH_SEQ":
            # Idempotent: backend already claimed this batch; advance the send-time
            # seq so we stop replaying it. Event watermark/delta stays in R3.
            self._record_duplicate(state, cfg, batch_seq=batch_seq)
            _log_beacon_outcome(reason="duplicate")
            return {"sent": False, "reason": "duplicate", "batch_seq": batch_seq}
        resp.raise_for_status()
        return {"sent": False, "reason": "unhandled"}

    def _recover_unknown_install(
        self,
        state: dict[str, Any],
        cfg,
        *,
        batch: dict[str, Any],
        batch_seq: int,
        allow_rehandshake: bool,
    ) -> dict[str, Any]:
        if not allow_rehandshake:
            # Already re-handshaked + retried once and STILL unknown: persistent
            # failure, back off without a second re-handshake.
            self._open_circuit(state, cfg, "UNKNOWN_INSTALL", http_status=401)
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "unknown_install_unresolved"}
        if not self._rehandshake_allowed(cfg, state):
            # No valid consent: do NOT re-register; persist an actionable block.
            self._record_blocked(state, cfg, reason_code="UNKNOWN_INSTALL")
            _log_beacon_outcome(reason="consent_blocked")
            return {"sent": False, "reason": "consent_blocked"}
        refreshed = self.handshake(open_circuit_on_failure=False)
        cfg = resolve_telemetry_config(self.settings)
        state = dict(cfg.state)
        token = state.get("install_token")
        if refreshed is None or not token:
            self._open_circuit(state, cfg, "UNKNOWN_INSTALL", http_status=401)
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "rehandshake_failed"}
        try:
            retry = self._sign_and_post_usage(cfg, token, batch, batch_seq)
        except requests.RequestException:
            self._open_circuit(state, cfg, "USAGE_NETWORK")
            _log_beacon_outcome(reason="transport_failed")
            return {"sent": False, "reason": "network"}
        outcome = self._handle_usage_response(
            retry, state, cfg, batch=batch, batch_seq=batch_seq, allow_rehandshake=False
        )
        if outcome.get("sent"):
            outcome["recovered"] = "rehandshake"
        return outcome

    def _record_duplicate(self, state: dict[str, Any], cfg, *, batch_seq: int) -> None:
        current = fs.read_failure_state(state)
        was_failing = current.status in (fs.STATUS_DEGRADED, fs.STATUS_FATAL) or current.retry_count > 0
        updated = fs.merge(
            current,
            status=fs.STATUS_OK,
            reason_code=None,
            http_status=None,
            next_retry_at=None,
            retry_count=0,
            recovered_at=now_utc() if was_failing else current.recovered_at,
        )
        state[fs.FAILURE_STATE_KEY] = updated.to_public_dict()
        state["next_batch_seq"] = batch_seq + 1
        state.pop("circuit_open_until", None)
        state.pop("last_failure_code", None)
        save_state(cfg.metrics_dir, state)

    def _record_blocked(self, state: dict[str, Any], cfg, *, reason_code: str) -> None:
        current = fs.read_failure_state(state)
        now = _utcnow()
        retry_count = current.retry_count + 1
        next_retry_at = _iso(now + timedelta(seconds=_backoff_delay_seconds(retry_count)))
        updated = fs.merge(
            current,
            status=fs.STATUS_BLOCKED,
            reason_code=reason_code,
            http_status=401,
            last_failure_at=_iso(now),
            next_retry_at=next_retry_at,
            retry_count=retry_count,
            recovered_at=None,
            publish_enabled=False,
            consent_state=fs.CONSENT_BLOCKED,
        )
        state[fs.FAILURE_STATE_KEY] = updated.to_public_dict()
        state["circuit_open_until"] = next_retry_at
        state["last_failure_code"] = reason_code
        save_state(cfg.metrics_dir, state)
        self._store().append_sent({"failed_at": now_utc(), "code": reason_code}, failed=True)
