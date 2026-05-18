"""High-level local telemetry service."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry.product import PRODUCT_AGGREGATE_FAMILIES
from okto_pulse.core.telemetry.schema import normalize_event, now_utc
from okto_pulse.core.telemetry.settings import (
    TelemetryMode,
    record_consent,
    resolve_telemetry_config,
    save_state,
)
from okto_pulse.core.telemetry.store import LocalTelemetryStore


class TelemetryService:
    def __init__(self, settings: CoreSettings):
        self.settings = settings

    def config(self):
        return resolve_telemetry_config(self.settings)

    def store(self) -> LocalTelemetryStore:
        cfg = self.config()
        return LocalTelemetryStore(cfg.metrics_dir, cfg.retention_days)

    def record_event(self, event_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = self.config()
        state = dict(cfg.state)
        if cfg.mode == "disabled":
            return {"written": False, "mode": cfg.mode, "rejected_fields_count": 0, "schema_version": cfg.schema_version}
        try:
            event, rejected = normalize_event(
                event_type,
                payload,
                app_version=getattr(self.settings, "app_version", "0.0.0+local"),
                schema_version=cfg.schema_version,
            )
        except Exception:
            state["schema_reject_count"] = int(state.get("schema_reject_count") or 0) + 1
            save_state(cfg.metrics_dir, state)
            return {"written": False, "mode": cfg.mode, "rejected_fields_count": 1, "schema_version": cfg.schema_version}
        path = self.store().append_event(event)
        if rejected:
            state["rejected_fields_count"] = int(state.get("rejected_fields_count") or 0) + rejected
            save_state(cfg.metrics_dir, state)
        return {
            "written": True,
            "mode": cfg.mode,
            "file": str(path),
            "event_id": event["event_id"],
            "rejected_fields_count": rejected,
            "schema_version": cfg.schema_version,
        }

    def summary(self, *, window_days: int = 30) -> dict[str, Any]:
        cfg = self.config()
        store = self.store()
        summary = store.summarize(window_days=window_days)
        state = cfg.state
        schema_status = state.get("schema_status") or "current"
        if cfg.source == "stale_persisted_consent":
            schema_status = "stale_consent"
        beacon_status = {
            "enabled": cfg.mode == "anonymous_beacon",
            "last_handshake_at": state.get("last_handshake_at"),
            "last_send_at": state.get("last_send_at"),
            "circuit_open_until": state.get("circuit_open_until"),
            "schema_status": schema_status,
        }
        return {
            "mode": cfg.mode,
            "source": cfg.source,
            "metrics_dir": str(cfg.metrics_dir),
            "retention_days": cfg.retention_days,
            "schema_version": cfg.schema_version,
            "product_aggregate_families": list(PRODUCT_AGGREGATE_FAMILIES),
            "summary": summary,
            "beacon_status": beacon_status,
            "next_opt_in_prompt_after": state.get("next_opt_in_prompt_after"),
            "consent": {
                "source": state.get("source"),
                "changed_at": state.get("changed_at"),
                "policy_version": state.get("policy_version"),
                "schema_version": state.get("schema_version"),
                "acknowledged_items": list(state.get("acknowledged_items") or []),
            },
            "resolved_precedence": list(cfg.resolved_precedence),
        }

    def update_settings(
        self,
        *,
        mode: TelemetryMode,
        source: str,
        policy_version: str | None = None,
        schema_version: str | None = None,
        acknowledged_items: list[str] | None = None,
    ) -> dict[str, Any]:
        if source not in {"settings_ui", "cli"}:
            raise ValueError("invalid telemetry settings source")
        if mode == "anonymous_beacon" and (not policy_version or not schema_version):
            raise ValueError("OPT_IN_PREREQUISITES_NOT_APPROVED")
        state = record_consent(
            self.settings,
            mode=mode,
            source=source,  # type: ignore[arg-type]
            policy_version=policy_version,
            schema_version=schema_version,
            acknowledged_items=acknowledged_items,
        )
        cfg = self.config()
        return {
            "mode": mode,
            "changed": True,
            "changed_at": state["changed_at"],
            "source": source,
            "schema_version": state.get("schema_version"),
            "acknowledged_items": list(state.get("acknowledged_items") or []),
            "next_opt_in_prompt_after": state.get("next_opt_in_prompt_after"),
            "resolved_precedence": list(cfg.resolved_precedence),
        }

    def export_local(self, output_path: str | None = None) -> dict[str, Any]:
        path = Path(output_path).expanduser() if output_path else None
        out = self.store().export_local(path)
        return {"output_path": str(out), "exported": True}

    def purge_local(self) -> dict[str, Any]:
        result = self.store().purge_local()
        result["purged_at"] = now_utc()
        return result

    def write_failure(self, code: str, detail: str | None = None) -> None:
        record = {"failed_at": now_utc(), "code": code}
        if detail:
            record["detail"] = detail[:160]
        self.store().append_sent(record, failed=True)
