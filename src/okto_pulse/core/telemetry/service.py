"""High-level telemetry policy orchestration over edition-neutral ports."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.telemetry import failure_state
from okto_pulse.core.telemetry import publish_health as publish_health_mod
from okto_pulse.core.telemetry.schema import normalize_event, now_utc
from okto_pulse.core.telemetry.settings import (
    TelemetryMode,
    mark_migration_notice_seen,
    record_consent,
    resolve_telemetry_config,
)
from okto_pulse.core.telemetry.event_store_registry import get_telemetry_event_store
from okto_pulse.core.telemetry.publish_health_source_registry import (
    get_external_source_descriptors,
)
from okto_pulse.core.telemetry.telemetry_state_registry import save_telemetry_state
from okto_pulse.core.ports.telemetry import (
    PRODUCT_AGGREGATE_FAMILIES,
    TelemetryEventStore,
)

logger = logging.getLogger("okto_pulse.telemetry.service")


def _log_runtime_skip(*, component: str, reason: str) -> None:
    logger.info(
        "metrics.runtime_skip",
        extra={
            "metric_name": "metrics_runtime_skip_total",
            "component": component,
            "outcome": "skipped",
            "reason": reason,
        },
    )


class TelemetryService:
    def __init__(self, settings: CoreSettings):
        self.settings = settings

    def config(self):
        return resolve_telemetry_config(self.settings)

    def store(self) -> TelemetryEventStore:
        # R10-B: obtain the EVENT store via the registered factory (Community
        # supplies the concrete adapter) — the core runtime no longer
        # instantiates LocalTelemetryStore directly.
        cfg = self.config()
        return get_telemetry_event_store(cfg.state_ref, cfg.retention_days)

    def record_event(
        self, event_type: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        cfg = self.config()
        state = dict(cfg.state)
        if cfg.mode == "disabled":
            _log_runtime_skip(component="record_event", reason="disabled")
            return {
                "written": False,
                "mode": cfg.mode,
                "rejected_fields_count": 0,
                "schema_version": cfg.schema_version,
            }
        try:
            event, rejected = normalize_event(
                event_type,
                payload,
                app_version=getattr(self.settings, "app_version", "0.0.0+local"),
                schema_version=cfg.schema_version,
            )
        except Exception:
            state["schema_reject_count"] = (
                int(state.get("schema_reject_count") or 0) + 1
            )
            save_telemetry_state(cfg.state_ref, state)
            return {
                "written": False,
                "mode": cfg.mode,
                "rejected_fields_count": 1,
                "schema_version": cfg.schema_version,
            }
        artifact_ref = self.store().append_event(event)
        if rejected:
            state["rejected_fields_count"] = (
                int(state.get("rejected_fields_count") or 0) + rejected
            )
            save_telemetry_state(cfg.state_ref, state)
        return {
            "written": True,
            "mode": cfg.mode,
            "file": str(artifact_ref),
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
        if cfg.migration_notice and cfg.migration_notice.get("pending"):
            logger.info(
                "metrics.migration_notice",
                extra={
                    "metric_name": "metrics_migration_notice_total",
                    "notice_key": cfg.migration_notice.get("type"),
                    "outcome": "pending_returned",
                },
            )
        return {
            "mode": cfg.mode,
            "ui_mode": cfg.ui_mode,
            "enabled": cfg.mode == "anonymous_beacon",
            "normalized_from": cfg.normalized_from,
            "migration_notice": cfg.migration_notice,
            "source": cfg.source,
            "retention_days": cfg.retention_days,
            "schema_version": cfg.schema_version,
            "product_aggregate_families": list(PRODUCT_AGGREGATE_FAMILIES),
            "summary": summary,
            "beacon_status": beacon_status,
            # R1-D: sanitized failure-state read for UI/MCP/CLI. Built from the
            # R1-A allowlist projection, so it can never carry install_token /
            # token_hash. Legacy state migrates to safe defaults (R1-A).
            "publish_status": failure_state.public_status_projection(state),
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

    def publish_health(self, *, now: datetime | None = None) -> dict[str, Any]:
        """R5C-A: compose the publish-health DTO for the MCP/UI health surface.

        Consumes the R5A PUBLIC failure-state projection (the allowlisted,
        already-redacted boundary surfaced by ``summary()['publish_status']``)
        and only CLASSIFIES the health status / source / freshness on top — no
        producer logic, no trust recomputation, no parallel schema, no
        re-derived redaction (see ``docs/architecture/telemetry_r5a_r5c_boundary.md``).
        When NO health source can be read, returns the structured
        ``HEALTH_SOURCE_UNAVAILABLE`` error instead of a (misleading) health DTO.
        """
        resolved_now = now or datetime.now(timezone.utc)
        try:
            cfg = self.config()
            state = dict(cfg.state)
            projection = failure_state.public_status_projection(state)
        except Exception:
            logger.info(
                "metrics.publish_health",
                extra={
                    "metric_name": "metrics_publish_health_total",
                    "outcome": "source_unavailable",
                    "source": publish_health_mod.SOURCE_LOCAL,
                },
            )
            return publish_health_mod.redact_health_payload(
                {
                    "error": publish_health_mod.HEALTH_SOURCE_UNAVAILABLE,
                    "source": publish_health_mod.SOURCE_LOCAL,
                    "message": "No publish-health source could be read.",
                    "redaction_applied": True,
                }
            )
        # R5C-C: compose the four distinguished sources. local + install_lifecycle
        # are REAL client signals; aws_ingest / report_athena have no adapter in the
        # core client (downstream R5B/R4) so they enter as an explicit gap and can
        # never be inferred healthy from a local send.
        install_lifecycle = publish_health_mod.derive_install_lifecycle(
            state, now=resolved_now
        )
        # R10-D: the external (aws_ingest / report_athena) descriptors come from the
        # registered edition provider (Community PublishHealthSource signals);
        # register-before-remove falls back to the core GAP default. The pure
        # resolve_publish_health classifier below is unchanged — a gap/stale/expired/
        # unavailable/absent source can never be inferred healthy.
        aws_ingest, report_athena = get_external_source_descriptors(self.settings)
        dto = publish_health_mod.resolve_publish_health(
            projection,
            now=resolved_now,
            install_lifecycle=install_lifecycle,
            aws_ingest=aws_ingest,
            report_athena=report_athena,
        )
        logger.info(
            "metrics.publish_health",
            extra={
                "metric_name": "metrics_publish_health_total",
                "outcome": dto.status,  # enum only — never the DTO/state itself
                "source": dto.source,
            },
        )
        # R5C-E guardrail: every surface (API/MCP/UI) gets ONLY redacted data —
        # recursively scrub forbidden keys + secret values (incl. any from the
        # source state) before the payload leaves the process.
        return publish_health_mod.redact_health_payload(
            dto.to_dict(),
            secret_values=publish_health_mod.collect_health_secret_values(state),
        )

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
        effective_mode = str(state.get("mode") or cfg.mode)
        return {
            "mode": effective_mode,
            "ui_mode": "on" if effective_mode == "anonymous_beacon" else "off",
            "enabled": effective_mode == "anonymous_beacon",
            "normalized_from": state.get("normalized_from"),
            "migration_notice": cfg.migration_notice,
            "changed": True,
            "changed_at": state["changed_at"],
            "source": source,
            "schema_version": state.get("schema_version"),
            "acknowledged_items": list(state.get("acknowledged_items") or []),
            "next_opt_in_prompt_after": state.get("next_opt_in_prompt_after"),
            "resolved_precedence": list(cfg.resolved_precedence),
        }

    def mark_migration_notice_seen(self, *, notice_key: str) -> dict[str, Any]:
        result = mark_migration_notice_seen(self.settings, notice_key=notice_key)
        logger.info(
            "metrics.migration_notice",
            extra={
                "metric_name": "metrics_migration_notice_total",
                "notice_key": notice_key,
                "outcome": "seen_idempotent"
                if result["idempotent"]
                else "seen_acknowledged",
            },
        )
        return result

    def export_events(self, destination_ref: str | None = None) -> dict[str, Any]:
        out = self.store().export_events(destination_ref)
        return {"output_path": str(out), "exported": True}

    def purge_events(self) -> dict[str, Any]:
        result = self.store().purge_events()
        result["purged_at"] = now_utc()
        return result

    def write_failure(self, code: str, detail: str | None = None) -> None:
        record = {"failed_at": now_utc(), "code": code}
        if detail:
            record["detail"] = detail[:160]
        self.store().append_sent(record, failed=True)
