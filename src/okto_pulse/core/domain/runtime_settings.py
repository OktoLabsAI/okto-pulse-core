"""Transport- and persistence-neutral runtime-settings policy contracts."""

from __future__ import annotations

from typing import Final

EVENT_QUEUE_KEYS: tuple[str, ...] = (
    "kg_queue_max_concurrent_workers",
    "kg_queue_min_interval_ms",
    "kg_queue_claim_timeout_s",
    "kg_queue_max_attempts",
    "kg_queue_alert_threshold",
)
DECAY_TICK_KEYS: tuple[str, ...] = (
    "kg_decay_tick_interval_minutes",
    "kg_decay_tick_staleness_days",
    "kg_decay_tick_max_age_days",
)
RUNTIME_KEYS: tuple[str, ...] = EVENT_QUEUE_KEYS + DECAY_TICK_KEYS
SETTINGS_RUNTIME_EFFECT_PORTS: Final[dict[str, str]] = {
    "kg_decay_tick_interval_minutes": "scheduler_control",
}


class ConfigChangeBlocked(Exception):
    """Application error raised when a guarded runtime change is rejected."""

    def __init__(
        self,
        *,
        reason: str,
        setting_group: str,
        audit_event: str,
    ) -> None:
        super().__init__(f"{reason} (setting_group={setting_group})")
        self.reason = reason
        self.setting_group = setting_group
        self.audit_event = audit_event


__all__ = [
    "DECAY_TICK_KEYS",
    "EVENT_QUEUE_KEYS",
    "RUNTIME_KEYS",
    "SETTINGS_RUNTIME_EFFECT_PORTS",
    "ConfigChangeBlocked",
]
