"""Pure policy helpers for revisioned Flow Health board settings."""

from __future__ import annotations

from collections.abc import Mapping

from okto_pulse.core.models.schemas import (
    BoardSettings,
    FlowHealthSettings,
    FlowHealthSettingsUpdate,
)


class FlowHealthSettingsVersionConflict(ValueError):
    """The caller edited a Flow Health policy revision that is no longer current."""

    code = "flow_health_settings_version_conflict"

    def __init__(self, *, expected_version: int, current_version: int) -> None:
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(
            f"{self.code}: expected version {expected_version}, "
            f"current version is {current_version}"
        )


def board_settings_with_next_flow_health_policy(
    persisted: Mapping[str, object] | BoardSettings | None,
    *,
    expected_version: int,
    update: FlowHealthSettingsUpdate | None,
) -> tuple[BoardSettings, FlowHealthSettings]:
    """Return a full settings document with one CAS-success successor policy.

    ``update=None`` is the governed restore operation. It restores Core's
    threshold/override defaults while still incrementing the policy revision,
    so stale editors cannot replay a pre-restore document.
    """

    root = (
        persisted
        if isinstance(persisted, BoardSettings)
        else BoardSettings.model_validate(dict(persisted or {}))
    )
    current = root.analytics.flow_health
    if current.version != expected_version:
        raise FlowHealthSettingsVersionConflict(
            expected_version=expected_version,
            current_version=current.version,
        )
    successor = FlowHealthSettings(
        version=current.version + 1,
        general_stale_hours=(72 if update is None else update.general_stale_hours),
        rejected_stale_hours=(96 if update is None else update.rejected_stale_hours),
        overrides={} if update is None else dict(update.overrides),
    )
    analytics = root.analytics.model_copy(update={"flow_health": successor})
    return root.model_copy(update={"analytics": analytics}), successor


__all__ = [
    "FlowHealthSettingsVersionConflict",
    "board_settings_with_next_flow_health_policy",
]
