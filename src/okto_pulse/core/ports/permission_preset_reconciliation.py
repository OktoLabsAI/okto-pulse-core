"""Edition-neutral persistence port for permission-preset bootstrap."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from okto_pulse.core.domain.permission_presets import (
    PersistedPermissionPreset,
    ReconciliationCommand,
)


@runtime_checkable
class PermissionPresetReconciliationRepository(Protocol):
    """Atomic adapter boundary used by the Core reconciliation use case."""

    async def list_permission_presets(self) -> Sequence[PersistedPermissionPreset]:
        """Return built-in and custom snapshots visible to bootstrap."""
        ...

    async def apply_permission_preset_commands(
        self,
        commands: Sequence[ReconciliationCommand],
    ) -> None:
        """Apply all commands atomically or leave persisted state unchanged."""
        ...


__all__ = ["PermissionPresetReconciliationRepository"]
