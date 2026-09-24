"""Startup configuration facade; no public tuning writer or runtime effect.

The edition reads and validates persisted deployment settings before composing
its runtime. Core only invokes the registered startup Protocol.
"""
from __future__ import annotations

from typing import Any
from okto_pulse.core.ports.relational_services import resolve_runtime_settings_adapter


async def apply_persisted_settings_to_core_settings() -> dict[str, Any]:
    return await resolve_runtime_settings_adapter().apply_persisted_settings_to_core_settings()


__all__ = ["apply_persisted_settings_to_core_settings"]
