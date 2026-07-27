"""Durable stale-reconcile intent boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


@dataclass(frozen=True, slots=True)
class ReconcileIntentCreate:
    board_id: str
    artifact_type: str
    artifact_id: str
    generation: int
    delete_event_id: str
    source_refs: tuple[str, ...]
    occurred_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ReconcileIntentReceipt:
    intent_id: str
    generation: int
    delete_event_id: str
    created: bool = field(default=False, compare=False)


class ReconcileIntentPort(Protocol):
    async def persist_reconcile_intent(
        self,
        context: Any,
        request: ReconcileIntentCreate,
    ) -> ReconcileIntentReceipt:
        """Persist an immutable intent without committing the caller's UoW."""
        ...


_RUNTIME_KEY = "ports.reconcile_intent.port"


def register_reconcile_intent_port(port: ReconcileIntentPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_reconcile_intent_port() -> ReconcileIntentPort:
    return require_runtime_value(
        _RUNTIME_KEY,
        "reconcile_intent_port_not_configured",
    )


def reset_reconcile_intent_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ReconcileIntentCreate",
    "ReconcileIntentPort",
    "ReconcileIntentReceipt",
    "get_reconcile_intent_port",
    "register_reconcile_intent_port",
    "reset_reconcile_intent_port_for_tests",
]
