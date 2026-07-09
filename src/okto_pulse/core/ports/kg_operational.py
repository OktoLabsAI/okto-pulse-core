"""Core contracts for KG operational relational state.

The concrete relational queries are owned by edition adapters. Core KG readers
and governance code depend on these narrow ports so their public signatures do
not expose SQLAlchemy symbols or ORM rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class KGOperationalProviderMissing(RuntimeError):
    """Raised when a required KG operational provider is not registered."""

    code = "kg_operational_provider_missing"

    def __init__(self, provider: str) -> None:
        self.provider = provider
        super().__init__(
            f"kg_operational_provider_missing: required provider '{provider}' "
            "not supplied"
        )


@dataclass(frozen=True, slots=True)
class KGOutboxCounts:
    pending: int = 0
    dead_letter: int = 0
    processed: int = 0


@dataclass(frozen=True, slots=True)
class KGCanonicalDebtSignal:
    artifact_type: str
    artifact_id: str
    source_ref: str | None
    canonical_state: str | None
    failure_reason: str | None = None
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class KGDeadLetterSignal:
    artifact_type: str
    artifact_id: str


@dataclass(frozen=True, slots=True)
class KGQueueEntrySnapshot:
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    attempts: int = 0
    last_error: str | None = None


@runtime_checkable
class KGOperationalReadModelPort(Protocol):
    """Read-only operational KG projections required by Core modules."""

    async def list_consolidation_audit(
        self,
        context: Any,
        *,
        board_id: str,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    async def list_all_board_ids(
        self,
        context: Any,
        *,
        limit: int = 100,
    ) -> Sequence[str]:
        ...

    async def list_pending_entries(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Sequence[Mapping[str, Any]]:
        ...

    async def build_pending_tree(
        self,
        context: Any,
        *,
        board_id: str,
        depth: int = 5,
    ) -> Mapping[str, Any]:
        ...

    async def queue_status_counts(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, int]:
        ...

    async def kuzu_node_ref_operation_counts(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, int]:
        ...

    async def global_outbox_counts(
        self,
        context: Any,
        *,
        board_id: str,
        max_retries: int,
        dead_letter_retry_sentinel: int,
    ) -> KGOutboxCounts:
        ...

    async def list_canonical_debt_signals(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Sequence[KGCanonicalDebtSignal]:
        ...

    async def list_dead_letter_signals(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Sequence[KGDeadLetterSignal]:
        ...


@runtime_checkable
class KGGovernanceEffectsPort(Protocol):
    """Write/effect contract for KG governance operations."""

    async def start_historical_consolidation(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, Any]:
        ...

    async def pause_historical(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, Any]:
        ...

    async def resume_historical(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, Any]:
        ...

    async def cancel_historical(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, Any]:
        ...

    async def get_historical_progress(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> Mapping[str, Any]:
        ...


@runtime_checkable
class KGWorkerQueuePort(Protocol):
    """Queue transition contract for KG workers."""

    async def route_to_dead_letter(
        self,
        context: Any,
        *,
        queue_entry: KGQueueEntrySnapshot,
        errors: Sequence[Mapping[str, Any]],
    ) -> Any:
        ...

    async def list_dead_letter(
        self,
        context: Any,
        *,
        board_id: str,
        limit: int = 100,
    ) -> Sequence[Any]:
        ...

    async def retry_pending_entry(
        self,
        context: Any,
        *,
        board_id: str,
        queue_entry_id: str,
        recursive: bool = False,
    ) -> Mapping[str, Any] | None:
        ...


@runtime_checkable
class KGWorkerAuditPort(Protocol):
    """Audit write/read contract for KG worker persistence."""

    async def emit_outbox_event(
        self,
        context: Any,
        *,
        event_id: str,
        board_id: str,
        session_id: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        ...

    async def record_audit_event(
        self,
        context: Any,
        *,
        payload: Mapping[str, Any],
    ) -> None:
        ...


_kg_operational_read_model_port: KGOperationalReadModelPort | None = None
_kg_governance_effects_port: KGGovernanceEffectsPort | None = None
_kg_worker_queue_port: KGWorkerQueuePort | None = None
_kg_worker_audit_port: KGWorkerAuditPort | None = None


def register_kg_operational_ports(
    *,
    read_model: KGOperationalReadModelPort | None = None,
    governance_effects: KGGovernanceEffectsPort | None = None,
    worker_queue: KGWorkerQueuePort | None = None,
    worker_audit: KGWorkerAuditPort | None = None,
) -> None:
    """Register edition-owned KG operational providers."""

    global _kg_operational_read_model_port
    global _kg_governance_effects_port
    global _kg_worker_queue_port
    global _kg_worker_audit_port

    if read_model is not None:
        _kg_operational_read_model_port = read_model
    if governance_effects is not None:
        _kg_governance_effects_port = governance_effects
    if worker_queue is not None:
        _kg_worker_queue_port = worker_queue
    if worker_audit is not None:
        _kg_worker_audit_port = worker_audit


def get_kg_operational_read_model_port() -> KGOperationalReadModelPort:
    if _kg_operational_read_model_port is None:
        raise KGOperationalProviderMissing("read_model")
    return _kg_operational_read_model_port


def get_kg_governance_effects_port() -> KGGovernanceEffectsPort:
    if _kg_governance_effects_port is None:
        raise KGOperationalProviderMissing("governance_effects")
    return _kg_governance_effects_port


def get_kg_worker_queue_port() -> KGWorkerQueuePort:
    if _kg_worker_queue_port is None:
        raise KGOperationalProviderMissing("worker_queue")
    return _kg_worker_queue_port


def get_kg_worker_audit_port() -> KGWorkerAuditPort:
    if _kg_worker_audit_port is None:
        raise KGOperationalProviderMissing("worker_audit")
    return _kg_worker_audit_port


def reset_kg_operational_ports_for_tests() -> None:
    """Drop registered providers for deterministic test isolation."""

    global _kg_operational_read_model_port
    global _kg_governance_effects_port
    global _kg_worker_queue_port
    global _kg_worker_audit_port

    _kg_operational_read_model_port = None
    _kg_governance_effects_port = None
    _kg_worker_queue_port = None
    _kg_worker_audit_port = None


__all__ = [
    "KGCanonicalDebtSignal",
    "KGDeadLetterSignal",
    "KGGovernanceEffectsPort",
    "KGOperationalProviderMissing",
    "KGOperationalReadModelPort",
    "KGOutboxCounts",
    "KGQueueEntrySnapshot",
    "KGWorkerAuditPort",
    "KGWorkerQueuePort",
    "get_kg_governance_effects_port",
    "get_kg_operational_read_model_port",
    "get_kg_worker_audit_port",
    "get_kg_worker_queue_port",
    "register_kg_operational_ports",
    "reset_kg_operational_ports_for_tests",
]
