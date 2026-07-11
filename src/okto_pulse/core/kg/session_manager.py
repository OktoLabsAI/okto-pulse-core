"""In-memory consolidation session state manager with TTL and asyncio locks.

One `ConsolidationSession` per (session_id) tracks:
- Owning agent_id (enforces ownership on every primitive)
- Accumulated node_candidates / edge_candidates
- Pre-computed content_hash for nothing-changed detection
- A per-session asyncio.Lock so concurrent primitive calls serialize
- Expiry timestamp for cleanup

The `SessionManager` is a process-wide singleton (single-process server). For
multi-process setups this would need to move to Redis — out of scope for MVP.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from okto_pulse.core.kg.schemas import (
    EdgeCandidate,
    NodeCandidate,
    ReconciliationHint,
    SessionStatus,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def now_utc() -> datetime:
    """Public facade for consolidation-session UTC timestamps."""

    return _now()


def compute_content_hash(raw_content: str, artifact_id: str, board_id: str) -> str:
    """Deterministic SHA256 over (board_id, artifact_id, raw_content)."""
    h = hashlib.sha256()
    h.update(board_id.encode("utf-8"))
    h.update(b"\x00")
    h.update(artifact_id.encode("utf-8"))
    h.update(b"\x00")
    h.update(raw_content.encode("utf-8"))
    return h.hexdigest()


@dataclass
class ConsolidationSession:
    """State of a single in-flight consolidation session."""

    session_id: str
    board_id: str
    artifact_id: str
    artifact_type: str
    agent_id: str
    content_hash: str
    started_at: datetime
    expires_at: datetime
    status: SessionStatus = SessionStatus.OPEN
    raw_content: str = ""
    node_candidates: dict[str, NodeCandidate] = field(default_factory=dict)
    edge_candidates: dict[str, EdgeCandidate] = field(default_factory=dict)
    reconciliation_hints: dict[str, ReconciliationHint] = field(default_factory=dict)
    # Fields populated during commit — used by abort/compensating delete.
    committed_kuzu_node_refs: list[dict[str, Any]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def is_expired(self) -> bool:
        return _now() >= self.expires_at

    def touch(self, ttl_seconds: int) -> None:
        """Extend expiry on activity."""
        self.expires_at = _now() + timedelta(seconds=ttl_seconds)

    def check_ownership(self, agent_id: str) -> bool:
        return self.agent_id == agent_id


class SessionManager:
    """Backward-compat wrapper — delegates to the registry's SessionStore.

    Existing code that calls get_session_manager() continues to work.
    New runtime code should resolve the store through
    ``get_kg_registry().require_session_store()`` so missing composition is
    reported as ``runtime_provider_missing``.
    """

    def __init__(self, default_ttl_seconds: int = 3600):
        self._default_ttl = default_ttl_seconds

    def _store(self):
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        # AC3 (base fail-closed): the session porta requires the registered store;
        # an absent slot raises ``runtime_provider_missing`` instead of a late
        # ``AttributeError`` on ``None`` or a silent concrete fallback.
        return get_kg_registry().require_session_store()

    @property
    def default_ttl_seconds(self) -> int:
        # A benign config read keeps its graceful default: unlike real session
        # operations (which go through the fail-closed ``_store()`` porta), the TTL
        # default does not gate on provider presence.
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        store = get_kg_registry().session_store
        return store.default_ttl_seconds if store else self._default_ttl

    async def create(self, **kwargs) -> ConsolidationSession:
        return await self._store().create(**kwargs)

    async def get(self, session_id: str) -> ConsolidationSession | None:
        return await self._store().get(session_id)

    async def remove(self, session_id: str) -> None:
        await self._store().remove(session_id)

    async def sweep_expired(self) -> int:
        return await self._store().sweep_expired()

    async def active_count(self) -> int:
        return await self._store().active_count()

    def clear_for_tests(self) -> None:
        store = self._store()
        if hasattr(store, "clear_for_tests"):
            store.clear_for_tests()


_RUNTIME_KEY = "kg.session_manager"


def get_session_manager() -> SessionManager:
    """Return the process-wide SessionManager (backward compat wrapper)."""
    manager = resolve_runtime_value(_RUNTIME_KEY)
    if manager is None:
        from okto_pulse.core.kg.interfaces.registry import get_kg_registry

        config = get_kg_registry().config
        manager = SessionManager(
            default_ttl_seconds=config.kg_session_ttl_seconds if config else 3600
        )
        register_runtime_value(_RUNTIME_KEY, manager)
    return manager


def reset_session_manager_for_tests() -> None:
    """Drop the cached SessionManager — tests only."""
    reset_runtime_values(_RUNTIME_KEY)
