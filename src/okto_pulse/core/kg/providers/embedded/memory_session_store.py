"""InMemorySessionStore — satisfies SessionStore Protocol.

Refactored from okto_pulse.core.kg.session_manager.SessionManager.
Preserves asyncio.Lock per-session and TTL expiry semantics.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from okto_pulse.core.kg.schemas import SessionStatus
from okto_pulse.core.kg.session_manager import (
    ConsolidationSession,
    _now,
    compute_content_hash,
)


class InMemorySessionStore:
    def __init__(self, default_ttl_seconds: int = 3600):
        self._sessions: dict[str, ConsolidationSession] = {}
        self._global_lock = asyncio.Lock()
        self._default_ttl = default_ttl_seconds

    @property
    def default_ttl_seconds(self) -> int:
        return self._default_ttl

    async def create(
        self,
        *,
        session_id: str,
        board_id: str,
        artifact_id: str,
        artifact_type: str,
        agent_id: str,
        raw_content: str,
        ttl_seconds: int | None = None,
    ) -> ConsolidationSession:
        ttl = ttl_seconds or self._default_ttl
        now = _now()
        content_hash = compute_content_hash(raw_content, artifact_id, board_id)
        session = ConsolidationSession(
            session_id=session_id,
            board_id=board_id,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            agent_id=agent_id,
            content_hash=content_hash,
            started_at=now,
            expires_at=now + timedelta(seconds=ttl),
            raw_content=raw_content,
        )
        async with self._global_lock:
            if session_id in self._sessions:
                raise ValueError(f"session_id already exists: {session_id}")
            self._sessions[session_id] = session
        return session

    async def get(self, session_id: str) -> ConsolidationSession | None:
        session = self._sessions.get(session_id)
        if session is None:
            return None
        if session.is_expired() and session.status == SessionStatus.OPEN:
            session.status = SessionStatus.EXPIRED
            async with self._global_lock:
                self._sessions.pop(session_id, None)
            return None
        return session

    async def remove(self, session_id: str) -> None:
        async with self._global_lock:
            self._sessions.pop(session_id, None)

    async def sweep_expired(self) -> int:
        count = 0
        async with self._global_lock:
            expired_ids = [
                sid
                for sid, s in self._sessions.items()
                if s.is_expired() and s.status == SessionStatus.OPEN
            ]
            for sid in expired_ids:
                self._sessions[sid].status = SessionStatus.EXPIRED
                del self._sessions[sid]
                count += 1
        return count

    async def active_count(self) -> int:
        async with self._global_lock:
            return sum(
                1 for s in self._sessions.values() if s.status == SessionStatus.OPEN
            )

    def clear_for_tests(self) -> None:
        self._sessions.clear()
