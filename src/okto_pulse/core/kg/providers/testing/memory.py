"""In-memory providers reserved for explicit Core test compositions.

These fakes exercise the cache, rate-limit and session-store port contracts.
They are never selected by a productive Core runtime; Community owns the
Local First implementations and future SaaS editions supply their own adapters.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
import hashlib
import json
import time
from typing import Any

from okto_pulse.core.kg.schemas import SessionStatus
from okto_pulse.core.kg.session_manager import (
    ConsolidationSession,
    _now,
    compute_content_hash,
)

_MAX_SIZE = 1000
_TTL_SECONDS = 60.0


class InMemoryCacheBackend:
    """LRU+TTL ``CacheBackend`` fake."""

    def __init__(self, max_size: int = _MAX_SIZE, ttl_seconds: float = _TTL_SECONDS):
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[float, Any]] = {}
        self._board_index: dict[str, set[str]] = {}

    def _key(self, tool_name: str, board_id: str, params: dict) -> str:
        raw = json.dumps({"t": tool_name, "b": board_id, "p": params}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, tool_name: str, board_id: str, params: dict) -> tuple[bool, Any]:
        key = self._key(tool_name, board_id, params)
        entry = self._cache.get(key)
        if entry is None:
            return False, None
        timestamp, value = entry
        if time.monotonic() - timestamp > self._ttl:
            self._cache.pop(key, None)
            return False, None
        return True, value

    def put(self, tool_name: str, board_id: str, params: dict, value: Any) -> None:
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache, key=lambda key: self._cache[key][0])
            self._cache.pop(oldest_key)
        key = self._key(tool_name, board_id, params)
        self._cache[key] = (time.monotonic(), value)
        self._board_index.setdefault(board_id, set()).add(key)

    def invalidate_board(self, board_id: str) -> int:
        keys = self._board_index.pop(board_id, set())
        for key in keys:
            self._cache.pop(key, None)
        return len(keys)

    def stats(self) -> dict:
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
            "boards_tracked": len(self._board_index),
        }

    def clear(self) -> None:
        self._cache.clear()
        self._board_index.clear()


class InMemoryTokenBucket:
    """Sliding-window ``RateLimiter`` fake."""

    def __init__(self, rate: int = 30, window: float = 60.0):
        self._rate = rate
        self._window = window
        self._tokens: dict[str, list[float]] = {}

    def allow(self, agent_id: str) -> tuple[bool, int]:
        now = time.monotonic()
        timestamps = self._tokens.setdefault(agent_id, [])
        cutoff = now - self._window
        self._tokens[agent_id] = [timestamp for timestamp in timestamps if timestamp > cutoff]
        timestamps = self._tokens[agent_id]
        if len(timestamps) >= self._rate:
            retry_after = int(self._window - (now - timestamps[0])) + 1
            return False, max(1, retry_after)
        timestamps.append(now)
        return True, 0

    def reset(self, agent_id: str) -> None:
        self._tokens.pop(agent_id, None)


class InMemorySessionStore:
    """Per-session lock and expiry ``SessionStore`` fake."""

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
        session = ConsolidationSession(
            session_id=session_id,
            board_id=board_id,
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            agent_id=agent_id,
            content_hash=compute_content_hash(raw_content, artifact_id, board_id),
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
        async with self._global_lock:
            expired_ids = [
                session_id
                for session_id, session in self._sessions.items()
                if session.is_expired() and session.status == SessionStatus.OPEN
            ]
            for session_id in expired_ids:
                self._sessions[session_id].status = SessionStatus.EXPIRED
                del self._sessions[session_id]
            return len(expired_ids)

    async def active_count(self) -> int:
        async with self._global_lock:
            return sum(
                1
                for session in self._sessions.values()
                if session.status == SessionStatus.OPEN
            )

    def clear_for_tests(self) -> None:
        self._sessions.clear()


__all__ = [
    "InMemoryCacheBackend",
    "InMemorySessionStore",
    "InMemoryTokenBucket",
]
