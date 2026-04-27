"""MCP request trace middleware.

Captures every tool call (request, response, errors, timing) to a JSONL file
so a real-life MCP session can be replayed as a regression test fixture later.

Activation
----------
Set ``MCP_TRACE_ENABLED=1`` (or ``true``/``yes``) on the MCP process. When
disabled, the middleware is never registered and there is zero runtime cost.

Output location
---------------
``MCP_TRACE_DIR`` env var, falling back to ``${KG_BASE_DIR}/mcp_traces``,
falling back to ``./mcp_traces``. One JSONL file per MCP session, named
``session_<session_id>_<utc_timestamp>.jsonl``. Sessions without a usable id
fall back to ``session_anon_<utc_timestamp>.jsonl``.

Record format (one JSON object per line)
----------------------------------------
::

    {
      "ts":          "2026-04-27T19:51:23.456789+00:00",  ISO 8601 UTC
      "session_id":  "<mcp session id or 'anon'>",
      "tool":        "<tool name>",
      "arguments":   { ... } | null,
      "duration_ms": 42.7,
      "is_error":    false,
      "response":    { "content": [...], "structured_content": {...}, "is_error": false } | null,
      "error":       { "type": "ValueError", "message": "..." } | null
    }
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext


def _trace_enabled() -> bool:
    return os.environ.get("MCP_TRACE_ENABLED", "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _resolve_trace_dir() -> Path:
    raw = (
        os.environ.get("MCP_TRACE_DIR")
        or (os.environ.get("KG_BASE_DIR") and f"{os.environ['KG_BASE_DIR']}/mcp_traces")
        or "./mcp_traces"
    )
    return Path(os.path.expanduser(raw))


def _safe_jsonable(obj: Any) -> Any:
    """Best-effort conversion to a JSON-serialisable structure."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _safe_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_safe_jsonable(v) for v in obj]
    if hasattr(obj, "model_dump"):
        try:
            return _safe_jsonable(obj.model_dump(mode="json"))
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        try:
            return _safe_jsonable({k: v for k, v in vars(obj).items() if not k.startswith("_")})
        except Exception:
            pass
    return repr(obj)


class TraceMiddleware(Middleware):
    """FastMCP middleware that appends each tool call to a per-session JSONL trace.

    Thread-safe: a single Lock serialises file appends across concurrent tool
    calls so lines never interleave. The cost is one write per call — fine for
    test-recording use cases, not intended for high-QPS production traffic.
    """

    def __init__(self, trace_dir: Path | str | None = None):
        self._dir = Path(trace_dir) if trace_dir else _resolve_trace_dir()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        # Cache the resolved path per session so all calls in one session land
        # in the same file (timestamps in filename only set on first call).
        self._session_files: dict[str, Path] = {}

    def _file_for(self, session_id: str) -> Path:
        existing = self._session_files.get(session_id)
        if existing is not None:
            return existing
        # Filename-safe timestamp + session id (truncated)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        safe_sid = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in session_id)[:32]
        path = self._dir / f"session_{safe_sid}_{ts}.jsonl"
        self._session_files[session_id] = path
        return path

    def _write(self, session_id: str, record: dict[str, Any]) -> None:
        path = self._file_for(session_id)
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self._lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    @staticmethod
    def _session_id_from(context: MiddlewareContext) -> str:
        ctx = getattr(context, "fastmcp_context", None)
        for attr in ("session_id", "client_id", "request_id"):
            value = getattr(ctx, attr, None) if ctx is not None else None
            if value:
                return str(value)
        return "anon"

    async def on_call_tool(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ):
        msg = context.message
        tool_name = getattr(msg, "name", None) or "<unknown>"
        arguments = getattr(msg, "arguments", None)
        session_id = self._session_id_from(context)

        ts = datetime.now(timezone.utc).isoformat()
        start = time.perf_counter()
        record: dict[str, Any] = {
            "ts": ts,
            "session_id": session_id,
            "tool": tool_name,
            "arguments": _safe_jsonable(arguments),
            "is_error": False,
            "response": None,
            "error": None,
            "duration_ms": None,
        }

        try:
            result = await call_next(context)
            record["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            record["response"] = _safe_jsonable(result)
            record["is_error"] = bool(getattr(result, "is_error", False))
            return result
        except asyncio.CancelledError:
            record["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            record["is_error"] = True
            record["error"] = {"type": "CancelledError", "message": "task cancelled"}
            raise
        except BaseException as exc:
            record["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            record["is_error"] = True
            record["error"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            try:
                self._write(session_id, record)
            except Exception:
                pass


def install_if_enabled(mcp) -> bool:
    """Register TraceMiddleware on the given FastMCP instance iff env enabled.

    Returns True when wired, False otherwise. Safe to call unconditionally —
    callers don't need to guard on the env var themselves.
    """
    if not _trace_enabled():
        return False
    mcp.add_middleware(TraceMiddleware())
    return True
