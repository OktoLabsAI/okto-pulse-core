"""MCP request trace middleware.

The core captures per-tool-call replay records and delegates persistence to an
explicit :class:`McpTraceSink`. Concrete storage is an edition adapter concern:
Community supplies the local JSONL sink when tracing is enabled.

Record format (one JSON-safe mapping per call)
----------------------------------------------
::

    {
      "ts":          "2026-04-27T19:51:23.456789+00:00",
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
import inspect
import time
from datetime import datetime, timezone
from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from okto_pulse.core.ports.mcp_trace import McpTraceSink


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
    """FastMCP middleware that sends every tool-call record to a trace sink."""

    def __init__(self, trace_sink: McpTraceSink):
        self._trace_sink = trace_sink

    @staticmethod
    def _session_id_from(context: MiddlewareContext) -> str:
        ctx = getattr(context, "fastmcp_context", None)
        for attr in ("session_id", "client_id", "request_id"):
            value = getattr(ctx, attr, None) if ctx is not None else None
            if value:
                return str(value)
        return "anon"

    async def _write_best_effort(
        self,
        session_id: str,
        record: dict[str, Any],
    ) -> None:
        try:
            result = self._trace_sink.write_trace(session_id, record)
            if inspect.isawaitable(result):
                await result
        except Exception:
            pass

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
        except Exception as exc:
            record["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
            record["is_error"] = True
            record["error"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            await self._write_best_effort(session_id, record)


def install_trace_sink(mcp, trace_sink: McpTraceSink | None) -> bool:
    """Register TraceMiddleware when an explicit sink is supplied."""
    if trace_sink is None:
        return False
    mcp.add_middleware(TraceMiddleware(trace_sink))
    return True


__all__ = ["TraceMiddleware", "_safe_jsonable", "install_trace_sink"]
