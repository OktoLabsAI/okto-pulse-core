"""AF-09 core MCP trace sink contract."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.trace_middleware import TraceMiddleware, install_trace_sink
from okto_pulse.core.ports import McpTraceSink


class _RecordingSink:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def write_trace(self, session_id: str, record: dict[str, Any]) -> None:
        self.records.append((session_id, dict(record)))


class _RaisingSink:
    def write_trace(self, session_id: str, record: dict[str, Any]) -> None:
        raise OSError("trace target unavailable")


class _AsyncSink:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    async def write_trace(self, session_id: str, record: dict[str, Any]) -> None:
        self.records.append((session_id, dict(record)))


def test_mcp_trace_sink_protocol_is_structural() -> None:
    assert isinstance(_RecordingSink(), McpTraceSink)
    assert isinstance(_AsyncSink(), McpTraceSink)


def test_install_trace_sink_requires_explicit_sink() -> None:
    class _Mcp:
        def __init__(self) -> None:
            self.middleware: list[Any] = []

        def add_middleware(self, middleware: Any) -> None:
            self.middleware.append(middleware)

    mcp = _Mcp()
    assert install_trace_sink(mcp, None) is False
    assert mcp.middleware == []

    sink = _RecordingSink()
    assert install_trace_sink(mcp, sink) is True
    assert len(mcp.middleware) == 1
    assert isinstance(mcp.middleware[0], TraceMiddleware)


def test_build_mcp_asgi_app_passes_sink_without_env_lookup(monkeypatch) -> None:
    calls: list[McpTraceSink | None] = []

    async def _asgi_app(scope, receive, send):
        return None

    class _Mcp:
        def http_app(self, *, transport: str):
            assert transport == "streamable-http"
            return _asgi_app

    def _fake_install(mcp, trace_sink=None) -> bool:
        calls.append(trace_sink)
        return trace_sink is not None

    monkeypatch.setenv("MCP_TRACE_ENABLED", "1")
    monkeypatch.setenv("MCP_TRACE_DIR", "would-not-be-read")
    monkeypatch.setattr(server, "mcp", _Mcp())
    monkeypatch.setattr(server, "_install_trace", _fake_install)

    server.build_mcp_asgi_app()
    sink = _RecordingSink()
    server.build_mcp_asgi_app(trace_sink=sink)

    assert calls == [None, sink]


def test_trace_middleware_records_success_and_sink_failure_is_best_effort() -> None:
    sink = _RecordingSink()
    context = _context(session_id="session-1")
    result = object()

    async def _call_next(ctx):
        assert ctx is context
        return result

    observed = asyncio.run(TraceMiddleware(sink).on_call_tool(context, _call_next))

    assert observed is result
    assert len(sink.records) == 1
    session_id, record = sink.records[0]
    assert session_id == "session-1"
    assert record["session_id"] == "session-1"
    assert record["tool"] == "example_tool"
    assert record["arguments"] == {"x": 1}
    assert record["is_error"] is False
    assert record["error"] is None
    assert record["duration_ms"] is not None

    observed = asyncio.run(TraceMiddleware(_RaisingSink()).on_call_tool(context, _call_next))
    assert observed is result


def test_trace_middleware_records_tool_errors_and_reraises() -> None:
    sink = _RecordingSink()
    context = _context(session_id="session-err")

    async def _call_next(_ctx):
        raise ValueError("bad input")

    try:
        asyncio.run(TraceMiddleware(sink).on_call_tool(context, _call_next))
    except ValueError:
        pass
    else:
        raise AssertionError("tool exception should be re-raised")

    assert len(sink.records) == 1
    session_id, record = sink.records[0]
    assert session_id == "session-err"
    assert record["is_error"] is True
    assert record["error"] == {"type": "ValueError", "message": "bad input"}
    assert record["response"] is None


def test_trace_middleware_records_cancelled_error_and_reraises() -> None:
    sink = _RecordingSink()
    context = _context(session_id="session-cancel")

    async def _call_next(_ctx):
        raise asyncio.CancelledError()

    try:
        asyncio.run(TraceMiddleware(sink).on_call_tool(context, _call_next))
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("cancelled tool call should be re-raised")

    assert len(sink.records) == 1
    session_id, record = sink.records[0]
    assert session_id == "session-cancel"
    assert record["is_error"] is True
    assert record["error"] == {"type": "CancelledError", "message": "task cancelled"}
    assert record["response"] is None


def test_async_trace_sink_is_awaited() -> None:
    sink = _AsyncSink()
    context = _context(session_id="session-async")

    async def _call_next(_ctx):
        return SimpleNamespace(is_error=False, value="ok")

    asyncio.run(TraceMiddleware(sink).on_call_tool(context, _call_next))

    assert len(sink.records) == 1
    assert sink.records[0][0] == "session-async"


def test_core_trace_module_has_no_env_or_filesystem_persistence() -> None:
    import okto_pulse.core.mcp.trace_middleware as trace_middleware

    source = Path(trace_middleware.__file__).read_text(encoding="utf-8")
    forbidden = (
        "os.environ",
        "MCP_TRACE_DIR",
        "KG_BASE_DIR",
        "Path.mkdir",
        "Path.open",
        "open(",
        "Lock(",
        "_session_files",
        "MCPProjectionTelemetrySink",
        "TelemetrySink",
    )
    for needle in forbidden:
        assert needle not in source


def _context(session_id: str):
    return SimpleNamespace(
        message=SimpleNamespace(name="example_tool", arguments={"x": 1}),
        fastmcp_context=SimpleNamespace(session_id=session_id),
    )
