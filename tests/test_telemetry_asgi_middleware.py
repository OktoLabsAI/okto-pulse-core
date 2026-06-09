"""Tests do middleware de telemetria como ASGI puro.

Contrato preservado da versão BaseHTTPMiddleware: registra método, template
da rota, status e duração APENAS para paths /api/. Contrato novo: não
envolve a resposta em task group/cancel scope (verificado indiretamente —
o streaming passa pelo middleware sem buffering nem consumo antecipado).
"""

from __future__ import annotations

import asyncio

import pytest

import okto_pulse.core.app as app_module
from okto_pulse.core.app import _TelemetryASGIMiddleware

pytestmark = pytest.mark.asyncio(loop_scope="session")


class _RecordingTelemetry:
    events: list[tuple[str, dict]] = []

    def __init__(self, _settings):
        pass

    def record_event(self, kind, payload):
        _RecordingTelemetry.events.append((kind, payload))


@pytest.fixture(autouse=True)
def _patch_telemetry(monkeypatch):
    _RecordingTelemetry.events = []
    monkeypatch.setattr(app_module, "TelemetryService", _RecordingTelemetry)


def _scope(path: str, method: str = "GET") -> dict:
    return {"type": "http", "path": path, "method": method, "headers": []}


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok", "more_body": False})


async def _noop_receive():
    return {"type": "http.request", "body": b"", "more_body": False}


async def _collect_send(messages):
    async def send(message):
        messages.append(message)
    return send


async def test_records_api_request_with_status():
    mw = _TelemetryASGIMiddleware(_ok_app, settings=object())
    messages: list = []
    await mw(_scope("/api/v1/boards"), _noop_receive, await _collect_send(messages))

    assert messages[0]["status"] == 200
    assert len(_RecordingTelemetry.events) == 1
    kind, payload = _RecordingTelemetry.events[0]
    assert kind == "http"
    assert payload["method"] == "GET"
    assert payload["status_code"] == 200
    assert payload["route_template"] == "/api/v1/boards"
    assert payload["duration_ms"] >= 0
    assert "error_class" not in payload


async def test_non_api_path_not_recorded():
    mw = _TelemetryASGIMiddleware(_ok_app, settings=object())
    messages: list = []
    await mw(_scope("/kg-health"), _noop_receive, await _collect_send(messages))
    assert messages[0]["status"] == 200
    assert _RecordingTelemetry.events == []


async def test_downstream_exception_recorded_and_reraised():
    async def _boom(scope, receive, send):
        raise RuntimeError("boom")

    mw = _TelemetryASGIMiddleware(_boom, settings=object())
    with pytest.raises(RuntimeError):
        await mw(_scope("/api/v1/x"), _noop_receive, await _collect_send([]))
    _, payload = _RecordingTelemetry.events[0]
    assert payload["error_class"] == "RuntimeError"
    assert payload["status_code"] == 500


async def test_streaming_chunks_pass_through_unbuffered():
    """SSE-like: cada chunk atravessa o middleware imediatamente — sem o
    consumo-em-task-separada do BaseHTTPMiddleware."""
    chunk_sent = asyncio.Event()
    release_second = asyncio.Event()

    async def _stream_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        chunk_sent.set()
        await release_second.wait()
        await send({"type": "http.response.body", "body": b"second", "more_body": False})

    mw = _TelemetryASGIMiddleware(_stream_app, settings=object())
    messages: list = []
    task = asyncio.create_task(
        mw(_scope("/api/v1/kg/boards/x/events"), _noop_receive, await _collect_send(messages))
    )
    await asyncio.wait_for(chunk_sent.wait(), timeout=2.0)
    # O primeiro chunk já chegou ao send original enquanto o app ainda roda.
    assert any(m.get("body") == b"first" for m in messages if m["type"] == "http.response.body")
    release_second.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert len(_RecordingTelemetry.events) == 1


async def test_hard_cancel_midstream_still_records_and_propagates():
    """Desconexão de cliente SSE: cancel no meio do stream não engole a
    cancelação e ainda registra o evento de telemetria."""
    started = asyncio.Event()

    async def _hanging_stream(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        started.set()
        await asyncio.sleep(30)

    mw = _TelemetryASGIMiddleware(_hanging_stream, settings=object())
    task = asyncio.create_task(
        mw(_scope("/api/v1/kg/boards/x/events"), _noop_receive, await _collect_send([]))
    )
    await asyncio.wait_for(started.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(_RecordingTelemetry.events) == 1
