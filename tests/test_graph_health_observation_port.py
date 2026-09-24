"""Health must require edition isolation before invoking graph probe code."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import interfaces
from okto_pulse.core.services import kg_health_service as health


@pytest.mark.parametrize("probe", [health._GRAPH_HEALTH_PROBE, health._PARITY_HEALTH_PROBE])
def test_missing_observation_port_refuses_before_graph_code(monkeypatch, probe):
    calls = []
    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace())
    with pytest.raises(RuntimeError, match="^graph_health_observation_unavailable$"):
        health._run_health_probe_step(
            probe_name=probe, board_id="board", step_name="read",
            build=lambda: calls.append("read"),
        )
    assert calls == []


@pytest.mark.parametrize("probe", [health._GRAPH_HEALTH_PROBE, health._PARITY_HEALTH_PROBE])
def test_port_wraps_probe_and_exits_after_error(monkeypatch, probe):
    calls = []

    @contextmanager
    def scope(board_id):
        calls.append(("enter", board_id))
        try:
            yield
        finally:
            calls.append("exit")

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace(
        graph_health_observation=SimpleNamespace(scope=scope)))

    def build():
        calls.append("read")
        raise ValueError("probe failed")

    with pytest.raises(ValueError, match="probe failed"):
        health._run_health_probe_step(
            probe_name=probe, board_id="board", step_name="read", build=build,
        )
    assert calls == [("enter", "board"), "read", "exit"]
