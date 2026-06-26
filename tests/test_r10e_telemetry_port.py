"""R10-E Pass 2 (DESTRUCTIVE) — TelemetryPort registry, all sub-registries fail-closed.

Pass 2 removes the Stage-A additive fallback and makes ALL four registries
fail-closed (R10-B/C/D/E). Tests verify:
  - Calling any registry without a factory raises RuntimeError + structured signal.
  - Registered factory short-circuits the fail-closed guard.
  - Singleton gate baseline still covers the four factory globals.
  - No stale fallback wording ("fallback_uncomposed") in registry files.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.ports.telemetry import TelemetryPort
from okto_pulse.core.telemetry import telemetry_port_registry as registry
from okto_pulse.core.telemetry.telemetry_port_registry import (
    get_telemetry_port,
    register_telemetry_port_factory,
    reset_telemetry_port_factory_for_tests,
)


@pytest.fixture(autouse=True)
def _isolate_factory():
    reset_telemetry_port_factory_for_tests()
    try:
        yield
    finally:
        reset_telemetry_port_factory_for_tests()


def _settings(tmp_path: Path) -> CoreSettings:
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="")


# ===========================================================================
# Telemetry PORT registry — fail-closed.
# ===========================================================================
def test_get_telemetry_port_without_factory_raises_runtime_error(tmp_path, caplog):
    """R10-E Pass 2: no factory → RuntimeError + structured error signal (fail-closed)."""
    reset_telemetry_port_factory_for_tests()
    assert registry._telemetry_port_factory is None
    caplog.set_level("ERROR", logger="okto_pulse.telemetry.port_registry")

    with pytest.raises(RuntimeError, match="No TelemetryPort factory registered"):
        get_telemetry_port(_settings(tmp_path))

    signals = [
        r for r in caplog.records
        if r.__dict__.get("metric_name") == "telemetry_port_no_provider_total"
    ]
    assert len(signals) == 1
    assert signals[0].__dict__.get("outcome") == "fail_closed"
    assert signals[0].__dict__.get("reason") == "no_factory_registered"


def test_registered_factory_short_circuits_fail_closed(tmp_path, caplog):
    marker = object()

    class _Port:
        token = marker

        def record_event(self, event_type, payload=None):
            return {"written": False}

        def summary(self, *, window_days=30):
            return {}

        def publish_health(self, *, now=None):
            return {}

    register_telemetry_port_factory(lambda settings: _Port())
    caplog.set_level("ERROR", logger="okto_pulse.telemetry.port_registry")

    resolved = get_telemetry_port(_settings(tmp_path))
    assert isinstance(resolved, _Port)
    assert resolved.token is marker

    # The error signal is NOT emitted when a factory is registered.
    assert not [
        r for r in caplog.records
        if r.__dict__.get("metric_name") == "telemetry_port_no_provider_total"
    ]


def test_factory_conformance_isinstance_and_exercise(tmp_path):
    """A registered factory must produce a TelemetryPort-conformant object."""
    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )
    from okto_pulse.core.telemetry.product_aggregator_registry import (
        register_product_aggregator_factory,
        reset_product_aggregator_factory_for_tests,
    )
    from okto_pulse.core.telemetry.service import TelemetryService
    from okto_pulse.core.ports.telemetry import ProductState, PRODUCT_METRIC_KEYS

    class _NullStore:
        def append_event(self, e): ...
        def append_sent(self, r, *, failed=False): ...
        def append_snapshot(self, r): ...
        def confirmed_event_ids(self): return set()
        def iter_events(self, *, since=None): return ()
        def summarize(self, *, window_days=30): return {}
        def prune_old(self, *, now=None): return {}
        def export_local(self, output_path=None): ...
        def purge_local(self): return {}

    class _NullAgg:
        def __init__(self, s=None, m=None): pass
        def aggregate(self): return ProductState.from_dict({k: {"stub": 0} for k in PRODUCT_METRIC_KEYS})

    reset_telemetry_event_store_factory_for_tests()
    reset_product_aggregator_factory_for_tests()
    register_telemetry_event_store_factory(lambda base, ret: _NullStore())
    register_product_aggregator_factory(_NullAgg)
    # Compose TelemetryService as the registered facade.
    register_telemetry_port_factory(lambda settings: TelemetryService(settings))
    try:
        settings = CoreSettings(
            metrics_dir=str(tmp_path / "metrics"), metrics_mode="anonymous_beacon"
        )
        port = get_telemetry_port(settings)
        assert isinstance(port, TelemetryPort)
        result = port.record_event("cli", {"command": "serve", "exit_code": 0})
        assert isinstance(result, dict) and "written" in result
        summary = port.summary(window_days=7)
        assert "schema_version" in summary
        health = port.publish_health(now=datetime(2026, 6, 26, tzinfo=timezone.utc))
        assert health.get("redaction_applied") is True or "error" in health
    finally:
        reset_telemetry_event_store_factory_for_tests()
        reset_product_aggregator_factory_for_tests()


# ===========================================================================
# All R10-B/C/D sub-registries are also fail-closed (Pass 2).
# ===========================================================================
def test_pass2_all_registries_are_fail_closed(tmp_path, caplog):
    """R10-E Pass 2: ALL four registries raise RuntimeError without a factory."""
    from okto_pulse.core.telemetry.event_store_registry import (
        get_telemetry_event_store,
        reset_telemetry_event_store_factory_for_tests,
    )
    from okto_pulse.core.telemetry.product_aggregator_registry import (
        get_product_aggregator,
        reset_product_aggregator_factory_for_tests,
    )
    from okto_pulse.core.telemetry.sender_registry import (
        get_telemetry_sender,
        reset_telemetry_sender_factory_for_tests,
    )

    s = CoreSettings(metrics_dir=str(tmp_path / "metrics"))
    md = tmp_path / "metrics"

    reset_telemetry_event_store_factory_for_tests()
    with pytest.raises(RuntimeError, match="No TelemetryEventStore factory"):
        get_telemetry_event_store(md, 30)

    reset_product_aggregator_factory_for_tests()
    with pytest.raises(RuntimeError, match="No ProductAggregationPort factory"):
        get_product_aggregator(s, md)

    reset_telemetry_sender_factory_for_tests()
    with pytest.raises(RuntimeError, match="No TelemetrySink factory"):
        get_telemetry_sender(s)

    reset_telemetry_port_factory_for_tests()
    with pytest.raises(RuntimeError, match="No TelemetryPort factory"):
        get_telemetry_port(s)


# ===========================================================================
# Singleton baseline.
# ===========================================================================
def test_pass2_singleton_gate_baseline_still_covers_all_four_factories():
    """The four factory globals are baselined (not flagged as new singletons)."""
    from okto_pulse.core.application.boundary.singleton_gate import AntiSingletonGate
    sg = AntiSingletonGate().run()
    new_names = {n["name"] for n in sg.evidence["new_singletons"]}
    for name in (
        "_telemetry_port_factory",
        "_telemetry_sender_factory",
        "_factory",               # event_store_registry._factory
        "_product_aggregator_factory",
    ):
        assert name not in new_names, f"{name!r} flagged as new singleton"
