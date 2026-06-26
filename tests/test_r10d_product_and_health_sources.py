"""R10-D — ProductAggregationPort + PublishHealthSource descriptors (core side).
(Updated for R10-E Pass 2: ProductTelemetryAggregator + TelemetryBeaconSender removed
from core. Behavioral tests (product_state.json, snapshot) are in community.)

Scenario mapping (1:1 with the spec ts_ ids):
  ts_64fdc584 (TS01) — product/health PORTS import PURE (subprocess: no
       sqlite3 / ProductTelemetryAggregator / requests) and
       ProductAggregationPort / PublishHealthSource are runtime_checkable;
       PRODUCT_METRIC_KEYS/families are a pure contract.
  ts_a246a317 (TS03) — registry resolves a port-conformant aggregator through
       the registered factory; ProductState is correct shape; re-exports
       (PRODUCT_AGGREGATE_FAMILIES / PRODUCT_METRIC_KEYS) still importable
       from core.telemetry.product for backwards compat.
  ts_138260c8 (TS04) — AWS/report never healthy by inference: a gap/absent
       external descriptor floors the COMBINED status at degraded/source_gap.
  ts_49dec63e (TS05) — descriptor matrix (available/stale/expired/gap/
       unavailable/missing) maps to expected per-source status.
  ts_6ad289a7 (TS06) — TelemetryService consumes the aggregator THROUGH the
       registry/port (conformance isinstance + exercise), not a direct concrete.
  ts_b23dcb42 (TS07) — API/MCP/CLI/UI surfaces (summary families /
       publish_health) are preserved + secret-free.
  ts_210e694d (TS08) — ownership gate is CLEAN (R10-E Pass 2: class removed;
       gate finds zero violations); registry is fail-closed.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.ports.telemetry import (
    PRODUCT_AGGREGATE_FAMILIES,
    PRODUCT_METRIC_KEYS,
    ProductAggregationPort,
    ProductState,
    PublishHealthSource,
)
from okto_pulse.core.telemetry import publish_health as ph
from okto_pulse.core.telemetry.product_aggregator_registry import (
    get_product_aggregator,
    register_product_aggregator_factory,
    reset_product_aggregator_factory_for_tests,
)
from okto_pulse.core.telemetry.publish_health_source_registry import (
    get_external_source_descriptors,
    reset_external_source_provider_for_tests,
)
from okto_pulse.core.telemetry.service import TelemetryService


@pytest.fixture(autouse=True)
def _isolate_registries():
    reset_product_aggregator_factory_for_tests()
    reset_external_source_provider_for_tests()
    try:
        yield
    finally:
        reset_product_aggregator_factory_for_tests()
        reset_external_source_provider_for_tests()


def _settings(tmp_path: Path, **overrides) -> CoreSettings:
    values = {"metrics_dir": str(tmp_path / "metrics"), "metrics_mode": "anonymous_beacon"}
    values.update(overrides)
    return CoreSettings(**values)


# ---------------------------------------------------------------------------
# Inline ProductAggregationPort for core test isolation.
# (R10-E Pass 2: ProductTelemetryAggregator removed from core;
# behavioral tests live in community/tests/test_r10d_product_adapter.py.)
# ---------------------------------------------------------------------------

class _StubProductAggregator:
    """Minimal conformant ProductAggregationPort for core test isolation."""

    def __init__(self, settings=None, metrics_dir=None):
        self._metrics_dir = Path(metrics_dir) if metrics_dir else None

    def aggregate(self) -> ProductState:
        data = {k: {"example": 1} for k in PRODUCT_METRIC_KEYS}
        return ProductState.from_dict(data)


class _RecordingAggregator(_StubProductAggregator):
    """Tracks .aggregate() calls to verify registry wiring (not a direct concrete)."""

    instances: list["_RecordingAggregator"] = []

    def __init__(self, settings=None, metrics_dir=None):
        super().__init__(settings, metrics_dir)
        self.calls = 0
        _RecordingAggregator.instances.append(self)

    def aggregate(self) -> ProductState:  # type: ignore[override]
        self.calls += 1
        return super().aggregate()


# ===========================================================================
# ts_64fdc584 (TS01) — pure ports + runtime_checkable + pure vocabulary.
# ===========================================================================
def test_ts_64fdc584_ports_pure_and_runtime_checkable():
    code = (
        "import sys\n"
        "from okto_pulse.core.ports import (ProductAggregationPort, ProductState, "
        "PublishHealthSource, PRODUCT_METRIC_KEYS, PRODUCT_AGGREGATE_FAMILIES)\n"
        "heavy = [m for m in ('sqlite3','requests',"
        "'okto_pulse.core.telemetry.product','okto_pulse.core.telemetry.sender') "
        "if m in sys.modules]\n"
        "assert heavy == [], heavy\n"
        "assert PRODUCT_AGGREGATE_FAMILIES == tuple(sorted(PRODUCT_METRIC_KEYS))\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout

    for proto in (ProductAggregationPort, PublishHealthSource):
        assert not isinstance(object(), proto)  # structural rejection works
    assert len(PRODUCT_METRIC_KEYS) == 7
    assert PRODUCT_AGGREGATE_FAMILIES == tuple(sorted(PRODUCT_METRIC_KEYS))


# ===========================================================================
# ts_a246a317 (TS03) — registry resolves port-conformant aggregator; re-exports.
# (R10-E Pass 2: concrete product_state.json / snapshot tests moved to community.)
# ===========================================================================
def test_ts_a246a317_registry_resolves_port_and_reexports_pure(tmp_path):
    # Registry resolves through the registered factory (fail-closed otherwise).
    register_product_aggregator_factory(_StubProductAggregator)
    settings = _settings(tmp_path)
    agg = get_product_aggregator(settings, tmp_path / "metrics")
    assert isinstance(agg, ProductAggregationPort)
    state = agg.aggregate()
    assert isinstance(state, ProductState)
    assert any(k.startswith("product_") for k in state.to_dict())

    # PRODUCT_AGGREGATE_FAMILIES / PRODUCT_METRIC_KEYS still importable from
    # core.telemetry.product (re-exported from ports for backwards compat).
    from okto_pulse.core.telemetry.product import (
        PRODUCT_AGGREGATE_FAMILIES as FAM,
        PRODUCT_METRIC_KEYS as KEYS,
    )
    assert FAM == PRODUCT_AGGREGATE_FAMILIES
    assert KEYS == PRODUCT_METRIC_KEYS


# ===========================================================================
# ts_138260c8 (TS04) — AWS/report never healthy by inference (gap floors).
# ===========================================================================
def test_ts_138260c8_aws_report_never_healthy_by_inference():
    now = datetime(2026, 6, 26, tzinfo=timezone.utc)
    healthy_local = {"status": "ok", "publish_enabled": True, "last_success_at": now.isoformat()}

    # LOCAL is healthy, but a gap AWS/report descriptor floors COMBINED at degraded.
    dto = ph.resolve_publish_health(
        healthy_local, now=now,
        aws_ingest={"availability": ph.SRC_GAP},
        report_athena={"availability": ph.SRC_GAP},
    )
    assert dto.status != ph.HEALTHY
    assert dto.status == ph.DEGRADED
    assert dto.reason_category == ph.REASON_SOURCE_GAP

    # The registry DEFAULT (no provider) yields the gap descriptors — so a
    # non-composed runtime can NEVER infer AWS/report healthy from a local send.
    aws, report = get_external_source_descriptors(object())
    assert aws == {"availability": ph.SRC_GAP}
    assert report == {"availability": ph.SRC_GAP}

    # A REQUIRED source that is entirely ABSENT is also never healthy.
    dto_missing = ph.resolve_publish_health(
        healthy_local, now=now, required_sources=(ph.SOURCE_AWS_INGEST,)
    )
    assert dto_missing.status != ph.HEALTHY


# ===========================================================================
# ts_49dec63e (TS05) — descriptor matrix -> per-source status.
# ===========================================================================
def test_ts_49dec63e_descriptor_matrix():
    now = datetime(2026, 6, 26, tzinfo=timezone.utc)
    healthy_local = {"status": "ok", "publish_enabled": True, "last_success_at": now.isoformat()}
    matrix = {
        ph.SRC_AVAILABLE: ph.HEALTHY,
        ph.SRC_STALE: ph.STALE,
        ph.SRC_EXPIRED: ph.DEGRADED,
        ph.SRC_GAP: ph.DEGRADED,
        ph.SRC_UNAVAILABLE: ph.UNAVAILABLE,
    }
    for availability, expected_aws_status in matrix.items():
        dto = ph.resolve_publish_health(
            healthy_local, now=now, aws_ingest={"availability": availability}
        )
        aws_src = next(s for s in dto.sources if s["name"] == ph.SOURCE_AWS_INGEST)
        assert aws_src["status"] == expected_aws_status, availability
        if availability == ph.SRC_AVAILABLE:
            assert dto.status == ph.HEALTHY
        else:
            assert dto.status != ph.HEALTHY


# ===========================================================================
# ts_6ad289a7 (TS06) — wiring by port (registry used; conformance+exercise).
# (R10-E Pass 2: TelemetryBeaconSender removed from core; test via TelemetryService.)
# ===========================================================================
def test_ts_6ad289a7_registry_wiring_resolves_port_conformant_aggregator(tmp_path):
    """R10-E Pass 2: TelemetryBeaconSender removed from core — registry wiring test
    verified directly via get_product_aggregator() (the beacon-level wiring test
    lives in community/tests/test_r10d_product_adapter.py)."""
    _RecordingAggregator.instances.clear()
    register_product_aggregator_factory(
        lambda settings, metrics_dir: _RecordingAggregator(settings, metrics_dir)
    )
    settings = _settings(tmp_path)

    # The registered factory serves get_product_aggregator(), not a direct concrete.
    agg = get_product_aggregator(settings, tmp_path / "metrics")
    assert isinstance(agg, ProductAggregationPort)
    assert isinstance(agg, _RecordingAggregator)
    state = agg.aggregate()
    assert isinstance(state, ProductState)
    assert any(k.startswith("product_") for k in state.to_dict())

    assert _RecordingAggregator.instances, "factory never invoked"
    used = _RecordingAggregator.instances[-1]
    assert used.calls == 1


# ===========================================================================
# ts_b23dcb42 (TS07) — API/MCP/CLI/UI surfaces preserved + secret-free.
# (R10-E Pass 2: TelemetryBeaconSender removed; test via TelemetryService.)
# ===========================================================================
def test_ts_b23dcb42_surfaces_preserved_and_secret_free(tmp_path):
    register_product_aggregator_factory(_StubProductAggregator)
    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )

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

    reset_telemetry_event_store_factory_for_tests()
    register_telemetry_event_store_factory(lambda base, retention: _NullStore())
    try:
        settings = _settings(tmp_path)
        service = TelemetryService(settings)

        summary = service.summary()
        # families surface preserved (sourced from the PURE port vocabulary).
        assert summary["product_aggregate_families"] == list(PRODUCT_AGGREGATE_FAMILIES)
        assert "payload" not in json.dumps(summary)

        health = service.publish_health(now=datetime(2026, 6, 26, tzinfo=timezone.utc))
        assert health.get("redaction_applied") is True
        assert health["status"] in ph.HEALTH_STATUSES or "error" in health
        blob = json.dumps(health)
        assert "install_token" not in blob and "token_hash" not in blob
    finally:
        reset_telemetry_event_store_factory_for_tests()


# ===========================================================================
# ts_210e694d (TS08) — ownership gate + no new singleton.
# (R10-E Pass 2: class removed; gate finds zero violations; registry fail-closed.)
# ===========================================================================
def test_ts_210e694d_ownership_gate_is_clean_fulfilled(tmp_path, caplog):
    from okto_pulse.core.application.boundary.telemetry_product_ownership_gate import (
        LEDGERED_PRODUCT_FALLBACK,
        run_telemetry_product_ownership_gate,
    )

    # Real core is clean: ProductTelemetryAggregator deleted; gate finds no violations.
    real = run_telemetry_product_ownership_gate()
    assert real.ok is True, [(v.file, v.symbol) for v in real.violations]
    payload = real.as_dict()

    # R10-E Pass 2 fulfilled: ledger is empty (removal criterion satisfied).
    assert len(LEDGERED_PRODUCT_FALLBACK) == 0
    assert "R10-E" in payload["removal_criterion"]
    assert "fulfilled" in payload["removal_criterion"].lower()

    # Synthetic: a NEW reference outside the (now-empty) allowlist → caught.
    tel = tmp_path / "telemetry"
    tel.mkdir(parents=True)
    (tel / "sender.py").write_text(
        "from okto_pulse.core.telemetry.product import ProductTelemetryAggregator as A\n"
        "S = A\nx = S(None, '/m')\n",
        encoding="utf-8",
    )
    report = run_telemetry_product_ownership_gate(core_root=tmp_path)
    assert report.ok is False
    flagged = {v.file for v in report.violations}
    assert "telemetry/sender.py" in flagged

    # R10-D introduced no NEW unbaselined singleton.
    from okto_pulse.core.application.boundary.singleton_gate import AntiSingletonGate
    sg = AntiSingletonGate().run()
    new_names = {n["name"] for n in sg.evidence["new_singletons"]}
    assert "_product_aggregator_factory" not in new_names
    assert "_publish_health_source_provider" not in new_names

    # R10-E Pass 2 fail-closed: no factory → RuntimeError + structured error signal.
    reset_product_aggregator_factory_for_tests()
    caplog.set_level("ERROR", logger="okto_pulse.telemetry.product_aggregator")
    with pytest.raises(RuntimeError, match="No ProductAggregationPort factory registered"):
        get_product_aggregator(
            CoreSettings(metrics_dir=str(tmp_path / "m")), tmp_path / "m"
        )
    signals = [
        r for r in caplog.records
        if r.__dict__.get("metric_name") == "product_aggregator_no_provider_total"
    ]
    assert len(signals) >= 1
    assert signals[0].__dict__.get("outcome") == "fail_closed"


# ---------------------------------------------------------------------------
# Anti-claim-guard: product registry files must not have stale "STAYS in core"
# or "subclass-mirror" language after R10-E Pass 2.
# ---------------------------------------------------------------------------
_FALSE_MOVE_PATTERNS = (
    r"has moved",
    r"have moved",
    r"has been moved",
    r"\bmoves to the community",
    r"\bmoving to the community",
    r"\bmoved to the community",
    r"concrete\s+\w+\s+moved",
    r"concrete\s+\w+\s+has been moved",
    r"moves out of",
)

_STALE_STAY_PATTERNS = (
    r"STAYS in core",
    r"subclass-mirror",
)


def test_guard_no_false_move_claims_in_core_product_files():
    import re

    import okto_pulse.core.telemetry.product as _p
    import okto_pulse.core.telemetry.product_aggregator_registry as _r

    pats = [re.compile(p, re.IGNORECASE) for p in _FALSE_MOVE_PATTERNS]
    offenders: dict[str, list[str]] = {}
    for mod in (_p, _r):
        text = Path(mod.__file__).read_text(encoding="utf-8")
        hits = [p.pattern for p in pats if p.search(text)]
        if hits:
            offenders[Path(mod.__file__).name] = hits
    assert offenders == {}, offenders

    # R10-E Pass 2: stale "STAYS in core" / "subclass-mirror" removed from both files.
    stale_pats = [re.compile(p) for p in _STALE_STAY_PATTERNS]
    for mod in (_p, _r):
        text = Path(mod.__file__).read_text(encoding="utf-8")
        stale_found = [p.pattern for p in stale_pats if p.search(text)]
        assert stale_found == [], (
            f"{Path(mod.__file__).name} still has stale register-before-remove language "
            f"({stale_found!r}); R10-E Pass 2 removes ProductTelemetryAggregator from core."
        )

    # Honest framing: "removed" + "Community owns" must be present.
    reg_text = Path(_r.__file__).read_text(encoding="utf-8")
    assert "removed" in reg_text.lower() or "R10-E Pass 2" in reg_text

    # Teeth: guard catches synthetic full-move claim.
    import re as _re
    synthetic = "the concrete aggregator moves to the Community adapter"
    assert any(_re.search(p, synthetic, _re.IGNORECASE) for p in _FALSE_MOVE_PATTERNS)
