"""R10-A — telemetry core PORTS + DTOs + alias-aware gates (ports-only).

Scenario mapping (1:1 with the spec ts_ ids):
  ts_16fde17c (TS01) — the contracts import in ISOLATION without the telemetry
       runtime (subprocess: no TelemetryService / LocalTelemetryStore /
       ProductTelemetryAggregator / requests / sqlite3 in sys.modules).
  ts_9c9eea2e (TS02) — Protocol conformance (runtime_checkable) + the DTOs MIRROR
       the live serialization (record_event / publish_health) bit-for-bit.
  ts_afca2df9 (TS03, negative) — the alias-aware import gate BLOCKS an aliased /
       assignment-chain bypass of TelemetryService outside the allowlist.
  ts_f93a8b2b (TS04, negative) — the event-schema gate detects a PHANTOM
       event_type / out-of-schema payload key; the real closed schema is ok.
  ts_8ea4f7f6 (TS05) — the dependency audit proves R08 auth does NOT import
       telemetry and core.ports is the single contract entry-point.
  ts_cee8e5d0 (TS06) — baseline telemetry stays GREEN: a real record_event /
       summary / publish_health round-trip through the DTOs unchanged.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from okto_pulse.core.ports import (
    HEALTH_REPORT_FIELDS,
    HealthReport,
    ProductAggregationPort,
    ProductState,
    PublishHealthSource,
    TelemetryPort,
    TelemetryResult,
    TelemetrySink,
    TelemetryState,
    TelemetryStateStore,
)


class _SimpleFileEventStore:
    """Minimal conformant TelemetryEventStore for core test isolation (no community dep)."""

    def __init__(self, metrics_dir: Path, retention_days: int = 30):
        self.metrics_dir = Path(metrics_dir).resolve()
        self.retention_days = retention_days

    @property
    def events_dir(self) -> Path:
        return self.metrics_dir / "events"

    @property
    def sent_dir(self) -> Path:
        return self.metrics_dir / "sent"

    @property
    def failures_dir(self) -> Path:
        return self.metrics_dir / "failures"

    @property
    def exports_dir(self) -> Path:
        return self.metrics_dir / "exports"

    @property
    def snapshots_dir(self) -> Path:
        return self.metrics_dir / "snapshots"

    def _ensure_dirs(self) -> None:
        for d in (self.metrics_dir, self.events_dir, self.sent_dir,
                  self.failures_dir, self.exports_dir, self.snapshots_dir):
            d.mkdir(parents=True, exist_ok=True)

    def append_event(self, event: dict[str, Any]) -> Path:
        self._ensure_dirs()
        dt = str(event.get("occurred_at", ""))[:10] or datetime.now(timezone.utc).date().isoformat()
        path = self.events_dir / f"events-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
        return path

    def append_sent(self, record: dict[str, Any], *, failed: bool = False) -> Path:
        self._ensure_dirs()
        root = self.failures_dir if failed else self.sent_dir
        dt = str(record.get("sent_at") or record.get("failed_at") or "")[:10]
        if not dt:
            dt = datetime.now(timezone.utc).date().isoformat()
        path = root / f"{'failures' if failed else 'sent'}-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
        return path

    def append_snapshot(self, record: dict[str, Any]) -> Path:
        self._ensure_dirs()
        dt = str(record.get("snapshot_at", ""))[:10] or datetime.now(timezone.utc).date().isoformat()
        path = self.snapshots_dir / f"snapshot-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
        return path

    def confirmed_event_ids(self) -> set[str]:
        return set()

    def unsent_events(self, max_age_days: int = 90) -> list[dict[str, Any]]:
        return []

    def pending_snapshots(self) -> list[dict[str, Any]]:
        return []

    def summarize(self, *, window_days: int = 30) -> dict[str, Any]:
        return {
            "total_events": 0,
            "sent_events": 0,
            "pending_events": 0,
            "event_counts": {},
        }

    def export(self, dest: Path, *, window_days: int = 90) -> Path:
        dest.mkdir(parents=True, exist_ok=True)
        return dest


@pytest.fixture
def _telemetry_service():
    """A real TelemetryService bound to a TEMP metrics dir (never the user's
    ~/.okto-pulse/metrics).

    R10-E Pass 2: the event store registry is fail-closed; register
    _SimpleFileEventStore as the factory so record_event / summary work in
    core tests without importing community adapters.
    """
    import okto_pulse.core.infra.config as _config
    from okto_pulse.core.infra.config import CoreSettings
    from okto_pulse.core.telemetry.event_store_registry import (
        register_telemetry_event_store_factory,
        reset_telemetry_event_store_factory_for_tests,
    )
    from okto_pulse.core.telemetry.publish_health_source_registry import (
        register_external_source_provider,
        reset_external_source_provider_for_tests,
    )
    from okto_pulse.core.runtime_context import (
        capture_runtime_values_for_tests,
        restore_runtime_values_for_tests,
    )
    from okto_pulse.core.telemetry.service import TelemetryService

    saved_runtime = capture_runtime_values_for_tests()
    with tempfile.TemporaryDirectory() as tmp:
        settings = CoreSettings(metrics_dir=str(Path(tmp) / "metrics"))
        _config.configure_settings(settings)
        reset_telemetry_event_store_factory_for_tests()
        register_telemetry_event_store_factory(_SimpleFileEventStore)
        reset_external_source_provider_for_tests()
        register_external_source_provider(
            lambda _settings: (
                {"availability": "gap"},
                {"availability": "gap"},
            )
        )
        try:
            yield TelemetryService(settings)
        finally:
            reset_telemetry_event_store_factory_for_tests()
            reset_external_source_provider_for_tests()
            restore_runtime_values_for_tests(saved_runtime)


# ===========================================================================
# ts_16fde17c (TS01) — pure import in isolation (subprocess).
# ===========================================================================
def test_ts_16fde17c_contracts_import_without_runtime():
    code = (
        "import sys\n"
        "import okto_pulse.core.ports as p\n"
        "from okto_pulse.core.ports import (TelemetryPort, TelemetrySink, "
        "TelemetryStateStore, PublishHealthSource, ProductAggregationPort, "
        "TelemetryResult, TelemetryState, HealthReport, ProductState)\n"
        "heavy = [m for m in ("
        "'okto_pulse.core.telemetry.service','okto_pulse.core.telemetry.store',"
        "'okto_pulse.core.telemetry.sender','okto_pulse.core.telemetry.product',"
        "'requests','sqlite3') if m in sys.modules]\n"
        "assert heavy == [], heavy\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout


# ===========================================================================
# ts_9c9eea2e (TS02) — Protocol conformance + DTO serialization mirror.
# ===========================================================================
def test_ts_9c9eea2e_protocol_conformance_and_dto_mirror(_telemetry_service):
    svc = _telemetry_service

    # Protocol conformance (runtime_checkable, structural).
    assert isinstance(svc, TelemetryPort)

    class _Broken:
        def record_event(self, *a, **k):
            return None

    assert not isinstance(_Broken(), TelemetryPort)  # missing summary/publish_health

    # Default (no consent) -> disabled real result round-trips bit-for-bit.
    disabled_real = svc.record_event("cli", {"command": "serve", "exit_code": 0})
    assert disabled_real["written"] is False
    assert "file" not in disabled_real and "event_id" not in disabled_real
    assert TelemetryResult.from_dict(disabled_real).to_dict() == disabled_real

    # With recorded consent -> written success real result round-trips bit-for-bit.
    svc.update_settings(
        mode="anonymous_beacon", source="cli",
        policy_version="1.0.0", schema_version="1.1.0",
    )
    real = svc.record_event("cli", {"command": "serve", "exit_code": 0})
    assert real["written"] is True
    assert TelemetryResult.from_dict(real).to_dict() == real

    # HealthReport mirrors a REAL PublishHealth.to_dict (the pure resolver).
    from okto_pulse.core.telemetry import publish_health as ph

    dto = ph.resolve_publish_health(
        {"status": "ok", "publish_enabled": True,
         "last_success_at": "2026-06-25T00:00:00Z"},
        now=datetime(2026, 6, 25, 1, 0, 0, tzinfo=timezone.utc),
    )
    real_health = dto.to_dict()
    assert set(real_health) == set(HEALTH_REPORT_FIELDS)
    assert HealthReport.from_dict(real_health).to_dict() == real_health

    # TelemetryState + ProductState round-trip.
    st = {"mode": "anonymous_beacon", "source": "cli", "changed_at": "t",
          "acknowledged_items": ["a", "b"]}
    assert TelemetryState.from_dict(st).mode == "anonymous_beacon"
    assert TelemetryState.from_dict(st).acknowledged_items == ("a", "b")
    prod = {"mcp_tools": {"created": 3}, "boards": {"created": 1}}
    assert ProductState.from_dict(prod).to_dict() == prod


def test_ts_9c9eea2e_provider_protocols_are_runtime_checkable():
    # The 5 provider ports are runtime_checkable Protocols.
    for proto in (
        TelemetryPort, TelemetrySink, TelemetryStateStore,
        PublishHealthSource, ProductAggregationPort,
    ):
        assert not isinstance(object(), proto)  # structural rejection works


# ===========================================================================
# ts_afca2df9 (TS03, negative) — alias-aware import gate blocks bypass.
# ===========================================================================
def test_ts_afca2df9_import_gate_blocks_aliased_bypass(tmp_path):
    from okto_pulse.core.application.boundary.telemetry_import_gate import (
        run_telemetry_import_gate,
    )

    # Real core: ok (current importers allowlisted).
    assert run_telemetry_import_gate().ok is True

    # Synthetic core tree with NEW aliased / assignment-chain uses outside the
    # allowlist (R05-D style repro).
    svc_dir = tmp_path / "services"
    svc_dir.mkdir(parents=True)
    (svc_dir / "rogue.py").write_text(
        "from okto_pulse.core.telemetry.service import TelemetryService as TS\n"
        "def f():\n    return TS(None)\n",
        encoding="utf-8",
    )
    # R10-E Pass 2: LocalTelemetryStore removed from core — no longer in
    # SENSITIVE_SYMBOLS. A second rogue file using a different aliased pattern
    # still tests multi-file detection (rogue.py already covers the alias path;
    # rogue2.py here tests assignment-chain bypass with TelemetryService).
    (svc_dir / "rogue2.py").write_text(
        "from okto_pulse.core.telemetry.service import TelemetryService\n"
        "Svc = TelemetryService\nx = Svc(None)\n",
        encoding="utf-8",
    )
    # A file under the allowlisted telemetry/ prefix is NOT a violation.
    tel = tmp_path / "telemetry"
    tel.mkdir(parents=True)
    (tel / "internal.py").write_text(
        "from okto_pulse.core.telemetry.service import TelemetryService\n"
        "y = TelemetryService(None)\n",
        encoding="utf-8",
    )
    report = run_telemetry_import_gate(core_root=tmp_path)
    assert report.ok is False
    flagged = {(v.file, v.symbol) for v in report.violations}
    assert ("services/rogue.py", "TelemetryService") in flagged
    assert ("services/rogue2.py", "TelemetryService") in flagged
    assert "telemetry/internal.py" not in {v.file for v in report.violations}


# ===========================================================================
# ts_f93a8b2b (TS04, negative) — event-schema gate detects phantom.
# ===========================================================================
def test_ts_f93a8b2b_event_schema_gate_detects_phantom():
    from okto_pulse.core.telemetry.event_schema_gate import (
        run_telemetry_event_schema_gate,
        validate_event_call,
    )

    # The real closed vocabulary is consistent.
    assert run_telemetry_event_schema_gate().ok is True

    # Phantom event_type -> not ok (runtime classifier).
    phantom = validate_event_call("ghost_event", {"x": 1})
    assert phantom.ok is False and phantom.phantom_event_type is True

    # Out-of-schema payload key -> not ok (phantom key).
    bad_key = validate_event_call("cli", {"command": "x", "secret_token": "y"})
    assert bad_key.ok is False
    assert bad_key.phantom_event_type is False
    assert bad_key.rejected_keys == ("secret_token",)

    # A valid call passes.
    good = validate_event_call("cli", {"command": "serve", "exit_code": 0})
    assert good.ok is True and good.rejected_keys == ()


def test_ts_f93a8b2b_ast_scanner_blocks_rogue_record_event_call(tmp_path):
    # AC4/TS04: an AST SCANNER (not just the runtime helper) detects rogue
    # record_event(...) calls in a source tree.
    from okto_pulse.core.telemetry.event_schema_gate import (
        OUT_OF_SCHEMA_PAYLOAD_KEY,
        PHANTOM_EVENT_TYPE,
        scan_record_event_calls,
    )

    rogue_dir = tmp_path / "okto_pulse" / "core" / "services"
    rogue_dir.mkdir(parents=True)
    (rogue_dir / "rogue.py").write_text(
        "def emit(svc):\n"
        '    svc.record_event("nao_existe", {"chave_fantasma": 1})\n',
        encoding="utf-8",
    )
    report = scan_record_event_calls(tmp_path)
    assert report.ok is False
    assert report.scanned_calls == 1
    kinds = {(v["kind"], v["symbol"]) for v in report.violations}
    assert (PHANTOM_EVENT_TYPE, "nao_existe") in kinds
    assert (OUT_OF_SCHEMA_PAYLOAD_KEY, "chave_fantasma") in kinds

    # Dynamic args are advisory, never a failure.
    (rogue_dir / "dyn.py").write_text(
        "def emit(svc, et, pl):\n    svc.record_event(et, pl)\n",
        encoding="utf-8",
    )
    dyn_only = tmp_path / "dynonly"
    (dyn_only / "okto_pulse" / "core").mkdir(parents=True)
    (dyn_only / "okto_pulse" / "core" / "d.py").write_text(
        "def emit(svc, et, pl):\n    svc.record_event(et, pl)\n", encoding="utf-8"
    )
    dyn_report = scan_record_event_calls(dyn_only)
    assert dyn_report.ok is True  # dynamic-only -> no static violation
    assert dyn_report.advisory_count >= 1

    # The REAL core scans clean (every real call uses a valid literal or dynamic).
    import okto_pulse.core

    real = scan_record_event_calls(Path(okto_pulse.core.__file__).parent)
    assert real.ok is True, real.violations


# ===========================================================================
# ts_8ea4f7f6 (TS05) — dependency audit: R08 auth has no telemetry import.
# ===========================================================================
def test_ts_8ea4f7f6_dependency_audit_axis_independence(tmp_path):
    from okto_pulse.core.application.boundary.telemetry_dependency_audit import (
        audit_telemetry_axis_independence,
    )

    real = audit_telemetry_axis_independence()
    assert real["ok"] is True
    assert real["r08_auth_imports_telemetry"] == []
    assert real["telemetry_port_single_entry_point"] is True

    # Negative: a synthetic R08 auth module that DOES import telemetry fails.
    portsdir = tmp_path / "ports"
    portsdir.mkdir(parents=True)
    (portsdir / "mcp_auth.py").write_text(
        "from okto_pulse.core.telemetry.service import TelemetryService\n",
        encoding="utf-8",
    )
    bad = audit_telemetry_axis_independence(core_root=tmp_path)
    assert bad["ok"] is False
    assert "ports/mcp_auth.py" in bad["r08_auth_imports_telemetry"]


# ===========================================================================
# ts_cee8e5d0 (TS06) — baseline telemetry stays green (semantics unchanged).
# ===========================================================================
def test_ts_cee8e5d0_baseline_telemetry_green(_telemetry_service):
    svc = _telemetry_service
    svc.update_settings(
        mode="anonymous_beacon", source="cli",
        policy_version="1.0.0", schema_version="1.1.0",
    )

    # record_event still writes the closed-schema event + returns the same shape.
    result = svc.record_event("mcp", {"tool_name": "okto_pulse_get_board", "status": "success"})
    assert set(result) == {
        "written", "mode", "file", "event_id", "rejected_fields_count", "schema_version"
    }
    assert result["written"] is True
    # the DTO mirrors it (no semantic drift).
    assert TelemetryResult.from_dict(result).to_dict() == result

    # summary + publish_health still produce their surfaces (redacted, no error).
    summary = svc.summary(window_days=7)
    assert summary["schema_version"] == result["schema_version"]
    assert "publish_status" in summary  # R5A redacted projection preserved

    health = svc.publish_health(now=datetime(2026, 6, 25, tzinfo=timezone.utc))
    assert health.get("redaction_applied") is True
    # the publish-health surface conforms to the HealthReport field set.
    assert set(health) <= set(HEALTH_REPORT_FIELDS) | {"error"}
