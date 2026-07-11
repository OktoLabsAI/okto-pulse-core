"""R10-B — TelemetryEventStore PORT + store-access rewire + ownership gate.
(Updated for R10-E Pass 2: LocalTelemetryStore removed from core.)

Scenario mapping:

  TS01 — port imports PURE (subprocess: no telemetry runtime / requests / sqlite3).
  TS02 — Protocol conformance (runtime_checkable); a partial impl is rejected.
  TS03 — TelemetryService.record_event obtains the store through the registered
         factory; public shape is byte-identical.
  TS04 — TelemetryService.store() goes through the factory (TelemetryBeaconSender
         removed from core in R10-E Pass 2 — concrete wiring tests in community).
  TS05 — consent gate preserved end-to-end through the port (disabled / anonymous_beacon).
  TS06 — confirmation-ledger: confirmed ids survive; a confirmed event never
         re-enters pending; idempotent duplicate confirmation.
  TS07 — retention/export/purge + path-guard (PATH_OUTSIDE_METRICS_DIR).
  TS08 — redaction + closed schema applied BEFORE the store sees the event.
  TS09 — registry is FAIL-CLOSED (R10-E Pass 2: no fallback to LocalTelemetryStore);
         gate is clean (empty allowlist; removal fulfilled; synthetic violations still caught).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.ports.telemetry import TelemetryEventStore, TelemetryStateStore
from okto_pulse.core.telemetry import event_store_registry as registry
from okto_pulse.core.telemetry.event_store_registry import (
    get_telemetry_event_store,
    register_telemetry_event_store_factory,
    reset_telemetry_event_store_factory_for_tests,
)
from okto_pulse.core.telemetry.schema import CURRENT_SCHEMA_VERSION
from okto_pulse.core.telemetry.service import TelemetryService


# ---------------------------------------------------------------------------
# Inline EventStore for core tests (R10-E Pass 2: LocalTelemetryStore removed).
# The behavioral contract is tested by community/tests/test_r10b_telemetry_store_adapter.py.
# ---------------------------------------------------------------------------


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _ensure_inside(base: Path, candidate: Path) -> Path:
    base_r = base.resolve()
    candidate_r = candidate.resolve()
    if candidate_r != base_r and base_r not in candidate_r.parents:
        raise ValueError("PATH_OUTSIDE_METRICS_DIR")
    return candidate_r


class _SimpleFileEventStore:
    """Lightweight conformant EventStore for core test isolation (inline, no community dep)."""

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
        for d in (
            self.metrics_dir,
            self.events_dir,
            self.sent_dir,
            self.failures_dir,
            self.exports_dir,
            self.snapshots_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

    def append_event(self, event: dict[str, Any]) -> Path:
        self._ensure_dirs()
        dt = (
            str(event.get("occurred_at", ""))[:10]
            or datetime.now(timezone.utc).date().isoformat()
        )
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
        dt = (
            str(record.get("snapshot_at", ""))[:10]
            or datetime.now(timezone.utc).date().isoformat()
        )
        path = self.snapshots_dir / f"snapshot-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
        return path

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return []
        result = []
        for line in lines:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                result.append(obj)
        return result

    def confirmed_event_ids(self) -> set[str]:
        confirmed: set[str] = set()
        if not self.sent_dir.exists():
            return confirmed
        for path in sorted(self.sent_dir.glob("sent-*.jsonl")):
            for record in self._read_jsonl(path):
                for eid in record.get("confirmed_event_ids") or []:
                    if isinstance(eid, str) and eid:
                        confirmed.add(eid)
        return confirmed

    def iter_events(self, *, since: datetime | None = None) -> Iterable[dict[str, Any]]:
        if not self.events_dir.exists():
            return
        for path in sorted(self.events_dir.glob("events-*.jsonl")):
            for event in self._read_jsonl(path):
                occurred = _parse_iso(str(event.get("occurred_at", "")))
                if since and occurred and occurred < since:
                    continue
                yield event

    def summarize(self, *, window_days: int = 30) -> dict[str, Any]:
        by_type: Counter[str] = Counter()
        by_day: Counter[str] = Counter()
        files = 0
        for _ in (
            self.events_dir.glob("events-*.jsonl") if self.events_dir.exists() else []
        ):
            files += 1
        for event in self.iter_events():
            by_type[str(event.get("event_type", "unknown"))] += 1
            day = str(event.get("occurred_at", ""))[:10]
            if day:
                by_day[day] += 1
        return {
            "event_count": sum(by_type.values()),
            "by_event_type": dict(sorted(by_type.items())),
            "by_day": dict(sorted(by_day.items())),
            "guided_help_counts": {},
            "files_count": files,
        }

    @staticmethod
    def _file_date(path: Path):
        try:
            return datetime.strptime(
                "-".join(path.stem.split("-")[-3:]), "%Y-%m-%d"
            ).date()
        except ValueError:
            return None

    def prune_old(self, *, now: datetime | None = None) -> dict[str, int]:
        from datetime import timedelta

        reference = (now or datetime.now(timezone.utc)).date()
        cutoff = reference - timedelta(days=self.retention_days)
        confirmed = self.confirmed_event_ids()
        removed_confirmed = 0
        preserved_pending = 0
        if self.events_dir.exists():
            for path in sorted(self.events_dir.glob("events-*.jsonl")):
                _ensure_inside(self.metrics_dir, path)
                fd = self._file_date(path)
                if fd is None or fd >= cutoff:
                    continue
                events = self._read_jsonl(path)
                pending = [
                    e for e in events if str(e.get("event_id") or "") not in confirmed
                ]
                removed_confirmed += len(events) - len(pending)
                preserved_pending += len(pending)
                if pending:
                    tmp = path.with_suffix(".tmp")
                    with tmp.open("w", encoding="utf-8", newline="\n") as out:
                        for e in pending:
                            out.write(json.dumps(e, sort_keys=True) + "\n")
                    tmp.replace(path)
                else:
                    path.unlink(missing_ok=True)
        return {
            "removed_confirmed_events": removed_confirmed,
            "preserved_pending_events": preserved_pending,
            "pruned_ledger_ids": 0,
            "removed_sent_files": 0,
            "removed_failure_files": 0,
        }

    def export_events(self, output_path: Path | None = None) -> Path:
        self._ensure_dirs()
        if output_path is None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_path = self.exports_dir / f"metrics-export-{stamp}.jsonl"
        output_path = _ensure_inside(self.metrics_dir, output_path)
        with output_path.open("w", encoding="utf-8", newline="\n") as out:
            for event in self.iter_events():
                out.write(json.dumps(event, sort_keys=True) + "\n")
        return output_path

    def purge_events(self) -> dict[str, int]:
        self._ensure_dirs()
        removed = 0
        for root in (
            self.events_dir,
            self.sent_dir,
            self.failures_dir,
            self.exports_dir,
        ):
            _ensure_inside(self.metrics_dir, root)
            if root.exists():
                for p in root.glob("*"):
                    _ensure_inside(self.metrics_dir, p)
                    if p.is_file():
                        p.unlink()
                        removed += 1
                    elif p.is_dir():
                        shutil.rmtree(p)
                        removed += 1
        return {"purged_files": removed}


class _RecordingEventStore(_SimpleFileEventStore):
    """A real store that records which methods the runtime called (proves the
    runtime goes THROUGH the registered factory, not a direct concrete)."""

    instances: list["_RecordingEventStore"] = []

    def __init__(self, metrics_dir: Path, retention_days: int = 30):
        super().__init__(metrics_dir, retention_days)
        self.calls: list[str] = []
        _RecordingEventStore.instances.append(self)

    def append_event(self, event):
        self.calls.append("append_event")
        return super().append_event(event)

    def iter_events(self, *, since=None):
        self.calls.append("iter_events")
        return super().iter_events(since=since)

    def confirmed_event_ids(self):
        self.calls.append("confirmed_event_ids")
        return super().confirmed_event_ids()


@pytest.fixture(autouse=True)
def _isolate_factory():
    """No registered factory leaks across tests."""
    reset_telemetry_event_store_factory_for_tests()
    try:
        yield
    finally:
        reset_telemetry_event_store_factory_for_tests()


@pytest.fixture
def _simple_store_factory():
    """Register _SimpleFileEventStore as the test factory."""
    register_telemetry_event_store_factory(_SimpleFileEventStore)
    yield
    reset_telemetry_event_store_factory_for_tests()


def _settings(tmp_path: Path, **overrides) -> CoreSettings:
    values = {"metrics_dir": str(tmp_path / "metrics"), "metrics_mode": ""}
    values.update(overrides)
    return CoreSettings(**values)


# ===========================================================================
# TS01 — pure import + protocol separation.
# ===========================================================================
def test_ts01_port_pure_import_and_separation():
    code = (
        "import sys\n"
        "from okto_pulse.core.ports import TelemetryEventStore, TelemetryStateStore\n"
        "heavy = [m for m in ("
        "'okto_pulse.core.telemetry.service','okto_pulse.core.telemetry.store',"
        "'okto_pulse.core.telemetry.sender','requests','sqlite3') "
        "if m in sys.modules]\n"
        "assert heavy == [], heavy\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout

    # EventStore and StateStore are SEPARATE runtime_checkable contracts.
    class _StateOnly:
        def load_state(self):
            return None

        def save_state(self, state):
            return None

    class _EventOnly:
        def append_event(self, event): ...

        def append_sent(self, record, *, failed=False): ...

        def append_snapshot(self, record): ...

        def confirmed_event_ids(self):
            return set()

        def iter_events(self, *, since=None):
            return ()

        def summarize(self, *, window_days=30):
            return {}

        def prune_old(self, *, now=None):
            return {}

        def export_events(self, output_path=None): ...

        def purge_events(self):
            return {}

    assert isinstance(_StateOnly(), TelemetryStateStore)
    assert not isinstance(_StateOnly(), TelemetryEventStore)
    assert isinstance(_EventOnly(), TelemetryEventStore)
    assert not isinstance(_EventOnly(), TelemetryStateStore)

    # The reconciled StateStore docstring no longer claims event persistence.
    doc = TelemetryStateStore.__doc__ or ""
    assert "event" not in doc.lower() or "does NOT own" in doc


# ===========================================================================
# TS02 — conformance (isinstance + exercise) + partial rejection.
# ===========================================================================
def test_ts02_conformance_isinstance_and_exercise(tmp_path):
    # R10-E Pass 2: LocalTelemetryStore removed from core. We verify the PORT
    # conformance contract via the inline _SimpleFileEventStore (same protocol).
    store = _SimpleFileEventStore(tmp_path / "metrics", 30)
    assert isinstance(store, TelemetryEventStore)

    # Exercise EVERY method on the store (not just structural typing).
    now = datetime.now(timezone.utc)
    occurred = now.isoformat()
    ev = {
        "schema_version": CURRENT_SCHEMA_VERSION,
        "event_type": "cli",
        "occurred_at": occurred,
        "event_id": "ev-1",
        "payload": {"command": "serve"},
    }
    assert store.append_event(ev).suffix == ".jsonl"
    assert [e["event_id"] for e in store.iter_events()] == ["ev-1"]
    assert store.summarize()["event_count"] == 1
    sent = store.append_sent({"sent_at": occurred, "confirmed_event_ids": ["ev-1"]})
    assert sent.suffix == ".jsonl"
    assert store.confirmed_event_ids() == {"ev-1"}
    assert (
        store.append_snapshot({"snapshot_at": occurred, "metrics": {}}).suffix
        == ".jsonl"
    )
    exported = store.export_events()
    assert exported.exists()
    assert isinstance(store.prune_old(now=now), dict)
    assert isinstance(store.purge_events(), dict)

    # A partial impl (missing methods) is NOT a TelemetryEventStore.
    class _Partial:
        def append_event(self, event): ...

    assert not isinstance(_Partial(), TelemetryEventStore)


# ===========================================================================
# TS03 — service goes through the registered factory; shape identical.
# ===========================================================================
def test_ts03_service_uses_registered_store_same_shape(tmp_path):
    _RecordingEventStore.instances.clear()
    register_telemetry_event_store_factory(_RecordingEventStore)
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    service = TelemetryService(settings)

    result = service.record_event("cli", {"command": "serve", "exit_code": 0})

    # Public shape is byte-identical to the pre-rewire contract.
    assert set(result) == {
        "written",
        "mode",
        "file",
        "event_id",
        "rejected_fields_count",
        "schema_version",
    }
    assert result["written"] is True
    assert result["schema_version"] == CURRENT_SCHEMA_VERSION
    # The registered store actually served it.
    assert _RecordingEventStore.instances, "factory was never invoked"
    assert any("append_event" in s.calls for s in _RecordingEventStore.instances)
    assert all(
        isinstance(s, TelemetryEventStore) for s in _RecordingEventStore.instances
    )


# ===========================================================================
# TS04 — TelemetryService.store() goes through the factory.
# (TelemetryBeaconSender removed from core in R10-E Pass 2; concrete wiring
# tests live in community/tests/test_r10c_telemetry_sender_adapter.py.)
# ===========================================================================
def test_ts04_service_store_via_factory(tmp_path):
    _RecordingEventStore.instances.clear()
    register_telemetry_event_store_factory(_RecordingEventStore)
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    service = TelemetryService(settings)
    service.record_event("cli", {"command": "serve"})

    # TelemetryService.store() resolves through the registry, not a direct concrete.
    store = service.store()
    assert isinstance(store, _RecordingEventStore)
    assert isinstance(store, TelemetryEventStore)

    # Pending = events with an id NOT in confirmed_event_ids (computed via the port).
    confirmed = store.confirmed_event_ids()
    pending = [
        e for e in store.iter_events() if str(e.get("event_id") or "") not in confirmed
    ]
    assert len(pending) == 1
    assert "iter_events" in store.calls and "confirmed_event_ids" in store.calls


# ===========================================================================
# TS05 — consent gate preserved end-to-end through the port.
# ===========================================================================
def test_ts05_consent_replay_through_port(tmp_path, _simple_store_factory):
    # disabled -> no write
    disabled = TelemetryService(_settings(tmp_path / "a", metrics_mode="disabled"))
    r = disabled.record_event("cli", {"command": "serve"})
    assert r["written"] is False

    # anonymous_beacon -> writes through the port
    on = TelemetryService(_settings(tmp_path / "c", metrics_mode="anonymous_beacon"))
    r3 = on.record_event("cli", {"command": "serve"})
    assert r3["written"] is True
    assert list((tmp_path / "c" / "metrics").glob("events/*.jsonl"))


# ===========================================================================
# TS06 — confirmation ledger: durable + non-replay + idempotent.
# ===========================================================================
def test_ts06_confirmation_ledger_non_replay(tmp_path, _simple_store_factory):
    store = get_telemetry_event_store(tmp_path / "metrics", 30)
    occurred = datetime.now(timezone.utc).isoformat()
    for eid in ("e1", "e2"):
        store.append_event(
            {
                "schema_version": CURRENT_SCHEMA_VERSION,
                "event_type": "cli",
                "occurred_at": occurred,
                "event_id": eid,
                "payload": {"command": "x"},
            }
        )

    # Confirm e1 (twice -> idempotent set).
    store.append_sent({"sent_at": occurred, "confirmed_event_ids": ["e1"]})
    store.append_sent({"sent_at": occurred, "confirmed_event_ids": ["e1"]})
    assert store.confirmed_event_ids() == {"e1"}

    # Non-replay: a confirmed event is excluded from pending.
    confirmed = store.confirmed_event_ids()
    pending = [
        e["event_id"] for e in store.iter_events() if e["event_id"] not in confirmed
    ]
    assert pending == ["e2"]


# ===========================================================================
# TS07 — retention/export/purge + path-guard preserved through the port.
# ===========================================================================
def test_ts07_retention_export_purge_path_guard(tmp_path, _simple_store_factory):
    from datetime import timedelta

    metrics = tmp_path / "metrics"
    store = get_telemetry_event_store(metrics, 30)
    now = datetime(2026, 6, 26, tzinfo=timezone.utc)
    old_day = (now - timedelta(days=120)).date().isoformat()

    # Two old events: one confirmed (prunable), one pending (preserved).
    for eid in ("old-confirmed", "old-pending"):
        store.append_event(
            {
                "schema_version": CURRENT_SCHEMA_VERSION,
                "event_type": "cli",
                "occurred_at": f"{old_day}T00:00:00+00:00",
                "event_id": eid,
                "payload": {"command": "x"},
            }
        )
    store.append_sent(
        {
            "sent_at": f"{old_day}T01:00:00+00:00",
            "confirmed_event_ids": ["old-confirmed"],
        }
    )

    stats = store.prune_old(now=now)
    assert stats["removed_confirmed_events"] == 1
    assert stats["preserved_pending_events"] == 1
    remaining = {e["event_id"] for e in store.iter_events()}
    assert remaining == {"old-pending"}

    # export_local writes inside metrics_dir.
    out = store.export_events()
    assert out.exists() and metrics.resolve() in out.resolve().parents

    # Path-guard: export OUTSIDE metrics_dir raises PATH_OUTSIDE_METRICS_DIR.
    with pytest.raises(ValueError, match="PATH_OUTSIDE_METRICS_DIR"):
        store.export_events(tmp_path / "escape.jsonl")

    # purge clears the local store.
    store.purge_events()
    assert list(store.iter_events()) == []


# ===========================================================================
# TS08 — redaction + closed schema BEFORE the store sees the event.
# ===========================================================================
def test_ts08_redaction_and_closed_schema_before_store(tmp_path, _simple_store_factory):
    settings = _settings(tmp_path, metrics_mode="anonymous_beacon")
    service = TelemetryService(settings)

    result = service.record_event(
        "http",
        {
            "method": "GET",
            "route_template": "/api/v1/cards/{card_id}",
            "status_code": 200,
            "board_id": "9ec5f06f-2028-42a7-81fd-3ad36f98a89d",
            "title": "secret",
            "email": "dev@example.com",
            "path": "D:\\Projects\\private",
        },
    )
    assert result["written"] is True
    assert result["rejected_fields_count"] >= 4

    # The PERSISTED event carries only the allowed closed-schema keys.
    ev_file = next((tmp_path / "metrics" / "events").glob("events-*.jsonl"))
    event = json.loads(ev_file.read_text(encoding="utf-8").splitlines()[0])
    assert event["payload"] == {
        "method": "GET",
        "route_template": "/api/v1/cards/{card_id}",
        "status_code": 200,
    }
    blob = json.dumps(event)
    for secret in ("secret", "dev@example.com", "9ec5f06f", "D:\\Projects"):
        assert secret not in blob

    # Unknown event_type is rejected before any store write.
    unknown = service.record_event("ghost_event", {"x": 1})
    assert unknown["written"] is False


# ===========================================================================
# TS09 — R10-E Pass 2: registry fail-closed + gate empty allowlist.
# ===========================================================================
def test_ts09_registry_is_fail_closed(tmp_path, caplog):
    """R10-E Pass 2: no factory → RuntimeError + structured error signal (fail-closed)."""
    reset_telemetry_event_store_factory_for_tests()
    assert registry._factory is None
    caplog.set_level("ERROR", logger="okto_pulse.telemetry.event_store")

    with pytest.raises(RuntimeError, match="No TelemetryEventStore factory registered"):
        get_telemetry_event_store(tmp_path / "metrics", 30)

    signals = [
        r
        for r in caplog.records
        if r.__dict__.get("metric_name") == "telemetry_event_store_no_provider_total"
    ]
    assert len(signals) == 1
    assert signals[0].__dict__.get("outcome") == "fail_closed"
    assert signals[0].__dict__.get("reason") == "no_factory_registered"


def test_ts09_ownership_gate_is_clean_fulfilled(tmp_path):
    """R10-E Pass 2 fulfilled: real core has ZERO LocalTelemetryStore references
    (class removed); gate passes with empty allowlist."""
    from okto_pulse.core.application.boundary.telemetry_store_ownership_gate import (
        LEDGERED_STORE_FALLBACK,
        run_telemetry_store_ownership_gate,
    )

    # Real core is clean: no LocalTelemetryStore references (class deleted).
    real = run_telemetry_store_ownership_gate()
    assert real.ok is True, [(v.file, v.symbol) for v in real.violations]

    # Gate ledger is empty (removal fulfilled — no longer gating shim locations).
    report = real.as_dict()
    assert report["ok"] is True
    assert len(LEDGERED_STORE_FALLBACK) == 0
    assert "R10-E" in report["removal_criterion"]
    assert "fulfilled" in report["removal_criterion"].lower()

    # Synthetic tree: a NEW reference outside the (now-empty) allowlist is caught.
    svc = tmp_path / "telemetry"
    svc.mkdir(parents=True)
    (svc / "service.py").write_text(
        "from okto_pulse.core.telemetry.store import LocalTelemetryStore\n"
        "def store():\n    return LocalTelemetryStore('/m', 30)\n",
        encoding="utf-8",
    )
    (svc / "sender.py").write_text(
        "from okto_pulse.core.telemetry.store import LocalTelemetryStore as LTS\n"
        "S = LTS\nT = S\nx = T('/m', 30)\n",
        encoding="utf-8",
    )
    report_syn = run_telemetry_store_ownership_gate(core_root=tmp_path)
    assert report_syn.ok is False
    flagged = {v.file for v in report_syn.violations}
    assert "telemetry/service.py" in flagged
    assert "telemetry/sender.py" in flagged
