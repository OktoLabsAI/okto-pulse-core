"""R10-C — TelemetrySink port/registry/guard (core side).
(Updated for R10-E Pass 2: TelemetryBeaconSender removed from core.)

Scenario mapping:
  ts_025f3226 (TS01) — port imports PURE; registry is FAIL-CLOSED (R10-E Pass 2:
       no fallback to TelemetryBeaconSender — raises RuntimeError + structured signal).
  + import-gate: the ownership gate BLOCKS a NEW concrete TelemetryBeaconSender
       (incl. aliased) or a NEW ``requests`` import in the telemetry runtime,
       outside the (now-empty) allowlist; gate reports "fulfilled" + "removed".
  + no new singleton; anti-claim-guard over the core sender files (no stale
       "STAYS in core" / "subclass-mirror" language after removal).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from okto_pulse.core.infra.config import CoreSettings
from okto_pulse.core.ports.telemetry import TelemetrySink
from okto_pulse.core.telemetry.sender_registry import (
    get_telemetry_sender,
    register_telemetry_sender_factory,
    reset_telemetry_sender_factory_for_tests,
)


@pytest.fixture(autouse=True)
def _isolate_factory():
    reset_telemetry_sender_factory_for_tests()
    try:
        yield
    finally:
        reset_telemetry_sender_factory_for_tests()


def _settings(tmp_path: Path) -> CoreSettings:
    return CoreSettings(metrics_dir=str(tmp_path / "metrics"), metrics_mode="")


# ===========================================================================
# ts_025f3226 (TS01) — pure port + runtime_checkable + fail-closed registry.
# ===========================================================================
def test_ts_025f3226_port_pure_and_registry_fail_closed(tmp_path, caplog):
    """R10-E Pass 2: registry has no factory → RuntimeError + structured error signal."""
    code = (
        "import sys\n"
        "from okto_pulse.core.ports import TelemetrySink\n"
        "heavy = [m for m in ('requests','okto_pulse.core.telemetry.sender',"
        "'okto_pulse.community') if m in sys.modules]\n"
        "assert heavy == [], heavy\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout

    assert not isinstance(object(), TelemetrySink)  # structural rejection works

    # R10-E Pass 2 fail-closed: no factory → RuntimeError + structured error signal.
    reset_telemetry_sender_factory_for_tests()
    caplog.set_level("ERROR", logger="okto_pulse.telemetry.sender_registry")

    with pytest.raises(RuntimeError, match="No TelemetrySink factory registered"):
        get_telemetry_sender(_settings(tmp_path))

    signals = [
        r for r in caplog.records
        if r.__dict__.get("metric_name") == "telemetry_sender_no_provider_total"
    ]
    assert len(signals) >= 1
    assert signals[0].__dict__.get("outcome") == "fail_closed"
    assert signals[0].__dict__.get("reason") == "no_factory_registered"


def test_registered_factory_short_circuits_fallback(tmp_path):
    marker = object()

    class _Sink:
        token = marker

        def send_pending(self):
            return {"sent": False, "reason": "stub"}

        def publish_product_snapshot(self):
            return {"sent": False, "reason": "stub"}

    register_telemetry_sender_factory(lambda settings: _Sink())
    resolved = get_telemetry_sender(_settings(tmp_path))
    assert isinstance(resolved, _Sink)
    assert resolved.token is marker  # the registered factory was used, not a fallback


# ===========================================================================
# import-gate — blocks a new concrete sender / requests outside the allowlist.
# ===========================================================================
def test_sender_ownership_gate_blocks_new_concrete_and_requests(tmp_path):
    from okto_pulse.core.application.boundary.telemetry_sender_ownership_gate import (
        LEDGERED_SENDER_FALLBACK,
        run_telemetry_sender_ownership_gate,
    )

    # Real core is clean (class removed; gate finds zero violations).
    real = run_telemetry_sender_ownership_gate()
    assert real.ok is True, [(v.file, v.symbol) for v in real.violations]
    payload = real.as_dict()

    # R10-E Pass 2 fulfilled: ledger is empty (removal criterion satisfied).
    assert len(LEDGERED_SENDER_FALLBACK) == 0
    assert "R10-E" in payload["removal_criterion"]
    assert "fulfilled" in payload["removal_criterion"].lower()

    # Synthetic core tree: a NEW concrete sender (aliased) + a NEW requests import
    # in the telemetry domain, outside the (now-empty) allowlist → violations.
    tel = tmp_path / "telemetry"
    tel.mkdir(parents=True)
    (tel / "rogue.py").write_text(
        "import requests\n"
        "from okto_pulse.core.telemetry.sender import TelemetryBeaconSender as S\n"
        "T = S\n"
        "def go(settings):\n    return T(settings, session=requests.Session())\n",
        encoding="utf-8",
    )
    report = run_telemetry_sender_ownership_gate(core_root=tmp_path)
    assert report.ok is False
    flagged = {(v.file, v.symbol) for v in report.violations}
    assert ("telemetry/rogue.py", "TelemetryBeaconSender") in flagged
    assert ("telemetry/rogue.py", "requests") in flagged


def test_no_new_singleton():
    from okto_pulse.core.application.boundary.singleton_gate import AntiSingletonGate
    sg = AntiSingletonGate().run()
    new_names = {n["name"] for n in sg.evidence["new_singletons"]}
    assert "_telemetry_sender_factory" not in new_names


# ===========================================================================
# anti-claim-guard — core sender files must not contain stale "STAYS in core"
# or "subclass-mirror" language after R10-E Pass 2 removal.
# ===========================================================================
_FALSE_MOVE_PATTERNS = (
    r"\bmoves to the community",
    r"\bmoving to the community",
    r"has moved",
    r"have moved",
    r"has been moved",
    r"\bmoved to the community",
    r"concrete\s+\w+\s+moved",
    r"moves out of",
)

_STALE_STAY_PATTERNS = (
    r"STAYS in core",
    r"subclass-mirror",
)


def test_guard_no_false_move_claims_in_core_sender_files():
    import re

    import okto_pulse.core.application.boundary.telemetry_sender_ownership_gate as _g
    import okto_pulse.core.telemetry.sender_registry as _r

    pats = [re.compile(p, re.IGNORECASE) for p in _FALSE_MOVE_PATTERNS]
    offenders: dict[str, list[str]] = {}
    for mod in (_r, _g):
        text = Path(mod.__file__).read_text(encoding="utf-8")
        hits = [p.pattern for p in pats if p.search(text)]
        if hits:
            offenders[Path(mod.__file__).name] = hits
    assert offenders == {}, offenders

    # R10-E Pass 2: stale "STAYS in core" / "subclass-mirror" language was removed
    # from sender_registry.py (TelemetryBeaconSender is deleted, it no longer STAYS).
    reg_text = Path(_r.__file__).read_text(encoding="utf-8")
    stale_pats = [re.compile(p) for p in _STALE_STAY_PATTERNS]
    stale_found = [p.pattern for p in stale_pats if p.search(reg_text)]
    assert stale_found == [], (
        "sender_registry.py still contains stale register-before-remove language "
        f"({stale_found!r}); R10-E Pass 2 removes TelemetryBeaconSender from core."
    )

    # Honest framing: "removed" + "Community owns" must be present post-Pass 2.
    assert "removed" in reg_text.lower() or "R10-E Pass 2" in reg_text

    # Teeth: the guard catches present/past/gerund full-move claims.
    for synthetic in (
        "the sender moves to the Community adapter",
        "the sender moving to the Community adapter",
        "the concrete sender has moved to the Community",
    ):
        assert any(p.search(synthetic) for p in pats), synthetic
