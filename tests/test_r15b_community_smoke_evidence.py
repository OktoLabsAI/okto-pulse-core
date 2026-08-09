"""R15B — core consumes Community runtime smoke evidence as data."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from okto_pulse.core.application.boundary import (
    CommunityRebuildReinstallSmokeGate,
    CommunitySmokeEvidenceInput,
)

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"


def _valid_payload(now: datetime) -> dict[str, object]:
    return {
        "schema_version": "1",
        "producer": "okto-pulse-community",
        "artifact_name": "community_runtime_smoke_evidence.json",
        "generated_at": now.isoformat(),
        "max_age_seconds": 3600,
        "core_version": "0.3.2",
        "community_version": "0.3.2",
        "core_commit": "core-sha",
        "community_commit": "community-sha",
        "wheel_hashes": {"core": "sha256:core", "community": "sha256:community"},
        "artifact_paths": {"runner": "scripts/r05e_community_preservation_smoke.py"},
        "commands_executed": ["python scripts/r05e_community_preservation_smoke.py"],
        "gate_report": {
            "axis": "community_smoke",
            "status": "passed",
            "baseline_policy": "exact",
            "required_surfaces": [
                "install",
                "imports",
                "composition",
                "seed",
                "routes",
                "mcp_tools",
                "cli_commands",
                "metadata",
            ],
            "observed_counts": {"routes": 2, "mcp_tools": 1, "cli_commands": 4},
            "symmetric_diff": {
                "routes": {"missing": [], "extra": []},
                "mcp_tools": {"missing": [], "extra": []},
                "cli_commands": {"missing": [], "extra": []},
            },
            "diagnostics": [],
        },
        "register_before_remove": {
            "removed_dependencies": ["asyncpg"],
            "community_adapters_registered": ["asyncpg"],
            "smoke_oracle": {
                "status": "passed",
                "evidence_id": "community-runtime-smoke",
                "commit": "community-sha",
                "wheel_hash": "sha256:community",
            },
        },
        "checks": {
            "install": {"status": "passed", "commands": ["uv pip install"]},
            "imports": {"status": "passed", "modules": ["okto_pulse.community.cli"]},
            "composition": {"status": "passed", "adapters": ["asyncpg"]},
            "seed": {"status": "passed"},
            "routes": {"status": "passed", "routes": ["/health", "/api/v1/boards"]},
            "mcp": {"status": "passed", "tools": ["okto_pulse_create_ideation"]},
            "cli": {"status": "passed", "commands": ["init", "serve", "status", "reset"]},
            "metadata": {"status": "passed", "dependencies": []},
        },
    }


def _run(payload: dict[str, object], now: datetime):
    return CommunityRebuildReinstallSmokeGate().run_evidence(
        CommunitySmokeEvidenceInput(
            payload=payload,
            now=now,
            expected_core_commit="core-sha",
            expected_community_commit="community-sha",
            expected_wheel_hashes={"core": "sha256:core", "community": "sha256:community"},
            expected_removed_dependencies=("asyncpg",),
        )
    )


def test_core_contract_imports_without_community_runtime(tmp_path: Path) -> None:
    code = (
        "import sys\n"
        "from datetime import datetime, timezone\n"
        "from okto_pulse.core.application.boundary import CommunityRebuildReinstallSmokeGate, "
        "CommunitySmokeEvidenceInput\n"
        "payload = {\n"
        "  'schema_version': '1', 'producer': 'okto-pulse-community',\n"
        "  'artifact_name': 'community_runtime_smoke_evidence.json',\n"
        "  'generated_at': '2026-07-01T00:00:00+00:00', 'max_age_seconds': 3600,\n"
        "  'core_version': '0.3.2', 'community_version': '0.3.2',\n"
        "  'core_commit': 'c', 'community_commit': 'k',\n"
        "  'wheel_hashes': {'core': 'sha256:c', 'community': 'sha256:k'},\n"
        "  'artifact_paths': {'runner': 'scripts/r05e_community_preservation_smoke.py'},\n"
        "  'commands_executed': ['python scripts/r05e_community_preservation_smoke.py'],\n"
        "  'gate_report': {'axis': 'community_smoke', 'status': 'passed',\n"
        "    'baseline_policy': 'exact',\n"
        "    'required_surfaces': ['install','imports','composition','seed','routes',"
        "'mcp_tools','cli_commands','metadata'],\n"
        "    'observed_counts': {'routes': 1, 'mcp_tools': 1, 'cli_commands': 1},\n"
        "    'symmetric_diff': {'routes': {'missing': [], 'extra': []},\n"
        "      'mcp_tools': {'missing': [], 'extra': []},\n"
        "      'cli_commands': {'missing': [], 'extra': []}}, 'diagnostics': []},\n"
        "  'register_before_remove': {'removed_dependencies': [],\n"
        "    'community_adapters_registered': [],\n"
        "    'smoke_oracle': {'status': 'passed', 'commit': 'k', 'wheel_hash': 'sha256:k'}},\n"
        "  'checks': {'install': {'status': 'passed'}, 'imports': {'status': 'passed'},\n"
        "    'composition': {'status': 'passed'}, 'seed': {'status': 'passed'},\n"
        "    'routes': {'status': 'passed'}, 'mcp': {'status': 'passed'},\n"
        "    'cli': {'status': 'passed'}, 'metadata': {'status': 'passed'}}\n"
        "}\n"
        "report = CommunityRebuildReinstallSmokeGate().run_evidence(\n"
        "  CommunitySmokeEvidenceInput(payload=payload, now=datetime(2026, 7, 1, tzinfo=timezone.utc)))\n"
        "assert report.status == 'passed', report.as_dict()\n"
        "leaked = [n for n in sys.modules if n == 'okto_pulse.community' or "
        "n.startswith('okto_pulse.community.')]\n"
        "assert leaked == [], leaked\n"
        "print('NO_COMMUNITY_RUNTIME')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "NO_COMMUNITY_RUNTIME" in proc.stdout


def test_core_rejects_failing_stale_mismatched_and_non_exact_evidence() -> None:
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)

    failing = _valid_payload(now)
    failing["gate_report"]["status"] = "blocking"  # type: ignore[index]
    assert _run(failing, now).evidence["error"] == "smoke_evidence_failing"

    stale = _valid_payload(now - timedelta(hours=2))
    assert _run(stale, now).evidence["error"] == "smoke_evidence_stale"

    mismatched = _valid_payload(now)
    mismatched["community_commit"] = "different"
    assert _run(mismatched, now).evidence["error"] == "smoke_evidence_mismatch"

    non_exact = _valid_payload(now)
    non_exact["gate_report"]["baseline_policy"] = "minimum"  # type: ignore[index]
    assert _run(non_exact, now).evidence["error"] == "smoke_evidence_failing"

    missing_commands = _valid_payload(now)
    missing_commands["commands_executed"] = []
    assert _run(missing_commands, now).evidence["error"] == "smoke_evidence_malformed"

    missing_artifacts = _valid_payload(now)
    missing_artifacts["artifact_paths"] = {}
    assert _run(missing_artifacts, now).evidence["error"] == "smoke_evidence_malformed"


def test_register_before_remove_missing_adapter_or_oracle_blocks_cleanup() -> None:
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)

    no_adapter = _valid_payload(now)
    no_adapter["register_before_remove"]["community_adapters_registered"] = []  # type: ignore[index]
    adapter_report = _run(no_adapter, now)
    assert adapter_report.status == "blocking"
    assert adapter_report.evidence["error"] == "community_adapter_missing"

    no_oracle = _valid_payload(now)
    no_oracle["register_before_remove"]["smoke_oracle"] = {"status": "blocking"}  # type: ignore[index]
    oracle_report = _run(no_oracle, now)
    assert oracle_report.status == "blocking"
    assert oracle_report.evidence["error"] == "smoke_oracle_missing"
