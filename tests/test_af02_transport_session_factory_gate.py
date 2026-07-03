from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary import transport_session_factory_gate
from okto_pulse.core.application.boundary.transport_session_factory_gate import (
    run_transport_session_factory_gate,
)
from okto_pulse.core.repositories.relational_boundary_gate import (
    RELATIONAL_BASELINE_R01B,
    relational_baseline_report,
)


CORE_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_af02_transport_gate_real_tree_has_only_bootstrap_allowlist():
    report = run_transport_session_factory_gate(CORE_REPO_ROOT / "src/okto_pulse/core")

    assert report.ok, report.as_dict()
    assert {
        (finding.file, finding.function, finding.allowlisted)
        for finding in report.findings
    } == {("mcp/server.py", "run_mcp_server", True)}
    assert not any(finding.file == "api/kg_events_hub.py" for finding in report.findings)
    assert not any(finding.file == "api/kg_tick.py" for finding in report.findings)


def test_af02_transport_gate_blocks_rogue_rest_session_factory(tmp_path: Path):
    api_dir = tmp_path / "api"
    api_dir.mkdir()
    (api_dir / "kg_rogue.py").write_text(
        "from okto_pulse.core.infra.database import get_session_factory\n"
        "\n"
        "async def handler():\n"
        "    session_factory = get_session_factory()\n"
        "    async with session_factory() as session:\n"
        "        return session\n",
        encoding="utf-8",
    )

    report = run_transport_session_factory_gate(tmp_path)

    assert report.ok is False
    assert {
        (finding.file, finding.function, finding.kind)
        for finding in report.violations
    } >= {
        ("api/kg_rogue.py", None, "import"),
        ("api/kg_rogue.py", "handler", "call"),
        ("api/kg_rogue.py", "handler", "derived_session_factory_call"),
    }


def test_af02_transport_gate_allowlist_is_function_scoped(tmp_path: Path):
    mcp_dir = tmp_path / "mcp"
    mcp_dir.mkdir()
    (mcp_dir / "server.py").write_text(
        "def run_mcp_server():\n"
        "    from okto_pulse.core.infra.database import get_session_factory\n"
        "    register_session_factory(get_session_factory())\n"
        "\n"
        "async def okto_pulse_kg_tick_run_now():\n"
        "    from okto_pulse.core.infra.database import get_session_factory\n"
        "    return get_session_factory()\n",
        encoding="utf-8",
    )

    report = run_transport_session_factory_gate(tmp_path)

    assert report.ok is False
    assert {
        (finding.file, finding.function, finding.allowlisted)
        for finding in report.findings
    } >= {
        ("mcp/server.py", "run_mcp_server", True),
        ("mcp/server.py", "okto_pulse_kg_tick_run_now", False),
    }


def test_af02_transport_gate_is_independent_from_r01_baseline():
    source = Path(transport_session_factory_gate.__file__).read_text(encoding="utf-8")

    assert "relational_boundary_gate" not in source
    report = relational_baseline_report()
    assert report["r01b_baseline"] == RELATIONAL_BASELINE_R01B
