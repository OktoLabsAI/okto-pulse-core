from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary import (
    AntiSingletonGate,
    AntiSingletonGateInput,
    ImportBoundaryGate,
    ImportBoundaryGateInput,
    LayerResolver,
)
from okto_pulse.core.application.boundary.gates import (
    IMPORT_BOUNDARY_BASELINE_LEDGER,
)
from repository_checkout_testing import community_repo_for


_RELATIONAL_CATEGORIES = {"sqlalchemy", "aiosqlite", "asyncpg"}


def test_ts1_new_non_relational_import_without_ledger_blocks(tmp_path: Path) -> None:
    adapter = tmp_path / "okto_pulse" / "core" / "infra" / "rogue_adapter.py"
    adapter.parent.mkdir(parents=True)
    adapter.write_text("import fastapi\n", encoding="utf-8")

    report = ImportBoundaryGate().run(
        ImportBoundaryGateInput(source_root=tmp_path, mode="bootstrap")
    )

    assert report.status == "blocking"
    violation = report.evidence["violations"][0]
    assert violation["file"] == "okto_pulse/core/infra/rogue_adapter.py"
    assert violation["category"] == "fastapi"
    assert violation["status"] == "blocking"
    assert violation["owner"] is None
    assert violation["baseline_key"].endswith("|fastapi|forbidden_import_root")
    assert "Missing non-relational import-boundary ledger entry" in violation["remediation_hint"]


def test_ts2_non_relational_baselines_have_complete_per_item_ownership() -> None:
    assert IMPORT_BOUNDARY_BASELINE_LEDGER == {}

    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="bootstrap"))
    non_relational_baselines = [
        v
        for v in report.evidence["violations"]
        if v["status"] == "baseline" and v["category"] not in _RELATIONAL_CATEGORIES
    ]

    assert non_relational_baselines == []


def test_ts3_singleton_gate_blocks_runtime_extra_baseline_without_ledger(
    tmp_path: Path,
) -> None:
    module = tmp_path / "okto_pulse" / "core" / "kg" / "runtime_handle.py"
    module.parent.mkdir(parents=True)
    module.write_text(
        "_runtime_handle = None\n"
        "\n"
        "def configure(value):\n"
        "    global _runtime_handle\n"
        "    _runtime_handle = value\n",
        encoding="utf-8",
    )
    key = "okto_pulse/core/kg/runtime_handle.py::_runtime_handle"

    report = AntiSingletonGate().run(
        AntiSingletonGateInput(source_root=tmp_path, extra_baseline=(key,))
    )

    assert report.status == "blocking"
    assert report.evidence["error"] == "missing_singleton_ledger"
    assert report.evidence["missing_runtime_ledger"] == [
        {
            "key": key,
            "name": "_runtime_handle",
            "file": "okto_pulse/core/kg/runtime_handle.py",
            "kind": "global_mutation",
            "reason": "missing_runtime_ledger",
        }
    ]


def test_ts3_current_runtime_singleton_baseline_is_fully_ledgered() -> None:
    report = AntiSingletonGate().run(AntiSingletonGateInput())

    assert report.status == "passed"
    assert report.evidence["missing_runtime_ledger"] == []
    assert report.evidence["detected_count"] == 0
    assert report.evidence["baseline_count"] == 0


def test_ts4_core_ports_resolves_as_real_pure_layer() -> None:
    resolution = LayerResolver().resolve("okto_pulse/core/ports/coordination.py")
    assert resolution.layer == "ports"
    assert resolution.requires_governance is False

    report = ImportBoundaryGate().run(ImportBoundaryGateInput(mode="blocking"))
    ports_blocking = [
        v
        for v in report.evidence["violations"]
        if v["status"] == "blocking" and v["file"].startswith("okto_pulse/core/ports/")
    ]
    assert ports_blocking == []


def test_ts5_readmes_document_core_community_baseline_policy() -> None:
    core_root = Path(__file__).resolve().parents[1]
    community_root = community_repo_for(core_root)
    core_architecture = (
        core_root / "docs" / "ARCHITECTURE-OVERVIEW.md"
    ).read_text(encoding="utf-8")
    community_architecture = (
        community_root / "docs" / "ARCHITECTURE.md"
    ).read_text(encoding="utf-8")

    assert "IMPORT_BOUNDARY_BASELINE_LEDGER" in core_architecture
    assert "RUNTIME_SINGLETON_BASELINE_LEDGER" in core_architecture
    assert "non-relational import-boundary" in core_architecture
    assert "removal criterion" in core_architecture
    assert "okto_pulse/core/ports" in core_architecture

    assert "IMPORT_BOUNDARY_BASELINE_LEDGER" in community_architecture
    assert "adapter-specific debt" in community_architecture
    assert "local-first adapters" in community_architecture
    assert "removal criterion" in community_architecture
    assert "okto_pulse/core/ports" in community_architecture
