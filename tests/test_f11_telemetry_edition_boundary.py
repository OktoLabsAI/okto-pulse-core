from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from okto_pulse.core.application.boundary.telemetry_edition_gate import (
    run_telemetry_edition_gate,
)
from okto_pulse.core.telemetry.effect_config_registry import (
    register_telemetry_effect_config_provider,
    reset_telemetry_effect_config_provider_for_tests,
)
from okto_pulse.core.telemetry.settings import resolve_telemetry_config


def test_f11_core_telemetry_boundary_has_zero_edition_details() -> None:
    report = run_telemetry_edition_gate()
    assert report.ok, report.as_dict()
    assert report.as_dict()["budget"] == 0


def test_f11_public_port_annotations_are_path_free() -> None:
    import okto_pulse.core.ports.telemetry as telemetry_ports

    path = Path(telemetry_ports.__file__)
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        (node.module or "").partition(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert "pathlib" not in imports
    for forbidden in ("metrics_dir", "install_id_path", "export_local", "purge_local"):
        assert forbidden not in source


def test_f11_policy_resolves_opaque_refs_with_standard_library_fake() -> None:
    class _Provider:
        def state_ref(self, settings) -> str:
            return f"memory://{settings.scope}"

        def delivery_target(self, settings) -> str:
            return "memory://sink"

    reset_telemetry_effect_config_provider_for_tests()
    register_telemetry_effect_config_provider(_Provider())
    try:
        configs = [
            resolve_telemetry_config(
                SimpleNamespace(
                    scope=scope,
                    metrics_mode="",
                    metrics_retention_days=30,
                    metrics_policy_version="policy",
                    metrics_schema_version="1.1.0",
                ),
                state_snapshot={"mode": "disabled"},
            )
            for scope in ("tenant-a", "tenant-b")
        ]
    finally:
        reset_telemetry_effect_config_provider_for_tests()

    assert [config.state_ref for config in configs] == [
        "memory://tenant-a",
        "memory://tenant-b",
    ]


def test_f11_ports_import_without_edition_runtime_dependencies() -> None:
    code = (
        "import sys\n"
        "import okto_pulse.core.ports.telemetry\n"
        "forbidden = {'requests', 'chardet', 'sqlite3'} & set(sys.modules)\n"
        "assert not forbidden, forbidden\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_f11_core_wheel_has_no_metrics_transport_module() -> None:
    import okto_pulse.core as core

    package_root = Path(core.__file__).resolve().parent
    assert not (package_root / "api" / "metrics.py").exists()
