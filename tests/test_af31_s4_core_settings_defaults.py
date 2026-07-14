"""AF31-S4 - core upload_dir neutrality and R17 ownership gates."""

from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.application.boundary.core_settings_defaults_gate import (
    CoreSettingDefaultEntry,
    build_core_settings_inventory,
    run_core_settings_defaults_gate,
)
from okto_pulse.core.infra.config import CoreSettings


CORE_SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
CORE_CONFIG_SOURCE = (CORE_SRC_ROOT / "infra" / "config.py").read_text(
    encoding="utf-8"
)
def _core_source_with_local_path_field(default_literal: str) -> str:
    anchor = '    metrics_mode: str = ""'
    assert anchor in CORE_CONFIG_SOURCE
    return CORE_CONFIG_SOURCE.replace(
        anchor,
        f'{anchor}\n    local_cache_dir: str = "{default_literal}"',
        1,
    )


def _finding_codes(report) -> set[str]:
    return {
        finding["diagnostic_code"]
        for finding in report.evidence.get("findings", [])
        if isinstance(finding, dict)
    }


def test_af31_s4_operational_settings_are_absent_from_core_contract(
    monkeypatch,
):
    for env_name in (
        "DATABASE_URL",
        "UPLOAD_DIR",
        "METRICS_BEACON_URL",
        "KG_EMBEDDING_MODEL",
    ):
        monkeypatch.delenv(env_name, raising=False)

    settings = CoreSettings()
    inventory = {entry.setting_name: entry for entry in build_core_settings_inventory()}

    operational = {
        "database_url",
        "upload_dir",
        "metrics_dir",
        "metrics_beacon_url",
        "host",
        "port",
        "mcp_port",
        "cors_origins",
        "kg_base_dir",
        "kg_embedding_model",
        "kg_kuzu_buffer_pool_mb",
    }
    assert operational.isdisjoint(CoreSettings.model_fields)
    assert set(inventory) == set(CoreSettings.model_fields)
    assert settings.metrics_mode == ""


def test_af31_s4_r17_blocks_unowned_local_first_upload_dir_default():
    mutant_source = _core_source_with_local_path_field("./runtime-cache")
    unowned_upload = CoreSettingDefaultEntry(
        setting_name="local_cache_dir",
        env_var="LOCAL_CACHE_DIR",
        default_repr="'./runtime-cache'",
        classification="core_contract_required",
        owner="okto-pulse-core/storage",
        spec_ref="AF31-S4-mutant",
        allowed_action="mutant must fail; upload_dir local paths are edition-owned",
        cleanup_status="keep",
        rationale="AF31-S4 mutant: local-first upload_dir cannot be core-owned.",
        public_contract="AF31 mutant upload dir contract.",
        effective_source="CoreSettings mutant local path.",
        compatibility_path="No edition compatibility path.",
        removal_criterion="Remove after AF31 mutant fixture is deleted.",
    )

    report = run_core_settings_defaults_gate(
        source_text=mutant_source,
        additional_entries=(unowned_upload,),
    )

    assert report.status == "blocking", report.evidence
    assert "unowned_local_first_default" in _finding_codes(report)


def test_af31_s4_explicit_upload_override_stays_core_only(tmp_path):
    custom_upload_dir = tmp_path / "edition-upload"
    settings = CoreSettings(upload_dir=str(custom_upload_dir))

    assert "upload_dir" not in CoreSettings.model_fields
    assert settings.upload_dir == str(custom_upload_dir)
    assert _core_imports_community_modules() == []


def _core_imports_community_modules() -> list[str]:
    offenders: list[str] = []
    for path in CORE_SRC_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            imported_modules: list[str] = []
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.append(node.module)
            if any(
                module == "okto_pulse.community"
                or module.startswith("okto_pulse.community.")
                for module in imported_modules
            ):
                offenders.append(path.relative_to(CORE_SRC_ROOT).as_posix())
                break
    return sorted(offenders)
