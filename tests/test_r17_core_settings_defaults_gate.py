"""R17 - CoreSettings default ownership and CommunitySettings parity.

These tests intentionally mutate CoreSettings source text in-memory to prove the
gate fails closed for new defaults and public setting renames.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.conformance_suite import ConformanceSuite
from okto_pulse.core.application.boundary.core_settings_defaults_gate import (
    COMMUNITY_PARITY_FIELDS,
    KG_RUNTIME_KNOBS,
    CoreSettingDefaultEntry,
    PublicSettingAlias,
    build_core_settings_inventory,
    evaluate_community_settings_parity,
    run_core_settings_defaults_gate,
    run_public_config_stability_gate,
)
from okto_pulse.core.infra.config import CoreSettings

_community_config = pytest.importorskip(
    "okto_pulse.community.config",
    reason="AF-04 R17 conformance requires CommunitySettings parity surface.",
)
CommunitySettings = _community_config.CommunitySettings


_CONFIG_SOURCE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "okto_pulse"
    / "core"
    / "infra"
    / "config.py"
).read_text(encoding="utf-8")


def _insert_after_upload_dir(source: str, field_line: str) -> str:
    for needle in (
        '    upload_dir: str = ""\n',
        '    upload_dir: str = "./uploads"\n',
    ):
        if needle in source:
            return source.replace(needle, f"{needle}{field_line}\n", 1)
    raise AssertionError("CoreSettings.upload_dir source line not found")


def _finding_codes(report) -> set[str]:
    return {
        finding["diagnostic_code"]
        for finding in report.evidence.get("findings", [])
        if isinstance(finding, dict)
    }


def test_inventory_covers_current_core_settings_and_required_r17_rows():
    inventory = build_core_settings_inventory()
    by_name = {entry.setting_name: entry for entry in inventory}

    assert set(by_name) == set(CoreSettings.model_fields)
    assert set(KG_RUNTIME_KNOBS) <= set(by_name)

    for entry in inventory:
        assert entry.setting_name
        assert entry.env_var
        assert entry.default_repr
        assert entry.classification
        assert entry.owner
        assert entry.spec_ref
        assert entry.allowed_action
        assert entry.cleanup_status

    for field in COMMUNITY_PARITY_FIELDS:
        assert by_name[field].classification == "edition_default_community"
        assert by_name[field].cleanup_status == "register_before_remove"
        assert by_name[field].community_surfaces

    for field in KG_RUNTIME_KNOBS:
        entry = by_name[field]
        assert entry.classification == "core_consumed_community_observable"
        assert entry.cleanup_status == "register_before_remove"
        assert "okto_pulse.core.kg.connection_pool" in entry.core_consumers
        assert "/api/v1/settings/runtime" in entry.community_surfaces


def test_community_settings_effective_values_match_r17_installed_parity(tmp_path):
    settings = CommunitySettings(
        data_dir=str(tmp_path / "pulse-data"),
        database_url="sqlite+aiosqlite:///./dashboard.db",
        upload_dir="./uploads",
        metrics_dir="",
        kg_base_dir="~/.okto-pulse",
        kg_embedding_mode="sentence-transformers",
    )

    report = evaluate_community_settings_parity(community_settings=settings)

    assert report.ok, report.as_dict()
    assert report.expected == report.observed
    assert report.observed["database_url"].replace("\\", "/").endswith(
        "/data/pulse.db"
    )
    assert report.observed["upload_dir"].replace("\\", "/").endswith("/uploads")
    assert report.observed["metrics_dir"].replace("\\", "/").endswith("/metrics")
    assert report.observed["kg_base_dir"] == str((tmp_path / "pulse-data").resolve())
    assert report.observed["kg_embedding_mode"] == "sentence-transformers"


def test_new_concrete_core_default_without_owner_fails_gate():
    mutated = _insert_after_upload_dir(
        _CONFIG_SOURCE,
        '    r17_local_cache_dir: str = "./runtime-cache"',
    )

    report = run_core_settings_defaults_gate(source_text=mutated)

    assert report.status == "blocking"
    assert _finding_codes(report) == {"new_unowned_core_default"}
    assert report.evidence["findings"][0]["setting_name"] == "r17_local_cache_dir"


def test_allowlisted_agnostic_default_with_owner_passes_gate():
    mutated = _insert_after_upload_dir(
        _CONFIG_SOURCE,
        "    r17_safe_feature_enabled: bool = False",
    )
    owner_row = CoreSettingDefaultEntry(
        setting_name="r17_safe_feature_enabled",
        env_var="R17_SAFE_FEATURE_ENABLED",
        default_repr="False",
        classification="common_agnostic",
        owner="okto-pulse-core/config",
        spec_ref="R17",
        allowed_action="keep as explicit common feature flag",
        cleanup_status="keep",
        rationale="R17 test-owned agnostic default.",
    )

    report = run_core_settings_defaults_gate(
        source_text=mutated,
        additional_entries=(owner_row,),
    )

    assert report.status == "passed", report.evidence


def test_public_setting_rename_without_alias_fails_and_alias_passes():
    renamed = _CONFIG_SOURCE.replace(
        '    database_url: str = ""',
        '    database_dsn: str = ""',
        1,
    )

    no_alias = run_public_config_stability_gate(source_text=renamed)

    assert no_alias.status == "blocking"
    assert _finding_codes(no_alias) == {
        "missing_public_core_setting",
        "new_unowned_core_default",
    }

    renamed_entry = CoreSettingDefaultEntry(
        setting_name="database_dsn",
        env_var="DATABASE_URL",
        default_repr="''",
        classification="edition_default_community",
        owner="okto-pulse-community/data",
        spec_ref="R17",
        allowed_action="rename only through DATABASE_URL migration alias",
        cleanup_status="register_before_remove",
        rationale="R17 test migration alias for the public database setting.",
        community_surfaces=("CommunitySettings",),
        migration_plan_ref="R17-MP-database-url",
    )
    alias = PublicSettingAlias(
        original_name="database_url",
        new_name="database_dsn",
        legacy_env_var="DATABASE_URL",
        migration_plan_ref="R17-MP-database-url",
    )

    with_alias = run_public_config_stability_gate(
        source_text=renamed,
        additional_entries=(renamed_entry,),
        aliases=(alias,),
    )

    assert with_alias.status == "passed", with_alias.evidence


def test_conformance_suite_exposes_r17_settings_axes():
    report = ConformanceSuite().run()
    axes = report["axes"]

    assert "core_settings_defaults" in axes
    assert "public_config_stability" in axes
    assert axes["core_settings_defaults"]["status"] == "passed"
    assert axes["public_config_stability"]["status"] == "passed"
