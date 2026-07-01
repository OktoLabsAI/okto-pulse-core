"""R16 - fake/test provider backstop matrix and removal gates."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from okto_pulse.core.application.boundary.conformance_matrix import (
    TESTING_PROVIDER_PREFIX,
)
from okto_pulse.core.application.boundary.provider_backstop_matrix import (
    REQUIRED_BACKSTOP_KEYS,
    audit_provider_backstop,
    audit_source_for_provider_backstop_leaks,
    build_provider_backstop_matrix,
    run_provider_backstop_gate,
    validate_provider_backstop_matrix,
)


def _by_key():
    return {entry.provider_key: entry for entry in build_provider_backstop_matrix()}


def _write(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_ts_c814ac43_sanctioned_fake_providers_are_test_only_policy() -> None:
    entries = _by_key()

    assert set(entries) == REQUIRED_BACKSTOP_KEYS
    row = entries["testing_provider_namespace"]
    assert row.module == TESTING_PROVIDER_PREFIX
    assert row.policy == "sanctioned_test_only"
    assert row.runtime_allowed is False
    assert row.test_allowed is True
    assert "composition" in row.migration_path
    assert validate_provider_backstop_matrix() == ()


def test_ts_5ec04dd2_rejects_production_bootstrap_using_test_defaults(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "okto_pulse/core/bootstrap.py",
        "from okto_pulse.core.kg.interfaces.registry import (\n"
        "    _build_defaults,\n"
        "    configure_kg_registry,\n"
        ")\n"
        "def boot():\n"
        "    return configure_kg_registry(defaults_factory=_build_defaults)\n",
    )

    findings = audit_source_for_provider_backstop_leaks((tmp_path,))
    codes = {finding.diagnostic_code for finding in findings}

    assert "production_build_defaults_import" in codes
    assert "production_defaults_factory_bootstrap" in codes
    gate = run_provider_backstop_gate(source_roots=(tmp_path,))
    assert gate.status == "blocking"
    assert gate.observed_value == len(findings)


def test_ts_5ec04dd2_allows_registry_internal_test_defaults_route(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "okto_pulse/core/kg/interfaces/registry.py",
        "def _build_defaults():\n"
        "    from okto_pulse.core.kg.providers.testing.memory_event_bus import (\n"
        "        InMemoryEventBus,\n"
        "    )\n"
        "    return InMemoryEventBus()\n"
        "def configure_kg_registry(*, defaults_factory=None, base_registry=None):\n"
        "    if defaults_factory is not None:\n"
        "        return defaults_factory()\n",
    )

    assert audit_source_for_provider_backstop_leaks((tmp_path,)) == ()


def test_ts_335131b5_documents_settings_config_and_mcp_auth_context_decisions() -> None:
    entries = _by_key()

    settings = entries["settings_config"]
    assert settings.owner == "okto-pulse-community/data"
    assert settings.runtime_allowed is True
    assert settings.policy == "community_owned_productive"
    assert "SettingsKGConfig" in settings.notes
    assert "R07/R16" in settings.decision_ref

    mcp = entries["mcp_auth_context"]
    assert mcp.owner == "okto-pulse-community/inbound-mcp"
    assert mcp.policy == "core_contract_tombstone"
    assert "auth_context_factory" in mcp.migration_path
    assert "R06/R16" in mcp.decision_ref


def test_ts_8133f131_incomplete_provider_matrix_fails_structural_completeness() -> None:
    entries = list(build_provider_backstop_matrix())
    settings = _by_key()["settings_config"]
    entries[entries.index(settings)] = replace(
        settings,
        owner="",
        decision_ref="",
        migration_path="",
    )

    report = audit_provider_backstop(entries=entries, source_roots=())
    assert report.ok is False
    violations = [v for v in report.matrix_violations if v.provider_key == "settings_config"]
    assert violations
    assert set(violations[0].missing_fields) == {
        "owner",
        "decision_ref",
        "migration_path",
    }


def test_ts_8133f131_missing_required_matrix_row_fails_closed() -> None:
    entries = [
        entry
        for entry in build_provider_backstop_matrix()
        if entry.provider_key != "mcp_auth_context"
    ]

    violations = validate_provider_backstop_matrix(entries)
    assert any(
        violation.provider_key == "mcp_auth_context"
        and violation.diagnostic_code == "missing_matrix_row"
        for violation in violations
    )


def test_ts_43d40b3c_auth_bridge_mutation_is_caught_by_existing_gate(
    tmp_path: Path,
) -> None:
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        run_mcp_credential_usage_gate,
    )

    _write(
        tmp_path,
        "okto_pulse/core/mcp/rogue_auth_bridge.py",
        "from okto_pulse.core.kg.providers.embedded.mcp_auth_context import MCPAuthContext\n"
        "from okto_pulse.core.services.main import AgentService\n"
        "async def bypass(db, agent):\n"
        "    ctx = MCPAuthContext(lambda: agent, lambda: db)\n"
        "    return await AgentService(db).list_boards_for_agent(agent.id), ctx\n",
    )

    report = run_mcp_credential_usage_gate(source_root=tmp_path)
    symbols = {violation.symbol for violation in report.violations}

    assert {"MCPAuthContext", "AgentService", "list_boards_for_agent"} <= symbols
    assert report.ok is False


def test_real_source_tree_has_no_productive_fake_provider_backstop_violations() -> None:
    gate = run_provider_backstop_gate()

    assert gate.status == "passed", gate.evidence
