"""AF31-S2 — resource/instruction and version providers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def test_af31_s2_config_import_does_not_read_pyproject(monkeypatch):
    """Core config import/version resolution must not probe source checkout files."""
    previous = sys.modules.pop("okto_pulse.core.infra.config", None)
    touched: list[str] = []
    real_read_text = Path.read_text

    def guarded_read_text(self: Path, *args, **kwargs):
        if self.name == "pyproject.toml":
            touched.append(str(self))
            raise AssertionError(f"config attempted to read {self}")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    try:
        config_mod = importlib.import_module("okto_pulse.core.infra.config")
        settings = config_mod.CoreSettings()
        assert settings.app_version
        assert settings.mcp_server_version == settings.app_version
        assert touched == []
    finally:
        if previous is not None:
            sys.modules["okto_pulse.core.infra.config"] = previous


def test_af31_s2_core_version_provider_supplies_runtime_version():
    from okto_pulse.core.infra import config as config_mod
    from okto_pulse.core.ports.package_version import MappingPackageVersionProvider

    config_mod.register_package_version_provider(
        MappingPackageVersionProvider({"okto-pulse-core": "9.8.7-test"})
    )
    try:
        settings = config_mod.CoreSettings()
        assert settings.app_version == "9.8.7-test"
        assert settings.mcp_server_version == "9.8.7-test"
    finally:
        config_mod.reset_package_version_provider_for_tests()


def test_af31_s2_core_instructions_do_not_require_app_prompt(monkeypatch):
    from okto_pulse.core.mcp import server

    server.reset_instruction_providers_for_tests()
    real_exists = Path.exists

    def guarded_exists(self: Path) -> bool:
        if self.as_posix() == "/app/prompts/agent_system_prompt.md":
            raise AssertionError("core must not probe the legacy /app prompt path")
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", guarded_exists)
    try:
        text = server._load_instructions()
        assert "okto-pulse://workflows/preflight" in text
    finally:
        server.reset_instruction_providers_for_tests()


def test_af31_s2_registered_instruction_provider_wins(tmp_path):
    from okto_pulse.core.mcp import server
    from okto_pulse.core.ports.mcp_instructions import StaticFileMcpInstructionProvider

    prompt = tmp_path / "agent_system_prompt.md"
    prompt.write_text("provider-owned instructions", encoding="utf-8")

    server.reset_instruction_providers_for_tests()
    try:
        server.register_instruction_provider(
            StaticFileMcpInstructionProvider(
                provider_id="test",
                base_dir=tmp_path,
                relative_path=prompt.name,
            )
        )
        assert server._load_instructions() == "provider-owned instructions"
        assert server.mcp.instructions == "provider-owned instructions"
    finally:
        server.reset_instruction_providers_for_tests()
