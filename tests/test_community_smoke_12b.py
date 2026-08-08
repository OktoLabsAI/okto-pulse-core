"""Spec #12 (card bed19910) — core/Community smoke install + register-before-remove.

The gates are deterministic evaluators (a fake CommandRunner replaces the
expensive real subprocess install). Negative cases — register-before-remove
without a Community adapter, a missing smoke_oracle, route/MCP deltas — are pure
and never stubbed by a comment.
"""

from __future__ import annotations

import os
from pathlib import Path

from okto_pulse.core.application.boundary import (
    CommandResult,
    CommunityRebuildReinstallSmokeGate,
    CommunitySmokeInput,
    CommunitySmokeOracle,
    CoreSmokeInstallGate,
    SmokeInstallInput,
)


class _FakeRunner:
    """Deterministic runner: commands containing any of ``fail_on`` return rc=1."""

    def __init__(self, fail_on: tuple[str, ...] = ()):
        self.fail_on = fail_on
        self.calls: list[str] = []

    def run(self, command: str) -> CommandResult:
        self.calls.append(command)
        bad = any(token in command for token in self.fail_on)
        return CommandResult(command=command, returncode=1 if bad else 0, ok=not bad, output_tail="")


def _oracle(**overrides) -> CommunitySmokeOracle:
    base = dict(
        baseline_ref="inline://baseline",
        route_inventory=("/health", "/boards"),
        mcp_tool_inventory=("okto_pulse_create_board",),
        command_inventory=("smoke_backend",),
    )
    base.update(overrides)
    return CommunitySmokeOracle(**base)


#: A passing smoke result that covers the default oracle's command_inventory.
_SMOKE_OK = (CommandResult(command="smoke_backend", returncode=0, ok=True),)


# --------------------------------------------------------------------------- #
# CoreSmokeInstallGate (api_55f0757f, ts_e4f4c2ac)
# --------------------------------------------------------------------------- #
def test_core_smoke_install_passes_with_clean_runner() -> None:
    runner = _FakeRunner()
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(
            wheel_path=Path("dist/okto_pulse_core-0.3.1-py3-none-any.whl"),
            expected_imports=("okto_pulse.core.application.boundary",),
            commands=("python -c \"import okto_pulse\"",),
        ),
        runner=runner,
    )
    assert report.status == "passed", report.evidence
    assert any("venv" in c for c in runner.calls)  # isolated env is created
    assert any("install dist" in c.replace("\\", "/") for c in runner.calls)
    assert report.evidence["import_checks"] == ["okto_pulse.core.application.boundary"]


def test_core_smoke_install_blocks_on_install_failure() -> None:
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(wheel_path=Path("broken.whl")),
        runner=_FakeRunner(fail_on=("install broken.whl",)),
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "install_failed"


def test_core_smoke_install_blocks_on_smoke_import_failure() -> None:
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(wheel_path=Path("ok.whl"), expected_imports=("okto_pulse.missing_mod",)),
        runner=_FakeRunner(fail_on=("missing_mod",)),
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "smoke_failed"


def test_core_smoke_install_blocks_on_venv_create_failure() -> None:
    # build/env-setup failure (cannot create the isolated venv) -> install_failed.
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(wheel_path=Path("ok.whl"), isolated_env=Path("build") / "e"),
        runner=_FakeRunner(fail_on=("-m venv",)),
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "install_failed"


def test_core_smoke_install_blocks_on_smoke_command_failure() -> None:
    # an expected smoke command failing (not an import) -> smoke_failed/blocking.
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(
            wheel_path=Path("ok.whl"),
            commands=("okto-pulse-core-boundary audit",),
        ),
        runner=_FakeRunner(fail_on=("boundary audit",)),
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "smoke_failed"


def test_core_smoke_install_installs_core_wheel_without_community() -> None:
    # ts_e4f4c2ac: a core-pure wheel installs + imports in isolation WITHOUT any
    # Community artifact.
    runner = _FakeRunner()
    wheel = Path("dist/okto_pulse_core-0.3.1-py3-none-any.whl")
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(
            wheel_path=wheel,
            isolated_env=Path("build") / "core_only",
            expected_imports=(
                "okto_pulse.core.application.boundary",
                "okto_pulse.core.composition",
            ),
        ),
        runner=runner,
    )
    assert report.status == "passed", report.evidence
    # nothing in the install/smoke commands references the Community package
    assert not any("community" in c.lower() for c in runner.calls)
    # only the core wheel is installed
    install_calls = [
        c for c in runner.calls if "install" in c and "okto_pulse_core" in c.replace("\\", "/")
    ]
    assert install_calls
    # the verified imports are core-only
    assert report.evidence["import_checks"] == [
        "okto_pulse.core.application.boundary",
        "okto_pulse.core.composition",
    ]
    assert all("okto_pulse.core" in m for m in report.evidence["import_checks"])


def test_core_smoke_install_runs_smoke_commands_with_venv_path() -> None:
    # rework (codex): a console-script smoke command must run with the venv's
    # Scripts/bin FIRST on PATH, proving it resolves to the ISOLATED install and
    # not a binary already on the host PATH.
    env = Path("build") / "smoke_path_env"
    runner = _FakeRunner()
    CoreSmokeInstallGate().run(
        SmokeInstallInput(
            wheel_path=Path("ok.whl"),
            isolated_env=env,
            commands=("okto-pulse-core-boundary audit",),
        ),
        runner=runner,
    )
    smoke_calls = [c for c in runner.calls if "okto-pulse-core-boundary audit" in c]
    assert smoke_calls, runner.calls
    venv_bin = str(env / ("Scripts" if os.name == "nt" else "bin"))
    # the venv bin dir is put on PATH ahead of the command
    assert all(venv_bin in c for c in smoke_calls), smoke_calls


# --------------------------------------------------------------------------- #
# CommunityRebuildReinstallSmokeGate (api_2f50f077, ts_17230d99)
# --------------------------------------------------------------------------- #
def test_community_smoke_passes_when_inventory_matches_oracle() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=oracle.route_inventory,
            observed_mcp_tools=oracle.mcp_tool_inventory,
            build_ok=True,
            reinstall_ok=True,
            smoke_results=_SMOKE_OK,
        )
    )
    assert report.status == "passed", report.evidence
    assert report.evidence["route_delta"] == []
    assert report.evidence["mcp_tool_delta"] == []


def test_community_smoke_missing_oracle_blocks() -> None:
    report = CommunityRebuildReinstallSmokeGate().run(CommunitySmokeInput(oracle=None))
    assert report.status == "blocking"
    assert report.evidence["error"] == "smoke_oracle_missing"


def test_community_smoke_route_delta_is_community_regression() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=("/health",),  # missing /boards
            observed_mcp_tools=oracle.mcp_tool_inventory,
            smoke_results=_SMOKE_OK,
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "community_regression"
    assert "/boards" in report.evidence["route_delta"]


def test_community_smoke_mcp_tool_delta_blocks() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=oracle.route_inventory,
            observed_mcp_tools=("okto_pulse_create_board", "okto_pulse_new_tool"),
            smoke_results=_SMOKE_OK,
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "mcp_tool_inventory_delta"
    assert "okto_pulse_new_tool" in report.evidence["mcp_tool_delta"]


def test_community_smoke_documented_decision_allows_delta() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=("/health", "/boards", "/new_route"),
            observed_mcp_tools=oracle.mcp_tool_inventory,
            allowed_delta_policy="documented_decision_only",
            accepted_decision_ref="dec_12345",
            smoke_results=_SMOKE_OK,
        )
    )
    assert report.status == "passed", report.evidence
    assert report.evidence["external_behavior_delta"] == "documented"


def test_community_rebuild_failure_blocks() -> None:
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(oracle=_oracle(), build_ok=False)
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "community_rebuild_failed"


# --------------------------------------------------------------------------- #
# register-before-remove (tr_709b39d5, ts_11275928) — the required negatives
# --------------------------------------------------------------------------- #
def test_register_before_remove_without_community_adapter_blocks() -> None:
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=_oracle(),
            removed_dependencies=("apscheduler",),
            community_adapters_registered=(),  # no equivalent adapter
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "community_adapter_missing"
    assert "apscheduler" in report.evidence["missing_community_adapter"]
    # dependency stays baseline/xfail_advisory with owner + promotion_criteria
    assert report.owner == "okto-pulse-core/architecture"
    assert report.promotion_criteria


def test_register_before_remove_without_oracle_blocks_smoke_oracle_missing() -> None:
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=None,  # no approved smoke_oracle
            removed_dependencies=("apscheduler",),
            community_adapters_registered=("apscheduler",),  # adapter present, oracle absent
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "smoke_oracle_missing"
    assert report.promotion_criteria


def test_register_before_remove_passes_with_adapter_and_oracle() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=oracle.route_inventory,
            observed_mcp_tools=oracle.mcp_tool_inventory,
            removed_dependencies=("apscheduler",),
            community_adapters_registered=("apscheduler",),
            smoke_results=_SMOKE_OK,
        )
    )
    assert report.status == "passed", report.evidence


def test_community_smoke_missing_expected_command_blocks() -> None:
    # codex rework (val_ab672f3c): an oracle command_inventory entry with no
    # corresponding passing smoke_result must BLOCK, never pass.
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=oracle.route_inventory,
            observed_mcp_tools=oracle.mcp_tool_inventory,
            smoke_results=(),  # smoke_backend expected but NOT run
        )
    )
    assert report.status == "blocking", report.evidence
    assert report.evidence["error"] == "community_regression"
    assert "smoke_backend" in report.evidence["missing_smoke_commands"]


def test_community_smoke_failed_expected_command_blocks() -> None:
    oracle = _oracle()
    report = CommunityRebuildReinstallSmokeGate().run(
        CommunitySmokeInput(
            oracle=oracle,
            observed_routes=oracle.route_inventory,
            observed_mcp_tools=oracle.mcp_tool_inventory,
            smoke_results=(CommandResult(command="smoke_backend", returncode=1, ok=False),),
        )
    )
    assert report.status == "blocking"
    assert report.evidence["error"] == "community_regression"
    assert "smoke_backend" in report.evidence["missing_smoke_commands"]


def test_core_smoke_install_uses_isolated_venv() -> None:
    # ts_e4f4c2ac: the wheel must install into an ISOLATED venv, not the current
    # interpreter. Proven via the constructed commands; no real build executed.
    env = Path("build") / "smoke_env_test"
    runner = _FakeRunner()
    report = CoreSmokeInstallGate().run(
        SmokeInstallInput(
            wheel_path=Path("dist/core.whl"),
            isolated_env=env,
            expected_imports=("okto_pulse.core.domain",),
        ),
        runner=runner,
    )
    assert report.status == "passed", report.evidence
    env_str = str(env)
    # a venv is created at the isolated path
    assert any("venv" in c and env_str in c for c in runner.calls)
    # the wheel installs with the venv pip (under the isolated env), not bare pip
    install_calls = [c for c in runner.calls if "install dist" in c.replace("\\", "/")]
    assert install_calls and all(env_str in c for c in install_calls)
    # imports run with the venv python (under the isolated env)
    import_calls = [c for c in runner.calls if "import okto_pulse.core.domain" in c]
    assert import_calls and all(env_str in c for c in import_calls)
    assert report.evidence["isolated_env"] == env_str
