"""Core/Community smoke-install gates + register-before-remove (spec #12, card bed19910).

- :class:`CoreSmokeInstallGate`               — api_55f0757f
- :class:`CommunityRebuildReinstallSmokeGate` — api_2f50f077 (+ register-before-remove, tr_709b39d5)

Real build/install is expensive, so the gates are DETERMINISTIC evaluators:

- ``CoreSmokeInstallGate`` runs its commands through an injected
  :class:`CommandRunner` (a real subprocess runner for an e2e run; a fake in
  tests). The classification of install/smoke results is pure.
- ``CommunityRebuildReinstallSmokeGate`` evaluates OBSERVED inventories
  (routes/MCP tools/build status) against a :class:`CommunitySmokeOracle` and the
  register-before-remove inputs — no subprocess needed for the deltas, the
  missing-oracle path, or the missing-adapter path (the negative cases).

Returns the shared #12 :class:`GateReport`. Backend-only; no Community change.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .report import GateReport

#: Real subprocess timeout for an e2e install/smoke run.
_REAL_RUN_TIMEOUT = 600


def _venv_python(env: Path) -> str:
    """Path to the Python interpreter INSIDE an isolated venv (cross-platform)."""
    if os.name == "nt":
        return str(env / "Scripts" / "python.exe")
    return str(env / "bin" / "python")


def _venv_pip(env: Path) -> str:
    """Path to pip INSIDE an isolated venv (cross-platform)."""
    if os.name == "nt":
        return str(env / "Scripts" / "pip.exe")
    return str(env / "bin" / "pip")


def _venv_bin_dir(env: Path) -> str:
    """The venv directory that holds console scripts (Scripts on Windows, bin)."""
    return str(env / ("Scripts" if os.name == "nt" else "bin"))


def _venv_path_prefix(env: Path) -> str:
    """A shell prefix that puts the venv's console scripts FIRST on PATH.

    Cross-platform: a console script (``okto-pulse-core-boundary``) then resolves
    to the one installed in the isolated venv, not one already on the host PATH.
    """
    bindir = _venv_bin_dir(env)
    if os.name == "nt":
        return f'set "PATH={bindir};%PATH%" && '
    return f'PATH="{bindir}:$PATH" '


@dataclass(frozen=True)
class CommandResult:
    command: str
    returncode: int
    ok: bool
    output_tail: str = ""

    def as_dict(self) -> dict:
        return {
            "command": self.command,
            "returncode": self.returncode,
            "ok": self.ok,
            "output_tail": self.output_tail,
        }


class CommandRunner(Protocol):
    """Runs a shell command and returns a :class:`CommandResult`."""

    def run(self, command: str) -> CommandResult:
        ...


class SubprocessCommandRunner:
    """Real subprocess runner — EXPENSIVE, for an explicit e2e run only.

    Reproduce an e2e run by injecting this runner; CI/unit runs inject a fake so
    the gates stay deterministic and fast.
    """

    def run(self, command: str) -> CommandResult:  # pragma: no cover - e2e only
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=_REAL_RUN_TIMEOUT
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        return CommandResult(
            command=command,
            returncode=proc.returncode,
            ok=proc.returncode == 0,
            output_tail=out[-500:],
        )


# --------------------------------------------------------------------------- #
# core_smoke_install (api_55f0757f)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SmokeInstallInput:
    wheel_path: Path
    python_version: str = ""
    commands: tuple[str, ...] = ()
    expected_imports: tuple[str, ...] = ()
    #: Isolated venv directory. The wheel is installed and imports run INSIDE this
    #: env (never the current interpreter), satisfying ts_e4f4c2ac.
    isolated_env: Path | None = None
    #: Base interpreter used to CREATE the isolated venv.
    base_python: str = "python"


class CoreSmokeInstallGate:
    """Installs the core-pure wheel in an ISOLATED venv and runs smoke commands.

    The wheel install, expected imports and (optionally) the smoke commands are
    executed with the venv's own pip/python — never the current interpreter — so a
    clean core-pure install is proven without Community present (ts_e4f4c2ac).
    """

    gate_id = "core_smoke_install"

    def run(self, gate_input: SmokeInstallInput, *, runner: CommandRunner | None = None) -> GateReport:
        runner = runner or SubprocessCommandRunner()
        env = gate_input.isolated_env or (Path("build") / "core_smoke_venv")
        vpython = _venv_python(env)
        vpip = _venv_pip(env)
        results: list[CommandResult] = []
        import_checks: list[str] = []

        # 1. create an ISOLATED venv (not the current environment).
        create = runner.run(f"{gate_input.base_python} -m venv {env}")
        results.append(create)
        if not create.ok:
            return self._fail("install_failed", results, import_checks,
                              f"Could not create isolated venv at {env}.")

        # 2. install the wheel INTO the isolated venv.
        install = runner.run(f"{vpip} install {gate_input.wheel_path}")
        results.append(install)
        if not install.ok:
            return self._fail("install_failed", results, import_checks,
                              "Core wheel failed to install in the isolated environment.")

        # 3. expected imports run with the isolated venv's python.
        for module in gate_input.expected_imports:
            res = runner.run(f"{vpython} -c \"import {module}\"")
            results.append(res)
            import_checks.append(module)
            if not res.ok:
                return self._fail("smoke_failed", results, import_checks,
                                  f"Expected core import failed in isolated env: {module}.")

        # 4. additional smoke commands run with the venv's console scripts FIRST on
        #    PATH, so a console script resolves to the ISOLATED install, never a
        #    binary already on the host PATH.
        path_prefix = _venv_path_prefix(env)
        for command in gate_input.commands:
            res = runner.run(path_prefix + command)
            results.append(res)
            if not res.ok:
                return self._fail("smoke_failed", results, import_checks,
                                  f"Expected core smoke command failed: {command}.")

        return GateReport(
            gate_id=self.gate_id,
            subject="core wheel isolated install",
            status="passed",
            severity="low",
            evidence={
                "isolated_env": str(env),
                "commands": [r.as_dict() for r in results],
                "import_checks": import_checks,
            },
        )

    def _fail(self, error: str, results, import_checks, hint: str) -> GateReport:
        return GateReport(
            gate_id=self.gate_id,
            subject="core wheel isolated install",
            status="blocking",
            severity="high",
            owner="okto-pulse-core/architecture",
            evidence={
                "error": error,
                "commands": [r.as_dict() for r in results],
                "import_checks": import_checks,
            },
            remediation_hint=hint,
        )


# --------------------------------------------------------------------------- #
# community_rebuild_reinstall_smoke (api_2f50f077) + register-before-remove
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CommunitySmokeOracle:
    """Objective baseline for the Community rebuild/reinstall smoke."""

    baseline_ref: str
    route_inventory: tuple[str, ...] = ()
    mcp_tool_inventory: tuple[str, ...] = ()
    command_inventory: tuple[str, ...] = ()
    #: ``none`` unless a linked accepted decision documents a delta.
    delta_tolerance: str = "none"

    def is_complete(self) -> bool:
        return bool(self.route_inventory and self.mcp_tool_inventory and self.command_inventory)


@dataclass(frozen=True)
class CommunitySmokeInput:
    oracle: CommunitySmokeOracle | None
    observed_routes: tuple[str, ...] = ()
    observed_mcp_tools: tuple[str, ...] = ()
    build_ok: bool = True
    reinstall_ok: bool = True
    smoke_results: tuple[CommandResult, ...] = ()
    #: ``none`` | ``documented_decision_only``
    allowed_delta_policy: str = "none"
    accepted_decision_ref: str | None = None
    #: register-before-remove inputs
    removed_dependencies: tuple[str, ...] = ()
    community_adapters_registered: tuple[str, ...] = field(default_factory=tuple)


_RBR_OWNER = "okto-pulse-core/architecture"
_RBR_PROMOTION = (
    "Keep the dependency as baseline/xfail_advisory until the Community registers an "
    "equivalent adapter/entrypoint AND an approved smoke_oracle proves equivalence."
)


class CommunityRebuildReinstallSmokeGate:
    """Preserves Community behaviour + enforces register-before-remove."""

    gate_id = "community_rebuild_reinstall_smoke"

    def run(self, gate_input: CommunitySmokeInput) -> GateReport:
        oracle = gate_input.oracle

        # register-before-remove (tr_709b39d5 / ts_11275928): a dependency used by
        # Community can only be removed once an equivalent adapter is registered AND
        # an approved smoke_oracle exists. Either gap fails before promotion.
        if gate_input.removed_dependencies:
            registered = set(gate_input.community_adapters_registered)
            missing_adapter = sorted(d for d in gate_input.removed_dependencies if d not in registered)
            if missing_adapter:
                return self._fail(
                    "community_adapter_missing",
                    {"removed_dependencies": list(gate_input.removed_dependencies),
                     "missing_community_adapter": missing_adapter},
                    "A dependency removal was promoted before an equivalent Community "
                    "adapter/entrypoint was registered.",
                    observed=missing_adapter,
                )
            if oracle is None or not oracle.is_complete():
                return self._fail(
                    "smoke_oracle_missing",
                    {"removed_dependencies": list(gate_input.removed_dependencies)},
                    "An approved smoke_oracle (routes/MCP tools/commands) is required "
                    "before removing a Community-used dependency.",
                )

        if oracle is None or not oracle.is_complete():
            return self._fail(
                "smoke_oracle_missing",
                {"oracle_present": oracle is not None},
                "Expected routes, MCP tools or smoke commands were not supplied.",
            )

        if not (gate_input.build_ok and gate_input.reinstall_ok):
            return self._fail(
                "community_rebuild_failed",
                {"build_ok": gate_input.build_ok, "reinstall_ok": gate_input.reinstall_ok},
                "Community could not rebuild/reinstall against the core artifact.",
            )

        documented = (
            gate_input.allowed_delta_policy == "documented_decision_only"
            and gate_input.accepted_decision_ref is not None
        )
        route_delta = sorted(set(gate_input.observed_routes) ^ set(oracle.route_inventory))
        mcp_tool_delta = sorted(set(gate_input.observed_mcp_tools) ^ set(oracle.mcp_tool_inventory))
        failed_smoke = [r for r in gate_input.smoke_results if not r.ok]
        # Every expected smoke command in the oracle MUST be observed passing.
        # Behaviour cannot be declared preserved without running the smoke.
        missing_commands = [
            cmd
            for cmd in oracle.command_inventory
            if not any(cmd in r.command and r.ok for r in gate_input.smoke_results)
        ]

        evidence = {
            "install": {"build_ok": gate_input.build_ok, "reinstall_ok": gate_input.reinstall_ok},
            "smoke": [r.as_dict() for r in gate_input.smoke_results],
            "route_delta": route_delta,
            "mcp_tool_delta": mcp_tool_delta,
            "missing_smoke_commands": missing_commands,
            "external_behavior_delta": "documented" if documented else "none",
            "baseline_ref": oracle.baseline_ref,
        }

        # Command coverage is a HARD requirement — a documented decision waives a
        # route/MCP delta, never the absence of expected smoke evidence.
        if missing_commands:
            return self._fail("community_regression", evidence,
                              "Expected smoke commands from the oracle were not observed passing.",
                              observed=missing_commands)
        if mcp_tool_delta and not documented:
            return self._fail("mcp_tool_inventory_delta", evidence,
                              "MCP tool inventory changed outside the allowed delta policy.",
                              observed=mcp_tool_delta)
        if (route_delta or failed_smoke) and not documented:
            return self._fail("community_regression", evidence,
                              "Community behaviour changed without an accepted decision.",
                              observed=route_delta or [r.command for r in failed_smoke])
        return GateReport(
            gate_id=self.gate_id,
            subject="Community rebuild/reinstall smoke",
            status="passed",
            severity="low",
            evidence=evidence,
        )

    def _fail(self, error: str, evidence: dict, hint: str, *, observed=None) -> GateReport:
        rbr = error in ("community_adapter_missing", "smoke_oracle_missing")
        return GateReport(
            gate_id=self.gate_id,
            subject="Community rebuild/reinstall smoke",
            status="blocking",
            severity="high",
            owner=_RBR_OWNER,
            evidence={**evidence, "error": error},
            promotion_criteria=_RBR_PROMOTION if rbr else None,
            observed_value=observed,
            expected_value=[] if observed is not None else None,
            remediation_hint=hint,
        )


__all__ = [
    "CommandResult",
    "CommandRunner",
    "CommunityRebuildReinstallSmokeGate",
    "CommunitySmokeInput",
    "CommunitySmokeOracle",
    "CoreSmokeInstallGate",
    "SmokeInstallInput",
    "SubprocessCommandRunner",
]
