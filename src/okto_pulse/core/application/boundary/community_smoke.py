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
import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

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
COMMUNITY_SMOKE_EVIDENCE_SCHEMA_VERSION = "1"
COMMUNITY_SMOKE_EVIDENCE_PRODUCER = "okto-pulse-community"
COMMUNITY_SMOKE_EVIDENCE_ARTIFACT = "community_runtime_smoke_evidence.json"
COMMUNITY_SMOKE_AXIS = "community_smoke"
COMMUNITY_SMOKE_BASELINE_POLICY = "exact"
COMMUNITY_SMOKE_REQUIRED_SURFACES: tuple[str, ...] = (
    "install",
    "imports",
    "composition",
    "seed",
    "routes",
    "mcp_tools",
    "cli_commands",
    "metadata",
)
COMMUNITY_SMOKE_ALLOWED_STATUSES = frozenset(
    {"passed", "baseline", "xfail_advisory", "blocking", "reject"}
)


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


@dataclass(frozen=True)
class CommunitySmokeEvidenceInput:
    """Structured Community-owned smoke evidence consumed by the core gate.

    The payload is data only. The core must not import Community modules, run a
    subprocess, or rebuild the runtime when validating this handoff.
    """

    payload: Mapping[str, Any] | str | None
    now: datetime | None = None
    expected_core_commit: str | None = None
    expected_community_commit: str | None = None
    expected_core_version: str | None = None
    expected_community_version: str | None = None
    expected_wheel_hashes: Mapping[str, str] | None = None
    expected_removed_dependencies: tuple[str, ...] = ()


_RBR_OWNER = "okto-pulse-core/architecture"
_RBR_PROMOTION = (
    "Keep the dependency as baseline/xfail_advisory until the Community registers an "
    "equivalent adapter/entrypoint AND an approved smoke_oracle proves equivalence."
)


def _load_evidence_payload(payload: Mapping[str, Any] | str) -> Mapping[str, Any]:
    if isinstance(payload, str):
        loaded = json.loads(payload)
        if not isinstance(loaded, Mapping):
            raise TypeError("Community smoke evidence JSON must decode to an object")
        return loaded
    return payload


def _parse_evidence_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    return [item for item in value if isinstance(item, str)]


def _check_status_is_passed(raw_checks: object, check_name: str) -> bool:
    if not isinstance(raw_checks, Mapping):
        return False
    raw_check = raw_checks.get(check_name)
    return isinstance(raw_check, Mapping) and raw_check.get("status") == "passed"


class CommunityRebuildReinstallSmokeGate:
    """Preserves Community behaviour + enforces register-before-remove."""

    gate_id = "community_rebuild_reinstall_smoke"

    def run_evidence(self, gate_input: CommunitySmokeEvidenceInput) -> GateReport:
        """Validate Community smoke evidence without running Community code."""
        if gate_input.payload is None or gate_input.payload == "":
            return self._fail(
                "smoke_evidence_missing",
                {"error": "smoke_evidence_missing"},
                "Community smoke evidence was not provided.",
            )
        try:
            payload = _load_evidence_payload(gate_input.payload)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            return self._fail(
                "smoke_evidence_malformed",
                {"error": "smoke_evidence_malformed", "details": str(exc)},
                "Community smoke evidence is not a valid JSON object.",
            )

        findings: list[dict[str, object]] = []

        def add(code: str, field: str, message: str, **extra: object) -> None:
            findings.append({"code": code, "field": field, "message": message, **extra})

        def require_equal(field: str, expected: object) -> None:
            actual = payload.get(field)
            if actual != expected:
                add(
                    "smoke_evidence_malformed",
                    field,
                    f"{field} must be {expected!r}.",
                    expected=expected,
                    actual=actual,
                )

        require_equal("schema_version", COMMUNITY_SMOKE_EVIDENCE_SCHEMA_VERSION)
        require_equal("producer", COMMUNITY_SMOKE_EVIDENCE_PRODUCER)
        require_equal("artifact_name", COMMUNITY_SMOKE_EVIDENCE_ARTIFACT)

        generated_at = _parse_evidence_datetime(payload.get("generated_at"))
        if generated_at is None:
            add(
                "smoke_evidence_malformed",
                "generated_at",
                "generated_at must be an ISO-8601 timestamp.",
            )
        else:
            max_age_raw = payload.get("max_age_seconds")
            try:
                max_age_seconds = int(max_age_raw)
            except (TypeError, ValueError):
                max_age_seconds = -1
            if max_age_seconds <= 0:
                add(
                    "smoke_evidence_malformed",
                    "max_age_seconds",
                    "max_age_seconds must be a positive integer.",
                )
            else:
                clock = (gate_input.now or datetime.now(timezone.utc)).astimezone(timezone.utc)
                if (clock - generated_at).total_seconds() > max_age_seconds:
                    add(
                        "smoke_evidence_stale",
                        "generated_at",
                        "Community smoke evidence is older than max_age_seconds.",
                    )

        expected_fields = (
            ("core_commit", gate_input.expected_core_commit),
            ("community_commit", gate_input.expected_community_commit),
            ("core_version", gate_input.expected_core_version),
            ("community_version", gate_input.expected_community_version),
        )
        for field_name, expected in expected_fields:
            if expected is not None and payload.get(field_name) != expected:
                add(
                    "smoke_evidence_mismatch",
                    field_name,
                    f"{field_name} does not match the expected value.",
                    expected=expected,
                    actual=payload.get(field_name),
                )

        wheel_hashes = payload.get("wheel_hashes")
        if not isinstance(wheel_hashes, Mapping):
            add(
                "smoke_evidence_malformed",
                "wheel_hashes",
                "wheel_hashes must contain core and community hashes.",
            )
            wheel_hashes = {}
        else:
            for wheel_key in ("core", "community"):
                if not wheel_hashes.get(wheel_key):
                    add(
                        "smoke_evidence_malformed",
                        f"wheel_hashes.{wheel_key}",
                        f"wheel_hashes.{wheel_key} is required.",
                    )
            for wheel_key, expected in (gate_input.expected_wheel_hashes or {}).items():
                if wheel_hashes.get(wheel_key) != expected:
                    add(
                        "smoke_evidence_mismatch",
                        f"wheel_hashes.{wheel_key}",
                        f"wheel_hashes.{wheel_key} does not match.",
                        expected=expected,
                        actual=wheel_hashes.get(wheel_key),
                    )

        commands_executed = _string_list(payload.get("commands_executed"))
        if not commands_executed:
            add(
                "smoke_evidence_malformed",
                "commands_executed",
                "commands_executed must contain the Community runner commands.",
            )

        artifact_paths = payload.get("artifact_paths")
        if not isinstance(artifact_paths, Mapping) or not artifact_paths:
            add(
                "smoke_evidence_malformed",
                "artifact_paths",
                "artifact_paths must contain the Community smoke artifacts.",
            )
            artifact_paths = {}

        gate_report = payload.get("gate_report")
        if not isinstance(gate_report, Mapping):
            add(
                "smoke_evidence_malformed",
                "gate_report",
                "gate_report must be an object.",
            )
            gate_report = {}
        else:
            if gate_report.get("axis") != COMMUNITY_SMOKE_AXIS:
                add(
                    "smoke_evidence_malformed",
                    "gate_report.axis",
                    "gate_report.axis must be community_smoke.",
                    actual=gate_report.get("axis"),
                )
            status = gate_report.get("status")
            if status not in COMMUNITY_SMOKE_ALLOWED_STATUSES:
                add(
                    "smoke_evidence_malformed",
                    "gate_report.status",
                    "gate_report.status is not a canonical GateReport status.",
                    actual=status,
                )
            elif status != "passed":
                add(
                    "smoke_evidence_failing",
                    "gate_report.status",
                    "Community smoke GateReport.status is not passed.",
                    actual=status,
                )
            if gate_report.get("baseline_policy") != COMMUNITY_SMOKE_BASELINE_POLICY:
                add(
                    "smoke_evidence_failing",
                    "gate_report.baseline_policy",
                    "Community smoke baseline_policy must be exact.",
                    actual=gate_report.get("baseline_policy"),
                )
            required = set(_string_list(gate_report.get("required_surfaces")))
            missing_surfaces = sorted(set(COMMUNITY_SMOKE_REQUIRED_SURFACES) - required)
            if missing_surfaces:
                add(
                    "smoke_evidence_malformed",
                    "gate_report.required_surfaces",
                    "Community smoke evidence is missing required surfaces.",
                    missing=missing_surfaces,
                )
            symmetric_diff = gate_report.get("symmetric_diff")
            if not isinstance(symmetric_diff, Mapping):
                add(
                    "smoke_evidence_malformed",
                    "gate_report.symmetric_diff",
                    "symmetric_diff must be an object for exact baselines.",
                )
            else:
                for surface in ("routes", "mcp_tools"):
                    delta = symmetric_diff.get(surface)
                    if not isinstance(delta, Mapping):
                        add(
                            "smoke_evidence_malformed",
                            f"gate_report.symmetric_diff.{surface}",
                            f"symmetric_diff.{surface} must contain missing/extra lists.",
                        )
                        continue
                    missing = _string_list(delta.get("missing"))
                    extra = _string_list(delta.get("extra"))
                    if missing or extra:
                        add(
                            "smoke_evidence_failing",
                            f"gate_report.symmetric_diff.{surface}",
                            "Community smoke exact baseline has a symmetric diff.",
                            missing=missing,
                            extra=extra,
                        )

        raw_checks = payload.get("checks")
        for check_name in ("install", "imports", "composition", "seed", "routes", "mcp", "cli", "metadata"):
            if not _check_status_is_passed(raw_checks, check_name):
                add(
                    "smoke_evidence_failing",
                    f"checks.{check_name}",
                    f"Community smoke check {check_name!r} is missing or not passed.",
                )

        expected_removed = set(gate_input.expected_removed_dependencies)
        raw_rbr = payload.get("register_before_remove")
        rbr_removed: set[str] = set()
        if isinstance(raw_rbr, Mapping):
            rbr_removed = set(_string_list(raw_rbr.get("removed_dependencies")))
        if expected_removed or rbr_removed:
            if not isinstance(raw_rbr, Mapping):
                add(
                    "smoke_oracle_missing",
                    "register_before_remove",
                    "register_before_remove evidence is required for Community cleanup.",
                )
            else:
                missing_removed = sorted(expected_removed - rbr_removed)
                if missing_removed:
                    add(
                        "community_adapter_missing",
                        "register_before_remove.removed_dependencies",
                        "Removed dependencies are not declared by the evidence package.",
                        missing=missing_removed,
                    )
                adapters = set(_string_list(raw_rbr.get("community_adapters_registered")))
                missing_adapters = sorted((expected_removed or rbr_removed) - adapters)
                if missing_adapters:
                    add(
                        "community_adapter_missing",
                        "register_before_remove.community_adapters_registered",
                        "Equivalent Community adapters are missing for removed dependencies.",
                        missing=missing_adapters,
                    )
                smoke_oracle = raw_rbr.get("smoke_oracle")
                if not isinstance(smoke_oracle, Mapping) or smoke_oracle.get("status") != "passed":
                    add(
                        "smoke_oracle_missing",
                        "register_before_remove.smoke_oracle",
                        "A passed smoke_oracle is required before Community cleanup.",
                    )
                else:
                    community_commit = payload.get("community_commit")
                    community_hash = wheel_hashes.get("community") if isinstance(wheel_hashes, Mapping) else None
                    if community_commit and smoke_oracle.get("commit") != community_commit:
                        add(
                            "smoke_oracle_missing",
                            "register_before_remove.smoke_oracle.commit",
                            "smoke_oracle commit does not match the evidence package.",
                            expected=community_commit,
                            actual=smoke_oracle.get("commit"),
                        )
                    if community_hash and smoke_oracle.get("wheel_hash") != community_hash:
                        add(
                            "smoke_oracle_missing",
                            "register_before_remove.smoke_oracle.wheel_hash",
                            "smoke_oracle wheel_hash does not match the Community wheel hash.",
                            expected=community_hash,
                            actual=smoke_oracle.get("wheel_hash"),
                        )

        if findings:
            first = str(findings[0]["code"])
            return self._fail(
                first,
                {
                    "error": first,
                    "diagnostics": findings,
                    "core_commit": payload.get("core_commit"),
                    "community_commit": payload.get("community_commit"),
                },
                "Produce fresh, passed Community runtime smoke evidence with exact "
                "baseline diffs, matching commits/wheel hashes and register-before-remove proof.",
                observed=findings,
            )

        return GateReport(
            gate_id=self.gate_id,
            subject="Community runtime smoke evidence",
            status="passed",
            severity="low",
            owner="okto-pulse-community/runtime-smoke",
            evidence={
                "artifact_name": payload.get("artifact_name"),
                "axis": COMMUNITY_SMOKE_AXIS,
                "baseline_policy": COMMUNITY_SMOKE_BASELINE_POLICY,
                "generated_at": payload.get("generated_at"),
                "core_commit": payload.get("core_commit"),
                "community_commit": payload.get("community_commit"),
                "wheel_hashes": dict(wheel_hashes),
                "commands_executed": commands_executed,
                "artifact_paths": dict(artifact_paths),
                "required_surfaces": list(COMMUNITY_SMOKE_REQUIRED_SURFACES),
                "register_before_remove": raw_rbr if isinstance(raw_rbr, Mapping) else None,
                "diagnostics": list(gate_report.get("diagnostics") or []),
            },
        )

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
    "COMMUNITY_SMOKE_AXIS",
    "COMMUNITY_SMOKE_BASELINE_POLICY",
    "COMMUNITY_SMOKE_EVIDENCE_ARTIFACT",
    "COMMUNITY_SMOKE_EVIDENCE_PRODUCER",
    "COMMUNITY_SMOKE_EVIDENCE_SCHEMA_VERSION",
    "COMMUNITY_SMOKE_REQUIRED_SURFACES",
    "CommunityRebuildReinstallSmokeGate",
    "CommunitySmokeEvidenceInput",
    "CommunitySmokeInput",
    "CommunitySmokeOracle",
    "CoreSmokeInstallGate",
    "SmokeInstallInput",
    "SubprocessCommandRunner",
]
