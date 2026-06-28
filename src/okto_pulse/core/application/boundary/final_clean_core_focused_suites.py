"""FCC-07E-IMP3 — Final Clean Core runner FOCUSED-SUITE execution + failure map.

This module registers and runs the FOCUSED pytest suites of the FCC-07E "final
clean core" runner and maps each result into the FCC-07E-IMP1 contract
(:class:`~okto_pulse.core.application.boundary.final_clean_core_runner.RunnerCommandResult`
plus a per-suite
:class:`~okto_pulse.core.application.boundary.final_clean_core_runner.RunnerGateResult`).
It is the layer the E-IMP1 docstring deferred ("It NEVER ... runs pytest
(FCC-07E-IMP3)").

Scope (closed, ADDITIVE, deterministic, synchronous):

* A DECLARATIVE :class:`FocusedSuite` set (:data:`DEFAULT_FOCUSED_SUITES`) names,
  per gate group, the REAL repo test files and the focused ``uv run pytest`` command
  that exercises them. The five required groups are covered: adapter readiness
  (FCC-07B), dependency/conformance (FCC-07A/C), fake/registry provider guard
  (FCC-07D), community adapter/wiring, and composition/startup smoke.
* :func:`run_focused_suites` runs each suite through an INJECTABLE
  :class:`~okto_pulse.core.application.boundary.community_smoke.CommandRunner`
  (the #12 pattern the E-IMP4 full-mode smoke reused): a real
  :class:`~okto_pulse.core.application.boundary.community_smoke.SubprocessCommandRunner`
  for an actual run, a deterministic fake in a unit test (so the unit test NEVER
  shells out to a real pytest). Each suite yields one ``RunnerCommandResult``
  (command / exit_code / test_files / owner) and one ``RunnerGateResult``.
* The KEY anti-requirement (FR6/AC6): a FAILED suite (``exit_code != 0``) is NEVER
  collapsed into a generic "pytest failed" message. Its ``RunnerGateResult`` is
  ``blocked`` and carries a SPECIFIC, actionable remediation containing the exact
  command, the exact failing test files, and the owning gate — everything needed
  to reproduce and fix the failure without re-deriving it.
* No Pulse server is needed for any of this — every check is local pytest run
  through the injected runner.

Out of scope (NOT reimplemented): the E-IMP1 shell
(:func:`~okto_pulse.core.application.boundary.final_clean_core_runner.run_final_clean_core`,
reused verbatim by :func:`run_final_clean_core_quick`), the E-IMP2 A/B/C/D
orchestration, the E-IMP4 full-mode smoke, and docs (E-IMP5).

Boundary hygiene: imports ONLY public core symbols (the ``CommandRunner`` /
``CommandResult`` / ``SubprocessCommandRunner`` live in
``core/application/boundary/community_smoke.py`` — that is CORE, importable here)
and stdlib. It NEVER imports ``okto_pulse.community`` and touches no private
symbol of any other module.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from .community_smoke import CommandRunner, SubprocessCommandRunner
from .final_clean_core_runner import (
    CommandInput,
    GateInput,
    RunnerCommandResult,
    RunnerGateResult,
    RunnerReport,
    run_final_clean_core,
)

#: The pytest launcher used by every focused suite (the repo convention is
#: ``uv run pytest`` from the project root).
PYTEST_LAUNCHER: tuple[str, ...] = ("uv", "run", "pytest")

#: Flags appended to every focused command (deterministic, no cache provider).
PYTEST_FLAGS: tuple[str, ...] = ("-p", "no:cacheprovider", "-q")

# --------------------------------------------------------------------------- #
# Stable suite gate ids — one per required group (used as RunnerGateResult.gate_id
# and to prove the default set covers every group).
# --------------------------------------------------------------------------- #
SUITE_ADAPTER_READINESS = "FCC07B"
SUITE_DEPENDENCY_CONFORMANCE = "FCC07A_C"
SUITE_FAKE_REGISTRY_GUARD = "FCC07D"
SUITE_COMMUNITY_WIRING = "community_wiring"
SUITE_COMPOSITION_SMOKE = "composition_smoke"

#: The five gate ids the default focused-suite set MUST cover (FR6 group coverage).
REQUIRED_SUITE_GATE_IDS: tuple[str, ...] = (
    SUITE_ADAPTER_READINESS,
    SUITE_DEPENDENCY_CONFORMANCE,
    SUITE_FAKE_REGISTRY_GUARD,
    SUITE_COMMUNITY_WIRING,
    SUITE_COMPOSITION_SMOKE,
)


@dataclass(frozen=True)
class FocusedSuite:
    """One declarative focused pytest suite for a clean-core gate group.

    ``test_files`` are the REAL repo test paths the suite exercises; the focused
    command is derived from them as ``uv run pytest <files> -p no:cacheprovider
    -q`` (overridable via ``command_override`` to satisfy the "argv OR string"
    contract). ``gate_id`` / ``owner`` are the gate identity stamped on the
    produced runner rows and quoted verbatim in a failure remediation.
    """

    gate_id: str
    owner: str
    test_files: tuple[str, ...]
    spec_id: str = ""
    description: str = ""
    #: Optional explicit command string; when ``None`` the command is derived
    #: from ``test_files`` so the two can never silently drift.
    command_override: str | None = None

    def __post_init__(self) -> None:
        if not self.gate_id:
            raise ValueError("FocusedSuite.gate_id must be a non-empty string.")
        if not self.test_files:
            raise ValueError(
                f"FocusedSuite {self.gate_id!r} must declare at least one test file "
                "(a focused suite with no tests can never prove the gate)."
            )

    def command_argv(self) -> tuple[str, ...]:
        """The focused pytest argv (launcher + the real test files + flags)."""
        return (*PYTEST_LAUNCHER, *self.test_files, *PYTEST_FLAGS)

    def command(self) -> str:
        """The focused command STRING run through the injected ``CommandRunner``."""
        if self.command_override is not None:
            return self.command_override
        return " ".join(self.command_argv())


# --------------------------------------------------------------------------- #
# The default focused-suite set — one suite per required group, each naming the
# REAL repo test files for that group.
# --------------------------------------------------------------------------- #
DEFAULT_FOCUSED_SUITES: tuple[FocusedSuite, ...] = (
    # Group 1 — adapter readiness (FCC-07B aggregator + binding + R05-A inventory).
    FocusedSuite(
        gate_id=SUITE_ADAPTER_READINESS,
        owner="okto-pulse-core/boundary",
        spec_id="FCC-07B",
        description="adapter readiness aggregator + removal binding + inventory",
        test_files=(
            "tests/test_fcc07b_readiness_aggregator.py",
            "tests/test_fcc07b_removal_binding.py",
            "tests/test_r05a_adapter_readiness_inventory.py",
        ),
    ),
    # Group 2 — dependency / conformance (FCC-07A matrix + FCC-07C ownership/audit).
    FocusedSuite(
        gate_id=SUITE_DEPENDENCY_CONFORMANCE,
        owner="okto-pulse-core/architecture",
        spec_id="FCC-07A/C",
        description="conformance matrix + packaging ownership + community audit",
        test_files=(
            "tests/test_fcc07a_conformance_matrix.py",
            "tests/test_fcc07c_packaging_gate.py",
            "tests/test_fcc07c_community_audit.py",
        ),
    ),
    # Group 3 — fake / registry provider guard (FCC-07D policy + runtime guard).
    FocusedSuite(
        gate_id=SUITE_FAKE_REGISTRY_GUARD,
        owner="okto-pulse-core/runtime",
        spec_id="FCC-07D",
        description="test-only provider policy + runtime composition provider guard",
        test_files=(
            "tests/test_fcc07d_test_provider_policy.py",
            "tests/test_fcc07d_runtime_guard.py",
        ),
    ),
    # Group 4 — community adapter / wiring (#12 core/Community smoke gates).
    FocusedSuite(
        gate_id=SUITE_COMMUNITY_WIRING,
        owner="okto-pulse-core/architecture",
        spec_id="spec-12",
        description="core/Community smoke install + register-before-remove wiring",
        test_files=("tests/test_community_smoke_12b.py",),
    ),
    # Group 5 — composition / startup smoke (RuntimeComposition + boundary audit).
    FocusedSuite(
        gate_id=SUITE_COMPOSITION_SMOKE,
        owner="okto-pulse-core/runtime",
        spec_id="spec-03",
        description="runtime composition + create_app strict + boundary audit suite",
        test_files=(
            "tests/test_runtime_composition_03.py",
            "tests/test_boundary_audit_12.py",
        ),
    ),
)


@dataclass(frozen=True)
class FocusedSuitesResult:
    """The focused-suite outcome: one ``RunnerCommandResult`` AND one
    ``RunnerGateResult`` per suite, both sorted by the shell's ordering keys so the
    result is deterministic regardless of suite iteration order.

    ``command_results`` feeds the shell's ``command_results`` (a non-zero
    ``exit_code`` forces the fail-closed exit); ``gate_results`` feeds the shell's
    ``gate_results`` and carries the SPECIFIC failure remediation on a blocked
    suite.
    """

    command_results: tuple[RunnerCommandResult, ...] = ()
    gate_results: tuple[RunnerGateResult, ...] = field(default_factory=tuple)

    @property
    def all_passed(self) -> bool:
        return not any(command.failed for command in self.command_results)

    def as_dict(self) -> dict[str, object]:
        """Stable, JSON-serialisable projection."""
        return {
            "command_results": [command.as_dict() for command in self.command_results],
            "gate_results": [gate.as_dict() for gate in self.gate_results],
        }


def _focused_remediation(suite: FocusedSuite, command: str, exit_code: int) -> str:
    """The SPECIFIC, actionable remediation for a FAILED focused suite (FR6/AC6).

    It quotes the EXACT command, the EXACT failing test files, the owning gate and
    the exit code — never a generic "pytest failed". Everything needed to
    reproduce and fix the failure is on the row, so the failure is not re-derived.
    """
    files = ", ".join(suite.test_files)
    return (
        f"focused suite '{suite.gate_id}' (owner={suite.owner}) FAILED with "
        f"exit_code={exit_code}. Re-run and fix the failing tests: `{command}`. "
        f"Failing test files: {files}. This gate blocks the final clean core "
        f"until these tests pass."
    )


def run_focused_suites(
    *,
    suites: Iterable[FocusedSuite] = DEFAULT_FOCUSED_SUITES,
    command_runner: CommandRunner | None = None,
    emit_gate_results: bool = True,
) -> FocusedSuitesResult:
    """Run each focused suite through the injected runner and map the results.

    Parameters
    ----------
    suites:
        The focused suites to run (defaults to :data:`DEFAULT_FOCUSED_SUITES`).
    command_runner:
        An INJECTABLE :class:`CommandRunner` (the #12 pattern). ``None`` uses the
        real :class:`SubprocessCommandRunner` (the actual subprocess path); a unit
        test injects a deterministic fake so NO real pytest is ever spawned.
    emit_gate_results:
        When True (default) a :class:`RunnerGateResult` is produced per suite — a
        ``success`` row on a zero exit, a ``blocked`` row with the SPECIFIC
        failure remediation otherwise.

    Returns
    -------
    FocusedSuitesResult
        Per-suite command rows + gate rows, each sorted by the shell's ordering
        keys. A non-zero ``exit_code`` maps to a ``blocked`` gate whose
        remediation names the command, the failing test files and the gate (never
        a generic message).
    """
    runner = command_runner or SubprocessCommandRunner()
    # Stable execution order (deterministic regardless of caller iteration order).
    ordered_suites = sorted(suites, key=lambda suite: (suite.gate_id, suite.command()))

    command_rows: list[RunnerCommandResult] = []
    gate_rows: list[RunnerGateResult] = []
    for suite in ordered_suites:
        command = suite.command()
        result = runner.run(command)
        exit_code = result.returncode
        command_rows.append(
            RunnerCommandResult(
                command=command,
                exit_code=exit_code,
                test_files=suite.test_files,
                owner=suite.owner,
            )
        )
        if not emit_gate_results:
            continue
        if exit_code == 0:
            gate_rows.append(
                RunnerGateResult(
                    gate_id=suite.gate_id,
                    spec_id=suite.spec_id,
                    status="success",
                    owner=suite.owner,
                    command=command,
                    test_file=", ".join(suite.test_files),
                )
            )
        else:
            gate_rows.append(
                RunnerGateResult(
                    gate_id=suite.gate_id,
                    spec_id=suite.spec_id,
                    status="blocked",
                    owner=suite.owner,
                    command=command,
                    test_file=", ".join(suite.test_files),
                    remediation=_focused_remediation(suite, command, exit_code),
                )
            )

    command_rows.sort(key=RunnerCommandResult.sort_key)
    gate_rows.sort(key=RunnerGateResult.sort_key)
    return FocusedSuitesResult(
        command_results=tuple(command_rows),
        gate_results=tuple(gate_rows),
    )


def run_final_clean_core_quick(
    *,
    suites: Iterable[FocusedSuite] = DEFAULT_FOCUSED_SUITES,
    command_runner: CommandRunner | None = None,
    gate_results: Iterable[GateInput] = (),
    command_results: Iterable[CommandInput] = (),
    required_gate_ids: Iterable[str] | None = None,
) -> RunnerReport:
    """Reuse the E-IMP1 shell in ``quick`` mode, INCORPORATING the focused suites.

    Runs the focused suites (:func:`run_focused_suites`) and concatenates their
    gate + command rows with any injected A/B/C/D rows, then calls the shell with
    ``mode='quick'``. The shell (:func:`run_final_clean_core`) is reused verbatim —
    NOT reimplemented — so the fail-closed exit code, ordering and schema are
    unchanged.
    """
    focused = run_focused_suites(suites=suites, command_runner=command_runner)
    return run_final_clean_core(
        mode="quick",
        gate_results=[*gate_results, *focused.gate_results],
        command_results=[*command_results, *focused.command_results],
        required_gate_ids=required_gate_ids,
    )


__all__ = [
    "PYTEST_LAUNCHER",
    "PYTEST_FLAGS",
    "SUITE_ADAPTER_READINESS",
    "SUITE_DEPENDENCY_CONFORMANCE",
    "SUITE_FAKE_REGISTRY_GUARD",
    "SUITE_COMMUNITY_WIRING",
    "SUITE_COMPOSITION_SMOKE",
    "REQUIRED_SUITE_GATE_IDS",
    "FocusedSuite",
    "DEFAULT_FOCUSED_SUITES",
    "FocusedSuitesResult",
    "run_focused_suites",
    "run_final_clean_core_quick",
]
