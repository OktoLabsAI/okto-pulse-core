"""FCC-07E-IMP4 — Final Clean Core runner FULL-MODE smoke integration.

This module wires the two #12 smoke gates
(:class:`~okto_pulse.core.application.boundary.community_smoke.CoreSmokeInstallGate`
and
:class:`~okto_pulse.core.application.boundary.community_smoke.CommunityRebuildReinstallSmokeGate`)
into the FCC-07E ``full`` mode and incorporates their verdicts into the
FCC-07E-IMP1 runner report as
:class:`~okto_pulse.core.application.boundary.final_clean_core_runner.RunnerGateResult`
rows. It is the layer the E-IMP1 docstring deferred ("NEVER runs the full-mode
smoke with a real venv (FCC-07E-IMP4)").

Scope (closed, ADDITIVE, deterministic, synchronous):

* An INJECTABLE :class:`SmokeGateAdapter` Protocol abstracts each smoke gate so
  tests inject a fake that returns a deterministic :class:`GateReport` WITHOUT
  creating a real venv or installing a wheel. The real adapters
  (:class:`CoreSmokeInstallAdapter` / :class:`CommunityRebuildReinstallSmokeAdapter`)
  wrap the concrete #12 gates and confine every filesystem write to a temporary
  sandbox.
* :func:`run_full_mode_smoke` ISOLATES the install in a temporary directory (via
  :class:`tempfile.TemporaryDirectory`) when the full prerequisites are present,
  CLEANS the sandbox at the end (and ONLY the sandbox — never a user file), and
  MAPS each smoke ``GateReport`` to a ``RunnerGateResult`` (and a supplementary
  ``RunnerCommandResult``).
* The KEY anti-requirement: when the full prerequisites (a built wheel / venv
  tooling / a writable temp root) are ABSENT, each gate is recorded as
  ``skipped`` with an HONEST reason ("full prerequisites unavailable: …") — the
  gate is NEVER even run, so a missing prerequisite can never be silently
  reported as a fake ``success``.

Out of scope (NOT reimplemented): the E-IMP1 shell
(:func:`~okto_pulse.core.application.boundary.final_clean_core_runner.run_final_clean_core`,
reused verbatim by :func:`run_final_clean_core_full_mode`), the E-IMP2 A/B/C/D
orchestration, the E-IMP3 focused pytest, and docs (E-IMP5).

Boundary hygiene: imports ONLY public core symbols (the smoke gates live in
``core/application/boundary/community_smoke.py`` — that is CORE, importable here)
and stdlib. It NEVER imports ``okto_pulse.community`` and touches no private
symbol of any other module.
"""

from __future__ import annotations

import importlib.util
import os
import re
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from .community_smoke import (
    CommandRunner,
    CommunityRebuildReinstallSmokeGate,
    CommunitySmokeInput,
    CoreSmokeInstallGate,
    SmokeInstallInput,
)
from .final_clean_core_runner import (
    CommandInput,
    GateInput,
    RunnerCommandResult,
    RunnerGateResult,
    RunnerGateStatus,
    RunnerReport,
    run_final_clean_core,
)
from .report import GateReport

#: Default human spec label for the full-mode smoke gate rows.
FULL_MODE_SPEC_ID = "FCC-07E"

#: Default owner for the produced rows (purely descriptive).
DEFAULT_FULL_SMOKE_OWNER = "okto-pulse-core/architecture"

#: Collapse a #12 :class:`GateReport` status into the E-IMP1 ``RunnerGateStatus``
#: enum (``success`` | ``blocked`` | ``skipped``). The smoke gates emit
#: ``passed`` / ``blocking``; ``baseline`` / ``xfail_advisory`` (advisory waivers)
#: collapse to ``skipped`` — NEVER a fake ``success`` — and ``reject`` to
#: ``blocked``. An unknown status fails closed to ``blocked`` (see
#: :func:`_runner_status_for`).
GATE_REPORT_STATUS_TO_RUNNER: dict[str, RunnerGateStatus] = {
    "passed": "success",
    "blocking": "blocked",
    "reject": "blocked",
    "baseline": "skipped",
    "xfail_advisory": "skipped",
}


# --------------------------------------------------------------------------- #
# Temporary isolation handed to each adapter.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SmokeIsolation:
    """A per-gate temporary sandbox handed to a :class:`SmokeGateAdapter`.

    Every filesystem write the adapter performs (the isolated venv, build/install
    artifacts) MUST stay under :attr:`root`. The caller (:func:`run_full_mode_smoke`)
    owns the lifecycle and deletes ``root`` — and ONLY ``root`` — when the run
    finishes, so nothing the adapter creates leaks into the user's environment.
    """

    root: Path

    def venv_dir(self) -> Path:
        """Conventional location for the isolated venv INSIDE the sandbox."""
        return self.root / "venv"


# --------------------------------------------------------------------------- #
# The injectable adapter Protocol + the two real adapters.
# --------------------------------------------------------------------------- #
class SmokeGateAdapter(Protocol):
    """Injectable interface for ONE full-mode smoke gate.

    Abstracts over the two concrete #12 gates so a test can inject a fake that
    returns a deterministic :class:`GateReport` without touching a real venv or
    filesystem. A real adapter wraps a ``CoreSmokeInstallGate`` /
    ``CommunityRebuildReinstallSmokeGate`` and confines all of its work to the
    supplied :class:`SmokeIsolation` sandbox.
    """

    #: Stable runner gate id for the produced row.
    gate_id: str
    #: Human spec label for the produced row.
    spec_id: str

    def run(self, isolation: SmokeIsolation) -> GateReport:
        """Run the gate, confining every filesystem write to ``isolation.root``."""
        ...


@dataclass(frozen=True)
class CoreSmokeInstallAdapter:
    """Real adapter: runs :class:`CoreSmokeInstallGate` with its isolated venv
    forced INTO the temporary sandbox.

    The wheel install + smoke imports therefore never touch the user's
    interpreter and are removed together with the sandbox. The expensive
    subprocess work is delegated to the gate's injected ``runner`` (a real
    :class:`SubprocessCommandRunner` for an e2e run; a deterministic fake in a
    test), so this adapter itself performs no real install.
    """

    gate_input: SmokeInstallInput
    runner: CommandRunner | None = None
    spec_id: str = FULL_MODE_SPEC_ID
    gate_id: str = CoreSmokeInstallGate.gate_id

    def run(self, isolation: SmokeIsolation) -> GateReport:
        scoped = replace(self.gate_input, isolated_env=isolation.venv_dir())
        return CoreSmokeInstallGate().run(scoped, runner=self.runner)


@dataclass(frozen=True)
class CommunityRebuildReinstallSmokeAdapter:
    """Real adapter: runs :class:`CommunityRebuildReinstallSmokeGate`.

    The gate is a pure evaluator over OBSERVED inventories (no subprocess), so the
    isolation sandbox is accepted for a uniform interface but not required.
    """

    gate_input: CommunitySmokeInput
    spec_id: str = FULL_MODE_SPEC_ID
    gate_id: str = CommunityRebuildReinstallSmokeGate.gate_id

    def run(self, isolation: SmokeIsolation) -> GateReport:
        return CommunityRebuildReinstallSmokeGate().run(self.gate_input)


# --------------------------------------------------------------------------- #
# Prerequisite detection (env / wheel / venv / temp root).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PrerequisiteCheck:
    """Outcome of the full-mode prerequisite probe.

    ``available`` gates whether the real smoke runs; ``reason`` is the HONEST
    human description surfaced in the ``skipped`` remediation when the
    prerequisites are absent.
    """

    available: bool
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {"available": self.available, "reason": self.reason}


@dataclass(frozen=True)
class FullModePrerequisites:
    """Declares what the full-mode smoke needs to run for real.

    The default real probe (:func:`detect_full_prerequisites`) verifies the
    declared wheel artifact(s) exist on disk and that the stdlib ``venv`` module
    is importable. A test injects a :class:`PrerequisiteCheck` directly instead.
    """

    required_wheels: tuple[Path, ...] = ()
    require_venv_module: bool = True


def detect_full_prerequisites(
    prereqs: FullModePrerequisites, *, temp_root: Path | None = None
) -> PrerequisiteCheck:
    """Probe the host for the full-mode prerequisites (REAL, deterministic).

    Honest and fail-closed: an undeclared/unbuilt wheel artifact, an unimportable
    ``venv`` module, or an unwritable temp root each makes the full smoke
    UNAVAILABLE (-> ``skipped``, never a fake ``success``). When nothing is
    missing the full smoke may run for real. The probe never builds anything — it
    only inspects what already exists.
    """
    missing: list[str] = []
    if prereqs.require_venv_module and importlib.util.find_spec("venv") is None:
        missing.append("python 'venv' module is not importable")
    if not prereqs.required_wheels:
        missing.append("no wheel artifact declared for the full-mode install smoke")
    for wheel in prereqs.required_wheels:
        if not wheel.exists():
            missing.append(f"wheel artifact not built: {wheel}")
    root = temp_root if temp_root is not None else Path(tempfile.gettempdir())
    if not (root.exists() and os.access(root, os.W_OK)):
        missing.append(f"temp root not writable: {root}")
    if missing:
        return PrerequisiteCheck(available=False, reason="; ".join(missing))
    return PrerequisiteCheck(
        available=True,
        reason="full prerequisites present (wheel + venv tooling + writable temp root)",
    )


# --------------------------------------------------------------------------- #
# The full-mode smoke result.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FullSmokeResult:
    """The full-mode smoke outcome: the mapped runner rows plus provenance.

    ``gate_results`` is one :class:`RunnerGateResult` per adapter (the value the
    runner shell consumes), sorted by the shell's five ordering keys so the result
    is deterministic regardless of adapter iteration order. ``command_results``
    mirrors each gate that RAN as a supplementary :class:`RunnerCommandResult`.
    ``isolation_root`` is the temporary sandbox that WAS created and has already
    been removed (``None`` when prerequisites were absent and nothing was
    isolated).
    """

    gate_results: tuple[RunnerGateResult, ...]
    command_results: tuple[RunnerCommandResult, ...]
    prerequisites: PrerequisiteCheck
    isolation_root: str | None = None

    @property
    def ran(self) -> bool:
        """True iff the prerequisites were present and the gates were executed."""
        return self.prerequisites.available

    def as_dict(self) -> dict[str, object]:
        """Stable, JSON-serialisable projection."""
        return {
            "prerequisites": self.prerequisites.as_dict(),
            "isolation_root": self.isolation_root,
            "gate_results": [gate.as_dict() for gate in self.gate_results],
            "command_results": [cmd.as_dict() for cmd in self.command_results],
        }


def _safe_subdir_name(gate_id: str) -> str:
    """A filesystem-safe per-adapter subdir name derived from the gate id."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", gate_id) or "gate"


def _runner_status_for(report: GateReport) -> RunnerGateStatus:
    """Collapse a smoke ``GateReport`` status to the runner enum (fail-closed)."""
    return GATE_REPORT_STATUS_TO_RUNNER.get(report.status, "blocked")


def _skipped_row(
    adapter: SmokeGateAdapter, *, owner: str | None, reason: str
) -> RunnerGateResult:
    """A ``skipped`` row carrying the HONEST unavailable reason — never success."""
    return RunnerGateResult(
        gate_id=adapter.gate_id,
        spec_id=adapter.spec_id,
        status="skipped",
        owner=owner,
        remediation=f"full prerequisites unavailable: {reason}",
    )


def _map_report(
    report: GateReport, adapter: SmokeGateAdapter, *, owner: str | None
) -> RunnerGateResult:
    """Map a smoke ``GateReport`` to the runner row (status is a pure collapse)."""
    return RunnerGateResult(
        gate_id=adapter.gate_id,
        spec_id=adapter.spec_id,
        status=_runner_status_for(report),
        owner=report.owner or owner,
        remediation=report.remediation_hint,
    )


def run_full_mode_smoke(
    *,
    adapters: Iterable[SmokeGateAdapter],
    prerequisites: PrerequisiteCheck | Callable[[], PrerequisiteCheck],
    temp_root: Path | None = None,
    owner: str | None = DEFAULT_FULL_SMOKE_OWNER,
    emit_command_results: bool = True,
) -> FullSmokeResult:
    """Run the FCC-07E ``full`` mode smoke gates and map them to runner rows.

    Parameters
    ----------
    adapters:
        The smoke gate adapters (real or injected fakes). Each yields one
        :class:`GateReport` when run inside its :class:`SmokeIsolation` sandbox.
    prerequisites:
        A :class:`PrerequisiteCheck` (or a zero-arg callable producing one). When
        ``available`` is False, NO sandbox is created and NO adapter is run — each
        adapter is recorded as ``skipped`` with the honest reason. This is the
        anti-requirement: an absent prerequisite can never become a fake success.
    temp_root:
        Optional parent directory for the temporary sandbox. ``None`` uses the
        system temp root. Only a uniquely-named child of this directory is created
        AND removed; the directory itself and its other contents are untouched.
    owner / emit_command_results:
        The owner stamped on the rows; whether to also emit a supplementary
        :class:`RunnerCommandResult` per gate that ran.

    Returns
    -------
    FullSmokeResult
        The mapped gate rows + (optional) command rows + the prerequisite probe.
        The temporary sandbox is always removed before this returns (even if an
        adapter raises), and only the sandbox is removed.
    """
    adapter_list = list(adapters)
    check = prerequisites() if callable(prerequisites) else prerequisites
    if not isinstance(check, PrerequisiteCheck):
        raise TypeError(
            "prerequisites must be a PrerequisiteCheck or a zero-arg callable "
            f"returning one; got {type(check).__name__!r}."
        )

    # Anti-requirement: prerequisites absent -> honest skip, gates never run.
    if not check.available:
        rows = tuple(
            sorted(
                (_skipped_row(a, owner=owner, reason=check.reason) for a in adapter_list),
                key=RunnerGateResult.sort_key,
            )
        )
        return FullSmokeResult(
            gate_results=rows,
            command_results=(),
            prerequisites=check,
            isolation_root=None,
        )

    gate_rows: list[RunnerGateResult] = []
    command_rows: list[RunnerCommandResult] = []
    # Temporary isolation: TemporaryDirectory creates a uniquely-named child of
    # temp_root and, on __exit__, removes THAT child and only that child — so the
    # cleanup can never reach a user file outside the sandbox.
    with tempfile.TemporaryDirectory(
        prefix="fcc07e_full_smoke_",
        dir=str(temp_root) if temp_root is not None else None,
    ) as tmp:
        root = Path(tmp)
        isolation_root = str(root)
        for adapter in adapter_list:
            sandbox = root / _safe_subdir_name(adapter.gate_id)
            sandbox.mkdir(parents=True, exist_ok=True)
            report = adapter.run(SmokeIsolation(root=sandbox))
            row = _map_report(report, adapter, owner=owner)
            gate_rows.append(row)
            if emit_command_results and row.status != "skipped":
                command_rows.append(
                    RunnerCommandResult(
                        command=f"{adapter.gate_id} (full-mode isolated smoke)",
                        exit_code=0 if row.status == "success" else 1,
                        owner=row.owner,
                    )
                )
    # The sandbox (and only the sandbox) has now been removed.

    gate_rows.sort(key=RunnerGateResult.sort_key)
    command_rows.sort(key=RunnerCommandResult.sort_key)
    return FullSmokeResult(
        gate_results=tuple(gate_rows),
        command_results=tuple(command_rows),
        prerequisites=check,
        isolation_root=isolation_root,
    )


def run_final_clean_core_full_mode(
    *,
    smoke: FullSmokeResult,
    gate_results: Iterable[GateInput] = (),
    command_results: Iterable[CommandInput] = (),
    required_gate_ids: Iterable[str] | None = None,
) -> RunnerReport:
    """Reuse the E-IMP1 shell in ``full`` mode, INCORPORATING the smoke rows.

    This concatenates the smoke gate + command rows from :func:`run_full_mode_smoke`
    with the injected A/B/C/D results and calls the shell with ``mode='full'``. The
    shell (:func:`run_final_clean_core`) is reused verbatim — NOT reimplemented —
    so the fail-closed exit code, ordering and schema are unchanged.
    """
    return run_final_clean_core(
        mode="full",
        gate_results=[*gate_results, *smoke.gate_results],
        command_results=[*command_results, *smoke.command_results],
        required_gate_ids=required_gate_ids,
    )


__all__ = [
    "FULL_MODE_SPEC_ID",
    "DEFAULT_FULL_SMOKE_OWNER",
    "GATE_REPORT_STATUS_TO_RUNNER",
    "SmokeIsolation",
    "SmokeGateAdapter",
    "CoreSmokeInstallAdapter",
    "CommunityRebuildReinstallSmokeAdapter",
    "PrerequisiteCheck",
    "FullModePrerequisites",
    "detect_full_prerequisites",
    "FullSmokeResult",
    "run_full_mode_smoke",
    "run_final_clean_core_full_mode",
]
