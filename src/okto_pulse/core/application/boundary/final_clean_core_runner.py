"""FCC-07E-IMP1 — Final Clean Core runner SHELL (contract ``api_170877a6``).

This module is the locally-callable ENTRYPOINT of the FCC-07E "final clean core"
runner — the *shell* only. It can be driven WITHOUT starting a Pulse server: it
accepts the per-gate results of the FCC-07A/B/C/D gates and the focused
command results by INJECTION (already-resolved values OR zero-arg callables that
produce them), aggregates them into one deterministic, JSON-serialisable report
(``internal://fcc07e/final-clean-core-runner-report``), and computes the
fail-closed exit code.

Scope — FCC-07E-IMP1 is the SHELL ONLY (additive, deterministic, synchronous):

* It NEVER wires the real FCC-07A/B/C/D gates (that is FCC-07E-IMP2), NEVER runs
  pytest (FCC-07E-IMP3) and NEVER runs the full-mode smoke with a real venv
  (FCC-07E-IMP4). It STRUCTURES the run and accepts those results by injection so
  it is testable in isolation.
* It imports ONLY public core symbols (``AdapterEvidence`` / ``REQUIRED_EVIDENCE``
  from the canonical adapter readiness inventory) and stdlib. It NEVER imports
  ``okto_pulse.community`` and touches no private symbol of any other module.

Report schema (``api_170877a6`` ``response_success``), emitted by
:meth:`RunnerReport.as_dict`:

* top-level: ``overall_status`` (``success`` | ``blocked``), ``mode``
  (``quick`` | ``full``), ``exit_code`` (int), ``ordering`` (the fixed list of
  the five sort-key NAMES — the ``gates`` array is sorted by exactly these keys),
  ``gates`` (list), ``commands`` (list), ``summary``
  (``{blocking, passed, skipped}``).
* each gate row: ``gate_id`` / ``spec_id`` / ``status`` / ``owner`` /
  ``adapter_key`` / ``dependency_family`` / ``provider_key`` / ``evidence_fields``
  (the six :data:`REQUIRED_EVIDENCE` tri-state fields) / ``command`` /
  ``test_file`` / ``remediation``.
* each command row: ``command`` / ``exit_code`` / ``test_files`` / ``owner``.

Determinism (TR3): gates are ordered by the five-tuple ``(gate_id, spec_id,
adapter_key, dependency_family, provider_key)`` (with the remaining string fields
as stable tiebreakers so a shuffled input yields a byte-identical report).

Exit code (TR2 / AC8): ``exit_code == 0`` (``overall_status == "success"``) IFF
no gate is ``blocking`` AND no focused command exited non-zero. Any blocking gate
OR any failed command -> non-zero (``blocked``). A required gate that did not run
at all is fail-closed: it is synthesised as a ``blocking`` row.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Literal, Union

from .adapter_readiness_inventory import REQUIRED_EVIDENCE, AdapterEvidence

#: Runner execution mode (FR1/FR2). ``quick`` runs gates + focused pytest without
#: a build; ``full`` additionally incorporates the smoke gates. The shell only
#: RECORDS and validates the mode — gate SELECTION per mode is FCC-07E-IMP2+.
RunnerMode = Literal["quick", "full"]

#: Normalised per-gate status (api_170877a6 ``gates[].status``). The richer
#: per-gate verdicts of FCC-07A/B/C/D are collapsed into these three by the wiring
#: layer (FCC-07E-IMP2) before injection. Note the contract's deliberate naming
#: asymmetry: gate status is ``success``/``blocked`` while the ``summary`` counts
#: those as ``passed``/``blocking``.
RunnerGateStatus = Literal["success", "blocked", "skipped"]

#: Top-level outcome. ``success`` IFF ``exit_code == 0``.
RunnerOverallStatus = Literal["success", "blocked"]

_VALID_MODES: frozenset[str] = frozenset({"quick", "full"})
_VALID_GATE_STATUSES: frozenset[str] = frozenset({"success", "blocked", "skipped"})

#: The five ordering keys (TR3), in priority order.
ORDERING_KEYS: tuple[str, ...] = (
    "gate_id",
    "spec_id",
    "adapter_key",
    "dependency_family",
    "provider_key",
)


@dataclass(frozen=True)
class RunnerGateResult:
    """One injected gate result, aligned with the ``api_170877a6`` gate row.

    ``evidence_fields`` is the canonical :class:`AdapterEvidence` DTO (FCC-07B
    FR1) — its :meth:`~AdapterEvidence.as_map` projects exactly the six
    :data:`REQUIRED_EVIDENCE` tri-state fields. For a non-adapter gate
    (e.g. FCC-07C dependency or FCC-07D provider guard) it stays the all-``None``
    default (not applicable). ``adapter_key`` / ``dependency_family`` /
    ``provider_key`` are the gate's identity within its family and double as
    ordering keys; each is ``None`` for a gate outside that family.
    """

    gate_id: str
    spec_id: str
    status: RunnerGateStatus
    owner: str | None = None
    adapter_key: str | None = None
    dependency_family: str | None = None
    provider_key: str | None = None
    evidence_fields: AdapterEvidence = field(default_factory=AdapterEvidence)
    command: str | None = None
    test_file: str | None = None
    remediation: str | None = None

    @property
    def is_blocking(self) -> bool:
        return self.status == "blocked"

    def sort_key(self) -> tuple[str, ...]:
        """Total, deterministic sort key: the five ordering keys (``None`` -> "")
        followed by the remaining string fields as stable tiebreakers so a
        shuffled input is still ordered byte-identically."""
        return (
            self.gate_id or "",
            self.spec_id or "",
            self.adapter_key or "",
            self.dependency_family or "",
            self.provider_key or "",
            self.status or "",
            self.owner or "",
            self.command or "",
            self.test_file or "",
            self.remediation or "",
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id,
            "spec_id": self.spec_id,
            "status": self.status,
            "owner": self.owner,
            "adapter_key": self.adapter_key,
            "dependency_family": self.dependency_family,
            "provider_key": self.provider_key,
            "evidence_fields": self.evidence_fields.as_map(),
            "command": self.command,
            "test_file": self.test_file,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class RunnerCommandResult:
    """One injected focused-command result, aligned with the ``api_170877a6``
    command row. A non-zero ``exit_code`` is a failed command and forces the
    runner to a non-zero exit (TR2)."""

    command: str
    exit_code: int
    test_files: tuple[str, ...] = ()
    owner: str | None = None

    @property
    def failed(self) -> bool:
        return self.exit_code != 0

    def sort_key(self) -> tuple[str, ...]:
        return (self.command or "", self.owner or "")

    def as_dict(self) -> dict[str, object]:
        return {
            "command": self.command,
            "exit_code": self.exit_code,
            "test_files": list(self.test_files),
            "owner": self.owner,
        }


@dataclass(frozen=True)
class RunnerSummary:
    """Deterministic gate tally (``api_170877a6`` ``summary``)."""

    blocking: int
    passed: int
    skipped: int

    def as_dict(self) -> dict[str, int]:
        return {
            "blocking": self.blocking,
            "passed": self.passed,
            "skipped": self.skipped,
        }


@dataclass(frozen=True)
class RunnerReport:
    """Deterministic, JSON-serialisable runner report (contract
    ``api_170877a6`` ``response_success``)."""

    overall_status: RunnerOverallStatus
    mode: RunnerMode
    exit_code: int
    #: The ordered names of the five sort keys (api_170877a6 ``ordering``); the
    #: realised gate order is the ``gates`` array, sorted by exactly these keys.
    ordering: tuple[str, ...]
    gates: tuple[RunnerGateResult, ...]
    commands: tuple[RunnerCommandResult, ...]
    summary: RunnerSummary

    @property
    def success(self) -> bool:
        return self.overall_status == "success"

    def as_dict(self) -> dict[str, object]:
        """Stable projection emitting the EXACT ``api_170877a6`` schema."""
        return {
            "overall_status": self.overall_status,
            "mode": self.mode,
            "exit_code": self.exit_code,
            "ordering": list(self.ordering),
            "gates": [gate.as_dict() for gate in self.gates],
            "commands": [command.as_dict() for command in self.commands],
            "summary": self.summary.as_dict(),
        }


#: A gate input may be an already-resolved result OR a zero-arg callable that
#: produces one (injectable inputs/gates — FR contract for the shell).
GateInput = Union[RunnerGateResult, Callable[[], RunnerGateResult]]
CommandInput = Union[RunnerCommandResult, Callable[[], RunnerCommandResult]]


def _resolve_gate(item: GateInput) -> RunnerGateResult:
    result = item() if not isinstance(item, RunnerGateResult) and callable(item) else item
    if not isinstance(result, RunnerGateResult):
        raise TypeError(
            "gate_results items must be RunnerGateResult or a zero-arg callable "
            f"returning one; got {type(result).__name__!r}."
        )
    if result.status not in _VALID_GATE_STATUSES:
        raise ValueError(
            f"gate {result.gate_id!r} has invalid status {result.status!r}; "
            f"expected one of {sorted(_VALID_GATE_STATUSES)}."
        )
    return result


def _resolve_command(item: CommandInput) -> RunnerCommandResult:
    result = (
        item()
        if not isinstance(item, RunnerCommandResult) and callable(item)
        else item
    )
    if not isinstance(result, RunnerCommandResult):
        raise TypeError(
            "command_results items must be RunnerCommandResult or a zero-arg "
            f"callable returning one; got {type(result).__name__!r}."
        )
    return result


def run_final_clean_core(
    *,
    mode: RunnerMode,
    gate_results: Iterable[GateInput],
    command_results: Iterable[CommandInput] = (),
    required_gate_ids: Iterable[str] | None = None,
) -> RunnerReport:
    """Aggregate injected gate + command results into a deterministic report.

    Parameters
    ----------
    mode:
        ``quick`` or ``full`` (FR1/FR2). RECORDED + validated only; per-mode gate
        selection is FCC-07E-IMP2+.
    gate_results:
        The FCC-07A/B/C/D gate results, injected as :class:`RunnerGateResult` or
        zero-arg callables producing them.
    command_results:
        The focused-command results (e.g. focused pytest in FCC-07E-IMP3),
        injected the same way. Empty by default.
    required_gate_ids:
        Optional fail-closed presence check. Any required ``gate_id`` absent from
        ``gate_results`` is synthesised as a ``blocking`` gate row so a gate that
        did not run can never be silently counted as success.

    Exit code (TR2/AC8): ``0`` IFF no gate is ``blocking`` AND no command exited
    non-zero; otherwise non-zero. ``overall_status == "success"`` IFF
    ``exit_code == 0``.
    """
    if mode not in _VALID_MODES:
        raise ValueError(f"mode must be 'quick' or 'full'; got {mode!r}.")

    gates: list[RunnerGateResult] = [_resolve_gate(item) for item in gate_results]
    commands: list[RunnerCommandResult] = [
        _resolve_command(item) for item in command_results
    ]

    # Fail-closed: a required gate that did not run is a blocking row (sorted so
    # the synthesis is deterministic regardless of caller iteration order).
    present_gate_ids = {gate.gate_id for gate in gates}
    for missing_id in sorted(set(required_gate_ids or ()) - present_gate_ids):
        gates.append(
            RunnerGateResult(
                gate_id=missing_id,
                spec_id="",
                status="blocked",
                remediation=(
                    "required gate did not run (fail-closed): inject its "
                    "RunnerGateResult before declaring the core clean."
                ),
            )
        )

    ordered_gates = tuple(sorted(gates, key=RunnerGateResult.sort_key))
    ordered_commands = tuple(sorted(commands, key=RunnerCommandResult.sort_key))

    summary = RunnerSummary(
        blocking=sum(1 for gate in ordered_gates if gate.status == "blocked"),
        passed=sum(1 for gate in ordered_gates if gate.status == "success"),
        skipped=sum(1 for gate in ordered_gates if gate.status == "skipped"),
    )

    command_failed = any(command.failed for command in ordered_commands)
    exit_code = 0 if (summary.blocking == 0 and not command_failed) else 1
    overall_status: RunnerOverallStatus = "success" if exit_code == 0 else "blocked"

    return RunnerReport(
        overall_status=overall_status,
        mode=mode,
        exit_code=exit_code,
        ordering=ORDERING_KEYS,
        gates=ordered_gates,
        commands=ordered_commands,
        summary=summary,
    )


def render_final_clean_core_report(
    report: RunnerReport, *, fmt: Literal["json", "markdown"] = "json"
) -> str:
    """Render the runner report as a STABLE string surface (FR5 — JSON/markdown).

    ``json`` emits the deterministic ``api_170877a6`` payload (sorted keys);
    ``markdown`` emits a deterministic human-readable summary (overall status, a
    gate table sorted by the five ordering keys, and the failing commands). Both
    are pure functions of the report — nothing depends on iteration order.
    """
    if fmt == "json":
        return json.dumps(report.as_dict(), indent=2, sort_keys=True)
    if fmt != "markdown":
        raise ValueError(f"fmt must be 'json' or 'markdown'; got {fmt!r}.")

    lines = [
        f"# Final clean-core runner — {report.overall_status.upper()}",
        "",
        f"- mode: `{report.mode}`",
        f"- exit_code: `{report.exit_code}`",
        (
            f"- summary: blocking={report.summary.blocking} "
            f"passed={report.summary.passed} skipped={report.summary.skipped}"
        ),
        "",
        "| gate_id | spec_id | status | owner | adapter_key | "
        "dependency_family | provider_key |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for gate in report.gates:
        lines.append(
            f"| {gate.gate_id} | {gate.spec_id} | {gate.status} | "
            f"{gate.owner or '-'} | {gate.adapter_key or '-'} | "
            f"{gate.dependency_family or '-'} | {gate.provider_key or '-'} |"
        )
    failing = [command for command in report.commands if command.failed]
    if failing:
        lines += ["", "## Failing commands", ""]
        for command in failing:
            lines.append(f"- `{command.command}` (exit_code={command.exit_code})")
    return "\n".join(lines)


__all__ = [
    "ORDERING_KEYS",
    "REQUIRED_EVIDENCE",
    "RunnerMode",
    "RunnerGateStatus",
    "RunnerOverallStatus",
    "RunnerGateResult",
    "RunnerCommandResult",
    "RunnerSummary",
    "RunnerReport",
    "GateInput",
    "CommandInput",
    "run_final_clean_core",
    "render_final_clean_core_report",
]
