"""DataBootstrapper port (spec R16-C, api_2247efff).

Contract-only boundary for DATA bootstrap — the seed / preset-reconcile /
permission-reconcile / discovery-intent steps that run AFTER schema migration
in ``infra.database.init_db``. This is the data-bootstrap fatia, kept STRICTLY
separate from relational schema migration (the R16-A ``RelationalSchemaMigrator``
port + R16-B Community adapter own that). A schema plan MUST NOT absorb data
bootstrap and a data-bootstrap plan MUST NOT absorb schema steps
(``br_e16ff5a1`` boundary; cross-checked by R16-C ts_5a7b50e2).

This module defines the ``DataBootstrapper`` Protocol and its typed DTOs —
``DataBootstrapStep``, ``DataBootstrapStepResult``, ``DataBootstrapPlan`` and
``DataBootstrapResult`` — making the data-bootstrap domains explicit WITHOUT
moving, reordering or executing the current imperative bootstrap functions.

Domains (``BOOTSTRAP_DOMAINS``):

  * ``presets``           — built-in permission presets seed + reconcile.
  * ``permissions``       — agent permission-flag reconcile.
  * ``discovery_intents`` — discovery-intent seed catalog bootstrap.
  * ``knowledge_propagation`` — resumable selective-propagation data backfill.
  * ``quality_assessment`` — one-shot durable legacy assessment import.

Failure semantics are fail-closed: a missing bootstrapper, an invalid plan or a
failing step yields a structured failure carrying ``step_id`` / ``domain`` /
``failure_reason`` / ``remediation`` — a partial or failed run can NEVER report
``success``, even if earlier steps applied.

Pure: stdlib ``dataclasses`` / ``typing`` only. It does NOT import SQLAlchemy,
``get_engine``, ``Base``, ``infra.database``, any ``_seed_*`` / ``_reconcile_*``
/ ``_bootstrap_*`` function, or ``okto_pulse.community`` — the concrete
Community adapter (R16-C) owns those.

Async seam (mirrors R16-B): the concrete bootstrap functions are ``async``, so
the Community adapter implements the sync ``execute`` Protocol method as a
facade over an async ``aexecute`` (documented, not resolved here — the port
only declares the sync contract).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

#: The data-bootstrap domain a step belongs to.
BootstrapDomain = Literal[
    "presets",
    "permissions",
    "discovery_intents",
    "knowledge_propagation",
    "quality_assessment",
]
#: Canonical domains the data-bootstrap fatia covers.
BOOTSTRAP_DOMAINS: tuple[BootstrapDomain, ...] = (
    "presets",
    "permissions",
    "discovery_intents",
    "knowledge_propagation",
    "quality_assessment",
)

#: Top-level bootstrap outcome. ``partial`` is a NON-SUCCESS diagnostic state.
DataBootstrapStatus = Literal["success", "failed", "partial"]
#: Per-step outcome.
StepStatus = Literal["applied", "skipped", "failed"]


class DataBootstrapError(Exception):
    """Structured, fail-closed data-bootstrap failure.

    Carries locating + remediation context so a caller never has to parse a
    free-text message. Raised for a missing bootstrapper, an invalid plan, or a
    step failure the caller chooses to surface as an exception rather than a
    ``failed`` :class:`DataBootstrapResult`.
    """

    def __init__(
        self,
        failure_reason: str,
        *,
        step_id: str | None = None,
        domain: BootstrapDomain | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.step_id = step_id
        self.domain = domain
        self.remediation = remediation
        detail = f"step_id={step_id} domain={domain}" if step_id or domain else ""
        super().__init__(f"{failure_reason}{(' [' + detail + ']') if detail else ''}")


@dataclass(frozen=True)
class DataBootstrapStep:
    """One ordered data-bootstrap step. Declarative description only — this DTO
    never carries or executes SQL."""

    step_id: str
    order: int
    owner: str
    domain: BootstrapDomain
    idempotent: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DataBootstrapStepResult:
    """Outcome of a single :class:`DataBootstrapStep`."""

    step_id: str
    status: StepStatus
    owner: str
    domain: BootstrapDomain
    failure_reason: str | None = None
    remediation: str | None = None
    duration_ms: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DataBootstrapPlan:
    """An ordered, declarative data-bootstrap plan."""

    plan_id: str
    target: str
    steps: tuple[DataBootstrapStep, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DataBootstrapResult:
    """Outcome of executing a :class:`DataBootstrapPlan`.

    Fail-closed invariant (api_2247efff semantics): if any ``failed_steps``
    exist OR ``failed_step`` is set, ``status`` MUST NOT be ``success`` —
    ``__post_init__`` raises :class:`ValueError` otherwise, so a failed/partial
    run can never be mislabelled as a successful bootstrap.
    """

    status: DataBootstrapStatus
    applied_steps: tuple[DataBootstrapStepResult, ...] = ()
    skipped_steps: tuple[DataBootstrapStepResult, ...] = ()
    failed_steps: tuple[DataBootstrapStepResult, ...] = ()
    warnings: tuple[str, ...] = ()
    duration_ms: float | None = None
    failed_step: DataBootstrapStepResult | None = None
    failure_reason: str | None = None
    remediation: str | None = None

    def __post_init__(self) -> None:
        if self.status == "success" and (self.failed_steps or self.failed_step):
            raise ValueError(
                "DataBootstrapResult fail-closed violation: status='success' "
                "with a failed step is not allowed (a failed/partial bootstrap "
                "must never report success)."
            )

    @property
    def is_success(self) -> bool:
        """True only for a clean success with no failed step."""
        return (
            self.status == "success"
            and not self.failed_steps
            and self.failed_step is None
        )

    @classmethod
    def failed_result(
        cls,
        failed_step: DataBootstrapStepResult,
        *,
        applied_steps: tuple[DataBootstrapStepResult, ...] = (),
        skipped_steps: tuple[DataBootstrapStepResult, ...] = (),
        warnings: tuple[str, ...] = (),
        duration_ms: float | None = None,
        partial: bool = False,
    ) -> "DataBootstrapResult":
        """Build a fail-closed ``failed`` (or ``partial`` diagnostic) result.

        ``partial`` is still a non-success state: it records that earlier steps
        applied, but the run did NOT complete — it never satisfies a successful
        bootstrap when a ``failed_step`` is present.
        """
        return cls(
            status="partial" if partial else "failed",
            applied_steps=applied_steps,
            skipped_steps=skipped_steps,
            failed_steps=(failed_step,),
            warnings=warnings,
            duration_ms=duration_ms,
            failed_step=failed_step,
            failure_reason=failed_step.failure_reason,
            remediation=failed_step.remediation,
        )


@runtime_checkable
class DataBootstrapper(Protocol):
    """Runtime-agnostic data-bootstrap contract.

    Implementations (e.g. the R16-C Community adapter) own the concrete
    SQLAlchemy execution; this port never references it. Because the concrete
    bootstrap functions are ``async``, the Community adapter implements
    ``execute`` as a sync facade over an internal async executor (same seam as
    the R16-B schema migrator).
    """

    def plan(self, *, target: str) -> DataBootstrapPlan:
        """Build the ordered data-bootstrap plan for ``target`` (no execution)."""
        ...

    def validate_plan(self, plan: DataBootstrapPlan) -> None:
        """Validate a plan's shape/ordering. Raise :class:`DataBootstrapError`
        (fail-closed) when a step lacks a valid ``step_id`` / ``order`` /
        ``domain`` or the ordering is inconsistent."""
        ...

    def execute(self, plan: DataBootstrapPlan) -> DataBootstrapResult:
        """Execute ``plan`` and return a :class:`DataBootstrapResult`. A step
        failure yields a fail-closed ``failed``/``partial`` result (never
        ``success``)."""
        ...


def require_bootstrapper(
    bootstrapper: DataBootstrapper | None,
    *,
    target: str | None = None,
) -> DataBootstrapper:
    """Fail-closed resolver for an absent bootstrapper.

    A missing/unregistered :class:`DataBootstrapper` MUST raise a structured
    :class:`DataBootstrapError` (``failure_reason='bootstrapper_absent'`` +
    remediation) — it NEVER silently degrades to a no-op success. Concrete
    registries (the Community adapter) call this before planning/executing.
    """
    if bootstrapper is None:
        raise DataBootstrapError(
            "bootstrapper_absent",
            remediation=(
                "Register/resolve a DataBootstrapper adapter (e.g. the Community "
                "data bootstrapper) before planning or executing"
                + (f" for target={target!r}." if target else ".")
            ),
        )
    return bootstrapper


__all__ = [
    "BOOTSTRAP_DOMAINS",
    "BootstrapDomain",
    "DataBootstrapStatus",
    "StepStatus",
    "DataBootstrapError",
    "DataBootstrapStep",
    "DataBootstrapStepResult",
    "DataBootstrapPlan",
    "DataBootstrapResult",
    "DataBootstrapper",
    "require_bootstrapper",
]
