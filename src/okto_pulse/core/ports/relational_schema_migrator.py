"""RelationalSchemaMigrator port (spec R16-A, api_bb6411fd).

Contract-only boundary for relational schema migration. This module defines the
``RelationalSchemaMigrator`` Protocol and its typed DTOs — ``MigrationStep``,
``MigrationStepResult``, ``MigrationPlan`` and ``MigrationResult`` — making the
schema lifecycle phases explicit WITHOUT moving, reordering or executing the
current imperative migrations.

The effective ordering of ``infra.database.init_db`` is modelled as four ordered
phases (``MIGRATION_PHASES``):

  * ``pre_create_all``       — schema ALTERs applied BEFORE ``create_all``.
  * ``create_all_boundary``  — ``Base.metadata.create_all`` (the table-create boundary).
  * ``post_create_all``      — schema ALTERs applied AFTER ``create_all``.
  * ``data_bootstrap_boundary`` — seed / preset reconcile / permission reconcile /
    discovery-intent bootstrap. This is DATA bootstrap, NOT schema migration, and
    a schema migration plan MUST NOT silently absorb it (see ``br_e16ff5a1``).

Failure semantics are fail-closed (``br_365f6e01`` / ``fr_3d223793``): a missing
migrator, an invalid plan or a failing step yields a structured failure carrying
``step_id`` / ``phase`` / ``failure_reason`` / ``remediation`` — a partial or
failed run can NEVER report ``success``, even if earlier steps applied.

Pure: stdlib ``dataclasses`` / ``typing`` only. It does NOT import SQLAlchemy,
``get_engine``, ``Base``, ``infra.database``, any ``_migrate_*`` function, or
``okto_pulse.community`` — the concrete Community adapter (R16-B) owns those.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

#: The schema lifecycle phase a step belongs to.
MigrationPhase = Literal[
    "pre_create_all",
    "create_all_boundary",
    "post_create_all",
    "data_bootstrap_boundary",
]
#: Canonical ordered phases — the documentable baseline of ``init_db``.
MIGRATION_PHASES: tuple[MigrationPhase, ...] = (
    "pre_create_all",
    "create_all_boundary",
    "post_create_all",
    "data_bootstrap_boundary",
)

#: Top-level migration outcome. ``partial`` is a NON-SUCCESS diagnostic state.
MigrationStatus = Literal["success", "failed", "partial"]
#: Per-step outcome.
StepStatus = Literal["applied", "skipped", "failed"]


class SchemaMigrationError(Exception):
    """Structured, fail-closed migration failure.

    Carries the locating + remediation context required by ``fr_3d223793`` so a
    caller never has to parse a free-text message. Raised for a missing migrator,
    an invalid plan, or a step failure that the caller chooses to surface as an
    exception rather than a ``failed`` :class:`MigrationResult`.
    """

    def __init__(
        self,
        failure_reason: str,
        *,
        step_id: str | None = None,
        phase: MigrationPhase | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.step_id = step_id
        self.phase = phase
        self.remediation = remediation
        detail = f"step_id={step_id} phase={phase}" if step_id or phase else ""
        super().__init__(
            f"{failure_reason}{(' [' + detail + ']') if detail else ''}"
        )


@dataclass(frozen=True)
class MigrationStep:
    """One ordered schema-lifecycle step. Declarative description only — this DTO
    never carries or executes SQL."""

    step_id: str
    order: int
    phase: MigrationPhase
    description: str
    idempotent: bool
    destructive: bool
    owner: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MigrationStepResult:
    """Outcome of a single :class:`MigrationStep`."""

    step_id: str
    status: StepStatus
    phase: MigrationPhase
    failure_reason: str | None = None
    remediation: str | None = None
    duration_ms: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MigrationPlan:
    """An ordered, declarative plan. ``boundaries`` names the ordered phase
    markers so ``create_all_boundary`` and ``data_bootstrap_boundary`` are
    explicit (``ac_a9018cb6``)."""

    plan_id: str
    target: str
    steps: tuple[MigrationStep, ...] = ()
    boundaries: tuple[MigrationPhase, ...] = MIGRATION_PHASES
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MigrationResult:
    """Outcome of executing a :class:`MigrationPlan`.

    Fail-closed invariant (``br_365f6e01`` / api_bb6411fd semantics): if any
    ``failed_steps`` exist OR ``failed_step`` is set, ``status`` MUST NOT be
    ``success`` — ``__post_init__`` raises :class:`ValueError` otherwise, so a
    failed/partial run can never be mislabelled as a successful migration.
    """

    status: MigrationStatus
    applied_steps: tuple[MigrationStepResult, ...] = ()
    skipped_steps: tuple[MigrationStepResult, ...] = ()
    failed_steps: tuple[MigrationStepResult, ...] = ()
    warnings: tuple[str, ...] = ()
    duration_ms: float | None = None
    failed_step: MigrationStepResult | None = None
    failure_reason: str | None = None
    remediation: str | None = None

    def __post_init__(self) -> None:
        if self.status == "success" and (self.failed_steps or self.failed_step):
            raise ValueError(
                "MigrationResult fail-closed violation: status='success' with a "
                "failed step is not allowed (a failed/partial migration must "
                "never report success)."
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
        failed_step: MigrationStepResult,
        *,
        applied_steps: tuple[MigrationStepResult, ...] = (),
        skipped_steps: tuple[MigrationStepResult, ...] = (),
        warnings: tuple[str, ...] = (),
        duration_ms: float | None = None,
        partial: bool = False,
    ) -> "MigrationResult":
        """Build a fail-closed ``failed`` (or ``partial`` diagnostic) result.

        ``partial`` is still a non-success state: it records that earlier steps
        applied, but the run did NOT complete — it never satisfies a successful
        migration when a ``failed_step`` is present.
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
class RelationalSchemaMigrator(Protocol):
    """Runtime-agnostic relational schema migration contract.

    Implementations (e.g. the future R16-B Community adapter) own the concrete
    SQLAlchemy execution; this port never references it.
    """

    def plan(self, *, target: str) -> MigrationPlan:
        """Build the ordered migration plan for ``target`` (no execution)."""
        ...

    def validate_plan(self, plan: MigrationPlan) -> None:
        """Validate a plan's shape/ordering. Raise :class:`SchemaMigrationError`
        (fail-closed) when a step lacks a valid ``step_id`` / ``order`` /
        ``phase`` or the phase ordering is inconsistent."""
        ...

    def execute(self, plan: MigrationPlan) -> MigrationResult:
        """Execute ``plan`` and return a :class:`MigrationResult`. A step failure
        yields a fail-closed ``failed``/``partial`` result (never ``success``)."""
        ...


def require_migrator(
    migrator: RelationalSchemaMigrator | None,
    *,
    target: str | None = None,
) -> RelationalSchemaMigrator:
    """Fail-closed resolver for an absent migrator (``fr_3d223793``).

    A missing/unregistered :class:`RelationalSchemaMigrator` MUST raise a
    structured :class:`SchemaMigrationError` (``failure_reason='migrator_absent'``
    + remediation) — it NEVER silently degrades to a no-op success. Concrete
    registries (the future Community adapter) call this before planning/executing.
    """
    if migrator is None:
        raise SchemaMigrationError(
            "migrator_absent",
            remediation=(
                "Register/resolve a RelationalSchemaMigrator adapter (e.g. the "
                "Community relational adapter) before planning or executing"
                + (f" for target={target!r}." if target else ".")
            ),
        )
    return migrator


__all__ = [
    "MIGRATION_PHASES",
    "MigrationPhase",
    "MigrationStatus",
    "StepStatus",
    "SchemaMigrationError",
    "MigrationStep",
    "MigrationStepResult",
    "MigrationPlan",
    "MigrationResult",
    "RelationalSchemaMigrator",
    "require_migrator",
]
