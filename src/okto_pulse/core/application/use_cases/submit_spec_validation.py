"""submit_spec_validation use case (spec #09 first cut, paired REST/MCP).

Behavior-preserving transport-free reimplementation of
``POST /api/v1/specs/{spec_id}/validation`` (``api/specs.py:submit_spec_validation``)
and the ``okto_pulse_submit_spec_validation`` MCP tool. The payload-shape
validation that lived in the REST handler moves here (into the command) so both
adapters share it and converge on the same use case (ac_03737dea). Coverage-gate
/ state errors raised by ``SpecService.submit_spec_validation``
(``ResourceGateError``/``ValueError``) propagate unchanged for the adapter to
map (HTTP 409); a missing spec becomes :class:`EntityNotFoundError` (HTTP 404).
"""

from __future__ import annotations

from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

from typing import Any, Mapping

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.mutation_permissions import entity_state
from okto_pulse.core.application.use_cases.spec_crud import (
    _require_actor_board_spec,
)
from okto_pulse.core.ports.application_services import ApplicationServiceCatalog

_LEGACY_REQUIRED_FIELDS = (
    "completeness",
    "completeness_justification",
    "assertiveness",
    "assertiveness_justification",
    "ambiguity",
    "ambiguity_justification",
    "general_justification",
    "recommendation",
)
_FORMAL_FIELDS = ("score", "summary")
_SCORE_DIMENSIONS = ("completeness", "assertiveness", "ambiguity")


class SubmitSpecValidationCommand:
    """Input for :class:`SubmitSpecValidationUseCase`.

    Carries the validation payload verbatim so the exact dict the existing
    ``SpecService`` consumes is preserved (no field dropped). ``validate()``
    reproduces the REST handler's input checks as transport-neutral errors.
    """

    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Mapping[str, Any]) -> None:
        self.spec_id = spec_id
        self.data = data

    def validate(self) -> None:
        for field_name in (
            "expected_validation_edition",
            "expected_spec_version",
            "expected_head_revision",
        ):
            value = self.data.get(field_name)
            minimum = 0 if field_name == "expected_head_revision" else 1
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < minimum
            ):
                raise CommandValidationError(f"{field_name} is invalid")

        formal = any(self.data.get(field) is not None for field in _FORMAL_FIELDS)
        legacy = any(
            self.data.get(field) is not None for field in _LEGACY_REQUIRED_FIELDS
        )
        if formal:
            if legacy:
                raise CommandValidationError(
                    "formal and legacy validation shapes are mutually exclusive"
                )
            score = self.data.get("score")
            if (
                not isinstance(score, (int, float))
                or isinstance(score, bool)
                or not 0 <= float(score) <= 100
            ):
                raise CommandValidationError("score must be between 0 and 100")
            summary = self.data.get("summary")
            if not isinstance(summary, str) or not summary.strip():
                raise CommandValidationError("summary is required")
            return

        missing = [f for f in _LEGACY_REQUIRED_FIELDS if self.data.get(f) is None]
        if missing:
            raise CommandValidationError(
                f"Missing required fields: {', '.join(missing)}"
            )
        if self.data.get("recommendation") not in ("approve", "reject"):
            raise CommandValidationError(
                "recommendation must be 'approve' or 'reject'"
            )
        for dim in _SCORE_DIMENSIONS:
            justification = self.data.get(f"{dim}_justification", "")
            if not isinstance(justification, str) or len(justification.strip()) < 10:
                raise CommandValidationError(
                    f"{dim}_justification must be at least 10 characters"
                )
        if len((self.data.get("general_justification") or "").strip()) < 20:
            raise CommandValidationError(
                "general_justification must be at least 20 characters"
            )


class SubmitSpecValidationResult:
    """Output of :class:`SubmitSpecValidationUseCase` — the validation record
    dict returned by ``SpecService`` (status 201 payload)."""

    __slots__ = ("payload",)

    def __init__(self, payload: dict) -> None:
        self.payload = payload


async def _resolve_reviewer_name(
    services: ApplicationServiceCatalog,
    actor_id: str,
    board_id: str,
) -> str:
    try:
        return await services.resolve_actor_name(actor_id, board_id)
    except Exception:
        return actor_id


class SubmitSpecValidationUseCase:
    """Submit a spec validation gate record without any transport dependency."""

    async def execute(
        self, command: SubmitSpecValidationCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> SubmitSpecValidationResult:
        service = uow.services.specs
        spec = await _require_actor_board_spec(
            uow, command.spec_id, actor, write=True
        )
        command.validate()
        state = entity_state(spec)
        await require_authorization(
            actor,
            PermissionRequirement(
                "spec.validation.submit",
                legacy_operation="specs:evaluate",
                entity="spec" if state is not None else None,
                state=state,
            ),
            uow=uow,
            board_id=spec.board_id,
        )
        # MCP supplies the resolved agent name (actor.actor_name); REST leaves it
        # None and we resolve it here — preserving each surface's reviewer_name.
        reviewer_name = actor.actor_name or await _resolve_reviewer_name(
            uow.services,
            actor.actor_id,
            spec.board_id,
        )
        # ResourceGateError / ValueError propagate to the adapter (HTTP 409),
        # mirroring api/specs.py:submit_spec_validation.
        result = await service.submit_spec_validation(
            spec_id=command.spec_id,
            reviewer_id=actor.actor_id,
            reviewer_name=reviewer_name,
            # Pydantic compatibility models may materialize absent optional
            # fields as ``None``.  The service consumes a discriminated
            # formal-or-legacy mapping, so omit those transport placeholders.
            data={
                key: value
                for key, value in command.data.items()
                if value is not None
            },
        )
        await commit(uow)
        return SubmitSpecValidationResult(payload=result)
