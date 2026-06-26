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

from typing import Any, Mapping

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    commit,
    session_of,
)
from okto_pulse.core.services import SpecService

_REQUIRED_FIELDS = (
    "completeness",
    "completeness_justification",
    "assertiveness",
    "assertiveness_justification",
    "ambiguity",
    "ambiguity_justification",
    "general_justification",
    "recommendation",
)
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
        missing = [f for f in _REQUIRED_FIELDS if self.data.get(f) is None]
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


async def _resolve_reviewer_name(session: Any, actor_id: str, board_id: str) -> str:
    try:
        from okto_pulse.core.services.main import resolve_actor_name

        return await resolve_actor_name(session, actor_id, board_id)
    except Exception:
        return actor_id


class SubmitSpecValidationUseCase:
    """Submit a spec validation gate record without any transport dependency."""

    async def execute(
        self, command: SubmitSpecValidationCommand, *, actor: ActorContext, uow: Any
    ) -> SubmitSpecValidationResult:
        command.validate()
        session = session_of(uow)
        service = SpecService(session)
        spec = await service.get_spec(command.spec_id)
        if spec is None:
            raise EntityNotFoundError("spec", command.spec_id)
        # MCP supplies the resolved agent name (actor.actor_name); REST leaves it
        # None and we resolve it here — preserving each surface's reviewer_name.
        reviewer_name = actor.actor_name or await _resolve_reviewer_name(
            session, actor.actor_id, spec.board_id
        )
        # ResourceGateError / ValueError propagate to the adapter (HTTP 409),
        # mirroring api/specs.py:submit_spec_validation.
        result = await service.submit_spec_validation(
            spec_id=command.spec_id,
            reviewer_id=actor.actor_id,
            reviewer_name=reviewer_name,
            data=dict(command.data),
        )
        await commit(uow)
        return SubmitSpecValidationResult(payload=result)
