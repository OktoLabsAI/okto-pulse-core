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
from okto_pulse.core.domain.spec_validation import (
    SpecValidationMetric,
    SpecValidationPinpoint,
    SpecValidationPinpointAnchorType,
)
from okto_pulse.core.domain.guideline_policy import PolicyEntityType, PolicySubjectRef
from okto_pulse.core.domain.quality_assessment import (
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.ports.semantic_subject_projection import (
    SemanticSubjectProjectionError,
    SemanticSubjectProjectionPort,
    SemanticSubjectProjectionRequest,
)

_CANONICAL_REQUIRED_FIELDS = (
    "confidence",
    "confidence_justification",
    "clarity",
    "clarity_justification",
    "assertiveness",
    "assertiveness_justification",
    "decidability",
    "decidability_justification",
    "ambiguity",
    "ambiguity_justification",
    "recommendation",
)
_CANONICAL_SCORE_DIMENSIONS = (
    "confidence",
    "clarity",
    "assertiveness",
    "decidability",
    "ambiguity",
)
_CANONICAL_ALLOWED_FIELDS = {
    "expected_validation_edition",
    "expected_spec_version",
    "expected_head_revision",
    *_CANONICAL_REQUIRED_FIELDS,
    "pinpoints",
}


def _canonical_pinpoints(value: object) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise CommandValidationError("pinpoints must be a list")
    normalized: list[dict[str, str]] = []
    identities: set[tuple[str | None, ...]] = set()
    required_fields = {"metric", "anchor_type", "detail"}
    allowed_fields = {*required_fields, "anchor_ref"}
    for raw in value:
        if (
            not isinstance(raw, Mapping)
            or not required_fields.issubset(raw)
            or not set(raw).issubset(allowed_fields)
        ):
            raise CommandValidationError(
                "each pinpoint must contain metric, anchor_type and detail; "
                "anchor_ref is optional"
            )
        try:
            pinpoint = SpecValidationPinpoint(
                metric=SpecValidationMetric(raw.get("metric")),
                anchor_type=SpecValidationPinpointAnchorType(raw.get("anchor_type")),
                anchor_ref=raw.get("anchor_ref"),
                detail=raw.get("detail"),
            )
        except (TypeError, ValueError) as exc:
            raise CommandValidationError(str(exc)) from exc
        projected = pinpoint.to_dict()
        identity = (
            projected["metric"],
            projected["anchor_type"],
            projected.get("anchor_ref"),
            projected["detail"],
        )
        if identity in identities:
            raise CommandValidationError("pinpoints must not contain duplicates")
        identities.add(identity)
        normalized.append(projected)
    return normalized


class SubmitSpecValidationCommand:
    """Input for :class:`SubmitSpecValidationUseCase`.

    Carries the validation payload verbatim so the exact dict the existing
    ``SpecService`` consumes is preserved (no field dropped). ``validate()``
    reproduces the REST handler's input checks as transport-neutral errors.
    """

    __slots__ = ("spec_id", "data")

    def __init__(self, spec_id: str, data: Mapping[str, Any]) -> None:
        self.spec_id = spec_id
        self.data = dict(data)

    def validate(self) -> None:
        unknown = sorted(set(self.data).difference(_CANONICAL_ALLOWED_FIELDS))
        if unknown:
            raise CommandValidationError(
                "Unknown spec validation fields: " + ", ".join(unknown)
            )
        for field_name in (
            "expected_validation_edition",
            "expected_spec_version",
            "expected_head_revision",
        ):
            value = self.data.get(field_name)
            minimum = 0 if field_name == "expected_head_revision" else 1
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise CommandValidationError(f"{field_name} is invalid")

        missing = [
            field
            for field in _CANONICAL_REQUIRED_FIELDS
            if self.data.get(field) is None
        ]
        if missing:
            raise CommandValidationError(
                "Missing required fields: " + ", ".join(missing)
            )
        if self.data.get("recommendation") not in ("approve", "reject"):
            raise CommandValidationError("recommendation must be 'approve' or 'reject'")
        for dimension in _CANONICAL_SCORE_DIMENSIONS:
            score = self.data.get(dimension)
            if (
                not isinstance(score, int)
                or isinstance(score, bool)
                or not 0 <= score <= 100
            ):
                raise CommandValidationError(f"{dimension} must be between 0 and 100")
            justification = self.data.get(f"{dimension}_justification")
            if not isinstance(justification, str) or len(justification.strip()) < 10:
                raise CommandValidationError(
                    f"{dimension}_justification must be at least 10 characters"
                )
        self.data["pinpoints"] = _canonical_pinpoints(self.data.get("pinpoints"))


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


async def _seal_pinpoint_snapshots(
    *,
    pinpoints: list[dict[str, Any]],
    spec: object,
    actor: ActorContext,
    uow: PulseUnitOfWork,
) -> list[dict[str, Any]]:
    if not pinpoints:
        return []
    projection = getattr(uow, "semantic_subject_projection", None)
    if not isinstance(projection, SemanticSubjectProjectionPort):
        raise TypeError("semantic_subject_projection_adapter_missing")
    subject = PolicySubjectRef(
        board_id=str(getattr(spec, "board_id")),
        entity_type=PolicyEntityType.SPEC,
        subject_id=str(getattr(spec, "id")),
        subject_version=int(getattr(spec, "version")),
        subject_edition=int(getattr(spec, "edition", 1) or 1),
    )
    sealed: list[dict[str, Any]] = []
    for raw in pinpoints:
        pinpoint = SpecValidationPinpoint.from_dict(raw)
        try:
            snapshot = await projection.resolve_semantic_anchor(
                SemanticSubjectProjectionRequest(
                    subject=subject,
                    anchor=UnboundFindingAnchor(
                        anchor_type=FindingAnchorType(pinpoint.anchor_type.value),
                        anchor_ref=pinpoint.anchor_ref,
                    ),
                    actor_id=actor.actor_id,
                )
            )
            sealed.append(pinpoint.seal(snapshot).to_dict())
        except SemanticSubjectProjectionError as exc:
            raise CommandValidationError(
                f"spec_validation_pinpoint_anchor_{exc.reason.value}"
            ) from exc
        except ValueError as exc:
            raise CommandValidationError(str(exc)) from exc
    return sealed


class SubmitSpecValidationUseCase:
    """Submit a spec validation gate record without any transport dependency."""

    async def execute(
        self,
        command: SubmitSpecValidationCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> SubmitSpecValidationResult:
        service = uow.services.specs
        spec = await _require_actor_board_spec(uow, command.spec_id, actor, write=True)
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
        sealed_pinpoints = await _seal_pinpoint_snapshots(
            pinpoints=command.data.get("pinpoints") or [],
            spec=spec,
            actor=actor,
            uow=uow,
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
                key: value for key, value in command.data.items() if value is not None
            }
            | {"pinpoints": sealed_pinpoints},
        )
        await commit(uow)
        return SubmitSpecValidationResult(payload=result)
