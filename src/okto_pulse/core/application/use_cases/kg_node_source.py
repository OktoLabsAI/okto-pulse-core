"""Bounded, authorized navigation from KG provenance to its owning artifact.

No graph engine or UI routing dependency: the inbound adapter reads the node;
this use case resolves its declared provenance through existing relational ports.
It never guesses ownership from node titles, opaque code sources or graph edges.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
    EvaluateCodeTraceabilityKGReadAccessUseCase,
)
from okto_pulse.core.kg.cognitive_source_ref_resolver import resolve_cognitive_source_ref
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


@dataclass(frozen=True, slots=True)
class KGSourceTarget:
    board_id: str
    entity_type: str
    entity_kind: str
    entity_id: str
    title: str
    source_version: int | None = None


@dataclass(frozen=True, slots=True)
class KGNodeSourceResult:
    status: Literal["resolved", "missing_source", "unsupported", "unavailable"]
    source_artifact_ref: str | None
    target: KGSourceTarget | None = None


_OWNERS = {
    "spec": ("specs", "get_spec"),
    "refinement": ("refinements", "get_refinement"),
    "ideation": ("ideations", "get_ideation"),
    "story": ("stories", "get_story"),
    "card": ("cards", "get_card"),
}
_INDIRECT = {"code_investigation_receipt", "code_evidence", "implementation_target"}


def _text(value: object) -> str:
    # Domain enums are string-valued, but str(Enum) need not be its wire value.
    return str(getattr(value, "value", value))


class ResolveKGNodeSourceUseCase:
    async def execute(
        self,
        *,
        board_id: str,
        source_artifact_ref: str | None,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> KGNodeSourceResult:
        if await load_accessible_board(uow, board_id, actor) is None:
            raise EntityNotFoundError("board", board_id)
        ref = source_artifact_ref
        if not ref:
            return KGNodeSourceResult("missing_source", ref)
        if len(ref) > 2048 or ref != ref.strip() or any(ord(c) < 32 for c in ref):
            return KGNodeSourceResult("unsupported", ref)
        # Reuse the governed cognitive alias rules (notably card:bug:<uuid>).
        canonical = (
            resolve_cognitive_source_ref(ref).canonical_artifact_ref
            if ref.startswith(("card:bug:", "bug:")) else ref
        )
        parts = canonical.split(":")
        if len(parts) < 2 or not parts[1]:
            return KGNodeSourceResult("unsupported", ref)
        kind, entity_id = parts[0], parts[1]
        source_version = None
        if kind in _INDIRECT:
            # Do not interpret suffixes or opaque source_ref values as owners.
            if len(parts) != 2:
                return KGNodeSourceResult("unsupported", ref)
            access = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
                actor=actor, board_id=board_id, uow=uow,
            )
            if not access.allowed:
                return KGNodeSourceResult("unavailable", ref)
            if kind == "code_investigation_receipt":
                record = await uow.services.code_investigations.get_receipt(
                    board_id=board_id, receipt_id=entity_id,
                )
                owner_fields = ("subject_type", "subject_id", "subject_version")
            elif kind == "code_evidence":
                record = await uow.services.code_traceability.get_evidence(
                    board_id=board_id, evidence_id=entity_id,
                )
                owner_fields = ("parent_type", "parent_id", "parent_version")
            else:
                record = await uow.services.code_traceability.get_target(
                    board_id=board_id, target_id=entity_id,
                )
                owner_fields = ("", "card_id", "")
            if record is None or record.board_id != board_id:
                return KGNodeSourceResult("unavailable", ref)
            kind = _text(getattr(record, owner_fields[0])) if owner_fields[0] else "card"
            entity_id = getattr(record, owner_fields[1])
            source_version = getattr(record, owner_fields[2], None) if owner_fields[2] else None
        kind = {"task": "card", "test": "card", "bug": "card"}.get(kind, kind)
        if kind not in _OWNERS or not isinstance(entity_id, str) or not entity_id:
            return KGNodeSourceResult("unsupported", ref)
        try:
            await require_authorization(
                actor, PermissionRequirement(f"{kind}.entity.read"), uow=uow, board_id=board_id,
            )
        except PermissionDeniedError:
            # Same envelope for removed, forbidden and out-of-board artifacts.
            return KGNodeSourceResult("unavailable", ref)
        service_name, method_name = _OWNERS[kind]
        entity = await getattr(getattr(uow.services, service_name), method_name)(entity_id)
        if entity is None or entity.board_id != board_id:
            return KGNodeSourceResult("unavailable", ref)
        entity_kind = _text(getattr(entity, "card_type", "card")) if kind == "card" else kind
        return KGNodeSourceResult(
            "resolved", ref,
            KGSourceTarget(
                board_id=board_id, entity_type=kind, entity_kind=entity_kind,
                entity_id=entity_id, title=entity.title or entity_id,
                source_version=source_version,
            ),
        )
