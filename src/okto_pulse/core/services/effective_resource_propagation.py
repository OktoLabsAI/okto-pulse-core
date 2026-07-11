"""R3 — propagate / copy EFFECTIVE (direct + inherited) Resource Gate resources.

Two entry points share the same effective-lineage resolution so the Resource Gate
sees a consistent identity end to end:

- ``propagate_effective_resources_to_spec`` (R3-IMP1): when a spec is created
  manually with a ``refinement_id``, propagate the refinement's EFFECTIVE
  resources (its own direct resources, or — when a type is absent on the
  refinement but present on an ancestor — the inherited/effective ones) into the
  new spec. Reuses the SAME primitives ``propagate_artifacts`` /
  ``propagate_architecture_designs`` that ``RefinementService.derive_spec`` uses,
  so the manual path and the derive path converge instead of drifting.

- ``resolve_effective_card_copy_plan`` (R3-IMP2): when a copy tool copies a spec
  resource to a card and the spec has no DIRECT resource of that type, resolve the
  spec's effective lineage so the copy tool can fall back to the inherited resource
  (preserving an identity field the gate reads) instead of returning a generic
  "no resources to copy" while the gate reports the resource as provided.

Contract (codex R3 decisions):
- Resolve the lineage via ``ResolvedResourceLineageService`` (the canonical
  resolver). A resolution failure raises ``ResourceLineageResolutionError`` — never
  a silent empty success.
- A propagation failure raises ``ResourcePropagationError`` carrying
  ``failed_resource_type`` / ``coverage_obligation_id`` / ``retryable`` — never a
  best-effort partial success and never an auto-created N/A.
- The copied / propagated resource persists at least one field read by
  ``ResourceGateService._resource_identity_values`` whose value equals the
  effective resource id, so the gate can match it to its coverage obligation.
- The aggregate status is ``created_with_effective_fallback`` if ANY type used an
  inherited/effective fallback; else ``created_with_resources`` if every used
  resource was direct; else ``created_without_required_resources``.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from okto_pulse.core.application.artifact_propagation import propagate_artifacts
from okto_pulse.core.ports.effective_resource import (
    get_effective_resource_persistence_port,
)

logger = logging.getLogger("okto_pulse.core.services.effective_resource_propagation")

# Per-type + aggregate status vocabulary (structured response field, NOT a
# persisted enum).
STATUS_WITH_RESOURCES = "created_with_resources"
STATUS_EFFECTIVE_FALLBACK = "created_with_effective_fallback"
STATUS_WITHOUT_REQUIRED = "created_without_required_resources"
STATUS_NOT_APPLICABLE = "not_applicable"

# Gate-readable identity fields, surfaced in actionable copy errors so a caller
# knows what identity a copied resource must carry to satisfy the obligation.
ACCEPTED_IDENTITY_FIELDS: dict[str, tuple[str, ...]] = {
    "knowledge_base": ("source_kb_id", "id", "source", "source_ref"),
    "mockup": ("id", "origin_id", "source_mockup_id"),
    "architecture": ("source_design_id", "source_ref", "id"),
}

# The lineage chain ancestor entity types, nearest first, per root entity type.
_PARENT_CHAIN: dict[str, tuple[str, ...]] = {
    "refinement": ("ideation",),
    "spec": ("refinement", "ideation"),
}


class ResourceLineageResolutionError(Exception):
    """The Resource Gate lineage could not be resolved — fail closed, never a
    silent empty propagation (maps to ``resource_lineage_resolution_failed``)."""

    def __init__(self, message: str, *, code: str = "resource_lineage_resolution_failed",
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "error": "resource_lineage_resolution_failed",
            "detail": str(self),
            "lineage_error_code": self.code,
            "lineage_error_details": self.details,
        }


class ResourcePropagationError(Exception):
    """A resource that the gate reports as provided could not be propagated/copied
    — surfaced as a structured, actionable error (maps to
    ``resource_propagation_failed``); never masked as success."""

    def __init__(
        self,
        message: str,
        *,
        failed_resource_type: str,
        coverage_obligation_id: str | None = None,
        retryable: bool = True,
    ):
        super().__init__(message)
        self.failed_resource_type = failed_resource_type
        self.coverage_obligation_id = coverage_obligation_id
        self.retryable = retryable

    def to_error_dict(self, *, spec_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "error": "resource_propagation_failed",
            "detail": str(self),
            "failed_resource_type": self.failed_resource_type,
            "coverage_obligation_id": self.coverage_obligation_id,
            "accepted_identity_fields": list(
                ACCEPTED_IDENTITY_FIELDS.get(self.failed_resource_type, ())
            ),
            "retryable": self.retryable,
        }
        if spec_id is not None:
            payload["spec_id"] = spec_id
        return payload


async def _resolve_lineage(db: Any, board_id: str, entity_type: str, entity_id: str):
    """Resolve the canonical effective lineage. Raises
    ``ResourceLineageResolutionError`` on any resolution failure (lineage error or
    a missing/invalid entity) so callers fail closed before persisting."""
    from okto_pulse.core.services.resource_gate import (
        ResourceGateService,
        ResourceGateViolation,
    )
    from okto_pulse.core.services.resource_lineage import (
        ResolvedResourceLineageService,
        ResourceLineageError,
    )

    try:
        return await ResolvedResourceLineageService(ResourceGateService(db)).resolve(
            board_id, entity_type, entity_id, include_coverage=True,
        )
    except (ResourceLineageError, ResourceGateViolation) as exc:
        details = dict(getattr(exc, "details", {}) or {})
        details.setdefault("entity_type", entity_type)
        details.setdefault("entity_id", entity_id)
        raise ResourceLineageResolutionError(
            f"could not resolve resource lineage for {entity_type} '{entity_id}': {exc}",
            code=getattr(exc, "code", "resource_lineage_resolution_failed"),
            details=details,
        ) from exc
    except Exception as exc:  # entity not found / unexpected provider failure
        raise ResourceLineageResolutionError(
            f"could not resolve resource lineage for {entity_type} '{entity_id}': {exc}",
            details={"entity_type": entity_type, "entity_id": entity_id,
                     "exception": type(exc).__name__},
        ) from exc


def _states_by_type(lineage) -> dict[str, Any]:
    return {state.resource_type: state for state in lineage.resource_states}


def _obligation_id_for_type(lineage, resource_type: str) -> str | None:
    for obligation in getattr(lineage, "coverage_obligations", ()) or ():
        if obligation.resource_type == resource_type:
            return obligation.unique_resource_id
    state = _states_by_type(lineage).get(resource_type)
    if state is not None:
        for ref in (*state.direct_refs, *state.inherited_refs):
            value = ref.get("unique_resource_id") or ref.get("id")
            if value:
                return str(value)
    return None


def _effective_ref_identity(resource_type: str, ref: dict[str, Any]) -> str:
    for key in (
        "unique_resource_id",
        "source_design_id",
        "source_kb_id",
        "source_mockup_id",
        "origin_id",
        "source_ref",
        "origin_ref",
        "source",
        "id",
    ):
        value = ref.get(key)
        if value:
            return f"{resource_type}:{value}"
    source_entity_type = ref.get("source_entity_type") or "unknown"
    source_entity_id = ref.get("source_entity_id") or "unknown"
    title = ref.get("title") or "untitled"
    return f"{resource_type}:{source_entity_type}:{source_entity_id}:{title}"


def _dedupe_effective_refs(
    resource_type: str,
    refs: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for ref in refs:
        item = dict(ref)
        identity = _effective_ref_identity(resource_type, item)
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(item)
    return deduped


def effective_ref_identity_values(ref: dict[str, Any]) -> set[str]:
    """Every id token by which a caller may legitimately reference an effective
    inherited ref — the immediate snapshot id AND every canonical-origin field
    (source_design_id, unique_resource_id, source_ref tail, origin_evidence.*,
    raw.*). A ``design_ids`` filter must match whether the caller passes the
    intermediate snapshot id or the inherited ROOT id (card 5c43a364).
    """
    values: set[str] = set()

    def _add(value: Any) -> None:
        if not value:
            return
        text = str(value)
        values.add(text)
        for sep in (":", "/", "\\"):
            if sep in text:
                values.add(text.rsplit(sep, 1)[-1])

    for key in (
        "id",
        "resource_id",
        "source_design_id",
        "unique_resource_id",
        "source_kb_id",
        "source_mockup_id",
        "origin_id",
        "source_ref",
        "origin_ref",
        "source",
    ):
        _add(ref.get(key))
    for nested_key in ("origin_evidence", "raw"):
        nested = ref.get(nested_key)
        if isinstance(nested, dict):
            for key in ("id", "source_design_id", "unique_resource_id", "source_ref"):
                _add(nested.get(key))
    return values


def _kb_dicts(entity, *, source_type: str, source_id: str | None,
              source_title: str | None, source_version: int | None) -> list[dict[str, Any]]:
    """Snapshot an entity's knowledge bases to source dicts that
    ``propagate_artifacts`` consumes. ``id`` is the EFFECTIVE kb id, copied into
    ``source_kb_id`` on the target so the gate can match the obligation."""
    out: list[dict[str, Any]] = []
    for kb in (getattr(entity, "knowledge_bases", None) or []):
        out.append({
            "id": kb.id,
            "title": kb.title,
            "description": getattr(kb, "description", None),
            "content": kb.content,
            "mime_type": getattr(kb, "mime_type", None) or "text/markdown",
            # R6-IMP4: carry the parent's canonical root forward so a later hop
            # (e.g. card copy) preserves the initial origin, not the effective kb.
            "root_source_kb_id": getattr(kb, "root_source_kb_id", None),
            "source_type": source_type,
            "source_id": source_id,
            "source_title": source_title,
            "source_version": source_version,
        })
    return out


def _mockup_dicts(entity) -> list[dict[str, Any]]:
    return [m for m in (getattr(entity, "screen_mockups", None) or []) if isinstance(m, dict)]


async def propagate_effective_resources_to_spec(
    db: Any,
    *,
    board_id: str,
    spec,
    refinement_id: str,
    user_id: str,
    mockup_ids: list[str] | None = None,
    kb_ids: list[str] | None = None,
    architecture_design_ids: list[str] | None = None,
    architecture_propagation_mode: str = "copy",
) -> dict[str, Any]:
    """R3-IMP1. Propagate the refinement's EFFECTIVE resources into ``spec``.

    Raises ``ResourceLineageResolutionError`` (resolution failure) or
    ``ResourcePropagationError`` (a provided resource could not be propagated) so
    the caller can fail the create transactionally — never a silent success.
    Returns ``{"status", "by_type", "counts"}``.
    """
    from okto_pulse.core.services.main import (
        RefinementService,
        propagate_architecture_designs,
    )

    lineage = await _resolve_lineage(db, board_id, "refinement", refinement_id)
    states = _states_by_type(lineage)

    refinement = await RefinementService(db).get_refinement(refinement_id)
    if refinement is None:  # lineage resolved but row vanished — fail closed
        raise ResourceLineageResolutionError(
            f"refinement '{refinement_id}' not found for resource propagation",
            details={"entity_type": "refinement", "entity_id": refinement_id},
        )
    ideation = getattr(refinement, "ideation", None)

    # Source entity per (resource_type, kind). nearest-first: refinement (direct)
    # then ideation (inherited/effective).
    self_entity = refinement
    ancestor_entity = ideation

    by_type: dict[str, Any] = {}
    counts = {"knowledge_base": 0, "mockup": 0, "architecture": 0}

    # --- Q&A: context only (not a gated resource). Propagate from the refinement
    #     once, mirroring derive_spec, so the manual spec keeps the decisions. ---
    qa_snapshot = list(getattr(refinement, "qa_items", None) or [])

    # --- knowledge_base ---
    kb_state = states.get("knowledge_base")
    try:
        if kb_state is not None and kb_state.na_mark:
            by_type["knowledge_base"] = {"status": STATUS_NOT_APPLICABLE, "source": None, "copied": 0}
        elif kb_state is not None and kb_state.direct_count > 0:
            kbs = _kb_dicts(self_entity, source_type="refinement", source_id=refinement.id,
                            source_title=refinement.title, source_version=refinement.version)
            await propagate_artifacts(
                db,
                source_mockups=None,
                source_qa_items=qa_snapshot,
                source_knowledge_bases=kbs,
                target_entity=spec,
                target_kb_entity="spec_knowledge_base",
                user_id=user_id,
                mockup_ids=None,
                kb_ids=kb_ids,
                source_type="refinement",
                source_id=refinement.id,
                source_title=refinement.title,
                source_version=refinement.version,
            )
            qa_snapshot = []  # only propagate Q&A once
            counts["knowledge_base"] = len(kbs)
            by_type["knowledge_base"] = {
                "status": STATUS_WITH_RESOURCES, "source": "direct",
                "source_entity_type": "refinement", "source_entity_id": refinement.id,
                "copied": len(kbs),
            }
        elif kb_state is not None and kb_state.inherited_count > 0 and ancestor_entity is not None:
            kbs = _kb_dicts(ancestor_entity, source_type="ideation", source_id=ancestor_entity.id,
                            source_title=getattr(ancestor_entity, "title", None),
                            source_version=getattr(ancestor_entity, "version", None))
            await propagate_artifacts(
                db,
                source_mockups=None,
                source_qa_items=qa_snapshot,
                source_knowledge_bases=kbs,
                target_entity=spec,
                target_kb_entity="spec_knowledge_base",
                user_id=user_id,
                mockup_ids=None,
                kb_ids=kb_ids,
                source_type="ideation",
                source_id=ancestor_entity.id,
                source_title=getattr(ancestor_entity, "title", None),
                source_version=getattr(ancestor_entity, "version", None),
            )
            qa_snapshot = []
            counts["knowledge_base"] = len(kbs)
            by_type["knowledge_base"] = {
                "status": STATUS_EFFECTIVE_FALLBACK, "source": "effective_fallback",
                "source_entity_type": "ideation", "source_entity_id": ancestor_entity.id,
                "copied": len(kbs),
            }
        else:
            by_type["knowledge_base"] = {"status": STATUS_WITHOUT_REQUIRED, "source": None, "copied": 0}
    except Exception as exc:
        raise ResourcePropagationError(
            f"knowledge_base propagation failed: {exc}",
            failed_resource_type="knowledge_base",
            coverage_obligation_id=_obligation_id_for_type(lineage, "knowledge_base"),
        ) from exc

    # --- mockup ---
    mk_state = states.get("mockup")
    try:
        if mk_state is not None and mk_state.na_mark:
            by_type["mockup"] = {"status": STATUS_NOT_APPLICABLE, "source": None, "copied": 0}
        elif mk_state is not None and mk_state.direct_count > 0:
            mockups = _mockup_dicts(self_entity)
            await propagate_artifacts(
                db,
                source_mockups=mockups,
                source_qa_items=qa_snapshot,
                source_knowledge_bases=None,
                target_entity=spec,
                target_kb_entity=None,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=None,
            )
            qa_snapshot = []
            counts["mockup"] = len(mockups)
            by_type["mockup"] = {
                "status": STATUS_WITH_RESOURCES, "source": "direct",
                "source_entity_type": "refinement", "source_entity_id": refinement.id,
                "copied": len(mockups),
            }
        elif mk_state is not None and mk_state.inherited_count > 0 and ancestor_entity is not None:
            mockups = _mockup_dicts(ancestor_entity)
            await propagate_artifacts(
                db,
                source_mockups=mockups,
                source_qa_items=qa_snapshot,
                source_knowledge_bases=None,
                target_entity=spec,
                target_kb_entity=None,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=None,
            )
            qa_snapshot = []
            counts["mockup"] = len(mockups)
            by_type["mockup"] = {
                "status": STATUS_EFFECTIVE_FALLBACK, "source": "effective_fallback",
                "source_entity_type": "ideation", "source_entity_id": ancestor_entity.id,
                "copied": len(mockups),
            }
        else:
            by_type["mockup"] = {"status": STATUS_WITHOUT_REQUIRED, "source": None, "copied": 0}
    except Exception as exc:
        raise ResourcePropagationError(
            f"mockup propagation failed: {exc}",
            failed_resource_type="mockup",
            coverage_obligation_id=_obligation_id_for_type(lineage, "mockup"),
        ) from exc

    # --- architecture (delegates to the existing snapshot-copy primitive) ---
    arch_state = states.get("architecture")
    try:
        if arch_state is not None and arch_state.na_mark:
            by_type["architecture"] = {"status": STATUS_NOT_APPLICABLE, "source": None, "copied": 0}
        elif arch_state is not None and arch_state.direct_count > 0:
            designs = await propagate_architecture_designs(
                db, source_parent_type="refinement", source_parent_id=refinement_id,
                target_parent_type="spec", target_parent_id=spec.id, actor_id=user_id,
                mode=architecture_propagation_mode, design_ids=architecture_design_ids,
            )
            counts["architecture"] = len(designs)
            by_type["architecture"] = {
                "status": STATUS_WITH_RESOURCES, "source": "direct",
                "source_entity_type": "refinement", "source_entity_id": refinement_id,
                "copied": len(designs),
            }
        elif arch_state is not None and arch_state.inherited_count > 0 and ideation is not None:
            designs = await propagate_architecture_designs(
                db, source_parent_type="ideation", source_parent_id=ideation.id,
                target_parent_type="spec", target_parent_id=spec.id, actor_id=user_id,
                mode=architecture_propagation_mode, design_ids=architecture_design_ids,
            )
            counts["architecture"] = len(designs)
            by_type["architecture"] = {
                "status": STATUS_EFFECTIVE_FALLBACK, "source": "effective_fallback",
                "source_entity_type": "ideation", "source_entity_id": ideation.id,
                "copied": len(designs),
            }
        else:
            by_type["architecture"] = {"status": STATUS_WITHOUT_REQUIRED, "source": None, "copied": 0}
    except Exception as exc:
        raise ResourcePropagationError(
            f"architecture propagation failed: {exc}",
            failed_resource_type="architecture",
            coverage_obligation_id=_obligation_id_for_type(lineage, "architecture"),
        ) from exc

    aggregate = _aggregate_status(by_type)
    logger.info(
        "resource_gate.spec_effective_propagation board=%s spec=%s refinement=%s status=%s",
        board_id, spec.id, refinement_id, aggregate,
        extra={"event": "resource_gate.spec_effective_propagation",
               "board_id": board_id, "spec_id": spec.id,
               "refinement_id": refinement_id, "status": aggregate},
    )
    return {"status": aggregate, "by_type": by_type, "counts": counts}


def _aggregate_status(by_type: dict[str, Any]) -> str:
    statuses = {info.get("status") for info in by_type.values()}
    if STATUS_EFFECTIVE_FALLBACK in statuses:
        return STATUS_EFFECTIVE_FALLBACK
    if STATUS_WITH_RESOURCES in statuses:
        return STATUS_WITH_RESOURCES
    return STATUS_WITHOUT_REQUIRED


async def resolve_effective_card_copy_plan(
    db: Any,
    *,
    board_id: str,
    spec_id: str,
    resource_type: str,
) -> dict[str, Any]:
    """R3-IMP2. Resolve whether a spec resource of ``resource_type`` is available
    DIRECTLY, only as an inherited/effective fallback, N/A, or genuinely absent —
    so a copy tool can fall back instead of returning a generic "nothing to copy"
    while the gate reports it provided.

    Returns ``{has_direct, fallback, source_entity_type, source_entity_id,
    not_applicable, has_obligation, coverage_obligation_id, accepted_identity_fields}``.
    Raises ``ResourceLineageResolutionError`` on a resolution failure.
    """
    lineage = await _resolve_lineage(db, board_id, "spec", spec_id)
    state = _states_by_type(lineage).get(resource_type)
    obligation_id = _obligation_id_for_type(lineage, resource_type)
    accepted = list(ACCEPTED_IDENTITY_FIELDS.get(resource_type, ()))

    plan = {
        "resource_type": resource_type,
        "has_direct": bool(state and state.direct_count > 0),
        "fallback": False,
        "fallback_refs": [],
        "not_applicable": bool(state and state.na_mark),
        "has_obligation": obligation_id is not None,
        "coverage_obligation_id": obligation_id,
        "accepted_identity_fields": accepted,
        "source_entity_type": None,
        "source_entity_id": None,
    }
    if not plan["has_direct"] and not plan["not_applicable"] and state and state.inherited_count > 0:
        fallback_refs = _dedupe_effective_refs(resource_type, state.inherited_refs or ())
        ref = (fallback_refs or [{}])[0]
        plan["fallback"] = True
        plan["fallback_refs"] = fallback_refs
        plan["source_entity_type"] = ref.get("source_entity_type")
        plan["source_entity_id"] = ref.get("source_entity_id")
    return plan


async def load_effective_kb_items(
    db: Any, source_entity_type: str, source_entity_id: str
) -> list[dict[str, Any]]:
    """Load the knowledge-base CONTENT of an effective ancestor (ideation/
    refinement/spec) for a card-copy fallback. ``id`` is the EFFECTIVE kb id the
    copy tool must echo into a gate-readable identity field."""
    return await get_effective_resource_persistence_port().load_knowledge_bases(
        db,
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
    )


async def load_effective_mockup_items(
    db: Any, source_entity_type: str, source_entity_id: str
) -> list[dict[str, Any]]:
    """Load the screen-mockup payloads of an effective ancestor for a card-copy
    fallback. Each mockup keeps its original ``id`` (the effective/obligation id)."""
    return await get_effective_resource_persistence_port().load_mockups(
        db,
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
    )


__all__ = [
    "ACCEPTED_IDENTITY_FIELDS",
    "STATUS_EFFECTIVE_FALLBACK",
    "STATUS_NOT_APPLICABLE",
    "STATUS_WITHOUT_REQUIRED",
    "STATUS_WITH_RESOURCES",
    "ResourceLineageResolutionError",
    "ResourcePropagationError",
    "propagate_effective_resources_to_spec",
    "resolve_effective_card_copy_plan",
    "load_effective_kb_items",
    "load_effective_mockup_items",
]
