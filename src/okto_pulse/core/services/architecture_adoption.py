"""Initialize explicit selection in the same transaction as Spec creation."""

from typing import Any

from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope
from okto_pulse.core.ports.architecture_persistence import ArchitectureRecord
from okto_pulse.core.services.architecture import architecture_design_identity_values
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.resource_lineage import ResolvedResourceLineageService


async def initial_spec_architecture_adoption(
    context: Any, *, board_id: str, spec_id: str, actor_id: str,
    selected_design_ids: list[str] | None,
    source_parent_type: str | None, source_parent_id: str | None,
) -> dict[str, Any]:
    """Resolve initial parent roots before inserting the new Spec.

    None adopts currently effective roots; [] explicitly adopts no inherited
    root. This helper is not an update/repair API for pre-existing Specs.
    Snapshot/reference propagation remains owned by the existing copy service.
    """
    lineage = None
    if source_parent_type and source_parent_id:
        lineage = await ResolvedResourceLineageService(ResourceGateService(context)).resolve(
            board_id, source_parent_type, source_parent_id,
            include_coverage=False, projection_profile="gate", resource_types=("architecture",),
        )
    roots: set[str] = set()
    # Match the existing copy preflight's selection-token normalization.
    requested = (
        {str(item).strip() for item in selected_design_ids if str(item).strip()}
        if selected_design_ids is not None else None
    )
    matched: set[str] = set()
    for attachment in lineage.attachments if lineage is not None else ():
        if attachment.resource_type != "architecture" or not attachment.effective:
            continue
        if attachment.attachment_kind == "not_applicable":
            continue
        if attachment.revision_stamp is None or not attachment.resource_id:
            raise ValueError("architecture_adoption_source_identity_required")
        tokens = architecture_design_identity_values(ArchitectureRecord("architecture_design", {
            "id": attachment.resource_id,
            "source_design_id": attachment.revision_stamp.root_id,
            "source_ref": attachment.raw.get("source_ref"),
        }))
        if requested is None or tokens & requested:
            roots.add(attachment.unique_resource_id)
            if requested is not None:
                matched.update(tokens & requested)
    if requested is not None and matched != requested:
        raise ValueError("architecture_adoption_selection_unavailable")
    return ArchitectureAdoptionScope(
        board_id=board_id, spec_id=spec_id, adopted_in_edition=1,
        actor_id=actor_id, inherited_resource_ids=tuple(sorted(roots)),
    ).model_dump(mode="json")
