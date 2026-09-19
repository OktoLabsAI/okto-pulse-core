"""Read adopted architecture contracts through the existing public ports.

Callers must authorize the Spec read before entering this service. It does not
grant start permission, write classifications, refresh snapshots or fetch refs.
The canonical lineage chooses the adopted owner of each root; all physical
variants at that owner are retained so conflicting contracts cannot disappear
through metadata deduplication.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.domain.architecture_candidates import (
    AdoptedArchitectureDesign,
    ArchitectureCandidatePopulation,
    project_architecture_candidates,
)
from okto_pulse.core.ports.architecture_persistence import (
    ArchitectureFilter,
    ArchitectureQuery,
    get_architecture_persistence_port,
)
from okto_pulse.core.services.resource_gate import ResourceGateService
from okto_pulse.core.services.resource_lineage import ResolvedResourceLineageService


async def load_spec_architecture_candidates(
    context: Any, *, board_id: str, spec_id: str,
) -> ArchitectureCandidatePopulation:
    store = get_architecture_persistence_port()
    spec = await store.get(context, entity="spec", record_id=spec_id)
    if spec is None or spec.board_id != board_id:
        raise ValueError("architecture_candidate_scope_unavailable")

    def project(
        designs: tuple[AdoptedArchitectureDesign, ...] = (), *, complete: bool,
    ) -> ArchitectureCandidatePopulation:
        return project_architecture_candidates(
            board_id=board_id, spec_id=spec_id, spec_edition=spec.edition,
            designs=tuple(designs), source_complete=complete,
        )

    try:
        lineage = await ResolvedResourceLineageService(
            ResourceGateService(context)
        ).resolve(board_id, "spec", spec_id, include_coverage=True, projection_profile="gate")
    except Exception:
        # An inaccessible or unenumerable source is not an empty adopted set.
        # Do not expose provider messages, paths or another scope's identifiers.
        return project(complete=False)

    adopted_owners = {
        obligation.unique_resource_id: (
            obligation.source_entity_type, obligation.source_entity_id,
        )
        for obligation in lineage.coverage_obligations
        if obligation.resource_type == "architecture"
    }
    selected = [
        attachment for attachment in lineage.attachments
        if attachment.resource_type == "architecture"
        and attachment.effective
        and attachment.attachment_kind != "not_applicable"
        and adopted_owners.get(attachment.unique_resource_id) == (
            attachment.source_entity_type, attachment.source_entity_id,
        )
    ]
    if not selected:
        # A claimed effective root with no recoverable attachment is unknown.
        return project(complete=not adopted_owners)
    if any(not item.resource_id or item.revision_stamp is None for item in selected):
        return project(complete=False)

    ids = tuple(sorted({item.resource_id for item in selected}))
    try:
        rows = await store.list(context, ArchitectureQuery(
            entity="architecture_design",
            filters=(ArchitectureFilter("board_id", "eq", board_id),
                     ArchitectureFilter("id", "in", ids)),
        ))
    except Exception:
        return project(complete=False)
    by_id = {row.id: row for row in rows}
    if set(by_id) != set(ids) or len(by_id) != len(rows):
        return project(complete=False)

    designs: list[AdoptedArchitectureDesign] = []
    seen: set[str] = set()
    for attachment in selected:
        row = by_id[attachment.resource_id]
        parent_type = attachment.source_entity_type
        if (
            row.board_id != board_id or row.parent_type != parent_type
            or getattr(row, f"{parent_type}_id", None) != attachment.source_entity_id
        ):
            return project(complete=False)
        # Metadata and payload must describe the same observed physical revision.
        observed_version = attachment.raw.get("design_version")
        if type(observed_version) is not int or observed_version != row.version:
            return project(complete=False)
        if not isinstance(row.interfaces, (list, tuple)):
            # Malformed legacy JSON must not masquerade as confirmed empty.
            return project(complete=False)
        if row.id in seen:
            continue
        seen.add(row.id)
        designs.append(AdoptedArchitectureDesign(
            board_id=board_id, design_id=row.id,
            root_design_id=attachment.revision_stamp.root_id,
            revision=row.version, interfaces=tuple(row.interfaces),
        ))
    return project(tuple(designs), complete=True)
