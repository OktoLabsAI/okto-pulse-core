"""Resolve authored current associations using the classification review policy.

Incomplete input cannot authorize deletion of an active set. Complete pending,
retired or outdated decisions contribute no current association. A surviving
partial scope remains current independently of another changed fragment.
"""
from collections import defaultdict
from dataclasses import replace

from okto_pulse.core.domain.architecture_classification_review import (
    architecture_classification_review,
)
from okto_pulse.core.ports.architecture_projection import ArchitectureAssociationProjection


def resolve_architecture_associations(
    *, board_id, spec_id, spec_edition, spec_version,
    population, decisions, integration_requirements,
) -> tuple[ArchitectureAssociationProjection, ...]:
    if not population.resolved:
        raise ValueError("architecture_projection_source_incomplete")
    candidates = defaultdict(list)
    records = defaultdict(list)
    requirements = defaultdict(list)
    for candidate in population.candidates:
        if candidate.spec_id != spec_id or candidate.spec_edition != spec_edition:
            raise ValueError("architecture_projection_scope_invalid")
        candidates[candidate.id].append(candidate)
    for record in decisions:
        if record.spec_id != spec_id or record.spec_edition != spec_edition:
            raise ValueError("architecture_projection_scope_invalid")
        records[record.candidate_id].append(record)
    for requirement in integration_requirements:
        requirements[requirement.get("id")].append(requirement)
    result = []
    # Group once: each decision, contract and selected IR is reviewed only in
    # its candidate group, without rescanning full populations per page.
    for identity in sorted(candidates.keys() | records.keys()):
        variants = candidates[identity]
        group = tuple(records[identity])
        selected_irs = tuple(
            requirement
            for ref in sorted({ref for record in group for ref in record.integration_requirement_ids})
            for requirement in requirements[ref]
        )
        detail = ({"candidate_id": identity, "source_digest": variants[0].source_digest}
                  if len(variants) == 1 else {})
        reviewed = architecture_classification_review(
            board_id=board_id, spec_id=spec_id, spec_edition=spec_edition,
            spec_version=spec_version,
            population=replace(population, candidates=tuple(variants)),
            decisions=group, integration_requirements=selected_irs, **detail,
        )
        if not reviewed["enumeration_complete"] or len(reviewed["items"]) != 1:
            raise ValueError("architecture_projection_source_incomplete")
        row = reviewed["items"][0]
        if row["state"] in {"unavailable", "unresolved"}:
            raise ValueError("architecture_projection_source_incomplete")
        if not variants or row["state"] in {"pending", "retired"}:
            continue
        current_ids = {item["decision_id"] for item in row["decisions"] if item["state"] == "current"}
        candidate = variants[0]
        for record in group:
            if record.id not in current_ids:
                continue
            for design_id, _revision in candidate.adopted_sources:
                for requirement_id in record.integration_requirement_ids:
                    result.append(ArchitectureAssociationProjection(
                        board_id, spec_id, spec_edition, identity, design_id,
                        candidate.interface_id, requirement_id, record.id,
                        candidate.source_digest, candidate.contract_json,
                    ))
    return tuple(result)
