"""Typed current associations for a derived KG projection, never delivery credit."""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArchitectureAssociationProjection:
    board_id: str
    spec_id: str
    spec_edition: int
    candidate_id: str
    design_id: str
    interface_id: str
    integration_requirement_id: str
    decision_id: str
    source_digest: str
    contract_json: str
