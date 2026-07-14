"""Pure physical Knowledge Graph ontology shared by ports and schema metadata."""

from __future__ import annotations

# Closed physical node vocabulary from the KG MVP contract. Semantic subtypes
# are data registered beneath these types and never extend this tuple.
NODE_TYPES: tuple[str, ...] = (
    "Decision",
    "Criterion",
    "Constraint",
    "Assumption",
    "Requirement",
    "Entity",
    "APIContract",
    "TestScenario",
    "Bug",
    "Learning",
    "Alternative",
)

__all__ = ["NODE_TYPES"]
