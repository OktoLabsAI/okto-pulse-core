"""Read-only correspondence between a fenced source plan and logical edges."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProjectionRelationComparison:
    expected_count: int
    matched_count: int
    missing_count: int
    unresolved_count: int
    unexpected_new_count: int
    expected_sha256: str
    issues: tuple[str, ...]
    issues_truncated: bool


def compare_projection_relations(*, document, schema, nodes, relations, new_sessions):
    """Compare complete inventories; never authenticate a client-supplied plan.

    The caller rederives the plan from fenced authoritative sources and proves
    the inventory and ACK sessions independently. An ambiguous historical
    endpoint remains unresolved. No record is written, removed or reparented.
    """
    from okto_pulse.core.application.projection_relations import compare
    return compare(document=document, schema=schema, nodes=nodes, relations=relations, new_sessions=new_sessions)
