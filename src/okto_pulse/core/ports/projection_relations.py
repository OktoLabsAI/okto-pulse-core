"""Read-only correspondence between a fenced source plan and logical edges."""

from dataclasses import dataclass
import re


@dataclass(frozen=True, slots=True)
class ProjectionRelationComparison:
    expected_count: int
    matched_count: int
    missing_count: int
    unresolved_count: int
    unexpected_new_count: int
    unplanned_existing_count: int
    duplicate_expected_count: int
    expected_sha256: str
    issues: tuple[str, ...]
    issues_truncated: bool

    def __post_init__(self):
        counts = (self.expected_count, self.matched_count, self.missing_count, self.unresolved_count,
            self.unexpected_new_count, self.unplanned_existing_count, self.duplicate_expected_count)
        if (any(type(value) is not int or not 0 <= value <= 500_000 for value in counts)
                or self.expected_count != self.matched_count + self.missing_count
                or type(self.expected_sha256) is not str or re.fullmatch('[0-9a-f]{64}', self.expected_sha256) is None
                or type(self.issues) is not tuple or len(self.issues) > 100
                or any(type(item) is not str or len(item) > 4096 for item in self.issues)
                or type(self.issues_truncated) is not bool):
            raise ValueError('projection_relation_comparison_invalid')


def compare_projection_relations(*, document, schema, nodes, relations, new_sessions):
    """Compare complete inventories; never authenticate a client-supplied plan.

    The caller rederives the plan from fenced authoritative sources and proves
    the inventory and ACK sessions independently. An ambiguous historical
    endpoint remains unresolved. No record is written, removed or reparented.
    """
    from okto_pulse.core.application.projection_relations import compare
    return compare(document=document, schema=schema, nodes=nodes, relations=relations, new_sessions=new_sessions)
