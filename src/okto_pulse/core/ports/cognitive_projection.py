"""Read-only comparison with durable cognitive sources, never admission authority."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CognitiveProjectionParity:
    node_type: str
    node_id: str
    generation: int
    source_revision: int
    source_fingerprint: str
    state: str
    differing_fields: tuple[str, ...]
    usage_differences: tuple[str, ...]


def compare_cognitive_projection(*, schema, board_id, record, node):
    """Compare a verified source to an optional portable node without writes.

    Matching content does not prove evidence validity, connectivity, maturity,
    permission, source accessibility or eligibility for runtime admission.
    The caller authenticates the source snapshot and complete graph inventory.
    """
    from okto_pulse.core.application.cognitive_projection import compare
    return compare(schema=schema, board_id=board_id, record=record, node=node)
