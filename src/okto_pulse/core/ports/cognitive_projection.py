"""Literal cognitive projection rules, never write or admission authority."""

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


def validate_cognitive_projection_sources(*, schema, board_id, records):
    """Validate every captured revision before selecting the latest sources.

    Returns the original records, including SQL JSON text, without rewriting
    authenticated snapshot bytes. Older revisions, scope and aggregate limits
    are checked even when a later revision would otherwise hide them.
    This proves source shape/integrity only, never evidence or access authority.
    """
    from okto_pulse.core.application.cognitive_projection import validate_sources
    return validate_sources(schema=schema, board_id=board_id, records=records)


def cognitive_projection_source_node(*, schema, board_id, record):
    """Decode one validated durable payload into portable graph properties.

    Missing nullable fields stay NULL, including generation if absent from the
    payload. The sole provenance fallback is the established source_session_id
    replay rule. This does not select a generation, authorize writes, infer
    relations, or prove maturity, evidence, connectivity or accessibility.
    Validate the complete source inventory before using individual records.
    """
    from okto_pulse.core.application.cognitive_projection import source_node
    return source_node(schema=schema, board_id=board_id, record=record)
