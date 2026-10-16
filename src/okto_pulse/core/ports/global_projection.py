"""Typed source facts for the existing Global Discovery recovery projection."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GlobalProjectionSource:
    node_id: str
    node_type: str
    title: str
    embedding: tuple[float, ...]
    source_artifact_ref: str
    graph_layer: str
    canonical_bug_count: int = 0
    relates_to_endpoints: tuple[tuple[str, str | None], ...] = ()


@dataclass(frozen=True, slots=True)
class GlobalProjectionComparison:
    state: str
    expected_nodes: int
    matched_nodes: int
    missing_nodes: int
    changed_nodes: int
    unexpected_nodes: int
    missing_relations: int
    unexpected_relations: int
    expected_sha256: str


def global_projection_sources_from_inventory(*, schema, nodes, relations):
    """Adapt a complete authenticated Board inventory to the existing source policy."""
    from okto_pulse.core.application.global_projection_inventory import source_facts
    return source_facts(schema=schema, nodes=nodes, relations=relations)


def compare_global_projection(*, schema, nodes, relations, seeds):
    """Compare all Global records, leaving unowned auxiliary records unmatched.

    The caller authenticates the seeds, complete inventories and historical
    preservation. This observation grants neither source access nor cutover.
    """
    from okto_pulse.core.application.global_projection_inventory import compare
    return compare(schema=schema, nodes=nodes, relations=relations, seeds=seeds)


def global_projection_summary_text(*, board_id, board_name):
    """The existing summary-embedding input shared by live and cold recovery."""
    if type(board_id) is not str or not board_id or type(board_name) is not str:
        raise ValueError('global_projection_board_identity_invalid')
    return f'Board {board_name or board_id}'


def build_global_projection_seed(*, source_input, expected_sources, sources, summary_embedding):
    """Apply publication policy to a complete authenticated Board inventory.

    No graph, embedding or relational I/O occurs here. The caller establishes
    source scope/stability and supplies the embedding from its admitted provider.
    A seed remains a projection input, never a cutover or access grant.
    """
    from okto_pulse.core.application.global_projection import build_seed
    return build_seed(source_input=source_input, expected_sources=expected_sources,
        sources=sources, summary_embedding=summary_embedding)
