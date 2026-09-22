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


def build_global_projection_seed(*, source_input, expected_sources, sources, summary_embedding):
    """Apply publication policy to a complete authenticated Board inventory.

    No graph, embedding or relational I/O occurs here. The caller establishes
    source scope/stability and supplies the embedding from its admitted provider.
    A seed remains a projection input, never a cutover or access grant.
    """
    from okto_pulse.core.application.global_projection import build_seed
    return build_seed(source_input=source_input, expected_sources=expected_sources,
        sources=sources, summary_embedding=summary_embedding)
