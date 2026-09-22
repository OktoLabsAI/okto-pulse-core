"""Connectivity observations over authenticated inventories, never admission."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProjectionConnectivityObservation:
    node_type: str
    node_id: str
    writer_path: str
    outcome: str
    reasons: tuple[str, ...]
    advisories: tuple[str, ...]


def observe_projection_connectivity(*, schema, board_id, nodes, relations, selected):
    """Apply the existing guard to selected historical identities without writes.

    The caller authenticates the full inventory and its Board. Passing only
    describes connectivity: source authority, payload parity, edge policy,
    maturity and access remain independent requirements. No historical RDL
    ownership exception is inferred from a reference-shaped string.
    """
    from okto_pulse.core.application.projection_connectivity import observe
    return observe(schema=schema, board_id=board_id, nodes=nodes, relations=relations, selected=selected)
