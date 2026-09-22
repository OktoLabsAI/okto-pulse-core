"""Typed observations between authenticated graph inventories, never authority.

Node identity can establish a changed record. Relations have no portable row ID:
changing their properties is an observed removal/addition, not an invented update
pairing. The caller must independently prove both inventories complete and bound
to the intended snapshots before using these observations for reconciliation.
"""

from dataclasses import dataclass
import re


def _identity(values):
    if any(type(value) is not str or not value or len(value) > 1024 for value in values):
        raise ValueError('projection_history_identity_invalid')


def _fingerprint(value):
    if type(value) is not str or re.fullmatch(r'[0-9a-f]{64}', value) is None:
        raise ValueError('projection_history_fingerprint_invalid')


@dataclass(frozen=True, slots=True, order=True)
class ProjectionNodeFingerprint:
    node_type: str
    node_id: str
    fingerprint: str

    def __post_init__(self):
        _identity((self.node_type, self.node_id))
        _fingerprint(self.fingerprint)


@dataclass(frozen=True, slots=True, order=True)
class ProjectionEdgeFingerprint:
    edge_type: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    fingerprint: str
    count: int

    def __post_init__(self):
        _identity((self.edge_type, self.source_type, self.source_id, self.target_type, self.target_id))
        _fingerprint(self.fingerprint)
        if type(self.count) is not int or not 1 <= self.count <= 500_000:
            raise ValueError('projection_history_multiplicity_invalid')


@dataclass(frozen=True, slots=True)
class ProjectionNodeChange:
    before: ProjectionNodeFingerprint
    after: ProjectionNodeFingerprint


@dataclass(frozen=True, slots=True)
class ProjectionHistoryDelta:
    unchanged_nodes: tuple[ProjectionNodeFingerprint, ...]
    introduced_nodes: tuple[ProjectionNodeFingerprint, ...]
    removed_nodes: tuple[ProjectionNodeFingerprint, ...]
    changed_nodes: tuple[ProjectionNodeChange, ...]
    retained_edges: tuple[ProjectionEdgeFingerprint, ...]
    introduced_edges: tuple[ProjectionEdgeFingerprint, ...]
    removed_edges: tuple[ProjectionEdgeFingerprint, ...]


def compare_projection_history(*, before_nodes, after_nodes, before_edges, after_edges) -> ProjectionHistoryDelta:
    """Observe exact identities/hashes/multiplicity without approving any delta."""
    from okto_pulse.core.application.projection_history import compare
    return compare(before_nodes, after_nodes, before_edges, after_edges)
