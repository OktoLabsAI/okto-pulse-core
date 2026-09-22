"""Typed observations between authenticated graph inventories, never authority.

Node identity can establish a changed record. Relations have no portable row ID:
changing their properties is an observed removal/addition, not an invented update
pairing. The caller must independently prove both inventories complete and bound
to the intended snapshots before using these observations for reconciliation.
"""

from dataclasses import dataclass
from enum import Enum
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


class ProjectionHistoryState(str, Enum):
    NO_PRIOR_RECORDS = 'no_prior_records'
    PRESERVED_UNCLASSIFIED = 'preserved_unclassified'
    PRIOR_CHANGES_UNCLASSIFIED = 'prior_changes_unclassified'


def classify_projection_history(delta: ProjectionHistoryDelta) -> ProjectionHistoryState:
    """Classify literal preservation only; no state authorizes runtime admission."""
    if type(delta) is not ProjectionHistoryDelta:
        raise TypeError('projection_history_delta_required')
    if delta.removed_nodes or delta.changed_nodes or delta.removed_edges:
        return ProjectionHistoryState.PRIOR_CHANGES_UNCLASSIFIED
    if delta.unchanged_nodes or delta.retained_edges:
        return ProjectionHistoryState.PRESERVED_UNCLASSIFIED
    return ProjectionHistoryState.NO_PRIOR_RECORDS


@dataclass(frozen=True, slots=True, order=True)
class ProjectionSourceRoot:
    node_type: str
    source_artifact_ref: str

    def __post_init__(self):
        _identity((self.node_type, self.source_artifact_ref))


@dataclass(frozen=True, slots=True)
class ProjectionSourceIdentity:
    node_type: str
    node_id: str
    source_artifact_ref: str
    generation: int | None
    superseded_by: str | None

    def __post_init__(self):
        _identity((self.node_type, self.node_id, self.source_artifact_ref))
        if self.generation is not None and (type(self.generation) is not int or self.generation < 0):
            raise ValueError('projection_history_generation_invalid')
        if self.superseded_by is not None:
            _identity((self.superseded_by,))


def select_projection_source_roots(
    *, roots: tuple[ProjectionSourceRoot, ...], nodes: tuple[ProjectionSourceIdentity, ...],
) -> tuple[ProjectionSourceIdentity, ...]:
    """Resolve current identities with the consolidator's existing ordering.

    Selection does not establish source authority, classify other generations,
    or approve any historical change. Callers still prove the complete census.
    """
    from okto_pulse.core.application.projection_history import select_source_roots
    return select_source_roots(roots, nodes)


def compare_projection_history(*, before_nodes, after_nodes, before_edges, after_edges) -> ProjectionHistoryDelta:
    """Observe exact identities/hashes/multiplicity without approving any delta."""
    from okto_pulse.core.application.projection_history import compare
    return compare(before_nodes, after_nodes, before_edges, after_edges)


def is_projection_technical_root(*, node_type, source_artifact_ref, created_by_agent, source_session_id):
    """Apply the existing orphan scanner's root policy to pinned observations.

    This is a connectivity classification only. It cannot establish source
    authority, approve historical changes, or authorize runtime admission.
    """
    from okto_pulse.core.application.projection_history import is_technical_root
    return is_technical_root(node_type, source_artifact_ref, created_by_agent, source_session_id)
