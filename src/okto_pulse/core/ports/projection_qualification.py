"""Composition of current-source observations over authenticated history."""

from dataclasses import dataclass

from .projection_history import ProjectionSourceIdentity


SOURCE_OBSERVATION_FIELDS = ('source_created_at', 'source_updated_at', 'source_status', 'severity',
    'resolved_at', 'graph_layer', 'maturity_status')


@dataclass(frozen=True, slots=True)
class ProjectionSourceObservation:
    identity: ProjectionSourceIdentity
    expected: tuple
    observed: tuple

    def __post_init__(self):
        if type(self.identity) is not ProjectionSourceIdentity:
            raise TypeError('projection_qualification_source_identity_required')
        for values in (self.expected, self.observed):
            if type(values) is not tuple or len(values) != len(SOURCE_OBSERVATION_FIELDS):
                raise ValueError('projection_qualification_source_fields_invalid')
            if any(value is not None and type(value) is not (int if index in (0, 1, 4) else str)
                    for index, value in enumerate(values)):
                raise ValueError('projection_qualification_source_fields_invalid')
        if self.expected[5] not in ('canonical', 'working') or not self.expected[6]:
            raise ValueError('projection_qualification_source_partition_invalid')


@dataclass(frozen=True, slots=True)
class ProjectionHistoryQualification:
    state: str
    current_source_node_count: int
    unclassified_node_count: int
    current_relation_count: int
    unclassified_relation_count: int
    reasons: tuple[str, ...]


def qualify_projection_history(*, history, sources, relations):
    """Qualify only history covered by the fenced current-source projection.

    The caller authenticates the complete history/property effects and obtains
    expected fields from the revalidated Core plan, never a client assertion.
    This describes a private projection. It grants no read, mutation, cognitive
    authority or runtime admission and does not classify Global history.
    """
    from okto_pulse.core.application.projection_qualification import qualify
    return qualify(history=history, sources=sources, relations=relations)
