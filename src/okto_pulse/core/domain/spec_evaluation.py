"""Edition currentness for native decomposition reviews.

An approval never supersedes a rejection inside the same edition.
"""
from collections.abc import Mapping


def _recorded_edition(evaluation: Mapping) -> int:
    recorded = evaluation.get('spec_edition')
    if type(recorded) is not int or recorded < 1:
        raise ValueError('spec_evaluation_edition_required')
    return recorded


def spec_evaluation_is_current(evaluation: Mapping, edition: int) -> bool:
    recorded = _recorded_edition(evaluation)
    if evaluation.get('stale'):
        return False
    return recorded == edition


def previous_spec_evaluations(evaluations: list, *, reopened_in_edition: int) -> list:
    """Change lifecycle metadata only; retain every authored historical field."""
    for item in evaluations:
        _recorded_edition(item)
    return [
        {**item, 'stale': True, 'stale_reason': 'spec_reopened',
         'stale_in_edition': reopened_in_edition}
        if not item.get('stale') else dict(item)
        for item in evaluations
    ]


def project_spec_evaluation(evaluation: Mapping, edition: int) -> dict:
    current = spec_evaluation_is_current(evaluation, edition)
    return {**evaluation, 'is_current': current,
            'lifecycle_state': 'current' if current else 'previous'}
