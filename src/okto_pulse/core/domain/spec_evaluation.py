"""Edition currentness for the existing decomposition-review ledger.

Legacy rows retain their prior verdict until an authorized reopen. No backfill
invents their original edition, and an approval never supersedes a rejection
inside the same edition.
"""
from collections.abc import Mapping


def spec_evaluation_is_current(evaluation: Mapping, edition: int) -> bool:
    if evaluation.get('stale'):
        return False
    recorded = evaluation.get('spec_edition')
    if recorded is None:
        return True  # Preserved legacy compatibility, ended explicitly on reopen.
    return type(recorded) is int and recorded == edition


def previous_spec_evaluations(evaluations: list, *, reopened_in_edition: int) -> list:
    """Change lifecycle metadata only; retain every authored historical field."""
    return [
        {**item, 'stale': True, 'stale_reason': 'spec_reopened',
         'stale_in_edition': reopened_in_edition}
        if not item.get('stale') else dict(item)
        for item in evaluations
    ]


def project_spec_evaluation(evaluation: Mapping, edition: int) -> dict:
    current = spec_evaluation_is_current(evaluation, edition)
    return {**evaluation, 'is_current': current,
            'lifecycle_state': 'current' if current else 'previous',
            'edition_origin': 'recorded' if evaluation.get('spec_edition') is not None else 'legacy_unknown'}
