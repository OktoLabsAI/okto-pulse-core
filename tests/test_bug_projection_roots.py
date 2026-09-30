"""The offline observer must preserve the writer's typed Bug identity."""

from dataclasses import replace

import pytest

from okto_pulse.core.ports.projection_history import (
    ProjectionSourceRoot, ProjectionSourceIdentity, select_projection_source_roots,
)


@pytest.mark.parametrize('alias', ['bug:id', 'card:id', 'card:bug:id'])
@pytest.mark.parametrize('marker', [None, ''])
def test_current_bug_root_preserves_every_active_whole_alias(alias, marker):
    root = ProjectionSourceRoot('Bug', 'card:id')
    current = ProjectionSourceIdentity('Bug', 'original', alias, 0, marker)
    old = replace(current, node_id='superseded', superseded_by='original')
    assert select_projection_source_roots(roots=(root,), nodes=(old, current)) == (current,)


def test_current_bug_root_never_selects_an_arbitrary_active_duplicate():
    root = ProjectionSourceRoot('Bug', 'card:id')
    first = ProjectionSourceIdentity('Bug', 'original', 'card:id', 0, None)
    other = replace(first, node_id='duplicate', generation=2)
    with pytest.raises(ValueError, match='ambiguous'):
        select_projection_source_roots(roots=(root,), nodes=(first, other))


@pytest.mark.parametrize('kind,ref', [('Entity', 'card:id'), ('Bug', 'bug:id-other'),
    ('Bug', 'bug:id:learning:x')])
def test_root_alias_does_not_match_other_types_prefixes_or_concepts(kind, ref):
    with pytest.raises(ValueError, match='current_root_missing'):
        select_projection_source_roots(roots=(ProjectionSourceRoot('Bug', 'card:id'),),
            nodes=(ProjectionSourceIdentity(kind, 'wrong', ref, 0, None),))


def test_two_aliases_cannot_declare_two_distinct_source_roots_for_the_same_bug():
    with pytest.raises(ValueError, match='duplicate_root'):
        select_projection_source_roots(roots=(ProjectionSourceRoot('Bug', 'card:id'),
            ProjectionSourceRoot('Bug', 'bug:id')), nodes=())
