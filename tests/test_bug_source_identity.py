"""Whole Bug aliases must never mint a second identity or hide ambiguity."""

import pytest

from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    bug_source_reference_aliases,
    resolve_cognitive_source_ref,
    typed_bug_source_reference_aliases,
)
from okto_pulse.core.kg.primitives import _lookup_existing_node, _resolve_endpoint


@pytest.mark.parametrize('bug_id', ['bug-context', 'a974936c-89cf-4c3e-a1e6-b1f085795747'])
def test_typed_aliases_share_exact_identity_without_changing_generic_parser(bug_id):
    aliases = bug_source_reference_aliases(bug_id)
    for source_ref in aliases:
        assert typed_bug_source_reference_aliases(source_ref) == aliases
    assert resolve_cognitive_source_ref('card:bug-context').resolution_status == 'invalid_source_ref'


@pytest.mark.parametrize('source_ref', ['', 'spec:id', 'bug:', 'bug:id:learning:x',
    'card:bug:id:decision:x', 'bug: id', 'bug:id '])
def test_typed_aliases_never_coerce_concepts_or_other_sources(source_ref):
    with pytest.raises(ValueError, match='bug_source_identity_invalid'):
        typed_bug_source_reference_aliases(source_ref)


class Scope:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def find_active_node_ids_by_source_refs(self, node_type, source_refs):
        self.calls.append((node_type, source_refs))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.parametrize('ref', ['bug:id', 'card:id', 'card:bug:id'])
def test_lookup_reuses_existing_identity_for_every_whole_bug_alias(ref):
    scope = Scope(('historical-identity',))
    assert _lookup_existing_node(scope, 'Bug', ref) == 'historical-identity'
    assert scope.calls == [('Bug', bug_source_reference_aliases('id'))]


def test_absence_is_distinct_from_ambiguity_and_unavailable_lookup():
    assert _lookup_existing_node(Scope(()), 'Bug', 'card:id') is None
    with pytest.raises(ValueError, match='canonical_bug_identity_ambiguous'):
        _lookup_existing_node(Scope(('old', 'duplicate')), 'Bug', 'card:id')
    failure = RuntimeError('reader unavailable')
    with pytest.raises(RuntimeError) as caught:
        _lookup_existing_node(Scope(failure), 'Bug', 'card:id')
    assert caught.value is failure


@pytest.mark.parametrize('ref', ['bug:id', 'card:id', 'card:bug:id'])
def test_external_bug_endpoint_uses_the_same_unique_active_identity(ref):
    scope = Scope(('historical-identity',))
    assert _resolve_endpoint('kgref:Bug:' + ref, {}, graph_scope=scope) == ('historical-identity', 'Bug')
    assert scope.calls == [('Bug', bug_source_reference_aliases('id'))]
    assert _resolve_endpoint('kgref:Bug:' + ref, {}, graph_scope=Scope(())) == (None, 'Bug')
    with pytest.raises(ValueError, match='canonical_bug_identity_ambiguous'):
        _resolve_endpoint('kgref:Bug:' + ref, {}, graph_scope=Scope(('a', 'b')))
    with pytest.raises(RuntimeError, match='unavailable'):
        _resolve_endpoint('kgref:Bug:' + ref, {}, graph_scope=Scope(RuntimeError('unavailable')))
