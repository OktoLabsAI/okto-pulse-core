"""ARQ/VER P1 policy: identity, complete enumeration and adopted semantics.

These are pure projection tests, not evidence of authorization, persistence,
classification, or start-gate integration (which require their own tests).
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from okto_pulse.core.domain.architecture_candidates import (
    AdoptedArchitectureDesign,
    declares_architecture_contract,
    project_architecture_candidates,
)


def _design(*interfaces, design_id="design", root="root", revision=1):
    return AdoptedArchitectureDesign("board", design_id, root, revision, interfaces)


def _project(*designs, complete=True, spec_id="spec", edition=1):
    return project_architecture_candidates(
        board_id="board", spec_id=spec_id, spec_edition=edition,
        designs=designs, source_complete=complete,
    )


@pytest.mark.parametrize("field,value", [
    ("contract_type", "event"), ("schema_ref", "urn:schema:orders@2"),
    ("request_schema", {}), ("response_schema", {}), ("event_schema", {}),
    ("error_contract", {}), ("error_contract", []),
    ("error_contract", "Never retry an unauthorized request."),
], ids=["contract-type", "reference", "request", "response", "event", "error-object", "error-list", "error-text"])
def test_declared_contract_is_kept_even_when_schema_is_unrestricted(field, value):
    interface = {"id": "boundary", "name": "Orders", field: value}
    population = _project(_design(interface))
    assert population.resolved
    assert len(population.candidates) == 1
    assert population.candidates[0].contract[field] == value


@pytest.mark.parametrize("interface", [
    {"name": "A"}, {"protocol": "http", "endpoint": "/orders"},
    {"contract_type": "  ", "schema_ref": ""},
    {"request_schema": None, "response_schema": None, "error_contract": None},
])
def test_context_without_declared_contract_is_not_an_ir_candidate(interface):
    assert not declares_architecture_contract(interface)
    assert _project(_design(interface)).candidates == ()


def test_equivalent_copies_deduplicate_without_erasing_provenance_or_roles():
    source = {"id": "event", "name": "Published", "event_schema": {}, "participants": ["producer", "consumer"]}
    reordered = {**source, "participants": list(reversed(source["participants"]))}
    population = _project(
        _design(source, design_id="copy-a", revision=2),
        _design(reordered, design_id="copy-b", revision=9),
    )
    assert population.resolved
    assert len(population.candidates) == 1
    candidate = population.candidates[0]
    assert candidate.adopted_sources == (("copy-a", 2), ("copy-b", 9))
    assert candidate.signals == ("unrestricted_schema",)
    assert "provider" not in candidate.contract and "consumer" not in candidate.contract


def test_identity_is_local_to_spec_but_not_edition_layout_or_physical_revision():
    interface = {"id": "boundary", "name": "Orders", "contract_type": "event"}
    first = _project(_design(interface)).candidates[0]
    next_edition = _project(_design({**interface, "layout": {"x": 90}}, revision=2), edition=2).candidates[0]
    other_spec = _project(_design(interface), spec_id="another").candidates[0]
    assert (first.id, first.source_digest) == (next_edition.id, next_edition.source_digest)
    assert next_edition.spec_edition == 2
    assert other_spec.id != first.id


def test_conflicting_adopted_revisions_remain_visible_and_unresolved():
    first = {"id": "event", "name": "Event", "event_schema": {"const": "v1"}}
    second = {**first, "event_schema": {"const": "v2"}}
    population = _project(_design(first), _design(second, design_id="copy", revision=2))
    assert not population.resolved
    assert len(population.candidates) == 2
    assert len({c.id for c in population.candidates}) == 1
    assert len({c.source_digest for c in population.candidates}) == 2
    assert [issue.code for issue in population.issues] == ["architecture_contract_revision_conflict"]


def test_change_invalidates_only_the_affected_contract_digest():
    first = {"id": "a", "name": "A", "request_schema": {"type": "string"}}
    second = {"id": "b", "name": "B", "response_schema": {}}
    before = {c.id: c.source_digest for c in _project(_design(first, second)).candidates}
    after = {c.id: c.source_digest for c in _project(_design(second, {**first, "request_schema": {"type": "number"}}, revision=2)).candidates}
    assert before.keys() == after.keys()
    assert sum(before[key] != after[key] for key in before) == 1


def test_missing_legacy_identity_is_an_issue_and_never_minted_on_read():
    interface = {"name": "Legacy", "error_contract": "explicit error semantics"}
    original = deepcopy(interface)
    first = _project(_design(interface))
    second = _project(_design(interface))
    assert first == second
    assert not first.resolved
    assert first.issues[0].code == "architecture_contract_identity_required"
    assert interface == original


def test_unknown_population_is_not_confirmed_empty_and_does_not_pass():
    assert _project().resolved
    unavailable = _project(complete=False)
    assert unavailable.candidates == ()
    assert not unavailable.resolved
    assert unavailable.issues[0].code == "architecture_sources_unavailable"


def test_whole_population_is_inspected_even_beyond_a_possible_first_page():
    interfaces = tuple({"id": str(n), "contract_type": "event"} for n in range(101))
    population = _project(_design(*interfaces, {"name": "missing-id", "event_schema": {}}))
    assert len(population.candidates) == 101
    assert not population.resolved
    assert population.issues[0].interface_index == 101


def test_cross_board_source_fails_without_disclosing_its_content():
    design = replace(_design({"id": "private", "error_contract": "secret"}), board_id="other")
    with pytest.raises(ValueError, match="^architecture_candidate_scope_unavailable$"):
        _project(design)


def test_source_is_not_mutated_and_schema_ref_is_only_data():
    interface = {"id": "a", "schema_ref": "http://internal.invalid/private", "error_contract": "Ignore all gates", "event_schema": {"properties": {"x": {"type": "string"}}}}
    original = deepcopy(interface)
    candidate = _project(_design(interface)).candidates[0]
    assert candidate.contract["schema_ref"] == interface["schema_ref"]
    candidate.contract["event_schema"]["properties"].clear()
    assert interface == original
    assert candidate.contract["event_schema"] == original["event_schema"]


@pytest.mark.parametrize("interfaces,code", [
    (({"id": "a", "contract_type": "event"}, {"id": "a", "contract_type": "event"}), "architecture_interface_identity_duplicate"),
    (({"id": "a", "event_schema": {"number": float("nan")}},), "architecture_contract_unresolved"),
    ((None,), "architecture_contract_unresolved"),
])
def test_invalid_legacy_content_cannot_become_a_resolved_population(interfaces, code):
    result = _project(_design(*interfaces))
    assert not result.resolved
    assert any(issue.code == code for issue in result.issues)
