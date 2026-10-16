"""A derived association is current classification, never delivery evidence."""
from dataclasses import replace

import pytest

from okto_pulse.core.domain.architecture_projection import resolve_architecture_associations
from test_architecture_classification_review import sources, interface, decision


def project(population, records=(), irs=()):
    return resolve_architecture_associations(
        board_id="board", spec_id="spec", spec_edition=2, spec_version=9,
        population=population, decisions=records, integration_requirements=irs,
    )


def associated(candidate, **changes):
    return decision(candidate, disposition="associate_existing_ir",
                    integration_requirement_ids=("ir",), reason=None, **changes)


@pytest.mark.parametrize("state", ["pending", "context", "retired", "changed", "missing-ir", "removed-ir", "ambiguous-ir"])
def test_noncurrent_associations_contribute_no_edges(state):
    population = sources(interface())
    records = (associated(population.candidates[0]),)
    irs = ({"id": "ir", "status": "active"},)
    if state == "pending":
        records = ()
    elif state == "context":
        records = (decision(population.candidates[0]),)
    elif state == "retired":
        population = sources()
    elif state == "changed":
        population = sources(interface(error_contract={"retry": False}))
    elif state == "missing-ir":
        irs = ()
    elif state == "removed-ir":
        irs = ({"id": "ir", "status": "removed"},)
    elif state == "ambiguous-ir":
        irs = irs * 2
    assert project(population, records, irs) == ()


@pytest.mark.parametrize("area", ["publish", "consume", "remainder"])
def test_partial_classification_keeps_only_current_fragments(area):
    candidate = sources(interface()).candidates[0]
    records = tuple(associated(candidate, id=part, scope_paths=(f"/event_schema/{part}",),
                               remainder_reason="Other context") for part in ("publish", "consume"))
    changed = interface()
    if area == "remainder":
        changed["error_contract"]["retry"] = False
    else:
        changed["event_schema"][area] = {"required": ["id"]}
    actual = project(sources(changed), records, ({"id": "ir", "status": "active"},))
    assert {item.decision_id for item in actual} == {"publish", "consume"} - {area}
    assert {item.design_id for item in actual} == {"adopted"}


@pytest.mark.parametrize("fault", ["incomplete", "foreign-spec", "foreign-edition", "corrupt-contract"])
def test_unsafe_source_cannot_authorize_empty_replacement(fault):
    population = sources(interface())
    record = associated(population.candidates[0])
    if fault == "incomplete":
        population = replace(population, source_complete=False)
    else:
        record = replace(record, **{
            "foreign-spec": {"spec_id": "other"},
            "foreign-edition": {"spec_edition": 1},
            "corrupt-contract": {"source_contract_json": "{}"},
        }[fault])
    with pytest.raises(ValueError, match="architecture_projection_"):
        project(population, (record,), ({"id": "ir", "status": "active"},))


@pytest.mark.parametrize("change", [
    {"owner_id": "other"}, {"target_ref": "spec:spec:fr:ir"},
    {"source_ref": "architecture_design:design:interface:"},
    {"source_type": "Entity"}, {"target_type": "Constraint"},
])
def test_cleanup_owns_only_the_exact_spec_ir_and_contract(change):
    from okto_pulse.core.ports.spec_projection import spec_relationship_family
    family = spec_relationship_family("architecture_associations")
    endpoints = dict(owner_id="spec", source_type="APIContract", target_type="Requirement",
                     source_ref="architecture_design:design:interface:boundary",
                     target_ref="spec:spec:integration_requirement:ir")
    assert family.owner_endpoint == "target"
    assert family.owns_endpoints(**endpoints)
    assert not family.owns_endpoints(**(endpoints | change))
    writer = dict(rule_id="implements/architecture_association@v1", layer="deterministic",
                  created_by="worker_layer1")
    assert family.owns_writer(**writer)
    for changed in ({"layer": "cognitive"}, {"created_by": "other"}, {"rule_id": "implements/other@v1"}):
        assert not family.owns_writer(**(writer | changed))
