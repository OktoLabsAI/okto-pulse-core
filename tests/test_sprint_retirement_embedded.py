import pytest
from dataclasses import asdict

from okto_pulse.core.domain.sprint_retirement_embedded import find_embedded_sprint_references
from okto_pulse.core.domain.quality_assessment import EvidenceRef


def test_typed_evidence_and_policy_receipt_keep_exact_locations_and_scope_hint():
    result = find_embedded_sprint_references({"evidence_refs": [
        {"source_type": "sprint", "source_id": "source", "source_version": 4, "content_hash": "a" * 64, "source_board_id": "board"},
        {"entity_type": "sprint", "subject_id": "past"},
    ]})
    assert [(r.path, r.sprint_id, r.board_hint) for r in result] == [
        (("evidence_refs", 0, "source_id"), "source", "board"),
        (("evidence_refs", 1, "subject_id"), "past", None),
    ]


def test_existing_quality_evidence_contract_admits_opaque_sprint_source():
    evidence = EvidenceRef(source_type="sprint", source_id="source", source_version=4, content_hash="a" * 64)
    reference, = find_embedded_sprint_references({"evidence_refs": [asdict(evidence)]})
    assert reference.sprint_id == "source" and reference.path == ("evidence_refs", 0, "source_id")


def test_reference_strings_are_scoped_to_reference_fields_not_prose():
    result = find_embedded_sprint_references({"title": "sprint:prose", "note": "mentioned sprint:note",
        "source_refs": ["sprint:source:17", "spec:spec"], "evidence_refs": ["sprint:past"]})
    assert [(r.path, r.sprint_id) for r in result] == [(("source_refs", 0), "source"), (("evidence_refs", 0), "past")]
    assert find_embedded_sprint_references("sprint:root") == ()
    assert find_embedded_sprint_references("sprint:root", field_name="source_ref")[0].sprint_id == "root"


def test_history_values_filters_and_migration_provenance_are_opaque_locations():
    result = find_embedded_sprint_references({"source_sprint_id": "source", "board_id": "board", "changes": [
        {"field": "sprint_id", "old_value": "old", "new_value": None},
        {"field": "sprint_id", "operator": "in", "value": ["first", "last"]},
    ]})
    assert [r.sprint_id for r in result] == ["source", "old", "first", "last"]
    assert result[-1].path == ("changes", 1, "value", 1)
    assert find_embedded_sprint_references({"sprint_id": None}) == ()


@pytest.mark.parametrize("value", [False, 0, "", " source ", [], {}])
def test_malformed_typed_identity_is_not_silently_ignored(value):
    with pytest.raises(ValueError, match="identity_invalid"):
        find_embedded_sprint_references({"source_type": "sprint", "source_id": value})


def test_missing_identity_and_malformed_board_hint_fail_closed():
    for value in ({"subject_type": "sprint"}, {"sprint_id": "source", "board_id": False}, {"source_ref": "sprint:"}):
        with pytest.raises(ValueError, match="identity_invalid"):
            find_embedded_sprint_references(value)


def test_limits_fail_before_a_late_reference_can_be_hidden():
    for value in ([None] * 5000 + [{"sprint_id": "late"}], {"field": "sprint_id", "value": ["id"] * 5001}):
        with pytest.raises(ValueError, match="structure_limit"):
            find_embedded_sprint_references(value)
    value = {"source_ref": "sprint:deep"}
    for _ in range(34):
        value = [value]
    with pytest.raises(ValueError, match="structure_limit"):
        find_embedded_sprint_references(value)
