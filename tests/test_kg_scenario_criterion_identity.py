"""Consolidated KG-01/02/03: IDs own scenario-to-criterion projection."""

from copy import deepcopy

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def project(criteria, links):
    return DeterministicWorker().process_spec({
        "id": "spec-identity", "board_id": "board-identity", "status": "done",
        "title": "Stable criterion identity", "acceptance_criteria": criteria,
        "test_scenarios": [{"id": "scenario-a", "title": "Scenario", "linked_criteria": links}],
    })


def _tests_relations(result):
    refs = {node.candidate_id: node.source_artifact_ref for node in result.nodes}
    return {(refs[edge.from_candidate_id], refs[edge.to_candidate_id])
            for edge in result.edges if edge.edge_type == "tests"}


def test_canonical_id_links_survive_criterion_reordering():
    criteria = [{"id": "ac-a", "text": "A condition"}, {"id": "ac-b", "text": "B condition"}]
    original = deepcopy(criteria)
    expected = {("spec:spec-identity:test_scenario:scenario-a", "spec:spec-identity:ac:ac-a")}
    assert _tests_relations(project(criteria, ["ac-a"])) == expected
    assert _tests_relations(project(list(reversed(criteria)), ["ac-a"])) == expected
    assert criteria == original


def test_legacy_duplicate_text_is_ambiguous_and_never_selects_a_target():
    result = project([{"id": "ac-a", "text": "Same condition"},
                      {"id": "ac-b", "text": "Same condition"}], ["Same condition"])
    assert _tests_relations(result) == set()
    missing = [item for item in result.missing_link_candidates if item.edge_type == "tests"]
    assert len(missing) == 1
    assert missing[0].reason == "ambiguous_criterion_match"
    assert len(missing[0].suggested_candidates) == 2


def test_legacy_unique_text_and_explicit_index_remain_supported():
    criteria = ["A condition", "B condition"]
    assert _tests_relations(project(criteria, ["A condition", 1])) == {
        ("spec:spec-identity:test_scenario:scenario-a", "spec:spec-identity:ac:0"),
        ("spec:spec-identity:test_scenario:scenario-a", "spec:spec-identity:ac:1"),
    }


def test_id_wins_over_colliding_text_and_numeric_legacy_index():
    criteria = [{"id": "other", "text": "1"}, {"id": "1", "text": "Chosen by ID"}]
    result = project(criteria, ["1"])
    assert _tests_relations(result) == {
        ("spec:spec-identity:test_scenario:scenario-a", "spec:spec-identity:ac:1"),
    }
    assert [edge.rule_id for edge in result.edges if edge.edge_type == "tests"] == ["tests/ac_match@v2.1"]


def test_ambiguous_numeric_text_does_not_fall_through_to_index():
    result = project(["1", "1"], ["1"])
    assert _tests_relations(result) == set()
    assert [item.reason for item in result.missing_link_candidates if item.edge_type == "tests"] == [
        "ambiguous_criterion_match",
    ]


def test_duplicate_ids_are_refused_even_when_text_has_a_unique_match():
    result = project([{"id": "ac-a", "text": "ac-a"},
                      {"id": "ac-a", "text": "Another condition"}], ["ac-a"])
    assert _tests_relations(result) == set()
    assert [item.reason for item in result.missing_link_candidates if item.edge_type == "tests"] == [
        "ambiguous_criterion_match",
    ]


def test_ambiguous_suggestions_are_bounded_and_unknown_ids_do_not_bind():
    result = project([{"id": f"ac-{index}", "text": "Repeated"} for index in range(10)],
                     ["Repeated", "ac-missing"])
    assert _tests_relations(result) == set()
    missing = [item for item in result.missing_link_candidates if item.edge_type == "tests"]
    assert [item.reason for item in missing] == ["ambiguous_criterion_match", "no_criterion_match"]
    assert len(missing[0].suggested_candidates) == 3


def test_unknown_canonical_id_does_not_bind_to_another_criterions_text():
    result = project([{"id": "ac_actual", "text": "ac_missing"}], ["ac_missing"])
    assert _tests_relations(result) == set()
    assert [item.reason for item in result.missing_link_candidates if item.edge_type == "tests"] == [
        "no_criterion_match",
    ]
