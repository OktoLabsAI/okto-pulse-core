"""Coverage deduplicates exact current IDs and never counts old aliases."""

import pytest
from okto_pulse.community.api.analytics import _resolve_linked_criteria_to_indices

AC_LIST = [{"id": f"ac_{index}", "text": f"Criterion {index}"} for index in range(10)]

@pytest.mark.parametrize("refs", [None, [], [0, 2, 5], ["0", "2", "5"], ["Criterion 0"], ["Criterion"], [True, False]])
def test_non_identity_references_have_no_coverage(refs):
    assert _resolve_linked_criteria_to_indices(refs, AC_LIST) == set()

def test_current_ids_resolve_to_display_positions():
    assert _resolve_linked_criteria_to_indices(["ac_0", "ac_2", "ac_5"], AC_LIST) == {0, 2, 5}

def test_repeated_ids_across_scenarios_do_not_double_count():
    scenarios = [[f"ac_{i}" for i in range(10)], [f"ac_{i}" for i in range(10)] * 2]
    covered = set()
    for refs in scenarios:
        covered |= _resolve_linked_criteria_to_indices(refs, AC_LIST)
    assert covered == set(range(10))
    assert len(covered) == len(AC_LIST)

def test_unresolved_and_old_aliases_cannot_inflate_coverage():
    refs = ["ac_2", "ac_2", "2", 2, "Criterion 2", "Criterion", "missing"]
    assert _resolve_linked_criteria_to_indices(refs, AC_LIST) == {2}

def test_duplicate_identity_is_not_silently_assigned():
    assert _resolve_linked_criteria_to_indices(["duplicate"], [
        {"id": "duplicate", "text": "First"}, {"id": "duplicate", "text": "Second"},
    ]) == set()

def test_same_text_keeps_distinct_identities():
    rows = [{"id": "first", "text": "Same"}, {"id": "second", "text": "Same"}]
    assert _resolve_linked_criteria_to_indices(["first"], rows) == {0}
    assert _resolve_linked_criteria_to_indices(["second"], rows) == {1}
    assert _resolve_linked_criteria_to_indices(["Same"], rows) == set()
