"""Tests for api/analytics.py coverage helpers — regression for AC double-counting bug.

Root cause: `test_scenarios[*].linked_criteria` may store entries as `int`, numeric
`str` index, or full AC text. The previous aggregator used a set over raw values,
producing `covered_ac > total_ac` when multiple shapes coexisted in one spec.

Fix: `_resolve_linked_criteria_to_indices` normalizes all shapes to 0-based int
indices before dedup. Out-of-range and unmatched texts are dropped.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.api.analytics import _resolve_linked_criteria_to_indices


AC_LIST = [
    "AC1 - first criterion text",
    "AC2 - second criterion",
    "AC3 - third",
    "AC4 - fourth",
    "AC5 - fifth",
    "AC6 - sixth",
    "AC7 - seventh",
    "AC8 - eighth",
    "AC9 - ninth",
    "AC10 - tenth",
]


class TestResolveLinkedCriteriaAllInt:
    def test_resolves_int_indices(self):
        assert _resolve_linked_criteria_to_indices([0, 2, 5], AC_LIST) == {0, 2, 5}

    def test_empty_list_returns_empty_set(self):
        assert _resolve_linked_criteria_to_indices([], AC_LIST) == set()

    def test_none_returns_empty_set(self):
        assert _resolve_linked_criteria_to_indices(None, AC_LIST) == set()


class TestResolveLinkedCriteriaAllStrIdx:
    def test_resolves_str_numeric_indices(self):
        assert _resolve_linked_criteria_to_indices(["0", "2", "5"], AC_LIST) == {0, 2, 5}

    def test_ignores_out_of_range_str_indices(self):
        assert _resolve_linked_criteria_to_indices(["0", "99", "100"], AC_LIST) == {0}

    def test_strips_whitespace(self):
        assert _resolve_linked_criteria_to_indices(["  1 ", "\t2"], AC_LIST) == {1, 2}


class TestResolveLinkedCriteriaAllText:
    def test_resolves_full_ac_text(self):
        assert _resolve_linked_criteria_to_indices(
            [AC_LIST[0], AC_LIST[2], AC_LIST[5]], AC_LIST
        ) == {0, 2, 5}

    def test_resolves_text_prefix_match(self):
        # AC text may start with the linked token (or vice versa)
        assert _resolve_linked_criteria_to_indices(["AC3"], AC_LIST) == {2}

    def test_unmatched_text_dropped(self):
        assert _resolve_linked_criteria_to_indices(["completely unknown"], AC_LIST) == set()


class TestResolveLinkedCriteriaMixed:
    def test_mixed_int_str_and_text_dedups(self):
        # index 2 referenced as int, str "2", AND full text — all resolve to {2}.
        # Plus index 0 and index 5 referenced by distinct shapes.
        entries = [0, "2", AC_LIST[2], "5", AC_LIST[5]]
        assert _resolve_linked_criteria_to_indices(entries, AC_LIST) == {0, 2, 5}

    def test_observed_r1_pattern(self):
        """Reproduces the R1 spec (a3023ed2) shape that caused covered_ac=20 total_ac=10.

        TS1-TS9 stored indices as numeric strings; ts_d8242d27 stored full AC texts.
        Combined, the unfixed aggregator counted each AC twice.
        """
        # TS1..TS9 → str-idx, ts_d8242d27 → full texts
        str_idx_entries = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        text_entries = list(AC_LIST)
        combined = str_idx_entries + text_entries
        resolved = _resolve_linked_criteria_to_indices(combined, AC_LIST)
        assert resolved == set(range(10))
        assert len(resolved) == 10  # NOT 20


class TestResolveLinkedCriteriaDegenerate:
    def test_ignores_booleans(self):
        # bool subclasses int; must not be coerced into index 1
        assert _resolve_linked_criteria_to_indices([True, False], AC_LIST) == set()

    def test_empty_string_ignored(self):
        assert _resolve_linked_criteria_to_indices(["", " ", "   "], AC_LIST) == set()

    def test_negative_indices_ignored(self):
        assert _resolve_linked_criteria_to_indices([-1, "-1"], AC_LIST) == set()

    def test_empty_ac_list_returns_empty(self):
        assert _resolve_linked_criteria_to_indices([0, "1", "text"], []) == set()

    def test_single_ac_many_duplicate_refs(self):
        """Degenerate input with more refs than ACs must never exceed total_ac."""
        entries = [0, "0", AC_LIST[0]]  # same AC referenced 3 ways
        resolved = _resolve_linked_criteria_to_indices(entries, AC_LIST)
        assert resolved == {0}
        assert len(resolved) <= len(AC_LIST)


class TestInvariantHolds:
    def test_covered_never_exceeds_total(self):
        """Main invariant: len(resolved) <= len(ac_list), ALWAYS."""
        # Throw everything at it — int, str-idx, text, out-of-range, dupes.
        entries = (
            [0, 1, 2, 3, 4]
            + ["0", "1", "2", "99", "100"]
            + list(AC_LIST)
            + ["bogus", True, -1, None]
            + [" 3 "]
        )
        # Filter None since linked_criteria in practice never has None, but helper
        # must still produce a valid set without crashing
        entries_safe = [e for e in entries if e is not None]
        resolved = _resolve_linked_criteria_to_indices(entries_safe, AC_LIST)
        assert len(resolved) <= len(AC_LIST)
        assert all(0 <= i < len(AC_LIST) for i in resolved)
