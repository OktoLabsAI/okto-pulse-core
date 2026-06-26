"""RKG-02 — shared canonical cognitive source_ref resolver (unit).

Covers TR3 regression refs + AC1/AC2/AC3:
  * AC1 / ts_15a04817 — canonical bug: bug:/card:/card:bug: resolve to the same
    artifact and are bug-derived.
  * AC2 / ts_7be24883 — a card that is NOT a canonical bug, referenced as
    card:<uuid>, yields an actionable non_bug_card status, not a Bug alias.
  * AC3 / ts_8f219c28 — spec Alternative/Assumption child refs + final_report
    allowlist preserve the original ref and base, no workaround/fabrication.
"""

from __future__ import annotations

from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    CognitiveRefResolutionStatus as St,
    CognitiveSourceKind as Kind,
    resolve_cognitive_source_ref,
    strip_concept_suffix,
)

U = "11111111-1111-1111-1111-111111111111"


def _bug_probe(known_bugs):
    return lambda uuid: uuid in known_bugs


# ---------------------------------------------------------------------------
# AC1 — canonical bug equivalence (bug: / card: / card:bug:)
# ---------------------------------------------------------------------------


def test_ac1_canonical_bug_aliases_resolve_equivalently():
    probe = _bug_probe({U})
    refs = [f"bug:{U}", f"card:{U}", f"card:bug:{U}"]
    resolutions = [resolve_cognitive_source_ref(r, canonical_bug_probe=probe) for r in refs]
    # All bug-derived, same canonical artifact identity.
    assert all(res.is_bug_derived for res in resolutions)
    assert all(res.source_kind == Kind.BUG.value for res in resolutions)
    assert len({res.canonical_artifact_ref for res in resolutions}) == 1
    assert resolutions[0].canonical_artifact_ref == f"card:{U}"
    # source_ref_original preserved verbatim for audit.
    assert resolutions[1].source_ref_original == f"card:{U}"


def test_bug_form_is_bug_derived_without_probe():
    res = resolve_cognitive_source_ref(f"bug:{U}")
    assert res.is_bug_derived is True
    assert res.resolution_status == St.RESOLVED.value


# ---------------------------------------------------------------------------
# AC2 — card that is NOT a canonical bug
# ---------------------------------------------------------------------------


def test_ac2_non_bug_card_is_actionable_not_bug_alias():
    probe = _bug_probe(set())  # no canonical bugs
    res = resolve_cognitive_source_ref(f"card:{U}", canonical_bug_probe=probe)
    assert res.is_bug_derived is False
    assert res.source_kind == Kind.CARD.value
    assert res.resolution_status == St.NON_BUG_CARD.value
    assert res.remediation  # actionable
    assert res.canonical_artifact_ref == f"card:{U}"


def test_card_without_probe_resolves_non_bug_no_failure():
    res = resolve_cognitive_source_ref(f"card:{U}")
    assert res.is_bug_derived is False
    assert res.resolution_status == St.RESOLVED.value
    assert res.source_kind == Kind.CARD.value


# ---------------------------------------------------------------------------
# AC3 — spec child refs + final_report allowlist
# ---------------------------------------------------------------------------


def test_ac3_spec_alternative_child_ref_preserves_base_and_original():
    ref = "spec:s1:alternative:deadbeef"
    res = resolve_cognitive_source_ref(ref)
    assert res.source_kind == Kind.CHILD.value
    assert res.base_ref == "spec:s1"
    assert res.canonical_artifact_ref == "spec:s1"
    assert res.source_ref_original == ref
    assert res.is_bug_derived is False
    assert res.resolution_status == St.RESOLVED.value


def test_ac3_spec_assumption_child_ref():
    res = resolve_cognitive_source_ref("spec:s2:assumption:abcd1234")
    assert res.source_kind == Kind.CHILD.value
    assert res.base_ref == "spec:s2"


def test_ac3_final_report_allowlisted():
    res = resolve_cognitive_source_ref("final_report:fr-1")
    assert res.source_kind == Kind.FINAL_REPORT.value
    assert res.resolution_status == St.FINAL_REPORT_ALLOWLISTED.value
    assert res.is_bug_derived is False


# ---------------------------------------------------------------------------
# TR3 — remaining ref classes
# ---------------------------------------------------------------------------


def test_plain_spec_ref():
    res = resolve_cognitive_source_ref("spec:s1")
    assert res.source_kind == Kind.SPEC.value
    assert res.resolution_status == St.RESOLVED.value


def test_invalid_source_ref_no_colon():
    res = resolve_cognitive_source_ref("not-a-ref")
    assert res.resolution_status == St.INVALID_SOURCE_REF.value
    assert res.is_bug_derived is False


def test_unsupported_type_is_unresolved():
    res = resolve_cognitive_source_ref("widget:123")
    assert res.resolution_status == St.UNRESOLVED_SOURCE_REF.value


def test_empty_and_none_are_invalid():
    assert resolve_cognitive_source_ref("").resolution_status == St.INVALID_SOURCE_REF.value
    assert resolve_cognitive_source_ref(None).resolution_status == St.INVALID_SOURCE_REF.value


def test_strip_concept_suffix_helper():
    assert strip_concept_suffix("spec:s1:alternative:x") == "spec:s1"
    assert strip_concept_suffix("card:bug:abc:learning:y") == "card:bug:abc"
    assert strip_concept_suffix("bug:abc") == "bug:abc"


def test_card_bug_form_bug_derived_even_without_probe():
    # card:bug:<uuid> is explicitly bug-derived regardless of probe.
    res = resolve_cognitive_source_ref(f"card:bug:{U}")
    assert res.is_bug_derived is True
    assert res.source_kind == Kind.BUG.value
