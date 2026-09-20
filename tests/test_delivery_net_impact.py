from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_impact import DeliveryImpactClaim, compose_delivery_impact
from okto_pulse.core.domain.delivery_progress import DeliveryProgress
from okto_pulse.core.models.schemas import ImpactEvidence

A, B, C, D = (letter * 40 for letter in "abcd")


def claim(identity, base, result, *, source="source", files=(), **sections):
    return DeliveryImpactClaim(identity, source, base, result, ImpactEvidence(files=list(files), **sections))


def file(action, path="a.py", *, repo="core", previous=None):
    return dict(repo=repo, path=path, change_kind=action, **({"previous_path": previous} if previous else {}))


@pytest.mark.parametrize("first,second,expected", [
    (file("created"), file("deleted"), []),
    (file("created"), file("modified"), [file("created")]),
    (file("modified"), file("deleted"), [file("deleted")]),
    (file("renamed", "b.py", previous="a.py"), file("renamed", "c.py", previous="b.py"), [file("renamed", "c.py", previous="a.py")]),
    (file("created"), file("renamed", "b.py", previous="a.py"), [file("created", "b.py")]),
    (file("renamed", "b.py", previous="a.py"), file("renamed", "a.py", previous="b.py"), []),
])
def test_ordered_net_effect_keeps_all_history(first, second, expected):
    history = (claim("one", A, B, files=[first]), claim("two", B, C, files=[second]))
    result = compose_delivery_impact(history)
    assert result == compose_delivery_impact(tuple(reversed(history)))
    assert result["status"] == "composed" and result["claim_only"] is True
    assert result["history_count"] == 2
    assert result["sources"][0]["record_ids"] == ["one", "two"]
    assert result["sources"][0]["impact_evidence"]["files"] == expected


@pytest.mark.parametrize("history,code", [
    ((claim("one", None, B, files=[file("created")]),), "revision_unknown"),
    ((claim("one", A, B, source=None, files=[file("created")]),), "source_unknown"),
    ((claim("one", A, A, files=[file("modified")]),), "unchanged_revision_with_delta"),
    ((claim("one", A, B), claim("two", A, C)), "revision_chain_ambiguous"),
    ((claim("one", A, B), claim("two", C, D)), "revision_chain_ambiguous"),
    ((claim("one", A, B), claim("two", B, A)), "revision_chain_ambiguous"),
    ((claim("one", A, B, files=[file("deleted")]), claim("two", B, C, files=[file("created")])), "path_recreated_or_conflicting"),
    ((claim("one", A, B, files=[file("created"), file("deleted")]),), "same_revision_conflicting_claims"),
    ((claim("one", A, B, files=[file("renamed", "b.py", previous="a.py"), file("renamed", "c.py", previous="b.py")]),), "same_revision_path_overlap"),
])
def test_missing_or_conflicting_information_needs_reconciliation(history, code):
    result = compose_delivery_impact(history)
    assert result["status"] == "needs_reconciliation"
    assert result["sources"] == []
    assert result["issues"][0]["code"] == code


def test_same_path_in_two_repos_and_sources_never_collapses():
    rows = (claim("one", A, B, files=[file("created"), file("created", repo="community")]),
            claim("two", A, B, source="other", files=[file("modified")]))
    result = compose_delivery_impact(rows)
    assert len(result["sources"]) == 2
    assert sum(len(row["impact_evidence"]["files"]) for row in result["sources"]) == 3


def test_identical_materialized_claim_retains_both_origins():
    first = claim("one", A, B, files=[file("deleted")])
    result = compose_delivery_impact((first, replace(first, record_id="another-actor")))
    source = result["sources"][0]
    assert source["record_ids"] == ["another-actor", "one"]
    assert source["impact_evidence"]["files"] == [file("deleted")]


def test_symbols_compose_but_file_scope_changes_need_reconciliation():
    symbol = dict(name="parse", kind="function", repo="core", file="a.py")
    first = claim("one", A, B, symbols=[dict(symbol, action="created")])
    second = claim("two", B, C, symbols=[dict(symbol, action="deleted")])
    assert compose_delivery_impact((first, second))["sources"][0]["impact_evidence"]["symbols"] == []
    moved = claim("two", B, C, files=[file("renamed", "b.py", previous="a.py")])
    assert compose_delivery_impact((first, moved))["issues"][0]["code"] == "artifact_scope_changed"


def test_cancelled_file_and_symbol_are_absent_from_net_but_remain_history():
    symbol = dict(name="parse", kind="function", repo="core", file="a.py")
    first = claim("one", A, B, files=[file("created")], symbols=[dict(symbol, action="created")])
    second = claim("two", B, C, files=[file("deleted")], symbols=[dict(symbol, action="deleted")])
    result = compose_delivery_impact((first, second))
    assert result["status"] == "composed" and result["history_count"] == 2
    assert result["sources"][0]["impact_evidence"]["files"] == []
    assert result["sources"][0]["impact_evidence"]["symbols"] == []


def test_rename_over_deleted_path_does_not_invent_replacement_identity():
    first = claim("one", A, B, files=[file("deleted", "b.py")])
    second = claim("two", B, C, files=[file("renamed", "b.py", previous="a.py")])
    assert compose_delivery_impact((first, second))["issues"][0]["code"] == "rename_destination_conflict"


def test_surfaces_not_reconfirmed_in_later_delta_are_not_silently_unioned():
    first = claim("one", A, B, surfaces=[dict(kind="mcp_tool", identifier="parse")])
    second = claim("two", B, C, files=[file("modified")])
    assert compose_delivery_impact((first, second))["issues"][0]["code"] == "surface_lifecycle_unknown"


def test_bounds_have_truthful_totals_and_no_partial_net():
    rows = tuple(claim(str(i), None, B, source=str(i)) for i in range(25))
    result = compose_delivery_impact(rows)
    assert result["issue_count"] == 25 and result["issues_truncated"]
    assert len(result["issues"]) == 20
    result = compose_delivery_impact(rows * 9)
    assert result["sources"] == [] and result["history_count"] == 225
    assert result["issues"][0]["code"] == "impact_population_limit"


def test_large_but_valid_claim_does_not_bypass_projection_byte_bound():
    rows = [file("created", f"{index}-" + "x" * 490) for index in range(180)]
    result = compose_delivery_impact((claim("one", A, B, files=rows),))
    assert result["status"] == "needs_reconciliation" and not result["sources"]
    assert result["issues"][0]["code"] == "net_impact_payload_limit"


def test_base_is_optional_for_history_but_never_invented_and_absence_preserves_digest():
    legacy = DeliveryProgress(source_state=dict(workspace_state="unknown", recoverability="unknown"), remaining="Continue")
    assert "impact_base_revision" not in legacy.model_dump()
    with pytest.raises(ValueError, match="impact_base_scope_required"):
        DeliveryProgress(**legacy.model_dump(), impact_base_revision=A)
    scoped = DeliveryProgress(source_state=dict(workspace_state="clean", recoverability="declared_commit", source_ref="source", declared_revision=B),
                              remaining="Review", impact_base_revision=A, impact_delta=ImpactEvidence(files=[file("created")]))
    assert scoped.model_dump()["impact_base_revision"] == A
