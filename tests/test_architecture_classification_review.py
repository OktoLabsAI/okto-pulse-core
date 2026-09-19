"""AC-ARQ-05/08/11/13/14/16: full-population, read-only classification review."""

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from okto_pulse.core.domain.architecture_candidates import (
    AdoptedArchitectureDesign,
    ArchitectureCandidateIssue,
    project_architecture_candidates,
)
from okto_pulse.core.domain.architecture_classification_review import (
    ArchitectureClassificationReadError,
    architecture_classification_review,
)
from okto_pulse.core.ports.architecture_classification import ArchitectureDecisionRecord


def sources(*interfaces, revision=1, complete=True):
    return project_architecture_candidates(
        board_id="board",
        spec_id="spec",
        spec_edition=2,
        source_complete=complete,
        designs=(
            AdoptedArchitectureDesign("board", "adopted", "root", revision, interfaces),
        ),
    )


def interface(identity="orders", **changes):
    return {
        "id": identity,
        "name": identity,
        "event_schema": {"publish": {}, "consume": {}},
        "error_contract": {"retry": True},
        "schema_ref": "https://do-not-fetch.invalid/schema",
    } | changes


def decision(candidate, **changes):
    return replace(
        ArchitectureDecisionRecord(
            id=f"decision-{candidate.interface_id}",
            spec_id="spec",
            spec_edition=2,
            spec_version=8,
            candidate_id=candidate.id,
            source_digest=candidate.source_digest,
            root_design_id=candidate.root_design_id,
            interface_id=candidate.interface_id,
            source_contract_json=candidate.contract_json,
            adopted_sources=candidate.adopted_sources,
            actor_id="author",
            classified_at=datetime(2026, 9, 19, tzinfo=UTC),
            disposition="context_only",
            integration_requirement_ids=(),
            reason="Outside this change",
        ),
        **changes,
    )


def review(population, decisions=(), irs=(), **kwargs):
    return architecture_classification_review(
        board_id="board",
        spec_id="spec",
        spec_edition=2,
        spec_version=9,
        population=population,
        decisions=decisions,
        integration_requirements=irs,
        **kwargs,
    )


def detail(population, decisions=(), irs=()):
    candidate = population.candidates[0]
    return review(
        population,
        decisions,
        irs,
        candidate_id=candidate.id,
        source_digest=candidate.source_digest,
    )["items"][0]


def test_global_pending_outside_page_and_filter_never_becomes_approval():
    pop = sources(*(interface(str(i)) for i in range(31)))
    records = tuple(decision(item) for item in pop.candidates[:30])
    result = review(pop, records, limit=1)
    assert len(result["items"]) == 1 and result["has_more"]
    assert (
        result["state_counts"]["pending"] == 1
        and result["state_counts"]["current"] == 30
    )
    assert result["total"] == 31 and not result["classification_complete"]
    assert all(
        result[key] is False
        for key in (
            "admission_evaluated",
            "semantic_review_evaluated",
            "rollout_evaluated",
        )
    )
    filtered = review(pop, records, state="pending")
    assert filtered["total"] == 1 and filtered["state_counts"] == result["state_counts"]
    assert filtered["items"][0]["candidate_id"] == pop.candidates[-1].id
    assert "current_contract" not in result["items"][0]


def test_layout_copy_revision_and_participant_order_preserve_currentness():
    before = sources(interface(participants=["a", "b"], position={"x": 1}))
    after = sources(interface(participants=["b", "a"], position={"x": 900}), revision=7)
    result = detail(after, (decision(before.candidates[0]),))
    assert result["state"] == "current" and result["changed_paths"] == []
    assert result["decisions"][0]["adopted_sources"] == [
        {"design_id": "adopted", "revision": 1}
    ]


def test_only_semantically_changed_candidate_requires_review():
    before = sources(interface("a"), interface("b"))
    after = sources(
        interface("a", error_contract={"retry": False}), interface("b"), revision=2
    )
    records = tuple(decision(item) for item in before.candidates)
    assert {
        row["interface_id"]: row["state"] for row in review(after, records)["items"]
    } == {"a": "review_required", "b": "current"}


@pytest.mark.parametrize("area", ["publish", "consume", "remainder"])
def test_partial_scope_invalidates_only_affected_fragment_or_remainder(area):
    before = sources(interface())
    candidate = before.candidates[0]
    records = tuple(
        decision(
            candidate,
            id=part,
            scope_paths=(f"/event_schema/{part}",),
            remainder_reason="The error contract stays contextual",
        )
        for part in ("publish", "consume")
    )
    changed = interface()
    if area == "remainder":
        changed["error_contract"]["retry"] = False
    else:
        changed["event_schema"][area] = {"required": ["id"]}
    result = detail(sources(changed), records)
    assert result["state"] == "review_required"
    assert {row["decision_id"]: row["state"] for row in result["decisions"]} == {
        part: "review_required" if part == area else "current"
        for part in ("publish", "consume")
    }
    assert result["remainder_state"] == (
        "review_required" if area == "remainder" else "current"
    )


@pytest.mark.parametrize("after", [1, None, [True]])
def test_json_type_and_presence_changes_are_material(after):
    before = sources(interface(error_contract={"retry": True}))
    current = sources(interface(error_contract={"retry": after}))
    result = detail(current, (decision(before.candidates[0]),))
    assert result["state"] == "review_required" and result["changed_paths"] == [
        "/error_contract/retry"
    ]
    absent = detail(
        sources(interface(error_contract={})), (decision(current.candidates[0]),)
    )
    assert absent["state"] == "review_required" and absent["changed_paths"] == [
        "/error_contract/retry"
    ]


def test_arrays_are_atomic_and_paths_escape_slash_and_tilde():
    before = sources(interface(event_schema={"a/b~c": ["first", "second"]}))
    after = sources(interface(event_schema={"a/b~c": ["second", "first"]}))
    record = decision(
        before.candidates[0],
        scope_paths=("/event_schema/a~1b~0c",),
        remainder_reason="Other context",
    )
    result = detail(after, (record,))
    assert (
        result["changed_paths"] == ["/event_schema/a~1b~0c"]
        and result["state"] == "review_required"
    )


@pytest.mark.parametrize(
    "irs",
    [
        (),
        ({"id": "other", "status": "active"},),
        ({"id": "ir", "status": "removed"},),
        ({"id": "ir"}, {"id": "ir"}),
    ],
)
def test_missing_inactive_or_ambiguous_ir_requires_review(irs):
    pop = sources(interface())
    record = decision(
        pop.candidates[0],
        disposition="associate_existing_ir",
        integration_requirement_ids=("ir",),
        reason=None,
    )
    assert (
        detail(pop, (record,), ({"id": "ir", "status": "active"},))["state"]
        == "current"
    )
    result = detail(pop, (record,), irs)
    assert (
        result["state"] == "review_required"
        and "architecture_classification_ir_not_active_in_spec" in result["issues"]
    )


def test_retired_contract_keeps_history_and_normative_ir_without_writes():
    before = sources(interface())
    candidate = before.candidates[0]
    record = decision(
        candidate,
        disposition="promote_to_ir",
        integration_requirement_ids=("ir",),
        reason=None,
    )
    irs = ({"id": "ir", "status": "active", "title": "Still normative"},)
    original = copy.deepcopy(irs)
    result = review(
        sources(),
        (record,),
        irs,
        candidate_id=candidate.id,
        source_digest=candidate.source_digest,
    )
    item = result["items"][0]
    assert item["state"] == "retired" and item["current_contract"] is None
    assert item["analyzed_contract"] == candidate.contract and item["decisions"][0][
        "integration_requirement_refs"
    ] == ["ir"]
    assert irs == original and result["admission_evaluated"] is False


@pytest.mark.parametrize("observed", [True, False])
def test_unavailable_source_never_means_retired_or_complete(observed):
    pop = sources(interface())
    record = decision(pop.candidates[0])
    unavailable = replace(pop if observed else sources(), source_complete=False)
    result = review(unavailable, (record,))
    assert result["items"][0]["state"] == "unavailable" and result["total"] is None
    assert (
        result["counts_scope"] == "observed" and not result["classification_complete"]
    )
    empty = review(sources(complete=False))
    assert empty["total"] is None and not empty["classification_complete"]


def test_unknown_global_issue_prevents_retirement_and_completeness():
    pop = sources(interface())
    unknown = replace(
        sources(),
        issues=(ArchitectureCandidateIssue("architecture_contract_identity_required"),),
    )
    result = review(unknown, (decision(pop.candidates[0]),))
    assert (
        result["items"][0]["state"] == "unavailable"
        and not result["classification_complete"]
    )


@pytest.mark.parametrize(
    "change",
    [
        {"source_contract_json": "{}"},
        {"source_contract_json": "not-json"},
        {"source_digest": "0" * 64},
        {"spec_version": 10},
        {"spec_version": True},
        {"actor_id": ""},
        {"classified_at": "yesterday"},
        {"adopted_sources": ()},
        {"adopted_sources": (("design", True),)},
        {"scope_paths": ("/missing",), "remainder_reason": "Other context"},
        {"scope_paths": ("/event_schema/0",), "remainder_reason": "Other context"},
    ],
)
def test_corrupt_witness_is_unresolved_not_repaired_or_exposed(change):
    pop = sources(interface())
    record = decision(pop.candidates[0], **change)
    result = detail(pop, (record,))
    assert result["state"] == "unresolved" and result["analyzed_contract"] is None
    assert result["decisions"] == [] and result["analyzed_source_digest"] is None
    retired = review(sources(), (record,))
    assert (
        retired["items"] == []
        and retired["total"] is None
        and not retired["classification_complete"]
    )


@pytest.mark.parametrize("change", [{"spec_id": "foreign-secret"}, {"spec_edition": 1}])
def test_foreign_history_not_exposed_and_cannot_satisfy_current_edition(change):
    pop = sources(interface())
    result = review(pop, (decision(pop.candidates[0], **change),))
    assert result["items"][0]["state"] == "pending" and result["total"] is None
    assert (
        "foreign-secret" not in json.dumps(result)
        and not result["classification_complete"]
    )


def test_conflicting_adopted_revisions_do_not_choose_a_winner():
    before, after = (
        sources(interface()),
        sources(interface(error_contract={"retry": False})),
    )
    conflict = replace(before, candidates=before.candidates + after.candidates)
    result = detail(conflict, (decision(before.candidates[0]),))
    assert result["state"] == "unresolved" and result["current_source_digest"] is None
    assert result["promotion_suggestion"] is None
    assert (
        result["source_variant_count"] == 2
        and result["decisions"][0]["state"] == "unresolved"
    )


def test_detail_rejects_stale_digest_and_pagination_validates_without_coercion():
    before, after = (
        sources(interface()),
        sources(interface(error_contract={"retry": False})),
    )
    with pytest.raises(ArchitectureClassificationReadError, match="source_changed"):
        review(
            after,
            candidate_id=before.candidates[0].id,
            source_digest=before.candidates[0].source_digest,
        )
    for kwargs in (
        {"offset": True},
        {"limit": False},
        {"limit": 101},
        {"state": "approved"},
        {"candidate_id": "x"},
    ):
        with pytest.raises(ArchitectureClassificationReadError):
            review(before, **kwargs)


def test_bounded_diff_does_not_hide_outdated_obligations_or_source_witness():
    before = sources(interface(event_schema={str(i): False for i in range(125)}))
    after = sources(interface(event_schema={str(i): True for i in range(125)}))
    result = detail(after, (decision(before.candidates[0]),))
    assert len(result["changed_paths"]) == 100 and result["changed_paths_truncated"]
    assert (
        result["state"] == "review_required"
        and len(result["analyzed_contract"]["event_schema"]) == 125
    )
