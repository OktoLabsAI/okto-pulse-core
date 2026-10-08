"""KG §6/§9 semantics; these are not adapter/authorization/UI acceptance tests."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.ports.bug_clusters import (
    BugClusterAssociation, BugClustersQuery, BugClustersSnapshot, ClusterBugFact,
)
from okto_pulse.core.services.bug_clusters import project_bug_clusters

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def query(**changes):
    return BugClustersQuery.recent(board_id="board-a", actor_scope_ref="actor-a", now=NOW, **changes)


def bug(identity="one", **changes):
    return replace(ClusterBugFact(identity, "Different diagnosis " + identity,
        NOW - timedelta(days=2), "done", "major", "spec:one", "revision-1",
        NOW - timedelta(days=1)), **changes)


def association(identity="one", **changes):
    return replace(BugClusterAssociation(identity, "proxy", "spec:one/constraint:one",
        "Associated constraint", "proxy-rule:one", "current"), **changes)


def snapshot(**changes):
    return replace(BugClustersSnapshot("board-a", "actor-a", NOW, (bug(), bug("two")),
        2, True, (association(), association("two")), graph_generation="generation-a",
        expected_projection_checkpoint="full-source-checkpoint", projection_checkpoint="full-source-checkpoint",
        projected_bug_ids=("one", "two")), **changes)


def test_two_different_causes_share_proxy_without_becoming_common_cause():
    result = project_bug_clusters(query(), snapshot())
    row, = result["items"]
    assert result["distinct_bug_count"] == row["distinct_bug_count"] == 2
    assert result["authority"] == "informational"
    assert row["assertion_basis"] == "origin_proxy"
    assert row["causal_conclusion"] == "not_established"
    assert row["bug_refs"] == ["card:one", "card:two"]


def test_join_fanout_and_multiple_targets_never_inflate_board_denominator():
    base = snapshot()
    result = project_bug_clusters(query(), replace(base, associations=base.associations * 3 + (
        association(target_ref="spec:one/constraint:two"),)))
    assert result["distinct_bug_count"] == 2
    assert [item["distinct_bug_count"] for item in result["items"]] == [2, 1]
    assert result["items"][0]["provenance_refs"] == ["proxy-rule:one"]


def test_default_window_is_fifteen_days_and_uses_source_creation():
    request = query()
    assert request.window.to_exclusive - request.window.from_inclusive == timedelta(days=15)
    old = bug(source_created_at=NOW - timedelta(days=16))
    with pytest.raises(ValueError, match="source_outside_scope"):
        project_bug_clusters(request, snapshot(bugs=(old, bug("two"))))
    boundary = bug(source_created_at=request.window.from_inclusive)
    project_bug_clusters(request, snapshot(bugs=(boundary, bug("two"))))
    with pytest.raises(ValueError, match="source_outside_scope"):
        project_bug_clusters(request, snapshot(bugs=(bug(source_created_at=NOW, resolved_at=NOW), bug("two"))))


@pytest.mark.parametrize('filters', [{'severity': 'high'}, {'severity': 'low'}, {'status': 'completed'}])
def test_filters_use_existing_domain_enums(filters):
    with pytest.raises(ValueError, match='filter_invalid'):
        query(**filters)


@pytest.mark.parametrize(("changes", "state"), [
    ({"expected_projection_checkpoint": None}, "unknown"),
    ({"projection_checkpoint": None}, "unknown"),
    ({"graph_generation": None}, "unknown"),
    ({"projection_checkpoint": "previous"}, "lagging"),
    ({"projected_bug_ids": ("one",), "associations": (association(),)}, "incomplete"),
    ({"associations_truncated": True}, "incomplete"),
    ({"expected_bug_count": 3, "source_inventory_complete": False}, "incomplete"),
    ({"graph_available": False, "graph_generation": None, "projection_checkpoint": None,
      "projected_bug_ids": (), "associations": ()}, "unavailable"),
])
def test_no_current_or_exact_cluster_count_without_complete_scope_proof(changes, state):
    result = project_bug_clusters(query(), snapshot(**changes))
    assert result["projection_freshness"]["state"] == state
    assert result["completeness"]["complete_for_scope"] is False
    assert result["cluster_count"] is None
    assert all(item["distinct_bug_count"] is None for item in result["items"])


def test_lost_projection_with_empty_graph_is_incomplete_not_no_bugs():
    result = project_bug_clusters(query(), snapshot(projected_bug_ids=(), associations=()))
    assert result["distinct_bug_count"] == 2
    assert result["items"] == []
    assert result["projection_freshness"]["state"] == "incomplete"


def test_unknown_denominator_is_not_zero_or_page_count():
    result = project_bug_clusters(query(), snapshot(expected_bug_count=None, source_inventory_complete=False))
    assert result["distinct_bug_count"] is None
    assert result["observed_bug_count"] == 2


@pytest.mark.parametrize("group_by", ["spec", "severity"])
def test_authoritative_grouping_survives_graph_unavailability(group_by):
    result = project_bug_clusters(query(group_by=group_by), snapshot(graph_available=False,
        graph_generation=None, projection_checkpoint=None, projected_bug_ids=(), associations=()))
    assert result["data_source"] == "relational"
    assert result["completeness"]["complete_for_scope"] is True
    assert result["items"][0]["distinct_bug_count"] == 2
    assert result["items"][0]["projection_freshness"] == "not_applicable"


def test_learning_keeps_previous_interpretation_separate_from_current():
    observations = (association(kind="learning", target_ref="learning:a", provenance_ref="source:a"),
        association("two", kind="learning", target_ref="learning:a", validity="previous", provenance_ref="source:old"))
    result = project_bug_clusters(query(group_by="learning"), snapshot(associations=observations))
    assert [(row["validity"], row["distinct_bug_count"]) for row in result["items"]] == [("current", 1), ("previous", 1)]
    assert all(row["assertion_basis"] == "recorded_learning" and row["causal_conclusion"] == "not_established"
               for row in result["items"])


def test_missing_resolution_is_unknown_and_reopened_is_not_resolved():
    result = project_bug_clusters(query(group_by="severity"), snapshot(bugs=(
        bug(resolved_at=None), bug("two", status="in_progress", resolved_at=None))))
    row, = result["items"]
    assert row["observed_done_count"] == 1
    assert row["observed_resolution_timestamp_count"] == 0
    assert row["observed_median_resolution_hours"] is None
    with pytest.raises(ValueError, match="resolution_invalid"):
        project_bug_clusters(query(), snapshot(bugs=(bug(status="in_progress"), bug("two"))))


def test_resolution_duration_uses_source_creation_and_verified_done_time():
    result = project_bug_clusters(query(), snapshot())
    assert result["items"][0]["observed_median_resolution_hours"] == 24


def test_counts_are_for_whole_scope_and_cursor_binds_generation_sources_filters_actor():
    state = snapshot(associations=(association(), association("two", target_ref="target:two")))
    request = query(limit=1)
    first = project_bug_clusters(request, state)
    assert first["cluster_count"] == first["distinct_bug_count"] == 2
    next_query = replace(request, cursor=first["next_cursor"])
    assert project_bug_clusters(next_query, state)["next_cursor"] is None
    for changes in ({"graph_generation": "new"}, {"bugs": (bug(source_revision="edited"), bug("two"))},
                    {"associations": (association(title="Changed title"), state.associations[1])}):
        with pytest.raises(ValueError, match="cursor_stale"):
            project_bug_clusters(next_query, replace(state, **changes))
    for changed_query, changed_state in (
        (replace(next_query, severity="major"), state),
        (replace(next_query, actor_scope_ref="actor-b"), replace(state, actor_scope_ref="actor-b")),
        (replace(next_query, board_id="board-b"), replace(state, board_id="board-b")),
    ):
        with pytest.raises(ValueError, match="cursor_stale"):
            project_bug_clusters(changed_query, changed_state)


def test_cursor_stable_across_read_order_and_checked_at():
    state = snapshot(associations=(association(), association("two", target_ref="target:two")))
    request = query(limit=1)
    first = project_bug_clusters(request, state)
    reordered = replace(state, bugs=tuple(reversed(state.bugs)), associations=tuple(reversed(state.associations)),
                        projected_bug_ids=tuple(reversed(state.projected_bug_ids)), checked_at=NOW + timedelta(seconds=1))
    assert project_bug_clusters(replace(request, cursor=first["next_cursor"]), reordered)["next_cursor"] is None


@pytest.mark.parametrize("cursor", ["junk", "bug-clusters-v1:" + "a" * 64 + ":0", "x" * 257])
def test_invalid_cursors_fail_explicitly(cursor):
    with pytest.raises(ValueError, match="cursor_invalid"):
        project_bug_clusters(query(cursor=cursor), snapshot())


@pytest.mark.parametrize("changes", [
    {"board_id": "foreign"}, {"actor_scope_ref": "foreign"},
    {"bugs": (bug(), bug())}, {"expected_bug_count": 1},
    {"expected_bug_count": 3}, {"projected_bug_ids": ("one", "foreign")},
    {"associations": (association("foreign"),)},
])
def test_inconsistent_or_foreign_inventory_is_rejected(changes):
    with pytest.raises(ValueError):
        project_bug_clusters(query(), snapshot(**changes))


def test_payload_budget_bounds_nested_arrays_not_only_rows(monkeypatch):
    import okto_pulse.core.services.bug_clusters as reducer
    state = snapshot(associations=(association(), association("two", target_ref="target:two")))
    request = query(limit=1)
    page = project_bug_clusters(request, state)
    budget = len(reducer._encoded(page)) + 10
    monkeypatch.setattr(reducer, "MAX_CLUSTER_RESPONSE_BYTES", budget)
    limited = project_bug_clusters(query(), state)
    assert len(limited["items"]) == 1
    assert limited["next_cursor"] is not None
    monkeypatch.setattr(reducer, "MAX_CLUSTER_RESPONSE_BYTES", 64)
    with pytest.raises(ValueError, match="single_cluster_payload_limit"):
        project_bug_clusters(query(), state)


def test_reopened_bug_duration_spans_creation_to_latest_done_not_recovery_episode():
    from okto_pulse.core.kg.source_projection_metadata import latest_resolution_time
    from okto_pulse.core.ports.consolidation import CardLifecycleTransition

    created = NOW - timedelta(days=4)
    first = CardLifecycleTransition("first", NOW - timedelta(days=3), "in_progress", "done")
    reopened = CardLifecycleTransition("reopened", NOW - timedelta(days=2), "done", "in_progress")
    last = CardLifecycleTransition("last", NOW - timedelta(days=1), "in_progress", "done")
    resolved = datetime.fromisoformat(latest_resolution_time("done", (last, reopened)))
    state = snapshot(bugs=(bug(source_created_at=created, resolved_at=resolved),
                           bug("two", resolved_at=None)))
    result = project_bug_clusters(query(), state)
    assert result["items"][0]["observed_median_resolution_hours"] == 72
    assert result["items"][0]["observed_resolution_timestamp_count"] == 1
    assert "latest_verified_done_transition" in result["resolution_semantics"]
    assert latest_resolution_time("in_progress", (reopened, first)) is None
    assert latest_resolution_time("done", ()) is None
