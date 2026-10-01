"""Pure Bug cluster semantics shared by the planned traceability and Analytics UI.

This reducer consumes authorized facts; it is not an authorization boundary or
a graph completeness detector. Editions must supply the checkpoint proof defined
by the read port. Missing proof remains unknown, even with an empty work queue.
"""

from dataclasses import asdict
from datetime import datetime
import hashlib
import json
import re
from statistics import median

from okto_pulse.core.ports.analytics_foundation import require_utc_datetime
from okto_pulse.core.ports.bug_clusters import (
    BugClustersQuery, BugClustersSnapshot, MAX_CLUSTER_ASSOCIATIONS,
    MAX_CLUSTER_BUGS, MAX_CLUSTER_RESPONSE_BYTES, ProjectionFreshnessState,
)


def _text(value, *, optional=False):
    if optional and value is None:
        return
    if type(value) is not str or not value.strip() or len(value) > 4096:
        raise ValueError("bug_clusters_fact_invalid")


def _stamp(value):
    return require_utc_datetime(value, field="bug_clusters_timestamp").isoformat()


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      default=lambda item: _stamp(item) if isinstance(item, datetime) else item,
                      allow_nan=False).encode("utf-8")


def _validate(query, snapshot):
    if (snapshot.board_id, snapshot.actor_scope_ref) != (query.board_id, query.actor_scope_ref):
        raise ValueError("bug_clusters_scope_mismatch")
    _stamp(snapshot.checked_at)
    if type(snapshot.bugs) is not tuple or len(snapshot.bugs) > MAX_CLUSTER_BUGS:
        raise ValueError("bug_clusters_source_bound")
    if type(snapshot.associations) is not tuple or len(snapshot.associations) > MAX_CLUSTER_ASSOCIATIONS:
        raise ValueError("bug_clusters_association_bound")
    for flag in (snapshot.graph_available, snapshot.source_inventory_complete, snapshot.associations_truncated):
        if type(flag) is not bool:
            raise ValueError("bug_clusters_fact_invalid")
    count = snapshot.expected_bug_count
    if count is not None and (type(count) is not int or count < len(snapshot.bugs)):
        raise ValueError("bug_clusters_denominator_invalid")
    if snapshot.source_inventory_complete and count != len(snapshot.bugs):
        raise ValueError("bug_clusters_inventory_inconsistent")
    ids = set()
    for bug in snapshot.bugs:
        for value in (bug.bug_id, bug.title, bug.status, bug.source_revision):
            _text(value)
        _text(bug.severity, optional=True)
        _text(bug.spec_ref, optional=True)
        if bug.spec_ref is not None and not bug.spec_ref.startswith("spec:"):
            raise ValueError("bug_clusters_spec_ref_invalid")
        if bug.bug_id in ids:
            raise ValueError("bug_clusters_duplicate_source")
        ids.add(bug.bug_id)
        if (not query.window.contains(bug.source_created_at)
                or bug.source_created_at > snapshot.checked_at
                or query.status is not None and bug.status != query.status
                or query.severity is not None and bug.severity != query.severity):
            raise ValueError("bug_clusters_source_outside_scope")
        if bug.resolved_at is not None:
            _stamp(bug.resolved_at)
            if bug.status != "done" or not bug.source_created_at <= bug.resolved_at <= snapshot.checked_at:
                raise ValueError("bug_clusters_resolution_invalid")
    if (type(snapshot.projected_bug_ids) is not tuple
            or len(snapshot.projected_bug_ids) != len(set(snapshot.projected_bug_ids))
            or not set(snapshot.projected_bug_ids) <= ids):
        raise ValueError("bug_clusters_projection_scope_invalid")
    for value in (snapshot.graph_generation, snapshot.expected_projection_checkpoint, snapshot.projection_checkpoint):
        _text(value, optional=True)
    if not snapshot.graph_available and (snapshot.associations or snapshot.projected_bug_ids
                                        or snapshot.projection_checkpoint is not None):
        raise ValueError("bug_clusters_unavailable_with_projection")
    for item in snapshot.associations:
        if (item.bug_id not in snapshot.projected_bug_ids or item.kind not in ("proxy", "learning")
                or item.validity not in ("current", "previous", "unknown")):
            raise ValueError("bug_clusters_association_invalid")
        for value in (item.target_ref, item.title, item.provenance_ref):
            _text(value)


def _freshness(snapshot) -> ProjectionFreshnessState:
    if not snapshot.graph_available:
        return "unavailable"
    if (not snapshot.source_inventory_complete or snapshot.associations_truncated
            or len(snapshot.projected_bug_ids) != len(snapshot.bugs)):
        return "incomplete"
    if not (snapshot.graph_generation and snapshot.expected_projection_checkpoint and snapshot.projection_checkpoint):
        return "unknown"
    if snapshot.expected_projection_checkpoint != snapshot.projection_checkpoint:
        return "lagging"
    return "current"


def project_bug_clusters(query: BugClustersQuery, snapshot: BugClustersSnapshot) -> dict:
    """Aggregate a bounded observation; never count join rows as distinct Bugs.

    Page rows are clusters. All counts are computed before pagination. A Bug may
    belong to multiple clusters; those cluster counts must not be summed into a
    Board total. Incomplete inventories yield null exact counts, with explicitly
    named observed counts. Projection positives keep their freshness labels.
    """
    _validate(query, snapshot)
    freshness = _freshness(snapshot)
    graph_grouping = query.group_by in ("proxy", "learning")
    complete = snapshot.source_inventory_complete and (not graph_grouping or freshness == "current")
    groups = {}
    bugs = {bug.bug_id: bug for bug in snapshot.bugs}
    for bug in snapshot.bugs:
        if query.group_by in ("spec", "severity"):
            key = bug.spec_ref if query.group_by == "spec" else bug.severity
            identity = (key, "source")
            groups.setdefault(identity, {"title": key or "Unspecified", "bugs": set(), "provenance": set()})["bugs"].add(bug.bug_id)
    for item in snapshot.associations:
        if item.kind != query.group_by:
            continue
        identity = (item.target_ref, item.validity)
        group = groups.setdefault(identity, {"title": item.title[:240], "bugs": set(), "provenance": set()})
        # A contradictory title is not resolved using join order.
        group["title"] = min(group["title"], item.title[:240])
        group["bugs"].add(item.bug_id)
        group["provenance"].add(item.provenance_ref)
    rows = []
    for (ref, validity), group in sorted(groups.items(), key=lambda pair: (pair[0][0] or "", pair[0][1])):
        members = [bugs[key] for key in sorted(group["bugs"])]
        resolved = [bug for bug in members if bug.resolved_at is not None]
        rows.append({
            "target_ref": ref, "title": group["title"][:240], "validity": validity,
            "assertion_basis": {"proxy": "origin_proxy", "learning": "recorded_learning",
                                "spec": "source_spec", "severity": "source_severity"}[query.group_by],
            "causal_conclusion": "not_established",
            "distinct_bug_count": len(members) if complete else None,
            "observed_bug_count": len(members),
            "observed_done_count": sum(bug.status == "done" for bug in members),
            "observed_resolution_timestamp_count": len(resolved),
            "observed_median_resolution_hours": median([
                (bug.resolved_at - bug.source_created_at).total_seconds() / 3600 for bug in resolved
            ]) if resolved else None,
            # The independent payload bound is applied below. Arrays never grow
            # without limit merely because SQL returned one aggregate row.
            "bug_refs": [f"card:{bug.bug_id}" for bug in members],
            "provenance_refs": sorted(group["provenance"]),
            "projection_freshness": freshness if graph_grouping else "not_applicable",
        })
    # Source revisions, grouping inputs and graph generation all participate;
    # checked_at alone does not invalidate an otherwise identical observation.
    identity_snapshot = asdict(snapshot)
    identity_snapshot.pop("checked_at")
    identity_snapshot["bugs"] = sorted(identity_snapshot["bugs"], key=lambda item: item["bug_id"])
    identity_snapshot["associations"] = sorted(identity_snapshot["associations"], key=lambda item: _encoded(item))
    identity_snapshot["projected_bug_ids"] = sorted(identity_snapshot["projected_bug_ids"])
    query_identity = asdict(query)
    query_identity.pop("cursor")
    digest = hashlib.sha256(_encoded([query_identity, identity_snapshot])).hexdigest()
    offset = 0
    if query.cursor is not None:
        match = re.fullmatch(r"bug-clusters-v1:([0-9a-f]{64}):([1-9][0-9]{0,5})", query.cursor)
        if not match:
            raise ValueError("bug_clusters_cursor_invalid")
        if match[1] != digest:
            raise ValueError("bug_clusters_cursor_stale")
        offset = int(match[2])
        if offset >= len(rows):
            raise ValueError("bug_clusters_cursor_invalid")
    limitations = []
    if not snapshot.source_inventory_complete:
        limitations.append("source_inventory_incomplete")
    if freshness != "current":
        limitations.append(f"graph_{freshness}")
    if snapshot.associations_truncated:
        limitations.append("association_limit")
    response = {
        "view": "bugs", "subject_ref": f"board:{query.board_id}", "group_by": query.group_by,
        "window": query.window.canonical_dict(), "authority": "informational",
        "data_source": "mixed_relational_graph" if graph_grouping else "relational",
        "projection_freshness": {"state": freshness, "graph_generation": snapshot.graph_generation,
            "source_checkpoint": snapshot.expected_projection_checkpoint,
            "projection_checkpoint": snapshot.projection_checkpoint, "checked_at": _stamp(snapshot.checked_at)},
        "completeness": {"complete_for_scope": complete, "truncated": False, "limitations": limitations},
        "distinct_bug_count": snapshot.expected_bug_count,
        "observed_bug_count": len(bugs), "cluster_count": len(rows) if complete else None,
        "observed_cluster_count": len(rows),
        "resolution_semantics": "latest_verified_done_transition_for_current_done_bugs; missing_timestamp_is_unknown",
        "items": [], "next_cursor": None,
    }
    # Budget includes the envelope, every nested Bug/provenance array and cursor.
    for row in rows[offset:offset + query.limit]:
        response["items"].append(row)
        next_offset = offset + len(response["items"])
        response["next_cursor"] = f"bug-clusters-v1:{digest}:{next_offset}" if next_offset < len(rows) else None
        if len(_encoded(response)) > MAX_CLUSTER_RESPONSE_BYTES:
            response["items"].pop()
            if not response["items"]:
                raise ValueError("bug_clusters_single_cluster_payload_limit")
            response["next_cursor"] = f"bug-clusters-v1:{digest}:{offset + len(response['items'])}"
            break
    response["completeness"]["truncated"] = snapshot.associations_truncated or not snapshot.source_inventory_complete
    return response
