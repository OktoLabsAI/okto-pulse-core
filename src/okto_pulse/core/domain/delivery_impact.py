"""Compose declared changes along explicit revision edges, never Git ancestry.

This is a claim projection, not receipt authentication, delivery credit or an
impact-policy decision. The immutable records remain the work history. Missing
bases and ambiguous changes are bounded reconciliation items, not guessed facts.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re

from okto_pulse.core.models.schemas import ImpactEvidence
from okto_pulse.core.domain.delivery_progress import DeliveryProgress, progress_change_scope


@dataclass(frozen=True)
class DeliveryImpactClaim:
    record_id: str
    source_ref: str | None
    base_revision: str | None
    result_revision: str | None
    impact: ImpactEvidence


@dataclass(frozen=True)
class DeliveryImpactObservation:
    receipt_id: str
    source_ref: str
    source_identity_sha256: str | None
    revision: str | None
    observed_at: datetime
    current_accepted_clean: bool


def progress_affects_impact_source(progress: DeliveryProgress, source_ref: str, target_sources: dict[str, str]) -> bool:
    if progress_change_scope(progress) == "none":
        return False
    if progress.source_state.source_ref:
        return progress.source_state.source_ref == source_ref
    if progress.target_ids:
        return any(target_sources.get(identity) in {None, source_ref} for identity in progress.target_ids)
    return True


def require_impact_observation(source: dict, observation: DeliveryImpactObservation | None,
                               material_checkpoints: tuple[datetime, ...], *, expected_identity: str | None = None) -> None:
    """Confirm a declared result against the known source head, not real code.

    No ancestry inference, source inspection or policy waiver. A checkpoint that
    arrived after the observation remains unresolved until an admissible later
    observation. Independent sources and context-only notes do not invalidate it.
    """
    if (observation is None or not observation.current_accepted_clean
        or observation.source_ref != source["source_ref"]
        or not observation.source_identity_sha256
        or (observation.revision or "").lower() != source["result_revision"]
        or (expected_identity is not None and observation.source_identity_sha256 != expected_identity)):
        raise ValueError("delivery_impact_current_observation_required")
    def utc(value):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if any(utc(observation.observed_at) <= utc(value) for value in material_checkpoints):
        raise ValueError("delivery_impact_material_progress_unobserved")


def reusable_impact_block(projection: dict) -> ImpactEvidence:
    """Keep the existing report shape, including scenario links and consumers."""
    if projection.get("status") != "composed":
        raise ValueError("delivery_impact_needs_reconciliation")
    sections = {name: [] for name in ("files", "symbols", "surfaces", "tests")}
    repo_sources, refs = {}, set()
    for source in projection["sources"]:
        impact = ImpactEvidence.model_validate(source["impact_evidence"])
        for name in sections:
            for item in getattr(impact, name):
                repo = getattr(item, "repo", None)
                if repo is not None:
                    if repo in repo_sources and repo_sources[repo] != source["source_ref"]:
                        raise ValueError("delivery_impact_repo_source_ambiguous")
                    repo_sources[repo] = source["source_ref"]
                sections[name].append(item)
        refs.update(impact.evidence_refs)
    try:
        return ImpactEvidence(**sections, evidence_refs=sorted(refs))
    except ValueError as exc:
        raise ValueError("delivery_impact_report_limit") from exc


class _Ambiguous(ValueError):
    pass


def _revision(value):
    return bool(isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", value))


def _ordered_edges(claims):
    edges = {}
    for claim in claims:
        if not _revision(claim.base_revision) or not _revision(claim.result_revision):
            raise _Ambiguous("revision_unknown")
        base, result = claim.base_revision.lower(), claim.result_revision.lower()
        if base == result:
            raise _Ambiguous("unchanged_revision_with_delta")
        edges.setdefault((base, result), []).append(claim)
    starts = {base for base, _ in edges} - {result for _, result in edges}
    if len(starts) != 1:
        raise _Ambiguous("revision_chain_ambiguous")
    cursor = next(iter(starts))
    ordered, visited = [], set()
    while True:
        outgoing = [edge for edge in edges if edge[0] == cursor]
        if not outgoing:
            break
        if len(outgoing) != 1 or cursor in visited:
            raise _Ambiguous("revision_chain_ambiguous")
        visited.add(cursor)
        edge = outgoing[0]
        ordered.append((edge, edges[edge]))
        cursor = edge[1]
    if len(ordered) != len(edges):
        raise _Ambiguous("revision_chain_ambiguous")
    return ordered


def _unique_rows(claims, section, key):
    rows = {}
    for claim in claims:
        for model in getattr(claim.impact, section):
            row = model.model_dump(exclude_none=True)
            row.pop("note", None)  # Notes remain attached to their original record.
            identity = key(row)
            if identity in rows and rows[identity] != row:
                raise _Ambiguous("same_revision_conflicting_claims")
            rows[identity] = row
    return [rows[key] for key in sorted(rows, key=repr)]


def _compose_source(claims):
    ordered = _ordered_edges(claims)
    live, seen, files = {}, set(), []
    symbols, tests, surfaces, refs = {}, {}, {}, set()
    changed_paths = set()
    for step, (_, members) in enumerate(ordered):
        file_rows = _unique_rows(members, "files", lambda row: (row["repo"], row.get("previous_path", row["path"])))
        touched = set()
        for row in file_rows:
            paths = {(row["repo"], row["path"]), (row["repo"], row.get("previous_path", row["path"]))}
            if touched & paths:
                raise _Ambiguous("same_revision_path_overlap")
            touched.update(paths)
        for row in file_rows:
            repo, path, action = row["repo"], row["path"], row["change_kind"]
            old = row.get("previous_path", path)
            key, destination = (repo, old), (repo, path)
            state = live.get(key)
            if action == "created":
                if key in seen:
                    raise _Ambiguous("path_recreated_or_conflicting")
                state = {"repo": repo, "origin": None, "path": path, "modified": False}
                files.append(state)
                live[key] = state
            else:
                if state is None:
                    if key in seen:
                        raise _Ambiguous("path_no_longer_present")
                    state = {"repo": repo, "origin": old, "path": old, "modified": False}
                    files.append(state)
                    live[key] = state
                if action == "modified":
                    state["modified"] = True
                elif action == "deleted":
                    state["path"] = None
                    live.pop(key)
                    changed_paths.add(key)
                elif action == "renamed":
                    if destination in live or destination == key or (
                        destination in seen and path != state["origin"]
                    ):
                        raise _Ambiguous("rename_destination_conflict")
                    live.pop(key)
                    live[destination] = state
                    state["path"] = path
                    changed_paths.update((key, destination))
            seen.update((key, destination))
        for row in _unique_rows(members, "symbols", lambda row: (row["repo"], row["file"], row["kind"], row["name"])):
            key = row["repo"], row["file"], row["kind"], row["name"]
            previous = symbols.get(key)
            if previous:
                old, new = previous["action"], row["action"]
                if old == "deleted" or new == "created":
                    raise _Ambiguous("symbol_lifecycle_ambiguous")
                if old == "created" and new == "deleted":
                    symbols[key] = dict(row, action="cancelled")
                    continue
                if old == "cancelled":
                    raise _Ambiguous("symbol_lifecycle_ambiguous")
                row = dict(row, action="created" if old == "created" else new)
            symbols[key] = row
        for row in _unique_rows(members, "tests", lambda row: (row["repo"], row["test_file_path"], row.get("test_function"), row.get("scenario_id"))):
            key = row["repo"], row["test_file_path"], row.get("test_function"), row.get("scenario_id")
            previous = tests.get(key)
            if previous and row["action"] == "added":
                raise _Ambiguous("test_lifecycle_ambiguous")
            tests[key] = dict(row, action=previous["action"] if previous else row["action"])
        step_surfaces = {(row["kind"], row["identifier"]): row for row in
                         _unique_rows(members, "surfaces", lambda row: (row["kind"], row["identifier"]))}
        if step and set(surfaces) - set(step_surfaces):
            raise _Ambiguous("surface_lifecycle_unknown")
        surfaces.update(step_surfaces)
        refs.update(ref for claim in members for ref in claim.impact.evidence_refs)
    if any(row["action"] != "cancelled" and (row["repo"], row["file"]) in changed_paths for row in symbols.values()) or any(
        (row["repo"], row["test_file_path"]) in changed_paths for row in tests.values()
    ):
        raise _Ambiguous("artifact_scope_changed")
    net = []
    for row in files:
        origin, path = row["origin"], row["path"]
        if origin is None and path is None:
            continue
        if origin is not None and origin == path and not row["modified"]:
            continue
        action = "created" if origin is None else "deleted" if path is None else "renamed" if path != origin else "modified"
        net.append(dict(repo=row["repo"], path=path or origin, change_kind=action,
                        **({"previous_path": origin} if action == "renamed" else {})))
    try:
        impact = ImpactEvidence(files=net, symbols=[row for row in symbols.values() if row["action"] != "cancelled"],
                                tests=list(tests.values()), surfaces=list(surfaces.values()), evidence_refs=sorted(refs))
    except ValueError as exc:
        raise _Ambiguous("net_impact_limit") from exc
    return dict(source_ref=claims[0].source_ref, base_revision=ordered[0][0][0],
                result_revision=ordered[-1][0][1], record_ids=sorted(claim.record_id for claim in claims),
                impact_evidence=impact.model_dump(mode="json", exclude_none=True))


def compose_delivery_impact(claims: tuple[DeliveryImpactClaim, ...]) -> dict:
    """Bounded net claims; never promote `composed` to verified/current/complete.

    Input order is irrelevant: only explicit base→result edges establish order.
    Every original record ID is retained, including identical claims by others.
    A source with ambiguous content has no invented partial net result.
    """
    groups, issues, sources = {}, [], []
    for claim in claims:
        groups.setdefault(claim.source_ref, []).append(claim)
    if len(claims) > 200:
        issues.append(dict(source_ref=None, code="impact_population_limit", record_ids=[]))
    else:
        for source in sorted(groups, key=lambda value: value or ""):
            members = groups[source]
            try:
                if not source:
                    raise _Ambiguous("source_unknown")
                sources.append(_compose_source(members))
            except _Ambiguous as exc:
                issues.append(dict(source_ref=source, code=str(exc), record_ids=sorted(row.record_id for row in members)[:20],
                                   record_count=len(members), records_truncated=len(members) > 20))
    if len(json.dumps(sources).encode("utf-8")) > 64 * 1024:
        sources = []
        issues.append(dict(source_ref=None, code="net_impact_payload_limit", record_ids=[]))
    return dict(contract_version="delivery-net-impact/v1", status="needs_reconciliation" if issues else "composed" if claims else "empty",
                claim_only=True, history_count=len(claims), sources=sources,
                issues=issues[:20], issue_count=len(issues), issues_truncated=len(issues) > 20)
