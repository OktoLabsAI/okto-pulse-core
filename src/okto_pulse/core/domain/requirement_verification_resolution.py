"""Relational qualification paths, without assigning work or admitting evidence.

Explicit AC links and explicitly selected inheritance are the only edges.
BR→FR, scenario results, graph adjacency and task links never add verification
credit. This projection is a reusable input to the later joint planning gate.
"""

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from okto_pulse.core.domain.criterion_verification import (
    CriterionVerification,
    VERIFICATION_REQUIREMENT_FIELDS,
    criterion_verification_fields,
)
from okto_pulse.core.domain.requirement_verification import (
    RequirementVerification,
    requirement_verification_digest,
    verification_default_proposal,
)

INACTIVE_STATES = frozenset({"revoked", "superseded", "cancelled", "deprecated"})
_MAX_NODES = 5000
_MAX_PATHS = 4096
_MAX_DEPTH = 32


def resolve_requirement_verification(
    *,
    spec_id: str,
    collections: Mapping[str, Any],
    minimum_profiles: Mapping[tuple[str, str], frozenset[str]] | None = None,
) -> dict[str, Any]:
    """Resolve the complete in-scope population before any transport pagination.

    ``minimum_profiles`` is supplied by authoritative policy, never request data.
    Missing collection facts and evaluation limits are unavailable, not zero.
    Paths retain each selected aspect and source definition binding. Conditions
    may be structurally linked and still require semantic rejection by a reviewer.
    """
    nodes: dict[tuple[str, str], list[Mapping]] = defaultdict(list)
    criteria: dict[str, list[Mapping]] = defaultdict(list)
    issues: list[dict] = []
    issue_count = 0
    complete = True

    def issue(code: str, **facts):
        nonlocal issue_count
        issue_count += 1
        if len(issues) < 100:
            issues.append({"code": code, **facts})

    for kind, field in {
        **VERIFICATION_REQUIREMENT_FIELDS,
        "acceptance_criterion": "acceptance_criteria",
    }.items():
        if field not in collections or (
            collections[field] is not None
            and not isinstance(collections[field], (list, tuple))
        ):
            complete = False
            issue("verification_population_unavailable", field=field)
            continue
        values = collections[field] or ()
        if len(values) > _MAX_NODES:
            complete = False
            issue("verification_resolution_limit", field=field)
        for item in values[:_MAX_NODES]:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("id"), str)
                or not item["id"].strip()
                or len(item["id"]) > 255
            ):
                complete = False
                issue("verification_identity_required", field=field)
                continue
            if kind == "acceptance_criterion":
                criteria[item["id"]].append(item)
            else:
                nodes[(kind, item["id"])].append(item)

    active = {
        key
        for key, values in nodes.items()
        if any(item.get("status") not in INACTIVE_STATES for item in values)
    }
    blockers: dict[tuple[str, str], list[dict]] = defaultdict(list)
    configs: dict[tuple[str, str], RequirementVerification] = {}
    digests: dict[tuple[str, str], str] = {}
    direct: dict[tuple[str, str], list[dict]] = defaultdict(list)
    paths: dict[tuple[str, str], list[dict]] = {}
    path_count = 0
    direct_count = 0
    visiting: list[tuple[str, str]] = []

    def block(key, code, **facts):
        value = {"code": code, **facts}
        if value not in blockers[key]:
            blockers[key].append(value)

    for key in sorted(active):
        values = nodes[key]
        if len(values) != 1:
            block(key, "verification_identity_ambiguous")
            continue
        row = values[0]
        if row.get("status") not in (None, "active", "not_applicable"):
            block(key, "verification_requirement_state_invalid")
        try:
            digests[key] = requirement_verification_digest(spec_id, key[0], row)
            if row.get("verification") is None:
                block(key, "verification_profile_required")
                continue
            configs[key] = config = RequirementVerification.model_validate(
                row["verification"]
            )
            if (
                not (minimum_profiles or {})
                .get(key, frozenset())
                .issubset(config.required_profiles)
            ):
                block(key, "verification_policy_minimum_required")
        except (ValidationError, ValueError, TypeError):
            block(key, "verification_configuration_invalid")

    for identity, values in sorted(criteria.items()):
        if len(values) != 1:
            issue("verification_criterion_ambiguous", criterion_id=identity)
            continue
        row = values[0]
        if row.get("status") in INACTIVE_STATES:
            continue
        if row.get("status") not in (None, "active", "not_applicable"):
            issue("verification_criterion_state_invalid", criterion_id=identity)
            continue
        text = row.get("text") or row.get("title")
        if not isinstance(text, str) or not text.strip():
            issue("criterion_condition_required", criterion_id=identity)
        try:
            metadata = CriterionVerification.model_validate(
                criterion_verification_fields(row)
            )
        except (ValidationError, ValueError, TypeError):
            issue("criterion_verification_invalid", criterion_id=identity)
            continue
        if metadata.verification_profile is None:
            issue("criterion_profile_required", criterion_id=identity)
        if not metadata.requirement_links:
            issue("criterion_requirement_link_missing", criterion_id=identity)
        for link in metadata.requirement_links or ():
            key = (link.requirement_type, link.requirement_id)
            if key not in active or len(nodes[key]) != 1:
                issue("criterion_requirement_link_unresolved", criterion_id=identity)
                continue
            if (
                not isinstance(text, str)
                or not text.strip()
                or metadata.verification_profile is None
            ):
                block(key, "verification_criterion_incomplete", criterion_id=identity)
                continue
            if direct_count >= _MAX_PATHS:
                complete = False
                block(key, "verification_resolution_limit")
                continue
            direct_count += 1
            direct[key].append(
                {
                    "criterion_id": identity,
                    "profile": metadata.verification_profile,
                    "path": [{"requirement_type": key[0], "requirement_id": key[1]}],
                    "aspects": [link.aspect],
                    "source_digests": [],
                }
            )

    def resolve(key):
        nonlocal path_count, complete
        if key in paths:
            return paths[key]
        if path_count >= _MAX_PATHS:
            block(key, "verification_resolution_limit")
            complete = False
            paths[key] = []
            return []
        if key in visiting:
            for owner in visiting[visiting.index(key) :]:
                block(owner, "verification_inheritance_cycle")
            return []
        if len(visiting) >= _MAX_DEPTH:
            block(key, "verification_resolution_limit")
            complete = False
            return []
        config = configs.get(key)
        if config is None:
            paths[key] = []
            return []
        visiting.append(key)
        selected = list(direct[key])
        for selection in config.inheritance:
            source = selection.source.key
            if source not in active or len(nodes[source]) != 1:
                block(key, "verification_inheritance_source_invalid")
                continue
            if digests.get(source) != selection.source_digest:
                block(
                    key,
                    "spec_scope_revision_conflict",
                    source=selection.source.model_dump(),
                )
                continue
            inherited = resolve(source)
            if blockers[source]:
                block(
                    key,
                    "verification_inheritance_source_unresolved",
                    source=selection.source.model_dump(),
                )
                continue
            for identity in selection.criterion_ids:
                matches = [
                    path for path in inherited if path["criterion_id"] == identity
                ]
                if not matches:
                    block(
                        key,
                        "verification_inheritance_terminal_missing",
                        criterion_id=identity,
                    )
                for match in matches:
                    selected.append(
                        {
                            **match,
                            "path": [
                                {"requirement_type": key[0], "requirement_id": key[1]},
                                *match["path"],
                            ],
                            "aspects": [selection.covered_aspect, *match["aspects"]],
                            "source_digests": [
                                selection.source_digest,
                                *match["source_digests"],
                            ],
                        }
                    )
                    if len(selected) > _MAX_PATHS:
                        break
                if len(selected) > _MAX_PATHS:
                    break
            if len(selected) > _MAX_PATHS:
                break
        visiting.pop()
        path_count += len(selected)
        if path_count > _MAX_PATHS:
            block(key, "verification_resolution_limit")
            complete = False
            selected = []
        covered = {path["profile"] for path in selected}
        for profile in config.required_profiles:
            if profile not in covered:
                block(key, "verification_path_missing", profile=profile)
        paths[key] = selected
        return selected

    # Detect cycles by identity independently of digest validity. A stale
    # digest in a cyclic graph is not permission to hide the structural cycle.
    visited: set[tuple[str, str]] = set()

    def detect_cycles(key, chain):
        nonlocal complete
        if key in chain:
            for owner in chain[chain.index(key) :]:
                block(owner, "verification_inheritance_cycle")
            return
        if key in visited:
            return
        if len(chain) >= _MAX_DEPTH:
            complete = False
            block(key, "verification_resolution_limit")
            return
        for selection in configs.get(key, ()).inheritance if key in configs else ():
            if selection.source.key in active:
                detect_cycles(selection.source.key, [*chain, key])
        visited.add(key)

    for key in sorted(active):
        detect_cycles(key, [])
        resolve(key)

    if not active:
        issue("spec_scope_verification_required")
    rows = []
    for key in sorted(active):
        item = nodes[key][0]
        config = configs.get(key)
        rows.append(
            {
                "requirement_type": key[0],
                "requirement_id": key[1],
                "title": str(
                    item.get("text") or item.get("title") or item.get("rule") or key[1]
                )[:500],
                "source_digest": digests.get(key),
                "verification": config.model_dump(mode="json") if config else None,
                "qualification_origin": "authored" if config else "absent_or_invalid",
                "default_proposal": verification_default_proposal(key[0]),
                "criteria_paths": paths.get(key, []),
                "blockers": blockers[key],
                "qualification_resolved": not blockers[key] and config is not None,
            }
        )
    return {
        "population_complete": complete,
        "requirements": rows,
        "issues": issues,
        "issue_count": issue_count,
        "issues_truncated": issue_count > len(issues),
        "criteria_resolution_complete": complete
        and not issue_count
        and all(row["qualification_resolved"] for row in rows),
        "methods_evaluated": False,
        "execution_evaluated": False,
        "semantic_review_evaluated": False,
        "delivery_evaluated": False,
        "rollout_evaluated": False,
    }
